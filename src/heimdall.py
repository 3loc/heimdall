#!/usr/bin/env python3
"""heimdall — HDMI capture daemon for loki.

Reads audio from a single ALSA capture device, fans it out as raw PCM
(s16le, 16 kHz mono by default — Whisper-ready) over a Unix domain
socket. Optionally serves on-demand PNG frame grabs from a v4l2 video
device over HTTP.

Designed to run as a systemd template service: one Python process per
audio source. The "meeting" instance captures the Elgato HDMI audio
and the Elgato HDMI video. A future "ted" instance will capture from
a USB mic on agneta and have video disabled.

Configuration is by environment variable, normally set via a systemd
EnvironmentFile per instance:

    HEIMDALL_LABEL              instance name (default: "default")
    HEIMDALL_AUDIO_DEVICE       ALSA capture device (default: "hw:0")
    HEIMDALL_AUDIO_SOCKET       fan-out Unix socket path
                                (default: /run/heimdall/$LABEL.sock)
    HEIMDALL_AUDIO_RATE         sample rate Hz (default: 16000)
    HEIMDALL_AUDIO_CHANNELS     channels, downmixed by ffmpeg (default: 1)
    HEIMDALL_VIDEO_ENABLED      "1" to expose /frame.png (default: "0")
    HEIMDALL_VIDEO_DEVICE       v4l2 device (default: /dev/video0)
    HEIMDALL_HTTP_HOST          HTTP bind address (default: 127.0.0.1)
    HEIMDALL_HTTP_PORT          HTTP bind port (default: 7100)

Endpoints (always served — /frame.png 404s when video is disabled):

    GET /healthz   → 200 "ok"
    GET /info      → 200 application/json with runtime stats
    GET /frame.png → 200 image/png (on-demand v4l2 grab)
                     404 when HEIMDALL_VIDEO_ENABLED != "1"

Audio fanout: connect a Unix socket client to HEIMDALL_AUDIO_SOCKET and
read raw bytes. The stream is continuous and unframed; consumers chunk
it themselves. New clients receive audio from "now"; there is no
replay of past audio. Slow consumers will block the fanout — the only
real consumer is mimir, which is expected to drain continuously.
"""

from __future__ import annotations

import http.server
import json
import logging
import os
import re
import select
import signal
import socket
import socketserver
import subprocess
import sys
import threading
import time
from pathlib import Path


# ─── config ──────────────────────────────────────────────────────────────────

LABEL = os.environ.get("HEIMDALL_LABEL", "default")

# Audio capture device. There are two ways to specify it:
#
#   HEIMDALL_AUDIO_CARD_NAME — preferred. We look up the ALSA card by
#       its persistent name in /proc/asound/cards, which survives
#       unplug/replug even if the card index changes (a USB device
#       removed and re-added may come back as hw:1 instead of hw:0).
#       Default "Neo" matches the Elgato Game Capture Neo.
#
#   HEIMDALL_AUDIO_DEVICE — escape hatch. If set, used as a literal
#       ALSA device string (e.g. "hw:0", "plughw:Neo,0", "default").
#       Bypasses the dynamic lookup. Useful if you want to point at a
#       fixed non-Elgato source.
AUDIO_CARD_NAME = os.environ.get("HEIMDALL_AUDIO_CARD_NAME", "Neo")
AUDIO_DEVICE_OVERRIDE = os.environ.get("HEIMDALL_AUDIO_DEVICE", "").strip()

AUDIO_SOCKET_PATH = Path(
    os.environ.get("HEIMDALL_AUDIO_SOCKET", f"/run/heimdall/{LABEL}.sock")
)

# Where the captured snapshot lives on disk. POST /snapshot.png writes
# here (atomically); GET /snapshot.png serves it. Single file per
# instance — each new POST overwrites the previous snapshot.
SNAPSHOT_PATH = Path(
    os.environ.get("HEIMDALL_SNAPSHOT_PATH", f"/run/heimdall/{LABEL}-snapshot.png")
)
AUDIO_RATE = int(os.environ.get("HEIMDALL_AUDIO_RATE", "16000"))
AUDIO_CHANNELS = int(os.environ.get("HEIMDALL_AUDIO_CHANNELS", "1"))
VIDEO_ENABLED = os.environ.get("HEIMDALL_VIDEO_ENABLED", "0") == "1"
VIDEO_DEVICE = os.environ.get("HEIMDALL_VIDEO_DEVICE", "/dev/video0")
HTTP_HOST = os.environ.get("HEIMDALL_HTTP_HOST", "127.0.0.1")
HTTP_PORT = int(os.environ.get("HEIMDALL_HTTP_PORT", "7100"))

# How long to wait on the audio pipe before declaring ffmpeg wedged
# and restarting it. The Elgato produces audio at ~32 kB/s when alive,
# so any silence > a few seconds means something is wrong (HDMI signal
# loss, USB suspend, ffmpeg internal hang, etc).
AUDIO_READ_TIMEOUT_SEC = 5.0

# 100 ms PCM chunks (16 kHz × 1 ch × 2 bytes / 10 = 3200 bytes)
CHUNK_BYTES = AUDIO_RATE * AUDIO_CHANNELS * 2 // 10


# ─── shared state ────────────────────────────────────────────────────────────

log = logging.getLogger(f"heimdall.{LABEL}")
shutdown_event = threading.Event()

audio_subscribers: list[socket.socket] = []
audio_subscribers_lock = threading.Lock()
audio_proc: subprocess.Popen | None = None

# Serializes concurrent /frame.png requests so only one ffmpeg at a time
# holds /dev/video0 (v4l2 devices are exclusive — overlapping opens
# return EBUSY). Also gates the small warm-frame cache below.
_frame_grab_lock = threading.Lock()
_cached_frame_bytes: bytes | None = None
_cached_frame_at: float = 0.0
# How long to serve a cached frame instead of grabbing fresh. A real
# pedal-press is on the order of seconds apart, so 500ms is short
# enough that nobody notices but long enough to deduplicate
# refresh-storms (e.g. browser-refresh testing).
_FRAME_CACHE_TTL_SEC = 0.5

stats: dict = {
    "label": LABEL,
    "audio_card_name": AUDIO_CARD_NAME,
    "audio_device_override": AUDIO_DEVICE_OVERRIDE or None,
    "audio_device_resolved": None,  # filled in once we open it
    "audio_rate": AUDIO_RATE,
    "audio_channels": AUDIO_CHANNELS,
    "audio_socket": str(AUDIO_SOCKET_PATH),
    "video_enabled": VIDEO_ENABLED,
    "video_device": VIDEO_DEVICE if VIDEO_ENABLED else None,
    "snapshot_path": str(SNAPSHOT_PATH),
    "http_endpoint": f"http://{HTTP_HOST}:{HTTP_PORT}",
    "started_at": None,
    "audio_bytes": 0,
    "audio_subscribers": 0,
    "audio_ffmpeg_restarts": 0,
    "frames_served": 0,
    "frame_errors": 0,
    "last_frame_at": None,
    "snapshot_taken_at": None,
    "snapshot_bytes": 0,
}


# ─── audio capture ───────────────────────────────────────────────────────────

def find_alsa_card_index(name: str) -> int | None:
    """Look up an ALSA card's integer index by its persistent card name.

    The card name is the bracketed identifier in /proc/asound/cards,
    e.g. " 0 [Neo            ]: USB-Audio - Elgato Game Capture Neo".
    USB hotplug can change the integer index (Elgato might come back
    as hw:1 after a replug), but the name stays the same.

    Returns None if the card isn't currently registered with the
    kernel — i.e. the device is unplugged.
    """
    try:
        with open("/proc/asound/cards") as f:
            for line in f:
                m = re.match(r"\s*(\d+)\s+\[(\S+)\s*\]", line)
                if m and m.group(2) == name:
                    return int(m.group(1))
    except FileNotFoundError:
        pass
    return None


def resolve_audio_device() -> str | None:
    """Compute the ALSA device string to pass to ffmpeg, or None if absent.

    If HEIMDALL_AUDIO_DEVICE is set, use it verbatim (escape hatch).
    Otherwise look up HEIMDALL_AUDIO_CARD_NAME in /proc/asound/cards
    and build hw:N from the resolved index.
    """
    if AUDIO_DEVICE_OVERRIDE:
        return AUDIO_DEVICE_OVERRIDE
    idx = find_alsa_card_index(AUDIO_CARD_NAME)
    if idx is None:
        return None
    return f"hw:{idx}"


def start_audio_ffmpeg(device: str) -> subprocess.Popen:
    cmd = [
        "ffmpeg",
        "-hide_banner", "-loglevel", "error", "-nostats",
        "-f", "alsa", "-i", device,
        "-ac", str(AUDIO_CHANNELS),
        "-ar", str(AUDIO_RATE),
        "-f", "s16le",
        "-",
    ]
    log.info("audio: starting %s", " ".join(cmd))
    return subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0
    )


def _terminate_audio_proc(proc: subprocess.Popen) -> None:
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()


def audio_pump_loop() -> None:
    """Long-lived: keep PCM flowing from the configured ALSA card to subscribers.

    Recovery loop. On every iteration: wait for the device to be
    present, spawn ffmpeg, read with a select() timeout, and broadcast
    chunks. Any failure (device gone, ffmpeg EOF, read timeout, etc.)
    cleans up ffmpeg and loops back to the top with exponential
    backoff. The loop only exits if shutdown_event is set.

    This means heimdall survives:
      * The Elgato being physically unplugged and re-plugged at any
        delay (the card index can even change — we look it up by name).
      * HDMI signal loss / source machine sleeping the display.
      * ffmpeg crashing or hanging mid-stream.
      * USB autosuspend.

    Without restarting the daemon, without losing TCP/HTTP/Unix-socket
    state, without dropping connected subscribers (mimir keeps its
    connection open and just sees a quiet period until audio resumes).
    """
    global audio_proc
    backoff = 0.5
    while not shutdown_event.is_set():
        # 1. Wait for the device to be present.
        device = resolve_audio_device()
        if device is None:
            log.warning(
                "audio: ALSA card %r not present in /proc/asound/cards; "
                "waiting %.1fs",
                AUDIO_CARD_NAME, backoff,
            )
            stats["audio_device_resolved"] = None
            shutdown_event.wait(backoff)
            backoff = min(backoff * 2, 5.0)
            continue

        # 2. Spawn ffmpeg.
        try:
            audio_proc = start_audio_ffmpeg(device)
        except Exception as e:
            log.error("audio: failed to spawn ffmpeg: %s", e)
            shutdown_event.wait(backoff)
            backoff = min(backoff * 2, 5.0)
            continue

        stats["audio_device_resolved"] = device
        stats["audio_ffmpeg_restarts"] += 1
        log.info("audio: ffmpeg up (device=%s, restart#%d)",
                 device, stats["audio_ffmpeg_restarts"])
        backoff = 0.5  # reset backoff after a successful start

        # 3. Read loop with select() timeout.
        stdout_fd = audio_proc.stdout.fileno()
        try:
            while not shutdown_event.is_set():
                ready, _, _ = select.select([stdout_fd], [], [], AUDIO_READ_TIMEOUT_SEC)
                if not ready:
                    log.warning(
                        "audio: no data from ffmpeg in %.0fs; assuming wedged, "
                        "restarting",
                        AUDIO_READ_TIMEOUT_SEC,
                    )
                    break
                try:
                    chunk = os.read(stdout_fd, CHUNK_BYTES)
                except OSError as e:
                    log.warning("audio: read error: %s", e)
                    break
                if not chunk:
                    # EOF — ffmpeg exited (Elgato unplugged, kernel
                    # closed the device, etc.). Don't try to read
                    # stderr here; it might also be closed and would
                    # block. The next iteration will pick up the new
                    # state.
                    log.warning("audio: ffmpeg EOF (rc=%s)", audio_proc.poll())
                    break
                stats["audio_bytes"] += len(chunk)
                _broadcast(chunk)
        finally:
            _terminate_audio_proc(audio_proc)
            audio_proc = None

        if not shutdown_event.is_set():
            log.info("audio: will retry in %.1fs", backoff)
            shutdown_event.wait(backoff)
            backoff = min(backoff * 2, 5.0)

    log.info("audio: pump exited")


def _broadcast(chunk: bytes) -> None:
    """Send a chunk to every subscriber. Drop slow or dead ones.

    Subscribers are non-blocking sockets (set in audio_socket_loop on
    accept). A healthy subscriber drains the kernel send buffer fast
    enough that send() returns the full byte count immediately. A slow
    or dead subscriber raises BlockingIOError once its send buffer
    fills, at which point we drop it — better to lose one slow client
    than to freeze the whole fanout for the rest. This is the fix for
    the bug where a `kill -9`'d subscriber whose buffer still held
    bytes would block the entire pump loop indefinitely.
    """
    dead: list[socket.socket] = []
    with audio_subscribers_lock:
        for sub in audio_subscribers:
            try:
                sent = sub.send(chunk)
            except BlockingIOError:
                log.warning("audio: subscriber too slow (kernel buffer full), dropping")
                dead.append(sub)
                continue
            except (BrokenPipeError, ConnectionResetError, OSError) as e:
                log.info("audio: subscriber dead (%s)", e)
                dead.append(sub)
                continue
            if sent < len(chunk):
                # Partial write: buffer was almost full. Drop rather
                # than retry — losing one slightly-truncated chunk is
                # better than risking a deadlock or stale audio.
                log.warning("audio: subscriber partial write (%d/%d), dropping",
                            sent, len(chunk))
                dead.append(sub)
        for sub in dead:
            audio_subscribers.remove(sub)
            try:
                sub.close()
            except OSError:
                pass
        stats["audio_subscribers"] = len(audio_subscribers)


def audio_socket_loop() -> None:
    """Listen on the Unix socket and register new subscribers."""
    AUDIO_SOCKET_PATH.parent.mkdir(parents=True, exist_ok=True)
    if AUDIO_SOCKET_PATH.exists():
        AUDIO_SOCKET_PATH.unlink()

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(AUDIO_SOCKET_PATH))
    server.listen(8)
    AUDIO_SOCKET_PATH.chmod(0o660)
    server.settimeout(1.0)
    log.info("audio: listening on %s", AUDIO_SOCKET_PATH)
    try:
        while not shutdown_event.is_set():
            try:
                conn, _ = server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            # Non-blocking sends are critical: without this, a dead-but-
            # still-ESTAB peer (e.g. one we kill -9'd while its kernel
            # send buffer wasn't empty) would block sendall() forever
            # and freeze the entire fanout. With non-blocking, sendall
            # raises BlockingIOError instead and _broadcast drops the
            # dead subscriber.
            conn.setblocking(False)
            with audio_subscribers_lock:
                audio_subscribers.append(conn)
                stats["audio_subscribers"] = len(audio_subscribers)
            log.info("audio: subscriber connected (%d total)", len(audio_subscribers))
    finally:
        server.close()
        try:
            AUDIO_SOCKET_PATH.unlink()
        except FileNotFoundError:
            pass
        log.info("audio: socket loop exited")


# ─── video frame grab ────────────────────────────────────────────────────────

def video_device_present() -> bool:
    """Quick check whether the v4l2 device file exists right now.

    Used by the HTTP handler to return 503 ("device not available")
    instead of 500 ("frame grab failed") when the Elgato is unplugged.
    Cheap stat — no ffmpeg spawn.
    """
    try:
        return Path(VIDEO_DEVICE).exists()
    except OSError:
        return False


def grab_frame() -> bytes | None:
    """Spawn a short-lived ffmpeg to grab a single PNG frame.

    Three ffmpeg flags here are non-obvious, plus a serializing lock and
    a small cache wrapping the call:

    * ``-input_format nv12 -video_size 1920x1080 -framerate 30`` —
      without these, v4l2 defaults to 1280x720 YUYV even though the
      Elgato is receiving and capable of 1080p. Forcing the format
      explicitly upgrades the grab to full HD, which is a real
      legibility win for screenshots of small UI text. NV12 is
      slightly less bandwidth than YUYV at the same dimensions.

    * ``-vf "select='gte(n\\,10)'" -vsync 0`` discards the first 10
      frames after STREAMON. Fresh-opened v4l2 devices return stale
      buffers or all-zero black frames for the first few hundred ms
      while the capture pipeline ramps up — taking frame 0 with
      ``-frames:v 1`` reliably gives garbage on the Elgato. Skipping
      to frame 10 (~333ms at 30fps) gives clean content every time.

    * ``-c:v png -f image2pipe`` is required to actually produce PNG.
      Without ``-c:v png`` ffmpeg's image2 muxer guesses MJPEG when
      the output is a pipe (because there is no filename extension to
      infer from), and you get a JPEG even though the URL says .png.

    On top of ffmpeg, the function takes ``_frame_grab_lock`` to
    serialize concurrent grabs (otherwise overlapping HTTP requests
    race for /dev/video0 and one or both get EBUSY and 500), and
    serves a cached frame for FRAME_CACHE_TTL after a successful grab
    so refresh-storms don't hammer the device.
    """
    global _cached_frame_bytes, _cached_frame_at

    with _frame_grab_lock:
        # Serve cached if recent enough.
        now = time.monotonic()
        if _cached_frame_bytes and (now - _cached_frame_at) < _FRAME_CACHE_TTL_SEC:
            age_ms = (now - _cached_frame_at) * 1000
            log.info("video: served cached frame (%.0fms old, %d bytes)",
                     age_ms, len(_cached_frame_bytes))
            stats["frames_served"] += 1
            stats["last_frame_at"] = time.time()
            return _cached_frame_bytes

        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            "-f", "v4l2",
            "-input_format", "nv12",
            "-video_size", "1920x1080",
            "-framerate", "30",
            "-i", VIDEO_DEVICE,
            "-vf", "select='gte(n\\,10)'",
            "-vsync", "0",
            "-frames:v", "1",
            "-c:v", "png",
            "-f", "image2pipe",
            "-",
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, timeout=10)
        except subprocess.TimeoutExpired:
            log.error("video: frame grab timed out")
            stats["frame_errors"] += 1
            return None
        if result.returncode != 0 or not result.stdout:
            log.error(
                "video: ffmpeg failed rc=%d stderr=%s",
                result.returncode,
                result.stderr.decode("utf-8", errors="replace").strip(),
            )
            stats["frame_errors"] += 1
            return None

        # Set cache timestamp AFTER ffmpeg returns, not before. If we
        # used the pre-ffmpeg `now`, the cache TTL would burn through
        # the 1+ seconds ffmpeg takes and a request that arrives
        # immediately after the lock is released would see an "old"
        # frame and refetch — defeating the cache.
        _cached_frame_bytes = result.stdout
        _cached_frame_at = time.monotonic()
        stats["frames_served"] += 1
        stats["last_frame_at"] = time.time()
        return result.stdout


# ─── HTTP server ─────────────────────────────────────────────────────────────

class HeimdallHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        log.debug("http: " + fmt, *args)

    def do_GET(self):  # noqa: N802 (BaseHTTPRequestHandler API)
        if self.path == "/healthz":
            return self._send(200, b"ok\n", "text/plain")
        if self.path == "/info":
            body = json.dumps(stats, default=str, indent=2).encode() + b"\n"
            return self._send(200, body, "application/json")
        if self.path == "/frame.png":
            return self._handle_frame()
        if self.path == "/snapshot.png":
            return self._handle_snapshot_get()
        self.send_error(404, "not found")

    def do_POST(self):  # noqa: N802
        if self.path == "/snapshot.png":
            return self._handle_snapshot_post()
        self.send_error(404, "not found")

    def _handle_frame(self) -> None:
        """Live frame grab — fresh capture on every call (with 500ms cache)."""
        if not VIDEO_ENABLED:
            return self.send_error(404, "video not enabled on this instance")
        # If the v4l2 device file is missing, the Elgato is unplugged.
        # Skip the ffmpeg spawn (which would fail with rc!=0 in ~50ms
        # anyway) and serve the cached frame if we have a recent one,
        # otherwise return 503 so the client knows it's a transient
        # device-gone state, not a code bug.
        if not video_device_present():
            if _cached_frame_bytes:
                age = time.monotonic() - _cached_frame_at
                log.info("video: device %s missing, serving cached frame (%.0fms old)",
                         VIDEO_DEVICE, age * 1000)
                return self._send(200, _cached_frame_bytes, "image/png")
            return self.send_error(503, f"video device {VIDEO_DEVICE} not available")
        png = grab_frame()
        if png is None:
            return self.send_error(500, "frame grab failed")
        return self._send(200, png, "image/png")

    def _handle_snapshot_get(self) -> None:
        """Serve the most recently POST'd snapshot file."""
        if not VIDEO_ENABLED:
            return self.send_error(404, "video not enabled on this instance")
        try:
            data = SNAPSHOT_PATH.read_bytes()
        except FileNotFoundError:
            return self.send_error(
                404, "no snapshot taken yet — POST /snapshot.png to capture one"
            )
        except OSError as e:
            return self.send_error(500, f"snapshot read failed: {e}")
        return self._send(200, data, "image/png")

    def _handle_snapshot_post(self) -> None:
        """Capture the current frame, save it to disk, return 200 JSON.

        odin POSTs here on its first pedal press of the cycle. The
        saved file is then served by GET /snapshot.png until the
        next POST overwrites it. The atomic .tmp + rename means a
        concurrent GET never sees a half-written file.
        """
        if not VIDEO_ENABLED:
            return self.send_error(404, "video not enabled on this instance")
        if not video_device_present():
            return self.send_error(503, f"video device {VIDEO_DEVICE} not available")
        png = grab_frame()
        if png is None:
            return self.send_error(500, "frame grab failed")
        try:
            SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
            tmp = SNAPSHOT_PATH.with_suffix(".png.tmp")
            tmp.write_bytes(png)
            tmp.rename(SNAPSHOT_PATH)
        except OSError as e:
            log.error("snapshot: save failed: %s", e)
            return self.send_error(500, f"snapshot save failed: {e}")
        stats["snapshot_taken_at"] = time.time()
        stats["snapshot_bytes"] = len(png)
        log.info("snapshot: saved %d bytes to %s", len(png), SNAPSHOT_PATH)
        body = json.dumps(
            {"saved": str(SNAPSHOT_PATH), "bytes": len(png)}
        ).encode() + b"\n"
        return self._send(200, body, "application/json")

    def _send(self, code: int, body: bytes, ctype: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def http_loop() -> None:
    server = ThreadedHTTPServer((HTTP_HOST, HTTP_PORT), HeimdallHandler)
    server.timeout = 1.0
    log.info("http: listening on http://%s:%d", HTTP_HOST, HTTP_PORT)
    try:
        while not shutdown_event.is_set():
            server.handle_request()
    finally:
        server.server_close()
        log.info("http: loop exited")


# ─── main ────────────────────────────────────────────────────────────────────

def shutdown(signum, frame) -> None:
    log.info("received signal %d, shutting down", signum)
    shutdown_event.set()
    if audio_proc and audio_proc.poll() is None:
        audio_proc.terminate()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    stats["started_at"] = time.time()
    log.info(
        "heimdall starting: label=%s audio_card=%s%s video=%s http=%s:%d",
        LABEL,
        AUDIO_CARD_NAME,
        f" (override={AUDIO_DEVICE_OVERRIDE})" if AUDIO_DEVICE_OVERRIDE else "",
        "yes" if VIDEO_ENABLED else "no",
        HTTP_HOST, HTTP_PORT,
    )

    threads = [
        threading.Thread(target=audio_socket_loop, name="audio-sock", daemon=True),
        threading.Thread(target=audio_pump_loop, name="audio-pump", daemon=True),
        threading.Thread(target=http_loop, name="http", daemon=True),
    ]
    for t in threads:
        t.start()

    # Watchdog: if the audio pump dies, exit so systemd restarts us.
    while not shutdown_event.is_set():
        for t in threads:
            if t.name == "audio-pump" and not t.is_alive():
                log.error("audio-pump thread died, exiting for systemd restart")
                shutdown_event.set()
                break
        shutdown_event.wait(1.0)

    log.info("heimdall stopped")
    sys.exit(0)


if __name__ == "__main__":
    main()
