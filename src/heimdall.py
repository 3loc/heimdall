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
AUDIO_DEVICE = os.environ.get("HEIMDALL_AUDIO_DEVICE", "hw:0")
AUDIO_SOCKET_PATH = Path(
    os.environ.get("HEIMDALL_AUDIO_SOCKET", f"/run/heimdall/{LABEL}.sock")
)
AUDIO_RATE = int(os.environ.get("HEIMDALL_AUDIO_RATE", "16000"))
AUDIO_CHANNELS = int(os.environ.get("HEIMDALL_AUDIO_CHANNELS", "1"))
VIDEO_ENABLED = os.environ.get("HEIMDALL_VIDEO_ENABLED", "0") == "1"
VIDEO_DEVICE = os.environ.get("HEIMDALL_VIDEO_DEVICE", "/dev/video0")
HTTP_HOST = os.environ.get("HEIMDALL_HTTP_HOST", "127.0.0.1")
HTTP_PORT = int(os.environ.get("HEIMDALL_HTTP_PORT", "7100"))

# 100 ms PCM chunks (16 kHz × 1 ch × 2 bytes / 10 = 3200 bytes)
CHUNK_BYTES = AUDIO_RATE * AUDIO_CHANNELS * 2 // 10


# ─── shared state ────────────────────────────────────────────────────────────

log = logging.getLogger(f"heimdall.{LABEL}")
shutdown_event = threading.Event()

audio_subscribers: list[socket.socket] = []
audio_subscribers_lock = threading.Lock()
audio_proc: subprocess.Popen | None = None

stats: dict = {
    "label": LABEL,
    "audio_device": AUDIO_DEVICE,
    "audio_rate": AUDIO_RATE,
    "audio_channels": AUDIO_CHANNELS,
    "audio_socket": str(AUDIO_SOCKET_PATH),
    "video_enabled": VIDEO_ENABLED,
    "video_device": VIDEO_DEVICE if VIDEO_ENABLED else None,
    "http_endpoint": f"http://{HTTP_HOST}:{HTTP_PORT}",
    "started_at": None,
    "audio_bytes": 0,
    "audio_subscribers": 0,
    "frames_served": 0,
    "frame_errors": 0,
    "last_frame_at": None,
}


# ─── audio capture ───────────────────────────────────────────────────────────

def start_audio_ffmpeg() -> subprocess.Popen:
    cmd = [
        "ffmpeg",
        "-hide_banner", "-loglevel", "error", "-nostats",
        "-f", "alsa", "-i", AUDIO_DEVICE,
        "-ac", str(AUDIO_CHANNELS),
        "-ar", str(AUDIO_RATE),
        "-f", "s16le",
        "-",
    ]
    log.info("audio: starting %s", " ".join(cmd))
    return subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0
    )


def audio_pump_loop() -> None:
    """Read PCM from the ffmpeg child, fan out to all connected subscribers."""
    global audio_proc
    audio_proc = start_audio_ffmpeg()
    try:
        while not shutdown_event.is_set():
            chunk = audio_proc.stdout.read(CHUNK_BYTES)
            if not chunk:
                stderr = audio_proc.stderr.read().decode("utf-8", errors="replace")
                log.error("audio: ffmpeg stdout closed; stderr=%s", stderr.strip())
                break
            stats["audio_bytes"] += len(chunk)
            _broadcast(chunk)
    finally:
        if audio_proc and audio_proc.poll() is None:
            audio_proc.terminate()
            try:
                audio_proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                audio_proc.kill()
        log.info("audio: pump exited")


def _broadcast(chunk: bytes) -> None:
    dead: list[socket.socket] = []
    with audio_subscribers_lock:
        for sub in audio_subscribers:
            try:
                sub.sendall(chunk)
            except (BrokenPipeError, ConnectionResetError, OSError) as e:
                log.info("audio: subscriber dead (%s)", e)
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

def grab_frame() -> bytes | None:
    """Spawn a short-lived ffmpeg to grab a single PNG frame.

    Note: ``-c:v png -f image2pipe`` is required to actually produce PNG.
    Without ``-c:v png`` ffmpeg's image2 muxer guesses MJPEG when the
    output is a pipe (because there is no filename extension to infer
    from), and you get a JPEG even though the URL says .png.
    """
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-f", "v4l2", "-i", VIDEO_DEVICE,
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
            if not VIDEO_ENABLED:
                return self.send_error(404, "video not enabled on this instance")
            png = grab_frame()
            if png is None:
                return self.send_error(500, "frame grab failed")
            return self._send(200, png, "image/png")
        self.send_error(404, "not found")

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
        "heimdall starting: label=%s audio=%s video=%s http=%s:%d",
        LABEL, AUDIO_DEVICE, "yes" if VIDEO_ENABLED else "no", HTTP_HOST, HTTP_PORT,
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
