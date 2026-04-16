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

    HEIMDALL_LABEL                       instance name (default: "default")
    HEIMDALL_AUDIO_CARD_NAME             ALSA card name to look up in
                                         /proc/asound/cards (default: "Neo").
                                         Special value "auto" picks the first
                                         capture-capable card not in the
                                         exclude list, preferring USB-Audio.
    HEIMDALL_AUDIO_AUTODETECT_EXCLUDE    comma-separated card names to skip
                                         when CARD_NAME=auto (default: "Neo")
    HEIMDALL_AUDIO_DEVICE                literal device escape hatch,
                                         e.g. "hw:0", "plughw:Neo,0" (ALSA)
                                         or a PulseAudio source name.
                                         Bypasses card-name lookup entirely.
    HEIMDALL_AUDIO_FORMAT                ffmpeg input format (default: "alsa").
                                         Set to "pulse" for PipeWire/PulseAudio
                                         sources (e.g. Bluetooth headsets).
    HEIMDALL_AUDIO_SOCKET                fan-out Unix socket path
                                         (default: /run/heimdall/$LABEL.sock)
    HEIMDALL_AUDIO_RATE                  sample rate Hz (default: 16000)
    HEIMDALL_AUDIO_CHANNELS              channels, downmixed by ffmpeg (default: 1)
    HEIMDALL_VIDEO_ENABLED               "1" to expose /frame.png (default: "0")
    HEIMDALL_VIDEO_DEVICE                v4l2 device (default: /dev/video0)
    HEIMDALL_HTTP_HOST                   HTTP bind address (default: 127.0.0.1)
    HEIMDALL_HTTP_PORT                   HTTP bind port (default: 7100)

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

# Audio capture device. Three ways to specify it, in priority order:
#
#   HEIMDALL_AUDIO_DEVICE — escape hatch. If set, used as a literal
#       ALSA device string (e.g. "hw:0", "plughw:Neo,0", "default").
#       Bypasses all card-name resolution. Useful if you want to point
#       at a fixed non-USB source.
#
#   HEIMDALL_AUDIO_CARD_NAME=<name> — preferred for known devices.
#       We look up the ALSA card by its persistent name in
#       /proc/asound/cards, which survives unplug/replug even if the
#       card index changes (a USB device removed and re-added may come
#       back as hw:1 instead of hw:0). Default "Neo" matches the
#       Elgato Game Capture Neo.
#
#   HEIMDALL_AUDIO_CARD_NAME=auto — preferred for "use whatever mic
#       is plugged in". Scans /proc/asound/cards for capture-capable
#       cards, skips any name in HEIMDALL_AUDIO_AUTODETECT_EXCLUDE
#       (default "Neo" — so an auto instance never accidentally claims
#       the Elgato away from heimdall@meeting), prefers USB-Audio over
#       internal HDA codecs, and picks the lowest-index match. Re-runs
#       on every recovery loop iteration so the result tracks live
#       hotplug state.
AUDIO_CARD_NAME = os.environ.get("HEIMDALL_AUDIO_CARD_NAME", "Neo")
AUDIO_DEVICE_OVERRIDE = os.environ.get("HEIMDALL_AUDIO_DEVICE", "").strip()
AUDIO_AUTODETECT_EXCLUDE = {
    s.strip()
    for s in os.environ.get("HEIMDALL_AUDIO_AUTODETECT_EXCLUDE", "Neo").split(",")
    if s.strip()
}

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
# Optional ffmpeg audio filter chain applied BEFORE the PCM output.
# Empty = no filter (passthrough). For the ted instance (close-talking
# lavalier), a noise gate + highpass filter rejects background noise
# and only passes speech from the mic's proximity zone:
#
#   HEIMDALL_AUDIO_FILTER=highpass=f=200,agate=threshold=0.015:ratio=8:attack=5:release=100
#
# The gate opens when signal exceeds ~threshold (-36 dB) — speech at
# 6 inches from a lav mic is typically -20 to -10 dB, so 0.015 passes
# speech comfortably and gates distant sounds / ambient noise.
AUDIO_FILTER = os.environ.get("HEIMDALL_AUDIO_FILTER", "").strip()
# ffmpeg input format for the audio capture device. "alsa" for hardware
# ALSA cards (default), "pulse" for PipeWire/PulseAudio sources (e.g.
# Bluetooth headsets that only appear as PipeWire nodes, not in
# /proc/asound/cards). When set to "pulse", HEIMDALL_AUDIO_DEVICE must
# name a PulseAudio source (pactl list sources short) and card-name
# resolution is skipped entirely.
AUDIO_FORMAT = os.environ.get("HEIMDALL_AUDIO_FORMAT", "alsa")
VIDEO_ENABLED = os.environ.get("HEIMDALL_VIDEO_ENABLED", "0") == "1"
VIDEO_DEVICE = os.environ.get("HEIMDALL_VIDEO_DEVICE", "/dev/video0")
# Continuous background frame grabber. Keeps /dev/video0 open and a
# rolling latest frame in memory at all times so the /snapshot.png POST
# handler returns in <10 ms instead of the ~900 ms cold-ffmpeg-spawn
# that users experience as "I have to wait before switching windows".
# Set to 0 to disable the warmer and fall back to on-demand grabs.
VIDEO_WARMER_FPS = int(os.environ.get("HEIMDALL_VIDEO_WARMER_FPS", "2"))
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

# Serializes concurrent on-demand /frame.png cold-grabs. Only used as
# a fallback when the warmer thread is down.
_frame_grab_lock = threading.Lock()

# The video warmer thread keeps a fresh PNG frame in memory at all
# times. Both /frame.png GET and /snapshot.png POST read from here
# instead of spawning a cold ffmpeg (which takes ~900 ms because of
# v4l2 warm-up frame skipping). Updated by video_warmer_loop(),
# read by grab_frame(). Thread-safe via Python's GIL for single
# assignments (the bytes object is immutable and the float is atomic
# under CPython).
_warmer_frame: bytes | None = None
_warmer_frame_at: float = 0.0
# Max age (seconds) before we consider the warmer stale and fall back
# to a cold grab. Should be well above 1/VIDEO_WARMER_FPS to allow
# for occasional CPU pressure jitter. 5 seconds is generous.
_WARMER_STALE_SEC = 5.0

stats: dict = {
    "label": LABEL,
    "audio_card_name": AUDIO_CARD_NAME,
    "audio_card_name_resolved": None,  # filled in by auto-detect if active
    "audio_autodetect_exclude": sorted(AUDIO_AUTODETECT_EXCLUDE),
    "audio_device_override": AUDIO_DEVICE_OVERRIDE or None,
    "audio_device_resolved": None,  # filled in once we open it
    "audio_rate": AUDIO_RATE,
    "audio_channels": AUDIO_CHANNELS,
    "audio_socket": str(AUDIO_SOCKET_PATH),
    "video_enabled": VIDEO_ENABLED,
    "video_device": VIDEO_DEVICE if VIDEO_ENABLED else None,
    "video_warmer_fps": VIDEO_WARMER_FPS if VIDEO_ENABLED else None,
    "video_warmer_running": False,
    "video_warmer_restarts": 0,
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

# /proc/asound/cards header line format:
#   " 3 [C920           ]: USB-Audio - HD Pro Webcam C920"
# Capture groups: index, short name, driver class.
_ALSA_CARD_LINE_RE = re.compile(r"^\s*(\d+)\s+\[(\S+)\s*\]:\s*(\S+)")


def list_alsa_cards() -> list[dict]:
    """Return one dict per ALSA card currently registered with the kernel.

    Each dict has: index (int), name (str), driver (str, e.g.
    "USB-Audio" or "HDA-Intel"), has_capture (bool — True iff the
    card exposes at least one PCM capture device).

    Returns an empty list if /proc/asound/cards is unreadable. Sorted
    by index ascending so callers can rely on deterministic ordering.
    """
    cards: list[dict] = []
    try:
        with open("/proc/asound/cards") as f:
            text = f.read()
    except FileNotFoundError:
        return cards

    for line in text.splitlines():
        m = _ALSA_CARD_LINE_RE.match(line)
        if not m:
            continue
        idx = int(m.group(1))
        # Capture-capable iff /proc/asound/cardN contains any "pcm*c"
        # entry. Playback-only cards (e.g. HDMI audio sinks) won't
        # have any pcmXc files and we'll correctly skip them.
        has_capture = False
        try:
            for entry in os.listdir(f"/proc/asound/card{idx}"):
                if re.match(r"pcm\d+c", entry):
                    has_capture = True
                    break
        except FileNotFoundError:
            pass
        cards.append({
            "index": idx,
            "name": m.group(2),
            "driver": m.group(3),
            "has_capture": has_capture,
        })
    cards.sort(key=lambda c: c["index"])
    return cards


def find_alsa_card_index(name: str) -> int | None:
    """Look up an ALSA card's integer index by its persistent card name.

    The card name is the bracketed identifier in /proc/asound/cards,
    e.g. " 0 [Neo            ]: USB-Audio - Elgato Game Capture Neo".
    USB hotplug can change the integer index (Elgato might come back
    as hw:1 after a replug), but the name stays the same.

    Returns None if the card isn't currently registered with the
    kernel — i.e. the device is unplugged.
    """
    for card in list_alsa_cards():
        if card["name"] == name:
            return card["index"]
    return None


def autodetect_capture_card(exclude: set[str]) -> tuple[int, str] | None:
    """Pick a capture-capable ALSA card to use as the audio source.

    Heuristic, in order:
      1. Drop any card whose name is in `exclude` (default "Neo" so we
         never accidentally claim the Elgato from heimdall@meeting).
      2. Drop any card without a PCM capture device.
      3. Prefer USB-Audio class cards (i.e. plugged-in mics, headsets,
         webcams) over HDA-Intel and other internal codecs.
      4. Within the preferred class, pick the lowest card index for
         deterministic ordering.

    Returns (index, name) on success, or None if no candidate exists.
    Re-evaluated every recovery loop iteration in audio_pump_loop, so
    the choice tracks live hotplug — unplug the C920 and the next
    iteration picks the next best capture device, plug it back in and
    the iteration after picks the C920 again.
    """
    candidates = [
        c for c in list_alsa_cards()
        if c["has_capture"] and c["name"] not in exclude
    ]
    if not candidates:
        return None
    usb = [c for c in candidates if c["driver"] == "USB-Audio"]
    pick = usb[0] if usb else candidates[0]
    return (pick["index"], pick["name"])


def resolve_audio_device() -> str | None:
    """Compute the audio device string to pass to ffmpeg, or None if absent.

    Priority:
      1. HEIMDALL_AUDIO_DEVICE — used verbatim (escape hatch).
      2. HEIMDALL_AUDIO_CARD_NAME == "auto" — auto-detect any plugged-in
         capture device, respecting HEIMDALL_AUDIO_AUTODETECT_EXCLUDE.
      3. HEIMDALL_AUDIO_CARD_NAME == "<name>" — look up that exact card.

    When HEIMDALL_AUDIO_FORMAT == "pulse", only HEIMDALL_AUDIO_DEVICE is
    checked (the PulseAudio source name). Card-name resolution is ALSA-only.
    """
    if AUDIO_DEVICE_OVERRIDE:
        return AUDIO_DEVICE_OVERRIDE
    if AUDIO_FORMAT == "pulse":
        return None  # no PulseAudio source name configured
    if AUDIO_CARD_NAME == "auto":
        result = autodetect_capture_card(AUDIO_AUTODETECT_EXCLUDE)
        if result is None:
            return None
        idx, name = result
        # Stash the picked card name in stats so /info shows what
        # auto-detect actually chose, not just "auto". Log only on
        # change so the journal records hot-swap events without
        # spamming on every recovery loop iteration.
        if stats.get("audio_card_name_resolved") != name:
            log.info("audio: auto-detected card %r at hw:%d", name, idx)
            stats["audio_card_name_resolved"] = name
        return f"plughw:{idx}"
    idx = find_alsa_card_index(AUDIO_CARD_NAME)
    if idx is None:
        return None
    return f"hw:{idx}"


def start_audio_ffmpeg(device: str) -> subprocess.Popen:
    cmd = [
        "ffmpeg",
        "-hide_banner", "-loglevel", "error", "-nostats",
        "-f", AUDIO_FORMAT, "-i", device,
    ]
    if AUDIO_FILTER:
        cmd.extend(["-af", AUDIO_FILTER])
    cmd.extend([
        "-ac", str(AUDIO_CHANNELS),
        "-ar", str(AUDIO_RATE),
        "-f", "s16le",
        "-",
    ])
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
            if AUDIO_CARD_NAME == "auto":
                log.warning(
                    "audio: no capture-capable ALSA card found "
                    "(exclude=%s); waiting %.1fs",
                    sorted(AUDIO_AUTODETECT_EXCLUDE), backoff,
                )
            else:
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


# ─── video warmer (continuous background frame grabber) ──────────────────────

_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


def video_warmer_loop() -> None:
    """Periodically grab a frame in the background so press-1 is fast.

    Sleeps for 1/VIDEO_WARMER_FPS seconds, then does ONE cold grab
    (the same ~900 ms ffmpeg spawn that grab_frame() used to do
    inline). The result is stored in ``_warmer_frame``. When the user
    presses the pedal, grab_frame() reads the cached frame instantly
    (<1 ms) instead of blocking the HTTP response for ~900 ms.

    This is "v2" of the warmer. v1 used a continuous ffmpeg pipe
    which maxed out all 8 cores because the Elgato's v4l2 driver
    only supports 30 fps — ffmpeg read at 30 fps even though we
    asked for 2, and burned CPU on every frame. This version spawns
    a short-lived ffmpeg per iteration and releases /dev/video0
    between grabs, keeping CPU usage to ~900 ms of work per cycle
    (one core, intermittent) instead of 771% continuous.

    Frame staleness: at most ``1/FPS + 0.9`` seconds (sleep interval
    + grab duration). At the default 2 FPS that's ~1.4 s worst case,
    which is fine for meetings where slides change on a seconds-to-
    minutes timescale. The frame is captured BEFORE the pedal press,
    not 900 ms after — which better matches user intent.
    """
    global _warmer_frame, _warmer_frame_at

    if not VIDEO_ENABLED or VIDEO_WARMER_FPS <= 0:
        log.info("video warmer: disabled (VIDEO_ENABLED=%s, FPS=%s)",
                 VIDEO_ENABLED, VIDEO_WARMER_FPS)
        return

    interval = 1.0 / VIDEO_WARMER_FPS
    log.info(
        "video warmer: starting (interval=%.1fs, cold-grab per cycle)",
        interval,
    )
    stats["video_warmer_running"] = True

    while not shutdown_event.is_set():
        if not video_device_present():
            log.warning("video warmer: %s not present; sleeping %.1fs",
                        VIDEO_DEVICE, interval)
            shutdown_event.wait(interval)
            continue

        frame = _cold_grab_frame()
        if frame is not None:
            _warmer_frame = frame
            _warmer_frame_at = time.monotonic()
            stats["video_warmer_restarts"] += 1  # reuse counter as "grabs done"

        shutdown_event.wait(interval)

    stats["video_warmer_running"] = False
    log.info("video warmer: exiting")


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
    """Return the latest PNG frame, preferring the warmer's in-memory copy.

    Fast path (sub-millisecond): read ``_warmer_frame`` which the
    video_warmer_loop thread updates continuously at VIDEO_WARMER_FPS.
    The frame is at most ~1/FPS seconds old, captured BEFORE the press
    (not ~900 ms after, which was the old cold-grab behavior).

    Fallback (~900 ms): if the warmer is down (``_warmer_frame`` is
    None or stale beyond ``_WARMER_STALE_SEC``), spawn a one-shot
    ffmpeg to grab a single frame the old way. This keeps heimdall
    functional even if the warmer thread dies.
    """
    # Fast path: warmer has a recent frame.
    frame = _warmer_frame
    frame_at = _warmer_frame_at
    if frame and (time.monotonic() - frame_at) < _WARMER_STALE_SEC:
        age_ms = (time.monotonic() - frame_at) * 1000
        log.info("video: served warmer frame (%.0fms old, %d bytes)",
                 age_ms, len(frame))
        stats["frames_served"] += 1
        stats["last_frame_at"] = time.time()
        return frame

    # Fallback: cold grab (original ~900ms path).
    log.warning("video: warmer frame stale or missing, falling back to cold grab")
    return _cold_grab_frame()


def _cold_grab_frame() -> bytes | None:
    """Spawn a short-lived ffmpeg to grab a single PNG frame (fallback).

    Only called when the video warmer is down. Takes ~900 ms due to
    v4l2 warm-up frame skipping. Serialized via _frame_grab_lock so
    concurrent requests don't race for /dev/video0 (which is exclusive).
    """
    with _frame_grab_lock:
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
            log.error("video: cold grab timed out")
            stats["frame_errors"] += 1
            return None
        if result.returncode != 0 or not result.stdout:
            log.error(
                "video: cold grab ffmpeg failed rc=%d stderr=%s",
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
    """IPv6 dual-stack HTTP server.

    The stock `http.server.HTTPServer` defaults to `AF_INET` — IPv4 only.
    avahi-daemon on agneta announces both an A and an AAAA record for
    `agneta.local`, and Chrome (and many other clients that use
    happy-eyeballs with IPv6-preference) tries the AAAA first. If we're
    only bound to IPv4 that attempt fails with `ERR_ADDRESS_UNREACHABLE`
    before the client falls back to IPv4 — unless the client is willing
    to race both, which curl does and Chrome sometimes doesn't.

    Fix: bind to an IPv6 socket with `IPV6_V6ONLY=0`, which on Linux
    accepts IPv4 connections as v4-mapped v6 addresses on the same
    socket. One port, both address families, no duplicate accept loop.
    `IPV6_V6ONLY=0` is already the Linux default, but we set it
    explicitly so the behavior survives a `/proc/sys/net/ipv6/bindv6only`
    change.
    """
    daemon_threads = True
    allow_reuse_address = True
    address_family = socket.AF_INET6

    def server_bind(self) -> None:
        self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        super().server_bind()


def _resolve_bind_host(host: str) -> str:
    """Translate IPv4-style wildcard/loopback addresses into IPv6 equivalents
    so they work with the AF_INET6 dual-stack socket above.

    Maps:
      "0.0.0.0" / ""      → "::"       (wildcard, accepts v4 and v6)
      "127.0.0.1"         → "::1"      (loopback — v6 loopback only, but
                                         v4 loopback is generally covered by
                                         mimir/odin running on the same host
                                         as heimdall; this is a judgement call)

    Other values are passed through unchanged. If you pin HEIMDALL_HTTP_HOST
    to a specific v4 literal (e.g. a LAN IP), that may fail — use the v6
    form or the host's mDNS name instead.
    """
    if host in ("0.0.0.0", ""):
        return "::"
    if host == "127.0.0.1":
        return "::1"
    return host


def http_loop() -> None:
    bind_host = _resolve_bind_host(HTTP_HOST)
    server = ThreadedHTTPServer((bind_host, HTTP_PORT), HeimdallHandler)
    server.timeout = 1.0
    log.info(
        "http: listening on http://%s:%d (configured host=%s, dual-stack)",
        bind_host, HTTP_PORT, HTTP_HOST,
    )
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
        threading.Thread(target=video_warmer_loop, name="video-warmer", daemon=True),
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
