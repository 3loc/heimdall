# CLAUDE.md — heimdall

## What this is

**heimdall** is the HDMI capture module for [loki](https://github.com/3loc/loki). It is the all-seeing, all-hearing component: one process opens the Elgato Game Capture Neo as a UVC device and exposes both an audio stream (continuous) and a video frame buffer (frame-grab on demand) to the rest of the loki stack.

Named after Heimdall, the Norse watchman of the gods — said to see for hundreds of miles and hear grass grow. Sight and hearing in one component, which is exactly the job.

## Why one component for both audio and video

The Elgato Neo enumerates as a single UVC device (`/dev/video0`, `/dev/media0`) that carries both video and audio. Splitting capture into two processes would mean two readers fighting over the same device, which UVC won't allow. heimdall opens the device once and fans out internally.

## Role in loki

```
HDMI from host ──► FeinTech splitter ──► Elgato Neo ──► heimdall ──┬── audio stream  ──► mimir (transcribe, future)
                                                                    │
                                                                    └── frame buffer  ──► odin  (orchestrator, future)
                                                                                              (frame grab on foot pedal)
```

heimdall does **not** do transcription, screen-change detection, OCR, or any ML. It is a thin capture layer.

## Hardware

- **Host:** Minisforum Venus UM790 Pro (`agneta` on the LAN at 192.168.10.13). Ryzen 9 7940HS, Radeon 780M, 32GB DDR5, 1TB NVMe, Arch Linux.
- **Capture device:** Elgato Game Capture Neo, USB ID `0fd9:008c`, enumerates as `/dev/video0` + `/dev/video1` + `/dev/media0` via `uvcvideo`.
- **Splitter:** FeinTech SP210 (HDMI 2.1, EDID-managed).

## Status

- [x] Scaffolding + ansible playbook
- [x] System packages installed (ffmpeg, v4l-utils, alsa-utils, openbsd-netcat, pipewire stack, python)
- [x] **Day-3 milestone:** end-to-end UVC path proven on `agneta`
- [x] **Heimdall daemon written and running** as a systemd template service
  - `repos/heimdall/src/heimdall.py` — pure stdlib Python, ~250 lines
  - `repos/heimdall/systemd/heimdall@.service` — template unit, one instance per audio source
  - `heimdall@meeting.service` is enabled and running on `agneta`, capturing the Elgato HDMI audio + video
  - Audio fans out as 16 kHz mono PCM (s16le, Whisper-ready) on `/run/heimdall/meeting.sock`
  - Video served on demand as PNG via `GET http://127.0.0.1:7100/frame.png`
  - Stats and health on `/info` and `/healthz`
  - Verified live: `make probe` shows the daemon active, `frames_served: 1+`, audio fanout delivering exactly 32 kB/s on the socket
- [ ] `heimdall@ted.service` — placeholder env file at `/etc/heimdall/ted.env.example`. Activate when the USB mic for Ted's voice arrives: copy to `/etc/heimdall/ted.env`, set `HEIMDALL_AUDIO_DEVICE` to the new card index, `systemctl enable --now heimdall@ted.service`. No code changes required.
- [ ] Integration with mimir (mimir consumes `/run/heimdall/meeting.sock` once it exists)
- [ ] Integration with odin (odin calls `GET /frame.png` on foot pedal press once it exists)

## Architecture summary

Single Python process per ALSA source. Each instance:

1. Spawns one long-lived `ffmpeg` child reading the configured ALSA device → 16 kHz mono PCM → stdin pipe.
2. Reads that pipe in a fanout thread; broadcasts each 100 ms chunk to every connected subscriber on a Unix domain socket.
3. Runs an HTTP server on 127.0.0.1 with `/healthz`, `/info`, and (if `HEIMDALL_VIDEO_ENABLED=1`) `/frame.png`.
4. Each `/frame.png` request spawns a *short-lived* ffmpeg child to grab one PNG from `/dev/video0` and exits. The video device is **not** held open between grabs — that would conflict with the long-lived audio capture per the constraint discovered in the 2026-04-08 session note.

**Two instances run concurrently when the Ted mic arrives:**

| Instance | Audio | Video | Audio socket | HTTP |
|---|---|---|---|---|
| `heimdall@meeting` (now) | `hw:0` (Elgato) | yes | `/run/heimdall/meeting.sock` | `127.0.0.1:7100` |
| `heimdall@ted` (later) | `hw:N` (USB mic) | no | `/run/heimdall/ted.sock` | `127.0.0.1:7101` |

Mimir will subscribe to both sockets and tag each transcript chunk by source — no pyannote diarization needed (see ADR 0007).

## Layout

```
heimdall/
├── CLAUDE.md
├── README.md
├── Makefile
├── ansible/
│   ├── inventory.yml         (agneta as localhost via ansible_connection: local)
│   └── deploy-heimdall.yml   (system packages, group memberships, idempotent)
└── src/                       (empty — Python module to come)
```

## Deployment model

heimdall is deployed *to the same host it runs on* (`agneta`). Ansible uses `ansible_connection: local`. This mirrors zerokb's deployment pattern (one playbook per module, idempotent, declarative) but without SSH. The same playbook will work unchanged if heimdall is ever moved to another host — just swap the inventory.

To deploy:

```
make deploy
```

which runs `ansible-playbook -i ansible/inventory.yml ansible/deploy-heimdall.yml --ask-become-pass`.
