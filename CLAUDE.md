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
- [x] System packages installed (ffmpeg, v4l-utils, alsa-utils, pipewire stack, python)
- [x] **Day-3 milestone:** end-to-end UVC path proven on `agneta`
  - Video frames flow from `/dev/video0` (Elgato → ffmpeg, NV12/YUYV @ 720p60)
  - Audio flows from ALSA `hw:0` (Elgato USB-Audio Class, 48 kHz stereo, real music levels: -12.9 dB mean / -0.4 dB peak)
  - **Constraint discovered:** a *single* ffmpeg process holding both `/dev/video0` and `hw:0` simultaneously starves the v4l2 video pipe (encodes ~0.6 fps instead of 30 fps). Two *separate* ffmpeg processes — one per input — work fine. Bug is in ffmpeg's input scheduler, not the kernel/USB layer. See `loki/docs/sessions/2026-04-08-elgato-stream-test.md`.
  - **Design implication:** heimdall's audio path and video path will run as two processes (or one process opening only one device at a time). This already matches the production design — audio streams continuously to mimir, video grabs single frames on demand from odin.
- [ ] Python module that opens the device and exposes audio + frame-grab API
- [ ] systemd unit + integration with mimir/odin

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
