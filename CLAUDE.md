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
- [x] System packages installed (ffmpeg, v4l-utils, alsa-utils, python)
- [ ] Prove `/dev/video0` produces video frames via ffmpeg
- [ ] Prove audio capture path (currently `arecord -l` shows no soundcards — needs investigation; UVC audio may need to be exposed via ALSA differently or pulled directly via ffmpeg's v4l2/alsa input)
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
