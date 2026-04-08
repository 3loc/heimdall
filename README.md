# heimdall

HDMI capture module for [loki](https://github.com/3loc/loki). Opens the Elgato Game Capture Neo as a UVC device and exposes audio + on-demand video frames to the rest of the stack.

Named after the Norse watchman of the gods. He sees and hears everything across the nine realms — same job, smaller scale.

## What it does

- Opens `/dev/video0` (Elgato Neo, UVC) once
- Streams audio continuously to consumers (loki's transcribe module, "mimir")
- Holds a rolling buffer of recent video frames in memory
- On request, hands the most recent frame as lossless PNG to the orchestrator ("odin")

No transcription, no OCR, no ML. Thin capture layer.

## Hardware

Designed to run on a Minisforum UM790 Pro with an Elgato Neo plugged in. Should work on any Linux box with a UVC-compliant HDMI capture device.

## Install

Requires Arch Linux (or another distro with adjustments to the playbook's package manager).

```
make deploy
```

This runs the ansible playbook in `ansible/deploy-heimdall.yml` against the local machine and installs everything heimdall needs (ffmpeg, v4l-utils, alsa-utils, python, ansible itself, group memberships).

## Status

Scaffolding only. No working capture code yet — see `CLAUDE.md`.

## License

TBD
