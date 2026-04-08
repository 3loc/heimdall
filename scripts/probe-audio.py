#!/usr/bin/env python3
"""Read 2 seconds of audio from a heimdall Unix socket and report.

Used by `make probe`. Stdlib only.
"""

from __future__ import annotations

import socket
import sys
import time

DEFAULT_SOCKET = "/run/heimdall/meeting.sock"


def main() -> int:
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SOCKET
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        s.connect(path)
    except FileNotFoundError:
        print(f"(no socket at {path} — is heimdall running?)")
        return 1
    except ConnectionRefusedError:
        print(f"(connection refused at {path} — heimdall not accepting)")
        return 1

    s.settimeout(0.5)
    total = 0
    end = time.time() + 2.0
    while time.time() < end:
        try:
            chunk = s.recv(4096)
        except socket.timeout:
            continue
        if not chunk:
            break
        total += len(chunk)

    samples = total // 2  # 16-bit mono
    print(
        f"{total} bytes received in 2s "
        f"({samples} samples = {samples / 16000:.2f}s of audio @ 16 kHz mono)"
    )
    if total == 0:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
