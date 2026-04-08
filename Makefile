INVENTORY = ansible/inventory.yml
PLAYBOOK  = ansible/deploy-heimdall.yml

.PHONY: deploy setup check probe

# Idempotent deploy. Assumes passwordless sudo is already set up.
deploy:
	ansible-playbook -i $(INVENTORY) $(PLAYBOOK)

# First-time setup — prompts for sudo password.
setup:
	ansible-playbook -i $(INVENTORY) $(PLAYBOOK) --ask-become-pass

# Dry run.
check:
	ansible-playbook -i $(INVENTORY) $(PLAYBOOK) --check --diff

# Quick smoke test that the Elgato is visible AND the meeting daemon
# is healthy. Run after `make deploy`.
probe:
	@echo "=== v4l2 devices ==="
	@v4l2-ctl --list-devices
	@echo
	@echo "=== ALSA capture devices ==="
	@arecord -l || true
	@echo
	@echo "=== USB ==="
	@lsusb | grep -i elgato || echo "no Elgato found on USB bus"
	@echo
	@echo "=== heimdall@meeting.service ==="
	@systemctl is-active heimdall@meeting.service || true
	@systemctl --no-pager --lines=10 status heimdall@meeting.service 2>&1 | head -20 || true
	@echo
	@echo "=== HTTP /healthz ==="
	@curl -s -o - http://127.0.0.1:7100/healthz || echo "(no response)"
	@echo
	@echo "=== HTTP /info ==="
	@curl -s http://127.0.0.1:7100/info || echo "(no response)"
	@echo
	@echo "=== HTTP /frame.png (saved to /tmp/heimdall-probe.png) ==="
	@curl -s -o /tmp/heimdall-probe.png -w "HTTP %{http_code}, %{size_download} bytes\n" \
		http://127.0.0.1:7100/frame.png && file /tmp/heimdall-probe.png || true
	@echo
	@echo "=== Audio fanout (2 s from /run/heimdall/meeting.sock) ==="
	@python3 scripts/probe-audio.py /run/heimdall/meeting.sock
