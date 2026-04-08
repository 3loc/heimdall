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

# Quick smoke test that the Elgato is visible.
probe:
	@echo "=== v4l2 devices ==="
	@v4l2-ctl --list-devices
	@echo
	@echo "=== ALSA capture devices ==="
	@arecord -l || true
	@echo
	@echo "=== USB ==="
	@lsusb | grep -i elgato || echo "no Elgato found on USB bus"
