#!/usr/bin/env bash
set -euo pipefail

# Packer's QEMU plugin does not wait for its swtpm control socket on macOS.
sleep 1
exec /opt/homebrew/bin/qemu-system-x86_64 "$@"
