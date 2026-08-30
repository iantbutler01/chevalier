#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
image_path=${OPENBRACKET_QEMU_IMAGE:?OPENBRACKET_QEMU_IMAGE is required}
receipt_port=${OPENBRACKET_RECEIPT_PORT:?OPENBRACKET_RECEIPT_PORT is required}
receipt_token=${OPENBRACKET_RECEIPT_TOKEN:?OPENBRACKET_RECEIPT_TOKEN is required}
timeout_seconds=${OPENBRACKET_WAIT_TIMEOUT_SECONDS:-7200}
deadline=$((SECONDS + timeout_seconds))
seen=0
receipt_status=$(mktemp "${TMPDIR:-/tmp}/openbracket-windows-receipt-status.XXXXXX")
rm -f "$receipt_status"

OPENBRACKET_RECEIPT_PORT="$receipt_port" \
  OPENBRACKET_RECEIPT_STATUS_FILE="$receipt_status" \
  OPENBRACKET_RECEIPT_TOKEN="$receipt_token" \
  node "$script_dir/receipt-server.mjs" &
receipt_server_pid=$!

cleanup() {
  if kill -0 "$receipt_server_pid" >/dev/null 2>&1; then
    kill "$receipt_server_pid" >/dev/null 2>&1 || true
    wait "$receipt_server_pid" >/dev/null 2>&1 || true
  fi
  rm -f "$receipt_status"
}
trap cleanup EXIT

for attempt in {1..100}; do
  if nc -z 127.0.0.1 "$receipt_port" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$receipt_server_pid" >/dev/null 2>&1; then
    echo "the authenticated Windows receipt server failed to start" >&2
    exit 1
  fi
  sleep 0.05
done
if ! nc -z 127.0.0.1 "$receipt_port" >/dev/null 2>&1; then
  echo "the authenticated Windows receipt server did not become ready" >&2
  exit 1
fi

qemu_owns_image() {
  local pid
  while IFS= read -r pid; do
    if ps -p "$pid" -o command= | grep -Fq -- "$image_path"; then
      return 0
    fi
  done < <(pgrep -f 'qemu-system-(aarch64|x86_64)' || true)
  return 1
}

verify_receipt() {
  if [[ ! -f $receipt_status ]]; then
    echo "the Windows guest stopped without a verified image receipt" >&2
    return 1
  fi
  local status
  status=$(<"$receipt_status")
  if [[ $status == failure ]]; then
    echo "the Windows guest reported an image verification failure" >&2
    return 1
  fi
  if [[ $status != verified && $status != success ]]; then
    echo "the Windows guest wrote an invalid image receipt" >&2
    return 1
  fi
}

while ((SECONDS < deadline)); do
  if qemu_owns_image; then
    seen=1
    sleep 2
    continue
  fi
  if ((seen)); then
    verify_receipt
    exit 0
  fi
  sleep 1
done

echo "timed out waiting for the Windows guest to verify, generalize, and stop" >&2
exit 1
