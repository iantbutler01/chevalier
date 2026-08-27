#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
module_dir=$(cd -- "$script_dir/.." && pwd)
repo_root=$(cd -- "$module_dir/../.." && pwd)
image_dir="$repo_root/sandbox/windows-image"
gold="$image_dir/output/windows-11-iot-enterprise-ltsc-2024-arm64/windows-11-iot-enterprise-ltsc-2024-arm64.qcow2"
firmware_code="$image_dir/artifacts/edk2-aarch64-secure-code.fd"
firmware_vars="$image_dir/artifacts/edk2-arm-secure-vars-64m.fd"
windows_iso="$image_dir/artifacts/windows-11-iot-enterprise-ltsc-2024-arm64-eval.iso"
windows_iso_sha256="3dcdba9c9c0aa0430d4332b60c9afcb3cd613d648a49cbba2d4ef7b5978f32e8"

for required in "$gold" "$firmware_code" "$firmware_vars" "$windows_iso"; do
  test -f "$required" || { echo "missing required artifact: $required" >&2; exit 1; }
done
test "$(shasum -a 256 "$windows_iso" | awk '{print tolower($1)}')" = "$windows_iso_sha256" || {
  echo "Windows ARM64 ISO checksum mismatch" >&2
  exit 1
}

test_root=$(mktemp -d /private/tmp/openbracket-winfsp-e2e.XXXXXX)
media="$test_root/media"
mkdir -m 700 "$media" "$test_root/storage"
status_file="$test_root/result.json"
host_log="$test_root/host.log"
qemu_log="$test_root/qemu.log"
serial_log="$test_root/serial.log"
stage_qemu_log="$test_root/stage-qemu.log"
stage_serial_log="$test_root/stage-serial.log"
gateway_token=$(openssl rand -hex 32)
result_token=$(openssl rand -hex 32)
owner="windows-arm-e2e"
scope="runs/$(openssl rand -hex 8)/workspace"
gateway_port=63349
result_port=18089
qemu_pid=
host_pid=
success=0

cleanup() {
  if [[ -n ${qemu_pid:-} ]] && kill -0 "$qemu_pid" >/dev/null 2>&1; then
    kill "$qemu_pid" >/dev/null 2>&1 || true
    wait "$qemu_pid" >/dev/null 2>&1 || true
  fi
  if [[ -n ${host_pid:-} ]] && kill -0 "$host_pid" >/dev/null 2>&1; then
    kill "$host_pid" >/dev/null 2>&1 || true
    wait "$host_pid" >/dev/null 2>&1 || true
  fi
  if [[ $success == 1 ]]; then
    rm -rf "$test_root"
  else
    printf 'WinFsp E2E diagnostics retained at %s\n' "$test_root" >&2
  fi
}
trap cleanup EXIT INT TERM

GOCACHE=/private/tmp/openbracket-go-cache CGO_ENABLED=0 GOOS=windows GOARCH=arm64 \
  go build -trimpath -o "$media/chevalier-vfs-winfsp.exe" ./cmd/chevalier-vfs-winfsp
cp "$script_dir/run-e2e.ps1" "$script_dir/stage-e2e.cmd" "$media/"
cp "$script_dir/Autounattend-winpe.xml" "$media/Autounattend.xml"
cp "$script_dir/unattend-specialize.xml" "$media/"
jq -n \
  --arg gatewayEndpoint "http://10.0.2.2:${gateway_port}/internal/chevalier/vfs/${owner}" \
  --arg gatewayToken "$gateway_token" \
  --arg scope "$scope" \
  --arg resultUrl "http://10.0.2.2:${result_port}/phase" \
  --arg resultToken "$result_token" \
  '{gatewayEndpoint:$gatewayEndpoint,gatewayToken:$gatewayToken,scope:$scope,resultUrl:$resultUrl,resultToken:$resultToken}' \
  >"$media/e2e-config.json"
hdiutil makehybrid -quiet -iso -joliet -default-volume-name OBVFS-E2E -o "$test_root/answer.iso" "$media"
qemu-img create -q -f qcow2 -F qcow2 -b "$gold" "$test_root/root.qcow2"
cp "$firmware_vars" "$test_root/efivars.fd"

qemu-system-aarch64 \
  -machine virt-10.2,highmem=off,accel=hvf \
  -cpu host -smp 4 -m 3072 \
  -drive "if=pflash,format=raw,readonly=on,file=$firmware_code" \
  -drive "if=pflash,format=raw,file=$test_root/efivars.fd" \
  -drive "if=none,id=root,format=qcow2,file=$test_root/root.qcow2,discard=unmap,detect-zeroes=unmap" \
  -device nvme,drive=root,serial=openbracket-e2e,bootindex=2 \
  -drive "if=none,id=install,format=raw,media=cdrom,readonly=on,file=$windows_iso" \
  -drive "if=none,id=answer,format=raw,media=cdrom,readonly=on,file=$test_root/answer.iso" \
  -device qemu-xhci \
  -device usb-storage,drive=install,bootindex=0 \
  -device usb-storage,drive=answer,bootindex=1 \
  -device usb-kbd -device usb-tablet \
  -device ramfb \
  -display none \
  -serial "file:$stage_serial_log" \
  -qmp "unix:$test_root/stage-qmp.sock,server=on,wait=off" \
  >"$stage_qemu_log" 2>&1 &
qemu_pid=$!

for _ in {1..100}; do
  [[ -S $test_root/stage-qmp.sock ]] && break
  kill -0 "$qemu_pid" >/dev/null 2>&1 || break
  sleep 0.1
done
if [[ -S $test_root/stage-qmp.sock ]]; then
  sleep 2
  {
    printf '%s\n' '{"execute":"qmp_capabilities"}'
    for _ in {1..16}; do
      printf '%s\n' '{"execute":"send-key","arguments":{"keys":[{"type":"qcode","data":"spc"}]}}'
      sleep 0.5
    done
  } | nc -U -w 1 "$test_root/stage-qmp.sock" >/dev/null || true
fi

stage_deadline=$((SECONDS + 600))
while kill -0 "$qemu_pid" >/dev/null 2>&1 && ((SECONDS < stage_deadline)); do
  sleep 1
done
if kill -0 "$qemu_pid" >/dev/null 2>&1; then
  echo "WinPE did not finish staging the WinFsp E2E payload" >&2
  exit 1
fi
wait "$qemu_pid"
qemu_pid=
rm -f "$test_root/stage-qmp.sock"

OPENBRACKET_WINFSP_GATEWAY_TOKEN="$gateway_token" \
OPENBRACKET_WINFSP_RESULT_TOKEN="$result_token" \
OPENBRACKET_WINFSP_STORAGE_ROOT="$test_root/storage" \
OPENBRACKET_WINFSP_STATUS_FILE="$status_file" \
OPENBRACKET_WINFSP_OWNER="$owner" \
OPENBRACKET_WINFSP_SCOPE="$scope" \
OPENBRACKET_WINFSP_GATEWAY_PORT="$gateway_port" \
OPENBRACKET_WINFSP_RESULT_PORT="$result_port" \
  node "$script_dir/host.mjs" >"$host_log" 2>&1 &
host_pid=$!
for _ in {1..100}; do
  nc -z 127.0.0.1 "$gateway_port" >/dev/null 2>&1 && nc -z 127.0.0.1 "$result_port" >/dev/null 2>&1 && break
  kill -0 "$host_pid" >/dev/null 2>&1 || { cat "$host_log" >&2; exit 1; }
  sleep 0.1
done
nc -z 127.0.0.1 "$gateway_port"
nc -z 127.0.0.1 "$result_port"

qemu-system-aarch64 \
  -machine virt-10.2,highmem=off,accel=hvf \
  -cpu host -smp 4 -m 3072 \
  -drive "if=pflash,format=raw,readonly=on,file=$firmware_code" \
  -drive "if=pflash,format=raw,file=$test_root/efivars.fd" \
  -drive "if=none,id=root,format=qcow2,file=$test_root/root.qcow2,discard=unmap,detect-zeroes=unmap" \
  -device nvme,drive=root,serial=openbracket-e2e,bootindex=1 \
  -device qemu-xhci \
  -device usb-kbd -device usb-tablet \
  -netdev user,id=net0 \
  -device virtio-net-pci,netdev=net0 \
  -device ramfb \
  -display none \
  -serial "file:$serial_log" \
  -qmp "unix:$test_root/qmp.sock,server=on,wait=off" \
  >"$qemu_log" 2>&1 &
qemu_pid=$!

deadline=$((SECONDS + 1800))
while ((SECONDS < deadline)); do
  if [[ -f $status_file ]]; then
    phase=$(jq -r '.phase // empty' "$status_file")
    if [[ $phase == success ]]; then
      break
    fi
    if [[ $phase == failure ]]; then
      cat "$status_file" >&2
      exit 1
    fi
  fi
  if ! kill -0 "$qemu_pid" >/dev/null 2>&1; then
    wait "$qemu_pid" || true
    cat "$host_log" >&2
    cat "$qemu_log" >&2
    exit 1
  fi
  sleep 2
done
test -f "$status_file"
test "$(jq -r .phase "$status_file")" = success

for _ in {1..30}; do
  kill -0 "$qemu_pid" >/dev/null 2>&1 || break
  sleep 1
done
if kill -0 "$qemu_pid" >/dev/null 2>&1; then
  {
    printf '%s\n' '{"execute":"qmp_capabilities"}'
    printf '%s\n' '{"execute":"system_powerdown"}'
  } | nc -U -w 2 "$test_root/qmp.sock" >/dev/null || true
  for _ in {1..60}; do
    kill -0 "$qemu_pid" >/dev/null 2>&1 || break
    sleep 1
  done
fi
if kill -0 "$qemu_pid" >/dev/null 2>&1; then
  {
    printf '%s\n' '{"execute":"qmp_capabilities"}'
    printf '%s\n' '{"execute":"quit"}'
  } | nc -U -w 2 "$test_root/qmp.sock" >/dev/null || true
  for _ in {1..30}; do
    kill -0 "$qemu_pid" >/dev/null 2>&1 || break
    sleep 1
  done
fi
if kill -0 "$qemu_pid" >/dev/null 2>&1; then
  kill "$qemu_pid"
fi
wait "$qemu_pid" || true
qemu_pid=
kill "$host_pid"
wait "$host_pid"
host_pid=

jq -e '.phase == "success" and .details.status.pending_events == 0 and .details.status.acknowledged_sequence == .details.status.last_committed_sequence' "$status_file" >/dev/null
test "$(cat "$test_root/storage/$scope/online.txt")" = "online-published"
test "$(cat "$test_root/storage/$scope/renamed.txt")" = "renamed-online"
test "$(cat "$test_root/storage/$scope/offline-renamed.txt")" = "offline-durable"
test ! -e "$test_root/storage/$scope/delete-me.txt"
test -d "$test_root/storage/$scope/repo/.git"

cat "$status_file"
success=1
