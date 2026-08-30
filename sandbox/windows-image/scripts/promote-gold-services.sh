#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
image_dir=$(cd -- "$script_dir/.." && pwd)
gold=${OPENBRACKET_WINDOWS_GOLD:-"$image_dir/output/windows-11-iot-enterprise-ltsc-2024-arm64/windows-11-iot-enterprise-ltsc-2024-arm64.qcow2"}
output=${1:-"$image_dir/output/windows-11-iot-enterprise-ltsc-2024-arm64-services"}
firmware_code=${OPENBRACKET_ARM_EFI_CODE:-"$image_dir/artifacts/edk2-aarch64-secure-code.fd"}
firmware_vars=${OPENBRACKET_ARM_EFI_VARS:-"$image_dir/artifacts/edk2-arm-secure-vars-64m.fd"}
windows_iso=${OPENBRACKET_WINDOWS_INSTALL_ISO:-"$image_dir/artifacts/windows-11-iot-enterprise-ltsc-2024-arm64-eval.iso"}
test_root=$(mktemp -d /private/tmp/openbracket-windows-services.XXXXXX)
qemu_pid=
success=0

cleanup() {
  if [[ -n ${qemu_pid:-} ]] && kill -0 "$qemu_pid" >/dev/null 2>&1; then
    kill "$qemu_pid" >/dev/null 2>&1 || true
    wait "$qemu_pid" >/dev/null 2>&1 || true
  fi
  if [[ $success == 1 ]]; then
    rm -rf "$test_root"
  else
    printf 'Windows service promotion diagnostics retained at %s\n' "$test_root" >&2
  fi
}
trap cleanup EXIT INT TERM

for required in "$gold" "$firmware_code" "$firmware_vars" "$windows_iso"; do
  test -f "$required"
done
test ! -e "$output"

"$script_dir/build-guest-services.sh"
media="$test_root/media"
mkdir -m 700 "$media"
cp "$image_dir/artifacts/chevalier-vfs-winfsp-arm64.exe" "$media/"
cp "$image_dir/artifacts/chevalier-guest-agent-arm64.exe" "$media/"
cp "$image_dir/artifacts/chevalier-guest-services.SHA256SUMS" "$media/"
cp "$script_dir/initialize-state.ps1" "$media/"
cp "$script_dir/install-runtime-services.ps1" "$media/"
cp "$script_dir/SetupComplete-services.cmd" "$media/"
cp "$script_dir/unattend-runtime-arm64.xml" "$media/Unattend-runtime.xml"
cp "$script_dir/stage-runtime-services.cmd" "$media/"
cp "$script_dir/Autounattend-stage-services.xml" "$media/Autounattend.xml"
vfs_hash=$(awk '$2 == "chevalier-vfs-winfsp-arm64.exe" { print $1 }' "$image_dir/artifacts/chevalier-guest-services.SHA256SUMS")
guest_hash=$(awk '$2 == "chevalier-guest-agent-arm64.exe" { print $1 }' "$image_dir/artifacts/chevalier-guest-services.SHA256SUMS")
test -n "$vfs_hash"
test -n "$guest_hash"
jq -n \
  --arg vfsHash "$vfs_hash" \
  --arg guestHash "$guest_hash" \
  '{
    schemaVersion: 1,
    profile: "windows-11-iot-enterprise-ltsc-2024-arm64-guest-winfsp-v2",
    platform: "windows",
    architecture: "arm64",
    workspaceTransport: "guest-winfsp",
    workspaceMount: "W:",
    runtimeServices: {
      registration: "first-boot-setup-complete",
      containsRuntimeSecrets: false,
      vfsSha256: $vfsHash,
      guestControlSha256: $guestHash
    },
    virtioWinVersion: "0.1.285",
    winFspVersion: "2.2.26215",
    powerShellVersion: "7.6.5",
    ripgrepVersion: "15.2.0",
    gitVersion: "2.55.0.windows.4"
  }' >"$media/image-manifest.json"
hdiutil makehybrid -quiet -iso -joliet -default-volume-name OBRUNTIME -o "$test_root/answer.iso" "$media"
qemu-img create -q -f qcow2 -F qcow2 -b "$gold" "$test_root/root.qcow2"
cp "$firmware_vars" "$test_root/efivars.fd"

qemu-system-aarch64 \
  -machine virt-10.2,highmem=off,accel=hvf \
  -cpu host -smp 4 -m 3072 \
  -drive "if=pflash,format=raw,readonly=on,file=$firmware_code" \
  -drive "if=pflash,format=raw,file=$test_root/efivars.fd" \
  -drive "if=none,id=root,format=qcow2,file=$test_root/root.qcow2,discard=unmap,detect-zeroes=unmap" \
  -device nvme,drive=root,serial=openbracket-services,bootindex=2 \
  -drive "if=none,id=install,format=raw,media=cdrom,readonly=on,file=$windows_iso" \
  -drive "if=none,id=answer,format=raw,media=cdrom,readonly=on,file=$test_root/answer.iso" \
  -device qemu-xhci \
  -device usb-storage,drive=install,bootindex=0 \
  -device usb-storage,drive=answer,bootindex=1 \
  -device usb-kbd -device usb-tablet \
  -device ramfb -display none \
  -serial "file:$test_root/serial.log" \
  -qmp "unix:$test_root/qmp.sock,server=on,wait=off" \
  >"$test_root/qemu.log" 2>&1 &
qemu_pid=$!

for _ in {1..100}; do
  [[ -S $test_root/qmp.sock ]] && break
  kill -0 "$qemu_pid" >/dev/null 2>&1 || break
  sleep 0.1
done
test -S "$test_root/qmp.sock"
sleep 2
{
  printf '%s\n' '{"execute":"qmp_capabilities"}'
  for _ in {1..16}; do
    printf '%s\n' '{"execute":"send-key","arguments":{"keys":[{"type":"qcode","data":"spc"}]}}'
    sleep 0.5
  done
} | nc -U -w 1 "$test_root/qmp.sock" >/dev/null || true

deadline=$((SECONDS + 600))
while kill -0 "$qemu_pid" >/dev/null 2>&1 && ((SECONDS < deadline)); do
  sleep 1
done
if kill -0 "$qemu_pid" >/dev/null 2>&1; then
  echo "WinPE did not finish staging the Windows runtime services" >&2
  exit 1
fi
wait "$qemu_pid"
qemu_pid=

mkdir "$output"
qemu-img convert -p -f qcow2 -O qcow2 -o compression_type=zlib "$test_root/root.qcow2" "$output/windows-11-iot-enterprise-ltsc-2024-arm64.qcow2"
cp "$test_root/efivars.fd" "$output/efivars.fd"
"$script_dir/seal-output.sh" "$output"
success=1
