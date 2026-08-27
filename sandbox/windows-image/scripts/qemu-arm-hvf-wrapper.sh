#!/usr/bin/env bash
set -euo pipefail

qemu_binary=${QEMU_SYSTEM_AARCH64_BIN:-/opt/homebrew/bin/qemu-system-aarch64}
serial_log=${OPENBRACKET_QEMU_SERIAL_LOG:-/private/tmp/openbracket-windows-arm64-serial.log}
rewritten=()
cdrom_devices=()
system_devices=()
cdrom_index=0
system_disk_index=0
needs_xhci=0

while (($#)); do
  if [[ $1 == "-drive" && $# -ge 2 && ",$2," == *,media=cdrom,* ]]; then
    IFS=',' read -r -a drive_parts <<<"$2"
    filtered_parts=()
    for part in "${drive_parts[@]}"; do
      case "$part" in
        if=* | id=* | index=*) ;;
        *) filtered_parts+=("$part") ;;
      esac
    done
    drive_id="openbracket-cdrom-${cdrom_index}"
    filtered_parts+=("if=none" "id=${drive_id}")
    drive_spec=$(IFS=,; printf '%s' "${filtered_parts[*]}")
    rewritten+=("-drive" "$drive_spec")
    if ((cdrom_index == 0)); then
      cdrom_boot_index=0
    else
      cdrom_boot_index=$((cdrom_index + 1))
    fi
    cdrom_devices+=("-device" "usb-storage,drive=${drive_id},bootindex=${cdrom_boot_index}")
    cdrom_index=$((cdrom_index + 1))
    needs_xhci=1
    shift 2
    continue
  fi
  if [[ $1 == "-drive" && $# -ge 2 && ",$2," == *,if=virtio,* ]]; then
    IFS=',' read -r -a drive_parts <<<"$2"
    filtered_parts=()
    for part in "${drive_parts[@]}"; do
      case "$part" in
        if=* | id=* | index=*) ;;
        *) filtered_parts+=("$part") ;;
      esac
    done
    drive_id="openbracket-system-${system_disk_index}"
    filtered_parts+=("if=none" "id=${drive_id}")
    drive_spec=$(IFS=,; printf '%s' "${filtered_parts[*]}")
    rewritten+=("-drive" "$drive_spec")
    system_devices+=("-device" "nvme,drive=${drive_id},serial=${drive_id},bootindex=$((system_disk_index + 1))")
    system_disk_index=$((system_disk_index + 1))
    shift 2
    continue
  fi
  rewritten+=("$1")
  shift
done

rewritten+=("${system_devices[@]}")
if ((needs_xhci)); then
  rewritten+=("-device" "qemu-xhci")
  rewritten+=("${cdrom_devices[@]}")
  rewritten+=("-device" "usb-kbd" "-device" "usb-tablet")
fi
rewritten+=("-device" "ramfb")
rewritten+=("-serial" "file:${serial_log}")

if [[ ${OPENBRACKET_QEMU_WRAPPER_PRINT_ARGS:-0} == 1 ]]; then
  printf '%q\n' "${rewritten[@]}"
  exit 0
fi

exec "$qemu_binary" "${rewritten[@]}"
