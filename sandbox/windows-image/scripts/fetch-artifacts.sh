#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
artifact_dir=$(cd -- "$script_dir/.." && pwd)/artifacts
mkdir -p "$artifact_dir"

firmware_code=${OPENBRACKET_ARM_EFI_CODE:-/Applications/UTM.app/Contents/Resources/qemu/edk2-aarch64-secure-code.fd}
firmware_vars=${OPENBRACKET_ARM_EFI_VARS:-/Applications/UTM.app/Contents/Resources/qemu/edk2-arm-secure-vars.fd}

checksum_file() {
  local algorithm=$1
  local path=$2

  if command -v "${algorithm}sum" >/dev/null 2>&1; then
    "${algorithm}sum" "$path" | awk '{print tolower($1)}'
  else
    shasum -a "${algorithm#sha}" "$path" | awk '{print tolower($1)}'
  fi
}

download_verified() {
  local algorithm=$1
  local url=$2
  local destination=$3
  local expected=$4
  local temporary="${destination}.partial"

  if [[ -f "$destination" ]] && [[ "$(checksum_file "$algorithm" "$destination")" == "$expected" ]]; then
    return
  fi

  rm -f "$temporary"
  curl --fail --location --proto '=https' --tlsv1.2 --output "$temporary" "$url"
  local actual
  actual=$(checksum_file "$algorithm" "$temporary")
  if [[ "$actual" != "$expected" ]]; then
    rm -f "$temporary"
    echo "$algorithm mismatch for $url: expected $expected, got $actual" >&2
    exit 1
  fi
  mv "$temporary" "$destination"
}

require_checksum() {
  local path=$1
  local expected=$2

  if [[ ! -f "$path" ]]; then
    echo "required artifact is missing: $path" >&2
    exit 1
  fi
  local actual
  actual=$(checksum_file sha256 "$path")
  if [[ "$actual" != "$expected" ]]; then
    echo "sha256 mismatch for $path: expected $expected, got $actual" >&2
    exit 1
  fi
}

require_checksum \
  "$firmware_code" \
  "c85a57de1ac39e550a6529bd66a4214eb1d8c14dcda7e22dedf72566a769fbc7"
require_checksum \
  "$firmware_vars" \
  "e16c802acef4b55ad6a9a21908ba0c3fdec59abec7b9cecb084a8e9261c75f06"

firmware_code_copy="$artifact_dir/edk2-aarch64-secure-code.fd"
if [[ ! -f "$firmware_code_copy" ]] || \
  [[ "$(checksum_file sha256 "$firmware_code_copy")" != "c85a57de1ac39e550a6529bd66a4214eb1d8c14dcda7e22dedf72566a769fbc7" ]]; then
  cp "$firmware_code" "$firmware_code_copy"
fi
require_checksum \
  "$firmware_code_copy" \
  "c85a57de1ac39e550a6529bd66a4214eb1d8c14dcda7e22dedf72566a769fbc7"

firmware_vars_64m="$artifact_dir/edk2-arm-secure-vars-64m.fd"
if [[ ! -f "$firmware_vars_64m" ]] || \
  [[ "$(checksum_file sha256 "$firmware_vars_64m")" != "d1cb54a9a527243eac1373559bbfcd0c627ae629079b39b45633f183e1cab9f9" ]]; then
  cp "$firmware_vars" "$firmware_vars_64m"
  truncate -s 67108864 "$firmware_vars_64m"
  require_checksum \
    "$firmware_vars_64m" \
    "d1cb54a9a527243eac1373559bbfcd0c627ae629079b39b45633f183e1cab9f9"
fi

download_verified \
  sha256 \
  "https://github.com/winfsp/winfsp/releases/download/v2.2B4/winfsp-2.2.26215.msi" \
  "$artifact_dir/winfsp-2.2.26215.msi" \
  "2ecb5c89405488a95bbd8a01875e02c48534fd37bbdfd84488f7590464d65944"

download_verified \
  sha256 \
  "https://github.com/PowerShell/PowerShell/releases/download/v7.6.5/PowerShell-7.6.5-win-arm64.msi" \
  "$artifact_dir/PowerShell-7.6.5-win-arm64.msi" \
  "a1633b48b8e45c7767902efc972bff235b3594c192ad575af4a4e8eb2ea3bd5a"

download_verified \
  sha256 \
  "https://aka.ms/vc14/vc_redist.arm64.exe" \
  "$artifact_dir/vc_redist.arm64-14.51.36247.exe" \
  "b70ef586669a620a0a30a1156969c05c6a3831dc8f8bc992da75779d2a92f944"

download_verified \
  sha256 \
  "https://github.com/BurntSushi/ripgrep/releases/download/15.2.0/ripgrep-15.2.0-aarch64-pc-windows-msvc.zip" \
  "$artifact_dir/ripgrep-15.2.0-aarch64-pc-windows-msvc.zip" \
  "e4abca10c3a64ebea742667dd7009449d49403db5460dd6873e389fa2945360f"

download_verified \
  sha256 \
  "https://github.com/git-for-windows/git/releases/download/v2.55.0.windows.4/Git-2.55.0.4-64-bit.exe" \
  "$artifact_dir/Git-2.55.0.4-64-bit.exe" \
  "0cbc0b34a74b3aff3ace0910328549155a770e228331b19cb1498218a120e7ff"

download_verified \
  sha256 \
  "https://fedorapeople.org/groups/virt/virtio-win/direct-downloads/archive-virtio/virtio-win-0.1.285-1/virtio-win-guest-tools.exe" \
  "$artifact_dir/virtio-win-guest-tools-0.1.285.exe" \
  "c8b4a9fe87e1fc5d8e843495e082dea53420587fe04740b1084d85089343f04d"

virtio_iso="$artifact_dir/virtio-win-0.1.285.iso"
download_verified \
  sha512 \
  "https://fedorapeople.org/groups/virt/virtio-win/direct-downloads/archive-virtio/virtio-win-0.1.285-1/virtio-win-0.1.285.iso" \
  "$virtio_iso" \
  "4f13070cc9241fa342deab4ebfac360565030580ff77b6e5f1951a64627621e5da4abfd30e1e46ca8bae2bb7dd4ff98141aff424142c9629a5876a61283962e5"

driver_root="$artifact_dir/answer-cd/\$WinPEDriver\$"
display_driver_root="$artifact_dir/viogpudo/w11/ARM64"
if [[ ! -f "$driver_root/viostor/w11/ARM64/viostor.inf" || ! -f "$driver_root/NetKVM/w11/ARM64/netkvm.inf" || ! -f "$display_driver_root/viogpudo.inf" ]]; then
  if ! command -v 7z >/dev/null 2>&1; then
    echo "7z is required to extract the boot-critical virtio-win driver" >&2
    exit 1
  fi
  driver_temp=$(mktemp -d "$artifact_dir/.winpe-drivers.XXXXXX")
  7z x -y -o"$driver_temp" "$virtio_iso" "viostor/w11/ARM64/*" "NetKVM/w11/ARM64/*" "viogpudo/w11/ARM64/*" >/dev/null
  rm -rf "$driver_root"
  rm -rf "$artifact_dir/viogpudo"
  mkdir -p "$driver_root"
  mv "$driver_temp/viostor" "$driver_temp/NetKVM" "$driver_root/"
  mv "$driver_temp/viogpudo" "$artifact_dir/"
  rmdir "$driver_temp"
fi
