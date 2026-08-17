#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
artifact_dir=$(cd -- "$script_dir/.." && pwd)/artifacts
mkdir -p "$artifact_dir"

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

download_verified \
  sha256 \
  "https://github.com/winfsp/winfsp/releases/download/v2.2B3/winfsp-2.2.26194.msi" \
  "$artifact_dir/winfsp-2.2.26194.msi" \
  "7b41020618cdcc33d699d0e15c1df660f0762a09b57080049c565857ac00bd9d"

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
if [[ ! -f "$driver_root/viostor/w11/amd64/viostor.inf" ]]; then
  if ! command -v 7z >/dev/null 2>&1; then
    echo "7z is required to extract the boot-critical virtio-win driver" >&2
    exit 1
  fi
  driver_temp=$(mktemp -d "$artifact_dir/.winpe-drivers.XXXXXX")
  7z x -y -o"$driver_temp" "$virtio_iso" "viostor/w11/amd64/*" >/dev/null
  rm -rf "$driver_root"
  mkdir -p "$driver_root"
  mv "$driver_temp/viostor" "$driver_root/"
  rmdir "$driver_temp"
fi
