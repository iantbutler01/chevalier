#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
builder_dir=$(cd -- "$script_dir/.." && pwd)
output_dir=${1:-"$builder_dir/output/windows-11-iot-enterprise-ltsc-2024-arm64"}
image="$output_dir/windows-11-iot-enterprise-ltsc-2024-arm64.qcow2"
efi_vars="$output_dir/efivars.fd"
firmware_code_source=${OPENBRACKET_ARM_EFI_CODE:-"$builder_dir/artifacts/edk2-aarch64-secure-code.fd"}
firmware_code="$output_dir/efi-code.fd"
manifest="$output_dir/image-manifest.json"

test -f "$image"
test -f "$efi_vars"
test -f "$firmware_code_source"
cp "$firmware_code_source" "$firmware_code.partial"
mv "$firmware_code.partial" "$firmware_code"
qemu-img check "$image"

image_sha256=$(shasum -a 256 "$image" | awk '{print $1}')
efi_vars_sha256=$(shasum -a 256 "$efi_vars" | awk '{print $1}')
firmware_code_sha256=$(shasum -a 256 "$firmware_code" | awk '{print $1}')
virtual_size=$(qemu-img info --output=json "$image" | jq -er '."virtual-size"')
image_size=$(stat -f '%z' "$image" 2>/dev/null || stat -c '%s' "$image")
efi_vars_size=$(stat -f '%z' "$efi_vars" 2>/dev/null || stat -c '%s' "$efi_vars")
firmware_code_size=$(stat -f '%z' "$firmware_code" 2>/dev/null || stat -c '%s' "$firmware_code")

jq -n \
  --arg profile windows-11-iot-enterprise-ltsc-2024-arm64-guest-winfsp-v2 \
  --arg imageSha256 "$image_sha256" \
  --arg efiVarsSha256 "$efi_vars_sha256" \
  --arg firmwareCodeSha256 "$firmware_code_sha256" \
  --argjson virtualSize "$virtual_size" \
  --argjson imageSize "$image_size" \
  --argjson efiVarsSize "$efi_vars_size" \
  --argjson firmwareCodeSize "$firmware_code_size" \
  '{
    schemaVersion: 1,
    status: "sealed-local-diagnostic",
    productionReady: false,
    profile: $profile,
    architecture: "arm64",
    accelerator: "hvf",
    workspaceTransport: "guest-winfsp",
    windowsIsoSha256: "3dcdba9c9c0aa0430d4332b60c9afcb3cd613d648a49cbba2d4ef7b5978f32e8",
    virtioWinVersion: "0.1.285",
    winFspVersion: "2.2.26215",
    image: {
      file: "windows-11-iot-enterprise-ltsc-2024-arm64.qcow2",
      sha256: $imageSha256,
      virtualSizeBytes: $virtualSize,
      fileSizeBytes: $imageSize
    },
    efiVars: {
      file: "efivars.fd",
      sha256: $efiVarsSha256,
      fileSizeBytes: $efiVarsSize,
      reusableForClones: false
    },
    firmwareCode: {
      file: "efi-code.fd",
      sha256: $firmwareCodeSha256,
      fileSizeBytes: $firmwareCodeSize
    },
    proved: [
      "unattended-install",
      "native-arm64",
      "hvf",
      "nvme-root",
      "virtio-net",
      "winfsp-arm64-memfs",
      "chevalier-runtime-service-payload-staged",
      "runtime-secret-free-base",
      "authenticated-guest-verification",
      "build-channel-cleanup",
      "sysprep-shutdown",
      "offline-image-check"
    ],
    notProved: [
      "first-boot-service-registration",
      "chevalier-services-live-vm",
      "guest-vfs-wal-recovery",
      "gateway-publication-drain",
      "fresh-identity-uniqueness-matrix",
      "persistent-runtime-tpm",
      "product-runtime"
    ]
  }' >"$manifest.partial"
mv "$manifest.partial" "$manifest"

(
  cd "$output_dir"
  shasum -a 256 windows-11-iot-enterprise-ltsc-2024-arm64.qcow2 efi-code.fd efivars.fd >SHA256SUMS
)
