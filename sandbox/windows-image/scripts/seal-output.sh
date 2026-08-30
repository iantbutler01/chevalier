#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
builder_dir=$(cd -- "$script_dir/.." && pwd)
architecture=${OPENBRACKET_WINDOWS_ARCHITECTURE:-arm64}
case "$architecture" in
  arm64)
    default_output="$builder_dir/output/windows-11-iot-enterprise-ltsc-2024-arm64"
    image_name=windows-11-iot-enterprise-ltsc-2024-arm64.qcow2
    profile=windows-11-iot-enterprise-ltsc-2024-arm64-guest-winfsp-v2
    accelerator=hvf
    firmware_code_source=${OPENBRACKET_ARM_EFI_CODE:-"$builder_dir/artifacts/edk2-aarch64-secure-code.fd"}
    windows_iso_sha256=3dcdba9c9c0aa0430d4332b60c9afcb3cd613d648a49cbba2d4ef7b5978f32e8
    architecture_proofs='["native-arm64", "hvf", "nvme-root", "virtio-net"]'
    ;;
  amd64)
    default_output="$builder_dir/output/windows-11-enterprise-25h2-amd64"
    image_name=windows-11-enterprise-25h2-amd64.qcow2
    profile=windows-11-enterprise-25h2-amd64-guest-winfsp-v2
    accelerator=kvm
    firmware_code_source=${OPENBRACKET_AMD64_EFI_CODE:-"$builder_dir/artifacts/edk2-x86_64-secure-code.fd"}
    windows_iso_sha256=a61adeab895ef5a4db436e0a7011c92a2ff17bb0357f58b13bbc4062e535e7b9
    architecture_proofs='["native-amd64", "kvm", "virtio-blk-root", "virtio-net"]'
    ;;
  *)
    echo "OPENBRACKET_WINDOWS_ARCHITECTURE must be arm64 or amd64" >&2
    exit 1
    ;;
esac
output_dir=${1:-"$default_output"}
image="$output_dir/$image_name"
efi_vars="$output_dir/efivars.fd"
firmware_code="$output_dir/efi-code.fd"
manifest="$output_dir/image-manifest.json"
service_checksums="$builder_dir/artifacts/chevalier-guest-services.SHA256SUMS"

test -f "$image"
test -f "$efi_vars"
test -f "$firmware_code_source"
test -f "$service_checksums"
cp "$firmware_code_source" "$firmware_code.partial"
mv "$firmware_code.partial" "$firmware_code"
qemu-img check "$image"

hash_sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

file_size() {
  if [[ $(uname -s) == Darwin ]]; then
    stat -f '%z' "$1"
  else
    stat -c '%s' "$1"
  fi
}

image_sha256=$(hash_sha256 "$image")
efi_vars_sha256=$(hash_sha256 "$efi_vars")
firmware_code_sha256=$(hash_sha256 "$firmware_code")
virtual_size=$(qemu-img info --output=json "$image" | jq -er '."virtual-size"')
image_size=$(file_size "$image")
efi_vars_size=$(file_size "$efi_vars")
firmware_code_size=$(file_size "$firmware_code")
vfs_sha256=$(awk -v name="chevalier-vfs-winfsp-$architecture.exe" '$2 == name { print $1 }' "$service_checksums")
guest_control_sha256=$(awk -v name="chevalier-guest-agent-$architecture.exe" '$2 == name { print $1 }' "$service_checksums")
test -n "$vfs_sha256"
test -n "$guest_control_sha256"

jq -n \
  --arg profile "$profile" \
  --arg architecture "$architecture" \
  --arg accelerator "$accelerator" \
  --arg imageName "$image_name" \
  --arg windowsIsoSha256 "$windows_iso_sha256" \
  --argjson architectureProofs "$architecture_proofs" \
  --arg imageSha256 "$image_sha256" \
  --arg efiVarsSha256 "$efi_vars_sha256" \
  --arg firmwareCodeSha256 "$firmware_code_sha256" \
  --arg vfsSha256 "$vfs_sha256" \
  --arg guestControlSha256 "$guest_control_sha256" \
  --argjson virtualSize "$virtual_size" \
  --argjson imageSize "$image_size" \
  --argjson efiVarsSize "$efi_vars_size" \
  --argjson firmwareCodeSize "$firmware_code_size" \
  '{
    schemaVersion: 1,
    status: "sealed-local-diagnostic",
    productionReady: false,
    profile: $profile,
    architecture: $architecture,
    accelerator: $accelerator,
    workspaceTransport: "guest-winfsp",
    runtimeServices: {
      registration: "sysprep-first-boot-setup-complete",
      containsRuntimeSecrets: false,
      vfsSha256: $vfsSha256,
      guestControlSha256: $guestControlSha256
    },
    windowsIsoSha256: $windowsIsoSha256,
    virtioWinVersion: "0.1.285",
    winFspVersion: "2.2.26215",
    image: {
      file: $imageName,
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
    proved: ($architectureProofs + [
      "unattended-install",
      "winfsp-native-memfs",
      "chevalier-runtime-service-payload-staged",
      "runtime-secret-free-base",
      "authenticated-guest-verification",
      "build-channel-cleanup",
      "sysprep-shutdown",
      "offline-image-check"
    ]),
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
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$image_name" efi-code.fd efivars.fd >SHA256SUMS
  else
    shasum -a 256 "$image_name" efi-code.fd efivars.fd >SHA256SUMS
  fi
)
