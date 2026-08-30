#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
image_dir=$(cd -- "$script_dir/.." && pwd)
module_dir=$(cd -- "$image_dir/../winfsp-vfs" && pwd)
artifact_dir="$image_dir/artifacts"
build_cache=$(mktemp -d "${TMPDIR:-/tmp}/openbracket-windows-services.XXXXXX")
trap 'rm -rf "$build_cache"' EXIT

mkdir -p "$artifact_dir"
cd "$module_dir"
for architecture in arm64 amd64; do
  GOWORK=off GOCACHE="$build_cache" CGO_ENABLED=0 GOOS=windows GOARCH="$architecture" \
    go build -mod=vendor -trimpath \
      -o "$artifact_dir/chevalier-vfs-winfsp-$architecture.exe" \
      ./cmd/chevalier-vfs-winfsp
  GOWORK=off GOCACHE="$build_cache" CGO_ENABLED=0 GOOS=windows GOARCH="$architecture" \
    go build -mod=vendor -trimpath \
      -o "$artifact_dir/chevalier-guest-agent-$architecture.exe" \
      ./cmd/chevalier-guest-agent
done

cd "$artifact_dir"
if command -v sha256sum >/dev/null 2>&1; then
  sha256sum \
    chevalier-vfs-winfsp-arm64.exe \
    chevalier-guest-agent-arm64.exe \
    chevalier-vfs-winfsp-amd64.exe \
    chevalier-guest-agent-amd64.exe \
    >chevalier-guest-services.SHA256SUMS
else
  shasum -a 256 \
    chevalier-vfs-winfsp-arm64.exe \
    chevalier-guest-agent-arm64.exe \
    chevalier-vfs-winfsp-amd64.exe \
    chevalier-guest-agent-amd64.exe \
    >chevalier-guest-services.SHA256SUMS
fi
