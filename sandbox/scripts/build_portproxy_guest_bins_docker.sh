#!/usr/bin/env bash
# @dive-file: Builds architecture-specific guest portproxy binaries inside Docker.
# @dive-rel: Used by integration and local workflows that require guest payload binaries.
# @dive-rel: Writes outputs to portproxy/bin consumed by VM bootstrap asset resolution.

set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/common.sh"

usage() {
  cat <<'USAGE'
Usage: build_portproxy_guest_bins_docker.sh [--all] [--image <docker-image>]

Builds guest Linux portproxy binaries into portproxy/bin.

Defaults:
  - Builds only the host-matching Linux arch (arm64 on Apple Silicon, amd64 on x86_64)
  - Uses Rust docker image rust:1.90-bookworm

Flags:
  --all               Build both linux/amd64 and linux/arm64 artifacts
  --image <image>     Docker image to use (default: rust:1.90)
USAGE
}

BUILD_ALL=0
RUST_IMAGE="rust:1.90-bookworm"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --all)
      BUILD_ALL=1
      shift
      ;;
    --image)
      RUST_IMAGE="${2:-}"
      if [[ -z "$RUST_IMAGE" ]]; then
        err "--image requires a value"
        exit 2
      fi
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      err "unknown argument: $1"
      usage
      exit 2
      ;;
  esac
done

require_cmd docker

BIN_DIR="$REPO_ROOT/portproxy/bin"
CHECKOUT_ROOT="$(cd "$REPO_ROOT/.." && pwd)"
mkdir -p "$BIN_DIR"

build_target() {
  local platform="$1"
  local target="$2"
  local out_name="$3"

  log "building $out_name via docker platform=$platform target=$target"
  docker run --rm \
    --platform "$platform" \
    -v "$CHECKOUT_ROOT:/work" \
    -w /work/sandbox/portproxy \
    "$RUST_IMAGE" \
    bash -lc "export PATH=/usr/local/cargo/bin:\$PATH; cargo build --release --target $target"

  local src=""
  local candidates=(
    "$REPO_ROOT/target/$target/release/portproxy"
    "$REPO_ROOT/target/release/portproxy"
    "$REPO_ROOT/portproxy/target/$target/release/portproxy"
    "$REPO_ROOT/portproxy/target/release/portproxy"
  )
  for candidate in "${candidates[@]}"; do
    if [[ -f "$candidate" ]]; then
      src="$candidate"
      break
    fi
  done
  if [[ -z "$src" ]]; then
    err "expected built binary not found; checked:"
    for candidate in "${candidates[@]}"; do
      err "  - $candidate"
    done
    exit 1
  fi

  cp "$src" "$BIN_DIR/$out_name"
  chmod +x "$BIN_DIR/$out_name"
  log "wrote $BIN_DIR/$out_name"
}

HOST_ARCH="$(uname -m)"
if [[ "$BUILD_ALL" -eq 1 ]]; then
  build_target "linux/amd64" "x86_64-unknown-linux-gnu" "portproxy-linux-amd64"
  build_target "linux/arm64" "aarch64-unknown-linux-gnu" "portproxy-linux-arm64"
else
  case "$HOST_ARCH" in
    arm64|aarch64)
      build_target "linux/arm64" "aarch64-unknown-linux-gnu" "portproxy-linux-arm64"
      ;;
    x86_64|amd64)
      build_target "linux/amd64" "x86_64-unknown-linux-gnu" "portproxy-linux-amd64"
      ;;
    *)
      err "unsupported host arch for default mode: $HOST_ARCH"
      err "rerun with --all or add explicit target support"
      exit 1
      ;;
  esac
fi

log "guest portproxy binary build complete"
