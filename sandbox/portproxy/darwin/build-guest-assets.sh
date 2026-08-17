#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PORTPROXY_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
SANDBOX_DIR=$(CDPATH= cd -- "$PORTPROXY_DIR/.." && pwd)
BRIDGE_DIR="$PORTPROXY_DIR/darwin-vsock-bridge"
TARGET=aarch64-apple-darwin
OUT_DIR=${1:-"$PORTPROXY_DIR/bin/darwin-arm64"}
SIGN_IDENTITY=${CHEVALIER_DARWIN_GUEST_CODESIGN_IDENTITY:--}

if [ "$(uname -s)" != Darwin ] || [ "$(uname -m)" != arm64 ]; then
  echo "Darwin arm64 guest assets must be built on an Apple Silicon Mac" >&2
  exit 69
fi

cargo test --manifest-path "$PORTPROXY_DIR/Cargo.toml" --target "$TARGET"
cargo build --manifest-path "$PORTPROXY_DIR/Cargo.toml" --release --target "$TARGET"
cargo test --manifest-path "$BRIDGE_DIR/Cargo.toml" --target "$TARGET"
cargo build --manifest-path "$BRIDGE_DIR/Cargo.toml" --release --target "$TARGET"

mkdir -p "$OUT_DIR"
install -m 0755 \
  "$SANDBOX_DIR/target/$TARGET/release/portproxy" \
  "$OUT_DIR/portproxy"
install -m 0755 \
  "$BRIDGE_DIR/target/$TARGET/release/portproxy-darwin-vsock-bridge" \
  "$OUT_DIR/portproxy-darwin-vsock-bridge"
install -m 0755 "$SCRIPT_DIR/launch-portproxy.sh" "$OUT_DIR/launch-portproxy.sh"
install -m 0755 "$SCRIPT_DIR/install-guest-assets.sh" "$OUT_DIR/install-guest-assets.sh"
install -m 0644 "$SCRIPT_DIR/com.bracket.portproxy.plist" "$OUT_DIR/com.bracket.portproxy.plist"
install -m 0644 \
  "$SCRIPT_DIR/com.bracket.portproxy-vsock-bridge.plist" \
  "$OUT_DIR/com.bracket.portproxy-vsock-bridge.plist"
install -m 0644 \
  "$SCRIPT_DIR/com.bracket.vfs-vsock-bridge.plist" \
  "$OUT_DIR/com.bracket.vfs-vsock-bridge.plist"

codesign --force --options runtime --sign "$SIGN_IDENTITY" "$OUT_DIR/portproxy"
codesign --force --options runtime --sign "$SIGN_IDENTITY" \
  "$OUT_DIR/portproxy-darwin-vsock-bridge"
codesign --verify --strict --verbose=2 "$OUT_DIR/portproxy"
codesign --verify --strict --verbose=2 "$OUT_DIR/portproxy-darwin-vsock-bridge"

"$OUT_DIR/portproxy-darwin-vsock-bridge" --check-config
file "$OUT_DIR/portproxy" "$OUT_DIR/portproxy-darwin-vsock-bridge"
shasum -a 256 "$OUT_DIR/portproxy" "$OUT_DIR/portproxy-darwin-vsock-bridge"
echo "Darwin arm64 guest assets staged in $OUT_DIR"
