#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PORTPROXY_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
SANDBOX_DIR=$(CDPATH= cd -- "$PORTPROXY_DIR/.." && pwd)
BRIDGE_DIR="$PORTPROXY_DIR/darwin-vsock-bridge"
TARGET=aarch64-apple-darwin
OUT_DIR=${1:-"$PORTPROXY_DIR/bin/darwin-arm64"}
SIGN_IDENTITY=${CHEVALIER_DARWIN_GUEST_CODESIGN_IDENTITY:--}
RELEASE_BUILD=${CHEVALIER_DARWIN_GUEST_RELEASE_BUILD:-0}

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
install -m 0755 "$SCRIPT_DIR/install-guest-assets.sh" "$OUT_DIR/install-guest-assets.sh"
install -m 0644 "$SCRIPT_DIR/com.bracket.portproxy.plist" "$OUT_DIR/com.bracket.portproxy.plist"
install -m 0644 \
  "$SCRIPT_DIR/com.bracket.portproxy-vsock-bridge.plist" \
  "$OUT_DIR/com.bracket.portproxy-vsock-bridge.plist"
install -m 0644 \
  "$SCRIPT_DIR/com.bracket.vfs-vsock-bridge.plist" \
  "$OUT_DIR/com.bracket.vfs-vsock-bridge.plist"
install -m 0644 \
  "$SCRIPT_DIR/com.bracket.runtime-config.plist" \
  "$OUT_DIR/com.bracket.runtime-config.plist"
install -m 0644 \
  "$SCRIPT_DIR/com.bracket.guest-ingress.plist" \
  "$OUT_DIR/com.bracket.guest-ingress.plist"

if [ "$RELEASE_BUILD" = 1 ] && [ "$SIGN_IDENTITY" = - ]; then
  echo "release guest assets require CHEVALIER_DARWIN_GUEST_CODESIGN_IDENTITY" >&2
  exit 78
fi

sign_guest_binary() {
  binary=$1
  identifier=$2
  if [ "$SIGN_IDENTITY" = - ]; then
    codesign --force --options runtime --identifier "$identifier" --sign - "$binary"
  else
    codesign \
      --force \
      --options runtime \
      --timestamp \
      --identifier "$identifier" \
      --sign "$SIGN_IDENTITY" \
      "$binary"
  fi
}

sign_guest_binary "$OUT_DIR/portproxy" com.bracket.chevalier.portproxy
sign_guest_binary \
  "$OUT_DIR/portproxy-darwin-vsock-bridge" \
  com.bracket.chevalier.portproxy-darwin-vsock-bridge
codesign --verify --strict --verbose=2 "$OUT_DIR/portproxy"
codesign --verify --strict --verbose=2 "$OUT_DIR/portproxy-darwin-vsock-bridge"

if [ "$RELEASE_BUILD" = 1 ]; then
  codesign -dv --verbose=4 "$OUT_DIR/portproxy" 2>&1 | grep -Eq '^TeamIdentifier=.+$'
  codesign -dv --verbose=4 "$OUT_DIR/portproxy-darwin-vsock-bridge" 2>&1 | \
    grep -Eq '^TeamIdentifier=.+$'
fi

"$OUT_DIR/portproxy-darwin-vsock-bridge" --check-config
file "$OUT_DIR/portproxy" "$OUT_DIR/portproxy-darwin-vsock-bridge"
shasum -a 256 "$OUT_DIR/portproxy" "$OUT_DIR/portproxy-darwin-vsock-bridge"
echo "Darwin arm64 guest assets staged in $OUT_DIR"
