#!/usr/bin/env bash
set -euo pipefail

VERSION=1.13.0
SHA256=dbb030d21fbbca232f865ddebc02f040649e5fd24e5a94677f52818cde5276d3
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SANDBOX_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
PATCH_FILE="$SANDBOX_DIR/patches/virtiofsd-${VERSION}-lock-forwarding.patch"
WORK_DIR=${VIRTIOFSD_BUILD_DIR:-/tmp/chevalier-virtiofsd-build}
INSTALL_ROOT=${VIRTIOFSD_INSTALL_ROOT:-/usr/local}
ARCHIVE="$WORK_DIR/virtiofsd-${VERSION}.crate"
SOURCE_DIR="$WORK_DIR/virtiofsd-${VERSION}"

rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
curl -fsSL "https://static.crates.io/crates/virtiofsd/virtiofsd-${VERSION}.crate" -o "$ARCHIVE"
echo "$SHA256  $ARCHIVE" | sha256sum --check
tar -xzf "$ARCHIVE" -C "$WORK_DIR"
patch --batch --forward --directory="$SOURCE_DIR" -p1 < "$PATCH_FILE"

# Cargo discovers workspaces by walking parent directories. Deploy builds keep
# this source under sandbox/target, so isolate the external crate from
# Chevalier's workspace before testing or installing it.
printf '\n[workspace]\n' >> "$SOURCE_DIR/Cargo.toml"

if [[ "${VIRTIOFSD_SKIP_TESTS:-0}" != "1" ]]; then
  cargo test --locked --manifest-path "$SOURCE_DIR/Cargo.toml"
fi
cargo install \
  --locked \
  --path "$SOURCE_DIR" \
  --root "$INSTALL_ROOT"

"$INSTALL_ROOT/bin/virtiofsd" --version
