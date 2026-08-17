#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
  echo "Usage: CHEVALIER_VFS_CODESIGN_IDENTITY=<identity> ./sign-vfs-guest.sh <binary>" >&2
  exit 64
fi
if [ -z "${CHEVALIER_VFS_CODESIGN_IDENTITY:-}" ]; then
  echo "CHEVALIER_VFS_CODESIGN_IDENTITY is required" >&2
  exit 64
fi

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
BINARY=$1
if [ ! -f "$BINARY" ]; then
  echo "VFS binary is not a regular file: $BINARY" >&2
  exit 66
fi

codesign \
  --force \
  --options runtime \
  --entitlements "$SCRIPT_DIR/ChevalierVFS.entitlements" \
  --sign "$CHEVALIER_VFS_CODESIGN_IDENTITY" \
  "$BINARY"
codesign --verify --strict --verbose=2 "$BINARY"
