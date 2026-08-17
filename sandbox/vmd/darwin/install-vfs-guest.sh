#!/bin/sh
set -eu

usage() {
  echo "Usage: sudo ./install-vfs-guest.sh --vfs-token-file <path> --endpoint <url> --scope <path>" >&2
}

if [ "$(id -u)" -ne 0 ]; then
  echo "install-vfs-guest.sh must run as root" >&2
  exit 77
fi
if [ "$(uname -s)" != Darwin ] || [ "$(uname -m)" != arm64 ]; then
  echo "install-vfs-guest.sh requires a macOS arm64 guest" >&2
  exit 69
fi
if [ "$#" -ne 6 ] || [ "$1" != "--vfs-token-file" ] || [ "$3" != "--endpoint" ] || [ "$5" != "--scope" ]; then
  usage
  exit 64
fi
if [ ! -f "$2" ]; then
  echo "VFS token file is not a regular file: $2" >&2
  exit 66
fi

TOKEN=$(tr -d '\r\n' <"$2")
ENDPOINT=$4
SCOPE=$6
case "$TOKEN" in
  ""|*[!A-Za-z0-9._~-]*)
    echo "VFS token contains unsupported characters" >&2
    exit 65
    ;;
esac
case "$ENDPOINT" in
  http://127.0.0.1:*/*) ;;
  *)
    echo "VFS endpoint must use guest loopback HTTP and include an owner path" >&2
    exit 65
    ;;
esac
ENDPOINT_REMAINDER=${ENDPOINT#http://127.0.0.1:}
ENDPOINT_PORT=${ENDPOINT_REMAINDER%%/*}
ENDPOINT_PATH=${ENDPOINT_REMAINDER#*/}
case "$ENDPOINT_PORT" in
  ""|*[!0-9]*|??????*)
    echo "VFS endpoint port must be an integer from 1 through 65535" >&2
    exit 65
    ;;
esac
if [ "$ENDPOINT_PORT" -lt 1 ] || [ "$ENDPOINT_PORT" -gt 65535 ] || [ -z "$ENDPOINT_PATH" ]; then
  echo "VFS endpoint must use a port from 1 through 65535 and include an owner path" >&2
  exit 65
fi
case "$SCOPE" in
  ""|/*|*..*|*[!A-Za-z0-9._/-]*)
    echo "VFS scope must be a non-empty relative path" >&2
    exit 65
    ;;
esac

MACFUSE_VERSION=$(
  /usr/libexec/PlistBuddy \
    -c "Print :CFBundleShortVersionString" \
    /Library/Filesystems/macfuse.fs/Contents/version.plist \
    2>/dev/null || true
)
if [ "$MACFUSE_VERSION" != "5.3.3" ]; then
  echo "macFUSE 5.3.3 must be installed and approved before installing the VFS service" >&2
  exit 69
fi

SERVICE_USER=openbracket
SERVICE_GROUP=staff
if ! id "$SERVICE_USER" >/dev/null 2>&1; then
  echo "the guest must have an openbracket account before installing the VFS service" >&2
  exit 69
fi

SOURCE_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
SOURCE_BINARY="$SOURCE_DIR/chevalier-vfs-fuse"
RUNTIME_ROOT="/Library/Application Support/Chevalier"
BIN_DIR="$RUNTIME_ROOT/bin"
ETC_DIR="$RUNTIME_ROOT/vfs-etc"
LOG_DIR="/Library/Logs/Chevalier"
LOG_FILE="$LOG_DIR/vfs.log"
DAEMON_PATH="/Library/LaunchDaemons/com.bracket.chevalier-vfs.plist"
MOUNTPOINT="/Volumes/OpenBracketWorkspace"
SHARED_ROOT="/Users/Shared/OpenBracket"
STATE_DIR="$SHARED_ROOT/vfs-state"
WORKSPACE_ALIAS="$SHARED_ROOT/workspace"

if ! codesign --verify --strict --verbose=2 "$SOURCE_BINARY"; then
  echo "VFS binary must have a valid strict code signature" >&2
  exit 65
fi
SIGNING_DETAILS=$(codesign -dv --verbose=4 "$SOURCE_BINARY" 2>&1)
case "$SIGNING_DETAILS" in
  *"TeamIdentifier="*"Runtime Version="*) ;;
  *)
    echo "VFS binary must use a team identity and hardened runtime" >&2
    exit 65
    ;;
esac
ENTITLEMENTS=$(codesign -d --entitlements - "$SOURCE_BINARY" 2>/dev/null)
case "$ENTITLEMENTS" in
  *"com.apple.security.cs.disable-library-validation"*"true"*) ;;
  *)
    echo "VFS binary must disable library validation for the macFUSE framework" >&2
    exit 65
    ;;
esac

if [ -e "$WORKSPACE_ALIAS" ] || [ -L "$WORKSPACE_ALIAS" ]; then
  if [ "$(readlink "$WORKSPACE_ALIAS" 2>/dev/null || true)" != "$MOUNTPOINT" ]; then
    echo "workspace alias already exists with a different target: $WORKSPACE_ALIAS" >&2
    exit 73
  fi
fi

umask 077
install -d -o root -g wheel -m 0755 "$RUNTIME_ROOT" "$BIN_DIR" "$LOG_DIR" "$SHARED_ROOT"
install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0700 "$ETC_DIR" "$STATE_DIR"
install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0755 "$MOUNTPOINT"
touch "$LOG_FILE"
chown "$SERVICE_USER:$SERVICE_GROUP" "$LOG_FILE"
chmod 0640 "$LOG_FILE"
install -o root -g wheel -m 0755 "$SOURCE_BINARY" "$BIN_DIR/chevalier-vfs-fuse"
codesign --verify --strict --verbose=2 "$BIN_DIR/chevalier-vfs-fuse"
install -o root -g wheel -m 0755 "$SOURCE_DIR/launch-vfs.sh" "$BIN_DIR/launch-vfs.sh"
install -o root -g wheel -m 0644 "$SOURCE_DIR/com.bracket.chevalier-vfs.plist" "$DAEMON_PATH"
printf '%s\n' "$TOKEN" >"$ETC_DIR/vfs.token"
printf '%s\n' "$ENDPOINT" >"$ETC_DIR/vfs.endpoint"
printf '%s\n' "$SCOPE" >"$ETC_DIR/vfs.scope"
chown "$SERVICE_USER:$SERVICE_GROUP" "$ETC_DIR/vfs.token" "$ETC_DIR/vfs.endpoint" "$ETC_DIR/vfs.scope"
chmod 0600 "$ETC_DIR/vfs.token" "$ETC_DIR/vfs.endpoint" "$ETC_DIR/vfs.scope"

if [ ! -L "$WORKSPACE_ALIAS" ]; then
  ln -s "$MOUNTPOINT" "$WORKSPACE_ALIAS"
fi

launchctl bootout system/com.bracket.chevalier-vfs 2>/dev/null || true
launchctl bootstrap system "$DAEMON_PATH"
launchctl kickstart -k system/com.bracket.chevalier-vfs

echo "Installed Darwin guest VFS service."
echo "Check with: launchctl print system/com.bracket.chevalier-vfs"
echo "Mountpoint: $MOUNTPOINT"
