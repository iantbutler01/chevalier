#!/bin/sh
set -eu

if [ "$(id -u)" -ne 0 ]; then
  echo "Usage: sudo ./bootstrap-macos-vz-dev-guest.sh" >&2
  exit 77
fi
if [ "$(uname -s)" != Darwin ] || [ "$(uname -m)" != arm64 ]; then
  echo "bootstrap requires a macOS arm64 guest" >&2
  exit 69
fi
if ! id openbracket >/dev/null 2>&1; then
  echo "Setup Assistant must create the stable short-name account 'openbracket'" >&2
  exit 69
fi
if [ ! -f /var/db/.AppleSetupDone ] || \
  /usr/bin/pgrep -f '/Setup Assistant.app/|/SetupAssistant.app/' >/dev/null 2>&1; then
  echo "Setup Assistant must be fully completed before sealing guest assets" >&2
  exit 69
fi

SOURCE_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
MACFUSE_DMG="$SOURCE_DIR/macfuse-5.3.3.dmg"
MACFUSE_MOUNT="/private/tmp/chevalier-macfuse-5.3.3"

if [ ! -f "$MACFUSE_DMG" ]; then
  echo "missing development macFUSE image: $MACFUSE_DMG" >&2
  exit 66
fi

macfuse_version() {
  /usr/libexec/PlistBuddy \
    -c "Print :CFBundleShortVersionString" \
    /Library/Filesystems/macfuse.fs/Contents/version.plist \
    2>/dev/null || true
}

detach_macfuse() {
  if /sbin/mount | /usr/bin/grep -Fq " on $MACFUSE_MOUNT "; then
    /usr/bin/hdiutil detach "$MACFUSE_MOUNT" >/dev/null || true
  fi
  /bin/rmdir "$MACFUSE_MOUNT" 2>/dev/null || true
}

if [ "$(macfuse_version)" != "5.3.3" ]; then
  detach_macfuse
  /bin/mkdir -m 0700 "$MACFUSE_MOUNT"
  trap detach_macfuse EXIT INT TERM
  /usr/bin/hdiutil attach \
    -nobrowse \
    -readonly \
    -mountpoint "$MACFUSE_MOUNT" \
    "$MACFUSE_DMG" >/dev/null
  if [ ! -f "$MACFUSE_MOUNT/Install macFUSE.pkg" ]; then
    echo "macFUSE package is missing from the pinned disk image" >&2
    exit 66
  fi
  /usr/sbin/installer -pkg "$MACFUSE_MOUNT/Install macFUSE.pkg" -target /
  detach_macfuse
  trap - EXIT INT TERM
fi

if [ "$(macfuse_version)" != "5.3.3" ]; then
  echo "macFUSE 5.3.3 installation did not produce the pinned framework" >&2
  exit 69
fi

BOOTSTRAP_SECRET_DIR=$(/usr/bin/mktemp -d /private/tmp/chevalier-bootstrap.XXXXXX)
trap '/bin/rm -rf "$BOOTSTRAP_SECRET_DIR"' EXIT INT TERM
/usr/bin/openssl rand -hex 32 >"$BOOTSTRAP_SECRET_DIR/portproxy.token"
/usr/bin/openssl rand -hex 32 >"$BOOTSTRAP_SECRET_DIR/vfs.token"
/bin/chmod 0600 "$BOOTSTRAP_SECRET_DIR/portproxy.token" "$BOOTSTRAP_SECRET_DIR/vfs.token"

"$SOURCE_DIR/install-guest-assets.sh" \
  --auth-token-file "$BOOTSTRAP_SECRET_DIR/portproxy.token"
"$SOURCE_DIR/install-vfs-guest.sh" \
  --vfs-token-file "$BOOTSTRAP_SECRET_DIR/vfs.token" \
  --endpoint http://127.0.0.1:18080/internal/chevalier/vfs/macos-vz-dev \
  --scope macos-dev/workspace

/bin/rm -rf "$BOOTSTRAP_SECRET_DIR"
trap - EXIT INT TERM

echo "Development guest assets installed."
echo "If the VFS log reports that approval is required, approve macFUSE in System Settings."
echo "Enable Screen Sharing once in System Settings; leave legacy VNC access disabled."
echo "Then check: sudo launchctl print system/com.bracket.chevalier-vfs"
