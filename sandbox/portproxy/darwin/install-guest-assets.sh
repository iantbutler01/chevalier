#!/bin/sh
set -eu

usage() {
  echo "Usage: sudo ./install-guest-assets.sh --auth-token-file <path>" >&2
}

if [ "$(id -u)" -ne 0 ]; then
  echo "install-guest-assets.sh must run as root" >&2
  exit 77
fi
if [ "$(uname -s)" != Darwin ] || [ "$(uname -m)" != arm64 ]; then
  echo "install-guest-assets.sh requires a macOS arm64 guest" >&2
  exit 69
fi
if [ "$#" -ne 2 ] || [ "$1" != "--auth-token-file" ]; then
  usage
  exit 64
fi
if [ ! -f "$2" ]; then
  echo "auth token file is not a regular file: $2" >&2
  exit 66
fi

AUTH_TOKEN=$(tr -d '\r\n' <"$2")
case "$AUTH_TOKEN" in
  ""|*[!A-Za-z0-9._~-]*)
    echo "auth token must use only A-Z, a-z, 0-9, dot, underscore, tilde, or hyphen" >&2
    exit 65
    ;;
esac

SOURCE_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
RUNTIME_ROOT="/Library/Application Support/Chevalier"
BIN_DIR="$RUNTIME_ROOT/bin"
ETC_DIR="$RUNTIME_ROOT/etc"
LOG_DIR="/Library/Logs/Chevalier"
DAEMON_DIR="/Library/LaunchDaemons"
SERVICE_USER="openbracket"

require_identifier() {
  binary=$1
  identifier=$2
  details=$(codesign -dv --verbose=4 "$binary" 2>&1)
  printf '%s\n' "$details" | grep -Fxq "Identifier=$identifier" || {
    echo "unexpected code-signing identifier for $binary" >&2
    exit 69
  }
  codesign --verify --strict --verbose=2 "$binary"
}

bootstrap_service() {
  plist=$1
  attempts=0
  until launchctl bootstrap system "$plist"; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 20 ]; then
      return 1
    fi
    sleep 0.25
  done
}

if ! /usr/bin/id "$SERVICE_USER" >/dev/null 2>&1; then
  echo "the guest must have an $SERVICE_USER account before installing control services" >&2
  exit 67
fi
require_identifier "$SOURCE_DIR/portproxy" com.bracket.chevalier.portproxy
require_identifier \
  "$SOURCE_DIR/portproxy-darwin-vsock-bridge" \
  com.bracket.chevalier.portproxy-darwin-vsock-bridge
umask 077
install -d -o root -g wheel -m 0755 "$RUNTIME_ROOT" "$BIN_DIR" "$LOG_DIR"
install -d -o root -g wheel -m 0700 "$ETC_DIR"
touch "$LOG_DIR/portproxy.log"
chown root:wheel "$LOG_DIR/portproxy.log"
chmod 0644 "$LOG_DIR/portproxy.log"
install -o root -g wheel -m 0755 "$SOURCE_DIR/portproxy" "$BIN_DIR/portproxy"
install -o root -g wheel -m 0755 \
  "$SOURCE_DIR/portproxy-darwin-vsock-bridge" \
  "$BIN_DIR/portproxy-darwin-vsock-bridge"
printf '%s\n' "$AUTH_TOKEN" >"$ETC_DIR/portproxy.token"
chown root:wheel "$ETC_DIR/portproxy.token"
chmod 0400 "$ETC_DIR/portproxy.token"
install -o root -g wheel -m 0644 \
  "$SOURCE_DIR/com.bracket.portproxy.plist" \
  "$DAEMON_DIR/com.bracket.portproxy.plist"
install -o root -g wheel -m 0644 \
  "$SOURCE_DIR/com.bracket.portproxy-vsock-bridge.plist" \
  "$DAEMON_DIR/com.bracket.portproxy-vsock-bridge.plist"
install -o root -g wheel -m 0644 \
  "$SOURCE_DIR/com.bracket.vfs-vsock-bridge.plist" \
  "$DAEMON_DIR/com.bracket.vfs-vsock-bridge.plist"
install -o root -g wheel -m 0644 \
  "$SOURCE_DIR/com.bracket.runtime-config.plist" \
  "$DAEMON_DIR/com.bracket.runtime-config.plist"
install -o root -g wheel -m 0644 \
  "$SOURCE_DIR/com.bracket.guest-ingress.plist" \
  "$DAEMON_DIR/com.bracket.guest-ingress.plist"

launchctl bootout system/com.bracket.guest-ingress 2>/dev/null || true
launchctl bootout system/com.bracket.runtime-config 2>/dev/null || true
launchctl bootout system/com.bracket.vfs-vsock-bridge 2>/dev/null || true
launchctl bootout system/com.bracket.portproxy-vsock-bridge 2>/dev/null || true
launchctl bootout system/com.bracket.portproxy 2>/dev/null || true
bootstrap_service "$DAEMON_DIR/com.bracket.portproxy.plist"
bootstrap_service "$DAEMON_DIR/com.bracket.portproxy-vsock-bridge.plist"
bootstrap_service "$DAEMON_DIR/com.bracket.vfs-vsock-bridge.plist"
bootstrap_service "$DAEMON_DIR/com.bracket.runtime-config.plist"
bootstrap_service "$DAEMON_DIR/com.bracket.guest-ingress.plist"
launchctl kickstart -k system/com.bracket.portproxy
launchctl kickstart -k system/com.bracket.portproxy-vsock-bridge
launchctl kickstart -k system/com.bracket.vfs-vsock-bridge
launchctl kickstart -k system/com.bracket.runtime-config
launchctl kickstart -k system/com.bracket.guest-ingress
/usr/bin/pmset -a displaysleep 0 sleep 0 disksleep 0 powernap 0

rm -f "$ETC_DIR/portproxy.env" "$BIN_DIR/launch-portproxy.sh"

echo "Installed Darwin guest control services."
echo "Check with: launchctl print system/com.bracket.portproxy"
echo "Check with: launchctl print system/com.bracket.portproxy-vsock-bridge"
echo "Check with: launchctl print system/com.bracket.vfs-vsock-bridge"
echo "Check with: launchctl print system/com.bracket.runtime-config"
echo "Check with: launchctl print system/com.bracket.guest-ingress"
