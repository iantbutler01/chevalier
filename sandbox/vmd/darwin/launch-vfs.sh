#!/bin/sh
set -eu

RUNTIME_ROOT="/Library/Application Support/Chevalier"
TOKEN_FILE="$RUNTIME_ROOT/vfs-etc/vfs.token"
ENDPOINT_FILE="$RUNTIME_ROOT/vfs-etc/vfs.endpoint"
SCOPE_FILE="$RUNTIME_ROOT/vfs-etc/vfs.scope"
MOUNTPOINT="/Volumes/OpenBracketWorkspace"
STATE_DIR="/Users/Shared/OpenBracket/vfs-state"

if [ "$(id -u)" -eq 0 ]; then
  mount_wait_seconds=0
  while /sbin/mount | /usr/bin/grep -F " on $MOUNTPOINT (" >/dev/null; do
    if [ "$mount_wait_seconds" -ge 60 ]; then
      echo "previous FSKit mount did not detach within 60s: $MOUNTPOINT" >&2
      exit 75
    fi
    /bin/sleep 1
    mount_wait_seconds=$((mount_wait_seconds + 1))
  done
  install -d -o openbracket -g staff -m 0755 "$MOUNTPOINT"
  /usr/bin/sudo -H -u openbracket -- "$0" --as-openbracket &
  child_pid=$!
  terminate() {
    /sbin/umount "$MOUNTPOINT" 2>/dev/null || true
    kill -USR1 "$child_pid" 2>/dev/null || true
  }
  trap terminate TERM INT HUP
  child_status=0
  while kill -0 "$child_pid" 2>/dev/null; do
    if wait "$child_pid"; then
      child_status=0
    else
      child_status=$?
    fi
  done
  exit "$child_status"
fi
if [ "$#" -ne 1 ] || [ "$1" != "--as-openbracket" ]; then
  echo "launch-vfs.sh must be started by its root LaunchDaemon" >&2
  exit 77
fi

CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN=$(tr -d '\r\n' <"$TOKEN_FILE")
ENDPOINT=$(tr -d '\r\n' <"$ENDPOINT_FILE")
SCOPE=$(tr -d '\r\n' <"$SCOPE_FILE")
export CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN
export CHEVALIER_VFS_LAUNCHD_SUPERVISED=1
export CHEVALIER_VFS_HASH_ALGORITHM=blake3

exec "$RUNTIME_ROOT/bin/chevalier-vfs-fuse" \
  --endpoint "$ENDPOINT" \
  --scope "$SCOPE" \
  --tag OpenBracketWorkspace \
  --state-dir "$STATE_DIR" \
  "$MOUNTPOINT"
