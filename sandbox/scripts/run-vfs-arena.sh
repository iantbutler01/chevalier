#!/usr/bin/env bash
#
# run-vfs-arena.sh -- run the mounted perf + correctness suites against the
# ARENA stack (a second vmd on bismuth and a second API on corvidae, both built
# from the working tree by OpenBracket scripts/deploy-arena.sh).
#
# This exists so the wiring cannot be got wrong by hand. Pointing the suites at
# the production endpoints does not fail — it silently measures the DEPLOYED
# build, so a change under test reads as having done nothing. That happened
# once; vfs-perf-arena.mjs now refuses the production ports outright, and this
# script is the correct way in.
#
# The endpoints and credentials come from the arena deployment itself rather
# than from anything typed here: the tokens are read out of the arena vmd's own
# 0600 token file over ssh, so this script holds no secret and cannot drift from
# what the arena is actually running.
#
# USAGE: sandbox/scripts/run-vfs-arena.sh [extra env assignments...]
#   VFS_STD_SMALL_COUNT=64 VFS_STD_LARGE_MIB=64 sandbox/scripts/run-vfs-arena.sh
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)

BISMUTH=${ARENA_BISMUTH:-crow@bismuth}
BISMUTH_IP=${ARENA_BISMUTH_IP:-100.118.49.55}
CORVIDAE_IP=${ARENA_CORVIDAE_IP:-100.66.99.67}
ARENA_VMD_PORT=${ARENA_VMD_PORT:-18072}
ARENA_API_PORT=${ARENA_API_PORT:-8931}
ARENA_TOKENS=${ARENA_TOKENS:-/home/crow/.openbracket-vmd-arena/tokens.env}

read_token() {
  ssh -o BatchMode=yes "$BISMUTH" "sed -n 's/^$1=//p' '$ARENA_TOKENS'" | tr -d '\r\n'
}

AUTH_TOKEN=$(read_token CHEVALIER_SANDBOX_AUTH_TOKEN)
VFS_TOKEN=$(read_token CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN)
[[ ${#AUTH_TOKEN} -eq 64 && ${#VFS_TOKEN} -eq 64 ]] || {
  echo "arena credentials unavailable -- is the arena deployed? (scripts/deploy-arena.sh)" >&2
  exit 2
}

# Liveness before fixtures: a suite that starts against a dead arena spends
# minutes staging a 1 GiB file before it discovers there is nothing to talk to.
ssh -o BatchMode=yes "$BISMUTH" "curl -fsS --max-time 3 -o /dev/null http://127.0.0.1:${ARENA_VMD_PORT}/ 2>/dev/null || nc -z 127.0.0.1 ${ARENA_VMD_PORT}" >/dev/null 2>&1 || {
  echo "arena vmd is not listening on ${BISMUTH_IP}:${ARENA_VMD_PORT}" >&2
  exit 2
}
curl -fsS --max-time 5 "http://${CORVIDAE_IP}:${ARENA_API_PORT}/health" >/dev/null || {
  echo "arena API is not healthy on ${CORVIDAE_IP}:${ARENA_API_PORT}" >&2
  exit 2
}

# The guest image is a read-only registry artifact, shared with production the
# way the kernel and the docker daemon are. Default to the newest durable-state
# tag rather than pinning a stale one by hand.
SANDBOX_IMAGE=${SANDBOX_IMAGE:-$(
  ssh -o BatchMode=yes "$BISMUTH" \
    "curl -fsS http://127.0.0.1:5000/v2/openbracket-sandbox-dind/tags/list" \
    | python3 -c "
import json, sys
tags = [t for t in (json.load(sys.stdin).get('tags') or []) if 'durable-state' in t]
print('127.0.0.1:5000/openbracket-sandbox-dind:' + sorted(tags)[-1] if tags else '')
"
)}
[[ -n "$SANDBOX_IMAGE" ]] || { echo "no durable-state sandbox image found in the registry" >&2; exit 2; }

echo "arena vmd     ${BISMUTH_IP}:${ARENA_VMD_PORT}"
echo "arena gateway ${CORVIDAE_IP}:${ARENA_API_PORT}"
echo "guest image   ${SANDBOX_IMAGE}"
echo

exec env \
  SANDBOX_ENDPOINT="http://${BISMUTH_IP}:${ARENA_VMD_PORT}" \
  SANDBOX_AUTH_TOKEN="$AUTH_TOKEN" \
  CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN="$VFS_TOKEN" \
  VFS_GATEWAY_URL="http://${CORVIDAE_IP}:${ARENA_API_PORT}" \
  SANDBOX_IMAGE="$SANDBOX_IMAGE" \
  BRACKET_VM_IMAGE="$SANDBOX_IMAGE" \
  node "$SCRIPT_DIR/vfs-perf-arena.mjs" "$@"
