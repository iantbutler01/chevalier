#!/usr/bin/env bash
#
# run-vfs-full-battery.sh -- every VFS gate, in one run.
#
# Unit suites alone cannot catch what breaks the mount: they use fake stores and
# a quiet namespace. The harnesses below drive the real
# gateway -> vmd -> virtiofsd -> guest path, and the torture/stress ones are the
# only things that exercise sustained churn. All of them have to pass before a
# substantial change ships.
#
# Every stage runs even if an earlier one fails; the summary at the end is the
# verdict. Exit is non-zero if ANY stage failed.
#
# USAGE: sandbox/scripts/run-vfs-full-battery.sh [stage ...]
#   with no arguments, runs every stage.

set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
LOG_DIR=${VFS_BATTERY_LOG_DIR:-${TMPDIR:-/tmp}/vfs-battery-$$}
mkdir -p "$LOG_DIR"

: "${SANDBOX_ENDPOINT:?SANDBOX_ENDPOINT is required}"
: "${SANDBOX_IMAGE:?SANDBOX_IMAGE is required}"
: "${SANDBOX_AUTH_TOKEN:?SANDBOX_AUTH_TOKEN is required}"
: "${GATEWAY_PUBLIC_HOST:?GATEWAY_PUBLIC_HOST is required (an address the vmd host can reach back on)}"

export SANDBOX_ENDPOINT SANDBOX_IMAGE SANDBOX_AUTH_TOKEN
export CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN=${CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN:-$SANDBOX_AUTH_TOKEN}
export CHEVALIER_MODULE_PATH=${CHEVALIER_MODULE_PATH:-$REPO_ROOT/ts/index.js}
export CHEVALIER_SANDBOX_MODULE_PATH=${CHEVALIER_SANDBOX_MODULE_PATH:-$REPO_ROOT/ts-sandbox/index.js}

# Each harness binds its own callback listener; distinct ports so stages cannot
# collide if one leaks a listener on failure.
export CHEVALIER_VFS_HARNESS_GATEWAY_PORT=19091
export CHEVALIER_VFS_GIT_MATRIX_GATEWAY_PORT=19092
export CHEVALIER_VFS_FAULT_GATEWAY_PORT=19093
export CHEVALIER_VFS_LIFECYCLE_GATEWAY_PORT=19094
export CHEVALIER_VFS_GIT_REF_STRESS_GATEWAY_PORT=19095
export CHEVALIER_VFS_HARNESS_GATEWAY_PUBLIC_URL="http://${GATEWAY_PUBLIC_HOST}:19091"
export CHEVALIER_VFS_GIT_MATRIX_GATEWAY_PUBLIC_URL="http://${GATEWAY_PUBLIC_HOST}:19092"
export CHEVALIER_VFS_FAULT_GATEWAY_PUBLIC_URL="http://${GATEWAY_PUBLIC_HOST}:19093"
export CHEVALIER_VFS_LIFECYCLE_GATEWAY_PUBLIC_URL="http://${GATEWAY_PUBLIC_HOST}:19094"
export CHEVALIER_VFS_GIT_REF_STRESS_GATEWAY_PUBLIC_URL="http://${GATEWAY_PUBLIC_HOST}:19095"

STAGE_NAMES=()
STAGE_STATUS=()
STAGE_SECONDS=()

run_stage() {
  local name=$1
  shift
  if [ "$#" -eq 0 ]; then return; fi
  if [ "${#SELECTED[@]}" -gt 0 ]; then
    local wanted=0
    for pick in "${SELECTED[@]}"; do [ "$pick" = "$name" ] && wanted=1; done
    [ "$wanted" -eq 1 ] || return
  fi
  local log="$LOG_DIR/$name.log"
  printf '\033[1;36m[battery]\033[0m %-22s running... (log: %s)\n' "$name" "$log"
  local started=$SECONDS
  ( "$@" ) >"$log" 2>&1
  local status=$?
  local elapsed=$((SECONDS - started))
  STAGE_NAMES+=("$name")
  STAGE_STATUS+=("$status")
  STAGE_SECONDS+=("$elapsed")
  if [ "$status" -eq 0 ]; then
    printf '\033[1;32m[battery]\033[0m %-22s PASS (%ss)\n' "$name" "$elapsed"
  else
    printf '\033[1;31m[battery]\033[0m %-22s FAIL rc=%s (%ss)\n' "$name" "$status" "$elapsed"
    tail -25 "$log" | sed 's/^/    | /'
  fi
}

SELECTED=("$@")

# --- Stage 1: every Rust crate, not just vmd -------------------------------
run_stage rust-vfs bash -c "cd '$REPO_ROOT/vfs' && cargo test --offline"
run_stage rust-vmd bash -c "cd '$REPO_ROOT/sandbox' && cargo test -p vmd --offline"
run_stage rust-hash bash -c "cd '$REPO_ROOT/hash' && cargo test --offline"

# --- Stage 2: TS suites (builds first; the artifacts are committed and lag) --
run_stage ts-suite bash -c "cd '$REPO_ROOT/ts' && npm test"

# --- Stage 3: model torture that needs no VM --------------------------------
run_stage posix-model bash -c "cd '$REPO_ROOT' && node sandbox/scripts/posix-model-torture.test.mjs"

# --- Stage 4: gateway wire protocol, no VM ----------------------------------
run_stage gateway-probe "$SCRIPT_DIR/run-vfs-gateway-protocol-probe.sh"

# --- Stage 5: the mounted path. Long, and the only real coverage ------------
run_stage git-conformance "$SCRIPT_DIR/run-vfs-virtiofs-git-conformance.sh"
run_stage git-matrix "$SCRIPT_DIR/run-vfs-virtiofs-git-matrix.sh"
run_stage git-ref-stress "$SCRIPT_DIR/run-vfs-virtiofs-git-ref-stress.sh"
run_stage fault-recovery "$SCRIPT_DIR/run-vfs-virtiofs-fault-recovery.sh"

# --- Stage 6: latency on the REAL mount ------------------------------------
# The only stage that can observe a slow filesystem. Everything above runs
# in-process against fakes and stays green while the mount is unusable.
run_stage perf-budget bash -c "cd '$REPO_ROOT' && node sandbox/scripts/vfs-mounted-perf-budget.mjs"

printf '\n\033[1m=== VFS battery summary ===\033[0m\n'
failed=0
for index in "${!STAGE_NAMES[@]}"; do
  status=${STAGE_STATUS[$index]}
  if [ "$status" -eq 0 ]; then
    printf '  \033[1;32mPASS\033[0m  %-22s %ss\n' "${STAGE_NAMES[$index]}" "${STAGE_SECONDS[$index]}"
  else
    printf '  \033[1;31mFAIL\033[0m  %-22s %ss  rc=%s\n' "${STAGE_NAMES[$index]}" "${STAGE_SECONDS[$index]}" "$status"
    failed=$((failed + 1))
  fi
done
printf 'logs: %s\n' "$LOG_DIR"
[ "$failed" -eq 0 ] || printf '\033[1;31m%s stage(s) failed\033[0m\n' "$failed"
exit $((failed > 0))
