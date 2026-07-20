#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
guest="$script_dir/vfs-git-ref-stress-guest.sh"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/chevalier-git-ref-stress.XXXXXX")"
export GIT_REF_STRESS_ROOT="$scratch/root"
pids=()
cleanup_pids=()

cleanup() {
  local status="$1"
  local control="$GIT_REF_STRESS_ROOT/control"
  local ready
  set +e
  if [[ -d "$control" ]]; then
    for ready in "$control"/lock-*.ready; do
      [[ -e "$ready" ]] || continue
      printf 'release\n' >"${ready%.ready}.release"
    done
    printf 'start\n' >"$control/workload-local-storm.start"
    printf 'ready\n' >"$control/feed-ready"
  fi
  for pid in "${cleanup_pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null
    fi
  done
  for pid in "${cleanup_pids[@]}"; do
    wait "$pid" 2>/dev/null
  done
  rm -rf "$scratch"
  trap - EXIT
  exit "$status"
}
trap 'cleanup $?' EXIT

run() {
  bash "$guest" "$@"
}

run setup

for kind in index packed-refs ref; do
  for round in 1 2; do
    holder_log="$scratch/holder-$kind-$round.log"
    run lock-holder "$kind" "$round" >"$holder_log" 2>&1 &
    holder_pid=$!
    cleanup_pids+=("$holder_pid")
    run wait-lock "$kind" "$round"
    run contend-lock "$kind" "$round"
    run release-lock "$kind" "$round"
    if ! wait "$holder_pid"; then
      cat "$holder_log" >&2
      exit 1
    fi
    run recover-lock "$kind" "$round"
  done
done

labels=()
start_background() {
  local label="$1"
  local pid
  shift
  labels+=("$label")
  run "$@" >"$scratch/$label.log" 2>&1 &
  pid=$!
  pids+=("$pid")
  cleanup_pids+=("$pid")
}

writer_count=6
feed_count=6
ref_count=30
maintenance_barrier="local-storm"

start_background writer-a writer a "$writer_count" "$maintenance_barrier"
start_background writer-b writer b "$writer_count" "$maintenance_barrier"
start_background publisher publisher "$feed_count" "$maintenance_barrier"
start_background integrator integrator 24 "$maintenance_barrier"
start_background reader-a reader 40 "$maintenance_barrier" reader-a
start_background reader-b reader 40 "$maintenance_barrier" reader-b
start_background refs-a ref-churn a "$ref_count" "$maintenance_barrier"
start_background refs-b ref-churn b "$ref_count" "$maintenance_barrier"
start_background gc maintenance gc 6 "$maintenance_barrier"
start_background repack maintenance repack 6 "$maintenance_barrier"
start_background commit-graph maintenance commit-graph 8 "$maintenance_barrier"
start_background midx maintenance midx 8 "$maintenance_barrier"

run wait-workload-barrier "$maintenance_barrier" 12
run release-workload-barrier "$maintenance_barrier"

failed=0
for index in "${!pids[@]}"; do
  if ! wait "${pids[$index]}"; then
    printf 'background workload failed: %s\n' "${labels[$index]}" >&2
    cat "$scratch/${labels[$index]}.log" >&2
    failed=1
  fi
done
if [[ "$failed" -ne 0 ]]; then
  exit 1
fi

if ! grep -Eq \
  '^MAINTENANCE_REJECTED kind=gc round=[0-9]+ exit=128 reason=native-gc-repack-pack-race stdout=' \
  "$scratch/gc.log"; then
  printf 'maintenance storm produced no exact native gc/repack rejection\n' >&2
  cat "$scratch/gc.log" >&2
  cat "$scratch/repack.log" >&2
  exit 1
fi
for label in "${labels[@]}"; do
  if ! grep -q \
    "^WORKLOAD_BARRIER_RELEASED barrier=$maintenance_barrier participant=$label$" \
    "$scratch/$label.log"; then
    printf 'workload omitted barrier-release receipt: %s\n' "$label" >&2
    cat "$scratch/$label.log" >&2
    exit 1
  fi
done

run finalize "$writer_count" "$feed_count" "$ref_count"
if ! head="$(git -C "$GIT_REF_STRESS_ROOT/repo" rev-parse HEAD)"; then
  printf 'failed to read local final HEAD\n' >&2
  exit 1
fi
if ! snapshot_a="$(run snapshot "$head")"; then
  printf 'first local snapshot failed\n' >&2
  exit 1
fi
if ! snapshot_b="$(run snapshot "$head")"; then
  printf 'second local snapshot failed\n' >&2
  exit 1
fi
if [[ "$snapshot_a" != "$snapshot_b" ]]; then
  printf 'local snapshots diverged\nA:\n%s\nB:\n%s\n' \
    "$snapshot_a" "$snapshot_b" >&2
  exit 1
fi
if ! grep -q '^SNAPSHOT_OK$' <<<"$snapshot_a"; then
  printf 'local snapshot omitted SNAPSHOT_OK\n%s\n' "$snapshot_a" >&2
  exit 1
fi

printf 'LOCAL_GIT_REF_STRESS_OK head=%s\n%s\n' "$head" "$snapshot_a"
