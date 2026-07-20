#!/usr/bin/env bash
set -euo pipefail

export GIT_AUTHOR_NAME="Chevalier Git Ref Stress"
export GIT_AUTHOR_EMAIL="git-ref-stress@chevalier.test"
export GIT_COMMITTER_NAME="$GIT_AUTHOR_NAME"
export GIT_COMMITTER_EMAIL="$GIT_AUTHOR_EMAIL"
export GIT_TERMINAL_PROMPT=0

root="${GIT_REF_STRESS_ROOT:-/workspace/git-ref-stress}"
repo="$root/repo"
origin="$root/origin.git"
publisher="$root/publisher"
writers="$root/writers"
control="$root/control"

gitc() {
  git -C "$repo" "$@"
}

configure_repo() {
  local target="$1"
  git -C "$target" config user.name "$GIT_AUTHOR_NAME"
  git -C "$target" config user.email "$GIT_AUTHOR_EMAIL"
  git -C "$target" config core.fsync committed
  git -C "$target" config gc.auto 0
}

lock_path() {
  case "$1" in
    index) printf '%s\n' "$repo/.git/index.lock" ;;
    packed-refs) printf '%s\n' "$repo/.git/packed-refs.lock" ;;
    ref) printf '%s\n' "$repo/.git/refs/heads/lock-target.lock" ;;
    *)
      printf 'unknown lock kind: %s\n' "$1" >&2
      return 2
      ;;
  esac
}

marker_path() {
  local kind="$1"
  local round="$2"
  local marker="$3"
  printf '%s\n' "$control/lock-${kind}-${round}.${marker}"
}

wait_for_path() {
  local path="$1"
  local expected="$2"
  local attempt
  for attempt in $(seq 1 1200); do
    if [[ "$expected" == "present" && -e "$path" ]]; then
      return 0
    fi
    if [[ "$expected" == "absent" && ! -e "$path" ]]; then
      return 0
    fi
    sleep 0.05
  done
  printf 'timed out waiting for %s to become %s\n' "$path" "$expected" >&2
  return 1
}

join_workload_barrier() {
  local barrier="$1"
  local participant="$2"
  if [[ -z "$barrier" ]]; then
    return 0
  fi
  printf 'ready\n' >"$control/workload-${barrier}-${participant}.ready"
  wait_for_path "$control/workload-${barrier}.start" present
  printf 'WORKLOAD_BARRIER_RELEASED barrier=%s participant=%s\n' \
    "$barrier" "$participant"
}

expect_lock_failure() {
  local label="$1"
  shift
  local stdout_path="$control/lock-receipt-${label}-$$.stdout"
  local stderr_path="$control/lock-receipt-${label}-$$.stderr"
  local stdout
  local stderr
  local status
  set +e
  "$@" >"$stdout_path" 2>"$stderr_path"
  status=$?
  set -e
  stdout="$(<"$stdout_path")"
  stderr="$(<"$stderr_path")"
  rm -f "$stdout_path" "$stderr_path"
  if [[ "$status" -eq 0 ]]; then
    printf '%s unexpectedly succeeded\nstdout:\n%s\nstderr:\n%s\n' \
      "$label" "$stdout" "$stderr" >&2
    return 1
  fi
  if ! grep -Eqi \
    '(\.lock|unable to create|cannot lock|could not lock|another git process|file exists)' \
    <<<"$stderr"; then
    printf '%s failed without an honest lock diagnostic (exit=%s)\nstdout:\n%s\nstderr:\n%s\n' \
      "$label" "$status" "$stdout" "$stderr" >&2
    return 1
  fi
  printf 'LOCK_REJECTED label=%s exit=%s stdout=%q stderr=%q\n' \
    "$label" "$status" "$stdout" "$stderr"
}

maintenance_command() {
  local kind="$1"
  case "$kind" in
    gc) gitc gc --prune=never ;;
    repack) gitc repack -ad ;;
    commit-graph) gitc commit-graph write --reachable --changed-paths ;;
    midx) gitc multi-pack-index write --bitmap ;;
    *)
      printf 'unknown maintenance kind: %s\n' "$kind" >&2
      return 2
      ;;
  esac
}

is_native_gc_repack_pack_race() {
  local kind="$1"
  local status="$2"
  local output="$3"
  local line_count
  local missing_pack_line
  local terminal_line
  local pack_hash
  local prefix_count
  local unexpected_prefix

  [[ "$kind" == "gc" ]] || return 1
  [[ "$status" -eq 128 ]] || return 1
  line_count="$(printf '%s\n' "$output" | wc -l | tr -d ' ')"
  [[ "$line_count" -ge 2 ]] || return 1
  missing_pack_line="$(printf '%s\n' "$output" | tail -n 2 | head -n 1)"
  terminal_line="$(printf '%s\n' "$output" | tail -n 1)"
  pack_hash="$(
    printf '%s\n' "$missing_pack_line" |
      sed -nE "s/^fatal: could not find pack 'pack-([0-9a-f]{40}|[0-9a-f]{64})\\.pack'$/\\1/p"
  )"
  [[ -n "$pack_hash" ]] || return 1
  [[ "$terminal_line" == "fatal: failed to run repack" ]] || return 1

  prefix_count=$((line_count - 2))
  if [[ "$prefix_count" -gt 0 ]]; then
    unexpected_prefix="$(
      printf '%s\n' "$output" |
        head -n "$prefix_count" |
        grep -Ev \
          "^error: packfile \\.git/objects/pack/pack-${pack_hash}\\.pack index unavailable$" ||
        true
    )"
    [[ -z "$unexpected_prefix" ]] || return 1
  fi
}

run_maintenance_rounds() {
  local kind="$1"
  local rounds="$2"
  local successes=0
  local pack_race_rejections=0
  local stdout_path
  local stderr_path
  local stdout
  local stderr
  local status
  local round
  for round in $(seq 1 "$rounds"); do
    stdout_path="$control/maintenance-${kind}-${round}-$$.stdout"
    stderr_path="$control/maintenance-${kind}-${round}-$$.stderr"
    set +e
    maintenance_command "$kind" >"$stdout_path" 2>"$stderr_path"
    status=$?
    set -e
    stdout="$(<"$stdout_path")"
    stderr="$(<"$stderr_path")"
    rm -f "$stdout_path" "$stderr_path"
    if [[ "$status" -eq 0 ]]; then
      successes=$((successes + 1))
      printf 'MAINTENANCE_ATTEMPT kind=%s round=%s exit=0 stdout=%q stderr=%q\n' \
        "$kind" "$round" "$stdout" "$stderr"
      continue
    fi
    # Native `git gc` can honestly lose the exact pack snapshot concurrently
    # replaced by the independent `git repack`. Git may prefix the fatal pair
    # with repeated "index unavailable" lines, but every line must name the
    # identical pack hash. Every other failure remains fatal, and serialized
    # maintenance plus strict fsck proves recovery.
    if [[ -z "$stdout" ]] &&
      is_native_gc_repack_pack_race "$kind" "$status" "$stderr"; then
      pack_race_rejections=$((pack_race_rejections + 1))
      printf 'MAINTENANCE_REJECTED kind=%s round=%s exit=%s reason=native-gc-repack-pack-race stdout=%q stderr=%q\n' \
        "$kind" "$round" "$status" "$stdout" "$stderr"
      continue
    fi
    printf 'unexpected %s failure in round %s (exit=%s)\nstdout:\n%s\nstderr:\n%s\n' \
      "$kind" "$round" "$status" "$stdout" "$stderr" >&2
    return 1
  done
  printf 'MAINTENANCE_OK kind=%s successes=%s pack_race_rejections=%s\n' \
    "$kind" "$successes" "$pack_race_rejections"
}

mode="${1:-}"
shift || true

case "$mode" in
  runtime)
    git --version
    findmnt -n -o FSTYPE,TARGET "$root" 2>/dev/null || findmnt -n -o FSTYPE,TARGET /workspace
    ;;

  setup)
    rm -rf "$root"
    mkdir -p "$root" "$control" "$writers"
    git init --bare --initial-branch=main "$origin"
    git clone "$origin" "$repo"
    configure_repo "$repo"
    mkdir -p "$repo/seed"
    for i in $(seq 1 64); do
      printf 'seed-%s\n' "$i" >"$repo/seed/$i.txt"
    done
    gitc add seed
    gitc commit -m seed
    gitc push -u origin main
    gitc repack -ad
    gitc worktree add -b writer-a "$writers/a" HEAD
    gitc worktree add -b writer-b "$writers/b" HEAD
    gitc worktree add -b integrator "$writers/integrator" HEAD
    configure_repo "$writers/a"
    configure_repo "$writers/b"
    configure_repo "$writers/integrator"
    git clone "$origin" "$publisher"
    configure_repo "$publisher"
    git -C "$publisher" switch -c feed
    gitc update-ref refs/heads/lock-target HEAD
    gitc fsck --full --strict
    if ! setup_head="$(gitc rev-parse HEAD)"; then
      printf 'failed to read setup HEAD\n' >&2
      exit 1
    fi
    printf 'SETUP_OK head=%s\n' "$setup_head"
    ;;

  lock-holder)
    kind="${1:?lock kind required}"
    round="${2:?round required}"
    lock="$(lock_path "$kind")"
    ready="$(marker_path "$kind" "$round" ready)"
    release="$(marker_path "$kind" "$round" release)"
    rm -f "$ready" "$release"
    mkdir -p "$(dirname "$lock")" "$control"
    python3 - "$lock" "$ready" "$release" <<'PY'
import os
import sys
import time

lock, ready, release = sys.argv[1:]
fd = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
try:
    os.write(fd, b"held-by-vfs-git-ref-stress\n")
    os.fsync(fd)
    ready_fd = os.open(ready, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(ready_fd, b"ready\n")
        os.fsync(ready_fd)
    finally:
        os.close(ready_fd)
    deadline = time.monotonic() + 120
    while not os.path.exists(release):
        if time.monotonic() >= deadline:
            raise TimeoutError(f"release marker did not appear: {release}")
        time.sleep(0.05)
finally:
    os.close(fd)
    try:
        os.unlink(lock)
    except FileNotFoundError:
        pass
print("LOCK_RELEASED")
PY
    ;;

  wait-lock)
    kind="${1:?lock kind required}"
    round="${2:?round required}"
    wait_for_path "$(marker_path "$kind" "$round" ready)" present
    wait_for_path "$(lock_path "$kind")" present
    printf 'LOCK_VISIBLE kind=%s round=%s\n' "$kind" "$round"
    ;;

  contend-lock)
    kind="${1:?lock kind required}"
    round="${2:?round required}"
    lock="$(lock_path "$kind")"
    wait_for_path "$lock" present
    case "$kind" in
      index)
        printf 'index-lock-round-%s\n' "$round" >"$repo/index-lock-$round.txt"
        expect_lock_failure "index-$round" gitc add "index-lock-$round.txt"
        ;;
      packed-refs)
        expect_lock_failure "packed-refs-$round" gitc pack-refs --all --prune
        ;;
      ref)
        expect_lock_failure "ref-$round" gitc update-ref refs/heads/lock-target HEAD
        ;;
    esac
    if [[ ! -e "$lock" ]]; then
      printf 'contended %s lock disappeared before holder release: %s\n' \
        "$kind" "$lock" >&2
      exit 1
    fi
    gitc fsck --connectivity-only --strict --no-dangling
    ;;

  release-lock)
    kind="${1:?lock kind required}"
    round="${2:?round required}"
    release="$(marker_path "$kind" "$round" release)"
    printf 'release\n' >"$release"
    ;;

  recover-lock)
    kind="${1:?lock kind required}"
    round="${2:?round required}"
    lock="$(lock_path "$kind")"
    wait_for_path "$lock" absent
    case "$kind" in
      index)
        gitc add "index-lock-$round.txt"
        gitc reset -q HEAD -- "index-lock-$round.txt"
        rm -f "$repo/index-lock-$round.txt"
        ;;
      packed-refs)
        gitc pack-refs --all --prune
        ;;
      ref)
        gitc update-ref refs/heads/lock-target HEAD
        ;;
    esac
    gitc fsck --connectivity-only --strict --no-dangling
    printf 'LOCK_RECOVERED kind=%s round=%s\n' "$kind" "$round"
    ;;

  writer)
    actor="${1:?writer actor required}"
    count="${2:?writer count required}"
    barrier="${3:-}"
    worktree="$writers/$actor"
    join_workload_barrier "$barrier" "writer-$actor"
    for i in $(seq 1 "$count"); do
      printf 'writer-%s-%s\n' "$actor" "$i" >"$worktree/writer-$actor-$i.txt"
      git -C "$worktree" add "writer-$actor-$i.txt"
      git -C "$worktree" commit -m "writer-$actor-$i"
    done
    if ! writer_head="$(git -C "$worktree" rev-parse HEAD)"; then
      printf 'failed to read writer-%s HEAD\n' "$actor" >&2
      exit 1
    fi
    printf 'WRITER_OK actor=%s commits=%s head=%s\n' \
      "$actor" "$count" "$writer_head"
    ;;

  publisher)
    count="${1:?publisher count required}"
    barrier="${2:-}"
    join_workload_barrier "$barrier" publisher
    for i in $(seq 1 "$count"); do
      printf 'feed-%s\n' "$i" >"$publisher/feed-$i.txt"
      git -C "$publisher" add "feed-$i.txt"
      git -C "$publisher" commit -m "feed-$i"
      git -C "$publisher" push origin HEAD:refs/heads/feed
      if [[ "$i" -eq 1 ]]; then
        printf 'ready\n' >"$control/feed-ready"
      fi
    done
    if ! publisher_head="$(git -C "$publisher" rev-parse HEAD)"; then
      printf 'failed to read publisher HEAD\n' >&2
      exit 1
    fi
    printf 'PUBLISHER_OK commits=%s head=%s\n' \
      "$count" "$publisher_head"
    ;;

  integrator)
    rounds="${1:?integrator rounds required}"
    barrier="${2:-}"
    worktree="$writers/integrator"
    join_workload_barrier "$barrier" integrator
    wait_for_path "$control/feed-ready" present
    for _ in $(seq 1 "$rounds"); do
      git -C "$worktree" fetch origin \
        refs/heads/feed:refs/remotes/origin/feed
      if git -C "$worktree" show-ref --verify --quiet refs/remotes/origin/feed; then
        git -C "$worktree" merge --ff-only refs/remotes/origin/feed
      fi
    done
    if ! integrator_head="$(git -C "$worktree" rev-parse HEAD)"; then
      printf 'failed to read integrator HEAD\n' >&2
      exit 1
    fi
    printf 'INTEGRATOR_OK rounds=%s head=%s\n' \
      "$rounds" "$integrator_head"
    ;;

  reader)
    rounds="${1:?reader rounds required}"
    barrier="${2:-}"
    participant="${3:-reader}"
    join_workload_barrier "$barrier" "$participant"
    for _ in $(seq 1 "$rounds"); do
      gitc status --porcelain=v2 >/dev/null
      gitc rev-parse --verify HEAD^{commit} >/dev/null
      gitc for-each-ref --format='%(objectname) %(refname)' refs/heads/ >/dev/null
      gitc cat-file -e HEAD^{tree}
    done
    printf 'READER_OK rounds=%s\n' "$rounds"
    ;;

  ref-churn)
    actor="${1:?ref actor required}"
    count="${2:?ref count required}"
    barrier="${3:-}"
    join_workload_barrier "$barrier" "refs-$actor"
    head="$(gitc rev-parse HEAD)"
    for i in $(seq 1 "$count"); do
      gitc update-ref "refs/heads/churn-$actor-$i" "$head"
    done
    if ! actual="$(
      gitc for-each-ref --format='%(refname)' "refs/heads/churn-$actor-*" |
        wc -l |
        tr -d ' '
    )"; then
      printf 'failed to enumerate ref churn for actor %s\n' "$actor" >&2
      exit 1
    fi
    if [[ "$actual" -ne "$count" ]]; then
      printf 'ref churn count mismatch for actor %s: expected=%s actual=%s\n' \
        "$actor" "$count" "$actual" >&2
      exit 1
    fi
    printf 'REF_CHURN_OK actor=%s refs=%s\n' "$actor" "$actual"
    ;;

  maintenance)
    kind="${1:?maintenance kind required}"
    rounds="${2:?maintenance rounds required}"
    barrier="${3:-}"
    join_workload_barrier "$barrier" "$kind"
    run_maintenance_rounds "$kind" "$rounds"
    ;;

  wait-workload-barrier)
    barrier="${1:?barrier required}"
    expected="${2:?participant count required}"
    for _ in $(seq 1 1200); do
      actual="$(
        find "$control" -maxdepth 1 -type f -name "workload-$barrier-*.ready" |
          wc -l |
          tr -d ' '
      )"
      if [[ "$actual" -eq "$expected" ]]; then
        printf 'WORKLOAD_BARRIER_READY barrier=%s participants=%s\n' \
          "$barrier" "$actual"
        exit 0
      fi
      sleep 0.05
    done
    printf 'maintenance barrier %s did not reach %s participants\n' \
      "$barrier" "$expected" >&2
    exit 1
    ;;

  release-workload-barrier)
    barrier="${1:?barrier required}"
    printf 'start\n' >"$control/workload-$barrier.start"
    ;;

  finalize)
    writer_count="${1:?writer count required}"
    feed_count="${2:?feed count required}"
    ref_count="${3:?ref count required}"
    gitc fetch origin \
      refs/heads/feed:refs/remotes/origin/feed
    gitc merge --no-ff -m "merge stress branches" \
      writer-a writer-b refs/remotes/origin/feed
    gitc push origin main
    maintenance_command gc
    maintenance_command repack
    maintenance_command commit-graph
    maintenance_command midx
    gitc fsck --full --strict
    git -C "$origin" fsck --full --strict
    git -C "$publisher" fsck --full --strict
    if ! writer_a_actual="$(gitc log --all --format=%s | grep -c '^writer-a-')"; then
      printf 'failed to count writer-a commits\n' >&2
      exit 1
    fi
    if ! writer_b_actual="$(gitc log --all --format=%s | grep -c '^writer-b-')"; then
      printf 'failed to count writer-b commits\n' >&2
      exit 1
    fi
    if ! feed_actual="$(gitc log --all --format=%s | grep -c '^feed-')"; then
      printf 'failed to count feed commits\n' >&2
      exit 1
    fi
    if ! refs_a_actual="$(
      gitc for-each-ref --format='%(refname)' 'refs/heads/churn-a-*' |
        wc -l |
        tr -d ' '
    )"; then
      printf 'failed to count churn-a refs\n' >&2
      exit 1
    fi
    if ! refs_b_actual="$(
      gitc for-each-ref --format='%(refname)' 'refs/heads/churn-b-*' |
        wc -l |
        tr -d ' '
    )"; then
      printf 'failed to count churn-b refs\n' >&2
      exit 1
    fi
    if [[ "$writer_a_actual" -ne "$writer_count" ||
      "$writer_b_actual" -ne "$writer_count" ||
      "$feed_actual" -ne "$feed_count" ||
      "$refs_a_actual" -ne "$ref_count" ||
      "$refs_b_actual" -ne "$ref_count" ]]; then
      printf 'final count mismatch: writer-a=%s/%s writer-b=%s/%s feed=%s/%s refs-a=%s/%s refs-b=%s/%s\n' \
        "$writer_a_actual" "$writer_count" \
        "$writer_b_actual" "$writer_count" \
        "$feed_actual" "$feed_count" \
        "$refs_a_actual" "$ref_count" \
        "$refs_b_actual" "$ref_count" >&2
      exit 1
    fi
    if find "$repo/.git" -type f -name '*.lock' -print -quit | grep -q .; then
      printf 'orphan Git lock remains:\n' >&2
      find "$repo/.git" -type f -name '*.lock' -print >&2
      exit 1
    fi
    if ! final_status="$(gitc status --porcelain)"; then
      printf 'failed to read final worktree status\n' >&2
      exit 1
    fi
    if [[ -n "$final_status" ]]; then
      printf 'final worktree is dirty:\n%s\n' "$final_status" >&2
      exit 1
    fi
    if ! final_head="$(gitc rev-parse HEAD)"; then
      printf 'failed to read final HEAD\n' >&2
      exit 1
    fi
    printf 'FINALIZE_OK head=%s\n' "$final_head"
    ;;

  snapshot)
    expected_head="${1:?expected HEAD required}"
    if ! actual_head="$(gitc rev-parse HEAD)"; then
      printf 'failed to read snapshot HEAD\n' >&2
      exit 1
    fi
    if [[ "$actual_head" != "$expected_head" ]]; then
      printf 'snapshot HEAD mismatch: expected=%s actual=%s\n' \
        "$expected_head" "$actual_head" >&2
      exit 1
    fi
    if ! origin_head="$(git -C "$origin" rev-parse refs/heads/main)"; then
      printf 'failed to read origin main HEAD\n' >&2
      exit 1
    fi
    if [[ "$origin_head" != "$expected_head" ]]; then
      printf 'origin main mismatch: expected=%s actual=%s\n' \
        "$expected_head" "$origin_head" >&2
      exit 1
    fi
    gitc fsck --full --strict
    if ! ref_digest="$(
      gitc for-each-ref --format='%(refname) %(objectname) %(*objectname)' |
        LC_ALL=C sort |
        git hash-object --stdin
    )"; then
      printf 'failed to compute ref digest\n' >&2
      exit 1
    fi
    if ! object_digest="$(
      gitc cat-file \
        --batch-all-objects \
        --batch-check='%(objectname) %(objecttype) %(objectsize)' |
        LC_ALL=C sort |
        git hash-object --stdin
    )"; then
      printf 'failed to compute all-object digest\n' >&2
      exit 1
    fi
    if ! origin_object_digest="$(
      git -C "$origin" cat-file \
        --batch-all-objects \
        --batch-check='%(objectname) %(objecttype) %(objectsize)' |
        LC_ALL=C sort |
        git hash-object --stdin
    )"; then
      printf 'failed to compute origin all-object digest\n' >&2
      exit 1
    fi
    printf 'HEAD=%s\nORIGIN_HEAD=%s\nREF_DIGEST=%s\nOBJECT_DIGEST=%s\nORIGIN_OBJECT_DIGEST=%s\nSNAPSHOT_OK\n' \
      "$actual_head" "$origin_head" "$ref_digest" "$object_digest" \
      "$origin_object_digest"
    ;;

  *)
    printf 'usage: %s {runtime|setup|lock-holder|wait-lock|contend-lock|release-lock|recover-lock|writer|publisher|integrator|reader|ref-churn|maintenance|wait-workload-barrier|release-workload-barrier|finalize|snapshot}\n' \
      "$0" >&2
    exit 2
    ;;
esac
