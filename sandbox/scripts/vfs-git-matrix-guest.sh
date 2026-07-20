#!/usr/bin/env bash
set -euo pipefail

export GIT_AUTHOR_NAME="Chevalier Git Matrix"
export GIT_AUTHOR_EMAIL="git-matrix@chevalier.test"
export GIT_COMMITTER_NAME="$GIT_AUTHOR_NAME"
export GIT_COMMITTER_EMAIL="$GIT_AUTHOR_EMAIL"
export GIT_TERMINAL_PROMPT=0

root=/workspace/git-matrix
repo="$root/repo"

gitc() {
  git -C "$repo" "$@"
}

configure_repo() {
  gitc config user.name "$GIT_AUTHOR_NAME"
  gitc config user.email "$GIT_AUTHOR_EMAIL"
  gitc config core.fsync committed
  gitc config core.fsyncMethod fsync
}

mode=${1:?usage: vfs-git-matrix-guest.sh MODE [ARGS...]}
shift

case "$mode" in
  runtime)
    printf 'git='
    git --version
    printf 'kernel='
    uname -srmo
    printf 'mount='
    findmnt -n -o FSTYPE,SOURCE,OPTIONS /workspace
    printf 'filesystem='
    stat -f -c '%T' /workspace
    ;;

  lifecycle)
    rm -rf "$root"
    mkdir -p "$root"
    git init --initial-branch=main "$repo"
    configure_repo

    printf 'base\n' >"$repo/base.txt"
    mkdir "$repo/nested"
    printf 'nested\n' >"$repo/nested/value.txt"
    gitc add .
    gitc commit -m base
    gitc status --porcelain=v2

    git clone --bare "$repo" "$root/origin.git"
    git clone --no-local "$root/origin.git" "$root/clone"
    git -C "$root/clone" fsck --strict --full

    gitc switch -c feature
    printf 'feature\n' >"$repo/feature.txt"
    gitc add feature.txt
    gitc commit -m feature
    feature_head=$(gitc rev-parse HEAD)

    gitc switch main
    printf 'main-side\n' >"$repo/main-side.txt"
    gitc add main-side.txt
    gitc commit -m main-side
    gitc merge --no-ff --no-edit feature
    gitc merge-base --is-ancestor "$feature_head" HEAD

    gitc switch -c rebase-topic
    printf 'topic\n' >"$repo/rebase-topic.txt"
    gitc add rebase-topic.txt
    gitc commit -m rebase-topic
    old_topic=$(gitc rev-parse HEAD)
    gitc switch main
    printf 'upstream\n' >"$repo/upstream.txt"
    gitc add upstream.txt
    gitc commit -m upstream
    gitc switch rebase-topic
    gitc rebase main
    new_topic=$(gitc rev-parse HEAD)
    test "$old_topic" != "$new_topic"
    gitc switch main
    gitc merge --ff-only rebase-topic

    gitc switch -c cherry-source
    printf 'cherry\n' >"$repo/cherry.txt"
    gitc add cherry.txt
    gitc commit -m cherry-source
    cherry=$(gitc rev-parse HEAD)
    gitc switch main
    gitc cherry-pick "$cherry"
    test "$(cat "$repo/cherry.txt")" = cherry

    printf 'dirty\n' >>"$repo/base.txt"
    printf 'untracked\n' >"$repo/untracked.txt"
    gitc stash push --include-untracked -m matrix-stash
    test ! -e "$repo/untracked.txt"
    test -z "$(gitc status --porcelain)"
    gitc stash pop
    grep -q dirty "$repo/base.txt"
    test -f "$repo/untracked.txt"
    gitc reset --hard HEAD
    gitc clean -fd

    printf 'soft\n' >"$repo/reset.txt"
    gitc add reset.txt
    gitc commit -m reset-soft-source
    gitc reset --soft HEAD^
    gitc diff --cached --quiet -- reset.txt && exit 71
    gitc commit -m reset-soft-recommit

    gitc reset --mixed HEAD^
    test -n "$(gitc status --porcelain -- reset.txt)"
    gitc add reset.txt
    gitc commit -m reset-mixed-recommit
    printf 'discard\n' >>"$repo/reset.txt"
    gitc reset --hard HEAD
    test "$(cat "$repo/reset.txt")" = soft

    gitc branch -D feature rebase-topic cherry-source
    gitc reflog -n 20 >/dev/null
    gitc fsck --strict --full
    head=$(gitc rev-parse HEAD)
    git clone --no-local "$repo" "$root/clone-final"
    test "$(git -C "$root/clone-final" rev-parse HEAD)" = "$head"
    git -C "$root/clone-final" fsck --strict --full
    printf 'LIFECYCLE_OK head=%s\n' "$head"
    ;;

  cross-read)
    expected=${1:?expected HEAD required}
    test "$(gitc rev-parse HEAD)" = "$expected"
    test -z "$(gitc status --porcelain)"
    gitc cat-file -e "$expected^{commit}"
    gitc fsck --strict --full
    test "$(cat "$repo/base.txt")" = base
    printf 'CROSS_READ_OK head=%s\n' "$expected"
    ;;

  refs)
    configure_repo
    head=$(gitc rev-parse HEAD)
    {
      printf 'start\n'
      for i in $(seq -w 0 63); do
        printf 'create refs/heads/matrix-%s %s\n' "$i" "$head"
        printf 'create refs/tags/matrix-%s %s\n' "$i" "$head"
      done
      printf 'prepare\ncommit\n'
    } | gitc update-ref --stdin
    test "$(gitc for-each-ref --format='%(refname)' 'refs/heads/matrix-*' | wc -l)" -eq 64
    test "$(gitc for-each-ref --format='%(refname)' 'refs/tags/matrix-*' | wc -l)" -eq 64
    gitc pack-refs --all --prune
    test -s "$repo/.git/packed-refs"
    grep -q 'refs/heads/matrix-00' "$repo/.git/packed-refs"
    gitc show-ref --verify refs/heads/matrix-63
    gitc show-ref --verify refs/tags/matrix-63
    gitc update-ref refs/heads/matrix-00 "$head" "$head"
    gitc fsck --strict --full
    printf 'REFS_OK head=%s refs=%s\n' "$head" "$(gitc for-each-ref --format='%(refname)' | wc -l)"
    ;;

  worktree)
    configure_repo
    rm -rf "$root/worktree-secondary" "$root/worktree-moved"
    gitc worktree prune
    gitc branch -D worktree-matrix 2>/dev/null || true
    gitc worktree add -b worktree-matrix "$root/worktree-secondary" HEAD
    test -f "$root/worktree-secondary/.git"
    grep -q '^gitdir: ' "$root/worktree-secondary/.git"
    printf 'worktree\n' >"$root/worktree-secondary/worktree.txt"
    git -C "$root/worktree-secondary" add worktree.txt
    git -C "$root/worktree-secondary" commit -m worktree-commit
    wt_head=$(git -C "$root/worktree-secondary" rev-parse HEAD)
    test "$(gitc rev-parse worktree-matrix)" = "$wt_head"
    gitc worktree move "$root/worktree-secondary" "$root/worktree-moved"
    test "$(git -C "$root/worktree-moved" rev-parse HEAD)" = "$wt_head"
    gitc worktree list --porcelain | grep -q "$root/worktree-moved"
    gitc worktree remove "$root/worktree-moved"
    gitc worktree prune
    gitc merge --ff-only worktree-matrix
    gitc branch -d worktree-matrix
    test "$(cat "$repo/worktree.txt")" = worktree
    gitc fsck --strict --full
    printf 'WORKTREE_OK head=%s\n' "$(gitc rev-parse HEAD)"
    ;;

  index-lock-prepare)
    configure_repo
    rm -f "$repo/.git/index.lock"
    printf 'index-lock\n' >"$repo/index-lock.txt"
    cp "$repo/.git/index" "$repo/.git/index.lock"
    test -s "$repo/.git/index.lock"
    printf 'INDEX_LOCK_HELD head=%s\n' "$(gitc rev-parse HEAD)"
    ;;

  index-lock-contend)
    expected=${1:?expected HEAD required}
    test -e "$repo/.git/index.lock"
    if gitc add index-lock.txt >/tmp/index-lock-add.out 2>/tmp/index-lock-add.err; then
      printf 'git add bypassed an existing index.lock\n' >&2
      exit 74
    fi
    grep -qi 'index.lock\|another git process' /tmp/index-lock-add.err
    test "$(gitc rev-parse HEAD)" = "$expected"
    printf 'INDEX_LOCK_REJECTED head=%s\n' "$expected"
    ;;

  index-lock-release)
    expected=${1:?expected HEAD required}
    test "$(gitc rev-parse HEAD)" = "$expected"
    rm "$repo/.git/index.lock"
    gitc add index-lock.txt
    gitc commit -m index-lock-recovered
    printf 'INDEX_LOCK_RECOVERED head=%s\n' "$(gitc rev-parse HEAD)"
    ;;

  seed-concurrency)
    configure_repo
    for i in $(seq -w 0 19); do
      {
        printf 'seed-%s-' "$i"
        head -c 4080 /dev/zero | tr '\0' x
        printf '\n'
      } >"$repo/concurrent-seed-$i.txt"
    done
    gitc add concurrent-seed-*.txt
    gitc commit -m concurrent-seed
    printf 'SEED_OK head=%s\n' "$(gitc rev-parse HEAD)"
    ;;

  writer)
    configure_repo
    count=${1:-24}
    for i in $(seq -w 1 "$count"); do
      printf 'writer-%s\n' "$i" >"$repo/writer-$i.txt"
      gitc add "writer-$i.txt"
      gitc commit --quiet -m "writer-$i"
    done
    printf 'WRITER_OK head=%s count=%s\n' "$(gitc rev-parse HEAD)" "$count"
    ;;

  reader)
    count=${1:-80}
    for _ in $(seq 1 "$count"); do
      head=$(gitc rev-parse HEAD)
      gitc cat-file -e "$head^{commit}"
      gitc status --porcelain=v2 >/dev/null
    done
    gitc fsck --connectivity-only
    printf 'READER_OK head=%s count=%s\n' "$(gitc rev-parse HEAD)" "$count"
    ;;

  refs-writer)
    prefix=${1:?prefix required}
    count=${2:-40}
    head=$(gitc rev-parse HEAD)
    for i in $(seq -w 1 "$count"); do
      gitc update-ref "refs/heads/concurrent-$prefix-$i" "$head"
    done
    printf 'REF_WRITER_OK prefix=%s count=%s\n' "$prefix" "$count"
    ;;

  cas-prepare)
    label=${1:?label required}
    base=$(gitc rev-parse HEAD)
    tree=$(gitc rev-parse 'HEAD^{tree}')
    commit=$(printf 'cas-%s\n' "$label" | gitc commit-tree "$tree" -p "$base")
    printf '%s\n' "$commit"
    ;;

  cas-contend)
    candidate=${1:?candidate required}
    expected=${2:?expected required}
    delay=${3:-1}
    sleep "$delay"
    if gitc update-ref refs/heads/concurrent-cas "$candidate" "$expected"; then
      printf 'CAS_WON candidate=%s\n' "$candidate"
    else
      printf 'CAS_LOST candidate=%s\n' "$candidate"
    fi
    ;;

  interrupt-prepare)
    configure_repo
    rm -f "$repo/.git/index.lock" "$repo/.git/refs/heads/main.lock"
    mkdir -p "$repo/.git/hooks"
    printf '#!/bin/sh\nprintf ready >%q\nsleep 120\n' "$root/hook-ready" >"$repo/.git/hooks/pre-commit"
    chmod +x "$repo/.git/hooks/pre-commit"
    printf 'interrupted\n' >"$repo/interrupted.txt"
    gitc add interrupted.txt
    rm -f "$root/hook-ready"
    printf 'INTERRUPT_PREPARED head=%s\n' "$(gitc rev-parse HEAD)"
    ;;

  interrupt-commit)
    printf '%s\n' "$$" >"$root/interrupt-pid"
    gitc commit -m interrupted-commit
    ;;

  interrupt-recover)
    expected=${1:?expected HEAD required}
    test "$(gitc rev-parse HEAD)" = "$expected"
    test -f "$root/hook-ready"
    test -n "$(gitc diff --cached --name-only -- interrupted.txt)"
    stale=none
    if test -e "$repo/.git/index.lock"; then
      stale=index.lock
      rm "$repo/.git/index.lock"
    fi
    if test -e "$repo/.git/refs/heads/main.lock"; then
      stale="$stale,main.lock"
      rm "$repo/.git/refs/heads/main.lock"
    fi
    rm -f "$repo/.git/hooks/pre-commit" "$root/hook-ready" "$root/interrupt-pid"
    gitc status --porcelain=v2 >/dev/null
    gitc commit -m recovered-interrupted-commit

    gitc switch -c conflict-topic
    printf 'topic-conflict\n' >"$repo/conflict.txt"
    gitc add conflict.txt
    gitc commit -m topic-conflict
    gitc switch main
    printf 'main-conflict\n' >"$repo/conflict.txt"
    gitc add conflict.txt
    gitc commit -m main-conflict
    if gitc merge conflict-topic; then
      exit 72
    fi
    test -f "$repo/.git/MERGE_HEAD"
    gitc merge --abort
    test ! -e "$repo/.git/MERGE_HEAD"
    test "$(cat "$repo/conflict.txt")" = main-conflict

    if gitc rebase conflict-topic; then
      exit 73
    fi
    test -d "$repo/.git/rebase-merge" -o -d "$repo/.git/rebase-apply"
    gitc rebase --abort
    test ! -d "$repo/.git/rebase-merge"
    test ! -d "$repo/.git/rebase-apply"
    test "$(gitc symbolic-ref --short HEAD)" = main
    gitc branch -D conflict-topic
    gitc fsck --strict --full
    printf 'INTERRUPT_RECOVERY_OK stale=%s head=%s\n' "$stale" "$(gitc rev-parse HEAD)"
    ;;

  maintenance)
    configure_repo
    gitc repack -Ad
    gitc prune-packed
    gitc commit-graph write --reachable
    gitc gc --prune=now
    gitc fsck --strict --full
    packs=$(find "$repo/.git/objects/pack" -name '*.pack' -type f | wc -l)
    test "$packs" -ge 1
    for idx in "$repo"/.git/objects/pack/*.idx; do
      gitc verify-pack -s "$idx" >/dev/null
    done
    test -z "$(find "$repo/.git" -type f \( -name '*.lock' -o -name 'tmp_*' \) -print -quit)"
    printf 'MAINTENANCE_OK head=%s packs=%s\n' "$(gitc rev-parse HEAD)" "$packs"
    ;;

  final)
    expected=${1:?expected HEAD required}
    test "$(gitc rev-parse HEAD)" = "$expected"
    test -z "$(gitc status --porcelain)"
    gitc fsck --strict --full
    gitc cat-file --batch-check --batch-all-objects >/dev/null
    test -s "$repo/.git/packed-refs"
    printf 'FINAL_OK head=%s objects=%s refs=%s\n' \
      "$expected" \
      "$(gitc cat-file --batch-check --batch-all-objects | wc -l)" \
      "$(gitc for-each-ref --format='%(refname)' | wc -l)"
    ;;

  *)
    printf 'unknown mode: %s\n' "$mode" >&2
    exit 64
    ;;
esac
