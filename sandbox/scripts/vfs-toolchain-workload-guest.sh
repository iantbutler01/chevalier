#!/usr/bin/env bash
set -euo pipefail

# Real-toolchain workload probes for the mounted VFS.
#
# The git harnesses prove Git's access patterns. This one proves the patterns
# every OTHER build tool relies on: bulk small-file creation, rename-swap of
# artifacts, fsync ordering, mmap reads, symlink trees, and — above all —
# reading back exactly the bytes that were written. A dropped write or a stale
# cached size does not fail a VFS primitive test; it fails as a short read, a
# SIGBUS, or a linker error the first time a real compiler touches the tree.
#
# Two rules encode lessons paid for in production:
#
#   1. Toolchains are pre-warmed OFF the mount. A pinned `rust-toolchain.toml`
#      makes the first `cargo` invocation download a few hundred MB through
#      rustup; a harness that skips `prewarm` measures that download, times out,
#      and reports a VFS fault that never happened.
#   2. Integrity manifests live OFF the mount, under $STATE_DIR. A manifest
#      stored on the filesystem under test cannot witness that filesystem
#      losing data.

export GIT_AUTHOR_NAME="Chevalier Toolchain Workload"
export GIT_AUTHOR_EMAIL="toolchain-workload@chevalier.test"
export GIT_COMMITTER_NAME="$GIT_AUTHOR_NAME"
export GIT_COMMITTER_EMAIL="$GIT_AUTHOR_EMAIL"
export GIT_TERMINAL_PROMPT=0
export CARGO_TERM_COLOR=never
export NPM_CONFIG_FUND=false
export NPM_CONFIG_AUDIT=false
export NPM_CONFIG_UPDATE_NOTIFIER=false

MOUNT=${WORKLOAD_MOUNT:-/workspace}
ROOT="$MOUNT/toolchain-workload"
STATE_DIR=${WORKLOAD_STATE_DIR:-/tmp/vfs-toolchain-workload}
LARGE_FILE_BYTES=$((10 * 1024 * 1024))

mkdir -p "$STATE_DIR"

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

note() {
  printf '%s\n' "$*"
}

# A path is "off mount" only if it does not resolve underneath $MOUNT. Used to
# assert that toolchain caches never sit on the filesystem under test.
assert_off_mount() {
  local label=$1 path=$2 resolved
  resolved=$(readlink -f "$path" 2>/dev/null || printf '%s' "$path")
  case "$resolved" in
    "$MOUNT"/*|"$MOUNT")
      fail "$label resolves onto the mount ($resolved); toolchain caches must live off it"
      ;;
  esac
  printf '%s=%s (off-mount)\n' "$label" "$resolved"
}

require_cmd() {
  local cmd=$1
  command -v "$cmd" >/dev/null 2>&1 || fail "missing required command '$cmd' — run the 'prewarm' mode first"
}

# Hash every regular file under a tree into a stable, sorted manifest. Symlinks
# are recorded by target rather than followed, so a build that swaps a link for
# a regular file is caught instead of silently compared against the resolved
# content.
write_manifest() {
  local tree=$1 out=$2
  (
    cd "$tree"
    find . \( -type f -o -type l \) -print0 |
      LC_ALL=C sort -z |
      while IFS= read -r -d '' entry; do
        if [ -L "$entry" ]; then
          printf 'link %s -> %s\n' "$entry" "$(readlink "$entry")"
        else
          printf 'file %s %s %s\n' "$entry" "$(stat -c '%s' "$entry")" "$(sha256sum "$entry" | cut -d' ' -f1)"
        fi
      done
  ) >"$out"
  printf 'entries=%s\n' "$(wc -l <"$out" | tr -d ' ')"
}

# The core regression check. Re-reads every file and compares size AND content
# hash against a manifest captured earlier. A short read, a dropped write, or a
# stale cached size all surface here as a concrete named path.
verify_manifest() {
  local tree=$1 manifest=$2 label=$3 current="$STATE_DIR/current-$label.manifest"
  [ -s "$manifest" ] || fail "no manifest to verify against at $manifest"
  write_manifest "$tree" "$current" >/dev/null
  if ! diff -u "$manifest" "$current" >"$STATE_DIR/diff-$label.txt"; then
    note "--- integrity drift ($label) ---"
    head -40 "$STATE_DIR/diff-$label.txt" >&2
    fail "tree changed after operations: $(grep -c '^[+-][^+-]' "$STATE_DIR/diff-$label.txt") differing line(s)"
  fi
  note "integrity verified ($label): $(wc -l <"$manifest" | tr -d ' ') entries byte-for-byte"
}

# Read a file through mmap, touching every page, and compare the mapped bytes to
# a plain read. This is the direct probe for the short-read / SIGBUS class: a
# file whose cached size exceeds its served content faults past the end of data.
mmap_verify() {
  local target=$1
  python3 - "$target" <<'PY'
import hashlib, mmap, os, sys

path = sys.argv[1]
size = os.path.getsize(path)
if size == 0:
    print(f"mmap skipped (empty): {path}")
    raise SystemExit(0)

with open(path, "rb") as handle:
    with mmap.mmap(handle.fileno(), 0, prot=mmap.PROT_READ) as mapped:
        if len(mapped) != size:
            raise SystemExit(f"FAIL: mmap length {len(mapped)} != stat size {size} for {path}")
        mapped_digest = hashlib.sha256()
        # Touch every page explicitly so a hole faults here rather than later.
        for offset in range(0, size, mmap.PAGESIZE):
            mapped_digest.update(mapped[offset : min(offset + mmap.PAGESIZE, size)])

with open(path, "rb") as handle:
    read_digest = hashlib.sha256()
    consumed = 0
    while True:
        chunk = handle.read(1 << 20)
        if not chunk:
            break
        consumed += len(chunk)
        read_digest.update(chunk)

if consumed != size:
    raise SystemExit(f"FAIL: short read {consumed} of {size} bytes for {path}")
if mapped_digest.hexdigest() != read_digest.hexdigest():
    raise SystemExit(f"FAIL: mmap and read disagree for {path}")
print(f"mmap+read agree over {size} bytes ({mapped_digest.hexdigest()[:16]}) for {path}")
PY
}

# A dependency-free cargo workspace. No registry access means `--offline` holds
# and the probe measures rustc's filesystem behaviour instead of the network.
scaffold_cargo() {
  local project="$ROOT/cargo"
  rm -rf "$project"
  mkdir -p "$project/crates/core/src" "$project/crates/app/src"

  cat >"$project/Cargo.toml" <<'TOML'
[workspace]
members = ["crates/core", "crates/app"]
resolver = "2"

[profile.dev]
incremental = true
TOML

  cat >"$project/crates/core/Cargo.toml" <<'TOML'
[package]
name = "workload-core"
version = "0.1.0"
edition = "2021"

[dependencies]
TOML

  cat >"$project/crates/app/Cargo.toml" <<'TOML'
[package]
name = "workload-app"
version = "0.1.0"
edition = "2021"

[dependencies]
workload-core = { path = "../core" }
TOML

  # Enough generated code that rustc produces real incremental state and a
  # multi-megabyte target/ tree rather than a trivial one-file build.
  {
    printf 'pub fn checksum(input: &[u8]) -> u64 {\n'
    printf '    input.iter().fold(1469598103934665603u64, |acc, b| (acc ^ *b as u64).wrapping_mul(1099511628211))\n'
    printf '}\n\n'
    for index in $(seq 1 200); do
      printf 'pub fn helper_%s(value: u64) -> u64 { value.wrapping_mul(%s).wrapping_add(%s) }\n' "$index" "$index" "$index"
    done
  } >"$project/crates/core/src/lib.rs"

  {
    printf 'use workload_core::checksum;\n\n'
    printf 'fn main() {\n'
    printf '    let data = std::env::args().nth(1).unwrap_or_else(|| "chevalier".to_string());\n'
    printf '    println!("{}", checksum(data.as_bytes()));\n'
    printf '}\n'
  } >"$project/crates/app/src/main.rs"
}

mode=${1:?usage: vfs-toolchain-workload-guest.sh MODE [ARGS...]}
shift || true

case "$mode" in
  runtime)
    printf 'mount='
    findmnt -n -o FSTYPE,SOURCE,OPTIONS "$MOUNT" || printf 'unknown\n'
    printf 'filesystem='
    stat -f -c '%T' "$MOUNT"
    printf 'kernel='
    uname -srmo
    for tool in cargo rustc node npm python3 git; do
      if command -v "$tool" >/dev/null 2>&1; then
        printf '%s=%s\n' "$tool" "$("$tool" --version 2>&1 | head -1)"
      else
        printf '%s=ABSENT\n' "$tool"
      fi
    done
    ;;

  # Pull every toolchain component BEFORE any timed workload runs, and prove the
  # caches live off the mount. Without this, the first cargo invocation pays for
  # a full pinned-toolchain download and the harness reports a timeout.
  prewarm)
    assert_off_mount CARGO_HOME "${CARGO_HOME:-$HOME/.cargo}"
    assert_off_mount RUSTUP_HOME "${RUSTUP_HOME:-$HOME/.rustup}"
    assert_off_mount NPM_CACHE "$(npm config get cache 2>/dev/null || printf '%s' "$HOME/.npm")"
    if command -v rustup >/dev/null 2>&1; then
      note "resolving pinned rust toolchain (this is the download the harness must not time)"
      rustup show active-toolchain || rustup toolchain install stable --profile minimal
    fi
    require_cmd cargo
    require_cmd rustc
    require_cmd node
    note "prewarm complete"
    ;;

  # Bulk small-file writes, artifact rename-swap, incremental rebuild, and a
  # byte-for-byte re-read of the entire produced tree.
  cargo-build)
    require_cmd cargo
    scaffold_cargo
    project="$ROOT/cargo"
    note "cold build"
    cargo build --offline --manifest-path "$project/Cargo.toml" --workspace
    binary="$project/target/debug/workload-app"
    [ -x "$binary" ] || fail "cargo produced no executable at $binary"
    first=$("$binary" chevalier)
    note "binary output: $first"

    write_manifest "$project/target" "$STATE_DIR/cargo-target.manifest"
    verify_manifest "$project/target" "$STATE_DIR/cargo-target.manifest" cargo-immediate

    note "incremental rebuild after source edit"
    printf '\npub fn added_later(value: u64) -> u64 { value + 7 }\n' >>"$project/crates/core/src/lib.rs"
    cargo build --offline --manifest-path "$project/Cargo.toml" --workspace
    second=$("$binary" chevalier)
    [ "$first" = "$second" ] || fail "binary output changed across an additive rebuild ($first -> $second)"

    note "re-reading the rebuilt binary through mmap"
    mmap_verify "$binary"
    ;;

  # node_modules is the harshest ordinary small-file workload there is: thousands
  # of files, deep nesting, and a .bin symlink tree.
  node-install)
    require_cmd npm
    project="$ROOT/node"
    rm -rf "$project"
    mkdir -p "$project"
    cat >"$project/package.json" <<'JSON'
{
  "name": "workload-node",
  "private": true,
  "version": "0.1.0",
  "type": "module",
  "dependencies": {
    "vite": "5.4.10"
  }
}
JSON
    note "npm install (network)"
    npm install --prefix "$project" --no-fund --no-audit
    [ -d "$project/node_modules" ] || fail "npm produced no node_modules"
    installed=$(find "$project/node_modules" -type f | wc -l | tr -d ' ')
    links=$(find "$project/node_modules" -type l | wc -l | tr -d ' ')
    note "installed files=$installed symlinks=$links"
    [ "$installed" -gt 100 ] || fail "node_modules has only $installed files; install did not land"
    write_manifest "$project/node_modules" "$STATE_DIR/node-modules.manifest"
    verify_manifest "$project/node_modules" "$STATE_DIR/node-modules.manifest" node-immediate
    ;;

  # A real bundler: mmap-heavy reads, rapid rewrite cycles, and a dist/ swap.
  vite-build)
    require_cmd npm
    project="$ROOT/node"
    [ -d "$project/node_modules" ] || fail "run the 'node-install' mode first"
    mkdir -p "$project/src"
    cat >"$project/index.html" <<'HTML'
<!doctype html>
<html>
  <head><title>workload</title></head>
  <body><div id="app"></div><script type="module" src="/src/main.js"></script></body>
</html>
HTML
    cat >"$project/src/main.js" <<'JS'
const marker = "chevalier-vfs-workload";
document.querySelector("#app").textContent = marker;
export default marker;
JS
    note "vite build"
    npm --prefix "$project" exec -- vite build --outDir dist --logLevel warn
    [ -f "$project/dist/index.html" ] || fail "vite produced no dist/index.html"
    grep -q "assets/" "$project/dist/index.html" || fail "dist/index.html references no bundled asset"
    write_manifest "$project/dist" "$STATE_DIR/vite-dist.manifest"
    verify_manifest "$project/dist" "$STATE_DIR/vite-dist.manifest" vite-immediate
    ;;

  # Serving from the mount exercises the read path under a live server, then
  # asserts the bytes on the wire match the bytes on disk.
  vite-serve)
    require_cmd npm
    project="$ROOT/node"
    [ -f "$project/dist/index.html" ] || fail "run the 'vite-build' mode first"
    port=${WORKLOAD_SERVE_PORT:-45173}
    log="$STATE_DIR/vite-preview.log"
    npm --prefix "$project" exec -- vite preview --outDir dist --port "$port" --strictPort >"$log" 2>&1 &
    server=$!
    # shellcheck disable=SC2064
    trap "kill $server 2>/dev/null || true" EXIT

    ready=0
    for _ in $(seq 1 60); do
      if curl -fsS "http://127.0.0.1:$port/" >"$STATE_DIR/served.html" 2>/dev/null; then
        ready=1
        break
      fi
      sleep 1
    done
    [ "$ready" -eq 1 ] || { note "--- server log ---"; cat "$log" >&2; fail "vite preview never became ready on port $port"; }

    served=$(sha256sum "$STATE_DIR/served.html" | cut -d' ' -f1)
    ondisk=$(sha256sum "$project/dist/index.html" | cut -d' ' -f1)
    [ "$served" = "$ondisk" ] || fail "served bytes differ from dist/index.html on the mount ($served != $ondisk)"
    note "server returned dist/index.html byte-for-byte ($served)"

    kill "$server" 2>/dev/null || true
    trap - EXIT
    ;;

  # Crosses LARGE_FILE_BYTES so the ranged/large-file read path is covered, not
  # only the cached whole-file path used by ordinary sources.
  large-file)
    project="$ROOT/large"
    rm -rf "$project"
    mkdir -p "$project"
    target="$project/blob.bin"
    size=$((LARGE_FILE_BYTES + 1024 * 1024))
    note "writing $size bytes (above the $LARGE_FILE_BYTES large-file threshold)"
    dd if=/dev/urandom of="$target" bs=1M count=$((size / 1024 / 1024)) status=none
    whole=$(sha256sum "$target" | cut -d' ' -f1)

    note "ranged re-reads must agree with the whole-file hash"
    mmap_verify "$target"
    middle=$(dd if="$target" bs=1M skip=5 count=1 status=none | sha256sum | cut -d' ' -f1)
    again=$(dd if="$target" bs=1M skip=5 count=1 status=none | sha256sum | cut -d' ' -f1)
    [ "$middle" = "$again" ] || fail "two identical ranged reads disagreed ($middle != $again)"
    recheck=$(sha256sum "$target" | cut -d' ' -f1)
    [ "$whole" = "$recheck" ] || fail "whole-file hash changed between reads ($whole != $recheck)"
    note "large-file reads stable ($whole)"
    ;;

  # Capture the state of everything built so far. Run this BEFORE the host
  # performs VFS operations (remount, gateway restart, sync, re-seed).
  snapshot)
    [ -d "$ROOT" ] || fail "nothing built at $ROOT to snapshot"
    write_manifest "$ROOT" "$STATE_DIR/workload-full.manifest"
    note "snapshot captured at $STATE_DIR/workload-full.manifest"
    ;;

  # The point of the whole harness: after the host has done something to the
  # filesystem, every byte must still be there, and the toolchains must still
  # work against the tree.
  verify-after-ops)
    [ -d "$ROOT" ] || fail "workload tree at $ROOT vanished entirely after operations"
    verify_manifest "$ROOT" "$STATE_DIR/workload-full.manifest" after-ops

    if [ -x "$ROOT/cargo/target/debug/workload-app" ]; then
      note "re-running the built binary after operations"
      "$ROOT/cargo/target/debug/workload-app" chevalier >/dev/null || fail "built binary no longer executes after operations"
      mmap_verify "$ROOT/cargo/target/debug/workload-app"
    fi

    if [ -f "$ROOT/cargo/Cargo.toml" ] && command -v cargo >/dev/null 2>&1; then
      note "cargo must consider the tree fresh (no spurious rebuild from changed mtimes/sizes)"
      cargo build --offline --manifest-path "$ROOT/cargo/Cargo.toml" --workspace
    fi
    note "post-operation verification passed"
    ;;

  final)
    note "workload tree summary"
    find "$ROOT" -maxdepth 2 -mindepth 1 -type d 2>/dev/null | LC_ALL=C sort | head -20
    printf 'total files=%s\n' "$(find "$ROOT" -type f 2>/dev/null | wc -l | tr -d ' ')"
    ;;

  clean)
    rm -rf "$ROOT" "$STATE_DIR"
    note "workload tree and state removed"
    ;;

  *)
    fail "unknown mode '$mode'"
    ;;
esac
