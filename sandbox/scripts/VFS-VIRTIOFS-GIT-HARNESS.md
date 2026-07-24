# Disposable VFS → virtiofsd → VM → Git harness

`run-vfs-virtiofs-git-conformance.sh` is the product-independent acceptance
harness for the complete mounted path:

```text
in-process HTTP gateway
  → vmd RemoteFuseFs host mount
  → patched virtiofsd
  → guest virtiofs mount
  → filesystem and Git commands
```

It uses a fresh local VFS root and first proves the product's normal one-VM /
one-mount topology. It then adds a second disposable VM on the same scope for
the stronger simultaneous cross-mount acceptance contract, deliberately
interrupts and restarts the callback gateway, and finally replaces one VM.
Cleanup discards every VM, stops the gateway, and removes the backing root even
when a check fails. Cleanup errors are emitted as a failing result instead of
being reported as success.

The machine running the harness must have host-native `ts/` and `ts-sandbox/`
bindings. The configured public gateway URL must route back to the harness
listener from the vmd container or host.

Before creating a VM, the harness drives an authenticated wire-level probe
through that public URL. It covers the complete `posix-lock/v1` action set,
exact-identity owner renewal, independent mount identities, POSIX/flock namespace separation, scoped
hard-link creation/alias lookup/mutation/unlink, and the lease-wrapped mutation
shape used by `RemoteFuseFs`. Each VM must then increase the in-process
gateway's request count and write a unique readiness challenge that the backing
store reads byte-for-byte before the guest removes it. That is the
vmd-to-callback reachability proof; another mount's heartbeat cannot satisfy it.

The preflight also creates a sibling disposable owner under the same valid
service bearer with Git metadata disabled. The Git-enabled owner must read its
private nested `.git/HEAD`, while the disabled owner must return 400 for the
write and 404 for stat/read. This proves that the mounted Git allowance is
owner policy, not a bearer-wide bypass.

The gateway half can be checked locally without vmd, QEMU, or a guest image:

```bash
./sandbox/scripts/run-vfs-gateway-protocol-probe.sh
```

Example:

```bash
SANDBOX_ENDPOINT=http://100.118.49.55:18062 \
SANDBOX_AUTH_TOKEN="$(cat /path/to/remote.token)" \
SANDBOX_IMAGE=/absolute/path/to/sandbox.qcow2 \
CHEVALIER_VFS_HARNESS_GATEWAY_PUBLIC_URL=http://100.64.1.2:19091 \
./sandbox/scripts/run-vfs-virtiofs-git-conformance.sh
```

Use `--help` for every variable. A focused rerun can select checks. Checks are
not dependency-expanded, so select prerequisite Git lifecycle checks when
selecting a later Git visibility or replacement check:

```bash
CHEVALIER_VFS_HARNESS_CHECKS=2,3,4 \
./sandbox/scripts/run-vfs-virtiofs-git-conformance.sh
```

Checks cover:

1. Actual one-VM `virtiofs` topology; mkdir/list/stat, symlink/lstat/readlink,
   chmod, sparse/random-offset writes, truncate, atomic replacement, open-unlink
   lifetime, unlink/rmdir, fsync/close/rename barriers, and bidirectional HTTP
   coherence.
2. Same-mount and synchronized cross-mount `O_CREAT|O_EXCL`: both mounts first
   complete a negative lookup, then issue the exclusive create while the winner
   keeps its descriptor open until the loser has received `EEXIST`; the winner's
   inode, exact mode, bytes, and authoritative stable file identity are checked
   from both mounts and the backing store. The same check then synchronizes two
   ordinary `O_CREAT|O_RDWR` callers after negative lookups, holds both
   descriptors open, publishes `A` and then `B` through opposite mounts, and
   requires both descriptors, both path views, and the backing store to converge
   on one stable identity, exact mode `0640`, and bytes `AB`.
3. Same/cross-mount flock, POSIX byte ranges, release, disjoint ranges, and a
   blocking lock handoff.
4. Three-alias hard-link inode/link-count identity, writes through every alias,
   cross-mount rename/unlink coherence, publication through an open descriptor
   after another mount deletes and first reuses its pathname with the same old
   bytes, a cold first read plus `fchmod`/truncate/write after the replacement
   diverges, stable-file-identity protection for both bytes and mode, and
   read/write after final pathname unlink with `st_nlink == 0` and no
   resurrection.
5. Conventional in-worktree `.git` init/add/commit/branch/merge/rebase/stash/fsck.
6. A 1,000-file Git correctness workload with a five-minute hard ceiling and
   machine-readable add/commit/cold-status/warm-status/gc/full-fsck timings.
7. Exact cross-mount HEAD/worktree visibility after close barriers, including
   relative, dangling, and nested `node_modules`-style symlink inodes.
8. Callback listener interruption that must surface as an honest guest I/O
   failure, followed by relisten, authenticated protocol reprobe, exact
   namespace replay, backing-store equality, and cross-mount visibility.
9. Seeded model-based POSIX torture against a local reference: 85 one-client
   actions and 117 alternating two-client actions, comparing operation results
   and complete actor/observer snapshots after every barrier.
10. After discarding both prior VMs, exact sequential replacement-VM
    HEAD/worktree visibility, symlink `lstat`/`readlink` and target behavior
    without restore `EIO`, and full fsck.
11. A separate Git usability gate over check 6's recorded workload evidence.
    Cold status must complete within 2 seconds and warm status within 1.5
    seconds by default. Override those budgets with
    `CHEVALIER_VFS_HARNESS_GIT_STATUS_COLD_MAX_MS` and
    `CHEVALIER_VFS_HARNESS_GIT_STATUS_WARM_MAX_MS`. Selecting check 11 requires
    selecting check 6 in the same run. The five-minute ceiling is a liveness
    gate, not a performance allowance.

    The 2s/1.5s status budgets are regression ceilings, not the product bar.
    The operative UX target (2026-07-24) is that ordinary interactive
    commands — single-file create/read/stat/rename/unlink and warm status
    over a normal working set — complete in **100–500ms** on a mounted
    workspace. Optimization work is measured against that bar; the harness
    gates only catch regressions past the ceilings.

## Evidence — 2026-07-24 coherence rebuild

Best full mounted runs on the rebuilt stack (corvidae harness → bismuth vmd,
two disposable VMs): checks 1–8 and 10 pass; 1,000-file workload
create 37.7s / add 99.7s / commit 5.9s / status cold 1682ms / warm 1594ms /
gc 51.4s / fsck 1664ms (prior baseline: 23-minute workload, status ≈2.4s).
Landed and regression-tested in this tree: generation-safe file identity
(`unix:{dev}:{ino}:{birth_s}:{birth_ns}`), descendant write-barrier +
self-healing deletion recovery, rename gate dedup, wire-backed metadata
serves with the revision watch + revocation-acked publications
(fsync-returns-after-observer-coherence), watch-liveness-bounded kernel
attr/entry leases with notifier push invalidation, and removal of
FUSE_WRITEBACK_CACHE negotiation (host-kernel size authority made sibling
extends invisible — the root cause of the deterministic cross-mount stale
reads).

Known issue (pre-existing, outside the VFS): bismuth guest microVMs
intermittently freeze 15–35s (guest timekeeping/vCPU tick stall;
soft-lockups recorded on production VMs before this work). The freeze
straddles the model torture's 30s per-command deadlines, so check 9 fails
on timeouts and check 11's warm budget is measured pessimistically.
Diagnostic dossier: host exonerated by PSI/schedstat/swap discrimination;
console log flood fixed (portproxy → file logging); invtsc now exposed
(guest no longer marks TSC unstable); in-guest backtrace capture and a
kvm-clock vs tsc clocksource A/B are the active threads.

The JSON result records both the initial and post-restart gateway protocol
evidence, the complete seeded model trace, request counts, per-check output, and
exact cleanup state. Redirect stdout and stderr separately to preserve a durable
receipt:

## Namespace coherence invariant

Namespace read-your-writes is journal projection, not a metadata-cache
exception. Each mount projects all queued create/mkdir/symlink/link,
unlink/rmdir, rename, and metadata mutations over authoritative reads until a
read observes the batch's committed server revision. Only an unprojected
authoritative result may enter the cache shared by sibling mounts, and it must
carry the exact revision attached to that response; sampling a newer shared
revision after the read is invalid. The gateway publishes storage changes and
the new revision under one per-owner write transaction, while
list/stat/subtree reads hold the matching shared snapshot. Content writes
continue through the separate write journal.

The subtree metadata snapshot is the RTT-amortization layer for ordinary
metadata-heavy workloads. It is revision-fenced and shared, not a Git or
temporary-file special case. The five-minute harness timeout is only a liveness
ceiling; check 11 independently enforces interactive Git-status latency.

```bash
./sandbox/scripts/run-vfs-virtiofs-git-conformance.sh \
  >"$EVIDENCE.json" 2>"$EVIDENCE.stderr.log"
```

The recorded model trace is exactly reproducible:

```bash
CHEVALIER_VFS_HARNESS_POSIX_SEED='<seed from evidence>' \
CHEVALIER_VFS_HARNESS_POSIX_ONE_STEPS=64 \
CHEVALIER_VFS_HARNESS_POSIX_TWO_STEPS=96 \
./sandbox/scripts/run-vfs-virtiofs-git-conformance.sh
```
