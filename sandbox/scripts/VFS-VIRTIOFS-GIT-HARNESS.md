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
2. Same-mount and simultaneous cross-mount `O_CREAT|O_EXCL`.
3. Same/cross-mount flock, POSIX byte ranges, release, disjoint ranges, and a
   blocking lock handoff.
4. Three-alias hard-link inode/link-count identity, writes through every alias,
   cross-mount rename/unlink coherence, and read/write through an open
   descriptor after final pathname unlink without resurrection.
5. Conventional in-worktree `.git` init/add/commit/branch/merge/rebase/stash/fsck.
6. A 1,000-file Git workload with machine-readable add/commit/cold-status/
   warm-status/gc/full-fsck timings.
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

The JSON result records both the initial and post-restart gateway protocol
evidence, the complete seeded model trace, request counts, per-check output, and
exact cleanup state. Redirect stdout and stderr separately to preserve a durable
receipt:

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
