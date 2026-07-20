# Mounted POSIX model torture

`posix-model-torture.mjs` is an injectable, product-independent torture lane
for two disposable Chevalier sessions that mount the same disposable VFS
scope. It does not create sessions, listeners, owners, or persistent state.
The caller retains ownership of those resources.

The controller applies each seeded operation to:

1. a Linux-local reference tree in the first guest;
2. the mounted VFS through the selected actor guest;
3. canonical snapshots read through the actor and the other guest.

Every mutating action includes the applicable file or parent-directory
`fsync` and close barrier before another guest observes it. Canonical
snapshots compare paths, types, permission bits, file sizes, symlink targets,
and SHA-256 content identity with boundary-byte diagnostics while intentionally
ignoring device/inode numbers, timestamps, and sparse allocation layout.

The fixed prefix covers create/open/read/write/pwrite/truncate/sparse,
fsync/close/stat/chmod/mkdir/rmdir/symlink/readlink/rename-overwrite/unlink,
and open-unlink descriptor lifetime. A deterministic randomized suffix
continues with valid model-derived operations.

Use it from the real mounted harness:

```js
import { runPosixModelTorture } from "./posix-model-torture.mjs";

const evidence = await runPosixModelTorture({
  sessions: [first, second],
  execGuest,
  mountPath: "/workspace",
  seed: "record-this-seed",
  oneClientSteps: 64,
  twoClientSteps: 96,
});
```

The returned object contains the seed, every operation, actor/observer
identity, duration, first exact divergence, and overall status. The harness
uses randomized names under `/workspace/.posix-model-*`, removes only those
trees in `finally`, and rejects hidden `EIO` output.

The action generator and controller can be checked without vmd or a VM:

```bash
node --test sandbox/scripts/posix-model-torture.test.mjs
node sandbox/scripts/posix-model-torture.mjs \
  --self-test \
  --seed local-proof-1 \
  --steps 100
```
