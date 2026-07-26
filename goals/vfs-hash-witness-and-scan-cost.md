# VFS hash witness, scan cost, and the large-file edges

Written 2026-07-25 after landing the durable stat witness. Records what was
measured, what was fixed, and what is deliberately left open, so the next
session does not have to re-derive it.

## The problem, as measured

The replica index was maintained by re-hashing the tree. On a measured owner
(74,142 files / 826 MB) this drove gateway latency:

| operation | before | after |
| --- | --- | --- |
| `subtree-metadata` (root, cold process) | 16,814–22,290 ms | 394 ms |
| `metadata-many` | 9,524 ms | 1.3–1.7 ms warm |
| `/stat` warm | ~1 ms | ~0.8 ms |
| slow-request log lines after a restart | many | 0 |

A 1,234-sample latency run against `/stat` had 20 samples over 100 ms and 8 over
1 s, max 7.94 s, with every slow sample inside the scan window.

## What was actually wrong

Three independent defects, none of which was the one originally assumed.

1. **The witness-validated hash cache existed and was unusable.**
   `local.rs` already compared `size + mtime + ctime` before reusing a hash, with
   a recency guard and a trusted-write flag. But `HASH_CACHE_MAX_AGE` was 30 s
   against a 600 s scan interval — every entry was guaranteed expired before the
   next scan reached it — and `MAX_HASH_CACHE_ENTRIES` was 16,384 against 74,142
   files, with oldest-first eviction, which is exactly pessimal for a sequential
   directory walk. Structural 100 % miss.

2. **The cache was per-process, so restarts paid full price.** Fixed by
   persisting the witness (`mtime_ns`, `ctime_ns` beside `content_hash` in
   `vfs_replica_entries`) and seeding the in-process cache from it on store
   creation. This is what git's index, restic's parent metadata, and borg's files
   cache all do; ours was the only one that was memory-only.

3. **`metadata-many` took the blocking lock.** It used `publications.read` while
   the far heavier `subtree-metadata` and `prefetch-subtree` use
   `optimisticRead`, so a single-path batch queued behind the writer backlog.

## Design notes worth keeping

- **Why ctime and not just mtime.** ctime cannot be set from userspace, so it
  still moves when a writer preserves or backdates mtime (`tar -x`,
  `rsync --times`, `touch -t`) and on metadata-only changes. A witness without it
  can be defeated by a same-size edit that restores mtime. Test:
  `local_storage_detects_same_size_out_of_band_edit_with_restored_mtime` — it
  fails if the ctime comparison is neutralised.
- **Only complete witnesses are ever stored or trusted.** A half-known pair is
  dropped at parse time; absent reads as "unknown" and forces a re-hash. Rows
  predating the columns therefore self-heal rather than lying.
- **Witness is not part of `replicaEntryIndexFingerprint`**, so carrying it
  cannot trigger spurious repairs or change-feed rows.
- **Nanoseconds are TEXT, not INTEGER.** ~1.75e18 exceeds
  `Number.MAX_SAFE_INTEGER`, and `node:sqlite` *rejects* such an INTEGER outright
  ("Value is too large to be represented as a JavaScript number") rather than
  rounding. The napi boundary carries them as `bigint` for the same reason
  `sizeBytes` does.
- **409 on a read is a retry signal**, not a rejection — `optimisticRead` returns
  it after `MAX_OPTIMISTIC_SNAPSHOT_ATTEMPTS` contended attempts. vmd previously
  classified it terminal, which made `subtree-metadata` and `prefetch-subtree`
  able to fail hard under sustained write churn. That was a latent bug, fixed
  before `metadata-many` was moved onto the same path.

## Open work

### 1. Unranged whole-file reads buffer entire files
`file/raw` without a `Range` header does `buf = await store.read(relPath)`,
materialising the whole file in the API process. Ranged reads already use
`readRange` and are bounded, and the FUSE mount always issues ranged reads — so
working against a large file *through the mount* is fine. The exposure is the
unranged API path used by sync/copy flows. This is the one edge that turns
absent-minded large-file use into an OOM rather than a slowdown. Fix by
streaming; it does not require chunking.

### 2. Metadata operations cannot bound hashing
`max_hash_bytes` exists and is plumbed, but `scanVfsReplicaFilesystem` requires a
valid sha256 per file (`/^[a-f0-9]{64}$/`), so large files cannot be opted out of
hashing without breaking the scan. Teach the scan to tolerate an absent hash,
then a stat on a multi-GB file need not pay a full read.

### 3. Scan is still a full tree walk
The hashing is gone from the steady state, but the walk and its per-entry DB work
remain; `/stat` p99 during a scan was still ~2.5 s in the last steady-state
sample. The intended shape is Ceph's split: a frequent light pass (stat-only,
compare against the persisted witness) and a rare, throttled deep pass that
re-hashes to catch bit rot. Note that fixing the cache *removed* the incidental
bit-rot detection that constant re-hashing was providing, so the deep pass is a
replacement for a property that was silently being relied on, not new polish.

### 4. ~3,159 rows never receive a witness
Of 28,668 hashed entries, 25,509 are witnessed and the count is stable across
generations. The remainder have a stored `content_hash` that does not match what
the scan observes, so the backfill correctly declines to witness them. Root cause
not established; they simply re-hash.

### 5. Chunking (CDC) — evaluated, not recommended yet
Identity is the whole-file hash, so any edit costs a full re-hash and full
re-transfer. Content-defined chunking (restic: Rabin-Karp, 512 KiB–8 MiB; borg:
BuzHash64) fixes that. Reasons it is not the next step:
- It does not fix open item 1; that is a buffering bug, not a chunking one.
- CDC's advantage over fixed-size blocks is *insertion resilience*. Dataset
  workloads tend to append or rewrite wholesale — append is served fine by fixed
  blocks, and a wholesale rewrite defeats both.
- Identity, sync convergence, and the witness all key off the whole-file hash;
  chunking means a chunk store, chunk GC, and a transfer protocol.
Revisit if in-place edits of large files prove to be a real workload. Price
fixed-size blocks before CDC. (CDC also has a known side channel: chunk
boundaries can leak chunker parameters — arxiv 2504.02095.)

## The BLAKE3 mount outage (2026-07-25)

Moving the content hash to BLAKE3 broke the mount: guest writes to
`/workspace/<repo>` returned EIO and terminal creation failed on its write
probe. Two independent defects, found in this order.

**1. vmd still hashed with SHA-256.** vmd sends a content fingerprint as a CAS
precondition (`base_content_hash` → `x-chevalier-vfs-precondition-fingerprint`).
It computes that itself, in `sandbox/vmd/src/fuse/{fs,write}.rs`, and **vmd does
not depend on the `chevalier-vfs` crate at all** — its only chevalier dependency
is `chevalier-sandbox`. So changing `vfs/src/local.rs` did not change vmd, and
every precondition-bearing write compared SHA-256 against a stored BLAKE3 hash
and 409'd. Creates use an `absent` precondition, so they still worked; that made
the symptom read as "creates fail" when the failure was actually on delete and
overwrite.

Both pinning tests asserted the SHA-256 empty vector, so they tracked the drift
rather than catching it. They now pin the same BLAKE3 vector as
`vfs/src/pack.rs` plus an `assert_ne!` against SHA-256.

**2. `create_hard_link` had no case in OpenBracket's namespace path filter.**
`namespaceMutationPaths` (`packages/api/src/runtime/vfs-git-filter.ts`) fell
through to `mutation.path`, which that variant lacks — it carries `source_path`
and `destination_path`. `undefined` reached a path normalizer and threw
`Cannot read properties of undefined (reading 'replace')`, returned as a 500.
git hard-links every object it writes, so ordinary git activity triggered it.
vmd cannot retire a failing namespace batch, so it retained and retried
forever; the pending batch made every namespace barrier fail, and unlink/rmdir
then blocked 30 s apiece. Observed as `rm -rf` hung with **zero** entries
removed in 40 s.

`applyNamespaceBatch` is typed `any` at the napi boundary, so reading a
nonexistent field compiled cleanly — `tsc` could not have caught it.

Worth keeping:
- **Any digest is defined in three places** — `vfs/src/local.rs`, the napi
  `VfsContentHasher`, and vmd's own copy. A one-sided change is not a wrong
  number, it is a dead mount. SHA-256 remains correct for VM images, portproxy
  assets, tap-name derivation, and `pack.rs::sha256_of` (on-disk format).
- **vmd's FUSE mount also exists on the host**, inside the container at
  `/var/lib/chevalier/vms/<vm-id>/fuse-mounts/<scope>`. Reproducing there with
  `docker exec` removes the VM, virtiofsd, and the guest from the loop, and is
  what separated "creates fail" from "deletes fail" in minutes.
- **A stuck namespace batch presents as a filesystem hang, not an error.** The
  30 s barrier waits are the tell; `WARN vfs namespace journal replay failed`
  names the offending mutation and the gateway's error text.

## Watch liveness and the 25s cache outage (2026-07-26)

`reply_ttl()` hands the kernel a positive attribute lease only while
`revision_watch_live()` is true; otherwise TTL=0 and every getattr/lookup is a
wire round trip. Liveness was asserted when a watch poll RETURNED — but an idle
long poll does not return until `REVISION_WATCH_TIMEOUT_MS` (25 s). So one
transport blip cost 500 ms backoff + a full idle long poll ≈ **25.5 s of
uncached serving on an already-healthy connection**. Measured: 9 flaps in 2 h,
each 25.5 s, against a 1.13 ms RTT.

Fixed by re-establishing on a 1 s window after a failure (same endpoint, same
`since` fence, so the same thing is confirmed, only sooner), reverting to the
25 s window on success. Steady-state cadence and load are unchanged. The gateway
floors `timeout_ms` at `WATCH_TIMEOUT_MIN_MS` (1 s), so no gateway change was
needed. Verified: outage 25.5 s → **1.51 s**, with warm `git status` and guest
`stat` unchanged.

What remains unexplained is what causes the blip. It recurs on a ~10 min cadence
matching `scanIntervalMs` (600 000 ms). During a stall the API's main thread is
parked in `futex_wait_queue_me` with **zero** threads in R or D and ~14 % of one
core — so it is blocked on a lock whose holder is awaiting I/O, not compute.
`/health` is a pure sync handler behind sync-only middleware, so a 28 s response
means the event loop never advanced. Ruled out by measurement: swap (none
configured), sqlite (this deployment is Postgres, zero `.db` fds), V8 GC (no
`node-V8Worker` running during stalls), Rust CPU (no `tokio-rt-worker` running),
and `seedHashCache` (0 invocations across a full cycle; startup only).
Identifying the lock needs a profile or instrumentation from inside the process.

## Process notes

Three stale-artifact incidents in one session, all the same root — a timestamp or
link standing in for "is this current", which is the same question the witness
answers:
- `rsync -a` preserved local mtimes, so a synced source looked older than the
  artifact built from it and cargo skipped the rebuild. The deploy reported
  success while running a four-hour-old binary. Fixed by syncing chevalier by
  content (`--checksum --no-times`).
- `mv`-restoring a file gave it an mtime older than the compiled test binary, so
  cargo reused a stale build and three "failures" were measuring old code.
- `native.d.ts` is generated from the Rust crate. Changing `ts/src/*.rs` without
  rebuilding meant `tsc` typechecked against a binding that lacked the new
  method. `napi build` writes a new inode, breaking pnpm's hardlink into its
  store, so the `.node` must be copied into `node_modules` explicitly; the
  generated `.d.ts`/`.js` stay linked and propagate on their own.

Also: `vitest` runs through esbuild and does not typecheck. Passing tests are not
a substitute for `tsc -p` on an edited package — that gap cost two deploy cycles.
