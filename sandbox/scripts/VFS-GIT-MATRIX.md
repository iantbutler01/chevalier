# Mounted VFS deep-Git matrix

`run-vfs-virtiofs-git-matrix.sh` exercises Git as an ordinary workload through
the complete network filesystem path:

```text
disposable authenticated HTTP gateway
  → vmd RemoteFuseFs
  → patched virtiofsd
  → two simultaneous guest virtiofs mounts
  → standard .git directory and linked worktree
```

The controller creates a unique VFS owner and scope, permits `.git` only for
that owner, boots two disposable VMs, and installs an SHA-256-verified copy of
`vfs-git-matrix-guest.sh` in each guest. It never opens or modifies an existing
user owner or repository. Its `finally` path discards both sessions, closes the
gateway, and removes the fresh backing root. Cleanup failure makes the run
fail.

The matrix covers:

- init, local bare and non-local clone, status, add, commit, switch, divergent
  merge and rebase, cherry-pick, stash with untracked files, and soft/mixed/hard
  reset;
- transactional `update-ref`, 64 branches, 64 tags, `packed-refs`, reflogs, and
  compare-and-swap exclusion with contenders on separate mounts;
- deterministic cross-mount `index.lock` rejection followed by explicit stale
  lock recovery and a successful commit;
- linked-worktree add, `.git` pointer use, commit, move, remove, prune, and
  merge;
- a writer committing while the other mount repeatedly reads the index, HEAD,
  objects, and status, plus simultaneous disjoint ref writers;
- deterministic process interruption inside a blocking pre-commit hook,
  stale-lock inspection/recovery, merge abort, and rebase abort;
- repack, prune-packed, commit-graph, gc, verify-pack, strict full fsck, and an
  exact final object/ref/HEAD check from both mounts.

Each successful run prints one JSON object containing guest Git/kernel/mount
versions, per-phase durations and command evidence, gateway request count,
exact disposable identities, and cleanup proof.

Example on the OpenBracket corvidae → bismuth topology (use a callback port
that no other harness owns):

```bash
set -a
source /path/to/private/server.env
set +a

CHEVALIER_VFS_GIT_MATRIX_GATEWAY_PUBLIC_URL=http://100.66.99.67:19092 \
CHEVALIER_VFS_GIT_MATRIX_GATEWAY_PORT=19092 \
./sandbox/scripts/run-vfs-virtiofs-git-matrix.sh
```

Run `./sandbox/scripts/run-vfs-virtiofs-git-matrix.sh --help` for the complete
environment contract.
