# VFS lifecycle and resource-leak torture harness

`run-vfs-lifecycle-leak-torture.sh` repeatedly drives the complete disposable
lifecycle:

```text
restartable authenticated HTTP callback gateway
  → vmd RemoteFuseFs host mount and journals
  → patched virtiofsd child
  → VM virtiofs mount
  → acknowledged file/namespace mutations plus live flock/POSIX locks
  → discard while the lock process is alive
  → provider, process, mount, lock, journal, and directory cleanup
```

The harness uses a random probe id in every owner, VM name, scope, and backing
root. Final cleanup lists provider sessions and deletes only names containing
that exact id. It never deletes or mutates a pre-existing session, VM directory,
volume, mount, or VFS owner.

## Coverage

Every cycle proves:

- callback listener stop/start does not lose the mounted scope;
- one qemu, patched virtiofsd child, host FUSE mount, VM/runtime directory, and
  journal set exist while the disposable VM is active;
- both flock and POSIX byte-range lock rows exist while the guest holder lives;
- discarding with that holder alive removes the provider session and both lock
  rows without waiting for lease expiry;
- every provider create/list/discard call has a finite deadline; a creation
  that settles after its caller timed out is tracked and discarded
  automatically, with any still-pending or retained session identifiers
  included in the final JSON;
- the exact VM's qemu/virtiofsd processes, FUSE mount, runtime/data directories,
  journal files, sockets, pid files, write staging, and temporary files vanish;
- vmd threads/file descriptors and callback-process file descriptors/RSS
  remain bounded after warm-up, and neither RSS series shows a positive
  per-cycle leak slope;
- final global qemu, virtiofsd, FUSE-mount, and zombie counts do not exceed the
  pre-run baseline.

The callback gateway supplies an inspectable transactional lock store. This is
deliberate: mounted requests still traverse the actual virtiofsd → host FUSE →
HTTP path, while the receipt can prove the exact lock rows were released.

## Bismuth run

Run after deploying the vmd and patched virtiofsd under test:

```bash
SANDBOX_ENDPOINT=http://100.118.49.55:18062 \
SANDBOX_AUTH_TOKEN="$(cat /path/to/remote.token)" \
SANDBOX_IMAGE=/absolute/path/to/sandbox.qcow2 \
CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN="$(cat /path/to/vfs.token)" \
CHEVALIER_VFS_LIFECYCLE_GATEWAY_PUBLIC_URL=http://100.66.99.67:19094 \
CHEVALIER_VFS_LIFECYCLE_OBSERVER_SSH_HOST=bismuth \
./sandbox/scripts/run-vfs-lifecycle-leak-torture.sh \
  >"$EVIDENCE.json" 2>"$EVIDENCE.stderr.log"
```

The observer reads only process state, `/proc/1/status`, file-descriptor counts,
mount tables, and paths beneath the configured vmd data/runtime roots. It does
not read container environment variables or file contents.

Use `CHEVALIER_VFS_LIFECYCLE_CYCLES` for longer torture. FD/RSS limits are
explicit environment settings recorded in the JSON result, so a loosened
threshold cannot be hidden from the receipt. Run `--help` for the complete
contract.
