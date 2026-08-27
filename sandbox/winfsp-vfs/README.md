# Chevalier Windows WinFsp VFS

This module is the native Windows workspace filesystem. It builds for ARM64
and AMD64, mounts a
guest-local NTFS materialized tree at `W:` through WinFsp, journals accepted
mutations to a guest-local append-only WAL, and publishes that WAL to the
existing authenticated Chevalier VFS gateway.

Build and unit-test from macOS without a Windows SDK:

```bash
GOCACHE=/private/tmp/openbracket-go-cache go test ./...
CGO_ENABLED=0 GOOS=windows GOARCH=arm64 go build \
  -o chevalier-vfs-winfsp.exe ./cmd/chevalier-vfs-winfsp
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build \
  -o chevalier-vfs-winfsp-amd64.exe ./cmd/chevalier-vfs-winfsp
```

`ChevalierVFS` runs the mount and publisher as an SCM service.
`ChevalierGuest` imports a one-use runtime configuration, stores its fresh
control and VFS bearers in an ACL-restricted directory, deletes the bootstrap
account and cached answer files, and exposes authenticated argv execution,
bounded native file RPCs, and orderly VFS drain. The sealed base contains the
executables and service registration, but no reusable runtime bearer, scope,
VM identity, or bootstrap password.

The development fault gate uses a disposable overlay and one-use answer media
to prove hydration, real `W:` I/O, `FlushFileBuffers`, ordered publication,
Git for Windows, offline local durability, forced filesystem-process death,
remount, and WAL replay:

```bash
./e2e/run.sh
```

The service acceptance gate builds both native architectures, starts a fresh
ARM64/HVF VM through vmd with a detachable runtime ISO and separate NTFS state
disk, and proves authenticated command/file RPCs, WinFsp create/write/flush/
read/delete/basic-info behavior, Git init/add/commit/fsck, publication drain,
and same-node restart without hot-injecting binaries:

```bash
./e2e/run-services.sh
```

The current implementation is deliberately one writable owner. Product
readiness still requires a gateway-enforced VM-generation authority fence,
power-cut/prepare-apply-commit recovery, persistent runtime TPM identity, and
the full WinFsp external semantics and repeated lifecycle suites.

## Dependency note

The module vendors the official MIT-licensed `github.com/winfsp/go-winfsp`
binding. The vendored `gofs` adapter has a narrow extension that forwards
`SetBasicInfo` to files implementing `FileSetBasicInfo`; upstream currently
rejects that callback unconditionally, which prevents ordinary Windows delete
and metadata flows. Running `go mod vendor` will overwrite the extension, so
preserve or upstream it before refreshing the vendor tree.
