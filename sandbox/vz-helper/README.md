# chevalier-vz

`chevalier-vz` is the macOS host helper for Chevalier's native
Virtualization.framework backend. It verifies restore images, installs guarded
macOS bundles, validates and starts deterministic no-NIC VMs, and bridges the
guest-initiated control stream to an optional host-loopback port.

## Build and sign

```sh
cd sandbox/vz-helper
CHEVALIER_VZ_CODESIGN_IDENTITY='Apple Development: Name (TEAMID)' ./scripts/build-signed.sh
```

The signing identity must be allowed to carry
`com.apple.security.virtualization`. Ad-hoc signing is the script default for
build-only checks; use a development or distribution identity for live VZ
operations.

## Commands

```sh
.build/release/chevalier-vz probe
.build/release/chevalier-vz inspect-image \
  --ipsw /path/to/Restore.ipsw \
  --expected-sha256 <digest>
.build/release/chevalier-vz install \
  --request /path/to/install-request.json \
  --events /path/to/install-events.jsonl
.build/release/chevalier-vz clone --request /path/to/clone-request.json
.build/release/chevalier-vz run --request /path/to/run-request.json
.build/release/chevalier-vz version
```

`probe` returns the exact latest restore image supported by the current host,
host CPU and memory bounds, entitlement state, and hardware-model requirements.
`inspect-image` loads a local IPSW through Virtualization.framework and streams
the file through SHA-256 for provenance without loading it into memory. It
rejects unsupported images and images without a usable configuration.

Discovery metadata is not installation identity. Persist the opaque hardware
model returned by the verified local IPSW; do not substitute the hardware model
returned by `latestSupported` even when its URL and build match.

`install` verifies the local IPSW before creating a guarded bundle, persists
the opaque Mac platform identity, validates a deterministic no-NIC VZ device
graph, and writes monotonic JSONL progress events while `VZMacOSInstaller`
runs. The request schema is defined by `InstallRequest`; callers must choose an
explicit free-space preflight threshold and a root disk of at least 64 GiB.

`clone` creates a distinct runnable bundle with APFS `clonefile` copies of both
`Disk.img` and the installed `AuxiliaryStorage`, an exact hardware model, and a
fresh machine identifier. `AuxiliaryStorage` contains boot data written during
macOS installation and cannot be replaced with blank storage. The command
rejects existing or cross-device destinations and never falls back to a full
copy.

`run` loads an installed bundle and may attach one read-only provisioning
directory for image maintenance. Runtime workspaces are mounted by the signed
guest `chevalier-vfs-fuse` service: it owns the APFS materialized tree, durable
WAL, gateway publisher, and macFUSE FSKit mount. Product readiness waits for the
guest mount sentinel and publisher status rather than treating VZ machine state
as workspace readiness.

Set `viewerMode` to `window` in the run request for a local diagnostic window;
`headless` and an omitted value create no `NSWindow` or
`VZVirtualMachineView`. The graphics device remains configured so the owning
helper can attach a view later without restarting the guest.

Set `loopbackRelayPort` in the run request to expose that one VM only on
`127.0.0.1:<port>` for the existing vmd gRPC client. Omitting it retains the
guest connection without creating a host listener.

The run helper remains the owning process for its `VZVirtualMachine`. When
`ownerControlSocketPath` and `runtimeGeneration` are present, it exposes the
versioned same-user lifecycle and lazy-viewer control protocol over that private
AF_UNIX socket. Virtualization.framework does not let another process attach a
view directly to the live VM.
