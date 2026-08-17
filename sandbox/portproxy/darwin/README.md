# Darwin guest control transport

The existing `portproxy` gRPC services compile and pass their tests on macOS
arm64. The Darwin-specific gap is transport: macOS `vsock(4)` supports stream
connections initiated by the guest, so the host must install a
`VZVirtioSocketListener`; host-initiated `connect(toPort:)` is not the correct
control path for a macOS guest.

`portproxy-darwin-vsock-bridge` connects from guest CID to
`VMADDR_CID_HOST:13338`, then relays that one full-duplex byte stream to the
guest-local portproxy endpoint at `127.0.0.1:13338`. HTTP/2 multiplexes exec and
file RPCs over the single persistent stream. The bridge reconnects after the
host or VM restarts.

The same binary has a guest-listener mode for services whose client lives in
the VM. `com.bracket.vfs-vsock-bridge` listens only on guest
`127.0.0.1:18080`; each accepted HTTP connection opens a guest-initiated stream
to host vsock port 13339. The helper maps that port to one configured host
loopback VFS endpoint and bounds concurrent sessions.

Build and stage the guest payload on Apple Silicon:

```sh
./sandbox/portproxy/darwin/build-guest-assets.sh
```

Expose `sandbox/portproxy/bin/darwin-arm64` to the VM with a
`VZVirtioFileSystemDeviceConfiguration` using
`VZVirtioFileSystemDeviceConfiguration.macOSGuestAutomountTag`. After macOS
Setup Assistant completes, run the staged installer once inside the guest with
a host-generated per-VM auth token file:

```sh
sudo ./install-guest-assets.sh --auth-token-file /path/to/token
```

The VM configuration must include `VZVirtioSocketDeviceConfiguration`. Before
starting the VM, register a `VZVirtioSocketListener` on port 13338. Retain the
listener and delegate for the VM lifetime. When the guest connection arrives,
retain its `VZVirtioSocketConnection`; its `fileDescriptor` is owned by that
object and closes when the object is released.

`chevalier-vz run` accepts an optional `loopbackRelayPort`. When present, it
binds only `127.0.0.1:<port>` and pairs one local TCP client with one accepted
guest Virtio-socket connection. Create a fresh gRPC channel after every
reconnect. Do not expose a shared or non-loopback TCP listener. Authenticate
every RPC with the same per-VM `CHEVALIER_PORTPROXY_AUTH_TOKEN` installed in the
guest.

The shared portproxy CLI exposes `--rpc-bind-address`; the Darwin launch script
sets it to `127.0.0.1`. The guest bridge is therefore the only route to gRPC,
including if a later profile adds a network device.

Darwin child processes inherit the service account's absolute `HOME`, falling
back to `/var/root`, and use a PATH that includes `/opt/homebrew/bin`. Linux
retains its existing `/root` and PATH defaults.

The LaunchDaemon runs as root, but macOS TCC can still deny protected user
folders. Keep managed workspaces outside Desktop, Documents, and Downloads
(for example `/Users/Shared/OpenBracket`) unless a signed build is granted Full
Disk Access through UI or an MDM PPPC profile.
