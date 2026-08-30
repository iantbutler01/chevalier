# Darwin guest control transport

The existing `portproxy` gRPC services compile and pass their tests on macOS
arm64. Long-lived control and VFS relays are guest-initiated through
`VZVirtioSocketListener`. A separate root-owned runtime-configuration agent
listens inside the guest so the VZ owner can use
`VZVirtioSocketDevice.connect(toPort:)` for bounded host-initiated updates.

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

That default produces ad-hoc-signed development binaries. A sealable template
must use an Apple-trusted signing identity so its designated requirement stays
valid across upgrades:

```sh
CHEVALIER_DARWIN_GUEST_RELEASE_BUILD=1 \
CHEVALIER_DARWIN_GUEST_CODESIGN_IDENTITY='Developer ID Application: Example (TEAMID)' \
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

The installer also installs `com.bracket.runtime-config` on guest vsock port
13340. A sealed template uses a generated, unknown bootstrap bearer; vmd sends
the clone's fresh portproxy bearer before declaring it ready. The same agent
keeps legacy VNC-password mode disabled. Enable macOS **Screen Sharing** once
in System Settings while producing the gold image, but leave **VNC viewers may
control screen with password** disabled. OpenBracket authenticates Screen
Sharing with guest account credentials supplied only for the active desktop
dialog.

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

macOS classifies the FSKit workspace as a protected volume even when macFUSE
reports it as local. The portproxy LaunchDaemon is the responsible process for
commands it launches, so a template must grant the release-signed portproxy
access to network volumes. Managed deployments use a supervised PPPC profile
with the exact designated requirement from `codesign -dr -`; an unmanaged gold
image requires a one-time user grant in Privacy & Security. Ad-hoc signatures
are diagnostic-only because their designated requirement is CDHash-pinned and
changes with every build. Keep managed workspaces outside Desktop, Documents,
and Downloads so no broader protected-folder grant is required.
