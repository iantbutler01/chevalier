# chevalier-sandbox

Sandbox runtime for Chevalier. It provides one facade over local/self-hosted `vmd` and managed OpenComputer sandboxes.

## What Is Here

| Path | Purpose |
| --- | --- |
| `crates/sandbox/` | Public Rust facade: connect, session, attach, exec, file operations, fork, mounts |
| `vmd/` | Host daemon for local or self-hosted sandbox execution |
| `portproxy/` | Guest/host port proxy support |
| `proto/` | gRPC contracts for `vmd` and portproxy |
| `scripts/` | Verification, integration, DR, security, and rollout drills |

## Providers

The facade is stable across providers:

- `chevalier` / `local` / `vmd`: Chevalier's own host daemon
- `opencomputer`: OpenComputer backend through their API

Default crate features include `local`. Remote-only consumers can avoid host/process dependencies:

```toml
chevalier-sandbox = { version = "0.1", default-features = false, features = ["client"] }
```

Use `distributed-control` for etcd/NATS-backed routing and HA control paths. Use `vfs-server` when serving VFS gateway routes from the sandbox crate.

## Client Image Contract

Chevalier clients must set `SandboxConfig.default_image`, the N-API `defaultImage` option, or
`BRACKET_VM_IMAGE`. There is no implicit guest image. A missing or blank image fails during client
initialization before any daemon request or image conversion starts. OpenComputer does not use this
Chevalier image setting.

An explicitly configured client may prewarm that selected image during its first connection. Long-
lived applications should retain and reuse the `Sandbox` handle; use `Sandbox::prewarm` when image
preparation should be a separate operator-visible startup step.

## VFS Mounts

Shared mounts carry Chevalier VFS metadata through the same session API. With OpenComputer, command mounts require `chevalier-vfs-fuse` to be present in the guest, either through a prepared checkpoint or an install/download step that runs before the mount command.

## Dedicated PCI Devices

Host PCI assignment is disabled unless `vmd` receives `--pci-policy <path>` or
`CHEVALIER_SANDBOX_PCI_POLICY`. The policy is an operator allowlist, must be owned by root or the
`vmd` service user with mode `0600`, and contains a capability token separate from the ordinary vmd
auth token:

```json
{
  "capabilityToken": "replace-with-a-dedicated-secret",
  "devices": [
    {
      "id": "gpu-0",
      "label": "Dedicated GPU",
      "bdfs": ["0000:09:00.0", "0000:09:00.1"],
      "managed": true,
      "hotplug": false,
      "prepareCommand": [],
      "releaseCommand": []
    }
  ]
}
```

`managed:true` requires an amd64 Linux host, populated and complete IOMMU groups, `vfio-pci`, and a
root `vmd`; QEMU still drops to `qemu_process.run_as_uid/run_as_gid`. `managed:false` requires every
allowlisted function to be pre-bound to `vfio-pci`. Hotplug is an explicit operator assertion; cold
attach is the baseline. Assigned-device VMs cannot snapshot or fork, and PCI policy cannot be enabled
with HA mode. Callers that omit PCI fields and the dedicated token retain the pre-feature behavior.

## Per-VM Resource Admission

`vmd` rejects VM requests above its per-VM admission bounds. Existing installations retain the
defaults of 8 vCPU, 16,384 MiB memory, and 100 GiB disk. Dedicated hosts can opt into larger VMs by
setting positive integer values before starting `vmd`:

- `CHEVALIER_SANDBOX_MAX_VM_VCPU`
- `CHEVALIER_SANDBOX_MAX_VM_MEMORY_MB`
- `CHEVALIER_SANDBOX_MAX_VM_DISK_GB`

The legacy `BRACKET_SANDBOX_*` prefix is also accepted. Invalid or non-positive configured bounds
fail daemon startup; omitted settings preserve the defaults.

## Verification

Common gates:

```bash
make verify
make verify-strict
make verify-e2e
make verify-strict-real PROFILE=local-dev
```

The Makefile also exposes targeted gates for fork, API facade, storage, admission, DR, security, SLO, ownership fence, partition handling, and control-gateway failover.

Real gates need real runtime dependencies. Keep mock/unit checks for fast iteration, but do not use them as proof of product readiness.
