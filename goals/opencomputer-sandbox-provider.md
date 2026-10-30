# Goal: OpenComputer As A Sandbox Backend

As built: 2026-10-30. The provider lives behind the existing sandbox facade in
`sandbox/crates/sandbox/src/opencomputer.rs` and `src/lib.rs`.

OpenComputer is just another sandbox provider. It must plug into the existing
`chevalier-sandbox` sandbox API instead of creating a parallel public API.

Non-negotiable direction:

- Do not add a separate downstream feature such as `sandbox-opencomputer`.
- Do not expose a parallel public `OpenComputerClient` / `OpenComputerSession`
  surface as the product-facing interface.
- Do not require consumers to change from the existing `Sandbox` / `Session`
  workflow to provider-specific types.
- The public shape should be config-level selection on the existing sandbox
  context, for example `SandboxConfig` choosing local/direct, distributed, or
  OpenComputer.
- Any OpenComputer REST implementation details should be private/internal
  plumbing behind the existing facade.
- Product repos should see this as a config swap, not an API migration.
- OtherYou is not part of this implementation pass except as historical
  reference for desired semantics.

Implementation contract:

- Keep OpenComputer transport private, without another public provider API.
- Existing `Sandbox::new`, `Sandbox::session`,
  `Sandbox::attach_session`, and `Session` operations branch through the
  configured backend.
- Do not assume OpenComputer lacks fork/snapshot or interactive shell support.
  Verify the API/SDK shape and implement those capabilities through the existing
  `Session::fork` / `Session::shell` surface when available.
- Only operations that are actually unavailable after verification should return
  the existing `SandboxError::Unsupported` from the existing methods.

Implementation note:

- OpenComputer fork/checkpoint support belongs behind `Session::fork`.
- OpenComputer websocket exec support also backs `Session::shell`, so callers
  should not see a separate interactive-shell API.
- OpenComputer also exposes a PTY endpoint. The SDK's own stateful shell helper
  is still backed by a long-running exec websocket, which is the current
  `Session::shell` mapping; use PTY later only if the facade grows terminal
  sizing/PTY-specific semantics.
- OpenComputer rclone/FUSE mounts are provider config behind the existing
  sandbox facade. Static provider mounts live in
  `OpenComputerBackendConfig.mounts`; Chevalier `SessionOptions.shared_mounts`
  require an explicit `OpenComputerBackendConfig.shared_mounts` mapping by
  mount tag, guest path, or backend profile so VFS mounts do not silently
  disappear under a provider swap.
- `Sandbox::list_sessions` reads the provider's sandbox listing and maps its
  state and metadata into `SessionInfo`. Persist physical sandbox IDs in the
  caller; logical session aliases held in memory do not survive a restart.
- Unsupported responses remain narrow: OpenComputer preview URLs do not fit
  the existing local-port `ForwardHandle` return shape. Use the provider preview
  URL through the existing facade instead.

## V2 lifecycle and persistence

The adapter sends `x-oc-sdk-version: 2.3.1`, matching the inspected OpenComputer
SDK contract. OpenComputer uses this header when choosing the runtime for a new
sandbox; omitting it can select its retired v1 runtime. A v1 checkpoint is not a
prepared v2 guest image.

`Session::state` reads the reported state rather than assuming every surviving
sandbox is running. `pause` and `stop` hibernate the sandbox, retaining its disk;
`resume` and `start` wake the same sandbox; `restart` calls the guest reboot
endpoint. `attach_session_passive` reads without waking, so lifecycle inspection
cannot undo idle cooling.

OpenComputer forgets its mount tracking across hibernate/wake. An attach with
`shared_mounts` lists the current mounts and restores missing ones. Callers must
provide their persisted mount requirements when reattaching. A configured
`mount_credentials` minter renders each mount credential for the physical
sandbox ID; credentials remain in the secret environment, not command arguments.

HTTP 404 means the sandbox is missing. Provider 5xx and rate-limit responses are
typed `ProviderUnavailable` errors; they are not evidence a persistent disk was
deleted. Callers retain their binding when those errors occur. A failed command
may already have run and must not be replayed merely because another provider is
available.

The local HTTP contract tests cover version headers, state, hibernate/wake,
restoring missing mounts and failure classification. Enabling this provider in
a product also requires live validation of its prepared guest, FUSE access,
commands and preview stream; passing those contract tests alone does not prove
provider capacity or image readiness.
