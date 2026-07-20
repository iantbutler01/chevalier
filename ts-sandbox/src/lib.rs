//! Node-API bindings for the Chevalier sandbox CLIENT. Connects to an external
//! sandbox provider; never spawns one. Isolated from the core `chevalier`
//! binding so sandbox provider work can't break the core.
//!
//! v1 surface: connect/session/attachSession, exec (bidirectional ExecHandle),
//! shell (interactive PTY ShellHandle), readFile/writeFile, fork,
//! sessionId/vmId. (listDir/daemons are follow-ups — some need engine facade
//! additions.)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chevalier_sandbox::{
    DurableVolumeInfo as EngineDurableVolumeInfo, EventStream, ExecEvent, ExecInput, ExecOptions,
    ForkOptions, ForwardHandle as EngineForwardHandle, HostPciDevice as EngineHostPciDevice,
    HostPciDeviceState as EngineHostPciDeviceState, HostPciFunction as EngineHostPciFunction,
    HostPciInventory as EngineHostPciInventory, OpenComputerBackendConfig, OpenComputerMountConfig,
    PciDeviceAction as EnginePciDeviceAction, ResourceLimits, Sandbox as EngineSandbox,
    SandboxConfig, SandboxError, SandboxProviderConfig, Session as EngineSession,
    SessionInfo as EngineSessionInfo, SessionOptions, SharedMount, SharedMountAvailability,
    SharedMountContinuity, ShellEvent, ShellInput, ShellOptions,
};
use napi::bindgen_prelude::Buffer;
use napi_derive::napi;
use tokio::sync::Mutex;

fn sb_err(e: SandboxError) -> napi::Error {
    napi::Error::new(napi::Status::GenericFailure, format!("Sandbox: {e}"))
}

// ---------------- exec streaming ----------------

/// A single exec event. `type` is `stdout` | `stderr` | `exit` | `timeout`.
#[napi(object)]
pub struct ExecEventJs {
    #[napi(js_name = "type")]
    pub kind: String,
    pub data: Option<Buffer>,
    pub code: Option<i32>,
}

impl From<ExecEvent> for ExecEventJs {
    fn from(e: ExecEvent) -> Self {
        match e {
            ExecEvent::Stdout(b) => ExecEventJs {
                kind: "stdout".into(),
                data: Some(Buffer::from(b)),
                code: None,
            },
            ExecEvent::Stderr(b) => ExecEventJs {
                kind: "stderr".into(),
                data: Some(Buffer::from(b)),
                code: None,
            },
            ExecEvent::Exit(c) => ExecEventJs {
                kind: "exit".into(),
                data: None,
                code: Some(c),
            },
            ExecEvent::Timeout => ExecEventJs {
                kind: "timeout".into(),
                data: None,
                code: None,
            },
        }
    }
}

/// Bidirectional handle to a running exec: write stdin, read events via `next()`.
#[napi]
pub struct ExecHandle {
    input: tokio::sync::mpsc::Sender<ExecInput>,
    events: Arc<Mutex<EventStream<ExecEvent>>>,
}

#[napi]
impl ExecHandle {
    #[napi]
    pub async fn write(&self, data: Buffer) -> napi::Result<()> {
        self.input
            .send(ExecInput::Data(data.to_vec()))
            .await
            .map_err(|_| napi::Error::from_reason("exec stdin closed"))
    }
    #[napi]
    pub async fn eof(&self) -> napi::Result<()> {
        self.input
            .send(ExecInput::Eof)
            .await
            .map_err(|_| napi::Error::from_reason("exec stdin closed"))
    }
    #[napi]
    pub async fn signal(&self, sig: i32) -> napi::Result<()> {
        self.input
            .send(ExecInput::Signal(sig))
            .await
            .map_err(|_| napi::Error::from_reason("exec stdin closed"))
    }
    #[napi]
    pub async fn resize(&self, cols: u32, rows: u32) -> napi::Result<()> {
        // Saturate rather than silently truncate (a terminal never exceeds u16).
        let clamp = |v: u32| u16::try_from(v).unwrap_or(u16::MAX);
        self.input
            .send(ExecInput::Resize {
                cols: clamp(cols),
                rows: clamp(rows),
            })
            .await
            .map_err(|_| napi::Error::from_reason("exec stdin closed"))
    }
    /// The next event, or `null` when the exec stream ends.
    #[napi]
    pub async fn next(&self) -> napi::Result<Option<ExecEventJs>> {
        use futures::StreamExt;
        let mut guard = self.events.lock().await;
        match guard.next().await {
            Some(Ok(ev)) => Ok(Some(ExecEventJs::from(ev))),
            Some(Err(e)) => Err(sb_err(e)),
            None => Ok(None),
        }
    }
}

/// A single interactive shell event. `type` is `output` | `exit`.
#[napi(object)]
pub struct ShellEventJs {
    #[napi(js_name = "type")]
    pub kind: String,
    pub data: Option<Buffer>,
    pub code: Option<i32>,
}

impl From<ShellEvent> for ShellEventJs {
    fn from(e: ShellEvent) -> Self {
        match e {
            ShellEvent::Output(b) => ShellEventJs {
                kind: "output".into(),
                data: Some(Buffer::from(b)),
                code: None,
            },
            ShellEvent::Exit(c) => ShellEventJs {
                kind: "exit".into(),
                data: None,
                code: Some(c),
            },
        }
    }
}

/// Bidirectional handle to a running interactive PTY shell.
#[napi]
pub struct ShellHandle {
    input: tokio::sync::mpsc::Sender<ShellInput>,
    events: Arc<Mutex<EventStream<ShellEvent>>>,
}

#[napi]
impl ShellHandle {
    #[napi]
    pub async fn write(&self, data: Buffer) -> napi::Result<()> {
        self.input
            .send(ShellInput::Data(data.to_vec()))
            .await
            .map_err(|_| napi::Error::from_reason("shell stdin closed"))
    }

    #[napi]
    pub async fn eof(&self) -> napi::Result<()> {
        self.input
            .send(ShellInput::Eof)
            .await
            .map_err(|_| napi::Error::from_reason("shell stdin closed"))
    }

    #[napi]
    pub async fn resize(&self, cols: u32, rows: u32) -> napi::Result<()> {
        let clamp = |v: u32| u16::try_from(v).unwrap_or(u16::MAX).max(1);
        self.input
            .send(ShellInput::Resize {
                cols: clamp(cols),
                rows: clamp(rows),
            })
            .await
            .map_err(|_| napi::Error::from_reason("shell stdin closed"))
    }

    /// The next shell event, or `null` when the shell stream ends.
    #[napi]
    pub async fn next(&self) -> napi::Result<Option<ShellEventJs>> {
        use futures::StreamExt;
        let mut guard = self.events.lock().await;
        match guard.next().await {
            Some(Ok(ev)) => Ok(Some(ShellEventJs::from(ev))),
            Some(Err(e)) => Err(sb_err(e)),
            None => Ok(None),
        }
    }
}

// ---------------- options ----------------

#[napi(object)]
pub struct ExecOpts {
    pub env: Option<HashMap<String, String>>,
    pub timeout_secs: Option<i32>,
    pub detach: Option<bool>,
    pub shell: Option<String>,
    pub close_stdin_on_start: Option<bool>,
}

#[napi(object)]
pub struct ShellOpts {
    pub shell: Option<String>,
    pub args: Option<Vec<String>>,
    pub env: Option<HashMap<String, String>>,
    pub cwd: Option<String>,
    pub cols: Option<u32>,
    pub rows: Option<u32>,
}

impl From<ShellOpts> for ShellOptions {
    fn from(o: ShellOpts) -> Self {
        let clamp = |value: Option<u32>| value.map(|v| u16::try_from(v).unwrap_or(u16::MAX).max(1));
        ShellOptions {
            shell: o.shell,
            args: o.args.unwrap_or_default(),
            env: o.env.unwrap_or_default(),
            cwd: o.cwd,
            cols: clamp(o.cols),
            rows: clamp(o.rows),
        }
    }
}

impl From<ExecOpts> for ExecOptions {
    fn from(o: ExecOpts) -> Self {
        ExecOptions {
            env: o.env.unwrap_or_default(),
            timeout_secs: o.timeout_secs,
            detach: o.detach.unwrap_or(false),
            shell: o.shell,
            close_stdin_on_start: o.close_stdin_on_start.unwrap_or(false),
        }
    }
}

#[napi(object)]
pub struct SharedMountOpts {
    pub host_path: Option<String>,
    pub guest_path: String,
    pub mount_tag: String,
    pub read_only: Option<bool>,
    pub availability: Option<String>,
    pub continuity: Option<String>,
    pub backend_profile: Option<String>,
    pub vfs_endpoint: Option<String>,
    pub vfs_scope_path: Option<String>,
}

fn shared_mount_availability(value: Option<String>) -> SharedMountAvailability {
    match value.as_deref() {
        Some("shared-storage") | Some("shared_storage") | Some("sharedStorage") => {
            SharedMountAvailability::SharedStorage
        }
        _ => SharedMountAvailability::NodeLocal,
    }
}

fn shared_mount_continuity(
    value: Option<String>,
    availability: &SharedMountAvailability,
) -> SharedMountContinuity {
    match value.as_deref() {
        Some("restore-cross-node") | Some("restore_cross_node") | Some("restoreCrossNode") => {
            SharedMountContinuity::RestoreCrossNode
        }
        Some("restart-same-node") | Some("restart_same_node") | Some("restartSameNode") => {
            SharedMountContinuity::RestartSameNode
        }
        _ => match availability {
            SharedMountAvailability::SharedStorage => SharedMountContinuity::RestoreCrossNode,
            SharedMountAvailability::NodeLocal => SharedMountContinuity::RestartSameNode,
        },
    }
}

impl SharedMountOpts {
    fn into_shared_mount(self) -> SharedMount {
        let availability = shared_mount_availability(self.availability);
        let continuity = shared_mount_continuity(self.continuity, &availability);
        SharedMount {
            host_path: self.host_path.unwrap_or_default(),
            guest_path: self.guest_path,
            mount_tag: self.mount_tag,
            read_only: self.read_only.unwrap_or(false),
            availability,
            continuity,
            backend_profile: self.backend_profile.unwrap_or_default(),
            vfs_endpoint: self.vfs_endpoint.unwrap_or_default(),
            vfs_scope_path: self.vfs_scope_path.unwrap_or_default(),
        }
    }
}

#[napi(object)]
pub struct SessionOpts {
    pub session_id: Option<String>,
    pub name: Option<String>,
    pub image: Option<String>,
    pub architecture: Option<String>,
    pub metadata: Option<HashMap<String, String>>,
    pub auto_start: Option<bool>,
    pub shared_mounts: Option<Vec<SharedMountOpts>>,
    pub egress_allowlist: Option<Vec<String>>,
    pub pci_device_ids: Option<Vec<String>>,
    pub storage_profile: Option<String>,
    pub volume_owner_key: Option<String>,
    pub volume_size_gb: Option<u32>,
}

impl From<SessionOpts> for SessionOptions {
    fn from(o: SessionOpts) -> Self {
        SessionOptions {
            session_id: o.session_id,
            name: o.name,
            image: o.image,
            architecture: o.architecture,
            metadata: o.metadata.unwrap_or_default(),
            auto_start: o.auto_start.unwrap_or(true),
            shared_mounts: o
                .shared_mounts
                .unwrap_or_default()
                .into_iter()
                .map(SharedMountOpts::into_shared_mount)
                .collect(),
            egress_allowlist: o.egress_allowlist,
            pci_device_ids: o.pci_device_ids.unwrap_or_default(),
            storage_profile: o
                .storage_profile
                .unwrap_or_else(|| "local-ephemeral".to_string()),
            volume_owner_key: o.volume_owner_key,
            volume_size_gb: o
                .volume_size_gb
                .and_then(|value| i32::try_from(value).ok())
                .filter(|value| *value > 0),
            ..Default::default()
        }
    }
}

#[napi(object)]
pub struct ForkOpts {
    pub child_name: Option<String>,
    pub child_metadata: Option<HashMap<String, String>>,
    pub auto_start_child: Option<bool>,
}

impl From<ForkOpts> for ForkOptions {
    fn from(o: ForkOpts) -> Self {
        ForkOptions {
            child_name: o.child_name,
            child_metadata: o.child_metadata.unwrap_or_default(),
            auto_start_child: o.auto_start_child.unwrap_or(true),
        }
    }
}

/// A local host-port forward into a guest port. Close it to release the listener.
#[napi]
pub struct ForwardHandle {
    inner: EngineForwardHandle,
}

#[napi]
impl ForwardHandle {
    #[napi(getter)]
    pub fn guest_port(&self) -> u32 {
        self.inner.guest_port.into()
    }

    #[napi(getter)]
    pub fn host_port(&self) -> u32 {
        self.inner.host_port.into()
    }

    #[napi]
    pub async fn close(&self) -> napi::Result<()> {
        self.inner.close().await.map_err(sb_err)
    }
}

// ---------------- session ----------------

/// A sandbox session (one microVM).
#[napi]
pub struct Session {
    inner: EngineSession,
}

/// One entry returned by `Session.listDir`.
#[napi(object)]
pub struct SessionDirectoryEntryJs {
    pub name: String,
    pub is_dir: bool,
    pub is_symlink: bool,
}

#[napi(object)]
pub struct SessionCheckpointJs {
    pub id: String,
}

#[napi(object)]
pub struct SessionSnapshotOpts {
    pub label: Option<String>,
    pub description: Option<String>,
}

#[napi(object)]
pub struct SessionSnapshotJs {
    pub id: String,
    pub name: String,
    pub label: String,
    pub description: String,
}

#[napi(object)]
pub struct SessionInfoJs {
    pub session_id: String,
    pub vm_id: String,
    pub name: String,
    pub state: i32,
    pub parent_session_id: Option<String>,
    pub fork_id: Option<String>,
}

impl From<EngineSessionInfo> for SessionInfoJs {
    fn from(info: EngineSessionInfo) -> Self {
        Self {
            session_id: info.session_id,
            vm_id: info.vm_id,
            name: info.name,
            state: info.state,
            parent_session_id: info.parent_session_id,
            fork_id: info.fork_id,
        }
    }
}

#[napi(object)]
pub struct DurableVolumeInfoJs {
    pub owner_key: String,
    pub volume_id: String,
    pub size_gb: i32,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub backing_volume_id: Option<String>,
    pub attached_vm_ids: Vec<String>,
}

impl From<EngineDurableVolumeInfo> for DurableVolumeInfoJs {
    fn from(info: EngineDurableVolumeInfo) -> Self {
        Self {
            owner_key: info.owner_key,
            volume_id: info.volume_id,
            size_gb: info.size_gb,
            created_at_ms: info.created_at_ms,
            updated_at_ms: info.updated_at_ms,
            backing_volume_id: info.backing_volume_id,
            attached_vm_ids: info.attached_vm_ids,
        }
    }
}

#[napi(object)]
pub struct HostPciFunctionJs {
    pub bdf: String,
    pub vendor_id: String,
    pub device_id: String,
    pub class_code: String,
    pub driver: String,
    pub iommu_group: String,
}

impl From<EngineHostPciFunction> for HostPciFunctionJs {
    fn from(function: EngineHostPciFunction) -> Self {
        Self {
            bdf: function.bdf,
            vendor_id: function.vendor_id,
            device_id: function.device_id,
            class_code: function.class_code,
            driver: function.driver,
            iommu_group: function.iommu_group,
        }
    }
}

#[napi(object)]
pub struct HostPciDeviceJs {
    pub id: String,
    pub label: String,
    pub functions: Vec<HostPciFunctionJs>,
    pub state: String,
    pub assigned_vm_id: String,
    pub managed: bool,
    pub hotplug_capable: bool,
    pub unavailable_reason: String,
}

impl From<EngineHostPciDevice> for HostPciDeviceJs {
    fn from(device: EngineHostPciDevice) -> Self {
        let state = match device.state {
            EngineHostPciDeviceState::Disabled => "disabled",
            EngineHostPciDeviceState::Unavailable => "unavailable",
            EngineHostPciDeviceState::Host => "host",
            EngineHostPciDeviceState::Ready => "ready",
            EngineHostPciDeviceState::Assigned => "assigned",
            EngineHostPciDeviceState::Error => "error",
            EngineHostPciDeviceState::Unknown => "unknown",
        };
        Self {
            id: device.id,
            label: device.label,
            functions: device.functions.into_iter().map(Into::into).collect(),
            state: state.to_string(),
            assigned_vm_id: device.assigned_vm_id,
            managed: device.managed,
            hotplug_capable: device.hotplug_capable,
            unavailable_reason: device.unavailable_reason,
        }
    }
}

#[napi(object)]
pub struct HostPciInventoryJs {
    pub enabled: bool,
    pub devices: Vec<HostPciDeviceJs>,
}

impl From<EngineHostPciInventory> for HostPciInventoryJs {
    fn from(inventory: EngineHostPciInventory) -> Self {
        Self {
            enabled: inventory.enabled,
            devices: inventory.devices.into_iter().map(Into::into).collect(),
        }
    }
}

#[napi(object)]
pub struct PciDeviceActionJs {
    pub device: Option<HostPciDeviceJs>,
    pub restart_required: bool,
    pub detail: String,
    pub vm_state: String,
}

impl From<EnginePciDeviceAction> for PciDeviceActionJs {
    fn from(action: EnginePciDeviceAction) -> Self {
        Self {
            device: action.device.map(Into::into),
            restart_required: action.restart_required,
            detail: action.detail,
            vm_state: vm_state_label(action.vm_state),
        }
    }
}

fn vm_state_label(state: i32) -> String {
    match state {
        1 => "creating",
        2 => "stopped",
        3 => "running",
        4 => "paused",
        5 => "error",
        _ => "unknown",
    }
    .to_string()
}

#[napi]
impl Session {
    #[napi(getter)]
    pub fn session_id(&self) -> String {
        self.inner.session_id().to_string()
    }
    #[napi(getter)]
    pub fn vm_id(&self) -> String {
        self.inner.vm_id().to_string()
    }

    /// Start a command; returns a bidirectional `ExecHandle`.
    #[napi]
    pub async fn exec(
        &self,
        command: String,
        options: Option<ExecOpts>,
    ) -> napi::Result<ExecHandle> {
        let opts = options.map(Into::into).unwrap_or_default();
        let h = self.inner.exec(&command, opts).await.map_err(sb_err)?;
        Ok(ExecHandle {
            input: h.input,
            events: Arc::new(Mutex::new(h.events)),
        })
    }

    /// Start an interactive PTY shell; returns a bidirectional `ShellHandle`.
    #[napi]
    pub async fn shell(&self, options: Option<ShellOpts>) -> napi::Result<ShellHandle> {
        let opts = options.map(Into::into).unwrap_or_default();
        let h = self.inner.shell(opts).await.map_err(sb_err)?;
        Ok(ShellHandle {
            input: h.input,
            events: Arc::new(Mutex::new(h.events)),
        })
    }

    /// Read a file from the guest.
    #[napi]
    pub async fn read_file(&self, path: String) -> napi::Result<Buffer> {
        let b = self.inner.read_file(&path).await.map_err(sb_err)?;
        Ok(Buffer::from(b))
    }

    /// List a directory through the guest's mounted filesystem.
    #[napi]
    pub async fn list_dir(&self, path: String) -> napi::Result<Vec<SessionDirectoryEntryJs>> {
        let entries = self.inner.list_dir(&path).await.map_err(sb_err)?;
        Ok(entries
            .into_iter()
            .map(|entry| SessionDirectoryEntryJs {
                name: entry.name,
                is_dir: entry.is_dir,
                is_symlink: entry.is_symlink,
            })
            .collect())
    }

    /// Write a file to the guest.
    #[napi]
    pub async fn write_file(&self, path: String, data: Buffer) -> napi::Result<()> {
        self.inner
            .write_file(&path, data.to_vec())
            .await
            .map_err(sb_err)
    }

    /// Fork this session (CoW); returns the child session.
    #[napi]
    pub async fn fork(&self, options: Option<ForkOpts>) -> napi::Result<Session> {
        let opts = options.map(Into::into).unwrap_or(ForkOptions {
            child_name: None,
            child_metadata: HashMap::new(),
            auto_start_child: true,
        });
        let r = self.inner.fork(opts).await.map_err(sb_err)?;
        Ok(Session { inner: r.child })
    }

    /// Create a checkpoint/snapshot for this session.
    #[napi]
    pub async fn checkpoint(&self, name: String) -> napi::Result<SessionCheckpointJs> {
        let checkpoint = self.inner.checkpoint(&name).await.map_err(sb_err)?;
        Ok(SessionCheckpointJs { id: checkpoint.id })
    }

    /// Restore a checkpoint into a new session.
    #[napi]
    pub async fn restore_checkpoint(&self, checkpoint_id: String) -> napi::Result<Session> {
        let session = self
            .inner
            .restore_checkpoint(&checkpoint_id)
            .await
            .map_err(sb_err)?;
        Ok(Session { inner: session })
    }

    /// Read the current VM state from the sandbox provider.
    #[napi]
    pub async fn get_state(&self) -> napi::Result<String> {
        self.inner.state().await.map(vm_state_label).map_err(sb_err)
    }

    #[napi]
    pub async fn list_pci_devices(&self) -> napi::Result<HostPciInventoryJs> {
        self.inner
            .list_pci_devices()
            .await
            .map(Into::into)
            .map_err(sb_err)
    }

    #[napi]
    pub async fn attach_pci_device(&self, device_id: String) -> napi::Result<PciDeviceActionJs> {
        self.inner
            .attach_pci_device(&device_id)
            .await
            .map(Into::into)
            .map_err(sb_err)
    }

    #[napi]
    pub async fn detach_pci_device(&self, device_id: String) -> napi::Result<PciDeviceActionJs> {
        self.inner
            .detach_pci_device(&device_id)
            .await
            .map(Into::into)
            .map_err(sb_err)
    }

    /// Pause the VM without deleting it.
    #[napi]
    pub async fn pause(&self) -> napi::Result<String> {
        self.inner.pause().await.map(vm_state_label).map_err(sb_err)
    }

    #[napi]
    pub async fn start(&self) -> napi::Result<String> {
        self.inner.start().await.map(vm_state_label).map_err(sb_err)
    }

    #[napi]
    pub async fn restart(&self) -> napi::Result<String> {
        self.inner
            .restart()
            .await
            .map(vm_state_label)
            .map_err(sb_err)
    }

    /// Resume a paused VM.
    #[napi]
    pub async fn resume(&self) -> napi::Result<String> {
        self.inner
            .resume()
            .await
            .map(vm_state_label)
            .map_err(sb_err)
    }

    /// Stop the VM without deleting its record.
    #[napi]
    pub async fn stop(&self) -> napi::Result<String> {
        self.inner.stop().await.map(vm_state_label).map_err(sb_err)
    }

    /// Create a VM snapshot/checkpoint.
    #[napi]
    pub async fn snapshot(
        &self,
        options: Option<SessionSnapshotOpts>,
    ) -> napi::Result<SessionSnapshotJs> {
        let label = options
            .as_ref()
            .and_then(|opts| opts.label.clone())
            .unwrap_or_else(|| "snapshot".to_string());
        let description = options
            .and_then(|opts| opts.description)
            .unwrap_or_default();
        let snapshot = self
            .inner
            .snapshot(&label, &description)
            .await
            .map_err(sb_err)?;
        Ok(SessionSnapshotJs {
            id: snapshot.id,
            name: snapshot.name,
            label: snapshot.label,
            description: snapshot.description,
        })
    }

    /// Restore this VM from a snapshot.
    #[napi]
    pub async fn restore(&self, snapshot_id: String) -> napi::Result<String> {
        self.inner
            .restore(&snapshot_id)
            .await
            .map(vm_state_label)
            .map_err(sb_err)
    }

    /// List snapshots for this VM.
    #[napi]
    pub async fn list_snapshots(&self) -> napi::Result<Vec<SessionSnapshotJs>> {
        let snapshots = self.inner.list_snapshots().await.map_err(sb_err)?;
        Ok(snapshots
            .into_iter()
            .map(|snapshot| SessionSnapshotJs {
                id: snapshot.id,
                name: snapshot.name,
                label: snapshot.label,
                description: snapshot.description,
            })
            .collect())
    }

    /// Delete a VM snapshot.
    #[napi]
    pub async fn delete_snapshot(&self, snapshot_id: String) -> napi::Result<()> {
        self.inner
            .delete_snapshot(&snapshot_id)
            .await
            .map_err(sb_err)
    }

    /// Forward a guest TCP port to a local host port.
    #[napi]
    pub async fn forward_port(&self, guest_port: u32) -> napi::Result<ForwardHandle> {
        let guest_port = u16::try_from(guest_port)
            .map_err(|_| napi::Error::from_reason("guestPort must fit in uint16"))?;
        let handle = self.inner.forward_port(guest_port).await.map_err(sb_err)?;
        Ok(ForwardHandle { inner: handle })
    }

    /// Return a provider-managed preview URL when the provider supports one.
    #[napi]
    pub async fn provider_preview_url(&self, guest_port: u32) -> napi::Result<String> {
        let guest_port = u16::try_from(guest_port)
            .map_err(|_| napi::Error::from_reason("guestPort must fit in uint16"))?;
        self.inner
            .provider_preview_url(guest_port)
            .await
            .map_err(sb_err)
    }

    /// Close the session handle without deleting the VM.
    #[napi]
    pub async fn close(&self) -> napi::Result<()> {
        self.inner.clone().close().await.map_err(sb_err)
    }

    /// Delete the backing VM/sandbox and purge provider resources.
    #[napi]
    pub async fn discard(&self) -> napi::Result<()> {
        self.inner.clone().discard().await.map_err(sb_err)
    }
}

// ---------------- sandbox ----------------

#[napi(object)]
pub struct SandboxConnectOptions {
    pub auth_token: Option<String>,
    pub pci_access_token: Option<String>,
    pub connect_timeout_ms: Option<f64>,
    pub default_image: Option<String>,
    pub default_architecture: Option<String>,
    pub default_vcpu: Option<u32>,
    pub default_memory_mb: Option<u32>,
    pub default_disk_gb: Option<u32>,
    pub provider: Option<String>,
    pub open_computer: Option<OpenComputerProviderOpts>,
}

fn positive_resource(value: Option<u32>, label: &str) -> Result<Option<i32>, SandboxError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value == 0 {
        return Err(SandboxError::InvalidConfig(format!(
            "{label} must be greater than zero"
        )));
    }
    i32::try_from(value)
        .map(Some)
        .map_err(|_| SandboxError::InvalidConfig(format!("{label} exceeds the supported maximum")))
}

#[napi(object)]
pub struct OpenComputerProviderOpts {
    pub api_url: Option<String>,
    pub api_key: Option<String>,
    pub template_id: Option<String>,
    pub timeout_secs: Option<f64>,
    pub default_cpu_count: Option<u32>,
    pub default_memory_mb: Option<u32>,
    pub default_disk_mb: Option<u32>,
    pub burst: Option<bool>,
    pub secret_store: Option<String>,
    pub egress_allowlist: Option<Vec<String>>,
    pub mounts: Option<Vec<OpenComputerMountOpts>>,
    pub shared_mounts: Option<HashMap<String, OpenComputerMountOpts>>,
}

#[napi(object)]
pub struct OpenComputerMountOpts {
    pub path: Option<String>,
    pub driver: Option<String>,
    pub remote: Option<String>,
    pub backend: Option<String>,
    pub command: Option<Vec<String>>,
    pub env: Option<HashMap<String, String>>,
    pub secrets: Option<HashMap<String, String>>,
    pub creds: Option<HashMap<String, String>>,
    pub rclone_config: Option<String>,
    pub read_only: Option<bool>,
    pub mount_options: Option<Vec<String>>,
}

impl OpenComputerMountOpts {
    fn into_config(self) -> OpenComputerMountConfig {
        OpenComputerMountConfig {
            path: self.path.unwrap_or_default(),
            driver: self.driver,
            remote: self.remote.unwrap_or_default(),
            backend: self.backend,
            command: self.command.unwrap_or_default(),
            env: self.env.unwrap_or_default(),
            secrets: self.secrets.unwrap_or_default(),
            creds: self.creds.unwrap_or_default(),
            rclone_config: self.rclone_config,
            read_only: self.read_only,
            mount_options: self.mount_options.unwrap_or_default(),
        }
    }
}

fn opencomputer_config_from_options(
    options: Option<OpenComputerProviderOpts>,
) -> Result<OpenComputerBackendConfig, SandboxError> {
    let mut cfg = OpenComputerBackendConfig::from_env()?;
    if let Some(options) = options {
        if let Some(api_url) = options.api_url {
            cfg.api_url = api_url;
        }
        if let Some(api_key) = options.api_key {
            cfg.api_key = api_key;
        }
        if let Some(template_id) = options.template_id {
            cfg.template_id = template_id;
        }
        if let Some(timeout_secs) = options.timeout_secs {
            cfg.timeout_secs = timeout_secs as u64;
        }
        cfg.default_cpu_count = options.default_cpu_count.or(cfg.default_cpu_count);
        cfg.default_memory_mb = options.default_memory_mb.or(cfg.default_memory_mb);
        cfg.default_disk_mb = options.default_disk_mb.or(cfg.default_disk_mb);
        cfg.burst = options.burst.or(cfg.burst);
        cfg.secret_store = options.secret_store.or(cfg.secret_store);
        cfg.egress_allowlist = options.egress_allowlist.or(cfg.egress_allowlist);
        if let Some(mounts) = options.mounts {
            cfg.mounts = mounts
                .into_iter()
                .map(OpenComputerMountOpts::into_config)
                .collect();
        }
        if let Some(shared_mounts) = options.shared_mounts {
            cfg.shared_mounts = shared_mounts
                .into_iter()
                .map(|(key, mount)| (key, mount.into_config()))
                .collect();
        }
    }
    Ok(cfg)
}

/// A connection to a Chevalier sandbox provider.
#[napi]
pub struct Sandbox {
    inner: EngineSandbox,
}

#[napi]
impl Sandbox {
    /// Connect to a sandbox provider.
    #[napi(factory)]
    pub async fn connect(
        endpoint: String,
        options: Option<SandboxConnectOptions>,
    ) -> napi::Result<Sandbox> {
        let mut cfg = SandboxConfig {
            endpoint: endpoint.clone(),
            ..Default::default()
        };
        if let Some(o) = options {
            if let Some(t) = o.auth_token {
                cfg.auth_token = Some(t);
            }
            if let Some(token) = o.pci_access_token {
                cfg.pci_access_token = Some(token);
            }
            if let Some(ms) = o.connect_timeout_ms {
                cfg.connect_timeout = Duration::from_millis(ms as u64);
            }
            if let Some(img) = o.default_image {
                cfg.default_image = img;
            }
            if let Some(architecture) = o.default_architecture {
                cfg.default_architecture = Some(architecture);
            }
            cfg.default_resources = ResourceLimits {
                vcpu: positive_resource(o.default_vcpu, "default vCPU count")
                    .map_err(sb_err)?
                    .unwrap_or(cfg.default_resources.vcpu),
                memory_mb: positive_resource(o.default_memory_mb, "default memory MB")
                    .map_err(sb_err)?
                    .unwrap_or(cfg.default_resources.memory_mb),
                disk_gb: positive_resource(o.default_disk_gb, "default disk GB")
                    .map_err(sb_err)?
                    .unwrap_or(cfg.default_resources.disk_gb),
            };
            let provider = o.provider.unwrap_or_else(|| "chevalier".to_string());
            match provider.as_str() {
                "chevalier" | "local" | "vmd" => {}
                "opencomputer" | "open-computer" => {
                    cfg.provider = SandboxProviderConfig::OpenComputer(
                        opencomputer_config_from_options(o.open_computer).map_err(sb_err)?,
                    );
                }
                other => {
                    return Err(sb_err(SandboxError::InvalidConfig(format!(
                        "unsupported sandbox provider `{other}`"
                    ))));
                }
            }
        }
        let sb = EngineSandbox::connect(endpoint, cfg)
            .await
            .map_err(sb_err)?;
        Ok(Sandbox { inner: sb })
    }

    /// Create a new session (microVM).
    #[napi]
    pub async fn session(&self, options: Option<SessionOpts>) -> napi::Result<Session> {
        let opts = options.map(Into::into).unwrap_or_default();
        let s = self.inner.session(opts).await.map_err(sb_err)?;
        Ok(Session { inner: s })
    }

    /// Attach to an existing session by id.
    #[napi]
    pub async fn attach_session(&self, session_id: String) -> napi::Result<Session> {
        let s = self
            .inner
            .attach_session(&session_id)
            .await
            .map_err(sb_err)?;
        Ok(Session { inner: s })
    }

    /// Attach without restoring, starting, or probing the VM.
    #[napi]
    pub async fn attach_session_passive(&self, session_id: String) -> napi::Result<Session> {
        let s = self
            .inner
            .attach_session_passive(&session_id)
            .await
            .map_err(sb_err)?;
        Ok(Session { inner: s })
    }

    /// List live sessions visible to this sandbox provider.
    #[napi]
    pub async fn list_sessions(&self) -> napi::Result<Vec<SessionInfoJs>> {
        let sessions = self.inner.list_sessions().await.map_err(sb_err)?;
        Ok(sessions.into_iter().map(Into::into).collect())
    }

    #[napi]
    pub async fn list_durable_volumes(&self) -> napi::Result<Vec<DurableVolumeInfoJs>> {
        self.inner
            .list_durable_volumes()
            .await
            .map(|volumes| volumes.into_iter().map(Into::into).collect())
            .map_err(sb_err)
    }

    #[napi]
    pub async fn delete_durable_volume(&self, owner_key: String) -> napi::Result<()> {
        self.inner
            .delete_durable_volume(&owner_key)
            .await
            .map_err(sb_err)
    }

    #[napi]
    pub async fn list_host_pci_devices(&self) -> napi::Result<HostPciInventoryJs> {
        self.inner
            .list_host_pci_devices()
            .await
            .map(Into::into)
            .map_err(sb_err)
    }

    /// Discard a provider session by id, even if this process does not hold a Session handle.
    #[napi]
    pub async fn discard_session_by_id(&self, session_id: String) -> napi::Result<()> {
        self.inner
            .discard_session_by_id(&session_id)
            .await
            .map_err(sb_err)
    }
}
