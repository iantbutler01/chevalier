// @dive-file: Core VM metadata/type definitions shared across manager and API conversion layers.
// @dive-rel: Used by vmd state manager and protobuf translation paths in app/ctl code.
// @dive-rel: Defines durable and runtime-adjacent structures for VM state and snapshot lineage.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::pci::PciDeviceAssignmentSpec;
use crate::state::runtime::VmRuntime;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum VmState {
    Creating,
    Stopped,
    Running,
    Paused,
    Error,
}

impl Default for VmState {
    fn default() -> Self {
        VmState::Stopped
    }
}

impl VmState {
    pub fn as_str(&self) -> &'static str {
        match self {
            VmState::Creating => "creating",
            VmState::Stopped => "stopped",
            VmState::Running => "running",
            VmState::Paused => "paused",
            VmState::Error => "error",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum VmSourceType {
    Docker,
    Snapshot,
    MacosTemplate,
    WindowsTemplate,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum GuestPlatform {
    #[default]
    Linux,
    Macos,
    Windows,
}

impl GuestPlatform {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Linux => "linux",
            Self::Macos => "macos",
            Self::Windows => "windows",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum WorkspaceTransport {
    VirtioFs,
    MacfuseFskit,
    Winfsp,
}

impl Default for WorkspaceTransport {
    fn default() -> Self {
        Self::VirtioFs
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum WorkspaceMode {
    OwnerAndObservers,
    OwnerOnly,
}

impl Default for WorkspaceMode {
    fn default() -> Self {
        Self::OwnerAndObservers
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum NetworkPolicyMode {
    TapTransparentProxy,
    NoNicVsockProxy,
    NoNicIsolated,
    QemuUserNetworking,
}

impl Default for NetworkPolicyMode {
    fn default() -> Self {
        Self::TapTransparentProxy
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GuestProfile {
    #[serde(default)]
    pub platform: GuestPlatform,
    #[serde(default)]
    pub architecture: String,
    #[serde(default)]
    pub os_version: String,
    #[serde(default)]
    pub os_build: String,
    #[serde(default)]
    pub template_id: String,
    #[serde(default)]
    pub template_digest: String,
    #[serde(default)]
    pub machine_profile: String,
    #[serde(default)]
    pub schema_version: u32,
    #[serde(default)]
    pub minimum_host_version: String,
}

impl Default for GuestProfile {
    fn default() -> Self {
        Self {
            platform: GuestPlatform::Linux,
            architecture: String::new(),
            os_version: String::new(),
            os_build: String::new(),
            template_id: String::new(),
            template_digest: String::new(),
            machine_profile: String::new(),
            schema_version: 0,
            minimum_host_version: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GuestRuntime {
    #[serde(default)]
    pub platform: GuestPlatform,
    #[serde(default)]
    pub architecture: String,
    #[serde(default = "linux_home_dir")]
    pub home_dir: String,
    #[serde(default = "linux_workspace_root")]
    pub workspace_root: String,
    #[serde(default)]
    pub workspace_alias: Option<String>,
    #[serde(default = "linux_temp_dir")]
    pub temp_dir: String,
    #[serde(default = "linux_runtime_dir")]
    pub runtime_dir: String,
    #[serde(default = "linux_environment_file_root")]
    pub environment_file_root: String,
    #[serde(default = "linux_default_shell")]
    pub default_shell: String,
    #[serde(default = "linux_service_manager")]
    pub service_manager: String,
}

impl Default for GuestRuntime {
    fn default() -> Self {
        Self {
            platform: GuestPlatform::Linux,
            architecture: String::new(),
            home_dir: linux_home_dir(),
            workspace_root: linux_workspace_root(),
            workspace_alias: None,
            temp_dir: linux_temp_dir(),
            runtime_dir: linux_runtime_dir(),
            environment_file_root: linux_environment_file_root(),
            default_shell: linux_default_shell(),
            service_manager: linux_service_manager(),
        }
    }
}

fn linux_home_dir() -> String {
    "/root".to_string()
}

fn linux_workspace_root() -> String {
    "/workspace".to_string()
}

fn linux_temp_dir() -> String {
    "/tmp".to_string()
}

fn linux_runtime_dir() -> String {
    "/run/openbracket".to_string()
}

fn linux_environment_file_root() -> String {
    "/run/openbracket/env".to_string()
}

fn linux_default_shell() -> String {
    "/bin/bash".to_string()
}

fn linux_service_manager() -> String {
    "systemd".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VmCapabilities {
    #[serde(default)]
    pub workspace_transport: WorkspaceTransport,
    #[serde(default)]
    pub workspace_mode: WorkspaceMode,
    #[serde(default)]
    pub network_policy_mode: NetworkPolicyMode,
    #[serde(default = "legacy_linux_capability_enabled")]
    pub durable_volume: bool,
    #[serde(default = "legacy_linux_capability_enabled")]
    pub docker: bool,
    #[serde(default = "legacy_linux_capability_enabled")]
    pub managed_services: bool,
    #[serde(default = "legacy_linux_capability_enabled")]
    pub pause_resume: bool,
    #[serde(default = "legacy_linux_capability_enabled")]
    pub cold_checkpoint: bool,
    #[serde(default = "legacy_linux_capability_enabled")]
    pub same_host_saved_state: bool,
    #[serde(default = "legacy_linux_capability_enabled")]
    pub stopped_fork: bool,
    #[serde(default = "legacy_linux_capability_enabled")]
    pub running_fork: bool,
    #[serde(default = "legacy_linux_capability_enabled")]
    pub computer_use: bool,
    #[serde(default = "legacy_linux_capability_enabled")]
    pub pci: bool,
    #[serde(default = "legacy_linux_capability_enabled")]
    pub cross_node_restore: bool,
}

impl Default for VmCapabilities {
    fn default() -> Self {
        Self {
            workspace_transport: WorkspaceTransport::VirtioFs,
            workspace_mode: WorkspaceMode::OwnerAndObservers,
            network_policy_mode: NetworkPolicyMode::TapTransparentProxy,
            durable_volume: true,
            docker: true,
            managed_services: true,
            pause_resume: true,
            cold_checkpoint: true,
            same_host_saved_state: true,
            stopped_fork: true,
            running_fork: true,
            computer_use: true,
            pci: true,
            cross_node_restore: true,
        }
    }
}

fn legacy_linux_capability_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VmSource {
    #[serde(rename = "type")]
    pub source_type: VmSourceType,
    pub reference: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceSpec {
    pub vcpu: i32,
    #[serde(rename = "memory_mb")]
    pub memory_mb: i32,
    #[serde(rename = "disk_gb")]
    pub disk_gb: i32,
}

impl Default for ResourceSpec {
    fn default() -> Self {
        Self {
            vcpu: 1,
            memory_mb: 1024,
            disk_gb: 10,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkSpec {
    pub mac: String,
    #[serde(default)]
    pub proxy_port: i32,
    #[serde(default)]
    pub rpc_port: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum SharedMountAvailability {
    NodeLocal,
    SharedStorage,
}

impl Default for SharedMountAvailability {
    fn default() -> Self {
        Self::NodeLocal
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum SharedMountContinuity {
    RestartSameNode,
    RestoreCrossNode,
}

impl Default for SharedMountContinuity {
    fn default() -> Self {
        Self::RestartSameNode
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedMountSpec {
    pub host_path: String,
    pub guest_path: String,
    #[serde(default)]
    pub mount_tag: String,
    #[serde(default)]
    pub read_only: bool,
    #[serde(default)]
    pub availability: SharedMountAvailability,
    #[serde(default)]
    pub continuity: SharedMountContinuity,
    #[serde(default)]
    pub backend_profile: String,
    #[serde(default)]
    pub vfs_endpoint: String,
    #[serde(default)]
    pub vfs_scope_path: String,
}

impl SharedMountSpec {
    pub fn is_fuse_backed(&self) -> bool {
        !self.vfs_endpoint.trim().is_empty()
    }
}

/// On-disk snapshot record.
///
/// Every snapshot is a live background snapshot produced by [`virt::save_vm_background`].
/// The disk state is pinned via a `blockdev-snapshot-internal-sync` internal snapshot
/// sharing the same `name`, and the RAM state is written as an external file at
/// `<vm_dir>/snapshots/<ram_file_name>`. Restored by reverting the disk snapshot offline
/// (`qemu-img snapshot -a`) and relaunching qemu with `-incoming file:<ram_path>`. See
/// `state::manager::restore_snapshot` for the pairing logic.
///
/// There is intentionally no "disk-only" or offline snapshot mode — snapshots exist to
/// resume a live VM, not to mark the state of a stopped one.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub description: String,
    #[serde(with = "iso8601")]
    pub created_at: DateTime<Utc>,
    /// Filename of the external RAM state file under `<vm_dir>/snapshots/`. Required.
    pub ram_file_name: String,
    /// QEMU RAM migration file format. Only `mapped-ram` is restorable; empty or older
    /// values are retained in metadata only so callers can reject and cold-start safely.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ram_format: String,
    /// Fingerprint of the guest runtime/bootstrap payload active when the RAM
    /// image was captured. Missing/mismatched values must not RAM-restore.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guest_runtime_fingerprint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VmMetadata {
    pub id: String,
    pub name: String,
    #[serde(with = "iso8601")]
    pub created_at: DateTime<Utc>,
    #[serde(with = "iso8601")]
    pub updated_at: DateTime<Utc>,
    pub state: VmState,
    #[serde(default)]
    pub architecture: String,
    #[serde(default)]
    pub guest_profile: GuestProfile,
    #[serde(default)]
    pub guest_runtime: GuestRuntime,
    #[serde(default)]
    pub capabilities: VmCapabilities,
    pub source: VmSource,
    pub resources: ResourceSpec,
    pub network: NetworkSpec,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
    #[serde(default)]
    pub snapshots: Vec<SnapshotMetadata>,
    #[serde(default)]
    pub shared_mounts: Vec<SharedMountSpec>,
    #[serde(default)]
    pub pci_devices: Vec<PciDeviceAssignmentSpec>,
    /// Optional machine-state disk that is owned independently from this VM.
    ///
    /// The volume lives under the daemon data directory and is retained when
    /// the VM is deleted. Persisting only the stable identity/size here keeps
    /// VM metadata portable while allowing the manager to derive the host path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub durable_volume: Option<DurableVolumeAttachment>,
    /// Absolute path of an external RAM file for the next `start_vm` to consume via
    /// `-incoming file:<path>`. Set transiently by `restore_snapshot` (and by fork Path
    /// A for both parent and child) right before the launch, cleared after the launch
    /// completes so subsequent boots of the VM don't loop back to the same RAM file.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub boot_incoming_ram_path: String,
    #[serde(default, with = "iso8601::option")]
    pub started_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DurableVolumeAttachment {
    pub owner_key: String,
    pub volume_id: String,
    pub size_gb: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DurableVolumeMetadata {
    pub owner_key: String,
    pub volume_id: String,
    pub size_gb: i32,
    #[serde(with = "iso8601")]
    pub created_at: DateTime<Utc>,
    #[serde(with = "iso8601")]
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backing_volume_id: Option<String>,
}

impl VmMetadata {
    pub fn snapshot_dir(&self, vm_dir: &PathBuf) -> PathBuf {
        vm_dir.join("snapshots")
    }
}

#[derive(Debug)]
pub struct VmInner {
    pub metadata: VmMetadata,
    pub runtime: VmRuntime,
}

#[derive(Debug)]
pub struct Vm {
    inner: Arc<tokio::sync::Mutex<VmInner>>,
    launch: tokio::sync::Mutex<()>,
    runtime_teardown: tokio::sync::Mutex<()>,
    delete_requested: AtomicBool,
    pub dir: PathBuf,
}

pub struct VmDeleteGuard<'a> {
    vm: &'a Vm,
    completed: bool,
}

impl VmDeleteGuard<'_> {
    pub fn complete(mut self) {
        self.completed = true;
    }
}

impl Drop for VmDeleteGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.vm.delete_requested.store(false, Ordering::Release);
        }
    }
}

impl Vm {
    pub fn new(metadata: VmMetadata, runtime: VmRuntime, dir: PathBuf) -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(VmInner { metadata, runtime })),
            launch: tokio::sync::Mutex::new(()),
            runtime_teardown: tokio::sync::Mutex::new(()),
            delete_requested: AtomicBool::new(false),
            dir,
        }
    }

    /// Serializes the complete launch lifecycle for this VM.
    ///
    /// Launch allocates VM-scoped sidecars and network policy that cleanup also
    /// addresses by VM id. Allowing two launch attempts to overlap lets a losing
    /// attempt unregister resources owned by the winner.
    pub async fn lock_launch(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.launch.lock().await
    }

    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, VmInner> {
        self.inner.lock().await
    }

    pub fn try_lock(
        &self,
    ) -> Result<tokio::sync::MutexGuard<'_, VmInner>, tokio::sync::TryLockError> {
        self.inner.try_lock()
    }

    pub async fn lock_owned(&self) -> tokio::sync::OwnedMutexGuard<VmInner> {
        self.inner.clone().lock_owned().await
    }

    pub async fn lock_runtime_teardown(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.runtime_teardown.lock().await
    }

    pub fn begin_delete(&self) -> Option<VmDeleteGuard<'_>> {
        self.delete_requested
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| VmDeleteGuard {
                vm: self,
                completed: false,
            })
    }

    pub fn delete_requested(&self) -> bool {
        self.delete_requested.load(Ordering::Acquire)
    }

    pub fn disk_path(&self) -> PathBuf {
        self.dir.join("disk.qcow2")
    }
}

#[derive(Clone, Debug)]
pub struct SnapshotRecord {
    pub vm_id: String,
    pub snapshot: SnapshotMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateVmParams {
    pub name: String,
    pub source: VmSource,
    pub resources: ResourceSpec,
    pub metadata: HashMap<String, String>,
    pub auto_start: bool,
    pub architecture: String,
    #[serde(default)]
    pub guest_profile: GuestProfile,
    #[serde(default)]
    pub guest_runtime: GuestRuntime,
    #[serde(default)]
    pub capabilities: VmCapabilities,
    pub shared_mounts: Vec<SharedMountSpec>,
    pub pci_device_ids: Vec<String>,
    pub storage_profile: String,
    pub volume_owner_key: Option<String>,
    pub volume_size_gb: Option<i32>,
}

#[derive(Debug, Clone, Default)]
pub struct UpdateVmParams {
    pub name: Option<String>,
    pub metadata: Option<HashMap<String, String>>,
    pub resources: Option<ResourceSpec>,
    pub shared_mounts: Option<Vec<SharedMountSpec>>,
}

#[derive(Debug, Clone, Default)]
pub struct ForkVmParams {
    pub child_name: Option<String>,
    pub child_metadata: HashMap<String, String>,
    pub auto_start_child: bool,
}

pub mod iso8601 {
    use chrono::{DateTime, SecondsFormat, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_rfc3339_opts(SecondsFormat::Millis, true))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        DateTime::parse_from_rfc3339(&s)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(serde::de::Error::custom)
    }

    pub mod option {
        use chrono::{DateTime, SecondsFormat, Utc};
        use serde::{self, Deserialize, Deserializer, Serializer};

        pub fn serialize<S>(value: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            match value {
                Some(ts) => {
                    serializer.serialize_some(&ts.to_rfc3339_opts(SecondsFormat::Millis, true))
                }
                None => serializer.serialize_none(),
            }
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
        where
            D: Deserializer<'de>,
        {
            let opt = Option::<String>::deserialize(deserializer)?;
            match opt {
                Some(s) => {
                    let dt = DateTime::parse_from_rfc3339(&s)
                        .map(|dt| dt.with_timezone(&Utc))
                        .map_err(serde::de::Error::custom)?;
                    Ok(Some(dt))
                }
                None => Ok(None),
            }
        }
    }
}

pub fn new_snapshot_metadata(label: String, description: String) -> SnapshotMetadata {
    let id = Uuid::new_v4().to_string();
    let name = format!("snap-{id}");
    let ram_file_name = format!("{name}.ram");
    SnapshotMetadata {
        id,
        name,
        label,
        description,
        created_at: Utc::now(),
        ram_file_name,
        ram_format: String::new(),
        guest_runtime_fingerprint: None,
    }
}

pub fn sanitize_name(input: &str) -> String {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        format!(
            "vm-{}",
            Uuid::new_v4().to_string().split('-').next().unwrap()
        )
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn legacy_vm_metadata_defaults_to_linux_guest_model() {
        let metadata: VmMetadata = serde_json::from_value(json!({
            "id": "vm-legacy",
            "name": "legacy",
            "created_at": "2026-08-09T00:00:00.000Z",
            "updated_at": "2026-08-09T00:00:00.000Z",
            "state": "stopped",
            "source": {
                "type": "docker",
                "reference": "docker.io/library/alpine:latest"
            },
            "resources": {
                "vcpu": 2,
                "memory_mb": 2048,
                "disk_gb": 20
            },
            "network": {
                "mac": "02:00:00:00:00:01"
            }
        }))
        .expect("legacy metadata should deserialize");

        assert_eq!(metadata.guest_profile.platform, GuestPlatform::Linux);
        assert_eq!(metadata.guest_runtime, GuestRuntime::default());
        assert_eq!(metadata.capabilities, VmCapabilities::default());
        assert_eq!(metadata.guest_runtime.workspace_root, "/workspace");
        assert_eq!(
            metadata.capabilities.workspace_transport,
            WorkspaceTransport::VirtioFs
        );
    }

    #[test]
    fn legacy_create_params_default_to_linux_guest_model() {
        let params: CreateVmParams = serde_json::from_value(json!({
            "name": "legacy",
            "source": {
                "type": "docker",
                "reference": "docker.io/library/alpine:latest"
            },
            "resources": {
                "vcpu": 2,
                "memory_mb": 2048,
                "disk_gb": 20
            },
            "metadata": {},
            "auto_start": false,
            "architecture": "x86_64",
            "shared_mounts": [],
            "pci_device_ids": [],
            "storage_profile": "ephemeral",
            "volume_owner_key": null,
            "volume_size_gb": null
        }))
        .expect("legacy create parameters should deserialize");

        assert_eq!(params.guest_profile.platform, GuestPlatform::Linux);
        assert_eq!(params.guest_runtime.platform, GuestPlatform::Linux);
        assert!(params.capabilities.docker);
        assert!(params.capabilities.running_fork);
    }

    #[test]
    fn macos_template_source_uses_stable_kebab_case_metadata_value() {
        assert_eq!(
            serde_json::to_value(VmSourceType::MacosTemplate).unwrap(),
            json!("macos-template")
        );
    }

    #[test]
    fn vm_delete_guard_clears_failed_attempt_and_latches_completed_delete() {
        let metadata: VmMetadata = serde_json::from_value(json!({
            "id": "vm-delete-guard",
            "name": "delete-guard",
            "created_at": "2026-08-27T00:00:00.000Z",
            "updated_at": "2026-08-27T00:00:00.000Z",
            "state": "stopped",
            "source": {
                "type": "docker",
                "reference": "docker.io/library/alpine:latest"
            },
            "resources": {
                "vcpu": 2,
                "memory_mb": 2048,
                "disk_gb": 20
            },
            "network": {
                "mac": "02:00:00:00:00:02"
            }
        }))
        .expect("test metadata should deserialize");
        let vm_dir = PathBuf::from("/tmp/vm-delete-guard");
        let vm = Vm::new(metadata, VmRuntime::new(&vm_dir), vm_dir);

        let failed_attempt = vm.begin_delete().expect("first delete should start");
        assert!(vm.delete_requested());
        assert!(vm.begin_delete().is_none());
        drop(failed_attempt);
        assert!(!vm.delete_requested());

        let completed_attempt = vm.begin_delete().expect("retry should start");
        completed_attempt.complete();
        assert!(vm.delete_requested());
    }
}
