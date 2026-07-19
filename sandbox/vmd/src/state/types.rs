// @dive-file: Core VM metadata/type definitions shared across manager and API conversion layers.
// @dive-rel: Used by vmd state manager and protobuf translation paths in app/ctl code.
// @dive-rel: Defines durable and runtime-adjacent structures for VM state and snapshot lineage.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

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
#[serde(rename_all = "lowercase")]
pub enum VmSourceType {
    Docker,
    Snapshot,
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
    pub dir: PathBuf,
}

impl Vm {
    pub fn new(metadata: VmMetadata, runtime: VmRuntime, dir: PathBuf) -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(VmInner { metadata, runtime })),
            dir,
        }
    }

    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, VmInner> {
        self.inner.lock().await
    }

    pub async fn lock_owned(&self) -> tokio::sync::OwnedMutexGuard<VmInner> {
        self.inner.clone().lock_owned().await
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

#[derive(Debug, Clone)]
pub struct CreateVmParams {
    pub name: String,
    pub source: VmSource,
    pub resources: ResourceSpec,
    pub metadata: HashMap<String, String>,
    pub auto_start: bool,
    pub architecture: String,
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
