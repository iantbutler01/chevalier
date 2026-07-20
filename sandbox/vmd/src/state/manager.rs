// @dive-file: Core VM lifecycle manager handling create/start/stop/snapshot/fork flows and on-disk metadata invariants.
// @dive-rel: Consumes policy limits from vmd/src/config.rs and enforces them on mutating VM operations.
// @dive-rel: Implements fork CoW lineage behavior that underpins facade-level Session::fork semantics.
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use rand::RngCore;
use serde_json::json;
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};
use tokio::time::sleep;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use crate::assets::portproxy;
use crate::bootstrap;
use crate::config::{self, Config};
use crate::fuse;
use crate::image::{self, BASE_IMAGE_EXT, BASE_IMAGE_SIZE_GB, PrebuiltImageStatus};
use crate::network;
use crate::pci::{self, PciDeviceAssignmentSpec, PciInventoryDevice, PciInventoryState};
use crate::state::metadata::{
    load_metadata, load_volume_metadata, save_metadata, save_volume_metadata, volume_metadata_path,
};
use crate::state::runtime::VmRuntime;
use crate::state::types::{
    CreateVmParams, DurableVolumeAttachment, DurableVolumeMetadata, ForkVmParams, NetworkSpec,
    SharedMountAvailability, SharedMountContinuity, SharedMountSpec, SnapshotMetadata,
    SnapshotRecord, UpdateVmParams, Vm, VmInner, VmMetadata, VmSource, VmSourceType, VmState,
    new_snapshot_metadata, sanitize_name,
};
use crate::virt;

const ARCH_AMD64: &str = "amd64";
const ARCH_ARM64: &str = "arm64";
const FORK_BASES_DIR_NAME: &str = "_fork_bases";
const VOLUMES_DIR_NAME: &str = "volumes";
const VOLUME_FORK_BASES_DIR_NAME: &str = "_fork_bases";
const META_FORK_ID: &str = "chevalier.fork_id";
const META_SESSION_ID: &str = "chevalier.session_id";
const META_PARENT_SESSION_ID: &str = "chevalier.parent_session_id";
const META_FORK_BASE_PATH: &str = "chevalier.fork_base_path";
const META_PARENT_VM_ID: &str = "chevalier.parent_vm_id";
const META_FORK_SNAPSHOT: &str = "chevalier.fork_snapshot";
const META_EXEC_RESTORE_SNAPSHOT_ID: &str = "chevalier.execution_restore_snapshot_id";
const META_EXEC_RESTORE_SNAPSHOT_NAME: &str = "chevalier.execution_restore_snapshot_name";
const META_FORK_DEPTH: &str = "chevalier.fork_depth";
const META_STORAGE_PROFILE: &str = "chevalier.storage_profile";
const META_FORK_DURABILITY_CLASS: &str = "chevalier.fork_durability_class";
const META_FORK_RESTORE_SCOPE: &str = "chevalier.fork_restore_scope";
const META_TIER_B_ELIGIBLE: &str = "chevalier.tier_b_eligible";
const META_EXECUTION_FIDELITY_REQUIREMENT: &str = "chevalier.execution_fidelity_requirement";
const META_TENANT_ID: &str = "tenant_id";
const META_WORKSPACE_ID: &str = "workspace_id";
const META_NETWORK_POLICY: &str = "chevalier.network_policy";
const META_NETWORK_POLICY_PROXY_UPSTREAM: &str = "chevalier.network_policy_proxy_upstream";
const META_NETWORK_EGRESS_SNAPSHOT: &str = "chevalier.network_egress";
const META_PORTPROXY_AUTH_TOKEN: &str = "chevalier.portproxy_auth_token";
const MAINTENANCE_DIR_NAME: &str = "_maintenance";
const FORK_COMPACTION_QUEUE_DIR_NAME: &str = "fork_compaction_queue";
const MAX_VM_VCPU: i32 = 8;
const MAX_VM_MEMORY_MB: i32 = 16 * 1024;
const MAX_VM_DISK_GB: i32 = 100;
const VM_RUNNING_TIMEOUT: Duration = Duration::from_secs(60);
const INCOMING_RESTORE_TOTAL_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const INCOMING_RESTORE_STALL_TIMEOUT: Duration = Duration::from_secs(90);
const AMD64_QEMU_RUNTIME_FINGERPRINT: &str = "qemu-amd64-apic-vapic-off-v1";
const ARM64_BIOS_CANDIDATES: [&str; 6] = [
    "/usr/share/qemu/edk2-aarch64-code.fd",
    "/usr/share/AAVMF/AAVMF_CODE.fd",
    "/usr/share/qemu-efi-aarch64/QEMU_EFI.fd",
    "/opt/homebrew/opt/qemu/share/qemu/edk2-aarch64-code.fd",
    "/usr/local/opt/qemu/share/qemu/edk2-aarch64-code.fd",
    "/Applications/UTM.app/Contents/Resources/qemu/edk2-aarch64-code.fd",
];

#[derive(Clone, Copy, Debug)]
struct VmResourceBounds {
    max_vcpu: i32,
    max_memory_mb: i32,
    max_disk_gb: i32,
}

impl Default for VmResourceBounds {
    fn default() -> Self {
        Self {
            max_vcpu: MAX_VM_VCPU,
            max_memory_mb: MAX_VM_MEMORY_MB,
            max_disk_gb: MAX_VM_DISK_GB,
        }
    }
}

impl VmResourceBounds {
    fn from_env() -> Result<Self> {
        Ok(Self {
            max_vcpu: positive_resource_bound_from_env(
                "CHEVALIER_SANDBOX_MAX_VM_VCPU",
                "BRACKET_SANDBOX_MAX_VM_VCPU",
                MAX_VM_VCPU,
            )?,
            max_memory_mb: positive_resource_bound_from_env(
                "CHEVALIER_SANDBOX_MAX_VM_MEMORY_MB",
                "BRACKET_SANDBOX_MAX_VM_MEMORY_MB",
                MAX_VM_MEMORY_MB,
            )?,
            max_disk_gb: positive_resource_bound_from_env(
                "CHEVALIER_SANDBOX_MAX_VM_DISK_GB",
                "BRACKET_SANDBOX_MAX_VM_DISK_GB",
                MAX_VM_DISK_GB,
            )?,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub enum CreateVmStage {
    DownloadImage,
    ConvertImage,
    StartVm,
}

#[derive(Clone, Debug)]
pub enum CreateVmProgressEvent {
    DownloadBytes {
        downloaded_bytes: u64,
        total_bytes: Option<u64>,
    },
    StageProgress {
        stage: CreateVmStage,
        percent: u32,
        message: Option<String>,
    },
}

pub type CreateVmProgressCallback = Arc<dyn Fn(CreateVmProgressEvent) + Send + Sync>;

#[derive(thiserror::Error, Debug)]
pub enum ManagerError {
    #[error("vm not found")]
    VmNotFound,
    #[error("snapshot not found")]
    SnapshotNotFound,
    #[error("durable volume not found")]
    VolumeNotFound,
    #[error("durable volume is attached to VM(s): {0}")]
    VolumeInUse(String),
    #[error("invalid VM state for operation")]
    InvalidState,
    #[error("operation cancelled")]
    Cancelled,
    #[error("PCI conflict: {0}")]
    PciConflict(String),
    #[error("PCI unavailable: {0}")]
    PciUnavailable(String),
    #[error("unsupported operation: {0}")]
    Unsupported(String),
    #[error("capacity exceeded for {resource}: limit={limit} current={current}")]
    CapacityExceeded {
        resource: &'static str,
        limit: usize,
        current: usize,
    },
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type ManagerResult<T> = Result<T, ManagerError>;

#[derive(Clone, Debug)]
pub struct VmHealthProbeTarget {
    pub vm_id: String,
    pub rpc_port: i32,
    pub portproxy_auth_token: Option<String>,
    pub started_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct SnapshotParams {
    pub label: String,
    pub description: String,
}

/// Handle returned by `Manager::create_snapshot_qemu_phase` describing where
/// qemu wrote the live RAM image (`staging_ram_path` if a staging dir is
/// configured, otherwise None) and where it must end up on durable shared
/// storage (`canonical_ram_path`). Pass to `Manager::promote_staged_snapshot`
/// to finish the snapshot — the second call performs the slow Filestore copy
/// and persists snapshot metadata, and is safe to run without holding any
/// per-vm wait-gate.
pub struct PendingSnapshot {
    pub meta: SnapshotMetadata,
    pub staging_ram_path: Option<std::path::PathBuf>,
    pub canonical_ram_path: std::path::PathBuf,
}

pub struct Manager {
    cfg: Config,
    host_arch: String,
    resource_bounds: VmResourceBounds,
    base_image_locks: Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>,
    vms: RwLock<HashMap<String, Arc<Vm>>>,
    vm_capacity_lock: Mutex<()>,
    session_registry: RwLock<HashMap<String, String>>,
    snapshots: RwLock<HashMap<String, SnapshotRecord>>,
    volumes: RwLock<HashMap<String, DurableVolumeMetadata>>,
    volume_operation_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    pci_leases: Mutex<HashMap<String, String>>,
}

#[derive(Clone, Debug)]
pub struct PciActionResult {
    pub metadata: VmMetadata,
    pub restart_required: bool,
    pub detail: String,
}

impl Manager {
    #[instrument(skip(cfg))]
    pub async fn new(mut cfg: Config) -> ManagerResult<Self> {
        cfg.normalize().map_err(ManagerError::Other)?;
        let host_arch = normalize_arch(std::env::consts::ARCH)?;
        let resource_bounds = VmResourceBounds::from_env().map_err(ManagerError::Other)?;
        info!(
            max_vcpu = resource_bounds.max_vcpu,
            max_memory_mb = resource_bounds.max_memory_mb,
            max_disk_gb = resource_bounds.max_disk_gb,
            "configured per-VM resource admission bounds"
        );

        let manager = Self {
            cfg,
            host_arch,
            resource_bounds,
            base_image_locks: Mutex::new(HashMap::new()),
            vms: RwLock::new(HashMap::new()),
            vm_capacity_lock: Mutex::new(()),
            session_registry: RwLock::new(HashMap::new()),
            snapshots: RwLock::new(HashMap::new()),
            volumes: RwLock::new(HashMap::new()),
            volume_operation_locks: Mutex::new(HashMap::new()),
            pci_leases: Mutex::new(HashMap::new()),
        };

        manager.discover_volumes().await?;
        manager.discover().await?;
        Ok(manager)
    }

    pub fn host_architecture(&self) -> &str {
        &self.host_arch
    }

    pub fn normalize_architecture(&self, arch: &str) -> ManagerResult<String> {
        normalize_arch(arch)
    }

    pub async fn lock_base_image(&self, path: &Path) -> OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self.base_image_locks.lock().await;
            Arc::clone(
                locks
                    .entry(path.to_path_buf())
                    .or_insert_with(|| Arc::new(Mutex::new(()))),
            )
        };
        lock.lock_owned().await
    }

    async fn lock_volume_operations(&self, owner_keys: &[&str]) -> Vec<OwnedMutexGuard<()>> {
        let mut sorted_owner_keys = owner_keys.to_vec();
        sorted_owner_keys.sort_unstable();
        sorted_owner_keys.dedup();
        let locks = {
            let mut operation_locks = self.volume_operation_locks.lock().await;
            sorted_owner_keys
                .into_iter()
                .map(|owner_key| {
                    Arc::clone(
                        operation_locks
                            .entry(owner_key.to_string())
                            .or_insert_with(|| Arc::new(Mutex::new(()))),
                    )
                })
                .collect::<Vec<_>>()
        };
        let mut guards = Vec::with_capacity(locks.len());
        for lock in locks {
            guards.push(lock.lock_owned().await);
        }
        guards
    }

    async fn metadata_with_runtime_network_snapshot(&self, mut metadata: VmMetadata) -> VmMetadata {
        match network::vm_proxy_activity_snapshot(&metadata.id, 50).await {
            Ok(Some(snapshot)) => match serde_json::to_string(&snapshot) {
                Ok(value) => {
                    metadata
                        .metadata
                        .insert(META_NETWORK_EGRESS_SNAPSHOT.to_string(), value);
                }
                Err(err) => {
                    warn!(
                        vm_id = %metadata.id,
                        error = %err,
                        "failed serializing runtime network snapshot"
                    );
                    metadata.metadata.remove(META_NETWORK_EGRESS_SNAPSHOT);
                }
            },
            Ok(None) => {
                metadata.metadata.remove(META_NETWORK_EGRESS_SNAPSHOT);
            }
            Err(err) => {
                warn!(
                    vm_id = %metadata.id,
                    error = %err,
                    "failed loading runtime network snapshot"
                );
                metadata.metadata.remove(META_NETWORK_EGRESS_SNAPSHOT);
            }
        }
        metadata
    }

    fn preserve_runtime_managed_metadata(
        existing: &std::collections::HashMap<String, String>,
        updated: &mut std::collections::HashMap<String, String>,
    ) {
        for key in [
            META_SESSION_ID,
            META_PARENT_SESSION_ID,
            META_PARENT_VM_ID,
            META_FORK_ID,
            META_FORK_SNAPSHOT,
            META_FORK_DEPTH,
            META_STORAGE_PROFILE,
            META_FORK_DURABILITY_CLASS,
            META_FORK_RESTORE_SCOPE,
            META_TIER_B_ELIGIBLE,
            META_EXECUTION_FIDELITY_REQUIREMENT,
            META_TENANT_ID,
            META_WORKSPACE_ID,
        ] {
            if !updated.contains_key(key) {
                if let Some(value) = existing.get(key) {
                    updated.insert(key.to_string(), value.clone());
                }
            }
        }
        updated.remove(META_PORTPROXY_AUTH_TOKEN);
        if let Some(value) = existing.get(META_PORTPROXY_AUTH_TOKEN) {
            updated.insert(META_PORTPROXY_AUTH_TOKEN.to_string(), value.clone());
        }
        if let Some(value) = existing.get(META_NETWORK_POLICY_PROXY_UPSTREAM) {
            updated.insert(
                META_NETWORK_POLICY_PROXY_UPSTREAM.to_string(),
                value.clone(),
            );
        }
    }

    async fn reapply_runtime_network_policy(
        &self,
        vm_id: &str,
        metadata: &std::collections::HashMap<String, String>,
        runtime_state: VmState,
    ) -> ManagerResult<()> {
        if !matches!(runtime_state, VmState::Running | VmState::Paused) {
            return Ok(());
        }

        let Some(upstream_addr) = metadata.get(META_NETWORK_POLICY_PROXY_UPSTREAM) else {
            return Ok(());
        };
        let listen_addr = upstream_addr
            .parse()
            .with_context(|| format!("parse vm proxy upstream addr {upstream_addr}"))
            .map_err(ManagerError::Other)?;

        let policy = match metadata.get(META_NETWORK_POLICY) {
            Some(policy_json) if !policy_json.trim().is_empty() => {
                serde_json::from_str::<network::VmProxyPolicyConfig>(policy_json)
                    .with_context(|| format!("parse vm network policy for {vm_id}"))
                    .map_err(ManagerError::Other)?
            }
            _ => network::VmProxyPolicyConfig::default(),
        };
        network::register_vm_proxy_policy(vm_id, listen_addr, policy)
            .await
            .map_err(ManagerError::Other)?;

        Ok(())
    }

    fn volumes_dir(&self) -> PathBuf {
        PathBuf::from(&self.cfg.data_dir).join(VOLUMES_DIR_NAME)
    }

    fn volume_disk_path(&self, volume_id: &str) -> PathBuf {
        self.volumes_dir().join(format!("{volume_id}.qcow2"))
    }

    fn validate_volume_owner_key(owner_key: &str) -> ManagerResult<()> {
        let valid = !owner_key.is_empty()
            && owner_key.len() <= 200
            && owner_key
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ':'));
        if !valid {
            return Err(ManagerError::Other(anyhow!(
                "durable volume owner key must use only ASCII letters, digits, '.', '_', '-', or ':'"
            )));
        }
        Ok(())
    }

    async fn discover_volumes(&self) -> ManagerResult<()> {
        let dir = self.volumes_dir();
        fs::create_dir_all(&dir)?;
        let mut discovered: HashMap<String, DurableVolumeMetadata> = HashMap::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            match load_volume_metadata(&path) {
                Ok(meta) => {
                    if let Err(error) = Self::validate_volume_owner_key(&meta.owner_key) {
                        warn!(path = %path.display(), error = %error, "skipping invalid durable volume metadata");
                        continue;
                    }
                    let disk_path = self.volume_disk_path(&meta.volume_id);
                    if !disk_path.is_file() {
                        warn!(
                            owner_key = %meta.owner_key,
                            volume_id = %meta.volume_id,
                            path = %disk_path.display(),
                            "durable volume metadata has no qcow2; surfacing metadata was skipped"
                        );
                        continue;
                    }
                    if let Some(kept) = discovered.get(&meta.owner_key) {
                        // One inconsistent metadata file must never brick daemon
                        // startup (and all VM discovery with it) — keep the first
                        // record, surface the duplicate loudly, and continue, like
                        // every other per-volume problem in this loop.
                        warn!(
                            owner_key = %meta.owner_key,
                            kept_volume_id = %kept.volume_id,
                            skipped_volume_id = %meta.volume_id,
                            "duplicate durable volume owner key discovered; keeping first record"
                        );
                        continue;
                    }
                    discovered.insert(meta.owner_key.clone(), meta);
                }
                Err(error) => {
                    warn!(path = %path.display(), error = %error, "failed to load durable volume metadata");
                }
            }
        }
        *self.volumes.write().await = discovered;
        info!(
            count = self.volumes.read().await.len(),
            "rehydrated durable volume registry"
        );
        Ok(())
    }

    async fn vm_refs(&self) -> Vec<Arc<Vm>> {
        self.vms.read().await.values().cloned().collect()
    }

    async fn attached_vm_ids_for_volume(&self, owner_key: &str) -> Vec<String> {
        let vms = self.vm_refs().await;
        let mut attached = Vec::new();
        for vm in vms {
            let inner = vm.lock().await;
            if inner
                .metadata
                .durable_volume
                .as_ref()
                .is_some_and(|volume| volume.owner_key == owner_key)
            {
                attached.push(inner.metadata.id.clone());
            }
        }
        attached.sort();
        attached
    }

    async fn visible_attached_vm_ids_for_volume(&self, owner_key: &str) -> Vec<String> {
        let vms = self.vm_refs().await;
        let mut attached = Vec::new();
        for vm in vms {
            // Inventory is observational and must remain available while a VM
            // operation owns its runtime lock. Mutation paths use the blocking
            // helper above so a busy VM can never make deletion look safe.
            let Ok(inner) = vm.try_lock() else {
                continue;
            };
            if inner
                .metadata
                .durable_volume
                .as_ref()
                .is_some_and(|volume| volume.owner_key == owner_key)
            {
                attached.push(inner.metadata.id.clone());
            }
        }
        attached.sort();
        attached
    }

    async fn ensure_durable_volume(
        &self,
        owner_key: &str,
        size_gb: i32,
    ) -> ManagerResult<DurableVolumeAttachment> {
        Self::validate_volume_owner_key(owner_key)?;
        if size_gb <= 0 || size_gb > self.resource_bounds.max_disk_gb {
            return Err(ManagerError::Other(anyhow!(
                "durable volume size must be between 1 and {} GB",
                self.resource_bounds.max_disk_gb
            )));
        }
        let _operation_guards = self.lock_volume_operations(&[owner_key]).await;
        if let Some(existing) = self.volumes.read().await.get(owner_key).cloned() {
            let attached = self.attached_vm_ids_for_volume(owner_key).await;
            if !attached.is_empty() {
                return Err(ManagerError::VolumeInUse(attached.join(",")));
            }
            return Ok(DurableVolumeAttachment {
                owner_key: existing.owner_key.clone(),
                volume_id: existing.volume_id.clone(),
                size_gb: existing.size_gb,
            });
        }

        let dir = self.volumes_dir();
        fs::create_dir_all(&dir)?;
        let volume_id = owner_key.to_string();
        let disk_path = self.volume_disk_path(&volume_id);
        if disk_path.exists() {
            return Err(ManagerError::Other(anyhow!(
                "untracked durable volume disk already exists at {}",
                disk_path.display()
            )));
        }
        virt::create_qcow2(&self.cfg.qemu_img_bin, &disk_path, size_gb)
            .await
            .map_err(ManagerError::Other)?;
        let now = Utc::now();
        let mut meta = DurableVolumeMetadata {
            owner_key: owner_key.to_string(),
            volume_id: volume_id.clone(),
            size_gb,
            created_at: now,
            updated_at: now,
            backing_volume_id: None,
        };
        if let Err(error) = save_volume_metadata(&dir, &mut meta) {
            let _ = fs::remove_file(&disk_path);
            return Err(ManagerError::Other(error));
        }
        self.volumes
            .write()
            .await
            .insert(owner_key.to_string(), meta);
        info!(
            owner_key = %owner_key,
            volume_id = %volume_id,
            size_gb,
            "created durable data volume"
        );
        Ok(DurableVolumeAttachment {
            owner_key: owner_key.to_string(),
            volume_id,
            size_gb,
        })
    }

    pub async fn list_durable_volumes(&self) -> Vec<(DurableVolumeMetadata, Vec<String>)> {
        let volumes = self
            .volumes
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut out = Vec::with_capacity(volumes.len());
        for volume in volumes {
            let attached = self
                .visible_attached_vm_ids_for_volume(&volume.owner_key)
                .await;
            out.push((volume, attached));
        }
        out.sort_by(|left, right| left.0.owner_key.cmp(&right.0.owner_key));
        out
    }

    pub async fn delete_durable_volume(&self, owner_key: &str) -> ManagerResult<()> {
        Self::validate_volume_owner_key(owner_key)?;
        let _operation_guards = self.lock_volume_operations(&[owner_key]).await;
        let attached = self.attached_vm_ids_for_volume(owner_key).await;
        if !attached.is_empty() {
            return Err(ManagerError::VolumeInUse(attached.join(",")));
        }
        let meta = self
            .volumes
            .read()
            .await
            .get(owner_key)
            .cloned()
            .ok_or(ManagerError::VolumeNotFound)?;
        let disk_path = self.volume_disk_path(&meta.volume_id);
        if let Err(error) = fs::remove_file(&disk_path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                return Err(ManagerError::Io(error));
            }
        }
        let metadata_path = volume_metadata_path(&self.volumes_dir(), &meta.volume_id);
        if let Err(error) = fs::remove_file(&metadata_path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                warn!(
                    owner_key = %owner_key,
                    path = %metadata_path.display(),
                    error = %error,
                    "durable volume disk deleted but metadata cleanup failed"
                );
            }
        }
        self.volumes.write().await.remove(owner_key);
        info!(owner_key = %owner_key, volume_id = %meta.volume_id, "deleted durable data volume");
        if let Some(backing_id) = meta.backing_volume_id.as_deref() {
            self.cleanup_volume_fork_base_if_unreferenced(backing_id)
                .await;
        }
        Ok(())
    }

    async fn fork_durable_volume(
        &self,
        parent: &DurableVolumeAttachment,
        child: &DurableVolumeAttachment,
        fork_id: &str,
    ) -> ManagerResult<()> {
        Self::validate_volume_owner_key(&child.owner_key)?;
        let _operation_guards = self
            .lock_volume_operations(&[&parent.owner_key, &child.owner_key])
            .await;
        let (mut parent_meta, child_exists) = {
            let volumes = self.volumes.read().await;
            (
                volumes
                    .get(&parent.owner_key)
                    .cloned()
                    .ok_or(ManagerError::VolumeNotFound)?,
                volumes.contains_key(&child.owner_key),
            )
        };
        if child_exists {
            return Err(ManagerError::Other(anyhow!(
                "durable child volume owner already exists: {}",
                child.owner_key
            )));
        }
        let parent_path = self.volume_disk_path(&parent.volume_id);
        let fork_root = self
            .volumes_dir()
            .join(VOLUME_FORK_BASES_DIR_NAME)
            .join(fork_id);
        fs::create_dir_all(&fork_root)?;
        let base_path = fork_root.join("base.qcow2");
        fs::rename(&parent_path, &base_path)?;

        let child_path = self.volume_disk_path(&child.volume_id);
        let create_parent = virt::create_overlay(
            &self.cfg.qemu_img_bin,
            &base_path,
            &parent_path,
            parent.size_gb,
        )
        .await;
        if let Err(error) = create_parent {
            let _ = fs::rename(&base_path, &parent_path);
            let _ = fs::remove_dir_all(&fork_root);
            return Err(ManagerError::Other(error));
        }
        let create_child = virt::create_overlay(
            &self.cfg.qemu_img_bin,
            &base_path,
            &child_path,
            child.size_gb,
        )
        .await;
        if let Err(error) = create_child {
            let _ = fs::remove_file(&parent_path);
            let _ = fs::rename(&base_path, &parent_path);
            let _ = fs::remove_dir_all(&fork_root);
            return Err(ManagerError::Other(error));
        }

        let backing_id = format!("fork-base:{fork_id}");
        parent_meta.backing_volume_id = Some(backing_id.clone());
        let mut child_meta = DurableVolumeMetadata {
            owner_key: child.owner_key.clone(),
            volume_id: child.volume_id.clone(),
            size_gb: child.size_gb,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            backing_volume_id: Some(backing_id),
        };
        if let Err(error) = save_volume_metadata(&self.volumes_dir(), &mut parent_meta)
            .and_then(|_| save_volume_metadata(&self.volumes_dir(), &mut child_meta))
        {
            let _ = fs::remove_file(&parent_path);
            let _ = fs::remove_file(&child_path);
            let _ = fs::rename(&base_path, &parent_path);
            let _ = fs::remove_dir_all(&fork_root);
            return Err(ManagerError::Other(error));
        }
        {
            let mut volumes = self.volumes.write().await;
            volumes.insert(parent.owner_key.clone(), parent_meta);
            volumes.insert(child.owner_key.clone(), child_meta);
        }
        info!(
            parent_owner_key = %parent.owner_key,
            child_owner_key = %child.owner_key,
            fork_id = %fork_id,
            "forked durable volume through immutable qcow2 backing"
        );
        Ok(())
    }

    async fn rollback_durable_volume_fork(
        &self,
        parent: &DurableVolumeAttachment,
        child: &DurableVolumeAttachment,
        fork_id: &str,
    ) {
        let _operation_guards = self
            .lock_volume_operations(&[&parent.owner_key, &child.owner_key])
            .await;
        let parent_path = self.volume_disk_path(&parent.volume_id);
        let child_path = self.volume_disk_path(&child.volume_id);
        let fork_root = self
            .volumes_dir()
            .join(VOLUME_FORK_BASES_DIR_NAME)
            .join(fork_id);
        let base_path = fork_root.join("base.qcow2");
        let _ = fs::remove_file(&parent_path);
        let _ = fs::remove_file(&child_path);
        let _ = fs::rename(&base_path, &parent_path);
        let _ = fs::remove_dir_all(&fork_root);
        let parent_meta = {
            let mut volumes = self.volumes.write().await;
            volumes.remove(&child.owner_key);
            volumes.get_mut(&parent.owner_key).map(|parent_meta| {
                parent_meta.backing_volume_id = None;
                parent_meta.clone()
            })
        };
        if let Some(mut parent_meta) = parent_meta {
            let _ = save_volume_metadata(&self.volumes_dir(), &mut parent_meta);
        }
        let _ = fs::remove_file(volume_metadata_path(&self.volumes_dir(), &child.volume_id));
    }

    async fn cleanup_volume_fork_base_if_unreferenced(&self, backing_id: &str) {
        let Some(fork_id) = backing_id.strip_prefix("fork-base:") else {
            return;
        };
        if self
            .volumes
            .read()
            .await
            .values()
            .any(|volume| volume.backing_volume_id.as_deref() == Some(backing_id))
        {
            return;
        }
        let fork_root = self
            .volumes_dir()
            .join(VOLUME_FORK_BASES_DIR_NAME)
            .join(fork_id);
        if let Err(error) = fs::remove_dir_all(&fork_root) {
            if error.kind() != std::io::ErrorKind::NotFound {
                warn!(
                    backing_id = %backing_id,
                    path = %fork_root.display(),
                    error = %error,
                    "failed removing unreferenced durable volume fork base"
                );
            }
        }
    }

    async fn rehydrate_session_registry(&self) -> usize {
        let vms = self.vm_refs().await;
        let mut registry = HashMap::new();
        for vm in vms {
            let inner = vm.lock().await;
            if matches!(inner.metadata.state, VmState::Error) {
                continue;
            }
            let Some(session_id) = inner.metadata.metadata.get(META_SESSION_ID) else {
                continue;
            };
            if session_id.trim().is_empty() {
                continue;
            }
            if let Some(previous) = registry.insert(session_id.clone(), inner.metadata.id.clone()) {
                warn!(
                    session_id = %session_id,
                    first_vm_id = %previous,
                    replacement_vm_id = %inner.metadata.id,
                    "duplicate session id found while rehydrating registry"
                );
            }
        }
        let count = registry.len();
        *self.session_registry.write().await = registry;
        count
    }

    pub async fn get_by_session_id(&self, session_id: &str) -> ManagerResult<VmMetadata> {
        let lookup = || async {
            let vm_id = self.session_registry.read().await.get(session_id).cloned();
            match vm_id {
                Some(vm_id) => self.get(&vm_id).await.ok(),
                None => None,
            }
        };
        if let Some(meta) = lookup().await {
            return Ok(meta);
        }
        let count = self.rehydrate_session_registry().await;
        info!(
            session_id = %session_id,
            registered_sessions = count,
            "session lookup missed; rehydrated registry once before confirming absence"
        );
        lookup().await.ok_or(ManagerError::VmNotFound)
    }

    async fn discover(&self) -> ManagerResult<()> {
        let entries = fs::read_dir(&self.cfg.data_dir)?;
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            if entry.file_name() == config::BASE_IMAGES_DIR_NAME {
                continue;
            }
            if entry.file_name() == FORK_BASES_DIR_NAME {
                continue;
            }
            if entry.file_name() == VOLUMES_DIR_NAME {
                continue;
            }

            let vm_dir = entry.path();
            match load_metadata(&vm_dir) {
                Ok(mut meta) => {
                    if meta.id.is_empty() {
                        if let Some(name) = vm_dir.file_name().and_then(|n| n.to_str()) {
                            meta.id = name.to_string();
                        } else {
                            warn!(
                                dir = %vm_dir.display(),
                                "unable to determine VM ID from directory name"
                            );
                            continue;
                        }
                    }
                    if matches!(
                        meta.state,
                        VmState::Running | VmState::Paused | VmState::Creating
                    ) {
                        meta.state = VmState::Stopped;
                    }
                    if meta.architecture.is_empty() {
                        meta.architecture = self.host_arch.clone();
                    }

                    let mut vm_device_ids = HashSet::new();
                    let mut leases = self.pci_leases.lock().await;
                    for assignment in &meta.pci_devices {
                        if !vm_device_ids.insert(assignment.id.as_str()) {
                            return Err(ManagerError::Other(anyhow!(
                                "VM {} contains duplicate PCI assignment `{}`",
                                meta.id,
                                assignment.id
                            )));
                        }
                        if let Some(owner) = leases.insert(assignment.id.clone(), meta.id.clone()) {
                            return Err(ManagerError::Other(anyhow!(
                                "PCI device `{}` is claimed by both VM {} and VM {}",
                                assignment.id,
                                owner,
                                meta.id
                            )));
                        }
                    }
                    drop(leases);

                    let vm = Arc::new(Vm::new(
                        meta.clone(),
                        VmRuntime::new(&vm_dir),
                        vm_dir.clone(),
                    ));
                    {
                        let mut inner = vm.lock().await;
                        inner.runtime.state = meta.state;
                    }

                    for snap in &meta.snapshots {
                        let record = SnapshotRecord {
                            vm_id: meta.id.clone(),
                            snapshot: snap.clone(),
                        };
                        self.snapshots
                            .write()
                            .await
                            .insert(record.snapshot.id.clone(), record);
                    }

                    let mut persisted = meta.clone();
                    if let Err(err) = save_metadata(&vm_dir, &mut persisted) {
                        warn!(
                            dir = %vm_dir.display(),
                            error = %err,
                            "failed to persist metadata during discovery"
                        );
                    }

                    self.vms.write().await.insert(meta.id.clone(), vm);
                    if !matches!(meta.state, VmState::Error) {
                        if let Some(session_id) = meta
                            .metadata
                            .get(META_SESSION_ID)
                            .filter(|value| !value.trim().is_empty())
                        {
                            let replaced = self
                                .session_registry
                                .write()
                                .await
                                .insert(session_id.clone(), meta.id.clone());
                            if let Some(previous) = replaced {
                                warn!(
                                    session_id = %session_id,
                                    previous_vm_id = %previous,
                                    vm_id = %meta.id,
                                    "duplicate session mapping replaced during discovery"
                                );
                            }
                            info!(
                                session_id = %session_id,
                                vm_id = %meta.id,
                                "rehydrated persisted session mapping"
                            );
                        }
                    }
                }
                Err(err) => {
                    warn!(
                        dir = %vm_dir.display(),
                        error = %err,
                        "failed to load VM metadata during discovery"
                    );
                }
            }
        }
        Ok(())
    }

    pub async fn list(&self) -> Vec<VmMetadata> {
        let vms = self.vm_refs().await;
        let mut snapshots = Vec::with_capacity(vms.len());
        for vm in vms {
            let inner = vm.lock().await;
            snapshots.push(inner.metadata.clone());
        }

        let mut vms = Vec::with_capacity(snapshots.len());
        for metadata in snapshots {
            vms.push(self.metadata_with_runtime_network_snapshot(metadata).await);
        }
        vms
    }

    pub async fn get(&self, id: &str) -> ManagerResult<VmMetadata> {
        let vm = self.vm_by_id(id).await?;
        let guard = vm.lock().await;
        let metadata = guard.metadata.clone();
        drop(guard);
        Ok(self.metadata_with_runtime_network_snapshot(metadata).await)
    }

    pub async fn get_with_runtime(&self, id: &str) -> ManagerResult<(VmMetadata, VmRuntime)> {
        let vm = self.vm_by_id(id).await?;
        let guard = vm.lock().await;
        let metadata = guard.metadata.clone();
        let runtime = guard.runtime.clone();
        drop(guard);
        Ok((
            self.metadata_with_runtime_network_snapshot(metadata).await,
            runtime,
        ))
    }

    pub fn pci_enabled(&self) -> bool {
        self.cfg.pci.enabled()
    }

    pub async fn list_host_pci_devices(&self) -> Vec<PciInventoryDevice> {
        let leases = self.pci_leases.lock().await.clone();
        pci::inventory(&self.cfg.pci, &leases)
    }

    fn ensure_pci_compatible(&self, metadata: &VmMetadata) -> ManagerResult<()> {
        if !self.cfg.pci.enabled() {
            return Err(ManagerError::Unsupported(
                "PCI assignment is disabled on this vmd".to_string(),
            ));
        }
        if !cfg!(target_os = "linux") || self.host_arch != ARCH_AMD64 {
            return Err(ManagerError::Unsupported(
                "PCI assignment requires an amd64 Linux vmd host".to_string(),
            ));
        }
        if metadata.architecture != ARCH_AMD64 {
            return Err(ManagerError::Unsupported(
                "PCI assignment requires an amd64 guest".to_string(),
            ));
        }
        if self.cfg.ha_mode {
            return Err(ManagerError::Unsupported(
                "PCI assignment is unavailable in ha mode".to_string(),
            ));
        }
        Ok(())
    }

    async fn reserve_pci_devices(
        &self,
        vm_id: &str,
        device_ids: &[String],
    ) -> ManagerResult<Vec<PciDeviceAssignmentSpec>> {
        if device_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut requested = HashSet::new();
        for device_id in device_ids {
            if !requested.insert(device_id.as_str()) {
                return Err(ManagerError::PciConflict(format!(
                    "device `{device_id}` was requested more than once"
                )));
            }
            if self.cfg.pci.device(device_id).is_none() {
                return Err(ManagerError::PciUnavailable(format!(
                    "device `{device_id}` is not allowlisted"
                )));
            }
        }

        let mut leases = self.pci_leases.lock().await;
        let inventory = pci::inventory(&self.cfg.pci, &leases);
        for device_id in device_ids {
            let device = inventory
                .iter()
                .find(|device| device.id == *device_id)
                .ok_or_else(|| {
                    ManagerError::PciUnavailable(format!("device `{device_id}` is not allowlisted"))
                })?;
            if device.state == PciInventoryState::Assigned {
                return Err(ManagerError::PciConflict(format!(
                    "device `{device_id}` is assigned to VM {}",
                    device.assigned_vm_id
                )));
            }
            if device.state == PciInventoryState::Unavailable {
                return Err(ManagerError::PciUnavailable(format!(
                    "device `{device_id}`: {}",
                    device.unavailable_reason
                )));
            }
        }

        let assignments = device_ids
            .iter()
            .map(|device_id| {
                let policy = self.cfg.pci.device(device_id).expect("validated policy");
                PciDeviceAssignmentSpec {
                    id: policy.id.clone(),
                    bdfs: policy.bdfs.clone(),
                    iommu_groups: Vec::new(),
                    original_drivers: HashMap::new(),
                }
            })
            .collect::<Vec<_>>();
        for assignment in &assignments {
            leases.insert(assignment.id.clone(), vm_id.to_string());
        }
        Ok(assignments)
    }

    async fn release_pci_leases(&self, vm_id: &str, assignments: &[PciDeviceAssignmentSpec]) {
        let mut leases = self.pci_leases.lock().await;
        for assignment in assignments {
            if leases
                .get(&assignment.id)
                .is_some_and(|owner| owner == vm_id)
            {
                leases.remove(&assignment.id);
            }
        }
    }

    pub async fn attach_pci_device(
        &self,
        vm_id: &str,
        device_id: &str,
    ) -> ManagerResult<PciActionResult> {
        let vm = self.vm_by_id(vm_id).await?;
        let mut inner = vm.lock().await;
        self.ensure_pci_compatible(&inner.metadata)?;
        if inner
            .metadata
            .pci_devices
            .iter()
            .any(|assignment| assignment.id == device_id)
        {
            return Ok(PciActionResult {
                metadata: inner.metadata.clone(),
                restart_required: false,
                detail: "device is already attached".to_string(),
            });
        }

        let policy = self.cfg.pci.device(device_id).cloned().ok_or_else(|| {
            ManagerError::PciUnavailable(format!("device `{device_id}` is not allowlisted"))
        })?;
        let running = matches!(inner.runtime.state, VmState::Running | VmState::Paused);
        if running && !policy.hotplug {
            return Ok(PciActionResult {
                metadata: inner.metadata.clone(),
                restart_required: true,
                detail: "device policy requires a cold attach".to_string(),
            });
        }

        let mut assignments = self
            .reserve_pci_devices(vm_id, &[device_id.to_string()])
            .await?;
        let mut assignment = assignments.remove(0);
        if !running {
            inner.metadata.pci_devices.push(assignment);
            if let Err(error) = save_metadata(&vm.dir, &mut inner.metadata) {
                let assignments = inner
                    .metadata
                    .pci_devices
                    .pop()
                    .into_iter()
                    .collect::<Vec<_>>();
                drop(inner);
                self.release_pci_leases(vm_id, &assignments).await;
                return Err(ManagerError::Other(error));
            }
            return Ok(PciActionResult {
                metadata: inner.metadata.clone(),
                restart_required: false,
                detail: "device will be assigned on the next VM start".to_string(),
            });
        }

        let monitor = inner
            .runtime
            .monitor
            .clone()
            .ok_or_else(|| ManagerError::Other(anyhow!("vm monitor unavailable")))?;
        assignment = match pci::prepare_assignment(
            &self.cfg.pci,
            device_id,
            self.cfg.qemu_process.run_as_uid,
            self.cfg.qemu_process.run_as_gid,
        )
        .await
        {
            Ok(assignment) => assignment,
            Err(error) => match error.downcast::<pci::PciRollbackError>() {
                Ok(failure) => {
                    let detail = failure.to_string();
                    inner.metadata.pci_devices.push(failure.assignment);
                    save_metadata(&vm.dir, &mut inner.metadata).map_err(ManagerError::Other)?;
                    return Ok(PciActionResult {
                        metadata: inner.metadata.clone(),
                        restart_required: true,
                        detail,
                    });
                }
                Err(error) => {
                    self.release_pci_leases(vm_id, std::slice::from_ref(&assignment))
                        .await;
                    return Err(ManagerError::Other(error));
                }
            },
        };
        let mut added: Vec<String> = Vec::new();
        for (index, bdf) in assignment.bdfs.iter().enumerate() {
            let qemu_id = pci::qemu_device_id(&assignment.id, index);
            if let Err(error) = monitor.device_add_vfio(&qemu_id, bdf).await {
                let mut cleanup_error = None;
                for added_id in added.iter().rev() {
                    if let Err(delete_error) = monitor
                        .device_del_wait(added_id, Duration::from_secs(5))
                        .await
                    {
                        cleanup_error = Some(delete_error);
                        break;
                    }
                }
                if let Some(cleanup_error) = cleanup_error {
                    inner.metadata.pci_devices.push(assignment);
                    save_metadata(&vm.dir, &mut inner.metadata).map_err(ManagerError::Other)?;
                    return Ok(PciActionResult {
                        metadata: inner.metadata.clone(),
                        restart_required: true,
                        detail: format!(
                            "PCI hot-add failed ({error}) and live cleanup failed ({cleanup_error})"
                        ),
                    });
                }
                if let Err(release_error) =
                    pci::release_assignment(&self.cfg.pci, &assignment).await
                {
                    inner.metadata.pci_devices.push(assignment);
                    save_metadata(&vm.dir, &mut inner.metadata).map_err(ManagerError::Other)?;
                    return Ok(PciActionResult {
                        metadata: inner.metadata.clone(),
                        restart_required: true,
                        detail: format!(
                            "PCI hot-add failed ({error}) and host release failed ({release_error})"
                        ),
                    });
                }
                self.release_pci_leases(vm_id, std::slice::from_ref(&assignment))
                    .await;
                return Err(ManagerError::Other(error));
            }
            added.push(qemu_id);
        }
        inner.metadata.pci_devices.push(assignment);
        if let Err(save_error) = save_metadata(&vm.dir, &mut inner.metadata) {
            let assignment = inner
                .metadata
                .pci_devices
                .pop()
                .expect("new PCI assignment must be present");
            let mut cleanup_error = None;
            for added_id in added.iter().rev() {
                if let Err(error) = monitor
                    .device_del_wait(added_id, Duration::from_secs(5))
                    .await
                {
                    cleanup_error = Some(error);
                    break;
                }
            }
            if let Some(cleanup_error) = cleanup_error {
                inner.metadata.pci_devices.push(assignment);
                let _ = save_metadata(&vm.dir, &mut inner.metadata);
                return Ok(PciActionResult {
                    metadata: inner.metadata.clone(),
                    restart_required: true,
                    detail: format!(
                        "PCI metadata persistence failed ({save_error}) and live cleanup failed ({cleanup_error})"
                    ),
                });
            }
            if let Err(release_error) = pci::release_assignment(&self.cfg.pci, &assignment).await {
                inner.metadata.pci_devices.push(assignment);
                let _ = save_metadata(&vm.dir, &mut inner.metadata);
                return Ok(PciActionResult {
                    metadata: inner.metadata.clone(),
                    restart_required: true,
                    detail: format!(
                        "PCI metadata persistence failed ({save_error}) and host release failed ({release_error})"
                    ),
                });
            }
            self.release_pci_leases(vm_id, std::slice::from_ref(&assignment))
                .await;
            return Err(ManagerError::Other(save_error));
        }
        Ok(PciActionResult {
            metadata: inner.metadata.clone(),
            restart_required: false,
            detail: "device hot-attached".to_string(),
        })
    }

    pub async fn detach_pci_device(
        &self,
        vm_id: &str,
        device_id: &str,
    ) -> ManagerResult<PciActionResult> {
        let vm = self.vm_by_id(vm_id).await?;
        let mut inner = vm.lock().await;
        self.ensure_pci_compatible(&inner.metadata)?;
        let Some(index) = inner
            .metadata
            .pci_devices
            .iter()
            .position(|assignment| assignment.id == device_id)
        else {
            return Ok(PciActionResult {
                metadata: inner.metadata.clone(),
                restart_required: false,
                detail: "device is already detached".to_string(),
            });
        };
        let assignment = inner.metadata.pci_devices[index].clone();
        let policy = self.cfg.pci.device(device_id);
        let running = matches!(inner.runtime.state, VmState::Running | VmState::Paused);
        if running && !policy.is_some_and(|policy| policy.hotplug) {
            return Ok(PciActionResult {
                metadata: inner.metadata.clone(),
                restart_required: true,
                detail: "device policy requires a cold detach".to_string(),
            });
        }
        if running {
            let monitor = inner
                .runtime
                .monitor
                .clone()
                .ok_or_else(|| ManagerError::Other(anyhow!("vm monitor unavailable")))?;
            for function_index in (0..assignment.bdfs.len()).rev() {
                let qemu_id = pci::qemu_device_id(&assignment.id, function_index);
                if let Err(error) = monitor
                    .device_del_wait(&qemu_id, Duration::from_secs(10))
                    .await
                {
                    return Ok(PciActionResult {
                        metadata: inner.metadata.clone(),
                        restart_required: true,
                        detail: format!("live detach failed: {error}"),
                    });
                }
            }
        }
        pci::release_assignment(&self.cfg.pci, &assignment)
            .await
            .map_err(ManagerError::Other)?;
        inner.metadata.pci_devices.remove(index);
        if let Err(error) = save_metadata(&vm.dir, &mut inner.metadata) {
            inner.metadata.pci_devices.insert(index, assignment.clone());
            return Err(ManagerError::Other(error));
        }
        self.release_pci_leases(vm_id, std::slice::from_ref(&assignment))
            .await;
        Ok(PciActionResult {
            metadata: inner.metadata.clone(),
            restart_required: false,
            detail: "device detached".to_string(),
        })
    }

    pub async fn running_vm_health_probe_targets(
        &self,
        now: DateTime<Utc>,
        now_instant: Instant,
        start_grace: Duration,
        min_probe_interval: Duration,
    ) -> Vec<VmHealthProbeTarget> {
        let vms: Vec<Arc<Vm>> = {
            let guard = self.vms.read().await;
            guard.values().cloned().collect()
        };

        let mut targets = Vec::new();
        for vm in vms {
            let mut inner = vm.lock().await;
            if inner.runtime.state != VmState::Running {
                continue;
            }
            let Some(started_at) = inner.runtime.started_at else {
                continue;
            };
            let age = now
                .signed_duration_since(started_at)
                .to_std()
                .unwrap_or_default();
            if age < start_grace {
                continue;
            }
            if inner
                .runtime
                .health_probe_suppressed_until
                .is_some_and(|until| now_instant < until)
            {
                continue;
            }
            if let Some(last_probe_at) = inner.runtime.last_health_probe_at {
                if now_instant
                    .checked_duration_since(last_probe_at)
                    .is_some_and(|elapsed| elapsed < min_probe_interval)
                {
                    continue;
                }
            }
            inner.runtime.last_health_probe_at = Some(now_instant);
            targets.push(VmHealthProbeTarget {
                vm_id: inner.metadata.id.clone(),
                rpc_port: inner.metadata.network.rpc_port,
                portproxy_auth_token: inner
                    .metadata
                    .metadata
                    .get(META_PORTPROXY_AUTH_TOKEN)
                    .cloned(),
                started_at,
            });
        }
        targets
    }

    pub async fn record_vm_health_probe_success(
        &self,
        id: &str,
        expected_started_at: DateTime<Utc>,
    ) -> ManagerResult<bool> {
        let vm = self.vm_by_id(id).await?;
        let mut inner = vm.lock().await;
        if inner.runtime.state != VmState::Running
            || inner.runtime.started_at != Some(expected_started_at)
        {
            return Ok(false);
        }
        let had_failures = inner.runtime.consecutive_health_failures > 0;
        inner.runtime.clear_health_failures();
        Ok(had_failures)
    }

    pub async fn record_vm_health_probe_failure(
        &self,
        id: &str,
        expected_started_at: DateTime<Utc>,
        failure_threshold: u32,
        permanent: bool,
    ) -> ManagerResult<Option<u32>> {
        let vm = self.vm_by_id(id).await?;
        let mut inner = vm.lock().await;
        if inner.runtime.state != VmState::Running
            || inner.runtime.started_at != Some(expected_started_at)
        {
            return Ok(None);
        }
        if inner
            .runtime
            .health_probe_suppressed_until
            .is_some_and(|until| Instant::now() < until)
        {
            inner.runtime.clear_health_failures();
            return Ok(None);
        }
        let failure_threshold = failure_threshold.max(1);
        if permanent {
            inner.runtime.consecutive_health_failures = inner
                .runtime
                .consecutive_health_failures
                .max(failure_threshold);
        } else {
            inner.runtime.consecutive_health_failures =
                inner.runtime.consecutive_health_failures.saturating_add(1);
        }
        let failures = inner.runtime.consecutive_health_failures;
        if failures >= failure_threshold {
            Ok(Some(failures))
        } else {
            Ok(None)
        }
    }

    pub async fn suppress_vm_health_probes_for(
        &self,
        vm_id: &str,
        duration: std::time::Duration,
    ) -> ManagerResult<()> {
        let vm = self.vm_by_id(vm_id).await?;
        let mut inner = vm.lock().await;
        if inner.runtime.state != VmState::Running {
            return Ok(());
        }
        inner.runtime.consecutive_health_failures = 0;
        inner.runtime.last_health_probe_at = None;
        inner.runtime.health_probe_suppressed_until = Some(Instant::now() + duration);
        Ok(())
    }

    pub async fn clear_vm_health_probe_suppression(&self, vm_id: &str) -> ManagerResult<()> {
        let vm = self.vm_by_id(vm_id).await?;
        let mut inner = vm.lock().await;
        inner.runtime.health_probe_suppressed_until = None;
        inner.runtime.clear_health_failures();
        Ok(())
    }

    async fn active_vm_count(&self) -> usize {
        let vms = self.vm_refs().await;
        let mut count = 0usize;
        for vm in vms {
            let inner = vm.lock().await;
            if matches!(
                inner.metadata.state,
                VmState::Running | VmState::Paused | VmState::Creating
            ) {
                count += 1;
            }
        }
        count
    }

    async fn enforce_create_vm_capacity(&self) -> ManagerResult<()> {
        let Some(limit) = self.cfg.max_active_vms else {
            return Ok(());
        };
        let current = self.active_vm_count().await;
        if current >= limit {
            return Err(ManagerError::CapacityExceeded {
                resource: "active_vms",
                limit,
                current,
            });
        }
        Ok(())
    }

    async fn insert_creating_vm_with_capacity(&self, id: String, vm: Arc<Vm>) -> ManagerResult<()> {
        // Serialize only the capacity decision and insertion. Snapshotting the VM
        // references before awaiting their runtime locks keeps the global VM map
        // available to observational reads while another VM is busy.
        let _capacity_guard = self.vm_capacity_lock.lock().await;
        if let Some(limit) = self.cfg.max_active_vms {
            let existing_vms = self.vm_refs().await;
            let mut current = 0usize;
            for existing in existing_vms {
                let inner = existing.lock().await;
                if matches!(
                    inner.metadata.state,
                    VmState::Running | VmState::Paused | VmState::Creating
                ) {
                    current += 1;
                }
            }
            if current >= limit {
                return Err(ManagerError::CapacityExceeded {
                    resource: "active_vms",
                    limit,
                    current,
                });
            }
        }
        self.vms.write().await.insert(id, vm);
        Ok(())
    }

    pub async fn create_vm(
        &self,
        params: CreateVmParams,
        progress: Option<CreateVmProgressCallback>,
    ) -> ManagerResult<VmMetadata> {
        let mut params = params;
        if params.resources.vcpu <= 0 {
            params.resources.vcpu = 1;
        }
        if params.resources.memory_mb <= 0 {
            params.resources.memory_mb = 1024;
        }
        if params.resources.disk_gb <= 0 {
            params.resources.disk_gb = 10;
        }
        enforce_resource_bounds(&params.resources, self.resource_bounds)?;
        self.enforce_create_vm_capacity().await?;
        let name = sanitize_name(&params.name);
        // @dive: Shared mounts are normalized before persistence so mount tags and host paths stay stable across restarts and forks.
        params.shared_mounts = normalize_shared_mounts(params.shared_mounts)?;
        ensure_mount_profiles_supported(&params.shared_mounts, &self.cfg.shared_mount_profiles)?;
        info!(
            name = %name,
            source = %params.source.reference,
            requested_architecture = %params.architecture,
            shared_mount_count = params.shared_mounts.len(),
            shared_mounts = ?params.shared_mounts,
            "creating vm with normalized shared mounts"
        );

        let mac = random_mac().map_err(ManagerError::Other)?;
        let requested_arch_str = normalize_arch(&params.architecture)?;
        let requested_arch = if requested_arch_str.is_empty() {
            None
        } else {
            Some(requested_arch_str)
        };

        let mut source_vm = None;
        let mut snapshot_record = None;
        let mut platform = None::<String>;

        let resolved_arch = match params.source.source_type {
            VmSourceType::Docker => {
                let (arch, plat) = self
                    .resolve_docker_platform(&params.source.reference, requested_arch.clone())
                    .await?;
                platform = Some(plat);
                Some(arch)
            }
            VmSourceType::Snapshot => {
                let record = self.snapshot_by_id(&params.source.reference).await?;
                let vm = self.vm_by_id(&record.vm_id).await?;
                {
                    let inner = vm.lock().await;
                    if let Some(req_arch) = &requested_arch {
                        if req_arch != &inner.metadata.architecture {
                            return Err(ManagerError::Other(anyhow!(
                                "snapshot architecture mismatch: requested {}, snapshot {}",
                                req_arch,
                                inner.metadata.architecture
                            )));
                        }
                    }
                    source_vm = Some(vm.clone());
                    snapshot_record = Some(record);
                    Some(inner.metadata.architecture.clone())
                }
            }
        };

        let arch = resolved_arch
            .or(requested_arch.clone())
            .unwrap_or_else(|| self.host_arch.clone());
        if arch != ARCH_AMD64 && arch != ARCH_ARM64 {
            return Err(ManagerError::Other(anyhow!(
                "unsupported architecture: {}",
                arch
            )));
        }

        let storage_profile = match params.storage_profile.trim() {
            "" => "local-ephemeral",
            "local-ephemeral" => "local-ephemeral",
            "durable-data" => "durable-data",
            other => {
                return Err(ManagerError::Other(anyhow!(
                    "unsupported VM storage profile `{other}`"
                )));
            }
        };
        let durable_volume = match (storage_profile, params.volume_owner_key.as_deref()) {
            ("durable-data", Some(owner_key)) if !owner_key.trim().is_empty() => Some(
                self.ensure_durable_volume(
                    owner_key,
                    params.volume_size_gb.unwrap_or(params.resources.disk_gb),
                )
                .await?,
            ),
            ("durable-data", _) => {
                return Err(ManagerError::Other(anyhow!(
                    "durable-data storage profile requires volume_owner_key"
                )));
            }
            (_, Some(_)) => {
                return Err(ManagerError::Other(anyhow!(
                    "volume_owner_key requires durable-data storage profile"
                )));
            }
            _ => None,
        };

        let id = Uuid::new_v4().to_string();
        let vm_dir = PathBuf::from(&self.cfg.data_dir).join(&id);
        fs::create_dir_all(&vm_dir)?;

        let mut meta = VmMetadata {
            id: id.clone(),
            name,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: VmState::Creating,
            architecture: arch.clone(),
            source: VmSource {
                source_type: params.source.source_type.clone(),
                reference: params.source.reference.clone(),
            },
            resources: params.resources.clone(),
            network: NetworkSpec {
                mac,
                proxy_port: 0,
                rpc_port: 0,
            },
            metadata: params.metadata.clone(),
            snapshots: Vec::new(),
            shared_mounts: params.shared_mounts.clone(),
            pci_devices: Vec::new(),
            durable_volume,
            boot_incoming_ram_path: String::new(),
            started_at: None,
        };
        if !params.pci_device_ids.is_empty() {
            if matches!(&params.source.source_type, VmSourceType::Snapshot) {
                let _ = remove_vm_dir_if_detached(&id, &vm_dir);
                return Err(ManagerError::Other(anyhow!(
                    "PCI devices cannot be assigned while creating from a snapshot"
                )));
            }
            if let Err(error) = self.ensure_pci_compatible(&meta) {
                let _ = remove_vm_dir_if_detached(&id, &vm_dir);
                return Err(error);
            }
            meta.pci_devices = match self.reserve_pci_devices(&id, &params.pci_device_ids).await {
                Ok(assignments) => assignments,
                Err(error) => {
                    let _ = remove_vm_dir_if_detached(&id, &vm_dir);
                    return Err(error);
                }
            };
        }
        meta.metadata.insert(
            META_STORAGE_PROFILE.to_string(),
            storage_profile.to_string(),
        );
        assign_new_portproxy_auth_token(&mut meta.metadata);

        if let Err(error) = save_metadata(&vm_dir, &mut meta) {
            self.release_pci_leases(&id, &meta.pci_devices).await;
            let _ = remove_vm_dir_if_detached(&id, &vm_dir);
            return Err(ManagerError::Other(error));
        }

        let runtime = VmRuntime::new(&vm_dir);
        let vm = Arc::new(Vm::new(meta.clone(), runtime, vm_dir.clone()));
        if let Err(error) = self
            .insert_creating_vm_with_capacity(id.clone(), vm.clone())
            .await
        {
            self.release_pci_leases(&id, &meta.pci_devices).await;
            let _ = remove_vm_dir_if_detached(&id, &vm_dir);
            return Err(error);
        }
        let mut vm_guard = vm.lock_owned().await;

        let create_result = match params.source.source_type {
            VmSourceType::Docker => {
                self.create_from_docker(
                    &vm,
                    vm_guard,
                    &params.source.reference,
                    &arch,
                    platform.as_deref(),
                    progress.clone(),
                )
                .await
            }
            VmSourceType::Snapshot => {
                if let (Some(source), Some(record)) = (source_vm, snapshot_record) {
                    self.create_from_snapshot(&vm, vm_guard, &source, &record)
                        .await
                } else {
                    Err(ManagerError::Other(anyhow!(
                        "snapshot source missing required metadata"
                    )))
                }
            }
        };
        vm_guard = match create_result {
            Ok(vm_guard) => vm_guard,
            Err(err) => {
                self.vms.write().await.remove(&id);
                self.release_pci_leases(&id, &meta.pci_devices).await;
                let _ = remove_vm_dir_if_detached(&id, &vm_dir);
                return Err(err);
            }
        };

        vm_guard.metadata.state = VmState::Stopped;
        if let Err(err) = save_metadata(&vm.dir, &mut vm_guard.metadata) {
            self.vms.write().await.remove(&id);
            self.release_pci_leases(&id, &meta.pci_devices).await;
            let _ = remove_vm_dir_if_detached(&id, &vm_dir);
            return Err(ManagerError::Other(err));
        }

        let created_metadata = vm_guard.metadata.clone();
        let vm_name = created_metadata.name.clone();
        drop(vm_guard);

        if let Some(session_id) = created_metadata
            .metadata
            .get(META_SESSION_ID)
            .filter(|value| !value.trim().is_empty())
        {
            self.session_registry
                .write()
                .await
                .insert(session_id.clone(), id.clone());
            info!(session_id = %session_id, vm_id = %id, "registered newly created session");
        }

        if params.auto_start {
            emit_stage_progress(
                &progress,
                CreateVmStage::StartVm,
                0,
                format!("starting VM {}", vm_name.clone()),
            );
            let _ = self.start_vm(&id).await?;
            emit_stage_progress(
                &progress,
                CreateVmStage::StartVm,
                100,
                format!("VM {} started", vm_name),
            );
        } else {
            emit_stage_progress(
                &progress,
                CreateVmStage::StartVm,
                100,
                "auto-start disabled".to_string(),
            );
        }

        Ok(created_metadata)
    }

    #[instrument(skip(self))]
    pub async fn update_vm(&self, id: &str, params: UpdateVmParams) -> ManagerResult<VmMetadata> {
        let vm = self.vm_by_id(id).await?;
        let (updated_metadata, runtime_state) = {
            let mut inner = vm.lock().await;
            let existing_metadata = inner.metadata.metadata.clone();
            {
                if let Some(name) = params.name {
                    inner.metadata.name = sanitize_name(&name);
                }
                if let Some(mut meta) = params.metadata {
                    Self::preserve_runtime_managed_metadata(&existing_metadata, &mut meta);
                    inner.metadata.metadata = meta;
                }
                save_metadata(&vm.dir, &mut inner.metadata).map_err(ManagerError::Other)?;
            }
            (inner.metadata.clone(), inner.runtime.state)
        };

        self.reapply_runtime_network_policy(id, &updated_metadata.metadata, runtime_state)
            .await?;

        Ok(updated_metadata)
    }

    pub async fn delete_vm(&self, id: &str, _purge_snapshots: bool) -> ManagerResult<()> {
        let vm = self.vm_by_id(id).await?;
        let (fork_base_path, session_id) = {
            let inner = vm.lock().await;
            (
                inner.metadata.metadata.get(META_FORK_BASE_PATH).cloned(),
                inner.metadata.metadata.get(META_SESSION_ID).cloned(),
            )
        };
        let state = {
            let inner = vm.lock().await;
            inner.runtime.state
        };
        if matches!(state, VmState::Running | VmState::Paused) {
            self.force_stop_vm(id).await?;
        }
        {
            let inner = vm.lock().await;
            if matches!(inner.runtime.state, VmState::Running | VmState::Paused) {
                return Err(ManagerError::InvalidState);
            }
        }
        cleanup_runtime_mounts(&vm)
            .await
            .with_context(|| format!("clean runtime mounts before deleting VM {id}"))
            .map_err(ManagerError::Other)?;
        fuse::unmount_active_mountpoints_under(&vm.dir.join("fuse-mounts"))
            .await
            .with_context(|| format!("retry path-only FUSE cleanup before deleting VM {id}"))
            .map_err(ManagerError::Other)?;
        ensure_vm_fuse_mounts_detached(id, &vm.dir).map_err(ManagerError::Other)?;
        let (runtime_dir, assignments) = {
            let inner = vm.lock().await;
            (
                inner.runtime.runtime_dir.clone(),
                inner.metadata.pci_devices.clone(),
            )
        };

        for assignment in assignments.iter().rev() {
            pci::release_assignment(&self.cfg.pci, assignment)
                .await
                .map_err(ManagerError::Other)?;
        }

        remove_vm_dir_if_detached(id, &vm.dir).map_err(ManagerError::Other)?;
        let _ = fs::remove_dir_all(runtime_dir);
        self.vms.write().await.remove(id);
        if let Some(session_id) = session_id {
            self.session_registry.write().await.remove(&session_id);
        }
        self.snapshots
            .write()
            .await
            .retain(|_, record| record.vm_id != id);
        self.release_pci_leases(id, &assignments).await;
        self.cleanup_fork_base_if_unreferenced(fork_base_path).await;
        self.garbage_collect_orphaned_fork_roots().await;
        Ok(())
    }

    pub async fn fork_vm(
        &self,
        parent_id: &str,
        params: ForkVmParams,
    ) -> ManagerResult<(VmMetadata, VmMetadata, String)> {
        let parent_vm = self.vm_by_id(parent_id).await?;

        let parent_state = {
            let inner = parent_vm.lock().await;
            if !inner.metadata.pci_devices.is_empty() {
                return Err(ManagerError::Unsupported(
                    "VMs with assigned PCI devices cannot be forked".to_string(),
                ));
            }
            inner.runtime.state
        };
        let parent_was_running = matches!(parent_state, VmState::Running | VmState::Paused);

        if matches!(parent_state, VmState::Creating | VmState::Error) {
            return Err(ManagerError::InvalidState);
        }

        if matches!(parent_state, VmState::Running) {
            self.pause_vm(parent_id).await?;
        }

        let mut fork_snapshot = None::<SnapshotMetadata>;
        if parent_was_running {
            let snapshot_params = SnapshotParams {
                label: format!("fork-{parent_id}"),
                description: "chevalier fork point".to_string(),
            };
            // @dive: With background-snapshot the live path does not freeze the guest,
            //        so we take a single straight-line snapshot. No offline fallback:
            //        fork is a live operation and if the snapshot fails the fork fails.
            let snapshot = self.create_snapshot(parent_id, snapshot_params).await?;
            self.force_stop_vm(parent_id).await?;
            fork_snapshot = Some(snapshot);
        }
        // @dive: A forked child must cold-boot its own bootstrap ISO so its network,
        //        auth token, and instance identity replace the parent's running guest
        //        configuration. The parent still resumes its RAM snapshot; the child
        //        starts from the exact qcow2 disk/volume fork point without inheriting
        //        the parent's live userspace or NIC state.
        let child_restore_snapshot = None::<SnapshotMetadata>;

        let (
            parent_name,
            parent_arch,
            parent_resources,
            parent_source,
            parent_metadata,
            parent_shared_mounts,
            parent_durable_volume,
            parent_depth,
        ) = {
            let inner = parent_vm.lock().await;
            if !matches!(inner.runtime.state, VmState::Stopped) {
                return Err(ManagerError::InvalidState);
            }
            let parent_depth = inner
                .metadata
                .metadata
                .get(META_FORK_DEPTH)
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(0);
            (
                inner.metadata.name.clone(),
                inner.metadata.architecture.clone(),
                inner.metadata.resources.clone(),
                inner.metadata.source.clone(),
                inner.metadata.metadata.clone(),
                inner.metadata.shared_mounts.clone(),
                inner.metadata.durable_volume.clone(),
                parent_depth,
            )
        };
        let child_depth = parent_depth.saturating_add(1);
        if child_depth > self.cfg.max_fork_chain_depth {
            return Err(ManagerError::CapacityExceeded {
                resource: "fork_chain_depth",
                limit: self.cfg.max_fork_chain_depth,
                current: child_depth,
            });
        }

        let child_id = Uuid::new_v4().to_string();
        let fork_id = Uuid::new_v4().to_string();
        let (fork_durability_class, fork_restore_scope) =
            fork_snapshot_durability(self.cfg.storage_profile, parent_was_running);
        let child_name = sanitize_name(
            params
                .child_name
                .as_deref()
                .unwrap_or(&format!("{parent_name}-fork")),
        );
        let mut child_metadata = parent_metadata;
        child_metadata.extend(params.child_metadata);
        let vm_storage_profile = child_metadata
            .get(META_STORAGE_PROFILE)
            .cloned()
            .unwrap_or_else(|| self.cfg.storage_profile.as_str().to_string());
        child_metadata.insert(META_FORK_ID.to_string(), fork_id.clone());
        child_metadata.insert(META_PARENT_VM_ID.to_string(), parent_id.to_string());
        child_metadata.insert(META_FORK_DEPTH.to_string(), child_depth.to_string());
        child_metadata.insert(META_STORAGE_PROFILE.to_string(), vm_storage_profile.clone());
        child_metadata.insert(
            META_FORK_DURABILITY_CLASS.to_string(),
            fork_durability_class.to_string(),
        );
        child_metadata.insert(
            META_FORK_RESTORE_SCOPE.to_string(),
            fork_restore_scope.to_string(),
        );
        assign_new_portproxy_auth_token(&mut child_metadata);
        child_metadata.remove(META_FORK_BASE_PATH);
        child_metadata.remove(META_EXEC_RESTORE_SNAPSHOT_ID);
        child_metadata.remove(META_EXEC_RESTORE_SNAPSHOT_NAME);
        let child_durable_volume = parent_durable_volume.as_ref().map(|parent_volume| {
            let child_owner_key = child_metadata
                .get(META_SESSION_ID)
                .filter(|value| !value.trim().is_empty())
                .map(|session_id| format!("fork:{session_id}"))
                .unwrap_or_else(|| format!("fork:{child_id}"));
            DurableVolumeAttachment {
                volume_id: child_owner_key.clone(),
                owner_key: child_owner_key,
                size_gb: parent_volume.size_gb,
            }
        });

        let child_dir = PathBuf::from(&self.cfg.data_dir).join(&child_id);
        let parent_disk = parent_vm.disk_path();

        let mut child_meta = VmMetadata {
            id: child_id.clone(),
            name: child_name,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: VmState::Stopped,
            architecture: parent_arch.clone(),
            source: VmSource {
                source_type: parent_source.source_type,
                reference: parent_source.reference,
            },
            resources: parent_resources.clone(),
            network: NetworkSpec {
                mac: random_mac().map_err(ManagerError::Other)?,
                proxy_port: 0,
                rpc_port: 0,
            },
            metadata: child_metadata,
            snapshots: child_restore_snapshot.clone().into_iter().collect(),
            shared_mounts: parent_shared_mounts,
            pci_devices: Vec::new(),
            durable_volume: child_durable_volume.clone(),
            boot_incoming_ram_path: String::new(),
            started_at: None,
        };
        ensure_mount_profiles_supported(
            &child_meta.shared_mounts,
            &self.cfg.shared_mount_profiles,
        )?;

        let parent_before_metadata = {
            let inner = parent_vm.lock().await;
            inner.metadata.clone()
        };

        let parent_after;
        if let Some(fork_snapshot_ref) = fork_snapshot.as_ref() {
            let snapshot_name = fork_snapshot_ref.name.clone();
            let snapshot_ram_file_name = fork_snapshot_ref.ram_file_name.clone();
            if let Err(err) = fs::create_dir_all(&child_dir) {
                let _ = self.start_vm(parent_id).await;
                return Err(ManagerError::Io(err));
            }
            // Keep the ordinary VM directory layout even though the child cold-boots
            // from the disk/volume fork and therefore owns no RAM snapshot.
            if let Err(err) = fs::create_dir_all(child_dir.join("snapshots")) {
                let _ = fs::remove_dir_all(&child_dir);
                let _ = self.start_vm(parent_id).await;
                return Err(ManagerError::Io(err));
            }

            // @dive: The production data path may be ext4 or another filesystem without
            //        FICLONE support. Move the stopped point-in-time disk into an
            //        immutable qcow2 base and give parent/child separate overlays, the
            //        same portable CoW model used for stopped forks and durable volumes.
            let fork_root = self.fork_base_root().join(&fork_id);
            if let Err(err) = fs::create_dir_all(&fork_root) {
                let _ = fs::remove_dir_all(&child_dir);
                let _ = self.start_vm(parent_id).await;
                return Err(ManagerError::Io(err));
            }
            let fork_base = fork_root.join("base.qcow2");
            if let Err(err) = fs::rename(&parent_disk, &fork_base) {
                let _ = fs::remove_dir_all(&fork_root);
                let _ = fs::remove_dir_all(&child_dir);
                let _ = self.start_vm(parent_id).await;
                return Err(ManagerError::Io(err));
            }

            let parent_size_gb = parent_resources.disk_gb.max(BASE_IMAGE_SIZE_GB);
            if let Err(err) = virt::create_overlay(
                &self.cfg.qemu_img_bin,
                fork_base.as_path(),
                parent_disk.as_path(),
                parent_size_gb,
            )
            .await
            {
                Self::rollback_stopped_fork_artifacts(
                    parent_disk.as_path(),
                    fork_base.as_path(),
                    fork_root.as_path(),
                    child_dir.as_path(),
                );
                let _ = self.start_vm(parent_id).await;
                return Err(ManagerError::Other(err));
            }

            let child_disk = child_dir.join("disk.qcow2");
            let child_size_gb = child_meta.resources.disk_gb.max(BASE_IMAGE_SIZE_GB);
            if let Err(err) = virt::create_overlay(
                &self.cfg.qemu_img_bin,
                fork_base.as_path(),
                child_disk.as_path(),
                child_size_gb,
            )
            .await
            {
                Self::rollback_stopped_fork_artifacts(
                    parent_disk.as_path(),
                    fork_base.as_path(),
                    fork_root.as_path(),
                    child_dir.as_path(),
                );
                let _ = self.start_vm(parent_id).await;
                return Err(ManagerError::Other(err));
            }
            child_meta.metadata.insert(
                META_FORK_BASE_PATH.to_string(),
                fork_base.to_string_lossy().to_string(),
            );

            let parent_ram_path = parent_vm
                .dir
                .join("snapshots")
                .join(&snapshot_ram_file_name);

            let bootstrap_path = child_dir.join("bootstrap.iso");
            if let Err(err) = bootstrap::create_iso(
                &bootstrap_path,
                bootstrap::Config {
                    instance_id: child_id.clone(),
                    hostname: child_meta.name.clone(),
                    arch: parent_arch,
                    shared_mounts: child_meta
                        .shared_mounts
                        .iter()
                        .cloned()
                        .map(map_bootstrap_shared_mount)
                        .collect(),
                    network: Some(map_bootstrap_network(&child_meta)),
                    http_proxy_url: None,
                    portproxy_auth_token: child_meta
                        .metadata
                        .get(META_PORTPROXY_AUTH_TOKEN)
                        .cloned(),
                    durable_volume: child_meta.durable_volume.is_some(),
                },
            ) {
                Self::rollback_stopped_fork_artifacts(
                    parent_disk.as_path(),
                    fork_base.as_path(),
                    fork_root.as_path(),
                    child_dir.as_path(),
                );
                let _ = self.start_vm(parent_id).await;
                return Err(ManagerError::Other(err));
            }
            if let Err(err) = save_metadata(&child_dir, &mut child_meta) {
                Self::rollback_stopped_fork_artifacts(
                    parent_disk.as_path(),
                    fork_base.as_path(),
                    fork_root.as_path(),
                    child_dir.as_path(),
                );
                let _ = self.start_vm(parent_id).await;
                return Err(ManagerError::Other(err));
            }

            {
                let mut inner = parent_vm.lock().await;
                inner
                    .metadata
                    .metadata
                    .insert(META_FORK_ID.to_string(), fork_id.clone());
                inner
                    .metadata
                    .metadata
                    .insert(META_FORK_DEPTH.to_string(), parent_depth.to_string());
                inner
                    .metadata
                    .metadata
                    .insert(META_STORAGE_PROFILE.to_string(), vm_storage_profile.clone());
                inner.metadata.metadata.insert(
                    META_FORK_DURABILITY_CLASS.to_string(),
                    fork_durability_class.to_string(),
                );
                inner.metadata.metadata.insert(
                    META_FORK_RESTORE_SCOPE.to_string(),
                    fork_restore_scope.to_string(),
                );
                inner
                    .metadata
                    .metadata
                    .insert(META_FORK_SNAPSHOT.to_string(), snapshot_name.clone());
                inner.metadata.metadata.insert(
                    META_FORK_BASE_PATH.to_string(),
                    fork_base.to_string_lossy().to_string(),
                );
                // @dive: Parent relaunches from the snapshot via -incoming. Parent
                //        and child have separate path links to the immutable RAM file.
                inner.metadata.boot_incoming_ram_path =
                    parent_ram_path.to_string_lossy().into_owned();
                if let Err(err) = save_metadata(&parent_vm.dir, &mut inner.metadata) {
                    inner.metadata = parent_before_metadata.clone();
                    Self::rollback_stopped_fork_artifacts(
                        parent_disk.as_path(),
                        fork_base.as_path(),
                        fork_root.as_path(),
                        child_dir.as_path(),
                    );
                    drop(inner);
                    let _ = self.start_vm(parent_id).await;
                    return Err(ManagerError::Other(err));
                }
            }

            if let (Some(parent_volume), Some(child_volume)) = (
                parent_durable_volume.as_ref(),
                child_durable_volume.as_ref(),
            ) {
                if let Err(err) = self
                    .fork_durable_volume(parent_volume, child_volume, &fork_id)
                    .await
                {
                    Self::rollback_stopped_fork_artifacts(
                        parent_disk.as_path(),
                        fork_base.as_path(),
                        fork_root.as_path(),
                        child_dir.as_path(),
                    );
                    let mut inner = parent_vm.lock().await;
                    inner.metadata = parent_before_metadata.clone();
                    let _ = save_metadata(&parent_vm.dir, &mut inner.metadata);
                    drop(inner);
                    let _ = self.start_vm(parent_id).await;
                    return Err(err);
                }
            }
            let started_parent = self.start_vm(parent_id).await;
            if let Err(err) = started_parent {
                if let (Some(parent_volume), Some(child_volume)) = (
                    parent_durable_volume.as_ref(),
                    child_durable_volume.as_ref(),
                ) {
                    self.rollback_durable_volume_fork(parent_volume, child_volume, &fork_id)
                        .await;
                }
                Self::rollback_stopped_fork_artifacts(
                    parent_disk.as_path(),
                    fork_base.as_path(),
                    fork_root.as_path(),
                    child_dir.as_path(),
                );
                let mut inner = parent_vm.lock().await;
                inner.metadata = parent_before_metadata.clone();
                let _ = save_metadata(&parent_vm.dir, &mut inner.metadata);
                return Err(err);
            }

            {
                let mut inner = parent_vm.lock().await;
                inner.metadata.boot_incoming_ram_path.clear();
                if let Err(err) = save_metadata(&parent_vm.dir, &mut inner.metadata) {
                    warn!(
                        vm_id = %parent_id,
                        error = %err,
                        "failed to clear one-shot parent fork incoming path"
                    );
                }
            }
            parent_after = self.get(parent_id).await?;
        } else {
            let fork_root = self.fork_base_root().join(&fork_id);
            fs::create_dir_all(&fork_root)?;
            let fork_base = fork_root.join("base.qcow2");
            fs::rename(&parent_disk, &fork_base)?;

            let parent_size_gb = parent_resources.disk_gb.max(BASE_IMAGE_SIZE_GB);
            if let Err(err) = virt::create_overlay(
                &self.cfg.qemu_img_bin,
                fork_base.as_path(),
                parent_disk.as_path(),
                parent_size_gb,
            )
            .await
            {
                Self::rollback_stopped_fork_artifacts(
                    parent_disk.as_path(),
                    fork_base.as_path(),
                    fork_root.as_path(),
                    child_dir.as_path(),
                );
                return Err(ManagerError::Other(err));
            }

            if let Err(err) = fs::create_dir_all(&child_dir) {
                Self::rollback_stopped_fork_artifacts(
                    parent_disk.as_path(),
                    fork_base.as_path(),
                    fork_root.as_path(),
                    child_dir.as_path(),
                );
                return Err(ManagerError::Io(err));
            }

            child_meta.metadata.insert(
                META_FORK_BASE_PATH.to_string(),
                fork_base.to_string_lossy().to_string(),
            );

            let child_disk = child_dir.join("disk.qcow2");
            let child_size_gb = child_meta.resources.disk_gb.max(BASE_IMAGE_SIZE_GB);
            if let Err(err) = virt::create_overlay(
                &self.cfg.qemu_img_bin,
                fork_base.as_path(),
                child_disk.as_path(),
                child_size_gb,
            )
            .await
            {
                Self::rollback_stopped_fork_artifacts(
                    parent_disk.as_path(),
                    fork_base.as_path(),
                    fork_root.as_path(),
                    child_dir.as_path(),
                );
                return Err(ManagerError::Other(err));
            }

            let bootstrap_path = child_dir.join("bootstrap.iso");
            if let Err(err) = bootstrap::create_iso(
                &bootstrap_path,
                bootstrap::Config {
                    instance_id: child_id.clone(),
                    hostname: child_meta.name.clone(),
                    arch: parent_arch,
                    shared_mounts: child_meta
                        .shared_mounts
                        .iter()
                        .cloned()
                        .map(map_bootstrap_shared_mount)
                        .collect(),
                    network: Some(map_bootstrap_network(&child_meta)),
                    http_proxy_url: None,
                    portproxy_auth_token: child_meta
                        .metadata
                        .get(META_PORTPROXY_AUTH_TOKEN)
                        .cloned(),
                    durable_volume: child_meta.durable_volume.is_some(),
                },
            ) {
                Self::rollback_stopped_fork_artifacts(
                    parent_disk.as_path(),
                    fork_base.as_path(),
                    fork_root.as_path(),
                    child_dir.as_path(),
                );
                return Err(ManagerError::Other(err));
            }
            if let Err(err) = save_metadata(&child_dir, &mut child_meta) {
                Self::rollback_stopped_fork_artifacts(
                    parent_disk.as_path(),
                    fork_base.as_path(),
                    fork_root.as_path(),
                    child_dir.as_path(),
                );
                return Err(ManagerError::Other(err));
            }

            {
                let mut inner = parent_vm.lock().await;
                inner
                    .metadata
                    .metadata
                    .insert(META_FORK_ID.to_string(), fork_id.clone());
                inner
                    .metadata
                    .metadata
                    .insert(META_FORK_DEPTH.to_string(), parent_depth.to_string());
                inner
                    .metadata
                    .metadata
                    .insert(META_STORAGE_PROFILE.to_string(), vm_storage_profile);
                inner.metadata.metadata.insert(
                    META_FORK_DURABILITY_CLASS.to_string(),
                    fork_durability_class.to_string(),
                );
                inner.metadata.metadata.insert(
                    META_FORK_RESTORE_SCOPE.to_string(),
                    fork_restore_scope.to_string(),
                );
                inner.metadata.metadata.insert(
                    META_FORK_BASE_PATH.to_string(),
                    fork_base.to_string_lossy().to_string(),
                );
                inner.metadata.metadata.remove(META_FORK_SNAPSHOT);
                if let Err(err) = save_metadata(&parent_vm.dir, &mut inner.metadata) {
                    Self::rollback_stopped_fork_artifacts(
                        parent_disk.as_path(),
                        fork_base.as_path(),
                        fork_root.as_path(),
                        child_dir.as_path(),
                    );
                    return Err(ManagerError::Other(err));
                }
                parent_after = inner.metadata.clone();
            }
            if let (Some(parent_volume), Some(child_volume)) = (
                parent_durable_volume.as_ref(),
                child_durable_volume.as_ref(),
            ) {
                if let Err(err) = self
                    .fork_durable_volume(parent_volume, child_volume, &fork_id)
                    .await
                {
                    Self::rollback_stopped_fork_artifacts(
                        parent_disk.as_path(),
                        fork_base.as_path(),
                        fork_root.as_path(),
                        child_dir.as_path(),
                    );
                    return Err(err);
                }
            }
        }

        let child_vm = Arc::new(Vm::new(
            child_meta.clone(),
            VmRuntime::new(&child_dir),
            child_dir,
        ));
        self.vms.write().await.insert(child_id.clone(), child_vm);
        if let Some(session_id) = child_meta
            .metadata
            .get(META_SESSION_ID)
            .filter(|value| !value.trim().is_empty())
        {
            self.session_registry
                .write()
                .await
                .insert(session_id.clone(), child_id.clone());
            info!(session_id = %session_id, vm_id = %child_id, "registered forked session");
        }
        if let Some(snapshot) = child_restore_snapshot {
            self.snapshots.write().await.insert(
                snapshot.id.clone(),
                SnapshotRecord {
                    vm_id: child_id.clone(),
                    snapshot,
                },
            );
        }

        let child_after = if params.auto_start_child {
            self.start_vm(&child_id).await?
        } else {
            child_meta
        };

        if child_depth >= self.cfg.fork_compaction_depth_threshold {
            self.enqueue_fork_compaction_task(&fork_id, parent_id, &child_id, child_depth)
                .await;
        }

        Ok((parent_after, child_after, fork_id))
    }

    pub async fn list_snapshots(&self, vm_id: &str) -> ManagerResult<Vec<SnapshotMetadata>> {
        let vm = self.vm_by_id(vm_id).await?;
        let inner = vm.lock().await;
        Ok(inner.metadata.snapshots.clone())
    }

    pub async fn snapshot(
        &self,
        vm_id: &str,
        snapshot_id: &str,
    ) -> ManagerResult<SnapshotMetadata> {
        let vm = self.vm_by_id(vm_id).await?;
        let inner = vm.lock().await;
        inner
            .metadata
            .snapshots
            .iter()
            .find(|snap| snap.id == snapshot_id)
            .cloned()
            .ok_or(ManagerError::SnapshotNotFound)
    }

    #[instrument(skip(self, params), fields(vm_id = %vm_id))]
    pub async fn create_snapshot(
        &self,
        vm_id: &str,
        params: SnapshotParams,
    ) -> ManagerResult<SnapshotMetadata> {
        let pending = self.create_snapshot_qemu_phase(vm_id, params).await?;
        self.promote_staged_snapshot(vm_id, pending).await
    }

    /// First phase of snapshot creation: validate state, run the qemu
    /// background migrate, return the in-flight handle. Holds the userfaultfd-WP
    /// write window — callers that want to serialize against attach_daemon
    /// should hold their per-vm wait-gate Mutex around this call only, then
    /// drop it before invoking `promote_staged_snapshot` so the slow Filestore
    /// copy does not block subsequent exec.stream.start handlers.
    pub async fn create_snapshot_qemu_phase(
        &self,
        vm_id: &str,
        params: SnapshotParams,
    ) -> ManagerResult<PendingSnapshot> {
        let vm = self.vm_by_id(vm_id).await?;
        let (state, monitor, disk_path, vm_dir, arch, has_pci_devices) = {
            let inner = vm.lock().await;
            (
                inner.runtime.state,
                inner.runtime.monitor.clone(),
                vm.disk_path(),
                vm.dir.clone(),
                inner.metadata.architecture.clone(),
                !inner.metadata.pci_devices.is_empty(),
            )
        };

        if has_pci_devices {
            return Err(ManagerError::Unsupported(
                "VMs with assigned PCI devices cannot be snapshotted".to_string(),
            ));
        }

        // @dive: Snapshots are always live, paired {disk internal snapshot, external RAM
        //        file} pairs. Stopped VMs have no RAM state to preserve, and a disk-only
        //        marker snapshot doesn't serve any real resume use case — see the plan at
        //        /Users/crow/.claude/plans/immutable-munching-pearl.md for the rationale.
        if !matches!(state, VmState::Running | VmState::Paused) {
            return Err(ManagerError::InvalidState);
        }
        let monitor =
            monitor.ok_or_else(|| ManagerError::Other(anyhow!("vm monitor unavailable")))?;

        let mut meta = new_snapshot_metadata(params.label, params.description);
        meta.guest_runtime_fingerprint = Some(current_guest_runtime_fingerprint(arch.as_str())?);
        let snapshots_dir = vm_dir.join("snapshots");
        tokio::fs::create_dir_all(&snapshots_dir)
            .await
            .with_context(|| format!("ensure snapshots dir {}", snapshots_dir.display()))
            .map_err(ManagerError::Other)?;
        ensure_qemu_writable_snapshot_dir(snapshots_dir.as_path(), &self.cfg)
            .with_context(|| format!("prepare snapshots dir {}", snapshots_dir.display()))
            .map_err(ManagerError::Other)?;
        let canonical_ram_path = snapshots_dir.join(&meta.ram_file_name);

        // @dive: When a fast-local staging dir is configured, qemu writes the RAM
        //        file there first so the userfaultfd-WP storm completes at local
        //        SSD speed (the in-guest network-facing services stay responsive)
        //        and we then dump the snapshot onto durable shared storage. Without
        //        staging, qemu writes directly to the canonical (durable) path —
        //        which on slow shared storage causes the migration thread to block
        //        on writes, leaving WP faults un-drained and wedging guest network
        //        I/O for the duration of the snapshot.
        let staging_ram_path = self.cfg.snapshot_staging_dir.as_ref().map(|staging| {
            std::path::PathBuf::from(staging)
                .join(vm_id)
                .join(&meta.ram_file_name)
        });
        let qemu_ram_path = staging_ram_path
            .clone()
            .unwrap_or_else(|| canonical_ram_path.clone());
        if let Some(staging_path) = staging_ram_path.as_ref() {
            if let Some(parent) = staging_path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .with_context(|| format!("ensure snapshot staging dir {}", parent.display()))
                    .map_err(ManagerError::Other)?;
                ensure_qemu_writable_snapshot_dir(parent, &self.cfg)
                    .with_context(|| format!("prepare snapshot staging dir {}", parent.display()))
                    .map_err(ManagerError::Other)?;
            }
        }

        // @dive: QEMU background-snapshot can transiently stall guest exec while
        //        userfaultfd-WP drains pages. That does not mean the VM is wedged;
        //        suppress the background health reconciler during the QMP phase so
        //        our own snapshot maintenance does not kill an otherwise healthy VM.
        self.suppress_vm_health_probes_for(
            vm_id,
            Duration::from_secs(virt::BACKGROUND_SNAPSHOT_TIMEOUT_SECS + 30),
        )
        .await?;
        let opts = virt::BackgroundSnapshotOptions::default();
        let save_result = virt::save_vm_background(
            &monitor,
            disk_path.as_path(),
            &meta.name,
            qemu_ram_path.as_path(),
            &opts,
        )
        .await;
        let _ = self.clear_vm_health_probe_suppression(vm_id).await;
        let status = save_result.map_err(ManagerError::Other)?;
        meta.ram_format = status.ram_format.clone();
        debug!(
            vm_id = %vm_id,
            snapshot = %meta.name,
            ram_bytes = ?status.bytes_transferred,
            ram_format = %meta.ram_format,
            total_ms = ?status.total_time_ms,
            staged = staging_ram_path.is_some(),
            "background snapshot qemu phase completed"
        );

        Ok(PendingSnapshot {
            meta,
            staging_ram_path,
            canonical_ram_path,
        })
    }

    /// Second phase of snapshot creation: copy the staged RAM file onto durable
    /// shared storage and persist the snapshot metadata. Safe to run without
    /// any per-vm lock — qemu has already released userfaultfd-WP, so the slow
    /// Filestore IO does not interfere with guest network I/O or with the next
    /// exec's attach_daemon RPC.
    pub async fn promote_staged_snapshot(
        &self,
        vm_id: &str,
        pending: PendingSnapshot,
    ) -> ManagerResult<SnapshotMetadata> {
        let PendingSnapshot {
            meta,
            staging_ram_path,
            canonical_ram_path,
        } = pending;

        if let Some(staging_path) = staging_ram_path.as_ref() {
            let copy_start = std::time::Instant::now();
            tokio::fs::copy(staging_path, &canonical_ram_path)
                .await
                .with_context(|| {
                    format!(
                        "copy staged snapshot {} → durable {}",
                        staging_path.display(),
                        canonical_ram_path.display()
                    )
                })
                .map_err(ManagerError::Other)?;
            if let Err(err) = tokio::fs::remove_file(staging_path).await {
                tracing::warn!(
                    vm_id = %vm_id,
                    snapshot = %meta.name,
                    staging = %staging_path.display(),
                    error = %err,
                    "failed removing staged snapshot file after durable copy; orphan will accumulate"
                );
            }
            debug!(
                vm_id = %vm_id,
                snapshot = %meta.name,
                copy_ms = copy_start.elapsed().as_millis() as u64,
                "promoted staged snapshot to durable storage"
            );
        }

        let vm = self.vm_by_id(vm_id).await?;
        {
            let mut inner = vm.lock().await;
            inner.metadata.snapshots.push(meta.clone());
            save_metadata(&vm.dir, &mut inner.metadata).map_err(ManagerError::Other)?;
        }
        self.snapshots.write().await.insert(
            meta.id.clone(),
            SnapshotRecord {
                vm_id: vm_id.to_string(),
                snapshot: meta.clone(),
            },
        );
        Ok(meta)
    }

    pub async fn delete_snapshot(&self, vm_id: &str, snapshot_id: &str) -> ManagerResult<()> {
        let vm = self.vm_by_id(vm_id).await?;
        let (state, monitor, disk_path, snapshot, vm_dir) = {
            let inner = vm.lock().await;
            let position = inner
                .metadata
                .snapshots
                .iter()
                .position(|snap| snap.id == snapshot_id)
                .ok_or(ManagerError::SnapshotNotFound)?;
            let snapshot = inner.metadata.snapshots[position].clone();
            (
                inner.runtime.state,
                inner.runtime.monitor.clone(),
                vm.disk_path(),
                snapshot,
                vm.dir.clone(),
            )
        };

        // @dive: Delete dispatches on current VM state. Running VMs own the qcow2 file
        //        and the disk snapshot is removed via QMP; stopped VMs use the offline
        //        qemu-img path. Both branches then unlink the external RAM file. This is
        //        runtime dispatch, not a fallback.
        match state {
            VmState::Running | VmState::Paused => {
                let monitor = monitor
                    .ok_or_else(|| ManagerError::Other(anyhow!("vm monitor unavailable")))?;
                let device = virt::find_block_device_id(&monitor, disk_path.as_path())
                    .await
                    .map_err(ManagerError::Other)?;
                if let Err(err) =
                    virt::blockdev_snapshot_delete_internal_sync(&monitor, &device, &snapshot.name)
                        .await
                {
                    if !is_snapshot_not_found(&err) {
                        return Err(ManagerError::Other(err));
                    }
                }
            }
            VmState::Stopped => {
                virt::delete_snapshot_offline(
                    &self.cfg.qemu_img_bin,
                    disk_path.as_path(),
                    &snapshot.name,
                )
                .await
                .map_err(ManagerError::Other)?;
            }
            _ => return Err(ManagerError::InvalidState),
        }

        // Always unlink the external RAM file regardless of VM state.
        let ram_path = vm_dir.join("snapshots").join(&snapshot.ram_file_name);
        if let Err(err) = tokio::fs::remove_file(&ram_path).await {
            if err.kind() != std::io::ErrorKind::NotFound {
                warn!(
                    vm_id = %vm_id,
                    snapshot = %snapshot.name,
                    path = %ram_path.display(),
                    error = %err,
                    "failed removing external ram snapshot file"
                );
            }
        }

        {
            let mut inner = vm.lock().await;
            inner
                .metadata
                .snapshots
                .retain(|snap| snap.id != snapshot_id);
            save_metadata(&vm.dir, &mut inner.metadata).map_err(ManagerError::Other)?;
        }

        self.snapshots.write().await.remove(snapshot_id);
        Ok(())
    }

    #[instrument(skip(self), fields(vm_id = %id))]
    pub async fn start_vm(&self, id: &str) -> ManagerResult<VmMetadata> {
        let vm = self.vm_by_id(id).await?;
        let cfg = self.cfg.clone();
        let host_arch = self.host_arch.clone();
        let id = id.to_string();

        let booting_from_incoming = {
            let inner = vm.lock().await;
            !inner.metadata.boot_incoming_ram_path.is_empty()
                && boot_incoming_snapshot_matches_current(&inner.metadata)?
        };
        if booting_from_incoming {
            // A deferred incoming qemu is not registered until restore completes. Keep
            // that launch caller-owned so cancellation drops its kill-on-drop child.
            return Self::start_vm_inner(cfg, host_arch, vm, id).await;
        }

        // Ordinary launches retain their established owned-task behavior so caller
        // cancellation cannot interrupt sidecar and runtime registration cleanup.
        match tokio::spawn(async move { Self::start_vm_inner(cfg, host_arch, vm, id).await }).await
        {
            Ok(result) => result,
            Err(err) => Err(ManagerError::Other(anyhow!(
                "start_vm launch task failed: {err}"
            ))),
        }
    }

    #[instrument(skip(self), fields(vm_id = %id))]
    pub async fn start_vm_for_exec_stream_resume(&self, id: &str) -> ManagerResult<VmMetadata> {
        let vm = self.vm_by_id(id).await?;
        if let Some(meta) = Self::try_adopt_local_runtime_for_resume(&self.cfg, vm, id).await? {
            return Ok(meta);
        }
        self.start_vm(id).await
    }

    async fn try_adopt_local_runtime_for_resume(
        cfg: &Config,
        vm: Arc<Vm>,
        id: &str,
    ) -> ManagerResult<Option<VmMetadata>> {
        let (vm_dir, qmp_path, pid_path) = {
            let inner = vm.lock().await;
            if matches!(inner.runtime.state, VmState::Running) {
                return Ok(Some(inner.metadata.clone()));
            }
            (
                vm.dir.clone(),
                inner.runtime.qmp_path.clone(),
                inner.runtime.pid_path.clone(),
            )
        };
        if !qmp_path.exists() {
            return Ok(None);
        }

        let monitor =
            match virt::wait_for_monitor(qmp_path.as_path(), Duration::from_millis(750)).await {
                Ok(monitor) => monitor,
                Err(err) => {
                    debug!(
                        vm_id = %id,
                        qmp_path = %qmp_path.display(),
                        error = %err,
                        "existing qmp socket was not adoptable for exec stream resume"
                    );
                    return Ok(None);
                }
            };
        if let Err(err) = virt::wait_for_running(&monitor, Duration::from_secs(2)).await {
            debug!(
                vm_id = %id,
                qmp_path = %qmp_path.display(),
                error = %err,
                "existing qemu runtime was not running for exec stream resume adoption"
            );
            return Ok(None);
        }

        let Some(pid) = read_pid_file(pid_path.as_path()).filter(|pid| pid_exists(*pid)) else {
            debug!(
                vm_id = %id,
                pid_path = %pid_path.display(),
                "existing qemu runtime had no live pid for exec stream resume adoption"
            );
            return Ok(None);
        };
        let Some(command) = read_process_command(pid) else {
            debug!(
                vm_id = %id,
                pid,
                "existing qemu pid could not be inspected for exec stream resume adoption"
            );
            return Ok(None);
        };
        if !pid_command_matches_vm(command.as_str(), vm_dir.as_path(), qmp_path.as_path()) {
            debug!(
                vm_id = %id,
                pid,
                command = %command,
                "existing qemu pid did not match VM markers for exec stream resume adoption"
            );
            return Ok(None);
        }

        let meta = {
            let mut inner = vm.lock().await;
            if matches!(inner.runtime.state, VmState::Running) {
                return Ok(Some(inner.metadata.clone()));
            }
            inner.runtime.monitor = Some(monitor);
            inner.runtime.state = VmState::Running;
            inner.runtime.started_at = Some(Utc::now());
            inner.runtime.reset_health_tracking();
            inner.runtime.command_pid = Some(pid);
            {
                let mut exit = inner
                    .runtime
                    .exit_status
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                *exit = None;
            }
            inner.metadata.state = VmState::Running;
            inner.metadata.started_at = inner.runtime.started_at;
            if let Err(err) = save_metadata(&vm.dir, &mut inner.metadata) {
                warn!(vm_id = %id, error = %err, "failed to persist metadata after runtime adoption");
            }
            inner.metadata.clone()
        };

        if let Err(err) = network::register_vm_process(id, pid).await {
            if cfg.ha_mode {
                mark_vm_state(&vm, VmState::Error).await;
                return Err(ManagerError::Other(err.context(
                    "failed to register adopted vm process for network guardrails",
                )));
            }
            warn!(
                vm_id = %id,
                pid,
                error = %err,
                "failed to register adopted vm process for network guardrails"
            );
        }

        info!(
            vm_id = %id,
            pid,
            rpc_port = meta.network.rpc_port,
            "adopted existing qemu runtime for exec stream resume"
        );
        Ok(Some(meta))
    }

    async fn start_vm_inner(
        cfg: Config,
        host_arch: String,
        vm: Arc<Vm>,
        id: String,
    ) -> ManagerResult<VmMetadata> {
        let id = id.as_str();

        let (
            binary,
            meta_snapshot,
            vm_dir,
            runtime_dir,
            qmp_path,
            pid_path,
            log_path,
            cleanup_stale_sidecars,
        ) = {
            let mut inner = vm.lock().await;
            if matches!(inner.runtime.state, VmState::Running) {
                return Ok(inner.metadata.clone());
            }
            let starting_from_state = inner.runtime.state;

            if !inner.metadata.pci_devices.is_empty() {
                if !cfg!(target_os = "linux")
                    || host_arch != ARCH_AMD64
                    || inner.metadata.architecture != ARCH_AMD64
                    || cfg.ha_mode
                {
                    return Err(ManagerError::Other(anyhow!(
                        "assigned PCI devices require a non-HA amd64 Linux host and guest"
                    )));
                }
                let mut resolved = Vec::with_capacity(inner.metadata.pci_devices.len());
                let mut newly_prepared = Vec::new();
                for current in &inner.metadata.pci_devices {
                    let policy = cfg.pci.device(&current.id).ok_or_else(|| {
                        ManagerError::Other(anyhow!(
                            "assigned PCI device `{}` is no longer in the vmd allowlist",
                            current.id
                        ))
                    })?;
                    if policy.bdfs != current.bdfs {
                        return Err(ManagerError::Other(anyhow!(
                            "assigned PCI device `{}` no longer matches the vmd allowlist",
                            current.id
                        )));
                    }
                    if pci::assignment_ready(&cfg.pci, current) {
                        resolved.push(current.clone());
                        continue;
                    }
                    match pci::prepare_assignment(
                        &cfg.pci,
                        &current.id,
                        cfg.qemu_process.run_as_uid,
                        cfg.qemu_process.run_as_gid,
                    )
                    .await
                    {
                        Ok(assignment) => {
                            newly_prepared.push(assignment.clone());
                            resolved.push(assignment);
                        }
                        Err(error) => {
                            for assignment in newly_prepared.iter().rev() {
                                let _ = pci::release_assignment(&cfg.pci, assignment).await;
                            }
                            let (error, failed_assignment) =
                                match error.downcast::<pci::PciRollbackError>() {
                                    Ok(failure) => {
                                        (anyhow!(failure.to_string()), Some(failure.assignment))
                                    }
                                    Err(error) => (error, None),
                                };
                            let mut durable = inner.metadata.pci_devices.clone();
                            for prepared in newly_prepared.iter().chain(failed_assignment.iter()) {
                                if let Some(slot) = durable
                                    .iter_mut()
                                    .find(|assignment| assignment.id == prepared.id)
                                {
                                    *slot = prepared.clone();
                                }
                            }
                            inner.metadata.pci_devices = durable;
                            if let Err(save_error) = save_metadata(&vm.dir, &mut inner.metadata) {
                                return Err(ManagerError::Other(error.context(format!(
                                    "persisting PCI rollback state also failed: {save_error}"
                                ))));
                            }
                            return Err(ManagerError::Other(error));
                        }
                    }
                }
                inner.metadata.pci_devices = resolved;
                if let Err(error) = save_metadata(&vm.dir, &mut inner.metadata) {
                    for assignment in newly_prepared.iter().rev() {
                        let _ = pci::release_assignment(&cfg.pci, assignment).await;
                    }
                    return Err(ManagerError::Other(error));
                }
            }

            let binary =
                Self::qemu_binary_for_arch_from(&cfg, &host_arch, &inner.metadata.architecture)?;

            let vm_dir = vm.dir.clone();
            let runtime_dir = inner.runtime.runtime_dir.clone();
            let qmp_path = inner.runtime.qmp_path.clone();
            let pid_path = inner.runtime.pid_path.clone();
            let log_path = vm_dir.join("qemu.log");

            // @dive: Failover can leave an orphaned local qemu process; reclaim ownership before relaunch to avoid disk-lock contention.
            let reclaimed_stale_runtime = match reclaim_local_runtime_ownership(
                id,
                vm_dir.as_path(),
                qmp_path.as_path(),
                pid_path.as_path(),
            )
            .await
            {
                Ok(reclaimed) => reclaimed,
                Err(err) => {
                    warn!(
                        vm_id = %id,
                        error = %err,
                        "runtime ownership reclaim failed before restart"
                    );
                    false
                }
            };

            if let Err(err) = prepare_qemu_runtime_dir(runtime_dir.as_path(), &cfg) {
                inner.runtime.state = VmState::Error;
                inner.metadata.state = VmState::Error;
                let _ = save_metadata(&vm.dir, &mut inner.metadata);
                return Err(ManagerError::Other(err));
            }
            let _ = fs::remove_file(&qmp_path);
            let _ = fs::remove_file(&pid_path);

            let mut reserved = HashSet::new();
            let preferred_proxy_port = if reclaimed_stale_runtime {
                0
            } else {
                inner.metadata.network.proxy_port
            };
            let preferred_rpc_port = if reclaimed_stale_runtime {
                0
            } else {
                inner.metadata.network.rpc_port
            };
            // @dive: After reclaiming orphan runtime ownership, rotate host ports to avoid stale hostfwd/socket state from prior qemu process.
            inner.metadata.network.proxy_port =
                allocate_host_port(preferred_proxy_port, &mut reserved)?;
            inner.metadata.network.rpc_port =
                allocate_host_port(preferred_rpc_port, &mut reserved)?;

            inner.runtime.monitor = None;
            inner.runtime.command_pid = None;
            inner.runtime.started_at = None;
            {
                let mut exit = inner
                    .runtime
                    .exit_status
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                *exit = None;
            }
            inner.runtime.state = VmState::Creating;
            inner.metadata.state = VmState::Creating;
            if !boot_incoming_snapshot_matches_current(&inner.metadata)? {
                warn!(
                    vm_id = %id,
                    incoming = %inner.metadata.boot_incoming_ram_path,
                    "clearing incompatible boot_incoming_ram_path before cold starting VM disk"
                );
                inner.metadata.boot_incoming_ram_path.clear();
            }

            // Consume the one-shot incoming path after copying launch metadata. This
            // keeps cancellation cleanup synchronous while the launch copy still emits
            // `-incoming defer` and starts the selected RAM format over QMP.
            let snapshot = inner.metadata.clone();
            inner.metadata.boot_incoming_ram_path.clear();
            save_metadata(&vm.dir, &mut inner.metadata).map_err(ManagerError::Other)?;

            (
                binary,
                snapshot,
                vm_dir,
                runtime_dir,
                qmp_path,
                pid_path,
                log_path,
                reclaimed_stale_runtime
                    || matches!(
                        starting_from_state,
                        VmState::Creating | VmState::Stopped | VmState::Error
                    ),
            )
        };
        let mut meta_snapshot = meta_snapshot;
        cleanup_runtime_mounts(&vm)
            .await
            .with_context(|| format!("clean stale runtime mounts before starting VM {id}"))
            .map_err(ManagerError::Other)?;
        if cleanup_stale_sidecars {
            cleanup_stale_runtime_sidecars(id, vm_dir.as_path(), runtime_dir.as_path()).await;
        }

        let use_managed_tap_network = requires_managed_tap_network(&cfg, &meta_snapshot);
        let (tap_spec, vm_proxy_upstream_addr) = if use_managed_tap_network {
            let mut reserved_proxy_ports = HashSet::from([
                meta_snapshot.network.proxy_port,
                meta_snapshot.network.rpc_port,
            ]);
            let upstream_port = allocate_host_port(0, &mut reserved_proxy_ports)?;
            let upstream_addr = format!("0.0.0.0:{upstream_port}");
            let proxy_policy =
                vm_proxy_policy_from_metadata(&meta_snapshot).map_err(ManagerError::Other)?;
            network::register_vm_proxy_policy(
                id,
                upstream_addr
                    .parse()
                    .with_context(|| format!("parse vm proxy upstream addr {upstream_addr}"))
                    .map_err(ManagerError::Other)?,
                proxy_policy,
            )
            .await
            .map_err(ManagerError::Other)?;
            meta_snapshot.metadata.insert(
                META_NETWORK_POLICY_PROXY_UPSTREAM.to_string(),
                upstream_addr.clone(),
            );
            let tap_spec = network::tap::spec_for_vm(
                id,
                port_forward_bind_address().as_str(),
                meta_snapshot.network.proxy_port,
                meta_snapshot.network.rpc_port,
                upstream_port as u16,
            )
            .map_err(ManagerError::Other)?;
            (Some(tap_spec), Some(upstream_addr))
        } else {
            meta_snapshot
                .metadata
                .remove(META_NETWORK_POLICY_PROXY_UPSTREAM);
            (None, None)
        };
        if let Err(err) = bootstrap::create_iso(
            vm_dir.join("bootstrap.iso"),
            bootstrap::Config {
                instance_id: meta_snapshot.id.clone(),
                hostname: meta_snapshot.name.clone(),
                arch: meta_snapshot.architecture.clone(),
                shared_mounts: meta_snapshot
                    .shared_mounts
                    .iter()
                    .cloned()
                    .map(map_bootstrap_shared_mount)
                    .collect(),
                network: tap_spec
                    .as_ref()
                    .map(|_| map_bootstrap_network(&meta_snapshot)),
                http_proxy_url: None,
                portproxy_auth_token: meta_snapshot
                    .metadata
                    .get(META_PORTPROXY_AUTH_TOKEN)
                    .cloned(),
                durable_volume: meta_snapshot.durable_volume.is_some(),
            },
        ) {
            let _ = network::unregister_vm_proxy_policy(id).await;
            mark_vm_state(&vm, VmState::Error).await;
            return Err(ManagerError::Other(err));
        }

        // @dive: Spawn one virtiofsd subprocess per shared mount BEFORE launching
        //        qemu. Each daemon creates its own vhost-user socket under <vm_dir>/,
        //        which the `-chardev socket,path=...` + `-device vhost-user-fs-pci`
        //        pair in the qemu args below connects to. If any spawn fails, reap
        //        the already-spawned ones and mark the VM in Error state.
        //
        //        virtiofsd is Linux-only (it uses userfaultfd + Linux mount namespaces),
        //        so on macOS / any host where the configured binary is missing we skip
        //        the spawn entirely and let `build_qemu_args` fall back to the legacy
        //        `-virtfs` (virtio-9p) shared-mount transport. That path doesn't support
        //        live migration, but keeps the dev workflow running on non-Linux hosts.
        let mut fuse_handles: Vec<fuse::FuseHandle> = Vec::new();
        for mount in &mut meta_snapshot.shared_mounts {
            if mount.is_fuse_backed() {
                let handle = match fuse::mount_vfs_fuse(&cfg, mount, vm_dir.as_path()).await {
                    Ok(handle) => handle,
                    Err(err) => {
                        if vm_proxy_upstream_addr.is_some() {
                            let _ = network::unregister_vm_proxy_policy(id).await;
                        }
                        cleanup_unpublished_fuse_handles(id, &fuse_handles).await;
                        mark_vm_state(&vm, VmState::Error).await;
                        return Err(ManagerError::Other(err));
                    }
                };
                mount.host_path = handle.mountpoint().to_string_lossy().into_owned();
                fuse_handles.push(handle);
            }
        }

        let mut virtiofsd_handles: Vec<virt::VirtiofsdHandle> = Vec::new();
        let can_use_virtiofsd = cfg!(target_os = "linux")
            && !cfg.virtiofsd_bin.is_empty()
            && Path::new(&cfg.virtiofsd_bin).exists();
        if can_use_virtiofsd {
            for (index, mount) in meta_snapshot.shared_mounts.iter().enumerate() {
                let socket_path = runtime_dir.join(format!("virtiofsd-{index}.sock"));
                let log_path = vm_dir.join(format!("virtiofsd-{index}.log"));
                let spawn = virt::VirtiofsdSpawn {
                    source_path: PathBuf::from(&mount.host_path),
                    socket_path,
                    tag: mount.mount_tag.clone(),
                    read_only: mount.read_only,
                };
                match virt::spawn_virtiofsd(&cfg.virtiofsd_bin, &spawn, log_path.as_path()).await {
                    Ok(handle) => virtiofsd_handles.push(handle),
                    Err(err) => {
                        if vm_proxy_upstream_addr.is_some() {
                            let _ = network::unregister_vm_proxy_policy(id).await;
                        }
                        for previous in &virtiofsd_handles {
                            virt::terminate_virtiofsd(previous);
                        }
                        cleanup_unpublished_fuse_handles(id, &fuse_handles).await;
                        mark_vm_state(&vm, VmState::Error).await;
                        return Err(ManagerError::Other(err));
                    }
                }
            }
        } else if !meta_snapshot.shared_mounts.is_empty() {
            debug!(
                vm_id = %id,
                virtiofsd_bin = %cfg.virtiofsd_bin,
                "virtiofsd unavailable; falling back to legacy -virtfs shared mounts (dev path)"
            );
        }

        let args = match build_qemu_args(
            &meta_snapshot,
            vm_dir.as_path(),
            qmp_path.as_path(),
            pid_path.as_path(),
            &host_arch,
            tap_spec.as_ref(),
            &virtiofsd_handles,
        ) {
            Ok(args) => args,
            Err(err) => {
                if vm_proxy_upstream_addr.is_some() {
                    let _ = network::unregister_vm_proxy_policy(id).await;
                }
                for handle in &virtiofsd_handles {
                    virt::terminate_virtiofsd(handle);
                }
                cleanup_unpublished_fuse_handles(id, &fuse_handles).await;
                mark_vm_state(&vm, VmState::Error).await;
                return Err(ManagerError::Other(err));
            }
        };
        let mut tap_network_handle = match tap_spec.as_ref() {
            Some(tap_spec) => match network::tap::start_vm_tap_network(&cfg, tap_spec).await {
                Ok(handle) => Some(handle),
                Err(err) => {
                    if vm_proxy_upstream_addr.is_some() {
                        let _ = network::unregister_vm_proxy_policy(id).await;
                    }
                    for handle in &virtiofsd_handles {
                        virt::terminate_virtiofsd(handle);
                    }
                    cleanup_unpublished_fuse_handles(id, &fuse_handles).await;
                    mark_vm_state(&vm, VmState::Error).await;
                    return Err(ManagerError::Other(err));
                }
            },
            None => None,
        };
        debug!(
            vm_id = %id,
            binary = %binary,
            args = ?args,
            log_path = %log_path.display(),
            "starting qemu process"
        );

        let mut cmd = Command::new(&binary);
        cmd.args(&args);
        cmd.current_dir(&vm_dir);
        cmd.stdin(std::process::Stdio::null());
        let mut qemu_cgroup = if cfg.ha_mode {
            match network::prepare_vm_process_cgroup(id) {
                Ok(handle) => handle,
                Err(err) => {
                    if let Some(handle) = tap_network_handle.take() {
                        handle.shutdown().await;
                    }
                    if vm_proxy_upstream_addr.is_some() {
                        let _ = network::unregister_vm_proxy_policy(id).await;
                    }
                    for handle in &virtiofsd_handles {
                        virt::terminate_virtiofsd(handle);
                    }
                    cleanup_unpublished_fuse_handles(id, &fuse_handles).await;
                    mark_vm_state(&vm, VmState::Error).await;
                    return Err(ManagerError::Other(err));
                }
            }
        } else {
            None
        };
        if qemu_cgroup.is_some() {
            let cgroup_procs_path = qemu_cgroup
                .as_ref()
                .expect("checked qemu cgroup presence")
                .cgroup_procs_path()
                .to_path_buf();
            if let Err(err) = network::configure_child_cgroup(&mut cmd, &cgroup_procs_path) {
                if let Some(cgroup) = qemu_cgroup.take() {
                    cgroup.cleanup();
                }
                if let Some(handle) = tap_network_handle.take() {
                    handle.shutdown().await;
                }
                if vm_proxy_upstream_addr.is_some() {
                    let _ = network::unregister_vm_proxy_policy(id).await;
                }
                for handle in &virtiofsd_handles {
                    virt::terminate_virtiofsd(handle);
                }
                cleanup_unpublished_fuse_handles(id, &fuse_handles).await;
                mark_vm_state(&vm, VmState::Error).await;
                return Err(ManagerError::Other(err));
            }
        }
        let additional_qemu_writable_paths = meta_snapshot
            .durable_volume
            .as_ref()
            .and_then(|volume| {
                vm_dir.parent().map(|data_dir| {
                    data_dir
                        .join(VOLUMES_DIR_NAME)
                        .join(format!("{}.qcow2", volume.volume_id))
                })
            })
            .into_iter()
            .collect::<Vec<_>>();
        if let Err(err) = configure_qemu_process_identity(
            &mut cmd,
            vm_dir.as_path(),
            runtime_dir.as_path(),
            &additional_qemu_writable_paths,
            &cfg,
        ) {
            if let Some(cgroup) = qemu_cgroup.take() {
                cgroup.cleanup();
            }
            if let Some(handle) = tap_network_handle.take() {
                handle.shutdown().await;
            }
            if vm_proxy_upstream_addr.is_some() {
                let _ = network::unregister_vm_proxy_policy(id).await;
            }
            for handle in &virtiofsd_handles {
                virt::terminate_virtiofsd(handle);
            }
            cleanup_unpublished_fuse_handles(id, &fuse_handles).await;
            mark_vm_state(&vm, VmState::Error).await;
            return Err(ManagerError::Other(err));
        }

        let log_file = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .with_context(|| format!("open qemu log {}", log_path.display()))
        {
            Ok(file) => file,
            Err(err) => {
                if let Some(cgroup) = qemu_cgroup.take() {
                    cgroup.cleanup();
                }
                if let Some(handle) = tap_network_handle.take() {
                    handle.shutdown().await;
                }
                if vm_proxy_upstream_addr.is_some() {
                    let _ = network::unregister_vm_proxy_policy(id).await;
                }
                for handle in &virtiofsd_handles {
                    virt::terminate_virtiofsd(handle);
                }
                cleanup_unpublished_fuse_handles(id, &fuse_handles).await;
                mark_vm_state(&vm, VmState::Error).await;
                return Err(ManagerError::Other(err));
            }
        };
        let stderr_file = match log_file
            .try_clone()
            .with_context(|| format!("clone qemu log {}", log_path.display()))
        {
            Ok(file) => file,
            Err(err) => {
                if let Some(cgroup) = qemu_cgroup.take() {
                    cgroup.cleanup();
                }
                if let Some(handle) = tap_network_handle.take() {
                    handle.shutdown().await;
                }
                if vm_proxy_upstream_addr.is_some() {
                    let _ = network::unregister_vm_proxy_policy(id).await;
                }
                for handle in &virtiofsd_handles {
                    virt::terminate_virtiofsd(handle);
                }
                cleanup_unpublished_fuse_handles(id, &fuse_handles).await;
                mark_vm_state(&vm, VmState::Error).await;
                return Err(ManagerError::Other(err));
            }
        };

        cmd.stdout(std::process::Stdio::from(log_file));
        cmd.stderr(std::process::Stdio::from(stderr_file));

        let booting_from_incoming = !meta_snapshot.boot_incoming_ram_path.is_empty();
        let mut child =
            match spawn_launch_child(&mut cmd, &format!("qemu {binary}"), booting_from_incoming) {
                Ok(child) => child,
                Err(err) => {
                    if let Some(cgroup) = qemu_cgroup.take() {
                        cgroup.cleanup();
                    }
                    if let Some(handle) = tap_network_handle.take() {
                        handle.shutdown().await;
                    }
                    if vm_proxy_upstream_addr.is_some() {
                        let _ = network::unregister_vm_proxy_policy(id).await;
                    }
                    for handle in &virtiofsd_handles {
                        virt::terminate_virtiofsd(handle);
                    }
                    cleanup_unpublished_fuse_handles(id, &fuse_handles).await;
                    mark_vm_state(&vm, VmState::Error).await;
                    return Err(ManagerError::Other(err));
                }
            };

        let monitor =
            match virt::wait_for_monitor(qmp_path.as_path(), Duration::from_secs(20)).await {
                Ok(handle) => handle,
                Err(err) => {
                    if let Some(handle) = tap_network_handle.take() {
                        handle.shutdown().await;
                    }
                    if vm_proxy_upstream_addr.is_some() {
                        let _ = network::unregister_vm_proxy_policy(id).await;
                    }
                    for handle in &virtiofsd_handles {
                        virt::terminate_virtiofsd(handle);
                    }
                    cleanup_unpublished_fuse_handles(id, &fuse_handles).await;
                    let err = abort_launch(vm.clone(), child, &log_path, err).await;
                    if let Some(cgroup) = qemu_cgroup.take() {
                        cgroup.cleanup();
                    }
                    return Err(err);
                }
            };
        if booting_from_incoming {
            let ram_format = boot_incoming_ram_format(&meta_snapshot);
            if let Err(err) = virt::start_incoming_migration_from_file(
                &monitor,
                Path::new(&meta_snapshot.boot_incoming_ram_path),
                ram_format,
                &virt::BackgroundSnapshotOptions::default(),
            )
            .await
            .with_context(|| {
                format!(
                    "start incoming VM restore from {}",
                    meta_snapshot.boot_incoming_ram_path
                )
            }) {
                if let Some(handle) = tap_network_handle.take() {
                    handle.shutdown().await;
                }
                if vm_proxy_upstream_addr.is_some() {
                    let _ = network::unregister_vm_proxy_policy(id).await;
                }
                for handle in &virtiofsd_handles {
                    virt::terminate_virtiofsd(handle);
                }
                cleanup_unpublished_fuse_handles(id, &fuse_handles).await;
                let err = abort_launch(vm.clone(), child, &log_path, err).await;
                if let Some(cgroup) = qemu_cgroup.take() {
                    cgroup.cleanup();
                }
                return Err(err);
            }
        }
        // Cooled/restarted VMs can spend longer in QMP pre-running states while
        // restoring disk/memory and reconnecting shared mounts. When booting from a
        // live RAM snapshot, QEMU sits in `inmigrate`; supervise migration progress
        // instead of treating a healthy-but-slow restore like a stuck launch.
        let running_result = if booting_from_incoming {
            virt::wait_for_running_or_incoming_restore(
                &monitor,
                VM_RUNNING_TIMEOUT,
                INCOMING_RESTORE_TOTAL_TIMEOUT,
                INCOMING_RESTORE_STALL_TIMEOUT,
            )
            .await
        } else {
            virt::wait_for_running(&monitor, VM_RUNNING_TIMEOUT).await
        };
        if let Err(err) = running_result {
            if let Some(handle) = tap_network_handle.take() {
                handle.shutdown().await;
            }
            if vm_proxy_upstream_addr.is_some() {
                let _ = network::unregister_vm_proxy_policy(id).await;
            }
            for handle in &virtiofsd_handles {
                virt::terminate_virtiofsd(handle);
            }
            cleanup_unpublished_fuse_handles(id, &fuse_handles).await;
            let err = abort_launch(vm.clone(), child, &log_path, err).await;
            if let Some(cgroup) = qemu_cgroup.take() {
                cgroup.cleanup();
            }
            return Err(err);
        }

        let child_pid = child.id();
        {
            let mut inner = vm.lock().await;
            inner.runtime.monitor = Some(monitor.clone());
            inner.runtime.state = VmState::Running;
            inner.runtime.started_at = Some(Utc::now());
            inner.runtime.reset_health_tracking();
            inner.runtime.command_pid = child_pid;
            inner.runtime.virtiofsd_handles = virtiofsd_handles;
            inner.runtime.fuse_handles = fuse_handles;
            inner.runtime.tap_network = tap_network_handle.take();
            {
                let mut exit = inner
                    .runtime
                    .exit_status
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                *exit = None;
            }
            inner.metadata.state = VmState::Running;
            inner.metadata.started_at = inner.runtime.started_at;
            if let Some(upstream_addr) = &vm_proxy_upstream_addr {
                inner.metadata.metadata.insert(
                    META_NETWORK_POLICY_PROXY_UPSTREAM.to_string(),
                    upstream_addr.clone(),
                );
            } else {
                inner
                    .metadata
                    .metadata
                    .remove(META_NETWORK_POLICY_PROXY_UPSTREAM);
            }
            if let Err(err) = save_metadata(&vm.dir, &mut inner.metadata) {
                warn!(vm_id = %id, error = %err, "failed to persist metadata after start");
            }
        }
        match child_pid {
            Some(pid) => {
                if let Err(err) = network::register_vm_process(id, pid).await {
                    if cfg.ha_mode {
                        let _ = child.start_kill();
                        let _ = child.wait().await;
                        if let Some(cgroup) = qemu_cgroup.take() {
                            cgroup.cleanup();
                        }
                        if let Some(handle) = tap_network_handle.take() {
                            handle.shutdown().await;
                        }
                        if vm_proxy_upstream_addr.is_some() {
                            let _ = network::unregister_vm_proxy_policy(id).await;
                        }
                        if let Err(cleanup_error) = cleanup_runtime_mounts(&vm).await {
                            warn!(
                                vm_id = %id,
                                error = %cleanup_error,
                                "failed cleaning runtime mounts after network registration failure"
                            );
                        }
                        mark_vm_state(&vm, VmState::Error).await;
                        return Err(ManagerError::Other(
                            err.context("failed to register vm process for network guardrails"),
                        ));
                    }
                    warn!(vm_id = %id, pid, error = %err, "failed to register vm process for network guardrails");
                }
            }
            None if cfg.ha_mode => {
                let _ = child.start_kill();
                let _ = child.wait().await;
                if let Some(cgroup) = qemu_cgroup.take() {
                    cgroup.cleanup();
                }
                if let Some(handle) = tap_network_handle.take() {
                    handle.shutdown().await;
                }
                if vm_proxy_upstream_addr.is_some() {
                    let _ = network::unregister_vm_proxy_policy(id).await;
                }
                if let Err(cleanup_error) = cleanup_runtime_mounts(&vm).await {
                    warn!(
                        vm_id = %id,
                        error = %cleanup_error,
                        "failed cleaning runtime mounts after missing qemu pid"
                    );
                }
                mark_vm_state(&vm, VmState::Error).await;
                return Err(ManagerError::Other(anyhow!(
                    "failed to register vm process for network guardrails: missing qemu pid"
                )));
            }
            None => {}
        }

        spawn_exit_task(vm.clone(), child, log_path, child_pid, qemu_cgroup.take());
        info!(vm_id = %id, "VM started");
        let meta = vm.lock().await.metadata.clone();
        Ok(meta)
    }

    #[instrument(skip(self), fields(vm_id = %id))]
    pub async fn stop_vm(&self, id: &str) -> ManagerResult<VmMetadata> {
        let vm = self.vm_by_id(id).await?;
        let (state, monitor) =
            {
                let inner = vm.lock().await;
                match inner.runtime.state {
                    VmState::Running | VmState::Paused => (
                        inner.runtime.state,
                        inner.runtime.monitor.clone().ok_or_else(|| {
                            ManagerError::Other(anyhow!("vm monitor unavailable"))
                        })?,
                    ),
                    _ => return Ok(inner.metadata.clone()),
                }
            };

        if matches!(state, VmState::Paused) {
            if let Err(err) = virt::cont(&monitor).await {
                warn!(
                    vm_id = %id,
                    error = %err,
                    "failed to resume paused vm for graceful stop; forcing stop"
                );
                return self.force_stop_vm(id).await;
            }
        }
        if let Err(err) = virt::system_powerdown(&monitor).await {
            warn!(
                vm_id = %id,
                error = %err,
                "failed to request graceful vm powerdown; forcing stop"
            );
            return self.force_stop_vm(id).await;
        }
        if let Err(err) = wait_for_exit(&vm, Duration::from_secs(10)).await {
            warn!(
                vm_id = %id,
                error = %err,
                "graceful vm stop timed out; forcing stop"
            );
            return self.force_stop_vm(id).await;
        }

        let meta = vm.lock().await.metadata.clone();
        Ok(meta)
    }

    #[instrument(skip(self), fields(vm_id = %id))]
    pub async fn restart_vm(&self, id: &str) -> ManagerResult<VmMetadata> {
        self.stop_vm(id).await?;
        self.start_vm(id).await
    }

    pub async fn pause_vm(&self, id: &str) -> ManagerResult<VmMetadata> {
        let vm = self.vm_by_id(id).await?;
        let monitor = {
            let inner = vm.lock().await;
            if !matches!(inner.runtime.state, VmState::Running) {
                return Ok(inner.metadata.clone());
            }
            inner
                .runtime
                .monitor
                .clone()
                .ok_or_else(|| ManagerError::Other(anyhow!("vm monitor unavailable")))?
        };

        virt::stop(&monitor).await.map_err(ManagerError::Other)?;

        {
            let mut inner = vm.lock().await;
            inner.runtime.state = VmState::Paused;
            inner.metadata.state = VmState::Paused;
            if let Err(err) = save_metadata(&vm.dir, &mut inner.metadata) {
                warn!(vm_id = %id, error = %err, "failed to persist metadata after pause");
            }
        }

        Ok(vm.lock().await.metadata.clone())
    }

    pub async fn resume_vm(&self, id: &str) -> ManagerResult<VmMetadata> {
        let vm = self.vm_by_id(id).await?;
        let monitor = {
            let inner = vm.lock().await;
            if !matches!(inner.runtime.state, VmState::Paused) {
                return Ok(inner.metadata.clone());
            }
            inner
                .runtime
                .monitor
                .clone()
                .ok_or_else(|| ManagerError::Other(anyhow!("vm monitor unavailable")))?
        };

        virt::cont(&monitor).await.map_err(ManagerError::Other)?;

        {
            let mut inner = vm.lock().await;
            inner.runtime.state = VmState::Running;
            inner.runtime.started_at = Some(Utc::now());
            inner.runtime.reset_health_tracking();
            inner.metadata.state = VmState::Running;
            inner.metadata.started_at = inner.runtime.started_at;
            if let Err(err) = save_metadata(&vm.dir, &mut inner.metadata) {
                warn!(vm_id = %id, error = %err, "failed to persist metadata after resume");
            }
        }

        Ok(vm.lock().await.metadata.clone())
    }

    pub async fn force_stop_vm(&self, id: &str) -> ManagerResult<VmMetadata> {
        let vm = self.vm_by_id(id).await?;
        let (runtime_state, monitor, pid, vm_dir, qmp_path, pid_path) = {
            let inner = vm.lock().await;
            let state = inner.runtime.state;
            let monitor = inner.runtime.monitor.clone();
            let pid = inner.runtime.command_pid.unwrap_or_default();
            (
                state,
                monitor,
                pid,
                vm.dir.clone(),
                inner.runtime.qmp_path.clone(),
                inner.runtime.pid_path.clone(),
            )
        };

        let mut stopped_with_monitor = false;
        let mut force_reclaimed_runtime = false;
        if matches!(runtime_state, VmState::Running | VmState::Paused) {
            if let Some(monitor) = monitor {
                let quit_result = match tokio::time::timeout(
                    Duration::from_secs(3),
                    virt::quit(&monitor),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(anyhow!("timed out sending qmp quit command")),
                };
                if quit_result.is_err() && pid > 0 {
                    kill_process(pid).map_err(ManagerError::Other)?;
                }
                match wait_for_exit(&vm, Duration::from_secs(3)).await {
                    Ok(()) => {
                        stopped_with_monitor = true;
                    }
                    Err(err) => {
                        warn!(
                            vm_id = %id,
                            pid,
                            error = %err,
                            "timeout waiting for qemu exit after force-stop; killing vm pid"
                        );
                        if pid > 0 {
                            if pid_exists(pid) {
                                kill_process(pid).map_err(ManagerError::Other)?;
                            }
                            let _ = wait_for_pid_exit(pid, Duration::from_secs(5)).await;
                            force_reclaimed_runtime = true;
                        }
                    }
                }
            }
        }

        if !stopped_with_monitor {
            let reclaimed = if force_reclaimed_runtime {
                true
            } else {
                reclaim_local_runtime_ownership(
                    id,
                    vm_dir.as_path(),
                    qmp_path.as_path(),
                    pid_path.as_path(),
                )
                .await
                .map_err(ManagerError::Other)?
            };
            if reclaimed {
                let _ = network::unregister_vm_proxy_policy(id).await;
                cleanup_runtime_mounts(&vm)
                    .await
                    .with_context(|| format!("clean runtime mounts while force-stopping VM {id}"))
                    .map_err(ManagerError::Other)?;
                // @dive: Attach/failover paths can invoke force-stop without local runtime bookkeeping; persist stopped state after reclaim.
                mark_vm_state(&vm, VmState::Stopped).await;
            }
        }

        let meta = vm.lock().await.metadata.clone();
        Ok(meta)
    }

    pub async fn mark_vm_error(&self, id: &str, reason: &str) -> ManagerResult<VmMetadata> {
        {
            let vm = self.vm_by_id(id).await?;
            let inner = vm.lock().await;
            if inner.runtime.state == VmState::Error && inner.metadata.state == VmState::Error {
                return Ok(inner.metadata.clone());
            }
        }

        if let Err(err) = self.force_stop_vm(id).await {
            warn!(
                vm_id = %id,
                error = %err,
                reason = %reason,
                "failed force-stopping vm before marking error"
            );
        }
        if let Err(err) = network::unregister_vm_proxy_policy(id).await {
            warn!(
                vm_id = %id,
                error = %err,
                reason = %reason,
                "failed unregistering vm proxy policy before marking error"
            );
        }
        let vm = self.vm_by_id(id).await?;
        if let Err(error) = cleanup_runtime_mounts(&vm).await {
            warn!(
                vm_id = %id,
                error = %error,
                reason = %reason,
                "failed cleaning runtime mounts while marking VM error"
            );
        }
        {
            let mut inner = vm.lock().await;
            inner.runtime.state = VmState::Error;
            inner.runtime.started_at = None;
            inner.runtime.reset_health_tracking();
            inner.metadata.state = VmState::Error;
            inner.metadata.started_at = None;
            {
                let mut exit = inner
                    .runtime
                    .exit_status
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                *exit = Some(anyhow!(reason.to_string()));
            }
            if let Err(err) = save_metadata(&vm.dir, &mut inner.metadata) {
                warn!(vm_id = %id, error = %err, "persist metadata after marking vm error failed");
            }
        }
        Ok(vm.lock().await.metadata.clone())
    }

    #[instrument(skip(self), fields(vm_id = %vm_id, snapshot_id = %snapshot_id))]
    pub async fn restore_snapshot(
        &self,
        vm_id: &str,
        snapshot_id: &str,
    ) -> ManagerResult<VmMetadata> {
        let vm = self.vm_by_id(vm_id).await?;
        let (state, snapshot, disk_path, vm_dir, arch) = {
            let inner = vm.lock().await;
            if !inner.metadata.pci_devices.is_empty() {
                return Err(ManagerError::Unsupported(
                    "VMs with assigned PCI devices cannot restore snapshots".to_string(),
                ));
            }
            let state = inner.runtime.state;
            let snapshot = inner
                .metadata
                .snapshots
                .iter()
                .find(|snap| snap.id == snapshot_id)
                .cloned()
                .ok_or(ManagerError::SnapshotNotFound)?;
            let disk_path = vm.disk_path();
            (
                state,
                snapshot,
                disk_path,
                vm.dir.clone(),
                inner.metadata.architecture.clone(),
            )
        };

        let current_runtime_fingerprint = current_guest_runtime_fingerprint(arch.as_str())?;
        if !snapshot_ram_format_is_supported(&snapshot) {
            return Err(ManagerError::Unsupported(format!(
                "snapshot {} uses unsupported RAM format `{}`; only `{}` is supported",
                snapshot.name,
                if snapshot.ram_format.trim().is_empty() {
                    "<missing>"
                } else {
                    snapshot.ram_format.as_str()
                },
                virt::RAM_SNAPSHOT_FORMAT_MAPPED,
            )));
        }
        if snapshot.guest_runtime_fingerprint.as_deref()
            != Some(current_runtime_fingerprint.as_str())
        {
            return Err(ManagerError::Other(anyhow!(
                "snapshot {} guest runtime fingerprint incompatible: snapshot={} current={}",
                snapshot.name,
                snapshot
                    .guest_runtime_fingerprint
                    .as_deref()
                    .unwrap_or("<missing>"),
                current_runtime_fingerprint
            )));
        }

        let ram_path = vm_dir.join("snapshots").join(&snapshot.ram_file_name);
        if !ram_path.exists() {
            return Err(ManagerError::Other(anyhow!(
                "snapshot {} missing ram file at {}",
                snapshot.name,
                ram_path.display()
            )));
        }
        // @dive: Single restore path for the background-snapshot world. qemu's `-incoming`
        //        flag can only be consumed at launch, so we always: stop the current qemu
        //        process (if any), revert the qcow2 disk state offline via `qemu-img
        //        snapshot -a`, set `boot_incoming_ram_path` on the metadata for the next
        //        launch, and restart via `start_vm_inner`. The launch consumes the
        //        one-shot path before spawning qemu so subsequent boots don't loop back.
        if matches!(state, VmState::Running | VmState::Paused) {
            self.force_stop_vm(vm_id).await?;
        }

        virt::revert_snapshot_offline(&self.cfg.qemu_img_bin, disk_path.as_path(), &snapshot.name)
            .await
            .map_err(ManagerError::Other)?;

        {
            let mut inner = vm.lock().await;
            inner.metadata.boot_incoming_ram_path = ram_path.to_string_lossy().into_owned();
            inner.metadata.state = VmState::Stopped;
            inner.runtime.state = VmState::Stopped;
            if let Err(err) = save_metadata(&vm.dir, &mut inner.metadata) {
                warn!(vm_id = %vm_id, error = %err, "persist metadata before incoming restart failed");
            }
        }

        let cfg = self.cfg.clone();
        let host_arch = self.host_arch.clone();
        let launch_vm_id = vm_id.to_string();

        // Relaunch qemu; `-incoming file:<path>` consumes the RAM file and brings the
        // guest back to the running state of the snapshot moment. `start_vm_inner`
        // consumes the one-shot incoming pointer before spawning, so this future can
        // remain cancellation-owned without pinning later starts to the RAM file.
        Self::start_vm_inner(cfg, host_arch, vm, launch_vm_id).await
    }

    async fn create_from_docker(
        &self,
        vm: &Arc<Vm>,
        mut guard: OwnedMutexGuard<VmInner>,
        reference: &str,
        arch: &str,
        platform: Option<&str>,
        progress: Option<CreateVmProgressCallback>,
    ) -> ManagerResult<OwnedMutexGuard<VmInner>> {
        let vm_id = guard.metadata.id.clone();
        debug!(
            vm_id = %vm_id,
            reference = %reference,
            arch = %arch,
            platform = ?platform,
            "preparing VM disk from docker source"
        );

        let base_dir = PathBuf::from(&self.cfg.data_dir).join(config::BASE_IMAGES_DIR_NAME);
        fs::create_dir_all(&base_dir)?;

        let base_name = image::base_image_file_name(reference, arch);
        let base_path = base_dir.join(&base_name);
        let abs_base_path = base_path
            .canonicalize()
            .unwrap_or_else(|_| base_path.clone());
        let _base_image_guard = self.lock_base_image(&abs_base_path).await;

        emit_stage_progress(
            &progress,
            CreateVmStage::DownloadImage,
            0,
            format!("preparing base image for {reference}"),
        );

        let mut need_convert = true;
        if let Ok(metadata) = fs::metadata(&abs_base_path) {
            if metadata.len() > 0 {
                need_convert = false;
                debug!(
                    vm_id = %vm_id,
                    base = %abs_base_path.display(),
                    "using cached base image"
                );
                emit_stage_progress(
                    &progress,
                    CreateVmStage::DownloadImage,
                    100,
                    format!("using cached base image {}", abs_base_path.display()),
                );
            } else {
                let _ = fs::remove_file(&abs_base_path);
            }
        }

        if need_convert {
            if self.cfg.force_local_build {
                emit_stage_progress(
                    &progress,
                    CreateVmStage::DownloadImage,
                    100,
                    format!("skipping prebuilt image download for {reference}"),
                );
            } else {
                debug!(
                    vm_id = %vm_id,
                    reference = %reference,
                    arch = %arch,
                    "no cached base image, attempting to download or build"
                );
                emit_stage_progress(
                    &progress,
                    CreateVmStage::DownloadImage,
                    0,
                    format!("downloading {reference}"),
                );
                let dl_progress = progress.clone();
                let download_result = image::download_prebuilt_image_with_progress(
                    reference,
                    arch,
                    abs_base_path.as_path(),
                    move |update| {
                        emit_download_progress(
                            &dl_progress,
                            update.downloaded_bytes,
                            update.total_bytes,
                        );
                    },
                )
                .await;
                match download_result {
                    Ok(PrebuiltImageStatus::Downloaded { .. }) => {
                        need_convert = false;
                        debug!(
                            vm_id = %vm_id,
                            base = %abs_base_path.display(),
                            "downloaded prebuilt VM disk"
                        );
                        emit_stage_progress(
                            &progress,
                            CreateVmStage::DownloadImage,
                            100,
                            format!("downloaded VM disk to {}", abs_base_path.display()),
                        );
                    }
                    Ok(PrebuiltImageStatus::NotFound) => {
                        emit_stage_progress(
                            &progress,
                            CreateVmStage::DownloadImage,
                            100,
                            format!("no prebuilt VM disk available for {reference}"),
                        );
                    }
                    Err(err) => {
                        let err_msg = err.to_string();
                        warn!(
                            vm_id = %vm_id,
                            reference = %reference,
                            error = %err_msg,
                            "failed to download prebuilt VM image"
                        );
                        emit_stage_progress(
                            &progress,
                            CreateVmStage::DownloadImage,
                            100,
                            format!("failed to download prebuilt image: {err_msg}"),
                        );
                        return Err(ManagerError::Other(err));
                    }
                }
            }
        }

        if !need_convert {
            emit_stage_progress(
                &progress,
                CreateVmStage::ConvertImage,
                100,
                "base image ready".to_string(),
            );
        }

        if need_convert {
            emit_stage_progress(
                &progress,
                CreateVmStage::ConvertImage,
                0,
                format!("converting docker image {}", reference),
            );
            let tmp_name = format!(
                "{}-{}.{}",
                base_name.trim_end_matches(BASE_IMAGE_EXT),
                Uuid::new_v4(),
                BASE_IMAGE_EXT.trim_start_matches('.')
            );
            let tmp_path = base_dir.join(&tmp_name);
            let options = virt::D2VmOptions {
                image: reference.to_string(),
                output: tmp_name.clone(),
                disk_gb: BASE_IMAGE_SIZE_GB,
                pull: true,
                platform: platform.map(ToString::to_string),
                include_bootstrap: true,
            };
            debug!(
                vm_id = %vm_id,
                reference = %reference,
                output = %tmp_path.display(),
                arch = %arch,
                platform = ?options.platform,
                "running d2vm to convert VM disk"
            );
            virt::run_d2vm(&self.cfg.docker_bin, base_dir.as_path(), options)
                .await
                .map_err(ManagerError::Other)?;
            fs::rename(&tmp_path, &abs_base_path)?;
            debug!(
                vm_id = %vm_id,
                base = %abs_base_path.display(),
                "stored converted VM disk"
            );
            emit_stage_progress(
                &progress,
                CreateVmStage::ConvertImage,
                100,
                "finished converting VM disk".to_string(),
            );
        }

        if let Err(err) = fs::remove_file(vm.disk_path()) {
            if err.kind() != std::io::ErrorKind::NotFound {
                return Err(ManagerError::Other(err.into()));
            }
        }

        let size_gb = guard.metadata.resources.disk_gb.max(BASE_IMAGE_SIZE_GB);
        let overlay_path = vm.disk_path();
        debug!(
            vm_id = %vm_id,
            base = %abs_base_path.display(),
            overlay = %overlay_path.display(),
            size_gb,
            "creating overlay disk"
        );
        virt::create_overlay(
            &self.cfg.qemu_img_bin,
            abs_base_path.as_path(),
            overlay_path.as_path(),
            size_gb,
        )
        .await
        .map_err(ManagerError::Other)?;

        let bootstrap_path = vm.dir.join("bootstrap.iso");
        debug!(vm_id = %vm_id, path = %bootstrap_path.display(), "writing bootstrap seed image");
        ensure_mount_profiles_supported(
            &guard.metadata.shared_mounts,
            &self.cfg.shared_mount_profiles,
        )?;
        ensure_portproxy_auth_token(&mut guard.metadata.metadata);
        let cfg = bootstrap::Config {
            instance_id: guard.metadata.id.clone(),
            hostname: guard.metadata.name.clone(),
            arch: arch.to_string(),
            shared_mounts: guard
                .metadata
                .shared_mounts
                .iter()
                .cloned()
                .map(map_bootstrap_shared_mount)
                .collect(),
            network: Some(map_bootstrap_network(&guard.metadata)),
            http_proxy_url: None,
            portproxy_auth_token: guard
                .metadata
                .metadata
                .get(META_PORTPROXY_AUTH_TOKEN)
                .cloned(),
            durable_volume: guard.metadata.durable_volume.is_some(),
        };
        bootstrap::create_iso(&bootstrap_path, cfg).map_err(ManagerError::Other)?;

        guard.metadata.architecture = arch.to_string();
        save_metadata(&vm.dir, &mut guard.metadata).map_err(ManagerError::Other)?;

        Ok(guard)
    }

    async fn create_from_snapshot(
        &self,
        vm: &Arc<Vm>,
        mut guard: OwnedMutexGuard<VmInner>,
        source_vm: &Arc<Vm>,
        record: &SnapshotRecord,
    ) -> ManagerResult<OwnedMutexGuard<VmInner>> {
        let vm_id = guard.metadata.id.clone();
        debug!(
            vm_id = %vm_id,
            source_vm_id = %record.vm_id,
            snapshot_id = %record.snapshot.id,
            "restoring VM disk from snapshot"
        );
        let (snapshot, src_disk, dst_disk, src_ram, dst_ram) = {
            let source = source_vm.lock().await;
            let src_snap = source
                .metadata
                .snapshots
                .iter()
                .find(|snap| snap.id == record.snapshot.id)
                .cloned()
                .ok_or(ManagerError::SnapshotNotFound)?;
            let src_disk = source_vm.disk_path();
            let dst_disk = vm.disk_path();
            let src_ram = source_vm
                .dir
                .join("snapshots")
                .join(&src_snap.ram_file_name);
            let dst_ram = vm.dir.join("snapshots").join(&src_snap.ram_file_name);
            (src_snap, src_disk, dst_disk, src_ram, dst_ram)
        };

        if !src_ram.exists() {
            return Err(ManagerError::Other(anyhow!(
                "snapshot {} missing ram file at {}",
                snapshot.name,
                src_ram.display()
            )));
        }

        virt::copy_file(&src_disk, &dst_disk)
            .await
            .map_err(ManagerError::Other)?;
        debug!(
            vm_id = %vm_id,
            path = %dst_disk.display(),
            "copied base disk from snapshot"
        );

        // Carry the external RAM file across so the new VM can launch via -incoming.
        tokio::fs::create_dir_all(vm.dir.join("snapshots"))
            .await
            .with_context(|| format!("ensure snapshots dir for {}", vm_id))
            .map_err(ManagerError::Other)?;
        virt::copy_file(&src_ram, &dst_ram)
            .await
            .map_err(ManagerError::Other)?;
        guard.metadata.boot_incoming_ram_path = dst_ram.to_string_lossy().into_owned();
        // The snapshot record was cloned into the new VM by the caller; no-op here.
        let _ = snapshot;
        save_metadata(&vm.dir, &mut guard.metadata).map_err(ManagerError::Other)?;

        Ok(guard)
    }

    fn qemu_binary_for_arch_from(
        cfg: &Config,
        host_arch: &str,
        arch: &str,
    ) -> ManagerResult<String> {
        let normalized = if arch.trim().is_empty() {
            host_arch.to_string()
        } else {
            normalize_arch(arch)?
        };
        match normalized.as_str() {
            ARCH_AMD64 => Ok(cfg.qemu_bin.clone()),
            ARCH_ARM64 => Ok(cfg.qemu_arm64_bin.clone()),
            other => Err(ManagerError::Other(anyhow!(
                "unsupported architecture: {other}"
            ))),
        }
    }

    async fn resolve_docker_platform(
        &self,
        reference: &str,
        requested: Option<String>,
    ) -> ManagerResult<(String, String)> {
        // @dive: Session clients already send a normalized architecture in the common path.
        // Trusting that explicit contract keeps VM creation off the network-bound manifest probe path.
        if let Some(req_arch) = requested {
            let req_arch = normalize_arch(&req_arch)?;
            return Ok((req_arch.clone(), format!("linux/{req_arch}")));
        }

        let platforms = virt::inspect_image_platforms(&self.cfg.docker_bin, reference)
            .await
            .map_err(ManagerError::Other)?;
        let arch = choose_preferred_architecture(&platforms, &self.host_arch).ok_or_else(|| {
            anyhow!("image {reference} does not provide a supported architecture")
        })?;
        Ok((arch.clone(), format!("linux/{arch}")))
    }

    async fn vm_by_id(&self, id: &str) -> ManagerResult<Arc<Vm>> {
        let guard = self.vms.read().await;
        guard.get(id).cloned().ok_or(ManagerError::VmNotFound)
    }

    fn fork_base_root(&self) -> PathBuf {
        PathBuf::from(&self.cfg.data_dir).join(FORK_BASES_DIR_NAME)
    }

    fn rollback_stopped_fork_artifacts(
        parent_disk: &Path,
        fork_base: &Path,
        fork_root: &Path,
        child_dir: &Path,
    ) {
        let _ = fs::remove_dir_all(child_dir);
        let _ = fs::remove_file(parent_disk);
        if fork_base.exists() {
            let _ = fs::rename(fork_base, parent_disk);
        }
        let _ = fs::remove_dir(fork_root);
    }

    async fn cleanup_fork_base_if_unreferenced(&self, fork_base_path: Option<String>) {
        let Some(path) = fork_base_path else {
            return;
        };

        let vm_refs: Vec<_> = {
            let guard = self.vms.read().await;
            guard.values().cloned().collect()
        };
        for vm in vm_refs {
            let inner = vm.lock().await;
            if inner
                .metadata
                .metadata
                .get(META_FORK_BASE_PATH)
                .map(|entry| entry == &path)
                .unwrap_or(false)
            {
                return;
            }
        }

        let base_path = PathBuf::from(path);
        let _ = fs::remove_file(&base_path);
        if let Some(parent) = base_path.parent() {
            let _ = fs::remove_dir(parent);
        }
    }

    fn maintenance_root(&self) -> PathBuf {
        PathBuf::from(&self.cfg.data_dir).join(MAINTENANCE_DIR_NAME)
    }

    fn fork_compaction_queue_root(&self) -> PathBuf {
        self.maintenance_root().join(FORK_COMPACTION_QUEUE_DIR_NAME)
    }

    async fn enqueue_fork_compaction_task(
        &self,
        fork_id: &str,
        parent_id: &str,
        child_id: &str,
        depth: usize,
    ) {
        // @dive: Fork compaction is queued as asynchronous maintenance intent so request-path fork latency remains metadata/overlay-only.
        let queue_root = self.fork_compaction_queue_root();
        if let Err(err) = fs::create_dir_all(&queue_root) {
            warn!(
                queue_root = %queue_root.display(),
                error = %err,
                "failed creating fork compaction queue root"
            );
            return;
        }
        let queue_id = format!("{}-{}-{}", unix_millis(), depth, fork_id);
        let queue_path = queue_root.join(format!("{queue_id}.json"));
        let payload = json!({
            "queue_id": queue_id,
            "fork_id": fork_id,
            "parent_vm_id": parent_id,
            "child_vm_id": child_id,
            "fork_depth": depth,
            "queued_at_unix_ms": unix_millis(),
        });
        if let Err(err) = fs::write(&queue_path, payload.to_string()) {
            warn!(
                queue_path = %queue_path.display(),
                error = %err,
                "failed writing fork compaction queue entry"
            );
        }
    }

    async fn garbage_collect_orphaned_fork_roots(&self) {
        // @dive: GC only removes fork-base roots with zero metadata references, preserving live fork isolation guarantees.
        let fork_root = self.fork_base_root();
        if !fork_root.exists() {
            return;
        }

        let vm_refs: Vec<_> = {
            let guard = self.vms.read().await;
            guard.values().cloned().collect()
        };
        let mut referenced_roots = HashSet::<PathBuf>::new();
        for vm in vm_refs {
            let inner = vm.lock().await;
            if let Some(path) = inner.metadata.metadata.get(META_FORK_BASE_PATH) {
                let base_path = PathBuf::from(path);
                if let Some(parent) = base_path.parent() {
                    referenced_roots.insert(parent.to_path_buf());
                }
            }
        }

        let Ok(entries) = fs::read_dir(&fork_root) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            if referenced_roots.contains(&path) {
                continue;
            }
            if let Err(err) = fs::remove_dir_all(&path) {
                warn!(
                    path = %path.display(),
                    error = %err,
                    "failed removing orphaned fork-base root"
                );
            }
        }
    }

    async fn snapshot_by_id(&self, id: &str) -> ManagerResult<SnapshotRecord> {
        let guard = self.snapshots.read().await;
        guard.get(id).cloned().ok_or(ManagerError::SnapshotNotFound)
    }
}

fn build_qemu_args(
    meta: &VmMetadata,
    vm_dir: &Path,
    qmp_path: &Path,
    pid_path: &Path,
    host_arch: &str,
    tap_network: Option<&network::tap::VmTapNetworkSpec>,
    virtiofsd_handles: &[virt::VirtiofsdHandle],
) -> Result<Vec<String>> {
    let guest_arch = if meta.architecture.trim().is_empty() {
        ARCH_AMD64
    } else {
        meta.architecture.as_str()
    };

    let running_on_linux = cfg!(target_os = "linux");
    let running_on_macos = cfg!(target_os = "macos");

    // @dive: virtio-fs (and qemu migration generally) requires shared-memory guest RAM
    //        backing so the host-side daemons can mmap guest pages. `memory-backend-memfd`
    //        creates an anonymous memfd-backed mapping marked `share=on`, which also
    //        satisfies userfaultfd-WP prerequisites for `background-snapshot` migration.
    //        The machine line references the backend via `memory-backend=mem`.
    //
    //        memfd_create is Linux-only, so we only emit the memfd backend when we're
    //        actually running virtio-fs (i.e. virtiofsd_handles non-empty). On macOS /
    //        other hosts we keep standard anonymous-mmap RAM backing, which in turn
    //        means we take the legacy -virtfs path for shared mounts below.
    let use_virtiofs = !virtiofsd_handles.is_empty();
    let memory_backend_suffix = if use_virtiofs {
        ",memory-backend=mem"
    } else {
        ""
    };

    let (machine, cpu, bios) = match guest_arch {
        ARCH_AMD64 => {
            let (machine_base, cpu) = if host_arch == ARCH_AMD64 && running_on_linux {
                ("q35,accel=kvm:tcg", "host")
            } else if host_arch == ARCH_AMD64 && running_on_macos {
                ("q35,accel=hvf:tcg", "host")
            } else {
                ("q35,accel=tcg", "qemu64")
            };
            (
                format!("{machine_base}{memory_backend_suffix}"),
                cpu.to_string(),
                None,
            )
        }
        ARCH_ARM64 => {
            let machine_base = if host_arch == ARCH_ARM64 && running_on_linux {
                "virt,accel=kvm:tcg"
            } else if host_arch == ARCH_ARM64 && running_on_macos {
                "virt,accel=hvf:tcg"
            } else {
                "virt,accel=tcg"
            };
            let cpu = if host_arch == ARCH_ARM64 && (running_on_linux || running_on_macos) {
                "host".to_string()
            } else {
                "cortex-a72".to_string()
            };
            (
                format!("{machine_base}{memory_backend_suffix}"),
                cpu,
                Some(resolve_arm64_bios_path()?),
            )
        }
        other => bail!("unsupported architecture: {other}"),
    };

    let disk_path = vm_dir.join("disk.qcow2");
    let netdev = match tap_network {
        Some(tap_network) => format!(
            "tap,id=net0,ifname={},script=no,downscript=no",
            tap_network.tap_name
        ),
        None => qemu_user_netdev(meta)?,
    };

    let mut args = vec!["-machine".to_string(), machine, "-cpu".to_string(), cpu];
    if guest_arch == ARCH_AMD64 {
        // @dive: The KVM VAPIC/TPR option ROM is a legacy optimization for old
        //        32-bit Windows guests. Our Linux guests do not need it, and its
        //        `kvm-tpr-opt` migration post-load hook can fail RAM restore with
        //        EPERM under the nested/Kubernetes KVM environment. Keep KVM on,
        //        but remove this migration-toxic APIC subfeature.
        args.push("-global".to_string());
        args.push("apic.vapic=off".to_string());
    }
    args.push("-smp".to_string());
    args.push(meta.resources.vcpu.to_string());
    args.push("-m".to_string());
    args.push(meta.resources.memory_mb.to_string());
    // Memfd-backed guest RAM. `share=on` is the critical flag: it marks the mapping
    // as MAP_SHARED so vhost-user-fs daemons can mmap the pages, and it's what enables
    // UFFD-WP-based `background-snapshot` migration to preserve page state correctly.
    // Only emitted when we're actually using virtio-fs (Linux production path).
    if use_virtiofs {
        args.push("-object".to_string());
        args.push(format!(
            "memory-backend-memfd,id=mem,size={}M,share=on",
            meta.resources.memory_mb
        ));
    }
    args.push("-drive".to_string());
    args.push(format!(
        "file={},if=virtio,index=0,cache=none,aio=threads,format=qcow2",
        disk_path.display()
    ));
    args.push("-netdev".to_string());
    args.push(netdev);
    args.push("-device".to_string());
    args.push(format!(
        "virtio-net-pci,netdev=net0,mac={}",
        meta.network.mac
    ));
    if tap_network.is_some() {
        let runtime_network = map_bootstrap_network(meta);
        let runtime_values = [
            ("tap-mac", runtime_network.mac_address.as_str()),
            ("tap-address-cidr", runtime_network.address_cidr.as_str()),
            ("tap-gateway", runtime_network.gateway.as_str()),
            ("tap-dns", runtime_network.dns.as_str()),
            ("hostname", meta.name.as_str()),
            (
                "portproxy-auth-token",
                meta.metadata
                    .get(META_PORTPROXY_AUTH_TOKEN)
                    .map(String::as_str)
                    .unwrap_or(""),
            ),
        ];
        for (name, value) in runtime_values {
            args.push("-fw_cfg".to_string());
            args.push(format!("name=opt/chevalier/{name},string={value}"));
            args.push("-smbios".to_string());
            args.push(format!("type=11,value=chevalier.{name}={value}"));
        }
    }
    for assignment in &meta.pci_devices {
        for (function_index, bdf) in assignment.bdfs.iter().enumerate() {
            args.push("-device".to_string());
            args.push(format!(
                "vfio-pci,host={bdf},id={}",
                pci::qemu_device_id(&assignment.id, function_index)
            ));
        }
    }
    args.push("-display".to_string());
    args.push("none".to_string());
    args.push("-serial".to_string());
    args.push("mon:stdio".to_string());
    args.push("-qmp".to_string());
    args.push(format!("unix:{},server=on,wait=off", qmp_path.display()));
    args.push("-pidfile".to_string());
    args.push(pid_path.display().to_string());

    // @dive: Shared mounts are served differently depending on whether we can use
    //        virtio-fs on the host. The Linux production path pre-spawns virtiofsd
    //        per mount (a vhost-user daemon) and consumes it via `-chardev` +
    //        `-device vhost-user-fs-pci`. virtio-fs is required in production because
    //        virtio-9p (`-virtfs`) blocks qemu migration entirely and we need
    //        migration-capable VMs for `background-snapshot`.
    //
    //        On macOS / any host where virtiofsd is unavailable, the caller passes an
    //        empty `virtiofsd_handles` slice and we fall back to `-virtfs local,...`.
    //        That legacy path doesn't support migration but lets developers run vmd
    //        locally without needing a Linux-only daemon.
    if use_virtiofs {
        for (index, handle) in virtiofsd_handles.iter().enumerate() {
            let chardev_id = format!("vfsd{index}");
            args.push("-chardev".to_string());
            args.push(format!(
                "socket,id={chardev_id},path={}",
                handle.socket_path.display()
            ));
            args.push("-device".to_string());
            args.push(format!(
                "vhost-user-fs-pci,queue-size=1024,chardev={chardev_id},tag={}",
                handle.tag
            ));
        }
    } else {
        for (index, mount) in meta.shared_mounts.iter().enumerate() {
            // Legacy virtio-9p fallback for non-Linux dev hosts.
            args.push("-virtfs".to_string());
            let readonly = if mount.read_only { ",readonly=on" } else { "" };
            args.push(format!(
                "local,id=share{index},path={},security_model=none,multidevs=remap,mount_tag={}{}",
                mount.host_path, mount.mount_tag, readonly
            ));
        }
    }

    if let Some(bios) = bios {
        args.push("-bios".to_string());
        args.push(bios);
    }

    // @dive: Defer incoming restore until after QMP is available. Fast mapped-ram
    //        restore requires migration capabilities/parameters to be set before
    //        qemu consumes the file, so `start_vm_inner` issues `migrate-incoming`
    //        after monitor setup.
    if !meta.boot_incoming_ram_path.is_empty() {
        args.push("-incoming".to_string());
        args.push("defer".to_string());
    }

    let bootstrap_iso = vm_dir.join("bootstrap.iso");
    if bootstrap_iso.exists() {
        args.push("-drive".to_string());
        args.push(format!(
            "file={},if=virtio,index=1,id=bootstrap,media=cdrom,readonly=on",
            bootstrap_iso.display()
        ));
    }
    // The d2vm base image's first-boot loader intentionally finds bootstrap.iso
    // at /dev/vdb. Keep that compatibility disk ahead of the durable attachment
    // and pin the latter to a high PCI slot; the durable disk has a stable serial
    // and is discovered by-id, so its /dev/vd* enumeration is deliberately irrelevant.
    if let Some(volume) = meta.durable_volume.as_ref() {
        let volume_path = vm_dir
            .parent()
            .ok_or_else(|| anyhow!("VM directory has no data-dir parent"))?
            .join(VOLUMES_DIR_NAME)
            .join(format!("{}.qcow2", volume.volume_id));
        args.push("-drive".to_string());
        args.push(format!(
            "file={},if=none,id=openbracket-durable,cache=none,aio=threads,format=qcow2",
            volume_path.display()
        ));
        args.push("-device".to_string());
        args.push(
            "virtio-blk-pci,drive=openbracket-durable,serial=openbracket-durable,bus=pcie.0,addr=0x1e"
                .to_string(),
        );
    }

    Ok(args)
}

fn qemu_user_netdev(meta: &VmMetadata) -> Result<String> {
    if meta.network.proxy_port <= 0 {
        bail!("qemu user networking requires a positive proxy port");
    }
    if meta.network.rpc_port <= 0 {
        bail!("qemu user networking requires a positive rpc port");
    }
    let bind_addr = port_forward_bind_address();
    Ok(format!(
        "user,id=net0,hostfwd=tcp:{bind_addr}:{}-:13337,hostfwd=tcp:{bind_addr}:{}-:13338",
        meta.network.proxy_port, meta.network.rpc_port
    ))
}

fn resolve_arm64_bios_path() -> Result<String> {
    for candidate in ARM64_BIOS_CANDIDATES {
        if Path::new(candidate).exists() {
            return Ok(candidate.to_string());
        }
    }

    bail!(
        "no arm64 UEFI firmware found; tried {}",
        ARM64_BIOS_CANDIDATES.join(", ")
    )
}

fn map_bootstrap_shared_mount(mount: SharedMountSpec) -> bootstrap::SharedMount {
    bootstrap::SharedMount {
        guest_path: mount.guest_path,
        mount_tag: mount.mount_tag,
        read_only: mount.read_only,
    }
}

fn map_bootstrap_network(meta: &VmMetadata) -> bootstrap::NetworkConfig {
    let addressing = network::tap::addressing_for_vm(&meta.id);
    bootstrap::NetworkConfig {
        mac_address: meta.network.mac.clone(),
        address_cidr: format!("{}/{}", addressing.guest_ip, addressing.prefix_len),
        gateway: addressing.gateway_ip.to_string(),
        dns: addressing.gateway_ip.to_string(),
    }
}

fn current_guest_runtime_fingerprint(arch: &str) -> ManagerResult<String> {
    let guest_runtime = portproxy::guest_runtime_fingerprint(arch).map_err(ManagerError::Other)?;
    if arch == ARCH_AMD64 {
        Ok(format!("{guest_runtime}:{AMD64_QEMU_RUNTIME_FINGERPRINT}"))
    } else {
        Ok(guest_runtime)
    }
}

fn snapshot_guest_runtime_matches_current(
    snapshot: &SnapshotMetadata,
    arch: &str,
) -> ManagerResult<bool> {
    let current = current_guest_runtime_fingerprint(arch)?;
    Ok(snapshot.guest_runtime_fingerprint.as_deref() == Some(current.as_str()))
}

fn snapshot_ram_format_is_supported(snapshot: &SnapshotMetadata) -> bool {
    snapshot.ram_format == virt::RAM_SNAPSHOT_FORMAT_MAPPED
}

fn boot_incoming_snapshot_matches_current(meta: &VmMetadata) -> ManagerResult<bool> {
    if meta.boot_incoming_ram_path.is_empty() {
        return Ok(true);
    }

    let Some(incoming_file_name) = Path::new(&meta.boot_incoming_ram_path)
        .file_name()
        .and_then(|value| value.to_str())
    else {
        return Ok(false);
    };

    let Some(snapshot) = meta
        .snapshots
        .iter()
        .find(|snapshot| snapshot.ram_file_name == incoming_file_name)
    else {
        return Ok(false);
    };

    Ok(snapshot_ram_format_is_supported(snapshot)
        && snapshot_guest_runtime_matches_current(snapshot, meta.architecture.as_str())?)
}

fn boot_incoming_ram_format(meta: &VmMetadata) -> &str {
    let Some(incoming_file_name) = Path::new(&meta.boot_incoming_ram_path)
        .file_name()
        .and_then(|value| value.to_str())
    else {
        return "";
    };

    meta.snapshots
        .iter()
        .find(|snapshot| snapshot.ram_file_name == incoming_file_name)
        .map(|snapshot| snapshot.ram_format.as_str())
        .unwrap_or("")
}

fn vm_proxy_policy_from_metadata(meta: &VmMetadata) -> Result<network::VmProxyPolicyConfig> {
    match meta.metadata.get(META_NETWORK_POLICY) {
        Some(policy_json) if !policy_json.trim().is_empty() => {
            serde_json::from_str::<network::VmProxyPolicyConfig>(policy_json)
                .with_context(|| format!("parse vm network policy for {}", meta.id))
        }
        _ => Ok(network::VmProxyPolicyConfig::default()),
    }
}

fn has_explicit_network_policy(meta: &VmMetadata) -> bool {
    meta.metadata
        .get(META_NETWORK_POLICY)
        .is_some_and(|policy| !policy.trim().is_empty())
}

fn requires_managed_tap_network(cfg: &Config, meta: &VmMetadata) -> bool {
    cfg.ha_mode
        || cfg.guest_network.http_proxy_upstream_addr.is_some()
        || has_explicit_network_policy(meta)
}

fn normalize_shared_mounts(
    shared_mounts: Vec<SharedMountSpec>,
) -> ManagerResult<Vec<SharedMountSpec>> {
    let mut normalized = Vec::with_capacity(shared_mounts.len());
    let mut guest_paths = HashSet::new();
    let mut mount_tags = HashSet::new();

    for (index, mount) in shared_mounts.into_iter().enumerate() {
        let host_path = mount.host_path.trim();
        let guest_path = mount.guest_path.trim();
        let vfs_endpoint = mount.vfs_endpoint.trim();
        let vfs_scope_path = mount.vfs_scope_path.trim();
        let is_fuse_backed = !vfs_endpoint.is_empty();
        if host_path.is_empty() && !is_fuse_backed {
            return Err(ManagerError::Other(anyhow!(
                "shared mount host_path is required"
            )));
        }
        if is_fuse_backed && mount.host_path.contains('\0') {
            return Err(ManagerError::Other(anyhow!(
                "shared mount host_path must not contain NUL bytes"
            )));
        }
        if guest_path.is_empty() {
            return Err(ManagerError::Other(anyhow!(
                "shared mount guest_path is required"
            )));
        }
        if !guest_path.starts_with('/') {
            return Err(ManagerError::Other(anyhow!(
                "shared mount guest_path must be absolute: {}",
                guest_path
            )));
        }

        let normalized_host = if is_fuse_backed {
            String::new()
        } else {
            fs::canonicalize(host_path)
                .with_context(|| format!("canonicalize shared mount host path {}", host_path))
                .map_err(ManagerError::Other)?
                .to_string_lossy()
                .to_string()
        };
        let guest_path = guest_path.to_string();
        if !guest_paths.insert(guest_path.clone()) {
            return Err(ManagerError::Other(anyhow!(
                "duplicate shared mount guest_path {}",
                guest_path
            )));
        }

        let mount_tag = normalize_mount_tag(mount.mount_tag.as_str(), index);
        if !mount_tags.insert(mount_tag.clone()) {
            return Err(ManagerError::Other(anyhow!(
                "duplicate shared mount mount_tag {}",
                mount_tag
            )));
        }

        let availability = normalize_mount_availability(mount.availability);
        let continuity = normalize_mount_continuity(mount.continuity, &availability)?;
        let backend_profile = normalize_mount_backend_profile(mount.backend_profile.as_str());
        if matches!(availability, SharedMountAvailability::SharedStorage)
            && backend_profile.is_empty()
        {
            return Err(ManagerError::Other(anyhow!(
                "shared-storage mount {} requires a non-empty backend_profile",
                mount_tag
            )));
        }

        normalized.push(SharedMountSpec {
            host_path: normalized_host,
            guest_path,
            mount_tag,
            read_only: mount.read_only,
            availability,
            continuity,
            backend_profile,
            vfs_endpoint: vfs_endpoint.to_string(),
            vfs_scope_path: vfs_scope_path.to_string(),
        });
    }

    Ok(normalized)
}

fn normalize_mount_availability(raw: SharedMountAvailability) -> SharedMountAvailability {
    raw
}

fn normalize_mount_continuity(
    raw: SharedMountContinuity,
    availability: &SharedMountAvailability,
) -> ManagerResult<SharedMountContinuity> {
    // @dive: Shared-storage mounts may opt into same-node restarts or cross-node restore, but
    // node-local mounts must never claim cross-node continuity because VMD cannot satisfy that
    // contract after failover.
    match (raw, availability) {
        (SharedMountContinuity::RestoreCrossNode, SharedMountAvailability::NodeLocal) => {
            Err(ManagerError::Other(anyhow!(
                "node-local shared mounts cannot declare cross-node restore continuity"
            )))
        }
        (SharedMountContinuity::RestoreCrossNode, SharedMountAvailability::SharedStorage) => {
            Ok(SharedMountContinuity::RestoreCrossNode)
        }
        (SharedMountContinuity::RestartSameNode, SharedMountAvailability::SharedStorage) => {
            Ok(SharedMountContinuity::RestartSameNode)
        }
        (SharedMountContinuity::RestartSameNode, SharedMountAvailability::NodeLocal) => {
            Ok(SharedMountContinuity::RestartSameNode)
        }
    }
}

fn normalize_mount_tag(raw: &str, index: usize) -> String {
    let trimmed = raw.trim();
    let candidate = if trimmed.is_empty() {
        format!("share{index}")
    } else {
        trimmed.to_string()
    };
    candidate
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '-'
            }
        })
        .collect()
}

fn normalize_mount_backend_profile(raw: &str) -> String {
    raw.trim().to_ascii_lowercase()
}

fn ensure_mount_profiles_supported(
    shared_mounts: &[SharedMountSpec],
    supported_profiles: &[String],
) -> ManagerResult<()> {
    let supported = supported_profiles
        .iter()
        .map(|value| normalize_mount_backend_profile(value))
        .filter(|value| !value.is_empty())
        .collect::<HashSet<_>>();

    for mount in shared_mounts {
        if !matches!(mount.availability, SharedMountAvailability::SharedStorage) {
            continue;
        }
        if supported.contains(&mount.backend_profile) {
            continue;
        }
        return Err(ManagerError::Other(anyhow!(
            "shared mount `{}` requires backend_profile `{}` but this node only supports {:?}",
            mount.mount_tag,
            mount.backend_profile,
            supported_profiles
        )));
    }

    Ok(())
}

async fn mark_vm_state(vm: &Arc<Vm>, state: VmState) {
    let mut inner = vm.lock().await;
    inner.runtime.state = state;
    if state != VmState::Running {
        inner.runtime.started_at = None;
        inner.runtime.reset_health_tracking();
    }
    inner.metadata.state = state;
    inner.metadata.started_at = inner.runtime.started_at;
    if let Err(err) = save_metadata(&vm.dir, &mut inner.metadata) {
        warn!(vm_id = %inner.metadata.id, error = %err, "persist metadata failed");
    }
}

fn ensure_vm_fuse_mounts_detached(vm_id: &str, vm_dir: &Path) -> Result<()> {
    let fuse_root = vm_dir.join("fuse-mounts");
    let active = fuse::active_mountpoints_under(&fuse_root)?;
    if active.is_empty() {
        return Ok(());
    }
    bail!(
        "refusing to remove VM {vm_id} directory while mountpoints remain active beneath {}: {}",
        fuse_root.display(),
        active
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn remove_vm_dir_if_detached(vm_id: &str, vm_dir: &Path) -> Result<()> {
    remove_vm_dir_with_mount_probe(vm_id, vm_dir, |fuse_root| {
        fuse::active_mountpoints_under(fuse_root)
    })
}

fn remove_vm_dir_with_mount_probe<F>(vm_id: &str, vm_dir: &Path, mount_probe: F) -> Result<()>
where
    F: FnOnce(&Path) -> Result<Vec<PathBuf>>,
{
    if !vm_dir.exists() {
        return Ok(());
    }
    let fuse_root = vm_dir.join("fuse-mounts");
    let active = mount_probe(&fuse_root)?;
    if !active.is_empty() {
        bail!(
            "refusing to remove VM {vm_id} directory while mountpoints remain active beneath {}: {}",
            fuse_root.display(),
            active
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    fs::remove_dir_all(vm_dir)
        .with_context(|| format!("remove detached VM {vm_id} directory {}", vm_dir.display()))
}

async fn cleanup_unpublished_fuse_handles(vm_id: &str, handles: &[fuse::FuseHandle]) {
    for handle in handles {
        if let Err(error) = fuse::unmount_fuse(handle).await {
            warn!(
                vm_id = %vm_id,
                mountpoint = %handle.mountpoint().display(),
                error = %error,
                "initial FUSE cleanup failed before runtime publication; retrying by pathname"
            );
            if let Err(retry_error) = fuse::unmount_fuse(handle).await {
                warn!(
                    vm_id = %vm_id,
                    mountpoint = %handle.mountpoint().display(),
                    error = %retry_error,
                    "path-based FUSE cleanup retry failed; VM-directory deletion remains fenced"
                );
            }
        }
    }
}

async fn cleanup_runtime_mounts(vm: &Arc<Vm>) -> Result<()> {
    let (virtiofsd_handles, fuse_handles, tap_network) = {
        let mut inner = vm.lock().await;
        (
            std::mem::take(&mut inner.runtime.virtiofsd_handles),
            std::mem::take(&mut inner.runtime.fuse_handles),
            inner.runtime.tap_network.take(),
        )
    };
    if let Some(handle) = tap_network {
        handle.shutdown().await;
    }
    for handle in &virtiofsd_handles {
        virt::terminate_virtiofsd(handle);
    }
    let mut failed_handles = Vec::new();
    let mut failures = Vec::new();
    for handle in fuse_handles {
        if let Err(error) = fuse::unmount_fuse(&handle).await {
            failures.push(format!("{}: {error:#}", handle.mountpoint().display()));
            failed_handles.push(handle);
        }
    }
    if !failed_handles.is_empty() {
        vm.lock().await.runtime.fuse_handles.extend(failed_handles);
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!(
            "failed unmounting VM FUSE filesystems: {}",
            failures.join("; ")
        )
    }
}

fn spawn_launch_child(
    command: &mut Command,
    context_label: &str,
    cancellation_owned: bool,
) -> Result<Child> {
    command.kill_on_drop(cancellation_owned);
    command
        .spawn()
        .with_context(|| format!("spawn {context_label}"))
}

async fn abort_launch(
    vm: Arc<Vm>,
    mut child: Child,
    log_path: &Path,
    err: anyhow::Error,
) -> ManagerError {
    let _ = child.kill().await;
    let _ = child.wait().await;
    mark_vm_state(&vm, VmState::Error).await;
    let tail = read_log_tail(log_path, 8192);
    let message = if let Some(tail) = tail {
        format!("{err}: {tail}")
    } else {
        err.to_string()
    };
    ManagerError::Other(anyhow!(message))
}

async fn wait_for_exit(vm: &Arc<Vm>, timeout: Duration) -> ManagerResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        {
            let inner = vm.lock().await;
            match inner.runtime.state {
                VmState::Running | VmState::Paused => {}
                _ => return Ok(()),
            }
        }

        if Instant::now() >= deadline {
            return Err(ManagerError::Other(anyhow!(
                "timeout waiting for VM to stop"
            )));
        }
        sleep(Duration::from_millis(200)).await;
    }
}

fn allocate_host_port(preferred: i32, reserved: &mut HashSet<i32>) -> Result<i32> {
    let bind_address = port_forward_bind_address();
    if preferred > 0 && !reserved.contains(&preferred) {
        if port_available(preferred, bind_address.as_str()) {
            reserved.insert(preferred);
            return Ok(preferred);
        }
    }

    loop {
        let listener = TcpListener::bind(format!("{bind_address}:0"))?;
        let port = listener.local_addr()?.port() as i32;
        drop(listener);
        if reserved.contains(&port) {
            continue;
        }
        reserved.insert(port);
        return Ok(port);
    }
}

fn port_available(port: i32, bind_address: &str) -> bool {
    if port <= 0 {
        return false;
    }
    match TcpListener::bind(format!("{bind_address}:{port}")) {
        Ok(listener) => {
            drop(listener);
            true
        }
        Err(_) => false,
    }
}

fn port_forward_bind_address() -> String {
    env::var("CHEVALIER_SANDBOX_PORT_FORWARD_BIND_ADDRESS")
        .or_else(|_| env::var("BRACKET_SANDBOX_PORT_FORWARD_BIND_ADDRESS"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "127.0.0.1".to_string())
}

fn read_log_tail(path: &Path, limit: usize) -> Option<String> {
    let mut file = OpenOptions::new().read(true).open(path).ok()?;
    let len = file.seek(SeekFrom::End(0)).ok()?;
    let start = if len as usize > limit {
        len - limit as u64
    } else {
        0
    };
    file.seek(SeekFrom::Start(start)).ok()?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).ok()?;
    Some(buf.trim().to_string())
}

fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn spawn_exit_task(
    vm: Arc<Vm>,
    mut child: Child,
    log_path: PathBuf,
    expected_pid: Option<u32>,
    qemu_cgroup: Option<network::ProcessCgroup>,
) {
    tokio::spawn(async move {
        let wait_result = child.wait().await;
        let tail = read_log_tail(&log_path, 8192);

        let (mut new_state, mut log_message, mut exit_error) = match wait_result {
            Ok(status) if status.success() => (VmState::Stopped, None, None),
            Ok(status) => {
                let err = anyhow!("qemu exited with status {status}");
                let msg = err.to_string();
                (VmState::Error, Some(msg), Some(err))
            }
            Err(err) => {
                let err = anyhow!(err);
                let msg = err.to_string();
                (VmState::Error, Some(msg), Some(err))
            }
        };

        let mut inner = vm.lock().await;
        let vm_id = inner.metadata.id.clone();
        let vm_id_for_proxy_cleanup = vm_id.clone();
        let current_pid = inner.runtime.command_pid;
        if current_pid != expected_pid {
            warn!(
                vm_id = %vm_id,
                expected_pid = ?expected_pid,
                current_pid = ?current_pid,
                "stale VM exit task observed; preserving newer runtime state"
            );
            if let Some(cgroup) = qemu_cgroup {
                cgroup.cleanup();
            }
            return;
        }
        inner.runtime.monitor = None;
        inner.runtime.command_pid = None;
        inner.runtime.started_at = None;
        inner.runtime.reset_health_tracking();

        // @dive: virtiofsd normally self-exits when qemu drops the vhost-user socket,
        //        but we still SIGTERM any straggler to make cleanup deterministic and
        //        to unlink the socket file.
        let handles = std::mem::take(&mut inner.runtime.virtiofsd_handles);
        for handle in &handles {
            virt::terminate_virtiofsd(handle);
        }
        let fuse_handles = std::mem::take(&mut inner.runtime.fuse_handles);
        let mut failed_fuse_handles = Vec::new();
        let mut unmount_failures = Vec::new();
        for handle in fuse_handles {
            if let Err(error) = fuse::unmount_fuse(&handle).await {
                unmount_failures.push(format!("{}: {error:#}", handle.mountpoint().display()));
                failed_fuse_handles.push(handle);
            }
        }
        if !failed_fuse_handles.is_empty() {
            inner.runtime.fuse_handles.extend(failed_fuse_handles);
            let detail = format!(
                "failed unmounting VM FUSE filesystems after qemu exit: {}",
                unmount_failures.join("; ")
            );
            warn!(vm_id = %vm_id, error = %detail, "VM runtime cleanup remains retryable");
            new_state = VmState::Error;
            log_message = Some(detail.clone());
            exit_error = Some(anyhow!(detail));
        }
        if let Some(handle) = inner.runtime.tap_network.take() {
            if tokio::time::timeout(Duration::from_secs(10), handle.shutdown())
                .await
                .is_err()
            {
                warn!(
                    vm_id = %vm_id,
                    "timed out cleaning up tap network after VM exit"
                );
            }
        }

        let preserve_existing_error_reason = {
            let mut exit_guard = inner
                .runtime
                .exit_status
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if inner.runtime.state == VmState::Error && exit_guard.is_some() {
                true
            } else {
                *exit_guard = exit_error.take();
                false
            }
        };

        let final_state = if preserve_existing_error_reason {
            VmState::Error
        } else {
            new_state
        };
        inner.runtime.state = final_state;
        inner.metadata.state = final_state;
        inner.metadata.started_at = inner.runtime.started_at;

        if let Some(msg) = log_message {
            if let Some(tail) = &tail {
                error!(vm_id = %vm_id, error = %msg, tail = %tail, "VM exited with failure");
            } else {
                error!(vm_id = %vm_id, error = %msg, "VM exited with failure");
            }
        }

        if let Err(err) = save_metadata(&vm.dir, &mut inner.metadata) {
            warn!(vm_id = %vm_id, error = %err, "persist metadata after exit failed");
        }
        drop(inner);
        if let Err(err) = network::unregister_vm_proxy_policy(&vm_id_for_proxy_cleanup).await {
            warn!(
                vm_id = %vm_id_for_proxy_cleanup,
                error = %err,
                "failed to unregister vm proxy policy after exit"
            );
        }
        if let Some(cgroup) = qemu_cgroup {
            cgroup.cleanup();
        }
    });
}

#[cfg(unix)]
fn configure_qemu_process_identity(
    cmd: &mut Command,
    vm_dir: &Path,
    runtime_dir: &Path,
    additional_writable_paths: &[PathBuf],
    cfg: &Config,
) -> Result<()> {
    if unsafe { libc::geteuid() } != 0 {
        return Ok(());
    }

    recursively_chown_path_skipping(
        vm_dir,
        cfg.qemu_process.run_as_uid,
        cfg.qemu_process.run_as_gid,
        &[vm_dir.join("fuse-mounts")],
    )
    .with_context(|| format!("chown vm dir {} for qemu user", vm_dir.display()))?;
    recursively_chown_path_skipping(
        runtime_dir,
        cfg.qemu_process.run_as_uid,
        cfg.qemu_process.run_as_gid,
        &[],
    )
    .with_context(|| {
        format!(
            "chown qemu runtime dir {} for qemu user",
            runtime_dir.display()
        )
    })?;
    for path in additional_writable_paths {
        chown_single_path(
            path,
            cfg.qemu_process.run_as_uid,
            cfg.qemu_process.run_as_gid,
        )
        .with_context(|| format!("chown qemu writable path {}", path.display()))?;
    }

    let uid = cfg.qemu_process.run_as_uid;
    let gid = cfg.qemu_process.run_as_gid;
    unsafe {
        cmd.pre_exec(move || {
            let groups = [gid as libc::gid_t];
            #[cfg(target_os = "linux")]
            let ngroups = groups.len();
            #[cfg(not(target_os = "linux"))]
            let ngroups: libc::c_int = groups
                .len()
                .try_into()
                .expect("supplementary group count fits in c_int");

            if libc::setgroups(ngroups, groups.as_ptr()) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::setgid(gid as libc::gid_t) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::setuid(uid as libc::uid_t) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn configure_qemu_process_identity(
    _cmd: &mut Command,
    _vm_dir: &Path,
    _runtime_dir: &Path,
    _additional_writable_paths: &[PathBuf],
    _cfg: &Config,
) -> Result<()> {
    Ok(())
}

fn prepare_qemu_runtime_dir(runtime_dir: &Path, cfg: &Config) -> Result<()> {
    fs::create_dir_all(runtime_dir)
        .with_context(|| format!("create qemu runtime dir {}", runtime_dir.display()))?;
    ensure_qemu_writable_runtime_dir(runtime_dir, cfg)
}

#[cfg(unix)]
fn ensure_qemu_writable_runtime_dir(path: &Path, cfg: &Config) -> Result<()> {
    if unsafe { libc::geteuid() } != 0 {
        return Ok(());
    }
    chown_single_path(
        path,
        cfg.qemu_process.run_as_uid,
        cfg.qemu_process.run_as_gid,
    )?;
    let permissions = fs::Permissions::from_mode(0o2770);
    fs::set_permissions(path, permissions)
        .with_context(|| format!("chmod qemu runtime dir {}", path.display()))
}

#[cfg(not(unix))]
fn ensure_qemu_writable_runtime_dir(_path: &Path, _cfg: &Config) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn recursively_chown_path_skipping(
    path: &Path,
    uid: u32,
    gid: u32,
    skipped_roots: &[PathBuf],
) -> Result<()> {
    if should_skip_chown_path(path, skipped_roots) {
        return Ok(());
    }

    chown_single_path(path, uid, gid)?;
    if path.is_dir() {
        for entry in fs::read_dir(path)
            .with_context(|| format!("read directory for chown {}", path.display()))?
        {
            let entry = entry.with_context(|| format!("walk directory {}", path.display()))?;
            let entry_path = entry.path();
            if should_skip_chown_path(entry_path.as_path(), skipped_roots) {
                continue;
            }
            let file_type = entry
                .file_type()
                .with_context(|| format!("stat {}", entry_path.display()))?;
            if file_type.is_dir() {
                recursively_chown_path_skipping(entry_path.as_path(), uid, gid, skipped_roots)?;
            } else {
                chown_single_path(entry_path.as_path(), uid, gid)?;
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
fn should_skip_chown_path(path: &Path, skipped_roots: &[PathBuf]) -> bool {
    skipped_roots.iter().any(|root| path.starts_with(root))
}

#[cfg(unix)]
fn ensure_qemu_writable_snapshot_dir(path: &Path, cfg: &Config) -> Result<()> {
    chown_single_path(
        path,
        cfg.qemu_process.run_as_uid,
        cfg.qemu_process.run_as_gid,
    )?;
    let permissions = fs::Permissions::from_mode(0o2770);
    fs::set_permissions(path, permissions)
        .with_context(|| format!("chmod qemu-writable snapshot dir {}", path.display()))
}

#[cfg(not(unix))]
fn ensure_qemu_writable_snapshot_dir(_path: &Path, _cfg: &Config) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn chown_single_path(path: &Path, uid: u32, gid: u32) -> Result<()> {
    let bytes = path.as_os_str().as_bytes();
    let c_path = std::ffi::CString::new(bytes)
        .with_context(|| format!("convert path to cstring {}", path.display()))?;
    let rc = unsafe { libc::lchown(c_path.as_ptr(), uid as libc::uid_t, gid as libc::gid_t) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("chown {}", path.display()));
    }
    Ok(())
}

#[cfg(not(unix))]
fn recursively_chown_path(_path: &Path, _uid: u32, _gid: u32) -> Result<()> {
    Ok(())
}

fn kill_process(pid: u32) -> Result<()> {
    if pid == 0 {
        return Ok(());
    }
    let result = unsafe { libc::kill(pid as i32, libc::SIGKILL) };
    if result != 0 {
        let err = std::io::Error::last_os_error();
        if matches!(err.raw_os_error(), Some(code) if code == libc::ESRCH) {
            return Ok(());
        }
        return Err(err.into());
    }
    Ok(())
}

// @dive: Local runtime ownership reclaim fences orphaned qemu by attempting QMP quit first, then a verified pid kill fallback.
async fn reclaim_local_runtime_ownership(
    vm_id: &str,
    vm_dir: &Path,
    qmp_path: &Path,
    pid_path: &Path,
) -> Result<bool> {
    let mut reclaimed = false;
    let mut quit_attempted = false;
    let mut seen_pids = HashSet::new();

    if qmp_path.exists() {
        if let Ok(monitor) = virt::wait_for_monitor(qmp_path, Duration::from_millis(750)).await {
            quit_attempted = true;
            match virt::quit(&monitor).await {
                Ok(()) => reclaimed = true,
                Err(err) => {
                    warn!(
                        vm_id = %vm_id,
                        qmp_path = %qmp_path.display(),
                        error = %err,
                        "failed to send qmp quit during ownership reclaim"
                    );
                }
            }
        }
    }

    if quit_attempted {
        let _ = wait_for_runtime_release(qmp_path, pid_path, Duration::from_secs(5)).await;
    }

    if let Some(pid) = read_pid_file(pid_path) {
        seen_pids.insert(pid);
        if !pid_exists(pid) {
        } else {
            let Some(command) = read_process_command(pid) else {
                warn!(
                    vm_id = %vm_id,
                    pid,
                    "skipping pid kill fallback because process command could not be inspected"
                );
                return Ok(reclaimed);
            };

            if !pid_command_matches_vm(command.as_str(), vm_dir, qmp_path) {
                warn!(
                    vm_id = %vm_id,
                    pid,
                    command = %command,
                    "skipping pid kill fallback because command does not match expected VM runtime markers"
                );
                return Ok(reclaimed);
            }

            kill_process(pid)?;
            reclaimed = true;
            let _ = wait_for_pid_exit(pid, Duration::from_secs(3)).await;
        }
    }

    // @dive: Some fail-stop paths leave orphaned qemu without a pidfile; sweep local process table for VM-path-matched qemu processes.
    for pid in list_local_qemu_pids_for_vm(vm_dir, qmp_path) {
        if !seen_pids.insert(pid) {
            continue;
        }
        if !pid_exists(pid) {
            continue;
        }
        kill_process(pid)?;
        reclaimed = true;
        let _ = wait_for_pid_exit(pid, Duration::from_secs(3)).await;
    }

    Ok(reclaimed)
}

async fn cleanup_stale_runtime_sidecars(vm_id: &str, vm_dir: &Path, runtime_dir: &Path) {
    for pid in list_local_virtiofsd_pids_for_vm(vm_dir, runtime_dir) {
        if !pid_exists(pid) {
            continue;
        }
        match kill_process(pid) {
            Ok(()) => {
                let _ = wait_for_pid_exit(pid, Duration::from_secs(3)).await;
                warn!(
                    vm_id = %vm_id,
                    pid,
                    "killed stale virtiofsd process after reclaiming VM runtime ownership"
                );
            }
            Err(err) => {
                warn!(
                    vm_id = %vm_id,
                    pid,
                    error = %err,
                    "failed killing stale virtiofsd process after reclaiming VM runtime ownership"
                );
            }
        }
    }

    lazy_unmount_stale_fuse_mounts(vm_id, vm_dir);
}

fn lazy_unmount_stale_fuse_mounts(vm_id: &str, vm_dir: &Path) {
    let fuse_dir = vm_dir.join("fuse-mounts");
    let Ok(entries) = fs::read_dir(&fuse_dir) else {
        return;
    };

    for entry in entries.flatten() {
        let mountpoint = entry.path();
        if !mountpoint.is_dir() {
            continue;
        }

        match StdCommand::new("umount")
            .arg("-l")
            .arg(&mountpoint)
            .status()
        {
            Ok(status) if status.success() => {
                warn!(
                    vm_id = %vm_id,
                    mountpoint = %mountpoint.display(),
                    "lazy-unmounted stale FUSE mount after reclaiming VM runtime ownership"
                );
            }
            Ok(status) => {
                debug!(
                    vm_id = %vm_id,
                    mountpoint = %mountpoint.display(),
                    status = %status,
                    "stale FUSE lazy-unmount did not detach mountpoint"
                );
            }
            Err(err) => {
                warn!(
                    vm_id = %vm_id,
                    mountpoint = %mountpoint.display(),
                    error = %err,
                    "failed running lazy unmount for stale FUSE mountpoint"
                );
            }
        }
    }
}

async fn wait_for_runtime_release(qmp_path: &Path, pid_path: &Path, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let pid_alive = read_pid_file(pid_path).map(pid_exists).unwrap_or(false);
        let qmp_present = qmp_path.exists();
        if !pid_alive && !qmp_present {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        sleep(Duration::from_millis(150)).await;
    }
}

async fn wait_for_pid_exit(pid: u32, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if !pid_exists(pid) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        sleep(Duration::from_millis(150)).await;
    }
}

fn pid_exists(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    let rc = unsafe { libc::kill(pid as i32, 0) };
    if rc == 0 {
        return true;
    }
    matches!(std::io::Error::last_os_error().raw_os_error(), Some(code) if code == libc::EPERM)
}

fn read_process_command(pid: u32) -> Option<String> {
    let pid_arg = pid.to_string();
    let output = StdCommand::new("ps")
        .arg("-p")
        .arg(&pid_arg)
        .arg("-o")
        .arg("command=")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let command = String::from_utf8(output.stdout).ok()?;
    let trimmed = command.trim().to_string();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed)
}

fn list_local_qemu_pids_for_vm(vm_dir: &Path, qmp_path: &Path) -> Vec<u32> {
    let output = StdCommand::new("ps")
        .arg("-Ao")
        .arg("pid=,command=")
        .output();
    let Ok(output) = output else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    let Ok(stdout) = String::from_utf8(output.stdout) else {
        return Vec::new();
    };

    let mut out = Vec::new();
    for line in stdout.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut iter = trimmed.split_whitespace();
        let Some(pid_token) = iter.next() else {
            continue;
        };
        let command = trimmed[pid_token.len()..].trim_start();
        if command.is_empty() {
            continue;
        }
        let Ok(pid) = pid_token.parse::<u32>() else {
            continue;
        };
        if pid == 0 {
            continue;
        }
        if pid_command_matches_vm(command, vm_dir, qmp_path) {
            out.push(pid);
        }
    }
    out
}

fn list_local_virtiofsd_pids_for_vm(vm_dir: &Path, runtime_dir: &Path) -> Vec<u32> {
    let output = StdCommand::new("ps")
        .arg("-Ao")
        .arg("pid=,command=")
        .output();
    let Ok(output) = output else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    let Ok(stdout) = String::from_utf8(output.stdout) else {
        return Vec::new();
    };

    let vm_dir_str = vm_dir.to_string_lossy();
    let runtime_dir_str = runtime_dir.to_string_lossy();
    let mut out = Vec::new();
    for line in stdout.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut iter = trimmed.split_whitespace();
        let Some(pid_token) = iter.next() else {
            continue;
        };
        let command = trimmed[pid_token.len()..].trim_start();
        if command.is_empty() {
            continue;
        }
        let Ok(pid) = pid_token.parse::<u32>() else {
            continue;
        };
        if pid == 0 {
            continue;
        }
        if command.contains("virtiofsd")
            && (command.contains(vm_dir_str.as_ref()) || command.contains(runtime_dir_str.as_ref()))
        {
            out.push(pid);
        }
    }
    out
}

fn pid_command_matches_vm(command: &str, vm_dir: &Path, qmp_path: &Path) -> bool {
    if !command.contains("qemu-system") {
        return false;
    }
    let vm_dir_str = vm_dir.to_string_lossy();
    let qmp_str = qmp_path.to_string_lossy();
    command.contains(vm_dir_str.as_ref()) || command.contains(qmp_str.as_ref())
}

fn read_pid_file(path: &Path) -> Option<u32> {
    let content = fs::read_to_string(path).ok()?;
    let pid = content.trim().parse::<u32>().ok()?;
    if pid == 0 {
        return None;
    }
    Some(pid)
}

fn normalize_arch(arch: &str) -> ManagerResult<String> {
    match arch.trim().to_lowercase().as_str() {
        "" => Ok(String::new()),
        "amd64" | "x86_64" => Ok(ARCH_AMD64.to_string()),
        "arm64" | "aarch64" => Ok(ARCH_ARM64.to_string()),
        other => Err(ManagerError::Other(anyhow!(
            "unsupported architecture: {other}"
        ))),
    }
}

fn random_mac() -> Result<String> {
    let mut bytes = [0u8; 6];
    let mut rng = rand::rng();
    rng.fill_bytes(&mut bytes);
    bytes[0] = (bytes[0] | 0x02) & 0xfe;
    Ok(format!(
        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5]
    ))
}

fn ensure_portproxy_auth_token(metadata: &mut HashMap<String, String>) -> String {
    match metadata
        .get(META_PORTPROXY_AUTH_TOKEN)
        .map(String::as_str)
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        Some(token) => token.to_string(),
        None => assign_new_portproxy_auth_token(metadata),
    }
}

fn assign_new_portproxy_auth_token(metadata: &mut HashMap<String, String>) -> String {
    let mut bytes = [0u8; 32];
    let mut rng = rand::rng();
    rng.fill_bytes(&mut bytes);
    let token = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    metadata.insert(META_PORTPROXY_AUTH_TOKEN.to_string(), token.clone());
    token
}

fn platform_supports_architecture(platforms: &[virt::Platform], arch: &str) -> bool {
    platforms.iter().any(|p| {
        p.os.to_lowercase() == "linux"
            && normalize_arch(&p.arch).map(|a| a == arch).unwrap_or(false)
    })
}

fn choose_preferred_architecture(platforms: &[virt::Platform], host: &str) -> Option<String> {
    let mut preferred = Vec::new();
    if !host.is_empty() {
        preferred.push(host.to_string());
    }
    preferred.push(ARCH_AMD64.to_string());
    preferred.push(ARCH_ARM64.to_string());

    for candidate in preferred {
        if platform_supports_architecture(platforms, &candidate) {
            return Some(candidate);
        }
    }
    None
}

fn emit_download_progress(
    progress: &Option<CreateVmProgressCallback>,
    downloaded_bytes: u64,
    total_bytes: Option<u64>,
) {
    if let Some(callback) = progress {
        callback(CreateVmProgressEvent::DownloadBytes {
            downloaded_bytes,
            total_bytes,
        });
    }
}

fn emit_stage_progress(
    progress: &Option<CreateVmProgressCallback>,
    stage: CreateVmStage,
    percent: u32,
    message: impl Into<String>,
) {
    if let Some(callback) = progress {
        callback(CreateVmProgressEvent::StageProgress {
            stage,
            percent,
            message: Some(message.into()),
        });
    }
}

fn is_snapshot_not_found(err: &anyhow::Error) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("snapshot") && msg.contains("not found")
}

fn fork_snapshot_durability(
    storage_profile: config::StorageProfile,
    parent_was_running: bool,
) -> (&'static str, &'static str) {
    match (storage_profile, parent_was_running) {
        (config::StorageProfile::LocalEphemeral, false) => {
            ("local-ephemeral-cow-overlay", "daemon-restart-same-node")
        }
        (config::StorageProfile::LocalEphemeral, true) => (
            "local-ephemeral-running-snapshot",
            "daemon-restart-same-node",
        ),
        (config::StorageProfile::DurableShared, false) => (
            "durable-shared-cow-overlay",
            "durable-storage-daemon-restart",
        ),
        (config::StorageProfile::DurableShared, true) => (
            "durable-shared-running-snapshot",
            "durable-storage-daemon-restart",
        ),
    }
}

fn positive_resource_bound_from_env(primary: &str, fallback: &str, default: i32) -> Result<i32> {
    let configured = env::var(primary)
        .ok()
        .map(|value| (primary, value))
        .or_else(|| env::var(fallback).ok().map(|value| (fallback, value)));
    let Some((name, raw)) = configured else {
        return Ok(default);
    };
    let value = raw
        .trim()
        .parse::<i32>()
        .with_context(|| format!("{name} must be a positive 32-bit integer"))?;
    if value <= 0 {
        bail!("{name} must be greater than zero");
    }
    Ok(value)
}

fn enforce_resource_bounds(
    resources: &crate::state::ResourceSpec,
    bounds: VmResourceBounds,
) -> ManagerResult<()> {
    if resources.vcpu > bounds.max_vcpu {
        return Err(ManagerError::CapacityExceeded {
            resource: "vcpu",
            limit: bounds.max_vcpu as usize,
            current: resources.vcpu as usize,
        });
    }
    if resources.memory_mb > bounds.max_memory_mb {
        return Err(ManagerError::CapacityExceeded {
            resource: "memory_mb",
            limit: bounds.max_memory_mb as usize,
            current: resources.memory_mb as usize,
        });
    }
    if resources.disk_gb > bounds.max_disk_gb {
        return Err(ManagerError::CapacityExceeded {
            resource: "disk_gb",
            limit: bounds.max_disk_gb as usize,
            current: resources.disk_gb as usize,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_resource_bounds_preserve_existing_admission_limits() {
        let resources = crate::state::ResourceSpec {
            vcpu: 8,
            memory_mb: 32 * 1024,
            disk_gb: 64,
        };

        let error = enforce_resource_bounds(&resources, VmResourceBounds::default())
            .expect_err("default memory ceiling should reject 32 GiB");
        assert!(matches!(
            error,
            ManagerError::CapacityExceeded {
                resource: "memory_mb",
                limit: 16_384,
                current: 32_768,
            }
        ));
    }

    #[test]
    fn configured_resource_bounds_allow_larger_dedicated_vm() {
        let resources = crate::state::ResourceSpec {
            vcpu: 8,
            memory_mb: 32 * 1024,
            disk_gb: 64,
        };
        let bounds = VmResourceBounds {
            max_vcpu: 16,
            max_memory_mb: 64 * 1024,
            max_disk_gb: 200,
        };

        enforce_resource_bounds(&resources, bounds)
            .expect("operator-configured bounds should admit the dedicated VM");
    }

    #[tokio::test]
    async fn base_image_lock_serializes_same_target() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let manager = Arc::new(
            Manager::new(Config {
                listen_address: "127.0.0.1:0".to_string(),
                data_dir: tmp.path().to_string_lossy().to_string(),
                ..Config::default()
            })
            .await
            .expect("create manager"),
        );
        let target = tmp.path().join("base-images/runtime.qcow2");
        let first = manager.lock_base_image(&target).await;
        let waiting_manager = Arc::clone(&manager);
        let waiting_target = target.clone();
        let waiting = tokio::spawn(async move {
            let _guard = waiting_manager.lock_base_image(&waiting_target).await;
        });

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            !waiting.is_finished(),
            "same-target preparation must wait for the active converter"
        );
        drop(first);
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("queued preparation should acquire the released lock")
            .expect("queued preparation task should succeed");
    }

    fn qemu_img_available() -> bool {
        std::process::Command::new("qemu-img")
            .arg("--version")
            .output()
            .is_ok()
    }

    #[cfg(unix)]
    fn blocking_qemu_img(root: &Path) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
        let script = root.join("blocking-qemu-img");
        let entered = root.join("qemu-img-entered");
        let release = root.join("qemu-img-release");
        let calls = root.join("qemu-img-calls");
        fs::write(
            &script,
            format!(
                "#!/bin/sh\nprintf 'call\\n' >> '{}'\ntouch '{}'\nwhile [ ! -f '{}' ]; do sleep 0.01; done\n",
                calls.display(),
                entered.display(),
                release.display(),
            ),
        )
        .expect("write blocking qemu-img");
        let mut permissions = fs::metadata(&script)
            .expect("read blocking qemu-img metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script, permissions).expect("make blocking qemu-img executable");
        (script, entered, release, calls)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn durable_volume_creation_does_not_block_inventory_and_serializes_same_owner() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let (qemu_img_bin, entered, release, calls) = blocking_qemu_img(tmp.path());
        let manager = Arc::new(
            Manager::new(Config {
                listen_address: "127.0.0.1:0".to_string(),
                data_dir: tmp.path().join("vmd").to_string_lossy().to_string(),
                qemu_img_bin: qemu_img_bin.to_string_lossy().to_string(),
                ..Config::default()
            })
            .await
            .expect("create manager"),
        );
        let owner_key = "workspace:test:thread:contended";

        let first_manager = Arc::clone(&manager);
        let first =
            tokio::spawn(async move { first_manager.ensure_durable_volume(owner_key, 1).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !entered.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("first qemu-img invocation should start");

        let second_manager = Arc::clone(&manager);
        let second =
            tokio::spawn(async move { second_manager.ensure_durable_volume(owner_key, 1).await });
        let listed =
            tokio::time::timeout(Duration::from_millis(200), manager.list_durable_volumes())
                .await
                .expect("inventory must not wait for qemu-img");
        assert!(listed.is_empty(), "uncommitted volume must stay invisible");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            fs::read_to_string(&calls)
                .expect("read qemu-img calls")
                .lines()
                .count(),
            1,
            "same-owner creation must wait on the per-volume reservation"
        );

        fs::write(&release, b"release").expect("release qemu-img");
        let first_volume = first
            .await
            .expect("join first creation")
            .expect("first creation succeeds");
        let second_volume = second
            .await
            .expect("join second creation")
            .expect("second creation reuses committed volume");
        assert_eq!(first_volume.volume_id, second_volume.volume_id);
        assert_eq!(manager.list_durable_volumes().await.len(), 1);
        assert_eq!(
            fs::read_to_string(calls)
                .expect("read final qemu-img calls")
                .lines()
                .count(),
            1,
        );
    }

    #[tokio::test]
    async fn durable_volume_inventory_does_not_wait_behind_capacity_insertion() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let manager = Arc::new(
            Manager::new(Config {
                listen_address: "127.0.0.1:0".to_string(),
                data_dir: tmp.path().join("vmd").to_string_lossy().to_string(),
                max_active_vms: Some(2),
                ..Config::default()
            })
            .await
            .expect("create manager"),
        );
        let attachment = DurableVolumeAttachment {
            owner_key: "workspace:test:thread:busy-runtime".to_string(),
            volume_id: "busy-runtime".to_string(),
            size_gb: 1,
        };
        manager.volumes.write().await.insert(
            attachment.owner_key.clone(),
            DurableVolumeMetadata {
                owner_key: attachment.owner_key.clone(),
                volume_id: attachment.volume_id.clone(),
                size_gb: attachment.size_gb,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                backing_volume_id: None,
            },
        );
        let vm_id = "busy-volume-vm".to_string();
        let vm_dir = tmp.path().join("vmd").join(&vm_id);
        fs::create_dir_all(&vm_dir).expect("create vm dir");
        let mut metadata = qemu_test_metadata(&vm_id);
        metadata.state = VmState::Running;
        metadata.durable_volume = Some(attachment);
        let vm = Arc::new(Vm::new(metadata, VmRuntime::new(&vm_dir), vm_dir));
        manager
            .vms
            .write()
            .await
            .insert(vm_id.clone(), Arc::clone(&vm));

        let busy_guard = vm.lock().await;
        let candidate_id = "capacity-candidate".to_string();
        let candidate_dir = tmp.path().join("vmd").join(&candidate_id);
        fs::create_dir_all(&candidate_dir).expect("create candidate VM dir");
        let mut candidate_metadata = qemu_test_metadata(&candidate_id);
        candidate_metadata.state = VmState::Creating;
        let candidate = Arc::new(Vm::new(
            candidate_metadata,
            VmRuntime::new(&candidate_dir),
            candidate_dir,
        ));
        let insert_manager = Arc::clone(&manager);
        let insert_id = candidate_id.clone();
        let insertion = tokio::spawn(async move {
            insert_manager
                .insert_creating_vm_with_capacity(insert_id, candidate)
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !insertion.is_finished(),
            "capacity insertion should be waiting on the busy VM runtime"
        );

        let listed =
            tokio::time::timeout(Duration::from_millis(200), manager.list_durable_volumes())
                .await
                .expect("inventory must not wait behind capacity insertion");
        assert_eq!(listed.len(), 1);
        assert!(listed[0].1.is_empty());
        drop(busy_guard);

        insertion
            .await
            .expect("join capacity insertion")
            .expect("capacity insertion should succeed");
        assert!(manager.vms.read().await.contains_key(&candidate_id));
        let listed = manager.list_durable_volumes().await;
        assert_eq!(listed[0].1, vec![vm_id]);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn durable_volume_fork_does_not_block_inventory() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let (qemu_img_bin, entered, release, calls) = blocking_qemu_img(tmp.path());
        let manager = Arc::new(
            Manager::new(Config {
                listen_address: "127.0.0.1:0".to_string(),
                data_dir: tmp.path().join("vmd").to_string_lossy().to_string(),
                qemu_img_bin: qemu_img_bin.to_string_lossy().to_string(),
                ..Config::default()
            })
            .await
            .expect("create manager"),
        );
        let parent = DurableVolumeAttachment {
            owner_key: "workspace:test:thread:fork-parent".to_string(),
            volume_id: "fork-parent".to_string(),
            size_gb: 1,
        };
        let child = DurableVolumeAttachment {
            owner_key: "workspace:test:thread:fork-child".to_string(),
            volume_id: "fork-child".to_string(),
            size_gb: 1,
        };
        fs::create_dir_all(manager.volumes_dir()).expect("create volumes directory");
        fs::write(manager.volume_disk_path(&parent.volume_id), b"parent")
            .expect("create parent volume disk");
        manager.volumes.write().await.insert(
            parent.owner_key.clone(),
            DurableVolumeMetadata {
                owner_key: parent.owner_key.clone(),
                volume_id: parent.volume_id.clone(),
                size_gb: parent.size_gb,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                backing_volume_id: None,
            },
        );

        let fork_manager = Arc::clone(&manager);
        let fork_parent = parent.clone();
        let fork_child = child.clone();
        let fork = tokio::spawn(async move {
            fork_manager
                .fork_durable_volume(&fork_parent, &fork_child, "contention-test")
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !entered.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("fork qemu-img invocation should start");

        let listed =
            tokio::time::timeout(Duration::from_millis(200), manager.list_durable_volumes())
                .await
                .expect("inventory must not wait for fork qemu-img");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].0.owner_key, parent.owner_key);

        fs::write(&release, b"release").expect("release fork qemu-img");
        fork.await
            .expect("join durable volume fork")
            .expect("durable volume fork succeeds");
        let listed = manager.list_durable_volumes().await;
        assert_eq!(listed.len(), 2);
        assert_eq!(
            fs::read_to_string(calls)
                .expect("read fork qemu-img calls")
                .lines()
                .count(),
            2,
        );
    }

    fn qemu_backing_file(path: &Path) -> Option<String> {
        let output = std::process::Command::new("qemu-img")
            .arg("info")
            .arg(path)
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines() {
            if let Some(rest) = line.trim().strip_prefix("backing file:") {
                let trimmed = rest.trim();
                let backing = trimmed
                    .split(" (actual")
                    .next()
                    .map(str::trim)
                    .unwrap_or_default();
                if !backing.is_empty() {
                    return Some(backing.to_string());
                }
            }
        }
        None
    }

    fn test_tap_spec(vm_id: &str) -> network::tap::VmTapNetworkSpec {
        network::tap::spec_for_vm(
            vm_id,
            "127.0.0.1",
            3000,
            3001,
            network::tap::POLICY_ENVOY_PORT,
        )
        .expect("test tap spec")
    }

    fn qemu_test_metadata(vm_id: &str) -> VmMetadata {
        VmMetadata {
            id: vm_id.to_string(),
            name: vm_id.to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: VmState::Stopped,
            architecture: ARCH_AMD64.to_string(),
            source: VmSource {
                source_type: VmSourceType::Docker,
                reference: "mock/image:latest".to_string(),
            },
            resources: crate::state::ResourceSpec {
                vcpu: 1,
                memory_mb: 512,
                disk_gb: 10,
            },
            network: NetworkSpec {
                mac: "02:00:00:00:00:20".to_string(),
                proxy_port: 3000,
                rpc_port: 3001,
            },
            metadata: HashMap::new(),
            snapshots: Vec::new(),
            shared_mounts: Vec::new(),
            pci_devices: Vec::new(),
            durable_volume: None,
            boot_incoming_ram_path: String::new(),
            started_at: None,
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancelled_launch_kills_owned_child_within_bound() {
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg("exec sleep 30");
        let child = spawn_launch_child(&mut command, "test qemu", true)
            .expect("spawn cancellation-owned child");
        let pid = child.id().expect("spawned child should have a pid");
        assert!(pid_exists(pid));

        let timeout_result = tokio::time::timeout(Duration::from_millis(25), async move {
            let _owned_child = child;
            std::future::pending::<()>().await;
        })
        .await;
        assert!(timeout_result.is_err(), "launch future should time out");
        assert!(
            wait_for_pid_exit(pid, Duration::from_secs(2)).await,
            "dropping a timed-out launch must kill its child"
        );
    }

    #[tokio::test]
    async fn fork_vm_stopped_parent_uses_shared_cow_backing() {
        if !qemu_img_available() {
            eprintln!("qemu-img unavailable; skipping fork CoW runtime test");
            return;
        }

        let test_arch = if cfg!(target_arch = "aarch64") {
            ARCH_ARM64
        } else {
            ARCH_AMD64
        };

        let tmp = tempfile::tempdir().expect("create tempdir");
        let data_dir = tmp.path().to_path_buf();
        fs::create_dir_all(data_dir.join(config::BASE_IMAGES_DIR_NAME))
            .expect("create base images");

        let cfg = Config {
            listen_address: "127.0.0.1:0".to_string(),
            data_dir: data_dir.to_string_lossy().to_string(),
            qemu_bin: "qemu-system-x86_64".to_string(),
            qemu_arm64_bin: "qemu-system-aarch64".to_string(),
            qemu_img_bin: "qemu-img".to_string(),
            virtiofsd_bin: "/usr/lib/qemu/virtiofsd".to_string(),
            docker_bin: "docker".to_string(),
            log_level: "info".to_string(),
            force_local_build: false,
            max_active_vms: None,
            max_fork_chain_depth: 32,
            fork_compaction_depth_threshold: 8,
            storage_profile: config::StorageProfile::LocalEphemeral,
            shared_mount_profiles: vec!["local-path".to_string(), "shared-posix".to_string()],
            ha_mode: false,
            node_registry: None,
            control_bus: None,
            vfs_internal_service_token: None,
            pci_policy_path: None,
            pci: Default::default(),
            qemu_process: Default::default(),
            guest_network: Default::default(),
            network_services: Default::default(),
            security: Default::default(),
            snapshot_staging_dir: None,
        };

        let manager = Manager {
            cfg,
            host_arch: test_arch.to_string(),
            resource_bounds: VmResourceBounds::default(),
            base_image_locks: Mutex::new(HashMap::new()),
            vms: RwLock::new(HashMap::new()),
            vm_capacity_lock: Mutex::new(()),
            session_registry: RwLock::new(HashMap::new()),
            volumes: RwLock::new(HashMap::new()),
            volume_operation_locks: Mutex::new(HashMap::new()),
            pci_leases: Mutex::new(HashMap::new()),
            snapshots: RwLock::new(HashMap::new()),
        };

        let proxy_bin_dir = data_dir.join("proxy-bin");
        fs::create_dir_all(&proxy_bin_dir).expect("create proxy bin dir");
        fs::write(
            proxy_bin_dir.join("portproxy-linux-amd64"),
            b"stub-portproxy-amd64",
        )
        .expect("write amd64 proxy stub");
        fs::write(
            proxy_bin_dir.join("portproxy-linux-arm64"),
            b"stub-portproxy-arm64",
        )
        .expect("write arm64 proxy stub");
        unsafe {
            std::env::set_var("PROXY_BIN", &proxy_bin_dir);
        }

        let parent_id = "parent-vm".to_string();
        let parent_dir = data_dir.join(&parent_id);
        fs::create_dir_all(&parent_dir).expect("create parent dir");
        let parent_disk = parent_dir.join("disk.qcow2");
        let create_output = std::process::Command::new("qemu-img")
            .args(["create", "-f", "qcow2"])
            .arg(&parent_disk)
            .arg("1G")
            .output()
            .expect("run qemu-img create");
        assert!(
            create_output.status.success(),
            "qemu-img create failed: {}",
            String::from_utf8_lossy(&create_output.stderr)
        );
        let parent_volume = manager
            .ensure_durable_volume("workspace:test:thread:parent", 1)
            .await
            .expect("create parent durable volume");

        let mut parent_meta = VmMetadata {
            id: parent_id.clone(),
            name: "parent".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: VmState::Stopped,
            architecture: test_arch.to_string(),
            source: VmSource {
                source_type: VmSourceType::Docker,
                reference: "mock/image:latest".to_string(),
            },
            resources: crate::state::ResourceSpec {
                vcpu: 1,
                memory_mb: 1024,
                disk_gb: 1,
            },
            network: NetworkSpec {
                mac: "02:00:00:00:00:01".to_string(),
                proxy_port: 0,
                rpc_port: 0,
            },
            metadata: HashMap::from([(
                META_STORAGE_PROFILE.to_string(),
                "durable-data".to_string(),
            )]),
            snapshots: Vec::new(),
            shared_mounts: vec![SharedMountSpec {
                host_path: parent_dir.to_string_lossy().to_string(),
                guest_path: "/workspace".to_string(),
                mount_tag: "runtimefs".to_string(),
                read_only: false,
                availability: SharedMountAvailability::SharedStorage,
                continuity: SharedMountContinuity::RestoreCrossNode,
                backend_profile: "shared-posix".to_string(),
                vfs_endpoint: String::new(),
                vfs_scope_path: String::new(),
            }],
            pci_devices: Vec::new(),
            durable_volume: Some(parent_volume.clone()),
            boot_incoming_ram_path: String::new(),
            started_at: None,
        };
        save_metadata(&parent_dir, &mut parent_meta).expect("save parent metadata");

        let parent_vm = Arc::new(Vm::new(
            parent_meta.clone(),
            VmRuntime::new(&parent_dir),
            parent_dir.clone(),
        ));
        manager
            .vms
            .write()
            .await
            .insert(parent_id.clone(), parent_vm);

        let mut child_meta_extra = HashMap::new();
        child_meta_extra.insert(
            "chevalier.session_id".to_string(),
            "child-session".to_string(),
        );
        let (parent_after, child_after, fork_id) = manager
            .fork_vm(
                &parent_id,
                ForkVmParams {
                    child_name: Some("child".to_string()),
                    child_metadata: child_meta_extra,
                    auto_start_child: false,
                },
            )
            .await
            .expect("fork stopped parent");

        assert_eq!(parent_after.id, parent_id);
        assert_eq!(parent_after.state, VmState::Stopped);
        assert_eq!(child_after.state, VmState::Stopped);
        assert_ne!(child_after.id, parent_after.id);
        assert_eq!(
            child_after
                .metadata
                .get(META_PARENT_VM_ID)
                .map(String::as_str),
            Some(parent_id.as_str())
        );
        assert_eq!(
            child_after.metadata.get(META_FORK_ID).map(String::as_str),
            Some(fork_id.as_str())
        );
        assert_eq!(
            child_after
                .metadata
                .get(META_STORAGE_PROFILE)
                .map(String::as_str),
            Some("durable-data")
        );
        assert_eq!(
            child_after
                .metadata
                .get(META_FORK_DURABILITY_CLASS)
                .map(String::as_str),
            Some("local-ephemeral-cow-overlay")
        );
        assert_eq!(
            child_after
                .metadata
                .get(META_FORK_RESTORE_SCOPE)
                .map(String::as_str),
            Some("daemon-restart-same-node")
        );
        assert_eq!(child_after.shared_mounts, parent_after.shared_mounts);
        assert_eq!(child_after.shared_mounts[0].guest_path, "/workspace");
        assert_eq!(child_after.shared_mounts[0].mount_tag, "runtimefs");
        let child_volume = child_after
            .durable_volume
            .as_ref()
            .expect("forked VM should have a child durable volume");
        assert_ne!(child_volume.volume_id, parent_volume.volume_id);

        let fork_base_path = parent_after
            .metadata
            .get(META_FORK_BASE_PATH)
            .expect("parent metadata missing fork base path")
            .clone();
        assert!(
            Path::new(&fork_base_path).exists(),
            "expected fork base file to exist: {fork_base_path}"
        );

        let child_disk = data_dir.join(&child_after.id).join("disk.qcow2");
        assert!(parent_disk.exists(), "parent disk missing");
        assert!(child_disk.exists(), "child disk missing");

        let parent_backing =
            qemu_backing_file(&parent_disk).expect("parent overlay missing backing file");
        let child_backing =
            qemu_backing_file(&child_disk).expect("child overlay missing backing file");
        assert_eq!(
            parent_backing, child_backing,
            "parent and child must share same backing file"
        );
        assert_eq!(
            PathBuf::from(parent_backing),
            PathBuf::from(fork_base_path),
            "overlay backing should point to persisted fork base"
        );

        let parent_volume_path = manager.volume_disk_path(&parent_volume.volume_id);
        let child_volume_path = manager.volume_disk_path(&child_volume.volume_id);
        let parent_volume_backing = qemu_backing_file(&parent_volume_path)
            .expect("parent durable overlay missing backing file");
        let child_volume_backing = qemu_backing_file(&child_volume_path)
            .expect("child durable overlay missing backing file");
        assert_eq!(
            parent_volume_backing, child_volume_backing,
            "parent and child durable volumes must share an immutable CoW backing"
        );

        unsafe {
            std::env::remove_var("PROXY_BIN");
        }
    }

    #[tokio::test]
    async fn discover_normalizes_running_state_to_stopped() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let data_dir = tmp.path().to_path_buf();
        fs::create_dir_all(data_dir.join(config::BASE_IMAGES_DIR_NAME))
            .expect("create base images");

        let vm_id = "discover-vm".to_string();
        let vm_dir = data_dir.join(&vm_id);
        fs::create_dir_all(&vm_dir).expect("create vm dir");

        let mut meta = VmMetadata {
            id: vm_id.clone(),
            name: "discover".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: VmState::Running,
            architecture: ARCH_AMD64.to_string(),
            source: VmSource {
                source_type: VmSourceType::Docker,
                reference: "mock/image:latest".to_string(),
            },
            resources: crate::state::ResourceSpec {
                vcpu: 1,
                memory_mb: 1024,
                disk_gb: 10,
            },
            network: NetworkSpec {
                mac: "02:00:00:00:00:01".to_string(),
                proxy_port: 0,
                rpc_port: 0,
            },
            metadata: HashMap::from([(
                META_SESSION_ID.to_string(),
                "persisted-session".to_string(),
            )]),
            snapshots: Vec::new(),
            shared_mounts: Vec::new(),
            pci_devices: Vec::new(),
            durable_volume: None,
            boot_incoming_ram_path: String::new(),
            started_at: None,
        };
        save_metadata(&vm_dir, &mut meta).expect("save vm metadata");

        let cfg = Config {
            listen_address: "127.0.0.1:0".to_string(),
            data_dir: data_dir.to_string_lossy().to_string(),
            qemu_bin: "qemu-system-x86_64".to_string(),
            qemu_arm64_bin: "qemu-system-aarch64".to_string(),
            qemu_img_bin: "qemu-img".to_string(),
            virtiofsd_bin: "/usr/lib/qemu/virtiofsd".to_string(),
            docker_bin: "docker".to_string(),
            log_level: "info".to_string(),
            force_local_build: false,
            max_active_vms: None,
            max_fork_chain_depth: 32,
            fork_compaction_depth_threshold: 8,
            storage_profile: config::StorageProfile::LocalEphemeral,
            shared_mount_profiles: vec!["local-path".to_string()],
            ha_mode: false,
            node_registry: None,
            control_bus: None,
            vfs_internal_service_token: None,
            pci_policy_path: None,
            pci: Default::default(),
            qemu_process: Default::default(),
            guest_network: Default::default(),
            network_services: Default::default(),
            security: Default::default(),
            snapshot_staging_dir: None,
        };

        let manager = Manager {
            cfg,
            host_arch: ARCH_AMD64.to_string(),
            resource_bounds: VmResourceBounds::default(),
            base_image_locks: Mutex::new(HashMap::new()),
            vms: RwLock::new(HashMap::new()),
            vm_capacity_lock: Mutex::new(()),
            session_registry: RwLock::new(HashMap::new()),
            volumes: RwLock::new(HashMap::new()),
            volume_operation_locks: Mutex::new(HashMap::new()),
            pci_leases: Mutex::new(HashMap::new()),
            snapshots: RwLock::new(HashMap::new()),
        };

        manager.discover().await.expect("discover VMs");
        let listed = manager.list().await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, vm_id);
        assert_eq!(listed[0].state, VmState::Stopped);
        assert_eq!(
            manager
                .get_by_session_id("persisted-session")
                .await
                .expect("rehydrated session lookup")
                .id,
            vm_id
        );
        manager.session_registry.write().await.clear();
        assert_eq!(
            manager
                .get_by_session_id("persisted-session")
                .await
                .expect("one lookup-time registry rebuild")
                .id,
            vm_id
        );
        assert!(matches!(
            manager.get_by_session_id("missing-session").await,
            Err(ManagerError::VmNotFound)
        ));

        let persisted = load_metadata(&vm_dir).expect("reload persisted metadata");
        assert_eq!(persisted.state, VmState::Stopped);
    }

    #[tokio::test]
    async fn durable_volume_registry_survives_manager_restart_and_explicit_delete() {
        if !qemu_img_available() {
            eprintln!("qemu-img unavailable; skipping durable volume registry runtime test");
            return;
        }

        let tmp = tempfile::tempdir().expect("create tempdir");
        let data_dir = tmp.path().to_path_buf();
        fs::create_dir_all(data_dir.join(config::BASE_IMAGES_DIR_NAME))
            .expect("create base images");
        let cfg = Config {
            listen_address: "127.0.0.1:0".to_string(),
            data_dir: data_dir.to_string_lossy().to_string(),
            qemu_bin: "qemu-system-x86_64".to_string(),
            qemu_arm64_bin: "qemu-system-aarch64".to_string(),
            qemu_img_bin: "qemu-img".to_string(),
            virtiofsd_bin: "/usr/lib/qemu/virtiofsd".to_string(),
            docker_bin: "docker".to_string(),
            log_level: "info".to_string(),
            force_local_build: false,
            max_active_vms: None,
            max_fork_chain_depth: 32,
            fork_compaction_depth_threshold: 8,
            storage_profile: config::StorageProfile::LocalEphemeral,
            shared_mount_profiles: vec!["local-path".to_string()],
            ha_mode: false,
            node_registry: None,
            control_bus: None,
            vfs_internal_service_token: None,
            pci_policy_path: None,
            pci: Default::default(),
            qemu_process: Default::default(),
            guest_network: Default::default(),
            network_services: Default::default(),
            security: Default::default(),
            snapshot_staging_dir: None,
        };

        let manager = Manager::new(cfg.clone()).await.expect("create manager");
        let created = manager
            .ensure_durable_volume("thread:durable-test", 1)
            .await
            .expect("create durable volume");
        assert_eq!(created.owner_key, "thread:durable-test");
        assert!(manager.volume_disk_path(&created.volume_id).is_file());
        drop(manager);

        let restarted = Manager::new(cfg).await.expect("restart manager");
        let listed = restarted.list_durable_volumes().await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].0.owner_key, "thread:durable-test");
        assert!(listed[0].1.is_empty());
        let reattached = restarted
            .ensure_durable_volume("thread:durable-test", 1)
            .await
            .expect("reattach retained volume");
        assert_eq!(reattached.volume_id, created.volume_id);

        let backing_id = "fork-base:durable-delete-test";
        let fork_root = restarted
            .volumes_dir()
            .join(VOLUME_FORK_BASES_DIR_NAME)
            .join("durable-delete-test");
        fs::create_dir_all(&fork_root).expect("create durable fork base directory");
        fs::write(fork_root.join("base.qcow2"), b"test").expect("create durable fork base");
        {
            let mut volumes = restarted.volumes.write().await;
            let volume = volumes
                .get_mut("thread:durable-test")
                .expect("durable volume registry entry");
            volume.backing_volume_id = Some(backing_id.to_string());
            save_volume_metadata(&restarted.volumes_dir(), volume)
                .expect("persist durable backing identity");
        }

        tokio::time::timeout(
            Duration::from_secs(2),
            restarted.delete_durable_volume("thread:durable-test"),
        )
        .await
        .expect("backed durable volume deletion must not deadlock")
        .expect("delete unattached durable volume");
        assert!(restarted.list_durable_volumes().await.is_empty());
        assert!(!restarted.volume_disk_path(&created.volume_id).exists());
        assert!(!fork_root.exists());
    }

    #[tokio::test]
    async fn create_vm_capacity_limit_rejects_when_active_vm_limit_reached() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let data_dir = tmp.path().to_path_buf();
        fs::create_dir_all(data_dir.join(config::BASE_IMAGES_DIR_NAME))
            .expect("create base images");

        let cfg = Config {
            listen_address: "127.0.0.1:0".to_string(),
            data_dir: data_dir.to_string_lossy().to_string(),
            qemu_bin: "qemu-system-x86_64".to_string(),
            qemu_arm64_bin: "qemu-system-aarch64".to_string(),
            qemu_img_bin: "qemu-img".to_string(),
            virtiofsd_bin: "/usr/lib/qemu/virtiofsd".to_string(),
            docker_bin: "docker".to_string(),
            log_level: "info".to_string(),
            force_local_build: false,
            max_active_vms: Some(1),
            max_fork_chain_depth: 32,
            fork_compaction_depth_threshold: 8,
            storage_profile: config::StorageProfile::LocalEphemeral,
            shared_mount_profiles: vec!["local-path".to_string()],
            ha_mode: false,
            node_registry: None,
            control_bus: None,
            vfs_internal_service_token: None,
            pci_policy_path: None,
            pci: Default::default(),
            qemu_process: Default::default(),
            guest_network: Default::default(),
            network_services: Default::default(),
            security: Default::default(),
            snapshot_staging_dir: None,
        };

        let manager = Manager {
            cfg,
            host_arch: ARCH_AMD64.to_string(),
            resource_bounds: VmResourceBounds::default(),
            base_image_locks: Mutex::new(HashMap::new()),
            vms: RwLock::new(HashMap::new()),
            vm_capacity_lock: Mutex::new(()),
            session_registry: RwLock::new(HashMap::new()),
            volumes: RwLock::new(HashMap::new()),
            volume_operation_locks: Mutex::new(HashMap::new()),
            pci_leases: Mutex::new(HashMap::new()),
            snapshots: RwLock::new(HashMap::new()),
        };

        let vm_id = "busy-vm".to_string();
        let vm_dir = data_dir.join(&vm_id);
        fs::create_dir_all(&vm_dir).expect("create vm dir");

        let metadata = VmMetadata {
            id: vm_id.clone(),
            name: "busy".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: VmState::Running,
            architecture: ARCH_AMD64.to_string(),
            source: VmSource {
                source_type: VmSourceType::Docker,
                reference: "mock/image:latest".to_string(),
            },
            resources: crate::state::ResourceSpec {
                vcpu: 1,
                memory_mb: 512,
                disk_gb: 10,
            },
            network: NetworkSpec {
                mac: "02:00:00:00:00:02".to_string(),
                proxy_port: 0,
                rpc_port: 0,
            },
            metadata: HashMap::new(),
            snapshots: Vec::new(),
            shared_mounts: Vec::new(),
            pci_devices: Vec::new(),
            durable_volume: None,
            boot_incoming_ram_path: String::new(),
            started_at: None,
        };
        manager.vms.write().await.insert(
            vm_id,
            Arc::new(Vm::new(metadata, VmRuntime::new(&vm_dir), vm_dir.clone())),
        );

        let err = manager
            .enforce_create_vm_capacity()
            .await
            .expect_err("capacity check should reject");
        match err {
            ManagerError::CapacityExceeded {
                resource,
                limit,
                current,
            } => {
                assert_eq!(resource, "active_vms");
                assert_eq!(limit, 1);
                assert_eq!(current, 1);
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[tokio::test]
    async fn concurrent_capacity_insertions_cannot_oversubscribe_limit() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let manager = Arc::new(
            Manager::new(Config {
                listen_address: "127.0.0.1:0".to_string(),
                data_dir: tmp.path().join("vmd").to_string_lossy().to_string(),
                max_active_vms: Some(1),
                ..Config::default()
            })
            .await
            .expect("create manager"),
        );

        let candidate = |id: &str| {
            let dir = tmp.path().join("vmd").join(id);
            fs::create_dir_all(&dir).expect("create candidate VM dir");
            let mut metadata = qemu_test_metadata(id);
            metadata.state = VmState::Creating;
            Arc::new(Vm::new(metadata, VmRuntime::new(&dir), dir))
        };
        let first_manager = Arc::clone(&manager);
        let second_manager = Arc::clone(&manager);
        let (first, second) = tokio::join!(
            first_manager.insert_creating_vm_with_capacity(
                "capacity-first".to_string(),
                candidate("capacity-first"),
            ),
            second_manager.insert_creating_vm_with_capacity(
                "capacity-second".to_string(),
                candidate("capacity-second"),
            ),
        );

        assert_eq!(
            usize::from(first.is_ok()) + usize::from(second.is_ok()),
            1,
            "exactly one concurrent insertion should reserve the only slot"
        );
        assert_eq!(manager.vms.read().await.len(), 1);
        let rejected = if let Err(error) = first {
            error
        } else {
            second.expect_err("second insertion should be the rejected candidate")
        };
        assert!(matches!(
            rejected,
            ManagerError::CapacityExceeded {
                resource: "active_vms",
                limit: 1,
                current: 1,
            }
        ));
    }

    #[tokio::test]
    async fn fork_vm_rejects_when_chain_depth_limit_exceeded() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let data_dir = tmp.path().to_path_buf();
        fs::create_dir_all(data_dir.join(config::BASE_IMAGES_DIR_NAME))
            .expect("create base images");

        let cfg = Config {
            listen_address: "127.0.0.1:0".to_string(),
            data_dir: data_dir.to_string_lossy().to_string(),
            qemu_bin: "qemu-system-x86_64".to_string(),
            qemu_arm64_bin: "qemu-system-aarch64".to_string(),
            qemu_img_bin: "qemu-img".to_string(),
            virtiofsd_bin: "/usr/lib/qemu/virtiofsd".to_string(),
            docker_bin: "docker".to_string(),
            log_level: "info".to_string(),
            force_local_build: false,
            max_active_vms: None,
            max_fork_chain_depth: 1,
            fork_compaction_depth_threshold: 1,
            storage_profile: config::StorageProfile::LocalEphemeral,
            shared_mount_profiles: vec!["local-path".to_string()],
            ha_mode: false,
            node_registry: None,
            control_bus: None,
            vfs_internal_service_token: None,
            pci_policy_path: None,
            pci: Default::default(),
            qemu_process: Default::default(),
            guest_network: Default::default(),
            network_services: Default::default(),
            security: Default::default(),
            snapshot_staging_dir: None,
        };

        let manager = Manager {
            cfg,
            host_arch: ARCH_AMD64.to_string(),
            resource_bounds: VmResourceBounds::default(),
            base_image_locks: Mutex::new(HashMap::new()),
            vms: RwLock::new(HashMap::new()),
            vm_capacity_lock: Mutex::new(()),
            session_registry: RwLock::new(HashMap::new()),
            volumes: RwLock::new(HashMap::new()),
            volume_operation_locks: Mutex::new(HashMap::new()),
            pci_leases: Mutex::new(HashMap::new()),
            snapshots: RwLock::new(HashMap::new()),
        };

        let parent_id = "parent-depth-limit".to_string();
        let parent_dir = data_dir.join(&parent_id);
        fs::create_dir_all(&parent_dir).expect("create parent dir");

        let mut metadata = VmMetadata {
            id: parent_id.clone(),
            name: "parent".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: VmState::Stopped,
            architecture: ARCH_AMD64.to_string(),
            source: VmSource {
                source_type: VmSourceType::Docker,
                reference: "mock/image:latest".to_string(),
            },
            resources: crate::state::ResourceSpec {
                vcpu: 1,
                memory_mb: 512,
                disk_gb: 10,
            },
            network: NetworkSpec {
                mac: "02:00:00:00:00:03".to_string(),
                proxy_port: 0,
                rpc_port: 0,
            },
            metadata: HashMap::from([(META_FORK_DEPTH.to_string(), "1".to_string())]),
            snapshots: Vec::new(),
            shared_mounts: Vec::new(),
            pci_devices: Vec::new(),
            durable_volume: None,
            boot_incoming_ram_path: String::new(),
            started_at: None,
        };
        save_metadata(&parent_dir, &mut metadata).expect("save parent metadata");
        manager.vms.write().await.insert(
            parent_id.clone(),
            Arc::new(Vm::new(
                metadata,
                VmRuntime::new(&parent_dir),
                parent_dir.clone(),
            )),
        );

        let err = manager
            .fork_vm(
                &parent_id,
                ForkVmParams {
                    child_name: Some("child".to_string()),
                    child_metadata: HashMap::new(),
                    auto_start_child: false,
                },
            )
            .await
            .expect_err("fork should reject once fork depth exceeds configured limit");
        match err {
            ManagerError::CapacityExceeded {
                resource,
                limit,
                current,
            } => {
                assert_eq!(resource, "fork_chain_depth");
                assert_eq!(limit, 1);
                assert_eq!(current, 2);
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[tokio::test]
    async fn create_snapshot_rejects_stopped_vm() {
        // @dive: Strict live semantics — snapshots are a live-VM operation. Stopped VMs
        //        have no RAM state to preserve, so create_snapshot must reject rather
        //        than silently produce a disk-only record.
        let tmp = tempfile::tempdir().expect("create tempdir");
        let data_dir = tmp.path().to_path_buf();
        fs::create_dir_all(data_dir.join(config::BASE_IMAGES_DIR_NAME))
            .expect("create base images");

        let cfg = Config {
            listen_address: "127.0.0.1:0".to_string(),
            data_dir: data_dir.to_string_lossy().to_string(),
            qemu_bin: "qemu-system-x86_64".to_string(),
            qemu_arm64_bin: "qemu-system-aarch64".to_string(),
            qemu_img_bin: "qemu-img".to_string(),
            virtiofsd_bin: "/usr/lib/qemu/virtiofsd".to_string(),
            docker_bin: "docker".to_string(),
            log_level: "info".to_string(),
            force_local_build: false,
            max_active_vms: None,
            max_fork_chain_depth: 8,
            fork_compaction_depth_threshold: 8,
            storage_profile: config::StorageProfile::LocalEphemeral,
            shared_mount_profiles: vec!["local-path".to_string()],
            ha_mode: false,
            node_registry: None,
            control_bus: None,
            vfs_internal_service_token: None,
            pci_policy_path: None,
            pci: Default::default(),
            qemu_process: Default::default(),
            guest_network: Default::default(),
            network_services: Default::default(),
            security: Default::default(),
            snapshot_staging_dir: None,
        };

        let manager = Manager {
            cfg,
            host_arch: ARCH_AMD64.to_string(),
            resource_bounds: VmResourceBounds::default(),
            base_image_locks: Mutex::new(HashMap::new()),
            vms: RwLock::new(HashMap::new()),
            vm_capacity_lock: Mutex::new(()),
            session_registry: RwLock::new(HashMap::new()),
            volumes: RwLock::new(HashMap::new()),
            volume_operation_locks: Mutex::new(HashMap::new()),
            pci_leases: Mutex::new(HashMap::new()),
            snapshots: RwLock::new(HashMap::new()),
        };

        let vm_id = "vm-stopped".to_string();
        let vm_dir = data_dir.join(&vm_id);
        fs::create_dir_all(&vm_dir).expect("create vm dir");

        let mut metadata = VmMetadata {
            id: vm_id.clone(),
            name: "vm-stopped".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: VmState::Stopped,
            architecture: ARCH_AMD64.to_string(),
            source: VmSource {
                source_type: VmSourceType::Docker,
                reference: "mock/image:latest".to_string(),
            },
            resources: crate::state::ResourceSpec {
                vcpu: 1,
                memory_mb: 512,
                disk_gb: 10,
            },
            network: NetworkSpec {
                mac: "02:00:00:00:00:10".to_string(),
                proxy_port: 0,
                rpc_port: 0,
            },
            metadata: HashMap::new(),
            snapshots: Vec::new(),
            shared_mounts: Vec::new(),
            pci_devices: Vec::new(),
            durable_volume: None,
            boot_incoming_ram_path: String::new(),
            started_at: None,
        };
        save_metadata(&vm_dir, &mut metadata).expect("save metadata");
        manager.vms.write().await.insert(
            vm_id.clone(),
            Arc::new(Vm::new(metadata, VmRuntime::new(&vm_dir), vm_dir.clone())),
        );

        let err = manager
            .create_snapshot(
                &vm_id,
                SnapshotParams {
                    label: "attempt".to_string(),
                    description: "should be rejected".to_string(),
                },
            )
            .await
            .expect_err("create_snapshot on Stopped must fail with InvalidState");
        assert!(
            matches!(err, ManagerError::InvalidState),
            "expected InvalidState, got {err:?}"
        );
    }

    #[tokio::test]
    async fn update_vm_preserves_runtime_managed_proxy_upstream_metadata() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let data_dir = tmp.path().to_path_buf();
        fs::create_dir_all(data_dir.join(config::BASE_IMAGES_DIR_NAME))
            .expect("create base images");

        let cfg = Config {
            listen_address: "127.0.0.1:0".to_string(),
            data_dir: data_dir.to_string_lossy().to_string(),
            qemu_bin: "qemu-system-x86_64".to_string(),
            qemu_arm64_bin: "qemu-system-aarch64".to_string(),
            qemu_img_bin: "qemu-img".to_string(),
            virtiofsd_bin: "/usr/lib/qemu/virtiofsd".to_string(),
            docker_bin: "docker".to_string(),
            log_level: "info".to_string(),
            force_local_build: false,
            max_active_vms: None,
            max_fork_chain_depth: 8,
            fork_compaction_depth_threshold: 8,
            storage_profile: config::StorageProfile::LocalEphemeral,
            shared_mount_profiles: vec!["local-path".to_string()],
            ha_mode: false,
            node_registry: None,
            control_bus: None,
            vfs_internal_service_token: None,
            pci_policy_path: None,
            pci: Default::default(),
            qemu_process: Default::default(),
            guest_network: Default::default(),
            network_services: Default::default(),
            security: Default::default(),
            snapshot_staging_dir: None,
        };

        let manager = Manager {
            cfg,
            host_arch: ARCH_AMD64.to_string(),
            resource_bounds: VmResourceBounds::default(),
            base_image_locks: Mutex::new(HashMap::new()),
            vms: RwLock::new(HashMap::new()),
            vm_capacity_lock: Mutex::new(()),
            session_registry: RwLock::new(HashMap::new()),
            volumes: RwLock::new(HashMap::new()),
            volume_operation_locks: Mutex::new(HashMap::new()),
            pci_leases: Mutex::new(HashMap::new()),
            snapshots: RwLock::new(HashMap::new()),
        };

        let vm_id = "vm-update-policy".to_string();
        let vm_dir = data_dir.join(&vm_id);
        fs::create_dir_all(&vm_dir).expect("create vm dir");

        let mut metadata = VmMetadata {
            id: vm_id.clone(),
            name: "vm-update-policy".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: VmState::Stopped,
            architecture: ARCH_AMD64.to_string(),
            source: VmSource {
                source_type: VmSourceType::Docker,
                reference: "mock/image:latest".to_string(),
            },
            resources: crate::state::ResourceSpec {
                vcpu: 1,
                memory_mb: 512,
                disk_gb: 10,
            },
            network: NetworkSpec {
                mac: "02:00:00:00:00:14".to_string(),
                proxy_port: 0,
                rpc_port: 0,
            },
            metadata: HashMap::from([
                (
                    META_SESSION_ID.to_string(),
                    "runtime-session-existing".to_string(),
                ),
                (META_TENANT_ID.to_string(), "tenant-existing".to_string()),
                (
                    META_WORKSPACE_ID.to_string(),
                    "workspace-existing".to_string(),
                ),
                (
                    META_TIER_B_ELIGIBLE.to_string(),
                    "true".to_string(),
                ),
                (
                    META_NETWORK_POLICY.to_string(),
                    "{\"domain_allowlist\":[\"github.com\"],\"domain_blocklist\":[],\"custom_port_allowlist\":[],\"bandwidth_cap_mb_per_hour\":1024,\"max_connections_per_minute\":1000}".to_string(),
                ),
                (
                    META_NETWORK_POLICY_PROXY_UPSTREAM.to_string(),
                    "127.0.0.1:43128".to_string(),
                ),
                (
                    META_PORTPROXY_AUTH_TOKEN.to_string(),
                    "existing-portproxy-token".to_string(),
                ),
            ]),
            snapshots: Vec::new(),
            shared_mounts: Vec::new(),
            pci_devices: Vec::new(),
            durable_volume: None,
            boot_incoming_ram_path: String::new(),
            started_at: None,
        };
        save_metadata(&vm_dir, &mut metadata).expect("save metadata");
        manager.vms.write().await.insert(
            vm_id.clone(),
            Arc::new(Vm::new(metadata, VmRuntime::new(&vm_dir), vm_dir.clone())),
        );

        let updated = manager
            .update_vm(
                &vm_id,
                UpdateVmParams {
                    name: None,
                    metadata: Some(HashMap::from([(
                        META_NETWORK_POLICY.to_string(),
                        "{\"domain_allowlist\":[\"api.openai.com\"],\"domain_blocklist\":[],\"custom_port_allowlist\":[8080],\"bandwidth_cap_mb_per_hour\":512,\"max_connections_per_minute\":100}".to_string(),
                    ), (
                        META_PORTPROXY_AUTH_TOKEN.to_string(),
                        "caller-supplied-token".to_string(),
                    )])),
                },
            )
            .await
            .expect("update vm metadata");

        assert_eq!(
            updated
                .metadata
                .get(META_NETWORK_POLICY_PROXY_UPSTREAM)
                .map(String::as_str),
            Some("127.0.0.1:43128")
        );
        assert_eq!(
            updated
                .metadata
                .get(META_PORTPROXY_AUTH_TOKEN)
                .map(String::as_str),
            Some("existing-portproxy-token")
        );
        assert_eq!(
            updated.metadata.get(META_SESSION_ID).map(String::as_str),
            Some("runtime-session-existing")
        );
        assert_eq!(
            updated.metadata.get(META_TENANT_ID).map(String::as_str),
            Some("tenant-existing")
        );
        assert_eq!(
            updated.metadata.get(META_WORKSPACE_ID).map(String::as_str),
            Some("workspace-existing")
        );
        assert_eq!(
            updated
                .metadata
                .get(META_TIER_B_ELIGIBLE)
                .map(String::as_str),
            Some("true")
        );
        assert!(
            updated
                .metadata
                .get(META_NETWORK_POLICY)
                .is_some_and(|value| value.contains("api.openai.com"))
        );
    }

    #[test]
    fn build_qemu_args_only_emits_vfio_for_explicit_assignments() {
        let mut meta = qemu_test_metadata("vm-pci-args");
        let baseline = build_qemu_args(
            &meta,
            Path::new("/tmp/vm-pci-args"),
            Path::new("/tmp/vm-pci-args/qmp.sock"),
            Path::new("/tmp/vm-pci-args/qemu.pid"),
            ARCH_AMD64,
            Some(&test_tap_spec(&meta.id)),
            &[],
        )
        .expect("build baseline qemu args");
        assert!(!baseline.iter().any(|arg| arg.contains("vfio-pci")));

        meta.pci_devices.push(PciDeviceAssignmentSpec {
            id: "rtx6000-ada".to_string(),
            bdfs: vec!["0000:09:00.0".to_string(), "0000:09:00.1".to_string()],
            iommu_groups: vec!["21".to_string()],
            original_drivers: HashMap::new(),
        });
        let assigned = build_qemu_args(
            &meta,
            Path::new("/tmp/vm-pci-args"),
            Path::new("/tmp/vm-pci-args/qmp.sock"),
            Path::new("/tmp/vm-pci-args/qemu.pid"),
            ARCH_AMD64,
            Some(&test_tap_spec(&meta.id)),
            &[],
        )
        .expect("build assigned qemu args");
        assert!(
            assigned
                .iter()
                .any(|arg| { arg == "vfio-pci,host=0000:09:00.0,id=pci_rtx6000_ada_0" })
        );
        assert!(
            assigned
                .iter()
                .any(|arg| { arg == "vfio-pci,host=0000:09:00.1,id=pci_rtx6000_ada_1" })
        );
    }

    #[test]
    fn build_qemu_args_falls_back_to_virtfs_when_virtiofsd_unavailable() {
        // @dive: Dev hosts (notably macOS) cannot run virtiofsd, so vmd passes an empty
        //        handle slice and the builder must fall back to legacy `-virtfs`
        //        shared-mount emission. Also: no memfd backend on this path (the qemu
        //        machine line stays clean) because virtio-9p doesn't need shared memory.
        let meta = VmMetadata {
            id: "vm-9p-fallback".to_string(),
            name: "dev9p".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: VmState::Stopped,
            architecture: ARCH_AMD64.to_string(),
            source: VmSource {
                source_type: VmSourceType::Docker,
                reference: "mock/image:latest".to_string(),
            },
            resources: crate::state::ResourceSpec {
                vcpu: 1,
                memory_mb: 512,
                disk_gb: 10,
            },
            network: NetworkSpec {
                mac: "02:00:00:00:00:12".to_string(),
                proxy_port: 3000,
                rpc_port: 3001,
            },
            metadata: HashMap::new(),
            snapshots: Vec::new(),
            shared_mounts: vec![SharedMountSpec {
                host_path: "/tmp/runtimefs".to_string(),
                guest_path: "/workspace".to_string(),
                mount_tag: "runtimefs".to_string(),
                read_only: true,
                availability: SharedMountAvailability::SharedStorage,
                continuity: SharedMountContinuity::RestoreCrossNode,
                backend_profile: "shared-posix".to_string(),
                vfs_endpoint: String::new(),
                vfs_scope_path: String::new(),
            }],
            pci_devices: Vec::new(),
            durable_volume: None,
            boot_incoming_ram_path: String::new(),
            started_at: None,
        };
        let args = build_qemu_args(
            &meta,
            Path::new("/tmp/vm-9p"),
            Path::new("/tmp/vm-9p/qmp.sock"),
            Path::new("/tmp/vm-9p/qemu.pid"),
            ARCH_AMD64,
            Some(&test_tap_spec(&meta.id)),
            &[],
        )
        .expect("build qemu args");

        // No memfd backend on the fallback path — virtio-9p doesn't need shared memory.
        assert!(
            !args
                .iter()
                .any(|arg| arg.starts_with("memory-backend-memfd")),
            "fallback path must not emit memfd memory backend"
        );
        assert!(
            !args.iter().any(|arg| arg.contains("memory-backend=mem")),
            "fallback path must not reference the memfd backend on the machine line"
        );

        // Legacy -virtfs device with the same mount tag and readonly flag.
        assert!(args.iter().any(|arg| arg == "-virtfs"));
        assert!(args.iter().any(|arg| {
            arg.contains("path=/tmp/runtimefs")
                && arg.contains("mount_tag=runtimefs")
                && arg.contains("readonly=on")
        }));
    }

    #[test]
    fn build_qemu_args_emits_incoming_when_boot_incoming_ram_path_set() {
        // @dive: Verifies the one-shot incoming restore path. When
        //        boot_incoming_ram_path is populated, qemu must start with
        //        `-incoming defer` so vmd can configure mapped-ram/direct-io
        //        over QMP before issuing `migrate-incoming file:<path>`.
        let meta = VmMetadata {
            id: "vm-incoming".to_string(),
            name: "incoming".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: VmState::Stopped,
            architecture: ARCH_AMD64.to_string(),
            source: VmSource {
                source_type: VmSourceType::Docker,
                reference: "mock/image:latest".to_string(),
            },
            resources: crate::state::ResourceSpec {
                vcpu: 1,
                memory_mb: 512,
                disk_gb: 10,
            },
            network: NetworkSpec {
                mac: "02:00:00:00:00:11".to_string(),
                proxy_port: 3000,
                rpc_port: 3001,
            },
            metadata: HashMap::new(),
            snapshots: Vec::new(),
            shared_mounts: Vec::new(),
            pci_devices: Vec::new(),
            durable_volume: None,
            boot_incoming_ram_path: "/srv/chevalier/vms/vm-incoming/snapshots/snap-abc.ram"
                .to_string(),
            started_at: None,
        };
        let args = build_qemu_args(
            &meta,
            Path::new("/tmp/vm-incoming"),
            Path::new("/tmp/vm-incoming/qmp.sock"),
            Path::new("/tmp/vm-incoming/qemu.pid"),
            ARCH_AMD64,
            Some(&test_tap_spec(&meta.id)),
            &[],
        )
        .expect("build qemu args");

        // `-incoming` must be present and deferred until QMP config is applied.
        let incoming_idx = args
            .iter()
            .position(|a| a == "-incoming")
            .expect("`-incoming` flag missing from qemu args");
        let next = args
            .get(incoming_idx + 1)
            .expect("`-incoming` missing its value arg");
        assert_eq!(next, "defer", "unexpected -incoming value");
    }

    #[test]
    fn only_mapped_ram_snapshots_are_restore_compatible() {
        let mut snapshot = new_snapshot_metadata("test".to_string(), String::new());

        snapshot.ram_format = "legacy".to_string();
        assert!(!snapshot_ram_format_is_supported(&snapshot));

        snapshot.ram_format = String::new();
        assert!(!snapshot_ram_format_is_supported(&snapshot));

        snapshot.ram_format = virt::RAM_SNAPSHOT_FORMAT_MAPPED.to_string();
        assert!(snapshot_ram_format_is_supported(&snapshot));
    }

    #[test]
    fn build_qemu_args_disables_amd64_vapic_for_ram_restore_compatibility() {
        let meta = VmMetadata {
            id: "vm-vapic-off".to_string(),
            name: "vapic-off".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: VmState::Stopped,
            architecture: ARCH_AMD64.to_string(),
            source: VmSource {
                source_type: VmSourceType::Docker,
                reference: "mock/image:latest".to_string(),
            },
            resources: crate::state::ResourceSpec {
                vcpu: 1,
                memory_mb: 512,
                disk_gb: 10,
            },
            network: NetworkSpec {
                mac: "02:00:00:00:00:12".to_string(),
                proxy_port: 3000,
                rpc_port: 3001,
            },
            metadata: HashMap::new(),
            snapshots: Vec::new(),
            shared_mounts: Vec::new(),
            pci_devices: Vec::new(),
            durable_volume: None,
            boot_incoming_ram_path: String::new(),
            started_at: None,
        };
        let args = build_qemu_args(
            &meta,
            Path::new("/tmp/vm-vapic-off"),
            Path::new("/tmp/vm-vapic-off/qmp.sock"),
            Path::new("/tmp/vm-vapic-off/qemu.pid"),
            ARCH_AMD64,
            Some(&test_tap_spec(&meta.id)),
            &[],
        )
        .expect("build qemu args");

        let global_idx = args
            .iter()
            .position(|arg| arg == "-global")
            .expect("missing -global");
        assert_eq!(
            args.get(global_idx + 1).map(String::as_str),
            Some("apic.vapic=off")
        );
    }

    #[test]
    fn amd64_runtime_fingerprint_includes_qemu_migration_shape() {
        let fingerprint =
            current_guest_runtime_fingerprint(ARCH_AMD64).expect("fingerprint should build");
        assert!(
            fingerprint.contains(AMD64_QEMU_RUNTIME_FINGERPRINT),
            "amd64 RAM snapshots must be invalidated when migration-sensitive QEMU args change"
        );
    }

    #[test]
    fn build_qemu_args_emits_tap_networking() {
        let meta = VmMetadata {
            id: "vm-guest-network".to_string(),
            name: "guest-network".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: VmState::Stopped,
            architecture: ARCH_AMD64.to_string(),
            source: VmSource {
                source_type: VmSourceType::Docker,
                reference: "mock/image:latest".to_string(),
            },
            resources: crate::state::ResourceSpec {
                vcpu: 1,
                memory_mb: 512,
                disk_gb: 10,
            },
            network: NetworkSpec {
                mac: "02:00:00:00:00:10".to_string(),
                proxy_port: 3000,
                rpc_port: 3001,
            },
            metadata: HashMap::from([(
                META_PORTPROXY_AUTH_TOKEN.to_string(),
                "runtime-token".to_string(),
            )]),
            snapshots: Vec::new(),
            shared_mounts: Vec::new(),
            pci_devices: Vec::new(),
            durable_volume: None,
            boot_incoming_ram_path: String::new(),
            started_at: None,
        };
        let tap_spec = test_tap_spec(&meta.id);

        let args = build_qemu_args(
            &meta,
            Path::new("/tmp/vm-guest-network"),
            Path::new("/tmp/vm-guest-network/qmp.sock"),
            Path::new("/tmp/vm-guest-network/qemu.pid"),
            ARCH_AMD64,
            Some(&tap_spec),
            &[],
        )
        .expect("build qemu args");

        let netdev_idx = args
            .iter()
            .position(|arg| arg == "-netdev")
            .expect("missing -netdev arg");
        let netdev = args
            .get(netdev_idx + 1)
            .expect("missing tap netdev value after -netdev");
        assert!(netdev.starts_with("tap,id=net0"));
        assert!(netdev.contains(format!("ifname={}", tap_spec.tap_name).as_str()));
        assert!(netdev.contains("script=no"));
        assert!(!netdev.contains("guestfwd="));
        assert!(!netdev.contains("hostfwd="));
        let fw_cfg = args
            .windows(2)
            .filter(|pair| pair[0] == "-fw_cfg")
            .map(|pair| pair[1].as_str())
            .collect::<Vec<_>>();
        assert_eq!(fw_cfg.len(), 6);
        assert!(fw_cfg.contains(&"name=opt/chevalier/tap-mac,string=02:00:00:00:00:10"));
        assert!(
            fw_cfg
                .iter()
                .any(|arg| arg.starts_with("name=opt/chevalier/tap-address-cidr,string="))
        );
        assert!(fw_cfg.contains(&"name=opt/chevalier/hostname,string=guest-network"));
        assert!(fw_cfg.contains(&"name=opt/chevalier/portproxy-auth-token,string=runtime-token"));
        let smbios = args
            .windows(2)
            .filter(|pair| pair[0] == "-smbios")
            .map(|pair| pair[1].as_str())
            .collect::<Vec<_>>();
        assert_eq!(smbios.len(), 6);
        assert!(smbios.contains(&"type=11,value=chevalier.tap-mac=02:00:00:00:00:10"));
        assert!(smbios.contains(&"type=11,value=chevalier.hostname=guest-network"));
        assert!(smbios.contains(&"type=11,value=chevalier.portproxy-auth-token=runtime-token"));
    }

    #[test]
    fn build_qemu_args_emits_qemu_user_networking_without_managed_tap() {
        let meta = VmMetadata {
            id: "vm-guest-network-override".to_string(),
            name: "guest-network-override".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: VmState::Stopped,
            architecture: ARCH_AMD64.to_string(),
            source: VmSource {
                source_type: VmSourceType::Docker,
                reference: "mock/image:latest".to_string(),
            },
            resources: crate::state::ResourceSpec {
                vcpu: 1,
                memory_mb: 512,
                disk_gb: 10,
            },
            network: NetworkSpec {
                mac: "02:00:00:00:00:13".to_string(),
                proxy_port: 3000,
                rpc_port: 3001,
            },
            metadata: HashMap::new(),
            snapshots: Vec::new(),
            shared_mounts: Vec::new(),
            pci_devices: Vec::new(),
            durable_volume: None,
            boot_incoming_ram_path: String::new(),
            started_at: None,
        };
        let args = build_qemu_args(
            &meta,
            Path::new("/tmp/vm-guest-network-override"),
            Path::new("/tmp/vm-guest-network-override/qmp.sock"),
            Path::new("/tmp/vm-guest-network-override/qemu.pid"),
            ARCH_AMD64,
            None,
            &[],
        )
        .expect("build qemu args");
        assert!(!args.iter().any(|arg| arg == "-fw_cfg"));
        assert!(!args.iter().any(|arg| arg == "-smbios"));

        let netdev_idx = args
            .iter()
            .position(|arg| arg == "-netdev")
            .expect("missing -netdev arg");
        let netdev = args
            .get(netdev_idx + 1)
            .expect("missing user netdev value after -netdev");
        assert!(netdev.starts_with("user,id=net0"));
        assert!(netdev.contains("hostfwd=tcp:127.0.0.1:3000-:13337"));
        assert!(netdev.contains("hostfwd=tcp:127.0.0.1:3001-:13338"));
        assert!(!netdev.contains("guestfwd="));
        assert!(!netdev.starts_with("tap,"));
    }

    #[test]
    fn new_snapshot_metadata_derives_ram_file_name_from_snapshot_name() {
        // @dive: Contract between `new_snapshot_metadata` and `create_snapshot`'s
        //        `<vm_dir>/snapshots/<ram_file_name>` layout. If this ever drifts,
        //        restore/delete will look at the wrong filename.
        let meta = new_snapshot_metadata("label".to_string(), "desc".to_string());
        assert!(meta.name.starts_with("snap-"));
        assert_eq!(meta.ram_file_name, format!("{}.ram", meta.name));
    }

    #[test]
    fn fork_snapshot_durability_tracks_profile_and_parent_state() {
        assert_eq!(
            fork_snapshot_durability(config::StorageProfile::LocalEphemeral, false),
            ("local-ephemeral-cow-overlay", "daemon-restart-same-node")
        );
        assert_eq!(
            fork_snapshot_durability(config::StorageProfile::LocalEphemeral, true),
            (
                "local-ephemeral-running-snapshot",
                "daemon-restart-same-node"
            )
        );
        assert_eq!(
            fork_snapshot_durability(config::StorageProfile::DurableShared, false),
            (
                "durable-shared-cow-overlay",
                "durable-storage-daemon-restart"
            )
        );
        assert_eq!(
            fork_snapshot_durability(config::StorageProfile::DurableShared, true),
            (
                "durable-shared-running-snapshot",
                "durable-storage-daemon-restart"
            )
        );
    }

    #[test]
    fn pid_command_match_requires_qemu_and_vm_markers() {
        let vm_dir = PathBuf::from("/tmp/chevalier/vm-123");
        let qmp_path = vm_dir.join("qmp.sock");

        assert!(pid_command_matches_vm(
            "qemu-system-aarch64 -qmp unix:/tmp/chevalier/vm-123/qmp.sock,server=on,wait=off",
            vm_dir.as_path(),
            qmp_path.as_path()
        ));
        assert!(pid_command_matches_vm(
            "qemu-system-aarch64 -drive file=/tmp/chevalier/vm-123/disk.qcow2,format=qcow2",
            vm_dir.as_path(),
            qmp_path.as_path()
        ));
        assert!(!pid_command_matches_vm(
            "qemu-system-aarch64 -drive file=/tmp/other/vm-999/disk.qcow2,format=qcow2",
            vm_dir.as_path(),
            qmp_path.as_path()
        ));
        assert!(!pid_command_matches_vm(
            "/usr/bin/some-other-process --flag /tmp/chevalier/vm-123",
            vm_dir.as_path(),
            qmp_path.as_path()
        ));
    }

    #[test]
    fn build_qemu_args_emits_shared_mounts_as_virtiofs() {
        // @dive: Shared mounts now flow through pre-spawned virtiofsd daemons and
        //        qemu consumes them via vhost-user-fs-pci. Verify the builder emits a
        //        matching -chardev + -device pair for each handle, plus the memfd
        //        memory backend that vhost-user needs.
        let meta = VmMetadata {
            id: "vm-shared-mounts".to_string(),
            name: "shared".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: VmState::Stopped,
            architecture: ARCH_AMD64.to_string(),
            source: VmSource {
                source_type: VmSourceType::Docker,
                reference: "mock/image:latest".to_string(),
            },
            resources: crate::state::ResourceSpec {
                vcpu: 1,
                memory_mb: 512,
                disk_gb: 10,
            },
            network: NetworkSpec {
                mac: "02:00:00:00:00:09".to_string(),
                proxy_port: 3000,
                rpc_port: 3001,
            },
            metadata: HashMap::new(),
            snapshots: Vec::new(),
            shared_mounts: vec![SharedMountSpec {
                host_path: "/tmp/runtimefs".to_string(),
                guest_path: "/workspace".to_string(),
                mount_tag: "runtimefs".to_string(),
                read_only: true,
                availability: SharedMountAvailability::SharedStorage,
                continuity: SharedMountContinuity::RestoreCrossNode,
                backend_profile: "shared-posix".to_string(),
                vfs_endpoint: String::new(),
                vfs_scope_path: String::new(),
            }],
            pci_devices: Vec::new(),
            durable_volume: None,
            boot_incoming_ram_path: String::new(),
            started_at: None,
        };
        let virtiofsd_handles = vec![virt::VirtiofsdHandle {
            pid: 0,
            socket_path: PathBuf::from("/tmp/vm-shared/virtiofsd-0.sock"),
            source_path: PathBuf::from("/tmp/runtimefs"),
            tag: "runtimefs".to_string(),
            read_only: true,
            terminate_tx: None,
        }];

        let args = build_qemu_args(
            &meta,
            Path::new("/tmp/vm-shared"),
            Path::new("/tmp/vm-shared/qmp.sock"),
            Path::new("/tmp/vm-shared/qemu.pid"),
            ARCH_AMD64,
            Some(&test_tap_spec(&meta.id)),
            &virtiofsd_handles,
        )
        .expect("build qemu args");

        // memfd memory backend with share=on — required for both vhost-user-fs and
        // `background-snapshot` migration.
        assert!(
            args.iter()
                .any(|arg| { arg.starts_with("memory-backend-memfd") && arg.contains("share=on") })
        );
        // Machine line threads the memfd backend into the machine definition.
        assert!(args.iter().any(|arg| arg.contains("memory-backend=mem")));

        // `-virtfs` must be gone — it would block qemu migration.
        assert!(
            !args.iter().any(|arg| arg == "-virtfs"),
            "build_qemu_args must not emit legacy -virtfs"
        );

        // `-chardev socket,id=vfsd0,path=...` pairing with a `-device
        // vhost-user-fs-pci ...,chardev=vfsd0,tag=runtimefs`.
        assert!(args.iter().any(|arg| {
            arg.starts_with("socket,id=vfsd0")
                && arg.contains("path=/tmp/vm-shared/virtiofsd-0.sock")
        }));
        assert!(args.iter().any(|arg| {
            arg.starts_with("vhost-user-fs-pci")
                && arg.contains("chardev=vfsd0")
                && arg.contains("tag=runtimefs")
        }));
    }

    #[test]
    fn normalize_shared_mounts_defaults_node_local_restart_same_node() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let host_path = tmp.path().join("shared");
        fs::create_dir_all(&host_path).expect("create shared dir");

        let normalized = normalize_shared_mounts(vec![SharedMountSpec {
            host_path: host_path.to_string_lossy().to_string(),
            guest_path: "/workspace".to_string(),
            mount_tag: "workspace".to_string(),
            read_only: false,
            availability: SharedMountAvailability::NodeLocal,
            continuity: SharedMountContinuity::RestartSameNode,
            backend_profile: String::new(),
            vfs_endpoint: String::new(),
            vfs_scope_path: String::new(),
        }])
        .expect("normalize shared mount");

        assert_eq!(normalized.len(), 1);
        assert_eq!(
            normalized[0].availability,
            SharedMountAvailability::NodeLocal
        );
        assert_eq!(
            normalized[0].continuity,
            SharedMountContinuity::RestartSameNode
        );
    }

    #[test]
    fn normalize_shared_mounts_rejects_cross_node_node_local_mounts() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let host_path = tmp.path().join("shared");
        fs::create_dir_all(&host_path).expect("create shared dir");

        let err = normalize_shared_mounts(vec![SharedMountSpec {
            host_path: host_path.to_string_lossy().to_string(),
            guest_path: "/workspace".to_string(),
            mount_tag: "workspace".to_string(),
            read_only: false,
            availability: SharedMountAvailability::NodeLocal,
            continuity: SharedMountContinuity::RestoreCrossNode,
            backend_profile: String::new(),
            vfs_endpoint: String::new(),
            vfs_scope_path: String::new(),
        }])
        .expect_err("node-local mount should reject cross-node continuity");

        assert!(format!("{err}").contains("node-local shared mounts"));
    }

    #[test]
    fn normalize_shared_mounts_rejects_shared_storage_without_backend_profile() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let host_path = tmp.path().join("shared");
        fs::create_dir_all(&host_path).expect("create shared dir");

        let err = normalize_shared_mounts(vec![SharedMountSpec {
            host_path: host_path.to_string_lossy().to_string(),
            guest_path: "/workspace".to_string(),
            mount_tag: "workspace".to_string(),
            read_only: false,
            availability: SharedMountAvailability::SharedStorage,
            continuity: SharedMountContinuity::RestoreCrossNode,
            backend_profile: String::new(),
            vfs_endpoint: String::new(),
            vfs_scope_path: String::new(),
        }])
        .expect_err("shared-storage mount should require backend_profile");

        assert!(format!("{err}").contains("backend_profile"));
    }

    #[test]
    fn normalize_shared_mounts_allows_fuse_backed_mounts_without_host_path() {
        let normalized = normalize_shared_mounts(vec![SharedMountSpec {
            host_path: String::new(),
            guest_path: "/workspace".to_string(),
            mount_tag: "runtimefs".to_string(),
            read_only: true,
            availability: SharedMountAvailability::SharedStorage,
            continuity: SharedMountContinuity::RestoreCrossNode,
            backend_profile: "gcs-vfs-fuse".to_string(),
            vfs_endpoint:
                " http://runtime-api.chevalier-vm.svc.cluster.local:3001/v1/internal/runtimefs/123 "
                    .to_string(),
            vfs_scope_path: " conversations/example/shared ".to_string(),
        }])
        .expect("normalize fuse-backed mount");

        assert_eq!(normalized.len(), 1);
        assert!(normalized[0].host_path.is_empty());
        assert_eq!(
            normalized[0].vfs_endpoint,
            "http://runtime-api.chevalier-vm.svc.cluster.local:3001/v1/internal/runtimefs/123"
        );
        assert_eq!(normalized[0].vfs_scope_path, "conversations/example/shared");
        assert!(normalized[0].is_fuse_backed());
    }

    #[tokio::test]
    async fn resolve_docker_platform_uses_requested_arch_without_docker_probe() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(data_dir.join(config::BASE_IMAGES_DIR_NAME))
            .expect("create base images");

        let fake_docker = tmp.path().join("docker");
        fs::write(
            &fake_docker,
            "#!/bin/sh\nprintf '%s\\n' \"$@\" >> \"$0.log\"\nexit 97\n",
        )
        .expect("write fake docker");
        let mut perms = fs::metadata(&fake_docker)
            .expect("stat fake docker")
            .permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            perms.set_mode(0o755);
        }
        fs::set_permissions(&fake_docker, perms).expect("chmod fake docker");

        let manager = Manager {
            cfg: Config {
                listen_address: "127.0.0.1:0".to_string(),
                data_dir: data_dir.to_string_lossy().to_string(),
                qemu_bin: "qemu-system-x86_64".to_string(),
                qemu_arm64_bin: "qemu-system-aarch64".to_string(),
                qemu_img_bin: "qemu-img".to_string(),
                virtiofsd_bin: "/usr/lib/qemu/virtiofsd".to_string(),
                docker_bin: fake_docker.to_string_lossy().to_string(),
                log_level: "info".to_string(),
                force_local_build: false,
                max_active_vms: None,
                max_fork_chain_depth: 8,
                fork_compaction_depth_threshold: 8,
                storage_profile: config::StorageProfile::LocalEphemeral,
                shared_mount_profiles: vec!["local-path".to_string()],
                ha_mode: false,
                node_registry: None,
                control_bus: None,
                vfs_internal_service_token: None,
                pci_policy_path: None,
                pci: Default::default(),
                qemu_process: Default::default(),
                guest_network: Default::default(),
                network_services: Default::default(),
                security: Default::default(),
                snapshot_staging_dir: None,
            },
            host_arch: ARCH_ARM64.to_string(),
            resource_bounds: VmResourceBounds::default(),
            base_image_locks: Mutex::new(HashMap::new()),
            vms: RwLock::new(HashMap::new()),
            vm_capacity_lock: Mutex::new(()),
            session_registry: RwLock::new(HashMap::new()),
            volumes: RwLock::new(HashMap::new()),
            volume_operation_locks: Mutex::new(HashMap::new()),
            pci_leases: Mutex::new(HashMap::new()),
            snapshots: RwLock::new(HashMap::new()),
        };

        let (arch, platform) = manager
            .resolve_docker_platform("example/image:latest", Some("arm64".to_string()))
            .await
            .expect("requested arch should bypass docker probing");

        assert_eq!(arch, "arm64");
        assert_eq!(platform, "linux/arm64");
        assert!(
            !fake_docker.with_extension("log").exists(),
            "explicit architecture should not invoke docker manifest/pull probes"
        );
    }

    #[test]
    fn vm_dir_removal_fails_closed_then_retries_after_mount_detaches() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let vm_dir = tmp.path().join("vm-id");
        let fuse_mount = vm_dir.join("fuse-mounts").join("workspace");
        let sentinel = vm_dir.join("metadata.json");
        fs::create_dir_all(&fuse_mount).expect("create fake mount path");
        fs::write(&sentinel, b"must survive a live mount fence").expect("write sentinel");

        let error =
            remove_vm_dir_with_mount_probe("vm-id", &vm_dir, |_| Ok(vec![fuse_mount.clone()]))
                .expect_err("live mount must fence recursive VM deletion");
        assert!(error.to_string().contains("mountpoints remain active"));
        assert!(sentinel.exists(), "fenced deletion must preserve VM data");
        assert!(
            vm_dir.exists(),
            "fenced deletion must preserve the VM record directory"
        );

        remove_vm_dir_with_mount_probe("vm-id", &vm_dir, |_| Ok(Vec::new()))
            .expect("retry after mount detaches");
        assert!(
            !vm_dir.exists(),
            "detached retry should remove the disposable VM directory"
        );
    }

    #[test]
    fn vm_dir_removal_preserves_data_when_mount_probe_fails() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let vm_dir = tmp.path().join("vm-id");
        let sentinel = vm_dir.join("metadata.json");
        fs::create_dir_all(&vm_dir).expect("create VM directory");
        fs::write(&sentinel, b"preserve").expect("write sentinel");

        let error = remove_vm_dir_with_mount_probe("vm-id", &vm_dir, |_| {
            Err(anyhow!("mountinfo unavailable"))
        })
        .expect_err("unknown mount state must fail closed");
        assert!(error.to_string().contains("mountinfo unavailable"));
        assert!(sentinel.exists(), "probe failure must preserve VM data");
    }

    #[cfg(unix)]
    #[test]
    fn should_skip_chown_path_matches_fuse_mount_subtree() {
        let vm_dir = PathBuf::from("/tmp/example-vm");
        let skipped = vec![vm_dir.join("fuse-mounts")];

        assert!(should_skip_chown_path(
            &vm_dir.join("fuse-mounts").join("runtimefs"),
            &skipped
        ));
        assert!(!should_skip_chown_path(
            &vm_dir.join("disk.qcow2"),
            &skipped
        ));
    }
}
