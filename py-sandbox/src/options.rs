use std::collections::HashMap;

use chevalier_sandbox::{
    DistributedControlConfig, ExecOptions, ForkOptions, OpenComputerBackendConfig,
    OpenComputerMountConfig, SandboxError, SharedMount, SharedMountAvailability,
    SharedMountContinuity, ShellOptions,
};
use serde::Deserialize;

#[derive(Default, Deserialize)]
pub struct ExecOpts {
    #[serde(default)]
    pub env: Option<HashMap<String, String>>,
    #[serde(default)]
    pub timeout_secs: Option<i32>,
    #[serde(default)]
    pub detach: Option<bool>,
    #[serde(default)]
    pub shell: Option<String>,
    #[serde(default)]
    pub close_stdin_on_start: Option<bool>,
}

impl From<ExecOpts> for ExecOptions {
    fn from(options: ExecOpts) -> Self {
        Self {
            env: options.env.unwrap_or_default(),
            timeout_secs: options.timeout_secs,
            detach: options.detach.unwrap_or(false),
            shell: options.shell,
            close_stdin_on_start: options.close_stdin_on_start.unwrap_or(false),
        }
    }
}

#[derive(Default, Deserialize)]
pub struct ShellOpts {
    #[serde(default)]
    pub shell: Option<String>,
    #[serde(default)]
    pub args: Option<Vec<String>>,
    #[serde(default)]
    pub env: Option<HashMap<String, String>>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub cols: Option<u32>,
    #[serde(default)]
    pub rows: Option<u32>,
}

impl From<ShellOpts> for ShellOptions {
    fn from(options: ShellOpts) -> Self {
        let dimension =
            |value: Option<u32>| value.map(|value| u16::try_from(value).unwrap_or(u16::MAX).max(1));
        Self {
            shell: options.shell,
            args: options.args.unwrap_or_default(),
            env: options.env.unwrap_or_default(),
            cwd: options.cwd,
            cols: dimension(options.cols),
            rows: dimension(options.rows),
        }
    }
}

#[derive(Deserialize)]
pub struct SharedMountOpts {
    #[serde(default)]
    host_path: Option<String>,
    guest_path: String,
    mount_tag: String,
    #[serde(default)]
    read_only: Option<bool>,
    #[serde(default)]
    availability: Option<String>,
    #[serde(default)]
    continuity: Option<String>,
    #[serde(default)]
    backend_profile: Option<String>,
    #[serde(default)]
    vfs_endpoint: Option<String>,
    #[serde(default)]
    vfs_scope_path: Option<String>,
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
    pub(crate) fn into_shared_mount(self) -> SharedMount {
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

#[derive(Default, Deserialize)]
pub struct SessionOpts {
    pub source_type: Option<SessionSourceType>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub image: Option<String>,
    #[serde(default)]
    pub architecture: Option<String>,
    #[serde(default)]
    pub metadata: Option<HashMap<String, String>>,
    #[serde(default)]
    pub auto_start: Option<bool>,
    #[serde(default)]
    pub shared_mounts: Option<Vec<SharedMountOpts>>,
    #[serde(default)]
    pub egress_allowlist: Option<Vec<String>>,
    #[serde(default)]
    pub pci_device_ids: Option<Vec<String>>,
    #[serde(default)]
    pub storage_profile: Option<String>,
    #[serde(default)]
    pub volume_owner_key: Option<String>,
    #[serde(default)]
    pub volume_size_gb: Option<u32>,
}

impl From<SessionOpts> for chevalier_sandbox::SessionOptions {
    fn from(options: SessionOpts) -> Self {
        Self {
            source_type: options.source_type.unwrap_or_default().into(),
            session_id: options.session_id,
            name: options.name,
            image: options.image,
            architecture: options.architecture,
            metadata: options.metadata.unwrap_or_default(),
            auto_start: options.auto_start.unwrap_or(true),
            shared_mounts: options
                .shared_mounts
                .unwrap_or_default()
                .into_iter()
                .map(SharedMountOpts::into_shared_mount)
                .collect(),
            egress_allowlist: options.egress_allowlist,
            pci_device_ids: options.pci_device_ids.unwrap_or_default(),
            storage_profile: options
                .storage_profile
                .unwrap_or_else(|| "local-ephemeral".to_string()),
            volume_owner_key: options.volume_owner_key,
            volume_size_gb: options
                .volume_size_gb
                .and_then(|value| i32::try_from(value).ok())
                .filter(|value| *value > 0),
            ..Default::default()
        }
    }
}

#[derive(Default, Deserialize)]
pub struct ForkOpts {
    #[serde(default)]
    pub child_name: Option<String>,
    #[serde(default)]
    pub child_metadata: Option<HashMap<String, String>>,
    #[serde(default)]
    pub auto_start_child: Option<bool>,
}

impl From<ForkOpts> for ForkOptions {
    fn from(options: ForkOpts) -> Self {
        Self {
            child_name: options.child_name,
            child_metadata: options.child_metadata.unwrap_or_default(),
            auto_start_child: options.auto_start_child.unwrap_or(true),
        }
    }
}

#[derive(Default, Deserialize)]
pub struct SessionSnapshotOpts {
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Default, Deserialize)]
pub struct SandboxConnectOptions {
    pub distributed_control: Option<DistributedControlOptions>,
    #[serde(default)]
    pub auth_token: Option<String>,
    #[serde(default)]
    pub pci_access_token: Option<String>,
    #[serde(default)]
    pub connect_timeout_ms: Option<f64>,
    #[serde(default)]
    pub default_image: Option<String>,
    #[serde(default)]
    pub default_architecture: Option<String>,
    #[serde(default)]
    pub default_vcpu: Option<u32>,
    #[serde(default)]
    pub default_memory_mb: Option<u32>,
    #[serde(default)]
    pub default_disk_gb: Option<u32>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub open_computer: Option<OpenComputerProviderOpts>,
}

#[derive(Default, Deserialize)]
pub struct OpenComputerProviderOpts {
    #[serde(default)]
    api_url: Option<String>,
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    template_id: Option<String>,
    #[serde(default)]
    timeout_secs: Option<f64>,
    #[serde(default)]
    default_cpu_count: Option<u32>,
    #[serde(default)]
    default_memory_mb: Option<u32>,
    #[serde(default)]
    default_disk_mb: Option<u32>,
    #[serde(default)]
    burst: Option<bool>,
    #[serde(default)]
    secret_store: Option<String>,
    #[serde(default)]
    egress_allowlist: Option<Vec<String>>,
    #[serde(default)]
    mounts: Option<Vec<OpenComputerMountOpts>>,
    #[serde(default)]
    shared_mounts: Option<HashMap<String, OpenComputerMountOpts>>,
}

#[derive(Default, Deserialize)]
struct OpenComputerMountOpts {
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    driver: Option<String>,
    #[serde(default)]
    remote: Option<String>,
    #[serde(default)]
    backend: Option<String>,
    #[serde(default)]
    command: Option<Vec<String>>,
    #[serde(default)]
    env: Option<HashMap<String, String>>,
    #[serde(default)]
    secrets: Option<HashMap<String, String>>,
    #[serde(default)]
    creds: Option<HashMap<String, String>>,
    #[serde(default)]
    rclone_config: Option<String>,
    #[serde(default)]
    read_only: Option<bool>,
    #[serde(default)]
    mount_options: Option<Vec<String>>,
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

pub fn opencomputer_config_from_options(
    options: Option<OpenComputerProviderOpts>,
) -> Result<OpenComputerBackendConfig, SandboxError> {
    let mut config = OpenComputerBackendConfig::from_env()?;
    if let Some(options) = options {
        if let Some(value) = options.api_url {
            config.api_url = value;
        }
        if let Some(value) = options.api_key {
            config.api_key = value;
        }
        if let Some(value) = options.template_id {
            config.template_id = value;
        }
        if let Some(value) = options.timeout_secs {
            config.timeout_secs = value as u64;
        }
        config.default_cpu_count = options.default_cpu_count.or(config.default_cpu_count);
        config.default_memory_mb = options.default_memory_mb.or(config.default_memory_mb);
        config.default_disk_mb = options.default_disk_mb.or(config.default_disk_mb);
        config.burst = options.burst.or(config.burst);
        config.secret_store = options.secret_store.or(config.secret_store);
        config.egress_allowlist = options.egress_allowlist.or(config.egress_allowlist);
        if let Some(mounts) = options.mounts {
            config.mounts = mounts
                .into_iter()
                .map(OpenComputerMountOpts::into_config)
                .collect();
        }
        if let Some(mounts) = options.shared_mounts {
            config.shared_mounts = mounts
                .into_iter()
                .map(|(key, mount)| (key, mount.into_config()))
                .collect();
        }
    }
    Ok(config)
}

pub fn positive_resource(value: Option<u32>, label: &str) -> Result<Option<i32>, SandboxError> {
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

#[derive(Deserialize)]
pub struct DistributedControlOptions {
    pub etcd_endpoints: Vec<String>,
    pub nats_url: String,
    pub etcd_prefix: Option<String>,
    pub cluster_id: Option<String>,
    pub nats_auth_token: Option<String>,
    pub required_continuity_tier: Option<String>,
    pub required_storage_profile: Option<String>,
    pub allow_tier_a_degraded: Option<bool>,
    pub allow_cross_node_recovery: Option<bool>,
    pub nats_stream_replicas: Option<u32>,
}

impl TryFrom<DistributedControlOptions> for DistributedControlConfig {
    type Error = SandboxError;

    fn try_from(options: DistributedControlOptions) -> Result<Self, Self::Error> {
        if options.etcd_endpoints.is_empty()
            || options
                .etcd_endpoints
                .iter()
                .any(|endpoint| endpoint.trim().is_empty())
            || options.nats_url.trim().is_empty()
        {
            return Err(SandboxError::InvalidConfig(
                "distributed control requires non-empty etcd endpoints and a NATS URL".into(),
            ));
        }
        if let Some(tier) = options.required_continuity_tier.as_deref()
            && !matches!(tier, "tier-a" | "tier-b")
        {
            return Err(SandboxError::InvalidConfig(
                "required continuity tier must be tier-a or tier-b".into(),
            ));
        }
        if options.nats_stream_replicas == Some(0) {
            return Err(SandboxError::InvalidConfig(
                "NATS stream replicas must be positive".into(),
            ));
        }
        let defaults = Self::default();
        Ok(Self {
            etcd_endpoints: options.etcd_endpoints,
            nats_url: options.nats_url,
            etcd_prefix: options.etcd_prefix.unwrap_or(defaults.etcd_prefix),
            cluster_id: options.cluster_id.unwrap_or(defaults.cluster_id),
            nats_auth_token: options.nats_auth_token,
            required_continuity_tier: options
                .required_continuity_tier
                .or(defaults.required_continuity_tier),
            required_storage_profile: options.required_storage_profile,
            allow_tier_a_degraded: options
                .allow_tier_a_degraded
                .unwrap_or(defaults.allow_tier_a_degraded),
            allow_cross_node_recovery: options
                .allow_cross_node_recovery
                .unwrap_or(defaults.allow_cross_node_recovery),
            nats_stream_replicas: options
                .nats_stream_replicas
                .map(|n| n as usize)
                .unwrap_or(defaults.nats_stream_replicas),
            ..defaults
        })
    }
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SessionSourceType {
    #[default]
    Docker,
    Snapshot,
    MacosTemplate,
    WindowsTemplate,
}
impl From<SessionSourceType> for chevalier_sandbox::SessionSourceType {
    fn from(value: SessionSourceType) -> Self {
        match value {
            SessionSourceType::Docker => Self::Docker,
            SessionSourceType::Snapshot => Self::Snapshot,
            SessionSourceType::MacosTemplate => Self::MacosTemplate,
            SessionSourceType::WindowsTemplate => Self::WindowsTemplate,
        }
    }
}
#[derive(Deserialize)]
pub struct SessionResourceOptions {
    pub vcpu: Option<u32>,
    pub memory_mb: Option<u32>,
}
