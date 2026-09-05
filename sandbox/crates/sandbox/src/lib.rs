// @dive-file: Public sandbox facade exposing local/dynamic and distributed control flows for session, exec/shell, and fork operations.
// @dive-rel: Delegates distributed placement, admission, and routing semantics to crates/sandbox/src/distributed.rs.
// @dive-rel: Preserves locked consumer API while layering HA/distributed policy and node-level multiplexer behavior.
#![allow(clippy::large_enum_variant)]

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::pin::Pin;
#[cfg(feature = "host")]
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{Stream, StreamExt};
#[cfg(feature = "distributed-control")]
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
#[cfg(feature = "host")]
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, OnceCell, mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use uuid::Uuid;

#[cfg(feature = "distributed-control")]
mod distributed;
mod freestyle;
mod opencomputer;
pub mod slo;
pub mod vfs;
pub mod vm;

pub mod proto {
    pub mod bracket {
        pub mod portproxy {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/bracket.portproxy.v1.rs"));
            }
        }
    }

    pub mod google {
        pub mod protobuf {
            include!(concat!(env!("OUT_DIR"), "/google.protobuf.rs"));
        }
    }

    pub mod vmd {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/vmd.v1.rs"));
        }
    }
}

use proto::bracket::portproxy::v1::port_proxy_client::PortProxyClient;
use proto::bracket::portproxy::v1::shell_exec_client::ShellExecClient;
use proto::bracket::portproxy::v1::{
    DeletePathRequest, ExecRequest, ExecResponse, ExecStart, InteractiveShellRequest,
    InteractiveShellResize, InteractiveShellResponse, InteractiveShellStart, ListDirectoryRequest,
    ReadFileRequest, WriteFileRequest, WriteFileStreamRequest, WriteFileStreamStart, exec_request,
    exec_response, interactive_shell_request, interactive_shell_response,
    write_file_stream_request,
};
use proto::vmd::v1::vmd_service_client::VmdServiceClient;
use proto::vmd::v1::{
    AttachPciDeviceRequest, CreateSnapshotRequest, CreateVmRequest, DeleteDurableVolumeRequest,
    DeleteSnapshotRequest, DesktopKind, DetachPciDeviceRequest, ForkVmRequest, GetVmBySessionRequest,
    GetVmRequest, GuestPlatform, GuestProfile, ListDurableVolumesRequest, ListHostPciDevicesRequest, ListSnapshotsRequest,
    ListVMsRequest, Metadata, PreDownloadVmImageRequest, ResizeDurableVolumeRequest, ResourceSpec,
    RestoreSnapshotRequest, UpdateVmRequest, Vm, VmActionRequest, VmSource, VmSourceType,
};

const PCI_CAPABILITY_HEADER: &str = "x-chevalier-pci-token";
const DURABLE_VOLUME_LIST_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_PORTPROXY_FILE_RPC_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_PORTPROXY_WRITE_FILE_RPC_TIMEOUT: Duration = Duration::from_secs(120);
/// VMD drains mount-local publication for up to 30 seconds before a destructive
/// delete, then stops qemu and removes its runtime resources. The ordinary
/// control-plane timeout is intentionally short and cannot cover that lifecycle
/// contract.
const VMD_DELETE_TIMEOUT: Duration = Duration::from_secs(60);
const VMD_VM_START_TIMEOUT: Duration = Duration::from_secs(600);
const VMD_VM_STOP_TIMEOUT: Duration = Duration::from_secs(150);

/// A graceful restart is a guest shutdown (vmd waits up to its 180 s graceful-stop
/// budget) followed by a fresh boot. The ordinary control-plane request timeout is a
/// few seconds, so issuing `RestartVm` under it guarantees a client-side timeout that
/// abandons the RPC mid-shutdown. Size the deadline to the lifecycle it covers.
const VMD_RESTART_TIMEOUT: Duration = Duration::from_secs(240);

const META_SESSION_ID: &str = "chevalier.session_id";
const META_PARENT_SESSION_ID: &str = "chevalier.parent_session_id";
const META_PARENT_VM_ID: &str = "chevalier.parent_vm_id";
const META_FORK_ID: &str = "chevalier.fork_id";
const META_FORK_SNAPSHOT: &str = "chevalier.fork_snapshot";
const META_EXEC_RESTORE_SNAPSHOT_ID: &str = "chevalier.execution_restore_snapshot_id";
const META_EXEC_RESTORE_SNAPSHOT_NAME: &str = "chevalier.execution_restore_snapshot_name";
const META_TIER_B_ELIGIBLE: &str = "chevalier.tier_b_eligible";
const META_EXECUTION_FIDELITY_REQUIREMENT: &str = "chevalier.execution_fidelity_requirement";
const META_PORTPROXY_AUTH_TOKEN: &str = "chevalier.portproxy_auth_token";

#[derive(Clone, Debug)]
pub struct DistributedControlConfig {
    pub etcd_endpoints: Vec<String>,
    pub etcd_prefix: String,
    pub cluster_id: String,
    pub nats_url: String,
    pub nats_auth_token: Option<String>,
    pub nats_subject_prefix: String,
    pub nats_stream_name: String,
    pub nats_stream_max_age_secs: u64,
    pub nats_stream_replicas: usize,
    pub nats_dead_letter_subject: String,
    pub required_storage_profile: Option<String>,
    pub required_continuity_tier: Option<String>,
    pub allow_tier_a_degraded: bool,
    pub allow_cross_node_recovery: bool,
    pub tenant_session_quota: Option<usize>,
    pub workspace_session_quota: Option<usize>,
    pub admission_retry_after_ms: u64,
}

impl Default for DistributedControlConfig {
    fn default() -> Self {
        let subject_prefix = "chevalier.sandbox.control".to_string();
        Self {
            etcd_endpoints: vec!["http://127.0.0.1:2379".to_string()],
            etcd_prefix: "/chevalier-sandbox".to_string(),
            cluster_id: "chevalier-sandbox-cluster".to_string(),
            nats_url: "nats://127.0.0.1:4222".to_string(),
            nats_auth_token: None,
            nats_subject_prefix: subject_prefix.clone(),
            nats_stream_name: "CHEVALIER_SANDBOX_CONTROL".to_string(),
            nats_stream_max_age_secs: 60 * 60 * 24 * 7,
            nats_stream_replicas: 1,
            nats_dead_letter_subject: format!("{subject_prefix}.dlq.commands"),
            required_storage_profile: None,
            required_continuity_tier: Some("tier-b".to_string()),
            allow_tier_a_degraded: false,
            allow_cross_node_recovery: true,
            tenant_session_quota: Some(256),
            workspace_session_quota: Some(64),
            admission_retry_after_ms: 2_000,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct TlsClientConfig {
    pub ca_cert_path: Option<PathBuf>,
    pub client_cert_path: Option<PathBuf>,
    pub client_key_path: Option<PathBuf>,
    pub domain_name: Option<String>,
}

#[derive(thiserror::Error, Debug)]
pub enum SandboxError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("gRPC status: {0}")]
    Grpc(#[from] tonic::Status),
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),
    #[error("invalid response: {0}")]
    InvalidResponse(String),
    #[error("session not found: {0}")]
    SessionNotFound(String),
    #[error("daemon unavailable: {0}")]
    DaemonUnavailable(String),
    /// The VM is running but its guest RPC sidecar (portproxy/shell exec) did not answer
    /// within the readiness budget. `restarted == false` means the guest was created or
    /// started by the current operation and is simply still booting: never a rebind
    /// candidate and never masked by a restart. `restarted == true` means an
    /// already-running VM stayed unready even after one bounded local restart, which
    /// authorizes cross-node escalation while keeping this original cause attached.
    #[error(
        "guest RPC not ready for vm {vm_id} on {endpoint} after {waited:?}{}",
        guest_rpc_restart_note(.restarted)
    )]
    GuestRpcNotReady {
        vm_id: String,
        endpoint: String,
        waited: Duration,
        restarted: bool,
    },
    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),
    #[error("ownership fence conflict: {0}")]
    FenceConflict(String),
    #[error("unsupported operation: {0}")]
    Unsupported(String),
    #[error("invalid config: {0}")]
    InvalidConfig(String),
}

pub type Result<T> = std::result::Result<T, SandboxError>;

fn guest_rpc_restart_note(restarted: &bool) -> &'static str {
    if *restarted {
        " (still not ready after one bounded local restart)"
    } else {
        ""
    }
}

#[derive(Clone, Debug)]
pub struct ResourceLimits {
    pub vcpu: i32,
    pub memory_mb: i32,
    pub disk_gb: i32,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            vcpu: 2,
            memory_mb: 2048,
            disk_gb: 10,
        }
    }
}

#[derive(Clone, Debug)]
pub struct WarmPoolProfile {
    pub image: String,
    pub architecture: Option<String>,
    pub min_inventory: usize,
}

impl WarmPoolProfile {
    fn normalized_architecture(&self) -> Option<String> {
        self.architecture
            .as_deref()
            .map(normalize_architecture_label)
            .filter(|value| !value.is_empty())
    }
}

#[derive(Clone, Debug, Default)]
pub enum SandboxProviderConfig {
    #[default]
    Chevalier,
    OpenComputer(OpenComputerBackendConfig),
    Freestyle(FreestyleBackendConfig),
}

impl SandboxProviderConfig {
    /// Stable lowercase provider name for logs, metrics, and callers that persist it.
    pub fn provider_name(&self) -> &'static str {
        match self {
            Self::Chevalier => "chevalier",
            Self::OpenComputer(_) => "opencomputer",
            Self::Freestyle(_) => "freestyle",
        }
    }
}

/// Freestyle (freestyle.sh) provider settings. Sessions are full Linux VMs booted
/// from `snapshot_id`; the facade talks to `api_url` with `api_key` and never to the
/// guest directly, so no host-side daemon, KVM, or FUSE cooperation is needed.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct FreestyleBackendConfig {
    pub api_url: String,
    pub api_key: String,
    /// Snapshot id, your slug, or a public `{owner}/{slug}`; empty means the platform default.
    pub snapshot_id: String,
    /// Suffix for preview hostnames; `style.dev` names are claimed on first use.
    pub preview_domain_suffix: String,
    /// A `POST /v5/tls/forward-auth` configuration id to attach to every preview rule.
    pub forward_auth_id: Option<String>,
    /// Pause after this many seconds without network activity; `None` never pauses.
    pub idle_timeout_secs: Option<u64>,
    /// Delete a VM once it has sat stopped/paused this long; `None` keeps it. Never 0 for
    /// sessions: Freestyle refuses to pause an ephemeral VM, so idle pause would fail.
    pub auto_delete_secs: Option<u64>,
    /// Delete checkpoints nobody has booted for this long; `None` keeps them.
    pub snapshot_auto_delete_secs: Option<u64>,
    /// Guest user for exec and shells; `None` is the image default (uid 1000, else root).
    pub linux_user: Option<String>,
    pub egress_allowlist: Option<Vec<String>>,
    /// Shared-mount launch templates keyed by mount tag, guest path, or backend profile.
    pub shared_mounts: HashMap<String, ManagedMountConfig>,
}

impl Default for FreestyleBackendConfig {
    fn default() -> Self {
        Self {
            api_url: "https://api.freestyle.sh".to_string(),
            api_key: String::new(),
            snapshot_id: String::new(),
            preview_domain_suffix: "style.dev".to_string(),
            forward_auth_id: None,
            idle_timeout_secs: None,
            auto_delete_secs: None,
            snapshot_auto_delete_secs: None,
            linux_user: None,
            egress_allowlist: None,
            shared_mounts: HashMap::new(),
        }
    }
}

impl FreestyleBackendConfig {
    pub fn from_env() -> Result<Self> {
        fn optional_secs(name: &str) -> Result<Option<u64>> {
            match std::env::var(name) {
                Ok(value) if !value.trim().is_empty() => {
                    value.trim().parse().map(Some).map_err(|err| {
                        SandboxError::InvalidConfig(format!("invalid {name}: {err}"))
                    })
                }
                _ => Ok(None),
            }
        }
        let mut cfg = Self {
            api_url: std::env::var("FREESTYLE_API_URL")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "https://api.freestyle.sh".to_string()),
            api_key: std::env::var("FREESTYLE_API_KEY").unwrap_or_default(),
            ..Self::default()
        };
        if let Ok(snapshot_id) = std::env::var("FREESTYLE_SNAPSHOT_ID") {
            cfg.snapshot_id = snapshot_id;
        }
        if let Ok(suffix) = std::env::var("FREESTYLE_PREVIEW_DOMAIN_SUFFIX")
            && !suffix.trim().is_empty()
        {
            cfg.preview_domain_suffix = suffix;
        }
        cfg.forward_auth_id = std::env::var("FREESTYLE_FORWARD_AUTH_ID")
            .ok()
            .filter(|value| !value.trim().is_empty());
        cfg.linux_user = std::env::var("FREESTYLE_LINUX_USER")
            .ok()
            .filter(|value| !value.trim().is_empty());
        cfg.idle_timeout_secs = optional_secs("FREESTYLE_IDLE_TIMEOUT_SECS")?;
        cfg.auto_delete_secs = optional_secs("FREESTYLE_AUTO_DELETE_SECS")?;
        cfg.snapshot_auto_delete_secs = optional_secs("FREESTYLE_SNAPSHOT_AUTO_DELETE_SECS")?;
        if let Ok(shared_mounts_json) = std::env::var("FREESTYLE_SHARED_MOUNTS_JSON") {
            cfg.shared_mounts = serde_json::from_str(&shared_mounts_json).map_err(|err| {
                SandboxError::InvalidConfig(format!("invalid FREESTYLE_SHARED_MOUNTS_JSON: {err}"))
            })?;
        }
        if let Ok(egress_allowlist_json) = std::env::var("FREESTYLE_EGRESS_ALLOWLIST_JSON") {
            cfg.egress_allowlist =
                Some(serde_json::from_str(&egress_allowlist_json).map_err(|err| {
                    SandboxError::InvalidConfig(format!(
                        "invalid FREESTYLE_EGRESS_ALLOWLIST_JSON: {err}"
                    ))
                })?);
        }
        Ok(cfg)
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct OpenComputerBackendConfig {
    pub api_url: String,
    pub api_key: String,
    pub template_id: String,
    pub checkpoint_id: String,
    pub timeout_secs: u64,
    pub default_cpu_count: Option<u32>,
    pub default_memory_mb: Option<u32>,
    pub default_disk_mb: Option<u32>,
    pub burst: Option<bool>,
    pub secret_store: Option<String>,
    pub egress_allowlist: Option<Vec<String>>,
    pub mounts: Vec<ManagedMountConfig>,
    pub shared_mounts: HashMap<String, ManagedMountConfig>,
}

/// A mount inside a provider-managed sandbox. OpenComputer accepts every field
/// (rclone remotes or a `command` driver); Freestyle honors only the `command`
/// shape (`path`, `command`, `env`, `secrets`, `read_only`).
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
pub struct ManagedMountConfig {
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub driver: Option<String>,
    #[serde(default)]
    pub remote: String,
    pub backend: Option<String>,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub secrets: HashMap<String, String>,
    #[serde(default)]
    pub creds: HashMap<String, String>,
    pub rclone_config: Option<String>,
    pub read_only: Option<bool>,
    #[serde(default)]
    pub mount_options: Vec<String>,
}

pub type OpenComputerMountConfig = ManagedMountConfig;
pub type FreestyleMountConfig = ManagedMountConfig;

impl Default for OpenComputerBackendConfig {
    fn default() -> Self {
        Self {
            api_url: "https://app.opencomputer.dev".to_string(),
            api_key: String::new(),
            template_id: "base".to_string(),
            checkpoint_id: String::new(),
            timeout_secs: 0,
            default_cpu_count: None,
            default_memory_mb: None,
            default_disk_mb: None,
            burst: None,
            secret_store: None,
            egress_allowlist: None,
            mounts: Vec::new(),
            shared_mounts: HashMap::new(),
        }
    }
}

impl ManagedMountConfig {
    pub fn rclone(
        path: impl Into<String>,
        remote: impl Into<String>,
        backend: impl Into<String>,
        creds: HashMap<String, String>,
    ) -> Self {
        Self {
            path: path.into(),
            driver: None,
            remote: remote.into(),
            backend: Some(backend.into()),
            command: Vec::new(),
            env: HashMap::new(),
            secrets: HashMap::new(),
            creds,
            rclone_config: None,
            read_only: None,
            mount_options: Vec::new(),
        }
    }

    pub fn with_rclone_config(
        path: impl Into<String>,
        remote: impl Into<String>,
        rclone_config: impl Into<String>,
    ) -> Self {
        Self {
            path: path.into(),
            driver: None,
            remote: remote.into(),
            backend: None,
            command: Vec::new(),
            env: HashMap::new(),
            secrets: HashMap::new(),
            creds: HashMap::new(),
            rclone_config: Some(rclone_config.into()),
            read_only: None,
            mount_options: Vec::new(),
        }
    }

    pub fn command<I, S>(path: impl Into<String>, command: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            path: path.into(),
            driver: Some("command".to_string()),
            remote: String::new(),
            backend: None,
            command: command.into_iter().map(Into::into).collect(),
            env: HashMap::new(),
            secrets: HashMap::new(),
            creds: HashMap::new(),
            rclone_config: None,
            read_only: None,
            mount_options: Vec::new(),
        }
    }
}

impl OpenComputerBackendConfig {
    pub fn from_env() -> Result<Self> {
        let mut cfg = Self {
            api_url: std::env::var("OPENCOMPUTER_API_URL")
                .unwrap_or_else(|_| "https://app.opencomputer.dev".to_string()),
            api_key: std::env::var("OPENCOMPUTER_API_KEY").unwrap_or_default(),
            ..Self::default()
        };
        if let Ok(template_id) = std::env::var("OPENCOMPUTER_TEMPLATE_ID") {
            cfg.template_id = template_id;
        }
        if let Ok(checkpoint_id) = std::env::var("OPENCOMPUTER_CHECKPOINT_ID") {
            cfg.checkpoint_id = checkpoint_id;
        }
        if let Ok(timeout) = std::env::var("OPENCOMPUTER_SANDBOX_TIMEOUT_SECS") {
            cfg.timeout_secs = timeout.parse().map_err(|err| {
                SandboxError::InvalidConfig(format!(
                    "invalid OPENCOMPUTER_SANDBOX_TIMEOUT_SECS: {err}"
                ))
            })?;
        }
        if let Ok(mounts_json) = std::env::var("OPENCOMPUTER_MOUNTS_JSON") {
            cfg.mounts = serde_json::from_str(&mounts_json).map_err(|err| {
                SandboxError::InvalidConfig(format!("invalid OPENCOMPUTER_MOUNTS_JSON: {err}"))
            })?;
        }
        if let Ok(shared_mounts_json) = std::env::var("OPENCOMPUTER_SHARED_MOUNTS_JSON") {
            cfg.shared_mounts = serde_json::from_str(&shared_mounts_json).map_err(|err| {
                SandboxError::InvalidConfig(format!(
                    "invalid OPENCOMPUTER_SHARED_MOUNTS_JSON: {err}"
                ))
            })?;
        }
        if let Ok(egress_allowlist_json) = std::env::var("OPENCOMPUTER_EGRESS_ALLOWLIST_JSON") {
            cfg.egress_allowlist =
                Some(serde_json::from_str(&egress_allowlist_json).map_err(|err| {
                    SandboxError::InvalidConfig(format!(
                        "invalid OPENCOMPUTER_EGRESS_ALLOWLIST_JSON: {err}"
                    ))
                })?);
        }
        Ok(cfg)
    }
}

#[derive(Clone, Debug)]
pub struct SandboxConfig {
    pub provider: SandboxProviderConfig,
    pub endpoint: String,
    pub control_gateway_endpoints: Vec<String>,
    pub endpoint_overrides: HashMap<String, String>,
    pub auto_spawn: bool,
    pub daemon_listen: String,
    pub daemon_bin: Option<PathBuf>,
    pub daemon_data_dir: Option<PathBuf>,
    pub daemon_start_timeout: Duration,
    pub portproxy_ready_timeout: Duration,
    pub connect_timeout: Duration,
    pub default_image: String,
    pub default_architecture: Option<String>,
    pub default_resources: ResourceLimits,
    pub default_shell: String,
    pub portproxy_client_bin: Option<PathBuf>,
    pub warm_pool_profiles: Vec<WarmPoolProfile>,
    pub prewarm_on_start: bool,
    pub distributed_control: Option<DistributedControlConfig>,
    pub auth_token: Option<String>,
    pub pci_access_token: Option<String>,
    pub tls: Option<TlsClientConfig>,
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            provider: SandboxProviderConfig::default(),
            endpoint: "http://127.0.0.1:8052".to_string(),
            control_gateway_endpoints: Vec::new(),
            endpoint_overrides: HashMap::new(),
            auto_spawn: true,
            daemon_listen: "127.0.0.1:8052".to_string(),
            daemon_bin: None,
            daemon_data_dir: None,
            daemon_start_timeout: Duration::from_secs(20),
            portproxy_ready_timeout: Duration::from_secs(90),
            connect_timeout: Duration::from_secs(5),
            default_image: std::env::var("BRACKET_VM_IMAGE").unwrap_or_default(),
            default_architecture: None,
            default_resources: ResourceLimits::default(),
            default_shell: "/bin/sh".to_string(),
            portproxy_client_bin: None,
            warm_pool_profiles: Vec::new(),
            prewarm_on_start: true,
            distributed_control: None,
            auth_token: None,
            pci_access_token: None,
            tls: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SessionSourceType {
    #[default]
    Docker,
    Snapshot,
    MacosTemplate,
    WindowsTemplate,
}

#[derive(Clone, Debug)]
pub struct SessionOptions {
    pub session_id: Option<String>,
    pub name: Option<String>,
    pub image: Option<String>,
    pub source_type: SessionSourceType,
    pub architecture: Option<String>,
    pub metadata: HashMap<String, String>,
    pub auto_start: bool,
    pub resources: Option<ResourceLimits>,
    pub shared_mounts: Vec<SharedMount>,
    pub egress_allowlist: Option<Vec<String>>,
    pub pci_device_ids: Vec<String>,
    pub storage_profile: String,
    pub volume_owner_key: Option<String>,
    pub volume_size_gb: Option<i32>,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            session_id: None,
            name: None,
            image: None,
            source_type: SessionSourceType::Docker,
            architecture: None,
            metadata: HashMap::new(),
            auto_start: true,
            resources: None,
            shared_mounts: Vec::new(),
            egress_allowlist: None,
            pci_device_ids: Vec::new(),
            storage_profile: "local-ephemeral".to_string(),
            volume_owner_key: None,
            volume_size_gb: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum SharedMountAvailability {
    #[default]
    NodeLocal,
    SharedStorage,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum SharedMountContinuity {
    #[default]
    RestartSameNode,
    RestoreCrossNode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SharedMount {
    pub host_path: String,
    pub guest_path: String,
    pub mount_tag: String,
    pub read_only: bool,
    pub availability: SharedMountAvailability,
    pub continuity: SharedMountContinuity,
    pub backend_profile: String,
    pub vfs_endpoint: String,
    pub vfs_scope_path: String,
}

#[derive(Clone, Debug, Default)]
pub struct ForkOptions {
    pub child_name: Option<String>,
    pub child_metadata: HashMap<String, String>,
    pub auto_start_child: bool,
}

#[derive(Clone, Debug, Default)]
pub struct ExecOptions {
    pub env: HashMap<String, String>,
    pub timeout_secs: Option<i32>,
    pub detach: bool,
    pub shell: Option<String>,
    pub close_stdin_on_start: bool,
}

#[derive(Clone, Debug, Default)]
pub struct ShellOptions {
    pub shell: Option<String>,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub cwd: Option<String>,
    pub cols: Option<u16>,
    pub rows: Option<u16>,
}

#[derive(Clone, Debug)]
pub enum ExecInput {
    Data(Vec<u8>),
    Eof,
    Signal(i32),
    Resize { cols: u16, rows: u16 },
}

#[derive(Clone, Debug)]
pub enum ExecEvent {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Exit(i32),
    Timeout,
}

#[derive(Clone, Debug)]
pub enum ShellInput {
    Data(Vec<u8>),
    Resize { cols: u16, rows: u16 },
    Eof,
}

#[derive(Clone, Debug)]
pub enum ShellEvent {
    Output(Vec<u8>),
    Exit(i32),
}

pub type EventStream<T> = Pin<Box<dyn Stream<Item = Result<T>> + Send>>;

pub struct ExecHandle {
    pub input: ExecInputSender,
    pub events: EventStream<ExecEvent>,
}

#[derive(Clone)]
pub struct ExecInputSender {
    data: mpsc::Sender<ExecInput>,
    control: mpsc::Sender<ExecInput>,
}

impl ExecInputSender {
    pub async fn send(
        &self,
        input: ExecInput,
    ) -> std::result::Result<(), mpsc::error::SendError<ExecInput>> {
        match input {
            ExecInput::Signal(_) => self.control.send(input).await,
            _ => self.data.send(input).await,
        }
    }
}

impl From<mpsc::Sender<ExecInput>> for ExecInputSender {
    fn from(data: mpsc::Sender<ExecInput>) -> Self {
        Self {
            control: data.clone(),
            data,
        }
    }
}

#[cfg(feature = "distributed-control")]
struct DistributedExecStreamState {
    endpoint: String,
    target_node_id: String,
    stream_id: String,
    producer_epoch: u64,
    events: mpsc::Receiver<Result<distributed::ExecStreamEvent>>,
}

#[cfg(feature = "distributed-control")]
struct DistributedExecStartParams<'a> {
    command: &'a str,
    opts: &'a ExecOptions,
    control: &'a distributed::DistributedControlPlane,
    endpoint: String,
    target_node_id: String,
    start_wait: Duration,
    idle_timeout: Duration,
}

#[cfg(feature = "distributed-control")]
struct DistributedExecRebindParams<'a> {
    control: &'a distributed::DistributedControlPlane,
    command: &'a str,
    opts: &'a ExecOptions,
    start_wait: Duration,
    current_endpoint: &'a str,
    logical_stream_id: &'a str,
    resume_after_event_seq: u64,
    idle_timeout: Duration,
    current_target_node_id: &'a str,
    current_producer_epoch: u64,
}

pub struct ShellHandle {
    pub input: mpsc::Sender<ShellInput>,
    pub events: EventStream<ShellEvent>,
}

#[cfg(feature = "distributed-control")]
#[derive(Clone)]
struct PortLifecycleContext {
    sandbox: Sandbox,
    session_id: String,
    vm_id: String,
    node_endpoint: String,
    ownership_fence: Option<String>,
}

struct ForwardRegistration {
    multiplexer: Arc<NodePortMultiplexer>,
    host_port: u16,
    #[cfg(feature = "distributed-control")]
    port_lease: Option<distributed::PortAllocationLease>,
}

struct ForwardTask {
    shutdown_tx: oneshot::Sender<()>,
    join: tokio::task::JoinHandle<()>,
}

#[derive(Default)]
struct NodePortMultiplexer {
    forwards: Mutex<HashMap<u16, ForwardTask>>,
}

fn configured_forward_bind_addr() -> String {
    std::env::var("CHEVALIER_SANDBOX_FORWARD_BIND_ADDR")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "127.0.0.1".to_string())
}

fn configured_forward_port_range() -> Option<(u16, u16)> {
    let raw = std::env::var("CHEVALIER_SANDBOX_FORWARD_PORT_RANGE").ok()?;
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }

    let normalized = raw.replace(':', "-");
    let (start, end) = normalized.split_once('-')?;
    let start = start.trim().parse::<u16>().ok()?;
    let end = end.trim().parse::<u16>().ok()?;
    if start == 0 || end == 0 {
        return None;
    }
    Some(if start <= end {
        (start, end)
    } else {
        (end, start)
    })
}

impl NodePortMultiplexer {
    async fn register(&self, guest_port: u16, server_addr: String) -> Result<u16> {
        let bind_addr = configured_forward_bind_addr();
        let listener = if let Some((range_start, range_end)) = configured_forward_port_range() {
            let mut bound: Option<TcpListener> = None;
            let mut last_error: Option<std::io::Error> = None;
            for port in range_start..=range_end {
                match TcpListener::bind((bind_addr.as_str(), port)).await {
                    Ok(listener) => {
                        bound = Some(listener);
                        break;
                    }
                    Err(error) => {
                        last_error = Some(error);
                    }
                }
            }
            match bound {
                Some(listener) => listener,
                None => {
                    return Err(SandboxError::Io(last_error.unwrap_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::AddrNotAvailable,
                            format!(
                                "no available forward port in range {}-{} on {}",
                                range_start, range_end, bind_addr
                            ),
                        )
                    })));
                }
            }
        } else {
            TcpListener::bind((bind_addr.as_str(), 0)).await?
        };
        let host_port = listener.local_addr()?.port();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let join = tokio::spawn(async move {
            run_forward_listener(listener, guest_port, server_addr, shutdown_rx).await;
        });

        let mut guard = self.forwards.lock().await;
        guard.insert(host_port, ForwardTask { shutdown_tx, join });
        Ok(host_port)
    }

    async fn unregister(&self, host_port: u16) {
        let task = {
            let mut guard = self.forwards.lock().await;
            guard.remove(&host_port)
        };

        if let Some(task) = task {
            let _ = task.shutdown_tx.send(());
            let _ = task.join.await;
        }
    }
}

pub struct ForwardHandle {
    pub guest_port: u16,
    pub host_port: u16,
    registration: Arc<Mutex<Option<ForwardRegistration>>>,
    #[cfg(feature = "distributed-control")]
    port_context: Option<PortLifecycleContext>,
}

impl ForwardHandle {
    pub async fn close(&self) -> Result<()> {
        let registration = {
            let mut guard = self.registration.lock().await;
            guard.take()
        };
        let released = registration.is_some();

        #[allow(unused_mut)]
        if let Some(mut registration) = registration {
            registration
                .multiplexer
                .unregister(registration.host_port)
                .await;
            #[cfg(feature = "distributed-control")]
            if let Some(port_lease) = registration.port_lease.take() {
                port_lease.shutdown().await;
            }
        }

        #[cfg(feature = "distributed-control")]
        if released {
            self.publish_release().await;
        }
        #[cfg(not(feature = "distributed-control"))]
        let _ = released;

        Ok(())
    }
}

impl Drop for ForwardHandle {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.registration.try_lock() {
            let registration = guard.take();
            drop(guard);
            #[allow(unused_mut)]
            if let Some(mut registration) = registration
                && let Ok(handle) = tokio::runtime::Handle::try_current()
            {
                handle.spawn(async move {
                    registration
                        .multiplexer
                        .unregister(registration.host_port)
                        .await;
                    #[cfg(feature = "distributed-control")]
                    if let Some(port_lease) = registration.port_lease.take() {
                        port_lease.shutdown().await;
                    }
                });
            }
        }
    }
}

async fn run_forward_listener(
    listener: TcpListener,
    guest_port: u16,
    server_addr: String,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = &mut shutdown_rx => break,
            accept_result = listener.accept() => {
                let Ok((socket, _peer)) = accept_result else {
                    break;
                };
                let server_addr = server_addr.clone();
                tokio::spawn(async move {
                    let _ = forward_client_connection(socket, guest_port, &server_addr).await;
                });
            }
        }
    }
}

async fn forward_client_connection(
    mut socket: TcpStream,
    forward_port: u16,
    server_addr: &str,
) -> std::io::Result<()> {
    let mut server = TcpStream::connect(server_addr).await?;
    server.write_all(&forward_port.to_be_bytes()).await?;
    let _ = tokio::io::copy_bidirectional(&mut socket, &mut server).await?;
    Ok(())
}

impl ForwardHandle {
    #[cfg(feature = "distributed-control")]
    async fn publish_release(&self) {
        if let Some(ctx) = &self.port_context {
            let _ = ctx
                .sandbox
                .publish_control_command(
                    "port.release",
                    &ctx.vm_id,
                    json!({
                        "session_id": ctx.session_id.as_str(),
                        "vm_id": ctx.vm_id.as_str(),
                        "endpoint": ctx.node_endpoint.as_str(),
                        "guest_port": self.guest_port,
                        "host_port": self.host_port,
                        "expected_fence": ctx.ownership_fence.as_deref(),
                    }),
                )
                .await;
        }
    }
}

#[derive(Clone, Debug)]
pub struct SessionInfo {
    pub session_id: String,
    pub vm_id: String,
    pub name: String,
    pub state: i32,
    pub parent_session_id: Option<String>,
    pub fork_id: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionDesktopKind {
    Vnc,
    NativeWindow,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionDesktopAuthentication {
    None,
    Password,
    Account,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionDesktopTarget {
    pub kind: SessionDesktopKind,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub password: Option<String>,
    pub authentication: SessionDesktopAuthentication,
    pub view_only: bool,
}

#[derive(Clone, Debug)]
pub struct DurableVolumeInfo {
    pub owner_key: String,
    pub volume_id: String,
    pub size_gb: i32,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub backing_volume_id: Option<String>,
    pub attached_vm_ids: Vec<String>,
}

pub struct ForkResult {
    pub parent_session_id: String,
    pub child_session_id: String,
    pub fork_id: String,
    pub child: Session,
}

#[derive(Clone, Debug)]
pub struct SessionCheckpoint {
    pub id: String,
}

#[derive(Clone, Debug)]
pub struct SessionSnapshot {
    pub id: String,
    pub name: String,
    pub label: String,
    pub description: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostPciDeviceState {
    Disabled,
    Unavailable,
    Host,
    Ready,
    Assigned,
    Error,
    Unknown,
}

#[derive(Clone, Debug)]
pub struct HostPciFunction {
    pub bdf: String,
    pub vendor_id: String,
    pub device_id: String,
    pub class_code: String,
    pub driver: String,
    pub iommu_group: String,
}

#[derive(Clone, Debug)]
pub struct HostPciDevice {
    pub id: String,
    pub label: String,
    pub functions: Vec<HostPciFunction>,
    pub state: HostPciDeviceState,
    pub assigned_vm_id: String,
    pub managed: bool,
    pub hotplug_capable: bool,
    pub unavailable_reason: String,
}

#[derive(Clone, Debug)]
pub struct HostPciInventory {
    pub enabled: bool,
    pub devices: Vec<HostPciDevice>,
}

#[derive(Clone, Debug)]
pub struct PciDeviceAction {
    pub device: Option<HostPciDevice>,
    pub restart_required: bool,
    pub detail: String,
    pub vm_state: i32,
}

#[derive(Clone)]
pub struct Sandbox {
    inner: Arc<SandboxInner>,
}

#[cfg(feature = "host")]
struct ManagedDaemon {
    child: Child,
}

struct SandboxInner {
    cfg: SandboxConfig,
    control_backend: ControlBackend,
    auth_header: Option<MetadataValue<Ascii>>,
    pci_auth_header: Option<MetadataValue<Ascii>>,
    #[cfg(feature = "host")]
    managed_daemon: Mutex<Option<ManagedDaemon>>,
    ready_vm_rpc: Mutex<HashMap<String, GuestRpcAccess>>,
    portproxy_channels: Mutex<HashMap<String, Arc<PortproxyChannelEntry>>>,
    node_multiplexers: Mutex<HashMap<String, Arc<NodePortMultiplexer>>>,
    warm_pool_ready: Mutex<HashSet<String>>,
    managed_session_aliases: Mutex<HashMap<String, String>>,
}

enum ControlBackend {
    Direct,
    /// A provider that owns the whole VM lifecycle behind an HTTP API (OpenComputer, Freestyle).
    Managed(ManagedControl),
    #[cfg(feature = "distributed-control")]
    Distributed(distributed::DistributedControlPlane),
}

/// A sandbox as a provider-managed backend reports it right after create/get.
#[derive(Clone, Debug)]
pub(crate) struct ManagedSandbox {
    pub id: String,
}

/// The provider-managed backends behind one method set. Every `Session`/`Sandbox`
/// branch for these providers goes through here, so adding a backend means adding
/// a variant and an arm per method, never another branch at a call site.
#[derive(Clone)]
enum ManagedControl {
    OpenComputer(opencomputer::OpenComputerControl),
    Freestyle(freestyle::FreestyleControl),
}

impl ManagedControl {
    fn provider_name(&self) -> &'static str {
        match self {
            Self::OpenComputer(_) => "opencomputer",
            Self::Freestyle(_) => "freestyle",
        }
    }

    fn api_url(&self) -> &str {
        match self {
            Self::OpenComputer(control) => control.api_url(),
            Self::Freestyle(control) => control.api_url(),
        }
    }

    async fn create_sandbox(
        &self,
        image: Option<String>,
        resources: Option<ResourceLimits>,
        metadata: HashMap<String, String>,
        egress_allowlist: Option<Vec<String>>,
        shared_mounts: &[SharedMount],
    ) -> Result<ManagedSandbox> {
        match self {
            Self::OpenComputer(control) => control
                .create_sandbox(
                    image,
                    resources,
                    metadata,
                    None,
                    egress_allowlist,
                    shared_mounts,
                )
                .await
                .map(|sandbox| ManagedSandbox {
                    id: sandbox.sandbox_id,
                }),
            Self::Freestyle(control) => control
                .create_sandbox(image, resources, metadata, egress_allowlist, shared_mounts)
                .await
                .map(|vm| ManagedSandbox { id: vm.id }),
        }
    }

    async fn get_sandbox(&self, sandbox_id: &str) -> Result<ManagedSandbox> {
        match self {
            Self::OpenComputer(control) => {
                control
                    .get_sandbox(sandbox_id)
                    .await
                    .map(|sandbox| ManagedSandbox {
                        id: sandbox.sandbox_id,
                    })
            }
            Self::Freestyle(control) => control
                .get_sandbox(sandbox_id)
                .await
                .map(|vm| ManagedSandbox { id: vm.id }),
        }
    }

    /// Look a sandbox up by the logical session id stamped in its metadata.
    async fn find_by_session_id(&self, session_id: &str) -> Result<Option<ManagedSandbox>> {
        match self {
            Self::OpenComputer(_) => Ok(None),
            Self::Freestyle(control) => Ok(control
                .find_by_session_id(session_id)
                .await?
                .map(|vm| ManagedSandbox { id: vm.id })),
        }
    }

    /// Bring a paused or stopped sandbox back before it is handed out as attached.
    async fn ensure_running(&self, sandbox_id: &str) -> Result<()> {
        match self {
            Self::OpenComputer(_) => Ok(()),
            Self::Freestyle(control) => control.ensure_running(sandbox_id).await,
        }
    }

    async fn ensure_configured_mounts(
        &self,
        sandbox_id: &str,
        shared_mounts: &[SharedMount],
    ) -> Result<()> {
        match self {
            Self::OpenComputer(control) => {
                control
                    .ensure_configured_mounts(sandbox_id, shared_mounts)
                    .await
            }
            Self::Freestyle(control) => {
                control
                    .ensure_configured_mounts(sandbox_id, shared_mounts)
                    .await
            }
        }
    }

    async fn delete_sandbox(&self, sandbox_id: &str) -> Result<()> {
        match self {
            Self::OpenComputer(control) => control.delete_sandbox(sandbox_id).await,
            Self::Freestyle(control) => control.delete_sandbox(sandbox_id).await,
        }
    }

    async fn list_sessions(&self) -> Result<Vec<SessionInfo>> {
        match self {
            Self::OpenComputer(control) => control.list_sessions().await,
            Self::Freestyle(control) => control.list_sessions().await,
        }
    }

    async fn read_file(&self, sandbox_id: &str, path: &str) -> Result<Vec<u8>> {
        match self {
            Self::OpenComputer(control) => control.read_file(sandbox_id, path).await,
            Self::Freestyle(control) => control.read_file(sandbox_id, path).await,
        }
    }

    async fn write_file(&self, sandbox_id: &str, path: &str, data: Vec<u8>) -> Result<()> {
        match self {
            Self::OpenComputer(control) => control.write_file(sandbox_id, path, data).await,
            Self::Freestyle(control) => control.write_file(sandbox_id, path, data).await,
        }
    }

    async fn list_dir(
        &self,
        sandbox_id: &str,
        path: &str,
    ) -> Result<Vec<proto::bracket::portproxy::v1::DirectoryEntry>> {
        match self {
            Self::OpenComputer(control) => control.list_dir(sandbox_id, path).await,
            Self::Freestyle(control) => control.list_dir(sandbox_id, path).await,
        }
    }

    async fn delete_path(&self, sandbox_id: &str, path: &str) -> Result<()> {
        match self {
            Self::OpenComputer(control) => control.delete_path(sandbox_id, path).await,
            Self::Freestyle(control) => control.delete_path(sandbox_id, path).await,
        }
    }

    async fn exec(&self, sandbox_id: &str, command: &str, opts: ExecOptions) -> Result<ExecHandle> {
        match self {
            Self::OpenComputer(control) => control.exec(sandbox_id, command, opts).await,
            Self::Freestyle(control) => control.exec(sandbox_id, command, opts).await,
        }
    }

    async fn shell(&self, sandbox_id: &str, opts: ShellOptions) -> Result<ShellHandle> {
        match self {
            Self::OpenComputer(control) => control.shell(sandbox_id, opts).await,
            Self::Freestyle(control) => control.shell(sandbox_id, opts).await,
        }
    }

    async fn create_checkpoint(&self, sandbox_id: &str, name: &str) -> Result<String> {
        match self {
            Self::OpenComputer(control) => control
                .create_checkpoint(sandbox_id, name)
                .await
                .map(|c| c.id),
            Self::Freestyle(control) => control
                .create_checkpoint(sandbox_id, name)
                .await
                .map(|c| c.id),
        }
    }

    async fn delete_checkpoint(&self, checkpoint_id: &str) -> Result<()> {
        match self {
            Self::OpenComputer(_) => Err(SandboxError::Unsupported(
                "delete_snapshot is not available for OpenComputer sandboxes".to_string(),
            )),
            Self::Freestyle(control) => control.delete_checkpoint(checkpoint_id).await,
        }
    }

    async fn create_from_checkpoint(
        &self,
        checkpoint_id: &str,
        metadata: HashMap<String, String>,
        egress_allowlist: Option<Vec<String>>,
        shared_mounts: &[SharedMount],
    ) -> Result<ManagedSandbox> {
        match self {
            Self::OpenComputer(control) => control
                .create_from_checkpoint(
                    checkpoint_id,
                    metadata,
                    None,
                    egress_allowlist,
                    shared_mounts,
                )
                .await
                .map(|sandbox| ManagedSandbox {
                    id: sandbox.sandbox_id,
                }),
            Self::Freestyle(control) => control
                .create_from_checkpoint(checkpoint_id, metadata, egress_allowlist, shared_mounts)
                .await
                .map(|vm| ManagedSandbox { id: vm.id }),
        }
    }

    async fn fork(
        &self,
        sandbox: Sandbox,
        parent: &Session,
        opts: ForkOptions,
    ) -> Result<ForkResult> {
        match self {
            Self::OpenComputer(control) => control.fork(sandbox, parent, opts).await,
            Self::Freestyle(control) => control.fork(sandbox, parent, opts).await,
        }
    }

    /// Public HTTPS URL for a guest port, created on demand where the provider needs a rule.
    async fn preview_url(&self, sandbox_id: &str, guest_port: u16) -> Result<String> {
        match self {
            Self::OpenComputer(control) => {
                let sandbox = control.get_sandbox(sandbox_id).await?;
                let preview = sandbox.preview_domain(guest_port).ok_or_else(|| {
                    SandboxError::Unsupported(
                        "OpenComputer did not return a preview domain for this sandbox".to_string(),
                    )
                })?;
                Ok(format!("https://{preview}"))
            }
            Self::Freestyle(control) => control.preview_url(sandbox_id, guest_port).await,
        }
    }

    async fn state(&self, sandbox_id: &str) -> Result<i32> {
        match self {
            Self::OpenComputer(control) => {
                control.get_sandbox(sandbox_id).await?;
                Ok(proto::vmd::v1::VmState::Running as i32)
            }
            Self::Freestyle(control) => Ok(control
                .get_sandbox(sandbox_id)
                .await?
                .state
                .as_proto_state()),
        }
    }

    async fn vm_action(&self, sandbox_id: &str, action: SessionVmAction) -> Result<i32> {
        match self {
            Self::OpenComputer(_) => Err(SandboxError::Unsupported(format!(
                "{action:?} is not available for OpenComputer sandboxes"
            ))),
            Self::Freestyle(control) => {
                let action = match action {
                    SessionVmAction::Start | SessionVmAction::Resume => {
                        freestyle::FreestyleVmAction::Start
                    }
                    SessionVmAction::Pause => freestyle::FreestyleVmAction::Pause,
                    SessionVmAction::Stop => freestyle::FreestyleVmAction::Stop,
                    SessionVmAction::Restart => {
                        control
                            .vm_action(sandbox_id, freestyle::FreestyleVmAction::Stop)
                            .await?;
                        freestyle::FreestyleVmAction::Start
                    }
                };
                control.vm_action(sandbox_id, action).await
            }
        }
    }
}

#[derive(Clone)]
pub struct Session {
    sandbox: Sandbox,
    session_id: String,
    vm_id: String,
    workspace_root: String,
    node_endpoint: Arc<Mutex<String>>,
    ownership_fence: Arc<Mutex<Option<String>>>,
    shared_mounts: Arc<Vec<SharedMount>>,
    desktop_forward: Arc<Mutex<Option<ForwardHandle>>>,
}

/// What the facade may do when a running VM misses its guest RPC readiness budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadinessRecovery {
    /// The VM was created/started within the current operation: report the miss
    /// verbatim and leave the boot in progress. Never restart.
    FreshBoot,
    /// The VM may have been running for a long time with a stale sidecar: allow one
    /// bounded restart, but only if this call did not itself start/resume the VM.
    RestartIfNotFreshlyStarted,
}

/// Result of [`Sandbox::ensure_vm_running_tracked`].
struct EnsuredVm {
    vm: Vm,
    /// True when this call issued `StartVm`/`ResumeVm`; false when the VM was already running.
    freshly_started: bool,
}

#[derive(Clone)]
struct GuestRpcAccess {
    endpoint: String,
    rpc_port: i32,
    auth_header: Option<MetadataValue<Ascii>>,
    platform: GuestPlatform,
}

struct PortproxyClientAccess {
    client: PortProxyClient<Channel>,
    auth_header: Option<MetadataValue<Ascii>>,
}

struct PortproxyChannelEntry {
    endpoint: String,
    channel: OnceCell<Channel>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RebindRestorePolicy {
    RequireTierBRestoreMarker,
    #[allow(dead_code)]
    AllowLiveReclaimWithoutRestoreMarker,
}

#[derive(Clone, Copy, Debug)]
enum SessionVmAction {
    Start,
    Restart,
    Pause,
    Resume,
    Stop,
}

impl RebindRestorePolicy {
    fn enforce_tier_b_restore_marker(self) -> bool {
        matches!(self, Self::RequireTierBRestoreMarker)
    }

    fn preflight_candidate_runtime(self) -> bool {
        matches!(self, Self::RequireTierBRestoreMarker)
    }
}

impl Session {
    async fn sync_guest_filesystems(&self) -> Result<()> {
        let mut handle = self
            .exec(
                "sync",
                ExecOptions {
                    timeout_secs: Some(30),
                    close_stdin_on_start: true,
                    ..ExecOptions::default()
                },
            )
            .await?;
        while let Some(event) = handle.events.next().await {
            match event? {
                ExecEvent::Exit(0) => return Ok(()),
                ExecEvent::Exit(code) => {
                    return Err(SandboxError::InvalidResponse(format!(
                        "guest filesystem sync exited with status {code}"
                    )));
                }
                ExecEvent::Timeout => {
                    return Err(SandboxError::DaemonUnavailable(
                        "guest filesystem sync timed out before snapshot".to_string(),
                    ));
                }
                ExecEvent::Stdout(_) | ExecEvent::Stderr(_) => {}
            }
        }
        Err(SandboxError::InvalidResponse(
            "guest filesystem sync ended without an exit status".to_string(),
        ))
    }

    pub(crate) fn new_with_backend(
        sandbox: Sandbox,
        session_id: String,
        vm_id: String,
        node_endpoint: String,
        ownership_fence: Option<String>,
        shared_mounts: Vec<SharedMount>,
    ) -> Self {
        let workspace_root = shared_mounts
            .first()
            .map(|mount| mount.guest_path.trim())
            .filter(|path| !path.is_empty())
            .unwrap_or("/workspace")
            .to_string();
        Self {
            sandbox,
            session_id,
            vm_id,
            workspace_root,
            node_endpoint: Arc::new(Mutex::new(node_endpoint)),
            ownership_fence: Arc::new(Mutex::new(ownership_fence)),
            shared_mounts: Arc::new(shared_mounts),
            desktop_forward: Arc::new(Mutex::new(None)),
        }
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn vm_id(&self) -> &str {
        &self.vm_id
    }

    pub fn workspace_root(&self) -> &str {
        &self.workspace_root
    }

    // @dive: Exposes the currently resolved owner endpoint so higher-level integrations can
    // persist the real routed node instead of pinning follow-up control-plane calls to a gateway.
    pub async fn resolved_endpoint(&self) -> Result<String> {
        if matches!(
            &self.sandbox.inner.control_backend,
            ControlBackend::Managed(_)
        ) {
            return Ok(self.current_node_endpoint().await);
        }
        self.resolve_session_endpoint().await
    }

    async fn ownership_fence(&self) -> Option<String> {
        self.ownership_fence.lock().await.clone()
    }

    async fn current_node_endpoint(&self) -> String {
        self.node_endpoint.lock().await.clone()
    }

    async fn update_route_state(
        &self,
        previous_endpoint: &str,
        resolved_endpoint: &str,
        next_fence: Option<String>,
    ) {
        if previous_endpoint != resolved_endpoint {
            let mut guard = self.node_endpoint.lock().await;
            *guard = resolved_endpoint.to_string();
        }
        if let Some(fence) = next_fence {
            let mut guard = self.ownership_fence.lock().await;
            *guard = Some(fence);
        }
    }

    async fn resolve_session_endpoint(&self) -> Result<String> {
        let current_endpoint = self.current_node_endpoint().await;
        let expected_fence = self.ownership_fence().await;
        let (resolved_endpoint, next_fence) = self
            .sandbox
            .resolve_session_endpoint(
                &self.session_id,
                &self.vm_id,
                &current_endpoint,
                expected_fence.as_deref(),
            )
            .await?;
        self.update_route_state(&current_endpoint, &resolved_endpoint, next_fence)
            .await;
        Ok(resolved_endpoint)
    }

    #[allow(dead_code)]
    async fn resolve_session_endpoint_for_active_stream(&self) -> Result<String> {
        let current_endpoint = self.current_node_endpoint().await;
        let expected_fence = self.ownership_fence().await;
        let (resolved_endpoint, next_fence) = self
            .sandbox
            .resolve_session_endpoint_for_active_stream(
                &self.session_id,
                &self.vm_id,
                &current_endpoint,
                expected_fence.as_deref(),
            )
            .await?;
        self.update_route_state(&current_endpoint, &resolved_endpoint, next_fence)
            .await;
        Ok(resolved_endpoint)
    }

    async fn ensure_session_rpc_access(&self) -> Result<GuestRpcAccess> {
        let current_endpoint = self.current_node_endpoint().await;
        let expected_fence = self.ownership_fence().await;
        let (resolved_endpoint, access, next_fence) = self
            .sandbox
            .ensure_vm_and_get_rpc_access_for_session(
                &self.session_id,
                &self.vm_id,
                &current_endpoint,
                expected_fence.as_deref(),
            )
            .await?;
        self.update_route_state(&current_endpoint, &resolved_endpoint, next_fence)
            .await;
        Ok(access)
    }

    async fn portproxy_client_access(&self) -> Result<PortproxyClientAccess> {
        // @dive: File RPCs share exec's cached session-access fast path. Calling
        // resolve_session_endpoint first would force vmd.GetVm (including its
        // runtime network snapshot) into every read/list/write hot path even
        // when this VM route is already proven ready.
        let access = self.ensure_session_rpc_access().await?;
        let endpoint = self.current_node_endpoint().await;
        self.sandbox
            .portproxy_client_for_access(&self.vm_id, &endpoint, access)
            .await
    }

    async fn invalidate_exec_transport_path(&self, endpoint: &str) {
        self.sandbox
            .invalidate_ready_vm_rpc(&self.vm_id, endpoint)
            .await;
    }

    async fn recover_exec_transport_path(&self, endpoint: &str) {
        self.invalidate_exec_transport_path(endpoint).await;
        let _ = self
            .sandbox
            .restart_vm_on_endpoint(&self.vm_id, endpoint, "exec transport recovery")
            .await;
    }

    pub async fn exec(&self, command: &str, opts: ExecOptions) -> Result<ExecHandle> {
        if let ControlBackend::Managed(control) = &self.sandbox.inner.control_backend {
            return control.exec(&self.vm_id, command, opts).await;
        }

        #[cfg(feature = "distributed-control")]
        if let ControlBackend::Distributed(control) = &self.sandbox.inner.control_backend {
            // @dive: Distributed mode keeps the facade API stable by routing exec via control commands/events instead of direct guest RPC sockets.
            return self.exec_via_control_plane(command, opts, control).await;
        }

        let started = Instant::now();
        for establish_attempt in 0..3 {
            let access = match self.ensure_session_rpc_access().await {
                Ok(access) => access,
                Err(err) => {
                    if is_rebind_candidate_error(&err) && establish_attempt < 2 {
                        let fallback_endpoint = self.current_node_endpoint().await;
                        if establish_attempt == 0 {
                            self.invalidate_exec_transport_path(fallback_endpoint.as_str())
                                .await;
                        } else {
                            self.recover_exec_transport_path(fallback_endpoint.as_str())
                                .await;
                        }
                        continue;
                    }
                    return Err(err);
                }
            };
            let recovery_endpoint = self.current_node_endpoint().await;

            // An unbounded connect parks silently while the guest wakes or the
            // forwarder accepts; bound it so the establish loop can retry fast.
            // connect_timeout only — the exec stream itself is long-lived.
            let exec_endpoint = Endpoint::from_shared(access.endpoint.clone())
                .map_err(|err| SandboxError::InvalidEndpoint(err.to_string()))?
                .connect_timeout(self.sandbox.inner.cfg.connect_timeout);
            let connect_started = Instant::now();
            let mut client = match exec_endpoint.connect().await {
                Ok(channel) => {
                    let connect_elapsed = connect_started.elapsed();
                    if connect_elapsed > Duration::from_secs(2) {
                        tracing::warn!(
                            elapsed_ms = connect_elapsed.as_millis() as u64,
                            attempt = establish_attempt,
                            "slow exec transport establishment"
                        );
                    }
                    ShellExecClient::new(channel)
                }
                Err(err) => {
                    let err = SandboxError::Transport(err);
                    if is_rebind_candidate_error(&err) && establish_attempt < 2 {
                        if establish_attempt == 0 {
                            self.invalidate_exec_transport_path(recovery_endpoint.as_str())
                                .await;
                        } else {
                            self.recover_exec_transport_path(recovery_endpoint.as_str())
                                .await;
                        }
                        continue;
                    }
                    return Err(err);
                }
            };

            let args = guest_shell_args(
                access.platform,
                opts.shell.as_deref(),
                self.sandbox.inner.cfg.default_shell.as_str(),
                command,
            );

            let (req_tx, req_rx) = mpsc::channel(64);
            let execution_id = Uuid::new_v4().to_string();
            req_tx
                .send(ExecRequest {
                    request: Some(exec_request::Request::Start(ExecStart {
                        args,
                        env: opts.env.clone(),
                        detach: opts.detach,
                        timeout: opts.timeout_secs,
                        execution_id: execution_id.clone(),
                        run_as_root: false,
                    })),
                })
                .await
                .map_err(|_| {
                    SandboxError::InvalidResponse("failed to enqueue exec start".into())
                })?;
            let close_stdin_on_start = opts.close_stdin_on_start;
            let mut req_tx_for_stdin = Some(req_tx);
            if close_stdin_on_start {
                drop(req_tx_for_stdin.take());
            }

            let exec_result = tokio::time::timeout(
                self.sandbox.inner.cfg.connect_timeout,
                client.exec(request_with_optional_auth(
                    ReceiverStream::new(req_rx),
                    access.auth_header.as_ref(),
                )),
            )
            .await;

            let exec_response = match exec_result {
                Ok(Ok(response)) => response,
                Ok(Err(status)) => {
                    let err = SandboxError::Grpc(status);
                    if is_rebind_candidate_error(&err) && establish_attempt < 2 {
                        if establish_attempt == 0 {
                            self.invalidate_exec_transport_path(recovery_endpoint.as_str())
                                .await;
                        } else {
                            self.recover_exec_transport_path(recovery_endpoint.as_str())
                                .await;
                        }
                        continue;
                    }
                    return Err(err);
                }
                Err(_) => {
                    let err = SandboxError::DaemonUnavailable(
                        "timed out establishing exec stream against guest RPC".to_string(),
                    );
                    if establish_attempt < 2 {
                        if establish_attempt == 0 {
                            self.invalidate_exec_transport_path(recovery_endpoint.as_str())
                                .await;
                        } else {
                            self.recover_exec_transport_path(recovery_endpoint.as_str())
                                .await;
                        }
                        continue;
                    }
                    return Err(err);
                }
            };
            let mut stream = exec_response.into_inner();

            let (input_tx, mut input_rx) = mpsc::channel(64);
            let (control_tx, mut control_rx) = mpsc::channel(8);
            let (event_tx, event_rx) = mpsc::channel(128);

            let mut control_client = ShellExecClient::new(exec_endpoint.connect_lazy());
            let control_auth = access.auth_header.clone();
            let control_events = event_tx.clone();
            let mut input_tasks = tokio::task::JoinSet::new();
            input_tasks.spawn(async move {
                let mut control_seq = 0;
                while let Some(input) = control_rx.recv().await {
                    if let ExecInput::Signal(signal) = input {
                        control_seq += 1;
                        let request = proto::bracket::portproxy::v1::ExecControlRequest {
                            execution_id: execution_id.clone(),
                            producer_epoch: 0,
                            control_seq,
                            control: Some(proto::bracket::portproxy::v1::exec_control_request::Control::Signal(signal)),
                        };
                        let mut request =
                            request_with_optional_auth(request, control_auth.as_ref());
                        request.set_timeout(Duration::from_secs(5));
                        if let Err(error) = control_client.control_exec(request).await {
                            let already_exited = error.code() == tonic::Code::NotFound
                                || (error.code() == tonic::Code::FailedPrecondition
                                    && error.message() == "execution has exited");
                            if !already_exited {
                                let _ = control_events.send(Err(SandboxError::Grpc(error))).await;
                            }
                        }
                    }
                }
            });

            let event_tx_input = event_tx.clone();
            let input_control = control_tx.clone();
            if let Some(req_tx) = req_tx_for_stdin {
                input_tasks.spawn(async move {
                    while let Some(input) = input_rx.recv().await {
                        match input {
                            ExecInput::Data(data) => {
                                if req_tx
                                    .send(ExecRequest {
                                        request: Some(exec_request::Request::StdinData(data)),
                                    })
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            ExecInput::Eof => {
                                let _ = req_tx
                                    .send(ExecRequest {
                                        request: Some(exec_request::Request::StdinEof(true)),
                                    })
                                    .await;
                                break;
                            }
                            ExecInput::Signal(signal) => {
                                let _ = input_control.send(ExecInput::Signal(signal)).await;
                            }
                            ExecInput::Resize { cols, rows } => {
                                let _ = event_tx_input
                                    .send(Err(SandboxError::Unsupported(format!(
                                        "exec resize not supported by portproxy API: {cols}x{rows}"
                                    ))))
                                    .await;
                            }
                        }
                    }
                    drop(req_tx);
                });
            } else {
                input_tasks.spawn(async move {
                    while let Some(input) = input_rx.recv().await {
                        match input {
                            ExecInput::Data(_) | ExecInput::Eof => {}
                            ExecInput::Signal(signal) => {
                                let _ = input_control.send(ExecInput::Signal(signal)).await;
                            }
                            ExecInput::Resize { cols, rows } => {
                                let _ = event_tx_input
                                    .send(Err(SandboxError::Unsupported(format!(
                                        "exec resize not supported by portproxy API: {cols}x{rows}"
                                    ))))
                                    .await;
                            }
                        }
                    }
                });
            }

            let event_tx_stream = event_tx.clone();
            tokio::spawn(async move {
                loop {
                    match stream.message().await {
                        Ok(Some(ExecResponse {
                            response: Some(exec_response::Response::StdoutData(data)),
                        })) => {
                            if event_tx_stream
                                .send(Ok(ExecEvent::Stdout(data)))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        Ok(Some(ExecResponse {
                            response: Some(exec_response::Response::StderrData(data)),
                        })) => {
                            if event_tx_stream
                                .send(Ok(ExecEvent::Stderr(data)))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        Ok(Some(ExecResponse {
                            response: Some(exec_response::Response::ExitCode(code)),
                        })) => {
                            if code == 124 {
                                let _ = event_tx_stream.send(Ok(ExecEvent::Timeout)).await;
                            }
                            let _ = event_tx_stream.send(Ok(ExecEvent::Exit(code))).await;
                            break;
                        }
                        Ok(Some(_)) => {}
                        Ok(None) => break,
                        Err(status) => {
                            let _ = event_tx_stream.send(Err(SandboxError::Grpc(status))).await;
                            break;
                        }
                    }
                }
                drop(input_tasks);
            });

            let handle = ExecHandle {
                input: ExecInputSender {
                    data: input_tx,
                    control: control_tx,
                },
                events: Box::pin(ReceiverStream::new(event_rx)),
            };
            log_slo_observation("exec.stream.establish.warm_vm", started.elapsed(), "ok");
            return Ok(handle);
        }

        Err(SandboxError::DaemonUnavailable(
            "failed to establish exec stream after transport recovery attempts".to_string(),
        ))
    }

    #[cfg(feature = "distributed-control")]
    async fn resolve_distributed_exec_target(
        &self,
        control: &distributed::DistributedControlPlane,
        endpoint: &str,
    ) -> Result<String> {
        let route_node = control
            .get_session_route(self.session_id.as_str())
            .await?
            .and_then(|route| route.node_id);
        let endpoint_node = control.node_id_for_endpoint(endpoint).await?;
        endpoint_node.or(route_node).ok_or_else(|| {
            SandboxError::InvalidResponse(format!(
                "unable to resolve target node for distributed exec session_id={} endpoint={endpoint}",
                self.session_id
            ))
        })
    }

    #[cfg(feature = "distributed-control")]
    async fn begin_distributed_exec_stream(
        &self,
        params: DistributedExecStartParams<'_>,
    ) -> Result<DistributedExecStreamState> {
        let DistributedExecStartParams {
            command,
            opts,
            control,
            endpoint,
            target_node_id,
            start_wait,
            idle_timeout,
        } = params;
        let timeout_secs = opts.timeout_secs.unwrap_or(30).max(1);
        let shell = opts
            .shell
            .clone()
            .unwrap_or_else(|| self.sandbox.inner.cfg.default_shell.clone());
        let stream_id = Uuid::new_v4().to_string();
        let start_idempotency_key = format!("exec-stream-start-{stream_id}");
        let expected_fence = self.ownership_fence().await;
        let distributed::ExecStreamSubscription {
            events: stream_events,
            started,
        } = control
            .subscribe_exec_stream_events(stream_id.as_str(), idle_timeout, None, true)
            .await?;
        let payload = json!({
            "stream_id": stream_id.as_str(),
            "logical_stream_id": stream_id.as_str(),
            "cluster_id": control.cluster_id(),
            "producer_epoch": 0u64,
            "resume_after_event_seq": 0u64,
            "session_id": self.session_id.as_str(),
            "vm_id": self.vm_id.as_str(),
            "endpoint": endpoint.as_str(),
            "target_node_id": target_node_id.as_str(),
            "command": command,
            "env": opts.env.clone(),
            "detach": opts.detach,
            "shell": shell,
            "timeout_secs": timeout_secs,
            "timeout_ms": (timeout_secs as u64) * 1000,
            "idempotency_key": start_idempotency_key.as_str(),
            "expected_fence": expected_fence.as_deref(),
        });

        let command_id = control
            .publish_command(
                "exec.stream.start",
                format!("exec-stream-start:{stream_id}").as_str(),
                payload,
            )
            .await?;
        match tokio::time::timeout(start_wait, started).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(err))) => return Err(err),
            Ok(Err(_)) => {
                return Err(SandboxError::DaemonUnavailable(format!(
                    "distributed exec stream did not become ready (dropped readiness signal) command_id={command_id} stream_id={stream_id}"
                )));
            }
            Err(_) => {
                return Err(SandboxError::DaemonUnavailable(format!(
                    "distributed exec stream did not become ready before timeout command_id={command_id} stream_id={stream_id}"
                )));
            }
        }

        Ok(DistributedExecStreamState {
            endpoint,
            target_node_id,
            stream_id,
            producer_epoch: 0,
            events: stream_events,
        })
    }

    #[cfg(feature = "distributed-control")]
    async fn try_rebind_distributed_exec_stream(
        &self,
        params: DistributedExecRebindParams<'_>,
    ) -> Result<Option<DistributedExecStreamState>> {
        let DistributedExecRebindParams {
            control,
            command,
            opts,
            start_wait,
            current_endpoint,
            logical_stream_id,
            resume_after_event_seq,
            idle_timeout,
            current_target_node_id,
            current_producer_epoch,
        } = params;
        let resolved_endpoint = self.resolve_session_endpoint_for_active_stream().await?;
        let endpoint_changed = resolved_endpoint != current_endpoint;
        let target_node_id = match self
            .resolve_distributed_exec_target(control, resolved_endpoint.as_str())
            .await
        {
            Ok(target_node_id) => target_node_id,
            Err(err) if endpoint_changed => return Err(err),
            Err(_) => current_target_node_id.to_string(),
        };
        let target_changed = target_node_id != current_target_node_id;
        let producer_epoch = if endpoint_changed || target_changed {
            current_producer_epoch.saturating_add(1)
        } else {
            current_producer_epoch
        };
        let needs_command_resume = endpoint_changed || target_changed;
        let distributed::ExecStreamSubscription {
            events: stream_events,
            started,
        } = control
            .subscribe_exec_stream_events(
                logical_stream_id,
                idle_timeout,
                Some(resume_after_event_seq),
                needs_command_resume,
            )
            .await?;
        if !needs_command_resume {
            return Ok(Some(DistributedExecStreamState {
                endpoint: resolved_endpoint,
                target_node_id,
                stream_id: logical_stream_id.to_string(),
                producer_epoch,
                events: stream_events,
            }));
        }
        let timeout_secs = opts.timeout_secs.unwrap_or(30).max(1);
        let shell = opts
            .shell
            .clone()
            .unwrap_or_else(|| self.sandbox.inner.cfg.default_shell.clone());
        let resume_idempotency_key = format!(
            "exec-stream-resume-{logical_stream_id}-{producer_epoch}-{resume_after_event_seq}"
        );
        let expected_fence = self.ownership_fence().await;
        let payload = json!({
            "stream_id": logical_stream_id,
            "logical_stream_id": logical_stream_id,
            "cluster_id": control.cluster_id(),
            "producer_epoch": producer_epoch,
            "resume_after_event_seq": resume_after_event_seq,
            "session_id": self.session_id.as_str(),
            "vm_id": self.vm_id.as_str(),
            "endpoint": resolved_endpoint.as_str(),
            "target_node_id": target_node_id.as_str(),
            "command": command,
            "env": opts.env.clone(),
            "detach": opts.detach,
            "shell": shell,
            "timeout_secs": timeout_secs,
            "timeout_ms": (timeout_secs as u64) * 1000,
            "idempotency_key": resume_idempotency_key.as_str(),
            "expected_fence": expected_fence.as_deref(),
        });
        let command_id = control
            .publish_command(
                "exec.stream.start",
                format!("exec-stream-resume:{logical_stream_id}:{resume_after_event_seq}").as_str(),
                payload,
            )
            .await?;
        match tokio::time::timeout(start_wait, started).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(err))) => return Err(err),
            Ok(Err(_)) => {
                return Err(SandboxError::DaemonUnavailable(format!(
                    "distributed exec stream resume did not become ready (dropped readiness signal) logical_stream_id={logical_stream_id} command_id={command_id}"
                )));
            }
            Err(_) => {
                return Err(SandboxError::DaemonUnavailable(format!(
                    "distributed exec stream resume did not become ready before timeout logical_stream_id={logical_stream_id} command_id={command_id}"
                )));
            }
        }
        Ok(Some(DistributedExecStreamState {
            endpoint: resolved_endpoint,
            target_node_id,
            stream_id: logical_stream_id.to_string(),
            producer_epoch,
            events: stream_events,
        }))
    }

    #[cfg(feature = "distributed-control")]
    async fn exec_via_control_plane(
        &self,
        command: &str,
        opts: ExecOptions,
        control: &distributed::DistributedControlPlane,
    ) -> Result<ExecHandle> {
        if command.trim().is_empty() {
            return Err(SandboxError::InvalidResponse(
                "exec command must be non-empty".to_string(),
            ));
        }

        let endpoint = self.resolve_session_endpoint().await?;
        let start_wait = std::cmp::max(
            self.sandbox.inner.cfg.connect_timeout,
            self.sandbox.inner.cfg.portproxy_ready_timeout,
        ) + Duration::from_secs(30);
        let timeout_secs = opts.timeout_secs.unwrap_or(30).max(1);
        let idle_timeout = Duration::from_secs(timeout_secs as u64 + 90);
        let target_node_id = self
            .resolve_distributed_exec_target(control, endpoint.as_str())
            .await?;
        let initial_state = match self
            .begin_distributed_exec_stream(DistributedExecStartParams {
                command,
                opts: &opts,
                control,
                endpoint: endpoint.clone(),
                target_node_id: target_node_id.clone(),
                start_wait,
                idle_timeout,
            })
            .await
        {
            Ok(state) => state,
            Err(err) => return Err(err),
        };

        let (input_tx, mut input_rx) = mpsc::channel(64);
        let (event_tx, event_rx) = mpsc::channel(128);
        let routing_state = Arc::new(Mutex::new((
            initial_state.stream_id.clone(),
            initial_state.target_node_id.clone(),
            initial_state.producer_epoch,
        )));

        let control_for_input = control.clone();
        let (control_tx, mut control_rx) = mpsc::channel(8);
        let control_for_signals = control.clone();
        let routing_for_signals = routing_state.clone();
        let signal_session = self.session_id.clone();
        let signal_vm = self.vm_id.clone();
        let signal_errors = event_tx.clone();
        let mut input_tasks = tokio::task::JoinSet::new();
        input_tasks.spawn(async move {
            let mut control_seq = 0u64;
            while let Some(ExecInput::Signal(signal)) = control_rx.recv().await {
                control_seq += 1;
                let (stream_id, target_node_id, producer_epoch) =
                    routing_for_signals.lock().await.clone();
                let request_id = Uuid::new_v4().to_string();
                let payload = json!({
                    "stream_id": stream_id,
                    "session_id": signal_session,
                    "vm_id": signal_vm,
                    "target_node_id": target_node_id,
                    "producer_epoch": producer_epoch,
                    "input_kind": "signal",
                    "signal": signal,
                    "input_seq": control_seq,
                    "idempotency_key": request_id,
                });
                if let Err(error) = control_for_signals
                    .publish_command("exec.stream.control", &request_id, payload)
                    .await
                {
                    let _ = signal_errors.send(Err(error)).await;
                }
            }
        });
        let routing_for_input = routing_state.clone();
        let session_id_input = self.session_id.clone();
        let vm_id_input = self.vm_id.clone();
        let event_tx_input = event_tx.clone();
        let input_control = control_tx.clone();
        input_tasks.spawn(async move {
            let mut input_seq = 0u64;
            let mut previous_route = None;
            while let Some(input) = input_rx.recv().await {
                let (stream_id_input, target_node_id_input, producer_epoch_input) = {
                    let guard = routing_for_input.lock().await;
                    (guard.0.clone(), guard.1.clone(), guard.2)
                };
                let route = (target_node_id_input.clone(), producer_epoch_input);
                if previous_route.as_ref() != Some(&route) {
                    input_seq = 0;
                    previous_route = Some(route);
                }
                match input {
                    ExecInput::Eof => {
                        input_seq = input_seq.saturating_add(1);
                        let payload = json!({
                            "stream_id": stream_id_input.as_str(),
                            "session_id": session_id_input.as_str(),
                            "vm_id": vm_id_input.as_str(),
                            "target_node_id": target_node_id_input.as_str(),
                            "input_seq": input_seq,
                            "input_kind": "eof",
                            "producer_epoch": producer_epoch_input,
                            "idempotency_key": format!("exec-stream-input-{stream_id_input}-{producer_epoch_input}-{input_seq}"),
                        });
                        if let Err(err) = control_for_input
                            .publish_command(
                                "exec.stream.input",
                                format!("exec-stream-input:{stream_id_input}:{producer_epoch_input}:{input_seq}").as_str(),
                                payload,
                            )
                            .await
                        {
                            let _ = event_tx_input.send(Err(err)).await;
                        }
                        break;
                    }
                    ExecInput::Signal(signal) => {
                        let _ = input_control.send(ExecInput::Signal(signal)).await;
                    }
                    ExecInput::Resize { cols, rows } => {
                        let _ = event_tx_input
                            .send(Err(SandboxError::Unsupported(format!(
                                "exec resize not supported by portproxy API: {cols}x{rows}"
                            ))))
                            .await;
                    }
                    ExecInput::Data(bytes) => {
                        input_seq = input_seq.saturating_add(1);
                        let payload = json!({
                            "stream_id": stream_id_input.as_str(),
                            "session_id": session_id_input.as_str(),
                            "vm_id": vm_id_input.as_str(),
                            "target_node_id": target_node_id_input.as_str(),
                            "input_seq": input_seq,
                            "input_kind": "stdin",
                            "data": bytes,
                            "producer_epoch": producer_epoch_input,
                            "idempotency_key": format!("exec-stream-input-{stream_id_input}-{producer_epoch_input}-{input_seq}"),
                        });
                        if let Err(err) = control_for_input
                            .publish_command(
                                "exec.stream.input",
                                format!("exec-stream-input:{stream_id_input}:{producer_epoch_input}:{input_seq}").as_str(),
                                payload,
                            )
                            .await
                        {
                            let _ = event_tx_input.send(Err(err)).await;
                            break;
                        }
                    }
                }
            }
        });

        let session_for_events = self.clone();
        let control_for_events = control.clone();
        let routing_for_events = routing_state.clone();
        let command_for_rebind = command.to_string();
        let opts_for_rebind = opts.clone();
        let start_wait_for_rebind = start_wait;
        let mut stream_events = initial_state.events;
        let mut active_endpoint = initial_state.endpoint;
        let mut active_stream_id = initial_state.stream_id;
        let mut active_target_node_id = initial_state.target_node_id;
        let mut active_producer_epoch = initial_state.producer_epoch;
        tokio::spawn(async move {
            let mut last_committed_event_seq = 0u64;
            let mut stall_recovery_attempts: u32 = 0;
            let mut idle_timeout_strikes: u32 = 0;
            let rebind_wait_timeout = start_wait_for_rebind + Duration::from_secs(5);
            const MAX_STALL_RECOVERY_ATTEMPTS: u32 = 5;
            const REBIND_IDLE_TIMEOUT_STRIKES: u32 = 3;
            loop {
                let next = tokio::time::timeout(Duration::from_secs(2), stream_events.recv()).await;
                let maybe_event_result = match next {
                    Ok(value) => {
                        idle_timeout_strikes = 0;
                        value
                    }
                    Err(_) => {
                        idle_timeout_strikes = idle_timeout_strikes.saturating_add(1);
                        if idle_timeout_strikes < REBIND_IDLE_TIMEOUT_STRIKES {
                            continue;
                        }
                        idle_timeout_strikes = 0;
                        match tokio::time::timeout(
                            rebind_wait_timeout,
                            session_for_events.try_rebind_distributed_exec_stream(
                                DistributedExecRebindParams {
                                    control: &control_for_events,
                                    command: command_for_rebind.as_str(),
                                    opts: &opts_for_rebind,
                                    start_wait: start_wait_for_rebind,
                                    current_endpoint: active_endpoint.as_str(),
                                    logical_stream_id: active_stream_id.as_str(),
                                    resume_after_event_seq: last_committed_event_seq,
                                    idle_timeout,
                                    current_target_node_id: active_target_node_id.as_str(),
                                    current_producer_epoch: active_producer_epoch,
                                },
                            ),
                        )
                        .await
                        {
                            Ok(Ok(Some(rebound))) => {
                                active_endpoint = rebound.endpoint.clone();
                                active_stream_id = rebound.stream_id.clone();
                                active_target_node_id = rebound.target_node_id.clone();
                                active_producer_epoch = rebound.producer_epoch;
                                {
                                    let mut guard = routing_for_events.lock().await;
                                    *guard = (
                                        rebound.stream_id.clone(),
                                        rebound.target_node_id,
                                        rebound.producer_epoch,
                                    );
                                }
                                stream_events = rebound.events;
                                stall_recovery_attempts = 0;
                                idle_timeout_strikes = 0;
                            }
                            Ok(Ok(None)) | Ok(Err(_)) | Err(_) => {
                                stall_recovery_attempts = stall_recovery_attempts.saturating_add(1);
                                if stall_recovery_attempts >= MAX_STALL_RECOVERY_ATTEMPTS {
                                    let _ = event_tx
                                        .send(Err(SandboxError::DaemonUnavailable(
                                            "distributed exec stream stalled after recovery attempts; command was not replayed"
                                                .to_string(),
                                        )))
                                        .await;
                                    break;
                                }
                                let _ = event_tx.send(Ok(ExecEvent::Timeout)).await;
                            }
                        }
                        continue;
                    }
                };
                let Some(event_result) = maybe_event_result else {
                    match tokio::time::timeout(
                        rebind_wait_timeout,
                        session_for_events.try_rebind_distributed_exec_stream(
                            DistributedExecRebindParams {
                                control: &control_for_events,
                                command: command_for_rebind.as_str(),
                                opts: &opts_for_rebind,
                                start_wait: start_wait_for_rebind,
                                current_endpoint: active_endpoint.as_str(),
                                logical_stream_id: active_stream_id.as_str(),
                                resume_after_event_seq: last_committed_event_seq,
                                idle_timeout,
                                current_target_node_id: active_target_node_id.as_str(),
                                current_producer_epoch: active_producer_epoch,
                            },
                        ),
                    )
                    .await
                    {
                        Ok(Ok(Some(rebound))) => {
                            active_endpoint = rebound.endpoint.clone();
                            active_stream_id = rebound.stream_id.clone();
                            active_target_node_id = rebound.target_node_id.clone();
                            active_producer_epoch = rebound.producer_epoch;
                            {
                                let mut guard = routing_for_events.lock().await;
                                *guard = (
                                    rebound.stream_id.clone(),
                                    rebound.target_node_id,
                                    rebound.producer_epoch,
                                );
                            }
                            stream_events = rebound.events;
                            stall_recovery_attempts = 0;
                            idle_timeout_strikes = 0;
                            continue;
                        }
                        Ok(Ok(None)) | Ok(Err(_)) | Err(_) => {
                            stall_recovery_attempts = stall_recovery_attempts.saturating_add(1);
                            if stall_recovery_attempts >= MAX_STALL_RECOVERY_ATTEMPTS {
                                let _ = event_tx
                                    .send(Err(SandboxError::DaemonUnavailable(
                                        "distributed exec stream ended and recovery exhausted; command was not replayed"
                                            .to_string(),
                                    )))
                                    .await;
                                break;
                            }
                            let _ = event_tx.send(Ok(ExecEvent::Timeout)).await;
                            continue;
                        }
                    }
                };
                let event = match event_result {
                    Ok(event) => event,
                    Err(err) => {
                        match tokio::time::timeout(
                            rebind_wait_timeout,
                            session_for_events.try_rebind_distributed_exec_stream(
                                DistributedExecRebindParams {
                                    control: &control_for_events,
                                    command: command_for_rebind.as_str(),
                                    opts: &opts_for_rebind,
                                    start_wait: start_wait_for_rebind,
                                    current_endpoint: active_endpoint.as_str(),
                                    logical_stream_id: active_stream_id.as_str(),
                                    resume_after_event_seq: last_committed_event_seq,
                                    idle_timeout,
                                    current_target_node_id: active_target_node_id.as_str(),
                                    current_producer_epoch: active_producer_epoch,
                                },
                            ),
                        )
                        .await
                        {
                            Ok(Ok(Some(rebound))) => {
                                active_endpoint = rebound.endpoint.clone();
                                active_stream_id = rebound.stream_id.clone();
                                active_target_node_id = rebound.target_node_id.clone();
                                active_producer_epoch = rebound.producer_epoch;
                                {
                                    let mut guard = routing_for_events.lock().await;
                                    *guard = (
                                        rebound.stream_id.clone(),
                                        rebound.target_node_id,
                                        rebound.producer_epoch,
                                    );
                                }
                                stream_events = rebound.events;
                                stall_recovery_attempts = 0;
                                idle_timeout_strikes = 0;
                                continue;
                            }
                            Ok(Ok(None)) | Ok(Err(_)) | Err(_) => {
                                stall_recovery_attempts = stall_recovery_attempts.saturating_add(1);
                                if stall_recovery_attempts >= MAX_STALL_RECOVERY_ATTEMPTS {
                                    let _ = event_tx.send(Err(err)).await;
                                    break;
                                }
                                let _ = event_tx.send(Ok(ExecEvent::Timeout)).await;
                                continue;
                            }
                        }
                    }
                };
                let event_seq = event.normalized_event_seq();
                if event_seq > 0 {
                    // @dive: Stream checkpoints enforce no-replay delivery after rebind/resubscribe by suppressing seq <= last committed.
                    if event_seq <= last_committed_event_seq {
                        continue;
                    }
                    last_committed_event_seq = event_seq;
                }
                stall_recovery_attempts = 0;
                idle_timeout_strikes = 0;
                match event.kind.as_str() {
                    "started" => {}
                    "stdout" => {
                        if event_tx
                            .send(Ok(ExecEvent::Stdout(event.data)))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    "stderr" => {
                        if event_tx
                            .send(Ok(ExecEvent::Stderr(event.data)))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    "timeout" => {
                        if event_tx.send(Ok(ExecEvent::Timeout)).await.is_err() {
                            break;
                        }
                    }
                    "exit" => {
                        let code =
                            event
                                .exit_code
                                .or(if event.timed_out { Some(124) } else { None });
                        if let Some(code) = code {
                            let _ = event_tx.send(Ok(ExecEvent::Exit(code))).await;
                        } else {
                            let _ = event_tx
                                .send(Err(SandboxError::InvalidResponse(
                                    "distributed exec stream exit event missing exit code"
                                        .to_string(),
                                )))
                                .await;
                        }
                        break;
                    }
                    "error" => {
                        // @dive: Producer terminal errors are surfaced immediately; command replay is intentionally disallowed on this path.
                        let message = event
                            .error
                            .filter(|value| !value.trim().is_empty())
                            .unwrap_or_else(|| {
                                "distributed exec stream failed with unspecified error".to_string()
                            });
                        let _ = event_tx
                            .send(Err(SandboxError::DaemonUnavailable(message)))
                            .await;
                        break;
                    }
                    _ => {}
                }
            }
            drop(input_tasks);
        });

        Ok(ExecHandle {
            input: ExecInputSender {
                data: input_tx,
                control: control_tx,
            },
            events: Box::pin(ReceiverStream::new(event_rx)),
        })
    }

    pub async fn shell(&self, opts: ShellOptions) -> Result<ShellHandle> {
        if let ControlBackend::Managed(control) = &self.sandbox.inner.control_backend {
            return control.shell(&self.vm_id, opts).await;
        }

        let started = Instant::now();
        let access = self.ensure_session_rpc_access().await?;
        let mut client = ShellExecClient::connect(access.endpoint).await?;

        let shell = opts
            .shell
            .clone()
            .unwrap_or_else(|| self.sandbox.inner.cfg.default_shell.clone());

        let (req_tx, req_rx) = mpsc::channel(64);
        req_tx
            .send(InteractiveShellRequest {
                request: Some(interactive_shell_request::Request::Start(
                    InteractiveShellStart {
                        shell,
                        args: opts.args,
                        env: opts.env,
                        cwd: opts.cwd.unwrap_or_default(),
                    },
                )),
            })
            .await
            .map_err(|_| SandboxError::InvalidResponse("failed to enqueue shell start".into()))?;

        let shell_response = tokio::time::timeout(
            self.sandbox.inner.cfg.connect_timeout,
            client.interactive_shell(request_with_optional_auth(
                ReceiverStream::new(req_rx),
                access.auth_header.as_ref(),
            )),
        )
        .await
        .map_err(|_| {
            SandboxError::DaemonUnavailable(
                "timed out establishing interactive shell stream against guest RPC".to_string(),
            )
        })??;
        let mut stream = shell_response.into_inner();

        let (input_tx, mut input_rx) = mpsc::channel(64);
        let (event_tx, event_rx) = mpsc::channel(128);

        let req_tx_for_input = req_tx.clone();
        tokio::spawn(async move {
            let req_tx = req_tx_for_input;
            while let Some(input) = input_rx.recv().await {
                match input {
                    ShellInput::Data(data) => {
                        if req_tx
                            .send(InteractiveShellRequest {
                                request: Some(interactive_shell_request::Request::StdinData(data)),
                            })
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    ShellInput::Resize { cols, rows } => {
                        if req_tx
                            .send(InteractiveShellRequest {
                                request: Some(interactive_shell_request::Request::Resize(
                                    InteractiveShellResize {
                                        cols: u32::from(cols),
                                        rows: u32::from(rows),
                                    },
                                )),
                            })
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    ShellInput::Eof => break,
                }
            }
            drop(req_tx);
        });

        let event_tx_stream = event_tx.clone();
        tokio::spawn(async move {
            loop {
                match stream.message().await {
                    Ok(Some(InteractiveShellResponse {
                        response: Some(interactive_shell_response::Response::OutputData(data)),
                    })) => {
                        if event_tx_stream
                            .send(Ok(ShellEvent::Output(data)))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(Some(InteractiveShellResponse {
                        response: Some(interactive_shell_response::Response::ExitCode(code)),
                    })) => {
                        let _ = event_tx_stream.send(Ok(ShellEvent::Exit(code))).await;
                        break;
                    }
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(status) => {
                        let _ = event_tx_stream.send(Err(SandboxError::Grpc(status))).await;
                        break;
                    }
                }
            }
        });

        let handle = ShellHandle {
            input: input_tx,
            events: Box::pin(ReceiverStream::new(event_rx)),
        };
        log_slo_observation("shell.stream.establish.warm_vm", started.elapsed(), "ok");
        Ok(handle)
    }

    pub async fn read_file(&self, path: &str) -> Result<Vec<u8>> {
        if let ControlBackend::Managed(control) = &self.sandbox.inner.control_backend {
            return control.read_file(&self.vm_id, path).await;
        }

        let mut access = self.portproxy_client_access().await?;
        let response = access
            .client
            .read_file(request_with_optional_auth_timeout(
                ReadFileRequest {
                    path: path.to_string(),
                },
                access.auth_header.as_ref(),
                configured_portproxy_timeout(
                    "CHEVALIER_PORTPROXY_READ_FILE_TIMEOUT_MS",
                    DEFAULT_PORTPROXY_FILE_RPC_TIMEOUT,
                )?,
            ))
            .await?
            .into_inner();
        Ok(response.data)
    }

    pub async fn write_file(&self, path: &str, data: Vec<u8>) -> Result<()> {
        if let ControlBackend::Managed(control) = &self.sandbox.inner.control_backend {
            return control.write_file(&self.vm_id, path, data).await;
        }

        let mut access = self.portproxy_client_access().await?;
        access
            .client
            .write_file(request_with_optional_auth_timeout(
                WriteFileRequest {
                    path: path.to_string(),
                    data,
                    create_parents: true,
                },
                access.auth_header.as_ref(),
                configured_portproxy_timeout(
                    "CHEVALIER_PORTPROXY_WRITE_FILE_TIMEOUT_MS",
                    DEFAULT_PORTPROXY_WRITE_FILE_RPC_TIMEOUT,
                )?,
            ))
            .await?;
        Ok(())
    }

    /// Stream a host-local file into the guest and atomically replace `path`.
    ///
    /// This uses the portproxy file RPC directly rather than the distributed
    /// exec/stdin control stream, so large payloads stay binary end to end.
    pub async fn write_file_from_file(
        &self,
        path: &str,
        source_path: &str,
        mode: Option<u32>,
    ) -> Result<()> {
        if let ControlBackend::Managed(control) = &self.sandbox.inner.control_backend {
            let data = tokio::fs::read(source_path).await?;
            return control.write_file(&self.vm_id, path, data).await;
        }

        match self
            .write_file_from_file_once(path, source_path, mode)
            .await
        {
            Err(SandboxError::Grpc(status)) if portproxy_method_is_unimplemented(&status) => {
                // A retained VM can still be running the portproxy bundled by
                // the prior VMD release. Keep the public operation binary and
                // atomic: bounded unary file RPCs stage parts outside the VFS,
                // then one guest command assembles and renames the destination.
                // No payload bytes cross exec stdin or the control-command bus.
                self.write_file_from_file_legacy(path, source_path, mode)
                    .await
            }
            result => result,
        }
    }

    async fn write_file_from_file_legacy(
        &self,
        path: &str,
        source_path: &str,
        mode: Option<u32>,
    ) -> Result<()> {
        // Retained guests expose only the unary binary file RPC, whose payload
        // ceiling is 16 MiB. Stay comfortably below that ceiling without
        // paying one connection round trip per MiB; current guests use the
        // client-streaming RPC above and never take this compatibility path.
        const LEGACY_CHUNK_BYTES: usize = 12 * 1024 * 1024;

        let target = std::path::Path::new(path);
        if !target.is_absolute() {
            return Err(SandboxError::InvalidConfig(
                "legacy streamed guest writes require an absolute destination".to_string(),
            ));
        }
        let parent = target.parent().unwrap_or_else(|| std::path::Path::new("/"));
        let file_name = target
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("file");
        let transfer_id = Uuid::new_v4();
        let staging_path = parent.join(format!(
            ".{file_name}.openbracket-write-legacy-{transfer_id}"
        ));
        let staging_path = staging_path.to_string_lossy().into_owned();
        let mut source = tokio::fs::File::open(source_path).await?;
        let expected_bytes = source.metadata().await?.len();
        let mut parts: Vec<String> = Vec::new();
        let mut buffer = vec![0_u8; LEGACY_CHUNK_BYTES];
        loop {
            let read = source.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            let part = format!("/tmp/chevalier-file-{transfer_id}-{}", parts.len());
            if let Err(error) = self
                .write_file(part.as_str(), buffer[..read].to_vec())
                .await
            {
                for staged in &parts {
                    let _ = self.delete_path(staged).await;
                }
                return Err(error);
            }
            parts.push(part);
        }

        let quoted_parts = parts
            .iter()
            .map(|part| shell_single_quote(part))
            .collect::<Vec<_>>()
            .join(" ");
        let quoted_parent = shell_single_quote(parent.to_string_lossy().as_ref());
        let quoted_target = shell_single_quote(path);
        let quoted_staging = shell_single_quote(staging_path.as_str());
        let content_command = if parts.is_empty() {
            format!(": > {quoted_staging}")
        } else {
            format!("cat {quoted_parts} > {quoted_staging}")
        };
        let permission_command = match mode {
            Some(mode) => format!("chmod {:o} {quoted_staging}", mode & 0o7777),
            None => format!(
                "if [ -e {quoted_target} ]; then chmod --reference={quoted_target} {quoted_staging}; fi"
            ),
        };
        let remove_parts_command = if parts.is_empty() {
            ":".to_string()
        } else {
            format!("rm -f {quoted_parts}")
        };
        let command = format!(
            "set -eu; mkdir -p {quoted_parent}; {content_command}; test \"$(wc -c < {quoted_staging})\" = {expected_bytes}; {permission_command}; mv -f {quoted_staging} {quoted_target}; {remove_parts_command}"
        );

        let result = async {
            let mut handle = self
                .exec(
                    command.as_str(),
                    ExecOptions {
                        timeout_secs: Some(30),
                        close_stdin_on_start: true,
                        ..ExecOptions::default()
                    },
                )
                .await?;
            while let Some(event) = handle.events.next().await {
                match event? {
                    ExecEvent::Exit(0) => return Ok(()),
                    ExecEvent::Exit(code) => {
                        return Err(SandboxError::InvalidResponse(format!(
                            "legacy streamed guest write exited with status {code}"
                        )));
                    }
                    ExecEvent::Timeout => {
                        return Err(SandboxError::DaemonUnavailable(
                            "legacy streamed guest write timed out".to_string(),
                        ));
                    }
                    ExecEvent::Stdout(_) | ExecEvent::Stderr(_) => {}
                }
            }
            Err(SandboxError::InvalidResponse(
                "legacy streamed guest write ended without an exit status".to_string(),
            ))
        }
        .await;
        for staged in parts
            .iter()
            .map(String::as_str)
            .chain(std::iter::once(staging_path.as_str()))
        {
            let _ = self.delete_path(staged).await;
        }
        result
    }

    async fn write_file_from_file_once(
        &self,
        path: &str,
        source_path: &str,
        mode: Option<u32>,
    ) -> Result<()> {
        let expected_bytes = tokio::fs::metadata(source_path).await?.len();
        let source_path = source_path.to_string();
        let path = path.to_string();
        let (tx, rx) = mpsc::channel(4);
        let producer = tokio::spawn(async move {
            tx.send(WriteFileStreamRequest {
                request: Some(write_file_stream_request::Request::Start(
                    WriteFileStreamStart {
                        path,
                        create_parents: true,
                        expected_bytes,
                        mode,
                    },
                )),
            })
            .await
            .map_err(|_| SandboxError::InvalidResponse("file stream closed before start".into()))?;

            let mut file = tokio::fs::File::open(source_path).await?;
            let mut buffer = vec![0_u8; 512 * 1024];
            loop {
                let read = file.read(&mut buffer).await?;
                if read == 0 {
                    break;
                }
                tx.send(WriteFileStreamRequest {
                    request: Some(write_file_stream_request::Request::Data(
                        buffer[..read].to_vec(),
                    )),
                })
                .await
                .map_err(|_| {
                    SandboxError::InvalidResponse("file stream closed before completion".into())
                })?;
            }
            Ok::<(), SandboxError>(())
        });

        let mut access = self.portproxy_client_access().await?;
        let rpc = access
            .client
            .write_file_stream(request_with_optional_auth_timeout(
                ReceiverStream::new(rx),
                access.auth_header.as_ref(),
                configured_portproxy_timeout(
                    "CHEVALIER_PORTPROXY_WRITE_FILE_TIMEOUT_MS",
                    DEFAULT_PORTPROXY_WRITE_FILE_RPC_TIMEOUT,
                )?,
            ));
        let (rpc_result, producer_result) = tokio::join!(rpc, producer);
        rpc_result?;
        producer_result.map_err(|err| {
            SandboxError::InvalidResponse(format!("file stream task failed: {err}"))
        })??;
        Ok(())
    }

    pub async fn list_dir(
        &self,
        path: &str,
    ) -> Result<Vec<proto::bracket::portproxy::v1::DirectoryEntry>> {
        if let ControlBackend::Managed(control) = &self.sandbox.inner.control_backend {
            return control.list_dir(&self.vm_id, path).await;
        }

        let mut access = self.portproxy_client_access().await?;
        let response = access
            .client
            .list_directory(request_with_optional_auth_timeout(
                ListDirectoryRequest {
                    path: path.to_string(),
                },
                access.auth_header.as_ref(),
                configured_portproxy_timeout(
                    "CHEVALIER_PORTPROXY_LIST_DIRECTORY_TIMEOUT_MS",
                    DEFAULT_PORTPROXY_FILE_RPC_TIMEOUT,
                )?,
            ))
            .await?
            .into_inner();
        Ok(response.entries)
    }

    pub async fn delete_path(&self, path: &str) -> Result<()> {
        if let ControlBackend::Managed(control) = &self.sandbox.inner.control_backend {
            return control.delete_path(&self.vm_id, path).await;
        }

        let mut access = self.portproxy_client_access().await?;
        access
            .client
            .delete_path(request_with_optional_auth_timeout(
                DeletePathRequest {
                    path: path.to_string(),
                },
                access.auth_header.as_ref(),
                configured_portproxy_timeout(
                    "CHEVALIER_PORTPROXY_DELETE_PATH_TIMEOUT_MS",
                    DEFAULT_PORTPROXY_FILE_RPC_TIMEOUT,
                )?,
            ))
            .await?;
        Ok(())
    }

    pub async fn forward_port(&self, guest_port: u16) -> Result<ForwardHandle> {
        if let ControlBackend::Managed(control) = &self.sandbox.inner.control_backend {
            let preview = control.preview_url(&self.vm_id, guest_port).await?;
            return Err(SandboxError::Unsupported(format!(
                "{} exposes guest port {guest_port} at {preview}; the current ForwardHandle API returns local host ports only",
                control.provider_name()
            )));
        }

        let started = Instant::now();
        let node_endpoint = self.resolve_session_endpoint().await?;
        let vm = self
            .sandbox
            .ensure_vm_running(&self.vm_id, &node_endpoint)
            .await?;
        let proxy_port = vm
            .network
            .and_then(|n| n.portproxy_ports)
            .map(|p| p.proxy_port)
            .ok_or_else(|| SandboxError::InvalidResponse("VM missing proxy port".into()))?;
        let proxy_port = u16::try_from(proxy_port).map_err(|_| {
            SandboxError::InvalidResponse(format!(
                "VM reported invalid proxy port for forwarding: {proxy_port}"
            ))
        })?;
        if proxy_port == 0 {
            return Err(SandboxError::InvalidResponse(
                "VM reported zero proxy port for forwarding".into(),
            ));
        }
        let proxy_addr = portproxy_server_addr(
            &self.sandbox.inner.cfg.endpoint_overrides,
            &node_endpoint,
            proxy_port,
        )?;
        let multiplexer = self
            .sandbox
            .node_multiplexer_for_endpoint(&node_endpoint)
            .await;
        let host_port = multiplexer.register(guest_port, proxy_addr).await?;

        #[cfg(feature = "distributed-control")]
        let port_lease =
            if let ControlBackend::Distributed(control) = &self.sandbox.inner.control_backend {
                Some(
                    control
                        .acquire_port_lease(distributed::PortAllocation {
                            session_id: self.session_id.clone(),
                            vm_id: self.vm_id.clone(),
                            endpoint: node_endpoint.clone(),
                            guest_port,
                            host_port,
                        })
                        .await?,
                )
            } else {
                None
            };

        #[cfg(feature = "distributed-control")]
        let ownership_fence = self.ownership_fence().await;

        #[cfg(feature = "distributed-control")]
        self.sandbox
            .publish_control_command(
                "port.alloc",
                &self.vm_id,
                json!({
                    "session_id": self.session_id.as_str(),
                    "vm_id": self.vm_id.as_str(),
                    "endpoint": node_endpoint.as_str(),
                    "guest_port": guest_port,
                    "host_port": host_port,
                    "expected_fence": ownership_fence.as_deref(),
                }),
            )
            .await?;
        let handle = ForwardHandle {
            guest_port,
            host_port,
            registration: Arc::new(Mutex::new(Some(ForwardRegistration {
                multiplexer,
                host_port,
                #[cfg(feature = "distributed-control")]
                port_lease,
            }))),
            #[cfg(feature = "distributed-control")]
            port_context: Some(PortLifecycleContext {
                sandbox: self.sandbox.clone(),
                session_id: self.session_id.clone(),
                vm_id: self.vm_id.clone(),
                node_endpoint,
                ownership_fence,
            }),
        };
        log_slo_observation("port.forward.establish", started.elapsed(), "ok");
        Ok(handle)
    }

    pub async fn provider_preview_url(&self, guest_port: u16) -> Result<String> {
        if let ControlBackend::Managed(control) = &self.sandbox.inner.control_backend {
            return control.preview_url(&self.vm_id, guest_port).await;
        }

        Err(SandboxError::Unsupported(
            "provider preview URLs are only available for provider-managed sandboxes".to_string(),
        ))
    }

    pub async fn open_desktop(&self) -> Result<SessionDesktopTarget> {
        if matches!(
            &self.sandbox.inner.control_backend,
            ControlBackend::Managed(_)
        ) {
            return Err(SandboxError::Unsupported(
                "VM desktops are only available for vmd-backed sandboxes".to_string(),
            ));
        }

        let node_endpoint = self.current_node_endpoint().await;
        self.sandbox
            .ensure_vm_running(&self.vm_id, &node_endpoint)
            .await?;
        let mut client = self.sandbox.vmd_client_for_endpoint(&node_endpoint).await?;
        let desktop = client
            .open_vm_desktop(self.sandbox.request_with_auth(VmActionRequest {
                vm_id: self.vm_id.clone(),
            }))
            .await?
            .into_inner();
        match DesktopKind::try_from(desktop.kind).unwrap_or(DesktopKind::Unspecified) {
            DesktopKind::NativeWindow => {
                client
                    .show_vm_desktop(self.sandbox.request_with_auth(VmActionRequest {
                        vm_id: self.vm_id.clone(),
                    }))
                    .await?;
                Ok(SessionDesktopTarget {
                    kind: SessionDesktopKind::NativeWindow,
                    host: None,
                    port: None,
                    password: None,
                    authentication: SessionDesktopAuthentication::None,
                    view_only: false,
                })
            }
            DesktopKind::Vnc => {
                let port = u16::try_from(desktop.port).map_err(|_| {
                    SandboxError::InvalidResponse(format!(
                        "VM reported invalid desktop port: {}",
                        desktop.port
                    ))
                })?;
                if port == 0 {
                    return Err(SandboxError::InvalidResponse(
                        "VM reported zero desktop port".to_string(),
                    ));
                }
                let host = endpoint_host(&node_endpoint)?;
                if !matches!(host.as_str(), "127.0.0.1" | "::1") {
                    return Err(SandboxError::Unsupported(
                        "remote-node QEMU desktop relay is not implemented; the VNC console remains node-loopback-only"
                            .to_string(),
                    ));
                }
                let password = (!desktop.password.is_empty()).then_some(desktop.password);
                Ok(SessionDesktopTarget {
                    kind: SessionDesktopKind::Vnc,
                    host: Some(host),
                    port: Some(port),
                    authentication: if password.is_some() {
                        SessionDesktopAuthentication::Password
                    } else {
                        SessionDesktopAuthentication::None
                    },
                    password,
                    view_only: desktop.view_only,
                })
            }
            DesktopKind::GuestVnc => {
                let port = u16::try_from(desktop.port).map_err(|_| {
                    SandboxError::InvalidResponse(format!(
                        "VM reported invalid guest desktop port: {}",
                        desktop.port
                    ))
                })?;
                if port == 0 {
                    return Err(SandboxError::InvalidResponse(
                        "VM reported zero guest desktop port".to_string(),
                    ));
                }

                let mut active_forward = self.desktop_forward.lock().await;
                if let Some(previous) = active_forward.take() {
                    previous.close().await?;
                }
                match self.forward_port(port).await {
                    Ok(forward) => {
                        let host_port = forward.host_port;
                        *active_forward = Some(forward);
                        Ok(SessionDesktopTarget {
                            kind: SessionDesktopKind::Vnc,
                            host: Some("127.0.0.1".to_string()),
                            port: Some(host_port),
                            password: (!desktop.password.is_empty()).then_some(desktop.password),
                            authentication: SessionDesktopAuthentication::Account,
                            view_only: desktop.view_only,
                        })
                    }
                    Err(error) => {
                        let _ = client
                            .close_vm_desktop(self.sandbox.request_with_auth(VmActionRequest {
                                vm_id: self.vm_id.clone(),
                            }))
                            .await;
                        Err(error)
                    }
                }
            }
            DesktopKind::Unspecified => Err(SandboxError::InvalidResponse(
                "VM reported an unspecified desktop kind".to_string(),
            )),
        }
    }

    pub async fn close_desktop(&self) -> Result<()> {
        if matches!(
            &self.sandbox.inner.control_backend,
            ControlBackend::Managed(_)
        ) {
            return Ok(());
        }
        let node_endpoint = self.current_node_endpoint().await;
        if let Some(forward) = self.desktop_forward.lock().await.take() {
            forward.close().await?;
        }
        let mut client = self.sandbox.vmd_client_for_endpoint(&node_endpoint).await?;
        client
            .close_vm_desktop(self.sandbox.request_with_auth(VmActionRequest {
                vm_id: self.vm_id.clone(),
            }))
            .await?
            .into_inner();
        Ok(())
    }

    pub async fn state(&self) -> Result<i32> {
        if let ControlBackend::Managed(control) = &self.sandbox.inner.control_backend {
            return control.state(&self.vm_id).await;
        }

        let node_endpoint = self.current_node_endpoint().await;
        let mut client = self.sandbox.vmd_client_for_endpoint(&node_endpoint).await?;
        let vm = client
            .get_vm(self.sandbox.request_with_auth(GetVmRequest {
                vm_id: self.vm_id.clone(),
            }))
            .await?
            .into_inner();
        Ok(vm.state)
    }

    pub async fn update_resources(&self, vcpu: Option<i32>, memory_mb: Option<i32>) -> Result<i32> {
        if matches!(
            &self.sandbox.inner.control_backend,
            ControlBackend::Managed(_)
        ) {
            return Err(SandboxError::Unsupported(
                "resource updates are only available for vmd-backed sandboxes".to_string(),
            ));
        }

        let node_endpoint = self.resolve_session_endpoint().await?;
        let mut client = self.sandbox.vmd_client_for_endpoint(&node_endpoint).await?;
        let current = client
            .get_vm(self.sandbox.request_with_auth(GetVmRequest {
                vm_id: self.vm_id.clone(),
            }))
            .await?
            .into_inner();
        let current_resources = current.resources.unwrap_or_default();
        let vm = client
            .update_vm(self.sandbox.request_with_auth(UpdateVmRequest {
                vm_id: self.vm_id.clone(),
                name: None,
                metadata: None,
                resources: Some(ResourceSpec {
                    vcpu: vcpu.unwrap_or(current_resources.vcpu),
                    memory_mb: memory_mb.unwrap_or(current_resources.memory_mb),
                    disk_gb: current_resources.disk_gb,
                }),
                shared_mounts: Vec::new(),
                replace_shared_mounts: None,
            }))
            .await?
            .into_inner();
        self.sandbox
            .invalidate_ready_vm_rpc(&self.vm_id, &node_endpoint)
            .await;
        Ok(vm.state)
    }

    pub async fn reconfigure_shared_mounts(&self, shared_mounts: Vec<SharedMount>) -> Result<i32> {
        if matches!(
            &self.sandbox.inner.control_backend,
            ControlBackend::Managed(_)
        ) {
            return Err(SandboxError::Unsupported(
                "shared mount reconfiguration is only available for vmd-backed sandboxes"
                    .to_string(),
            ));
        }

        let node_endpoint = self.resolve_session_endpoint().await?;
        let mut client = self.sandbox.vmd_client_for_endpoint(&node_endpoint).await?;
        let current = client
            .get_vm(self.sandbox.request_with_auth(GetVmRequest {
                vm_id: self.vm_id.clone(),
            }))
            .await?
            .into_inner();
        let matcher = vm::ManagedMountMatcher::new(
            shared_mounts.iter().map(|mount| mount.mount_tag.as_str()),
            std::iter::empty::<&str>(),
        );
        if vm::vm_has_mount_contract(&current, &shared_mounts, &matcher) {
            return Ok(current.state);
        }

        let original_state = current.state;
        let running_state = proto::vmd::v1::VmState::Running as i32;
        let paused_state = proto::vmd::v1::VmState::Paused as i32;
        if matches!(original_state, state if state == running_state || state == paused_state) {
            self.stop().await?;
        }
        let vm = client
            .update_vm(self.sandbox.request_with_auth(UpdateVmRequest {
                vm_id: self.vm_id.clone(),
                name: None,
                metadata: None,
                resources: None,
                shared_mounts: shared_mounts.into_iter().map(proto_shared_mount).collect(),
                replace_shared_mounts: Some(proto::google::protobuf::BoolValue { value: true }),
            }))
            .await?
            .into_inner();
        self.sandbox
            .invalidate_ready_vm_rpc(&self.vm_id, &node_endpoint)
            .await;
        match original_state {
            state if state == running_state => self.start().await,
            state if state == paused_state => {
                self.start().await?;
                self.pause().await
            }
            _ => Ok(vm.state),
        }
    }

    pub async fn list_pci_devices(&self) -> Result<HostPciInventory> {
        if matches!(
            &self.sandbox.inner.control_backend,
            ControlBackend::Managed(_)
        ) {
            return Err(SandboxError::Unsupported(
                "PCI assignment is only available for vmd-backed sandboxes".to_string(),
            ));
        }
        let endpoint = self.current_node_endpoint().await;
        self.sandbox.list_host_pci_devices_at(&endpoint).await
    }

    pub async fn attach_pci_device(&self, device_id: &str) -> Result<PciDeviceAction> {
        if matches!(
            &self.sandbox.inner.control_backend,
            ControlBackend::Managed(_)
        ) {
            return Err(SandboxError::Unsupported(
                "PCI assignment is only available for vmd-backed sandboxes".to_string(),
            ));
        }
        let endpoint = self.current_node_endpoint().await;
        let mut client = self.sandbox.vmd_client_for_endpoint(&endpoint).await?;
        let response = client
            .attach_pci_device(self.sandbox.request_with_pci_auth(AttachPciDeviceRequest {
                vm_id: self.vm_id.clone(),
                device_id: device_id.to_string(),
            })?)
            .await?
            .into_inner();
        Ok(map_pci_action(response))
    }

    pub async fn detach_pci_device(&self, device_id: &str) -> Result<PciDeviceAction> {
        if matches!(
            &self.sandbox.inner.control_backend,
            ControlBackend::Managed(_)
        ) {
            return Err(SandboxError::Unsupported(
                "PCI assignment is only available for vmd-backed sandboxes".to_string(),
            ));
        }
        let endpoint = self.current_node_endpoint().await;
        let mut client = self.sandbox.vmd_client_for_endpoint(&endpoint).await?;
        let response = client
            .detach_pci_device(self.sandbox.request_with_pci_auth(DetachPciDeviceRequest {
                vm_id: self.vm_id.clone(),
                device_id: device_id.to_string(),
            })?)
            .await?
            .into_inner();
        Ok(map_pci_action(response))
    }

    async fn vm_action(&self, action: SessionVmAction) -> Result<i32> {
        if let ControlBackend::Managed(control) = &self.sandbox.inner.control_backend {
            return control.vm_action(&self.vm_id, action).await;
        }

        let node_endpoint = self.current_node_endpoint().await;
        let request_timeout = match action {
            SessionVmAction::Start | SessionVmAction::Restart => VMD_VM_START_TIMEOUT,
            SessionVmAction::Stop => VMD_VM_STOP_TIMEOUT,
            SessionVmAction::Pause | SessionVmAction::Resume => {
                self.sandbox.inner.cfg.connect_timeout
            }
        };
        let mut client = self
            .sandbox
            .vmd_client_for_endpoint_with_timeout(&node_endpoint, request_timeout)
            .await?;
        let request = self.sandbox.request_with_auth(VmActionRequest {
            vm_id: self.vm_id.clone(),
        });
        let vm = match action {
            SessionVmAction::Start => client.start_vm(request).await?.into_inner(),
            SessionVmAction::Restart => client.restart_vm(request).await?.into_inner(),
            SessionVmAction::Pause => client.pause_vm(request).await?.into_inner(),
            SessionVmAction::Resume => client.resume_vm(request).await?.into_inner(),
            SessionVmAction::Stop => client.stop_vm(request).await?.into_inner(),
        };
        if matches!(action, SessionVmAction::Pause | SessionVmAction::Stop) {
            self.sandbox
                .invalidate_ready_vm_rpc(&self.vm_id, &node_endpoint)
                .await;
        }
        Ok(vm.state)
    }

    pub async fn pause(&self) -> Result<i32> {
        self.vm_action(SessionVmAction::Pause).await
    }

    pub async fn start(&self) -> Result<i32> {
        self.vm_action(SessionVmAction::Start).await
    }

    pub async fn restart(&self) -> Result<i32> {
        self.vm_action(SessionVmAction::Restart).await
    }

    pub async fn resume(&self) -> Result<i32> {
        self.vm_action(SessionVmAction::Resume).await
    }

    pub async fn stop(&self) -> Result<i32> {
        self.vm_action(SessionVmAction::Stop).await
    }

    pub async fn snapshot(&self, label: &str, description: &str) -> Result<SessionSnapshot> {
        if let ControlBackend::Managed(control) = &self.sandbox.inner.control_backend {
            let checkpoint_id = control.create_checkpoint(&self.vm_id, label).await?;
            return Ok(SessionSnapshot {
                id: checkpoint_id,
                name: String::new(),
                label: label.to_string(),
                description: description.to_string(),
            });
        }

        // @dive: QEMU has no general QMP block flush command. Flush guest page
        //        caches through the guest RPC before pausing/snapshotting so every
        //        writable disk, including a separately attached durable volume,
        //        reaches its cache=none backing file before the VM state is pinned.
        self.sync_guest_filesystems().await?;

        let node_endpoint = self.resolve_session_endpoint().await?;
        let mut client = self.sandbox.vmd_client_for_endpoint(&node_endpoint).await?;
        let snapshot = client
            .create_snapshot(self.sandbox.request_with_auth(CreateSnapshotRequest {
                vm_id: self.vm_id.clone(),
                label: label.to_string(),
                description: description.to_string(),
            }))
            .await?
            .into_inner();
        Ok(SessionSnapshot {
            id: snapshot.id,
            name: snapshot.name,
            label: snapshot.label,
            description: snapshot.description,
        })
    }

    pub async fn checkpoint(&self, name: &str) -> Result<SessionCheckpoint> {
        let snapshot = self.snapshot(name, "").await?;
        Ok(SessionCheckpoint { id: snapshot.id })
    }

    pub async fn list_snapshots(&self) -> Result<Vec<SessionSnapshot>> {
        if matches!(
            &self.sandbox.inner.control_backend,
            ControlBackend::Managed(_)
        ) {
            return Err(SandboxError::Unsupported(
                "list_snapshots is only available for vmd-backed sandboxes".to_string(),
            ));
        }

        let node_endpoint = self.resolve_session_endpoint().await?;
        let mut client = self.sandbox.vmd_client_for_endpoint(&node_endpoint).await?;
        let snapshots = client
            .list_snapshots(self.sandbox.request_with_auth(ListSnapshotsRequest {
                vm_id: self.vm_id.clone(),
            }))
            .await?
            .into_inner()
            .snapshots;
        Ok(snapshots
            .into_iter()
            .map(|snapshot| SessionSnapshot {
                id: snapshot.id,
                name: snapshot.name,
                label: snapshot.label,
                description: snapshot.description,
            })
            .collect())
    }

    pub async fn restore(&self, snapshot_id: &str) -> Result<i32> {
        if matches!(
            &self.sandbox.inner.control_backend,
            ControlBackend::Managed(_)
        ) {
            return Err(SandboxError::Unsupported(
                "restore is only available for vmd-backed sandboxes; use restore_checkpoint for provider-managed sandboxes"
                    .to_string(),
            ));
        }

        let node_endpoint = self.resolve_session_endpoint().await?;
        let mut client = self.sandbox.vmd_client_for_endpoint(&node_endpoint).await?;
        let vm = client
            .restore_snapshot(self.sandbox.request_with_auth(RestoreSnapshotRequest {
                vm_id: self.vm_id.clone(),
                snapshot_id: snapshot_id.to_string(),
            }))
            .await?
            .into_inner();
        self.sandbox
            .invalidate_ready_vm_rpc(&self.vm_id, &node_endpoint)
            .await;
        Ok(vm.state)
    }

    pub async fn delete_snapshot(&self, snapshot_id: &str) -> Result<()> {
        if let ControlBackend::Managed(control) = &self.sandbox.inner.control_backend {
            return control.delete_checkpoint(snapshot_id).await;
        }

        let node_endpoint = self.resolve_session_endpoint().await?;
        let mut client = self.sandbox.vmd_client_for_endpoint(&node_endpoint).await?;
        client
            .delete_snapshot(self.sandbox.request_with_auth(DeleteSnapshotRequest {
                vm_id: self.vm_id.clone(),
                snapshot_id: snapshot_id.to_string(),
            }))
            .await?;
        Ok(())
    }

    pub async fn restore_checkpoint(&self, checkpoint_id: &str) -> Result<Session> {
        if let ControlBackend::Managed(control) = &self.sandbox.inner.control_backend {
            let child = control
                .create_from_checkpoint(
                    checkpoint_id,
                    HashMap::new(),
                    None,
                    self.shared_mounts.as_slice(),
                )
                .await?;
            let restored_session_id = child.id.clone();
            return Ok(Session::new_with_backend(
                self.sandbox.clone(),
                restored_session_id,
                child.id,
                control.api_url().to_string(),
                None,
                self.shared_mounts.as_ref().clone(),
            ));
        }

        Err(SandboxError::Unsupported(
            "restore_checkpoint is only available for provider-managed sandboxes".to_string(),
        ))
    }

    pub async fn fork(&self, opts: ForkOptions) -> Result<ForkResult> {
        if let ControlBackend::Managed(control) = &self.sandbox.inner.control_backend {
            return control.fork(self.sandbox.clone(), self, opts).await;
        }

        self.sync_guest_filesystems().await?;

        let auto_start_child = opts.auto_start_child;
        let node_endpoint = self.resolve_session_endpoint().await?;
        let child_session_id = Uuid::new_v4().to_string();
        let ownership_fence = self.ownership_fence().await;
        let mut child_metadata = opts.child_metadata;
        child_metadata.insert(META_SESSION_ID.to_string(), child_session_id.clone());
        child_metadata.insert(META_PARENT_SESSION_ID.to_string(), self.session_id.clone());
        child_metadata.insert(META_PARENT_VM_ID.to_string(), self.vm_id.clone());

        #[cfg(feature = "distributed-control")]
        self.sandbox
            .publish_control_command(
                "vm.fork",
                &self.vm_id,
                json!({
                    "session_id": self.session_id.as_str(),
                    "vm_id": self.vm_id.as_str(),
                    "child_session_id": child_session_id.as_str(),
                    "endpoint": node_endpoint.as_str(),
                    "auto_start_child": auto_start_child,
                    "expected_fence": ownership_fence.as_deref(),
                }),
            )
            .await?;
        #[cfg(not(feature = "distributed-control"))]
        let _ = &ownership_fence;

        let mut client = self.sandbox.vmd_client_for_endpoint(&node_endpoint).await?;
        let response = client
            .fork_vm(self.sandbox.request_with_auth(ForkVmRequest {
                parent_vm_id: self.vm_id.clone(),
                child_name: opts.child_name.unwrap_or_default(),
                child_metadata: Some(Metadata {
                    entries: child_metadata,
                }),
                auto_start_child,
            }))
            .await?
            .into_inner();

        let child_vm = response.child_vm.ok_or_else(|| {
            SandboxError::InvalidResponse("fork response missing child VM".into())
        })?;

        if auto_start_child {
            let _ = self
                .sandbox
                .ensure_vm_and_get_rpc_port(&child_vm.id, &node_endpoint)
                .await?;
        }
        let (tenant_id, workspace_id) =
            self.sandbox.current_session_scope(&self.session_id).await?;

        let child_fence = self
            .sandbox
            .bind_session_route(
                &child_session_id,
                &child_vm.id,
                &node_endpoint,
                Some(response.fork_id.as_str()),
                Some(tenant_id.as_str()),
                Some(workspace_id.as_str()),
                vm_tier_b_eligible(&child_vm),
                vm_required_shared_mount_profiles(&child_vm),
                None,
            )
            .await?;

        let child = Session {
            sandbox: self.sandbox.clone(),
            session_id: child_session_id.clone(),
            workspace_root: vm_workspace_root(&child_vm),
            vm_id: child_vm.id,
            node_endpoint: Arc::new(Mutex::new(node_endpoint)),
            ownership_fence: Arc::new(Mutex::new(child_fence)),
            shared_mounts: self.shared_mounts.clone(),
            desktop_forward: Arc::new(Mutex::new(None)),
        };

        Ok(ForkResult {
            parent_session_id: self.session_id.clone(),
            child_session_id,
            fork_id: response.fork_id,
            child,
        })
    }

    pub async fn close(self) -> Result<()> {
        Ok(())
    }

    pub async fn discard(self) -> Result<()> {
        if let ControlBackend::Managed(control) = &self.sandbox.inner.control_backend {
            control.delete_sandbox(&self.vm_id).await?;
            self.sandbox
                .clear_managed_session_aliases(&self.session_id, &self.vm_id)
                .await;
            return Ok(());
        }

        let ownership_fence = self.ownership_fence().await;
        let node_endpoint = self.current_node_endpoint().await;
        self.sandbox
            .discard_vm(
                &self.vm_id,
                &node_endpoint,
                Some(&self.session_id),
                ownership_fence.as_deref(),
            )
            .await
    }
}

impl Sandbox {
    pub async fn new(mut config: SandboxConfig) -> Result<Self> {
        if matches!(&config.provider, SandboxProviderConfig::Chevalier) {
            config.default_image = config.default_image.trim().to_string();
            if config.default_image.is_empty() {
                return Err(SandboxError::InvalidConfig(
                    "default image is required for the Chevalier provider; set SandboxConfig.default_image or BRACKET_VM_IMAGE"
                        .to_string(),
                ));
            }
        }
        config.endpoint = normalize_endpoint(&config.endpoint)?;
        let mut normalized_gateways = Vec::new();
        for endpoint in std::mem::take(&mut config.control_gateway_endpoints) {
            if endpoint.trim().is_empty() {
                continue;
            }
            normalized_gateways.push(normalize_endpoint(&endpoint)?);
        }
        normalized_gateways.sort();
        normalized_gateways.dedup();
        normalized_gateways.retain(|endpoint| endpoint != &config.endpoint);
        config.control_gateway_endpoints = normalized_gateways;
        let auth_header = compile_auth_header(config.auth_token.as_deref())?;
        let pci_auth_header =
            compile_metadata_token(config.pci_access_token.as_deref(), "PCI capability token")?;

        let control_backend = Self::build_control_backend(&config).await?;

        let sandbox = Self {
            inner: Arc::new(SandboxInner {
                cfg: config,
                control_backend,
                auth_header,
                pci_auth_header,
                #[cfg(feature = "host")]
                managed_daemon: Mutex::new(None),
                ready_vm_rpc: Mutex::new(HashMap::new()),
                portproxy_channels: Mutex::new(HashMap::new()),
                node_multiplexers: Mutex::new(HashMap::new()),
                warm_pool_ready: Mutex::new(HashSet::new()),
                managed_session_aliases: Mutex::new(HashMap::new()),
            }),
        };

        sandbox.ensure_daemon_ready().await?;
        sandbox.prewarm_warm_pool_profiles().await?;
        Ok(sandbox)
    }

    pub async fn connect(endpoint: impl Into<String>, mut config: SandboxConfig) -> Result<Self> {
        config.endpoint = normalize_endpoint(&endpoint.into())?;
        config.auto_spawn = false;
        Self::new(config).await
    }

    /// Forces warm-pool/image prewarming without requiring session creation.
    /// This is intended for startup/bootstrap scripts to avoid first-user cold start.
    pub async fn prewarm(mut config: SandboxConfig) -> Result<()> {
        config.prewarm_on_start = false;
        let sandbox = Self::new(config).await?;
        sandbox.prewarm_warm_pool_profiles_with_mode(true).await
    }

    pub async fn session(&self, opts: SessionOptions) -> Result<Session> {
        let started = Instant::now();
        if let ControlBackend::Managed(control) = &self.inner.control_backend {
            if opts.source_type != SessionSourceType::Docker {
                return Err(SandboxError::InvalidConfig(
                    "OpenComputer supports only Docker session sources".to_string(),
                ));
            }
            let requested_session_id = opts.session_id;
            let mut metadata = opts.metadata;
            metadata
                .entry("workspace_id".to_string())
                .or_insert_with(|| "default".to_string());
            metadata
                .entry("tenant_id".to_string())
                .or_insert_with(|| "default".to_string());
            if let Some(requested_session_id) = requested_session_id.as_deref() {
                metadata.insert(
                    "chevalier.requested_session_id".to_string(),
                    requested_session_id.to_string(),
                );
            }
            if let Some(name) = opts.name.as_deref().filter(|name| !name.trim().is_empty()) {
                metadata.insert("chevalier.name".to_string(), name.to_string());
            }
            metadata.insert(META_TIER_B_ELIGIBLE.to_string(), "false".to_string());
            metadata.insert(
                META_EXECUTION_FIDELITY_REQUIREMENT.to_string(),
                "provider-managed".to_string(),
            );
            if let Some(requested_session_id) = requested_session_id.as_deref() {
                metadata.insert(
                    META_SESSION_ID.to_string(),
                    requested_session_id.to_string(),
                );
            }
            let sandbox = control
                .create_sandbox(
                    opts.image,
                    opts.resources,
                    metadata,
                    opts.egress_allowlist,
                    opts.shared_mounts.as_slice(),
                )
                .await?;
            let logical_session_id = requested_session_id.unwrap_or_else(|| sandbox.id.clone());
            self.bind_managed_session_alias(&logical_session_id, &sandbox.id)
                .await;
            let session = Session::new_with_backend(
                self.clone(),
                logical_session_id,
                sandbox.id,
                control.api_url().to_string(),
                None,
                opts.shared_mounts,
            );
            log_slo_observation(
                &format!("session.create.{}", control.provider_name()),
                started.elapsed(),
                "ok",
            );
            return Ok(session);
        }

        let session_id = opts
            .session_id
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let auto_start = opts.auto_start;

        let mut metadata = opts.metadata;
        let tenant_id = metadata
            .get("tenant_id")
            .cloned()
            .unwrap_or_else(|| "default".to_string());
        let workspace_id = metadata
            .get("workspace_id")
            .cloned()
            .unwrap_or_else(|| "default".to_string());
        metadata
            .entry("tenant_id".to_string())
            .or_insert_with(|| tenant_id.clone());
        metadata
            .entry("workspace_id".to_string())
            .or_insert_with(|| workspace_id.clone());
        let tier_b_eligible = resolve_tier_b_eligibility(&metadata);
        validate_shared_mount_contract(&opts.shared_mounts, tier_b_eligible)?;
        let required_mount_profiles = required_shared_mount_profiles(&opts.shared_mounts);
        metadata.insert(
            META_TIER_B_ELIGIBLE.to_string(),
            if tier_b_eligible { "true" } else { "false" }.to_string(),
        );
        metadata.insert(
            META_EXECUTION_FIDELITY_REQUIREMENT.to_string(),
            if tier_b_eligible {
                "disk+memory".to_string()
            } else {
                "best-effort".to_string()
            },
        );

        if let Some((vm, node_endpoint)) = self.find_vm_by_session_id(&session_id).await? {
            let expected_fence = self.current_session_fence(&session_id).await?;
            let (attached_tenant_id, attached_workspace_id) =
                self.current_session_scope(&session_id).await?;
            #[cfg(feature = "distributed-control")]
            self.publish_control_command(
                "session.attach",
                &session_id,
                json!({
                    "session_id": session_id.as_str(),
                    "vm_id": vm.id.as_str(),
                    "endpoint": node_endpoint.as_str(),
                    "tenant_id": attached_tenant_id.as_str(),
                    "workspace_id": attached_workspace_id.as_str(),
                    "expected_fence": expected_fence.as_deref(),
                }),
            )
            .await?;

            let endpoint_handoff = self
                .session_attach_is_endpoint_handoff(&session_id, &node_endpoint)
                .await?;
            // @dive: Same-endpoint reattach only restores stopped VMs. Cross-endpoint
            // handoff must restore even if stale metadata says Running; otherwise a
            // secondary clone can be trusted before its execution snapshot is materialized.
            if endpoint_handoff || vm.state != proto::vmd::v1::VmState::Running as i32 {
                let _ = self
                    .restore_execution_state_if_needed(
                        &session_id,
                        &vm.id,
                        &node_endpoint,
                        &vm,
                        endpoint_handoff,
                    )
                    .await?;
            }
            let vm = self.ensure_vm_running(&vm.id, &node_endpoint).await?;
            if vm_has_guest_rpc(&vm) {
            self.maybe_wait_for_session_guest_rpc(
                &vm.id,
                &node_endpoint,
                ReadinessRecovery::RestartIfNotFreshlyStarted,
            )
            .await?;
            }
            let next_fence = self
                .bind_session_route(
                    &session_id,
                    &vm.id,
                    &node_endpoint,
                    None,
                    Some(attached_tenant_id.as_str()),
                    Some(attached_workspace_id.as_str()),
                    vm_tier_b_eligible(&vm),
                    vm_required_shared_mount_profiles(&vm),
                    expected_fence.as_deref(),
                )
                .await?;
            let session = Session {
                sandbox: self.clone(),
                session_id,
                workspace_root: vm_workspace_root(&vm),
                vm_id: vm.id,
                node_endpoint: Arc::new(Mutex::new(node_endpoint)),
                ownership_fence: Arc::new(Mutex::new(next_fence)),
                shared_mounts: Arc::new(Vec::new()),
                desktop_forward: Arc::new(Mutex::new(None)),
            };
            log_slo_observation("session.attach", started.elapsed(), "ok");
            return Ok(session);
        }

        metadata.insert(META_SESSION_ID.to_string(), session_id.clone());

        let resources = opts
            .resources
            .unwrap_or_else(|| self.inner.cfg.default_resources.clone());
        let name = opts
            .name
            .unwrap_or_else(|| format!("chevalier-session-{}", &session_id[..8]));
        let image = opts
            .image
            .unwrap_or_else(|| self.inner.cfg.default_image.clone());
        let architecture = normalize_architecture_label(&opts.architecture.unwrap_or_else(|| {
            match opts.source_type {
                SessionSourceType::MacosTemplate => "arm64".to_string(),
                SessionSourceType::WindowsTemplate => String::new(),
                SessionSourceType::Docker | SessionSourceType::Snapshot => self
                    .inner
                    .cfg
                    .default_architecture
                    .clone()
                    .or_else(detect_host_architecture_label)
                    .unwrap_or_default(),
            }
        }));
        let (source_type, guest_profile) = match opts.source_type {
            SessionSourceType::Docker => (VmSourceType::Docker as i32, None),
            SessionSourceType::Snapshot => (VmSourceType::Snapshot as i32, None),
            SessionSourceType::MacosTemplate => (
                VmSourceType::MacosTemplate as i32,
                Some(GuestProfile {
                    platform: GuestPlatform::Macos as i32,
                    architecture: architecture.clone(),
                    schema_version: 1,
                    ..Default::default()
                }),
            ),
            SessionSourceType::WindowsTemplate => (
                VmSourceType::WindowsTemplate as i32,
                Some(GuestProfile {
                    platform: GuestPlatform::Windows as i32,
                    architecture: architecture.clone(),
                    schema_version: 1,
                    ..Default::default()
                }),
            ),
        };

        let session_shared_mounts = opts.shared_mounts.clone();
        let request = CreateVmRequest {
            name,
            source: Some(VmSource {
                r#type: source_type,
                reference: image.clone(),
            }),
            resources: Some(ResourceSpec {
                vcpu: resources.vcpu,
                memory_mb: resources.memory_mb,
                disk_gb: resources.disk_gb,
            }),
            metadata: Some(Metadata { entries: metadata }),
            auto_start: opts.auto_start,
            architecture: architecture.clone(),
            guest_profile,
            guest_runtime: None,
            capabilities: None,
            shared_mounts: opts
                .shared_mounts
                .into_iter()
                .map(proto_shared_mount)
                .collect(),
            pci_device_ids: opts.pci_device_ids,
            storage_profile: opts.storage_profile,
            volume_owner_key: opts.volume_owner_key.unwrap_or_default(),
            volume_size_gb: opts.volume_size_gb.unwrap_or_default(),
        };

        let node_endpoint = self
            .endpoint_for_new_session(
                &session_id,
                &tenant_id,
                &workspace_id,
                tier_b_eligible,
                &required_mount_profiles,
            )
            .await?;
        let warm_pool_key = warm_pool_key(&node_endpoint, image.as_str(), architecture.as_str());
        let warm_pool_hit = self.warm_pool_contains_key(warm_pool_key.as_str()).await;
        #[cfg(feature = "distributed-control")]
        self.publish_control_command(
            "session.create",
            &session_id,
            json!({
                "session_id": session_id.as_str(),
                "endpoint": node_endpoint.as_str(),
                "tenant_id": tenant_id.as_str(),
                "workspace_id": workspace_id.as_str(),
                "auto_start": auto_start,
                "tier_b_eligible": tier_b_eligible,
                "execution_fidelity_requirement": if tier_b_eligible { "disk+memory" } else { "best-effort" },
                "warm_pool_hit": warm_pool_hit,
                "architecture": architecture.as_str(),
            }),
        )
        .await?;

        let mut client = self.vmd_client_for_endpoint(&node_endpoint).await?;
        let request = if request.pci_device_ids.is_empty() {
            self.request_with_auth(request)
        } else {
            self.request_with_pci_auth(request)?
        };
        let mut stream = client.create_vm(request).await?.into_inner();

        let mut final_vm: Option<Vm> = None;
        while let Some(update) = stream.message().await? {
            if let Some(proto::vmd::v1::create_vm_stream_response::Event::Vm(vm)) = update.event {
                // Some daemon builds keep the create stream open after emitting the terminal VM
                // payload. Once we have that payload, waiting for EOF adds no value and can hang
                // session creation indefinitely.
                final_vm = Some(vm);
                break;
            }
        }

        let vm = final_vm
            .ok_or_else(|| SandboxError::InvalidResponse("create_vm stream missing VM".into()))?;

        let next_fence = self
            .bind_session_route(
                &session_id,
                &vm.id,
                &node_endpoint,
                None,
                Some(tenant_id.as_str()),
                Some(workspace_id.as_str()),
                tier_b_eligible,
                required_mount_profiles.clone(),
                None,
            )
            .await?;

        let running_state = proto::vmd::v1::VmState::Running as i32;
        if (auto_start || vm.state == running_state) && vm_has_guest_rpc(&vm) {
            // @dive: This VM was created (and auto-started) by this very call. A missed
            //        readiness budget here means "still booting", never "stale sidecar".
            self.maybe_wait_for_session_guest_rpc(
                &vm.id,
                &node_endpoint,
                ReadinessRecovery::FreshBoot,
            )
            .await?;
        }

        let session = Session {
            sandbox: self.clone(),
            session_id,
            workspace_root: vm_workspace_root(&vm),
            vm_id: vm.id,
            node_endpoint: Arc::new(Mutex::new(node_endpoint)),
            ownership_fence: Arc::new(Mutex::new(next_fence)),
            shared_mounts: Arc::new(session_shared_mounts),
            desktop_forward: Arc::new(Mutex::new(None)),
        };

        if warm_pool_hit {
            log_slo_observation("session.create.warm_pool", started.elapsed(), "ok");
        } else {
            log_slo_observation("session.create.cold_cache_hit", started.elapsed(), "ok");
            let sandbox = self.clone();
            let endpoint = session.current_node_endpoint().await;
            let refill_profile = WarmPoolProfile {
                image,
                architecture: Some(architecture),
                min_inventory: 1,
            };
            tokio::spawn(async move {
                let profiles = vec![refill_profile];
                let _ = sandbox
                    .prewarm_profiles_on_endpoint(endpoint.as_str(), &profiles, false)
                    .await;
            });
        }

        log_slo_observation("session.create", started.elapsed(), "ok");
        Ok(session)
    }

    pub async fn attach_session(&self, session_id: &str) -> Result<Session> {
        let started = Instant::now();
        if let ControlBackend::Managed(control) = &self.inner.control_backend {
            let provider_session_id = self.managed_provider_session_id(session_id).await;
            // @dive: The alias map is per-process. After a restart the provider is asked
            // for the VM that carries this logical session id in its metadata.
            let sandbox = match control.get_sandbox(&provider_session_id).await {
                Ok(sandbox) => sandbox,
                Err(SandboxError::SessionNotFound(_)) if provider_session_id == session_id => {
                    control
                        .find_by_session_id(session_id)
                        .await?
                        .ok_or_else(|| SandboxError::SessionNotFound(session_id.to_string()))?
                }
                Err(error) => return Err(error),
            };
            self.bind_managed_session_alias(session_id, &sandbox.id)
                .await;
            control.ensure_running(&sandbox.id).await?;
            control.ensure_configured_mounts(&sandbox.id, &[]).await?;
            let session = Session::new_with_backend(
                self.clone(),
                session_id.to_string(),
                sandbox.id,
                control.api_url().to_string(),
                None,
                Vec::new(),
            );
            log_slo_observation(
                &format!("session.attach.{}", control.provider_name()),
                started.elapsed(),
                "ok",
            );
            return Ok(session);
        }

        let (vm, node_endpoint) = self
            .find_vm_by_session_id(session_id)
            .await?
            .ok_or_else(|| SandboxError::SessionNotFound(session_id.to_string()))?;
        validate_vm_mount_contract(session_id, &vm)?;
        let expected_fence = self.current_session_fence(session_id).await?;
        let (tenant_id, workspace_id) = self.current_session_scope(session_id).await?;

        #[cfg(feature = "distributed-control")]
        self.publish_control_command(
            "session.attach",
            session_id,
            json!({
                "session_id": session_id,
                "vm_id": vm.id.as_str(),
                "endpoint": node_endpoint.as_str(),
                "tenant_id": tenant_id.as_str(),
                "workspace_id": workspace_id.as_str(),
                "expected_fence": expected_fence.as_deref(),
            }),
        )
        .await?;

        let endpoint_handoff = self
            .session_attach_is_endpoint_handoff(session_id, &node_endpoint)
            .await?;
        // @dive: Same-endpoint attach only restores stopped VMs. Cross-endpoint handoff
        // must restore even if stale metadata says Running; otherwise a secondary clone
        // can be trusted before its execution snapshot is materialized.
        if endpoint_handoff || vm.state != proto::vmd::v1::VmState::Running as i32 {
            let _ = self
                .restore_execution_state_if_needed(
                    session_id,
                    &vm.id,
                    &node_endpoint,
                    &vm,
                    endpoint_handoff,
                )
                .await?;
        }
        let vm = self.ensure_vm_running(&vm.id, &node_endpoint).await?;
        if vm_has_guest_rpc(&vm) {
        self.maybe_wait_for_session_guest_rpc(
            &vm.id,
            &node_endpoint,
            ReadinessRecovery::RestartIfNotFreshlyStarted,
        )
        .await?;
        }
        let next_fence = self
            .bind_session_route(
                session_id,
                &vm.id,
                &node_endpoint,
                None,
                Some(tenant_id.as_str()),
                Some(workspace_id.as_str()),
                vm_tier_b_eligible(&vm),
                vm_required_shared_mount_profiles(&vm),
                expected_fence.as_deref(),
            )
            .await?;

        let session = Session {
            sandbox: self.clone(),
            session_id: session_id.to_string(),
            workspace_root: vm_workspace_root(&vm),
            vm_id: vm.id,
            node_endpoint: Arc::new(Mutex::new(node_endpoint)),
            ownership_fence: Arc::new(Mutex::new(next_fence)),
            shared_mounts: Arc::new(Vec::new()),
            desktop_forward: Arc::new(Mutex::new(None)),
        };
        log_slo_observation("session.attach", started.elapsed(), "ok");
        Ok(session)
    }

    /// Reconstruct a session handle without restoring, starting, or probing the VM.
    /// Lifecycle owners use this when the next action is explicitly pause, restore,
    /// start, or discard; ordinary `attach_session` retains its ready-to-execute contract.
    pub async fn attach_session_passive(&self, session_id: &str) -> Result<Session> {
        let started = Instant::now();
        if matches!(&self.inner.control_backend, ControlBackend::Managed(_)) {
            return self.attach_session(session_id).await;
        }

        let (vm, node_endpoint) = self
            .find_vm_by_session_id(session_id)
            .await?
            .ok_or_else(|| SandboxError::SessionNotFound(session_id.to_string()))?;
        validate_vm_mount_contract(session_id, &vm)?;
        let current_fence = self.current_session_fence(session_id).await?;
        let session = Session {
            sandbox: self.clone(),
            session_id: session_id.to_string(),
            workspace_root: vm_workspace_root(&vm),
            vm_id: vm.id,
            node_endpoint: Arc::new(Mutex::new(node_endpoint)),
            ownership_fence: Arc::new(Mutex::new(current_fence)),
            shared_mounts: Arc::new(Vec::new()),
            desktop_forward: Arc::new(Mutex::new(None)),
        };
        log_slo_observation("session.attach.passive", started.elapsed(), "ok");
        Ok(session)
    }

    pub async fn find_vm_by_id_including_endpoint(
        &self,
        vm_id: &str,
        endpoint: &str,
    ) -> Result<Option<(Vm, String)>> {
        let mut endpoints = self.candidate_endpoints().await?;
        if !endpoint.trim().is_empty() {
            endpoints.push(normalize_endpoint(endpoint)?);
        }
        endpoints.sort();
        endpoints.dedup();

        for endpoint in endpoints {
            if let Some(vm) = self.find_vm_by_id_on_endpoint(vm_id, &endpoint).await? {
                return Ok(Some((vm, endpoint)));
            }
        }

        Ok(None)
    }

    pub async fn discard_session_by_id(&self, session_id: &str) -> Result<()> {
        if let ControlBackend::Managed(control) = &self.inner.control_backend {
            let provider_session_id = self.managed_provider_session_id(session_id).await;
            control.delete_sandbox(&provider_session_id).await?;
            self.clear_managed_session_aliases(session_id, &provider_session_id)
                .await;
            return Ok(());
        }

        let expected_fence = self.current_session_fence(session_id).await?;
        if let Some((vm, node_endpoint)) = self.find_vm_by_session_id(session_id).await? {
            return self
                .discard_vm(
                    &vm.id,
                    &node_endpoint,
                    Some(session_id),
                    expected_fence.as_deref(),
                )
                .await;
        }

        #[cfg(feature = "distributed-control")]
        if let ControlBackend::Distributed(control) = &self.inner.control_backend {
            if let Some(route) = control.get_session_route(session_id).await? {
                return self
                    .clear_session_route(Some(session_id), &route.vm_id, expected_fence.as_deref())
                    .await;
            }
        }

        Ok(())
    }

    pub async fn list_host_pci_devices(&self) -> Result<HostPciInventory> {
        if matches!(&self.inner.control_backend, ControlBackend::Managed(_)) {
            return Err(SandboxError::Unsupported(
                "PCI assignment is only available for vmd-backed sandboxes".to_string(),
            ));
        }
        self.list_host_pci_devices_at(&self.inner.cfg.endpoint)
            .await
    }

    async fn list_host_pci_devices_at(&self, endpoint: &str) -> Result<HostPciInventory> {
        let mut client = self.vmd_client_for_endpoint(endpoint).await?;
        let response = client
            .list_host_pci_devices(
                self.request_with_optional_pci_auth(ListHostPciDevicesRequest {}),
            )
            .await?
            .into_inner();
        Ok(HostPciInventory {
            enabled: response.enabled,
            devices: response
                .devices
                .into_iter()
                .map(map_host_pci_device)
                .collect(),
        })
    }

    pub async fn list_sessions(&self) -> Result<Vec<SessionInfo>> {
        if let ControlBackend::Managed(control) = &self.inner.control_backend {
            return control.list_sessions().await;
        }

        let mut sessions = Vec::new();
        let mut seen = HashSet::new();
        for endpoint in self.candidate_endpoints().await? {
            let mut client = match self.vmd_client_for_endpoint(&endpoint).await {
                Ok(client) => client,
                Err(_) => continue,
            };
            let response = match client
                .list_v_ms(self.request_with_auth(ListVMsRequest {
                    include_snapshots: false,
                }))
                .await
            {
                Ok(response) => response.into_inner(),
                Err(_) => continue,
            };
            for vm in response.vms {
                let Some(session_id) = vm.metadata.get(META_SESSION_ID).cloned() else {
                    continue;
                };
                if !seen.insert(session_id.clone()) {
                    continue;
                }
                sessions.push(SessionInfo {
                    session_id,
                    vm_id: vm.id,
                    name: vm.name,
                    state: vm.state,
                    parent_session_id: vm.metadata.get(META_PARENT_SESSION_ID).cloned(),
                    fork_id: vm.metadata.get(META_FORK_ID).cloned(),
                });
            }
        }

        Ok(sessions)
    }

    pub async fn list_durable_volumes(&self) -> Result<Vec<DurableVolumeInfo>> {
        if matches!(&self.inner.control_backend, ControlBackend::Managed(_)) {
            return Ok(Vec::new());
        }
        let mut volumes = HashMap::new();
        let mut contacted = false;
        let mut last_connect_error = None;
        for endpoint in self.candidate_endpoints().await? {
            let mut client = match self.vmd_client_for_endpoint(&endpoint).await {
                Ok(client) => client,
                Err(error) => {
                    last_connect_error = Some(error);
                    continue;
                }
            };
            contacted = true;
            let mut request = self.request_with_auth(ListDurableVolumesRequest {});
            request.set_timeout(DURABLE_VOLUME_LIST_TIMEOUT);
            let response = match tokio::time::timeout(
                DURABLE_VOLUME_LIST_TIMEOUT,
                client.list_durable_volumes(request),
            )
            .await
            {
                Ok(Ok(response)) => response.into_inner(),
                Ok(Err(status)) if status.code() == tonic::Code::Unimplemented => continue,
                Ok(Err(status))
                    if matches!(
                        status.code(),
                        tonic::Code::Cancelled | tonic::Code::DeadlineExceeded
                    ) =>
                {
                    return Err(SandboxError::Grpc(tonic::Status::deadline_exceeded(
                        "durable volume inventory timed out",
                    )));
                }
                Ok(Err(status)) => return Err(SandboxError::Grpc(status)),
                Err(_) => {
                    return Err(SandboxError::Grpc(tonic::Status::deadline_exceeded(
                        "durable volume inventory timed out",
                    )));
                }
            };
            for volume in response.volumes {
                let timestamp_ms = |value: Option<proto::google::protobuf::Timestamp>| {
                    value
                        .map(|timestamp| {
                            timestamp.seconds.saturating_mul(1_000)
                                + i64::from(timestamp.nanos).saturating_div(1_000_000)
                        })
                        .unwrap_or_default()
                };
                volumes
                    .entry(volume.owner_key.clone())
                    .or_insert(DurableVolumeInfo {
                        owner_key: volume.owner_key,
                        volume_id: volume.volume_id,
                        size_gb: volume.size_gb,
                        created_at_ms: timestamp_ms(volume.created_at),
                        updated_at_ms: timestamp_ms(volume.updated_at),
                        backing_volume_id: (!volume.backing_volume_id.is_empty())
                            .then_some(volume.backing_volume_id),
                        attached_vm_ids: volume.attached_vm_ids,
                    });
            }
        }
        if !contacted {
            return Err(last_connect_error.unwrap_or_else(|| {
                SandboxError::DaemonUnavailable(
                    "no sandbox endpoint available for durable volume inventory".to_string(),
                )
            }));
        }
        let mut volumes = volumes.into_values().collect::<Vec<_>>();
        volumes.sort_by(|left, right| left.owner_key.cmp(&right.owner_key));
        Ok(volumes)
    }

    pub async fn delete_durable_volume(&self, owner_key: &str) -> Result<()> {
        if matches!(&self.inner.control_backend, ControlBackend::Managed(_)) {
            return Err(SandboxError::Unsupported(
                "durable data volumes are only available for vmd-backed sandboxes".to_string(),
            ));
        }
        let mut found = false;
        let mut contacted = false;
        let mut last_connect_error = None;
        for endpoint in self.candidate_endpoints().await? {
            let mut client = match self.vmd_client_for_endpoint(&endpoint).await {
                Ok(client) => client,
                Err(error) => {
                    last_connect_error = Some(error);
                    continue;
                }
            };
            contacted = true;
            match client
                .delete_durable_volume(self.request_with_auth(DeleteDurableVolumeRequest {
                    owner_key: owner_key.to_string(),
                }))
                .await
            {
                Ok(_) => found = true,
                Err(status) if status.code() == tonic::Code::NotFound => {}
                Err(status) => return Err(SandboxError::Grpc(status)),
            }
        }
        if !contacted {
            return Err(last_connect_error.unwrap_or_else(|| {
                SandboxError::DaemonUnavailable(
                    "no sandbox endpoint available for durable volume deletion".to_string(),
                )
            }));
        }
        if found {
            Ok(())
        } else {
            Err(SandboxError::InvalidResponse(format!(
                "durable volume not found: {owner_key}"
            )))
        }
    }

    pub async fn resize_durable_volume(
        &self,
        owner_key: &str,
        size_gb: i32,
    ) -> Result<DurableVolumeInfo> {
        if matches!(&self.inner.control_backend, ControlBackend::Managed(_)) {
            return Err(SandboxError::Unsupported(
                "durable data volumes are only available for vmd-backed sandboxes".to_string(),
            ));
        }
        let mut contacted = false;
        let mut last_connect_error = None;
        for endpoint in self.candidate_endpoints().await? {
            let mut client = match self.vmd_client_for_endpoint(&endpoint).await {
                Ok(client) => client,
                Err(error) => {
                    last_connect_error = Some(error);
                    continue;
                }
            };
            contacted = true;
            match client
                .resize_durable_volume(self.request_with_auth(ResizeDurableVolumeRequest {
                    owner_key: owner_key.to_string(),
                    size_gb,
                }))
                .await
            {
                Ok(response) => {
                    let volume = response.into_inner();
                    let timestamp_ms = |value: Option<proto::google::protobuf::Timestamp>| {
                        value
                            .map(|timestamp| {
                                timestamp.seconds.saturating_mul(1_000)
                                    + i64::from(timestamp.nanos).saturating_div(1_000_000)
                            })
                            .unwrap_or_default()
                    };
                    return Ok(DurableVolumeInfo {
                        owner_key: volume.owner_key,
                        volume_id: volume.volume_id,
                        size_gb: volume.size_gb,
                        created_at_ms: timestamp_ms(volume.created_at),
                        updated_at_ms: timestamp_ms(volume.updated_at),
                        backing_volume_id: (!volume.backing_volume_id.is_empty())
                            .then_some(volume.backing_volume_id),
                        attached_vm_ids: volume.attached_vm_ids,
                    });
                }
                Err(status) if status.code() == tonic::Code::NotFound => continue,
                Err(status) => return Err(SandboxError::Grpc(status)),
            }
        }
        if !contacted {
            return Err(last_connect_error.unwrap_or_else(|| {
                SandboxError::DaemonUnavailable(
                    "no sandbox endpoint available for durable volume resize".to_string(),
                )
            }));
        }
        Err(SandboxError::InvalidResponse(format!(
            "durable volume not found: {owner_key}"
        )))
    }

    async fn build_control_backend(config: &SandboxConfig) -> Result<ControlBackend> {
        match &config.provider {
            SandboxProviderConfig::OpenComputer(opencomputer_config) => {
                return Ok(ControlBackend::Managed(ManagedControl::OpenComputer(
                    opencomputer::OpenComputerControl::new(opencomputer_config.clone())?,
                )));
            }
            SandboxProviderConfig::Freestyle(freestyle_config) => {
                return Ok(ControlBackend::Managed(ManagedControl::Freestyle(
                    freestyle::FreestyleControl::new(freestyle_config.clone())?,
                )));
            }
            SandboxProviderConfig::Chevalier => {}
        }

        if let Some(dist_cfg) = config.distributed_control.clone() {
            #[cfg(feature = "distributed-control")]
            {
                let control = distributed::DistributedControlPlane::connect(dist_cfg).await?;
                return Ok(ControlBackend::Distributed(control));
            }

            #[cfg(not(feature = "distributed-control"))]
            {
                let _ = dist_cfg;
                return Err(SandboxError::Unsupported(
                    "distributed control requested but crate feature `distributed-control` is disabled"
                        .to_string(),
                ));
            }
        }

        Ok(ControlBackend::Direct)
    }

    async fn bind_managed_session_alias(&self, logical_session_id: &str, provider_id: &str) {
        if logical_session_id == provider_id {
            return;
        }
        self.inner
            .managed_session_aliases
            .lock()
            .await
            .insert(logical_session_id.to_string(), provider_id.to_string());
    }

    async fn managed_provider_session_id(&self, session_id: &str) -> String {
        self.inner
            .managed_session_aliases
            .lock()
            .await
            .get(session_id)
            .cloned()
            .unwrap_or_else(|| session_id.to_string())
    }

    async fn clear_managed_session_aliases(&self, logical_session_id: &str, provider_id: &str) {
        let mut aliases = self.inner.managed_session_aliases.lock().await;
        aliases.remove(logical_session_id);
        aliases.retain(|_, mapped_provider_id| mapped_provider_id != provider_id);
    }

    async fn vmd_client_for_endpoint(
        &self,
        endpoint_raw: &str,
    ) -> Result<VmdServiceClient<tonic::transport::Channel>> {
        self.vmd_client_for_endpoint_with_timeout(endpoint_raw, self.inner.cfg.connect_timeout)
            .await
    }

    async fn vmd_client_for_endpoint_with_timeout(
        &self,
        endpoint_raw: &str,
        request_timeout: Duration,
    ) -> Result<VmdServiceClient<tonic::transport::Channel>> {
        let endpoint_raw = normalize_endpoint(endpoint_raw)?;
        let endpoint_raw = self.rewrite_endpoint(&endpoint_raw)?;
        let mut endpoint = Endpoint::from_shared(endpoint_raw.clone())
            .map_err(|err| SandboxError::InvalidEndpoint(err.to_string()))?
            .connect_timeout(self.inner.cfg.connect_timeout)
            .timeout(request_timeout);
        if endpoint_raw.starts_with("https://") {
            endpoint = endpoint
                .tls_config(self.build_client_tls_config(endpoint_raw.as_str())?)
                .map_err(|err| SandboxError::InvalidConfig(err.to_string()))?;
        }
        Ok(VmdServiceClient::connect(endpoint).await?)
    }

    fn rewrite_endpoint(&self, endpoint: &str) -> Result<String> {
        let normalized = normalize_endpoint(endpoint)?;
        if let Some(override_endpoint) = self.inner.cfg.endpoint_overrides.get(&normalized) {
            return normalize_endpoint(override_endpoint);
        }
        Ok(normalized)
    }

    async fn ensure_daemon_ready(&self) -> Result<()> {
        match &self.inner.control_backend {
            ControlBackend::Managed(_) => Ok(()),
            ControlBackend::Direct => {
                let mut last_health_err: Option<String> = None;
                for endpoint in self.candidate_endpoints().await? {
                    match self.health_check_endpoint(&endpoint).await {
                        Ok(()) => return Ok(()),
                        Err(err) => {
                            let err_msg = err.to_string();
                            tracing::warn!(
                                endpoint = %endpoint,
                                error = %err_msg,
                                "sandbox daemon health check failed"
                            );
                            last_health_err = Some(err_msg);
                        }
                    }
                }

                if !self.inner.cfg.auto_spawn {
                    let detail = last_health_err
                        .map(|msg| format!("; last health error: {msg}"))
                        .unwrap_or_default();
                    return Err(SandboxError::DaemonUnavailable(format!(
                        "unable to connect to sandbox daemon at any configured control endpoint (primary: {}){}",
                        self.inner.cfg.endpoint, detail
                    )));
                }

                self.spawn_daemon_if_needed().await?;

                let start = Instant::now();
                while start.elapsed() < self.inner.cfg.daemon_start_timeout {
                    if self
                        .health_check_endpoint(&self.inner.cfg.endpoint)
                        .await
                        .is_ok()
                    {
                        return Ok(());
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }

                Err(SandboxError::DaemonUnavailable(format!(
                    "sandbox daemon did not become ready at {}",
                    self.inner.cfg.endpoint
                )))
            }
            #[cfg(feature = "distributed-control")]
            ControlBackend::Distributed(_) => self.ensure_distributed_ready().await,
        }
    }

    async fn prewarm_warm_pool_profiles(&self) -> Result<()> {
        self.prewarm_warm_pool_profiles_with_mode(false).await
    }

    async fn prewarm_warm_pool_profiles_with_mode(&self, force: bool) -> Result<()> {
        if let ControlBackend::Managed(_) = &self.inner.control_backend {
            return Ok(());
        }

        if !force && !self.inner.cfg.prewarm_on_start {
            return Ok(());
        }

        let profiles = self.resolved_warm_pool_profiles();
        if profiles.is_empty() {
            return Ok(());
        }

        for endpoint in self.candidate_endpoints().await? {
            if self.health_check_endpoint(&endpoint).await.is_err() {
                continue;
            }
            self.prewarm_profiles_on_endpoint(&endpoint, &profiles, force)
                .await?;
        }
        Ok(())
    }

    fn resolved_warm_pool_profiles(&self) -> Vec<WarmPoolProfile> {
        if !self.inner.cfg.warm_pool_profiles.is_empty() {
            return self.inner.cfg.warm_pool_profiles.clone();
        }

        let architecture = self
            .inner
            .cfg
            .default_architecture
            .as_deref()
            .map(normalize_architecture_label)
            .filter(|value| !value.is_empty())
            .or_else(detect_host_architecture_label);

        vec![WarmPoolProfile {
            image: self.inner.cfg.default_image.clone(),
            architecture,
            min_inventory: 1,
        }]
    }

    async fn prewarm_profiles_on_endpoint(
        &self,
        endpoint: &str,
        profiles: &[WarmPoolProfile],
        strict: bool,
    ) -> Result<()> {
        for profile in profiles {
            if profile.image.trim().is_empty() {
                continue;
            }
            let architecture = profile
                .normalized_architecture()
                .or_else(detect_host_architecture_label)
                .unwrap_or_default();
            let key = warm_pool_key(endpoint, profile.image.as_str(), architecture.as_str());
            if self.warm_pool_contains_key(key.as_str()).await {
                continue;
            }
            match self
                .prewarm_profile_on_endpoint(endpoint, profile, architecture.as_str())
                .await
            {
                Ok(()) => {
                    self.warm_pool_mark_ready(key).await;
                }
                Err(err) => {
                    if strict {
                        return Err(err);
                    }
                    tracing::warn!(
                        endpoint = %endpoint,
                        image = %profile.image,
                        architecture = %architecture,
                        error = %err,
                        "warm-pool prewarm failed; continuing in best-effort mode"
                    );
                }
            }
        }
        Ok(())
    }

    async fn prewarm_profile_on_endpoint(
        &self,
        endpoint: &str,
        profile: &WarmPoolProfile,
        architecture: &str,
    ) -> Result<()> {
        let mut client = self.vmd_client_for_endpoint(endpoint).await?;
        let mut stream = client
            .pre_download_vm_image(self.request_with_auth(PreDownloadVmImageRequest {
                reference: profile.image.clone(),
                architecture: architecture.to_string(),
                force: false,
            }))
            .await?
            .into_inner();
        while stream.message().await?.is_some() {}
        Ok(())
    }

    async fn warm_pool_contains_key(&self, key: &str) -> bool {
        self.inner.warm_pool_ready.lock().await.contains(key)
    }

    async fn warm_pool_mark_ready(&self, key: String) {
        self.inner.warm_pool_ready.lock().await.insert(key);
    }

    fn build_client_tls_config(&self, endpoint: &str) -> Result<ClientTlsConfig> {
        let mut tls = ClientTlsConfig::new();
        let mut domain_set = false;
        if let Some(cfg) = self.inner.cfg.tls.as_ref() {
            if let Some(ca_path) = cfg.ca_cert_path.as_ref() {
                let ca_pem = fs::read(ca_path).map_err(|err| {
                    SandboxError::InvalidConfig(format!(
                        "read tls ca cert {}: {err}",
                        ca_path.to_string_lossy()
                    ))
                })?;
                tls = tls.ca_certificate(Certificate::from_pem(ca_pem));
            }
            match (cfg.client_cert_path.as_ref(), cfg.client_key_path.as_ref()) {
                (Some(cert_path), Some(key_path)) => {
                    let cert_pem = fs::read(cert_path).map_err(|err| {
                        SandboxError::InvalidConfig(format!(
                            "read tls client cert {}: {err}",
                            cert_path.to_string_lossy()
                        ))
                    })?;
                    let key_pem = fs::read(key_path).map_err(|err| {
                        SandboxError::InvalidConfig(format!(
                            "read tls client key {}: {err}",
                            key_path.to_string_lossy()
                        ))
                    })?;
                    tls = tls.identity(Identity::from_pem(cert_pem, key_pem));
                }
                (None, None) => {}
                _ => {
                    return Err(SandboxError::InvalidConfig(
                        "both tls client cert and key must be configured together".to_string(),
                    ));
                }
            }
            if let Some(domain_name) = cfg.domain_name.as_ref() {
                let trimmed = domain_name.trim();
                if !trimmed.is_empty() {
                    tls = tls.domain_name(trimmed.to_string());
                    domain_set = true;
                }
            }
        }
        if !domain_set {
            let host = endpoint_host(endpoint)?;
            if !host.trim().is_empty() {
                tls = tls.domain_name(host);
            }
        }
        Ok(tls)
    }

    fn request_with_auth<T>(&self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        if let Some(value) = self.inner.auth_header.as_ref() {
            request
                .metadata_mut()
                .insert("authorization", value.clone());
        }
        request
    }

    fn request_with_optional_pci_auth<T>(&self, message: T) -> Request<T> {
        let mut request = self.request_with_auth(message);
        if let Some(value) = self.inner.pci_auth_header.as_ref() {
            request
                .metadata_mut()
                .insert(PCI_CAPABILITY_HEADER, value.clone());
        }
        request
    }

    fn request_with_pci_auth<T>(&self, message: T) -> Result<Request<T>> {
        if self.inner.pci_auth_header.is_none() {
            return Err(SandboxError::InvalidConfig(
                "PCI operation requires SandboxConfig.pci_access_token".to_string(),
            ));
        }
        Ok(self.request_with_optional_pci_auth(message))
    }

    #[cfg(feature = "distributed-control")]
    async fn ensure_distributed_ready(&self) -> Result<()> {
        let start = Instant::now();
        while start.elapsed() < self.inner.cfg.daemon_start_timeout {
            for endpoint in self.candidate_endpoints().await? {
                if self.health_check_endpoint(&endpoint).await.is_ok() {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        Err(SandboxError::DaemonUnavailable(
            "no healthy distributed sandbox node discovered".to_string(),
        ))
    }

    #[cfg(feature = "host")]
    async fn spawn_daemon_if_needed(&self) -> Result<()> {
        let mut guard = self.inner.managed_daemon.lock().await;
        if guard.is_some() {
            return Ok(());
        }

        let bin = self
            .inner
            .cfg
            .daemon_bin
            .clone()
            .unwrap_or_else(|| PathBuf::from("vmd"));

        let mut cmd = Command::new(bin);
        cmd.arg("--listen").arg(&self.inner.cfg.daemon_listen);
        cmd.arg("--disable-node-registry");
        cmd.arg("--disable-control-bus");
        if let Some(data_dir) = &self.inner.cfg.daemon_data_dir {
            cmd.arg("--data-dir").arg(data_dir);
        }

        let daemon_log_dir = self
            .inner
            .cfg
            .daemon_data_dir
            .clone()
            .unwrap_or_else(|| std::env::temp_dir().join("chevalier-sandbox-daemon"));
        fs::create_dir_all(&daemon_log_dir)?;
        let stdout_log_path = daemon_log_dir.join("vmd.stdout.log");
        let stderr_log_path = daemon_log_dir.join("vmd.stderr.log");
        let stdout_log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&stdout_log_path)?;
        let stderr_log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&stderr_log_path)?;

        tracing::info!(
            listen = %self.inner.cfg.daemon_listen,
            stdout = %stdout_log_path.display(),
            stderr = %stderr_log_path.display(),
            "spawning sandbox daemon"
        );

        cmd.stdin(Stdio::null());
        cmd.stdout(Stdio::from(stdout_log));
        cmd.stderr(Stdio::from(stderr_log));

        let child = cmd.spawn()?;
        *guard = Some(ManagedDaemon { child });
        Ok(())
    }

    #[cfg(not(feature = "host"))]
    async fn spawn_daemon_if_needed(&self) -> Result<()> {
        Err(SandboxError::Unsupported(
            "local sandbox daemon autospawn requires the `host` feature; use Sandbox::connect for an external daemon or enable `local`"
                .to_string(),
        ))
    }

    async fn health_check_endpoint(&self, endpoint: &str) -> Result<()> {
        let mut client = self.vmd_client_for_endpoint(endpoint).await?;
        let _ = client
            .health(self.request_with_auth(proto::vmd::v1::HealthRequest {}))
            .await?;
        Ok(())
    }

    async fn node_multiplexer_for_endpoint(&self, endpoint: &str) -> Arc<NodePortMultiplexer> {
        let mut guard = self.inner.node_multiplexers.lock().await;
        guard
            .entry(endpoint.to_string())
            .or_insert_with(|| Arc::new(NodePortMultiplexer::default()))
            .clone()
    }

    async fn candidate_endpoints(&self) -> Result<Vec<String>> {
        let mut endpoints = Vec::new();
        endpoints.push(self.inner.cfg.endpoint.clone());
        endpoints.extend(self.inner.cfg.control_gateway_endpoints.iter().cloned());

        #[cfg(feature = "distributed-control")]
        if let ControlBackend::Distributed(control) = &self.inner.control_backend {
            for node in control.list_node_routes().await? {
                endpoints.push(node.endpoint);
            }
        }

        endpoints.sort();
        endpoints.dedup();
        Ok(endpoints)
    }

    async fn endpoint_for_new_session(
        &self,
        _session_id: &str,
        _tenant_id: &str,
        _workspace_id: &str,
        _tier_b_eligible: bool,
        _required_mount_profiles: &[String],
    ) -> Result<String> {
        #[cfg(feature = "distributed-control")]
        if let ControlBackend::Distributed(control) = &self.inner.control_backend {
            if let Some(endpoint) = control
                .get_session_route(_session_id)
                .await?
                .map(|route| route.endpoint)
                .filter(|endpoint| !endpoint.trim().is_empty())
            {
                return normalize_endpoint(&endpoint);
            }
            let node = control
                .select_node_for_session_with_eligibility(
                    _session_id,
                    _tenant_id,
                    _workspace_id,
                    _tier_b_eligible,
                    _required_mount_profiles,
                )
                .await?;
            return normalize_endpoint(&node.endpoint);
        }

        let _ = (_tier_b_eligible, _required_mount_profiles);
        for endpoint in self.candidate_endpoints().await? {
            if self.health_check_endpoint(&endpoint).await.is_ok() {
                return Ok(endpoint);
            }
        }

        Ok(self.inner.cfg.endpoint.clone())
    }

    #[cfg(feature = "distributed-control")]
    async fn publish_control_command(
        &self,
        command_type: &str,
        ordering_key: &str,
        payload: serde_json::Value,
    ) -> Result<()> {
        if let ControlBackend::Distributed(control) = &self.inner.control_backend {
            control
                .publish_command(command_type, ordering_key, payload)
                .await?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn bind_session_route(
        &self,
        _session_id: &str,
        _vm_id: &str,
        _endpoint: &str,
        _fork_id: Option<&str>,
        _tenant_id: Option<&str>,
        _workspace_id: Option<&str>,
        _tier_b_eligible: bool,
        _required_mount_profiles: Vec<String>,
        _expected_fence: Option<&str>,
    ) -> Result<Option<String>> {
        #[cfg(feature = "distributed-control")]
        if let ControlBackend::Distributed(control) = &self.inner.control_backend {
            let route = control
                .put_session_route(
                    distributed::SessionRoute {
                        session_id: _session_id.to_string(),
                        vm_id: _vm_id.to_string(),
                        endpoint: _endpoint.to_string(),
                        node_id: None,
                        fork_id: _fork_id.map(ToOwned::to_owned),
                        ownership_fence: None,
                        tenant_id: _tenant_id.unwrap_or("default").to_string(),
                        workspace_id: _workspace_id.unwrap_or("default").to_string(),
                        tier_b_eligible: _tier_b_eligible,
                        required_mount_profiles: _required_mount_profiles,
                    },
                    _expected_fence,
                )
                .await?;
            let fence = route.ownership_fence.clone();
            let tenant_id = route.tenant_id.clone();
            let workspace_id = route.workspace_id.clone();
            let _ = control
                .publish_event(
                    "session.bound",
                    serde_json::json!({
                        "session_id": _session_id,
                        "vm_id": _vm_id,
                        "endpoint": _endpoint,
                        "fork_id": _fork_id,
                        "tenant_id": tenant_id,
                        "workspace_id": workspace_id,
                        "ownership_fence": fence,
                    }),
                )
                .await;
            return Ok(route.ownership_fence);
        }
        let _ = (_tier_b_eligible, _required_mount_profiles, _expected_fence);
        Ok(None)
    }

    async fn current_session_scope(&self, session_id: &str) -> Result<(String, String)> {
        #[cfg(not(feature = "distributed-control"))]
        let _ = session_id;
        #[cfg(feature = "distributed-control")]
        if let ControlBackend::Distributed(control) = &self.inner.control_backend {
            if let Some(route) = control.get_session_route(session_id).await? {
                return Ok((route.tenant_id, route.workspace_id));
            }
        }
        Ok(("default".to_string(), "default".to_string()))
    }

    async fn session_attach_is_endpoint_handoff(
        &self,
        session_id: &str,
        node_endpoint: &str,
    ) -> Result<bool> {
        let node_endpoint = normalize_endpoint(node_endpoint)?;
        #[cfg(feature = "distributed-control")]
        if let ControlBackend::Distributed(control) = &self.inner.control_backend {
            if let Some(route_endpoint) = control
                .get_session_route(session_id)
                .await?
                .map(|route| route.endpoint)
                .filter(|endpoint| !endpoint.trim().is_empty())
            {
                return Ok(normalize_endpoint(&route_endpoint)? != node_endpoint);
            }
        }

        #[cfg(not(feature = "distributed-control"))]
        let _ = session_id;
        Ok(normalize_endpoint(&self.inner.cfg.endpoint)? != node_endpoint)
    }

    async fn clear_session_route(
        &self,
        _session_id: Option<&str>,
        _vm_id: &str,
        _expected_fence: Option<&str>,
    ) -> Result<()> {
        #[cfg(feature = "distributed-control")]
        if let (ControlBackend::Distributed(control), Some(session_id)) =
            (&self.inner.control_backend, _session_id)
        {
            control
                .delete_session_route(session_id, _expected_fence)
                .await?;
            let _ = control
                .publish_event(
                    "session.discarded",
                    serde_json::json!({
                        "session_id": session_id,
                        "vm_id": _vm_id,
                        "ownership_fence": _expected_fence,
                    }),
                )
                .await;
        }
        Ok(())
    }

    async fn current_session_fence(&self, session_id: &str) -> Result<Option<String>> {
        #[cfg(feature = "distributed-control")]
        if let ControlBackend::Distributed(control) = &self.inner.control_backend {
            return Ok(control
                .get_session_route(session_id)
                .await?
                .and_then(|route| route.ownership_fence));
        }
        let _ = session_id;
        Ok(None)
    }

    async fn ensure_vm_and_get_rpc_access_for_session(
        &self,
        session_id: &str,
        vm_id: &str,
        endpoint: &str,
        expected_fence: Option<&str>,
    ) -> Result<(String, GuestRpcAccess, Option<String>)> {
        let normalized_endpoint = normalize_endpoint(endpoint)?;
        if let Some(access) = self
            .cached_guest_rpc_access(vm_id, &normalized_endpoint)
            .await
        {
            return Ok((normalized_endpoint, access, None));
        }

        let (resolved_endpoint, next_fence) = self
            .resolve_session_endpoint(session_id, vm_id, endpoint, expected_fence)
            .await?;
        match self
            .ensure_vm_and_get_rpc_access(vm_id, &resolved_endpoint)
            .await
        {
            Ok(access) => Ok((resolved_endpoint, access, next_fence)),
            Err(err) => {
                if !is_rebind_candidate_error(&err) {
                    return Err(err);
                }

                // @dive: If guest RPC readiness fails due transport loss, try cross-node rebind before surfacing failure.
                if let Some((candidate_endpoint, rebound_fence)) = self
                    .rebind_session_endpoint(
                        session_id,
                        vm_id,
                        &resolved_endpoint,
                        next_fence.as_deref().or(expected_fence),
                        "ensure_vm_and_get_rpc_port",
                        RebindRestorePolicy::RequireTierBRestoreMarker,
                    )
                    .await?
                {
                    let access = self
                        .ensure_vm_and_get_rpc_access(vm_id, &candidate_endpoint)
                        .await?;
                    return Ok((candidate_endpoint, access, rebound_fence.or(next_fence)));
                }

                Err(err)
            }
        }
    }

    async fn resolve_session_endpoint(
        &self,
        session_id: &str,
        vm_id: &str,
        endpoint: &str,
        expected_fence: Option<&str>,
    ) -> Result<(String, Option<String>)> {
        self.resolve_session_endpoint_with_policy(
            session_id,
            vm_id,
            endpoint,
            expected_fence,
            "resolve_session_endpoint",
            RebindRestorePolicy::RequireTierBRestoreMarker,
        )
        .await
    }

    #[allow(dead_code)]
    async fn resolve_session_endpoint_for_active_stream(
        &self,
        session_id: &str,
        vm_id: &str,
        endpoint: &str,
        expected_fence: Option<&str>,
    ) -> Result<(String, Option<String>)> {
        self.resolve_session_endpoint_with_policy(
            session_id,
            vm_id,
            endpoint,
            expected_fence,
            "resolve_active_stream_endpoint",
            RebindRestorePolicy::AllowLiveReclaimWithoutRestoreMarker,
        )
        .await
    }

    async fn resolve_session_endpoint_with_policy(
        &self,
        session_id: &str,
        vm_id: &str,
        endpoint: &str,
        expected_fence: Option<&str>,
        reason: &str,
        restore_policy: RebindRestorePolicy,
    ) -> Result<(String, Option<String>)> {
        let normalized_endpoint = normalize_endpoint(endpoint)?;
        match self.ensure_vm_running(vm_id, &normalized_endpoint).await {
            Ok(_) => Ok((normalized_endpoint, None)),
            Err(initial_err) => {
                if let Some((rebound_endpoint, next_fence)) = self
                    .rebind_session_endpoint(
                        session_id,
                        vm_id,
                        &normalized_endpoint,
                        expected_fence,
                        reason,
                        restore_policy,
                    )
                    .await?
                {
                    return Ok((rebound_endpoint, next_fence));
                }
                Err(initial_err)
            }
        }
    }

    async fn rebind_session_endpoint(
        &self,
        session_id: &str,
        vm_id: &str,
        from_endpoint: &str,
        expected_fence: Option<&str>,
        reason: &str,
        restore_policy: RebindRestorePolicy,
    ) -> Result<Option<(String, Option<String>)>> {
        let candidates = {
            #[cfg(feature = "distributed-control")]
            if let ControlBackend::Distributed(control) = &self.inner.control_backend {
                control
                    .rebind_candidates_for_session(session_id, from_endpoint)
                    .await?
                    .into_iter()
                    .map(|route| route.endpoint)
                    .collect::<Vec<_>>()
            } else {
                self.candidate_endpoints().await?
            }

            #[cfg(not(feature = "distributed-control"))]
            {
                self.candidate_endpoints().await?
            }
        };

        for candidate in candidates {
            if candidate == from_endpoint {
                continue;
            }
            let Some(candidate_vm) = self.find_vm_by_id_on_endpoint(vm_id, &candidate).await?
            else {
                continue;
            };

            #[cfg(feature = "distributed-control")]
            if let ControlBackend::Distributed(control) = &self.inner.control_backend {
                // @dive: Rebind events include the trigger reason so failover behavior can be audited per stage.
                let _ = control
                    .publish_event(
                        "stream.rebinding",
                        json!({
                            "session_id": session_id,
                            "vm_id": vm_id,
                            "from_endpoint": from_endpoint,
                            "to_endpoint": candidate.clone(),
                            "expected_fence": expected_fence,
                            "reason": reason,
                        }),
                    )
                    .await;
            }

            let restore_snapshot_id = match self
                .restore_execution_state_if_needed(
                    session_id,
                    vm_id,
                    &candidate,
                    &candidate_vm,
                    restore_policy.enforce_tier_b_restore_marker(),
                )
                .await
            {
                Ok(snapshot_id) => snapshot_id,
                Err(err) => {
                    if matches!(err, SandboxError::InvalidResponse(_)) {
                        return Err(err);
                    }
                    #[cfg(feature = "distributed-control")]
                    if let ControlBackend::Distributed(control) = &self.inner.control_backend {
                        let _ = control
                            .publish_event(
                                "stream.failed",
                                json!({
                                    "session_id": session_id,
                                    "vm_id": vm_id,
                                    "from_endpoint": from_endpoint,
                                    "to_endpoint": candidate.clone(),
                                    "stage": "execution_state_restore",
                                    "reason": reason,
                                    "error": err.to_string(),
                                }),
                            )
                            .await;
                    }
                    continue;
                }
            };

            let running_vm = if restore_policy.preflight_candidate_runtime() {
                match self.ensure_vm_running(vm_id, &candidate).await {
                    Ok(vm) => vm,
                    Err(_) => {
                        #[cfg(feature = "distributed-control")]
                        if let ControlBackend::Distributed(control) = &self.inner.control_backend {
                            let _ = control
                                .publish_event(
                                    "stream.failed",
                                    json!({
                                        "session_id": session_id,
                                        "vm_id": vm_id,
                                        "from_endpoint": from_endpoint,
                                        "to_endpoint": candidate.clone(),
                                        "stage": "ensure_vm_running",
                                        "reason": reason,
                                    }),
                                )
                                .await;
                        }
                        continue;
                    }
                }
            } else {
                candidate_vm.clone()
            };

            if restore_policy.preflight_candidate_runtime()
                && self
                    .ensure_vm_and_get_rpc_port(vm_id, &candidate)
                    .await
                    .is_err()
            {
                #[cfg(feature = "distributed-control")]
                if let ControlBackend::Distributed(control) = &self.inner.control_backend {
                    let _ = control
                        .publish_event(
                            "stream.failed",
                            json!({
                                "session_id": session_id,
                                "vm_id": vm_id,
                                "from_endpoint": from_endpoint,
                                "to_endpoint": candidate.clone(),
                                "stage": "ensure_guest_rpc_ready",
                                "reason": reason,
                            }),
                        )
                        .await;
                }
                continue;
            }

            if !restore_policy.preflight_candidate_runtime() {
                #[cfg(feature = "distributed-control")]
                if let ControlBackend::Distributed(control) = &self.inner.control_backend {
                    let _ = control
                        .publish_event(
                            "stream.resume_preflight_skipped",
                            json!({
                                "session_id": session_id,
                                "vm_id": vm_id,
                                "from_endpoint": from_endpoint,
                                "to_endpoint": candidate.clone(),
                                "reason": reason,
                            }),
                        )
                        .await;
                }
            }

            #[cfg(feature = "distributed-control")]
            let mut next_fence = None;
            #[cfg(not(feature = "distributed-control"))]
            let next_fence = None;
            #[cfg(feature = "distributed-control")]
            if let ControlBackend::Distributed(_control) = &self.inner.control_backend {
                let (tenant_id, workspace_id) = self.current_session_scope(session_id).await?;
                next_fence = self
                    .bind_session_route(
                        session_id,
                        vm_id,
                        &candidate,
                        None,
                        Some(tenant_id.as_str()),
                        Some(workspace_id.as_str()),
                        vm_tier_b_eligible(&running_vm),
                        vm_required_shared_mount_profiles(&running_vm),
                        expected_fence,
                    )
                    .await?;
            }
            #[cfg(not(feature = "distributed-control"))]
            let _ = &running_vm;

            self.invalidate_ready_vm_rpc(vm_id, from_endpoint).await;

            #[cfg(feature = "distributed-control")]
            if let ControlBackend::Distributed(control) = &self.inner.control_backend {
                let _ = control
                    .publish_event(
                        "session.rebound",
                        json!({
                            "session_id": session_id,
                            "vm_id": vm_id,
                            "from_endpoint": from_endpoint,
                            "to_endpoint": candidate.clone(),
                            "expected_fence": expected_fence,
                            "reason": reason,
                        }),
                    )
                    .await;
                let _ = control
                    .publish_event(
                        "stream.rebound",
                        json!({
                            "session_id": session_id,
                            "vm_id": vm_id,
                            "from_endpoint": from_endpoint,
                            "to_endpoint": candidate.clone(),
                            "restored_snapshot_id": restore_snapshot_id,
                            "expected_fence": expected_fence,
                            "reason": reason,
                        }),
                    )
                    .await;
            } else {
                let _ = restore_snapshot_id;
            }
            #[cfg(not(feature = "distributed-control"))]
            let _ = &restore_snapshot_id;

            return Ok(Some((candidate, next_fence)));
        }

        let _ = (expected_fence, reason);
        Ok(None)
    }

    async fn restore_execution_state_if_needed(
        &self,
        _session_id: &str,
        vm_id: &str,
        endpoint: &str,
        vm: &Vm,
        enforce_tier_b: bool,
    ) -> Result<Option<String>> {
        let tier_b_eligible = vm_tier_b_eligible(vm);
        let Some(snapshot_id) = execution_restore_snapshot_id(vm) else {
            if enforce_tier_b && tier_b_eligible {
                return Err(SandboxError::InvalidResponse(format!(
                    "tier_b_eligible session `{_session_id}` requires execution restore snapshot marker"
                )));
            }
            tracing::info!(
                session_id = %_session_id,
                vm_id = %vm_id,
                endpoint = %endpoint,
                tier_b_eligible = tier_b_eligible,
                enforce_tier_b = enforce_tier_b,
                "execution restore skipped (no snapshot marker)"
            );
            return Ok(None);
        };
        tracing::info!(
            session_id = %_session_id,
            vm_id = %vm_id,
            endpoint = %endpoint,
            snapshot_id = %snapshot_id,
            tier_b_eligible = tier_b_eligible,
            enforce_tier_b = enforce_tier_b,
            "execution restore snapshot requested"
        );

        let mut client = self.vmd_client_for_endpoint(endpoint).await?;
        client
            .restore_snapshot(self.request_with_auth(RestoreSnapshotRequest {
                vm_id: vm_id.to_string(),
                snapshot_id: snapshot_id.clone(),
            }))
            .await?;

        #[cfg(feature = "distributed-control")]
        if let ControlBackend::Distributed(control) = &self.inner.control_backend {
            let _ = control
                .publish_event(
                    "execution_state.restored",
                    json!({
                        "session_id": _session_id,
                        "vm_id": vm_id,
                        "endpoint": endpoint,
                        "snapshot_id": snapshot_id,
                        "tier_b_eligible": tier_b_eligible,
                    }),
                )
                .await;
        }
        tracing::info!(
            session_id = %_session_id,
            vm_id = %vm_id,
            endpoint = %endpoint,
            snapshot_id = %snapshot_id,
            "execution restore snapshot applied"
        );
        let _ = (_session_id, enforce_tier_b);
        Ok(Some(snapshot_id))
    }

    async fn find_vm_by_id_on_endpoint(&self, vm_id: &str, endpoint: &str) -> Result<Option<Vm>> {
        let mut client = match self.vmd_client_for_endpoint(endpoint).await {
            Ok(client) => client,
            Err(_) => return Ok(None),
        };
        let request = self.request_with_auth(GetVmRequest {
            vm_id: vm_id.to_string(),
        });
        let response =
            tokio::time::timeout(self.inner.cfg.connect_timeout, client.get_vm(request)).await;
        match response {
            Err(_) => Ok(None),
            Ok(Ok(response)) => Ok(Some(response.into_inner())),
            Ok(Err(status)) if status.code() == tonic::Code::NotFound => Ok(None),
            Ok(Err(_)) => Ok(None),
        }
    }

    async fn find_vm_by_session_id(&self, session_id: &str) -> Result<Option<(Vm, String)>> {
        let mut successful_lookup = false;
        let mut last_error = None;
        let mut attempted = HashSet::new();
        #[cfg(feature = "distributed-control")]
        match &self.inner.control_backend {
            ControlBackend::Distributed(control) => {
                if let Some(endpoint) = control
                    .get_session_route(session_id)
                    .await?
                    .map(|route| route.endpoint)
                    .filter(|endpoint| !endpoint.trim().is_empty())
                {
                    let endpoint = normalize_endpoint(&endpoint)?;
                    if !control.allows_cross_node_recovery() {
                        return self
                            .find_vm_by_session_id_on_endpoint(session_id, &endpoint)
                            .await
                            .map(|vm| vm.map(|vm| (vm, endpoint)));
                    }
                    attempted.insert(endpoint.clone());
                    match self
                        .find_vm_by_session_id_on_endpoint(session_id, &endpoint)
                        .await
                    {
                        Ok(Some(vm)) => return Ok(Some((vm, endpoint))),
                        Ok(None) => successful_lookup = true,
                        Err(error) => last_error = Some(error),
                    }
                }
            }
            ControlBackend::Direct | ControlBackend::Managed(_) => {}
        }

        for endpoint in self.candidate_endpoints().await? {
            if !attempted.insert(endpoint.clone()) {
                continue;
            }
            match self
                .find_vm_by_session_id_on_endpoint(session_id, &endpoint)
                .await
            {
                Ok(Some(vm)) => return Ok(Some((vm, endpoint))),
                Ok(None) => successful_lookup = true,
                Err(error) => last_error = Some(error),
            }
        }

        if successful_lookup {
            Ok(None)
        } else if let Some(error) = last_error {
            Err(error)
        } else {
            Err(SandboxError::DaemonUnavailable(
                "no sandbox endpoint available for session registry lookup".to_string(),
            ))
        }
    }

    async fn find_vm_by_session_id_on_endpoint(
        &self,
        session_id: &str,
        endpoint: &str,
    ) -> Result<Option<Vm>> {
        let mut client = self.vmd_client_for_endpoint(endpoint).await?;
        let request = self.request_with_auth(GetVmBySessionRequest {
            session_id: session_id.to_string(),
        });
        match tokio::time::timeout(
            self.inner.cfg.connect_timeout,
            client.get_vm_by_session(request),
        )
        .await
        {
            Ok(Ok(response)) => Ok(Some(response.into_inner())),
            Ok(Err(status)) if status.code() == tonic::Code::NotFound => Ok(None),
            Ok(Err(status)) if status.code() == tonic::Code::Unimplemented => {
                // Rolling-upgrade compatibility for a daemon that predates the
                // explicit persisted session registry.
                let response = client
                    .list_v_ms(self.request_with_auth(ListVMsRequest {
                        include_snapshots: false,
                    }))
                    .await
                    .map_err(SandboxError::Grpc)?
                    .into_inner();
                Ok(response.vms.into_iter().find(|vm| {
                    vm.metadata.get(META_SESSION_ID).map(String::as_str) == Some(session_id)
                }))
            }
            Ok(Err(status)) => Err(SandboxError::Grpc(status)),
            Err(_) => Err(SandboxError::DaemonUnavailable(format!(
                "session registry lookup timed out at {endpoint}"
            ))),
        }
    }

    async fn ensure_vm_running(&self, vm_id: &str, endpoint: &str) -> Result<Vm> {
        Ok(self.ensure_vm_running_tracked(vm_id, endpoint).await?.vm)
    }

    /// Like [`Self::ensure_vm_running`], but also reports whether this call had to
    /// start or resume the VM. Readiness policy depends on that distinction: a VM
    /// that was already running and stops answering has a stale sidecar, while a VM
    /// we just booted is simply still booting.
    async fn ensure_vm_running_tracked(&self, vm_id: &str, endpoint: &str) -> Result<EnsuredVm> {
        let mut client = self.vmd_client_for_endpoint(endpoint).await?;
        let mut vm = client
            .get_vm(self.request_with_auth(GetVmRequest {
                vm_id: vm_id.to_string(),
            }))
            .await?
            .into_inner();

        let is_running = vm.state == proto::vmd::v1::VmState::Running as i32;
        let mut freshly_started = false;
        if !is_running {
            self.invalidate_ready_vm_rpc(vm_id, endpoint).await;
            let action_request = self.request_with_auth(VmActionRequest {
                vm_id: vm_id.to_string(),
            });
            vm = if vm.state == proto::vmd::v1::VmState::Paused as i32 {
                client.resume_vm(action_request).await?.into_inner()
            } else {
                client.start_vm(action_request).await?.into_inner()
            };
            freshly_started = true;
        }

        Ok(EnsuredVm {
            vm,
            freshly_started,
        })
    }

    async fn invalidate_ready_vm_rpc(&self, vm_id: &str, endpoint: &str) {
        let cache_key = ready_key(endpoint, vm_id);
        let mut ready = self.inner.ready_vm_rpc.lock().await;
        ready.remove(&cache_key);
        drop(ready);
        let mut channels = self.inner.portproxy_channels.lock().await;
        channels.remove(&cache_key);
    }

    /// Gracefully restart a VM on its node. `reason` is the failure that motivated the
    /// restart; it is carried into any restart error so the original cause is never
    /// masked by the recovery attempt.
    async fn restart_vm_on_endpoint(
        &self,
        vm_id: &str,
        endpoint: &str,
        reason: &str,
    ) -> Result<()> {
        let mut client = self
            .vmd_client_for_endpoint_with_timeout(endpoint, VMD_RESTART_TIMEOUT)
            .await?;
        if let Err(err) = client
            .restart_vm(self.request_with_auth(VmActionRequest {
                vm_id: vm_id.to_string(),
            }))
            .await
        {
            self.invalidate_ready_vm_rpc(vm_id, endpoint).await;
            return Err(SandboxError::DaemonUnavailable(format!(
                "restart of vm {vm_id} on {endpoint} (triggered by: {reason}) failed: {err}"
            )));
        }
        self.invalidate_ready_vm_rpc(vm_id, endpoint).await;
        Ok(())
    }

    async fn ensure_vm_and_get_rpc_port(&self, vm_id: &str, endpoint: &str) -> Result<i32> {
        Ok(self
            .ensure_vm_and_get_rpc_access(vm_id, endpoint)
            .await?
            .rpc_port)
    }

    async fn maybe_wait_for_session_guest_rpc(
        &self,
        vm_id: &str,
        endpoint: &str,
        recovery: ReadinessRecovery,
    ) -> Result<()> {
        #[cfg(feature = "distributed-control")]
        if matches!(&self.inner.control_backend, ControlBackend::Distributed(_)) {
            // Distributed sessions are route-bound before guest RPC is required; exec/watch paths
            // own readiness probing so a slow portproxy cannot orphan an otherwise valid session.
            return Ok(());
        }

        let _ = self
            .ensure_vm_and_get_rpc_access_with(vm_id, endpoint, recovery)
            .await?;
        Ok(())
    }

    async fn ensure_vm_and_get_rpc_access(
        &self,
        vm_id: &str,
        endpoint: &str,
    ) -> Result<GuestRpcAccess> {
        self.ensure_vm_and_get_rpc_access_with(
            vm_id,
            endpoint,
            ReadinessRecovery::RestartIfNotFreshlyStarted,
        )
        .await
    }

    async fn ensure_vm_and_get_rpc_access_with(
        &self,
        vm_id: &str,
        endpoint: &str,
        recovery: ReadinessRecovery,
    ) -> Result<GuestRpcAccess> {
        if let Some(access) = self.cached_guest_rpc_access(vm_id, endpoint).await {
            return Ok(access);
        }

        let EnsuredVm {
            vm,
            freshly_started,
        } = self.ensure_vm_running_tracked(vm_id, endpoint).await?;
        let access = self.guest_rpc_access_from_vm(&vm, endpoint)?;
        let fresh_boot = freshly_started || recovery == ReadinessRecovery::FreshBoot;

        match self.ensure_portproxy_ready(vm_id, endpoint, &access).await {
            Ok(()) => Ok(access),
            // @dive: A VM this operation just created, started, or resumed is still
            //        booting; a missed readiness budget is reported as-is and the VM is
            //        left running for the caller to decide. Restarting it here would only
            //        mask the real cause and discard the boot in progress.
            Err(err) if fresh_boot => Err(err),
            // @dive: A VM that was already running can survive daemon fail-stop while its
            //        guest RPC sidecars are stale; force exactly one bounded local restart
            //        before cross-node escalation, keeping the original failure attached.
            Err(err @ SandboxError::GuestRpcNotReady { .. }) => {
                self.restart_stale_sidecar_and_wait(vm_id, endpoint, err)
                    .await
            }
            Err(err) if is_rebind_candidate_error(&err) => {
                self.restart_stale_sidecar_and_wait(vm_id, endpoint, err)
                    .await
            }
            Err(err) => Err(err),
        }
    }

    async fn restart_stale_sidecar_and_wait(
        &self,
        vm_id: &str,
        endpoint: &str,
        cause: SandboxError,
    ) -> Result<GuestRpcAccess> {
        let reason = cause.to_string();
        let already_waited = match &cause {
            SandboxError::GuestRpcNotReady { waited, .. } => *waited,
            _ => Duration::ZERO,
        };
        self.invalidate_ready_vm_rpc(vm_id, endpoint).await;
        self.restart_vm_on_endpoint(vm_id, endpoint, &reason)
            .await?;
        let vm = self.ensure_vm_running(vm_id, endpoint).await?;
        let access = self.guest_rpc_access_from_vm(&vm, endpoint)?;
        match self.ensure_portproxy_ready(vm_id, endpoint, &access).await {
            Ok(()) => Ok(access),
            // @dive: The restart is the one local remedy. If the guest is still silent,
            //        surface the same typed cause (total wait, restart noted) so exec/attach
            //        callers can escalate cross-node exactly as they did before, without
            //        the readiness failure being rewritten into a channel error.
            Err(SandboxError::GuestRpcNotReady {
                vm_id,
                endpoint,
                waited,
                ..
            }) => Err(SandboxError::GuestRpcNotReady {
                vm_id,
                endpoint,
                waited: already_waited + waited,
                restarted: true,
            }),
            Err(err) => Err(err),
        }
    }

    fn guest_rpc_access_from_vm(&self, vm: &Vm, endpoint: &str) -> Result<GuestRpcAccess> {
        let rpc_port = vm
            .network
            .as_ref()
            .and_then(|network| network.portproxy_ports.as_ref())
            .map(|ports| ports.rpc_port)
            .filter(|port| *port > 0)
            .ok_or_else(|| SandboxError::InvalidResponse("VM missing rpc port".into()))?;
        Ok(GuestRpcAccess {
            endpoint: rpc_endpoint(&self.inner.cfg.endpoint_overrides, endpoint, rpc_port)?,
            rpc_port,
            auth_header: portproxy_auth_header_from_metadata(&vm.metadata)?,
            platform: vm_guest_platform(vm),
        })
    }

    async fn cached_guest_rpc_access(&self, vm_id: &str, endpoint: &str) -> Option<GuestRpcAccess> {
        let cache_key = ready_key(endpoint, vm_id);
        let ready = self.inner.ready_vm_rpc.lock().await;
        ready.get(&cache_key).cloned()
    }

    async fn ensure_portproxy_ready(
        &self,
        vm_id: &str,
        endpoint: &str,
        access: &GuestRpcAccess,
    ) -> Result<()> {
        let cache_key = ready_key(endpoint, vm_id);
        let cached_ready = {
            let ready = self.inner.ready_vm_rpc.lock().await;
            ready
                .get(&cache_key)
                .map(|cached| {
                    cached.rpc_port == access.rpc_port
                        && cached.endpoint == access.endpoint
                        && cached.platform == access.platform
                })
                .unwrap_or(false)
        };
        if cached_ready {
            return Ok(());
        }

        let start = Instant::now();
        let mut consecutive_successes = 0u8;

        while start.elapsed() < self.inner.cfg.portproxy_ready_timeout {
            if probe_shell_exec_ready(
                access.endpoint.as_str(),
                self.inner.cfg.connect_timeout,
                access.auth_header.as_ref(),
                access.platform,
            )
            .await
            {
                consecutive_successes = consecutive_successes.saturating_add(1);
                if consecutive_successes < 2 {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }

                let mut ready = self.inner.ready_vm_rpc.lock().await;
                ready.insert(cache_key.clone(), access.clone());
                return Ok(());
            }
            consecutive_successes = 0;
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        Err(SandboxError::GuestRpcNotReady {
            vm_id: vm_id.to_string(),
            endpoint: access.endpoint.clone(),
            waited: start.elapsed(),
            restarted: false,
        })
    }

    async fn portproxy_client_for_access(
        &self,
        vm_id: &str,
        endpoint: &str,
        access: GuestRpcAccess,
    ) -> Result<PortproxyClientAccess> {
        let channel_entry = self
            .portproxy_channel_entry(vm_id, endpoint, &access.endpoint)
            .await;
        let connect_timeout = self.inner.cfg.connect_timeout;
        let channel = channel_entry
            .channel
            .get_or_try_init(|| async {
                Endpoint::from_shared(access.endpoint.clone())
                    .map_err(|err| SandboxError::InvalidConfig(err.to_string()))?
                    .connect_timeout(connect_timeout)
                    .connect()
                    .await
                    .map_err(SandboxError::Transport)
            })
            .await?
            .clone();
        Ok(PortproxyClientAccess {
            client: PortProxyClient::new(channel),
            auth_header: access.auth_header,
        })
    }

    async fn portproxy_channel_entry(
        &self,
        vm_id: &str,
        endpoint: &str,
        rpc_endpoint: &str,
    ) -> Arc<PortproxyChannelEntry> {
        // @dive: tonic Channel clones multiplex RPCs over one reconnecting
        // HTTP/2 transport. The OnceCell prevents a cold parallel traversal
        // from opening one TCP connection per file before the first connects.
        let cache_key = ready_key(endpoint, vm_id);
        let mut channels = self.inner.portproxy_channels.lock().await;
        if let Some(entry) = channels.get(&cache_key) {
            if entry.endpoint == rpc_endpoint {
                return Arc::clone(entry);
            }
        }
        let entry = Arc::new(PortproxyChannelEntry {
            endpoint: rpc_endpoint.to_string(),
            channel: OnceCell::new(),
        });
        channels.insert(cache_key, Arc::clone(&entry));
        entry
    }

    async fn discard_vm(
        &self,
        vm_id: &str,
        endpoint: &str,
        session_id: Option<&str>,
        expected_fence: Option<&str>,
    ) -> Result<()> {
        #[cfg(feature = "distributed-control")]
        self.publish_control_command(
            "session.discard",
            session_id.unwrap_or(vm_id),
            json!({
                "session_id": session_id,
                "vm_id": vm_id,
                "endpoint": endpoint,
                "expected_fence": expected_fence,
            }),
        )
        .await?;

        let mut client = self
            .vmd_client_for_endpoint_with_timeout(endpoint, VMD_DELETE_TIMEOUT)
            .await?;
        // `DeleteVm` owns the destructive lifecycle boundary. In particular,
        // vmd drains every live mount's publication cursor before it stops qemu.
        // Pre-stopping here would tear down the publisher under the shorter
        // exit-reaper deadline and can leave an otherwise healthy WAL suffix
        // unpublished, forcing the fail-closed delete to retain the VM.
        client
            .delete_vm(self.request_with_auth(proto::vmd::v1::DeleteVmRequest {
                vm_id: vm_id.to_string(),
                purge_snapshots: true,
            }))
            .await?;
        self.invalidate_ready_vm_rpc(vm_id, endpoint).await;
        self.clear_session_route(session_id, vm_id, expected_fence)
            .await?;
        Ok(())
    }
}

async fn probe_shell_exec_ready(
    endpoint: &str,
    establish_timeout: Duration,
    auth_header: Option<&MetadataValue<Ascii>>,
    platform: GuestPlatform,
) -> bool {
    let Ok(probe_endpoint) = Endpoint::from_shared(endpoint.to_string()) else {
        return false;
    };
    let Ok(channel) = probe_endpoint
        .connect_timeout(establish_timeout)
        .connect()
        .await
    else {
        return false;
    };
    let mut client = ShellExecClient::new(channel);

    let (req_tx, req_rx) = mpsc::channel(2);
    if req_tx
        .send(ExecRequest {
            request: Some(exec_request::Request::Start(ExecStart {
                execution_id: String::new(),
                args: guest_shell_args(platform, None, "/bin/sh", "true"),
                env: HashMap::new(),
                detach: false,
                timeout: Some(5),
                run_as_root: false,
            })),
        })
        .await
        .is_err()
    {
        return false;
    }
    drop(req_tx);

    let exec_result = tokio::time::timeout(
        establish_timeout,
        client.exec(request_with_optional_auth(
            ReceiverStream::new(req_rx),
            auth_header,
        )),
    )
    .await;

    let exec_result = match exec_result {
        Ok(value) => value,
        Err(_) => return false,
    };

    match exec_result {
        Ok(response) => {
            let probe_read = async {
                let mut stream = response.into_inner();
                let mut saw_any_frame = false;
                let mut saw_exit_code = false;

                while let Some(frame) = stream.message().await.transpose() {
                    match frame {
                        Ok(ExecResponse {
                            response: Some(exec_response::Response::ExitCode(_)),
                        }) => {
                            saw_any_frame = true;
                            saw_exit_code = true;
                            break;
                        }
                        Ok(_) => {
                            saw_any_frame = true;
                        }
                        Err(_) => return false,
                    }
                }

                saw_any_frame && saw_exit_code
            };

            tokio::time::timeout(establish_timeout, probe_read)
                .await
                .unwrap_or(false)
        }
        Err(_) => false,
    }
}

impl Drop for SandboxInner {
    fn drop(&mut self) {
        #[cfg(feature = "host")]
        if let Ok(mut guard) = self.managed_daemon.try_lock() {
            if let Some(managed) = guard.as_mut() {
                let _ = managed.child.start_kill();
            }
            *guard = None;
        }
    }
}

fn compile_auth_header(raw_token: Option<&str>) -> Result<Option<MetadataValue<Ascii>>> {
    let Some(raw_token) = raw_token else {
        return Ok(None);
    };
    let trimmed = raw_token.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let value = if trimmed.starts_with("Bearer ") {
        trimmed.to_string()
    } else {
        format!("Bearer {trimmed}")
    };
    let metadata = MetadataValue::try_from(value.as_str()).map_err(|err| {
        SandboxError::InvalidConfig(format!("authorization token is not valid ASCII: {err}"))
    })?;
    Ok(Some(metadata))
}

fn compile_metadata_token(
    raw_token: Option<&str>,
    label: &str,
) -> Result<Option<MetadataValue<Ascii>>> {
    let Some(token) = raw_token.map(str::trim).filter(|token| !token.is_empty()) else {
        return Ok(None);
    };
    let metadata = MetadataValue::try_from(token).map_err(|error| {
        SandboxError::InvalidConfig(format!("{label} is not valid ASCII: {error}"))
    })?;
    Ok(Some(metadata))
}

fn portproxy_auth_header_from_metadata(
    metadata: &HashMap<String, String>,
) -> Result<Option<MetadataValue<Ascii>>> {
    compile_auth_header(
        metadata
            .get(META_PORTPROXY_AUTH_TOKEN)
            .map(String::as_str)
            .map(str::trim)
            .filter(|token| !token.is_empty()),
    )
}

fn request_with_optional_auth<T>(
    message: T,
    auth_header: Option<&MetadataValue<Ascii>>,
) -> Request<T> {
    let mut request = Request::new(message);
    if let Some(value) = auth_header {
        request
            .metadata_mut()
            .insert("authorization", value.clone());
    }
    request
}

fn request_with_optional_auth_timeout<T>(
    message: T,
    auth_header: Option<&MetadataValue<Ascii>>,
    timeout: Duration,
) -> Request<T> {
    let mut request = request_with_optional_auth(message, auth_header);
    request.set_timeout(timeout);
    request
}

fn configured_portproxy_timeout(name: &str, default: Duration) -> Result<Duration> {
    let Ok(raw) = std::env::var(name) else {
        return Ok(default);
    };
    let millis = raw
        .trim()
        .parse::<u64>()
        .map_err(|err| SandboxError::InvalidConfig(format!("invalid {name}: {err}")))?;
    if millis == 0 {
        return Err(SandboxError::InvalidConfig(format!(
            "invalid {name}: timeout must be greater than zero"
        )));
    }
    Ok(Duration::from_millis(millis))
}

fn normalize_endpoint(raw: &str) -> Result<String> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(SandboxError::InvalidEndpoint(
            "endpoint must not be empty".to_string(),
        ));
    }
    if value.contains("://") {
        return Ok(value.to_string());
    }
    Ok(format!("http://{value}"))
}

fn portproxy_server_addr(
    endpoint_overrides: &HashMap<String, String>,
    endpoint: &str,
    proxy_port: u16,
) -> Result<String> {
    let normalized = normalize_endpoint(endpoint)?;
    let effective = endpoint_overrides
        .get(&normalized)
        .map(String::as_str)
        .unwrap_or(normalized.as_str());
    let host = endpoint_host(effective)?;
    if host.contains(':') {
        return Ok(format!("[{host}]:{proxy_port}"));
    }
    Ok(format!("{host}:{proxy_port}"))
}

fn rpc_endpoint(
    endpoint_overrides: &HashMap<String, String>,
    daemon_endpoint: &str,
    rpc_port: i32,
) -> Result<String> {
    let proxy_port = u16::try_from(rpc_port).map_err(|_| {
        SandboxError::InvalidResponse(format!("invalid rpc port from vm metadata: {rpc_port}"))
    })?;
    let addr = portproxy_server_addr(endpoint_overrides, daemon_endpoint, proxy_port)?;
    Ok(format!("http://{addr}"))
}

fn endpoint_host(endpoint: &str) -> Result<String> {
    let normalized = normalize_endpoint(endpoint)?;
    let without_scheme = normalized
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(normalized.as_str());
    let authority = without_scheme.split('/').next().ok_or_else(|| {
        SandboxError::InvalidEndpoint(format!("endpoint missing authority: {normalized}"))
    })?;
    if authority.is_empty() {
        return Err(SandboxError::InvalidEndpoint(format!(
            "endpoint missing authority: {normalized}"
        )));
    }
    let authority = authority.rsplit('@').next().unwrap_or(authority);

    if authority.starts_with('[') {
        if let Some(close_idx) = authority.find(']') {
            let host = &authority[1..close_idx];
            if host.is_empty() {
                return Err(SandboxError::InvalidEndpoint(format!(
                    "endpoint has empty host: {normalized}"
                )));
            }
            return Ok(normalize_dial_host(host).to_string());
        }
        return Err(SandboxError::InvalidEndpoint(format!(
            "endpoint has malformed ipv6 host: {normalized}"
        )));
    }

    if let Some((host, _port)) = authority.rsplit_once(':')
        && !host.is_empty()
        && !host.contains(':')
    {
        return Ok(normalize_dial_host(host).to_string());
    }

    Ok(normalize_dial_host(authority).to_string())
}

fn normalize_dial_host(host: &str) -> &str {
    match host {
        "0.0.0.0" => "127.0.0.1",
        "::" => "::1",
        _ => host,
    }
}

fn ready_key(endpoint: &str, vm_id: &str) -> String {
    format!("{endpoint}::{vm_id}")
}

fn warm_pool_key(endpoint: &str, image: &str, architecture: &str) -> String {
    format!(
        "{endpoint}::{image}::{}",
        normalize_architecture_label(architecture)
    )
}

fn normalize_architecture_label(raw: &str) -> String {
    match raw.trim().to_ascii_lowercase().as_str() {
        "x86_64" | "amd64" => "amd64".to_string(),
        "aarch64" | "arm64" => "arm64".to_string(),
        other => other.to_string(),
    }
}

fn detect_host_architecture_label() -> Option<String> {
    let detected = normalize_architecture_label(std::env::consts::ARCH);
    if detected.is_empty() {
        None
    } else {
        Some(detected.to_string())
    }
}

fn resolve_tier_b_eligibility(metadata: &HashMap<String, String>) -> bool {
    if let Some(raw) = metadata
        .get(META_TIER_B_ELIGIBLE)
        .or_else(|| metadata.get("tier_b_eligible"))
    {
        return parse_bool_like(raw).unwrap_or(true);
    }
    true
}

fn vm_tier_b_eligible(vm: &Vm) -> bool {
    resolve_tier_b_eligibility(&vm.metadata)
}

fn vm_has_guest_rpc(vm: &Vm) -> bool {
    let _ = vm;
    true
}

fn vm_guest_platform(vm: &Vm) -> GuestPlatform {
    vm.guest_profile
        .as_ref()
        .and_then(|profile| GuestPlatform::try_from(profile.platform).ok())
        .unwrap_or(GuestPlatform::Linux)
}

fn vm_workspace_root(vm: &Vm) -> String {
    vm.guest_runtime
        .as_ref()
        .map(|runtime| runtime.workspace_root.trim())
        .filter(|path| !path.is_empty())
        .unwrap_or("/workspace")
        .to_string()
}

fn guest_shell_args(
    platform: GuestPlatform,
    requested_shell: Option<&str>,
    default_shell: &str,
    command: &str,
) -> Vec<String> {
    if platform == GuestPlatform::Windows {
        if let Some(shell) = requested_shell.filter(|shell| {
            shell.rsplit(['/', '\\']).next().is_some_and(|name| {
                name.eq_ignore_ascii_case("bash") || name.eq_ignore_ascii_case("bash.exe")
            })
        }) {
            return vec![shell.to_string(), "-lc".to_string(), command.to_string()];
        }
        return vec![
            requested_shell
                .unwrap_or(r"C:\Program Files\PowerShell\7\pwsh.exe")
                .to_string(),
            "-NoLogo".to_string(),
            "-NoProfile".to_string(),
            "-NonInteractive".to_string(),
            "-Command".to_string(),
            command.to_string(),
        ];
    }

    vec![
        requested_shell.unwrap_or(default_shell).to_string(),
        "-lc".to_string(),
        command.to_string(),
    ]
}

fn map_host_pci_device(device: proto::vmd::v1::HostPciDevice) -> HostPciDevice {
    let state = match proto::vmd::v1::HostPciDeviceState::try_from(device.state) {
        Ok(proto::vmd::v1::HostPciDeviceState::Disabled) => HostPciDeviceState::Disabled,
        Ok(proto::vmd::v1::HostPciDeviceState::Unavailable) => HostPciDeviceState::Unavailable,
        Ok(proto::vmd::v1::HostPciDeviceState::Host) => HostPciDeviceState::Host,
        Ok(proto::vmd::v1::HostPciDeviceState::Ready) => HostPciDeviceState::Ready,
        Ok(proto::vmd::v1::HostPciDeviceState::Assigned) => HostPciDeviceState::Assigned,
        Ok(proto::vmd::v1::HostPciDeviceState::Error) => HostPciDeviceState::Error,
        _ => HostPciDeviceState::Unknown,
    };
    HostPciDevice {
        id: device.id,
        label: device.label,
        functions: device
            .functions
            .into_iter()
            .map(|function| HostPciFunction {
                bdf: function.bdf,
                vendor_id: function.vendor_id,
                device_id: function.device_id,
                class_code: function.class_code,
                driver: function.driver,
                iommu_group: function.iommu_group,
            })
            .collect(),
        state,
        assigned_vm_id: device.assigned_vm_id,
        managed: device.managed,
        hotplug_capable: device.hotplug_capable,
        unavailable_reason: device.unavailable_reason,
    }
}

fn map_pci_action(response: proto::vmd::v1::PciDeviceActionResponse) -> PciDeviceAction {
    PciDeviceAction {
        device: response.device.map(map_host_pci_device),
        restart_required: response.restart_required,
        detail: response.detail,
        vm_state: response.vm.map_or(0, |vm| vm.state),
    }
}

fn shared_mount_availability_proto(availability: &SharedMountAvailability) -> i32 {
    match availability {
        SharedMountAvailability::NodeLocal => {
            proto::vmd::v1::SharedMountAvailability::NodeLocal as i32
        }
        SharedMountAvailability::SharedStorage => {
            proto::vmd::v1::SharedMountAvailability::SharedStorage as i32
        }
    }
}

fn shared_mount_continuity_proto(continuity: &SharedMountContinuity) -> i32 {
    match continuity {
        SharedMountContinuity::RestartSameNode => {
            proto::vmd::v1::SharedMountContinuity::RestartSameNode as i32
        }
        SharedMountContinuity::RestoreCrossNode => {
            proto::vmd::v1::SharedMountContinuity::RestoreCrossNode as i32
        }
    }
}

fn shared_mount_availability_from_proto(value: i32) -> SharedMountAvailability {
    match proto::vmd::v1::SharedMountAvailability::try_from(value)
        .unwrap_or(proto::vmd::v1::SharedMountAvailability::Unspecified)
    {
        proto::vmd::v1::SharedMountAvailability::SharedStorage => {
            SharedMountAvailability::SharedStorage
        }
        _ => SharedMountAvailability::NodeLocal,
    }
}

fn shared_mount_continuity_from_proto(
    value: i32,
    availability: &SharedMountAvailability,
) -> SharedMountContinuity {
    match proto::vmd::v1::SharedMountContinuity::try_from(value)
        .unwrap_or(proto::vmd::v1::SharedMountContinuity::Unspecified)
    {
        proto::vmd::v1::SharedMountContinuity::RestoreCrossNode => {
            SharedMountContinuity::RestoreCrossNode
        }
        proto::vmd::v1::SharedMountContinuity::RestartSameNode => {
            SharedMountContinuity::RestartSameNode
        }
        proto::vmd::v1::SharedMountContinuity::Unspecified => {
            default_mount_continuity(availability)
        }
    }
}

fn default_mount_continuity(availability: &SharedMountAvailability) -> SharedMountContinuity {
    match availability {
        SharedMountAvailability::NodeLocal => SharedMountContinuity::RestartSameNode,
        SharedMountAvailability::SharedStorage => SharedMountContinuity::RestoreCrossNode,
    }
}

/// Render the `{placeholder}` vocabulary shared by every provider-managed mount
/// template. `mountpoint` is the already-resolved guest path (a template's own
/// `path`, else the shared mount's `guest_path`); `None` leaves `{mountpoint}`
/// for a provider that substitutes it on its side (OpenComputer does).
pub(crate) fn render_shared_mount_template(
    template: &str,
    shared: &SharedMount,
    mountpoint: Option<&str>,
) -> String {
    let template = match mountpoint {
        Some(mountpoint) => template.replace("{mountpoint}", mountpoint),
        None => template.to_string(),
    };
    template
        .replace("{guest_path}", shared.guest_path.as_str())
        .replace("{mount_tag}", shared.mount_tag.as_str())
        .replace("{backend_profile}", shared.backend_profile.as_str())
        .replace("{vfs_endpoint}", shared.vfs_endpoint.as_str())
        .replace("{vfs_scope_path}", shared.vfs_scope_path.trim_matches('/'))
        .replace(
            "{read_only}",
            if shared.read_only { "true" } else { "false" },
        )
}

fn normalize_mount_backend_profile(raw: &str) -> String {
    raw.trim().to_ascii_lowercase()
}

fn validate_shared_mount_contract(
    shared_mounts: &[SharedMount],
    tier_b_eligible: bool,
) -> Result<()> {
    for mount in shared_mounts {
        let backend_profile = normalize_mount_backend_profile(&mount.backend_profile);
        if matches!(mount.availability, SharedMountAvailability::NodeLocal)
            && matches!(mount.continuity, SharedMountContinuity::RestoreCrossNode)
        {
            return Err(SandboxError::InvalidResponse(format!(
                "shared mount `{}` cannot claim cross-node restore continuity with node-local availability",
                mount.mount_tag
            )));
        }
        if matches!(mount.availability, SharedMountAvailability::SharedStorage)
            && backend_profile.is_empty()
        {
            return Err(SandboxError::InvalidResponse(format!(
                "shared-storage mount `{}` requires a non-empty backend_profile so distributed placement can verify backend availability",
                mount.mount_tag
            )));
        }
        if tier_b_eligible && !matches!(mount.continuity, SharedMountContinuity::RestoreCrossNode) {
            return Err(SandboxError::InvalidResponse(format!(
                "tier_b_eligible session requires cross-node-restorable shared mounts; mount `{}` is only `{}`",
                mount.mount_tag,
                match mount.continuity {
                    SharedMountContinuity::RestartSameNode => "restart-same-node",
                    SharedMountContinuity::RestoreCrossNode => "restore-cross-node",
                }
            )));
        }
    }
    Ok(())
}

fn required_shared_mount_profiles(shared_mounts: &[SharedMount]) -> Vec<String> {
    let mut profiles = shared_mounts
        .iter()
        .filter(|mount| matches!(mount.availability, SharedMountAvailability::SharedStorage))
        .map(|mount| normalize_mount_backend_profile(&mount.backend_profile))
        .filter(|profile| !profile.is_empty())
        .collect::<Vec<_>>();
    profiles.sort();
    profiles.dedup();
    profiles
}

fn proto_shared_mount(mount: SharedMount) -> proto::vmd::v1::SharedMount {
    proto::vmd::v1::SharedMount {
        host_path: mount.host_path,
        guest_path: mount.guest_path,
        mount_tag: mount.mount_tag,
        read_only: mount.read_only,
        availability: shared_mount_availability_proto(&mount.availability),
        continuity: shared_mount_continuity_proto(&mount.continuity),
        backend_profile: normalize_mount_backend_profile(&mount.backend_profile),
        vfs_endpoint: mount.vfs_endpoint,
        vfs_scope_path: mount.vfs_scope_path,
    }
}

fn vm_required_shared_mount_profiles(vm: &Vm) -> Vec<String> {
    let mut profiles = vm
        .shared_mounts
        .iter()
        .filter(|mount| {
            matches!(
                shared_mount_availability_from_proto(mount.availability),
                SharedMountAvailability::SharedStorage
            )
        })
        .map(|mount| normalize_mount_backend_profile(&mount.backend_profile))
        .filter(|profile| !profile.is_empty())
        .collect::<Vec<_>>();
    profiles.sort();
    profiles.dedup();
    profiles
}

fn validate_vm_mount_contract(session_id: &str, vm: &Vm) -> Result<()> {
    let tier_b_eligible = vm_tier_b_eligible(vm);
    let mounts = vm
        .shared_mounts
        .iter()
        .map(|mount| {
            let availability = shared_mount_availability_from_proto(mount.availability);
            let continuity = shared_mount_continuity_from_proto(mount.continuity, &availability);
            SharedMount {
                host_path: mount.host_path.clone(),
                guest_path: mount.guest_path.clone(),
                mount_tag: mount.mount_tag.clone(),
                read_only: mount.read_only,
                availability,
                continuity,
                backend_profile: mount.backend_profile.clone(),
                vfs_endpoint: mount.vfs_endpoint.clone(),
                vfs_scope_path: mount.vfs_scope_path.clone(),
            }
        })
        .collect::<Vec<_>>();
    validate_shared_mount_contract(&mounts, tier_b_eligible).map_err(|err| match err {
        SandboxError::InvalidResponse(message) => SandboxError::InvalidResponse(format!(
            "session `{session_id}` has non-continuous shared mount contract: {message}"
        )),
        other => other,
    })
}

fn parse_bool_like(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn is_rebind_candidate_error(err: &SandboxError) -> bool {
    match err {
        // tonic can surface the same dead-channel/connect failure either as a
        // Status or as a transport::Error depending on whether the HTTP/2
        // channel was established. Both authorize endpoint rebind/retry.
        SandboxError::Transport(_) => true,
        SandboxError::Grpc(status) => {
            let message = status.message().to_ascii_lowercase();
            status.code() == tonic::Code::Unavailable
                || status.code() == tonic::Code::Unknown
                || status.code() == tonic::Code::Cancelled
                || message.contains("transport error")
                || message.contains("connection reset")
                || message.contains("broken pipe")
                || message.contains("connection closed")
                || message.contains("operation was canceled")
                || message.contains("canceled")
                || message.contains("connection refused")
        }
        // A guest that is still booting is not a channel failure. A long-running guest
        // that stayed silent through its one local restart may be escalated cross-node.
        SandboxError::GuestRpcNotReady { restarted, .. } => *restarted,
        SandboxError::DaemonUnavailable(message) => {
            let lower = message.to_ascii_lowercase();
            lower.contains("transport")
                || lower.contains("connection reset")
                || lower.contains("broken pipe")
                || lower.contains("connection closed")
                || lower.contains("operation was canceled")
                || lower.contains("canceled")
                || lower.contains("connection refused")
        }
        _ => false,
    }
}

fn execution_restore_snapshot_id(vm: &Vm) -> Option<String> {
    // @dive: Restore selection prefers explicit snapshot IDs, then resolves snapshot names to IDs for compatibility.
    if let Some(snapshot_id) = vm
        .metadata
        .get(META_EXEC_RESTORE_SNAPSHOT_ID)
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Some(snapshot_id.to_string());
    }

    let mut candidate_names = Vec::new();
    if let Some(snapshot_name) = vm
        .metadata
        .get(META_EXEC_RESTORE_SNAPSHOT_NAME)
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        candidate_names.push(snapshot_name.to_string());
    }
    if let Some(fork_snapshot_name) = vm
        .metadata
        .get(META_FORK_SNAPSHOT)
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        candidate_names.push(fork_snapshot_name.to_string());
    }
    for snapshot_name in candidate_names {
        if let Some(snapshot_id) = vm
            .snapshots
            .iter()
            .find(|snapshot| snapshot.name == snapshot_name)
            .map(|snapshot| snapshot.id.clone())
            .filter(|id| !id.trim().is_empty())
        {
            return Some(snapshot_id);
        }
    }
    None
}

fn portproxy_method_is_unimplemented(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::Unimplemented
        || status
            .message()
            .eq_ignore_ascii_case("operation is not implemented or not supported")
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn log_slo_observation(metric: &str, elapsed: Duration, outcome: &str) {
    let elapsed_ms = elapsed.as_millis().min(u128::from(u64::MAX)) as u64;
    tracing::info!(
        target: "chevalier_sandbox::slo",
        metric = metric,
        elapsed_ms = elapsed_ms,
        outcome = outcome,
        "slo observation"
    );
}

pub type ChevalierSandboxResult<T> = Result<T>;

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn exec_signal_bypasses_a_full_stdin_queue() {
        let (data, mut data_receiver) = tokio::sync::mpsc::channel(1);
        let (control, mut control_receiver) = tokio::sync::mpsc::channel(1);
        let input = super::ExecInputSender { data, control };
        input.send(super::ExecInput::Data(vec![1])).await.unwrap();
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            input.send(super::ExecInput::Signal(9)),
        )
        .await
        .expect("control cannot wait for stdin space")
        .unwrap();
        assert!(matches!(
            control_receiver.recv().await,
            Some(super::ExecInput::Signal(9))
        ));
        assert!(matches!(
            data_receiver.recv().await,
            Some(super::ExecInput::Data(_))
        ));
    }
    use super::*;

    #[test]
    fn normalize_endpoint_adds_http_scheme() {
        let value = normalize_endpoint("127.0.0.1:8052").expect("endpoint should normalize");
        assert_eq!(value, "http://127.0.0.1:8052");
    }

    #[test]
    fn normalize_endpoint_keeps_scheme() {
        let value =
            normalize_endpoint("http://127.0.0.1:8052").expect("endpoint should stay unchanged");
        assert_eq!(value, "http://127.0.0.1:8052");
    }

    #[test]
    fn portproxy_server_addr_uses_endpoint_host() {
        let overrides = HashMap::new();
        let value = portproxy_server_addr(&overrides, "http://sandbox-node.internal:18072", 3001)
            .expect("address should derive host");
        assert_eq!(value, "sandbox-node.internal:3001");
    }

    #[test]
    fn portproxy_server_addr_rewrites_unspecified_hosts_for_local_dial() {
        let overrides = HashMap::new();
        let v4 = portproxy_server_addr(&overrides, "http://0.0.0.0:18072", 3001)
            .expect("v4 unspecified host should normalize");
        assert_eq!(v4, "127.0.0.1:3001");

        let v6 = portproxy_server_addr(&overrides, "http://[::]:18072", 3001)
            .expect("v6 unspecified host should normalize");
        assert_eq!(v6, "[::1]:3001");
    }

    #[test]
    fn portproxy_server_addr_supports_ipv6_endpoints() {
        let overrides = HashMap::new();
        let value = portproxy_server_addr(&overrides, "http://[2001:db8::42]:18072", 3001)
            .expect("ipv6 host should preserve brackets");
        assert_eq!(value, "[2001:db8::42]:3001");
    }

    #[test]
    fn compile_auth_header_adds_bearer_prefix() {
        let value = compile_auth_header(Some("token-value"))
            .expect("auth header should compile")
            .expect("auth header should be present");
        assert_eq!(value.to_str().expect("ascii header"), "Bearer token-value");
    }

    #[test]
    fn compile_auth_header_keeps_existing_bearer_prefix() {
        let value = compile_auth_header(Some("Bearer token-value"))
            .expect("auth header should compile")
            .expect("auth header should be present");
        assert_eq!(value.to_str().expect("ascii header"), "Bearer token-value");
    }

    #[test]
    fn portproxy_auth_header_uses_vm_metadata_token() {
        let metadata = HashMap::from([(
            META_PORTPROXY_AUTH_TOKEN.to_string(),
            "guest-token".to_string(),
        )]);
        let value = portproxy_auth_header_from_metadata(&metadata)
            .expect("portproxy auth header should compile")
            .expect("portproxy auth header should be present");
        assert_eq!(value.to_str().expect("ascii header"), "Bearer guest-token");
    }

    #[test]
    fn request_with_optional_auth_inserts_authorization_metadata() {
        let value = compile_auth_header(Some("guest-token"))
            .expect("auth header should compile")
            .expect("auth header should be present");
        let request = request_with_optional_auth((), Some(&value));
        assert_eq!(
            request
                .metadata()
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer guest-token")
        );
    }

    #[test]
    fn portproxy_unimplemented_detection_accepts_native_and_legacy_statuses() {
        assert!(portproxy_method_is_unimplemented(
            &tonic::Status::unimplemented("method is not available")
        ));
        assert!(portproxy_method_is_unimplemented(&tonic::Status::unknown(
            "Operation is not implemented or not supported"
        )));
        assert!(!portproxy_method_is_unimplemented(
            &tonic::Status::unavailable("portproxy is restarting")
        ));
    }

    #[test]
    fn shell_single_quote_preserves_posix_paths() {
        assert_eq!(shell_single_quote("/tmp/a b"), "'/tmp/a b'");
        assert_eq!(shell_single_quote("/tmp/ian's"), "'/tmp/ian'\"'\"'s'");
    }

    #[test]
    fn guest_shell_args_are_platform_native() {
        assert_eq!(
            guest_shell_args(GuestPlatform::Linux, None, "/bin/sh", "printf ok"),
            vec!["/bin/sh", "-lc", "printf ok"]
        );
        assert_eq!(
            guest_shell_args(GuestPlatform::Windows, None, "/bin/sh", "Write-Output ok"),
            vec![
                r"C:\Program Files\PowerShell\7\pwsh.exe",
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "Write-Output ok",
            ]
        );
        assert_eq!(
            guest_shell_args(
                GuestPlatform::Windows,
                Some(r"C:\Program Files\Git\bin\bash.exe"),
                "/bin/sh",
                "printf ok",
            ),
            vec![r"C:\Program Files\Git\bin\bash.exe", "-lc", "printf ok"]
        );
    }

    #[tokio::test]
    async fn chevalier_provider_requires_an_explicit_default_image() {
        let config = SandboxConfig {
            default_image: "  ".to_string(),
            auto_spawn: false,
            prewarm_on_start: false,
            ..SandboxConfig::default()
        };

        let error = match Sandbox::connect("http://127.0.0.1:1", config).await {
            Ok(_) => panic!("missing default image should fail before connecting"),
            Err(error) => error,
        };
        assert!(matches!(error, SandboxError::InvalidConfig(_)));
        assert!(error.to_string().contains("default image is required"));
    }

    #[tokio::test]
    async fn opencomputer_session_alias_resolves_and_clears_requested_ids() {
        let sandbox = Sandbox {
            inner: Arc::new(SandboxInner {
                cfg: SandboxConfig::default(),
                control_backend: ControlBackend::Direct,
                auth_header: None,
                pci_auth_header: None,
                #[cfg(feature = "host")]
                managed_daemon: Mutex::new(None),
                ready_vm_rpc: Mutex::new(HashMap::new()),
                portproxy_channels: Mutex::new(HashMap::new()),
                node_multiplexers: Mutex::new(HashMap::new()),
                warm_pool_ready: Mutex::new(HashSet::new()),
                managed_session_aliases: Mutex::new(HashMap::new()),
            }),
        };

        sandbox
            .bind_managed_session_alias("requested-session", "provider-session")
            .await;
        assert_eq!(
            sandbox
                .managed_provider_session_id("requested-session")
                .await,
            "provider-session"
        );

        sandbox
            .clear_managed_session_aliases("requested-session", "provider-session")
            .await;
        assert_eq!(
            sandbox
                .managed_provider_session_id("requested-session")
                .await,
            "requested-session"
        );
    }

    #[tokio::test]
    async fn portproxy_channels_are_single_flight_per_vm_route_and_invalidated() {
        let sandbox = Sandbox {
            inner: Arc::new(SandboxInner {
                cfg: SandboxConfig::default(),
                control_backend: ControlBackend::Direct,
                auth_header: None,
                pci_auth_header: None,
                #[cfg(feature = "host")]
                managed_daemon: Mutex::new(None),
                ready_vm_rpc: Mutex::new(HashMap::new()),
                portproxy_channels: Mutex::new(HashMap::new()),
                node_multiplexers: Mutex::new(HashMap::new()),
                warm_pool_ready: Mutex::new(HashSet::new()),
                managed_session_aliases: Mutex::new(HashMap::new()),
            }),
        };

        let (first, concurrent) = tokio::join!(
            sandbox.portproxy_channel_entry("vm-1", "node-1", "http://127.0.0.1:3001"),
            sandbox.portproxy_channel_entry("vm-1", "node-1", "http://127.0.0.1:3001"),
        );
        assert!(Arc::ptr_eq(&first, &concurrent));

        let rebound = sandbox
            .portproxy_channel_entry("vm-1", "node-1", "http://127.0.0.1:3002")
            .await;
        assert!(!Arc::ptr_eq(&first, &rebound));

        sandbox.invalidate_ready_vm_rpc("vm-1", "node-1").await;
        assert!(sandbox.inner.portproxy_channels.lock().await.is_empty());
    }

    #[test]
    fn guest_rpc_not_ready_is_never_a_rebind_candidate() {
        let err = SandboxError::GuestRpcNotReady {
            vm_id: "vm-1".to_string(),
            endpoint: "http://127.0.0.1:1".to_string(),
            waited: Duration::from_secs(240),
            restarted: false,
        };
        assert!(!is_rebind_candidate_error(&err));
        assert_eq!(
            err.to_string(),
            "guest RPC not ready for vm vm-1 on http://127.0.0.1:1 after 240s"
        );
        let escalate = SandboxError::GuestRpcNotReady {
            vm_id: "vm-1".to_string(),
            endpoint: "http://127.0.0.1:1".to_string(),
            waited: Duration::from_secs(480),
            restarted: true,
        };
        assert!(is_rebind_candidate_error(&escalate));
        assert!(
            escalate
                .to_string()
                .ends_with("after 480s (still not ready after one bounded local restart)")
        );
        // The legacy message form must not sneak back in through DaemonUnavailable either.
        let legacy = SandboxError::DaemonUnavailable(
            "sandbox guest RPC did not become ready for vm vm-1".to_string(),
        );
        assert!(!is_rebind_candidate_error(&legacy));
    }

    #[test]
    fn channel_failures_remain_rebind_candidates() {
        for message in [
            "transport error",
            "connection reset by peer",
            "broken pipe",
            "connection refused",
        ] {
            assert!(
                is_rebind_candidate_error(&SandboxError::DaemonUnavailable(message.to_string())),
                "{message} should authorize rebind"
            );
        }
        assert!(is_rebind_candidate_error(&SandboxError::Grpc(
            tonic::Status::unavailable("node down")
        )));
        assert!(is_rebind_candidate_error(&SandboxError::Grpc(
            tonic::Status::cancelled("operation was canceled")
        )));
    }

    #[test]
    fn restart_deadline_covers_graceful_stop() {
        // vmd waits up to 180 s for a graceful guest shutdown before it force-stops.
        assert!(VMD_RESTART_TIMEOUT >= Duration::from_secs(180 + 30));
    }
}
