// @dive-file: Centralizes daemon runtime configuration, policy defaults, and environment parsing for HA/distributed behavior.
// @dive-rel: Feeds control-plane queueing and node-label policies consumed by vmd/src/control_bus.rs and vmd/src/registry.rs.
// @dive-rel: Provides normalized admission/scheduling inputs that must align with crates/sandbox/src/distributed.rs.
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use uuid::Uuid;

use crate::pci::PciConfig;

pub const BASE_IMAGES_DIR_NAME: &str = "base_images";
const DEFAULT_NODE_REGISTRY_PREFIX: &str = "/chevalier-sandbox";
const DEFAULT_NODE_REGISTRY_TTL_SECS: i64 = 15;
const DEFAULT_CONTROL_SUBJECT_PREFIX: &str = "chevalier.sandbox.control";
const DEFAULT_CONTROL_CLUSTER_ID: &str = "chevalier-sandbox-cluster";
const DEFAULT_CONTROL_DEDUPE_PREFIX: &str = "/sandbox/command-dedupe";
const DEFAULT_CONTROL_STREAM_NAME: &str = "CHEVALIER_SANDBOX_CONTROL";
const DEFAULT_CONTROL_STREAM_MAX_AGE_SECS: u64 = 60 * 60 * 24 * 7;
const DEFAULT_CONTROL_STREAM_REPLICAS: usize = 1;
const DEFAULT_CONTROL_COMMAND_MAX_DELIVER: i64 = 5;
const DEFAULT_CONTROL_COMMAND_ACK_WAIT_MS: u64 = 30_000;
const DEFAULT_CONTROL_MAX_INFLIGHT_COMMANDS: usize = 1024;
const DEFAULT_CONTROL_OVERLOAD_RETRY_AFTER_MS: u64 = 2_000;
const DEFAULT_REQUIRE_CLIENT_CERT: bool = true;
const DEFAULT_MAX_FORK_CHAIN_DEPTH: usize = 32;
const DEFAULT_FORK_COMPACTION_DEPTH_THRESHOLD: usize = 8;
const STORAGE_PROFILE_LOCAL_EPHEMERAL: &str = "local-ephemeral";
const STORAGE_PROFILE_DURABLE_SHARED: &str = "durable-shared";
const CONTINUITY_TIER_A: &str = "tier-a";
const CONTINUITY_TIER_B: &str = "tier-b";
const DEFAULT_SHARED_MOUNT_PROFILE_LOCAL: &str = "local-path";
const DEFAULT_NODE_REGION: &str = "local-region";
const DEFAULT_NODE_ZONE: &str = "local-zone";
const DEFAULT_NODE_RACK: &str = "local-rack";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageProfile {
    LocalEphemeral,
    DurableShared,
}

impl StorageProfile {
    pub fn as_str(&self) -> &'static str {
        match self {
            StorageProfile::LocalEphemeral => STORAGE_PROFILE_LOCAL_EPHEMERAL,
            StorageProfile::DurableShared => STORAGE_PROFILE_DURABLE_SHARED,
        }
    }

    pub fn is_durable_shared(self) -> bool {
        matches!(self, StorageProfile::DurableShared)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContinuityTier {
    TierA,
    TierB,
}

impl ContinuityTier {
    pub fn as_str(&self) -> &'static str {
        match self {
            ContinuityTier::TierA => CONTINUITY_TIER_A,
            ContinuityTier::TierB => CONTINUITY_TIER_B,
        }
    }
}

#[derive(Clone, Debug)]
pub struct NodeRegistryConfig {
    pub etcd_endpoints: Vec<String>,
    pub key_prefix: String,
    pub node_id: String,
    pub advertise_endpoint: String,
    pub ttl_secs: i64,
    pub max_active_vms: Option<usize>,
    pub storage_profile: StorageProfile,
    pub continuity_tier: ContinuityTier,
    pub degraded_mode: bool,
    pub admission_frozen: bool,
    pub shared_mount_profiles: Vec<String>,
    pub region: String,
    pub zone: String,
    pub rack: String,
}

impl NodeRegistryConfig {
    pub fn defaults_for_listen(listen_address: &str) -> Self {
        Self {
            etcd_endpoints: vec!["http://127.0.0.1:2379".to_string()],
            key_prefix: DEFAULT_NODE_REGISTRY_PREFIX.to_string(),
            node_id: default_node_id(),
            advertise_endpoint: format!("http://{}", listen_address.trim()),
            ttl_secs: DEFAULT_NODE_REGISTRY_TTL_SECS,
            max_active_vms: default_max_active_vms_from_env(),
            storage_profile: default_storage_profile_from_env(),
            continuity_tier: default_continuity_tier_from_env(),
            degraded_mode: default_degraded_mode_from_env(),
            admission_frozen: default_admission_frozen_from_env(),
            shared_mount_profiles: default_shared_mount_profiles_from_env(),
            region: default_node_region_from_env(),
            zone: default_node_zone_from_env(),
            rack: default_node_rack_from_env(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ControlBusConfig {
    pub nats_url: String,
    pub nats_auth_token: Option<String>,
    pub subject_prefix: String,
    pub cluster_id: String,
    pub node_id: String,
    pub dedupe_etcd_endpoints: Vec<String>,
    pub dedupe_prefix: String,
    pub stream_name: String,
    pub stream_max_age_secs: u64,
    pub stream_replicas: usize,
    pub command_consumer_durable: String,
    pub command_max_deliver: i64,
    pub command_ack_wait_ms: u64,
    pub max_inflight_commands: usize,
    pub overload_retry_after_ms: u64,
    pub dead_letter_subject: String,
    pub replay_subject: String,
}

#[derive(Clone, Debug)]
pub struct AuthConfig {
    pub admin_token: String,
    pub readonly_token: Option<String>,
}

#[derive(Clone, Debug)]
pub struct TlsServerConfig {
    pub cert_path: String,
    pub key_path: String,
    pub client_ca_path: Option<String>,
    pub require_client_cert: bool,
}

#[derive(Clone, Debug, Default)]
pub struct SecurityConfig {
    pub auth: Option<AuthConfig>,
    pub tls: Option<TlsServerConfig>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QemuProcessConfig {
    pub run_as_uid: u32,
    pub run_as_gid: u32,
}

impl Default for QemuProcessConfig {
    fn default() -> Self {
        Self {
            run_as_uid: default_qemu_run_as_uid_from_env(),
            run_as_gid: default_qemu_run_as_gid_from_env(),
        }
    }
}

impl QemuProcessConfig {
    fn normalize(&mut self) -> Result<()> {
        if self.run_as_uid == 0 {
            bail!("qemu run_as_uid must be non-zero");
        }
        if self.run_as_gid == 0 {
            bail!("qemu run_as_gid must be non-zero");
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct NetworkServicesConfig {
    pub envoy_bin: String,
    pub envoy_log_level: String,
    pub envoy_base_id: Option<u32>,
    pub envoy_admin_addr: String,
    pub coredns_bin: String,
    pub coredns_bind_addr: String,
    pub coredns_threat_hosts_path: String,
    pub public_ingress_bind_addr: Option<String>,
    pub api_internal_base_url: Option<String>,
}

impl Default for NetworkServicesConfig {
    fn default() -> Self {
        Self {
            envoy_bin: default_envoy_bin_from_env(),
            envoy_log_level: default_envoy_log_level_from_env(),
            envoy_base_id: default_envoy_base_id_from_env(),
            envoy_admin_addr: default_envoy_admin_addr_from_env(),
            coredns_bin: default_coredns_bin_from_env(),
            coredns_bind_addr: default_coredns_bind_addr_from_env(),
            coredns_threat_hosts_path: default_coredns_threat_hosts_path_from_env(),
            public_ingress_bind_addr: default_public_ingress_bind_addr_from_env(),
            api_internal_base_url: default_api_internal_base_url_from_env(),
        }
    }
}

impl NetworkServicesConfig {
    fn normalize(&mut self, guest_network: &GuestNetworkConfig) -> Result<()> {
        self.envoy_log_level = self.envoy_log_level.trim().to_string();
        if self.envoy_log_level.is_empty() {
            self.envoy_log_level = "info".to_string();
        }
        self.envoy_admin_addr = normalize_socket_addr(&self.envoy_admin_addr, "envoy admin addr")?;
        if guest_network.http_proxy_upstream_addr.is_some() {
            self.envoy_bin = resolve_binary(&self.envoy_bin, "envoy")?;
        }
        if guest_network.http_proxy_upstream_addr.is_some() || guest_network.dns_server.is_some() {
            self.coredns_bin = resolve_binary(&self.coredns_bin, "coredns")?;
            self.coredns_bind_addr =
                normalize_socket_addr(&self.coredns_bind_addr, "coredns bind addr")?;
        }
        self.coredns_threat_hosts_path = self.coredns_threat_hosts_path.trim().to_string();
        if self.coredns_threat_hosts_path.is_empty() {
            self.coredns_threat_hosts_path =
                "/opt/chevalier/network/threat-domains.hosts".to_string();
        }
        self.public_ingress_bind_addr = normalize_optional_socket_addr(
            self.public_ingress_bind_addr.take(),
            "public ingress bind addr",
        )?;
        self.api_internal_base_url = self
            .api_internal_base_url
            .take()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GuestNetworkConfig {
    pub dns_server: Option<String>,
    pub http_proxy_guest_addr: Option<String>,
    pub http_proxy_upstream_addr: Option<String>,
}

impl GuestNetworkConfig {
    fn normalize(&mut self) -> Result<()> {
        self.dns_server = normalize_optional_ip_addr(self.dns_server.take(), "guest dns server")?;
        self.http_proxy_guest_addr = normalize_optional_socket_addr(
            self.http_proxy_guest_addr.take(),
            "guest http proxy guest addr",
        )?;
        self.http_proxy_upstream_addr = normalize_optional_socket_addr(
            self.http_proxy_upstream_addr.take(),
            "guest http proxy upstream addr",
        )?;

        match (
            self.http_proxy_guest_addr.is_some(),
            self.http_proxy_upstream_addr.is_some(),
        ) {
            (true, true) => {
                if self.dns_server.is_none() {
                    // @dive: Proxy-enabled guests still need slirp's built-in virtual
                    //        nameserver advertised so the guest has a stable DNS target.
                    //        The qemu-owned upstream DNS sockets are pinned to CoreDNS by
                    //        the pod-local firewall/NAT path.
                    self.dns_server = Some("10.0.2.3".to_string());
                }
                Ok(())
            }
            (false, false) => Ok(()),
            _ => bail!(
                "guest http proxy config requires both guest and upstream addresses to be set"
            ),
        }
    }

    pub fn http_proxy_url(&self) -> Option<String> {
        self.http_proxy_guest_addr
            .as_ref()
            .map(|addr| format!("http://{addr}"))
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub listen_address: String,
    pub data_dir: String,
    /// Optional fast-local directory for staging snapshot RAM files before they're
    /// copied to durable shared storage. When set, `migrate file:` writes here so
    /// qemu's userfaultfd-WP storm completes quickly (local SSD write speed) and
    /// guest network-facing services stay responsive throughout the snapshot. The
    /// staged file is then copied to the canonical `<vm_dir>/snapshots/<name>` path
    /// on durable storage (Filestore for HA, persistent for single-node) before the
    /// snapshot metadata is recorded. When unset, snapshots write directly to the
    /// canonical path.
    pub snapshot_staging_dir: Option<String>,
    pub qemu_bin: String,
    pub qemu_arm64_bin: String,
    pub qemu_img_bin: String,
    /// Absolute path to the `virtiofsd` binary used to serve host↔guest shared
    /// filesystems to live-migration-capable virtio-fs guest devices. Defaults to the
    /// location shipped in the qemu-system-common package on Ubuntu 22.04.
    pub virtiofsd_bin: String,
    pub docker_bin: String,
    pub log_level: String,
    pub force_local_build: bool,
    pub max_active_vms: Option<usize>,
    pub max_fork_chain_depth: usize,
    pub fork_compaction_depth_threshold: usize,
    pub storage_profile: StorageProfile,
    pub shared_mount_profiles: Vec<String>,
    pub vfs_internal_service_token: Option<String>,
    pub qemu_process: QemuProcessConfig,
    /// Optional root/operator-owned allowlist for dedicated PCI assignment. When
    /// absent, vmd performs no PCI discovery or host-device operations.
    pub pci_policy_path: Option<String>,
    pub pci: PciConfig,
    pub guest_network: GuestNetworkConfig,
    pub network_services: NetworkServicesConfig,
    pub ha_mode: bool,
    pub node_registry: Option<NodeRegistryConfig>,
    pub control_bus: Option<ControlBusConfig>,
    pub security: SecurityConfig,
}

impl Default for Config {
    fn default() -> Self {
        let data_dir = dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".bracket")
            .join("vms");

        Self {
            listen_address: "127.0.0.1:8052".to_string(),
            data_dir: data_dir.to_string_lossy().to_string(),
            snapshot_staging_dir: default_snapshot_staging_dir_from_env(),
            qemu_bin: "qemu-system-x86_64".to_string(),
            qemu_arm64_bin: "qemu-system-aarch64".to_string(),
            qemu_img_bin: "qemu-img".to_string(),
            virtiofsd_bin: default_virtiofsd_bin(),
            docker_bin: "docker".to_string(),
            log_level: "info".to_string(),
            force_local_build: false,
            max_active_vms: default_max_active_vms_from_env(),
            max_fork_chain_depth: default_max_fork_chain_depth_from_env(),
            fork_compaction_depth_threshold: default_fork_compaction_depth_threshold_from_env(),
            storage_profile: default_storage_profile_from_env(),
            shared_mount_profiles: default_shared_mount_profiles_from_env(),
            vfs_internal_service_token: default_vfs_internal_service_token_from_env(),
            qemu_process: QemuProcessConfig::default(),
            pci_policy_path: default_pci_policy_path_from_env(),
            pci: PciConfig::default(),
            guest_network: default_guest_network_from_env(),
            network_services: NetworkServicesConfig::default(),
            ha_mode: default_ha_mode_from_env(),
            node_registry: default_node_registry_from_env("127.0.0.1:8052"),
            control_bus: default_control_bus_from_env(),
            security: default_security_from_env(),
        }
    }
}

impl Config {
    pub fn base_images_dir(&self) -> PathBuf {
        Path::new(&self.data_dir).join(BASE_IMAGES_DIR_NAME)
    }

    pub fn normalize(&mut self) -> Result<()> {
        if self.listen_address.trim().is_empty() {
            bail!("listen address must be provided");
        }
        if self.data_dir.trim().is_empty() {
            bail!("data dir must be provided");
        }

        let data_dir = expand_home(&self.data_dir)?;
        fs::create_dir_all(&data_dir)
            .with_context(|| format!("create data dir {}", data_dir.to_string_lossy()))?;
        let base_dir = data_dir.join(BASE_IMAGES_DIR_NAME);
        fs::create_dir_all(&base_dir)
            .with_context(|| format!("create base image dir {}", base_dir.to_string_lossy()))?;
        self.data_dir = canonical_string(&data_dir)?;

        if let Some(ref staging) = self.snapshot_staging_dir {
            if !staging.trim().is_empty() {
                let staging_dir = expand_home(staging)?;
                fs::create_dir_all(&staging_dir).with_context(|| {
                    format!(
                        "create snapshot staging dir {}",
                        staging_dir.to_string_lossy()
                    )
                })?;
                self.snapshot_staging_dir = Some(canonical_string(&staging_dir)?);
            } else {
                self.snapshot_staging_dir = None;
            }
        }

        self.qemu_bin = resolve_binary(&self.qemu_bin, "qemu-system-x86_64")?;
        self.qemu_arm64_bin = resolve_binary(&self.qemu_arm64_bin, "qemu-system-aarch64")?;
        self.qemu_img_bin = resolve_binary(&self.qemu_img_bin, "qemu-img")?;
        self.docker_bin = resolve_binary(&self.docker_bin, "docker")?;
        if self.max_active_vms == Some(0) {
            self.max_active_vms = None;
        }
        if self.max_fork_chain_depth == 0 {
            self.max_fork_chain_depth = DEFAULT_MAX_FORK_CHAIN_DEPTH;
        }
        if self.fork_compaction_depth_threshold == 0 {
            self.fork_compaction_depth_threshold = DEFAULT_FORK_COMPACTION_DEPTH_THRESHOLD;
        }
        if self.fork_compaction_depth_threshold > self.max_fork_chain_depth {
            self.fork_compaction_depth_threshold = self.max_fork_chain_depth;
        }
        if self.ha_mode && !self.storage_profile.is_durable_shared() {
            bail!(
                "ha mode requires storage profile `{}` (got `{}`)",
                STORAGE_PROFILE_DURABLE_SHARED,
                self.storage_profile.as_str()
            );
        }
        self.shared_mount_profiles =
            normalize_shared_mount_profiles(self.shared_mount_profiles.clone());
        self.qemu_process.normalize()?;
        self.pci = match self.pci_policy_path.as_deref() {
            Some(path) if !path.trim().is_empty() => {
                let path = expand_home(path)?;
                self.pci_policy_path = Some(canonical_string(&path)?);
                PciConfig::load(&path)?
            }
            _ => {
                self.pci_policy_path = None;
                PciConfig::default()
            }
        };
        if self.ha_mode && self.pci.enabled() {
            bail!("PCI assignment is incompatible with ha mode");
        }
        self.guest_network.normalize()?;
        self.network_services.normalize(&self.guest_network)?;
        if let Some(registry) = self.node_registry.as_mut() {
            registry.storage_profile = self.storage_profile;
            registry.shared_mount_profiles = self.shared_mount_profiles.clone();
        }
        self.normalize_node_registry();
        self.normalize_control_bus();
        if self.ha_mode && self.node_registry.is_none() {
            bail!("ha mode requires node registry configuration");
        }
        if self.ha_mode && self.control_bus.is_none() {
            bail!("ha mode requires control bus configuration");
        }
        enforce_continuity_policy(self)?;
        self.normalize_security()?;

        Ok(())
    }

    fn normalize_node_registry(&mut self) {
        let Some(registry) = self.node_registry.as_mut() else {
            return;
        };

        registry.etcd_endpoints = registry
            .etcd_endpoints
            .iter()
            .map(String::as_str)
            .map(normalize_endpoint_like)
            .collect();
        registry.etcd_endpoints.retain(|value| !value.is_empty());

        if registry.etcd_endpoints.is_empty() {
            self.node_registry = None;
            return;
        }

        let prefix = registry.key_prefix.trim().trim_matches('/');
        registry.key_prefix = if prefix.is_empty() {
            DEFAULT_NODE_REGISTRY_PREFIX.to_string()
        } else {
            format!("/{prefix}")
        };

        if registry.node_id.trim().is_empty() {
            registry.node_id = default_node_id();
        }

        if registry.advertise_endpoint.trim().is_empty() {
            registry.advertise_endpoint = format!("http://{}", self.listen_address.trim());
        } else {
            registry.advertise_endpoint = normalize_endpoint_like(&registry.advertise_endpoint);
        }

        if registry.ttl_secs <= 0 {
            registry.ttl_secs = DEFAULT_NODE_REGISTRY_TTL_SECS;
        }
        if registry.max_active_vms == Some(0) {
            registry.max_active_vms = None;
        }
        registry.shared_mount_profiles =
            normalize_shared_mount_profiles(registry.shared_mount_profiles.clone());
        if registry.region.trim().is_empty() {
            registry.region = DEFAULT_NODE_REGION.to_string();
        }
        if registry.zone.trim().is_empty() {
            registry.zone = DEFAULT_NODE_ZONE.to_string();
        }
        if registry.rack.trim().is_empty() {
            registry.rack = DEFAULT_NODE_RACK.to_string();
        }
    }

    fn normalize_control_bus(&mut self) {
        let Some(control) = self.control_bus.as_mut() else {
            return;
        };

        control.nats_url = normalize_nats_url(&control.nats_url);
        if control.nats_url.is_empty() {
            self.control_bus = None;
            return;
        }

        if control.subject_prefix.trim().is_empty() {
            control.subject_prefix = DEFAULT_CONTROL_SUBJECT_PREFIX.to_string();
        }
        if control.cluster_id.trim().is_empty() {
            control.cluster_id = DEFAULT_CONTROL_CLUSTER_ID.to_string();
        }

        if control.node_id.trim().is_empty() {
            control.node_id = default_node_id();
        }

        control.dedupe_etcd_endpoints = control
            .dedupe_etcd_endpoints
            .iter()
            .map(String::as_str)
            .map(normalize_endpoint_like)
            .collect();
        control
            .dedupe_etcd_endpoints
            .retain(|value| !value.trim().is_empty());

        if control.dedupe_etcd_endpoints.is_empty() {
            if let Some(node_registry) = &self.node_registry {
                control.dedupe_etcd_endpoints = node_registry.etcd_endpoints.clone();
            }
        }

        let dedupe_prefix = control.dedupe_prefix.trim().trim_matches('/');
        control.dedupe_prefix = if dedupe_prefix.is_empty() {
            self.node_registry
                .as_ref()
                .and_then(|registry| {
                    control_dedupe_prefix_from_registry_prefix(&registry.key_prefix)
                })
                .unwrap_or_else(|| DEFAULT_CONTROL_DEDUPE_PREFIX.to_string())
        } else {
            format!("/{dedupe_prefix}")
        };

        if control.stream_name.trim().is_empty() {
            control.stream_name = DEFAULT_CONTROL_STREAM_NAME.to_string();
        }
        if control.stream_max_age_secs == 0 {
            control.stream_max_age_secs = DEFAULT_CONTROL_STREAM_MAX_AGE_SECS;
        }
        if control.stream_replicas == 0 {
            control.stream_replicas = DEFAULT_CONTROL_STREAM_REPLICAS;
        }
        if control.command_max_deliver <= 0 {
            control.command_max_deliver = DEFAULT_CONTROL_COMMAND_MAX_DELIVER;
        }
        if control.command_ack_wait_ms == 0 {
            control.command_ack_wait_ms = DEFAULT_CONTROL_COMMAND_ACK_WAIT_MS;
        }
        if control.max_inflight_commands == 0 {
            control.max_inflight_commands = DEFAULT_CONTROL_MAX_INFLIGHT_COMMANDS;
        }
        if control.overload_retry_after_ms == 0 {
            control.overload_retry_after_ms = DEFAULT_CONTROL_OVERLOAD_RETRY_AFTER_MS;
        }
        if control.dead_letter_subject.trim().is_empty() {
            control.dead_letter_subject = format!("{}.dlq.commands", control.subject_prefix);
        }
        if control.replay_subject.trim().is_empty() {
            control.replay_subject = format!("{}.replay.commands", control.subject_prefix);
        }
        if control.command_consumer_durable.trim().is_empty() {
            control.command_consumer_durable =
                format!("{}-cmd", sanitize_jetstream_name(&control.node_id));
        } else {
            control.command_consumer_durable =
                sanitize_jetstream_name(&control.command_consumer_durable);
        }
    }

    fn normalize_security(&mut self) -> Result<()> {
        if let Some(auth) = self.security.auth.as_mut() {
            auth.admin_token = auth.admin_token.trim().to_string();
            auth.readonly_token = auth
                .readonly_token
                .as_ref()
                .map(|value| value.trim().to_string());
            if auth.admin_token.is_empty() {
                self.security.auth = None;
            } else if auth
                .readonly_token
                .as_ref()
                .is_some_and(|value| value.is_empty())
            {
                auth.readonly_token = None;
            }
        }

        let Some(tls) = self.security.tls.as_mut() else {
            return Ok(());
        };

        if tls.cert_path.trim().is_empty() && tls.key_path.trim().is_empty() {
            self.security.tls = None;
            return Ok(());
        }
        if tls.cert_path.trim().is_empty() || tls.key_path.trim().is_empty() {
            bail!("both TLS cert path and key path must be configured");
        }

        let cert_path = expand_home(&tls.cert_path)?;
        fs::metadata(&cert_path)
            .with_context(|| format!("stat tls cert path {}", cert_path.to_string_lossy()))?;
        tls.cert_path = canonical_string(&cert_path)?;

        let key_path = expand_home(&tls.key_path)?;
        fs::metadata(&key_path)
            .with_context(|| format!("stat tls key path {}", key_path.to_string_lossy()))?;
        tls.key_path = canonical_string(&key_path)?;

        tls.client_ca_path = tls
            .client_ca_path
            .as_ref()
            .map(|value| value.trim().to_string());
        if let Some(client_ca_path) = tls.client_ca_path.as_ref() {
            if client_ca_path.is_empty() {
                tls.client_ca_path = None;
            } else {
                let expanded = expand_home(client_ca_path)?;
                fs::metadata(&expanded).with_context(|| {
                    format!("stat tls client ca path {}", expanded.to_string_lossy())
                })?;
                tls.client_ca_path = Some(canonical_string(&expanded)?);
            }
        }

        if tls.client_ca_path.is_none() {
            tls.require_client_cert = false;
        }

        Ok(())
    }
}

fn expand_home(path: &str) -> Result<PathBuf> {
    if path.is_empty() {
        bail!("empty path");
    }
    if let Some(stripped) = path.strip_prefix("~/") {
        let home = dirs::home_dir().context("resolve home directory")?;
        return Ok(home.join(stripped));
    }
    if path == "~" {
        let home = dirs::home_dir().context("resolve home directory")?;
        return Ok(home);
    }
    let candidate = PathBuf::from(path);
    if candidate.is_absolute() {
        Ok(candidate)
    } else {
        Ok(env::current_dir()?.join(candidate))
    }
}

fn resolve_binary(value: &str, fallback: &str) -> Result<String> {
    let candidate = {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            fallback
        } else {
            trimmed
        }
    };

    let path =
        lookup_executable(candidate).with_context(|| format!("locate executable {candidate}"))?;
    canonical_string(&path)
}

fn lookup_executable(candidate: &str) -> Result<PathBuf> {
    let path = PathBuf::from(candidate);
    if path.is_absolute() {
        fs::metadata(&path).with_context(|| format!("stat {}", path.to_string_lossy()))?;
        return Ok(path);
    }

    let paths = env::var_os("PATH").context("PATH environment variable unset")?;
    for dir in env::split_paths(&paths) {
        let full = dir.join(candidate);
        if full.is_file() {
            return Ok(full);
        }
    }
    bail!("{} not found in PATH", candidate);
}

fn canonical_string(path: &Path) -> Result<String> {
    let canonical = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    Ok(canonical.to_string_lossy().to_string())
}

fn default_node_registry_from_env(listen_address: &str) -> Option<NodeRegistryConfig> {
    let endpoints = parse_csv_env("CHEVALIER_SANDBOX_ETCD_ENDPOINTS")
        .or_else(|| parse_csv_env("BRACKET_SANDBOX_ETCD_ENDPOINTS"))?;

    let mut config = NodeRegistryConfig::defaults_for_listen(listen_address);
    config.etcd_endpoints = endpoints
        .into_iter()
        .map(|value| normalize_endpoint_like(&value))
        .collect();
    config.key_prefix = env::var("CHEVALIER_SANDBOX_ETCD_PREFIX")
        .or_else(|_| env::var("BRACKET_SANDBOX_ETCD_PREFIX"))
        .unwrap_or_else(|_| DEFAULT_NODE_REGISTRY_PREFIX.to_string());
    config.node_id = env::var("CHEVALIER_SANDBOX_NODE_ID")
        .or_else(|_| env::var("BRACKET_SANDBOX_NODE_ID"))
        .unwrap_or_else(|_| default_node_id());
    config.advertise_endpoint = env::var("CHEVALIER_SANDBOX_NODE_ENDPOINT")
        .or_else(|_| env::var("BRACKET_SANDBOX_NODE_ENDPOINT"))
        .map(|value| normalize_endpoint_like(&value))
        .unwrap_or_default();
    config.ttl_secs = env::var("CHEVALIER_SANDBOX_NODE_TTL_SECS")
        .or_else(|_| env::var("BRACKET_SANDBOX_NODE_TTL_SECS"))
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_NODE_REGISTRY_TTL_SECS);
    config.max_active_vms = env::var("CHEVALIER_SANDBOX_NODE_MAX_ACTIVE_VMS")
        .or_else(|_| env::var("BRACKET_SANDBOX_NODE_MAX_ACTIVE_VMS"))
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .or_else(default_max_active_vms_from_env);
    config.storage_profile = env::var("CHEVALIER_SANDBOX_NODE_STORAGE_PROFILE")
        .or_else(|_| env::var("BRACKET_SANDBOX_NODE_STORAGE_PROFILE"))
        .ok()
        .and_then(|value| parse_storage_profile(&value))
        .unwrap_or_else(default_storage_profile_from_env);
    config.continuity_tier = env::var("CHEVALIER_SANDBOX_NODE_CONTINUITY_TIER")
        .or_else(|_| env::var("BRACKET_SANDBOX_NODE_CONTINUITY_TIER"))
        .or_else(|_| env::var("CHEVALIER_SANDBOX_CONTINUITY_TIER"))
        .or_else(|_| env::var("BRACKET_SANDBOX_CONTINUITY_TIER"))
        .ok()
        .and_then(|value| parse_continuity_tier(&value))
        .unwrap_or_else(default_continuity_tier_from_env);
    config.degraded_mode = env::var("CHEVALIER_SANDBOX_NODE_DEGRADED_MODE")
        .or_else(|_| env::var("BRACKET_SANDBOX_NODE_DEGRADED_MODE"))
        .or_else(|_| env::var("CHEVALIER_SANDBOX_DEGRADED_MODE"))
        .or_else(|_| env::var("BRACKET_SANDBOX_DEGRADED_MODE"))
        .ok()
        .and_then(|value| parse_bool_flag(&value))
        .unwrap_or_else(default_degraded_mode_from_env);
    config.admission_frozen = env::var("CHEVALIER_SANDBOX_NODE_ADMISSION_FROZEN")
        .or_else(|_| env::var("BRACKET_SANDBOX_NODE_ADMISSION_FROZEN"))
        .ok()
        .and_then(|value| parse_bool_flag(&value))
        .unwrap_or_else(default_admission_frozen_from_env);
    config.shared_mount_profiles = env::var("CHEVALIER_SANDBOX_NODE_SHARED_MOUNT_PROFILES")
        .or_else(|_| env::var("BRACKET_SANDBOX_NODE_SHARED_MOUNT_PROFILES"))
        .ok()
        .map(|value| parse_csv_list(&value))
        .unwrap_or_else(default_shared_mount_profiles_from_env);

    Some(config)
}

fn default_control_bus_from_env() -> Option<ControlBusConfig> {
    let nats_url = env::var("CHEVALIER_SANDBOX_NATS_URL")
        .or_else(|_| env::var("BRACKET_SANDBOX_NATS_URL"))
        .ok()
        .map(|value| normalize_nats_url(&value))?;
    if nats_url.is_empty() {
        return None;
    }

    let subject_prefix = env::var("CHEVALIER_SANDBOX_NATS_SUBJECT_PREFIX")
        .or_else(|_| env::var("BRACKET_SANDBOX_NATS_SUBJECT_PREFIX"))
        .unwrap_or_else(|_| DEFAULT_CONTROL_SUBJECT_PREFIX.to_string());
    let cluster_id = env::var("CHEVALIER_SANDBOX_CLUSTER_ID")
        .or_else(|_| env::var("BRACKET_SANDBOX_CLUSTER_ID"))
        .unwrap_or_else(|_| DEFAULT_CONTROL_CLUSTER_ID.to_string());
    let node_id = env::var("CHEVALIER_SANDBOX_NODE_ID")
        .or_else(|_| env::var("BRACKET_SANDBOX_NODE_ID"))
        .unwrap_or_else(|_| default_node_id());
    let dedupe_etcd_endpoints = parse_csv_env("CHEVALIER_SANDBOX_ETCD_ENDPOINTS")
        .or_else(|| parse_csv_env("BRACKET_SANDBOX_ETCD_ENDPOINTS"))
        .unwrap_or_default()
        .into_iter()
        .map(|value| normalize_endpoint_like(&value))
        .collect();
    let dedupe_prefix = default_control_dedupe_prefix_from_env();
    let stream_name = env::var("CHEVALIER_SANDBOX_NATS_STREAM")
        .or_else(|_| env::var("BRACKET_SANDBOX_NATS_STREAM"))
        .unwrap_or_else(|_| DEFAULT_CONTROL_STREAM_NAME.to_string());
    let stream_max_age_secs = env::var("CHEVALIER_SANDBOX_NATS_STREAM_MAX_AGE_SECS")
        .or_else(|_| env::var("BRACKET_SANDBOX_NATS_STREAM_MAX_AGE_SECS"))
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_CONTROL_STREAM_MAX_AGE_SECS);
    let stream_replicas = env::var("CHEVALIER_SANDBOX_NATS_STREAM_REPLICAS")
        .or_else(|_| env::var("BRACKET_SANDBOX_NATS_STREAM_REPLICAS"))
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_CONTROL_STREAM_REPLICAS);
    let command_consumer_durable = env::var("CHEVALIER_SANDBOX_CONTROL_CONSUMER_DURABLE")
        .or_else(|_| env::var("BRACKET_SANDBOX_CONTROL_CONSUMER_DURABLE"))
        .unwrap_or_else(|_| format!("{}-cmd", sanitize_jetstream_name(&node_id)));
    let command_max_deliver = env::var("CHEVALIER_SANDBOX_CONTROL_MAX_DELIVER")
        .or_else(|_| env::var("BRACKET_SANDBOX_CONTROL_MAX_DELIVER"))
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_CONTROL_COMMAND_MAX_DELIVER);
    let command_ack_wait_ms = env::var("CHEVALIER_SANDBOX_CONTROL_ACK_WAIT_MS")
        .or_else(|_| env::var("BRACKET_SANDBOX_CONTROL_ACK_WAIT_MS"))
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_CONTROL_COMMAND_ACK_WAIT_MS);
    let dead_letter_subject = env::var("CHEVALIER_SANDBOX_NATS_DEAD_LETTER_SUBJECT")
        .or_else(|_| env::var("BRACKET_SANDBOX_NATS_DEAD_LETTER_SUBJECT"))
        .unwrap_or_else(|_| format!("{}.dlq.commands", subject_prefix));
    let replay_subject = env::var("CHEVALIER_SANDBOX_NATS_REPLAY_SUBJECT")
        .or_else(|_| env::var("BRACKET_SANDBOX_NATS_REPLAY_SUBJECT"))
        .unwrap_or_else(|_| format!("{}.replay.commands", subject_prefix));
    let max_inflight_commands = env::var("CHEVALIER_SANDBOX_CONTROL_MAX_INFLIGHT")
        .or_else(|_| env::var("BRACKET_SANDBOX_CONTROL_MAX_INFLIGHT"))
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_CONTROL_MAX_INFLIGHT_COMMANDS);
    let overload_retry_after_ms = env::var("CHEVALIER_SANDBOX_CONTROL_OVERLOAD_RETRY_AFTER_MS")
        .or_else(|_| env::var("BRACKET_SANDBOX_CONTROL_OVERLOAD_RETRY_AFTER_MS"))
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_CONTROL_OVERLOAD_RETRY_AFTER_MS);

    Some(ControlBusConfig {
        nats_url,
        nats_auth_token: optional_env_string(&[
            "CHEVALIER_SANDBOX_NATS_AUTH_TOKEN",
            "BRACKET_SANDBOX_NATS_AUTH_TOKEN",
        ]),
        subject_prefix: subject_prefix.clone(),
        cluster_id,
        node_id,
        dedupe_etcd_endpoints,
        dedupe_prefix,
        stream_name,
        stream_max_age_secs,
        stream_replicas,
        command_consumer_durable,
        command_max_deliver,
        command_ack_wait_ms,
        max_inflight_commands,
        overload_retry_after_ms,
        dead_letter_subject,
        replay_subject,
    })
}

fn default_control_dedupe_prefix_from_env() -> String {
    env::var("CHEVALIER_SANDBOX_CONTROL_DEDUPE_PREFIX")
        .or_else(|_| env::var("BRACKET_SANDBOX_CONTROL_DEDUPE_PREFIX"))
        .ok()
        .and_then(|value| normalize_nonempty_prefix(&value))
        .or_else(|| {
            env::var("CHEVALIER_SANDBOX_ETCD_PREFIX")
                .or_else(|_| env::var("BRACKET_SANDBOX_ETCD_PREFIX"))
                .ok()
                .and_then(|value| control_dedupe_prefix_from_registry_prefix(&value))
        })
        .unwrap_or_else(|| DEFAULT_CONTROL_DEDUPE_PREFIX.to_string())
}

fn control_dedupe_prefix_from_registry_prefix(registry_prefix: &str) -> Option<String> {
    let prefix = normalize_nonempty_prefix(registry_prefix)?;
    if prefix.ends_with("/command-dedupe") {
        Some(prefix)
    } else {
        Some(format!("{prefix}/command-dedupe"))
    }
}

fn normalize_nonempty_prefix(prefix: &str) -> Option<String> {
    let trimmed = prefix.trim().trim_matches('/');
    if trimmed.is_empty() {
        None
    } else {
        Some(format!("/{trimmed}"))
    }
}

fn default_max_active_vms_from_env() -> Option<usize> {
    env::var("CHEVALIER_SANDBOX_MAX_ACTIVE_VMS")
        .or_else(|_| env::var("BRACKET_SANDBOX_MAX_ACTIVE_VMS"))
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
}

fn default_snapshot_staging_dir_from_env() -> Option<String> {
    env::var("CHEVALIER_SANDBOX_SNAPSHOT_STAGING_DIR")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn default_max_fork_chain_depth_from_env() -> usize {
    env::var("CHEVALIER_SANDBOX_MAX_FORK_CHAIN_DEPTH")
        .or_else(|_| env::var("BRACKET_SANDBOX_MAX_FORK_CHAIN_DEPTH"))
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_FORK_CHAIN_DEPTH)
}

fn default_fork_compaction_depth_threshold_from_env() -> usize {
    env::var("CHEVALIER_SANDBOX_FORK_COMPACTION_DEPTH_THRESHOLD")
        .or_else(|_| env::var("BRACKET_SANDBOX_FORK_COMPACTION_DEPTH_THRESHOLD"))
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_FORK_COMPACTION_DEPTH_THRESHOLD)
}

/// Default location of the `virtiofsd` binary. Ubuntu 24.04's `virtiofsd` package
/// (the Rust reimplementation from gitlab.com/virtio-fs/virtiofsd) installs the binary
/// at `/usr/libexec/virtiofsd`. Override via `CHEVALIER_SANDBOX_VIRTIOFSD_BIN` when running
/// against a different base image or a custom build.
fn default_virtiofsd_bin() -> String {
    env::var("CHEVALIER_SANDBOX_VIRTIOFSD_BIN")
        .or_else(|_| env::var("BRACKET_SANDBOX_VIRTIOFSD_BIN"))
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "/usr/libexec/virtiofsd".to_string())
}

fn default_storage_profile_from_env() -> StorageProfile {
    env::var("CHEVALIER_SANDBOX_STORAGE_PROFILE")
        .or_else(|_| env::var("BRACKET_SANDBOX_STORAGE_PROFILE"))
        .ok()
        .and_then(|value| parse_storage_profile(&value))
        .unwrap_or(StorageProfile::LocalEphemeral)
}

fn default_continuity_tier_from_env() -> ContinuityTier {
    env::var("CHEVALIER_SANDBOX_CONTINUITY_TIER")
        .or_else(|_| env::var("BRACKET_SANDBOX_CONTINUITY_TIER"))
        .ok()
        .and_then(|value| parse_continuity_tier(&value))
        .unwrap_or(ContinuityTier::TierB)
}

fn default_degraded_mode_from_env() -> bool {
    env::var("CHEVALIER_SANDBOX_DEGRADED_MODE")
        .or_else(|_| env::var("BRACKET_SANDBOX_DEGRADED_MODE"))
        .ok()
        .and_then(|value| parse_bool_flag(&value))
        .unwrap_or(false)
}

fn default_node_region_from_env() -> String {
    env::var("CHEVALIER_SANDBOX_NODE_REGION")
        .or_else(|_| env::var("BRACKET_SANDBOX_NODE_REGION"))
        .unwrap_or_else(|_| DEFAULT_NODE_REGION.to_string())
}

fn default_node_zone_from_env() -> String {
    env::var("CHEVALIER_SANDBOX_NODE_ZONE")
        .or_else(|_| env::var("BRACKET_SANDBOX_NODE_ZONE"))
        .unwrap_or_else(|_| DEFAULT_NODE_ZONE.to_string())
}

fn default_node_rack_from_env() -> String {
    env::var("CHEVALIER_SANDBOX_NODE_RACK")
        .or_else(|_| env::var("BRACKET_SANDBOX_NODE_RACK"))
        .unwrap_or_else(|_| DEFAULT_NODE_RACK.to_string())
}

fn default_admission_frozen_from_env() -> bool {
    env::var("CHEVALIER_SANDBOX_NODE_ADMISSION_FROZEN")
        .or_else(|_| env::var("BRACKET_SANDBOX_NODE_ADMISSION_FROZEN"))
        .ok()
        .and_then(|value| parse_bool_flag(&value))
        .unwrap_or(false)
}

fn default_ha_mode_from_env() -> bool {
    env::var("CHEVALIER_SANDBOX_HA_MODE")
        .or_else(|_| env::var("BRACKET_SANDBOX_HA_MODE"))
        .ok()
        .and_then(|value| parse_bool_flag(&value))
        .unwrap_or(false)
}

fn default_shared_mount_profiles_from_env() -> Vec<String> {
    env::var("CHEVALIER_SANDBOX_SHARED_MOUNT_PROFILES")
        .or_else(|_| env::var("BRACKET_SANDBOX_SHARED_MOUNT_PROFILES"))
        .ok()
        .map(|value| parse_csv_list(&value))
        .unwrap_or_else(|| vec![DEFAULT_SHARED_MOUNT_PROFILE_LOCAL.to_string()])
}

fn default_vfs_internal_service_token_from_env() -> Option<String> {
    env::var("CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN")
        .or_else(|_| env::var("BRACKET_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn default_pci_policy_path_from_env() -> Option<String> {
    env::var("CHEVALIER_SANDBOX_PCI_POLICY")
        .or_else(|_| env::var("BRACKET_SANDBOX_PCI_POLICY"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn default_guest_network_from_env() -> GuestNetworkConfig {
    GuestNetworkConfig {
        dns_server: default_guest_dns_server_from_env(),
        http_proxy_guest_addr: default_guest_http_proxy_guest_addr_from_env(),
        http_proxy_upstream_addr: default_guest_http_proxy_upstream_addr_from_env(),
    }
}

fn default_qemu_run_as_uid_from_env() -> u32 {
    env::var("CHEVALIER_SANDBOX_QEMU_RUN_AS_UID")
        .or_else(|_| env::var("BRACKET_SANDBOX_QEMU_RUN_AS_UID"))
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(1000)
}

fn default_qemu_run_as_gid_from_env() -> u32 {
    env::var("CHEVALIER_SANDBOX_QEMU_RUN_AS_GID")
        .or_else(|_| env::var("BRACKET_SANDBOX_QEMU_RUN_AS_GID"))
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(1000)
}

fn default_guest_dns_server_from_env() -> Option<String> {
    env::var("CHEVALIER_SANDBOX_GUEST_DNS_SERVER")
        .or_else(|_| env::var("BRACKET_SANDBOX_GUEST_DNS_SERVER"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn default_guest_http_proxy_guest_addr_from_env() -> Option<String> {
    env::var("CHEVALIER_SANDBOX_GUEST_HTTP_PROXY_GUEST_ADDR")
        .or_else(|_| env::var("BRACKET_SANDBOX_GUEST_HTTP_PROXY_GUEST_ADDR"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn default_guest_http_proxy_upstream_addr_from_env() -> Option<String> {
    env::var("CHEVALIER_SANDBOX_GUEST_HTTP_PROXY_UPSTREAM_ADDR")
        .or_else(|_| env::var("BRACKET_SANDBOX_GUEST_HTTP_PROXY_UPSTREAM_ADDR"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn default_envoy_bin_from_env() -> String {
    env::var("CHEVALIER_SANDBOX_ENVOY_BIN")
        .or_else(|_| env::var("BRACKET_SANDBOX_ENVOY_BIN"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "envoy".to_string())
}

fn default_envoy_log_level_from_env() -> String {
    env::var("CHEVALIER_SANDBOX_ENVOY_LOG_LEVEL")
        .or_else(|_| env::var("BRACKET_SANDBOX_ENVOY_LOG_LEVEL"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "info".to_string())
}

fn default_envoy_base_id_from_env() -> Option<u32> {
    env::var("CHEVALIER_SANDBOX_ENVOY_BASE_ID")
        .or_else(|_| env::var("BRACKET_SANDBOX_ENVOY_BASE_ID"))
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
}

fn default_envoy_admin_addr_from_env() -> String {
    env::var("CHEVALIER_SANDBOX_ENVOY_ADMIN_ADDR")
        .or_else(|_| env::var("BRACKET_SANDBOX_ENVOY_ADMIN_ADDR"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "127.0.0.1:9901".to_string())
}

fn default_coredns_bin_from_env() -> String {
    env::var("CHEVALIER_SANDBOX_COREDNS_BIN")
        .or_else(|_| env::var("BRACKET_SANDBOX_COREDNS_BIN"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "coredns".to_string())
}

fn default_coredns_bind_addr_from_env() -> String {
    env::var("CHEVALIER_SANDBOX_COREDNS_BIND_ADDR")
        .or_else(|_| env::var("BRACKET_SANDBOX_COREDNS_BIND_ADDR"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "0.0.0.0:15053".to_string())
}

fn default_coredns_threat_hosts_path_from_env() -> String {
    env::var("CHEVALIER_SANDBOX_COREDNS_THREAT_HOSTS_PATH")
        .or_else(|_| env::var("BRACKET_SANDBOX_COREDNS_THREAT_HOSTS_PATH"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "/opt/chevalier/network/threat-domains.hosts".to_string())
}

fn default_public_ingress_bind_addr_from_env() -> Option<String> {
    env::var("CHEVALIER_SANDBOX_PUBLIC_INGRESS_BIND_ADDR")
        .or_else(|_| env::var("BRACKET_SANDBOX_PUBLIC_INGRESS_BIND_ADDR"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn default_api_internal_base_url_from_env() -> Option<String> {
    env::var("CHEVALIER_SANDBOX_API_INTERNAL_BASE_URL")
        .or_else(|_| env::var("BRACKET_SANDBOX_API_INTERNAL_BASE_URL"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn default_security_from_env() -> SecurityConfig {
    let admin_token = env::var("CHEVALIER_SANDBOX_AUTH_TOKEN")
        .or_else(|_| env::var("BRACKET_SANDBOX_AUTH_TOKEN"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let readonly_token = env::var("CHEVALIER_SANDBOX_READONLY_AUTH_TOKEN")
        .or_else(|_| env::var("BRACKET_SANDBOX_READONLY_AUTH_TOKEN"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    let auth = admin_token.map(|admin_token| AuthConfig {
        admin_token,
        readonly_token,
    });

    let cert_path = env::var("CHEVALIER_SANDBOX_TLS_CERT_PATH")
        .or_else(|_| env::var("BRACKET_SANDBOX_TLS_CERT_PATH"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let key_path = env::var("CHEVALIER_SANDBOX_TLS_KEY_PATH")
        .or_else(|_| env::var("BRACKET_SANDBOX_TLS_KEY_PATH"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let client_ca_path = env::var("CHEVALIER_SANDBOX_TLS_CLIENT_CA_PATH")
        .or_else(|_| env::var("BRACKET_SANDBOX_TLS_CLIENT_CA_PATH"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let require_client_cert = env::var("CHEVALIER_SANDBOX_TLS_REQUIRE_CLIENT_CERT")
        .or_else(|_| env::var("BRACKET_SANDBOX_TLS_REQUIRE_CLIENT_CERT"))
        .ok()
        .and_then(|value| parse_bool_flag(&value))
        .unwrap_or(DEFAULT_REQUIRE_CLIENT_CERT);

    let tls = match (cert_path, key_path) {
        (Some(cert_path), Some(key_path)) => Some(TlsServerConfig {
            cert_path,
            key_path,
            client_ca_path,
            require_client_cert,
        }),
        _ => None,
    };

    SecurityConfig { auth, tls }
}

fn parse_csv_env(name: &str) -> Option<Vec<String>> {
    let value = env::var(name).ok()?;
    let endpoints: Vec<String> = value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    if endpoints.is_empty() {
        return None;
    }
    Some(endpoints)
}

fn optional_env_string(names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        env::var(name)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

fn parse_csv_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn normalize_shared_mount_profiles(values: Vec<String>) -> Vec<String> {
    let mut normalized = values
        .into_iter()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if !normalized
        .iter()
        .any(|value| value == DEFAULT_SHARED_MOUNT_PROFILE_LOCAL)
    {
        normalized.push(DEFAULT_SHARED_MOUNT_PROFILE_LOCAL.to_string());
    }
    normalized.sort();
    normalized.dedup();
    normalized
}

fn normalize_endpoint_like(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if trimmed.contains("://") {
        return trimmed.to_string();
    }
    format!("http://{trimmed}")
}

fn normalize_nats_url(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if trimmed.contains("://") {
        return trimmed.to_string();
    }
    format!("nats://{trimmed}")
}

fn normalize_optional_ip_addr(value: Option<String>, field: &str) -> Result<Option<String>> {
    let Some(value) = value.map(|item| item.trim().to_string()) else {
        return Ok(None);
    };
    if value.is_empty() {
        return Ok(None);
    }
    let parsed = value
        .parse::<std::net::IpAddr>()
        .with_context(|| format!("invalid {field}: `{value}`"))?;
    Ok(Some(parsed.to_string()))
}

fn normalize_optional_socket_addr(value: Option<String>, field: &str) -> Result<Option<String>> {
    let Some(value) = value.map(|item| item.trim().to_string()) else {
        return Ok(None);
    };
    if value.is_empty() {
        return Ok(None);
    }
    let parsed = value
        .parse::<std::net::SocketAddr>()
        .with_context(|| format!("invalid {field}: `{value}`"))?;
    Ok(Some(parsed.to_string()))
}

fn normalize_socket_addr(raw: &str, field: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("{field} must be provided");
    }
    let parsed = trimmed
        .parse::<std::net::SocketAddr>()
        .with_context(|| format!("invalid {field}: `{trimmed}`"))?;
    Ok(parsed.to_string())
}

fn sanitize_jetstream_name(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "chevalier_sandbox".to_string()
    } else {
        trimmed.to_string()
    }
}

fn default_node_id() -> String {
    if let Ok(value) = env::var("HOSTNAME") {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    format!("node-{}", Uuid::new_v4())
}

fn parse_bool_flag(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn parse_storage_profile(raw: &str) -> Option<StorageProfile> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "local-ephemeral" | "local_ephemeral" | "local" => Some(StorageProfile::LocalEphemeral),
        "durable-shared" | "durable_shared" | "durable" => Some(StorageProfile::DurableShared),
        _ => None,
    }
}

fn parse_continuity_tier(raw: &str) -> Option<ContinuityTier> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "tier-a" | "tier_a" | "a" => Some(ContinuityTier::TierA),
        "tier-b" | "tier_b" | "b" => Some(ContinuityTier::TierB),
        _ => None,
    }
}

fn enforce_continuity_policy(config: &Config) -> Result<()> {
    let Some(registry) = config.node_registry.as_ref() else {
        return Ok(());
    };

    if registry.degraded_mode && registry.continuity_tier != ContinuityTier::TierA {
        bail!(
            "degraded mode requires continuity tier `{}` (got `{}`)",
            CONTINUITY_TIER_A,
            registry.continuity_tier.as_str()
        );
    }

    if !config.ha_mode {
        return Ok(());
    }

    match registry.continuity_tier {
        ContinuityTier::TierB => {
            if registry.degraded_mode {
                bail!(
                    "ha mode cannot advertise tier-b while degraded mode is enabled; set continuity tier to `{}`",
                    CONTINUITY_TIER_A
                );
            }
        }
        ContinuityTier::TierA => {
            if !registry.degraded_mode {
                bail!(
                    "ha mode defaults to continuity tier `{}`; tier-a requires explicit degraded mode",
                    CONTINUITY_TIER_B
                );
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{LazyLock, Mutex};

    use super::*;

    static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn make_config(ha_mode: bool, tier: ContinuityTier, degraded_mode: bool) -> Config {
        Config {
            ha_mode,
            node_registry: Some(NodeRegistryConfig {
                etcd_endpoints: vec!["http://127.0.0.1:2379".to_string()],
                key_prefix: "/chevalier-sandbox".to_string(),
                node_id: "node-test".to_string(),
                advertise_endpoint: "http://127.0.0.1:8052".to_string(),
                ttl_secs: 15,
                max_active_vms: Some(10),
                storage_profile: StorageProfile::DurableShared,
                continuity_tier: tier,
                degraded_mode,
                admission_frozen: false,
                shared_mount_profiles: vec![
                    DEFAULT_SHARED_MOUNT_PROFILE_LOCAL.to_string(),
                    "shared-posix".to_string(),
                ],
                region: "test-region".to_string(),
                zone: "test-zone".to_string(),
                rack: "test-rack".to_string(),
            }),
            ..Config::default()
        }
    }

    #[test]
    fn continuity_policy_allows_ha_tier_b_default() {
        let cfg = make_config(true, ContinuityTier::TierB, false);
        assert!(enforce_continuity_policy(&cfg).is_ok());
    }

    #[test]
    fn continuity_policy_rejects_ha_tier_a_without_degraded_mode() {
        let cfg = make_config(true, ContinuityTier::TierA, false);
        assert!(enforce_continuity_policy(&cfg).is_err());
    }

    #[test]
    fn continuity_policy_allows_ha_tier_a_only_when_degraded_mode_enabled() {
        let cfg = make_config(true, ContinuityTier::TierA, true);
        assert!(enforce_continuity_policy(&cfg).is_ok());
    }

    #[test]
    fn continuity_policy_rejects_degraded_mode_with_tier_b() {
        let cfg = make_config(true, ContinuityTier::TierB, true);
        assert!(enforce_continuity_policy(&cfg).is_err());
    }

    #[test]
    fn vfs_internal_service_token_prefers_chevalier_env_then_legacy_fallback() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        unsafe {
            env::remove_var("CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN");
            env::remove_var("BRACKET_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN");
        }
        assert_eq!(default_vfs_internal_service_token_from_env(), None);

        unsafe {
            env::set_var("BRACKET_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN", "legacy-token");
        }
        assert_eq!(
            default_vfs_internal_service_token_from_env().as_deref(),
            Some("legacy-token")
        );

        unsafe {
            env::set_var(
                "CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN",
                "primary-token",
            );
        }
        assert_eq!(
            default_vfs_internal_service_token_from_env().as_deref(),
            Some("primary-token")
        );

        unsafe {
            env::remove_var("CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN");
            env::remove_var("BRACKET_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN");
        }
    }

    #[test]
    fn envoy_base_id_prefers_chevalier_env_then_legacy_fallback() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        unsafe {
            env::remove_var("CHEVALIER_SANDBOX_ENVOY_BASE_ID");
            env::remove_var("BRACKET_SANDBOX_ENVOY_BASE_ID");
        }
        assert_eq!(default_envoy_base_id_from_env(), None);

        unsafe {
            env::set_var("BRACKET_SANDBOX_ENVOY_BASE_ID", "7");
        }
        assert_eq!(default_envoy_base_id_from_env(), Some(7));

        unsafe {
            env::set_var("CHEVALIER_SANDBOX_ENVOY_BASE_ID", "11");
        }
        assert_eq!(default_envoy_base_id_from_env(), Some(11));

        unsafe {
            env::remove_var("CHEVALIER_SANDBOX_ENVOY_BASE_ID");
            env::remove_var("BRACKET_SANDBOX_ENVOY_BASE_ID");
        }
    }

    #[test]
    fn control_dedupe_prefix_defaults_to_registry_prefix() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        unsafe {
            env::remove_var("CHEVALIER_SANDBOX_CONTROL_DEDUPE_PREFIX");
            env::remove_var("BRACKET_SANDBOX_CONTROL_DEDUPE_PREFIX");
            env::remove_var("CHEVALIER_SANDBOX_ETCD_PREFIX");
            env::remove_var("BRACKET_SANDBOX_ETCD_PREFIX");
            env::remove_var("CHEVALIER_SANDBOX_NATS_URL");
            env::remove_var("BRACKET_SANDBOX_NATS_URL");
        }

        unsafe {
            env::set_var("CHEVALIER_SANDBOX_NATS_URL", "nats://127.0.0.1:4222");
            env::set_var("BRACKET_SANDBOX_ETCD_PREFIX", "/legacy-registry");
        }
        assert_eq!(
            default_control_bus_from_env()
                .expect("control bus config")
                .dedupe_prefix,
            "/legacy-registry/command-dedupe"
        );

        unsafe {
            env::set_var(
                "CHEVALIER_SANDBOX_ETCD_PREFIX",
                "/chevalier-sandbox-otheryou",
            );
        }
        assert_eq!(
            default_control_bus_from_env()
                .expect("control bus config")
                .dedupe_prefix,
            "/chevalier-sandbox-otheryou/command-dedupe"
        );

        unsafe {
            env::set_var("CHEVALIER_SANDBOX_CONTROL_DEDUPE_PREFIX", "/custom-dedupe");
        }
        assert_eq!(
            default_control_bus_from_env()
                .expect("control bus config")
                .dedupe_prefix,
            "/custom-dedupe"
        );

        unsafe {
            env::remove_var("CHEVALIER_SANDBOX_CONTROL_DEDUPE_PREFIX");
            env::remove_var("BRACKET_SANDBOX_CONTROL_DEDUPE_PREFIX");
            env::remove_var("CHEVALIER_SANDBOX_ETCD_PREFIX");
            env::remove_var("BRACKET_SANDBOX_ETCD_PREFIX");
            env::remove_var("CHEVALIER_SANDBOX_NATS_URL");
            env::remove_var("BRACKET_SANDBOX_NATS_URL");
        }
    }

    #[test]
    fn guest_network_from_env_prefers_chevalier_env_then_legacy_fallback() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        unsafe {
            env::remove_var("CHEVALIER_SANDBOX_GUEST_DNS_SERVER");
            env::remove_var("BRACKET_SANDBOX_GUEST_DNS_SERVER");
            env::remove_var("CHEVALIER_SANDBOX_GUEST_HTTP_PROXY_GUEST_ADDR");
            env::remove_var("BRACKET_SANDBOX_GUEST_HTTP_PROXY_GUEST_ADDR");
            env::remove_var("CHEVALIER_SANDBOX_GUEST_HTTP_PROXY_UPSTREAM_ADDR");
            env::remove_var("BRACKET_SANDBOX_GUEST_HTTP_PROXY_UPSTREAM_ADDR");
        }

        assert_eq!(
            default_guest_network_from_env(),
            GuestNetworkConfig::default()
        );

        unsafe {
            env::set_var("BRACKET_SANDBOX_GUEST_DNS_SERVER", "10.0.2.3");
            env::set_var(
                "BRACKET_SANDBOX_GUEST_HTTP_PROXY_GUEST_ADDR",
                "10.0.2.100:3128",
            );
            env::set_var(
                "BRACKET_SANDBOX_GUEST_HTTP_PROXY_UPSTREAM_ADDR",
                "127.0.0.1:3128",
            );
        }
        assert_eq!(
            default_guest_network_from_env(),
            GuestNetworkConfig {
                dns_server: Some("10.0.2.3".to_string()),
                http_proxy_guest_addr: Some("10.0.2.100:3128".to_string()),
                http_proxy_upstream_addr: Some("127.0.0.1:3128".to_string()),
            }
        );

        unsafe {
            env::set_var("CHEVALIER_SANDBOX_GUEST_DNS_SERVER", "10.0.2.4");
            env::set_var(
                "CHEVALIER_SANDBOX_GUEST_HTTP_PROXY_GUEST_ADDR",
                "10.0.2.101:3129",
            );
            env::set_var(
                "CHEVALIER_SANDBOX_GUEST_HTTP_PROXY_UPSTREAM_ADDR",
                "127.0.0.1:3129",
            );
        }
        assert_eq!(
            default_guest_network_from_env(),
            GuestNetworkConfig {
                dns_server: Some("10.0.2.4".to_string()),
                http_proxy_guest_addr: Some("10.0.2.101:3129".to_string()),
                http_proxy_upstream_addr: Some("127.0.0.1:3129".to_string()),
            }
        );

        unsafe {
            env::remove_var("CHEVALIER_SANDBOX_GUEST_DNS_SERVER");
            env::remove_var("BRACKET_SANDBOX_GUEST_DNS_SERVER");
            env::remove_var("CHEVALIER_SANDBOX_GUEST_HTTP_PROXY_GUEST_ADDR");
            env::remove_var("BRACKET_SANDBOX_GUEST_HTTP_PROXY_GUEST_ADDR");
            env::remove_var("CHEVALIER_SANDBOX_GUEST_HTTP_PROXY_UPSTREAM_ADDR");
            env::remove_var("BRACKET_SANDBOX_GUEST_HTTP_PROXY_UPSTREAM_ADDR");
        }
    }

    #[test]
    fn qemu_process_identity_from_env_prefers_chevalier_env_then_legacy_fallback() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        unsafe {
            env::remove_var("CHEVALIER_SANDBOX_QEMU_RUN_AS_UID");
            env::remove_var("BRACKET_SANDBOX_QEMU_RUN_AS_UID");
            env::remove_var("CHEVALIER_SANDBOX_QEMU_RUN_AS_GID");
            env::remove_var("BRACKET_SANDBOX_QEMU_RUN_AS_GID");
        }

        assert_eq!(default_qemu_run_as_uid_from_env(), 1000);
        assert_eq!(default_qemu_run_as_gid_from_env(), 1000);

        unsafe {
            env::set_var("BRACKET_SANDBOX_QEMU_RUN_AS_UID", "2001");
            env::set_var("BRACKET_SANDBOX_QEMU_RUN_AS_GID", "2002");
        }
        assert_eq!(default_qemu_run_as_uid_from_env(), 2001);
        assert_eq!(default_qemu_run_as_gid_from_env(), 2002);

        unsafe {
            env::set_var("CHEVALIER_SANDBOX_QEMU_RUN_AS_UID", "3001");
            env::set_var("CHEVALIER_SANDBOX_QEMU_RUN_AS_GID", "3002");
        }
        assert_eq!(default_qemu_run_as_uid_from_env(), 3001);
        assert_eq!(default_qemu_run_as_gid_from_env(), 3002);

        unsafe {
            env::remove_var("CHEVALIER_SANDBOX_QEMU_RUN_AS_UID");
            env::remove_var("BRACKET_SANDBOX_QEMU_RUN_AS_UID");
            env::remove_var("CHEVALIER_SANDBOX_QEMU_RUN_AS_GID");
            env::remove_var("BRACKET_SANDBOX_QEMU_RUN_AS_GID");
        }
    }

    #[test]
    fn guest_network_normalize_requires_complete_proxy_pair() {
        let mut guest_network = GuestNetworkConfig {
            dns_server: None,
            http_proxy_guest_addr: Some("10.0.2.100:3128".to_string()),
            http_proxy_upstream_addr: None,
        };
        assert!(guest_network.normalize().is_err());

        let mut guest_network = GuestNetworkConfig {
            dns_server: Some(" 10.0.2.3 ".to_string()),
            http_proxy_guest_addr: Some(" 10.0.2.100:3128 ".to_string()),
            http_proxy_upstream_addr: Some(" 127.0.0.1:3128 ".to_string()),
        };
        guest_network
            .normalize()
            .expect("guest network config should normalize");
        assert_eq!(guest_network.dns_server.as_deref(), Some("10.0.2.3"));
        assert_eq!(
            guest_network.http_proxy_guest_addr.as_deref(),
            Some("10.0.2.100:3128")
        );
        assert_eq!(
            guest_network.http_proxy_upstream_addr.as_deref(),
            Some("127.0.0.1:3128")
        );

        let mut guest_network = GuestNetworkConfig {
            dns_server: None,
            http_proxy_guest_addr: Some("10.0.2.100:3128".to_string()),
            http_proxy_upstream_addr: Some("127.0.0.1:3128".to_string()),
        };
        guest_network
            .normalize()
            .expect("proxy-enabled guest should default slirp dns");
        assert_eq!(guest_network.dns_server.as_deref(), Some("10.0.2.3"));
    }

    #[test]
    fn network_services_normalize_keeps_local_mode_free_of_proxy_binaries() {
        let guest_network = GuestNetworkConfig::default();
        let mut network_services = NetworkServicesConfig {
            envoy_bin: "__missing_envoy_for_local_mode_test__".to_string(),
            coredns_bin: "__missing_coredns_for_local_mode_test__".to_string(),
            ..NetworkServicesConfig::default()
        };

        network_services
            .normalize(&guest_network)
            .expect("local mode should not require envoy or coredns binaries");
    }

    #[test]
    fn network_services_normalize_requires_proxy_binaries_when_enabled() {
        let guest_network = GuestNetworkConfig {
            dns_server: None,
            http_proxy_guest_addr: Some("10.0.2.100:3128".to_string()),
            http_proxy_upstream_addr: Some("127.0.0.1:3128".to_string()),
        };
        let mut network_services = NetworkServicesConfig {
            envoy_bin: "__missing_envoy_for_proxy_mode_test__".to_string(),
            coredns_bin: "__missing_coredns_for_proxy_mode_test__".to_string(),
            ..NetworkServicesConfig::default()
        };

        let err = network_services
            .normalize(&guest_network)
            .expect_err("proxy mode should require envoy/coredns binaries");
        assert!(format!("{err:#}").contains("__missing_envoy_for_proxy_mode_test__"));
    }
}
