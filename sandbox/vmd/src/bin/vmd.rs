// @dive-file: Main daemon entrypoint and CLI override surface for runtime/distributed control settings.
// @dive-rel: Produces Config consumed by vmd/src/app.rs and downstream control-bus/registry subsystems.
// @dive-rel: Must stay aligned with vmd/src/config.rs defaults so CLI overrides preserve policy invariants.
use anyhow::Result;
use clap::{Parser, ValueEnum};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use vmd_rs::app;
use vmd_rs::config::{
    AuthConfig, Config, ContinuityTier, ControlBusConfig, NodeRegistryConfig, SecurityConfig,
    StorageProfile, TlsServerConfig,
};

#[derive(Parser, Debug)]
#[command(name = "vmd", about = "Bracket VM daemon (Rust)")]
struct Args {
    #[arg(long)]
    listen: Option<String>,
    #[arg(long)]
    data_dir: Option<String>,
    #[arg(long)]
    qemu_bin: Option<String>,
    #[arg(long)]
    qemu_arm64_bin: Option<String>,
    #[arg(long)]
    qemu_img: Option<String>,
    #[arg(long)]
    docker_bin: Option<String>,
    #[arg(long)]
    qemu_run_as_uid: Option<u32>,
    #[arg(long)]
    qemu_run_as_gid: Option<u32>,
    /// Root/operator-owned allowlist enabling dedicated PCI assignment.
    #[arg(long)]
    pci_policy: Option<String>,
    #[arg(long)]
    guest_dns_server: Option<String>,
    #[arg(long)]
    guest_http_proxy_guest_addr: Option<String>,
    #[arg(long)]
    guest_http_proxy_upstream_addr: Option<String>,
    #[arg(long)]
    envoy_bin: Option<String>,
    #[arg(long)]
    envoy_log_level: Option<String>,
    #[arg(long)]
    envoy_admin_addr: Option<String>,
    #[arg(long)]
    coredns_bin: Option<String>,
    #[arg(long)]
    coredns_bind_addr: Option<String>,
    #[arg(long)]
    log_level: Option<String>,
    /// Skip checking S3 for prebuilt VM images and always build locally
    #[arg(long)]
    force_local_build: bool,
    /// Admission limit for concurrently active VMs on this node.
    #[arg(long)]
    max_active_vms: Option<usize>,
    /// Storage profile for VM state durability semantics.
    #[arg(long, value_enum)]
    storage_profile: Option<StorageProfileArg>,
    /// Enable HA mode guardrails (requires durable-shared storage profile + registry + control bus).
    #[arg(long)]
    ha_mode: bool,
    /// Continuity tier contract for distributed operation.
    #[arg(long, value_enum)]
    continuity_tier: Option<ContinuityTierArg>,
    /// Explicit degraded mode (Tier-A) escape hatch for incident response.
    #[arg(long)]
    degraded_mode: bool,
    /// Freeze new admissions on this node (planned drain handoff).
    #[arg(long)]
    admission_frozen: bool,
    /// Enable node registry heartbeats by providing etcd endpoints (comma-separated).
    #[arg(long)]
    registry_etcd_endpoints: Option<String>,
    /// Override etcd key prefix used for node registry keys.
    #[arg(long)]
    registry_prefix: Option<String>,
    /// Override registry node identifier.
    #[arg(long)]
    node_id: Option<String>,
    /// Override advertised daemon endpoint for node routing.
    #[arg(long)]
    advertise_endpoint: Option<String>,
    /// Registry key lease TTL in seconds.
    #[arg(long)]
    registry_ttl_secs: Option<i64>,
    /// Failure-domain region label advertised for scheduler placement.
    #[arg(long)]
    region: Option<String>,
    /// Failure-domain zone label advertised for scheduler placement.
    #[arg(long)]
    zone: Option<String>,
    /// Failure-domain rack label advertised for scheduler placement.
    #[arg(long)]
    rack: Option<String>,
    /// Disable node registry heartbeats even when env vars are present.
    #[arg(long)]
    disable_node_registry: bool,
    /// Enable control command consumer and override NATS URL.
    #[arg(long)]
    control_nats_url: Option<String>,
    /// Override control bus subject prefix.
    #[arg(long)]
    control_subject_prefix: Option<String>,
    /// Override control bus node identifier.
    #[arg(long)]
    control_node_id: Option<String>,
    /// Disable control command consumer even when env vars are present.
    #[arg(long)]
    disable_control_bus: bool,
    /// Bearer token required for control-plane RPCs.
    #[arg(long)]
    auth_token: Option<String>,
    /// Path to file containing bearer token required for control-plane RPCs.
    #[arg(long)]
    auth_token_file: Option<String>,
    /// Optional read-only bearer token that may call read RPCs only.
    #[arg(long)]
    readonly_auth_token: Option<String>,
    /// Path to file containing optional read-only bearer token.
    #[arg(long)]
    readonly_auth_token_file: Option<String>,
    /// PEM server certificate for TLS listener.
    #[arg(long)]
    tls_cert: Option<String>,
    /// PEM private key for TLS listener.
    #[arg(long)]
    tls_key: Option<String>,
    /// PEM CA bundle used to validate mTLS client certificates.
    #[arg(long)]
    tls_client_ca: Option<String>,
    /// Disable strict client-certificate requirement when TLS is enabled.
    #[arg(long)]
    tls_allow_optional_client_cert: bool,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum StorageProfileArg {
    LocalEphemeral,
    DurableShared,
}

impl From<StorageProfileArg> for StorageProfile {
    fn from(value: StorageProfileArg) -> Self {
        match value {
            StorageProfileArg::LocalEphemeral => StorageProfile::LocalEphemeral,
            StorageProfileArg::DurableShared => StorageProfile::DurableShared,
        }
    }
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum ContinuityTierArg {
    TierA,
    TierB,
}

impl From<ContinuityTierArg> for ContinuityTier {
    fn from(value: ContinuityTierArg) -> Self {
        match value {
            ContinuityTierArg::TierA => ContinuityTier::TierA,
            ContinuityTierArg::TierB => ContinuityTier::TierB,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let mut cfg = Config::default();

    if let Some(listen) = args.listen {
        cfg.listen_address = listen;
    }
    if let Some(dir) = args.data_dir {
        cfg.data_dir = dir;
    }
    if let Some(qemu) = args.qemu_bin {
        cfg.qemu_bin = qemu;
    }
    if let Some(qemu) = args.qemu_arm64_bin {
        cfg.qemu_arm64_bin = qemu;
    }
    if let Some(qemu_img) = args.qemu_img {
        cfg.qemu_img_bin = qemu_img;
    }
    if let Some(docker) = args.docker_bin {
        cfg.docker_bin = docker;
    }
    if let Some(uid) = args.qemu_run_as_uid {
        cfg.qemu_process.run_as_uid = uid;
    }
    if let Some(gid) = args.qemu_run_as_gid {
        cfg.qemu_process.run_as_gid = gid;
    }
    if let Some(policy) = args.pci_policy {
        cfg.pci_policy_path = Some(policy);
    }
    if let Some(guest_dns_server) = args.guest_dns_server {
        cfg.guest_network.dns_server = Some(guest_dns_server);
    }
    if let Some(guest_addr) = args.guest_http_proxy_guest_addr {
        cfg.guest_network.http_proxy_guest_addr = Some(guest_addr);
    }
    if let Some(upstream_addr) = args.guest_http_proxy_upstream_addr {
        cfg.guest_network.http_proxy_upstream_addr = Some(upstream_addr);
    }
    if let Some(envoy_bin) = args.envoy_bin {
        cfg.network_services.envoy_bin = envoy_bin;
    }
    if let Some(envoy_log_level) = args.envoy_log_level {
        cfg.network_services.envoy_log_level = envoy_log_level;
    }
    if let Some(envoy_admin_addr) = args.envoy_admin_addr {
        cfg.network_services.envoy_admin_addr = envoy_admin_addr;
    }
    if let Some(coredns_bin) = args.coredns_bin {
        cfg.network_services.coredns_bin = coredns_bin;
    }
    if let Some(coredns_bind_addr) = args.coredns_bind_addr {
        cfg.network_services.coredns_bind_addr = coredns_bind_addr;
    }
    if let Some(level) = args.log_level {
        cfg.log_level = level;
    }
    if args.force_local_build {
        cfg.force_local_build = true;
    }
    if let Some(limit) = args.max_active_vms {
        cfg.max_active_vms = if limit == 0 { None } else { Some(limit) };
        if let Some(registry) = cfg.node_registry.as_mut() {
            registry.max_active_vms = cfg.max_active_vms;
        }
    }
    if let Some(storage_profile) = args.storage_profile {
        cfg.storage_profile = storage_profile.into();
        if let Some(registry) = cfg.node_registry.as_mut() {
            registry.storage_profile = cfg.storage_profile;
        }
    }
    if args.ha_mode {
        cfg.ha_mode = true;
    }
    if let Some(continuity_tier) = args.continuity_tier {
        if let Some(registry) = cfg.node_registry.as_mut() {
            registry.continuity_tier = continuity_tier.into();
        }
    }
    if args.degraded_mode {
        if let Some(registry) = cfg.node_registry.as_mut() {
            registry.degraded_mode = true;
        }
    }
    if args.admission_frozen {
        if let Some(registry) = cfg.node_registry.as_mut() {
            registry.admission_frozen = true;
        }
    }
    if args.disable_node_registry {
        cfg.node_registry = None;
    }
    if args.disable_control_bus {
        cfg.control_bus = None;
    }

    let has_registry_overrides = !args.disable_node_registry
        && (args.registry_etcd_endpoints.is_some()
            || args.registry_prefix.is_some()
            || args.node_id.is_some()
            || args.advertise_endpoint.is_some()
            || args.registry_ttl_secs.is_some()
            || args.region.is_some()
            || args.zone.is_some()
            || args.rack.is_some()
            || args.continuity_tier.is_some()
            || args.degraded_mode
            || args.admission_frozen);
    if has_registry_overrides {
        let mut registry = cfg
            .node_registry
            .clone()
            .unwrap_or_else(|| NodeRegistryConfig::defaults_for_listen(&cfg.listen_address));
        if let Some(endpoints) = args.registry_etcd_endpoints {
            registry.etcd_endpoints = endpoints
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(ToOwned::to_owned)
                .collect();
        }
        if let Some(prefix) = args.registry_prefix {
            registry.key_prefix = prefix;
        }
        if let Some(node_id) = args.node_id {
            registry.node_id = node_id;
        }
        if let Some(endpoint) = args.advertise_endpoint {
            registry.advertise_endpoint = endpoint;
        }
        if let Some(ttl_secs) = args.registry_ttl_secs {
            registry.ttl_secs = ttl_secs;
        }
        if let Some(region) = args.region {
            registry.region = region;
        }
        if let Some(zone) = args.zone {
            registry.zone = zone;
        }
        if let Some(rack) = args.rack {
            registry.rack = rack;
        }
        if let Some(continuity_tier) = args.continuity_tier {
            registry.continuity_tier = continuity_tier.into();
        }
        if args.degraded_mode {
            registry.degraded_mode = true;
        }
        if args.admission_frozen {
            registry.admission_frozen = true;
        }
        cfg.node_registry = Some(registry);
    }

    let has_control_overrides = !args.disable_control_bus
        && (args.control_nats_url.is_some()
            || args.control_subject_prefix.is_some()
            || args.control_node_id.is_some());
    if has_control_overrides {
        let mut control = cfg.control_bus.clone().unwrap_or(ControlBusConfig {
            nats_url: "nats://127.0.0.1:4222".to_string(),
            nats_auth_token: None,
            subject_prefix: "chevalier.sandbox.control".to_string(),
            cluster_id: "chevalier-sandbox-cluster".to_string(),
            node_id: "vmd-node".to_string(),
            dedupe_etcd_endpoints: Vec::new(),
            dedupe_prefix: "/sandbox/command-dedupe".to_string(),
            stream_name: "CHEVALIER_SANDBOX_CONTROL".to_string(),
            stream_max_age_secs: 60 * 60 * 24 * 7,
            stream_replicas: 1,
            command_consumer_durable: "vmd-node-cmd".to_string(),
            command_max_deliver: 5,
            command_ack_wait_ms: 30_000,
            max_inflight_commands: 1024,
            overload_retry_after_ms: 2_000,
            dead_letter_subject: "chevalier.sandbox.control.dlq.commands".to_string(),
            replay_subject: "chevalier.sandbox.control.replay.commands".to_string(),
        });
        if let Some(url) = args.control_nats_url {
            control.nats_url = url;
        }
        if let Some(prefix) = args.control_subject_prefix {
            control.subject_prefix = prefix;
        }
        if let Some(node_id) = args.control_node_id {
            control.node_id = node_id;
        }
        cfg.control_bus = Some(control);
    }

    let mut security = cfg.security.clone();
    let auth_token = resolve_secret(args.auth_token, args.auth_token_file.as_deref())?;
    let readonly_auth_token = resolve_secret(
        args.readonly_auth_token,
        args.readonly_auth_token_file.as_deref(),
    )?;
    if auth_token.is_some() || readonly_auth_token.is_some() {
        let mut auth = security.auth.take().unwrap_or(AuthConfig {
            admin_token: String::new(),
            readonly_token: None,
        });
        if let Some(token) = auth_token {
            auth.admin_token = token;
        }
        if readonly_auth_token.is_some() {
            auth.readonly_token = readonly_auth_token;
        }
        security.auth = Some(auth);
    }

    if args.tls_cert.is_some() || args.tls_key.is_some() || args.tls_client_ca.is_some() {
        let mut tls = security.tls.take().unwrap_or(TlsServerConfig {
            cert_path: String::new(),
            key_path: String::new(),
            client_ca_path: None,
            require_client_cert: true,
        });
        if let Some(cert) = args.tls_cert {
            tls.cert_path = cert;
        }
        if let Some(key) = args.tls_key {
            tls.key_path = key;
        }
        if args.tls_client_ca.is_some() {
            tls.client_ca_path = args.tls_client_ca;
        }
        if args.tls_allow_optional_client_cert {
            tls.require_client_cert = false;
        }
        security.tls = Some(tls);
    }
    cfg.security = SecurityConfig {
        auth: security.auth,
        tls: security.tls,
    };

    let _tracing_guard = init_tracing(&cfg.log_level)?;
    app::run_server(cfg).await
}

struct TracingGuard {
    tracer_provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.tracer_provider.take() {
            if let Err(error) = provider.shutdown() {
                eprintln!("failed shutting down vmd tracer provider: {error}");
            }
        }
    }
}

fn init_tracing(level: &str) -> Result<TracingGuard> {
    let filter = EnvFilter::try_new(level)
        .or_else(|_| EnvFilter::try_new(format!("vmd_rs={level}")))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let fmt_layer = fmt::layer().with_target(false).compact();
    let registry = tracing_subscriber::registry().with(filter).with(fmt_layer);

    if let Ok(endpoint) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        use opentelemetry::trace::TracerProvider as _;
        use opentelemetry_sdk::trace::SdkTracerProvider;

        let service_name = std::env::var("OTEL_SERVICE_NAME")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "chevalier-vmd".to_string());
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .build()
            .expect("failed to build OTLP span exporter");

        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(
                opentelemetry_sdk::Resource::builder()
                    .with_service_name(service_name.clone())
                    .build(),
            )
            .build();

        let tracer = provider.tracer(service_name.clone());
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        registry.with(otel_layer).init();
        tracing::info!(%endpoint, service_name = %service_name, "opentelemetry tracing enabled");
        return Ok(TracingGuard {
            tracer_provider: Some(provider),
        });
    }

    registry.init();

    Ok(TracingGuard {
        tracer_provider: None,
    })
}

fn resolve_secret(inline: Option<String>, file_path: Option<&str>) -> Result<Option<String>> {
    if let Some(value) = inline {
        let trimmed = value.trim().to_string();
        if !trimmed.is_empty() {
            return Ok(Some(trimmed));
        }
    }
    let Some(file_path) = file_path else {
        return Ok(None);
    };
    let value = std::fs::read_to_string(file_path)?;
    let trimmed = value.trim().to_string();
    if trimmed.is_empty() {
        return Ok(None);
    }
    Ok(Some(trimmed))
}
