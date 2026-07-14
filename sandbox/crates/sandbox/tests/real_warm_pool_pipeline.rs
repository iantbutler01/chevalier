// @dive-file: Real warm-pool and cold-start integration tests across distributed and local auto-spawn deployment levels.
// @dive-rel: Executed by scripts/integration/verify_real_warm_pool.sh with live etcd+nats+vmd services and local auto-spawn daemon binary.
// @dive-rel: Covers CHEVALIER_SANDBOX_REAL_INTEGRATION_TEST_PLAN.md section 7.4 checklist items.
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use async_nats::jetstream::consumer::pull;
use async_nats::jetstream::{self};
use chevalier_sandbox::proto::vmd::v1::vmd_service_client::VmdServiceClient;
use chevalier_sandbox::proto::vmd::v1::{
    CreateVmRequest, DeleteVmRequest, Metadata, ResourceSpec, VmSource, VmSourceType,
};
use chevalier_sandbox::{
    DistributedControlConfig, Sandbox, SandboxConfig, SessionOptions, WarmPoolProfile,
};
use futures::StreamExt;
use serde_json::Value;
use tokio::time::{sleep, timeout};
use uuid::Uuid;

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("missing required env var: {name}"))
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn parse_csv_env(name: &str) -> Vec<String> {
    optional_env(name)
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn normalize_architecture_label(raw: &str) -> String {
    let lowered = raw.trim().to_ascii_lowercase();
    match lowered.as_str() {
        "x86_64" | "amd64" => "amd64".to_string(),
        "aarch64" | "arm64" => "arm64".to_string(),
        other => other.to_string(),
    }
}

fn sanitize_image_reference(reference: &str) -> String {
    reference
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '.' || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn base_image_file_name(reference: &str, arch: &str) -> String {
    format!("{}-{}.qcow2", sanitize_image_reference(reference), arch)
}

fn warm_pool_image() -> String {
    optional_env("CHEVALIER_SANDBOX_REAL_WARM_POOL_IMAGE").unwrap_or_else(|| {
        env::var("BRACKET_VM_IMAGE")
            .unwrap_or_else(|_| "ghcr.io/bracketdevelopers/uv-builder:main".to_string())
    })
}

fn warm_pool_architecture() -> String {
    optional_env("CHEVALIER_SANDBOX_REAL_WARM_POOL_ARCH")
        .map(|value| normalize_architecture_label(value.as_str()))
        .unwrap_or_else(|| {
            normalize_architecture_label(std::env::consts::ARCH)
                .trim()
                .to_string()
        })
}

fn ensure_seeded_base_image(data_dir: &Path, image: &str, arch: &str) {
    let base_dir = data_dir.join("base_images");
    fs::create_dir_all(&base_dir).expect("create base_images directory");
    let target = base_dir.join(base_image_file_name(image, arch));
    if target.exists() {
        fs::remove_file(&target).expect("remove stale seeded base image");
    }
    let qemu_img = optional_env("CHEVALIER_SANDBOX_REAL_QEMU_IMG_BIN")
        .unwrap_or_else(|| "qemu-img".to_string());
    let status = Command::new(qemu_img)
        .args([
            "create",
            "-f",
            "qcow2",
            target.to_string_lossy().as_ref(),
            "64M",
        ])
        .status()
        .expect("invoke qemu-img create for seeded base image");
    assert!(
        status.success(),
        "qemu-img create failed for seeded base image: {}",
        target.display()
    );
}

fn distributed_control_config_from_env() -> DistributedControlConfig {
    let mut cfg = DistributedControlConfig::default();
    let etcd_endpoints = parse_csv_env("CHEVALIER_SANDBOX_REAL_ETCD_ENDPOINTS");
    if !etcd_endpoints.is_empty() {
        cfg.etcd_endpoints = etcd_endpoints;
    }
    if let Some(prefix) = optional_env("CHEVALIER_SANDBOX_REAL_ETCD_PREFIX") {
        cfg.etcd_prefix = prefix;
    }
    if let Some(nats_url) = optional_env("CHEVALIER_SANDBOX_REAL_NATS_URL") {
        cfg.nats_url = nats_url;
    }
    if let Some(prefix) = optional_env("CHEVALIER_SANDBOX_REAL_NATS_SUBJECT_PREFIX") {
        cfg.nats_subject_prefix = prefix.clone();
        cfg.nats_dead_letter_subject = format!("{prefix}.dlq.commands");
    }
    if let Some(stream_name) = optional_env("CHEVALIER_SANDBOX_REAL_NATS_STREAM") {
        cfg.nats_stream_name = stream_name;
    }
    cfg
}

fn distributed_warm_pool_config(
    prewarm_on_start: bool,
    image: &str,
    architecture: &str,
) -> SandboxConfig {
    SandboxConfig {
        auto_spawn: false,
        prewarm_on_start,
        default_image: image.to_string(),
        connect_timeout: Duration::from_secs(15),
        daemon_start_timeout: Duration::from_secs(20),
        warm_pool_profiles: vec![WarmPoolProfile {
            image: image.to_string(),
            architecture: Some(architecture.to_string()),
            min_inventory: 1,
        }],
        distributed_control: Some(distributed_control_config_from_env()),
        ..SandboxConfig::default()
    }
}

fn route_data_dir() -> PathBuf {
    PathBuf::from(required_env("CHEVALIER_SANDBOX_REAL_WARM_POOL_DATA_DIR"))
}

async fn wait_for_session_create_payload(
    nats_url: &str,
    subject_prefix: &str,
    stream_name: &str,
    session_id: &str,
    timeout_dur: Duration,
) -> Option<Value> {
    let client = async_nats::connect(nats_url.to_string()).await.ok()?;
    let jetstream = jetstream::new(client);
    let stream = jetstream.get_stream(stream_name.to_string()).await.ok()?;
    let consumer_name = format!("real-warm-pool-cmd-{}", Uuid::new_v4().as_simple());
    let subject = format!("{subject_prefix}.cmd.session.create");
    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some(consumer_name),
            filter_subject: subject,
            ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
            max_deliver: 1,
            ..Default::default()
        })
        .await
        .ok()?;

    let mut messages = consumer.messages().await.ok()?;
    let started = tokio::time::Instant::now();
    while started.elapsed() < timeout_dur {
        let next = timeout(Duration::from_secs(1), messages.next()).await;
        let Some(Ok(message)) = (match next {
            Ok(value) => value,
            Err(_) => continue,
        }) else {
            continue;
        };
        let parsed = serde_json::from_slice::<Value>(&message.payload).ok();
        let _ = message.ack().await;
        let Some(parsed) = parsed else {
            continue;
        };
        let payload = parsed.get("payload").unwrap_or(&parsed);
        if payload
            .get("session_id")
            .and_then(Value::as_str)
            .is_some_and(|value| value == session_id)
        {
            return Some(payload.clone());
        }
    }

    None
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires real distributed control-plane daemons; run via scripts/integration/verify_real_warm_pool.sh"]
async fn startup_prewarm_executes_before_first_session_and_reports_warm_pool_hit() {
    let endpoint = required_env("CHEVALIER_SANDBOX_REAL_PRIMARY_ENDPOINT");
    let nats_url = required_env("CHEVALIER_SANDBOX_REAL_NATS_URL");
    let subject_prefix = required_env("CHEVALIER_SANDBOX_REAL_NATS_SUBJECT_PREFIX");
    let stream_name = required_env("CHEVALIER_SANDBOX_REAL_NATS_STREAM");
    let image = warm_pool_image();
    let arch = warm_pool_architecture();
    ensure_seeded_base_image(route_data_dir().as_path(), image.as_str(), arch.as_str());

    let sandbox = Sandbox::connect(
        endpoint,
        distributed_warm_pool_config(true, image.as_str(), arch.as_str()),
    )
    .await
    .expect("connect sandbox with startup prewarm enabled");

    let session_id = format!("real-warm-pool-prewarm-{}", Uuid::new_v4());
    let session = sandbox
        .session(SessionOptions {
            session_id: Some(session_id.clone()),
            image: Some(image.clone()),
            architecture: Some(arch.clone()),
            auto_start: false,
            ..SessionOptions::default()
        })
        .await
        .expect("create first session after startup prewarm");

    let payload = wait_for_session_create_payload(
        nats_url.as_str(),
        subject_prefix.as_str(),
        stream_name.as_str(),
        session_id.as_str(),
        Duration::from_secs(20),
    )
    .await
    .expect("observe session.create command payload");
    assert!(
        payload
            .get("warm_pool_hit")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        "startup prewarm should mark first session create as warm_pool_hit"
    );

    session
        .discard()
        .await
        .expect("discard startup prewarm session");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires real vmd daemon endpoint with seeded base image; run via scripts/integration/verify_real_warm_pool.sh"]
async fn warm_pool_hit_path_avoids_image_download_and_conversion_on_request_path() {
    let endpoint = required_env("CHEVALIER_SANDBOX_REAL_PRIMARY_ENDPOINT");
    let image = warm_pool_image();
    let arch = warm_pool_architecture();
    ensure_seeded_base_image(route_data_dir().as_path(), image.as_str(), arch.as_str());

    let mut client = VmdServiceClient::connect(endpoint.clone())
        .await
        .expect("connect vmd for direct create-vm warm-pool probe");
    let mut stream = client
        .create_vm(tonic::Request::new(CreateVmRequest {
            name: format!("real-warm-direct-{}", Uuid::new_v4()),
            source: Some(VmSource {
                r#type: VmSourceType::Docker as i32,
                reference: image.clone(),
            }),
            resources: Some(ResourceSpec {
                vcpu: 1,
                memory_mb: 1024,
                disk_gb: 10,
            }),
            metadata: Some(Metadata {
                entries: std::collections::HashMap::new(),
            }),
            auto_start: false,
            architecture: arch.clone(),
            shared_mounts: Vec::new(),
            pci_device_ids: Vec::new(),
        }))
        .await
        .expect("start direct create_vm stream")
        .into_inner();

    let mut saw_cached_hint = false;
    let mut saw_download = false;
    let mut saw_convert = false;
    let mut vm_id = None::<String>;
    while let Some(update) = stream
        .message()
        .await
        .expect("receive create_vm stream update")
    {
        if let Some(event) = update.event {
            match event {
                chevalier_sandbox::proto::vmd::v1::create_vm_stream_response::Event::Progress(
                    progress,
                ) => {
                    let message = progress.message.to_ascii_lowercase();
                    if message.contains("cached base image") || message.contains("base image ready")
                    {
                        saw_cached_hint = true;
                    }
                    if message.contains("downloading") {
                        saw_download = true;
                    }
                    if message.contains("converting docker image") {
                        saw_convert = true;
                    }
                }
                chevalier_sandbox::proto::vmd::v1::create_vm_stream_response::Event::Vm(vm) => {
                    vm_id = Some(vm.id);
                }
            }
        }
    }

    assert!(saw_cached_hint, "expected cached-base-image progress hints");
    assert!(
        !saw_download,
        "warm-pool hit request path should not download image when cache is seeded"
    );
    assert!(
        !saw_convert,
        "warm-pool hit request path should not convert docker image when cache is seeded"
    );

    if let Some(vm_id) = vm_id {
        client
            .delete_vm(tonic::Request::new(DeleteVmRequest {
                vm_id,
                purge_snapshots: true,
            }))
            .await
            .expect("delete direct warm-pool probe vm");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires real distributed control-plane daemons; run via scripts/integration/verify_real_warm_pool.sh"]
async fn cold_hit_triggers_async_refill_and_emits_refill_evidence() {
    let endpoint = required_env("CHEVALIER_SANDBOX_REAL_PRIMARY_ENDPOINT");
    let nats_url = required_env("CHEVALIER_SANDBOX_REAL_NATS_URL");
    let subject_prefix = required_env("CHEVALIER_SANDBOX_REAL_NATS_SUBJECT_PREFIX");
    let stream_name = required_env("CHEVALIER_SANDBOX_REAL_NATS_STREAM");
    let image = warm_pool_image();
    let arch = warm_pool_architecture();
    ensure_seeded_base_image(route_data_dir().as_path(), image.as_str(), arch.as_str());

    let sandbox = Sandbox::connect(
        endpoint,
        distributed_warm_pool_config(false, image.as_str(), arch.as_str()),
    )
    .await
    .expect("connect sandbox with startup prewarm disabled");

    let first_session_id = format!("real-warm-pool-cold-{}", Uuid::new_v4());
    let first = sandbox
        .session(SessionOptions {
            session_id: Some(first_session_id.clone()),
            image: Some(image.clone()),
            architecture: Some(arch.clone()),
            auto_start: false,
            ..SessionOptions::default()
        })
        .await
        .expect("create first session on cold warm-pool key");
    let first_payload = wait_for_session_create_payload(
        nats_url.as_str(),
        subject_prefix.as_str(),
        stream_name.as_str(),
        first_session_id.as_str(),
        Duration::from_secs(20),
    )
    .await
    .expect("observe first session.create payload");
    assert!(
        !first_payload
            .get("warm_pool_hit")
            .and_then(Value::as_bool)
            .unwrap_or(true),
        "first cold session should report warm_pool_hit=false"
    );
    first
        .discard()
        .await
        .expect("discard first cold-hit session");

    sleep(Duration::from_secs(2)).await;

    let second_session_id = format!("real-warm-pool-refill-{}", Uuid::new_v4());
    let second = sandbox
        .session(SessionOptions {
            session_id: Some(second_session_id.clone()),
            image: Some(image.clone()),
            architecture: Some(arch.clone()),
            auto_start: false,
            ..SessionOptions::default()
        })
        .await
        .expect("create second session after async refill window");
    let second_payload = wait_for_session_create_payload(
        nats_url.as_str(),
        subject_prefix.as_str(),
        stream_name.as_str(),
        second_session_id.as_str(),
        Duration::from_secs(20),
    )
    .await
    .expect("observe second session.create payload");
    assert!(
        second_payload
            .get("warm_pool_hit")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        "second session should observe refill evidence via warm_pool_hit=true"
    );
    second
        .discard()
        .await
        .expect("discard refill-evidence session");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires local vmd binary path; run via scripts/integration/verify_real_warm_pool.sh"]
async fn local_auto_spawn_prewarm_avoids_first_command_transport_reset_spam() {
    let daemon_bin = PathBuf::from(required_env("CHEVALIER_SANDBOX_REAL_VMD_BIN"));
    let image = warm_pool_image();
    let arch = warm_pool_architecture();

    let temp_dir = tempfile::Builder::new()
        .prefix("rsbap")
        .tempdir_in("/tmp")
        .expect("create short temp dir for local auto-spawn warm-pool test");
    ensure_seeded_base_image(temp_dir.path(), image.as_str(), arch.as_str());

    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind random port for local auto-spawn probe");
    let port = listener.local_addr().expect("resolve bound port").port();
    drop(listener);

    let cfg = SandboxConfig {
        endpoint: format!("http://127.0.0.1:{port}"),
        daemon_listen: format!("127.0.0.1:{port}"),
        auto_spawn: true,
        prewarm_on_start: true,
        connect_timeout: Duration::from_secs(120),
        daemon_start_timeout: Duration::from_secs(90),
        portproxy_ready_timeout: Duration::from_secs(20),
        daemon_bin: Some(daemon_bin),
        daemon_data_dir: Some(temp_dir.path().to_path_buf()),
        default_image: image.clone(),
        warm_pool_profiles: vec![WarmPoolProfile {
            image: image.clone(),
            architecture: Some(arch.clone()),
            min_inventory: 1,
        }],
        ..SandboxConfig::default()
    };

    let sandbox = Sandbox::new(cfg)
        .await
        .expect("local auto-spawn sandbox should initialize with prewarm");
    let session = sandbox
        .session(SessionOptions {
            session_id: Some(format!("real-autospawn-warm-{}", Uuid::new_v4())),
            image: Some(image),
            architecture: Some(arch),
            auto_start: false,
            ..SessionOptions::default()
        })
        .await
        .expect("create local auto-spawn session");
    // @dive: This lane validates local auto-spawn transport readiness via immediate control-plane calls,
    // keeping guest runtime assumptions out of the warm-pool startup signal.
    let sessions = sandbox
        .list_sessions()
        .await
        .expect("first post-spawn control call should succeed without transport-reset churn");
    assert!(
        sessions
            .iter()
            .any(|candidate| candidate.session_id == session.session_id()),
        "list_sessions should include the newly created local auto-spawn session"
    );

    session
        .discard()
        .await
        .expect("discard local auto-spawn warm-pool session");
}
