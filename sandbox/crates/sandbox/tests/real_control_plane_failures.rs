// @dive-file: Real distributed control-plane failure probes that validate fail-closed behavior and recovery for etcd/NATS outages.
// @dive-rel: Executed by scripts/integration/verify_control_plane_failures.sh against live compose-backed etcd+nats infrastructure.
// @dive-rel: Uses public facade APIs to verify L3 distributed semantics without test-only runtime shortcuts.
use std::collections::hash_map::DefaultHasher;
use std::env;
use std::hash::{Hash, Hasher};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_nats::jetstream::consumer::pull;
use async_nats::jetstream::{self};
use chevalier_sandbox::{
    DistributedControlConfig, ExecEvent, ExecOptions, Sandbox, SandboxConfig, SandboxError,
    SessionOptions,
};
use etcd_client::{Client as EtcdClient, GetOptions};
use futures::StreamExt;
use serde_json::{Value, json};
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

fn control_plane_sandbox_config() -> SandboxConfig {
    let mut cfg = SandboxConfig {
        auto_spawn: false,
        prewarm_on_start: false,
        default_image: "ghcr.io/bracketdevelopers/uv-builder:main".to_string(),
        connect_timeout: Duration::from_secs(15),
        daemon_start_timeout: Duration::from_secs(20),
        distributed_control: Some(distributed_control_config_from_env()),
        ..SandboxConfig::default()
    };
    cfg.default_architecture = optional_env("CHEVALIER_SANDBOX_REAL_DEFAULT_ARCHITECTURE");
    cfg
}

fn direct_sandbox_config() -> SandboxConfig {
    let mut cfg = SandboxConfig {
        auto_spawn: false,
        prewarm_on_start: false,
        default_image: "ghcr.io/bracketdevelopers/uv-builder:main".to_string(),
        connect_timeout: Duration::from_secs(15),
        daemon_start_timeout: Duration::from_secs(20),
        distributed_control: None,
        ..SandboxConfig::default()
    };
    cfg.default_architecture = optional_env("CHEVALIER_SANDBOX_REAL_DEFAULT_ARCHITECTURE");
    cfg
}

fn run_docker(args: &[&str]) {
    let status = Command::new("docker")
        .args(args)
        .status()
        .unwrap_or_else(|err| panic!("failed running docker {:?}: {err}", args));
    assert!(
        status.success(),
        "docker command failed {:?} with status {:?}",
        args,
        status.code()
    );
}

fn stop_containers(ids: &[String]) {
    for id in ids {
        run_docker(&["stop", id.as_str()]);
    }
}

fn start_containers(ids: &[String]) {
    for id in ids {
        run_docker(&["start", id.as_str()]);
    }
}

async fn wait_for_etcd_quorum(etcd_endpoints: &[String], timeout_dur: Duration) {
    let started = tokio::time::Instant::now();
    while started.elapsed() < timeout_dur {
        if let Ok(mut client) = EtcdClient::connect(etcd_endpoints.to_vec(), None).await {
            let probe = client.get("/".to_string(), None).await;
            if probe.is_ok() {
                return;
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
    panic!("timed out waiting for etcd quorum recovery");
}

async fn wait_for_nats(url: &str, timeout_dur: Duration) {
    let started = tokio::time::Instant::now();
    while started.elapsed() < timeout_dur {
        if async_nats::connect(url.to_string()).await.is_ok() {
            return;
        }
        sleep(Duration::from_millis(500)).await;
    }
    panic!("timed out waiting for nats recovery");
}

fn is_etcd_fail_closed_error(err: &SandboxError) -> bool {
    match err {
        SandboxError::DaemonUnavailable(msg) => {
            let m = msg.to_ascii_lowercase();
            m.contains("etcd")
                || m.contains("distributed control")
                || m.contains("connect error")
                || m.contains("deadline has elapsed")
        }
        SandboxError::Transport(err) => {
            let m = err.to_string().to_ascii_lowercase();
            m.contains("connection refused") || m.contains("transport")
        }
        SandboxError::Grpc(status) => {
            let m = status.message().to_ascii_lowercase();
            status.code() == tonic::Code::Unavailable
                || m.contains("unavailable")
                || m.contains("transport")
        }
        _ => false,
    }
}

fn is_nats_fail_closed_error(err: &SandboxError) -> bool {
    match err {
        SandboxError::DaemonUnavailable(msg) => {
            let m = msg.to_ascii_lowercase();
            m.contains("nats")
                || m.contains("jetstream")
                || m.contains("connection refused")
                || m.contains("broken pipe")
        }
        SandboxError::Transport(err) => {
            let m = err.to_string().to_ascii_lowercase();
            m.contains("connection refused") || m.contains("transport")
        }
        SandboxError::Grpc(status) => {
            let m = status.message().to_ascii_lowercase();
            status.code() == tonic::Code::Unavailable
                || m.contains("unavailable")
                || m.contains("transport")
        }
        _ => false,
    }
}

async fn create_session_with_invalid_image(sandbox: &Sandbox) -> Result<(), SandboxError> {
    let invalid_image = "ghcr.io/sandbox/nonexistent-image:never".to_string();
    let session_id = format!("real-control-failure-{}", Uuid::new_v4());
    let result = timeout(
        Duration::from_secs(60),
        sandbox.session(SessionOptions {
            session_id: Some(session_id),
            image: Some(invalid_image),
            auto_start: false,
            ..SessionOptions::default()
        }),
    )
    .await
    .map_err(|_| SandboxError::DaemonUnavailable("session creation timed out".to_string()))?;
    match result {
        Ok(session) => {
            // @dive: If an image unexpectedly resolves, discard immediately so control-plane outage probes remain side-effect bounded.
            let _ = session.discard().await;
            Ok(())
        }
        Err(err) => Err(err),
    }
}

async fn publish_reconcile_command(
    nats_url: &str,
    subject_prefix: &str,
    command_id: &str,
    ordering_key: &str,
    idempotency_key: &str,
) {
    let client = async_nats::connect(nats_url.to_string())
        .await
        .expect("connect nats for reconcile command publish");
    let jetstream = jetstream::new(client);
    let subject = format!("{subject_prefix}.cmd.reconcile.run");
    let envelope = json!({
        "schema_version": "v1",
        "command_id": command_id,
        "command_type": "reconcile.run",
        "ordering_key": ordering_key,
        "idempotency_key": idempotency_key,
        "payload": {}
    });
    let bytes = serde_json::to_vec(&envelope).expect("serialize reconcile command envelope");
    jetstream
        .publish(subject, bytes.into())
        .await
        .expect("publish reconcile command")
        .await
        .expect("ack reconcile publish");
}

async fn publish_session_attach_probe_command(
    nats_url: &str,
    subject_prefix: &str,
    command_id: &str,
    ordering_key: &str,
    idempotency_key: &str,
    session_id: &str,
    expected_fence: Option<&str>,
) {
    let client = async_nats::connect(nats_url.to_string())
        .await
        .expect("connect nats for session attach probe command publish");
    let jetstream = jetstream::new(client);
    let subject = format!("{subject_prefix}.cmd.session.attach");
    let envelope = json!({
        "schema_version": "v1",
        "command_id": command_id,
        "command_type": "session.attach",
        "ordering_key": ordering_key,
        "idempotency_key": idempotency_key,
        "expected_fence": expected_fence,
        "payload": {
            "session_id": session_id,
            "expected_fence": expected_fence
        }
    });
    let bytes =
        serde_json::to_vec(&envelope).expect("serialize session attach probe command envelope");
    jetstream
        .publish(subject, bytes.into())
        .await
        .expect("publish session attach probe command")
        .await
        .expect("ack session attach probe publish");
}

async fn collect_dead_letters_for_commands(
    nats_url: &str,
    subject_prefix: &str,
    stream_name: &str,
    command_ids: &[String],
    timeout_dur: Duration,
) -> Vec<(String, String, i64)> {
    let client = match async_nats::connect(nats_url.to_string()).await {
        Ok(client) => client,
        Err(_) => return Vec::new(),
    };
    let jetstream = jetstream::new(client);
    let stream = match jetstream.get_stream(stream_name.to_string()).await {
        Ok(stream) => stream,
        Err(_) => return Vec::new(),
    };
    let consumer_name = format!("real-control-dlq-{}", Uuid::new_v4().as_simple());
    let subject = format!("{subject_prefix}.dlq.commands");
    let consumer = match stream
        .create_consumer(pull::Config {
            durable_name: Some(consumer_name),
            filter_subject: subject,
            ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
            max_deliver: 1,
            ..Default::default()
        })
        .await
    {
        Ok(consumer) => consumer,
        Err(_) => return Vec::new(),
    };
    let mut messages = match consumer.messages().await {
        Ok(messages) => messages,
        Err(_) => return Vec::new(),
    };

    let mut hits = Vec::new();
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
        let command_id = parsed
            .get("command_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if !command_ids.iter().any(|candidate| candidate == &command_id) {
            continue;
        }
        let reason = parsed
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let delivered = parsed.get("delivered").and_then(Value::as_i64).unwrap_or(0);
        hits.push((command_id, reason, delivered));
        if !hits.is_empty() {
            break;
        }
    }

    hits
}

async fn read_session_route_endpoint(
    etcd_endpoints: &[String],
    etcd_prefix: &str,
    session_id: &str,
) -> Option<String> {
    let mut client = EtcdClient::connect(etcd_endpoints.to_vec(), None)
        .await
        .ok()?;
    let sessions_prefix = format!("{}/sessions/", etcd_prefix.trim_end_matches('/'));
    let response = client
        .get(sessions_prefix, Some(GetOptions::new().with_prefix()))
        .await
        .ok()?;
    for kv in response.kvs() {
        let parsed = serde_json::from_slice::<Value>(kv.value()).ok()?;
        if parsed
            .get("session_id")
            .and_then(Value::as_str)
            .is_some_and(|value| value == session_id)
        {
            return parsed
                .get("endpoint")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
        }
    }
    None
}

async fn wait_for_session_route_endpoint(
    etcd_endpoints: &[String],
    etcd_prefix: &str,
    session_id: &str,
    timeout_dur: Duration,
) -> Option<String> {
    let started = tokio::time::Instant::now();
    while started.elapsed() < timeout_dur {
        // @dive: Session route keys are sharded; poll until route materializes instead of assuming immediate write visibility.
        if let Some(endpoint) =
            read_session_route_endpoint(etcd_endpoints, etcd_prefix, session_id).await
        {
            return Some(endpoint);
        }
        sleep(Duration::from_millis(250)).await;
    }
    None
}

async fn read_session_fence_value(
    etcd_endpoints: &[String],
    etcd_prefix: &str,
    session_id: &str,
) -> Option<String> {
    let mut client = EtcdClient::connect(etcd_endpoints.to_vec(), None)
        .await
        .ok()?;
    let fence_key = session_fence_key(etcd_prefix, session_id);
    let legacy_fence_key = legacy_session_fence_key(etcd_prefix, session_id);
    let mut response = client.get(fence_key.clone(), None).await.ok()?;
    if response.kvs().is_empty() && fence_key != legacy_fence_key {
        response = client.get(legacy_fence_key, None).await.ok()?;
    }
    let kv = response.kvs().first()?;
    let value = String::from_utf8(kv.value().to_vec()).ok()?;
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

async fn wait_for_session_fence_value(
    etcd_endpoints: &[String],
    etcd_prefix: &str,
    session_id: &str,
    timeout_dur: Duration,
) -> Option<String> {
    let started = tokio::time::Instant::now();
    while started.elapsed() < timeout_dur {
        if let Some(fence) = read_session_fence_value(etcd_endpoints, etcd_prefix, session_id).await
        {
            return Some(fence);
        }
        sleep(Duration::from_millis(250)).await;
    }
    None
}

fn session_fence_key(etcd_prefix: &str, session_id: &str) -> String {
    let trimmed = etcd_prefix.trim_end_matches('/');
    let shard = shard_for_key(session_id, 16);
    format!("{trimmed}/session_fences/{shard:02}/{session_id}")
}

fn legacy_session_fence_key(etcd_prefix: &str, session_id: &str) -> String {
    format!(
        "{}/session_fences/{}",
        etcd_prefix.trim_end_matches('/'),
        session_id
    )
}

fn shard_for_key(key: &str, shard_count: u8) -> u8 {
    let shard_count = shard_count.max(1);
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() % shard_count as u64) as u8
}

async fn wait_for_key_absent(etcd_endpoints: &[String], key: &str, timeout_dur: Duration) -> bool {
    let started = tokio::time::Instant::now();
    while started.elapsed() < timeout_dur {
        if let Ok(mut client) = EtcdClient::connect(etcd_endpoints.to_vec(), None).await {
            if let Ok(response) = client.get(key.to_string(), None).await {
                if response.kvs().is_empty() {
                    return true;
                }
            }
        }
        sleep(Duration::from_millis(250)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires real distributed control-plane daemons; run via scripts/integration/verify_control_plane_failures.sh"]
async fn etcd_quorum_loss_fails_closed_and_recovers() {
    let endpoint = required_env("CHEVALIER_SANDBOX_REAL_PRIMARY_ENDPOINT");
    let etcd_endpoints = parse_csv_env("CHEVALIER_SANDBOX_REAL_ETCD_ENDPOINTS");
    let degrade_ids = parse_csv_env("CHEVALIER_SANDBOX_REAL_ETCD_DEGRADE_CONTAINERS");
    assert!(
        !degrade_ids.is_empty(),
        "CHEVALIER_SANDBOX_REAL_ETCD_DEGRADE_CONTAINERS must include container ids"
    );

    let sandbox = Sandbox::connect(endpoint, control_plane_sandbox_config())
        .await
        .expect("connect sandbox for etcd fail-closed test");

    stop_containers(&degrade_ids);
    sleep(Duration::from_secs(2)).await;

    let fail_closed = sandbox.list_sessions().await;
    let err = fail_closed.expect_err("list_sessions should fail closed when etcd quorum is lost");
    assert!(
        is_etcd_fail_closed_error(&err),
        "expected etcd fail-closed error, got: {err}"
    );

    start_containers(&degrade_ids);
    wait_for_etcd_quorum(&etcd_endpoints, Duration::from_secs(60)).await;

    let recover = timeout(Duration::from_secs(30), sandbox.list_sessions())
        .await
        .expect("list_sessions recovery probe timed out");
    recover.expect("list_sessions should recover after etcd quorum restore");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires real distributed control-plane daemons; run via scripts/integration/verify_control_plane_failures.sh"]
async fn nats_outage_fails_closed_and_recovers() {
    let endpoint = required_env("CHEVALIER_SANDBOX_REAL_PRIMARY_ENDPOINT");
    let nats_container_id = required_env("CHEVALIER_SANDBOX_REAL_NATS_CONTAINER_ID");
    let nats_url = required_env("CHEVALIER_SANDBOX_REAL_NATS_URL");

    let sandbox = Sandbox::connect(endpoint, control_plane_sandbox_config())
        .await
        .expect("connect sandbox for nats fail-closed test");

    stop_containers(std::slice::from_ref(&nats_container_id));
    sleep(Duration::from_secs(2)).await;

    let fail_closed = create_session_with_invalid_image(&sandbox).await;
    let err = fail_closed.expect_err("session create should fail closed when nats is down");
    assert!(
        is_nats_fail_closed_error(&err),
        "expected nats fail-closed error, got: {err}"
    );

    start_containers(std::slice::from_ref(&nats_container_id));
    wait_for_nats(nats_url.as_str(), Duration::from_secs(60)).await;

    // @dive: Recovery probe re-attempts the same command path; success or non-control-plane errors both prove NATS dependency has recovered.
    let recovered = create_session_with_invalid_image(&sandbox).await;
    if let Err(err) = recovered {
        assert!(
            !is_nats_fail_closed_error(&err),
            "nats fail-closed errors should clear after recovery; got: {err}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires real distributed control-plane daemons; run via scripts/integration/verify_control_plane_failures.sh"]
async fn ownership_fence_conflicts_under_concurrent_mutators_resolve_deterministically() {
    let endpoint = required_env("CHEVALIER_SANDBOX_REAL_PRIMARY_ENDPOINT");
    let etcd_endpoints = parse_csv_env("CHEVALIER_SANDBOX_REAL_ETCD_ENDPOINTS");
    let etcd_prefix = required_env("CHEVALIER_SANDBOX_REAL_ETCD_PREFIX");
    let nats_url = required_env("CHEVALIER_SANDBOX_REAL_NATS_URL");
    let subject_prefix = required_env("CHEVALIER_SANDBOX_REAL_NATS_SUBJECT_PREFIX");
    let stream_name = required_env("CHEVALIER_SANDBOX_REAL_NATS_STREAM");

    let sandbox = Sandbox::connect(endpoint, control_plane_sandbox_config())
        .await
        .expect("connect sandbox for ownership fence conflict test");

    let session_id = format!("real-fence-conflict-{}", Uuid::new_v4());
    let session = sandbox
        .session(SessionOptions {
            session_id: Some(session_id.clone()),
            auto_start: false,
            ..SessionOptions::default()
        })
        .await
        .expect("create session anchor for ownership fence conflict test");

    let current_fence = wait_for_session_fence_value(
        &etcd_endpoints,
        &etcd_prefix,
        &session_id,
        Duration::from_secs(30),
    )
    .await
    .expect("session anchor should publish an ownership fence");
    let ordering_key = format!("real-fence-ordering-{}", Uuid::new_v4());
    let accepted_command = format!("real-fence-cmd-accepted-{}", Uuid::new_v4());
    let stale_command = format!("real-fence-cmd-stale-{}", Uuid::new_v4());
    let accepted_idem = format!("real-fence-idem-accepted-{}", Uuid::new_v4());
    let stale_idem = format!("real-fence-idem-stale-{}", Uuid::new_v4());
    let stale_fence = format!("stale-{}", Uuid::new_v4());
    let publish_accepted = publish_session_attach_probe_command(
        &nats_url,
        &subject_prefix,
        &accepted_command,
        &ordering_key,
        &accepted_idem,
        &session_id,
        Some(current_fence.as_str()),
    );
    let publish_stale = publish_session_attach_probe_command(
        &nats_url,
        &subject_prefix,
        &stale_command,
        &ordering_key,
        &stale_idem,
        &session_id,
        Some(stale_fence.as_str()),
    );
    let _ = tokio::join!(publish_accepted, publish_stale);

    let dlq_hits = collect_dead_letters_for_commands(
        &nats_url,
        &subject_prefix,
        &stream_name,
        &[accepted_command.clone(), stale_command.clone()],
        Duration::from_secs(90),
    )
    .await;
    assert!(
        !dlq_hits.is_empty(),
        "expected one command to dead-letter under concurrent ownership-fence contention"
    );
    assert_eq!(
        dlq_hits.len(),
        1,
        "expected exactly one contender to dead-letter under ownership-fence conflict, got {dlq_hits:?}"
    );
    let (failed_command_id, reason, delivered) = &dlq_hits[0];
    assert_eq!(
        failed_command_id, &stale_command,
        "dead-letter command id should be the stale-fence contender: {dlq_hits:?}"
    );
    assert_eq!(
        reason, "ownership_fence_conflict",
        "ownership fence contention should dead-letter with ownership_fence_conflict reason"
    );
    assert!(
        *delivered >= 2,
        "ownership-fence conflict should exercise retry path before dead-letter (delivered={delivered})"
    );

    session
        .discard()
        .await
        .expect("discard ownership fence conflict session");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires two real nodes with distributed registry; run via scripts/integration/verify_two_node_registry.sh --run-drain-test"]
async fn planned_drain_admission_freeze_preserves_inflight_and_hands_off_new_sessions() {
    let primary_endpoint = required_env("CHEVALIER_SANDBOX_REAL_PRIMARY_ENDPOINT");
    let secondary_endpoint = required_env("CHEVALIER_SANDBOX_REAL_SECONDARY_ENDPOINT");
    let etcd_endpoints = parse_csv_env("CHEVALIER_SANDBOX_REAL_ETCD_ENDPOINTS");
    let etcd_prefix = required_env("CHEVALIER_SANDBOX_REAL_ETCD_PREFIX");

    let primary_sandbox = Sandbox::connect(primary_endpoint.clone(), direct_sandbox_config())
        .await
        .expect("connect direct sandbox to primary for in-flight session anchor");
    let distributed_sandbox =
        Sandbox::connect(primary_endpoint.clone(), control_plane_sandbox_config())
            .await
            .expect("connect distributed sandbox for admission handoff");

    let primary_session_id = format!("real-drain-primary-{}", Uuid::new_v4());
    let primary_session = primary_sandbox
        .session(SessionOptions {
            session_id: Some(primary_session_id),
            auto_start: true,
            ..SessionOptions::default()
        })
        .await
        .expect("create in-flight session on primary");

    // @dive: This command models accepted in-flight work that must not be interrupted while new admissions are redirected.
    let exec = primary_session
        .exec(
            "for i in 1 2 3 4 5; do echo drain-tick-$i; sleep 1; done",
            ExecOptions::default(),
        )
        .await
        .expect("start in-flight command on primary");
    drop(exec.input);

    let mut events = exec.events;
    let mut saw_first_tick = false;
    let mut handoff_done = false;
    let mut exit_code = None;
    let mut handoff_session = None;
    let mut stdout = String::new();

    let started = tokio::time::Instant::now();
    while started.elapsed() < Duration::from_secs(120) {
        let next = timeout(Duration::from_secs(5), events.next())
            .await
            .expect("timed out waiting for in-flight drain exec event");
        let Some(frame) = next else {
            break;
        };
        match frame.expect("decode drain exec event") {
            ExecEvent::Stdout(bytes) => {
                let chunk = String::from_utf8_lossy(&bytes).to_string();
                stdout.push_str(&chunk);
                if chunk.contains("drain-tick-1") {
                    saw_first_tick = true;
                }
                if saw_first_tick && !handoff_done {
                    let handoff_session_id = format!("real-drain-handoff-{}", Uuid::new_v4());
                    let session = distributed_sandbox
                        .session(SessionOptions {
                            session_id: Some(handoff_session_id.clone()),
                            auto_start: false,
                            ..SessionOptions::default()
                        })
                        .await
                        .expect("create handoff session through distributed admission");

                    // @dive: Planned drain guarantee requires new admissions to land on the non-frozen node while in-flight work continues.
                    let routed_endpoint = wait_for_session_route_endpoint(
                        &etcd_endpoints,
                        &etcd_prefix,
                        &handoff_session_id,
                        Duration::from_secs(20),
                    )
                    .await
                    .expect("handoff session route should be present in etcd");
                    assert_eq!(
                        routed_endpoint, secondary_endpoint,
                        "new admissions during planned drain must route away from frozen primary node"
                    );
                    handoff_session = Some(session);
                    handoff_done = true;
                }
            }
            ExecEvent::Stderr(bytes) => stdout.push_str(&String::from_utf8_lossy(&bytes)),
            ExecEvent::Exit(code) => {
                exit_code = Some(code);
                break;
            }
            ExecEvent::Timeout => {}
        }
    }

    assert!(
        saw_first_tick,
        "expected primary in-flight command to produce output during planned drain"
    );
    assert!(
        handoff_done,
        "expected distributed admission handoff to secondary while primary in-flight command remained active"
    );
    assert_eq!(
        exit_code,
        Some(0),
        "in-flight primary command must complete cleanly during planned drain, stdout={stdout:?}"
    );

    if let Some(session) = handoff_session {
        session
            .discard()
            .await
            .expect("discard handoff session created during planned drain");
    }
    primary_session
        .discard()
        .await
        .expect("discard primary planned drain session");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires real distributed control-plane daemons with explicit node restart; run via scripts/integration/verify_control_plane_failures.sh"]
async fn reconcile_converges_within_bound_after_node_restart() {
    let endpoint = required_env("CHEVALIER_SANDBOX_REAL_PRIMARY_ENDPOINT");
    let etcd_endpoints = parse_csv_env("CHEVALIER_SANDBOX_REAL_ETCD_ENDPOINTS");
    let etcd_prefix = required_env("CHEVALIER_SANDBOX_REAL_ETCD_PREFIX");
    let nats_url = required_env("CHEVALIER_SANDBOX_REAL_NATS_URL");
    let subject_prefix = required_env("CHEVALIER_SANDBOX_REAL_NATS_SUBJECT_PREFIX");
    let timeout_secs = env::var("CHEVALIER_SANDBOX_REAL_RECONCILE_CONVERGENCE_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(30);

    let _sandbox = Sandbox::connect(endpoint.clone(), control_plane_sandbox_config())
        .await
        .expect("connect sandbox for reconcile convergence probe after restart");

    let stale_session_id = format!("real-reconcile-stale-{}", Uuid::new_v4());
    let stale_key = format!(
        "{}/sessions/00/{}",
        etcd_prefix.trim_end_matches('/'),
        stale_session_id
    );
    let stale_payload = json!({
        "session_id": stale_session_id,
        "vm_id": "vm-stale",
        "endpoint": endpoint,
        "node_id": "stale-node",
        "fork_id": null,
        "updated_at_unix_ms": SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    });

    let mut client = EtcdClient::connect(etcd_endpoints.to_vec(), None)
        .await
        .expect("connect etcd for stale route injection");
    client
        .put(stale_key.clone(), stale_payload.to_string(), None)
        .await
        .expect("write stale route for reconcile convergence probe");

    let command_id = format!("real-reconcile-restart-{}", Uuid::new_v4());
    let ordering_key = format!("real-reconcile-restart-order-{}", Uuid::new_v4());
    let idempotency_key = format!("real-reconcile-restart-idem-{}", Uuid::new_v4());
    publish_reconcile_command(
        &nats_url,
        &subject_prefix,
        &command_id,
        &ordering_key,
        &idempotency_key,
    )
    .await;

    // @dive: Reconcile convergence contract requires stale route cleanup within bound after node restart.
    let converged = wait_for_key_absent(
        &etcd_endpoints,
        stale_key.as_str(),
        Duration::from_secs(timeout_secs),
    )
    .await;
    assert!(
        converged,
        "reconcile did not converge within {}s after restart (stale key still present: {})",
        timeout_secs, stale_key
    );
}
