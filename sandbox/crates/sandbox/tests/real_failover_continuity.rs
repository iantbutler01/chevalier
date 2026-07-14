// @dive-file: Real-machinery failover integration tests for continuity and exactly-once command acknowledgement under primary loss.
// @dive-rel: Executed by scripts/integration/verify_real_failover.sh against live vmd daemons and compose-provided control-plane services.
// @dive-rel: Uses public facade APIs only to validate locked failover behaviors without test-only runtime shortcuts.
use std::collections::{HashMap, HashSet};
use std::env;
use std::process::Command;
use std::time::Duration;

use async_nats::jetstream::consumer::pull;
use async_nats::jetstream::{self};
use chevalier_sandbox::{
    DistributedControlConfig, ExecEvent, ExecHandle, ExecOptions, Sandbox, SandboxConfig,
    SandboxError, Session, SessionOptions,
    proto::vmd::v1::{ListVMsRequest, vmd_service_client::VmdServiceClient},
};
use etcd_client::{Client as EtcdClient, GetOptions};
use futures::{Stream, StreamExt};
use serde_json::{Value, json};
use tokio::time::{sleep, timeout};
use uuid::Uuid;

const FAILOVER_ATTACH_RETRY_TIMEOUT: Duration = Duration::from_secs(180);

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("missing required env var: {name}"))
}

fn primary_pid() -> i32 {
    required_env("CHEVALIER_SANDBOX_REAL_PRIMARY_PID")
        .parse::<i32>()
        .expect("CHEVALIER_SANDBOX_REAL_PRIMARY_PID must be an integer")
}

fn kill_pid(pid: i32) {
    let status = Command::new("kill")
        .arg("-9")
        .arg(pid.to_string())
        .status()
        .expect("failed to invoke kill command");
    if status.success() {
        return;
    }
    let probe = Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .status()
        .expect("failed to invoke kill -0 probe");
    assert!(
        !probe.success(),
        "kill -9 failed but process {pid} still appears alive"
    );
}

fn failover_sandbox_config(secondary_endpoint: String) -> SandboxConfig {
    let mut cfg = SandboxConfig {
        auto_spawn: false,
        prewarm_on_start: false,
        default_image: "ghcr.io/bracketdevelopers/uv-builder:main".to_string(),
        connect_timeout: Duration::from_secs(30),
        daemon_start_timeout: Duration::from_secs(30),
        portproxy_ready_timeout: Duration::from_secs(180),
        control_gateway_endpoints: vec![secondary_endpoint],
        endpoint_overrides: parse_endpoint_overrides_env(
            "CHEVALIER_SANDBOX_REAL_ENDPOINT_OVERRIDES",
        ),
        auth_token: optional_env("CHEVALIER_SANDBOX_REAL_AUTH_TOKEN"),
        ..SandboxConfig::default()
    };
    cfg.default_architecture = optional_env("CHEVALIER_SANDBOX_REAL_DEFAULT_ARCHITECTURE");
    cfg
}

async fn collect_exec(
    mut events: impl Stream<Item = chevalier_sandbox::Result<ExecEvent>> + Unpin,
    total_timeout: Duration,
) -> (String, String, Option<i32>, bool) {
    let mut stdout = String::new();
    let mut stderr = String::new();
    let mut exit_code = None;
    let mut timed_out = false;
    let started = tokio::time::Instant::now();

    loop {
        if started.elapsed() > total_timeout {
            timed_out = true;
            break;
        }

        let next = timeout(Duration::from_secs(15), events.next())
            .await
            .expect("timed out waiting for exec event");
        let Some(frame) = next else {
            break;
        };
        match frame.expect("failed decoding exec event") {
            ExecEvent::Stdout(bytes) => stdout.push_str(&String::from_utf8_lossy(&bytes)),
            ExecEvent::Stderr(bytes) => stderr.push_str(&String::from_utf8_lossy(&bytes)),
            ExecEvent::Exit(code) => {
                exit_code = Some(code);
                break;
            }
            ExecEvent::Timeout => timed_out = true,
        }
    }

    (stdout, stderr, exit_code, timed_out)
}

fn is_transport_retryable(err: &SandboxError) -> bool {
    match err {
        SandboxError::Transport(transport_err) => {
            let msg = transport_err.to_string().to_ascii_lowercase();
            msg.contains("transport")
                || msg.contains("connection reset")
                || msg.contains("broken pipe")
                || msg.contains("connection closed")
                || msg.contains("canceled")
                || msg.contains("connection refused")
                || msg.contains("tcp connect error")
        }
        SandboxError::Grpc(status) => {
            let msg = status.message().to_ascii_lowercase();
            status.code() == tonic::Code::Unavailable
                || status.code() == tonic::Code::Unknown
                || status.code() == tonic::Code::Cancelled
                || msg.contains("transport error")
                || msg.contains("connection reset")
                || msg.contains("broken pipe")
                || msg.contains("connection closed")
                || msg.contains("canceled")
                || msg.contains("connection refused")
        }
        SandboxError::DaemonUnavailable(message) => {
            let msg = message.to_ascii_lowercase();
            msg.contains("transport")
                || msg.contains("connection reset")
                || msg.contains("broken pipe")
                || msg.contains("connection closed")
                || msg.contains("canceled")
                || msg.contains("connection refused")
                || msg.contains("did not become ready")
        }
        _ => false,
    }
}

async fn exec_with_transport_retry(
    session: &Session,
    command: &str,
    options: ExecOptions,
    retry_timeout: Duration,
) -> chevalier_sandbox::Result<ExecHandle> {
    let started = tokio::time::Instant::now();
    let mut attempts: u32 = 0;
    loop {
        attempts = attempts.saturating_add(1);
        eprintln!(
            "exec_with_transport_retry attempt={} elapsed={:?} cmd={}",
            attempts,
            started.elapsed(),
            command
        );
        match session.exec(command, options.clone()).await {
            Ok(handle) => return Ok(handle),
            Err(err) => {
                eprintln!(
                    "exec_with_transport_retry attempt={} failed elapsed={:?}: {}",
                    attempts,
                    started.elapsed(),
                    err
                );
                if is_transport_retryable(&err) && started.elapsed() < retry_timeout {
                    eprintln!(
                        "exec_with_transport_retry retrying after transport error (elapsed={:?}): {}",
                        started.elapsed(),
                        err
                    );
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                return Err(err);
            }
        }
    }
}

async fn attach_with_retry(
    sandbox: &Sandbox,
    session_id: &str,
    retry_timeout: Duration,
) -> chevalier_sandbox::Result<Session> {
    let started = tokio::time::Instant::now();
    // @dive: Real failover attach can include VM startup + guest RPC readiness on the secondary, so each
    // attempt needs a realistic timeout to avoid self-inflicted cancellation loops.
    let attach_attempt_timeout = attach_attempt_timeout();
    loop {
        let attempt =
            tokio::time::timeout(attach_attempt_timeout, sandbox.attach_session(session_id)).await;
        let attempt = match attempt {
            Ok(result) => result,
            Err(_) => {
                let err =
                    SandboxError::DaemonUnavailable("attach_session attempt timed out".to_string());
                if started.elapsed() < retry_timeout {
                    eprintln!(
                        "attach_with_retry retrying after attach timeout (elapsed={:?})",
                        started.elapsed()
                    );
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                return Err(err);
            }
        };
        match attempt {
            Ok(session) => return Ok(session),
            Err(err) => {
                let retryable = is_transport_retryable(&err)
                    || matches!(err, SandboxError::SessionNotFound(_))
                    || matches!(err, SandboxError::DaemonUnavailable(_));
                if retryable && started.elapsed() < retry_timeout {
                    eprintln!(
                        "attach_with_retry retrying (elapsed={:?}): {}",
                        started.elapsed(),
                        err
                    );
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                return Err(err);
            }
        }
    }
}

fn continuity_metadata() -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    // @dive: Real continuity tests pin to non-tier-b policy so rebind assertions do not depend on restore-snapshot marker setup.
    metadata.insert("tier_b_eligible".to_string(), "false".to_string());
    metadata
}

fn distributed_continuity_metadata() -> HashMap<String, String> {
    let mut metadata = continuity_metadata();
    metadata.insert("tier_b_eligible".to_string(), "true".to_string());
    metadata
}

fn attach_attempt_timeout() -> Duration {
    let secs = env::var("CHEVALIER_SANDBOX_REAL_ATTACH_ATTEMPT_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(120);
    Duration::from_secs(secs)
}

#[derive(Clone, Debug)]
struct SessionRouteSnapshot {
    endpoint: String,
    ownership_fence: Option<String>,
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

fn parse_endpoint_overrides_env(name: &str) -> HashMap<String, String> {
    optional_env(name)
        .map(|raw| {
            raw.split(';')
                .filter_map(|entry| {
                    let (from, to) = entry.split_once('=')?;
                    let from = from.trim();
                    let to = to.trim();
                    if from.is_empty() || to.is_empty() {
                        return None;
                    }
                    Some((from.to_string(), to.to_string()))
                })
                .collect::<HashMap<_, _>>()
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

fn distributed_failover_sandbox_config(secondary_endpoint: String) -> SandboxConfig {
    let mut cfg = failover_sandbox_config(secondary_endpoint);
    cfg.distributed_control = Some(distributed_control_config_from_env());
    cfg.default_architecture = optional_env("CHEVALIER_SANDBOX_REAL_DEFAULT_ARCHITECTURE");
    cfg
}

async fn read_session_route_snapshot(
    etcd_endpoints: &[String],
    etcd_prefix: &str,
    session_id: &str,
) -> Option<SessionRouteSnapshot> {
    let mut client = EtcdClient::connect(etcd_endpoints.to_vec(), None)
        .await
        .ok()?;
    let trimmed_prefix = etcd_prefix.trim_end_matches('/');
    let session_prefix = format!("{trimmed_prefix}/sessions/");
    let response = client
        .get(session_prefix, Some(GetOptions::new().with_prefix()))
        .await
        .ok()?;

    let mut endpoint = None;
    let mut ownership_fence = None;

    for kv in response.kvs() {
        let key = String::from_utf8_lossy(kv.key()).to_string();
        if !key.ends_with(&format!("/{session_id}")) && !key.ends_with(session_id) {
            continue;
        }
        let parsed = serde_json::from_slice::<Value>(kv.value()).ok()?;
        if parsed
            .get("session_id")
            .and_then(Value::as_str)
            .is_some_and(|value| value == session_id)
        {
            endpoint = parsed
                .get("endpoint")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            ownership_fence = parsed
                .get("ownership_fence")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            break;
        }
    }

    let endpoint = endpoint?;
    let fences_prefix = format!("{trimmed_prefix}/session_fences/");
    let fence_response = client
        .get(fences_prefix, Some(GetOptions::new().with_prefix()))
        .await
        .ok()?;
    for kv in fence_response.kvs() {
        let key = String::from_utf8_lossy(kv.key()).to_string();
        if !key.ends_with(&format!("/{session_id}")) && !key.ends_with(session_id) {
            continue;
        }
        if let Ok(value) = String::from_utf8(kv.value().to_vec()) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                ownership_fence = Some(trimmed.to_string());
                break;
            }
        }
    }

    Some(SessionRouteSnapshot {
        endpoint,
        ownership_fence,
    })
}

async fn wait_for_session_route_snapshot(
    etcd_endpoints: &[String],
    etcd_prefix: &str,
    session_id: &str,
    timeout_dur: Duration,
) -> Option<SessionRouteSnapshot> {
    let started = tokio::time::Instant::now();
    while started.elapsed() < timeout_dur {
        if let Some(snapshot) =
            read_session_route_snapshot(etcd_endpoints, etcd_prefix, session_id).await
        {
            return Some(snapshot);
        }
        sleep(Duration::from_millis(500)).await;
    }
    None
}

async fn wait_for_rebound_event(
    nats_url: &str,
    subject_prefix: &str,
    stream_name: &str,
    session_id: &str,
    timeout_dur: Duration,
) -> bool {
    let client = match async_nats::connect(nats_url.to_string()).await {
        Ok(client) => client,
        Err(_) => return false,
    };
    let jetstream = jetstream::new(client);
    let stream = match jetstream.get_stream(stream_name.to_string()).await {
        Ok(stream) => stream,
        Err(_) => return false,
    };
    let consumer_name = format!("real-failover-rebound-{}", Uuid::new_v4().as_simple());
    let subject = format!("{subject_prefix}.evt.session.rebound");
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
        Err(_) => return false,
    };

    let mut messages = match consumer.messages().await {
        Ok(messages) => messages,
        Err(_) => return false,
    };

    let started = tokio::time::Instant::now();
    while started.elapsed() < timeout_dur {
        let next = timeout(Duration::from_secs(1), messages.next()).await;
        let Some(Ok(message)) = (match next {
            Ok(value) => value,
            Err(_) => continue,
        }) else {
            continue;
        };

        let mut matched = false;
        if let Ok(parsed) = serde_json::from_slice::<Value>(&message.payload) {
            let payload_session = parsed
                .get("payload")
                .and_then(|value| value.get("session_id"))
                .and_then(Value::as_str);
            matched = payload_session.is_some_and(|value| value == session_id);
        }
        let _ = message.ack().await;
        if matched {
            return true;
        }
    }

    false
}

#[derive(Clone, Debug)]
struct CapturedExecStreamEvent {
    cluster_id: String,
    logical_stream_id: String,
    event_seq: u64,
    event_id: String,
    producer_epoch: u64,
    kind: String,
    session_id: String,
}

fn parse_exec_stream_event(raw: &[u8]) -> Option<CapturedExecStreamEvent> {
    let parsed = serde_json::from_slice::<Value>(raw).ok()?;
    let payload = parsed.get("payload").unwrap_or(&parsed);
    let session_id = payload
        .get("session_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?
        .to_string();
    let logical_stream_id = payload
        .get("logical_stream_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            payload
                .get("stream_id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_default()
        .to_string();
    let event_seq = payload
        .get("event_seq")
        .and_then(Value::as_u64)
        .or_else(|| payload.get("sequence").and_then(Value::as_u64))
        .unwrap_or(0);
    let event_id = payload
        .get("event_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let producer_epoch = payload
        .get("producer_epoch")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let cluster_id = payload
        .get("cluster_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let kind = payload
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    Some(CapturedExecStreamEvent {
        cluster_id,
        logical_stream_id,
        event_seq,
        event_id,
        producer_epoch,
        kind,
        session_id,
    })
}

async fn collect_exec_stream_events_for_session(
    nats_url: &str,
    subject_prefix: &str,
    stream_name: &str,
    session_id: &str,
    timeout_dur: Duration,
    min_events: usize,
) -> Vec<CapturedExecStreamEvent> {
    let client = match async_nats::connect(nats_url.to_string()).await {
        Ok(client) => client,
        Err(_) => return Vec::new(),
    };
    let jetstream = jetstream::new(client);
    let stream = match jetstream.get_stream(stream_name.to_string()).await {
        Ok(stream) => stream,
        Err(_) => return Vec::new(),
    };
    let consumer_name = format!("real-failover-stream-{}", Uuid::new_v4().as_simple());
    let subject = format!("{subject_prefix}.evt.exec.stream.>");
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
    let started = tokio::time::Instant::now();
    let mut events = Vec::new();
    let mut saw_terminal = false;

    while started.elapsed() < timeout_dur {
        let next = timeout(Duration::from_secs(1), messages.next()).await;
        let Some(Ok(message)) = (match next {
            Ok(value) => value,
            Err(_) => continue,
        }) else {
            continue;
        };

        if let Some(event) = parse_exec_stream_event(&message.payload) {
            if event.session_id == session_id {
                if matches!(event.kind.as_str(), "exit" | "timeout" | "error") {
                    saw_terminal = true;
                }
                events.push(event);
            }
        }
        let _ = message.ack().await;
        if saw_terminal && events.len() >= min_events {
            break;
        }
    }

    events
}

async fn wait_for_dead_letter_for_identity(
    nats_url: &str,
    subject_prefix: &str,
    stream_name: &str,
    command_id: &str,
    idempotency_key: &str,
    captured_after_unix_ms: Option<u64>,
    timeout_dur: Duration,
) -> Option<Value> {
    let client = async_nats::connect(nats_url.to_string()).await.ok()?;
    let jetstream = jetstream::new(client);
    let stream = jetstream.get_stream(stream_name.to_string()).await.ok()?;
    let consumer_name = format!("real-failover-dlq-{}", Uuid::new_v4().as_simple());
    let subject = format!("{subject_prefix}.dlq.commands");
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
        let msg_command_id = payload
            .get("command_id")
            .and_then(Value::as_str)
            .or_else(|| parsed.get("command_id").and_then(Value::as_str))
            .unwrap_or_default();
        let msg_idempotency_key = payload
            .get("idempotency_key")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let matches_command = !command_id.trim().is_empty() && msg_command_id == command_id;
        let matches_idempotency =
            !idempotency_key.trim().is_empty() && msg_idempotency_key == idempotency_key;
        let captured_at_unix_ms = parsed
            .get("captured_at_unix_ms")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let meets_cutoff = captured_after_unix_ms
            .map(|cutoff| captured_at_unix_ms > cutoff)
            .unwrap_or(true);
        if (matches_command || matches_idempotency) && meets_cutoff {
            return Some(parsed);
        }
    }

    None
}

async fn wait_for_vmd_ready(endpoint: &str, timeout_dur: Duration) -> bool {
    let started = tokio::time::Instant::now();
    while started.elapsed() < timeout_dur {
        let connect = timeout(
            Duration::from_secs(3),
            VmdServiceClient::connect(endpoint.to_string()),
        )
        .await;
        let Ok(Ok(mut client)) = connect else {
            sleep(Duration::from_millis(300)).await;
            continue;
        };
        let probe = timeout(
            Duration::from_secs(3),
            client.list_v_ms(tonic::Request::new(ListVMsRequest {
                include_snapshots: false,
            })),
        )
        .await;
        if matches!(probe, Ok(Ok(_))) {
            return true;
        }
        sleep(Duration::from_millis(300)).await;
    }
    false
}

async fn publish_invalid_exec_stream_input_command(
    nats_url: &str,
    subject_prefix: &str,
    command_id: &str,
    idempotency_key: &str,
    session_id: &str,
    vm_id: &str,
) {
    let client = async_nats::connect(nats_url.to_string())
        .await
        .expect("connect nats for invalid exec.stream.input publish");
    let jetstream = jetstream::new(client);
    let subject = format!("{subject_prefix}.cmd.exec.stream.input");
    let envelope = json!({
        "schema_version": "v1",
        "command_id": command_id,
        "command_type": "exec.stream.input",
        "ordering_key": "",
        "idempotency_key": idempotency_key,
        "payload": {
            "session_id": session_id,
            "vm_id": vm_id,
            "input_seq": 1,
            "input_kind": "stdin",
            "stream_id": ""
        }
    });
    let bytes =
        serde_json::to_vec(&envelope).expect("serialize invalid exec.stream.input envelope");
    jetstream
        .publish(subject, bytes.into())
        .await
        .expect("publish invalid exec.stream.input command")
        .await
        .expect("ack invalid exec.stream.input publish");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires real vmd daemons; run via scripts/integration/verify_real_failover.sh"]
async fn primary_node_loss_during_active_exec_stream_rebinds_session() {
    let primary_endpoint = required_env("CHEVALIER_SANDBOX_REAL_PRIMARY_ENDPOINT");
    let secondary_endpoint = required_env("CHEVALIER_SANDBOX_REAL_SECONDARY_ENDPOINT");
    let primary_pid = primary_pid();

    let sandbox = Sandbox::connect(
        primary_endpoint,
        failover_sandbox_config(secondary_endpoint.clone()),
    )
    .await
    .expect("connect sandbox against primary + secondary endpoints");

    let session_id = format!("real-continuity-{}", Uuid::new_v4());
    let session = sandbox
        .session(SessionOptions {
            session_id: Some(session_id.clone()),
            metadata: continuity_metadata(),
            auto_start: true,
            ..SessionOptions::default()
        })
        .await
        .expect("create real continuity session");

    let exec = exec_with_transport_retry(
        &session,
        "for i in 1 2 3 4 5; do echo tick-$i; sleep 1; done",
        ExecOptions::default(),
        Duration::from_secs(120),
    )
    .await
    .expect("start active command stream");
    drop(exec.input);

    let mut events = exec.events;
    let mut stdout = String::new();
    let mut killed_primary = false;
    let mut exit_code = None;
    let started = tokio::time::Instant::now();
    while started.elapsed() < Duration::from_secs(180) {
        let next = timeout(Duration::from_secs(5), events.next())
            .await
            .expect("timed out waiting for continuity exec event");
        let Some(frame) = next else {
            break;
        };
        match frame.expect("decode continuity exec event") {
            ExecEvent::Stdout(bytes) => {
                let chunk = String::from_utf8_lossy(&bytes).to_string();
                stdout.push_str(&chunk);
                if chunk.contains("tick-") && !killed_primary {
                    kill_pid(primary_pid);
                    killed_primary = true;
                }
            }
            ExecEvent::Stderr(bytes) => {
                let chunk = String::from_utf8_lossy(&bytes).to_string();
                stdout.push_str(&chunk);
            }
            ExecEvent::Exit(code) => {
                exit_code = Some(code);
                break;
            }
            ExecEvent::Timeout => {}
        }
    }

    assert!(
        killed_primary,
        "primary daemon should be killed while command stream is active"
    );
    assert!(
        stdout.contains("tick-"),
        "expected active command stream output, got: {stdout}"
    );
    assert_eq!(
        exit_code,
        Some(0),
        "active stream should complete after failover, stdout={stdout:?}"
    );
    // @dive: This selector focuses on in-flight stream continuity under primary daemon loss.
    // Additional post-failover command-path checks are covered by distributed selectors.
    if let Err(err) = session.discard().await {
        eprintln!("best-effort discard for continuity session failed: {err}");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires real vmd daemons; run via scripts/integration/verify_real_failover.sh"]
async fn inflight_exec_is_acknowledged_exactly_once_under_primary_loss() {
    let primary_endpoint = required_env("CHEVALIER_SANDBOX_REAL_PRIMARY_ENDPOINT");
    let secondary_endpoint = required_env("CHEVALIER_SANDBOX_REAL_SECONDARY_ENDPOINT");
    let primary_pid = primary_pid();

    let sandbox = Sandbox::connect(
        primary_endpoint,
        failover_sandbox_config(secondary_endpoint.clone()),
    )
    .await
    .expect("connect sandbox against primary + secondary endpoints");

    let session_id = format!("real-exactly-once-{}", Uuid::new_v4());
    let session = sandbox
        .session(SessionOptions {
            session_id: Some(session_id.clone()),
            metadata: continuity_metadata(),
            auto_start: true,
            ..SessionOptions::default()
        })
        .await
        .expect("create real exactly-once session");

    let command = "count_file=/tmp/chevalier_exactly_once_count; count=0; [ -f \"$count_file\" ] && count=$(cat \"$count_file\"); count=$((count+1)); echo \"$count\" > \"$count_file\"; echo started; sleep 3; cat \"$count_file\"";
    let exec = exec_with_transport_retry(
        &session,
        command,
        ExecOptions::default(),
        Duration::from_secs(120),
    )
    .await
    .expect("start in-flight exactly-once command");
    drop(exec.input);

    let mut events = exec.events;
    let mut stdout = String::new();
    let mut killed_primary = false;
    let mut exit_code = None;
    let started = tokio::time::Instant::now();
    while started.elapsed() < Duration::from_secs(180) {
        let next = timeout(Duration::from_secs(5), events.next())
            .await
            .expect("timed out waiting for exactly-once exec event");
        let Some(frame) = next else {
            break;
        };
        match frame.expect("decode exactly-once exec event") {
            ExecEvent::Stdout(bytes) => {
                let chunk = String::from_utf8_lossy(&bytes).to_string();
                stdout.push_str(&chunk);
                if chunk.contains("started") && !killed_primary {
                    kill_pid(primary_pid);
                    killed_primary = true;
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
        killed_primary,
        "primary daemon should be killed while in-flight command is active"
    );
    assert_eq!(
        exit_code,
        Some(0),
        "exactly-once command should complete successfully, stdout={stdout:?}"
    );
    assert!(
        stdout.contains("started"),
        "expected in-flight command progress marker before failover, stdout={stdout:?}"
    );
    let started_count = stdout.matches("started").count();
    assert_eq!(
        started_count, 1,
        "in-flight command should be acknowledged exactly once from client stream perspective (expected single start marker): stdout={stdout:?}"
    );

    if let Err(err) = session.discard().await {
        eprintln!("best-effort discard for exactly-once session failed: {err}");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires real distributed control-plane daemons; run via scripts/integration/verify_real_failover.sh --distributed"]
async fn distributed_failover_rebind_updates_route_and_emits_events() {
    let primary_endpoint = required_env("CHEVALIER_SANDBOX_REAL_PRIMARY_ENDPOINT");
    let secondary_endpoint = required_env("CHEVALIER_SANDBOX_REAL_SECONDARY_ENDPOINT");
    let primary_pid = primary_pid();

    let dist_cfg = distributed_control_config_from_env();
    let etcd_endpoints = dist_cfg.etcd_endpoints.clone();
    let etcd_prefix = dist_cfg.etcd_prefix.clone();
    let nats_url = dist_cfg.nats_url.clone();
    let nats_subject_prefix = dist_cfg.nats_subject_prefix.clone();
    let nats_stream_name = dist_cfg.nats_stream_name.clone();

    let sandbox = Sandbox::connect(
        primary_endpoint,
        distributed_failover_sandbox_config(secondary_endpoint.clone()),
    )
    .await
    .expect("connect distributed sandbox against primary + secondary endpoints");

    let session_id = format!("real-dist-failover-{}", Uuid::new_v4());
    let session = sandbox
        .session(SessionOptions {
            session_id: Some(session_id.clone()),
            metadata: distributed_continuity_metadata(),
            auto_start: true,
            ..SessionOptions::default()
        })
        .await
        .expect("create distributed failover session");

    let initial_route = wait_for_session_route_snapshot(
        &etcd_endpoints,
        &etcd_prefix,
        &session_id,
        Duration::from_secs(30),
    )
    .await
    .expect("initial distributed session route should exist in etcd");

    let exec = exec_with_transport_retry(
        &session,
        "count_file=/tmp/chevalier_dist_failover_count; count=0; [ -f \"$count_file\" ] && count=$(cat \"$count_file\"); count=$((count+1)); echo \"$count\" > \"$count_file\"; for i in 1 2 3 4 5; do echo dist-tick-$i; sleep 1; done",
        ExecOptions::default(),
        Duration::from_secs(120),
    )
    .await
    .expect("start distributed active command stream");
    drop(exec.input);

    let mut events = exec.events;
    let mut stdout = String::new();
    let mut killed_primary = false;
    let mut exit_code = None;
    let mut stream_error = None::<String>;
    let mut idle_strikes = 0u32;
    const MAX_IDLE_STRIKES: u32 = 12;
    let started = tokio::time::Instant::now();
    while started.elapsed() < Duration::from_secs(120) {
        let next = match timeout(Duration::from_secs(5), events.next()).await {
            Ok(value) => {
                idle_strikes = 0;
                value
            }
            Err(_) => {
                idle_strikes = idle_strikes.saturating_add(1);
                if idle_strikes >= MAX_IDLE_STRIKES {
                    panic!(
                        "timed out waiting for distributed continuity exec event after {idle_strikes} idle windows; stdout_so_far={stdout:?}"
                    );
                }
                continue;
            }
        };
        let Some(frame) = next else {
            break;
        };
        let event = match frame {
            Ok(event) => event,
            Err(err) => {
                stream_error = Some(err.to_string());
                break;
            }
        };
        match event {
            ExecEvent::Stdout(bytes) => {
                let chunk = String::from_utf8_lossy(&bytes).to_string();
                stdout.push_str(&chunk);
                if chunk.contains("dist-tick-") && !killed_primary {
                    kill_pid(primary_pid);
                    killed_primary = true;
                }
            }
            ExecEvent::Stderr(bytes) => {
                let chunk = String::from_utf8_lossy(&bytes).to_string();
                stdout.push_str(&chunk);
            }
            ExecEvent::Exit(code) => {
                exit_code = Some(code);
                break;
            }
            ExecEvent::Timeout => {}
        }
    }
    drop(events);

    assert!(
        killed_primary,
        "primary daemon should be killed while distributed stream is active"
    );
    assert!(
        stream_error.is_none(),
        "distributed stream should recover without terminal stream error: {stream_error:?}"
    );
    assert_eq!(
        exit_code,
        Some(0),
        "distributed active stream should complete after failover, stdout={stdout:?}"
    );

    let events = collect_exec_stream_events_for_session(
        &nats_url,
        &nats_subject_prefix,
        &nats_stream_name,
        &session_id,
        Duration::from_secs(20),
        5,
    )
    .await;
    assert!(
        !events.is_empty(),
        "expected distributed exec stream events for failover session {session_id}"
    );

    let primary_stream_id = events
        .first()
        .map(|event| event.logical_stream_id.clone())
        .unwrap_or_default();
    let primary_stream_events: Vec<&CapturedExecStreamEvent> = events
        .iter()
        .filter(|event| event.logical_stream_id == primary_stream_id)
        .collect();
    assert!(
        !primary_stream_events.is_empty(),
        "expected primary stream events for logical stream {primary_stream_id}"
    );

    let mut seen_event_ids = HashSet::new();
    for event in &primary_stream_events {
        assert!(
            !event.cluster_id.trim().is_empty(),
            "cluster_id must be present on distributed failover stream events: {event:?}"
        );
        assert!(
            !event.event_id.trim().is_empty(),
            "event_id must be present on distributed failover stream events: {event:?}"
        );
        assert!(
            seen_event_ids.insert(event.event_id.clone()),
            "event_id must be globally unique per emitted event: {}",
            event.event_id
        );
    }

    for window in primary_stream_events.windows(2) {
        let prev = window[0];
        let next = window[1];
        assert!(
            next.event_seq > prev.event_seq,
            "event_seq must be strictly increasing per logical stream (prev={}, next={})",
            prev.event_seq,
            next.event_seq
        );
    }

    let mut saw_epoch_change = false;
    for window in primary_stream_events.windows(2) {
        let prev = window[0];
        let next = window[1];
        if next.producer_epoch > prev.producer_epoch {
            saw_epoch_change = true;
            assert_eq!(
                next.event_seq,
                prev.event_seq + 1,
                "event_seq must continue from checkpoint + 1 across producer epoch change"
            );
        }
    }
    assert!(
        saw_epoch_change,
        "expected producer_epoch increase after distributed failover for stream {primary_stream_id}"
    );

    let rebound_route = wait_for_session_route_snapshot(
        &etcd_endpoints,
        &etcd_prefix,
        &session_id,
        Duration::from_secs(20),
    )
    .await
    .expect("rebound distributed session route should exist in etcd");
    assert_eq!(
        rebound_route.endpoint, secondary_endpoint,
        "session route endpoint should move to secondary after distributed failover"
    );
    assert_ne!(
        rebound_route.ownership_fence, initial_route.ownership_fence,
        "ownership fence should rotate after distributed failover rebind"
    );

    let saw_rebound_event = wait_for_rebound_event(
        &nats_url,
        &nats_subject_prefix,
        &nats_stream_name,
        &session_id,
        Duration::from_secs(20),
    )
    .await;
    assert!(
        saw_rebound_event,
        "expected distributed session rebound event in NATS stream for session {session_id}"
    );

    let secondary_sandbox = Sandbox::connect(
        secondary_endpoint.clone(),
        distributed_failover_sandbox_config(secondary_endpoint.clone()),
    )
    .await
    .expect("connect distributed sandbox directly to secondary endpoint");

    let rebound_session =
        attach_with_retry(&secondary_sandbox, &session_id, Duration::from_secs(60))
            .await
            .expect("attach distributed session on secondary after primary loss");
    let follow_up = exec_with_transport_retry(
        &rebound_session,
        "echo distributed-post-failover-ok",
        ExecOptions::default(),
        Duration::from_secs(120),
    )
    .await
    .expect("distributed follow-up command should execute after failover");
    drop(follow_up.input);
    let (stdout2, stderr2, exit2, timed_out2) =
        collect_exec(follow_up.events, Duration::from_secs(120)).await;
    assert!(!timed_out2, "distributed post-failover command timed out");
    assert_eq!(
        exit2,
        Some(0),
        "distributed post-failover command failed stdout={stdout2:?} stderr={stderr2:?}"
    );
    assert!(
        stdout2.contains("distributed-post-failover-ok"),
        "expected distributed post-failover sentinel output, got: {stdout2}"
    );

    let rerun_guard = exec_with_transport_retry(
        &rebound_session,
        "cat /tmp/chevalier_dist_failover_count",
        ExecOptions::default(),
        Duration::from_secs(120),
    )
    .await
    .expect("read distributed failover side-effect counter");
    drop(rerun_guard.input);
    let (counter_stdout, counter_stderr, counter_exit, counter_timed_out) =
        collect_exec(rerun_guard.events, Duration::from_secs(120)).await;
    assert!(
        !counter_timed_out,
        "distributed side-effect counter read timed out"
    );
    assert_eq!(
        counter_exit,
        Some(0),
        "distributed side-effect counter read failed stdout={counter_stdout:?} stderr={counter_stderr:?}"
    );
    assert_eq!(
        counter_stdout.trim(),
        "1",
        "distributed failover should not rerun the active command stream side effects"
    );

    rebound_session
        .discard()
        .await
        .expect("discard distributed failover session");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires real distributed control-plane daemons; run via scripts/integration/verify_real_failover.sh --distributed"]
async fn distributed_stream_resume_from_checkpoint_is_forward_only_without_replay() {
    let primary_endpoint = required_env("CHEVALIER_SANDBOX_REAL_PRIMARY_ENDPOINT");
    let secondary_endpoint = required_env("CHEVALIER_SANDBOX_REAL_SECONDARY_ENDPOINT");

    let sandbox = Sandbox::connect(
        primary_endpoint,
        distributed_failover_sandbox_config(secondary_endpoint),
    )
    .await
    .expect("connect distributed sandbox");

    let session_id = format!("real-stream-checkpoint-{}", Uuid::new_v4());
    let session = sandbox
        .session(SessionOptions {
            session_id: Some(session_id),
            metadata: distributed_continuity_metadata(),
            auto_start: true,
            ..SessionOptions::default()
        })
        .await
        .expect("create distributed stream checkpoint session");

    // @dive: Sleep gaps exceed the facade event-read timeout to force resume/re-subscribe behavior while command execution remains active.
    let command = "count_file=/tmp/chevalier_checkpoint_rebind_count; count=0; [ -f \"$count_file\" ] && count=$(cat \"$count_file\"); count=$((count+1)); echo \"$count\" > \"$count_file\"; for i in 1 2 3 4; do echo resume-$i; sleep 3; done";
    let exec = exec_with_transport_retry(
        &session,
        command,
        ExecOptions::default(),
        Duration::from_secs(180),
    )
    .await
    .expect("start distributed checkpointed stream command");
    drop(exec.input);
    let (stdout, stderr, exit, timed_out) =
        collect_exec(exec.events, Duration::from_secs(240)).await;
    assert!(!timed_out, "checkpoint resume command timed out");
    assert_eq!(
        exit,
        Some(0),
        "checkpoint resume command failed stdout={stdout:?} stderr={stderr:?}"
    );

    let resume_lines: Vec<String> = stdout
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("resume-"))
        .map(ToOwned::to_owned)
        .collect();
    assert_eq!(
        resume_lines,
        vec![
            "resume-1".to_string(),
            "resume-2".to_string(),
            "resume-3".to_string(),
            "resume-4".to_string()
        ],
        "resume stream should be forward-only without replay duplicates: {stdout:?}"
    );
    let unique: HashSet<String> = resume_lines.iter().cloned().collect();
    assert_eq!(
        unique.len(),
        4,
        "resume output should not replay previously delivered chunks: {resume_lines:?}"
    );

    let read_back = exec_with_transport_retry(
        &session,
        "cat /tmp/chevalier_checkpoint_rebind_count",
        ExecOptions::default(),
        Duration::from_secs(120),
    )
    .await
    .expect("read checkpoint rebind side-effect counter");
    drop(read_back.input);
    let (counter_stdout, counter_stderr, counter_exit, counter_timed_out) =
        collect_exec(read_back.events, Duration::from_secs(120)).await;
    assert!(!counter_timed_out, "checkpoint counter read timed out");
    assert_eq!(
        counter_exit,
        Some(0),
        "checkpoint counter read failed stdout={counter_stdout:?} stderr={counter_stderr:?}"
    );
    assert_eq!(
        counter_stdout.trim(),
        "1",
        "rebind path for non-terminal stream must not dispatch fresh command start"
    );

    session
        .discard()
        .await
        .expect("discard checkpoint resume session");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires real distributed control-plane daemons; run via scripts/integration/verify_real_failover.sh --distributed"]
async fn distributed_stream_events_include_identity_envelope_and_monotonic_sequence() {
    let primary_endpoint = required_env("CHEVALIER_SANDBOX_REAL_PRIMARY_ENDPOINT");
    let secondary_endpoint = required_env("CHEVALIER_SANDBOX_REAL_SECONDARY_ENDPOINT");
    let dist_cfg = distributed_control_config_from_env();
    let nats_url = dist_cfg.nats_url.clone();
    let nats_subject_prefix = dist_cfg.nats_subject_prefix.clone();
    let nats_stream_name = dist_cfg.nats_stream_name.clone();

    let sandbox = Sandbox::connect(
        primary_endpoint,
        distributed_failover_sandbox_config(secondary_endpoint),
    )
    .await
    .expect("connect distributed sandbox");

    let session_id = format!("real-stream-envelope-{}", Uuid::new_v4());
    let session = sandbox
        .session(SessionOptions {
            session_id: Some(session_id.clone()),
            metadata: distributed_continuity_metadata(),
            auto_start: true,
            ..SessionOptions::default()
        })
        .await
        .expect("create distributed stream envelope session");

    let exec = exec_with_transport_retry(
        &session,
        "for i in 1 2 3; do echo envelope-$i; sleep 1; done",
        ExecOptions::default(),
        Duration::from_secs(180),
    )
    .await
    .expect("start distributed envelope stream command");
    drop(exec.input);
    let (stdout, stderr, exit, timed_out) =
        collect_exec(exec.events, Duration::from_secs(180)).await;
    assert!(!timed_out, "envelope command timed out");
    assert_eq!(
        exit,
        Some(0),
        "envelope command failed stdout={stdout:?} stderr={stderr:?}"
    );

    let events = collect_exec_stream_events_for_session(
        &nats_url,
        &nats_subject_prefix,
        &nats_stream_name,
        &session_id,
        Duration::from_secs(30),
        4,
    )
    .await;
    assert!(
        !events.is_empty(),
        "expected distributed exec stream events in NATS for session {session_id}"
    );

    let mut seen_event_ids = HashSet::new();
    let mut last_seq_by_stream: HashMap<String, u64> = HashMap::new();
    for event in &events {
        assert!(
            !event.cluster_id.trim().is_empty(),
            "cluster_id must be present on stream event: {event:?}"
        );
        assert!(
            !event.logical_stream_id.trim().is_empty(),
            "logical_stream_id must be present on stream event: {event:?}"
        );
        assert!(
            event.event_seq > 0,
            "event_seq must be positive on stream event: {event:?}"
        );
        assert!(
            !event.event_id.trim().is_empty(),
            "event_id must be present on stream event: {event:?}"
        );
        assert!(
            seen_event_ids.insert(event.event_id.clone()),
            "event_id must be unique for the stream session: {}",
            event.event_id
        );
        let last = last_seq_by_stream
            .entry(event.logical_stream_id.clone())
            .or_insert(0);
        assert!(
            event.event_seq > *last,
            "event_seq must be strictly increasing per logical stream (prev={}, next={}, stream={})",
            *last,
            event.event_seq,
            event.logical_stream_id
        );
        *last = event.event_seq;
        let _ = event.producer_epoch;
    }

    session
        .discard()
        .await
        .expect("discard stream envelope session");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires real distributed control-plane daemons; run via scripts/integration/verify_real_failover.sh --distributed"]
async fn distributed_terminal_stream_is_not_rerun_after_primary_failover() {
    let primary_endpoint = required_env("CHEVALIER_SANDBOX_REAL_PRIMARY_ENDPOINT");
    let secondary_endpoint = required_env("CHEVALIER_SANDBOX_REAL_SECONDARY_ENDPOINT");
    let primary_pid = primary_pid();
    let dist_cfg = distributed_control_config_from_env();
    let nats_url = dist_cfg.nats_url.clone();
    let nats_subject_prefix = dist_cfg.nats_subject_prefix.clone();
    let nats_stream_name = dist_cfg.nats_stream_name.clone();

    let sandbox = Sandbox::connect(
        primary_endpoint,
        distributed_failover_sandbox_config(secondary_endpoint.clone()),
    )
    .await
    .expect("connect distributed sandbox");

    let session_id = format!("real-terminal-no-rerun-{}", Uuid::new_v4());
    let session = sandbox
        .session(SessionOptions {
            session_id: Some(session_id.clone()),
            metadata: distributed_continuity_metadata(),
            auto_start: true,
            ..SessionOptions::default()
        })
        .await
        .expect("create distributed terminal no-rerun session");

    let marker_command = "count_file=/tmp/chevalier_terminal_no_rerun_count; count=0; [ -f \"$count_file\" ] && count=$(cat \"$count_file\"); count=$((count+1)); echo \"$count\" > \"$count_file\"; echo terminal-complete";
    let exec = exec_with_transport_retry(
        &session,
        marker_command,
        ExecOptions::default(),
        Duration::from_secs(120),
    )
    .await
    .expect("run terminal marker command");
    drop(exec.input);
    let (stdout, stderr, exit, timed_out) =
        collect_exec(exec.events, Duration::from_secs(120)).await;
    assert!(!timed_out, "terminal marker command timed out");
    assert_eq!(
        exit,
        Some(0),
        "terminal marker command failed stdout={stdout:?} stderr={stderr:?}"
    );
    assert!(
        stdout.contains("terminal-complete"),
        "expected terminal marker output, got: {stdout}"
    );

    let pre_failover_events = collect_exec_stream_events_for_session(
        &nats_url,
        &nats_subject_prefix,
        &nats_stream_name,
        &session_id,
        Duration::from_secs(30),
        2,
    )
    .await;
    assert!(
        !pre_failover_events.is_empty(),
        "expected distributed stream events before failover for session {session_id}"
    );

    let mut terminal_checkpoint_by_stream: HashMap<String, (u64, u64)> = HashMap::new();
    for event in &pre_failover_events {
        if matches!(event.kind.as_str(), "exit" | "timeout" | "error") {
            let checkpoint = terminal_checkpoint_by_stream
                .entry(event.logical_stream_id.clone())
                .or_insert((0, 0));
            if event.event_seq >= checkpoint.0 {
                *checkpoint = (event.event_seq, event.producer_epoch);
            }
        }
    }
    assert!(
        !terminal_checkpoint_by_stream.is_empty(),
        "expected at least one terminal stream checkpoint before failover"
    );

    kill_pid(primary_pid);

    let secondary_sandbox = Sandbox::connect(
        secondary_endpoint.clone(),
        distributed_failover_sandbox_config(secondary_endpoint.clone()),
    )
    .await
    .expect("connect distributed sandbox directly to secondary endpoint");
    let rebound_session = attach_with_retry(
        &secondary_sandbox,
        &session_id,
        FAILOVER_ATTACH_RETRY_TIMEOUT,
    )
    .await
    .expect("attach distributed session on secondary after primary loss");

    let follow_up = exec_with_transport_retry(
        &rebound_session,
        "echo terminal-no-rerun-attach-ok",
        ExecOptions::default(),
        Duration::from_secs(120),
    )
    .await
    .expect("run post-failover terminal no-rerun follow-up command");
    drop(follow_up.input);
    let (stdout2, stderr2, exit2, timed_out2) =
        collect_exec(follow_up.events, Duration::from_secs(120)).await;
    assert!(!timed_out2, "terminal follow-up command timed out");
    assert_eq!(
        exit2,
        Some(0),
        "terminal follow-up command failed stdout={stdout2:?} stderr={stderr2:?}"
    );
    assert_eq!(
        stdout2.trim(),
        "terminal-no-rerun-attach-ok",
        "unexpected terminal follow-up output after failover"
    );

    let post_failover_events = collect_exec_stream_events_for_session(
        &nats_url,
        &nats_subject_prefix,
        &nats_stream_name,
        &session_id,
        Duration::from_secs(45),
        3,
    )
    .await;
    assert!(
        !post_failover_events.is_empty(),
        "expected distributed stream events after failover for session {session_id}"
    );

    let mut max_event_by_stream: HashMap<String, (u64, u64)> = HashMap::new();
    for event in &post_failover_events {
        let entry = max_event_by_stream
            .entry(event.logical_stream_id.clone())
            .or_insert((0, 0));
        if event.event_seq > entry.0
            || (event.event_seq == entry.0 && event.producer_epoch > entry.1)
        {
            *entry = (event.event_seq, event.producer_epoch);
        }
    }
    for (stream_id, (terminal_seq, terminal_epoch)) in terminal_checkpoint_by_stream {
        if let Some((max_seq, max_epoch)) = max_event_by_stream.get(stream_id.as_str()) {
            assert!(
                *max_seq <= terminal_seq && *max_epoch <= terminal_epoch,
                "terminal logical stream must not emit new events after failover (stream_id={stream_id}, terminal_seq={terminal_seq}, terminal_epoch={terminal_epoch}, observed_seq={max_seq}, observed_epoch={max_epoch})"
            );
        }
    }

    rebound_session
        .discard()
        .await
        .expect("discard terminal no-rerun session");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires real distributed control-plane daemons; run via scripts/integration/verify_real_failover.sh --distributed"]
async fn distributed_mq_retry_dedupe_dead_letter_behavior_under_primary_loss() {
    let primary_endpoint = required_env("CHEVALIER_SANDBOX_REAL_PRIMARY_ENDPOINT");
    let secondary_endpoint = required_env("CHEVALIER_SANDBOX_REAL_SECONDARY_ENDPOINT");
    let primary_pid = primary_pid();

    let dist_cfg = distributed_control_config_from_env();
    let nats_url = dist_cfg.nats_url.clone();
    let nats_subject_prefix = dist_cfg.nats_subject_prefix.clone();
    let nats_stream_name = dist_cfg.nats_stream_name.clone();

    let sandbox = Sandbox::connect(
        primary_endpoint,
        distributed_failover_sandbox_config(secondary_endpoint.clone()),
    )
    .await
    .expect("connect distributed sandbox for mq retry/dlq coverage");

    let session_id = format!("real-mq-dlq-{}", Uuid::new_v4());
    let session = sandbox
        .session(SessionOptions {
            session_id: Some(session_id.clone()),
            metadata: distributed_continuity_metadata(),
            auto_start: true,
            ..SessionOptions::default()
        })
        .await
        .expect("create distributed session for mq retry/dlq coverage");

    let warmup = exec_with_transport_retry(
        &session,
        "echo mq-dlq-warmup",
        ExecOptions::default(),
        Duration::from_secs(120),
    )
    .await
    .expect("warmup exec before node loss");
    drop(warmup.input);
    let (_warm_stdout, _warm_stderr, warm_exit, warm_timed_out) =
        collect_exec(warmup.events, Duration::from_secs(120)).await;
    assert!(!warm_timed_out, "warmup command timed out");
    assert_eq!(warm_exit, Some(0), "warmup command failed before node loss");

    kill_pid(primary_pid);
    assert!(
        wait_for_vmd_ready(secondary_endpoint.as_str(), Duration::from_secs(90)).await,
        "secondary vmd should become ready after primary loss"
    );

    let secondary_sandbox = Sandbox::connect(
        secondary_endpoint.clone(),
        distributed_failover_sandbox_config(secondary_endpoint.clone()),
    )
    .await
    .expect("connect distributed sandbox directly to secondary after primary loss");
    let rebound = attach_with_retry(
        &secondary_sandbox,
        &session_id,
        FAILOVER_ATTACH_RETRY_TIMEOUT,
    )
    .await
    .expect("attach distributed session on secondary for mq retry/dlq coverage");

    let idempotency_key = format!("real-mq-dlq-idem-{}", Uuid::new_v4());
    let first_command_id = String::new();
    publish_invalid_exec_stream_input_command(
        &nats_url,
        &nats_subject_prefix,
        first_command_id.as_str(),
        &idempotency_key,
        &session_id,
        rebound.vm_id(),
    )
    .await;

    let dead_letter = wait_for_dead_letter_for_identity(
        &nats_url,
        &nats_subject_prefix,
        &nats_stream_name,
        first_command_id.as_str(),
        &idempotency_key,
        None,
        Duration::from_secs(90),
    )
    .await
    .expect("expected dead-letter envelope for invalid exec.stream.input command");
    let payload = dead_letter.get("payload").unwrap_or(&dead_letter);
    let reason = payload
        .get("reason")
        .or_else(|| dead_letter.get("reason"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let delivered = payload
        .get("delivered")
        .or_else(|| dead_letter.get("delivered"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    assert_eq!(
        reason, "exec_stream_input_failed",
        "dead-letter reason should record failed exec.stream.input handling"
    );
    assert!(
        delivered >= 2,
        "dead-letter delivery count should demonstrate retry attempts before poison termination (delivered={delivered})"
    );
    let first_dead_letter_captured_at = dead_letter
        .get("captured_at_unix_ms")
        .and_then(Value::as_u64)
        .unwrap_or(0);

    let duplicate_command_id = String::new();
    publish_invalid_exec_stream_input_command(
        &nats_url,
        &nats_subject_prefix,
        duplicate_command_id.as_str(),
        &idempotency_key,
        &session_id,
        rebound.vm_id(),
    )
    .await;

    let duplicate_dead_letter = wait_for_dead_letter_for_identity(
        &nats_url,
        &nats_subject_prefix,
        &nats_stream_name,
        duplicate_command_id.as_str(),
        &idempotency_key,
        Some(first_dead_letter_captured_at),
        Duration::from_secs(20),
    )
    .await;
    assert!(
        duplicate_dead_letter.is_none(),
        "duplicate command with identical idempotency key should be deduped and must not create a second dead-letter"
    );

    rebound
        .discard()
        .await
        .expect("discard mq retry/dlq distributed session");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires real vmd daemons; run via scripts/integration/verify_real_failover.sh"]
async fn tierb_missing_restore_marker_fails_policy_under_failover_rebind() {
    let primary_endpoint = required_env("CHEVALIER_SANDBOX_REAL_PRIMARY_ENDPOINT");
    let secondary_endpoint = required_env("CHEVALIER_SANDBOX_REAL_SECONDARY_ENDPOINT");
    let primary_pid = primary_pid();

    let sandbox = Sandbox::connect(
        primary_endpoint,
        failover_sandbox_config(secondary_endpoint.clone()),
    )
    .await
    .expect("connect sandbox against primary + secondary endpoints");

    let session_id = format!("real-tierb-missing-marker-{}", Uuid::new_v4());
    let mut metadata = HashMap::new();
    metadata.insert("tier_b_eligible".to_string(), "true".to_string());
    let session = sandbox
        .session(SessionOptions {
            session_id: Some(session_id),
            metadata,
            auto_start: true,
            ..SessionOptions::default()
        })
        .await
        .expect("create tier-b eligible session");

    let warmup = exec_with_transport_retry(
        &session,
        "echo tierb-warmup",
        ExecOptions::default(),
        Duration::from_secs(120),
    )
    .await
    .expect("warmup command before failover");
    drop(warmup.input);
    let _ = collect_exec(warmup.events, Duration::from_secs(120)).await;

    kill_pid(primary_pid);

    let result = exec_with_transport_retry(
        &session,
        "echo should-not-run",
        ExecOptions::default(),
        Duration::from_secs(180),
    )
    .await;
    let err = match result {
        Ok(_) => panic!("tier-b rebind without restore marker should fail policy"),
        Err(err) => err,
    };
    match err {
        SandboxError::InvalidResponse(message) => {
            assert!(
                message.contains("requires execution restore snapshot marker"),
                "unexpected policy error message: {message}"
            );
        }
        other => panic!("expected InvalidResponse policy failure, got: {other}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires real vmd daemons; run via scripts/integration/verify_real_failover.sh"]
async fn tierb_restore_marker_rehydrates_and_resumes_under_failover_rebind() {
    let primary_endpoint = required_env("CHEVALIER_SANDBOX_REAL_PRIMARY_ENDPOINT");
    let secondary_endpoint = required_env("CHEVALIER_SANDBOX_REAL_SECONDARY_ENDPOINT");
    let primary_pid = primary_pid();

    let sandbox = Sandbox::connect(
        primary_endpoint.clone(),
        failover_sandbox_config(secondary_endpoint.clone()),
    )
    .await
    .expect("connect sandbox against primary + secondary endpoints");

    let parent_id = format!("real-tierb-parent-{}", Uuid::new_v4());
    let mut parent_metadata = HashMap::new();
    parent_metadata.insert("tier_b_eligible".to_string(), "true".to_string());
    let parent = sandbox
        .session(SessionOptions {
            session_id: Some(parent_id),
            metadata: parent_metadata,
            auto_start: true,
            ..SessionOptions::default()
        })
        .await
        .expect("create parent session for fork marker test");

    let fork = parent
        .fork(chevalier_sandbox::ForkOptions {
            child_name: Some(format!("tierb-child-{}", Uuid::new_v4())),
            auto_start_child: false,
            child_metadata: HashMap::new(),
        })
        .await
        .expect("fork child should create restore marker from snapshot");
    let child = fork.child;
    let child_session_id = child.session_id().to_string();

    // @dive: Rebind policy relies on snapshot marker metadata produced by running-parent fork; assert marker exists before failover.
    let mut primary_client = VmdServiceClient::connect(primary_endpoint.clone())
        .await
        .expect("connect primary vmd client");
    let child_vm = primary_client
        .get_vm(tonic::Request::new(
            chevalier_sandbox::proto::vmd::v1::GetVmRequest {
                vm_id: child.vm_id().to_string(),
            },
        ))
        .await
        .expect("fetch child vm metadata")
        .into_inner();
    let has_fork_snapshot_marker = child_vm
        .metadata
        .get("chevalier.fork_snapshot")
        .is_some_and(|value| !value.trim().is_empty());
    let has_restore_snapshot_marker = child_vm
        .metadata
        .get("chevalier.execution_restore_snapshot_id")
        .is_some_and(|value| !value.trim().is_empty())
        || child_vm
            .metadata
            .get("chevalier.execution_restore_snapshot_name")
            .is_some_and(|value| !value.trim().is_empty());
    assert!(
        has_fork_snapshot_marker,
        "forked child should carry chevalier.fork_snapshot marker metadata"
    );
    assert!(
        has_restore_snapshot_marker,
        "forked child should carry execution restore snapshot marker metadata"
    );

    kill_pid(primary_pid);
    // @dive: After primary loss, Tier-B marker recovery should execute on a rebound session handle routed by the failover-aware facade.
    let rebound_child = attach_with_retry(&sandbox, &child_session_id, Duration::from_secs(180))
        .await
        .expect("rebind child session after primary node loss");

    let resumed = exec_with_transport_retry(
        &rebound_child,
        "echo tierb-restore-ok",
        ExecOptions::default(),
        Duration::from_secs(300),
    )
    .await
    .expect("tier-b rebind with restore marker should recover");
    drop(resumed.input);
    let (stdout, stderr, exit, timed_out) =
        collect_exec(resumed.events, Duration::from_secs(180)).await;
    assert!(!timed_out, "tier-b marker resume command timed out");
    assert_eq!(
        exit,
        Some(0),
        "tier-b marker resume command failed stdout={stdout:?} stderr={stderr:?}"
    );
    assert!(
        stdout.contains("tierb-restore-ok"),
        "expected restore marker success output, got: {stdout}"
    );

    rebound_child
        .discard()
        .await
        .expect("discard child session after marker restore test");
}
