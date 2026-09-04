// @dive-file: Runs distributed control-bus command consumption with idempotency, ownership fencing, and failure handling.
// @dive-rel: Consumes ControlBusConfig from vmd/src/config.rs and enforces bounded in-flight command behavior.
// @dive-rel: Publishes replay/dead-letter/overload signals consumed by operational gates in scripts/verify_chevalier_sandbox.sh.
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use async_nats::jetstream;
use async_nats::jetstream::AckKind;
use async_nats::jetstream::consumer::{AckPolicy, pull};
use chrono::Utc;
use etcd_client::{Client as EtcdClient, Compare, CompareOp, PutOptions, Txn, TxnOp};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Mutex, OwnedMutexGuard, Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::assets::portproxy;
use crate::config::ControlBusConfig;
use crate::guest_exec_probe::{
    META_PORTPROXY_AUTH_TOKEN, ensure_guest_portproxy_binary, portproxy_auth_header_from_metadata,
    probe_guest_exec_ready_anyhow, request_with_portproxy_auth,
};
use crate::partition::PartitionGate;
use crate::proto::bracket::portproxy::v1::daemon_manager_client::DaemonManagerClient;
use crate::proto::bracket::portproxy::v1::shell_exec_client::ShellExecClient;
use crate::proto::bracket::portproxy::v1::{
    AttachDaemonRequest, AttachDaemonResponse, AttachDaemonStart, ExecDaemonRequest, ExecRequest,
    ExecResponse, ExecStart, attach_daemon_request, attach_daemon_response, exec_request,
    exec_response,
};
use crate::state::{Manager, PendingSnapshot, SnapshotParams, UpdateVmParams, VmMetadata, VmState};

const META_EXEC_RESTORE_SNAPSHOT_ID: &str = "chevalier.execution_restore_snapshot_id";
const META_EXEC_RESTORE_SNAPSHOT_NAME: &str = "chevalier.execution_restore_snapshot_name";
const META_TIER_B_ELIGIBLE: &str = "chevalier.tier_b_eligible";
const MAX_EXEC_RUN_OUTPUT_BYTES: usize = 4 * 1024 * 1024;
const ETCD_DEDUPE_TTL_SECS: i64 = 24 * 60 * 60;
const COMMAND_ACK_PROGRESS_MAX_DURATION: Duration = Duration::from_secs(10 * 60);
const COMMAND_ACK_PROGRESS_MIN_INTERVAL_MS: u64 = 500;
const COMMAND_ACK_PROGRESS_MAX_INTERVAL_MS: u64 = 15_000;
// @dive: Keep below the API-side distributed exec start wait budget so vmd can
// mark the bad VM and publish a terminal error before the caller gives up locally.
const GUEST_EXEC_READY_TIMEOUT: Duration = Duration::from_secs(180);
const WARM_GUEST_EXEC_READY_TIMEOUT: Duration = Duration::from_secs(30);
const RUNNING_VM_COLD_GUEST_EXEC_GRACE: Duration = Duration::from_secs(90);
const GUEST_EXEC_READY_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(3);
const VM_READY_CACHE_TTL: Duration = Duration::from_secs(5 * 60);
// @dive: Skip refreshing the execution-restore marker if we already refreshed within
// this window. Bounds tier-B snapshot IOPS on shared RWX storage when an agent fires
// many small commands in close succession; older marker remains valid as resume target.
const EXEC_RESTORE_REFRESH_THROTTLE: Duration = Duration::from_secs(30);
const EXEC_RESTORE_REFRESH_QUIET_PERIOD: Duration = Duration::from_secs(15);
const VM_SNAPSHOT_STATE_GC_TTL: Duration = Duration::from_secs(60 * 60);

pub struct CommandConsumerHandle {
    stop_tx: Option<oneshot::Sender<()>>,
    join: Option<JoinHandle<()>>,
}

impl CommandConsumerHandle {
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.await;
        }
    }
}

#[derive(Debug, Deserialize)]
struct CommandEnvelope {
    #[serde(default)]
    command_id: String,
    #[serde(default)]
    idempotency_key: String,
    #[serde(default)]
    command_type: String,
    #[serde(default)]
    ordering_key: String,
    #[serde(default)]
    expected_fence: Option<String>,
    #[serde(default)]
    target_node_id: Option<String>,
    #[serde(default)]
    payload: Value,
}

#[derive(Debug, Deserialize)]
struct ExecRunPayload {
    #[serde(default)]
    session_id: String,
    #[serde(default)]
    vm_id: String,
    #[serde(default)]
    command: String,
    #[serde(default)]
    env: HashMap<String, String>,
    #[serde(default)]
    timeout_secs: Option<i32>,
    #[serde(default)]
    detach: bool,
    #[serde(default)]
    shell: Option<String>,
}

#[derive(Clone)]
struct ActiveExecStream {
    request_tx: mpsc::Sender<AttachDaemonRequest>,
    last_input_seq: std::sync::Arc<Mutex<u64>>,
    session_id: String,
    vm_id: String,
    node_id: String,
    producer_epoch: u64,
    daemon_name: String,
    control_client: DaemonManagerClient<tonic::transport::Channel>,
    auth: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
}

#[derive(Debug, Deserialize)]
struct ExecStreamStartPayload {
    #[serde(default)]
    stream_id: String,
    #[serde(default)]
    logical_stream_id: String,
    #[serde(default)]
    cluster_id: String,
    #[serde(default)]
    producer_epoch: u64,
    #[serde(default)]
    resume_after_event_seq: u64,
    #[serde(default)]
    session_id: String,
    #[serde(default)]
    vm_id: String,
    #[serde(default)]
    command: String,
    #[serde(default)]
    env: HashMap<String, String>,
    #[serde(default)]
    timeout_secs: Option<i32>,
    #[serde(default)]
    detach: bool,
    #[serde(default)]
    shell: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ExecStreamInputPayload {
    #[serde(default)]
    stream_id: String,
    #[serde(default)]
    input_seq: u64,
    #[serde(default)]
    input_kind: String,
    #[serde(default)]
    data: Option<Vec<u8>>,
    #[serde(default)]
    producer_epoch: Option<u64>,
    #[serde(default)]
    session_id: String,
    #[serde(default)]
    vm_id: String,
    #[serde(default)]
    target_node_id: String,
    #[serde(default)]
    signal: Option<i32>,
}

#[derive(Debug, Serialize)]
struct ExecRunResult {
    command_id: String,
    session_id: String,
    vm_id: String,
    stdout: String,
    stderr: String,
    stdout_truncated: bool,
    stderr_truncated: bool,
    exit_code: Option<i32>,
    timed_out: bool,
    error: Option<String>,
    executed_by_node_id: String,
    completed_at_unix_ms: u64,
}

#[derive(Debug, Default)]
struct BoundedTextOutput {
    value: String,
    bytes: usize,
    truncated: bool,
}

impl BoundedTextOutput {
    fn push_lossy(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        let remaining = MAX_EXEC_RUN_OUTPUT_BYTES.saturating_sub(self.bytes);
        if remaining == 0 {
            self.truncated = true;
            return;
        }
        let take = remaining.min(bytes.len());
        self.value
            .push_str(&String::from_utf8_lossy(&bytes[..take]));
        self.bytes += take;
        if take < bytes.len() {
            self.truncated = true;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExecStreamEvent {
    cluster_id: String,
    logical_stream_id: String,
    stream_id: String,
    event_seq: u64,
    event_id: String,
    producer_epoch: u64,
    command_id: String,
    session_id: String,
    vm_id: String,
    kind: String,
    #[serde(default)]
    data: Vec<u8>,
    #[serde(default)]
    exit_code: Option<i32>,
    #[serde(default)]
    timed_out: bool,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    sequence: u64,
    emitted_by_node_id: String,
    emitted_at_unix_ms: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct DeadLetterEnvelope {
    dead_letter_id: String,
    #[serde(default)]
    command_id: String,
    #[serde(default)]
    original_subject: String,
    #[serde(default)]
    reason: String,
    #[serde(default)]
    details: String,
    #[serde(default)]
    delivered: i64,
    #[serde(default)]
    node_id: String,
    #[serde(default)]
    captured_at_unix_ms: u64,
    payload: Value,
}

#[derive(Clone)]
struct EtcdDedupeStore {
    etcd: std::sync::Arc<Mutex<EtcdClient>>,
    key_prefix: String,
}

#[derive(Clone)]
struct EtcdOwnershipFenceStore {
    etcd: std::sync::Arc<Mutex<EtcdClient>>,
    session_fences_prefix: String,
    session_shard_count: u8,
}

impl EtcdDedupeStore {
    async fn connect(config: &ControlBusConfig) -> Result<Option<Self>> {
        if config.dedupe_etcd_endpoints.is_empty() {
            return Ok(None);
        }
        let client = EtcdClient::connect(config.dedupe_etcd_endpoints.clone(), None)
            .await
            .context("connect etcd for command dedupe")?;
        Ok(Some(Self {
            etcd: std::sync::Arc::new(Mutex::new(client)),
            key_prefix: config.dedupe_prefix.trim_end_matches('/').to_string(),
        }))
    }

    async fn mark_or_duplicate(&self, idempotency_key: &str) -> Result<bool> {
        let key = format!("{}/{}", self.key_prefix, idempotency_key);
        let mut client = self.etcd.lock().await;
        let lease = client
            .lease_grant(ETCD_DEDUPE_TTL_SECS, None)
            .await
            .context("grant dedupe key lease")?;
        let txn = Txn::new()
            .when(vec![Compare::version(key.clone(), CompareOp::Equal, 0)])
            .and_then(vec![TxnOp::put(
                key,
                b"1",
                Some(PutOptions::new().with_lease(lease.id())),
            )]);
        let response = client.txn(txn).await.context("dedupe txn")?;
        Ok(!response.succeeded())
    }

    async fn is_completed(&self, idempotency_key: &str) -> Result<bool> {
        let key = self.completed_key(idempotency_key);
        let mut client = self.etcd.lock().await;
        let response = client
            .get(key, None)
            .await
            .context("read completed command marker")?;
        Ok(!response.kvs().is_empty())
    }

    async fn mark_completed(&self, idempotency_key: &str) -> Result<()> {
        let key = self.completed_key(idempotency_key);
        let mut client = self.etcd.lock().await;
        let lease = client
            .lease_grant(ETCD_DEDUPE_TTL_SECS, None)
            .await
            .context("grant completed command marker lease")?;
        client
            .put(key, b"1", Some(PutOptions::new().with_lease(lease.id())))
            .await
            .context("write completed command marker")?;
        Ok(())
    }

    fn completed_key(&self, idempotency_key: &str) -> String {
        format!("{}/completed/{}", self.key_prefix, idempotency_key)
    }

    async fn acquire_inflight(
        &self,
        idempotency_key: &str,
        owner: &str,
        ttl: Duration,
    ) -> Result<Option<DistributedCommandInFlightGuard>> {
        let key = self.inflight_key(idempotency_key);
        let ttl_secs = i64::try_from(ttl.as_secs().max(1)).unwrap_or(i64::MAX);
        let mut client = self.etcd.lock().await;
        let lease = client
            .lease_grant(ttl_secs, None)
            .await
            .context("grant in-flight command lease")?;
        let txn = Txn::new()
            .when(vec![Compare::version(key.clone(), CompareOp::Equal, 0)])
            .and_then(vec![TxnOp::put(
                key.clone(),
                owner.as_bytes(),
                Some(PutOptions::new().with_lease(lease.id())),
            )]);
        let response = client
            .txn(txn)
            .await
            .context("in-flight command acquire txn")?;
        if !response.succeeded() {
            return Ok(None);
        }
        Ok(Some(DistributedCommandInFlightGuard {
            store: self.clone(),
            key: Some(key),
            owner: owner.to_string(),
        }))
    }

    async fn release_inflight(&self, key: &str, owner: &str) -> Result<()> {
        let mut client = self.etcd.lock().await;
        let txn = Txn::new()
            .when(vec![Compare::value(key, CompareOp::Equal, owner)])
            .and_then(vec![TxnOp::delete(key, None)]);
        client
            .txn(txn)
            .await
            .context("in-flight command release txn")?;
        Ok(())
    }

    fn inflight_key(&self, idempotency_key: &str) -> String {
        format!("{}/inflight/{}", self.key_prefix, idempotency_key)
    }
}

impl EtcdOwnershipFenceStore {
    async fn connect(config: &ControlBusConfig) -> Result<Option<Self>> {
        if config.dedupe_etcd_endpoints.is_empty() {
            return Ok(None);
        }
        let client = EtcdClient::connect(config.dedupe_etcd_endpoints.clone(), None)
            .await
            .context("connect etcd for ownership fences")?;
        let etcd_prefix = session_etcd_prefix_from_dedupe_prefix(&config.dedupe_prefix);
        Ok(Some(Self {
            etcd: std::sync::Arc::new(Mutex::new(client)),
            session_fences_prefix: format!("{}/session_fences", etcd_prefix.trim_end_matches('/')),
            session_shard_count: 16,
        }))
    }

    async fn check_session_fence(
        &self,
        session_id: Option<&str>,
        expected_fence: Option<&str>,
    ) -> Result<()> {
        let expected_fence = expected_fence
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let Some(session_id) = session_id.map(str::trim).filter(|value| !value.is_empty()) else {
            if expected_fence.is_some() {
                return Err(anyhow!("expected ownership fence requires session_id"));
            }
            return Ok(());
        };
        let key = self.session_fence_key(session_id);
        let legacy_key = self.legacy_session_fence_key(session_id);
        let mut client = self.etcd.lock().await;
        let mut current = read_fence_value(&mut client, &key).await?;
        if current.is_none() && key != legacy_key {
            current = read_fence_value(&mut client, &legacy_key).await?;
        }
        if !ownership_fence_allows_transition(current.as_deref(), expected_fence) {
            let current_display = current.as_deref().unwrap_or("<none>");
            let expected_display = expected_fence.unwrap_or("<none>");
            return Err(anyhow!(
                "session ownership fence mismatch for session_id={session_id}: expected={expected_display} current={current_display}"
            ));
        }
        Ok(())
    }

    fn session_fence_key(&self, session_id: &str) -> String {
        let shard = shard_for_key(session_id, self.session_shard_count.max(1));
        format!("{}/{shard:02}/{session_id}", self.session_fences_prefix)
    }

    fn legacy_session_fence_key(&self, session_id: &str) -> String {
        format!("{}/{}", self.session_fences_prefix, session_id)
    }
}

struct CommandAckProgress {
    stop_tx: Option<oneshot::Sender<()>>,
}

struct CommandInFlightGuard {
    in_flight: std::sync::Arc<Mutex<HashMap<String, Instant>>>,
    key: Option<String>,
}

struct DistributedCommandInFlightGuard {
    store: EtcdDedupeStore,
    key: Option<String>,
    owner: String,
}

#[derive(Clone, Debug)]
struct VmReadinessState {
    locks: std::sync::Arc<Mutex<HashMap<String, std::sync::Arc<Mutex<()>>>>>,
    ready: std::sync::Arc<Mutex<HashMap<String, VmReadyCacheEntry>>>,
}

#[derive(Clone, Debug)]
struct VmReadyCacheEntry {
    fingerprint: VmReadyFingerprint,
    verified_at: Instant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VmReadyFingerprint {
    started_at_unix_ms: i64,
    rpc_port: i32,
    portproxy_auth_token: Option<String>,
}

#[derive(Clone, Debug)]
struct VmExecActivityState {
    active_counts: std::sync::Arc<std::sync::Mutex<HashMap<String, usize>>>,
}

#[derive(Debug)]
struct VmExecActivityLease {
    state: VmExecActivityState,
    vm_id: String,
}

impl VmReadinessState {
    fn new() -> Self {
        Self {
            locks: std::sync::Arc::new(Mutex::new(HashMap::new())),
            ready: std::sync::Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn lock(&self, vm_id: &str) -> OwnedMutexGuard<()> {
        let lock = {
            let mut guard = self.locks.lock().await;
            guard
                .entry(vm_id.to_string())
                .or_insert_with(|| std::sync::Arc::new(Mutex::new(())))
                .clone()
        };
        lock.lock_owned().await
    }

    async fn lock_with_timeout(
        &self,
        vm_id: &str,
        timeout: Duration,
    ) -> Result<OwnedMutexGuard<()>> {
        tokio::time::timeout(timeout, self.lock(vm_id))
            .await
            .map_err(|_| {
                anyhow!(
                    "timed out after {}s waiting for vm {vm_id} guest exec readiness gate",
                    timeout.as_secs()
                )
            })
    }

    async fn is_ready(&self, vm_id: &str, fingerprint: &VmReadyFingerprint) -> bool {
        let now = Instant::now();
        let mut guard = self.ready.lock().await;
        guard.retain(|_, entry| now.duration_since(entry.verified_at) < VM_READY_CACHE_TTL);
        guard
            .get(vm_id)
            .is_some_and(|entry| entry.fingerprint == *fingerprint)
    }

    async fn mark_ready(&self, vm_id: &str, fingerprint: VmReadyFingerprint) {
        let mut guard = self.ready.lock().await;
        guard.insert(
            vm_id.to_string(),
            VmReadyCacheEntry {
                fingerprint,
                verified_at: Instant::now(),
            },
        );
    }

    async fn clear_ready(&self, vm_id: &str) {
        let mut guard = self.ready.lock().await;
        guard.remove(vm_id);
    }
}

impl VmReadyFingerprint {
    fn from_metadata(metadata: &VmMetadata) -> Option<Self> {
        let started_at_unix_ms = metadata.started_at.map(|value| value.timestamp_millis())?;
        Some(Self {
            started_at_unix_ms,
            rpc_port: metadata.network.rpc_port,
            portproxy_auth_token: metadata.metadata.get(META_PORTPROXY_AUTH_TOKEN).cloned(),
        })
    }
}

impl VmExecActivityState {
    fn new() -> Self {
        Self {
            active_counts: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    fn begin(&self, vm_id: &str) -> VmExecActivityLease {
        let mut guard = self
            .active_counts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard.entry(vm_id.to_string()).or_insert(0) += 1;
        VmExecActivityLease {
            state: self.clone(),
            vm_id: vm_id.to_string(),
        }
    }

    fn end(&self, vm_id: &str) {
        let mut guard = self
            .active_counts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(count) = guard.get_mut(vm_id) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            guard.remove(vm_id);
        }
    }

    fn is_active(&self, vm_id: &str) -> bool {
        let guard = self
            .active_counts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.get(vm_id).copied().unwrap_or(0) > 0
    }
}

impl Drop for VmExecActivityLease {
    fn drop(&mut self) {
        self.state.end(&self.vm_id);
    }
}

impl CommandInFlightGuard {
    async fn acquire(
        in_flight: std::sync::Arc<Mutex<HashMap<String, Instant>>>,
        key: &str,
        ttl: Duration,
    ) -> Option<Self> {
        let now = Instant::now();
        let mut guard = in_flight.lock().await;
        guard.retain(|_, ts| now.duration_since(*ts) < ttl);
        if guard.contains_key(key) {
            return None;
        }
        guard.insert(key.to_string(), now);
        drop(guard);
        Some(Self {
            in_flight,
            key: Some(key.to_string()),
        })
    }
}

impl Drop for CommandInFlightGuard {
    fn drop(&mut self) {
        let Some(key) = self.key.take() else {
            return;
        };
        let in_flight = std::sync::Arc::clone(&self.in_flight);
        tokio::spawn(async move {
            in_flight.lock().await.remove(&key);
        });
    }
}

impl Drop for DistributedCommandInFlightGuard {
    fn drop(&mut self) {
        let Some(key) = self.key.take() else {
            return;
        };
        let store = self.store.clone();
        let owner = self.owner.clone();
        tokio::spawn(async move {
            if let Err(err) = store.release_inflight(key.as_str(), owner.as_str()).await {
                warn!(
                    key = %key,
                    error = %err,
                    "failed releasing distributed in-flight command guard"
                );
            }
        });
    }
}

impl CommandAckProgress {
    fn start(
        message: jetstream::Message,
        command_id: String,
        command_type: String,
        node_id: String,
        ack_wait_ms: u64,
    ) -> Option<Self> {
        if !command_needs_ack_progress(command_type.as_str()) {
            return None;
        }
        let interval_ms = (ack_wait_ms.max(1) / 2).clamp(
            COMMAND_ACK_PROGRESS_MIN_INTERVAL_MS,
            COMMAND_ACK_PROGRESS_MAX_INTERVAL_MS,
        );
        let interval = Duration::from_millis(interval_ms);
        let (stop_tx, mut stop_rx) = oneshot::channel();
        tokio::spawn(async move {
            let started = Instant::now();
            loop {
                tokio::select! {
                    _ = &mut stop_rx => break,
                    _ = tokio::time::sleep(interval) => {}
                }

                if started.elapsed() >= COMMAND_ACK_PROGRESS_MAX_DURATION {
                    warn!(
                        node_id = %node_id,
                        command_id = %command_id,
                        command_type = %command_type,
                        "stopping command ack progress heartbeat after max duration"
                    );
                    break;
                }

                if let Err(err) = message.ack_with(AckKind::Progress).await {
                    warn!(
                        node_id = %node_id,
                        command_id = %command_id,
                        command_type = %command_type,
                        err = %err,
                        "failed sending command ack progress heartbeat"
                    );
                    break;
                }
            }
        });
        Some(Self {
            stop_tx: Some(stop_tx),
        })
    }
}

impl Drop for CommandAckProgress {
    fn drop(&mut self) {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
    }
}

fn command_needs_ack_progress(command_type: &str) -> bool {
    matches!(command_type, "exec.run" | "exec.stream.start")
}

fn command_needs_single_owner(command_type: &str) -> bool {
    matches!(command_type, "exec.run" | "exec.stream.start")
}

fn readiness_gate_wait_timeout(ready_timeout: Duration) -> Duration {
    ready_timeout
        .checked_add(Duration::from_secs(5))
        .unwrap_or(ready_timeout)
}

async fn guest_exec_ready_timeout_for_start(manager: &Manager, vm_id: &str) -> Duration {
    match manager.get_with_runtime(vm_id).await {
        Ok((_, runtime)) if runtime.state == VmState::Running => {
            if runtime
                .started_at
                .and_then(|started_at| Utc::now().signed_duration_since(started_at).to_std().ok())
                .is_some_and(|age| age >= RUNNING_VM_COLD_GUEST_EXEC_GRACE)
            {
                WARM_GUEST_EXEC_READY_TIMEOUT
            } else {
                // @dive: QMP Running means the VM process is live, not that guest userspace
                //        has finished booting portproxy/shell-exec. Recently-started VMs
                //        still need the cold budget or deploy-induced cold starts get marked
                //        Error while booting normally.
                GUEST_EXEC_READY_TIMEOUT
            }
        }
        Ok(_) | Err(_) => GUEST_EXEC_READY_TIMEOUT,
    }
}

async fn cached_ready_running_vm(
    manager: &Manager,
    vm_readiness_state: &VmReadinessState,
    vm_id: &str,
    resume_only: bool,
) -> Option<VmMetadata> {
    if resume_only {
        return None;
    }
    let Ok((metadata, runtime)) = manager.get_with_runtime(vm_id).await else {
        return None;
    };
    if runtime.state != VmState::Running || metadata.state == VmState::Error {
        return None;
    }
    let fingerprint = VmReadyFingerprint::from_metadata(&metadata)?;
    if vm_readiness_state.is_ready(vm_id, &fingerprint).await {
        Some(metadata)
    } else {
        None
    }
}

async fn fail_if_vm_already_error(manager: &Manager, vm_id: &str, context: &str) -> Result<()> {
    match manager.get_with_runtime(vm_id).await {
        Ok((metadata, runtime))
            if metadata.state == VmState::Error || runtime.state == VmState::Error =>
        {
            Err(anyhow!(
                "{context} target vm {vm_id} is marked Error; recover/rebind before exec"
            ))
        }
        Ok(_) => Ok(()),
        Err(err) => Err(anyhow!("{context} target vm {vm_id} unavailable: {err}")),
    }
}

// @dive: Throttle state for the post-exit snapshot worker. Claims refresh
//        eligibility before QMP work starts, not after it completes, so multiple
//        fast exec exits cannot all launch background migrations against the
//        same VM. The older marker remains a valid resume target.
//
//        Owner_fence + RWX storage already guarantee single-writer per VM across
//        nodes, so this state is process-local and does not need cross-node sync.
#[derive(Clone, Debug)]
struct VmSnapshotState {
    last_refresh_attempt: std::sync::Arc<Mutex<HashMap<String, Instant>>>,
}

impl VmSnapshotState {
    fn new() -> Self {
        Self {
            last_refresh_attempt: std::sync::Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn try_begin_refresh(&self, vm_id: &str, throttle: Duration) -> bool {
        let now = Instant::now();
        let mut guard = self.last_refresh_attempt.lock().await;
        guard.retain(|_, ts| now.duration_since(*ts) < VM_SNAPSHOT_STATE_GC_TTL);
        match guard.get(vm_id) {
            Some(last) if now.duration_since(*last) < throttle => false,
            _ => {
                guard.insert(vm_id.to_string(), now);
                true
            }
        }
    }
}

pub async fn start(
    config: Option<ControlBusConfig>,
    manager: std::sync::Arc<Manager>,
) -> Result<Option<CommandConsumerHandle>> {
    start_with_trigger(config, manager, None, None).await
}

pub async fn start_with_trigger(
    config: Option<ControlBusConfig>,
    manager: std::sync::Arc<Manager>,
    reconcile_trigger: Option<mpsc::UnboundedSender<()>>,
    partition_gate: Option<PartitionGate>,
) -> Result<Option<CommandConsumerHandle>> {
    let Some(config) = config else {
        return Ok(None);
    };

    let nats = connect_nats(
        &config.nats_url,
        config.nats_auth_token.as_deref(),
        "connect nats for command consumer",
    )
    .await?;
    let jetstream = jetstream::new(nats);
    ensure_control_stream(&jetstream, &config).await?;

    let command_subject = format!("{}.cmd.>", config.subject_prefix);
    let control_stream = jetstream
        .get_stream(config.stream_name.clone())
        .await
        .context("get control stream")?;
    let consumer = control_stream
        .get_or_create_consumer(
            &config.command_consumer_durable,
            pull::Config {
                durable_name: Some(config.command_consumer_durable.clone()),
                ack_policy: AckPolicy::Explicit,
                ack_wait: Duration::from_millis(config.command_ack_wait_ms),
                max_deliver: config.command_max_deliver,
                filter_subject: command_subject.clone(),
                max_ack_pending: 1_024,
                ..Default::default()
            },
        )
        .await
        .context("get or create command consumer")?;
    let mut messages = consumer
        .messages()
        .await
        .context("start command consumer stream")?;

    let dedupe_store = EtcdDedupeStore::connect(&config).await?;
    let fence_store = EtcdOwnershipFenceStore::connect(&config).await?;
    let node_id = config.node_id.clone();
    let log_node_id = config.node_id.clone();
    let log_nats_url = config.nats_url.clone();
    let log_stream_name = config.stream_name.clone();
    let log_durable = config.command_consumer_durable.clone();
    let log_max_deliver = config.command_max_deliver;
    let log_dead_letter_subject = config.dead_letter_subject.clone();
    let log_dedupe_endpoint_count = config.dedupe_etcd_endpoints.len();
    let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
    // @dive: Dedupe state is shared across spawned command workers so duplicate suppression remains cluster-node local and bounded by TTL.
    let seen_commands: std::sync::Arc<Mutex<HashMap<String, Instant>>> =
        std::sync::Arc::new(Mutex::new(HashMap::new()));
    let active_commands: std::sync::Arc<Mutex<HashMap<String, Instant>>> =
        std::sync::Arc::new(Mutex::new(HashMap::new()));
    let completed_commands: std::sync::Arc<Mutex<HashMap<String, Instant>>> =
        std::sync::Arc::new(Mutex::new(HashMap::new()));
    let active_exec_streams: std::sync::Arc<Mutex<HashMap<String, ActiveExecStream>>> =
        std::sync::Arc::new(Mutex::new(HashMap::new()));
    let vm_readiness_state = VmReadinessState::new();
    let vm_exec_activity_state = VmExecActivityState::new();
    let vm_snapshot_state = VmSnapshotState::new();
    // @dive: Enforces a hard bound on in-flight control commands; overload is signaled deterministically via NAK+retry hint.
    let inflight_limit = std::sync::Arc::new(Semaphore::new(config.max_inflight_commands));
    let control_limit = std::sync::Arc::new(Semaphore::new(8));

    let join = tokio::spawn(async move {
        const DEDUPE_TTL: Duration = Duration::from_secs(600);
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                maybe_msg = messages.next() => {
                    let Some(maybe_msg) = maybe_msg else {
                        break;
                    };
                    match maybe_msg {
                        Ok(message) => {
                            let is_exec_control = serde_json::from_slice::<CommandEnvelope>(&message.payload)
                                .is_ok_and(|envelope| envelope.command_type == "exec.stream.control");
                            let limit = if is_exec_control { &control_limit } else { &inflight_limit };
                            let permit = match limit.clone().try_acquire_owned() {
                                Ok(permit) => permit,
                                Err(_) => {
                                    handle_overloaded_command_message(
                                        &message,
                                        &config,
                                        &node_id,
                                        &jetstream,
                                    )
                                    .await;
                                    continue;
                                }
                            };
                            let config = config.clone();
                            let node_id = node_id.clone();
                            let dedupe_store = dedupe_store.clone();
                            let fence_store = fence_store.clone();
                            let partition_gate = partition_gate.clone();
                            let seen_commands = seen_commands.clone();
                            let active_commands = active_commands.clone();
                            let completed_commands = completed_commands.clone();
                            let active_exec_streams = active_exec_streams.clone();
                            let vm_readiness_state = vm_readiness_state.clone();
                            let vm_exec_activity_state = vm_exec_activity_state.clone();
                            let vm_snapshot_state = vm_snapshot_state.clone();
                            let reconcile_trigger = reconcile_trigger.clone();
                            let manager = std::sync::Arc::clone(&manager);
                            let jetstream = jetstream.clone();
                            tokio::spawn(async move {
                                let _permit = permit;
                                process_command_message(
                                    message,
                                    &config,
                                    &node_id,
                                    &manager,
                                    dedupe_store.as_ref(),
                                    fence_store.as_ref(),
                                    partition_gate.as_ref(),
                                    &seen_commands,
                                    &active_commands,
                                    &completed_commands,
                                    &active_exec_streams,
                                    &vm_readiness_state,
                                    &vm_exec_activity_state,
                                    &vm_snapshot_state,
                                    reconcile_trigger.as_ref(),
                                    &jetstream,
                                    DEDUPE_TTL,
                                )
                                .await;
                            });
                        }
                        Err(err) => {
                            warn!(
                                node_id = %node_id,
                                err = %err,
                                "command consumer stream yielded an error"
                            );
                        }
                    }
                }
            }
        }
    });

    info!(
        node_id = %log_node_id,
        nats_url = %log_nats_url,
        stream_name = %log_stream_name,
        subject = %command_subject,
        durable = %log_durable,
        max_deliver = log_max_deliver,
        dead_letter_subject = %log_dead_letter_subject,
        dedupe_etcd_endpoints = log_dedupe_endpoint_count,
        "control command consumer enabled"
    );

    Ok(Some(CommandConsumerHandle {
        stop_tx: Some(stop_tx),
        join: Some(join),
    }))
}

pub async fn replay_dead_letters(config: ControlBusConfig, limit: usize) -> Result<usize> {
    if limit == 0 {
        return Ok(0);
    }

    let nats = connect_nats(
        &config.nats_url,
        config.nats_auth_token.as_deref(),
        "connect nats for dead-letter replay",
    )
    .await?;
    let jetstream = jetstream::new(nats);
    ensure_control_stream(&jetstream, &config).await?;

    let control_stream = jetstream
        .get_stream(config.stream_name.clone())
        .await
        .context("get control stream for replay")?;
    let replay_consumer_name = format!("{}-dlq-replay", config.command_consumer_durable);
    let consumer = control_stream
        .get_or_create_consumer(
            &replay_consumer_name,
            pull::Config {
                durable_name: Some(replay_consumer_name.clone()),
                ack_policy: AckPolicy::Explicit,
                ack_wait: Duration::from_millis(config.command_ack_wait_ms),
                max_deliver: config.command_max_deliver,
                filter_subject: config.dead_letter_subject.clone(),
                max_ack_pending: 256,
                ..Default::default()
            },
        )
        .await
        .context("get or create dead-letter replay consumer")?;

    let mut messages = consumer
        .messages()
        .await
        .context("start dead-letter replay stream")?;

    let mut replayed = 0usize;
    let mut idle_timeouts = 0usize;
    while replayed < limit {
        let next = tokio::time::timeout(Duration::from_millis(500), messages.next()).await;
        let maybe_msg = match next {
            Ok(value) => {
                idle_timeouts = 0;
                value
            }
            Err(_) => {
                idle_timeouts += 1;
                if idle_timeouts >= 3 {
                    break;
                }
                continue;
            }
        };

        let Some(maybe_msg) = maybe_msg else {
            break;
        };

        let message = match maybe_msg {
            Ok(message) => message,
            Err(err) => {
                warn!(err = %err, "dead-letter replay stream yielded error");
                continue;
            }
        };

        match serde_json::from_slice::<DeadLetterEnvelope>(&message.payload) {
            Ok(envelope) => {
                if envelope.original_subject.trim().is_empty() {
                    warn!("dead-letter envelope missing original subject; dropping");
                    let _ = message.ack_with(AckKind::Term).await;
                    continue;
                }

                let payload_bytes =
                    serde_json::to_vec(&envelope.payload).context("serialize replay payload")?;
                let replay_id = if envelope.command_id.trim().is_empty() {
                    format!("replay-{}", Uuid::new_v4())
                } else {
                    format!("replay-{}-{}", envelope.command_id, unix_millis())
                };

                let publish_ack = jetstream
                    .send_publish(
                        envelope.original_subject.clone(),
                        jetstream::context::Publish::build()
                            .message_id(replay_id)
                            .payload(payload_bytes.into()),
                    )
                    .await
                    .context("publish replay command")?;
                publish_ack.await.context("await replay publish ack")?;

                let replay_event = json!({
                    "replayed_at_unix_ms": unix_millis(),
                    "dead_letter_id": envelope.dead_letter_id,
                    "command_id": envelope.command_id,
                    "original_subject": envelope.original_subject,
                });
                let replay_event_payload =
                    serde_json::to_vec(&replay_event).context("serialize replay event payload")?;
                let event_ack = jetstream
                    .publish(config.replay_subject.clone(), replay_event_payload.into())
                    .await
                    .context("publish replay event")?;
                event_ack.await.context("await replay event ack")?;

                message
                    .ack()
                    .await
                    .map_err(|err| anyhow!("ack dead-letter message: {err}"))?;
                replayed += 1;
            }
            Err(err) => {
                warn!(err = %err, "invalid dead-letter payload; terminating message");
                let _ = message.ack_with(AckKind::Term).await;
            }
        }
    }

    Ok(replayed)
}

async fn connect_nats(
    url: &str,
    auth_token: Option<&str>,
    context: &'static str,
) -> Result<async_nats::Client> {
    let token = auth_token.map(str::trim).filter(|value| !value.is_empty());
    match token {
        Some(token) => {
            async_nats::ConnectOptions::with_token(token.to_string())
                .connect(url.to_string())
                .await
        }
        None => async_nats::connect(url.to_string()).await,
    }
    .with_context(|| context)
}

#[allow(clippy::too_many_arguments)]
async fn process_command_message(
    message: jetstream::Message,
    config: &ControlBusConfig,
    node_id: &str,
    manager: &std::sync::Arc<Manager>,
    dedupe_store: Option<&EtcdDedupeStore>,
    fence_store: Option<&EtcdOwnershipFenceStore>,
    partition_gate: Option<&PartitionGate>,
    seen_commands: &std::sync::Arc<Mutex<HashMap<String, Instant>>>,
    active_commands: &std::sync::Arc<Mutex<HashMap<String, Instant>>>,
    completed_commands: &std::sync::Arc<Mutex<HashMap<String, Instant>>>,
    active_exec_streams: &std::sync::Arc<Mutex<HashMap<String, ActiveExecStream>>>,
    vm_readiness_state: &VmReadinessState,
    vm_exec_activity_state: &VmExecActivityState,
    vm_snapshot_state: &VmSnapshotState,
    reconcile_trigger: Option<&mpsc::UnboundedSender<()>>,
    jetstream: &jetstream::Context,
    dedupe_ttl: Duration,
) {
    let dispatch_start = tokio::time::Instant::now();
    let parsed = serde_json::from_slice::<CommandEnvelope>(&message.payload);
    let envelope = match parsed {
        Ok(envelope) => envelope,
        Err(err) => {
            handle_failed_command_message(
                &message,
                config,
                node_id,
                jetstream,
                "invalid_envelope",
                &err.to_string(),
            )
            .await;
            return;
        }
    };

    if let Some(target_node_id) = envelope
        .target_node_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if target_node_id != node_id {
            // @dive: Node-target filtering must happen before dedupe/fence CAS so non-owner consumers never steal ownership transitions.
            //        Targeted commands use a shared JetStream consumer, so a wrong-node delivery must be redelivered instead of acked.
            debug!(
                node_id = %node_id,
                target_node_id = %target_node_id,
                command_id = %envelope.command_id,
                command_type = %envelope.command_type,
                "skipping control command targeted at a different node"
            );
            if let Err(err) = message
                .ack_with(AckKind::Nak(Some(Duration::from_millis(250))))
                .await
            {
                warn!(
                    node_id = %node_id,
                    command_id = %envelope.command_id,
                    err = %err,
                    "failed to nack command targeted at different node"
                );
            }
            return;
        }
    }

    if let Some(gate) = partition_gate {
        if !gate.mutation_allowed().await {
            let reason = gate.mutation_rejection_reason().await.unwrap_or_else(|| {
                "network partition fail-closed: rejecting mutating commands".to_string()
            });
            warn!(
                node_id = %node_id,
                subject = %message.subject,
                command_id = %envelope.command_id,
                command_type = %envelope.command_type,
                reason = %reason,
                "rejecting control command while quorum visibility is lost"
            );
            if let Err(err) = message
                .ack_with(AckKind::Nak(Some(gate.command_retry_delay())))
                .await
            {
                warn!(
                    node_id = %node_id,
                    command_id = %envelope.command_id,
                    err = %err,
                    "failed to nack control command during partition fail-closed enforcement"
                );
            }
            return;
        }
    }

    let now = Instant::now();
    let delivery_attempt = message
        .info()
        .map(|info| info.delivered)
        .unwrap_or(1)
        .max(1);
    let is_redelivery = delivery_attempt > 1;

    let dedupe_key = dedupe_key(&envelope);
    if !is_redelivery {
        if let Some(store) = dedupe_store {
            match store.mark_or_duplicate(&dedupe_key).await {
                Ok(true) => {
                    debug!(
                        node_id = %node_id,
                        subject = %message.subject,
                        command_id = %envelope.command_id,
                        idempotency_key = %dedupe_key,
                        "dropping duplicate control command (etcd dedupe)"
                    );
                    if let Err(err) = message.ack().await {
                        warn!(
                            node_id = %node_id,
                            command_id = %envelope.command_id,
                            idempotency_key = %dedupe_key,
                            err = %err,
                            "failed to ack duplicate command after etcd dedupe"
                        );
                    }
                    return;
                }
                Ok(false) => {}
                Err(err) => {
                    warn!(
                        node_id = %node_id,
                        subject = %message.subject,
                        idempotency_key = %dedupe_key,
                        err = %err,
                        "etcd dedupe check failed; falling back to local dedupe"
                    );
                }
            }
        }

        {
            let mut seen = seen_commands.lock().await;
            seen.retain(|_, ts| now.duration_since(*ts) < dedupe_ttl);
            if seen.contains_key(&dedupe_key) {
                debug!(
                    node_id = %node_id,
                    subject = %message.subject,
                    command_id = %envelope.command_id,
                    idempotency_key = %dedupe_key,
                    "dropping duplicate control command"
                );
                if let Err(err) = message.ack().await {
                    warn!(
                        node_id = %node_id,
                        command_id = %envelope.command_id,
                        idempotency_key = %dedupe_key,
                        err = %err,
                        "failed to ack duplicate command after local dedupe"
                    );
                }
                return;
            }
            seen.insert(dedupe_key.clone(), now);
        }
    } else {
        match command_completed(dedupe_store, completed_commands, &dedupe_key, dedupe_ttl).await {
            Ok(true) => {
                debug!(
                    node_id = %node_id,
                    subject = %message.subject,
                    command_id = %envelope.command_id,
                    idempotency_key = %dedupe_key,
                    delivery_attempt = delivery_attempt,
                    "dropping completed control command redelivery"
                );
                if let Err(err) = message.ack().await {
                    warn!(
                        node_id = %node_id,
                        command_id = %envelope.command_id,
                        idempotency_key = %dedupe_key,
                        err = %err,
                        "failed to ack completed command redelivery"
                    );
                }
                return;
            }
            Ok(false) => {}
            Err(err) => {
                warn!(
                    node_id = %node_id,
                    subject = %message.subject,
                    command_id = %envelope.command_id,
                    idempotency_key = %dedupe_key,
                    err = %err,
                    "completed command marker check failed; processing redelivery"
                );
            }
        }
        // @dive: Broker redeliveries must bypass duplicate suppression so failed commands can exhaust retry budget and reach DLQ.
        debug!(
            node_id = %node_id,
            subject = %message.subject,
            command_id = %envelope.command_id,
            idempotency_key = %dedupe_key,
            delivery_attempt = delivery_attempt,
            "processing control command redelivery without dedupe short-circuit"
        );
    }

    let needs_single_owner = command_needs_single_owner(envelope.command_type.as_str());
    let _distributed_command_in_flight = if needs_single_owner {
        if let Some(store) = dedupe_store {
            let owner = format!(
                "{}:{}:{}",
                node_id,
                envelope.command_id.trim(),
                Uuid::new_v4()
            );
            match store
                .acquire_inflight(dedupe_key.as_str(), owner.as_str(), dedupe_ttl)
                .await
            {
                Ok(Some(guard)) => Some(guard),
                Ok(None) => {
                    warn!(
                        node_id = %node_id,
                        subject = %message.subject,
                        command_id = %envelope.command_id,
                        idempotency_key = %dedupe_key,
                        delivery_attempt = delivery_attempt,
                        "control command already owned by another in-flight handler"
                    );
                    if let Err(err) = message.ack_with(AckKind::Progress).await {
                        warn!(
                            node_id = %node_id,
                            command_id = %envelope.command_id,
                            idempotency_key = %dedupe_key,
                            err = %err,
                            "failed to progress-ack distributed in-flight command"
                        );
                    }
                    return;
                }
                Err(err) => {
                    warn!(
                        node_id = %node_id,
                        subject = %message.subject,
                        command_id = %envelope.command_id,
                        idempotency_key = %dedupe_key,
                        err = %err,
                        "distributed in-flight command acquire failed; falling back to local guard"
                    );
                    None
                }
            }
        } else {
            None
        }
    } else {
        None
    };

    let _command_in_flight = if needs_single_owner {
        match CommandInFlightGuard::acquire(
            std::sync::Arc::clone(active_commands),
            &dedupe_key,
            dedupe_ttl,
        )
        .await
        {
            Some(guard) => Some(guard),
            None => {
                warn!(
                    node_id = %node_id,
                    subject = %message.subject,
                    command_id = %envelope.command_id,
                    idempotency_key = %dedupe_key,
                    delivery_attempt = delivery_attempt,
                    "control command redelivered while original handler is still in flight"
                );
                if let Err(err) = message.ack_with(AckKind::Progress).await {
                    warn!(
                        node_id = %node_id,
                        command_id = %envelope.command_id,
                        idempotency_key = %dedupe_key,
                        err = %err,
                        "failed to progress-ack in-flight command redelivery"
                    );
                }
                return;
            }
        }
    } else {
        None
    };

    if let Some(store) = fence_store {
        if let Err(err) = store
            .check_session_fence(
                command_session_id(&envelope),
                envelope.expected_fence.as_deref(),
            )
            .await
        {
            handle_failed_command_message(
                &message,
                config,
                node_id,
                jetstream,
                "ownership_fence_conflict",
                &err.to_string(),
            )
            .await;
            return;
        }
    }

    // @dive: Long VM recovery can legitimately exceed JetStream ack_wait. Progress
    //        heartbeats prevent the broker from redelivering the same command while
    //        this handler still owns restore/start/readiness work.
    let _ack_progress = CommandAckProgress::start(
        message.clone(),
        envelope.command_id.clone(),
        envelope.command_type.clone(),
        node_id.to_string(),
        config.command_ack_wait_ms,
    );

    debug!(
        node_id = %node_id,
        subject = %message.subject,
        command_id = %envelope.command_id,
        idempotency_key = %dedupe_key,
        command_type = %envelope.command_type,
        ordering_key = %envelope.ordering_key,
        "received control command"
    );
    info!(
        command_id = %envelope.command_id,
        command_type = %envelope.command_type,
        dispatch_prep_ms = dispatch_start.elapsed().as_millis() as u64,
        "DISPATCH_PREP dispatcher ready to call handler"
    );

    if envelope.command_type == "reconcile.run" {
        if let Some(tx) = reconcile_trigger {
            if tx.send(()).is_err() {
                warn!(
                    node_id = %node_id,
                    command_id = %envelope.command_id,
                    "reconcile trigger receiver dropped"
                );
            }
        }
    } else if envelope.command_type == "exec.run" {
        if let Err(err) = handle_exec_run_command(
            &envelope,
            config,
            node_id,
            manager.as_ref(),
            vm_readiness_state,
            jetstream,
        )
        .await
        {
            let error_text = err.to_string();
            match publish_exec_run_error_result(&envelope, config, node_id, jetstream, &error_text)
                .await
            {
                Ok(()) => {
                    if let Err(mark_err) = mark_command_completed(
                        dedupe_store,
                        completed_commands,
                        &dedupe_key,
                        dedupe_ttl,
                    )
                    .await
                    {
                        warn!(
                            node_id = %node_id,
                            command_id = %envelope.command_id,
                            idempotency_key = %dedupe_key,
                            err = %mark_err,
                            "failed to mark exec.run error result complete before ack"
                        );
                    }
                    if let Err(ack_err) = message.ack().await {
                        warn!(
                            node_id = %node_id,
                            command_id = %envelope.command_id,
                            err = %ack_err,
                            "failed to ack exec.run command after publishing error result"
                        );
                    }
                }
                Err(publish_err) => {
                    warn!(
                        node_id = %node_id,
                        command_id = %envelope.command_id,
                        exec_error = %error_text,
                        publish_err = %publish_err,
                        "failed to publish exec.run error result; falling back to retry handling"
                    );
                    handle_failed_command_message(
                        &message,
                        config,
                        node_id,
                        jetstream,
                        "exec_run_failed",
                        &error_text,
                    )
                    .await;
                }
            }
            return;
        }
    } else if envelope.command_type == "exec.stream.start" {
        if let Err(err) = handle_exec_stream_start_command(
            &envelope,
            config,
            node_id,
            manager,
            active_exec_streams,
            vm_readiness_state,
            vm_exec_activity_state,
            vm_snapshot_state,
            jetstream,
        )
        .await
        {
            // @dive: Only surface handler failure as a synthetic stream error event on
            //        the *final* delivery attempt. Intermediate NAK retries are routine
            //        — a transient attach_daemon hang during a snapshot-overlap window
            //        will redeliver and often succeed. Publishing an error per attempt
            //        races the next successful delivery and makes the API client mark
            //        the stream dead before its real "started" event arrives.
            let delivered = message
                .info()
                .map(|info| info.delivered)
                .unwrap_or(1)
                .max(1);
            let max_deliver = config.command_max_deliver.max(1);
            if delivered >= max_deliver {
                publish_exec_stream_handler_failure_event(
                    &envelope,
                    config,
                    node_id,
                    jetstream,
                    &err.to_string(),
                )
                .await;
            }
            handle_failed_command_message(
                &message,
                config,
                node_id,
                jetstream,
                "exec_stream_start_failed",
                &err.to_string(),
            )
            .await;
            return;
        }
    } else if envelope.command_type == "exec.stream.input"
        || envelope.command_type == "exec.stream.control"
    {
        if let Err(err) = handle_exec_stream_input_command(&envelope, active_exec_streams).await {
            handle_failed_command_message(
                &message,
                config,
                node_id,
                jetstream,
                "exec_stream_input_failed",
                &err.to_string(),
            )
            .await;
            return;
        }
    }

    if let Err(mark_err) =
        mark_command_completed(dedupe_store, completed_commands, &dedupe_key, dedupe_ttl).await
    {
        warn!(
            node_id = %node_id,
            command_id = %envelope.command_id,
            idempotency_key = %dedupe_key,
            err = %mark_err,
            "failed to mark control command complete before ack"
        );
    }

    if let Err(err) = message.ack().await {
        warn!(
            node_id = %node_id,
            command_id = %envelope.command_id,
            err = %err,
            "failed to ack control command"
        );
    }
}

async fn command_completed(
    dedupe_store: Option<&EtcdDedupeStore>,
    completed_commands: &std::sync::Arc<Mutex<HashMap<String, Instant>>>,
    dedupe_key: &str,
    dedupe_ttl: Duration,
) -> Result<bool> {
    {
        let now = Instant::now();
        let mut completed = completed_commands.lock().await;
        completed.retain(|_, ts| now.duration_since(*ts) < dedupe_ttl);
        if completed.contains_key(dedupe_key) {
            return Ok(true);
        }
    }
    if let Some(store) = dedupe_store {
        return store.is_completed(dedupe_key).await;
    }
    Ok(false)
}

async fn mark_command_completed(
    dedupe_store: Option<&EtcdDedupeStore>,
    completed_commands: &std::sync::Arc<Mutex<HashMap<String, Instant>>>,
    dedupe_key: &str,
    dedupe_ttl: Duration,
) -> Result<()> {
    {
        let now = Instant::now();
        let mut completed = completed_commands.lock().await;
        completed.retain(|_, ts| now.duration_since(*ts) < dedupe_ttl);
        completed.insert(dedupe_key.to_string(), now);
    }
    if let Some(store) = dedupe_store {
        store.mark_completed(dedupe_key).await?;
    }
    Ok(())
}

#[tracing::instrument(
    skip(envelope, config, manager, jetstream),
    fields(command_id = %envelope.command_id, command_type = %envelope.command_type, node_id = %node_id)
)]
async fn handle_exec_run_command(
    envelope: &CommandEnvelope,
    config: &ControlBusConfig,
    node_id: &str,
    manager: &Manager,
    vm_readiness_state: &VmReadinessState,
    jetstream: &jetstream::Context,
) -> Result<()> {
    let payload: ExecRunPayload = serde_json::from_value(envelope.payload.clone())
        .context("decode exec.run command payload")?;
    if payload.vm_id.trim().is_empty() {
        return Err(anyhow!("exec.run payload missing vm_id"));
    }
    if payload.command.trim().is_empty() {
        return Err(anyhow!("exec.run payload missing command"));
    }

    let ready_timeout = guest_exec_ready_timeout_for_start(manager, payload.vm_id.as_str()).await;
    let _vm_readiness_guard = vm_readiness_state
        .lock_with_timeout(
            payload.vm_id.as_str(),
            readiness_gate_wait_timeout(ready_timeout),
        )
        .await?;
    fail_if_vm_already_error(manager, payload.vm_id.as_str(), "exec.run").await?;
    let vm = manager
        .start_vm(payload.vm_id.as_str())
        .await
        .map_err(|err| anyhow!("ensure vm running for exec.run: {err}"))?;
    let rpc_port = vm.network.rpc_port;
    if rpc_port <= 0 {
        return Err(anyhow!(
            "exec.run vm {} missing rpc_port after start",
            payload.vm_id
        ));
    }
    let portproxy_auth = portproxy_auth_header_from_metadata(&vm.metadata)?;
    let endpoint = format!("http://127.0.0.1:{rpc_port}");
    wait_for_guest_exec_ready_or_mark_vm_error(
        manager,
        payload.vm_id.as_str(),
        endpoint.as_str(),
        portproxy_auth.as_ref(),
        ready_timeout,
        "wait for guest exec readiness before exec.run",
    )
    .await?;
    drop(_vm_readiness_guard);
    let mut client = ShellExecClient::connect(endpoint)
        .await
        .context("connect shell exec client for exec.run")?;

    let shell = payload.shell.unwrap_or_else(|| "/bin/sh".to_string());
    let (req_tx, req_rx) = mpsc::channel(8);
    req_tx
        .send(ExecRequest {
            request: Some(exec_request::Request::Start(ExecStart {
                execution_id: String::new(),
                args: vec![shell, "-lc".to_string(), payload.command.clone()],
                env: payload.env,
                detach: payload.detach,
                timeout: payload.timeout_secs,
            })),
        })
        .await
        .map_err(|_| anyhow!("enqueue exec.run start request"))?;
    drop(req_tx);

    let response = client
        .exec(request_with_portproxy_auth(
            ReceiverStream::new(req_rx),
            portproxy_auth.as_ref(),
        ))
        .await
        .context("invoke shell exec stream for exec.run")?;
    let mut stream = response.into_inner();

    let mut stdout = BoundedTextOutput::default();
    let mut stderr = BoundedTextOutput::default();
    let mut exit_code = None;
    let mut timed_out = false;

    while let Some(frame) = stream
        .message()
        .await
        .context("read exec.run stream frame")?
    {
        match frame {
            ExecResponse {
                response: Some(exec_response::Response::StdoutData(bytes)),
            } => {
                stdout.push_lossy(&bytes);
            }
            ExecResponse {
                response: Some(exec_response::Response::StderrData(bytes)),
            } => {
                stderr.push_lossy(&bytes);
            }
            ExecResponse {
                response: Some(exec_response::Response::ExitCode(code)),
            } => {
                if code == 124 {
                    timed_out = true;
                }
                exit_code = Some(code);
                break;
            }
            ExecResponse { response: None } => {}
        }
    }

    let command_id = if envelope.command_id.trim().is_empty() {
        Uuid::new_v4().to_string()
    } else {
        envelope.command_id.clone()
    };
    let event_subject = format!(
        "{}.evt.exec.result.{}",
        config.subject_prefix,
        sanitize_subject_token(command_id.as_str())
    );
    // @dive: Command-scoped result subjects let facade waits filter exact exec completions without scanning shared event traffic.
    let result = ExecRunResult {
        command_id,
        session_id: payload.session_id,
        vm_id: payload.vm_id,
        stdout: stdout.value,
        stderr: stderr.value,
        stdout_truncated: stdout.truncated,
        stderr_truncated: stderr.truncated,
        exit_code,
        timed_out,
        error: None,
        executed_by_node_id: node_id.to_string(),
        completed_at_unix_ms: unix_millis(),
    };
    let bytes = serde_json::to_vec(&result).context("serialize exec.run result event")?;
    let publish_ack = jetstream
        .publish(event_subject, bytes.into())
        .await
        .context("publish exec.run result event")?;
    publish_ack
        .await
        .context("await exec.run result event publish ack")?;
    Ok(())
}

async fn publish_exec_run_error_result(
    envelope: &CommandEnvelope,
    config: &ControlBusConfig,
    node_id: &str,
    jetstream: &jetstream::Context,
    error: &str,
) -> Result<()> {
    let payload: ExecRunPayload = serde_json::from_value(envelope.payload.clone())
        .context("decode exec.run error payload")?;
    let command_id = if envelope.command_id.trim().is_empty() {
        Uuid::new_v4().to_string()
    } else {
        envelope.command_id.clone()
    };
    let event_subject = format!(
        "{}.evt.exec.result.{}",
        config.subject_prefix,
        sanitize_subject_token(command_id.as_str())
    );
    let result = ExecRunResult {
        command_id,
        session_id: payload.session_id,
        vm_id: payload.vm_id,
        stdout: String::new(),
        stderr: String::new(),
        stdout_truncated: false,
        stderr_truncated: false,
        exit_code: None,
        timed_out: false,
        error: Some(error.to_string()),
        executed_by_node_id: node_id.to_string(),
        completed_at_unix_ms: unix_millis(),
    };
    let bytes = serde_json::to_vec(&result).context("serialize exec.run error result event")?;
    let publish_ack = jetstream
        .publish(event_subject, bytes.into())
        .await
        .context("publish exec.run error result event")?;
    publish_ack
        .await
        .context("await exec.run error result publish ack")?;
    Ok(())
}

#[tracing::instrument(
    skip(envelope, config, manager, active_exec_streams, vm_snapshot_state, jetstream),
    fields(command_id = %envelope.command_id, command_type = %envelope.command_type, node_id = %node_id)
)]
#[allow(clippy::too_many_arguments)]
async fn handle_exec_stream_start_command(
    envelope: &CommandEnvelope,
    config: &ControlBusConfig,
    node_id: &str,
    manager: &std::sync::Arc<Manager>,
    active_exec_streams: &std::sync::Arc<Mutex<HashMap<String, ActiveExecStream>>>,
    vm_readiness_state: &VmReadinessState,
    vm_exec_activity_state: &VmExecActivityState,
    vm_snapshot_state: &VmSnapshotState,
    jetstream: &jetstream::Context,
) -> Result<()> {
    let payload: ExecStreamStartPayload = serde_json::from_value(envelope.payload.clone())
        .context("decode exec.stream.start command payload")?;
    let logical_stream_id = if payload.logical_stream_id.trim().is_empty() {
        payload.stream_id.trim().to_string()
    } else {
        payload.logical_stream_id.trim().to_string()
    };
    if logical_stream_id.is_empty() {
        return Err(anyhow!(
            "exec.stream.start payload missing logical_stream_id/stream_id"
        ));
    }
    if payload.vm_id.trim().is_empty() {
        return Err(anyhow!("exec.stream.start payload missing vm_id"));
    }
    let resume_only = payload.resume_after_event_seq > 0;
    if payload.command.trim().is_empty() && !resume_only {
        return Err(anyhow!("exec.stream.start payload missing command"));
    }
    let stream_id = logical_stream_id.clone();
    let cluster_id = if payload.cluster_id.trim().is_empty() {
        config.cluster_id.clone()
    } else {
        payload.cluster_id.trim().to_string()
    };
    let producer_epoch = payload.producer_epoch;
    let command_id = if envelope.command_id.trim().is_empty() {
        Uuid::new_v4().to_string()
    } else {
        envelope.command_id.clone()
    };
    let session_id = payload.session_id.clone();
    let vm_id = payload.vm_id.clone();
    let mut sequence = payload.resume_after_event_seq;

    let handler_start = tokio::time::Instant::now();
    info!(stream_id = %stream_id, command_id = %command_id, vm_id = %payload.vm_id, "STREAMSTART_T1 handler_entry");

    {
        let guard = active_exec_streams.lock().await;
        if guard.contains_key(&stream_id) {
            // @dive: Resume commands are idempotent by logical stream id; if producer is already active on this node we keep it and avoid rerun.
            info!(
                stream_id = %stream_id,
                command_id = %command_id,
                producer_epoch = producer_epoch,
                "exec.stream.start ignored because stream is already active"
            );
            return Ok(());
        }
    }
    info!(stream_id = %stream_id, elapsed_ms = handler_start.elapsed().as_millis() as u64, "STREAMSTART_T2 after_active_streams_check");

    // Track exec intent before taking the readiness gate. Background
    // execution-restore refreshes use this to yield to back-to-back tool calls,
    // including calls that are still waiting for guest readiness and are not yet
    // present in `active_exec_streams`.
    let exec_activity_lease = vm_exec_activity_state.begin(vm_id.as_str());
    let mut ready_timeout =
        guest_exec_ready_timeout_for_start(manager.as_ref(), payload.vm_id.as_str()).await;
    let cached_ready_vm = cached_ready_running_vm(
        manager.as_ref(),
        vm_readiness_state,
        payload.vm_id.as_str(),
        resume_only,
    )
    .await;
    let cached_ready_used = cached_ready_vm.is_some();
    if cached_ready_used {
        info!(
            stream_id = %stream_id,
            vm_id = %vm_id,
            elapsed_ms = handler_start.elapsed().as_millis() as u64,
            "STREAMSTART_T2A cached_ready_vm"
        );
    }
    let vm_readiness_guard = if cached_ready_used {
        None
    } else {
        Some(
            match vm_readiness_state
                .lock_with_timeout(
                    payload.vm_id.as_str(),
                    readiness_gate_wait_timeout(ready_timeout),
                )
                .await
            {
                Ok(guard) => guard,
                Err(error) => {
                    let error_text = error.to_string();
                    spawn_mark_vm_guest_exec_unhealthy(
                        std::sync::Arc::clone(manager),
                        payload.vm_id.clone(),
                        error_text.clone(),
                    );
                    sequence = sequence.saturating_add(1);
                    publish_exec_stream_event(
                        config,
                        jetstream,
                        ExecStreamEvent {
                            cluster_id: cluster_id.clone(),
                            logical_stream_id: logical_stream_id.clone(),
                            stream_id: stream_id.clone(),
                            event_seq: sequence,
                            event_id: next_stream_event_id(cluster_id.as_str()),
                            producer_epoch,
                            command_id: command_id.clone(),
                            session_id: session_id.clone(),
                            vm_id: vm_id.clone(),
                            kind: "error".to_string(),
                            data: Vec::new(),
                            exit_code: None,
                            timed_out: false,
                            error: Some(error_text),
                            sequence,
                            emitted_by_node_id: node_id.to_string(),
                            emitted_at_unix_ms: unix_millis(),
                        },
                    )
                    .await?;
                    drop(exec_activity_lease);
                    return Ok(());
                }
            },
        )
    };
    if let Err(error) = fail_if_vm_already_error(
        manager.as_ref(),
        payload.vm_id.as_str(),
        "exec.stream.start",
    )
    .await
    {
        let error_text = error.to_string();
        sequence = sequence.saturating_add(1);
        publish_exec_stream_event(
            config,
            jetstream,
            ExecStreamEvent {
                cluster_id: cluster_id.clone(),
                logical_stream_id: logical_stream_id.clone(),
                stream_id: stream_id.clone(),
                event_seq: sequence,
                event_id: next_stream_event_id(cluster_id.as_str()),
                producer_epoch,
                command_id: command_id.clone(),
                session_id: session_id.clone(),
                vm_id: vm_id.clone(),
                kind: "error".to_string(),
                data: Vec::new(),
                exit_code: None,
                timed_out: false,
                error: Some(error_text),
                sequence,
                emitted_by_node_id: node_id.to_string(),
                emitted_at_unix_ms: unix_millis(),
            },
        )
        .await?;
        return Ok(());
    }

    let mut vm = if let Some(vm) = cached_ready_vm {
        vm
    } else if resume_only {
        manager
            .start_vm_for_exec_stream_resume(payload.vm_id.as_str())
            .await
            .map_err(|err| anyhow!("ensure vm running for exec.stream.resume: {err}"))?
    } else {
        manager
            .start_vm(payload.vm_id.as_str())
            .await
            .map_err(|err| anyhow!("ensure vm running for exec.stream.start: {err}"))?
    };
    info!(stream_id = %stream_id, elapsed_ms = handler_start.elapsed().as_millis() as u64, "STREAMSTART_T3 after_manager_start_vm");
    let mut vm_metadata = vm.metadata.clone();
    let tier_b_eligible = metadata_tier_b_eligible(&vm_metadata);
    if resume_only && tier_b_eligible {
        if let Some(snapshot_id) = resolve_execution_restore_snapshot_id(
            manager.as_ref(),
            payload.vm_id.as_str(),
            &vm_metadata,
        )
        .await?
        {
            match manager
                .restore_snapshot(payload.vm_id.as_str(), snapshot_id.as_str())
                .await
            {
                Ok(restored_vm) => {
                    vm = restored_vm;
                    vm_metadata = vm.metadata.clone();
                    ready_timeout = GUEST_EXEC_READY_TIMEOUT;
                    info!(
                        vm_id = %vm_id,
                        stream_id = %stream_id,
                        snapshot_id = %snapshot_id,
                        "restored tier-b execution snapshot for exec stream resume"
                    );
                }
                Err(err) => {
                    // @dive: Resume attach remains no-rerun; restore failure must terminate stream instead of replaying command.
                    sequence = sequence.saturating_add(1);
                    publish_exec_stream_event(
                        config,
                        jetstream,
                        ExecStreamEvent {
                            cluster_id: cluster_id.clone(),
                            logical_stream_id: logical_stream_id.clone(),
                            stream_id: stream_id.clone(),
                            event_seq: sequence,
                            event_id: next_stream_event_id(cluster_id.as_str()),
                            producer_epoch,
                            command_id: command_id.clone(),
                            session_id: session_id.clone(),
                            vm_id: vm_id.clone(),
                            kind: "error".to_string(),
                            data: Vec::new(),
                            exit_code: None,
                            timed_out: false,
                            error: Some(format!(
                                "tier-b execution snapshot restore failed for exec stream resume: {err}"
                            )),
                            sequence,
                            emitted_by_node_id: node_id.to_string(),
                            emitted_at_unix_ms: unix_millis(),
                        },
                    )
                    .await?;
                    return Ok(());
                }
            }
        } else {
            // @dive: Active stream resume is attach-only, so a missing restore
            // marker can still recover by live-reclaiming the running VM. If
            // the daemon target is absent, the attach path below emits the
            // terminal no-rerun error.
            info!(
                vm_id = %vm_id,
                stream_id = %stream_id,
                "tier-b exec stream resume has no restore snapshot marker; trying live attach"
            );
        }
    }
    let rpc_port = vm.network.rpc_port;
    if rpc_port <= 0 {
        return Err(anyhow!(
            "exec.stream.start vm {} missing rpc_port after start",
            payload.vm_id
        ));
    }
    let portproxy_auth = portproxy_auth_header_from_metadata(&vm_metadata)?;
    let endpoint = format!("http://127.0.0.1:{rpc_port}");
    if vm_readiness_guard.is_some() {
        if let Err(error) = wait_for_guest_exec_ready_or_mark_vm_error(
            manager.as_ref(),
            payload.vm_id.as_str(),
            endpoint.as_str(),
            portproxy_auth.as_ref(),
            ready_timeout,
            "wait for guest exec readiness before exec.stream.start",
        )
        .await
        {
            let error_text = error.to_string();
            sequence = sequence.saturating_add(1);
            publish_exec_stream_event(
                config,
                jetstream,
                ExecStreamEvent {
                    cluster_id: cluster_id.clone(),
                    logical_stream_id: logical_stream_id.clone(),
                    stream_id: stream_id.clone(),
                    event_seq: sequence,
                    event_id: next_stream_event_id(cluster_id.as_str()),
                    producer_epoch,
                    command_id: command_id.clone(),
                    session_id: session_id.clone(),
                    vm_id: vm_id.clone(),
                    kind: "error".to_string(),
                    data: Vec::new(),
                    exit_code: None,
                    timed_out: false,
                    error: Some(error_text),
                    sequence,
                    emitted_by_node_id: node_id.to_string(),
                    emitted_at_unix_ms: unix_millis(),
                },
            )
            .await?;
            return Ok(());
        }
        let expected_portproxy_sha = portproxy::binary_sha256_hex(vm.architecture.as_str())
            .context("hash bundled portproxy")?;
        let bundled_portproxy =
            portproxy::binary(vm.architecture.as_str()).context("load bundled portproxy")?;
        let upgraded_portproxy = ensure_guest_portproxy_binary(
            endpoint.as_str(),
            portproxy_auth.as_ref(),
            vm_metadata
                .get(META_PORTPROXY_AUTH_TOKEN)
                .map(String::as_str),
            expected_portproxy_sha.as_str(),
            bundled_portproxy.as_slice(),
        )
        .await
        .context("ensure guest portproxy binary matches bundled runtime")?;
        if upgraded_portproxy {
            info!(
                stream_id = %stream_id,
                vm_id = %vm_id,
                expected_sha256 = %expected_portproxy_sha,
                "upgraded guest portproxy binary before exec stream"
            );
        }
        if !resume_only {
            if let Some(fingerprint) = VmReadyFingerprint::from_metadata(&vm) {
                vm_readiness_state
                    .mark_ready(payload.vm_id.as_str(), fingerprint)
                    .await;
            }
        }
    }
    drop(vm_readiness_guard);
    info!(stream_id = %stream_id, endpoint = %endpoint, elapsed_ms = handler_start.elapsed().as_millis() as u64, "STREAMSTART_T4 before_daemon_connect");
    let mut daemon_client = match DaemonManagerClient::connect(endpoint.clone()).await {
        Ok(client) => client,
        Err(error) => {
            if cached_ready_used {
                vm_readiness_state.clear_ready(payload.vm_id.as_str()).await;
            }
            return Err(error).context("connect daemon manager client for exec.stream.start");
        }
    };
    info!(stream_id = %stream_id, elapsed_ms = handler_start.elapsed().as_millis() as u64, "STREAMSTART_T5 after_daemon_connect");
    let daemon_name = format!(
        "chevalier-exec-stream-{}",
        sanitize_subject_token(&stream_id)
    );
    if !resume_only {
        let shell = payload
            .shell
            .clone()
            .unwrap_or_else(|| "/bin/sh".to_string());
        if let Err(error) = daemon_client
            .exec_daemon(request_with_portproxy_auth(
                ExecDaemonRequest {
                    name: daemon_name.clone(),
                    args: vec![shell, "-lc".to_string(), payload.command.clone()],
                    env: payload.env.clone(),
                    timeout: payload.timeout_secs,
                    detach: payload.detach,
                },
                portproxy_auth.as_ref(),
            ))
            .await
        {
            if cached_ready_used {
                vm_readiness_state.clear_ready(payload.vm_id.as_str()).await;
            }
            return Err(error).context("invoke daemon manager for exec.stream.start");
        }
        info!(stream_id = %stream_id, elapsed_ms = handler_start.elapsed().as_millis() as u64, "STREAMSTART_T6 after_exec_daemon_rpc");
    }

    // @dive: No wait-gate against the post-exit snapshot worker. The snapshot
    //        is a background process: qemu's userfaultfd-WP migration drains
    //        WP faults concurrently with guest execution, and we stage the RAM
    //        write to local SSD so the migration thread is never blocked on
    //        slow Filestore IO. attach_daemon talks to guest gRPC over vsock,
    //        which is unaffected by the snapshot. Earlier wedges were caused
    //        by qemu blocking on Filestore during migrate (since fixed by the
    //        staging dir) — the wait-gate was a redundant belt that turned
    //        into a 60s hang waiting for the unrelated Filestore promotion.

    let attach_retry_deadline = if resume_only {
        Some(tokio::time::Instant::now() + Duration::from_secs(20))
    } else {
        None
    };
    let (response, req_tx) = loop {
        info!(stream_id = %stream_id, elapsed_ms = handler_start.elapsed().as_millis() as u64, "STREAMSTART_T7 before_attach_connect");
        let mut attach_client = match DaemonManagerClient::connect(endpoint.clone()).await {
            Ok(client) => client,
            Err(error) => {
                if cached_ready_used {
                    vm_readiness_state.clear_ready(payload.vm_id.as_str()).await;
                }
                return Err(error).context("connect daemon manager client for exec.stream attach");
            }
        };
        info!(stream_id = %stream_id, elapsed_ms = handler_start.elapsed().as_millis() as u64, "STREAMSTART_T8 after_attach_connect");
        let (req_tx, req_rx) = mpsc::channel(64);
        req_tx
            .send(AttachDaemonRequest {
                request: Some(attach_daemon_request::Request::Start(AttachDaemonStart {
                    name: daemon_name.clone(),
                })),
            })
            .await
            .map_err(|_| anyhow!("enqueue exec.stream attach start request"))?;
        info!(stream_id = %stream_id, elapsed_ms = handler_start.elapsed().as_millis() as u64, "STREAMSTART_T9 before_attach_daemon_rpc");
        match attach_client
            .attach_daemon(request_with_portproxy_auth(
                ReceiverStream::new(req_rx),
                portproxy_auth.as_ref(),
            ))
            .await
        {
            Ok(response) => {
                info!(stream_id = %stream_id, elapsed_ms = handler_start.elapsed().as_millis() as u64, "STREAMSTART_T10 after_attach_daemon_rpc_ok");
                break (response, req_tx);
            }
            Err(status) => {
                if resume_only && status.code() == tonic::Code::NotFound {
                    if let Some(deadline) = attach_retry_deadline {
                        if tokio::time::Instant::now() < deadline {
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            continue;
                        }
                    }
                    // @dive: Resume path is attach-only to prevent rerunning non-idempotent commands when producer is already gone.
                    sequence = sequence.saturating_add(1);
                    publish_exec_stream_event(
                        config,
                        jetstream,
                        ExecStreamEvent {
                            cluster_id: cluster_id.clone(),
                            logical_stream_id: logical_stream_id.clone(),
                            stream_id: stream_id.clone(),
                            event_seq: sequence,
                            event_id: next_stream_event_id(cluster_id.as_str()),
                            producer_epoch,
                            command_id: command_id.clone(),
                            session_id: session_id.clone(),
                            vm_id: vm_id.clone(),
                            kind: "error".to_string(),
                            data: Vec::new(),
                            exit_code: None,
                            timed_out: false,
                            error: Some(format!(
                                "exec stream resume attach target missing for {} (producer not rerun)",
                                daemon_name
                            )),
                            sequence,
                            emitted_by_node_id: node_id.to_string(),
                            emitted_at_unix_ms: unix_millis(),
                        },
                    )
                    .await?;
                    return Ok(());
                }
                warn!(
                    stream_id = %stream_id,
                    vm_id = %vm_id,
                    elapsed_ms = handler_start.elapsed().as_millis() as u64,
                    grpc_code = ?status.code(),
                    grpc_message = %status.message(),
                    "STREAMSTART attach_daemon RPC failed"
                );
                if cached_ready_used {
                    vm_readiness_state.clear_ready(payload.vm_id.as_str()).await;
                }
                return Err(anyhow!(
                    "invoke daemon attach for exec.stream.start: {status}"
                ));
            }
        }
    };
    let mut stream = response.into_inner();

    let mut control_client = DaemonManagerClient::new(
        tonic::transport::Endpoint::from_shared(endpoint.clone())?
            .connect_timeout(Duration::from_secs(5))
            .connect_lazy(),
    );
    let mut claim = request_with_portproxy_auth(
        crate::proto::bracket::portproxy::v1::ExecControlRequest {
            execution_id: daemon_name.clone(),
            producer_epoch,
            control_seq: 0,
            control: Some(
                crate::proto::bracket::portproxy::v1::exec_control_request::Control::Claim(true),
            ),
        },
        portproxy_auth.as_ref(),
    );
    claim.set_timeout(Duration::from_secs(5));
    if let Err(error) = control_client.control_daemon(claim).await {
        if error.code() != tonic::Code::FailedPrecondition
            || error.message() != "execution has exited"
        {
            return Err(anyhow!("claim exec stream control: {error}"));
        }
    }

    {
        let mut guard = active_exec_streams.lock().await;
        guard.insert(
            stream_id.clone(),
            ActiveExecStream {
                request_tx: req_tx.clone(),
                last_input_seq: std::sync::Arc::new(Mutex::new(0)),
                session_id: session_id.clone(),
                vm_id: vm_id.clone(),
                node_id: node_id.to_string(),
                producer_epoch,
                daemon_name: daemon_name.clone(),
                control_client,
                auth: portproxy_auth.clone(),
            },
        );
    }
    info!(stream_id = %stream_id, elapsed_ms = handler_start.elapsed().as_millis() as u64, "STREAMSTART_T11 after_active_streams_insert");
    // @dive: tier-B execution-restore snapshot is now refreshed AFTER the command's terminal
    //        event (clean exit), not at stream start. Capturing pre-execution state was
    //        useless as a resume target (any restore would replay the command), and the
    //        userfaultfd-WP storm raced the next exec's attach_daemon RPC. Post-exit refresh
    //        is the actual no-rerun resume target and runs on a quiescent guest.
    //        See `spawn_execution_restore_snapshot_refresh` invocations below.
    info!(
        stream_id = %stream_id,
        command_id = %command_id,
        vm_id = %vm_id,
        producer_epoch = producer_epoch,
        resume_after_event_seq = payload.resume_after_event_seq,
        "exec.stream.start established"
    );

    sequence = sequence.saturating_add(1);
    info!(stream_id = %stream_id, elapsed_ms = handler_start.elapsed().as_millis() as u64, "STREAMSTART_T13 before_publish_started_event");
    if let Err(err) = publish_exec_stream_event(
        config,
        jetstream,
        ExecStreamEvent {
            cluster_id: cluster_id.clone(),
            logical_stream_id: logical_stream_id.clone(),
            stream_id: stream_id.clone(),
            event_seq: sequence,
            event_id: next_stream_event_id(cluster_id.as_str()),
            producer_epoch,
            command_id: command_id.clone(),
            session_id: session_id.clone(),
            vm_id: vm_id.clone(),
            kind: "started".to_string(),
            data: Vec::new(),
            exit_code: None,
            timed_out: false,
            error: None,
            sequence,
            emitted_by_node_id: node_id.to_string(),
            emitted_at_unix_ms: unix_millis(),
        },
    )
    .await
    {
        {
            let mut guard = active_exec_streams.lock().await;
            if guard
                .get(&stream_id)
                .is_some_and(|active| active.producer_epoch == producer_epoch)
            {
                guard.remove(&stream_id);
            }
        }
        return Err(err);
    }
    info!(stream_id = %stream_id, elapsed_ms = handler_start.elapsed().as_millis() as u64, "STREAMSTART_T14 after_publish_started_event_handler_done");

    let config = config.clone();
    let node_id = node_id.to_string();
    let jetstream = jetstream.clone();
    let active_exec_streams = active_exec_streams.clone();
    let snapshot_manager = std::sync::Arc::clone(manager);
    let snapshot_vm_metadata = vm_metadata.clone();
    let snapshot_tier_b_eligible = tier_b_eligible;
    let snapshot_state_for_loop = vm_snapshot_state.clone();
    let vm_exec_activity_for_loop = vm_exec_activity_state.clone();
    // @dive: Carry the attach context into the spawn loop so we can re-issue
    //        attach_daemon when the response stream closes without an
    //        ExitCode frame. The portproxy in older guest images races
    //        between three forwarder tasks (stdout/stderr/exit) and can drop
    //        the response stream before the exit forwarder sends ExitCode for
    //        fast back-to-back commands. Re-attaching hits the post-exit
    //        cached_exit fast path on the same daemon name and recovers the
    //        terminal frame deterministically — no guest-image rebuild
    //        required.
    let reattach_endpoint = endpoint.clone();
    let reattach_daemon_name = daemon_name.clone();
    let reattach_portproxy_auth = portproxy_auth.clone();
    let stream_response_idle_timeout =
        Duration::from_secs(payload.timeout_secs.unwrap_or(30).max(1) as u64 + 15);
    const REATTACH_MAX_ATTEMPTS: u32 = 3;
    const REATTACH_BACKOFF: Duration = Duration::from_millis(150);
    tokio::spawn(async move {
        let _exec_activity_lease = exec_activity_lease;
        let mut terminal_emitted = false;
        let mut pending_exit_code: Option<i32> = None;
        let mut refresh_execution_restore_snapshot = false;
        const EXIT_FLUSH_GRACE: Duration = Duration::from_millis(150);
        let mut reattach_attempts: u32 = 0;
        loop {
            let next_message = if let Some(code) = pending_exit_code {
                match tokio::time::timeout(EXIT_FLUSH_GRACE, stream.message()).await {
                    Ok(message) => message,
                    Err(_) => {
                        sequence = sequence.saturating_add(1);
                        debug!(
                            stream_id = %stream_id,
                            sequence = sequence,
                            exit_code = code,
                            "exec.stream deferred exit frame"
                        );
                        if let Err(err) = publish_exec_stream_event(
                            &config,
                            &jetstream,
                            ExecStreamEvent {
                                cluster_id: cluster_id.clone(),
                                logical_stream_id: logical_stream_id.clone(),
                                stream_id: stream_id.clone(),
                                event_seq: sequence,
                                event_id: next_stream_event_id(cluster_id.as_str()),
                                producer_epoch,
                                command_id: command_id.clone(),
                                session_id: session_id.clone(),
                                vm_id: vm_id.clone(),
                                kind: "exit".to_string(),
                                data: Vec::new(),
                                exit_code: Some(code),
                                timed_out: code == 124,
                                error: None,
                                sequence,
                                emitted_by_node_id: node_id.clone(),
                                emitted_at_unix_ms: unix_millis(),
                            },
                        )
                        .await
                        {
                            warn!(
                                stream_id = %stream_id,
                                sequence = sequence,
                                err = %err,
                                "failed publishing deferred exec.stream exit event"
                            );
                        }
                        terminal_emitted = true;
                        if snapshot_tier_b_eligible {
                            refresh_execution_restore_snapshot = true;
                        }
                        break;
                    }
                }
            } else {
                match tokio::time::timeout(stream_response_idle_timeout, stream.message()).await {
                    Ok(message) => message,
                    Err(_) => {
                        sequence = sequence.saturating_add(1);
                        warn!(
                            stream_id = %stream_id,
                            sequence = sequence,
                            idle_timeout_ms = stream_response_idle_timeout.as_millis() as u64,
                            "exec.stream attach_daemon response idle timeout"
                        );
                        if let Err(err) = publish_exec_stream_event(
                            &config,
                            &jetstream,
                            ExecStreamEvent {
                                cluster_id: cluster_id.clone(),
                                logical_stream_id: logical_stream_id.clone(),
                                stream_id: stream_id.clone(),
                                event_seq: sequence,
                                event_id: next_stream_event_id(cluster_id.as_str()),
                                producer_epoch,
                                command_id: command_id.clone(),
                                session_id: session_id.clone(),
                                vm_id: vm_id.clone(),
                                kind: "timeout".to_string(),
                                data: Vec::new(),
                                exit_code: None,
                                timed_out: true,
                                error: Some(
                                    "exec stream timed out waiting for guest terminal event"
                                        .to_string(),
                                ),
                                sequence,
                                emitted_by_node_id: node_id.clone(),
                                emitted_at_unix_ms: unix_millis(),
                            },
                        )
                        .await
                        {
                            warn!(
                                stream_id = %stream_id,
                                sequence = sequence,
                                err = %err,
                                "failed publishing exec.stream idle timeout event"
                            );
                        }
                        terminal_emitted = true;
                        break;
                    }
                }
            };
            match next_message {
                Ok(Some(AttachDaemonResponse {
                    response: Some(attach_daemon_response::Response::StdoutData(bytes)),
                })) => {
                    sequence = sequence.saturating_add(1);
                    debug!(
                        stream_id = %stream_id,
                        sequence = sequence,
                        bytes = bytes.len(),
                        "exec.stream stdout frame"
                    );
                    if let Err(err) = publish_exec_stream_event(
                        &config,
                        &jetstream,
                        ExecStreamEvent {
                            cluster_id: cluster_id.clone(),
                            logical_stream_id: logical_stream_id.clone(),
                            stream_id: stream_id.clone(),
                            event_seq: sequence,
                            event_id: next_stream_event_id(cluster_id.as_str()),
                            producer_epoch,
                            command_id: command_id.clone(),
                            session_id: session_id.clone(),
                            vm_id: vm_id.clone(),
                            kind: "stdout".to_string(),
                            data: bytes,
                            exit_code: None,
                            timed_out: false,
                            error: None,
                            sequence,
                            emitted_by_node_id: node_id.clone(),
                            emitted_at_unix_ms: unix_millis(),
                        },
                    )
                    .await
                    {
                        warn!(
                            stream_id = %stream_id,
                            sequence = sequence,
                            err = %err,
                            "failed publishing exec.stream stdout event"
                        );
                    }
                }
                Ok(Some(AttachDaemonResponse {
                    response: Some(attach_daemon_response::Response::StderrData(bytes)),
                })) => {
                    sequence = sequence.saturating_add(1);
                    debug!(
                        stream_id = %stream_id,
                        sequence = sequence,
                        bytes = bytes.len(),
                        "exec.stream stderr frame"
                    );
                    if let Err(err) = publish_exec_stream_event(
                        &config,
                        &jetstream,
                        ExecStreamEvent {
                            cluster_id: cluster_id.clone(),
                            logical_stream_id: logical_stream_id.clone(),
                            stream_id: stream_id.clone(),
                            event_seq: sequence,
                            event_id: next_stream_event_id(cluster_id.as_str()),
                            producer_epoch,
                            command_id: command_id.clone(),
                            session_id: session_id.clone(),
                            vm_id: vm_id.clone(),
                            kind: "stderr".to_string(),
                            data: bytes,
                            exit_code: None,
                            timed_out: false,
                            error: None,
                            sequence,
                            emitted_by_node_id: node_id.clone(),
                            emitted_at_unix_ms: unix_millis(),
                        },
                    )
                    .await
                    {
                        warn!(
                            stream_id = %stream_id,
                            sequence = sequence,
                            err = %err,
                            "failed publishing exec.stream stderr event"
                        );
                    }
                }
                Ok(Some(AttachDaemonResponse {
                    response: Some(attach_daemon_response::Response::ExitCode(code)),
                })) => {
                    if code == 124 {
                        sequence = sequence.saturating_add(1);
                        debug!(
                            stream_id = %stream_id,
                            sequence = sequence,
                            "exec.stream timeout frame"
                        );
                        if let Err(err) = publish_exec_stream_event(
                            &config,
                            &jetstream,
                            ExecStreamEvent {
                                cluster_id: cluster_id.clone(),
                                logical_stream_id: logical_stream_id.clone(),
                                stream_id: stream_id.clone(),
                                event_seq: sequence,
                                event_id: next_stream_event_id(cluster_id.as_str()),
                                producer_epoch,
                                command_id: command_id.clone(),
                                session_id: session_id.clone(),
                                vm_id: vm_id.clone(),
                                kind: "timeout".to_string(),
                                data: Vec::new(),
                                exit_code: None,
                                timed_out: true,
                                error: None,
                                sequence,
                                emitted_by_node_id: node_id.clone(),
                                emitted_at_unix_ms: unix_millis(),
                            },
                        )
                        .await
                        {
                            warn!(
                                stream_id = %stream_id,
                                sequence = sequence,
                                err = %err,
                                "failed publishing exec.stream timeout event"
                            );
                        }
                    }
                    pending_exit_code = Some(code);
                    continue;
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    info!(
                        stream_id = %stream_id,
                        pending_exit_code = ?pending_exit_code,
                        sequence = sequence,
                        reattach_attempts = reattach_attempts,
                        "exec.stream attach_daemon response stream closed (Ok(None))"
                    );
                    if pending_exit_code.is_none() && reattach_attempts < REATTACH_MAX_ATTEMPTS {
                        reattach_attempts = reattach_attempts.saturating_add(1);
                        tokio::time::sleep(REATTACH_BACKOFF).await;
                        match reattach_daemon_for_exit(
                            reattach_endpoint.as_str(),
                            reattach_daemon_name.as_str(),
                            reattach_portproxy_auth.as_ref(),
                        )
                        .await
                        {
                            Ok(Some(new_stream)) => {
                                info!(
                                    stream_id = %stream_id,
                                    reattach_attempts = reattach_attempts,
                                    "exec.stream re-attached to recover missing ExitCode"
                                );
                                stream = new_stream;
                                continue;
                            }
                            Ok(None) => {
                                warn!(
                                    stream_id = %stream_id,
                                    reattach_attempts = reattach_attempts,
                                    "exec.stream re-attach succeeded but daemon entry gone (will fall through to synthetic error)"
                                );
                            }
                            Err(err) => {
                                warn!(
                                    stream_id = %stream_id,
                                    reattach_attempts = reattach_attempts,
                                    err = %err,
                                    "exec.stream re-attach RPC failed (will retry or give up)"
                                );
                                continue;
                            }
                        }
                    }
                    if let Some(code) = pending_exit_code {
                        sequence = sequence.saturating_add(1);
                        debug!(
                            stream_id = %stream_id,
                            sequence = sequence,
                            exit_code = code,
                            "exec.stream exit frame"
                        );
                        if let Err(err) = publish_exec_stream_event(
                            &config,
                            &jetstream,
                            ExecStreamEvent {
                                cluster_id: cluster_id.clone(),
                                logical_stream_id: logical_stream_id.clone(),
                                stream_id: stream_id.clone(),
                                event_seq: sequence,
                                event_id: next_stream_event_id(cluster_id.as_str()),
                                producer_epoch,
                                command_id: command_id.clone(),
                                session_id: session_id.clone(),
                                vm_id: vm_id.clone(),
                                kind: "exit".to_string(),
                                data: Vec::new(),
                                exit_code: Some(code),
                                timed_out: code == 124,
                                error: None,
                                sequence,
                                emitted_by_node_id: node_id.clone(),
                                emitted_at_unix_ms: unix_millis(),
                            },
                        )
                        .await
                        {
                            warn!(
                                stream_id = %stream_id,
                                sequence = sequence,
                                err = %err,
                                "failed publishing exec.stream exit event"
                            );
                        }
                        terminal_emitted = true;
                        if snapshot_tier_b_eligible {
                            refresh_execution_restore_snapshot = true;
                        }
                    }
                    debug!(
                        stream_id = %stream_id,
                        sequence = sequence,
                        "exec.stream response stream closed"
                    );
                    break;
                }
                Err(err) => {
                    if let Some(code) = pending_exit_code {
                        sequence = sequence.saturating_add(1);
                        debug!(
                            stream_id = %stream_id,
                            sequence = sequence,
                            exit_code = code,
                            "exec.stream exit frame after transport close"
                        );
                        if let Err(err) = publish_exec_stream_event(
                            &config,
                            &jetstream,
                            ExecStreamEvent {
                                cluster_id: cluster_id.clone(),
                                logical_stream_id: logical_stream_id.clone(),
                                stream_id: stream_id.clone(),
                                event_seq: sequence,
                                event_id: next_stream_event_id(cluster_id.as_str()),
                                producer_epoch,
                                command_id: command_id.clone(),
                                session_id: session_id.clone(),
                                vm_id: vm_id.clone(),
                                kind: "exit".to_string(),
                                data: Vec::new(),
                                exit_code: Some(code),
                                timed_out: code == 124,
                                error: None,
                                sequence,
                                emitted_by_node_id: node_id.clone(),
                                emitted_at_unix_ms: unix_millis(),
                            },
                        )
                        .await
                        {
                            warn!(
                                stream_id = %stream_id,
                                sequence = sequence,
                                err = %err,
                                "failed publishing exec.stream exit event"
                            );
                        }
                        terminal_emitted = true;
                        if snapshot_tier_b_eligible {
                            refresh_execution_restore_snapshot = true;
                        }
                        break;
                    }
                    sequence = sequence.saturating_add(1);
                    warn!(
                        stream_id = %stream_id,
                        sequence = sequence,
                        err = %err,
                        "exec.stream transport error"
                    );
                    if let Err(err) = publish_exec_stream_event(
                        &config,
                        &jetstream,
                        ExecStreamEvent {
                            cluster_id: cluster_id.clone(),
                            logical_stream_id: logical_stream_id.clone(),
                            stream_id: stream_id.clone(),
                            event_seq: sequence,
                            event_id: next_stream_event_id(cluster_id.as_str()),
                            producer_epoch,
                            command_id: command_id.clone(),
                            session_id: session_id.clone(),
                            vm_id: vm_id.clone(),
                            kind: "error".to_string(),
                            data: Vec::new(),
                            exit_code: None,
                            timed_out: false,
                            error: Some(format!("exec stream transport error: {err}")),
                            sequence,
                            emitted_by_node_id: node_id.clone(),
                            emitted_at_unix_ms: unix_millis(),
                        },
                    )
                    .await
                    {
                        warn!(
                            stream_id = %stream_id,
                            sequence = sequence,
                            err = %err,
                            "failed publishing exec.stream error event"
                        );
                    }
                    terminal_emitted = true;
                    break;
                }
            }
        }

        if !terminal_emitted {
            warn!(
                stream_id = %stream_id,
                vm_id = %vm_id,
                pending_exit_code = ?pending_exit_code,
                last_sequence = sequence,
                "exec.stream spawn loop ended without terminal event; publishing synthetic error to NATS"
            );
            sequence = sequence.saturating_add(1);
            if let Err(err) = publish_exec_stream_event(
                &config,
                &jetstream,
                ExecStreamEvent {
                    cluster_id: cluster_id.clone(),
                    logical_stream_id: logical_stream_id.clone(),
                    stream_id: stream_id.clone(),
                    event_seq: sequence,
                    event_id: next_stream_event_id(cluster_id.as_str()),
                    producer_epoch,
                    command_id: command_id.clone(),
                    session_id: session_id.clone(),
                    vm_id: vm_id.clone(),
                    kind: "error".to_string(),
                    data: Vec::new(),
                    exit_code: None,
                    timed_out: false,
                    error: Some("exec stream ended before terminal event".to_string()),
                    sequence,
                    emitted_by_node_id: node_id.clone(),
                    emitted_at_unix_ms: unix_millis(),
                },
            )
            .await
            {
                warn!(
                    stream_id = %stream_id,
                    sequence = sequence,
                    err = %err,
                    "failed publishing terminal exec.stream error event"
                );
            }
        }

        {
            let mut guard = active_exec_streams.lock().await;
            guard.remove(&stream_id);
        }
        if refresh_execution_restore_snapshot {
            spawn_execution_restore_snapshot_refresh(
                std::sync::Arc::clone(&snapshot_manager),
                vm_id.clone(),
                stream_id.clone(),
                snapshot_vm_metadata.clone(),
                snapshot_state_for_loop.clone(),
                vm_exec_activity_for_loop.clone(),
            );
        }
    });

    Ok(())
}

/// Re-issues an attach_daemon RPC for a daemon that has already exited so the
/// portproxy `cached_exit` fast path can deliver the missing ExitCode frame.
/// Returns:
/// - `Ok(Some(stream))` — fresh response stream that will deliver the exit
///   code; caller continues the spawn loop reading from it.
/// - `Ok(None)` — daemon entry was reaped (NotFound). Caller should fall
///   through to the synthetic-error path.
/// - `Err` — transport failure. Caller should retry or give up per its own
///   policy.
async fn reattach_daemon_for_exit(
    endpoint: &str,
    daemon_name: &str,
    portproxy_auth: Option<&tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
) -> Result<Option<tonic::Streaming<AttachDaemonResponse>>> {
    let mut client = DaemonManagerClient::connect(endpoint.to_string())
        .await
        .with_context(|| format!("reattach: connect daemon manager at {endpoint}"))?;
    let (req_tx, req_rx) = mpsc::channel(2);
    req_tx
        .send(AttachDaemonRequest {
            request: Some(attach_daemon_request::Request::Start(AttachDaemonStart {
                name: daemon_name.to_string(),
            })),
        })
        .await
        .map_err(|_| anyhow!("reattach: enqueue start request"))?;
    drop(req_tx);
    match client
        .attach_daemon(request_with_portproxy_auth(
            ReceiverStream::new(req_rx),
            portproxy_auth,
        ))
        .await
    {
        Ok(response) => Ok(Some(response.into_inner())),
        Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
        Err(status) => Err(anyhow!("reattach attach_daemon RPC: {status}")),
    }
}

async fn handle_exec_stream_input_command(
    envelope: &CommandEnvelope,
    active_exec_streams: &std::sync::Arc<Mutex<HashMap<String, ActiveExecStream>>>,
) -> Result<()> {
    let payload: ExecStreamInputPayload = serde_json::from_value(envelope.payload.clone())
        .context("decode exec stream control/input payload")?;
    let active = active_exec_streams
        .lock()
        .await
        .get(&payload.stream_id)
        .cloned()
        .ok_or_else(|| anyhow!("exec stream is not active: {}", payload.stream_id))?;
    validate_exec_input_identity(&active, &payload)?;
    if payload.input_kind == "signal" {
        let signal = payload
            .signal
            .ok_or_else(|| anyhow!("missing exec signal"))?;
        let mut client = active.control_client.clone();
        let mut request = request_with_portproxy_auth(
            crate::proto::bracket::portproxy::v1::ExecControlRequest {
                execution_id: active.daemon_name.clone(),
                producer_epoch: active.producer_epoch,
                control_seq: payload.input_seq,
                control: Some(
                    crate::proto::bracket::portproxy::v1::exec_control_request::Control::Signal(
                        signal,
                    ),
                ),
            },
            active.auth.as_ref(),
        );
        request.set_timeout(Duration::from_secs(5));
        client
            .control_daemon(request)
            .await
            .context("forward exec stream signal")?;
        return Ok(());
    }
    let request = match payload.input_kind.as_str() {
        "stdin" => attach_daemon_request::Request::StdinData(payload.data.unwrap_or_default()),
        "eof" => attach_daemon_request::Request::StdinEof(true),
        other => return Err(anyhow!("unsupported exec stream input kind: {other}")),
    };
    let mut last = active.last_input_seq.lock().await;
    if payload.input_seq == 0 {
        return Err(anyhow!("input_seq must be positive"));
    }
    if payload.input_seq <= *last {
        return Ok(());
    }
    if payload.input_seq != last.saturating_add(1) {
        return Err(anyhow!(
            "exec stream input out of order: got {}, expected {}",
            payload.input_seq,
            last.saturating_add(1)
        ));
    }
    tokio::time::timeout(
        Duration::from_secs(2),
        active.request_tx.send(AttachDaemonRequest {
            request: Some(request),
        }),
    )
    .await
    .context("exec stream stdin queue busy")?
    .map_err(|_| anyhow!("exec stream stdin closed"))?;
    *last = payload.input_seq;
    Ok(())
}

fn validate_exec_input_identity(
    active: &ActiveExecStream,
    payload: &ExecStreamInputPayload,
) -> Result<()> {
    if payload.producer_epoch != Some(active.producer_epoch)
        || payload.session_id != active.session_id
        || payload.vm_id != active.vm_id
        || payload.target_node_id != active.node_id
    {
        return Err(anyhow!("stale or mismatched exec stream owner"));
    }
    Ok(())
}

// @dive: Publish a synthetic terminal "error" event when handle_exec_stream_start_command
//        returns Err and would otherwise be silently DLQ'd. Without this, the API client
//        subscribed to exec stream events sits on its 210s start-wait budget even though
//        vmd has already given up on this command. The API treats this event the same as
//        any other stream-side error and fast-fails the start.
async fn publish_exec_stream_handler_failure_event(
    envelope: &CommandEnvelope,
    config: &ControlBusConfig,
    node_id: &str,
    jetstream: &jetstream::Context,
    error_text: &str,
) {
    let payload: ExecStreamStartPayload = match serde_json::from_value(envelope.payload.clone()) {
        Ok(payload) => payload,
        Err(err) => {
            warn!(
                node_id = %node_id,
                command_id = %envelope.command_id,
                error = %err,
                "skipping handler-failure stream event publish; payload undecodable"
            );
            return;
        }
    };
    let stream_id = payload.stream_id.trim().to_string();
    if stream_id.is_empty() {
        warn!(
            node_id = %node_id,
            command_id = %envelope.command_id,
            "skipping handler-failure stream event publish; payload missing stream_id"
        );
        return;
    }
    let logical_stream_id = if payload.logical_stream_id.trim().is_empty() {
        stream_id.clone()
    } else {
        payload.logical_stream_id.trim().to_string()
    };
    let event = ExecStreamEvent {
        cluster_id: payload.cluster_id.clone(),
        logical_stream_id,
        stream_id: stream_id.clone(),
        // @dive: Synthetic event_seq=1 since this fires before the handler's normal
        //        sequence accounting. Clients treat the first error as terminal anyway.
        event_seq: 1,
        event_id: next_stream_event_id(payload.cluster_id.as_str()),
        producer_epoch: payload.producer_epoch,
        command_id: envelope.command_id.clone(),
        session_id: payload.session_id.clone(),
        vm_id: payload.vm_id.clone(),
        kind: "error".to_string(),
        data: Vec::new(),
        exit_code: None,
        timed_out: false,
        error: Some(format!("exec.stream.start handler failed: {error_text}")),
        sequence: 1,
        emitted_by_node_id: node_id.to_string(),
        emitted_at_unix_ms: unix_millis(),
    };
    if let Err(err) = publish_exec_stream_event(config, jetstream, event).await {
        warn!(
            node_id = %node_id,
            command_id = %envelope.command_id,
            stream_id = %stream_id,
            error = %err,
            "failed publishing handler-failure stream error event"
        );
    }
}

async fn publish_exec_stream_event(
    config: &ControlBusConfig,
    jetstream: &jetstream::Context,
    event: ExecStreamEvent,
) -> Result<()> {
    let logical_stream_id = if event.logical_stream_id.trim().is_empty() {
        event.stream_id.as_str()
    } else {
        event.logical_stream_id.as_str()
    };
    let subject = format!(
        "{}.evt.exec.stream.{}",
        config.subject_prefix,
        sanitize_subject_token(logical_stream_id)
    );
    let bytes = serde_json::to_vec(&event).context("serialize exec stream event")?;
    let publish_ack = jetstream
        .publish(subject, bytes.into())
        .await
        .context("publish exec stream event")?;
    publish_ack
        .await
        .context("await exec stream event publish ack")?;
    Ok(())
}

async fn handle_overloaded_command_message(
    message: &jetstream::Message,
    config: &ControlBusConfig,
    node_id: &str,
    jetstream: &jetstream::Context,
) {
    let retry_after_ms = config.overload_retry_after_ms.max(1);
    let details = format!("ResourceExhausted retry_after_ms={retry_after_ms}");
    let payload = json!({
        "node_id": node_id,
        "subject": message.subject.to_string(),
        "reason": "resource_exhausted",
        "retry_after_ms": retry_after_ms,
        "captured_at_unix_ms": unix_millis(),
    });
    let subject = format!("{}.evt.command.overloaded", config.subject_prefix);
    if let Ok(bytes) = serde_json::to_vec(&payload) {
        if let Ok(ack) = jetstream.publish(subject, bytes.into()).await {
            let _ = ack.await;
        }
    }
    if let Err(err) = message
        .ack_with(AckKind::Nak(Some(Duration::from_millis(retry_after_ms))))
        .await
    {
        warn!(
            node_id = %node_id,
            err = %err,
            details = %details,
            "failed to nack overloaded control command"
        );
    }
}

async fn handle_failed_command_message(
    message: &jetstream::Message,
    config: &ControlBusConfig,
    node_id: &str,
    jetstream: &jetstream::Context,
    reason: &str,
    details: &str,
) {
    let delivered = message
        .info()
        .map(|info| info.delivered)
        .unwrap_or(1)
        .max(1);
    let max_deliver = config.command_max_deliver.max(1);

    if delivered >= max_deliver {
        if let Err(err) =
            publish_dead_letter(message, config, node_id, jetstream, reason, details).await
        {
            warn!(
                node_id = %node_id,
                reason = %reason,
                err = %err,
                "failed publishing control command dead-letter"
            );
        }
        if let Err(err) = message.ack_with(AckKind::Term).await {
            warn!(
                node_id = %node_id,
                reason = %reason,
                err = %err,
                "failed to terminate poison control command"
            );
        }
        return;
    }

    if let Err(err) = message.ack_with(AckKind::Nak(None)).await {
        warn!(
            node_id = %node_id,
            reason = %reason,
            err = %err,
            "failed to nack control command"
        );
    }
}

async fn publish_dead_letter(
    message: &jetstream::Message,
    config: &ControlBusConfig,
    node_id: &str,
    jetstream: &jetstream::Context,
    reason: &str,
    details: &str,
) -> Result<()> {
    let delivered = message
        .info()
        .map(|info| info.delivered)
        .unwrap_or(1)
        .max(1);
    let payload = serde_json::from_slice::<Value>(&message.payload).unwrap_or_else(|_| {
        json!({
            "raw_payload": String::from_utf8_lossy(&message.payload).to_string(),
        })
    });

    let dead_letter = DeadLetterEnvelope {
        dead_letter_id: Uuid::new_v4().to_string(),
        command_id: serde_json::from_slice::<CommandEnvelope>(&message.payload)
            .map(|envelope| envelope.command_id)
            .unwrap_or_default(),
        original_subject: message.subject.to_string(),
        reason: reason.to_string(),
        details: details.to_string(),
        delivered,
        node_id: node_id.to_string(),
        captured_at_unix_ms: unix_millis(),
        payload,
    };
    let bytes = serde_json::to_vec(&dead_letter).context("serialize dead-letter envelope")?;

    let publish_ack = jetstream
        .send_publish(
            config.dead_letter_subject.clone(),
            jetstream::context::Publish::build()
                .message_id(dead_letter.dead_letter_id.clone())
                .payload(bytes.into()),
        )
        .await
        .context("publish dead-letter envelope")?;
    publish_ack
        .await
        .context("await dead-letter publish acknowledgement")?;

    Ok(())
}

async fn ensure_control_stream(
    jetstream: &jetstream::Context,
    config: &ControlBusConfig,
) -> Result<()> {
    let mut subjects = vec![
        format!("{}.cmd.>", config.subject_prefix),
        format!("{}.evt.>", config.subject_prefix),
        config.dead_letter_subject.clone(),
        config.replay_subject.clone(),
    ];
    subjects.sort();
    subjects.dedup();

    jetstream
        .get_or_create_stream(jetstream::stream::Config {
            name: config.stream_name.clone(),
            subjects,
            max_age: Duration::from_secs(config.stream_max_age_secs.max(60)),
            storage: jetstream::stream::StorageType::File,
            num_replicas: config.stream_replicas.max(1),
            ..Default::default()
        })
        .await
        .context("ensure control stream")?;

    Ok(())
}

fn metadata_tier_b_eligible(metadata: &HashMap<String, String>) -> bool {
    let raw = metadata
        .get(META_TIER_B_ELIGIBLE)
        .or_else(|| metadata.get("tier_b_eligible"))
        .map(String::as_str)
        .unwrap_or("true");
    !matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "0" | "false" | "no" | "off"
    )
}

async fn wait_for_guest_exec_ready(
    endpoint: &str,
    auth_header: Option<&tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
    timeout: Duration,
) -> Result<()> {
    let start = Instant::now();
    let mut last_error: Option<String> = None;

    while start.elapsed() < timeout {
        match tokio::time::timeout(
            GUEST_EXEC_READY_ATTEMPT_TIMEOUT,
            probe_guest_exec_ready_anyhow(endpoint, auth_header, 5),
        )
        .await
        {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(err)) => {
                last_error = Some(err.to_string());
            }
            Err(_) => {
                last_error = Some("guest exec readiness probe timed out".to_string());
            }
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let suffix = last_error
        .map(|error| format!("; last error: {error}"))
        .unwrap_or_default();
    Err(anyhow!(
        "guest exec RPC did not become ready on {endpoint} within {}s{suffix}",
        timeout.as_secs()
    ))
}

async fn wait_for_guest_exec_ready_or_mark_vm_error(
    manager: &Manager,
    vm_id: &str,
    endpoint: &str,
    auth_header: Option<&tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
    timeout: Duration,
    context: &'static str,
) -> Result<()> {
    match wait_for_guest_exec_ready(endpoint, auth_header, timeout)
        .await
        .context(context)
    {
        Ok(()) => Ok(()),
        Err(error) => {
            let error_text = error.to_string();
            if tokio::time::timeout(
                Duration::from_secs(10),
                mark_vm_guest_exec_unhealthy(manager, vm_id, error_text.as_str()),
            )
            .await
            .is_err()
            {
                warn!(
                    vm_id = %vm_id,
                    error = %error_text,
                    "timed out marking vm error after guest exec readiness failure"
                );
            }
            Err(error)
        }
    }
}

fn spawn_mark_vm_guest_exec_unhealthy(
    manager: std::sync::Arc<Manager>,
    vm_id: String,
    error: String,
) {
    tokio::spawn(async move {
        mark_vm_guest_exec_unhealthy(manager.as_ref(), vm_id.as_str(), error.as_str()).await;
    });
}

async fn mark_vm_guest_exec_unhealthy(manager: &Manager, vm_id: &str, error: &str) {
    match manager.mark_vm_error(vm_id, error).await {
        Ok(_) => {
            warn!(
                vm_id = %vm_id,
                error = %error,
                "marked vm error after guest exec readiness failure"
            );
        }
        Err(mark_error) => {
            warn!(
                vm_id = %vm_id,
                error = %error,
                mark_error = %mark_error,
                "failed marking vm error after guest exec readiness failure"
            );
        }
    }
}

async fn resolve_execution_restore_snapshot_id(
    manager: &Manager,
    vm_id: &str,
    metadata: &HashMap<String, String>,
) -> Result<Option<String>> {
    if let Some(snapshot_id) = metadata
        .get(META_EXEC_RESTORE_SNAPSHOT_ID)
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
    {
        return Ok(Some(snapshot_id));
    }
    let Some(snapshot_name) = metadata
        .get(META_EXEC_RESTORE_SNAPSHOT_NAME)
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
    else {
        return Ok(None);
    };
    let snapshots = manager.list_snapshots(vm_id).await.map_err(|err| {
        anyhow!("list snapshots for execution restore marker lookup failed: {err}")
    })?;
    Ok(snapshots
        .into_iter()
        .find(|snapshot| snapshot.name == snapshot_name)
        .map(|snapshot| snapshot.id))
}

// @dive: Fire-and-forget post-exit snapshot refresh. Captures the quiesced
//        post-command VM state so a subsequent restore actually resumes past
//        the command instead of replaying it. Throttles per-VM to bound IOPS
//        on shared RWX storage when many small commands fire in succession.
//
//        Runs in the background and deliberately stays off the per-VM readiness
//        gate. Foreground exec startup owns that gate; this refresh only runs
//        after an idle quiet period and yields if another exec arrives.
fn spawn_execution_restore_snapshot_refresh(
    manager: std::sync::Arc<Manager>,
    vm_id: String,
    stream_id: String,
    vm_metadata: HashMap<String, String>,
    state: VmSnapshotState,
    vm_exec_activity_state: VmExecActivityState,
) {
    tokio::spawn(async move {
        if !state
            .try_begin_refresh(vm_id.as_str(), EXEC_RESTORE_REFRESH_THROTTLE)
            .await
        {
            debug!(
                vm_id = %vm_id,
                stream_id = %stream_id,
                throttle_secs = EXEC_RESTORE_REFRESH_THROTTLE.as_secs(),
                "skipping execution restore snapshot refresh; previous refresh within throttle window"
            );
            return;
        }

        tokio::time::sleep(EXEC_RESTORE_REFRESH_QUIET_PERIOD).await;
        if vm_exec_activity_state.is_active(vm_id.as_str()) {
            debug!(
                vm_id = %vm_id,
                stream_id = %stream_id,
                quiet_ms = EXEC_RESTORE_REFRESH_QUIET_PERIOD.as_millis() as u64,
                "skipping execution restore snapshot refresh; VM accepted another exec during quiet window"
            );
            return;
        }

        let pending = {
            if vm_exec_activity_state.is_active(vm_id.as_str()) {
                debug!(
                    vm_id = %vm_id,
                    stream_id = %stream_id,
                    "skipping execution restore snapshot refresh; VM accepted another exec before snapshot phase"
                );
                return;
            }

            match manager
                .create_snapshot_qemu_phase(
                    vm_id.as_str(),
                    SnapshotParams {
                        label: "execution-restore".to_string(),
                        description: "control-bus exec stream restore marker".to_string(),
                    },
                )
                .await
            {
                Ok(pending) => pending,
                Err(err) if background_snapshot_unsupported(&err) => {
                    if let Err(persist_err) = disable_tier_b_after_unsupported(
                        manager.as_ref(),
                        vm_id.as_str(),
                        &vm_metadata,
                    )
                    .await
                    {
                        warn!(
                            vm_id = %vm_id,
                            stream_id = %stream_id,
                            error = %persist_err,
                            underlying = %err,
                            "failed disabling tier-b eligibility after unsupported background snapshot"
                        );
                    } else {
                        warn!(
                            vm_id = %vm_id,
                            stream_id = %stream_id,
                            underlying = %err,
                            "background snapshot unsupported on host; disabled tier-b restore-marker enforcement for this vm"
                        );
                    }
                    return;
                }
                Err(err) => {
                    warn!(
                        vm_id = %vm_id,
                        stream_id = %stream_id,
                        error = %err,
                        "failed qemu phase of execution restore snapshot refresh"
                    );
                    return;
                }
            }
        };

        match finalize_execution_restore_snapshot_marker(
            manager.as_ref(),
            vm_id.as_str(),
            &vm_metadata,
            pending,
        )
        .await
        {
            Ok((snapshot_id, snapshot_name)) => {
                info!(
                    vm_id = %vm_id,
                    stream_id = %stream_id,
                    snapshot_id = %snapshot_id,
                    snapshot_name = %snapshot_name,
                    "refreshed execution restore snapshot marker after exec stream exit"
                );
            }
            Err(err) => {
                warn!(
                    vm_id = %vm_id,
                    stream_id = %stream_id,
                    error = %err,
                    "failed finalizing execution restore snapshot marker after exec stream exit"
                );
            }
        }
    });
}

async fn disable_tier_b_after_unsupported(
    manager: &Manager,
    vm_id: &str,
    vm_metadata: &HashMap<String, String>,
) -> Result<()> {
    let mut degraded_metadata = vm_metadata.clone();
    degraded_metadata.remove(META_EXEC_RESTORE_SNAPSHOT_ID);
    degraded_metadata.remove(META_EXEC_RESTORE_SNAPSHOT_NAME);
    degraded_metadata.insert(META_TIER_B_ELIGIBLE.to_string(), "false".to_string());
    manager
        .update_vm(
            vm_id,
            UpdateVmParams {
                name: None,
                metadata: Some(degraded_metadata),
                resources: None,
                shared_mounts: None,
            },
        )
        .await
        .map_err(|persist_err| {
            anyhow!(
                "disable tier-b eligibility after unsupported background snapshot failed: {persist_err}"
            )
        })?;
    Ok(())
}

async fn finalize_execution_restore_snapshot_marker(
    manager: &Manager,
    vm_id: &str,
    vm_metadata: &HashMap<String, String>,
    pending: PendingSnapshot,
) -> Result<(String, String)> {
    // @dive: Write order is create-new → update-metadata → delete-old so a crash
    //        between any two steps leaves the previous (valid) snapshot still
    //        addressable from metadata. Reversing this order — delete-old first —
    //        could leave metadata pointing at a deleted snapshot after a node drain,
    //        silently downgrading Tier-B to base-clone on resume.
    let previous_snapshot_id = vm_metadata
        .get(META_EXEC_RESTORE_SNAPSHOT_ID)
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);

    let snapshot = manager
        .promote_staged_snapshot(vm_id, pending)
        .await
        .map_err(|err| anyhow!("promote staged execution restore snapshot failed: {err}"))?;

    let mut updated_metadata = vm_metadata.clone();
    updated_metadata.insert(
        META_EXEC_RESTORE_SNAPSHOT_ID.to_string(),
        snapshot.id.clone(),
    );
    updated_metadata.insert(
        META_EXEC_RESTORE_SNAPSHOT_NAME.to_string(),
        snapshot.name.clone(),
    );
    manager
        .update_vm(
            vm_id,
            UpdateVmParams {
                name: None,
                metadata: Some(updated_metadata),
                resources: None,
                shared_mounts: None,
            },
        )
        .await
        .map_err(|err| anyhow!("persist execution restore snapshot marker failed: {err}"))?;

    if let Some(previous_snapshot_id) = previous_snapshot_id {
        if previous_snapshot_id != snapshot.id {
            if let Err(err) = manager
                .delete_snapshot(vm_id, previous_snapshot_id.as_str())
                .await
            {
                warn!(
                    vm_id = %vm_id,
                    previous_snapshot_id = %previous_snapshot_id,
                    new_snapshot_id = %snapshot.id,
                    error = %err,
                    "failed deleting previous execution restore snapshot after marker refresh; orphan snapshot will be reaped on next refresh"
                );
            }
        }
    }

    Ok((snapshot.id, snapshot.name))
}

/// Classify an error from `create_snapshot_qemu_phase` as a true "host kernel
/// can't do background-snapshot" so the caller can disable tier-b for the VM
/// instead of silently retrying forever. Only match on substrings that
/// actually indicate the kernel/qemu lacks the feature — NOT on the wrapping
/// anyhow context string `enable background-snapshot capabilities`, which is
/// emitted for *any* error during that step (including transient QMP errors,
/// PII state mismatches, etc.) and was producing false positives that
/// permanently disabled tier-b on a healthy host.
fn background_snapshot_unsupported(err: &crate::state::ManagerError) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    message.contains("background-snapshot is not supported by host kernel")
        || message.contains("userfaultfd is not available")
        || message.contains("uffd-wp")
        || message.contains("the kernel does not support background snapshot")
        || (message.contains("parameter 'capability'")
            && (message.contains("background-snapshot") || message.contains("mapped-ram")))
}

fn dedupe_key(envelope: &CommandEnvelope) -> String {
    if !envelope.idempotency_key.trim().is_empty() {
        return envelope.idempotency_key.trim().to_string();
    }
    if !envelope.command_id.trim().is_empty() {
        return envelope.command_id.trim().to_string();
    }
    format!("anon-{}", Uuid::new_v4())
}

fn command_session_id(envelope: &CommandEnvelope) -> Option<&str> {
    envelope
        .payload
        .get("session_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn session_etcd_prefix_from_dedupe_prefix(dedupe_prefix: &str) -> String {
    let trimmed = dedupe_prefix.trim().trim_end_matches('/');
    trimmed
        .split("/command-dedupe")
        .next()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("/chevalier-sandbox")
        .to_string()
}

fn shard_for_key(key: &str, shard_count: u8) -> u8 {
    let shard_count = shard_count.max(1);
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() % shard_count as u64) as u8
}

fn ownership_fence_allows_transition(current: Option<&str>, expected: Option<&str>) -> bool {
    match expected {
        Some(expected) => current.is_some_and(|value| value == expected),
        None => current.is_none(),
    }
}

fn sanitize_key_component(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "key".to_string()
    } else {
        trimmed.to_string()
    }
}

fn sanitize_subject_token(raw: &str) -> String {
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
        "command".to_string()
    } else {
        trimmed.to_string()
    }
}

async fn read_fence_value(client: &mut EtcdClient, key: &str) -> Result<Option<String>> {
    let response = client
        .get(key.to_string(), None)
        .await
        .context("read ownership fence key")?;
    let Some(kv) = response.kvs().first() else {
        return Ok(None);
    };
    let value = String::from_utf8(kv.value().to_vec()).context("decode ownership fence value")?;
    if value.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(value))
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn next_stream_event_id(cluster_id: &str) -> String {
    format!(
        "{}-{}",
        sanitize_key_component(cluster_id),
        Uuid::now_v7().as_simple()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::guest_exec_probe::META_PORTPROXY_AUTH_TOKEN;

    #[test]
    fn command_session_id_prefers_payload_session() {
        let envelope = CommandEnvelope {
            command_id: "command-1".to_string(),
            idempotency_key: String::new(),
            command_type: "session.attach".to_string(),
            ordering_key: "session-1".to_string(),
            expected_fence: None,
            target_node_id: None,
            payload: json!({ "session_id": "session-from-payload" }),
        };
        assert_eq!(command_session_id(&envelope), Some("session-from-payload"));
    }

    #[test]
    fn ownership_fence_transition_rejects_stale_expectation() {
        assert!(ownership_fence_allows_transition(None, None));
        assert!(!ownership_fence_allows_transition(Some("fence-1"), None));
        assert!(ownership_fence_allows_transition(
            Some("fence-1"),
            Some("fence-1")
        ));
        assert!(!ownership_fence_allows_transition(
            Some("fence-2"),
            Some("fence-1")
        ));
    }

    #[test]
    fn stream_event_id_is_cluster_scoped_and_unique() {
        let first = next_stream_event_id("cluster-alpha");
        let second = next_stream_event_id("cluster-alpha");
        assert!(first.starts_with("cluster-alpha-"));
        assert!(second.starts_with("cluster-alpha-"));
        assert_ne!(first, second);
    }

    #[test]
    fn portproxy_auth_header_uses_vm_metadata_token() {
        let metadata = HashMap::from([(
            META_PORTPROXY_AUTH_TOKEN.to_string(),
            "guest-token".to_string(),
        )]);
        let value = portproxy_auth_header_from_metadata(&metadata)
            .expect("portproxy auth metadata should compile")
            .expect("portproxy auth metadata should be present");
        assert_eq!(value.to_str().expect("ascii header"), "Bearer guest-token");
    }

    #[test]
    fn request_with_portproxy_auth_inserts_authorization_metadata() {
        let metadata = HashMap::from([(
            META_PORTPROXY_AUTH_TOKEN.to_string(),
            "guest-token".to_string(),
        )]);
        let value = portproxy_auth_header_from_metadata(&metadata)
            .expect("portproxy auth metadata should compile")
            .expect("portproxy auth metadata should be present");
        let request = request_with_portproxy_auth((), Some(&value));
        assert_eq!(
            request
                .metadata()
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer guest-token")
        );
    }

    #[tokio::test]
    async fn vm_readiness_cache_requires_same_running_instance() {
        let state = VmReadinessState::new();
        let fingerprint = VmReadyFingerprint {
            started_at_unix_ms: 1000,
            rpc_port: 4242,
            portproxy_auth_token: Some("token-a".to_string()),
        };

        assert!(!state.is_ready("vm-1", &fingerprint).await);
        state.mark_ready("vm-1", fingerprint.clone()).await;
        assert!(state.is_ready("vm-1", &fingerprint).await);
        assert!(
            !state
                .is_ready(
                    "vm-1",
                    &VmReadyFingerprint {
                        started_at_unix_ms: 2000,
                        ..fingerprint.clone()
                    }
                )
                .await
        );
    }

    #[tokio::test]
    async fn vm_readiness_cache_can_be_cleared_after_transport_failure() {
        let state = VmReadinessState::new();
        let fingerprint = VmReadyFingerprint {
            started_at_unix_ms: 1000,
            rpc_port: 4242,
            portproxy_auth_token: None,
        };

        state.mark_ready("vm-1", fingerprint.clone()).await;
        state.clear_ready("vm-1").await;
        assert!(!state.is_ready("vm-1", &fingerprint).await);
    }

    #[test]
    fn exec_stream_event_serializes_identity_envelope_fields() {
        let event = ExecStreamEvent {
            cluster_id: "cluster-a".to_string(),
            logical_stream_id: "logical-stream-1".to_string(),
            stream_id: "logical-stream-1".to_string(),
            event_seq: 4,
            event_id: "cluster-a-event-123".to_string(),
            producer_epoch: 2,
            command_id: "cmd-1".to_string(),
            session_id: "session-1".to_string(),
            vm_id: "vm-1".to_string(),
            kind: "stdout".to_string(),
            data: b"ok".to_vec(),
            exit_code: None,
            timed_out: false,
            error: None,
            sequence: 4,
            emitted_by_node_id: "node-a".to_string(),
            emitted_at_unix_ms: 42,
        };

        let value = serde_json::to_value(event).expect("serialize event");
        assert_eq!(
            value.get("cluster_id").and_then(Value::as_str),
            Some("cluster-a")
        );
        assert_eq!(
            value.get("logical_stream_id").and_then(Value::as_str),
            Some("logical-stream-1")
        );
        assert_eq!(value.get("event_seq").and_then(Value::as_u64), Some(4));
        assert_eq!(
            value.get("event_id").and_then(Value::as_str),
            Some("cluster-a-event-123")
        );
        assert_eq!(value.get("producer_epoch").and_then(Value::as_u64), Some(2));
        assert_eq!(value.get("sequence").and_then(Value::as_u64), Some(4));
    }

    #[tokio::test]
    async fn exec_stream_input_preserves_control_after_eof_and_rejects_stale_owners() {
        let (sender, mut receiver) = mpsc::channel(2);
        let active = ActiveExecStream {
            request_tx: sender,
            last_input_seq: std::sync::Arc::new(Mutex::new(0)),
            session_id: "session".into(),
            vm_id: "vm".into(),
            node_id: "node".into(),
            producer_epoch: 3,
            daemon_name: "daemon".into(),
            auth: None,
            control_client: DaemonManagerClient::new(
                tonic::transport::Endpoint::from_static("http://127.0.0.1:9").connect_lazy(),
            ),
        };
        let streams = std::sync::Arc::new(Mutex::new(HashMap::from([(
            "stream".into(),
            active.clone(),
        )])));
        let payload = json!({"stream_id":"stream", "session_id":"session", "vm_id":"vm", "target_node_id":"node", "producer_epoch":3, "input_seq":1, "input_kind":"eof"});
        let envelope =
            serde_json::from_value(json!({"command_type":"exec.stream.input", "payload":payload}))
                .unwrap();
        handle_exec_stream_input_command(&envelope, &streams)
            .await
            .unwrap();
        assert!(matches!(
            receiver.recv().await.unwrap().request,
            Some(attach_daemon_request::Request::StdinEof(true))
        ));
        handle_exec_stream_input_command(&envelope, &streams)
            .await
            .unwrap();
        assert!(receiver.try_recv().is_err());
        assert!(streams.lock().await.contains_key("stream"));
        for (field, value) in [
            ("producer_epoch", json!(2)),
            ("session_id", json!("other")),
            ("vm_id", json!("other")),
            ("target_node_id", json!("other")),
        ] {
            let mut stale = payload.clone();
            stale[field] = value;
            stale["input_kind"] = json!("signal");
            stale["signal"] = json!(9);
            let stale: ExecStreamInputPayload = serde_json::from_value(stale).unwrap();
            assert!(
                validate_exec_input_identity(&active, &stale).is_err(),
                "accepted stale {field}"
            );
        }
        drop(receiver);
        let mut failed = payload;
        failed["input_kind"] = json!("stdin");
        failed["input_seq"] = json!(2);
        let envelope =
            serde_json::from_value(json!({"command_type":"exec.stream.input", "payload":failed}))
                .unwrap();
        assert!(
            handle_exec_stream_input_command(&envelope, &streams)
                .await
                .is_err()
        );
        assert_eq!(
            *active.last_input_seq.lock().await,
            1,
            "failed forwarding must not acknowledge input"
        );
    }

    #[test]
    fn exec_stream_start_payload_supports_legacy_stream_id_field() {
        let raw = json!({
            "stream_id": "legacy-stream",
            "session_id": "session-1",
            "vm_id": "vm-1",
            "command": "echo ok"
        });
        let payload: ExecStreamStartPayload = serde_json::from_value(raw).expect("decode payload");
        assert_eq!(payload.stream_id, "legacy-stream");
        assert!(payload.logical_stream_id.is_empty());
        assert_eq!(payload.resume_after_event_seq, 0);
        assert_eq!(payload.producer_epoch, 0);
    }
}
