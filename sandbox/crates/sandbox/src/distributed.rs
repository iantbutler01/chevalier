// @dive-file: Distributed control-plane adapter for etcd + NATS routing, admission, and durable control-event publication.
// @dive-rel: Consumed by crates/sandbox/src/lib.rs to keep facade APIs host-agnostic across local and distributed deployments.
// @dive-rel: Mirrors node/control semantics emitted by vmd/src/registry.rs and vmd/src/control_bus.rs.
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_nats::jetstream;
use async_nats::jetstream::AckKind;
use async_nats::jetstream::consumer::{AckPolicy, pull};
use etcd_client::{
    Client as EtcdClient, Compare, CompareOp, GetOptions, LeaseKeepAliveStream, LeaseKeeper,
    PutOptions, Txn, TxnOp,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::{DistributedControlConfig, Result, SandboxError};

const STORAGE_PROFILE_LOCAL_EPHEMERAL: &str = "local-ephemeral";
const STORAGE_PROFILE_DURABLE_SHARED: &str = "durable-shared";
const CONTINUITY_TIER_A: &str = "tier-a";
const CONTINUITY_TIER_B: &str = "tier-b";
const SHARED_MOUNT_PROFILE_LOCAL: &str = "local-path";
const CONTROL_ENVELOPE_SCHEMA_VERSION: &str = "v1";
const DEFAULT_SHARD_COUNT: u8 = 16;

#[derive(Clone)]
pub(crate) struct DistributedControlPlane {
    cfg: DistributedControlConfig,
    etcd: Arc<Mutex<EtcdClient>>,
    jetstream: jetstream::Context,
}

#[derive(Clone, Debug)]
pub(crate) struct SessionRoute {
    pub session_id: String,
    pub vm_id: String,
    pub endpoint: String,
    pub node_id: Option<String>,
    pub fork_id: Option<String>,
    pub ownership_fence: Option<String>,
    pub tenant_id: String,
    pub workspace_id: String,
    pub tier_b_eligible: bool,
    pub required_mount_profiles: Vec<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct NodeRoute {
    pub node_id: String,
    pub endpoint: String,
    pub max_active_vms: Option<usize>,
    pub storage_profile: String,
    pub continuity_tier: String,
    pub shared_mount_profiles: Vec<String>,
    pub degraded_mode: bool,
    pub admission_frozen: bool,
    pub region: String,
    pub zone: String,
    pub rack: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[allow(dead_code)]
pub(crate) struct ExecCommandResult {
    #[serde(default)]
    pub command_id: String,
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub vm_id: String,
    #[serde(default)]
    pub stdout: String,
    #[serde(default)]
    pub stderr: String,
    #[serde(default)]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub timed_out: bool,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ExecStreamEvent {
    #[serde(default)]
    pub cluster_id: String,
    #[serde(default)]
    pub logical_stream_id: String,
    #[serde(default)]
    pub event_seq: u64,
    #[serde(default)]
    pub event_id: String,
    #[serde(default)]
    pub producer_epoch: u64,
    #[serde(default)]
    pub stream_id: String,
    #[serde(default)]
    pub command_id: String,
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub vm_id: String,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub data: Vec<u8>,
    #[serde(default)]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub timed_out: bool,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub sequence: u64,
}

impl ExecStreamEvent {
    pub(crate) fn normalized_event_seq(&self) -> u64 {
        if self.event_seq == 0 {
            self.sequence
        } else {
            self.event_seq
        }
    }
}

pub(crate) struct ExecStreamSubscription {
    pub(crate) events: mpsc::Receiver<Result<ExecStreamEvent>>,
    pub(crate) started: oneshot::Receiver<Result<()>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SessionRouteRecord {
    session_id: String,
    vm_id: String,
    endpoint: String,
    #[serde(default)]
    node_id: Option<String>,
    #[serde(default)]
    fork_id: Option<String>,
    #[serde(default)]
    ownership_fence: Option<String>,
    #[serde(default)]
    tenant_id: String,
    #[serde(default)]
    workspace_id: String,
    #[serde(default)]
    tier_b_eligible: bool,
    #[serde(default)]
    required_mount_profiles: Vec<String>,
    #[serde(default)]
    updated_at_unix_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NodeRecord {
    #[serde(default)]
    node_id: String,
    endpoint: String,
    #[serde(default)]
    max_active_vms: Option<usize>,
    #[serde(default)]
    storage_profile: String,
    #[serde(default)]
    continuity_tier: String,
    #[serde(default)]
    shared_mount_profiles: Vec<String>,
    #[serde(default)]
    degraded_mode: bool,
    #[serde(default)]
    admission_frozen: bool,
    #[serde(default)]
    region: String,
    #[serde(default)]
    zone: String,
    #[serde(default)]
    rack: String,
    #[serde(default)]
    updated_at_unix_ms: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct PortAllocation {
    pub session_id: String,
    pub vm_id: String,
    pub endpoint: String,
    pub guest_port: u16,
    pub host_port: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PortAllocationRecord {
    session_id: String,
    vm_id: String,
    endpoint: String,
    guest_port: u16,
    host_port: u16,
    #[serde(default)]
    updated_at_unix_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OutboxRecord {
    kind: String,
    command_id: String,
    subject: String,
    payload: Value,
    #[serde(default)]
    created_at_unix_ms: u64,
    #[serde(default)]
    updated_at_unix_ms: u64,
    #[serde(default)]
    attempts: u64,
}

pub(crate) struct PortAllocationLease {
    stop_tx: Option<oneshot::Sender<()>>,
    join: Option<JoinHandle<()>>,
}

impl PortAllocationLease {
    pub(crate) async fn shutdown(mut self) {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.await;
        }
    }
}

impl DistributedControlPlane {
    pub(crate) fn allows_cross_node_recovery(&self) -> bool {
        self.cfg.allow_cross_node_recovery
    }
    pub(crate) async fn connect(cfg: DistributedControlConfig) -> Result<Self> {
        if cfg.etcd_endpoints.is_empty() {
            return Err(SandboxError::InvalidEndpoint(
                "distributed control requires at least one etcd endpoint".to_string(),
            ));
        }
        let etcd = EtcdClient::connect(cfg.etcd_endpoints.clone(), None)
            .await
            .map_err(|err| {
                SandboxError::DaemonUnavailable(format!("failed connecting to etcd: {err}"))
            })?;
        let normalized = normalize_cfg(cfg);
        let nats =
            connect_nats(&normalized.nats_url, normalized.nats_auth_token.as_deref()).await?;
        let jetstream = jetstream::new(nats.clone());
        ensure_control_stream(&jetstream, &normalized).await?;
        let plane = Self {
            cfg: normalized,
            etcd: Arc::new(Mutex::new(etcd)),
            jetstream,
        };
        spawn_outbox_replay_worker(plane.clone());
        Ok(plane)
    }

    pub(crate) fn cluster_id(&self) -> &str {
        self.cfg.cluster_id.as_str()
    }

    pub(crate) async fn get_session_route(&self, session_id: &str) -> Result<Option<SessionRoute>> {
        let started = tokio::time::Instant::now();
        let route_key = self.session_key(session_id);
        let legacy_route_key = self.legacy_session_key(session_id);
        let fence_key = self.session_fence_key(session_id);
        let legacy_fence_key = self.legacy_session_fence_key(session_id);
        let mut client = self.etcd.lock().await;
        let mut response = client.get(route_key.clone(), None).await.map_err(|err| {
            SandboxError::DaemonUnavailable(format!("etcd get session route failed: {err}"))
        })?;
        if response.kvs().is_empty() && route_key != legacy_route_key {
            response = client.get(legacy_route_key, None).await.map_err(|err| {
                SandboxError::DaemonUnavailable(format!(
                    "etcd get legacy session route failed: {err}"
                ))
            })?;
        }
        let route_get_ms = started.elapsed().as_millis() as u64;
        let Some(kv) = response.kvs().first() else {
            tracing::info!(
                session_id = %session_id,
                route_get_ms,
                total_ms = started.elapsed().as_millis() as u64,
                "distributed.get_session_route miss"
            );
            return Ok(None);
        };
        let mut route = decode_session_route(kv.value())?;
        let mut fence_response = client.get(fence_key.clone(), None).await.map_err(|err| {
            SandboxError::DaemonUnavailable(format!("etcd get session fence failed: {err}"))
        })?;
        if fence_response.kvs().is_empty() && fence_key != legacy_fence_key {
            fence_response = client.get(legacy_fence_key, None).await.map_err(|err| {
                SandboxError::DaemonUnavailable(format!(
                    "etcd get legacy session fence failed: {err}"
                ))
            })?;
        }
        if let Some(fence_kv) = fence_response.kvs().first() {
            if let Ok(fence) = String::from_utf8(fence_kv.value().to_vec()) {
                if !fence.trim().is_empty() {
                    route.ownership_fence = Some(fence);
                }
            }
        }
        tracing::info!(
            session_id = %session_id,
            route_get_ms,
            fence_get_ms = started.elapsed().as_millis() as u64 - route_get_ms,
            total_ms = started.elapsed().as_millis() as u64,
            "distributed.get_session_route ok"
        );
        Ok(Some(route))
    }

    pub(crate) async fn put_session_route(
        &self,
        route: SessionRoute,
        expected_fence: Option<&str>,
    ) -> Result<SessionRoute> {
        let required_mount_profiles =
            normalize_shared_mount_profiles(&route.required_mount_profiles);
        let route_key = self.session_key(&route.session_id);
        let fence_key = self.session_fence_key(&route.session_id);
        let next_fence = Uuid::new_v4().to_string();
        let payload = serde_json::to_vec(&SessionRouteRecord {
            session_id: route.session_id.clone(),
            vm_id: route.vm_id.clone(),
            endpoint: route.endpoint.clone(),
            node_id: route.node_id.clone(),
            fork_id: route.fork_id.clone(),
            ownership_fence: Some(next_fence.clone()),
            tenant_id: route.tenant_id.clone(),
            workspace_id: route.workspace_id.clone(),
            tier_b_eligible: route.tier_b_eligible,
            required_mount_profiles: required_mount_profiles.clone(),
            updated_at_unix_ms: unix_millis(),
        })
        .map_err(|err| SandboxError::InvalidResponse(format!("serialize session route: {err}")))?;
        let mut client = self.etcd.lock().await;
        let legacy_fence_key = self.legacy_session_fence_key(&route.session_id);
        let mut compare_key = fence_key.clone();
        let mut current_fence = read_fence_value(&mut client, &fence_key).await?;
        if current_fence.is_none() && fence_key != legacy_fence_key {
            current_fence = read_fence_value(&mut client, &legacy_fence_key).await?;
            if current_fence.is_some() {
                compare_key = legacy_fence_key.clone();
            }
        }
        if !ownership_fence_allows_transition(current_fence.as_deref(), expected_fence) {
            let current_display = current_fence.as_deref().unwrap_or("<none>");
            let expected_display = expected_fence.unwrap_or("<none>");
            return Err(SandboxError::FenceConflict(format!(
                "session route ownership fence mismatch: expected={expected_display} current={current_display}"
            )));
        }
        let compare = match expected_fence {
            Some(fence) => Compare::value(compare_key, CompareOp::Equal, fence),
            None => Compare::version(fence_key.clone(), CompareOp::Equal, 0),
        };
        let txn = Txn::new().when(vec![compare]).and_then(vec![
            TxnOp::put(route_key, payload, None),
            TxnOp::put(fence_key.clone(), next_fence.clone(), None),
            TxnOp::delete(self.legacy_session_key(&route.session_id), None),
            TxnOp::delete(self.legacy_session_fence_key(&route.session_id), None),
        ]);
        let response = client.txn(txn).await.map_err(|err| {
            SandboxError::DaemonUnavailable(format!("etcd put session route txn failed: {err}"))
        })?;
        if !response.succeeded() {
            let current_fence = read_fence_value(&mut client, &fence_key).await?;
            let current_display = current_fence.as_deref().unwrap_or("<none>");
            let expected_display = expected_fence.unwrap_or("<none>");
            return Err(SandboxError::FenceConflict(format!(
                "session route ownership fence mismatch: expected={expected_display} current={current_display}"
            )));
        }

        let mut updated = route;
        updated.required_mount_profiles = required_mount_profiles;
        updated.ownership_fence = Some(next_fence);
        Ok(updated)
    }

    pub(crate) async fn rebind_candidates_for_session(
        &self,
        session_id: &str,
        from_endpoint: &str,
    ) -> Result<Vec<NodeRoute>> {
        if !self.allows_cross_node_recovery() {
            return Ok(Vec::new());
        }
        let routes = self.list_node_routes().await?;
        if routes.is_empty() {
            return Ok(Vec::new());
        }
        let session_route = self.get_session_route(session_id).await?;
        let session_routes = self.session_route_records().await?;
        Ok(eligible_rebind_routes(
            &routes,
            &session_routes,
            session_route.as_ref(),
            from_endpoint,
            self.cfg.required_storage_profile.as_deref(),
            self.cfg.required_continuity_tier.as_deref(),
            self.cfg.allow_tier_a_degraded,
        ))
    }

    pub(crate) async fn delete_session_route(
        &self,
        session_id: &str,
        expected_fence: Option<&str>,
    ) -> Result<()> {
        let route_key = self.session_key(session_id);
        let fence_key = self.session_fence_key(session_id);
        let legacy_route_key = self.legacy_session_key(session_id);
        let legacy_fence_key = self.legacy_session_fence_key(session_id);
        let mut client = self.etcd.lock().await;
        if let Some(expected) = expected_fence {
            let mut compare_key = fence_key.clone();
            let mut current_fence = read_fence_value(&mut client, &fence_key).await?;
            if current_fence.is_none() && fence_key != legacy_fence_key {
                current_fence = read_fence_value(&mut client, &legacy_fence_key).await?;
                if current_fence.is_some() {
                    compare_key = legacy_fence_key.clone();
                }
            }
            if !ownership_fence_allows_transition(current_fence.as_deref(), Some(expected)) {
                let current_display = current_fence.as_deref().unwrap_or("<none>");
                return Err(SandboxError::FenceConflict(format!(
                    "session route delete ownership fence mismatch: expected={expected} current={current_display}"
                )));
            }
            let txn = Txn::new()
                .when(vec![Compare::value(
                    compare_key,
                    CompareOp::Equal,
                    expected,
                )])
                .and_then(vec![
                    TxnOp::delete(route_key, None),
                    TxnOp::delete(fence_key.clone(), None),
                    TxnOp::delete(legacy_route_key, None),
                    TxnOp::delete(legacy_fence_key, None),
                ]);
            let response = client.txn(txn).await.map_err(|err| {
                SandboxError::DaemonUnavailable(format!(
                    "etcd delete session route txn failed: {err}"
                ))
            })?;
            if !response.succeeded() {
                let current_fence = read_fence_value(&mut client, &fence_key).await?;
                let current_display = current_fence.as_deref().unwrap_or("<none>");
                return Err(SandboxError::FenceConflict(format!(
                    "session route delete ownership fence mismatch: expected={expected} current={current_display}"
                )));
            }
            return Ok(());
        }

        client.delete(route_key, None).await.map_err(|err| {
            SandboxError::DaemonUnavailable(format!("etcd delete session route failed: {err}"))
        })?;
        client.delete(fence_key, None).await.map_err(|err| {
            SandboxError::DaemonUnavailable(format!("etcd delete session fence failed: {err}"))
        })?;
        if let Err(err) = client.delete(legacy_route_key, None).await {
            tracing::warn!(error = %err, "failed to delete legacy session route key");
        }
        if let Err(err) = client.delete(legacy_fence_key, None).await {
            tracing::warn!(error = %err, "failed to delete legacy session fence key");
        }
        Ok(())
    }

    pub(crate) async fn list_node_routes(&self) -> Result<Vec<NodeRoute>> {
        let key_prefix = self.nodes_prefix();
        let mut client = self.etcd.lock().await;
        let response = client
            .get(key_prefix, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(|err| {
                SandboxError::DaemonUnavailable(format!("etcd list nodes failed: {err}"))
            })?;

        let mut routes = Vec::new();
        for kv in response.kvs() {
            let key = String::from_utf8_lossy(kv.key()).to_string();
            if let Ok(route) = decode_node_route(&key, kv.value()) {
                routes.push(route);
            }
        }
        routes.sort_by(|a, b| a.node_id.cmp(&b.node_id).then(a.endpoint.cmp(&b.endpoint)));
        routes.dedup_by(|a, b| a.node_id == b.node_id && a.endpoint == b.endpoint);
        Ok(routes)
    }

    pub(crate) async fn select_node_for_session_with_eligibility(
        &self,
        session_id: &str,
        tenant_id: &str,
        workspace_id: &str,
        tier_b_eligible: bool,
        required_mount_profiles: &[String],
    ) -> Result<NodeRoute> {
        let routes = self.list_node_routes().await?;
        if routes.is_empty() {
            return Err(SandboxError::DaemonUnavailable(
                "distributed control has no registered nodes in etcd".to_string(),
            ));
        }

        let tenant_id = normalize_scope_value(tenant_id);
        let workspace_id = normalize_scope_value(workspace_id);
        let session_routes = self.session_route_records().await?;
        if let Err(err) = self
            .enforce_admission_budgets(&session_routes, &tenant_id, &workspace_id)
            .await
        {
            let _ = self
                .publish_event(
                    "admission.decision",
                    json!({
                        "result": "rejected",
                        "reason": "budget_exhausted",
                        "session_id": session_id,
                        "tenant_id": tenant_id,
                        "workspace_id": workspace_id,
                        "error": err.to_string(),
                    }),
                )
                .await;
            return Err(err);
        }
        let usage = session_counts_by_endpoint(&session_routes);
        let required_continuity_tier = if tier_b_eligible {
            self.cfg.required_continuity_tier.as_deref()
        } else {
            None
        };
        let allow_tier_a_degraded = if tier_b_eligible {
            self.cfg.allow_tier_a_degraded
        } else {
            true
        };
        let eligible = eligible_routes_with_profile(
            &routes,
            &usage,
            self.cfg.required_storage_profile.as_deref(),
            required_continuity_tier,
            allow_tier_a_degraded,
            required_mount_profiles,
        );
        if eligible.is_empty() {
            let mut details = Vec::new();
            for route in &routes {
                let used = usage.get(&route.endpoint).copied().unwrap_or(0);
                let limit = route
                    .max_active_vms
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unbounded".to_string());
                details.push(format!(
                    "{}@{} used={} limit={} storage_profile={} continuity_tier={} shared_mount_profiles={:?} degraded_mode={} admission_frozen={}",
                    route.node_id,
                    route.endpoint,
                    used,
                    limit,
                    route.storage_profile,
                    route.continuity_tier,
                    route.shared_mount_profiles,
                    route.degraded_mode,
                    route.admission_frozen
                ));
            }
            let required_profile = self
                .cfg
                .required_storage_profile
                .as_deref()
                .unwrap_or("any");
            let required_tier = self
                .cfg
                .required_continuity_tier
                .as_deref()
                .unwrap_or("any");
            let retry_after_ms = self.cfg.admission_retry_after_ms.max(1);
            let _ = self
                .publish_event(
                    "admission.decision",
                    json!({
                        "result": "rejected",
                        "reason": "no_eligible_nodes",
                        "session_id": session_id,
                        "tenant_id": tenant_id,
                        "workspace_id": workspace_id,
                        "retry_after_ms": retry_after_ms,
                        "candidate_count": routes.len(),
                        "tier_b_eligible": tier_b_eligible,
                        "required_mount_profiles": required_mount_profiles,
                    }),
                )
                .await;
            return Err(SandboxError::ResourceExhausted(format!(
                "no eligible distributed nodes (required_storage_profile={}, required_continuity_tier={}, allow_tier_a_degraded={}, retry_after_ms={}, tenant_id={}, workspace_id={}, {})",
                required_profile,
                if tier_b_eligible {
                    required_tier
                } else {
                    "any"
                },
                allow_tier_a_degraded,
                retry_after_ms,
                tenant_id,
                workspace_id,
                details.join(", ")
            )));
        }

        let node_by_endpoint: HashMap<String, NodeRoute> = routes
            .iter()
            .cloned()
            .map(|route| (route.endpoint.clone(), route))
            .collect();
        let mut zone_usage: HashMap<String, usize> = HashMap::new();
        let mut rack_usage: HashMap<String, usize> = HashMap::new();
        let mut workspace_endpoint_usage: HashMap<String, usize> = HashMap::new();
        for route in &session_routes {
            if let Some(node) = node_by_endpoint.get(&route.endpoint) {
                *zone_usage.entry(node.zone.clone()).or_insert(0) += 1;
                *rack_usage.entry(node.rack.clone()).or_insert(0) += 1;
            }
            if route.tenant_id == tenant_id && route.workspace_id == workspace_id {
                *workspace_endpoint_usage
                    .entry(route.endpoint.clone())
                    .or_insert(0) += 1;
            }
        }

        let mut ranked = eligible;
        ranked.sort_by(|a, b| {
            let a_workspace = workspace_endpoint_usage
                .get(&a.endpoint)
                .copied()
                .unwrap_or(0);
            let b_workspace = workspace_endpoint_usage
                .get(&b.endpoint)
                .copied()
                .unwrap_or(0);
            let a_zone = zone_usage.get(&a.zone).copied().unwrap_or(0);
            let b_zone = zone_usage.get(&b.zone).copied().unwrap_or(0);
            let a_rack = rack_usage.get(&a.rack).copied().unwrap_or(0);
            let b_rack = rack_usage.get(&b.rack).copied().unwrap_or(0);
            let a_total = usage.get(&a.endpoint).copied().unwrap_or(0);
            let b_total = usage.get(&b.endpoint).copied().unwrap_or(0);
            a_workspace
                .cmp(&b_workspace)
                .then(a_zone.cmp(&b_zone))
                .then(a_rack.cmp(&b_rack))
                .then(a_total.cmp(&b_total))
                .then(score_node(session_id, &a.node_id).cmp(&score_node(session_id, &b.node_id)))
        });
        let selected = ranked.first().cloned().ok_or_else(|| {
            SandboxError::DaemonUnavailable("no ranked node candidate".to_string())
        })?;
        let _ = self
            .publish_event(
                "admission.decision",
                json!({
                    "result": "accepted",
                    "session_id": session_id,
                    "tenant_id": tenant_id,
                    "workspace_id": workspace_id,
                    "selected_node_id": selected.node_id,
                    "selected_zone": selected.zone,
                    "selected_rack": selected.rack,
                    "selected_region": selected.region,
                    "selected_endpoint": selected.endpoint,
                    "workspace_endpoint_load": workspace_endpoint_usage.get(&selected.endpoint).copied().unwrap_or(0),
                    "zone_load": zone_usage.get(&selected.zone).copied().unwrap_or(0),
                    "rack_load": rack_usage.get(&selected.rack).copied().unwrap_or(0),
                    "endpoint_load": usage.get(&selected.endpoint).copied().unwrap_or(0),
                    "tier_b_eligible": tier_b_eligible,
                    "required_continuity_tier": if tier_b_eligible {
                        required_continuity_tier.unwrap_or("any")
                    } else {
                        "any"
                    },
                    "required_mount_profiles": required_mount_profiles,
                    "allow_tier_a_degraded": allow_tier_a_degraded,
                }),
            )
            .await;
        Ok(selected)
    }

    pub(crate) async fn publish_event(&self, event_name: &str, payload: Value) -> Result<()> {
        // @dive: Control events are published on the `.evt.*` namespace so stream subject filters capture them durably.
        let subject = format!("{}.evt.{event_name}", self.cfg.nats_subject_prefix);
        let envelope = build_event_envelope(event_name, payload);
        let bytes = serde_json::to_vec(&envelope).map_err(|err| {
            SandboxError::InvalidResponse(format!("serialize control event: {err}"))
        })?;
        self.jetstream
            .publish(subject, bytes.into())
            .await
            .map_err(|err| {
                SandboxError::DaemonUnavailable(format!("jetstream publish failed: {err}"))
            })?
            .await
            .map_err(|err| {
                SandboxError::DaemonUnavailable(format!("jetstream publish ack failed: {err}"))
            })?;
        Ok(())
    }

    pub(crate) async fn publish_command(
        &self,
        command_type: &str,
        ordering_key: &str,
        payload: Value,
    ) -> Result<String> {
        let command_id = Uuid::new_v4().to_string();
        let idempotency_key = payload
            .get("idempotency_key")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| derive_idempotency_key(command_type, ordering_key, &payload));
        let envelope = build_command_envelope(
            &command_id,
            command_type,
            ordering_key,
            idempotency_key.as_str(),
            &payload,
        );
        let subject = format!("{}.cmd.{command_type}", self.cfg.nats_subject_prefix);
        let outbox = OutboxRecord {
            kind: "command".to_string(),
            command_id: command_id.clone(),
            subject,
            payload: envelope,
            created_at_unix_ms: unix_millis(),
            updated_at_unix_ms: unix_millis(),
            attempts: 0,
        };
        let publish_started = tokio::time::Instant::now();
        tracing::info!(
            command_type = %command_type,
            command_id = %outbox.command_id,
            subject = %outbox.subject,
            "distributed.publish_command start"
        );
        self.put_outbox_record(&outbox).await?;
        let outbox_put_ms = publish_started.elapsed().as_millis() as u64;
        self.publish_outbox_record(&outbox).await?;
        let publish_done_ms = publish_started.elapsed().as_millis() as u64;
        self.delete_outbox_record(&outbox.command_id).await?;
        tracing::info!(
            command_type = %command_type,
            command_id = %outbox.command_id,
            outbox_put_ms,
            nats_publish_ms = publish_done_ms - outbox_put_ms,
            total_ms = publish_started.elapsed().as_millis() as u64,
            "distributed.publish_command ok"
        );
        Ok(command_id)
    }

    pub(crate) async fn node_id_for_endpoint(&self, endpoint: &str) -> Result<Option<String>> {
        let normalized = endpoint.trim();
        if normalized.is_empty() {
            return Ok(None);
        }
        let routes = self.list_node_routes().await?;
        Ok(routes
            .into_iter()
            .find(|route| route.endpoint.trim() == normalized)
            .map(|route| route.node_id))
    }

    #[allow(dead_code)]
    pub(crate) async fn wait_for_exec_result(
        &self,
        command_id: &str,
        timeout: Duration,
    ) -> Result<ExecCommandResult> {
        let command_id = command_id.trim();
        if command_id.is_empty() {
            return Err(SandboxError::InvalidResponse(
                "exec result wait requires non-empty command_id".to_string(),
            ));
        }
        if timeout.is_zero() {
            return Err(SandboxError::InvalidConfig(
                "exec result wait timeout must be positive".to_string(),
            ));
        }

        let subject = format!(
            "{}.evt.exec.result.{}",
            self.cfg.nats_subject_prefix,
            sanitize_subject_token(command_id)
        );
        let stream = self
            .jetstream
            .get_stream(self.cfg.nats_stream_name.clone())
            .await
            .map_err(|err| {
                SandboxError::DaemonUnavailable(format!(
                    "fetch control stream for exec result failed: {err}"
                ))
            })?;
        let consumer_name = format!("exec-result-{}", Uuid::new_v4().simple());
        let consumer = stream
            .create_consumer(pull::Config {
                durable_name: Some(consumer_name.clone()),
                ack_policy: AckPolicy::Explicit,
                ack_wait: timeout,
                max_deliver: 3,
                filter_subject: subject.clone(),
                max_ack_pending: 32,
                inactive_threshold: Duration::from_secs(30),
                ..Default::default()
            })
            .await
            .map_err(|err| {
                SandboxError::DaemonUnavailable(format!(
                    "create exec result consumer failed: {err}"
                ))
            })?;
        // @dive: Ephemeral per-command consumers preserve exactly-once correlation semantics without coupling callers to a shared mutable cursor.
        let mut messages = consumer.messages().await.map_err(|err| {
            SandboxError::DaemonUnavailable(format!(
                "start exec result consumer stream failed: {err}"
            ))
        })?;

        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                if let Err(err) = stream.delete_consumer(&consumer_name).await {
                    tracing::warn!(
                        error = %err,
                        consumer = %consumer_name,
                        command_id = %command_id,
                        "failed to delete timed-out exec result consumer"
                    );
                }
                return Err(SandboxError::DaemonUnavailable(format!(
                    "timed out waiting for exec result command_id={command_id}"
                )));
            }
            let remaining = deadline.saturating_duration_since(now);
            let maybe_message = tokio::time::timeout(remaining, messages.next()).await;
            let maybe_message = match maybe_message {
                Ok(message) => message,
                Err(_) => {
                    if let Err(err) = stream.delete_consumer(&consumer_name).await {
                        tracing::warn!(
                            error = %err,
                            consumer = %consumer_name,
                            command_id = %command_id,
                            "failed to delete exec result consumer after timeout"
                        );
                    }
                    return Err(SandboxError::DaemonUnavailable(format!(
                        "timed out waiting for exec result command_id={command_id}"
                    )));
                }
            };
            let Some(message) = maybe_message else {
                continue;
            };
            let message = message.map_err(|err| {
                SandboxError::DaemonUnavailable(format!(
                    "exec result consumer yielded error: {err}"
                ))
            })?;

            let parsed = decode_exec_result_payload(&message.payload)?;
            if let Err(err) = message.ack().await {
                tracing::warn!(
                    error = %err,
                    command_id = %command_id,
                    "failed to ack exec result message"
                );
            }
            if parsed.command_id.trim() == command_id {
                if let Err(err) = stream.delete_consumer(&consumer_name).await {
                    tracing::warn!(
                        error = %err,
                        consumer = %consumer_name,
                        command_id = %command_id,
                        "failed to delete exec result consumer after successful match"
                    );
                }
                return Ok(parsed);
            }
        }
    }

    pub(crate) async fn subscribe_exec_stream_events(
        &self,
        stream_id: &str,
        idle_timeout: Duration,
        resume_after_event_seq: Option<u64>,
        expect_started_event: bool,
    ) -> Result<ExecStreamSubscription> {
        let subscribe_started = tokio::time::Instant::now();
        let stream_id = stream_id.trim();
        if stream_id.is_empty() {
            return Err(SandboxError::InvalidResponse(
                "exec stream subscription requires non-empty stream_id".to_string(),
            ));
        }
        if idle_timeout.is_zero() {
            return Err(SandboxError::InvalidConfig(
                "exec stream subscription idle timeout must be positive".to_string(),
            ));
        }
        let resume_after_event_seq = resume_after_event_seq.unwrap_or_default();

        let subject = format!(
            "{}.evt.exec.stream.{}",
            self.cfg.nats_subject_prefix,
            sanitize_subject_token(stream_id)
        );
        let stream = self
            .jetstream
            .get_stream(self.cfg.nats_stream_name.clone())
            .await
            .map_err(|err| {
                SandboxError::DaemonUnavailable(format!(
                    "fetch control stream for exec stream subscription failed: {err}"
                ))
            })?;
        tracing::info!(
            stream_id = %stream_id,
            get_stream_ms = subscribe_started.elapsed().as_millis() as u64,
            "SUBSCRIBE_T1 get_stream_done"
        );
        let consumer_name = format!("exec-stream-{}", Uuid::new_v4().simple());
        let consumer = stream
            .create_consumer(pull::Config {
                durable_name: Some(consumer_name.clone()),
                ack_policy: AckPolicy::Explicit,
                ack_wait: idle_timeout,
                max_deliver: 3,
                filter_subject: subject.clone(),
                max_ack_pending: 256,
                inactive_threshold: Duration::from_secs(45),
                ..Default::default()
            })
            .await
            .map_err(|err| {
                SandboxError::DaemonUnavailable(format!(
                    "create exec stream consumer failed: {err}"
                ))
            })?;
        tracing::info!(
            stream_id = %stream_id,
            create_consumer_ms = subscribe_started.elapsed().as_millis() as u64,
            consumer = %consumer_name,
            "SUBSCRIBE_T2 create_consumer_done"
        );
        // @dive: Every exec stream uses an isolated ephemeral consumer so ordered stream semantics survive control-plane fan-in without shared cursors.
        let mut messages = consumer.messages().await.map_err(|err| {
            SandboxError::DaemonUnavailable(format!(
                "start exec stream consumer stream failed: {err}"
            ))
        })?;

        let (event_tx, event_rx) = mpsc::channel(256);
        let (started_tx, started_rx) = oneshot::channel();
        let mut started_tx = Some(started_tx);
        let stream_name = self.cfg.nats_stream_name.clone();
        let cleanup_consumer_name = consumer_name.clone();
        let stream_id = stream_id.to_string();
        let jetstream = self.jetstream.clone();
        let subscribe_loop_started = subscribe_started;
        if !expect_started_event {
            if let Some(tx) = started_tx.take() {
                let _ = tx.send(Ok(()));
            }
        }

        tokio::spawn(async move {
            loop {
                let next_message = tokio::time::timeout(idle_timeout, messages.next()).await;
                let maybe_message = match next_message {
                    Ok(message) => message,
                    Err(_) => {
                        let msg = "timed out waiting for distributed exec stream event".to_string();
                        if let Some(tx) = started_tx.take() {
                            let _ = tx.send(Err(SandboxError::DaemonUnavailable(msg.clone())));
                        }
                        let _ = event_tx
                            .send(Err(SandboxError::DaemonUnavailable(msg)))
                            .await;
                        break;
                    }
                };

                let Some(maybe_message) = maybe_message else {
                    let msg =
                        "distributed exec stream event consumer ended unexpectedly".to_string();
                    if let Some(tx) = started_tx.take() {
                        let _ = tx.send(Err(SandboxError::DaemonUnavailable(msg.clone())));
                    }
                    let _ = event_tx
                        .send(Err(SandboxError::DaemonUnavailable(msg)))
                        .await;
                    break;
                };

                let message = match maybe_message {
                    Ok(message) => message,
                    Err(err) => {
                        let msg = format!("distributed exec stream consumer yielded error: {err}");
                        if let Some(tx) = started_tx.take() {
                            let _ = tx.send(Err(SandboxError::DaemonUnavailable(msg.clone())));
                        }
                        let _ = event_tx
                            .send(Err(SandboxError::DaemonUnavailable(msg)))
                            .await;
                        break;
                    }
                };

                let event = match decode_exec_stream_event_payload_for_stream(
                    &message.payload,
                    stream_id.as_str(),
                ) {
                    Ok(event) => event,
                    Err(err) => {
                        if let Err(ack_err) = message.ack_with(AckKind::Term).await {
                            tracing::warn!(
                                error = %ack_err,
                                stream_id = %stream_id,
                                consumer = %cleanup_consumer_name,
                                "failed to terminate invalid distributed exec stream event"
                            );
                        }
                        if let Some(tx) = started_tx.take() {
                            let _ = tx.send(Err(SandboxError::DaemonUnavailable(format!("{err}"))));
                        }
                        let _ = event_tx.send(Err(err)).await;
                        break;
                    }
                };
                let event_seq = event.normalized_event_seq();
                // @dive: Resume semantics are forward-only; once a consumer checkpoint exists, replayed event_seq frames are dropped.
                if should_drop_replayed_event(event_seq, resume_after_event_seq) {
                    if let Err(err) = message.ack().await {
                        tracing::warn!(
                            error = %err,
                            stream_id = %stream_id,
                            consumer = %cleanup_consumer_name,
                            event_seq = event_seq,
                            "failed to ack dropped replayed distributed exec stream event"
                        );
                    }
                    continue;
                }

                if let Some(tx) = started_tx.take() {
                    tracing::info!(
                        stream_id = %stream_id,
                        first_event_kind = %event.kind,
                        first_event_ms = subscribe_loop_started.elapsed().as_millis() as u64,
                        "SUBSCRIBE_T3 first_event_received"
                    );
                    if expect_started_event && event.kind == "error" {
                        let err = SandboxError::DaemonUnavailable(
                            event
                                .error
                                .clone()
                                .filter(|value| !value.trim().is_empty())
                                .unwrap_or_else(|| {
                                    "distributed exec stream returned start error".to_string()
                                }),
                        );
                        let _ = tx.send(Err(err));
                    } else {
                        let _ = tx.send(Ok(()));
                    }
                }

                let terminal = matches!(event.kind.as_str(), "exit" | "timeout" | "error");
                if event_tx.send(Ok(event)).await.is_err() {
                    break;
                }
                if let Err(err) = message.ack().await {
                    tracing::warn!(
                        error = %err,
                        stream_id = %stream_id,
                        consumer = %cleanup_consumer_name,
                        event_seq = event_seq,
                        "failed to ack delivered distributed exec stream event"
                    );
                }
                if terminal {
                    break;
                }
            }

            if let Ok(stream) = jetstream.get_stream(stream_name).await {
                let delete_started = tokio::time::Instant::now();
                if let Err(err) = stream.delete_consumer(&cleanup_consumer_name).await {
                    tracing::warn!(
                        error = %err,
                        consumer = %cleanup_consumer_name,
                        stream_id = %stream_id,
                        "failed to delete distributed exec stream consumer"
                    );
                } else {
                    tracing::info!(
                        consumer = %cleanup_consumer_name,
                        stream_id = %stream_id,
                        delete_consumer_ms = delete_started.elapsed().as_millis() as u64,
                        total_ms = subscribe_loop_started.elapsed().as_millis() as u64,
                        "SUBSCRIBE_T4 delete_consumer_done"
                    );
                }
            }
        });

        Ok(ExecStreamSubscription {
            events: event_rx,
            started: started_rx,
        })
    }

    pub(crate) async fn acquire_port_lease(
        &self,
        allocation: PortAllocation,
    ) -> Result<PortAllocationLease> {
        const DEFAULT_LEASE_TTL_SECS: i64 = 30;
        let key = self.port_key(&allocation.vm_id, allocation.host_port);
        let mut lease_id = put_port_allocation_with_new_lease(
            &self.etcd,
            &key,
            &allocation,
            DEFAULT_LEASE_TTL_SECS,
        )
        .await?;

        let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
        let etcd = Arc::clone(&self.etcd);
        let key_for_task = key.clone();
        let allocation_for_task = allocation.clone();
        let interval = lease_interval(DEFAULT_LEASE_TTL_SECS);
        let join = tokio::spawn(async move {
            let mut keepalive_client = {
                let client = etcd.lock().await;
                client.clone()
            };
            let mut keepalive =
                match start_port_allocation_keepalive(&mut keepalive_client, lease_id).await {
                    Ok(keepalive) => Some(keepalive),
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            port_key = %key_for_task,
                            lease_id,
                            "failed to start port allocation lease keepalive"
                        );
                        None
                    }
                };
            loop {
                tokio::select! {
                    _ = &mut stop_rx => break,
                    _ = tokio::time::sleep(interval) => {
                        let keepalive_result = match keepalive.as_mut() {
                            Some(keepalive) => keep_port_allocation_alive(keepalive).await,
                            None => Err(SandboxError::DaemonUnavailable(
                                "port allocation lease keepalive is not active".to_string(),
                            )),
                        };
                        if let Err(err) = keepalive_result {
                            tracing::warn!(
                                error = %err,
                                port_key = %key_for_task,
                                lease_id,
                                "failed to keep port allocation lease alive; refreshing allocation"
                            );
                            match put_port_allocation_with_new_lease(
                                &etcd,
                                &key_for_task,
                                &allocation_for_task,
                                DEFAULT_LEASE_TTL_SECS,
                            ).await {
                                Ok(next_lease_id) => {
                                    lease_id = next_lease_id;
                                    let mut next_keepalive_client = {
                                        let client = etcd.lock().await;
                                        client.clone()
                                    };
                                    keepalive = match start_port_allocation_keepalive(
                                        &mut next_keepalive_client,
                                        lease_id,
                                    ).await {
                                        Ok(next_keepalive) => Some(next_keepalive),
                                        Err(err) => {
                                            tracing::warn!(
                                                error = %err,
                                                port_key = %key_for_task,
                                                lease_id,
                                                "failed to restart port allocation lease keepalive"
                                            );
                                            None
                                        }
                                    };
                                }
                                Err(err) => {
                                    tracing::warn!(
                                        error = %err,
                                        port_key = %key_for_task,
                                        "failed to refresh port allocation lease"
                                    );
                                }
                            }
                        }
                    }
                }
            }

            let _ = delete_etcd_key(&etcd, &key_for_task).await;
        });

        Ok(PortAllocationLease {
            stop_tx: Some(stop_tx),
            join: Some(join),
        })
    }

    fn session_key(&self, session_id: &str) -> String {
        let shard = shard_for_key(session_id, DEFAULT_SHARD_COUNT);
        format!("{}{shard:02}/{session_id}", self.sessions_prefix())
    }

    fn legacy_session_key(&self, session_id: &str) -> String {
        format!("{}{session_id}", self.sessions_prefix())
    }

    fn nodes_prefix(&self) -> String {
        format!("{}/nodes/", self.cfg.etcd_prefix)
    }

    fn sessions_prefix(&self) -> String {
        format!("{}/sessions/", self.cfg.etcd_prefix)
    }

    fn session_fence_key(&self, session_id: &str) -> String {
        let shard = shard_for_key(session_id, DEFAULT_SHARD_COUNT);
        format!("{}{shard:02}/{session_id}", self.session_fences_prefix())
    }

    fn legacy_session_fence_key(&self, session_id: &str) -> String {
        format!("{}{session_id}", self.session_fences_prefix())
    }

    fn session_fences_prefix(&self) -> String {
        format!("{}/session_fences/", self.cfg.etcd_prefix)
    }

    fn ports_prefix(&self) -> String {
        format!("{}/ports/", self.cfg.etcd_prefix)
    }

    fn outbox_prefix(&self) -> String {
        format!("{}/outbox/", self.cfg.etcd_prefix)
    }

    fn outbox_pending_prefix(&self) -> String {
        format!("{}pending/", self.outbox_prefix())
    }

    fn outbox_pending_key(&self, command_id: &str) -> String {
        format!("{}{}", self.outbox_pending_prefix(), command_id)
    }

    fn port_key(&self, vm_id: &str, host_port: u16) -> String {
        format!("{}{}/{}", self.ports_prefix(), vm_id, host_port)
    }

    async fn session_route_records(&self) -> Result<Vec<SessionRouteRecord>> {
        let prefix = self.sessions_prefix();
        let mut client = self.etcd.lock().await;
        let response = client
            .get(prefix, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(|err| {
                SandboxError::DaemonUnavailable(format!("etcd list session routes failed: {err}"))
            })?;
        drop(client);

        let mut records = HashMap::<String, SessionRouteRecord>::new();
        for kv in response.kvs() {
            let key = String::from_utf8_lossy(kv.key()).to_string();
            if let Ok(record) = decode_session_route_record(&key, kv.value()) {
                let should_replace = records
                    .get(&record.session_id)
                    .map(|existing| existing.updated_at_unix_ms <= record.updated_at_unix_ms)
                    .unwrap_or(true);
                if should_replace {
                    records.insert(record.session_id.clone(), record);
                }
            }
        }
        Ok(records.into_values().collect())
    }

    async fn enforce_admission_budgets(
        &self,
        routes: &[SessionRouteRecord],
        tenant_id: &str,
        workspace_id: &str,
    ) -> Result<()> {
        let retry_after_ms = self.cfg.admission_retry_after_ms.max(1);
        if let Some(violation) = admission_budget_violation(
            routes,
            tenant_id,
            workspace_id,
            self.cfg.tenant_session_quota,
            self.cfg.workspace_session_quota,
        ) {
            return Err(SandboxError::ResourceExhausted(format!(
                "{violation} retry_after_ms={retry_after_ms}"
            )));
        }
        Ok(())
    }

    async fn put_outbox_record(&self, record: &OutboxRecord) -> Result<()> {
        let key = self.outbox_pending_key(&record.command_id);
        let payload = serde_json::to_vec(record).map_err(|err| {
            SandboxError::InvalidResponse(format!("serialize outbox record: {err}"))
        })?;
        let mut client = self.etcd.lock().await;
        client.put(key, payload, None).await.map_err(|err| {
            SandboxError::DaemonUnavailable(format!("etcd put outbox record failed: {err}"))
        })?;
        Ok(())
    }

    async fn delete_outbox_record(&self, command_id: &str) -> Result<()> {
        let key = self.outbox_pending_key(command_id);
        let mut client = self.etcd.lock().await;
        client.delete(key, None).await.map_err(|err| {
            SandboxError::DaemonUnavailable(format!("etcd delete outbox record failed: {err}"))
        })?;
        Ok(())
    }

    async fn publish_outbox_record(&self, record: &OutboxRecord) -> Result<()> {
        let bytes = serde_json::to_vec(&record.payload).map_err(|err| {
            SandboxError::InvalidResponse(format!("serialize outbox publish payload: {err}"))
        })?;
        self.jetstream
            .send_publish(
                record.subject.clone(),
                jetstream::context::Publish::build()
                    .message_id(record.command_id.clone())
                    .payload(bytes.into()),
            )
            .await
            .map_err(|err| {
                SandboxError::DaemonUnavailable(format!(
                    "jetstream publish outbox record failed (command_id={}): {err}",
                    record.command_id
                ))
            })?
            .await
            .map_err(|err| {
                SandboxError::DaemonUnavailable(format!(
                    "jetstream publish outbox ack failed (command_id={}): {err}",
                    record.command_id
                ))
            })?;
        Ok(())
    }

    pub(crate) async fn replay_outbox_once(&self) -> Result<()> {
        let prefix = self.outbox_pending_prefix();
        let mut client = self.etcd.lock().await;
        let response = client
            .get(prefix, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(|err| {
                SandboxError::DaemonUnavailable(format!("etcd list outbox records failed: {err}"))
            })?;
        let records: Vec<OutboxRecord> = response
            .kvs()
            .iter()
            .filter_map(|kv| serde_json::from_slice::<OutboxRecord>(kv.value()).ok())
            .collect();
        drop(client);

        for mut record in records {
            if self.publish_outbox_record(&record).await.is_ok() {
                if let Err(err) = self.delete_outbox_record(&record.command_id).await {
                    tracing::warn!(
                        error = %err,
                        command_id = %record.command_id,
                        "failed to delete outbox record after successful replay"
                    );
                }
            } else {
                record.attempts += 1;
                record.updated_at_unix_ms = unix_millis();
                if let Err(err) = self.put_outbox_record(&record).await {
                    tracing::warn!(
                        error = %err,
                        command_id = %record.command_id,
                        attempts = record.attempts,
                        "failed to persist outbox retry record"
                    );
                }
            }
        }

        Ok(())
    }
}

fn spawn_outbox_replay_worker(control: DistributedControlPlane) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            if let Err(err) = control.replay_outbox_once().await {
                tracing::warn!(error = %err, "outbox replay tick failed");
            }
        }
    });
}

fn normalize_cfg(mut cfg: DistributedControlConfig) -> DistributedControlConfig {
    cfg.etcd_prefix = normalize_prefix(&cfg.etcd_prefix);
    cfg.nats_auth_token = cfg
        .nats_auth_token
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    if cfg.cluster_id.trim().is_empty() {
        cfg.cluster_id = "chevalier-sandbox-cluster".to_string();
    }
    if cfg.nats_subject_prefix.trim().is_empty() {
        cfg.nats_subject_prefix = "chevalier.sandbox.control".to_string();
    }
    if cfg.nats_stream_name.trim().is_empty() {
        cfg.nats_stream_name = "CHEVALIER_SANDBOX_CONTROL".to_string();
    }
    if cfg.nats_stream_max_age_secs == 0 {
        cfg.nats_stream_max_age_secs = 60 * 60 * 24 * 7;
    }
    if cfg.nats_stream_replicas == 0 {
        cfg.nats_stream_replicas = 1;
    }
    if cfg.nats_dead_letter_subject.trim().is_empty() {
        cfg.nats_dead_letter_subject = format!("{}.dlq.commands", cfg.nats_subject_prefix);
    }
    cfg.required_storage_profile = cfg
        .required_storage_profile
        .as_deref()
        .and_then(normalize_storage_profile);
    cfg.required_continuity_tier = cfg
        .required_continuity_tier
        .as_deref()
        .and_then(normalize_continuity_tier)
        .or_else(|| Some(CONTINUITY_TIER_B.to_string()));
    if cfg.tenant_session_quota == Some(0) {
        cfg.tenant_session_quota = None;
    }
    if cfg.workspace_session_quota == Some(0) {
        cfg.workspace_session_quota = None;
    }
    if cfg.admission_retry_after_ms == 0 {
        cfg.admission_retry_after_ms = 2_000;
    }
    cfg
}

async fn connect_nats(url: &str, auth_token: Option<&str>) -> Result<async_nats::Client> {
    let token = auth_token.map(str::trim).filter(|value| !value.is_empty());
    let client = match token {
        Some(token) => {
            async_nats::ConnectOptions::with_token(token.to_string())
                .connect(url.to_string())
                .await
        }
        None => async_nats::connect(url.to_string()).await,
    }
    .map_err(|err| SandboxError::DaemonUnavailable(format!("failed connecting to nats: {err}")))?;
    Ok(client)
}

async fn ensure_control_stream(
    jetstream: &jetstream::Context,
    cfg: &DistributedControlConfig,
) -> Result<()> {
    let mut subjects = vec![
        format!("{}.cmd.>", cfg.nats_subject_prefix),
        format!("{}.evt.>", cfg.nats_subject_prefix),
        cfg.nats_dead_letter_subject.clone(),
        format!("{}.replay.>", cfg.nats_subject_prefix),
    ];
    subjects.sort();
    subjects.dedup();

    jetstream
        .get_or_create_stream(jetstream::stream::Config {
            name: cfg.nats_stream_name.clone(),
            subjects,
            max_age: Duration::from_secs(cfg.nats_stream_max_age_secs),
            storage: jetstream::stream::StorageType::File,
            num_replicas: cfg.nats_stream_replicas,
            ..Default::default()
        })
        .await
        .map_err(|err| {
            SandboxError::DaemonUnavailable(format!(
                "ensure control jetstream stream failed: {err}"
            ))
        })?;
    Ok(())
}

fn normalize_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim().trim_matches('/');
    if trimmed.is_empty() {
        return "/chevalier-sandbox".to_string();
    }
    format!("/{trimmed}")
}

fn decode_session_route(raw: &[u8]) -> Result<SessionRoute> {
    if let Ok(record) = serde_json::from_slice::<SessionRouteRecord>(raw) {
        return Ok(SessionRoute {
            session_id: record.session_id,
            vm_id: record.vm_id,
            endpoint: record.endpoint,
            node_id: record.node_id,
            fork_id: record.fork_id,
            ownership_fence: record.ownership_fence,
            tenant_id: normalize_scope_value(&record.tenant_id),
            workspace_id: normalize_scope_value(&record.workspace_id),
            tier_b_eligible: record.tier_b_eligible,
            required_mount_profiles: normalize_shared_mount_profiles(
                &record.required_mount_profiles,
            ),
        });
    }

    let endpoint = String::from_utf8(raw.to_vec()).map_err(|err| {
        SandboxError::InvalidResponse(format!("invalid session route value: {err}"))
    })?;
    Ok(SessionRoute {
        session_id: String::new(),
        vm_id: String::new(),
        endpoint,
        node_id: None,
        fork_id: None,
        ownership_fence: None,
        tenant_id: "default".to_string(),
        workspace_id: "default".to_string(),
        tier_b_eligible: false,
        required_mount_profiles: Vec::new(),
    })
}

fn decode_node_route(key: &str, raw: &[u8]) -> Result<NodeRoute> {
    if let Ok(record) = serde_json::from_slice::<NodeRecord>(raw) {
        let node_id = if record.node_id.trim().is_empty() {
            key.rsplit('/').next().unwrap_or_default().to_string()
        } else {
            record.node_id
        };
        if record.endpoint.trim().is_empty() {
            return Err(SandboxError::InvalidResponse(
                "node record missing endpoint".to_string(),
            ));
        }
        return Ok(NodeRoute {
            node_id,
            endpoint: record.endpoint,
            max_active_vms: record.max_active_vms,
            storage_profile: normalize_storage_profile(&record.storage_profile)
                .unwrap_or_else(|| STORAGE_PROFILE_LOCAL_EPHEMERAL.to_string()),
            continuity_tier: normalize_continuity_tier(&record.continuity_tier)
                .unwrap_or_else(|| CONTINUITY_TIER_A.to_string()),
            shared_mount_profiles: normalize_shared_mount_profiles(&record.shared_mount_profiles),
            degraded_mode: record.degraded_mode,
            admission_frozen: record.admission_frozen,
            region: normalize_scope_value(&record.region),
            zone: normalize_scope_value(&record.zone),
            rack: normalize_scope_value(&record.rack),
        });
    }

    let endpoint = String::from_utf8(raw.to_vec())
        .map_err(|err| SandboxError::InvalidResponse(format!("invalid node route value: {err}")))?;
    let node_id = key.rsplit('/').next().unwrap_or_default().to_string();
    Ok(NodeRoute {
        node_id,
        endpoint,
        max_active_vms: None,
        storage_profile: STORAGE_PROFILE_LOCAL_EPHEMERAL.to_string(),
        continuity_tier: CONTINUITY_TIER_A.to_string(),
        shared_mount_profiles: vec![SHARED_MOUNT_PROFILE_LOCAL.to_string()],
        degraded_mode: true,
        admission_frozen: false,
        region: "default".to_string(),
        zone: "default".to_string(),
        rack: "default".to_string(),
    })
}

fn decode_session_route_record(key: &str, raw: &[u8]) -> Result<SessionRouteRecord> {
    if let Ok(mut record) = serde_json::from_slice::<SessionRouteRecord>(raw) {
        if record.session_id.trim().is_empty() {
            record.session_id = key.rsplit('/').next().unwrap_or_default().to_string();
        }
        record.tenant_id = normalize_scope_value(&record.tenant_id);
        record.workspace_id = normalize_scope_value(&record.workspace_id);
        record.required_mount_profiles =
            normalize_shared_mount_profiles(&record.required_mount_profiles);
        return Ok(record);
    }

    let endpoint = String::from_utf8(raw.to_vec()).map_err(|err| {
        SandboxError::InvalidResponse(format!("invalid session route value: {err}"))
    })?;
    let session_id = key.rsplit('/').next().unwrap_or_default().to_string();
    Ok(SessionRouteRecord {
        session_id,
        vm_id: String::new(),
        endpoint,
        node_id: None,
        fork_id: None,
        ownership_fence: None,
        tenant_id: "default".to_string(),
        workspace_id: "default".to_string(),
        tier_b_eligible: false,
        required_mount_profiles: Vec::new(),
        updated_at_unix_ms: 0,
    })
}

fn eligible_rebind_routes(
    routes: &[NodeRoute],
    session_routes: &[SessionRouteRecord],
    session_route: Option<&SessionRoute>,
    from_endpoint: &str,
    required_storage_profile: Option<&str>,
    required_continuity_tier: Option<&str>,
    allow_tier_a_degraded: bool,
) -> Vec<NodeRoute> {
    let usage = session_counts_by_endpoint(session_routes);
    let (route_tier, route_allow_tier_a_degraded, route_profiles) = match session_route {
        Some(route) if route.tier_b_eligible => (
            required_continuity_tier,
            allow_tier_a_degraded,
            route.required_mount_profiles.as_slice(),
        ),
        Some(route) => (None, true, route.required_mount_profiles.as_slice()),
        None => (None, true, &[][..]),
    };

    eligible_routes_with_profile(
        routes,
        &usage,
        required_storage_profile,
        route_tier,
        route_allow_tier_a_degraded,
        route_profiles,
    )
    .into_iter()
    // @dive: Rebind candidate filtering excludes the current endpoint before transport probing so session continuity obeys mount/backend policy first.
    .filter(|route| route.endpoint != from_endpoint)
    .collect()
}

fn session_counts_by_endpoint(records: &[SessionRouteRecord]) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    for record in records {
        if !record.endpoint.trim().is_empty() {
            *counts.entry(record.endpoint.clone()).or_insert(0) += 1;
        }
    }
    counts
}

fn admission_budget_violation(
    routes: &[SessionRouteRecord],
    tenant_id: &str,
    workspace_id: &str,
    tenant_limit: Option<usize>,
    workspace_limit: Option<usize>,
) -> Option<String> {
    let tenant_sessions = routes
        .iter()
        .filter(|route| route.tenant_id == tenant_id)
        .count();
    if let Some(limit) = tenant_limit {
        if tenant_sessions >= limit {
            return Some(format!(
                "tenant admission budget exhausted (tenant_id={tenant_id}, used={tenant_sessions}, limit={limit})"
            ));
        }
    }
    let workspace_sessions = routes
        .iter()
        .filter(|route| route.tenant_id == tenant_id && route.workspace_id == workspace_id)
        .count();
    if let Some(limit) = workspace_limit {
        if workspace_sessions >= limit {
            return Some(format!(
                "workspace admission budget exhausted (tenant_id={tenant_id}, workspace_id={workspace_id}, used={workspace_sessions}, limit={limit})"
            ));
        }
    }
    None
}

fn eligible_routes_with_profile(
    routes: &[NodeRoute],
    usage: &HashMap<String, usize>,
    required_storage_profile: Option<&str>,
    required_continuity_tier: Option<&str>,
    allow_tier_a_degraded: bool,
    required_mount_profiles: &[String],
) -> Vec<NodeRoute> {
    let mut out = Vec::new();
    let required_storage_profile = required_storage_profile.and_then(normalize_storage_profile);
    let required_continuity_tier = required_continuity_tier.and_then(normalize_continuity_tier);
    let required_mount_profiles = normalize_shared_mount_profiles(required_mount_profiles);
    for route in routes {
        if route.admission_frozen {
            continue;
        }
        if let Some(required_profile) = required_storage_profile.as_ref() {
            let route_profile = normalize_storage_profile(&route.storage_profile)
                .unwrap_or_else(|| STORAGE_PROFILE_LOCAL_EPHEMERAL.to_string());
            if &route_profile != required_profile {
                continue;
            }
        }
        let route_tier = normalize_continuity_tier(&route.continuity_tier)
            .unwrap_or_else(|| CONTINUITY_TIER_A.to_string());
        if let Some(required_tier) = required_continuity_tier.as_ref() {
            if &route_tier != required_tier {
                continue;
            }
        }
        if !required_mount_profiles.is_empty() {
            let route_profiles = normalize_shared_mount_profiles(&route.shared_mount_profiles);
            if required_mount_profiles
                .iter()
                .any(|profile| !route_profiles.iter().any(|candidate| candidate == profile))
            {
                continue;
            }
        }
        if route_tier == CONTINUITY_TIER_A {
            if !route.degraded_mode {
                continue;
            }
            if !allow_tier_a_degraded {
                continue;
            }
        }
        if route_tier == CONTINUITY_TIER_B && route.degraded_mode {
            continue;
        }
        let used = usage.get(&route.endpoint).copied().unwrap_or(0);
        if route.max_active_vms.is_none_or(|limit| used < limit) {
            out.push(route.clone());
        }
    }
    out
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn derive_idempotency_key(command_type: &str, ordering_key: &str, payload: &Value) -> String {
    let mut hasher = DefaultHasher::new();
    command_type.hash(&mut hasher);
    ordering_key.hash(&mut hasher);
    serde_json::to_string(payload)
        .unwrap_or_default()
        .hash(&mut hasher);
    format!("idem-{:016x}", hasher.finish())
}

fn build_event_envelope(event_name: &str, payload: Value) -> Value {
    json!({
        "schema_version": CONTROL_ENVELOPE_SCHEMA_VERSION,
        "event": event_name,
        "timestamp_unix_ms": unix_millis(),
        "payload": payload,
    })
}

fn build_command_envelope(
    command_id: &str,
    command_type: &str,
    ordering_key: &str,
    idempotency_key: &str,
    payload: &Value,
) -> Value {
    json!({
        "schema_version": CONTROL_ENVELOPE_SCHEMA_VERSION,
        "command_id": command_id,
        "command_type": command_type,
        "tenant_id": payload.get("tenant_id").and_then(Value::as_str).unwrap_or("default"),
        "workspace_id": payload.get("workspace_id").and_then(Value::as_str).unwrap_or("default"),
        "session_id": payload.get("session_id").and_then(Value::as_str),
        "vm_id": payload.get("vm_id").and_then(Value::as_str),
        "target_node_id": payload.get("target_node_id").and_then(Value::as_str),
        "ordering_key": ordering_key,
        "issued_at_unix_ms": unix_millis(),
        "timeout_ms": payload.get("timeout_ms").and_then(Value::as_u64).unwrap_or(30_000),
        "idempotency_key": idempotency_key,
        "trace_id": payload
            .get("trace_id")
            .and_then(Value::as_str)
            .unwrap_or(command_id),
        "causation_id": payload.get("causation_id").and_then(Value::as_str),
        "expected_fence": payload.get("expected_fence"),
        "expected_versions": payload.get("expected_versions"),
        "payload": payload,
    })
}

#[allow(dead_code)]
fn decode_exec_result_payload(raw: &[u8]) -> Result<ExecCommandResult> {
    let value: Value = serde_json::from_slice(raw).map_err(|err| {
        SandboxError::InvalidResponse(format!("decode exec result payload json: {err}"))
    })?;
    let payload = value.get("payload").cloned().unwrap_or(value);
    serde_json::from_value(payload).map_err(|err| {
        SandboxError::InvalidResponse(format!("decode exec result payload shape: {err}"))
    })
}

fn decode_exec_stream_event_payload(raw: &[u8]) -> Result<ExecStreamEvent> {
    let value: Value = serde_json::from_slice(raw).map_err(|err| {
        SandboxError::InvalidResponse(format!("decode exec stream payload json: {err}"))
    })?;
    let payload = value.get("payload").cloned().unwrap_or(value);
    let mut parsed: ExecStreamEvent = serde_json::from_value(payload).map_err(|err| {
        SandboxError::InvalidResponse(format!("decode exec stream payload shape: {err}"))
    })?;
    if parsed.logical_stream_id.trim().is_empty() {
        parsed.logical_stream_id = parsed.stream_id.clone();
    }
    if parsed.stream_id.trim().is_empty() {
        parsed.stream_id = parsed.logical_stream_id.clone();
    }
    if parsed.event_seq == 0 {
        parsed.event_seq = parsed.sequence;
    }
    if parsed.sequence == 0 {
        parsed.sequence = parsed.event_seq;
    }
    Ok(parsed)
}

fn decode_exec_stream_event_payload_for_stream(
    raw: &[u8],
    expected_stream_id: &str,
) -> Result<ExecStreamEvent> {
    let expected_stream_id = expected_stream_id.trim();
    if expected_stream_id.is_empty() {
        return Err(SandboxError::InvalidResponse(
            "exec stream event validation requires expected stream id".to_string(),
        ));
    }

    let parsed = decode_exec_stream_event_payload(raw)?;
    if parsed.logical_stream_id.trim().is_empty() || parsed.stream_id.trim().is_empty() {
        return Err(SandboxError::InvalidResponse(
            "exec stream event missing stream identity".to_string(),
        ));
    }
    if parsed.logical_stream_id.trim() != expected_stream_id
        || parsed.stream_id.trim() != expected_stream_id
    {
        return Err(SandboxError::InvalidResponse(format!(
            "exec stream event identity mismatch: expected {}, got logical_stream_id={} stream_id={}",
            expected_stream_id, parsed.logical_stream_id, parsed.stream_id
        )));
    }
    Ok(parsed)
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

fn should_drop_replayed_event(event_seq: u64, resume_after_event_seq: u64) -> bool {
    resume_after_event_seq > 0 && event_seq > 0 && event_seq <= resume_after_event_seq
}

fn normalize_storage_profile(raw: &str) -> Option<String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "" => None,
        "local-ephemeral" | "local_ephemeral" | "local" => {
            Some(STORAGE_PROFILE_LOCAL_EPHEMERAL.to_string())
        }
        "durable-shared" | "durable_shared" | "durable" => {
            Some(STORAGE_PROFILE_DURABLE_SHARED.to_string())
        }
        _ => None,
    }
}

fn normalize_continuity_tier(raw: &str) -> Option<String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "" => None,
        "tier-a" | "tier_a" | "a" => Some(CONTINUITY_TIER_A.to_string()),
        "tier-b" | "tier_b" | "b" => Some(CONTINUITY_TIER_B.to_string()),
        _ => None,
    }
}

fn normalize_shared_mount_profiles(values: &[String]) -> Vec<String> {
    let mut normalized = values
        .iter()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if !normalized
        .iter()
        .any(|value| value == SHARED_MOUNT_PROFILE_LOCAL)
    {
        normalized.push(SHARED_MOUNT_PROFILE_LOCAL.to_string());
    }
    normalized.sort();
    normalized.dedup();
    normalized
}

fn normalize_scope_value(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        "default".to_string()
    } else {
        trimmed.to_string()
    }
}

fn score_node(session_id: &str, node_id: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    session_id.hash(&mut hasher);
    node_id.hash(&mut hasher);
    hasher.finish()
}

fn shard_for_key(key: &str, shard_count: u8) -> u8 {
    if shard_count <= 1 {
        return 0;
    }
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as u8) % shard_count
}

fn lease_interval(ttl_secs: i64) -> std::time::Duration {
    let ttl = ttl_secs.max(2) as u64;
    let interval = (ttl / 2).max(1);
    std::time::Duration::from_secs(interval)
}

type PortAllocationKeepAlive = (LeaseKeeper, LeaseKeepAliveStream);

async fn put_port_allocation_with_new_lease(
    etcd: &Arc<Mutex<EtcdClient>>,
    key: &str,
    allocation: &PortAllocation,
    ttl_secs: i64,
) -> Result<i64> {
    let mut client = etcd.lock().await;
    let lease = client.lease_grant(ttl_secs, None).await.map_err(|err| {
        SandboxError::DaemonUnavailable(format!("etcd lease grant failed: {err}"))
    })?;
    let lease_id = lease.id();
    let payload = serde_json::to_vec(&PortAllocationRecord {
        session_id: allocation.session_id.clone(),
        vm_id: allocation.vm_id.clone(),
        endpoint: allocation.endpoint.clone(),
        guest_port: allocation.guest_port,
        host_port: allocation.host_port,
        updated_at_unix_ms: unix_millis(),
    })
    .map_err(|err| SandboxError::InvalidResponse(format!("serialize port allocation: {err}")))?;
    client
        .put(key, payload, Some(PutOptions::new().with_lease(lease_id)))
        .await
        .map_err(|err| SandboxError::DaemonUnavailable(format!("etcd put failed: {err}")))?;
    Ok(lease_id)
}

async fn start_port_allocation_keepalive(
    client: &mut EtcdClient,
    lease_id: i64,
) -> Result<PortAllocationKeepAlive> {
    client.lease_keep_alive(lease_id).await.map_err(|err| {
        SandboxError::DaemonUnavailable(format!("etcd lease keepalive start failed: {err}"))
    })
}

async fn keep_port_allocation_alive(keepalive: &mut PortAllocationKeepAlive) -> Result<()> {
    const KEEPALIVE_RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);

    keepalive.0.keep_alive().await.map_err(|err| {
        SandboxError::DaemonUnavailable(format!("etcd lease keepalive failed: {err}"))
    })?;
    let response = tokio::time::timeout(KEEPALIVE_RESPONSE_TIMEOUT, keepalive.1.message())
        .await
        .map_err(|_| {
            SandboxError::DaemonUnavailable("etcd lease keepalive response timed out".to_string())
        })?
        .map_err(|err| {
            SandboxError::DaemonUnavailable(format!("etcd lease keepalive response failed: {err}"))
        })?;
    let Some(response) = response else {
        return Err(SandboxError::DaemonUnavailable(
            "etcd lease keepalive stream closed".to_string(),
        ));
    };
    if response.ttl() <= 0 {
        return Err(SandboxError::DaemonUnavailable(format!(
            "etcd lease keepalive returned non-positive ttl for lease {}",
            response.id()
        )));
    }
    Ok(())
}

async fn delete_etcd_key(etcd: &Arc<Mutex<EtcdClient>>, key: &str) -> Result<()> {
    let mut client = etcd.lock().await;
    client
        .delete(key, None)
        .await
        .map_err(|err| SandboxError::DaemonUnavailable(format!("etcd delete failed: {err}")))?;
    Ok(())
}

async fn read_fence_value(client: &mut EtcdClient, key: &str) -> Result<Option<String>> {
    let response = client.get(key.to_string(), None).await.map_err(|err| {
        SandboxError::DaemonUnavailable(format!("etcd read fence key failed: {err}"))
    })?;
    let Some(kv) = response.kvs().first() else {
        return Ok(None);
    };
    let value = String::from_utf8(kv.value().to_vec()).map_err(|err| {
        SandboxError::InvalidResponse(format!("decode fence value failed: {err}"))
    })?;
    if value.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(value))
}

fn ownership_fence_allows_transition(current: Option<&str>, expected: Option<&str>) -> bool {
    match expected {
        Some(expected) => current.is_some_and(|value| value == expected),
        None => current.is_none(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node_route(
        node_id: &str,
        endpoint: &str,
        storage_profile: &str,
        continuity_tier: &str,
        degraded_mode: bool,
        shared_mount_profiles: &[&str],
    ) -> NodeRoute {
        NodeRoute {
            node_id: node_id.to_string(),
            endpoint: endpoint.to_string(),
            max_active_vms: None,
            storage_profile: storage_profile.to_string(),
            continuity_tier: continuity_tier.to_string(),
            shared_mount_profiles: shared_mount_profiles
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
            degraded_mode,
            admission_frozen: false,
            region: "r1".to_string(),
            zone: "z1".to_string(),
            rack: "rack-a".to_string(),
        }
    }

    fn session_route(
        session_id: &str,
        endpoint: &str,
        tier_b_eligible: bool,
        required_mount_profiles: &[&str],
    ) -> SessionRoute {
        SessionRoute {
            session_id: session_id.to_string(),
            vm_id: "vm-1".to_string(),
            endpoint: endpoint.to_string(),
            node_id: Some("node-a".to_string()),
            fork_id: None,
            ownership_fence: None,
            tenant_id: "tenant-a".to_string(),
            workspace_id: "workspace-a".to_string(),
            tier_b_eligible,
            required_mount_profiles: required_mount_profiles
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
        }
    }

    #[test]
    fn eligible_routes_filters_nodes_at_capacity() {
        let routes = vec![
            NodeRoute {
                node_id: "node-a".to_string(),
                endpoint: "http://node-a:8052".to_string(),
                max_active_vms: Some(2),
                storage_profile: STORAGE_PROFILE_DURABLE_SHARED.to_string(),
                continuity_tier: CONTINUITY_TIER_B.to_string(),
                shared_mount_profiles: vec![
                    SHARED_MOUNT_PROFILE_LOCAL.to_string(),
                    "shared-posix".to_string(),
                ],
                degraded_mode: false,
                admission_frozen: false,
                region: "r1".to_string(),
                zone: "z1".to_string(),
                rack: "rack-a".to_string(),
            },
            NodeRoute {
                node_id: "node-b".to_string(),
                endpoint: "http://node-b:8052".to_string(),
                max_active_vms: Some(1),
                storage_profile: STORAGE_PROFILE_DURABLE_SHARED.to_string(),
                continuity_tier: CONTINUITY_TIER_B.to_string(),
                shared_mount_profiles: vec![
                    SHARED_MOUNT_PROFILE_LOCAL.to_string(),
                    "shared-posix".to_string(),
                ],
                degraded_mode: false,
                admission_frozen: false,
                region: "r1".to_string(),
                zone: "z2".to_string(),
                rack: "rack-b".to_string(),
            },
            NodeRoute {
                node_id: "node-c".to_string(),
                endpoint: "http://node-c:8052".to_string(),
                max_active_vms: None,
                storage_profile: STORAGE_PROFILE_LOCAL_EPHEMERAL.to_string(),
                continuity_tier: CONTINUITY_TIER_A.to_string(),
                shared_mount_profiles: vec![SHARED_MOUNT_PROFILE_LOCAL.to_string()],
                degraded_mode: true,
                admission_frozen: false,
                region: "r1".to_string(),
                zone: "z3".to_string(),
                rack: "rack-c".to_string(),
            },
        ];

        let usage = HashMap::from([
            ("http://node-a:8052".to_string(), 1usize),
            ("http://node-b:8052".to_string(), 1usize),
            ("http://node-c:8052".to_string(), 4usize),
        ]);

        let eligible = eligible_routes_with_profile(
            &routes,
            &usage,
            None,
            Some(CONTINUITY_TIER_B),
            false,
            &[],
        );
        assert_eq!(eligible.len(), 1);
        assert_eq!(eligible[0].node_id, "node-a");
    }

    #[test]
    fn eligible_routes_skips_admission_frozen_nodes() {
        let routes = vec![
            NodeRoute {
                node_id: "node-frozen".to_string(),
                endpoint: "http://node-frozen:8052".to_string(),
                max_active_vms: None,
                storage_profile: STORAGE_PROFILE_DURABLE_SHARED.to_string(),
                continuity_tier: CONTINUITY_TIER_B.to_string(),
                shared_mount_profiles: vec![
                    SHARED_MOUNT_PROFILE_LOCAL.to_string(),
                    "shared-posix".to_string(),
                ],
                degraded_mode: false,
                admission_frozen: true,
                region: "r1".to_string(),
                zone: "z1".to_string(),
                rack: "rack-a".to_string(),
            },
            NodeRoute {
                node_id: "node-ready".to_string(),
                endpoint: "http://node-ready:8052".to_string(),
                max_active_vms: None,
                storage_profile: STORAGE_PROFILE_DURABLE_SHARED.to_string(),
                continuity_tier: CONTINUITY_TIER_B.to_string(),
                shared_mount_profiles: vec![
                    SHARED_MOUNT_PROFILE_LOCAL.to_string(),
                    "shared-posix".to_string(),
                ],
                degraded_mode: false,
                admission_frozen: false,
                region: "r1".to_string(),
                zone: "z2".to_string(),
                rack: "rack-b".to_string(),
            },
        ];
        let usage = HashMap::new();
        let eligible = eligible_routes_with_profile(
            &routes,
            &usage,
            Some(STORAGE_PROFILE_DURABLE_SHARED),
            Some(CONTINUITY_TIER_B),
            false,
            &[],
        );
        assert_eq!(eligible.len(), 1);
        assert_eq!(eligible[0].node_id, "node-ready");
    }

    #[test]
    fn eligible_routes_filters_by_required_storage_profile() {
        let routes = vec![
            NodeRoute {
                node_id: "node-a".to_string(),
                endpoint: "http://node-a:8052".to_string(),
                max_active_vms: None,
                storage_profile: STORAGE_PROFILE_LOCAL_EPHEMERAL.to_string(),
                continuity_tier: CONTINUITY_TIER_B.to_string(),
                shared_mount_profiles: vec![
                    SHARED_MOUNT_PROFILE_LOCAL.to_string(),
                    "shared-posix".to_string(),
                ],
                degraded_mode: false,
                admission_frozen: false,
                region: "r1".to_string(),
                zone: "z1".to_string(),
                rack: "rack-a".to_string(),
            },
            NodeRoute {
                node_id: "node-b".to_string(),
                endpoint: "http://node-b:8052".to_string(),
                max_active_vms: None,
                storage_profile: STORAGE_PROFILE_DURABLE_SHARED.to_string(),
                continuity_tier: CONTINUITY_TIER_B.to_string(),
                shared_mount_profiles: vec![
                    SHARED_MOUNT_PROFILE_LOCAL.to_string(),
                    "shared-posix".to_string(),
                ],
                degraded_mode: false,
                admission_frozen: false,
                region: "r1".to_string(),
                zone: "z2".to_string(),
                rack: "rack-b".to_string(),
            },
        ];

        let usage = HashMap::new();
        let durable_only = eligible_routes_with_profile(
            &routes,
            &usage,
            Some(STORAGE_PROFILE_DURABLE_SHARED),
            Some(CONTINUITY_TIER_B),
            false,
            &[],
        );
        assert_eq!(durable_only.len(), 1);
        assert_eq!(durable_only[0].node_id, "node-b");

        let local_only = eligible_routes_with_profile(
            &routes,
            &usage,
            Some(STORAGE_PROFILE_LOCAL_EPHEMERAL),
            Some(CONTINUITY_TIER_B),
            false,
            &[],
        );
        assert_eq!(local_only.len(), 1);
        assert_eq!(local_only[0].node_id, "node-a");
    }

    #[test]
    fn eligible_routes_enforces_tier_b_default_and_tier_a_degraded_policy() {
        let routes = vec![
            NodeRoute {
                node_id: "node-tier-b".to_string(),
                endpoint: "http://node-tier-b:8052".to_string(),
                max_active_vms: None,
                storage_profile: STORAGE_PROFILE_DURABLE_SHARED.to_string(),
                continuity_tier: CONTINUITY_TIER_B.to_string(),
                shared_mount_profiles: vec![
                    SHARED_MOUNT_PROFILE_LOCAL.to_string(),
                    "shared-posix".to_string(),
                ],
                degraded_mode: false,
                admission_frozen: false,
                region: "r1".to_string(),
                zone: "z1".to_string(),
                rack: "rack-a".to_string(),
            },
            NodeRoute {
                node_id: "node-tier-a-degraded".to_string(),
                endpoint: "http://node-tier-a-degraded:8052".to_string(),
                max_active_vms: None,
                storage_profile: STORAGE_PROFILE_DURABLE_SHARED.to_string(),
                continuity_tier: CONTINUITY_TIER_A.to_string(),
                shared_mount_profiles: vec![
                    SHARED_MOUNT_PROFILE_LOCAL.to_string(),
                    "shared-posix".to_string(),
                ],
                degraded_mode: true,
                admission_frozen: false,
                region: "r1".to_string(),
                zone: "z2".to_string(),
                rack: "rack-b".to_string(),
            },
            NodeRoute {
                node_id: "node-tier-a-invalid".to_string(),
                endpoint: "http://node-tier-a-invalid:8052".to_string(),
                max_active_vms: None,
                storage_profile: STORAGE_PROFILE_DURABLE_SHARED.to_string(),
                continuity_tier: CONTINUITY_TIER_A.to_string(),
                shared_mount_profiles: vec![
                    SHARED_MOUNT_PROFILE_LOCAL.to_string(),
                    "shared-posix".to_string(),
                ],
                degraded_mode: false,
                admission_frozen: false,
                region: "r1".to_string(),
                zone: "z3".to_string(),
                rack: "rack-c".to_string(),
            },
        ];
        let usage = HashMap::new();

        let tier_b_only = eligible_routes_with_profile(
            &routes,
            &usage,
            Some(STORAGE_PROFILE_DURABLE_SHARED),
            Some(CONTINUITY_TIER_B),
            false,
            &[],
        );
        assert_eq!(tier_b_only.len(), 1);
        assert_eq!(tier_b_only[0].node_id, "node-tier-b");

        let allow_tier_a_degraded = eligible_routes_with_profile(
            &routes,
            &usage,
            Some(STORAGE_PROFILE_DURABLE_SHARED),
            Some(CONTINUITY_TIER_A),
            true,
            &[],
        );
        assert_eq!(allow_tier_a_degraded.len(), 1);
        assert_eq!(allow_tier_a_degraded[0].node_id, "node-tier-a-degraded");
    }

    #[test]
    fn eligible_routes_filters_by_required_shared_mount_profiles() {
        let routes = vec![
            node_route(
                "node-local-only",
                "http://node-local-only:8052",
                STORAGE_PROFILE_DURABLE_SHARED,
                CONTINUITY_TIER_B,
                false,
                &[],
            ),
            node_route(
                "node-shared-posix",
                "http://node-shared-posix:8052",
                STORAGE_PROFILE_DURABLE_SHARED,
                CONTINUITY_TIER_B,
                false,
                &["shared-posix"],
            ),
            node_route(
                "node-shared-alt",
                "http://node-shared-alt:8052",
                STORAGE_PROFILE_DURABLE_SHARED,
                CONTINUITY_TIER_B,
                false,
                &["shared-alt"],
            ),
        ];
        let usage = HashMap::new();

        let eligible = eligible_routes_with_profile(
            &routes,
            &usage,
            Some(STORAGE_PROFILE_DURABLE_SHARED),
            Some(CONTINUITY_TIER_B),
            false,
            &["shared-posix".to_string()],
        );
        assert_eq!(eligible.len(), 1);
        assert_eq!(eligible[0].node_id, "node-shared-posix");
    }

    #[test]
    fn eligible_rebind_routes_enforce_session_mount_contract_and_exclude_current_endpoint() {
        let routes = vec![
            node_route(
                "node-primary",
                "http://node-primary:8052",
                STORAGE_PROFILE_DURABLE_SHARED,
                CONTINUITY_TIER_B,
                false,
                &["shared-posix"],
            ),
            node_route(
                "node-secondary-valid",
                "http://node-secondary-valid:8052",
                STORAGE_PROFILE_DURABLE_SHARED,
                CONTINUITY_TIER_B,
                false,
                &["shared-posix"],
            ),
            node_route(
                "node-secondary-wrong-profile",
                "http://node-secondary-wrong-profile:8052",
                STORAGE_PROFILE_DURABLE_SHARED,
                CONTINUITY_TIER_B,
                false,
                &["shared-alt"],
            ),
            node_route(
                "node-secondary-tier-a",
                "http://node-secondary-tier-a:8052",
                STORAGE_PROFILE_DURABLE_SHARED,
                CONTINUITY_TIER_A,
                false,
                &["shared-posix"],
            ),
        ];
        let session_routes = vec![SessionRouteRecord {
            session_id: "session-1".to_string(),
            vm_id: "vm-1".to_string(),
            endpoint: "http://node-primary:8052".to_string(),
            node_id: Some("node-primary".to_string()),
            fork_id: None,
            ownership_fence: Some("fence-1".to_string()),
            tenant_id: "tenant-a".to_string(),
            workspace_id: "workspace-a".to_string(),
            tier_b_eligible: true,
            required_mount_profiles: vec!["shared-posix".to_string()],
            updated_at_unix_ms: 1,
        }];

        let eligible = eligible_rebind_routes(
            &routes,
            &session_routes,
            Some(&session_route(
                "session-1",
                "http://node-primary:8052",
                true,
                &["shared-posix"],
            )),
            "http://node-primary:8052",
            Some(STORAGE_PROFILE_DURABLE_SHARED),
            Some(CONTINUITY_TIER_B),
            false,
        );

        assert_eq!(eligible.len(), 1);
        assert_eq!(eligible[0].node_id, "node-secondary-valid");
    }

    #[test]
    fn command_envelope_contains_required_fields_and_version() {
        let payload = json!({
            "session_id": "session-1",
            "tenant_id": "tenant-1",
            "workspace_id": "workspace-1",
            "trace_id": "trace-1",
            "timeout_ms": 5000
        });
        let envelope = build_command_envelope(
            "command-1",
            "session.create",
            "session-1",
            "idem-1",
            &payload,
        );
        assert_eq!(
            envelope.get("schema_version").and_then(Value::as_str),
            Some(CONTROL_ENVELOPE_SCHEMA_VERSION)
        );
        assert_eq!(
            envelope.get("command_id").and_then(Value::as_str),
            Some("command-1")
        );
        assert_eq!(
            envelope.get("idempotency_key").and_then(Value::as_str),
            Some("idem-1")
        );
        assert!(envelope.get("issued_at_unix_ms").is_some());
        assert!(envelope.get("payload").and_then(Value::as_object).is_some());
    }

    #[test]
    fn event_envelope_contains_required_fields_and_version() {
        let envelope = build_event_envelope("session.bound", json!({"session_id":"session-1"}));
        assert_eq!(
            envelope.get("schema_version").and_then(Value::as_str),
            Some(CONTROL_ENVELOPE_SCHEMA_VERSION)
        );
        assert_eq!(
            envelope.get("event").and_then(Value::as_str),
            Some("session.bound")
        );
        assert!(envelope.get("timestamp_unix_ms").is_some());
        assert!(envelope.get("payload").is_some());
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
    fn sharding_and_scope_helpers_are_stable() {
        assert_eq!(normalize_scope_value(""), "default");
        assert_eq!(normalize_scope_value(" workspace-a "), "workspace-a");

        let shard_a = shard_for_key("session-a", 16);
        let shard_b = shard_for_key("session-a", 16);
        assert_eq!(shard_a, shard_b);
        assert!(shard_a < 16);
    }

    #[test]
    fn admission_budget_violation_detects_tenant_and_workspace_limits() {
        let routes = vec![
            SessionRouteRecord {
                session_id: "s1".to_string(),
                vm_id: "vm1".to_string(),
                endpoint: "http://node-a:8052".to_string(),
                node_id: Some("node-a".to_string()),
                fork_id: None,
                ownership_fence: None,
                tenant_id: "tenant-a".to_string(),
                workspace_id: "workspace-a".to_string(),
                tier_b_eligible: false,
                required_mount_profiles: Vec::new(),
                updated_at_unix_ms: 1,
            },
            SessionRouteRecord {
                session_id: "s2".to_string(),
                vm_id: "vm2".to_string(),
                endpoint: "http://node-b:8052".to_string(),
                node_id: Some("node-b".to_string()),
                fork_id: None,
                ownership_fence: None,
                tenant_id: "tenant-a".to_string(),
                workspace_id: "workspace-a".to_string(),
                tier_b_eligible: false,
                required_mount_profiles: Vec::new(),
                updated_at_unix_ms: 2,
            },
        ];
        let tenant_violation =
            admission_budget_violation(&routes, "tenant-a", "workspace-a", Some(2), None);
        assert!(tenant_violation.is_some());

        let workspace_violation =
            admission_budget_violation(&routes, "tenant-a", "workspace-a", None, Some(2));
        assert!(workspace_violation.is_some());

        let no_violation =
            admission_budget_violation(&routes, "tenant-a", "workspace-b", Some(3), Some(1));
        assert!(no_violation.is_none());
    }

    #[test]
    fn decode_exec_result_payload_accepts_wrapped_or_raw_forms() {
        let wrapped = json!({
            "event": "exec.result",
            "payload": {
                "command_id": "cmd-1",
                "session_id": "session-1",
                "vm_id": "vm-1",
                "stdout": "ok",
                "stderr": "",
                "exit_code": 0,
                "timed_out": false,
                "error": null
            }
        });
        let parsed = decode_exec_result_payload(
            serde_json::to_string(&wrapped)
                .expect("serialize wrapped")
                .as_bytes(),
        )
        .expect("decode wrapped result");
        assert_eq!(parsed.command_id, "cmd-1");
        assert_eq!(parsed.exit_code, Some(0));

        let raw = json!({
            "command_id": "cmd-2",
            "session_id": "session-2",
            "vm_id": "vm-2",
            "stdout": "",
            "stderr": "warn",
            "exit_code": 1,
            "timed_out": false,
            "error": null
        });
        let parsed = decode_exec_result_payload(
            serde_json::to_string(&raw)
                .expect("serialize raw")
                .as_bytes(),
        )
        .expect("decode raw result");
        assert_eq!(parsed.command_id, "cmd-2");
        assert_eq!(parsed.stderr, "warn");
    }

    #[test]
    fn decode_exec_stream_event_payload_accepts_wrapped_or_raw_forms() {
        let wrapped = json!({
            "event": "exec.stream",
            "payload": {
                "cluster_id": "cluster-a",
                "logical_stream_id": "stream-1",
                "command_id": "cmd-1",
                "session_id": "session-1",
                "vm_id": "vm-1",
                "kind": "stdout",
                "data": [111, 107],
                "exit_code": null,
                "timed_out": false,
                "error": null,
                "event_seq": 2,
                "event_id": "cluster-a-evt-1",
                "producer_epoch": 0
            }
        });
        let parsed = decode_exec_stream_event_payload(
            serde_json::to_string(&wrapped)
                .expect("serialize wrapped stream event")
                .as_bytes(),
        )
        .expect("decode wrapped stream event");
        assert_eq!(parsed.logical_stream_id, "stream-1");
        assert_eq!(parsed.kind, "stdout");
        assert_eq!(parsed.data, b"ok");
        assert_eq!(parsed.cluster_id, "cluster-a");
        assert_eq!(parsed.normalized_event_seq(), 2);
        assert_eq!(parsed.event_id, "cluster-a-evt-1");
        assert_eq!(parsed.producer_epoch, 0);

        let raw = json!({
            "stream_id": "stream-2",
            "command_id": "cmd-2",
            "session_id": "session-2",
            "vm_id": "vm-2",
            "kind": "exit",
            "data": [],
            "exit_code": 0,
            "timed_out": false,
            "error": null,
            "sequence": 3
        });
        let parsed = decode_exec_stream_event_payload(
            serde_json::to_string(&raw)
                .expect("serialize raw stream event")
                .as_bytes(),
        )
        .expect("decode raw stream event");
        assert_eq!(parsed.logical_stream_id, "stream-2");
        assert_eq!(parsed.kind, "exit");
        assert_eq!(parsed.exit_code, Some(0));
        assert_eq!(parsed.normalized_event_seq(), 3);
    }

    #[test]
    fn decode_exec_stream_event_payload_for_stream_rejects_wrong_stream() {
        let raw = json!({
            "logical_stream_id": "stream-a",
            "stream_id": "stream-b",
            "kind": "stdout",
            "data": [111, 107],
            "event_seq": 1
        });
        let err = decode_exec_stream_event_payload_for_stream(
            serde_json::to_string(&raw)
                .expect("serialize stream event")
                .as_bytes(),
            "stream-a",
        )
        .expect_err("mismatched stream identity should be rejected");
        assert!(format!("{err}").contains("identity mismatch"));
    }

    #[test]
    fn decode_exec_stream_event_payload_for_stream_accepts_legacy_stream_id_only() {
        let raw = json!({
            "stream_id": "stream-a",
            "kind": "exit",
            "exit_code": 0,
            "sequence": 1
        });
        let parsed = decode_exec_stream_event_payload_for_stream(
            serde_json::to_string(&raw)
                .expect("serialize stream event")
                .as_bytes(),
            "stream-a",
        )
        .expect("legacy stream identity should normalize");
        assert_eq!(parsed.logical_stream_id, "stream-a");
        assert_eq!(parsed.stream_id, "stream-a");
    }

    #[test]
    fn replay_filter_drops_events_at_or_below_checkpoint() {
        assert!(should_drop_replayed_event(1, 1));
        assert!(should_drop_replayed_event(2, 3));
        assert!(!should_drop_replayed_event(4, 3));
        assert!(!should_drop_replayed_event(0, 3));
        assert!(!should_drop_replayed_event(5, 0));
    }
}
