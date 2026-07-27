// @dive-file: Publishes node liveness and scheduling labels into etcd with lease-backed heartbeats.
// @dive-rel: Emits node capability/placement metadata consumed by crates/sandbox/src/distributed.rs for placement decisions.
// @dive-rel: Uses vmd/src/config.rs NodeRegistryConfig as the authoritative source for node identity and failure-domain labels.
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use etcd_client::{Client as EtcdClient, LeaseKeepAliveStream, LeaseKeeper, PutOptions};
use serde_json::json;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::config::NodeRegistryConfig;

pub struct NodeRegistryHandle {
    key: String,
    stop_tx: Option<oneshot::Sender<()>>,
    join: Option<JoinHandle<()>>,
}

struct NodeRegistration {
    lease_id: i64,
    keepalive: NodeRegistrationKeepAlive,
}

type NodeRegistrationKeepAlive = (LeaseKeeper, LeaseKeepAliveStream);

impl NodeRegistryHandle {
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.await;
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}

pub async fn start(config: Option<NodeRegistryConfig>) -> Result<Option<NodeRegistryHandle>> {
    let Some(config) = config else {
        return Ok(None);
    };

    let key = registry_key(&config);
    let mut client = connect_client(&config).await?;
    let mut registration = register_with_new_lease(&mut client, &config, &key)
        .await
        .with_context(|| format!("initial node registry write for key {key}"))?;

    let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
    let task_config = config.clone();
    let task_key = key.clone();
    let interval = heartbeat_interval(task_config.ttl_secs);
    let join = tokio::spawn(async move {
        let mut client = client;
        loop {
            tokio::select! {
                _ = &mut stop_rx => {
                    break;
                }
                _ = tokio::time::sleep(interval) => {
                    let heartbeat_result = async {
                        keep_node_registration_alive(&mut registration).await?;
                        write_node_record(
                            &mut client,
                            &task_config,
                            &task_key,
                            registration.lease_id,
                        ).await
                    }.await;
                    if let Err(err) = heartbeat_result {
                        warn!(
                            key = %task_key,
                            err = %err,
                            "node registry heartbeat write failed"
                        );
                        match connect_client(&task_config).await {
                            Ok(mut reconnected) => {
                                match register_with_new_lease(
                                    &mut reconnected,
                                    &task_config,
                                    &task_key,
                                ).await {
                                    Ok(next_registration) => {
                                        client = reconnected;
                                        registration = next_registration;
                                    }
                                    Err(register_err) => warn!(
                                        key = %task_key,
                                        err = %register_err,
                                        "node registry heartbeat re-register failed"
                                    ),
                                }
                            }
                            Err(connect_err) => warn!(
                                key = %task_key,
                                err = %connect_err,
                                "node registry heartbeat reconnect failed"
                            ),
                        }
                    }
                }
            }
        }

        if let Err(err) = delete_key(&mut client, &task_key).await {
            warn!(
                key = %task_key,
                err = %err,
                "failed deleting node registry key on shutdown"
            );
        }
    });

    info!(
        key = %key,
        endpoint = %config.advertise_endpoint,
        storage_profile = %config.storage_profile.as_str(),
        continuity_tier = %config.continuity_tier.as_str(),
        shared_mount_profiles = ?config.shared_mount_profiles,
        degraded_mode = config.degraded_mode,
        admission_frozen = config.admission_frozen,
        region = %config.region,
        zone = %config.zone,
        rack = %config.rack,
        ttl_secs = config.ttl_secs,
        "node registry heartbeat enabled"
    );

    Ok(Some(NodeRegistryHandle {
        key,
        stop_tx: Some(stop_tx),
        join: Some(join),
    }))
}

async fn connect_client(config: &NodeRegistryConfig) -> Result<EtcdClient> {
    EtcdClient::connect(config.etcd_endpoints.clone(), None)
        .await
        .context("connect etcd for node heartbeat")
}

async fn register_with_new_lease(
    client: &mut EtcdClient,
    config: &NodeRegistryConfig,
    key: &str,
) -> Result<NodeRegistration> {
    let lease = client
        .lease_grant(config.ttl_secs, None)
        .await
        .context("grant lease for node heartbeat")?;
    let lease_id = lease.id();
    write_node_record(client, config, key, lease_id).await?;
    let keepalive = start_node_registration_keepalive(client, lease_id).await?;
    Ok(NodeRegistration {
        lease_id,
        keepalive,
    })
}

async fn write_node_record(
    client: &mut EtcdClient,
    config: &NodeRegistryConfig,
    key: &str,
    lease_id: i64,
) -> Result<()> {
    client
        .put(
            key,
            node_record_payload(config),
            Some(PutOptions::new().with_lease(lease_id)),
        )
        .await
        .context("put node heartbeat key")?;
    Ok(())
}

async fn start_node_registration_keepalive(
    client: &mut EtcdClient,
    lease_id: i64,
) -> Result<NodeRegistrationKeepAlive> {
    client
        .lease_keep_alive(lease_id)
        .await
        .context("start node heartbeat lease keepalive")
}

async fn keep_node_registration_alive(registration: &mut NodeRegistration) -> Result<()> {
    const KEEPALIVE_RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);

    registration
        .keepalive
        .0
        .keep_alive()
        .await
        .context("send node heartbeat lease keepalive")?;
    let response = tokio::time::timeout(
        KEEPALIVE_RESPONSE_TIMEOUT,
        registration.keepalive.1.message(),
    )
    .await
    .context("node heartbeat lease keepalive response timed out")?
    .context("read node heartbeat lease keepalive response")?;
    let Some(response) = response else {
        bail!("node heartbeat lease keepalive stream closed");
    };
    if response.ttl() <= 0 {
        bail!(
            "node heartbeat lease keepalive returned non-positive ttl for lease {}",
            response.id()
        );
    }
    Ok(())
}

fn node_record_payload(config: &NodeRegistryConfig) -> String {
    json!({
        "node_id": config.node_id,
        "endpoint": config.advertise_endpoint,
        "max_active_vms": config.max_active_vms,
        "storage_profile": config.storage_profile.as_str(),
        "continuity_tier": config.continuity_tier.as_str(),
        "shared_mount_profiles": config.shared_mount_profiles,
        "degraded_mode": config.degraded_mode,
        "admission_frozen": config.admission_frozen,
        "region": config.region,
        "zone": config.zone,
        "rack": config.rack,
        "updated_at_unix_ms": unix_millis(),
    })
    .to_string()
}

async fn delete_key(client: &mut EtcdClient, key: &str) -> Result<()> {
    client.delete(key, None).await.context("delete node key")?;
    Ok(())
}

fn registry_key(config: &NodeRegistryConfig) -> String {
    format!(
        "{}/nodes/{}",
        config.key_prefix.trim_end_matches('/'),
        config.node_id
    )
}

fn heartbeat_interval(ttl_secs: i64) -> Duration {
    let ttl = ttl_secs.max(2) as u64;
    let interval = (ttl / 2).max(1);
    Duration::from_secs(interval)
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ContinuityTier, StorageProfile};

    fn test_config() -> NodeRegistryConfig {
        NodeRegistryConfig {
            etcd_endpoints: vec!["http://127.0.0.1:2379".to_string()],
            key_prefix: "/chevalier-sandbox".to_string(),
            node_id: "node-a".to_string(),
            advertise_endpoint: "http://10.0.0.10:8052".to_string(),
            ttl_secs: 30,
            max_active_vms: Some(8),
            storage_profile: StorageProfile::DurableShared,
            continuity_tier: ContinuityTier::TierB,
            degraded_mode: false,
            admission_frozen: true,
            shared_mount_profiles: vec!["local-path".to_string(), "gcs-vfs-fuse".to_string()],
            region: "us-west1".to_string(),
            zone: "regional".to_string(),
            rack: "rack-a".to_string(),
        }
    }

    #[test]
    fn registry_key_uses_configured_prefix_and_node_id() {
        assert_eq!(
            registry_key(&test_config()),
            "/chevalier-sandbox/nodes/node-a"
        );
    }

    #[test]
    fn heartbeat_interval_uses_half_ttl_with_minimum() {
        assert_eq!(heartbeat_interval(30), Duration::from_secs(15));
        assert_eq!(heartbeat_interval(1), Duration::from_secs(1));
    }

    #[test]
    fn node_record_payload_preserves_scheduling_fields() {
        let payload = node_record_payload(&test_config());
        let value: serde_json::Value = serde_json::from_str(&payload).expect("json payload");

        assert_eq!(value["node_id"], "node-a");
        assert_eq!(value["endpoint"], "http://10.0.0.10:8052");
        assert_eq!(value["max_active_vms"], 8);
        assert_eq!(value["storage_profile"], "durable-shared");
        assert_eq!(value["continuity_tier"], "tier-b");
        assert_eq!(value["admission_frozen"], true);
        assert_eq!(value["shared_mount_profiles"][1], "gcs-vfs-fuse");
        assert!(value["updated_at_unix_ms"].as_u64().is_some());
    }
}
