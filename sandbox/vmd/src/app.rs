// @dive-file: gRPC daemon service wiring for VM lifecycle, snapshots, forking, and control-plane policy enforcement.
// @dive-rel: Delegates orchestration to vmd/src/state/manager.rs and control-plane workers in reconcile/control_bus modules.
// @dive-rel: Translates manager/runtime failures into stable gRPC status contracts consumed by crates/sandbox.
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use futures::Stream;
use http::{Request as HttpRequest, Response as HttpResponse};
use prost_types::Timestamp;
use tokio::{fs, signal, sync::mpsc};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::metadata::MetadataMap;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tonic::{GrpcMethod, Request, Response, Status};
use tower_http::classify::GrpcFailureClass;
use tower_http::trace::TraceLayer;
use tracing::{Level, Span, debug, error, info};
use uuid::Uuid;

use crate::config::{Config, TlsServerConfig};
use crate::image::{self, PrebuiltImageStatus};
use crate::network;
use crate::partition::{self, PartitionGate, PartitionPolicyConfig};
use crate::pci::{PCI_CAPABILITY_HEADER, PciInventoryDevice, PciInventoryState};
use crate::proto::v1::{
    AttachPciDeviceRequest, CreateSnapshotRequest, CreateVmPhase, CreateVmProgress,
    CreateVmRequest, CreateVmStreamResponse, DeleteDurableVolumeRequest, DeleteSnapshotRequest,
    DeleteVmRequest, DetachPciDeviceRequest, DurableVolume, DurableVolumeAttachment, ForkVmRequest,
    ForkVmResponse, GetSnapshotRequest, GetVmBySessionRequest, GetVmRequest, HealthRequest,
    HealthResponse, HostPciDevice, HostPciDeviceState, HostPciFunction, InfoRequest, InfoResponse,
    ListDurableVolumesRequest, ListDurableVolumesResponse, ListHostPciDevicesRequest,
    ListHostPciDevicesResponse, ListSnapshotsRequest, ListSnapshotsResponse, ListVMsRequest,
    ListVMsResponse, PciDeviceActionResponse, PreDownloadVmImagePhase, PreDownloadVmImageRequest,
    PreDownloadVmImageResponse, ResourceSpec, RestoreSnapshotRequest, Snapshot, UpdateVmRequest,
    Vm, VmActionRequest, VmSource, VmSourceType as ProtoVmSourceType, VmState as ProtoVmState,
    create_vm_stream_response,
    vmd_service_server::{VmdService, VmdServiceServer},
};
use crate::state::manager::{CreateVmProgressCallback, CreateVmProgressEvent, CreateVmStage};
use crate::state::{
    CreateVmParams, ForkVmParams, Manager, ManagerError, SharedMountAvailability,
    SharedMountContinuity, SharedMountSpec, SnapshotMetadata, SnapshotParams, UpdateVmParams,
    VmMetadata, VmSource as StateVmSource, VmSourceType as StateVmSourceType, VmState,
};
use crate::{
    config::{ControlBusConfig, NodeRegistryConfig},
    control_bus, health_reconciler, public_ingress, reconcile, registry, virt,
};

pub async fn run_server(mut config: Config) -> Result<()> {
    config.normalize().context("normalize vmd server config")?;
    let addr: SocketAddr = config
        .listen_address
        .parse()
        .with_context(|| format!("parse listen address {}", config.listen_address))?;

    let manager = Arc::new(Manager::new(config.clone()).await.map_err(|e| anyhow!(e))?);
    let network_handle = network::start(&config)
        .await
        .context("start network services")?;
    let public_ingress_handle = public_ingress::start(&config, Arc::clone(&manager))
        .await
        .context("start public ingress server")?;
    let vm_health_reconciler_handle = health_reconciler::start(Arc::clone(&manager))
        .await
        .context("start vm health reconciler")?;
    let registry_handle = start_node_registry(config.node_registry.clone()).await?;
    let partition_handle =
        start_partition_monitor(config.node_registry.as_ref(), config.control_bus.as_ref()).await?;
    let partition_gate = partition_handle.as_ref().map(|handle| handle.gate());
    let svc = GrpcService {
        manager: Arc::clone(&manager),
        cfg: config.clone(),
        partition_gate: partition_gate.clone(),
    };

    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<VmdServiceServer<GrpcService>>()
        .await;

    let grpc_trace = TraceLayer::new_for_grpc()
        .make_span_with(|request: &HttpRequest<_>| {
            let grpc_method = request
                .extensions()
                .get::<GrpcMethod>()
                .map(|method| format!("{}.{}", method.service(), method.method()));
            match grpc_method {
                Some(name) => tracing::span!(
                    Level::DEBUG,
                    "grpc",
                    http_method = %request.method(),
                    path = %request.uri().path(),
                    grpc_method = %name
                ),
                None => tracing::span!(
                    Level::DEBUG,
                    "grpc",
                    http_method = %request.method(),
                    path = %request.uri().path()
                ),
            }
        })
        .on_request(|_request: &HttpRequest<_>, span: &Span| {
            debug!(parent: span, "gRPC request received");
        })
        .on_response(
            |response: &HttpResponse<_>, latency: Duration, span: &Span| {
                debug!(
                    parent: span,
                    status = %response.status(),
                    elapsed = ?latency,
                    "gRPC response sent"
                );
            },
        )
        .on_failure(|class: GrpcFailureClass, latency: Duration, span: &Span| {
            debug!(
                parent: span,
                classification = %class,
                elapsed = ?latency,
                "gRPC request failed"
            );
        });

    info!(listen = %addr, "starting vmd gRPC server");

    let (reconcile_trigger_tx, reconcile_trigger_rx) = mpsc::unbounded_channel::<()>();
    let _reconcile_trigger_guard = reconcile_trigger_tx.clone();
    let reconcile_handle = start_reconcile_worker(
        Arc::clone(&manager),
        config.node_registry.clone(),
        config.control_bus.clone(),
        reconcile_trigger_rx,
    )
    .await?;
    let command_consumer_handle = start_control_bus(
        config.control_bus.clone(),
        Arc::clone(&manager),
        Some(reconcile_trigger_tx),
        partition_gate,
    )
    .await?;

    let mut server = Server::builder().layer(grpc_trace);
    if let Some(tls_cfg) = config.security.tls.as_ref() {
        let tls = load_server_tls_config(tls_cfg)?;
        server = server.tls_config(tls)?;
    }

    server
        .add_service(health_service)
        .add_service(VmdServiceServer::new(svc))
        .serve_with_shutdown(addr, shutdown_signal())
        .await
        .context("serve gRPC")?;

    if let Some(handle) = registry_handle {
        handle.shutdown().await;
    }
    if let Some(handle) = command_consumer_handle {
        handle.shutdown().await;
    }
    if let Some(handle) = reconcile_handle {
        handle.shutdown().await;
    }
    if let Some(handle) = vm_health_reconciler_handle {
        handle.shutdown().await;
    }
    if let Some(handle) = partition_handle {
        handle.shutdown().await;
    }
    if let Some(handle) = network_handle {
        handle.shutdown().await;
    }
    if let Some(handle) = public_ingress_handle {
        handle.shutdown().await;
    }

    info!("vmd gRPC server stopped");
    Ok(())
}

fn load_server_tls_config(cfg: &TlsServerConfig) -> Result<ServerTlsConfig> {
    let cert = std::fs::read(&cfg.cert_path)
        .with_context(|| format!("read tls cert {}", cfg.cert_path))?;
    let key =
        std::fs::read(&cfg.key_path).with_context(|| format!("read tls key {}", cfg.key_path))?;
    let mut tls = ServerTlsConfig::new().identity(Identity::from_pem(cert, key));
    if let Some(client_ca_path) = &cfg.client_ca_path {
        let client_ca = std::fs::read(client_ca_path)
            .with_context(|| format!("read tls client ca {client_ca_path}"))?;
        tls = tls.client_ca_root(Certificate::from_pem(client_ca));
        if !cfg.require_client_cert {
            tls = tls.client_auth_optional(true);
        }
    }
    Ok(tls)
}

async fn start_node_registry(
    node_registry: Option<NodeRegistryConfig>,
) -> Result<Option<registry::NodeRegistryHandle>> {
    registry::start(node_registry)
        .await
        .context("start node registry heartbeat task")
}

async fn start_control_bus(
    control_bus_cfg: Option<ControlBusConfig>,
    manager: Arc<Manager>,
    reconcile_trigger_tx: Option<mpsc::UnboundedSender<()>>,
    partition_gate: Option<PartitionGate>,
) -> Result<Option<control_bus::CommandConsumerHandle>> {
    control_bus::start_with_trigger(
        control_bus_cfg,
        manager,
        reconcile_trigger_tx,
        partition_gate,
    )
    .await
    .context("start control command consumer")
}

async fn start_partition_monitor(
    node_registry: Option<&NodeRegistryConfig>,
    control_bus_cfg: Option<&ControlBusConfig>,
) -> Result<Option<partition::PartitionMonitorHandle>> {
    let config = derive_partition_policy_config(node_registry, control_bus_cfg);
    partition::start(config)
        .await
        .context("start partition monitor")
}

fn derive_partition_policy_config(
    node_registry: Option<&NodeRegistryConfig>,
    control_bus_cfg: Option<&ControlBusConfig>,
) -> Option<PartitionPolicyConfig> {
    let mut etcd_endpoints = Vec::new();
    let mut key_prefix = "/chevalier-sandbox".to_string();

    if let Some(registry) = node_registry {
        key_prefix = registry.key_prefix.clone();
        etcd_endpoints.extend(
            registry
                .etcd_endpoints
                .iter()
                .map(String::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
        );
    }

    if let Some(control) = control_bus_cfg {
        if etcd_endpoints.is_empty() {
            key_prefix = control
                .dedupe_prefix
                .split("/command-dedupe")
                .next()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or("/chevalier-sandbox")
                .to_string();
        }
        etcd_endpoints.extend(
            control
                .dedupe_etcd_endpoints
                .iter()
                .map(String::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
        );
    }

    etcd_endpoints.sort();
    etcd_endpoints.dedup();
    if etcd_endpoints.is_empty() {
        return None;
    }

    Some(PartitionPolicyConfig {
        etcd_endpoints,
        key_prefix,
        probe_interval: Duration::from_secs(2),
        failure_threshold: 3,
        local_stream_grace: Duration::from_secs(30),
        command_retry_delay: Duration::from_secs(2),
    })
}

async fn start_reconcile_worker(
    manager: Arc<Manager>,
    node_registry: Option<NodeRegistryConfig>,
    control_bus_cfg: Option<ControlBusConfig>,
    reconcile_trigger_rx: mpsc::UnboundedReceiver<()>,
) -> Result<Option<reconcile::ReconcileHandle>> {
    reconcile::start(
        manager,
        node_registry,
        control_bus_cfg,
        reconcile_trigger_rx,
    )
    .await
    .context("start reconcile worker")
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        let mut term = signal::unix::signal(signal::unix::SignalKind::terminate()).unwrap();
        term.recv().await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[derive(Clone)]
struct GrpcService {
    manager: Arc<Manager>,
    cfg: Config,
    partition_gate: Option<PartitionGate>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AccessLevel {
    Read,
    Write,
}

impl GrpcService {
    async fn authorize<T>(
        &self,
        request: &Request<T>,
        required: AccessLevel,
    ) -> Result<(), Status> {
        authorize_metadata(
            self.cfg.security.auth.as_ref(),
            request.metadata(),
            required,
        )?;

        if required == AccessLevel::Write {
            if let Some(gate) = &self.partition_gate {
                if !gate.mutation_allowed().await {
                    return Err(Status::unavailable(
                        gate.mutation_rejection_reason().await.unwrap_or_else(|| {
                            "network partition fail-closed: rejecting mutating commands".to_string()
                        }),
                    ));
                }
            }
        }

        Ok(())
    }

    fn authorize_pci(&self, metadata: &MetadataMap) -> Result<(), Status> {
        authorize_pci_metadata(self.cfg.pci.capability_token(), metadata)
    }
}

fn authorize_pci_metadata(expected: Option<&str>, metadata: &MetadataMap) -> Result<(), Status> {
    let expected = expected
        .ok_or_else(|| Status::failed_precondition("PCI assignment is disabled on this vmd"))?;
    let supplied = metadata
        .get(PCI_CAPABILITY_HEADER)
        .ok_or_else(|| Status::permission_denied("PCI capability token is required"))?
        .to_str()
        .map_err(|_| Status::permission_denied("PCI capability token is not valid ASCII"))?;
    if !constant_time_eq(supplied.as_bytes(), expected.as_bytes()) {
        return Err(Status::permission_denied("PCI capability token is invalid"));
    }
    Ok(())
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let width = left.len().max(right.len());
    for index in 0..width {
        difference |= usize::from(left.get(index).copied().unwrap_or_default())
            ^ usize::from(right.get(index).copied().unwrap_or_default());
    }
    difference == 0
}

fn authorize_metadata(
    auth_cfg: Option<&crate::config::AuthConfig>,
    metadata: &MetadataMap,
    required: AccessLevel,
) -> Result<(), Status> {
    let Some(auth_cfg) = auth_cfg else {
        return Ok(());
    };

    let token = extract_bearer_token(metadata)?;
    if token == auth_cfg.admin_token {
        return Ok(());
    }

    if required == AccessLevel::Read
        && auth_cfg
            .readonly_token
            .as_ref()
            .is_some_and(|readonly| token == *readonly)
    {
        return Ok(());
    }

    Err(Status::permission_denied(
        "token does not have sufficient permissions for this operation",
    ))
}

fn extract_bearer_token(metadata: &MetadataMap) -> Result<String, Status> {
    if let Some(token) = metadata.get("x-chevalier-auth-token") {
        let value = token
            .to_str()
            .map_err(|_| Status::unauthenticated("x-chevalier-auth-token is not valid ASCII"))?;
        if !value.trim().is_empty() {
            return Ok(value.trim().to_string());
        }
    }

    let value = metadata
        .get("authorization")
        .ok_or_else(|| Status::unauthenticated("authorization token is required"))?
        .to_str()
        .map_err(|_| Status::unauthenticated("authorization header is not valid ASCII"))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(Status::unauthenticated("authorization header is empty"));
    }
    if let Some(rest) = trimmed.strip_prefix("Bearer ") {
        let token = rest.trim();
        if token.is_empty() {
            return Err(Status::unauthenticated(
                "authorization bearer token is empty",
            ));
        }
        return Ok(token.to_string());
    }

    Ok(trimmed.to_string())
}

type GrpcResult<T> = Result<Response<T>, Status>;

#[tonic::async_trait]
impl VmdService for GrpcService {
    type CreateVMStream =
        Pin<Box<dyn Stream<Item = Result<CreateVmStreamResponse, Status>> + Send>>;
    type PreDownloadVmImageStream =
        Pin<Box<dyn Stream<Item = Result<PreDownloadVmImageResponse, Status>> + Send>>;

    async fn health(&self, request: Request<HealthRequest>) -> GrpcResult<HealthResponse> {
        self.authorize(&request, AccessLevel::Read).await?;
        Ok(Response::new(HealthResponse {
            status: "ok".to_string(),
        }))
    }

    async fn info(&self, request: Request<InfoRequest>) -> GrpcResult<InfoResponse> {
        self.authorize(&request, AccessLevel::Read).await?;
        Ok(Response::new(InfoResponse {
            listen: self.cfg.listen_address.clone(),
            data_dir: self.cfg.data_dir.clone(),
            qemu_bin: self.cfg.qemu_bin.clone(),
            qemu_img: self.cfg.qemu_img_bin.clone(),
            docker_bin: self.cfg.docker_bin.clone(),
            log_level: self.cfg.log_level.clone(),
        }))
    }

    async fn list_v_ms(&self, request: Request<ListVMsRequest>) -> GrpcResult<ListVMsResponse> {
        self.authorize(&request, AccessLevel::Read).await?;
        let include_snapshots = request.into_inner().include_snapshots;
        let vms = self.manager.list().await;
        let mut response = Vec::with_capacity(vms.len());
        for meta in vms {
            let (detail, runtime) = self
                .manager
                .get_with_runtime(&meta.id)
                .await
                .map_err(status_from_error)?;
            response.push(build_vm(&detail, Some(&runtime), include_snapshots));
        }
        Ok(Response::new(ListVMsResponse { vms: response }))
    }

    async fn get_vm(&self, request: Request<GetVmRequest>) -> GrpcResult<Vm> {
        self.authorize(&request, AccessLevel::Read).await?;
        let vm_id = request.into_inner().vm_id;
        if vm_id.is_empty() {
            return Err(Status::invalid_argument("vm_id is required"));
        }
        let (meta, runtime) = self
            .manager
            .get_with_runtime(&vm_id)
            .await
            .map_err(status_from_error)?;
        Ok(Response::new(build_vm(&meta, Some(&runtime), true)))
    }

    async fn get_vm_by_session(&self, request: Request<GetVmBySessionRequest>) -> GrpcResult<Vm> {
        self.authorize(&request, AccessLevel::Read).await?;
        let session_id = request.into_inner().session_id;
        if session_id.trim().is_empty() {
            return Err(Status::invalid_argument("session_id is required"));
        }
        let meta = self
            .manager
            .get_by_session_id(&session_id)
            .await
            .map_err(status_from_error)?;
        let (detail, runtime) = self
            .manager
            .get_with_runtime(&meta.id)
            .await
            .map_err(status_from_error)?;
        Ok(Response::new(build_vm(&detail, Some(&runtime), true)))
    }

    async fn create_vm(
        &self,
        request: Request<CreateVmRequest>,
    ) -> Result<Response<Self::CreateVMStream>, Status> {
        self.authorize(&request, AccessLevel::Write).await?;
        if !request.get_ref().pci_device_ids.is_empty() {
            self.authorize_pci(request.metadata())?;
        }
        let req = request.into_inner();
        let source = req
            .source
            .ok_or_else(|| Status::invalid_argument("source must be provided"))?;
        let source_type = map_source_type(source.r#type)?;
        if source.reference.is_empty() {
            return Err(Status::invalid_argument(
                "source reference must be provided",
            ));
        }

        let resources = req.resources.unwrap_or_default();
        let params = CreateVmParams {
            name: req.name,
            source: StateVmSource {
                source_type,
                reference: source.reference,
            },
            resources: crate::state::ResourceSpec {
                vcpu: resources.vcpu.max(0),
                memory_mb: resources.memory_mb.max(0),
                disk_gb: resources.disk_gb.max(0),
            },
            metadata: req.metadata.map_or_else(Default::default, |m| m.entries),
            auto_start: req.auto_start,
            architecture: req.architecture,
            // @dive: Shared mounts stay typed over gRPC so the daemon can validate and persist mount intent independently of opaque metadata keys.
            shared_mounts: req
                .shared_mounts
                .into_iter()
                .map(|mount| SharedMountSpec {
                    availability: match crate::proto::v1::SharedMountAvailability::try_from(
                        mount.availability,
                    )
                    .unwrap_or(crate::proto::v1::SharedMountAvailability::Unspecified)
                    {
                        crate::proto::v1::SharedMountAvailability::SharedStorage => {
                            SharedMountAvailability::SharedStorage
                        }
                        _ => SharedMountAvailability::NodeLocal,
                    },
                    continuity: {
                        let availability =
                            match crate::proto::v1::SharedMountAvailability::try_from(
                                mount.availability,
                            )
                            .unwrap_or(crate::proto::v1::SharedMountAvailability::Unspecified)
                            {
                                crate::proto::v1::SharedMountAvailability::SharedStorage => {
                                    SharedMountAvailability::SharedStorage
                                }
                                _ => SharedMountAvailability::NodeLocal,
                            };
                        match crate::proto::v1::SharedMountContinuity::try_from(mount.continuity)
                            .unwrap_or(crate::proto::v1::SharedMountContinuity::Unspecified)
                        {
                            crate::proto::v1::SharedMountContinuity::RestoreCrossNode => {
                                SharedMountContinuity::RestoreCrossNode
                            }
                            crate::proto::v1::SharedMountContinuity::RestartSameNode => {
                                SharedMountContinuity::RestartSameNode
                            }
                            crate::proto::v1::SharedMountContinuity::Unspecified => {
                                match availability {
                                    SharedMountAvailability::SharedStorage => {
                                        SharedMountContinuity::RestoreCrossNode
                                    }
                                    SharedMountAvailability::NodeLocal => {
                                        SharedMountContinuity::RestartSameNode
                                    }
                                }
                            }
                        }
                    },
                    backend_profile: mount.backend_profile.trim().to_ascii_lowercase(),
                    host_path: mount.host_path,
                    guest_path: mount.guest_path,
                    mount_tag: mount.mount_tag,
                    read_only: mount.read_only,
                    vfs_endpoint: mount.vfs_endpoint,
                    vfs_scope_path: mount.vfs_scope_path,
                })
                .collect(),
            pci_device_ids: req.pci_device_ids,
            storage_profile: req.storage_profile,
            volume_owner_key: if req.volume_owner_key.trim().is_empty() {
                None
            } else {
                Some(req.volume_owner_key)
            },
            volume_size_gb: (req.volume_size_gb > 0).then_some(req.volume_size_gb),
        };

        let manager = Arc::clone(&self.manager);
        let (tx, rx) = mpsc::unbounded_channel::<Result<CreateVmStreamResponse, Status>>();

        tokio::spawn(async move {
            let download_percent = Arc::new(Mutex::new(None::<u32>));
            let progress_tx = tx.clone();
            let progress_callback: CreateVmProgressCallback = {
                let progress_tx = progress_tx.clone();
                let download_percent = Arc::clone(&download_percent);
                Arc::new(move |event: CreateVmProgressEvent| {
                    handle_create_vm_progress(&progress_tx, &download_percent, event);
                })
            };

            let result = manager.create_vm(params, Some(progress_callback)).await;
            match result {
                Ok(meta) => match manager.get_with_runtime(&meta.id).await {
                    Ok((detail, runtime)) => {
                        publish_progress(
                            &progress_tx,
                            CreateVmPhase::Complete,
                            100,
                            format!("VM {} ready", detail.name),
                        );
                        let vm = build_vm(&detail, Some(&runtime), true);
                        let response = CreateVmStreamResponse {
                            event: Some(create_vm_stream_response::Event::Vm(vm)),
                        };
                        let _ = progress_tx.send(Ok(response));
                    }
                    Err(err) => {
                        let status = status_from_error(err);
                        let _ = progress_tx.send(Err(status));
                    }
                },
                Err(err) => {
                    let status = status_from_error(err);
                    let _ = tx.send(Err(status));
                }
            }
        });

        Ok(Response::new(
            Box::pin(UnboundedReceiverStream::new(rx)) as Self::CreateVMStream
        ))
    }

    async fn update_vm(&self, request: Request<UpdateVmRequest>) -> GrpcResult<Vm> {
        self.authorize(&request, AccessLevel::Write).await?;
        let req = request.into_inner();
        if req.vm_id.is_empty() {
            return Err(Status::invalid_argument("vm_id is required"));
        }
        let mut params = UpdateVmParams::default();
        if let Some(name) = req.name {
            params.name = Some(name);
        }
        if let Some(meta) = req.metadata {
            params.metadata = Some(meta.entries);
        }
        let meta = self
            .manager
            .update_vm(&req.vm_id, params)
            .await
            .map_err(status_from_error)?;
        let (detail, runtime) = self
            .manager
            .get_with_runtime(&meta.id)
            .await
            .map_err(status_from_error)?;
        Ok(Response::new(build_vm(&detail, Some(&runtime), true)))
    }

    async fn delete_vm(&self, request: Request<DeleteVmRequest>) -> GrpcResult<()> {
        self.authorize(&request, AccessLevel::Write).await?;
        let req = request.into_inner();
        if req.vm_id.is_empty() {
            return Err(Status::invalid_argument("vm_id is required"));
        }
        self.manager
            .delete_vm(&req.vm_id, req.purge_snapshots)
            .await
            .map_err(status_from_error)?;
        Ok(Response::new(()))
    }

    async fn fork_vm(&self, request: Request<ForkVmRequest>) -> GrpcResult<ForkVmResponse> {
        self.authorize(&request, AccessLevel::Write).await?;
        let req = request.into_inner();
        if req.parent_vm_id.trim().is_empty() {
            return Err(Status::invalid_argument("parent_vm_id is required"));
        }

        let params = ForkVmParams {
            child_name: if req.child_name.trim().is_empty() {
                None
            } else {
                Some(req.child_name)
            },
            child_metadata: req.child_metadata.map_or_else(HashMap::new, |m| m.entries),
            auto_start_child: req.auto_start_child,
        };

        let (parent_meta, child_meta, fork_id) = self
            .manager
            .fork_vm(&req.parent_vm_id, params)
            .await
            .map_err(status_from_error)?;
        let (parent_detail, parent_runtime) = self
            .manager
            .get_with_runtime(&parent_meta.id)
            .await
            .map_err(status_from_error)?;
        let (child_detail, child_runtime) = self
            .manager
            .get_with_runtime(&child_meta.id)
            .await
            .map_err(status_from_error)?;

        Ok(Response::new(ForkVmResponse {
            parent_vm: Some(build_vm(&parent_detail, Some(&parent_runtime), true)),
            child_vm: Some(build_vm(&child_detail, Some(&child_runtime), true)),
            fork_id,
        }))
    }

    async fn list_durable_volumes(
        &self,
        request: Request<ListDurableVolumesRequest>,
    ) -> GrpcResult<ListDurableVolumesResponse> {
        self.authorize(&request, AccessLevel::Read).await?;
        let volumes = self
            .manager
            .list_durable_volumes()
            .await
            .into_iter()
            .map(|(volume, attached_vm_ids)| DurableVolume {
                owner_key: volume.owner_key,
                volume_id: volume.volume_id,
                size_gb: volume.size_gb,
                created_at: Some(to_timestamp(volume.created_at)),
                updated_at: Some(to_timestamp(volume.updated_at)),
                backing_volume_id: volume.backing_volume_id.unwrap_or_default(),
                attached_vm_ids,
            })
            .collect();
        Ok(Response::new(ListDurableVolumesResponse { volumes }))
    }

    async fn delete_durable_volume(
        &self,
        request: Request<DeleteDurableVolumeRequest>,
    ) -> GrpcResult<()> {
        self.authorize(&request, AccessLevel::Write).await?;
        let owner_key = request.into_inner().owner_key;
        if owner_key.trim().is_empty() {
            return Err(Status::invalid_argument("owner_key is required"));
        }
        self.manager
            .delete_durable_volume(&owner_key)
            .await
            .map_err(status_from_error)?;
        Ok(Response::new(()))
    }

    async fn start_vm(&self, request: Request<VmActionRequest>) -> GrpcResult<Vm> {
        self.authorize(&request, AccessLevel::Write).await?;
        self.vm_action(request.into_inner(), Action::Start).await
    }

    async fn stop_vm(&self, request: Request<VmActionRequest>) -> GrpcResult<Vm> {
        self.authorize(&request, AccessLevel::Write).await?;
        self.vm_action(request.into_inner(), Action::Stop).await
    }

    async fn restart_vm(&self, request: Request<VmActionRequest>) -> GrpcResult<Vm> {
        self.authorize(&request, AccessLevel::Write).await?;
        self.vm_action(request.into_inner(), Action::Restart).await
    }

    async fn pause_vm(&self, request: Request<VmActionRequest>) -> GrpcResult<Vm> {
        self.authorize(&request, AccessLevel::Write).await?;
        self.vm_action(request.into_inner(), Action::Pause).await
    }

    async fn resume_vm(&self, request: Request<VmActionRequest>) -> GrpcResult<Vm> {
        self.authorize(&request, AccessLevel::Write).await?;
        self.vm_action(request.into_inner(), Action::Resume).await
    }

    async fn force_stop_vm(&self, request: Request<VmActionRequest>) -> GrpcResult<Vm> {
        self.authorize(&request, AccessLevel::Write).await?;
        self.vm_action(request.into_inner(), Action::ForceStop)
            .await
    }

    async fn list_host_pci_devices(
        &self,
        request: Request<ListHostPciDevicesRequest>,
    ) -> GrpcResult<ListHostPciDevicesResponse> {
        self.authorize(&request, AccessLevel::Read).await?;
        if !self.manager.pci_enabled() {
            return Ok(Response::new(ListHostPciDevicesResponse {
                enabled: false,
                devices: Vec::new(),
            }));
        }
        self.authorize_pci(request.metadata())?;
        let devices = self
            .manager
            .list_host_pci_devices()
            .await
            .into_iter()
            .map(build_host_pci_device)
            .collect();
        Ok(Response::new(ListHostPciDevicesResponse {
            enabled: true,
            devices,
        }))
    }

    async fn attach_pci_device(
        &self,
        request: Request<AttachPciDeviceRequest>,
    ) -> GrpcResult<PciDeviceActionResponse> {
        self.authorize(&request, AccessLevel::Write).await?;
        self.authorize_pci(request.metadata())?;
        let req = request.into_inner();
        if req.vm_id.trim().is_empty() || req.device_id.trim().is_empty() {
            return Err(Status::invalid_argument("vm_id and device_id are required"));
        }
        let result = self
            .manager
            .attach_pci_device(&req.vm_id, &req.device_id)
            .await
            .map_err(status_from_error)?;
        let (_, runtime) = self
            .manager
            .get_with_runtime(&req.vm_id)
            .await
            .map_err(status_from_error)?;
        let device = self
            .manager
            .list_host_pci_devices()
            .await
            .into_iter()
            .find(|device| device.id == req.device_id)
            .map(build_host_pci_device);
        Ok(Response::new(PciDeviceActionResponse {
            vm: Some(build_vm(&result.metadata, Some(&runtime), true)),
            device,
            restart_required: result.restart_required,
            detail: result.detail,
        }))
    }

    async fn detach_pci_device(
        &self,
        request: Request<DetachPciDeviceRequest>,
    ) -> GrpcResult<PciDeviceActionResponse> {
        self.authorize(&request, AccessLevel::Write).await?;
        self.authorize_pci(request.metadata())?;
        let req = request.into_inner();
        if req.vm_id.trim().is_empty() || req.device_id.trim().is_empty() {
            return Err(Status::invalid_argument("vm_id and device_id are required"));
        }
        let result = self
            .manager
            .detach_pci_device(&req.vm_id, &req.device_id)
            .await
            .map_err(status_from_error)?;
        let (_, runtime) = self
            .manager
            .get_with_runtime(&req.vm_id)
            .await
            .map_err(status_from_error)?;
        let device = self
            .manager
            .list_host_pci_devices()
            .await
            .into_iter()
            .find(|device| device.id == req.device_id)
            .map(build_host_pci_device);
        Ok(Response::new(PciDeviceActionResponse {
            vm: Some(build_vm(&result.metadata, Some(&runtime), true)),
            device,
            restart_required: result.restart_required,
            detail: result.detail,
        }))
    }

    async fn pre_download_vm_image(
        &self,
        request: Request<PreDownloadVmImageRequest>,
    ) -> Result<Response<Self::PreDownloadVmImageStream>, Status> {
        self.authorize(&request, AccessLevel::Write).await?;
        let req = request.into_inner();
        if req.reference.trim().is_empty() {
            return Err(Status::invalid_argument("reference is required"));
        }

        let arch = if req.architecture.trim().is_empty() {
            self.manager.host_architecture().to_string()
        } else {
            self.manager
                .normalize_architecture(&req.architecture)
                .map_err(status_from_error)?
        };
        if arch.is_empty() {
            return Err(Status::failed_precondition(
                "daemon host architecture is unknown",
            ));
        }

        let reference = req.reference;
        let target = image::default_base_image_path(&self.cfg, &reference, &arch);
        let force = req.force;
        let docker_bin = self.cfg.docker_bin.clone();
        let manager = Arc::clone(&self.manager);
        let (tx, rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            if tx
                .send(Ok(build_pre_download_response(
                    PreDownloadVmImagePhase::CheckingCache,
                    format!("checking cache for {}", reference),
                    0,
                    None,
                )))
                .is_err()
            {
                return;
            }

            let _base_image_guard = manager.lock_base_image(&target).await;
            match fs::metadata(&target).await {
                Ok(metadata) if metadata.len() > 0 && !force => {
                    let size = metadata.len();
                    let message = format!("base image already present at {}", target.display());
                    let _ = tx.send(Ok(build_pre_download_response(
                        PreDownloadVmImagePhase::AlreadyPresent,
                        message.clone(),
                        size,
                        Some(size),
                    )));
                    let _ = tx.send(Ok(build_pre_download_response(
                        PreDownloadVmImagePhase::Complete,
                        message,
                        size,
                        Some(size),
                    )));
                    return;
                }
                Ok(metadata) if metadata.len() > 0 && force => {
                    let _ = tx.send(Ok(build_pre_download_response(
                        PreDownloadVmImagePhase::CheckingCache,
                        format!("replacing existing image at {}", target.display()),
                        metadata.len(),
                        Some(metadata.len()),
                    )));
                    if let Err(err) = fs::remove_file(&target).await {
                        let status = Status::internal(format!(
                            "failed to remove existing image {}: {err}",
                            target.display()
                        ));
                        let _ = tx.send(Err(status));
                        return;
                    }
                }
                Ok(_) => {
                    if let Err(err) = fs::remove_file(&target).await {
                        if err.kind() != ErrorKind::NotFound {
                            let status = Status::internal(format!(
                                "failed to remove empty image {}: {err}",
                                target.display()
                            ));
                            let _ = tx.send(Err(status));
                            return;
                        }
                    }
                }
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => {
                    let status = Status::internal(format!(
                        "failed to inspect image {}: {err}",
                        target.display()
                    ));
                    let _ = tx.send(Err(status));
                    return;
                }
            }

            let download_message = format!("downloading {} for {}", reference, arch);
            let _ = tx.send(Ok(build_pre_download_response(
                PreDownloadVmImagePhase::Downloading,
                download_message.clone(),
                0,
                None,
            )));

            let progress_sender = tx.clone();
            let download_result = image::download_prebuilt_image_with_progress(
                &reference,
                &arch,
                target.as_path(),
                move |progress| {
                    let _ = progress_sender.send(Ok(build_pre_download_response(
                        PreDownloadVmImagePhase::Downloading,
                        String::new(),
                        progress.downloaded_bytes,
                        progress.total_bytes,
                    )));
                },
            )
            .await;

            match download_result {
                Ok(PrebuiltImageStatus::Downloaded { bytes }) => {
                    let message = format!("stored VM image at {}", target.display());
                    let _ = tx.send(Ok(build_pre_download_response(
                        PreDownloadVmImagePhase::Complete,
                        message,
                        bytes,
                        Some(bytes),
                    )));
                }
                Ok(PrebuiltImageStatus::NotFound) => {
                    // @dive: Prewarm must fully seed qcow cache; if registry miss occurs, fallback to d2vm conversion now.
                    let _ = tx.send(Ok(build_pre_download_response(
                        PreDownloadVmImagePhase::Downloading,
                        format!(
                            "no prebuilt VM image for {reference} ({arch}); converting from container image"
                        ),
                        0,
                        None,
                    )));

                    match convert_container_image_to_qcow(
                        docker_bin.as_str(),
                        reference.as_str(),
                        arch.as_str(),
                        target.as_path(),
                    )
                    .await
                    {
                        Ok(bytes) => {
                            let message =
                                format!("stored converted VM image at {}", target.display());
                            let _ = tx.send(Ok(build_pre_download_response(
                                PreDownloadVmImagePhase::Complete,
                                message,
                                bytes,
                                Some(bytes),
                            )));
                        }
                        Err(status) => {
                            let _ = tx.send(Err(status));
                        }
                    }
                }
                Err(err) => {
                    let status = Status::internal(format!(
                        "failed to download VM image {reference} ({arch}): {err}"
                    ));
                    let _ = tx.send(Err(status));
                }
            }
        });

        let stream = UnboundedReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(stream) as Self::PreDownloadVmImageStream
        ))
    }

    async fn list_snapshots(
        &self,
        request: Request<ListSnapshotsRequest>,
    ) -> GrpcResult<ListSnapshotsResponse> {
        self.authorize(&request, AccessLevel::Read).await?;
        let req = request.into_inner();
        if req.vm_id.is_empty() {
            return Err(Status::invalid_argument("vm_id is required"));
        }
        let snaps = self
            .manager
            .list_snapshots(&req.vm_id)
            .await
            .map_err(status_from_error)?;
        let response = ListSnapshotsResponse {
            snapshots: snaps.into_iter().map(build_snapshot).collect(),
        };
        Ok(Response::new(response))
    }

    async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> GrpcResult<Snapshot> {
        self.authorize(&request, AccessLevel::Write).await?;
        let req = request.into_inner();
        if req.vm_id.is_empty() {
            return Err(Status::invalid_argument("vm_id is required"));
        }
        let params = SnapshotParams {
            label: req.label,
            description: req.description,
        };
        let snap = self
            .manager
            .create_snapshot(&req.vm_id, params)
            .await
            .map_err(status_from_error)?;
        Ok(Response::new(build_snapshot(snap)))
    }

    async fn get_snapshot(&self, request: Request<GetSnapshotRequest>) -> GrpcResult<Snapshot> {
        self.authorize(&request, AccessLevel::Read).await?;
        let req = request.into_inner();
        if req.vm_id.is_empty() || req.snapshot_id.is_empty() {
            return Err(Status::invalid_argument(
                "vm_id and snapshot_id are required",
            ));
        }
        let snap = self
            .manager
            .snapshot(&req.vm_id, &req.snapshot_id)
            .await
            .map_err(status_from_error)?;
        Ok(Response::new(build_snapshot(snap)))
    }

    async fn restore_snapshot(&self, request: Request<RestoreSnapshotRequest>) -> GrpcResult<Vm> {
        self.authorize(&request, AccessLevel::Write).await?;
        let req = request.into_inner();
        if req.vm_id.is_empty() || req.snapshot_id.is_empty() {
            return Err(Status::invalid_argument(
                "vm_id and snapshot_id are required",
            ));
        }
        let meta = self
            .manager
            .restore_snapshot(&req.vm_id, &req.snapshot_id)
            .await
            .map_err(status_from_error)?;
        let (detail, runtime) = self
            .manager
            .get_with_runtime(&meta.id)
            .await
            .map_err(status_from_error)?;
        Ok(Response::new(build_vm(&detail, Some(&runtime), true)))
    }

    async fn delete_snapshot(&self, request: Request<DeleteSnapshotRequest>) -> GrpcResult<()> {
        self.authorize(&request, AccessLevel::Write).await?;
        let req = request.into_inner();
        if req.vm_id.is_empty() || req.snapshot_id.is_empty() {
            return Err(Status::invalid_argument(
                "vm_id and snapshot_id are required",
            ));
        }
        self.manager
            .delete_snapshot(&req.vm_id, &req.snapshot_id)
            .await
            .map_err(status_from_error)?;
        Ok(Response::new(()))
    }
}

async fn convert_container_image_to_qcow(
    docker_bin: &str,
    reference: &str,
    arch: &str,
    target: &std::path::Path,
) -> Result<u64, Status> {
    let base_dir = target.parent().ok_or_else(|| {
        Status::internal(format!(
            "failed to resolve base image directory for {}",
            target.display()
        ))
    })?;

    if let Err(err) = fs::create_dir_all(base_dir).await {
        return Err(Status::internal(format!(
            "failed to create base image directory {}: {err}",
            base_dir.display()
        )));
    }

    let base_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            Status::internal(format!("invalid target file name {}", target.display()))
        })?;
    let tmp_name = format!(
        "{}-{}.{}",
        base_name.trim_end_matches(image::BASE_IMAGE_EXT),
        Uuid::new_v4(),
        image::BASE_IMAGE_EXT.trim_start_matches('.')
    );
    let tmp_path = base_dir.join(&tmp_name);

    let options = virt::D2VmOptions {
        image: reference.to_string(),
        output: tmp_name.clone(),
        disk_gb: image::BASE_IMAGE_SIZE_GB,
        pull: true,
        platform: Some(format!("linux/{arch}")),
        include_bootstrap: true,
    };

    if let Err(err) = virt::run_d2vm(docker_bin, base_dir, options).await {
        let _ = fs::remove_file(&tmp_path).await;
        return Err(Status::internal(format!(
            "failed to convert VM image {reference} ({arch}): {err}"
        )));
    }

    if let Err(err) = fs::rename(&tmp_path, target).await {
        let _ = fs::remove_file(&tmp_path).await;
        return Err(Status::internal(format!(
            "failed to store converted VM image at {}: {err}",
            target.display()
        )));
    }

    fs::metadata(target)
        .await
        .map(|metadata| metadata.len())
        .map_err(|err| {
            Status::internal(format!(
                "failed to inspect converted VM image {}: {err}",
                target.display()
            ))
        })
}

enum Action {
    Start,
    Stop,
    Restart,
    Pause,
    Resume,
    ForceStop,
}

impl GrpcService {
    async fn vm_action(&self, req: VmActionRequest, action: Action) -> GrpcResult<Vm> {
        let vm_id = req.vm_id.clone();
        if vm_id.is_empty() {
            return Err(Status::invalid_argument("vm_id is required"));
        }
        let meta = match action {
            Action::Start => self.manager.start_vm(&vm_id).await,
            Action::Stop => self.manager.stop_vm(&vm_id).await,
            Action::Restart => self.manager.restart_vm(&vm_id).await,
            Action::Pause => self.manager.pause_vm(&vm_id).await,
            Action::Resume => self.manager.resume_vm(&vm_id).await,
            Action::ForceStop => self.manager.force_stop_vm(&vm_id).await,
        }
        .map_err(status_from_error)?;
        let (detail, runtime) = self
            .manager
            .get_with_runtime(&meta.id)
            .await
            .map_err(status_from_error)?;
        Ok(Response::new(build_vm(&detail, Some(&runtime), true)))
    }
}

fn build_pre_download_response(
    phase: PreDownloadVmImagePhase,
    message: String,
    downloaded: u64,
    total: Option<u64>,
) -> PreDownloadVmImageResponse {
    PreDownloadVmImageResponse {
        phase: phase as i32,
        message,
        downloaded_bytes: downloaded,
        total_bytes: total.unwrap_or(0),
    }
}

fn handle_create_vm_progress(
    sender: &mpsc::UnboundedSender<Result<CreateVmStreamResponse, Status>>,
    download_percent: &Mutex<Option<u32>>,
    event: CreateVmProgressEvent,
) {
    match event {
        CreateVmProgressEvent::DownloadBytes {
            downloaded_bytes,
            total_bytes,
        } => {
            if let Some(total) = total_bytes {
                if total == 0 {
                    publish_progress(
                        sender,
                        CreateVmPhase::DownloadingImage,
                        0,
                        format!("downloading VM image: {downloaded_bytes} bytes"),
                    );
                } else {
                    let mut percent = (downloaded_bytes as f64 / total as f64) * 100.0;
                    if percent > 100.0 {
                        percent = 100.0;
                    }
                    let percent_u32 = percent.round() as u32;
                    let mut guard = download_percent
                        .lock()
                        .expect("download percent lock poisoned");
                    if guard.map(|p| percent_u32 > p).unwrap_or(true) {
                        *guard = Some(percent_u32);
                        publish_progress(
                            sender,
                            CreateVmPhase::DownloadingImage,
                            percent_u32,
                            format!(
                                "downloading VM image: {} / {} bytes",
                                downloaded_bytes, total
                            ),
                        );
                    }
                }
            } else {
                publish_progress(
                    sender,
                    CreateVmPhase::DownloadingImage,
                    0,
                    format!("downloading VM image: {} bytes", downloaded_bytes),
                );
            }
        }
        CreateVmProgressEvent::StageProgress {
            stage,
            percent,
            message,
        } => {
            let phase = map_stage_to_phase(stage);
            let msg = message
                .filter(|m| !m.is_empty())
                .unwrap_or_else(|| default_progress_message(phase).to_string());
            publish_progress(sender, phase, percent, msg);
        }
    }
}

fn publish_progress(
    sender: &mpsc::UnboundedSender<Result<CreateVmStreamResponse, Status>>,
    phase: CreateVmPhase,
    percent: u32,
    message: String,
) {
    let response = CreateVmStreamResponse {
        event: Some(create_vm_stream_response::Event::Progress(
            CreateVmProgress {
                phase: phase as i32,
                message,
                percent,
            },
        )),
    };
    let _ = sender.send(Ok(response));
}

fn map_stage_to_phase(stage: CreateVmStage) -> CreateVmPhase {
    match stage {
        CreateVmStage::DownloadImage => CreateVmPhase::DownloadingImage,
        CreateVmStage::ConvertImage => CreateVmPhase::ConvertingImage,
        CreateVmStage::StartVm => CreateVmPhase::StartingVm,
    }
}

fn default_progress_message(phase: CreateVmPhase) -> &'static str {
    match phase {
        CreateVmPhase::DownloadingImage => "downloading VM image",
        CreateVmPhase::ConvertingImage => "converting VM disk",
        CreateVmPhase::StartingVm => "starting VM",
        CreateVmPhase::Complete => "VM ready",
        CreateVmPhase::Unspecified => "working",
    }
}

fn build_host_pci_device(device: PciInventoryDevice) -> HostPciDevice {
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
        state: match device.state {
            PciInventoryState::Unavailable => HostPciDeviceState::Unavailable as i32,
            PciInventoryState::Host => HostPciDeviceState::Host as i32,
            PciInventoryState::Ready => HostPciDeviceState::Ready as i32,
            PciInventoryState::Assigned => HostPciDeviceState::Assigned as i32,
            PciInventoryState::Error => HostPciDeviceState::Error as i32,
        },
        assigned_vm_id: device.assigned_vm_id,
        managed: device.managed,
        hotplug_capable: device.hotplug_capable,
        unavailable_reason: device.unavailable_reason,
    }
}

fn build_vm(
    meta: &VmMetadata,
    runtime: Option<&crate::state::VmRuntime>,
    include_snapshots: bool,
) -> Vm {
    let mut vm = Vm {
        id: meta.id.clone(),
        name: meta.name.clone(),
        state: map_vm_state(meta.state.clone()),
        architecture: meta.architecture.clone(),
        created_at: Some(to_timestamp(meta.created_at)),
        updated_at: Some(to_timestamp(meta.updated_at)),
        source: Some(VmSource {
            r#type: map_source_type_proto(&meta.source.source_type),
            reference: meta.source.reference.clone(),
        }),
        resources: Some(ResourceSpec {
            vcpu: meta.resources.vcpu,
            memory_mb: meta.resources.memory_mb,
            disk_gb: meta.resources.disk_gb,
        }),
        network: Some(crate::proto::v1::NetworkSpec {
            mac: meta.network.mac.clone(),
            portproxy_ports: Some(crate::proto::v1::PortProxyPorts {
                proxy_port: meta.network.proxy_port,
                rpc_port: meta.network.rpc_port,
            }),
        }),
        metadata: meta.metadata.clone(),
        snapshots: Vec::new(),
        started_at: meta.started_at.map(to_timestamp),
        shared_mounts: meta
            .shared_mounts
            .iter()
            .map(|mount| crate::proto::v1::SharedMount {
                host_path: mount.host_path.clone(),
                guest_path: mount.guest_path.clone(),
                mount_tag: mount.mount_tag.clone(),
                read_only: mount.read_only,
                availability: match mount.availability {
                    SharedMountAvailability::NodeLocal => {
                        crate::proto::v1::SharedMountAvailability::NodeLocal as i32
                    }
                    SharedMountAvailability::SharedStorage => {
                        crate::proto::v1::SharedMountAvailability::SharedStorage as i32
                    }
                },
                continuity: match mount.continuity {
                    SharedMountContinuity::RestartSameNode => {
                        crate::proto::v1::SharedMountContinuity::RestartSameNode as i32
                    }
                    SharedMountContinuity::RestoreCrossNode => {
                        crate::proto::v1::SharedMountContinuity::RestoreCrossNode as i32
                    }
                },
                backend_profile: mount.backend_profile.clone(),
                vfs_endpoint: mount.vfs_endpoint.clone(),
                vfs_scope_path: mount.vfs_scope_path.clone(),
            })
            .collect(),
        pci_devices: meta
            .pci_devices
            .iter()
            .map(|assignment| crate::proto::v1::PciDeviceAssignment {
                id: assignment.id.clone(),
                bdfs: assignment.bdfs.clone(),
            })
            .collect(),
        durable_volume: meta
            .durable_volume
            .as_ref()
            .map(|volume| DurableVolumeAttachment {
                owner_key: volume.owner_key.clone(),
                volume_id: volume.volume_id.clone(),
                size_gb: volume.size_gb,
            }),
    };

    if let Some(runtime) = runtime {
        vm.state = map_vm_state(runtime.state.clone());
        if let Some(started) = runtime.started_at {
            if runtime.state == VmState::Running {
                vm.started_at = Some(to_timestamp(started));
            }
        }
    }

    if include_snapshots {
        vm.snapshots = meta.snapshots.iter().cloned().map(build_snapshot).collect();
    }

    vm
}

fn build_snapshot(meta: SnapshotMetadata) -> Snapshot {
    Snapshot {
        id: meta.id,
        name: meta.name,
        label: meta.label,
        description: meta.description,
        created_at: Some(to_timestamp(meta.created_at)),
    }
}

fn map_vm_state(state: VmState) -> i32 {
    match state {
        VmState::Creating => ProtoVmState::Creating as i32,
        VmState::Stopped => ProtoVmState::Stopped as i32,
        VmState::Running => ProtoVmState::Running as i32,
        VmState::Paused => ProtoVmState::Paused as i32,
        VmState::Error => ProtoVmState::Error as i32,
    }
}

fn map_source_type_proto(source_type: &StateVmSourceType) -> i32 {
    match source_type {
        StateVmSourceType::Docker => ProtoVmSourceType::Docker as i32,
        StateVmSourceType::Snapshot => ProtoVmSourceType::Snapshot as i32,
    }
}

fn map_source_type(value: i32) -> Result<StateVmSourceType, Status> {
    match ProtoVmSourceType::try_from(value) {
        Ok(ProtoVmSourceType::Docker) => Ok(StateVmSourceType::Docker),
        Ok(ProtoVmSourceType::Snapshot) => Ok(StateVmSourceType::Snapshot),
        Ok(ProtoVmSourceType::Unspecified) | Err(_) => {
            Err(Status::invalid_argument("source type must be provided"))
        }
    }
}

fn to_timestamp(ts: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: ts.timestamp(),
        nanos: ts.timestamp_subsec_nanos() as i32,
    }
}

fn status_from_error(err: ManagerError) -> Status {
    match err {
        ManagerError::VmNotFound => Status::not_found(err.to_string()),
        ManagerError::SnapshotNotFound => Status::not_found(err.to_string()),
        ManagerError::VolumeNotFound => Status::not_found(err.to_string()),
        ManagerError::VolumeInUse(_) => Status::failed_precondition(err.to_string()),
        ManagerError::InvalidState => Status::failed_precondition(err.to_string()),
        ManagerError::Cancelled => Status::cancelled(err.to_string()),
        ManagerError::PciConflict(_) => Status::already_exists(err.to_string()),
        ManagerError::PciUnavailable(_) => Status::failed_precondition(err.to_string()),
        ManagerError::Unsupported(_) => Status::unimplemented(err.to_string()),
        ManagerError::CapacityExceeded { .. } => {
            Status::resource_exhausted(format!("{} retry_after_ms=2000", err))
        }
        ManagerError::Other(e) => {
            let message = sanitize_status_message(&e.to_string());
            error!(error = ?e, status_message = %message, "manager operation failed");
            Status::internal(message)
        }
        ManagerError::Io(e) => {
            let message = sanitize_status_message(&e.to_string());
            error!(error = ?e, status_message = %message, "manager io operation failed");
            Status::internal(message)
        }
    }
}

fn sanitize_status_message(raw: &str) -> String {
    const MAX_LEN: usize = 1024;
    let mut out = String::with_capacity(raw.len().min(MAX_LEN));
    let mut truncated = false;

    for ch in raw.chars() {
        let mapped = match ch {
            '\n' | '\r' | '\t' => ' ',
            c if c.is_control() => continue,
            c => c,
        };
        if out.len() + mapped.len_utf8() > MAX_LEN {
            truncated = true;
            break;
        }
        out.push(mapped);
    }

    let compact = out.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.is_empty() {
        "internal error".to_string()
    } else if truncated {
        format!("{compact} ...(truncated)")
    } else {
        compact
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    #[test]
    fn extract_bearer_token_supports_authorization_header() {
        let mut req = Request::new(());
        req.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from("Bearer super-secret").expect("valid metadata value"),
        );

        let token = extract_bearer_token(req.metadata()).expect("token should parse");
        assert_eq!(token, "super-secret");
    }

    #[test]
    fn authorize_metadata_enforces_readonly_vs_write() {
        let auth = crate::config::AuthConfig {
            admin_token: "admin".to_string(),
            readonly_token: Some("readonly".to_string()),
        };

        let mut read_req = Request::new(());
        read_req.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from("Bearer readonly").expect("valid metadata value"),
        );
        assert!(authorize_metadata(Some(&auth), read_req.metadata(), AccessLevel::Read).is_ok());
        assert!(authorize_metadata(Some(&auth), read_req.metadata(), AccessLevel::Write).is_err());
    }

    #[test]
    fn authorize_metadata_accepts_admin_for_write() {
        let auth = crate::config::AuthConfig {
            admin_token: "admin".to_string(),
            readonly_token: Some("readonly".to_string()),
        };

        let mut req = Request::new(());
        req.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from("Bearer admin").expect("valid metadata value"),
        );
        assert!(authorize_metadata(Some(&auth), req.metadata(), AccessLevel::Write).is_ok());
    }

    #[test]
    fn pci_capability_is_separate_and_disabled_without_policy() {
        let mut request = Request::new(());
        request.metadata_mut().insert(
            PCI_CAPABILITY_HEADER,
            MetadataValue::try_from("pci-secret").expect("valid metadata value"),
        );
        assert!(authorize_pci_metadata(None, request.metadata()).is_err());
        assert!(authorize_pci_metadata(Some("different"), request.metadata()).is_err());
        assert!(authorize_pci_metadata(Some("pci-secret"), request.metadata()).is_ok());
    }
}
