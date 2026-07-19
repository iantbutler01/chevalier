// @dive-file: Facade-level contract tests covering session lifecycle, fork behavior, and distributed continuity paths.
// @dive-rel: Exercises crates/sandbox/src/lib.rs user-surface semantics against mock gRPC runtimes.
// @dive-rel: Provides verifier-backed evidence for distributed checklist gates in scripts/verify_chevalier_sandbox.sh.
use std::collections::{BTreeSet, HashMap};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chevalier_sandbox::proto::bracket::portproxy::v1::port_proxy_server::{
    PortProxy, PortProxyServer,
};
use chevalier_sandbox::proto::bracket::portproxy::v1::shell_exec_server::{
    ShellExec, ShellExecServer,
};
use chevalier_sandbox::proto::bracket::portproxy::v1::{
    DeletePathRequest, DirectoryEntry, ExecRequest, ExecResponse, InteractiveShellRequest,
    InteractiveShellResponse, ListDirectoryRequest, ListDirectoryResponse, ReadFileRequest,
    ReadFileResponse, WriteFileRequest, exec_request, exec_response, interactive_shell_request,
    interactive_shell_response,
};
use chevalier_sandbox::proto::google::protobuf::Empty;
use chevalier_sandbox::proto::vmd::v1::vmd_service_server::{VmdService, VmdServiceServer};
use chevalier_sandbox::proto::vmd::v1::{
    AttachPciDeviceRequest, CreateSnapshotRequest, CreateVmRequest, CreateVmStreamResponse,
    DeleteDurableVolumeRequest, DeleteSnapshotRequest, DeleteVmRequest, DetachPciDeviceRequest,
    ForkVmRequest, ForkVmResponse, GetSnapshotRequest, GetVmBySessionRequest, GetVmRequest,
    HealthRequest, HealthResponse, InfoRequest, InfoResponse, ListDurableVolumesRequest,
    ListDurableVolumesResponse, ListHostPciDevicesRequest, ListHostPciDevicesResponse,
    ListSnapshotsRequest, ListSnapshotsResponse, ListVMsRequest, ListVMsResponse, NetworkSpec,
    PciDeviceActionResponse, PortProxyPorts, PreDownloadVmImageRequest, PreDownloadVmImageResponse,
    ResourceSpec, RestoreSnapshotRequest, Snapshot, UpdateVmRequest, Vm, VmActionRequest, VmSource,
    VmSourceType, VmState, create_vm_stream_response,
};
use chevalier_sandbox::{
    ExecEvent, ExecInput, ExecOptions, ForkOptions, Sandbox, SandboxConfig, SandboxError,
    SessionOptions, SharedMount, SharedMountAvailability, SharedMountContinuity, ShellEvent,
    ShellInput, WarmPoolProfile,
};
use futures::{Stream, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, oneshot};
use tokio::time::{sleep, timeout};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

#[derive(Default)]
struct MockVmdState {
    vms: HashMap<String, Vm>,
    next_vm_id: usize,
    start_calls: usize,
    stop_calls: usize,
    delete_calls: usize,
    restore_calls: usize,
    restored_snapshot_ids: Vec<String>,
    predownload_requests: Vec<(String, String)>,
    create_requests: Vec<CreateVmRequest>,
    list_durable_volumes_delay: Option<Duration>,
}

#[derive(Clone)]
struct MockVmd {
    state: Arc<Mutex<MockVmdState>>,
    rpc_port: i32,
    proxy_port: i32,
}

impl MockVmd {
    fn build_vm(
        &self,
        id: String,
        name: String,
        state: i32,
        metadata: HashMap<String, String>,
    ) -> Vm {
        Vm {
            id,
            name,
            state,
            architecture: "amd64".to_string(),
            created_at: None,
            updated_at: None,
            source: Some(VmSource {
                r#type: VmSourceType::Docker as i32,
                reference: "mock/image:latest".to_string(),
            }),
            resources: Some(ResourceSpec {
                vcpu: 2,
                memory_mb: 1024,
                disk_gb: 10,
            }),
            network: Some(NetworkSpec {
                mac: "02:00:00:00:00:01".to_string(),
                portproxy_ports: Some(PortProxyPorts {
                    proxy_port: self.proxy_port,
                    rpc_port: self.rpc_port,
                }),
            }),
            metadata,
            snapshots: Vec::new(),
            started_at: None,
            shared_mounts: Vec::new(),
            pci_devices: Vec::new(),
            durable_volume: None,
        }
    }

    async fn vm_mutate(&self, vm_id: &str, state: i32, increment_stop: bool) -> Result<Vm, Status> {
        let mut guard = self.state.lock().await;
        let vm = guard
            .vms
            .get_mut(vm_id)
            .ok_or_else(|| Status::not_found("vm not found"))?;
        vm.state = state;
        let vm_clone = vm.clone();
        if increment_stop {
            guard.stop_calls += 1;
        }
        Ok(vm_clone)
    }
}

#[tonic::async_trait]
impl VmdService for MockVmd {
    type CreateVMStream =
        Pin<Box<dyn Stream<Item = Result<CreateVmStreamResponse, Status>> + Send>>;
    type PreDownloadVmImageStream =
        Pin<Box<dyn Stream<Item = Result<PreDownloadVmImageResponse, Status>> + Send>>;

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status: "ok".to_string(),
        }))
    }

    async fn info(&self, _request: Request<InfoRequest>) -> Result<Response<InfoResponse>, Status> {
        Ok(Response::new(InfoResponse::default()))
    }

    async fn list_v_ms(
        &self,
        _request: Request<ListVMsRequest>,
    ) -> Result<Response<ListVMsResponse>, Status> {
        let guard = self.state.lock().await;
        Ok(Response::new(ListVMsResponse {
            vms: guard.vms.values().cloned().collect(),
        }))
    }

    async fn get_vm(&self, request: Request<GetVmRequest>) -> Result<Response<Vm>, Status> {
        let vm_id = request.into_inner().vm_id;
        let guard = self.state.lock().await;
        let vm = guard
            .vms
            .get(&vm_id)
            .cloned()
            .ok_or_else(|| Status::not_found("vm not found"))?;
        Ok(Response::new(vm))
    }

    async fn get_vm_by_session(
        &self,
        request: Request<GetVmBySessionRequest>,
    ) -> Result<Response<Vm>, Status> {
        let session_id = request.into_inner().session_id;
        let guard = self.state.lock().await;
        let vm = guard
            .vms
            .values()
            .find(|vm| {
                vm.metadata
                    .get("chevalier.session_id")
                    .is_some_and(|value| value == &session_id)
            })
            .cloned()
            .ok_or_else(|| Status::not_found("session not found"))?;
        Ok(Response::new(vm))
    }

    async fn create_vm(
        &self,
        request: Request<CreateVmRequest>,
    ) -> Result<Response<Self::CreateVMStream>, Status> {
        let req = request.into_inner();
        let mut guard = self.state.lock().await;
        guard.create_requests.push(req.clone());
        guard.next_vm_id += 1;
        let vm_id = format!("vm-{}", guard.next_vm_id);
        let state = if req.auto_start {
            VmState::Running as i32
        } else {
            VmState::Stopped as i32
        };
        let metadata = req.metadata.map(|m| m.entries).unwrap_or_default();
        let mut vm = self.build_vm(
            vm_id.clone(),
            if req.name.is_empty() {
                format!("session-{vm_id}")
            } else {
                req.name
            },
            state,
            metadata,
        );
        vm.shared_mounts = req.shared_mounts;
        guard.vms.insert(vm_id, vm.clone());

        let stream = tokio_stream::iter(vec![Ok(CreateVmStreamResponse {
            event: Some(create_vm_stream_response::Event::Vm(vm)),
        })]);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn update_vm(&self, _request: Request<UpdateVmRequest>) -> Result<Response<Vm>, Status> {
        Err(Status::unimplemented(
            "update_vm is not required for this test",
        ))
    }

    async fn delete_vm(
        &self,
        request: Request<DeleteVmRequest>,
    ) -> Result<Response<Empty>, Status> {
        let vm_id = request.into_inner().vm_id;
        let mut guard = self.state.lock().await;
        guard.vms.remove(&vm_id);
        guard.delete_calls += 1;
        Ok(Response::new(Empty {}))
    }

    async fn fork_vm(
        &self,
        request: Request<ForkVmRequest>,
    ) -> Result<Response<ForkVmResponse>, Status> {
        let req = request.into_inner();

        let mut guard = self.state.lock().await;
        let parent = guard
            .vms
            .get(&req.parent_vm_id)
            .cloned()
            .ok_or_else(|| Status::not_found("parent vm not found"))?;

        guard.next_vm_id += 1;
        let child_id = format!("vm-{}", guard.next_vm_id);
        let fork_id = format!("fork-{}", child_id);

        let mut child_metadata = parent.metadata.clone();
        if let Some(child_meta) = req.child_metadata {
            child_metadata.extend(child_meta.entries);
        }
        child_metadata.insert("chevalier.fork_id".to_string(), fork_id.clone());
        child_metadata.insert("chevalier.parent_vm_id".to_string(), parent.id.clone());

        let mut child_vm = self.build_vm(
            child_id.clone(),
            if req.child_name.is_empty() {
                format!("{}-fork", parent.name)
            } else {
                req.child_name
            },
            if req.auto_start_child {
                VmState::Running as i32
            } else {
                VmState::Stopped as i32
            },
            child_metadata,
        );
        child_vm.shared_mounts = parent.shared_mounts.clone();

        guard.vms.insert(child_id, child_vm.clone());

        Ok(Response::new(ForkVmResponse {
            parent_vm: Some(parent),
            child_vm: Some(child_vm),
            fork_id,
        }))
    }

    async fn list_durable_volumes(
        &self,
        _request: Request<ListDurableVolumesRequest>,
    ) -> Result<Response<ListDurableVolumesResponse>, Status> {
        let delay = self.state.lock().await.list_durable_volumes_delay;
        if let Some(delay) = delay {
            sleep(delay).await;
        }
        Ok(Response::new(ListDurableVolumesResponse {
            volumes: Vec::new(),
        }))
    }

    async fn delete_durable_volume(
        &self,
        _request: Request<DeleteDurableVolumeRequest>,
    ) -> Result<Response<Empty>, Status> {
        Err(Status::not_found("durable volume not found"))
    }

    async fn start_vm(&self, request: Request<VmActionRequest>) -> Result<Response<Vm>, Status> {
        let vm_id = request.into_inner().vm_id;
        let mut guard = self.state.lock().await;
        let vm = guard
            .vms
            .get_mut(&vm_id)
            .ok_or_else(|| Status::not_found("vm not found"))?;
        vm.state = VmState::Running as i32;
        let vm_clone = vm.clone();
        guard.start_calls += 1;
        Ok(Response::new(vm_clone))
    }

    async fn stop_vm(&self, request: Request<VmActionRequest>) -> Result<Response<Vm>, Status> {
        let vm = self
            .vm_mutate(&request.into_inner().vm_id, VmState::Stopped as i32, true)
            .await?;
        Ok(Response::new(vm))
    }

    async fn restart_vm(&self, request: Request<VmActionRequest>) -> Result<Response<Vm>, Status> {
        let vm = self
            .vm_mutate(&request.into_inner().vm_id, VmState::Running as i32, false)
            .await?;
        Ok(Response::new(vm))
    }

    async fn pause_vm(&self, request: Request<VmActionRequest>) -> Result<Response<Vm>, Status> {
        let vm = self
            .vm_mutate(&request.into_inner().vm_id, VmState::Paused as i32, false)
            .await?;
        Ok(Response::new(vm))
    }

    async fn resume_vm(&self, request: Request<VmActionRequest>) -> Result<Response<Vm>, Status> {
        let vm = self
            .vm_mutate(&request.into_inner().vm_id, VmState::Running as i32, false)
            .await?;
        Ok(Response::new(vm))
    }

    async fn force_stop_vm(
        &self,
        request: Request<VmActionRequest>,
    ) -> Result<Response<Vm>, Status> {
        let vm = self
            .vm_mutate(&request.into_inner().vm_id, VmState::Stopped as i32, true)
            .await?;
        Ok(Response::new(vm))
    }

    async fn list_host_pci_devices(
        &self,
        _request: Request<ListHostPciDevicesRequest>,
    ) -> Result<Response<ListHostPciDevicesResponse>, Status> {
        Ok(Response::new(ListHostPciDevicesResponse {
            enabled: false,
            devices: Vec::new(),
        }))
    }

    async fn attach_pci_device(
        &self,
        _request: Request<AttachPciDeviceRequest>,
    ) -> Result<Response<PciDeviceActionResponse>, Status> {
        Err(Status::failed_precondition("PCI assignment disabled"))
    }

    async fn detach_pci_device(
        &self,
        _request: Request<DetachPciDeviceRequest>,
    ) -> Result<Response<PciDeviceActionResponse>, Status> {
        Err(Status::failed_precondition("PCI assignment disabled"))
    }

    async fn list_snapshots(
        &self,
        request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        let vm_id = request.into_inner().vm_id;
        let guard = self.state.lock().await;
        let snapshots = guard
            .vms
            .get(&vm_id)
            .map(|vm| vm.snapshots.clone())
            .unwrap_or_default();
        Ok(Response::new(ListSnapshotsResponse { snapshots }))
    }

    async fn create_snapshot(
        &self,
        _request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<Snapshot>, Status> {
        Err(Status::unimplemented(
            "create_snapshot is not required for this test",
        ))
    }

    async fn get_snapshot(
        &self,
        _request: Request<GetSnapshotRequest>,
    ) -> Result<Response<Snapshot>, Status> {
        Err(Status::unimplemented(
            "get_snapshot is not required for this test",
        ))
    }

    async fn restore_snapshot(
        &self,
        request: Request<RestoreSnapshotRequest>,
    ) -> Result<Response<Vm>, Status> {
        let req = request.into_inner();
        let mut guard = self.state.lock().await;
        let vm = guard
            .vms
            .get_mut(&req.vm_id)
            .ok_or_else(|| Status::not_found("vm not found"))?;
        if !vm.snapshots.iter().any(|snap| snap.id == req.snapshot_id) {
            return Err(Status::not_found("snapshot not found"));
        }
        // @dive: Snapshot restore is modeled as state recovery before resume/start in continuity tests.
        vm.state = VmState::Stopped as i32;
        let vm_clone = vm.clone();
        guard.restore_calls += 1;
        guard.restored_snapshot_ids.push(req.snapshot_id);
        Ok(Response::new(vm_clone))
    }

    async fn delete_snapshot(
        &self,
        _request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<Empty>, Status> {
        Ok(Response::new(Empty {}))
    }

    async fn pre_download_vm_image(
        &self,
        request: Request<PreDownloadVmImageRequest>,
    ) -> Result<Response<Self::PreDownloadVmImageStream>, Status> {
        let req = request.into_inner();
        let mut guard = self.state.lock().await;
        guard
            .predownload_requests
            .push((req.reference, req.architecture));
        let stream = tokio_stream::iter(Vec::<Result<PreDownloadVmImageResponse, Status>>::new());
        Ok(Response::new(Box::pin(stream)))
    }
}

#[derive(Default)]
struct MockPortProxyState {
    files: HashMap<String, Vec<u8>>,
}

#[derive(Clone)]
struct MockPortProxy {
    state: Arc<Mutex<MockPortProxyState>>,
}

#[tonic::async_trait]
impl PortProxy for MockPortProxy {
    async fn prepare_shutdown(&self, _request: Request<Empty>) -> Result<Response<Empty>, Status> {
        Ok(Response::new(Empty {}))
    }

    async fn read_file(
        &self,
        request: Request<ReadFileRequest>,
    ) -> Result<Response<ReadFileResponse>, Status> {
        let path = request.into_inner().path;
        let guard = self.state.lock().await;
        let data = guard
            .files
            .get(&path)
            .cloned()
            .ok_or_else(|| Status::not_found("path not found"))?;
        Ok(Response::new(ReadFileResponse { data }))
    }

    async fn write_file(
        &self,
        request: Request<WriteFileRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        self.state.lock().await.files.insert(req.path, req.data);
        Ok(Response::new(Empty {}))
    }

    async fn list_directory(
        &self,
        request: Request<ListDirectoryRequest>,
    ) -> Result<Response<ListDirectoryResponse>, Status> {
        let path = request.into_inner().path;
        let prefix = if path.ends_with('/') {
            path
        } else {
            format!("{path}/")
        };

        let guard = self.state.lock().await;
        let mut names = BTreeSet::new();
        for full in guard.files.keys() {
            if full.starts_with(&prefix) {
                let remainder = &full[prefix.len()..];
                if !remainder.is_empty() {
                    let name = remainder.split('/').next().unwrap_or_default();
                    if !name.is_empty() {
                        names.insert(name.to_string());
                    }
                }
            }
        }

        let entries = names
            .into_iter()
            .map(|name| DirectoryEntry {
                name,
                is_dir: false,
                is_symlink: false,
            })
            .collect();

        Ok(Response::new(ListDirectoryResponse { entries }))
    }

    async fn delete_path(
        &self,
        request: Request<DeletePathRequest>,
    ) -> Result<Response<Empty>, Status> {
        let path = request.into_inner().path;
        self.state.lock().await.files.remove(&path);
        Ok(Response::new(Empty {}))
    }
}

#[derive(Default)]
struct MockShellExecState {
    command_invocations: HashMap<String, usize>,
}

#[derive(Clone)]
struct MockShellExec {
    state: Arc<Mutex<MockShellExecState>>,
}

#[tonic::async_trait]
impl ShellExec for MockShellExec {
    type ExecStream = Pin<Box<dyn Stream<Item = Result<ExecResponse, Status>> + Send>>;
    type InteractiveShellStream =
        Pin<Box<dyn Stream<Item = Result<InteractiveShellResponse, Status>> + Send>>;

    async fn exec(
        &self,
        request: Request<tonic::Streaming<ExecRequest>>,
    ) -> Result<Response<Self::ExecStream>, Status> {
        let mut inbound = request.into_inner();
        let start = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("missing exec start"))?;
        let start = match start.request {
            Some(exec_request::Request::Start(start)) => start,
            _ => return Err(Status::invalid_argument("expected exec start frame")),
        };

        let command = start.args.last().cloned().unwrap_or_default();
        {
            let mut guard = self.state.lock().await;
            *guard
                .command_invocations
                .entry(command.clone())
                .or_insert(0) += 1;
        }

        let mut frames = vec![
            Ok(ExecResponse {
                response: Some(exec_response::Response::StdoutData(
                    b"stdout:ready".to_vec(),
                )),
            }),
            Ok(ExecResponse {
                response: Some(exec_response::Response::StderrData(
                    b"stderr:ready".to_vec(),
                )),
            }),
        ];

        let exit_code = if command.contains("timeout") { 124 } else { 0 };
        frames.push(Ok(ExecResponse {
            response: Some(exec_response::Response::ExitCode(exit_code)),
        }));

        Ok(Response::new(Box::pin(tokio_stream::iter(frames))))
    }

    async fn interactive_shell(
        &self,
        request: Request<tonic::Streaming<InteractiveShellRequest>>,
    ) -> Result<Response<Self::InteractiveShellStream>, Status> {
        let mut inbound = request.into_inner();
        let start = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("missing shell start"))?;
        let _ = match start.request {
            Some(interactive_shell_request::Request::Start(start)) => start,
            _ => return Err(Status::invalid_argument("expected shell start frame")),
        };

        let frames = vec![
            Ok(InteractiveShellResponse {
                response: Some(interactive_shell_response::Response::OutputData(
                    b"shell:ready".to_vec(),
                )),
            }),
            Ok(InteractiveShellResponse {
                response: Some(interactive_shell_response::Response::ExitCode(0)),
            }),
        ];

        Ok(Response::new(Box::pin(tokio_stream::iter(frames))))
    }
}

struct TestHarness {
    vmd_endpoint: String,
    portproxy_port: i32,
    vmd_state: Arc<Mutex<MockVmdState>>,
    _portproxy_state: Arc<Mutex<MockPortProxyState>>,
    shell_exec_state: Arc<Mutex<MockShellExecState>>,
    vmd_shutdown: Option<oneshot::Sender<()>>,
    portproxy_shutdown: Option<oneshot::Sender<()>>,
    vmd_join: tokio::task::JoinHandle<()>,
    portproxy_join: tokio::task::JoinHandle<()>,
}

impl TestHarness {
    async fn start() -> Self {
        let portproxy_state = Arc::new(Mutex::new(MockPortProxyState::default()));
        let shell_exec_state = Arc::new(Mutex::new(MockShellExecState::default()));
        let vmd_state = Arc::new(Mutex::new(MockVmdState::default()));

        let portproxy_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock portproxy listener");
        let portproxy_addr = portproxy_listener
            .local_addr()
            .expect("get mock portproxy listener addr");
        let (portproxy_shutdown_tx, portproxy_shutdown_rx) = oneshot::channel::<()>();

        let portproxy_server = MockPortProxy {
            state: Arc::clone(&portproxy_state),
        };
        let shell_exec_server = MockShellExec {
            state: Arc::clone(&shell_exec_state),
        };

        let portproxy_join = tokio::spawn(async move {
            Server::builder()
                .add_service(PortProxyServer::new(portproxy_server))
                .add_service(ShellExecServer::new(shell_exec_server))
                .serve_with_incoming_shutdown(TcpListenerStream::new(portproxy_listener), async {
                    let _ = portproxy_shutdown_rx.await;
                })
                .await
                .expect("serve mock portproxy/shell grpc");
        });

        let vmd_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock vmd listener");
        let vmd_addr = vmd_listener
            .local_addr()
            .expect("get mock vmd listener addr");
        let (vmd_shutdown_tx, vmd_shutdown_rx) = oneshot::channel::<()>();

        let vmd_server = MockVmd {
            state: Arc::clone(&vmd_state),
            rpc_port: portproxy_addr.port() as i32,
            proxy_port: portproxy_addr.port() as i32,
        };

        let vmd_join = tokio::spawn(async move {
            Server::builder()
                .add_service(VmdServiceServer::new(vmd_server))
                .serve_with_incoming_shutdown(TcpListenerStream::new(vmd_listener), async {
                    let _ = vmd_shutdown_rx.await;
                })
                .await
                .expect("serve mock vmd grpc");
        });

        let vmd_endpoint = format!("http://{}", vmd_addr);

        for _ in 0..40 {
            let mut client =
                match chevalier_sandbox::proto::vmd::v1::vmd_service_client::VmdServiceClient::connect(
                    vmd_endpoint.clone(),
                )
                .await
                {
                    Ok(client) => client,
                    Err(_) => {
                        sleep(Duration::from_millis(25)).await;
                        continue;
                    }
                };

            if client
                .health(Request::new(HealthRequest::default()))
                .await
                .is_ok()
            {
                return Self {
                    vmd_endpoint,
                    portproxy_port: portproxy_addr.port() as i32,
                    vmd_state,
                    _portproxy_state: portproxy_state,
                    shell_exec_state,
                    vmd_shutdown: Some(vmd_shutdown_tx),
                    portproxy_shutdown: Some(portproxy_shutdown_tx),
                    vmd_join,
                    portproxy_join,
                };
            }
            sleep(Duration::from_millis(25)).await;
        }

        panic!("mock harness failed to become ready");
    }

    async fn shutdown(mut self) {
        if let Some(tx) = self.vmd_shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(tx) = self.portproxy_shutdown.take() {
            let _ = tx.send(());
        }
        let _ = self.vmd_join.await;
        let _ = self.portproxy_join.await;
    }
}

fn sandbox_config() -> SandboxConfig {
    SandboxConfig {
        auto_spawn: false,
        default_image: "ghcr.io/bracketdevelopers/uv-builder:main".to_string(),
        connect_timeout: Duration::from_secs(2),
        daemon_start_timeout: Duration::from_secs(2),
        portproxy_ready_timeout: Duration::from_millis(700),
        ..SandboxConfig::default()
    }
}

async fn wait_for_port_open(port: u16) {
    for _ in 0..120 {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return;
        }
        sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for port to open: {port}");
}

async fn wait_for_port_closed(port: u16) {
    for _ in 0..120 {
        if TcpStream::connect(("127.0.0.1", port)).await.is_err() {
            return;
        }
        sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for port to close: {port}");
}

#[tokio::test(flavor = "multi_thread")]
async fn disabled_pci_inventory_is_backward_compatible_without_a_capability_token() {
    let harness = TestHarness::start().await;
    let sandbox = Sandbox::connect(harness.vmd_endpoint.clone(), sandbox_config())
        .await
        .expect("connect sandbox facade to mock vmd");

    let inventory = sandbox
        .list_host_pci_devices()
        .await
        .expect("disabled inventory should not require a PCI token");
    assert!(!inventory.enabled);
    assert!(inventory.devices.is_empty());

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn durable_volume_inventory_has_a_short_rpc_deadline() {
    let harness = TestHarness::start().await;
    harness.vmd_state.lock().await.list_durable_volumes_delay = Some(Duration::from_secs(30));
    let mut config = sandbox_config();
    config.connect_timeout = Duration::from_secs(30);
    let sandbox = Sandbox::connect(harness.vmd_endpoint.clone(), config)
        .await
        .expect("connect sandbox facade to mock vmd");

    let started = Instant::now();
    let error = timeout(Duration::from_secs(4), sandbox.list_durable_volumes())
        .await
        .expect("durable inventory must honor its own deadline")
        .expect_err("delayed durable inventory should time out");
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "durable inventory exceeded its short deadline"
    );
    match error {
        SandboxError::Grpc(status) => {
            assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
        }
        other => panic!("expected gRPC deadline error, got {other:?}"),
    }

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn session_reuse_and_fork_lineage_contract() {
    let harness = TestHarness::start().await;

    let sandbox = Sandbox::connect(harness.vmd_endpoint.clone(), sandbox_config())
        .await
        .expect("connect sandbox facade to mock vmd");

    let parent = sandbox
        .session(SessionOptions {
            session_id: Some("session-parent".to_string()),
            auto_start: false,
            ..SessionOptions::default()
        })
        .await
        .expect("create parent session");

    let reused = sandbox
        .session(SessionOptions {
            session_id: Some("session-parent".to_string()),
            ..SessionOptions::default()
        })
        .await
        .expect("reuse existing session by id");

    assert_eq!(parent.vm_id(), reused.vm_id());

    let detached = sandbox
        .attach_session("session-parent")
        .await
        .expect("attach by durable session id");
    detached.close().await.expect("close detaches handle only");

    let fork_result = reused
        .fork(ForkOptions::default())
        .await
        .expect("fork should create child session");

    assert_eq!(fork_result.parent_session_id, "session-parent");
    assert_ne!(fork_result.child_session_id, "session-parent");
    assert!(!fork_result.fork_id.is_empty());

    let sessions = sandbox.list_sessions().await.expect("list sessions");
    assert!(sessions.iter().any(|s| s.session_id == "session-parent"));

    let child = sessions
        .iter()
        .find(|s| s.session_id == fork_result.child_session_id)
        .expect("child session should be discoverable");
    assert_eq!(child.parent_session_id.as_deref(), Some("session-parent"));
    assert_eq!(child.fork_id.as_deref(), Some(fork_result.fork_id.as_str()));

    parent
        .discard()
        .await
        .expect("discard should delete parent resources");

    match sandbox.attach_session("session-parent").await {
        Err(SandboxError::SessionNotFound(_)) => {}
        _ => panic!("expected SessionNotFound after discard"),
    }

    fork_result
        .child
        .discard()
        .await
        .expect("child session should discard independently");

    let guard = harness.vmd_state.lock().await;
    assert!(
        guard.stop_calls >= 2,
        "discard should stop VMs before delete"
    );
    assert!(
        guard.delete_calls >= 2,
        "parent and child should both delete"
    );
    drop(guard);

    timeout(Duration::from_secs(3), harness.shutdown())
        .await
        .expect("mock harness shutdown timed out");
}

#[tokio::test(flavor = "multi_thread")]
async fn session_create_forwards_shared_mounts_contract() {
    let harness = TestHarness::start().await;

    let sandbox = Sandbox::connect(harness.vmd_endpoint.clone(), sandbox_config())
        .await
        .expect("connect sandbox facade to mock vmd");

    sandbox
        .session(SessionOptions {
            session_id: Some("session-mounts".to_string()),
            auto_start: false,
            storage_profile: "durable-data".to_string(),
            volume_owner_key: Some("workspace:test:thread:session-mounts".to_string()),
            volume_size_gb: Some(64),
            shared_mounts: vec![SharedMount {
                host_path: "/tmp/runtimefs".to_string(),
                guest_path: "/workspace".to_string(),
                mount_tag: "runtimefs".to_string(),
                read_only: false,
                availability: SharedMountAvailability::SharedStorage,
                continuity: SharedMountContinuity::RestoreCrossNode,
                backend_profile: "shared-posix".to_string(),
                vfs_endpoint: String::new(),
                vfs_scope_path: String::new(),
            }],
            ..SessionOptions::default()
        })
        .await
        .expect("create session with shared mounts");

    let guard = harness.vmd_state.lock().await;
    let req = guard
        .create_requests
        .last()
        .expect("expected create request to be recorded");
    assert_eq!(req.shared_mounts.len(), 1);
    assert_eq!(req.shared_mounts[0].host_path, "/tmp/runtimefs");
    assert_eq!(req.shared_mounts[0].guest_path, "/workspace");
    assert_eq!(req.shared_mounts[0].mount_tag, "runtimefs");
    assert!(!req.shared_mounts[0].read_only);
    assert_eq!(
        req.shared_mounts[0].availability,
        chevalier_sandbox::proto::vmd::v1::SharedMountAvailability::SharedStorage as i32
    );
    assert_eq!(
        req.shared_mounts[0].continuity,
        chevalier_sandbox::proto::vmd::v1::SharedMountContinuity::RestoreCrossNode as i32
    );
    assert_eq!(req.shared_mounts[0].backend_profile, "shared-posix");
    assert_eq!(req.storage_profile, "durable-data");
    assert_eq!(req.volume_owner_key, "workspace:test:thread:session-mounts");
    assert_eq!(req.volume_size_gb, 64);
}

#[tokio::test(flavor = "multi_thread")]
async fn tier_b_session_rejects_node_local_shared_mounts() {
    let harness = TestHarness::start().await;

    let sandbox = Sandbox::connect(harness.vmd_endpoint.clone(), sandbox_config())
        .await
        .expect("connect sandbox facade to mock vmd");

    let mut metadata = HashMap::new();
    metadata.insert("tier_b_eligible".to_string(), "true".to_string());

    let err = sandbox
        .session(SessionOptions {
            session_id: Some("session-tier-b-node-local".to_string()),
            auto_start: false,
            metadata,
            shared_mounts: vec![SharedMount {
                host_path: "/tmp/local-mount".to_string(),
                guest_path: "/workspace".to_string(),
                mount_tag: "workspace".to_string(),
                read_only: false,
                availability: SharedMountAvailability::NodeLocal,
                continuity: SharedMountContinuity::RestartSameNode,
                backend_profile: String::new(),
                vfs_endpoint: String::new(),
                vfs_scope_path: String::new(),
            }],
            ..SessionOptions::default()
        })
        .await;

    let err = match err {
        Ok(_) => panic!("tier-b session should reject node-local mounts"),
        Err(err) => err,
    };

    match err {
        SandboxError::InvalidResponse(message) => {
            assert!(message.contains("tier_b_eligible"));
            assert!(message.contains("workspace"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn tier_b_session_accepts_cross_node_shared_mounts() {
    let harness = TestHarness::start().await;

    let sandbox = Sandbox::connect(harness.vmd_endpoint.clone(), sandbox_config())
        .await
        .expect("connect sandbox facade to mock vmd");

    let mut metadata = HashMap::new();
    metadata.insert("tier_b_eligible".to_string(), "true".to_string());

    sandbox
        .session(SessionOptions {
            session_id: Some("session-tier-b-shared".to_string()),
            auto_start: false,
            metadata,
            shared_mounts: vec![SharedMount {
                host_path: "/mnt/shared/workspace".to_string(),
                guest_path: "/workspace".to_string(),
                mount_tag: "workspace".to_string(),
                read_only: false,
                availability: SharedMountAvailability::SharedStorage,
                continuity: SharedMountContinuity::RestoreCrossNode,
                backend_profile: "shared-posix".to_string(),
                vfs_endpoint: String::new(),
                vfs_scope_path: String::new(),
            }],
            ..SessionOptions::default()
        })
        .await
        .expect("tier-b session should accept shared-storage mounts");
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_storage_mount_requires_backend_profile() {
    let harness = TestHarness::start().await;

    let sandbox = Sandbox::connect(harness.vmd_endpoint.clone(), sandbox_config())
        .await
        .expect("connect sandbox facade to mock vmd");

    let result = sandbox
        .session(SessionOptions {
            session_id: Some("session-shared-missing-profile".to_string()),
            auto_start: false,
            shared_mounts: vec![SharedMount {
                host_path: "/mnt/shared/workspace".to_string(),
                guest_path: "/workspace".to_string(),
                mount_tag: "workspace".to_string(),
                read_only: false,
                availability: SharedMountAvailability::SharedStorage,
                continuity: SharedMountContinuity::RestoreCrossNode,
                backend_profile: String::new(),
                vfs_endpoint: String::new(),
                vfs_scope_path: String::new(),
            }],
            ..SessionOptions::default()
        })
        .await;

    match result {
        Ok(_) => panic!("shared-storage mount should require backend_profile"),
        Err(SandboxError::InvalidResponse(message)) => {
            assert!(message.contains("backend_profile"));
            assert!(message.contains("workspace"));
        }
        Err(other) => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn bidi_exec_shell_and_file_contract() {
    let harness = TestHarness::start().await;

    let sandbox = Sandbox::connect(harness.vmd_endpoint.clone(), sandbox_config())
        .await
        .expect("connect sandbox facade to mock vmd");

    let session = sandbox
        .session(SessionOptions {
            session_id: Some("session-io".to_string()),
            auto_start: true,
            ..SessionOptions::default()
        })
        .await
        .expect("create io session");

    session
        .write_file("/tmp/alpha.txt", b"abc123".to_vec())
        .await
        .expect("write file");

    let content = session
        .read_file("/tmp/alpha.txt")
        .await
        .expect("read file");
    assert_eq!(content, b"abc123".to_vec());

    let entries = session.list_dir("/tmp").await.expect("list dir");
    assert!(entries.iter().any(|e| e.name == "alpha.txt"));

    session
        .delete_path("/tmp/alpha.txt")
        .await
        .expect("delete file");
    assert!(session.read_file("/tmp/alpha.txt").await.is_err());

    let exec = session
        .exec("trigger-timeout", ExecOptions::default())
        .await
        .expect("start exec");

    exec.input
        .send(ExecInput::Data(b"hello".to_vec()))
        .await
        .expect("send exec stdin");
    exec.input
        .send(ExecInput::Eof)
        .await
        .expect("send exec eof");
    drop(exec.input);

    let mut exec_events = exec.events;
    let mut observed_exec = Vec::new();
    for _ in 0..16 {
        let event = timeout(Duration::from_secs(3), exec_events.next())
            .await
            .expect("timed out waiting for exec event")
            .expect("exec stream ended before exit");
        match event.expect("exec event should decode") {
            ExecEvent::Stdout(bytes) => {
                observed_exec.push(format!("stdout:{}", String::from_utf8_lossy(&bytes)));
            }
            ExecEvent::Stderr(bytes) => {
                observed_exec.push(format!("stderr:{}", String::from_utf8_lossy(&bytes)));
            }
            ExecEvent::Timeout => observed_exec.push("timeout".to_string()),
            ExecEvent::Exit(code) => {
                observed_exec.push(format!("exit:{code}"));
                break;
            }
        }
    }

    assert_eq!(
        observed_exec,
        vec![
            "stdout:stdout:ready",
            "stderr:stderr:ready",
            "timeout",
            "exit:124",
        ]
    );

    let shell = session
        .shell(Default::default())
        .await
        .expect("start interactive shell");
    shell
        .input
        .send(ShellInput::Data(b"world".to_vec()))
        .await
        .expect("send shell stdin");
    shell
        .input
        .send(ShellInput::Eof)
        .await
        .expect("send shell eof");
    drop(shell.input);

    let mut shell_events = shell.events;
    let mut observed_shell = Vec::new();
    for _ in 0..16 {
        let event = timeout(Duration::from_secs(3), shell_events.next())
            .await
            .expect("timed out waiting for shell event")
            .expect("shell stream ended before exit");
        match event.expect("shell event should decode") {
            ShellEvent::Output(bytes) => {
                observed_shell.push(format!("output:{}", String::from_utf8_lossy(&bytes)));
            }
            ShellEvent::Exit(code) => {
                observed_shell.push(format!("exit:{code}"));
                break;
            }
        }
    }

    assert_eq!(observed_shell, vec!["output:shell:ready", "exit:0"]);

    session.discard().await.expect("cleanup io session");
    timeout(Duration::from_secs(3), harness.shutdown())
        .await
        .expect("mock harness shutdown timed out");
}

#[tokio::test(flavor = "multi_thread")]
async fn control_gateway_failover_prefers_healthy_secondary_endpoint() {
    let harness = TestHarness::start().await;

    let mut cfg = sandbox_config();
    cfg.endpoint = "http://127.0.0.1:65534".to_string();
    cfg.control_gateway_endpoints = vec![harness.vmd_endpoint.clone()];

    let sandbox = Sandbox::new(cfg)
        .await
        .expect("sandbox should fail over to healthy secondary control endpoint");

    let session = sandbox
        .session(SessionOptions {
            session_id: Some("gateway-failover-session".to_string()),
            auto_start: false,
            ..SessionOptions::default()
        })
        .await
        .expect("session creation should use healthy secondary endpoint");

    let sessions = sandbox
        .list_sessions()
        .await
        .expect("list sessions after failover");
    assert!(
        sessions
            .iter()
            .any(|entry| entry.session_id == "gateway-failover-session"),
        "session created through secondary endpoint should be discoverable"
    );

    session.discard().await.expect("discard failover session");
    timeout(Duration::from_secs(3), harness.shutdown())
        .await
        .expect("mock harness shutdown timed out");
}

#[tokio::test(flavor = "multi_thread")]
async fn attach_starts_stopped_vm_and_sessions_are_isolated() {
    let harness = TestHarness::start().await;

    let sandbox = Sandbox::connect(harness.vmd_endpoint.clone(), sandbox_config())
        .await
        .expect("connect sandbox facade to mock vmd");

    let stopped = sandbox
        .session(SessionOptions {
            session_id: Some("session-stopped".to_string()),
            auto_start: false,
            ..SessionOptions::default()
        })
        .await
        .expect("create stopped session");
    let running = sandbox
        .session(SessionOptions {
            session_id: Some("session-running".to_string()),
            auto_start: true,
            ..SessionOptions::default()
        })
        .await
        .expect("create running session");

    assert_ne!(stopped.vm_id(), running.vm_id());

    let attached = sandbox
        .attach_session("session-stopped")
        .await
        .expect("attach should start stopped vm");
    assert_eq!(attached.session_id(), "session-stopped");
    assert_eq!(attached.vm_id(), stopped.vm_id());

    running
        .discard()
        .await
        .expect("discard one session should not remove other");

    let retained = sandbox
        .attach_session("session-stopped")
        .await
        .expect("other session should remain attachable");
    assert_eq!(retained.vm_id(), stopped.vm_id());
    retained.discard().await.expect("discard retained session");

    let guard = harness.vmd_state.lock().await;
    assert!(
        guard.start_calls >= 1,
        "attach_session should issue start for stopped VMs"
    );
    drop(guard);

    timeout(Duration::from_secs(3), harness.shutdown())
        .await
        .expect("mock harness shutdown timed out");
}

#[tokio::test(flavor = "multi_thread")]
async fn tier_b_eligibility_classifier_sets_metadata_policy() {
    let harness = TestHarness::start().await;
    let sandbox = Sandbox::connect(harness.vmd_endpoint.clone(), sandbox_config())
        .await
        .expect("connect sandbox facade to mock vmd");

    let default_session = sandbox
        .session(SessionOptions {
            session_id: Some("session-tier-b-default".to_string()),
            auto_start: false,
            ..SessionOptions::default()
        })
        .await
        .expect("create default tier-b eligible session");

    let mut metadata = HashMap::new();
    metadata.insert("tier_b_eligible".to_string(), "false".to_string());
    let ineligible_session = sandbox
        .session(SessionOptions {
            session_id: Some("session-tier-b-ineligible".to_string()),
            metadata,
            auto_start: false,
            ..SessionOptions::default()
        })
        .await
        .expect("create tier-b ineligible session");

    let guard = harness.vmd_state.lock().await;
    let default_vm = guard
        .vms
        .get(default_session.vm_id())
        .expect("default session vm should exist");
    assert_eq!(
        default_vm
            .metadata
            .get("chevalier.tier_b_eligible")
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        default_vm
            .metadata
            .get("chevalier.execution_fidelity_requirement")
            .map(String::as_str),
        Some("disk+memory")
    );

    let ineligible_vm = guard
        .vms
        .get(ineligible_session.vm_id())
        .expect("ineligible session vm should exist");
    assert_eq!(
        ineligible_vm
            .metadata
            .get("chevalier.tier_b_eligible")
            .map(String::as_str),
        Some("false")
    );
    assert_eq!(
        ineligible_vm
            .metadata
            .get("chevalier.execution_fidelity_requirement")
            .map(String::as_str),
        Some("best-effort")
    );
    drop(guard);

    default_session
        .discard()
        .await
        .expect("discard default eligibility session");
    ineligible_session
        .discard()
        .await
        .expect("discard ineligible session");

    timeout(Duration::from_secs(3), harness.shutdown())
        .await
        .expect("mock harness shutdown timed out");
}

#[tokio::test(flavor = "multi_thread")]
async fn tier_b_eligible_failover_requires_restore_snapshot_marker() {
    let mut primary = TestHarness::start().await;
    let secondary = TestHarness::start().await;
    wait_for_port_open(primary.portproxy_port as u16).await;
    wait_for_port_open(secondary.portproxy_port as u16).await;

    let mut cfg = sandbox_config();
    cfg.control_gateway_endpoints = vec![secondary.vmd_endpoint.clone()];
    let sandbox = Sandbox::connect(primary.vmd_endpoint.clone(), cfg)
        .await
        .expect("connect sandbox facade with primary and secondary endpoints");

    let session = sandbox
        .session(SessionOptions {
            session_id: Some("session-tier-b-restore-required".to_string()),
            auto_start: true,
            ..SessionOptions::default()
        })
        .await
        .expect("create tier-b eligible session");

    let vm_id = session.vm_id().to_string();
    let vm_clone = {
        let guard = primary.vmd_state.lock().await;
        guard
            .vms
            .get(&vm_id)
            .cloned()
            .expect("vm should exist on primary")
    };
    let mut vm_clone = vm_clone;
    vm_clone
        .metadata
        .remove("chevalier.execution_restore_snapshot_id");
    vm_clone
        .metadata
        .remove("chevalier.execution_restore_snapshot_name");
    vm_clone.metadata.remove("chevalier.fork_snapshot");
    vm_clone.snapshots.clear();
    if let Some(network) = vm_clone.network.as_mut() {
        network.portproxy_ports = Some(PortProxyPorts {
            proxy_port: secondary.portproxy_port,
            rpc_port: secondary.portproxy_port,
        });
    }
    {
        let mut guard = secondary.vmd_state.lock().await;
        guard.vms.insert(vm_id.clone(), vm_clone);
    }

    if let Some(tx) = primary.portproxy_shutdown.take() {
        let _ = tx.send(());
    }
    wait_for_port_closed(primary.portproxy_port as u16).await;

    match session
        .exec("echo should-fail", ExecOptions::default())
        .await
    {
        Err(SandboxError::InvalidResponse(message)) => {
            assert!(
                message.contains("tier_b_eligible"),
                "expected tier-b eligibility policy violation, got: {message}"
            );
        }
        _ => panic!("expected tier-b restore policy failure"),
    }

    session
        .discard()
        .await
        .expect("discard tier-b restore policy session");

    timeout(Duration::from_secs(3), primary.shutdown())
        .await
        .expect("primary harness shutdown timed out");
    timeout(Duration::from_secs(3), secondary.shutdown())
        .await
        .expect("secondary harness shutdown timed out");
}

#[tokio::test(flavor = "multi_thread")]
async fn warm_pool_prewarms_profiles_by_architecture() {
    let harness = TestHarness::start().await;
    let mut cfg = sandbox_config();
    cfg.warm_pool_profiles = vec![
        WarmPoolProfile {
            image: "ghcr.io/bracketdevelopers/uv-builder:main".to_string(),
            architecture: Some("amd64".to_string()),
            min_inventory: 1,
        },
        WarmPoolProfile {
            image: "ghcr.io/bracketdevelopers/uv-builder:main".to_string(),
            architecture: Some("arm64".to_string()),
            min_inventory: 1,
        },
    ];
    let _sandbox = Sandbox::connect(harness.vmd_endpoint.clone(), cfg)
        .await
        .expect("connect sandbox and prewarm profiles");

    let guard = harness.vmd_state.lock().await;
    assert!(
        guard.predownload_requests.iter().any(|(reference, arch)| {
            reference == "ghcr.io/bracketdevelopers/uv-builder:main" && arch == "amd64"
        }),
        "warm pool should request predownload for amd64 profile"
    );
    assert!(
        guard.predownload_requests.iter().any(|(reference, arch)| {
            reference == "ghcr.io/bracketdevelopers/uv-builder:main" && arch == "arm64"
        }),
        "warm pool should request predownload for arm64 profile"
    );
    drop(guard);

    timeout(Duration::from_secs(3), harness.shutdown())
        .await
        .expect("mock harness shutdown timed out");
}

#[tokio::test(flavor = "multi_thread")]
async fn continuity_rebinds_session_after_primary_vmd_loss() {
    let mut primary = TestHarness::start().await;
    let secondary = TestHarness::start().await;
    wait_for_port_open(primary.portproxy_port as u16).await;
    wait_for_port_open(secondary.portproxy_port as u16).await;

    let mut cfg = sandbox_config();
    cfg.control_gateway_endpoints = vec![secondary.vmd_endpoint.clone()];
    let sandbox = Sandbox::connect(primary.vmd_endpoint.clone(), cfg)
        .await
        .expect("connect sandbox facade with primary and secondary endpoints");

    let _session = sandbox
        .session(SessionOptions {
            session_id: Some("session-continuity".to_string()),
            auto_start: true,
            ..SessionOptions::default()
        })
        .await
        .expect("create continuity session");

    let vm_id = _session.vm_id().to_string();
    let vm_clone = {
        let guard = primary.vmd_state.lock().await;
        guard
            .vms
            .get(&vm_id)
            .cloned()
            .expect("vm should exist on primary")
    };
    let mut vm_clone = vm_clone;
    vm_clone.metadata.insert(
        "chevalier.execution_restore_snapshot_name".to_string(),
        "continuity-snapshot".to_string(),
    );
    vm_clone.snapshots = vec![Snapshot {
        id: "snap-continuity-1".to_string(),
        name: "continuity-snapshot".to_string(),
        label: "continuity".to_string(),
        description: "test continuity restore".to_string(),
        created_at: None,
    }];
    if let Some(network) = vm_clone.network.as_mut() {
        network.portproxy_ports = Some(PortProxyPorts {
            proxy_port: secondary.portproxy_port,
            rpc_port: secondary.portproxy_port,
        });
    }
    {
        let mut guard = secondary.vmd_state.lock().await;
        guard.vms.insert(vm_id.clone(), vm_clone);
    }

    if let Some(tx) = primary.vmd_shutdown.take() {
        let _ = tx.send(());
    }
    sleep(Duration::from_millis(120)).await;

    let rebound = sandbox
        .attach_session("session-continuity")
        .await
        .expect("attach should recover session via secondary endpoint");

    let exec = rebound
        .exec("echo continuity", ExecOptions::default())
        .await
        .expect("exec should run from rebound session");
    drop(exec.input);

    let mut events = exec.events;
    let mut saw_exit_zero = false;
    loop {
        let next = timeout(Duration::from_secs(3), events.next())
            .await
            .expect("timed out waiting for continuity exec event");
        let Some(event) = next else {
            break;
        };
        match event.expect("decode continuity exec event") {
            ExecEvent::Exit(code) => {
                saw_exit_zero = code == 0;
                break;
            }
            ExecEvent::Stdout(_) | ExecEvent::Stderr(_) | ExecEvent::Timeout => {}
        }
    }
    assert!(
        saw_exit_zero,
        "exec stream should finish with exit=0 after endpoint rebinding"
    );
    let secondary_guard = secondary.vmd_state.lock().await;
    assert!(
        secondary_guard.restore_calls >= 1,
        "continuity handoff should invoke execution-state restore on secondary"
    );
    assert!(
        secondary_guard
            .restored_snapshot_ids
            .iter()
            .any(|id| id == "snap-continuity-1"),
        "restore path should target continuity snapshot id"
    );
    drop(secondary_guard);

    rebound.discard().await.expect("discard continuity session");

    timeout(Duration::from_secs(3), primary.shutdown())
        .await
        .expect("primary harness shutdown timed out");
    timeout(Duration::from_secs(3), secondary.shutdown())
        .await
        .expect("secondary harness shutdown timed out");
}

#[tokio::test(flavor = "multi_thread")]
async fn inflight_exec_rebinds_on_rpc_loss_and_runs_exactly_once() {
    let mut primary = TestHarness::start().await;
    let secondary = TestHarness::start().await;
    wait_for_port_open(primary.portproxy_port as u16).await;
    wait_for_port_open(secondary.portproxy_port as u16).await;

    let mut cfg = sandbox_config();
    cfg.control_gateway_endpoints = vec![secondary.vmd_endpoint.clone()];
    let sandbox = Sandbox::connect(primary.vmd_endpoint.clone(), cfg)
        .await
        .expect("connect sandbox facade with primary and secondary endpoints");

    let session = sandbox
        .session(SessionOptions {
            session_id: Some("session-exactly-once".to_string()),
            auto_start: true,
            ..SessionOptions::default()
        })
        .await
        .expect("create session on primary");

    let vm_id = session.vm_id().to_string();
    let vm_clone = {
        let guard = primary.vmd_state.lock().await;
        guard
            .vms
            .get(&vm_id)
            .cloned()
            .expect("vm should exist on primary")
    };
    let mut vm_clone = vm_clone;
    vm_clone.metadata.insert(
        "chevalier.execution_restore_snapshot_name".to_string(),
        "exactly-once-snapshot".to_string(),
    );
    vm_clone.snapshots = vec![Snapshot {
        id: "snap-exactly-once-1".to_string(),
        name: "exactly-once-snapshot".to_string(),
        label: "exactly-once".to_string(),
        description: "test exactly-once failover restore".to_string(),
        created_at: None,
    }];
    if let Some(network) = vm_clone.network.as_mut() {
        network.portproxy_ports = Some(PortProxyPorts {
            proxy_port: secondary.portproxy_port,
            rpc_port: secondary.portproxy_port,
        });
    }
    {
        let mut guard = secondary.vmd_state.lock().await;
        guard.vms.insert(vm_id.clone(), vm_clone);
    }

    if let Some(tx) = primary.portproxy_shutdown.take() {
        let _ = tx.send(());
    }
    wait_for_port_closed(primary.portproxy_port as u16).await;

    let command = "echo exactly-once-marker";
    let exec = session
        .exec(command, ExecOptions::default())
        .await
        .expect("exec should recover by rebinding endpoint");
    drop(exec.input);

    let mut saw_exit_zero = false;
    let mut events = exec.events;
    loop {
        let next = timeout(Duration::from_secs(3), events.next())
            .await
            .expect("timed out waiting for exec event");
        let Some(event) = next else {
            break;
        };
        match event.expect("decode exec event") {
            ExecEvent::Exit(code) => {
                saw_exit_zero = code == 0;
                break;
            }
            ExecEvent::Stdout(_) | ExecEvent::Stderr(_) | ExecEvent::Timeout => {}
        }
    }
    assert!(
        saw_exit_zero,
        "rebound exec should complete successfully after rpc endpoint loss"
    );

    let primary_count = {
        let guard = primary.shell_exec_state.lock().await;
        guard.command_invocations.get(command).copied().unwrap_or(0)
    };
    let secondary_count = {
        let guard = secondary.shell_exec_state.lock().await;
        guard.command_invocations.get(command).copied().unwrap_or(0)
    };
    // @dive: Exactly-once verification asserts command dispatch did not duplicate across primary->secondary failover.
    assert_eq!(
        primary_count + secondary_count,
        1,
        "command should be dispatched exactly once across failover"
    );
    assert_eq!(
        secondary_count, 1,
        "rebinding should land command execution on surviving secondary node"
    );

    session
        .discard()
        .await
        .expect("discard exactly-once session");

    timeout(Duration::from_secs(3), primary.shutdown())
        .await
        .expect("primary harness shutdown timed out");
    timeout(Duration::from_secs(3), secondary.shutdown())
        .await
        .expect("secondary harness shutdown timed out");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(unix)]
async fn forward_port_handle_releases_multiplexer_binding() {
    let harness = TestHarness::start().await;
    let sandbox = Sandbox::connect(harness.vmd_endpoint.clone(), sandbox_config())
        .await
        .expect("connect sandbox facade to mock vmd");

    let session = sandbox
        .session(SessionOptions {
            session_id: Some("session-forward".to_string()),
            auto_start: true,
            ..SessionOptions::default()
        })
        .await
        .expect("create forwarding session");

    for index in 0..3 {
        let handle = session
            .forward_port(8080)
            .await
            .expect("start forward handle");
        let host_port = handle.host_port;
        wait_for_port_open(host_port).await;

        if index % 2 == 0 {
            handle.close().await.expect("close forward handle");
        } else {
            drop(handle);
        }
        wait_for_port_closed(host_port).await;
    }

    session.discard().await.expect("discard forwarding session");
    timeout(Duration::from_secs(3), harness.shutdown())
        .await
        .expect("mock harness shutdown timed out");
}
