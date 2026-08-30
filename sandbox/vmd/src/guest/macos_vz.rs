use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::time::sleep;
use uuid::Uuid;

use crate::state::types::{ResourceSpec, SharedMountSpec};

const PROTOCOL_VERSION: u32 = 1;
const REQUEST_SCHEMA_VERSION: u32 = 1;
const MAX_FRAME_BYTES: usize = 64 * 1024;
const CONTROL_VSOCK_PORT: u32 = 13_338;
const VFS_VSOCK_PORT: u32 = 13_339;
const HELPER_ENV: &str = "CHEVALIER_SANDBOX_VZ_HELPER_BIN";
const LEGACY_HELPER_ENV: &str = "BRACKET_SANDBOX_VZ_HELPER_BIN";
pub const RUNTIME_CONFIG_VSOCK_PORT: u32 = 13_340;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerOperation {
    Status,
    RequestStop,
    ForceStop,
    Pause,
    Resume,
    ShowViewer,
    HideViewer,
    ConfigureGuest,
    ShutdownHelper,
}

impl OwnerOperation {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::RequestStop => "requestStop",
            Self::ForceStop => "forceStop",
            Self::Pause => "pause",
            Self::Resume => "resume",
            Self::ShowViewer => "showViewer",
            Self::HideViewer => "hideViewer",
            Self::ConfigureGuest => "configureGuest",
            Self::ShutdownHelper => "shutdownHelper",
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OwnerResponse {
    pub protocol_version: u32,
    pub id: String,
    pub ok: bool,
    pub state: Option<String>,
    pub generation: Option<String>,
    pub pid: Option<u32>,
    pub error: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OwnerRequest<'a> {
    protocol_version: u32,
    id: String,
    operation: &'a str,
    expected_generation: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    guest_configuration: Option<&'a GuestRuntimeConfiguration>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GuestRuntimeConfiguration {
    pub schema_version: u32,
    pub portproxy_auth_token: Option<String>,
    pub vnc_legacy_enabled: Option<bool>,
    pub vnc_password: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CloneRequest<'a> {
    schema_version: u32,
    source_bundle_path: &'a str,
    destination_bundle_path: &'a str,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TemplateDescriptor {
    pub schema_version: u32,
    pub status: String,
    pub architecture: String,
    pub guest_operating_system_version: String,
    pub guest_build_version: String,
    pub device_profile: String,
    #[serde(rename = "hardwareModelSHA256")]
    pub hardware_model_sha256: String,
    #[serde(rename = "ipswSHA256")]
    pub ipsw_sha256: String,
}

impl TemplateDescriptor {
    pub fn digest(&self, manifest_bytes: &[u8]) -> String {
        format!("sha256:{:x}", Sha256::digest(manifest_bytes))
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RunRequest<'a> {
    schema_version: u32,
    bundle_path: &'a str,
    cpu_count: i32,
    memory_bytes: u64,
    provisioning_directory_path: Option<&'a str>,
    provisioning_directory_read_only: Option<bool>,
    network_mode: &'static str,
    viewer_mode: &'static str,
    loopback_relay_port: u16,
    guest_ingress_relay_port: u16,
    guest_service_relays: Vec<GuestServiceRelay>,
    shared_directories: Vec<SharedDirectory>,
    owner_control_socket_path: &'a str,
    runtime_generation: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GuestServiceRelay {
    vsock_port: u32,
    host_loopback_port: u16,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SharedDirectory {
    host_path: String,
    name: String,
    read_only: bool,
}

#[derive(Clone, Debug)]
pub struct RuntimePaths {
    pub bundle: PathBuf,
    pub run_request: PathBuf,
    pub clone_request: PathBuf,
    pub control_socket: PathBuf,
    pub pid: PathBuf,
    pub log: PathBuf,
}

impl RuntimePaths {
    pub fn new(vm_dir: &Path, runtime_dir: &Path) -> Self {
        Self {
            bundle: vm_dir.join("vz.bundle"),
            run_request: vm_dir.join("vz-run-request.json"),
            clone_request: vm_dir.join("vz-clone-request.json"),
            control_socket: runtime_dir.join("vz-owner.sock"),
            pid: vm_dir.join("vz-helper.pid"),
            log: vm_dir.join("vz-helper.log"),
        }
    }
}

pub fn helper_binary() -> Result<String> {
    env::var(HELPER_ENV)
        .or_else(|_| env::var(LEGACY_HELPER_ENV))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow!("macOS VZ source requires {HELPER_ENV} to name the signed chevalier-vz helper")
        })
}

pub fn load_template_descriptor(bundle: &Path) -> Result<(TemplateDescriptor, String)> {
    let manifest = bundle.join("manifest.json");
    let bytes = fs::read(&manifest)
        .with_context(|| format!("read macOS VZ template manifest {}", manifest.display()))?;
    let descriptor: TemplateDescriptor = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode macOS VZ template manifest {}", manifest.display()))?;
    if descriptor.schema_version != 1 || descriptor.status != "installed" {
        bail!("macOS VZ template manifest must describe an installed schema-v1 bundle");
    }
    if descriptor.architecture != "arm64" {
        bail!("macOS VZ template manifest architecture must be arm64");
    }
    if descriptor.hardware_model_sha256.trim().is_empty()
        || descriptor.ipsw_sha256.trim().is_empty()
    {
        bail!("macOS VZ template manifest is missing immutable provenance digests");
    }
    let digest = descriptor.digest(&bytes);
    Ok((descriptor, digest))
}

pub async fn instantiate_bundle(
    helper: &str,
    source_bundle: &Path,
    paths: &RuntimePaths,
) -> Result<()> {
    if !cfg!(target_os = "macos") {
        bail!("macOS VZ guests require a macOS host");
    }
    if !source_bundle.is_dir() {
        bail!(
            "macOS VZ template bundle does not exist: {}",
            source_bundle.display()
        );
    }
    if paths.bundle.exists() {
        bail!(
            "macOS VZ destination bundle already exists: {}",
            paths.bundle.display()
        );
    }

    let source_bundle_path = path_string(source_bundle)?;
    let destination_bundle_path = path_string(&paths.bundle)?;
    let request = CloneRequest {
        schema_version: REQUEST_SCHEMA_VERSION,
        source_bundle_path: &source_bundle_path,
        destination_bundle_path: &destination_bundle_path,
    };
    write_json(&paths.clone_request, &request)?;
    let output = Command::new(helper)
        .arg("clone")
        .arg("--request")
        .arg(&paths.clone_request)
        .stdin(Stdio::null())
        .output()
        .await
        .with_context(|| format!("launch macOS VZ bundle clone through {helper}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        bail!(
            "macOS VZ bundle clone failed with {}{}",
            output.status,
            if stderr.is_empty() {
                String::new()
            } else {
                format!(": {stderr}")
            }
        );
    }
    if !paths.bundle.is_dir() {
        bail!(
            "macOS VZ helper reported a successful clone without creating {}",
            paths.bundle.display()
        );
    }
    Ok(())
}

pub async fn launch(
    helper: &str,
    paths: &RuntimePaths,
    resources: &ResourceSpec,
    rpc_port: i32,
    proxy_port: i32,
    shared_mounts: &[SharedMountSpec],
    generation: &str,
) -> Result<Child> {
    if !cfg!(target_os = "macos") {
        bail!("macOS VZ guests require a macOS host");
    }
    let rpc_port = u16::try_from(rpc_port)
        .ok()
        .filter(|port| *port > 0)
        .ok_or_else(|| anyhow!("macOS VZ control relay requires a valid loopback port"))?;
    let proxy_port = u16::try_from(proxy_port)
        .ok()
        .filter(|port| *port > 0)
        .ok_or_else(|| anyhow!("macOS VZ guest ingress requires a valid loopback port"))?;
    if proxy_port == rpc_port {
        bail!("macOS VZ guest ingress and control relay ports must be distinct");
    }
    let guest_service_relays = vfs_guest_service_relays(shared_mounts)?;
    let bundle_path = path_string(&paths.bundle)?;
    let owner_control_socket_path = path_string(&paths.control_socket)?;
    let request = RunRequest {
        schema_version: REQUEST_SCHEMA_VERSION,
        bundle_path: &bundle_path,
        cpu_count: resources.vcpu,
        memory_bytes: u64::try_from(resources.memory_mb)
            .unwrap_or_default()
            .saturating_mul(1024 * 1024),
        provisioning_directory_path: None,
        provisioning_directory_read_only: None,
        network_mode: "natDevelopment",
        viewer_mode: "headless",
        loopback_relay_port: rpc_port,
        guest_ingress_relay_port: proxy_port,
        guest_service_relays,
        shared_directories: Vec::new(),
        owner_control_socket_path: &owner_control_socket_path,
        runtime_generation: generation,
    };
    write_json(&paths.run_request, &request)?;
    if let Some(parent) = paths.control_socket.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create VZ runtime directory {}", parent.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(parent, fs::Permissions::from_mode(0o700)).with_context(|| {
                format!(
                    "set private VZ runtime directory mode on {}",
                    parent.display()
                )
            })?;
        }
    }
    let _ = fs::remove_file(&paths.control_socket);

    let stdout = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.log)
        .with_context(|| format!("open VZ helper log {}", paths.log.display()))?;
    let stderr = stdout
        .try_clone()
        .with_context(|| format!("clone VZ helper log handle {}", paths.log.display()))?;
    let child = Command::new(helper)
        .arg("run")
        .arg("--request")
        .arg(&paths.run_request)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .kill_on_drop(false)
        .spawn()
        .with_context(|| format!("launch macOS VZ helper {helper}"))?;
    let pid = child
        .id()
        .ok_or_else(|| anyhow!("macOS VZ helper did not expose a process id"))?;
    fs::write(&paths.pid, format!("{pid}\n"))
        .with_context(|| format!("write VZ helper pid file {}", paths.pid.display()))?;
    Ok(child)
}

pub async fn wait_for_state(
    socket: &Path,
    generation: &str,
    expected: &[&str],
    timeout: Duration,
) -> Result<OwnerResponse> {
    let deadline = Instant::now() + timeout;
    loop {
        let last_error = match request(socket, generation, OwnerOperation::Status).await {
            Ok(response)
                if response.ok
                    && response
                        .state
                        .as_deref()
                        .is_some_and(|state| expected.contains(&state)) =>
            {
                return Ok(response);
            }
            Ok(response) if !response.ok => response
                .error
                .unwrap_or_else(|| "owner rejected status request".to_string()),
            Ok(response) => {
                format!(
                    "helper state is {}",
                    response.state.as_deref().unwrap_or("unknown")
                )
            }
            Err(error) => error.to_string(),
        };
        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for macOS VZ helper state [{}]: {}",
                expected.join(", "),
                last_error
            );
        }
        sleep(Duration::from_millis(100)).await;
    }
}

pub async fn request(
    socket: &Path,
    generation: &str,
    operation: OwnerOperation,
) -> Result<OwnerResponse> {
    let request = OwnerRequest {
        protocol_version: PROTOCOL_VERSION,
        id: Uuid::new_v4().to_string(),
        operation: operation.as_str(),
        expected_generation: generation,
        guest_configuration: None,
    };
    send_owner_request(socket, generation, request).await
}

pub async fn configure_guest(
    socket: &Path,
    generation: &str,
    configuration: &GuestRuntimeConfiguration,
) -> Result<OwnerResponse> {
    let request = OwnerRequest {
        protocol_version: PROTOCOL_VERSION,
        id: Uuid::new_v4().to_string(),
        operation: OwnerOperation::ConfigureGuest.as_str(),
        expected_generation: generation,
        guest_configuration: Some(configuration),
    };
    send_owner_request(socket, generation, request).await
}

async fn send_owner_request(
    socket: &Path,
    generation: &str,
    request: OwnerRequest<'_>,
) -> Result<OwnerResponse> {
    let payload = serde_json::to_vec(&request)?;
    if payload.len() > MAX_FRAME_BYTES {
        bail!("macOS VZ owner request exceeds maximum frame size");
    }
    let mut stream = UnixStream::connect(socket)
        .await
        .with_context(|| format!("connect to macOS VZ owner socket {}", socket.display()))?;
    stream.write_u32(payload.len() as u32).await?;
    stream.write_all(&payload).await?;
    stream.flush().await?;

    let length = stream.read_u32().await? as usize;
    if length == 0 || length > MAX_FRAME_BYTES {
        bail!("macOS VZ owner returned invalid frame length {length}");
    }
    let mut response = vec![0u8; length];
    stream.read_exact(&mut response).await?;
    let response: OwnerResponse =
        serde_json::from_slice(&response).context("decode macOS VZ owner response")?;
    if response.protocol_version != PROTOCOL_VERSION {
        bail!(
            "macOS VZ owner protocol mismatch: expected {PROTOCOL_VERSION}, got {}",
            response.protocol_version
        );
    }
    if response.id != request.id {
        bail!("macOS VZ owner response id mismatch");
    }
    if response.generation.as_deref() != Some(generation) {
        bail!("macOS VZ owner runtime generation mismatch");
    }
    Ok(response)
}

pub async fn request_stop_or_observe_terminal(
    socket: &Path,
    generation: &str,
    operation: OwnerOperation,
) -> Result<OwnerResponse> {
    if !matches!(
        operation,
        OwnerOperation::RequestStop | OwnerOperation::ForceStop
    ) {
        bail!("{} is not a stop operation", operation.as_str());
    }
    let status = request(socket, generation, OwnerOperation::Status).await?;
    if response_is_terminal(&status) {
        return Ok(status);
    }
    let response = request(socket, generation, operation).await?;
    if response.ok {
        return Ok(response);
    }
    let observed = request(socket, generation, OwnerOperation::Status).await?;
    if response_is_terminal(&observed) {
        Ok(observed)
    } else {
        Ok(response)
    }
}

fn response_is_terminal(response: &OwnerResponse) -> bool {
    response.ok
        && response
            .state
            .as_deref()
            .is_some_and(|state| matches!(state, "stopped" | "error"))
}

pub fn runtime_generation() -> String {
    Uuid::new_v4().to_string()
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    fs::write(path, bytes).with_context(|| format!("write {}", path.display()))
}

fn path_string(path: &Path) -> Result<String> {
    path.to_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("path is not valid UTF-8: {}", path.display()))
}

fn vfs_guest_service_relays(shared_mounts: &[SharedMountSpec]) -> Result<Vec<GuestServiceRelay>> {
    let mut host_loopback_port = None;
    for mount in shared_mounts {
        if !mount.is_fuse_backed() {
            bail!(
                "macOS VZ shared mount {} must use the guest-native VFS profile",
                mount.mount_tag
            );
        }
        let endpoint = reqwest::Url::parse(&mount.vfs_endpoint).with_context(|| {
            format!(
                "parse macOS VFS endpoint for shared mount {}",
                mount.mount_tag
            )
        })?;
        if endpoint.scheme() != "http"
            || !matches!(endpoint.host_str(), Some("127.0.0.1" | "localhost"))
        {
            bail!(
                "macOS VFS endpoint must use host loopback HTTP: {}",
                mount.vfs_endpoint
            );
        }
        let port = endpoint
            .port_or_known_default()
            .ok_or_else(|| anyhow!("macOS VFS endpoint has no port: {}", mount.vfs_endpoint))?;
        match host_loopback_port {
            Some(existing) if existing != port => {
                bail!("all macOS VFS mounts must use one host gateway port ({existing} != {port})")
            }
            Some(_) => {}
            None => host_loopback_port = Some(port),
        }
    }
    Ok(host_loopback_port
        .map(|host_loopback_port| {
            vec![GuestServiceRelay {
                vsock_port: VFS_VSOCK_PORT,
                host_loopback_port,
            }]
        })
        .unwrap_or_default())
}

pub fn control_vsock_port() -> u32 {
    CONTROL_VSOCK_PORT
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::types::{SharedMountAvailability, SharedMountContinuity};

    fn mount(tag: &str) -> SharedMountSpec {
        SharedMountSpec {
            host_path: format!("/tmp/{tag}"),
            guest_path: "/Volumes/OpenBracketWorkspace".to_string(),
            mount_tag: tag.to_string(),
            read_only: false,
            availability: SharedMountAvailability::NodeLocal,
            continuity: SharedMountContinuity::RestartSameNode,
            backend_profile: "openbracket-vfs-fuse".to_string(),
            vfs_endpoint: "http://127.0.0.1:63339".to_string(),
            vfs_scope_path: "scope".to_string(),
        }
    }

    #[test]
    fn maps_guest_vfs_to_host_gateway_relay() {
        let relays = vfs_guest_service_relays(&[mount("workspace")]).unwrap();
        assert_eq!(relays.len(), 1);
        assert_eq!(relays[0].vsock_port, VFS_VSOCK_PORT);
        assert_eq!(relays[0].host_loopback_port, 63339);
    }

    #[test]
    fn rejects_non_loopback_or_mixed_gateway_relays() {
        let mut remote = mount("workspace");
        remote.vfs_endpoint = "https://example.com/vfs".to_string();
        assert!(vfs_guest_service_relays(&[remote]).is_err());

        let mut second = mount("tools");
        second.vfs_endpoint = "http://127.0.0.1:63340/vfs".to_string();
        assert!(vfs_guest_service_relays(&[mount("workspace"), second]).is_err());
    }

    #[test]
    fn runtime_paths_are_backend_specific() {
        let paths = RuntimePaths::new(Path::new("/tmp/vm"), Path::new("/tmp/run/vm"));
        assert_eq!(paths.bundle, PathBuf::from("/tmp/vm/vz.bundle"));
        assert_eq!(
            paths.control_socket,
            PathBuf::from("/tmp/run/vm/vz-owner.sock")
        );
        assert_ne!(runtime_generation(), runtime_generation());
        assert_eq!(control_vsock_port(), 13_338);
    }

    #[test]
    fn loads_installed_template_provenance() {
        let bundle = tempfile::tempdir().unwrap();
        fs::write(
            bundle.path().join("manifest.json"),
            r#"{
                "schemaVersion": 1,
                "status": "installed",
                "architecture": "arm64",
                "guestOperatingSystemVersion": "26.6.1",
                "guestBuildVersion": "25G76",
                "deviceProfile": "macos-v1",
                "hardwareModelSHA256": "hardware",
                "ipswSHA256": "ipsw"
            }"#,
        )
        .unwrap();

        let (descriptor, digest) = load_template_descriptor(bundle.path()).unwrap();
        assert_eq!(descriptor.guest_operating_system_version, "26.6.1");
        assert_eq!(descriptor.guest_build_version, "25G76");
        assert_eq!(descriptor.device_profile, "macos-v1");
        assert!(digest.starts_with("sha256:"));
    }
}
