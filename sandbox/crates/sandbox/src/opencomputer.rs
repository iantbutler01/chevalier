// @dive-file: Private OpenComputer transport backing the existing chevalier-sandbox facade.
// @dive-rel: Called only by Sandbox/Session branches so consumers keep one sandbox API.

use std::collections::HashMap;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use crate::proto::bracket::portproxy::v1::DirectoryEntry;
use crate::{
    ExecEvent, ExecHandle, ExecInput, ExecOptions, ForkOptions, ForkResult,
    OpenComputerBackendConfig, OpenComputerMountConfig, Result, Sandbox, SandboxError, Session,
    SharedMount, ShellEvent, ShellHandle, ShellInput, ShellOptions,
};

const API_KEY_HEADER: &str = "X-API-Key";

#[derive(Clone)]
pub(crate) struct OpenComputerControl {
    cfg: OpenComputerBackendConfig,
    client: Client,
}

impl OpenComputerControl {
    pub(crate) fn new(mut cfg: OpenComputerBackendConfig) -> Result<Self> {
        cfg.api_url = normalize_api_url(&cfg.api_url)?;
        cfg.template_id = cfg.template_id.trim().to_string();
        if cfg.template_id.is_empty() {
            cfg.template_id = "base".to_string();
        }
        cfg.checkpoint_id = cfg.checkpoint_id.trim().to_string();
        if cfg.api_key.trim().is_empty() {
            return Err(SandboxError::InvalidConfig(
                "OPENCOMPUTER_API_KEY is required when SandboxConfig.provider is OpenComputer"
                    .to_string(),
            ));
        }
        Ok(Self {
            cfg,
            client: Client::new(),
        })
    }

    pub(crate) fn api_url(&self) -> &str {
        &self.cfg.api_url
    }

    pub(crate) async fn create_sandbox(
        &self,
        template_id: Option<String>,
        resources: Option<crate::ResourceLimits>,
        metadata: HashMap<String, String>,
        envs: Option<HashMap<String, String>>,
        egress_allowlist: Option<Vec<String>>,
        shared_mounts: &[SharedMount],
    ) -> Result<OpenComputerSandbox> {
        let template_id = template_id
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        if template_id.is_none() && !self.cfg.checkpoint_id.is_empty() {
            let resource_fields = resolve_resource_fields(resources.as_ref(), &self.cfg);
            return self
                .create_from_checkpoint_with_resource_fields(
                    self.cfg.checkpoint_id.as_str(),
                    metadata,
                    envs,
                    egress_allowlist,
                    resource_fields,
                    shared_mounts,
                )
                .await;
        }
        let resource_fields = resolve_resource_fields(resources.as_ref(), &self.cfg);
        let body = CreateSandboxBody {
            template_id: Some(template_id.unwrap_or_else(|| self.cfg.template_id.clone())),
            timeout: Some(self.cfg.timeout_secs),
            envs,
            metadata: (!metadata.is_empty()).then_some(metadata),
            burst: self.cfg.burst,
            cpu_count: resource_fields.cpu_count,
            memory_mb: resource_fields.memory_mb,
            disk_mb: resource_fields.disk_mb,
            secret_store: self.cfg.secret_store.clone(),
            egress_allowlist: egress_allowlist.or_else(|| self.cfg.egress_allowlist.clone()),
        };
        let response = self
            .send(self.client.post(self.url("/sandboxes")).json(&body))
            .await?;
        let sandbox: OpenComputerSandbox = decode_json(response, "create sandbox").await?;
        self.ensure_configured_mounts(&sandbox.sandbox_id, shared_mounts)
            .await?;
        Ok(sandbox)
    }

    pub(crate) async fn get_sandbox(&self, sandbox_id: &str) -> Result<OpenComputerSandbox> {
        let response = self
            .send(self.client.get(self.sandbox_url(sandbox_id, "")))
            .await?;
        decode_json(response, "get sandbox").await
    }

    pub(crate) async fn delete_sandbox(&self, sandbox_id: &str) -> Result<()> {
        let response = self
            .client
            .delete(self.sandbox_url(sandbox_id, ""))
            .header(API_KEY_HEADER, self.cfg.api_key.as_str())
            .send()
            .await
            .map_err(|err| {
                SandboxError::DaemonUnavailable(format!("OpenComputer request failed: {err}"))
            })?;
        if delete_sandbox_status_ok(response.status()) {
            return Ok(());
        }
        Err(opencomputer_response_error(response).await)
    }

    pub(crate) async fn ensure_configured_mounts(
        &self,
        sandbox_id: &str,
        shared_mounts: &[SharedMount],
    ) -> Result<()> {
        let mounts = self.resolve_mounts(shared_mounts)?;
        for mount in mounts {
            self.add_mount(sandbox_id, mount).await?;
        }
        Ok(())
    }

    pub(crate) async fn list_sessions(&self) -> Result<Vec<crate::SessionInfo>> {
        Err(SandboxError::Unsupported(
            "OpenComputer does not expose sandbox listing through the documented API; persist session ids in the caller"
                .to_string(),
        ))
    }

    pub(crate) async fn read_file(&self, sandbox_id: &str, path: &str) -> Result<Vec<u8>> {
        let response = self
            .send(
                self.client
                    .get(self.sandbox_url(sandbox_id, "/files"))
                    .query(&[("path", path)]),
            )
            .await?;
        let bytes = response
            .bytes()
            .await
            .map_err(|err| SandboxError::InvalidResponse(format!("read file body: {err}")))?;
        Ok(bytes.to_vec())
    }

    pub(crate) async fn write_file(
        &self,
        sandbox_id: &str,
        path: &str,
        data: Vec<u8>,
    ) -> Result<()> {
        self.send_empty(
            self.client
                .put(self.sandbox_url(sandbox_id, "/files"))
                .query(&[("path", path)])
                .body(data),
        )
        .await
    }

    pub(crate) async fn list_dir(
        &self,
        sandbox_id: &str,
        path: &str,
    ) -> Result<Vec<DirectoryEntry>> {
        let response = self
            .send(
                self.client
                    .get(self.sandbox_url(sandbox_id, "/files/list"))
                    .query(&[("path", path)]),
            )
            .await?;
        let entries: Vec<FileEntry> = decode_json(response, "list directory").await?;
        Ok(entries
            .into_iter()
            .map(|entry| DirectoryEntry {
                name: entry.name,
                is_dir: entry.is_dir,
                is_symlink: false,
            })
            .collect())
    }

    pub(crate) async fn delete_path(&self, sandbox_id: &str, path: &str) -> Result<()> {
        self.send_empty(
            self.client
                .delete(self.sandbox_url(sandbox_id, "/files"))
                .query(&[("path", path)]),
        )
        .await
    }

    pub(crate) async fn exec(
        &self,
        sandbox_id: &str,
        command: &str,
        opts: ExecOptions,
    ) -> Result<ExecHandle> {
        let shell = opts.shell.unwrap_or_else(|| "/bin/sh".to_string());
        let start = ExecStartBody {
            cmd: shell,
            args: vec!["-lc".to_string(), command.to_string()],
            envs: (!opts.env.is_empty()).then_some(opts.env),
            cwd: None,
            timeout: opts
                .timeout_secs
                .and_then(|value| u64::try_from(value).ok()),
            max_run_after_disconnect: opts.detach.then_some(24 * 60 * 60),
        };
        self.exec_with_start_body(sandbox_id, start, opts.close_stdin_on_start)
            .await
    }

    pub(crate) async fn shell(&self, sandbox_id: &str, opts: ShellOptions) -> Result<ShellHandle> {
        let shell = opts.shell.unwrap_or_else(|| "bash".to_string());
        let args = if opts.args.is_empty() {
            vec![
                "--noprofile".to_string(),
                "--norc".to_string(),
                "+m".to_string(),
            ]
        } else {
            opts.args
        };
        let start = ExecStartBody {
            cmd: shell,
            args,
            envs: (!opts.env.is_empty()).then_some(opts.env),
            cwd: opts.cwd,
            timeout: None,
            max_run_after_disconnect: None,
        };
        let response = self
            .send(
                self.client
                    .post(self.sandbox_url(sandbox_id, "/exec"))
                    .json(&start),
            )
            .await?;
        let created: ExecSessionCreated = decode_json(response, "create shell exec").await?;
        let ws_url = self.exec_ws_url(sandbox_id, &created.session_id);
        let (ws, response) = connect_async(&ws_url).await.map_err(|err| {
            SandboxError::DaemonUnavailable(format!("OpenComputer shell ws: {err}"))
        })?;
        if response.status() != StatusCode::SWITCHING_PROTOCOLS {
            return Err(SandboxError::InvalidResponse(format!(
                "OpenComputer shell websocket returned {}",
                response.status()
            )));
        }
        let (mut write, mut read) = ws.split();
        let (input_tx, mut input_rx) = mpsc::channel(64);
        let (event_tx, event_rx) = mpsc::channel(128);

        tokio::spawn(async move {
            while let Some(input) = input_rx.recv().await {
                match input {
                    ShellInput::Data(data) => {
                        if write.send(stdin_frame(data)).await.is_err() {
                            break;
                        }
                    }
                    ShellInput::Resize { .. } => {}
                    ShellInput::Eof => {
                        let _ = write.close().await;
                        break;
                    }
                }
            }
        });

        tokio::spawn(async move {
            while let Some(message) = read.next().await {
                match message {
                    Ok(Message::Binary(data)) => {
                        if let Some(event) = shell_event_from_ws_frame(data.as_ref()) {
                            let terminal = matches!(event, ShellEvent::Exit(_));
                            if event_tx.send(Ok(event)).await.is_err() || terminal {
                                break;
                            }
                        }
                    }
                    Ok(Message::Text(text)) => {
                        if event_tx
                            .send(Ok(ShellEvent::Output(text.to_string().into_bytes())))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(Message::Close(_)) => {
                        let _ = event_tx.send(Ok(ShellEvent::Exit(0))).await;
                        break;
                    }
                    Ok(_) => {}
                    Err(err) => {
                        let _ = event_tx
                            .send(Err(SandboxError::DaemonUnavailable(format!(
                                "OpenComputer shell websocket failed: {err}"
                            ))))
                            .await;
                        break;
                    }
                }
            }
        });

        Ok(ShellHandle {
            input: input_tx,
            events: Box::pin(ReceiverStream::new(event_rx)),
        })
    }

    pub(crate) async fn fork(
        &self,
        sandbox: Sandbox,
        parent: &Session,
        opts: ForkOptions,
    ) -> Result<ForkResult> {
        let checkpoint_name = opts
            .child_name
            .as_deref()
            .filter(|name| !name.trim().is_empty())
            .map(|name| format!("chevalier-fork-{name}"))
            .unwrap_or_else(|| format!("chevalier-fork-{}", Uuid::new_v4()));
        let checkpoint = self
            .create_checkpoint(parent.vm_id(), checkpoint_name.as_str())
            .await?;
        let child = self
            .create_from_checkpoint(
                checkpoint.id.as_str(),
                opts.child_metadata,
                None,
                None,
                parent.shared_mounts.as_slice(),
            )
            .await?;
        let child_session_id = child.sandbox_id.clone();
        let session = Session::new_with_backend(
            sandbox,
            child_session_id.clone(),
            child.sandbox_id,
            self.api_url().to_string(),
            None,
            parent.shared_mounts.as_ref().clone(),
        );
        Ok(ForkResult {
            parent_session_id: parent.session_id().to_string(),
            child_session_id,
            fork_id: checkpoint.id,
            child: session,
        })
    }

    async fn exec_with_start_body(
        &self,
        sandbox_id: &str,
        start: ExecStartBody,
        close_stdin_on_start: bool,
    ) -> Result<ExecHandle> {
        let response = self
            .send(
                self.client
                    .post(self.sandbox_url(sandbox_id, "/exec"))
                    .json(&start),
            )
            .await?;
        let created: ExecSessionCreated = decode_json(response, "create exec").await?;
        let ws_url = self.exec_ws_url(sandbox_id, &created.session_id);
        let (ws, response) = connect_async(&ws_url).await.map_err(|err| {
            SandboxError::DaemonUnavailable(format!("OpenComputer exec ws: {err}"))
        })?;
        if response.status() != StatusCode::SWITCHING_PROTOCOLS {
            return Err(SandboxError::InvalidResponse(format!(
                "OpenComputer exec websocket returned {}",
                response.status()
            )));
        }
        let (mut write, mut read) = ws.split();
        let (input_tx, mut input_rx) = mpsc::channel(64);
        let (event_tx, event_rx) = mpsc::channel(128);
        let kill_control = self.clone();
        let kill_sandbox_id = sandbox_id.to_string();
        let kill_session_id = created.session_id.clone();

        if close_stdin_on_start {
            let _ = write.close().await;
        } else {
            let event_tx_input = event_tx.clone();
            tokio::spawn(async move {
                while let Some(input) = input_rx.recv().await {
                    match input {
                        ExecInput::Data(data) => {
                            if write.send(stdin_frame(data)).await.is_err() {
                                break;
                            }
                        }
                        ExecInput::Eof => {
                            let _ = write.close().await;
                            break;
                        }
                        ExecInput::Signal(signal) => {
                            if let Err(err) = kill_control
                                .kill_exec_session(&kill_sandbox_id, &kill_session_id, Some(signal))
                                .await
                            {
                                let _ = event_tx_input.send(Err(err)).await;
                            }
                        }
                        ExecInput::Resize { cols, rows } => {
                            let _ = event_tx_input
                                .send(Err(SandboxError::Unsupported(format!(
                                    "OpenComputer exec resize is not exposed through exec streams: {cols}x{rows}"
                                ))))
                                .await;
                        }
                    }
                }
            });
        }

        tokio::spawn(async move {
            while let Some(message) = read.next().await {
                match message {
                    Ok(Message::Binary(data)) => {
                        if let Some(event) = exec_event_from_ws_frame(data.as_ref()) {
                            let terminal = matches!(event, ExecEvent::Exit(_) | ExecEvent::Timeout);
                            if event_tx.send(Ok(event)).await.is_err() || terminal {
                                break;
                            }
                        }
                    }
                    Ok(Message::Close(_)) => break,
                    Ok(_) => {}
                    Err(err) => {
                        let _ = event_tx
                            .send(Err(SandboxError::DaemonUnavailable(format!(
                                "OpenComputer exec websocket failed: {err}"
                            ))))
                            .await;
                        break;
                    }
                }
            }
        });

        Ok(ExecHandle {
            input: input_tx,
            events: Box::pin(ReceiverStream::new(event_rx)),
        })
    }

    pub(crate) async fn create_checkpoint(
        &self,
        sandbox_id: &str,
        name: &str,
    ) -> Result<CheckpointInfo> {
        let response = self
            .send(
                self.client
                    .post(self.sandbox_url(sandbox_id, "/checkpoints"))
                    .json(&CheckpointCreateBody { name }),
            )
            .await?;
        decode_json(response, "create checkpoint").await
    }

    pub(crate) async fn create_from_checkpoint(
        &self,
        checkpoint_id: &str,
        metadata: HashMap<String, String>,
        envs: Option<HashMap<String, String>>,
        egress_allowlist: Option<Vec<String>>,
        shared_mounts: &[SharedMount],
    ) -> Result<OpenComputerSandbox> {
        self.create_from_checkpoint_with_resource_fields(
            checkpoint_id,
            metadata,
            envs,
            egress_allowlist,
            resolve_resource_fields(None, &self.cfg),
            shared_mounts,
        )
        .await
    }

    async fn create_from_checkpoint_with_resource_fields(
        &self,
        checkpoint_id: &str,
        metadata: HashMap<String, String>,
        envs: Option<HashMap<String, String>>,
        egress_allowlist: Option<Vec<String>>,
        resource_fields: ResourceFields,
        shared_mounts: &[SharedMount],
    ) -> Result<OpenComputerSandbox> {
        let body = CreateFromCheckpointBody {
            timeout: Some(self.cfg.timeout_secs),
            envs,
            metadata: (!metadata.is_empty()).then_some(metadata),
            burst: self.cfg.burst,
            cpu_count: resource_fields.cpu_count,
            memory_mb: resource_fields.memory_mb,
            disk_mb: resource_fields.disk_mb,
            secret_store: self.cfg.secret_store.clone(),
            egress_allowlist: egress_allowlist.or_else(|| self.cfg.egress_allowlist.clone()),
        };
        let response = self
            .send(
                self.client
                    .post(self.url(&format!("/sandboxes/from-checkpoint/{checkpoint_id}")))
                    .json(&body),
            )
            .await?;
        let sandbox: OpenComputerSandbox =
            decode_json(response, "create sandbox from checkpoint").await?;
        self.ensure_configured_mounts(&sandbox.sandbox_id, shared_mounts)
            .await?;
        Ok(sandbox)
    }

    async fn add_mount(&self, sandbox_id: &str, mount: OpenComputerMountConfig) -> Result<()> {
        let body = AddMountBody::from_config(mount)?;
        let response = self
            .send(
                self.client
                    .post(self.sandbox_url(sandbox_id, "/mounts"))
                    .json(&body),
            )
            .await?;
        let _body: serde_json::Value = decode_json(response, "add mount").await?;
        Ok(())
    }

    fn resolve_mounts(
        &self,
        shared_mounts: &[SharedMount],
    ) -> Result<Vec<OpenComputerMountConfig>> {
        let mut mounts = self.cfg.mounts.clone();
        for shared in shared_mounts {
            let mut mount = self.shared_mount_config(shared)?.clone();
            if mount.path.trim().is_empty() {
                mount.path = shared.guest_path.clone();
            }
            if mount.read_only.is_none() {
                mount.read_only = Some(shared.read_only);
            }
            render_shared_mount_config(&mut mount, shared);
            mounts.push(mount);
        }
        Ok(mounts)
    }

    fn shared_mount_config(&self, shared: &SharedMount) -> Result<&OpenComputerMountConfig> {
        let backend_profile = crate::normalize_mount_backend_profile(&shared.backend_profile);
        self.cfg
            .shared_mounts
            .get(shared.mount_tag.as_str())
            .or_else(|| self.cfg.shared_mounts.get(shared.guest_path.as_str()))
            .or_else(|| self.cfg.shared_mounts.get(backend_profile.as_str()))
            .ok_or_else(|| {
                SandboxError::Unsupported(format!(
                    "OpenComputer shared mount `{}` at `{}` needs an OpenComputerBackendConfig.shared_mounts mapping by mount tag, guest path, or backend profile",
                    shared.mount_tag, shared.guest_path
                ))
            })
    }

    async fn kill_exec_session(
        &self,
        sandbox_id: &str,
        session_id: &str,
        signal: Option<i32>,
    ) -> Result<()> {
        self.send_empty(
            self.client
                .post(self.sandbox_url(sandbox_id, &format!("/exec/{session_id}/kill")))
                .json(&ExecKillBody { signal }),
        )
        .await
    }

    fn url(&self, suffix: &str) -> String {
        format!(
            "{}/{}",
            self.cfg.api_url.trim_end_matches('/'),
            suffix.trim_start_matches('/')
        )
    }

    fn sandbox_url(&self, sandbox_id: &str, suffix: &str) -> String {
        format!(
            "{}/sandboxes/{}{}",
            self.cfg.api_url.trim_end_matches('/'),
            sandbox_id,
            suffix
        )
    }

    fn exec_ws_url(&self, sandbox_id: &str, session_id: &str) -> String {
        format!(
            "{}/sandboxes/{}/exec/{}?api_key={}",
            websocket_api_url(&self.cfg.api_url),
            sandbox_id,
            session_id,
            urlencoding::encode(self.cfg.api_key.as_str())
        )
    }

    async fn send(&self, builder: reqwest::RequestBuilder) -> Result<reqwest::Response> {
        let response = builder
            .header(API_KEY_HEADER, self.cfg.api_key.as_str())
            .send()
            .await
            .map_err(|err| {
                SandboxError::DaemonUnavailable(format!("OpenComputer request failed: {err}"))
            })?;
        if response.status().is_success() {
            return Ok(response);
        }
        Err(opencomputer_response_error(response).await)
    }

    async fn send_empty(&self, builder: reqwest::RequestBuilder) -> Result<()> {
        let response = self.send(builder).await?;
        if response.status() == StatusCode::NO_CONTENT || response.status().is_success() {
            Ok(())
        } else {
            Err(SandboxError::InvalidResponse(format!(
                "OpenComputer returned unexpected success status {}",
                response.status()
            )))
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct OpenComputerSandbox {
    #[serde(rename = "sandboxID", alias = "sandboxId")]
    pub sandbox_id: String,
    #[serde(rename = "sandboxDomain", default)]
    pub sandbox_domain: Option<String>,
}

impl OpenComputerSandbox {
    pub(crate) fn preview_domain(&self, port: u16) -> Option<String> {
        let domain = self.sandbox_domain.as_deref()?.trim();
        (!domain.is_empty()).then(|| format!("{}-p{port}.{domain}", self.sandbox_id))
    }
}

#[derive(Serialize)]
struct CreateSandboxBody {
    #[serde(rename = "templateID", skip_serializing_if = "Option::is_none")]
    template_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    envs: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    burst: Option<bool>,
    #[serde(rename = "cpuCount", skip_serializing_if = "Option::is_none")]
    cpu_count: Option<u32>,
    #[serde(rename = "memoryMB", skip_serializing_if = "Option::is_none")]
    memory_mb: Option<u32>,
    #[serde(rename = "diskMB", skip_serializing_if = "Option::is_none")]
    disk_mb: Option<u32>,
    #[serde(rename = "secretStore", skip_serializing_if = "Option::is_none")]
    secret_store: Option<String>,
    #[serde(rename = "egressAllowlist", skip_serializing_if = "Option::is_none")]
    egress_allowlist: Option<Vec<String>>,
}

#[derive(Debug, PartialEq, Eq)]
struct ResourceFields {
    cpu_count: Option<u32>,
    memory_mb: Option<u32>,
    disk_mb: Option<u32>,
}

fn resolve_resource_fields(
    resources: Option<&crate::ResourceLimits>,
    cfg: &OpenComputerBackendConfig,
) -> ResourceFields {
    ResourceFields {
        cpu_count: resource_vcpu(resources).or(cfg.default_cpu_count),
        memory_mb: resource_memory_mb(resources).or(cfg.default_memory_mb),
        disk_mb: resource_disk_mb(resources).or(cfg.default_disk_mb),
    }
}

fn resource_vcpu(resources: Option<&crate::ResourceLimits>) -> Option<u32> {
    resources.and_then(|resources| u32::try_from(resources.vcpu).ok())
}

fn resource_memory_mb(resources: Option<&crate::ResourceLimits>) -> Option<u32> {
    resources.and_then(|resources| u32::try_from(resources.memory_mb).ok())
}

fn resource_disk_mb(resources: Option<&crate::ResourceLimits>) -> Option<u32> {
    resources.and_then(|resources| {
        u32::try_from(resources.disk_gb)
            .ok()
            .and_then(|gb| gb.checked_mul(1024))
    })
}

#[derive(Serialize)]
struct CreateFromCheckpointBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    envs: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    burst: Option<bool>,
    #[serde(rename = "cpuCount", skip_serializing_if = "Option::is_none")]
    cpu_count: Option<u32>,
    #[serde(rename = "memoryMB", skip_serializing_if = "Option::is_none")]
    memory_mb: Option<u32>,
    #[serde(rename = "diskMB", skip_serializing_if = "Option::is_none")]
    disk_mb: Option<u32>,
    #[serde(rename = "secretStore", skip_serializing_if = "Option::is_none")]
    secret_store: Option<String>,
    #[serde(rename = "egressAllowlist", skip_serializing_if = "Option::is_none")]
    egress_allowlist: Option<Vec<String>>,
}

#[derive(Serialize)]
struct AddMountBody {
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    driver: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    remote: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    backend: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    command: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    env: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    secrets: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    creds: Option<HashMap<String, String>>,
    #[serde(rename = "rcloneConfig", skip_serializing_if = "Option::is_none")]
    rclone_config: Option<String>,
    #[serde(rename = "readOnly", skip_serializing_if = "Option::is_none")]
    read_only: Option<bool>,
    #[serde(rename = "mountOptions", skip_serializing_if = "Vec::is_empty")]
    mount_options: Vec<String>,
}

impl AddMountBody {
    fn from_config(mut config: OpenComputerMountConfig) -> Result<Self> {
        config.path = config.path.trim().to_string();
        config.remote = config.remote.trim().to_string();
        if config.path.is_empty() {
            return Err(SandboxError::InvalidConfig(
                "OpenComputer mount path must not be empty".to_string(),
            ));
        }
        let driver = config
            .driver
            .map(|driver| driver.trim().to_ascii_lowercase())
            .filter(|driver| !driver.is_empty());
        if !matches!(driver.as_deref(), None | Some("rclone") | Some("command")) {
            return Err(SandboxError::InvalidConfig(format!(
                "OpenComputer mount `{}` uses unsupported driver `{}`",
                config.path,
                driver.as_deref().unwrap_or_default()
            )));
        }
        let command_driver = matches!(driver.as_deref(), Some("command"));
        if command_driver && config.command.is_empty() {
            return Err(SandboxError::InvalidConfig(format!(
                "OpenComputer command mount `{}` requires command argv",
                config.path
            )));
        }
        if !command_driver && config.remote.is_empty() {
            return Err(SandboxError::InvalidConfig(format!(
                "OpenComputer mount `{}` remote must not be empty",
                config.path
            )));
        }
        let backend = if command_driver {
            None
        } else {
            config
                .backend
                .map(|backend| backend.trim().to_ascii_lowercase())
                .filter(|backend| !backend.is_empty())
        };
        let rclone_config = if command_driver {
            None
        } else {
            config
                .rclone_config
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        Ok(Self {
            path: config.path,
            driver,
            remote: (!command_driver).then_some(config.remote),
            backend,
            command: config.command,
            env: (!config.env.is_empty()).then_some(config.env),
            secrets: (!config.secrets.is_empty()).then_some(config.secrets),
            creds: (!command_driver && !config.creds.is_empty()).then_some(config.creds),
            rclone_config,
            read_only: config.read_only,
            mount_options: if command_driver {
                Vec::new()
            } else {
                config.mount_options
            },
        })
    }
}

fn render_shared_mount_config(config: &mut OpenComputerMountConfig, shared: &SharedMount) {
    config.path = render_shared_mount_template(&config.path, shared);
    config.remote = render_shared_mount_template(&config.remote, shared);
    config.command = config
        .command
        .iter()
        .map(|value| render_shared_mount_template(value, shared))
        .collect();
    config.env = config
        .env
        .iter()
        .map(|(key, value)| {
            (
                render_shared_mount_template(key, shared),
                render_shared_mount_template(value, shared),
            )
        })
        .collect();
    config.secrets = config
        .secrets
        .iter()
        .map(|(key, value)| {
            (
                render_shared_mount_template(key, shared),
                render_shared_mount_template(value, shared),
            )
        })
        .collect();
}

fn render_shared_mount_template(template: &str, shared: &SharedMount) -> String {
    template
        .replace("{guest_path}", shared.guest_path.as_str())
        .replace("{mount_tag}", shared.mount_tag.as_str())
        .replace("{backend_profile}", shared.backend_profile.as_str())
        .replace("{vfs_endpoint}", shared.vfs_endpoint.as_str())
        .replace("{vfs_scope_path}", shared.vfs_scope_path.trim_matches('/'))
        .replace(
            "{read_only}",
            if shared.read_only { "true" } else { "false" },
        )
}

#[derive(Serialize)]
struct ExecStartBody {
    cmd: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    args: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    envs: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout: Option<u64>,
    #[serde(
        rename = "maxRunAfterDisconnect",
        skip_serializing_if = "Option::is_none"
    )]
    max_run_after_disconnect: Option<u64>,
}

#[derive(Deserialize)]
struct ExecSessionCreated {
    #[serde(rename = "sessionID", alias = "sessionId")]
    session_id: String,
}

#[derive(Serialize)]
struct ExecKillBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    signal: Option<i32>,
}

#[derive(Serialize)]
struct CheckpointCreateBody<'a> {
    name: &'a str,
}

#[derive(Deserialize)]
pub(crate) struct CheckpointInfo {
    pub(crate) id: String,
}

#[derive(Deserialize)]
struct FileEntry {
    name: String,
    #[serde(rename = "isDir")]
    is_dir: bool,
}

fn stdin_frame(data: Vec<u8>) -> Message {
    let mut frame = Vec::with_capacity(data.len() + 1);
    frame.push(0x00);
    frame.extend(data);
    Message::Binary(Bytes::from(frame))
}

fn exec_event_from_ws_frame(frame: &[u8]) -> Option<ExecEvent> {
    let (&stream_id, payload) = frame.split_first()?;
    match stream_id {
        0x01 => Some(ExecEvent::Stdout(payload.to_vec())),
        0x02 => Some(ExecEvent::Stderr(payload.to_vec())),
        0x03 => Some(ExecEvent::Exit(decode_exit_code(payload))),
        _ => None,
    }
}

fn shell_event_from_ws_frame(frame: &[u8]) -> Option<ShellEvent> {
    let (&stream_id, payload) = frame.split_first()?;
    match stream_id {
        0x01 | 0x02 => Some(ShellEvent::Output(payload.to_vec())),
        0x03 => Some(ShellEvent::Exit(decode_exit_code(payload))),
        _ => None,
    }
}

fn decode_exit_code(payload: &[u8]) -> i32 {
    if payload.len() >= 4 {
        i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]])
    } else {
        0
    }
}

fn normalize_api_url(raw: &str) -> Result<String> {
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(SandboxError::InvalidEndpoint(
            "OpenComputer API URL must not be empty".to_string(),
        ));
    }
    Ok(if trimmed.ends_with("/api") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/api")
    })
}

fn websocket_api_url(api_url: &str) -> String {
    let base = api_url.trim_end_matches('/');
    if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        format!("wss://{base}")
    }
}

async fn decode_json<T>(response: reqwest::Response, action: &str) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    response.json().await.map_err(|err| {
        SandboxError::InvalidResponse(format!("failed to decode OpenComputer {action}: {err}"))
    })
}

async fn opencomputer_response_error(response: reqwest::Response) -> SandboxError {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    SandboxError::InvalidResponse(format!("OpenComputer request failed with {status}: {body}"))
}

fn delete_sandbox_status_ok(status: StatusCode) -> bool {
    status == StatusCode::NOT_FOUND || status == StatusCode::NO_CONTENT || status.is_success()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ResourceLimits, SharedMountAvailability, SharedMountContinuity};
    use serde_json::json;

    #[test]
    fn api_urls_match_opencomputer_control_plane_shape() {
        assert_eq!(
            normalize_api_url("https://app.opencomputer.dev").unwrap(),
            "https://app.opencomputer.dev/api"
        );
        assert_eq!(
            websocket_api_url("https://app.opencomputer.dev/api"),
            "wss://app.opencomputer.dev/api"
        );
    }

    #[test]
    fn ws_frames_decode_exec_events() {
        assert!(matches!(
            exec_event_from_ws_frame(&[0x01, b'o', b'k']),
            Some(ExecEvent::Stdout(bytes)) if bytes == b"ok"
        ));
        assert!(matches!(
            exec_event_from_ws_frame(&[0x03, 0, 0, 0, 7]),
            Some(ExecEvent::Exit(7))
        ));
    }

    #[test]
    fn explicit_session_resources_override_opencomputer_defaults() {
        let cfg = OpenComputerBackendConfig {
            default_cpu_count: Some(8),
            default_memory_mb: Some(16_384),
            default_disk_mb: Some(80 * 1024),
            ..OpenComputerBackendConfig::default()
        };
        let resources = ResourceLimits {
            vcpu: 1,
            memory_mb: 512,
            disk_gb: 2,
        };

        assert_eq!(
            resolve_resource_fields(Some(&resources), &cfg),
            ResourceFields {
                cpu_count: Some(1),
                memory_mb: Some(512),
                disk_mb: Some(2 * 1024)
            }
        );
    }

    #[test]
    fn opencomputer_defaults_apply_when_session_resources_are_absent() {
        let cfg = OpenComputerBackendConfig {
            default_cpu_count: Some(8),
            default_memory_mb: Some(16_384),
            default_disk_mb: Some(80 * 1024),
            ..OpenComputerBackendConfig::default()
        };

        assert_eq!(
            resolve_resource_fields(None, &cfg),
            ResourceFields {
                cpu_count: Some(8),
                memory_mb: Some(16_384),
                disk_mb: Some(80 * 1024)
            }
        );
    }

    #[test]
    fn opencomputer_omits_resource_fields_without_explicit_resources_or_defaults() {
        assert_eq!(
            resolve_resource_fields(None, &OpenComputerBackendConfig::default()),
            ResourceFields {
                cpu_count: None,
                memory_mb: None,
                disk_mb: None
            }
        );
    }

    #[test]
    fn create_sandbox_body_includes_egress_allowlist() {
        let body = CreateSandboxBody {
            template_id: Some("base".to_string()),
            timeout: Some(0),
            envs: None,
            metadata: None,
            burst: None,
            cpu_count: None,
            memory_mb: None,
            disk_mb: None,
            secret_store: None,
            egress_allowlist: Some(vec![
                "api.anthropic.com".to_string(),
                "*.openai.com".to_string(),
            ]),
        };

        assert_eq!(
            serde_json::to_value(body).unwrap(),
            json!({
                "templateID": "base",
                "timeout": 0,
                "egressAllowlist": ["api.anthropic.com", "*.openai.com"],
            })
        );
    }

    #[test]
    fn create_from_checkpoint_body_includes_resource_and_egress_fields() {
        let mut metadata = HashMap::new();
        metadata.insert("tenant_id".to_string(), "tenant".to_string());
        let body = CreateFromCheckpointBody {
            timeout: Some(60),
            envs: None,
            metadata: Some(metadata),
            burst: Some(false),
            cpu_count: Some(1),
            memory_mb: Some(4096),
            disk_mb: Some(20 * 1024),
            secret_store: None,
            egress_allowlist: Some(vec!["example.com".to_string()]),
        };

        assert_eq!(
            serde_json::to_value(body).unwrap(),
            json!({
                "timeout": 60,
                "metadata": {"tenant_id": "tenant"},
                "burst": false,
                "cpuCount": 1,
                "memoryMB": 4096,
                "diskMB": 20480,
                "egressAllowlist": ["example.com"],
            })
        );
    }

    #[test]
    fn delete_sandbox_statuses_are_idempotent_for_missing_sandboxes() {
        assert!(delete_sandbox_status_ok(StatusCode::NO_CONTENT));
        assert!(delete_sandbox_status_ok(StatusCode::OK));
        assert!(delete_sandbox_status_ok(StatusCode::NOT_FOUND));
        assert!(!delete_sandbox_status_ok(StatusCode::INTERNAL_SERVER_ERROR));
    }

    #[test]
    fn add_mount_body_matches_opencomputer_sdk_shape() {
        let mut creds = HashMap::new();
        creds.insert(
            "service_account_file".to_string(),
            "/run/keys/gcs.json".to_string(),
        );
        let mut config =
            OpenComputerMountConfig::rclone(" /mnt/data ", " gcs:bucket/prefix ", "GCS", creds);
        config.read_only = Some(false);
        config.mount_options = vec!["--dir-cache-time".to_string(), "1m".to_string()];

        let body = AddMountBody::from_config(config).expect("mount body");
        assert_eq!(
            serde_json::to_value(body).unwrap(),
            json!({
                "path": "/mnt/data",
                "remote": "gcs:bucket/prefix",
                "backend": "gcs",
                "creds": { "service_account_file": "/run/keys/gcs.json" },
                "readOnly": false,
                "mountOptions": ["--dir-cache-time", "1m"]
            })
        );
    }

    #[test]
    fn command_mount_body_matches_opencomputer_sdk_shape() {
        let mut config = OpenComputerMountConfig::command(
            " /mnt/data ",
            [
                "chevalier-vfs-fuse",
                "--endpoint",
                "https://api.example.test/vfs",
                "{mountpoint}",
            ],
        );
        config.env.insert("PLAIN".to_string(), "value".to_string());
        config.secrets.insert(
            "CHEVALIER_VFS_TOKEN".to_string(),
            "secret-token".to_string(),
        );
        config.read_only = Some(true);
        config.remote = "ignored-for-command".to_string();
        config.backend = Some("ignored".to_string());

        let body = AddMountBody::from_config(config).expect("mount body");
        assert_eq!(
            serde_json::to_value(body).unwrap(),
            json!({
                "path": "/mnt/data",
                "driver": "command",
                "command": [
                    "chevalier-vfs-fuse",
                    "--endpoint",
                    "https://api.example.test/vfs",
                    "{mountpoint}"
                ],
                "env": { "PLAIN": "value" },
                "secrets": { "CHEVALIER_VFS_TOKEN": "secret-token" },
                "readOnly": true
            })
        );
    }

    #[test]
    fn mount_config_deserializes_from_json_config() {
        let config: OpenComputerMountConfig = serde_json::from_value(json!({
            "path": "/nym/vm/mounts/task",
            "remote": "gcs:nym-vfs/task",
            "backend": "gcs",
            "creds": { "service_account_file": "/run/keys/gcs.json" },
            "rclone_config": null,
            "read_only": false,
            "mount_options": ["--dir-cache-time", "1m"]
        }))
        .expect("mount config");

        assert_eq!(config.path, "/nym/vm/mounts/task");
        assert_eq!(config.remote, "gcs:nym-vfs/task");
        assert_eq!(config.backend.as_deref(), Some("gcs"));
        assert_eq!(config.read_only, Some(false));
        assert_eq!(config.mount_options, ["--dir-cache-time", "1m"]);
    }

    #[test]
    fn command_mount_config_deserializes_from_json_config() {
        let config: OpenComputerMountConfig = serde_json::from_value(json!({
            "path": "",
            "driver": "command",
            "command": [
                "chevalier-vfs-fuse",
                "--endpoint",
                "{vfs_endpoint}",
                "--scope",
                "{vfs_scope_path}",
                "{mountpoint}"
            ],
            "env": { "CHEVALIER_VFS_SCOPE": "{vfs_scope_path}" },
            "secrets": { "CHEVALIER_VFS_TOKEN": "token" },
            "read_only": true
        }))
        .expect("mount config");

        assert_eq!(config.driver.as_deref(), Some("command"));
        assert!(config.remote.is_empty());
        assert_eq!(
            config.command.last().map(String::as_str),
            Some("{mountpoint}")
        );
        assert_eq!(
            config.env.get("CHEVALIER_VFS_SCOPE").map(String::as_str),
            Some("{vfs_scope_path}")
        );
        assert_eq!(config.read_only, Some(true));
    }

    #[test]
    fn mount_config_defaults_optional_collections_from_json_config() {
        let config: OpenComputerMountConfig = serde_json::from_value(json!({
            "path": "/mnt/data",
            "remote": "webdav:team"
        }))
        .expect("minimal mount config");

        assert_eq!(config.path, "/mnt/data");
        assert_eq!(config.remote, "webdav:team");
        assert!(config.creds.is_empty());
        assert!(config.mount_options.is_empty());
        assert_eq!(config.backend, None);
    }

    #[test]
    fn shared_mounts_require_and_apply_opencomputer_mapping() {
        let mut shared_mounts = HashMap::new();
        shared_mounts.insert(
            "task".to_string(),
            OpenComputerMountConfig {
                path: String::new(),
                remote: "gcs:nym-vfs/task".to_string(),
                backend: Some("gcs".to_string()),
                driver: None,
                command: Vec::new(),
                env: HashMap::new(),
                secrets: HashMap::new(),
                creds: HashMap::new(),
                rclone_config: None,
                read_only: None,
                mount_options: Vec::new(),
            },
        );
        let control = OpenComputerControl::new(OpenComputerBackendConfig {
            api_key: "test-key".to_string(),
            shared_mounts,
            ..OpenComputerBackendConfig::default()
        })
        .expect("control");

        let shared = SharedMount {
            host_path: String::new(),
            guest_path: "/nym/vm/mounts/task".to_string(),
            mount_tag: "task".to_string(),
            read_only: false,
            availability: SharedMountAvailability::SharedStorage,
            continuity: SharedMountContinuity::RestoreCrossNode,
            backend_profile: "gcs-fuse".to_string(),
            vfs_endpoint: "https://example.test/vfs".to_string(),
            vfs_scope_path: "/task".to_string(),
        };

        let resolved = control.resolve_mounts(&[shared]).expect("mapped mount");
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].path, "/nym/vm/mounts/task");
        assert_eq!(resolved[0].remote, "gcs:nym-vfs/task");
        assert_eq!(resolved[0].read_only, Some(false));
    }

    #[test]
    fn shared_command_mounts_render_vfs_placeholders() {
        let mut shared_mounts = HashMap::new();
        shared_mounts.insert(
            "gcs-vfs-fuse".to_string(),
            OpenComputerMountConfig::command(
                "",
                [
                    "chevalier-vfs-fuse",
                    "--endpoint",
                    "{vfs_endpoint}",
                    "--scope",
                    "{vfs_scope_path}",
                    "--tag",
                    "{mount_tag}",
                    "{mountpoint}",
                ],
            ),
        );
        let control = OpenComputerControl::new(OpenComputerBackendConfig {
            api_key: "test-key".to_string(),
            shared_mounts,
            ..OpenComputerBackendConfig::default()
        })
        .expect("control");

        let shared = SharedMount {
            host_path: String::new(),
            guest_path: "/nym/vm/mounts/task".to_string(),
            mount_tag: "task".to_string(),
            read_only: false,
            availability: SharedMountAvailability::SharedStorage,
            continuity: SharedMountContinuity::RestoreCrossNode,
            backend_profile: "gcs-vfs-fuse".to_string(),
            vfs_endpoint: "https://example.test/internal/chevalier/vfs/owner-1".to_string(),
            vfs_scope_path: "/conversations/abc/0001_assistant/mount".to_string(),
        };

        let resolved = control.resolve_mounts(&[shared]).expect("mapped mount");
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].path, "/nym/vm/mounts/task");
        assert_eq!(resolved[0].read_only, Some(false));
        assert_eq!(
            resolved[0].command,
            [
                "chevalier-vfs-fuse",
                "--endpoint",
                "https://example.test/internal/chevalier/vfs/owner-1",
                "--scope",
                "conversations/abc/0001_assistant/mount",
                "--tag",
                "task",
                "{mountpoint}"
            ]
        );

        let body =
            AddMountBody::from_config(resolved.into_iter().next().unwrap()).expect("command body");
        assert_eq!(
            serde_json::to_value(body).unwrap(),
            json!({
                "path": "/nym/vm/mounts/task",
                "driver": "command",
                "command": [
                    "chevalier-vfs-fuse",
                    "--endpoint",
                    "https://example.test/internal/chevalier/vfs/owner-1",
                    "--scope",
                    "conversations/abc/0001_assistant/mount",
                    "--tag",
                    "task",
                    "{mountpoint}"
                ],
                "readOnly": false
            })
        );
    }

    #[test]
    fn shared_mounts_without_mapping_are_rejected() {
        let control = OpenComputerControl::new(OpenComputerBackendConfig {
            api_key: "test-key".to_string(),
            ..OpenComputerBackendConfig::default()
        })
        .expect("control");
        let shared = SharedMount {
            host_path: String::new(),
            guest_path: "/nym/vm/mounts/task".to_string(),
            mount_tag: "task".to_string(),
            read_only: true,
            availability: SharedMountAvailability::SharedStorage,
            continuity: SharedMountContinuity::RestoreCrossNode,
            backend_profile: "gcs-fuse".to_string(),
            vfs_endpoint: "https://example.test/vfs".to_string(),
            vfs_scope_path: "/task".to_string(),
        };

        assert!(matches!(
            control.resolve_mounts(&[shared]),
            Err(SandboxError::Unsupported(message)) if message.contains("needs an OpenComputerBackendConfig.shared_mounts mapping")
        ));
    }
}
