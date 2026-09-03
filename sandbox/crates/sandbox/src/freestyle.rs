// @dive-file: Private Freestyle (freestyle.sh) transport backing the chevalier-sandbox facade.
// @dive-rel: Second provider-managed backend beside opencomputer.rs; reached only through
// @dive-rel: ControlBackend::Managed so consumers keep one sandbox API. Wire shapes follow the
// @dive-rel: public v5 OpenAPI spec (https://api.freestyle.sh/openapi.json).

use std::collections::HashMap;

use base64::Engine;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use uuid::Uuid;

use crate::proto::bracket::portproxy::v1::DirectoryEntry;
use crate::{
    ExecEvent, ExecHandle, ExecInput, ExecOptions, ForkOptions, ForkResult, FreestyleBackendConfig,
    ManagedMountConfig, Result, Sandbox, SandboxError, Session, SharedMount, ShellEvent,
    ShellHandle, ShellInput, ShellOptions, render_shared_mount_template,
};

/// Every session the facade creates carries this metadata key so `list_sessions`
/// can find them without a registry of its own.
const META_MANAGED_BY: &str = "chevalier.managed_by";
const MANAGED_BY_VALUE: &str = "chevalier-sandbox";
const META_SESSION_ID: &str = "chevalier.session_id";
const META_NAME: &str = "chevalier.name";
/// Guest-side marker directory: one file per mount tag records that the mount
/// command was launched in this boot, so attach does not relaunch it.
const MOUNT_STATE_DIR: &str = "/run/chevalier/mounts";
/// The mount script and unit are written into the guest so mounts come back on
/// every boot without the facade having to remember them.
const MOUNT_SCRIPT_PATH: &str = "/etc/chevalier/mounts.sh";
const MOUNT_UNIT_PATH: &str = "/etc/systemd/system/chevalier-mounts.service";
const MOUNT_UNIT: &str = "[Unit]\nDescription=Chevalier shared mounts\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=oneshot\nRemainAfterExit=yes\nExecStart=/bin/sh /etc/chevalier/mounts.sh\n\n[Install]\nWantedBy=multi-user.target\n";
/// Freestyle caps `exec-await` at five minutes of wall clock.
const EXEC_AWAIT_MAX_MS: u64 = 300_000;
/// Guest administration (units, mounts, poweroff) always runs as root regardless of
/// the session user.
const ROOT_USER: &str = "root";
const EXEC_AWAIT_DEFAULT_MS: u64 = 30_000;

#[derive(Clone)]
pub(crate) struct FreestyleControl {
    cfg: FreestyleBackendConfig,
    client: Client,
}

/// The subset of a Freestyle VM record the facade acts on.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct FreestyleVm {
    pub id: String,
    pub state: FreestyleVmState,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
    #[serde(default, rename = "displayName")]
    pub display_name: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum FreestyleVmState {
    Starting,
    Running,
    Pausing,
    Paused,
    Stopped,
}

impl FreestyleVmState {
    pub(crate) fn as_proto_state(self) -> i32 {
        use crate::proto::vmd::v1::VmState;
        match self {
            Self::Starting => VmState::Creating as i32,
            Self::Running => VmState::Running as i32,
            Self::Pausing | Self::Paused => VmState::Paused as i32,
            Self::Stopped => VmState::Stopped as i32,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FreestyleVmAction {
    Start,
    Pause,
    Stop,
}

impl FreestyleControl {
    pub(crate) fn new(mut cfg: FreestyleBackendConfig) -> Result<Self> {
        cfg.api_url = normalize_api_url(&cfg.api_url)?;
        if cfg.api_key.trim().is_empty() {
            return Err(SandboxError::InvalidConfig(
                "FREESTYLE_API_KEY is required when SandboxConfig.provider is Freestyle"
                    .to_string(),
            ));
        }
        cfg.snapshot_id = cfg.snapshot_id.trim().to_string();
        cfg.preview_domain_suffix = cfg
            .preview_domain_suffix
            .trim()
            .trim_start_matches('.')
            .to_string();
        if cfg.preview_domain_suffix.is_empty() {
            cfg.preview_domain_suffix = "style.dev".to_string();
        }
        Ok(Self {
            cfg,
            client: Client::new(),
        })
    }

    pub(crate) fn api_url(&self) -> &str {
        &self.cfg.api_url
    }

    /// Boot a VM for a new session. `image` is a snapshot id or slug; empty means the
    /// configured default. The returned record is the VM as the API sees it right after
    /// create, i.e. in `starting`; the facade does not wait for `running` because the
    /// first exec blocks until the guest answers anyway.
    pub(crate) async fn create_sandbox(
        &self,
        image: Option<String>,
        resources: Option<crate::ResourceLimits>,
        mut metadata: HashMap<String, String>,
        egress_allowlist: Option<Vec<String>>,
        shared_mounts: &[SharedMount],
    ) -> Result<FreestyleVm> {
        let snapshot_id = image
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .or_else(|| (!self.cfg.snapshot_id.is_empty()).then(|| self.cfg.snapshot_id.clone()));
        metadata.insert(META_MANAGED_BY.to_string(), MANAGED_BY_VALUE.to_string());
        let slug = metadata
            .get("chevalier.requested_session_id")
            .map(|value| session_slug(value));
        let display_name = metadata.get(META_NAME).cloned();
        let body = CreateVmBody {
            reassign_slug: slug.is_some(),
            snapshot_id,
            slug,
            display_name,
            metadata: metadata_within_limits(metadata),
            firewall: FirewallSpec::for_egress(
                egress_allowlist.or_else(|| self.cfg.egress_allowlist.clone()),
            ),
            tls: TlsSpec::for_egress_domains(self.cfg.egress_allowlist.as_deref()),
            idle_timeout_seconds: self.cfg.idle_timeout_secs.map(|value| value as i64),
            auto_delete_seconds: self.cfg.auto_delete_secs.map(|value| value as i64),
            automatic_restart: Some(true),
        };
        let response = self
            .send(self.client.post(self.url("/v5/vms")).json(&body))
            .await?;
        let vm: FreestyleVm = decode_json(response, "create vm").await?;
        let bootstrap = async {
            if let Some(resources) = resources {
                self.resize(&vm.id, &resources).await?;
            }
            self.ensure_configured_mounts(&vm.id, shared_mounts).await
        };
        if let Err(error) = bootstrap.await {
            // Never leave a half-configured VM behind: the caller has no id to reap it by.
            let _ = self.delete_sandbox(&vm.id).await;
            return Err(error);
        }
        Ok(vm)
    }

    pub(crate) async fn get_sandbox(&self, vm_id: &str) -> Result<FreestyleVm> {
        let response = self.send(self.client.get(self.vm_url(vm_id, ""))).await?;
        decode_json(response, "get vm").await
    }

    pub(crate) async fn delete_sandbox(&self, vm_id: &str) -> Result<()> {
        let response = self
            .client
            .delete(self.vm_url(vm_id, ""))
            .bearer_auth(&self.cfg.api_key)
            .send()
            .await
            .map_err(|err| {
                SandboxError::DaemonUnavailable(format!("Freestyle request failed: {err}"))
            })?;
        if response.status() == StatusCode::NOT_FOUND || response.status().is_success() {
            return Ok(());
        }
        Err(freestyle_response_error(response).await)
    }

    pub(crate) async fn vm_action(&self, vm_id: &str, action: FreestyleVmAction) -> Result<i32> {
        let vm = match action {
            FreestyleVmAction::Start => {
                let response = self
                    .send(self.client.post(self.vm_url(vm_id, "/start")))
                    .await?;
                decode_json::<FreestyleVm>(response, "start vm").await?
            }
            FreestyleVmAction::Pause => {
                let response = self
                    .send(self.client.post(self.vm_url(vm_id, "/pause")))
                    .await?;
                decode_json::<FreestyleVm>(response, "pause vm").await?
            }
            FreestyleVmAction::Stop => {
                // There is no stop endpoint: the guest powers itself off. The command
                // never returns cleanly because the machine goes away underneath it.
                let _ = self
                    .exec_await(
                        vm_id,
                        "systemctl poweroff || poweroff",
                        None,
                        Some(15_000),
                        None,
                        Some(ROOT_USER),
                    )
                    .await;
                self.get_sandbox(vm_id).await?
            }
        };
        Ok(vm.state.as_proto_state())
    }

    /// Resume a paused VM or boot a stopped one; a running VM is left alone.
    pub(crate) async fn ensure_running(&self, vm_id: &str) -> Result<()> {
        let vm = self.get_sandbox(vm_id).await?;
        match vm.state {
            FreestyleVmState::Running | FreestyleVmState::Starting => Ok(()),
            FreestyleVmState::Paused | FreestyleVmState::Pausing | FreestyleVmState::Stopped => {
                self.vm_action(vm_id, FreestyleVmAction::Start)
                    .await
                    .map(|_| ())
            }
        }
    }

    pub(crate) async fn list_sessions(&self) -> Result<Vec<crate::SessionInfo>> {
        let mut sessions = Vec::new();
        let mut offset = 0usize;
        loop {
            let response = self
                .send(self.client.get(self.url("/v5/vms")).query(&[
                    ("metadata", format!("{META_MANAGED_BY}:{MANAGED_BY_VALUE}")),
                    ("limit", "100".to_string()),
                    ("offset", offset.to_string()),
                ]))
                .await?;
            let page: ListVmsResponse = decode_json(response, "list vms").await?;
            let count = page.vms.len();
            for vm in page.vms {
                let Some(session_id) = vm.metadata.get(META_SESSION_ID).cloned() else {
                    continue;
                };
                sessions.push(crate::SessionInfo {
                    session_id,
                    vm_id: vm.id,
                    name: vm.display_name.unwrap_or_default(),
                    state: vm.state.as_proto_state(),
                    parent_session_id: vm.metadata.get("chevalier.parent_session_id").cloned(),
                    fork_id: vm.metadata.get("chevalier.fork_id").cloned(),
                });
            }
            offset += count;
            if count == 0 || offset >= page.total_count {
                break;
            }
        }
        Ok(sessions)
    }

    /// Find a VM by the logical session id the facade stamped into its metadata.
    pub(crate) async fn find_by_session_id(&self, session_id: &str) -> Result<Option<FreestyleVm>> {
        let response = self
            .send(self.client.get(self.url("/v5/vms")).query(&[
                ("metadata", format!("{META_SESSION_ID}:{session_id}")),
                ("limit", "2".to_string()),
            ]))
            .await?;
        let page: ListVmsResponse = decode_json(response, "list vms").await?;
        Ok(page.vms.into_iter().find(|vm| {
            vm.metadata
                .get(META_SESSION_ID)
                .is_some_and(|value| value == session_id)
        }))
    }

    // ----- files -----

    pub(crate) async fn read_file(&self, vm_id: &str, path: &str) -> Result<Vec<u8>> {
        let response = self
            .send(
                self.client
                    .get(self.vm_url(vm_id, "/fs/read"))
                    .query(&[("path", path)]),
            )
            .await?;
        let bytes = response
            .bytes()
            .await
            .map_err(|err| SandboxError::InvalidResponse(format!("read file body: {err}")))?;
        Ok(bytes.to_vec())
    }

    pub(crate) async fn write_file(&self, vm_id: &str, path: &str, data: Vec<u8>) -> Result<()> {
        if let Some(parent) = std::path::Path::new(path).parent() {
            let parent = parent.to_string_lossy();
            if !parent.is_empty() && parent != "/" {
                self.mkdir(vm_id, &parent).await?;
            }
        }
        self.send_empty(
            self.client
                .put(self.vm_url(vm_id, "/fs/write"))
                .query(&[("path", path)])
                .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
                .body(data),
        )
        .await
    }

    pub(crate) async fn mkdir(&self, vm_id: &str, path: &str) -> Result<()> {
        self.send_empty(
            self.client
                .post(self.vm_url(vm_id, "/fs/mkdir"))
                .json(&MakeDirBody { path }),
        )
        .await
    }

    pub(crate) async fn list_dir(&self, vm_id: &str, path: &str) -> Result<Vec<DirectoryEntry>> {
        let response = self
            .send(
                self.client
                    .get(self.vm_url(vm_id, "/fs/dir"))
                    .query(&[("path", path)]),
            )
            .await?;
        let listing: ReadDirResponse = decode_json(response, "list directory").await?;
        Ok(listing
            .entries
            .into_iter()
            .map(|entry| DirectoryEntry {
                name: entry.name,
                is_dir: entry.kind == "directory",
                is_symlink: entry.kind == "symlink",
            })
            .collect())
    }

    pub(crate) async fn delete_path(&self, vm_id: &str, path: &str) -> Result<()> {
        self.send_empty(
            self.client
                .delete(self.vm_url(vm_id, "/fs/remove"))
                .query(&[("path", path)]),
        )
        .await
    }

    // ----- exec / shell -----

    /// Run a command and stream its (buffered) result as facade events. Freestyle's
    /// `exec-await` returns whole stdout/stderr after the command exits, so the handle
    /// yields at most one stdout chunk, one stderr chunk, and an exit or timeout event.
    /// Interactive stdin is not available on this transport; `detach` launches the
    /// command under `nohup` and reports exit 0 once it is running.
    pub(crate) async fn exec(
        &self,
        vm_id: &str,
        command: &str,
        opts: ExecOptions,
    ) -> Result<ExecHandle> {
        let shell = opts.shell.unwrap_or_else(|| "/bin/sh".to_string());
        let timeout_ms = opts
            .timeout_secs
            .and_then(|secs| u64::try_from(secs).ok())
            .map(|secs| secs.saturating_mul(1000))
            .unwrap_or(EXEC_AWAIT_DEFAULT_MS)
            .clamp(1, EXEC_AWAIT_MAX_MS);
        let wrapped = if opts.detach {
            detached_command(&shell, command)
        } else {
            shell_command(&shell, command)
        };
        let result = self
            .exec_await(
                vm_id,
                &wrapped,
                (!opts.env.is_empty()).then_some(opts.env),
                Some(timeout_ms),
                None,
                None,
            )
            .await?;

        let (input_tx, _input_rx) = mpsc::channel::<ExecInput>(1);
        let (event_tx, event_rx) = mpsc::channel(4);
        let stdout = result.stdout.unwrap_or_default();
        let stderr = result.stderr.unwrap_or_default();
        if !stdout.is_empty() {
            let _ = event_tx
                .send(Ok(ExecEvent::Stdout(stdout.into_bytes())))
                .await;
        }
        if !stderr.is_empty() {
            let _ = event_tx
                .send(Ok(ExecEvent::Stderr(stderr.into_bytes())))
                .await;
        }
        let terminal = match result.status_code {
            Some(code) => ExecEvent::Exit(code),
            None => ExecEvent::Timeout,
        };
        let _ = event_tx.send(Ok(terminal)).await;
        drop(event_tx);
        Ok(ExecHandle {
            input: input_tx,
            events: Box::pin(ReceiverStream::new(event_rx)),
        })
    }

    /// `linux_user`: `Some(ROOT_USER)` for guest administration (units, mounts), `None`
    /// for the configured session user.
    pub(crate) async fn exec_await(
        &self,
        vm_id: &str,
        command: &str,
        env: Option<HashMap<String, String>>,
        timeout_ms: Option<u64>,
        stdin: Option<&[u8]>,
        linux_user: Option<&str>,
    ) -> Result<ExecAwaitResponse> {
        let body = ExecAwaitBody {
            command,
            env,
            timeout_ms: timeout_ms.map(|value| value.clamp(1, EXEC_AWAIT_MAX_MS)),
            stdin: stdin.map(|bytes| base64::engine::general_purpose::STANDARD.encode(bytes)),
            linux_user: linux_user.or(self.cfg.linux_user.as_deref()),
        };
        let response = self
            .send(
                self.client
                    .post(self.vm_url(vm_id, "/exec-await"))
                    .json(&body),
            )
            .await?;
        decode_json(response, "exec-await").await
    }

    /// Open an interactive terminal over the PTY WebSocket. Binary frames are terminal
    /// bytes in both directions; text frames are JSON control messages.
    pub(crate) async fn shell(&self, vm_id: &str, opts: ShellOptions) -> Result<ShellHandle> {
        let mut query: Vec<(String, String)> = Vec::new();
        let program = opts.shell.unwrap_or_else(|| "bash".to_string());
        let mut exec_line = shell_words_join(
            std::iter::once(program.as_str()).chain(opts.args.iter().map(String::as_str)),
        );
        if let Some(cwd) = opts.cwd.as_deref().filter(|cwd| !cwd.trim().is_empty()) {
            exec_line = format!("cd {} && exec {}", shell_quote(cwd), exec_line);
        }
        if !opts.env.is_empty() {
            let exports = opts
                .env
                .iter()
                .map(|(key, value)| format!("{key}={}", shell_quote(value)))
                .collect::<Vec<_>>()
                .join(" ");
            exec_line = format!("env {exports} {exec_line}");
        }
        query.push(("exec".to_string(), exec_line));
        if let Some(cols) = opts.cols {
            query.push(("cols".to_string(), cols.to_string()));
        }
        if let Some(rows) = opts.rows {
            query.push(("rows".to_string(), rows.to_string()));
        }
        if let Some(user) = self.cfg.linux_user.as_deref() {
            query.push(("linuxUser".to_string(), user.to_string()));
        }
        let ws_url = format!(
            "{}/v5/vms/{}/pty?{}",
            websocket_api_url(&self.cfg.api_url),
            urlencoding::encode(vm_id),
            query
                .iter()
                .map(|(key, value)| format!("{key}={}", urlencoding::encode(value)))
                .collect::<Vec<_>>()
                .join("&")
        );
        let mut request = ws_url
            .as_str()
            .into_client_request()
            .map_err(|err| SandboxError::InvalidEndpoint(format!("Freestyle pty url: {err}")))?;
        request.headers_mut().insert(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", self.cfg.api_key)
                .parse()
                .map_err(|err| {
                    SandboxError::InvalidConfig(format!("Freestyle api key header: {err}"))
                })?,
        );
        let (ws, response) = connect_async(request)
            .await
            .map_err(|err| SandboxError::DaemonUnavailable(format!("Freestyle pty ws: {err}")))?;
        if response.status() != StatusCode::SWITCHING_PROTOCOLS {
            return Err(SandboxError::InvalidResponse(format!(
                "Freestyle pty websocket returned {}",
                response.status()
            )));
        }
        let (mut write, mut read) = ws.split();
        let (input_tx, mut input_rx) = mpsc::channel(64);
        let (event_tx, event_rx) = mpsc::channel(128);

        tokio::spawn(async move {
            while let Some(input) = input_rx.recv().await {
                let frame = match input {
                    ShellInput::Data(data) => Message::Binary(Bytes::from(data)),
                    ShellInput::Resize { cols, rows } => Message::Text(
                        serde_json::json!({ "type": "resize", "cols": cols, "rows": rows })
                            .to_string()
                            .into(),
                    ),
                    ShellInput::Eof => {
                        // The PTY has no half-close; end-of-input for a shell is EOT.
                        Message::Binary(Bytes::from_static(&[0x04]))
                    }
                };
                if write.send(frame).await.is_err() {
                    break;
                }
            }
            let _ = write.close().await;
        });

        tokio::spawn(async move {
            while let Some(message) = read.next().await {
                match message {
                    Ok(Message::Binary(data)) => {
                        if event_tx
                            .send(Ok(ShellEvent::Output(data.to_vec())))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(Message::Text(text)) => match pty_control_event(&text) {
                        Some(PtyControl::Exited(code)) => {
                            let _ = event_tx.send(Ok(ShellEvent::Exit(code))).await;
                            break;
                        }
                        Some(PtyControl::Error(message)) => {
                            let _ = event_tx
                                .send(Err(SandboxError::InvalidResponse(format!(
                                    "Freestyle pty: {message}"
                                ))))
                                .await;
                            break;
                        }
                        Some(PtyControl::SessionInfo) | None => {}
                    },
                    Ok(Message::Close(_)) => {
                        let _ = event_tx.send(Ok(ShellEvent::Exit(0))).await;
                        break;
                    }
                    Ok(_) => {}
                    Err(err) => {
                        let _ = event_tx
                            .send(Err(SandboxError::DaemonUnavailable(format!(
                                "Freestyle pty websocket failed: {err}"
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

    // ----- snapshots / fork -----

    /// Capture memory and disk. The snapshot is a full resume point, so a VM created
    /// from it continues exactly where this one was paused.
    pub(crate) async fn create_checkpoint(
        &self,
        vm_id: &str,
        name: &str,
    ) -> Result<CheckpointInfo> {
        let body = SnapshotBody {
            display_name: Some(name.to_string()),
            slug: None,
            auto_delete_seconds: self.cfg.snapshot_auto_delete_secs.map(|value| value as i64),
        };
        let response = self
            .send(
                self.client
                    .post(self.vm_url(vm_id, "/snapshot"))
                    .json(&body),
            )
            .await?;
        let created: SnapshotResponse = decode_json(response, "create snapshot").await?;
        Ok(CheckpointInfo {
            id: created.snapshot_id,
        })
    }

    pub(crate) async fn delete_checkpoint(&self, checkpoint_id: &str) -> Result<()> {
        let response = self
            .client
            .delete(self.url(&format!(
                "/v5/snapshots/{}",
                urlencoding::encode(checkpoint_id)
            )))
            .bearer_auth(&self.cfg.api_key)
            .send()
            .await
            .map_err(|err| {
                SandboxError::DaemonUnavailable(format!("Freestyle request failed: {err}"))
            })?;
        if response.status() == StatusCode::NOT_FOUND || response.status().is_success() {
            return Ok(());
        }
        Err(freestyle_response_error(response).await)
    }

    pub(crate) async fn create_from_checkpoint(
        &self,
        checkpoint_id: &str,
        metadata: HashMap<String, String>,
        egress_allowlist: Option<Vec<String>>,
        shared_mounts: &[SharedMount],
    ) -> Result<FreestyleVm> {
        self.create_sandbox(
            Some(checkpoint_id.to_string()),
            None,
            metadata,
            egress_allowlist,
            shared_mounts,
        )
        .await
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
        let mut child_metadata = opts.child_metadata;
        let child_session_id = Uuid::new_v4().to_string();
        child_metadata.insert(META_SESSION_ID.to_string(), child_session_id.clone());
        child_metadata.insert(
            "chevalier.requested_session_id".to_string(),
            child_session_id.clone(),
        );
        child_metadata.insert(
            "chevalier.parent_session_id".to_string(),
            parent.session_id().to_string(),
        );
        child_metadata.insert("chevalier.fork_id".to_string(), checkpoint.id.clone());
        let child = self
            .create_from_checkpoint(
                checkpoint.id.as_str(),
                child_metadata,
                None,
                parent.shared_mounts.as_slice(),
            )
            .await?;
        let session = Session::new_with_backend(
            sandbox,
            child_session_id.clone(),
            child.id,
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

    // ----- mounts -----

    /// Launch every shared mount's command inside the guest, once per boot. Mount
    /// commands are the same `command` driver shape OpenComputer uses (an argv run
    /// through `sh -lc` with `{placeholder}` rendering); the daemon is left running
    /// under `nohup` with its own stdio so the exec returns immediately.
    pub(crate) async fn ensure_configured_mounts(
        &self,
        vm_id: &str,
        shared_mounts: &[SharedMount],
    ) -> Result<()> {
        let mounts = self.resolve_mounts(shared_mounts)?;
        if mounts.is_empty() {
            // Attach passes no mounts: re-run whatever this guest already persisted.
            let result = self
                .exec_await(
                    vm_id,
                    &format!("[ -f {MOUNT_SCRIPT_PATH} ] && /bin/sh {MOUNT_SCRIPT_PATH} || true"),
                    None,
                    Some(120_000),
                    None,
                    Some(ROOT_USER),
                )
                .await?;
            return match result.status_code {
                Some(0) => Ok(()),
                _ => Err(SandboxError::InvalidResponse(format!(
                    "Freestyle mount replay failed: {}",
                    result.stderr.unwrap_or_default().trim()
                ))),
            };
        }
        let mut script = String::from("set -e\n");
        script.push_str(&format!("mkdir -p {MOUNT_STATE_DIR}\n"));
        for mount in mounts {
            script.push_str(&mount.launch_script());
        }
        self.write_file(vm_id, MOUNT_SCRIPT_PATH, script.into_bytes())
            .await?;
        self.write_file(vm_id, MOUNT_UNIT_PATH, MOUNT_UNIT.as_bytes().to_vec())
            .await?;
        let result = self
            .exec_await(
                vm_id,
                &format!(
                    "chmod 600 {MOUNT_SCRIPT_PATH} && systemctl daemon-reload && systemctl enable --now chevalier-mounts.service && systemctl is-active chevalier-mounts.service"
                ),
                None,
                Some(120_000),
                None, Some(ROOT_USER))
            .await?;
        match result.status_code {
            Some(0) => Ok(()),
            Some(code) => Err(SandboxError::InvalidResponse(format!(
                "Freestyle mount bootstrap exited with status {code}: {}",
                result.stderr.unwrap_or_default().trim()
            ))),
            None => Err(SandboxError::DaemonUnavailable(
                "Freestyle mount bootstrap timed out".to_string(),
            )),
        }
    }

    fn resolve_mounts(&self, shared_mounts: &[SharedMount]) -> Result<Vec<RenderedMount>> {
        let mut rendered = Vec::with_capacity(shared_mounts.len());
        for shared in shared_mounts {
            let template = self.shared_mount_template(shared)?;
            rendered.push(RenderedMount::render(template, shared));
        }
        Ok(rendered)
    }

    fn shared_mount_template(&self, shared: &SharedMount) -> Result<&ManagedMountConfig> {
        let backend_profile = crate::normalize_mount_backend_profile(&shared.backend_profile);
        self.cfg
            .shared_mounts
            .get(shared.mount_tag.as_str())
            .or_else(|| self.cfg.shared_mounts.get(shared.guest_path.as_str()))
            .or_else(|| self.cfg.shared_mounts.get(backend_profile.as_str()))
            .ok_or_else(|| {
                SandboxError::Unsupported(format!(
                    "Freestyle shared mount `{}` at `{}` needs a FreestyleBackendConfig.shared_mounts mapping by mount tag, guest path, or backend profile",
                    shared.mount_tag, shared.guest_path
                ))
            })
    }

    // ----- ingress / preview -----

    /// Public HTTPS entry to a guest port. Freestyle terminates TLS at its edge and
    /// forwards plaintext to the VM; the hostname is claimed on first use under the
    /// configured suffix (`style.dev` by default, no verification needed). The rule is
    /// created once per (vm, port) and found again by domain on later calls.
    pub(crate) async fn preview_url(&self, vm_id: &str, guest_port: u16) -> Result<String> {
        let domain = self.preview_domain(vm_id, guest_port);
        let existing = self
            .send(
                self.client
                    .get(self.url("/v5/tls"))
                    .query(&[("domain", domain.as_str())]),
            )
            .await?;
        let rules: ListTlsRulesResponse = decode_json(existing, "list tls rules").await?;
        let matches = rules.rules.iter().any(|rule| {
            rule.domain == domain
                && rule.destination.vm_id.as_deref() == Some(vm_id)
                && rule.destination.port == Some(guest_port)
        });
        if !matches {
            let body = CreateTlsRuleBody {
                action: "allow",
                domain: domain.clone(),
                source: TlsEndpoint::public(),
                destination: TlsEndpoint::vm_port(vm_id, guest_port),
                protocol: "http",
                forward_auth: self
                    .cfg
                    .forward_auth_id
                    .as_deref()
                    .map(|id| TlsForwardAuthRef { id: id.to_string() }),
            };
            let response = self
                .send(self.client.post(self.url("/v5/tls")).json(&body))
                .await?;
            let _rule: TlsRule = decode_json(response, "create tls rule").await?;
        }
        Ok(format!("https://{domain}"))
    }

    fn preview_domain(&self, vm_id: &str, guest_port: u16) -> String {
        let label = vm_id
            .trim_start_matches("vm-")
            .replace(|c: char| !c.is_ascii_alphanumeric(), "");
        let label: String = label.chars().take(40).collect();
        format!(
            "nym-{label}-p{guest_port}.{}",
            self.cfg.preview_domain_suffix
        )
    }

    async fn resize(&self, vm_id: &str, resources: &crate::ResourceLimits) -> Result<()> {
        let body = ResizeBody {
            cpu: u32::try_from(resources.vcpu)
                .ok()
                .filter(|value| *value > 0),
            memory: u32::try_from(resources.memory_mb)
                .ok()
                .filter(|value| *value > 0),
            storage: u32::try_from(resources.disk_gb)
                .ok()
                .filter(|value| *value > 0)
                .and_then(|gb| gb.checked_mul(1024)),
        };
        if body.cpu.is_none() && body.memory.is_none() && body.storage.is_none() {
            return Ok(());
        }
        // Resize is grow-only; asking for less than the snapshot's size is a 400 the
        // facade should not treat as fatal for session creation.
        match self
            .send(self.client.post(self.vm_url(vm_id, "/resize")).json(&body))
            .await
        {
            Ok(_) => Ok(()),
            Err(SandboxError::InvalidResponse(message)) if message.contains("400") => Ok(()),
            Err(err) => Err(err),
        }
    }

    // ----- plumbing -----

    fn url(&self, suffix: &str) -> String {
        format!(
            "{}/{}",
            self.cfg.api_url.trim_end_matches('/'),
            suffix.trim_start_matches('/')
        )
    }

    fn vm_url(&self, vm_id: &str, suffix: &str) -> String {
        format!(
            "{}/v5/vms/{}{}",
            self.cfg.api_url.trim_end_matches('/'),
            urlencoding::encode(vm_id),
            suffix
        )
    }

    async fn send(&self, builder: reqwest::RequestBuilder) -> Result<reqwest::Response> {
        let response = builder
            .bearer_auth(&self.cfg.api_key)
            .send()
            .await
            .map_err(|err| {
                SandboxError::DaemonUnavailable(format!("Freestyle request failed: {err}"))
            })?;
        if response.status().is_success() {
            return Ok(response);
        }
        Err(freestyle_response_error(response).await)
    }

    async fn send_empty(&self, builder: reqwest::RequestBuilder) -> Result<()> {
        self.send(builder).await.map(|_| ())
    }
}

// ----- guest-side mount rendering -----

/// A shared mount's launch command after `{placeholder}` rendering.
#[derive(Clone, Debug, PartialEq, Eq)]
struct RenderedMount {
    mount_tag: String,
    mountpoint: String,
    command: Vec<String>,
    env: HashMap<String, String>,
    read_only: bool,
}

impl RenderedMount {
    fn render(template: &ManagedMountConfig, shared: &SharedMount) -> Self {
        let mountpoint = if template.path.trim().is_empty() {
            shared.guest_path.clone()
        } else {
            render_shared_mount_template(&template.path, shared, Some(&shared.guest_path))
        };
        let render = |value: &str| render_shared_mount_template(value, shared, Some(&mountpoint));
        let command = template.command.iter().map(|value| render(value)).collect();
        let mut env: HashMap<String, String> = template
            .env
            .iter()
            .map(|(key, value)| (key.clone(), render(value)))
            .collect();
        for (key, value) in &template.secrets {
            env.insert(key.clone(), render(value));
        }
        Self {
            mount_tag: shared.mount_tag.clone(),
            mountpoint,
            command,
            env,
            read_only: template.read_only.unwrap_or(shared.read_only),
        }
    }

    /// Shell lines that start this mount's daemon once per boot and record it.
    fn launch_script(&self) -> String {
        let marker = format!("{MOUNT_STATE_DIR}/{}", sanitize_tag(&self.mount_tag));
        let mut exports = self
            .env
            .iter()
            .map(|(key, value)| format!("export {key}={}", shell_quote(value)))
            .collect::<Vec<_>>();
        exports.sort();
        let argv = shell_words_join(self.command.iter().map(String::as_str));
        format!(
            "if [ ! -e {marker} ]; then\n  mkdir -p {mountpoint}\n  {exports}\n  export CHEVALIER_VFS_READ_ONLY={read_only}\n  nohup {argv} >{log} 2>&1 </dev/null &\n  echo $! > {marker}\nfi\n",
            marker = shell_quote(&marker),
            mountpoint = shell_quote(&self.mountpoint),
            exports = if exports.is_empty() {
                ":".to_string()
            } else {
                exports.join("\n  ")
            },
            read_only = if self.read_only { "true" } else { "false" },
            argv = argv,
            log = shell_quote(&format!("{marker}.log")),
        )
    }
}

fn sanitize_tag(tag: &str) -> String {
    tag.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// `sh -c`-safe single quoting.
fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn shell_words_join<'a>(words: impl IntoIterator<Item = &'a str>) -> String {
    words
        .into_iter()
        .map(shell_quote)
        .collect::<Vec<_>>()
        .join(" ")
}

fn shell_command(shell: &str, command: &str) -> String {
    format!("{} -lc {}", shell_quote(shell), shell_quote(command))
}

fn detached_command(shell: &str, command: &str) -> String {
    format!(
        "nohup {} -lc {} >/dev/null 2>&1 </dev/null & echo $!",
        shell_quote(shell),
        shell_quote(command)
    )
}

/// Freestyle's slug rules: 1–63 chars of `[a-z0-9-]`, no leading/trailing/repeated `-`.
fn session_slug(session_id: &str) -> String {
    let mut slug = String::with_capacity(63);
    let mut last_dash = true;
    for c in session_id.chars().flat_map(char::to_lowercase) {
        let c = if c.is_ascii_alphanumeric() { c } else { '-' };
        if c == '-' {
            if last_dash {
                continue;
            }
            last_dash = true;
        } else {
            last_dash = false;
        }
        slug.push(c);
        if slug.len() == 63 {
            break;
        }
    }
    let slug = slug.trim_end_matches('-').to_string();
    if slug.is_empty() {
        format!("s-{}", Uuid::new_v4().simple())
    } else {
        slug
    }
}

/// Metadata is capped at 64 entries of 63-char keys/values; drop what cannot fit
/// rather than failing the create.
fn metadata_within_limits(metadata: HashMap<String, String>) -> HashMap<String, String> {
    let mut entries: Vec<(String, String)> = metadata
        .into_iter()
        .filter(|(key, value)| key.len() <= 63 && value.len() <= 63)
        .collect();
    entries.sort();
    entries.truncate(64);
    entries.into_iter().collect()
}

// ----- wire shapes -----

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateVmBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    slug: Option<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    reassign_slug: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
    metadata: HashMap<String, String>,
    firewall: FirewallSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    tls: Option<TlsSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    idle_timeout_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    auto_delete_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    automatic_restart: Option<bool>,
}

#[derive(Serialize)]
struct FirewallSpec {
    rules: Vec<FirewallRuleSpec>,
}

impl FirewallSpec {
    /// No allowlist means open egress. An allowlist means L3 egress stays closed and
    /// named hosts are reached through TLS rules instead (see `TlsSpec`).
    fn for_egress(allowlist: Option<Vec<String>>) -> Self {
        match allowlist {
            None => Self {
                rules: vec![FirewallRuleSpec {
                    action: "allow",
                    source: FirewallEndpoint::default(),
                    destination: FirewallEndpoint {
                        public: Some(true),
                        ..FirewallEndpoint::default()
                    },
                }],
            },
            Some(_) => Self { rules: Vec::new() },
        }
    }
}

#[derive(Serialize)]
struct FirewallRuleSpec {
    action: &'static str,
    source: FirewallEndpoint,
    destination: FirewallEndpoint,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct FirewallEndpoint {
    #[serde(skip_serializing_if = "Option::is_none")]
    public: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cidr: Option<String>,
}

#[derive(Serialize)]
struct TlsSpec {
    rules: Vec<CreateTlsRuleBody>,
}

impl TlsSpec {
    fn for_egress_domains(allowlist: Option<&[String]>) -> Option<Self> {
        let domains = allowlist?;
        let rules = domains
            .iter()
            .filter(|domain| !domain.starts_with("__"))
            .map(|domain| CreateTlsRuleBody {
                action: "allow",
                domain: domain.clone(),
                source: TlsEndpoint::default(),
                destination: TlsEndpoint::public(),
                protocol: "tcp",
                forward_auth: None,
            })
            .collect();
        Some(Self { rules })
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateTlsRuleBody {
    action: &'static str,
    domain: String,
    source: TlsEndpoint,
    destination: TlsEndpoint,
    protocol: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    forward_auth: Option<TlsForwardAuthRef>,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TlsEndpoint {
    #[serde(skip_serializing_if = "Option::is_none")]
    public: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    vm_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
}

impl TlsEndpoint {
    fn public() -> Self {
        Self {
            public: Some(true),
            ..Self::default()
        }
    }

    fn vm_port(vm_id: &str, port: u16) -> Self {
        Self {
            vm_id: Some(vm_id.to_string()),
            port: Some(port),
            ..Self::default()
        }
    }
}

#[derive(Serialize)]
struct TlsForwardAuthRef {
    id: String,
}

#[derive(Deserialize)]
struct TlsRule {
    #[allow(dead_code)]
    id: String,
    domain: String,
    destination: TlsEndpoint,
}

#[derive(Deserialize)]
struct ListTlsRulesResponse {
    #[serde(default)]
    rules: Vec<TlsRule>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ResizeBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    cpu: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    memory: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    storage: Option<u32>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExecAwaitBody<'a> {
    command: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    env: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stdin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    linux_user: Option<&'a str>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExecAwaitResponse {
    pub status_code: Option<i32>,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
}

#[derive(Serialize)]
struct MakeDirBody<'a> {
    path: &'a str,
}

#[derive(Deserialize)]
struct ReadDirResponse {
    entries: Vec<DirEntry>,
}

#[derive(Deserialize)]
struct DirEntry {
    name: String,
    kind: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListVmsResponse {
    vms: Vec<FreestyleVm>,
    #[serde(default)]
    total_count: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    slug: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    auto_delete_seconds: Option<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotResponse {
    snapshot_id: String,
}

#[derive(Deserialize)]
pub(crate) struct CheckpointInfo {
    pub(crate) id: String,
}

enum PtyControl {
    Exited(i32),
    Error(String),
    SessionInfo,
}

fn pty_control_event(text: &str) -> Option<PtyControl> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    match value.get("type").and_then(serde_json::Value::as_str)? {
        "exited" => Some(PtyControl::Exited(
            value
                .get("exitCode")
                .and_then(serde_json::Value::as_i64)
                .and_then(|code| i32::try_from(code).ok())
                .unwrap_or(0),
        )),
        "error" => Some(PtyControl::Error(
            value
                .get("message")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown error")
                .to_string(),
        )),
        "sessionInfo" => Some(PtyControl::SessionInfo),
        _ => None,
    }
}

fn normalize_api_url(raw: &str) -> Result<String> {
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(SandboxError::InvalidEndpoint(
            "Freestyle API URL must not be empty".to_string(),
        ));
    }
    if !(trimmed.starts_with("https://") || trimmed.starts_with("http://")) {
        return Err(SandboxError::InvalidEndpoint(format!(
            "Freestyle API URL must be absolute: {trimmed}"
        )));
    }
    Ok(trimmed.to_string())
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
        SandboxError::InvalidResponse(format!("failed to decode Freestyle {action}: {err}"))
    })
}

/// Every Freestyle error is `{code, message}`; the code is stable and worth keeping
/// in the message so callers can branch on it (`VM_NOT_FOUND`, `RATE_LIMITED`, …).
async fn freestyle_response_error(response: reqwest::Response) -> SandboxError {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let code = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|value| {
            value
                .get("code")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        });
    match (status, code.as_deref()) {
        (StatusCode::NOT_FOUND, _) => SandboxError::SessionNotFound(format!("Freestyle: {body}")),
        _ => SandboxError::InvalidResponse(format!(
            "Freestyle request failed with {status}{}: {body}",
            code.map(|code| format!(" [{code}]")).unwrap_or_default()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SharedMountAvailability, SharedMountContinuity};
    use serde_json::json;

    fn shared_mount() -> SharedMount {
        SharedMount {
            host_path: String::new(),
            guest_path: "/mnt/nymfs".to_string(),
            mount_tag: "nymfs-root".to_string(),
            read_only: false,
            availability: SharedMountAvailability::SharedStorage,
            continuity: SharedMountContinuity::RestoreCrossNode,
            backend_profile: "gcs-vfs-fuse".to_string(),
            vfs_endpoint: "https://api.example.com/v1/runtime/internal/vfs/owner".to_string(),
            vfs_scope_path: "/nyms/abc/".to_string(),
        }
    }

    #[test]
    fn api_urls_are_absolute_and_websocket_scheme_follows() {
        assert_eq!(
            normalize_api_url("https://api.freestyle.sh/").unwrap(),
            "https://api.freestyle.sh"
        );
        assert!(normalize_api_url("api.freestyle.sh").is_err());
        assert_eq!(
            websocket_api_url("https://api.freestyle.sh"),
            "wss://api.freestyle.sh"
        );
    }

    #[test]
    fn create_body_uses_v5_shape_with_open_egress_by_default() {
        let body = CreateVmBody {
            snapshot_id: Some("sh-1".to_string()),
            slug: Some("nym-abc".to_string()),
            reassign_slug: true,
            display_name: None,
            metadata: HashMap::from([("chevalier.session_id".to_string(), "abc".to_string())]),
            firewall: FirewallSpec::for_egress(None),
            tls: TlsSpec::for_egress_domains(None),
            idle_timeout_seconds: Some(900),
            auto_delete_seconds: None,
            automatic_restart: Some(true),
        };
        assert_eq!(
            serde_json::to_value(body).unwrap(),
            json!({
                "snapshotId": "sh-1",
                "slug": "nym-abc",
                "reassignSlug": true,
                "metadata": {"chevalier.session_id": "abc"},
                "firewall": {"rules": [{"action": "allow", "source": {}, "destination": {"public": true}}]},
                "idleTimeoutSeconds": 900,
                "automaticRestart": true
            })
        );
    }

    #[test]
    fn egress_allowlist_closes_l3_and_opens_named_tls_sessions() {
        let allow = Some(vec![
            "api.openai.com".to_string(),
            "__chevalier_no_hosts_allowed__".to_string(),
        ]);
        let firewall = FirewallSpec::for_egress(allow.clone());
        assert!(firewall.rules.is_empty());
        let tls = TlsSpec::for_egress_domains(allow.as_deref()).unwrap();
        assert_eq!(tls.rules.len(), 1);
        assert_eq!(tls.rules[0].domain, "api.openai.com");
        assert_eq!(
            serde_json::to_value(&tls.rules[0]).unwrap()["destination"],
            json!({"public": true})
        );
    }

    #[test]
    fn exec_wrapping_quotes_through_the_login_shell() {
        assert_eq!(
            shell_command("/bin/sh", "echo 'hi'"),
            "'/bin/sh' -lc 'echo '\\''hi'\\'''"
        );
        assert!(
            detached_command("/bin/bash", "sleep 100")
                .starts_with("nohup '/bin/bash' -lc 'sleep 100' >/dev/null")
        );
    }

    #[test]
    fn session_slugs_follow_freestyle_rules() {
        assert_eq!(session_slug("3F1A-2B__x"), "3f1a-2b-x");
        assert_eq!(session_slug("--Nym--"), "nym");
        assert_eq!(session_slug(&"a".repeat(100)).len(), 63);
        assert!(session_slug("!!!").starts_with("s-"));
    }

    #[test]
    fn mount_launch_script_is_idempotent_per_boot_and_carries_secrets_as_env() {
        let mut template = ManagedMountConfig::command(
            "",
            [
                "sh",
                "-lc",
                "exec /usr/local/bin/chevalier-vfs-fuse --endpoint {vfs_endpoint} --scope {vfs_scope_path} --tag {mount_tag} {mountpoint}",
            ],
        );
        template.secrets.insert(
            "CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN".to_string(),
            "tok".to_string(),
        );
        let rendered = RenderedMount::render(&template, &shared_mount());
        assert_eq!(rendered.mountpoint, "/mnt/nymfs");
        assert_eq!(
            rendered.command[2],
            "exec /usr/local/bin/chevalier-vfs-fuse --endpoint https://api.example.com/v1/runtime/internal/vfs/owner --scope nyms/abc --tag nymfs-root /mnt/nymfs"
        );
        let script = rendered.launch_script();
        assert!(script.contains("if [ ! -e '/run/chevalier/mounts/nymfs-root' ]"));
        assert!(script.contains("export CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN='tok'"));
        assert!(script.contains("export CHEVALIER_VFS_READ_ONLY=false"));
        assert!(script.contains("nohup 'sh' '-lc'"));
    }

    #[test]
    fn pty_control_frames_decode() {
        assert!(matches!(
            pty_control_event(r#"{"type":"exited","exitCode":3}"#),
            Some(PtyControl::Exited(3))
        ));
        assert!(
            matches!(pty_control_event(r#"{"type":"error","message":"boom"}"#), Some(PtyControl::Error(m)) if m == "boom")
        );
        assert!(matches!(
            pty_control_event(r#"{"type":"sessionInfo","sessionId":"x"}"#),
            Some(PtyControl::SessionInfo)
        ));
        assert!(pty_control_event("not json").is_none());
    }

    #[test]
    fn vm_states_map_onto_vmd_states() {
        use crate::proto::vmd::v1::VmState;
        assert_eq!(
            FreestyleVmState::Running.as_proto_state(),
            VmState::Running as i32
        );
        assert_eq!(
            FreestyleVmState::Paused.as_proto_state(),
            VmState::Paused as i32
        );
        assert_eq!(
            FreestyleVmState::Stopped.as_proto_state(),
            VmState::Stopped as i32
        );
        let vm: FreestyleVm = serde_json::from_value(json!({
            "id": "vm-1", "state": "pausing", "metadata": {}
        }))
        .unwrap();
        assert_eq!(vm.state, FreestyleVmState::Pausing);
    }

    #[test]
    fn metadata_is_trimmed_to_platform_limits() {
        let mut metadata = HashMap::new();
        for index in 0..70 {
            metadata.insert(format!("k{index}"), "v".to_string());
        }
        metadata.insert("too-long".to_string(), "x".repeat(64));
        let kept = metadata_within_limits(metadata);
        assert_eq!(kept.len(), 64);
        assert!(!kept.contains_key("too-long"));
    }

    #[test]
    fn preview_domains_are_stable_per_vm_and_port() {
        let control = FreestyleControl::new(FreestyleBackendConfig {
            api_key: "k".to_string(),
            ..FreestyleBackendConfig::default()
        })
        .unwrap();
        assert_eq!(
            control.preview_domain("vm-0f3a-9b", 8080),
            "nym-0f3a9b-p8080.style.dev"
        );
    }
}
