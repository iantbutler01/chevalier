// @dive-file: QEMU/virt helpers for conversion, monitor operations, and VM process orchestration.
// @dive-rel: Called by vmd state manager runtime lifecycle transitions and snapshot operations.
// @dive-rel: Encapsulates platform-specific virtualization behavior behind stable helper APIs.

#[cfg(target_os = "macos")]
use std::ffi::CString;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::process::Command;
use tokio::task;
use tokio::time::sleep;
use tracing::{debug, trace, warn};

pub const RAM_SNAPSHOT_FORMAT_MAPPED: &str = "mapped-ram";
pub const BACKGROUND_SNAPSHOT_TIMEOUT_SECS: u64 = 600;

#[derive(Clone, Debug)]
pub struct MonitorHandle {
    path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Platform {
    pub os: String,
    pub arch: String,
}

#[derive(Clone, Debug)]
pub struct D2VmOptions {
    pub image: String,
    pub output: String,
    pub disk_gb: i32,
    pub pull: bool,
    pub platform: Option<String>,
    pub include_bootstrap: bool,
}

#[derive(Clone, Debug, Default)]
pub struct StatusInfo {
    pub running: bool,
    pub status: String,
}

const DEFAULT_D2VM_IMAGE: &str = "linkacloud/d2vm:latest";
const D2VM_CONTAINER_DIR: &str = "/workspace";

fn configured_d2vm_bin() -> Option<String> {
    std::env::var("CHEVALIER_SANDBOX_D2VM_BIN")
        .or_else(|_| std::env::var("BRACKET_SANDBOX_D2VM_BIN"))
        .map(|raw| raw.trim().to_string())
        .ok()
        .filter(|value| !value.is_empty())
}

fn configured_d2vm_image() -> String {
    std::env::var("CHEVALIER_SANDBOX_D2VM_IMAGE")
        .or_else(|_| std::env::var("BRACKET_SANDBOX_D2VM_IMAGE"))
        .map(|raw| raw.trim().to_string())
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_D2VM_IMAGE.to_string())
}

fn configured_d2vm_include_bootstrap_arg() -> bool {
    std::env::var("CHEVALIER_SANDBOX_D2VM_INCLUDE_BOOTSTRAP")
        .or_else(|_| std::env::var("BRACKET_SANDBOX_D2VM_INCLUDE_BOOTSTRAP"))
        .ok()
        .as_deref()
        .is_some_and(parse_env_bool)
}

fn configured_d2vm_docker_api_version() -> Option<String> {
    std::env::var("CHEVALIER_SANDBOX_D2VM_DOCKER_API_VERSION")
        .or_else(|_| std::env::var("BRACKET_SANDBOX_D2VM_DOCKER_API_VERSION"))
        .or_else(|_| std::env::var("DOCKER_API_VERSION"))
        .map(|raw| raw.trim().to_string())
        .ok()
        .filter(|value| !value.is_empty())
}

fn configured_d2vm_host_workdir() -> Option<PathBuf> {
    std::env::var("CHEVALIER_SANDBOX_D2VM_HOST_WORKDIR")
        .or_else(|_| std::env::var("BRACKET_SANDBOX_D2VM_HOST_WORKDIR"))
        .ok()
        .map(|raw| PathBuf::from(raw.trim()))
        .filter(|path| !path.as_os_str().is_empty())
}

fn parse_env_bool(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn host_container_platform() -> Option<&'static str> {
    match std::env::consts::ARCH {
        "aarch64" | "arm64" => Some("linux/arm64"),
        "x86_64" | "amd64" => Some("linux/amd64"),
        _ => None,
    }
}

pub async fn run_d2vm(docker_bin: &str, host_dir: &Path, opts: D2VmOptions) -> Result<()> {
    if opts.image.trim().is_empty() {
        bail!("d2vm image reference is required");
    }
    let d2vm_bin = configured_d2vm_bin();
    let d2vm_image = configured_d2vm_image();
    let include_bootstrap_arg = opts.include_bootstrap && configured_d2vm_include_bootstrap_arg();
    let docker_api_version = configured_d2vm_docker_api_version();
    let output_name = if opts.output.trim().is_empty() {
        "disk.qcow2".to_string()
    } else {
        opts.output.clone()
    };
    let converter_platform = host_container_platform();
    let disk_gb = if opts.disk_gb <= 0 { 10 } else { opts.disk_gb };
    let host_dir = host_dir
        .canonicalize()
        .with_context(|| format!("canonicalize {}", host_dir.display()))?;
    // When vmd itself runs in a container with the host Docker socket, `host_dir`
    // is a path in vmd's mount namespace. The sibling converter container needs
    // the corresponding Docker-host path so both containers see the same bind.
    let docker_host_dir = configured_d2vm_host_workdir().unwrap_or_else(|| host_dir.clone());
    if !docker_host_dir.is_absolute() {
        bail!(
            "configured d2vm host workdir must be absolute: {}",
            docker_host_dir.display()
        );
    }
    debug!(
        image = %opts.image,
        output = %output_name,
        disk_gb,
        platform = ?opts.platform,
        pull = opts.pull,
        include_bootstrap = opts.include_bootstrap,
        include_bootstrap_arg,
        docker_api_version = ?docker_api_version,
        converter_image = %d2vm_image,
        converter_bin = ?d2vm_bin,
        converter_platform = ?converter_platform,
        workdir = %host_dir.display(),
        docker_host_workdir = %docker_host_dir.display(),
        "running d2vm conversion"
    );

    if let Some(bin) = d2vm_bin {
        let mut cmd = Command::new(&bin);
        cmd.current_dir(&host_dir)
            .arg("--verbose")
            .arg("convert")
            .arg(&opts.image)
            .arg("--output")
            .arg(&output_name)
            .arg("--size")
            .arg(format!("{disk_gb}G"));
        if opts.pull {
            cmd.arg("--pull");
        }
        if let Some(platform) = &opts.platform {
            if !platform.trim().is_empty() {
                cmd.arg("--platform").arg(platform);
            }
        }
        if include_bootstrap_arg {
            cmd.arg("--include-bootstrap");
        }
        trace!(command = ?cmd, "spawning native d2vm");
        let output = cmd.output().await.context("spawn native d2vm")?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            bail!("d2vm convert failed: {stderr}");
        }
        debug!(output = %output_name, "d2vm conversion complete");
        return Ok(());
    }

    let build_cmd = |include_bootstrap_arg: bool| {
        let mut cmd = Command::new(docker_bin);
        cmd.arg("run")
            .arg("--rm")
            .arg("--privileged")
            .arg("-v")
            .arg("/var/run/docker.sock:/var/run/docker.sock")
            .arg("-v")
            .arg(format!(
                "{}:{}",
                docker_host_dir.display(),
                D2VM_CONTAINER_DIR
            ))
            .arg("-w")
            .arg(D2VM_CONTAINER_DIR)
            // Force converter image to host architecture so qemu-img itself never runs under
            // Rosetta/user-mode emulation on Apple Silicon.
            .args(
                converter_platform
                    .map(|platform| vec!["--platform", platform])
                    .unwrap_or_default(),
            );
        if let Some(version) = docker_api_version.as_deref() {
            cmd.arg("-e").arg(format!("DOCKER_API_VERSION={version}"));
        }
        cmd.arg(&d2vm_image)
            .arg("--verbose")
            .arg("convert")
            .arg(&opts.image)
            .arg("--output")
            .arg(&output_name)
            .arg("--size")
            .arg(format!("{disk_gb}G"));

        if opts.pull {
            cmd.arg("--pull");
        }
        if let Some(platform) = &opts.platform {
            if !platform.trim().is_empty() {
                cmd.arg("--platform").arg(platform);
            }
        }
        if include_bootstrap_arg {
            cmd.arg("--include-bootstrap");
        }
        cmd
    };

    let mut cmd = build_cmd(include_bootstrap_arg);
    trace!(command = ?cmd, "spawning docker d2vm");
    let output = cmd.output().await.context("spawn docker d2vm")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        bail!("d2vm convert failed: {stderr}");
    }
    debug!(output = %output_name, "d2vm conversion complete");
    Ok(())
}

pub async fn create_overlay(
    qemu_img_bin: &str,
    base_path: &Path,
    overlay_path: &Path,
    size_gb: i32,
) -> Result<()> {
    if size_gb <= 0 {
        bail!("invalid overlay size: {size_gb}GB");
    }
    let size_arg = format!("{size_gb}G");
    let output = Command::new(qemu_img_bin)
        .arg("create")
        .arg("-f")
        .arg("qcow2")
        .arg("-F")
        .arg("qcow2")
        .arg("-b")
        .arg(base_path)
        .arg(overlay_path)
        .arg(size_arg)
        .output()
        .await
        .with_context(|| "spawn qemu-img create")?;
    if !output.status.success() {
        bail!(
            "qemu-img overlay create failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

pub async fn create_qcow2(qemu_img_bin: &str, path: &Path, size_gb: i32) -> Result<()> {
    if size_gb <= 0 {
        bail!("invalid qcow2 size: {size_gb}GB");
    }
    let size_arg = format!("{size_gb}G");
    let output = Command::new(qemu_img_bin)
        .arg("create")
        .arg("-f")
        .arg("qcow2")
        .arg(path)
        .arg(size_arg)
        .output()
        .await
        .with_context(|| "spawn qemu-img create")?;
    if !output.status.success() {
        bail!(
            "qemu-img create failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

pub async fn delete_snapshot_offline(
    qemu_img_bin: &str,
    disk_path: &Path,
    name: &str,
) -> Result<()> {
    let args = [
        OsStr::new("snapshot"),
        OsStr::new("-d"),
        OsStr::new(name),
        disk_path.as_os_str(),
    ];
    run_qemu_img(qemu_img_bin, &args, "snapshot delete").await
}

pub async fn revert_snapshot_offline(
    qemu_img_bin: &str,
    disk_path: &Path,
    name: &str,
) -> Result<()> {
    let args = [
        OsStr::new("snapshot"),
        OsStr::new("-a"),
        OsStr::new(name),
        disk_path.as_os_str(),
    ];
    run_qemu_img(qemu_img_bin, &args, "snapshot apply").await
}

async fn run_qemu_img(qemu_img_bin: &str, args: &[&OsStr], context_label: &str) -> Result<()> {
    let mut cmd = Command::new(qemu_img_bin);
    for arg in args {
        cmd.arg(arg);
    }
    let output = cmd
        .output()
        .await
        .with_context(|| format!("spawn qemu-img {context_label}"))?;
    if !output.status.success() {
        bail!(
            "qemu-img {context_label} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

pub async fn wait_for_monitor(path: &Path, timeout: Duration) -> Result<MonitorHandle> {
    let deadline = Instant::now() + timeout;
    loop {
        match establish_connection(path).await {
            Ok(conn) => {
                drop(conn);
                return Ok(MonitorHandle {
                    path: path.to_path_buf(),
                });
            }
            Err(err) => {
                if Instant::now() >= deadline {
                    return Err(err);
                }
                sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

pub async fn wait_for_running(monitor: &MonitorHandle, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let status = monitor.query_status().await?;
        if status.running {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "timeout waiting for VM to report running state (last qmp status: {})",
                status.status
            );
        }
        sleep(Duration::from_millis(500)).await;
    }
}

pub async fn wait_for_running_or_incoming_restore(
    monitor: &MonitorHandle,
    running_timeout: Duration,
    incoming_total_timeout: Duration,
    incoming_stall_timeout: Duration,
) -> Result<()> {
    let started = Instant::now();
    let running_deadline = started + running_timeout;
    let incoming_deadline = started + incoming_total_timeout;
    let mut incoming_seen = false;
    let mut last_progress_at = started;
    let mut last_bytes_transferred = None;
    let mut last_migrate_status: Option<String> = None;
    let mut resume_requested = false;

    loop {
        let status = monitor.query_status().await?;
        if status.running {
            return Ok(());
        }

        if status.status == "inmigrate" {
            incoming_seen = true;
        }

        if incoming_seen {
            match query_migrate(monitor).await {
                Ok(migrate) => {
                    let status_changed =
                        last_migrate_status.as_deref() != Some(migrate.status.as_str());
                    let bytes_progressed = match (last_bytes_transferred, migrate.bytes_transferred)
                    {
                        (Some(previous), Some(current)) => current > previous,
                        (None, Some(current)) => current > 0,
                        _ => false,
                    };
                    if status_changed || bytes_progressed {
                        last_progress_at = Instant::now();
                        last_migrate_status = Some(migrate.status.clone());
                        last_bytes_transferred = migrate.bytes_transferred;
                        debug!(
                            status = %migrate.status,
                            bytes = ?migrate.bytes_transferred,
                            total_ms = ?migrate.total_time_ms,
                            "incoming migration restore made progress"
                        );
                    }

                    match migrate.status.as_str() {
                        "failed" | "cancelled" | "cancelling" => {
                            bail!(
                                "incoming migration restore ended in status {}: {}",
                                migrate.status,
                                migrate.error_desc.as_deref().unwrap_or("<no detail>")
                            );
                        }
                        _ => {}
                    }
                }
                Err(err) => {
                    warn!(
                        error = %err,
                        "failed querying incoming migration progress"
                    );
                }
            }

            let migration_completed = last_migrate_status.as_deref() == Some("completed");
            if migration_completed && status.status == "paused" && !resume_requested {
                cont(monitor)
                    .await
                    .with_context(|| "resume VM after completed incoming restore")?;
                resume_requested = true;
                debug!("resumed VM after incoming migration restored paused runstate");
            }

            let now = Instant::now();
            if now >= incoming_deadline {
                bail!(
                    "timeout waiting for incoming VM restore to finish after {}s (last qmp status: {}, last migrate status: {}, bytes: {:?})",
                    incoming_total_timeout.as_secs(),
                    status.status,
                    last_migrate_status.as_deref().unwrap_or("<unknown>"),
                    last_bytes_transferred
                );
            }
            if !migration_completed
                && now.duration_since(last_progress_at) >= incoming_stall_timeout
            {
                bail!(
                    "incoming VM restore stalled for {}s (last qmp status: {}, last migrate status: {}, bytes: {:?})",
                    incoming_stall_timeout.as_secs(),
                    status.status,
                    last_migrate_status.as_deref().unwrap_or("<unknown>"),
                    last_bytes_transferred
                );
            }
        } else if Instant::now() >= running_deadline {
            bail!(
                "timeout waiting for VM to report running state (last qmp status: {})",
                status.status
            );
        }

        sleep(Duration::from_millis(500)).await;
    }
}

pub async fn system_powerdown(monitor: &MonitorHandle) -> Result<()> {
    monitor.execute("system_powerdown", None).await.map(|_| ())
}

pub async fn quit(monitor: &MonitorHandle) -> Result<()> {
    monitor.execute("quit", None).await.map(|_| ())
}

pub async fn stop(monitor: &MonitorHandle) -> Result<()> {
    monitor.execute("stop", None).await.map(|_| ())
}

pub async fn cont(monitor: &MonitorHandle) -> Result<()> {
    monitor.execute("cont", None).await.map(|_| ())
}

/// Tunables for the background-snapshot migrate path.
///
/// `max_bandwidth_bytes_per_sec` is passed to QMP `migrate-set-parameters`; the qemu default
/// is 32 MiB/s which would cap our snapshot throughput absurdly low. We set this to a
/// large value so the snapshot writes at the underlying storage's full speed.
///
/// QEMU 9+ supports the `mapped-ram` file migration format, which pairs with `direct-io`
/// so RAM pages are written to stable offsets rather than a single sequential stream.
/// vmd requires that format for snapshots; callers cold-start when the host cannot provide it.
#[derive(Clone, Debug)]
pub struct BackgroundSnapshotOptions {
    pub max_bandwidth_bytes_per_sec: u64,
    pub direct_io: bool,
}

impl Default for BackgroundSnapshotOptions {
    fn default() -> Self {
        Self {
            // 10 GiB/s — effectively uncapped; qemu clamps to real disk throughput anyway.
            max_bandwidth_bytes_per_sec: 10u64 * 1024 * 1024 * 1024,
            direct_io: true,
        }
    }
}

/// Discover the qemu block device id backing `disk_path`. Required because our launch
/// args use the `-drive` shorthand (no explicit node name), so qemu auto-generates ids
/// like `virtio0` / `ide0-hd0` / `virtio-disk0`. We query `query-block`, find the device
/// whose `inserted.file` matches our disk, and return its `device` name so we can use it
/// as the target of `blockdev-snapshot-internal-sync`.
pub async fn find_block_device_id(monitor: &MonitorHandle, disk_path: &Path) -> Result<String> {
    let value = monitor.execute("query-block", None).await?;
    let canon_disk = disk_path
        .canonicalize()
        .unwrap_or_else(|_| disk_path.to_path_buf());
    let Some(array) = value.as_array() else {
        bail!("query-block did not return an array: {value}");
    };
    for entry in array {
        let file = entry
            .get("inserted")
            .and_then(|ins| ins.get("file"))
            .and_then(|f| f.as_str());
        let device = entry.get("device").and_then(|d| d.as_str());
        if let (Some(file), Some(device)) = (file, device) {
            let candidate = PathBuf::from(file);
            let candidate = candidate.canonicalize().unwrap_or(candidate);
            if candidate == canon_disk && !device.is_empty() {
                return Ok(device.to_string());
            }
        }
    }
    bail!(
        "query-block: no block device matches disk path {}",
        disk_path.display()
    )
}

/// Take a disk-only internal qcow2 snapshot via QMP `blockdev-snapshot-internal-sync`.
/// Metadata-only operation, sub-millisecond even on slow storage.
pub async fn blockdev_snapshot_internal_sync(
    monitor: &MonitorHandle,
    device: &str,
    name: &str,
) -> Result<()> {
    let args = json!({ "device": device, "name": name });
    monitor
        .execute("blockdev-snapshot-internal-sync", Some(args))
        .await
        .map(|_| ())
}

/// Delete a disk-only internal qcow2 snapshot via QMP
/// `blockdev-snapshot-delete-internal-sync`. Safe to call from the background task that
/// created it; idempotent enough that callers can `let _ = …` on delete failures.
pub async fn blockdev_snapshot_delete_internal_sync(
    monitor: &MonitorHandle,
    device: &str,
    name: &str,
) -> Result<()> {
    let args = json!({ "device": device, "name": name });
    monitor
        .execute("blockdev-snapshot-delete-internal-sync", Some(args))
        .await
        .map(|_| ())
}

async fn configure_mapped_background_snapshot(
    monitor: &MonitorHandle,
    opts: &BackgroundSnapshotOptions,
) -> Result<()> {
    let args = json!({
        "capabilities": [
            { "capability": "background-snapshot", "state": true },
            { "capability": "mapped-ram", "state": true }
        ]
    });
    monitor
        .execute("migrate-set-capabilities", Some(args))
        .await?;
    configure_mapped_snapshot_parameters(monitor, opts).await
}

async fn configure_paused_mapped_snapshot(
    monitor: &MonitorHandle,
    opts: &BackgroundSnapshotOptions,
) -> Result<()> {
    let args = json!({
        "capabilities": [
            { "capability": "background-snapshot", "state": false },
            { "capability": "mapped-ram", "state": true }
        ]
    });
    monitor
        .execute("migrate-set-capabilities", Some(args))
        .await?;
    configure_mapped_snapshot_parameters(monitor, opts).await
}

/// Configure migration runtime parameters for a mapped-RAM save or restore.
async fn configure_mapped_snapshot_parameters(
    monitor: &MonitorHandle,
    opts: &BackgroundSnapshotOptions,
) -> Result<()> {
    let params = json!({
        "max-bandwidth": opts.max_bandwidth_bytes_per_sec,
        "direct-io": opts.direct_io,
    });
    monitor
        .execute("migrate-set-parameters", Some(params))
        .await
        .map(|_| ())
}

async fn poll_file_migration(
    monitor: &MonitorHandle,
    disk_snapshot_name: &str,
    ram_format: &str,
) -> Result<MigrationStatus> {
    let deadline = Instant::now() + Duration::from_secs(BACKGROUND_SNAPSHOT_TIMEOUT_SECS);
    loop {
        let status = query_migrate(monitor).await?;
        trace!(
            status = %status.status,
            bytes = ?status.bytes_transferred,
            snapshot = %disk_snapshot_name,
            "snapshot migrate poll"
        );
        match status.status.as_str() {
            "completed" => {
                let mut status = status;
                status.ram_format = ram_format.to_string();
                return Ok(status);
            }
            "failed" | "cancelled" | "cancelling" => {
                bail!(
                    "snapshot migrate ended in status {}: {}",
                    status.status,
                    status.error_desc.as_deref().unwrap_or("<no detail>")
                );
            }
            // setup | active | pre-switchover | device | postcopy-* | wait-unplug — still in flight
            _ => {}
        }
        if Instant::now() >= deadline {
            bail!(
                "snapshot migrate timed out after {}s",
                BACKGROUND_SNAPSHOT_TIMEOUT_SECS
            );
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn save_vm_paused_mapped_after_disk_snapshot(
    monitor: &MonitorHandle,
    disk_snapshot_name: &str,
    ram_path: &Path,
    opts: &BackgroundSnapshotOptions,
) -> Result<MigrationStatus> {
    let was_running = monitor
        .query_status()
        .await
        .map(|status| status.running)
        .unwrap_or(true);
    if was_running {
        stop(monitor)
            .await
            .with_context(|| "pause VM before mapped-ram snapshot")?;
    }
    let resume_on_error = || async {
        if was_running {
            let _ = cont(monitor).await;
        }
    };

    if let Err(err) = configure_paused_mapped_snapshot(monitor, opts)
        .await
        .with_context(|| "configure paused mapped-ram snapshot")
    {
        resume_on_error().await;
        return Err(err);
    }

    if let Some(parent) = ram_path.parent() {
        if let Err(err) = tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| "create mapped-ram snapshot directory")
        {
            resume_on_error().await;
            return Err(err);
        }
    }
    let _ = tokio::fs::remove_file(ram_path).await;

    let migrate_uri = format!("file:{}", ram_path.display());
    if let Err(err) = monitor
        .execute("migrate", Some(json!({ "uri": migrate_uri })))
        .await
        .map(|_| ())
        .with_context(|| "initiate paused mapped-ram migration")
    {
        resume_on_error().await;
        return Err(err);
    }

    match poll_file_migration(monitor, disk_snapshot_name, RAM_SNAPSHOT_FORMAT_MAPPED).await {
        Ok(status) => Ok(status),
        Err(err) => {
            resume_on_error().await;
            Err(err)
        }
    }
}

pub async fn start_incoming_migration_from_file(
    monitor: &MonitorHandle,
    ram_path: &Path,
    ram_format: &str,
    opts: &BackgroundSnapshotOptions,
) -> Result<()> {
    if ram_format != RAM_SNAPSHOT_FORMAT_MAPPED {
        bail!(
            "unsupported RAM snapshot format `{}`; only `{}` is supported",
            if ram_format.trim().is_empty() {
                "<missing>"
            } else {
                ram_format
            },
            RAM_SNAPSHOT_FORMAT_MAPPED,
        );
    }
    configure_mapped_incoming_restore(monitor, opts).await?;

    let migrate_uri = format!("file:{}", ram_path.display());
    monitor
        .execute("migrate-incoming", Some(json!({ "uri": migrate_uri })))
        .await
        .map(|_| ())
}

async fn configure_mapped_incoming_restore(
    monitor: &MonitorHandle,
    opts: &BackgroundSnapshotOptions,
) -> Result<()> {
    let args = json!({
        "capabilities": [
            { "capability": "mapped-ram", "state": true }
        ]
    });
    monitor
        .execute("migrate-set-capabilities", Some(args))
        .await?;
    configure_mapped_snapshot_parameters(monitor, opts).await
}

/// Status snapshot of an in-progress migration, as returned by QMP `query-migrate`.
#[derive(Clone, Debug)]
pub struct MigrationStatus {
    pub status: String,
    pub total_time_ms: Option<u64>,
    pub bytes_transferred: Option<u64>,
    pub expected_downtime_ms: Option<u64>,
    pub error_desc: Option<String>,
    pub ram_format: String,
}

/// Query the current migration state.
pub async fn query_migrate(monitor: &MonitorHandle) -> Result<MigrationStatus> {
    let value = monitor.execute("query-migrate", None).await?;
    let status = value
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("none")
        .to_string();
    let total_time_ms = value.get("total-time").and_then(|v| v.as_u64());
    let bytes_transferred = value
        .get("ram")
        .and_then(|ram| ram.get("transferred"))
        .and_then(|v| v.as_u64());
    let expected_downtime_ms = value.get("expected-downtime").and_then(|v| v.as_u64());
    let error_desc = value
        .get("error-desc")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    Ok(MigrationStatus {
        status,
        total_time_ms,
        bytes_transferred,
        expected_downtime_ms,
        error_desc,
        ram_format: String::new(),
    })
}

/// Take a live snapshot of a running VM using qemu's `background-snapshot` migration
/// capability to write RAM to `ram_path` and the internal block-device snapshot `disk_snapshot_name`
/// to pin the disk state. Guest execution is **not** paused for the RAM save: qemu installs
/// userfaultfd-WP on guest memory and a background thread streams pages to the destination
/// while the VM keeps running.
///
/// Caller must ensure the VM is in `Running` state and its monitor is available. Completion
/// is observed via `query-migrate` (status transitions to `completed`). Returns when the
/// migration has fully drained to disk.
///
/// Ordering inside this function:
///   1. Discover the block device name via `query-block`.
///   2. Take an internal disk snapshot pinning the point-in-time disk state.
///   3. Prefer live `background-snapshot` + `mapped-ram`; if the host kernel lacks
///      background snapshots, pause briefly and write the same mapped-RAM format.
///   4. Configure `max-bandwidth` to an effectively-uncapped value and, for mapped RAM,
///      enable direct I/O.
///   5. Issue `migrate file:<ram_path>` — qemu installs UFFD-WP and begins the async copy.
///   6. Poll `query-migrate` until the migration finishes or fails.
///
/// On failure at any step, best-effort delete the disk snapshot and remove the partial RAM
/// file so retries can start clean.
pub async fn save_vm_background(
    monitor: &MonitorHandle,
    disk_path: &Path,
    disk_snapshot_name: &str,
    ram_path: &Path,
    opts: &BackgroundSnapshotOptions,
) -> Result<MigrationStatus> {
    let device = find_block_device_id(monitor, disk_path)
        .await
        .with_context(|| "resolve block device for background snapshot")?;

    // @dive: The disk snapshot is metadata-only via blockdev-snapshot-internal-sync, so the
    //        VM stays running throughout. Pinning disk state before the RAM migration begins
    //        gives us a consistent point-in-time pair (disk ≤ ram_start_point) for restore.
    blockdev_snapshot_internal_sync(monitor, &device, disk_snapshot_name)
        .await
        .with_context(|| "blockdev-snapshot-internal-sync for background save")?;

    if let Err(err) = configure_mapped_background_snapshot(monitor, opts).await {
        warn!(
            error = %err,
            "live mapped-ram snapshot unavailable; retrying mapped-ram while paused"
        );
        let fallback =
            save_vm_paused_mapped_after_disk_snapshot(monitor, disk_snapshot_name, ram_path, opts)
                .await;
        return match fallback {
            Ok(status) => Ok(status),
            Err(fallback_err) => {
                let _ =
                    blockdev_snapshot_delete_internal_sync(monitor, &device, disk_snapshot_name)
                        .await;
                let _ = tokio::fs::remove_file(ram_path).await;
                Err(fallback_err.context(format!(
                    "paused mapped-ram snapshot failed after live setup failed: {err}"
                )))
            }
        };
    }

    if let Some(parent) = ram_path.parent() {
        if let Err(err) = tokio::fs::create_dir_all(parent).await {
            let _ =
                blockdev_snapshot_delete_internal_sync(monitor, &device, disk_snapshot_name).await;
            return Err(anyhow::anyhow!(err).context("create ram snapshot directory"));
        }
    }
    // Best-effort clean any stale file from a prior failed save so qemu's O_CREAT|O_EXCL
    // path doesn't trip over leftover state.
    let _ = tokio::fs::remove_file(ram_path).await;

    let migrate_uri = format!("file:{}", ram_path.display());
    let migrate_args = json!({ "uri": migrate_uri });
    if let Err(err) = monitor
        .execute("migrate", Some(migrate_args))
        .await
        .map(|_| ())
    {
        let _ = blockdev_snapshot_delete_internal_sync(monitor, &device, disk_snapshot_name).await;
        let _ = tokio::fs::remove_file(ram_path).await;
        return Err(err.context("initiate background-snapshot migrate"));
    }

    match poll_file_migration(monitor, disk_snapshot_name, RAM_SNAPSHOT_FORMAT_MAPPED).await {
        Ok(status) => Ok(status),
        Err(err) => {
            let _ =
                blockdev_snapshot_delete_internal_sync(monitor, &device, disk_snapshot_name).await;
            let _ = tokio::fs::remove_file(ram_path).await;
            Err(err)
        }
    }
}

/// Delete the paired {disk internal snapshot, RAM file} pair produced by
/// [`save_vm_background`]. Best-effort: callers treat this like a GC step.
pub async fn delete_background_snapshot(
    monitor: &MonitorHandle,
    disk_path: &Path,
    disk_snapshot_name: &str,
    ram_path: &Path,
) -> Result<()> {
    let device = find_block_device_id(monitor, disk_path)
        .await
        .with_context(|| "resolve block device for background snapshot delete")?;
    let _ = blockdev_snapshot_delete_internal_sync(monitor, &device, disk_snapshot_name).await;
    let _ = tokio::fs::remove_file(ram_path).await;
    Ok(())
}

pub async fn inspect_image_platforms(docker_bin: &str, reference: &str) -> Result<Vec<Platform>> {
    let manifest_context = match Command::new(docker_bin)
        .arg("manifest")
        .arg("inspect")
        .arg(reference)
        .output()
        .await
    {
        Ok(output) if output.status.success() => {
            let value: Value = serde_json::from_slice(&output.stdout)
                .context("parse docker manifest inspect output")?;
            let platforms = parse_platforms_from_manifest_json(&value);
            if !platforms.is_empty() {
                return Ok(platforms);
            }
            "docker manifest inspect returned no platform information".to_string()
        }
        Ok(output) => format!(
            "docker manifest inspect failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ),
        Err(err) => format!("docker manifest inspect command failed: {err}"),
    };

    warn!(
        reference = %reference,
        manifest_context = %manifest_context,
        "falling back to docker pull platform probes"
    );
    let platforms = probe_platforms_via_pull(docker_bin, reference).await?;
    if platforms.is_empty() {
        bail!(
            "unable to determine supported platforms for image {reference}: {manifest_context}; docker pull --platform probes found no supported linux/amd64 or linux/arm64"
        );
    }
    Ok(platforms)
}

fn parse_platforms_from_manifest_json(value: &Value) -> Vec<Platform> {
    let mut platforms = Vec::new();
    if let Some(manifests) = value.get("manifests").and_then(|v| v.as_array()) {
        for manifest in manifests {
            if let Some(platform) = manifest.get("platform") {
                if let (Some(os), Some(arch)) = (platform.get("os"), platform.get("architecture")) {
                    if let (Some(os), Some(arch)) = (os.as_str(), arch.as_str()) {
                        platforms.push(Platform {
                            os: os.to_string(),
                            arch: arch.to_string(),
                        });
                    }
                }
            }
        }
    }
    if platforms.is_empty() {
        if let (Some(os), Some(arch)) = (
            value.get("os").and_then(|v| v.as_str()),
            value.get("architecture").and_then(|v| v.as_str()),
        ) {
            platforms.push(Platform {
                os: os.to_string(),
                arch: arch.to_string(),
            });
        }
    }
    platforms
}

async fn probe_platform_via_pull(docker_bin: &str, reference: &str, arch: &str) -> Result<bool> {
    let output = Command::new(docker_bin)
        .arg("pull")
        .arg("--platform")
        .arg(format!("linux/{arch}"))
        .arg("--quiet")
        .arg(reference)
        .output()
        .await
        .with_context(|| format!("docker pull --platform linux/{arch} {reference}"))?;
    Ok(output.status.success())
}

async fn probe_platforms_via_pull(docker_bin: &str, reference: &str) -> Result<Vec<Platform>> {
    let mut out = Vec::new();
    for arch in ["amd64", "arm64"] {
        if probe_platform_via_pull(docker_bin, reference, arch).await? {
            out.push(Platform {
                os: "linux".to_string(),
                arch: arch.to_string(),
            });
        }
    }
    Ok(out)
}

pub async fn copy_file(src: &Path, dst: &Path) -> Result<()> {
    let src = src.to_owned();
    let dst = dst.to_owned();
    task::spawn_blocking(move || {
        std::fs::copy(&src, &dst)
            .with_context(|| format!("copy {} -> {}", src.display(), dst.display()))?;
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

pub async fn clone_file_cow(src: &Path, dst: &Path) -> Result<()> {
    let src = src.to_owned();
    let dst = dst.to_owned();
    task::spawn_blocking(move || clone_file_cow_sync(src.as_path(), dst.as_path())).await??;
    Ok(())
}

#[cfg(target_os = "linux")]
fn clone_file_cow_sync(src: &Path, dst: &Path) -> Result<()> {
    use std::fs::OpenOptions;
    use std::os::fd::AsRawFd;

    const FICLONE: libc::c_ulong = 0x4004_9409;

    let src_file = OpenOptions::new()
        .read(true)
        .open(src)
        .with_context(|| format!("open clone source {}", src.display()))?;
    let dst_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(dst)
        .with_context(|| format!("open clone destination {}", dst.display()))?;

    // Enforce CoW clone semantics; never fall back to byte-for-byte copy.
    let rc = unsafe { libc::ioctl(dst_file.as_raw_fd(), FICLONE, src_file.as_raw_fd()) };
    if rc != 0 {
        bail!(
            "reflink clone failed {} -> {}: {}",
            src.display(),
            dst.display(),
            std::io::Error::last_os_error()
        );
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn clone_file_cow_sync(src: &Path, dst: &Path) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;

    let src_c = CString::new(src.as_os_str().as_bytes())
        .with_context(|| format!("clone source path contains NUL: {}", src.display()))?;
    let dst_c = CString::new(dst.as_os_str().as_bytes())
        .with_context(|| format!("clone destination path contains NUL: {}", dst.display()))?;

    // Enforce CoW clone semantics; never fall back to byte-for-byte copy.
    let rc = unsafe { libc::clonefile(src_c.as_ptr(), dst_c.as_ptr(), 0) };
    if rc != 0 {
        bail!(
            "clonefile failed {} -> {}: {}",
            src.display(),
            dst.display(),
            std::io::Error::last_os_error()
        );
    }

    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn clone_file_cow_sync(src: &Path, dst: &Path) -> Result<()> {
    let _ = (src, dst);
    bail!("CoW file cloning is not supported on this platform")
}

/// Handle to a running virtiofsd subprocess serving one shared mount to a guest VM.
///
/// Virtio-fs replaces the migration-incompatible virtio-9p (`-virtfs`) backend for
/// host↔guest shared filesystems. Each shared mount gets its own virtiofsd subprocess,
/// its own Unix socket under `<vm_dir>/virtiofsd-{index}.sock`, and its own qemu
/// `-chardev socket,...` + `-device vhost-user-fs-pci,...` device pair.
///
/// virtiofsd exits automatically when qemu disconnects from the vhost-user socket, so
/// normal VM-stop paths don't strictly need to kill it. This handle carries the PID so
/// stop/force-stop code paths can best-effort reap the child and so crash-reclaim logic
/// can surface orphans.
#[derive(Clone, Debug)]
pub struct VirtiofsdHandle {
    pub pid: u32,
    pub socket_path: PathBuf,
    pub source_path: PathBuf,
    pub tag: String,
    pub read_only: bool,
}

/// Configuration knobs for a single virtiofsd spawn. Separate from `SharedMountSpec`
/// so this module doesn't depend on state::types.
#[derive(Clone, Debug)]
pub struct VirtiofsdSpawn {
    /// Host-side directory tree exposed to the guest.
    pub source_path: PathBuf,
    /// Vhost-user socket path (unique per shared mount).
    pub socket_path: PathBuf,
    /// Mount tag — the guest will `mount -t virtiofs <tag> <guest_path>`.
    pub tag: String,
    /// Read-only export.
    pub read_only: bool,
}

/// Spawn a virtiofsd daemon as an async child process, wait for its vhost-user socket
/// to appear, and return a handle with the PID + paths.
///
/// Target: **Rust virtiofsd** (`gitlab.com/virtio-fs/virtiofsd`, shipped in Ubuntu 24.04
/// as `/usr/libexec/virtiofsd`). qemu 8.x is required for vhost-user-fs migration
/// cooperation, and Ubuntu 24.04 ships both the correct qemu and the Rust virtiofsd
/// version. The C virtiofsd bundled with qemu 6.2 on Ubuntu 22.04 used a different
/// FUSE-style CLI (`-o source=`, `-o cache=`, etc.) which is NOT what we invoke here.
///
/// Invocation:
///   `virtiofsd --socket-path=<sock> --shared-dir=<host> --cache=auto --sandbox=<mode>
///    --log-level=warn [--readonly]`
///
/// The sandbox mode is selected at runtime. Root daemons use `chroot`; non-root
/// daemons use `namespace`, which Rust virtiofsd supports through an unprivileged
/// user namespace. Override with `CHEVALIER_SANDBOX_VIRTIOFSD_SANDBOX` when a host
/// has stricter namespace policy.
///
/// The parent directory of `socket_path` must already exist. Caller is expected to put
/// it under `<vm_dir>/` alongside qmp.sock and qemu.pid.
pub async fn spawn_virtiofsd(
    virtiofsd_bin: &str,
    spawn: &VirtiofsdSpawn,
    log_path: &Path,
) -> Result<VirtiofsdHandle> {
    if !spawn.source_path.is_dir() {
        bail!(
            "virtiofsd source path does not exist or is not a directory: {}",
            spawn.source_path.display()
        );
    }

    // Pre-remove both artifacts from a prior crashed daemon. Rust virtiofsd creates
    // `<socket>.pid` alongside the socket and refuses to start while that pid file
    // remains, even when the recorded process disappeared with a vmd/container
    // restart.
    let _ = std::fs::remove_file(&spawn.socket_path);
    let pid_path = PathBuf::from(format!("{}.pid", spawn.socket_path.to_string_lossy()));
    let _ = std::fs::remove_file(&pid_path);

    let mut cmd = Command::new(virtiofsd_bin);
    cmd.arg(format!(
        "--socket-path={}",
        spawn.socket_path.to_string_lossy()
    ));
    cmd.arg(format!(
        "--shared-dir={}",
        spawn.source_path.to_string_lossy()
    ));
    cmd.arg("--cache=auto");
    let sandbox_mode = configured_virtiofsd_sandbox_mode();
    cmd.arg(format!("--sandbox={sandbox_mode}"));
    cmd.arg("--log-level=warn");
    // @dive: `--migration-mode=find-paths` is required for vhost-user migration
    //        cooperation. Without it, virtiofsd doesn't advertise the
    //        `VHOST_USER_PROTOCOL_F_LOG_SHMFD` feature bit, and qemu rejects every
    //        `migrate` call with "Migration disabled: vhost-user backend lacks
    //        VHOST_USER_PROTOCOL_F_LOG_SHMFD feature". Added in virtiofsd 1.12.0,
    //        stabilized in 1.13.0. The `find-paths` mode reconstructs open-file paths
    //        from file handles during migrate, as opposed to `file-handles` which
    //        serializes kernel file handles — `find-paths` is more portable across
    //        filesystems (ext4/xfs/btrfs/etc).
    cmd.arg("--migration-mode=find-paths");
    // @dive: Read-only enforcement runs on two layers: (1) the host filesystem at the
    //        VFS export root is already mounted ro, and (2) bootstrap/init.sh appends
    //        `,ro` to the guest `mount -t virtiofs` options when the SharedMountSpec
    //        marks the mount read-only. Linux enforces it at the VFS layer, which is
    //        sufficient — virtiofsd has no daemon-side `--readonly` flag.
    let _ = spawn.read_only;

    // Route stdout/stderr to a log file so failures leave a trail.
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .with_context(|| format!("open virtiofsd log {}", log_path.display()))?;
    let stderr_clone = log_file
        .try_clone()
        .with_context(|| format!("clone virtiofsd log fd {}", log_path.display()))?;
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::from(log_file));
    cmd.stderr(std::process::Stdio::from(stderr_clone));

    let child = cmd.spawn().with_context(|| {
        format!(
            "spawn virtiofsd {} for source {} -> socket {}",
            virtiofsd_bin,
            spawn.source_path.display(),
            spawn.socket_path.display()
        )
    })?;
    let pid = child.id().unwrap_or(0);

    // We intentionally detach the Child here: virtiofsd's lifetime is tied to qemu's
    // vhost-user connection, not to this Rust future. Once qemu drops the socket
    // virtiofsd exits; and if vmd needs to forcibly reap it, the PID in VirtiofsdHandle
    // is what we use.
    std::mem::forget(child);

    // Wait for the vhost-user socket to appear so the subsequent qemu launch can
    // connect to it immediately. virtiofsd creates the socket file as soon as it
    // finishes its listen() call. 5 seconds is generous; usually it's <50 ms.
    let deadline = Instant::now() + Duration::from_secs(5);
    while !spawn.socket_path.exists() {
        if Instant::now() >= deadline {
            // Try to reap the daemon if it died early so we don't leak a zombie.
            unsafe {
                libc::kill(pid as libc::pid_t, libc::SIGTERM);
            }
            bail!(
                "virtiofsd socket {} did not appear within 5s (daemon probably failed to start; check {})",
                spawn.socket_path.display(),
                log_path.display()
            );
        }
        sleep(Duration::from_millis(25)).await;
    }

    debug!(
        pid,
        socket = %spawn.socket_path.display(),
        source = %spawn.source_path.display(),
        tag = %spawn.tag,
        read_only = spawn.read_only,
        sandbox_mode = %sandbox_mode,
        "virtiofsd ready"
    );

    Ok(VirtiofsdHandle {
        pid,
        socket_path: spawn.socket_path.clone(),
        source_path: spawn.source_path.clone(),
        tag: spawn.tag.clone(),
        read_only: spawn.read_only,
    })
}

fn configured_virtiofsd_sandbox_mode() -> String {
    std::env::var("CHEVALIER_SANDBOX_VIRTIOFSD_SANDBOX")
        .or_else(|_| std::env::var("BRACKET_SANDBOX_VIRTIOFSD_SANDBOX"))
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| matches!(value.as_str(), "namespace" | "chroot" | "none"))
        .unwrap_or_else(default_virtiofsd_sandbox_mode)
}

fn default_virtiofsd_sandbox_mode() -> String {
    if running_as_root() {
        "chroot".to_string()
    } else {
        "namespace".to_string()
    }
}

#[cfg(unix)]
fn running_as_root() -> bool {
    unsafe { libc::geteuid() == 0 }
}

#[cfg(not(unix))]
fn running_as_root() -> bool {
    false
}

/// Terminate a running virtiofsd by PID. Best-effort; if the daemon already exited
/// (normal case when qemu closes the socket), this is a no-op.
pub fn terminate_virtiofsd(handle: &VirtiofsdHandle) {
    if handle.pid == 0 {
        return;
    }
    unsafe {
        libc::kill(handle.pid as libc::pid_t, libc::SIGTERM);
    }
    // Best-effort unlink of the socket file (virtiofsd should have cleaned it up).
    let _ = std::fs::remove_file(&handle.socket_path);
    let pid_path = PathBuf::from(format!("{}.pid", handle.socket_path.to_string_lossy()));
    let _ = std::fs::remove_file(pid_path);
}

impl MonitorHandle {
    pub async fn query_status(&self) -> Result<StatusInfo> {
        let mut conn = establish_connection(&self.path).await?;
        let value = execute_raw(&mut conn, "query-status", None).await?;
        #[derive(Deserialize)]
        struct Response {
            running: bool,
            status: String,
        }
        let resp: Response = serde_json::from_value(value)?;
        Ok(StatusInfo {
            running: resp.running,
            status: resp.status,
        })
    }

    async fn execute(&self, command: &str, args: Option<Value>) -> Result<Value> {
        let mut conn = establish_connection(&self.path).await?;
        execute_raw(&mut conn, command, args).await
    }

    pub async fn device_add_vfio(&self, id: &str, bdf: &str) -> Result<()> {
        self.execute(
            "device_add",
            Some(json!({
                "driver": "vfio-pci",
                "host": bdf,
                "id": id,
            })),
        )
        .await?;
        Ok(())
    }

    pub async fn device_del_wait(&self, id: &str, timeout: Duration) -> Result<()> {
        let mut conn = establish_connection(&self.path).await?;
        write_command(
            &mut conn.writer,
            &json!({
                "execute": "device_del",
                "arguments": { "id": id },
            }),
        )
        .await?;

        let deadline = Instant::now() + timeout;
        let mut acknowledged = false;
        let mut deleted = false;
        while !(acknowledged && deleted) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                bail!("timed out waiting for DEVICE_DELETED for {id}");
            }
            let message = tokio::time::timeout(remaining, read_json_message(&mut conn.reader))
                .await
                .with_context(|| format!("timed out waiting for DEVICE_DELETED for {id}"))??;
            if let Some(error) = message.get("error") {
                bail!("qmp error: {error}");
            }
            if message.get("return").is_some() {
                acknowledged = true;
                continue;
            }
            if message.get("event").and_then(Value::as_str) == Some("DEVICE_DELETED") {
                let data = message.get("data").and_then(Value::as_object);
                let event_device = data
                    .and_then(|data| data.get("device"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let event_path = data
                    .and_then(|data| data.get("path"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if event_device == id || event_path.ends_with(&format!("/{id}")) {
                    deleted = true;
                }
            }
        }
        Ok(())
    }
}

struct QmpConnection {
    reader: BufReader<tokio::net::unix::OwnedReadHalf>,
    writer: tokio::net::unix::OwnedWriteHalf,
}

async fn establish_connection(path: &Path) -> Result<QmpConnection> {
    let stream = UnixStream::connect(path)
        .await
        .with_context(|| format!("connect qmp {}", path.display()))?;
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    let greeting = read_message(&mut reader).await?;
    if !greeting.get("QMP").is_some() {
        bail!("unexpected QMP greeting: {greeting}");
    }
    write_command(&mut write_half, &json!({ "execute": "qmp_capabilities" })).await?;
    let response = read_message(&mut reader).await?;
    if response.get("return").is_none() {
        bail!("qmp_capabilities handshake failed: {response}");
    }

    Ok(QmpConnection {
        reader,
        writer: write_half,
    })
}

async fn execute_raw(
    conn: &mut QmpConnection,
    command: &str,
    args: Option<Value>,
) -> Result<Value> {
    let mut payload = json!({ "execute": command });
    if let Some(arguments) = args {
        payload
            .as_object_mut()
            .unwrap()
            .insert("arguments".to_string(), arguments);
    }
    write_command(&mut conn.writer, &payload).await?;
    let response = read_message(&mut conn.reader).await?;
    if let Some(err) = response.get("error") {
        bail!("qmp error: {err}");
    }
    Ok(response.get("return").cloned().unwrap_or(Value::Null))
}

async fn write_command(writer: &mut tokio::net::unix::OwnedWriteHalf, value: &Value) -> Result<()> {
    let mut data = serde_json::to_vec(value)?;
    data.push(b'\n');
    writer.write_all(&data).await?;
    writer.flush().await?;
    Ok(())
}

async fn read_message(reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>) -> Result<Value> {
    loop {
        let value = read_json_message(reader).await?;
        if value.get("event").is_none() {
            return Ok(value);
        }
    }
}

async fn read_json_message(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
) -> Result<Value> {
    loop {
        let mut line = String::new();
        let bytes = reader.read_line(&mut line).await?;
        if bytes == 0 {
            bail!("qmp connection closed");
        }
        match serde_json::from_str::<Value>(&line) {
            Ok(value) => return Ok(value),
            Err(err) if err.is_data() => continue,
            Err(err) => return Err(err.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{LazyLock, Mutex};
    use tokio::net::UnixListener;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    async fn serve_qmp_script(listener: UnixListener, script: Vec<(&'static str, Value)>) {
        for (expected_command, response) in script {
            let (stream, _) = listener.accept().await.expect("accept qmp client");
            let (read, mut write) = stream.into_split();
            let mut read = BufReader::new(read);

            write_command(&mut write, &json!({ "QMP": {} }))
                .await
                .expect("write qmp greeting");
            let mut line = String::new();
            read.read_line(&mut line)
                .await
                .expect("read qmp capabilities");
            let capabilities: Value = serde_json::from_str(&line).expect("parse qmp capabilities");
            assert_eq!(capabilities["execute"], "qmp_capabilities");
            write_command(&mut write, &json!({ "return": {} }))
                .await
                .expect("ack qmp capabilities");

            line.clear();
            read.read_line(&mut line)
                .await
                .expect("read scripted qmp command");
            let command: Value = serde_json::from_str(&line).expect("parse scripted qmp command");
            assert_eq!(command["execute"], expected_command);
            write_command(&mut write, &json!({ "return": response }))
                .await
                .expect("write scripted qmp response");
        }
    }

    #[tokio::test]
    async fn incoming_restore_resumes_completed_paused_snapshot_and_requires_running() {
        let temp = tempfile::tempdir().expect("create qmp tempdir");
        let socket = temp.path().join("qmp.sock");
        let listener = UnixListener::bind(&socket).expect("bind qmp socket");
        let server = tokio::spawn(serve_qmp_script(
            listener,
            vec![
                (
                    "query-status",
                    json!({ "running": false, "status": "inmigrate" }),
                ),
                (
                    "query-migrate",
                    json!({
                        "status": "active",
                        "ram": { "transferred": 4096 },
                        "total-time": 10,
                    }),
                ),
                (
                    "query-status",
                    json!({ "running": false, "status": "paused" }),
                ),
                (
                    "query-migrate",
                    json!({
                        "status": "completed",
                        "ram": { "transferred": 8192 },
                        "total-time": 20,
                    }),
                ),
                ("cont", json!({})),
                (
                    "query-status",
                    json!({ "running": true, "status": "running" }),
                ),
            ],
        ));

        tokio::time::timeout(
            Duration::from_secs(3),
            wait_for_running_or_incoming_restore(
                &MonitorHandle { path: socket },
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(1),
            ),
        )
        .await
        .expect("incoming restore waiter should remain bounded")
        .expect("completed paused restore should resume and reach running");
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("qmp script should be fully consumed")
            .expect("qmp mock should finish");
    }

    #[tokio::test]
    async fn legacy_incoming_restore_is_rejected_before_qmp_connect() {
        let temp = tempfile::tempdir().expect("create qmp tempdir");
        let socket = temp.path().join("missing-qmp.sock");
        let ram_path = temp.path().join("snapshot.ram");
        let error = start_incoming_migration_from_file(
            &MonitorHandle { path: socket },
            &ram_path,
            "legacy",
            &BackgroundSnapshotOptions::default(),
        )
        .await
        .expect_err("legacy restore must be rejected");
        assert!(error.to_string().contains("only `mapped-ram` is supported"));
    }

    #[tokio::test]
    async fn paused_snapshot_fallback_still_writes_mapped_ram() {
        let temp = tempfile::tempdir().expect("create qmp tempdir");
        let socket = temp.path().join("qmp.sock");
        let ram_path = temp.path().join("snapshot.ram");
        let listener = UnixListener::bind(&socket).expect("bind qmp socket");
        let server = tokio::spawn(serve_qmp_script(
            listener,
            vec![
                (
                    "query-status",
                    json!({ "running": true, "status": "running" }),
                ),
                ("stop", json!({})),
                ("migrate-set-capabilities", json!({})),
                ("migrate-set-parameters", json!({})),
                ("migrate", json!({})),
                (
                    "query-migrate",
                    json!({
                        "status": "completed",
                        "ram": { "transferred": 8192 },
                        "total-time": 20,
                    }),
                ),
            ],
        ));

        let status = save_vm_paused_mapped_after_disk_snapshot(
            &MonitorHandle { path: socket },
            "snap-test",
            &ram_path,
            &BackgroundSnapshotOptions::default(),
        )
        .await
        .expect("paused mapped-ram snapshot should complete");
        assert_eq!(status.ram_format, RAM_SNAPSHOT_FORMAT_MAPPED);
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("qmp script should be fully consumed")
            .expect("qmp mock should finish");
    }

    #[tokio::test]
    async fn device_del_wait_accepts_event_before_command_acknowledgement() {
        let temp = tempfile::tempdir().expect("create qmp tempdir");
        let socket = temp.path().join("qmp.sock");
        let listener = UnixListener::bind(&socket).expect("bind qmp socket");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept qmp client");
            let (read, mut write) = stream.into_split();
            let mut read = BufReader::new(read);
            write
                .write_all(b"{\"QMP\":{}}\n")
                .await
                .expect("write greeting");
            let mut line = String::new();
            read.read_line(&mut line).await.expect("read capabilities");
            write
                .write_all(b"{\"return\":{}}\n")
                .await
                .expect("ack capabilities");
            line.clear();
            read.read_line(&mut line).await.expect("read device_del");
            let command: Value = serde_json::from_str(&line).expect("parse device_del");
            assert_eq!(command["execute"], "device_del");
            assert_eq!(command["arguments"]["id"], "pci_gpu_0");
            write
                .write_all(
                    b"{\"event\":\"DEVICE_DELETED\",\"data\":{\"device\":\"pci_gpu_0\",\"path\":\"/machine/peripheral/pci_gpu_0\"}}\n",
                )
                .await
                .expect("write deletion event");
            write
                .write_all(b"{\"return\":{}}\n")
                .await
                .expect("ack device_del");
        });

        MonitorHandle { path: socket }
            .device_del_wait("pci_gpu_0", Duration::from_secs(1))
            .await
            .expect("device deletion should complete");
        server.await.expect("qmp mock should finish");
    }

    #[derive(Clone, Copy)]
    enum ManifestMode {
        List,
        Empty,
        Fail,
    }

    fn create_fake_docker(
        manifest_mode: ManifestMode,
        pull_amd64: bool,
        pull_arm64: bool,
    ) -> (tempfile::TempDir, String, PathBuf) {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let log_path = tmp.path().join("fake-docker.log");
        let script_path = tmp.path().join("fake-docker.sh");
        let manifest_behavior = match manifest_mode {
            ManifestMode::List => {
                r#"cat <<'JSON'
{"manifests":[{"platform":{"os":"linux","architecture":"amd64"}},{"platform":{"os":"linux","architecture":"arm64"}}]}
JSON
exit 0"#
            }
            ManifestMode::Empty => {
                r#"cat <<'JSON'
{"schemaVersion":2}
JSON
exit 0"#
            }
            ManifestMode::Fail => {
                r#"echo "no such manifest" >&2
exit 1"#
            }
        };
        let script = format!(
            r#"#!/bin/sh
set -eu
printf '%s\n' "$*" >> "{log_path}"
if [ "${{1:-}}" = "manifest" ] && [ "${{2:-}}" = "inspect" ]; then
  {manifest_behavior}
fi
if [ "${{1:-}}" = "pull" ]; then
  platform=""
  while [ $# -gt 0 ]; do
    if [ "$1" = "--platform" ]; then
      shift
      platform="$1"
      continue
    fi
    shift
  done
  case "$platform" in
    linux/amd64)
      if [ "{pull_amd64}" = "true" ]; then exit 0; fi
      exit 1
      ;;
    linux/arm64)
      if [ "{pull_arm64}" = "true" ]; then exit 0; fi
      exit 1
      ;;
    *)
      exit 1
      ;;
  esac
fi
echo "unsupported args: $*" >&2
exit 2
"#,
            log_path = log_path.display(),
            manifest_behavior = manifest_behavior,
            pull_amd64 = pull_amd64,
            pull_arm64 = pull_arm64
        );
        fs::write(&script_path, script).expect("write fake docker script");
        #[cfg(unix)]
        {
            let mut perms = fs::metadata(&script_path)
                .expect("read script metadata")
                .permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&script_path, perms).expect("set script executable");
        }
        (tmp, script_path.to_string_lossy().to_string(), log_path)
    }

    fn create_fake_d2vm_docker() -> (tempfile::TempDir, String, PathBuf) {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let log_path = tmp.path().join("fake-d2vm-docker.log");
        let script_path = tmp.path().join("fake-d2vm-docker.sh");
        let script = format!(
            r#"#!/bin/sh
set -eu
printf '%s\n' "$*" >> "{log_path}"
if [ "${{1:-}}" = "run" ]; then
  exit 0
fi
echo "unsupported args: $*" >&2
exit 2
"#,
            log_path = log_path.display(),
        );
        fs::write(&script_path, script).expect("write fake docker script");
        #[cfg(unix)]
        {
            let mut perms = fs::metadata(&script_path)
                .expect("read script metadata")
                .permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&script_path, perms).expect("set script executable");
        }
        (tmp, script_path.to_string_lossy().to_string(), log_path)
    }

    fn clear_d2vm_env() {
        unsafe {
            std::env::remove_var("CHEVALIER_SANDBOX_D2VM_BIN");
            std::env::remove_var("BRACKET_SANDBOX_D2VM_BIN");
            std::env::remove_var("CHEVALIER_SANDBOX_D2VM_IMAGE");
            std::env::remove_var("BRACKET_SANDBOX_D2VM_IMAGE");
            std::env::remove_var("CHEVALIER_SANDBOX_D2VM_INCLUDE_BOOTSTRAP");
            std::env::remove_var("BRACKET_SANDBOX_D2VM_INCLUDE_BOOTSTRAP");
            std::env::remove_var("CHEVALIER_SANDBOX_D2VM_DOCKER_API_VERSION");
            std::env::remove_var("BRACKET_SANDBOX_D2VM_DOCKER_API_VERSION");
            std::env::remove_var("CHEVALIER_SANDBOX_D2VM_HOST_WORKDIR");
            std::env::remove_var("BRACKET_SANDBOX_D2VM_HOST_WORKDIR");
            std::env::remove_var("DOCKER_API_VERSION");
        }
    }

    fn d2vm_options() -> D2VmOptions {
        D2VmOptions {
            image: "ubuntu:22.04".to_string(),
            output: "disk.qcow2".to_string(),
            disk_gb: 10,
            pull: true,
            platform: Some("linux/amd64".to_string()),
            include_bootstrap: true,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::await_holding_lock)]
    async fn run_d2vm_uses_public_converter_without_private_bootstrap_flag_by_default() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        clear_d2vm_env();
        let (tmp, docker_bin, log_path) = create_fake_d2vm_docker();

        run_d2vm(&docker_bin, tmp.path(), d2vm_options())
            .await
            .expect("fake docker run should succeed");

        let log = fs::read_to_string(log_path).expect("read fake docker log");
        assert!(log.contains("linkacloud/d2vm:latest"));
        assert!(!log.contains("--include-bootstrap"));
        clear_d2vm_env();
    }

    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::await_holding_lock)]
    async fn run_d2vm_can_opt_into_legacy_include_bootstrap_flag() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        clear_d2vm_env();
        unsafe {
            std::env::set_var("CHEVALIER_SANDBOX_D2VM_INCLUDE_BOOTSTRAP", "true");
        }
        let (tmp, docker_bin, log_path) = create_fake_d2vm_docker();

        run_d2vm(&docker_bin, tmp.path(), d2vm_options())
            .await
            .expect("fake docker run should succeed");

        let log = fs::read_to_string(log_path).expect("read fake docker log");
        assert!(log.contains("--include-bootstrap"));
        clear_d2vm_env();
    }

    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::await_holding_lock)]
    async fn run_d2vm_passes_docker_api_version_to_converter_container() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        clear_d2vm_env();
        unsafe {
            std::env::set_var("CHEVALIER_SANDBOX_D2VM_DOCKER_API_VERSION", "1.42");
        }
        let (tmp, docker_bin, log_path) = create_fake_d2vm_docker();

        run_d2vm(&docker_bin, tmp.path(), d2vm_options())
            .await
            .expect("fake docker run should succeed");

        let log = fs::read_to_string(log_path).expect("read fake docker log");
        assert!(log.contains("-e DOCKER_API_VERSION=1.42"));
        clear_d2vm_env();
    }

    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::await_holding_lock)]
    async fn run_d2vm_uses_docker_host_workdir_for_sibling_converter() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        clear_d2vm_env();
        unsafe {
            std::env::set_var(
                "CHEVALIER_SANDBOX_D2VM_HOST_WORKDIR",
                "/host-visible/chevalier/base-images",
            );
        }
        let (tmp, docker_bin, log_path) = create_fake_d2vm_docker();

        run_d2vm(&docker_bin, tmp.path(), d2vm_options())
            .await
            .expect("fake docker run should succeed");

        let log = fs::read_to_string(log_path).expect("read fake docker log");
        assert!(log.contains("-v /host-visible/chevalier/base-images:/workspace"));
        clear_d2vm_env();
    }

    #[tokio::test]
    async fn inspect_image_platforms_uses_manifest_when_available() {
        let (_tmp, docker_bin, log_path) = create_fake_docker(ManifestMode::List, false, false);
        let platforms = inspect_image_platforms(&docker_bin, "example/image:latest")
            .await
            .expect("platform lookup should succeed");
        assert_eq!(platforms.len(), 2);
        assert!(
            platforms
                .iter()
                .any(|p| p.os == "linux" && p.arch == "amd64")
        );
        assert!(
            platforms
                .iter()
                .any(|p| p.os == "linux" && p.arch == "arm64")
        );

        let log = fs::read_to_string(log_path).expect("read fake docker log");
        assert!(!log.contains("pull "), "fallback pull probe should not run");
    }

    #[tokio::test]
    async fn inspect_image_platforms_falls_back_to_pull_probe_on_manifest_failure() {
        let (_tmp, docker_bin, _log_path) = create_fake_docker(ManifestMode::Fail, true, false);
        let platforms = inspect_image_platforms(&docker_bin, "example/image:latest")
            .await
            .expect("fallback probe should succeed");
        assert_eq!(
            platforms,
            vec![Platform {
                os: "linux".to_string(),
                arch: "amd64".to_string(),
            }]
        );
    }

    #[tokio::test]
    async fn inspect_image_platforms_falls_back_on_manifest_without_platform_fields() {
        let (_tmp, docker_bin, _log_path) = create_fake_docker(ManifestMode::Empty, true, false);
        let platforms = inspect_image_platforms(&docker_bin, "example/image:latest")
            .await
            .expect("fallback probe should succeed");
        assert_eq!(
            platforms,
            vec![Platform {
                os: "linux".to_string(),
                arch: "amd64".to_string(),
            }]
        );
    }

    #[tokio::test]
    async fn inspect_image_platforms_errors_when_no_platform_detected() {
        let (_tmp, docker_bin, _log_path) = create_fake_docker(ManifestMode::Fail, false, false);
        let err = inspect_image_platforms(&docker_bin, "example/missing:latest")
            .await
            .expect_err("lookup should fail");
        let msg = err.to_string();
        assert!(msg.contains("example/missing:latest"));
        assert!(msg.contains("docker manifest inspect failed"));
        assert!(msg.contains("docker pull --platform probes found no supported"));
    }
}
