use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use tracing_subscriber::EnvFilter;
use vmd_rs::fuse::auth::VfsAuth;
use vmd_rs::fuse::change_stream::FollowControl;
use vmd_rs::fuse::computer_mounts::MountSpawner;
use vmd_rs::fuse::fs::MountOwner;
use vmd_rs::fuse::{
    ComputerMountsOptions, DEFAULT_VFS_DRAIN_TIMEOUT, FollowOptions, control,
    default_vfs_state_dir, mount_remote_vfs_fuse_following, unmount_fuse,
};

#[derive(Debug, Parser)]
#[command(about = "Mount a Chevalier remote VFS endpoint as a foreground FUSE filesystem")]
struct Args {
    /// Required unless `--control` is given.
    #[arg(long)]
    endpoint: Option<String>,
    #[arg(long, default_value = "")]
    scope: String,
    #[arg(long, default_value = "chevalier-vfs")]
    tag: String,
    #[arg(long)]
    token: Option<String>,
    #[arg(long, default_value = "CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN")]
    token_env: String,
    /// Environment variable holding this mount's own credential. When set, the mount
    /// exchanges it at `<endpoint>/access-token` for short-lived access tokens and uses
    /// the token above only if the exchange fails. Taken from the environment only, so
    /// it never shows in the process list.
    #[arg(long, default_value = "CHEVALIER_VFS_MOUNT_CREDENTIAL")]
    credential_env: String,
    #[arg(long)]
    read_only: bool,
    /// Guest account (name or `uid:gid`) the mount presents its entries as owned by, so a
    /// root-started mount serves that user as its owner. Unset reports the stored owners.
    /// Also read from CHEVALIER_VFS_OWNER.
    #[arg(long)]
    owner: Option<String>,
    #[arg(long)]
    mountpoint: Option<PathBuf>,
    /// Durable local state for this mount: the backing tree, the write-ahead log
    /// and the payload store. Must be on the same filesystem as itself (payload
    /// capture reflinks from the backing tree) and must never be inside the
    /// mountpoint. Defaults to a `.<name>-vfs-state` sibling of the mountpoint.
    #[arg(long)]
    state_dir: Option<PathBuf>,
    /// How long the shutdown signal waits for the publisher to bring the gateway
    /// replica level with this mount before detaching it. Whatever is still
    /// unpublished stays in the durable WAL and is replayed by the next mount.
    #[arg(long)]
    drain_timeout_secs: Option<u64>,
    /// Send one request to a running daemon's control socket and print its answer
    /// (`reconnect`, `applied`, or `sync <seq> <timeout_ms>`), then exit. The socket is
    /// CHEVALIER_VFS_CONTROL_SOCKET or /run/chevalier-vfs-fuse.sock.
    #[arg(long, value_name = "REQUEST")]
    control: Option<String>,
    #[arg(value_name = "MOUNTPOINT")]
    positional_mountpoint: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let args = Args::parse();
    if let Some(request) = args.control.as_deref() {
        let answer = control::request(&control::socket_path_from_env(), request).await?;
        println!("{answer}");
        if answer.starts_with("ok") {
            return Ok(());
        }
        std::process::exit(1);
    }
    let endpoint = args
        .endpoint
        .clone()
        .ok_or_else(|| anyhow!("--endpoint is required"))?;
    let mountpoint = args
        .mountpoint
        .or(args.positional_mountpoint)
        .ok_or_else(|| anyhow!("mountpoint is required"))?;
    let token = args
        .token
        .or_else(|| std::env::var(args.token_env.as_str()).ok());
    let credential = std::env::var(args.credential_env.as_str()).ok();
    let auth = VfsAuth::new(&endpoint, credential.as_deref(), token.as_deref()).with_context(
        || {
            format!(
                "missing VFS credential; set {} or {}, or pass --token",
                args.credential_env, args.token_env
            )
        },
    )?;
    let read_only =
        args.read_only || env_truthy("OC_MOUNT_READONLY") || env_truthy("CHEVALIER_VFS_READ_ONLY");
    let state_dir = match args.state_dir {
        Some(state_dir) => state_dir,
        None => default_vfs_state_dir(&mountpoint)?,
    };

    let owner = args
        .owner
        .or_else(|| std::env::var("CHEVALIER_VFS_OWNER").ok())
        .as_deref()
        .filter(|account| !account.trim().is_empty())
        .map(MountOwner::resolve)
        .transpose()?;

    // Spec 86 D: a read-only mount follows the change stream, falling back to polling when
    // the gateway does not serve one (an older API, or a Nym outside the rollout). On the
    // Nym's root it also keeps the computer's task bindings mounted once the API lists them
    // in the manifest (CHEVALIER_VFS_COMPUTER_MOUNTS=off turns that off).
    // Only the Nym's root mount answers for the computer: its progress is what the API's
    // barrier waits on, and it owns the control socket. A scoped mount (the skills daemon of
    // the older four-mount set) follows locally and keeps a socket of its own, so it neither
    // satisfies the barrier for the root view nor removes the root's socket at start.
    let root_scope = args.scope.trim_matches('/').is_empty();
    let control = if root_scope {
        FollowControl::new()
    } else {
        FollowControl::local_only()
    };
    let manifest_path = std::env::var("CHEVALIER_VFS_COMPUTER_MOUNTS")
        .unwrap_or_else(|_| DEFAULT_COMPUTER_MOUNTS.to_string());
    let follow = (read_only && !env_falsy("CHEVALIER_VFS_CHANGE_STREAM")).then(|| FollowOptions {
        control: control.clone(),
        computer_mounts: Some(manifest_path)
            .filter(|path| {
                !path.trim().is_empty()
                    && path.trim() != "off"
                    && args.scope.trim_matches('/').is_empty()
            })
            .map(|manifest_path| ComputerMountsOptions {
                spawner: MountSpawner {
                    endpoint: endpoint.clone(),
                    credential: credential.clone(),
                    token: token.clone(),
                    owner,
                    state_root: state_dir
                        .parent()
                        .map(PathBuf::from)
                        .unwrap_or_else(|| state_dir.clone()),
                    root_mountpoint: mountpoint.clone(),
                },
                manifest_path,
            }),
    });
    let following = follow.is_some();

    let handle = mount_remote_vfs_fuse_following(
        endpoint.as_str(),
        auth,
        args.scope.as_str(),
        args.tag.as_str(),
        &mountpoint,
        &state_dir,
        read_only,
        owner,
        follow,
    )
    .await
    .with_context(|| format!("mount remote VFS at {}", mountpoint.display()))?;

    let _control_socket = if following {
        let path = if root_scope {
            control::socket_path_from_env()
        } else {
            control::scoped_socket_path(&control::socket_path_from_env(), args.tag.as_str())
        };
        match control::serve(&path, control.clone()) {
            Ok(task) => Some(task),
            Err(error) => {
                tracing::warn!(%error, "no vfs control socket; reconnect requests will not arrive");
                None
            }
        }
    } else {
        None
    };
    #[cfg(unix)]
    let _reconnect_signal = following.then(|| {
        let control = control.clone();
        tokio::spawn(async move {
            let Ok(mut signal) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined2())
            else {
                return;
            };
            while signal.recv().await.is_some() {
                control.request_reconnect();
            }
        })
    });

    wait_for_shutdown_signal().await?;
    // The task bindings sit inside this mount: they go first.
    handle.shutdown_computer_mounts().await;
    // The signal means this process stops being the mount's owner. Drain the
    // publication cursor, stop the publisher and seal the log *before* detaching
    // the kernel mount, so the state directory left behind is one a restart
    // recovers from cheaply. A residue is logged, never silently dropped: the
    // WAL is durable and the next mount replays it from the acknowledged cursor.
    let drain_timeout = args
        .drain_timeout_secs
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_VFS_DRAIN_TIMEOUT);
    handle.shutdown_publication(drain_timeout).await;
    unmount_fuse(&handle).await
}

/// Where the API lists the computer's live task bindings (`NYMFS_COMPUTER_MOUNTS_PATH`).
const DEFAULT_COMPUTER_MOUNTS: &str = ".runtime/computer/mounts.json";

fn env_falsy(name: &str) -> bool {
    std::env::var(name)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "no" | "off"
            )
        })
        .unwrap_or(false)
}

fn env_truthy(name: &str) -> bool {
    std::env::var(name)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("install SIGTERM handler")?;
    if env_truthy("CHEVALIER_VFS_LAUNCHD_SUPERVISED") {
        let mut supervised =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())
                .context("install SIGUSR1 handler")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("wait for ctrl-c")?,
            _ = supervised.recv() => {},
        }
    } else {
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("wait for ctrl-c")?,
            _ = terminate.recv() => {},
        }
    }
    Ok(())
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> Result<()> {
    tokio::signal::ctrl_c().await.context("wait for ctrl-c")
}
