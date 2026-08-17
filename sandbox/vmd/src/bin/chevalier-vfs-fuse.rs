use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use tracing_subscriber::EnvFilter;
use vmd_rs::fuse::{
    DEFAULT_VFS_DRAIN_TIMEOUT, default_vfs_state_dir, mount_remote_vfs_fuse, unmount_fuse,
};

#[derive(Debug, Parser)]
#[command(about = "Mount a Chevalier remote VFS endpoint as a foreground FUSE filesystem")]
struct Args {
    #[arg(long)]
    endpoint: String,
    #[arg(long, default_value = "")]
    scope: String,
    #[arg(long, default_value = "chevalier-vfs")]
    tag: String,
    #[arg(long)]
    token: Option<String>,
    #[arg(long, default_value = "CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN")]
    token_env: String,
    #[arg(long)]
    read_only: bool,
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
    let mountpoint = args
        .mountpoint
        .or(args.positional_mountpoint)
        .ok_or_else(|| anyhow!("mountpoint is required"))?;
    let token = args
        .token
        .or_else(|| std::env::var(args.token_env.as_str()).ok())
        .ok_or_else(|| anyhow!("missing VFS token; pass --token or set {}", args.token_env))?;
    let read_only =
        args.read_only || env_truthy("OC_MOUNT_READONLY") || env_truthy("CHEVALIER_VFS_READ_ONLY");
    let state_dir = match args.state_dir {
        Some(state_dir) => state_dir,
        None => default_vfs_state_dir(&mountpoint)?,
    };

    let handle = mount_remote_vfs_fuse(
        args.endpoint.as_str(),
        token.as_str(),
        args.scope.as_str(),
        args.tag.as_str(),
        &mountpoint,
        &state_dir,
        read_only,
    )
    .await
    .with_context(|| format!("mount remote VFS at {}", mountpoint.display()))?;

    wait_for_shutdown_signal().await?;
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
