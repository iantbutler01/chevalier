use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use tokio::runtime::Handle;

use crate::config::Config;
use crate::state::types::SharedMountSpec;

use super::client::RemoteVfsClient;
use super::fs::RemoteFuseFs;

pub struct FuseHandle {
    session: Arc<Mutex<Option<fuser::BackgroundSession>>>,
    mountpoint: PathBuf,
}

impl Clone for FuseHandle {
    fn clone(&self) -> Self {
        Self {
            session: Arc::clone(&self.session),
            mountpoint: self.mountpoint.clone(),
        }
    }
}

impl fmt::Debug for FuseHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FuseHandle")
            .field("mountpoint", &self.mountpoint)
            .finish()
    }
}

impl FuseHandle {
    pub fn mountpoint(&self) -> &Path {
        &self.mountpoint
    }
}

pub async fn mount_vfs_fuse(
    cfg: &Config,
    mount: &SharedMountSpec,
    vm_dir: &Path,
) -> Result<FuseHandle> {
    if !cfg!(target_os = "linux") {
        bail!("vfs fuse mounts are only supported on linux hosts");
    }
    let auth_token = cfg.vfs_internal_service_token.as_deref().ok_or_else(|| {
        anyhow!("missing CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN for fuse-backed mount")
    })?;
    let mountpoint = vm_dir.join("fuse-mounts").join(&mount.mount_tag);
    mount_remote_vfs_fuse(
        &mount.vfs_endpoint,
        auth_token,
        &mount.vfs_scope_path,
        &mount.mount_tag,
        &mountpoint,
        mount.read_only,
    )
    .await
}

pub async fn mount_remote_vfs_fuse(
    endpoint: &str,
    auth_token: &str,
    scope_path: &str,
    mount_tag: &str,
    mountpoint: &Path,
    read_only: bool,
) -> Result<FuseHandle> {
    if !cfg!(target_os = "linux") {
        bail!("vfs fuse mounts are only supported on linux hosts");
    }
    tokio::fs::create_dir_all(&mountpoint)
        .await
        .with_context(|| format!("create fuse mountpoint {}", mountpoint.display()))?;

    let client = RemoteVfsClient::new(endpoint, auth_token, scope_path)?;
    let journal_path = mountpoint
        .parent()
        .unwrap_or(mountpoint)
        .join(format!(".{}-namespace.jsonl", mount_tag));
    let filesystem = RemoteFuseFs::new_with_namespace_journal(
        client,
        read_only,
        scope_path,
        journal_path.as_path(),
        Handle::current(),
    )?;
    let options = filesystem.mount_options(mount_tag);
    // Concurrent dispatch: the single-threaded fuser session loop only decodes
    // requests; ops fan out to workers (see fuse/dispatch.rs).
    let filesystem = super::dispatch::SpawnedFuseFs::new(filesystem);
    let session = fuser::spawn_mount2(filesystem, mountpoint, &options)
        .with_context(|| format!("mount fuse filesystem at {}", mountpoint.display()))?;

    let handle = FuseHandle {
        session: Arc::new(Mutex::new(Some(session))),
        mountpoint: mountpoint.to_path_buf(),
    };

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if mountpoint_is_active(mountpoint).await? {
            return Ok(handle);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let _ = unmount_fuse(&handle).await;
    bail!(
        "fuse mount {} did not become ready within 5s",
        mountpoint.display()
    )
}

pub async fn unmount_fuse(handle: &FuseHandle) -> Result<()> {
    let session = handle
        .session
        .lock()
        .map_err(|_| anyhow!("fuse handle lock poisoned"))?
        .take();
    if let Some(session) = session {
        session
            .umount_and_join()
            .with_context(|| format!("unmount fuse {}", handle.mountpoint.display()))?;
    }
    Ok(())
}

async fn mountpoint_is_active(mountpoint: &Path) -> Result<bool> {
    let mountpoint = mountpoint.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let canonical = std::fs::canonicalize(&mountpoint)
            .with_context(|| format!("canonicalize {}", mountpoint.display()))?;
        let mountinfo =
            std::fs::read_to_string("/proc/self/mountinfo").context("read /proc/self/mountinfo")?;
        let target = canonical.to_string_lossy();
        Ok(mountinfo.lines().any(|line| {
            let mut parts = line.split_whitespace();
            parts.nth(4).is_some_and(|value| value == target)
        }))
    })
    .await
    .context("join fuse mount readiness probe")?
}
