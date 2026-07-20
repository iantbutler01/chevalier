use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use tokio::process::Command;
use tokio::runtime::Handle;

use crate::config::Config;
use crate::state::types::SharedMountSpec;

use super::client::RemoteVfsClient;
use super::fs::RemoteFuseFs;

const UNMOUNT_TIMEOUT: Duration = Duration::from_secs(10);

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
        let mountpoint = handle.mountpoint.clone();
        let unmount = tokio::task::spawn_blocking(move || session.umount_and_join());
        match tokio::time::timeout(UNMOUNT_TIMEOUT, unmount).await {
            Ok(joined) => joined
                .context("join blocking FUSE unmount")?
                .with_context(|| format!("unmount fuse {}", mountpoint.display()))?,
            Err(_) => bail!(
                "unmount fuse {} timed out after {:?}",
                handle.mountpoint.display(),
                UNMOUNT_TIMEOUT
            ),
        }
    } else if mountpoint_is_active(&handle.mountpoint).await? {
        // `BackgroundSession::umount_and_join` consumes the session even when
        // the kernel unmount fails. Keep the FuseHandle in runtime state on
        // that error; a later cleanup attempt reaches this path and retries by
        // pathname instead of losing the only way to detach the live mount.
        unmount_path(&handle.mountpoint).await?;
    }
    if mountpoint_is_active(&handle.mountpoint).await? {
        bail!(
            "fuse mount {} remains active after unmount",
            handle.mountpoint.display()
        );
    }
    Ok(())
}

/// Retry cleanup for mountpoints whose original `BackgroundSession` was lost
/// during an earlier failed launch or process restart. Paths are detached
/// deepest-first and every `umount` has a finite deadline.
pub async fn unmount_active_mountpoints_under(root: &Path) -> Result<()> {
    let mut active = active_mountpoints_under(root)?;
    active.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    let mut failures = Vec::new();
    for mountpoint in active {
        if let Err(error) = unmount_path(&mountpoint).await {
            failures.push(format!("{}: {error:#}", mountpoint.display()));
        }
    }
    let remaining = active_mountpoints_under(root)?;
    if !remaining.is_empty() {
        failures.push(format!(
            "mountpoints remain active: {}",
            remaining
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("failed retrying FUSE unmounts: {}", failures.join("; "))
    }
}

async fn unmount_path(mountpoint: &Path) -> Result<()> {
    let mut command = Command::new("umount");
    command.arg(mountpoint).kill_on_drop(true);
    let status = tokio::time::timeout(UNMOUNT_TIMEOUT, command.status())
        .await
        .map_err(|_| {
            anyhow!(
                "retry unmount fuse {} timed out after {:?}",
                mountpoint.display(),
                UNMOUNT_TIMEOUT
            )
        })?
        .with_context(|| format!("retry unmount fuse {}", mountpoint.display()))?;
    if !status.success() {
        bail!(
            "retry unmount fuse {} failed with status {}",
            mountpoint.display(),
            status
        );
    }
    Ok(())
}

async fn mountpoint_is_active(mountpoint: &Path) -> Result<bool> {
    let mountpoint = mountpoint.to_path_buf();
    tokio::task::spawn_blocking(move || Ok(!active_mountpoints_under(&mountpoint)?.is_empty()))
        .await
        .context("join fuse mount readiness probe")?
}

/// Return every live mountpoint at or beneath `root` in vmd's own mount
/// namespace. VM directory deletion must call this before recursive removal:
/// traversing a still-mounted FUSE path could delete the remote workspace
/// rather than disposable VM metadata.
pub fn active_mountpoints_under(root: &Path) -> Result<Vec<PathBuf>> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = root;
        return Ok(Vec::new());
    }
    #[cfg(target_os = "linux")]
    let mountinfo =
        std::fs::read_to_string("/proc/self/mountinfo").context("read /proc/self/mountinfo")?;
    #[cfg(target_os = "linux")]
    {
        Ok(active_mountpoints_from_mountinfo(root, &mountinfo))
    }
}

fn active_mountpoints_from_mountinfo(root: &Path, mountinfo: &str) -> Vec<PathBuf> {
    let mut active = mountinfo
        .lines()
        .filter_map(|line| line.split_whitespace().nth(4))
        .map(decode_mountinfo_path)
        .filter(|mountpoint| mountpoint.starts_with(root))
        .collect::<Vec<_>>();
    active.sort();
    active.dedup();
    active
}

fn decode_mountinfo_path(raw: &str) -> PathBuf {
    PathBuf::from(
        raw.replace("\\040", " ")
            .replace("\\011", "\t")
            .replace("\\012", "\n")
            .replace("\\134", "\\"),
    )
}

#[cfg(test)]
mod tests {
    use super::active_mountpoints_from_mountinfo;
    use std::path::{Path, PathBuf};

    #[test]
    fn mountinfo_probe_matches_only_root_and_descendants() {
        let mountinfo = "\
36 25 0:32 / /data/vms/a/fuse-mounts/work rw - fuse.chevalier none rw
37 25 0:33 / /data/vms/a/fuse-mounts/work/nested rw - tmpfs none rw
38 25 0:34 / /data/vms/a-sibling/fuse-mounts/work rw - fuse.chevalier none rw
39 25 0:35 / /data/vms/b/fuse-mounts/work rw - fuse.chevalier none rw
";
        assert_eq!(
            active_mountpoints_from_mountinfo(Path::new("/data/vms/a/fuse-mounts"), mountinfo),
            vec![
                PathBuf::from("/data/vms/a/fuse-mounts/work"),
                PathBuf::from("/data/vms/a/fuse-mounts/work/nested"),
            ]
        );
    }

    #[test]
    fn mountinfo_probe_decodes_escaped_mount_paths() {
        let mountinfo = "\
36 25 0:32 / /data/vms/a/fuse-mounts/with\\040space rw - fuse.chevalier none rw
37 25 0:33 / /data/vms/a/fuse-mounts/with\\134slash rw - fuse.chevalier none rw
";
        assert_eq!(
            active_mountpoints_from_mountinfo(Path::new("/data/vms/a/fuse-mounts"), mountinfo),
            vec![
                PathBuf::from("/data/vms/a/fuse-mounts/with space"),
                PathBuf::from("/data/vms/a/fuse-mounts/with\\slash"),
            ]
        );
    }
}
