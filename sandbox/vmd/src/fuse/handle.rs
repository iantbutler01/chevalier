use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use chevalier_sandbox::vfs::{VFS_SURFACE_KIND_VM_SHARED, VFS_SURFACE_KIND_VM_WORKSPACE};
use tokio::process::Command;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use crate::config::Config;
use crate::state::types::SharedMountSpec;

use super::client::RemoteVfsClient;
use super::fs::RemoteFuseFs;
use super::local_view::hydrate::{
    KernelPathInvalidator, MountHydrator, RemoteFollower, RemoteFollowerHandle,
};
use super::local_view::mount::{
    MountLocalView, MountLocalViewOptions, MountOpen, MountOwnership,
    default_state_dir_for_mountpoint,
};
use super::local_view::publisher::{
    AuthoritativePathSource, MountPublisher, PublisherOptions, TreeGenerationSource,
};
use super::local_view::types::{DrainOutcome, MountOwnerRecord, PublicationHealth};
use super::local_view::wal::MountWal;
use super::local_view::{
    DEFAULT_DRAIN_TIMEOUT_MS, GUARDED_DRAIN_TIMEOUT_MS, MountStateLayout, read_json,
};

const UNMOUNT_TIMEOUT: Duration = Duration::from_secs(10);

/// Bound for an ordinary lifecycle drain: unmount, snapshot, fork, delete.
pub const DEFAULT_VFS_DRAIN_TIMEOUT: Duration = Duration::from_millis(DEFAULT_DRAIN_TIMEOUT_MS);

/// Bound for a drain taken while a per-VM guard is held across the await — the
/// qemu exit reaper. Deliberately much shorter: the WAL is durable either way,
/// and holding the guard is what would wedge every other operation on that VM.
pub const GUARDED_VFS_DRAIN_TIMEOUT: Duration = Duration::from_millis(GUARDED_DRAIN_TIMEOUT_MS);

/// How often the per-mount watchdog re-reads and re-reports publication health.
const PUBLICATION_HEALTH_INTERVAL: Duration = Duration::from_secs(15);

pub struct FuseHandle {
    session: Arc<Mutex<Option<fuser::BackgroundSession>>>,
    mountpoint: PathBuf,
    // The mount-local materialized view: the backing tree, the WAL, the payload
    // store and the ownership lock on this mount's state directory. Shared with
    // the `RemoteFuseFs` inside the session, and kept here so lifecycle
    // transitions (unmount, snapshot, fork, delete) can drain the publication
    // cursor without reaching through the session.
    local: Arc<MountLocalView>,
    // Read-only observers only: the task that re-reads the remote scope when it
    // advances. `Arc` because `FuseHandle` is cloned into `VmRuntime`; the
    // follower stops when the last clone drops.
    _follower: Option<Arc<RemoteFollowerHandle>>,
    // Writable mounts only: the task that keeps publication health in the log.
    // Aborted when the last clone drops, exactly like the follower.
    _publication_watch: Option<Arc<PublicationWatch>>,
}

impl Clone for FuseHandle {
    fn clone(&self) -> Self {
        Self {
            session: Arc::clone(&self.session),
            mountpoint: self.mountpoint.clone(),
            local: Arc::clone(&self.local),
            _follower: self._follower.clone(),
            _publication_watch: self._publication_watch.clone(),
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

    /// How far the gateway replica is behind this mount's locally accepted view.
    /// This is the mount's contribution to vmd's health surface: a blocked
    /// suffix is never silent, because the local view stays authoritative and
    /// nothing else would ever notice.
    pub fn publication_status(&self) -> VfsPublicationStatus {
        VfsPublicationStatus::of(&self.mountpoint, &self.local)
    }

    /// Wait until the gateway has acknowledged everything this mount has
    /// committed, then report whether it actually got there.
    ///
    /// Every lifecycle transition that ends, forks or destroys the mount's
    /// authority calls this. It never returns an error and never fails its
    /// caller: the WAL is durable, so an undrained residue is a replication
    /// delay, not lost work — and refusing to stop a VM because the gateway is
    /// unreachable would turn a replication outage into a control-plane outage.
    /// A residue is therefore logged loudly and handed to the next mount of this
    /// state directory, which replays it from the acknowledged cursor.
    pub async fn drain_publication(&self, reason: &str, deadline: Duration) -> bool {
        match self.local.drain(deadline).await {
            Ok(outcome) => {
                report_drain_outcome(reason, &self.mountpoint, &outcome);
                // Whatever the cursor reached is written down before the caller
                // proceeds. A lifecycle boundary is exactly where a checkpoint
                // pays for itself: it is what a restart, a restored snapshot or
                // a replacement owner starts its replay from.
                self.checkpoint_now().await;
                outcome.is_drained()
            }
            Err(error) => {
                tracing::error!(
                    reason,
                    mountpoint = %self.mountpoint.display(),
                    error = %format!("{error:#}"),
                    "vfs mount publication drain failed; the durable WAL is preserved"
                );
                false
            }
        }
    }

    /// Publish a checkpoint now. Checkpoint construction is blocking device
    /// work, so it goes to the blocking pool rather than onto a runtime worker
    /// that a publisher task shares.
    async fn checkpoint_now(&self) {
        let view = Arc::clone(&self.local);
        let mountpoint = self.mountpoint.clone();
        let checkpointed = tokio::task::spawn_blocking(move || view.checkpoint_now()).await;
        match checkpointed {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(
                mountpoint = %mountpoint.display(),
                error = %format!("{error:#}"),
                "publishing a mount-local checkpoint at a lifecycle boundary failed"
            ),
            Err(error) => tracing::warn!(
                mountpoint = %mountpoint.display(),
                error = %error,
                "joining the mount-local checkpoint at a lifecycle boundary failed"
            ),
        }
    }

    /// Drain, then stop the publisher and the maintenance task and seal the log.
    ///
    /// This is the signal-handler form: the process is going away, so the mount
    /// stops being an owner immediately rather than at the kernel unmount. The
    /// unmount that follows finds the cursor already stopped and does not wait
    /// on it a second time.
    pub async fn shutdown_publication(&self, deadline: Duration) -> bool {
        match self.local.shutdown(deadline).await {
            Ok(outcome) => {
                report_drain_outcome("shutdown", &self.mountpoint, &outcome);
                outcome.is_drained()
            }
            Err(error) => {
                tracing::error!(
                    mountpoint = %self.mountpoint.display(),
                    error = %format!("{error:#}"),
                    "sealing the mount-local state directory during shutdown failed"
                );
                false
            }
        }
    }
}

/// One mount's publication status, in the terms an operator needs.
#[derive(Clone, Debug)]
pub struct VfsPublicationStatus {
    pub mountpoint: PathBuf,
    pub scope_path: String,
    pub read_only: bool,
    pub acknowledged_sequence: u64,
    pub last_committed_sequence: u64,
    pub pending_events: u64,
    pub pending_payload_bytes: u64,
    /// Set only when a permanently rejected event blocks its own WAL suffix. It
    /// is never dead-lettered and never reordered around, so this is a standing
    /// condition until the gateway comes back into agreement.
    pub blocked_sequence: Option<u64>,
    pub blocked_reason: Option<String>,
    pub last_error: Option<String>,
    pub storage_pressure_soft: bool,
    pub storage_pressure_hard: bool,
}

impl VfsPublicationStatus {
    fn of(mountpoint: &Path, view: &MountLocalView) -> Self {
        Self::from_health(
            mountpoint,
            view.scope_path(),
            view.is_read_only(),
            view.publication_health(),
        )
    }

    fn from_health(
        mountpoint: &Path,
        scope_path: &str,
        read_only: bool,
        health: PublicationHealth,
    ) -> Self {
        Self {
            mountpoint: mountpoint.to_path_buf(),
            scope_path: scope_path.to_string(),
            read_only,
            acknowledged_sequence: health.acknowledged_sequence,
            last_committed_sequence: health.last_committed_sequence,
            pending_events: health.pending_events,
            pending_payload_bytes: health.pending_payload_bytes,
            blocked_sequence: health.blocked_sequence,
            blocked_reason: health.blocked_reason,
            last_error: health.last_error,
            storage_pressure_soft: health.storage_pressure_soft,
            storage_pressure_hard: health.storage_pressure_hard,
        }
    }

    /// A permanently rejected event is preserving and blocking its WAL suffix.
    pub fn is_blocked(&self) -> bool {
        self.blocked_sequence.is_some()
    }

    /// Nothing is blocked and local durable storage is not exhausted. A backlog
    /// on its own is not unhealthy — it is the architecture working.
    pub fn is_healthy(&self) -> bool {
        !self.is_blocked() && !self.storage_pressure_hard
    }
}

fn report_drain_outcome(reason: &str, mountpoint: &Path, outcome: &DrainOutcome) {
    match outcome {
        DrainOutcome::Drained { through_sequence } => tracing::debug!(
            reason,
            mountpoint = %mountpoint.display(),
            through_sequence,
            "vfs mount publication drained"
        ),
        DrainOutcome::TimedOut {
            acknowledged_sequence,
            pending_events,
        } => tracing::warn!(
            reason,
            mountpoint = %mountpoint.display(),
            acknowledged_sequence,
            pending_events,
            "vfs mount publication drain timed out; the durable WAL keeps the residue and the \
             next mount of this state directory replays it"
        ),
        DrainOutcome::Blocked {
            blocked_sequence,
            reason: blocked_reason,
        } => tracing::error!(
            reason,
            mountpoint = %mountpoint.display(),
            blocked_sequence,
            blocked_reason = %blocked_reason,
            "vfs mount publication is blocked at a permanently rejected event; the local view is \
             authoritative and the event is preserved, but its WAL suffix cannot be published"
        ),
    }
}

/// The per-mount publication watchdog: a blocked suffix must be loud, and a
/// backlog that stops moving must be visible before it becomes a support ticket.
struct PublicationWatch(JoinHandle<()>);

impl Drop for PublicationWatch {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl PublicationWatch {
    /// Writable mounts only. A read-only observer has no WAL, no publisher and
    /// nothing to report.
    fn spawn(mountpoint: &Path, view: &Arc<MountLocalView>, tokio: &Handle) -> Option<Arc<Self>> {
        if view.is_read_only() {
            return None;
        }
        // `Weak` so the watchdog never keeps the mount alive: it stops the first
        // time it wakes after the last `FuseHandle` clone is dropped.
        let weak = Arc::downgrade(view);
        let mountpoint = mountpoint.to_path_buf();
        let task = tokio.spawn(async move {
            let mut was_blocked = false;
            let mut was_pressured = false;
            loop {
                tokio::time::sleep(PUBLICATION_HEALTH_INTERVAL).await;
                let Some(view) = weak.upgrade() else {
                    return;
                };
                let status = VfsPublicationStatus::of(&mountpoint, &view);
                drop(view);
                if status.is_blocked() {
                    // Re-asserted every interval rather than logged once: a
                    // blocked suffix is a standing condition, and an operator
                    // who joins the log late must still see it.
                    tracing::error!(
                        mountpoint = %mountpoint.display(),
                        scope = %status.scope_path,
                        blocked_sequence = ?status.blocked_sequence,
                        blocked_reason = ?status.blocked_reason,
                        acknowledged_sequence = status.acknowledged_sequence,
                        last_committed_sequence = status.last_committed_sequence,
                        pending_events = status.pending_events,
                        "vfs mount publication is blocked"
                    );
                } else if was_blocked {
                    tracing::info!(
                        mountpoint = %mountpoint.display(),
                        scope = %status.scope_path,
                        acknowledged_sequence = status.acknowledged_sequence,
                        "vfs mount publication recovered and is advancing again"
                    );
                }
                if status.storage_pressure_hard {
                    tracing::error!(
                        mountpoint = %mountpoint.display(),
                        scope = %status.scope_path,
                        pending_events = status.pending_events,
                        pending_payload_bytes = status.pending_payload_bytes,
                        "vfs mount durable storage is exhausted; content mutations are refused \
                         with ENOSPC until the publication backlog drains"
                    );
                } else if status.storage_pressure_soft {
                    tracing::warn!(
                        mountpoint = %mountpoint.display(),
                        scope = %status.scope_path,
                        pending_events = status.pending_events,
                        pending_payload_bytes = status.pending_payload_bytes,
                        "vfs mount durable storage is under pressure from the publication backlog"
                    );
                } else if was_pressured {
                    tracing::info!(
                        mountpoint = %mountpoint.display(),
                        scope = %status.scope_path,
                        "vfs mount durable storage pressure cleared"
                    );
                }
                if !status.is_blocked() && status.pending_events > 0 {
                    tracing::debug!(
                        mountpoint = %mountpoint.display(),
                        scope = %status.scope_path,
                        acknowledged_sequence = status.acknowledged_sequence,
                        last_committed_sequence = status.last_committed_sequence,
                        pending_events = status.pending_events,
                        pending_payload_bytes = status.pending_payload_bytes,
                        last_error = ?status.last_error,
                        "vfs mount publication backlog"
                    );
                }
                was_blocked = status.is_blocked();
                was_pressured = status.storage_pressure_soft || status.storage_pressure_hard;
            }
        });
        Some(Arc::new(Self(task)))
    }
}

/// The state directory a mountpoint with no VM directory gets: a sibling of the
/// mountpoint, never inside it. Exposed for the operator binary's `--state-dir`
/// default.
pub fn default_vfs_state_dir(mountpoint: &Path) -> Result<PathBuf> {
    Ok(default_state_dir_for_mountpoint(mountpoint)?
        .root()
        .to_path_buf())
}

/// Drop the process umask before the first mount.
///
/// The backing tree applies every creation with exactly one syscall — `mkdirat`
/// or `openat(O_CREAT)` — whose mode argument the kernel masks with the process
/// umask, and the single-atomic-syscall rule forbids a follow-up `fchmod` to
/// repair it. FUSE has already resolved the *guest's* umask before the callback
/// reaches vmd, so masking again here would silently strip mode bits the guest
/// explicitly asked for (typically by 022). vmd's own files are created with
/// explicit modes or under directories it owns, so clearing the mask costs
/// nothing elsewhere.
fn clear_process_umask() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // SAFETY: `umask` has no preconditions and cannot fail.
        unsafe { libc::umask(0) };
    });
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
    // `<vm_dir>/vfs-state/<tag>`, deliberately outside `fuse-mounts/`: the stale
    // sidecar reaper lazily unmounts every directory directly under
    // `fuse-mounts`, and this one is a real backing tree, not a mountpoint.
    // `configure_qemu_process_identity` skips `vfs-state` in return, so the
    // launch chown never walks the tree nor hands the guest's qemu uid this
    // mount's authoritative local state.
    let state_dir = MountStateLayout::for_mount(vm_dir, &mount.mount_tag);
    mount_remote_vfs_fuse(
        &mount.vfs_endpoint,
        auth_token,
        &mount.vfs_scope_path,
        &mount.mount_tag,
        &mountpoint,
        state_dir.root(),
        mount.read_only,
    )
    .await
}

/// Mount one remote VFS scope.
///
/// `state_dir` holds this mount's durable local state — the backing tree, the
/// WAL and the payload store — and is always passed explicitly: the operator
/// binary mounts at an arbitrary path, so deriving it from `mountpoint.parent()`
/// would scatter authoritative state wherever the caller happened to mount.
pub async fn mount_remote_vfs_fuse(
    endpoint: &str,
    auth_token: &str,
    scope_path: &str,
    mount_tag: &str,
    mountpoint: &Path,
    state_dir: &Path,
    read_only: bool,
) -> Result<FuseHandle> {
    if !cfg!(target_os = "linux") {
        bail!("vfs fuse mounts are only supported on linux hosts");
    }
    clear_process_umask();
    tokio::fs::create_dir_all(&mountpoint)
        .await
        .with_context(|| format!("create fuse mountpoint {}", mountpoint.display()))?;

    let client = RemoteVfsClient::new(endpoint, auth_token, scope_path)?;
    let local = open_mount_local_view(
        &client, scope_path, endpoint, mount_tag, mountpoint, state_dir, read_only,
    )
    .await?;

    let filesystem = RemoteFuseFs::new_for_mount(
        client.clone(),
        read_only,
        scope_path,
        Arc::clone(&local.view),
        Handle::current(),
    )?;
    let options = filesystem.mount_options(mount_tag);
    // Capture the inode table a post-mount notifier has to resolve through while
    // the fs is still reachable — the fuser notifier only exists after the
    // session is spawned, by which point the fs has been moved into it.
    let notifier_binding = filesystem.kernel_invalidation_binding();
    // The patched fuser session runs the configured native request-thread pool.
    // The wrapper keeps distributed blocking-lock waits on a separate bounded
    // Tokio pool while ordinary callbacks stay on those native threads.
    let filesystem = super::dispatch::SpawnedFuseFs::new(filesystem);
    let session = fuser::spawn_mount2(filesystem, mountpoint, &options)
        .with_context(|| format!("mount fuse filesystem at {}", mountpoint.display()))?;

    // A read-only observer holds no local authority over its scope, so it has to
    // follow the gateway: re-read what advanced, then revoke exactly what it
    // re-read. Both halves run off the FUSE threads — the follower on the tokio
    // runtime, the revocation on the invalidator's own thread. A writable mount
    // is the authority for its scope and never follows anything.
    let follower = if read_only {
        let invalidator: Arc<dyn KernelPathInvalidator> =
            notifier_binding.path_invalidator(session.notifier());
        RemoteFollower::spawn(
            client,
            Arc::clone(&local.hydrator),
            Arc::clone(&local.view),
            invalidator,
            Handle::current(),
        )
        .map(Arc::new)
    } else {
        None
    };

    let handle = FuseHandle {
        session: Arc::new(Mutex::new(Some(session))),
        mountpoint: mountpoint.to_path_buf(),
        local: local.view,
        _follower: follower,
        _publication_watch: local.publication_watch,
    };

    // Bounds the kernel mount itself and nothing else: the state directory is
    // already open, recovered and hydrated by this point, so no amount of remote
    // work can push the mount past this deadline.
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

/// The mount-local view plus the hydrator that materialized it. The hydrator is
/// kept because a read-only observer reuses it for every later convergence pass.
struct OpenedMountLocalView {
    view: Arc<MountLocalView>,
    hydrator: Arc<MountHydrator>,
    publication_watch: Option<Arc<PublicationWatch>>,
}

/// The gateway surface a scope's publications are attributed to. Derived once
/// per mount from the scope itself rather than per path: a scope is entirely a
/// shared surface or entirely a workspace surface, and asking the question per
/// path only ever produced a wrong answer for a workspace that happened to
/// contain a directory called `shared`.
fn surface_kind_for_scope(scope_path: &str) -> &'static str {
    if scope_path.split('/').any(|component| component == "shared") {
        VFS_SURFACE_KIND_VM_SHARED
    } else {
        VFS_SURFACE_KIND_VM_WORKSPACE
    }
}

/// Open, recover and (when the state directory is fresh) eagerly hydrate the
/// mount-local view.
///
/// Runs entirely **before** `spawn_mount2`, so the guest never observes a
/// partial tree, and so the readiness deadline that follows the spawn covers
/// only the kernel mount. A recovered state directory is deliberately not
/// re-hydrated: it may hold committed local work the gateway has not
/// acknowledged yet, and re-materializing over it would mount a lagging replica
/// on top of an unpublished WAL.
async fn open_mount_local_view(
    client: &RemoteVfsClient,
    scope_path: &str,
    endpoint: &str,
    mount_tag: &str,
    mountpoint: &Path,
    state_dir: &Path,
    read_only: bool,
) -> Result<OpenedMountLocalView> {
    let options = MountLocalViewOptions {
        layout: MountStateLayout::new(state_dir),
        scope_path: scope_path.trim_matches('/').to_string(),
        endpoint: endpoint.to_string(),
        mount_tag: mount_tag.to_string(),
        read_only,
        tokio: Handle::current(),
    };
    // Replay, pre-image classification and re-sealing are device-bound and can
    // run for as long as the WAL is deep; none of it belongs on a runtime worker.
    let opened = tokio::task::spawn_blocking(move || MountLocalView::open(options))
        .await
        .context("join mount-local view open")?;
    let MountOpen {
        view,
        needs_hydration,
        recovery,
    } = opened.with_context(|| {
        format!(
            "open mount state directory {} for vfs scope {:?}",
            state_dir.display(),
            scope_path
        )
    })?;

    tracing::info!(
        mountpoint = %mountpoint.display(),
        state_dir = %state_dir.display(),
        scope = scope_path,
        read_only,
        needs_hydration,
        replayed_events = recovery.replayed_events,
        committed_unacknowledged = recovery.committed_unacknowledged,
        resolved_committed = recovery.resolved_committed,
        resolved_aborted = recovery.resolved_aborted,
        resealed_paths = recovery.resealed_paths,
        acknowledged_sequence = recovery.acknowledged_sequence,
        remote_revision = recovery.remote_revision,
        "opened the mount-local vfs state directory"
    );

    let hydrator = Arc::new(MountHydrator::new(client.clone(), Arc::clone(&view)));
    if needs_hydration {
        let outcome = hydrator.hydrate().await.with_context(|| {
            format!(
                "hydrate vfs scope {:?} into {}",
                scope_path,
                state_dir.display()
            )
        })?;
        tracing::info!(
            mountpoint = %mountpoint.display(),
            scope = scope_path,
            remote_revision = outcome.remote_revision,
            directories = outcome.directories,
            files = outcome.files,
            symlinks = outcome.symlinks,
            hard_links = outcome.hard_links,
            bytes = outcome.bytes,
            "eagerly hydrated the mount-local backing tree"
        );
    }

    // The publisher is attached before the session is spawned, so no callback
    // can ever commit an event into a mount that has nobody to replicate it.
    // It is attached *after* the hydrate for one reason: the hydrate seeds the
    // acknowledgement cursor with the revision it is a faithful copy of, and the
    // publisher must resume from that base rather than from revision zero.
    attach_mount_publisher(client, scope_path, &view).await?;

    // Checkpointing, log rotation, compaction and storage-pressure sampling all
    // live here, off every callback path. A read-only mount has no WAL and the
    // call is a no-op for it.
    view.spawn_maintenance()
        .context("start mount-local maintenance")?;

    let publication_watch = PublicationWatch::spawn(mountpoint, &view, &Handle::current());

    Ok(OpenedMountLocalView {
        view,
        hydrator,
        publication_watch,
    })
}

/// Start the ordered asynchronous publisher for a writable mount and bind it to
/// the view.
///
/// It is the only component that mutates the gateway. A read-only observer has
/// no WAL and therefore no publisher: it follows the remote scope instead of
/// replicating to it.
async fn attach_mount_publisher(
    client: &RemoteVfsClient,
    scope_path: &str,
    view: &Arc<MountLocalView>,
) -> Result<()> {
    let Some(wal) = view.wal() else {
        return Ok(());
    };
    // `Weak`, deliberately: the view owns the publisher, so a strong reference
    // back would make the pair immortal and leak the mount's state-directory
    // lock for the lifetime of the process. A dropped view reports generation 0,
    // which the WAL reads as "keep the generation the last checkpoint carried".
    let weak_generation = Arc::downgrade(view);
    let weak_paths = Arc::downgrade(view);
    let options = PublisherOptions::defaults(surface_kind_for_scope(scope_path))
        .with_tree_generation(TreeGenerationSource::new(move || {
            weak_generation
                .upgrade()
                .map(|view| view.tree().tree_generation())
                .unwrap_or(0)
        }))
        .with_authoritative_paths(AuthoritativePathSource::new(move |path| {
            let view = weak_paths.upgrade().ok_or_else(|| {
                anyhow::anyhow!("mount local view closed during publication reconciliation")
            })?;
            Ok(view.tree().lstat(path)?.map(|metadata| metadata.kind))
        }));
    let publisher = MountPublisher::spawn(client.clone(), wal.clone(), Handle::current(), options);
    if let Err(error) = view.attach_publisher(Arc::clone(&publisher)) {
        // Unreachable for a freshly opened view, and deliberately not left to
        // chance: a coordinator nobody owns would hold a `MountWal` clone, and
        // with it this mount's state directory, for the life of the process.
        let _ = publisher.shutdown(Duration::ZERO).await;
        return Err(error).context("attach the mount-local publisher");
    }
    Ok(())
}

/// Unmount with the ordinary lifecycle drain bound.
pub async fn unmount_fuse(handle: &FuseHandle) -> Result<()> {
    unmount_fuse_with_drain(handle, DEFAULT_VFS_DRAIN_TIMEOUT).await
}

/// Unmount, draining the publication cursor first under an explicit bound.
///
/// Unmount is one of the lifecycle boundaries the architecture drains at: after
/// this the mount is no longer the owner of its scope, so everything it accepted
/// should already be in the replica. The drain runs **before** the session is
/// consumed, because a mount that has been torn down can no longer seal a dirty
/// generation.
///
/// The publisher and the maintenance task are stopped only once the kernel mount
/// is confirmed gone: while the mountpoint is still live the guest can still
/// write, and a stopped publisher would silently stop replicating those writes.
pub async fn unmount_fuse_with_drain(handle: &FuseHandle, drain_deadline: Duration) -> Result<()> {
    handle.drain_publication("unmount", drain_deadline).await;
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
    finalize_mount_local_view(handle).await;
    Ok(())
}

/// Stop the publisher and the maintenance task, seal the log and publish a final
/// checkpoint. Called once the kernel mount is provably detached.
///
/// The deadline is zero on purpose: `unmount_fuse_with_drain` already waited for
/// the cursor, and a second full wait here would double every teardown against
/// an unreachable gateway. `shutdown` still re-reads the cursor, so a drain that
/// completed during the unmount itself is still observed.
async fn finalize_mount_local_view(handle: &FuseHandle) {
    match handle.local.shutdown(Duration::ZERO).await {
        Ok(outcome) => report_drain_outcome("unmount-finalize", &handle.mountpoint, &outcome),
        Err(error) => tracing::error!(
            mountpoint = %handle.mountpoint.display(),
            error = %format!("{error:#}"),
            "sealing the mount-local state directory after unmount failed"
        ),
    }
}

/// Durable local work a mount state directory still holds that the gateway has
/// never acknowledged.
#[derive(Clone, Debug)]
pub struct UnpublishedMountState {
    pub state_dir: PathBuf,
    pub scope_path: String,
    pub endpoint: String,
    pub acknowledged_sequence: u64,
    pub last_committed_sequence: u64,
    /// Set when the state directory could not be examined at all: another
    /// process still owns it, or its WAL fails closed. Either way the residue is
    /// unknown rather than provably zero, which is the answer a destructive
    /// caller has to hear.
    pub inspection_error: Option<String>,
}

impl UnpublishedMountState {
    fn pending_events(&self) -> u64 {
        self.last_committed_sequence
            .saturating_sub(self.acknowledged_sequence)
    }
}

/// Report every mount state directory under `vm_dir` that still holds committed
/// work the gateway never acknowledged.
///
/// A destructive VM lifecycle transition calls this immediately before removing
/// the VM directory. After that point the WAL is gone, so an unpublished suffix
/// has to be *recorded* rather than silently discarded — the mount was the
/// authority for those events, and nothing else in the system knows they existed.
///
/// A state directory whose ownership lock is still held belongs to a live mount
/// this process lost track of. It is reported and never opened: two writers on
/// one WAL is the one thing recovery cannot survive.
pub async fn report_unpublished_vfs_state_under(vm_dir: &Path) -> Vec<UnpublishedMountState> {
    let root = MountStateLayout::vfs_state_root(vm_dir);
    let failed_root = root.clone();
    let tokio = Handle::current();
    // Ownership acquisition, replay and checkpoint validation are all blocking
    // device work; none of it belongs on a runtime worker.
    tokio::task::spawn_blocking(move || inspect_vfs_state_root(&root, &tokio))
        .await
        .unwrap_or_else(|error| {
            tracing::warn!(
                error = %error,
                "joining the unpublished vfs state probe failed"
            );
            vec![unknown_unpublished_state(
                failed_root,
                format!("join unpublished vfs state probe: {error}"),
            )]
        })
}

fn inspect_vfs_state_root(root: &Path, tokio: &Handle) -> Vec<UnpublishedMountState> {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
        Err(error) => {
            tracing::warn!(
                state_root = %root.display(),
                error = %error,
                "reading the vfs state root before a destructive transition failed"
            );
            return vec![unknown_unpublished_state(
                root.to_path_buf(),
                format!("read vfs state root: {error}"),
            )];
        }
    };
    let mut residue = Vec::new();
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                residue.push(unknown_unpublished_state(
                    root.to_path_buf(),
                    format!("read vfs state directory entry: {error}"),
                ));
                continue;
            }
        };
        if !entry.path().is_dir() {
            continue;
        }
        if let Some(state) = inspect_mount_state_dir(&entry.path(), tokio) {
            residue.push(state);
        }
    }
    residue
}

fn unknown_unpublished_state(state_dir: PathBuf, error: String) -> UnpublishedMountState {
    UnpublishedMountState {
        state_dir,
        scope_path: String::new(),
        endpoint: String::new(),
        acknowledged_sequence: 0,
        last_committed_sequence: 0,
        inspection_error: Some(error),
    }
}

/// `None` when the directory provably holds nothing unpublished.
fn inspect_mount_state_dir(state_dir: &Path, tokio: &Handle) -> Option<UnpublishedMountState> {
    let layout = MountStateLayout::new(state_dir);
    let record: MountOwnerRecord = match read_json(&layout.owner_path()) {
        // No owner record: the directory was never a mount, so it holds nothing.
        Ok(None) => return None,
        Ok(Some(record)) => record,
        Err(error) => {
            return Some(UnpublishedMountState {
                state_dir: state_dir.to_path_buf(),
                scope_path: String::new(),
                endpoint: String::new(),
                acknowledged_sequence: 0,
                last_committed_sequence: 0,
                inspection_error: Some(format!("{error:#}")),
            });
        }
    };
    let unknown = |error: anyhow::Error| UnpublishedMountState {
        state_dir: state_dir.to_path_buf(),
        scope_path: record.scope_path.clone(),
        endpoint: record.endpoint.clone(),
        acknowledged_sequence: 0,
        last_committed_sequence: 0,
        inspection_error: Some(format!("{error:#}")),
    };

    let options = MountLocalViewOptions {
        layout: layout.clone(),
        scope_path: record.scope_path.clone(),
        endpoint: record.endpoint.clone(),
        mount_tag: record.mount_tag.clone(),
        read_only: false,
        tokio: tokio.clone(),
    };
    // Taking the lock is what proves nobody else is writing this WAL. A failure
    // here is never treated as "empty".
    let ownership = match MountOwnership::acquire(&options) {
        Ok(ownership) => ownership,
        Err(error) => return Some(unknown(error)),
    };
    let wal = match MountWal::open(&layout, Some(ownership.epoch())) {
        Ok(wal) => wal,
        Err(error) => return Some(unknown(error)),
    };
    let acknowledged_sequence = wal.acknowledged_sequence();
    let last_committed_sequence = wal.last_committed_sequence();
    if last_committed_sequence <= acknowledged_sequence {
        return None;
    }
    Some(UnpublishedMountState {
        state_dir: state_dir.to_path_buf(),
        scope_path: record.scope_path,
        endpoint: record.endpoint,
        acknowledged_sequence,
        last_committed_sequence,
        inspection_error: None,
    })
}

/// Log the residue fencing a destructive transition. Returns whether anything
/// was reported, so the caller can refuse to destroy the authoritative WAL.
pub fn warn_unpublished_vfs_state(context: &str, residue: &[UnpublishedMountState]) -> bool {
    for state in residue {
        match state.inspection_error.as_deref() {
            Some(error) => tracing::error!(
                context,
                state_dir = %state.state_dir.display(),
                scope = %state.scope_path,
                endpoint = %state.endpoint,
                error,
                "vfs mount state directory could not be proven drained; refusing destructive \
                 transition"
            ),
            None => tracing::error!(
                context,
                state_dir = %state.state_dir.display(),
                scope = %state.scope_path,
                endpoint = %state.endpoint,
                acknowledged_sequence = state.acknowledged_sequence,
                last_committed_sequence = state.last_committed_sequence,
                pending_events = state.pending_events(),
                "vfs mount state directory still holds unpublished committed events; refusing \
                 destructive transition"
            ),
        }
    }
    !residue.is_empty()
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
