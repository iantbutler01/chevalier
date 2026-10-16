//! Spec 86 D §4.6: the one NymFS daemon on a computer keeps the computer's task bindings
//! mounted itself.
//!
//! The API writes the live bindings to `.runtime/computer/mounts.json` in the Nym's files
//! (`mounts`: writable scopes; `links`: symlinks in the root view). It reaches this daemon
//! through the change feed like any other file, and after each batch that touches it the daemon
//! mounts new scopes, drains and unmounts finished ones, and repoints the links. So
//! `/nym/vm/mounts/task` and `/shared` follow the current task without a remount or a guest
//! restart, and `/nym/vm/mounts/skills` is a link into the root view rather than a second copy
//! of the skills.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::{info, warn};

use super::auth::VfsAuth;
use super::fs::MountOwner;
use super::handle::{FuseHandle, mount_remote_vfs_fuse, unmount_fuse};
use super::local_view::hydrate::{KernelPathInvalidator, MountHydrator};

/// Everything needed to start one more writable mount next to the root one.
#[derive(Clone, Debug)]
pub struct MountSpawner {
    pub endpoint: String,
    pub credential: Option<String>,
    pub token: Option<String>,
    pub owner: Option<MountOwner>,
    /// Holds one state directory per mount (the root mount's state directory's parent).
    pub state_root: PathBuf,
    /// Where the root mount is (`/nym`); a scope is mounted at `<root>/<scope>`.
    pub root_mountpoint: PathBuf,
}

#[derive(Debug, Default, Deserialize)]
struct Manifest {
    #[serde(default)]
    mounts: Vec<ManifestMount>,
    #[serde(default)]
    links: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct ManifestMount {
    scope: String,
}

pub(crate) struct ComputerMounts {
    spawner: MountSpawner,
    /// The manifest, relative to the root mount's scope.
    manifest_path: String,
    hydrator: Arc<MountHydrator>,
    invalidator: Arc<dyn KernelPathInvalidator>,
    state: Mutex<Applied>,
}

#[derive(Default)]
struct Applied {
    mounts: BTreeMap<String, FuseHandle>,
    links: BTreeMap<String, String>,
    /// Consecutive reconciles that left a wanted binding unmounted.
    failed_rounds: u32,
    /// A retry is scheduled (at most one at a time).
    retry_pending: bool,
}

/// First wait before retrying a binding that failed to mount; doubles up to `RETRY_MAX`.
const RETRY_MIN: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(60);

fn retry_delay(failed_rounds: u32) -> Duration {
    RETRY_MIN
        .saturating_mul(1u32 << failed_rounds.saturating_sub(1).min(6))
        .min(RETRY_MAX)
}

impl ComputerMounts {
    pub(crate) fn new(
        spawner: MountSpawner,
        manifest_path: &str,
        hydrator: Arc<MountHydrator>,
        invalidator: Arc<dyn KernelPathInvalidator>,
    ) -> Arc<Self> {
        Arc::new(Self {
            spawner,
            manifest_path: manifest_path.trim_matches('/').to_string(),
            hydrator,
            invalidator,
            state: Mutex::default(),
        })
    }

    pub(crate) fn is_manifest(&self, path: &str) -> bool {
        path == self.manifest_path
    }

    /// The manifest, or `None` when the Nym has none (its computer is not on the one-daemon
    /// mount set, so this daemon leaves the task paths to the mounts that serve them).
    fn read_manifest(&self) -> Option<Manifest> {
        let backing = match self.hydrator.view().tree().resolve(&self.manifest_path) {
            Ok(backing) => backing,
            Err(error) => {
                warn!(%error, "could not resolve the computer mounts manifest");
                return None;
            }
        };
        match std::fs::read(&backing) {
            Ok(bytes) => Some(serde_json::from_slice(&bytes).unwrap_or_else(|error| {
                warn!(%error, "the computer mounts manifest is not valid; ignoring it");
                Manifest::default()
            })),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                warn!(%error, "could not read the computer mounts manifest");
                None
            }
        }
    }

    /// Bring mounts and links in line with the manifest. Nothing here fails the root mount: a
    /// binding that fails to mount (its hydrate can lose a race with a guest write) is retried
    /// with backoff until it mounts or the manifest stops listing it, and a link to it is
    /// absent until then: a missing `/nym/vm/mounts/task` fails loudly, where its previous
    /// target (often another task's live, writable workspace) or the empty read-only directory
    /// under the root view would take the new task's writes somewhere wrong.
    pub(crate) async fn reconcile(self: &Arc<Self>) {
        // Right after a hydrate the manifest may still be a placeholder whose bytes have not
        // arrived; reading the backing file then would see nothing.
        let hydrator = Arc::clone(&self.hydrator);
        let path = self.manifest_path.clone();
        if let Ok(Err(error)) =
            tokio::task::spawn_blocking(move || hydrator.view().lazy().ensure(&path)).await
        {
            warn!(%error, "could not fetch the computer mounts manifest");
            return;
        }
        let manifest = self.read_manifest();
        let mut state = self.state.lock().await;
        let manifest = match manifest {
            Some(manifest) => manifest,
            // Never had one: nothing to do. Had one: the API took it away, so take down
            // what it listed.
            None if state.links.is_empty() && state.mounts.is_empty() => return,
            None => Manifest::default(),
        };
        let failed = self.reconcile_mounts(&mut state, &manifest.mounts).await;
        self.pin_mountpoints(&state);
        self.reconcile_links(&mut state, &manifest.links, &failed).await;
        if failed.is_empty() {
            state.failed_rounds = 0;
            return;
        }
        state.failed_rounds = state.failed_rounds.saturating_add(1);
        if state.retry_pending {
            return;
        }
        state.retry_pending = true;
        let delay = retry_delay(state.failed_rounds);
        warn!(
            scopes = ?failed,
            retry_ms = delay.as_millis() as u64,
            "task bindings did not mount; retrying"
        );
        tokio::spawn(Self::retry_after(Arc::clone(self), delay));
    }

    /// A later `reconcile`, boxed behind a declared `Send` type: `reconcile` schedules this,
    /// and an opaque recursive future could not be proven `Send`.
    fn retry_after(
        this: Arc<Self>,
        delay: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            this.state.lock().await.retry_pending = false;
            this.reconcile().await;
        })
    }

    /// Spare every mount stacked inside the root one (this daemon's bindings and any served
    /// by other daemons) from later dentry invalidation, so a resync's sweep never detaches
    /// them (spec 86 review a026e8f4).
    fn pin_mountpoints(&self, state: &Applied) {
        let mut pinned: Vec<String> = state.mounts.keys().cloned().collect();
        pinned.extend(self.served_by_other_mounts());
        self.invalidator.pin_dentries(&pinned);
    }

    /// Re-pin before a sweep (mounts another daemon started since the last reconcile).
    pub(crate) async fn pin_before_sweep(&self) {
        let state = self.state.lock().await;
        self.pin_mountpoints(&state);
    }

    /// Paths below the root mount that another mount currently serves (the separate
    /// `current` mounts of the older mount set, until the guest restarts onto this one).
    fn served_by_other_mounts(&self) -> Vec<String> {
        let root = &self.spawner.root_mountpoint;
        super::handle::active_mountpoints_under(root)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|mountpoint| {
                mountpoint
                    .strip_prefix(root)
                    .ok()
                    .map(|relative| relative.to_string_lossy().to_string())
            })
            .filter(|relative| !relative.is_empty())
            .collect()
    }

    async fn reconcile_links(
        &self,
        state: &mut Applied,
        wanted: &BTreeMap<String, String>,
        unmounted: &BTreeSet<String>,
    ) {
        let wanted = links_to_mounted(wanted, unmounted);
        // A link never replaces a path another mount is serving: invalidating that dentry
        // would detach the mount under whatever is using it.
        let served = self.served_by_other_mounts();
        let wanted: BTreeMap<String, String> = wanted
            .into_iter()
            .filter(|(link, _)| {
                let busy = served.iter().any(|mountpoint| mountpoint == link);
                if busy {
                    warn!(link, "another mount serves this path; not linking it");
                }
                !busy
            })
            .collect();
        if wanted == state.links {
            return;
        }
        self.hydrator
            .set_synthetic_paths(wanted.keys().cloned().collect::<HashSet<_>>());
        let mut changed: Vec<String> = Vec::new();
        for (link, target) in &wanted {
            match self.hydrator.place_synthetic_link(link, target) {
                Ok(()) => changed.push(link.clone()),
                Err(error) => warn!(link, target, %error, "could not place a computer mount link"),
            }
        }
        for link in state.links.keys().filter(|link| !wanted.contains_key(*link)) {
            if let Err(error) = self.hydrator.drop_synthetic_link(link).await {
                warn!(link, %error, "could not remove a computer mount link");
            }
            changed.push(link.clone());
        }
        if !changed.is_empty() {
            self.invalidator.invalidate_paths(&changed);
        }
        info!(links = ?wanted, "computer mount links updated");
        state.links = wanted;
    }

    /// Returns the wanted scopes that are still not mounted.
    async fn reconcile_mounts(
        &self,
        state: &mut Applied,
        wanted: &[ManifestMount],
    ) -> BTreeSet<String> {
        let wanted: Vec<String> = wanted
            .iter()
            .map(|mount| mount.scope.trim_matches('/').to_string())
            .filter(|scope| !scope.is_empty() && !scope.split('/').any(|part| part == ".."))
            .collect();
        let finished: Vec<String> = state
            .mounts
            .keys()
            .filter(|scope| !wanted.contains(scope))
            .cloned()
            .collect();
        for scope in finished {
            let Some(handle) = state.mounts.get(&scope) else {
                continue;
            };
            // Drains what the guest wrote there before detaching it.
            match unmount_fuse(handle).await {
                Ok(()) => {
                    info!(scope, "unmounted a finished task binding");
                    state.mounts.remove(&scope);
                }
                // Busy (a process still has it open): kept, and tried again next time.
                Err(error) => warn!(scope, %error, "could not unmount a finished task binding yet"),
            }
        }
        let mut failed = BTreeSet::new();
        for scope in wanted {
            if state.mounts.contains_key(&scope) {
                continue;
            }
            match self.mount(&scope).await {
                Ok(handle) => {
                    info!(scope, mountpoint = %handle.mountpoint().display(), "mounted a task binding");
                    state.mounts.insert(scope, handle);
                }
                Err(error) => {
                    warn!(scope, error = %format!("{error:#}"), "could not mount a task binding");
                    failed.insert(scope);
                }
            }
        }
        failed
    }

    async fn mount(&self, scope: &str) -> Result<FuseHandle> {
        // The mountpoint is a directory of the root view; the feed normally delivered it
        // before the manifest that names it.
        if self.hydrator.ensure_local_directory(scope)? {
            self.invalidator.invalidate_paths(&[scope.to_string()]);
        }
        let spawner = &self.spawner;
        let auth = VfsAuth::new(
            &spawner.endpoint,
            spawner.credential.as_deref(),
            spawner.token.as_deref(),
        )?;
        let tag = binding_tag(scope);
        let mountpoint = spawner.root_mountpoint.join(scope);
        let state_dir = spawner.state_root.join(&tag);
        mount_remote_vfs_fuse(
            &spawner.endpoint,
            auth,
            scope,
            &tag,
            &mountpoint,
            &state_dir,
            false,
            spawner.owner,
        )
        .await
        .with_context(|| format!("mount task binding {scope}"))
    }

    /// Drain and unmount every binding mount (the daemon is stopping).
    pub async fn shutdown(&self) {
        let mut state = self.state.lock().await;
        for (scope, handle) in std::mem::take(&mut state.mounts) {
            if let Err(error) = unmount_fuse(&handle).await {
                warn!(scope, %error, "could not unmount a task binding at shutdown");
            }
        }
    }
}

/// The links to place, each relative to its own directory: every wanted link whose target
/// scope is mounted. A link whose binding did not mount is left out (removed if it was there)
/// until a retry mounts it, so nothing writes through it into another scope meanwhile.
fn links_to_mounted(
    wanted: &BTreeMap<String, String>,
    unmounted: &BTreeSet<String>,
) -> BTreeMap<String, String> {
    wanted
        .iter()
        .filter_map(|(link, target)| {
            let link = link.trim_matches('/').to_string();
            let target_scope = target.trim_matches('/');
            if link.is_empty() || unmounted.contains(target_scope) {
                return None;
            }
            let target = relative_target(&link, target_scope);
            Some((link, target))
        })
        .collect()
}

/// `target` (relative to the mount root) as seen from the directory holding `link`.
fn relative_target(link: &str, target: &str) -> String {
    let depth = link.matches('/').count();
    format!("{}{target}", "../".repeat(depth))
}

/// A stable mount tag (and state directory name) for a binding scope.
fn binding_tag(scope: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in scope.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    let kind = if scope.ends_with("/shared") {
        "sh"
    } else {
        "tw"
    };
    format!("nfs-{kind}-{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn links_point_back_up_to_the_mount_root() {
        assert_eq!(
            relative_target("vm/mounts/task", "conversations/c/0003_assistant/mount"),
            "../../conversations/c/0003_assistant/mount"
        );
        assert_eq!(relative_target("top", "a/b"), "a/b");
    }

    #[test]
    fn binding_tags_are_stable_and_distinct() {
        let shared = binding_tag("conversations/c/shared");
        assert_eq!(shared, binding_tag("conversations/c/shared"));
        assert!(shared.starts_with("nfs-sh-"));
        assert_ne!(shared, binding_tag("conversations/d/shared"));
        assert!(binding_tag("conversations/c/0003_assistant/mount").starts_with("nfs-tw-"));
    }

    #[test]
    fn a_failed_binding_is_retried_with_a_bounded_backoff() {
        assert_eq!(retry_delay(1), Duration::from_secs(1));
        assert_eq!(retry_delay(2), Duration::from_secs(2));
        assert_eq!(retry_delay(4), Duration::from_secs(8));
        assert_eq!(retry_delay(7), Duration::from_secs(60));
        assert_eq!(retry_delay(1000), Duration::from_secs(60));
    }

    /// While the current task's binding has not mounted, `vm/mounts/task` is absent, not left
    /// on the previous task's workspace; links to mounted scopes (and to the root view's own
    /// paths) are placed as usual.
    #[test]
    fn a_link_to_an_unmounted_binding_is_left_out() {
        let wanted: BTreeMap<String, String> = [
            ("vm/mounts/task", "conversations/c/0004_assistant/mount"),
            ("vm/mounts/shared", "conversations/c/shared"),
            ("vm/mounts/skills", "skills"),
            ("/", "x"),
        ]
        .into_iter()
        .map(|(link, target)| (link.to_string(), target.to_string()))
        .collect();
        let unmounted: BTreeSet<String> =
            ["conversations/c/0004_assistant/mount".to_string()].into();
        let links = links_to_mounted(&wanted, &unmounted);
        assert!(!links.contains_key("vm/mounts/task"), "{links:?}");
        assert_eq!(links["vm/mounts/shared"], "../../conversations/c/shared");
        assert_eq!(links["vm/mounts/skills"], "../../skills");
        assert_eq!(links.len(), 2);
        // Once it mounts, the link is placed.
        let links = links_to_mounted(&wanted, &BTreeSet::new());
        assert_eq!(
            links["vm/mounts/task"],
            "../../conversations/c/0004_assistant/mount"
        );
    }
}
