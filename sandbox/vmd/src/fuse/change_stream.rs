//! Spec 86 D: a read-only mount follows its Nym's NymFS change feed instead of polling.
//!
//! One request per mount stays open against `{endpoint}/changes` and the gateway pushes
//! coalesced, final-state events as they commit. Nothing runs on a timer: an idle mount sends
//! nothing, and a stream that drops is reopened only after the failure (jittered backoff, 0.5 s
//! doubling to 30 s) or when asked to (`FollowControl::request_reconnect`: the control socket
//! or SIGUSR2, which the API's pre-command barrier uses after a pause).
//!
//! The cursor (`<epoch>:<seq>`) lives in the mount's state directory and is advanced only after
//! a batch is applied and synced, so a crash replays at most one batch, and replay is a no-op:
//! an event whose content the backing copy already holds (the guest's own write coming back)
//! costs a local hash, not a fetch. A cursor the gateway can no longer serve (pruned, another
//! epoch, a huge backlog) gets `resync`: the mount walks its scope and continues from the head
//! the gateway sent.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, watch};
use tracing::{debug, info, warn};

use super::client::{
    ChangeEvent, ChangeFrame, ChangeStream, ChangeStreamOpen, RemoteVfsClient, request_status,
};
use super::computer_mounts::ComputerMounts;
use super::local_view::hydrate::{KernelPathInvalidator, MountHydrator, RemoteFollower};
use super::local_view::{read_json, write_json_atomic};

const BACKOFF_MIN: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
const CURSOR_FILE: &str = "change-cursor.json";
/// Paths whose change events were given up on; the next walk heals them (and a restart
/// that finds any walks).
const QUARANTINE_FILE: &str = "change-quarantine.json";
/// Consecutive auth refusals (401/403) of the stream, despite an exchanged per-computer
/// token, before the mount stops asking and polls.
const MAX_AUTH_REFUSALS: u32 = 3;
/// Consecutive failures to apply the batch at one cursor before its events are applied
/// one at a time and the ones that still fail are quarantined.
const MAX_BATCH_ATTEMPTS: u32 = 4;
/// A session open at least this long counts as healthy when it ends: the next failure
/// starts the backoff over (an idle stream cut by a proxy reconnects promptly).
const HEALTHY_SESSION: Duration = Duration::from_secs(60);

/// Shared between a mount's change follower and whoever may nudge it (the control socket).
pub struct FollowControl {
    reconnect: Notify,
    applied: watch::Sender<i64>,
    /// Whether this mount's progress is reported to the gateway as the computer's. Only the
    /// Nym's root mount does: during the four-mount rollout a scoped daemon (skills) also
    /// follows, and its cursor says nothing about the root view the barrier waits on.
    reports_applied: bool,
}

impl FollowControl {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            reconnect: Notify::new(),
            applied: watch::Sender::new(0),
            reports_applied: true,
        })
    }

    /// A follower whose progress stays local (a mount scoped below the Nym's root).
    pub fn local_only() -> Arc<Self> {
        Arc::new(Self {
            reconnect: Notify::new(),
            applied: watch::Sender::new(0),
            reports_applied: false,
        })
    }

    /// Drop the current stream and open a new one now (a connection held across a pause
    /// may be dead without either end knowing), or cut short a backoff. Nothing is stored for
    /// later: a request while the follower is busy opening a stream changes nothing, since
    /// that stream is new anyway.
    pub fn request_reconnect(&self) {
        self.reconnect.notify_waiters();
    }

    /// The highest feed `seq` applied.
    pub fn applied(&self) -> i64 {
        *self.applied.borrow()
    }

    /// Wait until the feed is applied through `seq`, at most `timeout`.
    pub async fn wait_applied(&self, seq: i64, timeout: Duration) -> bool {
        let mut applied = self.applied.subscribe();
        tokio::time::timeout(timeout, applied.wait_for(|applied| *applied >= seq))
            .await
            .is_ok_and(|result| result.is_ok())
    }

    fn note_applied(&self, seq: i64) {
        self.applied.send_if_modified(|applied| {
            let raised = seq > *applied;
            if raised {
                *applied = seq;
            }
            raised
        });
    }
}

/// Where a mount is in its Nym's feed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChangeCursor {
    pub(crate) epoch: String,
    pub(crate) seq: i64,
}

impl ChangeCursor {
    fn wire(&self) -> String {
        format!("{}:{}", self.epoch, self.seq)
    }
}

pub(crate) fn cursor_path(state_dir: &std::path::Path) -> PathBuf {
    state_dir.join(CURSOR_FILE)
}

pub(crate) fn load_cursor(state_dir: &std::path::Path) -> Option<ChangeCursor> {
    match read_json::<ChangeCursor>(&cursor_path(state_dir)) {
        Ok(cursor) => cursor,
        Err(error) => {
            warn!(%error, "ignoring an unreadable vfs change cursor");
            None
        }
    }
}

pub(crate) fn store_cursor(state_dir: &std::path::Path, cursor: &ChangeCursor) -> Result<()> {
    write_json_atomic(&cursor_path(state_dir), cursor).context("persist the vfs change cursor")
}

pub(crate) fn forget_cursor(state_dir: &std::path::Path) {
    let _ = std::fs::remove_file(cursor_path(state_dir));
}

pub(crate) fn load_quarantine(state_dir: &std::path::Path) -> BTreeSet<String> {
    read_json::<BTreeSet<String>>(&state_dir.join(QUARANTINE_FILE))
        .ok()
        .flatten()
        .unwrap_or_default()
}

fn store_quarantine(state_dir: &std::path::Path, paths: &BTreeSet<String>) -> Result<()> {
    let path = state_dir.join(QUARANTINE_FILE);
    if paths.is_empty() {
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).context("clear the vfs change quarantine"),
        }
    } else {
        write_json_atomic(&path, paths).context("persist the vfs change quarantine")
    }
}

/// The gateway refused the credential (as opposed to being unreachable or failing).
fn is_auth_refusal(error: &anyhow::Error) -> bool {
    request_status(error).is_some_and(|status| status.as_u16() == 401 || status.as_u16() == 403)
}

/// What the gateway says about the change feed before a mount walks its scope.
pub(crate) enum FeedHead {
    /// It serves no change stream: poll.
    Unsupported,
    /// The head to stream from once the walk is done.
    At(ChangeCursor),
    /// It could not be asked; the follower starts without a cursor and resyncs.
    Unknown,
}

/// Read the feed head before hydrating, so a change committed during the walk is replayed
/// from it rather than lost (spec 86 D §4.5).
pub(crate) async fn read_feed_head(client: &RemoteVfsClient) -> FeedHead {
    let opened = match client.open_change_stream(None).await {
        Ok(ChangeStreamOpen::Unsupported) => return FeedHead::Unsupported,
        Ok(ChangeStreamOpen::Stream(stream)) => stream,
        Err(error) => {
            warn!(%error, "could not read the vfs change feed head before hydrating");
            return FeedHead::Unknown;
        }
    };
    match first_head(opened).await {
        Ok(cursor) => FeedHead::At(cursor),
        Err(error) => {
            warn!(%error, "the vfs change stream opened without a head");
            FeedHead::Unknown
        }
    }
}

async fn first_head(mut stream: ChangeStream) -> Result<ChangeCursor> {
    match tokio::time::timeout(Duration::from_secs(10), stream.next_frame()).await {
        Ok(Ok(Some(ChangeFrame::CaughtUp {
            epoch: Some(epoch),
            head,
        }))) => Ok(ChangeCursor { epoch, seq: head }),
        Ok(Ok(other)) => Err(anyhow!("unexpected first change frame {other:?}")),
        Ok(Err(error)) => Err(error),
        Err(_) => Err(anyhow!("no first change frame within 10s")),
    }
}

/// How one stream session ended.
enum SessionEnd {
    /// Open a new stream now.
    Reconnect,
    /// The gateway serves no change stream.
    Unsupported,
    /// The mount is going away.
    Stop,
}

pub(crate) struct ChangeStreamFollower {
    client: RemoteVfsClient,
    hydrator: Arc<MountHydrator>,
    invalidator: Arc<dyn KernelPathInvalidator>,
    state_dir: PathBuf,
    control: Arc<FollowControl>,
    computer_mounts: Option<Arc<ComputerMounts>>,
    cursor: Option<ChangeCursor>,
    /// When the current session got a stream open (a successful auth: resets the refusal
    /// count). Opening alone does not reset the failure backoff: a failure right after
    /// every open (a poison event, a gateway that closes at once, a failing resync walk)
    /// would otherwise retry every 0.5 s forever.
    opened: Option<Instant>,
    /// The current session applied something (advanced the cursor). Progress, or a session
    /// that stayed open `HEALTHY_SESSION` (an idle stream a proxy cut), resets the backoff.
    progressed: bool,
    /// Consecutive stream refusals with an exchanged token (see `MAX_AUTH_REFUSALS`).
    auth_refusals: u32,
    /// The cursor seq a batch keeps failing at, and how many times it has.
    batch_failures: Option<(i64, u32)>,
    /// Paths whose events were given up on, healed by the next walk.
    quarantine: BTreeSet<String>,
    cancel: Arc<Notify>,
    stopped: Arc<AtomicBool>,
}

/// Stops the follower when the mount goes away (dropped with the last `FuseHandle` clone).
pub(crate) struct ChangeStreamFollowerHandle {
    cancel: Arc<Notify>,
    stopped: Arc<AtomicBool>,
}

impl ChangeStreamFollowerHandle {
    fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.cancel.notify_waiters();
        self.cancel.notify_one();
    }
}

impl Drop for ChangeStreamFollowerHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

impl ChangeStreamFollower {
    pub(crate) fn spawn(
        client: RemoteVfsClient,
        hydrator: Arc<MountHydrator>,
        invalidator: Arc<dyn KernelPathInvalidator>,
        state_dir: PathBuf,
        control: Arc<FollowControl>,
        computer_mounts: Option<Arc<ComputerMounts>>,
        mounted: tokio::sync::oneshot::Receiver<()>,
    ) -> ChangeStreamFollowerHandle {
        let cancel = Arc::new(Notify::new());
        let stopped = Arc::new(AtomicBool::new(false));
        let quarantine = load_quarantine(&state_dir);
        let cursor = load_cursor(&state_dir);
        if let Some(cursor) = &cursor {
            control.note_applied(cursor.seq);
        }
        // Events were given up on before this restart: walk once to heal them.
        let cursor = cursor.filter(|_| quarantine.is_empty());
        let follower = Self {
            client,
            hydrator,
            invalidator,
            state_dir,
            control,
            computer_mounts,
            cursor,
            opened: None,
            progressed: false,
            auth_refusals: 0,
            batch_failures: None,
            quarantine,
            cancel: Arc::clone(&cancel),
            stopped: Arc::clone(&stopped),
        };
        tokio::spawn(async move {
            // Nothing follows until the kernel mount is live (a mount that failed never is).
            if mounted.await.is_ok() {
                follower.run().await;
            }
        });
        ChangeStreamFollowerHandle { cancel, stopped }
    }

    async fn run(mut self) {
        if let Some(mounts) = &self.computer_mounts {
            mounts.reconcile().await;
        }
        let mut backoff = BACKOFF_MIN;
        loop {
            if self.stopped.load(Ordering::Acquire) {
                break;
            }
            match self.session().await {
                Ok(SessionEnd::Reconnect) => backoff = BACKOFF_MIN,
                Ok(SessionEnd::Stop) => break,
                Ok(SessionEnd::Unsupported) => {
                    info!(
                        scope = %self.hydrator.view().scope_path(),
                        "the gateway serves no vfs change stream; following by polling"
                    );
                    self.poll().await;
                    break;
                }
                Err(error) => {
                    // A stream that opened means the credential was accepted. The backoff
                    // starts over only after the session made progress or stayed up a while
                    // (an idle stream a proxy cut): a failure right after every open would
                    // otherwise retry every 0.5 s forever (spec 86 review 35bac974).
                    let opened = self.opened.take();
                    let progressed = std::mem::take(&mut self.progressed);
                    if opened.is_some() {
                        self.auth_refusals = 0;
                    }
                    if progressed
                        || opened.is_some_and(|at| at.elapsed() >= HEALTHY_SESSION)
                    {
                        backoff = BACKOFF_MIN;
                    }
                    if is_auth_refusal(&error) {
                        self.auth_refusals += 1;
                        if self.auth_refusals >= MAX_AUTH_REFUSALS {
                            warn!(
                                scope = %self.hydrator.view().scope_path(),
                                error = %format!("{error:#}"),
                                refusals = self.auth_refusals,
                                "the gateway keeps refusing this mount's vfs change stream; \
                                 following by polling"
                            );
                            self.poll().await;
                            break;
                        }
                    } else {
                        self.auth_refusals = 0;
                    }
                    warn!(
                        scope = %self.hydrator.view().scope_path(),
                        error = %format!("{error:#}"),
                        retry_ms = backoff.as_millis() as u64,
                        "vfs change stream failed; reconnecting after a backoff"
                    );
                    if self.pause(jittered(backoff)).await {
                        break;
                    }
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                }
            }
        }
        debug!(scope = %self.hydrator.view().scope_path(), "vfs change follower stopped");
    }

    async fn poll(&self) {
        RemoteFollower::poll_until_stopped(
            self.client.clone(),
            Arc::clone(&self.hydrator),
            Arc::clone(&self.invalidator),
            Arc::clone(&self.cancel),
            Arc::clone(&self.stopped),
        )
        .await;
    }

    /// Sleep after a failure; `true` when the follower must stop. A reconnect request cuts
    /// the wait short.
    async fn pause(&self, duration: Duration) -> bool {
        if self.stopped.load(Ordering::Acquire) {
            return true;
        }
        tokio::select! {
            _ = self.cancel.notified() => true,
            _ = self.control.reconnect.notified() => {
                info!(scope = %self.hydrator.view().scope_path(), "vfs change stream retrying now on request");
                self.stopped.load(Ordering::Acquire)
            }
            _ = tokio::time::sleep(duration) => self.stopped.load(Ordering::Acquire),
        }
    }

    async fn session(&mut self) -> Result<SessionEnd> {
        let after = self.cursor.as_ref().map(ChangeCursor::wire);
        let opened = tokio::select! {
            opened = self.client.open_change_stream(after.as_deref()) => opened?,
            _ = self.cancel.notified() => return Ok(SessionEnd::Stop),
        };
        let mut stream = match opened {
            ChangeStreamOpen::Unsupported => return Ok(SessionEnd::Unsupported),
            ChangeStreamOpen::Stream(stream) => stream,
        };
        self.opened = Some(Instant::now());
        info!(
            scope = %self.hydrator.view().scope_path(),
            after = after.as_deref().unwrap_or("head"),
            "vfs change stream open"
        );
        loop {
            let frame = tokio::select! {
                frame = stream.next_frame() => frame?,
                _ = self.cancel.notified() => return Ok(SessionEnd::Stop),
                _ = self.control.reconnect.notified() => {
                    info!(scope = %self.hydrator.view().scope_path(), "vfs change stream reconnecting on request");
                    return Ok(SessionEnd::Reconnect);
                }
            };
            let Some(frame) = frame else {
                bail!("the gateway closed the vfs change stream");
            };
            match frame {
                ChangeFrame::Changes { head, events } => {
                    let epoch = self
                        .cursor
                        .as_ref()
                        .map(|cursor| cursor.epoch.clone())
                        .ok_or_else(|| anyhow!("change events arrived before a cursor"))?;
                    self.apply_batch(&events).await?;
                    self.advance(ChangeCursor { epoch, seq: head }).await?;
                }
                ChangeFrame::CaughtUp { epoch, head } => {
                    if self.cursor.is_none() {
                        // Opened without a cursor: walk the scope, then continue from the head
                        // read before the walk (the stream keeps the changes since).
                        let epoch = epoch.ok_or_else(|| anyhow!("caught_up without an epoch"))?;
                        self.walk("no cursor").await?;
                        self.advance(ChangeCursor { epoch, seq: head }).await?;
                    }
                }
                ChangeFrame::Resync {
                    epoch,
                    head,
                    reason,
                } => {
                    self.walk(&reason).await?;
                    self.advance(ChangeCursor { epoch, seq: head }).await?;
                    return Ok(SessionEnd::Reconnect);
                }
                ChangeFrame::Reauth => {
                    self.client.reauthenticate().await;
                    return Ok(SessionEnd::Reconnect);
                }
            }
        }
    }

    /// Apply one batch, bounding how often the same batch is retried: after
    /// `MAX_BATCH_ATTEMPTS` failures at one cursor its events are applied one at a time, and
    /// any that still fail are logged and quarantined (the next walk heals them) so the
    /// changes behind them are not stuck.
    async fn apply_batch(&mut self, events: &[ChangeEvent]) -> Result<()> {
        let at = self.cursor.as_ref().map_or(0, |cursor| cursor.seq);
        let error = match self.apply(events).await {
            Ok(()) => {
                self.batch_failures = None;
                return Ok(());
            }
            Err(error) => error,
        };
        let attempts = match &mut self.batch_failures {
            Some((seq, attempts)) if *seq == at => {
                *attempts += 1;
                *attempts
            }
            _ => {
                self.batch_failures = Some((at, 1));
                1
            }
        };
        if attempts < MAX_BATCH_ATTEMPTS {
            return Err(error);
        }
        warn!(
            scope = %self.hydrator.view().scope_path(),
            after = at,
            attempts,
            error = %format!("{error:#}"),
            "a vfs change batch keeps failing; applying its events one at a time"
        );
        for event in events {
            if let Err(error) = self.apply(std::slice::from_ref(event)).await {
                warn!(
                    scope = %self.hydrator.view().scope_path(),
                    seq = event.seq,
                    path = %event.path,
                    error = %format!("{error:#}"),
                    "quarantined a vfs change event; the next resync heals it"
                );
                if let Some(path) = self.client.scope_relative(&event.path) {
                    self.quarantine.insert(path);
                }
            }
        }
        store_quarantine(&self.state_dir, &self.quarantine)?;
        self.batch_failures = None;
        Ok(())
    }

    /// Converge on the whole scope (no usable cursor): the same walk the polling follower
    /// does, then revoke everything the kernel cached. Heals every quarantined path.
    async fn walk(&mut self, reason: &str) -> Result<()> {
        let started = Instant::now();
        let outcome = self
            .hydrator
            .rehydrate_paths(&[], &[String::new()])
            .await
            .context("resync the vfs scope")?;
        if let Some(mounts) = &self.computer_mounts {
            mounts.pin_before_sweep().await;
        }
        self.invalidator.invalidate_all();
        info!(
            scope = %self.hydrator.view().scope_path(),
            reason,
            files = outcome.files,
            bytes = outcome.bytes,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "vfs change follower resynced its scope"
        );
        if !self.quarantine.is_empty() {
            self.quarantine.clear();
            store_quarantine(&self.state_dir, &self.quarantine)?;
        }
        if let Some(mounts) = &self.computer_mounts {
            mounts.reconcile().await;
        }
        Ok(())
    }

    /// Apply one batch: deletes locally, unchanged content skipped, the rest re-read.
    async fn apply(&self, events: &[ChangeEvent]) -> Result<()> {
        let started = Instant::now();
        let mut paths: Vec<String> = Vec::new();
        let mut subtrees: Vec<String> = Vec::new();
        let mut deleted: Vec<String> = Vec::new();
        let mut chmodded: Vec<String> = Vec::new();
        let mut skipped = 0usize;
        let mut touches_mounts = false;
        for event in events {
            let Some(path) = self.client.scope_relative(&event.path) else {
                continue;
            };
            if let Some(mounts) = &self.computer_mounts {
                touches_mounts |= mounts.is_manifest(&path);
            }
            match event.op.as_str() {
                "delete" => {
                    if path.is_empty() || self.hydrator.locally_absent(&path)? {
                        skipped += 1;
                    } else {
                        self.hydrator.apply_remote_delete(&path)?;
                        deleted.push(path);
                    }
                }
                "subtree" => subtrees.push(path),
                _ => {
                    let current = match event.kind.as_deref() {
                        Some("directory") => self.hydrator.locally_directory(&path)?,
                        _ => {
                            event.hash.is_some()
                                && self.hydrator.locally_current(
                                    &path,
                                    event.size.unwrap_or(0),
                                    event.hash.as_deref(),
                                )?
                        }
                    };
                    if current {
                        // Same content: only the mode may have changed (a chmod).
                        if let Some(mode) = event.mode
                            && self.hydrator.apply_remote_mode(&path, mode)?
                        {
                            chmodded.push(path);
                        } else {
                            skipped += 1;
                        }
                    } else {
                        paths.push(path);
                    }
                }
            }
        }
        if !paths.is_empty() || !subtrees.is_empty() {
            self.hydrator
                .rehydrate_paths(&paths, &subtrees)
                .await
                .context("apply vfs change events")?;
            if !paths.is_empty() {
                self.invalidator.invalidate_paths(&paths);
            }
            if !subtrees.is_empty() {
                self.invalidator.invalidate_subtrees(&subtrees);
            }
        }
        if !deleted.is_empty() {
            self.invalidator.invalidate_paths(&deleted);
        }
        if !chmodded.is_empty() {
            self.invalidator.invalidate_paths(&chmodded);
        }
        if touches_mounts && let Some(mounts) = &self.computer_mounts {
            mounts.reconcile().await;
        }
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|now| now.as_millis() as i64)
            .unwrap_or_default();
        let delivery_ms = events
            .iter()
            .filter_map(|event| event.at_ms)
            .map(|at| now_ms - at)
            .max();
        info!(
            scope = %self.hydrator.view().scope_path(),
            events = events.len(),
            fetched = paths.len(),
            walked = subtrees.len(),
            deleted = deleted.len(),
            chmodded = chmodded.len(),
            skipped,
            apply_ms = started.elapsed().as_millis() as u64,
            nymfs_change_delivery_ms = delivery_ms,
            "applied vfs change events"
        );
        Ok(())
    }

    /// Persist the cursor, then tell the gateway (its barrier waits on it).
    async fn advance(&mut self, cursor: ChangeCursor) -> Result<()> {
        store_cursor(&self.state_dir, &cursor)?;
        self.progressed = true;
        self.control.note_applied(cursor.seq);
        if self.control.reports_applied
            && let Err(error) = self
                .client
                .report_changes_applied(&cursor.epoch, cursor.seq)
                .await
        {
            debug!(%error, "could not report applied vfs changes");
        }
        self.cursor = Some(cursor);
        Ok(())
    }
}

fn jittered(base: Duration) -> Duration {
    let spread = base.as_millis() as u64 / 4;
    let offset = if spread == 0 {
        0
    } else {
        rand::random::<u64>() % (spread * 2)
    };
    Duration::from_millis((base.as_millis() as u64).saturating_sub(spread) + offset)
}
