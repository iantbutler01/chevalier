//! The ordered asynchronous publisher: the **only** component that mutates the
//! gateway.
//!
//! It reads committed WAL records, adapts them onto the existing `namespace-many`
//! and `write-many` routes, and advances the acknowledgement cursor. It never
//! touches the backing tree, never invalidates anything the mount serves, and is
//! never awaited by an ordinary FUSE callback. A gateway outage is backpressure
//! on replication, not on the guest.
//!
//! ## Batching and order-aware parallelism
//!
//! After a short idle/size window (10 ms, capped at 100 ms) the coordinator pulls
//! one contiguous committed prefix and splits it into **runs**:
//!
//! * `Namespace` -- consecutive namespace mutations issued as one ordered
//!   `namespace-many`; the gateway applies the array in order, so a run may carry
//!   dependent events.
//! * `Content` -- consecutive content generations. Their dependency key sets are
//!   pairwise non-conflicting by construction (a same-path repeat splits the
//!   run), so payload uploads inside a run may proceed concurrently through a
//!   bounded pool.
//! * `Cursor` -- local-only events (`SetTimes`, `SetOwner`) and aborted gaps: the
//!   cursor advances with no request at all.
//!
//! Runs are issued in dependency order and awaited before the next one starts.
//! Independent paths may move across a route-class boundary so a batch does not
//! alternate `namespace-many`, `write-many`, `namespace-many`, `write-many` for
//! thousands of unrelated files. Conflicting paths retain their exact WAL
//! order; namespace events within one request retain their WAL order too.
//!
//! ## The creation fold
//!
//! A `CreateFile{path,mode}` followed by a `ReplaceFile{path}` with no
//! intervening conflicting event is folded into one `write-many` item carrying
//! `mode` and an `Absent` precondition. `SetMode` mutations for that new file
//! are folded into the same write, while owner/time mutations remain local-only.
//! This collapses the dominant copy/install pattern even when many worker tasks
//! interleave their operations across unrelated files.
//!
//! ## Crash-safe idempotency
//!
//! `operation_ids` are **not** deduplicated across requests by the gateway: it
//! validates uniqueness within a batch and drops them. Idempotency is therefore
//! client-side. Every event's `{epoch}:{sequence}` key is stable across restart,
//! and a rejection is reconciled rather than trusted:
//!
//! * `rejected_request_status` classifies permanence -- only `400`, `409` and
//!   `412` can never succeed. `401/403` (auth), `404` (route skew during a
//!   deploy), `408` and `429` are transient and must be retried, never
//!   dead-lettered.
//! * On a permanent rejection the publisher re-reads the affected paths and
//!   compares them with the event's intent (content hash, identity, kind, mode,
//!   absence). A match means the event already landed on an earlier attempt:
//!   acknowledge it. A genuine mismatch **blocks the WAL suffix**: the event is
//!   preserved, publication stops at that sequence, and the failure is surfaced
//!   through `PublicationHealth`. It is never silently dropped, because the local
//!   accepted view is authoritative.
//!
//! ## GCS parity
//!
//! Every request goes through the gateway contract, so the local and object-store
//! backends consume the same ordered batches and acknowledge the same sequence
//! semantics. There is no backend-specific path here and there must never be one.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use chevalier_sandbox::vfs::{
    VfsMetadata as RemoteMetadata, VfsNamespaceMutation, VfsWritePrecondition,
};
use tokio::runtime::Handle;
use tokio::sync::{Notify, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, sleep};

use super::types::{
    DrainOutcome, LocalKind, MountEvent, MountMutation, PublicationHealth, PublishBatch,
    PublishRun, StoragePressure, dependency_sets_conflict,
};
use super::wal::{MountWal, WalNotify};
use crate::fuse::client::{
    RemotePublication, RemoteVfsClient, RemoteWrite, rejected_request_status,
};

/// The `base_content_hash` value `scope_remote_write` turns into
/// `VfsCasPredicate::Absent`. A folded creation is expressed with it rather than
/// with a second wire field, which is why the fold needs no client change.
const ABSENT_PRECONDITION: &str = "absent";

/// Backoff applied between publication attempts after a stall. A permanently
/// rejected event never leaves this loop -- it blocks its suffix and is retried
/// forever, because only the gateway coming back into agreement can resolve it.
const RETRY_BACKOFF_MIN: Duration = Duration::from_millis(100);
const RETRY_BACKOFF_MAX: Duration = Duration::from_secs(5);

/// Longest a drain waits between cursor re-reads when no progress notification
/// arrives. Purely a liveness backstop; the progress notify is the fast path.
const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// How long `shutdown` waits for the coordinator to observe the stop flag before
/// aborting it. Aborting is safe: nothing is acknowledged until a publication
/// returns, so an aborted request simply republishes on the next mount.
const SHUTDOWN_JOIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Reads the backing tree's mutation counter for the checkpoint an
/// acknowledgement publishes.
///
/// The publisher deliberately holds no reference to `BackingTree` -- it must not
/// be able to touch it -- so the mount hands it this one read-only accessor when
/// it spawns the publisher.
#[derive(Clone)]
pub(crate) struct TreeGenerationSource(Arc<dyn Fn() -> u64 + Send + Sync>);

impl TreeGenerationSource {
    pub(crate) fn new(source: impl Fn() -> u64 + Send + Sync + 'static) -> Self {
        Self(Arc::new(source))
    }

    pub(crate) fn current(&self) -> u64 {
        (self.0)()
    }
}

impl std::fmt::Debug for TreeGenerationSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TreeGenerationSource")
    }
}

/// Read-only access to the mount's current authoritative path state.
///
/// Rejection recovery needs this only for a replayed rename whose old and new
/// paths both exist remotely. The event history alone cannot distinguish a
/// legitimate conflict from a stale temporary subtree recreated by an earlier
/// partially-applied replay; the current local view can.
#[derive(Clone)]
pub(crate) struct AuthoritativePathSource(
    Arc<dyn Fn(&str) -> Result<Option<LocalKind>> + Send + Sync>,
);

impl AuthoritativePathSource {
    pub(crate) fn new(
        source: impl Fn(&str) -> Result<Option<LocalKind>> + Send + Sync + 'static,
    ) -> Self {
        Self(Arc::new(source))
    }

    fn kind(&self, path: &str) -> Result<Option<LocalKind>> {
        (self.0)(path)
    }
}

impl std::fmt::Debug for AuthoritativePathSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AuthoritativePathSource")
    }
}

/// Tunables the mount passes in, defaulted from the module constants.
#[derive(Clone, Debug)]
pub(crate) struct PublisherOptions {
    pub(crate) max_events: usize,
    pub(crate) max_payload_bytes: u64,
    pub(crate) idle_window: Duration,
    pub(crate) max_window: Duration,
    pub(crate) concurrency: usize,
    pub(crate) stream_threshold_bytes: u64,
    pub(crate) surface_kind: String,
    /// Supplied by the mount so an acknowledgement's checkpoint witnesses the
    /// backing tree generation. `None` records `0`, which the WAL reads as
    /// "unknown -- keep the generation the previous checkpoint carried".
    pub(crate) tree_generation: Option<TreeGenerationSource>,
    /// Current local path state, used only to resolve ambiguous replayed
    /// namespace operations after a gateway rejection.
    pub(crate) authoritative_paths: Option<AuthoritativePathSource>,
}

impl PublisherOptions {
    pub(crate) fn defaults(surface_kind: &str) -> Self {
        Self {
            max_events: super::PUBLISH_MAX_EVENTS,
            max_payload_bytes: super::PUBLISH_MAX_PAYLOAD_BYTES,
            idle_window: Duration::from_millis(super::PUBLISH_IDLE_WINDOW_MS),
            max_window: Duration::from_millis(super::PUBLISH_MAX_WINDOW_MS),
            concurrency: super::PUBLISH_CONCURRENCY,
            stream_threshold_bytes: super::STREAM_PAYLOAD_THRESHOLD_BYTES,
            surface_kind: surface_kind.to_string(),
            tree_generation: None,
            authoritative_paths: None,
        }
    }

    pub(crate) fn with_tree_generation(mut self, source: TreeGenerationSource) -> Self {
        self.tree_generation = Some(source);
        self
    }

    pub(crate) fn with_authoritative_paths(mut self, source: AuthoritativePathSource) -> Self {
        self.authoritative_paths = Some(source);
        self
    }
}

/// Owns the event order and the remote acknowledgement cursor. Not a single
/// serialized worker: it coordinates a bounded pool.
pub(crate) struct MountPublisher {
    shared: Arc<PublisherShared>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl MountPublisher {
    /// Spawn the coordinator on the mount's tokio runtime.
    pub(crate) fn spawn(
        client: RemoteVfsClient,
        wal: MountWal,
        tokio: Handle,
        options: PublisherOptions,
    ) -> Arc<Self> {
        let shared = Arc::new(PublisherShared {
            wake: wal.notify_handle(),
            client,
            wal,
            options,
            progress: Notify::new(),
            stop: Notify::new(),
            stopping: AtomicBool::new(false),
            state: Mutex::new(PublisherState::default()),
        });
        let task = tokio.spawn(coordinate(Arc::clone(&shared)));
        Arc::new(Self {
            shared,
            task: Mutex::new(Some(task)),
        })
    }

    /// Wake the coordinator. Called from a native FUSE request thread after a
    /// commit; must never block.
    pub(crate) fn notify(&self) {
        self.shared.wake.notify_one();
    }

    pub(crate) fn health(&self) -> PublicationHealth {
        let state = self.shared.state();
        let pressure = self.shared.wal.storage_pressure();
        PublicationHealth {
            acknowledged_sequence: self.shared.wal.acknowledged_sequence(),
            last_committed_sequence: self.shared.wal.last_committed_sequence(),
            pending_events: self.shared.wal.pending_publication_depth(),
            pending_payload_bytes: self.shared.wal.pending_payload_bytes(),
            blocked_sequence: state.blocked_sequence,
            blocked_reason: state.blocked_reason.clone(),
            last_error: state.last_error.clone(),
            storage_pressure_soft: !matches!(pressure, StoragePressure::None),
            storage_pressure_hard: pressure.blocks_content(),
        }
    }

    /// Wait until the acknowledgement cursor covers everything committed at the
    /// moment of the call.
    pub(crate) async fn drain(&self, deadline: Duration) -> Result<DrainOutcome> {
        let committed = self.shared.wal.last_committed_sequence();
        self.drain_through(committed, deadline).await
    }

    /// Wait until the cursor covers one specific sequence.
    pub(crate) async fn drain_through(
        &self,
        sequence: u64,
        deadline: Duration,
    ) -> Result<DrainOutcome> {
        let expiry = Instant::now() + deadline;
        loop {
            // Register for the progress signal *before* reading the cursor, so a
            // publication that completes between the read and the wait cannot be
            // missed.
            let notified = self.shared.progress.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            self.shared.wake.notify_one();
            let acknowledged = self.shared.wal.acknowledged_sequence();
            if acknowledged >= sequence {
                return Ok(DrainOutcome::Drained {
                    through_sequence: acknowledged,
                });
            }
            if let Some((blocked_sequence, reason)) = self.shared.blocked()
                && blocked_sequence <= sequence
            {
                return Ok(DrainOutcome::Blocked {
                    blocked_sequence,
                    reason,
                });
            }
            let now = Instant::now();
            if now >= expiry {
                return Ok(DrainOutcome::TimedOut {
                    acknowledged_sequence: acknowledged,
                    pending_events: self.shared.wal.pending_publication_depth(),
                });
            }
            let wait = DRAIN_POLL_INTERVAL.min(expiry - now);
            tokio::select! {
                _ = &mut notified => {}
                _ = sleep(wait) => {}
            }
        }
    }

    /// Drain then stop. Idempotent.
    pub(crate) async fn shutdown(&self, deadline: Duration) -> Result<DrainOutcome> {
        let outcome = self.drain(deadline).await?;
        self.shared.stopping.store(true, Ordering::Release);
        self.shared.stop.notify_waiters();
        self.shared.wake.notify_one();
        let task = self
            .task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(mut task) = task
            && tokio::time::timeout(SHUTDOWN_JOIN_TIMEOUT, &mut task)
                .await
                .is_err()
        {
            task.abort();
        }
        Ok(outcome)
    }
}

// ---------------------------------------------------------------------------
// Coordinator
// ---------------------------------------------------------------------------

#[derive(Default)]
struct PublisherState {
    blocked_sequence: Option<u64>,
    blocked_reason: Option<String>,
    last_error: Option<String>,
}

struct PublisherShared {
    client: RemoteVfsClient,
    wal: MountWal,
    options: PublisherOptions,
    /// The WAL's own notify: every commit wakes it, and so does `notify()`.
    wake: WalNotify,
    /// Signalled after every cursor advance and every health change, so a drain
    /// never has to poll tightly.
    progress: Notify,
    stop: Notify,
    stopping: AtomicBool,
    state: Mutex<PublisherState>,
}

/// What one pass over the committed prefix achieved.
enum PublishProgress {
    /// Nothing publishable right now.
    Idle,
    /// At least one run was acknowledged; try again immediately.
    Advanced,
    /// A run failed. The suffix is preserved and retried after a backoff.
    Stalled,
}

/// One run's result. The cursor advances only after the whole
/// dependency-reordered batch succeeds. Replaying an already-landed prefix is
/// safe through rejection reconciliation; acknowledging it independently would
/// not be safe when a folded earlier sequence is represented by a later run.
enum RunOutcome {
    Complete { revision: u64 },
    Failed { failure: PublishFailure },
}

#[derive(Clone, Debug)]
struct PublishFailure {
    sequence: u64,
    /// `true` only when the gateway rejected the payload permanently *and* a
    /// re-read proved the event did not land. Everything else is retried.
    permanent: bool,
    reason: String,
}

impl PublishFailure {
    fn transient(sequence: u64, reason: String) -> Self {
        Self {
            sequence,
            permanent: false,
            reason,
        }
    }

    fn permanent(sequence: u64, reason: String) -> Self {
        Self {
            sequence,
            permanent: true,
            reason,
        }
    }
}

async fn coordinate(shared: Arc<PublisherShared>) {
    let mut backoff = RETRY_BACKOFF_MIN;
    loop {
        if shared.stopping.load(Ordering::Acquire) {
            break;
        }
        match shared.publish_once().await {
            Ok(PublishProgress::Advanced) => {
                backoff = RETRY_BACKOFF_MIN;
                shared.coalesce_live_tail().await;
            }
            Ok(PublishProgress::Idle) => {
                backoff = RETRY_BACKOFF_MIN;
                shared.wait_for_work().await;
            }
            Ok(PublishProgress::Stalled) => {
                shared.sleep_or_stop(backoff).await;
                backoff = (backoff * 2).min(RETRY_BACKOFF_MAX);
            }
            Err(error) => {
                shared.record_error(format!("{error:#}"));
                tracing::warn!(error = %format!("{error:#}"), "vfs mount publication pass failed");
                shared.sleep_or_stop(backoff).await;
                backoff = (backoff * 2).min(RETRY_BACKOFF_MAX);
            }
        }
    }
}

impl PublisherShared {
    fn state(&self) -> std::sync::MutexGuard<'_, PublisherState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn blocked(&self) -> Option<(u64, String)> {
        let state = self.state();
        let sequence = state.blocked_sequence?;
        Some((
            sequence,
            state
                .blocked_reason
                .clone()
                .unwrap_or_else(|| "publication blocked".to_string()),
        ))
    }

    fn record_error(&self, reason: String) {
        self.state().last_error = Some(reason);
        self.progress.notify_waiters();
    }

    fn record_failure(&self, failure: &PublishFailure) {
        {
            let mut state = self.state();
            state.last_error = Some(failure.reason.clone());
            if failure.permanent {
                state.blocked_sequence = Some(failure.sequence);
                state.blocked_reason = Some(failure.reason.clone());
            } else {
                state.blocked_sequence = None;
                state.blocked_reason = None;
            }
        }
        if failure.permanent {
            tracing::error!(
                sequence = failure.sequence,
                reason = %failure.reason,
                "vfs mount publication blocked: the local view is authoritative and the event is \
                 preserved, but its WAL suffix cannot be published"
            );
        } else {
            tracing::warn!(
                sequence = failure.sequence,
                reason = %failure.reason,
                "vfs mount publication retrying"
            );
        }
        self.progress.notify_waiters();
    }

    fn clear_failure(&self) {
        let mut state = self.state();
        state.blocked_sequence = None;
        state.blocked_reason = None;
        state.last_error = None;
    }

    /// Block until a commit arrives, then coalesce: extend the window while
    /// commits keep coming, but never past `max_window`.
    async fn wait_for_work(&self) {
        tokio::select! {
            _ = self.wake.notified() => {}
            _ = self.stop.notified() => return,
        }
        let deadline = Instant::now() + self.options.max_window;
        loop {
            if self.stopping.load(Ordering::Acquire) {
                return;
            }
            let now = Instant::now();
            if now >= deadline {
                return;
            }
            let idle = self.options.idle_window.min(deadline - now);
            tokio::select! {
                _ = sleep(idle) => return,
                _ = self.wake.notified() => {}
                _ = self.stop.notified() => return,
            }
        }
    }

    /// Let a producer that is still appending fill the next request instead of
    /// chasing its live tail one syscall at a time.
    ///
    /// The initial idle transition already coalesces, but without this second
    /// window a publisher that completes its first request while the guest is
    /// still writing immediately reads whatever tiny suffix exists at that
    /// instant. It then stays in that request-per-file loop indefinitely. A
    /// real backlog (at least one full batch) skips the window and drains at
    /// full speed, so outage recovery is not artificially rate-limited.
    async fn coalesce_live_tail(&self) {
        if self.wal.pending_publication_depth() >= self.options.max_events as u64 {
            return;
        }
        let deadline = Instant::now() + self.options.max_window;
        loop {
            if self.stopping.load(Ordering::Acquire)
                || self.wal.pending_publication_depth() >= self.options.max_events as u64
            {
                return;
            }
            let now = Instant::now();
            if now >= deadline {
                return;
            }
            let idle = self.options.idle_window.min(deadline - now);
            tokio::select! {
                _ = sleep(idle) => return,
                _ = self.wake.notified() => {}
                _ = self.stop.notified() => return,
            }
        }
    }

    async fn sleep_or_stop(&self, duration: Duration) {
        tokio::select! {
            _ = sleep(duration) => {}
            _ = self.stop.notified() => {}
        }
    }

    fn tree_generation(&self) -> u64 {
        self.options
            .tree_generation
            .as_ref()
            .map(TreeGenerationSource::current)
            .unwrap_or(0)
    }

    /// Advance the cursor. A remote acknowledgement moves this and nothing else:
    /// it never evicts, invalidates, or rolls back anything the mount serves.
    fn acknowledge(&self, through_sequence: u64, revision: u64) -> Result<()> {
        if through_sequence <= self.wal.acknowledged_sequence() {
            return Ok(());
        }
        let revision = revision.max(self.wal.remote_revision());
        self.wal
            .acknowledge(through_sequence, revision, self.tree_generation())
            .with_context(|| {
                format!("acknowledge vfs mount publication through {through_sequence}")
            })?;
        self.progress.notify_waiters();
        Ok(())
    }

    async fn publish_once(&self) -> Result<PublishProgress> {
        let cursor_before = self.wal.acknowledged_sequence();
        let Some(batch) = self.wal.next_publish_batch(
            self.options.max_events,
            self.options.max_payload_bytes,
            self.options.stream_threshold_bytes,
        )?
        else {
            return Ok(PublishProgress::Idle);
        };
        let runs = plan_runs(&batch)?;
        if runs.is_empty() {
            return Ok(PublishProgress::Idle);
        }
        let mut remote_revision = self.wal.remote_revision();
        for run in runs {
            match self.issue_run(run).await? {
                RunOutcome::Complete { revision } => {
                    remote_revision = remote_revision.max(revision);
                }
                RunOutcome::Failed { failure } => {
                    self.record_failure(&failure);
                    return Ok(PublishProgress::Stalled);
                }
            }
        }
        // One committed prefix gets one durable acknowledgement. Checkpointing
        // every route-class run turns workloads that alternate namespace and
        // content events into an fsync + atomic checkpoint rewrite per file.
        // If the process dies before this acknowledgement, replaying an already
        // landed run is safe because rejection reconciliation is idempotent.
        self.acknowledge(batch.through_sequence, remote_revision)?;
        // Every run of a non-empty batch acknowledged, so the cursor must have
        // moved. Reporting `Advanced` without it would spin the coordinator on
        // the same prefix, so a cursor that stood still is treated as a stall.
        if self.wal.acknowledged_sequence() > cursor_before {
            self.clear_failure();
            self.progress.notify_waiters();
            return Ok(PublishProgress::Advanced);
        }
        self.record_error(format!(
            "publication of vfs mount sequences through {} left the acknowledgement cursor at {}",
            batch.through_sequence, cursor_before
        ));
        Ok(PublishProgress::Stalled)
    }

    async fn issue_run(&self, run: PublishRun) -> Result<RunOutcome> {
        match run {
            // Local-only mutations and aborted gaps: the cursor moves with no
            // request at all. The divergence is recorded, never silent.
            PublishRun::Cursor { .. } => Ok(RunOutcome::Complete {
                revision: self.wal.remote_revision(),
            }),
            PublishRun::Namespace { events, .. } => self.publish_namespace(events).await,
            PublishRun::Content { events, .. } => self.publish_content(events).await,
        }
    }

    // -- namespace ----------------------------------------------------------

    async fn publish_namespace(&self, events: Vec<MountEvent>) -> Result<RunOutcome> {
        match self.issue_namespace_events(&events).await {
            Ok(publication) => Ok(RunOutcome::Complete {
                revision: publication.revision,
            }),
            Err(error) => {
                let reason = format!("{error:#}");
                if rejected_request_status(&error).is_none() {
                    return Ok(RunOutcome::Failed {
                        failure: PublishFailure::transient(events[0].sequence, reason),
                    });
                }
                // The local backend applies an ordered namespace batch
                // sequentially, so a rejected request may have committed a
                // prefix. Isolate it in original order instead of declaring the
                // first unapplied collateral event to be the conflict.
                self.isolate_rejected_namespace(events, reason, 0).await
            }
        }
    }

    async fn issue_namespace_events(&self, events: &[MountEvent]) -> Result<RemotePublication> {
        let mut operation_ids = Vec::with_capacity(events.len());
        let mut mutations = Vec::with_capacity(events.len());
        for event in events {
            operation_ids.push(event.idempotency_key.clone());
            mutations.push(namespace_mutation_for(event)?);
        }
        self.client
            .apply_namespace_batch(&operation_ids, &mutations, &self.options.surface_kind)
            .await
    }

    async fn isolate_rejected_namespace(
        &self,
        events: Vec<MountEvent>,
        reason: String,
        initial_revision: u64,
    ) -> Result<RunOutcome> {
        // `Some(reason)` is a group already known to have rejected. `None` is
        // an isolated subgroup ready to retry. Right is pushed before left so
        // the LIFO stack never changes namespace order.
        let mut pending = vec![(events, Some(reason))];
        let mut revision = initial_revision;
        while let Some((mut events, rejection)) = pending.pop() {
            if let Some(reason) = rejection {
                if events.len() == 1 {
                    let event = &events[0];
                    let mut snapshots = RemoteSnapshots::new(&self.client);
                    snapshots.observe_revision(revision);
                    match self.event_landed(event, &mut snapshots).await {
                        Ok(true) => {
                            revision = revision.max(snapshots.revision());
                        }
                        Ok(false) => {
                            return Ok(RunOutcome::Failed {
                                failure: PublishFailure::permanent(event.sequence, reason),
                            });
                        }
                        Err(error) => {
                            return Ok(RunOutcome::Failed {
                                failure: PublishFailure::transient(
                                    event.sequence,
                                    format!("{reason}; reconciliation read failed: {error:#}"),
                                ),
                            });
                        }
                    }
                    continue;
                }
                let right = events.split_off(events.len() / 2);
                pending.push((right, None));
                pending.push((events, None));
                continue;
            }

            match self.issue_namespace_events(&events).await {
                Ok(publication) => {
                    revision = revision.max(publication.revision);
                }
                Err(error) => {
                    let reason = format!("{error:#}");
                    if rejected_request_status(&error).is_none() {
                        return Ok(RunOutcome::Failed {
                            failure: PublishFailure::transient(events[0].sequence, reason),
                        });
                    }
                    pending.push((events, Some(reason)));
                }
            }
        }
        Ok(RunOutcome::Complete { revision })
    }

    // -- content ------------------------------------------------------------

    async fn publish_content(&self, events: Vec<MountEvent>) -> Result<RunOutcome> {
        let requests = self.plan_content_requests(&events)?;
        let permits = Arc::new(Semaphore::new(self.options.concurrency.max(1)));
        let mut tasks: JoinSet<ContentTaskResult> = JoinSet::new();
        for request in requests {
            let client = self.client.clone();
            let wal = self.wal.clone();
            let surface_kind = self.options.surface_kind.clone();
            let permits = Arc::clone(&permits);
            tasks.spawn(async move {
                // The permit is taken inside the task so the spawn loop never
                // blocks the coordinator; `PUBLISH_CONCURRENCY` bounds only the
                // requests actually in flight.
                let _permit = permits.acquire_owned().await.ok();
                let outcome =
                    issue_content_request(&client, &wal, &request, surface_kind.as_str()).await;
                ContentTaskResult {
                    events: request.events,
                    outcome,
                }
            });
        }

        let mut publications: Vec<RemotePublication> = Vec::new();
        let mut failures: Vec<(Vec<MountEvent>, anyhow::Error)> = Vec::new();
        while let Some(joined) = tasks.join_next().await {
            let result = joined.context("join vfs mount content publication task")?;
            match result.outcome {
                Ok(publication) => publications.push(publication),
                Err(error) => failures.push((result.events, error)),
            }
        }

        let revision = publications
            .iter()
            .map(|publication| publication.revision)
            .max()
            .unwrap_or(0);
        if failures.is_empty() {
            return Ok(RunOutcome::Complete { revision });
        }
        self.reconcile_content(failures, revision).await
    }

    /// Chunk a content run into concurrently issuable requests: small payloads
    /// batch into `write-many`, large ones stream individually from their
    /// dedicated payload file.
    fn plan_content_requests(&self, events: &[MountEvent]) -> Result<Vec<ContentRequest>> {
        let concurrency = self.options.concurrency.max(1);
        let chunk_bytes = (self.options.max_payload_bytes / concurrency as u64)
            .max(super::MAX_SEGMENTED_PAYLOAD_BYTES as u64);
        let chunk_events = (self.options.max_events / concurrency).max(1);
        let mut requests: Vec<ContentRequest> = Vec::new();
        let mut batched: Vec<MountEvent> = Vec::new();
        let mut batched_bytes: u64 = 0;
        for event in events {
            let payload = event
                .payload
                .as_ref()
                .ok_or_else(|| anyhow!("content event {} carries no payload", event.sequence))?;
            if payload.length >= self.options.stream_threshold_bytes {
                requests.push(ContentRequest {
                    kind: ContentRequestKind::Streamed,
                    events: vec![event.clone()],
                });
                continue;
            }
            if !batched.is_empty()
                && (batched.len() >= chunk_events
                    || batched_bytes.saturating_add(payload.length) > chunk_bytes)
            {
                requests.push(ContentRequest {
                    kind: ContentRequestKind::Batched,
                    events: std::mem::take(&mut batched),
                });
                batched_bytes = 0;
            }
            batched_bytes = batched_bytes.saturating_add(payload.length);
            batched.push(event.clone());
        }
        if !batched.is_empty() {
            requests.push(ContentRequest {
                kind: ContentRequestKind::Batched,
                events: batched,
            });
        }
        Ok(requests)
    }

    /// Content events inside a run are independent, so a rejected request can
    /// be isolated by dependency-safe bisection. A batch-level 409 does not say
    /// which item conflicted: treating its first missing item as permanent would
    /// deadlock on collateral work that the atomic request never attempted.
    async fn reconcile_content(
        &self,
        failures: Vec<(Vec<MountEvent>, anyhow::Error)>,
        mut revision: u64,
    ) -> Result<RunOutcome> {
        for (failed_events, error) in failures {
            let reason = format!("{error:#}");
            if rejected_request_status(&error).is_none() {
                return Ok(RunOutcome::Failed {
                    failure: PublishFailure::transient(failed_events[0].sequence, reason),
                });
            }
            match self
                .isolate_rejected_content(failed_events, reason, revision)
                .await?
            {
                RunOutcome::Complete { revision: resolved } => revision = revision.max(resolved),
                failed @ RunOutcome::Failed { .. } => return Ok(failed),
            }
        }
        Ok(RunOutcome::Complete { revision })
    }

    async fn isolate_rejected_content(
        &self,
        events: Vec<MountEvent>,
        reason: String,
        initial_revision: u64,
    ) -> Result<RunOutcome> {
        // `Some(reason)` means this group was just rejected and must be
        // reconciled. `None` means it is an isolated subgroup ready to retry.
        let mut pending = vec![(events, Some(reason))];
        let mut revision = initial_revision;
        while let Some((events, rejection)) = pending.pop() {
            let Some(reason) = rejection else {
                let request = ContentRequest {
                    kind: if events.len() == 1
                        && events[0].payload.as_ref().is_some_and(|payload| {
                            payload.length >= self.options.stream_threshold_bytes
                        }) {
                        ContentRequestKind::Streamed
                    } else {
                        ContentRequestKind::Batched
                    },
                    events: events.clone(),
                };
                match issue_content_request(
                    &self.client,
                    &self.wal,
                    &request,
                    self.options.surface_kind.as_str(),
                )
                .await
                {
                    Ok(publication) => {
                        revision = revision.max(publication.revision);
                    }
                    Err(error) => {
                        let reason = format!("{error:#}");
                        if rejected_request_status(&error).is_none() {
                            return Ok(RunOutcome::Failed {
                                failure: PublishFailure::transient(events[0].sequence, reason),
                            });
                        }
                        pending.push((events, Some(reason)));
                    }
                }
                continue;
            };

            let mut snapshots = RemoteSnapshots::new(&self.client);
            snapshots.observe_revision(revision);
            let attempted_len = events.len();
            let mut unlanded = Vec::new();
            for event in events {
                match self.event_landed(&event, &mut snapshots).await {
                    Ok(true) => {}
                    Ok(false) => unlanded.push(event),
                    Err(read_error) => {
                        return Ok(RunOutcome::Failed {
                            failure: PublishFailure::transient(
                                event.sequence,
                                format!("{reason}; reconciliation read failed: {read_error:#}"),
                            ),
                        });
                    }
                }
            }
            revision = revision.max(snapshots.revision());
            match unlanded.len() {
                0 => {}
                1 if attempted_len == 1 => {
                    return Ok(RunOutcome::Failed {
                        failure: PublishFailure::permanent(unlanded[0].sequence, reason),
                    });
                }
                1 => pending.push((unlanded, None)),
                length => {
                    let right = unlanded.split_off(length / 2);
                    pending.push((right, None));
                    pending.push((unlanded, None));
                }
            }
        }
        Ok(RunOutcome::Complete { revision })
    }

    // -- reconciliation predicate -------------------------------------------

    /// Whether the gateway already holds the state this event intended. This is
    /// the client-side idempotency the gateway does not provide: `operation_ids`
    /// are validated within a batch and dropped, so a replay is only safe when
    /// the remote state is compared against the event's intent.
    async fn event_landed(
        &self,
        event: &MountEvent,
        snapshots: &mut RemoteSnapshots<'_>,
    ) -> Result<bool> {
        match &event.mutation {
            MountMutation::CreateDirectory { path, mode } => {
                let Some(metadata) = snapshots.get(path).await? else {
                    return Ok(false);
                };
                Ok(is_kind(&metadata, LocalKind::Directory) && mode_agrees(&metadata, *mode))
            }
            MountMutation::CreateFile { path, mode } => {
                let Some(metadata) = snapshots.get(path).await? else {
                    return Ok(false);
                };
                Ok(is_kind(&metadata, LocalKind::File) && mode_agrees(&metadata, *mode))
            }
            MountMutation::ReplaceFile {
                path,
                mode,
                base_content_hash,
                ..
            } => {
                let payload = event.payload.as_ref().ok_or_else(|| {
                    anyhow!("content event {} carries no payload", event.sequence)
                })?;
                let Some(metadata) = snapshots.get(path).await? else {
                    return Ok(false);
                };
                // Mode is deliberately not compared: `write-many` applies it only
                // when the write creates the path, so an overwrite legitimately
                // leaves an older mode in place.
                Ok(is_kind(&metadata, LocalKind::File)
                    && metadata.content_hash.as_deref() == Some(payload.content_hash.as_str())
                    && (base_content_hash.as_deref() != Some(ABSENT_PRECONDITION)
                        || mode_agrees(&metadata, *mode)))
            }
            MountMutation::CreateSymlink { path, target } => {
                let remote = snapshots.get(path).await?;
                if remote.as_ref().is_some_and(|metadata| {
                    is_kind(metadata, LocalKind::Symlink)
                        && metadata.link_target.as_deref() == Some(target.as_str())
                }) {
                    return Ok(true);
                }
                // A short-lived guest workaround may create a symlink that the
                // gateway correctly rejects (for example an absolute target),
                // then replace it before the publisher reaches that sequence.
                // Requiring the obsolete intermediate link to land would freeze
                // the entire ordered suffix even though later WAL events carry
                // the authoritative final state. Only treat the rejection as
                // superseded when the live mount proves the path is no longer a
                // symlink; a still-live or retargeted symlink remains blocked.
                self.path_kind_intent_is_superseded(path, LocalKind::Symlink)
            }
            MountMutation::CreateHardLink {
                existing_path,
                new_path,
            } => {
                let Some(created) = snapshots.get(new_path).await? else {
                    return Ok(false);
                };
                let Some(existing) = snapshots.get(existing_path).await? else {
                    return Ok(false);
                };
                Ok(
                    match (existing.file_id.as_deref(), created.file_id.as_deref()) {
                        (Some(source), Some(destination)) => source == destination,
                        // A backend that does not surface file ids still surfaces the
                        // link count, which only exceeds one once the alias exists.
                        _ => existing.link_count > 1 && created.link_count > 1,
                    },
                )
            }
            MountMutation::Rename {
                old_path, new_path, ..
            } => {
                let old = snapshots.get(old_path).await?;
                let new = snapshots.get(new_path).await?;
                if old.is_none() {
                    return Ok(new.is_some());
                }
                if new.is_some() && self.rename_source_is_superseded(old_path, new_path)? {
                    let cleanup_revision = self
                        .remove_remote_subtree(old_path, event.idempotency_key.as_str())
                        .await?;
                    snapshots.observe_revision(cleanup_revision);
                    return Ok(true);
                }
                Ok(false)
            }
            MountMutation::RemoveFile { path, .. } | MountMutation::RemoveDirectory { path } => {
                Ok(snapshots.get(path).await?.is_none())
            }
            MountMutation::SetMode { path, mode } => {
                let Some(metadata) = snapshots.get(path).await? else {
                    return Ok(false);
                };
                Ok(mode_matches(&metadata, *mode))
            }
            // Never published, so never reconciled: the cursor walks past them.
            MountMutation::SetTimes { .. } | MountMutation::SetOwner { .. } => Ok(true),
        }
    }

    fn rename_source_is_superseded(&self, old_path: &str, new_path: &str) -> Result<bool> {
        let Some(source) = self.options.authoritative_paths.as_ref() else {
            return Ok(false);
        };
        Ok(source.kind(old_path)?.is_none() && source.kind(new_path)?.is_some())
    }

    fn path_kind_intent_is_superseded(&self, path: &str, intended: LocalKind) -> Result<bool> {
        let Some(source) = self.options.authoritative_paths.as_ref() else {
            return Ok(false);
        };
        Ok(kind_intent_is_superseded(source.kind(path)?, intended))
    }

    /// Remove one stale remote subtree through the ordinary gateway namespace
    /// contract. This is used only when the current mount-local view proves the
    /// replayed rename source no longer exists, and therefore cannot delete a
    /// live local path or race another owner under the one-mount invariant.
    async fn remove_remote_subtree(&self, root: &str, operation_prefix: &str) -> Result<u64> {
        let mut pending = vec![(root.to_string(), false)];
        let mut removals = Vec::new();
        let mut revision = self.client.observed_namespace_revision();
        while let Some((path, visited)) = pending.pop() {
            if visited {
                removals.push(VfsNamespaceMutation::RemoveDirectory { path });
                continue;
            }
            let listing = self.client.list_dir_versioned(path.as_str()).await?;
            revision = revision.max(listing.revision);
            let Some(entries) = listing.value else {
                removals.push(VfsNamespaceMutation::DeleteFile {
                    path,
                    precondition: None,
                });
                continue;
            };
            pending.push((path.clone(), true));
            for entry in entries.into_iter().rev() {
                let child = format!("{path}/{}", entry.name);
                if LocalKind::from_wire_kind(entry.kind.as_str()) == Some(LocalKind::Directory) {
                    pending.push((child, false));
                } else {
                    removals.push(VfsNamespaceMutation::DeleteFile {
                        path: child,
                        precondition: None,
                    });
                }
            }
        }

        for (chunk_index, chunk) in removals.chunks(self.options.max_events.max(1)).enumerate() {
            let operation_ids = chunk
                .iter()
                .enumerate()
                .map(|(index, _)| format!("{operation_prefix}:cleanup:{chunk_index}:{index}"))
                .collect::<Vec<_>>();
            let publication = self
                .client
                .apply_namespace_batch(&operation_ids, chunk, self.options.surface_kind.as_str())
                .await?;
            revision = revision.max(publication.revision);
        }
        Ok(revision)
    }
}

// ---------------------------------------------------------------------------
// Content requests
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ContentRequestKind {
    /// Payloads under the stream threshold, packed into one `write-many`.
    Batched,
    /// One oversized payload streamed from its dedicated payload file.
    Streamed,
}

struct ContentRequest {
    kind: ContentRequestKind,
    events: Vec<MountEvent>,
}

struct ContentTaskResult {
    events: Vec<MountEvent>,
    outcome: Result<RemotePublication>,
}

async fn issue_content_request(
    client: &RemoteVfsClient,
    wal: &MountWal,
    request: &ContentRequest,
    surface_kind: &str,
) -> Result<RemotePublication> {
    match request.kind {
        ContentRequestKind::Batched => {
            // Payload reads are ordinary blocking file reads out of the payload
            // store; they never belong on an async worker.
            let wal = wal.clone();
            let events = request.events.clone();
            let writes = tokio::task::spawn_blocking(move || {
                let mut writes = Vec::with_capacity(events.len());
                for event in &events {
                    let payload = event.payload.as_ref().ok_or_else(|| {
                        anyhow!("content event {} carries no payload", event.sequence)
                    })?;
                    let bytes = wal.payload_bytes(payload).with_context(|| {
                        format!("read payload for vfs mount event {}", event.sequence)
                    })?;
                    writes.push(remote_write_for(event, bytes)?);
                }
                anyhow::Ok(writes)
            })
            .await
            .context("join vfs mount payload read")??;
            client.write_many(writes, surface_kind).await
        }
        ContentRequestKind::Streamed => {
            let event = request
                .events
                .first()
                .ok_or_else(|| anyhow!("streamed content request carries no event"))?;
            let payload = event
                .payload
                .as_ref()
                .ok_or_else(|| anyhow!("content event {} carries no payload", event.sequence))?;
            let MountMutation::ReplaceFile {
                path,
                mode,
                expected_file_id,
                base_content_hash,
            } = &event.mutation
            else {
                bail!(
                    "vfs mount event {} is not a content generation",
                    event.sequence
                );
            };
            // D9: a payload this large is always a dedicated, immutable file
            // whose length is exactly `PayloadRef::length`, which is what lets
            // the existing `write_staged_file` signature carry it unchanged.
            let staged = wal.payload_path(payload).ok_or_else(|| {
                anyhow!(
                    "vfs mount event {} has a streamable payload with no dedicated file",
                    event.sequence
                )
            })?;
            client
                .write_staged_file(
                    path.as_str(),
                    staged.as_path(),
                    payload.length,
                    payload.content_hash.as_str(),
                    base_content_hash.as_deref(),
                    remote_file_id(expected_file_id.as_deref()),
                    Some(mode & 0o7777),
                    surface_kind,
                )
                .await
        }
    }
}

fn remote_write_for(event: &MountEvent, bytes: Vec<u8>) -> Result<RemoteWrite> {
    let MountMutation::ReplaceFile {
        path,
        mode,
        expected_file_id,
        base_content_hash,
    } = &event.mutation
    else {
        bail!(
            "vfs mount event {} is not a content generation",
            event.sequence
        );
    };
    Ok(RemoteWrite {
        path: path.clone(),
        bytes,
        base_content_hash: base_content_hash.clone(),
        expected_file_id: remote_file_id(expected_file_id.as_deref()).map(ToOwned::to_owned),
        // `mode` is applied only when the write creates the path, which is what
        // makes the creation fold carry the executable bit instead of silently
        // dropping it.
        mode: Some(mode & 0o7777),
    })
}

fn namespace_mutation_for(event: &MountEvent) -> Result<VfsNamespaceMutation> {
    Ok(match &event.mutation {
        MountMutation::CreateDirectory { path, mode } => VfsNamespaceMutation::CreateDirectory {
            path: path.clone(),
            mode: Some(mode & 0o7777),
        },
        MountMutation::CreateFile { path, mode } => VfsNamespaceMutation::CreateFile {
            path: path.clone(),
            mode: Some(mode & 0o7777),
        },
        MountMutation::CreateSymlink { path, target } => VfsNamespaceMutation::CreateSymlink {
            path: path.clone(),
            target: target.clone(),
        },
        MountMutation::CreateHardLink {
            existing_path,
            new_path,
        } => VfsNamespaceMutation::CreateHardLink {
            source_path: existing_path.clone(),
            destination_path: new_path.clone(),
        },
        // `RENAME_NOREPLACE` has no wire form and needs none: `renameat2` already
        // enforced it locally, and single ownership makes that authoritative.
        MountMutation::Rename {
            old_path, new_path, ..
        } => VfsNamespaceMutation::Rename {
            from: old_path.clone(),
            to: new_path.clone(),
        },
        MountMutation::RemoveFile {
            path,
            expected_file_id,
        } => VfsNamespaceMutation::DeleteFile {
            path: path.clone(),
            precondition: remote_file_id(expected_file_id.as_deref()).map(|file_id| {
                VfsWritePrecondition {
                    predicate: None,
                    fingerprint: None,
                    secondary_fingerprint: None,
                    expected_file_id: Some(file_id.to_string()),
                }
            }),
        },
        MountMutation::RemoveDirectory { path } => {
            VfsNamespaceMutation::RemoveDirectory { path: path.clone() }
        }
        MountMutation::SetMode { path, mode } => VfsNamespaceMutation::SetMode {
            path: path.clone(),
            mode: mode & 0o7777,
        },
        MountMutation::ReplaceFile { .. } => bail!(
            "vfs mount event {} is a content generation and cannot be published as a namespace \
             mutation",
            event.sequence
        ),
        MountMutation::SetTimes { .. } | MountMutation::SetOwner { .. } => bail!(
            "vfs mount event {} is local-only and must advance the cursor without a request",
            event.sequence
        ),
    })
}

/// A `MountMutation`'s `expected_file_id` is this mount's local `dev:ino`
/// identity (D15), which the gateway has never seen: forwarding one as a CAS
/// precondition would reject every write and delete that carries it. Only a
/// value that is not a local identity is forwarded, so the field keeps working
/// the day a gateway file id is recorded in it.
fn remote_file_id(candidate: Option<&str>) -> Option<&str> {
    let value = candidate?;
    if is_local_identity(value) {
        return None;
    }
    Some(value)
}

fn is_local_identity(value: &str) -> bool {
    match value.split_once(':') {
        Some((device, inode)) => {
            !device.is_empty()
                && !inode.is_empty()
                && device.bytes().all(|byte| byte.is_ascii_digit())
                && inode.bytes().all(|byte| byte.is_ascii_digit())
        }
        None => false,
    }
}

fn is_kind(metadata: &RemoteMetadata, kind: LocalKind) -> bool {
    LocalKind::from_wire_kind(metadata.kind.as_str()) == Some(kind)
}

fn kind_intent_is_superseded(current: Option<LocalKind>, intended: LocalKind) -> bool {
    current.is_none_or(|kind| kind != intended)
}

/// Permissive mode comparison for a creation: a backend that does not report a
/// mode cannot contradict the intent.
fn mode_agrees(metadata: &RemoteMetadata, mode: u32) -> bool {
    metadata
        .mode
        .is_none_or(|reported| reported == mode & 0o7777)
}

/// Strict mode comparison for `SetMode`, whose entire intent is the mode. When
/// the backend reports no mode, the executable bit is the only observable it
/// carries and is what the comparison falls back to.
fn mode_matches(metadata: &RemoteMetadata, mode: u32) -> bool {
    match metadata.mode {
        Some(reported) => reported == mode & 0o7777,
        None => metadata.executable == (mode & 0o111 != 0),
    }
}

// ---------------------------------------------------------------------------
// Remote snapshots
// ---------------------------------------------------------------------------

/// Post-rejection view of the gateway, memoized per path.
///
/// A publication's own `entries[].metadata` snapshot is preferred where one is in
/// hand, because it is the state the gateway published and costs no extra round
/// trip; anything it does not cover falls back to `stat_versioned`.
struct RemoteSnapshots<'a> {
    client: &'a RemoteVfsClient,
    entries: HashMap<String, Option<RemoteMetadata>>,
    revision: u64,
}

impl<'a> RemoteSnapshots<'a> {
    fn new(client: &'a RemoteVfsClient) -> Self {
        Self {
            client,
            entries: HashMap::new(),
            revision: 0,
        }
    }

    fn observe_revision(&mut self, revision: u64) {
        self.revision = self.revision.max(revision);
    }

    fn seed(&mut self, publication: &RemotePublication) {
        self.observe_revision(publication.revision);
        for entry in &publication.entries {
            self.entries
                .insert(entry.path.clone(), entry.metadata.clone());
        }
    }

    async fn get(&mut self, path: &str) -> Result<Option<RemoteMetadata>> {
        if let Some(entry) = self.entries.get(path) {
            return Ok(entry.clone());
        }
        let versioned =
            self.client.stat_versioned(path).await.with_context(|| {
                format!("re-read {path} after a rejected vfs mount publication")
            })?;
        self.observe_revision(versioned.revision);
        self.entries
            .insert(path.to_string(), versioned.value.clone());
        Ok(versioned.value)
    }

    fn revision(&self) -> u64 {
        self.revision
    }
}

#[cfg(test)]
mod tests {
    use super::{ABSENT_PRECONDITION, kind_intent_is_superseded, plan_runs};
    use crate::fuse::local_view::types::{
        LocalKind, MountEvent, MountMutation, MountPreImage, PayloadRef, PayloadStorage,
        PublishBatch, PublishRun,
    };

    fn event(sequence: u64, mutation: MountMutation, payload: Option<PayloadRef>) -> MountEvent {
        MountEvent {
            format_version: super::super::WAL_FORMAT_VERSION,
            epoch: "test-epoch".to_string(),
            sequence,
            idempotency_key: format!("test-epoch:{sequence}"),
            mutation,
            payload,
            pre_image: MountPreImage::empty(),
            local_identity: Some(format!("1:{sequence}")),
        }
    }

    fn payload(sequence: u64) -> PayloadRef {
        PayloadRef {
            storage: PayloadStorage::Segment {
                file: "segment.bin".to_string(),
                offset: sequence,
            },
            length: 1,
            hash_algorithm: "blake3".to_string(),
            content_hash: format!("hash-{sequence}"),
        }
    }

    #[test]
    fn rejected_symlink_is_superseded_only_after_authoritative_kind_changes() {
        assert!(!kind_intent_is_superseded(
            Some(LocalKind::Symlink),
            LocalKind::Symlink,
        ));
        assert!(kind_intent_is_superseded(
            Some(LocalKind::File),
            LocalKind::Symlink,
        ));
        assert!(kind_intent_is_superseded(None, LocalKind::Symlink));
    }

    #[test]
    fn sibling_create_write_pairs_fold_into_one_content_run() {
        let batch = PublishBatch {
            events: vec![
                event(
                    1,
                    MountMutation::CreateFile {
                        path: "node_modules/a.js".to_string(),
                        mode: 0o644,
                    },
                    None,
                ),
                event(
                    2,
                    MountMutation::ReplaceFile {
                        path: "node_modules/a.js".to_string(),
                        mode: 0o600,
                        expected_file_id: None,
                        base_content_hash: None,
                    },
                    Some(payload(2)),
                ),
                event(
                    3,
                    MountMutation::CreateFile {
                        path: "node_modules/b.js".to_string(),
                        mode: 0o755,
                    },
                    None,
                ),
                event(
                    4,
                    MountMutation::ReplaceFile {
                        path: "node_modules/b.js".to_string(),
                        mode: 0o600,
                        expected_file_id: None,
                        base_content_hash: None,
                    },
                    Some(payload(4)),
                ),
            ],
            through_sequence: 4,
        };

        let runs = plan_runs(&batch).expect("plan sibling package files");
        let [
            PublishRun::Content {
                events,
                through_sequence,
            },
        ] = runs.as_slice()
        else {
            panic!("sibling create/write pairs should produce one content run: {runs:?}");
        };
        assert_eq!(*through_sequence, 4);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].mutation.primary_path(), "node_modules/a.js");
        assert_eq!(events[1].mutation.primary_path(), "node_modules/b.js");
        for (event, expected_mode) in events.iter().zip([0o644, 0o755]) {
            let MountMutation::ReplaceFile {
                mode,
                base_content_hash,
                ..
            } = &event.mutation
            else {
                panic!("folded event was not a content generation");
            };
            assert_eq!(*mode, expected_mode);
            assert_eq!(base_content_hash.as_deref(), Some(ABSENT_PRECONDITION));
        }
    }

    #[test]
    fn interleaved_copy_metadata_folds_and_batches_by_dependency() {
        let path_a = "node_modules/pkg/a.js";
        let path_b = "node_modules/pkg/b.js";
        let batch = PublishBatch {
            events: vec![
                event(
                    1,
                    MountMutation::CreateFile {
                        path: path_a.to_string(),
                        mode: 0o600,
                    },
                    None,
                ),
                event(
                    2,
                    MountMutation::CreateFile {
                        path: path_b.to_string(),
                        mode: 0o600,
                    },
                    None,
                ),
                event(
                    3,
                    MountMutation::SetTimes {
                        path: path_a.to_string(),
                        atime: None,
                        mtime: None,
                    },
                    None,
                ),
                event(
                    4,
                    MountMutation::SetMode {
                        path: path_a.to_string(),
                        mode: 0o644,
                    },
                    None,
                ),
                event(
                    5,
                    MountMutation::SetOwner {
                        path: path_a.to_string(),
                        uid: Some(1000),
                        gid: Some(1000),
                    },
                    None,
                ),
                event(
                    6,
                    MountMutation::SetMode {
                        path: path_b.to_string(),
                        mode: 0o755,
                    },
                    None,
                ),
                event(
                    7,
                    MountMutation::ReplaceFile {
                        path: path_a.to_string(),
                        mode: 0o600,
                        expected_file_id: None,
                        base_content_hash: None,
                    },
                    Some(payload(7)),
                ),
                event(
                    8,
                    MountMutation::ReplaceFile {
                        path: path_b.to_string(),
                        mode: 0o600,
                        expected_file_id: None,
                        base_content_hash: None,
                    },
                    Some(payload(8)),
                ),
            ],
            through_sequence: 8,
        };

        let runs = plan_runs(&batch).expect("plan interleaved package copies");
        let [
            PublishRun::Content {
                events,
                through_sequence,
            },
        ] = runs.as_slice()
        else {
            panic!("interleaved copies should produce one content run: {runs:?}");
        };
        assert_eq!(*through_sequence, 8);
        assert_eq!(events.len(), 2);
        for (event, (expected_path, expected_mode)) in
            events.iter().zip([(path_a, 0o644), (path_b, 0o755)])
        {
            let MountMutation::ReplaceFile {
                path,
                mode,
                base_content_hash,
                ..
            } = &event.mutation
            else {
                panic!("folded event was not a content generation");
            };
            assert_eq!(path, expected_path);
            assert_eq!(*mode, expected_mode);
            assert_eq!(base_content_hash.as_deref(), Some(ABSENT_PRECONDITION));
        }
    }
}

// ---------------------------------------------------------------------------
// Run planning
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RunClass {
    Namespace,
    Content,
}

/// Split a contiguous committed prefix into dependency-ordered route batches.
///
/// Folds a newly-created file's mode and first content generation into one
/// absent-precondition write. It then topologically schedules the remaining
/// operations by the exact same dependency keys used by callbacks: unrelated
/// paths may cross route-class boundaries, while every conflicting pair retains
/// WAL order. Ordered namespace mutations can share one request; content
/// generations share a run only when they are mutually independent.
pub(crate) fn plan_runs(batch: &PublishBatch) -> Result<Vec<PublishRun>> {
    let events = batch.events.as_slice();
    for pair in events.windows(2) {
        if pair[1].sequence <= pair[0].sequence {
            bail!(
                "publish batch sequences are not strictly increasing ({} then {})",
                pair[0].sequence,
                pair[1].sequence
            );
        }
    }
    if let Some(last) = events.last()
        && batch.through_sequence < last.sequence
    {
        bail!(
            "publish batch through sequence {} is behind its last event {}",
            batch.through_sequence,
            last.sequence
        );
    }

    // Pass 1 -- the creation fold. A folded creation carries no wire item of its
    // own: its final mode and its `Absent` precondition move onto the write.
    // The publisher acknowledges only after every run in this whole batch
    // succeeds, so an interleaved unrelated run can never acknowledge the
    // creation before the content operation that represents it.
    let mut planned: Vec<MountEvent> = events.to_vec();
    let mut folded = vec![false; events.len()];
    for index in 0..events.len() {
        let MountMutation::CreateFile { path, mode } = &events[index].mutation else {
            continue;
        };
        let created_mode = *mode;
        let Some(target) = ((index + 1)..events.len()).find(|candidate| {
            matches!(
                &events[*candidate].mutation,
                MountMutation::ReplaceFile { path: written, .. } if written == path
            )
        }) else {
            continue;
        };
        if !creation_folds_into_write(&events[index], &events[target], &events[index + 1..target]) {
            continue;
        }
        folded[index] = true;
        let mut final_mode = created_mode;
        for candidate in index + 1..target {
            if let MountMutation::SetMode {
                path: changed,
                mode,
            } = &events[candidate].mutation
                && changed == path
            {
                final_mode = *mode;
                folded[candidate] = true;
            }
        }
        // A chmod shortly after the first write is also part of materializing a
        // newly-created file. Absorb it until another remote mutation conflicts
        // with this path; unrelated work and local-only metadata commute.
        let creation_keys = events[index].dependency_keys();
        for candidate in target + 1..events.len() {
            match &events[candidate].mutation {
                MountMutation::SetMode {
                    path: changed,
                    mode,
                } if changed == path => {
                    final_mode = *mode;
                    folded[candidate] = true;
                }
                mutation if mutation.is_local_only() => {}
                _ if dependency_sets_conflict(
                    &creation_keys,
                    &events[candidate].dependency_keys(),
                ) =>
                {
                    break;
                }
                _ => {}
            }
        }
        if let MountMutation::ReplaceFile {
            mode,
            base_content_hash,
            ..
        } = &mut planned[target].mutation
        {
            *mode = final_mode;
            *base_content_hash = Some(ABSENT_PRECONDITION.to_string());
        }
    }

    // Pass 2 -- dependency scheduling. Start with the oldest unscheduled wire
    // event, then pull every later event of the same route class that has no
    // earlier conflicting event waiting in another run. Selected namespace
    // events do not block later namespace events because `namespace-many`
    // applies them in their original order. Selected content events do block a
    // conflict because their uploads execute concurrently.
    let publishable: Vec<MountEvent> = planned
        .into_iter()
        .enumerate()
        .filter_map(|(index, event)| {
            (!folded[index] && !event.mutation.is_local_only()).then_some(event)
        })
        .collect();
    if publishable.is_empty() {
        return Ok((batch.through_sequence > 0)
            .then_some(PublishRun::Cursor {
                through_sequence: batch.through_sequence,
            })
            .into_iter()
            .collect());
    }
    let classes: Vec<RunClass> = publishable
        .iter()
        .map(|event| {
            if event.mutation.is_content() {
                RunClass::Content
            } else {
                RunClass::Namespace
            }
        })
        .collect();
    let keys: Vec<_> = publishable
        .iter()
        .map(MountEvent::dependency_keys)
        .collect();
    let mut remaining = vec![true; publishable.len()];
    let mut runs: Vec<PublishRun> = Vec::new();
    while let Some(first) = remaining.iter().position(|pending| *pending) {
        let class = classes[first];
        let mut selected = vec![false; publishable.len()];
        for candidate in first..publishable.len() {
            if !remaining[candidate] || classes[candidate] != class {
                continue;
            }
            let blocked = (0..candidate).any(|predecessor| {
                remaining[predecessor]
                    && (!selected[predecessor] || class == RunClass::Content)
                    && dependency_sets_conflict(&keys[predecessor], &keys[candidate])
            });
            if !blocked {
                selected[candidate] = true;
            }
        }
        let indices: Vec<usize> = selected
            .iter()
            .enumerate()
            .filter_map(|(index, chosen)| chosen.then_some(index))
            .collect();
        debug_assert!(!indices.is_empty());
        let through_sequence = indices
            .iter()
            .map(|index| publishable[*index].sequence)
            .max()
            .unwrap_or(batch.through_sequence);
        let events = indices
            .iter()
            .map(|index| publishable[*index].clone())
            .collect();
        for index in indices {
            remaining[index] = false;
        }
        runs.push(match class {
            RunClass::Namespace => PublishRun::Namespace {
                events,
                through_sequence,
            },
            RunClass::Content => PublishRun::Content {
                events,
                through_sequence,
            },
        });
    }
    Ok(runs)
}

/// Whether `create` may be folded into `write` given everything between them.
pub(crate) fn creation_folds_into_write(
    create: &MountEvent,
    write: &MountEvent,
    between: &[MountEvent],
) -> bool {
    let MountMutation::CreateFile { path: created, .. } = &create.mutation else {
        return false;
    };
    let MountMutation::ReplaceFile {
        path: written,
        expected_file_id,
        ..
    } = &write.mutation
    else {
        return false;
    };
    if created != written || create.sequence >= write.sequence {
        return false;
    }
    // An identity precondition cannot be reconciled with the `Absent` predicate
    // the fold installs, so such a write is published on its own.
    if remote_file_id(expected_file_id.as_deref()).is_some() {
        return false;
    }
    // Local-only metadata and chmods on the newly-created path can collapse
    // into the absent-precondition write. Any other path conflict preserves the
    // separate events and their original remote order.
    let keys = create.dependency_keys();
    !between.iter().any(|event| {
        if event.mutation.is_local_only()
            || matches!(
                &event.mutation,
                MountMutation::SetMode { path, .. } if path == created
            )
        {
            return false;
        }
        dependency_sets_conflict(&keys, &event.dependency_keys())
    })
}
