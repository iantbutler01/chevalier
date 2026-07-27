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
//! Runs are issued strictly in WAL order and awaited before the next one starts.
//! Throughput comes from bigger batches and from concurrency *inside* a run --
//! never from reordering a rename, delete or write dependency.
//!
//! ## The creation fold
//!
//! A `CreateFile{path,mode}` followed by a `ReplaceFile{path}` with no
//! intervening event on that path or its ancestors is folded into one
//! `write-many` item carrying `mode` and an `Absent` precondition. This is what
//! collapses the dominant install pattern (`mkdir`, then N create+write) into one
//! `namespace-many` plus K `write-many`, instead of two namespace publications
//! and an ordering fence per file.
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

use std::collections::{HashMap, HashSet};
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
        }
    }

    pub(crate) fn with_tree_generation(mut self, source: TreeGenerationSource) -> Self {
        self.tree_generation = Some(source);
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

/// One run's result, expressed so a partially landed run can still advance the
/// cursor over the part that is provably remote.
enum RunOutcome {
    Complete {
        revision: u64,
    },
    Partial {
        through_sequence: u64,
        revision: u64,
        failure: PublishFailure,
    },
    Failed {
        failure: PublishFailure,
    },
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
            Ok(PublishProgress::Advanced) => backoff = RETRY_BACKOFF_MIN,
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
        let Some(batch) = self
            .wal
            .next_publish_batch(self.options.max_events, self.options.max_payload_bytes)?
        else {
            return Ok(PublishProgress::Idle);
        };
        let runs = plan_runs(&batch)?;
        if runs.is_empty() {
            return Ok(PublishProgress::Idle);
        }
        for run in runs {
            let through_sequence = run.through_sequence();
            match self.issue_run(run).await? {
                RunOutcome::Complete { revision } => {
                    self.acknowledge(through_sequence, revision)?;
                }
                RunOutcome::Partial {
                    through_sequence,
                    revision,
                    failure,
                } => {
                    self.acknowledge(through_sequence, revision)?;
                    self.record_failure(&failure);
                    return Ok(PublishProgress::Stalled);
                }
                RunOutcome::Failed { failure } => {
                    self.record_failure(&failure);
                    return Ok(PublishProgress::Stalled);
                }
            }
        }
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
        let mut operation_ids = Vec::with_capacity(events.len());
        let mut mutations = Vec::with_capacity(events.len());
        for event in &events {
            operation_ids.push(event.idempotency_key.clone());
            mutations.push(namespace_mutation_for(event)?);
        }
        match self
            .client
            .apply_namespace_batch(&operation_ids, &mutations, &self.options.surface_kind)
            .await
        {
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
                // A permanent rejection is never trusted: the batch may have
                // applied a prefix before the failing mutation, and a replay
                // after a lost response rejects work that in fact landed.
                self.reconcile_ordered(&events, reason).await
            }
        }
    }

    /// Walk an ordered run against the gateway and find the longest prefix that
    /// provably landed. Everything after the first genuine mismatch is preserved
    /// and blocked.
    async fn reconcile_ordered(&self, events: &[MountEvent], reason: String) -> Result<RunOutcome> {
        let mut snapshots = RemoteSnapshots::new(&self.client);
        let mut landed_through: Option<u64> = None;
        for event in events {
            let failure = match self.event_landed(event, &mut snapshots).await {
                Ok(true) => {
                    landed_through = Some(event.sequence);
                    continue;
                }
                Ok(false) => PublishFailure::permanent(event.sequence, reason.clone()),
                Err(error) => PublishFailure::transient(
                    event.sequence,
                    format!("{reason}; reconciliation read failed: {error:#}"),
                ),
            };
            return Ok(match landed_through {
                Some(through_sequence) => RunOutcome::Partial {
                    through_sequence,
                    revision: snapshots.revision(),
                    failure,
                },
                None => RunOutcome::Failed { failure },
            });
        }
        Ok(RunOutcome::Complete {
            revision: snapshots.revision(),
        })
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
        self.reconcile_content(&events, &publications, failures, revision)
            .await
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

    /// Content events inside a run are independent, so a failure is per-request.
    /// Verdicts are collected per sequence and then walked in WAL order: the
    /// cursor may only advance over an unbroken landed prefix.
    async fn reconcile_content(
        &self,
        events: &[MountEvent],
        publications: &[RemotePublication],
        failures: Vec<(Vec<MountEvent>, anyhow::Error)>,
        revision: u64,
    ) -> Result<RunOutcome> {
        let mut snapshots = RemoteSnapshots::new(&self.client);
        snapshots.observe_revision(revision);
        for publication in publications {
            snapshots.seed(publication);
        }

        let failed_sequences: HashSet<u64> = failures
            .iter()
            .flat_map(|(events, _)| events.iter().map(|event| event.sequence))
            .collect();
        let mut verdicts: HashMap<u64, EventVerdict> = HashMap::new();
        for event in events {
            if !failed_sequences.contains(&event.sequence) {
                verdicts.insert(event.sequence, EventVerdict::landed());
            }
        }
        for (failed_events, error) in &failures {
            let reason = format!("{error:#}");
            if rejected_request_status(error).is_none() {
                for event in failed_events {
                    verdicts.insert(event.sequence, EventVerdict::failed(false, reason.clone()));
                }
                continue;
            }
            for event in failed_events {
                let verdict = match self.event_landed(event, &mut snapshots).await {
                    Ok(true) => EventVerdict::landed(),
                    Ok(false) => EventVerdict::failed(true, reason.clone()),
                    Err(read_error) => EventVerdict::failed(
                        false,
                        format!("{reason}; reconciliation read failed: {read_error:#}"),
                    ),
                };
                verdicts.insert(event.sequence, verdict);
            }
        }

        let mut landed_through: Option<u64> = None;
        for event in events {
            let verdict = verdicts.get(&event.sequence).ok_or_else(|| {
                anyhow!(
                    "content run produced no verdict for sequence {}",
                    event.sequence
                )
            })?;
            if verdict.landed {
                landed_through = Some(event.sequence);
                continue;
            }
            let failure = PublishFailure {
                sequence: event.sequence,
                permanent: verdict.permanent,
                reason: verdict.reason.clone(),
            };
            return Ok(match landed_through {
                Some(through_sequence) => RunOutcome::Partial {
                    through_sequence,
                    revision: snapshots.revision(),
                    failure,
                },
                None => RunOutcome::Failed { failure },
            });
        }
        Ok(RunOutcome::Complete {
            revision: snapshots.revision(),
        })
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
            MountMutation::ReplaceFile { path, .. } => {
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
                    && metadata.content_hash.as_deref() == Some(payload.content_hash.as_str()))
            }
            MountMutation::CreateSymlink { path, target } => {
                let Some(metadata) = snapshots.get(path).await? else {
                    return Ok(false);
                };
                Ok(is_kind(&metadata, LocalKind::Symlink)
                    && metadata.link_target.as_deref() == Some(target.as_str()))
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
                if snapshots.get(old_path).await?.is_some() {
                    return Ok(false);
                }
                Ok(snapshots.get(new_path).await?.is_some())
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

struct EventVerdict {
    landed: bool,
    permanent: bool,
    reason: String,
}

impl EventVerdict {
    fn landed() -> Self {
        Self {
            landed: true,
            permanent: false,
            reason: String::new(),
        }
    }

    fn failed(permanent: bool, reason: String) -> Self {
        Self {
            landed: false,
            permanent,
            reason,
        }
    }
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

// ---------------------------------------------------------------------------
// Run planning
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RunClass {
    Namespace,
    Content,
}

/// A run under construction. `class` stays `None` while only local-only or
/// folded-away events have been absorbed, which is exactly a `Cursor` run.
struct PendingRun {
    class: Option<RunClass>,
    events: Vec<MountEvent>,
    through_sequence: u64,
}

impl PendingRun {
    fn absorbing(through_sequence: u64) -> Self {
        Self {
            class: None,
            events: Vec::new(),
            through_sequence,
        }
    }

    fn finish(self) -> PublishRun {
        match self.class {
            None => PublishRun::Cursor {
                through_sequence: self.through_sequence,
            },
            Some(RunClass::Namespace) => PublishRun::Namespace {
                events: self.events,
                through_sequence: self.through_sequence,
            },
            Some(RunClass::Content) => PublishRun::Content {
                events: self.events,
                through_sequence: self.through_sequence,
            },
        }
    }

    /// A namespace run absorbs anything ordered; a content run only absorbs an
    /// event that conflicts with none of its members, because that is what makes
    /// its uploads safe to issue concurrently.
    fn accepts(&self, class: RunClass, event: &MountEvent) -> bool {
        match self.class {
            None => true,
            Some(RunClass::Namespace) => class == RunClass::Namespace,
            Some(RunClass::Content) => {
                class == RunClass::Content && {
                    let keys = event.dependency_keys();
                    self.events
                        .iter()
                        .all(|member| !dependency_sets_conflict(&member.dependency_keys(), &keys))
                }
            }
        }
    }
}

/// Split a contiguous committed prefix into runs that may be issued in order.
///
/// Splits when the route class changes, when a content run would repeat a path,
/// or when two events inside a prospective run have conflicting dependency key
/// sets. Folds an eligible `CreateFile` into the following `ReplaceFile`.
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
    // own: its mode and its `Absent` precondition move onto the write, and its
    // sequence is absorbed by whichever run covers it. That is safe precisely
    // because the write now *is* the creation, so a crash before the write is
    // published replays a create-and-write, never a bare write.
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
        if let MountMutation::ReplaceFile {
            mode,
            base_content_hash,
            ..
        } = &mut planned[target].mutation
        {
            *mode = created_mode;
            *base_content_hash = Some(ABSENT_PRECONDITION.to_string());
        }
    }

    // Pass 2 -- grouping. Local-only and folded-away events produce no request,
    // so they are absorbed into the run in progress instead of splitting it.
    let mut runs: Vec<PublishRun> = Vec::new();
    let mut pending: Option<PendingRun> = None;
    for (index, event) in planned.iter().enumerate() {
        if folded[index] || event.mutation.is_local_only() {
            match pending.as_mut() {
                Some(run) => run.through_sequence = event.sequence,
                None => pending = Some(PendingRun::absorbing(event.sequence)),
            }
            continue;
        }
        let class = if event.mutation.is_content() {
            RunClass::Content
        } else {
            RunClass::Namespace
        };
        let splits = pending
            .as_ref()
            .is_some_and(|run| !run.accepts(class, event));
        if splits && let Some(run) = pending.take() {
            runs.push(run.finish());
        }
        let run = pending.get_or_insert_with(|| PendingRun::absorbing(event.sequence));
        run.class = Some(class);
        run.events.push(event.clone());
        run.through_sequence = event.sequence;
    }

    match pending.take() {
        Some(mut run) => {
            // Aborted sequences trailing the last event are folded into the final
            // run so the cursor never stalls on a gap that will never publish.
            run.through_sequence = run.through_sequence.max(batch.through_sequence);
            runs.push(run.finish());
        }
        None => {
            if batch.through_sequence > 0 {
                runs.push(PublishRun::Cursor {
                    through_sequence: batch.through_sequence,
                });
            }
        }
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
    // Local-only events never reach the gateway, so they cannot invalidate the
    // `Absent` predicate even though they hold the path's key locally.
    let keys = create.dependency_keys();
    !between.iter().any(|event| {
        !event.mutation.is_local_only() && dependency_sets_conflict(&keys, &event.dependency_keys())
    })
}
