// chevalier VFS gateway SERVER, in TypeScript.
//
// chevalier already ships the gateway PROTOCOL (HTTP routes under
// `/internal/chevalier/vfs/{owner_id}/...`), a Rust SERVER (the `vfs-server`
// feature, `VfsGatewayBackend` + axum routes in
// sandbox/crates/sandbox/src/vfs.rs), and Rust + TS CLIENTS
// (`GatewayVfsStorage` / the bound `VfsStorage.gateway({endpoint,scopePath})`).
// The missing third corner was a TS server. This is it: a framework-agnostic
// WHATWG `Request -> Response` handler that speaks the exact same wire contract
// (mirrored from vfs/src/gateway.rs, the client this must satisfy), backed by any
// `VfsStorage` (typically `VfsStorage.local(scopeRoot)`).
//
// With this, the already-bound `VfsStorage.gateway` client, the Rust client, and a
// VM FUSE mount all talk to a pure-Node server unchanged. Host it on any HTTP stack
// (Hono, node:http, etc.) by bridging that stack's request to a WHATWG `Request`.
//
// Wire facts mirrored from vfs/src/gateway.rs (the client) + sandbox vfs.rs (DTOs):
//   - endpoint already includes the {owner_id} segment; the file path is the
//     `?path=` query arg (scope already folded in by the client).
//   - GET  {owner}/stat?path=            -> 200 RemoteMetadata | 404
//   - GET  {owner}/file/raw?path=        -> 200 bytes (Range -> 206) | 404
//   - GET  {owner}/tree?path=&name_like= -> 200 RemoteDirEntry[]
//   - PUT  {owner}/file?path=            -> 2xx (body ignored by client); honors the
//                                           typed precondition-kind +
//                                           precondition-fingerprint headers, with
//                                           legacy `If-Match` / `ifMatch` aliases -> 409.
//                                           Optional identity CAS uses
//                                           `x-chevalier-vfs-precondition-file-id`.
//                                           Exact POSIX mode is decimal in
//                                           `x-chevalier-vfs-mode`; the legacy
//                                           executable header remains a fallback.
//                                           `If-Match` is an alias in chevalier's
//                                           protocol, not a separate HTTP 412 path.
//                                           Fingerprint is `contentHash`: SHA-256 hex
//                                           of the current logical file bytes.
//   - DELETE {owner}/file?path=&return_metadata=true -> 200 {previous}; same precondition
//   - PUT/DELETE {owner}/dir?path=       -> 2xx
//   - PUT  {owner}/symlink?path=&target= -> 2xx
//   - POST {owner}/rename?from=&to=&return_metadata=true -> 200 {previous,current}
//   - POST/DELETE {owner}/lease          -> 200 {resource_key,owner_token} / 2xx
//   - POST {owner}/{metadata-many,read-many,write-many} -> batch
//   - POST {owner}/namespace-many      -> ordered namespace mutation batch
//   DTOs are snake_case; `kind` is exactly "file" | "directory"; errors map
//   404->NotFound, 400->BadRequest, 409->Conflict (vfs/src/gateway.rs:1016).
import { createHash, randomUUID } from "node:crypto";
import { mkdtemp, open, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, posix } from "node:path";
import { VfsContentHasher } from "./native.js";
import type { VfsStorage, VfsMetadata } from "./native.js";

const DEFAULT_ROUTE_PREFIX = "/internal/chevalier/vfs";
const PRECONDITION_KIND_HEADER = "x-chevalier-vfs-precondition-kind";
const PRECONDITION_FINGERPRINT_HEADER = "x-chevalier-vfs-precondition-fingerprint";
const PRECONDITION_FILE_ID_HEADER = "x-chevalier-vfs-precondition-file-id";
const IF_MATCH_HEADER = "if-match";
const EXECUTABLE_HEADER = "x-chevalier-vfs-executable";
const MODE_HEADER = "x-chevalier-vfs-mode";
const EXPECTED_CONTENT_HASH_HEADER = "x-chevalier-vfs-expected-content-sha256";
const STREAM_UPLOAD_HEADER = "x-chevalier-vfs-stream-upload";
const RANGE_FINGERPRINT_HEADER = "x-chevalier-vfs-range-fingerprint";
const NAMESPACE_REVISION_HEADER = "x-chevalier-vfs-namespace-revision";
const LEASE_MODE_HEADER = "x-chevalier-vfs-lease-mode";
const ADVISORY_LOCK_LEASE_MS = 45_000;
const MAX_BATCH_ITEMS = 4096;
const MAX_OPTIMISTIC_SNAPSHOT_ATTEMPTS = 3;
/**
 * Re-resolutions allowed when a hard-link alias candidate is unlinked between
 * being found and being confirmed. Bounded because a workspace churning hard
 * enough to lose three consecutive candidates is one whose answer the caller
 * should not wait on: the unlink path needs a verdict, not a retry loop.
 */
const MAX_ALIAS_VALIDATION_ATTEMPTS = 3;

/** How many publications of affected-path history an owner retains. A watcher
 *  polls continuously, so it is normally one publication behind; this covers a
 *  watcher that missed a burst without letting the history grow with the mount's
 *  lifetime. Mirrors `PUBLICATION_HISTORY_LIMIT` in the Rust gateway. */
const PUBLICATION_HISTORY_LIMIT = 256;
/** Ceiling on paths returned for one watch answer. Package-manager namespace
 *  batches routinely affect two or three thousand paths; truncating those
 *  forces a mount-wide kernel sweep, which is both less precise and far more
 *  expensive than carrying the known set. Mirrors `WATCH_PATHS_LIMIT` in the
 *  Rust gateway. */
const WATCH_PATHS_LIMIT = 8192;

/** One publication's affected paths, retained so a lagging watcher can be told
 *  exactly what to revoke. `subtrees` is the subset of `paths` whose ENTIRE
 *  subtree the publication superseded — only `remove_directory` and `rename` can
 *  do that. Everything else is point-scoped and must never cost a sibling entry
 *  its cached metadata. */
type VfsPublishedPaths = {
  revision: number;
  paths: readonly string[];
  subtrees: readonly string[];
};

/**
 * The affected set one watch answer reports: the point-scoped paths the
 * publications touched, plus the strictly smaller set of prefixes whose whole
 * subtree they superseded.
 *
 * A watcher that cannot tell the two apart has to treat every affected path as a
 * prefix, so publishing one lock file inside `.git/` — whose affected set names
 * the parent directory — evicts every sibling's cached metadata. That was
 * measured as 30 point stats over 9 `.git` internals per warm `git status`.
 */
type VfsAffected = {
  paths: string[];
  subtrees: string[];
};

/**
 * A resolved long poll: the revision to answer with, plus the affected set
 * published in `(since, revision]`. `affected` is null when the answer cannot be
 * trusted to be complete (the watcher is behind the retained history, or the
 * union exceeds the cap) — the responder reports truncation and the watcher
 * falls back to its own conservative revocation rather than acting on a partial
 * set.
 */
type VfsWatchResult = {
  revision: number;
  affected: VfsAffected | null;
};

type VfsPublicationState = {
  revision: number;
  activityEpoch: number;
  activeReaders: number;
  activeWriter: boolean;
  queue: Array<{
    kind: "read" | "write";
    resolve: (release: () => void) => void;
  }>;
  // Parked long-poll `watch` requests. Resolved from the single writer-release
  // site the instant a mutation advances `revision` past their `since`, or by
  // their own timer on timeout. `settle` is idempotent and self-removing, so the
  // array never accumulates resolved watchers and no timer leaks.
  pendingWatchers: VfsPendingWatcher[];
  // Revocation-ack registry: watcherId -> its highest acked revision plus a
  // liveness deadline. A poll's `since` acks that revision; a watcher past its
  // deadline (absent a fresh poll) is pruned so it never gates forever.
  watcherAcks: Map<string, { ackedRevision: number; expiresAt: number }>;
  // Publications parked until their revision is acked by every live watcher.
  // Settled from `#notifyAckWaiters` on each ack, or by their own cap timer.
  pendingAckWaiters: VfsAckWaiter[];
  // Epoch-ms of the last fail-open WARN, rate-limiting it to <=1/sec/owner.
  lastAckWarnAt: number;
  // Recent publications as (revision, affected paths), newest last.
  //
  // A watcher learns only a revision number from its long poll, which leaves it
  // no choice but to invalidate everything it has cached — a revocation
  // proportional to the whole working set rather than to what actually changed.
  // Retaining the affected paths lets the watch answer with the exact set
  // instead, so a mount revokes what a publication touched and nothing else.
  // Bounded: a watcher that has fallen further behind than this history is told
  // the answer is truncated and falls back to its own conservative handling.
  publicationHistory: VfsPublishedPaths[];
  // Highest revision evicted from `publicationHistory`, or the owner's initial
  // revision before any eviction. A watcher at or beyond this floor has already
  // observed everything no longer retained; comparing only with the oldest
  // retained publication incorrectly marked the very first publication (and
  // the exact eviction boundary) as truncated.
  publicationHistoryFloor: number;
};

type VfsPendingWatcher = {
  since: number;
  settle: (revision: number) => void;
};

type VfsAckWaiter = {
  revision: number;
  settle: () => void;
};

class VfsSnapshotChangedError extends Error {}

class VfsPublicationCoordinator {
  readonly #states = new Map<string, VfsPublicationState>();
  /** Hard cap (ms) a publication waits for watcher acks before failing open. */
  readonly #ackTimeoutMs: number;
  /** Optional fixed watcher-liveness grace (ms); null derives it per poll. */
  readonly #watcherGraceOverrideMs: number | null;

  constructor(options?: { ackTimeoutMs?: number; watcherGraceMs?: number }) {
    this.#ackTimeoutMs = options?.ackTimeoutMs ?? publicationAckTimeoutFromEnv();
    this.#watcherGraceOverrideMs = options?.watcherGraceMs ?? null;
  }

  #state(ownerId: string): VfsPublicationState {
    let state = this.#states.get(ownerId);
    if (state === undefined) {
      const initialRevision = Date.now() * 1_000;
      state = {
        revision: initialRevision,
        activityEpoch: 0,
        activeReaders: 0,
        activeWriter: false,
        queue: [],
        pendingWatchers: [],
        watcherAcks: new Map(),
        pendingAckWaiters: [],
        lastAckWarnAt: 0,
        publicationHistory: [],
        publicationHistoryFloor: initialRevision,
      };
      this.#states.set(ownerId, state);
    }
    return state;
  }

  async read<T>(ownerId: string, read: () => Promise<T>): Promise<{ value: T; revision: number }> {
    const state = this.#state(ownerId);
    const release = await this.#acquire(state, "read");
    try {
      return { value: await read(), revision: state.revision };
    } finally {
      release();
    }
  }

  /**
   * Run a potentially slow recursive read without excluding mutations for its
   * duration. The two short checkpoints linearize the result only when no
   * writer overlapped the scan; otherwise the discarded scan is retried.
   */
  /**
   * The current revision, without holding the namespace against writers.
   *
   * For a read whose correctness is established by validating its own answer
   * (see the hard-link-alias handler), a whole-owner snapshot buys nothing and
   * cannot be satisfied by a busy workspace. The revision is still reported so
   * callers can fence on it.
   */
  async currentRevision(ownerId: string): Promise<number> {
    return (await this.#checkpoint(ownerId)).revision;
  }

  async optimisticRead<T>(
    ownerId: string,
    read: () => Promise<T>,
  ): Promise<{ value: T; revision: number }> {
    for (let attempt = 0; attempt < MAX_OPTIMISTIC_SNAPSHOT_ATTEMPTS; attempt += 1) {
      const before = await this.#checkpoint(ownerId);
      const value = await read();
      const after = await this.#checkpoint(ownerId);
      if (before.activityEpoch === after.activityEpoch) {
        return { value, revision: after.revision };
      }
    }
    throw new VfsSnapshotChangedError(
      "namespace changed during recursive snapshot; retry",
    );
  }

  /**
   * Publish a mutation. `paths` is the set the mutation affected, recorded with
   * the new revision so watchers can revoke precisely; omit it only where the
   * handler genuinely does not know the set (it is then recorded as empty).
   * `subtrees` is the subset of `paths` whose whole subtree the mutation
   * superseded (`remove_directory` / `rename`); omitting it means "this
   * mutation superseded no subtree", which is the truth for every point
   * mutation.
   */
  async mutate<T>(
    ownerId: string,
    mutate: () => Promise<T>,
    paths?: readonly string[],
    subtrees?: readonly string[],
  ): Promise<{ value: T; revision: number }> {
    return this.transact(ownerId, async () => ({
      value: await mutate(),
      mutated: true,
      paths,
      subtrees,
    }));
  }

  async transact<T>(
    ownerId: string,
    transaction: () => Promise<{
      value: T;
      mutated: boolean;
      paths?: readonly string[];
      subtrees?: readonly string[];
    }>,
  ): Promise<{ value: T; revision: number }> {
    const state = this.#state(ownerId);
    const release = await this.#acquire(state, "write");
    let outcome!: { value: T; revision: number; mutated: boolean };
    try {
      const { value, mutated, paths, subtrees } = await transaction();
      if (mutated) {
        state.revision = Math.max(state.revision + 1, Date.now() * 1_000);
        // Record before releasing the writer, which is what wakes parked
        // watchers: a watcher woken for a revision must never find the history
        // missing the revision it was woken for. A publication whose affected
        // set is unknown records an empty entry rather than none — a MISSING
        // revision is what forces a later watcher onto the truncated fallback,
        // so skipping the entry would silently downgrade every watcher behind
        // it.
        this.#recordPublication(state, state.revision, paths ?? [], subtrees ?? []);
      }
      outcome = { value, revision: state.revision, mutated };
    } finally {
      release();
    }
    if (outcome.mutated) {
      // Revocation-ack: the write lock is already released and the revision
      // published, so this holds NO lock — it purely delays the HTTP response
      // until every live watcher has re-polled past this revision (or the cap).
      await this.#awaitPublicationAcks(state, outcome.revision);
    }
    return { value: outcome.value, revision: outcome.revision };
  }

  #acquire(
    state: VfsPublicationState,
    kind: "read" | "write",
  ): Promise<() => void> {
    if (
      kind === "read" &&
      !state.activeWriter &&
      !state.queue.some((waiter) => waiter.kind === "write")
    ) {
      state.activeReaders += 1;
      return Promise.resolve(this.#releaseReader(state));
    }
    if (
      kind === "write" &&
      !state.activeWriter &&
      state.activeReaders === 0 &&
      state.queue.length === 0
    ) {
      state.activeWriter = true;
      return Promise.resolve(this.#releaseWriter(state));
    }
    return new Promise((resolve) => {
      state.queue.push({ kind, resolve });
    });
  }

  #releaseReader(state: VfsPublicationState): () => void {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      state.activeReaders -= 1;
      this.#drain(state);
    };
  }

  #releaseWriter(state: VfsPublicationState): () => void {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      state.activityEpoch += 1;
      state.activeWriter = false;
      // A writer just released; if it advanced the revision, wake every parked
      // watcher whose `since` it passed. Non-mutating writers leave `revision`
      // unchanged, so no parked watcher (all with `since >= revision`) matches.
      this.#notifyWatchers(state);
      this.#drain(state);
    };
  }

  /**
   * Long-poll for the owner's revision to advance past `since`. Resolves to the
   * current revision: immediately when it already exceeds `since`, otherwise as
   * soon as a mutation advances it, or on `timeoutMs` with the revision
   * unchanged (the caller distinguishes 200 vs 204 by comparing against `since`).
   *
   * Parking never touches the reader/writer lock, so a watch can neither block
   * nor slow a concurrent mutation — the mutation's only added cost is the
   * `#notifyWatchers` scan on release.
   */
  watch(
    ownerId: string,
    since: number,
    timeoutMs: number,
    watcherId: string,
  ): Promise<VfsWatchResult> {
    const state = this.#state(ownerId);
    // This poll's `since` acks that revision for this watcher and unblocks any
    // sibling publication waiting on it. Register before the fast path so a fast
    // 200 still counts as an ack. Anonymous watchers (empty id) are not
    // registered: they are notified but never gate a publication.
    this.#recordWatcherAck(state, watcherId, since, timeoutMs);
    // Fast path: reading `state.revision` and (below) registering the watcher
    // happen with no `await` between them, so a mutation cannot slip in and be
    // missed — JS runs this to completion before any writer's revision bump.
    if (state.revision > since) {
      return Promise.resolve(this.#watchResult(state, state.revision, since));
    }
    return new Promise<VfsWatchResult>((resolve) => {
      let settled = false;
      const watcher: VfsPendingWatcher = {
        since,
        settle: (revision: number) => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          const index = state.pendingWatchers.indexOf(watcher);
          if (index >= 0) state.pendingWatchers.splice(index, 1);
          resolve(this.#watchResult(state, revision, since));
        },
      };
      const timer = setTimeout(() => watcher.settle(state.revision), timeoutMs);
      state.pendingWatchers.push(watcher);
    });
  }

  /**
   * Pair `revision` with the paths that carried the watcher to it. Always called
   * in the same synchronous turn that read `revision` — the fast path above, or
   * `settle` inside the writer release — so the answered paths cover exactly the
   * revisions the watcher is being advanced across, with no publication able to
   * interleave between the two reads. (This is the JS equivalent of the Rust
   * gateway reading the history under the guard that produced `current`.)
   */
  #watchResult(
    state: VfsPublicationState,
    revision: number,
    since: number,
  ): VfsWatchResult {
    // An unadvanced revision is answered 204, which carries no paths.
    if (revision <= since) return { revision, affected: { paths: [], subtrees: [] } };
    return { revision, affected: this.#pathsPublishedSince(state, since) };
  }

  /** Retain a publication's affected paths for lagging watchers, evicting the
   *  oldest once the bound is reached. `subtrees` is a subset of `paths` for
   *  every producer in this file. */
  #recordPublication(
    state: VfsPublicationState,
    revision: number,
    paths: readonly string[],
    subtrees: readonly string[],
  ): void {
    state.publicationHistory.push({
      revision,
      paths: [...new Set(paths.map(normalizePath))],
      subtrees: [...new Set(subtrees.map(normalizePath))],
    });
    while (state.publicationHistory.length > PUBLICATION_HISTORY_LIMIT) {
      const evicted = state.publicationHistory.shift();
      if (evicted !== undefined) state.publicationHistoryFloor = evicted.revision;
    }
  }

  /**
   * The union of paths published in `(since, current]`, together with the union
   * of the subtree prefixes among them.
   *
   * Returns null when the answer cannot be trusted to be complete — the watcher
   * is further behind than the retained history, or the union exceeds
   * `WATCH_PATHS_LIMIT` — in which case the caller reports truncation and the
   * watcher falls back to its own handling rather than acting on a partial set.
   */
  #pathsPublishedSince(state: VfsPublicationState, since: number): VfsAffected | null {
    if (state.publicationHistory.length === 0) return null;
    // `since` must be covered: the watcher needs every publication after it, and
    // anything before the eviction floor may have evicted publications the
    // watcher never saw. The floor itself is covered: `since` means the watcher
    // has already observed that exact revision.
    if (since < state.publicationHistoryFloor) return null;
    const seen = new Set<string>();
    const union: string[] = [];
    const seenSubtrees = new Set<string>();
    const subtrees: string[] = [];
    for (const entry of state.publicationHistory) {
      if (entry.revision <= since) continue;
      for (const path of entry.paths) {
        if (seen.has(path)) continue;
        seen.add(path);
        union.push(path);
        if (union.length > WATCH_PATHS_LIMIT) return null;
      }
      for (const prefix of entry.subtrees) {
        if (seenSubtrees.has(prefix)) continue;
        seenSubtrees.add(prefix);
        subtrees.push(prefix);
        // Subtrees are a subset of `paths` for every producer here, so this can
        // only fire if that invariant is ever broken; truncation is the
        // fail-closed answer.
        if (subtrees.length > WATCH_PATHS_LIMIT) return null;
      }
    }
    return { paths: union, subtrees };
  }

  #notifyWatchers(state: VfsPublicationState): void {
    if (state.pendingWatchers.length === 0) return;
    const revision = state.revision;
    // Iterate a snapshot because `settle` splices the watcher out of the live
    // array; guard on `since < revision` so only truly-passed watchers wake.
    for (const watcher of [...state.pendingWatchers]) {
      if (watcher.since < revision) watcher.settle(revision);
    }
  }

  /**
   * Record that `watcherId` has observed (acked) `since`, refreshing its
   * liveness deadline (2x its poll timeout, capped 60s, or a fixed override).
   * Anonymous watchers (empty id) are never registered. Wakes any publication
   * whose revision this ack now satisfies.
   */
  #recordWatcherAck(
    state: VfsPublicationState,
    watcherId: string,
    since: number,
    timeoutMs: number,
  ): void {
    if (watcherId === "") return;
    const graceMs =
      this.#watcherGraceOverrideMs ?? Math.min(timeoutMs * 2, 60_000);
    const existing = state.watcherAcks.get(watcherId);
    state.watcherAcks.set(watcherId, {
      ackedRevision: Math.max(existing?.ackedRevision ?? 0, since),
      expiresAt: Date.now() + graceMs,
    });
    this.#notifyAckWaiters(state);
  }

  /**
   * Count registered watchers that have not yet acked `revision`, pruning any
   * past their liveness deadline first. Zero means every live watcher has
   * observed the revision (or there are none).
   */
  #unackedWatchers(state: VfsPublicationState, revision: number): number {
    const now = Date.now();
    for (const [id, ack] of state.watcherAcks) {
      if (ack.expiresAt <= now) state.watcherAcks.delete(id);
    }
    let laggards = 0;
    for (const ack of state.watcherAcks.values()) {
      if (ack.ackedRevision < revision) laggards += 1;
    }
    return laggards;
  }

  /** Wake parked publications whose revision is now fully acked. */
  #notifyAckWaiters(state: VfsPublicationState): void {
    if (state.pendingAckWaiters.length === 0) return;
    for (const waiter of [...state.pendingAckWaiters]) {
      if (this.#unackedWatchers(state, waiter.revision) === 0) waiter.settle();
    }
  }

  /**
   * Block until every live watcher acks `revision`, bounded by the ack cap.
   * Returns at once when no watcher lags (single-mount / all-acked fast path).
   * On cap expiry with laggards, resolves fail-open and logs a rate-limited WARN.
   */
  async #awaitPublicationAcks(
    state: VfsPublicationState,
    revision: number,
  ): Promise<void> {
    if (this.#unackedWatchers(state, revision) === 0) return;
    const cap = this.#ackTimeoutMs;
    if (cap <= 0) return;
    await new Promise<void>((resolve) => {
      let settled = false;
      const settle = () => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        const index = state.pendingAckWaiters.indexOf(waiter);
        if (index >= 0) state.pendingAckWaiters.splice(index, 1);
        resolve();
      };
      const waiter: VfsAckWaiter = { revision, settle };
      const timer = setTimeout(() => {
        const laggards = this.#unackedWatchers(state, revision);
        if (laggards > 0) this.#warnPublicationLag(state, revision, laggards);
        settle();
      }, cap);
      state.pendingAckWaiters.push(waiter);
    });
  }

  /** Emit at most one fail-open WARN per second per owner, naming the laggards. */
  #warnPublicationLag(
    state: VfsPublicationState,
    revision: number,
    laggards: number,
  ): void {
    const now = Date.now();
    if (now - state.lastAckWarnAt < 1_000) return;
    state.lastAckWarnAt = now;
    console.warn(
      `vfs publication ack cap elapsed; proceeding fail-open with ${laggards} ` +
        `unacked watcher(s) at revision ${revision}`,
    );
  }

  async #checkpoint(
    ownerId: string,
  ): Promise<{ revision: number; activityEpoch: number }> {
    const state = this.#state(ownerId);
    const release = await this.#acquire(state, "read");
    try {
      return {
        revision: state.revision,
        activityEpoch: state.activityEpoch,
      };
    } finally {
      release();
    }
  }

  #drain(state: VfsPublicationState): void {
    if (state.activeWriter || state.activeReaders !== 0 || state.queue.length === 0) return;
    if (state.queue[0]?.kind === "write") {
      const waiter = state.queue.shift();
      if (waiter === undefined) return;
      state.activeWriter = true;
      waiter.resolve(this.#releaseWriter(state));
      return;
    }
    while (state.queue[0]?.kind === "read") {
      const waiter = state.queue.shift();
      if (waiter === undefined) break;
      state.activeReaders += 1;
      waiter.resolve(this.#releaseReader(state));
    }
  }
}

export type VfsAdvisoryLockKind = "read" | "write";
export type VfsAdvisoryLockNamespace = "posix" | "flock";

export type VfsAdvisoryLock = {
  ownerId: string;
  mountId: string;
  lockOwner: string;
  namespace: VfsAdvisoryLockNamespace;
  fileId: string;
  start: bigint;
  end: bigint;
  kind: VfsAdvisoryLockKind;
  pid: number;
  expiresAt: number;
};

export type VfsAdvisoryLockTransactionResult<T> = {
  locks: VfsAdvisoryLock[];
  result: T;
};

/**
 * Transactional state boundary for the POSIX lock coordinator. Implementations
 * must serialize transactions for one owner across every gateway process.
 */
export interface VfsAdvisoryLockStateStore {
  transact<T>(
    ownerId: string,
    transaction: (locks: VfsAdvisoryLock[]) => VfsAdvisoryLockTransactionResult<T>,
  ): Promise<T>;
}

type AdvisoryLockRequest = {
  action: "get" | "set" | "release_owner" | "renew_owners" | "renew_mount" | "release_mount";
  path?: string;
  file_id?: string;
  mount_id?: string;
  lock_owner?: string;
  namespace?: VfsAdvisoryLockNamespace;
  identities?: unknown;
  start?: string;
  end?: string;
  kind?: VfsAdvisoryLockKind | "unlock";
  pid?: number;
};

type AdvisoryLockRenewalIdentity = {
  lockOwner: string;
  namespace: VfsAdvisoryLockNamespace;
  fileId: string;
};

/**
 * Coordinates leased POSIX locks independently from write authorization and
 * namespace mutation leases. Production multi-process gateways supply a shared
 * transactional state store; the default is intentionally process-local for
 * embedders and tests.
 */
class AdvisoryLockCoordinator {
  constructor(private readonly state: VfsAdvisoryLockStateStore) {}

  async handle(ownerId: string, fileId: string | null, request: AdvisoryLockRequest): Promise<Response> {
    const now = Date.now();
    const mountId = nonEmptyString(request.mount_id);
    if (mountId === null) return errorResponse(400, "posix lock requires mount_id");

    if (request.action === "renew_mount") {
      return errorResponse(400, "renew_mount is unsupported; use renew_owners with exact identities");
    }
    if (request.action === "renew_owners") {
      const identities = normalizeAdvisoryLockRenewalIdentities(request.identities);
      if (identities instanceof Response) return identities;
      const identityKeys = new Set(
        identities.map(({ lockOwner, namespace, fileId }) =>
          advisoryLockIdentityKey(lockOwner, namespace, fileId),
        ),
      );
      return this.state.transact(ownerId, (stored) => {
        const locks = liveLocks(stored, now);
        for (const lock of locks) {
          if (
            lock.mountId === mountId &&
            identityKeys.has(
              advisoryLockIdentityKey(
                lock.lockOwner,
                lock.namespace ?? "posix",
                lock.fileId,
              ),
            )
          ) {
            lock.expiresAt = now + ADVISORY_LOCK_LEASE_MS;
          }
        }
        return {
          locks,
          result: json(200, { ok: true, lease_ms: ADVISORY_LOCK_LEASE_MS }),
        };
      });
    }
    if (request.action === "release_mount") {
      return this.state.transact(ownerId, (stored) => ({
        locks: liveLocks(stored, now).filter((lock) => lock.mountId !== mountId),
        result: json(200, { ok: true }),
      }));
    }

    const lockOwner = nonEmptyString(request.lock_owner);
    if (lockOwner === null) return errorResponse(400, "posix lock requires lock_owner");
    const namespace = request.namespace ?? "posix";
    if (namespace !== "posix" && namespace !== "flock") {
      return errorResponse(400, "advisory lock namespace must be posix or flock");
    }
    if (request.action === "release_owner") {
      const releasedFileId = nonEmptyString(request.file_id);
      if (releasedFileId === null) return errorResponse(400, "posix lock release requires file_id");
      return this.state.transact(ownerId, (stored) => ({
        locks: liveLocks(stored, now).filter(
          (lock) =>
            lock.mountId !== mountId ||
            lock.lockOwner !== lockOwner ||
            lock.fileId !== releasedFileId ||
            (lock.namespace ?? "posix") !== namespace,
        ),
        result: json(200, { ok: true }),
      }));
    }
    if (fileId === null) return errorResponse(501, "stable file identity is unavailable for posix locking");

    const start = parseLockOffset(request.start, "start");
    if (start instanceof Response) return start;
    const end = parseLockOffset(request.end, "end");
    if (end instanceof Response) return end;
    if (end < start) return errorResponse(400, "posix lock end precedes start");
    const kind = request.kind;
    if (kind !== "read" && kind !== "write" && kind !== "unlock") {
      return errorResponse(400, "posix lock kind must be read, write, or unlock");
    }
    const pid = Number.isSafeInteger(request.pid) && (request.pid ?? -1) >= 0 ? request.pid! : 0;
    const identity = { ownerId, mountId, lockOwner, namespace, fileId, start, end, kind, pid };

    if (request.action === "get") {
      if (kind === "unlock") return errorResponse(400, "get posix lock cannot query unlock");
      return this.state.transact(ownerId, (stored) => {
        const locks = liveLocks(stored, now);
        const conflict = firstConflict(locks, { ...identity, kind });
        return {
          locks,
          result: json(200, {
            acquired: conflict === undefined,
            conflict: conflict === undefined ? null : lockResponse(conflict),
            file_id: fileId,
            lease_ms: ADVISORY_LOCK_LEASE_MS,
          }),
        };
      });
    }
    if (request.action !== "set") return errorResponse(400, `unsupported posix lock action: ${request.action}`);

    return this.state.transact(ownerId, (stored) => {
      const locks = liveLocks(stored, now);
      const ownKey = (lock: VfsAdvisoryLock): boolean =>
        lock.mountId === mountId &&
        lock.lockOwner === lockOwner &&
        (lock.namespace ?? "posix") === namespace &&
        lock.fileId === fileId;
      const replacement = locks.flatMap((lock) => {
        if (!ownKey(lock) || !rangesOverlap(lock.start, lock.end, start, end)) return [lock];
        return subtractRange(lock, start, end);
      });
      if (kind === "unlock") {
        return {
          locks: replacement,
          result: json(200, {
            acquired: true,
            conflict: null,
            file_id: fileId,
            lease_ms: ADVISORY_LOCK_LEASE_MS,
          }),
        };
      }

      const conflict = firstConflict(replacement, { ...identity, kind });
      if (conflict !== undefined) {
        return {
          // A failed F_SETLK conversion must leave every lock that the caller
          // already held unchanged. `replacement` contains the proposed
          // subtract/convert state; persisting it here would silently drop the
          // caller's old range when (for example) a read-to-write upgrade is
          // rejected by another reader.
          locks,
          result: json(200, {
            acquired: false,
            conflict: lockResponse(conflict),
            file_id: fileId,
            lease_ms: ADVISORY_LOCK_LEASE_MS,
          }),
        };
      }
      replacement.push({
        ownerId,
        mountId,
        lockOwner,
        namespace,
        fileId,
        start,
        end,
        kind,
        pid,
        expiresAt: now + ADVISORY_LOCK_LEASE_MS,
      });
      return {
        locks: replacement,
        result: json(200, {
          acquired: true,
          conflict: null,
          file_id: fileId,
          lease_ms: ADVISORY_LOCK_LEASE_MS,
        }),
      };
    });
  }
}

class InMemoryAdvisoryLockStateStore implements VfsAdvisoryLockStateStore {
  private readonly byOwner = new Map<string, VfsAdvisoryLock[]>();

  async transact<T>(
    ownerId: string,
    transaction: (locks: VfsAdvisoryLock[]) => VfsAdvisoryLockTransactionResult<T>,
  ): Promise<T> {
    const outcome = transaction([...(this.byOwner.get(ownerId) ?? [])]);
    if (outcome.locks.length === 0) {
      this.byOwner.delete(ownerId);
    } else {
      this.byOwner.set(ownerId, outcome.locks);
    }
    return outcome.result;
  }
}

function liveLocks(locks: VfsAdvisoryLock[], now: number): VfsAdvisoryLock[] {
  return locks.filter((lock) => lock.expiresAt > now);
}

function firstConflict(
  locks: VfsAdvisoryLock[],
  request: {
    ownerId: string;
    mountId: string;
    lockOwner: string;
    namespace: VfsAdvisoryLockNamespace;
    fileId: string;
    start: bigint;
    end: bigint;
    kind: VfsAdvisoryLockKind;
  },
): VfsAdvisoryLock | undefined {
  return locks.find(
    (lock) =>
      lock.fileId === request.fileId &&
      (lock.namespace ?? "posix") === request.namespace &&
      !(lock.mountId === request.mountId && lock.lockOwner === request.lockOwner) &&
      rangesOverlap(lock.start, lock.end, request.start, request.end) &&
      (lock.kind === "write" || request.kind === "write"),
  );
}

function normalizeAdvisoryLockRenewalIdentities(
  value: unknown,
): AdvisoryLockRenewalIdentity[] | Response {
  if (!Array.isArray(value) || value.length === 0) {
    return errorResponse(400, "renew_owners requires a non-empty identities[]");
  }
  if (value.length > MAX_BATCH_ITEMS) {
    return errorResponse(
      400,
      `renew_owners accepts at most ${MAX_BATCH_ITEMS} identities`,
    );
  }

  const identities: AdvisoryLockRenewalIdentity[] = [];
  for (const [index, item] of value.entries()) {
    if (typeof item !== "object" || item === null || Array.isArray(item)) {
      return errorResponse(400, `renew_owners identity ${index} must be an object`);
    }
    const identity = item as Record<string, unknown>;
    const lockOwner = nonEmptyString(identity.lock_owner);
    if (lockOwner === null) {
      return errorResponse(400, `renew_owners identity ${index} requires lock_owner`);
    }
    const fileId = nonEmptyString(identity.file_id);
    if (fileId === null) {
      return errorResponse(400, `renew_owners identity ${index} requires file_id`);
    }
    const namespace = identity.namespace;
    if (namespace !== "posix" && namespace !== "flock") {
      return errorResponse(
        400,
        `renew_owners identity ${index} namespace must be posix or flock`,
      );
    }
    identities.push({ lockOwner, namespace, fileId });
  }
  return identities;
}

function advisoryLockIdentityKey(
  lockOwner: string,
  namespace: VfsAdvisoryLockNamespace,
  fileId: string,
): string {
  return JSON.stringify([lockOwner, namespace, fileId]);
}

function nonEmptyString(value: unknown): string | null {
  return typeof value === "string" && value.trim() !== "" ? value : null;
}

function parseLockOffset(value: unknown, name: string): bigint | Response {
  if (typeof value !== "string" || !/^\d+$/.test(value)) {
    return errorResponse(400, `posix lock ${name} must be an unsigned decimal string`);
  }
  try {
    return BigInt(value);
  } catch {
    return errorResponse(400, `invalid posix lock ${name}`);
  }
}

function rangesOverlap(aStart: bigint, aEnd: bigint, bStart: bigint, bEnd: bigint): boolean {
  return aStart <= bEnd && bStart <= aEnd;
}

function subtractRange(lock: VfsAdvisoryLock, start: bigint, end: bigint): VfsAdvisoryLock[] {
  const out: VfsAdvisoryLock[] = [];
  if (lock.start < start) out.push({ ...lock, end: start - 1n });
  if (lock.end > end) out.push({ ...lock, start: end + 1n });
  return out;
}

function lockResponse(lock: VfsAdvisoryLock) {
  return {
    start: lock.start.toString(),
    end: lock.end.toString(),
    kind: lock.kind,
    pid: lock.pid,
  };
}

type StreamingWrite = {
  path: string;
  body: number[];
  /** Applied only when the write creates the path; ignored on overwrite. */
  mode?: number;
  precondition?: { predicate?: VfsCasPredicate; expected_file_id?: string };
};

type StreamingBase64Write = Omit<StreamingWrite, "body"> & {
  body_base64: string;
};

type StreamingVfsStorage = VfsStorage & {
  readRange?: (path: string, offset: bigint, length: number) => Promise<Buffer>;
  writeFromFile?: (
    path: string,
    sourcePath: string,
    expectedContentHash: string,
    options?: {
      ifMatch?: string | null;
      expectedFileId?: string | null;
      executable?: boolean;
      mode?: number;
    } | null,
  ) => Promise<unknown>;
  writeMany?: (writes: StreamingWrite[]) => Promise<StreamingWriteManyResult[]>;
  writeManyBase64?: (writes: StreamingBase64Write[]) => Promise<StreamingWriteManyResult[]>;
  prefetchSubtree?: (
    prefix: string,
    options?: {
      includeSmallFileBytes?: boolean;
      maxEntries?: number;
      maxPackBytes?: number;
    },
  ) => Promise<Array<{ path: string; body: Buffer }>>;
};

type StreamingWriteManyResult = {
  path: string;
  content_hash?: string;
  contentHash?: string;
  previous_hash?: string | null;
  previousHash?: string | null;
  changed: boolean;
};

export interface VfsGatewayServerOptions {
  /** Map a request's `{owner_id}` to the backing store. Typically
   *  `(ownerId) => VfsStorage.local(scopeRootFor(ownerId))`. */
  resolveStore: (ownerId: string) => VfsStorage | Promise<VfsStorage>;
  /** If set, requests must carry `Authorization: Bearer <authToken>`. */
  authToken?: string;
  /** Route prefix the routes live under. Default `/internal/chevalier/vfs`. */
  routePrefix?: string;
  /** Shared transactional state for POSIX advisory locks. Production gateways
   *  with more than one process must provide a cross-process implementation. */
  advisoryLockState?: VfsAdvisoryLockStateStore;
  /** Per-owner product policy gate for replica-local `.git` metadata.
   *  Defaults to false, preserving the historical exclusion. */
  allowGitMetadata?: (ownerId: string) => boolean | Promise<boolean>;
  /** Hard cap (ms) a mutation waits for live watchers to ack the published
   *  revision before proceeding fail-open. Default 25 (a healthy watcher acks
   *  within a few ms; a longer cap only charges a lagging watcher to the
   *  writer). Overridable via
   *  `CHEVALIER_VFS_PUBLICATION_ACK_TIMEOUT_MS`. */
  publicationAckTimeoutMs?: number;
  /** Override (ms) for how long a silent watcher stays registered before it is
   *  pruned. Defaults to 2x the watcher's poll timeout (capped 60s). Advanced /
   *  test knob. */
  publicationWatcherGraceMs?: number;
}

/** Build a WHATWG `(Request) => Promise<Response>` handler that serves chevalier's
 *  VFS gateway protocol, delegating storage to `resolveStore(ownerId)`.
 *
 *  HOSTING REQUIREMENT — the revision-watch route (`GET .../watch`) is a long
 *  poll held open up to ~25s (see `REVISION_WATCH_TIMEOUT_MS` on the vmd client).
 *  Whatever http server hosts this handler MUST keep idle keep-alive
 *  connections open comfortably past that window — for Node's `http.Server`,
 *  set `server.keepAliveTimeout` (default 5s) and `server.headersTimeout` well
 *  above 25s (e.g. 120s / 125s). The default 5s reaps the watch's idle socket
 *  and RSTs it, so the client's next pooled poll fails with a send-class error
 *  and the watch flaps to strict serves. (The OB API host is configured
 *  separately.) */
export function createVfsGatewayServer(
  opts: VfsGatewayServerOptions,
): (req: Request) => Promise<Response> {
  const prefix = opts.routePrefix ?? DEFAULT_ROUTE_PREFIX;
  const advisoryLocks = new AdvisoryLockCoordinator(
    opts.advisoryLockState ?? new InMemoryAdvisoryLockStateStore(),
  );
  const publications = new VfsPublicationCoordinator({
    ackTimeoutMs: opts.publicationAckTimeoutMs,
    watcherGraceMs: opts.publicationWatcherGraceMs,
  });

  return async function handle(req: Request): Promise<Response> {
    try {
      if (opts.authToken !== undefined && opts.authToken !== "") {
        const auth = req.headers.get("authorization") ?? "";
        if (auth !== `Bearer ${opts.authToken}`) return errorResponse(401, "unauthorized");
      }

      const url = new URL(req.url);
      const idx = url.pathname.indexOf(prefix);
      if (idx < 0) return errorResponse(404, "not a chevalier vfs route");
      const rest = url.pathname.slice(idx + prefix.length).replace(/^\/+/, "");
      const segs = rest.split("/").filter((s) => s !== "");
      const ownerId = segs.shift() ?? "";
      const op = segs.join("/");
      if (ownerId === "") return errorResponse(404, "missing owner_id segment");

      // Long-poll revision watch. Handled before store resolution so a parked
      // watch never resolves/holds a store, and it shares the bearer auth checked
      // above with every other route.
      if (req.method.toUpperCase() === "GET" && op === "watch") {
        const since = parseWatchSince(url.searchParams.get("since"));
        const timeoutMs = parseWatchTimeout(url.searchParams.get("timeout_ms"));
        const watcherId = parseWatcherId(url.searchParams.get("watcher_id"));
        const { revision, affected } = await publications.watch(
          ownerId,
          since,
          timeoutMs,
          watcherId,
        );
        if (revision > since) {
          // `paths` and `subtrees` are serialized whenever the affected set is
          // known, the empty set included: an omitted field means "this gateway
          // does not report that set" and sends the watcher down a conservative
          // fallback, which is a different statement from "this publication
          // touched nothing" / "this publication superseded no subtree". For
          // `subtrees` that fallback is specifically "treat every affected path
          // as a prefix", which is what an older gateway forces. `truncated`
          // appears only when true, and then the watcher must not treat either
          // list as exhaustive.
          const body =
            affected === null
              ? { revision, paths: [], subtrees: [], truncated: true }
              : { revision, paths: affected.paths, subtrees: affected.subtrees };
          return withNamespaceRevision(json(200, body), revision);
        }
        return withNamespaceRevision(new Response(null, { status: 204 }), revision);
      }

      const allowGitMetadata = (await opts.allowGitMetadata?.(ownerId)) ?? false;
      const isExcludedPath = (path: string) =>
        !allowGitMetadata && isGitExcludedPath(path);

      const store = await opts.resolveStore(ownerId);
      const q = url.searchParams;
      const method = req.method.toUpperCase();
      const relPath = normalizePath(q.get("path"));

      if (method === "POST" && op === "posix-lock/v1") {
        const body = (await req.json().catch(() => null)) as AdvisoryLockRequest | null;
        if (body === null || typeof body !== "object" || typeof body.action !== "string") {
          return errorResponse(400, "invalid posix lock request");
        }
        if (
          body.action === "renew_owners" ||
          body.action === "renew_mount" ||
          body.action === "release_mount" ||
          body.action === "release_owner"
        ) {
          return await advisoryLocks.handle(ownerId, null, body);
        }
        const lockPath = normalizePath(body.path ?? null);
        if (lockPath === "" || isExcludedPath(lockPath)) {
          return errorResponse(400, `invalid posix lock path: ${lockPath}`);
        }
        const metadata = await store.stat(lockPath);
        if (metadata === null) return errorResponse(404, `not found: ${lockPath}`);
        return await advisoryLocks.handle(ownerId, metadata.fileId ?? null, body);
      }

      // ---- reads ----------------------------------------------------------
      if (method === "GET" && op === "stat") {
        if (isExcludedPath(relPath)) return errorResponse(404, `not found: ${relPath}`);
        const maxHashBytes = parseOptionalNonNegativeInteger(
          q.get("max_hash_bytes") ?? q.get("maxHashBytes"),
          "max_hash_bytes",
        );
        if (maxHashBytes instanceof Response) return maxHashBytes;
        const snapshot = await publications.read(ownerId, () =>
          store.stat(
            relPath,
            maxHashBytes === null ? undefined : { maxHashBytes },
          ),
        );
        const md = snapshot.value;
        if (md === null) {
          return withNamespaceRevision(
            errorResponse(404, `not found: ${relPath}`),
            snapshot.revision,
          );
        }
        return withNamespaceRevision(
          json(200, toRemoteMetadata(md)),
          snapshot.revision,
        );
      }

      if (method === "GET" && op === "file/raw") {
        if (isExcludedPath(relPath)) return errorResponse(404, `not found: ${relPath}`);
        const snapshot = await publications.read(ownerId, async () => {
        const requestedRange = req.headers.get("range");
        if (requestedRange !== null) {
          // Ranged reads are path-addressed across many requests, so they carry
          // a cheap (size, mtime) fingerprint instead of a content hash: the
          // client pins the file identity it started reading, and a replace in
          // between surfaces as 412 rather than a spliced old/new file. The
          // hashless stat also avoids re-hashing large files per range.
          let metadata: VfsMetadata | null;
          try {
            metadata = await store.stat(relPath, { maxHashBytes: 0 });
          } catch (error) {
            if (isVfsNotFoundError(error)) return errorResponse(404, `not found: ${relPath}`);
            throw error;
          }
          if (metadata === null) return errorResponse(404, `not found: ${relPath}`);
          const fingerprint = rangeFingerprint(metadata);
          const expectedFingerprint = req.headers.get(RANGE_FINGERPRINT_HEADER);
          if (expectedFingerprint !== null && expectedFingerprint !== fingerprint) {
            return staleRangeResponse(relPath, fingerprint);
          }
          const size = Number(metadata.sizeBytes);
          const range = parseRange(requestedRange, size);
          if (range === null) return errorResponse(416, `invalid range for ${relPath}`);
          const length = range.end - range.start + 1;
          const streamingStore = store as StreamingVfsStorage;
          const slice =
            typeof streamingStore.readRange === "function"
              ? await streamingStore.readRange(relPath, BigInt(range.start), length)
              : (await store.read(relPath)).subarray(range.start, range.end + 1);
          // Bracket the read: if the file changed while we were reading it, the
          // slice may mix old and new bytes. Never return it.
          let after: VfsMetadata | null;
          try {
            after = await store.stat(relPath, { maxHashBytes: 0 });
          } catch (error) {
            if (isVfsNotFoundError(error)) return errorResponse(404, `not found: ${relPath}`);
            throw error;
          }
          if (after === null) return errorResponse(404, `not found: ${relPath}`);
          const afterFingerprint = rangeFingerprint(after);
          if (afterFingerprint !== fingerprint) {
            return staleRangeResponse(relPath, afterFingerprint);
          }
          return new Response(asBody(slice), {
            status: 206,
            headers: {
              "content-type": "application/octet-stream",
              "content-range": `bytes ${range.start}-${range.end}/${size}`,
              "content-length": String(slice.byteLength),
              [RANGE_FINGERPRINT_HEADER]: fingerprint,
            },
          });
        }
        let buf: Buffer;
        try {
          buf = await store.read(relPath);
        } catch (error) {
          if (isVfsNotFoundError(error)) return errorResponse(404, `not found: ${relPath}`);
          throw error;
        }
        return new Response(asBody(buf), {
          status: 200,
          headers: { "content-type": "application/octet-stream" },
        });
        });
        return withNamespaceRevision(snapshot.value, snapshot.revision);
      }

      if (method === "GET" && op === "tree") {
        if (isExcludedPath(relPath)) return errorResponse(404, `not found: ${relPath}`);
        const dir = relPath === "" ? "." : relPath;
        const maxHashBytes = parseOptionalNonNegativeInteger(
          q.get("max_hash_bytes") ?? q.get("maxHashBytes"),
          "max_hash_bytes",
        );
        if (maxHashBytes instanceof Response) return maxHashBytes;
        const snapshot = await publications.read(ownerId, async () => {
          try {
            return {
              entries: await store.listDir(
                dir,
                maxHashBytes === null ? undefined : { maxHashBytes },
              ),
              missing: false,
            };
          } catch (error) {
            if (isVfsNotFoundError(error)) {
              return { entries: [] as VfsMetadata[], missing: true };
            }
            throw error;
          }
        });
        if (snapshot.value.missing) {
          return withNamespaceRevision(
            errorResponse(404, `not found: ${dir}`),
            snapshot.revision,
          );
        }
        const nameLike = q.get("name_like");
        const nameNotLike = q.get("name_not_like");
        const out = snapshot.value.entries
          .filter((entry) => !isExcludedPath(entry.path))
          .map(toRemoteDirEntry)
          .filter((e) => (nameLike === null || e.name.includes(nameLike)))
          .filter((e) => (nameNotLike === null || !e.name.includes(nameNotLike)));
        return withNamespaceRevision(json(200, out), snapshot.revision);
      }

      // ---- leases (mutations acquire/release one; we issue a synthetic grant) --
      if (op === "lease" && method === "POST") {
        const body = (await req.json().catch(() => ({}))) as { path?: unknown };
        const leasePath = normalizePath(typeof body.path === "string" ? body.path : relPath);
        if (isExcludedPath(leasePath)) return errorResponse(400, `excluded path: ${leasePath}`);
        const response = json(200, {
          resource_key: `rk:${ownerId}:${leasePath}`,
          owner_token: randomToken(),
        });
        response.headers.set(LEASE_MODE_HEADER, "implicit");
        return response;
      }
      if (op === "lease" && method === "DELETE") {
        return new Response(null, { status: 204 });
      }

      if (method === "POST" && op === "namespace-many") {
        const body = await requestJsonObject(req, "namespace-many");
        if (body instanceof Response) return body;
        const operationIds = normalizeNamespaceOperationIds(body.operation_ids);
        if (operationIds instanceof Response) return operationIds;
        const mutations = normalizeNamespaceMutations(body.mutations, isExcludedPath);
        if (mutations instanceof Response) return mutations;
        //  Supplied ids must still line up one-to-one; absent ids are minted here
        //  so the batch is applied with the same shape either way.
        if (operationIds.length === 0) {
          for (let index = 0; index < mutations.length; index += 1) {
            operationIds.push(`srv-${randomUUID()}`);
          }
        } else if (operationIds.length !== mutations.length) {
          return errorResponse(
            400,
            "namespace-many requires one operation_id per mutation",
          );
        }
        try {
          const affected = mutationSnapshotPaths(mutations);
          const superseded = mutationSupersededSubtrees(mutations);
          const publication = await publications.mutate(
            ownerId,
            async () => {
              await store.applyNamespaceBatch(mutations);
              return { entries: await snapshotPaths(store, affected) };
            },
            affected,
            superseded,
          );
          return withNamespaceRevision(
            json(200, publication.value),
            publication.revision,
          );
        } catch (error) {
          const conflict = conflictResponseFromStoreError(error, "namespace-many");
          if (conflict !== null) return conflict;
          throw error;
        }
      }

      // ---- single-file mutations -----------------------------------------
      if (method === "PUT" && op === "file") {
        if (isExcludedPath(relPath)) return errorResponse(400, `excluded path: ${relPath}`);
        const precondition = requestPrecondition(req, q);
        const expectedFileId = requestExpectedFileId(req);
        const writeOptions = requestWriteOptions(req);
        const failed = await enforceFingerprintPrecondition(store, relPath, precondition);
        if (failed !== null) return failed;
        if (req.headers.get(STREAM_UPLOAD_HEADER) === "1") {
          const expectedHash = req.headers.get(EXPECTED_CONTENT_HASH_HEADER)?.trim().toLowerCase() ?? "";
          if (!/^[a-f0-9]{64}$/.test(expectedHash)) {
            return errorResponse(400, `${EXPECTED_CONTENT_HASH_HEADER} must be a 64-character content digest`);
          }
          const declaredLength = parseOptionalNonNegativeInteger(req.headers.get("content-length"), "content-length");
          if (declaredLength instanceof Response) return declaredLength;
          const stagedDir = await mkdtemp(join(tmpdir(), "chevalier-vfs-upload-"));
          const stagedPath = join(stagedDir, "payload");
          try {
            const staged = await open(stagedPath, "wx", 0o600);
            // Must be the same digest the storage layer computes (BLAKE3, see
            // pack::hex_hash). This verifies the client's declared hash, so a
            // mismatch in algorithm would fail every upload's integrity check.
            const hasher = new VfsContentHasher();
            let received = 0;
            try {
              const reader = req.body?.getReader();
              if (reader !== undefined) {
                for (;;) {
                  const { done, value } = await reader.read();
                  if (done) break;
                  if (value.byteLength === 0) continue;
                  hasher.update(Buffer.from(value.buffer, value.byteOffset, value.byteLength));
                  let offset = 0;
                  while (offset < value.byteLength) {
                    const { bytesWritten } = await staged.write(
                      value,
                      offset,
                      value.byteLength - offset,
                      null,
                    );
                    if (bytesWritten === 0) {
                      throw new Error(`streamed upload made no write progress for ${relPath}`);
                    }
                    offset += bytesWritten;
                  }
                  received += value.byteLength;
                }
              }
              await staged.sync();
            } finally {
              await staged.close();
            }
            if (declaredLength !== null && received !== declaredLength) {
              return errorResponse(400, `streamed upload length mismatch for ${relPath}`);
            }
            if (hasher.digest() !== expectedHash) {
              return errorResponse(409, `streamed upload hash mismatch for ${relPath}`);
            }
            const streamingStore = store as StreamingVfsStorage;
            const options = {
              ...preconditionOptions(precondition, expectedFileId),
              ...writeOptions,
            };
            const publication = await publications.transact(ownerId, async () => {
              const result =
                typeof streamingStore.writeFromFile === "function"
                  ? await streamingStore.writeFromFile(relPath, stagedPath, expectedHash, options)
                  : await store.write(relPath, await readFile(stagedPath), options);
              const value = result as {
                content_hash?: string;
                contentHash?: string;
                previous_hash?: string | null;
                previousHash?: string | null;
                changed?: boolean;
              };
              const affected = writeManyAffectedPaths(
                [relPath],
                [{
                  path: relPath,
                  content_hash: value.content_hash ?? value.contentHash,
                  previous_hash: value.previous_hash ?? value.previousHash ?? null,
                  changed: value.changed ?? true,
                }],
              );
              return {
                value: {
                  result: value,
                  entries: await snapshotPaths(store, affected),
                },
                mutated: true,
                paths: affected,
              };
            });
            const value = publication.value.result as {
              content_hash?: string;
              contentHash?: string;
              previous_hash?: string | null;
              previousHash?: string | null;
              changed?: boolean;
            };
            return withNamespaceRevision(json(200, {
              path: relPath,
              content_hash: value.content_hash ?? value.contentHash ?? expectedHash,
              previous_hash: value.previous_hash ?? value.previousHash ?? null,
              changed: value.changed ?? true,
              entries: publication.value.entries,
            }), publication.revision);
          } catch (error) {
            const conflict = conflictResponseFromStoreError(error, relPath);
            if (conflict !== null) return conflict;
            throw error;
          } finally {
            await rm(stagedDir, { recursive: true, force: true }).catch(() => undefined);
          }
        }
        const body = Buffer.from(await req.arrayBuffer());
        try {
          const publication = await publications.mutate(ownerId, () =>
            store.write(relPath, body, {
              ...preconditionOptions(precondition, expectedFileId),
              ...writeOptions,
            }),
          );
          const res = publication.value as {
            content_hash?: string;
            contentHash?: string;
            previous_hash?: string | null;
            changed?: boolean;
          };
          // The bound client ignores this body on the plain-write path and
          // recomputes its own result; return the real result for completeness.
          return withNamespaceRevision(json(200, {
            path: relPath,
            content_hash: res.content_hash ?? res.contentHash ?? null,
            previous_hash: res.previous_hash ?? null,
            changed: res.changed ?? true,
          }), publication.revision);
        } catch (e) {
          const failed = conflictResponseFromStoreError(e, relPath);
          if (failed !== null) return failed;
          throw e;
        }
      }

      if (method === "DELETE" && op === "file") {
        if (isExcludedPath(relPath)) return errorResponse(400, `excluded path: ${relPath}`);
        const precondition = requestPrecondition(req, q);
        const failed = await enforceFingerprintPrecondition(store, relPath, precondition);
        if (failed !== null) return failed;
        let previous: ReturnType<typeof toRemoteMetadata> | null = null;
        if (q.get("return_metadata") === "true") {
          const cur = await store.stat(relPath);
          previous = cur === null ? null : toRemoteMetadata(cur);
        }
        try {
          const publication = await publications.mutate(ownerId, () =>
            store.remove(relPath, preconditionOptions(precondition)),
          );
          return withNamespaceRevision(json(200, { previous }), publication.revision);
        } catch (e) {
          const failed = conflictResponseFromStoreError(e, relPath);
          if (failed !== null) return failed;
          throw e;
        }
      }

      if (method === "PUT" && op === "dir") {
        if (isExcludedPath(relPath)) return errorResponse(400, `excluded path: ${relPath}`);
        const writeOptions = requestWriteOptions(req);
        const publication = await publications.mutate(ownerId, () =>
          store.mkdir(
            relPath,
            Object.keys(writeOptions).length === 0 ? undefined : writeOptions,
          ),
        );
        return withNamespaceRevision(
          new Response(null, { status: 204 }),
          publication.revision,
        );
      }
      if (method === "PUT" && op === "symlink") {
        if (isExcludedPath(relPath)) return errorResponse(400, `excluded path: ${relPath}`);
        const target = q.get("target");
        if (target === null || target === "") return errorResponse(400, "symlink requires target");
        if (isExcludedPath(symlinkTargetPath(relPath, target))) {
          return errorResponse(400, `excluded symlink target: ${target}`);
        }
        try {
          const publication = await publications.mutate(ownerId, () =>
            store.createSymlink(relPath, target),
          );
          return withNamespaceRevision(
            new Response(null, { status: 204 }),
            publication.revision,
          );
        } catch (e) {
          if (isVfsBadRequestError(e)) return errorResponse(400, (e as Error).message);
          throw e;
        }
      }
      if (method === "DELETE" && op === "dir") {
        if (isExcludedPath(relPath)) return errorResponse(400, `excluded path: ${relPath}`);
        try {
          const publication = await publications.mutate(ownerId, () => store.rmdir(relPath));
          return withNamespaceRevision(
            new Response(null, { status: 204 }),
            publication.revision,
          );
        } catch (error) {
          if (!isVfsNotFoundError(error)) throw error;
          return new Response(null, { status: 204 });
        }
      }

      if (method === "POST" && op === "hard-link/v1") {
        const body = (await req.json()) as {
          source_path?: unknown;
          destination_path?: unknown;
        };
        const source = normalizePath(
          typeof body.source_path === "string" ? body.source_path : "",
        );
        const destination = normalizePath(
          typeof body.destination_path === "string" ? body.destination_path : "",
        );
        if (source === "" || destination === "") {
          return errorResponse(400, "hard-link requires source_path + destination_path");
        }
        if (isExcludedPath(source) || isExcludedPath(destination)) {
          return errorResponse(
            400,
            `excluded path: ${isExcludedPath(source) ? source : destination}`,
          );
        }
        try {
          const publication = await publications.mutate(ownerId, () =>
            store.createHardLink(source, destination),
          );
          return withNamespaceRevision(json(200, {
            source: toRemoteMetadata(publication.value.source),
            destination: toRemoteMetadata(publication.value.destination),
          }), publication.revision);
        } catch (error) {
          const conflict = conflictResponseFromStoreError(error, destination);
          if (conflict !== null) return conflict;
          if (isVfsBadRequestError(error)) {
            return errorResponse(400, (error as Error).message);
          }
          throw error;
        }
      }

      if (method === "POST" && op === "hard-link-alias/v1") {
        const body = (await req.json()) as {
          file_id?: unknown;
          excluding_path?: unknown;
        };
        if (typeof body.file_id !== "string" || body.file_id.trim() === "") {
          return errorResponse(400, "hard-link alias resolution requires file_id");
        }
        const fileId = body.file_id;
        const excludingPath = normalizePath(
          typeof body.excluding_path === "string" ? body.excluding_path : "",
        );
        if (isExcludedPath(excludingPath)) return errorResponse(400, `excluded path: ${excludingPath}`);
        // Validate the ANSWER, not the whole namespace.
        //
        // This ran under `optimisticRead`, whose activity epoch is OWNER-GLOBAL:
        // any write anywhere invalidated the attempt. Alias resolution runs on
        // the unlink path, and `pnpm install` hard-links thousands of files, so
        // "some unrelated path changed" is the steady state — the snapshot could
        // never converge, every attempt 409'd, and vmd spent its 30s metadata
        // budget on a signal that only ever meant "busy". Observed as
        // operation="unlink" operation_time_ms=30723.
        //
        // The blocking `read` is not the alternative either: it queues every
        // delete behind the writer backlog.
        //
        // What actually matters is narrow: does the path we are about to hand
        // back still name this file_id? That is answerable by re-statting the
        // candidate, costs one lookup, and is unaffected by writes elsewhere. A
        // candidate that raced away is retried a bounded number of times; the
        // endpoint then reports "no alias we can vouch for" rather than a 409 the
        // caller can only spin on.
        let alias: string | null = null;
        for (let attempt = 0; attempt < MAX_ALIAS_VALIDATION_ATTEMPTS; attempt += 1) {
          const candidate = await store.findHardLinkAlias(fileId, excludingPath);
          if (candidate === null) break;
          const confirmed = await store.stat(candidate).catch(() => null);
          if (confirmed !== null && confirmed.fileId === fileId) {
            alias = candidate;
            break;
          }
        }
        return withNamespaceRevision(
          json(200, { path: alias !== null && isExcludedPath(alias) ? null : alias }),
          await publications.currentRevision(ownerId),
        );
      }

      if (method === "POST" && op === "rename") {
        const from = normalizePath(q.get("from"));
        const to = normalizePath(q.get("to"));
        if (from === "" || to === "") return errorResponse(400, "rename requires from + to");
        if (isExcludedPath(from) || isExcludedPath(to)) {
          return errorResponse(400, `excluded path: ${isExcludedPath(from) ? from : to}`);
        }
        const publication = await publications.mutate(ownerId, async () => {
          const previous = q.get("return_metadata") === "true" ? await store.stat(from) : null;
          await store.rename(from, to);
          const current = q.get("return_metadata") === "true" ? await store.stat(to) : null;
          return { previous, current };
        });
        return withNamespaceRevision(json(200, {
          previous: publication.value.previous === null
            ? null
            : toRemoteMetadata(publication.value.previous),
          current: publication.value.current === null
            ? null
            : toRemoteMetadata(publication.value.current),
        }), publication.revision);
      }

      // ---- batch ops: loop the per-path primitives (matches the Rust trait's
      //      default impls; the bound TS client only uses per-path ops today) ----
      if (method === "POST" && op === "subtree-metadata") {
        const body = await requestJsonObject(req, "subtree-metadata");
        if (body instanceof Response) return body;
        const subtreePrefix = normalizePath(
          typeof body.prefix === "string" ? body.prefix : "",
        );
        if (isExcludedPath(subtreePrefix)) {
          return errorResponse(404, `not found: ${subtreePrefix}`);
        }
        const limit = parseOptionalNonNegativeInteger(
          typeof body.limit === "number" ? String(body.limit) : null,
          "limit",
        );
        if (limit instanceof Response) return limit;
        const maxHashBytes = parseOptionalNonNegativeInteger(
          typeof body.max_hash_bytes === "number"
            ? String(body.max_hash_bytes)
            : null,
          "max_hash_bytes",
        );
        if (maxHashBytes instanceof Response) return maxHashBytes;
        const snapshot = await publications.optimisticRead(ownerId, async () => {
          const entries: ReturnType<typeof toRemoteSubtreeMetadata>[] = [];
          const pending = [subtreePrefix];
          const statOptions =
            maxHashBytes === null ? undefined : { maxHashBytes };
          while (pending.length !== 0 && (limit === null || entries.length < limit)) {
            const directory = pending.pop() ?? "";
            let children: VfsMetadata[];
            try {
              children = await store.listDir(
                directory === "" ? "." : directory,
                statOptions,
              );
            } catch (error) {
              if (isVfsNotFoundError(error)) continue;
              throw error;
            }
            for (const child of children) {
              const name =
                child.path.split("/").filter((segment) => segment !== "").pop() ??
                child.path;
              const childPath =
                directory === "" ? name : posix.join(directory, name);
              if (name === "" || isExcludedPath(childPath)) continue;
              const kind = wireKind(child.kind);
              if (kind === "directory") {
                pending.push(childPath);
                continue;
              }
              if (kind !== "file" && kind !== "symlink") continue;
              entries.push(toRemoteSubtreeMetadata(child, childPath));
              if (limit !== null && entries.length >= limit) break;
            }
          }
          entries.sort((left, right) => left.path.localeCompare(right.path));
          return entries;
        });
        return withNamespaceRevision(
          json(200, { entries: snapshot.value }),
          snapshot.revision,
        );
      }

      if (method === "POST" && op === "prefetch-subtree") {
        const body = await requestJsonObject(req, "prefetch-subtree");
        if (body instanceof Response) return body;
        const subtreePrefix = normalizePath(
          typeof body.prefix === "string" ? body.prefix : "",
        );
        if (isExcludedPath(subtreePrefix)) {
          return errorResponse(404, `not found: ${subtreePrefix}`);
        }
        const maxEntries = parseOptionalNonNegativeInteger(
          typeof body.max_entries === "number" ? String(body.max_entries) : null,
          "max_entries",
        );
        if (maxEntries instanceof Response) return maxEntries;
        const maxPackBytes = parseOptionalNonNegativeInteger(
          typeof body.max_pack_bytes === "number"
            ? String(body.max_pack_bytes)
            : null,
          "max_pack_bytes",
        );
        if (maxPackBytes instanceof Response) return maxPackBytes;
        const streamingStore = store as StreamingVfsStorage;
        if (typeof streamingStore.prefetchSubtree !== "function") {
          return errorResponse(404, "prefetch-subtree is unavailable");
        }
        const snapshot = await publications.optimisticRead(ownerId, () =>
          streamingStore.prefetchSubtree!(subtreePrefix, {
            includeSmallFileBytes: body.include_small_file_bytes === true,
            ...(maxEntries === null ? {} : { maxEntries }),
            ...(maxPackBytes === null ? {} : { maxPackBytes }),
          }),
        );
        return withNamespaceRevision(
          json(200, {
            warmed_file_bytes: snapshot.value
              .filter((entry) => !isExcludedPath(entry.path))
              .map((entry) => ({ path: entry.path, body: [...entry.body] })),
          }),
          snapshot.revision,
        );
      }

      if (method === "POST" && op === "metadata-many") {
        const body = await requestJsonObject(req, "metadata-many");
        if (body instanceof Response) return body;
        const paths = normalizePathBatch(body.paths, "metadata-many");
        if (paths instanceof Response) return paths;
        const maxHashBytes = parseOptionalNonNegativeInteger(
          q.get("max_hash_bytes") ?? q.get("maxHashBytes"),
          "max_hash_bytes",
        );
        if (maxHashBytes instanceof Response) return maxHashBytes;
        // Optimistic, like the heavier recursive reads: a batch stat must not
        // queue behind the writer backlog. This route previously took the
        // blocking read and a single-path batch was measured at 9,524ms while
        // subtree-metadata -- which walks the whole tree -- returned in 394ms.
        // Safe only because a 409 retry signal is now transient for vmd reads.
        const snapshot = await publications.optimisticRead(ownerId, async () => {
          const entries: (ReturnType<typeof toRemoteMetadata> | null)[] = [];
          const statOptions = maxHashBytes === null ? undefined : { maxHashBytes };
          const concurrency = maxHashBytes === null ? 1 : 64;
          for (let offset = 0; offset < paths.length; offset += concurrency) {
            const batch = await Promise.all(
              paths.slice(offset, offset + concurrency).map(async (path) => {
                const md = isExcludedPath(path) ? null : await store.stat(path, statOptions);
                return md === null ? null : toRemoteMetadata(md);
              }),
            );
            entries.push(...batch);
          }
          return entries;
        });
        return withNamespaceRevision(
          json(200, { entries: snapshot.value }),
          snapshot.revision,
        );
      }

      if (method === "POST" && op === "read-many") {
        const body = await requestJsonObject(req, "read-many");
        if (body instanceof Response) return body;
        const paths = normalizePathBatch(body.paths, "read-many");
        if (paths instanceof Response) return paths;
        const snapshot = await publications.read(ownerId, async () => {
          const entries: (number[] | null)[] = [];
          for (const path of paths) {
            if (isExcludedPath(path)) {
              entries.push(null);
              continue;
            }
            try {
              const buf = await store.read(path);
              entries.push([...buf]);
            } catch (error) {
              if (isVfsNotFoundError(error)) entries.push(null);
              else throw error;
            }
          }
          return entries;
        });
        return withNamespaceRevision(
          json(200, { entries: snapshot.value }),
          snapshot.revision,
        );
      }

      if (method === "POST" && op === "write-many") {
        const body = await requestJsonObject(req, "write-many");
        if (body instanceof Response) return body;
        const writes = normalizeWriteManyItems(body.writes, isExcludedPath);
        if (writes instanceof Response) return writes;
        const streamingStore = store as StreamingVfsStorage;
        if (typeof streamingStore.writeMany === "function") {
          const compact =
            typeof streamingStore.writeManyBase64 === "function" &&
            writes.every((write) => write.body_base64 !== undefined);
          const normalizedWrites = writes.map((write) => {
            const precondition = writeItemPrecondition(write);
            const expectedFileId = writeItemExpectedFileId(write);
            const wirePrecondition = {
              ...(precondition.present ? { predicate: precondition.predicate } : {}),
              ...(expectedFileId === undefined
                ? {}
                : { expected_file_id: expectedFileId }),
            };
            const mode = ownValue(write, "mode");
            return {
              path: write.path,
              ...(compact
                ? { body_base64: write.body_base64! }
                : { body: decodedWriteBody(write) }),
              ...(typeof mode === "number" ? { mode } : {}),
              ...(Object.keys(wirePrecondition).length === 0
                ? {}
                : { precondition: wirePrecondition }),
            };
          });
          try {
            const publication = await publications.transact(ownerId, async () => {
              const results = compact
                ? await streamingStore.writeManyBase64!(
                    normalizedWrites as StreamingBase64Write[],
                  )
                : await streamingStore.writeMany!(
                    normalizedWrites as StreamingWrite[],
                  );
              const affected = writeManyAffectedPaths(
                normalizedWrites.map((write) => write.path),
                results,
              );
              return {
                value: {
                  results,
                  entries: await snapshotPaths(store, affected),
                },
                mutated: true,
                paths: affected,
              };
            });
            return withNamespaceRevision(json(200, {
              results: publication.value.results.map((result: StreamingWriteManyResult) => ({
                path: result.path,
                content_hash: result.content_hash ?? result.contentHash ?? "",
                previous_hash: result.previous_hash ?? result.previousHash ?? null,
                changed: result.changed,
              })),
              entries: publication.value.entries,
            }), publication.revision);
          } catch (error) {
            const conflict = conflictResponseFromStoreError(error, "write-many");
            if (conflict !== null) return conflict;
            throw error;
          }
        }
        const publication = await publications.transact(ownerId, async () => {
          // Atomic-ish: check all preconditions while publication is excluded,
          // then apply. Any mismatch returns without advancing the revision.
          for (const write of writes) {
            const failed = await enforceFingerprintPrecondition(
              store,
              write.path,
              writeItemPrecondition(write),
            );
            if (failed !== null) return { value: failed, mutated: false };
          }
          const results = [];
          for (const write of writes) {
            const p = write.path;
            const cur = await store.stat(p);
            const prev = cur?.contentHash ?? null;
            const precondition = writeItemPrecondition(write);
            const expectedFileId = writeItemExpectedFileId(write);
            let res: {
              content_hash?: string;
              contentHash?: string;
              previous_hash?: string | null;
              previousHash?: string | null;
              changed?: boolean;
            };
            try {
              res = (await store.write(
                p,
                bufferedWriteBody(write),
                preconditionOptions(precondition, expectedFileId),
              )) as typeof res;
            } catch (e) {
              const failed = conflictResponseFromStoreError(e, p);
              if (failed !== null) return { value: failed, mutated: false };
              throw e;
            }
            const hash = res.content_hash ?? res.contentHash ?? "";
            const previousHash = res.previous_hash ?? res.previousHash ?? prev;
            results.push({
              path: p,
              content_hash: hash,
              previous_hash: previousHash,
              changed: res.changed ?? previousHash !== hash,
            });
          }
          const affected = writeManyAffectedPaths(
            writes.map((write) => write.path),
            results,
          );
          return {
            value: json(200, {
              results,
              entries: await snapshotPaths(store, affected),
            }),
            mutated: true,
            paths: affected,
          };
        });
        if (!publication.value.ok) {
          return publication.value;
        }
        return withNamespaceRevision(publication.value, publication.revision);
      }

      return errorResponse(404, `unhandled route: ${method} ${op}`);
    } catch (e) {
      if (isVfsBadRequestError(e)) return errorResponse(400, (e as Error).message);
      if (e instanceof VfsSnapshotChangedError) {
        return errorResponse(409, e.message);
      }
      console.error(
        "[chevalier-vfs-gateway] unexpected request failure",
        e instanceof Error ? (e.stack ?? e.message) : String(e),
      );
      return errorResponse(500, `gateway server error: ${(e as Error).message}`);
    }
  };
}

// ---- helpers ---------------------------------------------------------------

/** A `Buffer` is a `Uint8Array` at runtime and is a valid response body, but the
 *  DOM lib types don't list it as `BodyInit`; hand back a plain `Uint8Array` view
 *  (zero-copy) so typing is happy without a copy. */
function asBody(buf: Buffer): BodyInit {
  // Zero-copy view; cast because this tsconfig's DOM lib omits Uint8Array from
  // BodyInit even though it is a valid body at runtime.
  return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength) as unknown as BodyInit;
}

function normalizePath(raw: string | null): string {
  if (raw === null) return "";
  const t = raw.trim().replace(/^\/+/, "").replace(/\/+$/, "");
  return t === "." ? "" : t;
}

function asciiCaseFold(value: string): string {
  return value.replace(/[A-Z]/g, (character) =>
    String.fromCharCode(character.charCodeAt(0) + 32),
  );
}

function isGitMetadataSegment(segment: string): boolean {
  return asciiCaseFold(segment) === ".git";
}

function isGitExcludedPath(path: string): boolean {
  return path
    .replace(/\\/g, "/")
    .split("/")
    .filter((part) => part !== "" && part !== ".")
    .some(isGitMetadataSegment);
}

function symlinkTargetPath(path: string, target: string): string {
  const normalizedPath = normalizePath(path).replace(/\\/g, "/");
  const normalizedTarget = target.replace(/\\/g, "/");
  if (posix.isAbsolute(normalizedTarget)) return normalizePath(normalizedTarget);
  return posix.normalize(posix.join(posix.dirname(normalizedPath), normalizedTarget));
}

async function requestJsonObject(
  request: Request,
  operation: string,
): Promise<Record<string, unknown> | Response> {
  let value: unknown;
  try {
    value = await request.json();
  } catch {
    return errorResponse(400, `${operation} requires a JSON object`);
  }
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return errorResponse(400, `${operation} requires a JSON object`);
  }
  return value as Record<string, unknown>;
}

function normalizePathBatch(value: unknown, operation: string): string[] | Response {
  if (!Array.isArray(value)) return errorResponse(400, `${operation} requires paths[]`);
  if (value.length > MAX_BATCH_ITEMS) {
    return errorResponse(400, `${operation} accepts at most ${MAX_BATCH_ITEMS} paths`);
  }
  const paths: string[] = [];
  for (const item of value) {
    if (typeof item !== "string") {
      return errorResponse(400, `${operation} paths must be strings`);
    }
    paths.push(normalizePath(item));
  }
  return paths;
}

function vfsErrorStatus(error: unknown): number | null {
  const value = error as { status?: unknown; statusCode?: unknown; code?: unknown; message?: unknown };
  if (typeof value?.status === "number") return value.status;
  if (typeof value?.statusCode === "number") return value.statusCode;
  if (typeof value?.code === "number") return value.code;
  if (typeof value?.message === "string") {
    const match = /status=(\d{3})/.exec(value.message);
    if (match !== null) return Number(match[1]);
  }
  return null;
}

function isVfsNotFoundError(error: unknown): boolean {
  const value = error as { code?: unknown; message?: unknown };
  return value?.code === "VFS_NOT_FOUND" || vfsErrorStatus(error) === 404;
}

function isVfsBadRequestError(error: unknown): boolean {
  const value = error as { code?: unknown };
  return value?.code === "VFS_BAD_REQUEST" || vfsErrorStatus(error) === 400;
}

type VfsCasPredicate =
  | { kind: "absent" }
  | { kind: "content_fingerprint"; fingerprint: string };

type FingerprintPrecondition =
  | { present: false }
  | { present: true; predicate: VfsCasPredicate };

function normalizeFingerprint(raw: string | null | undefined): string | null {
  if (raw === null || raw === undefined) return null;
  let next = raw.trim();
  if (next.startsWith("W/")) next = next.slice(2).trim();
  if (
    (next.startsWith('"') && next.endsWith('"')) ||
    (next.startsWith("'") && next.endsWith("'"))
  ) {
    next = next.slice(1, -1).trim();
  }
  if (next.startsWith("sha256:")) next = next.slice("sha256:".length);
  if (next === "" || next.toLowerCase() === "null") return null;
  return next;
}

function preconditionFromRaw(raw: string | null): FingerprintPrecondition {
  const fingerprint = normalizeFingerprint(raw);
  return {
    present: true,
    predicate:
      fingerprint === null
        ? { kind: "absent" }
        : { kind: "content_fingerprint", fingerprint },
  };
}

function queryIfMatch(query: URLSearchParams): string | null {
  return query.get("ifMatch") ?? query.get("if_match");
}

function requestPrecondition(req: Request, query: URLSearchParams): FingerprintPrecondition {
  const kind = req.headers.get(PRECONDITION_KIND_HEADER);
  const raw =
    req.headers.get(PRECONDITION_FINGERPRINT_HEADER) ??
    req.headers.get(IF_MATCH_HEADER) ??
    queryIfMatch(query);
  if (kind === "absent") {
    if (raw !== null) throw badPrecondition("absent precondition cannot include a fingerprint");
    return { present: true, predicate: { kind: "absent" } };
  }
  if (kind === "content_fingerprint") {
    if (raw === null) throw badPrecondition("content_fingerprint precondition requires a fingerprint");
    const normalized = normalizeFingerprint(raw);
    if (normalized === null) {
      throw badPrecondition("content_fingerprint precondition requires a non-empty fingerprint");
    }
    return {
      present: true,
      predicate: { kind: "content_fingerprint", fingerprint: normalized },
    };
  }
  if (kind !== null) throw badPrecondition(`unsupported precondition kind: ${kind}`);
  if (raw !== null) return preconditionFromRaw(raw);
  return { present: false };
}

function badPrecondition(message: string): Error {
  return Object.assign(new Error(message), { code: "VFS_BAD_REQUEST", status: 400 });
}

function parseExpectedFileId(raw: unknown, source: string): string | undefined {
  if (raw === undefined || raw === null) return undefined;
  if (typeof raw !== "string" || raw.length === 0) {
    throw Object.assign(new Error(`${source} must be a non-empty string`), {
      code: "VFS_BAD_REQUEST",
      status: 400,
    });
  }
  return raw;
}

function requestExpectedFileId(req: Request): string | undefined {
  return parseExpectedFileId(
    req.headers.get(PRECONDITION_FILE_ID_HEADER),
    PRECONDITION_FILE_ID_HEADER,
  );
}

type WriteOptions = { executable?: boolean; mode?: number };

function parseMode(raw: unknown, source: string): number | undefined {
  if (raw === undefined || raw === null) return undefined;
  const value =
    typeof raw === "string" && /^[0-9]+$/.test(raw)
      ? Number(raw)
      : typeof raw === "number"
        ? raw
        : Number.NaN;
  if (!Number.isSafeInteger(value) || value < 0 || value > 0o7777) {
    throw Object.assign(new Error(`${source} must be an integer between 0 and 4095`), {
      code: "VFS_BAD_REQUEST",
      status: 400,
    });
  }
  return value;
}

function requestWriteOptions(req: Request): WriteOptions {
  const rawMode = req.headers.get(MODE_HEADER);
  const mode = parseMode(rawMode, MODE_HEADER);
  const rawExecutable = req.headers.get(EXECUTABLE_HEADER);
  let executable: boolean | undefined;
  if (rawExecutable === "true") executable = true;
  else if (rawExecutable === "false") executable = false;
  else if (rawExecutable !== null) {
    throw Object.assign(new Error(`${EXECUTABLE_HEADER} must be true or false`), {
      code: "VFS_BAD_REQUEST",
      status: 400,
    });
  }
  if (mode !== undefined) return { executable: (mode & 0o111) !== 0, mode };
  return executable === undefined ? {} : { executable };
}

function preconditionOptions(
  precondition: FingerprintPrecondition,
  expectedFileId?: string,
): { ifMatch?: string | null; expectedFileId?: string } | undefined {
  if (!precondition.present && expectedFileId === undefined) return undefined;
  return {
    ...(precondition.present
      ? {
          ifMatch:
            precondition.predicate.kind === "absent"
              ? null
              : precondition.predicate.fingerprint,
        }
      : {}),
    ...(expectedFileId === undefined ? {} : { expectedFileId }),
  };
}

type WriteManyRequestItem = {
  path: string;
  body?: number[];
  body_base64?: string;
  ifMatch?: string | null;
  if_match?: string | null;
  precondition?: {
    predicate?: VfsCasPredicate;
    fingerprint?: string | null;
    ifMatch?: string | null;
    if_match?: string | null;
    expected_file_id?: string | null;
  };
};

function isCanonicalBase64(value: unknown): value is string {
  if (typeof value !== "string" || value.length % 4 !== 0) return false;
  if (!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)) {
    return false;
  }
  const decoded = Buffer.from(value, "base64");
  return decoded.toString("base64") === value;
}

function decodedWriteBody(write: WriteManyRequestItem): number[] {
  if (write.body !== undefined) return write.body;
  return Array.from(Buffer.from(write.body_base64!, "base64"));
}

function bufferedWriteBody(write: WriteManyRequestItem): Buffer {
  return write.body !== undefined ? Buffer.from(write.body) : Buffer.from(write.body_base64!, "base64");
}

function normalizeWriteManyItems(
  value: unknown,
  isExcludedPath: (path: string) => boolean,
): WriteManyRequestItem[] | Response {
  if (!Array.isArray(value)) return errorResponse(400, "write-many requires writes[]");
  if (value.length > MAX_BATCH_ITEMS) {
    return errorResponse(400, `write-many accepts at most ${MAX_BATCH_ITEMS} writes`);
  }
  const writes: WriteManyRequestItem[] = [];
  for (const item of value) {
    if (typeof item !== "object" || item === null || Array.isArray(item)) {
      return errorResponse(400, "invalid write-many item");
    }
    const write = item as Record<string, unknown>;
    if (typeof write.path !== "string") return errorResponse(400, "write-many path must be a string");
    const path = normalizePath(write.path);
    if (path === "" || isExcludedPath(path)) {
      return errorResponse(400, `invalid write-many path: ${path}`);
    }
    if (write.body !== undefined && write.body_base64 !== undefined) {
      return errorResponse(400, "write-many accepts exactly one of body or body_base64");
    }
    const legacyBody =
      Array.isArray(write.body) &&
      write.body.every((byte) => Number.isSafeInteger(byte) && byte >= 0 && byte <= 255)
        ? [...write.body]
        : null;
    const encodedBody = isCanonicalBase64(write.body_base64) ? write.body_base64 : null;
    if (legacyBody === null && encodedBody === null) {
      return errorResponse(400, "write-many body must be a byte array or canonical base64");
    }
    // Optional POSIX mode, applied only when this write CREATES the path.
    // Without it a create cannot be folded into its write: the namespace
    // mutation is the only carrier of mode today, so dropping that round trip
    // would silently strip the executable bit off every file a build produces.
    const modeValue = ownValue(write, "mode");
    if (
      modeValue !== undefined &&
      modeValue !== null &&
      !(Number.isSafeInteger(modeValue) && (modeValue as number) >= 0 && (modeValue as number) <= 0o7777)
    ) {
      return errorResponse(400, "write-many mode must be an integer in 0..0o7777");
    }
    const preconditionValue = ownValue(write, "precondition");
    if (
      preconditionValue !== undefined &&
      preconditionValue !== null &&
      (typeof preconditionValue !== "object" ||
        Array.isArray(preconditionValue))
    ) {
      return errorResponse(400, "write-many precondition must be an object or null");
    }
    try {
      writeItemPrecondition(write as WriteManyRequestItem);
      writeItemExpectedFileId(write as WriteManyRequestItem);
    } catch (error) {
      return errorResponse(400, error instanceof Error ? error.message : String(error));
    }
    writes.push({
      ...(write as WriteManyRequestItem),
      path,
      ...(legacyBody === null ? { body_base64: encodedBody! } : { body: legacyBody }),
    });
  }
  return writes;
}

type NamespaceMutation =
  | { kind: "create_file"; path: string; mode?: number }
  | { kind: "create_directory"; path: string; mode?: number }
  | { kind: "set_mode"; path: string; mode: number }
  | { kind: "create_symlink"; path: string; target: string }
  | { kind: "create_hard_link"; source_path: string; destination_path: string }
  | {
      kind: "delete_file";
      path: string;
      precondition?: {
        predicate?: VfsCasPredicate;
        expected_file_id?: string;
      };
    }
  | { kind: "remove_directory"; path: string }
  | { kind: "rename"; from: string; to: string };

type PublicationSnapshotEntry = {
  path: string;
  metadata: ReturnType<typeof toRemoteMetadata> | null;
};

function mutationPaths(mutation: NamespaceMutation): string[] {
  switch (mutation.kind) {
    case "create_hard_link":
      return [mutation.source_path, mutation.destination_path];
    case "rename":
      return [mutation.from, mutation.to];
    default:
      return [mutation.path];
  }
}

function immediateParent(path: string): string {
  const parent = posix.dirname(normalizePath(path));
  return parent === "." ? "" : normalizePath(parent);
}

/**
 * The paths a `write-many` affected: the files it wrote, plus the parent of
 * every write that CREATED its path.
 *
 * A write that created a file changed its parent directory too — the parent may
 * not have existed at all a moment ago — so it is reported and snapshotted
 * exactly as a `create_file` namespace mutation would be. Without it, a watcher
 * that had cached the parent's absence keeps that negative entry: an unrelated
 * publication RETAGS a surviving negative rather than dropping it (which is what
 * makes sibling reads cheap), so a parent nobody names is never re-read and the
 * directory stays invisible to that mount. This is exactly what a mount's
 * create/write fold produces — the write carries the creation, so the write is
 * the only publication there is.
 *
 * An OVERWRITE still names only the file. That is what keeps a write from
 * evicting every cached sibling in its directory, and it is untouched here: an
 * overwrite's parent did not change. `previousHash === null` is the
 * discriminator the store already reports.
 */
function writeManyAffectedPaths(
  writtenPaths: readonly string[],
  results: readonly StreamingWriteManyResult[],
): string[] {
  const affected = writtenPaths.map(normalizePath);
  const seen = new Set(affected);
  for (const result of results) {
    const previousHash = result.previous_hash ?? result.previousHash ?? null;
    if (previousHash !== null) continue;
    const parent = immediateParent(result.path);
    if (parent === "" || seen.has(parent)) continue;
    seen.add(parent);
    affected.push(parent);
  }
  return affected;
}

async function snapshotPaths(
  store: VfsStorage,
  requestedPaths: readonly string[],
): Promise<PublicationSnapshotEntry[]> {
  const paths = [...new Set(requestedPaths.map(normalizePath))];
  if (paths.length === 0) return [];
  const stat = (store as Partial<VfsStorage>).stat;
  let metadata: Array<VfsMetadata | undefined | null>;
  if (typeof store.metadataMany === "function") {
    metadata = await store.metadataMany(paths);
  } else if (typeof stat === "function") {
    metadata = await Promise.all(
      paths.map(async (path) => {
        try {
          return await stat.call(store, path);
        } catch (error) {
          if (isVfsNotFoundError(error)) return null;
          throw error;
        }
      }),
    );
  } else {
    return [];
  }
  if (metadata.length !== paths.length) {
    throw new Error(
      `publication snapshot returned ${metadata.length} entries for ${paths.length} paths`,
    );
  }
  return paths.map((path, index) => ({
    path,
    metadata: metadata[index] == null ? null : toRemoteMetadata(metadata[index]),
  }));
}

/** Every path a namespace batch touches — each mutation's own paths plus their
 *  immediate parents, whose listings the batch also invalidates. Feeds both the
 *  publication snapshot and the publication's recorded affected set, so a
 *  watcher revokes exactly what the snapshot re-states. */
function mutationSnapshotPaths(mutations: readonly NamespaceMutation[]): string[] {
  const paths: string[] = [];
  for (const mutation of mutations) {
    for (const path of mutationPaths(mutation)) {
      paths.push(path, immediateParent(path));
    }
  }
  return paths;
}

/**
 * The prefixes a namespace batch superseded WHOLESALE — every path a watcher
 * must drop everything beneath, not merely the path itself.
 *
 * Only `remove_directory` and `rename` qualify: the first removes a whole tree,
 * the second moves one (and can land on top of another). Every other mutation
 * supersedes exactly its own path plus its parent's listing, both of which
 * `mutationSnapshotPaths` already names point-scoped. Reporting a create or a
 * delete as a prefix is what makes a sibling's cached metadata collateral
 * damage. Mirrors `namespace_superseded_subtrees` in the Rust gateway.
 */
function mutationSupersededSubtrees(
  mutations: readonly NamespaceMutation[],
): string[] {
  const prefixes: string[] = [];
  for (const mutation of mutations) {
    if (mutation.kind !== "remove_directory" && mutation.kind !== "rename") {
      continue;
    }
    prefixes.push(...mutationPaths(mutation));
  }
  return prefixes;
}

function normalizeNamespaceOperationIds(value: unknown): string[] | Response {
  //  `operation_ids` is OPTIONAL. It exists so a client that retries a batch can
  //  have the retry recognized as the same operation; a client that does not
  //  supply them simply does not get that idempotency, which is exactly how this
  //  endpoint behaved before the field existed. Rejecting a writer for omitting
  //  it would break every client that predates the field — including the sync
  //  rail — for a property only the caller can benefit from.
  if (value === undefined || value === null) {
    return [];
  }
  if (!Array.isArray(value)) {
    return errorResponse(400, "namespace-many operation_ids must be an array");
  }
  if (value.length > MAX_BATCH_ITEMS) {
    return errorResponse(
      400,
      `namespace-many accepts at most ${MAX_BATCH_ITEMS} operation_ids`,
    );
  }
  const ids: string[] = [];
  const seen = new Set<string>();
  for (const id of value) {
    if (typeof id !== "string" || id.trim() === "") {
      return errorResponse(400, "namespace operation_id must be a non-empty string");
    }
    if (seen.has(id)) {
      return errorResponse(400, `duplicate namespace operation_id: ${id}`);
    }
    seen.add(id);
    ids.push(id);
  }
  return ids;
}

function normalizeNamespaceMutations(
  value: unknown,
  isExcludedPath: (path: string) => boolean = isGitExcludedPath,
): NamespaceMutation[] | Response {
  if (!Array.isArray(value)) return errorResponse(400, "namespace-many requires mutations[]");
  if (value.length > MAX_BATCH_ITEMS) {
    return errorResponse(400, `namespace-many accepts at most ${MAX_BATCH_ITEMS} mutations`);
  }
  const out: NamespaceMutation[] = [];
  for (const item of value) {
    if (typeof item !== "object" || item === null || typeof (item as { kind?: unknown }).kind !== "string") {
      return errorResponse(400, "invalid namespace mutation");
    }
    const mutation = item as Record<string, unknown>;
    const kind = mutation.kind;
    if (kind === "create_hard_link") {
      const sourcePath = normalizePath(
        typeof mutation.source_path === "string" ? mutation.source_path : null,
      );
      const destinationPath = normalizePath(
        typeof mutation.destination_path === "string"
          ? mutation.destination_path
          : null,
      );
      if (sourcePath === "" || destinationPath === "") {
        return errorResponse(
          400,
          "create_hard_link requires source_path + destination_path",
        );
      }
      if (isExcludedPath(sourcePath) || isExcludedPath(destinationPath)) {
        return errorResponse(400, "excluded hard-link path");
      }
      out.push({
        kind,
        source_path: sourcePath,
        destination_path: destinationPath,
      });
      continue;
    }
    if (kind === "rename") {
      const from = normalizePath(typeof mutation.from === "string" ? mutation.from : null);
      const to = normalizePath(typeof mutation.to === "string" ? mutation.to : null);
      if (from === "" || to === "") return errorResponse(400, "rename requires from + to");
      if (isExcludedPath(from) || isExcludedPath(to)) return errorResponse(400, "excluded rename path");
      out.push({ kind, from, to });
      continue;
    }
    const path = normalizePath(typeof mutation.path === "string" ? mutation.path : null);
    if (path === "" || isExcludedPath(path)) return errorResponse(400, `invalid namespace path: ${path}`);
    if (kind === "delete_file") {
      let precondition: FingerprintPrecondition;
      let expectedFileId: string | undefined;
      try {
        precondition = writeItemPrecondition(mutation as WriteManyRequestItem);
        expectedFileId = writeItemExpectedFileId(mutation as WriteManyRequestItem);
      } catch (error) {
        return errorResponse(400, error instanceof Error ? error.message : String(error));
      }
      const wirePrecondition = {
        ...(precondition.present ? { predicate: precondition.predicate } : {}),
        ...(expectedFileId === undefined
          ? {}
          : { expected_file_id: expectedFileId }),
      };
      out.push({
        kind,
        path,
        ...(Object.keys(wirePrecondition).length > 0
          ? { precondition: wirePrecondition }
          : {}),
      });
      continue;
    }
    if (kind === "create_file" || kind === "create_directory") {
      let mode: number | undefined;
      try {
        mode = parseMode(mutation.mode, `${kind} mode`);
      } catch (error) {
        return errorResponse(400, error instanceof Error ? error.message : String(error));
      }
      out.push({ kind, path, ...(mode === undefined ? {} : { mode }) });
      continue;
    }
    if (kind === "set_mode") {
      let mode: number | undefined;
      try {
        mode = parseMode(mutation.mode, "set_mode mode");
      } catch (error) {
        return errorResponse(400, error instanceof Error ? error.message : String(error));
      }
      if (mode === undefined) return errorResponse(400, "set_mode requires mode");
      out.push({ kind, path, mode });
      continue;
    }
    if (kind === "remove_directory") {
      out.push({ kind, path });
      continue;
    }
    if (kind === "create_symlink") {
      if (typeof mutation.target !== "string" || mutation.target === "") {
        return errorResponse(400, "create_symlink requires target");
      }
      if (isExcludedPath(symlinkTargetPath(path, mutation.target))) {
        return errorResponse(400, "excluded symlink target");
      }
      out.push({ kind, path, target: mutation.target });
      continue;
    }
    return errorResponse(400, `unsupported namespace mutation: ${String(kind)}`);
  }
  return out;
}

function ownValue<T extends object, K extends PropertyKey>(obj: T | null | undefined, key: K): unknown {
  if (obj == null || !Object.prototype.hasOwnProperty.call(obj, key)) return undefined;
  return (obj as Record<K, unknown>)[key];
}

function writeItemPrecondition(write: WriteManyRequestItem): FingerprintPrecondition {
  const predicate = ownValue(write.precondition, "predicate");
  if (predicate !== undefined) {
    if (typeof predicate !== "object" || predicate === null || Array.isArray(predicate)) {
      throw new Error("invalid write precondition: predicate must be an object");
    }
    const kind = ownValue(predicate, "kind");
    if (kind === "absent") {
      return { present: true, predicate: { kind: "absent" } };
    }
    if (kind === "content_fingerprint") {
      const fingerprint = ownValue(predicate, "fingerprint");
      if (typeof fingerprint !== "string" || fingerprint.length === 0) {
        throw new Error(
          "invalid write precondition: content_fingerprint requires a non-empty fingerprint",
        );
      }
      return {
        present: true,
        predicate: { kind: "content_fingerprint", fingerprint },
      };
    }
    throw new Error(`invalid write precondition kind: ${String(kind)}`);
  }
  let raw = ownValue(write.precondition, "fingerprint");
  if (raw === undefined) raw = ownValue(write.precondition, "ifMatch");
  if (raw === undefined) raw = ownValue(write.precondition, "if_match");
  if (raw === undefined) raw = ownValue(write, "ifMatch");
  if (raw === undefined) raw = ownValue(write, "if_match");
  if (raw === undefined) return { present: false };
  if (raw !== null && typeof raw !== "string") {
    throw new Error("invalid write precondition: ifMatch/fingerprint must be a string or null");
  }
  return preconditionFromRaw(raw);
}

function writeItemExpectedFileId(write: WriteManyRequestItem): string | undefined {
  return parseExpectedFileId(
    ownValue(write.precondition, "expected_file_id"),
    "invalid write precondition: expected_file_id",
  );
}

function conflictResponseFromStoreError(error: unknown, path: string): Response | null {
  const message = error instanceof Error ? error.message : String(error);
  if (
    message.includes("VFS_CONFLICT") ||
    message.includes("status=409") ||
    /\bconflict:/i.test(message)
  ) {
    return errorResponse(409, `precondition failed for ${path}`);
  }
  return null;
}

function parseOptionalNonNegativeInteger(
  raw: string | null,
  name: string,
): number | null | Response {
  if (raw === null || raw.trim() === "") return null;
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < 0) {
    return errorResponse(400, `${name} must be a non-negative integer`);
  }
  return value;
}

const WATCH_TIMEOUT_MIN_MS = 1_000;
const WATCH_TIMEOUT_MAX_MS = 30_000;
const WATCH_TIMEOUT_DEFAULT_MS = 25_000;

/** `since` absent/invalid -> 0 (so the fast path answers with the current
 *  revision). Decimal-only, matching the Rust gateway's `u64` parse. */
function parseWatchSince(raw: string | null): number {
  if (raw === null) return 0;
  const text = raw.trim();
  if (!/^\d+$/.test(text)) return 0;
  const value = Number(text);
  return Number.isSafeInteger(value) ? value : 0;
}

/** `timeout_ms` clamps to [1000, 30000]; absent or non-integer -> 25000. */
function parseWatchTimeout(raw: string | null): number {
  if (raw === null) return WATCH_TIMEOUT_DEFAULT_MS;
  const text = raw.trim();
  if (!/^-?\d+$/.test(text)) return WATCH_TIMEOUT_DEFAULT_MS;
  const value = Number(text);
  if (!Number.isFinite(value)) return WATCH_TIMEOUT_DEFAULT_MS;
  return Math.min(WATCH_TIMEOUT_MAX_MS, Math.max(WATCH_TIMEOUT_MIN_MS, value));
}

/** Trimmed, non-empty `watcher_id`, or "" for an anonymous (unregistered)
 *  watcher that is notified but never gates a publication. */
function parseWatcherId(raw: string | null): string {
  return raw === null ? "" : raw.trim();
}

const PUBLICATION_ACK_TIMEOUT_DEFAULT_MS = 25;

/** Hard cap (ms) a publication waits for watcher acks before failing open.
 *  Overridable via `CHEVALIER_VFS_PUBLICATION_ACK_TIMEOUT_MS`; default 25. */
function publicationAckTimeoutFromEnv(): number {
  const raw = process.env.CHEVALIER_VFS_PUBLICATION_ACK_TIMEOUT_MS;
  if (raw === undefined) return PUBLICATION_ACK_TIMEOUT_DEFAULT_MS;
  const text = raw.trim();
  if (!/^\d+$/.test(text)) return PUBLICATION_ACK_TIMEOUT_DEFAULT_MS;
  const value = Number(text);
  return Number.isSafeInteger(value) ? value : PUBLICATION_ACK_TIMEOUT_DEFAULT_MS;
}

async function enforceFingerprintPrecondition(
  store: VfsStorage,
  path: string,
  precondition: FingerprintPrecondition,
): Promise<Response | null> {
  if (!precondition.present) return null;
  const cur = await store.stat(path);
  const curHash = mutationFingerprint(cur);
  if (
    (precondition.predicate.kind === "absent" && cur === null) ||
    (precondition.predicate.kind === "content_fingerprint" &&
      precondition.predicate.fingerprint === curHash)
  ) {
    return null;
  }
  // CAS mismatch -> 409 Conflict; the file is NOT touched (no clobber).
  return errorResponse(409, `precondition failed for ${path}`);
}

function mutationFingerprint(metadata: VfsMetadata | null): string | null {
  if (metadata === null) return null;
  if (metadata.kind.toLowerCase().startsWith("sym")) {
    return typeof metadata.linkTarget === "string" && metadata.linkTarget !== ""
      ? `symlink:${createHash("sha256").update(metadata.linkTarget).digest("hex")}`
      : null;
  }
  return metadata.contentHash ?? null;
}

/** `VfsStorage` metadata `kind` is PascalCase; the wire uses lowercase kinds. */
function wireKind(kind: string): "file" | "directory" | "symlink" | "special" {
  const lower = kind.toLowerCase();
  if (lower.startsWith("dir")) return "directory";
  if (lower.startsWith("sym")) return "symlink";
  if (lower.startsWith("spec")) return "special";
  return "file";
}

function toRemoteMetadata(md: VfsMetadata) {
  const mode = md.mode ?? null;
  return {
    kind: wireKind(md.kind),
    size_bytes: Number(md.sizeBytes),
    file_id: md.fileId ?? null,
    link_count: Number(md.linkCount ?? 1n),
    mode,
    executable: mode === null ? md.executable ?? false : (mode & 0o111) !== 0,
    link_target: md.linkTarget ?? null,
    content_hash: md.contentHash ?? null,
    updated_at: md.updatedAt ?? null,
  };
}

function toRemoteDirEntry(md: VfsMetadata) {
  const name = md.path.split("/").filter((s) => s !== "").pop() ?? md.path;
  const mode = md.mode ?? null;
  return {
    name,
    kind: wireKind(md.kind),
    size_bytes: Number(md.sizeBytes),
    file_id: md.fileId ?? null,
    link_count: Number(md.linkCount ?? 1n),
    mode,
    executable: mode === null ? md.executable ?? false : (mode & 0o111) !== 0,
    link_target: md.linkTarget ?? null,
    content_hash: md.contentHash ?? null,
    updated_at: md.updatedAt ?? null,
  };
}

function toRemoteSubtreeMetadata(md: VfsMetadata, path: string) {
  const metadata = toRemoteMetadata(md);
  const objectState = md.objectState;
  return {
    path,
    ...metadata,
    token_count: md.tokenCount ?? null,
    version: md.version ?? null,
    object_state:
      objectState === undefined
        ? null
        : {
            size_bytes: Number(objectState.sizeBytes),
            pack_key: objectState.packKey,
            pack_slot_offset: Number(objectState.packSlotOffset),
            pack_slot_length: Number(objectState.packSlotLength),
            pack_slot_compression: objectState.packSlotCompression,
          },
  };
}

/** Cheap file-identity fingerprint for pinning ranged reads. Epoch millis is
 *  the canonical form on both sides — the Rust FUSE client mirrors this in
 *  `range_fingerprint` (sandbox/vmd/src/fuse/fs.rs); keep them identical. */
function rangeFingerprint(metadata: VfsMetadata): string {
  const raw = metadata.updatedAt ?? null;
  const millis = raw === null ? -1 : Date.parse(raw);
  return `${Number(metadata.sizeBytes)}:${Number.isNaN(millis) ? -1 : millis}`;
}

function staleRangeResponse(path: string, currentFingerprint: string): Response {
  return new Response(`range fingerprint mismatch for ${path}`, {
    status: 412,
    headers: {
      "content-type": "text/plain",
      [RANGE_FINGERPRINT_HEADER]: currentFingerprint,
    },
  });
}

function parseRange(header: string | null, len: number): { start: number; end: number } | null {
  if (header === null) return null;
  const m = /^bytes=(\d+)-(\d*)$/.exec(header.trim());
  if (m === null) return null;
  const start = Number(m[1]);
  const end = m[2] === "" ? len - 1 : Number(m[2]);
  if (Number.isNaN(start) || Number.isNaN(end) || start > end || start >= len) return null;
  return { start, end: Math.min(end, len - 1) };
}

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function withNamespaceRevision(response: Response, revision: number): Response {
  response.headers.set(NAMESPACE_REVISION_HEADER, String(revision));
  return response;
}

function errorResponse(status: number, message: string): Response {
  return new Response(message, { status, headers: { "content-type": "text/plain" } });
}

function randomToken(): string {
  // Rust FUSE clients deserialize owner_token as a UUID.
  return randomUUID();
}
