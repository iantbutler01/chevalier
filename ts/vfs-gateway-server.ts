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
      state = {
        revision: Date.now() * 1_000,
        activityEpoch: 0,
        activeReaders: 0,
        activeWriter: false,
        queue: [],
        pendingWatchers: [],
        watcherAcks: new Map(),
        pendingAckWaiters: [],
        lastAckWarnAt: 0,
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

  async mutate<T>(
    ownerId: string,
    mutate: () => Promise<T>,
  ): Promise<{ value: T; revision: number }> {
    return this.transact(ownerId, async () => ({ value: await mutate(), mutated: true }));
  }

  async transact<T>(
    ownerId: string,
    transaction: () => Promise<{ value: T; mutated: boolean }>,
  ): Promise<{ value: T; revision: number }> {
    const state = this.#state(ownerId);
    const release = await this.#acquire(state, "write");
    let outcome!: { value: T; revision: number; mutated: boolean };
    try {
      const { value, mutated } = await transaction();
      if (mutated) {
        state.revision = Math.max(state.revision + 1, Date.now() * 1_000);
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
  ): Promise<number> {
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
      return Promise.resolve(state.revision);
    }
    return new Promise<number>((resolve) => {
      let settled = false;
      const watcher: VfsPendingWatcher = {
        since,
        settle: (revision: number) => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          const index = state.pendingWatchers.indexOf(watcher);
          if (index >= 0) state.pendingWatchers.splice(index, 1);
          resolve(revision);
        },
      };
      const timer = setTimeout(() => watcher.settle(state.revision), timeoutMs);
      state.pendingWatchers.push(watcher);
    });
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
  writeMany?: (writes: Array<{
    path: string;
    body: number[];
    precondition?: { predicate?: VfsCasPredicate; expected_file_id?: string };
  }>) => Promise<StreamingWriteManyResult[]>;
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
   *  revision before proceeding fail-open. Default 150, overridable via
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
        const revision = await publications.watch(ownerId, since, timeoutMs, watcherId);
        if (revision > since) {
          return withNamespaceRevision(json(200, { revision }), revision);
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
        if (operationIds.length !== mutations.length) {
          return errorResponse(
            400,
            "namespace-many requires one operation_id per mutation",
          );
        }
        try {
          const publication = await publications.mutate(ownerId, async () => {
            await store.applyNamespaceBatch(mutations);
            return {
              entries: await snapshotMutationPaths(store, mutations),
            };
          });
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
            return errorResponse(400, `${EXPECTED_CONTENT_HASH_HEADER} must be a SHA-256 hex digest`);
          }
          const declaredLength = parseOptionalNonNegativeInteger(req.headers.get("content-length"), "content-length");
          if (declaredLength instanceof Response) return declaredLength;
          const stagedDir = await mkdtemp(join(tmpdir(), "chevalier-vfs-upload-"));
          const stagedPath = join(stagedDir, "payload");
          try {
            const staged = await open(stagedPath, "wx", 0o600);
            const hasher = createHash("sha256");
            let received = 0;
            try {
              const reader = req.body?.getReader();
              if (reader !== undefined) {
                for (;;) {
                  const { done, value } = await reader.read();
                  if (done) break;
                  if (value.byteLength === 0) continue;
                  hasher.update(value);
                  await staged.write(value);
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
            if (hasher.digest("hex") !== expectedHash) {
              return errorResponse(409, `streamed upload hash mismatch for ${relPath}`);
            }
            const streamingStore = store as StreamingVfsStorage;
            const options = {
              ...preconditionOptions(precondition, expectedFileId),
              ...writeOptions,
            };
            const publication = await publications.mutate(ownerId, async () =>
              typeof streamingStore.writeFromFile === "function"
                ? streamingStore.writeFromFile(relPath, stagedPath, expectedHash, options)
                : store.write(relPath, await readFile(stagedPath), options),
            );
            const value = publication.value as {
              content_hash?: string;
              contentHash?: string;
              previous_hash?: string | null;
              changed?: boolean;
            };
            return withNamespaceRevision(json(200, {
              path: relPath,
              content_hash: value.content_hash ?? value.contentHash ?? expectedHash,
              previous_hash: value.previous_hash ?? null,
              changed: value.changed ?? true,
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
        const snapshot = await publications.read(ownerId, () =>
          store.findHardLinkAlias(fileId, excludingPath),
        );
        return withNamespaceRevision(
          json(200, {
            path:
              snapshot.value !== null && isExcludedPath(snapshot.value)
                ? null
                : snapshot.value,
          }),
          snapshot.revision,
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
        const snapshot = await publications.read(ownerId, async () => {
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
          const writeMany = streamingStore.writeMany.bind(streamingStore);
          const normalizedWrites = writes.map((write) => {
            const precondition = writeItemPrecondition(write);
            const expectedFileId = writeItemExpectedFileId(write);
            const wirePrecondition = {
              ...(precondition.present ? { predicate: precondition.predicate } : {}),
              ...(expectedFileId === undefined
                ? {}
                : { expected_file_id: expectedFileId }),
            };
            return {
              path: write.path,
              body: write.body,
              ...(Object.keys(wirePrecondition).length === 0
                ? {}
                : { precondition: wirePrecondition }),
            };
          });
          try {
            const publication = await publications.mutate(ownerId, async () => {
              const results = await writeMany(normalizedWrites);
              return {
                results,
                entries: await snapshotPaths(
                  store,
                  normalizedWrites.map((write) => write.path),
                ),
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
                Buffer.from(write.body),
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
          return {
            value: json(200, {
              results,
              entries: await snapshotPaths(
                store,
                writes.map((write) => write.path),
              ),
            }),
            mutated: true,
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
  body: number[];
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
    if (
      !Array.isArray(write.body) ||
      !write.body.every((byte) => Number.isSafeInteger(byte) && byte >= 0 && byte <= 255)
    ) {
      return errorResponse(400, "write-many body must be an array of bytes");
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
    writes.push({ ...(write as WriteManyRequestItem), path, body: [...write.body] });
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

async function snapshotMutationPaths(
  store: VfsStorage,
  mutations: readonly NamespaceMutation[],
): Promise<PublicationSnapshotEntry[]> {
  const paths: string[] = [];
  for (const mutation of mutations) {
    for (const path of mutationPaths(mutation)) {
      paths.push(path, immediateParent(path));
    }
  }
  return snapshotPaths(store, paths);
}

function normalizeNamespaceOperationIds(value: unknown): string[] | Response {
  if (!Array.isArray(value)) {
    return errorResponse(400, "namespace-many requires operation_ids[]");
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

const PUBLICATION_ACK_TIMEOUT_DEFAULT_MS = 150;

/** Hard cap (ms) a publication waits for watcher acks before failing open.
 *  Overridable via `CHEVALIER_VFS_PUBLICATION_ACK_TIMEOUT_MS`; default 150. */
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
