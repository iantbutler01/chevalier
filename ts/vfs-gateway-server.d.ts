import type { VfsStorage } from "./native.js";
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
    transact<T>(ownerId: string, transaction: (locks: VfsAdvisoryLock[]) => VfsAdvisoryLockTransactionResult<T>): Promise<T>;
}
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
export declare function createVfsGatewayServer(opts: VfsGatewayServerOptions): (req: Request) => Promise<Response>;
