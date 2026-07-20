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
}
/** Build a WHATWG `(Request) => Promise<Response>` handler that serves chevalier's
 *  VFS gateway protocol, delegating storage to `resolveStore(ownerId)`. */
export declare function createVfsGatewayServer(opts: VfsGatewayServerOptions): (req: Request) => Promise<Response>;
