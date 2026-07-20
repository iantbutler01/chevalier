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
//                                           precondition-fingerprint header, `If-Match`,
//                                           or `ifMatch` query alias -> 409.
//                                           `If-Match` is an alias in chevalier's
//                                           protocol, not a separate HTTP 412 path.
//                                           Fingerprint is `contentHash`: SHA-256 hex
//                                           of the current logical file bytes.
//   - DELETE {owner}/file?path=&return_metadata=true -> 200 {previous}; same precondition
//   - PUT/DELETE {owner}/dir?path=       -> 2xx
//   - PUT  {owner}/symlink?path=&target= -> 2xx
//   - POST {owner}/rename?from=&to=&return_metadata=true -> 200 {previous,current}
//   - POST/DELETE {owner}/lease          -> 200 {resource_key,owner_token} / 2xx
//   - POST {owner}/{metadata-many,read-many,write-many} -> batch (per-path loop)
//   - POST {owner}/namespace-many      -> ordered namespace mutation batch
//   DTOs are snake_case; `kind` is exactly "file" | "directory"; errors map
//   404->NotFound, 400->BadRequest, 409->Conflict (vfs/src/gateway.rs:1016).
import { createHash, randomUUID } from "node:crypto";
import { mkdtemp, open, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { VfsStorage, VfsMetadata } from "./native.js";

const DEFAULT_ROUTE_PREFIX = "/internal/chevalier/vfs";
const PRECONDITION_FINGERPRINT_HEADER = "x-chevalier-vfs-precondition-fingerprint";
const IF_MATCH_HEADER = "if-match";
const EXECUTABLE_HEADER = "x-chevalier-vfs-executable";
const EXPECTED_CONTENT_HASH_HEADER = "x-chevalier-vfs-expected-content-sha256";
const STREAM_UPLOAD_HEADER = "x-chevalier-vfs-stream-upload";
const RANGE_FINGERPRINT_HEADER = "x-chevalier-vfs-range-fingerprint";
const ADVISORY_LOCK_LEASE_MS = 45_000;

export type VfsAdvisoryLockKind = "read" | "write";

export type VfsAdvisoryLock = {
  ownerId: string;
  mountId: string;
  lockOwner: string;
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
  action: "get" | "set" | "release_owner" | "renew_mount" | "release_mount";
  path?: string;
  file_id?: string;
  mount_id?: string;
  lock_owner?: string;
  start?: string;
  end?: string;
  kind?: VfsAdvisoryLockKind | "unlock";
  pid?: number;
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
      return this.state.transact(ownerId, (stored) => {
        const locks = liveLocks(stored, now);
        for (const lock of locks) {
          if (lock.mountId === mountId) lock.expiresAt = now + ADVISORY_LOCK_LEASE_MS;
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
    if (request.action === "release_owner") {
      const releasedFileId = nonEmptyString(request.file_id);
      if (releasedFileId === null) return errorResponse(400, "posix lock release requires file_id");
      return this.state.transact(ownerId, (stored) => ({
        locks: liveLocks(stored, now).filter(
          (lock) =>
            lock.mountId !== mountId ||
            lock.lockOwner !== lockOwner ||
            lock.fileId !== releasedFileId,
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
    const identity = { ownerId, mountId, lockOwner, fileId, start, end, kind, pid };

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
        lock.mountId === mountId && lock.lockOwner === lockOwner && lock.fileId === fileId;
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
          locks: replacement,
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
    this.byOwner.set(ownerId, outcome.locks);
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
    fileId: string;
    start: bigint;
    end: bigint;
    kind: VfsAdvisoryLockKind;
  },
): VfsAdvisoryLock | undefined {
  return locks.find(
    (lock) =>
      lock.fileId === request.fileId &&
      !(lock.mountId === request.mountId && lock.lockOwner === request.lockOwner) &&
      rangesOverlap(lock.start, lock.end, request.start, request.end) &&
      (lock.kind === "write" || request.kind === "write"),
  );
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
    options?: { ifMatch?: string | null; executable?: boolean } | null,
  ) => Promise<unknown>;
  writeMany?: (writes: Array<{
    path: string;
    body: number[];
    precondition?: { fingerprint?: string | null };
  }>) => Promise<StreamingWriteManyResult[]>;
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
}

/** Build a WHATWG `(Request) => Promise<Response>` handler that serves chevalier's
 *  VFS gateway protocol, delegating storage to `resolveStore(ownerId)`. */
export function createVfsGatewayServer(
  opts: VfsGatewayServerOptions,
): (req: Request) => Promise<Response> {
  const prefix = opts.routePrefix ?? DEFAULT_ROUTE_PREFIX;
  const advisoryLocks = new AdvisoryLockCoordinator(
    opts.advisoryLockState ?? new InMemoryAdvisoryLockStateStore(),
  );

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

      const store = await opts.resolveStore(ownerId);
      const q = url.searchParams;
      const method = req.method.toUpperCase();
      const relPath = normalizePath(q.get("path"));

      if (method === "POST" && op === "posix-lock/v1") {
        const body = (await req.json().catch(() => null)) as AdvisoryLockRequest | null;
        if (body === null || typeof body !== "object" || typeof body.action !== "string") {
          return errorResponse(400, "invalid posix lock request");
        }
        if (body.action === "renew_mount" || body.action === "release_mount" || body.action === "release_owner") {
          return await advisoryLocks.handle(ownerId, null, body);
        }
        const lockPath = normalizePath(body.path ?? null);
        if (lockPath === "" || isGitExcludedPath(lockPath)) {
          return errorResponse(400, `invalid posix lock path: ${lockPath}`);
        }
        const metadata = await store.stat(lockPath);
        if (metadata === null) return errorResponse(404, `not found: ${lockPath}`);
        return await advisoryLocks.handle(ownerId, metadata.fileId ?? null, body);
      }

      // ---- reads ----------------------------------------------------------
      if (method === "GET" && op === "stat") {
        if (isGitExcludedPath(relPath)) return errorResponse(404, `not found: ${relPath}`);
        const maxHashBytes = parseOptionalNonNegativeInteger(
          q.get("max_hash_bytes") ?? q.get("maxHashBytes"),
          "max_hash_bytes",
        );
        if (maxHashBytes instanceof Response) return maxHashBytes;
        const md = await store.stat(
          relPath,
          maxHashBytes === null ? undefined : { maxHashBytes },
        );
        if (md === null) return errorResponse(404, `not found: ${relPath}`);
        return json(200, toRemoteMetadata(md));
      }

      if (method === "GET" && op === "file/raw") {
        if (isGitExcludedPath(relPath)) return errorResponse(404, `not found: ${relPath}`);
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
      }

      if (method === "GET" && op === "tree") {
        if (isGitExcludedPath(relPath)) return errorResponse(404, `not found: ${relPath}`);
        const dir = relPath === "" ? "." : relPath;
        const maxHashBytes = parseOptionalNonNegativeInteger(
          q.get("max_hash_bytes") ?? q.get("maxHashBytes"),
          "max_hash_bytes",
        );
        if (maxHashBytes instanceof Response) return maxHashBytes;
        let entries: VfsMetadata[];
        try {
          entries = await store.listDir(
            dir,
            maxHashBytes === null ? undefined : { maxHashBytes },
          );
        } catch (error) {
          if (isVfsNotFoundError(error)) return errorResponse(404, `not found: ${dir}`);
          throw error;
        }
        const nameLike = q.get("name_like");
        const nameNotLike = q.get("name_not_like");
        const out = entries
          .filter((entry) => !isGitExcludedPath(entry.path))
          .map(toRemoteDirEntry)
          .filter((e) => (nameLike === null || e.name.includes(nameLike)))
          .filter((e) => (nameNotLike === null || !e.name.includes(nameNotLike)));
        return json(200, out);
      }

      // ---- leases (mutations acquire/release one; we issue a synthetic grant) --
      if (op === "lease" && method === "POST") {
        const body = (await req.json().catch(() => ({}))) as { path?: unknown };
        const leasePath = normalizePath(typeof body.path === "string" ? body.path : relPath);
        return json(200, { resource_key: `rk:${ownerId}:${leasePath}`, owner_token: randomToken() });
      }
      if (op === "lease" && method === "DELETE") {
        return new Response(null, { status: 204 });
      }

      if (method === "POST" && op === "namespace-many") {
        const body = (await req.json()) as { mutations?: unknown };
        const mutations = normalizeNamespaceMutations(body.mutations);
        if (mutations instanceof Response) return mutations;
        try {
          await store.applyNamespaceBatch(mutations);
        } catch (error) {
          const conflict = conflictResponseFromStoreError(error, "namespace-many");
          if (conflict !== null) return conflict;
          throw error;
        }
        return new Response(null, { status: 204 });
      }

      // ---- single-file mutations -----------------------------------------
      if (method === "PUT" && op === "file") {
        if (isGitExcludedPath(relPath)) return errorResponse(400, `excluded path: ${relPath}`);
        const precondition = requestPrecondition(req, q);
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
            const options = { ...preconditionOptions(precondition), ...writeOptions };
            const res =
              typeof streamingStore.writeFromFile === "function"
                ? await streamingStore.writeFromFile(relPath, stagedPath, expectedHash, options)
                : await store.write(relPath, await readFile(stagedPath), options);
            const value = res as {
              content_hash?: string;
              contentHash?: string;
              previous_hash?: string | null;
              changed?: boolean;
            };
            return json(200, {
              path: relPath,
              content_hash: value.content_hash ?? value.contentHash ?? expectedHash,
              previous_hash: value.previous_hash ?? null,
              changed: value.changed ?? true,
            });
          } catch (error) {
            const conflict = conflictResponseFromStoreError(error, relPath);
            if (conflict !== null) return conflict;
            throw error;
          } finally {
            await rm(stagedDir, { recursive: true, force: true }).catch(() => undefined);
          }
        }
        const body = Buffer.from(await req.arrayBuffer());
        let res: {
          content_hash?: string;
          contentHash?: string;
          previous_hash?: string | null;
          changed?: boolean;
        };
        try {
          res = (await store.write(relPath, body, {
            ...preconditionOptions(precondition),
            ...writeOptions,
          })) as {
            content_hash?: string;
            contentHash?: string;
            previous_hash?: string | null;
            changed?: boolean;
          };
        } catch (e) {
          const failed = conflictResponseFromStoreError(e, relPath);
          if (failed !== null) return failed;
          throw e;
        }
        // The bound client ignores this body on the plain-write path and recomputes
        // its own result; we return the real result for completeness.
        return json(200, {
          path: relPath,
          content_hash: res.content_hash ?? res.contentHash ?? null,
          previous_hash: res.previous_hash ?? null,
          changed: res.changed ?? true,
        });
      }

      if (method === "DELETE" && op === "file") {
        if (isGitExcludedPath(relPath)) return errorResponse(400, `excluded path: ${relPath}`);
        const precondition = requestPrecondition(req, q);
        const failed = await enforceFingerprintPrecondition(store, relPath, precondition);
        if (failed !== null) return failed;
        let previous: ReturnType<typeof toRemoteMetadata> | null = null;
        if (q.get("return_metadata") === "true") {
          const cur = await store.stat(relPath);
          previous = cur === null ? null : toRemoteMetadata(cur);
        }
        try {
          await store.remove(relPath, preconditionOptions(precondition));
        } catch (e) {
          const failed = conflictResponseFromStoreError(e, relPath);
          if (failed !== null) return failed;
          throw e;
        }
        return json(200, { previous });
      }

      if (method === "PUT" && op === "dir") {
        if (isGitExcludedPath(relPath)) return errorResponse(400, `excluded path: ${relPath}`);
        await store.mkdir(relPath);
        return new Response(null, { status: 204 });
      }
      if (method === "PUT" && op === "symlink") {
        if (isGitExcludedPath(relPath)) return errorResponse(400, `excluded path: ${relPath}`);
        const target = q.get("target");
        if (target === null || target === "") return errorResponse(400, "symlink requires target");
        try {
          await store.createSymlink(relPath, target);
        } catch (e) {
          if (isVfsBadRequestError(e)) return errorResponse(400, (e as Error).message);
          throw e;
        }
        return new Response(null, { status: 204 });
      }
      if (method === "DELETE" && op === "dir") {
        if (isGitExcludedPath(relPath)) return errorResponse(400, `excluded path: ${relPath}`);
        try {
          await store.rmdir(relPath);
        } catch (error) {
          if (!isVfsNotFoundError(error)) throw error;
        }
        return new Response(null, { status: 204 });
      }

      if (method === "POST" && op === "rename") {
        const from = normalizePath(q.get("from"));
        const to = normalizePath(q.get("to"));
        if (from === "" || to === "") return errorResponse(400, "rename requires from + to");
        if (isGitExcludedPath(from) || isGitExcludedPath(to)) {
          return errorResponse(400, `excluded path: ${isGitExcludedPath(from) ? from : to}`);
        }
        const previous = q.get("return_metadata") === "true" ? await store.stat(from) : null;
        await store.rename(from, to);
        const current = q.get("return_metadata") === "true" ? await store.stat(to) : null;
        return json(200, {
          previous: previous === null ? null : toRemoteMetadata(previous),
          current: current === null ? null : toRemoteMetadata(current),
        });
      }

      // ---- batch ops: loop the per-path primitives (matches the Rust trait's
      //      default impls; the bound TS client only uses per-path ops today) ----
      if (method === "POST" && op === "metadata-many") {
        const { paths } = (await req.json()) as { paths: string[] };
        const entries: (ReturnType<typeof toRemoteMetadata> | null)[] = [];
        for (const p of paths) {
          const path = normalizePath(p);
          const md = isGitExcludedPath(path) ? null : await store.stat(path);
          entries.push(md === null ? null : toRemoteMetadata(md));
        }
        return json(200, { entries });
      }

      if (method === "POST" && op === "read-many") {
        const { paths } = (await req.json()) as { paths: string[] };
        const entries: (number[] | null)[] = [];
        for (const p of paths) {
          const path = normalizePath(p);
          if (isGitExcludedPath(path)) {
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
        return json(200, { entries });
      }

      if (method === "POST" && op === "write-many") {
        const body = (await req.json()) as {
          writes: WriteManyRequestItem[];
        };
        const streamingStore = store as StreamingVfsStorage;
        if (typeof streamingStore.writeMany === "function") {
          const writes = body.writes.map((write) => {
            const path = normalizePath(write.path);
            if (isGitExcludedPath(path)) throw Object.assign(new Error(`excluded path: ${path}`), {
              code: "VFS_BAD_REQUEST",
            });
            const precondition = writeItemPrecondition(write);
            return {
              path,
              body: write.body,
              ...(precondition.present
                ? { precondition: { fingerprint: precondition.fingerprint } }
                : {}),
            };
          });
          try {
            const results = await streamingStore.writeMany(writes);
            return json(200, {
              results: results.map((result: StreamingWriteManyResult) => ({
                path: result.path,
                content_hash: result.content_hash ?? result.contentHash ?? "",
                previous_hash: result.previous_hash ?? result.previousHash ?? null,
                changed: result.changed,
              })),
            });
          } catch (error) {
            const conflict = conflictResponseFromStoreError(error, "write-many");
            if (conflict !== null) return conflict;
            throw error;
          }
        }
        // Atomic-ish: check all preconditions first, then apply. Any mismatch -> 409.
        for (const w of body.writes) {
          const path = normalizePath(w.path);
          if (isGitExcludedPath(path)) return errorResponse(400, `excluded path: ${path}`);
          const failed = await enforceFingerprintPrecondition(store, path, writeItemPrecondition(w));
          if (failed !== null) return failed;
        }
        const results = [];
        for (const w of body.writes) {
          const p = normalizePath(w.path);
          const cur = await store.stat(p);
          const prev = cur?.contentHash ?? null;
          const precondition = writeItemPrecondition(w);
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
              Buffer.from(w.body),
              preconditionOptions(precondition),
            )) as {
              content_hash?: string;
              contentHash?: string;
              previous_hash?: string | null;
              previousHash?: string | null;
              changed?: boolean;
            };
          } catch (e) {
            const failed = conflictResponseFromStoreError(e, p);
            if (failed !== null) return failed;
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
        return json(200, { results });
      }

      return errorResponse(404, `unhandled route: ${method} ${op}`);
    } catch (e) {
      if (isVfsBadRequestError(e)) return errorResponse(400, (e as Error).message);
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

function isGitExcludedPath(path: string): boolean {
  return path
    .replace(/\\/g, "/")
    .split("/")
    .filter((part) => part !== "" && part !== ".")
    .some((part) => part === ".git");
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

type FingerprintPrecondition =
  | { present: false }
  | { present: true; fingerprint: string | null };

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
  return { present: true, fingerprint: normalizeFingerprint(raw) };
}

function queryIfMatch(query: URLSearchParams): string | null {
  return query.get("ifMatch") ?? query.get("if_match");
}

function requestPrecondition(req: Request, query: URLSearchParams): FingerprintPrecondition {
  const raw =
    req.headers.get(PRECONDITION_FINGERPRINT_HEADER) ??
    req.headers.get(IF_MATCH_HEADER) ??
    queryIfMatch(query);
  if (raw !== null) return preconditionFromRaw(raw);
  return { present: false };
}

function requestWriteOptions(req: Request): { executable?: boolean } {
  const raw = req.headers.get(EXECUTABLE_HEADER);
  if (raw === null) return {};
  if (raw === "true") return { executable: true };
  if (raw === "false") return { executable: false };
  throw Object.assign(new Error(`${EXECUTABLE_HEADER} must be true or false`), {
    code: "VFS_BAD_REQUEST",
    status: 400,
  });
}

function preconditionOptions(
  precondition: FingerprintPrecondition,
): { ifMatch?: string | null } | undefined {
  if (!precondition.present) return undefined;
  return { ifMatch: precondition.fingerprint };
}

type WriteManyRequestItem = {
  path: string;
  body: number[];
  ifMatch?: string | null;
  if_match?: string | null;
  precondition?: {
    fingerprint?: string | null;
    ifMatch?: string | null;
    if_match?: string | null;
  };
};

type NamespaceMutation =
  | { kind: "create_directory"; path: string }
  | { kind: "create_symlink"; path: string; target: string }
  | {
      kind: "delete_file";
      path: string;
      precondition?: { fingerprint?: string | null };
    }
  | { kind: "remove_directory"; path: string }
  | { kind: "rename"; from: string; to: string };

function normalizeNamespaceMutations(value: unknown): NamespaceMutation[] | Response {
  if (!Array.isArray(value)) return errorResponse(400, "namespace-many requires mutations[]");
  if (value.length > 4096) return errorResponse(400, "namespace-many accepts at most 4096 mutations");
  const out: NamespaceMutation[] = [];
  for (const item of value) {
    if (typeof item !== "object" || item === null || typeof (item as { kind?: unknown }).kind !== "string") {
      return errorResponse(400, "invalid namespace mutation");
    }
    const mutation = item as Record<string, unknown>;
    const kind = mutation.kind;
    if (kind === "rename") {
      const from = normalizePath(typeof mutation.from === "string" ? mutation.from : null);
      const to = normalizePath(typeof mutation.to === "string" ? mutation.to : null);
      if (from === "" || to === "") return errorResponse(400, "rename requires from + to");
      if (isGitExcludedPath(from) || isGitExcludedPath(to)) return errorResponse(400, "excluded rename path");
      out.push({ kind, from, to });
      continue;
    }
    const path = normalizePath(typeof mutation.path === "string" ? mutation.path : null);
    if (path === "" || isGitExcludedPath(path)) return errorResponse(400, `invalid namespace path: ${path}`);
    if (kind === "delete_file") {
      const precondition = writeItemPrecondition(mutation as WriteManyRequestItem);
      out.push({
        kind,
        path,
        ...(precondition.present
          ? { precondition: { fingerprint: precondition.fingerprint } }
          : {}),
      });
      continue;
    }
    if (kind === "create_directory" || kind === "remove_directory") {
      out.push({ kind, path });
      continue;
    }
    if (kind === "create_symlink") {
      if (typeof mutation.target !== "string" || mutation.target === "") {
        return errorResponse(400, "create_symlink requires target");
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

async function enforceFingerprintPrecondition(
  store: VfsStorage,
  path: string,
  precondition: FingerprintPrecondition,
): Promise<Response | null> {
  if (!precondition.present) return null;
  const cur = await store.stat(path);
  const curHash = mutationFingerprint(cur);
  if (precondition.fingerprint === curHash) return null;
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
  return {
    kind: wireKind(md.kind),
    size_bytes: Number(md.sizeBytes),
    file_id: md.fileId ?? null,
    link_count: Number(md.linkCount ?? 1n),
    executable: md.executable ?? false,
    link_target: md.linkTarget ?? null,
    content_hash: md.contentHash ?? null,
    updated_at: md.updatedAt ?? null,
  };
}

function toRemoteDirEntry(md: VfsMetadata) {
  const name = md.path.split("/").filter((s) => s !== "").pop() ?? md.path;
  return {
    name,
    kind: wireKind(md.kind),
    size_bytes: Number(md.sizeBytes),
    file_id: md.fileId ?? null,
    link_count: Number(md.linkCount ?? 1n),
    executable: md.executable ?? false,
    link_target: md.linkTarget ?? null,
    content_hash: md.contentHash ?? null,
    updated_at: md.updatedAt ?? null,
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

function errorResponse(status: number, message: string): Response {
  return new Response(message, { status, headers: { "content-type": "text/plain" } });
}

function randomToken(): string {
  // Rust FUSE clients deserialize owner_token as a UUID.
  return randomUUID();
}
