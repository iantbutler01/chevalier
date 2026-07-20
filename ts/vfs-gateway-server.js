"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createVfsGatewayServer = createVfsGatewayServer;
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
//   - POST {owner}/{metadata-many,read-many,write-many} -> batch (per-path loop)
//   - POST {owner}/namespace-many      -> ordered namespace mutation batch
//   DTOs are snake_case; `kind` is exactly "file" | "directory"; errors map
//   404->NotFound, 400->BadRequest, 409->Conflict (vfs/src/gateway.rs:1016).
const node_crypto_1 = require("node:crypto");
const promises_1 = require("node:fs/promises");
const node_os_1 = require("node:os");
const node_path_1 = require("node:path");
const DEFAULT_ROUTE_PREFIX = "/internal/chevalier/vfs";
const PRECONDITION_FINGERPRINT_HEADER = "x-chevalier-vfs-precondition-fingerprint";
const PRECONDITION_FILE_ID_HEADER = "x-chevalier-vfs-precondition-file-id";
const IF_MATCH_HEADER = "if-match";
const EXECUTABLE_HEADER = "x-chevalier-vfs-executable";
const MODE_HEADER = "x-chevalier-vfs-mode";
const EXPECTED_CONTENT_HASH_HEADER = "x-chevalier-vfs-expected-content-sha256";
const STREAM_UPLOAD_HEADER = "x-chevalier-vfs-stream-upload";
const RANGE_FINGERPRINT_HEADER = "x-chevalier-vfs-range-fingerprint";
const ADVISORY_LOCK_LEASE_MS = 45_000;
const MAX_BATCH_ITEMS = 4096;
/**
 * Coordinates leased POSIX locks independently from write authorization and
 * namespace mutation leases. Production multi-process gateways supply a shared
 * transactional state store; the default is intentionally process-local for
 * embedders and tests.
 */
class AdvisoryLockCoordinator {
    constructor(state) {
        this.state = state;
    }
    async handle(ownerId, fileId, request) {
        const now = Date.now();
        const mountId = nonEmptyString(request.mount_id);
        if (mountId === null)
            return errorResponse(400, "posix lock requires mount_id");
        if (request.action === "renew_mount") {
            return errorResponse(400, "renew_mount is unsupported; use renew_owners with exact identities");
        }
        if (request.action === "renew_owners") {
            const identities = normalizeAdvisoryLockRenewalIdentities(request.identities);
            if (identities instanceof Response)
                return identities;
            const identityKeys = new Set(identities.map(({ lockOwner, namespace, fileId }) => advisoryLockIdentityKey(lockOwner, namespace, fileId)));
            return this.state.transact(ownerId, (stored) => {
                const locks = liveLocks(stored, now);
                for (const lock of locks) {
                    if (lock.mountId === mountId &&
                        identityKeys.has(advisoryLockIdentityKey(lock.lockOwner, lock.namespace ?? "posix", lock.fileId))) {
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
        if (lockOwner === null)
            return errorResponse(400, "posix lock requires lock_owner");
        const namespace = request.namespace ?? "posix";
        if (namespace !== "posix" && namespace !== "flock") {
            return errorResponse(400, "advisory lock namespace must be posix or flock");
        }
        if (request.action === "release_owner") {
            const releasedFileId = nonEmptyString(request.file_id);
            if (releasedFileId === null)
                return errorResponse(400, "posix lock release requires file_id");
            return this.state.transact(ownerId, (stored) => ({
                locks: liveLocks(stored, now).filter((lock) => lock.mountId !== mountId ||
                    lock.lockOwner !== lockOwner ||
                    lock.fileId !== releasedFileId ||
                    (lock.namespace ?? "posix") !== namespace),
                result: json(200, { ok: true }),
            }));
        }
        if (fileId === null)
            return errorResponse(501, "stable file identity is unavailable for posix locking");
        const start = parseLockOffset(request.start, "start");
        if (start instanceof Response)
            return start;
        const end = parseLockOffset(request.end, "end");
        if (end instanceof Response)
            return end;
        if (end < start)
            return errorResponse(400, "posix lock end precedes start");
        const kind = request.kind;
        if (kind !== "read" && kind !== "write" && kind !== "unlock") {
            return errorResponse(400, "posix lock kind must be read, write, or unlock");
        }
        const pid = Number.isSafeInteger(request.pid) && (request.pid ?? -1) >= 0 ? request.pid : 0;
        const identity = { ownerId, mountId, lockOwner, namespace, fileId, start, end, kind, pid };
        if (request.action === "get") {
            if (kind === "unlock")
                return errorResponse(400, "get posix lock cannot query unlock");
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
        if (request.action !== "set")
            return errorResponse(400, `unsupported posix lock action: ${request.action}`);
        return this.state.transact(ownerId, (stored) => {
            const locks = liveLocks(stored, now);
            const ownKey = (lock) => lock.mountId === mountId &&
                lock.lockOwner === lockOwner &&
                (lock.namespace ?? "posix") === namespace &&
                lock.fileId === fileId;
            const replacement = locks.flatMap((lock) => {
                if (!ownKey(lock) || !rangesOverlap(lock.start, lock.end, start, end))
                    return [lock];
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
class InMemoryAdvisoryLockStateStore {
    constructor() {
        this.byOwner = new Map();
    }
    async transact(ownerId, transaction) {
        const outcome = transaction([...(this.byOwner.get(ownerId) ?? [])]);
        if (outcome.locks.length === 0) {
            this.byOwner.delete(ownerId);
        }
        else {
            this.byOwner.set(ownerId, outcome.locks);
        }
        return outcome.result;
    }
}
function liveLocks(locks, now) {
    return locks.filter((lock) => lock.expiresAt > now);
}
function firstConflict(locks, request) {
    return locks.find((lock) => lock.fileId === request.fileId &&
        (lock.namespace ?? "posix") === request.namespace &&
        !(lock.mountId === request.mountId && lock.lockOwner === request.lockOwner) &&
        rangesOverlap(lock.start, lock.end, request.start, request.end) &&
        (lock.kind === "write" || request.kind === "write"));
}
function normalizeAdvisoryLockRenewalIdentities(value) {
    if (!Array.isArray(value) || value.length === 0) {
        return errorResponse(400, "renew_owners requires a non-empty identities[]");
    }
    if (value.length > MAX_BATCH_ITEMS) {
        return errorResponse(400, `renew_owners accepts at most ${MAX_BATCH_ITEMS} identities`);
    }
    const identities = [];
    for (const [index, item] of value.entries()) {
        if (typeof item !== "object" || item === null || Array.isArray(item)) {
            return errorResponse(400, `renew_owners identity ${index} must be an object`);
        }
        const identity = item;
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
            return errorResponse(400, `renew_owners identity ${index} namespace must be posix or flock`);
        }
        identities.push({ lockOwner, namespace, fileId });
    }
    return identities;
}
function advisoryLockIdentityKey(lockOwner, namespace, fileId) {
    return JSON.stringify([lockOwner, namespace, fileId]);
}
function nonEmptyString(value) {
    return typeof value === "string" && value.trim() !== "" ? value : null;
}
function parseLockOffset(value, name) {
    if (typeof value !== "string" || !/^\d+$/.test(value)) {
        return errorResponse(400, `posix lock ${name} must be an unsigned decimal string`);
    }
    try {
        return BigInt(value);
    }
    catch {
        return errorResponse(400, `invalid posix lock ${name}`);
    }
}
function rangesOverlap(aStart, aEnd, bStart, bEnd) {
    return aStart <= bEnd && bStart <= aEnd;
}
function subtractRange(lock, start, end) {
    const out = [];
    if (lock.start < start)
        out.push({ ...lock, end: start - 1n });
    if (lock.end > end)
        out.push({ ...lock, start: end + 1n });
    return out;
}
function lockResponse(lock) {
    return {
        start: lock.start.toString(),
        end: lock.end.toString(),
        kind: lock.kind,
        pid: lock.pid,
    };
}
/** Build a WHATWG `(Request) => Promise<Response>` handler that serves chevalier's
 *  VFS gateway protocol, delegating storage to `resolveStore(ownerId)`. */
function createVfsGatewayServer(opts) {
    const prefix = opts.routePrefix ?? DEFAULT_ROUTE_PREFIX;
    const advisoryLocks = new AdvisoryLockCoordinator(opts.advisoryLockState ?? new InMemoryAdvisoryLockStateStore());
    return async function handle(req) {
        try {
            if (opts.authToken !== undefined && opts.authToken !== "") {
                const auth = req.headers.get("authorization") ?? "";
                if (auth !== `Bearer ${opts.authToken}`)
                    return errorResponse(401, "unauthorized");
            }
            const url = new URL(req.url);
            const idx = url.pathname.indexOf(prefix);
            if (idx < 0)
                return errorResponse(404, "not a chevalier vfs route");
            const rest = url.pathname.slice(idx + prefix.length).replace(/^\/+/, "");
            const segs = rest.split("/").filter((s) => s !== "");
            const ownerId = segs.shift() ?? "";
            const op = segs.join("/");
            if (ownerId === "")
                return errorResponse(404, "missing owner_id segment");
            const allowGitMetadata = (await opts.allowGitMetadata?.(ownerId)) ?? false;
            const isExcludedPath = (path) => !allowGitMetadata && isGitExcludedPath(path);
            const store = await opts.resolveStore(ownerId);
            const q = url.searchParams;
            const method = req.method.toUpperCase();
            const relPath = normalizePath(q.get("path"));
            if (method === "POST" && op === "posix-lock/v1") {
                const body = (await req.json().catch(() => null));
                if (body === null || typeof body !== "object" || typeof body.action !== "string") {
                    return errorResponse(400, "invalid posix lock request");
                }
                if (body.action === "renew_owners" ||
                    body.action === "renew_mount" ||
                    body.action === "release_mount" ||
                    body.action === "release_owner") {
                    return await advisoryLocks.handle(ownerId, null, body);
                }
                const lockPath = normalizePath(body.path ?? null);
                if (lockPath === "" || isExcludedPath(lockPath)) {
                    return errorResponse(400, `invalid posix lock path: ${lockPath}`);
                }
                const metadata = await store.stat(lockPath);
                if (metadata === null)
                    return errorResponse(404, `not found: ${lockPath}`);
                return await advisoryLocks.handle(ownerId, metadata.fileId ?? null, body);
            }
            // ---- reads ----------------------------------------------------------
            if (method === "GET" && op === "stat") {
                if (isExcludedPath(relPath))
                    return errorResponse(404, `not found: ${relPath}`);
                const maxHashBytes = parseOptionalNonNegativeInteger(q.get("max_hash_bytes") ?? q.get("maxHashBytes"), "max_hash_bytes");
                if (maxHashBytes instanceof Response)
                    return maxHashBytes;
                const md = await store.stat(relPath, maxHashBytes === null ? undefined : { maxHashBytes });
                if (md === null)
                    return errorResponse(404, `not found: ${relPath}`);
                return json(200, toRemoteMetadata(md));
            }
            if (method === "GET" && op === "file/raw") {
                if (isExcludedPath(relPath))
                    return errorResponse(404, `not found: ${relPath}`);
                const requestedRange = req.headers.get("range");
                if (requestedRange !== null) {
                    // Ranged reads are path-addressed across many requests, so they carry
                    // a cheap (size, mtime) fingerprint instead of a content hash: the
                    // client pins the file identity it started reading, and a replace in
                    // between surfaces as 412 rather than a spliced old/new file. The
                    // hashless stat also avoids re-hashing large files per range.
                    let metadata;
                    try {
                        metadata = await store.stat(relPath, { maxHashBytes: 0 });
                    }
                    catch (error) {
                        if (isVfsNotFoundError(error))
                            return errorResponse(404, `not found: ${relPath}`);
                        throw error;
                    }
                    if (metadata === null)
                        return errorResponse(404, `not found: ${relPath}`);
                    const fingerprint = rangeFingerprint(metadata);
                    const expectedFingerprint = req.headers.get(RANGE_FINGERPRINT_HEADER);
                    if (expectedFingerprint !== null && expectedFingerprint !== fingerprint) {
                        return staleRangeResponse(relPath, fingerprint);
                    }
                    const size = Number(metadata.sizeBytes);
                    const range = parseRange(requestedRange, size);
                    if (range === null)
                        return errorResponse(416, `invalid range for ${relPath}`);
                    const length = range.end - range.start + 1;
                    const streamingStore = store;
                    const slice = typeof streamingStore.readRange === "function"
                        ? await streamingStore.readRange(relPath, BigInt(range.start), length)
                        : (await store.read(relPath)).subarray(range.start, range.end + 1);
                    // Bracket the read: if the file changed while we were reading it, the
                    // slice may mix old and new bytes. Never return it.
                    let after;
                    try {
                        after = await store.stat(relPath, { maxHashBytes: 0 });
                    }
                    catch (error) {
                        if (isVfsNotFoundError(error))
                            return errorResponse(404, `not found: ${relPath}`);
                        throw error;
                    }
                    if (after === null)
                        return errorResponse(404, `not found: ${relPath}`);
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
                let buf;
                try {
                    buf = await store.read(relPath);
                }
                catch (error) {
                    if (isVfsNotFoundError(error))
                        return errorResponse(404, `not found: ${relPath}`);
                    throw error;
                }
                return new Response(asBody(buf), {
                    status: 200,
                    headers: { "content-type": "application/octet-stream" },
                });
            }
            if (method === "GET" && op === "tree") {
                if (isExcludedPath(relPath))
                    return errorResponse(404, `not found: ${relPath}`);
                const dir = relPath === "" ? "." : relPath;
                const maxHashBytes = parseOptionalNonNegativeInteger(q.get("max_hash_bytes") ?? q.get("maxHashBytes"), "max_hash_bytes");
                if (maxHashBytes instanceof Response)
                    return maxHashBytes;
                let entries;
                try {
                    entries = await store.listDir(dir, maxHashBytes === null ? undefined : { maxHashBytes });
                }
                catch (error) {
                    if (isVfsNotFoundError(error))
                        return errorResponse(404, `not found: ${dir}`);
                    throw error;
                }
                const nameLike = q.get("name_like");
                const nameNotLike = q.get("name_not_like");
                const out = entries
                    .filter((entry) => !isExcludedPath(entry.path))
                    .map(toRemoteDirEntry)
                    .filter((e) => (nameLike === null || e.name.includes(nameLike)))
                    .filter((e) => (nameNotLike === null || !e.name.includes(nameNotLike)));
                return json(200, out);
            }
            // ---- leases (mutations acquire/release one; we issue a synthetic grant) --
            if (op === "lease" && method === "POST") {
                const body = (await req.json().catch(() => ({})));
                const leasePath = normalizePath(typeof body.path === "string" ? body.path : relPath);
                if (isExcludedPath(leasePath))
                    return errorResponse(400, `excluded path: ${leasePath}`);
                return json(200, { resource_key: `rk:${ownerId}:${leasePath}`, owner_token: randomToken() });
            }
            if (op === "lease" && method === "DELETE") {
                return new Response(null, { status: 204 });
            }
            if (method === "POST" && op === "namespace-many") {
                const body = await requestJsonObject(req, "namespace-many");
                if (body instanceof Response)
                    return body;
                const mutations = normalizeNamespaceMutations(body.mutations, isExcludedPath);
                if (mutations instanceof Response)
                    return mutations;
                try {
                    await store.applyNamespaceBatch(mutations);
                }
                catch (error) {
                    const conflict = conflictResponseFromStoreError(error, "namespace-many");
                    if (conflict !== null)
                        return conflict;
                    throw error;
                }
                return new Response(null, { status: 204 });
            }
            // ---- single-file mutations -----------------------------------------
            if (method === "PUT" && op === "file") {
                if (isExcludedPath(relPath))
                    return errorResponse(400, `excluded path: ${relPath}`);
                const precondition = requestPrecondition(req, q);
                const expectedFileId = requestExpectedFileId(req);
                const writeOptions = requestWriteOptions(req);
                const failed = await enforceFingerprintPrecondition(store, relPath, precondition);
                if (failed !== null)
                    return failed;
                if (req.headers.get(STREAM_UPLOAD_HEADER) === "1") {
                    const expectedHash = req.headers.get(EXPECTED_CONTENT_HASH_HEADER)?.trim().toLowerCase() ?? "";
                    if (!/^[a-f0-9]{64}$/.test(expectedHash)) {
                        return errorResponse(400, `${EXPECTED_CONTENT_HASH_HEADER} must be a SHA-256 hex digest`);
                    }
                    const declaredLength = parseOptionalNonNegativeInteger(req.headers.get("content-length"), "content-length");
                    if (declaredLength instanceof Response)
                        return declaredLength;
                    const stagedDir = await (0, promises_1.mkdtemp)((0, node_path_1.join)((0, node_os_1.tmpdir)(), "chevalier-vfs-upload-"));
                    const stagedPath = (0, node_path_1.join)(stagedDir, "payload");
                    try {
                        const staged = await (0, promises_1.open)(stagedPath, "wx", 0o600);
                        const hasher = (0, node_crypto_1.createHash)("sha256");
                        let received = 0;
                        try {
                            const reader = req.body?.getReader();
                            if (reader !== undefined) {
                                for (;;) {
                                    const { done, value } = await reader.read();
                                    if (done)
                                        break;
                                    if (value.byteLength === 0)
                                        continue;
                                    hasher.update(value);
                                    await staged.write(value);
                                    received += value.byteLength;
                                }
                            }
                            await staged.sync();
                        }
                        finally {
                            await staged.close();
                        }
                        if (declaredLength !== null && received !== declaredLength) {
                            return errorResponse(400, `streamed upload length mismatch for ${relPath}`);
                        }
                        if (hasher.digest("hex") !== expectedHash) {
                            return errorResponse(409, `streamed upload hash mismatch for ${relPath}`);
                        }
                        const streamingStore = store;
                        const options = {
                            ...preconditionOptions(precondition, expectedFileId),
                            ...writeOptions,
                        };
                        const res = typeof streamingStore.writeFromFile === "function"
                            ? await streamingStore.writeFromFile(relPath, stagedPath, expectedHash, options)
                            : await store.write(relPath, await (0, promises_1.readFile)(stagedPath), options);
                        const value = res;
                        return json(200, {
                            path: relPath,
                            content_hash: value.content_hash ?? value.contentHash ?? expectedHash,
                            previous_hash: value.previous_hash ?? null,
                            changed: value.changed ?? true,
                        });
                    }
                    catch (error) {
                        const conflict = conflictResponseFromStoreError(error, relPath);
                        if (conflict !== null)
                            return conflict;
                        throw error;
                    }
                    finally {
                        await (0, promises_1.rm)(stagedDir, { recursive: true, force: true }).catch(() => undefined);
                    }
                }
                const body = Buffer.from(await req.arrayBuffer());
                let res;
                try {
                    res = (await store.write(relPath, body, {
                        ...preconditionOptions(precondition, expectedFileId),
                        ...writeOptions,
                    }));
                }
                catch (e) {
                    const failed = conflictResponseFromStoreError(e, relPath);
                    if (failed !== null)
                        return failed;
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
                if (isExcludedPath(relPath))
                    return errorResponse(400, `excluded path: ${relPath}`);
                const precondition = requestPrecondition(req, q);
                const failed = await enforceFingerprintPrecondition(store, relPath, precondition);
                if (failed !== null)
                    return failed;
                let previous = null;
                if (q.get("return_metadata") === "true") {
                    const cur = await store.stat(relPath);
                    previous = cur === null ? null : toRemoteMetadata(cur);
                }
                try {
                    await store.remove(relPath, preconditionOptions(precondition));
                }
                catch (e) {
                    const failed = conflictResponseFromStoreError(e, relPath);
                    if (failed !== null)
                        return failed;
                    throw e;
                }
                return json(200, { previous });
            }
            if (method === "PUT" && op === "dir") {
                if (isExcludedPath(relPath))
                    return errorResponse(400, `excluded path: ${relPath}`);
                const writeOptions = requestWriteOptions(req);
                await store.mkdir(relPath, Object.keys(writeOptions).length === 0 ? undefined : writeOptions);
                return new Response(null, { status: 204 });
            }
            if (method === "PUT" && op === "symlink") {
                if (isExcludedPath(relPath))
                    return errorResponse(400, `excluded path: ${relPath}`);
                const target = q.get("target");
                if (target === null || target === "")
                    return errorResponse(400, "symlink requires target");
                if (isExcludedPath(symlinkTargetPath(relPath, target))) {
                    return errorResponse(400, `excluded symlink target: ${target}`);
                }
                try {
                    await store.createSymlink(relPath, target);
                }
                catch (e) {
                    if (isVfsBadRequestError(e))
                        return errorResponse(400, e.message);
                    throw e;
                }
                return new Response(null, { status: 204 });
            }
            if (method === "DELETE" && op === "dir") {
                if (isExcludedPath(relPath))
                    return errorResponse(400, `excluded path: ${relPath}`);
                try {
                    await store.rmdir(relPath);
                }
                catch (error) {
                    if (!isVfsNotFoundError(error))
                        throw error;
                }
                return new Response(null, { status: 204 });
            }
            if (method === "POST" && op === "hard-link/v1") {
                const body = (await req.json());
                const source = normalizePath(typeof body.source_path === "string" ? body.source_path : "");
                const destination = normalizePath(typeof body.destination_path === "string" ? body.destination_path : "");
                if (source === "" || destination === "") {
                    return errorResponse(400, "hard-link requires source_path + destination_path");
                }
                if (isExcludedPath(source) || isExcludedPath(destination)) {
                    return errorResponse(400, `excluded path: ${isExcludedPath(source) ? source : destination}`);
                }
                try {
                    const result = await store.createHardLink(source, destination);
                    return json(200, {
                        source: toRemoteMetadata(result.source),
                        destination: toRemoteMetadata(result.destination),
                    });
                }
                catch (error) {
                    const conflict = conflictResponseFromStoreError(error, destination);
                    if (conflict !== null)
                        return conflict;
                    if (isVfsBadRequestError(error)) {
                        return errorResponse(400, error.message);
                    }
                    throw error;
                }
            }
            if (method === "POST" && op === "hard-link-alias/v1") {
                const body = (await req.json());
                if (typeof body.file_id !== "string" || body.file_id.trim() === "") {
                    return errorResponse(400, "hard-link alias resolution requires file_id");
                }
                const excludingPath = normalizePath(typeof body.excluding_path === "string" ? body.excluding_path : "");
                if (isExcludedPath(excludingPath))
                    return errorResponse(400, `excluded path: ${excludingPath}`);
                const path = await store.findHardLinkAlias(body.file_id, excludingPath);
                return json(200, { path: path !== null && isExcludedPath(path) ? null : path });
            }
            if (method === "POST" && op === "rename") {
                const from = normalizePath(q.get("from"));
                const to = normalizePath(q.get("to"));
                if (from === "" || to === "")
                    return errorResponse(400, "rename requires from + to");
                if (isExcludedPath(from) || isExcludedPath(to)) {
                    return errorResponse(400, `excluded path: ${isExcludedPath(from) ? from : to}`);
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
                const body = await requestJsonObject(req, "metadata-many");
                if (body instanceof Response)
                    return body;
                const paths = normalizePathBatch(body.paths, "metadata-many");
                if (paths instanceof Response)
                    return paths;
                const entries = [];
                for (const path of paths) {
                    const md = isExcludedPath(path) ? null : await store.stat(path);
                    entries.push(md === null ? null : toRemoteMetadata(md));
                }
                return json(200, { entries });
            }
            if (method === "POST" && op === "read-many") {
                const body = await requestJsonObject(req, "read-many");
                if (body instanceof Response)
                    return body;
                const paths = normalizePathBatch(body.paths, "read-many");
                if (paths instanceof Response)
                    return paths;
                const entries = [];
                for (const path of paths) {
                    if (isExcludedPath(path)) {
                        entries.push(null);
                        continue;
                    }
                    try {
                        const buf = await store.read(path);
                        entries.push([...buf]);
                    }
                    catch (error) {
                        if (isVfsNotFoundError(error))
                            entries.push(null);
                        else
                            throw error;
                    }
                }
                return json(200, { entries });
            }
            if (method === "POST" && op === "write-many") {
                const body = await requestJsonObject(req, "write-many");
                if (body instanceof Response)
                    return body;
                const writes = normalizeWriteManyItems(body.writes, isExcludedPath);
                if (writes instanceof Response)
                    return writes;
                const streamingStore = store;
                if (typeof streamingStore.writeMany === "function") {
                    const normalizedWrites = writes.map((write) => {
                        const precondition = writeItemPrecondition(write);
                        const expectedFileId = writeItemExpectedFileId(write);
                        const wirePrecondition = {
                            ...(precondition.present ? { fingerprint: precondition.fingerprint } : {}),
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
                        const results = await streamingStore.writeMany(normalizedWrites);
                        return json(200, {
                            results: results.map((result) => ({
                                path: result.path,
                                content_hash: result.content_hash ?? result.contentHash ?? "",
                                previous_hash: result.previous_hash ?? result.previousHash ?? null,
                                changed: result.changed,
                            })),
                        });
                    }
                    catch (error) {
                        const conflict = conflictResponseFromStoreError(error, "write-many");
                        if (conflict !== null)
                            return conflict;
                        throw error;
                    }
                }
                // Atomic-ish: check all preconditions first, then apply. Any mismatch -> 409.
                for (const write of writes) {
                    const failed = await enforceFingerprintPrecondition(store, write.path, writeItemPrecondition(write));
                    if (failed !== null)
                        return failed;
                }
                const results = [];
                for (const write of writes) {
                    const p = write.path;
                    const cur = await store.stat(p);
                    const prev = cur?.contentHash ?? null;
                    const precondition = writeItemPrecondition(write);
                    const expectedFileId = writeItemExpectedFileId(write);
                    let res;
                    try {
                        res = (await store.write(p, Buffer.from(write.body), preconditionOptions(precondition, expectedFileId)));
                    }
                    catch (e) {
                        const failed = conflictResponseFromStoreError(e, p);
                        if (failed !== null)
                            return failed;
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
        }
        catch (e) {
            if (isVfsBadRequestError(e))
                return errorResponse(400, e.message);
            return errorResponse(500, `gateway server error: ${e.message}`);
        }
    };
}
// ---- helpers ---------------------------------------------------------------
/** A `Buffer` is a `Uint8Array` at runtime and is a valid response body, but the
 *  DOM lib types don't list it as `BodyInit`; hand back a plain `Uint8Array` view
 *  (zero-copy) so typing is happy without a copy. */
function asBody(buf) {
    // Zero-copy view; cast because this tsconfig's DOM lib omits Uint8Array from
    // BodyInit even though it is a valid body at runtime.
    return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
}
function normalizePath(raw) {
    if (raw === null)
        return "";
    const t = raw.trim().replace(/^\/+/, "").replace(/\/+$/, "");
    return t === "." ? "" : t;
}
function asciiCaseFold(value) {
    return value.replace(/[A-Z]/g, (character) => String.fromCharCode(character.charCodeAt(0) + 32));
}
function isGitMetadataSegment(segment) {
    return asciiCaseFold(segment) === ".git";
}
function isGitExcludedPath(path) {
    return path
        .replace(/\\/g, "/")
        .split("/")
        .filter((part) => part !== "" && part !== ".")
        .some(isGitMetadataSegment);
}
function symlinkTargetPath(path, target) {
    const normalizedPath = normalizePath(path).replace(/\\/g, "/");
    const normalizedTarget = target.replace(/\\/g, "/");
    if (node_path_1.posix.isAbsolute(normalizedTarget))
        return normalizePath(normalizedTarget);
    return node_path_1.posix.normalize(node_path_1.posix.join(node_path_1.posix.dirname(normalizedPath), normalizedTarget));
}
async function requestJsonObject(request, operation) {
    let value;
    try {
        value = await request.json();
    }
    catch {
        return errorResponse(400, `${operation} requires a JSON object`);
    }
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
        return errorResponse(400, `${operation} requires a JSON object`);
    }
    return value;
}
function normalizePathBatch(value, operation) {
    if (!Array.isArray(value))
        return errorResponse(400, `${operation} requires paths[]`);
    if (value.length > MAX_BATCH_ITEMS) {
        return errorResponse(400, `${operation} accepts at most ${MAX_BATCH_ITEMS} paths`);
    }
    const paths = [];
    for (const item of value) {
        if (typeof item !== "string") {
            return errorResponse(400, `${operation} paths must be strings`);
        }
        paths.push(normalizePath(item));
    }
    return paths;
}
function vfsErrorStatus(error) {
    const value = error;
    if (typeof value?.status === "number")
        return value.status;
    if (typeof value?.statusCode === "number")
        return value.statusCode;
    if (typeof value?.code === "number")
        return value.code;
    if (typeof value?.message === "string") {
        const match = /status=(\d{3})/.exec(value.message);
        if (match !== null)
            return Number(match[1]);
    }
    return null;
}
function isVfsNotFoundError(error) {
    const value = error;
    return value?.code === "VFS_NOT_FOUND" || vfsErrorStatus(error) === 404;
}
function isVfsBadRequestError(error) {
    const value = error;
    return value?.code === "VFS_BAD_REQUEST" || vfsErrorStatus(error) === 400;
}
function normalizeFingerprint(raw) {
    if (raw === null || raw === undefined)
        return null;
    let next = raw.trim();
    if (next.startsWith("W/"))
        next = next.slice(2).trim();
    if ((next.startsWith('"') && next.endsWith('"')) ||
        (next.startsWith("'") && next.endsWith("'"))) {
        next = next.slice(1, -1).trim();
    }
    if (next.startsWith("sha256:"))
        next = next.slice("sha256:".length);
    if (next === "" || next.toLowerCase() === "null")
        return null;
    return next;
}
function preconditionFromRaw(raw) {
    return { present: true, fingerprint: normalizeFingerprint(raw) };
}
function queryIfMatch(query) {
    return query.get("ifMatch") ?? query.get("if_match");
}
function requestPrecondition(req, query) {
    const raw = req.headers.get(PRECONDITION_FINGERPRINT_HEADER) ??
        req.headers.get(IF_MATCH_HEADER) ??
        queryIfMatch(query);
    if (raw !== null)
        return preconditionFromRaw(raw);
    return { present: false };
}
function parseExpectedFileId(raw, source) {
    if (raw === undefined || raw === null)
        return undefined;
    if (typeof raw !== "string" || raw.length === 0) {
        throw Object.assign(new Error(`${source} must be a non-empty string`), {
            code: "VFS_BAD_REQUEST",
            status: 400,
        });
    }
    return raw;
}
function requestExpectedFileId(req) {
    return parseExpectedFileId(req.headers.get(PRECONDITION_FILE_ID_HEADER), PRECONDITION_FILE_ID_HEADER);
}
function parseMode(raw, source) {
    if (raw === undefined || raw === null)
        return undefined;
    const value = typeof raw === "string" && /^[0-9]+$/.test(raw)
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
function requestWriteOptions(req) {
    const rawMode = req.headers.get(MODE_HEADER);
    const mode = parseMode(rawMode, MODE_HEADER);
    const rawExecutable = req.headers.get(EXECUTABLE_HEADER);
    let executable;
    if (rawExecutable === "true")
        executable = true;
    else if (rawExecutable === "false")
        executable = false;
    else if (rawExecutable !== null) {
        throw Object.assign(new Error(`${EXECUTABLE_HEADER} must be true or false`), {
            code: "VFS_BAD_REQUEST",
            status: 400,
        });
    }
    if (mode !== undefined)
        return { executable: (mode & 0o111) !== 0, mode };
    return executable === undefined ? {} : { executable };
}
function preconditionOptions(precondition, expectedFileId) {
    if (!precondition.present && expectedFileId === undefined)
        return undefined;
    return {
        ...(precondition.present ? { ifMatch: precondition.fingerprint } : {}),
        ...(expectedFileId === undefined ? {} : { expectedFileId }),
    };
}
function normalizeWriteManyItems(value, isExcludedPath) {
    if (!Array.isArray(value))
        return errorResponse(400, "write-many requires writes[]");
    if (value.length > MAX_BATCH_ITEMS) {
        return errorResponse(400, `write-many accepts at most ${MAX_BATCH_ITEMS} writes`);
    }
    const writes = [];
    for (const item of value) {
        if (typeof item !== "object" || item === null || Array.isArray(item)) {
            return errorResponse(400, "invalid write-many item");
        }
        const write = item;
        if (typeof write.path !== "string")
            return errorResponse(400, "write-many path must be a string");
        const path = normalizePath(write.path);
        if (path === "" || isExcludedPath(path)) {
            return errorResponse(400, `invalid write-many path: ${path}`);
        }
        if (!Array.isArray(write.body) ||
            !write.body.every((byte) => Number.isSafeInteger(byte) && byte >= 0 && byte <= 255)) {
            return errorResponse(400, "write-many body must be an array of bytes");
        }
        const preconditionValue = ownValue(write, "precondition");
        if (preconditionValue !== undefined &&
            preconditionValue !== null &&
            (typeof preconditionValue !== "object" ||
                Array.isArray(preconditionValue))) {
            return errorResponse(400, "write-many precondition must be an object or null");
        }
        try {
            writeItemPrecondition(write);
            writeItemExpectedFileId(write);
        }
        catch (error) {
            return errorResponse(400, error instanceof Error ? error.message : String(error));
        }
        writes.push({ ...write, path, body: [...write.body] });
    }
    return writes;
}
function normalizeNamespaceMutations(value, isExcludedPath = isGitExcludedPath) {
    if (!Array.isArray(value))
        return errorResponse(400, "namespace-many requires mutations[]");
    if (value.length > MAX_BATCH_ITEMS) {
        return errorResponse(400, `namespace-many accepts at most ${MAX_BATCH_ITEMS} mutations`);
    }
    const out = [];
    for (const item of value) {
        if (typeof item !== "object" || item === null || typeof item.kind !== "string") {
            return errorResponse(400, "invalid namespace mutation");
        }
        const mutation = item;
        const kind = mutation.kind;
        if (kind === "rename") {
            const from = normalizePath(typeof mutation.from === "string" ? mutation.from : null);
            const to = normalizePath(typeof mutation.to === "string" ? mutation.to : null);
            if (from === "" || to === "")
                return errorResponse(400, "rename requires from + to");
            if (isExcludedPath(from) || isExcludedPath(to))
                return errorResponse(400, "excluded rename path");
            out.push({ kind, from, to });
            continue;
        }
        const path = normalizePath(typeof mutation.path === "string" ? mutation.path : null);
        if (path === "" || isExcludedPath(path))
            return errorResponse(400, `invalid namespace path: ${path}`);
        if (kind === "delete_file") {
            let precondition;
            try {
                precondition = writeItemPrecondition(mutation);
            }
            catch (error) {
                return errorResponse(400, error instanceof Error ? error.message : String(error));
            }
            out.push({
                kind,
                path,
                ...(precondition.present
                    ? { precondition: { fingerprint: precondition.fingerprint } }
                    : {}),
            });
            continue;
        }
        if (kind === "create_directory") {
            let mode;
            try {
                mode = parseMode(mutation.mode, "create_directory mode");
            }
            catch (error) {
                return errorResponse(400, error instanceof Error ? error.message : String(error));
            }
            out.push({ kind, path, ...(mode === undefined ? {} : { mode }) });
            continue;
        }
        if (kind === "set_mode") {
            let mode;
            try {
                mode = parseMode(mutation.mode, "set_mode mode");
            }
            catch (error) {
                return errorResponse(400, error instanceof Error ? error.message : String(error));
            }
            if (mode === undefined)
                return errorResponse(400, "set_mode requires mode");
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
function ownValue(obj, key) {
    if (obj == null || !Object.prototype.hasOwnProperty.call(obj, key))
        return undefined;
    return obj[key];
}
function writeItemPrecondition(write) {
    let raw = ownValue(write.precondition, "fingerprint");
    if (raw === undefined)
        raw = ownValue(write.precondition, "ifMatch");
    if (raw === undefined)
        raw = ownValue(write.precondition, "if_match");
    if (raw === undefined)
        raw = ownValue(write, "ifMatch");
    if (raw === undefined)
        raw = ownValue(write, "if_match");
    if (raw === undefined)
        return { present: false };
    if (raw !== null && typeof raw !== "string") {
        throw new Error("invalid write precondition: ifMatch/fingerprint must be a string or null");
    }
    return preconditionFromRaw(raw);
}
function writeItemExpectedFileId(write) {
    return parseExpectedFileId(ownValue(write.precondition, "expected_file_id"), "invalid write precondition: expected_file_id");
}
function conflictResponseFromStoreError(error, path) {
    const message = error instanceof Error ? error.message : String(error);
    if (message.includes("VFS_CONFLICT") ||
        message.includes("status=409") ||
        /\bconflict:/i.test(message)) {
        return errorResponse(409, `precondition failed for ${path}`);
    }
    return null;
}
function parseOptionalNonNegativeInteger(raw, name) {
    if (raw === null || raw.trim() === "")
        return null;
    const value = Number(raw);
    if (!Number.isSafeInteger(value) || value < 0) {
        return errorResponse(400, `${name} must be a non-negative integer`);
    }
    return value;
}
async function enforceFingerprintPrecondition(store, path, precondition) {
    if (!precondition.present)
        return null;
    const cur = await store.stat(path);
    const curHash = mutationFingerprint(cur);
    if (precondition.fingerprint === curHash)
        return null;
    // CAS mismatch -> 409 Conflict; the file is NOT touched (no clobber).
    return errorResponse(409, `precondition failed for ${path}`);
}
function mutationFingerprint(metadata) {
    if (metadata === null)
        return null;
    if (metadata.kind.toLowerCase().startsWith("sym")) {
        return typeof metadata.linkTarget === "string" && metadata.linkTarget !== ""
            ? `symlink:${(0, node_crypto_1.createHash)("sha256").update(metadata.linkTarget).digest("hex")}`
            : null;
    }
    return metadata.contentHash ?? null;
}
/** `VfsStorage` metadata `kind` is PascalCase; the wire uses lowercase kinds. */
function wireKind(kind) {
    const lower = kind.toLowerCase();
    if (lower.startsWith("dir"))
        return "directory";
    if (lower.startsWith("sym"))
        return "symlink";
    if (lower.startsWith("spec"))
        return "special";
    return "file";
}
function toRemoteMetadata(md) {
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
function toRemoteDirEntry(md) {
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
/** Cheap file-identity fingerprint for pinning ranged reads. Epoch millis is
 *  the canonical form on both sides — the Rust FUSE client mirrors this in
 *  `range_fingerprint` (sandbox/vmd/src/fuse/fs.rs); keep them identical. */
function rangeFingerprint(metadata) {
    const raw = metadata.updatedAt ?? null;
    const millis = raw === null ? -1 : Date.parse(raw);
    return `${Number(metadata.sizeBytes)}:${Number.isNaN(millis) ? -1 : millis}`;
}
function staleRangeResponse(path, currentFingerprint) {
    return new Response(`range fingerprint mismatch for ${path}`, {
        status: 412,
        headers: {
            "content-type": "text/plain",
            [RANGE_FINGERPRINT_HEADER]: currentFingerprint,
        },
    });
}
function parseRange(header, len) {
    if (header === null)
        return null;
    const m = /^bytes=(\d+)-(\d*)$/.exec(header.trim());
    if (m === null)
        return null;
    const start = Number(m[1]);
    const end = m[2] === "" ? len - 1 : Number(m[2]);
    if (Number.isNaN(start) || Number.isNaN(end) || start > end || start >= len)
        return null;
    return { start, end: Math.min(end, len - 1) };
}
function json(status, body) {
    return new Response(JSON.stringify(body), {
        status,
        headers: { "content-type": "application/json" },
    });
}
function errorResponse(status, message) {
    return new Response(message, { status, headers: { "content-type": "text/plain" } });
}
function randomToken() {
    // Rust FUSE clients deserialize owner_token as a UUID.
    return (0, node_crypto_1.randomUUID)();
}
