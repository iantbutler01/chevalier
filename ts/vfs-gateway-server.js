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
const IF_MATCH_HEADER = "if-match";
const EXECUTABLE_HEADER = "x-chevalier-vfs-executable";
const EXPECTED_CONTENT_HASH_HEADER = "x-chevalier-vfs-expected-content-sha256";
const STREAM_UPLOAD_HEADER = "x-chevalier-vfs-stream-upload";
/** Build a WHATWG `(Request) => Promise<Response>` handler that serves chevalier's
 *  VFS gateway protocol, delegating storage to `resolveStore(ownerId)`. */
function createVfsGatewayServer(opts) {
    const prefix = opts.routePrefix ?? DEFAULT_ROUTE_PREFIX;
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
            const store = await opts.resolveStore(ownerId);
            const q = url.searchParams;
            const method = req.method.toUpperCase();
            const relPath = normalizePath(q.get("path"));
            // ---- reads ----------------------------------------------------------
            if (method === "GET" && op === "stat") {
                if (isGitExcludedPath(relPath))
                    return errorResponse(404, `not found: ${relPath}`);
                const md = await store.stat(relPath);
                if (md === null)
                    return errorResponse(404, `not found: ${relPath}`);
                return json(200, toRemoteMetadata(md));
            }
            if (method === "GET" && op === "file/raw") {
                if (isGitExcludedPath(relPath))
                    return errorResponse(404, `not found: ${relPath}`);
                const requestedRange = req.headers.get("range");
                if (requestedRange !== null) {
                    let metadata;
                    try {
                        metadata = await store.stat(relPath);
                    }
                    catch (error) {
                        if (isVfsNotFoundError(error))
                            return errorResponse(404, `not found: ${relPath}`);
                        throw error;
                    }
                    if (metadata === null)
                        return errorResponse(404, `not found: ${relPath}`);
                    const size = Number(metadata.sizeBytes);
                    const range = parseRange(requestedRange, size);
                    if (range === null)
                        return errorResponse(416, `invalid range for ${relPath}`);
                    const length = range.end - range.start + 1;
                    const streamingStore = store;
                    const slice = typeof streamingStore.readRange === "function"
                        ? await streamingStore.readRange(relPath, BigInt(range.start), length)
                        : (await store.read(relPath)).subarray(range.start, range.end + 1);
                    return new Response(asBody(slice), {
                        status: 206,
                        headers: {
                            "content-type": "application/octet-stream",
                            "content-range": `bytes ${range.start}-${range.end}/${size}`,
                            "content-length": String(slice.byteLength),
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
                if (isGitExcludedPath(relPath))
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
                    .filter((entry) => !isGitExcludedPath(entry.path))
                    .map(toRemoteDirEntry)
                    .filter((e) => (nameLike === null || e.name.includes(nameLike)))
                    .filter((e) => (nameNotLike === null || !e.name.includes(nameNotLike)));
                return json(200, out);
            }
            // ---- leases (mutations acquire/release one; we issue a synthetic grant) --
            if (op === "lease" && method === "POST") {
                const body = (await req.json().catch(() => ({})));
                const leasePath = normalizePath(typeof body.path === "string" ? body.path : relPath);
                return json(200, { resource_key: `rk:${ownerId}:${leasePath}`, owner_token: randomToken() });
            }
            if (op === "lease" && method === "DELETE") {
                return new Response(null, { status: 204 });
            }
            if (method === "POST" && op === "namespace-many") {
                const body = (await req.json());
                const mutations = normalizeNamespaceMutations(body.mutations);
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
                if (isGitExcludedPath(relPath))
                    return errorResponse(400, `excluded path: ${relPath}`);
                const precondition = requestPrecondition(req, q);
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
                        const options = { ...preconditionOptions(precondition), ...writeOptions };
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
                        ...preconditionOptions(precondition),
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
                if (isGitExcludedPath(relPath))
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
                if (isGitExcludedPath(relPath))
                    return errorResponse(400, `excluded path: ${relPath}`);
                await store.mkdir(relPath);
                return new Response(null, { status: 204 });
            }
            if (method === "PUT" && op === "symlink") {
                if (isGitExcludedPath(relPath))
                    return errorResponse(400, `excluded path: ${relPath}`);
                const target = q.get("target");
                if (target === null || target === "")
                    return errorResponse(400, "symlink requires target");
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
                if (isGitExcludedPath(relPath))
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
            if (method === "POST" && op === "rename") {
                const from = normalizePath(q.get("from"));
                const to = normalizePath(q.get("to"));
                if (from === "" || to === "")
                    return errorResponse(400, "rename requires from + to");
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
                const { paths } = (await req.json());
                const entries = [];
                for (const p of paths) {
                    const path = normalizePath(p);
                    const md = isGitExcludedPath(path) ? null : await store.stat(path);
                    entries.push(md === null ? null : toRemoteMetadata(md));
                }
                return json(200, { entries });
            }
            if (method === "POST" && op === "read-many") {
                const { paths } = (await req.json());
                const entries = [];
                for (const p of paths) {
                    const path = normalizePath(p);
                    if (isGitExcludedPath(path)) {
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
                const body = (await req.json());
                const streamingStore = store;
                if (typeof streamingStore.writeMany === "function") {
                    const writes = body.writes.map((write) => {
                        const path = normalizePath(write.path);
                        if (isGitExcludedPath(path))
                            throw Object.assign(new Error(`excluded path: ${path}`), {
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
                for (const w of body.writes) {
                    const path = normalizePath(w.path);
                    if (isGitExcludedPath(path))
                        return errorResponse(400, `excluded path: ${path}`);
                    const failed = await enforceFingerprintPrecondition(store, path, writeItemPrecondition(w));
                    if (failed !== null)
                        return failed;
                }
                const results = [];
                for (const w of body.writes) {
                    const p = normalizePath(w.path);
                    const cur = await store.stat(p);
                    const prev = cur?.contentHash ?? null;
                    const precondition = writeItemPrecondition(w);
                    let res;
                    try {
                        res = (await store.write(p, Buffer.from(w.body), preconditionOptions(precondition)));
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
function isGitExcludedPath(path) {
    return path
        .replace(/\\/g, "/")
        .split("/")
        .filter((part) => part !== "" && part !== ".")
        .some((part) => part === ".git");
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
function requestWriteOptions(req) {
    const raw = req.headers.get(EXECUTABLE_HEADER);
    if (raw === null)
        return {};
    if (raw === "true")
        return { executable: true };
    if (raw === "false")
        return { executable: false };
    throw Object.assign(new Error(`${EXECUTABLE_HEADER} must be true or false`), {
        code: "VFS_BAD_REQUEST",
        status: 400,
    });
}
function preconditionOptions(precondition) {
    if (!precondition.present)
        return undefined;
    return { ifMatch: precondition.fingerprint };
}
function normalizeNamespaceMutations(value) {
    if (!Array.isArray(value))
        return errorResponse(400, "namespace-many requires mutations[]");
    if (value.length > 4096)
        return errorResponse(400, "namespace-many accepts at most 4096 mutations");
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
            if (isGitExcludedPath(from) || isGitExcludedPath(to))
                return errorResponse(400, "excluded rename path");
            out.push({ kind, from, to });
            continue;
        }
        const path = normalizePath(typeof mutation.path === "string" ? mutation.path : null);
        if (path === "" || isGitExcludedPath(path))
            return errorResponse(400, `invalid namespace path: ${path}`);
        if (kind === "delete_file") {
            const precondition = writeItemPrecondition(mutation);
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
    return {
        kind: wireKind(md.kind),
        size_bytes: Number(md.sizeBytes),
        executable: md.executable ?? false,
        link_target: md.linkTarget ?? null,
        content_hash: md.contentHash ?? null,
        updated_at: md.updatedAt ?? null,
    };
}
function toRemoteDirEntry(md) {
    const name = md.path.split("/").filter((s) => s !== "").pop() ?? md.path;
    return {
        name,
        kind: wireKind(md.kind),
        size_bytes: Number(md.sizeBytes),
        executable: md.executable ?? false,
        link_target: md.linkTarget ?? null,
        content_hash: md.contentHash ?? null,
        updated_at: md.updatedAt ?? null,
    };
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
