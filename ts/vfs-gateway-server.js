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
//                                           precondition-fingerprint header -> 409
//   - DELETE {owner}/file?path=&return_metadata=true -> 200 {previous}
//   - PUT/DELETE {owner}/dir?path=       -> 2xx
//   - POST {owner}/rename?from=&to=&return_metadata=true -> 200 {previous,current}
//   - POST/DELETE {owner}/lease          -> 200 {resource_key,owner_token} / 2xx
//   - POST {owner}/{metadata-many,read-many,write-many} -> batch (per-path loop)
//   DTOs are snake_case; `kind` is exactly "file" | "directory"; errors map
//   404->NotFound, 400->BadRequest, 409->Conflict (vfs/src/gateway.rs:1016).
const node_crypto_1 = require("node:crypto");
const DEFAULT_ROUTE_PREFIX = "/internal/chevalier/vfs";
const PRECONDITION_FINGERPRINT_HEADER = "x-chevalier-vfs-precondition-fingerprint";
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
                let buf;
                try {
                    buf = await store.read(relPath);
                }
                catch {
                    return errorResponse(404, `not found: ${relPath}`);
                }
                const range = parseRange(req.headers.get("range"), buf.length);
                if (range !== null) {
                    const slice = buf.subarray(range.start, range.end + 1);
                    return new Response(asBody(slice), {
                        status: 206,
                        headers: {
                            "content-type": "application/octet-stream",
                            "content-range": `bytes ${range.start}-${range.end}/${buf.length}`,
                        },
                    });
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
                let entries;
                try {
                    entries = await store.listDir(dir);
                }
                catch {
                    return errorResponse(404, `not found: ${dir}`);
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
            // ---- single-file mutations -----------------------------------------
            if (method === "PUT" && op === "file") {
                if (isGitExcludedPath(relPath))
                    return errorResponse(400, `excluded path: ${relPath}`);
                const precond = req.headers.get(PRECONDITION_FINGERPRINT_HEADER);
                if (precond !== null && precond !== "") {
                    const cur = await store.stat(relPath);
                    const curHash = cur?.contentHash ?? null;
                    if (precond !== curHash) {
                        // CAS mismatch -> 409 Conflict; the file is NOT touched (no clobber).
                        return errorResponse(409, `precondition failed for ${relPath}`);
                    }
                }
                const body = Buffer.from(await req.arrayBuffer());
                const res = (await store.write(relPath, body));
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
                let previous = null;
                if (q.get("return_metadata") === "true") {
                    const cur = await store.stat(relPath);
                    previous = cur === null ? null : toRemoteMetadata(cur);
                }
                try {
                    await store.remove(relPath);
                }
                catch {
                    /* idempotent delete: absent is fine */
                }
                return json(200, { previous });
            }
            if (method === "PUT" && op === "dir") {
                if (isGitExcludedPath(relPath))
                    return errorResponse(400, `excluded path: ${relPath}`);
                await store.mkdir(relPath);
                return new Response(null, { status: 204 });
            }
            if (method === "DELETE" && op === "dir") {
                if (isGitExcludedPath(relPath))
                    return errorResponse(400, `excluded path: ${relPath}`);
                try {
                    await store.rmdir(relPath);
                }
                catch {
                    /* idempotent */
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
                    catch {
                        entries.push(null);
                    }
                }
                return json(200, { entries });
            }
            if (method === "POST" && op === "write-many") {
                const body = (await req.json());
                // Atomic-ish: check all preconditions first, then apply. Any mismatch -> 409.
                for (const w of body.writes) {
                    const path = normalizePath(w.path);
                    if (isGitExcludedPath(path))
                        return errorResponse(400, `excluded path: ${path}`);
                    const fp = w.precondition?.fingerprint ?? null;
                    if (fp !== null) {
                        const cur = await store.stat(path);
                        if (fp !== (cur?.contentHash ?? null)) {
                            return errorResponse(409, `precondition failed for ${w.path}`);
                        }
                    }
                }
                const results = [];
                for (const w of body.writes) {
                    const p = normalizePath(w.path);
                    const cur = await store.stat(p);
                    const prev = cur?.contentHash ?? null;
                    const res = (await store.write(p, Buffer.from(w.body)));
                    const hash = res.content_hash ?? res.contentHash ?? "";
                    results.push({ path: p, content_hash: hash, previous_hash: prev, changed: res.changed ?? prev !== hash });
                }
                return json(200, { results });
            }
            return errorResponse(404, `unhandled route: ${method} ${op}`);
        }
        catch (e) {
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
/** `VfsStorage` metadata `kind` is "File"|"Directory"; the wire wants "file"|"directory". */
function wireKind(kind) {
    return kind.toLowerCase().startsWith("dir") ? "directory" : "file";
}
function toRemoteMetadata(md) {
    return {
        kind: wireKind(md.kind),
        size_bytes: Number(md.sizeBytes),
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
