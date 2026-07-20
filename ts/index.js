"use strict";
// Chevalier — TypeScript ergonomic layer over the native (napi-rs) bindings.
// Adds: Zod-typed structured output, Zod tool schemas, `for await` streaming,
// typed errors, and the `agentic()` higher-order helper. Raw bindings live in
// ./native.js.
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.Runtime = exports.ChevalierError = exports.createVfsGatewayServer = exports.version = exports.VfsStorage = exports.McpServer = exports.McpClient = void 0;
exports.agentic = agentic;
const native = __importStar(require("./native.js"));
const zod_to_json_schema_1 = require("zod-to-json-schema");
const VFS_ERR_PREFIX = /^VFS:\s+\[([A-Z0-9_]+) status=(\d{3})\]\s*([\s\S]*)$/;
function setErrorField(error, field, value) {
    try {
        Object.defineProperty(error, field, {
            value,
            enumerable: true,
            configurable: true,
            writable: true,
        });
    }
    catch {
        try {
            error[field] = value;
        }
        catch {
            /* best effort */
        }
    }
}
function normalizeVfsNativeError(e) {
    if (e instanceof Error) {
        const match = VFS_ERR_PREFIX.exec(e.message);
        if (match) {
            const status = Number(match[2]);
            setErrorField(e, "code", match[1]);
            setErrorField(e, "status", status);
            setErrorField(e, "statusCode", status);
        }
    }
    throw e;
}
const VFS_ERROR_PATCHED = Symbol.for("chevalier.vfs.errorPatch");
const vfsStorageProto = native.VfsStorage.prototype;
if (!vfsStorageProto[VFS_ERROR_PATCHED]) {
    for (const method of [
        "read",
        "write",
        "stat",
        "listDir",
        "mkdir",
        "createSymlink",
        "createHardLink",
        "findHardLinkAlias",
        "remove",
        "rmdir",
        "rename",
        "applyNamespaceBatch",
        "writeMany",
    ]) {
        const original = vfsStorageProto[method];
        if (typeof original === "function") {
            const originalFn = original;
            vfsStorageProto[method] = function patchedVfsMethod(...args) {
                return Promise.resolve(originalFn.apply(this, args)).catch(normalizeVfsNativeError);
            };
        }
    }
    Object.defineProperty(vfsStorageProto, VFS_ERROR_PATCHED, { value: true });
}
var native_js_1 = require("./native.js");
Object.defineProperty(exports, "McpClient", { enumerable: true, get: function () { return native_js_1.McpClient; } });
Object.defineProperty(exports, "McpServer", { enumerable: true, get: function () { return native_js_1.McpServer; } });
Object.defineProperty(exports, "VfsStorage", { enumerable: true, get: function () { return native_js_1.VfsStorage; } });
Object.defineProperty(exports, "version", { enumerable: true, get: function () { return native_js_1.version; } });
// The TS implementation of chevalier's VFS gateway SERVER (the missing third
// corner — the Rust server + Rust/TS clients already exist). A pure-Node
// `Request -> Response` handler that speaks the gateway wire protocol, backed by
// any `VfsStorage`. The bound `VfsStorage.gateway` client talks to it unchanged.
var vfs_gateway_server_js_1 = require("./vfs-gateway-server.js");
Object.defineProperty(exports, "createVfsGatewayServer", { enumerable: true, get: function () { return vfs_gateway_server_js_1.createVfsGatewayServer; } });
/** Error thrown by Chevalier, carrying a machine-readable `code` and a
 *  `retryable` hint parsed from the engine. */
class ChevalierError extends Error {
    constructor(message, code = "ERROR", retryable = false, output) {
        super(message);
        this.name = "ChevalierError";
        this.code = code;
        this.retryable = retryable;
        this.output = output;
    }
}
exports.ChevalierError = ChevalierError;
// Native errors carry `[CODE retryable=bool] message`; lift that into ChevalierError.
const ERR_PREFIX = /^\[([A-Z0-9_]+) retryable=(true|false)\]\s*([\s\S]*)$/;
function toChevalierError(e) {
    if (e instanceof ChevalierError)
        return e;
    if (e instanceof Error) {
        const m = ERR_PREFIX.exec(e.message);
        if (m)
            return new ChevalierError(m[3], m[1], m[2] === "true");
    }
    return e;
}
function isZod(s) {
    return (!!s &&
        typeof s.parse === "function" &&
        typeof s.safeParse === "function");
}
function toJsonSchema(s) {
    if (!isZod(s))
        return s;
    // Zod v4 ships a native converter; prefer it. Fall back to zod-to-json-schema (v3).
    try {
        // eslint-disable-next-line @typescript-eslint/no-var-requires
        const z = require("zod");
        if (z && typeof z.toJSONSchema === "function")
            return z.toJSONSchema(s);
    }
    catch {
        /* zod not resolvable here (v3, or ESM-only) — fall through */
    }
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return (0, zod_to_json_schema_1.zodToJsonSchema)(s);
}
/** The Chevalier agent runtime. */
class Runtime {
    constructor(options) {
        this.native = new native.Runtime(options);
    }
    /** Non-streaming inference. Pass `output` (Zod) to get a typed, validated `value`. */
    async run(args = {}) {
        const { output, ...rest } = args;
        const outputSchema = output ? toJsonSchema(output) : undefined;
        let res;
        try {
            res = (await this.native.run({ ...rest, outputSchema }));
        }
        catch (e) {
            throw toChevalierError(e);
        }
        if (output && isZod(output)) {
            res.value = decodeOutput(output, res.text);
        }
        return res;
    }
    /** Streaming inference as an async iterator: `for await (const ev of rt.runStream(...))`.
     *  When `output` is given, the `complete` event carries a decoded `value`.
     *  Always closes the underlying stream on exit (including early `break`). */
    async *runStream(args = {}) {
        const { output, ...rest } = args;
        const outputSchema = output ? toJsonSchema(output) : undefined;
        const handle = await this.native.runStream({ ...rest, outputSchema });
        let text = "";
        try {
            for (;;) {
                let ev;
                try {
                    ev = await handle.next();
                }
                catch (e) {
                    throw toChevalierError(e);
                }
                if (ev == null)
                    return;
                if (ev.type === "content" && ev.text)
                    text += ev.text;
                if (ev.type === "complete" && output && isZod(output)) {
                    try {
                        ev.value = output.parse(JSON.parse(text));
                    }
                    catch {
                        /* leave value unset; don't abort the stream on decode failure */
                    }
                }
                yield ev;
            }
        }
        finally {
            handle.close();
        }
    }
    /** Register a tool. With `handler`, the engine runs it on `executeToolCall`;
     *  without, it's schema-only (the model can call it; you dispatch it). */
    async tool(def) {
        const schema = toJsonSchema(def.schema);
        if (def.handler) {
            const h = def.handler;
            await this.native.tool(def.name, def.description ?? "", schema, async (a) => {
                const r = await h(a);
                return typeof r === "string" ? r : JSON.stringify(r);
            });
        }
        else {
            await this.native.registerToolSchema(def.name, def.description ?? "", schema);
        }
    }
    async executeToolCall(toolName, args) {
        try {
            return await this.native.executeToolCall(toolName, args);
        }
        catch (e) {
            throw toChevalierError(e);
        }
    }
    getToolSchemas() {
        return this.native.getToolSchemas();
    }
    setSystemMessages(messages) {
        return this.native.setSystemMessages(messages);
    }
    setDefaultPrompt(prompt) {
        return this.native.setDefaultPrompt(prompt);
    }
    setProviderConfig(config) {
        return this.native.setProviderConfig(config);
    }
    rawResponse() {
        return this.native.rawResponse();
    }
    reasoning() {
        return this.native.reasoning();
    }
    reasoningSegments() {
        return this.native.reasoningSegments();
    }
    /** Connect to an MCP server and register its tools (auto-detected transport). */
    mcp(uri) {
        return this.native.mcp(uri);
    }
    /** Like `mcp`, but namespaces tools as `{label}_{tool}`. */
    mcpAs(uri, label) {
        return this.native.mcpAs(uri, label);
    }
    /** Release tool handlers so the Runtime can be GC'd. Important when a tool
     *  handler captures the Runtime (the napi_ref ↔ closure cycle otherwise leaks
     *  it). Call when done with a short-lived (e.g. per-request) Runtime. */
    dispose() {
        return this.native.dispose();
    }
}
exports.Runtime = Runtime;
/** Parse `text` as JSON and validate against `schema`, throwing a typed
 *  ChevalierError (code `OUTPUT_PARSE`) on malformed/empty JSON instead of a
 *  bare SyntaxError. Zod validation errors propagate as ZodError. */
function decodeOutput(schema, text) {
    let parsed;
    try {
        parsed = JSON.parse(text);
    }
    catch {
        throw new ChevalierError(`model did not return valid JSON for structured output (got ${text.length} char(s))`, "OUTPUT_PARSE", false, text);
    }
    return schema.parse(parsed);
}
/** "An agent is just a function." Wraps a function so a fresh `Runtime` is
 *  created per call and passed as the last argument. */
function agentic(config, fn) {
    return (...args) => fn(...[...args, new Runtime(config)]);
}
