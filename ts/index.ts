// Chevalier — TypeScript ergonomic layer over the native (napi-rs) bindings.
// Adds: Zod-typed structured output, Zod tool schemas, `for await` streaming,
// typed errors, and the `agentic()` higher-order helper. Raw bindings live in
// ./native.js.

import * as native from "./native.js";
import { zodToJsonSchema } from "zod-to-json-schema";
import type { ZodType } from "zod";

export type {
  RunResult,
  ToolCallJs,
  ToolSchemaJs,
  StreamEvent,
  Message,
  MediaPartInput,
  GatewayOptions,
  ProviderConfigInput,
  AnthropicCacheConfig,
  CodexSubscriptionConfigInput,
  VfsMetadata,
  VfsObjectState,
  VfsWriteOptions,
} from "./native.js";

const VFS_ERR_PREFIX = /^VFS:\s+\[([A-Z0-9_]+) status=(\d{3})\]\s*([\s\S]*)$/;

function setErrorField(error: Error, field: string, value: unknown): void {
  try {
    Object.defineProperty(error, field, {
      value,
      enumerable: true,
      configurable: true,
      writable: true,
    });
  } catch {
    try {
      (error as Error & Record<string, unknown>)[field] = value;
    } catch {
      /* best effort */
    }
  }
}

function normalizeVfsNativeError(e: unknown): never {
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
const vfsStorageProto = native.VfsStorage.prototype as unknown as Record<PropertyKey, unknown>;
if (!vfsStorageProto[VFS_ERROR_PATCHED]) {
  for (const method of [
    "read",
    "write",
    "stat",
    "listDir",
    "mkdir",
    "createSymlink",
    "remove",
    "rmdir",
    "rename",
  ] as const) {
    const original = vfsStorageProto[method];
    if (typeof original === "function") {
      const originalFn = original as (
        this: native.VfsStorage,
        ...args: unknown[]
      ) => Promise<unknown>;
      vfsStorageProto[method] = function patchedVfsMethod(this: native.VfsStorage, ...args: unknown[]) {
        return Promise.resolve(originalFn.apply(this, args)).catch(normalizeVfsNativeError);
      };
    }
  }
  Object.defineProperty(vfsStorageProto, VFS_ERROR_PATCHED, { value: true });
}

export { McpClient, McpServer, VfsStorage, version } from "./native.js";
// The TS implementation of chevalier's VFS gateway SERVER (the missing third
// corner — the Rust server + Rust/TS clients already exist). A pure-Node
// `Request -> Response` handler that speaks the gateway wire protocol, backed by
// any `VfsStorage`. The bound `VfsStorage.gateway` client talks to it unchanged.
export { createVfsGatewayServer } from "./vfs-gateway-server.js";
export type { VfsGatewayServerOptions } from "./vfs-gateway-server.js";

/** Error thrown by Chevalier, carrying a machine-readable `code` and a
 *  `retryable` hint parsed from the engine. */
export class ChevalierError extends Error {
  readonly code: string;
  readonly retryable: boolean;
  /** Raw model text, when the failure was decoding structured output. */
  readonly output?: string;
  constructor(message: string, code = "ERROR", retryable = false, output?: string) {
    super(message);
    this.name = "ChevalierError";
    this.code = code;
    this.retryable = retryable;
    this.output = output;
  }
}

// Native errors carry `[CODE retryable=bool] message`; lift that into ChevalierError.
const ERR_PREFIX = /^\[([A-Z0-9_]+) retryable=(true|false)\]\s*([\s\S]*)$/;
function toChevalierError(e: unknown): unknown {
  if (e instanceof ChevalierError) return e;
  if (e instanceof Error) {
    const m = ERR_PREFIX.exec(e.message);
    if (m) return new ChevalierError(m[3], m[1], m[2] === "true");
  }
  return e;
}

function isZod(s: unknown): s is ZodType {
  return (
    !!s &&
    typeof (s as { parse?: unknown }).parse === "function" &&
    typeof (s as { safeParse?: unknown }).safeParse === "function"
  );
}

function toJsonSchema(s: ZodType | object): unknown {
  if (!isZod(s)) return s;
  // Zod v4 ships a native converter; prefer it. Fall back to zod-to-json-schema (v3).
  try {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const z = require("zod");
    if (z && typeof z.toJSONSchema === "function") return z.toJSONSchema(s);
  } catch {
    /* zod not resolvable here (v3, or ESM-only) — fall through */
  }
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return zodToJsonSchema(s as any) as unknown;
}

export interface RuntimeOptions {
  /** Provider model string, e.g. `anthropic:claude-3-5-sonnet` or
   *  `custom-openai:my-model@server_url=http://host:port/v1/chat/completions`. */
  model?: string;
  apiKey?: string;
}

export interface RunArgs<T = unknown> {
  prompt?: string;
  system?: string;
  temperature?: number;
  topP?: number;
  maxTokens?: number;
  model?: string;
  apiKey?: string;
  /** Zod schema (gives a typed, validated `value`) or a raw JSON Schema. */
  output?: ZodType<T> | object;
  outputType?: string;
  history?: native.Message[];
  timeoutMs?: number;
}

export interface ToolDef {
  name: string;
  description?: string;
  /** Zod schema or raw JSON Schema describing the tool's args. */
  schema: ZodType | object;
  /** Async handler. Non-string returns are JSON-stringified. Omit for a
   *  schema-only (host-dispatched) tool. */
  handler?: (args: any) => unknown | Promise<unknown>;
}

export type ProviderRateLimitScope = "session" | "subscription";

export interface ProviderRateLimit {
  scope: ProviderRateLimitScope;
  usedPercent: number;
  windowMinutes: number;
  resetsAtEpochSec: number;
}

export interface RateLimitsStreamEvent {
  type: "rateLimits";
  data: ProviderRateLimit[];
}

/** Result of `run`, with an optional decoded `value` when an output schema is given. */
export type TypedRunResult<T> = native.RunResult & { value?: T };

/** A stream event, with an optional decoded `value` on the `complete` event when
 *  an output schema was provided. Codex subscription streams may also emit
 *  `rateLimits` with `ProviderRateLimit[]` in `data`. */
export type TypedStreamEvent<T> = native.StreamEvent & { value?: T };

/** The Chevalier agent runtime. */
export class Runtime {
  /** @internal access to the raw napi runtime */
  readonly native: native.Runtime;

  constructor(options?: RuntimeOptions) {
    this.native = new native.Runtime(options);
  }

  /** Non-streaming inference. Pass `output` (Zod) to get a typed, validated `value`. */
  async run<T = unknown>(args: RunArgs<T> = {}): Promise<TypedRunResult<T>> {
    const { output, ...rest } = args;
    const outputSchema = output ? toJsonSchema(output) : undefined;
    let res: TypedRunResult<T>;
    try {
      res = (await this.native.run({ ...rest, outputSchema })) as TypedRunResult<T>;
    } catch (e) {
      throw toChevalierError(e);
    }
    if (output && isZod(output)) {
      res.value = decodeOutput(output as ZodType<T>, res.text);
    }
    return res;
  }

  /** Streaming inference as an async iterator: `for await (const ev of rt.runStream(...))`.
   *  When `output` is given, the `complete` event carries a decoded `value`.
   *  Always closes the underlying stream on exit (including early `break`). */
  async *runStream<T = unknown>(
    args: RunArgs<T> = {},
  ): AsyncGenerator<TypedStreamEvent<T>, void, void> {
    const { output, ...rest } = args;
    const outputSchema = output ? toJsonSchema(output) : undefined;
    const handle = await this.native.runStream({ ...rest, outputSchema });
    let text = "";
    try {
      for (;;) {
        let ev: native.StreamEvent | null;
        try {
          ev = await handle.next();
        } catch (e) {
          throw toChevalierError(e);
        }
        if (ev == null) return;
        if (ev.type === "content" && ev.text) text += ev.text;
        if (ev.type === "complete" && output && isZod(output)) {
          try {
            (ev as TypedStreamEvent<T>).value = (output as ZodType<T>).parse(JSON.parse(text));
          } catch {
            /* leave value unset; don't abort the stream on decode failure */
          }
        }
        yield ev as TypedStreamEvent<T>;
      }
    } finally {
      handle.close();
    }
  }

  /** Register a tool. With `handler`, the engine runs it on `executeToolCall`;
   *  without, it's schema-only (the model can call it; you dispatch it). */
  async tool(def: ToolDef): Promise<void> {
    const schema = toJsonSchema(def.schema);
    if (def.handler) {
      const h = def.handler;
      await this.native.tool(def.name, def.description ?? "", schema, async (a: any) => {
        const r = await h(a);
        return typeof r === "string" ? r : JSON.stringify(r);
      });
    } else {
      await this.native.registerToolSchema(def.name, def.description ?? "", schema);
    }
  }

  async executeToolCall(toolName: string, args: unknown): Promise<string> {
    try {
      return await this.native.executeToolCall(toolName, args);
    } catch (e) {
      throw toChevalierError(e);
    }
  }
  getToolSchemas(): Promise<native.ToolSchemaJs[]> {
    return this.native.getToolSchemas();
  }
  setSystemMessages(messages: native.Message[]): Promise<void> {
    return this.native.setSystemMessages(messages);
  }
  setDefaultPrompt(prompt: string): Promise<void> {
    return this.native.setDefaultPrompt(prompt);
  }
  setProviderConfig(config: native.ProviderConfigInput): Promise<void> {
    return this.native.setProviderConfig(config);
  }
  rawResponse(): Promise<string> {
    return this.native.rawResponse();
  }
  reasoning(): Promise<string> {
    return this.native.reasoning();
  }
  reasoningSegments(): Promise<unknown> {
    return this.native.reasoningSegments();
  }
  /** Connect to an MCP server and register its tools (auto-detected transport). */
  mcp(uri: string): Promise<void> {
    return this.native.mcp(uri);
  }
  /** Like `mcp`, but namespaces tools as `{label}_{tool}`. */
  mcpAs(uri: string, label: string): Promise<void> {
    return this.native.mcpAs(uri, label);
  }
  /** Release tool handlers so the Runtime can be GC'd. Important when a tool
   *  handler captures the Runtime (the napi_ref ↔ closure cycle otherwise leaks
   *  it). Call when done with a short-lived (e.g. per-request) Runtime. */
  dispose(): Promise<void> {
    return this.native.dispose();
  }
}

/** Parse `text` as JSON and validate against `schema`, throwing a typed
 *  ChevalierError (code `OUTPUT_PARSE`) on malformed/empty JSON instead of a
 *  bare SyntaxError. Zod validation errors propagate as ZodError. */
function decodeOutput<T>(schema: ZodType<T>, text: string): T {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw new ChevalierError(
      `model did not return valid JSON for structured output (got ${text.length} char(s))`,
      "OUTPUT_PARSE",
      false,
      text,
    );
  }
  return schema.parse(parsed);
}

/** "An agent is just a function." Wraps a function so a fresh `Runtime` is
 *  created per call and passed as the last argument. */
export function agentic<A extends unknown[], R>(
  config: RuntimeOptions,
  fn: (...argsAndRuntime: [...A, Runtime]) => R,
): (...args: A) => R {
  return (...args: A) => fn(...([...args, new Runtime(config)] as [...A, Runtime]));
}
