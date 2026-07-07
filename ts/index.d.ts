import * as native from "./native.js";
import type { ZodType } from "zod";
export type { RunResult, ToolCallJs, ToolSchemaJs, StreamEvent, Message, MediaPartInput, GatewayOptions, ProviderConfigInput, AnthropicCacheConfig, CodexSubscriptionConfigInput, VfsMetadata, VfsObjectState, VfsWriteOptions, } from "./native.js";
export { McpClient, McpServer, VfsStorage, version } from "./native.js";
export { createVfsGatewayServer } from "./vfs-gateway-server.js";
export type { VfsGatewayServerOptions } from "./vfs-gateway-server.js";
/** Error thrown by Chevalier, carrying a machine-readable `code` and a
 *  `retryable` hint parsed from the engine. */
export declare class ChevalierError extends Error {
    readonly code: string;
    readonly retryable: boolean;
    /** Raw model text, when the failure was decoding structured output. */
    readonly output?: string;
    constructor(message: string, code?: string, retryable?: boolean, output?: string);
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
export type TypedRunResult<T> = native.RunResult & {
    value?: T;
};
/** A stream event, with an optional decoded `value` on the `complete` event when
 *  an output schema was provided. Codex subscription streams may also emit
 *  `rateLimits` with `ProviderRateLimit[]` in `data`. */
export type TypedStreamEvent<T> = native.StreamEvent & {
    value?: T;
};
/** The Chevalier agent runtime. */
export declare class Runtime {
    /** @internal access to the raw napi runtime */
    readonly native: native.Runtime;
    constructor(options?: RuntimeOptions);
    /** Non-streaming inference. Pass `output` (Zod) to get a typed, validated `value`. */
    run<T = unknown>(args?: RunArgs<T>): Promise<TypedRunResult<T>>;
    /** Streaming inference as an async iterator: `for await (const ev of rt.runStream(...))`.
     *  When `output` is given, the `complete` event carries a decoded `value`.
     *  Always closes the underlying stream on exit (including early `break`). */
    runStream<T = unknown>(args?: RunArgs<T>): AsyncGenerator<TypedStreamEvent<T>, void, void>;
    /** Register a tool. With `handler`, the engine runs it on `executeToolCall`;
     *  without, it's schema-only (the model can call it; you dispatch it). */
    tool(def: ToolDef): Promise<void>;
    executeToolCall(toolName: string, args: unknown): Promise<string>;
    getToolSchemas(): Promise<native.ToolSchemaJs[]>;
    setSystemMessages(messages: native.Message[]): Promise<void>;
    setDefaultPrompt(prompt: string): Promise<void>;
    setProviderConfig(config: native.ProviderConfigInput): Promise<void>;
    rawResponse(): Promise<string>;
    reasoning(): Promise<string>;
    reasoningSegments(): Promise<unknown>;
    /** Connect to an MCP server and register its tools (auto-detected transport). */
    mcp(uri: string): Promise<void>;
    /** Like `mcp`, but namespaces tools as `{label}_{tool}`. */
    mcpAs(uri: string, label: string): Promise<void>;
    /** Release tool handlers so the Runtime can be GC'd. Important when a tool
     *  handler captures the Runtime (the napi_ref ↔ closure cycle otherwise leaks
     *  it). Call when done with a short-lived (e.g. per-request) Runtime. */
    dispose(): Promise<void>;
}
/** "An agent is just a function." Wraps a function so a fresh `Runtime` is
 *  created per call and passed as the last argument. */
export declare function agentic<A extends unknown[], R>(config: RuntimeOptions, fn: (...argsAndRuntime: [...A, Runtime]) => R): (...args: A) => R;
