import * as native from "./native.js";
import type { ZodType } from "zod";
export * from "./programmatic";
export type { RunResult, ToolCallJs, ToolSchemaJs, StreamEvent, Message, MediaPartInput, GatewayOptions, ProviderConfigInput, AnthropicCacheConfig, CodexSubscriptionConfigInput, VfsMetadata, VfsObjectState, VfsWriteOptions, } from "./native.js";
export type McpClientConfig = {
    transport: "http" | "websocket";
    url: string;
    headers?: Record<string, string>;
} | {
    transport: "stdio";
    command: string;
    args?: string[];
    env?: Record<string, string>;
    cwd?: string;
};
export { McpClient, McpServer, VfsStorage, 
/** Incremental VFS content hash (BLAKE3). Must be used wherever a caller needs
 *  the same digest the storage layer computes. */
VfsContentHasher, 
/** One-shot VFS content hash (BLAKE3). */
vfsContentHash, version, } from "./native.js";
export { createVfsGatewayServer } from "./vfs-gateway-server.js";
export type { VfsAdvisoryLock, VfsAdvisoryLockKind, VfsAdvisoryLockNamespace, VfsAdvisoryLockStateStore, VfsAdvisoryLockTransactionResult, VfsGatewayServerOptions, } from "./vfs-gateway-server.js";
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
     *  `custom-openai:my-model@server_url=http://host:port/v1/chat/completions`.
     *  OpenRouter chat completions accepts `@provider=fireworks,deepseek,baseten` for an
     *  ordered allowlist. A single provider disables fallbacks. */
    model?: string;
    apiKey?: string;
}
export interface RunArgs<T = unknown> {
    signal?: AbortSignal;
    onControl?: (control: StreamControl) => void;
    responses?: {
        websocket?: boolean;
        compactionThreshold?: number;
    };
    prompt?: string;
    system?: string;
    temperature?: number;
    topP?: number;
    /** Chat Completions / Responses: false retains schemas but disables native tool calls. */
    allowToolCalls?: boolean;
    maxTokens?: number;
    model?: string;
    apiKey?: string;
    /** Zod schema (gives a typed, validated `value`) or a raw JSON Schema. */
    output?: ZodType<T> | object;
    outputType?: string;
    history?: native.Message[];
    timeoutMs?: number;
    /** Responses API response to continue from. */
    previousResponseId?: string;
}
export interface ToolDef {
    async?: boolean;
    name: string;
    description?: string;
    /** Zod schema or raw JSON Schema describing the tool's args. */
    schema: ZodType | object;
    /** Async handler. Non-string returns are JSON-stringified. Omit for a
     *  schema-only (host-dispatched) tool. */
    handler?: (args: any) => unknown | Promise<unknown>;
}
export interface ClaudeSessionConfig {
    model: string;
    systemPrompt?: string;
    effort?: "low" | "medium" | "high" | "xhigh" | "max" | "ultra";
    resume?: {
        sessionId: string;
        cwd: string;
    };
    cwd?: string;
    cliPath?: string;
    clientApp?: string;
    serverName?: string;
    /** Surface every call (including handler-backed and MCP tools) as a `toolCall` event for the host to gate and run. */
    hostDispatchAll?: boolean;
    idleTimeoutMs?: number;
    maxTurns?: number;
}
export interface ClaudeToolCall {
    toolUseId: string;
    toolName: string;
    args: unknown;
}
export type ClaudeToolContent = {
    type: "text";
    text: string;
} | {
    type: "image";
    dataBase64: string;
    mimeType: string;
};
export interface ClaudeToolOutput {
    content: ClaudeToolContent[];
    isError?: boolean;
}
export type ClaudeSessionEvent = {
    type: "init";
    sessionId: string;
    cwd: string;
    model: string;
    cliVersion: string;
} | {
    type: "textDelta";
    text: string;
} | {
    type: "thinkingDelta";
    text: string;
} | {
    type: "assistantMessage";
    text: string;
} | {
    type: "toolCall";
    callId: string;
    call: ClaudeToolCall;
} | {
    type: "toolExecuted";
    call: ClaudeToolCall;
    output: ClaudeToolOutput;
} | {
    type: "toolCancelled";
    callId: string;
} | {
    type: "rateLimits";
    data: ProviderRateLimit[];
} | {
    type: "apiRetry";
    attempt: number;
    delayMs: number;
    error: string;
} | {
    type: "turnComplete";
    usage: unknown;
    listPriceUsd: number | null;
    numTurns: number;
    isError: boolean;
    subtype: string;
    result: string | null;
};
export type ClaudeSubscriptionStatus = {
    status: "ready";
    email: string | null;
    subscription_type: string | null;
} | {
    status: "notLoggedIn";
} | {
    status: "cliNotFound";
    probed: string[];
} | {
    status: "error";
    message: string;
};
export interface ClaudeExitSummary {
    status: string;
    stderr_tail: string;
}
export declare class ClaudeSession {
    readonly native: native.ClaudeSession;
    constructor(native: native.ClaudeSession);
    next(): Promise<ClaudeSessionEvent | null>;
    send(turn: {
        text: string;
        images?: native.MediaPartInput[];
    }): Promise<void>;
    respondTool(callId: string, output: ClaudeToolOutput): Promise<void>;
    interrupt(): Promise<void>;
    close(): Promise<ClaudeExitSummary>;
}
export declare function claudeSubscriptionStatus(cliPath?: string): Promise<ClaudeSubscriptionStatus>;
export declare function events(session: ClaudeSession, { signal }?: {
    signal?: AbortSignal;
}): AsyncGenerator<ClaudeSessionEvent>;
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
export interface StreamControl {
    steer(input: string | object[]): Promise<void>;
    continueResponse(input: object[]): Promise<void>;
    cancel(): void;
}
/** The Chevalier agent runtime. */
export declare class Runtime {
    /** @internal access to the raw napi runtime */
    readonly native: native.Runtime;
    constructor(options?: RuntimeOptions);
    claudeSession(config: ClaudeSessionConfig): Promise<ClaudeSession>;
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
    setModelToolNames(names: string[] | null): Promise<void>;
    setToolAsync(name: string, asynchronous: boolean): Promise<void>;
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
