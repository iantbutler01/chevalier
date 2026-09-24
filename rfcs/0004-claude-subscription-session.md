# RFC 0004: Claude Subscription Session

Status: Implemented 2026-09-24 in Rust, napi and pyo3 (as-built notes in §As built). Wire contract verified live against Claude Code 2.1.281 and 2.1.282 on a Max subscription (evidence in §Evidence).

## Summary

Add a first-party Chevalier **session** that runs Claude on the user's own Claude subscription by hosting the unmodified official `claude` binary as a child process. Chevalier speaks the host side of the CLI's stdio control protocol (the protocol Anthropic's TypeScript and Python Agent SDKs speak), serves the caller's registered tools to Claude as an in-process tool server, and turns the CLI's output into Chevalier stream events. The session lives in Chevalier's Rust core and is exposed through the TypeScript (napi) and Python (pyo3) bindings, like every other Chevalier surface.

Model strings use:

```text
claude-subscription:<alias-or-id>      e.g. claude-subscription:opus, claude-subscription:claude-fable-5-1
```

This is not a provider. A Chevalier provider is one model call that the caller drives through a tool loop. Here the CLI owns the agent loop: it decides tool calls, calls Chevalier mid-stream to execute them, and handles compaction, retries and authentication. Chevalier owns *what the agent can do* (only the caller's tools) and *sees every call* (it dispatches them).

## Background

- OpenBracket needs its full product loop (tools, guardians, approvals, remote execution, events) to run with Claude billed to the user's subscription, not an API key.
- Wonderloom proved the shape in Rust (`wonderloom/specs/17-cli-agent-executors`, `crates/loom-forge/src/executor/`). It used a loopback MCP-over-HTTP server (rmcp) and hit protocol traps (`server/discover`, session keep-alive). This RFC replaces that transport: the CLI routes tool calls for "SDK" MCP servers over its own stdio control channel, so no socket, port or MCP library is needed.
- There is no official Rust Agent SDK. Anthropic's docs sanction running the CLI as a subprocess from other languages ("To drive the same agent loop from a language other than Python or TypeScript, run the CLI as a subprocess"; Agent SDK overview).

## Terms-of-service rules (normative)

Source: Claude Code docs, *Legal and compliance → Authentication and credential use*, and the Agent SDK overview. The allowed case is "an end user … signing in to the unmodified Claude Code binary with their own Claude subscription". The prohibited cases are offering claude.ai login in your own application, routing requests through Free/Pro/Max credentials on behalf of other users, and collecting, storing or intermediating claude.ai credentials or session tokens.

1. **The official, unmodified `claude` binary does all authentication and all Anthropic API traffic.** Chevalier never reads, copies, refreshes or forwards tokens from `~/.claude`, `~/.claude.json` or the macOS Keychain, and never calls an Anthropic endpoint with subscription credentials.
2. **Login happens only through Anthropic's own flow** (`claude auth login` / first interactive `claude`). Chevalier exposes read-only status detection (`claude auth status --json`) and nothing that accepts, stores or transmits a claude.ai credential.
3. **One user, one subscription, on the machine where that user logged in.** Chevalier never moves credentials between machines. A host that runs sessions on a server (e.g. OpenBracket on corvidae) requires the user to log in to `claude` on that server themselves.
4. **No binary modification and no disabling of auth methods.** Session configuration uses documented flags only.
5. **Subscription or fail.** If the CLI reports `apiKeySource` other than `"none"` in `system/init`, or the environment would route to an API key, Bedrock, Vertex or Foundry, the session fails with `ClaudeSessionError::NotSubscription` before the first user turn. No silent paid fallback.
6. **Honest identification.** The child environment sets `CLAUDE_AGENT_SDK_CLIENT_APP=<host app>` (from config). Chevalier does not impersonate the TS SDK (`CLAUDE_AGENT_SDK_VERSION` is never set) or the interactive CLI.
7. **Branding.** Surfaces label this "Claude (subscription)"; "Claude Code" is not used as a product or feature name. Plain text may say a feature "runs Claude Code".
8. **Black-box only.** The Consumer Terms forbid reducing the Services "to human-readable form". Chevalier never reads, unpacks or extracts from the `claude` binary or its bundle. Every wire fact comes from observing the stdin/stdout of the process Chevalier runs. The `claude-codes` crate's schema-extraction tooling and its `auth` feature (which relays the login flow through a PTY) are never used or copied.

Rule-sensitive code (env builder, CLI resolution, status detection) lives in one module, `claude_subscription/policy.rs`, with a test per rule.

## Goals

- One Rust implementation of the CLI host protocol, exposed unchanged through napi and pyo3.
- Serve every tool registered on a `Runtime`: handler-backed tools are executed by Chevalier; schema-only tools are surfaced to the host as call events and answered by the host (OpenBracket's path; its guardians and approvals stay host-side).
- Multimodal tool results (text + images).
- Streaming text and thinking deltas, steering (user messages during a turn), interrupt, and resume across processes.
- Subscription quota surfaced as `RateLimits`, usage as `Usage` (list-price cost reported separately, never as billing).
- Deterministic offline tests against a fake `claude` executable replaying captured wire, plus a live `#[ignore]` test on a real subscription (CONTRIBUTING: new features need real-LLM integration tests).

## Non-goals

- Claude Code's built-in tools (Bash, Read, Edit, …). Always disabled; the agent only reaches the caller's tools.
- User settings, hooks, plugins, skills, CLAUDE.md memory, the user's own MCP servers, claude.ai connectors. All excluded.
- Claude Code subagents (`Task`/agents). Host-side delegation is a normal tool.
- API-key Claude. That is the existing `anthropic` provider.
- A Codex-CLI executor. The design is generic enough to add one later; not in scope.

## Design

### Module layout (`rust/src/claude_subscription/`)

| File | Owns |
|---|---|
| `mod.rs` | `ClaudeSession`, `ClaudeSessionConfig`, `ClaudeSessionEvent`, public API |
| `policy.rs` | CLI resolution, env builder, argv builder, auth-status detection (ToS rules 1–6) |
| `wire.rs` | thin layer over `claude-codes` types (decode of every CLI message, `ControlResponse` encode) plus local types `claude-codes` lacks: the `initialize` request with `systemPrompt`, `sdkMcpServers`, `sdkMcpServerManifests`, and the tool-result `mcp_response` payload |
| `process.rs` | spawn, stdio pumps, stderr ring, kill ladder |
| `tools.rs` | manifest from `Runtime` schemas; `tools/call` dispatch (handler or host) |
| `events.rs` | CLI message → `ClaudeSessionEvent` mapping |

Feature-gated as `claude-subscription` (default on for `ts/` and `py/` builds). One new dependency: `claude-codes = { version = "=2.1.281", default-features = false, features = ["types"] }` (Apache-2.0, meawoppl/rust-code-agent-sdks). It supplies typed serde models of the CLI's stream-json and control messages and versions itself to the CLI release it was tested against. Types only: its clients, PTY login (`auth`) and schema tooling are not enabled. The pin is exact; a bump is a reviewed change that re-runs the fixture and live tests. It does not model the `initialize` fields this design sends, so those stay local in `wire.rs`. Everything else (tokio `process`/`io-util`, serde_json, uuid, base64, tokio-util) is already in `rust/Cargo.toml`.

### Model string

`is_claude_subscription_model(model: &str) -> bool` in `types.rs` matches the `claude-subscription:` prefix. `Runtime::run`/`run_stream` with such a model fail with `NonRetryable("claude-subscription models run through Runtime::claude_session")` instead of the generic unsupported-provider error. `parse_model_string` accepts `@effort=<low|medium|high|xhigh|max>` for this provider; other `@` params are rejected as today.

### Public Rust API

```rust
pub struct ClaudeSessionConfig {
    pub model: String,                    // alias or id after the "claude-subscription:" prefix
    pub system_prompt: Option<String>,    // sent in the initialize request
    pub effort: Option<Effort>,           // --effort; `ultra` normalizes to `max` at the host
    pub resume: Option<ResumeTarget>,     // { session_id, cwd } from a previous Init event
    pub cwd: PathBuf,                     // stable per conversation; sessions persist under ~/.claude/projects/<cwd>
    pub cli_path: Option<PathBuf>,        // explicit override; else resolution order in policy.rs
    pub client_app: String,               // CLAUDE_AGENT_SDK_CLIENT_APP, e.g. "openbracket"
    pub server_name: String,              // tool server name; tools appear to the model as mcp__<server>__<tool>
    pub idle_timeout: Duration,           // no stdout byte for this long ⇒ interrupt + fail (default 240s)
    pub max_turns: Option<u32>,           // --max-turns per user turn; None = CLI default
}

impl Runtime {
    pub async fn claude_session(&self, config: ClaudeSessionConfig) -> Result<ClaudeSession>;
}

impl ClaudeSession {
    pub async fn send(&self, message: UserTurn) -> Result<()>;          // first turn, follow-up, or mid-turn steer
    pub async fn next_event(&mut self) -> Option<Result<ClaudeSessionEvent>>;
    pub async fn respond_tool(&self, call_id: &str, result: ToolOutput) -> Result<()>;
    pub async fn interrupt(&self) -> Result<()>;                         // control_request interrupt
    pub async fn close(self) -> Result<ExitSummary>;                     // close stdin, then kill ladder
}

pub struct UserTurn { pub text: String, pub images: Vec<MediaPart> }
pub struct ToolOutput { pub content: Vec<ToolContent>, pub is_error: bool }
pub enum ToolContent { Text(String), Image { data_base64: String, mime_type: String } }
```

`claude_session` snapshots `get_tool_schemas()` (honouring `model_tool_names`) and takes `tool_executor()` before spawning, so the session never holds the `Runtime` lock (the same reason napi keeps a separate executor, `ts/src/runtime.rs:188-191`).

### Events

```rust
pub enum ClaudeSessionEvent {
    Init { session_id: String, cwd: PathBuf, model: String, cli_version: String },
    TextDelta(String),                       // stream_event content_block_delta text_delta
    ThinkingDelta(String),                   // stream_event thinking_delta
    AssistantMessage { text: String },       // complete assistant text for the message (for hosts that don't consume deltas)
    ToolCall { call_id: String, call: ToolCall },          // schema-only tool: host must respond_tool(call_id, ..)
    ToolExecuted { call: ToolCall, output: ToolOutput },   // handler-backed tool Chevalier already ran
    ToolCancelled { call_id: String },       // CLI sent notifications/cancelled for an in-flight call (after interrupt); host aborts it
    RateLimits(Vec<ProviderRateLimit>),
    ApiRetry { attempt: u32, delay_ms: u64, error: String },
    TurnComplete { usage: TokenUsage, list_price_usd: Option<f64>, num_turns: u32, is_error: bool, subtype: String, result: Option<String> },
}
```

- `call.tool_use_id` is `_meta["claudecode/toolUseId"]` so ids match the CLI transcript; `call_id` is the control `request_id` the reply must echo.
- `RateLimits` maps `rate_limit_info.unifiedWindows{name:{utilization,resetsAt}}` to `ProviderRateLimit{scope: Subscription, used_percent: round(utilization*100), window_minutes: {five_hour:300, seven_day:10080, else: 0}, resets_at_epoch_sec}`; without `unifiedWindows`, synthesize one window from `(rateLimitType, utilization, resetsAt)` when all three exist.
- `Usage` comes from `result.usage` only. `assistant.message.usage` is the `message_start` snapshot and undercounts output (wonderloom §3.2).
- `list_price_usd` is `result.total_cost_usd`, a client-side estimate. It is labelled as such and must not be treated as spend.

### Wire contract (host side)

Transport: newline-delimited JSON on the child's stdin/stdout. Every outbound line is one JSON object followed by `\n`.

**Spawn** (argv mirrors the TS SDK 0.3.281 exactly, plus the lock-down flags; captured in §Evidence):

```text
claude --output-format stream-json --verbose --input-format stream-json
       --model <model> [--effort <effort>] [--max-turns <n>] [--resume=<session_id>]
       --tools "" --allowedTools mcp__<server> --setting-sources= --strict-mcp-config
       --permission-mode default
```

`--tools ""` removes every built-in tool; `--setting-sources=` drops user/project settings; `--strict-mcp-config` drops the user's MCP servers **and claude.ai connectors** (verified: without it, a claude.ai connector appeared in the tool list even with `settingSources: []`). `--allowedTools mcp__<server>` pre-approves our tools so no permission prompt is raised.

**Environment** (`policy::spawn_env`): inherited environment minus
`ANTHROPIC_API_KEY`, `ANTHROPIC_AUTH_TOKEN`, `ANTHROPIC_BASE_URL`, `CLAUDE_CODE_USE_BEDROCK`, `CLAUDE_CODE_USE_VERTEX`, `CLAUDE_CODE_USE_FOUNDRY`, `CLAUDE_CODE_OAUTH_TOKEN`, `CLAUDECODE`, every `CLAUDE_CODE_*` session/nesting variable (`CLAUDE_CODE_ENTRYPOINT`, `CLAUDE_CODE_CHILD_SESSION`, `CLAUDE_CODE_SESSION_ID`, `CLAUDE_CODE_SESSION_ATTENDED`, `CLAUDE_CODE_MESSAGING_SOCKET`, `CLAUDE_CODE_MESSAGING_TOKEN`, `CLAUDE_CODE_EXECPATH`), `CLAUDE_PID`, `CLAUDE_EFFORT`, `CLAUDE_AGENT_SDK_VERSION`; plus
`CLAUDE_AGENT_SDK_CLIENT_APP=<client_app>`, `CLAUDE_CODE_DISABLE_AUTO_MEMORY=1`, `CLAUDE_CODE_DISABLE_TERMINAL_TITLE=1`. `HOME` and `PATH` are kept (the CLI finds its login through them).

Stripping `CLAUDE_CODE_OAUTH_TOKEN` is deliberate: a session must use the login the user made on this machine (ToS rule 2/3), not an injected token. Evidence for the nesting variables: a child spawned from inside a Claude Code session inherited `CLAUDECODE=1`, the parent's messaging socket and token, and its session id.

**CLI resolution** (`policy::resolve_cli`): `cli_path` → `CHEVALIER_CLAUDE_BIN` → `PATH` lookup → `~/.local/bin/claude`, `~/.claude/local/claude`, `/opt/homebrew/bin/claude`, `/usr/local/bin/claude`. An explicit path that does not exist is an error, never a fallback. Not found ⇒ `ClaudeSessionError::CliNotFound { probed }`.

**Version gate.** `claude --version` is read once per resolved path (cached). Below `2.1.281` (the captured contract) ⇒ `ClaudeSessionError::CliTooOld`. Newer versions are allowed; decode is lenient.

**1. Initialize** (host → CLI, first line):

```json
{"type":"control_request","request_id":"<id>","request":{
  "subtype":"initialize",
  "systemPrompt":["<system prompt or empty string>"],
  "sdkMcpServers":["<server>"],
  "sdkMcpServerManifests":{"<server>":{
    "initializeResult":{"protocolVersion":"2025-11-25","capabilities":{"tools":{"listChanged":true}},"serverInfo":{"name":"<server>","version":"<chevalier version>"}},
    "toolsListResult":{"tools":[{"name":"...","description":"...","inputSchema":{...JSON Schema...},"execution":{"taskSupport":"forbidden"}}]}}}}}
```

The CLI answers `control_response{subtype:"success", request_id:<id>}`. `inputSchema` is the registered JSON Schema verbatim (the manifest carries plain JSON Schema; no zod or rmcp conversion). Tools are sorted by registration order (`tool_order`) for prompt-cache stability.

**2. User turn** (host → CLI, any time, including mid-turn):

```json
{"type":"user","session_id":"","message":{"role":"user","content":[{"type":"text","text":"..."},{"type":"image","source":{"type":"base64","media_type":"image/png","data":"..."}}]},"parent_tool_use_id":null}
```

A message sent while a turn is running is delivered to the model within that turn (verified: a steer sent while a tool call was pending was reflected in the same turn's answer).

**3. Tool call** (CLI → host):

```json
{"type":"control_request","request_id":"<rid>","request":{"subtype":"mcp_message","server_name":"<server>",
  "message":{"jsonrpc":"2.0","id":<n>,"method":"tools/call","params":{"name":"<tool>","arguments":{...},"_meta":{"claudecode/toolUseId":"toolu_…","progressToken":<n>}}}}}
```

Reply (host → CLI):

```json
{"type":"control_response","response":{"subtype":"success","request_id":"<rid>",
  "response":{"mcp_response":{"jsonrpc":"2.0","id":<n>,"result":{"content":[{"type":"text","text":"..."},{"type":"image","data":"<b64>","mimeType":"image/png"}],"isError":false}}}}}
```

Calls can arrive concurrently (verified: two reads in flight together). Each is dispatched independently; replies may go out in any order. Other `mcp_message` methods (`ping`, `notifications/*`) get the JSON-RPC-correct empty reply. An unknown tool name returns `result.isError=true` with a text block, not a JSON-RPC error, so the model can recover.

**4. Interrupt** (host → CLI): `{"type":"control_request","request_id":"<id>","request":{"subtype":"interrupt"}}`. Captured sequence (`interrupt.*.jsonl`): the CLI sends `control_request/mcp_message` with `{"method":"notifications/cancelled","params":{"requestId":<jsonrpc id of the in-flight tools/call>,"reason":"…"}}`; the host acks it with `mcp_response {"jsonrpc":"2.0","result":{},"id":0}`, aborts that call (handler-backed: drop its future; schema-only: emit `ToolCancelled{call_id}` and ignore any later `respond_tool` for it); the CLI acks the interrupt with `control_response{response:{still_queued:[]}}`; the turn ends with `result{subtype:"error_during_execution", is_error:true}`, reported as `TurnComplete{is_error:true, subtype}`, and the host decides meaning.

**5. CLI → host messages consumed:** `system/init`, `stream_event` (partial deltas; requires `--include-partial-messages`, added to argv when the host asks for deltas), `assistant`, `user` (tool-result echoes; ignored), `rate_limit_event`, `system/api_retry`, `result`, `control_response` (acks). Everything else is ignored after a `debug!`. More than 20 consecutive non-JSON stdout lines ⇒ `ClaudeSessionError::Protocol`.

**6. Init validation.** On the first `system/init`: `apiKeySource == "none"` (else `NotSubscription`), our server present with `status:"connected"` and at least one `mcp__<server>__*` tool listed (else `Protocol` with `mcp_servers` and the stderr tail).

### Process lifecycle

- `kill_on_drop(true)`, stdin/stdout/stderr piped, `cwd = config.cwd` (created if missing).
- stderr drained by its own task into a 64 KiB ring from spawn, before the first stdin write (an undrained pipe blocks the child).
- Idle watchdog: any stdout byte re-arms it; expiry ⇒ interrupt, then kill, then `ClaudeSessionError::Idle{stderr_tail}`.
- `close()`: close stdin, wait 10 s, SIGTERM, wait 5 s, SIGKILL. Exit status is logged, never used as a success signal (an interrupt exits 1, SIGTERM 143).
- Dropping `ClaudeSession` aborts its tasks and kills the child.
- Concurrency is not capped by Chevalier. Hosts may impose their own resource limit.

### Errors

New variant `Error::ClaudeSession(ClaudeSessionError)` with `CliNotFound{probed}`, `CliTooOld{found, required}`, `NotLoggedIn`, `NotSubscription{api_key_source}`, `Protocol(String)`, `Idle{stderr_tail}`, `Exited{status, stderr_tail}`. All non-retryable. `ts/src/error.rs` (exhaustive match) and `py/src/errors.rs` get matching codes: `CLAUDE_CLI_NOT_FOUND`, `CLAUDE_CLI_TOO_OLD`, `CLAUDE_NOT_LOGGED_IN`, `CLAUDE_NOT_SUBSCRIPTION`, `CLAUDE_PROTOCOL`, `CLAUDE_IDLE`, `CLAUDE_EXITED`.

### Status detection

`pub async fn claude_subscription_status(cli_path: Option<&Path>) -> ClaudeSubscriptionStatus` runs `<cli> auth status --json` (5 s timeout, `spawn_env`), parses the whole stdout, and returns `Ready{email, subscription_type}` / `NotLoggedIn` / `CliNotFound{probed}` / `Error{message}`. `loggedIn !== true` or `apiProvider != "firstParty"` is not `Ready`. Read-only; exposed through both bindings.

### TypeScript surface (napi)

```ts
class Runtime {
  claudeSession(config: ClaudeSessionConfig): Promise<ClaudeSession>;
}
class ClaudeSession {
  next(): Promise<ClaudeSessionEvent | null>;
  send(turn: { text: string; images?: MediaPartInput[] }): Promise<void>;
  respondTool(callId: string, output: { content: ToolContent[]; isError?: boolean }): Promise<void>;
  interrupt(): Promise<void>;
  close(): Promise<ExitSummary>;
}
function claudeSubscriptionStatus(cliPath?: string): Promise<ClaudeSubscriptionStatus>;
```

`ClaudeSessionEvent` is a literal discriminated union in `index.ts` (not `type: string`). `index.ts` adds `events(session, { signal })`, an async generator that closes the session on abort and in `finally`, matching `Runtime.runStream`.

### Python surface (pyo3)

`Runtime.claude_session(config) -> Awaitable[ClaudeSession]`; `ClaudeSession.next()`, `send()`, `respond_tool()`, `interrupt()`, `close()` are awaitables; `async for event in session` is supported. Handler-backed tools registered with `Runtime.tool` run inside the session with no host code. Event classes and `claude_subscription_status` are added to `__init__.py` and `__init__.pyi`.

## OpenBracket consumption contract

OpenBracket's own spec (`OpenBracket/specs/63-claude-subscription-runs.md`) owns the product integration. What it may rely on from Chevalier:

- Its tools stay schema-only (`rt.tool({name, description, schema})`), so every call arrives as `ToolCall{call_id, call}` and OpenBracket runs its existing guardian/approval/execution path, then `respondTool`.
- `Init.session_id` + `Init.cwd` are the resume key; OpenBracket persists them per thread and passes `resume` on the next run on the same host.
- `RateLimits` uses the existing `ProviderRateLimit` shape, so OpenBracket's `provider_usage` path handles it unchanged.
- Chevalier never touches claude.ai credentials; OpenBracket must not either.

## Compatibility

- Additive. Existing providers, `Runtime::run_stream` and bindings are unchanged.
- The control protocol is the Agent SDK's internal contract, not a documented public API. It is pinned by captured fixtures, a minimum CLI version and the exact `claude-codes` version; a CLI release that changes it fails the fixture-replay tests and the live test before it reaches hosts. Drift detection is black-box (rule 8): replayed fixtures, the live test, and a hard failure on any CLI message shape Chevalier acts on but cannot decode.

## Validation

**Rust unit tests** (in-module): argv and env builders (exact flag set; every stripped variable; `CLAUDE_CODE_OAUTH_TOKEN` and API-key variables never reach the child); CLI resolution order with a fake PATH/home; version gate; wire decode of every captured message (`rust/tests/fixtures/claude-subscription/*.jsonl`, sanitized: session tokens and the slash-command list removed); rate-limit mapping (unified windows, documented fallback, empty); manifest from registered schemas (order, `model_tool_names` filter, verbatim JSON Schema); init validation (`apiKeySource != none` ⇒ `NotSubscription`; missing server ⇒ `Protocol`).

**Rust integration tests** (`rust/tests/claude_subscription.rs`) against `rust/tests/fixtures/claude-subscription/fake-claude.cjs`, a Node script that validates argv/env (writes them to `$CHEVALIER_FAKE_CLAUDE_RECORD`), answers `initialize`, and replays scripted stdout while honouring stdin. Modes: `happy` (two concurrent tool calls, one handler-backed, one schema-only, one image result, a rate-limit event, a result); `steer` (a user line during a pending tool call is visible to the script before the result); `interrupt`; `api-key` (`apiKeySource:"ANTHROPIC_API_KEY"` ⇒ `NotSubscription`, no user turn written); `crash-before-init`; `idle`; `garbage` (non-JSON flood ⇒ `Protocol`).

**Live** (`#[ignore = "requires a logged-in claude CLI on a Claude subscription"]`, gated on `CHEVALIER_LIVE_CLAUDE_SUBSCRIPTION=1`): `sonnet`, two tools (text + PNG), asserts `apiKeySource == none`, both tools called, the image described, `RateLimits` received, then resume in a new session and recall a tool result.

**TypeScript** (`ts/test/claude-session.test.cjs`): the same fake CLI through the napi surface: schema-only call → `respondTool`, steering, interrupt, abort-signal cleanup (child gone), error codes.

**Python** (`integration_tests/test_python_claude_session.py`): handler-backed tool run end to end with the fake CLI; `async for`; error mapping.

Gates: `cargo test --all-features`, `cargo clippy --all-features -- -D warnings`, `cargo fmt --check`, `npm test` in `ts/`, `pytest integration_tests` after `maturin develop`.

## Evidence

Captured 2026-09-24 (raw transcripts, sanitized, in `rust/tests/fixtures/claude-subscription/{happy,steer,resume,interrupt}.{argv,stdin.jsonl,stdout.jsonl}`) by driving `@anthropic-ai/claude-agent-sdk@0.3.281` through a tee wrapper around `claude` 2.1.281, logged in with `authMethod: claude.ai`, `subscriptionType: max`:

- argv: `--output-format stream-json --verbose --input-format stream-json --max-turns 8 --model sonnet --allowedTools mcp__ob --tools  --setting-sources= --permission-mode default` (+ `--resume=<id>` when resuming).
- `system/init`: `apiKeySource: "none"`, `model: "claude-sonnet-5"`, `mcp_servers: [{name:"ob", status:"connected", source:"sdk"}]`.
- Tool calls arrived as `control_request/mcp_message` with `_meta["claudecode/toolUseId"]`; two `read_file` calls were in flight together; a PNG image result was described by the model.
- `rate_limit_event.rate_limit_info`: `{status:"allowed", rateLimitType:"five_hour", unifiedWindows:{five_hour:{utilization:0.04,…}, seven_day:{utilization:0.5,…}}, overageStatus:"rejected"}`.
- A second user message during a pending tool call changed that turn's answer; `--resume=<session_id>` from a new process recalled the earlier tool result. Session transcripts live under `~/.claude/projects/<mangled cwd>/`.
- Without `--strict-mcp-config`, a claude.ai connector (`claude.ai Claude Docs`) was loaded despite empty setting sources.

## As built (2026-09-24)

Deviations from the design above, accepted:

- **`apiKeySource` arrives after the first user turn.** The CLI emits `system/init` only once a user message is queued, so rule 5 cannot be enforced before that write. The session instead preflights `claude auth status --json` before spawning (requires `loggedIn`, `authMethod == "claude.ai"`, `apiProvider == "firstParty"`; a Console or third-party login fails with a message naming the method), strips every API-key/provider variable from the child env, and still kills the session with `NotSubscription` if `system/init` reports anything but `"none"`.
- **Schema-only marker.** Schema-only registration used to install a throwing handler, indistinguishable from a real one. `Runtime::register_tool_schema` now records `ToolSchemaInfo.schema_only`; both bindings' handler-less `tool()` use it.
- **`host_dispatch_all` (config) / `hostDispatchAll` (TS).** Surfaces every call as `ToolCall`, including handler-backed tools such as MCP client tools, so a host that gates tools itself (OpenBracket) runs them through its own path and `Runtime::execute_tool_call`. Without it, handler-backed tools would bypass the host's guardians.
- **Idle watchdog counts outstanding tool calls as activity.** It polls every 250 ms and fires only after `idle_timeout` with no stdout byte *and* no pending call; a host tool waiting on an approval for minutes is not a stall.
- **`next_event(&self)` and `shutdown(&self)`** in addition to the consuming `close(self)`, so bindings can close while a receive is pending.
- **Init decode.** `claude-codes` types decode every captured line, including `system/init` (the first fixture sanitization had flattened `memory_paths` to a list; the real shape is an object and the fixtures now keep it).

## Open questions

- OpenBracket's existing Claude integration stores a `claude setup-token` value (`sk-ant-oat01-…`) in its vault and injects it into VM terminals (`OpenBracket/packages/api/src/lib/claude-code.ts`). That is close to "collect, store, or intermediate … session tokens". This RFC does not use it (and strips `CLAUDE_CODE_OAUTH_TOKEN`). Ian to decide whether that feature is replaced by in-VM `claude auth login` plus status detection.
- Image input on user turns is in the wire (base64 `image` blocks); fixtures cover it, the live test does not yet.
