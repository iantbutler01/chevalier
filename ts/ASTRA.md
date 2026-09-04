# Astra Responses integration

`openai:gpt-6-astra` selects Responses automatically. `openai:resp:gpt-6-astra`
and `openai-responses:gpt-6-astra` remain explicit forms. API keys, inline
`@server_url=...`, and `@reasoning=...` retain their existing meanings. Astra
requests omit temperature and top-p. Other models retain their existing routes.

## Native conversation state

Each `responseItems` stream event has `data: { model, responseId, items }`.
These are the original completed Responses output items, including opaque
reasoning, message phase, compaction, and tool metadata. An automatic steering
continuation emits another event for its own response: retain both, in order.
Do not append reconstructed assistant/tool messages alongside these items.
The collector retains `response.output_item.done` by output index when a backend
omits terminal output or sends an empty terminal array. Empty native snapshots
are not emitted in place of an ordinary visible assistant response.

Store each event on an ordinary history message:

```ts
const message = {
  type: "assistantResponse",
  content: visibleText,
  toolCalls: visibleToolCalls,
  providerResponse: { model: event.data.model, items: event.data.items },
};
```

The matching OpenAI Responses model replays `items` instead of reconstructing
the message. Another model/provider uses the ordinary text/tool fallback.
The `model` is the requested provider model name, not the prefixed Chevalier
model string or a server-returned snapshot alias. A `complete` event also
contains aggregated native state as `data.provider_response`; `run()` exposes
the native state as `providerResponse`.

## Steering and cancellation

```ts
for await (const event of runtime.runStream({
  prompt,
  responses: { websocket: true },
  signal: abortController.signal,
  onControl: control => { activeControl = control; },
})) {
  consume(event);
}
```

Wait for the first `responseId` event before calling `activeControl.steer(input)`.
Its promise confirms the send, not model acceptance or application. `steering`
events expose `data.status` as `accepted`, `pending`, `applied`, or `failed`,
with the provider `steer` identity. `pending` retains `required_input`.
`applied` is synthesized when the same connection starts the successor
response; it is not a claim that the task is finished.

On `pending`, return required native tool-output/approval items through
`activeControl.continueResponse(items)`. That sends `response.create` on the
same socket with the original configuration and current response ID. Do not
repeat accepted steering. Automatic continuation needs no client create call.

`activeControl.cancel()` or the AbortSignal closes the stream/request. The
AbortSignal makes iteration reject with its abort reason. Neither operation
undoes host tools; the host owns their cancellation. Persist steering inputs
and delivery state in the host: queued steering does not survive disconnects.

WebSocket mode is explicit and does not silently fall back to SSE. The Codex
subscription transport uses its own URL and credentials; local fixtures prove
the wire implementation, not upstream subscription capability. Unsupported
provider/model responses remain errors. No post-dispatch stream retry occurs.

## Async tools

`runtime.tool({ name, schema, async: true })` retains ordinary host dispatch.
For Astra only, function definitions include `async: true`. A complete
`toolCall` event exposes `toolCall.async` and `toolCall.providerMetadata` as
soon as arguments finish, before the provider response finishes. Execute it
through the host's existing authorization/dispatch path. A `toolMetadata`
event also carries the complete native call item when it arrives.

The host owns pending jobs, result delivery, and side effects. Chevalier does
not execute an asynchronous job simply because its schema is marked async.
Native custom-tool items are retained in history, but the typed Runtime tool
registration/dispatch surface remains function-based.
Registered handlers can run through `runtime.executeToolCall` during an active
stream: their shared executor does not take the inference runtime mutex.

## Programmatic execution: existing sandbox, Rust owner

The reusable executor lives in `chevalier::programmatic` in the Rust crate.
`execute_programmatic` accepts tool descriptors, a `ProgrammaticSandbox`, a
`ProgrammaticDispatcher`, and cancellation/deadline options. Rust owns the guest
runner, concurrent tool RPC, selected output, limits, and process teardown.
Tokio supplies task coordination and cancellation; the custom protocol connects
sandbox JavaScript to the host's existing authorized tool dispatcher.

With the Rust `sandbox` feature, `ProgrammaticSandboxConfig` accepts an already
selected Chevalier `Session`, working directory, and environment. It does not
create or select a second sandbox. Other hosts can implement the same traits
around their existing interactive execution backend.

The TypeScript `executeProgrammatic` export is a native binding plus callbacks:

```ts
const result = await executeProgrammatic(
  'const results = await Promise.all([tools.read({path: "a"}), tools.read({path: "b"})]); text(results);',
  {
    sandbox: { startExec: options => backend.startExec({ ...options, cwd: backend.root }) },
    tools: enabledToolDescriptors,
    dispatch: (name, args, context) => authorizedDispatcher(name, args, context),
    signal: abortController.signal,
    timeoutMs: 30000,
  },
);
```

The backend must be the host-selected sandbox, not a local-shell fallback.
Node.js must already exist inside it. Omitting the sandbox is an error.
The program has that sandbox's ordinary filesystem/network/process access;
this is not a JavaScript security isolate. Named tool calls use the supplied
dispatcher and therefore retain its approvals and permissions. Every call must
be awaited. Only values passed to `text` are returned as selected output.

OpenBracket configures this primitive in its Agent layer using the thread's
existing execution backend. Its workspace toggle defaults off, independently
of native compaction. No OpenBracket dependency is required by the Rust core.

VMD execution control (2026-09-04): direct exec uses `ControlExec`; distributed
`exec.stream` uses `ControlDaemon` for its existing named guest process group.
Control uses a separate connection from stdin/stdout so backpressure cannot
block signal delivery. EOF remains ordered after earlier input and leaves the
control handle usable. Distributed controls carry the stream's session, VM,
node, producer epoch, and control sequence; the guest rejects stale owners and
does not apply duplicate signals twice. Reattachment does not rerun the command.

JavaScript retains `ExecHandle.write()`, `eof()`, `signal()`, and `next()`.
Rust retains `handle.input.send(ExecInput::...)`, but the field now has type
`ExecInputSender` rather than a raw Tokio sender. Custom adapters constructing
`ExecHandle` can convert their existing sender with `.into()`.

The client, VMD, and guest portproxy must all be updated. An older guest cannot
honor the new control RPCs. Signal acceptance is not exit confirmation: the
executor still waits for the guest's terminal result and reports missing
confirmation instead of claiming a process stopped. Detached commands retain
their separate lifetime semantics.

## Compaction: opt-in only

Only `responses: { compactionThreshold: 200000 }` sends native
`context_management`. Omission leaves it off. For stateless replay, this
option also discards input before the latest native compaction item, retaining
that item and everything after it. Explicit `previousResponseId` chaining is
not manually pruned. The subscription request path remains stateless.

There is no standalone `/responses/compact` API here. Its output is a canonical
window with different pruning rules and must not be treated as this mode.

## Validation

With the repository Rust toolchain selected:

```sh
cd ts
npm run build:native:debug
npm run build:ts
node --test test/astra-provider.test.cjs
cd ..
cargo test --manifest-path rust/Cargo.toml --lib
cargo test --manifest-path rust/Cargo.toml --lib native_binding_controls -- --ignored
cargo test --manifest-path rust/Cargo.toml --lib native_binding_executes -- --ignored
```

The explicit ignored test requires freshly built Node bindings. It connects
the public JS Runtime to a local WebSocket server and verifies steering and
AbortSignal socket closure. HTTP tests prove native-state replay, model-switch
fallback, opt-in prefix compaction, early async dispatch, and cancellation.
They also cover Astra image inputs and surfacing failures after dispatch without
retrying tool side effects.

Live subscription validation on 2026-09-04 also completed a manual tool round
trip: a reasoning item contained 1,868 characters of encrypted content, the
function call and that item replayed successfully, and the continuation returned
the expected test marker. No credential or encrypted payload was logged.
This does not establish live subscription async-tool or compaction support.

Protocol references: [steering](https://developers.openai.com/api/docs/guides/steering),
[async tools](https://developers.openai.com/api/docs/guides/async-tool-calling),
[reasoning](https://developers.openai.com/api/docs/guides/reasoning), and
[compaction](https://developers.openai.com/api/docs/guides/compaction).
