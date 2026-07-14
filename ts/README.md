# chevalier

TypeScript bindings for **Chevalier** — *agents are just functions*. A thin,
typed layer over the Rust engine (via [napi-rs](https://napi.rs)), so you get
native performance with idiomatic TypeScript. Works on **Node** and **Bun**
(Deno too, via Node-API).

```bash
npm install chevalier        # optionally: npm install zod
```

## Quick start

```ts
import { Runtime, agentic } from "chevalier";
import { z } from "zod";

// Structured output with Zod — typed AND runtime-validated.
const Person = z.object({ name: z.string(), age: z.number() });

const extract = agentic(
  { model: "anthropic:claude-3-5-sonnet" },
  (text: string, rt) => rt.run({ prompt: `Extract a person from: ${text}`, output: Person }),
);

const p = await extract("Alice is 30 years old");
console.log(p.value); // { name: "Alice", age: 30 }  (typed)
```

`agentic(config, fn)` wraps a function so a fresh `Runtime` is created per call
and passed as the last argument — "an agent is just a function." Or use the
`Runtime` class directly.

## Providers

A model string selects the provider, e.g. `anthropic:claude-3-5-sonnet`,
`openai:gpt-4o`, `google-gemini:gemini-2.0-flash`,
`openrouter:anthropic/claude-3.5-sonnet`. For an OpenAI-compatible server
(vLLM, Ollama, …) use `custom-openai` with the **full** chat-completions URL:

```ts
const rt = new Runtime({
  model: "custom-openai:my-model@server_url=http://host:8000/v1/chat/completions",
  apiKey: "EMPTY",
});
```

API keys come from `apiKey` or the provider's env var (`ANTHROPIC_API_KEY`, …).

## Tools

```ts
await rt.tool({
  name: "get_weather",
  description: "Weather for a city",
  schema: z.object({ city: z.string() }),     // Zod or raw JSON Schema
  handler: async ({ city }) => `Sunny in ${city}`,
});

const res = await rt.run({ prompt: "Weather in Tokyo?" });
for (const call of res.toolCalls) {
  const result = await rt.executeToolCall(call.toolName, call.args);
  // feed result back into history and run again…
}
```

Omit `handler` to register a **schema-only** tool the model can call but you
dispatch yourself (e.g. an event-driven loop). The agent loop is yours to drive;
`run`, `runStream`, `executeToolCall`, and `getToolSchemas` are the primitives.

## Streaming

```ts
for await (const ev of rt.runStream({ prompt: "Write a haiku" })) {
  if (ev.type === "content") process.stdout.write(ev.text);
  // ev.type: content | reasoning | signature | toolCall | toolPartial | usage | complete
}
```

## Multi-turn & multimodal

```ts
await rt.run({
  prompt: "What did I say my name was?",
  history: [
    { type: "chat", role: "user", content: "My name is Alice." },
    { type: "chat", role: "assistant", content: "Nice to meet you, Alice." },
    // images:
    { type: "multimodal", parts: [{ type: "image", imageBase64: "...", mimeType: "image/png" }] },
    // tool results: { type: "toolResult", toolUseId, content, isError? }
  ],
});
```

## MCP

```ts
import { McpClient, McpServer } from "chevalier";

// Register a remote MCP server's tools onto a runtime:
await rt.mcp("http://localhost:8080/mcp");            // or rt.mcpAs(uri, "label")

// Standalone client:
const client = await McpClient.http("http://localhost:8080/mcp");
await client.callTool("search", { q: "chevalier" });

// Structured connection config (headers are passed to HTTP/WS handshakes):
const authenticated = await McpClient.connect({
  transport: "http",
  url: "https://mcp.example.com/mcp",
  headers: { Authorization: `Bearer ${token}` },
});

// Stdio is spawned directly without a shell. Keep credentials in env, not args:
const local = await McpClient.connect({
  transport: "stdio",
  command: "node",
  args: ["./mcp-server.js"],
  env: { MCP_API_TOKEN: token },
  cwd: "/path/to/project",
});

// Serve your own:
const server = new McpServer("my-server");
await server.tool("greet", "Greet", { type: "object", properties: { name: { type: "string" } } },
  async ({ name }) => `Hello, ${name}!`);
await server.serve("http", "127.0.0.1:8080"); // or "stdio" / "websocket"
```

## VFS

```ts
import { VfsStorage } from "chevalier";

const vfs = VfsStorage.local("/data");               // or VfsStorage.gateway({ endpoint })
await vfs.write("notes/hello.txt", Buffer.from("hi"));
const bytes = await vfs.read("notes/hello.txt");
```

## Sandbox

MicroVM sandboxes live in the separate **[`chevalier-sandbox`](../ts-sandbox)**
package (it pulls in a gRPC client). It connects to an external `vmd` daemon:

```ts
import { Sandbox } from "chevalier-sandbox";
const sb = await Sandbox.connect("http://127.0.0.1:8052");
const sess = await sb.session({ image: "ubuntu:22.04" });
const ex = await sess.exec("ls -la");
for (;;) { const e = await ex.next(); if (!e) break; /* e.type: stdout|stderr|exit|timeout */ }
```

## Build from source

```bash
npm install
npm run build          # napi build (release) + tsc
npm test               # offline regression suite
```

Prebuilt per-platform binaries (macOS arm64/x64, Linux gnu/musl, Windows x64)
are published as `optionalDependencies`; `npm install` fetches the right one.

## License

Apache-2.0
