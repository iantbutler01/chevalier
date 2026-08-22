# chevalier

Python bindings for Chevalier's Rust agent runtime. The package exposes the same Rust-backed runtime, streaming, MCP, and VFS capabilities as the native TypeScript binding.

The sandbox client is packaged separately as `chevalier-sandbox`, matching the TypeScript package split.

## Installation

```bash
pip install chevalier
pip install chevalier-sandbox  # only when sandbox support is needed
```

## Runtime

```python
import asyncio
from chevalier import Runtime

async def main() -> None:
    runtime = Runtime({"model": "openai:gpt-4o", "api_key": "..."})
    result = await runtime.run({
        "prompt": "Summarize this text",
        "temperature": 0.2,
        "max_tokens": 300,
    })
    print(result.text())

asyncio.run(main())
```

`Runtime.run()` returns the Rust runtime's canonical `AssistantResponse`. Its ordered
`output` contains `TextResponsePart`, `ReasoningResponsePart`, `ToolResponsePart`,
and `SignatureResponsePart` values; `text()`, `reasoning()`, `tool_calls()`, and
`signatures()` provide the same projections as Rust.

## Tools

Tool handlers are annotated async functions that receive normal Python keyword arguments and
return a string or `None`. Chevalier derives the tool's JSON Schema from the annotations, then
validates and hydrates provider arguments before calling the function. A `None` result is sent to
the Rust runtime as an empty tool-result string.

```python
async def weather(city: str, units: str = "celsius") -> str:
    return f"Sunny in {city} ({units})"

await runtime.tool(
    weather,
    description="Get the weather for a city",
)

result = await runtime.execute_tool_call("weather", {"city": "Tokyo"})
```

Nested dataclasses and Pydantic models are hydrated as their declared Python types. Invalid
arguments fail before the handler runs. `Runtime.run()` performs one model turn and returns the
requested tool calls; it does not execute them or continue an agent loop automatically. Use
`execute_tool_call()` to dispatch a registered Python handler, or `register_tool_schema()` when
the host owns dispatch.

## Streaming

```python
from chevalier import OutputStreamEvent, TextResponsePart

stream = await runtime.run_stream({"prompt": "Write a short story"})
try:
    while (event := await stream.next()) is not None:
        if isinstance(event, OutputStreamEvent) and isinstance(
            event.output, TextResponsePart
        ):
            print(event.output.text, end="", flush=True)
finally:
    stream.close()
```

The stream preserves Rust's `ResponseStreamEvent` variants as
`OutputStreamEvent`, `ToolPartialStreamEvent`, `UsageStreamEvent`,
`RateLimitsStreamEvent`, and `CompleteStreamEvent`. In particular,
`CompleteStreamEvent.response` is an `AssistantResponse`, not untyped JSON.
Closing a stream cancels its provider request and releases the runtime for another call.

## MCP

```python
from chevalier import McpClient

client = await McpClient.connect({
    "transport": "stdio",
    "command": "npx",
    "args": ["@modelcontextprotocol/server-filesystem", "/tmp"],
})

tools = await client.list_tools()
result = await client.call_tool("read_file", {"path": "/tmp/example.txt"})
```

`McpServer` exposes the same stdio, HTTP, and WebSocket server transports as the TypeScript binding. `Runtime.mcp()` and `Runtime.mcp_as()` register a remote server's tools directly on a runtime.

## VFS

```python
from chevalier import VfsStorage

storage = VfsStorage.local("./workspace")
await storage.mkdir("notes")
await storage.write("notes/today.txt", b"hello")
body = await storage.read("notes/today.txt")
metadata = await storage.stat("notes/today.txt")
```

`VfsStorage.gateway()` selects the HTTP gateway backend. Local and gateway instances share the read, range-read, write, metadata, directory, link, rename, batch, and prefetch operations declared in `chevalier/__init__.pyi`.

## Development

```bash
cd py
python -m pip install maturin
maturin develop
cargo test --features pyo3/extension-module
pytest ../integration_tests
```
