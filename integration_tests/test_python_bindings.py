import asyncio
import json
import socket
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Literal

import chevalier
import pytest


def run(coroutine):
    return asyncio.run(coroutine)


def test_runtime_tool_registry_executes_python_coroutines():
    async def scenario():
        runtime = chevalier.Runtime({})
        calls = []

        async def add(left: int, right: int = 5) -> str:
            calls.append((left, right))
            return str(left + right)

        await runtime.tool(add, description="Add two numbers")
        assert await runtime.execute_tool_call("add", {"left": "2"}) == "7"
        assert calls == [(2, 5)]

        schemas = await runtime.get_tool_schemas()
        assert len(schemas) == 1
        assert schemas[0]["name"] == "add"
        assert schemas[0]["description"] == "Add two numbers"
        assert schemas[0]["parameters"]["properties"]["left"]["type"] == "integer"
        assert schemas[0]["parameters"]["properties"]["right"]["type"] == "integer"
        assert schemas[0]["parameters"]["required"] == ["left"]

        await runtime.register_tool_schema("host_only", "Host dispatched", {"type": "object"})
        try:
            await runtime.execute_tool_call("host_only", {})
        except chevalier.ChevalierError as error:
            assert error.code == "NON_RETRYABLE"
            assert error.retryable is False
            assert error.output is None
        else:
            raise AssertionError("schema-only tool unexpectedly executed")

        await runtime.dispose()
        assert await runtime.get_tool_schemas() == []

    run(scenario())


def test_runtime_tool_hydrates_nested_annotated_arguments():
    @dataclass
    class Location:
        city: str
        country: str

    async def scenario():
        runtime = chevalier.Runtime({})
        received = []

        async def weather(
            location: Location,
            unit: Literal["celsius", "fahrenheit"] = "celsius",
        ) -> str:
            received.append((location, unit))
            return f"{location.city}:{unit}"

        await runtime.tool(weather)
        result = await runtime.execute_tool_call(
            "weather",
            {"location": {"city": "Tokyo", "country": "JP"}},
        )

        assert result == "Tokyo:celsius"
        assert received == [(Location(city="Tokyo", country="JP"), "celsius")]

    run(scenario())


def test_runtime_tool_rejects_invalid_arguments_before_calling_handler():
    async def scenario():
        runtime = chevalier.Runtime({})
        called = False

        async def square(value: int) -> str:
            nonlocal called
            called = True
            return str(value * value)

        await runtime.tool(square)
        for arguments in (
            {"value": "not-an-int"},
            {"value": 2, "unexpected": True},
        ):
            try:
                await runtime.execute_tool_call("square", arguments)
            except chevalier.ChevalierError as error:
                assert error.code == "NON_RETRYABLE"
            else:
                raise AssertionError("invalid tool arguments reached the handler")
        assert called is False

    run(scenario())


def test_runtime_tool_requires_an_annotated_async_function():
    runtime = chevalier.Runtime({})

    def synchronous(value: int) -> str:
        return str(value)

    async def missing_annotation(value) -> str:
        return str(value)

    with pytest.raises(TypeError, match="async function"):
        runtime.tool(synchronous)
    with pytest.raises(TypeError, match="requires a type annotation"):
        runtime.tool(missing_annotation)


def test_runtime_tool_converts_none_results_and_rejects_other_types():
    async def scenario():
        runtime = chevalier.Runtime({})
        received = []

        async def record(value: str) -> None:
            received.append(value)

        await runtime.tool(record)
        assert await runtime.execute_tool_call("record", {"value": "saved"}) == ""
        assert received == ["saved"]

        async def invalid_result() -> dict[str, str]:
            return {"status": "saved"}

        await runtime.tool(invalid_result)
        with pytest.raises(
            chevalier.ChevalierError,
            match="tool handler must return a string or None",
        ):
            await runtime.execute_tool_call("invalid_result", {})

    run(scenario())


def test_chevalier_error_defaults_match_typescript_error():
    error = chevalier.ChevalierError("message")
    assert error.code == "ERROR"
    assert error.retryable is False
    assert error.output is None

    structured = chevalier.ChevalierError("invalid output", "OUTPUT_PARSE", False, "raw")
    assert str(structured) == "invalid output"
    assert structured.code == "OUTPUT_PARSE"
    assert structured.retryable is False
    assert structured.output == "raw"


class _OpenAiHandler(BaseHTTPRequestHandler):
    requests = []
    stream_release = threading.Event()

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["content-length"])))
        self.requests.append(body)
        if body.get("stream"):
            self.send_response(200)
            self.send_header("content-type", "text/event-stream")
            self.send_header("connection", "close")
            self.end_headers()
            chunk = {"choices": [{"index": 0, "delta": {"content": "first"}}]}
            self.wfile.write(f"data: {json.dumps(chunk)}\n\n".encode())
            self.wfile.flush()
            if "typed stream" in json.dumps(body):
                finished = {
                    "choices": [
                        {"index": 0, "delta": {}, "finish_reason": "stop"}
                    ]
                }
                self.wfile.write(f"data: {json.dumps(finished)}\n\n".encode())
                usage = {
                    "choices": [],
                    "usage": {
                        "prompt_tokens": 3,
                        "completion_tokens": 1,
                        "prompt_tokens_details": {"cached_tokens": 2},
                    },
                }
                self.wfile.write(f"data: {json.dumps(usage)}\n\n".encode())
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.flush()
                return
            self.stream_release.wait(5)
            return

        response = {
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "created": 0,
            "model": "test",
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "complete"},
                    "finish_reason": "stop",
                }
            ],
        }
        encoded = json.dumps(response).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, *_args):
        pass


def _openai_server():
    _OpenAiHandler.requests = []
    _OpenAiHandler.stream_release.clear()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _OpenAiHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def test_runtime_uses_core_request_path_and_preserves_history():
    server = _openai_server()

    async def scenario():
        runtime = chevalier.Runtime(
            {
                "model": f"custom-openai:test@server_url=http://127.0.0.1:{server.server_port}/v1",
                "api_key": "test",
            }
        )

        async def lookup(query: str) -> str:
            return query

        await runtime.tool(lookup, description="Lookup a value")
        result = await runtime.run(
            {
                "prompt": "next",
                "history": [
                    {"type": "reasoning", "content": "prior thought"},
                    {
                        "type": "toolResult",
                        "tool_use_id": "call-1",
                        "tool_name": "lookup",
                        "content": "prior result",
                    },
                ],
                "temperature": 0.25,
                "top_p": 0.9,
                "max_tokens": 20,
                "timeout_ms": 2_000,
            }
        )
        assert isinstance(result, chevalier.AssistantResponse)
        assert result.text() == "complete"
        assert result.reasoning() == ""
        assert result.tool_calls() == []
        assert result.signatures() == []
        assert len(result.output) == 1
        assert isinstance(result.output[0], chevalier.TextResponsePart)
        assert result.output[0].text == "complete"

    try:
        run(scenario())
    finally:
        server.shutdown()

    request = _OpenAiHandler.requests[0]
    assert request["tools"][0]["function"]["name"] == "lookup"
    assert request["tools"][0]["function"]["description"] == "Lookup a value"
    assert request["tools"][0]["function"]["parameters"]["properties"] == {
        "query": {"type": "string"}
    }
    assert request["tools"][0]["function"]["parameters"]["required"] == ["query"]
    assert request["messages"][:2] == [
        {"type": "reasoning", "content": "prior thought"},
        {
            "role": "tool",
            "tool_call_id": "call-1",
            "content": "prior result",
        },
    ]
    assert request["temperature"] == 0.25
    assert request["max_completion_tokens"] == 20


def test_stream_close_cancels_driver_and_releases_runtime():
    server = _openai_server()

    async def scenario():
        runtime = chevalier.Runtime(
            {
                "model": f"custom-openai:test@server_url=http://127.0.0.1:{server.server_port}/v1",
                "api_key": "test",
            }
        )
        stream = await runtime.run_stream({"prompt": "stream"})
        event = await asyncio.wait_for(stream.next(), 2)
        assert isinstance(event, chevalier.OutputStreamEvent)
        assert event.type == "output"
        assert isinstance(event.output, chevalier.TextResponsePart)
        assert event.output.text == "first"
        stream.close()
        result = await asyncio.wait_for(runtime.run({"prompt": "after close"}), 2)
        assert result.text() == "complete"

    try:
        run(scenario())
    finally:
        _OpenAiHandler.stream_release.set()
        server.shutdown()


def test_stream_errors_preserve_typed_error_contract():
    async def scenario():
        runtime = chevalier.Runtime({"model": "missing-provider:model"})
        stream = await runtime.run_stream({"prompt": "fail"})
        try:
            await stream.next()
        except chevalier.ChevalierError as error:
            assert error.code == "NON_RETRYABLE"
            assert error.retryable is False
            assert error.output is None
            assert not str(error).startswith("[")
        else:
            raise AssertionError("invalid provider stream did not raise")

    run(scenario())


def test_stream_complete_preserves_canonical_assistant_response():
    server = _openai_server()

    async def scenario():
        runtime = chevalier.Runtime(
            {
                "model": f"custom-openai:test@server_url=http://127.0.0.1:{server.server_port}/v1",
                "api_key": "test",
            }
        )
        stream = await runtime.run_stream({"prompt": "typed stream"})
        try:
            output = await asyncio.wait_for(stream.next(), 2)
            assert isinstance(output, chevalier.OutputStreamEvent)
            assert output.type == "output"
            assert isinstance(output.output, chevalier.TextResponsePart)
            assert output.output.text == "first"

            usage = await asyncio.wait_for(stream.next(), 2)
            assert isinstance(usage, chevalier.UsageStreamEvent)
            assert usage.type == "usage"
            assert isinstance(usage.usage, chevalier.TokenUsage)
            assert usage.usage.input_tokens == 3
            assert usage.usage.output_tokens == 1
            assert usage.usage.cached_tokens == 2
            assert usage.usage.total_tokens() == 4

            complete = await asyncio.wait_for(stream.next(), 2)
            assert isinstance(complete, chevalier.CompleteStreamEvent)
            assert complete.type == "complete"
            assert isinstance(complete.response, chevalier.AssistantResponse)
            assert complete.response.text() == "first"
            assert isinstance(complete.response.output[0], chevalier.TextResponsePart)
            assert complete.response.output[0].text == "first"
            assert await asyncio.wait_for(stream.next(), 2) is None
        finally:
            stream.close()

    try:
        run(scenario())
    finally:
        server.shutdown()


def test_mcp_http_server_and_client_execute_python_handler():
    async def scenario():
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()

        calls = []
        server = chevalier.McpServer("python-test", {"version": "0.0.1"})

        async def echo(args):
            calls.append(args)
            return f"echo:{args['value']}"

        await server.tool(
            "echo",
            "Echo a value",
            {
                "type": "object",
                "properties": {"value": {"type": "string"}},
                "required": ["value"],
            },
            echo,
        )
        serving = asyncio.ensure_future(server.serve("http", f"127.0.0.1:{port}"))
        try:
            client = None
            for _ in range(100):
                try:
                    client = await chevalier.McpClient.http(
                        f"http://127.0.0.1:{port}/mcp"
                    )
                    break
                except RuntimeError:
                    await asyncio.sleep(0.01)
            assert client is not None
            tools = await client.list_tools()
            result = await client.call_tool("echo", {"value": "yes"})
            assert tools["tools"][0]["name"] == "echo"
            assert result["content"][0]["text"] == "echo:yes"
            assert calls == [{"value": "yes"}]
        finally:
            serving.cancel()
            try:
                await serving
            except asyncio.CancelledError:
                pass

    run(scenario())
