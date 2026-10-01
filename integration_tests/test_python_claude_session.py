import asyncio
from pathlib import Path

import pytest
from chevalier import Runtime, ChevalierError, ClaudeInitEvent, claude_subscription_status


CLI = Path(__file__).resolve().parents[1] / "rust/tests/fixtures/claude-subscription/fake-claude.cjs"


async def make_runtime():
    runtime = Runtime()

    async def read_file(path: str):
        assert path == "a.txt"
        return "alpha-7"

    await runtime.tool(read_file, name="read_file", description="Read file")
    await runtime.register_tool_schema("screenshot", "Screenshot", {"type": "object", "additionalProperties": True})
    return runtime


def config(tmp_path, mode):
    return {"model": "sonnet", "cli_path": str(CLI), "cwd": str(tmp_path / mode),
            "client_app": "chevalier-test", "server_name": "ob", "idle_timeout_ms": 2000}


@pytest.mark.asyncio
async def test_status_is_read_only():
    status = await claude_subscription_status(str(CLI))
    assert status["status"] == "ready"
    assert status["subscription_type"] == "max"


@pytest.mark.asyncio
async def test_handler_and_async_iteration(tmp_path):
    runtime = await make_runtime()
    session = await runtime.claude_session(config(tmp_path, "happy"))
    await session.send({"text": "go"})
    seen = set()
    async for event in session:
        if event["type"] == "init":
            assert isinstance(event, ClaudeInitEvent)
        seen.add(event["type"])
        if event["type"] == "toolCall":
            await session.respond_tool(event["callId"], {"content": [{"type": "image", "data": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8DwHwAFBQIAX8jx0gAAAABJRU5ErkJggg==", "mimeType": "image/png"}]})
        if event["type"] == "turnComplete":
            break
    assert {"init", "toolExecuted", "toolCall", "rateLimits", "turnComplete"} <= seen
    await session.close()
    await runtime.dispose()


@pytest.mark.asyncio
async def test_error_code(tmp_path):
    runtime = await make_runtime()
    session = await runtime.claude_session(config(tmp_path, "api-key"))
    with pytest.raises(ChevalierError) as error:
        await session.next()
    assert error.value.code == "CLAUDE_NOT_SUBSCRIPTION"
    await session.close()
    await runtime.dispose()
