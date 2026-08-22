from typing import List, Optional

from typing_extensions import assert_type

from chevalier import (
    AssistantResponse,
    CompleteStreamEvent,
    OutputStreamEvent,
    ResponsePart,
    ResponseStreamEvent,
    Runtime,
    TextResponsePart,
)


async def response_contract(runtime: Runtime) -> None:
    async def add(left: int, right: int = 0) -> str:
        return str(left + right)

    async def record(value: str) -> None:
        pass

    await runtime.tool(add)
    await runtime.tool(add, name="sum", description="Add two numbers")
    await runtime.tool(record)

    response = await runtime.run({"prompt": "hello"})
    assert_type(response, AssistantResponse)
    assert_type(response.output, List[ResponsePart])

    stream = await runtime.run_stream({"prompt": "hello"})
    event: Optional[ResponseStreamEvent] = await stream.next()
    if isinstance(event, OutputStreamEvent):
        if isinstance(event.output, TextResponsePart):
            assert_type(event.output.text, str)
    elif isinstance(event, CompleteStreamEvent):
        assert_type(event.response, AssistantResponse)
