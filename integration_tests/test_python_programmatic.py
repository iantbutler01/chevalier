import asyncio
import os
import signal

import chevalier
import pytest


def test_programmatic_execution_runs_code_dispatches_tools_and_reaps_process(tmp_path):
    async def scenario():
        events = asyncio.Queue()
        process = None
        reader = None
        calls = []
        finished = False

        async def pipe_output(stream, kind):
            while data := await stream.read(4096):
                await events.put({"type": kind, "text": data.decode()})

        async def collect_output():
            await asyncio.gather(
                pipe_output(process.stdout, "stdout"),
                pipe_output(process.stderr, "stderr"),
            )
            await events.put({"type": "exit", "code": await process.wait()})

        async def callback(request):
            nonlocal process, reader, finished
            operation = request["op"]
            if operation == "start":
                process = await asyncio.create_subprocess_shell(
                    request["command"], cwd=tmp_path, start_new_session=True,
                    stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                reader = asyncio.create_task(collect_output())
            elif operation == "write":
                process.stdin.write(request["data"].encode())
                await process.stdin.drain()
            elif operation == "next":
                if finished:
                    return None
                event = await events.get()
                finished = event["type"] == "exit"
                return event
            elif operation == "call":
                calls.append((request["name"], request["args"]))
                return {"value": 42}
            elif operation in ("signal", "cancel"):
                if process is not None and process.returncode is None:
                    os.killpg(process.pid, request.get("signal", signal.SIGKILL))
            else:
                raise AssertionError(operation)
            return None

        tools = [{"name": "read", "description": "Read a value", "schema": {"type": "object"}}]
        execution = chevalier.ProgrammaticExecution(callback)
        try:
            result = await asyncio.wait_for(
                execution.execute('text(await tools.read({key: "answer"}))', tools), 10
            )
            assert result["output"] == [{"value": 42}]
            assert calls == [("read", {"key": "answer"})]
            assert process.returncode == 0
            assert "read" in chevalier.programmatic_description(tools)
            with pytest.raises(chevalier.ChevalierError, match="only execute once"):
                await execution.execute("text(1)", tools)
        finally:
            execution.cancel()
            if process is not None and process.returncode is None:
                os.killpg(process.pid, signal.SIGKILL)
                await process.wait()
            if reader is not None:
                await reader

    asyncio.run(scenario())
