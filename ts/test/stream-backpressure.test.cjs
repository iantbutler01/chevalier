const { test } = require("node:test");
const assert = require("node:assert/strict");
const http = require("node:http");
const { once } = require("node:events");
const { setTimeout: delay } = require("node:timers/promises");
const { Runtime } = require("../index.js");

test("slow consumers bound growing tool snapshots and cancellation releases the runtime", { timeout: 15000 }, async (context) => {
  let requests = 0;
  const server = http.createServer(async (request, response) => {
    for await (const chunk of request) {}
    response.writeHead(200, { "Content-Type": "text/event-stream" });
    const emit = (delta, finish_reason = null) => response.write(`data: ${JSON.stringify({ choices: [{ index: 0, delta, finish_reason }] })}\n\n`);
    if (++requests === 1) {
      emit({ tool_calls: [{ index: 0, id: "call-large", type: "function", function: { name: "delegate", arguments: '{"task":"' } }] });
      for (let index = 0; index < 1500; index++) {
        emit({ tool_calls: [{ index: 0, function: { arguments: "x".repeat(128) } }] });
      }
      emit({ tool_calls: [{ index: 0, function: { arguments: '"}' } }] });
      emit({}, "tool_calls");
    } else {
      for (let index = 0; index < 100; index++) emit({ content: "x" });
      emit({}, "stop");
    }
    response.end("data: [DONE]\n\n");
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  context.after(() => { server.closeAllConnections(); server.close(); });
  const runtime = new Runtime({
    model: `custom-openai:fixture@server_url=http://127.0.0.1:${server.address().port}/chat/completions`,
    apiKey: "fixture",
  });
  await runtime.tool({ name: "delegate", schema: { type: "object", properties: { task: { type: "string" } } } });
  const controller = new AbortController();
  const stream = runtime.runStream({ prompt: "start", signal: controller.signal });
  const before = process.memoryUsage().rss;
  assert.equal((await stream.next()).done, false);
  await delay(1000);
  const growth = process.memoryUsage().rss - before;
  controller.abort();
  await stream.return();
  assert.ok(growth < 96 * 1024 * 1024, `queued snapshots grew RSS by ${growth} bytes`);
  let text = "";
  for await (const event of runtime.runStream({ prompt: "again" })) {
    if (event.type === "content") { text += event.text; await delay(1); }
  }
  assert.equal(text, "x".repeat(100));
});
