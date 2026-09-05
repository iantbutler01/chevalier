const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { once } = require('node:events');
const { Runtime } = require('../index.js');

async function fixture(context, handler) {
  const requests = [];
  const server = http.createServer(async (request, response) => {
    let body = '';
    for await (const chunk of request) body += chunk;
    const parsed = JSON.parse(body);
    assert.equal(request.url, '/v1/responses');
    requests.push(parsed);
    response.writeHead(200, { 'Content-Type': 'text/event-stream' });
    const emit = (value) => response.write(`data: ${JSON.stringify(value)}\n\n`);
    await handler({ request, response, emit, parsed, requests });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  context.after(() => { server.closeAllConnections(); server.close(); });
  return { requests, model: `openai:gpt-6-astra@server_url=http://127.0.0.1:${server.address().port}/v1/responses` };
}

const items = [
  { type: 'reasoning', id: 'rs_1', encrypted_content: 'opaque', summary: [] },
  { type: 'message', id: 'msg_1', role: 'assistant', phase: 'commentary', status: 'completed', content: [{ type: 'output_text', text: 'working', annotations: [] }] },
  { type: 'function_call', id: 'fc_1', call_id: 'call_1', name: 'lookup', arguments: '{}', async: true, caller: { type: 'direct' } },
];

test('PDF document input reaches Responses as a file rather than an image or placeholder', { timeout: 10000 }, async (context) => {
  const { model, requests } = await fixture(context, async ({ response, emit }) => {
    emit({ type: 'response.output_text.delta', delta: 'read' });
    emit({ type: 'response.completed', response: { id: 'pdf', output: [] } });
    response.end();
  });
  const runtime = new Runtime({ model, apiKey: 'fixture' });
  for await (const event of runtime.runStream({ prompt: '', history: [{ type: 'multimodal', role: 'user', parts: [
    { type: 'text', text: 'Read this' },
    { type: 'document', documentBase64: 'JVBERi0xLjQ=', mimeType: 'application/pdf' },
  ] }] })) {}
  assert.deepEqual(requests[0].input[0].content[1], { type: 'input_file', filename: 'attachment.pdf', file_data: 'data:application/pdf;base64,JVBERi0xLjQ=' });
});

test('native state survives the binding and replay without duplicating fallback; async tools start before response finishes', { timeout: 10000 }, async (context) => {
  let release;
  const dispatched = new Promise((resolve) => { release = resolve; });
  const { model, requests } = await fixture(context, async ({ response, emit, requests }) => {
    emit({ type: 'response.output_text.delta', delta: 'working' });
    if (requests.length === 1) {
      emit({ type: 'response.output_item.added', output_index: 2, item: { ...items[2], arguments: '' } });
      emit({ type: 'response.function_call_arguments.done', output_index: 2, arguments: '{}' });
      await dispatched;
    }
    for (const index of [2, 0, 1]) emit({ type: 'response.output_item.done', output_index: index, item: items[index] });
    emit({ type: 'response.completed', response: { id: 'resp_1', model: 'gpt-6-astra-snapshot', output: requests.length === 1 ? [] : undefined } });
    response.end();
  });
  const runtime = new Runtime({ model: `${model}@reasoning=max`, apiKey: 'fixture' });
  await runtime.tool({ name: 'lookup', schema: { type: 'object', properties: {} }, async: true });
  assert.equal((await runtime.getToolSchemas())[0].async, true);
  let nativeState;
  let calls = 0;
  for await (const event of runtime.runStream({ prompt: 'start', temperature: 0.7, topP: 1 })) {
    if (event.type === 'toolCall') {
      calls += 1;
      assert.equal(event.toolCall.async, true);
      assert.equal(event.toolCall.providerMetadata.caller.type, 'direct');
      release();
    }
    if (event.type === 'responseItems') nativeState = event.data;
  }
  assert.equal(calls, 1);
  assert.deepEqual(nativeState.items, items);
  assert.equal(nativeState.model, 'gpt-6-astra');
  assert.equal(requests[0].temperature, undefined);
  assert.equal(requests[0].top_p, undefined);
  assert.equal(requests[0].reasoning.effort, 'max');
  assert.equal(requests[0].context_management, undefined);
  assert.equal(requests[0].tools[0].async, true);
  const history = [{ type: 'assistantResponse', content: 'fallback', toolCalls: [{ toolUseId: 'call_1', toolName: 'lookup', args: '{}' }], providerResponse: nativeState }];
  for await (const event of runtime.runStream({ prompt: 'next', history })) {}
  assert.deepEqual(requests[1].input.slice(0, 3), items);
  assert.ok(!JSON.stringify(requests[1].input).includes('fallback'));
  for await (const event of runtime.runStream({ prompt: 'switch', history, model: model.replace('gpt-6-astra', 'resp:gpt-4') })) {}
  assert.ok(JSON.stringify(requests[2].input).includes('fallback'));
  assert.ok(!JSON.stringify(requests[2].input).includes('opaque'));
  assert.equal(requests[2].tools[0].async, undefined);
});

test('native compaction is opt-in and replaces only the compacted prefix', { timeout: 10000 }, async (context) => {
  const { model, requests } = await fixture(context, async ({ response, emit }) => {
    emit({ type: 'response.output_text.delta', delta: 'done' });
    emit({ type: 'response.completed', response: { id: 'resp_2', output: [] } });
    response.end();
  });
  const runtime = new Runtime({ model, apiKey: 'fixture' });
  const compacted = { type: 'compaction', id: 'cmp_1', encrypted_content: 'packed' };
  const history = [
    { type: 'chat', role: 'user', content: 'old context' },
    { type: 'assistantResponse', content: 'fallback', providerResponse: { model: 'gpt-6-astra', items: [items[0], compacted, items[1]] } },
  ];
  for await (const event of runtime.runStream({ prompt: 'continue', history })) {}
  assert.equal(requests[0].context_management, undefined);
  for await (const event of runtime.runStream({ prompt: 'continue', history, responses: { compactionThreshold: 200000 } })) {}
  assert.deepEqual(requests[1].context_management, [{ type: 'compaction', compact_threshold: 200000 }]);
  assert.deepEqual(requests[1].input.slice(0, 2), [compacted, items[1]]);
  assert.ok(!JSON.stringify(requests[1].input).includes('old context'));
});

test('abort interrupts a silent provider and closes its request', { timeout: 10000 }, async (context) => {
  let closed;
  const disconnected = new Promise((resolve) => { closed = resolve; });
  const controller = new AbortController();
  const { model } = await fixture(context, async ({ response, emit }) => {
    response.on('close', closed);
    emit({ type: 'response.output_text.delta', delta: 'started' });
  });
  const runtime = new Runtime({ model, apiKey: 'fixture' });
  await assert.rejects(async () => {
    for await (const event of runtime.runStream({ prompt: 'wait', signal: controller.signal })) {
      if (event.type === 'content') setTimeout(() => controller.abort(), 20);
    }
  }, { name: 'AbortError' });
  await disconnected;
});

test('a failure after async dispatch is surfaced without retrying the call', { timeout: 10000 }, async (context) => {
  const { model, requests } = await fixture(context, async ({ response, emit }) => {
    emit({ type: 'response.output_item.added', output_index: 0, item: { ...items[2], arguments: '' } });
    emit({ type: 'response.function_call_arguments.done', output_index: 0, arguments: '{}' });
    emit({ type: 'response.failed', response: { error: { code: 'server_error', message: 'fixture failure' } } });
    response.end();
  });
  const runtime = new Runtime({ model, apiKey: 'fixture' });
  await runtime.tool({ name: 'lookup', schema: { type: 'object', properties: {} }, async: true });
  let executions = 0;
  await assert.rejects(async () => {
    for await (const event of runtime.runStream({ prompt: 'start' })) {
      if (event.type === 'toolCall') executions += 1;
    }
  }, /fixture failure/);
  assert.equal(executions, 1);
  assert.equal(requests.length, 1);
});

test('an already aborted invocation never starts a request', { timeout: 10000 }, async (context) => {
  const { model, requests } = await fixture(context, async ({ response }) => response.end());
  const runtime = new Runtime({ model, apiKey: 'fixture' });
  const controller = new AbortController();
  controller.abort();
  await assert.rejects(async () => {
    for await (const event of runtime.runStream({ prompt: 'start', signal: controller.signal })) {}
  }, { name: 'AbortError' });
  assert.equal(requests.length, 0);
});

test('Astra image input crosses the actual Runtime capability gate and Responses boundary', { timeout: 10000 }, async (context) => {
  const { model, requests } = await fixture(context, async ({ response, emit }) => {
    emit({ type: 'response.output_text.delta', delta: 'image received' });
    emit({ type: 'response.completed', response: { id: 'resp_image', output: [] } });
    response.end();
  });
  const runtime = new Runtime({ model, apiKey: 'fixture' });
  for await (const event of runtime.runStream({ prompt: 'inspect', history: [{ type: 'multimodal', role: 'user', parts: [{ type: 'image', imageUrl: 'https://example.invalid/image.png' }] }] })) {}
  assert.deepEqual(requests[0].input[0].content, [{ type: 'input_image', image_url: 'https://example.invalid/image.png' }]);
});
