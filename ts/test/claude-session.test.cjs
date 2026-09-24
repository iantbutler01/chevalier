const { test } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const fs = require('node:fs');
const os = require('node:os');
const { Runtime, events, claudeSubscriptionStatus } = require('../index.js');

const cliPath = path.resolve(__dirname, '../../rust/tests/fixtures/claude-subscription/fake-claude.cjs');
function config(mode) { return { model: 'sonnet', cliPath, cwd: path.join(os.tmpdir(), 'chevalier-ts-claude', mode), clientApp: 'chevalier-test', serverName: 'ob', idleTimeoutMs: 2000 }; }
async function runtime() {
  const rt = new Runtime();
  await rt.tool({ name: 'read_file', description: 'Read file', schema: { type: 'object', additionalProperties: true }, handler: async () => 'alpha-7' });
  await rt.tool({ name: 'screenshot', description: 'Screenshot', schema: { type: 'object', additionalProperties: true } });
  return rt;
}
const image = { type: 'image', mimeType: 'image/png', dataBase64: 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8DwHwAFBQIAX8jx0gAAAABJRU5ErkJggg==' };

test('status uses the CLI auth-status command', async () => {
  const status = await claudeSubscriptionStatus(cliPath);
  assert.equal(status.status, 'ready');
  assert.equal(status.subscription_type, 'max');
});

test('schema-only call and handler-backed call complete a turn', async () => {
  const rt = await runtime();
  const session = await rt.claudeSession(config('happy'));
  await session.send({ text: 'go' });
  const seen = new Set();
  for await (const event of events(session)) {
    seen.add(event.type);
    if (event.type === 'toolCall') await session.respondTool(event.callId, { content: [image] });
    if (event.type === 'turnComplete') { assert.ok(event.usage.output_tokens > 0); break; }
  }
  for (const type of ['init', 'toolExecuted', 'toolCall', 'rateLimits', 'turnComplete']) assert.ok(seen.has(type), type);
  await rt.dispose();
});

test('steering is delivered during a pending call', async () => {
  const rt = await runtime();
  const session = await rt.claudeSession(config('steer'));
  await session.send({ text: 'go' });
  for await (const event of events(session)) {
    if (event.type === 'toolCall') { await session.send({ text: 'steer now' }); await session.respondTool(event.callId, { content: [image] }); }
    if (event.type === 'turnComplete') break;
  }
  await rt.dispose();
});

test('API-key init raises a stable error', async () => {
  const rt = await runtime();
  const session = await rt.claudeSession(config('api-key'));
  await assert.rejects(session.next(), { code: 'CLAUDE_NOT_SUBSCRIPTION' });
  await session.close();
  await rt.dispose();
});

test('interrupt cancels a pending host call', async () => {
  const rt = await runtime();
  const session = await rt.claudeSession(config('interrupt'));
  await session.send({ text: 'go' });
  let callId;
  let cancelled = false;
  for await (const event of events(session)) {
    if (event.type === 'toolCall') { callId = event.callId; await session.interrupt(); }
    if (event.type === 'toolCancelled') cancelled = true;
    if (event.type === 'turnComplete') { assert.equal(event.isError, true); break; }
  }
  assert.ok(cancelled);
  await assert.rejects(session.respondTool(callId, { content: [{ type: 'text', text: 'late' }] }), { code: 'CLAUDE_PROTOCOL' });
  await rt.dispose();
});

test('abort closes the child', async () => {
  const rt = await runtime();
  const session = await rt.claudeSession(config('idle'));
  const controller = new AbortController();
  const stream = events(session, { signal: controller.signal });
  const next = stream.next();
  controller.abort();
  await assert.rejects(next, /abort/i);
  const record = JSON.parse(fs.readFileSync(path.join(config('idle').cwd, 'fake-record.json')));
  assert.throws(() => process.kill(record.pid, 0), { code: 'ESRCH' });
  await rt.dispose();
});
