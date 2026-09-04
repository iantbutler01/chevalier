const assert = require('node:assert/strict');
const { Runtime } = require('../index.js');

async function main() {
  const runtime = new Runtime({ model: `openai:gpt-6-astra@server_url=${process.env.CHEVALIER_ASTRA_FIXTURE}`, apiKey: 'fixture' });
  if (process.env.CHEVALIER_ASTRA_FIXTURE_MODE === 'async') {
    await runtime.tool({ name: 'lookup', async: true, schema: { type: 'object', properties: {} }, handler: async () => 'EXECUTED' });
    let text = '';
    let executions = 0;
    for await (const event of runtime.runStream({ prompt: 'work', responses: { websocket: true } })) {
      if (event.type === 'toolCall') {
        assert.equal(await runtime.executeToolCall(event.toolCall.toolName, event.toolCall.args), 'EXECUTED');
        executions += 1;
        process.stdout.write('EXECUTED\n');
      }
      if (event.type === 'content') text += event.text;
    }
    assert.equal(executions, 1);
    assert.equal(text, 'DONE');
    return;
  }
  const controller = new AbortController();
  const statuses = [];
  let control;
  let sent = false;
  await assert.rejects(async () => {
    for await (const event of runtime.runStream({ prompt: 'work', responses: { websocket: true }, signal: controller.signal, onControl: (value) => { control = value; } })) {
      if (event.type === 'responseId' && !sent) {
        sent = true;
        await control.steer('Change direction');
      }
      if (event.type === 'steering') statuses.push(event.data.status);
      if (event.type === 'content') setTimeout(() => controller.abort(), 10);
    }
  }, { name: 'AbortError' });
  assert.deepEqual(statuses, ['accepted', 'applied']);
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
