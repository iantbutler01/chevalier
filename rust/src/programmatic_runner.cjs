const readline = require('node:readline');
const lines = readline.createInterface({ input: process.stdin });
const pending = new Map();
let started = false;
let sequence = 0;
let marker;
let bytes = 0;
let emitted = 0;
let finished = false;
let writes = Promise.resolve();
const send = (frame) => {
  const data = marker + JSON.stringify(frame) + '\n';
  writes = writes.then(() => new Promise((resolve, reject) => process.stdout.write(data, (error) => error ? reject(error) : resolve())));
  writes.catch(() => process.exit(1));
};
const finish = (error) => {
  if (finished) return;
  finished = true;
  send(error ? { kind: 'error', message: String(error.message ?? error) } : { kind: 'done' });
  writes.then(() => process.exit(error ? 1 : 0));
};
lines.on('line', (line) => {
  try {
    const frame = JSON.parse(line);
    if (!started) {
      started = true;
      marker = frame.marker;
      const tools = Object.create(null);
      for (const name of frame.names) {
        tools[name] = (args = {}) => {
          if (finished) return Promise.reject(new Error('Program completed'));
          if (++sequence > 1000) return Promise.reject(new Error('Programmatic tool call limit exceeded'));
          if (Buffer.byteLength(JSON.stringify(args)) > 1048576) return Promise.reject(new Error('Tool arguments exceed size limit'));
          const id = sequence;
          const promise = new Promise((resolve, reject) => pending.set(id, { resolve, reject }));
          send({ kind: 'call', id, name, args });
          return promise;
        };
      }
      const text = (value) => {
        const json = JSON.stringify(value ?? null);
        bytes += Buffer.byteLength(json);
        if (++emitted > 1000 || bytes > 1048576) throw new Error('Program output exceeds size limit');
        send({ kind: 'output', value: JSON.parse(json) });
      };
      const run = new Function('tools', 'text', 'return (async () => {\n' + frame.code + '\n})()');
      Promise.resolve(run(Object.freeze(tools), text)).then(() => {
        finish(pending.size ? new Error('Program ended with unawaited tool calls; await every call') : undefined);
      }, finish);
    } else {
      const call = pending.get(frame.id);
      if (!call) throw new Error('Unknown tool result');
      pending.delete(frame.id);
      if (frame.error !== undefined) call.reject(new Error(frame.error));
      else call.resolve(frame.value);
    }
  } catch (error) {
    if (marker) finish(error);
    else process.exit(1);
  }
});
lines.on('close', () => process.exit(1));
process.on('unhandledRejection', finish);
