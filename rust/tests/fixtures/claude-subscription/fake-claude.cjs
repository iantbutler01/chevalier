#!/usr/bin/env node
const fs = require('node:fs');
const readline = require('node:readline');
const path = require('node:path');

const mode = process.env.CHEVALIER_FAKE_CLAUDE_MODE || path.basename(process.cwd());
if (process.argv.includes('--version')) { console.log('2.1.281 (Claude Code)'); process.exit(0); }
if (process.argv.slice(2).join(' ') === 'auth status --json') {
  console.log(JSON.stringify({ loggedIn: true, authMethod: mode === 'console-login' ? 'console' : 'claude.ai', apiProvider: 'firstParty', email: 'test@example.invalid', subscriptionType: 'max' }));
  process.exit(0);
}

const forbidden = Object.keys(process.env).filter(k =>
  ['ANTHROPIC_API_KEY', 'ANTHROPIC_AUTH_TOKEN', 'ANTHROPIC_BASE_URL', 'CLAUDECODE', 'CLAUDE_PID', 'CLAUDE_EFFORT', 'CLAUDE_AGENT_SDK_VERSION'].includes(k) ||
  (k.startsWith('CLAUDE_CODE_') && !['CLAUDE_CODE_DISABLE_AUTO_MEMORY', 'CLAUDE_CODE_DISABLE_TERMINAL_TITLE'].includes(k)));
const argv = process.argv.slice(2);
const required = ['--output-format', 'stream-json', '--verbose', '--input-format', '--model', '--tools', '--allowedTools', '--setting-sources=', '--strict-mcp-config', '--permission-mode', '--include-partial-messages'];
if (forbidden.length || required.some(flag => !argv.includes(flag)) || argv[argv.indexOf('--tools') + 1] !== '' ||
    argv[argv.indexOf('--allowedTools') + 1] !== 'mcp__ob' ||
    argv[argv.indexOf('--permission-mode') + 1] !== 'default' ||
    process.env.CLAUDE_AGENT_SDK_CLIENT_APP !== 'chevalier-test' ||
    process.env.CLAUDE_CODE_DISABLE_AUTO_MEMORY !== '1' ||
    process.env.CLAUDE_CODE_DISABLE_TERMINAL_TITLE !== '1') {
  console.error(JSON.stringify({ forbidden, argv, app: process.env.CLAUDE_AGENT_SDK_CLIENT_APP }));
  process.exit(32);
}
const record = {
  pid: process.pid,
  argv,
  env: Object.fromEntries(['HOME', 'PATH', 'CLAUDE_AGENT_SDK_CLIENT_APP', 'CLAUDE_CODE_DISABLE_AUTO_MEMORY', 'CLAUDE_CODE_DISABLE_TERMINAL_TITLE']
    .filter(key => key in process.env).map(key => [key, process.env[key]])),
  forbidden,
  messages: [],
};
function save() { fs.writeFileSync(process.env.CHEVALIER_FAKE_CLAUDE_RECORD || path.join(process.cwd(), 'fake-record.json'), JSON.stringify(record)); }
function emit(value) { process.stdout.write(JSON.stringify(value) + '\n'); }
const fixture = fs.readFileSync(path.join(__dirname, 'happy.stdout.jsonl'), 'utf8').trim().split('\n').map(JSON.parse);
const select = (predicate) => fixture.find(predicate);
const init = select(x => x.type === 'system' && x.subtype === 'init');
const calls = fixture.filter(x => x.type === 'control_request' && x.request.subtype === 'mcp_message' && x.request.message.method === 'tools/call');
const result = select(x => x.type === 'result');
let replies = 0;
let pending = false;
let steered = false;
let initialized = false;
let interrupted = false;
const rl = readline.createInterface({ input: process.stdin });
rl.on('line', line => {
  let msg;
  try { msg = JSON.parse(line); } catch { process.exit(33); }
  record.messages.push(msg); save();
  if (msg.type === 'control_request' && msg.request.subtype === 'initialize') {
    const tools = msg.request.sdkMcpServerManifests?.ob?.toolsListResult?.tools;
    if (initialized || JSON.stringify(tools?.map(tool => tool.name)) !== JSON.stringify(['read_file', 'screenshot']) ||
        tools.some(tool => tool.execution?.taskSupport !== 'forbidden')) process.exit(34);
    initialized = true;
    emit({ type: 'control_response', response: { subtype: 'success', request_id: msg.request_id } });
    if (mode === 'crash-before-init') process.exit(31);
    if (mode === 'idle') return;
    if (mode === 'garbage') { for (let i = 0; i < 21; i++) process.stdout.write('garbage\n'); return; }
    const next = JSON.parse(JSON.stringify(init));
    if (mode === 'api-key') next.apiKeySource = 'ANTHROPIC_API_KEY';
    emit(next);
    return;
  }
  if (mode === 'api-key') { console.error('user turn before subscription validation'); process.exit(35); }
  if (msg.type === 'user') {
    if (!pending) {
      pending = true;
      if (mode === 'interrupt') emit(calls[2]);
      else { emit(calls[0]); emit(calls[2]); }
    } else steered = true;
    return;
  }
  if (msg.type === 'control_request' && msg.request.subtype === 'interrupt') {
    if (mode === 'interrupt') {
      emit({ type:'control_request', request_id:'cancel-1', request:{ subtype:'mcp_message', server_name:'ob', message:{ jsonrpc:'2.0', method:'notifications/cancelled', params:{ requestId:calls[2].request.message.id, reason:'interrupt' } } } });
      emit({ type:'control_response', response:{ subtype:'success', request_id:msg.request_id, response:{ still_queued:[] } } });
      interrupted = true;
    }
    return;
  }
  if (msg.type === 'control_response' && msg.response.request_id === 'cancel-1') {
    if (msg.response.response?.mcp_response?.id !== 0) process.exit(37);
    if (interrupted) emit({ ...result, is_error:true, subtype:'error_during_execution', result:null });
    return;
  }
  if (msg.type === 'control_response' && msg.response.response?.mcp_response?.result?.content) {
    replies++;
    if (replies === 2) {
      if (mode === 'steer' && !steered) process.exit(36);
      emit(select(x => x.type === 'rate_limit_event'));
      for (const line of fixture.filter(x => x.type === 'stream_event' && ['text_delta', 'thinking_delta'].includes(x.event?.delta?.type))) emit(line);
      emit(select(x => x.type === 'assistant' && x.message.content.some(p => p.type === 'text' && p.text.includes('alpha-7+'))));
      emit(result);
    }
  }
});
rl.on('close', () => { save(); process.exit(0); });
