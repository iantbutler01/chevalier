const assert = require('node:assert/strict')
const { Sandbox } = require('../index.js')

async function main() {
  const endpoint = process.env.SANDBOX_ENDPOINT
  const sessionId = process.env.SANDBOX_TEST_SESSION_ID
  const distributedControl = JSON.parse(process.env.OPENBRACKET_SANDBOX_DISTRIBUTED_CONTROL || 'null')
  assert.ok(endpoint && sessionId && distributedControl, 'Set the sandbox endpoint, distributed control options and existing test session ID')
  const sandbox = await Sandbox.connect(endpoint, {
    authToken: process.env.SANDBOX_AUTH_TOKEN,
    defaultImage: process.env.SANDBOX_IMAGE,
    defaultArchitecture: process.env.SANDBOX_ARCHITECTURE,
    distributedControl,
  })
  const original = await sandbox.attachSession(sessionId)
  const drain = async handle => {
    let output = ''
    for (;;) {
      const event = await handle.next()
      assert.ok(event, 'exec stream ended without an exit event')
      if (event.type === 'stdout') output += Buffer.from(event.data).toString()
      if (event.type === 'exit') {
        return { code: event.code, output }
      }
      assert.notEqual(event.type, 'timeout')
    }
  }
  const run = async session => {
    const result = await drain(await session.exec('printf "fence-ok\\n"', {
      timeoutSecs: 15,
      closeStdinOnStart: true,
    }))
    assert.deepEqual(result, { code: 0, output: 'fence-ok\n' })
  }
  await run(original)
  const second = await sandbox.attachSession(sessionId)
  assert.equal(second.vmId, original.vmId)
  await run(original)
  console.log('PASS: cached handle executes after another attachment')

  const interactive = await original.exec('read line; printf "%s\\n" "$line"; cat', { timeoutSecs: 15 })
  await sandbox.attachSession(sessionId)
  await interactive.write(Buffer.from('fence-ok\n'))
  await interactive.eof()
  assert.deepEqual(await drain(interactive), { code: 0, output: 'fence-ok\n' })
  console.log('PASS: stdin and EOF survive another attachment while the command is running')

  assert.deepEqual(await drain(await original.exec('cat', { timeoutSecs: 15, closeStdinOnStart: true })), {
    code: 0, output: '',
  })
  console.log('PASS: noninteractive execution closes stdin at launch')

  const cancellable = await original.exec('exec sleep 30', { timeoutSecs: 30 })
  await sandbox.attachSession(sessionId)
  await cancellable.signal(15)
  assert.notEqual((await drain(cancellable)).code, 0)
  console.log('PASS: cancellation survives another attachment while the command is running')
}

const deadline = setTimeout(() => {
  console.error('FAIL: cached session regression did not complete within 60 seconds')
  process.exit(1)
}, 60000)
main().then(() => {
  clearTimeout(deadline)
  process.exit(0)
}, error => {
  console.error(error)
  process.exit(1)
})
