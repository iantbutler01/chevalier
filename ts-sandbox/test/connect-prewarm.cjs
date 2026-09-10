const assert = require('node:assert/strict')
const http2 = require('node:http2')
const { once } = require('node:events')
const { setTimeout: delay } = require('node:timers/promises')
const { Sandbox } = require('../index.js')

async function main() {
  const server = http2.createServer()
  const sessions = new Set()
  const pendingImages = []
  server.on('session', session => {
    sessions.add(session)
    session.on('close', () => sessions.delete(session))
  })
  server.on('stream', (stream, headers) => {
    stream.on('error', () => {})
    stream.resume()
    stream.respond({ ':status': 200, 'content-type': 'application/grpc' }, { waitForTrailers: true })
    stream.on('wantTrailers', () => stream.sendTrailers({ 'grpc-status': '0' }))
    if (headers[':path'] === '/vmd.v1.VMDService/Health') {
      stream.end(Buffer.alloc(5))
    } else if (headers[':path'] === '/vmd.v1.VMDService/PreDownloadVmImage') {
      pendingImages.push(stream)
    } else {
      assert.fail(`Unexpected RPC: ${headers[':path']}`)
    }
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const endpoint = `http://127.0.0.1:${server.address().port}`
  const options = { defaultImage: 'test.invalid/uncached:latest', connectTimeoutMs: 1000 }
  try {
    let defaultConnected = false
    const defaultConnection = Sandbox.connect(endpoint, options).then(value => {
      defaultConnected = true
      return value
    })
    while (pendingImages.length === 0) await delay(10)
    await Sandbox.connect(endpoint, { ...options, prewarmOnStart: false })
    assert.equal(defaultConnected, false, 'Default connection must still await its image stream')
    assert.equal(pendingImages.length, 1, 'Disabled prewarming must not request image preparation')
    pendingImages.shift().end()
    await defaultConnection
    assert.equal(defaultConnected, true)
    console.log('PASS: native connect skips blocked image preparation only when prewarmOnStart is false')
  } finally {
    for (const session of sessions) session.destroy()
    await new Promise(resolve => server.close(resolve))
  }
}

const deadline = setTimeout(() => {
  console.error('FAIL: native startup regression timed out')
  process.exit(1)
}, 10000)
main().then(() => {
  clearTimeout(deadline)
  process.exit(0)
}, error => {
  console.error(error)
  process.exit(1)
})
