const assert = require('node:assert/strict')
const { randomUUID } = require('node:crypto')
const { Sandbox } = require('../index.js')

async function main() {
  const endpoint = process.env.SANDBOX_TEST_NODE_ENDPOINT
  const etcd = process.env.SANDBOX_TEST_ETCD_HTTP_URL
  const nats = process.env.SANDBOX_TEST_NATS_URL
  assert.ok(endpoint && etcd && nats, 'Set SANDBOX_TEST_NODE_ENDPOINT, SANDBOX_TEST_ETCD_HTTP_URL and SANDBOX_TEST_NATS_URL')
  const prefix = `/chevalier-node-bound-test/${randomUUID()}`
  const sessionId = randomUUID()
  const encode = value => Buffer.from(value).toString('base64')
  const request = async (operation, body) => {
    const response = await fetch(`${etcd}/v3/kv/${operation}`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body),
    })
    assert.equal(response.status, 200)
    const result = await response.json()
    assert.equal(result.error, undefined)
  }
  const put = (key, value) => request('put', { key: encode(key), value: encode(JSON.stringify(value)) })
  try {
    await put(`${prefix}/nodes/healthy`, { node_id: 'healthy', endpoint, continuity_tier: 'tier-a' })
    await put(`${prefix}/sessions/${sessionId}`, {
      session_id: sessionId, vm_id: randomUUID(), endpoint: 'http://127.0.0.1:1', node_id: 'unavailable', tier_b_eligible: false,
    })
    const connect = allowCrossNodeRecovery => Sandbox.connect(endpoint, {
      defaultImage: process.env.SANDBOX_TEST_IMAGE || 'ubuntu:24.04',
      authToken: process.env.SANDBOX_TEST_AUTH_TOKEN,
      connectTimeoutMs: 1000,
      distributedControl: {
        etcdEndpoints: [etcd], natsUrl: nats, natsAuthToken: process.env.SANDBOX_TEST_NATS_AUTH_TOKEN,
        etcdPrefix: prefix, requiredContinuityTier: 'tier-a', allowCrossNodeRecovery,
      },
    })
    const nodeBound = await connect(false)
    await assert.rejects(nodeBound.attachSessionPassive(sessionId), error => {
      assert.doesNotMatch(error.message, /session not found/i)
      return true
    })
    const recoverable = await connect(true)
    await assert.rejects(recoverable.attachSessionPassive(sessionId), /session not found/i)
    console.log('PASS: unavailable node-bound owner remains an error despite a healthy secondary lookup miss')
  } finally {
    await request('deleterange', { key: encode(`${prefix}/`), range_end: encode(`${prefix}0`) })
  }
}
main().then(() => process.exit(0), error => { console.error(error); process.exit(1) })
