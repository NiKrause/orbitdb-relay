// Limits per client by its own IP, also behind the proxy on the same machine,
// and the release of a relay reservation when its peer disconnects. Made-up
// client addresses from the documentation ranges.
import assert from 'node:assert/strict'
import http from 'node:http'

import { createLibp2p } from 'libp2p'
import { tcp } from '@libp2p/tcp'
import { webSockets } from '@libp2p/websockets'
import { identify } from '@libp2p/identify'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { circuitRelayServer, circuitRelayTransport } from '@libp2p/circuit-relay-v2'
import { multiaddr } from '@multiformats/multiaddr'
import WebSocket from 'ws'

const ATTACKER = '203.0.113.66'
const OTHER = '198.51.100.7'

const waitFor = async (check, ms = 10000) => {
  const until = Date.now() + ms
  while (Date.now() < until) {
    if (await check()) return true
    await new Promise((r) => setTimeout(r, 50))
  }
  return false
}

/** An upgrade to the node's WebSocket as the proxy would send it: 101, or the refusal's status. */
function upgrade(port, forwardedFor) {
  return new Promise((resolve) => {
    const ws = new WebSocket(`ws://127.0.0.1:${port}`, { headers: { 'x-forwarded-for': forwardedFor } })
    ws.once('open', () => resolve({ status: 101, ws }))
    ws.once('unexpected-response', (_req, res) => resolve({ status: res.statusCode, ws }))
    ws.once('error', () => resolve({ status: 0, ws }))
  })
}

describe('limits per client', function () {
  this.timeout(60000)

  let limits

  before(async () => {
    limits = await import('../dist/services/client-limits.js')
  })

  it('takes the client from X-Forwarded-For only from the proxy on this machine', () => {
    assert.equal(limits.clientIp('127.0.0.1', `10.0.0.1, ${ATTACKER}`), ATTACKER)
    assert.equal(limits.clientIp('::ffff:127.0.0.1', OTHER), OTHER)
    assert.equal(limits.clientIp('::1', undefined), '::1')
    // Anyone else names nobody but themselves.
    assert.equal(limits.clientIp(OTHER, ATTACKER), OTHER)
  })

  it('counts new attempts per window and open ones, per client', () => {
    let now = 0
    const limiter = limits.createClientLimiter({ perWindow: 2, windowMs: 1000, maxOpen: 3 }, () => now)
    const a = limiter.acquire(ATTACKER)
    const b = limiter.acquire(ATTACKER)
    assert.ok(a && b)
    assert.equal(limiter.acquire(ATTACKER), null, 'over the attempts per window')
    assert.ok(limiter.acquire(OTHER), 'another client is not affected')
    now = 1500
    const c = limiter.acquire(ATTACKER)
    assert.ok(c)
    now = 3000
    assert.equal(limiter.acquire(ATTACKER), null, 'three open')
    a.release()
    a.release()
    assert.equal(limiter.open(ATTACKER), 2, 'a slot is released once')
    assert.ok(limiter.acquire(ATTACKER))
  })

  it('refuses a client’s WebSocket upgrades over its limits, and not another client’s', async () => {
    const node = await createLibp2p({
      addresses: { listen: ['/ip4/127.0.0.1/tcp/0/ws'] },
      transports: [webSockets()],
      connectionEncrypters: [noise()],
      streamMuxers: [yamux()],
    })
    const opened = []
    try {
      const limiter = limits.createClientLimiter({ perWindow: 2, windowMs: 60000, maxOpen: 2 })
      assert.equal(limits.guardWebSocketUpgrades(node, limiter), 1)
      const port = Number(node.getMultiaddrs()[0].toString().split('/')[4])

      for (let i = 0; i < 2; i++) {
        const { status, ws } = await upgrade(port, ATTACKER)
        opened.push(ws)
        assert.equal(status, 101)
      }
      const refused = await upgrade(port, ATTACKER)
      opened.push(refused.ws)
      assert.equal(refused.status, 429)

      const other = await upgrade(port, OTHER)
      opened.push(other.ws)
      assert.equal(other.status, 101, 'one client over its limits locks nobody else out')
    } finally {
      for (const ws of opened) ws.terminate()
      await node.stop()
    }
  })

  it('releases a relay reservation when its peer disconnects', async () => {
    const relay = await createLibp2p({
      addresses: { listen: ['/ip4/127.0.0.1/tcp/0'] },
      transports: [tcp()],
      connectionEncrypters: [noise()],
      streamMuxers: [yamux()],
      services: { identify: identify(), relay: circuitRelayServer() },
    })
    const stop = limits.releaseReservationsOnDisconnect(relay)
    const client = await createLibp2p({
      addresses: { listen: [`${relay.getMultiaddrs()[0].toString()}/p2p-circuit`] },
      transports: [tcp(), circuitRelayTransport()],
      connectionEncrypters: [noise()],
      streamMuxers: [yamux()],
      services: { identify: identify() },
    })
    try {
      assert.ok(await waitFor(() => relay.services.relay.reservations.size === 1), 'reserved')
      await client.hangUp(relay.peerId)
      await client.stop()
      assert.ok(await waitFor(() => relay.services.relay.reservations.size === 0), 'released on disconnect')
    } finally {
      stop()
      await client.stop().catch(() => {})
      await relay.stop()
    }
  })

  it('answers 429 to a client over its limits on the routes that do work, not to another', async () => {
    const { createPinningHttpRequestHandler } = await import('../dist/http/pinning-http.js')
    const limiter = limits.createClientLimiter({ perWindow: 2, windowMs: 60000, maxOpen: 2 })
    const handler = createPinningHttpRequestHandler({ clientLimiter: limiter })
    const server = http.createServer((req, res) => void handler(req, res))
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
    const { port } = server.address()
    const get = (path, forwardedFor) =>
      fetch(`http://127.0.0.1:${port}${path}`, { headers: { 'x-forwarded-for': forwardedFor } }).then(
        (r) => r.status,
      )
    const cid = 'bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy'
    try {
      assert.notEqual(await get(`/ipfs/${cid}`, ATTACKER), 429)
      assert.notEqual(await get(`/ipfs/${cid}`, ATTACKER), 429)
      assert.equal(await get(`/ipfs/${cid}`, ATTACKER), 429)
      assert.notEqual(await get(`/ipfs/${cid}`, OTHER), 429)
      // What does no work stays open.
      assert.equal(await get('/health', ATTACKER), 200)
    } finally {
      await new Promise((r) => server.close(r))
    }
  })
})
