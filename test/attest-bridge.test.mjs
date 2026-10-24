import assert from 'node:assert/strict'
import { test } from 'node:test'
import { createServer } from 'node:http'
import { createLibp2p } from 'libp2p'
import { tcp } from '@libp2p/tcp'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { WebSocketServer } from 'ws'
import { attestBridgeService, ATTEST_PROTOCOL, clientAddressFor, parseAttestHeader } from '../dist/services/attest-bridge-service.js'

const CRN = 'a'.repeat(64)
const VM = 'b'.repeat(64)

test('the header names a node and an instance by their hashes', () => {
	assert.deepEqual(parseAttestHeader(JSON.stringify({ crn: CRN, vm: VM })), { crn: CRN, vm: VM })
	assert.equal(parseAttestHeader(JSON.stringify({ crn: 'x', vm: VM })), null)
	assert.equal(parseAttestHeader('not json'), null)
})

test('clients are counted by their public IP, else by peer id', () => {
	assert.equal(clientAddressFor('/ip4/203.0.113.7/tcp/4001', 'peer'), '203.0.113.7')
	const a = clientAddressFor('/ip4/127.0.0.1/tcp/9092/ws', 'peer-a')
	const b = clientAddressFor('/ip4/127.0.0.1/tcp/9092/ws', 'peer-b')
	assert.match(a, /^fd00:/)
	assert.notEqual(a, b)
})

/** A stand-in for relay-button's attest relay: checks what it is asked, echoes the bytes reversed per chunk. */
async function fakeAttestRelay() {
	const server = createServer()
	const wss = new WebSocketServer({ server, path: '/attest' })
	const seen = []
	wss.on('connection', (ws, request) => {
		const url = new URL(request.url, 'http://x')
		seen.push({ crn: url.searchParams.get('crn'), vm: url.searchParams.get('vm'), forwarded: request.headers['x-forwarded-for'] })
		ws.on('message', (data) => ws.send(Buffer.from(data).reverse(), { binary: true }))
	})
	await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
	return { url: `ws://127.0.0.1:${server.address().port}/attest`, seen, close: () => new Promise((r) => { wss.close(); server.close(r) }) }
}

const node = (services = {}) =>
	createLibp2p({ addresses: { listen: ['/ip4/127.0.0.1/tcp/0'] }, transports: [tcp()], connectionEncrypters: [noise()], streamMuxers: [yamux()], services })

test('a stream reaches the attest relay with the named target, and bytes flow both ways', async () => {
	const relay = await fakeAttestRelay()
	const server = await node({ attestBridge: attestBridgeService({ bridgeUrl: relay.url }) })
	const client = await node()
	try {
		const stream = await client.dialProtocol(server.getMultiaddrs()[0], ATTEST_PROTOCOL)
		stream.send(new TextEncoder().encode(JSON.stringify({ crn: CRN, vm: VM }) + '\nhello'))
		const iterator = stream[Symbol.asyncIterator]()
		const first = await iterator.next()
		assert.equal(new TextDecoder().decode(first.value.subarray()), 'olleh')
		stream.send(new TextEncoder().encode('world'))
		const second = await iterator.next()
		assert.equal(new TextDecoder().decode(second.value.subarray()), 'dlrow')
		await stream.close()
		assert.equal(relay.seen.length, 1)
		assert.equal(relay.seen[0].crn, CRN)
		assert.equal(relay.seen[0].vm, VM)
		assert.match(relay.seen[0].forwarded, /^fd00:/)
	} finally {
		await client.stop()
		await server.stop()
		await relay.close()
	}
})

test('a bad header closes the stream without asking the attest relay', async () => {
	const relay = await fakeAttestRelay()
	const server = await node({ attestBridge: attestBridgeService({ bridgeUrl: relay.url }) })
	const client = await node()
	try {
		const stream = await client.dialProtocol(server.getMultiaddrs()[0], ATTEST_PROTOCOL)
		stream.send(new TextEncoder().encode('{"crn":"x"}\n'))
		const next = await stream[Symbol.asyncIterator]().next()
		assert.equal(next.done, true)
		assert.equal(relay.seen.length, 0)
	} finally {
		await client.stop()
		await server.stop()
		await relay.close()
	}
})
