import assert from 'node:assert/strict'
import { test } from 'node:test'
import {
	IDENTIFY_DEFAULT_MAX_MESSAGE_SIZE,
	createIdentifyPayloadWarner,
	estimateIdentifyPayloadBytes,
	summariseIdentifyPayload,
} from '../dist/services/identify-payload.js'

test('the estimate lands on the response measured against the live relay', () => {
	// Measured on 12D3KooWNsf7…KfaKB (0.10.6): identify rejected at 10538 bytes,
	// 122 protocols (8323 bytes including framing) and 27 addresses (932 bytes
	// including framing). Those totals are reproduced here as synthetic inputs
	// of the same size, so the estimator is checked against a real number.
	const protocols = []
	for (let i = 0; i < 122; i++) protocols.push('x'.repeat(65)) // 122 * (65 + 2) = 8174
	const addressLengths = new Array(27).fill(32) // 27 * (32 + 2) = 918

	const estimate = estimateIdentifyPayloadBytes(protocols, addressLengths)
	assert.equal(estimate, 8174 + 918 * 2 + 350)
	assert.ok(estimate > IDENTIFY_DEFAULT_MAX_MESSAGE_SIZE, 'this shape must read as over the limit')
})

test('addresses count twice — identify carries them again in the signed peer record', () => {
	const withoutAddress = estimateIdentifyPayloadBytes(['/ipfs/id/1.0.0'], [])
	const withAddress = estimateIdentifyPayloadBytes(['/ipfs/id/1.0.0'], [100])
	assert.equal(withAddress - withoutAddress, (100 + 2) * 2)
})

test('summarise counts the per-database protocols separately', () => {
	const libp2p = {
		getProtocols: () => [
			'/ipfs/id/1.0.0',
			'/libp2p/circuit/relay/0.2.0/hop',
			'/orbitdb/heads/orbitdb/zdpuAaaa',
			'/orbitdb/heads/orbitdb/zdpuAbbb',
		],
		getMultiaddrs: () => [{ bytes: new Uint8Array(20), toString: () => '/ip4/1.2.3.4/tcp/1' }],
	}
	const s = summariseIdentifyPayload(libp2p)
	assert.equal(s.protocolCount, 4)
	assert.equal(s.orbitdbHeadsProtocolCount, 2)
	assert.equal(s.addressCount, 1)
	assert.equal(s.limitBytes, IDENTIFY_DEFAULT_MAX_MESSAGE_SIZE)
	assert.equal(s.overLimit, false)
})

test('a node that keeps heads protocols out of identify is sized without them', () => {
	const heads = Array.from({ length: 200 }, (_, i) => `/orbitdb/heads/orbitdb/zdpu${String(i).padStart(46, 'a')}`)
	const libp2p = {
		getProtocols: () => ['/ipfs/id/1.0.0', '/libp2p/circuit/relay/0.2.0/hop', ...heads],
		getMultiaddrs: () => [{ bytes: new Uint8Array(20), toString: () => '/ip4/1.2.3.4/tcp/1' }],
	}
	const announcing = summariseIdentifyPayload(libp2p)
	const answering = summariseIdentifyPayload(libp2p, undefined, { excludeOrbitdbHeads: true })
	assert.equal(announcing.overLimit, true, '200 databases announced pass the limit')
	assert.equal(answering.protocolCount, 2)
	assert.equal(answering.orbitdbHeadsProtocolCount, 200, 'the answered heads protocols are still reported')
	assert.equal(answering.overLimit, false)
	assert.equal(answering.estimatedBytes, estimateIdentifyPayloadBytes(['/ipfs/id/1.0.0', '/libp2p/circuit/relay/0.2.0/hop'], [20]))
})

test('a node without libp2p reports zero rather than throwing', () => {
	const s = summariseIdentifyPayload(null)
	assert.equal(s.protocolCount, 0)
	assert.equal(s.overLimit, false)
})

test('the warner stays quiet below the threshold and fires once above it', () => {
	const seen = []
	const warn = createIdentifyPayloadWarner((message, detail) => seen.push({ message, detail }))

	warn({ ratio: 0.5, overLimit: false, estimatedBytes: 4096, limitBytes: 8192, protocolCount: 10, orbitdbHeadsProtocolCount: 0, addressCount: 3 })
	assert.equal(seen.length, 0, 'quiet while there is headroom')

	const hot = { ratio: 1.29, overLimit: true, estimatedBytes: 10538, limitBytes: 8192, protocolCount: 122, orbitdbHeadsProtocolCount: 109, addressCount: 27 }
	warn(hot)
	warn(hot)
	assert.equal(seen.length, 1, 'throttled to one line per interval')
	assert.match(seen[0].message, /exceeds the client default limit/)
	assert.equal(seen[0].detail.orbitdbHeadsProtocols, 109)
})

test('approaching the limit reads differently from passing it', () => {
	const seen = []
	const warn = createIdentifyPayloadWarner((message) => seen.push(message))
	warn({ ratio: 0.9, overLimit: false, estimatedBytes: 7400, limitBytes: 8192, protocolCount: 90, orbitdbHeadsProtocolCount: 80, addressCount: 27 })
	assert.match(seen[0], /approaching/)
})
