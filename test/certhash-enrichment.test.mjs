import assert from 'node:assert/strict'
import { test } from 'node:test'
import { enrichBrowserTransportCerthash } from '../dist/http/pinning-http.js'

const PEER = '/p2p/12D3KooWExample'

test('grafts the listener certhash onto bare webrtc-direct announce addresses', () => {
	const input = [
		`/ip4/127.0.0.1/udp/9093/webrtc-direct/certhash/uEiAAAA${PEER}`,
		`/ip4/46.255.204.201/udp/44574/webrtc-direct${PEER}`,
	]
	const out = enrichBrowserTransportCerthash(input)
	assert.equal(out.length, 2)
	assert.ok(out.includes(`/ip4/46.255.204.201/udp/44574/webrtc-direct/certhash/uEiAAAA${PEER}`))
})

test('drops bare webrtc-direct addresses when no listener certhash exists', () => {
	const input = [
		`/ip4/46.255.204.201/tcp/44577/ws${PEER}`,
		`/ip4/46.255.204.201/udp/44574/webrtc-direct${PEER}`,
		`/ip6/2a01::1/udp/9093/webrtc-direct${PEER}`,
	]
	const out = enrichBrowserTransportCerthash(input)
	assert.deepEqual(out, [`/ip4/46.255.204.201/tcp/44577/ws${PEER}`])
})

test('keeps webtransport double certhash intact when grafting', () => {
	const input = [
		`/ip4/127.0.0.1/udp/9095/quic-v1/webtransport/certhash/uEiA1/certhash/uEiA2${PEER}`,
		`/ip4/46.255.204.201/udp/44576/quic-v1/webtransport${PEER}`,
	]
	const out = enrichBrowserTransportCerthash(input)
	assert.ok(
		out.includes(`/ip4/46.255.204.201/udp/44576/quic-v1/webtransport/certhash/uEiA1/certhash/uEiA2${PEER}`)
	)
})

test('leaves non-browser transports untouched', () => {
	const input = [`/ip4/1.2.3.4/tcp/4001${PEER}`, `/ip4/1.2.3.4/udp/4002/quic-v1${PEER}`]
	assert.deepEqual(enrichBrowserTransportCerthash(input), input)
})
