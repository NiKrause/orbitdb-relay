/**
 * Shared helpers for relay + Alice + Bob OrbitDB replication tests (Node mocha).
 *
 * True WebAuthn worker vs hardware (varsig) identities need a browser or software test doubles
 * from `@le-space/orbitdb-identity-provider-webauthn-did`; this harness uses the same default
 * OrbitDB/Helia stack as `relay-media-replication.mjs` so we can still stress libp2p + relay pinning
 * per scenario label.
 */

import assert from 'node:assert/strict'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'

import { createLibp2p } from 'libp2p'
import { createHeliaLight } from 'helia'
import { withBitswap } from '@helia/bitswap'
import { withHTTP } from '@helia/http'
import { withLibp2p } from '@helia/libp2p'
import { createOrbitDB } from '@orbitdb/core'
import { gossipsub } from '@libp2p/gossipsub'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { identify } from '@libp2p/identify'
import { ping } from '@libp2p/ping'
import { circuitRelayTransport } from '@libp2p/circuit-relay-v2'
import { kadDHT, removePrivateAddressesMapper } from '@libp2p/kad-dht'
import { pubsubPeerDiscovery } from '@libp2p/pubsub-peer-discovery'
import { tcp } from '@libp2p/tcp'
import { webSockets } from '@libp2p/websockets'
import { CID } from 'multiformats/cid'
import { sha256, sha512 } from 'multiformats/hashes/sha2'
import * as dagCbor from '@ipld/dag-cbor'
import * as dagJson from '@ipld/dag-json'
import * as json from 'multiformats/codecs/json'
import * as raw from 'multiformats/codecs/raw'
import { fromString as uint8ArrayFromString } from 'uint8arrays/from-string'

/** Default 120s: relay pinning + Bob sync can exceed 45s under load or slow CI. */
export const waitFor = async (check, timeoutMs = 120000, intervalMs = 250) => {
  const start = Date.now()
  while (Date.now() - start <= timeoutMs) {
    try {
      const value = await check()
      if (value) return value
    } catch {
      // keep polling
    }
    await delay(intervalMs)
  }
  throw new Error(`Timed out after ${timeoutMs}ms`)
}

export const withTimeout = async (promise, timeoutMs) => {
  return await Promise.race([
    promise,
    delay(timeoutMs).then(() => {
      throw new Error(`Operation timed out after ${timeoutMs}ms`)
    }),
  ])
}

const toUint8Array = async (value) => {
  if (value instanceof Uint8Array) return value
  if (value instanceof ArrayBuffer) return new Uint8Array(value)
  if (value != null && typeof value === 'object') {
    if (value.bytes instanceof Uint8Array) return value.bytes
    if (typeof value.subarray === 'function') return value.subarray()
    if (typeof value[Symbol.asyncIterator] === 'function') {
      const chunks = []
      for await (const chunk of value) {
        chunks.push(await toUint8Array(chunk))
      }
      const total = chunks.reduce((sum, chunk) => sum + chunk.length, 0)
      const out = new Uint8Array(total)
      let offset = 0
      for (const chunk of chunks) {
        out.set(chunk, offset)
        offset += chunk.length
      }
      return out
    }
    if (typeof value[Symbol.iterator] === 'function') return Uint8Array.from(value)
  }
  throw new TypeError(`Unexpected byte value type: ${Object.prototype.toString.call(value)}`)
}

export const fetchJson = async (url) => {
  const res = await fetch(url)
  const text = await res.text()
  let data
  try {
    data = JSON.parse(text)
  } catch {
    throw new Error(`Not JSON (${res.status}): ${text}`)
  }
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}: ${text}`)
  }
  return data
}

const createHeliaWithLibp2p = (libp2p) => {
  return withBitswap(
    withLibp2p(
      withHTTP(
        createHeliaLight({
          codecs: [dagCbor, dagJson, json],
          hashers: [sha512],
        }),
      ),
      libp2p,
    ),
  )
}

export const createClient = async (orbitdbDirectory) => {
  const discoveryTopics = (process.env.PUBSUB_TOPICS || process.env.VITE_PUBSUB_TOPICS || 'todo._peer-discovery._p2p._pubsub')
    .split(',')
    .map((t) => t.trim())
    .filter(Boolean)

  const libp2p = await createLibp2p({
    addresses: {
      listen: ['/ip4/127.0.0.1/tcp/0', '/ip4/127.0.0.1/tcp/0/ws'],
    },
    transports: [tcp(), webSockets(), circuitRelayTransport()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    peerDiscovery: [
      pubsubPeerDiscovery({
        interval: 5000,
        topics: discoveryTopics,
        listenOnly: false,
        emitSelf: true,
      }),
    ],
    services: {
      identify: identify(),
      ping: ping(),
      pubsub: gossipsub({ allowPublishToZeroTopicPeers: true }),
      aminoDHT: kadDHT({
        protocol: '/ipfs/kad/1.0.0',
        peerInfoMapper: removePrivateAddressesMapper,
      }),
    },
  })

  const ipfs = await createHeliaWithLibp2p(libp2p).start()
  const orbitdb = await createOrbitDB({ ipfs, directory: orbitdbDirectory })
  return { libp2p, ipfs, orbitdb }
}

export const stopClient = async (client) => {
  try {
    await client.orbitdb.stop()
  } catch {}
  try {
    await client.ipfs.stop()
  } catch {}
  try {
    await client.libp2p.stop()
  } catch {}
}

async function runRelayMediaReplicationScenarioCore(opts) {
  const {
    tempRoot,
    relayAddr,
    dbBaseName,
    syncDatabase,
    getStats,
    getDatabases,
    getRelayPeerId,
  } = opts

  const safeName = dbBaseName.replace(/[^a-zA-Z0-9-_]/g, '-')

  let alice = await createClient(join(tempRoot, `alice-${safeName}`))
  await alice.libp2p.dial(relayAddr)

  const aliceDb = await alice.orbitdb.open(safeName.slice(0, 80), { type: 'events', sync: true })

  const imageBlocks = []
  for (let i = 1; i <= 1; i++) {
    const bytes = uint8ArrayFromString(`image-${i}-from-alice-${safeName}`)
    const hash = await sha256.digest(bytes)
    const imageCid = CID.createV1(raw.code, hash)

    await alice.ipfs.blockstore.put(imageCid, bytes)
    imageBlocks.push({ cid: imageCid, bytes })

    await aliceDb.add({
      postId: `post-${i}`,
      imageCid: imageCid.toString(),
      text: `Post ${i}`,
    })

    await delay(1200)
  }

  const aliceAll = await aliceDb.all()
  assert.equal(aliceAll.length, 1, 'Alice should have one local event before relay sync')

  const aliceDbAddress = aliceDb.address
  const expectedCidStr = imageBlocks[0].cid.toString()

  const syncJson = await waitFor(async () => {
    const parsed = await syncDatabase(aliceDbAddress)
    if (!parsed?.ok) {
      return null
    }
    if (!Array.isArray(parsed.extractedMediaCids) || !parsed.extractedMediaCids.includes(expectedCidStr)) {
      return null
    }
    return parsed
  }, 120000)

  assert.ok(
    syncJson.receivedUpdate === true || syncJson.fallbackScanUsed === true,
    'Relay sync should use a live update event or db.all() fallback so media extraction is grounded',
  )
  assert.ok(
    Array.isArray(syncJson.extractedMediaCids) && syncJson.extractedMediaCids.includes(expectedCidStr),
    `extractedMediaCids should include ${expectedCidStr}, got ${JSON.stringify(syncJson.extractedMediaCids)}`,
  )

  await waitFor(async () => {
    const stats = await getStats()
    const pinned = stats.pinnedMediaCids
    return Array.isArray(pinned) && pinned.includes(expectedCidStr) ? stats : null
  })

  const dbList = await getDatabases()
  assert.ok(
    dbList.databases?.some((d) => d.address === aliceDbAddress),
    `Relay should list the DB after sync: ${aliceDbAddress}`,
  )

  const relayPeerIdStr = await getRelayPeerId()
  assert.ok(typeof relayPeerIdStr === 'string' && relayPeerIdStr.length > 0, 'relay should expose peerId')

  await stopClient(alice)
  alice = null
  await delay(500)

  let bob = await createClient(join(tempRoot, `bob-${safeName}`))
  await bob.libp2p.dial(relayAddr)
  await delay(500)

  const bobHasRelay = bob.libp2p.getConnections().some((c) => c.remotePeer.toString() === relayPeerIdStr)
  assert.ok(bobHasRelay, 'Bob should have an active libp2p connection to the relay')

  const bobDb = await bob.orbitdb.open(aliceDbAddress, { sync: true })

  await waitFor(async () => {
    const all = await bobDb.all()
    return all.length >= 1 ? all : null
  })

  const records = await bobDb.all()
  assert.equal(records.length, 1)
  const seenCids = records.map((record) => record?.value?.imageCid).filter(Boolean)
  assert.equal(seenCids.length, 1)

  for (const { cid, bytes } of imageBlocks) {
    const fetched = await waitFor(async () => {
      const block = await withTimeout(bob.ipfs.blockstore.get(cid), 1500)
      return await toUint8Array(block)
    }, 120000)
    assert.deepEqual(Buffer.from(fetched), Buffer.from(bytes))
  }

  await stopClient(bob)
}

/**
 * Full flow: Alice writes → relay /pinning/sync + stats → Alice offline → Bob opens DB + reads block.
 * Cleans up Alice/Bob clients before returning.
 *
 * @param {{ tempRoot: string, relayAddr: import('multiformats/multiaddr').Multiaddr, metricsPort: number, dbBaseName: string }} opts
 */
export async function runRelayMediaReplicationScenario(opts) {
  const { tempRoot, relayAddr, metricsPort, dbBaseName } = opts
  await runRelayMediaReplicationScenarioCore({
    tempRoot,
    relayAddr,
    dbBaseName,
    syncDatabase: async (dbAddress) => {
      const syncRes = await fetch(`http://127.0.0.1:${metricsPort}/pinning/sync`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ dbAddress }),
      })
      const syncText = await syncRes.text()
      let parsed
      try {
        parsed = JSON.parse(syncText)
      } catch {
        return null
      }
      if (!syncRes.ok) {
        return null
      }
      return parsed
    },
    getStats: async () => await fetchJson(`http://127.0.0.1:${metricsPort}/pinning/stats`),
    getDatabases: async () => await fetchJson(`http://127.0.0.1:${metricsPort}/pinning/databases`),
    getRelayPeerId: async () => {
      const relayHealth = await fetchJson(`http://127.0.0.1:${metricsPort}/health`)
      return relayHealth.peerId
    },
  })
}

export async function runOrbitdbReplicationServiceScenario(opts) {
  const { tempRoot, relayAddr, relayService, relayPeerId, dbBaseName } = opts
  const pinning = relayService.createPinningHttpHandlers()

  await runRelayMediaReplicationScenarioCore({
    tempRoot,
    relayAddr,
    dbBaseName,
    syncDatabase: async (dbAddress) => await pinning.syncDatabase(dbAddress),
    getStats: async () => pinning.getStats(),
    getDatabases: async () => pinning.getDatabases(),
    getRelayPeerId: async () => relayPeerId,
  })
}
