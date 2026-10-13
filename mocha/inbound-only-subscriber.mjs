/**
 * A subscriber the relay cannot dial back — a browser, which only ever
 * connects inbound and whose own addresses are circuits through this very
 * relay — must survive a fresh `/pinning/sync`, and must be able to pull heads
 * for a database the relay knows but has closed (NiKrause/orbitdb-relay#59).
 *
 * The client here is a Node libp2p node that listens only on `/p2p-circuit`,
 * like a browser does: its only address is a reservation on the relay.
 */
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
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
import { circuitRelayServer, circuitRelayTransport } from '@libp2p/circuit-relay-v2'
import { tcp } from '@libp2p/tcp'
import { webSockets } from '@libp2p/websockets'
import { multiaddr } from '@multiformats/multiaddr'
import { sha512 } from 'multiformats/hashes/sha2'
import * as dagCbor from '@ipld/dag-cbor'
import * as dagJson from '@ipld/dag-json'
import * as json from 'multiformats/codecs/json'

import { waitFor } from './lib/relay-replication-harness.mjs'

const DISCOVERY_TOPIC = 'orbitdb.relay.inbound-only.test._peer-discovery._p2p._pubsub'

const heliaFor = (libp2p) =>
  withBitswap(
    withLibp2p(
      withHTTP(createHeliaLight({ codecs: [dagCbor, dagJson, json], hashers: [sha512] })),
      libp2p,
    ),
  )

async function createRelayNode({ storageDir, wsPort, tcpPort }) {
  const [{ initializeStorage }, { orbitdbReplicationService }] = await Promise.all([
    import('../dist/services/storage.js'),
    import('../dist/index.js'),
  ])
  const storage = await initializeStorage(storageDir)
  const libp2p = await createLibp2p({
    privateKey: storage.privateKey,
    datastore: storage.datastore,
    addresses: {
      listen: [`/ip4/127.0.0.1/tcp/${tcpPort}`, `/ip4/127.0.0.1/tcp/${wsPort}/ws`],
    },
    transports: [tcp(), webSockets()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    services: {
      identify: identify(),
      ping: ping(),
      pubsub: gossipsub({ allowPublishToZeroTopicPeers: true }),
      relay: circuitRelayServer(),
      orbitdbReplication: orbitdbReplicationService({
        datastore: storage.datastore,
        blockstore: storage.blockstore,
        orbitdbDirectory: join(storageDir, 'orbitdb'),
      }),
    },
  })
  return {
    storage,
    libp2p,
    service: libp2p.services.orbitdbReplication,
    address: multiaddr(`/ip4/127.0.0.1/tcp/${wsPort}/ws/p2p/${libp2p.peerId.toString()}`),
  }
}

/** A client that can only be reached through the relay, as a browser is. */
async function createInboundOnlyClient({ relay, directory }) {
  const libp2p = await createLibp2p({
    addresses: { listen: ['/p2p-circuit'] },
    transports: [webSockets(), circuitRelayTransport()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    services: {
      identify: identify(),
      ping: ping(),
      pubsub: gossipsub({ allowPublishToZeroTopicPeers: true }),
    },
  })
  await libp2p.dial(relay.address)
  const ipfs = await heliaFor(libp2p).start()
  const orbitdb = await createOrbitDB({ ipfs, directory })
  return { libp2p, ipfs, orbitdb }
}

async function stopClient(client) {
  for (const stop of [
    () => client.orbitdb.stop(),
    () => client.ipfs.stop(),
    () => client.libp2p.stop(),
  ]) {
    try {
      await stop()
    } catch {}
  }
}

/** The relay has nothing queued or running: what the app's sync meets in practice. */
const relayIsIdle = (pinning, dbService) => {
  const { syncQueue } = pinning.getStats()
  return syncQueue?.active === 0 && syncQueue?.waiting === 0 && dbService.syncInFlight.size === 0
}

describe('an inbound-only OrbitDB subscriber', function () {
  this.timeout(180000)

  let tempRoot
  let relay
  let dbService
  let pinning
  const originalTopics = process.env.PUBSUB_TOPICS

  before(async () => {
    process.env.PUBSUB_TOPICS = DISCOVERY_TOPIC
    tempRoot = await mkdtemp(join(tmpdir(), 'relay-inbound-only-'))
    const wsPort = 27492 + Math.floor(Math.random() * 1000)
    relay = await createRelayNode({ storageDir: join(tempRoot, 'relay'), wsPort, tcpPort: wsPort - 1 })
    // White-box, as mocha/database-peer-directory.mjs is: the subscriber
    // directory and the open-database bookkeeping are what the fixes change.
    dbService = relay.service.databaseService
    pinning = relay.service.createPinningHttpHandlers()
  })

  after(async () => {
    if (typeof originalTopics === 'undefined') delete process.env.PUBSUB_TOPICS
    else process.env.PUBSUB_TOPICS = originalTopics
    try {
      await relay?.libp2p.stop()
    } catch {}
    try {
      await relay?.storage.datastore.close()
    } catch {}
    try {
      await relay?.storage.blockstore.close()
    } catch {}
    if (tempRoot) await rm(tempRoot, { recursive: true, force: true })
  })

  it('keeps its connection through a fresh /pinning/sync', async () => {
    const client = await createInboundOnlyClient({ relay, directory: join(tempRoot, 'writer') })
    const clientId = client.libp2p.peerId.toString()
    const relayId = relay.libp2p.peerId.toString()
    try {
      const db = await client.orbitdb.open(`inbound-only-${Date.now()}`)
      await db.add('one entry, so the relay has state to keep')
      // Subscribing to the database topic is what puts a peer into the directory.
      await waitFor(() => dbService.knownDatabasePeers.get(db.address)?.has(clientId) === true, 60000)
      // The app's sync arrives long after the relay's own pass for the topic
      // has finished. One that overlaps an in-flight pass is coalesced and
      // never re-dials the directory, so wait for the relay to go idle.
      await waitFor(() => relayIsIdle(pinning, dbService), 120000)

      const disconnectedFrom = []
      client.libp2p.addEventListener('peer:disconnect', (event) => {
        disconnectedFrom.push(event.detail.toString())
      })

      // What the app sends after every write; `requireFresh` re-dials the directory.
      await pinning.syncDatabase(db.address)
      await delay(500)

      assert.deepEqual(
        disconnectedFrom.filter((id) => id === relayId),
        [],
        'the relay hung up on a subscriber it cannot dial back',
      )
      assert.ok(
        client.libp2p.getConnections(relay.libp2p.peerId).some((c) => c.status === 'open'),
        'the connection to the relay is gone',
      )
      assert.equal(
        dbService.knownDatabasePeers.get(db.address)?.has(clientId),
        true,
        'a subscriber that is still connected was forgotten',
      )
    } finally {
      await stopClient(client)
    }
  })

  it('pulls heads for a database the relay knows but has closed, without a reset', async () => {
    const client = await createInboundOnlyClient({ relay, directory: join(tempRoot, 'puller') })
    try {
      // An empty database: the relay's pass finds no state and closes it again,
      // but remembers the address — the on-demand handler is now the only thing
      // answering its heads protocol. That is the list registry in the issue.
      const db = await client.orbitdb.open(`on-demand-${Date.now()}`)
      await waitFor(() => dbService.knownDatabasesByAddress.has(db.address), 60000)
      await waitFor(
        () =>
          relayIsIdle(pinning, dbService) &&
          !dbService.databaseUseCounts.has(db.address) &&
          !dbService.pinnedOpenDatabases.has(db.address),
        60000,
      )

      await db.add('a head the relay has not seen')
      const heads = await db.log.heads()
      assert.equal(heads.length, 1)

      // The exchange as OrbitDB's Sync runs it: send our heads, read theirs,
      // close. A reset instead of a clean close is what the browsers saw.
      const stream = await client.libp2p.dialProtocol(relay.libp2p.peerId, `/orbitdb/heads${db.address}`)
      const received = []
      const failures = []
      const reading = (async () => {
        try {
          for await (const chunk of stream) received.push(chunk.subarray())
        } catch (error) {
          failures.push(error)
        }
      })()
      for (const { hash } of heads) stream.send(await db.log.storage.get(hash))
      await stream.close()
      await reading

      assert.deepEqual(
        failures.map((error) => `${error.name}: ${error.message}`),
        [],
        'the relay reset the on-demand heads stream',
      )
      assert.ok(
        client.libp2p.getConnections(relay.libp2p.peerId).some((c) => c.status === 'open'),
        'the connection to the relay is gone',
      )
      assert.ok(Array.isArray(received))
    } finally {
      await stopClient(client)
    }
  })
})
