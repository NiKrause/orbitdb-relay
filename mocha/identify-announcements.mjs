/**
 * The relay answers `/orbitdb/heads/<address>` but does not announce it.
 *
 * Announced, the per-database protocols grew identify past the clients'
 * 8 KiB limit after about 100 databases (#50), sent the whole list to every
 * connected peer each time a database opened or closed, and told every peer
 * which databases the relay holds (#51). OrbitDB dials the protocol directly
 * and never reads identify, so the relay only has to keep accepting it.
 */
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { setTimeout as delay } from 'node:timers/promises'

import { createLibp2p } from 'libp2p'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { identify, identifyPush } from '@libp2p/identify'
import { gossipsub } from '@libp2p/gossipsub'
import { ping } from '@libp2p/ping'
import { circuitRelayServer, circuitRelayTransport } from '@libp2p/circuit-relay-v2'
import { tcp } from '@libp2p/tcp'
import { webSockets } from '@libp2p/websockets'
import { multiaddr } from '@multiformats/multiaddr'

import { createClient, stopClient, waitFor } from './lib/relay-replication-harness.mjs'

const HOP = '/libp2p/circuit/relay/0.2.0/hop'
const PUSH = '/ipfs/id/push/1.0.0'
const isHeads = (protocol) => protocol.startsWith('/orbitdb/heads/')

/** The relay as `src/config/libp2p.ts` sets up identify, trimmed to what the test needs. */
async function createRelay(storageDir) {
  const [{ initializeStorage }, { orbitdbReplicationService, withoutOrbitdbHeadsAnnouncements }] =
    await Promise.all([import('../dist/services/storage.js'), import('../dist/index.js')])
  const storage = await initializeStorage(storageDir)
  const wsPort = 27492 + Math.floor(Math.random() * 1000)
  const libp2p = await createLibp2p({
    privateKey: storage.privateKey,
    datastore: storage.datastore,
    addresses: { listen: [`/ip4/127.0.0.1/tcp/${wsPort - 1}`, `/ip4/127.0.0.1/tcp/${wsPort}/ws`] },
    transports: [circuitRelayTransport(), tcp(), webSockets()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    services: {
      identify: withoutOrbitdbHeadsAnnouncements(identify()),
      identifyPush: withoutOrbitdbHeadsAnnouncements(identifyPush()),
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
  return { storage, libp2p, address: multiaddr(`/ip4/127.0.0.1/tcp/${wsPort}/ws`) }
}

/** A peer that records every identify push it receives, as raw protobuf text. */
async function createPushObserver() {
  const pushes = []
  const libp2p = await createLibp2p({
    transports: [webSockets(), tcp()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    services: { identify: identify({ maxMessageSize: 1024 * 1024 }) },
  })
  await libp2p.handle(
    PUSH,
    async (stream) => {
      let text = ''
      for await (const chunk of stream) text += Buffer.from(chunk.subarray()).toString('latin1')
      await stream.close().catch(() => {})
      pushes.push(text)
    },
    { runOnLimitedConnection: true },
  )
  return { libp2p, pushes }
}

/** A heads exchange that sends nothing: whether the relay accepts the protocol at all. */
async function exchangeNothing(client, relayPeerId, dbAddress) {
  const stream = await client.dialProtocol(relayPeerId, `/orbitdb/heads${dbAddress}`)
  const reading = (async () => {
    for await (const _ of stream) {
      // heads the relay holds; not what this test checks
    }
  })()
  await stream.close()
  await reading
}

describe('the relay answers per-database heads protocols without announcing them', function () {
  this.timeout(180000)

  let tempRoot
  let relay
  let dbService
  let writer
  let observer
  const open = []
  let closedAddress
  const localOrbitDbDir = join(process.cwd(), 'orbitdb')

  before(async () => {
    await rm(localOrbitDbDir, { recursive: true, force: true })
    tempRoot = await mkdtemp(join(tmpdir(), 'identify-announcements-'))
    relay = await createRelay(join(tempRoot, 'relay'))
    dbService = relay.libp2p.services.orbitdbReplication.databaseService

    // Connected before any database exists, to see what opening them pushes.
    observer = await createPushObserver()
    await observer.libp2p.dial(relay.address)
    await waitFor(async () => (await relay.libp2p.peerStore.get(observer.libp2p.peerId)).protocols.includes(PUSH), 30000)
    await delay(2500) // identify-push is debounced by 1 s
    observer.pushes.length = 0

    writer = await createClient(join(tempRoot, 'writer'))
    await writer.libp2p.dial(relay.address)
    for (let i = 0; i < 3; i++) {
      const db = await writer.orbitdb.open(`announced-${i}-${Date.now()}`, { type: 'events' })
      await db.add({ i })
      open.push(db.address)
    }
    // An empty database: the relay finds nothing, closes it, and remembers it.
    closedAddress = (await writer.orbitdb.open(`empty-${Date.now()}`, { type: 'events' })).address

    // Pubsub alone does not always get every database onto the relay; that is
    // not what this test is about, so ask for each one as the app does.
    const pinning = relay.libp2p.services.orbitdbReplication.createPinningHttpHandlers()
    for (const address of open) {
      await waitFor(async () => {
        if (!dbService.pinnedOpenDatabases.has(address)) await pinning.syncDatabase(address)
        return dbService.pinnedOpenDatabases.has(address)
      }, 120000)
    }
    await waitFor(
      () =>
        dbService.knownDatabasesByAddress.has(closedAddress) &&
        !dbService.databaseUseCounts.has(closedAddress) &&
        dbService.syncInFlight.size === 0,
      120000,
    )
    await delay(2500)
  })

  after(async () => {
    if (writer) await stopClient(writer)
    try {
      await observer?.libp2p.stop()
    } catch {}
    try {
      await relay?.libp2p.stop()
    } catch {}
    try {
      await relay?.storage.datastore.close()
    } catch {}
    try {
      await relay?.storage.blockstore.close()
    } catch {}
    await rm(localOrbitDbDir, { recursive: true, force: true })
    if (tempRoot) await rm(tempRoot, { recursive: true, force: true })
  })

  it('keeps them out of identify, and still accepts them', async () => {
    const answered = relay.libp2p.getProtocols()
    for (const address of [...open, closedAddress]) {
      assert.ok(answered.includes(`/orbitdb/heads${address}`), `the relay should answer ${address}`)
    }

    const client = await createLibp2p({
      transports: [webSockets(), tcp()],
      connectionEncrypters: [noise()],
      streamMuxers: [yamux()],
      services: { identify: identify() }, // the default 8 KiB limit, as browsers have it
    })
    try {
      await client.dial(relay.address)
      const seen = await waitFor(async () => {
        const protocols = (await client.peerStore.get(relay.libp2p.peerId)).protocols
        return protocols.includes(HOP) ? protocols : null
      }, 30000)
      assert.deepEqual(seen.filter(isHeads), [], 'identify announced per-database protocols')
      assert.ok(seen.includes(PUSH))

      // Held open: OrbitDB's own handler. Closed: the on-demand handler.
      await exchangeNothing(client, relay.libp2p.peerId, open[0])
      await exchangeNothing(client, relay.libp2p.peerId, closedAddress)
    } finally {
      await client.stop()
    }
  })

  it('pushes nothing when databases open and close, and still pushes what it announces', async () => {
    assert.deepEqual(
      observer.pushes,
      [],
      'opening and closing databases pushed identify to every connected peer',
    )

    await relay.libp2p.handle('/probe/announced/1.0.0', async (stream) => stream.close())
    const push = await waitFor(() => observer.pushes.find((text) => text.includes('/probe/announced/1.0.0')), 10000)
    assert.equal(push.includes('/orbitdb/heads/'), false, 'a push carried per-database protocols')
  })
})
