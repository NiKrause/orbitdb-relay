import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'

import { createLibp2p } from 'libp2p'
import { gossipsub } from '@libp2p/gossipsub'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { identify } from '@libp2p/identify'
import { ping } from '@libp2p/ping'
import { circuitRelayTransport, circuitRelayServer } from '@libp2p/circuit-relay-v2'
import { kadDHT, removePrivateAddressesMapper } from '@libp2p/kad-dht'
import { pubsubPeerDiscovery } from '@libp2p/pubsub-peer-discovery'
import { tcp } from '@libp2p/tcp'
import { webSockets } from '@libp2p/websockets'
import { multiaddr } from '@multiformats/multiaddr'

import { runOrbitdbReplicationServiceScenario } from './lib/relay-replication-harness.mjs'

async function createPluginRelayNode({ storageDir, discoveryTopic, wsPort, tcpPort }) {
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
    transports: [circuitRelayTransport(), tcp(), webSockets()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    peerDiscovery: [
      pubsubPeerDiscovery({
        interval: 5000,
        topics: [discoveryTopic],
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
    relayAddr: multiaddr(`/ip4/127.0.0.1/tcp/${wsPort}/ws`),
  }
}

async function closePluginRelayNode(node) {
  try {
    await node.libp2p.stop()
  } catch {}
  try {
    await node.storage.datastore.close()
  } catch {}
  try {
    await node.storage.blockstore.close()
  } catch {}
}

describe('orbitdb replication libp2p service', function () {
  this.timeout(180000)

  let tempRoot
  const localOrbitDbDir = join(process.cwd(), 'orbitdb')

  before(async () => {
    await rm(localOrbitDbDir, { recursive: true, force: true })
    tempRoot = await mkdtemp(join(tmpdir(), 'orbitdb-replication-service-'))
  })

  after(async () => {
    await rm(localOrbitDbDir, { recursive: true, force: true })
    if (tempRoot) {
      await rm(tempRoot, { recursive: true, force: true })
    }
  })

  it('replicates and pins media when mounted as a plain libp2p service', async () => {
    const wsPort = 24492 + Math.floor(Math.random() * 1000)
    const tcpPort = wsPort - 1
    const discoveryTopic = 'orbitdb.replication.service.test._peer-discovery._p2p._pubsub'
    const originalTopic = process.env.PUBSUB_TOPICS
    process.env.PUBSUB_TOPICS = discoveryTopic

    const node = await createPluginRelayNode({
      storageDir: join(tempRoot, 'plain-service'),
      discoveryTopic,
      wsPort,
      tcpPort,
    })

    try {
      await runOrbitdbReplicationServiceScenario({
        tempRoot,
        relayAddr: node.relayAddr,
        relayService: node.libp2p.services.orbitdbReplication,
        relayPeerId: node.libp2p.peerId.toString(),
        dbBaseName: 'plugin-service',
      })
    } finally {
      if (typeof originalTopic === 'undefined') delete process.env.PUBSUB_TOPICS
      else process.env.PUBSUB_TOPICS = originalTopic
      await closePluginRelayNode(node)
    }
  })

  it('survives stop/start on the same libp2p node and still replicates', async () => {
    const wsPort = 25492 + Math.floor(Math.random() * 1000)
    const tcpPort = wsPort - 1
    const discoveryTopic = 'orbitdb.replication.service.restart._peer-discovery._p2p._pubsub'
    const originalTopic = process.env.PUBSUB_TOPICS
    process.env.PUBSUB_TOPICS = discoveryTopic

    const node = await createPluginRelayNode({
      storageDir: join(tempRoot, 'restart-service'),
      discoveryTopic,
      wsPort,
      tcpPort,
    })

    try {
      await node.libp2p.stop()
      await node.libp2p.start()

      await runOrbitdbReplicationServiceScenario({
        tempRoot,
        relayAddr: node.relayAddr,
        relayService: node.libp2p.services.orbitdbReplication,
        relayPeerId: node.libp2p.peerId.toString(),
        dbBaseName: 'plugin-restart',
      })
    } finally {
      if (typeof originalTopic === 'undefined') delete process.env.PUBSUB_TOPICS
      else process.env.PUBSUB_TOPICS = originalTopic
      await closePluginRelayNode(node)
    }
  })
})
