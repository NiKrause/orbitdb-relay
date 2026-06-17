import assert from 'node:assert/strict'
import { execFile as execFileCb } from 'node:child_process'
import { mkdtemp, mkdir, rm, symlink, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { promisify } from 'node:util'

const execFile = promisify(execFileCb)

async function linkConsumerDependencyTree(consumerDir) {
  const repoRoot = process.cwd()
  const consumerNodeModules = join(consumerDir, 'node_modules')

  await mkdir(consumerNodeModules, { recursive: true })

  const links = [
    ['orbitdb-relay', join(repoRoot)],
    ['libp2p', join(repoRoot, 'node_modules', 'libp2p')],
    ['datastore-level', join(repoRoot, 'node_modules', 'datastore-level')],
    ['blockstore-level', join(repoRoot, 'node_modules', 'blockstore-level')],
    ['@chainsafe', join(repoRoot, 'node_modules', '@chainsafe')],
    ['@libp2p', join(repoRoot, 'node_modules', '@libp2p')],
  ]

  for (const [name, src] of links) {
    await symlink(src, join(consumerNodeModules, name), 'dir')
  }
}

describe('package consumer libp2p service integration', function () {
  this.timeout(180000)

  let tempRoot

  before(async () => {
    tempRoot = await mkdtemp(join(tmpdir(), 'relay-consumer-'))
  })

  after(async () => {
    if (tempRoot) {
      await rm(tempRoot, { recursive: true, force: true })
    }
  })

  it('can be imported from another project package context and mounted in libp2p', async () => {
    const consumerDir = await mkdtemp(join(tempRoot, 'app-'))

    await writeFile(
      join(consumerDir, 'package.json'),
      JSON.stringify(
        {
          name: 'relay-consumer-smoke',
          private: true,
          type: 'module',
        },
        null,
        2,
      ),
    )

    await linkConsumerDependencyTree(consumerDir)

    await writeFile(
      join(consumerDir, 'consumer-app.mjs'),
      `
import { mkdir } from 'node:fs/promises'
import { join } from 'node:path'

import { createLibp2p } from 'libp2p'
import { LevelDatastore } from 'datastore-level'
import { LevelBlockstore } from 'blockstore-level'
import { gossipsub } from '@chainsafe/libp2p-gossipsub'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { identify } from '@libp2p/identify'
import { ping } from '@libp2p/ping'
import { circuitRelayTransport, circuitRelayServer } from '@libp2p/circuit-relay-v2'
import { kadDHT, removePrivateAddressesMapper } from '@libp2p/kad-dht'
import { pubsubPeerDiscovery } from '@libp2p/pubsub-peer-discovery'
import { tcp } from '@libp2p/tcp'
import { webSockets } from '@libp2p/websockets'

import { orbitdbReplicationService } from 'orbitdb-relay'

const storageDir = join(process.cwd(), 'consumer-runtime')
await mkdir(storageDir, { recursive: true })

const datastore = new LevelDatastore(join(storageDir, 'ipfs', 'data'))
const blockstore = new LevelBlockstore(join(storageDir, 'ipfs', 'blocks'))

await datastore.open()
await blockstore.open()

const libp2p = await createLibp2p({
  datastore,
  addresses: {
    listen: ['/ip4/127.0.0.1/tcp/0', '/ip4/127.0.0.1/tcp/0/ws']
  },
  transports: [circuitRelayTransport(), tcp(), webSockets()],
  connectionEncrypters: [noise()],
  streamMuxers: [yamux()],
  peerDiscovery: [
    pubsubPeerDiscovery({
      interval: 5000,
      topics: ['consumer.package.test._peer-discovery._p2p._pubsub'],
      listenOnly: false,
      emitSelf: true
    })
  ],
  services: {
    identify: identify(),
    ping: ping(),
    pubsub: gossipsub({ allowPublishToZeroTopicPeers: true }),
    aminoDHT: kadDHT({
      protocol: '/ipfs/kad/1.0.0',
      peerInfoMapper: removePrivateAddressesMapper
    }),
    relay: circuitRelayServer(),
    orbitdbReplication: orbitdbReplicationService({
      datastore,
      blockstore,
      orbitdbDirectory: join(storageDir, 'orbitdb')
    })
  }
})

const service = libp2p.services.orbitdbReplication
if (typeof service?.syncAllOrbitDBRecords !== 'function') {
  throw new Error('syncAllOrbitDBRecords export missing from consumer project import')
}
if (typeof service?.createPinningHttpHandlers !== 'function') {
  throw new Error('createPinningHttpHandlers export missing from consumer project import')
}

await libp2p.stop()
await datastore.close()
await blockstore.close()

console.log('consumer-package-import-ok')
`,
    )

    const { stdout } = await execFile(process.execPath, ['consumer-app.mjs'], {
      cwd: consumerDir,
      env: {
        ...process.env,
        PUBSUB_TOPICS: 'consumer.package.test._peer-discovery._p2p._pubsub',
      },
    })

    assert.match(stdout, /consumer-package-import-ok/)
  })
})
