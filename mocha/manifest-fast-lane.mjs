import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { setTimeout as delay } from 'node:timers/promises'

import { OrbitDBAccessController, parseAddress } from '@orbitdb/core'
import * as Block from 'multiformats/block'
import { CID } from 'multiformats/cid'
import * as dagCbor from '@ipld/dag-cbor'
import { sha256 } from 'multiformats/hashes/sha2'
import { base58btc } from 'multiformats/bases/base58'

import {
  closeRelayServiceNode,
  createClient,
  createRelayServiceNode,
  stopClient,
  withTimeout,
} from './lib/relay-replication-harness.mjs'

/**
 * #66, step 2. A browser opening somebody else's fresh list needs the list's
 * manifest blocks within OrbitDB's 30 s block timeout, and the relay is the
 * fallback when the two browsers cannot reach each other. The relay used to
 * fetch those blocks only inside a queued topic sync, behind two slots that
 * each take 5–50 s, so under load it held a new list ~52 s after it was
 * created. The fast lane fetches the manifest chain on first sight of a topic,
 * outside that queue.
 */

class FakePubsub extends EventTarget {
  // Synchronous and returning `undefined`, as gossipsub 16 does.
  subscribe() {}

  announce(topic) {
    this.dispatchEvent(
      new CustomEvent('subscription-change', {
        detail: {
          peerId: '12D3KooWFakeRemotePeer',
          subscriptions: [{ topic, subscribe: true }],
        },
      }),
    )
  }
}

/**
 * A blockstore in front of a pretend network: a block that exists "out there"
 * arrives after a round trip and is kept locally; one that does not never
 * arrives, until the caller's signal aborts the want, as bitswap's does.
 */
function networkedBlockstore(network) {
  const local = new Map()
  return {
    local,
    blockstore: {
      get(cid, options = {}) {
        const key = cid.toString()
        return (async function* () {
          if (!local.has(key)) {
            if (!network.has(key)) {
              await new Promise((_, reject) => {
                options.signal?.addEventListener(
                  'abort',
                  () => reject(options.signal.reason),
                  { once: true },
                )
              })
            }
            await delay(20)
            local.set(key, network.get(key))
          }
          yield local.get(key)
        })()
      },
    },
  }
}

async function putBlock(network, value) {
  const block = await Block.encode({ value, codec: dagCbor, hasher: sha256 })
  network.set(block.cid.toString(), block.bytes)
  return block.cid
}

/** A manifest chain as OrbitDB 4 writes it, returning the address and every CID. */
async function createChain(network, name, { orbitdbAccessController = false } = {}) {
  const cids = []
  const ipfsAc = await putBlock(network, { type: 'ipfs', write: ['*'] })
  cids.push(ipfsAc)
  let accessController = `/ipfs/${ipfsAc.toString(base58btc)}`
  if (orbitdbAccessController) {
    const acManifest = await putBlock(network, {
      name: `${name}-acl`,
      type: 'keyvalue',
      accessController,
    })
    cids.push(acManifest)
    accessController = `/orbitdb/${acManifest.toString(base58btc)}`
  }
  const manifest = await putBlock(network, { name, type: 'keyvalue', accessController })
  cids.push(manifest)
  return { address: `/orbitdb/${manifest.toString(base58btc)}`, cids }
}

async function waitFor(predicate, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await predicate()) return
    await delay(20)
  }
  assert.fail(`timed out after ${timeoutMs}ms waiting for ${label}`)
}

describe('manifest fast lane', function () {
  this.timeout(30000)

  let DatabaseService
  let setupOrbitdbReplicationHandlers
  let handlers
  let releaseSyncs = []
  let syncsReleased = false

  before(async () => {
    ;({ DatabaseService } = await import('../dist/services/database.js'))
    ;({ setupOrbitdbReplicationHandlers } = await import(
      '../dist/services/orbitdb-replication-service.js'
    ))
  })

  afterEach(async () => {
    syncsReleased = true
    for (const release of releaseSyncs) release()
    releaseSyncs = []
    await handlers?.cleanup()
    handlers = undefined
    syncsReleased = false
  })

  it('holds a new topic\'s manifest chain within 5 s while both sync slots hang', async () => {
    const network = new Map()
    const service = new DatabaseService()
    const store = networkedBlockstore(network)
    // Only the blockstore is exercised; a full initialize() would need Helia.
    service.ipfs = store
    const syncStarts = []
    // Syncs that never finish, as two slow databases do on a loaded relay.
    service.syncAllOrbitDBRecords = (topic) => {
      syncStarts.push(topic)
      if (syncsReleased) return Promise.resolve()
      return new Promise((resolve) => releaseSyncs.push(resolve))
    }

    const pubsub = new FakePubsub()
    handlers = setupOrbitdbReplicationHandlers({ services: { pubsub } }, service)

    const fillers = [
      await createChain(network, 'slow-one'),
      await createChain(network, 'slow-two'),
    ]
    for (const { address } of fillers) pubsub.announce(address)
    await waitFor(() => syncStarts.length === 2, 2000, 'both sync slots to be taken')

    const fresh = await createChain(network, 'fresh-list')
    const shared = await createChain(network, 'shared-list', {
      orbitdbAccessController: true,
    })
    const startedAt = Date.now()
    pubsub.announce(fresh.address)
    pubsub.announce(shared.address)

    const wanted = [...fresh.cids, ...shared.cids].map((cid) => cid.toString())
    await waitFor(
      () => wanted.every((cid) => store.local.has(cid)),
      5000,
      `the manifest chains to be held (have ${wanted.filter((cid) => store.local.has(cid)).length} of ${wanted.length} blocks)`,
    )
    const elapsedMs = Date.now() - startedAt

    // Fetched outside the queue: the slots are still taken, and neither new
    // topic's sync has started.
    const stats = handlers.getQueueStats()
    assert.equal(stats.active, 2)
    assert.ok(!syncStarts.includes(fresh.address))
    assert.ok(!syncStarts.includes(shared.address))
    assert.ok(elapsedMs < 5000, `held after ${elapsedMs}ms`)
  })

  it('gives up on a manifest nobody serves after its block timeout', async () => {
    const service = new DatabaseService()
    service.ipfs = networkedBlockstore(new Map())

    const missing = await createChain(new Map(), 'nowhere')
    const startedAt = Date.now()
    const result = await service.fetchManifestChain(missing.address, {
      blockTimeoutMs: 300,
    })
    const elapsedMs = Date.now() - startedAt

    assert.equal(result.held, false)
    assert.equal(result.failure?.step, 'manifest')
    assert.ok(elapsedMs < 2000, `gave up after ${elapsedMs}ms`)
  })
})

/** The CIDs `orbitdb.open(address)` needs, read from the creator's own blocks. */
async function manifestChainCids(ipfs, address) {
  const read = async (hash) => {
    const cid = CID.parse(hash, base58btc)
    const chunks = []
    for await (const chunk of ipfs.blockstore.get(cid, { offline: true })) chunks.push(chunk)
    const bytes = Buffer.concat(chunks)
    const { value } = await Block.decode({ bytes, codec: dagCbor, hasher: sha256 })
    return { cid, value }
  }
  const cids = []
  const manifest = await read(parseAddress(address).hash)
  cids.push(manifest.cid)
  let accessController = manifest.value.accessController
  if (accessController.startsWith('/orbitdb/')) {
    const acManifest = await read(parseAddress(accessController).hash)
    cids.push(acManifest.cid)
    accessController = acManifest.value.accessController
  }
  const ipfsAc = await read(accessController.replace('/ipfs/', ''))
  cids.push(ipfsAc.cid)
  return cids
}

describe('manifest fast lane against a real OrbitDB client', function () {
  this.timeout(120000)

  let tempRoot
  const localOrbitDbDir = join(process.cwd(), 'orbitdb')

  before(async () => {
    await rm(localOrbitDbDir, { recursive: true, force: true })
    tempRoot = await mkdtemp(join(tmpdir(), 'manifest-fast-lane-'))
  })

  after(async () => {
    await rm(localOrbitDbDir, { recursive: true, force: true })
    if (tempRoot) await rm(tempRoot, { recursive: true, force: true })
  })

  /**
   * Alice creates two databases while both of the relay's sync slots are held
   * by `takeSyncSlots`, then leaves; Bob, who can reach only the relay, opens
   * both by address.
   */
  async function openFreshDatabasesThroughRelay(label, takeSyncSlots) {
    const wsPort = 26492 + Math.floor(Math.random() * 1000)
    const relay = await createRelayServiceNode({
      storageDir: join(tempRoot, label, 'relay'),
      wsPort,
      tcpPort: wsPort - 1,
    })
    // No full sync ever finishes, so whatever the relay holds came from the
    // fast lane alone.
    const releaseSyncs = []
    const syncStarts = []
    let syncsReleased = false
    const databaseService = relay.libp2p.services.orbitdbReplication.databaseService
    databaseService.syncAllOrbitDBRecords = (topic) => {
      syncStarts.push(topic)
      return syncsReleased
        ? Promise.resolve()
        : new Promise((resolve) => releaseSyncs.push(resolve))
    }

    let alice
    let bob
    try {
      alice = await createClient(join(tempRoot, label, 'alice'))
      await alice.libp2p.dial(relay.relayAddr)

      await takeSyncSlots(alice)
      await waitFor(() => syncStarts.length === 2, 10000, 'both sync slots to be taken')

      const createdAt = Date.now()
      const freshDb = await alice.orbitdb.open('fresh-list', { type: 'keyvalue' })
      const sharedDb = await alice.orbitdb.open('shared-list', {
        type: 'keyvalue',
        AccessController: OrbitDBAccessController({ write: [alice.orbitdb.identity.id] }),
      })
      const databases = [
        { address: freshDb.address, name: 'fresh-list', acType: 'ipfs' },
        { address: sharedDb.address, name: 'shared-list', acType: 'orbitdb' },
      ]
      const wanted = []
      for (const { address } of databases) {
        wanted.push(...(await manifestChainCids(alice.ipfs, address)))
      }

      const missing = async () => {
        const absent = []
        for (const cid of wanted) {
          if (!(await relay.storage.blockstore.has(cid))) absent.push(cid.toString())
        }
        return absent
      }
      await waitFor(
        async () => (await missing()).length === 0,
        5000,
        'the relay to hold both manifest chains',
      ).catch(async (error) => {
        throw new Error(`${error.message}; missing ${JSON.stringify(await missing())}`)
      })
      const heldAfterMs = Date.now() - createdAt

      // The creator leaves; the relay is all Bob can reach.
      await stopClient(alice)
      alice = undefined

      bob = await createClient(join(tempRoot, label, 'bob'))
      await bob.libp2p.dial(relay.relayAddr)
      for (const { address, name, acType } of databases) {
        const startedAt = Date.now()
        // Under OrbitDB's own 30 s block timeout, so a miss fails here.
        const db = await withTimeout(bob.orbitdb.open(address), 20000)
        assert.equal(db.name, name)
        assert.equal(db.access.type, acType)
        assert.ok(
          Date.now() - startedAt < 10000,
          `opening ${name} took ${Date.now() - startedAt}ms`,
        )
      }
      assert.ok(heldAfterMs < 5000, `relay held the chains after ${heldAfterMs}ms`)
    } finally {
      // Peers leaving announce their topics again; those syncs must not hang
      // the relay's shutdown, which waits for the queue to drain.
      syncsReleased = true
      for (const release of releaseSyncs) release()
      if (bob) await stopClient(bob)
      if (alice) await stopClient(alice)
      await closeRelayServiceNode(relay)
    }
  }

  it('lets a peer open a fresh database the relay knows only from the fast lane', async () => {
    // Two other databases take both sync slots first, as slow databases do
    // on a loaded relay. Alice serves them.
    await openFreshDatabasesThroughRelay('served-slot-holders', async (alice) => {
      for (const name of ['slow-one', 'slow-two']) {
        await alice.orbitdb.open(name, { type: 'keyvalue' })
      }
    })
  })

  it('serves that chain while the relay also wants manifests nobody serves', async () => {
    // Two topics whose manifests nobody serves take both sync slots, so the
    // relay wants those manifests from every peer, Bob included. Bob runs a
    // stock `withBitswap` client, whose ledger fetches a wanted block through
    // Helia's networked blockstore: he wants them back from the relay, in the
    // same message as the access-controller block he needs. A relay ledger
    // that read through the network as well waited on a manifest it will
    // never get, and that block stayed unsent with it (#66).
    await openFreshDatabasesThroughRelay('unserved-slot-holders', async (alice) => {
      for (const name of ['nobody-serves-one', 'nobody-serves-two']) {
        const block = await Block.encode({ value: { name }, codec: dagCbor, hasher: sha256 })
        alice.libp2p.services.pubsub.subscribe(`/orbitdb/${block.cid.toString(base58btc)}`)
      }
    })
  })
})
