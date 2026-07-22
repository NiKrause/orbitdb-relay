// Minimal reproduction of an @orbitdb/core Sync deadlock, intended for an
// upstream issue. Uses only @orbitdb/core + helia + libp2p — none of this
// repository's relay code.
//
// The defect (sync.js, @orbitdb/core 4.0.0):
//   1. `startSync()` discovers peers ONLY via `subscription-change` events —
//      peers already subscribed when the database opens are never exchanged
//      with.
//   2. `peers` is a one-shot cache: each peer gets exactly one heads
//      exchange until disconnect; there is no re-exchange when local heads
//      change later.
//   3. `sync.add()` publishes only locally-appended entries; heads received
//      via the exchange are never re-announced on the topic.
//
// Together these deadlock a common topology: two edge peers (browsers behind
// NAT) connected only through a pinning relay. This test reproduces the
// exact sequence observed live on 2026-07-22 with gossipsub debug logging.
import assert from 'node:assert/strict'
import { rm, mkdtemp } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { createLibp2p } from 'libp2p'
import { tcp } from '@libp2p/tcp'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { identify } from '@libp2p/identify'
import { gossipsub } from '@libp2p/gossipsub'
import { createHeliaLight } from 'helia'
import { withBitswap } from '@helia/bitswap'
import { withHTTP } from '@helia/http'
import { withLibp2p } from '@helia/libp2p'
import { createOrbitDB } from '@orbitdb/core'
import * as dagCbor from '@ipld/dag-cbor'
import * as dagJson from '@ipld/dag-json'
import * as json from 'multiformats/codecs/json'
import { sha512 } from 'multiformats/hashes/sha2'

const waitFor = async (check, timeoutMs = 15000, intervalMs = 200) => {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    const value = await check()
    if (value) return value
    if (Date.now() > deadline) throw new Error(`waitFor timed out after ${timeoutMs}ms`)
    await new Promise((resolve) => setTimeout(resolve, intervalMs))
  }
}

const createNode = async (dir) => {
  const libp2p = await createLibp2p({
    addresses: { listen: ['/ip4/127.0.0.1/tcp/0'] },
    transports: [tcp()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    services: {
      identify: identify(),
      pubsub: gossipsub({ emitSelf: false, allowPublishToZeroTopicPeers: true })
    }
  })
  const ipfs = await withBitswap(
    withLibp2p(
      withHTTP(createHeliaLight({ codecs: [dagCbor, dagJson, json], hashers: [sha512] })),
      libp2p
    )
  ).start()
  const orbitdb = await createOrbitDB({ ipfs, directory: join(dir, 'orbitdb') })
  return { libp2p, ipfs, orbitdb }
}

const stopNode = async (node) => {
  try { await node.orbitdb.stop() } catch {}
  try { await node.ipfs.stop() } catch {}
  try { await node.libp2p.stop() } catch {}
}

describe('upstream @orbitdb/core: Sync misses pre-existing subscribers', function () {
  this.timeout(120000)

  let tempRoot
  let writerA, readerB, relayR

  before(async () => {
    tempRoot = await mkdtemp(join(tmpdir(), 'orbitdb-sync-repro-'))
    writerA = await createNode(join(tempRoot, 'a'))
    readerB = await createNode(join(tempRoot, 'b'))
    relayR = await createNode(join(tempRoot, 'r'))
  })

  after(async () => {
    await Promise.allSettled([writerA, readerB, relayR].map(stopNode))
    if (tempRoot) await rm(tempRoot, { recursive: true, force: true })
  })

  it('reader connected only via a relay never receives the first entry', async () => {
    const relayAddr = relayR.libp2p.getMultiaddrs()

    // The live failure was a timing race — the writer published its first
    // entry in the ~200 ms window before the relay (the only interconnection
    // point) had (re)subscribed to the topic. We force the same end-state
    // deterministically by isolating the writer only for the publish.

    // 1. Star topology A—R—B. A creates the DB; R and B open it by address
    //    (manifest propagates A→R→B). Everyone subscribes; a gossipsub mesh
    //    settles. All empty.
    await writerA.libp2p.dial(relayAddr)
    await readerB.libp2p.dial(relayAddr)
    const dbA = await writerA.orbitdb.open('sync-repro', { type: 'keyvalue' })
    const address = dbA.address.toString()
    const dbR = await relayR.orbitdb.open(address)
    const dbB = await readerB.orbitdb.open(address)
    // R has both A and B in its mesh — the exact state the live logs showed
    // (GRAFT: Add mesh link from A / from B), and R has done its one-shot
    // heads exchange with each while empty.
    await waitFor(() => dbR.peers.size >= 2 && dbB.peers.size >= 1)
    const relayMesh = () => relayR.libp2p.services.pubsub.mesh?.get?.(address)
    await waitFor(() => (relayMesh()?.size ?? 0) >= 2)

    // 2. Isolate the writer, then append the FIRST entry. `sync.add` publishes
    //    it, but A now has no connected peer — the publish reaches nobody.
    await writerA.libp2p.hangUp(relayR.libp2p.peerId)
    await waitFor(() => writerA.libp2p.getConnections(relayR.libp2p.peerId).length === 0)
    await dbA.put('k', 'first-entry')

    // 3. Let A's gossipsub message cache (mcache) expire so that reconnecting
    //    cannot rescue B via IHAVE/IWANT gossip — this isolates the pure
    //    heads-exchange path, which is exactly how the relay obtained the
    //    entry live.
    await new Promise((resolve) => setTimeout(resolve, 8000))

    // 4. A reconnects. R was never unsubscribed, but A disconnected, so R
    //    cleared A from its Sync `peers` cache (peer:disconnect) — on
    //    reconnect the subscription replay fires a subscription-change and R
    //    performs a fresh heads exchange with A. R now HAS the entry, obtained
    //    via the exchange protocol, NOT via a gossipsub publish.
    await writerA.libp2p.dial(relayAddr)
    await waitFor(async () => {
      const all = await dbR.all()
      return all.length === 1 ? all : null
    })

    // 5. THE BUG. B is connected to R, subscribed, and in R's gossipsub mesh —
    //    but R got the entry via the heads exchange and never re-announces it,
    //    and B was never disconnected, so it is still in R's one-shot `peers`
    //    cache and is never re-exchanged with. B stays empty.
    await new Promise((resolve) => setTimeout(resolve, 10000))
    const bEntries = await dbB.all()
    assert.equal(
      bEntries.length,
      0,
      `expected the deadlock: B must NOT have the entry, got ${bEntries.length} ` +
        '(1 would mean the bug is fixed or gossip rescued it — see the issue analysis)'
    )

    // 6. Control — the data is one `sync.add` away. Re-announcing the relay's
    //    heads (the workaround this repo ships) delivers to B within moments,
    //    proving the deadlock is a Sync coordination gap, not a transport fault.
    for (const head of await dbR.log.heads()) {
      await dbR.sync.add(head)
    }
    await waitFor(async () => {
      const all = await dbB.all()
      return all.length === 1 ? all : null
    })
  })
})
