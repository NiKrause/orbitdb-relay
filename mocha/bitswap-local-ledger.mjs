import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'

import * as Block from 'multiformats/block'
import * as dagCbor from '@ipld/dag-cbor'
import { sha256 } from 'multiformats/hashes/sha2'

import {
  closeRelayServiceNode,
  createClient,
  createRelayServiceNode,
  stopClient,
  withTimeout,
} from './lib/relay-replication-harness.mjs'

/**
 * #66. The relay's bitswap answers a peer from the blocks it holds. Upstream
 * `withBitswap` hands bitswap Helia's networked blockstore, so a peer's want
 * for a block the relay lacked became the relay's own want, and every block
 * asked for in the same message waited on it, without end.
 */

const encode = (value) => Block.encode({ value, codec: dagCbor, hasher: sha256 })

async function collect(source) {
  const chunks = []
  for await (const chunk of source) chunks.push(chunk)
  return Buffer.concat(chunks)
}

describe('relay bitswap ledger', function () {
  this.timeout(60000)

  let tempRoot
  let relay
  let bob

  before(async () => {
    tempRoot = await mkdtemp(join(tmpdir(), 'bitswap-local-ledger-'))
    const wsPort = 27592 + Math.floor(Math.random() * 1000)
    relay = await createRelayServiceNode({
      storageDir: join(tempRoot, 'relay'),
      wsPort,
      tcpPort: wsPort - 1,
    })
    bob = await createClient(join(tempRoot, 'bob'))
    await bob.libp2p.dial(relay.relayAddr)
  })

  after(async () => {
    if (bob) await stopClient(bob)
    if (relay) await closeRelayServiceNode(relay)
    if (tempRoot) await rm(tempRoot, { recursive: true, force: true })
  })

  it('still fetches a block a peer holds', async () => {
    const block = await encode({ heldBy: 'bob' })
    await bob.ipfs.blockstore.put(block.cid, block.bytes)

    const ipfs = relay.libp2p.services.orbitdbReplication.ipfs
    const bytes = await collect(
      ipfs.blockstore.get(block.cid, { signal: AbortSignal.timeout(10000) }),
    )
    assert.deepEqual(bytes, Buffer.from(block.bytes))
    assert.ok(await relay.storage.blockstore.has(block.cid))
  })

  it('sends the blocks it holds without fetching the rest on a peer\'s behalf', async () => {
    const held = await encode({ heldBy: 'relay' })
    const missing = await encode({ heldBy: 'nobody' })
    await relay.storage.blockstore.put(held.cid, held.bytes)

    // Asked for together, so both wants reach the relay in one message.
    const missingWant = collect(
      bob.ipfs.blockstore.get(missing.cid, { signal: AbortSignal.timeout(3000) }),
    ).catch(() => null)
    const bytes = await withTimeout(collect(bob.ipfs.blockstore.get(held.cid)), 10000)
    assert.deepEqual(bytes, Buffer.from(held.bytes))
    assert.equal(await missingWant, null)

    const bitswap = relay.libp2p.services.orbitdbReplication.ipfs.blockBrokers.find(
      (broker) => broker.name === 'bitswap',
    ).bitswap
    assert.ok(
      !bitswap.getWantlist().some(({ cid }) => cid.equals(missing.cid)),
      'the relay wants a block only Bob asked it for',
    )
  })
})
