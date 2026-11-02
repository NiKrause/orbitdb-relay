import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { setTimeout as delay } from 'node:timers/promises'

import { CID } from 'multiformats/cid'
import { sha256 } from 'multiformats/hashes/sha2'
import * as raw from 'multiformats/codecs/raw'
import { fromString as uint8ArrayFromString } from 'uint8arrays/from-string'

import {
  closeRelayServiceNode,
  createClient,
  createRelayServiceNode,
  stopClient,
  waitFor,
} from './lib/relay-replication-harness.mjs'

/**
 * #66, step 3. A database the relay holds open is replicated by OrbitDB
 * itself, yet every pubsub message on its topic used to queue another full
 * sync of it, taking a slot from databases the relay had not seen yet. Those
 * syncs were also what pinned media from new entries, so without them the
 * open database has to do that from its own `update` events.
 */

async function putImage(client, text) {
  const bytes = uint8ArrayFromString(text)
  const cid = CID.createV1(raw.code, await sha256.digest(bytes))
  await client.ipfs.blockstore.put(cid, bytes)
  return cid.toString()
}

describe('a database the relay holds open', function () {
  this.timeout(120000)

  let tempRoot
  const localOrbitDbDir = join(process.cwd(), 'orbitdb')

  before(async () => {
    await rm(localOrbitDbDir, { recursive: true, force: true })
    tempRoot = await mkdtemp(join(tmpdir(), 'held-open-database-'))
  })

  after(async () => {
    await rm(localOrbitDbDir, { recursive: true, force: true })
    if (tempRoot) await rm(tempRoot, { recursive: true, force: true })
  })

  it('pins media from a later entry without queueing another sync', async () => {
    const wsPort = 27492 + Math.floor(Math.random() * 1000)
    const relay = await createRelayServiceNode({
      storageDir: join(tempRoot, 'relay'),
      wsPort,
      tcpPort: wsPort - 1,
    })
    const databaseService = relay.libp2p.services.orbitdbReplication.databaseService

    let alice
    try {
      alice = await createClient(join(tempRoot, 'alice'))
      await alice.libp2p.dial(relay.relayAddr)

      const db = await alice.orbitdb.open('posts', { type: 'events' })
      const firstImage = await putImage(alice, 'first image')
      await db.add({ postId: 'post-1', imageCid: firstImage })

      // Found over pubsub alone: no /pinning/sync call anywhere in this test.
      await waitFor(
        () =>
          databaseService.pinnedOpenDatabases.has(db.address) &&
          databaseService.pinnedImageCids.has(firstImage),
        60000,
      )
      // Let any sync still queued from the first round of messages finish.
      await waitFor(() => databaseService.syncInFlight.size === 0, 30000)
      const syncsBefore = databaseService.pinningSyncOperations

      const laterImage = await putImage(alice, 'later image')
      await db.add({ postId: 'post-2', imageCid: laterImage })

      await waitFor(() => databaseService.pinnedImageCids.has(laterImage), 30000)
      // A message on the topic would have queued a sync within milliseconds.
      await delay(1000)
      assert.equal(
        databaseService.pinningSyncOperations,
        syncsBefore,
        'a message on a held-open database queued another sync',
      )
    } finally {
      if (alice) await stopClient(alice)
      await closeRelayServiceNode(relay)
    }
  })
})
