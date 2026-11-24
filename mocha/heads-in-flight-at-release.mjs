/**
 * The relay must not close a database while a heads exchange for it is still
 * delivering entries.
 *
 * A closed database drops what arrives afterwards: OrbitDB's `close()` waits
 * for the joins already queued, but a head that comes in on an exchange that
 * is still open is joined against a closed log and lost. The peer that sent it
 * counts the exchange as done and does not send it again, so a reader that
 * depends on the relay never gets the entry.
 *
 * Seen in simple-todo's escrow01 E2E: the relay opened a list on demand,
 * received the owner's head and closed the list 356 ms later without it.
 *
 * Both paths that release a database are covered: the on-demand heads handler,
 * and a sync pass whose update wait ends while an exchange is still running.
 * In both the exchange is a raw heads stream from Alice whose head arrives
 * after the point where the relay used to close: exactly what a slow peer, or
 * a busy browser on the other end of a circuit, looks like to the relay.
 */
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { setTimeout as delay } from 'node:timers/promises'

import {
  closeRelayServiceNode,
  createClient,
  createRelayServiceNode,
  stopClient,
  waitFor,
} from './lib/relay-replication-harness.mjs'

/**
 * Open a heads exchange with the relay the way OrbitDB's Sync does, but let
 * the test decide when the heads go out.
 */
async function openHeadsExchange(client, relayPeerId, dbAddress) {
  const stream = await client.libp2p.dialProtocol(relayPeerId, `/orbitdb/heads${dbAddress}`)
  const failures = []
  const reading = (async () => {
    try {
      for await (const _ of stream) {
        // The relay's heads are not what these tests are about.
      }
    } catch (error) {
      failures.push(error)
    }
  })()
  return {
    async sendHeadsAndClose(db) {
      for (const { hash } of await db.log.heads()) {
        stream.send(await db.log.storage.get(hash))
      }
      await stream.close()
      await reading
      assert.deepEqual(failures.map((e) => `${e.name}: ${e.message}`), [], 'the heads stream failed')
    },
  }
}

/** Whether the relay stored the entry, asked with Alice gone so it cannot fetch it now. */
async function relayHasEntry(dbService, dbAddress, hash) {
  const db = await dbService.retainOpenDatabase(dbAddress)
  try {
    return await db.log.has(hash)
  } finally {
    await dbService.releaseOpenDatabase(dbAddress, db)
  }
}

describe('a heads exchange still running when the relay is done with a database', function () {
  this.timeout(120000)

  let tempRoot
  let relay
  let dbService
  const localOrbitDbDir = join(process.cwd(), 'orbitdb')

  before(async () => {
    await rm(localOrbitDbDir, { recursive: true, force: true })
    tempRoot = await mkdtemp(join(tmpdir(), 'heads-in-flight-at-release-'))
    const wsPort = 27492 + Math.floor(Math.random() * 1000)
    relay = await createRelayServiceNode({
      storageDir: join(tempRoot, 'relay'),
      wsPort,
      tcpPort: wsPort - 1,
    })
    // White-box, as mocha/held-open-database.mjs is: when the relay opens,
    // keeps and closes a database is what the fix changes.
    dbService = relay.libp2p.services.orbitdbReplication.databaseService
  })

  after(async () => {
    if (relay) await closeRelayServiceNode(relay)
    await rm(localOrbitDbDir, { recursive: true, force: true })
    if (tempRoot) await rm(tempRoot, { recursive: true, force: true })
  })

  /**
   * Alice's database with one entry the relay has never seen. Alice runs no
   * Sync of her own (`sync: false`): every exchange in these tests is one the
   * test opens, so nothing else can hand the relay the entry.
   */
  async function aliceWithOneEntry(name) {
    const alice = await createClient(join(tempRoot, name))
    await alice.libp2p.dial(relay.relayAddr)
    const db = await alice.orbitdb.open(`${name}-${Date.now()}`, { type: 'events', sync: false })
    const hash = await db.add({ text: 'written while the relay had the list closed' })
    return { alice, db, hash }
  }

  it('keeps an on-demand database open until a concurrent exchange has delivered its heads', async () => {
    const { alice, db, hash } = await aliceWithOneEntry('on-demand')
    const dbAddress = db.address
    const relayPeerId = relay.libp2p.peerId
    let stopped = false
    try {
      // The relay knows the list but holds nothing of it, and has closed it:
      // only the on-demand handler answers its heads protocol now.
      dbService.rememberKnownDatabaseAddress(dbAddress)
      assert.equal(dbService.databaseUseCounts.has(dbAddress), false)

      const onDemand = []
      const handle = dbService.handleOnDemandHeadsProtocol.bind(dbService)
      dbService.handleOnDemandHeadsProtocol = (...args) => {
        const run = handle(...args)
        onDemand.push(run)
        return run
      }
      const republished = []
      const republish = dbService.republishHeadsToSubscribers.bind(dbService)
      dbService.republishHeadsToSubscribers = async (address, ...rest) => {
        republished.push(address)
        return republish(address, ...rest)
      }

      // A pull that brings nothing opens the list on demand...
      const pull = await openHeadsExchange(alice, relayPeerId, dbAddress)
      await waitFor(() => dbService.databaseUseCounts.has(dbAddress), 30000)
      // ...and while it is open, OrbitDB's own handler takes a second exchange.
      const push = await openHeadsExchange(alice, relayPeerId, dbAddress)
      assert.equal(onDemand.length, 1, 'the second exchange should reach OrbitDB, not the on-demand handler')

      // The pull ends first. The relay used to close the list right here.
      await pull.sendHeadsAndClose({ log: { heads: async () => [] } })
      await delay(1000)
      await push.sendHeadsAndClose(db)

      await Promise.all(onDemand)
      await stopClient(alice)
      stopped = true

      assert.equal(
        await relayHasEntry(dbService, dbAddress, hash),
        true,
        'the relay closed the list before the head it was being sent arrived',
      )
      assert.equal(
        dbService.pinnedOpenDatabases.has(dbAddress),
        true,
        'a list that now has entries should stay open, as after a sync pass',
      )
      assert.equal(dbService.pinnedDatabasesByAddress.get(dbAddress)?.entryCount, 1)
      assert.ok(republished.includes(dbAddress), 'the heads the relay learnt were not re-announced')
    } finally {
      if (!stopped) await stopClient(alice)
    }
  })

  it('extends a sync pass while an exchange is still delivering heads', async () => {
    const { alice, db, hash } = await aliceWithOneEntry('sync-pass')
    const dbAddress = db.address
    let stopped = false
    try {
      // A sync pass waits 5 s for an update once it has the database open.
      // Alice's head is on its way, but arrives after that window: the pass
      // used to find nothing and close.
      const sync = dbService.syncAllOrbitDBRecords(dbAddress)
      await waitFor(() => dbService.databaseUseCounts.has(dbAddress), 30000)
      const openedAt = Date.now()
      const push = await openHeadsExchange(alice, relay.libp2p.peerId, dbAddress)
      await delay(Math.max(0, 6500 - (Date.now() - openedAt)))
      await push.sendHeadsAndClose(db)

      await sync
      await stopClient(alice)
      stopped = true

      assert.equal(
        await relayHasEntry(dbService, dbAddress, hash),
        true,
        'the sync pass closed the database before the head it was being sent arrived',
      )
      assert.equal(dbService.pinnedOpenDatabases.has(dbAddress), true)
    } finally {
      if (!stopped) await stopClient(alice)
    }
  })
})
