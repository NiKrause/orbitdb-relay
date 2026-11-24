import assert from 'node:assert/strict'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'

import {
  closeRelayServiceNode,
  createClient,
  createRelayServiceNode,
  stopClient,
  waitFor,
} from './lib/relay-replication-harness.mjs'

/**
 * deContact's address books use the access controller from
 * @le-space/orbitdb-access-controller-decontact. The relay depends on it and the
 * shipped configuration names it in RELAY_ACCESS_CONTROLLERS, so a relay set up
 * from deploy/orbitdb-relay.env.example replicates those books.
 */

const PACKAGE = '@le-space/orbitdb-access-controller-decontact'

describe("deContact's access controller", function () {
  this.timeout(120000)

  let mod
  before(async () => {
    mod = await import('../dist/access/configured-access-controllers.js')
  })

  it('is named in the shipped relay configuration', async () => {
    const env = await readFile(join(process.cwd(), 'deploy', 'orbitdb-relay.env.example'), 'utf8')
    const line = env.split('\n').find((l) => l.startsWith('RELAY_ACCESS_CONTROLLERS='))
    assert.ok(line, 'RELAY_ACCESS_CONTROLLERS is set (not commented out)')
    assert.ok(mod.parseAccessControllerSpecifiers(line.split('=')[1]).includes(PACKAGE))
  })

  it('loads by its package name from where the relay is installed', async () => {
    const [controller] = await mod.loadAccessControllers([PACKAGE])
    assert.equal(controller.type, 'decontact-address-book')
  })

  describe('replication', () => {
    let tempRoot
    const localOrbitDbDir = join(process.cwd(), 'orbitdb')

    before(async () => {
      await rm(localOrbitDbDir, { recursive: true, force: true })
      tempRoot = await mkdtemp(join(tmpdir(), 'decontact-access-controller-'))
    })

    after(async () => {
      await rm(localOrbitDbDir, { recursive: true, force: true })
      if (tempRoot) await rm(tempRoot, { recursive: true, force: true })
    })

    it('replicates an address book the owner writes into', async () => {
      const accessControllers = await mod.loadAccessControllers([PACKAGE])
      const [AddressBookAccessController] = accessControllers
      const wsPort = 29692 + Math.floor(Math.random() * 1000)
      const relay = await createRelayServiceNode({
        storageDir: join(tempRoot, 'relay'),
        wsPort,
        tcpPort: wsPort - 1,
        accessControllers,
      })

      let alice
      try {
        alice = await createClient(join(tempRoot, 'alice'))
        await alice.libp2p.dial(relay.relayAddr)
        const book = await alice.orbitdb.open('/myAddressBook/test', {
          type: 'documents',
          AccessController: AddressBookAccessController(),
        })
        await book.put({ _id: 'own-card', firstName: 'Alice', lastName: 'Wolkenfabrik', owner: alice.orbitdb.identity.id })

        const pinning = relay.libp2p.services.orbitdbReplication.createPinningHttpHandlers()
        let result
        await waitFor(async () => {
          result = await pinning.syncDatabase(book.address)
          return result.ok && (result.entryCount ?? 0) >= 1
        }, 60000, 1000)
        assert.equal(result.ok, true, result.error)
        assert.ok(result.entryCount >= 1, 'the relay holds the card')
      } finally {
        if (alice) await stopClient(alice)
        await closeRelayServiceNode(relay)
      }
    })
  })
})
