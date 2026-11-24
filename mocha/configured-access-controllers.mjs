import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { pathToFileURL } from 'node:url'

import {
  closeRelayServiceNode,
  createClient,
  createRelayServiceNode,
  stopClient,
  waitFor,
} from './lib/relay-replication-harness.mjs'

/**
 * An app with its own OrbitDB access controller can have its databases
 * replicated: the operator names the controller's module in
 * RELAY_ACCESS_CONTROLLERS (or passes it as `accessControllers`), and the relay
 * registers it next to the built-in ones.
 */

const fixtures = join(process.cwd(), 'mocha', 'fixtures', 'access-controllers')

describe('configured access controllers', function () {
  this.timeout(120000)

  let mod
  before(async () => {
    mod = await import('../dist/access/configured-access-controllers.js')
  })

  it('reads a comma-separated list of module specifiers', () => {
    assert.deepEqual(mod.parseAccessControllerSpecifiers(' a, ./b.js,,c '), ['a', './b.js', 'c'])
    assert.deepEqual(mod.parseAccessControllerSpecifiers(undefined), [])
  })

  it('resolves paths against the working directory and leaves package names alone', () => {
    assert.equal(mod.toImportSpecifier('./x.js', '/srv/relay'), pathToFileURL('/srv/relay/x.js').href)
    assert.equal(mod.toImportSpecifier('/opt/acl/x.js', '/srv/relay'), pathToFileURL('/opt/acl/x.js').href)
    assert.equal(mod.toImportSpecifier('@example/orbitdb-access-controller', '/srv/relay'), '@example/orbitdb-access-controller')
  })

  it('loads a default export and a named accessControllers array', async () => {
    const loaded = await mod.loadAccessControllers(['./default-export.mjs', './named-array.mjs'], { cwd: fixtures })
    assert.deepEqual(loaded.map((ac) => ac.type), ['fixture-default', 'fixture-named-a', 'fixture-named-b'])
  })

  it('stops on a module that exports no controller, or cannot be imported', async () => {
    await assert.rejects(
      mod.loadAccessControllers(['./no-controller.mjs'], { cwd: fixtures }),
      /exports no access controller/,
    )
    await assert.rejects(
      mod.loadAccessControllers(['./missing.mjs'], { cwd: fixtures }),
      /cannot import "\.\/missing\.mjs"/,
    )
  })

  it('refuses to replace a controller the relay brings itself', () => {
    const registered = []
    const fake = (type) => Object.assign(() => async () => ({}), { type })
    assert.throws(
      () => mod.registerAccessControllers([fake('orbitdb')], (ac) => registered.push(ac.type)),
      /built into the relay/,
    )
    assert.deepEqual(mod.registerAccessControllers([fake('my-app')], (ac) => registered.push(ac.type)), ['my-app'])
    assert.deepEqual(registered, ['my-app'])
  })

  describe('replication', () => {
    let tempRoot
    const localOrbitDbDir = join(process.cwd(), 'orbitdb')

    before(async () => {
      await rm(localOrbitDbDir, { recursive: true, force: true })
      tempRoot = await mkdtemp(join(tmpdir(), 'configured-access-controllers-'))
    })

    after(async () => {
      await rm(localOrbitDbDir, { recursive: true, force: true })
      if (tempRoot) await rm(tempRoot, { recursive: true, force: true })
    })

    it("replicates a database whose controller came from the relay's configuration", async () => {
      // a type of its own: the registry is per process and shared with the client below
      const type = `configured-test-${Date.now()}`
      const ConfiguredAccessController = () => async ({ address }) => ({
        type,
        address: address ?? `/${type}/acl`,
        canAppend: async () => true,
      })
      ConfiguredAccessController.type = type

      const wsPort = 28592 + Math.floor(Math.random() * 1000)
      const relay = await createRelayServiceNode({
        storageDir: join(tempRoot, 'relay'),
        wsPort,
        tcpPort: wsPort - 1,
        accessControllers: [ConfiguredAccessController],
      })

      let alice
      try {
        alice = await createClient(join(tempRoot, 'alice'))
        await alice.libp2p.dial(relay.relayAddr)
        const db = await alice.orbitdb.open('contacts', {
          type: 'documents',
          AccessController: ConfiguredAccessController(),
        })
        await db.put({ _id: 'card', name: 'Wolkenfabrik Hosting GmbH' })

        const pinning = relay.libp2p.services.orbitdbReplication.createPinningHttpHandlers()
        let result
        await waitFor(async () => {
          result = await pinning.syncDatabase(db.address)
          return result.ok && (result.entryCount ?? 0) >= 1
        }, 60000, 1000)
        assert.equal(result.ok, true, result.error)
        assert.ok(result.entryCount >= 1, 'the relay holds the entry')
      } finally {
        if (alice) await stopClient(alice)
        await closeRelayServiceNode(relay)
      }
    })
  })
})
