import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'

import { Identities, KeyStore, MemoryStorage } from '@orbitdb/core'
import {
  PROOF_CONTEXTS,
  WebAuthnDIDProvider,
  getWebAuthnConfig,
  proofFormat,
} from '@le-space/orbitdb-identity-provider-webauthn-did'

import {
  closeRelayServiceNode,
  createClient,
  createHeliaNode,
  createRelayServiceNode,
  stopClient,
  waitFor,
} from './lib/relay-replication-harness.mjs'
import {
  createFirstFormatPasskeyIdentity,
  createPasskeyIdentity,
  createSoftwarePasskey,
} from './lib/software-passkey.mjs'

/**
 * Provider 0.14.0 (GHSA-7j39-c465-x35x) gives every new passkey identity a proof in
 * a new format, which 0.13.0 and older cannot verify; devices set up before keep the
 * first-format proof they stored. The relay verifies identities for every app it
 * replicates, so it has to take both: the new ones, and the old ones that existing
 * history is written under.
 */

const localOrbitDbDir = join(process.cwd(), 'orbitdb')

describe('passkey identity proofs through the relay (provider 0.14.0)', function () {
  this.timeout(120000)

  let tempRoot
  let node
  let service
  let relayIdentities
  let verifyIdentityWithFallback
  let DelegatedTodoAccessController

  before(async () => {
    await rm(localOrbitDbDir, { recursive: true, force: true })
    tempRoot = await mkdtemp(join(tmpdir(), 'webauthn-identity-proofs-'))
    const [{ DatabaseService }, shared, delegated] = await Promise.all([
      import('../dist/services/database.js'),
      import('../dist/access/shared.js'),
      import('../dist/access/delegated-todo-access-controller.js'),
    ])
    verifyIdentityWithFallback = shared.verifyIdentityWithFallback
    DelegatedTodoAccessController = delegated.default

    node = await createHeliaNode()
    service = new DatabaseService()
    await service.initialize(node.ipfs, join(tempRoot, 'relay-orbitdb'))
    relayIdentities = service.orbitdb.identities
  })

  after(async () => {
    try {
      await service?.stop()
    } catch {}
    try {
      await node?.ipfs.stop()
    } catch {}
    try {
      await node?.libp2p.stop()
    } catch {}
    await rm(localOrbitDbDir, { recursive: true, force: true })
    if (tempRoot) await rm(tempRoot, { recursive: true, force: true })
  })

  it('keeps the provider default, so first-format proofs stay accepted', () => {
    assert.equal(getWebAuthnConfig().acceptLegacyProofs, undefined)
  })

  describe("the relay's identities", () => {
    it('verify an identity with a current-format proof, as provider 0.14.0 creates it', async () => {
      const passkey = await createSoftwarePasskey({ name: 'alice' })
      const { identity } = await createPasskeyIdentity(passkey, { ipfs: node.ipfs })
      assert.equal(identity.type, 'webauthn')
      assert.equal(proofFormat(identity.signatures.publicKey), 2)

      assert.equal(await relayIdentities.verifyIdentity(identity), true)
      assert.equal(await verifyIdentityWithFallback(relayIdentities, identity), true)
    })

    it('verify an identity with a first-format proof, as devices set up before 0.14.0 have it', async () => {
      const passkey = await createSoftwarePasskey({ name: 'bob' })
      const { identity } = await createFirstFormatPasskeyIdentity(passkey, { ipfs: node.ipfs })
      assert.equal(identity.type, 'webauthn')
      assert.equal(proofFormat(identity.signatures.publicKey), 1)

      assert.equal(await relayIdentities.verifyIdentity(identity), true)
      assert.equal(await verifyIdentityWithFallback(relayIdentities, identity), true)
    })

    it('verify both proofs of one passkey, the first format seen first', async () => {
      // A device moves from its first-format proof to a current one: the same DID
      // and signing key, so the same `signatures.id`, under a new identity document.
      const passkey = await createSoftwarePasskey({ name: 'carol' })
      const { identity: before } = await createFirstFormatPasskeyIdentity(passkey, { ipfs: node.ipfs })
      passkey.forgetIdentityProofs()
      const { identity: after } = await createPasskeyIdentity(passkey, { ipfs: node.ipfs })

      assert.equal(after.id, before.id)
      assert.equal(after.signatures.id, before.signatures.id)
      assert.notEqual(after.hash, before.hash)
      assert.equal(proofFormat(after.signatures.publicKey), 2)

      assert.equal(await relayIdentities.verifyIdentity(before), true)
      assert.equal(await relayIdentities.verifyIdentity(after), true)
      assert.equal(await relayIdentities.verifyIdentity(before), true)

      // OrbitDB's own cache, keyed by `signatures.id`, refuses the second document
      // (orbitdb/orbitdb#1258); the relay's identities are wrapped against that.
      const plain = await Identities({
        keystore: await KeyStore({ storage: await MemoryStorage() }),
        storage: await MemoryStorage(),
      })
      assert.equal(await plain.verifyIdentity(before), true)
      assert.equal(await plain.verifyIdentity(after), false)
    })

    it('refuse a proof that another passkey made, in either format', async () => {
      const owner = await createSoftwarePasskey({ name: 'owner' })
      const other = await createSoftwarePasskey({ name: 'other' })
      const { identity } = await createPasskeyIdentity(owner, { ipfs: node.ipfs })
      const data = identity.publicKey + identity.signatures.id

      const currentByOther = await other.use(() =>
        new WebAuthnDIDProvider(other.credential).sign(data, { context: PROOF_CONTEXTS.IDENTITY }),
      )
      const firstByOther = await other.firstFormatProof(data)
      assert.equal(proofFormat(currentByOther), 2)
      assert.equal(proofFormat(firstByOther), 1)

      for (const proof of [currentByOther, firstByOther]) {
        const claimed = { ...identity, signatures: { ...identity.signatures, publicKey: proof } }
        assert.equal(await relayIdentities.verifyIdentity(claimed), false)
        assert.equal(await verifyIdentityWithFallback(relayIdentities, claimed), false)
      }
    })
  })

  describe('the todo-delegation access controller', () => {
    let access

    before(async () => {
      access = await DelegatedTodoAccessController({ write: [service.orbitdb.identity.id] })({
        orbitdb: service.orbitdb,
        identities: relayIdentities,
        name: 'webauthn-identity-proofs-delegated-list',
      })
    })

    const delegationAction = (delegateDid, overrides = {}) => ({
      op: 'PUT',
      key: `delegation-action/task-1/${encodeURIComponent(delegateDid)}/action-1`,
      value: {
        type: 'delegation-action',
        action: 'set-completed',
        setCompleted: true,
        taskKey: 'task-1',
        delegateDid,
        ...overrides,
      },
    })

    it("accepts a delegate's action under a current-format proof", async () => {
      const passkey = await createSoftwarePasskey({ name: 'dave' })
      const { identity } = await createPasskeyIdentity(passkey, { ipfs: node.ipfs })
      assert.equal(proofFormat(identity.signatures.publicKey), 2)
      assert.equal(await access.canAppend({ identity: identity.hash, payload: delegationAction(identity.id) }), true)
    })

    it("accepts a delegate's action under a first-format proof", async () => {
      const passkey = await createSoftwarePasskey({ name: 'erin' })
      const { identity } = await createFirstFormatPasskeyIdentity(passkey, { ipfs: node.ipfs })
      assert.equal(await access.canAppend({ identity: identity.hash, payload: delegationAction(identity.id) }), true)
    })

    it('refuses an action written for another delegate', async () => {
      const passkey = await createSoftwarePasskey({ name: 'frank' })
      const { identity } = await createPasskeyIdentity(passkey, { ipfs: node.ipfs })
      const otherDid = 'did:key:zDnaeAnotherDelegate'
      assert.equal(await access.canAppend({ identity: identity.hash, payload: delegationAction(otherDid) }), false)
    })

    it('refuses an action outside the delegation rules', async () => {
      const passkey = await createSoftwarePasskey({ name: 'grace' })
      const { identity } = await createPasskeyIdentity(passkey, { ipfs: node.ipfs })
      const patchingOtherFields = delegationAction(identity.id, { action: 'patch-fields', patch: { owner: 'grace' } })
      assert.equal(await access.canAppend({ identity: identity.hash, payload: patchingOtherFields }), false)
    })
  })
})

describe('replication of passkey writers through a relay (provider 0.14.0)', function () {
  this.timeout(180000)

  let tempRoot

  before(async () => {
    await rm(localOrbitDbDir, { recursive: true, force: true })
    tempRoot = await mkdtemp(join(tmpdir(), 'webauthn-identity-replication-'))
  })

  after(async () => {
    await rm(localOrbitDbDir, { recursive: true, force: true })
    if (tempRoot) await rm(tempRoot, { recursive: true, force: true })
  })

  it('holds the entries of a writer in either proof format', async () => {
    // Away from the ports other local E2E suites use (4411-4451, 29300-29323).
    const wsPort = 31001 + Math.floor(Math.random() * 1000)
    const relay = await createRelayServiceNode({
      storageDir: join(tempRoot, 'relay'),
      wsPort,
      tcpPort: wsPort - 1,
    })

    try {
      const pinning = relay.libp2p.services.orbitdbReplication.createPinningHttpHandlers()
      const writers = [
        { label: 'current', format: 2, makeIdentity: createPasskeyIdentity },
        { label: 'first', format: 1, makeIdentity: createFirstFormatPasskeyIdentity },
      ]

      for (const { label, format, makeIdentity } of writers) {
        const passkey = await createSoftwarePasskey({ name: `writer-${label}` })
        let client
        try {
          client = await createClient(join(tempRoot, `writer-${label}`), {
            makeIdentity: (ipfs) => makeIdentity(passkey, { ipfs }),
          })
          assert.equal(client.orbitdb.identity.type, 'webauthn')
          assert.equal(proofFormat(client.orbitdb.identity.signatures.publicKey), format)

          await client.libp2p.dial(relay.relayAddr)
          const db = await client.orbitdb.open(`passkey-writer-${label}`, { type: 'events' })
          await db.add({ text: `written under a ${label}-format proof` })

          let result
          await waitFor(async () => {
            result = await pinning.syncDatabase(db.address)
            return result.ok && (result.entryCount ?? 0) >= 1
          }, 60000, 1000).catch((error) => {
            throw new Error(`${label} format: ${error.message}; last sync result: ${JSON.stringify(result)}`)
          })
          assert.equal(result.ok, true, result.error)
          assert.ok(result.entryCount >= 1, `the relay holds the entry written under a ${label}-format proof`)
        } finally {
          if (client) await stopClient(client)
        }
      }
    } finally {
      await closeRelayServiceNode(relay)
    }
  })
})
