import assert from 'node:assert/strict'

describe('delegated todo access controller varsig fallback', function () {
  let verifyIdentityWithFallback

  before(async () => {
    ;({ verifyIdentityWithFallback } = await import('../dist/access/shared.js'))
  })

  it('uses verifyIdentityFallback when verifyIdentity throws unsupported varsig header', async () => {
    let primaryCalls = 0
    let fallbackCalls = 0

    const identities = {
      verifyIdentity: async () => {
        primaryCalls += 1
        throw new Error('Unsupported varsig header')
      },
      verifyIdentityFallback: async () => {
        fallbackCalls += 1
        return true
      }
    }

    const verified = await verifyIdentityWithFallback(identities, { id: 'did:key:test' })
    assert.equal(verified, true)
    assert.equal(primaryCalls, 1)
    assert.equal(fallbackCalls, 1)
  })

  it('returns false when fallback verifier returns false', async () => {
    const identities = {
      verifyIdentity: async () => {
        throw new Error('Unsupported varsig header')
      },
      verifyIdentityFallback: async () => false
    }

    const verified = await verifyIdentityWithFallback(identities, { id: 'did:key:test' })
    assert.equal(verified, false)
  })

  it('rethrows non-varsig verification errors', async () => {
    const identities = {
      verifyIdentity: async () => {
        throw new Error('invalid signature payload')
      }
    }

    await assert.rejects(
      () => verifyIdentityWithFallback(identities, { id: 'did:key:test' }),
      /invalid signature payload/
    )
  })
})
