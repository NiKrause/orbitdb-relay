import assert from 'node:assert/strict'

/**
 * Regression cover for the hang that made topic syncs time out on the deployed
 * relay: `loadManifest` wrapped `blockstore.get()` in `withTimeout`, but that
 * call returns a *generator*, and `Promise.race` settles a non-thenable at once.
 * The deadline was therefore a no-op, and the `for await` that actually drains
 * the blocks ran unbounded — so a manifest that never arrived over bitswap
 * parked the task until the outer 120 s task timeout reclaimed its slot.
 *
 * Signature in production: ~15 timeouts in 88 minutes, every one a distinct
 * topic hanging on its first fetch, with no log line at all.
 */

// A real address from the deployed relay, so it parses like the genuine article.
const DB_ADDRESS =
  '/orbitdb/zdpuAwHERKDrG4x1FZP2NXEHyQzD2KyUozhMTnAftGiZDgZeq'

/** A blockstore whose blocks never arrive — an unresponsive bitswap peer. */
function neverYieldingBlockstore() {
  return {
    blockstore: {
      get() {
        return (async function* () {
          await new Promise(() => {})
          yield new Uint8Array()
        })()
      },
    },
  }
}

describe('manifest load deadline', function () {
  this.timeout(60000)

  let DatabaseService

  before(async () => {
    ;({ DatabaseService } = await import('../dist/services/database.js'))
  })

  it('gives up on a manifest whose blocks never arrive', async () => {
    const service = new DatabaseService()
    // Only the blockstore is exercised; a full initialize() would need Helia.
    service.ipfs = neverYieldingBlockstore()

    const startedAt = Date.now()
    // Swallows its own errors, so it resolves either way — the point is *when*.
    await service.prefetchManifestForLogging(DB_ADDRESS)
    const elapsedMs = Date.now() - startedAt

    assert.ok(
      elapsedMs < 30000,
      `manifest load ran unbounded (${elapsedMs}ms); the deadline did not apply`,
    )
    assert.ok(
      elapsedMs >= 1000,
      `expected the deadline to be awaited, returned after only ${elapsedMs}ms`,
    )
  })

  it('refuses a non-thenable instead of silently dropping the deadline', async () => {
    const service = new DatabaseService()
    await assert.rejects(
      // Reaches the private helper the same way the broken call site did.
      service.withTimeout((function* () {})(), 50, 'generator'),
      /requires a thenable/,
      'a generator must be rejected, not raced and returned instantly',
    )
  })
})
