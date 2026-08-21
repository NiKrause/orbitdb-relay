import assert from 'node:assert/strict'

/**
 * Regression cover for the jam that stopped `either-thing-fatal-true.2n6.me`
 * from discovering databases over pubsub: two topic syncs that never finished
 * held both slots of the sync queue for good, so every topic announced after
 * them was queued and never run. From outside the process the node looked fine
 * — `connections` was healthy, and `POST /pinning/sync` still answered, because
 * the HTTP handler bypasses this queue entirely.
 */

class FakePubsub extends EventTarget {
  constructor() {
    super()
    this.subscribed = []
    this.hangingSubscribes = new Set()
  }

  async subscribe(topic) {
    if (this.hangingSubscribes.has(topic)) {
      // No abort path, exactly like the real gossipsub call that hung.
      await new Promise(() => {})
    }
    this.subscribed.push(topic)
  }

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

function createFakeDatabaseService() {
  const state = {
    hangingTopics: new Set(),
    syncedTopics: [],
    syncStarts: [],
  }

  return {
    state,
    prefetchManifestForLogging: async () => {},
    getCachedDbName: () => undefined,
    rememberDatabasePeer: () => {},
    syncAllOrbitDBRecords: async (topic) => {
      state.syncStarts.push(topic)
      if (state.hangingTopics.has(topic)) {
        // No abort path — exactly like the real OrbitDB/Helia call.
        await new Promise(() => {})
      }
      state.syncedTopics.push(topic)
    },
  }
}

async function waitFor(predicate, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (predicate()) return
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
  assert.fail(`timed out after ${timeoutMs}ms waiting for ${label}`)
}

const HANGING = [
  '/orbitdb/zdpuHangingSettingsDbOne',
  '/orbitdb/zdpuHangingSettingsDbTwo',
  '/orbitdb/zdpuHangingSettingsDbThree',
]
const HEALTHY = '/orbitdb/zdpuHealthyPostsDb'

describe('orbitdb topic sync queue', function () {
  this.timeout(30000)

  let setupOrbitdbReplicationHandlers
  let handlers
  let previousEnv

  before(async () => {
    previousEnv = {
      timeout: process.env.RELAY_ORBITDB_SYNC_TIMEOUT_MS,
      cooldown: process.env.RELAY_ORBITDB_SYNC_COOLDOWN_MS,
      maxCooldown: process.env.RELAY_ORBITDB_SYNC_MAX_COOLDOWN_MS,
    }
    ;({ setupOrbitdbReplicationHandlers } = await import(
      '../dist/services/orbitdb-replication-service.js'
    ))
  })

  after(() => {
    for (const [key, value] of [
      ['RELAY_ORBITDB_SYNC_TIMEOUT_MS', previousEnv.timeout],
      ['RELAY_ORBITDB_SYNC_COOLDOWN_MS', previousEnv.cooldown],
      ['RELAY_ORBITDB_SYNC_MAX_COOLDOWN_MS', previousEnv.maxCooldown],
    ]) {
      if (value === undefined) delete process.env[key]
      else process.env[key] = value
    }
  })

  afterEach(async () => {
    await handlers?.cleanup()
    handlers = undefined
  })

  it('keeps running new topics while other syncs hang', async () => {
    process.env.RELAY_ORBITDB_SYNC_TIMEOUT_MS = '300'
    process.env.RELAY_ORBITDB_SYNC_COOLDOWN_MS = '5000'
    process.env.RELAY_ORBITDB_SYNC_MAX_COOLDOWN_MS = '5000'

    const pubsub = new FakePubsub()
    const databaseService = createFakeDatabaseService()
    for (const topic of HANGING) databaseService.state.hangingTopics.add(topic)

    handlers = setupOrbitdbReplicationHandlers(
      { services: { pubsub } },
      databaseService,
    )

    // Three databases that never finish, against a queue with two slots.
    for (const topic of HANGING) pubsub.announce(topic)
    await waitFor(
      () => databaseService.state.syncStarts.length === 2,
      2000,
      'both queue slots to be occupied',
    )

    // A healthy database announced while the queue is full. Before the timeout
    // existed this sync never started, no matter how long the relay ran.
    pubsub.announce(HEALTHY)
    await waitFor(
      () => databaseService.state.syncedTopics.includes(HEALTHY),
      5000,
      'the healthy database to be synced',
    )

    const stats = handlers.getQueueStats()
    assert.ok(
      stats.timedOutSyncs >= 2,
      `expected the hanging syncs to time out, got ${stats.timedOutSyncs}`,
    )
    assert.ok(
      stats.cooldownTopics >= 1,
      `expected timed-out topics to be in cooldown, got ${stats.cooldownTopics}`,
    )
  })

  it('holds a timed-out topic back instead of re-queueing it at once', async () => {
    process.env.RELAY_ORBITDB_SYNC_TIMEOUT_MS = '200'
    process.env.RELAY_ORBITDB_SYNC_COOLDOWN_MS = '60000'
    process.env.RELAY_ORBITDB_SYNC_MAX_COOLDOWN_MS = '60000'

    const pubsub = new FakePubsub()
    const databaseService = createFakeDatabaseService()
    databaseService.state.hangingTopics.add(HANGING[0])

    handlers = setupOrbitdbReplicationHandlers(
      { services: { pubsub } },
      databaseService,
    )

    pubsub.announce(HANGING[0])
    await waitFor(
      () => handlers.getQueueStats().timedOutSyncs === 1,
      3000,
      'the hanging sync to time out',
    )

    // Re-announcement during the cooldown must not start the sync again;
    // otherwise a permanently broken database still owns a slot continuously.
    for (let i = 0; i < 5; i++) pubsub.announce(HANGING[0])
    await new Promise((resolve) => setTimeout(resolve, 300))

    assert.equal(databaseService.state.syncStarts.length, 1)
    assert.equal(handlers.getQueueStats().cooldownTopics, 1)
  })

  it('clears a topic\'s backoff once its sync succeeds', async () => {
    process.env.RELAY_ORBITDB_SYNC_TIMEOUT_MS = '200'
    process.env.RELAY_ORBITDB_SYNC_COOLDOWN_MS = '250'
    process.env.RELAY_ORBITDB_SYNC_MAX_COOLDOWN_MS = '250'

    const pubsub = new FakePubsub()
    const databaseService = createFakeDatabaseService()
    databaseService.state.hangingTopics.add(HANGING[0])

    handlers = setupOrbitdbReplicationHandlers(
      { services: { pubsub } },
      databaseService,
    )

    pubsub.announce(HANGING[0])
    await waitFor(
      () => handlers.getQueueStats().timedOutSyncs === 1,
      3000,
      'the hanging sync to time out',
    )

    databaseService.state.hangingTopics.delete(HANGING[0])
    await waitFor(
      () => handlers.getQueueStats().cooldownTopics === 0,
      3000,
      'the cooldown to elapse',
    )

    pubsub.announce(HANGING[0])
    await waitFor(
      () => databaseService.state.syncedTopics.includes(HANGING[0]),
      3000,
      'the recovered database to be synced',
    )
    assert.equal(handlers.getQueueStats().cooldownTopics, 0)
  })

  it('does not let a hanging subscribe hold a slot', async () => {
    // Where the deployed relay actually hung: every timed-out task stopped
    // inside the subscribe step, before it reached any sync work.
    process.env.RELAY_ORBITDB_SUBSCRIBE_TIMEOUT_MS = '200'
    process.env.RELAY_ORBITDB_SYNC_TIMEOUT_MS = '10000'
    process.env.RELAY_ORBITDB_SYNC_COOLDOWN_MS = '5000'
    process.env.RELAY_ORBITDB_SYNC_MAX_COOLDOWN_MS = '5000'

    const pubsub = new FakePubsub()
    const databaseService = createFakeDatabaseService()
    pubsub.hangingSubscribes.add(HEALTHY)

    handlers = setupOrbitdbReplicationHandlers(
      { services: { pubsub } },
      databaseService,
    )

    pubsub.announce(HEALTHY)
    // Without the deadline the task would sit in subscribe for the full 10 s
    // sync timeout; with it, the sync runs as soon as subscribing gives up.
    await waitFor(
      () => databaseService.state.syncedTopics.includes(HEALTHY),
      3000,
      'the sync to run despite a hanging subscribe',
    )
    assert.equal(handlers.getQueueStats().timedOutSyncs, 0)
  })

  it('names the topics currently holding a slot', async () => {
    process.env.RELAY_ORBITDB_SUBSCRIBE_TIMEOUT_MS = '5000'
    process.env.RELAY_ORBITDB_SYNC_TIMEOUT_MS = '10000'
    process.env.RELAY_ORBITDB_SYNC_COOLDOWN_MS = '5000'
    process.env.RELAY_ORBITDB_SYNC_MAX_COOLDOWN_MS = '5000'

    const pubsub = new FakePubsub()
    const databaseService = createFakeDatabaseService()
    databaseService.state.hangingTopics.add(HANGING[0])

    handlers = setupOrbitdbReplicationHandlers(
      { services: { pubsub } },
      databaseService,
    )

    pubsub.announce(HANGING[0])
    await waitFor(
      () => handlers.getQueueStats().activeTopics.length === 1,
      3000,
      'the running topic to be reported',
    )

    const [entry] = handlers.getQueueStats().activeTopics
    assert.equal(entry.topic, HANGING[0])
    assert.ok(
      Number.isFinite(entry.runningMs) && entry.runningMs >= 0,
      `expected a runtime for the held slot, got ${entry.runningMs}`,
    )

    // A finished topic must not linger in the readout.
    databaseService.state.hangingTopics.delete(HANGING[0])
    pubsub.announce(HEALTHY)
    await waitFor(
      () => databaseService.state.syncedTopics.includes(HEALTHY),
      5000,
      'the healthy topic to finish',
    )
    assert.ok(
      !handlers.getQueueStats().activeTopics.some((e) => e.topic === HEALTHY),
      'a finished topic should not be reported as active',
    )
  })
})
