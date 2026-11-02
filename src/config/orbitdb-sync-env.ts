/**
 * Bounds on pubsub-driven OrbitDB topic syncs.
 *
 * The relay reacts to gossipsub `subscription-change` / `message` events by
 * queueing a sync for the announced topic. That queue has a small fixed
 * concurrency, and the work it runs — `loadManifest` and friends — has no abort
 * path of its own, so a database that never finishes syncing used to hold its
 * slot forever. Two of those disabled p2p discovery for the whole relay while
 * HTTP `POST /pinning/sync` kept answering, because that path bypasses the
 * queue. These knobs are what stops a stuck database from becoming a stuck node.
 */

/** How long a queued topic sync may hold its slot before we stop waiting. */
export const DEFAULT_ORBITDB_TOPIC_SYNC_TIMEOUT_MS = 120000

/**
 * How long a topic is held out of the queue after a sync timed out or failed.
 * Without it, a permanently broken database would be re-queued the instant its
 * slot is released and would still monopolise the queue — the same jam, only
 * with a timer in it.
 */
export const DEFAULT_ORBITDB_TOPIC_SYNC_COOLDOWN_MS = 60000

/** Ceiling for the doubling cooldown, so a hopeless topic is still retried now and then. */
export const DEFAULT_ORBITDB_TOPIC_SYNC_MAX_COOLDOWN_MS = 15 * 60 * 1000

/**
 * How long `pubsub.subscribe()` may take before a queued topic sync gives up on
 * it. Belt and braces: gossipsub subscribes synchronously, so this never fires
 * in practice, but `pubsub` is untyped at the call site. The hang this was
 * originally meant to catch turned out to be the manifest fetch alongside it.
 */
export const DEFAULT_ORBITDB_SUBSCRIBE_TIMEOUT_MS = 15000

function envInt(name: string, defaultVal: number): number {
  const raw = process.env[name]
  if (raw === undefined || raw === '') return defaultVal
  const n = Number(raw)
  return Number.isFinite(n) && n >= 0 ? Math.floor(n) : defaultVal
}

export function getOrbitdbTopicSyncTimeoutMs(): number {
  return envInt('RELAY_ORBITDB_SYNC_TIMEOUT_MS', DEFAULT_ORBITDB_TOPIC_SYNC_TIMEOUT_MS)
}

export function getOrbitdbTopicSyncCooldownMs(): number {
  return envInt('RELAY_ORBITDB_SYNC_COOLDOWN_MS', DEFAULT_ORBITDB_TOPIC_SYNC_COOLDOWN_MS)
}

export function getOrbitdbTopicSyncMaxCooldownMs(): number {
  return envInt('RELAY_ORBITDB_SYNC_MAX_COOLDOWN_MS', DEFAULT_ORBITDB_TOPIC_SYNC_MAX_COOLDOWN_MS)
}

export function getOrbitdbSubscribeTimeoutMs(): number {
  return envInt('RELAY_ORBITDB_SUBSCRIBE_TIMEOUT_MS', DEFAULT_ORBITDB_SUBSCRIBE_TIMEOUT_MS)
}

/**
 * The fast lane (#66): on first sight of a topic, fetch the blocks a peer needs
 * to `orbitdb.open()` it — the manifest and its access controller — outside
 * the sync queue. A guest opening somebody else's fresh list gives up after
 * OrbitDB's 30 s block timeout, and the queue alone held new lists ~52 s late.
 */

/** Chain fetches run at once. Each is a handful of bounded block fetches. */
export const DEFAULT_ORBITDB_FAST_LANE_CONCURRENCY = 8

/** How long one block of the chain may take before the fetch gives up. */
export const DEFAULT_ORBITDB_FAST_LANE_BLOCK_TIMEOUT_MS = 5000

/** How soon a topic whose chain could not be fetched may be tried again. */
export const DEFAULT_ORBITDB_FAST_LANE_RETRY_MS = 30000

export function getOrbitdbFastLaneConcurrency(): number {
  return Math.max(1, envInt('RELAY_ORBITDB_FAST_LANE_CONCURRENCY', DEFAULT_ORBITDB_FAST_LANE_CONCURRENCY))
}

export function getOrbitdbFastLaneBlockTimeoutMs(): number {
  return envInt('RELAY_ORBITDB_FAST_LANE_BLOCK_TIMEOUT_MS', DEFAULT_ORBITDB_FAST_LANE_BLOCK_TIMEOUT_MS)
}

export function getOrbitdbFastLaneRetryMs(): number {
  return envInt('RELAY_ORBITDB_FAST_LANE_RETRY_MS', DEFAULT_ORBITDB_FAST_LANE_RETRY_MS)
}
