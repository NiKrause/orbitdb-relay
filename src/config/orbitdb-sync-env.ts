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
 * it. Measured on the deployed relay: every timed-out sync hung *here*, before
 * any sync work started — the task never reached its first log line. Subscribing
 * is normally near-instant, so this is a stuck-detector, not a budget.
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
