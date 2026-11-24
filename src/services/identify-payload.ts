/**
 * How close identify is to the size at which clients drop it.
 *
 * libp2p rejects an identify response larger than `maxMessageSize` — 8192 by
 * default — and it rejects the whole message, not the entries that made it
 * large. A relay that holds many databases open therefore loses `hop`,
 * `bitswap` and `meshsub` from every client's view at once, silently on both
 * sides. See #50 for the measurement and #51 for why the protocol list grows.
 *
 * Nothing here changes what is announced. It reports the number the relay is
 * about to exceed, so the cliff is visible before it is reached.
 */

/** libp2p's default `identify({ maxMessageSize })`. */
export const IDENTIFY_DEFAULT_MAX_MESSAGE_SIZE = 8192

/** Warn once the payload passes this share of the limit. */
export const IDENTIFY_PAYLOAD_WARN_RATIO = 0.85

export type IdentifyPayloadSummary = {
  /** Every protocol identify announces. */
  protocolCount: number
  /**
   * The `/orbitdb/heads/*` protocols the node answers, one per database it
   * holds open or knows. Not part of `protocolCount` or `estimatedBytes` when
   * they are kept out of identify ({@link SummariseIdentifyPayloadOptions}).
   */
  orbitdbHeadsProtocolCount: number
  addressCount: number
  estimatedBytes: number
  limitBytes: number
  /** `estimatedBytes / limitBytes`, rounded to three places. */
  ratio: number
  overLimit: boolean
}

const ORBITDB_HEADS_PREFIX = '/orbitdb/heads/'

export type SummariseIdentifyPayloadOptions = {
  /**
   * The node answers `/orbitdb/heads/*` but keeps it out of identify, as the
   * relay does with `withoutOrbitdbHeadsAnnouncements`. Size the payload
   * without those protocols.
   */
  excludeOrbitdbHeads?: boolean
}

/**
 * Estimate the encoded size of an identify response.
 *
 * Addresses are counted twice on purpose: identify carries them once plainly
 * and once more inside the signed peer record. Calibrated against a relay
 * whose real response measured 10538 bytes with 122 protocols and 27
 * addresses, where this returns 10537 — close enough to warn on, not exact
 * enough to gate anything.
 */
export function estimateIdentifyPayloadBytes(protocols: string[], addressByteLengths: number[]): number {
  const protocolBytes = protocols.reduce((total, protocol) => total + Buffer.byteLength(protocol) + 2, 0)
  const addressBytes = addressByteLengths.reduce((total, length) => total + length + 2, 0)
  const FIXED_OVERHEAD = 350 // public key, agent and protocol version, observed address, signature
  return protocolBytes + addressBytes * 2 + FIXED_OVERHEAD
}

export type IdentifySource = {
  getProtocols?: () => string[]
  getMultiaddrs?: () => Array<{ bytes?: Uint8Array; toString: () => string }>
}

/** Read the current protocol and address set and size it up. */
export function summariseIdentifyPayload(
  libp2p: IdentifySource | null | undefined,
  limitBytes: number = IDENTIFY_DEFAULT_MAX_MESSAGE_SIZE,
  options: SummariseIdentifyPayloadOptions = {},
): IdentifyPayloadSummary {
  const answered = libp2p?.getProtocols?.() ?? []
  const orbitdbHeadsProtocolCount = answered.filter((p) => p.startsWith(ORBITDB_HEADS_PREFIX)).length
  const protocols = options.excludeOrbitdbHeads
    ? answered.filter((p) => !p.startsWith(ORBITDB_HEADS_PREFIX))
    : answered
  const addresses = libp2p?.getMultiaddrs?.() ?? []
  const addressByteLengths = addresses.map((ma) => ma.bytes?.length ?? Buffer.byteLength(ma.toString()))
  const estimatedBytes = estimateIdentifyPayloadBytes(protocols, addressByteLengths)
  return {
    protocolCount: protocols.length,
    orbitdbHeadsProtocolCount,
    addressCount: addresses.length,
    estimatedBytes,
    limitBytes,
    ratio: Math.round((estimatedBytes / limitBytes) * 1000) / 1000,
    overLimit: estimatedBytes > limitBytes,
  }
}

/**
 * Warn at most once per `intervalMs` while the payload sits near or over the
 * limit — a log line per scrape would bury the one that matters.
 */
export function createIdentifyPayloadWarner(
  warn: (message: string, detail: Record<string, unknown>) => void,
  intervalMs = 5 * 60_000,
): (summary: IdentifyPayloadSummary) => void {
  let lastWarnedAt = 0
  return (summary) => {
    if (summary.ratio < IDENTIFY_PAYLOAD_WARN_RATIO) return
    const now = Date.now()
    if (now - lastWarnedAt < intervalMs) return
    lastWarnedAt = now
    warn(
      summary.overLimit
        ? 'identify response exceeds the client default limit — peers cannot see this relay as a relay'
        : 'identify response is approaching the client default limit',
      {
        estimatedBytes: summary.estimatedBytes,
        limitBytes: summary.limitBytes,
        protocols: summary.protocolCount,
        orbitdbHeadsProtocols: summary.orbitdbHeadsProtocolCount,
        addresses: summary.addressCount,
      },
    )
  }
}
