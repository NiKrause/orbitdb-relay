import http from 'http'
import https from 'https'
import client from 'prom-client'
import { logger } from '@libp2p/logger'
import { base36 } from 'multiformats/bases/base36'
import { createPinningHttpRequestHandler } from '../http/pinning-http.js'

const log = logger('le-space:relay')

let metricsInstance: MetricsServer | null = null
let defaultMetricsInitialized = false

type Libp2pLike = {
  peerId?: { toString: () => string; toCID?: () => { bytes: Uint8Array } }
  getMultiaddrs?: () => Array<{ toString: () => string }>
  getConnections?: () => unknown[]
}

/** Result of POST `/pinning/sync` (also embedded in JSON response with `dbAddress`). */
export type PinningSyncResult = {
  ok: boolean
  error?: string
  /** Relay observed at least one OrbitDB `update` while syncing this address. */
  receivedUpdate?: boolean
  /** True when no live `update` arrived and media CIDs were taken from `db.all()` instead. */
  fallbackScanUsed?: boolean
  /** Media CIDs extracted from updates and/or `db.all()` fallback (same field rules as update-driven pinning). */
  extractedMediaCids?: string[]
  /** Number of rows/entries visible in the relay's local snapshot after sync. */
  entryCount?: number | null
  /** Sanitized preview of the last row/entry visible in the relay's local snapshot after sync. */
  lastRecord?: Record<string, unknown> | null
  /** Where the relay local snapshot came from, e.g. `db.all()`, `iterator`, or update-event burst. */
  snapshotSource?: string
  /** True when this request waited on another in-flight sync for the same address. */
  coalesced?: boolean
}

/** Result of {@link PinningHttpHandlers.streamPinnedCid} for GET `/ipfs/...`. */
export type StreamPinnedCidResult =
  | { ok: true; contentType?: string; chunks: AsyncIterable<Uint8Array> }
  | { ok: false; status: number; error: string }

export type PinningHttpHandlers = {
  getStats: () => Record<string, unknown>
  /** When `address` is set, returns at most one entry (relay sync history only). */
  getDatabases: (opts?: { address?: string }) => { databases: Array<Record<string, unknown>>; total: number }
  syncDatabase: (dbAddress: string) => Promise<PinningSyncResult>
  /**
   * Stream bytes for a CID only if it is pinned in Helia and only from local storage
   * (`offline` / no network fetch). Optional `pathWithin` is a UnixFS path inside a directory CID.
   */
  streamPinnedCid?: (cidStr: string, pathWithin?: string) => Promise<StreamPinnedCidResult>
}

type MetricsServerOptions = {
  getLibp2p?: () => Libp2pLike | null
  pinning?: PinningHttpHandlers
  getHelia?: () => any | null
  pinningHttp?: {
    enabled?: boolean
    fallbackMode?: 'pinned-only' | 'pinned-first-network-fallback'
    catTimeoutMs?: number
  }
}

const syncCounter = new client.Counter({
  name: 'orbitdb_sync_total',
  help: 'Total number of OrbitDB synchronization operations',
  labelNames: ['type', 'status'],
})

const syncDurationHistogram = new client.Histogram({
  name: 'orbitdb_sync_duration_seconds',
  help: 'Duration of OrbitDB synchronization operations',
  labelNames: ['type'],
  buckets: [0.1, 0.5, 1, 2, 5, 10],
})

const relayInboundOrbitdbHeadsRejectCounter = new client.Counter({
  name: 'relay_inbound_rejected_missing_orbitdb_heads_total',
  help: 'Inbound connections closed after Identify because the remote advertised no /orbitdb/heads/* protocol',
})

/** Incremented when `RELAY_REQUIRE_ORBITDB_HEADS_PROTOCOL` closes an inbound peer after Identify. */
export function incRelayInboundOrbitdbHeadsReject(): void {
  relayInboundOrbitdbHeadsRejectCounter.inc()
}

function isPublicAddress(addr: string): boolean {
  if (!addr) return false
  if (addr.includes('/ip4/127.')) return false
  if (addr.includes('/ip4/10.')) return false
  if (addr.includes('/ip4/192.168.')) return false
  const m = addr.match(/\/ip4\/172\.(\d+)\./)
  if (m) {
    const octet = Number(m[1])
    if (octet >= 16 && octet <= 31) return false
  }
  if (addr.includes('/ip6/::1')) return false
  if (addr.includes('/ip6/fc') || addr.includes('/ip6/fd')) return false
  return true
}

function prioritizeAddresses(addrs: string[]): string[] {
  return [...addrs].sort((a, b) => {
    const aPublic = isPublicAddress(a)
    const bPublic = isPublicAddress(b)
    if (aPublic !== bPublic) return aPublic ? -1 : 1
    return a.localeCompare(b)
  })
}

/** `*.${zone}` is the AutoTLS wildcard; this helper derives the serving zone from the relay peer id. */
export function autoTlsServingZoneFromPeerId(peerId: Libp2pLike['peerId']): string | null {
  if (!peerId || typeof peerId.toCID !== 'function') return null
  try {
    const cid = peerId.toCID()
    if (!cid?.bytes) return null
    return `${base36.encode(cid.bytes)}.libp2p.direct`
  } catch {
    return null
  }
}

function readMetricsHttpsEnabled(): boolean {
  const v = process.env.METRICS_HTTPS_ENABLED?.trim().toLowerCase()
  return v === '1' || v === 'true'
}

function readMetricsHttpsPort(): number {
  const raw = process.env.METRICS_HTTPS_PORT?.trim()
  if (raw !== undefined && raw !== '') {
    const n = Number(raw)
    return Number.isFinite(n) ? n : 9443
  }
  return 9443
}
function readOptionalPortEnvVar(name: string): number | null {
  const raw = process.env[name]?.trim()
  if (!raw) return null
  const value = Number(raw)
  if (!Number.isFinite(value) || value < 1 || value > 65535) return null
  return value
}

function ipv6AutoTlsHost(ipv6: string, zone: string): string {
  let subdomain = ipv6.replace(/:/g, '-')
  if (subdomain.startsWith('-')) {
    subdomain = `0${subdomain}`
  }
  if (subdomain.endsWith('-')) {
    subdomain = `${subdomain}0`
  }
  return `${subdomain}.${zone}`
}

function metricsHttpsHost(autoTlsZone: string | null): string | null {
  const configuredHost = process.env.METRICS_HTTPS_PUBLIC_HOST?.trim()
  if (configuredHost) return configuredHost
  if (!autoTlsZone) return null

  const publicIpv4 = process.env.PUBLIC_IPV4?.trim()
  if (publicIpv4) {
    return `${publicIpv4.replace(/\./g, '-')}.${autoTlsZone}`
  }

  const publicIpv6 = process.env.PUBLIC_IPV6?.trim()
  if (publicIpv6) {
    return ipv6AutoTlsHost(publicIpv6, autoTlsZone)
  }

  return `metrics.${autoTlsZone}`
}

function metricsHttpsPayload(listening: boolean, tlsPort: number | null, autoTlsZone: string | null) {
  const enabled = readMetricsHttpsEnabled()
  const configuredPort = readMetricsHttpsPort()
  const internalPort = enabled ? (listening && tlsPort != null ? tlsPort : configuredPort) : null
  const externalPort = enabled ? (readOptionalPortEnvVar('EXTERNAL_METRICS_HTTPS_PORT') ?? internalPort) : null
  const host = enabled ? metricsHttpsHost(autoTlsZone) : null
  let exampleUrl: string | null = null
  if (enabled && host && externalPort != null) {
    exampleUrl = `https://${host}:${externalPort}/health`
  }
  let internalExampleUrl: string | null = null
  if (enabled && host && internalPort != null) {
    internalExampleUrl = `https://${host}:${internalPort}/health`
  }
  return {
    enabled,
    listening,
    port: internalPort,
    internalPort,
    externalPort,
    host,
    exampleUrl,
    internalExampleUrl,
  }
}

/** `*` or comma-separated exact origins (scheme+host+port). See `METRICS_CORS_ORIGIN`. */
type CorsOriginConfig = '*' | string[]

function readCorsOriginConfig(): CorsOriginConfig {
  const raw = process.env.METRICS_CORS_ORIGIN?.trim()
  if (!raw || raw === '*') return '*'
  const list = raw
    .split(',')
    .map((o) => o.trim())
    .filter(Boolean)
  return list.length > 0 ? list : '*'
}

/**
 * Cross-origin browser access (dashboards, dev servers). Sets headers for preflight and responses.
 * `METRICS_CORS_ORIGIN=*` (default) allows any origin. Use a comma-separated allowlist for production.
 */
function applyCorsHeaders(req: http.IncomingMessage, res: http.ServerResponse, config: CorsOriginConfig) {
  const requestOrigin = req.headers.origin
  let allowOrigin: string | undefined
  if (config === '*') {
    allowOrigin = '*'
  } else if (requestOrigin && config.includes(requestOrigin)) {
    allowOrigin = requestOrigin
    res.setHeader('Vary', 'Origin')
  }

  if (allowOrigin) {
    res.setHeader('Access-Control-Allow-Origin', allowOrigin)
  }

  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, HEAD')
  res.setHeader(
    'Access-Control-Allow-Headers',
    process.env.METRICS_CORS_ALLOW_HEADERS?.trim() || 'Content-Type, Authorization',
  )
  res.setHeader('Access-Control-Max-Age', process.env.METRICS_CORS_MAX_AGE?.trim() || '86400')
}

export class MetricsServer {
  server: http.Server | null
  tlsServer: https.Server | null
  startPromise: Promise<http.Server | null> | null
  tlsStartPromise: Promise<https.Server | null> | null
  private tlsApplyChain: Promise<void>
  private detachAutoTls: (() => void) | null
  options: MetricsServerOptions

  constructor(options: MetricsServerOptions = {}) {
    this.server = null
    this.tlsServer = null
    this.startPromise = null
    this.tlsStartPromise = null
    this.tlsApplyChain = Promise.resolve()
    this.detachAutoTls = null
    this.options = options

    if (!metricsInstance) {
      metricsInstance = this
    }

    if (options.getLibp2p) {
      metricsInstance.options.getLibp2p = options.getLibp2p
    }
    if (options.pinning) {
      metricsInstance.options.pinning = options.pinning
    }

    return metricsInstance
  }

  async getMetrics() {
    return await client.register.metrics()
  }

  private getLibp2p(): Libp2pLike | null {
    try {
      return this.options.getLibp2p?.() || null
    } catch {
      return null
    }
  }

  private ensureDefaultMetrics() {
    if (!defaultMetricsInitialized && !client.register.getSingleMetric('process_cpu_user_seconds_total')) {
      client.collectDefaultMetrics()
      defaultMetricsInitialized = true
    }
  }

  /**
   * When `METRICS_HTTPS_ENABLED` is set, subscribe to libp2p AutoTLS `certificate:*` events and reuse PEM material
   * for a second listener (see `METRICS_HTTPS_PORT`). Call after `start()`.
   */
  attachAutoTlsFromLibp2p(libp2p: {
    addEventListener: (type: string, fn: (ev: Event) => void) => void
    removeEventListener: (type: string, fn: (ev: Event) => void) => void
    services?: { autoTLS?: { certificate?: { key?: string; cert?: string } } }
  }): void {
    if (this.detachAutoTls) {
      this.detachAutoTls()
      this.detachAutoTls = null
    }

    const onCert = (ev: Event) => {
      const detail = (ev as CustomEvent<{ key?: string; cert?: string }>).detail
      if (detail?.key && detail?.cert) {
        this.applyAutoTlsPem({ key: detail.key, cert: detail.cert })
      }
    }

    libp2p.addEventListener('certificate:provision', onCert)
    libp2p.addEventListener('certificate:renew', onCert)

    this.detachAutoTls = () => {
      libp2p.removeEventListener('certificate:provision', onCert)
      libp2p.removeEventListener('certificate:renew', onCert)
    }

    const existing = libp2p.services?.autoTLS?.certificate
    if (existing?.key && existing?.cert) {
      this.applyAutoTlsPem({ key: existing.key, cert: existing.cert })
    }
  }

  /** Serialized application of PEM material (listen or `setSecureContext` on renewal). */
  applyAutoTlsPem(tls: { key: string; cert: string }): void {
    if (!readMetricsHttpsEnabled()) return
    if (process.env.METRICS_DISABLED === 'true' || process.env.METRICS_DISABLED === '1') return

    this.tlsApplyChain = this.tlsApplyChain
      .then(async () => {
        this.ensureDefaultMetrics()
        const desiredPort = readMetricsHttpsPort()

        if (this.tlsServer) {
          this.tlsServer.setSecureContext(tls)
          log('Metrics HTTPS: TLS context updated (renewal)')
          return
        }

        if (this.tlsStartPromise) {
          await this.tlsStartPromise
          const bound = this.tlsServer as https.Server | null
          if (bound != null) {
            bound.setSecureContext(tls)
            log('Metrics HTTPS: TLS context updated after listen')
          }
          return
        }

        const handler = this.createMetricsRequestListener()

        const listen = (p: number): Promise<https.Server | null> =>
          new Promise((resolve) => {
            const server = https.createServer(tls, handler)
            server.on('error', (err: any) => {
              if (err?.code === 'EADDRINUSE' && p !== 0) {
                log(`Metrics HTTPS port ${p} in use; retrying on an ephemeral port`)
                listen(0).then(resolve)
                return
              }
              log('Metrics HTTPS server failed to start:', err?.message || err)
              resolve(null)
            })
            server.listen(p, () => resolve(server))
          })

        this.tlsStartPromise = listen(Number.isFinite(desiredPort) ? desiredPort : 9443).then((srv) => {
          this.tlsServer = srv
          this.tlsStartPromise = null
          if (srv) {
            log('Metrics HTTPS listening (AutoTLS certificate)')
          }
          return srv
        })

        await this.tlsStartPromise
      })
      .catch((e: unknown) => {
        log('Metrics HTTPS error: %s', e instanceof Error ? e.message : String(e))
      })
  }

  private createMetricsRequestListener() {
    const corsConfig = readCorsOriginConfig()
    const pinningHandler = createPinningHttpRequestHandler({
      getLibp2p: () => this.getLibp2p(),
      pinning: this.options.pinning,
      getHelia: this.options.getHelia,
      ipfsGateway: {
        enabled: this.options.pinningHttp?.enabled ?? true,
        fallbackMode: this.options.pinningHttp?.fallbackMode ?? 'pinned-first-network-fallback',
        catTimeoutMs:
          this.options.pinningHttp?.catTimeoutMs ?? Number(process.env.RELAY_IPFS_CAT_TIMEOUT_MS || 120_000),
      },
      cors: {
        origin: corsConfig,
        allowHeaders: (process.env.METRICS_CORS_ALLOW_HEADERS?.trim() || 'Content-Type, Authorization')
          .split(',')
          .map((value) => value.trim())
          .filter(Boolean),
        maxAgeSeconds: Number(process.env.METRICS_CORS_MAX_AGE?.trim() || '86400'),
      },
      getMetricsHttpsInfo: () => {
        const libp2p = this.getLibp2p()
        const tlsListening = Boolean(this.tlsServer?.listening)
        const tlsAddress = this.tlsServer?.address()
        const tlsPort =
          tlsListening && tlsAddress && typeof tlsAddress === 'object' && 'port' in tlsAddress
            ? (tlsAddress as { port: number }).port
            : null
        const zone = autoTlsServingZoneFromPeerId(libp2p?.peerId)
        return {
          autoTlsServingZone: zone,
          ...metricsHttpsPayload(tlsListening, tlsPort, zone),
        }
      },
    })

    return async (req: http.IncomingMessage, res: http.ServerResponse) => {
      try {
        const pathname = (req.url || '/').split('?')[0] || '/'
        if (pathname !== '/metrics') {
          await pinningHandler(req, res)
          if (res.writableEnded || res.destroyed) return
        }

        if (pathname === '/metrics') {
          res.setHeader('Content-Type', client.register.contentType)
          res.end(await this.getMetrics())
          return
        }

        res.statusCode = 404
        res.end('Not found')
      } catch (error: any) {
        log('Metrics request failed: %s', error?.stack || error?.message || String(error))
        if (res.writableEnded || res.destroyed) return
        res.statusCode = 500
        res.setHeader('Content-Type', 'application/json')
        res.end(JSON.stringify({ error: 'Internal server error' }))
      }
    }
  }

  start(port: number | string = process.env.METRICS_PORT || 9090) {
    if (process.env.METRICS_DISABLED === 'true' || process.env.METRICS_DISABLED === '1') {
      log('Metrics server disabled (METRICS_DISABLED)')
      return null
    }

    if (this.server) return Promise.resolve(this.server)
    if (this.startPromise) return this.startPromise

    this.ensureDefaultMetrics()

    const desiredPort = typeof port === 'string' ? Number(port) : port

    const createServer = () => http.createServer(this.createMetricsRequestListener())

    const listen = (p: number): Promise<http.Server | null> =>
      new Promise((resolve) => {
        const server = createServer()
        server.on('error', (err: any) => {
          if (err?.code === 'EADDRINUSE' && p !== 0) {
            log(`Metrics port ${p} in use; retrying on an ephemeral port`)
            listen(0).then(resolve)
            return
          }
          log('Metrics server failed to start:', err?.message || err)
          resolve(null)
        })

        server.listen(p, () => resolve(server))
      })

    this.startPromise = listen(Number.isFinite(desiredPort) ? desiredPort : 9090).then((server) => {
      this.server = server
      this.startPromise = null
      return server
    })

    return this.startPromise
  }

  trackSync(type: string, status = 'success') {
    syncCounter.labels(type, status).inc()
  }

  startSyncTimer(type: string) {
    return syncDurationHistogram.startTimer({ type })
  }

  async stop() {
    if (this.startPromise) {
      await this.startPromise
    }
    if (this.tlsStartPromise) {
      await this.tlsStartPromise
    }

    if (this.detachAutoTls) {
      try {
        this.detachAutoTls()
      } catch {
        // ignore
      }
      this.detachAutoTls = null
    }

    if (this.tlsServer) {
      await new Promise<void>((resolve) => {
        this.tlsServer?.close(() => resolve())
      })
      this.tlsServer = null
    }

    if (!this.server) return

    await new Promise<void>((resolve) => {
      this.server?.close(() => resolve())
    })
    this.server = null
  }
}
