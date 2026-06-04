import http from 'http'
import https from 'https'
import { unixfs } from '@helia/unixfs'
import { CID } from 'multiformats/cid'

import type { PinningHttpHandlers } from '../services/metrics.js'

export type Libp2pLike = {
  peerId?: { toString: () => string; toCID?: () => { bytes: Uint8Array } }
  getMultiaddrs?: () => Array<{ toString: () => string }>
  getConnections?: () => unknown[]
}

type HeliaLike = any

export type PinningHttpFallbackMode = 'pinned-only' | 'pinned-first-network-fallback'

export type PinningHttpRequestHandlerOptions = {
  getLibp2p?: () => Libp2pLike | null
  pinning?: PinningHttpHandlers
  getHelia?: () => HeliaLike | null
  ipfsGateway?: {
    enabled?: boolean
    fallbackMode?: PinningHttpFallbackMode
    catTimeoutMs?: number
  }
  cors?: {
    origin?: '*' | string[]
    allowHeaders?: string[]
    maxAgeSeconds?: number
  }
  getMetricsHttpsInfo?: () => Record<string, unknown> | null
}

export type JsonErrorCode =
  | 'bad_request'
  | 'invalid_json'
  | 'missing_db_address'
  | 'database_not_found'
  | 'sync_failed'
  | 'invalid_cid'
  | 'invalid_path_encoding'
  | 'directory_path_required'
  | 'ipfs_unavailable'
  | 'content_not_found'
  | 'gateway_timeout'
  | 'internal_error'

const MAX_JSON_BODY_BYTES = 16_384
const DEFAULT_CAT_TIMEOUT_MS = 120_000

function pathnameOnly(urlValue: string | undefined): string {
  return (urlValue ?? '/').split('?')[0] || '/'
}

function firstSearchParam(reqUrl: string | undefined, names: string[]): string {
  const url = new URL(reqUrl || '/', 'http://pinning.local')
  for (const name of names) {
    const value = url.searchParams.get(name)
    if (value != null && value.trim() !== '') return value.trim()
  }
  return ''
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

function applyCorsHeaders(req: http.IncomingMessage, res: http.ServerResponse, config: PinningHttpRequestHandlerOptions['cors']) {
  const originConfig = config?.origin ?? '*'
  const requestOrigin = req.headers.origin
  let allowOrigin: string | undefined
  if (originConfig === '*') {
    allowOrigin = '*'
  } else if (requestOrigin && originConfig.includes(requestOrigin)) {
    allowOrigin = requestOrigin
    res.setHeader('Vary', 'Origin')
  }
  if (allowOrigin) {
    res.setHeader('Access-Control-Allow-Origin', allowOrigin)
  }
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, HEAD')
  res.setHeader(
    'Access-Control-Allow-Headers',
    (config?.allowHeaders && config.allowHeaders.length > 0
      ? config.allowHeaders.join(', ')
      : 'Content-Type, Authorization'),
  )
  res.setHeader('Access-Control-Max-Age', String(config?.maxAgeSeconds ?? 86400))
}

function sendJson(res: http.ServerResponse, status: number, body: Record<string, unknown>): void {
  res.statusCode = status
  res.setHeader('Content-Type', 'application/json')
  res.end(JSON.stringify(body))
}

function sendError(res: http.ServerResponse, status: number, code: JsonErrorCode, error: string): void {
  sendJson(res, status, { ok: false, code, error })
}

async function readJsonBody(req: http.IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = []
  let total = 0
  for await (const chunk of req) {
    const buf = chunk as Buffer
    total += buf.length
    if (total > MAX_JSON_BODY_BYTES) {
      throw new Error('body too large')
    }
    chunks.push(buf)
  }
  if (chunks.length === 0) return {}
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8')) as unknown
  } catch {
    const error = new Error('Invalid JSON body')
    ;(error as Error & { code?: string }).code = 'invalid_json'
    throw error
  }
}

type ParsedIpfsRequest =
  | { handled: false }
  | { handled: true; ok: false; status: number; code: JsonErrorCode; error: string }
  | { handled: true; ok: true; cidStr: string; pathWithin?: string }

function parseIpfsRequest(req: Pick<http.IncomingMessage, 'method' | 'url'>): ParsedIpfsRequest {
  if (req.method !== 'GET') return { handled: false }
  const pathname = pathnameOnly(req.url)
  if (!pathname.startsWith('/ipfs/')) return { handled: false }
  let parts: string[]
  try {
    parts = pathname
      .slice('/ipfs/'.length)
      .split('/')
      .filter((part) => part.length > 0)
      .map((part) => decodeURIComponent(part))
  } catch {
    return { handled: true, ok: false, status: 400, code: 'invalid_path_encoding', error: 'Invalid path encoding' }
  }
  if (parts.length === 0) {
    return { handled: true, ok: false, status: 400, code: 'bad_request', error: 'Missing CID' }
  }
  return {
    handled: true,
    ok: true,
    cidStr: parts[0],
    pathWithin: parts.length > 1 ? parts.slice(1).join('/') : undefined,
  }
}

async function streamNetworkFallback(
  res: http.ServerResponse,
  helia: HeliaLike,
  parsed: Extract<ParsedIpfsRequest, { handled: true; ok: true }>,
  timeoutMs: number,
): Promise<void> {
  let cid: CID
  try {
    cid = CID.parse(parsed.cidStr)
  } catch {
    sendError(res, 400, 'invalid_cid', 'Invalid CID')
    return
  }
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(new Error('gateway timeout')), timeoutMs)
  const fsApi = unixfs(helia)
  const opts = {
    signal: controller.signal,
    ...(parsed.pathWithin ? { path: parsed.pathWithin } : {}),
  }
  try {
    const stat = await fsApi.stat(cid, opts)
    if (stat.type === 'directory') {
      sendError(res, 400, 'directory_path_required', 'Directory download is not supported; specify a file path under the CID')
      return
    }
    res.statusCode = 200
    res.setHeader('Content-Type', 'application/octet-stream')
    res.setHeader('Cache-Control', 'private, no-store')
    for await (const chunk of fsApi.cat(cid, opts)) {
      if (!res.write(chunk)) {
        await new Promise<void>((resolve, reject) => {
          res.once('drain', resolve)
          res.once('error', reject)
        })
      }
    }
    res.end()
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    if (message.toLowerCase().includes('abort')) {
      sendError(res, 504, 'gateway_timeout', 'Timed out while fetching CID over libp2p')
      return
    }
    if (
      message.includes('ERR_NOT_FOUND') ||
      message.toLowerCase().includes('not found') ||
      message.toLowerCase().includes('missing block') ||
      message.toLowerCase().includes('no links named')
    ) {
      sendError(res, 404, 'content_not_found', 'Content not found on relay or network')
      return
    }
    if (!res.headersSent) {
      sendError(res, 502, 'internal_error', message)
    } else {
      try {
        res.destroy(error instanceof Error ? error : undefined)
      } catch {
        // ignore
      }
    }
  } finally {
    clearTimeout(timeout)
  }
}

export function isManagedPinningHttpPath(pathname: string): boolean {
  return (
    pathname === '/health' ||
    pathname === '/multiaddrs' ||
    pathname === '/pinning/stats' ||
    pathname === '/pinning/databases' ||
    pathname === '/pinning/sync' ||
    pathname.startsWith('/ipfs/')
  )
}

export function createPinningHttpRequestHandler(options: PinningHttpRequestHandlerOptions = {}) {
  const fallbackMode = options.ipfsGateway?.fallbackMode ?? 'pinned-first-network-fallback'
  const catTimeoutMs = Number.isFinite(options.ipfsGateway?.catTimeoutMs)
    ? Math.max(1, Math.trunc(options.ipfsGateway?.catTimeoutMs as number))
    : DEFAULT_CAT_TIMEOUT_MS

  return async (req: http.IncomingMessage, res: http.ServerResponse): Promise<void> => {
    applyCorsHeaders(req, res, options.cors)

    if (req.method === 'OPTIONS') {
      res.statusCode = 204
      res.end()
      return
    }

    const pathname = pathnameOnly(req.url)
    const pinning = options.pinning

    if (pathname === '/health' && req.method === 'GET') {
      const libp2p = options.getLibp2p?.() ?? null
      const connections = libp2p?.getConnections?.() || []
      const multiaddrs = (libp2p?.getMultiaddrs?.() || []).map((ma) => ma.toString())
      const metricsHttpsInfo = options.getMetricsHttpsInfo?.() ?? null
      sendJson(res, 200, {
        status: 'ok',
        peerId: libp2p?.peerId?.toString?.() || null,
        connections: { active: connections.length },
        multiaddrs: multiaddrs.length,
        autoTlsServingZone:
          metricsHttpsInfo != null && 'autoTlsServingZone' in metricsHttpsInfo
            ? (metricsHttpsInfo.autoTlsServingZone as string | null)
            : null,
        metricsHttps: metricsHttpsInfo,
        timestamp: new Date().toISOString(),
      })
      return
    }

     if (pathname === '/multiaddrs' && req.method === 'GET') {
       const libp2p = options.getLibp2p?.() ?? null
       
       // Check for deployment indicators to determine if we should filter internal addresses
       const isDeployedMode = Boolean(
         process.env.PROXY_HOSTNAME?.trim() ||
         process.env.PUBLIC_IPV4?.trim() ||
         process.env.EXTERNAL_TCP_PORT?.trim() ||
         process.env.EXTERNAL_WS_PORT?.trim() ||
         process.env.EXTERNAL_WEBRTC_PORT?.trim() ||
         process.env.EXTERNAL_QUIC_PORT?.trim()
       )
       
       const allRaw = (libp2p?.getMultiaddrs?.() || []).map((ma) => ma.toString())
       let all: string[]
       
       if (isDeployedMode) {
         // Filter out internal-only addresses in deployed mode
         all = prioritizeAddresses(allRaw.filter(isPublicAddress))
       } else {
         // Keep all addresses in development mode
         all = prioritizeAddresses(allRaw)
       }
       
       const byTransport = {
         webrtc: all.filter((ma) => ma.includes('/webrtc')),
         tcp: all.filter((ma) => ma.includes('/tcp/') && !ma.includes('/ws')),
         websocket: all.filter((ma) => ma.includes('/ws')),
       }
       const metricsHttpsInfo = options.getMetricsHttpsInfo?.() ?? null
       sendJson(res, 200, {
         peerId: libp2p?.peerId?.toString?.() || null,
         all,
         byTransport,
         best: {
           webrtc: byTransport.webrtc[0] || null,
           websocket: byTransport.websocket[0] || null,
           tcp: byTransport.tcp[0] || null,
         },
         autoTlsServingZone:
           metricsHttpsInfo != null && 'autoTlsServingZone' in metricsHttpsInfo
             ? (metricsHttpsInfo.autoTlsServingZone as string | null)
             : null,
         metricsHttps: metricsHttpsInfo,
         timestamp: new Date().toISOString(),
         // Include mode info for debugging
         deployedMode: isDeployedMode,
       })
       return
     }

    if (pinning && pathname === '/pinning/stats' && req.method === 'GET') {
      sendJson(res, 200, pinning.getStats())
      return
    }

    if (pinning && pathname === '/pinning/databases' && req.method === 'GET') {
      const filterRaw = firstSearchParam(req.url, ['address', 'dbAddress'])
      const payload = pinning.getDatabases(filterRaw ? { address: filterRaw } : undefined)
      if (filterRaw && payload.total === 0) {
        sendError(res, 404, 'database_not_found', 'Database address not found in relay sync history')
        return
      }
      sendJson(res, 200, payload)
      return
    }

    if (pinning && pathname === '/pinning/sync' && req.method === 'POST') {
      try {
        const body = (await readJsonBody(req)) as { dbAddress?: string }
        const fromBody = typeof body?.dbAddress === 'string' ? body.dbAddress.trim() : ''
        const fromQuery = firstSearchParam(req.url, ['dbAddress', 'address'])
        const dbAddress = fromBody || fromQuery
        if (!dbAddress) {
          sendError(res, 400, 'missing_db_address', 'Missing or invalid dbAddress')
          return
        }
        const result = await pinning.syncDatabase(dbAddress)
        if (!result.ok) {
          sendError(res, 500, 'sync_failed', result.error || 'sync failed')
          return
        }
        sendJson(res, 200, {
          ok: true,
          dbAddress,
          receivedUpdate: result.receivedUpdate ?? false,
          fallbackScanUsed: result.fallbackScanUsed ?? false,
          extractedMediaCids: result.extractedMediaCids ?? [],
          ...(result.coalesced ? { coalesced: true } : {}),
        })
      } catch (error: any) {
        const code = error?.code === 'invalid_json' ? 'invalid_json' : 'bad_request'
        sendError(res, 400, code, error?.message || String(error))
      }
      return
    }

    const parsed = parseIpfsRequest(req)
    if (parsed.handled) {
      if (!parsed.ok) {
        sendError(res, parsed.status, parsed.code, parsed.error)
        return
      }
      if (options.ipfsGateway?.enabled === false) {
        sendError(res, 404, 'content_not_found', 'IPFS gateway is disabled')
        return
      }
      const pinnedResult =
        pinning?.streamPinnedCid == null ? null : await pinning.streamPinnedCid(parsed.cidStr, parsed.pathWithin)
      if (pinnedResult?.ok) {
        res.statusCode = 200
        res.setHeader('Content-Type', pinnedResult.contentType || 'application/octet-stream')
        res.setHeader('Cache-Control', 'private, no-store')
        for await (const chunk of pinnedResult.chunks) {
          if (!res.write(chunk)) {
            await new Promise<void>((resolve, reject) => {
              res.once('drain', resolve)
              res.once('error', reject)
            })
          }
        }
        res.end()
        return
      }
      if (fallbackMode === 'pinned-only') {
        const status = pinnedResult?.status ?? 404
        const error = pinnedResult?.error ?? 'Content not pinned locally'
        const code: JsonErrorCode =
          status === 503 ? 'ipfs_unavailable' : status === 400 ? 'invalid_cid' : 'content_not_found'
        sendError(res, status, code, error)
        return
      }
      if (pinnedResult != null && pinnedResult.status !== 404) {
        const code: JsonErrorCode =
          pinnedResult.status === 503 ? 'ipfs_unavailable' : pinnedResult.status === 400 ? 'invalid_cid' : 'internal_error'
        sendError(res, pinnedResult.status, code, pinnedResult.error)
        return
      }
      const helia = options.getHelia?.() ?? null
      if (helia == null) {
        sendError(res, 503, 'ipfs_unavailable', 'Helia gateway is not available')
        return
      }
      await streamNetworkFallback(res, helia, parsed, catTimeoutMs)
      return
    }

    res.statusCode = 404
    res.end('Not found')
  }
}

export type PinningHttpServerOptions = PinningHttpRequestHandlerOptions & {
  port?: number
  host?: string
  tls?: { cert: string | Buffer; key: string | Buffer } | null
}

export class PinningHttpServer {
  private readonly options: PinningHttpServerOptions
  private server: http.Server | https.Server | null = null

  constructor(options: PinningHttpServerOptions = {}) {
    this.options = options
  }

  async start(): Promise<http.Server | https.Server> {
    if (this.server != null) return this.server
    const listener = createPinningHttpRequestHandler(this.options)
    this.server = this.options.tls
      ? https.createServer(this.options.tls, listener)
      : http.createServer(listener)
    await new Promise<void>((resolve, reject) => {
      this.server!.once('error', reject)
      this.server!.listen(this.options.port ?? 9090, this.options.host ?? '0.0.0.0', () => resolve())
    })
    return this.server
  }

  async stop(): Promise<void> {
    if (this.server == null) return
    const server = this.server
    this.server = null
    await new Promise<void>((resolve) => {
      server.close(() => resolve())
    })
  }
}
