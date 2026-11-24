/**
 * Limits per client, by the client's own IP.
 *
 * Browsers reach the relay through the Caddy in front of it (relay-button's
 * image), so libp2p and the HTTP server see every one of them as 127.0.0.1.
 * libp2p's own limits per host then count all browsers as one client. Caddy
 * names the client in `X-Forwarded-For`; that header is taken from a loopback
 * peer only, the proxy on the same machine, and from nobody else.
 *
 * Here: a limiter of new attempts per window and of open ones at a time, for
 * the WebSocket upgrades of the libp2p listeners and for the HTTP routes that
 * do work on request, and the release of a circuit relay reservation when its
 * peer disconnects.
 */
import type { IncomingMessage } from 'node:http'
import type { Duplex } from 'node:stream'

/** Whether an address is this machine. */
export function isLoopback(address: string | undefined): boolean {
  const a = String(address ?? '').replace(/^::ffff:/i, '')
  return a === '::1' || /^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(a)
}

/**
 * The client a request comes from: the socket's address, or – from the proxy
 * on this machine – the last address it put into `X-Forwarded-For`, which is
 * the one it saw itself.
 */
export function clientIp(remoteAddress: string | undefined, forwardedFor: string | string[] | undefined): string {
  const own = String(remoteAddress ?? '').replace(/^::ffff:/i, '') || 'unknown'
  if (!isLoopback(own)) return own
  const header = Array.isArray(forwardedFor) ? forwardedFor.join(',') : String(forwardedFor ?? '')
  const last = header
    .split(',')
    .map((part) => part.trim())
    .filter(Boolean)
    .at(-1)
  return last ? last.replace(/^::ffff:/i, '') : own
}

export interface ClientLimits {
  /** New attempts one client may start per window. */
  perWindow: number
  /** The window, in milliseconds. */
  windowMs: number
  /** Attempts one client may have open at a time. */
  maxOpen: number
}

export interface ClientLimiter {
  /** A slot for this client, or null when it is over its limits; `release` frees it once. */
  acquire(ip: string): { release: () => void } | null
  /** Open slots of one client (tests, metrics). */
  open(ip: string): number
}

export function createClientLimiter(limits: ClientLimits, now: () => number = Date.now): ClientLimiter {
  const recent = new Map<string, number[]>()
  const openCount = new Map<string, number>()
  let lastSweep = now()

  const sweep = (t: number) => {
    if (t - lastSweep < limits.windowMs) return
    lastSweep = t
    for (const [ip, times] of recent) {
      const kept = times.filter((at) => t - at < limits.windowMs)
      if (kept.length === 0) recent.delete(ip)
      else recent.set(ip, kept)
    }
  }

  return {
    acquire(ip) {
      const t = now()
      sweep(t)
      const times = (recent.get(ip) ?? []).filter((at) => t - at < limits.windowMs)
      const busy = openCount.get(ip) ?? 0
      if (times.length >= limits.perWindow || busy >= limits.maxOpen) {
        recent.set(ip, times)
        return null
      }
      times.push(t)
      recent.set(ip, times)
      openCount.set(ip, busy + 1)
      let released = false
      return {
        release() {
          if (released) return
          released = true
          const left = (openCount.get(ip) ?? 1) - 1
          if (left <= 0) openCount.delete(ip)
          else openCount.set(ip, left)
        },
      }
    },
    open(ip) {
      return openCount.get(ip) ?? 0
    },
  }
}

function envCount(name: string, fallback: number): number {
  const raw = process.env[name]
  if (raw === undefined || raw === '') return fallback
  const n = Number(raw)
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : fallback
}

/** WebSocket connections per client: 10 new per second, 64 open (env-overridable). */
export function webSocketLimitsFromEnv(): ClientLimits {
  return {
    perWindow: envCount('RELAY_WS_NEW_PER_SECOND_PER_IP', 10),
    windowMs: 1000,
    maxOpen: envCount('RELAY_WS_MAX_OPEN_PER_IP', 64),
  }
}

/** HTTP requests that do work, per client: 20 per 10 s, 4 at a time (env-overridable). */
export function httpLimitsFromEnv(): ClientLimits {
  return {
    perWindow: envCount('RELAY_HTTP_REQUESTS_PER_10S_PER_IP', 20),
    windowMs: 10_000,
    maxOpen: envCount('RELAY_HTTP_MAX_OPEN_PER_IP', 4),
  }
}

/** libp2p's limits per host, which behind the proxy apply to all browsers at once. */
export function connectionManagerLimitsFromEnv(): {
  inboundConnectionThreshold: number
  maxIncomingPendingConnections: number
} {
  return {
    inboundConnectionThreshold: envCount('RELAY_INBOUND_CONNECTION_THRESHOLD', 200),
    maxIncomingPendingConnections: envCount('RELAY_MAX_INCOMING_PENDING_CONNECTIONS', 256),
  }
}

type UpgradeHandler = (req: IncomingMessage, socket: Duplex, head: Buffer, done: (...args: any[]) => void) => void

/**
 * Put the limiter in front of every WebSocket listener of this libp2p node: an
 * upgrade over the client's limits is answered 429 and closed; an admitted one
 * holds its slot until its socket closes. The listeners' WebSocket server is
 * the one place every upgrade passes, plain or TLS, also after AutoTLS
 * replaced the HTTPS server.
 *
 * @returns how many listeners it guards
 */
export function guardWebSocketUpgrades(libp2p: any, limiter: ClientLimiter): number {
  const listeners: any[] = libp2p?.components?.transportManager?.getListeners?.() ?? []
  let guarded = 0
  for (const listener of listeners) {
    const wsServer = listener?.wsServer
    if (!wsServer || typeof wsServer.handleUpgrade !== 'function' || wsServer.__clientLimits) continue
    const handleUpgrade: UpgradeHandler = wsServer.handleUpgrade.bind(wsServer)
    wsServer.handleUpgrade = (req: IncomingMessage, socket: Duplex, head: Buffer, done: (...args: any[]) => void) => {
      const slot = limiter.acquire(clientIp(req.socket?.remoteAddress, req.headers['x-forwarded-for']))
      if (!slot) {
        try {
          socket.end('HTTP/1.1 429 Too Many Requests\r\nConnection: close\r\nContent-Length: 0\r\n\r\n')
        } finally {
          socket.destroy()
        }
        return
      }
      socket.once('close', slot.release)
      handleUpgrade(req, socket, head, done)
    }
    wsServer.__clientLimits = true
    guarded++
  }
  return guarded
}

/**
 * A circuit relay reservation goes when its peer disconnects. The relay server
 * keeps it for its whole TTL otherwise, and a browser that reloads comes back
 * under a new peer id each time.
 *
 * @returns a function that stops listening
 */
export function releaseReservationsOnDisconnect(libp2p: any, serviceName = 'relay'): () => void {
  const onDisconnect = (event: any) => {
    const peer = event?.detail
    const reservations = libp2p?.services?.[serviceName]?.reservations
    if (peer != null && typeof reservations?.delete === 'function') reservations.delete(peer)
  }
  libp2p.addEventListener('peer:disconnect', onDisconnect)
  return () => libp2p.removeEventListener('peer:disconnect', onDisconnect)
}
