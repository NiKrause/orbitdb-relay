import { logger } from '@libp2p/logger'
import { createHash } from 'node:crypto'
import net from 'node:net'
import WebSocket from 'ws'

const log = logger('le-space:attest-bridge')

/**
 * The attest relay as a libp2p protocol (NiKrause/relay-button#200): a browser
 * that reaches this relay over any libp2p transport (WebSockets with AutoTLS,
 * WebRTC-Direct) opens a stream, names the node and the instance in one line,
 * and its RA-TLS records then go to the guest's attestation port and back.
 *
 * The stream is handed to the attest relay that runs beside the relay in the
 * image (`attest-relay.service`, ws://127.0.0.1:8787/attest), which already
 * resolves the target from Aleph, refuses private addresses and enforces the
 * byte, time and connection limits. This service adds no second copy of any of
 * that: it only frames the stream. Like the WebSocket route it sees TLS
 * records only.
 */
export const ATTEST_PROTOCOL = '/le-space/attest/1.0.0'

const DEFAULT_BRIDGE_URL = 'ws://127.0.0.1:8787/attest'
const HASH = /^[0-9a-f]{64}$/u
const MAX_HEADER_BYTES = 512
const HEADER_TIMEOUT_MS = 10_000

export type AttestBridgeServiceInit = {
  /** The local attest relay's WebSocket route (default ws://127.0.0.1:8787/attest). */
  bridgeUrl?: string
}

type StreamLike = {
  send?: (buf: Uint8Array) => boolean | void | Promise<boolean | void>
  onDrain?: () => Promise<void>
  close: () => Promise<void>
  abort?: (error: Error) => void
  [Symbol.asyncIterator]: () => AsyncIterator<unknown>
}

type IncomingLike = { stream?: StreamLike; connection?: { remoteAddr?: { toString(): string }; remotePeer?: { toString(): string } } } | StreamLike

function chunkBytes(chunk: unknown): Uint8Array {
  if (chunk instanceof Uint8Array) return chunk
  if (chunk != null && typeof chunk === 'object' && 'subarray' in chunk) {
    return (chunk as { subarray: () => Uint8Array }).subarray()
  }
  throw new TypeError('unexpected stream chunk')
}

/** `{"crn":"…","vm":"…"}` naming a node and an instance by their hashes, or null. */
export function parseAttestHeader(line: string): { crn: string; vm: string } | null {
  try {
    const value = JSON.parse(line) as { crn?: unknown; vm?: unknown }
    const crn = String(value?.crn ?? '')
    const vm = String(value?.vm ?? '')
    return HASH.test(crn) && HASH.test(vm) ? { crn, vm } : null
  } catch {
    return null
  }
}

/**
 * The address the local attest relay counts the client by (X-Forwarded-For,
 * which it honours from loopback only): the peer's IP when the connection has
 * a public one, else an address in fd00::/8 derived from the peer id, so
 * browsers behind the relay's own proxy are not all counted as one.
 */
export function clientAddressFor(remoteAddr: string | undefined, remotePeer: string | undefined): string {
  const ip = /^\/ip[46]\/([^/]+)/u.exec(remoteAddr ?? '')?.[1]
  if (ip && net.isIP(ip) && !/^(127\.|::1$|::ffff:127\.)/u.test(ip)) return ip
  const digest = createHash('sha256').update(remotePeer ?? 'unknown').digest('hex')
  return `fd00:${digest.slice(0, 4)}:${digest.slice(4, 8)}:${digest.slice(8, 12)}::1`
}

/** Read up to the first newline: the header line and whatever followed it in the same chunks. */
async function readHeader(iterator: AsyncIterator<unknown>): Promise<{ line: string; rest: Uint8Array }> {
  let buffered = new Uint8Array(0)
  const deadline = Date.now() + HEADER_TIMEOUT_MS
  for (;;) {
    const remaining = deadline - Date.now()
    if (remaining <= 0) throw new Error('no header in time')
    let timer: NodeJS.Timeout | undefined
    const next = await Promise.race([
      iterator.next(),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error('no header in time')), remaining)
      }),
    ]).finally(() => clearTimeout(timer))
    if (next.done) throw new Error('stream ended before the header')
    const bytes = chunkBytes(next.value)
    const joined = new Uint8Array(buffered.length + bytes.length)
    joined.set(buffered, 0)
    joined.set(bytes, buffered.length)
    buffered = joined
    const newline = buffered.indexOf(0x0a)
    if (newline !== -1) {
      return { line: new TextDecoder().decode(buffered.subarray(0, newline)), rest: buffered.subarray(newline + 1) }
    }
    if (buffered.length > MAX_HEADER_BYTES) throw new Error('header too long')
  }
}

class AttestBridgeService {
  private readonly components: any
  private readonly bridgeUrl: string
  private started = false

  constructor(components: any, init: AttestBridgeServiceInit) {
    this.components = components
    this.bridgeUrl = init.bridgeUrl ?? DEFAULT_BRIDGE_URL
  }

  async start(): Promise<void> {}

  async afterStart(): Promise<void> {
    if (this.started) return
    await this.components.registrar.handle(ATTEST_PROTOCOL, (incoming: IncomingLike) => {
      this.bridge(incoming).catch((error: unknown) => log('stream ended: %s', error instanceof Error ? error.message : String(error)))
    })
    this.started = true
  }

  async beforeStop(): Promise<void> {
    if (!this.started) return
    this.started = false
    try {
      await this.components.registrar.unhandle(ATTEST_PROTOCOL)
    } catch {
      // ignore
    }
  }

  async stop(): Promise<void> {}

  private async bridge(incoming: IncomingLike): Promise<void> {
    const stream = ('stream' in incoming && incoming.stream ? incoming.stream : incoming) as StreamLike
    const connection = 'connection' in incoming ? incoming.connection : undefined
    const iterator = stream[Symbol.asyncIterator]()
    let ws: WebSocket | null = null
    let ended = false
    const end = (why: string) => {
      if (ended) return
      ended = true
      log('closing: %s', why)
      if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) ws.close(1000, why.slice(0, 120))
      stream.close().catch(() => stream.abort?.(new Error(why)))
    }

    let header: { line: string; rest: Uint8Array }
    try {
      header = await readHeader(iterator)
    } catch (error) {
      return end(error instanceof Error ? error.message : String(error))
    }
    const target = parseAttestHeader(header.line)
    if (!target) return end('bad header')

    const url = new URL(this.bridgeUrl)
    url.searchParams.set('crn', target.crn)
    url.searchParams.set('vm', target.vm)
    const client = clientAddressFor(connection?.remoteAddr?.toString(), connection?.remotePeer?.toString())
    ws = new WebSocket(url, { headers: { 'x-forwarded-for': client }, maxPayload: 256 * 1024 })
    ws.binaryType = 'nodebuffer'

    // Guest -> browser, with backpressure from the stream.
    let sending = Promise.resolve()
    ws.on('message', (data: Buffer, isBinary: boolean) => {
      if (!isBinary) return end('binary frames only')
      sending = sending.then(async () => {
        if (ended || typeof stream.send !== 'function') return
        if ((await stream.send(new Uint8Array(data))) === false && typeof stream.onDrain === 'function') await stream.onDrain()
      }).catch(() => end('client write failed'))
    })
    ws.on('close', () => end('guest side closed'))
    ws.on('error', (error: Error) => end(`attest relay: ${error.message}`))
    ws.on('unexpected-response', (_request, response) => end(`attest relay answered ${response.statusCode}`))

    await new Promise<void>((resolve) => {
      ws!.once('open', () => resolve())
      ws!.once('close', () => resolve())
    })
    if (ended || ws.readyState !== WebSocket.OPEN) return end('attest relay not reachable')

    // Browser -> guest: what came after the header, then the rest of the stream.
    if (header.rest.length > 0) ws.send(header.rest, { binary: true })
    try {
      for (;;) {
        const next = await iterator.next()
        if (next.done || ended) break
        ws.send(chunkBytes(next.value), { binary: true })
      }
    } catch {
      // the client went away
    }
    end('client closed')
  }
}

export function attestBridgeService(init: AttestBridgeServiceInit = {}) {
  return (components: any): AttestBridgeService => new AttestBridgeService(components, init)
}
