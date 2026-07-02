import { serviceDependencies } from '@libp2p/interface'
import { logger } from '@libp2p/logger'

const log = logger('le-space:connectivity-debug-protocols')

export const CONNECTIVITY_ECHO_PROTOCOL = '/connectivity-echo/1.0.0'
export const CONNECTIVITY_BULK_PROTOCOL = '/connectivity-bulk/1.0.0'

const DEFAULT_BULK_MAX_FRAME_BYTES = 256 * 1024
const DEFAULT_BULK_READ_TIMEOUT_MS = 10_000
const DEFAULT_BULK_IDLE_TIMEOUT_MS = 30_000

export type ConnectivityDebugProtocolsServiceInit = {
  echo?: {
    enabled?: boolean
  }
  bulk?: {
    enabled?: boolean
    maxFrameBytes?: number
    readTimeoutMs?: number
    idleTimeoutMs?: number
  }
}

type StreamLike = {
  source?: AsyncIterable<unknown>
  sink?: (source: AsyncIterable<Uint8Array>) => Promise<void>
  send?: (buf: Uint8Array) => boolean | void | Promise<boolean | void>
  onDrain?: () => Promise<void>
  sendCloseWrite?: () => Promise<void>
  close: () => Promise<void>
  [Symbol.asyncIterator]?: () => AsyncIterator<unknown>
}

function chunkToUint8Array(chunk: unknown): Uint8Array {
  if (chunk instanceof Uint8Array) return chunk
  if (
    chunk != null &&
    typeof chunk === 'object' &&
    'subarray' in chunk &&
    typeof (chunk as { subarray: () => Uint8Array }).subarray === 'function'
  ) {
    return (chunk as { subarray: () => Uint8Array }).subarray()
  }
  throw new TypeError('Unexpected stream chunk type')
}

class TimeoutError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'TimeoutError'
  }
}

class ByteStreamReader {
  private carry = new Uint8Array(0)
  private readonly iter: AsyncIterator<unknown>
  private lastActivityAt = Date.now()

  constructor(
    private readonly stream: StreamLike,
    private readonly readTimeoutMs: number,
    private readonly idleTimeoutMs: number,
  ) {
    this.iter = getStreamSource(stream)[Symbol.asyncIterator]()
  }

  private async nextChunk(): Promise<void> {
    const elapsedIdle = Date.now() - this.lastActivityAt
    const idleBudget = Math.max(this.idleTimeoutMs - elapsedIdle, 1)
    const timeoutMs = Math.max(Math.min(this.readTimeoutMs, idleBudget), 1)
    const result = await Promise.race([
      this.iter.next(),
      new Promise<IteratorResult<unknown>>((_, reject) => {
        setTimeout(() => {
          if (elapsedIdle >= this.idleTimeoutMs) {
            reject(new TimeoutError(`bulk idle timeout after ${this.idleTimeoutMs}ms`))
          } else {
            reject(new TimeoutError(`bulk read timeout after ${this.readTimeoutMs}ms`))
          }
        }, timeoutMs)
      }),
    ])
    if (result.done) {
      throw new Error('stream ended')
    }
    this.lastActivityAt = Date.now()
    this.carry = new Uint8Array(chunkToUint8Array(result.value))
  }

  async readExactly(n: number): Promise<Uint8Array> {
    const out = new Uint8Array(n)
    let offset = 0
    while (offset < n) {
      if (this.carry.length === 0) {
        await this.nextChunk()
      }
      const use = Math.min(n - offset, this.carry.length)
      out.set(this.carry.subarray(0, use), offset)
      offset += use
      this.carry = this.carry.subarray(use)
      if (use > 0) {
        this.lastActivityAt = Date.now()
      }
    }
    return out
  }
}

function encodeU32be(n: number): Uint8Array {
  const out = new Uint8Array(4)
  new DataView(out.buffer).setUint32(0, n, false)
  return out
}

function decodeU32be(buf: Uint8Array): number {
  return new DataView(buf.buffer, buf.byteOffset, 4).getUint32(0, false)
}

function encodeFrame(payload: Uint8Array): Uint8Array {
  const frame = new Uint8Array(4 + payload.length)
  frame.set(encodeU32be(payload.length), 0)
  frame.set(payload, 4)
  return frame
}

async function readFramedChunk(reader: ByteStreamReader, maxFrameBytes: number): Promise<Uint8Array> {
  const len = decodeU32be(await reader.readExactly(4))
  if (len > maxFrameBytes) {
    throw new Error(`frame length ${len} exceeds max ${maxFrameBytes}`)
  }
  if (len === 0) {
    return new Uint8Array(0)
  }
  return await reader.readExactly(len)
}

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

function getStreamSource(stream: StreamLike): AsyncIterable<unknown> {
  if (stream.source != null) return stream.source
  if (typeof stream[Symbol.asyncIterator] === 'function') return stream as AsyncIterable<unknown>
  throw new TypeError('Stream has no readable source')
}

function getIncomingStream(value: { stream?: StreamLike } | StreamLike): StreamLike {
  return (value as { stream?: StreamLike })?.stream ?? (value as StreamLike)
}

async function writeChunks(stream: StreamLike, source: AsyncIterable<Uint8Array>): Promise<void> {
  if (typeof stream.sink === 'function') {
    await stream.sink(source)
    return
  }
  if (typeof stream.send === 'function') {
    for await (const chunk of source) {
      const canSendMore = await stream.send(chunk)
      if (canSendMore === false && typeof stream.onDrain === 'function') {
        await stream.onDrain()
      }
    }
    if (typeof stream.sendCloseWrite === 'function') {
      await stream.sendCloseWrite()
    }
    return
  }
  throw new TypeError('Stream has no writable sink')
}

async function readLine(stream: StreamLike): Promise<string> {
  let buf = ''
  for await (const chunk of getStreamSource(stream)) {
    buf += textDecoder.decode(chunkToUint8Array(chunk), { stream: true })
    const idx = buf.indexOf('\n')
    if (idx !== -1) {
      return buf.slice(0, idx).trim()
    }
  }
  return buf.trim()
}

async function writeLine(stream: StreamLike, line: string): Promise<void> {
  await writeChunks(
    stream,
    (async function* () {
      yield textEncoder.encode(`${line}\n`)
    })(),
  )
}

type Libp2pLike = {
  handle: (protocols: string | string[], handler: unknown, options?: Record<string, unknown>) => Promise<void>
  unhandle: (protocols: string | string[], options?: Record<string, unknown>) => Promise<void>
}

class ConnectivityDebugProtocolsService {
  readonly [serviceDependencies]: string[] = []
  readonly [Symbol.toStringTag] = '@le-space/connectivity-debug-protocols-service'

  private readonly components: any
  private readonly init: ConnectivityDebugProtocolsServiceInit
  private libp2p: Libp2pLike | null
  private started = false

  constructor(components: any, init: ConnectivityDebugProtocolsServiceInit) {
    this.components = components
    this.init = init
    this.libp2p = null
  }

  async start(): Promise<void> {}

  async afterStart(): Promise<void> {
    if (this.started) return
    const libp2p: Libp2pLike = {
      handle: this.components.registrar.handle.bind(this.components.registrar),
      unhandle: this.components.registrar.unhandle.bind(this.components.registrar),
    }

    if (this.init.echo?.enabled) {
      await libp2p.handle(CONNECTIVITY_ECHO_PROTOCOL, async (incoming: { stream?: StreamLike } | StreamLike) => {
        const stream = getIncomingStream(incoming)
        try {
          const line = await readLine(stream)
          await writeLine(stream, line.length > 0 ? `echo:${line}` : 'echo:(empty)')
        } catch (error: any) {
          log('echo handler error: %s', error?.message || String(error))
        } finally {
          try {
            await stream.close()
          } catch {
            // ignore
          }
        }
      })
    }

    if (this.init.bulk?.enabled) {
      const maxFrameBytes = Number.isFinite(this.init.bulk.maxFrameBytes)
        ? Math.max(1, Math.trunc(this.init.bulk.maxFrameBytes as number))
        : DEFAULT_BULK_MAX_FRAME_BYTES
      const readTimeoutMs = Number.isFinite(this.init.bulk.readTimeoutMs)
        ? Math.max(1, Math.trunc(this.init.bulk.readTimeoutMs as number))
        : DEFAULT_BULK_READ_TIMEOUT_MS
      const idleTimeoutMs = Number.isFinite(this.init.bulk.idleTimeoutMs)
        ? Math.max(1, Math.trunc(this.init.bulk.idleTimeoutMs as number))
        : DEFAULT_BULK_IDLE_TIMEOUT_MS

      await libp2p.handle(CONNECTIVITY_BULK_PROTOCOL, async (incoming: { stream?: StreamLike } | StreamLike) => {
        const stream = getIncomingStream(incoming)
        try {
          const reader = new ByteStreamReader(stream, readTimeoutMs, idleTimeoutMs)
          await writeChunks(
            stream,
            (async function* () {
              for (;;) {
                const payload = await readFramedChunk(reader, maxFrameBytes)
                if (payload.length === 0) return
                yield encodeFrame(payload)
              }
            })(),
          )
        } catch (error: any) {
          log('bulk handler error: %s', error?.message || String(error))
        } finally {
          try {
            await stream.close()
          } catch {
            // ignore
          }
        }
      })
    }

    this.libp2p = libp2p
    this.started = true
  }

  async beforeStop(): Promise<void> {
    if (!this.started || this.libp2p == null) return
    const libp2p = this.libp2p
    this.started = false
    this.libp2p = null
    if (this.init.echo?.enabled) {
      try {
        await libp2p.unhandle(CONNECTIVITY_ECHO_PROTOCOL)
      } catch {
        // ignore
      }
    }
    if (this.init.bulk?.enabled) {
      try {
        await libp2p.unhandle(CONNECTIVITY_BULK_PROTOCOL)
      } catch {
        // ignore
      }
    }
  }

  async stop(): Promise<void> {}
}

export function connectivityDebugProtocolsService(init: ConnectivityDebugProtocolsServiceInit = {}) {
  return (components: any): ConnectivityDebugProtocolsService => new ConnectivityDebugProtocolsService(components, init)
}
