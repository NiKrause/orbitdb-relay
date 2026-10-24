import { createLibp2p } from 'libp2p'
import { fromString as uint8ArrayFromString } from 'uint8arrays/from-string'
import { privateKeyFromProtobuf } from '@libp2p/crypto/keys'
import { join } from 'node:path'

import { createLibp2pConfig } from './config/libp2p.js'
import { initializeStorage } from './services/storage.js'
import { MetricsServer } from './services/metrics.js'
import { orbitdbReplicationService, type OrbitdbReplicationServiceApi } from './services/orbitdb-replication-service.js'
import { connectivityDebugProtocolsService, type ConnectivityDebugProtocolsServiceInit } from './services/connectivity-debug-protocols-service.js'
import { attestBridgeService } from './services/attest-bridge-service.js'
import { setupEventHandlers } from './events/handlers.js'
import { loggingConfig } from './config/logging.js'
import { headsStreamLog, log } from './utils/logger.js'
import {
  createIdentifyPayloadWarner,
  summariseIdentifyPayload,
} from './services/identify-payload.js'
import { setIdentifyPayloadProvider } from './services/metrics.js'

export type RelayOptions = {
  testMode?: boolean
  storageDir?: string
  debugProtocols?: ConnectivityDebugProtocolsServiceInit
  pinningHttp?: {
    enabled?: boolean
    fallbackMode?: 'pinned-only' | 'pinned-first-network-fallback'
    catTimeoutMs?: number
    syncTimeoutMs?: number
  }
}

export type RelayRuntime = {
  stop: () => Promise<void>
}

function readBooleanEnvVar(name: string): boolean | undefined {
  const raw = process.env[name]?.trim().toLowerCase()
  if (raw == null || raw === '') return undefined
  if (raw === '1' || raw === 'true') return true
  if (raw === '0' || raw === 'false') return false
  return undefined
}

function readNumberEnvVar(name: string): number | undefined {
  const raw = process.env[name]?.trim()
  if (raw == null || raw === '') return undefined
  const parsed = Number(raw)
  return Number.isFinite(parsed) ? parsed : undefined
}

function readDebugProtocolsFromEnv(): ConnectivityDebugProtocolsServiceInit | undefined {
  const echoEnabled = readBooleanEnvVar('RELAY_CONNECTIVITY_ECHO_ENABLED')
  const bulkEnabled = readBooleanEnvVar('RELAY_CONNECTIVITY_BULK_ENABLED')
  const maxFrameBytes = readNumberEnvVar('RELAY_CONNECTIVITY_BULK_MAX_FRAME_BYTES')
  const readTimeoutMs = readNumberEnvVar('RELAY_CONNECTIVITY_BULK_READ_TIMEOUT_MS')
  const idleTimeoutMs = readNumberEnvVar('RELAY_CONNECTIVITY_BULK_IDLE_TIMEOUT_MS')

  const echo =
    echoEnabled === undefined
      ? undefined
      : {
          enabled: echoEnabled,
        }

  const bulk =
    bulkEnabled === undefined &&
    maxFrameBytes === undefined &&
    readTimeoutMs === undefined &&
    idleTimeoutMs === undefined
      ? undefined
      : {
          enabled: bulkEnabled,
          maxFrameBytes,
          readTimeoutMs,
          idleTimeoutMs,
        }

  if (echo == null && bulk == null) return undefined
  return { ...(echo ? { echo } : {}), ...(bulk ? { bulk } : {}) }
}

function mergeDebugProtocolOptions(
  envOptions: ConnectivityDebugProtocolsServiceInit | undefined,
  runtimeOptions: ConnectivityDebugProtocolsServiceInit | undefined,
): ConnectivityDebugProtocolsServiceInit | undefined {
  const merged: ConnectivityDebugProtocolsServiceInit = {
    echo:
      envOptions?.echo || runtimeOptions?.echo
        ? {
            ...(envOptions?.echo ?? {}),
            ...(runtimeOptions?.echo ?? {}),
          }
        : undefined,
    bulk:
      envOptions?.bulk || runtimeOptions?.bulk
        ? {
            ...(envOptions?.bulk ?? {}),
            ...(runtimeOptions?.bulk ?? {}),
          }
        : undefined,
  }

  if (merged.echo == null && merged.bulk == null) return undefined
  return merged
}

function attachOrbitdbHeadsStreamLogging(libp2p: any) {
  if (!libp2p || libp2p.__orbitdbHeadsLoggingAttached) return

  const originalDialProtocol = libp2p.dialProtocol?.bind(libp2p)
  const originalHandle = libp2p.handle?.bind(libp2p)

  if (typeof originalDialProtocol === 'function') {
    libp2p.dialProtocol = async (peer: any, protocol: string, options: any) => {
      const isHeadsProtocol = typeof protocol === 'string' && protocol.startsWith('/orbitdb/heads/')
      const peerId = peer?.toString?.() || peer?.id?.toString?.() || 'unknown'

      if (isHeadsProtocol) {
        headsStreamLog('dial:start %s %s', peerId, protocol)
      }

      try {
        const stream = await originalDialProtocol(peer, protocol, options)
        if (isHeadsProtocol) {
          headsStreamLog('dial:connected %s %s', peerId, protocol)
        }
        return stream
      } catch (error: any) {
        if (isHeadsProtocol) {
          headsStreamLog('dial:error %s %s %s', peerId, protocol, error?.message || String(error))
        }
        throw error
      }
    }
  }

  if (typeof originalHandle === 'function') {
    libp2p.handle = async (protocol: string, handler: any, options: any) => {
      const isHeadsProtocol = typeof protocol === 'string' && protocol.startsWith('/orbitdb/heads/')
      if (!isHeadsProtocol) {
        return await originalHandle(protocol, handler, options)
      }

      headsStreamLog('handle:registered %s', protocol)
      // libp2p 3 calls a handler as `(stream, connection)`.
      const wrappedHandler = async (stream: any, connection: any) => {
        headsStreamLog(
          'handle:received %s %s',
          protocol,
          connection?.remotePeer?.toString?.() || 'unknown'
        )
        return await handler(stream, connection)
      }

      return await originalHandle(protocol, wrappedHandler, options)
    }
  }

  libp2p.__orbitdbHeadsLoggingAttached = true
}

export async function startRelay(opts: RelayOptions = {}): Promise<RelayRuntime> {
  const isTestMode = Boolean(opts.testMode)
  const debugProtocols = mergeDebugProtocolOptions(readDebugProtocolsFromEnv(), opts.debugProtocols)
  const storageDir = opts.storageDir || process.env.DATASTORE_PATH || process.env.RELAY_DATASTORE_PATH || './orbitdb/relay'

  const storage = await initializeStorage(storageDir)
  const { blockstore, datastore } = storage

  let privateKey = storage.privateKey
  const injectedPrivateKeyHex = process.env.RELAY_PRIV_KEY || (isTestMode ? process.env.TEST_PRIVATE_KEY : undefined)
  if (injectedPrivateKeyHex) {
    privateKey = privateKeyFromProtobuf(uint8ArrayFromString(injectedPrivateKeyHex, 'hex'))
  }

  const libp2p = await createLibp2p(
    createLibp2pConfig(privateKey, datastore, {
      orbitdbReplication: orbitdbReplicationService({
        datastore,
        blockstore,
        orbitdbDirectory: join(storageDir, 'orbitdb'),
      }),
      ...(debugProtocols != null
        ? {
            connectivityDebugProtocols: connectivityDebugProtocolsService(debugProtocols),
          }
        : {}),
      // The attest relay as a libp2p protocol, handed to the attest relay
      // beside this one (relay-button's image runs it on 127.0.0.1:8787).
      ...(!['off', 'false', '0'].includes(String(process.env.RELAY_ATTEST_BRIDGE ?? '').toLowerCase())
        ? {
            attestBridge: attestBridgeService({ bridgeUrl: process.env.RELAY_ATTEST_BRIDGE_URL || undefined }),
          }
        : {}),
    }),
  )
  attachOrbitdbHeadsStreamLogging(libp2p as any)
  const orbitdbReplication = (libp2p as any).services.orbitdbReplication as OrbitdbReplicationServiceApi
  const cleanupEventHandlers = await setupEventHandlers(libp2p as any)

  const metricsServer = new MetricsServer({
    getLibp2p: () => libp2p as any,
    pinning: orbitdbReplication.createPinningHttpHandlers(),
    getHelia: () => orbitdbReplication.ipfs ?? null,
    pinningHttp: {
      enabled: opts.pinningHttp?.enabled ?? true,
      fallbackMode: opts.pinningHttp?.fallbackMode ?? 'pinned-first-network-fallback',
      catTimeoutMs: opts.pinningHttp?.catTimeoutMs,
      syncTimeoutMs: opts.pinningHttp?.syncTimeoutMs,
    },
  })
  await metricsServer.start()
  metricsServer.attachAutoTlsFromLibp2p(libp2p as any)

  // Identify is dropped whole once it passes the client's maxMessageSize, so a
  // relay can lose `hop` from every peer's view without anything failing
  // locally (#50). Report the number rather than wait for the symptom.
  const identifySummary = () => summariseIdentifyPayload(libp2p as any)
  setIdentifyPayloadProvider(identifySummary)
  const warnIdentifyPayload = createIdentifyPayloadWarner((message, detail) => {
    console.warn(`⚠️  ${message}`, detail)
  })
  warnIdentifyPayload(identifySummary())
  const identifyPayloadTimer = setInterval(() => {
    try {
      warnIdentifyPayload(identifySummary())
    } catch {
      // observability must never take the relay down
    }
  }, 60_000)
  identifyPayloadTimer.unref?.()

  if (loggingConfig.enableGeneralLogs) {
    log('Relay PeerId: %s', libp2p.peerId.toString())
    log('p2p addr: %o', libp2p.getMultiaddrs().map((ma) => ma.toString()))
  }

  return {
    stop: async () => {
      clearInterval(identifyPayloadTimer)
      setIdentifyPayloadProvider(null)
      try {
        await cleanupEventHandlers?.()
      } catch {
        // ignore
      }
      try {
        await metricsServer.stop()
      } catch {
        // ignore
      }
      try {
        await libp2p.stop()
      } catch {
        // ignore
      }
      try {
        await datastore.close()
      } catch {
        // ignore
      }
      try {
        await blockstore.close()
      } catch {
        // ignore
      }
    },
  }
}
