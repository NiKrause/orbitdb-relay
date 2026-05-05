import { noise } from '@chainsafe/libp2p-noise'
import { quic } from '@chainsafe/libp2p-quic'
import { yamux } from '@chainsafe/libp2p-yamux'
import { circuitRelayTransport, circuitRelayServer } from '@libp2p/circuit-relay-v2'
import { identify, identifyPush } from '@libp2p/identify'
import { webRTC, webRTCDirect } from '@libp2p/webrtc'
import { webSockets } from '@libp2p/websockets'
import { gossipsub } from '@chainsafe/libp2p-gossipsub'
import { pubsubPeerDiscovery } from '@libp2p/pubsub-peer-discovery'
import { kadDHT, removePrivateAddressesMapper } from '@libp2p/kad-dht'
import { bootstrap } from '@libp2p/bootstrap'
import { tcp } from '@libp2p/tcp'
import { ping } from '@libp2p/ping'
import { autoNAT } from '@libp2p/autonat'
import { dcutr } from '@libp2p/dcutr'
import { autoTLS } from '@ipshipyard/libp2p-auto-tls'
import { keychain } from '@libp2p/keychain'
import { prometheusMetrics } from '@libp2p/prometheus-metrics'
import type { PrivateKey } from '@libp2p/interface'
import type { Datastore } from 'interface-datastore'

import {
  getCircuitRelayDefaultDataLimitBytes,
  getCircuitRelayDefaultDurationLimitMs,
  getCircuitRelayHopTimeoutMs,
  getCircuitRelayMaxReservations,
  getCircuitRelayReservationTtlMs,
} from './circuit-relay-env.js'
import { IPFS_PUBLIC_BOOTSTRAP_LIST } from './ipfs-bootstrap-peers.js'

/** Read env at config build time so tests (and multiple `startRelay` calls) can change ports between runs. */
function readRelayListenEnv() {
  const appendAnnounce =
    (process.env.NODE_ENV === 'development' ? process.env.VITE_APPEND_ANNOUNCE_DEV : process.env.VITE_APPEND_ANNOUNCE) ||
    ''

  const appendAnnounceArray = appendAnnounce
    .split(',')
    .map((addr) => addr.trim())
    .filter(Boolean)

  const tcpPort = Number(process.env.RELAY_TCP_PORT || 9091)
  const wsPort = Number(process.env.RELAY_WS_PORT || 9092)
  const webrtcPort = Number(process.env.RELAY_WEBRTC_PORT || 9093)
  const quicPort = Number(process.env.RELAY_QUIC_PORT || 9094)
  const listenIpv4 = process.env.RELAY_LISTEN_IPV4 || '0.0.0.0'
  const listenIpv6 = process.env.RELAY_LISTEN_IPV6 || '::'
  const disableIpv6 = process.env.RELAY_DISABLE_IPV6 === 'true' || process.env.RELAY_DISABLE_IPV6 === '1'
  const disableWebRtc =
    process.env.RELAY_DISABLE_WEBRTC === 'true' || process.env.RELAY_DISABLE_WEBRTC === '1'
  const disableQuic = process.env.RELAY_DISABLE_QUIC === 'true' || process.env.RELAY_DISABLE_QUIC === '1'
  const disableBootstrap =
    process.env.RELAY_DISABLE_BOOTSTRAP === 'true' || process.env.RELAY_DISABLE_BOOTSTRAP === '1'
  const disableAutoNAT =
    process.env.RELAY_DISABLE_AUTONAT === 'true' || process.env.RELAY_DISABLE_AUTONAT === '1'
  const disableDHT =
    process.env.RELAY_DISABLE_DHT === 'true' || process.env.RELAY_DISABLE_DHT === '1'

  const pubsubTopics = (
    process.env.PUBSUB_TOPICS ||
    process.env.VITE_PUBSUB_TOPICS ||
    'todo._peer-discovery._p2p._pubsub'
  )
    .split(',')
    .map((t) => t.trim())
    .filter(Boolean)

  return {
    appendAnnounceArray,
    tcpPort,
    wsPort,
    webrtcPort,
    quicPort,
    listenIpv4,
    listenIpv6,
    disableIpv6,
    disableWebRtc,
    disableQuic,
    disableBootstrap,
    disableAutoNAT,
    disableDHT,
    pubsubTopics,
  }
}

type ExtraServiceFactories = Record<string, (components: any) => unknown>

export const createLibp2pConfig = (
  privateKey: PrivateKey,
  datastore: Datastore,
  extraServices: ExtraServiceFactories = {},
) => {
  const e = readRelayListenEnv()

  return {
    privateKey,
    datastore,
    metrics: prometheusMetrics(),
    addresses: {
      listen: [
        `/ip4/${e.listenIpv4}/tcp/${e.tcpPort}`,
        `/ip4/${e.listenIpv4}/tcp/${e.wsPort}/ws`,
        ...(!e.disableQuic ? [`/ip4/${e.listenIpv4}/udp/${e.quicPort}/quic-v1`] : []),
        ...(!e.disableWebRtc ? [`/ip4/${e.listenIpv4}/udp/${e.webrtcPort}/webrtc-direct`] : []),
        ...(!e.disableIpv6
          ? [
              `/ip6/${e.listenIpv6}/tcp/${e.tcpPort}`,
              `/ip6/${e.listenIpv6}/tcp/${e.wsPort}/ws`,
              ...(!e.disableQuic ? [`/ip6/${e.listenIpv6}/udp/${e.quicPort}/quic-v1`] : []),
              ...(!e.disableWebRtc ? [`/ip6/${e.listenIpv6}/udp/${e.webrtcPort}/webrtc-direct`] : []),
            ]
          : []),
      ],
      ...(e.appendAnnounceArray.length > 0 && { appendAnnounce: e.appendAnnounceArray }),
    },
    transports: [
      circuitRelayTransport(),
      tcp(),
      ...(!e.disableQuic ? [quic()] : []),
      ...(!e.disableWebRtc ? [webRTC(), webRTCDirect()] : []),
      webSockets(),
    ],
    peerDiscovery: [
      ...(!e.disableBootstrap
        ? [
            bootstrap({
              list: IPFS_PUBLIC_BOOTSTRAP_LIST,
            }),
          ]
        : []),
      pubsubPeerDiscovery({
        interval: 5000,
        topics: e.pubsubTopics,
        listenOnly: false,
        emitSelf: true,
      } as any),
    ],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    services: {
      ping: ping(),
      ...(!e.disableAutoNAT && {
        autonat: autoNAT(),
      }),
      dcutr: dcutr(),
      ...(!e.disableDHT && {
        aminoDHT: kadDHT({
          protocol: '/ipfs/kad/1.0.0',
          peerInfoMapper: removePrivateAddressesMapper,
        }),
      }),
      relay: circuitRelayServer({
        hopTimeout: getCircuitRelayHopTimeoutMs(),
        reservations: {
          maxReservations: getCircuitRelayMaxReservations(),
          reservationTtl: getCircuitRelayReservationTtlMs(),
          defaultDataLimit: getCircuitRelayDefaultDataLimitBytes(),
          defaultDurationLimit: getCircuitRelayDefaultDurationLimitMs(),
        },
      }),
      identify: identify(),
      identifyPush: identifyPush(),
      pubsub: gossipsub({ allowPublishToZeroTopicPeers: true }),
      ...(!process.env.disableAutoTLS && {
        autoTLS: autoTLS({
          autoConfirmAddress: true,
          ...(process.env.STAGING === 'true' && {
            acmeDirectory: 'https://acme-staging-v02.api.letsencrypt.org/directory',
          }),
        }),
      }),
      keychain: keychain(),
      ...extraServices,
    },
    connectionGater: {
      denyDialMultiaddr: async () => false,
    },
  } as any
}
