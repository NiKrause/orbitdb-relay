# Libp2p Service Integration

This package can be used in two ways:

- `startRelay()` for the full relay runtime
- `orbitdbReplicationService()` for mounting the OrbitDB replication + Helia pinning logic inside an existing libp2p node

It also exports:

- `connectivityDebugProtocolsService()` for opt-in `/connectivity-echo/1.0.0` and `/connectivity-bulk/1.0.0`
- `createPinningHttpRequestHandler()` for embedding `/health`, `/multiaddrs`, `/pinning/*`, and `/ipfs/*`
- `PinningHttpServer` as a small HTTP(S) wrapper around that request handler

In the default `startRelay()` runtime, those debug protocols stay disabled unless you either:

- pass `debugProtocols` programmatically, or
- set `RELAY_CONNECTIVITY_ECHO_ENABLED=1` and/or `RELAY_CONNECTIVITY_BULK_ENABLED=1`

## Install

Published package:

```bash
npm install orbitdb-relay
```

If you need this branch before it is published:

```bash
npm install github:NiKrause/orbitdb-relay#feat/libp2p-orbitdb-replication-service
```

Local checkout:

```bash
npm install ../path/to/orbitdb-relay
```

## What Your Project Still Provides

`orbitdbReplicationService()` plugs into your libp2p node, but your project still owns:

- the libp2p transports / muxers / encryption / discovery stack
- the shared `datastore`
- the shared `blockstore`
- starting and stopping the libp2p node

The service itself owns:

- its Helia instance
- its OrbitDB instance
- OrbitDB pubsub sync listeners for `/orbitdb/*`
- media-CID extraction and Helia pinning

## Minimal Example

```ts
import { join } from 'node:path'

import { createLibp2p } from 'libp2p'
import { LevelDatastore } from 'datastore-level'
import { LevelBlockstore } from 'blockstore-level'
import { gossipsub } from '@chainsafe/libp2p-gossipsub'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { identify } from '@libp2p/identify'
import { ping } from '@libp2p/ping'
import { circuitRelayTransport, circuitRelayServer } from '@libp2p/circuit-relay-v2'
import { kadDHT, removePrivateAddressesMapper } from '@libp2p/kad-dht'
import { pubsubPeerDiscovery } from '@libp2p/pubsub-peer-discovery'
import { tcp } from '@libp2p/tcp'
import { webSockets } from '@libp2p/websockets'

import { orbitdbReplicationService } from 'orbitdb-relay'

const storageDir = './orbitdb-replication-node'

const datastore = new LevelDatastore(join(storageDir, 'ipfs', 'data'))
const blockstore = new LevelBlockstore(join(storageDir, 'ipfs', 'blocks'))

await datastore.open()
await blockstore.open()

const libp2p = await createLibp2p({
  datastore,
  addresses: {
    listen: ['/ip4/127.0.0.1/tcp/0', '/ip4/127.0.0.1/tcp/0/ws']
  },
  transports: [circuitRelayTransport(), tcp(), webSockets()],
  connectionEncrypters: [noise()],
  streamMuxers: [yamux()],
  peerDiscovery: [
    pubsubPeerDiscovery({
      interval: 5000,
      topics: ['myapp._peer-discovery._p2p._pubsub'],
      listenOnly: false,
      emitSelf: true
    })
  ],
  services: {
    identify: identify(),
    ping: ping(),
    pubsub: gossipsub({ allowPublishToZeroTopicPeers: true }),
    aminoDHT: kadDHT({
      protocol: '/ipfs/kad/1.0.0',
      peerInfoMapper: removePrivateAddressesMapper
    }),
    relay: circuitRelayServer(),
    orbitdbReplication: orbitdbReplicationService({
      datastore,
      blockstore,
      orbitdbDirectory: join(storageDir, 'orbitdb')
    })
  }
})

await libp2p.services.orbitdbReplication.syncAllOrbitDBRecords('/orbitdb/zdpu...')

const pinning = libp2p.services.orbitdbReplication.createPinningHttpHandlers()
console.log(pinning.getStats())

await libp2p.stop()
await datastore.close()
await blockstore.close()
```

## API

Mounted service:

```ts
libp2p.services.orbitdbReplication
```

Methods:

- `syncAllOrbitDBRecords(dbAddress: string)`
- `createPinningHttpHandlers()`

Additional reusable exports:

- `connectivityDebugProtocolsService(init)`
- `createPinningHttpRequestHandler(options)`
- `PinningHttpServer`

Init options:

- `datastore`: shared datastore used by the host node
- `blockstore`: shared blockstore used by the host node
- `orbitdbDirectory?`: optional directory for OrbitDB metadata

## Requirements

- The service declares a libp2p dependency on `@libp2p/pubsub`, so your node must provide a `pubsub` service.
- In practice you will usually also want `identify`, plus whatever transports and discovery mechanisms your app uses.
- Your node should share the same datastore/blockstore objects with the replication service.

## Lifecycle Notes

- `libp2p.start()` starts the replication service automatically.
- `libp2p.stop()` stops the replication service, its Helia instance, and its OrbitDB instance.
- The service does **not** close your datastore or blockstore; close those after `libp2p.stop()`.

## Troubleshooting

- If `orbitdbReplicationService` is missing from the import, you are probably on an older published version that predates this export.
- If you are testing a branch before release, install from Git or a local checkout instead of the registry.
- If your node does not provide `pubsub`, libp2p will reject the service at startup because of the declared dependency.
