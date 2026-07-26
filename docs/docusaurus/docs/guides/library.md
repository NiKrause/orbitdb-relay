---
id: library
title: Using as a library
description: Import startRelay or mount orbitdbReplicationService and the pinning HTTP handler in your own code.
---

# Using as a library

`orbitdb-relay` is published to npm and can be used as a library, not just a CLI. There are two
ways to consume it:

- **`startRelay()`** — the full relay runtime (this is what the CLI runs).
- **`orbitdbReplicationService()`** — mount the OrbitDB replication + Helia pinning logic inside an
  existing libp2p node.

```bash
npm install orbitdb-relay
```

## `startRelay()`

`startRelay()` is the compatibility wrapper used by the CLI and the existing tests. It builds the
whole runtime (libp2p, Helia, OrbitDB, event handlers, and the HTTP server) and returns a handle
with a `stop()` method:

```ts
import { startRelay } from 'orbitdb-relay'

const relay = await startRelay({ testMode: false })
// … later …
await relay.stop()
```

It reads the same [environment variables](../reference/environment-variables.md) as the CLI.

## Reusable building blocks

The package also exports building blocks for embedded consumers:

- `orbitdbReplicationService()` — the OrbitDB replication + Helia pinning libp2p service.
- `connectivityDebugProtocolsService()` — opt-in test/debug echo + bulk protocols
  (`/connectivity-echo/1.0.0`, `/connectivity-bulk/1.0.0`).
- `createPinningHttpRequestHandler()` and `PinningHttpServer` — serve `/health`, `/multiaddrs`,
  `/pinning/*`, and `/ipfs/*` from your own HTTP server.

## Mount the replication service

`orbitdbReplicationService()` plugs the OrbitDB replication and Helia pinning logic directly into
any libp2p node that provides a `pubsub` service:

```ts
import { createLibp2p } from 'libp2p'
import { LevelDatastore } from 'datastore-level'
import { LevelBlockstore } from 'blockstore-level'
import { gossipsub } from '@chainsafe/libp2p-gossipsub'
import { identify } from '@libp2p/identify'
import { orbitdbReplicationService } from 'orbitdb-relay'

const datastore = new LevelDatastore('./tmp/ipfs/data')
const blockstore = new LevelBlockstore('./tmp/ipfs/blocks')

await datastore.open()
await blockstore.open()

const libp2p = await createLibp2p({
  datastore,
  services: {
    identify: identify(),
    pubsub: gossipsub(),
    orbitdbReplication: orbitdbReplicationService({
      datastore,
      blockstore,
      orbitdbDirectory: './tmp/orbitdb'
    })
  }
})

await libp2p.services.orbitdbReplication.syncAllOrbitDBRecords('/orbitdb/...')
```

Ownership rules:

- `orbitdbReplicationService()` expects **caller-owned** `datastore` and `blockstore`.
- Stopping the libp2p node stops the replication service, OrbitDB, and its Helia instance.
- The caller still closes `datastore` and `blockstore` **after** `libp2p.stop()`.

For the complete mount walkthrough (transports, requirements, lifecycle, troubleshooting), see
[libp2p integration](./libp2p-integration.md).

## Peer directory note

Since `0.9.7`, subscription changes retain an in-memory database-topic-to-peer hint.
`POST /pinning/sync` may reconnect those known subscribers after opening the database so OrbitDB's
native heads protocol can recover a newer writer head. No application records travel through this
directory — see [Peer recovery](../concepts/peer-recovery.md).
