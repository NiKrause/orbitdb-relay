# orbitdb-relay

[![Build](https://github.com/NiKrause/orbitdb-relay/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/NiKrause/orbitdb-relay/actions/workflows/ci.yml?query=branch%3Amain)
[![Relay media integration](https://github.com/NiKrause/orbitdb-relay/actions/workflows/relay-media-integration.yml/badge.svg?branch=main)](https://github.com/NiKrause/orbitdb-relay/actions/workflows/relay-media-integration.yml?query=branch%3Amain)

OrbitDB relay used by our apps and tests, including media pinning and sync.

## AI Agents

See `AGENTS.md` for an architecture and feature guide (entrypoints, data flow, env vars, and extension points).

## Docs

- Relay media pinning flow: `docs/relay-media-pinning.md`
- HTTP API (`/health`, `/multiaddrs`, `/pinning/*`, `/ipfs/*`, `/metrics`): `docs/http-api.md`
- Libp2p service integration guide: `docs/libp2p-service.md`

## CLI

Install:

```bash
npm i -g orbitdb-relay
```

Run:

```bash
orbitdb-relay
```

Optional connectivity debug protocols for test tooling are disabled by default in the CLI/runtime. Enable them with env vars before startup:

```bash
RELAY_CONNECTIVITY_ECHO_ENABLED=1 orbitdb-relay
RELAY_CONNECTIVITY_BULK_ENABLED=1 orbitdb-relay
```

Bulk tuning is also available through `RELAY_CONNECTIVITY_BULK_MAX_FRAME_BYTES`, `RELAY_CONNECTIVITY_BULK_READ_TIMEOUT_MS`, and `RELAY_CONNECTIVITY_BULK_IDLE_TIMEOUT_MS`.

Default listener ports:

- TCP: `9091` via `RELAY_TCP_PORT`
- WebSocket: `9092` via `RELAY_WS_PORT`
- WebRTC-direct: `9093` via `RELAY_WEBRTC_PORT`
- QUIC: `9094` via `RELAY_QUIC_PORT`
- Metrics HTTP: `9090` via `METRICS_PORT`
- Metrics HTTPS: `9443` via `METRICS_HTTPS_PORT` when `METRICS_HTTPS_ENABLED=1`

AutoTLS notes:

- AutoTLS is enabled by default. It is only disabled if `disableAutoTLS` is set in the environment.
- The default startup logs do not print much AutoTLS-specific output, so it is normal not to see certificate activity unless you enable the relevant debug namespaces.
- When AutoTLS has provisioned a certificate for the WebSocket listener, the relay should advertise secure WebSocket multiaddrs ending in `/tls/ws`.
- If you use `VITE_APPEND_ANNOUNCE`, keep the public WebSocket address as plain `/ws` with your real public IP and port. Do not manually change it to `/tls/ws`; AutoTLS/domain mapping adds the secure advertised addresses at runtime.
- To verify WSS is live, check `GET /multiaddrs` and look for entries containing `/tls/ws`.

Show AutoTLS logs:

```bash
DEBUG='libp2p:auto-tls,libp2p:auto-tls:*,libp2p:websockets:listener' ENABLE_GENERAL_LOGS=1 orbitdb-relay
```

If you also want the relay to expose the metrics routes over HTTPS once AutoTLS has provisioned a certificate:

```bash
METRICS_HTTPS_ENABLED=1 DEBUG='libp2p:auto-tls,libp2p:auto-tls:*,libp2p:websockets:listener' ENABLE_GENERAL_LOGS=1 orbitdb-relay
```

With those flags enabled, you should see AutoTLS messages such as certificate fetch attempts, reasons it is not fetching yet, and WebSocket HTTPS listener updates.

Test mode (deterministic peer id via `TEST_PRIVATE_KEY` or `RELAY_PRIV_KEY`):

```bash
orbitdb-relay --test
```

## Library

The package still exports `startRelay()` as the compatibility wrapper used by the CLI and the existing tests.

For a fuller install + integration walkthrough, see `docs/libp2p-service.md`.

It also exports reusable building blocks for embedded consumers:

- `orbitdbReplicationService()`
- `connectivityDebugProtocolsService()` for opt-in test/debug echo + bulk protocols
- `createPinningHttpRequestHandler()` and `PinningHttpServer` for `/health`, `/multiaddrs`, `/pinning/*`, and `/ipfs/*`

`orbitdbReplicationService()` mounts the OrbitDB replication + Helia pinning logic directly in any libp2p node:

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

Notes:

- `orbitdbReplicationService()` expects caller-owned `datastore` and `blockstore`.
- Stopping the libp2p node stops the replication service, OrbitDB, and its Helia instance.
- The caller still closes `datastore` and `blockstore` after `libp2p.stop()`.

## Supported Access Controllers

`orbitdb-relay` supports the following Access Controller types when opening OrbitDB databases:

- `orbitdb` (built-in OrbitDB access controller)
- `orbitdb-deferred` (custom deferred OrbitDB ACL registered by this package)
- `todo-delegation` — delegated todo writes (same rules as the app; see below)

Notes:

- Existing databases are opened using the manifest `accessController.type`.
- Creating a new database without explicitly passing an `AccessController` still defaults to `orbitdb`.

### `todo-delegation` package

The delegated todo access controller lives in **`@le-space/orbitdb-access-controller-delegated-todo`** (shared with simple-todo) so pinning uses the same `canAppend` and `verifyDelegationWriterIdentity` behavior as the browser. This relay depends on **`^0.1.0`** of that package.

If `npm install` cannot resolve it yet, install from a local checkout of simple-todo:

```bash
npm install ../path/to/simple-todo/packages/orbitdb-access-controller-delegated-todo
```

## Identity providers (OrbitDB)

The relay registers OrbitDB identity providers so it can verify oplog entries from peers that use passkey-backed identities (same stack as `@le-space/orbitdb-identity-provider-webauthn-did`):

- `publickey` — default from `@orbitdb/core`
- `did` — `@orbitdb/identity-provider-did`
- `webauthn` — worker WebAuthn + keystore (e.g. Ed25519 `did:key` via keystore)
- `webauthn-varsig` — hardware varsig identities (verification uses embedded public key only; no passkey on the server)

## Environment Variables (common)

See **`.env.example`** for a full list including **circuit relay v2** tuning (`RELAY_CIRCUIT_*`).

- `RELAY_TCP_PORT`, `RELAY_WS_PORT`, `RELAY_WEBRTC_PORT`
- `RELAY_DISABLE_WEBRTC=true` to disable UDP `/webrtc-direct` listener in constrained environments
- `METRICS_PORT=0` to bind metrics on an ephemeral port (avoid `EADDRINUSE`)
- `METRICS_CORS_ORIGIN` — CORS for HTTP helpers (`/health`, `/multiaddrs`, `/pinning/*`, …); default `*`; use a comma-separated origin allowlist in production
- `DATASTORE_PATH` or `RELAY_DATASTORE_PATH` to control where LevelDB data is stored
- `PUBSUB_TOPICS` to override pubsub peer discovery topics (default: `todo._peer-discovery._p2p._pubsub`)
- `TEST_PRIVATE_KEY` / `RELAY_PRIV_KEY` for `--test` runs (optional)
- **Circuit relay (v0.4+):** `RELAY_CIRCUIT_HOP_TIMEOUT_MS`, `RELAY_CIRCUIT_MAX_RESERVATIONS`, `RELAY_CIRCUIT_RESERVATION_TTL_MS`, `RELAY_CIRCUIT_DEFAULT_DATA_LIMIT_BYTES`, `RELAY_CIRCUIT_DEFAULT_DURATION_LIMIT_MS` — defaults are set to **10×** the pre-0.4 hardcoded limits (see `src/config/circuit-relay-env.ts`).

## Development

Mocha suites live under **`mocha/`** (not `test/`) so **`node --test`** (Node’s built-in runner) does not auto-load them; those files use Mocha’s `describe` / `it`. Run **`npm test`** for Mocha.

```bash
npm i
npm run build
node dist/cli.js --test
```

## Docker Compose Example

See `docker-compose.example.yml` for a minimal deployment example with:

- persistent datastore volume (PeerId/key survives restarts)
- relay + metrics ports exposed (`9091/tcp`, `9092/tcp`, `9093/udp`, `9090/tcp`)
- WebRTC enabled and AutoTLS left enabled by default
