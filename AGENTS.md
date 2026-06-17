# AI Agents Guide: `orbitdb-relay`

This document is written for AI agents (and humans) making changes to this repo. It focuses on how the service works end-to-end, where to look for behavior, and what to be careful about when extending it.

## What This Repo Is

`orbitdb-relay` is a Node.js (ESM) service that:

- Boots a libp2p node configured to act as a circuit relay (relay v2 server) with multiple transports (TCP/WS/WebRTC-direct).
- Creates a Helia instance on top of that libp2p node, backed by LevelDB blockstore/datastore.
- Creates an OrbitDB instance that can verify entries produced by `did:key` identities (via `@orbitdb/identity-provider-did` + `key-did-resolver`).
- Listens to pubsub events, opens relevant OrbitDB databases, and processes update events to extract/pin media CIDs.
- Exposes Prometheus metrics on an HTTP endpoint (`/metrics`).

The primary consumer is the CLI `orbitdb-relay`, but the library export also exposes `startRelay()` for embedding.

## Quick Map (Entry Points)

- CLI entry: `src/cli.ts` (compiled to `dist/cli.js`, published as the package `bin`)
- Library entry: `src/index.ts` (compiled to `dist/index.js`)
- Service bootstrap: `src/relay.ts` (`startRelay()` orchestrates everything)

## Lifecycle Overview

`startRelay()` in `src/relay.ts` does:

1. Resolve storage directory:
   - `opts.storageDir` OR `DATASTORE_PATH` OR `RELAY_DATASTORE_PATH` OR default `./orbitdb/relay`
2. Initialize storage via `initializeStorage()` in `src/services/storage.ts`:
   - Opens a LevelDB datastore at `<storageDir>/ipfs/data`
   - Opens a LevelDB blockstore at `<storageDir>/ipfs/blocks`
   - Loads or creates the relay's libp2p private key, stored in the datastore under key `/le-space/relay/private-key`
3. Determine libp2p identity key:
   - Normally uses the persisted key from the datastore
   - In `--test` mode only: optionally overrides with `TEST_PRIVATE_KEY` or `RELAY_PRIV_KEY` (hex-encoded protobuf bytes)
4. Create libp2p node:
   - `createLibp2p(createLibp2pConfig(privateKey, datastore))` from `src/config/libp2p.ts`
5. Create Helia:
   - `createHelia({ libp2p, datastore, blockstore })`
6. Initialize OrbitDB:
   - `DatabaseService.initialize(ipfs)` registers the DID identity provider + resolver, then `createOrbitDB({ ipfs })`
7. Set up event handlers:
   - `setupEventHandlers(libp2p, databaseService)` hooks libp2p + pubsub events to drive syncing
8. Start metrics server:
   - `MetricsServer.start()` in `src/services/metrics.ts`
9. Print startup markers:
   - Logs `Relay PeerId: ...` and the node’s multiaddrs (used by Playwright in downstream tests)

Shutdown:

- CLI wires `SIGINT`/`SIGTERM` to `runtime.stop()` (best-effort cleanup).
- `stop()` coordinates handler/queue cleanup, OrbitDB stop, metrics server stop, and IPFS/libp2p/storage shutdown.

## Core Features (Where Implemented)

### 1. Relay / libp2p networking

Configured in `src/config/libp2p.ts`:

- Listeners:
  - `/tcp` (default `9091`)
  - `/ws` (default `9092`)
  - `/webrtc-direct` over UDP (default `9093`, can be disabled)
  - Optional IPv6 listeners (can be disabled)
- Services:
  - Circuit relay v2 server (`@libp2p/circuit-relay-v2`) with reservation limits (env-tunable via `RELAY_CIRCUIT_*`; defaults in `src/config/circuit-relay-env.ts`, see `.env.example`)
  - Pubsub: gossipsub (`@chainsafe/libp2p-gossipsub`)
  - Peer discovery: public IPFS/libp2p bootstrap nodes (`@libp2p/bootstrap`, same list as Helia/Kubo; disable with `RELAY_DISABLE_BOOTSTRAP=1`), plus pubsub-based (`@libp2p/pubsub-peer-discovery`)
  - Identify / identifyPush, ping, AutoNAT
  - Optional AutoTLS (`@ipshipyard/libp2p-auto-tls`) unless `disableAutoTLS` is set
  - DHT (`@libp2p/kad-dht`) registered as `aminoDHT` using protocol `/ipfs/kad/1.0.0`
  - Prometheus metrics for libp2p (`@libp2p/prometheus-metrics`)

Announcement:

- `appendAnnounce` multiaddrs can be injected via `VITE_APPEND_ANNOUNCE` (or `VITE_APPEND_ANNOUNCE_DEV` in development).

### 2. Helia-backed persistence

`src/services/storage.ts`:

- Uses `datastore-level` and `blockstore-level`.
- Default on-disk layout under the storage directory:
  - `ipfs/data` (datastore)
  - `ipfs/blocks` (blockstore)

Important: The relay's peer identity private key is persisted in the datastore, so the PeerId remains stable across restarts (unless you delete the datastore or override key in test mode).

### 3. OrbitDB initialization with `did:key` verification

`src/services/database.ts`:

- Sets DID resolver: `OrbitDBIdentityProviderDID.setDIDResolver(KeyDIDResolver.getResolver())`
- Registers identity provider: `useIdentityProvider(OrbitDBIdentityProviderDID)`
- Creates OrbitDB instance: `createOrbitDB({ ipfs })`

This is the primary "why this exists" feature: enabling OrbitDB to verify entries created by `did:key` identities.

### 4. Sync / "pinning" behavior

Detailed reference:

- `docs/relay-media-pinning.md` (full flow, extraction rules, queueing, shutdown, and test scenario)

Event wiring in `src/events/handlers.ts`:

- On pubsub `message` events:
  - If `msg.topic` starts with `/orbitdb/`, enqueue a sync task (concurrency 2).
- On pubsub `subscription-change` events:
  - For each subscription where `subscription.topic` starts with `/orbitdb/`, enqueue a sync task.

Sync implementation in `DatabaseService.syncAllOrbitDBRecords(dbAddress: string)`:

- Opens the OrbitDB database by `dbAddress`.
- Waits up to ~5 seconds for an OrbitDB `update` event.
- Extracts media CIDs from update entry payloads (eg `imageCid`, `profilePicture*`, `mediaId`, `mediaIds`) and enqueues pinning tasks.
- Pinning uses Helia `pins.add()` through a non-blocking queue (concurrency 4) with dedupe sets for queued/already-pinned CIDs.
- Closes the DB handle after sync.
- Tracks metrics `orbitdb_sync_total` and `orbitdb_sync_duration_seconds`.

Important caveat for agents:

- The handlers pass `msg.topic` / `subscription.topic` directly to `orbitdb.open()`.
- This assumes the pubsub topic string is a valid OrbitDB address (or that OrbitDB can open it as-is). If you change pubsub topic conventions, you likely need a mapping layer.

### 5. Prometheus metrics endpoint

Human-facing route reference: **`docs/http-api.md`**.

`src/services/metrics.ts`:

- HTTP server exposes:
  - `GET /metrics` for Prometheus scraping
  - `GET /health`, `GET /multiaddrs`, pinning JSON routes (`/pinning/*`), and `GET /ipfs/<cid>` (pinned-local bytes only; see `streamPinnedCid` in `DatabaseService`)
- When `METRICS_HTTPS_ENABLED` is set and AutoTLS has PEM material, the same routes are also served over HTTPS on `METRICS_HTTPS_PORT` (see `docs/http-api.md`).
- **CORS:** all of the above respond with `Access-Control-Allow-*` so browser `fetch` from another origin works. Default allow-all: `METRICS_CORS_ORIGIN=*` (unset). Use a comma-separated allowlist of exact origins in production. `OPTIONS` preflight returns `204`.
- Defaults to port `9090`, but handles `EADDRINUSE` by retrying on an ephemeral port if the requested port is not `0`.
- Uses a singleton instance so creating `MetricsServer` multiple times does not double-register metrics.
- Supports explicit `stop()` to close the HTTP server on relay shutdown.

## Configuration (Environment Variables)

### Runtime / CLI

- `DATASTORE_PATH` / `RELAY_DATASTORE_PATH`: storage directory root
- `TEST_PRIVATE_KEY` / `RELAY_PRIV_KEY`: hex-encoded protobuf private key bytes (only used when running with `--test`)

### libp2p network

- `RELAY_TCP_PORT` (default `9091`)
- `RELAY_WS_PORT` (default `9092`)
- `RELAY_WEBRTC_PORT` (default `9093`)
- `RELAY_LISTEN_IPV4` (default `0.0.0.0`)
- `RELAY_LISTEN_IPV6` (default `::`)
- `RELAY_DISABLE_IPV6=true|1`
- `RELAY_DISABLE_WEBRTC=true|1`
- `RELAY_DISABLE_BOOTSTRAP=true|1`: skip default public bootstrap peers (tests use this; isolated deployments only)
- `PUBSUB_TOPICS` or `VITE_PUBSUB_TOPICS`: comma-separated pubsub peer discovery topics
- `VITE_APPEND_ANNOUNCE` / `VITE_APPEND_ANNOUNCE_DEV`: comma-separated multiaddrs to append to announce set
- `disableAutoTLS` (truthy): disables AutoTLS service creation
- `STAGING=true`: makes AutoTLS use Let’s Encrypt staging directory

### Metrics

- `METRICS_PORT` (default `9090`, `0` for ephemeral)
- `METRICS_DISABLED=true|1`
- `METRICS_HTTPS_ENABLED=true|1`: when set, after AutoTLS provisions a cert, also listen for HTTPS on `METRICS_HTTPS_PORT` with the same routes; clients must use a hostname under `*.<base36PeerId>.libp2p.direct` (see `GET /health` / `docs/http-api.md`).
- `METRICS_HTTPS_PORT` (default `9443`)
- Nym VPN users: choose ports from [Nym exit policy](https://nymtech.net/.wellknown/network-requester/exit-policy.txt); see **`docs/nym-vpn-ports.md`**
- `METRICS_CORS_ORIGIN`: `*` (default) or comma-separated allowlist, e.g. `https://app.example.com,http://localhost:5173`
- `METRICS_CORS_ALLOW_HEADERS` (optional): defaults to `Content-Type, Authorization`
- `METRICS_CORS_MAX_AGE` (optional): preflight cache seconds, default `86400`

### Logging

`src/config/logging.ts` controls both toggles and per-area log levels:

- `ENABLE_GENERAL_LOGS=false|0`
- `ENABLE_SYNC_LOGS=false|0`
- `ENABLE_SYNC_STATS=false|0`
- `LOG_LEVEL_CONNECTION=true`
- `LOG_LEVEL_PEER=true`
- `LOG_LEVEL_DATABASE=true`
- `LOG_LEVEL_SYNC=true`

Note: `src/utils/logger.ts` uses `@libp2p/logger` namespaces under `le-space:relay:*`.

## Build / Dist

- TypeScript build: `npm run build` runs `tsc` and then `scripts/add-shebang.mjs` to make the CLI executable.
- Published artifacts:
  - `dist/` (compiled JS + d.ts)
  - `README.md`, `LICENSE`
- Node requirement: `>=22` (see `package.json`).
- Module system: ESM (`"type": "module"`).

## Common Changes Agents Make (With Pointers)

### Add a new CLI flag

- `src/cli.ts` currently only checks for `--test`.
- Add parsing here (keep it simple, or introduce a parser library if you need options/values).
- Thread the option into `startRelay({ ... })` and/or environment resolution in `src/relay.ts`.

### Change syncing behavior

- If you need "real pinning" semantics (ensuring blocks are present), you will likely need to:
  - Decide which OrbitDB store types you support and how to traverse entries deterministically.
  - Move away from `db.all()` only, and use OrbitDB store APIs or underlying IPFS/Helia block fetching.
- Current implementation is in `src/services/database.ts` and triggered from `src/events/handlers.ts`.

### Adjust transport / listen addresses

- `src/config/libp2p.ts` is the single place to change listens/transports/services.
- Be careful changing defaults: downstream apps/tests may assume the current ports and the startup log markers.

### Extend metrics

- `src/services/metrics.ts` is a good place for new counters/histograms.
- Keep the singleton behavior (avoid double-registration in `prom-client`).

## Sharp Edges / Assumptions

- Pubsub topic strings are treated as OrbitDB addresses (see caveat above).
- `--test` key override expects a *hex string of protobuf bytes*, not a raw seed or base64.
- Sync waits for update events before enqueueing pin jobs; if no update is observed during timeout, no new media pin enqueue occurs in that sync run.
- Shutdown now attempts coordinated queue/listener teardown, metrics server close, OrbitDB stop, and IPFS/libp2p stop; ordering still matters when extending lifecycle code.
- Under heavy load, **Node** can rarely **abort** in **`CipherJob` / `errors->Empty()`** (OpenSSL/WebCrypto thread pool), often involving **`@peculiar/webcrypto`** (libp2p TLS/WebRTC). Mitigations: **`UV_THREADPOOL_SIZE`** (e.g. `1` to serialize), **`RELAY_DISABLE_WEBRTC`**, or a different Node patch build — see **`docs/systemd-deployment.md`** (Troubleshooting).

## Files Worth Reading First

- `src/relay.ts`
- `src/config/libp2p.ts`
- `src/events/handlers.ts`
- `src/services/database.ts`
- `src/services/storage.ts`
- `src/services/metrics.ts`
