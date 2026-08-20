---
id: environment-variables
title: Environment variables
description: The complete list of environment variables orbitdb-relay reads, with defaults, enumerated from source.
---

# Environment variables

Almost all configuration is done through environment variables (the CLI only takes
[`--test`](./cli.md)). This page enumerates **every** variable the relay reads, grouped by concern,
with defaults taken from the source. See
[`.env.example`](https://github.com/NiKrause/orbitdb-relay/blob/main/.env.example) for a
copy-and-edit template.

Boolean variables are truthy when set to `true` or `1` (unless noted otherwise).

## Listeners & transports

Read in `src/config/libp2p.ts`.

| Variable | Default | Description |
| --- | --- | --- |
| `RELAY_TCP_PORT` | `9091` | libp2p TCP listener port. |
| `RELAY_WS_PORT` | `9092` | libp2p WebSocket listener port (AutoTLS adds the secure `/tls/ws` address at runtime). |
| `RELAY_WEBRTC_PORT` | `9093` | WebRTC-direct UDP listener port. |
| `RELAY_QUIC_PORT` | `9094` | QUIC UDP listener port. |
| `RELAY_LISTEN_IPV4` | `0.0.0.0` | IPv4 bind address for all listeners. |
| `RELAY_LISTEN_IPV6` | `::` | IPv6 bind address for all listeners. |
| `RELAY_DISABLE_IPV6` | `false` | Disable all IPv6 listeners. |
| `RELAY_DISABLE_QUIC` | `false` | Disable the QUIC transport + listener. |
| `RELAY_DISABLE_WEBRTC` | `false` | Disable the WebRTC / WebRTC-direct transports + UDP listener (useful on constrained or crash-prone hosts). |
| `RELAY_DISABLE_BOOTSTRAP` | `false` | Do not dial the public IPFS/Kubo bootstrap peers (isolated networks). |
| `RELAY_DISABLE_AUTONAT` | `false` | Disable the AutoNAT service. |
| `RELAY_DISABLE_DHT` | `false` | Disable the Amino (IPFS) kad-DHT service. |
| `RELAY_MAX_CONNECTIONS` | `1024` | Upper bound on established libp2p connections. |
| `VITE_APPEND_ANNOUNCE` | *(empty)* | Comma-separated public multiaddrs to advertise (behind NAT/VPS). Keep WS as plain `/ws`; AutoTLS adds `/tls/ws`. |
| `VITE_APPEND_ANNOUNCE_DEV` | *(empty)* | Used **instead** of `VITE_APPEND_ANNOUNCE` when `NODE_ENV=development`. |
| `NODE_ENV` | *(unset)* | When `development`, the relay reads `VITE_APPEND_ANNOUNCE_DEV` for announce addresses. |

## TLS

Read in `src/config/libp2p.ts`. **Note the lowercase name** `disableAutoTLS`.

| Variable | Default | Description |
| --- | --- | --- |
| `disableAutoTLS` | *(unset)* | Set to any non-empty value to disable AutoTLS. Leave unset for secure WSS via `libp2p.direct`. |
| `STAGING` | `false` | When `true`, use the Let's Encrypt **staging** ACME directory (for testing; certificates are untrusted). |

## Discovery & identity

Read in `src/config/libp2p.ts`, `src/config/orbitdb-inbound-filter-env.ts`, and `src/relay.ts`.

| Variable | Default | Description |
| --- | --- | --- |
| `PUBSUB_TOPICS` | `todo._peer-discovery._p2p._pubsub` | Comma-separated pubsub peer-discovery topics. |
| `VITE_PUBSUB_TOPICS` | *(unset)* | Fallback source for `PUBSUB_TOPICS` if the latter is unset. |
| `RELAY_REQUIRE_ORBITDB_HEADS_PROTOCOL` | `false` | When true, inbound peers must advertise at least one `/orbitdb/heads/*` protocol (from Identify) or they are disconnected. Outbound dials are unaffected. |
| `RELAY_PRIV_KEY` | *(unset)* | Hex protobuf private key to inject a deterministic PeerId (applies with or without `--test`). |
| `TEST_PRIVATE_KEY` | *(unset)* | Hex protobuf private key used only when `--test` is passed. |

## Storage

Read in `src/relay.ts`.

| Variable | Default | Description |
| --- | --- | --- |
| `DATASTORE_PATH` | `./orbitdb/relay` | Directory for the LevelDB datastore/blockstore, relay key, and libp2p state. OrbitDB metadata lives under `<DATASTORE_PATH>/orbitdb`. |
| `RELAY_DATASTORE_PATH` | *(unset)* | Alias used if `DATASTORE_PATH` is unset. |

## Circuit relay v2

Read in `src/config/circuit-relay-env.ts`. Defaults are **10×** the pre-0.4 hardcoded values, to
support heavier multi-peer / test workloads.

| Variable | Default | Description |
| --- | --- | --- |
| `RELAY_CIRCUIT_HOP_TIMEOUT_MS` | `300000` (5 min) | Max time an incoming relay hop may take. |
| `RELAY_CIRCUIT_MAX_RESERVATIONS` | `10000` | Max simultaneous relay reservations. |
| `RELAY_CIRCUIT_RESERVATION_TTL_MS` | `72000000` (20 h) | How long a reservation stays valid. |
| `RELAY_CIRCUIT_DEFAULT_DATA_LIMIT_BYTES` | `10737418240` (10 GiB) | Max bytes per relayed connection (bigint string). |
| `RELAY_CIRCUIT_DEFAULT_DURATION_LIMIT_MS` | `1200000` (20 min) | Max duration a single relayed circuit may stay open. |

## Metrics & HTTP

Read in `src/services/metrics.ts` and `src/http/pinning-http.ts`. See the
[HTTP API reference](./http-api.md) for the routes these affect.

| Variable | Default | Description |
| --- | --- | --- |
| `METRICS_PORT` | `9090` | HTTP listener for `/health`, `/multiaddrs`, `/metrics`, `/pinning/*`, `/ipfs/*`. Set `0` for an ephemeral port (avoids `EADDRINUSE`). |
| `METRICS_DISABLED` | `false` | When `true`/`1`, the HTTP server is not started. |
| `METRICS_HTTPS_ENABLED` | `false` | When `true`, also serve the same routes over HTTPS using the AutoTLS PEM (requires AutoTLS active and a dialable address). |
| `METRICS_HTTPS_PORT` | `9443` | Port for the HTTPS listener. |
| `METRICS_HTTPS_PUBLIC_HOST` | *(unset)* | Overrides the host used to build the advertised HTTPS example URL. |
| `EXTERNAL_METRICS_HTTPS_PORT` | *(= internal port)* | External port advertised in the `metricsHttps` object / example URL (behind port mapping). |
| `METRICS_CORS_ORIGIN` | `*` | CORS allowlist for the HTTP helper routes. Use a comma-separated list of exact origins in production. |
| `METRICS_CORS_ALLOW_HEADERS` | `Content-Type, Authorization` | Comma-separated `Access-Control-Allow-Headers` value. |
| `METRICS_CORS_MAX_AGE` | `86400` | `Access-Control-Max-Age` (seconds) for CORS preflight. |
| `RELAY_IPFS_CAT_TIMEOUT_MS` | `120000` | Timeout for `/ipfs/<cid>` Helia `cat` reads (overridable per handler). |
| `RELAY_PINNING_SYNC_TIMEOUT_MS` | `120000` | How long `POST /pinning/sync` waits before answering `504 sync_timeout`. The sync keeps running; a later request coalesces onto it. |
| `RELAY_ORBITDB_SYNC_TIMEOUT_MS` | `120000` | How long a pubsub-triggered topic sync may hold one of the sync queue's two slots. |
| `RELAY_ORBITDB_SYNC_COOLDOWN_MS` | `60000` | How long a topic is kept out of the sync queue after its sync timed out or failed. Doubles on each further setback. |
| `RELAY_ORBITDB_SYNC_MAX_COOLDOWN_MS` | `900000` (15 min) | Ceiling for that doubling cooldown. |

### Deployed-mode indicators

The presence of **any** of these switches `/multiaddrs` into **deployed mode**, which filters
internal/private addresses out of the advertised list (`src/http/pinning-http.ts`). `PUBLIC_IPV4` /
`PUBLIC_IPV6` are additionally used to derive the advertised HTTPS host (`src/services/metrics.ts`).

| Variable | Effect |
| --- | --- |
| `PROXY_HOSTNAME` | Deployed-mode indicator. |
| `PUBLIC_IPV4` | Deployed-mode indicator; source for the HTTPS example host. |
| `PUBLIC_IPV6` | Fallback source for the HTTPS example host. |
| `EXTERNAL_TCP_PORT` | Deployed-mode indicator. |
| `EXTERNAL_WS_PORT` | Deployed-mode indicator. |
| `EXTERNAL_WEBRTC_PORT` | Deployed-mode indicator. |
| `EXTERNAL_QUIC_PORT` | Deployed-mode indicator. |

:::note
These `EXTERNAL_*_PORT` variables only *signal* deployed mode; they do not rewrite advertised
ports. To advertise public dial addresses, set `VITE_APPEND_ANNOUNCE`.
:::

## Connectivity debug protocols

Read in `src/relay.ts`. Opt-in libp2p protocols for test tooling; **disabled by default**.

| Variable | Default | Description |
| --- | --- | --- |
| `RELAY_CONNECTIVITY_ECHO_ENABLED` | `false` | Expose `/connectivity-echo/1.0.0` (line echo). |
| `RELAY_CONNECTIVITY_BULK_ENABLED` | `false` | Expose `/connectivity-bulk/1.0.0` (framed bulk round-trip). |
| `RELAY_CONNECTIVITY_BULK_MAX_FRAME_BYTES` | `262144` | Max bulk frame size. |
| `RELAY_CONNECTIVITY_BULK_READ_TIMEOUT_MS` | `10000` | Bulk read timeout. |
| `RELAY_CONNECTIVITY_BULK_IDLE_TIMEOUT_MS` | `30000` | Bulk idle timeout. |

## Runtime

Read in `src/cli.ts`.

| Variable | Default | Description |
| --- | --- | --- |
| `RELAY_STOP_TIMEOUT_MS` | `20000` | Max time the graceful shutdown may take before the process exits anyway. |

## Logging

Read in `src/config/logging.ts` (plus `DEBUG` for the libp2p logger). All are off by default
**except** `ENABLE_HEADS_STREAM_LOGS`, which is **on** unless explicitly set to `false`/`0`.

| Variable | Default | Effect |
| --- | --- | --- |
| `ENABLE_GENERAL_LOGS` | `false` | `le-space:relay:*` logger: startup `Relay PeerId` / multiaddrs. |
| `ENABLE_SYNC_LOGS` | `false` | Pubsub, subscribe, `syncAllOrbitDBRecords`, open DB, updates, extracted CID counts, pinned CIDs. |
| `ENABLE_SYNC_STATS` | `false` | `DB_SYNC` blocks with record counts. |
| `ENABLE_HEADS_STREAM_LOGS` | `true` | `/orbitdb/heads/*` dial/handle stream logging. Set `false`/`0` to silence. |
| `LOG_LEVEL_CONNECTION` | `false` | `connection:open` per connection. |
| `LOG_LEVEL_PEER` | `false` | `peer:connect`; identify failures on `console.error`. |
| `LOG_LEVEL_DATABASE` | `false` | Failed sync / failed pin on `console.error`. |
| `LOG_LEVEL_SYNC` | `false` | Sync-level log detail. |
| `LOG_LEVEL_BLOCK_FETCH` | `false` | Block-fetch log detail. |
| `DEBUG` | *(unset)* | Standard `debug`/libp2p namespaces, e.g. `le-space:relay:*` or `libp2p:auto-tls,libp2p:auto-tls:*`. |

:::note Verification note
`LOG_LEVEL_SYNC` and `LOG_LEVEL_BLOCK_FETCH` are read in `src/config/logging.ts`; the exact
downstream log lines they gate were not exhaustively traced. The other logging flags are described
by the [systemd logging table](../getting-started/systemd.md#verbose-logs-and-log-files).
:::

## Nym VPN presets

If clients reach the relay through Nym VPN, the default ports are blocked by the mixnet exit policy.
Pick allowed ports for `METRICS_PORT`, `METRICS_HTTPS_PORT`, `RELAY_TCP_PORT`, `RELAY_WS_PORT`,
`RELAY_QUIC_PORT`, `RELAY_WEBRTC_PORT`, and set `VITE_APPEND_ANNOUNCE` to match. See
[Ports](./ports.md).
