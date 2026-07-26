---
id: quickstart
title: Quickstart (CLI)
description: Get an orbitdb-relay running from the CLI in about two minutes.
---

# Quickstart (CLI)

Get a relay running from the command line in about two minutes. You need **Node.js ≥ 22**
(`engines.node` in the package requires it).

## 1. Run the relay

The fastest way, without a global install:

```bash
npx -y orbitdb-relay
```

Or install it globally and run the `orbitdb-relay` binary:

```bash
npm i -g orbitdb-relay
orbitdb-relay
```

That is the whole command — there are no required flags. On startup the relay:

- generates (or loads) a persistent peer key under `DATASTORE_PATH` (default `./orbitdb/relay`),
- binds its libp2p listeners and the HTTP metrics server,
- enables AutoTLS, circuit relay v2, pubsub peer discovery, and the OrbitDB replication service.

:::tip See what it is doing
The default logs are quiet. For a talkative startup that prints the PeerId and multiaddrs:

```bash
ENABLE_GENERAL_LOGS=1 DEBUG='le-space:relay:*' npx -y orbitdb-relay
```
:::

## 2. Default ports

| Role | Default port | Env var |
| --- | --- | --- |
| libp2p TCP | `9091` | `RELAY_TCP_PORT` |
| libp2p WebSocket | `9092` | `RELAY_WS_PORT` |
| libp2p WebRTC-direct (UDP) | `9093` | `RELAY_WEBRTC_PORT` |
| libp2p QUIC (UDP) | `9094` | `RELAY_QUIC_PORT` |
| Metrics / HTTP API | `9090` | `METRICS_PORT` |
| Metrics HTTPS (opt-in) | `9443` | `METRICS_HTTPS_PORT` |

See [Ports](../reference/ports.md) for the full list and Nym VPN alternatives, and
[Environment variables](../reference/environment-variables.md) for everything you can tune.

## 3. Verify it is up

In another terminal, hit the built-in HTTP API on `METRICS_PORT`:

```bash
curl -sS http://127.0.0.1:9090/health
# {"status":"ok","version":"…","peerId":"12D3KooW…","connections":{"active":0},"multiaddrs":13,…}

curl -sS http://127.0.0.1:9090/multiaddrs
# full dial address list, grouped by transport
```

If `/health` returns `status: ok` and `/multiaddrs` lists addresses, the relay is running. See the
[HTTP API reference](../reference/http-api.md) for every route.

## 4. Test mode (deterministic PeerId)

For tests and local development you often want a stable PeerId across restarts. Pass `--test` and
supply a private key via `TEST_PRIVATE_KEY` (or `RELAY_PRIV_KEY`, which also applies without
`--test`):

```bash
TEST_PRIVATE_KEY=<hex-protobuf-private-key> orbitdb-relay --test
```

`--test` is the only CLI flag. Everything else is configured through
[environment variables](../reference/environment-variables.md).

## 5. Optional: connectivity debug protocols

Two opt-in libp2p protocols exist for test tooling and are **disabled by default**. Enable them
before startup:

```bash
RELAY_CONNECTIVITY_ECHO_ENABLED=1 orbitdb-relay   # /connectivity-echo/1.0.0 (line echo)
RELAY_CONNECTIVITY_BULK_ENABLED=1 orbitdb-relay   # /connectivity-bulk/1.0.0 (framed bulk round-trip)
```

Bulk tuning: `RELAY_CONNECTIVITY_BULK_MAX_FRAME_BYTES`, `RELAY_CONNECTIVITY_BULK_READ_TIMEOUT_MS`,
`RELAY_CONNECTIVITY_BULK_IDLE_TIMEOUT_MS`.

## Where to go next

- **Deploy it properly:** [Docker Compose](./docker-compose.md) or [systemd](./systemd.md).
- **Understand it:** [Architecture overview](../concepts/architecture.md).
- **Embed it:** [Using as a library](../guides/library.md).
