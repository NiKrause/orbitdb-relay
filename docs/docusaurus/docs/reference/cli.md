---
id: cli
title: CLI reference
description: The orbitdb-relay command, its single flag, signals, and exit behavior.
---

# CLI reference

The package installs a single binary, **`orbitdb-relay`** (mapped to `dist/cli.js`). It starts the
full relay runtime. Almost all configuration is done through
[environment variables](./environment-variables.md); the CLI itself takes just one flag.

## Synopsis

```bash
orbitdb-relay [--test]
```

Run without installing:

```bash
npx -y orbitdb-relay
```

## Flags

| Flag | Description |
| --- | --- |
| `--test` | Test mode. Uses a deterministic peer id from `TEST_PRIVATE_KEY` (or `RELAY_PRIV_KEY`) instead of the persisted key, so a relay comes up with a known PeerId across runs. |

There are no other flags. `--test` is detected by a simple `process.argv` check in `src/cli.ts`;
unknown arguments are ignored.

:::note
`RELAY_PRIV_KEY` injects a deterministic key **with or without** `--test`. `TEST_PRIVATE_KEY` is
only read when `--test` is passed.
:::

## Signals and shutdown

The CLI installs handlers for **`SIGINT`** and **`SIGTERM`** and shuts down gracefully:

- The first signal triggers an orderly stop (event handlers detach, HTTP server closes, libp2p
  stops, datastore/blockstore close).
- A **second** interrupt exits immediately with code `1`.
- Graceful stop is bounded by **`RELAY_STOP_TIMEOUT_MS`** (default **`20000`** ms); if `stop()`
  does not finish in time, the process exits anyway with code `0`.
- An unhandled startup error prints the error and exits with code `1`.

## Logging

Startup is quiet by default. To see the PeerId and multiaddrs:

```bash
ENABLE_GENERAL_LOGS=1 DEBUG='le-space:relay:*' orbitdb-relay
```

The `start` npm script in the package sets `DEBUG=le-space:relay:*` for you. See the
[logging environment variables](./environment-variables.md#logging) for the full matrix (sync logs,
sync stats, per-level connection/peer/database logging).

## AutoTLS visibility

AutoTLS is enabled by default and prints little unless you enable the relevant debug namespaces:

```bash
DEBUG='libp2p:auto-tls,libp2p:auto-tls:*,libp2p:websockets:listener' \
  ENABLE_GENERAL_LOGS=1 orbitdb-relay
```

To also expose the metrics routes over HTTPS once AutoTLS has a certificate, add
`METRICS_HTTPS_ENABLED=1`. See [systemd deployment](../getting-started/systemd.md#verify-autotls) to
verify AutoTLS end to end.

## npm scripts (development)

When working from a checkout of the repository:

| Script | Purpose |
| --- | --- |
| `pnpm run build` | Write the version file, compile TypeScript, add the CLI shebang. |
| `pnpm start` | `DEBUG=le-space:relay:* node dist/cli.js`. |
| `pnpm test` | Build, then run the Mocha suites under `mocha/`. |
| `node dist/cli.js --test` | Run the built CLI in test mode. |

The Mocha suites live under **`mocha/`** (not `test/`) so Node's built-in `node --test` runner does
not auto-load them.
