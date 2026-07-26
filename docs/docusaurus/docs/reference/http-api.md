---
id: http-api
title: HTTP API
description: The metrics server routes — /health, /multiaddrs, /metrics, /pinning/*, and /ipfs/<cid>.
---

# HTTP API (metrics server)

The relay exposes a **plain HTTP** listener on **`METRICS_PORT`** (default **`9090`**). If
**`METRICS_DISABLED=true`**, it is not started.

Optionally, when **`METRICS_HTTPS_ENABLED=true`** and **AutoTLS** has provisioned a certificate
(`@ipshipyard/libp2p-auto-tls`, not disabled via **`disableAutoTLS`**), a second listener serves the
**same routes** over **HTTPS** on **`METRICS_HTTPS_PORT`** (default **`9443`**). The certificate is
the same PEM pair libp2p uses for **WSS**; the TLS hostname must match the AutoTLS wildcard:

- Serving zone: **`<base36(peerId CID bytes)>.libp2p.direct`**
- Valid HTTPS hostnames are one label under that zone, e.g. an IP-derived host like
  **`198-51-100-10.<that-zone>`** (not your vanity VPS hostname).

**Base URL (HTTP):** `http://<host>:<METRICS_PORT>`
**Base URL (HTTPS, when enabled and cert is ready):**
`https://<METRICS_HTTPS_PUBLIC_HOST or fallback host>:<EXTERNAL_METRICS_HTTPS_PORT or METRICS_HTTPS_PORT>`

`GET /health` and `GET /multiaddrs` include **`autoTlsServingZone`** and a **`metricsHttps`** object
(`enabled`, `listening`, `port`, `internalPort`, `externalPort`, `host`, `exampleUrl`,
`internalExampleUrl`) so operators can see the expected URL once TLS is up.

Implementation: `src/http/pinning-http.ts` (shared handler/server), `src/services/metrics.ts`
(metrics + TLS integration), and `DatabaseService.createPinningHttpHandlers()` in
`src/services/database.ts` (pinning handler methods).

**Nym VPN:** mixnet exits only allow specific destination ports. If you use
[Nym's exit policy](https://nymtech.net/.wellknown/network-requester/exit-policy.txt), pick
**`METRICS_PORT`** / **`METRICS_HTTPS_PORT`** from that list (e.g. **8008**, **8443** or **9443**)
and align **`RELAY_*`** / **`VITE_APPEND_ANNOUNCE`** as in [Ports](./ports.md).

The default **`relay`** entrypoint registers pinning handlers, so **`/pinning/*`** and **`/ipfs/*`**
are available. Embedded consumers can reuse the same behavior via
**`createPinningHttpRequestHandler()`** or **`PinningHttpServer`**.

## CORS

All documented routes support **browser cross-origin** access:

- **`OPTIONS *`** → **204** with `Access-Control-Allow-Methods`, `Access-Control-Allow-Headers`,
  `Access-Control-Max-Age`.
- Responses include **`Access-Control-Allow-Origin`**: `*` by default, or the request **`Origin`**
  when it matches **`METRICS_CORS_ORIGIN`** (comma-separated allowlist).

See **`METRICS_CORS_ORIGIN`**, **`METRICS_CORS_ALLOW_HEADERS`**, **`METRICS_CORS_MAX_AGE`** in
[Environment variables](./environment-variables.md#metrics--http).

**CORS preflight example**

```bash
BASE=http://127.0.0.1:9090
curl -sS -i -X OPTIONS "$BASE/pinning/sync" \
  -H "Origin: https://app.example.com" \
  -H "Access-Control-Request-Method: POST" \
  -H "Access-Control-Request-Headers: content-type"
```

Expect **`204`** and `Access-Control-*` headers.

## Endpoints

Use a base URL that matches your relay (replace host/port as needed):

```bash
BASE=http://127.0.0.1:9090
```

### `GET /health`

Liveness / quick status.

**Example**

```bash
curl -sS "$BASE/health"
```

**Response:** `200` `application/json`

```json
{
  "status": "ok",
  "version": "0.10.1",
  "peerId": "12D3KooW…",
  "connections": { "active": 42 },
  "multiaddrs": 13,
  "autoTlsServingZone": "…libp2p.direct",
  "metricsHttps": {
    "enabled": false,
    "listening": false,
    "port": null,
    "exampleUrl": null
  },
  "timestamp": "2026-04-03T12:00:00.000Z"
}
```

`version` is the relay's package version (from `package.json`, embedded at build time), so clients
can display which relay build they are connected to. `multiaddrs` is the **count** of advertised
multiaddrs, not the list (use **`GET /multiaddrs`**, whose response also carries `version`). When
**`METRICS_HTTPS_ENABLED`** is true and a certificate is active, **`metricsHttps.listening`** is
true, **`internalPort`** is the bound port, **`externalPort`** uses `EXTERNAL_METRICS_HTTPS_PORT`
when available, and **`exampleUrl`** uses the resolved AutoTLS hostname.

---

### `GET /multiaddrs`

Full dial address list for operators and clients.

**Example**

```bash
curl -sS "$BASE/multiaddrs"
```

**Response:** `200` `application/json`

- **`all`:** string multiaddrs (public addresses sorted before private).
- **`byTransport`:** `webrtc`, `tcp`, `websocket` subsets.
- **`best`:** first entry per transport (may be `null`).

```json
{
  "peerId": "12D3KooW…",
  "all": ["/ip4/…/tcp/…/p2p/…", "…"],
  "byTransport": {
    "webrtc": ["…"],
    "tcp": ["…"],
    "websocket": ["…"]
  },
  "best": {
    "webrtc": "…",
    "websocket": "…",
    "tcp": "…"
  },
  "autoTlsServingZone": "…libp2p.direct",
  "metricsHttps": {
    "enabled": false,
    "listening": false,
    "port": null,
    "exampleUrl": null
  },
  "timestamp": "2026-04-03T12:00:00.000Z"
}
```

In **deployed mode** (any of `PROXY_HOSTNAME`, `PUBLIC_IPV4`, or an `EXTERNAL_*_PORT` is set),
internal/private addresses are filtered out of `all`; in development mode all addresses are kept.
The response includes a `deployedMode` boolean for debugging.

---

### `GET /metrics`

Prometheus scrape endpoint.

**Response:** `200` text; **`Content-Type`** from `prom-client` (typically
`text/plain; version=0.0.4`).

Includes default process metrics and relay counters such as **`orbitdb_sync_*`**.

**Example**

```bash
curl -sS "$BASE/metrics"
curl -sS "$BASE/metrics" | head -n 20   # first lines only
```

---

### `GET /pinning/stats`

Aggregate pinning / sync counters maintained in memory.

**Example**

```bash
curl -sS "$BASE/pinning/stats"
```

**Response:** `200` `application/json`

```json
{
  "totalPinned": 5,
  "syncOperations": 120,
  "failedSyncs": 2,
  "pinnedMediaCids": ["bafy…", "…"],
  "timestamp": "2026-04-03T12:00:00.000Z"
}
```

---

### `GET /pinning/databases`

Databases the relay has successfully synced at least once (tracked addresses).

**Query (optional, targeted listing)**

- **`address`** or **`dbAddress`** — exact OrbitDB address (same string as in the list). Use URL
  encoding if the value contains characters that are not safe in a query string.

When this parameter is present, the relay returns **only** that entry if it appears in sync history;
otherwise **`404`**.

**Examples**

```bash
# All databases the relay has recorded
curl -sS "$BASE/pinning/databases"

# One database by address (percent-encode if needed)
curl -sS -G "$BASE/pinning/databases" \
  --data-urlencode "address=/orbitdb/zdpuBExampleAddressReplaceMe"
```

**Response:** `200` `application/json`

```json
{
  "databases": [
    { "address": "/orbitdb/…", "lastSyncedAt": "2026-04-03T12:00:00.000Z" }
  ],
  "total": 1
}
```

**Targeted, unknown address:** **`404`** `application/json`

```json
{
  "ok": false,
  "error": "Database address not found in relay sync history"
}
```

---

### `POST /pinning/sync`

Trigger **targeted sync** for one OrbitDB address (same work as pubsub-driven sync: open DB, wait
for updates / fallback scan, enqueue media pins).

Unlike the normal pubsub-triggered path, this explicit recovery path starts a fresh sync and may
reconnect peers previously observed subscribing to the database topic. Reconnection gives OrbitDB's
native heads topology another opportunity to discover a newer writer head; the HTTP API does not
transfer application entries itself. See [Peer recovery](../concepts/peer-recovery.md).

**Address (choose one)**

1. **JSON body** (preferred): `Content-Type: application/json`

```json
{ "dbAddress": "/orbitdb/zdpu…" }
```

2. **Query string:** **`dbAddress`** or **`address`** — useful when you want a POST without a body,
   e.g. `curl -X POST "$BASE/pinning/sync?dbAddress=..."`.

If both are provided, the **JSON `dbAddress` wins** when it is a non-empty string after trim.

**Examples**

```bash
curl -sS -X POST "$BASE/pinning/sync" \
  -H "Content-Type: application/json" \
  -d '{"dbAddress":"/orbitdb/zdpuBExampleAddressReplaceMe"}'

# Same sync, address only in the query string (encode `/` as %2F if your client requires it)
curl -sS -X POST "$BASE/pinning/sync?dbAddress=/orbitdb/zdpuBExampleAddressReplaceMe"
```

**Success:** `200` `application/json`

```json
{
  "ok": true,
  "dbAddress": "/orbitdb/zdpu…",
  "receivedUpdate": true,
  "fallbackScanUsed": false,
  "extractedMediaCids": ["bafy…"],
  "coalesced": true
}
```

- **`receivedUpdate`:** at least one OrbitDB **`update`** event during the sync window.
- **`fallbackScanUsed`:** media CIDs taken from **`db.all()`** because no update arrived in time.
- **`coalesced`:** present only when this call waited on another in-flight sync for the same
  **`dbAddress`**.

**Errors:**

- **`400`** — invalid JSON, or missing **`dbAddress`** in both JSON body and query
  (`dbAddress` / `address`): `{ "ok": false, "error": "…" }`
- **`500`** — sync failed: `{ "ok": false, "error": "…" }`

---

### `GET /ipfs/<cid>` and `GET /ipfs/<cid>/<path…>`

Stream **raw bytes** for content using the configured gateway mode. The default relay uses
**pinned-first with Helia network fallback**:

1. try locally pinned content first,
2. if not pinned locally, fall back to Helia `unixfs.cat`,
3. network fetch is therefore allowed in the default mode.

Embedded consumers can switch the handler to **`pinned-only`** mode when they want strict local-only
behavior.

**Path:**

- The first segment after `/ipfs/` is the **CID** (URL-encoded if needed).
- Further segments are an optional **UnixFS path inside a directory CID** (e.g.
  `/ipfs/bafyDIR/file.jpg`).

**Success:** `200`

- **`Content-Type`:** `application/octet-stream` (current implementation).
- **`Cache-Control`:** `private, no-store`.

**Errors:** JSON body `application/json` where noted.

| Status | Meaning |
| ------ | ------- |
| `400`  | Missing CID, invalid encoding, invalid CID, or directory without a file path (directories cannot be listed as a whole). |
| `404`  | CID not pinned locally, or file/path not available offline. |
| `500`  | Pin check failed. |
| `503`  | IPFS/blockstore not available. |

**Security note:** This endpoint is **unauthenticated** in the default relay. Expose
**`METRICS_PORT`** only on trusted networks, or put an authenticating proxy in front.

**Examples**

```bash
# Raw file or block (CID must be pinned locally on this relay)
curl -sS -f -o ./out.bin "$BASE/ipfs/bafyBEIGReplaceWithRealCid"

# File inside a directory CID (UnixFS path after the CID)
curl -sS -f -o ./photo.jpg "$BASE/ipfs/bafyBEIGDirectoryCid/photos/cat.jpg"
```

`-f` makes curl exit non-zero on HTTP **4xx/5xx** (errors return JSON bodies you can inspect without
`-f`).

---

### Other methods / paths

- **`OPTIONS`** — CORS preflight for any path; **204** (see example under [CORS](#cors)).
- Unknown path — **`404`** plain text `Not found`.

```bash
curl -sS -i "$BASE/no-such-route"
```

## Disabling the server

Set **`METRICS_DISABLED=true`** (or **`1`**). The CLI and library will not listen on
**`METRICS_PORT`**; all routes above are unavailable.

## Related docs

- [Environment variables](./environment-variables.md) (Metrics + CORS).
- [Ports](./ports.md) and [systemd deployment](../getting-started/systemd.md).
