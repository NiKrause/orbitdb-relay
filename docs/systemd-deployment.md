# systemd deployment (Linux relay host)

This guide installs **`orbitdb-relay`** under **systemd** on a Linux VPS or bare-metal host that may also run **Kubo**, without **nginx** (or any other reverse proxy) for libp2p TLS. **Secure WebSockets** come from **AutoTLS** (`@ipshipyard/libp2p-auto-tls`): a certificate for **`<peerId>.libp2p.direct`** via [registration.libp2p.direct](https://registration.libp2p.direct) and Let’s Encrypt DNS-01 (see the upstream walkthrough: [libp2p/js-libp2p-example-auto-tls](https://github.com/libp2p/js-libp2p-example-auto-tls)).

> **From your laptop:** SSH in and run the steps below (or use `deploy/install-on-server.sh` from this repo). Example second relay host: **`ssh root@relay2.seidenwege.com`**. Automated agents in CI sandboxes often **cannot** resolve or reach your private relay hostname.

## Port plan vs Kubo

Default **Kubo** ports to leave alone:

| Service | Typical ports |
|---------|----------------|
| Swarm   | `4001/tcp`, `4001/udp` |
| API     | `5001/tcp` |
| Gateway | `8080/tcp` |

Relay uses a **disjoint** block (change if these collide with other software):

| Role | Port |
|------|------|
| Metrics / `/health` / `/multiaddrs` | `28190/tcp` |
| libp2p TCP | `28191/tcp` |
| libp2p WebSocket | `28192/tcp` |
| WebRTC-direct UDP | `28193/udp` |

## How this repo lines up with js-libp2p-example-auto-tls

The [AutoTLS example](https://github.com/libp2p/js-libp2p-example-auto-tls) expects, in short:

1. A **publicly dialable** socket (or `appendAnnounce` when you know your public IP/ports).
2. **TCP and/or WebSocket** listeners with **Noise** + **Yamux**.
3. **Identify** (and usually **identify push**) plus **keychain** for persistent ACME/account keys.
4. **`autoTLS()`** — on servers, **`autoConfirmAddress: true`** matches the example’s “auto-confirm” path so addresses are trusted without waiting on extra AutoNAT rounds.

`orbitdb-relay` already wires **TCP**, **WebSockets**, **WebRTC**, **noise**, **yamux**, **identify** / **identifyPush**, **keychain**, **autoNAT**, **amino DHT** (`/ipfs/kad/1.0.0`), and **`autoTLS({ autoConfirmAddress: true })`** in `src/config/libp2p.ts`. **Persistent datastore** stores the relay key and libp2p state under `DATASTORE_PATH`.

Operational requirement on your side: **firewall** and **`VITE_APPEND_ANNOUNCE`** must reflect the **same** public IP and **28191–28193** ports so the node’s advertised addresses match what the internet can dial.

## Prerequisites

- **Node.js ≥ 22** ([`engines` in package.json](https://github.com/NiKrause/orbitdb-relay/blob/main/package.json)).
- Outbound HTTPS to Let’s Encrypt and `registration.libp2p.direct`.
- Inbound rules for `28191/tcp`, `28192/tcp`, `28193/udp` (and optionally `28190/tcp` only if you scrape metrics remotely).

On **Ubuntu** without Node, install 22.x before running `install-on-server.sh` (example using [NodeSource](https://github.com/nodesource/distributions)):

```bash
curl -fsSL https://deb.nodesource.com/setup_22.x | bash -
apt-get install -y nodejs
```

## Quick install (script)

As **root** on the server, after copying this repository (or at least `deploy/`):

```bash
bash deploy/install-on-server.sh
```

The script creates the service user, installs the npm package under `/opt/orbitdb-relay`, writes `/etc/default/orbitdb-relay` (attempting to detect public IPv4), installs the systemd unit, and enables the service. **Review** the generated `/etc/default/orbitdb-relay` before relying on production certs.

## Manual install

### 1. System user and directories

```bash
sudo mkdir -p /opt/orbitdb-relay /var/lib/orbitdb-relay
if ! id orbitdb-relay &>/dev/null; then
  sudo useradd --system --home /var/lib/orbitdb-relay --create-home --shell /usr/sbin/nologin orbitdb-relay
fi
sudo chown -R orbitdb-relay:orbitdb-relay /var/lib/orbitdb-relay
```

### 2. Install the package under `/opt`

```bash
sudo tee /opt/orbitdb-relay/package.json >/dev/null <<'EOF'
{
  "private": true,
  "type": "module",
  "dependencies": {
    "orbitdb-relay": "^0.9.1"
  }
}
EOF
sudo chown -R orbitdb-relay:orbitdb-relay /opt/orbitdb-relay
sudo -u orbitdb-relay bash -lc 'cd /opt/orbitdb-relay && npm install --omit=dev'
```

If scoped packages fail to resolve, see [README](../README.md).

### 3. Environment file

```bash
sudo cp deploy/orbitdb-relay.env.example /etc/default/orbitdb-relay
sudo chmod 640 /etc/default/orbitdb-relay
sudo chown root:orbitdb-relay /etc/default/orbitdb-relay
```

Edit `/etc/default/orbitdb-relay`:

1. **`DATASTORE_PATH=/var/lib/orbitdb-relay`**
2. **`VITE_APPEND_ANNOUNCE`** — set to your **public IPv4** and ports **28191–28193**. Example for **`relay2.seidenwege.com`** (or **`relay.seidenwege.com`**) at `203.0.113.7`:

  ```bash
  VITE_APPEND_ANNOUNCE=/ip4/203.0.113.7/tcp/28191,/ip4/203.0.113.7/tcp/28192/ws,/ip4/203.0.113.7/udp/28193/webrtc-direct
  ```

3. Do **not** set `disableAutoTLS` for production WSS via AutoTLS.
4. Optional: `STAGING=true` while testing Let’s Encrypt **staging** (then remove for production).
5. Optional: enable the test/debug libp2p protocols only when you actively need them:

   ```bash
   RELAY_CONNECTIVITY_ECHO_ENABLED=1
   RELAY_CONNECTIVITY_BULK_ENABLED=1
   ```

   Optional bulk tuning:

   ```bash
   RELAY_CONNECTIVITY_BULK_MAX_FRAME_BYTES=262144
   RELAY_CONNECTIVITY_BULK_READ_TIMEOUT_MS=10000
   RELAY_CONNECTIVITY_BULK_IDLE_TIMEOUT_MS=30000
   ```

   Leave them unset for normal production operation; they are disabled by default.

### 4. systemd unit

```bash
sudo cp deploy/orbitdb-relay.service /etc/systemd/system/orbitdb-relay.service
sudo systemctl daemon-reload
sudo systemctl enable --now orbitdb-relay
sudo systemctl status orbitdb-relay
sudo journalctl -u orbitdb-relay -f
```

### 5. Firewall (example: `ufw`)

```bash
sudo ufw allow 28191/tcp comment 'orbitdb-relay tcp'
sudo ufw allow 28192/tcp comment 'orbitdb-relay ws'
sudo ufw allow 28193/udp comment 'orbitdb-relay webrtc-direct'
# optional: sudo ufw allow 28190/tcp comment 'relay metrics'
sudo ufw reload
```

## Verify AutoTLS

1. Logs should show **`Relay PeerId:`** and listener multiaddrs.
2. On the host:

   ```bash
   curl -sS http://127.0.0.1:28190/multiaddrs | jq .
   curl -sS http://127.0.0.1:28190/health
   ```

   After issuance, expect multiaddrs containing **`/tls/ws`** (often with SNI / `*.libp2p.direct` style names as in the [upstream example output](https://github.com/libp2p/js-libp2p-example-auto-tls)). The cert is for **libp2p.direct**, not your vanity hostname (e.g. `relay2.seidenwege.com`).

## Why no nginx-proxy

AutoTLS terminates TLS **inside libp2p** for WSS. Nginx would not replace the **DNS-01** flow that **libp2p.direct** uses; adding another proxy is unnecessary for this stack unless you run **separate** non-libp2p HTTPS apps on the same machine.

## Coexisting with Kubo

- Keep Kubo on its ports (e.g. **4001 / 5001 / 8080**).
- Run the relay only on **28190–28193** (or adjust if you move WS to **80** — see below).
- Data: relay uses **`DATASTORE_PATH`** (Helia + LevelDB), not Kubo’s repository.

## WebSocket listener on port 80

Yes: set **`RELAY_WS_PORT=80`** in `/etc/default/orbitdb-relay`. **AutoTLS** will then request/announce certificates for the WS listener on **80** as well (same `libp2p.direct` flow).

**Important details:**

1. **Privileged ports:** On Linux, ports **below 1024** need **`CAP_NET_BIND_SERVICE`**. The base unit sets **`NoNewPrivileges=true`**; for a **non-root** `User=`, systemd then **ignores** `AmbientCapabilities=`, so binding fails with **`errno=13` (EACCES)** on TCP or UDP (often first on **WebRTC UDP**). Install a drop-in that sets **`NoNewPrivileges=false`** **and** the capability — use **`deploy/orbitdb-relay-low-ports.conf.example`** (or **`orbitdb-relay-ws-port80.conf.example`** for WS-on-80 only):

   ```bash
   sudo mkdir -p /etc/systemd/system/orbitdb-relay.service.d
   sudo cp deploy/orbitdb-relay-low-ports.conf.example \
     /etc/systemd/system/orbitdb-relay.service.d/low-ports.conf
   sudo systemctl daemon-reload
   ```

2. **`VITE_APPEND_ANNOUNCE`:** Use the **same** public IP and **port 80** for the WS multiaddr, e.g.  
   `…/tcp/28191,…/tcp/80/ws,…/udp/28193/webrtc-direct`  
   (replace `28192` with `80` in both `RELAY_WS_PORT` and this line).

3. **Firewall:** Allow **`80/tcp`** instead of (or in addition to) **`28192/tcp`** if you no longer expose WS on 28192.

4. **Conflicts:** Nothing else may listen on **80** (nginx, Apache, another app, or a Kubo gateway if you ever bound it to 80).

5. **Metrics / TCP / WebRTC** can stay on **28190 / 28191 / 28193**; only the **WS** port moves to **80**.

## Troubleshooting: `errno=13` / `UDP socket binding failed`

If logs show **`UDP socket binding failed … errno=13`** or the service **exits right after start**, you are likely on a **privileged port** without effective **`CAP_NET_BIND_SERVICE`**. The usual cause is **`NoNewPrivileges=true`** in the main unit **without** **`NoNewPrivileges=false`** in the drop-in (see **Privileged ports** above). **Alternative:** use ports **≥ 1024** only and remove the low-port drop-in.

## Troubleshooting: `Assertion failed: errors->Empty()` / `CipherJob` / `AESCipherTraits`

If **`node` aborts** with a native stack through **`node::crypto::CipherJob::…::ToResult`** and **`Assertion failed: errors->Empty()`**, that is a **Node.js OpenSSL/WebCrypto thread-pool** failure (not an application `throw`). It can surface under load when many **AES** jobs run in parallel—paths that matter here include **libp2p TLS/WebCrypto** (`@peculiar/webcrypto` via **`@libp2p/webrtc`**, **`@libp2p/tls`**) and **AutoTLS/WSS**.

**Mitigations** (pick one or combine; restart the service after changes):

1. **Serialize libuv’s pool** (strongest dampener for races; default pool size is 4): in `/etc/default/orbitdb-relay` set  
   **`UV_THREADPOOL_SIZE=1`**  
   If **DNS or `fs`** feels slow, try **`8`** or **`16`** instead (trade-off vs. parallelism).

2. **Reduce WebRTC crypto surface:** set **`RELAY_DISABLE_WEBRTC=1`** (TCP + WS + AutoTLS remain; **UDP WebRTC-direct** is off). Remove **`/udp/…/webrtc-direct`** from **`VITE_APPEND_ANNOUNCE`** if you use it.

3. **Node build:** stay on a **supported Node 22.x** from [NodeSource](https://github.com/nodesource/distributions) or [nodejs.org](https://nodejs.org/); after a major OS OpenSSL upgrade, reinstall/restart Node if crashes appear.

4. **Operational:** keep **`Restart=on-failure`** (or **`Restart=always`**) so a rare abort does not leave the relay down for long.

If you can reproduce on a staging host, consider opening an issue with **Node version**, **`node -p process.versions`**, and **journal** excerpt at [nodejs/node](https://github.com/nodejs/node/issues).

## Verbose logs and log files

OrbitDB sync, media CID extraction, and pin steps are controlled by **`src/config/logging.ts`** (see **`AGENTS.md`**). Typical production flags in **`/etc/default/orbitdb-relay`**:

| Variable | Effect |
|----------|--------|
| **`ENABLE_SYNC_LOGS=1`** | Pubsub, subscribe, `syncAllOrbitDBRecords`, open DB, updates, extracted CID counts, `Pinned image CID:` |
| **`ENABLE_SYNC_STATS=1`** | `console.log` blocks with **`DB_SYNC`** and record counts |
| **`ENABLE_GENERAL_LOGS=1`** | **`le-space:relay:*`** (libp2p logger): startup **`Relay PeerId`** / multiaddrs |
| **`LOG_LEVEL_CONNECTION=true`** | **`connection:open`** (per connection) |
| **`LOG_LEVEL_PEER=true`** | **`peer:connect`**; identify failures on **`console.error`** |
| **`LOG_LEVEL_DATABASE=true`** | Failed sync / failed pin on **`console.error`** |

**Log file:** With **`StandardOutput=append:`** / **`StandardError=append:`** (see **`deploy/orbitdb-relay-file-log.conf.example`**), app output goes to e.g. **`/var/log/orbitdb-relay/relay.log`**. Node often **block-buffers** when stdout is not a TTY, so **`tail -f`** can lag until the buffer fills. Optional: **`deploy/orbitdb-relay-line-buffer.conf.example`** wraps **`stdbuf -oL -eL`** around **`node`** (may not suit all native stacks—see comments in that file).

**Follow:**

```bash
sudo tail -f /var/log/orbitdb-relay/relay.log
# optional: still see systemd unit messages
sudo journalctl -u orbitdb-relay -f
```

## Repo artifacts

| File | Purpose |
|------|---------|
| `deploy/orbitdb-relay.service` | systemd unit |
| `deploy/orbitdb-relay.env.example` | env template (`2819x` ports + logging hints) |
| `deploy/orbitdb-relay-low-ports.conf.example` | drop-in for ports **below 1024** (`NoNewPrivileges=false` + bind cap) |
| `deploy/orbitdb-relay-ws-port80.conf.example` | same idea when only **WS** is on **80** |
| `deploy/orbitdb-relay-file-log.conf.example` | append stdout/stderr to **`/var/log/.../relay.log`** |
| `deploy/orbitdb-relay-line-buffer.conf.example` | **`stdbuf -oL -eL`** for line-buffered logs |
| `deploy/install-on-server.sh` | optional one-shot installer (root) |
