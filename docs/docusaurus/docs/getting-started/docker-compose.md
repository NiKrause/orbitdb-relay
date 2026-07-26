---
id: docker-compose
title: Docker Compose
description: Run orbitdb-relay as a container with a persistent datastore volume.
---

# Docker Compose

A minimal Compose deployment runs the published npm package on a plain Node runtime image, with a
persistent datastore volume so the PeerId/key survives restarts.

The repository ships [`docker-compose.example.yml`](https://github.com/NiKrause/orbitdb-relay/blob/main/docker-compose.example.yml).
Copy it to `docker-compose.yml` and adjust:

```yaml
services:
  orbitdb-relay:
    # Plain Node runtime that runs the published npm package via npx.
    # If you publish a dedicated image for this service, replace image + command.
    image: node:22-bookworm-slim
    container_name: orbitdb-relay
    restart: unless-stopped
    command: >
      sh -lc "npx -y orbitdb-relay@latest"
    environment:
      DATASTORE_PATH: /data
      METRICS_PORT: "9090"
      RELAY_TCP_PORT: "9091"
      RELAY_WS_PORT: "9092"
      RELAY_WEBRTC_PORT: "9093"
      RELAY_DISABLE_WEBRTC: "0"
      RELAY_DISABLE_IPV6: "0"
      # AutoTLS stays enabled unless disableAutoTLS is set.
      # Optional: announce public addrs to avoid private/VPN addrs in peers.
      # VITE_APPEND_ANNOUNCE: "/ip4/<PUBLIC_IP>/tcp/9091,/ip4/<PUBLIC_IP>/tcp/9092/ws,/ip4/<PUBLIC_IP>/udp/9093/webrtc-direct"
    ports:
      - "9090:9090/tcp"
      - "9091:9091/tcp"
      - "9092:9092/tcp"
      - "9093:9093/udp"
    volumes:
      - orbitdb-relay-data:/data

volumes:
  orbitdb-relay-data:
```

Start it:

```bash
docker compose up -d
docker compose logs -f orbitdb-relay
```

## What the example covers

- **Persistent datastore volume** — `orbitdb-relay-data` mounted at `/data` (matching
  `DATASTORE_PATH`) keeps the PeerId/key and libp2p state across restarts.
- **Relay + metrics ports exposed** — `9091/tcp`, `9092/tcp`, `9093/udp`, and `9090/tcp`.
- **WebRTC enabled** and **AutoTLS left enabled** by default.

## Notes and adjustments

- **QUIC:** the example does not publish the QUIC UDP port. Add `"9094:9094/udp"` and keep
  `RELAY_DISABLE_QUIC` unset if you want QUIC reachable from outside the host.
- **Public announce:** behind NAT or a cloud load balancer, set `VITE_APPEND_ANNOUNCE` to your
  public IPv4 and the same published ports so advertised multiaddrs match what the internet can
  dial. Keep the WebSocket entry as plain `/ws` — AutoTLS adds the secure `/tls/ws` addresses at
  runtime.
- **AutoTLS:** for a real certificate the container needs a publicly dialable address and outbound
  HTTPS to Let's Encrypt and `registration.libp2p.direct`. See [systemd](./systemd.md) for the
  AutoTLS walkthrough — the same requirements apply in Docker.
- **Firewall:** open the same TCP/UDP ports on the host and any cloud security group.
- **Environment:** every value above is optional and matches the built-in defaults. See
  [Environment variables](../reference/environment-variables.md) for the complete list, including
  circuit-relay v2 tuning.

## Verify

```bash
curl -sS http://127.0.0.1:9090/health
curl -sS http://127.0.0.1:9090/multiaddrs | head
```

Once AutoTLS has provisioned a certificate, `/multiaddrs` should include entries containing
`/tls/ws`.
