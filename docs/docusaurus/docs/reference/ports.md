---
id: ports
title: Ports
description: Default relay ports and Nym VPN-compatible alternatives.
---

# Ports

## Default ports

| Role | Default | Env var | Protocol |
| --- | --- | --- | --- |
| libp2p TCP | `9091` | `RELAY_TCP_PORT` | TCP |
| libp2p WebSocket | `9092` | `RELAY_WS_PORT` | TCP |
| libp2p WebRTC-direct | `9093` | `RELAY_WEBRTC_PORT` | UDP |
| libp2p QUIC | `9094` | `RELAY_QUIC_PORT` | UDP |
| Metrics / HTTP API | `9090` | `METRICS_PORT` | TCP |
| Metrics HTTPS (opt-in) | `9443` | `METRICS_HTTPS_PORT` | TCP |

Open the same TCP/UDP ports on the host firewall and any cloud security group. Behind NAT, set
`VITE_APPEND_ANNOUNCE` to your public IP and these ports so advertised multiaddrs match what the
internet can dial. See [Environment variables](./environment-variables.md) for the full list.

## Nym VPN and relay ports

If you reach your relay **through [Nym VPN](https://nymtech.net/)**, traffic exits the mixnet at a
Nym node that enforces an **exit policy**: only connections to certain **remote destination ports**
are allowed. The default relay ports (**9090–9094**) are **not** on that allow list, so browsers
and clients tunneled via Nym will fail to connect.

**Authoritative policy (refresh before production):**
[https://nymtech.net/.wellknown/network-requester/exit-policy.txt](https://nymtech.net/.wellknown/network-requester/exit-policy.txt)

### Recommended ports (verified against policy lines)

These appear under `ExitPolicy accept` in the file above (v1.6.0–style listing). Adjust if your
policy version differs.

| Role | Env var | Suggested port | Policy hint |
|------|---------|----------------|-------------|
| Metrics HTTP | `METRICS_PORT` | **8008** | "HTTP alternate" |
| Metrics HTTPS (AutoTLS PEM) | `METRICS_HTTPS_PORT` | **8443** or **9443** | "PCsync HTTPS…" / "alternative HTTPS" |
| libp2p TCP | `RELAY_TCP_PORT` | **50001** (or any in **50000–65535**) | "Discord voice call" range |
| WebSocket | `RELAY_WS_PORT` | **6300** | "Websocket" |
| QUIC UDP | `RELAY_QUIC_PORT` | **50005** (or another in **50000–65535**) | same range (UDP) |
| WebRTC-direct UDP | `RELAY_WEBRTC_PORT` | **50003** (or another in **50000–65535**) | same range (UDP) |

Also set **`VITE_APPEND_ANNOUNCE`** (or the dev variant) to public multiaddrs using **these same
ports**, e.g.:

```bash
PUBLIC_IP=203.0.113.50
VITE_APPEND_ANNOUNCE=/ip4/${PUBLIC_IP}/tcp/50001,/ip4/${PUBLIC_IP}/tcp/6300/ws,/ip4/${PUBLIC_IP}/udp/50005/quic-v1,/ip4/${PUBLIC_IP}/udp/50003/webrtc-direct
```

Enable metrics HTTPS only if needed:

```bash
METRICS_HTTPS_ENABLED=true
METRICS_HTTPS_PORT=8443
```

**Firewall:** open the same TCP/UDP ports on the host and any cloud security group.

**IPv6:** if the host cannot bind IPv6 listeners, set **`RELAY_DISABLE_IPV6=1`** (see
[systemd deployment](../getting-started/systemd.md)).

### Checking the policy from a shell

```bash
curl -fsSL 'https://nymtech.net/.wellknown/network-requester/exit-policy.txt' | head -40
```

Confirm your chosen ports appear in `ExitPolicy accept *:<port>` or inside an accepted range.
