# Connectivity + Pinning HTTP Refactor Plan

This document records the first-pass refactor that turns `orbitdb-relay` into the canonical home for:

- `connectivityDebugProtocolsService()` as an opt-in libp2p service
- reusable PinningHttp request handling for `/health`, `/multiaddrs`, `/pinning/*`, and `/ipfs/*`
- configurable `/ipfs/*` gateway behavior with pinned-first and Helia fallback modes

Implementation goals:

- preserve current wire compatibility for `/connectivity-echo/1.0.0` and `/connectivity-bulk/1.0.0`
- keep `/pinning/*` success payloads backward-compatible
- standardize JSON errors for managed PinningHttp routes
- let embedded consumers reuse both the libp2p services and the HTTP API

Key defaults:

- debug protocols disabled by default
- bulk `maxFrameBytes = 262144`
- bulk `readTimeoutMs = 10000`
- bulk `idleTimeoutMs = 30000`
- PinningHttp `/ipfs/*` fallback mode defaults to `pinned-first-network-fallback`
- PinningHttp `catTimeoutMs = 120000`
