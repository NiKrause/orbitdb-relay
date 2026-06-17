# Changes

## v0.9.0

- Added `connectivityDebugProtocolsService()` as an opt-in embeddable libp2p service for the versioned `/connectivity-echo/1.0.0` and `/connectivity-bulk/1.0.0` debug protocols.
- Added reusable PinningHttp exports so embedded consumers can reuse `/health`, `/multiaddrs`, `/pinning/*`, and `/ipfs/*` without copying relay-local HTTP code.
- Moved shared route behavior into `createPinningHttpRequestHandler()` and `PinningHttpServer`, and updated the relay runtime to use that shared implementation.
- Added configurable `/ipfs/*` gateway fallback so the default runtime serves pinned content first and then falls back to Helia network retrieval when content is not pinned locally.
- Added upstream test coverage for the new debug-protocol service export and the shared PinningHttp handler error/fallback behavior.
- Verified with `pnpm test`, plus downstream `helia-connectivity-lab` build, PWA check, and the key Playwright relay replication scenario.

### Release note

This is a minor release that expands `orbitdb-relay` from a reusable OrbitDB replication service into a more complete embeddable relay toolkit. Consumers can now opt into shared debug protocols, reuse the standard pinning and IPFS HTTP surface directly, and rely on the same pinned-first gateway semantics used by the full relay runtime.

## v0.8.0

- Switched repository and GitHub Actions installs from `npm` to `pnpm`, including lockfile-based CI setup and pinned package-manager metadata.
- Added the missing direct dependencies `interface-blockstore` and `@multiformats/multiaddr`, which `pnpm` correctly requires because the code and integration tests import them directly.
- Added optional libp2p QUIC transport support, including `RELAY_QUIC_PORT`, `RELAY_DISABLE_QUIC`, and updated Nym/VPN-facing port documentation and examples.
- Added on-demand OrbitDB heads protocol support for known replicated databases so the relay can serve heads when the protocol is requested dynamically.
- Updated the replication and media integration tests for the new transport settings, while preserving fast update-driven pinning without forcing an eager `db.all()` scan.
- Verified with `pnpm test` locally and green GitHub Actions runs for the build matrix and relay media integration workflow.

### Release note

This is a minor release focused on release reliability and replication connectivity. The project now uses `pnpm` consistently in local development and GitHub Actions, declares the direct dependencies required for strict installs, adds optional QUIC transport support, and improves OrbitDB heads replication behavior without regressing fast update-event media pinning.

## v0.7.1

- Added non-fatal OrbitDB database error handlers so emitted `error` events are logged with database context instead of surfacing as unhandled runtime failures during sync.
- Keeps the `0.7.0` libp2p replication-service release intact while addressing follow-up operational stability seen after publishing.
- Verified with `npm test` and `npm pack --dry-run`.

### Release note

This is a patch release for the `0.7.x` line. It keeps the new embeddable libp2p OrbitDB replication service from `0.7.0`, while hardening the relay against non-fatal OrbitDB sync errors by logging them with enough context to debug production issues safely.

## v0.7.0

- Exported `orbitdbReplicationService()` so OrbitDB replication and Helia-backed pinning can be mounted directly inside any libp2p node.
- Refactored `startRelay()` to use the shared replication service internally, keeping the CLI-compatible relay runtime while exposing the service as a library feature.
- Added integration coverage for the direct libp2p-service mount path and for importing the package from a separate consumer project.
- Documented library usage in `README.md` and added `docs/libp2p-service.md`, which is now included in the published package.
- Added optional inbound Identify filtering for peers that do not advertise `/orbitdb/heads/*`, plus metrics and env docs for that behavior.

### Release note

This is a feature release that turns the relay’s OrbitDB replication logic into a reusable libp2p service. Consumers can now embed the replication and pinning behavior in their own libp2p nodes without giving up the existing CLI and `startRelay()` workflow.

## v0.6.4

- Updated `@chainsafe/libp2p-gossipsub` from `14.1.1` to `14.1.2`.
- Updated `@le-space/orbitdb-identity-provider-webauthn-did` from `^0.2.10` to `^0.2.13`.
- Aligned the dependency specifier in `pnpm-lock.yaml` with the package manifest.
- Verified the project with `npm test`; the full suite passed locally, including relay integration coverage.

### Release note

This is a low-risk patch release focused on dependency maintenance. The updated WebAuthn OrbitDB identity provider includes hybrid verification fixes for mixed varsig and default OrbitDB/WebAuthn verification paths, and gossipsub is aligned with the current package set used by the project.
