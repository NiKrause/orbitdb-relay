---
id: identity-providers
title: Identity providers
description: The OrbitDB identity providers orbitdb-relay registers so it can verify entries from passkey-backed peers.
---

# Identity providers

The relay registers OrbitDB identity providers so it can verify oplog entries from peers that use
passkey-backed identities. It uses the same stack as
[`@le-space/orbitdb-identity-provider-webauthn-did`](https://www.npmjs.com/package/@le-space/orbitdb-identity-provider-webauthn-did).

| Provider | Source | Notes |
| --- | --- | --- |
| **`publickey`** | `@orbitdb/core` | The default OrbitDB identity provider. |
| **`did`** | `@orbitdb/identity-provider-did` | DID-based identities (for example Ed25519 `did:key`). |
| **`webauthn`** | worker WebAuthn + keystore | Verifies passkey-backed identities (for example Ed25519 `did:key` via the keystore). |
| **`webauthn-varsig`** | hardware varsig identities | Verification uses the **embedded public key only**; no passkey lives on the server. |

## How verification works on the relay

The relay does not hold users' passkeys. It only needs to **verify** signatures on entries it
replicates. The verification fallback (`src/identity/relay-verify-fallback.ts`) routes each entry
by its identity `type`:

- `did` identities are verified through the DID provider.
- `webauthn` identities are verified via the worker WebAuthn provider's `verifyIdentity`.
- `webauthn-varsig` identities are verified using the public key embedded in the identity — the
  hardware authenticator is never contacted from the server.
- A worker Ed25519 path (`src/identity/worker-ed25519.ts`) verifies `did:key` Ed25519 signatures
  directly.

This lets the relay evaluate `canAppend` for databases whose writers authenticate with passkeys,
without ever needing the passkey itself.

## Proof formats (provider 0.14.0)

The relay depends on `@le-space/orbitdb-identity-provider-webauthn-did` **`^0.14.0`**, the
security release for
[GHSA-7j39-c465-x35x](https://github.com/Le-Space/orbitdb-identity-provider-webauthn-did/security/advisories/GHSA-7j39-c465-x35x).
A `webauthn` identity carries a WebAuthn assertion in `signatures.publicKey` that binds its signing
key to its DID:

- **Current format** — what 0.14.0 creates for every new identity. 0.13.0 and older cannot verify
  it, so relays have to run 0.14.0 before devices create such identities.
- **First format** — what 0.13.0 and older created. A device that stored such a proof keeps using
  it, and everything written before is under it.

The relay verifies both. It keeps the provider's default (`acceptLegacyProofs` unset): a
first-format proof is accepted, with one console notice per process. It does not call
`configureWebAuthn({ acceptLegacyProofs: false })`, which would stop existing history from
verifying.

The relay's identities are wrapped with the provider's `withoutStaleIdentityCache`. OrbitDB caches
a verified identity under `signatures.id` and refuses any other document with the same id
signature ([orbitdb/orbitdb#1258](https://github.com/orbitdb/orbitdb/issues/1258)); a passkey keeps
its signing key, and with it `signatures.id`, while its proof changes (another session, a restore,
a move from a first-format proof to a current one). The wrapper checks such a `webauthn` identity
again instead of refusing it.

`mocha/webauthn-identity-proofs.mjs` covers both formats with a software passkey: through the
relay's identities, through the `todo-delegation` controller, and through replication to a relay.

## Relationship to access controllers

Identity providers answer *"is this entry validly signed by the identity it claims?"*. Access
controllers answer *"is that identity allowed to write here?"*. Both must line up with the
application:

- The [access controller](./access-controllers.md) must match the database manifest.
- The identity provider stack must match the identities your app issues.

If either drifts from the app, the relay may reject entries it should accept (or fail to verify a
custom AC's own store). Keep them aligned per [Version compatibility](../reference/version-compatibility.md).
