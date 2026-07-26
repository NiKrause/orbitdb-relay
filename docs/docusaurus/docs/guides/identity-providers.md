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

## Relationship to access controllers

Identity providers answer *"is this entry validly signed by the identity it claims?"*. Access
controllers answer *"is that identity allowed to write here?"*. Both must line up with the
application:

- The [access controller](./access-controllers.md) must match the database manifest.
- The identity provider stack must match the identities your app issues.

If either drifts from the app, the relay may reject entries it should accept (or fail to verify a
custom AC's own store). Keep them aligned per [Version compatibility](../reference/version-compatibility.md).
