---
id: version-compatibility
title: Version compatibility
description: Why the relay image, the relay-button tooling tag, and the access-controller package must move together.
---

# Version compatibility

Replication between an application and `orbitdb-relay` only works when both sides agree on the
**access controller** and **identity** stack. Three things must be kept in lockstep, or the relay
will fail to replicate a custom access controller's **own** store — which in turn blocks the
databases that depend on it.

## The three things that must move together

| Moving part | What it is | Where it is pinned |
| --- | --- | --- |
| **Relay image** | The deployed `orbitdb-relay` build (container / rootfs) actually running as the pinner. | `orbitdb-relay` package version — currently **v0.10.1**. |
| **relay-button `tooling_ref` tag** | The tag [relay-button](https://github.com/NiKrause/relay-button) deploys when it stands a relay up. It selects which relay build (and its bundled AC/identity versions) goes live. | relay-button deployment config. |
| **`@le-space/orbitdb-access-controller-delegated-todo`** | The delegated todo access controller shared between the app (simple-todo) and the relay. | Relay depends on **`^0.1.0`**; the app must match. |

If the app ships one version of the access controller and the relay ships another, the AC's own
store (the delegation/authorization store the controller reads from) can diverge in manifest or
`canAppend`/verification semantics. When that store fails to replicate onto the relay, the relay
cannot authorize writes to the databases guarded by that controller — so pinning and recovery for
those databases fail even though ordinary OrbitDB replication is healthy.

## Why the access controller is special

An access controller is not just code — it has **its own OrbitDB store** (the set of grants/writers
it consults). The relay must:

1. register the **same** access controller type the database manifest names, and
2. replicate and verify the **controller's own store** using identical `canAppend` and identity
   verification.

Both the [access controller](../guides/access-controllers.md) and the
[identity providers](../guides/identity-providers.md) are therefore version-sensitive. A drift on
either side breaks verification of the controller's store, not just the application store.

## Related pinned versions in this relay

For reference, the relay currently pins (see `package.json`):

| Dependency | Version | Notes |
| --- | --- | --- |
| `@le-space/orbitdb-access-controller-delegated-todo` | `^0.1.0` | Must match the app. |
| `@le-space/orbitdb-identity-provider-webauthn-did` | `^0.3.1` | Passkey/DID identity stack; must match the app's identities. |
| `@orbitdb/core` | `^4.0.0` | OrbitDB major line. |
| `@libp2p/gossipsub` | pinned `16.1.0` | Pinned via package overrides; see [Sync & the #1255 workaround](../concepts/sync-and-1255-workaround.md). |

The package's `pnpm.peerDependencyRules` additionally pin the delegated-todo controller's transitive
`@le-space/orbitdb-identity-provider-webauthn-did` and `@orbitdb/core` versions, so the controller
verifies identities exactly as the app does.

## Practical rule

When you bump any one of these, bump the others in the same change:

1. Update `@le-space/orbitdb-access-controller-delegated-todo` in **both** the app and the relay.
2. Publish/deploy a matching **relay image**.
3. Point relay-button's **`tooling_ref`** at that relay build.

Then verify replication end to end (for example with the cross-provider simple-todo flow described
in [Peer recovery](../concepts/peer-recovery.md#verified-outcome-and-remaining-limitation)) before
relying on it.

:::note Verification note
The exact current value of relay-button's `tooling_ref` tag is a deployment-side setting and is not
defined in this repository; confirm it against the live relay-button configuration. The dependency
versions above are read directly from this relay's `package.json`.
:::
