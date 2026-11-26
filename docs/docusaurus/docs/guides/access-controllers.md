---
id: access-controllers
title: Access controllers
description: The OrbitDB access controllers orbitdb-relay supports, including the delegated todo controller.
---

# Access controllers

`orbitdb-relay` supports the following OrbitDB Access Controller types when opening databases:

- **`orbitdb`** — the built-in OrbitDB access controller.
- **`orbitdb-deferred`** — a custom deferred OrbitDB ACL registered by this package
  (`src/access/deferred-orbitdb-access-controller.ts`).
- **`todo-delegation`** — delegated todo writes (same rules as the app; see below).

Notes:

- Existing databases are opened using the manifest's `accessController.type`.
- Creating a new database **without** explicitly passing an `AccessController` still defaults to
  `orbitdb`.

## Why the relay needs the app's access controller

The relay verifies oplog entries as part of replication. To evaluate `canAppend` the same way the
browser does, it must register the **same** access controller the application uses. If the relay's
access-controller package version drifts from the app's, verification of a custom AC's own store
can fail — see [Version compatibility](../reference/version-compatibility.md).

## The `todo-delegation` controller

The delegated todo access controller is part of the relay
(`src/access/delegated-todo-access-controller.ts`, writer verification in `src/access/shared.ts`).
It was taken from **`@le-space/orbitdb-access-controller-delegated-todo` 0.1.0**, which
[simple-todo](https://github.com/Le-Space/simple-todo) uses, with the same rules, so pinning
keeps the `canAppend` and `verifyDelegationWriterIdentity` behavior of the browser:

- the owner writes through OrbitDB's `OrbitDBAccessController`;
- a delegate's `PUT` under `delegation-action/<taskKey>/<encoded delegate DID>/<id>` is admitted
  when its value is a `delegation-action` (`set-completed` with a boolean `setCompleted`, or
  `patch-fields` with string `text` and/or `description`) matching the key, not past its
  `expiresAt`, and written by a verified identity whose id is the delegate DID.

That package declares peer dependencies on `@le-space/orbitdb-identity-provider-webauthn-did`
`^0.2.10` and `@orbitdb/core` `^3`, and kept a vulnerable provider in the relay's tree
(GHSA-7j39-c465-x35x). The relay verifies with the provider it depends on itself.

## Choosing a controller

- Use **`orbitdb`** for a standard shared database where the write set is the set of known
  identities.
- Use **`orbitdb-deferred`** when you need the deferred OrbitDB ACL semantics this package
  registers.
- Use **`todo-delegation`** when your app already uses the delegated-todo controller and you want
  the relay to enforce the identical delegation rules while it pins.

The relay does not decide the controller — the database manifest does. Your application picks the
access controller when it creates the database; the relay simply opens the manifest and registers
the matching controller so it can verify entries.

## Registering your own

An app with its own access controller can have its databases replicated too: the
relay registers additional controllers from its configuration, next to the built-in
ones. A controller is what OrbitDB's `useAccessController` takes, a factory function
with a string `type`; the module exports it as its default export (one or an array)
or as a named `accessControllers` array.

```bash
RELAY_ACCESS_CONTROLLERS=@example/orbitdb-access-controller-contacts,./acl/notes.js orbitdb-relay
```

```ts
import { startRelay } from 'orbitdb-relay'
import ContactsAccessController from '@example/orbitdb-access-controller-contacts'

await startRelay({ accessControllers: [ContactsAccessController] })
// or, embedded: orbitdbReplicationService({ datastore, blockstore, accessControllers: [ContactsAccessController] })
```

- A path (`./`, `../`, absolute, `file:`) resolves against the working directory; a
  package name resolves from where orbitdb-relay is installed, so install the package
  next to it.
- Only code already on the machine is loaded; nothing is fetched from the network.
- A module that can't be imported, or exports no controller, stops the relay at start.
- A configured type may not replace a built-in one (`ipfs`, `orbitdb`,
  `orbitdb-deferred`, `todo-delegation`).
- The controller runs inside the relay and decides what it accepts and pins: register
  only code you trust, and keep its version in step with the app's.

### Bundled: deContact

The relay depends on **`@le-space/orbitdb-access-controller-decontact`** (type
`decontact-address-book`). `deploy/orbitdb-relay.env.example` names it in
`RELAY_ACCESS_CONTROLLERS`, so relays installed from it, the Aleph image included, replicate
[deContact](https://github.com/NiKrause/deContact) address books:

- the owner (every device with the owner's DID) may write anything;
- anybody else may write only their own contact card, with an invitation signed by the
  owner's did:key.

## See also

- [Identity providers](./identity-providers.md) — how the relay verifies the identities that
  `canAppend` checks.
- [Version compatibility](../reference/version-compatibility.md) — keeping the AC package in sync
  between app and relay.
