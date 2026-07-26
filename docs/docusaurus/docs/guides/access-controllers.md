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

## The `todo-delegation` package

The delegated todo access controller lives in
**`@le-space/orbitdb-access-controller-delegated-todo`** (shared with
[simple-todo](https://github.com/NiKrause/simple-todo)) so pinning uses the same `canAppend` and
`verifyDelegationWriterIdentity` behavior as the browser. This relay depends on **`^0.1.0`** of
that package.

If `npm install` cannot resolve it yet, install from a local checkout of simple-todo:

```bash
npm install ../path/to/simple-todo/packages/orbitdb-access-controller-delegated-todo
```

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

## See also

- [Identity providers](./identity-providers.md) — how the relay verifies the identities that
  `canAppend` checks.
- [Version compatibility](../reference/version-compatibility.md) — keeping the AC package in sync
  between app and relay.
