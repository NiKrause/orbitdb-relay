---
id: sync-and-1255-workaround
title: "Sync & the #1255 workaround"
sidebar_label: "Sync & #1255"
description: The OrbitDB Sync first-entry deadlock, why gossipsub upgrades do not fix it, and the relay's heads re-announce mitigation.
---

# Sync & the #1255 workaround

OrbitDB replication is normally reliable between two directly connected peers. It becomes fragile
when a reader is connected to a writer **only through a relay**, and the very first entry of a
database has to cross that relay. This page explains the deadlock (tracked upstream as
[orbitdb/orbitdb#1255](https://github.com/orbitdb/orbitdb/issues/1255)), why a gossipsub upgrade
does **not** fix it, and the mitigation `orbitdb-relay` ships.

## How OrbitDB Sync bootstraps

OrbitDB's [`Sync`](https://github.com/orbitdb/orbitdb/blob/v4.0.0/src/sync.js) coordinates heads
between peers. For each open, syncing database it:

- subscribes to the database address as a pubsub topic;
- listens for pubsub **`subscription-change`** events;
- registers the `/orbitdb/heads/<address>` protocol;
- on a peer's subscription change, **dials that peer once** to exchange current heads;
- publishes newly appended entries on the database topic; and
- joins received entries into the local oplog.

Two properties of this design matter here:

1. **The heads exchange is edge-triggered.** A writer only dials the heads exchange when it
   *observes* a `subscription-change` for the topic. If no fresh change edge arrives, no dial
   happens.
2. **The per-peer exchange is one-shot.** Once two peers have exchanged heads, Sync caches that and
   does not re-run the exchange for the same peer. Entries received later over the heads protocol
   are **not** re-announced on the topic.

## The deadlock (#1255)

Combine those two properties with a relay that opens databases *lazily*, and you get a race:

1. A browser writer publishes the first entry into a topic **nobody is listening to yet**.
2. The relay learns the address from pubsub activity and opens the database *after* the fact.
3. A reader (browser B) is subscribed and mesh-grafted the whole time, and it exchanges heads with
   the relay **before** the relay has actually obtained the writer's entry.
4. Because the exchange already happened, Sync's one-shot cache means B never asks the relay again;
   because heads received over the protocol are never re-announced, the relay never pushes the
   entry out. B stays empty until a timeout.

Observed live: writer A published into a silent topic, the relay received A's entry moments later
via A's proof-triggered exchange, and reader B — subscribed and grafted the entire time — never
saw it. This is the first-entry deadlock behind
[#1255](https://github.com/orbitdb/orbitdb/issues/1255).

## Why a gossipsub upgrade does not fix it

It is tempting to blame the pubsub mesh: if B's subscription had grafted sooner, maybe the entry
would have propagated. `orbitdb-relay` runs **gossipsub 16.1.0** (pinned via the package
overrides), which includes **graft-on-subscribe** behavior. It does not fix #1255.

The reason is that these are **two different problems**:

- **A transport race** — whether the pubsub mesh is grafted in time for a message to be delivered.
  Graft-on-subscribe helps here.
- **A Sync-coordination gap** — whether OrbitDB's edge-triggered, one-shot heads exchange ever runs
  again after the relevant heads finally arrive on the relay. Graft-on-subscribe does **not** touch
  this; the mesh can be perfectly grafted and the entry still never gets re-announced, because Sync
  simply never publishes heads it received over the heads protocol.

So a newer gossipsub improves mesh timing but leaves the coordination deadlock in place. The fix
has to live at the OrbitDB Sync layer (the durable upstream fix would be for Sync to enumerate
peers that are *already* subscribed when it starts, instead of relying on the change edge).

## The relay's mitigation: `republishHeadsToSubscribers()`

Until the upstream fix lands, `orbitdb-relay` closes the gap after every successful sync. In
`src/services/database.ts`, once a sync pass has actually received an update, the relay
re-publishes the database's **current heads** back onto the topic through OrbitDB's own
`sync.add()`:

```ts
// src/services/database.ts (paraphrased)
private async republishHeadsToSubscribers(dbAddress, db, dbName) {
  const heads = (await db?.log?.heads?.()) ?? []
  for (const head of heads) {
    await db?.sync?.add?.(head)   // OrbitDB's own publish path
  }
}
```

Why this works:

- `sync.add()` is the *same* path OrbitDB uses when a local peer appends, so it delivers the heads
  to **every current topic subscriber** — including the reader that already exhausted its one-shot
  exchange.
- Receivers **de-duplicate** via `joinEntry`, so re-publishing a head they already have is
  harmless.
- The caller **gates on `receivedUpdate`**, so an unchanged sync publishes nothing. This prevents
  relay-to-relay echo loops.

The result: the reader that was stuck now receives the heads it missed, and OrbitDB joins them
normally. No custom replication protocol is introduced — the relay only nudges OrbitDB's existing
publish path.

## A related dead end (do not re-attempt)

A tempting "fix" is to keep the relay **permanently subscribed** to each database topic so it never
misses a change. This was tried (2026-07-22) and made things **worse**:

> OrbitDB's Sync bootstrap is edge-triggered. A relay that stays subscribed makes its next database
> open a *no-op* subscription change — writers never observe an edge, never dial, and the relay
> never receives heads. Every sync pass then reports `receivedUpdate: false`.

So the relay deliberately does **not** hold subscriptions across database closes. The correct
lever is re-announcing heads after a real sync, not staying subscribed.

## Relationship to explicit recovery

The heads re-announce is the *passive* mitigation applied on every successful sync. It is
complementary to the *explicit* `POST /pinning/sync` recovery path, which reconnects remembered
peers so OrbitDB's native heads exchange can run again. See [Peer recovery](./peer-recovery.md) for
that mechanism, and the [HTTP API](../reference/http-api.md#post-pinningsync) for the endpoint.

## Status

Passive writer-to-relay convergence is still not guaranteed in every topology; that remaining
objective is tracked in [orbitdb-relay#10](https://github.com/NiKrause/orbitdb-relay/issues/10).
The preferred future outcome is for the normal OrbitDB path to be reliable enough that neither the
re-announce nor the recovery endpoint is needed. Upstream: [orbitdb/orbitdb#1255](https://github.com/orbitdb/orbitdb/issues/1255).
