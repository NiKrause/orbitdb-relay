---
id: media-pinning
title: Media pinning flow
description: How orbitdb-relay pins IPFS media CIDs while replicating OrbitDB databases.
---

# Media pinning flow

This page explains how `orbitdb-relay` pins IPFS media CIDs while syncing OrbitDB databases.

## Goal

When peers publish OrbitDB entries containing media references (for example `imageCid`), the relay
should:

1. replicate the OrbitDB records,
2. extract media CIDs from those records,
3. pin those CIDs locally in Helia/IPFS,
4. keep that media available to later peers that sync through the relay.

## Trigger path

The event entrypoint is `src/events/handlers.ts`.

- Pubsub **`message`**:
  - If the topic starts with `/orbitdb/`, queue a sync task.
- Pubsub **`subscription-change`**:
  - Remember the subscribing peer as an in-memory hint for that OrbitDB topic.
  - For each topic starting with `/orbitdb/`, queue a normal sync task.

Sync tasks call `DatabaseService.syncAllOrbitDBRecords(dbAddress)` in `src/services/database.ts`.

## Sync flow

For each sync task:

1. Open the OrbitDB database from `dbAddress`.
2. Wait for OrbitDB `update` event(s).
3. If updates were observed, extract media CIDs from those update entries.
4. If no update event is observed, scan `db.all()` because replication may have completed before
   the listener was attached.
5. Enqueue discovered media CIDs asynchronously and record an exact local-state snapshot.
6. Keep databases with replicated state open so the relay continues serving native OrbitDB heads.

Notes:

- Sync does not call `db.all()` when an update event supplied the records.
- If no update arrives within the observation window, `db.all()` provides the fallback scan and
  sync proof.
- A short update-burst window is used to collect multiple closely spaced updates in the same sync
  execution.
- An explicit [`POST /pinning/sync`](../reference/http-api.md#post-pinningsync) may reconnect peers
  previously observed subscribing to the database topic. The reconnect happens only after the
  database is open and its native OrbitDB heads topology is registered.

## CID extraction rules (update payload)

CID extraction inspects update payload objects and currently supports:

- `imageCid`
- `imageCID`
- `image.cid`
- `profilePicture`
- `profilePictureCid`
- `profilePictureCID`
- key/value style profile picture records:
  - `_id` equals `profilePicture`, `profilePictureCid`, or `profilePictureCID`
  - CID is read from `value`
- `mediaId`
- `mediaIds` (array)

Only non-empty string candidates are kept, deduplicated per sync run and across queue state.

## Pinning mechanics

Pinning is done through the Helia pin API, not direct blockstore writes:

- `ipfs.pins.add(CID.parse(cidString))`

Pinning uses an internal `PQueue` in `DatabaseService`:

- Concurrency: `4`
- Non-blocking from the sync method's perspective
- Dedupe sets:
  - `queuedImageCids` prevents duplicate queued work
  - `pinnedImageCids` avoids re-pinning already pinned CIDs

## Shutdown behavior

Shutdown is coordinated across relay, handlers, metrics, and the DB service:

- Event handlers stop accepting new sync tasks and drain/clear the queue.
- The database service enters shutdown mode, drains/clears the pin queue, and stops OrbitDB.
- The metrics HTTP server is closed.
- Helia/IPFS and libp2p are stopped.

This reduces hanging test runs and avoids lingering background tasks.

## Test coverage

Integration coverage exists in `mocha/relay-media-replication.mjs`:

1. Alice writes a post with an image CID.
2. The relay syncs and pins those CIDs.
3. Alice goes offline.
4. Bob connects and syncs the same DB.
5. Bob fetches each CID from his blockstore and the bytes are asserted against Alice's original
   content.

Additional unit-style integration coverage lives in `mocha/database-update-event-pinning.mjs`,
which verifies that a single update event triggers pinning and that `db.all()` is not used for this
update-driven pinning path.

## Related

- [Peer recovery](./peer-recovery.md) — why the relay remembers subscribing peers and how explicit
  recovery reconnects them.
- [HTTP API](../reference/http-api.md) — the `/pinning/*` and `/ipfs/*` routes.
