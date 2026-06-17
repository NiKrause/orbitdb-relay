# Relay Media Pinning Logic

This document explains how `orbitdb-relay` pins IPFS media CIDs while syncing OrbitDB databases.

## Goal

When peers publish OrbitDB entries containing media references (for example `imageCid`), the relay should:

1. replicate the OrbitDB records,
2. extract media CIDs from those records,
3. pin those CIDs locally in Helia/IPFS,
4. keep that media available to later peers that sync through the relay.

## Trigger Path

Event entrypoint is `src/events/handlers.ts`.

- Pubsub `message`:
  - If topic starts with `/orbitdb/`, queue a sync task.
- Pubsub `subscription-change`:
  - For each topic starting with `/orbitdb/`, queue a sync task.

Sync tasks call:

- `DatabaseService.syncAllOrbitDBRecords(dbAddress)` in `src/services/database.ts`.

## Sync Flow

For each sync task:

1. Open the OrbitDB database from `dbAddress`.
2. Wait (up to 5s) for OrbitDB `update` event(s).
3. If updates were observed, extract media CIDs from those update entries only.
4. Enqueue media pinning asynchronously (non-blocking for sync completion).
5. Close the opened DB handle.

Notes:

- Sync does not call `db.all()` in the update-driven path.
- If no update arrives within timeout, no new pin enqueue occurs for that sync run.
- A short update-burst window is used to collect multiple closely spaced updates in the same sync execution.

## CID Extraction Rules (Update Payload)

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

## Pinning Mechanics

Pinning is done through Helia pin API, not direct blockstore writes:

- `ipfs.pins.add(CID.parse(cidString))`

Pinning uses an internal `PQueue` in `DatabaseService`:

- Concurrency: `4`
- Non-blocking from sync method perspective
- Dedupe sets:
  - `queuedImageCids` prevents duplicate queued work
  - `pinnedImageCids` avoids re-pinning already pinned CIDs

## Shutdown Behavior

Shutdown is coordinated across relay, handlers, metrics, and DB service:

- Event handlers stop accepting new sync tasks and drain/clear queue.
- Database service enters shutdown mode, drains/clears pin queue, stops OrbitDB.
- Metrics HTTP server is closed.
- Helia/IPFS and libp2p are stopped.

This reduces hanging test runs and avoids lingering background tasks.

## Test Coverage

Integration coverage exists in:

- `mocha/relay-media-replication.mjs`

Scenario:

1. Alice writes a post with an image CID.
2. Relay syncs and pins those CIDs.
3. Alice goes offline.
4. Bob connects and syncs same DB.
5. Bob fetches each CID from his blockstore and bytes are asserted against Alice's original content.

Additional unit-style integration coverage:

- `mocha/database-update-event-pinning.mjs`

This verifies that a single update event triggers pinning and that `db.all()` is not used for this update-driven pinning path.
