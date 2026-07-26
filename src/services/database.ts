/**
 * Relay / pinner OrbitDB layer.
 *
 * {@link DatabaseService} is the relay's entire OrbitDB surface. Browsers in
 * the todo app come and go, so they hand their databases to this always-on
 * relay to keep them replicated and their referenced media pinned. For each
 * database the relay is asked about, this service:
 *   - opens and retains the OrbitDB instance (and its access-controller DB),
 *   - runs a sync / heads-exchange pass that pulls the writer's entries into
 *     the relay's local IPFS/LevelDB store,
 *   - snapshots the resulting local state as a replication proof,
 *   - pins any media CIDs (images, profile pictures, generic content CIDs)
 *     referenced by the synced records, and
 *   - re-announces the freshly synced heads so other subscribed browsers
 *     actually converge.
 * It also serves the pinning HTTP API and an on-demand libp2p heads protocol,
 * registers the custom access controllers / identity providers the app needs,
 * and records sync/replication metrics.
 *
 * Features implemented here:
 *   - Database open with in-flight de-duplication ({@link DatabaseService.openDatabase},
 *     {@link DatabaseService.retainOpenDatabase}/{@link DatabaseService.releaseOpenDatabase}
 *     reference-count opens) and per-address sync de-duplication with coalescing
 *     and an explicit "wait then run fresh" path
 *     ({@link DatabaseService.syncAllOrbitDBRecordsWithResult}).
 *   - Access-controller registration in {@link DatabaseService.initialize}:
 *     the built-in `orbitdb` controller from @orbitdb/core plus the local
 *     `ipfs` ({@link IPFSAccessController}), `orbitdb-deferred`
 *     ({@link DeferredOrbitDBAccessController}) and `todo-delegation`
 *     ({@link DelegatedTodoAccessController} / DelegatedTodoAccessControllerBase)
 *     controllers.
 *   - Identity-provider registration: `publickey` (built into @orbitdb/core),
 *     `did`, `webauthn` and `webauthn-varsig`, plus a relay-side identity
 *     verification fallback for mixed writer modes.
 *   - Pinning HTTP handlers ({@link DatabaseService.createPinningHttpHandlers})
 *     backing `/pinning/*`, and an IPFS gateway that streams UnixFS/raw content
 *     for a CID only when it is locally pinned
 *     ({@link DatabaseService.streamPinnedIpfsContent}).
 *   - A heads re-announce workaround for the OrbitDB Sync first-entry deadlock
 *     (upstream issue orbitdb/orbitdb#1255, {@link https://github.com/orbitdb/orbitdb/issues/1255}):
 *     see {@link DatabaseService.republishHeadsToSubscribers}. This is a
 *     relay-side mitigation — it re-publishes already-synced heads through
 *     OrbitDB's own `sync.add()` so late-joining subscribers receive them — and
 *     is NOT a fix for Sync itself.
 *   - An on-demand heads libp2p protocol
 *     ({@link DatabaseService.handleOnDemandHeadsProtocol}) so a peer can pull
 *     heads for a known database even after the relay has closed it, plus
 *     known-subscriber reconnection ({@link DatabaseService.reconnectKnownDatabasePeers})
 *     to re-trigger OrbitDB's edge-triggered heads exchange.
 *   - Replication proofs / metrics: {@link DatabaseService.pinnedDatabasesByAddress}
 *     records the last snapshot (entry count + last record) per address, exposed
 *     via the pinning stats/databases handlers and the {@link MetricsServer}.
 *
 * Overall sync flow (see {@link DatabaseService.syncAllOrbitDBRecordsWithResult}):
 * a sync request first de-duplicates against any in-flight sync for the same
 * address. It loads the database manifest (to learn the name and the
 * access-controller address), remembers both addresses as "known", and
 * pre-opens the access-controller database so writer identities can be verified
 * before entries arrive. It then opens (and retains) the target database,
 * optionally reconnecting known subscribers to nudge OrbitDB's edge-triggered
 * heads exchange, and waits briefly for an `update` event burst. Received
 * updates (or a `db.all()` fallback scan) are mined for media CIDs, which are
 * queued for pinning; the local state is snapshotted as a proof and stored in
 * {@link DatabaseService.pinnedDatabasesByAddress}. When there is replicated
 * state the database is kept open (so OrbitDB keeps its native heads topology
 * registered) and, if a genuine update was received, the heads are re-announced
 * to subscribers. Databases opened only transiently are released in a `finally`
 * block.
 *
 * @remarks Documentation-only module header; describes behavior implemented by
 * the class and helpers below.
 */
import {
  createOrbitDB,
  Identities,
  parseAddress,
  useAccessController,
  useIdentityProvider,
} from "@orbitdb/core";
import {
  OrbitDBWebAuthnIdentityProviderFunction,
  verifyVarsigIdentity,
} from "@le-space/orbitdb-identity-provider-webauthn-did";
import OrbitDBIdentityProviderDID from "@orbitdb/identity-provider-did";
import * as KeyDIDResolver from "key-did-resolver";
import { unixfs } from "@helia/unixfs";
import { CID } from "multiformats/cid";
import * as Block from "multiformats/block";
import * as dagCbor from "@ipld/dag-cbor";
import { sha256 } from "multiformats/hashes/sha2";
import { base58btc } from "multiformats/bases/base58";
import { setTimeout as delay } from "node:timers/promises";
import { inspect } from "node:util";
import PQueue from "p-queue";

import {
  MetricsServer,
  type PinningHttpHandlers,
  type StreamPinnedCidResult,
} from "./metrics.js";
import { syncLog, logSyncStats } from "../utils/logger.js";
import { loggingConfig } from "../config/logging.js";
import IPFSAccessController from "../access/ipfs-access-controller.js";
import DelegatedTodoAccessControllerBase from "@le-space/orbitdb-access-controller-delegated-todo";
import DeferredOrbitDBAccessController from "../access/deferred-orbitdb-access-controller.js";
import { verifyIdentityWithFallback } from "../access/shared.js";
import {
  createRelayVerifyIdentityFallback,
  defaultRelayVerifyIdentityDeps,
} from "../identity/relay-verify-fallback.js";
import { inspectWorkerEd25519Identity } from "../identity/worker-ed25519.js";

/** Relay: same delegated AC as clients, without verbose browser logging. */
const DelegatedTodoAccessController = (opts: { write?: string[] } = {}) =>
  DelegatedTodoAccessControllerBase({ ...opts, verbose: false });
(DelegatedTodoAccessController as any).type = (
  DelegatedTodoAccessControllerBase as any
).type;

const DEFERRED_ACL_PREFIX = "/orbitdb-deferred/";
const ORBITDB_PREFIX = "/orbitdb/";
const ORBITDB_HEADS_PREFIX = "/orbitdb/heads";
const ON_DEMAND_HEADS_HANDLER_TIMEOUT_MS = 5000;
const RELAY_ERROR_HANDLER_INSTALLED = Symbol("relayErrorHandlerInstalled");
const SYNC_IN_FLIGHT_STALE_MS = 45_000;
const COALESCED_SYNC_WAIT_MS = 5_000;
const OPEN_IN_FLIGHT_STALE_MS = 45_000;
const MANIFEST_LOAD_TIMEOUT_MS = 5_000;

/** Deduped media CID plus which payload fields referenced it (for sync/pin logs). */
export type ExtractedMediaCid = { cid: string; sources: string[] };
/**
 * A shared in-flight operation keyed by database address, used to de-duplicate
 * concurrent opens and syncs. `startedAt` (epoch ms) lets callers detect a
 * stale operation and decide whether to keep waiting or coalesce.
 */
type InFlightRecord<T> = {
  promise: Promise<T>;
  startedAt: number;
};
/**
 * Summary of a database's local state captured right after a sync pass, used as
 * a human-readable replication proof in logs and in {@link PinnedDatabaseRecord}.
 * `entryCount` is null when it could not be counted (e.g. only an update burst
 * was observed); `source` names how the snapshot was produced (`db.all()`,
 * `iterator`, `update-event burst only`, …).
 */
export type DatabaseSyncSnapshot = {
  entryCount: number | null;
  lastRecord: Record<string, unknown> | null;
  source: string;
};
/**
 * Persisted record of the most recent successful sync for one database address,
 * surfaced by the pinning HTTP handlers. `snapshotSource` mirrors
 * {@link DatabaseSyncSnapshot.source}.
 */
export type PinnedDatabaseRecord = {
  address: string;
  lastSyncedAt: string;
  entryCount: number | null;
  lastRecord: Record<string, unknown> | null;
  snapshotSource: string;
};

/**
 * Best-effort coercion of an arbitrary value into a single {@link Uint8Array}.
 *
 * IPFS/Helia block APIs return bytes in several shapes depending on the
 * backend (Uint8Array, ArrayBuffer, other typed-array/DataView views, plain
 * number arrays, or objects exposing `subarray`/`slice`). This normalizes all
 * of them so downstream decoding (manifest/DAG-CBOR) sees plain bytes.
 *
 * @param value - The candidate byte container.
 * @returns The bytes as a `Uint8Array`; an empty array when `value` is not a
 * recognizable byte source.
 */
function toUint8Array(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  if (Array.isArray(value)) return Uint8Array.from(value);
  if (
    value != null &&
    typeof (value as { subarray?: unknown }).subarray === "function"
  ) {
    return toUint8Array((value as { subarray: () => unknown }).subarray());
  }
  if (
    value != null &&
    typeof (value as { slice?: unknown }).slice === "function"
  ) {
    return toUint8Array((value as { slice: () => unknown }).slice());
  }
  return new Uint8Array();
}

/**
 * Fully materialize a (possibly streamed) byte source into one contiguous
 * {@link Uint8Array}.
 *
 * Helia's blockstore may hand back an async iterable of chunks, a sync iterable
 * of chunks, or a single buffer. Async iterables are drained and concatenated;
 * non-empty sync iterables of chunks are likewise concatenated; anything else
 * falls back to {@link toUint8Array}. Used before decoding an OrbitDB manifest
 * block.
 *
 * @param value - A byte buffer, or a sync/async iterable of byte chunks.
 * @returns A promise for the concatenated bytes.
 */
async function collectUint8Array(value: unknown): Promise<Uint8Array> {
  if (
    value != null &&
    typeof (value as { [Symbol.asyncIterator]?: unknown })[
      Symbol.asyncIterator
    ] === "function"
  ) {
    const chunks: Uint8Array[] = [];
    for await (const chunk of value as AsyncIterable<unknown>) {
      chunks.push(toUint8Array(chunk));
    }
    return concatUint8Arrays(chunks);
  }

  if (
    value != null &&
    !(value instanceof Uint8Array) &&
    typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] ===
      "function"
  ) {
    const chunks = Array.from(value as Iterable<unknown>, (chunk) =>
      toUint8Array(chunk),
    );
    if (chunks.length > 0 && chunks.every((chunk) => chunk.byteLength > 0)) {
      return concatUint8Arrays(chunks);
    }
  }

  return toUint8Array(value);
}

/**
 * Concatenate byte chunks into a single {@link Uint8Array} sized to their total
 * length.
 *
 * @param chunks - Ordered byte chunks to join.
 * @returns One `Uint8Array` containing every chunk in order.
 */
function concatUint8Arrays(chunks: Uint8Array[]): Uint8Array {
  const size = chunks.reduce((total, chunk) => total + chunk.byteLength, 0);
  const output = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return output;
}

export class DatabaseService {
  metrics: MetricsServer;
  identityDatabases: Map<string, any>;
  databaseContexts: Map<string, any>;
  updateTimers: Map<string, any>;
  eventHandlers: Map<string, any>;
  syncInFlight: Map<string, InFlightRecord<void>>;
  openInFlight: Map<string, InFlightRecord<any>>;
  databaseUseCounts: Map<string, number>;
  pinnedOpenDatabases: Map<string, any>;
  pinQueue: PQueue;
  queuedImageCids: Set<string>;
  pinnedImageCids: Set<string>;
  isShuttingDown: boolean;
  orbitdb: any;
  ipfs: any;
  /** Count of sync attempts started (HTTP + pubsub; excludes duplicate coalesced waits). */
  pinningSyncOperations: number;
  pinningFailedSyncs: number;
  pinnedDatabasesByAddress: Map<string, PinnedDatabaseRecord>;
  knownDatabasesByAddress: Set<string>;
  /** Manifest `name` by OrbitDB address (for logs / pubsub). */
  orbitDbNameByAddress: Map<string, string>;
  /** Peers observed subscribing to a native OrbitDB database topic. */
  knownDatabasePeers: Map<string, Map<string, unknown>>;

  /**
   * Initialize all in-memory bookkeeping (metrics server, the caches/maps for
   * open databases, in-flight opens/syncs, use counts, pinned records and known
   * peers, and the bounded media-pin queue). Does not touch IPFS/OrbitDB — call
   * {@link DatabaseService.initialize} with a running IPFS node before use.
   */
  constructor() {
    this.metrics = new MetricsServer();
    this.identityDatabases = new Map();
    this.databaseContexts = new Map();
    this.updateTimers = new Map();
    this.eventHandlers = new Map();
    this.syncInFlight = new Map();
    this.openInFlight = new Map();
    this.databaseUseCounts = new Map();
    this.pinnedOpenDatabases = new Map();
    this.pinQueue = new PQueue({ concurrency: 4 });
    this.queuedImageCids = new Set();
    this.pinnedImageCids = new Set();
    this.isShuttingDown = false;
    this.pinningSyncOperations = 0;
    this.pinningFailedSyncs = 0;
    this.pinnedDatabasesByAddress = new Map();
    this.knownDatabasesByAddress = new Set();
    this.orbitDbNameByAddress = new Map();
    this.knownDatabasePeers = new Map();
  }

  /**
   * Record a peer seen subscribing to a database's native OrbitDB topic, keyed
   * by database address then peer id. These are later re-dialed by
   * {@link DatabaseService.reconnectKnownDatabasePeers} to re-trigger OrbitDB's
   * edge-triggered heads exchange. Ignores non-`/orbitdb/` addresses and peers
   * that stringify to nothing useful (`""` / `[object Object]`).
   *
   * @param dbAddress - The OrbitDB database address the peer subscribed to.
   * @param peer - The peer id (or object with `toString()`); stored as-is for
   * later dialing.
   */
  rememberDatabasePeer(dbAddress: string, peer: unknown): void {
    if (!dbAddress?.startsWith(ORBITDB_PREFIX) || peer == null) return;
    const peerId = (peer as any)?.toString?.() || String(peer);
    if (!peerId || peerId === "[object Object]") return;
    let peers = this.knownDatabasePeers.get(dbAddress);
    if (!peers) {
      peers = new Map();
      this.knownDatabasePeers.set(dbAddress, peers);
    }
    peers.set(peerId, peer);
  }

  /**
   * Re-dial every known subscriber of a database (hang up then dial) after the
   * database has been opened.
   *
   * OrbitDB's Sync bootstrap is edge-triggered: a writer only dials the heads
   * exchange when it observes a fresh `subscription-change` for the topic. When
   * the relay reopens a database whose subscribers are already connected, no
   * such edge fires. Forcing a reconnect makes libp2p re-notify OrbitDB's
   * native heads topology (registered during open) without needing a custom
   * data protocol. Best-effort and fault-tolerant: failures are logged, never
   * thrown, and the local peer is skipped.
   *
   * @param dbAddress - Address of the just-opened database whose subscribers to
   * reconnect.
   */
  private async reconnectKnownDatabasePeers(dbAddress: string): Promise<void> {
    const peers = this.knownDatabasePeers.get(dbAddress);
    const libp2p = (this.ipfs as any)?.libp2p;
    if (!peers?.size || !libp2p) return;

    await Promise.allSettled(
      Array.from(peers.entries(), async ([peerId, peer]) => {
        if (peerId === libp2p.peerId?.toString?.()) return;
        try {
          // OrbitDB registers its native heads topology while opening the DB.
          // Reconnecting an already-known subscriber after that registration
          // lets libp2p notify the topology without a custom data protocol.
          await libp2p.hangUp(peer).catch(() => {});
          await libp2p.dial(peer);
          syncLog("Reconnected known OrbitDB subscriber after database open:", {
            dbAddress,
            peerId,
          });
        } catch (error: any) {
          syncLog("Failed to reconnect known OrbitDB subscriber:", {
            dbAddress,
            peerId,
            error: error?.message || String(error),
          });
        }
      }),
    );
  }

  /** Resolved DB name from last successful manifest load for this address (if any). */
  getCachedDbName(dbAddress: string): string | undefined {
    return this.orbitDbNameByAddress.get(dbAddress);
  }

  /** Load manifest to populate {@link orbitDbNameByAddress} without sync logging (e.g. before pubsub subscribe log). */
  async prefetchManifestForLogging(dbAddress: string): Promise<void> {
    if (
      !dbAddress?.startsWith(ORBITDB_PREFIX) ||
      this.orbitDbNameByAddress.has(dbAddress)
    )
      return;
    try {
      await this.loadManifest(dbAddress, { quiet: true });
    } catch {
      // ignore
    }
  }

  /**
   * List the on-demand heads libp2p protocol ids for every database the relay
   * currently knows about (one per known address, of the form
   * `/orbitdb/heads/orbitdb/<hash>`). Used to advertise/handle the on-demand
   * heads protocol so peers can pull heads even after the relay closed the DB.
   *
   * @returns An array of protocol id strings.
   */
  getKnownHeadsProtocols(): string[] {
    return Array.from(this.knownDatabasesByAddress).map(
      (dbAddress) => `${ORBITDB_HEADS_PREFIX}${dbAddress}`,
    );
  }

  /**
   * Whether a libp2p protocol id is an on-demand heads protocol for a database
   * this relay knows about.
   *
   * @param protocol - A libp2p protocol id to test.
   * @returns True when it parses to a known `/orbitdb/` database address.
   */
  isKnownHeadsProtocol(protocol: string): boolean {
    const dbAddress = this.dbAddressFromHeadsProtocol(protocol);
    return dbAddress != null && this.knownDatabasesByAddress.has(dbAddress);
  }

  /**
   * Handle an inbound on-demand heads-protocol stream for a database the relay
   * has replicated but may have since closed.
   *
   * A browser reconnecting after downtime dials `/orbitdb/heads/<address>` to
   * pull the latest heads. The relay may not have that database open, so this
   * method (re)opens it just long enough to serve the request: it ensures a
   * connection back to the requesting peer, retains the database open, waits
   * for OrbitDB to register the real heads-protocol handler, invokes it, and
   * always releases the database afterward.
   *
   * @param protocol - The dialed heads protocol id (must map to a known DB).
   * @param context - libp2p stream context; `connection.remotePeer` identifies
   * the caller so the relay can dial back if needed.
   * @param getRegisteredHandler - Lookup for OrbitDB's own handler registered
   * for `protocol`; may throw until registration completes (polled with a
   * timeout).
   * @throws If no replicated database is known for `protocol`, or the handler
   * does not register within the timeout.
   */
  async handleOnDemandHeadsProtocol(
    protocol: string,
    context: { connection?: { remotePeer?: unknown } },
    getRegisteredHandler: (protocol: string) => any,
  ): Promise<void> {
    const dbAddress = this.dbAddressFromHeadsProtocol(protocol);
    if (dbAddress == null || !this.knownDatabasesByAddress.has(dbAddress)) {
      throw new Error(`No replicated database known for protocol ${protocol}`);
    }

    await this.ensurePeerConnection(context?.connection?.remotePeer);

    const db = await this.retainOpenDatabase(dbAddress);
    try {
      const handlerRecord = await this.waitForRegisteredHeadsHandler(
        protocol,
        getRegisteredHandler,
      );
      await handlerRecord.handler(context);
    } finally {
      await this.releaseOpenDatabase(dbAddress, db);
    }
  }

  /**
   * Build the callback bundle that backs the pinning HTTP API exposed by the
   * {@link MetricsServer}. The returned handlers are thin adapters over this
   * service's state:
   *   - `getStats` — totals for pinned databases, sync operations, failed syncs
   *     and pinned media CIDs.
   *   - `getDatabases` — all pinned-database records, or a single record looked
   *     up by (optionally URL-encoded) address.
   *   - `syncDatabase` — force a fresh sync of one address and return a
   *     structured result; if the first pass produces no local proof it retries
   *     once after topic/heads registration.
   *   - `streamPinnedCid` — gateway read of locally-pinned IPFS content.
   *
   * @returns A {@link PinningHttpHandlers} object wired to this instance.
   */
  createPinningHttpHandlers(): PinningHttpHandlers {
    return {
      getStats: () => ({
        totalPinned: this.pinnedDatabasesByAddress.size,
        syncOperations: this.pinningSyncOperations,
        failedSyncs: this.pinningFailedSyncs,
        pinnedMediaCids: Array.from(this.pinnedImageCids),
        timestamp: new Date().toISOString(),
      }),
      getDatabases: (opts?: { address?: string }) => {
        const raw = opts?.address?.trim();
        if (!raw) {
          const databases = Array.from(this.pinnedDatabasesByAddress.values());
          return { databases, total: databases.length };
        }
        let addr = raw;
        try {
          addr = decodeURIComponent(raw);
        } catch {
          /* keep raw */
        }
        const entry = this.pinnedDatabasesByAddress.get(addr);
        if (entry) {
          return { databases: [entry], total: 1 };
        }
        return { databases: [], total: 0 };
      },
      syncDatabase: async (dbAddress: string) => {
        try {
          let r = await this.syncAllOrbitDBRecordsWithResult(dbAddress, {
            requireFresh: true,
          });
          const hasLocalProof =
            (r.entryCount ?? 0) > 0 ||
            r.lastRecord != null ||
            r.extractedMediaCids.length > 0;
          if (r.success && !hasLocalProof) {
            syncLog(
              "Explicit sync produced no local records; starting one fresh pass after topic/heads registration:",
              dbAddress,
            );
            r = await this.syncAllOrbitDBRecordsWithResult(dbAddress, {
              requireFresh: true,
            });
          }
          if (!r.success) {
            return { ok: false, error: "Sync failed" };
          }
          return {
            ok: true,
            receivedUpdate: r.receivedUpdate,
            fallbackScanUsed: r.fallbackScanUsed,
            extractedMediaCids: r.extractedMediaCids,
            entryCount: r.entryCount,
            lastRecord: r.lastRecord,
            snapshotSource: r.snapshotSource,
            ...(r.coalesced ? { coalesced: true } : {}),
          };
        } catch (e: any) {
          return { ok: false, error: e?.message || String(e) };
        }
      },
      streamPinnedCid: (cidStr: string, pathWithin?: string) =>
        this.streamPinnedIpfsContent(cidStr, pathWithin),
    };
  }

  /**
   * GET `/ipfs/<cid>` — stream content only when the CID is pinned in Helia and all bytes come from the local blockstore.
   */
  private async streamPinnedIpfsContent(
    cidStr: string,
    pathWithin?: string,
  ): Promise<StreamPinnedCidResult> {
    if (!this.ipfs?.pins?.isPinned || !this.ipfs?.blockstore?.get) {
      return { ok: false, status: 503, error: "IPFS node not available" };
    }

    let cid: CID;
    try {
      cid = CID.parse(cidStr);
    } catch {
      return { ok: false, status: 400, error: "Invalid CID" };
    }

    let pinned: boolean;
    try {
      pinned = await this.ipfs.pins.isPinned(cid);
    } catch {
      return { ok: false, status: 500, error: "Pin check failed" };
    }
    if (!pinned) {
      return { ok: false, status: 404, error: "CID is not pinned locally" };
    }

    const ufs = unixfs(this.ipfs);
    const statOpts = {
      offline: true as const,
      ...(pathWithin ? { path: pathWithin } : {}),
    };

    try {
      const st = await ufs.stat(cid, statOpts);
      if (st.type === "directory") {
        return {
          ok: false,
          status: 400,
          error:
            "Directory download is not supported; specify a file path under the CID",
        };
      }
      const chunks = ufs.cat(cid, {
        offline: true,
        ...(pathWithin ? { path: pathWithin } : {}),
      });
      return { ok: true, contentType: "application/octet-stream", chunks };
    } catch {
      try {
        if (pathWithin) {
          return {
            ok: false,
            status: 404,
            error: "Content not available locally at path",
          };
        }
        const block = await this.ipfs.blockstore.get(cid, { offline: true });
        async function* single() {
          yield block;
        }
        return {
          ok: true,
          contentType: "application/octet-stream",
          chunks: single(),
        };
      } catch {
        return {
          ok: false,
          status: 404,
          error: "Content not available locally",
        };
      }
    }
  }

  /**
   * Same as {@link syncAllOrbitDBRecords} but returns structured result for HTTP `/pinning/sync`
   * and observability.
   */
  private async syncAllOrbitDBRecordsWithResult(
    dbAddress: string,
    options: { requireFresh?: boolean } = {},
  ): Promise<{
    success: boolean;
    receivedUpdate: boolean;
    fallbackScanUsed: boolean;
    extractedMediaCids: string[];
    entryCount: number | null;
    lastRecord: Record<string, unknown> | null;
    snapshotSource: string;
    coalesced?: boolean;
  }> {
    const empty = {
      success: false as const,
      receivedUpdate: false,
      fallbackScanUsed: false,
      extractedMediaCids: [] as string[],
      entryCount: null,
      lastRecord: null,
      snapshotSource: "not-synced",
    };
    if (this.isShuttingDown) return empty;
    const existing = this.syncInFlight.get(dbAddress);
    if (existing) {
      const ageMs = Date.now() - existing.startedAt;
      syncLog(
        ageMs < SYNC_IN_FLIGHT_STALE_MS
          ? "Sync already in progress for database, skipping duplicate request:"
          : "Sync still in progress past stale threshold, coalescing to avoid duplicate database open:",
        dbAddress,
      );
      if (options.requireFresh) {
        syncLog(
          "Explicit sync waiting for the in-progress sync before starting a fresh pass:",
          dbAddress,
        );
        await this.withTimeout(
          existing.promise,
          SYNC_IN_FLIGHT_STALE_MS,
          `In-progress sync for ${dbAddress}`,
        );
        // The owner removes its record in a finally block after resolving the
        // shared promise. Yield once so that cleanup runs before the fresh pass.
        await delay(0);
        if (this.syncInFlight.get(dbAddress) === existing) {
          this.syncInFlight.delete(dbAddress);
        }
        return await this.syncAllOrbitDBRecordsWithResult(dbAddress);
      }

      await this.waitForCoalescedInFlight(
        existing.promise,
        COALESCED_SYNC_WAIT_MS,
      );
    }

    const afterCoalesced = this.syncInFlight.get(dbAddress);
    if (afterCoalesced && afterCoalesced === existing) {
      const pinned = this.pinnedDatabasesByAddress.get(dbAddress);
      return {
        success: Boolean(pinned),
        receivedUpdate: false,
        fallbackScanUsed: false,
        extractedMediaCids: [],
        entryCount: pinned?.entryCount ?? null,
        lastRecord: pinned?.lastRecord ?? null,
        snapshotSource:
          pinned?.snapshotSource ?? "coalesced-without-pinned-record",
        coalesced: true,
      };
    }

    const syncPromise = (async (): Promise<{
      success: boolean;
      receivedUpdate: boolean;
      fallbackScanUsed: boolean;
      extractedMediaCids: string[];
      entryCount: number | null;
      lastRecord: Record<string, unknown> | null;
      snapshotSource: string;
    }> => {
      this.pinningSyncOperations++;
      this.rememberKnownDatabaseAddress(dbAddress);
      const manifest = await this.loadManifest(dbAddress);
      let dbName: string | null =
        typeof manifest?.name === "string" && manifest.name
          ? manifest.name
          : null;
      syncLog(
        "Starting sync for database:",
        inspect(
          { dbAddress, dbName },
          { depth: null, colors: false, compact: false },
        ),
      );
      const endTimer = this.metrics.startSyncTimer("all_databases");
      let db: any;
      let aclDb: any;
      let success = false;
      let receivedUpdate = false;
      let fallbackScanUsed = false;
      let extractedMediaCids: string[] = [];
      let snapshot: DatabaseSyncSnapshot = {
        entryCount: null,
        lastRecord: null,
        source: "not-snapshotted",
      };

      const aclDbAddress = this.normalizeOrbitdbAccessAddress(
        manifest?.accessController || null,
      );
      this.rememberKnownDatabaseAddress(aclDbAddress);

      try {
        aclDb = await this.preOpenAccessController(dbAddress, { manifest });
        syncLog(
          "Opening database:",
          inspect(
            { dbAddress, dbName },
            { depth: null, colors: false, compact: false },
          ),
        );
        db = await this.retainOpenDatabase(dbAddress);
        if (options.requireFresh) {
          await this.reconnectKnownDatabasePeers(dbAddress);
        }
        if (typeof db?.name === "string" && db.name) {
          dbName = db.name;
        }
        syncLog(
          "Opened database:",
          inspect(
            { dbAddress, dbName },
            { depth: null, colors: false, compact: false },
          ),
        );
        this.installAccessControllerDebugHooks(db, dbAddress);

        syncLog(
          "Waiting for database update event:",
          inspect(
            { dbAddress, dbName },
            { depth: null, colors: false, compact: false },
          ),
        );
        const { didReceiveUpdate, updates } = await this.waitForUpdateEvent(db);
        if (!didReceiveUpdate) {
          syncLog(
            "No update event received within timeout:",
            inspect(
              { dbAddress, dbName },
              { depth: null, colors: false, compact: false },
            ),
          );
        } else {
          syncLog(
            "Received update event for database:",
            inspect(
              { dbAddress, dbName },
              { depth: null, colors: false, compact: false },
            ),
            "updates:",
            updates.length,
          );
        }

        if (didReceiveUpdate) {
          receivedUpdate = true;
          const updateRecords = updates.map((entry) => ({
            value: entry?.payload?.value ?? entry?.value ?? entry,
          }));
          this.rememberKnownDatabaseAddressesFromRecords(updateRecords);
          const extractedEntries = this.extractImageCids(updateRecords);
          extractedMediaCids = extractedEntries.map((e) => e.cid);
          this.logDiscoveredMediaCids(dbAddress, extractedEntries, "updates");
          if (extractedEntries.length === 0 && updateRecords.length > 0) {
            this.logMediaExtractionMiss(
              dbAddress,
              dbName,
              "updates",
              updateRecords,
            );
          }
          this.enqueueImageCidsForPinning(extractedEntries, dbAddress);
        } else if (typeof db?.all === "function") {
          // HTTP sync often runs after the writer already replicated; no new `update` may fire.
          syncLog(
            "Falling back to db.all() scan for media CIDs:",
            inspect(
              { dbAddress, dbName },
              { depth: null, colors: false, compact: false },
            ),
          );
          try {
            const all = await db.all();
            const rows = Array.isArray(all) ? all : [];
            const scanRecords = rows.map((row: any) => ({
              value: row?.value ?? row,
            }));
            this.rememberKnownDatabaseAddressesFromRecords(scanRecords);
            const scanEntries = this.extractImageCids(scanRecords);
            extractedMediaCids = scanEntries.map((e) => e.cid);
            if (rows.length === 0) {
              syncLog(
                "db.all() fallback: 0 rows",
                inspect(
                  { dbAddress, dbName },
                  { depth: null, colors: false, compact: false },
                ),
              );
            } else {
              fallbackScanUsed = true;
              if (extractedMediaCids.length > 0) {
                this.logDiscoveredMediaCids(dbAddress, scanEntries, "db.all");
                this.enqueueImageCidsForPinning(scanEntries, dbAddress);
              } else {
                this.logMediaExtractionMiss(
                  dbAddress,
                  dbName,
                  "db.all",
                  scanRecords,
                );
              }
            }
          } catch (scanErr: any) {
            syncLog(
              "db.all() fallback failed:",
              inspect(
                { dbAddress, dbName },
                { depth: null, colors: false, compact: false },
              ),
              scanErr?.message || scanErr,
            );
          }
        }

        if (db) {
          snapshot = await this.snapshotLocalStateAfterSync(db, {
            updates,
            didReceiveUpdate,
          });
          if (loggingConfig.enableSyncLogs) {
            syncLog(
              "Sync local state summary: %s",
              inspect(
                {
                  dbAddress,
                  dbName,
                  entryCount: snapshot.entryCount,
                  lastRecord: snapshot.lastRecord,
                  snapshotSource: snapshot.source,
                },
                { depth: 10, colors: false, compact: false },
              ),
            );
          }
        }

        this.metrics.trackSync("documents", "success");
        endTimer();
        const existingPinned = this.pinnedDatabasesByAddress.get(dbAddress);
        const shouldKeepExistingPositiveProof =
          (existingPinned?.entryCount ?? 0) > 0 &&
          (snapshot.entryCount == null || snapshot.entryCount === 0);
        this.pinnedDatabasesByAddress.set(
          dbAddress,
          shouldKeepExistingPositiveProof
            ? {
                ...existingPinned!,
                lastSyncedAt: new Date().toISOString(),
              }
            : {
                address: dbAddress,
                lastSyncedAt: new Date().toISOString(),
                entryCount: snapshot.entryCount,
                lastRecord: snapshot.lastRecord,
                snapshotSource: snapshot.source,
              },
        );
        const hasReplicatedState =
          (snapshot.entryCount ?? 0) > 0 ||
          snapshot.lastRecord != null ||
          receivedUpdate ||
          extractedMediaCids.length > 0;
        if (hasReplicatedState) {
          // A relay/pinner must keep a synchronized OrbitDB open. OrbitDB owns
          // the native heads protocol registration and identify-push
          // notifications. Do not promote an early empty discovery pass: it
          // must be allowed to close and reopen once writer heads are present.
          this.pinnedOpenDatabases.set(dbAddress, db);
          if (aclDb && aclDbAddress) {
            this.pinnedOpenDatabases.set(aclDbAddress, aclDb);
          }
          if (receivedUpdate) {
            await this.republishHeadsToSubscribers(dbAddress, db, dbName);
          }
        }
        success = true;
      } catch (err: any) {
        this.pinningFailedSyncs++;
        this.metrics.trackSync("documents", "failure");
        endTimer();
        if (loggingConfig.logLevels.database) {
          // eslint-disable-next-line no-console
          console.error("Failed to sync database:", err);
        }
      } finally {
        if (db && this.pinnedOpenDatabases.get(dbAddress) !== db) {
          await this.releaseOpenDatabase(dbAddress, db);
        }
        if (
          aclDb &&
          aclDb !== db &&
          aclDbAddress &&
          this.pinnedOpenDatabases.get(aclDbAddress) !== aclDb
        ) {
          await this.releaseOpenDatabase(aclDbAddress, aclDb);
        }
      }

      return {
        success,
        receivedUpdate,
        fallbackScanUsed,
        extractedMediaCids,
        entryCount: snapshot.entryCount,
        lastRecord: snapshot.lastRecord,
        snapshotSource: snapshot.source,
      };
    })();

    const inFlightPromise = syncPromise.then(() => {});
    this.syncInFlight.set(dbAddress, {
      promise: inFlightPromise,
      startedAt: Date.now(),
    });
    try {
      return await syncPromise;
    } finally {
      if (this.syncInFlight.get(dbAddress)?.promise === inFlightPromise) {
        this.syncInFlight.delete(dbAddress);
      }
    }
  }

  /**
   * Bootstrap the OrbitDB stack on top of a running IPFS/Helia node.
   *
   * Registers the identity providers the todo app uses — `did` (with a
   * key-DID resolver), `webauthn` (worker WebAuthn + keystore) and
   * `webauthn-varsig` (hardware varsig) — on top of the `publickey` provider
   * built into @orbitdb/core, and the access controllers `ipfs`
   * ({@link IPFSAccessController}), `todo-delegation`
   * ({@link DelegatedTodoAccessController}) and `orbitdb-deferred`
   * ({@link DeferredOrbitDBAccessController}) alongside core's built-in
   * `orbitdb` controller. It then creates the OrbitDB instance with a relay
   * identities object that adds a `verifyIdentityFallback`: the relay must
   * verify writers signed under mixed modes (e.g. varsig plus non-varsig DID
   * signatures), and the DID provider's `verifyIdentity` throws or leaks
   * unhandled rejections for non-`did` shapes, so the fallback only runs DID
   * JWS verification for actual `did` identities.
   *
   * @param ipfs - A running IPFS/Helia node (must expose `libp2p`, `blockstore`
   * and `pins`). Stored on `this.ipfs`.
   * @param directory - Optional OrbitDB storage directory; defaults to
   * OrbitDB's own default when omitted.
   */
  async initialize(ipfs: any, directory?: string) {
    OrbitDBIdentityProviderDID.setDIDResolver(KeyDIDResolver.getResolver());
    useIdentityProvider(OrbitDBIdentityProviderDID as any);
    // Worker WebAuthn + keystore (type: webauthn); hardware varsig (type: webauthn-varsig)
    useIdentityProvider(OrbitDBWebAuthnIdentityProviderFunction as any);
    useIdentityProvider({
      type: "webauthn-varsig",
      verifyIdentity: verifyVarsigIdentity,
    } as any);
    useAccessController(IPFSAccessController as any);
    useAccessController(DelegatedTodoAccessController as any);
    useAccessController(DeferredOrbitDBAccessController as any);
    this.ipfs = ipfs;

    // Add a fallback verifier for mixed writer modes (e.g. varsig + non-varsig DID signatures).
    const baseIdentities = await Identities({ ipfs });
    const relayIdentities = {
      ...baseIdentities,
      // Only run DID JWS verification for `did` identities. The DID provider's verifyIdentity
      // builds a JWS from signatures.publicKey; calling it for webauthn / varsig shapes hits
      // dids ("No kid found in jws") or unhandled rejections because verifyJWS is not awaited upstream.
      verifyIdentityFallback: createRelayVerifyIdentityFallback(
        defaultRelayVerifyIdentityDeps(),
      ),
    };

    this.orbitdb = await createOrbitDB({
      ipfs,
      identities: relayIdentities,
      ...(directory ? { directory } : {}),
    });
  }

  /**
   * Extract every media CID referenced by a single record payload, together
   * with which field(s) referenced each one.
   *
   * The todo app stores media references under many shapes across app versions,
   * so this checks a broad set of fields: `imageCid`/`imageCID`/`image.cid`,
   * the various `profilePicture*` forms (including the `_id`/`value` keyvalue
   * shape), `mediaId`, `mediaIds[]`, and generic
   * `cid`/`contentCid`/`ipfsCid`/`mediaCid`/`thumbnailCid`. It also recurses
   * into a nested `value` that is either an object or a JSON-encoded string,
   * up to a depth of 4, prefixing discovered sources with `value.`/`value(json).`
   * so logs show provenance. CIDs are de-duplicated across all matching fields.
   *
   * @param payload - A record value (or nested value) to scan.
   * @param depth - Internal recursion guard; callers pass 0.
   * @returns One {@link ExtractedMediaCid} per distinct CID, each with its
   * sorted list of source field names.
   */
  private extractImageCidsFromPayload(
    payload: any,
    depth = 0,
  ): ExtractedMediaCid[] {
    const maxDepth = 4;
    const byCid = new Map<string, Set<string>>();
    const add = (raw: unknown, source: string) => {
      if (typeof raw !== "string" || raw.length === 0) return;
      if (!byCid.has(raw)) byCid.set(raw, new Set());
      byCid.get(raw)!.add(source);
    };

    if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
      return [];
    }

    const imageCid =
      payload?.imageCid ?? payload?.imageCID ?? payload?.image?.cid;
    if (typeof imageCid === "string" && imageCid.length > 0) {
      const source = payload?.imageCid
        ? "imageCid"
        : payload?.imageCID
          ? "imageCID"
          : "image.cid";
      add(imageCid, source);
    }

    const profilePictureCid =
      payload?.profilePicture ??
      payload?.profilePictureCid ??
      payload?.profilePictureCID ??
      (payload?._id === "profilePicture" ||
      payload?._id === "profilePictureCid" ||
      payload?._id === "profilePictureCID"
        ? payload?.value
        : undefined);
    if (typeof profilePictureCid === "string" && profilePictureCid.length > 0) {
      const source = payload?.profilePicture
        ? "profilePicture"
        : payload?.profilePictureCid
          ? "profilePictureCid"
          : payload?.profilePictureCID
            ? "profilePictureCID"
            : "profilePicture(_id/value)";
      add(profilePictureCid, source);
    }

    const mediaId = payload?.mediaId;
    if (typeof mediaId === "string" && mediaId.length > 0)
      add(mediaId, "mediaId");

    const mediaIds = Array.isArray(payload?.mediaIds) ? payload.mediaIds : [];
    for (let i = 0; i < mediaIds.length; i++) {
      add(mediaIds[i], `mediaIds[${i}]`);
    }

    const genericCid =
      payload.cid ??
      payload.contentCid ??
      payload.ipfsCid ??
      payload.mediaCid ??
      payload.thumbnailCid;
    if (typeof genericCid === "string" && genericCid.length > 0) {
      const source = payload.cid
        ? "cid"
        : payload.contentCid
          ? "contentCid"
          : payload.ipfsCid
            ? "ipfsCid"
            : payload.mediaCid
              ? "mediaCid"
              : "thumbnailCid";
      add(genericCid, source);
    }

    const rawValue = payload.value;
    if (
      depth < maxDepth &&
      typeof rawValue === "string" &&
      rawValue.length > 0
    ) {
      try {
        const parsed = JSON.parse(rawValue) as unknown;
        if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
          for (const { cid, sources } of this.extractImageCidsFromPayload(
            parsed,
            depth + 1,
          )) {
            for (const s of sources) {
              add(cid, `value(json).${s}`);
            }
          }
        }
      } catch {
        // not JSON; ignore
      }
    } else if (
      depth < maxDepth &&
      rawValue &&
      typeof rawValue === "object" &&
      !Array.isArray(rawValue)
    ) {
      for (const { cid, sources } of this.extractImageCidsFromPayload(
        rawValue,
        depth + 1,
      )) {
        for (const s of sources) {
          add(cid, `value.${s}`);
        }
      }
    }

    return Array.from(byCid.entries()).map(([cid, sources]) => ({
      cid,
      sources: [...sources].sort(),
    }));
  }

  /**
   * Run {@link DatabaseService.extractImageCidsFromPayload} across many records
   * and merge the results, de-duplicating CIDs and unioning their source
   * field names across all records.
   *
   * @param records - Records to scan; each may be a raw value or a `{ value }`
   * wrapper.
   * @returns One {@link ExtractedMediaCid} per distinct CID found across all
   * records, with sorted merged sources.
   */
  private extractImageCids(records: any[]): ExtractedMediaCid[] {
    const byCid = new Map<string, Set<string>>();

    for (const record of records) {
      const payload = record?.value ?? record;
      for (const { cid, sources } of this.extractImageCidsFromPayload(
        payload,
      )) {
        if (!byCid.has(cid)) byCid.set(cid, new Set());
        for (const s of sources) byCid.get(cid)!.add(s);
      }
    }

    return Array.from(byCid.entries()).map(([cid, sources]) => ({
      cid,
      sources: [...sources].sort(),
    }));
  }

  /**
   * Emit a sync-log line listing the media CIDs discovered for a database and
   * where they came from.
   *
   * @param dbAddress - The database the CIDs were discovered in.
   * @param entries - The extracted CIDs with their source fields.
   * @param origin - Whether they came from live `update` events or a
   * `db.all()` fallback scan.
   */
  private logDiscoveredMediaCids(
    dbAddress: string,
    entries: ExtractedMediaCid[],
    origin: "updates" | "db.all",
  ) {
    syncLog(
      "Discovered media CIDs (%s) db=%s count=%d detail=%o",
      origin,
      dbAddress,
      entries.length,
      entries.map((e) => ({ cid: e.cid, sources: e.sources })),
    );
  }

  /**
   * Build a compact, log-safe debug summary of records when media-CID
   * extraction found nothing, to help diagnose why records did not match any
   * known media field.
   *
   * For up to `maxSamples` records it reports the top-level keys and a bounded
   * preview of the `value` field (string preview or object key list), plus the
   * full list of fields the extractor looks at. Does not include full payloads.
   *
   * @param records - The records that yielded no CIDs.
   * @param maxSamples - Maximum number of records to sample (default 3).
   * @returns A plain object with `recordCount`, `samples` and `fieldsWeMatch`.
   */
  private summarizeMediaExtractionDebug(records: any[], maxSamples = 3) {
    const samples: unknown[] = [];
    const n = Math.min(records.length, maxSamples);
    for (let i = 0; i < n; i++) {
      const r = records[i];
      const payload = r?.value ?? r;
      if (payload && typeof payload === "object" && !Array.isArray(payload)) {
        const keys = Object.keys(payload as object).sort();
        const v = (payload as any).value;
        const hint: Record<string, unknown> = {
          index: i,
          topLevelKeys: keys,
        };
        if (typeof v === "string") {
          hint.valueKind = "string";
          hint.valuePreview =
            v.length > 120 ? `${v.slice(0, 120)}…(${v.length} chars)` : v;
        } else if (v && typeof v === "object" && !Array.isArray(v)) {
          hint.valueKind = "object";
          hint.valueKeys = Object.keys(v as object).sort();
        } else if (v !== undefined) {
          hint.valueKind = typeof v;
        }
        samples.push(hint);
      } else {
        samples.push({
          index: i,
          payloadKind: payload === null ? "null" : typeof payload,
        });
      }
    }
    return {
      recordCount: records.length,
      samples,
      fieldsWeMatch: [
        "imageCid",
        "imageCID",
        "image.cid",
        "profilePicture*",
        "mediaId",
        "mediaIds[]",
        "cid",
        "contentCid",
        "ipfsCid",
        "mediaCid",
        "thumbnailCid",
        "value (nested object or JSON string with those fields inside)",
      ],
    };
  }

  /**
   * Emit a diagnostic sync-log line when records were present but no media CID
   * matched, using {@link DatabaseService.summarizeMediaExtractionDebug} for a
   * bounded hint. No-op for an empty record set.
   *
   * @param dbAddress - The database being scanned.
   * @param dbName - Resolved database name, if known (for the log).
   * @param origin - Whether the records came from `update` events or a
   * `db.all()` scan.
   * @param records - The records that failed to yield any CID.
   */
  private logMediaExtractionMiss(
    dbAddress: string,
    dbName: string | null,
    origin: "updates" | "db.all",
    records: any[],
  ) {
    if (records.length === 0) return;
    syncLog(
      "Media CID extraction: no CIDs matched (origin=%s, records=%d). Hint=%s",
      origin,
      records.length,
      inspect(
        { dbAddress, dbName, ...this.summarizeMediaExtractionDebug(records) },
        { depth: 8, colors: false, compact: false },
      ),
    );
  }

  /**
   * Produce a bounded, colorless `util.inspect` string of a value for sync
   * logs, capping depth, array length and string length so a large record does
   * not flood the logs. Falls back to `String(value)` if inspection throws.
   *
   * @param value - The value to render.
   * @param maxString - Maximum rendered string length (default 800).
   * @returns A single-line-safe preview string.
   */
  private previewForSyncLog(value: unknown, maxString = 800): string {
    try {
      return inspect(value, {
        depth: 5,
        maxArrayLength: 24,
        maxStringLength: maxString,
        breakLength: 100,
        colors: false,
        compact: false,
      });
    } catch {
      return String(value);
    }
  }

  /**
   * Summarize the last row returned by `db.all()` for the sync log, keeping
   * `hash`/`key` when present and a bounded preview of `value` (or the whole
   * row when it has no `value`).
   *
   * @param row - The final `db.all()` row (may be null / non-object).
   * @returns A small plain object safe to log.
   */
  private summarizeLastDbRowForSyncLog(row: any): Record<string, unknown> {
    if (row == null) return { row: null };
    if (typeof row !== "object") return { row: String(row) };
    const out: Record<string, unknown> = {};
    if (row.hash != null) out.hash = row.hash;
    if (row.key != null) out.key = row.key;
    if ("value" in row) {
      out.value = this.previewForSyncLog(row.value);
    } else {
      out.entry = this.previewForSyncLog(row);
    }
    return out;
  }

  /**
   * Summarize the last entry yielded by a `db.iterator()` for the sync log,
   * keeping identifying fields (`hash`/`key`/`id`/`clock`) and a bounded
   * preview of `payload`/`value` (or the whole entry when neither is present).
   *
   * @param entry - The final iterator entry (may be null / non-object).
   * @returns A small plain object safe to log.
   */
  private summarizeLastIteratorEntryForSyncLog(
    entry: any,
  ): Record<string, unknown> {
    if (entry == null) return { entry: null };
    if (typeof entry !== "object") return { entry: String(entry) };
    const out: Record<string, unknown> = {};
    for (const k of ["hash", "key", "id", "clock"]) {
      if (entry[k] != null) out[k] = entry[k];
    }
    if ("payload" in entry || "value" in entry) {
      out.value = this.previewForSyncLog(entry.payload ?? entry.value);
    } else {
      out.entry = this.previewForSyncLog(entry);
    }
    return out;
  }

  /**
   * Summarize the last entry seen in an `update`-event burst for the sync log:
   * the entry `hash`, a truncated `identity` hash, and a bounded preview of the
   * payload/value.
   *
   * @param entry - The final update-event entry.
   * @returns A small plain object safe to log.
   */
  private summarizeLastUpdateEntryForSyncLog(
    entry: any,
  ): Record<string, unknown> {
    const id = entry?.identity;
    return {
      hash: entry?.hash ?? null,
      identity:
        typeof id === "string"
          ? id.length > 28
            ? `${id.slice(0, 28)}…`
            : id
          : (id ?? null),
      payloadPreview: this.previewForSyncLog(
        entry?.payload ?? entry?.value ?? entry,
      ),
    };
  }

  /**
   * Capture a database's local state right after a sync pass as a replication
   * proof for logs and {@link PinnedDatabaseRecord}.
   *
   * Prefers the cheapest accurate source available: if an `update` burst was
   * observed it summarizes only that burst (entry count left null, since the
   * burst is not a full count); otherwise it counts via `db.all()`; failing
   * that it walks `db.iterator()` (capped at 100k entries); and if the database
   * exposes neither it reports an "unknown" source. Errors are captured into
   * the `source` string rather than thrown.
   *
   * @param db - The synced database instance.
   * @param ctx - The update context from {@link DatabaseService.waitForUpdateEvent}
   * (`updates` burst and whether any update arrived).
   * @returns A {@link DatabaseSyncSnapshot}-shaped result: entry count (or
   * null), summarized last record, and the source description.
   */
  private async snapshotLocalStateAfterSync(
    db: any,
    ctx: { updates: any[]; didReceiveUpdate: boolean },
  ): Promise<{
    entryCount: number | null;
    lastRecord: Record<string, unknown> | null;
    source: string;
  }> {
    if (ctx.didReceiveUpdate && ctx.updates.length > 0) {
      const last = ctx.updates[ctx.updates.length - 1];
      return {
        entryCount: null,
        lastRecord: this.summarizeLastUpdateEntryForSyncLog(last),
        source: `update-event burst only (n=${ctx.updates.length})`,
      };
    }

    if (typeof db?.all === "function") {
      try {
        const all = await db.all();
        const rows = Array.isArray(all) ? all : [];
        const last = rows.length > 0 ? rows[rows.length - 1] : null;
        return {
          entryCount: rows.length,
          lastRecord: last ? this.summarizeLastDbRowForSyncLog(last) : null,
          source: "db.all()",
        };
      } catch (e: any) {
        return {
          entryCount: null,
          lastRecord: null,
          source: `db.all() error: ${e?.message || e}`,
        };
      }
    }

    if (typeof db?.iterator === "function") {
      try {
        let count = 0;
        let last: any = null;
        for await (const entry of db.iterator()) {
          count++;
          last = entry;
          if (count >= 100_000) break;
        }
        return {
          entryCount: count,
          lastRecord: last
            ? this.summarizeLastIteratorEntryForSyncLog(last)
            : null,
          source: count >= 100_000 ? "iterator (stopped at 100k)" : "iterator",
        };
      } catch (e: any) {
        return {
          entryCount: null,
          lastRecord: null,
          source: `iterator error: ${e?.message || e}`,
        };
      }
    }

    return {
      entryCount: null,
      lastRecord: null,
      source: "unknown (no db.all / iterator / updates)",
    };
  }

  /**
   * Pin one media CID into the local Helia node, driving the `pins.add` async
   * generator to completion.
   *
   * Before pinning it probes the blockstore to log whether the root block is
   * already local (a pin may still fetch missing DAG parts from the network).
   * Throws on failure so the caller's pin queue can record the failure.
   *
   * @param imageCid - The CID string to pin (parsed to a {@link CID}).
   * @param ctx - Context for logging: the owning `dbAddress` and the field
   * `sources` that referenced this CID.
   */
  private async pinImageCid(
    imageCid: string,
    ctx: { dbAddress: string; sources?: string[] },
  ) {
    const cid = CID.parse(imageCid);
    let hadLocalBlock: boolean | undefined;
    if (typeof this.ipfs?.blockstore?.has === "function") {
      try {
        hadLocalBlock = await this.ipfs.blockstore.has(cid);
      } catch {
        hadLocalBlock = undefined;
      }
    }

    syncLog(
      "Media pin start: db=%s cid=%s sources=%o hadLocalBlock=%s note=%s",
      ctx.dbAddress,
      imageCid,
      ctx.sources ?? [],
      hadLocalBlock === undefined ? "unknown" : String(hadLocalBlock),
      hadLocalBlock === true
        ? "root block already local; pin may still fetch missing DAG parts"
        : "root block not local (or unknown); pin will fetch from network if available",
    );

    for await (const _ of this.ipfs.pins.add(cid)) {
      // consume the async generator to completion
    }

    syncLog("Media pin ok: db=%s cid=%s", ctx.dbAddress, imageCid);
  }

  /**
   * Queue discovered media CIDs onto the bounded pin queue (concurrency 4),
   * skipping any CID already pinned or already queued.
   *
   * Pinning runs asynchronously off the sync path: each task calls
   * {@link DatabaseService.pinImageCid}, moving the CID from
   * `queuedImageCids` to `pinnedImageCids` on success and logging (but
   * swallowing) failures so a bad CID never rejects the queue. No-op while
   * shutting down, when there are no entries, or when IPFS has no pin API.
   *
   * @param entries - Extracted media CIDs (with source fields) to pin.
   * @param dbAddress - The owning database address, for logging.
   */
  private enqueueImageCidsForPinning(
    entries: ExtractedMediaCid[],
    dbAddress: string,
  ) {
    if (!this.ipfs?.pins || entries.length === 0 || this.isShuttingDown) return;

    for (const { cid: imageCid, sources } of entries) {
      if (
        this.pinnedImageCids.has(imageCid) ||
        this.queuedImageCids.has(imageCid)
      ) {
        if (loggingConfig.logLevels.database) {
          syncLog(
            "Media CID skip (already pinned or queued): db=%s cid=%s",
            dbAddress,
            imageCid,
          );
        }
        continue;
      }

      this.queuedImageCids.add(imageCid);
      this.pinQueue
        .add(async () => {
          try {
            await this.pinImageCid(imageCid, { dbAddress, sources });
            this.pinnedImageCids.add(imageCid);
          } catch (err: any) {
            syncLog(
              "Media pin failed: db=%s cid=%s sources=%o error=%s",
              dbAddress,
              imageCid,
              sources,
              err?.message || String(err),
            );
            if (loggingConfig.logLevels.database) {
              // eslint-disable-next-line no-console
              console.error(
                `Failed to pin image CID ${imageCid}:`,
                err?.message || err,
              );
            }
          } finally {
            this.queuedImageCids.delete(imageCid);
          }
        })
        .catch(() => {
          // handled in task body
        });
    }
  }

  /**
   * Wait for the database to emit `update` events, collecting the whole initial
   * burst.
   *
   * Polls (100 ms) until the first `update` arrives or `timeoutMs` elapses,
   * then keeps collecting for as long as further updates keep arriving within
   * 300 ms of the last one (so a multi-entry replication burst is captured as a
   * unit). Returns immediately if the database has no event emitter, and stops
   * early on shutdown.
   *
   * @param db - The database to observe.
   * @param timeoutMs - Overall wait budget in ms (default 5000).
   * @returns Whether any update was received and the collected update entries.
   */
  private async waitForUpdateEvent(
    db: any,
    timeoutMs = 5000,
  ): Promise<{ didReceiveUpdate: boolean; updates: any[] }> {
    if (!db?.events?.on || !db?.events?.off)
      return { didReceiveUpdate: false, updates: [] };

    let didUpdate = false;
    const updates: any[] = [];
    let lastUpdateAt = 0;
    const onUpdate = (entry: any) => {
      didUpdate = true;
      lastUpdateAt = Date.now();
      if (entry) updates.push(entry);
    };

    db.events.on("update", onUpdate);
    try {
      const startedAt = Date.now();
      while (
        !didUpdate &&
        !this.isShuttingDown &&
        Date.now() - startedAt < timeoutMs
      ) {
        await delay(100);
      }

      // After first update, collect closely-following updates in the same sync burst.
      while (
        didUpdate &&
        !this.isShuttingDown &&
        Date.now() - startedAt < timeoutMs &&
        Date.now() - lastUpdateAt < 300
      ) {
        await delay(100);
      }
      return { didReceiveUpdate: didUpdate, updates };
    } finally {
      db.events.off("update", onUpdate);
    }
  }

  /**
   * Normalize a manifest `accessController` address to an openable `/orbitdb/`
   * database address, or null when it is not an OrbitDB-backed controller.
   *
   * A `/orbitdb-deferred/<hash>` controller address refers to the same
   * underlying OrbitDB keyvalue store as `/orbitdb/<hash>`, so it is rewritten
   * to the `/orbitdb/` form the relay can open. Plain `/orbitdb/` addresses are
   * returned unchanged; anything else (e.g. an `/ipfs/` controller) yields null.
   *
   * @param address - The manifest access-controller address (may be null).
   * @returns The openable `/orbitdb/` address, or null.
   */
  private normalizeOrbitdbAccessAddress(address: string | null): string | null {
    if (!address || typeof address !== "string") return null;
    if (address.startsWith(DEFERRED_ACL_PREFIX)) {
      return `${ORBITDB_PREFIX}${address.slice(DEFERRED_ACL_PREFIX.length)}`;
    }
    return address.startsWith(ORBITDB_PREFIX) ? address : null;
  }

  /**
   * Add a database address to the known-databases set (used to advertise the
   * on-demand heads protocol and to reopen on demand). Ignores anything that is
   * not an `/orbitdb/` address.
   *
   * @param dbAddress - Candidate address to remember.
   */
  private rememberKnownDatabaseAddress(
    dbAddress: string | null | undefined,
  ): void {
    if (
      typeof dbAddress !== "string" ||
      !dbAddress.startsWith(ORBITDB_PREFIX)
    ) {
      return;
    }

    this.knownDatabasesByAddress.add(dbAddress);
  }

  /**
   * Walk records (up to depth 4 through arrays and objects) and remember every
   * `/orbitdb/` address string found anywhere inside them.
   *
   * Todo records can embed references to other databases (e.g. per-list or
   * ACL addresses); discovering them lets the relay advertise their heads
   * protocols and reopen them on demand.
   *
   * @param records - Records to scan; each may be a raw value or a `{ value }`
   * wrapper.
   */
  private rememberKnownDatabaseAddressesFromRecords(records: any[]): void {
    const visit = (value: unknown, depth = 0) => {
      if (depth > 4 || value == null) return;

      if (typeof value === "string") {
        this.rememberKnownDatabaseAddress(value);
        return;
      }

      if (Array.isArray(value)) {
        for (const item of value) visit(item, depth + 1);
        return;
      }

      if (typeof value === "object") {
        for (const nested of Object.values(value as Record<string, unknown>)) {
          visit(nested, depth + 1);
        }
      }
    };

    for (const record of records) {
      visit(record?.value ?? record);
    }
  }

  /**
   * Parse the OrbitDB database address out of an on-demand heads protocol id.
   * Inverse of the mapping in {@link DatabaseService.getKnownHeadsProtocols}.
   *
   * @param protocol - A protocol id such as `/orbitdb/heads/orbitdb/<hash>`.
   * @returns The embedded `/orbitdb/<hash>` address, or null when the protocol
   * is not a well-formed heads protocol.
   */
  private dbAddressFromHeadsProtocol(protocol: string): string | null {
    if (
      typeof protocol !== "string" ||
      !protocol.startsWith(`${ORBITDB_HEADS_PREFIX}/`)
    )
      return null;
    const dbAddress = protocol.slice(ORBITDB_HEADS_PREFIX.length);
    return dbAddress.startsWith(ORBITDB_PREFIX) ? dbAddress : null;
  }

  /**
   * Poll for OrbitDB's own handler registration for a heads protocol.
   *
   * When the relay reopens a database to serve an on-demand heads request,
   * OrbitDB registers the protocol handler asynchronously during open. This
   * retries the provided lookup every 50 ms until it returns a handler or the
   * timeout elapses, then rethrows the last lookup error.
   *
   * @param protocol - The heads protocol id whose handler is awaited.
   * @param getRegisteredHandler - Lookup that returns the handler or throws
   * until it exists.
   * @param timeoutMs - Wait budget (default {@link ON_DEMAND_HEADS_HANDLER_TIMEOUT_MS}).
   * @returns The registered handler record.
   * @throws The last lookup error (or a timeout error) if none registers in
   * time.
   */
  private async waitForRegisteredHeadsHandler(
    protocol: string,
    getRegisteredHandler: (protocol: string) => any,
    timeoutMs = ON_DEMAND_HEADS_HANDLER_TIMEOUT_MS,
  ): Promise<any> {
    const startedAt = Date.now();
    let lastError: any = null;

    while (!this.isShuttingDown && Date.now() - startedAt < timeoutMs) {
      try {
        return getRegisteredHandler(protocol);
      } catch (error: any) {
        lastError = error;
        await delay(50);
      }
    }

    throw lastError ?? new Error(`Timed out waiting for handler ${protocol}`);
  }

  /**
   * Ensure there is a live libp2p connection back to a peer before serving its
   * on-demand heads request.
   *
   * If an open connection already exists it does nothing; otherwise it attempts
   * a best-effort dial. Never throws — dial failures are logged so the heads
   * exchange can still be attempted over whatever connection libp2p has.
   *
   * @param remotePeer - The peer to (re)connect to; null/undefined is a no-op.
   */
  private async ensurePeerConnection(remotePeer: unknown): Promise<void> {
    if (remotePeer == null) return;

    const libp2p = (this.ipfs as any)?.libp2p;
    if (
      !libp2p ||
      typeof libp2p.getConnections !== "function" ||
      typeof libp2p.dial !== "function"
    ) {
      return;
    }

    try {
      const existingConnections = libp2p.getConnections(remotePeer);
      if (
        Array.isArray(existingConnections) &&
        existingConnections.some(
          (connection: any) => connection?.status !== "closed",
        )
      ) {
        return;
      }
    } catch {
      // fall through and try a best-effort dial
    }

    const peerId = (remotePeer as any)?.toString?.() || String(remotePeer);
    try {
      await libp2p.dial(remotePeer);
      syncLog("Dialed peer back before on-demand heads open:", peerId);
    } catch (error: any) {
      syncLog(
        "Failed to dial peer back before on-demand heads open:",
        peerId,
        error?.message || String(error),
      );
    }
  }

  /**
   * Reduce a decoded OrbitDB manifest to the fields worth logging (name, type,
   * access-controller address, meta) alongside the address and manifest CID.
   *
   * @param manifest - The decoded manifest value (may be null-ish).
   * @param dbAddress - The database address the manifest belongs to.
   * @param cid - The manifest block CID string.
   * @returns A small plain object for logging.
   */
  private summarizeManifest(manifest: any, dbAddress: string, cid: string) {
    return {
      dbAddress,
      manifestCid: cid,
      name: manifest?.name || null,
      type: manifest?.type || null,
      accessController: manifest?.accessController || null,
      meta: manifest?.meta || null,
    };
  }

  /**
   * Load and decode an OrbitDB database manifest directly from the blockstore,
   * without opening the database.
   *
   * Parses the address, fetches the manifest block by CID (with a timeout so a
   * missing block cannot hang sync), and DAG-CBOR-decodes it. As a side effect
   * it caches the manifest `name` in {@link DatabaseService.orbitDbNameByAddress}
   * for later log lines. Used by sync to learn the name and access-controller
   * address up front. Fault-tolerant: returns null (and warns when database
   * logging is on) rather than throwing.
   *
   * @param dbAddress - The database address whose manifest to load.
   * @param options - `quiet` suppresses the manifest sync-log line (used by the
   * logging prefetch path).
   * @returns The decoded manifest object, or null on any failure.
   */
  private async loadManifest(
    dbAddress: string,
    options?: { quiet?: boolean },
  ): Promise<any | null> {
    try {
      const orbitAddress = parseAddress(dbAddress);
      const cid = CID.parse(orbitAddress.hash, base58btc);
      const bytes = await collectUint8Array(
        await this.withTimeout(
          this.ipfs.blockstore.get(cid),
          MANIFEST_LOAD_TIMEOUT_MS,
          `OrbitDB manifest load for ${dbAddress}`,
        ),
      );
      const { value } = await Block.decode({
        bytes,
        codec: dagCbor,
        hasher: sha256,
      });
      const manifest: any = value ?? null;
      const name = manifest?.name;
      if (typeof name === "string" && name.length > 0) {
        this.orbitDbNameByAddress.set(dbAddress, name);
      }
      if (!options?.quiet) {
        syncLog(
          "OrbitDB manifest:",
          inspect(this.summarizeManifest(manifest, dbAddress, cid.toString()), {
            depth: null,
            colors: false,
            compact: false,
          }),
        );
      }
      return manifest;
    } catch (error: any) {
      if (loggingConfig.logLevels.database) {
        // eslint-disable-next-line no-console
        console.warn("Failed to load OrbitDB manifest before sync:", {
          dbAddress,
          error: error?.message || String(error),
        });
      }
      return null;
    }
  }

  /**
   * Wait for the first sign of replication life on a database — either a peer
   * `join` or an `update` — used when pre-opening an access-controller DB so
   * writer permissions are populated before the main sync.
   *
   * Polls (100 ms) until activity is seen or the timeout elapses; `update`
   * takes precedence over `join` in the reported `activity`. Returns
   * immediately when the database has no event emitter, and stops early on
   * shutdown.
   *
   * @param db - The database to observe.
   * @param timeoutMs - Wait budget in ms (default 5000).
   * @returns Whether activity was seen and which kind (first `join`, upgraded
   * to `update` if one arrives).
   */
  private async waitForDatabaseActivity(
    db: any,
    timeoutMs = 5000,
  ): Promise<{
    didReceiveActivity: boolean;
    activity: "join" | "update" | null;
  }> {
    if (!db?.events?.on || !db?.events?.off) {
      return { didReceiveActivity: false, activity: null };
    }

    let didReceiveActivity = false;
    let activity: "join" | "update" | null = null;

    const onJoin = () => {
      didReceiveActivity = true;
      activity = activity || "join";
    };

    const onUpdate = () => {
      didReceiveActivity = true;
      activity = "update";
    };

    db.events.on("join", onJoin);
    db.events.on("update", onUpdate);

    try {
      const startedAt = Date.now();
      while (
        !didReceiveActivity &&
        !this.isShuttingDown &&
        Date.now() - startedAt < timeoutMs
      ) {
        await delay(100);
      }

      return { didReceiveActivity, activity };
    } finally {
      db.events.off("join", onJoin);
      db.events.off("update", onUpdate);
    }
  }

  /**
   * Poll a database's log until it has at least one head (i.e. its oplog is
   * non-empty), or the timeout elapses.
   *
   * Used when pre-opening an access-controller database: the relay needs the
   * ACL's heads present before it can verify writer permissions, otherwise
   * legitimate appends would be rejected. Errors while reading heads are
   * ignored between polls (250 ms).
   *
   * @param db - The database whose heads to await.
   * @param timeoutMs - Wait budget in ms (default 20000).
   * @returns Whether heads appeared, how many, and their hashes.
   */
  private async waitForDatabaseHeads(
    db: any,
    timeoutMs = 20000,
  ): Promise<{
    didReceiveHeads: boolean;
    headCount: number;
    headHashes: string[];
  }> {
    const startedAt = Date.now();

    while (!this.isShuttingDown && Date.now() - startedAt < timeoutMs) {
      try {
        if (db?.log?.heads) {
          const heads = await db.log.heads();
          const headHashes = heads
            .map((entry: any) => entry?.hash)
            .filter(Boolean);
          if (headHashes.length > 0) {
            return {
              didReceiveHeads: true,
              headCount: headHashes.length,
              headHashes,
            };
          }
        }
      } catch {}

      await delay(250);
    }

    return {
      didReceiveHeads: false,
      headCount: 0,
      headHashes: [],
    };
  }

  /**
   * Open the database's access-controller database *before* the main database
   * sync and wait for it to become usable.
   *
   * Access control for the todo app depends on entries stored in a separate
   * OrbitDB access-controller database. If the main database syncs before the
   * ACL has replicated its heads, the relay's `canAppend` checks reject
   * legitimate writers. This opens the ACL DB (retained), installs debug hooks,
   * then waits for both replication activity and non-empty heads (logging each
   * outcome) so permission checks during sync see current data.
   *
   * @param dbAddress - The main database whose access controller to pre-open.
   * @param options - `manifest` reuses an already-loaded manifest (otherwise it
   * is loaded); `timeoutMs` bounds the activity/heads waits (default 20000).
   * @returns The retained access-controller database, or null when the
   * database has no OrbitDB-backed access controller. Caller is responsible for
   * releasing it (see the sync `finally` block).
   */
  private async preOpenAccessController(
    dbAddress: string,
    options?: { manifest?: any | null; timeoutMs?: number },
  ): Promise<any | null> {
    const timeoutMs = options?.timeoutMs ?? 20000;
    const manifest =
      options != null && "manifest" in options
        ? options.manifest!
        : await this.loadManifest(dbAddress);
    const dbName =
      typeof manifest?.name === "string" && manifest.name
        ? manifest.name
        : null;
    const dbCtx = () =>
      inspect(
        { dbAddress, dbName },
        { depth: null, colors: false, compact: false },
      );
    const accessControllerAddress = this.normalizeOrbitdbAccessAddress(
      manifest?.accessController || null,
    );

    if (!accessControllerAddress) {
      syncLog(
        "No pre-openable OrbitDB access controller found for database:",
        dbCtx(),
      );
      return null;
    }

    syncLog(
      "Pre-opening access controller before database sync:",
      dbCtx(),
      "acl:",
      accessControllerAddress,
    );
    const aclDb = await this.retainOpenDatabase(accessControllerAddress);
    this.installAccessControllerDebugHooks(aclDb, accessControllerAddress);
    syncLog(
      "Access-controller state after open:",
      inspect(await this.snapshotDatabaseState(aclDb, "acl-open"), {
        depth: null,
        colors: false,
        compact: false,
      }),
    );

    const { didReceiveActivity, activity } = await this.waitForDatabaseActivity(
      aclDb,
      timeoutMs,
    );
    const headStatus = await this.waitForDatabaseHeads(aclDb, timeoutMs);
    if (!didReceiveActivity) {
      syncLog(
        "No access-controller activity received within timeout:",
        dbCtx(),
        accessControllerAddress,
        inspect(await this.snapshotDatabaseState(aclDb, "acl-timeout"), {
          depth: null,
          colors: false,
          compact: false,
        }),
      );
    } else {
      syncLog(
        "Access-controller activity observed before database sync:",
        dbCtx(),
        accessControllerAddress,
        "activity:",
        activity,
        inspect(await this.snapshotDatabaseState(aclDb, "acl-activity"), {
          depth: null,
          colors: false,
          compact: false,
        }),
      );
    }
    if (!headStatus.didReceiveHeads) {
      syncLog(
        "No access-controller heads received within timeout:",
        dbCtx(),
        accessControllerAddress,
        inspect(await this.snapshotDatabaseState(aclDb, "acl-head-timeout"), {
          depth: null,
          colors: false,
          compact: false,
        }),
      );
    } else {
      syncLog(
        "Access-controller heads became visible before database sync:",
        dbCtx(),
        accessControllerAddress,
        inspect(headStatus, { depth: null, colors: false, compact: false }),
      );
    }

    return aclDb;
  }

  /**
   * Open a database via OrbitDB, de-duplicating concurrent opens of the same
   * address.
   *
   * Opening the same LevelDB-backed database twice concurrently can corrupt or
   * error, so an in-flight open is shared: callers arriving while an open is
   * pending await the same promise (logging differently once the pending open
   * passes a staleness threshold). On completion it installs non-fatal error
   * handlers and clears the in-flight record.
   *
   * @param dbAddress - The address to open.
   * @returns The opened OrbitDB database instance.
   */
  private async openDatabase(dbAddress: string): Promise<any> {
    const existing = this.openInFlight.get(dbAddress);
    if (existing) {
      const ageMs = Date.now() - existing.startedAt;
      syncLog(
        ageMs < OPEN_IN_FLIGHT_STALE_MS
          ? "Open already in progress for database, waiting on existing open:"
          : "Open still in progress past stale threshold, waiting to avoid duplicate LevelDB open:",
        dbAddress,
      );
      return await existing.promise;
    }

    const openPromise = this.orbitdb.open(dbAddress);
    this.openInFlight.set(dbAddress, {
      promise: openPromise,
      startedAt: Date.now(),
    });
    try {
      const db = await openPromise;
      this.installNonFatalDatabaseErrorHandlers(db, dbAddress);
      return db;
    } finally {
      if (this.openInFlight.get(dbAddress)?.promise === openPromise) {
        this.openInFlight.delete(dbAddress);
      }
    }
  }

  /**
   * Wait for an in-flight sync to finish, but give up after `timeoutMs` so a
   * coalesced caller returns promptly with the pinned record rather than
   * blocking on a slow sync.
   *
   * @param promise - The in-flight sync's completion promise.
   * @param timeoutMs - Maximum time to wait before returning anyway.
   */
  private async waitForCoalescedInFlight(
    promise: Promise<void>,
    timeoutMs: number,
  ): Promise<void> {
    await Promise.race([promise, delay(timeoutMs)]);
  }

  /**
   * Race a promise against a timeout, rejecting with a labeled error if the
   * promise does not settle in time. The timer is always cleared so it cannot
   * keep the event loop alive after the race resolves.
   *
   * @typeParam T - The promise's resolved type.
   * @param promise - The operation to bound.
   * @param timeoutMs - Timeout in ms.
   * @param label - Human-readable label included in the timeout error message.
   * @returns The promise's resolved value.
   * @throws An `Error` (`"<label> timed out after <ms>ms"`) if the timeout wins.
   */
  private async withTimeout<T>(
    promise: Promise<T>,
    timeoutMs: number,
    label: string,
  ): Promise<T> {
    let timeout: ReturnType<typeof setTimeout> | null = null;
    try {
      return await Promise.race([
        promise,
        new Promise<never>((_, reject) => {
          timeout = setTimeout(
            () => reject(new Error(`${label} timed out after ${timeoutMs}ms`)),
            timeoutMs,
          );
        }),
      ]);
    } finally {
      if (timeout) clearTimeout(timeout);
    }
  }

  /**
   * Open a database and increment its reference (use) count.
   *
   * The relay opens the same database from several overlapping code paths
   * (main sync, ACL pre-open, on-demand heads). Reference counting via
   * {@link DatabaseService.databaseUseCounts} lets the last releaser close it
   * exactly once. Every `retainOpenDatabase` must be balanced by a
   * {@link DatabaseService.releaseOpenDatabase}.
   *
   * @param dbAddress - The address to open and retain.
   * @returns The opened database instance.
   */
  private async retainOpenDatabase(dbAddress: string): Promise<any> {
    const db = await this.openDatabase(dbAddress);
    this.databaseUseCounts.set(
      dbAddress,
      (this.databaseUseCounts.get(dbAddress) ?? 0) + 1,
    );
    return db;
  }

  /**
   * Decrement a database's reference count and close it once the last holder
   * releases it. Counterpart to {@link DatabaseService.retainOpenDatabase}.
   *
   * When the count drops to zero the database is closed silently; otherwise the
   * count is just decremented (another holder still needs it). Databases the
   * relay decides to keep open for replication are tracked separately in
   * {@link DatabaseService.pinnedOpenDatabases} and are not released here.
   *
   * @param dbAddress - The address being released.
   * @param db - The database instance to close if this was the last reference.
   */
  private async releaseOpenDatabase(dbAddress: string, db: any): Promise<void> {
    const current = this.databaseUseCounts.get(dbAddress) ?? 0;
    if (current <= 1) {
      this.databaseUseCounts.delete(dbAddress);
      await this.closeDatabaseSilently(db);
      return;
    }

    this.databaseUseCounts.set(dbAddress, current - 1);
  }

  // NOTE: do NOT try to keep the relay pubsub-subscribed to a database topic
  // across closes (tried 2026-07-22). OrbitDB's Sync bootstrap is edge-
  // triggered: writers only dial the heads exchange when they observe a
  // `subscription-change` for the topic. A relay that stays subscribed makes
  // its next database open a no-op change — writers never dial, the relay
  // never receives heads, and every sync pass reports `receivedUpdate: false`
  // (reproduced by mocha/relay-media-replication.mjs going red). The durable
  // upstream fix is for Sync to enumerate peers that are already subscribed
  // when it starts, instead of relying on the change edge.

  /**
   * Push freshly synced heads onto the database's pubsub topic.
   *
   * OrbitDB only publishes an entry when the appending peer calls
   * `sync.add()`; heads received via the heads-exchange protocol are never
   * re-announced, and Sync's per-peer one-shot cache means a browser that
   * exchanged heads with this relay BEFORE the relay obtained the writer's
   * entry will never ask again. Observed live: writer A published into a
   * topic nobody heard, the relay received the entry moments later via A's
   * proof-triggered exchange, and reader B — subscribed and mesh-grafted the
   * whole time — stayed empty until the test's 120s timeout. Re-publishing
   * the current heads through OrbitDB's own `sync.add()` delivers them to
   * every current topic subscriber; receivers de-duplicate via `joinEntry`.
   * Gated on `receivedUpdate` by the caller so unchanged syncs publish
   * nothing (no relay-to-relay echo loops).
   */
  private async republishHeadsToSubscribers(
    dbAddress: string,
    db: any,
    dbName: string | null,
  ): Promise<void> {
    try {
      const heads = (await db?.log?.heads?.()) ?? [];
      for (const head of heads) {
        await db?.sync?.add?.(head);
      }
      if (heads.length > 0) {
        syncLog("Republished synced heads to topic subscribers:", {
          dbAddress,
          dbName,
          heads: heads.length,
        });
      }
    } catch (error: any) {
      syncLog(
        "Failed to republish synced heads:",
        dbAddress,
        error?.message || String(error),
      );
    }
  }

  /**
   * Close a database, swallowing any close error.
   *
   * Closing is best-effort during release/shutdown; a failed close must never
   * propagate and abort the surrounding cleanup.
   *
   * @param db - The database to close (null/undefined is a no-op).
   */
  private async closeDatabaseSilently(db: any): Promise<void> {
    try {
      await db?.close?.();
    } catch {
      // ignore close failures
    }
  }

  /**
   * Attach `error` listeners to a database's event emitters so OrbitDB's
   * non-fatal errors are logged instead of surfacing as unhandled
   * EventEmitter errors (which would crash the process).
   *
   * Hooks both `db.events` and, when distinct, `db.sync.events`. Uses a symbol
   * marker ({@link RELAY_ERROR_HANDLER_INSTALLED}) so re-opening the same
   * emitter does not stack duplicate handlers.
   *
   * @param db - The opened database instance.
   * @param dbAddress - The address, included in each logged error payload.
   */
  private installNonFatalDatabaseErrorHandlers(db: any, dbAddress: string) {
    const attach = (emitter: any, source: string) => {
      if (!emitter?.on) return;
      if (emitter[RELAY_ERROR_HANDLER_INSTALLED]) return;

      emitter.on("error", (error: any) => {
        const payload = {
          dbAddress,
          dbName: db?.name || null,
          source,
          error: error?.message || String(error),
          stack: error?.stack || null,
        };
        // eslint-disable-next-line no-console
        console.error(
          "OrbitDB emitted a non-fatal error:",
          inspect(payload, { depth: null, colors: false, compact: false }),
        );
      });

      emitter[RELAY_ERROR_HANDLER_INSTALLED] = true;
    };

    attach(db?.events, "db.events");
    if (db?.sync?.events && db.sync.events !== db.events) {
      attach(db.sync.events, "db.sync.events");
    }
  }

  /**
   * Wrap a database's access controller `canAppend` to log *why* an append was
   * rejected — only when sync logging is enabled.
   *
   * Diagnosing "the relay silently dropped my write" is hard without knowing
   * which permission check failed. This wrapper leaves the allow/deny decision
   * untouched (it returns exactly what the original `canAppend` returns) but,
   * on a rejection, logs the writer identity/key, the payload op/key, decoded
   * delegation-action fields, and identity-verification debug from
   * {@link DatabaseService.collectDatabaseIdentityDebug}. Guarded by a
   * `__debugCanAppendWrapped` flag so it is installed at most once per access
   * controller. Failures to install are logged and swallowed.
   *
   * @param db - The opened database whose access controller to instrument.
   * @param dbAddress - The address, included in the debug logs.
   */
  private installAccessControllerDebugHooks(db: any, dbAddress: string) {
    if (!loggingConfig.enableSyncLogs) return;
    try {
      const access = db?.access;
      const canAppend = access?.canAppend;
      if (typeof canAppend !== "function" || access.__debugCanAppendWrapped)
        return;

      const wrapped = async (entry: any) => {
        const allowed = await canAppend.call(access, entry);
        if (!allowed) {
          const payload = entry?.payload || null;
          const value = payload?.value || null;
          const writerIdentityHash = entry?.identity || null;
          const writerKey = entry?.key || null;
          const identityDebug = await this.collectDatabaseIdentityDebug(
            db,
            writerIdentityHash,
          );
          // eslint-disable-next-line no-console
          console.warn(
            "🚫 Relay AC rejected append",
            inspect(
              {
                dbAddress,
                dbName: db?.name || null,
                accessType: access?.type || null,
                writerIdentityHash,
                writerKey,
                payloadOp: payload?.op || null,
                payloadKey: payload?.key || null,
                valueType: value?.type || null,
                valueAction: value?.action || null,
                valueTaskKey: value?.taskKey || null,
                valueDelegateDid: value?.delegateDid || null,
                valuePerformedBy: value?.performedBy || null,
                valueExpiresAt: value?.expiresAt || null,
                identityDebug,
              },
              { depth: null, colors: false, compact: false },
            ),
          );
        }
        return allowed;
      };

      access.canAppend = wrapped;
      access.__debugCanAppendWrapped = true;
      // eslint-disable-next-line no-console
      console.log("🔍 Relay AC debug hook installed", {
        dbAddress,
        dbName: db?.name || null,
        accessType: access?.type || null,
      });
    } catch (error: any) {
      // eslint-disable-next-line no-console
      console.warn("⚠️ Failed to install relay AC debug hook", {
        dbAddress,
        error: error?.message || String(error),
      });
    }
  }

  /**
   * Resolve an identity by its hash and report how it verifies, for
   * append-rejection diagnostics.
   *
   * Looks the identity up in OrbitDB's identities store and records both the
   * base `verifyIdentity` result (and any error) and the relay's
   * fallback-verifier result, so mismatches between the two — the usual cause
   * of rejected writes across mixed identity modes — are visible. For
   * `worker-ed25519` identities it also attaches extra worker debug. Never
   * throws: all failures are folded into the returned object.
   *
   * @param hash - The identity hash to inspect (null/undefined yields null).
   * @returns A debug object describing the identity and its verification, or
   * null when no hash was given.
   */
  private async inspectIdentityHash(hash: string | null | undefined) {
    if (!hash) return null;
    try {
      const identity = await this.orbitdb?.identities?.getIdentity?.(hash);
      if (!identity) {
        return { hash, found: false };
      }

      let baseVerified: boolean | null = null;
      let baseError: string | null = null;
      try {
        baseVerified = await this.orbitdb.identities.verifyIdentity(identity);
      } catch (error: any) {
        baseError = error?.message || String(error);
      }

      let fallbackVerified: boolean | null = null;
      try {
        fallbackVerified = await verifyIdentityWithFallback(
          this.orbitdb.identities,
          identity,
        );
      } catch (error: any) {
        return {
          hash,
          found: true,
          id: identity.id || null,
          type: identity.type || null,
          baseVerified,
          baseError,
          fallbackVerified: null,
          fallbackError: error?.message || String(error),
        };
      }

      return {
        hash,
        found: true,
        id: identity.id || null,
        type: identity.type || null,
        baseVerified,
        baseError,
        fallbackVerified,
        workerEd25519Debug:
          identity?.type === "worker-ed25519"
            ? await inspectWorkerEd25519Identity(identity)
            : null,
      };
    } catch (error: any) {
      return {
        hash,
        found: false,
        error: error?.message || String(error),
      };
    }
  }

  /**
   * Collect up to `limit` distinct writer identity hashes from a database's
   * oplog and inspect each via {@link DatabaseService.inspectIdentityHash}.
   *
   * Used by append-rejection diagnostics to show which writers appear in a
   * database (or its ACL) and whether their identities verify. Fault-tolerant:
   * an iteration error is captured into the returned object.
   *
   * @param db - The database whose log to walk.
   * @param label - A label (e.g. `"db-log"`/`"acl-log"`) identifying this set.
   * @param limit - Maximum distinct identities to inspect (default 20).
   * @returns An object with the label and the inspected identities (or an
   * error).
   */
  private async collectIdentityHashesFromLog(
    db: any,
    label: string,
    limit = 20,
  ) {
    const hashes = new Set<string>();
    try {
      for await (const entry of db?.log?.iterator?.() || []) {
        if (typeof entry?.identity === "string") hashes.add(entry.identity);
        if (hashes.size >= limit) break;
      }
    } catch (error: any) {
      return {
        label,
        error: error?.message || String(error),
        identities: [],
      };
    }

    const identities = [];
    for (const hash of hashes) {
      identities.push(await this.inspectIdentityHash(hash));
    }

    return {
      label,
      count: identities.length,
      identities,
    };
  }

  /**
   * Capture a small, log-safe snapshot of a database's live state: address,
   * name, access-controller type, head count/hashes and connected-peer count.
   * Reads that fail are ignored so the snapshot never throws.
   *
   * @param db - The database to snapshot.
   * @param label - A label identifying the call site in logs.
   * @returns A plain object describing the current state.
   */
  private async snapshotDatabaseState(db: any, label: string) {
    let headHashes: string[] = [];
    let headCount = 0;
    let peerCount = 0;

    try {
      if (db?.log?.heads) {
        const heads = await db.log.heads();
        headHashes = heads.map((entry: any) => entry?.hash).filter(Boolean);
        headCount = headHashes.length;
      }
    } catch {}

    try {
      peerCount = db?.peers?.size || 0;
    } catch {}

    return {
      label,
      address: db?.address?.toString?.() || db?.address || null,
      name: db?.name || null,
      type: db?.access?.type || null,
      headCount,
      headHashes,
      peerCount,
    };
  }

  /**
   * Assemble the full identity/state debug bundle logged when the relay's
   * access controller rejects an append.
   *
   * Combines the rejected writer's identity inspection, a snapshot of the
   * database state and its log identities, and — when the access controller
   * exposes a `debugDb` — the same for the ACL database, so a rejected write
   * can be traced to a specific identity/verification mismatch.
   *
   * @param db - The database that rejected the append.
   * @param writerIdentityHash - The rejected entry's writer identity hash.
   * @returns A composite debug object (writer, db state, per-log identities and
   * optional ACL state).
   */
  private async collectDatabaseIdentityDebug(
    db: any,
    writerIdentityHash: string | null,
  ) {
    const writer = await this.inspectIdentityHash(writerIdentityHash);
    const logs: any[] = [];
    logs.push(await this.collectIdentityHashesFromLog(db, "db-log"));

    const aclDebugDb = db?.access?.debugDb;
    if (aclDebugDb) {
      logs.push(await this.collectIdentityHashesFromLog(aclDebugDb, "acl-log"));
    }

    return {
      writer,
      state: await this.snapshotDatabaseState(db, "db"),
      logs,
      aclState: aclDebugDb
        ? await this.snapshotDatabaseState(aclDebugDb, "acl")
        : null,
    };
  }

  /**
   * Fire-and-forget sync of one database address (the public entry point used
   * by pubsub-triggered replication). Thin wrapper over
   * {@link DatabaseService.syncAllOrbitDBRecordsWithResult} that discards the
   * structured result; no-op while shutting down.
   *
   * @param dbAddress - The database address to sync.
   */
  async syncAllOrbitDBRecords(dbAddress: string) {
    if (this.isShuttingDown) return;
    await this.syncAllOrbitDBRecordsWithResult(dbAddress);
  }

  /**
   * Flip the shutdown flag so in-progress waits/loops
   * ({@link DatabaseService.waitForUpdateEvent}, sync passes, pin enqueue, …)
   * bail out early. Idempotent and cheap; call before {@link DatabaseService.stop}
   * or as an early shutdown signal.
   */
  beginShutdown() {
    this.isShuttingDown = true;
  }

  /**
   * Gracefully shut the service down: signal shutdown, drain and clear the
   * media pin queue, drop the maps of open/pinned databases and their use
   * counts, and stop the OrbitDB instance (swallowing any stop error). After
   * this the service must not be reused.
   */
  async stop() {
    this.beginShutdown();

    this.pinQueue.pause();
    this.pinQueue.clear();
    await this.pinQueue.onIdle();

    this.pinnedOpenDatabases.clear();
    this.databaseUseCounts.clear();

    try {
      await this.orbitdb?.stop?.();
    } catch {
      // ignore stop failures
    }
  }
}
