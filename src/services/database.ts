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
type InFlightRecord<T> = {
  promise: Promise<T>;
  startedAt: number;
};
export type DatabaseSyncSnapshot = {
  entryCount: number | null;
  lastRecord: Record<string, unknown> | null;
  source: string;
};
export type PinnedDatabaseRecord = {
  address: string;
  lastSyncedAt: string;
  entryCount: number | null;
  lastRecord: Record<string, unknown> | null;
  snapshotSource: string;
};

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
  if (value != null && typeof (value as { slice?: unknown }).slice === "function") {
    return toUint8Array((value as { slice: () => unknown }).slice());
  }
  return new Uint8Array();
}

async function collectUint8Array(value: unknown): Promise<Uint8Array> {
  if (
    value != null &&
    typeof (value as { [Symbol.asyncIterator]?: unknown })[Symbol.asyncIterator] ===
      "function"
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

  constructor() {
    this.metrics = new MetricsServer();
    this.identityDatabases = new Map();
    this.databaseContexts = new Map();
    this.updateTimers = new Map();
    this.eventHandlers = new Map();
    this.syncInFlight = new Map();
    this.openInFlight = new Map();
    this.databaseUseCounts = new Map();
    this.pinQueue = new PQueue({ concurrency: 4 });
    this.queuedImageCids = new Set();
    this.pinnedImageCids = new Set();
    this.isShuttingDown = false;
    this.pinningSyncOperations = 0;
    this.pinningFailedSyncs = 0;
    this.pinnedDatabasesByAddress = new Map();
    this.knownDatabasesByAddress = new Set();
    this.orbitDbNameByAddress = new Map();
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

  getKnownHeadsProtocols(): string[] {
    return Array.from(this.knownDatabasesByAddress).map(
      (dbAddress) => `${ORBITDB_HEADS_PREFIX}${dbAddress}`,
    );
  }

  isKnownHeadsProtocol(protocol: string): boolean {
    const dbAddress = this.dbAddressFromHeadsProtocol(protocol);
    return dbAddress != null && this.knownDatabasesByAddress.has(dbAddress);
  }

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
          const r = await this.syncAllOrbitDBRecordsWithResult(dbAddress);
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
  private async syncAllOrbitDBRecordsWithResult(dbAddress: string): Promise<{
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
        if (db) {
          await this.releaseOpenDatabase(dbAddress, db);
        }
        if (aclDb && aclDb !== db && aclDbAddress) {
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

  async importOrbitDBIdentityBlock(payload: {
    hash?: unknown;
    bytes?: unknown;
  }): Promise<boolean> {
    if (
      !this.ipfs?.blockstore ||
      typeof payload?.hash !== "string" ||
      !Array.isArray(payload.bytes)
    ) {
      return false;
    }

    const cid = CID.parse(payload.hash, base58btc);
    if (await this.ipfs.blockstore.has(cid)) {
      return true;
    }

    await this.ipfs.blockstore.put(cid, toUint8Array(payload.bytes));
    syncLog("Imported OrbitDB identity block:", payload.hash);
    return true;
  }

  async importTodoDatabaseEntries(payload: {
    dbAddress?: unknown;
    entries?: unknown;
    records?: unknown;
    blocks?: unknown;
  }): Promise<DatabaseSyncSnapshot | null> {
    if (typeof payload?.dbAddress !== "string") return null;

    const dbAddress = payload.dbAddress;
    const entries = Array.isArray(payload.entries) ? payload.entries : [];
    const records = Array.isArray(payload.records) ? payload.records : [];
    const importedBlocks = await this.importIpfsBlocks(payload.blocks);
    if (entries.length === 0 && records.length === 0) return null;

    this.rememberKnownDatabaseAddress(dbAddress);
    const recordRows = records
      .filter((record: any) => record?.key && record?.value)
      .map((record: any) => ({ key: record.key, value: record.value }));
    const recordSnapshot =
      recordRows.length > 0
        ? {
            entryCount: recordRows.length,
            lastRecord: this.summarizeLastDbRowForSyncLog(
              recordRows[recordRows.length - 1],
            ),
            source: "todo-entry-exchange records",
          }
        : null;

    if (recordRows.length > 0) {
      const scanRecords = recordRows.map((row: any) => ({ value: row.value }));
      this.rememberKnownDatabaseAddressesFromRecords(scanRecords);
      const extractedEntries = this.extractImageCids(scanRecords);
      this.enqueueImageCidsForPinning(extractedEntries, dbAddress);
    }

    let dbSnapshot: DatabaseSyncSnapshot | null = null;
    let joinedEntries = 0;
    let failedEntries = 0;

    const bridgedSnapshot =
      recordSnapshot ??
      (entries.length > 0
        ? {
            entryCount: entries.length,
            lastRecord: this.summarizeLastIteratorEntryForSyncLog(
              entries[entries.length - 1],
            ),
            source: "todo-entry-exchange entries",
          }
        : null);

    if (!bridgedSnapshot) return null;

    this.pinnedDatabasesByAddress.set(dbAddress, {
      address: dbAddress,
      lastSyncedAt: new Date().toISOString(),
      entryCount: bridgedSnapshot.entryCount,
      lastRecord: bridgedSnapshot.lastRecord,
      snapshotSource: bridgedSnapshot.source,
    });

    syncLog(
      "Imported bridged OrbitDB todo entries:",
      inspect(
        {
          dbAddress,
          entries: entries.length,
          records: recordRows.length,
          importedBlocks,
          joinedEntries,
          failedEntries,
          entryCount: bridgedSnapshot.entryCount,
          snapshotSource: bridgedSnapshot.source,
          pendingDatabaseJoin: true,
        },
        { depth: null, colors: false, compact: false },
      ),
    );

    void (async () => {
      let db: any;

      try {
        db = await this.retainOpenDatabase(dbAddress);
        this.installAccessControllerDebugHooks(db, dbAddress);

        if (typeof db?.log?.joinEntry === "function") {
          for (const entry of entries) {
            if (!entry?.hash) continue;
            try {
              await db.log.joinEntry(entry);
              joinedEntries++;
            } catch (error: any) {
              failedEntries++;
              syncLog(
                "Failed to join bridged OrbitDB todo entry:",
                inspect(
                  {
                    dbAddress,
                    hash: entry?.hash ?? null,
                    identity: entry?.identity ?? null,
                    error: error?.message || String(error),
                  },
                  { depth: null, colors: false, compact: false },
                ),
              );
            }
          }
        }

        dbSnapshot = await this.snapshotLocalStateAfterSync(db, {
          updates: [],
          didReceiveUpdate: false,
        });
      } catch (error: any) {
        syncLog(
          "Failed to import bridged OrbitDB todo entries into local database:",
          inspect(
            { dbAddress, error: error?.message || String(error) },
            { depth: null, colors: false, compact: false },
          ),
        );
      } finally {
        if (db) {
          await this.releaseOpenDatabase(dbAddress, db);
        }
      }

      const snapshot =
        dbSnapshot?.entryCount != null && dbSnapshot.entryCount > 0
          ? dbSnapshot
          : bridgedSnapshot;

      this.pinnedDatabasesByAddress.set(dbAddress, {
        address: dbAddress,
        lastSyncedAt: new Date().toISOString(),
        entryCount: snapshot.entryCount,
        lastRecord: snapshot.lastRecord,
        snapshotSource: snapshot.source,
      });

      syncLog(
        "Finished bridged OrbitDB todo entry database join:",
        inspect(
          {
            dbAddress,
            entries: entries.length,
            records: recordRows.length,
            importedBlocks,
            joinedEntries,
            failedEntries,
            entryCount: snapshot.entryCount,
            snapshotSource: snapshot.source,
          },
          { depth: null, colors: false, compact: false },
        ),
      );
    })();

    return bridgedSnapshot;
  }

  private async importIpfsBlocks(blocks: unknown): Promise<number> {
    if (!this.ipfs?.blockstore || !Array.isArray(blocks)) return 0;

    let imported = 0;
    for (const block of blocks) {
      const candidate = block as any;
      if (
        typeof candidate?.hash !== "string" ||
        !Array.isArray(candidate.bytes)
      ) {
        continue;
      }

      try {
        const cid = CID.parse(candidate.hash, base58btc);
        if (await this.ipfs.blockstore.has(cid)) continue;

        await this.ipfs.blockstore.put(cid, toUint8Array(candidate.bytes));
        imported++;
      } catch (error: any) {
        syncLog(
          "Failed to import bridged IPFS block:",
          inspect(
            {
              hash: candidate.hash,
              error: error?.message || String(error),
            },
            { depth: null, colors: false, compact: false },
          ),
        );
      }
    }

    if (imported > 0) {
      syncLog("Imported bridged IPFS blocks:", imported);
    }

    return imported;
  }

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

  private normalizeOrbitdbAccessAddress(address: string | null): string | null {
    if (!address || typeof address !== "string") return null;
    if (address.startsWith(DEFERRED_ACL_PREFIX)) {
      return `${ORBITDB_PREFIX}${address.slice(DEFERRED_ACL_PREFIX.length)}`;
    }
    return address.startsWith(ORBITDB_PREFIX) ? address : null;
  }

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

  private dbAddressFromHeadsProtocol(protocol: string): string | null {
    if (
      typeof protocol !== "string" ||
      !protocol.startsWith(`${ORBITDB_HEADS_PREFIX}/`)
    )
      return null;
    const dbAddress = protocol.slice(ORBITDB_HEADS_PREFIX.length);
    return dbAddress.startsWith(ORBITDB_PREFIX) ? dbAddress : null;
  }

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

  private async loadManifest(
    dbAddress: string,
    options?: { quiet?: boolean },
  ): Promise<any | null> {
    try {
      const orbitAddress = parseAddress(dbAddress);
      const cid = CID.parse(orbitAddress.hash, base58btc);
      const bytes = await collectUint8Array(await this.withTimeout(
        this.ipfs.blockstore.get(cid),
        MANIFEST_LOAD_TIMEOUT_MS,
        `OrbitDB manifest load for ${dbAddress}`,
      ));
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

  private async waitForCoalescedInFlight(
    promise: Promise<void>,
    timeoutMs: number,
  ): Promise<void> {
    await Promise.race([promise, delay(timeoutMs)]);
  }

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

  private async retainOpenDatabase(dbAddress: string): Promise<any> {
    const db = await this.openDatabase(dbAddress);
    this.databaseUseCounts.set(
      dbAddress,
      (this.databaseUseCounts.get(dbAddress) ?? 0) + 1,
    );
    return db;
  }

  private async releaseOpenDatabase(dbAddress: string, db: any): Promise<void> {
    const current = this.databaseUseCounts.get(dbAddress) ?? 0;
    if (current <= 1) {
      this.databaseUseCounts.delete(dbAddress);
      await this.closeDatabaseSilently(db);
      return;
    }

    this.databaseUseCounts.set(dbAddress, current - 1);
  }

  private async closeDatabaseSilently(db: any): Promise<void> {
    try {
      await db?.close?.();
    } catch {
      // ignore close failures
    }
  }

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

  async syncAllOrbitDBRecords(dbAddress: string) {
    if (this.isShuttingDown) return;
    await this.syncAllOrbitDBRecordsWithResult(dbAddress);
  }

  beginShutdown() {
    this.isShuttingDown = true;
  }

  async stop() {
    this.beginShutdown();

    this.pinQueue.pause();
    this.pinQueue.clear();
    await this.pinQueue.onIdle();

    try {
      await this.orbitdb?.stop?.();
    } catch {
      // ignore stop failures
    }
  }
}
