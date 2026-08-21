import { createHeliaLight } from "helia";
import { withBitswap } from "@helia/bitswap";
import { withHTTP } from "@helia/http";
import { withLibp2p } from "@helia/libp2p";
import * as dagCbor from "@ipld/dag-cbor";
import * as dagJson from "@ipld/dag-json";
import { serviceDependencies } from "@libp2p/interface";
import * as json from "multiformats/codecs/json";
import { sha512 } from "multiformats/hashes/sha2";
import PQueue from "p-queue";
import { inspect } from "node:util";
import type { Blockstore } from "interface-blockstore";
import type { Datastore } from "interface-datastore";

import {
  getOrbitdbSubscribeTimeoutMs,
  getOrbitdbTopicSyncCooldownMs,
  getOrbitdbTopicSyncMaxCooldownMs,
  getOrbitdbTopicSyncTimeoutMs,
} from "../config/orbitdb-sync-env.js";
import type { PinningHttpHandlers } from "./metrics.js";
import { DatabaseService } from "./database.js";
import { syncLog } from "../utils/logger.js";

export type OrbitdbReplicationServiceInit = {
  datastore: Datastore;
  blockstore: Blockstore;
  orbitdbDirectory?: string;
};

export interface OrbitdbReplicationServiceApi {
  createPinningHttpHandlers(): PinningHttpHandlers;
  syncAllOrbitDBRecords(dbAddress: string): Promise<void>;
  readonly ipfs?: any | null;
  start(): Promise<void>;
  stop(): Promise<void>;
  afterStart?(): Promise<void>;
  beforeStop?(): Promise<void>;
}

function createHeliaWithLibp2p(
  libp2p: Libp2pFacade,
  init: Pick<OrbitdbReplicationServiceInit, "datastore" | "blockstore">,
): any {
  return withBitswap(
    withLibp2p(
      withHTTP(
        createHeliaLight({
          datastore: init.datastore,
          blockstore: init.blockstore,
          codecs: [dagCbor, dagJson, json],
          hashers: [sha512],
        }),
      ),
      libp2p as any,
    ),
  );
}

type StreamHandlerRecordLike = {
  handler: (context: any) => Promise<void>;
  options?: Record<string, unknown>;
};

type Libp2pFacade = {
  peerId: unknown;
  peerStore: unknown;
  contentRouting: unknown;
  peerRouting: unknown;
  metrics: unknown;
  logger: unknown;
  services: Record<string, unknown>;
  status: "started";
  isStarted: () => boolean;
  addEventListener: (type: string, listener: any) => void;
  removeEventListener: (type: string, listener: any) => void;
  dispatchEvent: (event: Event) => boolean;
  safeDispatchEvent?: (type: string, init?: Record<string, unknown>) => boolean;
  getConnections: (peerId?: unknown) => unknown[];
  getMultiaddrs: () => unknown[];
  getProtocols: () => string[];
  dial: (peer: unknown, options?: Record<string, unknown>) => Promise<any>;
  dialProtocol: (
    peer: unknown,
    protocols: string | string[],
    options?: Record<string, unknown>,
  ) => Promise<any>;
  hangUp: (peer: unknown, options?: Record<string, unknown>) => Promise<void>;
  handle: (
    protocols: string | string[],
    handler: unknown,
    options?: Record<string, unknown>,
  ) => Promise<void>;
  unhandle: (
    protocols: string | string[],
    options?: Record<string, unknown>,
  ) => Promise<void>;
  register: (
    protocol: string,
    topology: unknown,
    options?: Record<string, unknown>,
  ) => Promise<string>;
  unregister: (id: string) => void;
  isDialable: (
    multiaddr: unknown,
    options?: Record<string, unknown>,
  ) => Promise<boolean>;
  start: () => Promise<void>;
  stop: () => Promise<void>;
};

function createLibp2pServiceFacade(components: any): Libp2pFacade {
  const events = components.events;
  const services = new Proxy<Record<string, unknown>>(
    {},
    {
      get: (_target, prop) => {
        if (typeof prop !== "string") return undefined;
        return components[prop];
      },
    },
  );

  const dial = async (peer: unknown, options: Record<string, unknown> = {}) => {
    return await components.connectionManager.openConnection(peer, {
      priority: 75,
      ...options,
    });
  };

  return {
    peerId: components.peerId,
    peerStore: components.peerStore,
    contentRouting: components.contentRouting,
    peerRouting: components.peerRouting,
    metrics: components.metrics,
    logger: components.logger,
    services,
    status: "started",
    isStarted: () => true,
    addEventListener: events.addEventListener.bind(events),
    removeEventListener: events.removeEventListener.bind(events),
    dispatchEvent: events.dispatchEvent.bind(events),
    safeDispatchEvent: events.safeDispatchEvent?.bind(events),
    getConnections: (peerId?: unknown) =>
      components.connectionManager.getConnections(peerId),
    getMultiaddrs: () => components.addressManager.getAddresses(),
    getProtocols: () => components.registrar.getProtocols(),
    dial,
    dialProtocol: async (
      peer: unknown,
      protocols: string | string[],
      options: Record<string, unknown> = {},
    ) => {
      const connection = await dial(peer, options);
      const protocolList = Array.isArray(protocols) ? protocols : [protocols];
      return await connection.newStream(protocolList, options);
    },
    hangUp: async (peer: unknown, options?: Record<string, unknown>) => {
      await components.connectionManager.closeConnections(peer, options);
    },
    handle: async (
      protocols: string | string[],
      handler: unknown,
      options?: Record<string, unknown>,
    ) => {
      const protocolList = Array.isArray(protocols) ? protocols : [protocols];
      await Promise.all(
        protocolList.map(
          async (protocol) =>
            await components.registrar.handle(protocol, handler, options),
        ),
      );
    },
    unhandle: async (
      protocols: string | string[],
      options?: Record<string, unknown>,
    ) => {
      const protocolList = Array.isArray(protocols) ? protocols : [protocols];
      await Promise.all(
        protocolList.map(
          async (protocol) =>
            await components.registrar.unhandle(protocol, options),
        ),
      );
    },
    register: async (
      protocol: string,
      topology: unknown,
      options?: Record<string, unknown>,
    ) => {
      return await components.registrar.register(protocol, topology, options);
    },
    unregister: (id: string) => {
      components.registrar.unregister(id);
    },
    isDialable: async (
      multiaddr: unknown,
      options?: Record<string, unknown>,
    ) => {
      return await components.connectionManager.isDialable(multiaddr, options);
    },
    start: async () => {},
    stop: async () => {},
  };
}

/**
 * Await `promise`, but give up after `timeoutMs` and reject instead.
 *
 * The underlying work is not cancellable, so it keeps running detached; only the
 * waiting stops. Its rejection is handled here so dropping it cannot surface as
 * an unhandled rejection.
 */
async function withDeadline<T>(
  promise: Promise<T>,
  timeoutMs: number,
  label: string,
): Promise<T> {
  promise.catch(() => {});
  let timer: ReturnType<typeof setTimeout> | undefined;
  const expiry = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`${label} timed out after ${timeoutMs}ms`)),
      timeoutMs,
    );
    timer.unref?.();
  });
  try {
    return await Promise.race([promise, expiry]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

export type OrbitdbSyncQueueStats = {
  /** Topic syncs waiting for a free slot. */
  waiting: number;
  /** Topic syncs currently running. */
  active: number;
  /**
   * The topics in those slots and how long each has held one, longest first.
   * `active` alone cannot answer the question that matters when syncs stall —
   * *which* database is stuck — and answering it used to mean reading the
   * relay's journal on the host.
   */
  activeTopics: Array<{ topic: string; runningMs: number }>;
  /** Topics queued or running, i.e. deduped against re-announcement. */
  queuedTopics: number;
  /** Topics currently held back after a sync timed out or failed. */
  cooldownTopics: number;
  /** Syncs whose slot was reclaimed because they exceeded the timeout. */
  timedOutSyncs: number;
};

type OrbitdbReplicationHandlers = {
  cleanup: () => Promise<void>;
  getQueueStats: () => OrbitdbSyncQueueStats;
};

/**
 * Wire the relay's pubsub events to OrbitDB topic syncs.
 *
 * Exported for `mocha/orbitdb-sync-queue.mjs`, which drives it with a fake
 * pubsub and a database service that hangs on demand — the cheapest way to
 * reproduce a jammed queue without a real relay.
 */
export function setupOrbitdbReplicationHandlers(
  libp2p: Libp2pFacade,
  databaseService: DatabaseService,
): OrbitdbReplicationHandlers {
  const syncQueue = new PQueue({ concurrency: 2 });
  const subscribedOrbitdbTopics = new Set<string>();
  const queuedOrbitdbSyncTopics = new Set<string>();
  /** Topic -> when its task took a slot, for the `activeTopics` readout. */
  const activeSyncStartedAt = new Map<string, number>();
  /**
   * Earliest time a topic may re-enter the queue after its sync timed out or
   * threw. The delay doubles up to a cap, so a database that can never be
   * synced costs a slot occasionally instead of continuously.
   */
  const syncRetryBackoff = new Map<
    string,
    { notBefore: number; delayMs: number }
  >();
  const subscribeTimeoutMs = getOrbitdbSubscribeTimeoutMs();
  const syncTimeoutMs = getOrbitdbTopicSyncTimeoutMs();
  const syncCooldownMs = getOrbitdbTopicSyncCooldownMs();
  const syncMaxCooldownMs = getOrbitdbTopicSyncMaxCooldownMs();
  const pubsub = libp2p.services.pubsub as any;
  let isShuttingDown = false;
  let timedOutSyncs = 0;

  const ensureOrbitdbTopicSubscribed = async (topic: string) => {
    if (!topic?.startsWith("/orbitdb/")) return;
    if (subscribedOrbitdbTopics.has(topic)) return;

    try {
      // Defence in depth only. 0.10.7 added this believing it was where topic
      // syncs hung; it is not — gossipsub's subscribe is synchronous, so the
      // await cannot block. The real hang was the unbounded manifest fetch in
      // `prefetchManifestForLogging` below, fixed in 0.10.8. This deadline stays
      // because `pubsub` is untyped here and another implementation may well
      // return a promise.
      await withDeadline(
        pubsub.subscribe(topic),
        subscribeTimeoutMs,
        `pubsub subscribe for ${topic}`,
      );
      subscribedOrbitdbTopics.add(topic);
      await databaseService.prefetchManifestForLogging(topic);
      const dbName = databaseService.getCachedDbName(topic);
      syncLog(
        "Explicitly subscribed relay pubsub to OrbitDB topic:",
        inspect(dbName ? { topic, dbName } : { topic }, {
          depth: null,
          colors: false,
          compact: false,
        }),
      );
    } catch (error: any) {
      syncLog(
        "Failed to subscribe relay pubsub to OrbitDB topic:",
        topic,
        error?.message || String(error),
      );
    }
  };

  /**
   * Run one topic's sync, but stop *waiting* on it after `syncTimeoutMs`.
   *
   * The work itself cannot be cancelled — `syncAllOrbitDBRecords` bottoms out in
   * OrbitDB and Helia calls that take no abort signal — so on timeout it keeps
   * running detached and only the queue slot is handed back. That is the whole
   * point: two databases that never finish syncing used to hold both slots for
   * good, which silently switched off pubsub-driven discovery for the entire
   * relay. `POST /pinning/sync` kept answering throughout, because the HTTP
   * handler calls the database service directly and never touches this queue,
   * so from the outside the node looked healthy.
   *
   * @returns true when the sync finished inside the timeout.
   */
  const runTopicSyncWithTimeout = async (topic: string): Promise<boolean> => {
    const work = (async () => {
      await ensureOrbitdbTopicSubscribed(topic);
      await databaseService.syncAllOrbitDBRecords(topic);
    })();
    // The detached run outlives the race on timeout; keep its rejection handled.
    work.catch(() => {});

    let timer: ReturnType<typeof setTimeout> | undefined;
    const expiry = new Promise<"timed-out">((resolve) => {
      timer = setTimeout(() => resolve("timed-out"), syncTimeoutMs);
      timer.unref?.();
    });

    try {
      const outcome = await Promise.race([
        work.then(() => "completed" as const),
        expiry,
      ]);
      if (outcome === "completed") return true;

      timedOutSyncs++;
      syncLog(
        "Queued OrbitDB topic sync timed out, releasing its slot:",
        inspect(
          { topic, timeoutMs: syncTimeoutMs },
          { depth: null, colors: false, compact: false },
        ),
      );
      return false;
    } finally {
      if (timer !== undefined) clearTimeout(timer);
    }
  };

  const noteSyncSetback = (topic: string) => {
    const previousDelayMs = syncRetryBackoff.get(topic)?.delayMs ?? 0;
    const delayMs = Math.min(
      previousDelayMs > 0 ? previousDelayMs * 2 : syncCooldownMs,
      syncMaxCooldownMs,
    );
    syncRetryBackoff.set(topic, { notBefore: Date.now() + delayMs, delayMs });
  };

  const scheduleOrbitdbTopicSync = (topic: string) => {
    if (!topic?.startsWith("/orbitdb/") || queuedOrbitdbSyncTopics.has(topic))
      return;

    const backoff = syncRetryBackoff.get(topic);
    if (backoff !== undefined && Date.now() < backoff.notBefore) return;

    queuedOrbitdbSyncTopics.add(topic);
    syncQueue
      .add(async () => {
        activeSyncStartedAt.set(topic, Date.now());
        try {
          if (await runTopicSyncWithTimeout(topic)) {
            syncRetryBackoff.delete(topic);
          } else {
            noteSyncSetback(topic);
          }
        } finally {
          activeSyncStartedAt.delete(topic);
          queuedOrbitdbSyncTopics.delete(topic);
        }
      })
      .catch((error: any) => {
        noteSyncSetback(topic);
        syncLog(
          "Queued OrbitDB topic sync failed:",
          topic,
          error?.message || String(error),
        );
      });
  };

  const pubsubMessageHandler = (event: any) => {
    if (isShuttingDown) return;
    const msg = event.detail;
    if (typeof msg.topic === "string" && msg.topic.startsWith("/orbitdb/")) {
      const dbName = databaseService.getCachedDbName(msg.topic);
      syncLog(
        "Received pubsub message:",
        inspect(dbName ? { topic: msg.topic, dbName } : { topic: msg.topic }, {
          depth: null,
          colors: false,
          compact: false,
        }),
      );
    }
    if (msg.topic?.startsWith("/orbitdb/")) {
      scheduleOrbitdbTopicSync(msg.topic);
    }
  };

  const subscriptionChangeHandler = (event: any) => {
    if (isShuttingDown) return;
    if (event.detail?.subscriptions) {
      for (const subscription of event.detail.subscriptions) {
        if (subscription.topic?.startsWith("/orbitdb/")) {
          if (
            subscription.subscribe !== false &&
            event.detail?.peerId != null
          ) {
            databaseService.rememberDatabasePeer(
              subscription.topic,
              event.detail.peerId,
            );
          }
          scheduleOrbitdbTopicSync(subscription.topic);
        }
      }
    }
  };

  /**
   * Queue depth, exposed on `/pinning/stats`. A jammed queue used to be
   * indistinguishable from an idle one from outside the process: `connections`
   * looked healthy and `syncOperations` simply stopped moving, because the
   * counter is incremented inside the sync that never started.
   */
  const getQueueStats = (): OrbitdbSyncQueueStats => {
    const now = Date.now();
    let cooldownTopics = 0;
    for (const backoff of syncRetryBackoff.values()) {
      if (now < backoff.notBefore) cooldownTopics++;
    }
    const activeTopics = Array.from(activeSyncStartedAt.entries())
      .map(([topic, startedAt]) => ({ topic, runningMs: now - startedAt }))
      .sort((a, b) => b.runningMs - a.runningMs);
    return {
      waiting: syncQueue.size,
      active: syncQueue.pending,
      activeTopics,
      queuedTopics: queuedOrbitdbSyncTopics.size,
      cooldownTopics,
      timedOutSyncs,
    };
  };

  pubsub.addEventListener("message", pubsubMessageHandler);
  pubsub.addEventListener("subscription-change", subscriptionChangeHandler);

  return {
    getQueueStats,
    cleanup: async () => {
      isShuttingDown = true;
      pubsub.removeEventListener("message", pubsubMessageHandler);
      pubsub.removeEventListener(
        "subscription-change",
        subscriptionChangeHandler,
      );
      syncQueue.pause();
      syncQueue.clear();
      // Cleared tasks never reach their `finally`, so drop their markers here.
      queuedOrbitdbSyncTopics.clear();
      activeSyncStartedAt.clear();
      await syncQueue.onIdle();
    },
  };
}

function installOnDemandOrbitdbHeadsSupport(
  registrar: any,
  databaseService: DatabaseService,
): () => void {
  const originalGetProtocols = registrar.getProtocols?.bind(registrar);
  const originalGetHandler = registrar.getHandler?.bind(registrar);

  if (
    typeof originalGetProtocols !== "function" ||
    typeof originalGetHandler !== "function"
  ) {
    return () => {};
  }

  registrar.getProtocols = () => {
    return Array.from(
      new Set<string>([
        ...originalGetProtocols(),
        ...databaseService.getKnownHeadsProtocols(),
      ]),
    ).sort();
  };

  registrar.getHandler = (protocol: string): StreamHandlerRecordLike => {
    try {
      return originalGetHandler(protocol);
    } catch (error: any) {
      if (!databaseService.isKnownHeadsProtocol(protocol)) {
        throw error;
      }

      syncLog(
        "Providing on-demand handler for known OrbitDB heads protocol:",
        inspect({ protocol }, { depth: null, colors: false, compact: false }),
      );

      return {
        options: {
          runOnLimitedConnection: true,
        },
        handler: async (context: any) => {
          await databaseService.handleOnDemandHeadsProtocol(
            protocol,
            context,
            originalGetHandler,
          );
        },
      };
    }
  };

  return () => {
    registrar.getProtocols = originalGetProtocols;
    registrar.getHandler = originalGetHandler;
  };
}

class OrbitdbReplicationService implements OrbitdbReplicationServiceApi {
  readonly [serviceDependencies]: string[] = ["@libp2p/pubsub"];
  readonly [Symbol.toStringTag] = "@le-space/orbitdb-replication-service";

  private readonly components: any;
  private readonly init: OrbitdbReplicationServiceInit;
  private libp2p: Libp2pFacade | null;
  private ipfsInstance: any | null;
  private databaseService: DatabaseService | null;
  private cleanupSyncHandlers: (() => Promise<void>) | null;
  private syncQueueStats: (() => OrbitdbSyncQueueStats) | null;
  private cleanupRegistrarHooks: (() => void) | null;
  private started: boolean;

  constructor(components: any, init: OrbitdbReplicationServiceInit) {
    this.components = components;
    this.init = init;
    this.libp2p = null;
    this.ipfsInstance = null;
    this.databaseService = null;
    this.cleanupSyncHandlers = null;
    this.syncQueueStats = null;
    this.cleanupRegistrarHooks = null;
    this.started = false;
  }

  async start(): Promise<void> {}

  async afterStart(): Promise<void> {
    if (this.started) return;

    const libp2p = createLibp2pServiceFacade(this.components);
    const ipfs = await createHeliaWithLibp2p(libp2p, this.init).start();
    const databaseService = new DatabaseService();

    try {
      await databaseService.initialize(ipfs as any, this.init.orbitdbDirectory);
      const cleanupRegistrarHooks = installOnDemandOrbitdbHeadsSupport(
        this.components.registrar,
        databaseService,
      );
      const replicationHandlers = setupOrbitdbReplicationHandlers(
        libp2p,
        databaseService,
      );

      this.libp2p = libp2p;
      this.ipfsInstance = ipfs;
      this.databaseService = databaseService;
      this.cleanupSyncHandlers = replicationHandlers.cleanup;
      this.syncQueueStats = replicationHandlers.getQueueStats;
      this.cleanupRegistrarHooks = cleanupRegistrarHooks;
      this.started = true;
    } catch (error) {
      try {
        this.cleanupRegistrarHooks?.();
      } catch {
        // ignore cleanup failures
      }
      this.cleanupRegistrarHooks = null;
      databaseService.beginShutdown();
      try {
        await databaseService.stop();
      } catch {
        // ignore cleanup failures
      }
      try {
        await ipfs.stop();
      } catch {
        // ignore cleanup failures
      }
      throw error;
    }
  }

  async beforeStop(): Promise<void> {
    if (!this.started && this.databaseService == null && this.ipfs == null)
      return;

    const cleanupSyncHandlers = this.cleanupSyncHandlers;
    const cleanupRegistrarHooks = this.cleanupRegistrarHooks;
    const databaseService = this.databaseService;
    const ipfs = this.ipfsInstance;

    this.started = false;
    this.cleanupSyncHandlers = null;
    this.syncQueueStats = null;
    this.cleanupRegistrarHooks = null;
    this.databaseService = null;
    this.ipfsInstance = null;
    this.libp2p = null;

    databaseService?.beginShutdown();

    try {
      await cleanupSyncHandlers?.();
    } catch {
      // ignore cleanup failures
    }

    try {
      cleanupRegistrarHooks?.();
    } catch {
      // ignore cleanup failures
    }

    try {
      await databaseService?.stop();
    } catch {
      // ignore cleanup failures
    }

    try {
      await ipfs?.stop();
    } catch {
      // ignore cleanup failures
    }
  }

  async stop(): Promise<void> {}

  get ipfs(): any | null {
    return this.ipfsInstance;
  }

  createPinningHttpHandlers(): PinningHttpHandlers {
    const handlers = this.requireDatabaseService().createPinningHttpHandlers();
    // Read the queue stats per call, not once here: the handler set is built
    // as the relay wires up its HTTP server, and stays in use across restarts
    // of the replication service underneath it.
    return {
      ...handlers,
      getStats: () => {
        const syncQueue = this.syncQueueStats?.();
        const stats = handlers.getStats();
        return syncQueue == null ? stats : { ...stats, syncQueue };
      },
    };
  }

  async syncAllOrbitDBRecords(dbAddress: string): Promise<void> {
    await this.requireDatabaseService().syncAllOrbitDBRecords(dbAddress);
  }

  private requireDatabaseService(): DatabaseService {
    if (this.databaseService == null) {
      throw new Error("OrbitDB replication service is not started");
    }
    return this.databaseService;
  }
}

export function orbitdbReplicationService(init: OrbitdbReplicationServiceInit) {
  return (components: any): OrbitdbReplicationServiceApi => {
    return new OrbitdbReplicationService(components, init);
  };
}

export type { PinningHttpHandlers };
