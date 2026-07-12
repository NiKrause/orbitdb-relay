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

function setupOrbitdbReplicationHandlers(
  libp2p: Libp2pFacade,
  databaseService: DatabaseService,
) {
  const syncQueue = new PQueue({ concurrency: 2 });
  const subscribedOrbitdbTopics = new Set<string>();
  const queuedOrbitdbSyncTopics = new Set<string>();
  const pubsub = libp2p.services.pubsub as any;
  let isShuttingDown = false;

  const ensureOrbitdbTopicSubscribed = async (topic: string) => {
    if (!topic?.startsWith("/orbitdb/")) return;
    if (subscribedOrbitdbTopics.has(topic)) return;

    try {
      await pubsub.subscribe(topic);
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

  const scheduleOrbitdbTopicSync = (topic: string) => {
    if (!topic?.startsWith("/orbitdb/") || queuedOrbitdbSyncTopics.has(topic))
      return;

    queuedOrbitdbSyncTopics.add(topic);
    syncQueue
      .add(async () => {
        try {
          await ensureOrbitdbTopicSubscribed(topic);
          await databaseService.syncAllOrbitDBRecords(topic);
        } finally {
          queuedOrbitdbSyncTopics.delete(topic);
        }
      })
      .catch((error: any) => {
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

  pubsub.addEventListener("message", pubsubMessageHandler);
  pubsub.addEventListener("subscription-change", subscriptionChangeHandler);

  return async () => {
    isShuttingDown = true;
    pubsub.removeEventListener("message", pubsubMessageHandler);
    pubsub.removeEventListener(
      "subscription-change",
      subscriptionChangeHandler,
    );
    syncQueue.pause();
    syncQueue.clear();
    await syncQueue.onIdle();
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
  private cleanupRegistrarHooks: (() => void) | null;
  private started: boolean;

  constructor(components: any, init: OrbitdbReplicationServiceInit) {
    this.components = components;
    this.init = init;
    this.libp2p = null;
    this.ipfsInstance = null;
    this.databaseService = null;
    this.cleanupSyncHandlers = null;
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
      const cleanupSyncHandlers = setupOrbitdbReplicationHandlers(
        libp2p,
        databaseService,
      );

      this.libp2p = libp2p;
      this.ipfsInstance = ipfs;
      this.databaseService = databaseService;
      this.cleanupSyncHandlers = cleanupSyncHandlers;
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
    return this.requireDatabaseService().createPinningHttpHandlers();
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
