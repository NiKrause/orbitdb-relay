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

const DEFAULT_ORBITDB_IDENTITY_EXCHANGE_TOPICS = [
  "simple-todo.orbitdb-identities",
];
const DEFAULT_TODO_ENTRY_EXCHANGE_TOPICS = [
  "simple-todo.orbitdb-todo-entries",
];
const DEFAULT_TODO_ENTRY_EXCHANGE_PROTOCOLS = [
  "/simple-todo/orbitdb-entries/1.0.0",
];
const BRIDGED_MESSAGE_REPUBLISH_MS = 5_000;

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
  const subscribedIdentityTopics = new Set<string>();
  const subscribedTodoEntryTopics = new Set<string>();
  const queuedOrbitdbSyncTopics = new Set<string>();
  const storedIdentityMessages = new Map<
    string,
    { topic: string; data: Uint8Array }
  >();
  const storedTodoEntryMessages = new Map<
    string,
    { topic: string; data: Uint8Array }
  >();
  const pubsub = libp2p.services.pubsub as any;
  let isShuttingDown = false;

  const identityExchangeTopics = getOrbitdbIdentityExchangeTopics();
  const todoEntryExchangeTopics = getTodoEntryExchangeTopics();
  const todoEntryExchangeProtocols = getTodoEntryExchangeProtocols();

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

  const ensureIdentityTopicSubscribed = async (topic: string) => {
    if (!topic || subscribedIdentityTopics.has(topic)) return;

    try {
      await pubsub.subscribe(topic);
      subscribedIdentityTopics.add(topic);
      syncLog("Subscribed relay pubsub to OrbitDB identity topic:", topic);
    } catch (error: any) {
      syncLog(
        "Failed to subscribe relay pubsub to OrbitDB identity topic:",
        topic,
        error?.message || String(error),
      );
    }
  };

  const ensureTodoEntryTopicSubscribed = async (topic: string) => {
    if (!topic || subscribedTodoEntryTopics.has(topic)) return;

    try {
      await pubsub.subscribe(topic);
      subscribedTodoEntryTopics.add(topic);
      syncLog("Subscribed relay pubsub to OrbitDB todo-entry topic:", topic);
    } catch (error: any) {
      syncLog(
        "Failed to subscribe relay pubsub to OrbitDB todo-entry topic:",
        topic,
        error?.message || String(error),
      );
    }
  };

  const importOrbitdbIdentityMessage = async (msg: any) => {
    try {
      const payload = JSON.parse(new TextDecoder().decode(msg.data));
      const imported =
        await databaseService.importOrbitDBIdentityBlock(payload);
      if (!imported) {
        syncLog(
          "Ignored invalid OrbitDB identity message on topic:",
          msg.topic,
        );
        return;
      }
      if (typeof payload?.hash === "string") {
        storedIdentityMessages.set(payload.hash, {
          topic: msg.topic,
          data: msg.data,
        });
      }
    } catch (error: any) {
      syncLog(
        "Failed to import OrbitDB identity from pubsub:",
        msg.topic,
        error?.message || String(error),
      );
    }
  };

  const importTodoEntryMessage = async (msg: any) => {
    try {
      const payload = JSON.parse(new TextDecoder().decode(msg.data));
      const imported = await databaseService.importTodoDatabaseEntries(payload);
      if (!imported) {
        syncLog("Ignored invalid OrbitDB todo-entry message on topic:", msg.topic);
        return;
      }
      if (typeof payload?.dbAddress === "string") {
        storedTodoEntryMessages.set(payload.dbAddress, {
          topic: msg.topic,
          data: msg.data,
        });
        scheduleOrbitdbTopicSync(payload.dbAddress);
      }
    } catch (error: any) {
      syncLog(
        "Failed to import OrbitDB todo entries from pubsub:",
        msg.topic,
        error?.message || String(error),
      );
    }
  };

  const importTodoEntryStream = async (event: any) => {
    try {
      const stream = event?.stream ?? event;
      const bytes = await readStreamBytes(stream.source ?? stream);
      const payload = JSON.parse(new TextDecoder().decode(bytes));
      const imported = await databaseService.importTodoDatabaseEntries(payload);
      if (!imported) {
        syncLog("Ignored invalid streamed OrbitDB todo-entry payload");
        return;
      }
      if (typeof payload?.dbAddress === "string") {
        storedTodoEntryMessages.set(payload.dbAddress, {
          topic: todoEntryExchangeTopics[0],
          data: bytes,
        });
        scheduleOrbitdbTopicSync(payload.dbAddress);
      }
    } catch (error: any) {
      syncLog(
        "Failed to import streamed OrbitDB todo entries:",
        error?.message || String(error),
      );
    }
  };

  const republishStoredBridgeMessages = async () => {
    if (isShuttingDown) return;

    const messages = [
      ...storedIdentityMessages.values(),
      ...storedTodoEntryMessages.values(),
    ];
    for (const message of messages) {
      try {
        await pubsub.publish(message.topic, message.data);
      } catch (error: any) {
        syncLog(
          "Failed to republish bridged OrbitDB message:",
          message.topic,
          error?.message || String(error),
        );
      }
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
    if (identityExchangeTopics.includes(msg.topic)) {
      syncQueue.add(() => importOrbitdbIdentityMessage(msg));
      return;
    }
    if (todoEntryExchangeTopics.includes(msg.topic)) {
      syncQueue.add(() => importTodoEntryMessage(msg));
      return;
    }
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
          scheduleOrbitdbTopicSync(subscription.topic);
        }
      }
    }
  };

  const connectionOpenHandler = () => {
    syncQueue.add(() => republishStoredBridgeMessages()).catch(() => {
      // ignore queue shutdown races
    });
  };

  pubsub.addEventListener("message", pubsubMessageHandler);
  pubsub.addEventListener("subscription-change", subscriptionChangeHandler);
  libp2p.addEventListener("connection:open", connectionOpenHandler);
  const republishInterval = setInterval(() => {
    syncQueue.add(() => republishStoredBridgeMessages()).catch(() => {
      // ignore queue shutdown races
    });
  }, BRIDGED_MESSAGE_REPUBLISH_MS);
  for (const topic of identityExchangeTopics) {
    syncQueue.add(() => ensureIdentityTopicSubscribed(topic));
  }
  for (const topic of todoEntryExchangeTopics) {
    syncQueue.add(() => ensureTodoEntryTopicSubscribed(topic));
  }
  for (const protocol of todoEntryExchangeProtocols) {
    libp2p
      .handle(protocol, importTodoEntryStream)
      .then(() => {
        syncLog("Registered relay OrbitDB todo-entry stream protocol:", protocol);
      })
      .catch((error: any) => {
        syncLog(
          "Failed to register relay OrbitDB todo-entry stream protocol:",
          protocol,
          error?.message || String(error),
        );
      });
  }

  return async () => {
    isShuttingDown = true;
    pubsub.removeEventListener("message", pubsubMessageHandler);
    pubsub.removeEventListener(
      "subscription-change",
      subscriptionChangeHandler,
    );
    libp2p.removeEventListener("connection:open", connectionOpenHandler);
    clearInterval(republishInterval);

    syncQueue.pause();
    syncQueue.clear();
    await syncQueue.onIdle();
    for (const topic of subscribedIdentityTopics) {
      try {
        await pubsub.unsubscribe(topic);
      } catch {
        // ignore shutdown unsubscribe failures
      }
    }
    for (const topic of subscribedTodoEntryTopics) {
      try {
        await pubsub.unsubscribe(topic);
      } catch {
        // ignore shutdown unsubscribe failures
      }
    }
    for (const protocol of todoEntryExchangeProtocols) {
      try {
        await libp2p.unhandle(protocol);
      } catch {
        // ignore shutdown unhandle failures
      }
    }
  };
}

function getOrbitdbIdentityExchangeTopics(): string[] {
  const raw = process.env.ORBITDB_IDENTITY_EXCHANGE_TOPICS?.trim();
  if (!raw) return DEFAULT_ORBITDB_IDENTITY_EXCHANGE_TOPICS;

  const topics = raw
    .split(",")
    .map((topic) => topic.trim())
    .filter(Boolean);

  return topics.length > 0 ? topics : DEFAULT_ORBITDB_IDENTITY_EXCHANGE_TOPICS;
}

function getTodoEntryExchangeTopics(): string[] {
  const raw = process.env.ORBITDB_TODO_ENTRY_EXCHANGE_TOPICS?.trim();
  if (!raw) return DEFAULT_TODO_ENTRY_EXCHANGE_TOPICS;

  const topics = raw
    .split(",")
    .map((topic) => topic.trim())
    .filter(Boolean);

  return topics.length > 0 ? topics : DEFAULT_TODO_ENTRY_EXCHANGE_TOPICS;
}

function getTodoEntryExchangeProtocols(): string[] {
  const raw = process.env.ORBITDB_TODO_ENTRY_EXCHANGE_PROTOCOLS?.trim();
  if (!raw) return DEFAULT_TODO_ENTRY_EXCHANGE_PROTOCOLS;

  const protocols = raw
    .split(",")
    .map((protocol) => protocol.trim())
    .filter(Boolean);

  return protocols.length > 0
    ? protocols
    : DEFAULT_TODO_ENTRY_EXCHANGE_PROTOCOLS;
}

async function readStreamBytes(
  source: AsyncIterable<Uint8Array | { subarray: () => Uint8Array }>,
): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  let length = 0;

  for await (const chunk of source) {
    const bytes = chunk instanceof Uint8Array ? chunk : chunk.subarray();
    chunks.push(bytes);
    length += bytes.length;
  }

  const result = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.length;
  }

  return result;
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
