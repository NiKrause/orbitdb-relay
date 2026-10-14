import assert from "node:assert/strict";

import { DatabaseService } from "../dist/services/database.js";

describe("database peer directory", () => {
  it("deduplicates OrbitDB subscribers and reconnects them for explicit recovery", async () => {
    const service = new DatabaseService();
    const calls = [];
    service.ipfs = {
      libp2p: {
        peerId: { toString: () => "relay" },
        hangUp: async (peer) => calls.push(["hangUp", peer.toString()]),
        dial: async (peer) => calls.push(["dial", peer.toString()]),
      },
    };
    const bob = { toString: () => "bob" };
    const address = "/orbitdb/test-address";

    service.rememberDatabasePeer(address, bob);
    service.rememberDatabasePeer(address, bob);
    service.rememberDatabasePeer("not-an-orbitdb-address", bob);

    assert.equal(service.knownDatabasePeers.get(address)?.size, 1);
    assert.equal(
      service.knownDatabasePeers.has("not-an-orbitdb-address"),
      false,
    );

    await service.reconnectKnownDatabasePeers(address);

    assert.deepEqual(calls, [
      ["hangUp", "bob"],
      ["dial", "bob"],
    ]);
  });

  it("forgets a subscriber that can no longer be dialled", async () => {
    // A browser that closed its tab stayed in the directory forever, so every
    // later open of the same database spent a dial timeout on a peer that was
    // never coming back.
    const service = new DatabaseService();
    const dialled = [];
    service.ipfs = {
      libp2p: {
        peerId: { toString: () => "relay" },
        hangUp: async () => {},
        dial: async (peer) => {
          dialled.push(peer.toString());
          if (peer.toString() === "gone") throw new Error("no valid addresses");
        },
      },
    };
    const address = "/orbitdb/test-address";

    service.rememberDatabasePeer(address, { toString: () => "gone" });
    service.rememberDatabasePeer(address, { toString: () => "alive" });
    assert.equal(service.knownDatabasePeers.get(address).size, 2);

    await service.reconnectKnownDatabasePeers(address);

    const remaining = service.knownDatabasePeers.get(address);
    assert.equal(remaining.size, 1, "the unreachable peer should be dropped");
    assert.equal(remaining.has("gone"), false);
    assert.equal(remaining.has("alive"), true, "a reachable peer must survive");

    // The second open must not pay for the dead peer again.
    dialled.length = 0;
    await service.reconnectKnownDatabasePeers(address);
    assert.deepEqual(dialled, ["alive"]);
  });

  it("re-registers a peer that comes back after being forgotten", async () => {
    // Eviction is cheap to get wrong precisely because it is reversible: the
    // same subscription-change that first recorded a peer records it again.
    const service = new DatabaseService();
    let reachable = false;
    service.ipfs = {
      libp2p: {
        peerId: { toString: () => "relay" },
        hangUp: async () => {},
        dial: async () => {
          if (!reachable) throw new Error("no valid addresses");
        },
      },
    };
    const address = "/orbitdb/test-address";
    const peer = { toString: () => "flaky" };

    service.rememberDatabasePeer(address, peer);
    await service.reconnectKnownDatabasePeers(address);
    assert.equal(service.knownDatabasePeers.get(address).size, 0);

    reachable = true;
    service.rememberDatabasePeer(address, peer);
    await service.reconnectKnownDatabasePeers(address);
    assert.equal(service.knownDatabasePeers.get(address).size, 1);
  });

  it("leaves a connected subscriber alone when it cannot be dialled back", async () => {
    // A browser only ever connects inbound, and its addresses are circuits
    // through this very relay. Hanging up on it cost it every circuit and its
    // reservation, for a dial that could not succeed (#59). A peer with an
    // address of its own is still reconnected.
    const service = new DatabaseService();
    const calls = [];
    const relayId = "relay";
    const circuitThroughUs = `/ip4/127.0.0.1/tcp/4001/ws/p2p/${relayId}/p2p-circuit/p2p/browser`;
    service.ipfs = {
      libp2p: {
        peerId: { toString: () => relayId },
        hangUp: async (peer) => calls.push(["hangUp", peer.toString()]),
        dial: async (peer) => calls.push(["dial", peer.toString()]),
        getConnections: (peer) =>
          peer.toString() === "browser" ? [{ status: "open" }] : [],
        peerStore: {
          get: async (peer) => ({
            addresses: [
              {
                multiaddr: {
                  toString: () =>
                    peer.toString() === "browser"
                      ? circuitThroughUs
                      : "/ip4/10.0.0.2/tcp/4001",
                },
              },
            ],
          }),
        },
        isDialable: async (multiaddr) =>
          !multiaddr.toString().includes("/p2p-circuit"),
      },
    };
    const address = "/orbitdb/test-address";
    service.rememberDatabasePeer(address, { toString: () => "browser" });
    service.rememberDatabasePeer(address, { toString: () => "node" });

    await service.reconnectKnownDatabasePeers(address);

    assert.deepEqual(calls, [
      ["hangUp", "node"],
      ["dial", "node"],
    ]);
    assert.deepEqual(
      [...service.knownDatabasePeers.get(address).keys()].sort(),
      ["browser", "node"],
      "the connected browser must stay in the directory",
    );
  });

  it("forgets a subscriber that has gone and left no address to dial back", async () => {
    const service = new DatabaseService();
    const calls = [];
    service.ipfs = {
      libp2p: {
        peerId: { toString: () => "relay" },
        hangUp: async (peer) => calls.push(["hangUp", peer.toString()]),
        dial: async (peer) => calls.push(["dial", peer.toString()]),
        getConnections: () => [],
        peerStore: { get: async () => ({ addresses: [] }) },
        isDialable: async () => true,
      },
    };
    const address = "/orbitdb/test-address";
    service.rememberDatabasePeer(address, { toString: () => "closed-tab" });

    await service.reconnectKnownDatabasePeers(address);

    assert.deepEqual(calls, [], "nothing to hang up or dial");
    assert.equal(service.knownDatabasePeers.get(address).size, 0);
  });
});
