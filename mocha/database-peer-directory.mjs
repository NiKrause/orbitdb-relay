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
});
