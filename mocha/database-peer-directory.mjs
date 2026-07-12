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
});
