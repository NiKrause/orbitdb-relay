import assert from 'node:assert/strict'
import http from 'node:http'

import { createHelia } from 'helia'
import { MemoryBlockstore } from 'blockstore-core'
import { MemoryDatastore } from 'datastore-core'
import { unixfs } from '@helia/unixfs'

function listen(server) {
  return new Promise((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => resolve(server.address()))
  })
}

function close(server) {
  return new Promise((resolve) => {
    server.close(() => resolve())
  })
}

describe('PinningHttp request handler', function () {
  this.timeout(60000)

  let exports

  before(async () => {
    exports = await import('../dist/index.js')
  })

  it('returns JSON error contract for pinning route validation failures', async () => {
    const handler = exports.createPinningHttpRequestHandler({
      pinning: {
        getStats: () => ({ ok: true }),
        getDatabases: () => ({ databases: [], total: 0 }),
        syncDatabase: async () => ({ ok: true, extractedMediaCids: [], entryCount: 1, snapshotSource: 'db.all()', lastRecord: { key: 'todo_1' } }),
      },
    })
    const server = http.createServer(handler)
    const addr = await listen(server)
    const base = `http://127.0.0.1:${addr.port}`

    try {
      const dbRes = await fetch(`${base}/pinning/databases?address=%2Forbitdb%2Fmissing`)
      assert.equal(dbRes.status, 404)
      assert.deepEqual(await dbRes.json(), {
        ok: false,
        code: 'database_not_found',
        error: 'Database address not found in relay sync history',
      })

      const syncRes = await fetch(`${base}/pinning/sync`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}',
      })
      assert.equal(syncRes.status, 400)
      assert.deepEqual(await syncRes.json(), {
        ok: false,
        code: 'missing_db_address',
        error: 'Missing or invalid dbAddress',
      })
    } finally {
      await close(server)
    }
  })

  it('falls back to Helia content when pinning returns not found', async () => {
    const helia = await createHelia({
      datastore: new MemoryDatastore(),
      blockstore: new MemoryBlockstore(),
    })
    const fsApi = unixfs(helia)
    const bytes = new TextEncoder().encode('pinning-http-fallback-ok')
    const cid = await fsApi.addBytes(bytes)

    const handler = exports.createPinningHttpRequestHandler({
      pinning: {
        getStats: () => ({}),
        getDatabases: () => ({ databases: [], total: 0 }),
        syncDatabase: async () => ({ ok: true, extractedMediaCids: [] }),
        streamPinnedCid: async () => ({ ok: false, status: 404, error: 'CID is not pinned locally' }),
      },
      getHelia: () => helia,
      ipfsGateway: {
        enabled: true,
        fallbackMode: 'pinned-first-network-fallback',
        catTimeoutMs: 10000,
      },
    })
    const server = http.createServer(handler)
    const addr = await listen(server)
    const base = `http://127.0.0.1:${addr.port}`

    try {
      const response = await fetch(`${base}/ipfs/${encodeURIComponent(cid.toString())}`)
      assert.equal(response.status, 200)
      assert.equal(await response.text(), 'pinning-http-fallback-ok')
    } finally {
      await close(server)
      await helia.stop()
    }
  })
})
