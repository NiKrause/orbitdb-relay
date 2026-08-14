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

  it('reports the relay version on GET /health and /multiaddrs', async () => {
    const { RELAY_VERSION } = await import('../dist/version.js')
    const handler = exports.createPinningHttpRequestHandler({
      getLibp2p: () => null,
    })
    const server = http.createServer(handler)
    const addr = await listen(server)
    const base = `http://127.0.0.1:${addr.port}`

    try {
      const healthRes = await fetch(`${base}/health`)
      assert.equal(healthRes.status, 200)
      const health = await healthRes.json()
      assert.equal(health.status, 'ok')
      assert.equal(health.version, RELAY_VERSION)
      assert.ok(/^\d+\.\d+\.\d+/.test(health.version), 'version looks like semver')

      const maRes = await fetch(`${base}/multiaddrs`)
      assert.equal(maRes.status, 200)
      const ma = await maRes.json()
      assert.equal(ma.version, RELAY_VERSION)
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
  it('answers entry membership over /pinning/has-entry, keeping "unknown" distinct from "no"', async () => {
    const calls = []
    const handler = exports.createPinningHttpRequestHandler({
      pinning: {
        getStats: () => ({ ok: true }),
        getDatabases: () => ({ databases: [], total: 0 }),
        syncDatabase: async () => ({ ok: true, extractedMediaCids: [] }),
        hasEntry: async (dbAddress, entryHash) => {
          calls.push({ dbAddress, entryHash })
          if (entryHash === 'zdpuKnown') return { ok: true, hasEntry: true, entryCount: 3, source: 'db.all()' }
          if (entryHash === 'zdpuMissing') return { ok: true, hasEntry: false, entryCount: 3, source: 'db.all()' }
          return { ok: true, hasEntry: null, entryCount: null, source: 'database-not-open' }
        },
      },
    })
    const server = http.createServer(handler)
    const addr = await listen(server)
    const base = `http://127.0.0.1:${addr.port}`
    const ask = (body) =>
      fetch(`${base}/pinning/has-entry`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
      })

    try {
      const found = await ask({ dbAddress: '/orbitdb/x', entryHash: 'zdpuKnown' })
      assert.equal(found.status, 200)
      assert.deepEqual(await found.json(), {
        ok: true,
        dbAddress: '/orbitdb/x',
        entryHash: 'zdpuKnown',
        hasEntry: true,
        entryCount: 3,
        source: 'db.all()',
      })

      // A scanned miss is a real "no" and must not be confused with the
      // unscannable case below — the client retries on one and gives up on
      // neither, but it may only ever show a red state for this one.
      const missing = await ask({ dbAddress: '/orbitdb/x', entryHash: 'zdpuMissing' })
      assert.equal((await missing.json()).hasEntry, false)

      const unknown = await ask({ dbAddress: '/orbitdb/x', entryHash: 'zdpuOther' })
      const unknownBody = await unknown.json()
      assert.equal(unknownBody.hasEntry, null)
      assert.equal(unknownBody.source, 'database-not-open')

      assert.equal(calls.length, 3)

      const noHash = await ask({ dbAddress: '/orbitdb/x' })
      assert.equal(noHash.status, 400)
      assert.equal((await noHash.json()).code, 'missing_entry_hash')

      const noAddress = await ask({ entryHash: 'zdpuKnown' })
      assert.equal(noAddress.status, 400)
      assert.equal((await noAddress.json()).code, 'missing_db_address')
    } finally {
      await close(server)
    }
  })

  it('reports 501 rather than 404 when the relay predates entry membership', async () => {
    const handler = exports.createPinningHttpRequestHandler({
      pinning: {
        getStats: () => ({ ok: true }),
        getDatabases: () => ({ databases: [], total: 0 }),
        syncDatabase: async () => ({ ok: true, extractedMediaCids: [] }),
        // hasEntry deliberately absent: the handler type keeps it optional so an
        // older embedder still compiles, and the client needs to tell "this
        // relay cannot answer" apart from "wrong URL" to pick its fallback.
      },
    })
    const server = http.createServer(handler)
    const addr = await listen(server)
    const base = `http://127.0.0.1:${addr.port}`

    try {
      const res = await fetch(`${base}/pinning/has-entry`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ dbAddress: '/orbitdb/x', entryHash: 'zdpuKnown' }),
      })
      assert.equal(res.status, 501)
      assert.equal((await res.json()).code, 'not_implemented')
    } finally {
      await close(server)
    }
  })
})
