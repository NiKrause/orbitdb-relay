import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'

import { DatabaseService } from '../dist/services/database.js'

describe('database service update-event pinning', function () {
  this.timeout(10000)

  it('exposes row-level sync proof for non-media database rows', async () => {
    const service = new DatabaseService()

    const rows = [
      {
        key: 'todo_1',
        value: {
          text: 'relay-visible todo',
          completed: false,
        },
      },
    ]

    const db = {
      name: 'simple-todos-test',
      all: async () => rows,
      close: async () => {},
    }

    service.ipfs = {
      pins: {
        add: async function * (cid) {
          yield cid
        },
      },
    }

    service.orbitdb = {
      open: async () => db,
      stop: async () => {},
    }

    const pinning = service.createPinningHttpHandlers()
    const result = await pinning.syncDatabase('/orbitdb/zdpuTodoRows')

    assert.equal(result.ok, true)
    assert.equal(result.fallbackScanUsed, true)
    assert.equal(result.entryCount, 1)
    assert.equal(result.snapshotSource, 'db.all()')
    assert.equal(result.lastRecord?.key, 'todo_1')

    const listed = pinning.getDatabases({ address: '/orbitdb/zdpuTodoRows' })
    assert.equal(listed.total, 1)
    assert.equal(listed.databases[0].entryCount, 1)
    assert.equal(listed.databases[0].snapshotSource, 'db.all()')

    await service.stop()
  })

  it('pins media CID from update entry without full db.all() scan', async () => {
    const service = new DatabaseService()

    const imageCid = 'bafkreiad2y7aldkdy6vfxazrdb5s2tcebev6levelxomvavd2acyll67pe'
    const pinned = []
    let allCalls = 0

    const events = new EventEmitter()
    const db = {
      events,
      all: async () => {
        allCalls += 1
        return []
      },
      close: async () => {},
    }

    service.ipfs = {
      pins: {
        add: async function * (cid) {
          pinned.push(cid.toString())
          yield cid
        },
      },
    }

    service.orbitdb = {
      open: async () => db,
      stop: async () => {},
    }

    const syncPromise = service.syncAllOrbitDBRecords('/orbitdb/zdpuTestAddressForUpdatePinning')

    setTimeout(() => {
      events.emit('update', {
        payload: {
          value: {
            imageCid,
            text: 'post created via update event',
          },
        },
      })
    }, 50)

    await syncPromise
    await service.pinQueue.onIdle()

    assert.equal(allCalls, 0, 'db.all() should not be called for update-driven pinning')
    assert.deepEqual(pinned, [imageCid])

    await service.stop()
  })

  it('keeps pinning media from a database it holds open, without another sync (#66)', async () => {
    // Once the relay holds a database open, a message on its topic no longer
    // queues a sync — OrbitDB replicates it natively. That sync was also what
    // pinned media from new entries, so the open database has to do it itself.
    const service = new DatabaseService()
    const address = '/orbitdb/zdpuTestAddressHeldOpenPinning'

    const firstCid = 'bafkreiad2y7aldkdy6vfxazrdb5s2tcebev6levelxomvavd2acyll67pe'
    const laterCid = 'bafkreid56lahptpnrggtvsbpasr7es37qp4lgx7perixw3nsqd74qs3f4i'
    const pinned = []

    const events = new EventEmitter()
    const db = {
      events,
      all: async () => [],
      close: async () => {},
    }

    service.ipfs = {
      pins: {
        add: async function * (cid) {
          pinned.push(cid.toString())
          yield cid
        },
      },
    }

    service.orbitdb = {
      open: async () => db,
      stop: async () => {},
    }

    const syncPromise = service.syncAllOrbitDBRecords(address)
    setTimeout(() => {
      events.emit('update', { payload: { value: { imageCid: firstCid } } })
    }, 50)
    await syncPromise
    await service.pinQueue.onIdle()
    const syncsAfterFirst = service.pinningSyncOperations

    // A later entry, replicated by OrbitDB with no sync running.
    events.emit('update', { payload: { value: { imageCid: laterCid, text: 'later post' } } })

    const deadline = Date.now() + 3000
    while (!pinned.includes(laterCid) && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 20))
    }
    assert.deepEqual(pinned, [firstCid, laterCid])
    assert.equal(service.pinningSyncOperations, syncsAfterFirst, 'no further sync ran')
    assert.equal(service.isHeldOpen(address), true)

    const [record] = service.createPinningHttpHandlers().getDatabases({ address }).databases
    assert.match(record.snapshotSource, /update-event/)
    assert.match(JSON.stringify(record.lastRecord), /later post/, 'the proof shows the later entry')

    await service.stop()
    assert.equal(events.listenerCount('update'), 0, 'stop() detaches the listener')
  })
})
