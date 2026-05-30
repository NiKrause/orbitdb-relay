import { LevelBlockstore } from 'blockstore-level'
import { LevelDatastore } from 'datastore-level'
import { join } from 'path'
import { Key } from 'interface-datastore'
import { generateKeyPair, privateKeyFromProtobuf, privateKeyToProtobuf } from '@libp2p/crypto/keys'
import type { Datastore } from 'interface-datastore'

async function loadOrCreateRelayPrivateKey(datastore: Datastore) {
  const key = new Key('/le-space/relay/private-key')
  const injectedHex = process.env.RELAY_PRIV_KEY?.trim()

  if (injectedHex) {
    const injectedKey = privateKeyFromProtobuf(Buffer.from(injectedHex, 'hex'))
    await datastore.put(key, privateKeyToProtobuf(injectedKey))
    return injectedKey
  }

  try {
    const bytes = await datastore.get(key)
    return privateKeyFromProtobuf(bytes)
  } catch (err: any) {
    if (err?.code !== 'ERR_NOT_FOUND') throw err
  }

  const privateKey = await generateKeyPair('Ed25519')
  await datastore.put(key, privateKeyToProtobuf(privateKey))
  return privateKey
}

export async function initializeStorage(hostDirectory: string): Promise<{
  datastore: any
  blockstore: any
  privateKey: any
}> {
  const datastore = new LevelDatastore(join(hostDirectory, '/', 'ipfs', '/', 'data'))
  await datastore.open()

  const blockstore = new LevelBlockstore(join(hostDirectory, '/', 'ipfs', '/', 'blocks'))
  await blockstore.open()

  const privateKey = await loadOrCreateRelayPrivateKey(datastore)
  
  return { datastore, blockstore, privateKey }
}
