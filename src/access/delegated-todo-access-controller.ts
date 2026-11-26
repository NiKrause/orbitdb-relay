import { OrbitDBAccessController } from '@orbitdb/core'

import { verifyDelegationWriterIdentity } from './shared.js'

/**
 * The `todo-delegation` access controller of simple-todo's delegated lists.
 *
 * Taken from `@le-space/orbitdb-access-controller-delegated-todo` 0.1.0 (published
 * from DecentraSol/simple-todo), with the same rules, so the relay keeps accepting
 * exactly what the clients accept. The package itself declares peer dependencies on
 * provider `^0.2.10` and `@orbitdb/core` `^3`; it now verifies writers with the
 * provider the relay depends on (see `./shared.ts`).
 *
 * The owner writes through OrbitDB's own `OrbitDBAccessController` (write set,
 * grant/revoke, the replicated permissions database). On top of that, `canAppend`
 * admits a delegate's `delegation-action` entries:
 * - a `PUT` under `delegation-action/<taskKey>/<encoded delegate DID>/<id>`,
 * - whose value is `{ type: 'delegation-action', action, taskKey, delegateDid }`
 *   matching the key, with `action` `set-completed` (boolean `setCompleted`) or
 *   `patch-fields` (a non-empty `patch` of string `text` and/or `description`),
 * - not past its `expiresAt`,
 * - written by a verified identity whose id is the delegate DID.
 */
export const delegatedTodoAccessControllerType = 'todo-delegation'

type DelegatedTodoAccessControllerOptions = {
  write?: string[]
  /** Log rejection reasons to the console (default false, as on the relay). */
  verbose?: boolean
}

function parseDelegationActionKey(key: unknown): { taskKey: string; delegateDid: string } | null {
  if (typeof key !== 'string') return null
  const match = /^delegation-action\/([^/]+)\/([^/]+)\/[^/]+$/.exec(key)
  if (!match) return null
  try {
    return {
      taskKey: match[1],
      delegateDid: decodeURIComponent(match[2]),
    }
  } catch {
    return null
  }
}

const DelegatedTodoAccessController =
  ({ write, verbose = false }: DelegatedTodoAccessControllerOptions = {}) =>
  async ({ orbitdb, identities, address, name }: any) => {
    const baseFactory = OrbitDBAccessController({ write })
    const baseAccess = await baseFactory({ orbitdb, identities, address, name })

    const logReject = (code: string, detail: Record<string, unknown>) => {
      if (!verbose) return
      console.warn(`🚫 Delegated AC rejected: ${code}`, detail)
    }

    const canAppend = async (entry: any): Promise<boolean> => {
      if (await baseAccess.canAppend(entry)) {
        return true
      }

      const payload = entry?.payload
      if (!payload || payload.op !== 'PUT') {
        logReject('not a PUT payload', { op: payload?.op, key: payload?.key })
        return false
      }

      const parsedKey = parseDelegationActionKey(payload.key)
      if (!parsedKey) {
        logReject('key is not delegation-action format', { key: payload.key })
        return false
      }

      const value = payload.value
      if (!value || value.type !== 'delegation-action') {
        logReject('invalid action envelope', { key: payload.key, type: value?.type })
        return false
      }
      if (!['set-completed', 'patch-fields'].includes(value.action)) {
        logReject('unsupported delegation action', { key: payload.key, action: value.action })
        return false
      }
      if (value.action === 'set-completed' && typeof value.setCompleted !== 'boolean') {
        logReject('set-completed missing boolean payload', {
          key: payload.key,
          setCompleted: value.setCompleted,
        })
        return false
      }
      if (value.action === 'patch-fields') {
        if (!value.patch || typeof value.patch !== 'object') {
          logReject('patch-fields missing patch object', { key: payload.key })
          return false
        }
        const allowedPatchKeys = ['text', 'description']
        const patchKeys = Object.keys(value.patch)
        if (patchKeys.length === 0) {
          logReject('empty patch payload', { key: payload.key })
          return false
        }
        if (!patchKeys.every((k) => allowedPatchKeys.includes(k))) {
          logReject('patch contains unsupported fields', { key: payload.key, patchKeys })
          return false
        }
        if (value.patch.text !== undefined && typeof value.patch.text !== 'string') {
          logReject('patch.text must be string', { key: payload.key, textType: typeof value.patch.text })
          return false
        }
        if (value.patch.description !== undefined && typeof value.patch.description !== 'string') {
          return false
        }
      }
      if (value.taskKey !== parsedKey.taskKey) {
        logReject('taskKey mismatch', {
          key: payload.key,
          taskKey: value.taskKey,
          expectedTaskKey: parsedKey.taskKey,
        })
        return false
      }
      if (value.delegateDid !== parsedKey.delegateDid) {
        logReject('delegateDid mismatch', {
          key: payload.key,
          delegateDid: value.delegateDid,
          expectedDelegateDid: parsedKey.delegateDid,
        })
        return false
      }

      if (value.expiresAt && Date.parse(value.expiresAt) < Date.now()) {
        logReject('delegation action expired', { key: payload.key, expiresAt: value.expiresAt })
        return false
      }

      const writerIdentity = await identities.getIdentity(entry.identity)
      if (!writerIdentity) {
        logReject('writer identity not found', { key: payload.key, entryIdentity: entry.identity })
        return false
      }
      if (!(await verifyDelegationWriterIdentity(identities, writerIdentity))) {
        logReject('writer identity verification failed', {
          key: payload.key,
          entryIdentity: entry.identity,
          writerId: writerIdentity.id,
          writerType: writerIdentity.type,
        })
        return false
      }

      const writerMatchesDelegate = writerIdentity.id === value.delegateDid
      if (!writerMatchesDelegate) {
        logReject('entry writer does not match delegate DID', {
          key: payload.key,
          writerId: writerIdentity.id,
          delegateDid: value.delegateDid,
        })
      }
      return writerMatchesDelegate
    }

    return {
      ...baseAccess,
      type: delegatedTodoAccessControllerType,
      canAppend,
    }
  }

;(DelegatedTodoAccessController as any).type = delegatedTodoAccessControllerType

export default DelegatedTodoAccessController
