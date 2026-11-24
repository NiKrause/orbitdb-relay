/**
 * Answer `/orbitdb/heads/<address>`, but do not announce it.
 *
 * OrbitDB registers one libp2p protocol per open database, and the on-demand
 * heads handler adds one for every database the relay knows. All of them used
 * to go out in identify and identify-push. That cost three things:
 *
 * - **Size.** About 74 bytes per database. Past roughly 105 databases the
 *   message passes libp2p's 8192-byte default and clients drop it whole, `hop`
 *   included (#50).
 * - **Pushes.** Every database the relay opens or closes changes its protocol
 *   list, and identify-push then sends the whole list to every connected peer.
 * - **Privacy.** Any peer that connects receives the address of every database
 *   the relay knows (#51).
 *
 * Nothing needs the announcement. OrbitDB's Sync dials the heads protocol with
 * `dialProtocol` and never reads identify. What has to stay is the relay
 * accepting the protocol when it is dialled, and libp2p reads that from the
 * registrar's own `getProtocols()` while negotiating a stream, which this
 * leaves alone.
 *
 * {@link withoutOrbitdbHeadsAnnouncements} wraps the identify and
 * identify-push service factories so that only they see the filtered view:
 * - `registrar.getProtocols()` without heads protocols (what identify-push
 *   sends);
 * - `peerStore.get(self)` without them (what an identify response sends);
 * - `self:peer:update` events dropped when nothing announced changed, so
 *   opening or closing a database pushes nothing.
 */

/** Prefix of OrbitDB's per-database heads protocol ids. */
export const ORBITDB_HEADS_PROTOCOL_PREFIX = '/orbitdb/heads/'

/** Whether a protocol id is one of OrbitDB's per-database heads protocols. */
export function isOrbitdbHeadsProtocol(protocol: unknown): boolean {
  return typeof protocol === 'string' && protocol.startsWith(ORBITDB_HEADS_PROTOCOL_PREFIX)
}

/** The protocols a node announces: all but OrbitDB's per-database heads protocols. */
export function announcedProtocols(protocols: readonly string[]): string[] {
  return protocols.filter((protocol) => !isOrbitdbHeadsProtocol(protocol))
}

/**
 * Wrap a libp2p service factory — `identify()` or `identifyPush()` — so the
 * service announces no `/orbitdb/heads/*` protocol. The node still answers
 * them. Use it for both services; wrapping only one leaves the other
 * announcing the full list.
 *
 * @example
 * services: {
 *   identify: withoutOrbitdbHeadsAnnouncements(identify()),
 *   identifyPush: withoutOrbitdbHeadsAnnouncements(identifyPush()),
 * }
 */
export function withoutOrbitdbHeadsAnnouncements<T>(
  factory: (components: any) => T,
): (components: any) => T {
  return (components: any) => factory(announcementView(components))
}

/** Bind methods to the real object: libp2p's emitters and stores check `this`. */
function passThrough(target: any, prop: string | symbol): any {
  const value = Reflect.get(target, prop, target)
  return typeof value === 'function' ? value.bind(target) : value
}

function announcementView(components: any): any {
  const views = new WeakMap<object, any>()
  const viewOf = (target: any, make: (target: any) => any): any => {
    if (target == null || (typeof target !== 'object' && typeof target !== 'function')) return target
    let view = views.get(target)
    if (view === undefined) {
      view = make(target)
      views.set(target, view)
    }
    return view
  }

  return new Proxy(components, {
    get(target, prop) {
      if (prop === 'registrar') return viewOf(Reflect.get(target, prop), registrarView)
      if (prop === 'peerStore') {
        return viewOf(Reflect.get(target, prop), (peerStore) => peerStoreView(peerStore, () => target.peerId))
      }
      if (prop === 'events') return viewOf(Reflect.get(target, prop), eventsView)
      return Reflect.get(target, prop)
    },
  })
}

function registrarView(registrar: any): any {
  return new Proxy(registrar, {
    get(target, prop) {
      if (prop === 'getProtocols') {
        // Read at call time: the on-demand heads support replaces the
        // registrar's `getProtocols` after the services have started.
        return () => announcedProtocols(target.getProtocols())
      }
      return passThrough(target, prop)
    },
  })
}

function peerStoreView(peerStore: any, selfPeerId: () => any): any {
  return new Proxy(peerStore, {
    get(target, prop) {
      if (prop === 'get') {
        return async (peerId: any, options?: unknown) => {
          const peer = await target.get(peerId, options)
          if (peer == null || selfPeerId()?.equals?.(peerId) !== true) return peer
          return { ...peer, protocols: announcedProtocols(peer.protocols ?? []) }
        }
      }
      return passThrough(target, prop)
    },
  })
}

/**
 * What identify-push sends about this node, apart from the parts it computes
 * fresh: protocols (filtered), addresses, and metadata such as the agent
 * version. Two updates with the same signature announce nothing new.
 */
function announcedSignature(peer: any): string | null {
  if (peer == null) return null
  const metadata = Array.from(peer.metadata?.entries?.() ?? [], ([key, value]: [string, Uint8Array]) => [
    key,
    Buffer.from(value ?? []).toString('base64'),
  ]).sort()
  return JSON.stringify([
    announcedProtocols(peer.protocols ?? []).sort(),
    Array.from(peer.addresses ?? [], (address: any) => String(address?.multiaddr)).sort(),
    metadata,
  ])
}

function eventsView(events: any): any {
  const wrapped = new WeakMap<object, any>()
  const wrap = (listener: any): any => {
    if (listener == null) return listener
    let filtered = wrapped.get(listener)
    if (filtered === undefined) {
      const call = (event: any) =>
        typeof listener === 'function' ? listener(event) : listener.handleEvent?.(event)
      filtered = (event: any) => {
        const detail = event?.detail
        if (
          detail?.previous != null &&
          announcedSignature(detail.peer) === announcedSignature(detail.previous)
        ) {
          return
        }
        return call(event)
      }
      wrapped.set(listener, filtered)
    }
    return filtered
  }

  return new Proxy(events, {
    get(target, prop) {
      if (prop === 'addEventListener') {
        return (type: string, listener: any, options?: unknown) =>
          target.addEventListener(type, type === 'self:peer:update' ? wrap(listener) : listener, options)
      }
      if (prop === 'removeEventListener') {
        return (type: string, listener: any, options?: unknown) =>
          target.removeEventListener(
            type,
            type === 'self:peer:update' ? (wrapped.get(listener) ?? listener) : listener,
            options,
          )
      }
      return passThrough(target, prop)
    },
  })
}
