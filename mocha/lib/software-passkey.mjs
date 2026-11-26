/**
 * A software passkey for Node tests: a real P-256 key behind `navigator.credentials`,
 * so `@le-space/orbitdb-identity-provider-webauthn-did` builds a genuine `webauthn`
 * identity, its WebAuthn proof and all, the way it does in a browser.
 *
 * Modelled on the provider's own `tests/helpers/mock-authenticator.js`
 * (Le-Space/orbitdb-identity-provider-webauthn-did). The browser globals it needs
 * (`navigator`, `window`, `localStorage`, `PublicKeyCredential`) are installed only
 * while the provider talks to the passkey and restored right after, because every
 * mocha file runs in this one process alongside libp2p.
 *
 * Proof formats (provider 0.14.0, GHSA-7j39-c465-x35x):
 * - current (2): what 0.14.0 creates for every new identity;
 * - first (1): what 0.13.0 and older created. A device that stored such a proof keeps
 *   using it, so the relay keeps seeing identities with it.
 */
import { Identities, KeyStore, MemoryStorage } from '@orbitdb/core'
import {
  OrbitDBWebAuthnIdentityProviderFunction,
  WebAuthnDIDProvider,
  proofFormat,
} from '@le-space/orbitdb-identity-provider-webauthn-did'

const utf8 = (text) => new TextEncoder().encode(text)
const b64url = (bytes) => Buffer.from(bytes).toString('base64url')
const sha256 = async (bytes) => new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))
const concat = (...parts) => {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0))
  let offset = 0
  for (const part of parts) {
    out.set(part, offset)
    offset += part.length
  }
  return out
}
const toArrayBuffer = (bytes) => bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength)

/** Raw r‖s → DER, as an authenticator returns an ES256 signature. */
function derEncodeEcdsaSignature(raw) {
  const integer = (bytes) => {
    let i = 0
    while (i < bytes.length - 1 && bytes[i] === 0) i += 1
    let body = bytes.subarray(i)
    if (body[0] & 0x80) body = new Uint8Array([0, ...body])
    return new Uint8Array([0x02, body.length, ...body])
  }
  const r = integer(raw.subarray(0, 32))
  const s = integer(raw.subarray(32))
  return new Uint8Array([0x30, r.length + s.length, ...r, ...s])
}

/** The provider's key for a stored identity proof (`src/webauthn/identity-proof-store.js`). */
async function identityProofStorageKey(credentialId, data) {
  const credential = utf8(String(credentialId))
  const digest = await sha256(concat(credential, new Uint8Array([0x1f]), utf8(data)))
  return `webauthn-identity-proof:${b64url(digest)}`
}

/**
 * One passkey on one device: the authenticator, and the device's local storage,
 * which is where the provider keeps the identity proof it reuses.
 */
export async function createSoftwarePasskey({ rpId = 'localhost', name = 'relay-test' } = {}) {
  const keypair = await crypto.subtle.generateKey({ name: 'ECDSA', namedCurve: 'P-256' }, false, ['sign', 'verify'])
  const spki = new Uint8Array(await crypto.subtle.exportKey('spki', keypair.publicKey))
  const rawId = crypto.getRandomValues(new Uint8Array(32))
  const prfSecret = crypto.getRandomValues(new Uint8Array(32))
  const rpIdHash = await sha256(utf8(rpId))
  const deviceStorage = new Map()
  let signCount = 0

  const authenticatorData = () => {
    const count = new Uint8Array(4)
    new DataView(count.buffer).setUint32(0, signCount)
    return concat(rpIdHash, new Uint8Array([0x01 | 0x04]), count) // UP | UV
  }
  const clientDataJSON = (type, challenge) =>
    utf8(JSON.stringify({ type, challenge: b64url(challenge), origin: `https://${rpId}`, crossOrigin: false }))

  const credentials = {
    async create(options) {
      signCount += 1
      const challenge = new Uint8Array(options?.publicKey?.challenge ?? new Uint8Array(32))
      return {
        id: b64url(rawId),
        rawId: toArrayBuffer(rawId),
        type: 'public-key',
        response: {
          // The provider reads the key through getPublicKey() and keeps this as it is.
          attestationObject: new ArrayBuffer(0),
          clientDataJSON: toArrayBuffer(clientDataJSON('webauthn.create', challenge)),
          getPublicKey: () => toArrayBuffer(spki),
        },
        getClientExtensionResults: () => ({}),
      }
    },
    async get(options) {
      signCount += 1
      const challenge = new Uint8Array(options?.publicKey?.challenge ?? new Uint8Array(32))
      const authData = authenticatorData()
      const clientData = clientDataJSON('webauthn.get', challenge)
      const raw = new Uint8Array(
        await crypto.subtle.sign(
          { name: 'ECDSA', hash: 'SHA-256' },
          keypair.privateKey,
          concat(authData, await sha256(clientData)),
        ),
      )
      // PRF: the same input gives the same output, as a real passkey's does; the
      // provider derives the OrbitDB signing key from it.
      const prfInput = options?.publicKey?.extensions?.prf?.eval?.first
      const extensionResults = prfInput
        ? { prf: { results: { first: toArrayBuffer(await sha256(concat(prfSecret, utf8('prf'), new Uint8Array(prfInput)))) } } }
        : {}
      return {
        id: b64url(rawId),
        rawId: toArrayBuffer(rawId),
        type: 'public-key',
        response: {
          authenticatorData: toArrayBuffer(authData),
          clientDataJSON: toArrayBuffer(clientData),
          signature: toArrayBuffer(derEncodeEcdsaSignature(raw)),
          userHandle: null,
        },
        getClientExtensionResults: () => extensionResults,
      }
    },
  }

  const passkey = {
    rpId,
    credential: null,
    /** Run `fn` with this passkey and its device storage standing in for the browser's. */
    async use(fn) {
      const restore = installBrowserGlobals({ rpId, credentials, deviceStorage })
      try {
        return await fn()
      } finally {
        restore()
      }
    },
    /** Forget the stored identity proof, as a device that never stored one. */
    forgetIdentityProofs() {
      for (const key of [...deviceStorage.keys()]) {
        if (key.startsWith('webauthn-identity-proof:')) deviceStorage.delete(key)
      }
    },
    /**
     * A proof in the first format over `data`, as provider 0.13.0 and older made it:
     * its challenge is SHA-256(rawCredentialId ‖ data).
     */
    async firstFormatProof(data) {
      const assertion = await credentials.get({
        publicKey: { challenge: await sha256(concat(rawId, utf8(data))) },
      })
      return b64url(
        utf8(
          JSON.stringify({
            credentialId: b64url(rawId),
            authenticatorData: b64url(new Uint8Array(assertion.response.authenticatorData)),
            clientDataJSON: new TextDecoder().decode(assertion.response.clientDataJSON),
            signature: b64url(new Uint8Array(assertion.response.signature)),
          }),
        ),
      )
    },
    async storeIdentityProof(data, proof) {
      deviceStorage.set(await identityProofStorageKey(passkey.credential.credentialId, data), proof)
    },
  }

  passkey.credential = await passkey.use(() =>
    WebAuthnDIDProvider.createCredential({ userId: name, displayName: name }),
  )
  return passkey
}

function installBrowserGlobals({ rpId, credentials, deviceStorage }) {
  const names = ['navigator', 'window', 'localStorage', 'PublicKeyCredential']
  const previous = new Map(names.map((name) => [name, Object.getOwnPropertyDescriptor(globalThis, name)]))

  const navigatorShim = { credentials }
  class SoftwarePublicKeyCredential {
    static isUserVerifyingPlatformAuthenticatorAvailable() {
      return Promise.resolve(true)
    }
  }
  const define = (name, value) =>
    Object.defineProperty(globalThis, name, { value, configurable: true, writable: true, enumerable: false })

  define('navigator', navigatorShim)
  define('PublicKeyCredential', SoftwarePublicKeyCredential)
  define('localStorage', {
    getItem: (k) => (deviceStorage.has(k) ? deviceStorage.get(k) : null),
    setItem: (k, v) => deviceStorage.set(k, String(v)),
    removeItem: (k) => deviceStorage.delete(k),
    clear: () => deviceStorage.clear(),
    key: (i) => [...deviceStorage.keys()][i] ?? null,
    get length() {
      return deviceStorage.size
    },
  })
  define('window', {
    location: { hostname: rpId, origin: `https://${rpId}` },
    navigator: navigatorShim,
    PublicKeyCredential: SoftwarePublicKeyCredential,
  })

  return () => {
    for (const [name, descriptor] of previous) {
      if (descriptor) Object.defineProperty(globalThis, name, descriptor)
      else delete globalThis[name]
    }
  }
}

/**
 * The passkey's OrbitDB identity, made by the provider, in the current proof format.
 *
 * @param {object} passkey from {@link createSoftwarePasskey}
 * @param {{ ipfs?: any, keystore?: any }} [options] with `ipfs`, the identity is stored
 *   in that node's blockstore, where peers (and the relay) fetch it from.
 */
export async function createPasskeyIdentity(passkey, { ipfs, keystore } = {}) {
  keystore = keystore ?? (await KeyStore({ storage: await MemoryStorage() }))
  const identities = await Identities({ keystore, ...(ipfs ? { ipfs } : {}) })
  const identity = await passkey.use(() =>
    identities.createIdentity({
      provider: OrbitDBWebAuthnIdentityProviderFunction({ webauthnCredential: passkey.credential }),
    }),
  )
  return { identities, identity }
}

/**
 * The passkey's OrbitDB identity as a device set up before provider 0.14.0 has it:
 * the provider keeps the first-format proof that device stored, so the identity
 * document carries that proof.
 */
export async function createFirstFormatPasskeyIdentity(passkey, options = {}) {
  // The signing key comes from the passkey's PRF, so `publicKey + signatures.id`
  // is the same in every session; a throwaway identity tells us what it is.
  const { identity: probe } = await createPasskeyIdentity(passkey)
  const data = probe.publicKey + probe.signatures.id
  await passkey.storeIdentityProof(data, await passkey.firstFormatProof(data))

  const result = await createPasskeyIdentity(passkey, options)
  const format = proofFormat(result.identity.signatures.publicKey)
  if (format !== 1) throw new Error(`expected a first-format identity proof, got format ${format}`)
  return result
}
