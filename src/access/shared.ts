import { OrbitDBWebAuthnIdentityProviderFunction } from '@le-space/orbitdb-identity-provider-webauthn-did'

/**
 * Writer-identity verification shared by the relay's access controllers.
 *
 * Taken from `@le-space/orbitdb-access-controller-delegated-todo` 0.1.0 (published
 * from DecentraSol/simple-todo), unchanged in behaviour. That package is pinned by
 * its peer dependencies to provider `^0.2.10` and `@orbitdb/core` `^3`, which kept
 * a vulnerable `@le-space/orbitdb-identity-provider-webauthn-did` in the relay's
 * tree (GHSA-7j39-c465-x35x). Here it runs against the provider the relay itself
 * depends on.
 */

type IdentitiesLike = {
  verifyIdentity: (identity: unknown) => Promise<boolean>
  verifyIdentityFallback?: (identity: unknown) => Promise<boolean>
}

function isUnsupportedVarsigHeaderError(error: unknown): boolean {
  return String((error as { message?: unknown } | null)?.message || '')
    .toLowerCase()
    .includes('unsupported varsig header')
}

/**
 * Verify a writer identity across the identity modes the relay replicates: the
 * primary `identities.verifyIdentity`, then the unsupported-varsig-header edge
 * case, then the WebAuthn provider directly, then `identities.verifyIdentityFallback`.
 *
 * Which WebAuthn proof formats pass is the provider's decision
 * (`configureWebAuthn({ acceptLegacyProofs })`); the relay keeps its default, so
 * both the current format and the first one verify.
 *
 * @returns true when one of the verifiers accepts the identity. A failure of the
 * primary verifier other than the varsig header error is rethrown.
 */
export async function verifyDelegationWriterIdentity(
  identities: IdentitiesLike,
  writerIdentity: any,
): Promise<boolean> {
  try {
    if (await identities.verifyIdentity(writerIdentity)) {
      return true
    }
  } catch (error) {
    if (!isUnsupportedVarsigHeaderError(error)) {
      throw error
    }
  }

  try {
    const provider = OrbitDBWebAuthnIdentityProviderFunction as any
    if (writerIdentity?.type === 'webauthn' && typeof provider.verifyIdentity === 'function') {
      return await provider.verifyIdentity(writerIdentity)
    }
  } catch {
    // continue
  }

  const fallbackVerify = identities?.verifyIdentityFallback
  if (typeof fallbackVerify === 'function') {
    try {
      return await fallbackVerify(writerIdentity)
    } catch {
      // continue
    }
  }

  return false
}

/** The name the relay's access controllers use for {@link verifyDelegationWriterIdentity}. */
export const verifyIdentityWithFallback = verifyDelegationWriterIdentity
