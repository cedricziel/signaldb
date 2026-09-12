// OAuth connector consent API, layered over the generated OpenAPI SDK (mirrors
// the wrapping style in `management.ts`). The consent screen is authenticated
// by the browser session cookie; these calls carry it same-origin.
import "./client";

import {
  type ConsentContextResponse,
  type ConsentDecision,
  type ConsentTenant,
  type ConsentTenantGrant,
  oauthConsentContext,
  oauthConsentDecision,
} from "./gen";
import { unwrapSdkResult } from "./http";

function consentErrorMessage(error: unknown): string | undefined {
  const err = error as
    { error_description?: string; error?: string } | undefined;
  return err?.error_description ?? err?.error;
}

/** Fetch the consent context for a client: its display name and the tenants
 * the signed-in user may grant. Throws `ApiError(401)` when not signed in. */
export async function consentContext(
  clientId: string,
): Promise<ConsentContextResponse> {
  return unwrapSdkResult(
    await oauthConsentContext({ query: { client_id: clientId } }),
    (status) => `Consent request failed (${status})`,
    consentErrorMessage,
  );
}

/** Submit the consent decision; returns the URL the browser should navigate to
 * (the client's redirect URI carrying the `code` or an `error`). */
export async function submitConsentDecision(
  decision: ConsentDecision,
): Promise<string> {
  const result = unwrapSdkResult(
    await oauthConsentDecision({ body: decision }),
    (status) => `Consent request failed (${status})`,
    consentErrorMessage,
  );
  return result.redirect;
}

export type {
  ConsentContextResponse,
  ConsentDecision,
  ConsentTenant,
  ConsentTenantGrant,
};
