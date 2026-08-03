// OAuth connector consent API, layered over the generated OpenAPI SDK (mirrors
// the wrapping style in `management.ts`). The consent screen is authenticated
// by the browser session cookie; these calls carry it same-origin.
import "./client";

import {
  type ConsentContextResponse,
  type ConsentDecision,
  oauthConsentContext,
  oauthConsentDecision,
} from "./gen";
import { ApiError } from "./http";

interface SdkResult<T> {
  data?: T;
  error?: unknown;
  response?: Response;
}

/** Unwrap a generated SDK result into its data, re-throwing failures as
 * `ApiError` (with the HTTP status, so `isAuthError(401)` keeps working). */
function unwrap<T>(result: SdkResult<T>): T {
  const { error, response } = result;
  if (error !== undefined || !response?.ok) {
    const status = response?.status ?? 0;
    const err = error as
      { error_description?: string; error?: string } | undefined;
    const message =
      err?.error_description ??
      err?.error ??
      `Consent request failed (${status})`;
    throw new ApiError(message, status);
  }
  return result.data as T;
}

/** Fetch the consent context for a client: its display name and the tenants
 * the signed-in user may grant. Throws `ApiError(401)` when not signed in. */
export async function consentContext(
  clientId: string,
): Promise<ConsentContextResponse> {
  return unwrap(await oauthConsentContext({ query: { client_id: clientId } }));
}

/** Submit the consent decision; returns the URL the browser should navigate to
 * (the client's redirect URI carrying the `code` or an `error`). */
export async function submitConsentDecision(
  decision: ConsentDecision,
): Promise<string> {
  const result = unwrap(await oauthConsentDecision({ body: decision }));
  return result.redirect;
}

export type { ConsentContextResponse, ConsentDecision };
