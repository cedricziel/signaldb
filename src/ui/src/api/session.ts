// Client for the router's UI session endpoints (/ui/session) and the
// tenant-scoped whoami endpoint (/api/v1/whoami), all through the generated
// OpenAPI client (`import "./client"` registers its shared config). The
// session cookie is HttpOnly — browser code never reads it; it only creates
// and clears it.
import "./client";

import {
  type CreateSessionResponse,
  createSession as createSessionSdk,
  type CurrentSessionResponse,
  currentSession as currentSessionSdk,
  deleteSession as deleteSessionSdk,
  type LoginConfigResponse,
  loginConfig as loginConfigSdk,
  type OidcLoginConfig,
  type SessionMembership,
  type SessionUser,
  type WhoamiDataset,
  type WhoamiIdentityResponse,
  whoami as whoamiSdk,
} from "./gen";
import { errorEnvelopeMessage, unwrapSdkResult } from "./http";

/** `GET /ui/session/config`: which credentials the login page may offer.
 * Unauthenticated; throws `ApiError` on a non-2xx response (a 404 from an
 * older router, a 5xx). */
export async function loginConfig(): Promise<LoginConfigResponse> {
  return unwrapSdkResult(
    await loginConfigSdk(),
    (status) => `Request failed (${status})`,
  );
}

/** `GET /ui/session`: the signed-in user, their memberships, and the
 * auto-selected tenant/dataset — authenticated by the session cookie alone.
 * Throws `ApiError(401)` without a valid session. */
export async function currentSession(): Promise<CurrentSessionResponse> {
  return unwrapSdkResult(
    await currentSessionSdk(),
    (status) => `Request failed (${status})`,
  );
}

export type {
  CreateSessionResponse,
  CurrentSessionResponse,
  LoginConfigResponse,
  OidcLoginConfig,
  SessionMembership,
  SessionUser,
  WhoamiDataset,
  WhoamiIdentityResponse,
};

export interface SessionCredentials {
  email: string;
  password: string;
  /** Optional: when omitted the server auto-selects a sole membership or
   * returns the membership list for the UI's tenant picker. */
  tenant?: string;
  dataset?: string;
}

/** Create a session: the server validates the credentials and sets the
 * HttpOnly session cookie. Throws `ApiError` with the server's message on
 * invalid credentials. */
export async function createSession(
  creds: SessionCredentials,
): Promise<CreateSessionResponse> {
  return unwrapSdkResult(
    await createSessionSdk({
      body: {
        email: creds.email,
        password: creds.password,
        ...(creds.tenant ? { tenant: creds.tenant } : {}),
        ...(creds.dataset ? { dataset: creds.dataset } : {}),
      },
    }),
    (status) => `Login failed (${status})`,
    errorEnvelopeMessage,
  );
}

/** Log out: the server clears the session cookie. */
export async function deleteSession(): Promise<void> {
  unwrapSdkResult(
    await deleteSessionSdk(),
    (status) => `Logout failed (${status})`,
  );
}

/** Fetch the authenticated tenant and its datasets. Throws `ApiError`
 * (404 on servers without the endpoint, 401 when unauthenticated). Pass
 * `tenant` to scope the lookup to a specific tenant (e.g. right after
 * picking one at login) instead of the current tenant context. */
export async function whoami(tenant?: string): Promise<WhoamiIdentityResponse> {
  return unwrapSdkResult(
    await whoamiSdk(
      tenant ? { headers: { "X-Tenant-ID": tenant } } : undefined,
    ),
    (status) => `whoami failed (${status})`,
  );
}
