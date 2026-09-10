// Client for the router's UI session endpoints (/ui/session) and the
// tenant-scoped whoami endpoint (/api/v1/whoami). The session cookie is
// HttpOnly — browser code never reads it; it only creates and clears it.
//
// `loginConfig` and `currentSession` go through the generated OpenAPI client
// (`import "./client"` registers its shared config) instead of raw fetch —
// new endpoints are consumed through `src/api/gen` per the migration in
// progress; the rest of this file predates that and is migrated separately.
import "./client";

import {
  type CurrentSessionResponse,
  currentSession as currentSessionSdk,
  type LoginConfigResponse,
  loginConfig as loginConfigSdk,
  type OidcLoginConfig,
  type SessionMembership,
  type SessionUser,
} from "./gen";
import {
  ApiError,
  retryAfterMsFrom,
  retryingFetch,
  tenantHeaders,
  unwrapSdkResult,
} from "./http";

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
  CurrentSessionResponse,
  LoginConfigResponse,
  OidcLoginConfig,
  SessionMembership,
  SessionUser,
};

export interface SessionCredentials {
  email: string;
  password: string;
  /** Optional: when omitted the server auto-selects a sole membership or
   * returns the membership list for the UI's tenant picker. */
  tenant?: string;
  dataset?: string;
}

export interface SessionResult {
  /** Null when the user must still pick a tenant from `memberships`. */
  tenant: string | null;
  dataset: string | null;
  memberships: SessionMembership[];
}

export interface WhoamiDataset {
  id: string;
  slug: string;
  is_default: boolean;
}

export interface WhoamiResponse {
  user?: {
    id: string;
    email: string;
    display_name: string | null;
    is_instance_admin: boolean;
  };
  memberships: Array<{
    tenant_id: string;
    role: "admin" | "member" | "viewer";
  }>;
  tenant: { id: string; slug: string; name: string };
  datasets: WhoamiDataset[];
  default_dataset: string | null;
}

/** Create a session: the server validates the credentials and sets the
 * HttpOnly session cookie. Throws `ApiError` with the server's message on
 * invalid credentials. */
export async function createSession(
  creds: SessionCredentials,
): Promise<SessionResult> {
  const res = await retryingFetch("/ui/session", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      email: creds.email,
      password: creds.password,
      ...(creds.tenant ? { tenant: creds.tenant } : {}),
      ...(creds.dataset ? { dataset: creds.dataset } : {}),
    }),
  });
  if (!res.ok) {
    const body = (await res.json().catch(() => null)) as {
      error?: string;
    } | null;
    throw new ApiError(
      body?.error ?? `Login failed (${res.status})`,
      res.status,
      retryAfterMsFrom(res),
    );
  }
  return (await res.json()) as SessionResult;
}

/** Log out: the server clears the session cookie. */
export async function deleteSession(): Promise<void> {
  const res = await retryingFetch("/ui/session", { method: "DELETE" });
  if (!res.ok) {
    throw new ApiError(
      `Logout failed (${res.status})`,
      res.status,
      retryAfterMsFrom(res),
    );
  }
}

/** Fetch the authenticated tenant and its datasets. Throws `ApiError`
 * (404 on servers without the endpoint, 401 when unauthenticated). Pass
 * `tenant` to scope the lookup to a specific tenant (e.g. right after
 * picking one at login) instead of the current tenant context. */
export async function whoami(tenant?: string): Promise<WhoamiResponse> {
  const headers = tenant
    ? { Accept: "application/json", "X-Tenant-ID": tenant }
    : tenantHeaders();
  const res = await retryingFetch("/api/v1/whoami", { headers });
  if (!res.ok) {
    throw new ApiError(
      `whoami failed (${res.status})`,
      res.status,
      retryAfterMsFrom(res),
    );
  }
  return (await res.json()) as WhoamiResponse;
}
