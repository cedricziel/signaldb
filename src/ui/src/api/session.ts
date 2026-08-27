// Client for the router's UI session endpoints (/ui/session) and the
// tenant-scoped whoami endpoint (/api/v1/whoami). The session cookie is
// HttpOnly — browser code never reads it; it only creates and clears it.

import "./client";
import { sessionConfig as sessionConfigOp } from "./gen";
import {
  ApiError,
  retryAfterMsFrom,
  retryingFetch,
  tenantHeaders,
  unwrapSdkResult,
} from "./http";

export interface SessionCredentials {
  email: string;
  password: string;
  /** Optional: when omitted the server auto-selects a sole membership or
   * returns the membership list for the UI's tenant picker. */
  tenant?: string;
  dataset?: string;
}

export interface SessionMembership {
  tenant_id: string;
  name: string;
  role: "admin" | "member" | "viewer";
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

/** The SSO offering, when OIDC is configured and currently usable. */
export interface SessionOidcOffering {
  /** Display label for the SSO button. */
  name: string;
}

/** Login surface reported by the unauthenticated `GET /ui/session/config`
 * probe: which doors the login panel should offer. */
export interface SessionConfig {
  /** Whether the email/password form should be shown. */
  password_enabled: boolean;
  /** The SSO button to offer, or `null` when OIDC isn't configured/usable. */
  oidc: SessionOidcOffering | null;
}

/** Path the SSO button navigates to (full-page redirect, never fetched). */
export const OIDC_START_PATH = "/ui/session/oidc/start";

/** Probe the login surface via the generated client. Throws on any
 * non-2xx/network failure so callers can fall back to the password form
 * (a probe failure must never block password login). */
export async function fetchSessionConfig(): Promise<SessionConfig> {
  const data = unwrapSdkResult(await sessionConfigOp(), "Session config");
  return { password_enabled: data.password_enabled, oidc: data.oidc ?? null };
}
