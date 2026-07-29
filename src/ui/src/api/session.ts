// Client for the router's UI session endpoints (/ui/session) and the
// tenant-scoped whoami endpoint (/api/v1/whoami). The session cookie is
// HttpOnly — browser code never reads it; it only creates and clears it.

import { ApiError, tenantHeaders } from "./http";

export interface SessionCredentials {
  apiKey: string;
  tenant: string;
  dataset?: string;
}

export interface WhoamiDataset {
  id: string;
  slug: string;
  is_default: boolean;
}

export interface WhoamiResponse {
  tenant: { id: string; slug: string; name: string };
  datasets: WhoamiDataset[];
  default_dataset: string | null;
}

/** Create a session: the server validates the credentials and sets the
 * HttpOnly session cookie. Throws `ApiError` with the server's message on
 * invalid credentials. */
export async function createSession(creds: SessionCredentials): Promise<void> {
  const res = await fetch("/ui/session", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      api_key: creds.apiKey,
      tenant: creds.tenant,
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
    );
  }
}

/** Log out: the server clears the session cookie. */
export async function deleteSession(): Promise<void> {
  const res = await fetch("/ui/session", { method: "DELETE" });
  if (!res.ok) {
    throw new ApiError(`Logout failed (${res.status})`, res.status);
  }
}

/** Fetch the authenticated tenant and its datasets. Throws `ApiError`
 * (404 on servers without the endpoint, 401 when unauthenticated) — the
 * UI falls back to free-text tenant entry on any failure. */
export async function whoami(): Promise<WhoamiResponse> {
  const res = await fetch("/api/v1/whoami", { headers: tenantHeaders() });
  if (!res.ok) {
    throw new ApiError(`whoami failed (${res.status})`, res.status);
  }
  return (await res.json()) as WhoamiResponse;
}
