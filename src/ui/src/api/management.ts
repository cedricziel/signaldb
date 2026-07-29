import { ApiError, tenantHeaders } from "./http";

export type IngestScope =
  | "metrics:write"
  | "logs:write"
  | "traces:write"
  | "profiles:write";

export interface ManagedApiKey {
  id: string;
  name: string | null;
  dataset_id: string | null;
  scopes: IngestScope[] | null;
  revoked: boolean;
}

export interface ManagedMembership {
  user_id: string;
  email: string;
  role: "admin" | "member" | "viewer";
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    ...init,
    headers: {
      ...tenantHeaders(),
      ...(init?.body ? { "Content-Type": "application/json" } : {}),
      ...init?.headers,
    },
  });
  if (!response.ok) {
    const body = (await response.json().catch(() => null)) as {
      error?: string;
    } | null;
    throw new ApiError(
      body?.error ?? `Management request failed (${response.status})`,
      response.status,
    );
  }
  if (response.status === 204) return undefined as T;
  return (await response.json()) as T;
}

export const listApiKeys = (tenant: string) =>
  request<ManagedApiKey[]>(`/api/v1/manage/tenants/${tenant}/api-keys`);

export const createApiKey = (
  tenant: string,
  input: { name?: string; dataset_id?: string; scopes: IngestScope[] },
) =>
  request<ManagedApiKey & { key: string }>(
    `/api/v1/manage/tenants/${tenant}/api-keys`,
    { method: "POST", body: JSON.stringify(input) },
  );

export const revokeApiKey = (tenant: string, keyId: string) =>
  request<void>(`/api/v1/manage/tenants/${tenant}/api-keys/${keyId}`, {
    method: "DELETE",
  });

export const createDataset = (tenant: string, name: string) =>
  request<{ id: string; name: string }>(
    `/api/v1/manage/tenants/${tenant}/datasets`,
    { method: "POST", body: JSON.stringify({ name }) },
  );

export const deleteDataset = (tenant: string, name: string) =>
  request<void>(
    `/api/v1/manage/tenants/${tenant}/datasets/${encodeURIComponent(name)}`,
    { method: "DELETE" },
  );

export const createTenant = (input: {
  id: string;
  name: string;
  default_dataset?: string;
}) =>
  request<{ id: string }>("/api/v1/manage/tenants", {
    method: "POST",
    body: JSON.stringify(input),
  });

export const listMemberships = (tenant: string) =>
  request<ManagedMembership[]>(
    `/api/v1/manage/tenants/${tenant}/memberships`,
  );

export const upsertMembership = (
  tenant: string,
  input: { email: string; role: ManagedMembership["role"] },
) =>
  request<ManagedMembership>(
    `/api/v1/manage/tenants/${tenant}/memberships`,
    { method: "PUT", body: JSON.stringify(input) },
  );

export const removeMembership = (tenant: string, userId: string) =>
  request<void>(
    `/api/v1/manage/tenants/${tenant}/memberships/${userId}`,
    { method: "DELETE" },
  );
