// Tenant management API, layered over the generated OpenAPI SDK. The exported
// function names and signatures are the stable surface the management UI
// depends on; internally each call delegates to a generated `manage*`
// operation and unwraps the result envelope back into the historical
// contract: the response data on success, an `ApiError` carrying the HTTP
// status on failure (so `isAuthError` keeps working on 401).
import "./client";

import {
  createTenantTables,
  listTenantTables,
  manageCreateApiKey,
  manageCreateDataset,
  manageCreateTenant,
  manageDeleteDataset,
  manageGetSchema,
  manageListApiKeys,
  manageListMemberships,
  manageRemoveMembership,
  manageRevokeApiKey,
  manageUpdateApiKey,
  manageUpsertMembership,
  type CreateTenantTablesResponse,
  type ListTablesResponse,
  type ManageApiKeyResponse,
  type ManageCreatedApiKey,
  type ManageCreatedTenant,
  type ManageDatasetResponse,
  type ManageSchemaResponse,
  type MembershipResponse,
} from "./gen";
import { ApiError, retryAfterMsFrom } from "./http";

/** Ingestion scopes an API key may be granted. */
export type IngestScope =
  "metrics:write" | "logs:write" | "traces:write" | "profiles:write";

/** Schema-registry scopes an API key may be granted. */
export type SchemaScope = "schema:read" | "schema:write";

/** Every scope selectable on the management UI. Narrower than the generated
 * `string[]`, this drives the scope picker; the vocabulary mirrors
 * `common::auth::API_KEY_SCOPES` (the read scopes are OAuth-only). */
export type ApiKeyScope = IngestScope | SchemaScope;

/** Scope picker groups with one-line descriptions, in display order. */
export const SCOPE_GROUPS: ReadonlyArray<{
  name: string;
  scopes: ReadonlyArray<{ scope: ApiKeyScope; description: string }>;
}> = [
  {
    name: "Ingestion",
    scopes: [
      { scope: "metrics:write", description: "Ingest metrics over OTLP" },
      { scope: "logs:write", description: "Ingest logs over OTLP" },
      { scope: "traces:write", description: "Ingest traces over OTLP" },
      { scope: "profiles:write", description: "Ingest profiles over OTLP" },
    ],
  },
  {
    name: "Schema",
    scopes: [
      {
        scope: "schema:read",
        description:
          "Read the schema registry: registries and attribute, entity, and metric lookups",
      },
      {
        scope: "schema:write",
        description: "Create, replace, validate, and delete custom registries",
      },
    ],
  },
];

/** All selectable scopes, flattened in display order. */
export const ALL_SCOPES: ReadonlyArray<ApiKeyScope> = SCOPE_GROUPS.flatMap(
  (group) => group.scopes.map((entry) => entry.scope),
);

/** API key as returned by the management API. Structurally the generated
 * wire type (scopes surface as `string[]`). */
export type ManagedApiKey = ManageApiKeyResponse;

/** Tenant membership as returned by the management API. */
export type ManagedMembership = MembershipResponse;

/** Logical + physical schema, as returned by the management API. */
export type ManagedSchema = ManageSchemaResponse;

/** Result envelope produced by the generated SDK (`RequestResult` with the
 * default `fields` response style). */
interface SdkResult<T> {
  data?: T;
  error?: unknown;
  response?: Response;
}

/** Unwrap a generated SDK result, preserving the error contract callers rely
 * on. The SDK does not throw by default: it returns `error` set (and
 * `response` unset on a network/URL error) instead. Re-throw as `ApiError`
 * with the HTTP status so `isAuthError(401)` keeps working. */
function unwrap<T>(result: SdkResult<T>): T {
  const { error, response } = result;
  if (error !== undefined || !response?.ok) {
    const status = response?.status ?? 0;
    const message =
      (error as { error?: string } | undefined)?.error ??
      `Management request failed (${status})`;
    throw new ApiError(message, status, retryAfterMsFrom(response));
  }
  return result.data as T;
}

export const listApiKeys = async (tenant: string): Promise<ManagedApiKey[]> =>
  unwrap(await manageListApiKeys({ path: { tenant_id: tenant } }));

export const createApiKey = async (
  tenant: string,
  input: { name?: string; dataset_id?: string; scopes: ApiKeyScope[] },
): Promise<ManageCreatedApiKey> =>
  unwrap(
    await manageCreateApiKey({ path: { tenant_id: tenant }, body: input }),
  );

/** Change a live key's scopes and/or dataset restriction without rotating
 * its secret; absent fields are left untouched. Revoked keys are rejected. */
export const updateApiKey = async (
  tenant: string,
  keyId: string,
  input: { scopes?: ApiKeyScope[]; dataset_id?: string },
): Promise<ManagedApiKey> =>
  unwrap(
    await manageUpdateApiKey({
      path: { tenant_id: tenant, key_id: keyId },
      body: input,
    }),
  );

export const revokeApiKey = async (
  tenant: string,
  keyId: string,
): Promise<void> => {
  unwrap(
    await manageRevokeApiKey({ path: { tenant_id: tenant, key_id: keyId } }),
  );
};

export const createDataset = async (
  tenant: string,
  name: string,
): Promise<ManageDatasetResponse> =>
  unwrap(
    await manageCreateDataset({ path: { tenant_id: tenant }, body: { name } }),
  );

export const deleteDataset = async (
  tenant: string,
  name: string,
): Promise<void> => {
  unwrap(
    await manageDeleteDataset({
      path: { tenant_id: tenant, dataset_name: name },
    }),
  );
};

export const createTenant = async (input: {
  id: string;
  name: string;
  default_dataset?: string;
}): Promise<ManageCreatedTenant> =>
  unwrap(await manageCreateTenant({ body: input }));

export const listMemberships = async (
  tenant: string,
): Promise<ManagedMembership[]> =>
  unwrap(await manageListMemberships({ path: { tenant_id: tenant } }));

export const upsertMembership = async (
  tenant: string,
  input: { email: string; role: ManagedMembership["role"] },
): Promise<ManagedMembership> =>
  unwrap(
    await manageUpsertMembership({ path: { tenant_id: tenant }, body: input }),
  );

export const getSchema = async (): Promise<ManagedSchema> =>
  unwrap(await manageGetSchema());

export const removeMembership = async (
  tenant: string,
  userId: string,
): Promise<void> => {
  unwrap(
    await manageRemoveMembership({
      path: { tenant_id: tenant, user_id: userId },
    }),
  );
};

/** The tenant's provisioned signal tables. */
export type ManagedTables = ListTablesResponse;

export const listTables = async (tenant: string): Promise<ManagedTables> =>
  unwrap(await listTenantTables({ path: { tenant_id: tenant } }));

/** Provision (create) the tenant's enabled signal tables — the manual
 * trigger from the table-provisioning docs. */
export const provisionTables = async (
  tenant: string,
): Promise<CreateTenantTablesResponse> =>
  unwrap(await createTenantTables({ path: { tenant_id: tenant } }));
