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
import { type SdkResult, unwrapSdkResult } from "./http";

/** Ingestion scopes an API key may be granted. */
export type IngestScope =
  "metrics:write" | "logs:write" | "traces:write" | "profiles:write";

/** Schema-registry scopes an API key may be granted. */
export type SchemaScope = "schema:read" | "schema:write";

/** Tenant self-management scope: the key may call the management API for
 * its own tenant. Explicit only — a legacy unscoped key never gains it. */
export type ManagementScope = "tenant:manage";

/** Every scope selectable on the management UI. Narrower than the generated
 * `string[]`, this drives the scope picker; the vocabulary mirrors
 * `common::auth::API_KEY_SCOPES` (the read scopes are OAuth-only). */
export type ApiKeyScope = IngestScope | SchemaScope | ManagementScope;

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
  {
    name: "Management",
    scopes: [
      {
        scope: "tenant:manage",
        description:
          "Manage this tenant's datasets, keys, and members through the management API (explicit only; automation stand-in for a tenant admin)",
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

/** Tenant membership as returned by the management API. `granted_by` is
 * `"local"` (granted via this API/CLI/MCP) or `"oidc_mapping"` (synced from
 * an OIDC group claim) — a local and a mapped row can coexist for the same
 * user, so callers must key lists on `user_id` + `granted_by`, not
 * `user_id` alone, and only offer removal for `"local"` rows (mapped rows
 * are managed by the IdP). */
export type ManagedMembership = MembershipResponse;

/** Logical + physical schema, as returned by the management API. */
export type ManagedSchema = ManageSchemaResponse;

/** Unwrap a generated SDK result with the "Management" label. */
function unwrap<T>(result: SdkResult<T>): T {
  return unwrapSdkResult(result, "Management");
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
