// Schema registry API, layered over the generated OpenAPI SDK. Every call
// delegates to a generated `schema*` operation and unwraps the result
// envelope into data-or-`ApiError` (same contract as `api/management.ts`).
import "../../api/client";

import {
  schemaCreateRegistry,
  schemaDeleteRegistry,
  schemaGetRegistry,
  schemaListRegistries,
  schemaReplaceRegistry,
  schemaResolveAttribute,
  schemaResolveEntity,
  schemaResolveMetric,
  schemaSearchEntities,
  schemaSearchMetrics,
  schemaValidateRegistry,
  type AttributeHit,
  type AttributeResolution,
  type EntityHit,
  type EntityResolution,
  type MetricHit,
  type MetricResolution,
  type RegistryResponse,
  type RegistrySummary,
  type ValidationReport,
} from "../../api/gen";
import { ApiError, retryAfterMsFrom } from "../../api/http";

export type {
  AttributeHit,
  AttributeResolution,
  EntityHit,
  EntityResolution,
  MetricHit,
  MetricResolution,
  RegistryResponse,
  RegistrySummary,
  ValidationReport,
};

/** A registry document as uploaded (Weaver model), untyped beyond a map. */
export type RegistryDocument = Record<string, unknown>;

interface SdkResult<T> {
  data?: T;
  error?: unknown;
  response?: Response;
}

function unwrap<T>(result: SdkResult<T>): T {
  const { error, response } = result;
  if (error !== undefined || !response?.ok) {
    const status = response?.status ?? 0;
    const message =
      (error as { error?: string } | undefined)?.error ??
      `Schema request failed (${status})`;
    throw new ApiError(message, status, retryAfterMsFrom(response));
  }
  return result.data as T;
}

export const listRegistries = async (): Promise<RegistrySummary[]> =>
  unwrap(await schemaListRegistries()).registries;

export const getRegistry = async (
  namespace: string,
  version: string,
): Promise<RegistryResponse> =>
  unwrap(await schemaGetRegistry({ path: { namespace, version } }));

export const validateRegistry = async (
  document: RegistryDocument,
): Promise<ValidationReport> =>
  unwrap(await schemaValidateRegistry({ body: document }));

export const createRegistry = async (
  document: RegistryDocument,
): Promise<RegistrySummary> =>
  unwrap(await schemaCreateRegistry({ body: document }));

export const replaceRegistry = async (
  namespace: string,
  version: string,
  document: RegistryDocument,
): Promise<RegistrySummary> =>
  unwrap(
    await schemaReplaceRegistry({
      path: { namespace, version },
      body: document,
    }),
  );

export const deleteRegistry = async (
  namespace: string,
  version: string,
): Promise<void> => {
  unwrap(await schemaDeleteRegistry({ path: { namespace, version } }));
};

export const resolveAttribute = async (
  key: string,
): Promise<AttributeResolution> =>
  unwrap(await schemaResolveAttribute({ path: { key } }));

export const resolveEntity = async (name: string): Promise<EntityResolution> =>
  unwrap(await schemaResolveEntity({ path: { name } }));

/** Every entity type visible to the tenant, bundled registries and its own.
 * The catalog derives its entity types from this (see
 * `features/catalog/deriveEntityTypes.ts`); the server caps `limit` at 200,
 * which is well clear of the 64 the bundled OTel registry declares. */
export const searchEntities = async (limit = 200): Promise<EntityHit[]> =>
  (await unwrap(await schemaSearchEntities({ query: { limit } }))).hits;

export const resolveMetric = async (name: string): Promise<MetricResolution> =>
  unwrap(await schemaResolveMetric({ path: { name } }));

/** Metric definitions whose name starts with `prefix`, across the visible
 * registries. The server caps `limit` at 200 and offers no cursor, so a
 * prefix broad enough to overflow that returns a truncated namespace — call
 * it with a segment (`system`, `process`), not with the empty string. */
export const searchMetrics = async (
  prefix: string,
  limit = 200,
): Promise<MetricHit[]> =>
  (await unwrap(await schemaSearchMetrics({ query: { prefix, limit } }))).hits;

/** What kind of definition a lookup landed on. */
export type DefinitionKind = "attributes" | "entities" | "metrics";

export type LookupResult =
  | { kind: "attributes"; hits: AttributeHit[] }
  | { kind: "entities"; hits: EntityHit[] }
  | { kind: "metrics"; hits: MetricHit[] }
  | null;

/** Resolve a name as an attribute key, then an entity, then a metric; the
 * first kind with any hit wins. A 404 from one kind falls through. */
export async function lookup(name: string): Promise<LookupResult> {
  const attempt = async <T>(fn: () => Promise<{ hits: T[] }>): Promise<T[]> => {
    try {
      return (await fn()).hits;
    } catch (e) {
      if (e instanceof ApiError && e.status === 404) return [];
      throw e;
    }
  };
  const attributes = await attempt(() => resolveAttribute(name));
  if (attributes.length > 0) return { kind: "attributes", hits: attributes };
  const entities = await attempt(() => resolveEntity(name));
  if (entities.length > 0) return { kind: "entities", hits: entities };
  const metrics = await attempt(() => resolveMetric(name));
  if (metrics.length > 0) return { kind: "metrics", hits: metrics };
  return null;
}
