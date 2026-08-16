// Schema registry API (attribute semantics), layered over the generated
// OpenAPI SDK in the style of `management.ts`. Consumers get the wire types
// back on success and an `ApiError` carrying the HTTP status on failure.
import "./client";

import {
  schemaSearchAttributes,
  type AttributeHit,
  type AttributeResolution,
} from "./gen";
import { ApiError, retryAfterMsFrom } from "./http";

export type { AttributeHit, AttributeResolution };

/** Largest `keys=` batch one resolve call carries (the server's `limit` cap). */
export const SCHEMA_BATCH_LIMIT = 200;

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

/** Resolve exact keys in one round trip (`?keys=a,b,c`); one resolution per
 * requested key, empty `hits` for keys no visible registry defines. */
export async function resolveAttributes(
  keys: string[],
): Promise<AttributeResolution[]> {
  if (keys.length === 0) return [];
  if (keys.length > SCHEMA_BATCH_LIMIT) {
    throw new Error(
      `resolveAttributes: ${keys.length} keys exceeds ${SCHEMA_BATCH_LIMIT}`,
    );
  }
  const data = unwrap(
    await schemaSearchAttributes({
      query: { keys: keys.join(","), limit: keys.length },
    }),
  );
  return data.resolutions ?? [];
}

/** Prefix search over attribute definitions across the visible registries. */
export async function searchAttributes(
  prefix: string,
  limit = 20,
): Promise<AttributeHit[]> {
  const data = unwrap(
    await schemaSearchAttributes({ query: { prefix, limit } }),
  );
  return data.hits;
}
