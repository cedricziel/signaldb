// The router's `GET /api/v1/connection` endpoint, layered over the generated
// OpenAPI SDK (mirrors the wrapping style in `management.ts`/`consent.ts`):
// the deployment's real, public-facing ingest/query/mcp endpoints for the
// current tenant, honoring `[public]` in `signaldb.toml` instead of the
// browser's own hostname.
import "./client";

import { connectionInfo as getConnectionInfo, type ConnectionInfoResponse } from "./gen";
import { ApiError, retryAfterMsFrom } from "./http";

interface SdkResult<T> {
  data?: T;
  error?: unknown;
  response?: Response;
}

/** Unwrap a generated SDK result into its data, re-throwing failures as
 * `ApiError` (with the HTTP status, so callers can react to specific codes). */
function unwrap<T>(result: SdkResult<T>): T {
  const { error, response } = result;
  if (error !== undefined || !response?.ok) {
    const status = response?.status ?? 0;
    const message =
      (error as { error?: string } | undefined)?.error ??
      `connection info failed (${status})`;
    throw new ApiError(message, status, retryAfterMsFrom(response));
  }
  return result.data as T;
}

/** Fetch the deployment's real ingest/query/mcp endpoints for the current
 * tenant. Same auth and tenant scoping as `whoami`. Throws `ApiError` on
 * failure — the only failure mode is transient, since this page and the
 * endpoint are served by the same router build. */
export async function connectionInfo(): Promise<ConnectionInfoResponse> {
  return unwrap(await getConnectionInfo());
}

export type {
  ConnectionHeaders,
  ConnectionInfoResponse,
  ConnectionIngest,
  ConnectionMcp,
  ConnectionOtelEnv,
  ConnectionQuery,
  ConnectionScopes,
  OtlpGrpcEndpoint,
  OtlpHttpEndpoint,
} from "./gen";
