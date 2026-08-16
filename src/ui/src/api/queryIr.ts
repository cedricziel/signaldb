// Native Query IR client, layered over the generated OpenAPI SDK. The UI never
// hand-writes the HTTP call — it delegates to the generated `queryIr` operation
// and unwraps the result envelope, mirroring api/management.ts.
import "./client";

import { queryIr, type QueryIrRequest, type QueryIrResponse } from "./gen";
import { ApiError, retryAfterMsFrom, tenantHeaders } from "./http";

/** Submit an IR document and return the enveloped result. */
export async function runIrQuery(
  doc: QueryIrRequest,
): Promise<QueryIrResponse> {
  const res = await queryIr({ body: doc, headers: tenantHeaders() });
  if (res.error || !res.data) {
    const status = res.response?.status ?? 500;
    // `error` is the rate-limited/`ApiErrorBody` envelope's human-readable
    // field (see `ApiErrorBody` in `./gen`), never a bare string — but the
    // request can also fail before a body decodes, so check defensively.
    const error: unknown = res.error;
    const detail =
      error && typeof error === "object" && "error" in error
        ? String((error as { error: unknown }).error)
        : undefined;
    throw new ApiError(
      detail ?? `IR query failed (${status})`,
      status,
      retryAfterMsFrom(res.response),
    );
  }
  return res.data;
}
