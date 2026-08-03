// Native Query IR client, layered over the generated OpenAPI SDK. The UI never
// hand-writes the HTTP call — it delegates to the generated `queryIr` operation
// and unwraps the result envelope, mirroring api/management.ts.
import "./client";

import { queryIr, type QueryIrRequest, type QueryIrResponse } from "./gen";
import { ApiError, tenantHeaders } from "./http";

/** Submit an IR document and return the enveloped result. */
export async function runIrQuery(
  doc: QueryIrRequest,
): Promise<QueryIrResponse> {
  const res = await queryIr({ body: doc, headers: tenantHeaders() });
  if (res.error || !res.data) {
    const status = res.response?.status ?? 500;
    const message =
      typeof res.error === "string" ? res.error : `IR query failed (${status})`;
    throw new ApiError(message, status);
  }
  return res.data;
}
