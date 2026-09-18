// GitHub source-context lookup, layered over the generated OpenAPI SDK — the
// "View source" affordance behind stack frames in the trace detail, errors,
// and profiles views (see docs/users/explore-ui.md's "View source
// (GitHub)"). Same shape as api/github.ts: delegates to the generated
// `sourceContext` operation and unwraps the result into the historical
// contract (data on success, `ApiError` on failure).
import "./client";

import {
  sourceContext,
  sourceContextAvailability,
  type SourceContextAvailability,
  type SourceContextRequest,
  type SourceContextResponse,
  type SourceSnippet,
  type SourceContextStatus,
  type UnavailableReason,
} from "./gen";
import { unwrapErrorEnvelope } from "./http";

export type {
  SourceContextAvailability,
  SourceContextRequest,
  SourceContextResponse,
  SourceSnippet,
  SourceContextStatus,
  UnavailableReason,
};

/** `GET /api/v1/tenants/{id}/source-context`: whether `[github]` is
 * configured and the tenant has linked an installation — the read-level
 * probe behind the "View source" gate (`lib/useSourceContextEnabled.ts`).
 * Unlike `listGithubInstallations`, any signal reader can call this, not
 * just a tenant manager. */
export const fetchSourceContextAvailability = async (
  tenant: string,
): Promise<SourceContextAvailability> =>
  unwrapErrorEnvelope(
    await sourceContextAvailability({ path: { tenant_id: tenant } }),
    "Source context availability",
  );

/** `POST /api/v1/tenants/{id}/source-context`: the bounded snippet around one
 * frame's file/line, from the tenant's linked GitHub repositories. Always
 * resolves — even a well-formed request that can't be served comes back as
 * `status: "unavailable"` with a `reason`, never a rejection; this only
 * throws (`ApiError`) on a malformed request or an authorization failure. */
export const fetchSourceContext = async (
  tenant: string,
  request: SourceContextRequest,
): Promise<SourceContextResponse> =>
  unwrapErrorEnvelope(
    await sourceContext({ path: { tenant_id: tenant }, body: request }),
    "Source context",
  );
