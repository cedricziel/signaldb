// Stamps request-scoped context onto every span at start:
//
//   session.id  — the RUM session (see `session.ts`), so all spans from one
//                 user session can be followed together.
//   tenant.id / dataset.id — the current multi-tenant context (mirrors the
//                 X-Tenant-ID / X-Dataset-ID request headers the API clients
//                 send), so frontend spans line up with the backend's
//                 per-tenant traces.
//
// These are set at `onStart` rather than baked into the Resource because both
// change over the page's lifetime: the session can roll, and the user can
// switch tenant/dataset without a reload.

import type { Context } from "@opentelemetry/api";
import type { Span, SpanProcessor } from "@opentelemetry/sdk-trace-base";
import { getTenantContext } from "../api/http";
import type { SessionManager } from "./session";

export class SessionSpanProcessor implements SpanProcessor {
  constructor(private readonly session: SessionManager) {}

  onStart(span: Span, _parentContext?: Context): void {
    span.setAttribute("session.id", this.session.getId());
    const { tenant, dataset } = getTenantContext();
    if (tenant) span.setAttribute("tenant.id", tenant);
    if (dataset) span.setAttribute("dataset.id", dataset);
  }

  onEnd(): void {}

  shutdown(): Promise<void> {
    return Promise.resolve();
  }

  forceFlush(): Promise<void> {
    return Promise.resolve();
  }
}
