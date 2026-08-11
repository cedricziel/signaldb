// Log-record counterpart of `sessionSpanProcessor.ts`: stamps the same
// request-scoped context onto every log record at emit time.
//
//   session.id  — the RUM session (see `session.ts`), so all telemetry from
//                 one user session — spans and log records alike — can be
//                 followed together.
//   tenant.id / dataset.id — the current multi-tenant context (mirrors the
//                 X-Tenant-ID / X-Dataset-ID request headers the API clients
//                 send).
//
// Set at `onEmit` rather than baked into the Resource for the same reason as
// the span version: both can change over the page's lifetime without a
// reload.

import type { Context } from "@opentelemetry/api";
import type { LogRecordProcessor, SdkLogRecord } from "@opentelemetry/sdk-logs";
import { getTenantContext } from "../api/http";
import type { SessionManager } from "./session";

export class SessionLogRecordProcessor implements LogRecordProcessor {
  constructor(private readonly session: SessionManager) {}

  onEmit(logRecord: SdkLogRecord, _context?: Context): void {
    logRecord.setAttribute("session.id", this.session.getId());
    const { tenant, dataset } = getTenantContext();
    if (tenant) logRecord.setAttribute("tenant.id", tenant);
    if (dataset) logRecord.setAttribute("dataset.id", dataset);
  }

  forceFlush(): Promise<void> {
    return Promise.resolve();
  }

  shutdown(): Promise<void> {
    return Promise.resolve();
  }
}
