// Stamps `url.template` (see `routeTemplate.ts`) on every log record. The
// router sets the current template from `RootLayout` once it has matched a
// route; records emitted before that carry none. Must not import `zone.js`:
// `routes.tsx` imports this module.

import type { Context } from "@opentelemetry/api";
import type { LogRecordProcessor, SdkLogRecord } from "@opentelemetry/sdk-logs";

let template: string | undefined;

export function setRouteTemplate(next: string | undefined): void {
  template = next;
}

export function currentRouteTemplate(): string | undefined {
  return template;
}

export class RouteTemplateLogRecordProcessor implements LogRecordProcessor {
  onEmit(logRecord: SdkLogRecord, _context?: Context): void {
    const current = currentRouteTemplate();
    if (current !== undefined) logRecord.setAttribute("url.template", current);
  }

  forceFlush(): Promise<void> {
    return Promise.resolve();
  }

  shutdown(): Promise<void> {
    return Promise.resolve();
  }
}
