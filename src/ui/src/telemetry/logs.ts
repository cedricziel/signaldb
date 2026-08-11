// Event-based browser telemetry via `@opentelemetry/browser-instrumentation`
// (https://github.com/open-telemetry/opentelemetry-browser) — log records,
// not spans, complementing the span-based instrumentation in `index.ts`
// rather than replacing any of it. See the `frontend-instrumentation` skill
// for which instrumentations are enabled and why.
//
// Called from `initTelemetry()` once the trace `WebTracerProvider` is
// registered, so the global `ZoneContextManager` it installs is already in
// place — log records emitted from async handlers (clicks, errors) pick up
// the active span's context for free, no separate wiring needed here.

import { logs } from "@opentelemetry/api-logs";
import { registerInstrumentations } from "@opentelemetry/instrumentation";
import { ConsoleInstrumentation } from "@opentelemetry/browser-instrumentation/experimental/console";
import { ErrorsInstrumentation } from "@opentelemetry/browser-instrumentation/experimental/errors";
import { NavigationInstrumentation } from "@opentelemetry/browser-instrumentation/experimental/navigation";
import { NavigationTimingInstrumentation } from "@opentelemetry/browser-instrumentation/experimental/navigation-timing";
import { ResourceTimingInstrumentation } from "@opentelemetry/browser-instrumentation/experimental/resource-timing";
import { WebVitalsInstrumentation } from "@opentelemetry/browser-instrumentation/experimental/web-vitals";
import { OTLPLogExporter } from "@opentelemetry/exporter-logs-otlp-http";
import {
  BatchLogRecordProcessor,
  ConsoleLogRecordExporter,
  LoggerProvider,
  SimpleLogRecordProcessor,
  type LogRecordExporter,
  type LogRecordProcessor,
} from "@opentelemetry/sdk-logs";
import { resolveExportConfig } from "./runtimeConfig";
import { RUNTIME_CONFIG, buildResource } from "./resource";
import { sanitizeNavigationUrl } from "./sanitizeNavigationUrl";
import { SessionLogRecordProcessor } from "./sessionLogRecordProcessor";
import { getDefaultSessionManager } from "./session";

// Endpoint baked in at build time; the runtime config takes precedence. Same
// build-time variable as the trace exporter (see index.ts) — one config
// section, `[self_monitoring.frontend]`, drives both signals.
const BUILD_TIME_OTLP_ENDPOINT =
  typeof __SIGNALDB_OTLP_ENDPOINT__ !== "undefined"
    ? __SIGNALDB_OTLP_ENDPOINT__
    : "";

/** Resolve an OTLP base to its logs URL, tolerating a full path or a bare
 * collector origin. */
function logsUrl(endpoint: string): string {
  const base = endpoint.replace(/\/+$/, "");
  return base.endsWith("/v1/logs") ? base : `${base}/v1/logs`;
}

/** Choose the log exporter: OTLP when configured, console in dev, otherwise
 * none. Mirrors `resolveExporter()` in index.ts for the trace pipeline. */
function resolveLogExporter(): LogRecordExporter | null {
  const cfg = resolveExportConfig(RUNTIME_CONFIG, BUILD_TIME_OTLP_ENDPOINT);
  if (cfg)
    return new OTLPLogExporter({
      url: logsUrl(cfg.endpoint),
      headers: cfg.headers,
    });
  if (import.meta.env.DEV) return new ConsoleLogRecordExporter();
  return null;
}

let started = false;

/**
 * Initialise the browser log-record pipeline: a `LoggerProvider` plus the
 * event-based instrumentations from `@opentelemetry/browser-instrumentation`.
 * Idempotent; call once from `initTelemetry()`.
 */
export function initBrowserLogs(): void {
  if (started) return;
  started = true;

  const processors: LogRecordProcessor[] = [
    new SessionLogRecordProcessor(getDefaultSessionManager()),
  ];
  const exporter = resolveLogExporter();
  if (exporter) {
    processors.push(
      exporter instanceof ConsoleLogRecordExporter
        ? new SimpleLogRecordProcessor({ exporter })
        : new BatchLogRecordProcessor({ exporter }),
    );
  }

  const provider = new LoggerProvider({
    resource: buildResource(),
    processors,
  });
  logs.setGlobalLoggerProvider(provider);

  registerInstrumentations({
    loggerProvider: provider,
    instrumentations: [
      new WebVitalsInstrumentation(),
      new NavigationTimingInstrumentation(),
      new ResourceTimingInstrumentation({
        // Don't report on our own telemetry export requests — they are not
        // meaningful "resources" and would just add export-triggered export
        // noise.
        ignoreUrls: [/\/v1\/traces$/, /\/v1\/logs$/],
      }),
      // Complements, not replaces, the span-based Navigation collapse in
      // navigationSpanProcessor.ts: instrumentation-user-interaction renames
      // the active click span on any history.pushState/replaceState call
      // with no way to disable just that behavior, so both signals coexist.
      // sanitizeUrl redacts credentials/known-sensitive query params before
      // the URL reaches a log record (the upstream package's documented
      // `defaultSanitizeUrl` helper is not present in the pinned version).
      new NavigationInstrumentation({ sanitizeUrl: sanitizeNavigationUrl }),
      // Replaces installErrorCapture(): same window error/unhandledrejection
      // listeners, as log-record exception events instead of hand-rolled
      // browser.error/browser.unhandledrejection spans. Deliberate signal
      // change — see docs/users/explore-ui.md.
      new ErrorsInstrumentation(),
      // Scoped to error/warn only (not the full log/warn/error/info/debug
      // default): console.log/debug output routinely carries stack dumps,
      // query results, or accidentally-logged tokens that should not ship to
      // the backend by default.
      new ConsoleInstrumentation({ logMethods: ["error", "warn"] }),
    ],
  });
}
