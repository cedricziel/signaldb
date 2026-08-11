// OpenTelemetry browser instrumentation for the SignalDB UI.
//
// Goals:
//   * End-to-end correlation — the auto fetch/XHR instrumentation injects a
//     W3C `traceparent` into every same-origin API call (/tempo, /loki,
//     /prometheus, /api, /ui/session), so a click in the UI and the querier
//     spans it triggers land in one trace.
//   * Followable sessions — every span carries `session.id` (see
//     `SessionSpanProcessor`), plus the active `tenant.id` / `dataset.id`.
//
// This is vanilla OpenTelemetry (no vendor SDK). Export is opt-in: set
// SIGNALDB_OTLP_ENDPOINT to ship spans; otherwise the SDK still runs so
// `traceparent` propagation works (and DEV prints spans to the console).
//
// `zone.js` is imported for `ZoneContextManager`, which keeps the active span
// alive across async boundaries (promises, timers, event handlers) so child
// spans nest under the interaction that caused them. It patches global async
// primitives, so this module must only be imported from the browser entry
// point (`main.tsx`) — never from test or library code.
import "zone.js";

import {
  context as apiContext,
  SpanStatusCode,
  trace,
} from "@opentelemetry/api";
import {
  CompositePropagator,
  W3CBaggagePropagator,
  W3CTraceContextPropagator,
} from "@opentelemetry/core";
import { ZoneContextManager } from "@opentelemetry/context-zone";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { getWebAutoInstrumentations } from "@opentelemetry/auto-instrumentations-web";
import { registerInstrumentations } from "@opentelemetry/instrumentation";
import { DocumentLoadInstrumentation } from "@opentelemetry/instrumentation-document-load";
import {
  defaultResource,
  resourceFromAttributes,
} from "@opentelemetry/resources";
import {
  BatchSpanProcessor,
  ConsoleSpanExporter,
  SimpleSpanProcessor,
  type SpanExporter,
  type SpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { WebTracerProvider } from "@opentelemetry/sdk-trace-web";
import {
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_VERSION,
} from "@opentelemetry/semantic-conventions";
import { getDefaultSessionManager } from "./session";
import { NavigationSpanProcessor } from "./navigationSpanProcessor";
import { ServerCorrelationSpanProcessor } from "./serverCorrelationSpanProcessor";
import { documentTraceParentContext } from "./documentTraceContext";
import { SessionSpanProcessor } from "./sessionSpanProcessor";
import { resolveExportConfig, resolveServiceName } from "./runtimeConfig";

// Runtime config the router injected via `/ui/runtime-config.js` before the app
// booted (see runtimeConfig.ts). Absent in tests/SSR and when the script 404s.
const RUNTIME_CONFIG =
  typeof window !== "undefined"
    ? window.__SIGNALDB_RUNTIME_CONFIG__
    : undefined;

const BUILD_TIME_SERVICE_NAME =
  typeof __SIGNALDB_TELEMETRY_SERVICE_NAME__ !== "undefined" &&
  __SIGNALDB_TELEMETRY_SERVICE_NAME__
    ? __SIGNALDB_TELEMETRY_SERVICE_NAME__
    : "signaldb-ui";

const SERVICE_NAME = resolveServiceName(
  RUNTIME_CONFIG,
  BUILD_TIME_SERVICE_NAME,
);

const SERVICE_VERSION =
  typeof __SIGNALDB_UI_VERSION__ !== "undefined"
    ? __SIGNALDB_UI_VERSION__
    : "0.0.0";

// Endpoint baked in at build time; the runtime config (above) takes precedence.
const BUILD_TIME_OTLP_ENDPOINT =
  typeof __SIGNALDB_OTLP_ENDPOINT__ !== "undefined"
    ? __SIGNALDB_OTLP_ENDPOINT__
    : "";

/** Tracer for hand-written spans around user-meaningful UI operations. */
export const tracer = trace.getTracer(SERVICE_NAME, SERVICE_VERSION);

/** Resolve an OTLP base to its traces URL, tolerating a full path or a bare
 * collector origin. */
function tracesUrl(endpoint: string): string {
  const base = endpoint.replace(/\/+$/, "");
  return base.endsWith("/v1/traces") ? base : `${base}/v1/traces`;
}

/** Choose the exporter: OTLP when configured (runtime config wins, else the
 * build-time endpoint), console in dev for feedback, otherwise none —
 * propagation still works with no exporter. */
function resolveExporter(): SpanExporter | null {
  const cfg = resolveExportConfig(RUNTIME_CONFIG, BUILD_TIME_OTLP_ENDPOINT);
  if (cfg)
    return new OTLPTraceExporter({
      url: tracesUrl(cfg.endpoint),
      headers: cfg.headers,
    });
  if (import.meta.env.DEV) return new ConsoleSpanExporter();
  return null;
}

let started = false;

/**
 * Initialise browser telemetry. Idempotent and browser-only — call once from
 * `main.tsx` before rendering so instrumentation patches are in place before
 * the app issues its first request.
 */
export function initTelemetry(): void {
  if (started || typeof window === "undefined") return;
  started = true;

  const resource = defaultResource().merge(
    resourceFromAttributes({
      [ATTR_SERVICE_NAME]: SERVICE_NAME,
      [ATTR_SERVICE_VERSION]: SERVICE_VERSION,
      "browser.language": navigator.language,
      "browser.mobile": /Mobi/i.test(navigator.userAgent),
    }),
  );

  const processors: SpanProcessor[] = [
    // Collapse the auto-instrumentation's `Navigation: <url>` span to a
    // low-cardinality name (URL moves to url.* attributes) before it is stamped
    // and exported.
    new NavigationSpanProcessor(),
    new SessionSpanProcessor(getDefaultSessionManager()),
    // Fallback correlation for deployments where documentLoad wasn't given a
    // real parent below (no <meta name="traceparent">, e.g. an older router
    // or a dev proxy): link it to the server span read back from the
    // navigation entry's Server-Timing traceparent instead.
    new ServerCorrelationSpanProcessor(),
  ];
  const exporter = resolveExporter();
  if (exporter) {
    // Console feedback should be immediate; real export is batched.
    processors.push(
      exporter instanceof ConsoleSpanExporter
        ? new SimpleSpanProcessor(exporter)
        : new BatchSpanProcessor(exporter),
    );
  }

  const provider = new WebTracerProvider({
    resource,
    spanProcessors: processors,
  });
  provider.register({
    contextManager: new ZoneContextManager(),
    propagator: new CompositePropagator({
      propagators: [
        new W3CTraceContextPropagator(),
        new W3CBaggagePropagator(),
      ],
    }),
  });

  registerInstrumentations({
    tracerProvider: provider,
    instrumentations: [
      getWebAutoInstrumentations({
        // Same-origin API calls (fetch and XHR) receive `traceparent`
        // automatically; we do not set propagateTraceHeaderCorsUrls, so no
        // headers leak to third-party (cross-origin) requests.
        //
        // clearTimingResources regularly clears the browser's
        // PerformanceResourceTiming buffer (capped at 250 entries in Chrome,
        // 150 in Safari), which each instrumentation reads to attach network
        // timing to its spans — without it, timing data silently stops once
        // the buffer fills.
        "@opentelemetry/instrumentation-fetch": { clearTimingResources: true },
        "@opentelemetry/instrumentation-xml-http-request": {
          clearTimingResources: true,
        },
        // Registered separately below, parented to the server's span — a
        // SpanProcessor (which is all a bundled instrumentation gets to work
        // with) cannot change a span's parent after creation.
        "@opentelemetry/instrumentation-document-load": { enabled: false },
      }),
    ],
  });

  // The documentLoad root span must be created with the server's span (from
  // `<meta name="traceparent">`, see documentTraceContext.ts) already active
  // as its parent — DocumentLoadInstrumentation calls `tracer.startSpan()`
  // with no explicit context, so it picks up whatever `context.active()`
  // resolves to at that moment. Registering it here, inside
  // `context.with(...)`, relies on ZoneContextManager to carry that context
  // into the `window.addEventListener('load', ...)` callback the
  // instrumentation installs during `enable()`. Falls back to the ambient
  // context (i.e. no special parent) when there is no meta tag to read.
  apiContext.with(documentTraceParentContext() ?? apiContext.active(), () => {
    registerInstrumentations({
      tracerProvider: provider,
      instrumentations: [new DocumentLoadInstrumentation()],
    });
  });

  installErrorCapture();
}

/** Surface uncaught errors and rejected promises as spans — the RUM signal
 * raw browser OTel does not capture on its own. */
function installErrorCapture(): void {
  window.addEventListener("error", (event) => {
    const span = tracer.startSpan("browser.error");
    span.recordException(event.error ?? new Error(event.message));
    span.setStatus({ code: SpanStatusCode.ERROR });
    span.end();
  });
  window.addEventListener("unhandledrejection", (event) => {
    const span = tracer.startSpan("browser.unhandledrejection");
    const reason = event.reason;
    span.recordException(
      reason instanceof Error ? reason : new Error(String(reason)),
    );
    span.setStatus({ code: SpanStatusCode.ERROR });
    span.end();
  });
}
