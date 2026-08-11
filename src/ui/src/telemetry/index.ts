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
// Span-based instrumentation lives here; `logs.ts` (called at the end of
// `initTelemetry()`) adds complementary event-based instrumentation via
// `@opentelemetry/browser-instrumentation` — Web Vitals, navigation/resource
// timing, and (replacing this module's own error capture) exception log
// records. See the `frontend-instrumentation` skill for the full signal map.
//
// `zone.js` is imported for `ZoneContextManager`, which keeps the active span
// alive across async boundaries (promises, timers, event handlers) so child
// spans nest under the interaction that caused them. It patches global async
// primitives, so this module must only be imported from the browser entry
// point (`main.tsx`) — never from test or library code.
import "zone.js";

import { context as apiContext, trace } from "@opentelemetry/api";
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
  BatchSpanProcessor,
  ConsoleSpanExporter,
  SimpleSpanProcessor,
  type SpanExporter,
  type SpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { WebTracerProvider } from "@opentelemetry/sdk-trace-web";
import { getDefaultSessionManager } from "./session";
import { initBrowserLogs } from "./logs";
import { NavigationSpanProcessor } from "./navigationSpanProcessor";
import { ServerCorrelationSpanProcessor } from "./serverCorrelationSpanProcessor";
import { documentTraceParentContext } from "./documentTraceContext";
import { SessionSpanProcessor } from "./sessionSpanProcessor";
import { resolveExportConfig } from "./runtimeConfig";
import {
  buildResource,
  RUNTIME_CONFIG,
  SERVICE_NAME,
  SERVICE_VERSION,
} from "./resource";

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

  const resource = buildResource();

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

  // Log-record instrumentation (Web Vitals, navigation/resource timing,
  // exception capture, ...) — see logs.ts. Runs after provider.register()
  // above so its ZoneContextManager is already the global context manager;
  // log records emitted from async handlers pick up the active span's
  // context automatically.
  initBrowserLogs();
}
