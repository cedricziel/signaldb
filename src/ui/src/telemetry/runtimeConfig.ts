// Runtime telemetry config resolution.
//
// The router serves `/ui/runtime-config.js`, which sets
// `window.__SIGNALDB_RUNTIME_CONFIG__` from the SignalDB config file BEFORE the
// app boots. This lets one container image enable browser telemetry export and
// point it at any endpoint via config alone — no rebuild. When no runtime
// config is present (e.g. that script 404s), we fall back to the build-time
// `SIGNALDB_OTLP_ENDPOINT` so local dev keeps working.
//
// This module is intentionally pure (no `zone.js`, no OTel imports) so it can
// be unit-tested without patching global async primitives — see
// `telemetry/index.ts` for the "never import from tests" rule.

/** Shape the router injects via `/ui/runtime-config.js`. */
export interface RuntimeConfig {
  telemetry?: {
    enabled?: boolean;
    endpoint?: string;
    apiKey?: string | null;
    tenantId?: string;
    datasetId?: string;
    serviceName?: string;
  };
}

/** A resolved browser OTLP export target. */
export interface ResolvedExport {
  /** OTLP/HTTP base URL (traces path appended by the caller). */
  endpoint: string;
  /** Auth/tenant headers sent on every export request. */
  headers: Record<string, string>;
}

/**
 * Resolve the export target, preferring the router-injected runtime config and
 * falling back to the build-time endpoint. Returns `null` when export is
 * disabled or unconfigured — the SDK still runs (so `traceparent` propagation
 * works), it just exports nothing.
 */
export function resolveExportConfig(
  runtime: RuntimeConfig | undefined,
  buildTimeEndpoint: string,
): ResolvedExport | null {
  const t = runtime?.telemetry;
  if (t?.enabled && t.endpoint) {
    const headers: Record<string, string> = {};
    // The ingest key is delivered to the browser deliberately (world-readable);
    // it must be an ingest-only key. See FrontendMonitoringConfig in the
    // backend for the security rationale.
    if (t.apiKey) headers["Authorization"] = `Bearer ${t.apiKey}`;
    if (t.tenantId) headers["X-Tenant-ID"] = t.tenantId;
    if (t.datasetId) headers["X-Dataset-ID"] = t.datasetId;
    return { endpoint: t.endpoint, headers };
  }
  // No runtime export config: fall back to the build-time endpoint with no auth
  // headers (a local dev collector, or one that does not require auth).
  if (buildTimeEndpoint) return { endpoint: buildTimeEndpoint, headers: {} };
  return null;
}

/** Resolve `service.name`, preferring the runtime config over the build-time
 * default. */
export function resolveServiceName(
  runtime: RuntimeConfig | undefined,
  buildTimeName: string,
): string {
  return runtime?.telemetry?.serviceName || buildTimeName;
}
