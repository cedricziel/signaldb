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
    /** `service.namespace` — always `"signaldb"` server-side today, but
     * sourced from config rather than hardcoded in case that ever changes. */
    namespace?: string;
    /** The *backend's* build version (`CARGO_PKG_VERSION`) — distinct from
     * the UI bundle's own `service.version` (see `resource.ts`). Read by
     * `resolveServerVersion` onto the custom `signaldb.server.version`
     * attribute, so a frontend session can be correlated with the backend
     * build that served it. */
    version?: string;
    /** `deployment.environment.name` — the same value the backend's own
     * telemetry uses (`[self_monitoring].environment`). */
    deploymentEnvironment?: string;
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

/** `service.namespace`, when the router injected one. No build-time
 * fallback: unlike `service.name`, there is nothing meaningful to guess at
 * (e.g. bare `vite dev` with no router in front). */
export function resolveServiceNamespace(
  runtime: RuntimeConfig | undefined,
): string | undefined {
  return runtime?.telemetry?.namespace;
}

/** The backend's own build version, when the router injected one. Distinct
 * from the UI bundle's `service.version` — see `RuntimeConfig.version`. */
export function resolveServerVersion(
  runtime: RuntimeConfig | undefined,
): string | undefined {
  return runtime?.telemetry?.version;
}

/** `deployment.environment.name`, when the router injected one. */
export function resolveDeploymentEnvironment(
  runtime: RuntimeConfig | undefined,
): string | undefined {
  return runtime?.telemetry?.deploymentEnvironment;
}
