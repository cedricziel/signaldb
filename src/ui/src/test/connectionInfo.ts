// Shared `GET /api/v1/connection` fixture — used by both `api/connection.test.ts`
// (the wrapper's own contract) and `features/management/Instrumentation.test.tsx`
// (the page that consumes it), so the two suites can't drift out of sync with
// each other or with the router's real values.
import type { ConnectionInfoResponse } from "../api/gen";

/** A deployment with a dedicated ingest host — distinct from
 * `window.location.hostname` under test (jsdom's default, `localhost`), so a
 * leaked hostname-derived fallback would be caught. `tls` toggles both the
 * scheme in `url` and the `tls` flag snippets branch on. */
export function connectionInfoBody(
  overrides: Partial<ConnectionInfoResponse> & { tls?: boolean } = {},
): ConnectionInfoResponse {
  const { tls = true, ...rest } = overrides;
  const scheme = tls ? "https" : "http";
  const base: ConnectionInfoResponse = {
    tenant_id: "acme",
    dataset_id: "production",
    public_endpoints_configured: true,
    headers: {
      authorization: "Bearer <api-key>",
      "x-tenant-id": "acme",
      "x-dataset-id": "production",
    },
    ingest: {
      otlp_grpc: {
        url: `${scheme}://ingest.acme.example.com:4317`,
        authority: "ingest.acme.example.com:4317",
        tls,
        protocol: "grpc",
        signals: ["traces", "logs", "metrics", "profiles"],
      },
      otlp_http: {
        url: `${scheme}://ingest.acme.example.com:4318`,
        tls,
        protocol: "http/protobuf",
        paths: {
          traces: "/v1/traces",
          logs: "/v1/logs",
          metrics: "/v1/metrics",
          profiles: "/v1development/profiles",
        },
      },
      prometheus_remote_write: `${scheme}://ingest.acme.example.com:4318/api/v1/write`,
    },
    query: {
      api_url: "https://acme.example.com",
      query_ir: "/api/v1/query",
      openapi: "/api/v1/openapi.json",
      compat: {
        tempo: "/tempo/api",
        loki: "/loki/api/v1",
        prometheus: "/prometheus/api/v1",
        pyroscope: "/pyroscope",
      },
    },
    required_scopes: {
      ingest: [
        "metrics:write",
        "logs:write",
        "traces:write",
        "profiles:write",
      ],
      query: ["traces:read", "logs:read", "metrics:read", "profiles:read"],
    },
    otel_env: {
      OTEL_EXPORTER_OTLP_ENDPOINT: `${scheme}://ingest.acme.example.com:4317`,
      OTEL_EXPORTER_OTLP_PROTOCOL: "grpc",
      OTEL_EXPORTER_OTLP_HEADERS:
        "authorization=Bearer <api-key>,x-tenant-id=acme,x-dataset-id=production",
    },
    notes: [],
  };
  return { ...base, ...rest };
}
