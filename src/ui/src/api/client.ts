// Configures the shared generated OpenAPI client. Importing this module for
// its side effects wires the client up before any SDK call runs:
//
//   * a same-origin base URL, so generated operation paths like
//     `/api/v1/tenants/...` are requested verbatim against the current origin
//     (the router serves the UI and the API from the same host); and
//   * a request interceptor that attaches the tenant/dataset headers to every
//     outgoing request that doesn't already name its tenant, mirroring what
//     the hand-written clients used to do via `tenantHeaders()`; and
//   * `retryingFetch` as the client's fetch, so every generated operation
//     backs off on throttling (429) and idempotent transient failures the same
//     way the Rust SDK does (see `client-retry-on-throttle`); and
//   * `withProxyLoginRecovery` on top of that, so a reverse proxy's expired
//     login (see `lib/proxyLoginRecovery.ts`) gets caught and recovered from
//     on every generated operation too.
import { client } from "./gen/client.gen";
import { retryingFetch, tenantHeaders } from "./http";
import { withProxyLoginRecovery } from "../lib/proxyLoginRecovery";

client.setConfig({ baseUrl: "", fetch: withProxyLoginRecovery(retryingFetch) });

client.interceptors.request.use((request) => {
  // A call that names its tenant itself (`whoami(tenant)` right after a
  // tenant is picked at login) owns its tenant context.
  if (request.headers.has("X-Tenant-ID")) return request;
  for (const [key, value] of Object.entries(tenantHeaders())) {
    request.headers.set(key, value);
  }
  return request;
});
