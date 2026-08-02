// Configures the shared generated OpenAPI client. Importing this module for
// its side effects wires the client up before any SDK call runs:
//
//   * a same-origin base URL, so generated operation paths like
//     `/api/v1/manage/...` are requested verbatim against the current origin
//     (the router serves the UI and the API from the same host); and
//   * a request interceptor that attaches the tenant/dataset headers to every
//     outgoing request, mirroring what the hand-written clients used to do via
//     `tenantHeaders()`.
import { client } from "./gen/client.gen";
import { tenantHeaders } from "./http";

client.setConfig({ baseUrl: "" });

client.interceptors.request.use((request) => {
  for (const [key, value] of Object.entries(tenantHeaders())) {
    request.headers.set(key, value);
  }
  return request;
});
