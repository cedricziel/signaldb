// Test helper for stubbing the generated OpenAPI client's transport.
//
// The client constructs its own `Request(url)` before any fetch mock runs
// (see client/client.gen.ts), and `Request` rejects relative URLs under
// jsdom/Node — so `vi.stubGlobal("fetch", ...)` alone doesn't work for calls
// that go through the generated client. An absolute `baseUrl` avoids that.
import { client } from "../api/gen/client.gen";

const realFetch = globalThis.fetch;

/** Reset the generated client to its default config after a test that
 * stubbed it, so later tests see the real transport again. */
export function resetApiClient(): void {
  client.setConfig({ baseUrl: "/", fetch: realFetch });
}

export interface RecordedRequest {
  url: string;
  body: unknown;
  headers: Record<string, string>;
}

/**
 * Stub the generated client's fetch with a fixed JSON response, recording
 * each request's URL, headers, and (if present) JSON body. Call
 * `resetApiClient()` (e.g. in `afterEach`) to restore the real transport.
 */
export function stubApiFetch(body: unknown, status = 200): RecordedRequest[] {
  const calls: RecordedRequest[] = [];
  const testFetch = async (input: RequestInfo | URL): Promise<Response> => {
    const request = input as Request;
    const payload = await request
      .clone()
      .text()
      .catch(() => undefined);
    calls.push({
      url: request.url,
      body: payload ? JSON.parse(payload) : undefined,
      headers: Object.fromEntries(request.headers.entries()),
    });
    return new Response(JSON.stringify(body), {
      status,
      headers: { "Content-Type": "application/json" },
    });
  };
  client.setConfig({ baseUrl: "http://localhost", fetch: testFetch });
  return calls;
}
