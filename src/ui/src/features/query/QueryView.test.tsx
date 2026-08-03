import { fireEvent, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import { client } from "../../api/gen/client.gen";
import { renderWithClient } from "../../test/render";
import { QueryView } from "./QueryView";

const realFetch = globalThis.fetch;
afterEach(() => client.setConfig({ baseUrl: "/", fetch: realFetch }));

/** Inject a fetch into the generated client that records the request URL +
 * parsed body and returns a fixed IR response. An absolute `baseUrl` is set so
 * the library's `new Request(url)` succeeds under jsdom (which rejects relative
 * URLs). The client invokes fetch with a `Request`. */
function stubIrFetch(body: unknown) {
  const calls: { url: string; body: unknown }[] = [];
  const testFetch = async (
    input: RequestInfo | URL,
    _init?: RequestInit,
  ): Promise<Response> => {
    const request = input as Request;
    const payload = await request
      .clone()
      .text()
      .catch(() => undefined);
    calls.push({
      url: request.url,
      body: payload ? JSON.parse(payload) : undefined,
    });
    return new Response(JSON.stringify(body), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  };
  client.setConfig({ baseUrl: "http://localhost", fetch: testFetch });
  return calls;
}

describe("QueryView", () => {
  // Task 9.2 — the view is chosen from the declared envelope before results.
  it("selects the view from the declared envelope up front", () => {
    stubIrFetch({});
    renderWithClient(<QueryView />);

    // Default `rows` → list view, no query run yet.
    expect(screen.getByTestId("ir-view-list")).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("result"), {
      target: { value: "series" },
    });
    expect(screen.getByTestId("ir-view-chart")).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("result"), {
      target: { value: "table" },
    });
    expect(screen.getByTestId("ir-view-table")).toBeInTheDocument();
  });

  // Task 9.1 — the builder emits a valid IR document via the generated client.
  it("emits an IR document to /api/v1/query and renders the rows result", async () => {
    const calls = stubIrFetch({
      result: "rows",
      window: { start_ns: 0, end_ns: 1 },
      columns: [{ name: "service_name", type: "string" }],
      rows: [["checkout"]],
    });
    renderWithClient(<QueryView />);

    fireEvent.click(screen.getByText("Run"));

    await waitFor(() => expect(calls.length).toBeGreaterThan(0));
    const first = calls[0]!;
    // The request went to the native IR endpoint via the generated client.
    expect(first.url).toContain("/api/v1/query");
    // The body is a structured IR document (versioned), not a dialect string.
    const doc = first.body as { irVersion?: number; from?: string };
    expect(doc.irVersion).toBe(1);
    expect(doc.from).toBe("logs");

    // The rows envelope renders.
    await screen.findByText("checkout");
  });
});
