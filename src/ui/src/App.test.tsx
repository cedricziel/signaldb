import { screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { App } from "./App";
import {
  emptyLabels,
  emptyMatrix,
  emptyStreams,
  renderWithClient,
  stubFetchRoutes,
} from "./test/render";

afterEach(() => {
  vi.unstubAllGlobals();
  window.history.replaceState(null, "", "/");
});

describe("App", () => {
  it("renders the shell with the product mark and explore tabs", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/labels?", body: emptyLabels },
    ]);
    renderWithClient(<App />);
    expect(screen.getByRole("banner")).toHaveTextContent(/signaldb/i);
    expect(screen.getByRole("tab", { name: "Logs" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    expect(
      await screen.findByText(/No log lines match this query/),
    ).toBeInTheDocument();
  });

  it("changes the tenant context from the top bar", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/labels?", body: emptyLabels },
    ]);
    renderWithClient(<App />);
    const user = (await import("@testing-library/user-event")).default;
    await user.click(
      screen.getByTitle("Tenant / dataset context for all queries"),
    );
    await user.clear(screen.getByLabelText("Tenant"));
    await user.type(screen.getByLabelText("Tenant"), "acme");
    await user.clear(screen.getByLabelText("Dataset"));
    await user.type(screen.getByLabelText("Dataset"), "prod");
    await user.click(screen.getByRole("button", { name: "Apply" }));
    expect(window.location.search).toContain("tenant=acme");
    expect(window.location.search).toContain("dataset=prod");
    expect(screen.getByRole("button", { name: /acme/ })).toHaveTextContent(
      "acme·prod",
    );
  });

  it("switches signals via tabs", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyMatrix },
      { match: "/labels?", body: emptyLabels },
      { match: "/tempo/api/search", body: { traces: [], metrics: {} } },
    ]);
    renderWithClient(<App />);
    screen.getByRole("tab", { name: "Traces" }).click();
    expect(await screen.findByLabelText("Trace ID")).toBeInTheDocument();
    expect(window.location.search).toContain("signal=traces");
  });
});
