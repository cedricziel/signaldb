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

  it("switches signals via tabs", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyMatrix },
      { match: "/labels?", body: emptyLabels },
    ]);
    renderWithClient(<App />);
    screen.getByRole("tab", { name: "Traces" }).click();
    expect(
      await screen.findByText(/Trace view lands in phase 2/),
    ).toBeInTheDocument();
    expect(window.location.search).toContain("signal=traces");
  });
});
