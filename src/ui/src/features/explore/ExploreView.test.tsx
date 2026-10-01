import { screen } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import { renderWithClient } from "../../test/render";
import { ExploreView } from "./ExploreView";

// The child views each issue their own queries and are covered by their own
// test files; ExploreView's own behavior (the signal tabs and the Live
// toggle) doesn't depend on what they render. Each mock also records the
// `state` it was handed, so tests can assert on the `live` flag ExploreView
// passes down rather than only the one it received.
const receivedStates: Record<string, ExploreState> = {};
function capturing(name: string, label: string) {
  return ({ state }: { state: ExploreState }) => {
    receivedStates[name] = state;
    return <div>{label}</div>;
  };
}
vi.mock("../catalog/CatalogView", () => ({
  CatalogView: capturing("catalog", "catalog view"),
}));
vi.mock("../errors/ErrorsView", () => ({
  ErrorsView: capturing("errors", "errors view"),
}));
vi.mock("../logs/LogsView", () => ({ LogsView: capturing("logs", "logs view") }));
vi.mock("../metrics/MetricsView", () => ({
  MetricsView: capturing("metrics", "metrics view"),
}));
vi.mock("../profiles/ProfilesView", () => ({
  ProfilesView: capturing("profiles", "profiles view"),
}));
vi.mock("../traces/TracesView", () => ({
  TracesView: capturing("traces", "traces view"),
}));
vi.mock("../query/QueryView", () => ({
  QueryView: capturing("query", "query view"),
}));

function renderView(state: Partial<ExploreState> = {}) {
  const update = vi.fn();
  renderWithClient(
    <MemoryRouter>
      <ExploreView state={{ ...DEFAULT_STATE, ...state }} update={update} />
    </MemoryRouter>,
  );
  return update;
}

describe("ExploreView Live toggle", () => {
  it("is enabled and toggles live on a tailable signal with a relative range", async () => {
    const update = renderView({ signal: "logs" });
    const btn = screen.getByRole("button", { name: /Live/ });
    expect(btn).not.toBeDisabled();
    expect(btn).toHaveAttribute("aria-pressed", "false");
    btn.click();
    expect(update).toHaveBeenCalledWith({ live: true });
  });

  it.each(["catalog", "errors", "query"] as const)(
    "disables Live on the %s view",
    (signal) => {
      renderView({ signal, live: true });
      const btn = screen.getByRole("button", { name: /Live/ });
      expect(btn).toBeDisabled();
      expect(btn).toHaveAttribute("aria-disabled", "true");
      expect(btn).toHaveAttribute(
        "title",
        "Live tail isn't available on this view",
      );
      // Not actually live-tailing on this view — don't show it pressed.
      expect(btn).toHaveAttribute("aria-pressed", "false");
    },
  );

  it("disables Live for an absolute time range with a range-specific title", () => {
    renderView({
      signal: "traces",
      range: { type: "absolute", fromMs: 0, toMs: 1000 },
    });
    const btn = screen.getByRole("button", { name: /Live/ });
    expect(btn).toBeDisabled();
    expect(btn).toHaveAttribute("title", "Live tail needs a relative time range");
  });

  it("stays enabled on traces with a relative range", () => {
    renderView({ signal: "traces" });
    expect(screen.getByRole("button", { name: /Live/ })).not.toBeDisabled();
  });
});

describe("ExploreView Live normalisation for child views", () => {
  it("normalises live to false for the child view on an absolute range", () => {
    renderView({
      signal: "metrics",
      live: true,
      range: { type: "absolute", fromMs: 0, toMs: 1000 },
    });
    expect(receivedStates["metrics"]!.live).toBe(false);
  });

  it("normalises live to false for the child view on a live-unsupported signal", () => {
    renderView({ signal: "catalog", live: true });
    expect(receivedStates["catalog"]!.live).toBe(false);
  });

  it("passes live through unchanged when it is actually available", () => {
    renderView({ signal: "traces", live: true });
    expect(receivedStates["traces"]!.live).toBe(true);
  });
});
