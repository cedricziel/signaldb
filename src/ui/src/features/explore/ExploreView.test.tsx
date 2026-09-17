import { screen } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import { renderWithClient } from "../../test/render";
import { ExploreView } from "./ExploreView";

// The child views each issue their own queries and are covered by their own
// test files; ExploreView's own behavior (the signal tabs and the Live
// toggle) doesn't depend on what they render.
vi.mock("../catalog/CatalogView", () => ({
  CatalogView: () => <div>catalog view</div>,
}));
vi.mock("../errors/ErrorsView", () => ({
  ErrorsView: () => <div>errors view</div>,
}));
vi.mock("../logs/LogsView", () => ({ LogsView: () => <div>logs view</div> }));
vi.mock("../metrics/MetricsView", () => ({
  MetricsView: () => <div>metrics view</div>,
}));
vi.mock("../profiles/ProfilesView", () => ({
  ProfilesView: () => <div>profiles view</div>,
}));
vi.mock("../traces/TracesView", () => ({
  TracesView: () => <div>traces view</div>,
}));
vi.mock("../query/QueryView", () => ({
  QueryView: () => <div>query view</div>,
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
