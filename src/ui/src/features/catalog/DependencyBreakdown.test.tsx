import { fireEvent, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as dependencyBreakdownApi from "../../api/dependencyBreakdown";
import { renderWithClient } from "../../test/render";
import { DependencyBreakdown } from "./DependencyBreakdown";

vi.mock("../../api/dependencyBreakdown", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../api/dependencyBreakdown")>();
  return { ...actual, fetchDependencyBreakdown: vi.fn() };
});

const fetchDependencyBreakdown = vi.mocked(
  dependencyBreakdownApi.fetchDependencyBreakdown,
);

afterEach(() => {
  fetchDependencyBreakdown.mockReset();
});

function renderBreakdown() {
  return renderWithClient(
    <DependencyBreakdown
      serviceName="gateway"
      range={{ fromMs: 0, toMs: 3_600_000 }}
      rangeKey="r"
    />,
  );
}

describe("DependencyBreakdown bar tooltip", () => {
  it("shows the category, time, share, and call count on hover", async () => {
    fetchDependencyBreakdown.mockResolvedValue([
      { key: "database", label: "Database", durationNs: 300_000_000, count: 3 },
      { key: "http", label: "HTTP", durationNs: 100_000_000, count: 1 },
    ]);
    renderBreakdown();
    const segs = await screen.findAllByTestId("dep-seg");
    expect(segs).toHaveLength(2);
    fireEvent.pointerMove(segs[0]!, { clientX: 100, clientY: 10 });
    const tip = screen.getByRole("tooltip");
    expect(within(tip).getByText("Database")).toBeInTheDocument();
    const rows = within(tip).getAllByTestId("viz-tip-row");
    expect(rows.map((r) => r.textContent)).toEqual([
      "time300 ms",
      "share75.0%",
      "calls3",
    ]);
    expect(
      within(rows[0]!).getByTestId("viz-tip-swatch").style.background,
    ).toBe("var(--svc-a)");
    expect(segs[0]).toHaveAttribute("aria-describedby", tip.id);
  });

  it("hides the tooltip when the pointer leaves", async () => {
    fetchDependencyBreakdown.mockResolvedValue([
      { key: "database", label: "Database", durationNs: 300, count: 3 },
    ]);
    renderBreakdown();
    const seg = (await screen.findAllByTestId("dep-seg"))[0]!;
    fireEvent.pointerMove(seg, { clientX: 100, clientY: 10 });
    expect(screen.getByRole("tooltip")).toBeInTheDocument();
    fireEvent.pointerLeave(seg);
    expect(screen.queryByRole("tooltip")).toBeNull();
  });

  it("reaches the same detail from the keyboard", async () => {
    fetchDependencyBreakdown.mockResolvedValue([
      { key: "database", label: "Database", durationNs: 300, count: 3 },
      { key: "rpc", label: "RPC", durationNs: 100, count: 1 },
    ]);
    renderBreakdown();
    const segs = await screen.findAllByTestId("dep-seg");
    const seg = segs[1]!;
    // Only the first segment is a native tab stop; ArrowRight moves the
    // roving one over to it (see the roving-focus suite below).
    segs[0]!.focus();
    fireEvent.keyDown(segs[0]!, { key: "ArrowRight" });
    expect(seg).toHaveFocus();
    fireEvent.focus(seg);
    expect(screen.getByRole("tooltip")).toHaveTextContent("RPC");
    expect(screen.getByRole("tooltip")).toHaveTextContent("25.0%");
    fireEvent.blur(seg);
    expect(screen.queryByRole("tooltip")).toBeNull();
  });
});

describe("DependencyBreakdown legend", () => {
  it("gives each legend label a small swatch dot rather than a full-block background", async () => {
    fetchDependencyBreakdown.mockResolvedValue([
      { key: "database", label: "Database", durationNs: 300, count: 3 },
    ]);
    renderBreakdown();
    const label = await screen.findByText("Database");
    // The label's own element must not carry the bare `dep-database` class
    // that colors a full block (that class is for the bar segment) — only
    // the shared `dep-swatch` marker, colored via `--kind-color` like
    // `DependencyTable`'s Kind column.
    expect(label.className).toBe("dep-swatch");
    expect(label.style.getPropertyValue("--kind-color")).toBe("var(--svc-a)");
  });
});

describe("DependencyBreakdown roving focus", () => {
  it("gives the first segment the only tab stop and moves it with ArrowRight", async () => {
    fetchDependencyBreakdown.mockResolvedValue([
      { key: "database", label: "Database", durationNs: 300, count: 3 },
      { key: "rpc", label: "RPC", durationNs: 100, count: 1 },
    ]);
    renderBreakdown();
    const segs = await screen.findAllByTestId("dep-seg");
    expect(segs[0]).toHaveAttribute("tabindex", "0");
    expect(segs[1]).toHaveAttribute("tabindex", "-1");
    segs[0]!.focus();
    fireEvent.keyDown(segs[0]!, { key: "ArrowRight" });
    expect(segs[1]).toHaveFocus();
    expect(segs[1]).toHaveAttribute("tabindex", "0");
  });

  // Tab should land wherever the pointer last showed detail for, matching
  // `docs/users/explore-ui.md`'s "the last one you pointed at" — not just
  // wherever an arrow key or native focus left it.
  it("moves the tab stop to the segment the pointer moves over", async () => {
    fetchDependencyBreakdown.mockResolvedValue([
      { key: "database", label: "Database", durationNs: 300, count: 3 },
      { key: "rpc", label: "RPC", durationNs: 100, count: 1 },
    ]);
    renderBreakdown();
    const segs = await screen.findAllByTestId("dep-seg");
    fireEvent.pointerMove(segs[1]!, { clientX: 100, clientY: 10 });
    expect(segs[1]).toHaveAttribute("tabindex", "0");
    expect(segs[0]).toHaveAttribute("tabindex", "-1");
  });
});
