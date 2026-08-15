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
    const seg = (await screen.findAllByTestId("dep-seg"))[1]!;
    expect(seg).toHaveAttribute("tabindex", "0");
    fireEvent.focus(seg);
    expect(screen.getByRole("tooltip")).toHaveTextContent("RPC");
    expect(screen.getByRole("tooltip")).toHaveTextContent("25.0%");
    fireEvent.blur(seg);
    expect(screen.queryByRole("tooltip")).toBeNull();
  });
});
