import { fireEvent, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as dependencyTargetsApi from "../../api/dependencyTargets";
import { renderWithClient } from "../../test/render";
import { DependencyTable } from "./DependencyTable";

vi.mock("../../api/dependencyTargets", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../api/dependencyTargets")>();
  return { ...actual, fetchDependencyTargets: vi.fn() };
});

const fetchDependencyTargets = vi.mocked(
  dependencyTargetsApi.fetchDependencyTargets,
);

afterEach(() => {
  fetchDependencyTargets.mockReset();
});

function renderTable() {
  return renderWithClient(
    <DependencyTable
      serviceName="checkout"
      range={{ fromMs: 0, toMs: 3_600_000 }}
      rangeKey="r"
    />,
  );
}

describe("DependencyTable", () => {
  it("renders a sorted row per target plus a self row", async () => {
    fetchDependencyTargets.mockResolvedValue({
      requestDurationNs: 1_000_000_000, // 1s of request time
      requestCount: 100,
      selfDurationNs: 400_000_000, // 0.4s in-process
      rows: [
        {
          key: "database:orders-db",
          kind: "database",
          target: "orders-db",
          operation: "postgresql · SELECT",
          durationNs: 300_000_000, // 0.3s -> 30%
          count: 60,
          p95Ns: 12_000_000,
        },
        {
          key: "http:payments",
          kind: "http",
          target: "payments",
          operation: "GET",
          durationNs: 300_000_000, // 0.3s -> 30%
          count: 30,
          p95Ns: 40_000_000,
        },
      ],
    });
    renderTable();

    const rows = await screen.findAllByRole("row");
    // Header + self (40%) + two ties at 30% (insertion order preserved).
    expect(rows).toHaveLength(4);
    expect(within(rows[1]!).getByText("checkout (self)")).toBeInTheDocument();
    expect(within(rows[1]!).getAllByText("–")).toHaveLength(2); // p95, calls/req
    expect(within(rows[2]!).getByText("orders-db")).toBeInTheDocument();
    expect(
      within(rows[2]!).getByText("postgresql · SELECT"),
    ).toBeInTheDocument();
    expect(within(rows[2]!).getByText("0.60")).toBeInTheDocument(); // calls/req
    expect(within(rows[3]!).getByText("payments")).toBeInTheDocument();
  });

  it("shows a tooltip with time/share/calls on hover", async () => {
    fetchDependencyTargets.mockResolvedValue({
      requestDurationNs: 1_000_000_000,
      requestCount: 10,
      selfDurationNs: 0,
      rows: [
        {
          key: "database:orders-db",
          kind: "database",
          target: "orders-db",
          operation: "postgresql · SELECT",
          durationNs: 250_000_000,
          count: 5,
          p95Ns: 12_000_000,
        },
      ],
    });
    renderTable();

    const bar = await screen.findByTestId("dep-table-share-bar");
    fireEvent.pointerMove(bar, { clientX: 10, clientY: 10 });
    const tip = screen.getByRole("tooltip");
    expect(within(tip).getByText("orders-db")).toBeInTheDocument();
    const tipRows = within(tip).getAllByTestId("viz-tip-row");
    expect(tipRows.map((r) => r.textContent)).toEqual([
      "time250 ms",
      "share25.0%",
      "calls5",
    ]);
    fireEvent.pointerLeave(bar);
    expect(screen.queryByRole("tooltip")).toBeNull();
  });

  it("renders nothing when there are no targets and no self time", async () => {
    fetchDependencyTargets.mockResolvedValue({
      requestDurationNs: 0,
      requestCount: 0,
      selfDurationNs: 0,
      rows: [],
    });
    const { container } = renderTable();
    await vi.waitFor(() => {
      expect(fetchDependencyTargets).toHaveBeenCalled();
    });
    expect(container.querySelector("table")).toBeNull();
  });
});
