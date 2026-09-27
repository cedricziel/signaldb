import { screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { RunsView } from "./RunsView";
import { F } from "../../api/evals";
import * as queryIrApi from "../../api/queryIr";
import { aggBy, mockIr, renderEvalView, runRows, table } from "./testUtils";

vi.mock("../../api/queryIr", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/queryIr")>();
  return { ...actual, runIrQuery: vi.fn() };
});

afterEach(() => {
  vi.mocked(queryIrApi.runIrQuery).mockReset();
});

const NOW = Date.UTC(2026, 8, 27, 12, 0);

interface RunFixture {
  id: string;
  set: string;
  version: string;
  firstMs: number;
  lastMs: number;
  n: number;
  errors: number;
  unlinked: number;
  cases: number;
}

/** Wires fetchAgents, the runs table (identity × label × error, with the
 * stats `passRateOf(run.stats)` reads) and the per-run case counts. */
function mockRuns(runs: RunFixture[]) {
  mockIr((doc) => {
    const by = aggBy(doc);
    if (by.length === 2 && by.includes(F.agent)) return table([]); // fetchAgents
    if (by.includes(F.set)) return table(runs.flatMap(runRows));
    if (by.includes(F.caseId)) {
      return table(
        runs.flatMap((r) =>
          Array.from({ length: r.cases }, (_, i) => [r.id, `case-${i}`, 1]),
        ),
      );
    }
    return table([]);
  });
}

const renderView = () => renderEvalView(RunsView);

describe("RunsView", () => {
  it("shows the No runs empty state when nothing has run", async () => {
    mockRuns([]);
    renderView();
    expect(await screen.findByText("No runs yet")).toBeInTheDocument();
  });

  it("renders complete, partial (with reasons) and receiving-results statuses", async () => {
    mockRuns([
      {
        id: "run-complete",
        set: "triage-golden-200",
        version: "v1.7.3",
        firstMs: NOW - 20 * 60_000,
        lastMs: NOW - 15 * 60_000,
        n: 200,
        errors: 0,
        unlinked: 0,
        cases: 200,
      },
      {
        id: "run-partial",
        set: "triage-golden-200",
        version: "v1.8.0",
        firstMs: NOW - 30 * 60_000,
        lastMs: NOW - 20 * 60_000,
        n: 200,
        errors: 4,
        unlinked: 1,
        cases: 195,
      },
      {
        id: "run-live",
        set: "triage-golden-200",
        version: "v1.8.1",
        firstMs: NOW - 2 * 60_000,
        lastMs: NOW - 1 * 60_000,
        n: 20,
        errors: 0,
        unlinked: 0,
        cases: 20,
      },
    ]);
    vi.setSystemTime(NOW);
    renderView();

    const completeRow = await screen.findByRole("row", {
      name: /run-complete/,
    });
    expect(within(completeRow).getByText("complete")).toBeInTheDocument();

    const partialRow = screen.getByRole("row", { name: /run-partial/ });
    expect(within(partialRow).getByText("partial")).toBeInTheDocument();
    expect(
      within(partialRow).getByText("4 not scored · 1 unmatched"),
    ).toBeInTheDocument();

    const liveRow = screen.getByRole("row", { name: /run-live/ });
    expect(within(liveRow).getByText("receiving results")).toBeInTheDocument();

    vi.useRealTimers();
  });

  it("links Compare to the newest earlier run of the same eval set", async () => {
    mockRuns([
      {
        id: "run-0927-1004",
        set: "triage-golden-200",
        version: "v1.8.0",
        firstMs: NOW - 20 * 60_000,
        lastMs: NOW - 15 * 60_000,
        n: 200,
        errors: 0,
        unlinked: 0,
        cases: 200,
      },
      {
        id: "run-0923-0915",
        set: "triage-golden-200",
        version: "v1.7.3",
        firstMs: NOW - 4 * 86_400_000,
        lastMs: NOW - 4 * 86_400_000 + 60_000,
        n: 200,
        errors: 0,
        unlinked: 0,
        cases: 200,
      },
    ]);
    vi.setSystemTime(NOW);
    renderView();

    const candidateRow = await screen.findByRole("row", {
      name: /run-0927-1004/,
    });
    const link = within(candidateRow).getByRole("link", { name: "Compare ›" });
    expect(link.getAttribute("href")).toMatch(
      /^\/evals\/compare\?baseline=run-0923-0915&candidate=run-0927-1004/,
    );

    const baselineRow = screen.getByRole("row", { name: /run-0923-0915/ });
    expect(
      within(baselineRow).queryByRole("link", { name: "Compare ›" }),
    ).not.toBeInTheDocument();

    vi.useRealTimers();
  });
});
