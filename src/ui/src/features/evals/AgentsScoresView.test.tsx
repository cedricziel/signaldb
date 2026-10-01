import { screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentsScoresView } from "./AgentsScoresView";
import { F } from "../../api/evals";
import * as queryIrApi from "../../api/queryIr";
import type { ExploreState } from "../../lib/urlState";
import { aggBy, mockIr, renderEvalView, table, type IrDoc } from "./testUtils";

vi.mock("../../api/queryIr", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/queryIr")>();
  return { ...actual, runIrQuery: vi.fn() };
});

afterEach(() => {
  vi.mocked(queryIrApi.runIrQuery).mockReset();
});

function wherePreds(doc: IrDoc): unknown[] {
  return doc.pipeline?.flatMap((s) => (s.where ? [s.where] : [])) ?? [];
}

const series = {
  result: "series",
  window: { start_ns: 0, end_ns: 0 },
  series: [],
};

/**
 * Dispatches every AgentsScoresView query. `statsRows` answers the one
 * query that varies per test — `fetchStats(range, scope, [F.name])`, called
 * once for the current window and once for the previous one — everything
 * else gets a neutral/empty answer so the page renders past "loading".
 */
function mockView(statsRows: (doc: IrDoc) => unknown[][]) {
  mockIr((doc) => {
    const by = aggBy(doc);
    if (doc.from === "traces") return table(by.length === 0 ? [[10]] : []);
    if (by.includes(F.agent)) return table([["support-triage", null, 10]]);
    if (doc.result === "series") return series;
    if (by.includes(F.agentVersion)) return table([]);
    if (by.length === 1 && by[0] === "trace_id") return table([]);
    if (by.includes(F.evaluator)) return table([]);
    if (by.length && by[0] === F.name) return table(statsRows(doc));
    return table([]);
  });
}

const renderView = (state: Partial<ExploreState> = {}) =>
  renderEvalView(AgentsScoresView, state);

describe("AgentsScoresView", () => {
  it("defaults to the offline source: the stats query's where stages require signaldb.eval.run_id", async () => {
    let sawOfflinePredicate = false;
    mockView((doc) => {
      const preds = wherePreds(doc);
      if (
        preds.some(
          (p) =>
            typeof p === "object" &&
            p !== null &&
            (p as { field?: string; op?: string }).field === F.runId &&
            (p as { field?: string; op?: string }).op === "exists",
        )
      ) {
        sawOfflinePredicate = true;
      }
      return [];
    });
    renderView();
    expect(await screen.findByText("No evaluations yet")).toBeInTheDocument();
    expect(sawOfflinePredicate).toBe(true);
  });

  it("shows the No evaluations empty state when nothing scored", async () => {
    mockView(() => []);
    renderView();
    expect(await screen.findByText("No evaluations yet")).toBeInTheDocument();
  });

  it("sorts evaluators by biggest drop first and flags the regressed one", async () => {
    mockView((doc) => {
      const from = Number(doc.range?.from ?? 0);
      // The current (later) window has the larger `from`.
      const isPrevious = from < 8_000_000_000_000;
      return isPrevious
        ? [
            ["Correctness", "pass", null, 100, 0, 0, 90, 100],
            ["Groundedness", "pass", null, 100, 0, 0, 80, 100],
          ]
        : [
            ["Correctness", "pass", null, 100, 0, 0, 60, 100], // dropped 0.30
            ["Groundedness", "pass", null, 100, 0, 0, 82, 100], // + 0.02, under epsilon
          ];
    });
    renderView({
      range: { type: "absolute", fromMs: 10_000_000, toMs: 15_000_000 },
    });

    await screen.findAllByText("Correctness");
    const rows = within(screen.getByRole("table")).getAllByRole("row").slice(1);
    expect(
      rows.map(
        (r) => within(r).getByText(/Correctness|Groundedness/).textContent,
      ),
    ).toEqual(["Correctness", "Groundedness"]);
    expect(within(rows[0]!).getByLabelText("regressed")).toBeInTheDocument();
    expect(
      within(rows[1]!).queryByLabelText("regressed"),
    ).not.toBeInTheDocument();
  });

  it("shows an error banner and an errored tag for an evaluator over the error-share threshold", async () => {
    mockView(() => [
      // 10 of 100 results errored: 10% > the 5% banner threshold.
      ["Correctness", null, "timeout", 10, 0, 0, 0, 0],
      ["Correctness", "pass", null, 90, 0, 0, 85, 90],
    ]);
    renderView();
    expect(
      await screen.findByText(/The Correctness evaluator errored on/),
    ).toBeInTheDocument();
    expect(screen.getAllByText(/errored/).length).toBeGreaterThan(0);
  });
});
