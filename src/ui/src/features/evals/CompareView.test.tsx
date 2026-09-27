import { screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CompareView } from "./CompareView";
import { F } from "../../api/evals";
import * as queryIrApi from "../../api/queryIr";
import { DEFAULT_STATE } from "../../lib/urlState";
import {
  aggBy,
  mockIr,
  renderEvalView,
  rows,
  runIdsOf,
  runRows,
  table,
  wherePred,
} from "./testUtils";

vi.mock("../../api/queryIr", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/queryIr")>();
  return { ...actual, runIrQuery: vi.fn() };
});

afterEach(() => {
  vi.mocked(queryIrApi.runIrQuery).mockReset();
});

const BASELINE = "run-0923-0915";
const CANDIDATE = "run-0927-1004";

/** Stats row tail per [caseId, evaluator]: label, error, n, high, low,
 * score_sum, scored. */
const CASE_STATS: Record<string, Record<string, [string | null, number]>> = {
  [`${BASELINE}\0case-117`]: {
    ToolTrajectory: ["pass", 1],
    Correctness: ["pass", 1],
  },
  [`${CANDIDATE}\0case-117`]: {
    ToolTrajectory: ["fail", 0],
    Correctness: ["fail", 0],
  },
  [`${BASELINE}\0case-064`]: { Correctness: ["fail", 0] },
  [`${CANDIDATE}\0case-064`]: { Correctness: ["pass", 1] },
  [`${BASELINE}\0case-088`]: { Correctness: ["pass", 1] },
  [`${CANDIDATE}\0case-088`]: { Correctness: ["pass", 1] },
  [`${CANDIDATE}\0case-500`]: { Correctness: ["fail", 0] },
};

const CASE_TRACES: Record<string, string> = {
  [`${BASELINE}\0case-117`]: "trace-base-117",
  [`${CANDIDATE}\0case-117`]: "trace-cand-117",
  [`${BASELINE}\0case-064`]: "trace-base-064",
  [`${CANDIDATE}\0case-064`]: "trace-cand-064",
  [`${BASELINE}\0case-088`]: "trace-base-088",
  [`${CANDIDATE}\0case-088`]: "trace-cand-088",
  [`${CANDIDATE}\0case-500`]: "trace-cand-500",
};

/** `tools` per trace id, used to build the execute_tool spans. */
const TRACE_TOOLS: Record<string, string[]> = {
  "trace-base-117": ["lookup_order", "check_policy", "issue_refund"],
  "trace-cand-117": ["lookup_order", "issue_refund"], // check_policy skipped
  "trace-base-064": ["lookup_order"],
  "trace-cand-064": ["lookup_order", "search_kb"], // new call
  "trace-base-088": ["lookup_order"],
  "trace-cand-088": ["lookup_order"],
  "trace-cand-500": ["lookup_order"],
};

/** `fetchAgentSpans`' slim rows: trace_id, span_id, start, duration,
 * span name, then the GenAI attributes (operation, tool, …). */
function agentSpanRows(traceId: string) {
  const tools = TRACE_TOOLS[traceId] ?? [];
  const span = (
    id: string,
    startNs: number,
    durNs: number,
    name: string,
    op: string,
    tool: string | null,
  ) => [traceId, id, startNs, durNs, name, op, tool, null, null, null, null];
  return [
    span(
      "agent",
      1_000_000_000,
      1_000_000_000,
      "invoke_agent support-triage",
      "invoke_agent",
      null,
    ),
    ...tools.map((tool, i) =>
      span(
        `tool-${i}`,
        1_000_000_000 + (i + 1) * 10_000_000,
        5_000_000,
        `execute_tool ${tool}`,
        "execute_tool",
        tool,
      ),
    ),
  ];
}

function mockCompare() {
  mockIr((doc) => {
    const by = aggBy(doc);
    if (doc.from === "traces") {
      // fetchAgentSpans: one call for every trace id in the `in` predicate.
      const ids = (wherePred(doc, "trace_id")?.value as string[]) ?? [];
      return rows(ids.flatMap(agentSpanRows));
    }
    if (by.includes(F.set)) {
      return table([
        ...runRows({
          id: CANDIDATE,
          set: "triage-golden-200",
          version: "v1.8.0",
          firstMs: 2_000_000,
          lastMs: 2_001_000,
          n: 200,
        }),
        ...runRows({
          id: BASELINE,
          set: "triage-golden-200",
          version: "v1.7.3",
          firstMs: 1_000_000,
          lastMs: 1_001_000,
          n: 200,
        }),
      ]);
    }
    if (by.includes(F.caseId) && by.length === 2 && by[0] === F.runId) {
      // per-run case counts
      return table([
        [BASELINE, "case-117", 1],
        [BASELINE, "case-064", 1],
        [BASELINE, "case-088", 1],
        [CANDIDATE, "case-117", 1],
        [CANDIDATE, "case-064", 1],
        [CANDIDATE, "case-088", 1],
        [CANDIDATE, "case-500", 1],
      ]);
    }
    const runId = runIdsOf(doc)[0] ?? "";
    if (by.includes(F.caseId) && by.includes(F.name)) {
      // fetchRunCases' fetchStats([F.caseId, F.name]), scoped to one run.
      return table(
        Object.entries(CASE_STATS)
          .filter(([k]) => k.startsWith(`${runId}\0`))
          .flatMap(([k, evaluators]) => {
            const caseId = k.split("\0")[1]!;
            return Object.entries(evaluators).map(([name, [label, pass]]) => [
              caseId,
              name,
              label,
              null,
              1,
              pass,
              1 - pass,
              pass,
              1,
            ]);
          }),
      );
    }
    if (doc.irVersion === 5) {
      // the case→trace lookup, scoped to one run.
      return table(
        Object.entries(CASE_TRACES)
          .filter(([k]) => k.startsWith(`${runId}\0`))
          .map(([k, trace]) => [k.split("\0")[1]!, trace]),
      );
    }
    return table([]);
  });
}

const renderView = () =>
  renderEvalView(CompareView, {
    evals: {
      ...DEFAULT_STATE.evals,
      baseline: BASELINE,
      candidate: CANDIDATE,
    },
  });

describe("CompareView", () => {
  it("counts and orders cases: regressions first, then improvements, then unchanged", async () => {
    mockCompare();
    renderView();

    await screen.findAllByText(/^case-/);
    expect(
      await screen.findByRole("button", { name: "Regressions 2" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Improvements 1" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Unchanged 1" }),
    ).toBeInTheDocument();

    // Regressions is the default filter, and case-117/case-500 (no
    // baseline, failing) are both regressions, largest drop first.
    const rows = screen
      .getAllByRole("row")
      .filter((r) => within(r).queryAllByText(/^case-/).length > 0);
    expect(
      rows.map(
        (r) => within(r).getAllByRole("cell")[0]!.firstChild?.textContent,
      ),
    ).toEqual(["case-117", "case-500"]);
  });

  it("renders a skipped tool struck through and marks the case with no baseline", async () => {
    mockCompare();
    renderView();

    const row117 = await screen.findByRole("row", { name: /case-117/ });
    const skipped = await within(row117).findByText("check_policy");
    expect(skipped).toHaveClass("evals-tool", "skipped");

    const row500 = screen.getByRole("row", { name: /case-500/ });
    expect(within(row500).getByText("no baseline")).toBeInTheDocument();
  });

  it("shows improvements when that filter is picked", async () => {
    const userEvent = (await import("@testing-library/user-event")).default;
    mockCompare();
    renderView();
    await screen.findByRole("button", { name: "Improvements 1" });
    await userEvent
      .setup()
      .click(screen.getByRole("button", { name: /Improvements/ }));
    expect(screen.getByRole("row", { name: /case-064/ })).toBeInTheDocument();
  });
});
