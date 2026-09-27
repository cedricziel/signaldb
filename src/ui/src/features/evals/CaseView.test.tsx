import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CaseView } from "./CaseView";
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
const CASE_ID = "case-117";

const SPAN_COLUMNS = [
  { name: "trace_id", type: "string" },
  { name: "span_id", type: "string" },
  { name: "parent_span_id", type: "string" },
  { name: "span_name", type: "string" },
  { name: "service_name", type: "string" },
  { name: "status_code", type: "string" },
  { name: "status_message", type: "string" },
  { name: "start_time_unix_nano", type: "string" },
  { name: "duration_nanos", type: "string" },
  { name: "span_kind", type: "string" },
  { name: "span_attributes", type: "string" },
  { name: "scope_attributes", type: "string" },
  { name: "resource_attributes", type: "string" },
  { name: "span_events", type: "string" },
];

/** `traceId -> execute_tool names in call order` for the candidate trace,
 * whose baseline made an extra `check_policy` call the candidate skipped. */
function spanRows(traceId: string, tools: string[]) {
  const rows: unknown[][] = [
    [
      traceId,
      "agent",
      null,
      "invoke_agent support-triage",
      "support-triage",
      "OK",
      null,
      1_000_000_000,
      5_000_000_000,
      "Server",
      { "gen_ai.operation.name": "invoke_agent" },
      {},
      {},
      null,
    ],
  ];
  tools.forEach((tool, i) => {
    rows.push([
      traceId,
      `tool-${i}`,
      "agent",
      `execute_tool ${tool}`,
      "support-triage",
      "OK",
      null,
      1_000_000_000 + (i + 1) * 10_000_000,
      5_000_000,
      "Internal",
      { "gen_ai.operation.name": "execute_tool", "gen_ai.tool.name": tool },
      {},
      {},
      null,
    ]);
  });
  return rows;
}

function mockCaseView() {
  mockIr((doc) => {
    const by = aggBy(doc);

    if (doc.from === "traces") {
      // fetchTraces: both case traces in one `in` request.
      const ids = (wherePred(doc, "trace_id")?.value as string[]) ?? [];
      return {
        result: "rows",
        window: { start_ns: 0, end_ns: 0 },
        columns: SPAN_COLUMNS,
        rows: ids.flatMap((traceId) =>
          spanRows(
            traceId,
            traceId === "trace-base-117"
              ? ["lookup_order", "check_policy", "issue_refund"]
              : ["lookup_order", "issue_refund"],
          ),
        ),
      };
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
      return table([
        [BASELINE, CASE_ID, 1],
        [CANDIDATE, CASE_ID, 1],
      ]);
    }
    if (by.includes(F.caseId) && by.includes(F.name)) {
      return table([]); // fetchRunCases' fetchStats — unused by CaseView
    }
    if (doc.fields?.includes(F.runId)) {
      // fetchCaseResults: RESULT_FIELDS rows
      return rows([
        [
          BASELINE,
          "trace-base-117",
          "tool-1", // check_policy in the baseline
          "ToolTrajectory",
          1,
          "pass",
          "Followed policy.",
          "trajectory-match@2.1.0",
          null,
        ],
        [
          CANDIDATE,
          "trace-cand-117",
          "tool-1", // issue_refund in the candidate (index 1 of 2 tools)
          "ToolTrajectory",
          0,
          "fail",
          "Skipped the policy check.",
          "trajectory-match@2.1.0",
          null,
        ],
      ]);
    }
    // The case→trace lookup (irVersion 5).
    const runId = runIdsOf(doc)[0];
    const trace = runId === BASELINE ? "trace-base-117" : "trace-cand-117";
    return table([[CASE_ID, trace]]);
  });
}

const renderView = () =>
  renderEvalView(CaseView, {
    evals: {
      ...DEFAULT_STATE.evals,
      baseline: BASELINE,
      candidate: CANDIDATE,
      case: CASE_ID,
    },
  });

describe("CaseView", () => {
  it("renders the result badge on the span it scored, not on the invoke_agent row", async () => {
    mockCaseView();
    renderView();

    const toolRow = await screen.findByText("execute_tool issue_refund");
    const agentRow = screen.getByText("invoke_agent support-triage");
    const toolTrajRow = toolRow.closest(".evals-traj-row") as HTMLElement;
    const agentTrajRow = agentRow.closest(".evals-traj-row") as HTMLElement;

    expect(within(toolTrajRow).getByText(/ToolTrajectory/)).toBeInTheDocument();
    expect(
      within(agentTrajRow).queryByText(/ToolTrajectory/),
    ).not.toBeInTheDocument();
  });

  it("shows an expected-not-called ghost row for a tool the baseline called", async () => {
    mockCaseView();
    renderView();

    const ghost = await screen.findByText("execute_tool check_policy");
    const ghostRow = ghost.closest(".evals-traj-row") as HTMLElement;
    expect(ghostRow).toHaveClass("ghost");
    expect(
      within(ghostRow).getByText("expected, not called"),
    ).toBeInTheDocument();
    expect(
      within(ghostRow).getByText(/missing vs baseline/),
    ).toBeInTheDocument();
  });

  it("switches to side-by-side mode via update", async () => {
    mockCaseView();
    const update = renderView();
    await screen.findByText("invoke_agent support-triage");

    await userEvent.click(screen.getByRole("radio", { name: "Side by side" }));
    expect(update).toHaveBeenCalledWith({
      evals: expect.objectContaining({ mode: "side" }),
    });
  });
});
