import { afterEach, describe, expect, it, vi } from "vitest";
import {
  agentTraceOf,
  buildRunsDoc,
  buildStatsDoc,
  decodeStats,
  EVAL_EVENT,
  F,
  fetchAgentSpans,
  fetchEvaluators,
  fetchRunCases,
  fetchRuns,
  messageText,
  resultsWhere,
} from "./evals";
import { runIrQuery } from "./queryIr";
import type { QueryIrResponse } from "./gen";
import type { TempoSpan, TempoTrace } from "./traceTypes";
import { withColumns } from "../features/evals/evalFixtures";

vi.mock("./queryIr", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./queryIr")>();
  return { ...actual, runIrQuery: vi.fn() };
});

const mockRunIrQuery = vi.mocked(runIrQuery);

/** Answers each request with `handler`'s positional rows plus the columns
 * the request implies. */
function answer(handler: (doc: unknown) => QueryIrResponse) {
  mockRunIrQuery.mockImplementation(
    async (doc) => withColumns(doc, handler(doc)) as QueryIrResponse,
  );
}

afterEach(() => {
  vi.clearAllMocks();
});

const RANGE = { fromMs: 1_000_000, toMs: 2_000_000 };

function table(rows: unknown[][]): QueryIrResponse {
  return {
    result: "table",
    window: { start_ns: 0, end_ns: 0 },
    rows,
  } as QueryIrResponse;
}

// ---- resultsWhere ----------------------------------------------------------

describe("resultsWhere", () => {
  it("always filters to the evaluation-result event", () => {
    expect(resultsWhere()).toEqual([
      { where: { field: "event_name", op: "eq", value: EVAL_EVENT } },
    ]);
  });

  it("scopes to offline runs via signaldb.eval.run_id existing", () => {
    const stages = resultsWhere({ source: "offline" });
    expect(stages).toEqual([
      { where: { field: "event_name", op: "eq", value: EVAL_EVENT } },
      { where: { field: F.runId, op: "exists" } },
    ]);
  });

  it("scopes to production via the run id being absent", () => {
    const stages = resultsWhere({ source: "production" });
    expect(stages).toContainEqual({
      where: { not: { field: F.runId, op: "exists" } },
    });
  });

  it("leaves both sources unfiltered", () => {
    const stages = resultsWhere({ source: "both" });
    expect(stages).toEqual([
      { where: { field: "event_name", op: "eq", value: EVAL_EVENT } },
    ]);
  });

  it("filters by run ids, case id and agent (with the service.name fallback)", () => {
    const stages = resultsWhere({
      agent: "support-triage",
      runIds: ["run-1", "run-2"],
      caseId: "case-117",
    });
    expect(stages).toEqual([
      { where: { field: "event_name", op: "eq", value: EVAL_EVENT } },
      {
        where: {
          or: [
            { field: F.agent, op: "eq", value: "support-triage" },
            {
              and: [
                { not: { field: F.agent, op: "exists" } },
                { field: "service.name", op: "eq", value: "support-triage" },
              ],
            },
          ],
        },
      },
      { where: { field: F.runId, op: "in", value: ["run-1", "run-2"] } },
      { where: { field: F.caseId, op: "eq", value: "case-117" } },
    ]);
  });
});

// ---- buildStatsDoc / decodeStats -------------------------------------------

describe("buildStatsDoc", () => {
  it("groups by the given keys plus label and error, with the pass-rule aggs", () => {
    const doc = buildStatsDoc(RANGE, {}, [F.runId]);
    expect(doc.from).toBe("logs");
    expect(doc.result).toBe("table");
    expect(doc.pipeline).toEqual([
      { where: { field: "event_name", op: "eq", value: EVAL_EVENT } },
      {
        aggregate: {
          by: [F.runId, F.label, F.error],
          aggs: [
            { fn: "count", as: "n" },
            {
              fn: "count",
              as: "high",
              where: { field: F.score, op: "gte", value: 0.5 },
            },
            {
              fn: "count",
              as: "low",
              where: { field: F.score, op: "lt", value: 0.5 },
            },
            { fn: "sum", of: F.score, as: "score_sum" },
            {
              fn: "count",
              as: "scored",
              where: { field: F.score, op: "exists" },
            },
          ],
        },
      },
      { limit: 50_000 },
    ]);
  });
});

/** A stats response for `by`, columns as the server names them. */
function statsTable(by: string[], rows: unknown[][]): QueryIrResponse {
  return withColumns(
    buildStatsDoc(RANGE, {}, by),
    table(rows),
  ) as QueryIrResponse;
}

describe("decodeStats", () => {
  it("folds rows sharing a by-key across label/error groups into one EvalStats", () => {
    // Two by-keys (run_id): each has a passing label group and an error group.
    const res = statsTable(
      [F.runId],
      [
        ["run-a", "pass", null, 8, 0, 0, 7.2, 8],
        ["run-a", null, "timeout", 2, 0, 0, 0, 0],
        ["run-b", "fail", null, 3, 0, 3, 0.9, 3],
      ],
    );
    const stats = decodeStats(res, [F.runId]);
    expect(stats.get("run-a")).toEqual({
      results: 10,
      errors: 2,
      pass: 8,
      fail: 0,
      scoreSum: 7.2,
      scored: 8,
    });
    expect(stats.get("run-b")).toEqual({
      results: 3,
      errors: 0,
      pass: 0,
      fail: 3,
      scoreSum: 0.9,
      scored: 3,
    });
  });

  it("joins a multi-key by with NUL", () => {
    const by = [F.caseId, F.name];
    const res = statsTable(by, [
      ["case-1", "Correctness", null, null, 1, 0, 0, 0.9, 1],
    ]);
    const stats = decodeStats(res, by);
    expect([...stats.keys()]).toEqual(["case-1\0Correctness"]);
  });

  it("returns an empty map for no rows", () => {
    expect(decodeStats(statsTable([F.runId], []), [F.runId]).size).toBe(0);
  });
});

// ---- fetchRuns --------------------------------------------------------------

describe("buildRunsDoc", () => {
  it("scopes to offline results and groups by run identity, label and error", () => {
    const doc = buildRunsDoc(RANGE, {});
    expect(doc.pipeline?.[0]).toEqual({
      where: { field: "event_name", op: "eq", value: EVAL_EVENT },
    });
    expect(doc.pipeline?.[1]).toEqual({
      where: { field: F.runId, op: "exists" },
    });
    const agg = (
      doc.pipeline?.[2] as {
        aggregate: { by: string[]; aggs: { as: string; where?: unknown }[] };
      }
    ).aggregate;
    expect(agg.by).toEqual([
      F.runId,
      F.set,
      F.agent,
      "service.name",
      F.agentVersion,
      "service.version",
      F.label,
      F.error,
    ]);
    expect(agg.aggs.map((a) => a.as)).toEqual([
      "n",
      "high",
      "low",
      "score_sum",
      "scored",
      "first",
      "last",
      "unlinked",
    ]);
    expect(agg.aggs.at(-1)?.where).toEqual({
      not: {
        and: [
          { field: "trace_id", op: "exists" },
          { field: "trace_id", op: "ne", value: "" },
        ],
      },
    });
  });
});

describe("fetchRuns", () => {
  it("merges the run's rows, counts cases and carries stats, errors, unlinked", async () => {
    const first = Date.UTC(2026, 8, 23, 9, 15);
    const last = Date.UTC(2026, 8, 23, 10, 2);
    answer((doc) => {
      const d = doc as { pipeline?: { aggregate?: { by?: string[] } }[] };
      const by = d.pipeline?.find((s) => s.aggregate)?.aggregate?.by ?? [];
      if (by.includes(F.caseId)) {
        // the per-run case-count query
        return table([
          ["run-0923-0915", "case-001", 1],
          ["run-0923-0915", "case-002", 1],
        ]);
      }
      // buildRunsDoc: one row per label/error group of the run
      const run = [
        "run-0923-0915",
        "triage-golden-200",
        "support-triage",
        null,
        "v1.7.3",
        null,
      ];
      return table([
        [...run, "pass", null, 195, 0, 0, 190, 195, first * 1e6, last * 1e6, 1],
        [...run, null, "timeout", 4, 0, 0, 0, 0, first * 1e6, last * 1e6, 0],
        [...run, "fail", null, 1, 0, 0, 0, 0, first * 1e6, first * 1e6, 0],
      ]);
    });

    const runs = await fetchRuns(RANGE, {});
    expect(runs).toHaveLength(1);
    const run = runs[0]!;
    expect(run).toMatchObject({
      id: "run-0923-0915",
      set: "triage-golden-200",
      agent: "support-triage",
      version: "v1.7.3",
      results: 200,
      errors: 4,
      unlinked: 1,
      cases: 2,
    });
    expect(run.stats.pass).toBe(195);
    expect(run.stats.fail).toBe(1);
    expect(run.firstMs).toBe(first);
    expect(run.lastMs).toBe(last);
  });
});

// ---- fetchRunCases -----------------------------------------------------------

describe("fetchRunCases", () => {
  it("builds a case→evaluator→stats map and a case→trace map", async () => {
    answer((doc) => {
      const d = doc as { irVersion?: number };
      if (d.irVersion === 5) {
        // the trace-per-case lookup
        return table([["case-117", "trace-abc"]]);
      }
      // fetchStats([F.caseId, F.name])
      return table([
        ["case-117", "ToolTrajectory", null, null, 1, 1, 0, 0.9, 1],
        ["case-117", "Correctness", "pass", null, 1, 0, 0, 0.95, 1],
      ]);
    });

    const { cases, traces } = await fetchRunCases(RANGE, "run-0927-1004");
    expect(traces.get("case-117")).toBe("trace-abc");
    const scores = cases.get("case-117")!;
    expect(scores.get("ToolTrajectory")?.pass).toBe(1);
    expect(scores.get("Correctness")?.pass).toBe(1);
  });
});

// ---- fetchEvaluators ---------------------------------------------------------

describe("fetchEvaluators", () => {
  it("merges versions across rows and joins operation from the span query", async () => {
    answer((doc) => {
      const d = doc as { from?: string };
      if (d.from === "logs") {
        return table([
          [
            "ToolTrajectory",
            "trajectory-match@2.1.0",
            120,
            1_000_000_000,
            2_000_000_000,
            120,
            0,
            120,
            "span-1",
          ],
          [
            "ToolTrajectory",
            "trajectory-match@2.0.0",
            30,
            500_000_000,
            900_000_000,
            30,
            0,
            30,
            "span-2",
          ],
        ]);
      }
      // fetchSpanOperations over "traces"
      return table([["span-1", "execute_tool"]]);
    });

    const evaluators = await fetchEvaluators(RANGE);
    expect(evaluators).toHaveLength(1);
    const e = evaluators[0]!;
    expect(e.name).toBe("ToolTrajectory");
    expect(e.versions).toEqual([
      "trajectory-match@2.1.0",
      "trajectory-match@2.0.0",
    ]);
    expect(e.results).toBe(150);
    expect(e.operation).toBe("execute_tool");
  });
});

// ---- agentTraceOf / messageText ----------------------------------------------

function span(
  p: Partial<TempoSpan> & { attributes?: TempoSpan["attributes"] },
): TempoSpan {
  return {
    spanId: p.spanId ?? "s",
    parentSpanId: p.parentSpanId ?? null,
    name: p.name ?? "span",
    serviceName: p.serviceName ?? "support-triage",
    status: p.status ?? "ok",
    startNs: p.startNs ?? "0",
    durNs: p.durNs ?? "0",
    attributes: p.attributes ?? {},
    events: p.events ?? [],
  };
}

function trace(spans: TempoSpan[]): TempoTrace {
  return {
    traceId: "trace-1",
    rootServiceName: "support-triage",
    rootTraceName: "invoke_agent",
    startNs: "0",
    durationMs: 1000,
    rootAttributes: {},
    rootError: false,
    profiles: [],
    spans,
  };
}

describe("messageText", () => {
  it("returns the last text part of a GenAI messages JSON array, filtered by role", () => {
    const raw = JSON.stringify([
      { role: "user", parts: [{ type: "text", content: "hello" }] },
      { role: "assistant", parts: [{ type: "text", content: "hi there" }] },
      { role: "user", parts: [{ type: "text", content: "follow up" }] },
    ]);
    expect(messageText(raw, "user")).toBe("follow up");
    expect(messageText(raw)).toBe("follow up");
  });

  it("returns null for empty/non-string input", () => {
    expect(messageText(undefined)).toBeNull();
    expect(messageText("")).toBeNull();
  });

  it("falls back to the raw string when it isn't valid JSON", () => {
    expect(messageText("plain text")).toBe("plain text");
  });

  it("returns null for a valid JSON array with no text parts", () => {
    expect(messageText("[1,2,3]")).toBeNull();
  });
});

describe("agentTraceOf", () => {
  it("orders tools by start time, sums tokens from chat spans, and reads input/output text", () => {
    const input = JSON.stringify([
      {
        role: "user",
        parts: [{ type: "text", content: "Refund order #88213" }],
      },
    ]);
    const output = JSON.stringify([
      {
        role: "assistant",
        parts: [{ type: "text", content: "Refund issued." }],
      },
    ]);
    const t = trace([
      span({
        spanId: "agent",
        name: "invoke_agent support-triage",
        startNs: "0",
        durNs: "5000000000",
        attributes: {
          "gen_ai.operation.name": "invoke_agent",
          "gen_ai.input.messages": input,
          "gen_ai.output.messages": output,
        },
      }),
      span({
        spanId: "tool-2",
        name: "execute_tool issue_refund",
        startNs: "3000000000",
        attributes: {
          "gen_ai.operation.name": "execute_tool",
          "gen_ai.tool.name": "issue_refund",
        },
      }),
      span({
        spanId: "tool-1",
        name: "execute_tool lookup_order",
        startNs: "1000000000",
        attributes: {
          "gen_ai.operation.name": "execute_tool",
          "gen_ai.tool.name": "lookup_order",
        },
      }),
      span({
        spanId: "chat-1",
        name: "chat",
        startNs: "500000000",
        attributes: {
          "gen_ai.operation.name": "chat",
          "gen_ai.usage.input_tokens": 100,
          "gen_ai.usage.output_tokens": 40,
        },
      }),
    ]);

    const agent = agentTraceOf(t);
    expect(agent.tools).toEqual(["lookup_order", "issue_refund"]);
    expect(agent.llmCalls).toBe(1);
    expect(agent.tokens).toBe(140);
    expect(agent.durationMs).toBe(5000);
    expect(agent.input).toBe("Refund order #88213");
    expect(agent.output).toBe("Refund issued.");
  });

  it("falls back to the agent span's own usage when there are no chat spans", () => {
    const t = trace([
      span({
        spanId: "agent",
        attributes: {
          "gen_ai.operation.name": "invoke_agent",
          "gen_ai.usage.input_tokens": 10,
          "gen_ai.usage.output_tokens": 5,
        },
      }),
    ]);
    expect(agentTraceOf(t).tokens).toBe(15);
  });

  it("reports null tokens when there is nothing to sum", () => {
    const t = trace([
      span({ attributes: { "gen_ai.operation.name": "invoke_agent" } }),
    ]);
    expect(agentTraceOf(t).tokens).toBeNull();
  });
});

describe("fetchAgentSpans", () => {
  it("builds each trace's AgentTrace from the slim GenAI span rows", async () => {
    let fields: string[] = [];
    answer((doc) => {
      fields = (doc as { fields: string[] }).fields;
      // trace_id, span_id, start, duration, span.name, operation, tool,
      // input/output tokens, input/output messages
      return {
        result: "rows",
        window: { start_ns: 0, end_ns: 0 },
        rows: [
          ["t1", "tool-b", 3_000, 10, "execute_tool b", "execute_tool", "b"],
          ["t1", "tool-a", 2_000, 10, "execute_tool a", "execute_tool", "a"],
          ["t1", "chat", 1_500, 10, "chat", "chat", null, 30, 12],
          [
            "t1",
            "agent",
            1_000,
            2_000_000_000,
            "invoke_agent",
            "invoke_agent",
            null,
            null,
            null,
            JSON.stringify([
              { role: "user", parts: [{ type: "text", content: "hi" }] },
            ]),
          ],
        ],
      } as QueryIrResponse;
    });
    const traces = await fetchAgentSpans(RANGE, ["t1"]);
    expect(fields).not.toContain("span.attributes");
    expect(traces.get("t1")).toEqual({
      traceId: "t1",
      tools: ["a", "b"],
      durationMs: 2000,
      llmCalls: 1,
      tokens: 42,
      input: "hi",
      output: null,
    });
  });
});
