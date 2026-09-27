import { describe, expect, it } from "vitest";
import type { EvalRun } from "../../api/evals";
import {
  baselinesOf,
  classifyCase,
  compareCases,
  emptyStats,
  foldStats,
  meanOf,
  passRateOf,
  pickRuns,
  runStatus,
  statsDelta,
  toolDiff,
  verdictOfLabel,
  type EvalStats,
} from "./evalModel";

function stats(p: Partial<EvalStats>): EvalStats {
  return { ...emptyStats(), ...p };
}

describe("verdictOfLabel", () => {
  it("recognises pass and fail labels case-insensitively", () => {
    expect(verdictOfLabel("Pass")).toBe("pass");
    expect(verdictOfLabel("safe")).toBe("pass");
    expect(verdictOfLabel("UNSAFE")).toBe("fail");
    expect(verdictOfLabel("incorrect")).toBe("fail");
    expect(verdictOfLabel("partial")).toBeNull();
    expect(verdictOfLabel(null)).toBeNull();
  });
});

describe("foldStats", () => {
  it("counts errors apart and never as failures", () => {
    const s = foldStats(emptyStats(), {
      label: null,
      error: "timeout",
      n: 412,
      high: 0,
      low: 0,
      scoreSum: 0,
      scored: 0,
    });
    expect(s).toMatchObject({ results: 412, errors: 412, pass: 0, fail: 0 });
    expect(passRateOf(s)).toBeNull();
  });

  it("lets a recognised label win over the score", () => {
    const s = foldStats(emptyStats(), {
      label: "pass",
      error: null,
      n: 3,
      high: 0,
      low: 3,
      scoreSum: 0.99,
      scored: 3,
    });
    expect(s.pass).toBe(3);
    expect(s.fail).toBe(0);
    expect(meanOf(s)).toBeCloseTo(0.33);
  });

  it("falls back to the 0.5 threshold without a recognised label", () => {
    const s = foldStats(emptyStats(), {
      label: "partial",
      error: null,
      n: 4,
      high: 3,
      low: 1,
      scoreSum: 2.8,
      scored: 4,
    });
    expect(s).toMatchObject({ pass: 3, fail: 1 });
    expect(passRateOf(s)).toBe(0.75);
  });

  it("leaves label-less, score-less results without a verdict", () => {
    const s = foldStats(emptyStats(), {
      label: "maybe",
      error: null,
      n: 2,
      high: 0,
      low: 0,
      scoreSum: 0,
      scored: 0,
    });
    expect(s).toMatchObject({ results: 2, pass: 0, fail: 0 });
    expect(meanOf(s)).toBeNull();
  });
});

describe("runStatus", () => {
  const now = 1_000_000_000;
  const base = { lastMs: now - 12 * 60_000, errors: 0, unlinked: 0 };

  it("is in progress while results are under 10 minutes old", () => {
    expect(runStatus({ ...base, lastMs: now - 60_000 }, now).kind).toBe(
      "running",
    );
  });

  it("is complete when quiet, linked and error-free", () => {
    expect(runStatus(base, now)).toEqual({ kind: "complete", reasons: [] });
  });

  it("is partial with its reasons", () => {
    expect(runStatus({ ...base, errors: 412, unlinked: 4 }, now)).toEqual({
      kind: "partial",
      reasons: ["412 not scored", "4 unmatched"],
    });
  });
});

describe("classifyCase", () => {
  it("flags a pass turning into a fail as a regression, even with gains elsewhere", () => {
    const c = classifyCase(
      new Map([
        ["Correctness", stats({ pass: 1, scoreSum: 0.95, scored: 1 })],
        ["Groundedness", stats({ pass: 1, scoreSum: 0.5, scored: 1 })],
      ]),
      new Map([
        ["Correctness", stats({ fail: 1, scoreSum: 0.2, scored: 1 })],
        ["Groundedness", stats({ pass: 1, scoreSum: 0.9, scored: 1 })],
      ]),
    );
    expect(c.kind).toBe("regression");
    expect(c.evaluators.get("Correctness")).toBe("worse");
    expect(c.evaluators.get("Groundedness")).toBe("better");
  });

  it("ignores wobbles under 0.05", () => {
    const c = classifyCase(
      new Map([["G", stats({ pass: 1, scoreSum: 0.95, scored: 1 })]]),
      new Map([["G", stats({ pass: 1, scoreSum: 0.96, scored: 1 })]]),
    );
    expect(c.kind).toBe("unchanged");
  });

  it("marks cases with no baseline", () => {
    const c = classifyCase(
      undefined,
      new Map([["G", stats({ fail: 1, scoreSum: 0.1, scored: 1 })]]),
    );
    expect(c.noBaseline).toBe(true);
    expect(c.kind).toBe("regression");
  });
});

describe("compareCases", () => {
  it("sorts regressions by largest drop first", () => {
    const b = new Map([
      ["small", new Map([["G", stats({ pass: 1, scoreSum: 0.9, scored: 1 })]])],
      ["big", new Map([["G", stats({ pass: 1, scoreSum: 1, scored: 1 })]])],
    ]);
    const c = new Map([
      ["small", new Map([["G", stats({ pass: 1, scoreSum: 0.8, scored: 1 })]])],
      ["big", new Map([["G", stats({ fail: 1, scoreSum: 0.2, scored: 1 })]])],
    ]);
    expect(compareCases(b, c).map((r) => [r.caseId, r.kind])).toEqual([
      ["big", "regression"],
      ["small", "regression"],
    ]);
  });
});

describe("toolDiff", () => {
  it("marks a skipped call", () => {
    expect(
      toolDiff(
        ["lookup_order", "check_policy", "issue_refund"],
        ["lookup_order", "issue_refund"],
      ),
    ).toEqual([
      { name: "lookup_order", kind: "same" },
      { name: "check_policy", kind: "skipped" },
      { name: "issue_refund", kind: "same" },
    ]);
  });

  it("marks repeated, reordered and new calls", () => {
    expect(
      toolDiff(
        ["lookup_order", "track_shipment"],
        ["lookup_order", "lookup_order", "track_shipment"],
      ).map((t) => t.kind),
    ).toEqual(["same", "repeated", "same"]);
    // The LCS keeps b in place, so a is the call that moved.
    expect(
      toolDiff(["a", "b", "c"], ["b", "a", "c"]).map((t) => [t.name, t.kind]),
    ).toEqual([
      ["b", "same"],
      ["a", "reordered"],
      ["c", "same"],
    ]);
    expect(toolDiff([], ["search_kb"])).toEqual([
      { name: "search_kb", kind: "new" },
    ]);
  });
});

function run(id: string, set: string | null, firstMs: number): EvalRun {
  return {
    id,
    set,
    agent: null,
    version: null,
    firstMs,
    lastMs: firstMs,
    results: 0,
    errors: 0,
    unlinked: 0,
    cases: 0,
    stats: emptyStats(),
  };
}

describe("baselinesOf / pickRuns", () => {
  // Newest first, as fetchRuns returns them.
  const runs = [
    run("c3", "golden", 30),
    run("x2", "other", 25),
    run("c2", "golden", 20),
    run("c1", "golden", 10),
  ];

  it("pairs each run with the newest earlier run of its eval set", () => {
    const b = baselinesOf(runs);
    expect(b.get("c3")?.id).toBe("c2");
    expect(b.get("c2")?.id).toBe("c1");
    expect(b.has("c1")).toBe(false);
    expect(b.has("x2")).toBe(false);
  });

  it("defaults to the newest run with a baseline, and honours the URL", () => {
    const def = pickRuns(runs, "", "");
    expect([def.baseline?.id, def.candidate?.id]).toEqual(["c2", "c3"]);
    const url = pickRuns(runs, "c1", "c3");
    expect([url.baseline?.id, url.candidate?.id]).toEqual(["c1", "c3"]);
  });
});

describe("statsDelta", () => {
  it("compares means when both sides have one, else pass rates", () => {
    expect(
      statsDelta(
        stats({ scoreSum: 0.9, scored: 1 }),
        stats({ scoreSum: 0.6, scored: 1 }),
      ),
    ).toEqual({ d: expect.closeTo(-0.3), unit: "score" });
    expect(
      statsDelta(stats({ pass: 3, fail: 1 }), stats({ pass: 1, fail: 1 })),
    ).toEqual({ d: -0.25, unit: "pp" });
    expect(statsDelta(stats({}), stats({ pass: 1 }))).toBeNull();
  });
});
