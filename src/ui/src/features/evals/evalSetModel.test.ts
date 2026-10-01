import { describe, expect, it } from "vitest";
import type { EvalRun } from "../../api/evals";
import { EvalApiError } from "../../api/evalSets";
import { emptyStats, type EvalStats } from "./evalModel";
import {
  builtFrom,
  casesToJsonl,
  caseScore,
  countMatchingCases,
  fmtBytes,
  formatOf,
  FROM_COMPARE_TAG,
  parseCasesJsonl,
  parseCsv,
  previewResults,
  regressionCases,
  regressionsDescription,
  regressionsSetName,
  runsBySet,
  saveRegressionsBlocked,
  validSetName,
} from "./evalSetModel";

const stats = (s: Partial<EvalStats>): EvalStats => ({
  ...emptyStats(),
  results: 1,
  ...s,
});

describe("validSetName", () => {
  it("follows the server's slug rule", () => {
    expect(validSetName("triage-golden-200")).toBe(true);
    expect(validSetName("a.b_c")).toBe(true);
    expect(validSetName("")).toBe(false);
    expect(validSetName("-lead")).toBe(false);
    expect(validSetName("Upper")).toBe(false);
    expect(validSetName("a".repeat(129))).toBe(false);
  });
});

describe("case JSONL", () => {
  it("round-trips cases one per line", () => {
    const cases = [
      { id: "a", input: "x", expected_tools: ["t"] },
      { id: "b", input: "y", reference: "r" },
    ];
    const text = casesToJsonl(cases);
    expect(text.split("\n")).toHaveLength(3);
    const parsed = parseCasesJsonl(text);
    expect(parsed.errors).toEqual([]);
    expect(parsed.cases[0]).toEqual({
      ...cases[0],
      source: { kind: "upload" },
    });
    expect(parsed.cases[1]!.reference).toBe("r");
  });

  it("reports each bad line and skips blank ones", () => {
    const parsed = parseCasesJsonl(
      [
        '{"id":"a","input":"x","tags":["t"],"source":{"kind":"hand_written"}}',
        "",
        "not json",
        "[1]",
        '{"id":"a","input":"dup"}',
        '{"input":1,"expected_tools":"x","reference":2,"tags":[1]}',
      ].join("\n"),
    );
    expect(parsed.cases).toHaveLength(1);
    expect(parsed.cases[0]!.source).toEqual({ kind: "hand_written" });
    expect(parsed.errors.map((e) => e.line)).toEqual([3, 4, 5, 6]);
    expect(parsed.errors[0]!.reason).toBe("not valid JSON");
    expect(parsed.errors[1]!.reason).toBe("not a JSON object");
    expect(parsed.errors[2]!.reason).toContain("duplicate id");
    expect(parsed.errors[3]!.reason).toContain("`id`");
    expect(parsed.errors[3]!.reason).toContain("`expected_tools`");
    expect(parsed.errors[3]!.reason).toContain("`reference`");
    expect(parsed.errors[3]!.reason).toContain("`tags`");
  });
});

describe("builtFrom", () => {
  const set = (
    trace: number,
    upload: number,
    hand_written: number,
    description?: string,
  ) => ({
    case_count: trace + upload + hand_written,
    sources: { trace, upload, hand_written },
    description,
  });

  it("counts sources, names uploads and Compare saves", () => {
    expect(builtFrom(set(0, 0, 0))).toBe("empty");
    expect(builtFrom(set(2, 0, 1))).toBe("traces 2 · hand-written 1");
    expect(builtFrom(set(0, 1, 0))).toBe("JSONL upload");
    expect(builtFrom(set(0, 1, 1))).toBe("upload 1 · hand-written 1");
    expect(
      builtFrom(set(1, 0, 0, regressionsDescription("v1.8.0", "v1.7.3"))),
    ).toBe("saved from Compare");
    expect(builtFrom(set(1, 0, 0, "Refunds that regressed"))).toBe("traces 1");
  });
});

describe("saveRegressionsBlocked", () => {
  const notStored =
    "golden isn't a stored eval set, so the cases' inputs aren't known";

  it("needs a named set", () => {
    expect(saveRegressionsBlocked({ sourceSet: "" })).toBe(
      "These runs name no eval set, so the cases' inputs aren't known",
    );
  });

  it("allows saving until the list or the load says otherwise", () => {
    expect(saveRegressionsBlocked({ sourceSet: "golden" })).toBeNull();
    expect(
      saveRegressionsBlocked({ sourceSet: "golden", storedSets: ["golden"] }),
    ).toBeNull();
  });

  it("names a set the list doesn't hold", () => {
    expect(
      saveRegressionsBlocked({ sourceSet: "golden", storedSets: ["other"] }),
    ).toBe(notStored);
  });

  it("explains a failed load", () => {
    expect(
      saveRegressionsBlocked({
        sourceSet: "golden",
        loadError: new EvalApiError("no eval set golden", 404, []),
      }),
    ).toBe(notStored);
    expect(
      saveRegressionsBlocked({
        sourceSet: "golden",
        loadError: new Error("boom"),
      }),
    ).toBe("Could not load golden: boom");
  });
});

describe("countMatchingCases", () => {
  it("counts the file's case ids the set holds", () => {
    const cases = ["a", "b", "c"].map((id) => ({ id, input: "" }));
    expect(countMatchingCases(new Set(["a", "c", "z"]), cases)).toBe(2);
    expect(countMatchingCases(new Set(), cases)).toBe(0);
  });
});

describe("runsBySet", () => {
  it("groups runs by set, keeping order and skipping ad-hoc runs", () => {
    const run = (id: string, set: string | null) => ({ id, set }) as EvalRun;
    const by = runsBySet([run("r2", "s"), run("r1", "s"), run("x", null)]);
    expect([...by.keys()]).toEqual(["s"]);
    expect(by.get("s")!.map((r) => r.id)).toEqual(["r2", "r1"]);
  });
});

describe("caseScore", () => {
  it("is pass, N failing, or P of E under the pass rule", () => {
    expect(caseScore(undefined)).toBeNull();
    expect(caseScore(new Map())).toBeNull();
    expect(
      caseScore(
        new Map([
          ["A", stats({ pass: 1 })],
          ["B", stats({ pass: 1 })],
        ]),
      ),
    ).toMatchObject({ tone: "pass", text: "pass" });
    expect(
      caseScore(
        new Map([
          ["A", stats({ fail: 1 })],
          ["B", stats({ fail: 1 })],
          ["C", stats({ pass: 1 })],
        ]),
      ),
    ).toMatchObject({
      tone: "fail",
      text: "2 failing",
      title: "Failing: A, B",
    });
    expect(
      caseScore(
        new Map([
          ["A", stats({ pass: 1 })],
          ["B", stats({ errors: 1 })],
          ["C", stats({})],
        ]),
      ),
    ).toMatchObject({
      tone: "partial",
      text: "1 of 3",
      title: "No verdict: B, C",
    });
  });
});

describe("results files", () => {
  it("parses quoted CSV", () => {
    expect(parseCsv('a,b\r\n"x, ""y""",2\n\n3,')).toEqual([
      ["a", "b"],
      ['x, "y"', "2"],
      ["3", ""],
    ]);
  });

  it("previews JSONL: cases, evaluators, span-linked and run-level rows", () => {
    const p = previewResults(
      [
        '{"case_id":"c1","name":"A","score":1,"trace_id":"AB"}',
        '{"case_id":"c1","name":"B","label":"pass","trace_id":"ab"}',
        '{"case_id":"c2","name":"A","score":0}',
        '{"name":"A","score":0}',
        "oops",
        "3",
      ].join("\n"),
      "jsonl",
    );
    expect(p.rows).toBe(4);
    expect([...p.caseIds]).toEqual(["c1", "c2"]);
    expect([...p.evaluators]).toEqual(["A", "B"]);
    expect(p.linked).toBe(2);
    expect(p.runLevel).toBe(2);
    expect(p.traceIds).toEqual(["ab"]);
    expect([...p.columns].sort()).toEqual([
      "case_id",
      "label",
      "name",
      "score",
      "trace_id",
    ]);
    expect(p.errors).toEqual([
      { line: 4, reason: "no `case_id`" },
      { line: 5, reason: "not valid JSON" },
      { line: 6, reason: "not a JSON object" },
    ]);
  });

  it("previews CSV with case-insensitive headers", () => {
    const p = previewResults(
      "Case_ID,NAME,score,trace_id\nc1,A,0.5,ab\nc2,,1,\n",
      "csv",
    );
    expect(p.rows).toBe(2);
    expect(p.linked).toBe(1);
    expect(p.columns.has("case_id")).toBe(true);
    expect(p.errors).toEqual([{ line: 3, reason: "no `name`" }]);
  });

  it("names formats and sizes", () => {
    expect(formatOf("results.CSV")).toBe("csv");
    expect(formatOf("results.jsonl")).toBe("jsonl");
    expect(fmtBytes(12)).toBe("12 B");
    expect(fmtBytes(2048)).toBe("2.0 KB");
    expect(fmtBytes(1.5 * 1024 * 1024)).toBe("1.5 MB");
  });
});

describe("saving regressions", () => {
  it("names the set after the day", () => {
    expect(regressionsSetName(Date.UTC(2026, 8, 27, 10))).toBe(
      "regressions-0927",
    );
  });

  it("copies the original cases, linking the candidate's trace", () => {
    const trace = "4BF92F3577B34DA6A3CE929D0E0E4736";
    const { cases, missing } = regressionCases(
      ["a", "b", "zz"],
      [
        {
          id: "a",
          input: "in-a",
          expected_tools: ["t"],
          reference: "ref",
          tags: ["x", FROM_COMPARE_TAG],
          source: { kind: "upload" },
        },
        { id: "b", input: "in-b" },
      ],
      new Map([
        ["a", trace],
        ["b", "not-a-trace"],
      ]),
    );
    expect(missing).toEqual(["zz"]);
    expect(cases[0]).toEqual({
      id: "a",
      input: "in-a",
      expected_tools: ["t"],
      reference: "ref",
      tags: ["x", FROM_COMPARE_TAG],
      source: { kind: "trace", trace_id: trace.toLowerCase() },
    });
    expect(cases[1]).toEqual({
      id: "b",
      input: "in-b",
      tags: [FROM_COMPARE_TAG],
      source: { kind: "hand_written" },
    });
  });
});
