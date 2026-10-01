// Eval sets and results files as the UI reads them: the case JSONL format
// (the same one `signaldb-cli eval-sets export` writes and `append` reads),
// a results file's preview figures, and how a set's cases score in its
// newest run. Pure functions, so the dialogs and their tests agree.

import type { EvalRun } from "../../api/evals";
import {
  isNotFound,
  type EvalCase,
  type EvalSetSummaryResponse,
} from "../../api/evalSets";
import { toErrorMessage } from "../../api/http";
import { verdictOf, type CaseScores } from "./evalModel";

/** An eval set name: lowercase letters, digits, `-`, `_` and `.`, starting
 * with a letter or digit, 1-128 characters (the server's rule). */
const SET_NAME_RE = /^[a-z0-9][a-z0-9._-]{0,127}$/;

export function validSetName(name: string): boolean {
  return SET_NAME_RE.test(name);
}

/** Tag on the cases "Save regressed cases as eval set" creates. */
export const FROM_COMPARE_TAG = "saved-from-compare";

// ---- case JSONL -----------------------------------------------------------

/** One case per line, in set order. */
export function casesToJsonl(cases: EvalCase[]): string {
  return cases.map((c) => JSON.stringify(c)).join("\n") + "\n";
}

export interface LineError {
  line: number;
  reason: string;
}

export interface ParsedCases {
  cases: EvalCase[];
  errors: LineError[];
}

const isStrings = (v: unknown): v is string[] =>
  Array.isArray(v) && v.every((s) => typeof s === "string");

/** A JSONL file of cases, validated line by line. Cases without a `source`
 * are marked as uploaded. */
export function parseCasesJsonl(text: string): ParsedCases {
  const cases: EvalCase[] = [];
  const errors: LineError[] = [];
  const seen = new Set<string>();
  text.split(/\r?\n/).forEach((raw, i) => {
    const line = i + 1;
    if (!raw.trim()) return;
    let v: unknown;
    try {
      v = JSON.parse(raw);
    } catch {
      errors.push({ line, reason: "not valid JSON" });
      return;
    }
    if (!v || typeof v !== "object" || Array.isArray(v)) {
      errors.push({ line, reason: "not a JSON object" });
      return;
    }
    const o = v as Record<string, unknown>;
    const problems: string[] = [];
    if (typeof o.id !== "string" || !o.id || o.id.length > 128)
      problems.push("`id` must be a string of 1-128 characters");
    else if (seen.has(o.id)) problems.push(`duplicate id \`${o.id}\``);
    if (typeof o.input !== "string") problems.push("`input` must be a string");
    if (o.expected_tools != null && !isStrings(o.expected_tools))
      problems.push("`expected_tools` must be a list of strings");
    if (o.reference != null && typeof o.reference !== "string")
      problems.push("`reference` must be a string");
    if (o.tags != null && !isStrings(o.tags))
      problems.push("`tags` must be a list of strings");
    if (problems.length) {
      errors.push({ line, reason: problems.join("; ") });
      return;
    }
    const id = o.id as string;
    seen.add(id);
    cases.push({
      id,
      input: o.input as string,
      ...(isStrings(o.expected_tools)
        ? { expected_tools: o.expected_tools }
        : {}),
      ...(typeof o.reference === "string" ? { reference: o.reference } : {}),
      ...(isStrings(o.tags) ? { tags: o.tags } : {}),
      source: (o.source as EvalCase["source"]) ?? { kind: "upload" },
    });
  });
  return { cases, errors };
}

// ---- what a set was built from ---------------------------------------------

/** Start of the description "Save regressed cases as eval set" writes; the
 * list reads it back to say a set was saved from Compare. */
const FROM_COMPARE_DESCRIPTION = "Cases that regressed in ";

/** The description of a set saved from Compare. */
export function regressionsDescription(candidate: string, baseline: string) {
  return `${FROM_COMPARE_DESCRIPTION}${candidate} against ${baseline}`;
}

/** "traces 162 · hand-written 38", "JSONL upload", "saved from Compare" or
 * "empty", from the list's per-source case counts and the description. */
export function builtFrom(
  set: Pick<EvalSetSummaryResponse, "case_count" | "sources" | "description">,
): string {
  const { case_count: total, sources } = set;
  if (total === 0) return "empty";
  if (set.description?.startsWith(FROM_COMPARE_DESCRIPTION))
    return "saved from Compare";
  if (sources.upload === total) return "JSONL upload";
  return (
    [
      ["traces", sources.trace],
      ["upload", sources.upload],
      ["hand-written", sources.hand_written],
    ] as const
  )
    .filter(([, n]) => n > 0)
    .map(([label, n]) => `${label} ${n}`)
    .join(" · ");
}

// ---- runs of a set ----------------------------------------------------------

/** Each set's runs, newest first (`runs` is newest first already). */
export function runsBySet(runs: EvalRun[]): Map<string, EvalRun[]> {
  const out = new Map<string, EvalRun[]>();
  for (const r of runs) {
    if (!r.set) continue;
    const list = out.get(r.set) ?? [];
    list.push(r);
    out.set(r.set, list);
  }
  return out;
}

export interface CaseScore {
  tone: "pass" | "fail" | "partial" | null;
  text: string;
  title: string;
}

/** A case's result in one run under the pass rule: "pass" when every
 * evaluator passed, "N failing" when any failed, else "P of E" when some
 * evaluators reached no verdict (errors, unrecognised labels). */
export function caseScore(scores: CaseScores | undefined): CaseScore | null {
  if (!scores || scores.size === 0) return null;
  const names = [...scores.keys()].sort();
  const failing = names.filter((n) => verdictOf(scores.get(n)!) === "fail");
  const passing = names.filter((n) => verdictOf(scores.get(n)!) === "pass");
  if (failing.length)
    return {
      tone: "fail",
      text: `${failing.length} failing`,
      title: `Failing: ${failing.join(", ")}`,
    };
  if (passing.length === names.length)
    return { tone: "pass", text: "pass", title: `Passed: ${names.join(", ")}` };
  const open = names.filter((n) => !passing.includes(n));
  return {
    tone: "partial",
    text: `${passing.length} of ${names.length}`,
    title: `No verdict: ${open.join(", ")}`,
  };
}

// ---- results files ----------------------------------------------------------

/** The columns a results file may carry; `required` must be present in
 * every row, `oneOf` means each row needs one of score/label/error. */
export const RESULT_COLUMNS: {
  column: string;
  target: string;
  required?: boolean;
  oneOf?: boolean;
}[] = [
  { column: "case_id", target: "case_id", required: true },
  { column: "name", target: "gen_ai.evaluation.name", required: true },
  { column: "score", target: "score.value", oneOf: true },
  { column: "label", target: "score.label", oneOf: true },
  { column: "error", target: "error.type", oneOf: true },
  { column: "explanation", target: "explanation" },
  { column: "trace_id", target: "trace_id" },
  { column: "span_id", target: "span_id" },
  { column: "evaluator", target: "evaluator" },
];

export function formatOf(fileName: string): "csv" | "jsonl" {
  return fileName.toLowerCase().endsWith(".csv") ? "csv" : "jsonl";
}

/** RFC 4180 CSV: quoted fields, doubled quotes, CRLF or LF. */
export function parseCsv(text: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let quoted = false;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i]!;
    if (quoted) {
      if (ch === '"' && text[i + 1] === '"') {
        field += '"';
        i++;
      } else if (ch === '"') quoted = false;
      else field += ch;
    } else if (ch === '"') quoted = true;
    else if (ch === ",") {
      row.push(field);
      field = "";
    } else if (ch === "\n" || ch === "\r") {
      if (ch === "\r" && text[i + 1] === "\n") i++;
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
    } else field += ch;
  }
  if (field !== "" || row.length) {
    row.push(field);
    rows.push(row);
  }
  return rows.filter((r) => r.some((c) => c.trim() !== ""));
}

export interface ResultsPreview {
  rows: number;
  caseIds: Set<string>;
  evaluators: Set<string>;
  linked: number;
  runLevel: number;
  /** Known columns present in the file (lower-case). */
  columns: Set<string>;
  /** Trace ids of span-linked rows, in file order, deduplicated. */
  traceIds: string[];
  errors: LineError[];
}

const present = (v: unknown) => v !== undefined && v !== null && v !== "";

/** A results file's figures for the preview. The server validates the file
 * again on upload; this only counts what it can read. */
export function previewResults(
  text: string,
  format: "csv" | "jsonl",
): ResultsPreview {
  const out: ResultsPreview = {
    rows: 0,
    caseIds: new Set(),
    evaluators: new Set(),
    linked: 0,
    runLevel: 0,
    columns: new Set(),
    traceIds: [],
    errors: [],
  };
  const traces = new Set<string>();
  const known = new Set(RESULT_COLUMNS.map((c) => c.column));
  const add = (line: number, rec: Record<string, unknown>) => {
    out.rows++;
    for (const [k, v] of Object.entries(rec))
      if (known.has(k) && present(v)) out.columns.add(k);
    if (present(rec.case_id)) out.caseIds.add(String(rec.case_id));
    else out.errors.push({ line, reason: "no `case_id`" });
    if (present(rec.name)) out.evaluators.add(String(rec.name));
    else out.errors.push({ line, reason: "no `name`" });
    if (present(rec.trace_id)) {
      out.linked++;
      traces.add(String(rec.trace_id).toLowerCase());
    } else out.runLevel++;
  };
  if (format === "csv") {
    const [header, ...body] = parseCsv(text);
    const names = (header ?? []).map((h) => h.trim().toLowerCase());
    for (const n of names) if (known.has(n)) out.columns.add(n);
    body.forEach((cells, i) =>
      add(i + 2, Object.fromEntries(names.map((n, j) => [n, cells[j]]))),
    );
  } else {
    text.split(/\r?\n/).forEach((raw, i) => {
      if (!raw.trim()) return;
      try {
        const v: unknown = JSON.parse(raw);
        if (v && typeof v === "object" && !Array.isArray(v))
          add(i + 1, v as Record<string, unknown>);
        else out.errors.push({ line: i + 1, reason: "not a JSON object" });
      } catch {
        out.errors.push({ line: i + 1, reason: "not valid JSON" });
      }
    });
  }
  out.traceIds = [...traces];
  return out;
}

/** How many of `caseIds` are ids of `setCases`. */
export function countMatchingCases(
  caseIds: Set<string>,
  setCases: EvalCase[],
): number {
  const inSet = new Set(setCases.map((c) => c.id));
  let n = 0;
  for (const id of caseIds) if (inSet.has(id)) n++;
  return n;
}

export function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

// ---- saving regressions -------------------------------------------------------

/** Why "Save regressed cases as eval set" can't run, or null when it can.
 * `storedSets` (the list's names, once loaded) settles whether the source set
 * exists before its cases are fetched; `loadError` is the error of that
 * fetch, once the dialog makes it. */
export function saveRegressionsBlocked({
  sourceSet,
  storedSets,
  loadError,
}: {
  sourceSet: string;
  storedSets?: readonly string[];
  loadError?: unknown;
}): string | null {
  if (!sourceSet)
    return "These runs name no eval set, so the cases' inputs aren't known";
  const notStored = `${sourceSet} isn't a stored eval set, so the cases' inputs aren't known`;
  if (storedSets && !storedSets.includes(sourceSet)) return notStored;
  if (loadError == null) return null;
  return isNotFound(loadError)
    ? notStored
    : `Could not load ${sourceSet}: ${toErrorMessage(loadError)}`;
}

/** `regressions-MMDD` for the day of `ms` (UTC). */
export function regressionsSetName(ms: number): string {
  const d = new Date(ms);
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `regressions-${mm}${dd}`;
}

const TRACE_ID_RE = /^[0-9a-f]{32}$/i;

/** The regressed cases as new cases: input, expected tools and reference
 * copied from the source set's case with the same id; source the
 * candidate's trace when it is a valid trace id, else the original's. */
export function regressionCases(
  caseIds: string[],
  setCases: EvalCase[],
  candidateTraces: Map<string, string>,
): { cases: EvalCase[]; missing: string[] } {
  const byId = new Map(setCases.map((c) => [c.id, c]));
  const cases: EvalCase[] = [];
  const missing: string[] = [];
  for (const id of caseIds) {
    const original = byId.get(id);
    if (!original) {
      missing.push(id);
      continue;
    }
    const trace = candidateTraces.get(id);
    cases.push({
      id,
      input: original.input,
      ...(original.expected_tools
        ? { expected_tools: original.expected_tools }
        : {}),
      ...(original.reference != null ? { reference: original.reference } : {}),
      tags: [
        ...(original.tags ?? []).filter((t) => t !== FROM_COMPARE_TAG),
        FROM_COMPARE_TAG,
      ],
      source:
        trace && TRACE_ID_RE.test(trace)
          ? { kind: "trace", trace_id: trace.toLowerCase() }
          : (original.source ?? { kind: "hand_written" }),
    });
  }
  return { cases, missing };
}
