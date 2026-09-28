// Shared Storybook fixture data for the Evaluate pages: one agent
// (support-triage), one eval set (triage-golden-200) replayed as a
// baseline (v1.7.3) and a candidate (v1.8.0) run, and the evaluators that
// scored them. Every `Pages/Evals/*` story answers `/api/v1/query` from
// this one scenario via {@link evalsIrResponse}, so the pages agree with
// each other the way they would against a real backend.
import { F } from "../../api/evals";
import { irColumn } from "../../api/queryIr";

const AGENT = "support-triage";
const EVAL_SET = "triage-golden-200";
const BASELINE_RUN = "run-0923-0915";
const CANDIDATE_RUN = "run-0927-1004";
const BASELINE_VERSION = "v1.7.3";
const CANDIDATE_VERSION = "v1.8.0";

const MS = 1_000_000;
export const DAY_MS = 86_400_000;

const BASELINE_FIRST_MS = Date.UTC(2026, 8, 23, 9, 15);
const CANDIDATE_FIRST_MS = Date.UTC(2026, 8, 27, 10, 4);

/**
 * The window a story should pass as `state.range`: wide enough that the
 * "previous window" (an equal-length span right before it — see
 * `evalsIrResponse`'s `previous` check) falls entirely before
 * {@link BASELINE_FIRST_MS}, so Agents & scores' window-over-window compare
 * has a real previous window to fall back to instead of repeating the
 * current one.
 */
export const SCENARIO_RANGE = {
  type: "absolute" as const,
  fromMs: BASELINE_FIRST_MS + DAY_MS,
  toMs: CANDIDATE_FIRST_MS + DAY_MS,
};

interface EvaluatorSpec {
  name: string;
  evaluatorId: string;
  operation: "execute_tool" | "chat" | "invoke_agent";
  kind: "score" | "label" | "both";
  /** Only scored offline (baseline/candidate cases), never in production. */
  offlineOnly?: boolean;
  /** Only scored in production, never as part of an offline run. */
  productionOnly?: boolean;
}

const EVALUATORS: EvaluatorSpec[] = [
  {
    name: "ToolTrajectory",
    evaluatorId: "trajectory-match@2.1.0",
    operation: "execute_tool",
    kind: "both",
    offlineOnly: true,
  },
  {
    name: "Correctness",
    evaluatorId: "correctness-judge@1.4",
    operation: "chat",
    kind: "label",
  },
  {
    name: "Groundedness",
    evaluatorId: "groundedness@1.0",
    operation: "chat",
    kind: "score",
  },
  {
    name: "ToolArgsValid",
    evaluatorId: "args-schema@1.2",
    operation: "execute_tool",
    kind: "label",
    offlineOnly: true,
  },
  {
    name: "Toxicity",
    evaluatorId: "toxicity@1.0",
    operation: "chat",
    kind: "label",
    productionOnly: true,
  },
];

interface CaseResult {
  label?: "pass" | "fail" | "safe" | "unsafe";
  score?: number;
  error?: "timeout";
}

interface CaseFixture {
  id: string;
  input: string;
  baseline: { tools: string[]; results: Record<string, CaseResult> };
  candidate: { tools: string[]; results: Record<string, CaseResult> };
}

function fill(
  tools: string[],
  results: Record<string, CaseResult>,
  overrides: Partial<Record<string, CaseResult>> = {},
): { tools: string[]; results: Record<string, CaseResult> } {
  return {
    tools,
    results: { ...results, ...overrides } as Record<string, CaseResult>,
  };
}

/** The three named cases from the scenario, plus filler cases so the
 * aggregate tables (Runs, Agents & scores, Evaluators) show real counts. */
const CASES: CaseFixture[] = [
  {
    id: "case-117",
    input: "Refund order #88213, it arrived broken",
    baseline: {
      tools: ["lookup_order", "check_policy", "issue_refund"],
      results: {
        ToolTrajectory: { label: "pass", score: 0.95 },
        Correctness: { label: "pass" },
        Groundedness: { score: 0.85 },
        ToolArgsValid: { label: "pass" },
      },
    },
    candidate: {
      // check_policy skipped.
      tools: ["lookup_order", "issue_refund"],
      results: {
        ToolTrajectory: { label: "fail", score: 0.3 },
        Correctness: { label: "fail" },
        Groundedness: { score: 0.4 },
        ToolArgsValid: { label: "pass" },
      },
    },
  },
  {
    id: "case-088",
    input: "I never got a confirmation email for order #71190",
    baseline: {
      tools: ["lookup_order", "lookup_order"],
      results: {
        ToolTrajectory: { label: "pass", score: 0.9 },
        Correctness: { label: "pass" },
        Groundedness: { score: 0.78 },
        ToolArgsValid: { label: "pass" },
      },
    },
    candidate: {
      tools: ["lookup_order", "lookup_order"],
      results: {
        ToolTrajectory: { label: "pass", score: 0.9 },
        Correctness: { label: "pass" },
        Groundedness: { score: 0.8 },
        ToolArgsValid: { label: "pass" },
      },
    },
  },
  {
    id: "case-064",
    input: "What's your return policy for opened electronics?",
    baseline: {
      tools: ["lookup_order"],
      results: {
        ToolTrajectory: { label: "pass", score: 0.85 },
        Correctness: { label: "fail" },
        Groundedness: { score: 0.5 },
        ToolArgsValid: { label: "pass" },
      },
    },
    candidate: {
      // A new search_kb call grounds the answer in the policy doc.
      tools: ["lookup_order", "search_kb"],
      results: {
        ToolTrajectory: { label: "pass", score: 0.95 },
        Correctness: { label: "pass" },
        Groundedness: { score: 0.82 },
        ToolArgsValid: { label: "pass" },
      },
    },
  },
  ...Array.from({ length: 9 }, (_, i) => {
    const id = `case-${String(i + 1).padStart(3, "0")}`;
    const timedOut = i === 2 || i === 6; // a couple of judge timeouts
    const unchanged = fill(["lookup_order"], {
      ToolTrajectory: { label: "pass", score: 0.9 },
      Correctness: { label: "pass" },
      Groundedness: { score: 0.8 },
      ToolArgsValid: { label: "pass" },
    });
    return {
      id,
      input: "Where is my package for order #" + (90000 + i) + "?",
      baseline: unchanged,
      candidate: timedOut
        ? fill(["lookup_order"], unchanged.results, {
            Correctness: { error: "timeout" },
          })
        : unchanged,
    };
  }),
];

// ---- eval sets (`/api/v1/eval-sets…`) ----------------------------------------

const REFERENCES: Record<string, string> = {
  "case-117": "Decline the refund, open a warranty claim",
  "case-088": "Resend the confirmation, share the order status",
  "case-064": "30 days, unused, with receipt; opened electronics excluded",
};

/** A W3C trace id for the fixture's `n`th trace-sourced case. */
const hexTrace = (n: number) =>
  `4bf92f3577b34da6a3ce929d0e0e${(0x4700 + n).toString(16)}`;

/** triage-golden-200's cases: every scenario case, its baseline tools as
 * the expected trajectory; the named cases came from traces, the filler
 * cases were written by hand. */
function triageCases() {
  return CASES.map((c, i) => ({
    id: c.id,
    input: c.input,
    expected_tools: c.baseline.tools,
    ...(REFERENCES[c.id] ? { reference: REFERENCES[c.id] } : {}),
    source:
      i < 3
        ? { kind: "trace" as const, trace_id: hexTrace(i) }
        : { kind: "hand_written" as const },
  }));
}

const setLinks = (name: string) => ({
  self: { href: `/api/v1/eval-sets/${name}` },
  replace: { href: `/api/v1/eval-sets/${name}`, method: "PUT" },
  delete: { href: `/api/v1/eval-sets/${name}`, method: "DELETE" },
  append_cases: { href: `/api/v1/eval-sets/${name}/cases`, method: "POST" },
  append_cases_from_traces: {
    href: `/api/v1/eval-sets/${name}/cases/from-traces`,
    method: "POST",
  },
});

interface FixtureSet {
  name: string;
  agent: string;
  description: string;
  createdMs: number;
  updatedMs: number;
  cases: {
    id: string;
    input: string;
    expected_tools?: string[];
    reference?: string;
    tags?: string[];
    source?:
      | { kind: "trace"; trace_id: string }
      | { kind: "upload" }
      | { kind: "hand_written" };
  }[];
}

const FIXTURE_SETS: FixtureSet[] = [
  {
    name: "billing-golden-80",
    agent: "billing-assist",
    description: "Invoices, plan changes, tax questions",
    createdMs: Date.UTC(2026, 8, 10, 9),
    updatedMs: Date.UTC(2026, 8, 12, 14),
    cases: [
      {
        id: "trace-71ac0e92a1b2c3d4",
        input: "Why was I charged tax on my March invoice?",
        expected_tools: ["lookup_invoice", "explain_tax"],
        source: { kind: "trace", trace_id: hexTrace(10) },
      },
      {
        id: "trace-c30d8b14e5f60718",
        input: "Move me to the annual plan",
        expected_tools: ["lookup_account", "change_plan"],
        source: { kind: "trace", trace_id: hexTrace(11) },
      },
    ],
  },
  {
    name: "refund-edge-cases-40",
    agent: AGENT,
    description: "Out-of-window, partial and duplicate refunds",
    createdMs: Date.UTC(2026, 8, 20, 11),
    updatedMs: Date.UTC(2026, 8, 22, 16),
    cases: [
      {
        id: "edge-01",
        input: "Refund order #71002, delivered 35 days ago",
        expected_tools: ["lookup_order", "check_policy"],
        reference: "Decline: outside the 30-day window",
        source: { kind: "upload" },
      },
      {
        id: "edge-02",
        input: "Refund half of order #71118, one item broke",
        expected_tools: ["lookup_order", "check_policy", "issue_refund"],
        reference: "Partial refund for the broken item",
        source: { kind: "upload" },
      },
      {
        id: "edge-03",
        input: "You refunded me twice, is that ok?",
        expected_tools: ["lookup_payments"],
        source: { kind: "upload" },
      },
    ],
  },
  {
    name: "regressions-0927",
    agent: AGENT,
    description: "Cases that regressed in v1.8.0 against v1.7.3",
    createdMs: Date.UTC(2026, 8, 27, 12),
    updatedMs: Date.UTC(2026, 8, 27, 12),
    cases: [
      {
        id: "case-117",
        input: "Refund order #88213, it arrived broken",
        expected_tools: ["lookup_order", "check_policy", "issue_refund"],
        reference: REFERENCES["case-117"],
        tags: ["saved-from-compare"],
        source: { kind: "trace", trace_id: hexTrace(20) },
      },
    ],
  },
  {
    name: EVAL_SET,
    agent: AGENT,
    description: "Core support flows: refunds, tracking, account",
    createdMs: Date.UTC(2026, 8, 1, 9),
    updatedMs: Date.UTC(2026, 8, 25, 15),
    cases: triageCases(),
  },
];

function setSummary(s: FixtureSet) {
  return {
    name: s.name,
    agent: s.agent,
    description: s.description,
    case_count: s.cases.length,
    created_at: new Date(s.createdMs).toISOString(),
    updated_at: new Date(s.updatedMs).toISOString(),
    _links: setLinks(s.name),
  };
}

function sourceCounts(s: FixtureSet) {
  const counts = { trace: 0, upload: 0, hand_written: 0 };
  for (const c of s.cases) counts[c.source?.kind ?? "hand_written"]++;
  return counts;
}

/** `GET /api/v1/eval-sets`. */
export const EVAL_SET_LIST = {
  items: FIXTURE_SETS.map((s) => ({
    ...setSummary(s),
    sources: sourceCounts(s),
  })),
  _links: {
    self: { href: "/api/v1/eval-sets" },
    create: { href: "/api/v1/eval-sets", method: "POST" },
  },
};

/** `GET /api/v1/eval-sets/{name}` for each fixture set. */
export const EVAL_SET_DETAILS = Object.fromEntries(
  FIXTURE_SETS.map((s) => [
    s.name,
    {
      ...setSummary(s),
      tenant_id: "acme",
      dataset: "production",
      cases: s.cases,
    },
  ]),
);

/** Every case id run in both the baseline and the candidate. */
const CASE_IDS = CASES.map((c) => c.id);

function rangeBounds(range: unknown): { fromMs: number; toMs: number } {
  const r = (range ?? {}) as { from?: string; to?: string };
  const toMs = Number(r.to ?? 0) / MS || Date.now();
  const fromMs = Number(r.from ?? 0) / MS || toMs - 7 * DAY_MS;
  return { fromMs, toMs };
}

function table(rows: unknown[][]) {
  return { result: "table", window: { start_ns: 0, end_ns: 0 }, rows };
}

/** The Query IR document shape the fixtures dispatch on. */
export interface IrDoc {
  from?: string;
  result?: string;
  fields?: string[];
  irVersion?: number;
  range?: { from?: string; to?: string };
  pipeline?: {
    where?: { field?: string; op?: string; value?: unknown };
    aggregate?: { by?: string[]; aggs?: { as: string }[]; step?: string };
  }[];
}

/** The columns the server answers `doc` with: an aggregate's `by` fields
 * then its `as` names, else the projected `fields`. */
function irColumnsOf(doc: IrDoc): string[] {
  const agg = doc.pipeline?.find((s) => s.aggregate)?.aggregate;
  if (agg)
    return [
      ...(agg.by ?? []).map(irColumn),
      ...(agg.aggs ?? []).map((a) => a.as),
    ];
  return (doc.fields ?? []).map((f) =>
    f === "duration" ? "duration_nanos" : irColumn(f),
  );
}

/** `res` with the columns `doc` would get, unless it names its own —
 * lets a fixture answer with positional rows only. */
export function withColumns(doc: unknown, res: unknown): unknown {
  const r = res as { rows?: unknown[]; columns?: unknown[] };
  if (!r || !r.rows || r.columns?.length) return res;
  return {
    ...r,
    columns: irColumnsOf(doc as IrDoc).map((name) => ({ name, type: "" })),
  };
}

function evaluatorResult(
  c: CaseFixture,
  run: "baseline" | "candidate",
  name: string,
): CaseResult | undefined {
  return c[run].results[name];
}

/** Aggregated [label, error, n, high, low, score_sum, scored] tail for one
 * evaluator across every case of one run, in the shape `decodeStats`
 * expects (one row per distinct label/error combination). */
function statRowsForEvaluator(
  run: "baseline" | "candidate",
  name: string,
): unknown[][] {
  const groups = new Map<
    string,
    { n: number; high: number; low: number; sum: number; scored: number }
  >();
  for (const c of CASES) {
    const r = evaluatorResult(c, run, name);
    if (!r) continue;
    const key = `${r.label ?? ""}\0${r.error ?? ""}`;
    const g = groups.get(key) ?? { n: 0, high: 0, low: 0, sum: 0, scored: 0 };
    g.n += 1;
    if (r.score !== undefined) {
      g.scored += 1;
      g.sum += r.score;
      if (r.score >= 0.5) g.high += 1;
      else g.low += 1;
    }
    groups.set(key, g);
  }
  return [...groups].map(([key, g]) => {
    const [label, error] = key.split("\0");
    return [label || null, error || null, g.n, g.high, g.low, g.sum, g.scored];
  });
}

/** Answers every Query IR request the five Evaluate pages issue, from the
 * scenario above. Pass it as a story's `bodyFor`. */
export function evalsIrResponse(raw: unknown): unknown {
  return withColumns(raw, answer(raw));
}

function answer(raw: unknown): unknown {
  const doc = (raw ?? {}) as IrDoc;
  const pipe = doc.pipeline ?? [];
  const agg = pipe.find((s) => s.aggregate)?.aggregate;
  const by = agg?.by ?? [];
  const { fromMs, toMs } = rangeBounds(doc.range);
  const wherePred = (field: string) =>
    pipe.find((s) => s.where?.field === field)?.where;

  // fetchTraces (CaseView): every span of the case's traces.
  if (doc.from === "traces" && doc.fields?.includes("span.attributes")) {
    const pred = wherePred("trace_id");
    const ids =
      pred?.op === "in"
        ? ((pred.value as string[]) ?? [])
        : [String(pred?.value ?? "")];
    return {
      result: "rows",
      window: { start_ns: 0, end_ns: 0 },
      columns: SPAN_COLUMNS,
      rows: ids.flatMap((id) => spansForTrace(id).map(spanRow)),
    };
  }
  // fetchAgentSpans (CompareView): the GenAI spans' slim projection.
  if (doc.from === "traces" && doc.fields?.includes(F.inputMessages)) {
    const ids = (wherePred("trace_id")?.value as string[]) ?? [];
    const columns = irColumnsOf(doc);
    return {
      result: "rows",
      window: { start_ns: 0, end_ns: 0 },
      columns: columns.map((name) => ({ name })),
      rows: ids.flatMap((id) =>
        spansForTrace(id).map((sp) => {
          const cells: Record<string, unknown> = {
            trace_id: sp.traceId,
            span_id: sp.spanId,
            start_time_unix_nano: sp.startNs,
            duration_nanos: sp.durNs,
            span_name: sp.name,
          };
          for (const [k, v] of Object.entries(sp.attributes))
            cells[irColumn(k)] = v;
          return columns.map((c) => cells[c] ?? null);
        }),
      ),
    };
  }
  // fetchSpanOperations
  if (doc.from === "traces" && doc.fields?.includes(F.operation)) {
    return table(EVALUATORS.map((e) => [`span-${e.name}`, e.operation]));
  }
  // fetchTraceVersions (Upload dialog): the linked traces' version.
  if (doc.from === "traces" && by.length === 1 && by[0] === "service.version") {
    return table([[CANDIDATE_VERSION, CASES.length]]);
  }
  // coverage: invoke_agent count
  if (doc.from === "traces" && by.length === 0) {
    return table([[CASES.length + 30]]);
  }

  // fetchAgents
  if (by.length === 2 && by.includes(F.agent)) {
    return table([[AGENT, null, CASES.length * 2]]);
  }
  // fetchMeanSeries
  if (doc.result === "series") {
    const agg = doc.pipeline?.find((s) => s.aggregate)?.aggregate;
    const step = agg?.step === "1h" ? 3_600_000 : DAY_MS;
    const buckets = Math.max(2, Math.ceil((toMs - fromMs) / step));
    return {
      result: "series",
      window: { start_ns: 0, end_ns: 0 },
      series: EVALUATORS.filter((e) => !e.productionOnly).map((e, ei) => ({
        labels: { name: e.name },
        points: Array.from({ length: buckets }, (_, i) => {
          const t = (fromMs + i * step) * MS;
          const base = e.name === "Correctness" ? 0.78 : 0.85;
          const dip = e.name === "Correctness" && i > buckets * 0.6 ? -0.15 : 0;
          const wobble = 0.02 * Math.sin(i / 2 + ei);
          return [t, Math.max(0, Math.min(1, base + dip + wobble))];
        }),
      })),
    };
  }
  // buildRunsDoc: run identity × label × error, with the stats and span.
  if (by.includes(F.set)) {
    return table(
      (
        [
          [CANDIDATE_RUN, "candidate", CANDIDATE_VERSION, CANDIDATE_FIRST_MS],
          [BASELINE_RUN, "baseline", BASELINE_VERSION, BASELINE_FIRST_MS],
        ] as const
      ).flatMap(([run, side, version, firstMs]) =>
        EVALUATORS.filter((e) => !e.productionOnly).flatMap((e) =>
          statRowsForEvaluator(side, e.name).map((tail) => [
            run,
            EVAL_SET,
            AGENT,
            null,
            version,
            null,
            ...tail,
            firstMs * MS,
            (firstMs + 22 * 60_000) * MS,
            0,
          ]),
        ),
      ),
    );
  }
  // fetchVersions
  if (by.length === 2 && by.includes(F.agentVersion)) {
    return table([
      [
        BASELINE_VERSION,
        null,
        BASELINE_FIRST_MS * MS,
        (BASELINE_FIRST_MS + 3 * DAY_MS) * MS,
      ],
      [CANDIDATE_VERSION, null, CANDIDATE_FIRST_MS * MS, toMs * MS],
    ]);
  }
  // coverage: scoredRuns (distinct trace_id)
  if (by.length === 1 && by[0] === "trace_id") {
    return table(CASE_IDS.map((id) => [`trace-cand-${id}`]));
  }
  // fetchEvaluators
  if (by.length === 2 && by.includes(F.evaluator)) {
    return table(
      EVALUATORS.map((e) => {
        const n = e.productionOnly ? 480 : CASES.length * 2;
        const scored = e.kind !== "label" ? n : 0;
        const labelled = e.kind !== "score" ? n : 0;
        const offline = e.productionOnly ? 0 : n;
        return [
          e.name,
          e.evaluatorId,
          n,
          BASELINE_FIRST_MS * MS,
          toMs * MS,
          offline,
          scored,
          labelled,
          `span-${e.name}`,
        ];
      }),
    );
  }
  // per-run case counts (by = [F.runId, F.caseId])
  if (by.length === 2 && by[0] === F.runId && by.includes(F.caseId)) {
    return table(
      [BASELINE_RUN, CANDIDATE_RUN].flatMap((run) =>
        CASE_IDS.map((id) => [run, id, 1]),
      ),
    );
  }
  // fetchRunCases' fetchStats([F.caseId, F.name]), scoped via runIds.
  if (by.length >= 2 && by.includes(F.caseId) && by.includes(F.name)) {
    const runPred = wherePred(F.runId);
    const runId = (runPred?.value as string[] | undefined)?.[0];
    const run = runId === BASELINE_RUN ? "baseline" : "candidate";
    return table(
      CASES.flatMap((c) =>
        Object.entries(c[run].results).map(([name, r]) => [
          c.id,
          name,
          r.label ?? null,
          r.error ?? null,
          1,
          r.score !== undefined && r.score >= 0.5 ? 1 : 0,
          r.score !== undefined && r.score < 0.5 ? 1 : 0,
          r.score ?? 0,
          r.score !== undefined ? 1 : 0,
        ]),
      ),
    );
  }
  // fetchStats([F.name]) — AgentsScoresView's evaluator table.
  if (by.length && by[0] === F.name) {
    const previous = fromMs < BASELINE_FIRST_MS;
    return table(
      EVALUATORS.filter((e) => !e.productionOnly).flatMap((e) => {
        const rows = statRowsForEvaluator("baseline", e.name);
        const candRows = statRowsForEvaluator("candidate", e.name);
        const chosen = previous ? rows : candRows;
        return chosen.map((tail) => [e.name, ...tail]);
      }),
    );
  }
  // the case→trace lookup (irVersion 5), scoped via runIds.
  if (doc.irVersion === 5) {
    const runPred = wherePred(F.runId);
    const runId = (runPred?.value as string[] | undefined)?.[0];
    const prefix = runId === BASELINE_RUN ? "trace-base-" : "trace-cand-";
    return table(CASE_IDS.map((id) => [id, `${prefix}${id}`]));
  }
  // fetchCaseResults (RESULT_FIELDS rows).
  if (doc.fields?.includes(F.runId)) {
    const caseIdPred = wherePred(F.caseId);
    const caseId = String(caseIdPred?.value ?? CASE_IDS[0]);
    const c = CASES.find((x) => x.id === caseId) ?? CASES[0]!;
    const rows: unknown[][] = [];
    for (const [run, key, prefix] of [
      [BASELINE_RUN, "baseline", "trace-base-"],
      [CANDIDATE_RUN, "candidate", "trace-cand-"],
    ] as const) {
      const side = c[key];
      const traceId = `${prefix}${caseId}`;
      for (const [name, r] of Object.entries(side.results)) {
        const spec = EVALUATORS.find((e) => e.name === name)!;
        const spanId =
          spec.operation === "execute_tool"
            ? `${prefix}${caseId}-tool-0`
            : `${prefix}${caseId}-agent`;
        rows.push([
          run,
          traceId,
          spanId,
          name,
          r.score ?? null,
          r.label ?? null,
          r.error
            ? `The judge timed out.`
            : name === "ToolTrajectory" && r.label === "fail"
              ? "Skipped a required policy check."
              : name === "Correctness" && r.label === "pass"
                ? "Matches the reference answer."
                : null,
          spec.evaluatorId,
          r.error ?? null,
        ]);
      }
    }
    return {
      result: "rows",
      window: { start_ns: 0, end_ns: 0 },
      columns: [],
      rows,
    };
  }
  return table([]);
}

const SPAN_COLUMNS = [
  { name: "trace_id" },
  { name: "span_id" },
  { name: "parent_span_id" },
  { name: "span_name" },
  { name: "service_name" },
  { name: "status_code" },
  { name: "status_message" },
  { name: "start_time_unix_nano" },
  { name: "duration_nanos" },
  { name: "span_kind" },
  { name: "span_attributes" },
  { name: "scope_attributes" },
  { name: "resource_attributes" },
  { name: "span_events" },
];

interface FixtureSpan {
  traceId: string;
  spanId: string;
  parentSpanId: string | null;
  name: string;
  startNs: number;
  durNs: number;
  kind: string;
  attributes: Record<string, unknown>;
  resource: Record<string, unknown>;
}

function spanRow(sp: FixtureSpan): unknown[] {
  return [
    sp.traceId,
    sp.spanId,
    sp.parentSpanId,
    sp.name,
    AGENT,
    "OK",
    null,
    sp.startNs,
    sp.durNs,
    sp.kind,
    sp.attributes,
    {},
    sp.resource,
    null,
  ];
}

/** The agent trajectory spans for one trace id (`trace-base-<case>` /
 * `trace-cand-<case>`): the case's own tool calls in order and a GenAI
 * `chat` span carrying token usage, under an `invoke_agent` span with the
 * input/output messages. */
function spansForTrace(traceId: string): FixtureSpan[] {
  const [, side, caseId] = traceId.match(/^trace-(base|cand)-(.+)$/) ?? [];
  const c = CASES.find((x) => x.id === caseId);
  if (!c) return [];
  const run = side === "base" ? "baseline" : "candidate";
  const tools = c[run].tools;
  const t0 = (run === "baseline" ? BASELINE_FIRST_MS : CANDIDATE_FIRST_MS) * MS;
  const input = JSON.stringify([
    { role: "user", parts: [{ type: "text", content: c.input }] },
  ]);
  const output = JSON.stringify([
    {
      role: "assistant",
      parts: [
        {
          type: "text",
          content:
            run === "candidate" && caseId === "case-117"
              ? "I've processed the refund for order #88213."
              : "Here's what I found — let me know if you need anything else.",
        },
      ],
    },
  ]);
  const agentId = `${traceId}-agent`;
  return [
    {
      traceId,
      spanId: agentId,
      parentSpanId: null,
      name: `invoke_agent ${AGENT}`,
      startNs: t0,
      durNs: (tools.length + 1) * 900_000_000,
      kind: "Server",
      attributes: {
        [F.operation]: "invoke_agent",
        [F.inputMessages]: input,
        [F.outputMessages]: output,
      },
      resource: {
        "service.version":
          run === "baseline" ? BASELINE_VERSION : CANDIDATE_VERSION,
      },
    },
    {
      traceId,
      spanId: `${traceId}-chat`,
      parentSpanId: agentId,
      name: "chat gpt-4o-mini",
      startNs: t0 + 50_000_000,
      durNs: 600_000_000,
      kind: "Client",
      attributes: {
        [F.operation]: "chat",
        [F.inputTokens]: 480,
        [F.outputTokens]: 120,
      },
      resource: {},
    },
    ...tools.map((tool, i) => ({
      traceId,
      spanId: `${traceId}-tool-${i}`,
      parentSpanId: agentId,
      name: `execute_tool ${tool}`,
      startNs: t0 + 800_000_000 + i * 700_000_000,
      durNs: 500_000_000,
      kind: "Internal",
      attributes: { [F.operation]: "execute_tool", [F.tool]: tool },
      resource: {},
    })),
  ];
}
