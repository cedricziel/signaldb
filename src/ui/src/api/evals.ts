// Queries behind the Evaluate pages. An evaluator result is a log record
// with `event_name = gen_ai.evaluation.result` (OTel GenAI semconv) linked
// to the span it scores through its trace context; `signaldb.eval.*`
// attributes group offline results into runs (see the
// `agent-evaluation-results` spec). Every figure is a Query IR read.
import type { QueryIrRequest, QueryIrResponse } from "./gen";
import {
  decodePoints,
  irColumn as col,
  namedRows,
  rangeDoc,
  runIrQuery,
  type IrPoint,
  type IrRow,
} from "./queryIr";
import { buildTraceSpansDoc, traceFromIrResponses } from "./traceDetail";
import type { TempoSpan, TempoTrace } from "./traceTypes";
import {
  emptyStats,
  foldStats,
  PASS_THRESHOLD,
  type CaseScores,
  type EvalStats,
} from "../features/evals/evalModel";
import type { ResolvedRange } from "../lib/time";
import type { EvalSource } from "../lib/urlState";

const NANOS_PER_MS = 1_000_000;

export const EVAL_EVENT = "gen_ai.evaluation.result";
export const F = {
  name: "gen_ai.evaluation.name",
  score: "gen_ai.evaluation.score.value",
  label: "gen_ai.evaluation.score.label",
  explanation: "gen_ai.evaluation.explanation",
  error: "error.type",
  agent: "gen_ai.agent.name",
  agentVersion: "gen_ai.agent.version",
  runId: "signaldb.eval.run_id",
  set: "signaldb.eval.set",
  caseId: "signaldb.eval.case_id",
  evaluator: "signaldb.eval.evaluator",
  operation: "gen_ai.operation.name",
  tool: "gen_ai.tool.name",
  inputTokens: "gen_ai.usage.input_tokens",
  outputTokens: "gen_ai.usage.output_tokens",
  inputMessages: "gen_ai.input.messages",
  outputMessages: "gen_ai.output.messages",
} as const;

type Stage = Record<string, unknown>;
type Pred = Record<string, unknown>;

const eq = (field: string, value: unknown): Pred => ({
  field,
  op: "eq",
  value,
});
const exists = (field: string): Pred => ({ field, op: "exists" });
const absent = (field: string): Pred => ({ not: exists(field) });
/** Log records without trace context store an empty trace id. */
const LINKED: Pred = {
  and: [exists("trace_id"), { field: "trace_id", op: "ne", value: "" }],
};
const UNLINKED: Pred = { not: LINKED };

const COUNT_N = { fn: "count", as: "n" };
const FIRST_SEEN = { fn: "min", of: "timestamp", as: "first" };
const LAST_SEEN = { fn: "max", of: "timestamp", as: "last" };

function irDoc(
  from: "logs" | "traces",
  result: "table" | "rows" | "series",
  range: ResolvedRange,
  pipeline: Stage[],
  extra: { irVersion?: number; fields?: string[] } = {},
): QueryIrRequest {
  return {
    irVersion: 4,
    from,
    range: rangeDoc(range),
    result,
    pipeline,
    ...extra,
  };
}

const logsTable = (range: ResolvedRange, pipeline: Stage[], irVersion = 4) =>
  irDoc("logs", "table", range, pipeline, { irVersion });

const tracesRows = (
  range: ResolvedRange,
  fields: string[],
  pipeline: Stage[],
) => irDoc("traces", "rows", range, pipeline, { irVersion: 1, fields });

const str = (v: unknown): string | null =>
  typeof v === "string" && v !== "" ? v : null;
const num = (v: unknown): number => (typeof v === "number" ? v : 0);
const msOf = (ns: unknown): number => num(ns) / NANOS_PER_MS;

interface Seen {
  firstMs: number;
  lastMs: number;
}

/** `prev`'s first/last widened by a row's `first`/`last` timestamps. */
function widen(prev: Seen | undefined, row: IrRow): Seen {
  const firstMs = msOf(row.first);
  const lastMs = msOf(row.last);
  return prev
    ? {
        firstMs: Math.min(prev.firstMs, firstMs),
        lastMs: Math.max(prev.lastMs, lastMs),
      }
    : { firstMs, lastMs };
}

/** `gen_ai.agent.name`, or the resource's `service.name` when the result
 * doesn't name its agent. */
function agentPred(agent: string): Pred {
  return {
    or: [
      eq(F.agent, agent),
      { and: [absent(F.agent), eq("service.name", agent)] },
    ],
  };
}

export interface ResultScope {
  agent?: string;
  source?: EvalSource;
  runIds?: string[];
  caseId?: string;
}

export function resultsWhere(scope: ResultScope = {}): Stage[] {
  const preds: Pred[] = [eq("event_name", EVAL_EVENT)];
  if (scope.agent) preds.push(agentPred(scope.agent));
  if (scope.source === "offline") preds.push(exists(F.runId));
  if (scope.source === "production") preds.push(absent(F.runId));
  if (scope.runIds?.length)
    preds.push({ field: F.runId, op: "in", value: scope.runIds });
  if (scope.caseId) preds.push(eq(F.caseId, scope.caseId));
  return preds.map((p) => ({ where: p }));
}

const STAT_AGGS = [
  COUNT_N,
  {
    fn: "count",
    as: "high",
    where: { field: F.score, op: "gte", value: PASS_THRESHOLD },
  },
  {
    fn: "count",
    as: "low",
    where: { field: F.score, op: "lt", value: PASS_THRESHOLD },
  },
  { fn: "sum", of: F.score, as: "score_sum" },
  { fn: "count", as: "scored", where: exists(F.score) },
];

/** Results grouped by `by` plus label and error type, with the counts the
 * pass rule needs (`foldStats`). */
export function buildStatsDoc(
  range: ResolvedRange,
  scope: ResultScope,
  by: string[],
): QueryIrRequest {
  return logsTable(range, [
    ...resultsWhere(scope),
    { aggregate: { by: [...by, F.label, F.error], aggs: STAT_AGGS } },
    { limit: 50_000 },
  ]);
}

/** One stats row (see `STAT_AGGS`) folded into `acc`. */
function foldRow(acc: EvalStats | undefined, row: IrRow): EvalStats {
  return foldStats(acc ?? emptyStats(), {
    label: str(row[col(F.label)]),
    error: str(row[col(F.error)]),
    n: num(row.n),
    high: num(row.high),
    low: num(row.low),
    scoreSum: num(row.score_sum),
    scored: num(row.scored),
  });
}

/** Folds stats rows into one `EvalStats` per `by` key (joined with NUL). */
export function decodeStats(
  res: QueryIrResponse,
  by: string[],
): Map<string, EvalStats> {
  const out = new Map<string, EvalStats>();
  for (const row of namedRows(res)) {
    const key = by
      .map((field) => {
        const k = row[col(field)];
        return k == null ? "" : String(k);
      })
      .join("\0");
    out.set(key, foldRow(out.get(key), row));
  }
  return out;
}

export async function fetchStats(
  range: ResolvedRange,
  scope: ResultScope,
  by: string[],
): Promise<Map<string, EvalStats>> {
  return decodeStats(await runIrQuery(buildStatsDoc(range, scope, by)), by);
}

// ---- agents --------------------------------------------------------------

/** Agents that sent results in the window, most results first. */
export async function fetchAgents(range: ResolvedRange): Promise<string[]> {
  const res = await runIrQuery(
    logsTable(range, [
      ...resultsWhere(),
      { aggregate: { by: [F.agent, "service.name"], aggs: [COUNT_N] } },
      { order: [{ of: "n", dir: "desc" }] },
      { limit: 200 },
    ]),
  );
  const seen = new Set<string>();
  for (const row of namedRows(res)) {
    const name = str(row[col(F.agent)]) ?? str(row.service_name);
    if (name) seen.add(name);
  }
  return [...seen];
}

// ---- trend ---------------------------------------------------------------

export interface DailyMean {
  evaluator: string;
  points: IrPoint[];
}

/** Mean score per evaluator per step, errors left out. */
export async function fetchMeanSeries(
  range: ResolvedRange,
  scope: ResultScope,
  step: string,
): Promise<DailyMean[]> {
  const res = await runIrQuery(
    irDoc("logs", "series", range, [
      ...resultsWhere(scope),
      { where: absent(F.error) },
      { where: exists(F.score) },
      {
        aggregate: {
          by: [F.name],
          aggs: [{ fn: "avg", of: F.score, as: "mean" }],
          step,
        },
      },
    ]),
  );
  return (res.series ?? []).map((s) => ({
    evaluator: Object.values(s.labels)[0] ?? "",
    points: decodePoints(s.points),
  }));
}

export interface VersionSpan {
  version: string;
  firstMs: number;
  lastMs: number;
}

/** Each agent version's first and last result, oldest first — the chart's
 * version markers. */
export async function fetchVersions(
  range: ResolvedRange,
  scope: ResultScope,
): Promise<VersionSpan[]> {
  const res = await runIrQuery(
    logsTable(range, [
      ...resultsWhere(scope),
      {
        aggregate: {
          by: [F.agentVersion, "service.version"],
          aggs: [FIRST_SEEN, LAST_SEEN],
        },
      },
    ]),
  );
  const byVersion = new Map<string, VersionSpan>();
  for (const row of namedRows(res)) {
    const version =
      str(row[col(F.agentVersion)]) ?? str(row[col("service.version")]);
    if (!version) continue;
    byVersion.set(version, {
      version,
      ...widen(byVersion.get(version), row),
    });
  }
  return [...byVersion.values()].sort((a, b) => a.firstMs - b.firstMs);
}

// ---- coverage ------------------------------------------------------------

export interface Coverage {
  /** `invoke_agent` spans of the agent in the window. */
  agentRuns: number;
  /** Distinct traces holding at least one result. */
  scoredRuns: number;
}

export async function fetchCoverage(
  range: ResolvedRange,
  scope: ResultScope,
): Promise<Coverage> {
  const agentPreds: Stage[] = [{ where: eq(F.operation, "invoke_agent") }];
  if (scope.agent) agentPreds.push({ where: eq(F.agent, scope.agent) });
  const [spans, traces] = await Promise.all([
    runIrQuery(
      irDoc("traces", "table", range, [
        ...agentPreds,
        { aggregate: { by: [], aggs: [COUNT_N] } },
      ]),
    ),
    runIrQuery(
      logsTable(range, [
        ...resultsWhere(scope),
        { where: LINKED },
        { aggregate: { by: ["trace_id"], aggs: [COUNT_N] } },
        { limit: 100_000 },
      ]),
    ),
  ]);
  return {
    agentRuns: num(namedRows(spans)[0]?.n),
    scoredRuns: traces.rows?.length ?? 0,
  };
}

// ---- runs ----------------------------------------------------------------

export interface EvalRun {
  id: string;
  set: string | null;
  agent: string | null;
  version: string | null;
  firstMs: number;
  lastMs: number;
  results: number;
  errors: number;
  /** Results without trace context: they score the run, not a span. */
  unlinked: number;
  cases: number;
  stats: EvalStats;
}

const RUN_BY = [
  F.runId,
  F.set,
  F.agent,
  "service.name",
  F.agentVersion,
  "service.version",
];

/** Offline runs: grouped by run identity plus label and error, so one
 * query carries both the run's span and its pass-rule stats. */
export function buildRunsDoc(
  range: ResolvedRange,
  scope: ResultScope,
): QueryIrRequest {
  return logsTable(range, [
    ...resultsWhere({ ...scope, source: "offline" }),
    {
      aggregate: {
        by: [...RUN_BY, F.label, F.error],
        aggs: [
          ...STAT_AGGS,
          FIRST_SEEN,
          LAST_SEEN,
          { fn: "count", as: "unlinked", where: UNLINKED },
        ],
      },
    },
    { limit: 50_000 },
  ]);
}

/** Offline runs in the window, newest first, with their case counts and
 * stats. */
export async function fetchRuns(
  range: ResolvedRange,
  scope: ResultScope,
): Promise<EvalRun[]> {
  const [runsRes, casesRes] = await Promise.all([
    runIrQuery(buildRunsDoc(range, scope)),
    // The IR has no distinct count, so cases are counted from the
    // (run, case) groups.
    runIrQuery(
      logsTable(range, [
        ...resultsWhere({ ...scope, source: "offline" }),
        { aggregate: { by: [F.runId, F.caseId], aggs: [COUNT_N] } },
        { limit: 100_000 },
      ]),
    ),
  ]);
  const cases = new Map<string, number>();
  for (const row of namedRows(casesRes)) {
    const run = str(row[col(F.runId)]);
    if (run && str(row[col(F.caseId)]))
      cases.set(run, (cases.get(run) ?? 0) + 1);
  }
  const runs = new Map<string, EvalRun>();
  for (const row of namedRows(runsRes)) {
    const id = str(row[col(F.runId)]);
    if (!id) continue;
    const prev = runs.get(id);
    const stats = foldRow(prev?.stats, row);
    runs.set(id, {
      id,
      set: prev?.set ?? str(row[col(F.set)]),
      agent: prev?.agent ?? str(row[col(F.agent)]) ?? str(row.service_name),
      version:
        prev?.version ??
        str(row[col(F.agentVersion)]) ??
        str(row[col("service.version")]),
      ...widen(prev, row),
      results: stats.results,
      errors: stats.errors,
      unlinked: (prev?.unlinked ?? 0) + num(row.unlinked),
      cases: cases.get(id) ?? 0,
      stats,
    });
  }
  return [...runs.values()].sort((a, b) => b.firstMs - a.firstMs);
}

// ---- per-case results of two runs -----------------------------------------

export interface RunCases {
  /** case id → evaluator → stats */
  cases: Map<string, CaseScores>;
  /** case id → the trace the run's agent produced for it */
  traces: Map<string, string>;
}

export async function fetchRunCases(
  range: ResolvedRange,
  runId: string,
): Promise<RunCases> {
  const scope = { runIds: [runId] };
  const [stats, tracesRes] = await Promise.all([
    fetchStats(range, scope, [F.caseId, F.name]),
    runIrQuery(
      logsTable(
        range,
        [
          ...resultsWhere(scope),
          { where: LINKED },
          {
            aggregate: {
              by: [F.caseId],
              aggs: [{ fn: "first", of: "trace_id", as: "trace" }],
            },
          },
          { limit: 10_000 },
        ],
        5,
      ),
    ),
  ]);
  const cases = new Map<string, CaseScores>();
  for (const [key, s] of stats) {
    const [caseId, name] = key.split("\0") as [string, string];
    if (!caseId || !name) continue;
    const scores = cases.get(caseId) ?? new Map<string, EvalStats>();
    scores.set(name, s);
    cases.set(caseId, scores);
  }
  const traces = new Map<string, string>();
  for (const row of namedRows(tracesRes)) {
    const caseId = str(row[col(F.caseId)]);
    const trace = str(row.trace);
    if (caseId && trace) traces.set(caseId, trace);
  }
  return { cases, traces };
}

// ---- agent traces --------------------------------------------------------

export interface AgentTrace {
  traceId: string;
  /** `execute_tool` names in call order. */
  tools: string[];
  /** The `invoke_agent` span's duration. */
  durationMs: number | null;
  llmCalls: number;
  tokens: number | null;
  input: string | null;
  output: string | null;
}

/** What `agentTraceFromSpans` reads off one span, from either a full trace
 * or the slim rows of `fetchAgentSpans`. */
export interface AgentSpan {
  name: string;
  startNs: bigint;
  durNs: bigint;
  attributes: Record<string, unknown>;
}

/** Spans of many traces at once, split per trace. */
export async function fetchTraces(
  range: ResolvedRange,
  traceIds: string[],
): Promise<Map<string, TempoTrace>> {
  const out = new Map<string, TempoTrace>();
  if (traceIds.length === 0) return out;
  const res = await runIrQuery(buildTraceSpansDoc(traceIds, range));
  const idCol = (res.columns ?? []).findIndex((c) => c.name === "trace_id");
  const byTrace = new Map<string, unknown[][]>();
  for (const row of res.rows ?? []) {
    const id = String(row[idCol]);
    let rows = byTrace.get(id);
    if (!rows) byTrace.set(id, (rows = []));
    rows.push(row);
  }
  for (const [id, rows] of byTrace) {
    const trace = traceFromIrResponses(id, { ...res, rows }, undefined);
    if (trace) out.set(id, trace);
  }
  return out;
}

const AGENT_OPS = ["invoke_agent", "execute_tool", "chat"];
const AGENT_ATTRS = [
  F.operation,
  F.tool,
  F.inputTokens,
  F.outputTokens,
  F.inputMessages,
  F.outputMessages,
];

const nanos = (v: unknown): bigint => {
  if (typeof v === "number") return BigInt(Math.trunc(v));
  if (typeof v === "string" && /^\d+$/.test(v)) return BigInt(v);
  return 0n;
};

/** Just the GenAI spans Compare reads, with only the fields
 * `agentTraceFromSpans` needs. */
export async function fetchAgentSpans(
  range: ResolvedRange,
  traceIds: string[],
): Promise<Map<string, AgentTrace>> {
  if (traceIds.length === 0) return new Map();
  const res = await runIrQuery(
    tracesRows(
      range,
      [
        "trace_id",
        "span_id",
        "start_time_unix_nano",
        "duration",
        "span.name",
        ...AGENT_ATTRS,
      ],
      [
        { where: { field: "trace_id", op: "in", value: traceIds } },
        { where: { field: F.operation, op: "in", value: AGENT_OPS } },
        { limit: 100_000 },
      ],
    ),
  );
  const byTrace = new Map<string, AgentSpan[]>();
  for (const row of namedRows(res)) {
    const id = String(row.trace_id);
    let spans = byTrace.get(id);
    if (!spans) byTrace.set(id, (spans = []));
    spans.push({
      name: String(row.span_name ?? ""),
      startNs: nanos(row.start_time_unix_nano),
      durNs: nanos(row.duration_nanos ?? row.duration),
      attributes: Object.fromEntries(AGENT_ATTRS.map((a) => [a, row[col(a)]])),
    });
  }
  return new Map(
    [...byTrace].map(([id, spans]) => [id, agentTraceFromSpans(id, spans)]),
  );
}

/** The last text part of a GenAI messages attribute (`gen_ai.input.messages`
 * / `gen_ai.output.messages`, a JSON array of `{role, parts}`), or the raw
 * string when it isn't that shape. */
export function messageText(raw: unknown, role?: string): string | null {
  if (typeof raw !== "string" || raw === "") return null;
  try {
    const msgs = JSON.parse(raw) as {
      role?: string;
      parts?: { type?: string; content?: unknown }[];
    }[];
    if (!Array.isArray(msgs)) return raw;
    const texts = msgs
      .filter((m) => !role || m.role === role)
      .flatMap((m) => m.parts ?? [])
      .filter((p) => p.type === "text" && typeof p.content === "string")
      .map((p) => p.content as string);
    return texts.at(-1) ?? null;
  } catch {
    return raw;
  }
}

function agentTraceFromSpans(
  traceId: string,
  unsorted: AgentSpan[],
): AgentTrace {
  const spans = [...unsorted].sort((a, b) =>
    a.startNs < b.startNs ? -1 : a.startNs > b.startNs ? 1 : 0,
  );
  const op = (s: AgentSpan) => s.attributes[F.operation];
  const agent = spans.find((s) => op(s) === "invoke_agent");
  const chats = spans.filter((s) => op(s) === "chat");
  const usage = (s: AgentSpan) =>
    num(s.attributes[F.inputTokens]) + num(s.attributes[F.outputTokens]);
  const chatTokens = chats.reduce((acc, s) => acc + usage(s), 0);
  const tokens = chatTokens || (agent ? usage(agent) : 0);
  return {
    traceId,
    tools: spans
      .filter((s) => op(s) === "execute_tool")
      .map((s) => String(s.attributes[F.tool] ?? s.name)),
    durationMs: agent ? Number(agent.durNs / 1_000_000n) : null,
    llmCalls: chats.length,
    tokens: tokens || null,
    input: messageText(agent?.attributes[F.inputMessages], "user"),
    output: messageText(agent?.attributes[F.outputMessages]),
  };
}

function agentSpanOf(s: TempoSpan): AgentSpan {
  return {
    name: s.name,
    startNs: nanos(s.startNs),
    durNs: nanos(s.durNs),
    attributes: s.attributes,
  };
}

export function agentTraceOf(trace: TempoTrace): AgentTrace {
  return agentTraceFromSpans(trace.traceId, trace.spans.map(agentSpanOf));
}

// ---- single results ------------------------------------------------------

export interface EvalResult {
  runId: string | null;
  traceId: string | null;
  spanId: string | null;
  name: string;
  score: number | null;
  label: string | null;
  explanation: string | null;
  evaluator: string | null;
  error: string | null;
}

const RESULT_FIELDS = [
  F.runId,
  "trace_id",
  "span_id",
  F.name,
  F.score,
  F.label,
  F.explanation,
  F.evaluator,
  F.error,
];

/** Every result for one case in the given runs. */
export async function fetchCaseResults(
  range: ResolvedRange,
  runIds: string[],
  caseId: string,
): Promise<EvalResult[]> {
  const res = await runIrQuery(
    irDoc(
      "logs",
      "rows",
      range,
      [...resultsWhere({ runIds, caseId }), { limit: 1_000 }],
      { fields: RESULT_FIELDS },
    ),
  );
  return namedRows(res).flatMap((row): EvalResult[] => {
    const name = str(row[col(F.name)]);
    if (!name) return [];
    const score = row[col(F.score)];
    return [
      {
        runId: str(row[col(F.runId)]),
        traceId: str(row.trace_id),
        spanId: str(row.span_id),
        name,
        score: typeof score === "number" ? score : null,
        label: str(row[col(F.label)]),
        explanation: str(row[col(F.explanation)]),
        evaluator: str(row[col(F.evaluator)]),
        error: str(row[col(F.error)]),
      },
    ];
  });
}

// ---- evaluators ----------------------------------------------------------

export interface EvaluatorInfo {
  name: string;
  versions: string[];
  firstMs: number;
  lastMs: number;
  results: number;
  offline: number;
  scored: number;
  labelled: number;
  /** `gen_ai.operation.name` of a span this evaluator scored. */
  operation: string | null;
}

export async function fetchEvaluators(
  range: ResolvedRange,
): Promise<EvaluatorInfo[]> {
  const res = await runIrQuery(
    logsTable(
      range,
      [
        ...resultsWhere(),
        {
          aggregate: {
            by: [F.name, F.evaluator],
            aggs: [
              COUNT_N,
              FIRST_SEEN,
              LAST_SEEN,
              { fn: "count", as: "offline", where: exists(F.runId) },
              { fn: "count", as: "scored", where: exists(F.score) },
              { fn: "count", as: "labelled", where: exists(F.label) },
              { fn: "last", of: "span_id", as: "span" },
            ],
          },
        },
        { limit: 5_000 },
      ],
      5,
    ),
  );
  const byName = new Map<string, EvaluatorInfo & { span: string | null }>();
  for (const row of namedRows(res)) {
    const name = str(row[col(F.name)]);
    if (!name) continue;
    const prev = byName.get(name);
    const version = str(row[col(F.evaluator)]);
    byName.set(name, {
      name,
      versions: [...(prev?.versions ?? []), ...(version ? [version] : [])],
      ...widen(prev, row),
      results: (prev?.results ?? 0) + num(row.n),
      offline: (prev?.offline ?? 0) + num(row.offline),
      scored: (prev?.scored ?? 0) + num(row.scored),
      labelled: (prev?.labelled ?? 0) + num(row.labelled),
      operation: null,
      span: prev?.span ?? str(row.span),
    });
  }
  const spans = [...byName.values()].flatMap((e) => (e.span ? [e.span] : []));
  const ops = spans.length
    ? await fetchSpanOperations(range, spans)
    : new Map<string, string>();
  return [...byName.values()]
    .map(({ span, ...e }) => ({
      ...e,
      versions: [...e.versions].sort().reverse(),
      operation: (span && ops.get(span)) ?? null,
    }))
    .sort((a, b) => a.firstMs - b.firstMs);
}

async function fetchSpanOperations(
  range: ResolvedRange,
  spanIds: string[],
): Promise<Map<string, string>> {
  const res = await runIrQuery(
    tracesRows(
      range,
      ["span_id", F.operation],
      [
        { where: { field: "span_id", op: "in", value: spanIds } },
        { limit: spanIds.length },
      ],
    ),
  );
  const out = new Map<string, string>();
  for (const row of namedRows(res)) {
    const span = str(row.span_id);
    const op = str(row[col(F.operation)]);
    if (span && op) out.set(span, op);
  }
  return out;
}
