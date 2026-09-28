//! # Reading eval runs and comparisons through the Query IR
//!
//! The Query IR documents behind the Runs and Compare pages (the UI's
//! `api/evals.ts`), their decoding, and the two reads the CLI
//! (`evals runs|compare`) and the MCP tools (`list_eval_runs`,
//! `compare_eval_runs`) share: every figure comes from `POST /api/v1/query`
//! (design goal "one read model"), through an [`IrSource`] each client
//! implements over its own HTTP client.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;

use chrono::{DateTime, SecondsFormat};
pub use query_ir::Document;
use query_ir::{
    Agg, AggFn, Aggregate, ComparisonOp, Leaf, Predicate, Range, ResultEnvelope, Stage,
};
use serde::Serialize;
use serde_json::{Value, json};

use super::compare::{
    CaseKind, CaseRow, CaseScores, DeltaUnit, Direction, EvalStats, ResultGroup, RunStatusKind,
    StatsDelta, ToolStep, compare_cases, run_status, stats_delta, summarize_evaluators, tool_diff,
};
use super::{
    AGENT_NAME, AGENT_VERSION, CASE_ID, ERROR_TYPE, EVALUATION_NAME, EVALUATION_RESULT_EVENT,
    EVALUATION_SCORE_LABEL, EVALUATION_SCORE_VALUE, OPERATION_EXECUTE_TOOL, OPERATION_NAME,
    PASS_THRESHOLD, RUN_ID, SET, TOOL_NAME, Verdict,
};

/// Row cap of the per-run and per-case stats queries (as the UI's).
pub const STATS_ROW_LIMIT: u64 = 50_000;
/// Row cap of the (run, case) count query.
pub const CASE_ROW_LIMIT: u64 = 100_000;
/// Row cap of the per-case trace query.
pub const TRACE_ROW_LIMIT: u64 = 20_000;
/// Row cap of the tool span query.
pub const TOOL_SPAN_ROW_LIMIT: u64 = 100_000;
/// How far back `latest:<version>` looks by default.
pub const LATEST_LOOKBACK: &str = "now-30d";
/// Prefix of a run reference naming the newest run of a version.
pub const LATEST_PREFIX: &str = "latest:";

const SERVICE_NAME: &str = "service.name";
const SERVICE_VERSION: &str = "service.version";
const TRACE_ID: &str = "trace_id";

/// A Query IR `table` or `rows` result: column names and rows of cells.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct IrTable {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

impl IrTable {
    /// The index of the column a logical field comes back under: its dots
    /// become underscores (`gen_ai.agent.name` -> `gen_ai_agent_name`); an
    /// aggregate's column is its `as`.
    fn col(&self, field: &str) -> Option<usize> {
        let underscored = field.replace('.', "_");
        self.columns
            .iter()
            .position(|c| *c == underscored)
            .or_else(|| self.columns.iter().position(|c| c == field))
    }
}

/// Runs a Query IR document against the caller's tenant and dataset.
pub trait IrSource {
    type Error;

    fn query(
        &self,
        document: &Document,
    ) -> impl Future<Output = Result<IrTable, Self::Error>> + Send;
}

/// Why a run read failed.
#[derive(Debug, thiserror::Error)]
pub enum ReadError<E> {
    /// The Query IR request itself failed.
    #[error("{0}")]
    Query(E),
    #[error("run `{run_id}` has no results between {from} and {to}")]
    UnknownRun {
        run_id: String,
        from: String,
        to: String,
    },
    #[error(
        "no run of agent `{agent}` version `{version}` on eval set `{set}` between {from} and {to}"
    )]
    NoRunForVersion {
        agent: String,
        version: String,
        set: String,
        from: String,
        to: String,
    },
    /// The request can't be answered as given.
    #[error("{0}")]
    Invalid(String),
}

/// The IR `range` of a read: RFC3339, relative (`now-7d`) or epoch-nanosecond
/// strings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Window {
    pub from: String,
    pub to: String,
}

/// Which offline runs a read covers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunFilter {
    /// `gen_ai.agent.name`, or `service.name` when a result doesn't name
    /// its agent.
    pub agent: Option<String>,
    /// `gen_ai.agent.version`, or `service.version` likewise.
    pub version: Option<String>,
    pub set: Option<String>,
    pub run_ids: Vec<String>,
}

// ---- documents -------------------------------------------------------------

fn leaf(field: &str, op: ComparisonOp, value: Option<Value>) -> Predicate {
    Predicate::Leaf(Leaf {
        field: field.to_string(),
        op,
        value,
    })
}

fn eq(field: &str, value: &str) -> Predicate {
    leaf(field, ComparisonOp::Eq, Some(json!(value)))
}

fn ne(field: &str, value: &str) -> Predicate {
    leaf(field, ComparisonOp::Ne, Some(json!(value)))
}

fn exists(field: &str) -> Predicate {
    leaf(field, ComparisonOp::Exists, None)
}

fn in_list(field: &str, values: &[String]) -> Predicate {
    leaf(field, ComparisonOp::In, Some(json!(values)))
}

fn absent(field: &str) -> Predicate {
    Predicate::Not(Box::new(exists(field)))
}

/// Log records without trace context store an empty trace id.
fn linked() -> Predicate {
    Predicate::And(vec![exists(TRACE_ID), ne(TRACE_ID, "")])
}

/// `field = value`, or `fallback = value` on results without `field`.
fn with_fallback(field: &str, fallback: &str, value: &str) -> Predicate {
    Predicate::Or(vec![
        eq(field, value),
        Predicate::And(vec![absent(field), eq(fallback, value)]),
    ])
}

/// The `where` stages selecting offline results under `filter`.
fn offline_results(filter: &RunFilter) -> Vec<Stage> {
    let mut preds = vec![eq("event_name", EVALUATION_RESULT_EVENT), exists(RUN_ID)];
    if let Some(agent) = &filter.agent {
        preds.push(with_fallback(AGENT_NAME, SERVICE_NAME, agent));
    }
    if let Some(version) = &filter.version {
        preds.push(with_fallback(AGENT_VERSION, SERVICE_VERSION, version));
    }
    if let Some(set) = &filter.set {
        preds.push(eq(SET, set));
    }
    if !filter.run_ids.is_empty() {
        preds.push(in_list(RUN_ID, &filter.run_ids));
    }
    preds.into_iter().map(Stage::Where).collect()
}

/// A document over `window` declaring the lowest `irVersion` it needs.
fn document(
    from: &str,
    window: &Window,
    result: ResultEnvelope,
    fields: Option<Vec<String>>,
    pipeline: Vec<Stage>,
) -> Document {
    let mut doc = Document {
        ir_version: 1,
        from: from.to_string(),
        range: Range {
            from: json!(window.from),
            to: json!(window.to),
        },
        result,
        fields,
        pipeline,
        focus: None,
        depth: None,
        trace_id: None,
    };
    doc.ir_version = doc.minimum_ir_version();
    doc
}

fn logs_table(window: &Window, pipeline: Vec<Stage>) -> Document {
    document("logs", window, ResultEnvelope::Table, None, pipeline)
}

fn agg(func: AggFn, of: Option<&str>, as_name: &str, scope: Option<Predicate>) -> Agg {
    Agg {
        func,
        of: of.map(str::to_string),
        arg: None,
        divisor: None,
        as_name: as_name.to_string(),
        scope,
        across: None,
        window: None,
    }
}

fn aggregate(by: &[&str], aggs: Vec<Agg>) -> Stage {
    Stage::Aggregate(Aggregate {
        by: by.iter().map(|f| f.to_string()).collect(),
        aggs,
        step: None,
    })
}

/// The counts the pass rule needs per group (see [`EvalStats::fold`]).
fn stat_aggs() -> Vec<Agg> {
    let score = |op| {
        Some(leaf(
            EVALUATION_SCORE_VALUE,
            op,
            Some(json!(PASS_THRESHOLD)),
        ))
    };
    vec![
        agg(AggFn::Count, None, "n", None),
        agg(AggFn::Count, None, "high", score(ComparisonOp::Gte)),
        agg(AggFn::Count, None, "low", score(ComparisonOp::Lt)),
        agg(AggFn::Sum, Some(EVALUATION_SCORE_VALUE), "score_sum", None),
        agg(
            AggFn::Count,
            None,
            "scored",
            Some(exists(EVALUATION_SCORE_VALUE)),
        ),
    ]
}

const RUN_BY: [&str; 6] = [
    RUN_ID,
    SET,
    AGENT_NAME,
    SERVICE_NAME,
    AGENT_VERSION,
    SERVICE_VERSION,
];

/// Offline runs grouped by run identity, evaluator, label and error: one
/// query carries each run's span, its pass-rule stats per evaluator and its
/// results without trace context. The UI's `buildRunsDoc` plus the
/// evaluator name.
pub fn runs_document(window: &Window, filter: &RunFilter) -> Document {
    let mut by: Vec<&str> = RUN_BY.to_vec();
    by.extend([EVALUATION_NAME, EVALUATION_SCORE_LABEL, ERROR_TYPE]);
    let mut aggs = stat_aggs();
    aggs.extend([
        agg(AggFn::Min, Some("timestamp"), "first", None),
        agg(AggFn::Max, Some("timestamp"), "last", None),
        agg(
            AggFn::Count,
            None,
            "unlinked",
            Some(Predicate::Not(Box::new(linked()))),
        ),
    ]);
    let mut pipeline = offline_results(filter);
    pipeline.push(aggregate(&by, aggs));
    pipeline.push(Stage::Limit(STATS_ROW_LIMIT));
    logs_table(window, pipeline)
}

/// One row per (run, case): the IR has no distinct count, so cases are
/// counted from the groups.
pub fn run_cases_document(window: &Window, filter: &RunFilter) -> Document {
    let mut pipeline = offline_results(filter);
    pipeline.push(aggregate(
        &[RUN_ID, CASE_ID],
        vec![agg(AggFn::Count, None, "n", None)],
    ));
    pipeline.push(Stage::Limit(CASE_ROW_LIMIT));
    logs_table(window, pipeline)
}

/// Per run, case and evaluator stats of the given runs.
pub fn case_stats_document(window: &Window, run_ids: &[String]) -> Document {
    let filter = RunFilter {
        run_ids: run_ids.to_vec(),
        ..RunFilter::default()
    };
    let mut pipeline = offline_results(&filter);
    pipeline.push(aggregate(
        &[
            RUN_ID,
            CASE_ID,
            EVALUATION_NAME,
            EVALUATION_SCORE_LABEL,
            ERROR_TYPE,
        ],
        stat_aggs(),
    ));
    pipeline.push(Stage::Limit(STATS_ROW_LIMIT));
    logs_table(window, pipeline)
}

/// The trace each run's agent produced for each of `case_ids` (the first
/// linked result's).
pub fn case_traces_document(window: &Window, run_ids: &[String], case_ids: &[String]) -> Document {
    let filter = RunFilter {
        run_ids: run_ids.to_vec(),
        ..RunFilter::default()
    };
    let mut pipeline = offline_results(&filter);
    pipeline.push(Stage::Where(in_list(CASE_ID, case_ids)));
    pipeline.push(Stage::Where(linked()));
    pipeline.push(aggregate(
        &[RUN_ID, CASE_ID],
        vec![agg(AggFn::First, Some(TRACE_ID), "trace", None)],
    ));
    pipeline.push(Stage::Limit(TRACE_ROW_LIMIT));
    logs_table(window, pipeline)
}

/// The `execute_tool` spans of the given traces.
pub fn tool_spans_document(window: &Window, trace_ids: &[String]) -> Document {
    document(
        "traces",
        window,
        ResultEnvelope::Rows,
        Some(
            [
                TRACE_ID,
                "start_time_unix_nano",
                "span.name",
                OPERATION_NAME,
                TOOL_NAME,
            ]
            .map(str::to_string)
            .to_vec(),
        ),
        vec![
            Stage::Where(in_list(TRACE_ID, trace_ids)),
            Stage::Where(eq(OPERATION_NAME, OPERATION_EXECUTE_TOOL)),
            Stage::Limit(TOOL_SPAN_ROW_LIMIT),
        ],
    )
}

/// The last result time of every run of `agent` at `version` on `set`,
/// other than `exclude_run`: what `latest:<version>` resolves through.
pub fn latest_run_document(
    agent: &str,
    version: &str,
    set: &str,
    exclude_run: Option<&str>,
    window: &Window,
) -> Document {
    let mut pipeline: Vec<Stage> = [
        eq("event_name", EVALUATION_RESULT_EVENT),
        with_fallback(AGENT_NAME, SERVICE_NAME, agent),
        with_fallback(AGENT_VERSION, SERVICE_VERSION, version),
        eq(SET, set),
    ]
    .into_iter()
    .chain(exclude_run.map(|run| ne(RUN_ID, run)))
    .map(Stage::Where)
    .collect();
    pipeline.push(aggregate(
        &[RUN_ID],
        vec![agg(AggFn::Max, Some("timestamp"), "last", None)],
    ));
    logs_table(window, pipeline)
}

// ---- decoding --------------------------------------------------------------

fn text(row: &[Value], col: Option<usize>) -> Option<&str> {
    col.and_then(|i| row.get(i))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

fn number(row: &[Value], col: Option<usize>) -> f64 {
    match col.and_then(|i| row.get(i)) {
        Some(Value::Number(n)) => n.as_f64().unwrap_or(0.0),
        Some(Value::String(s)) => s.parse().unwrap_or(0.0),
        _ => 0.0,
    }
}

fn count(row: &[Value], col: Option<usize>) -> u64 {
    match col.and_then(|i| row.get(i)) {
        Some(Value::Number(n)) => n
            .as_u64()
            .or_else(|| n.as_f64().map(|f| f.max(0.0) as u64))
            .unwrap_or(0),
        Some(Value::String(s)) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

/// A nanosecond timestamp cell (a JSON integer, float or numeric string).
fn nanos(row: &[Value], col: Option<usize>) -> Option<i64> {
    match col.and_then(|i| row.get(i))? {
        Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// The stats columns of one result table.
struct StatCols {
    label: Option<usize>,
    error: Option<usize>,
    n: Option<usize>,
    high: Option<usize>,
    low: Option<usize>,
    score_sum: Option<usize>,
    scored: Option<usize>,
}

impl StatCols {
    fn of(table: &IrTable) -> Self {
        Self {
            label: table.col(EVALUATION_SCORE_LABEL),
            error: table.col(ERROR_TYPE),
            n: table.col("n"),
            high: table.col("high"),
            low: table.col("low"),
            score_sum: table.col("score_sum"),
            scored: table.col("scored"),
        }
    }

    fn group<'a>(&self, row: &'a [Value]) -> ResultGroup<'a> {
        ResultGroup {
            label: text(row, self.label),
            error: text(row, self.error),
            n: count(row, self.n),
            high: count(row, self.high),
            low: count(row, self.low),
            score_sum: number(row, self.score_sum),
            scored: count(row, self.scored),
        }
    }
}

/// One offline run: every result sharing a `signaldb.eval.run_id`.
#[derive(Debug, Clone, PartialEq)]
pub struct EvalRun {
    pub id: String,
    pub set: Option<String>,
    pub agent: Option<String>,
    pub version: Option<String>,
    pub first_ns: i64,
    pub last_ns: i64,
    /// Results without trace context: they score the run, not a span.
    pub unlinked: u64,
    pub cases: u64,
    /// Over every evaluator.
    pub stats: EvalStats,
    pub evaluators: BTreeMap<String, EvalStats>,
}

/// Runs decoded from a [`runs_document`] and a [`run_cases_document`]
/// result, newest first.
pub fn decode_runs(runs: &IrTable, cases: &IrTable) -> Vec<EvalRun> {
    let mut case_counts: HashMap<&str, u64> = HashMap::new();
    let (run_col, case_col) = (cases.col(RUN_ID), cases.col(CASE_ID));
    for row in &cases.rows {
        if let (Some(run), Some(_)) = (text(row, run_col), text(row, case_col)) {
            *case_counts.entry(run).or_default() += 1;
        }
    }
    let stat_cols = StatCols::of(runs);
    let run_col = runs.col(RUN_ID);
    let set_col = runs.col(SET);
    let agent_cols = (runs.col(AGENT_NAME), runs.col(SERVICE_NAME));
    let version_cols = (runs.col(AGENT_VERSION), runs.col(SERVICE_VERSION));
    let name_col = runs.col(EVALUATION_NAME);
    let (first_col, last_col, unlinked_col) =
        (runs.col("first"), runs.col("last"), runs.col("unlinked"));
    let mut by_id: BTreeMap<String, EvalRun> = BTreeMap::new();
    for row in &runs.rows {
        let Some(id) = text(row, run_col) else {
            continue;
        };
        let first = nanos(row, first_col).unwrap_or(0);
        let last = nanos(row, last_col).unwrap_or(0);
        let run = by_id.entry(id.to_string()).or_insert_with(|| EvalRun {
            id: id.to_string(),
            set: None,
            agent: None,
            version: None,
            first_ns: first,
            last_ns: last,
            unlinked: 0,
            cases: case_counts.get(id).copied().unwrap_or(0),
            stats: EvalStats::default(),
            evaluators: BTreeMap::new(),
        });
        run.set = run
            .set
            .take()
            .or_else(|| text(row, set_col).map(str::to_string));
        run.agent = run.agent.take().or_else(|| {
            text(row, agent_cols.0)
                .or_else(|| text(row, agent_cols.1))
                .map(str::to_string)
        });
        run.version = run.version.take().or_else(|| {
            text(row, version_cols.0)
                .or_else(|| text(row, version_cols.1))
                .map(str::to_string)
        });
        run.first_ns = run.first_ns.min(first);
        run.last_ns = run.last_ns.max(last);
        run.unlinked += count(row, unlinked_col);
        let group = stat_cols.group(row);
        run.stats.fold(&group);
        if let Some(name) = text(row, name_col) {
            run.evaluators
                .entry(name.to_string())
                .or_default()
                .fold(&group);
        }
    }
    let mut runs: Vec<EvalRun> = by_id.into_values().collect();
    runs.sort_by(|a, b| b.first_ns.cmp(&a.first_ns).then_with(|| a.id.cmp(&b.id)));
    runs
}

/// The run each run compares against: the newest earlier run of the same
/// eval set, keyed by run id. Runs sharing a start time aren't each other's
/// baseline.
pub fn baselines_of(runs: &[EvalRun]) -> HashMap<String, String> {
    let mut by_set: HashMap<Option<&str>, Vec<&EvalRun>> = HashMap::new();
    for run in runs {
        by_set.entry(run.set.as_deref()).or_default().push(run);
    }
    let mut out = HashMap::new();
    for group in by_set.values_mut() {
        group.sort_by_key(|r| std::cmp::Reverse(r.first_ns));
        let mut j = 0;
        for run in group.iter() {
            while j < group.len() && group[j].first_ns >= run.first_ns {
                j += 1;
            }
            if let Some(earlier) = group.get(j) {
                out.insert(run.id.clone(), earlier.id.clone());
            }
        }
    }
    out
}

/// The run id with the latest `last` in a [`latest_run_document`] result.
pub fn newest_run(table: &IrTable) -> Option<String> {
    let run_col = table.col(RUN_ID);
    let last_col = table.col("last");
    table
        .rows
        .iter()
        .filter_map(|row| {
            let run = text(row, run_col)?;
            Some((number(row, last_col), run))
        })
        .max_by(|a, b| a.0.total_cmp(&b.0))
        .map(|(_, run)| run.to_string())
}

/// Per run: case id -> evaluator -> stats, from a [`case_stats_document`]
/// result.
pub fn decode_case_stats(table: &IrTable) -> HashMap<String, BTreeMap<String, CaseScores>> {
    let cols = StatCols::of(table);
    let (run_col, case_col, name_col) = (
        table.col(RUN_ID),
        table.col(CASE_ID),
        table.col(EVALUATION_NAME),
    );
    let mut out: HashMap<String, BTreeMap<String, CaseScores>> = HashMap::new();
    for row in &table.rows {
        let (Some(run), Some(case), Some(name)) =
            (text(row, run_col), text(row, case_col), text(row, name_col))
        else {
            continue;
        };
        out.entry(run.to_string())
            .or_default()
            .entry(case.to_string())
            .or_default()
            .entry(name.to_string())
            .or_default()
            .fold(&cols.group(row));
    }
    out
}

/// Per run: case id -> trace id, from a [`case_traces_document`] result.
pub fn decode_case_traces(table: &IrTable) -> HashMap<String, BTreeMap<String, String>> {
    let (run_col, case_col, trace_col) =
        (table.col(RUN_ID), table.col(CASE_ID), table.col("trace"));
    let mut out: HashMap<String, BTreeMap<String, String>> = HashMap::new();
    for row in &table.rows {
        if let (Some(run), Some(case), Some(trace)) = (
            text(row, run_col),
            text(row, case_col),
            text(row, trace_col),
        ) {
            out.entry(run.to_string())
                .or_default()
                .insert(case.to_string(), trace.to_string());
        }
    }
    out
}

/// Trace id -> `execute_tool` names in call order, from a
/// [`tool_spans_document`] result. A span without `gen_ai.tool.name` counts
/// under its span name.
pub fn decode_tool_calls(table: &IrTable) -> HashMap<String, Vec<String>> {
    let trace_col = table.col("trace_id");
    let start_col = table.col("start_time_unix_nano");
    let tool_col = table.col(TOOL_NAME);
    let span_name_col = table.col("span.name");
    let mut spans: HashMap<String, Vec<(i64, String)>> = HashMap::new();
    for row in &table.rows {
        let Some(trace) = text(row, trace_col) else {
            continue;
        };
        let name = text(row, tool_col)
            .or_else(|| text(row, span_name_col))
            .unwrap_or_default();
        spans
            .entry(trace.to_string())
            .or_default()
            .push((nanos(row, start_col).unwrap_or(0), name.to_string()));
    }
    spans
        .into_iter()
        .map(|(trace, mut calls)| {
            calls.sort_by_key(|(start, _)| *start);
            (trace, calls.into_iter().map(|(_, name)| name).collect())
        })
        .collect()
}

// ---- reports ---------------------------------------------------------------

/// The wall clock in epoch milliseconds, for [`run_status`].
pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// RFC3339 (milliseconds, UTC) of an epoch-nanosecond timestamp.
fn rfc3339(ns: i64) -> String {
    DateTime::from_timestamp_nanos(ns).to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// One evaluator's figures in a run.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EvaluatorFigures {
    pub results: u64,
    pub errors: u64,
    /// Mean score, evaluator errors left out.
    pub mean: Option<f64>,
    /// Passes / (passes + fails); `None` when no result has a verdict.
    pub pass_rate: Option<f64>,
}

impl From<&EvalStats> for EvaluatorFigures {
    fn from(s: &EvalStats) -> Self {
        Self {
            results: s.results,
            errors: s.errors,
            mean: s.mean(),
            pass_rate: s.pass_rate(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct NamedFigures {
    pub name: String,
    #[serde(flatten)]
    pub figures: EvaluatorFigures,
}

/// One run as the CLI and MCP tools report it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RunSummary {
    pub run_id: String,
    pub set: Option<String>,
    pub agent: Option<String>,
    pub version: Option<String>,
    /// The run's first result.
    pub started_at: String,
    pub last_result_at: String,
    pub status: RunStatusKind,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub status_reasons: Vec<String>,
    pub results: u64,
    pub cases: u64,
    /// Evaluator errors: left out of means and pass rates.
    pub errors: u64,
    /// Results without trace context.
    pub unlinked: u64,
    /// Pass rate over every evaluator.
    pub pass_rate: Option<f64>,
    pub evaluators: Vec<NamedFigures>,
    /// The newest earlier run of the same eval set in the window: the
    /// natural baseline to compare this run against.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_run_id: Option<String>,
}

impl RunSummary {
    pub fn new(run: &EvalRun, previous_run_id: Option<String>, now_ms: i64) -> Self {
        let status = run_status(
            run.last_ns / 1_000_000,
            run.stats.errors,
            run.unlinked,
            now_ms,
        );
        Self {
            run_id: run.id.clone(),
            set: run.set.clone(),
            agent: run.agent.clone(),
            version: run.version.clone(),
            started_at: rfc3339(run.first_ns),
            last_result_at: rfc3339(run.last_ns),
            status: status.kind,
            status_reasons: status.reasons,
            results: run.stats.results,
            cases: run.cases,
            errors: run.stats.errors,
            unlinked: run.unlinked,
            pass_rate: run.stats.pass_rate(),
            evaluators: run
                .evaluators
                .iter()
                .map(|(name, s)| NamedFigures {
                    name: name.clone(),
                    figures: s.into(),
                })
                .collect(),
            previous_run_id,
        }
    }
}

/// What [`list_runs`] returns.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RunList {
    pub window: Window,
    /// Runs in the window matching the filter.
    pub total_runs: usize,
    pub returned: usize,
    /// More runs matched than `returned`, or the stats hit a row cap.
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// Newest first.
    pub runs: Vec<RunSummary>,
}

async fn fetch_runs<S: IrSource>(
    source: &S,
    window: &Window,
    filter: &RunFilter,
) -> Result<(Vec<EvalRun>, bool), ReadError<S::Error>> {
    let (runs_doc, cases_doc) = (
        runs_document(window, filter),
        run_cases_document(window, filter),
    );
    let (runs, cases) = futures::try_join!(source.query(&runs_doc), source.query(&cases_doc))
        .map_err(ReadError::Query)?;
    let capped = reached(&runs, STATS_ROW_LIMIT) || reached(&cases, CASE_ROW_LIMIT);
    Ok((decode_runs(&runs, &cases), capped))
}

/// Whether a result came back with as many rows as its query's cap.
fn reached(table: &IrTable, cap: u64) -> bool {
    table.rows.len() as u64 >= cap
}

/// Offline runs in the window, newest first, at most `limit` of them.
pub async fn list_runs<S: IrSource>(
    source: &S,
    window: &Window,
    filter: &RunFilter,
    limit: usize,
    now_ms: i64,
) -> Result<RunList, ReadError<S::Error>> {
    let (runs, capped) = fetch_runs(source, window, filter).await?;
    let previous = baselines_of(&runs);
    let total_runs = runs.len();
    let summaries: Vec<RunSummary> = runs
        .iter()
        .take(limit)
        .map(|run| RunSummary::new(run, previous.get(&run.id).cloned(), now_ms))
        .collect();
    let returned = summaries.len();
    let mut notes = Vec::new();
    if returned < total_runs {
        notes.push(format!(
            "showing the newest {returned} of {total_runs} runs; narrow the window or filter by agent, version or eval set"
        ));
    }
    if capped {
        notes.push(
            "the stats query hit its row cap, so older runs or figures may be incomplete; narrow the window"
                .to_string(),
        );
    }
    Ok(RunList {
        window: window.clone(),
        total_runs,
        returned,
        truncated: returned < total_runs || capped,
        note: (!notes.is_empty()).then(|| notes.join("; ")),
        runs: summaries,
    })
}

/// A run named by id, or `latest:<version>`: the newest run of that version
/// of the same agent on the same eval set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunRef {
    Id(String),
    Latest(String),
}

impl RunRef {
    pub fn parse(reference: &str) -> Result<Self, String> {
        let reference = reference.trim();
        match reference.strip_prefix(LATEST_PREFIX) {
            Some(version) if version.trim().is_empty() => Err(format!(
                "`{reference}` names no version (`latest:<version>`)"
            )),
            Some(version) => Ok(Self::Latest(version.trim().to_string())),
            None if reference.is_empty() => Err("the run id is empty".to_string()),
            None => Ok(Self::Id(reference.to_string())),
        }
    }

    /// [`RunRef::parse`] with the error naming which side (`baseline`,
    /// `candidate`) the reference was given for.
    pub fn parse_named(which: &str, reference: &str) -> Result<Self, String> {
        Self::parse(reference).map_err(|e| format!("`{which}`: {e}"))
    }
}

/// What [`compare_runs`] compares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompareRequest {
    pub baseline: RunRef,
    pub candidate: RunRef,
    /// Agent and eval set for resolving `latest:<version>`; default to the
    /// other side's run.
    pub agent: Option<String>,
    pub set: Option<String>,
    pub window: Window,
    /// Most regressed cases to list.
    pub limit: usize,
    /// Add each listed regression's tool-call diff.
    pub include_tools: bool,
}

/// One evaluator across the two runs.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EvaluatorComparison {
    pub name: String,
    pub baseline: EvaluatorFigures,
    pub candidate: EvaluatorFigures,
    /// The mean's change when both runs have one, else the pass rate's.
    pub delta: Option<StatsDelta>,
    /// Cases this evaluator scored worse / better.
    pub worse: u64,
    pub better: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CaseCounts {
    pub regressions: usize,
    pub improvements: usize,
    pub unchanged: usize,
    /// Regressions among cases only the candidate ran.
    pub no_baseline: usize,
    /// Baseline cases the candidate has no results for (not classified).
    pub baseline_only: usize,
}

/// One evaluator's cell of one case.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CaseCell {
    pub mean: Option<f64>,
    pub pass_rate: Option<f64>,
    pub verdict: Option<&'static str>,
    pub errors: u64,
}

impl From<&EvalStats> for CaseCell {
    fn from(s: &EvalStats) -> Self {
        Self {
            mean: s.mean(),
            pass_rate: s.pass_rate(),
            verdict: s.verdict().map(|v| match v {
                Verdict::Pass => "pass",
                Verdict::Fail => "fail",
            }),
            errors: s.errors,
        }
    }
}

/// How one evaluator moved on a regressed case.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EvaluatorChange {
    pub name: String,
    /// `worse` or `better`; `failing` on a case with no baseline.
    pub change: &'static str,
    pub baseline: Option<CaseCell>,
    pub candidate: CaseCell,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RegressedCase {
    pub case_id: String,
    /// Only the candidate ran this case, and an evaluator fails it.
    pub no_baseline: bool,
    /// Sum of mean-score changes across evaluators; most negative first.
    pub delta: f64,
    /// Evaluators that got worse (or fail, without a baseline), then the
    /// ones that got better.
    pub evaluators: Vec<EvaluatorChange>,
    pub baseline_trace_id: Option<String>,
    pub candidate_trace_id: Option<String>,
    /// The candidate's tool calls marked against the baseline's.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<ToolStep>>,
}

/// What [`compare_runs`] returns.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Comparison {
    pub baseline: RunSummary,
    pub candidate: RunSummary,
    pub evaluators: Vec<EvaluatorComparison>,
    pub counts: CaseCounts,
    /// Largest drop first, at most `limit`.
    pub regressions: Vec<RegressedCase>,
    /// More regressions exist than listed.
    pub regressions_truncated: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

fn change_label(direction: Direction) -> Option<&'static str> {
    match direction {
        Direction::Worse => Some("worse"),
        Direction::Better => Some("better"),
        Direction::Same => None,
    }
}

async fn resolve_latest<S: IrSource>(
    source: &S,
    version: &str,
    agent: Option<&str>,
    set: Option<&str>,
    exclude: Option<&str>,
    window: &Window,
) -> Result<String, ReadError<S::Error>> {
    let (Some(agent), Some(set)) = (agent, set) else {
        return Err(ReadError::Invalid(format!(
            "`latest:{version}` needs the agent and eval set: give a run id for the other side, or `agent` and `set`"
        )));
    };
    let table = source
        .query(&latest_run_document(agent, version, set, exclude, window))
        .await
        .map_err(ReadError::Query)?;
    newest_run(&table).ok_or_else(|| ReadError::NoRunForVersion {
        agent: agent.to_string(),
        version: version.to_string(),
        set: set.to_string(),
        from: window.from.clone(),
        to: window.to.clone(),
    })
}

/// The agent and eval set a `latest:` side resolves within: the request's,
/// else those of the other side's run.
fn latest_context<'a>(
    request: &'a CompareRequest,
    runs: &'a HashMap<String, EvalRun>,
    other: &RunRef,
) -> (Option<&'a str>, Option<&'a str>) {
    let other = match other {
        RunRef::Id(id) => runs.get(id),
        RunRef::Latest(_) => None,
    };
    (
        request
            .agent
            .as_deref()
            .or_else(|| other.and_then(|r| r.agent.as_deref())),
        request
            .set
            .as_deref()
            .or_else(|| other.and_then(|r| r.set.as_deref())),
    )
}

async fn fetch_named_runs<S: IrSource>(
    source: &S,
    window: &Window,
    ids: &[String],
) -> Result<HashMap<String, EvalRun>, ReadError<S::Error>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let filter = RunFilter {
        run_ids: ids.to_vec(),
        ..RunFilter::default()
    };
    let (runs, _) = fetch_runs(source, window, &filter).await?;
    Ok(runs.into_iter().map(|r| (r.id.clone(), r)).collect())
}

/// A regressed case without its trace ids: the evaluators that got worse
/// (or fail, without a baseline), then the ones that got better.
fn regressed_case(
    row: &CaseRow,
    base: Option<&CaseScores>,
    cand: Option<&CaseScores>,
) -> RegressedCase {
    let cell = |scores: Option<&CaseScores>, name: &str| {
        scores.and_then(|s| s.get(name)).map(CaseCell::from)
    };
    let mut changes: Vec<EvaluatorChange> = if row.comparison.no_baseline {
        cand.into_iter()
            .flatten()
            .filter(|(_, s)| s.verdict() == Some(Verdict::Fail))
            .map(|(name, s)| EvaluatorChange {
                name: name.clone(),
                change: "failing",
                baseline: None,
                candidate: s.into(),
            })
            .collect()
    } else {
        row.comparison
            .evaluators
            .iter()
            .filter_map(|(name, dir)| {
                Some(EvaluatorChange {
                    name: name.clone(),
                    change: change_label(*dir)?,
                    baseline: cell(base, name),
                    candidate: cell(cand, name)?,
                })
            })
            .collect()
    };
    changes.sort_by_key(|c| c.change == "better");
    RegressedCase {
        case_id: row.case_id.clone(),
        no_baseline: row.comparison.no_baseline,
        delta: row.comparison.delta,
        evaluators: changes,
        baseline_trace_id: None,
        candidate_trace_id: None,
        tools: None,
    }
}

/// Compares a candidate run with a baseline run case by case (design D5).
pub async fn compare_runs<S: IrSource>(
    source: &S,
    request: &CompareRequest,
    now_ms: i64,
) -> Result<Comparison, ReadError<S::Error>> {
    let window = &request.window;
    let concrete: Vec<String> = [&request.baseline, &request.candidate]
        .into_iter()
        .filter_map(|r| match r {
            RunRef::Id(id) => Some(id.clone()),
            RunRef::Latest(_) => None,
        })
        .collect();
    let mut runs = fetch_named_runs(source, window, &concrete).await?;
    for id in &concrete {
        if !runs.contains_key(id) {
            return Err(ReadError::UnknownRun {
                run_id: id.clone(),
                from: window.from.clone(),
                to: window.to.clone(),
            });
        }
    }

    let candidate_known = match &request.candidate {
        RunRef::Id(id) => Some(id.as_str()),
        RunRef::Latest(_) => None,
    };
    let baseline_id = match &request.baseline {
        RunRef::Id(id) => id.clone(),
        RunRef::Latest(version) => {
            let (agent, set) = latest_context(request, &runs, &request.candidate);
            resolve_latest(source, version, agent, set, candidate_known, window).await?
        }
    };
    let candidate_id = match &request.candidate {
        RunRef::Id(id) => id.clone(),
        RunRef::Latest(version) => {
            let (agent, set) = latest_context(request, &runs, &request.baseline);
            resolve_latest(source, version, agent, set, Some(&baseline_id), window).await?
        }
    };
    if baseline_id == candidate_id {
        return Err(ReadError::Invalid(format!(
            "baseline and candidate are the same run `{baseline_id}`"
        )));
    }
    let missing: Vec<String> = [&baseline_id, &candidate_id]
        .into_iter()
        .filter(|id| !runs.contains_key(*id))
        .cloned()
        .collect();
    let ids = [baseline_id.clone(), candidate_id.clone()];
    let stats_doc = case_stats_document(window, &ids);
    let (missing_runs, stats_table) =
        futures::try_join!(fetch_named_runs(source, window, &missing), async {
            source.query(&stats_doc).await.map_err(ReadError::Query)
        })?;
    runs.extend(missing_runs);
    let run = |id: &str| {
        runs.get(id).cloned().ok_or_else(|| ReadError::UnknownRun {
            run_id: id.to_string(),
            from: window.from.clone(),
            to: window.to.clone(),
        })
    };
    let (baseline_run, candidate_run) = (run(&baseline_id)?, run(&candidate_id)?);

    let mut stats = decode_case_stats(&stats_table);
    let baseline_cases = stats.remove(&baseline_id).unwrap_or_default();
    let candidate_cases = stats.remove(&candidate_id).unwrap_or_default();

    let rows = compare_cases(&baseline_cases, &candidate_cases);
    let evaluators = summarize_evaluators(&baseline_cases, &candidate_cases, &rows)
        .into_iter()
        .map(|s| EvaluatorComparison {
            delta: stats_delta(&s.baseline, &s.candidate),
            baseline: (&s.baseline).into(),
            candidate: (&s.candidate).into(),
            name: s.name,
            worse: s.worse,
            better: s.better,
        })
        .collect();
    let kind_count = |kind: CaseKind| rows.iter().filter(|r| r.comparison.kind == kind).count();
    let counts = CaseCounts {
        regressions: kind_count(CaseKind::Regression),
        improvements: kind_count(CaseKind::Improvement),
        unchanged: kind_count(CaseKind::Unchanged),
        no_baseline: rows
            .iter()
            .filter(|r| r.comparison.kind == CaseKind::Regression && r.comparison.no_baseline)
            .count(),
        baseline_only: baseline_cases
            .keys()
            .filter(|c| !candidate_cases.contains_key(*c))
            .count(),
    };

    let mut regressions: Vec<RegressedCase> = rows
        .iter()
        .filter(|r| r.comparison.kind == CaseKind::Regression)
        .take(request.limit)
        .map(|row| {
            regressed_case(
                row,
                baseline_cases.get(&row.case_id),
                candidate_cases.get(&row.case_id),
            )
        })
        .collect();

    // Trace ids are read only for the regressions listed, after
    // classification, so this can't run alongside the stats query.
    if !regressions.is_empty() {
        let case_ids: Vec<String> = regressions.iter().map(|r| r.case_id.clone()).collect();
        let table = source
            .query(&case_traces_document(window, &ids, &case_ids))
            .await
            .map_err(ReadError::Query)?;
        let mut traces = decode_case_traces(&table);
        let baseline_traces = traces.remove(&baseline_id).unwrap_or_default();
        let candidate_traces = traces.remove(&candidate_id).unwrap_or_default();
        for case in &mut regressions {
            case.baseline_trace_id = baseline_traces.get(&case.case_id).cloned();
            case.candidate_trace_id = candidate_traces.get(&case.case_id).cloned();
        }
    }

    if request.include_tools {
        let trace_ids: Vec<String> = regressions
            .iter()
            .flat_map(|r| [r.baseline_trace_id.clone(), r.candidate_trace_id.clone()])
            .flatten()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        if !trace_ids.is_empty() {
            let table = source
                .query(&tool_spans_document(window, &trace_ids))
                .await
                .map_err(ReadError::Query)?;
            let calls = decode_tool_calls(&table);
            let none = Vec::new();
            for case in &mut regressions {
                let Some(candidate) = &case.candidate_trace_id else {
                    continue;
                };
                let cand = calls.get(candidate).unwrap_or(&none);
                // Without a baseline trace the candidate is diffed against
                // itself, as the Compare page does.
                let base = case
                    .baseline_trace_id
                    .as_ref()
                    .map_or(cand, |t| calls.get(t).unwrap_or(&none));
                case.tools = Some(tool_diff(base, cand));
            }
        }
    }

    let mut warnings = Vec::new();
    if baseline_run.set != candidate_run.set {
        warnings.push(format!(
            "the runs replayed different eval sets ({} vs {}); only cases with the same id are compared",
            baseline_run.set.as_deref().unwrap_or("none"),
            candidate_run.set.as_deref().unwrap_or("none")
        ));
    }
    if reached(&stats_table, STATS_ROW_LIMIT) {
        warnings.push(format!(
            "the per-case stats hit the {STATS_ROW_LIMIT}-row cap; some cases are missing"
        ));
    }
    let listed = regressions.len();
    Ok(Comparison {
        baseline: RunSummary::new(&baseline_run, None, now_ms),
        candidate: RunSummary::new(&candidate_run, None, now_ms),
        evaluators,
        counts,
        regressions,
        regressions_truncated: listed < counts.regressions,
        warnings,
    })
}

/// A [`StatsDelta`] as text: `-0.120` for a score, `-12.0pp` for a pass rate.
pub fn format_delta(delta: Option<&StatsDelta>) -> String {
    match delta {
        None => "-".to_string(),
        Some(StatsDelta {
            d,
            unit: DeltaUnit::Score,
        }) => format!("{d:+.3}"),
        Some(StatsDelta {
            d,
            unit: DeltaUnit::PassRate,
        }) => format!("{:+.1}pp", d * 100.0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{
        FakeIr, IrRows, RUN_COLUMNS, RunRow, Shape, case_trace, latest, run_case, stats_row,
        tool_span,
    };

    fn table(columns: &[&str], rows: Vec<Vec<Value>>) -> IrTable {
        IrTable {
            columns: columns.iter().map(|c| c.to_string()).collect(),
            rows,
        }
    }

    fn window() -> Window {
        Window {
            from: "now-7d".to_string(),
            to: "now".to_string(),
        }
    }

    fn json_of(doc: &Document) -> Value {
        serde_json::to_value(doc).expect("documents serialize")
    }

    const MIN: i64 = 60_000_000_000;

    #[allow(clippy::too_many_arguments)]
    fn run_row(
        run: &str,
        version: &str,
        name: &str,
        error: Option<&str>,
        n: u64,
        high: u64,
        score_sum: f64,
        first: i64,
        last: i64,
        unlinked: u64,
    ) -> Vec<Value> {
        RunRow {
            run,
            set: "golden",
            agent: None,
            service: Some("support-triage"),
            version,
            evaluator: name,
            error,
            n,
            high,
            score_sum,
            first,
            last,
            unlinked,
        }
        .row()
    }

    #[test]
    fn runs_fold_per_evaluator_and_fall_back_to_the_service_identity() {
        let runs = table(
            RUN_COLUMNS,
            vec![
                run_row("r2", "v2", "C", None, 4, 3, 2.8, 30 * MIN, 31 * MIN, 1),
                run_row(
                    "r2",
                    "v2",
                    "C",
                    Some("timeout"),
                    2,
                    0,
                    0.0,
                    30 * MIN,
                    32 * MIN,
                    0,
                ),
                run_row("r2", "v2", "T", None, 2, 2, 2.0, 29 * MIN, 30 * MIN, 0),
                run_row("r1", "v1", "C", None, 4, 4, 3.6, 10 * MIN, 11 * MIN, 0),
            ],
        );
        let cases = table(
            &["signaldb_eval_run_id", "signaldb_eval_case_id", "n"],
            vec![
                vec![json!("r2"), json!("a"), json!(3)],
                vec![json!("r2"), json!("b"), json!(3)],
                vec![json!("r1"), json!("a"), json!(4)],
                vec![json!("r1"), Value::Null, json!(1)],
            ],
        );
        let runs = decode_runs(&runs, &cases);
        assert_eq!(runs.len(), 2);
        let r2 = &runs[0];
        assert_eq!(r2.id, "r2");
        assert_eq!(r2.agent.as_deref(), Some("support-triage"));
        assert_eq!(r2.version.as_deref(), Some("v2"));
        assert_eq!((r2.first_ns, r2.last_ns), (29 * MIN, 32 * MIN));
        assert_eq!((r2.cases, r2.unlinked), (2, 1));
        assert_eq!((r2.stats.results, r2.stats.errors), (8, 2));
        assert_eq!(r2.evaluators["C"].pass_rate(), Some(0.75));
        assert_eq!(runs[1].cases, 1);
        assert_eq!(
            baselines_of(&runs).get("r2").map(String::as_str),
            Some("r1")
        );

        let summary = RunSummary::new(r2, None, 60 * MIN / 1_000_000);
        assert_eq!(summary.status, RunStatusKind::Partial);
        assert_eq!(summary.started_at, "1970-01-01T00:29:00.000Z");
    }

    #[test]
    fn the_newest_other_run_wins() {
        let t = table(
            &["signaldb_eval_run_id", "last"],
            vec![
                vec![json!("run-a"), json!(100)],
                vec![json!("run-b"), json!("300")],
                vec![json!(""), json!(900)],
                vec![json!("run-c"), json!(200)],
            ],
        );
        assert_eq!(newest_run(&t).as_deref(), Some("run-b"));
        let doc = json_of(&latest_run_document(
            "a",
            "v1",
            "golden",
            Some("run-42"),
            &window(),
        ));
        assert_eq!(doc["pipeline"][4]["where"]["op"], "ne");
        assert_eq!(doc["pipeline"][4]["where"]["value"], "run-42");
        let doc = json_of(&latest_run_document("a", "v1", "golden", None, &window()));
        assert!(doc["pipeline"][4].get("aggregate").is_some());
    }

    #[test]
    fn run_references_parse() {
        assert_eq!(RunRef::parse(" run-1 "), Ok(RunRef::Id("run-1".into())));
        assert_eq!(
            RunRef::parse("latest:v1.8.0"),
            Ok(RunRef::Latest("v1.8.0".into()))
        );
        assert!(RunRef::parse("latest:").is_err());
        assert!(RunRef::parse("").is_err());
        let err = RunRef::parse_named("baseline", "latest:").expect_err("no version");
        assert!(err.starts_with("`baseline`: "), "{err}");
    }

    #[test]
    fn documents_declare_the_lowest_ir_version_they_need() {
        let filter = RunFilter::default();
        let ids = ["r".to_string()];
        assert_eq!(runs_document(&window(), &filter).ir_version, 1);
        assert_eq!(case_stats_document(&window(), &ids).ir_version, 1);
        assert_eq!(case_traces_document(&window(), &ids, &ids).ir_version, 5);
        let doc = json_of(&runs_document(&window(), &filter));
        assert_eq!(
            doc["pipeline"][2]["aggregate"]["aggs"][1],
            json!({"fn": "count", "as": "high",
                   "where": {"field": EVALUATION_SCORE_VALUE, "op": "gte", "value": PASS_THRESHOLD}})
        );
        assert_eq!(
            doc["pipeline"][2]["aggregate"]["aggs"][7],
            json!({"fn": "count", "as": "unlinked", "where": {"not": {"and": [
                {"field": "trace_id", "op": "exists"},
                {"field": "trace_id", "op": "ne", "value": ""},
            ]}}})
        );
    }

    #[test]
    fn tool_calls_come_back_in_call_order() {
        let t = table(
            &[
                "trace_id",
                "start_time_unix_nano",
                "span_name",
                "gen_ai_tool_name",
            ],
            vec![
                vec![json!("t1"), json!(30), json!("x"), json!("issue_refund")],
                vec![
                    json!("t1"),
                    json!(10),
                    json!("execute_tool lookup"),
                    Value::Null,
                ],
                vec![json!("t1"), json!("20"), json!("x"), json!("check_policy")],
            ],
        );
        assert_eq!(
            decode_tool_calls(&t)["t1"],
            ["execute_tool lookup", "check_policy", "issue_refund"]
        );
    }

    fn rows() -> IrRows {
        IrRows {
            runs: vec![
                run_row("base", "v1", "Correctness", None, 2, 2, 1.8, MIN, MIN, 0),
                run_row(
                    "cand",
                    "v2",
                    "Correctness",
                    None,
                    2,
                    0,
                    0.3,
                    2 * MIN,
                    2 * MIN,
                    0,
                ),
            ],
            run_cases: vec![run_case("base", "refund"), run_case("cand", "refund")],
            case_stats: vec![
                stats_row("base", "refund", "Correctness", "pass", 0.9),
                stats_row("cand", "refund", "Correctness", "fail", 0.2),
                stats_row("cand", "new-case", "Correctness", "fail", 0.1),
                stats_row("base", "gone", "Correctness", "pass", 0.9),
            ],
            case_traces: vec![
                case_trace("base", "refund", "tb"),
                case_trace("cand", "refund", "tc"),
                case_trace("cand", "new-case", "tn"),
            ],
            latest: vec![latest("base", 5)],
            tool_spans: vec![
                tool_span("tb", 1, "lookup_order"),
                tool_span("tb", 2, "check_policy"),
                tool_span("tc", 1, "lookup_order"),
            ],
        }
    }

    fn fake() -> FakeIr {
        FakeIr::new(rows())
    }

    #[tokio::test]
    async fn compare_resolves_latest_and_lists_regressions_with_tool_diffs() {
        let fake = fake();
        let request = CompareRequest {
            baseline: RunRef::Latest("v1".into()),
            candidate: RunRef::Id("cand".into()),
            agent: None,
            set: None,
            window: window(),
            limit: 1,
            include_tools: true,
        };
        let cmp = compare_runs(&fake, &request, 100 * MIN / 1_000_000)
            .await
            .expect("compares");
        assert_eq!(cmp.baseline.run_id, "base");
        assert_eq!(cmp.candidate.run_id, "cand");
        assert_eq!(
            (
                cmp.counts.regressions,
                cmp.counts.no_baseline,
                cmp.counts.baseline_only
            ),
            (2, 1, 1)
        );
        assert!(cmp.regressions_truncated);
        let first = &cmp.regressions[0];
        assert_eq!(first.case_id, "refund");
        assert_eq!(first.evaluators[0].change, "worse");
        assert_eq!(first.candidate_trace_id.as_deref(), Some("tc"));
        let tools = first.tools.as_ref().expect("tools");
        assert_eq!(tools[1].name, "check_policy");
        assert_eq!(tools[1].kind, super::super::compare::ToolMark::Skipped);

        let seen: Vec<Value> = fake.seen().iter().map(json_of).collect();
        let latest = seen
            .iter()
            .find(|d| d["pipeline"].as_array().is_some_and(|p| p.len() == 6))
            .expect("latest resolution document");
        // The agent filter falls back to service.name, like the rest of the model.
        let agent = latest["pipeline"][1]["where"].to_string();
        assert!(agent.contains(r#""field":"service.name","op":"eq","value":"support-triage""#));
        assert_eq!(latest["pipeline"][4]["where"]["value"], "cand");
    }

    #[tokio::test]
    async fn traces_are_read_only_for_the_listed_regressions() {
        let fake = fake();
        let request = CompareRequest {
            baseline: RunRef::Id("base".into()),
            candidate: RunRef::Id("cand".into()),
            agent: None,
            set: None,
            window: window(),
            limit: 1,
            include_tools: true,
        };
        compare_runs(&fake, &request, 0).await.expect("compares");
        let seen = fake.seen();
        let in_values = |shape, field: &str| -> Vec<Value> {
            let doc = seen
                .iter()
                .find(|d| Shape::of(d) == Some(shape))
                .map(json_of)
                .expect("document asked");
            doc["pipeline"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|s| s["where"]["field"] == field && s["where"]["op"] == "in")
                .and_then(|s| s["where"]["value"].as_array().cloned())
                .unwrap_or_default()
        };
        assert_eq!(in_values(Shape::CaseTraces, CASE_ID), [json!("refund")]);
        assert_eq!(
            in_values(Shape::ToolSpans, "trace_id"),
            [json!("tb"), json!("tc")]
        );
    }

    #[tokio::test]
    async fn without_regressions_no_trace_is_read() {
        let fake = FakeIr::new(IrRows {
            case_stats: vec![
                stats_row("base", "refund", "Correctness", "pass", 0.9),
                stats_row("cand", "refund", "Correctness", "pass", 0.9),
            ],
            ..rows()
        });
        let request = CompareRequest {
            baseline: RunRef::Id("base".into()),
            candidate: RunRef::Id("cand".into()),
            agent: None,
            set: None,
            window: window(),
            limit: 10,
            include_tools: true,
        };
        let cmp = compare_runs(&fake, &request, 0).await.expect("compares");
        assert!(cmp.regressions.is_empty());
        assert!(
            fake.seen()
                .iter()
                .all(|d| !matches!(Shape::of(d), Some(Shape::CaseTraces | Shape::ToolSpans)))
        );
    }

    #[tokio::test]
    async fn latest_on_both_sides_needs_agent_and_set() {
        let fake = fake();
        let request = CompareRequest {
            baseline: RunRef::Latest("v1".into()),
            candidate: RunRef::Latest("v2".into()),
            agent: None,
            set: None,
            window: window(),
            limit: 10,
            include_tools: false,
        };
        let err = compare_runs(&fake, &request, 0)
            .await
            .expect_err("unresolvable");
        assert!(matches!(err, ReadError::Invalid(_)), "{err}");
    }

    #[test]
    fn deltas_format_by_unit() {
        assert_eq!(format_delta(None), "-");
        assert_eq!(
            format_delta(Some(&StatsDelta {
                d: -0.12,
                unit: DeltaUnit::Score
            })),
            "-0.120"
        );
        assert_eq!(
            format_delta(Some(&StatsDelta {
                d: 0.25,
                unit: DeltaUnit::PassRate
            })),
            "+25.0pp"
        );
    }
}
