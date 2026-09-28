//! # A Query IR stand-in for the eval reads
//!
//! [`FakeIr`] answers each document [`runs`](crate::runs) builds from the
//! [`IrRows`] the test provides, recognising the document by its source and its
//! aggregate's `by` (see [`Shape::of`]). It narrows rows by the `in`
//! predicates on run id, case id and trace id the way the router would, so a
//! test sees what a narrowed read returns. [`FakeIr::response_body`] wraps an
//! answer in the `POST /api/v1/query` response body, for HTTP mocks.

use std::sync::Mutex;

use query_ir::{AggFn, ComparisonOp, Document, Predicate, Stage};
use serde_json::{Value, json};

use crate::runs::{IrSource, IrTable};
use crate::{CASE_ID, PASS_THRESHOLD, RUN_ID, SET};

/// Columns of a [`runs_document`](crate::runs::runs_document) answer; rows
/// from [`RunRow::row`].
pub const RUN_COLUMNS: &[&str] = &[
    "signaldb_eval_run_id",
    "signaldb_eval_set",
    "gen_ai_agent_name",
    "service_name",
    "gen_ai_agent_version",
    "service_version",
    "gen_ai_evaluation_name",
    "gen_ai_evaluation_score_label",
    "error_type",
    "n",
    "high",
    "low",
    "score_sum",
    "scored",
    "first",
    "last",
    "unlinked",
];
/// Columns of a [`run_cases_document`](crate::runs::run_cases_document)
/// answer; rows from [`run_case`].
pub const RUN_CASE_COLUMNS: &[&str] = &["signaldb_eval_run_id", "signaldb_eval_case_id", "n"];
/// Columns of a [`case_stats_document`](crate::runs::case_stats_document)
/// answer; rows from [`stats_row`].
pub const CASE_STATS_COLUMNS: &[&str] = &[
    "signaldb_eval_run_id",
    "signaldb_eval_case_id",
    "gen_ai_evaluation_name",
    "gen_ai_evaluation_score_label",
    "error_type",
    "n",
    "high",
    "low",
    "score_sum",
    "scored",
];
/// Columns of a [`case_traces_document`](crate::runs::case_traces_document)
/// answer; rows from [`case_trace`].
pub const CASE_TRACE_COLUMNS: &[&str] = &["signaldb_eval_run_id", "signaldb_eval_case_id", "trace"];
/// Columns of a [`latest_run_document`](crate::runs::latest_run_document)
/// answer; rows from [`latest`].
pub const LATEST_COLUMNS: &[&str] = &["signaldb_eval_run_id", "last"];
/// Columns of a [`tool_spans_document`](crate::runs::tool_spans_document)
/// answer; rows from [`tool_span`].
pub const TOOL_SPAN_COLUMNS: &[&str] = &[
    "trace_id",
    "start_time_unix_nano",
    "span_name",
    "gen_ai_tool_name",
];

/// Which of the eval reads a document is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    Runs,
    RunCases,
    CaseStats,
    CaseTraces,
    Latest,
    ToolSpans,
}

impl Shape {
    /// The read `document` is, by its source and its aggregate's `by`.
    pub fn of(document: &Document) -> Option<Self> {
        if document.from == "traces" {
            return Some(Self::ToolSpans);
        }
        let aggregate = document.pipeline.iter().find_map(|stage| match stage {
            Stage::Aggregate(a) => Some(a),
            _ => None,
        })?;
        let by: Vec<&str> = aggregate.by.iter().map(String::as_str).collect();
        Some(match by.as_slice() {
            [RUN_ID] => Self::Latest,
            [RUN_ID, CASE_ID] if aggregate.aggs.iter().any(|a| a.func == AggFn::First) => {
                Self::CaseTraces
            }
            [RUN_ID, CASE_ID] => Self::RunCases,
            [RUN_ID, CASE_ID, ..] => Self::CaseStats,
            [RUN_ID, SET, ..] => Self::Runs,
            _ => return None,
        })
    }

    fn columns(self) -> &'static [&'static str] {
        match self {
            Self::Runs => RUN_COLUMNS,
            Self::RunCases => RUN_CASE_COLUMNS,
            Self::CaseStats => CASE_STATS_COLUMNS,
            Self::CaseTraces => CASE_TRACE_COLUMNS,
            Self::Latest => LATEST_COLUMNS,
            Self::ToolSpans => TOOL_SPAN_COLUMNS,
        }
    }
}

/// The rows a [`FakeIr`] answers each [`Shape`] with, in that shape's
/// columns.
#[derive(Debug, Clone, Default)]
pub struct IrRows {
    pub runs: Vec<Vec<Value>>,
    pub run_cases: Vec<Vec<Value>>,
    pub case_stats: Vec<Vec<Value>>,
    pub case_traces: Vec<Vec<Value>>,
    pub latest: Vec<Vec<Value>>,
    pub tool_spans: Vec<Vec<Value>>,
}

/// Answers every eval read from [`IrRows`], recording each document it is
/// asked.
#[derive(Debug, Default)]
pub struct FakeIr {
    rows: IrRows,
    seen: Mutex<Vec<Document>>,
}

impl FakeIr {
    pub fn new(rows: IrRows) -> Self {
        Self {
            rows,
            seen: Mutex::default(),
        }
    }

    /// The documents asked so far, in order.
    pub fn seen(&self) -> Vec<Document> {
        self.seen.lock().map(|s| s.clone()).unwrap_or_default()
    }

    /// The table the router would answer `document` with; an unrecognised
    /// document gets an empty one.
    pub fn answer(&self, document: &Document) -> IrTable {
        if let Ok(mut seen) = self.seen.lock() {
            seen.push(document.clone());
        }
        let Some(shape) = Shape::of(document) else {
            return IrTable::default();
        };
        let rows = match shape {
            Shape::Runs => &self.rows.runs,
            Shape::RunCases => &self.rows.run_cases,
            Shape::CaseStats => &self.rows.case_stats,
            Shape::CaseTraces => &self.rows.case_traces,
            Shape::Latest => &self.rows.latest,
            Shape::ToolSpans => &self.rows.tool_spans,
        };
        let columns = shape.columns();
        let filters = in_filters(document);
        let keep = |row: &Vec<Value>| {
            filters.iter().all(|(field, values)| {
                let column = field.replace('.', "_");
                columns
                    .iter()
                    .position(|c| *c == column)
                    .and_then(|i| row.get(i))
                    .is_none_or(|cell| values.contains(cell))
            })
        };
        IrTable {
            columns: columns.iter().map(|c| c.to_string()).collect(),
            rows: rows.iter().filter(|row| keep(row)).cloned().collect(),
        }
    }

    /// The `POST /api/v1/query` response body answering a request body; a
    /// body that isn't a document gets an empty table.
    pub fn response_body(&self, request_body: &[u8]) -> Value {
        let Ok(document) = serde_json::from_slice::<Document>(request_body) else {
            return json!({"result": "table", "columns": [], "rows": [],
                          "window": {"start_ns": 0, "end_ns": 1}});
        };
        let table = self.answer(&document);
        json!({
            "result": document.result.as_str(),
            "columns": table.columns.iter()
                .map(|c| json!({"name": c, "type": "string"}))
                .collect::<Vec<_>>(),
            "rows": table.rows,
            "window": {"start_ns": 0, "end_ns": 1},
        })
    }
}

impl IrSource for FakeIr {
    type Error = std::convert::Infallible;

    async fn query(&self, document: &Document) -> Result<IrTable, Self::Error> {
        Ok(self.answer(document))
    }
}

/// The top-level `field in [...]` stages of a document.
fn in_filters(document: &Document) -> Vec<(&str, Vec<Value>)> {
    document
        .pipeline
        .iter()
        .filter_map(|stage| match stage {
            Stage::Where(Predicate::Leaf(leaf)) if leaf.op == ComparisonOp::In => Some((
                leaf.field.as_str(),
                leaf.value
                    .as_ref()
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default(),
            )),
            _ => None,
        })
        .collect()
}

/// One row of a runs answer: one (run, evaluator, label, error) group.
#[derive(Debug, Clone, Default)]
pub struct RunRow<'a> {
    pub run: &'a str,
    pub set: &'a str,
    /// `gen_ai.agent.name`; `None` leaves only `service`.
    pub agent: Option<&'a str>,
    pub service: Option<&'a str>,
    pub version: &'a str,
    pub evaluator: &'a str,
    pub error: Option<&'a str>,
    pub n: u64,
    /// Results scoring at least [`PASS_THRESHOLD`]; every other scored
    /// result is low. An errored group has no scored results.
    pub high: u64,
    pub score_sum: f64,
    pub first: i64,
    pub last: i64,
    pub unlinked: u64,
}

impl RunRow<'_> {
    pub fn row(&self) -> Vec<Value> {
        let scored = if self.error.is_some() { 0 } else { self.n };
        vec![
            json!(self.run),
            json!(self.set),
            json!(self.agent),
            json!(self.service),
            json!(self.version),
            Value::Null,
            json!(self.evaluator),
            Value::Null,
            json!(self.error),
            json!(self.n),
            json!(self.high),
            json!(scored.saturating_sub(self.high)),
            json!(self.score_sum),
            json!(scored),
            json!(self.first),
            json!(self.last),
            json!(self.unlinked),
        ]
    }
}

/// One (run, case) of a run cases answer.
pub fn run_case(run: &str, case: &str) -> Vec<Value> {
    vec![json!(run), json!(case), json!(1)]
}

/// One evaluator's single result on one case of a run.
pub fn stats_row(run: &str, case: &str, evaluator: &str, label: &str, score: f64) -> Vec<Value> {
    let high = u64::from(score >= PASS_THRESHOLD);
    vec![
        json!(run),
        json!(case),
        json!(evaluator),
        json!(label),
        Value::Null,
        json!(1),
        json!(high),
        json!(1 - high),
        json!(score),
        json!(1),
    ]
}

/// The trace a run's agent produced for a case.
pub fn case_trace(run: &str, case: &str, trace: &str) -> Vec<Value> {
    vec![json!(run), json!(case), json!(trace)]
}

/// A run's last result time, for `latest:` resolution.
pub fn latest(run: &str, last: i64) -> Vec<Value> {
    vec![json!(run), json!(last)]
}

/// One `execute_tool` span.
pub fn tool_span(trace: &str, start: i64, tool: &str) -> Vec<Value> {
    vec![
        json!(trace),
        json!(start),
        json!("execute_tool"),
        json!(tool),
    ]
}
