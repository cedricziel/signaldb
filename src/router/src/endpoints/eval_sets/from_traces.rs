//! # Appending eval cases from traces
//!
//! `POST /api/v1/eval-sets/{name}/cases/from-traces` (change:
//! agent-offline-evals, task 5.6): one case per matching agent trace not
//! already in the set. Every read goes through the Query IR, as three
//! documents run server-side for the caller's tenant and dataset:
//!
//! 1. **matches** (`traces`, `table`): the agent spans (`gen_ai.operation.name`,
//!    `gen_ai.agent.name`, extra predicates) grouped by `trace_id` with their
//!    latest start, newest first, at most [`MATCH_TRACE_CAP`] traces;
//! 2. **failing results** (`logs`, `rows`, only with `failing_evaluator`):
//!    that evaluator's `gen_ai.evaluation.result` records with trace context,
//!    at most [`RESULT_ROW_CAP`] rows, judged by
//!    [`common::evals::verdict_of_result`];
//! 3. **spans** (`traces`, `rows`): the sampled traces' agent and
//!    `execute_tool` spans, at most [`SPAN_ROW_CAP`] rows, from which each
//!    case's input, expected tools and reference are read.
//!
//! Selection is deterministic: newest trace first (ties by trace id), traces
//! whose id is already a case source in the set skipped, then the first
//! `sample`. Document building, row decoding and selection are pure
//! functions below so they are testable without a querier.

use std::collections::HashSet;
use std::rc::Rc;

use axum::http::StatusCode;
use common::eval_sets::{EvalCase, EvalCaseSource};
use common::evals::{
    AGENT_NAME, ERROR_TYPE, EVALUATION_NAME, EVALUATION_RESULT_EVENT, EVALUATION_SCORE_LABEL,
    EVALUATION_SCORE_VALUE, EvalResult, INPUT_MESSAGES, OPERATION_EXECUTE_TOOL,
    OPERATION_INVOKE_AGENT, OPERATION_NAME, OUTPUT_MESSAGES, TOOL_NAME, Verdict, verdict_of_result,
};
use common::query_ir::{
    Agg, AggFn, Aggregate, ComparisonOp, Direction, Document, Leaf, Order, Predicate, Range,
    ResultEnvelope, Stage,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::endpoints::api_error::ApiError;
use crate::endpoints::query::{QueryRange, ResultColumn};

/// Cases added per call when `sample` is omitted.
pub(super) const DEFAULT_SAMPLE: u32 = 50;
/// Most cases one call may add.
pub(super) const MAX_SAMPLE: u32 = 1_000;
/// Most distinct matching traces read; `matches` never exceeds it.
pub(super) const MATCH_TRACE_CAP: u64 = 10_000;
/// Most evaluator result rows read for the `failing_evaluator` filter.
pub(super) const RESULT_ROW_CAP: u64 = 50_000;
/// Most span rows read for the sampled traces (agent plus tool spans).
pub(super) const SPAN_ROW_CAP: u64 = 100_000;

const TRACE_ID: &str = "trace_id";
const START: &str = "start_time_unix_nano";
const SPAN_NAME: &str = "span.name";
const LAST_START: &str = "last_start";

/// Row-lookup spellings of the logical fields above whose name holds a dot:
/// a projected or grouped field comes back under its name with dots
/// replaced by underscores, and [`Row`] is keyed on that spelling directly
/// (see `Row::all`), so these are computed once, by hand, rather than at
/// every lookup.
const SPAN_NAME_COL: &str = "span_name";
const OPERATION_NAME_COL: &str = "gen_ai_operation_name";
const TOOL_NAME_COL: &str = "gen_ai_tool_name";
const INPUT_MESSAGES_COL: &str = "gen_ai_input_messages";
const OUTPUT_MESSAGES_COL: &str = "gen_ai_output_messages";
const ERROR_TYPE_COL: &str = "error_type";
const EVALUATION_SCORE_LABEL_COL: &str = "gen_ai_evaluation_score_label";
const EVALUATION_SCORE_VALUE_COL: &str = "gen_ai_evaluation_score_value";

/// Body of `POST /api/v1/eval-sets/{name}/cases/from-traces`.
///
/// Unknown keys are rejected: every option narrows or shapes the query, so
/// a misspelt one silently widening it would add the wrong cases.
#[derive(Debug, Clone, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AppendCasesFromTracesRequest {
    /// Time window of the agent spans, as in a Query IR document (e.g.
    /// `{"from": "now-7d", "to": "now"}`).
    pub range: QueryRange,
    /// `gen_ai.agent.name` of the agent span (a span without one matches on
    /// `service.name`). Defaults to the eval set's agent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    /// `gen_ai.operation.name` of the agent span. Default `invoke_agent`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
    /// Extra Query IR predicates the agent span must satisfy (logical field
    /// names, e.g. `{"field": "deployment.environment", "op": "eq", "value":
    /// "prod"}`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schema(value_type = Vec<Object>)]
    pub filters: Vec<Predicate>,
    /// Keep only traces holding at least one failing result
    /// (`gen_ai.evaluation.result`) of the evaluator with this
    /// `gen_ai.evaluation.name` in the same window. Evaluator errors never
    /// count as failures.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failing_evaluator: Option<String>,
    /// How many new cases to add, 1-1000. Default 50.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(minimum = 1, maximum = 1000)]
    pub sample: Option<u32>,
    /// Set each case's expected tools to the trace's `execute_tool` spans'
    /// `gen_ai.tool.name`s in call order.
    #[serde(default)]
    pub expected_tools: bool,
    /// Set each case's reference to the agent's answer (the last text of
    /// the agent span's `gen_ai.output.messages`).
    #[serde(default)]
    pub reference_from_answer: bool,
    /// Tags put on every new case.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
}

/// Result of appending cases from traces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct AppendCasesFromTracesOutcome {
    /// Distinct matching traces (after the `failing_evaluator` filter), at
    /// most 10,000.
    pub matches: u64,
    /// Matching traces the set already holds a case for.
    pub already_present: u64,
    pub added: u64,
    /// Ids of the new cases, in the order they now appear in the set.
    pub added_ids: Vec<String>,
}

/// The request with its defaults applied and its options checked.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Options {
    /// `None` matches any agent; the handler fills in the set's agent.
    pub(super) agent: Option<String>,
    pub(super) operation: String,
    pub(super) filters: Vec<Predicate>,
    pub(super) failing_evaluator: Option<String>,
    pub(super) sample: usize,
    pub(super) expected_tools: bool,
    pub(super) reference_from_answer: bool,
    pub(super) tags: Vec<String>,
}

impl Options {
    /// Applies the defaults; a `422` names the first invalid option.
    pub(super) fn new(request: AppendCasesFromTracesRequest) -> Result<Self, ApiError> {
        let non_empty = |value: Option<String>, what: &str| match value {
            Some(v) if v.trim().is_empty() => Err(invalid(format!("`{what}` must not be empty"))),
            other => Ok(other),
        };
        let sample = request.sample.unwrap_or(DEFAULT_SAMPLE);
        if !(1..=MAX_SAMPLE).contains(&sample) {
            return Err(invalid(format!(
                "`sample` must be between 1 and {MAX_SAMPLE}, got {sample}"
            )));
        }
        if request.tags.iter().any(|t| t.trim().is_empty()) {
            return Err(invalid("`tags` must not hold an empty tag"));
        }
        Ok(Self {
            agent: non_empty(request.agent, "agent")?,
            operation: non_empty(request.operation, "operation")?
                .unwrap_or_else(|| OPERATION_INVOKE_AGENT.to_string()),
            filters: request.filters,
            failing_evaluator: non_empty(request.failing_evaluator, "failing_evaluator")?,
            sample: sample as usize,
            expected_tools: request.expected_tools,
            reference_from_answer: request.reference_from_answer,
            tags: request.tags,
        })
    }
}

/// A `422` in the shared error envelope.
pub(super) fn invalid(message: impl Into<String>) -> ApiError {
    ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, message)
}

// ---- IR documents ---------------------------------------------------------

fn eq(field: &str, value: &str) -> Predicate {
    Predicate::Leaf(Leaf {
        field: field.to_string(),
        op: ComparisonOp::Eq,
        value: Some(json!(value)),
    })
}

fn ne(field: &str, value: &str) -> Predicate {
    Predicate::Leaf(Leaf {
        field: field.to_string(),
        op: ComparisonOp::Ne,
        value: Some(json!(value)),
    })
}

fn exists(field: &str) -> Predicate {
    Predicate::Leaf(Leaf {
        field: field.to_string(),
        op: ComparisonOp::Exists,
        value: None,
    })
}

fn in_list(field: &str, values: &[String]) -> Predicate {
    Predicate::Leaf(Leaf {
        field: field.to_string(),
        op: ComparisonOp::In,
        value: Some(json!(values)),
    })
}

/// The agent span of `options`: its operation and agent (by
/// `gen_ai.agent.name`, or `service.name` when the span names no agent, as
/// the Evaluate pages match agents) plus the caller's predicates.
fn agent_span_predicate(options: &Options) -> Predicate {
    let mut all = vec![eq(OPERATION_NAME, &options.operation)];
    if let Some(agent) = &options.agent {
        all.push(Predicate::Or(vec![
            eq(AGENT_NAME, agent),
            Predicate::And(vec![
                Predicate::Not(Box::new(exists(AGENT_NAME))),
                eq("service.name", agent),
            ]),
        ]));
    }
    all.extend(options.filters.iter().cloned());
    Predicate::And(all)
}

/// Absolute nanosecond bounds, so every document sees one window.
fn range(start_ns: i64, end_ns: i64) -> Range {
    Range {
        from: json!(start_ns.to_string()),
        to: json!(end_ns.to_string()),
    }
}

/// Builds a document from its parts and declares the lowest `irVersion` it
/// needs.
fn build_document(
    from: &str,
    start_ns: i64,
    end_ns: i64,
    result: ResultEnvelope,
    fields: Option<Vec<String>>,
    pipeline: Vec<Stage>,
) -> Document {
    let mut doc = Document {
        ir_version: 1,
        from: from.to_string(),
        range: range(start_ns, end_ns),
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

/// One named `max` aggregate output.
fn max_agg(of: &str, as_name: &str) -> Agg {
    Agg {
        func: AggFn::Max,
        of: Some(of.to_string()),
        arg: None,
        divisor: None,
        as_name: as_name.to_string(),
        scope: None,
        across: None,
        window: None,
    }
}

/// Query 1: distinct traces holding a matching agent span, each with its
/// latest agent-span start, newest first.
pub(super) fn matches_document(options: &Options, start_ns: i64, end_ns: i64) -> Document {
    build_document(
        "traces",
        start_ns,
        end_ns,
        ResultEnvelope::Table,
        None,
        vec![
            Stage::Where(agent_span_predicate(options)),
            Stage::Aggregate(Aggregate {
                by: vec![TRACE_ID.to_string()],
                aggs: vec![max_agg(START, LAST_START)],
                step: None,
            }),
            Stage::Order(vec![Order {
                of: LAST_START.to_string(),
                dir: Direction::Desc,
            }]),
            Stage::Limit(MATCH_TRACE_CAP),
        ],
    )
}

/// Query 2: `evaluator`'s results carrying trace context, with what the
/// pass rule reads.
pub(super) fn results_document(evaluator: &str, start_ns: i64, end_ns: i64) -> Document {
    build_document(
        "logs",
        start_ns,
        end_ns,
        ResultEnvelope::Rows,
        Some(vec![
            TRACE_ID.to_string(),
            EVALUATION_SCORE_LABEL.to_string(),
            EVALUATION_SCORE_VALUE.to_string(),
            ERROR_TYPE.to_string(),
        ]),
        vec![
            Stage::Where(Predicate::And(vec![
                eq("event_name", EVALUATION_RESULT_EVENT),
                eq(EVALUATION_NAME, evaluator),
                exists(TRACE_ID),
                ne(TRACE_ID, ""),
            ])),
            Stage::Limit(RESULT_ROW_CAP),
        ],
    )
}

/// Query 3: the matching agent spans and every `execute_tool` span of
/// `trace_ids`.
pub(super) fn spans_document(
    options: &Options,
    trace_ids: &[String],
    start_ns: i64,
    end_ns: i64,
) -> Document {
    build_document(
        "traces",
        start_ns,
        end_ns,
        ResultEnvelope::Rows,
        Some(vec![
            TRACE_ID.to_string(),
            START.to_string(),
            SPAN_NAME.to_string(),
            OPERATION_NAME.to_string(),
            TOOL_NAME.to_string(),
            INPUT_MESSAGES.to_string(),
            OUTPUT_MESSAGES.to_string(),
        ]),
        vec![
            Stage::Where(in_list(TRACE_ID, trace_ids)),
            Stage::Where(Predicate::Or(vec![
                eq(OPERATION_NAME, OPERATION_EXECUTE_TOOL),
                agent_span_predicate(options),
            ])),
            Stage::Limit(SPAN_ROW_CAP),
        ],
    )
}

// ---- rows -----------------------------------------------------------------

/// One result row by column name. A projected or grouped logical field comes
/// back under its name with dots as underscores (`gen_ai.tool.name` →
/// `gen_ai_tool_name`), so the column names are normalised to that spelling
/// once per result set (in [`Row::all`]) and callers look fields up under
/// their precomputed underscore spelling (the `_COL` constants above),
/// rather than reformatting the field name on every lookup.
pub(super) struct Row<'a> {
    /// Underscore-normalised column names, shared (cheaply, by reference
    /// count) by every row of the same result set.
    keys: Rc<[String]>,
    values: &'a [Value],
}

impl<'a> Row<'a> {
    pub(super) fn all(columns: &'a [ResultColumn], rows: &'a [Vec<Value>]) -> Vec<Row<'a>> {
        let keys: Rc<[String]> = columns
            .iter()
            .map(|c| c.name.replace('.', "_"))
            .collect::<Vec<_>>()
            .into();
        rows.iter()
            .map(|values| Row {
                keys: keys.clone(),
                values: values.as_slice(),
            })
            .collect()
    }

    fn get(&self, field: &str) -> Option<&'a Value> {
        let index = self.keys.iter().position(|k| k == field)?;
        self.values.get(index).filter(|v| !v.is_null())
    }

    fn operation(&self) -> Option<&'a str> {
        self.str(OPERATION_NAME_COL)
    }

    fn str(&self, field: &str) -> Option<&'a str> {
        self.get(field)?.as_str().filter(|s| !s.is_empty())
    }

    fn i64(&self, field: &str) -> Option<i64> {
        common::query_ir::coerce(self.get(field)?, &common::query_ir::ValueType::Int64)
            .ok()
            .and_then(|lit| match lit {
                common::query_ir::Literal::Int64(n) => Some(n),
                _ => None,
            })
    }

    fn f64(&self, field: &str) -> Option<f64> {
        common::query_ir::coerce(self.get(field)?, &common::query_ir::ValueType::Float64)
            .ok()
            .and_then(|lit| match lit {
                common::query_ir::Literal::Float64(n) => Some(n),
                _ => None,
            })
    }

    /// A lower-cased W3C trace id, or `None` when the cell is not one.
    fn trace_id(&self) -> Option<String> {
        self.str(TRACE_ID)
            .filter(|id| id.len() == 32 && id.chars().all(|c| c.is_ascii_hexdigit()))
            .map(str::to_ascii_lowercase)
    }
}

/// A matching trace and the start of its latest matching agent span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Candidate {
    pub(super) trace_id: String,
    pub(super) last_start_ns: i64,
}

/// Candidates from the matches rows, newest first, ties broken by trace id
/// so the order never depends on the scan. One row per trace id: the
/// matches document already aggregates by `trace_id` with `max(start)`, so
/// each row maps straight to a candidate.
pub(super) fn decode_candidates(rows: &[Row<'_>]) -> Vec<Candidate> {
    let mut candidates: Vec<Candidate> = rows
        .iter()
        .filter_map(|row| {
            let trace_id = row.trace_id()?;
            let last_start_ns = row.i64(LAST_START).unwrap_or(i64::MIN);
            Some(Candidate {
                trace_id,
                last_start_ns,
            })
        })
        .collect();
    candidates.sort_by(|a, b| {
        b.last_start_ns
            .cmp(&a.last_start_ns)
            .then_with(|| a.trace_id.cmp(&b.trace_id))
    });
    candidates
}

/// Traces holding at least one failing result by the pass rule.
pub(super) fn failing_traces(rows: &[Row<'_>]) -> HashSet<String> {
    rows.iter()
        .filter(|row| {
            verdict_of_result(EvalResult {
                error: row.str(ERROR_TYPE_COL),
                label: row.str(EVALUATION_SCORE_LABEL_COL),
                score: row.f64(EVALUATION_SCORE_VALUE_COL),
            }) == Some(Verdict::Fail)
        })
        .filter_map(Row::trace_id)
        .collect()
}

/// What to add: the counts the response reports and the sampled trace ids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Selection {
    pub(super) matches: u64,
    pub(super) already_present: u64,
    /// Newest first, at most `sample`.
    pub(super) trace_ids: Vec<String>,
}

/// Keeps the candidates in `failing` (when given), counts those already in
/// `present`, and samples the newest `sample` of the rest.
pub(super) fn select(
    candidates: &[Candidate],
    failing: Option<&HashSet<String>>,
    present: &HashSet<String>,
    sample: usize,
) -> Selection {
    let matching: Vec<&Candidate> = candidates
        .iter()
        .filter(|c| failing.is_none_or(|f| f.contains(&c.trace_id)))
        .collect();
    let (already, fresh): (Vec<&Candidate>, Vec<&Candidate>) =
        matching.iter().partition(|c| present.contains(&c.trace_id));
    Selection {
        matches: matching.len() as u64,
        already_present: already.len() as u64,
        trace_ids: fresh
            .into_iter()
            .take(sample)
            .map(|c| c.trace_id.clone())
            .collect(),
    }
}

/// The case id for a trace: stable across calls, so re-running a query
/// never duplicates a case.
pub(super) fn case_id(trace_id: &str) -> String {
    let prefix: String = trace_id.chars().take(16).collect();
    format!("trace-{prefix}")
}

/// The last text part of a GenAI messages attribute (`gen_ai.input.messages`
/// / `gen_ai.output.messages`: a JSON array of `{role, parts: [{type,
/// content}]}`), keeping only messages of `role` when given; the raw string
/// when it is not JSON or not an array. Mirrors the UI's `messageText`.
fn message_text(raw: &Value, role: Option<&str>) -> Option<String> {
    let parsed;
    let messages = match raw {
        Value::String(s) if s.is_empty() => return None,
        Value::String(s) => match serde_json::from_str::<Value>(s) {
            Ok(Value::Array(messages)) => {
                parsed = messages;
                &parsed
            }
            _ => return Some(s.clone()),
        },
        Value::Array(messages) => messages,
        _ => return None,
    };
    messages
        .iter()
        .filter(|m| role.is_none_or(|r| m.get("role").and_then(Value::as_str) == Some(r)))
        .filter_map(|m| m.get("parts").and_then(Value::as_array))
        .flatten()
        .filter(|p| p.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|p| p.get("content").and_then(Value::as_str))
        .next_back()
        .map(str::to_string)
}

/// One case per sampled trace, in `trace_ids` order, from the spans rows.
/// The agent span is the trace's earliest span of `options.operation`; the
/// expected tools are its `execute_tool` spans in start order. A trace whose
/// agent span holds no user text gets an empty input.
pub(super) fn build_cases(
    rows: &[Row<'_>],
    trace_ids: &[String],
    options: &Options,
) -> Vec<EvalCase> {
    let mut spans: std::collections::HashMap<String, Vec<&Row<'_>>> =
        std::collections::HashMap::new();
    for row in rows {
        if let Some(trace_id) = row.trace_id() {
            spans.entry(trace_id).or_default().push(row);
        }
    }
    for trace in spans.values_mut() {
        trace.sort_by_key(|row| row.i64(START).unwrap_or(i64::MAX));
    }
    trace_ids
        .iter()
        .map(|trace_id| {
            let trace = spans.get(trace_id).map(Vec::as_slice).unwrap_or_default();
            let agent = trace
                .iter()
                .find(|row| row.operation() == Some(options.operation.as_str()));
            let text = |field: &str, role: Option<&str>| {
                agent
                    .and_then(|a| a.get(field))
                    .and_then(|v| message_text(v, role))
            };
            let expected_tools = if options.expected_tools {
                trace
                    .iter()
                    .filter(|row| row.operation() == Some(OPERATION_EXECUTE_TOOL))
                    .filter_map(|row| row.str(TOOL_NAME_COL).or_else(|| row.str(SPAN_NAME_COL)))
                    .map(str::to_string)
                    .collect()
            } else {
                Vec::new()
            };
            EvalCase {
                id: case_id(trace_id),
                input: text(INPUT_MESSAGES_COL, Some("user")).unwrap_or_default(),
                expected_tools,
                reference: if options.reference_from_answer {
                    text(OUTPUT_MESSAGES_COL, None)
                } else {
                    None
                },
                tags: options.tags.clone(),
                source: EvalCaseSource::Trace {
                    trace_id: trace_id.clone(),
                },
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const T1: &str = "11111111111111111111111111111111";
    const T2: &str = "22222222222222222222222222222222";
    const T3: &str = "33333333333333333333333333333333";
    const T4: &str = "44444444444444444444444444444444";

    fn request(body: Value) -> AppendCasesFromTracesRequest {
        serde_json::from_value(body).expect("request parses")
    }

    fn options(body: Value) -> Options {
        Options::new(request(body)).expect("valid options")
    }

    fn base() -> Value {
        json!({"range": {"from": "now-7d", "to": "now"}})
    }

    fn with(extra: Value) -> Value {
        let mut body = base();
        body.as_object_mut()
            .expect("object")
            .extend(extra.as_object().expect("object").clone());
        body
    }

    fn columns(names: &[&str]) -> Vec<ResultColumn> {
        names
            .iter()
            .map(|n| ResultColumn {
                name: n.to_string(),
                value_type: "string".to_string(),
            })
            .collect()
    }

    fn doc_json(doc: &Document) -> Value {
        serde_json::to_value(doc).expect("document serializes")
    }

    #[test]
    fn defaults_apply_and_invalid_options_are_422() {
        let o = options(base());
        assert_eq!(o.agent, None, "the handler fills in the set's agent");
        assert_eq!(o.operation, "invoke_agent");
        assert_eq!(o.sample, 50);
        assert!(!o.expected_tools && !o.reference_from_answer);

        for (extra, why) in [
            (json!({"sample": 0}), "sample 0"),
            (json!({"sample": 1001}), "sample over the cap"),
            (json!({"agent": " "}), "blank agent"),
            (json!({"operation": ""}), "blank operation"),
            (json!({"failing_evaluator": ""}), "blank evaluator"),
            (json!({"tags": ["ok", ""]}), "blank tag"),
        ] {
            let err = Options::new(request(with(extra))).expect_err(why);
            assert_eq!(err.status, StatusCode::UNPROCESSABLE_ENTITY, "{why}");
        }
        assert!(
            serde_json::from_value::<AppendCasesFromTracesRequest>(with(
                json!({"failing_evaluater": "Correctness"})
            ))
            .is_err(),
            "a misspelt option is rejected, not ignored"
        );
        assert!(
            serde_json::from_value::<AppendCasesFromTracesRequest>(with(
                json!({"filters": [{"field": "x", "op": "eq", "value": 1, "column": "y"}]})
            ))
            .is_err(),
            "filters use the IR predicate grammar"
        );
    }

    #[test]
    fn matches_document_groups_agent_spans_by_trace_newest_first() {
        let o = options(with(json!({
            "agent": "billing",
            "filters": [{"field": "deployment.environment", "op": "eq", "value": "prod"}],
        })));
        let doc = matches_document(&o, 100, 200);
        assert_eq!(doc.from, "traces");
        assert_eq!(doc.ir_version, 1);
        assert_eq!(
            doc_json(&doc),
            json!({
                "irVersion": 1,
                "from": "traces",
                "range": {"from": "100", "to": "200"},
                "result": "table",
                "pipeline": [
                    {"where": {"and": [
                        {"field": "gen_ai.operation.name", "op": "eq", "value": "invoke_agent"},
                        {"or": [
                            {"field": "gen_ai.agent.name", "op": "eq", "value": "billing"},
                            {"and": [
                                {"not": {"field": "gen_ai.agent.name", "op": "exists"}},
                                {"field": "service.name", "op": "eq", "value": "billing"},
                            ]},
                        ]},
                        {"field": "deployment.environment", "op": "eq", "value": "prod"},
                    ]}},
                    {"aggregate": {
                        "by": ["trace_id"],
                        "aggs": [{"fn": "max", "of": "start_time_unix_nano", "as": "last_start"}],
                    }},
                    {"order": [{"of": "last_start", "dir": "desc"}]},
                    {"limit": 10_000},
                ],
            })
        );
    }

    #[test]
    fn results_and_spans_documents_read_what_the_cases_need() {
        let doc = results_document("Correctness", 1, 2);
        let v = doc_json(&doc);
        assert_eq!(v["from"], "logs");
        assert_eq!(v["result"], "rows");
        assert_eq!(
            v["fields"],
            json!([
                "trace_id",
                "gen_ai.evaluation.score.label",
                "gen_ai.evaluation.score.value",
                "error.type"
            ])
        );
        let text = v["pipeline"].to_string();
        assert!(text.contains("gen_ai.evaluation.result") && text.contains("Correctness"));
        assert_eq!(v["pipeline"][1], json!({"limit": 50_000}));

        let o = options(base());
        let doc = spans_document(&o, &[T1.to_string(), T2.to_string()], 1, 2);
        let v = doc_json(&doc);
        assert_eq!(v["from"], "traces");
        assert_eq!(
            v["pipeline"][0],
            json!({"where": {"field": "trace_id", "op": "in", "value": [T1, T2]}})
        );
        assert_eq!(
            v["pipeline"][1]["where"]["or"][0],
            json!({"field": "gen_ai.operation.name", "op": "eq", "value": "execute_tool"})
        );
        assert_eq!(v["pipeline"][2], json!({"limit": 100_000}));
        assert!(
            v["fields"]
                .as_array()
                .expect("fields")
                .contains(&json!("gen_ai.input.messages"))
        );
    }

    #[test]
    fn message_text_takes_the_last_text_part_of_the_role() {
        let messages = json!([
            {"role": "system", "parts": [{"type": "text", "content": "be terse"}]},
            {"role": "user", "parts": [{"type": "text", "content": "first"}]},
            {"role": "assistant", "parts": [{"type": "tool_call", "name": "lookup"}]},
            {"role": "user", "parts": [
                {"type": "text", "content": "refund order 1182"},
                {"type": "image", "content": "…"},
            ]},
        ])
        .to_string();
        let raw = Value::String(messages);
        assert_eq!(
            message_text(&raw, Some("user")).as_deref(),
            Some("refund order 1182")
        );
        assert_eq!(
            message_text(&raw, Some("system")).as_deref(),
            Some("be terse")
        );
        assert_eq!(message_text(&raw, Some("tool")), None);
        assert_eq!(
            message_text(&raw, None).as_deref(),
            Some("refund order 1182")
        );
        assert_eq!(
            message_text(&json!("plain question"), Some("user")).as_deref(),
            Some("plain question"),
            "a non-JSON string is the text"
        );
        assert_eq!(
            message_text(&json!("{\"role\": \"user\"}"), None).as_deref(),
            Some("{\"role\": \"user\"}"),
            "JSON that is not an array is kept raw"
        );
        assert_eq!(message_text(&json!(""), None), None);
        assert_eq!(message_text(&Value::Null, None), None);
        assert_eq!(
            message_text(
                &json!([{"role": "user", "parts": [{"type": "text", "content": "x"}]}]),
                None
            )
            .as_deref(),
            Some("x"),
            "an already-decoded array"
        );
    }

    #[test]
    fn candidates_are_newest_first_with_a_stable_tie_break() {
        // The matches document already groups by trace_id with max(start),
        // so one row per trace id is expected; this only exercises the
        // sort, the case fold and the invalid-id filter.
        let cols = columns(&["trace_id", "last_start"]);
        let rows = vec![
            vec![json!(T2), json!(20)],
            vec![json!(T1.to_uppercase()), json!(30)],
            vec![json!(T3), json!(30)],
            vec![json!("not-a-trace-id"), json!(99)],
        ];
        let candidates = decode_candidates(&Row::all(&cols, &rows));
        let ids: Vec<&str> = candidates.iter().map(|c| c.trace_id.as_str()).collect();
        assert_eq!(ids, [T1, T3, T2]);
        assert_eq!(candidates[2].last_start_ns, 20);
    }

    #[test]
    fn failing_traces_follow_the_pass_rule() {
        let cols = columns(&[
            "trace_id",
            "gen_ai_evaluation_score_label",
            "gen_ai_evaluation_score_value",
            "error_type",
        ]);
        let rows = vec![
            vec![json!(T1), json!("FAIL"), Value::Null, Value::Null],
            vec![json!(T2), Value::Null, json!(0.2), Value::Null],
            vec![json!(T3), json!("fail"), json!(0.0), json!("timeout")],
            vec![json!(T4), json!("pass"), json!(0.1), Value::Null],
            vec![json!(T4), json!("partial"), Value::Null, Value::Null],
        ];
        let failing = failing_traces(&Row::all(&cols, &rows));
        assert_eq!(
            failing,
            HashSet::from([T1.to_string(), T2.to_string()]),
            "an evaluator error (T3) is not a failure; a pass label beats a low score (T4)"
        );
    }

    fn candidates(ids: &[&str]) -> Vec<Candidate> {
        ids.iter()
            .enumerate()
            .map(|(i, id)| Candidate {
                trace_id: id.to_string(),
                last_start_ns: 100 - i as i64,
            })
            .collect()
    }

    #[test]
    fn selection_skips_present_traces_and_caps_at_the_sample() {
        let all = candidates(&[T1, T2, T3, T4]);
        let present = HashSet::from([T2.to_string()]);
        let s = select(&all, None, &present, 2);
        assert_eq!(s.matches, 4);
        assert_eq!(s.already_present, 1);
        assert_eq!(s.trace_ids, [T1, T3]);

        let failing = HashSet::from([T2.to_string(), T4.to_string()]);
        let s = select(&all, Some(&failing), &present, 50);
        assert_eq!(s.matches, 2, "only failing traces match");
        assert_eq!(s.already_present, 1);
        assert_eq!(s.trace_ids, [T4]);

        let s = select(&all, Some(&HashSet::new()), &present, 50);
        assert_eq!((s.matches, s.already_present), (0, 0));
        assert!(s.trace_ids.is_empty());
    }

    #[test]
    fn cases_take_input_tools_and_answer_from_the_trace() {
        let cols = columns(&[
            "trace_id",
            "start_time_unix_nano",
            "span_name",
            "gen_ai_operation_name",
            "gen_ai_tool_name",
            "gen_ai_input_messages",
            "gen_ai_output_messages",
        ]);
        let input =
            json!([{"role": "user", "parts": [{"type": "text", "content": "refund 1182"}]}])
                .to_string();
        let output =
            json!([{"role": "assistant", "parts": [{"type": "text", "content": "refund issued"}]}])
                .to_string();
        let rows = vec![
            vec![
                json!(T1),
                json!(30),
                json!("execute_tool issue_refund"),
                json!("execute_tool"),
                json!("issue_refund"),
                Value::Null,
                Value::Null,
            ],
            vec![
                json!(T1),
                json!(10),
                json!("invoke_agent"),
                json!("invoke_agent"),
                Value::Null,
                json!(input),
                json!(output),
            ],
            vec![
                json!(T1),
                json!(20),
                json!("lookup_order"),
                json!("execute_tool"),
                Value::Null,
                Value::Null,
                Value::Null,
            ],
            vec![
                json!(T2),
                json!(5),
                json!("invoke_agent"),
                json!("invoke_agent"),
                Value::Null,
                json!("plain"),
                Value::Null,
            ],
        ];
        let rows = Row::all(&cols, &rows);
        let ids = [T1.to_string(), T2.to_string(), T3.to_string()];

        let o = options(with(
            json!({"expected_tools": true, "reference_from_answer": true, "tags": ["prod"]}),
        ));
        let cases = build_cases(&rows, &ids, &o);
        assert_eq!(cases.len(), 3);
        assert_eq!(cases[0].id, "trace-1111111111111111");
        assert_eq!(cases[0].input, "refund 1182");
        assert_eq!(
            cases[0].expected_tools,
            ["lookup_order", "issue_refund"],
            "start order, span name fallback"
        );
        assert_eq!(cases[0].reference.as_deref(), Some("refund issued"));
        assert_eq!(cases[0].tags, ["prod"]);
        assert_eq!(
            cases[0].source,
            EvalCaseSource::Trace {
                trace_id: T1.to_string()
            }
        );
        assert_eq!(cases[1].input, "plain");
        assert_eq!(cases[1].reference, None);
        assert_eq!(cases[2].input, "", "no spans found: empty input");

        let o = options(base());
        let cases = build_cases(&rows, &ids[..1], &o);
        assert!(cases[0].expected_tools.is_empty());
        assert_eq!(cases[0].reference, None);
    }

    #[test]
    fn case_ids_are_stable_per_trace() {
        assert_eq!(case_id(T1), case_id(T1));
        assert_ne!(case_id(T1), case_id(T2));
        assert_eq!(
            case_id("4bf92f3577b34da6a3ce929d0e0e4736"),
            "trace-4bf92f3577b34da6"
        );
    }
}
