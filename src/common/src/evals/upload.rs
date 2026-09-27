//! # Eval results upload: parsing, summary and conversion
//!
//! Reads a JSONL or CSV results file, validates every row, summarizes it per
//! evaluator and builds the `gen_ai.evaluation.result` OTLP logs export the
//! ingest path writes (change: agent-offline-evals; docs/users/evaluations.md).

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::str::FromStr;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, InstrumentationScope, KeyValue, any_value::Value,
};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use serde::{Deserialize, Serialize};

use super::{
    AGENT_NAME, AGENT_VERSION, CASE_ID, ERROR_TYPE, EVALUATION_EXPLANATION, EVALUATION_NAME,
    EVALUATION_RESULT_EVENT, EVALUATION_SCORE_LABEL, EVALUATION_SCORE_VALUE, EVALUATOR, EvalResult,
    RUN_ID, SET, TRIAL, Verdict, verdict_of_result,
};

/// Most result rows one file may hold.
pub const MAX_ROWS: usize = 100_000;
/// Most problems a rejected file lists; the total is still counted.
pub const MAX_LISTED_ERRORS: usize = 100;
/// Longest case id, as for eval set cases.
const MAX_CASE_ID_LEN: usize = 128;
/// Longest agent, version, run id or evaluator name.
const MAX_NAME_LEN: usize = 256;

/// Instrumentation scope of the uploaded records.
const SCOPE_NAME: &str = "signaldb.evals.upload";
const SEVERITY_INFO: i32 = 9;

const REQUIRED_COLUMNS: [&str; 2] = ["case_id", "name"];
const OPTIONAL_COLUMNS: [&str; 8] = [
    "score",
    "label",
    "explanation",
    "trace_id",
    "span_id",
    "evaluator",
    "error",
    "trial",
];

/// The format of an uploaded results file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
#[schema(as = EvalResultsFormat)]
pub enum ResultsFormat {
    Csv,
    Jsonl,
}

impl ResultsFormat {
    /// The format a request `Content-Type` names: `text/csv`, or
    /// `application/x-ndjson` / `application/jsonl` for JSONL. Any other
    /// type (including `text/plain` and `application/octet-stream`) names
    /// none.
    pub fn from_content_type(content_type: &str) -> Option<Self> {
        let media_type = content_type
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        match media_type.as_str() {
            "text/csv" => Some(Self::Csv),
            "application/x-ndjson" | "application/jsonl" | "application/x-jsonlines" => {
                Some(Self::Jsonl)
            }
            _ => None,
        }
    }
}

impl FromStr for ResultsFormat {
    type Err = String;

    /// `csv`, or `jsonl` (alias `ndjson`), case-insensitive.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "csv" => Ok(Self::Csv),
            "jsonl" | "ndjson" => Ok(Self::Jsonl),
            other => Err(format!(
                "unknown format `{other}`; expected `csv` or `jsonl`"
            )),
        }
    }
}

/// The run an upload belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunMetadata {
    pub agent: String,
    pub version: String,
    /// Eval set name; the set need not exist.
    pub set: String,
    pub run_id: String,
}

impl RunMetadata {
    /// Validates the run metadata; a missing `run_id` is generated (UUID v4).
    pub fn new(
        agent: &str,
        version: &str,
        set: &str,
        run_id: Option<&str>,
    ) -> Result<Self, String> {
        let agent = required_name("agent", agent)?;
        let version = required_name("version", version)?;
        crate::eval_sets::validate_name(set).map_err(|_| {
            format!("`set`: `{set}` is not a valid eval set name ([a-z0-9][a-z0-9._-]{{0,127}})")
        })?;
        let run_id = match run_id.map(str::trim).filter(|id| !id.is_empty()) {
            Some(id) => required_name("run_id", id)?,
            None => uuid::Uuid::new_v4().to_string(),
        };
        Ok(Self {
            agent,
            version,
            set: set.to_string(),
            run_id,
        })
    }
}

fn required_name(field: &str, value: &str) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(format!("`{field}` must not be empty"));
    }
    if value.len() > MAX_NAME_LEN {
        return Err(format!("`{field}` must be at most {MAX_NAME_LEN} bytes"));
    }
    Ok(value.to_string())
}

/// One evaluator result read from the file.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResultRow {
    pub case_id: String,
    /// The evaluator (`gen_ai.evaluation.name`).
    pub name: String,
    pub score: Option<f64>,
    pub label: Option<String>,
    pub explanation: Option<String>,
    /// Lower-case hex; `None` for a run-level result.
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    /// Evaluator implementation and version.
    pub evaluator: Option<String>,
    /// `error.type`: the evaluator itself failed.
    pub error: Option<String>,
    pub trial: Option<i64>,
}

/// One problem with an uploaded file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowError {
    /// 1-based line of the file the row starts on (a CSV header is line 1);
    /// `None` for a problem with the file as a whole.
    pub row: Option<u64>,
    pub column: Option<String>,
    pub reason: String,
}

impl RowError {
    fn file(column: Option<&str>, reason: impl Into<String>) -> Self {
        Self {
            row: None,
            column: column.map(str::to_string),
            reason: reason.into(),
        }
    }
}

impl fmt::Display for RowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.row, &self.column) {
            (Some(row), Some(column)) => write!(f, "row {row}, `{column}`: {}", self.reason),
            (Some(row), None) => write!(f, "row {row}: {}", self.reason),
            (None, _) => f.write_str(&self.reason),
        }
    }
}

/// Why a file was rejected: every problem found (at most
/// [`MAX_LISTED_ERRORS`] listed) and how many there were in all.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{}", self.summary())]
pub struct FileErrors {
    pub total: usize,
    pub errors: Vec<RowError>,
}

impl FileErrors {
    fn one(error: RowError) -> Self {
        Self {
            total: 1,
            errors: vec![error],
        }
    }

    fn summary(&self) -> String {
        let first = self
            .errors
            .first()
            .map(ToString::to_string)
            .unwrap_or_default();
        match self.total {
            0 | 1 => format!("invalid results file: {first}"),
            total if total > self.errors.len() => format!(
                "invalid results file: {total} problems ({} listed), first: {first}",
                self.errors.len()
            ),
            total => format!("invalid results file: {total} problems, first: {first}"),
        }
    }
}

/// Collects row errors up to the listing cap while counting all of them.
#[derive(Default)]
struct Problems {
    total: usize,
    listed: Vec<RowError>,
}

impl Problems {
    fn push(&mut self, row: u64, column: Option<&str>, reason: impl Into<String>) {
        self.total += 1;
        if self.listed.len() < MAX_LISTED_ERRORS {
            self.listed.push(RowError {
                row: Some(row),
                column: column.map(str::to_string),
                reason: reason.into(),
            });
        }
    }

    fn into_result(self, rows: Vec<ResultRow>) -> Result<Vec<ResultRow>, FileErrors> {
        if self.total > 0 {
            return Err(FileErrors {
                total: self.total,
                errors: self.listed,
            });
        }
        if rows.is_empty() {
            return Err(FileErrors::one(RowError::file(
                None,
                "the file holds no results",
            )));
        }
        Ok(rows)
    }
}

/// Parses and validates a whole results file.
pub fn parse_results(text: &str, format: ResultsFormat) -> Result<Vec<ResultRow>, FileErrors> {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    match format {
        ResultsFormat::Csv => parse_csv(text),
        ResultsFormat::Jsonl => parse_jsonl(text),
    }
}

fn too_many_rows() -> FileErrors {
    FileErrors::one(RowError::file(
        None,
        format!("the file holds more than {MAX_ROWS} results; split it into several uploads"),
    ))
}

fn parse_jsonl(text: &str) -> Result<Vec<ResultRow>, FileErrors> {
    let mut problems = Problems::default();
    let mut rows = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        if rows.len() + problems.total >= MAX_ROWS {
            return Err(too_many_rows());
        }
        let row_no = index as u64 + 1;
        let object = match serde_json::from_str::<serde_json::Value>(line) {
            Ok(serde_json::Value::Object(object)) => object,
            Ok(_) => {
                problems.push(row_no, None, "not a JSON object");
                continue;
            }
            Err(e) => {
                problems.push(row_no, None, format!("not valid JSON: {e}"));
                continue;
            }
        };
        let cell = |column: &str| match object.get(column) {
            None | Some(serde_json::Value::Null) => Cell::Absent,
            Some(value) => Cell::Json(value),
        };
        if let Some(row) = read_row(row_no, cell, &mut problems) {
            rows.push(row);
        }
    }
    problems.into_result(rows)
}

fn parse_csv(text: &str) -> Result<Vec<ResultRow>, FileErrors> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(text.as_bytes());
    let headers: Vec<String> = match reader.headers() {
        Ok(headers) => headers
            .iter()
            .map(|h| h.trim().to_ascii_lowercase())
            .collect(),
        Err(e) => {
            return Err(FileErrors::one(RowError::file(
                None,
                format!("unreadable CSV header: {e}"),
            )));
        }
    };
    if headers.iter().all(String::is_empty) {
        return Err(FileErrors::one(RowError::file(
            None,
            "the file holds no results",
        )));
    }
    let mut header_errors = Vec::new();
    let mut seen = HashSet::new();
    for header in &headers {
        if !header.is_empty() && !seen.insert(header.as_str()) {
            header_errors.push(RowError::file(
                Some(header),
                format!("column `{header}` appears more than once"),
            ));
        }
    }
    for column in REQUIRED_COLUMNS {
        if !seen.contains(column) {
            header_errors.push(RowError::file(
                Some(column),
                format!("missing required column `{column}`"),
            ));
        }
    }
    if !header_errors.is_empty() {
        return Err(FileErrors {
            total: header_errors.len(),
            errors: header_errors,
        });
    }
    let index_of = |column: &str| headers.iter().position(|h| h == column);
    let positions: BTreeMap<&str, usize> = REQUIRED_COLUMNS
        .iter()
        .chain(OPTIONAL_COLUMNS.iter())
        .filter_map(|column| index_of(column).map(|i| (*column, i)))
        .collect();

    let mut problems = Problems::default();
    let mut rows = Vec::new();
    for record in reader.records() {
        if rows.len() + problems.total >= MAX_ROWS {
            return Err(too_many_rows());
        }
        let record = match record {
            Ok(record) => record,
            Err(e) => {
                let row_no = e.position().map_or(0, |p| p.line());
                let reason = match e.kind() {
                    csv::ErrorKind::UnequalLengths {
                        expected_len, len, ..
                    } => format!("has {len} fields, the header has {expected_len}"),
                    _ => format!("unreadable CSV row: {e}"),
                };
                problems.push(row_no, None, reason);
                continue;
            }
        };
        let row_no = record.position().map_or(0, |p| p.line());
        let cell = |column: &str| match positions.get(column).and_then(|&i| record.get(i)) {
            Some(value) if !value.trim().is_empty() => Cell::Text(value.trim()),
            _ => Cell::Absent,
        };
        if let Some(row) = read_row(row_no, cell, &mut problems) {
            rows.push(row);
        }
    }
    problems.into_result(rows)
}

/// One field of a row: absent (missing, empty or `null`), CSV text or a
/// JSON value.
enum Cell<'a> {
    Absent,
    Text(&'a str),
    Json(&'a serde_json::Value),
}

impl Cell<'_> {
    /// Text of a string, number or boolean field.
    fn text(&self) -> Result<Option<String>, &'static str> {
        match self {
            Cell::Absent => Ok(None),
            Cell::Text(s) => Ok(Some((*s).to_string())),
            Cell::Json(serde_json::Value::String(s)) if s.trim().is_empty() => Ok(None),
            Cell::Json(serde_json::Value::String(s)) => Ok(Some(s.trim().to_string())),
            Cell::Json(serde_json::Value::Number(n)) => Ok(Some(n.to_string())),
            Cell::Json(serde_json::Value::Bool(b)) => Ok(Some(b.to_string())),
            Cell::Json(_) => Err("must be a string"),
        }
    }

    fn number(&self) -> Result<Option<f64>, &'static str> {
        let value = match self {
            Cell::Json(serde_json::Value::Number(n)) => n.as_f64(),
            Cell::Json(serde_json::Value::Bool(_))
            | Cell::Json(serde_json::Value::Array(_))
            | Cell::Json(serde_json::Value::Object(_)) => return Err("must be a number"),
            _ => match self.text()? {
                None => return Ok(None),
                Some(s) => s.parse::<f64>().ok(),
            },
        };
        match value {
            Some(v) if v.is_finite() => Ok(Some(v)),
            _ => Err("must be a finite number"),
        }
    }

    fn integer(&self) -> Result<Option<i64>, &'static str> {
        let value = match self {
            Cell::Json(serde_json::Value::Number(n)) => n.as_i64(),
            Cell::Json(serde_json::Value::String(_)) | Cell::Text(_) => match self.text()? {
                None => return Ok(None),
                Some(s) => s.parse::<i64>().ok(),
            },
            Cell::Absent => return Ok(None),
            Cell::Json(_) => None,
        };
        match value {
            Some(v) if v >= 0 => Ok(Some(v)),
            _ => Err("must be a non-negative integer"),
        }
    }
}

/// Reads and validates one row, recording its problems; `None` when it has
/// any.
fn read_row<'a>(
    row_no: u64,
    cell: impl Fn(&str) -> Cell<'a>,
    problems: &mut Problems,
) -> Option<ResultRow> {
    let before = problems.total;
    let text = |column: &str| cell(column).text().map_err(String::from);
    let required = |column: &str, max: usize| match text(column)? {
        None => Err("required".to_string()),
        Some(v) if v.len() > max => Err(format!("must be at most {max} bytes")),
        Some(v) => Ok(Some(v)),
    };
    let hex = |column: &str, len: usize| {
        text(column).and_then(|id| id.map(|id| hex_id(&id, len)).transpose())
    };

    let case_id = required("case_id", MAX_CASE_ID_LEN);
    let name = required("name", MAX_NAME_LEN);
    let score = cell("score").number().map_err(String::from);
    let label = text("label");
    let error = text("error");
    let trace_id = hex("trace_id", 32);
    let span_id = hex("span_id", 16);
    // Cross-field rules only apply to fields that are themselves valid.
    let span_without_trace = matches!((&trace_id, &span_id), (Ok(None), Ok(Some(_))));
    let no_outcome = matches!((&score, &label, &error), (Ok(None), Ok(None), Ok(None)));

    // Each field's value, or `None` with its problem recorded.
    let mut take = |column: &str, value: Result<Option<String>, String>| {
        value.unwrap_or_else(|reason| {
            problems.push(row_no, Some(column), reason);
            None
        })
    };
    let case_id = take("case_id", case_id);
    let name = take("name", name);
    let label = take("label", label);
    let error = take("error", error);
    let explanation = take("explanation", text("explanation"));
    let evaluator = take("evaluator", text("evaluator"));
    let trace_id = take("trace_id", trace_id);
    let span_id = take("span_id", span_id);
    let score = score.unwrap_or_else(|reason| {
        problems.push(row_no, Some("score"), reason);
        None
    });
    let trial = cell("trial").integer().unwrap_or_else(|reason| {
        problems.push(row_no, Some("trial"), reason);
        None
    });
    if span_without_trace {
        problems.push(row_no, Some("span_id"), "needs a trace_id");
    }
    if no_outcome {
        problems.push(row_no, None, "needs a score, a label or an error");
    }

    if problems.total > before {
        return None;
    }
    Some(ResultRow {
        case_id: case_id?,
        name: name?,
        score,
        label,
        explanation,
        trace_id,
        span_id,
        evaluator,
        error,
        trial,
    })
}

/// Validates a W3C id of `len` hex digits and lower-cases it.
fn hex_id(id: &str, len: usize) -> Result<String, String> {
    if id.len() != len || !id.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(format!("must be {len} hex characters"));
    }
    if id.chars().all(|c| c == '0') {
        return Err("must not be all zeros".to_string());
    }
    Ok(id.to_ascii_lowercase())
}

/// Figures for one evaluator of an upload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct EvaluatorSummary {
    /// `gen_ai.evaluation.name`.
    pub name: String,
    /// Result rows, evaluator errors included.
    pub results: u64,
    /// Rows with an `error`: never counted as failures.
    pub errors: u64,
    /// Mean score over the rows with a score and no error; `null` when none.
    pub mean: Option<f64>,
    /// Passes / (passes + fails) under the pass rule; `null` when no row
    /// has a verdict.
    pub pass_rate: Option<f64>,
}

/// What an upload holds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct UploadSummary {
    /// Result rows written.
    pub rows: u64,
    /// Distinct case ids.
    pub cases: u64,
    /// Rows with a `trace_id`.
    pub span_linked: u64,
    /// Rows without a `trace_id` (run-level results).
    pub run_level: u64,
    /// Per evaluator, ordered by name.
    pub evaluators: Vec<EvaluatorSummary>,
}

/// Summarizes validated rows with the shared pass rule.
pub fn summarize(rows: &[ResultRow]) -> UploadSummary {
    #[derive(Default)]
    struct Acc {
        results: u64,
        errors: u64,
        score_sum: f64,
        scored: u64,
        passes: u64,
        fails: u64,
    }
    let mut by_name: BTreeMap<&str, Acc> = BTreeMap::new();
    for row in rows {
        let acc = by_name.entry(row.name.as_str()).or_default();
        acc.results += 1;
        let is_error = row.error.as_deref().is_some_and(|e| !e.is_empty());
        if is_error {
            acc.errors += 1;
        } else if let Some(score) = row.score {
            acc.score_sum += score;
            acc.scored += 1;
        }
        match verdict_of_result(EvalResult {
            error: row.error.as_deref(),
            label: row.label.as_deref(),
            score: row.score,
        }) {
            Some(Verdict::Pass) => acc.passes += 1,
            Some(Verdict::Fail) => acc.fails += 1,
            None => {}
        }
    }
    let span_linked = rows.iter().filter(|r| r.trace_id.is_some()).count() as u64;
    let cases: HashSet<&str> = rows.iter().map(|r| r.case_id.as_str()).collect();
    UploadSummary {
        rows: rows.len() as u64,
        cases: cases.len() as u64,
        span_linked,
        run_level: rows.len() as u64 - span_linked,
        evaluators: by_name
            .into_iter()
            .map(|(name, acc)| EvaluatorSummary {
                name: name.to_string(),
                results: acc.results,
                errors: acc.errors,
                mean: (acc.scored > 0).then(|| acc.score_sum / acc.scored as f64),
                pass_rate: (acc.passes + acc.fails > 0)
                    .then(|| acc.passes as f64 / (acc.passes + acc.fails) as f64),
            })
            .collect(),
    }
}

fn string_attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

fn any_attr(key: &str, value: Value) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue { value: Some(value) }),
        ..Default::default()
    }
}

/// Builds the OTLP logs export for an upload: one `gen_ai.evaluation.result`
/// record per row, under a resource naming the agent (`service.name`,
/// `service.version`). Row `i` is stamped `time_unix_nano + i`.
pub fn to_logs_request(
    rows: &[ResultRow],
    run: &RunMetadata,
    time_unix_nano: u64,
) -> ExportLogsServiceRequest {
    let log_records = rows
        .iter()
        .enumerate()
        .map(|(i, row)| {
            let mut attributes = vec![
                string_attr(EVALUATION_NAME, &row.name),
                string_attr(CASE_ID, &row.case_id),
                string_attr(RUN_ID, &run.run_id),
                string_attr(SET, &run.set),
                string_attr(AGENT_NAME, &run.agent),
                string_attr(AGENT_VERSION, &run.version),
            ];
            if let Some(score) = row.score {
                attributes.push(any_attr(EVALUATION_SCORE_VALUE, Value::DoubleValue(score)));
            }
            for (key, value) in [
                (EVALUATION_SCORE_LABEL, &row.label),
                (EVALUATION_EXPLANATION, &row.explanation),
                (EVALUATOR, &row.evaluator),
                (ERROR_TYPE, &row.error),
            ] {
                if let Some(value) = value {
                    attributes.push(string_attr(key, value));
                }
            }
            if let Some(trial) = row.trial {
                attributes.push(any_attr(TRIAL, Value::IntValue(trial)));
            }
            let time = time_unix_nano.saturating_add(i as u64);
            LogRecord {
                time_unix_nano: time,
                observed_time_unix_nano: time,
                severity_number: SEVERITY_INFO,
                severity_text: "INFO".to_string(),
                attributes,
                trace_id: decode_hex(row.trace_id.as_deref()),
                span_id: decode_hex(row.span_id.as_deref()),
                event_name: EVALUATION_RESULT_EVENT.to_string(),
                ..Default::default()
            }
        })
        .collect();
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    string_attr("service.name", &run.agent),
                    string_attr("service.version", &run.version),
                ],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: SCOPE_NAME.to_string(),
                    ..Default::default()
                }),
                log_records,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// Bytes of a validated hex id; empty when absent.
fn decode_hex(id: Option<&str>) -> Vec<u8> {
    id.and_then(|id| hex::decode(id).ok()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    const TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
    const SPAN: &str = "00f067aa0ba902b7";

    fn run() -> RunMetadata {
        RunMetadata::new("support-triage", "v1.9.0", "triage-golden", Some("run-1"))
            .expect("valid run")
    }

    fn errors(result: Result<Vec<ResultRow>, FileErrors>) -> FileErrors {
        result.expect_err("file must be rejected")
    }

    #[test]
    fn csv_rows_parse_with_optional_columns_and_run_level_rows() {
        let csv = format!(
            "case_id,name,score,label,explanation,trace_id,span_id,evaluator,error,trial\n\
             case-1,Correctness,0.9,pass,\"Looks right, cites order\",{},{SPAN},judge@1,,0\n\
             case-1,Groundedness,0.4,,,,,,,\n\
             case-2,Correctness,,,,,,,timeout,\n",
            TRACE.to_uppercase()
        );
        let rows = parse_results(&csv, ResultsFormat::Csv).expect("valid csv");
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows[0],
            ResultRow {
                case_id: "case-1".into(),
                name: "Correctness".into(),
                score: Some(0.9),
                label: Some("pass".into()),
                explanation: Some("Looks right, cites order".into()),
                trace_id: Some(TRACE.into()),
                span_id: Some(SPAN.into()),
                evaluator: Some("judge@1".into()),
                error: None,
                trial: Some(0),
            }
        );
        assert_eq!(rows[1].trace_id, None, "run-level row");
        assert_eq!(rows[2].error.as_deref(), Some("timeout"));
    }

    #[test]
    fn csv_headers_are_trimmed_case_insensitive_and_extra_columns_ignored() {
        let csv = "\u{feff} Case_ID , NAME ,score,notes\ncase-1,Correctness,1,ignored\n";
        let rows = parse_results(csv, ResultsFormat::Csv).expect("valid csv");
        assert_eq!(rows[0].case_id, "case-1");
        assert_eq!(rows[0].score, Some(1.0));
    }

    #[test]
    fn a_missing_required_csv_column_is_one_file_level_error_naming_it() {
        let err = errors(parse_results(
            "name,score\nCorrectness,0.5\n",
            ResultsFormat::Csv,
        ));
        assert_eq!(err.total, 1);
        assert_eq!(
            err.errors[0],
            RowError {
                row: None,
                column: Some("case_id".into()),
                reason: "missing required column `case_id`".into(),
            }
        );
        assert!(err.to_string().contains("case_id"), "{err}");
    }

    #[test]
    fn every_bad_row_is_reported_with_its_line_and_column() {
        let csv = "case_id,name,score,trace_id,span_id,trial\n\
                   case-1,Correctness,high,,,\n\
                   ,Correctness,0.5,,,\n\
                   case-3,Correctness,0.5,xyz,,\n\
                   case-4,Correctness,0.5,,00f067aa0ba902b7,\n\
                   case-5,Correctness,0.5,,,-1\n\
                   case-6,Correctness,,,,\n\
                   case-7,Correctness,0.5\n";
        let err = errors(parse_results(csv, ResultsFormat::Csv));
        let found: Vec<(Option<u64>, Option<&str>)> = err
            .errors
            .iter()
            .map(|e| (e.row, e.column.as_deref()))
            .collect();
        assert_eq!(
            found,
            [
                (Some(2), Some("score")),
                (Some(3), Some("case_id")),
                (Some(4), Some("trace_id")),
                (Some(5), Some("span_id")),
                (Some(6), Some("trial")),
                (Some(7), None),
                (Some(8), None),
            ],
            "{err:?}"
        );
        assert_eq!(err.total, 7);
    }

    #[test]
    fn listed_errors_are_capped_but_all_are_counted() {
        let mut csv = String::from("case_id,name,score\n");
        for i in 0..(MAX_LISTED_ERRORS + 20) {
            csv.push_str(&format!("case-{i},Correctness,nope\n"));
        }
        let err = errors(parse_results(&csv, ResultsFormat::Csv));
        assert_eq!(err.total, MAX_LISTED_ERRORS + 20);
        assert_eq!(err.errors.len(), MAX_LISTED_ERRORS);
        assert!(err.to_string().contains("(100 listed)"), "{err}");
    }

    #[test]
    fn jsonl_rows_parse_and_bad_lines_are_reported_by_line() {
        let jsonl = format!(
            "{{\"case_id\": \"case-1\", \"name\": \"Correctness\", \"score\": 1, \"trace_id\": \"{TRACE}\"}}\n\
             \n\
             {{\"case_id\": 17, \"name\": \"Safety\", \"label\": true, \"trial\": 2, \"explanation\": null}}\n"
        );
        let rows = parse_results(&jsonl, ResultsFormat::Jsonl).expect("valid jsonl");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].trace_id.as_deref(), Some(TRACE));
        assert_eq!(rows[1].case_id, "17");
        assert_eq!(rows[1].label.as_deref(), Some("true"));
        assert_eq!(rows[1].trial, Some(2));

        let bad = "{\"case_id\": \"a\", \"name\": \"C\", \"score\": \"x\"}\n\
                   [1, 2]\n\
                   {not json\n\
                   {\"case_id\": \"b\", \"name\": [\"C\"], \"score\": 1}\n\
                   {\"case_id\": \"c\", \"name\": \"C\", \"score\": true}\n";
        let err = errors(parse_results(bad, ResultsFormat::Jsonl));
        let found: Vec<(Option<u64>, Option<&str>)> = err
            .errors
            .iter()
            .map(|e| (e.row, e.column.as_deref()))
            .collect();
        assert_eq!(
            found,
            [
                (Some(1), Some("score")),
                (Some(2), None),
                (Some(3), None),
                (Some(4), Some("name")),
                (Some(5), Some("score")),
            ],
            "{err:?}"
        );
    }

    #[test]
    fn an_empty_file_is_rejected() {
        for (text, format) in [
            ("", ResultsFormat::Csv),
            ("case_id,name\n", ResultsFormat::Csv),
            ("\n  \n", ResultsFormat::Jsonl),
        ] {
            let err = errors(parse_results(text, format));
            assert!(err.to_string().contains("no results"), "{text:?}: {err}");
        }
    }

    #[test]
    fn a_file_over_the_row_cap_is_rejected() {
        let mut jsonl = String::with_capacity(MAX_ROWS * 40);
        for _ in 0..=MAX_ROWS {
            jsonl.push_str("{\"case_id\":\"c\",\"name\":\"C\",\"score\":1}\n");
        }
        let err = errors(parse_results(&jsonl, ResultsFormat::Jsonl));
        assert!(err.to_string().contains("more than 100000"), "{err}");
    }

    #[test]
    fn formats_come_from_a_name_or_a_content_type() {
        assert_eq!("CSV".parse(), Ok(ResultsFormat::Csv));
        assert_eq!("ndjson".parse(), Ok(ResultsFormat::Jsonl));
        assert!("xlsx".parse::<ResultsFormat>().is_err());
        assert_eq!(
            ResultsFormat::from_content_type("text/csv; charset=utf-8"),
            Some(ResultsFormat::Csv)
        );
        assert_eq!(
            ResultsFormat::from_content_type("application/x-ndjson"),
            Some(ResultsFormat::Jsonl)
        );
        assert_eq!(ResultsFormat::from_content_type("text/plain"), None);
    }

    #[test]
    fn run_metadata_is_validated_and_a_missing_run_id_generated() {
        let generated = RunMetadata::new("agent", "v1", "golden-200", None).expect("valid");
        assert!(uuid::Uuid::parse_str(&generated.run_id).is_ok());
        assert_ne!(
            generated.run_id,
            RunMetadata::new("agent", "v1", "golden-200", Some("  "))
                .expect("blank run id is generated")
                .run_id
        );
        assert!(RunMetadata::new(" ", "v1", "golden", None).is_err());
        assert!(RunMetadata::new("agent", "", "golden", None).is_err());
        let err = RunMetadata::new("agent", "v1", "Golden Set", None).expect_err("bad set");
        assert!(err.contains("`set`"), "{err}");
    }

    #[test]
    fn summary_applies_the_pass_rule_and_leaves_errors_out() {
        let row = |name: &str,
                   case: &str,
                   score: Option<f64>,
                   label: Option<&str>,
                   error: Option<&str>,
                   trace: bool| ResultRow {
            case_id: case.into(),
            name: name.into(),
            score,
            label: label.map(Into::into),
            error: error.map(Into::into),
            trace_id: trace.then(|| TRACE.to_string()),
            ..Default::default()
        };
        let rows = vec![
            row("Correctness", "c1", Some(0.9), None, None, true),
            row("Correctness", "c2", Some(0.33), Some("pass"), None, true),
            row("Correctness", "c3", Some(0.2), None, None, true),
            row("Correctness", "c4", Some(0.0), None, Some("timeout"), true),
            row("Tone", "c1", None, Some("friendly"), None, false),
        ];
        let summary = summarize(&rows);
        assert_eq!(summary.rows, 5);
        assert_eq!(summary.cases, 4);
        assert_eq!(summary.span_linked, 4);
        assert_eq!(summary.run_level, 1);
        let correctness = &summary.evaluators[0];
        assert_eq!(correctness.name, "Correctness");
        assert_eq!((correctness.results, correctness.errors), (4, 1));
        let mean = correctness.mean.expect("mean");
        assert!((mean - (0.9 + 0.33 + 0.2) / 3.0).abs() < 1e-9, "{mean}");
        let pass_rate = correctness.pass_rate.expect("pass rate");
        assert!((pass_rate - 2.0 / 3.0).abs() < 1e-9, "{pass_rate}");
        let tone = &summary.evaluators[1];
        assert_eq!((tone.mean, tone.pass_rate), (None, None));
    }

    fn attr<'a>(record: &'a LogRecord, key: &str) -> Option<&'a Value> {
        record
            .attributes
            .iter()
            .find(|kv| kv.key == key)
            .and_then(|kv| kv.value.as_ref())
            .and_then(|v| v.value.as_ref())
    }

    fn string<'a>(record: &'a LogRecord, key: &str) -> Option<&'a str> {
        match attr(record, key) {
            Some(Value::StringValue(s)) => Some(s),
            _ => None,
        }
    }

    #[test]
    fn rows_become_evaluation_result_records_with_the_run_attributes() {
        let rows = vec![
            ResultRow {
                case_id: "case-1".into(),
                name: "Correctness".into(),
                score: Some(0.9),
                label: Some("pass".into()),
                explanation: Some("fine".into()),
                trace_id: Some(TRACE.into()),
                span_id: Some(SPAN.into()),
                evaluator: Some("judge@1".into()),
                error: None,
                trial: Some(3),
            },
            ResultRow {
                case_id: "case-2".into(),
                name: "Correctness".into(),
                error: Some("timeout".into()),
                ..Default::default()
            },
        ];
        let request = to_logs_request(&rows, &run(), 1_000);
        let resource_logs = &request.resource_logs[0];
        let resource = resource_logs.resource.as_ref().expect("resource");
        let service: Vec<(&str, &str)> = resource
            .attributes
            .iter()
            .filter_map(
                |kv| match kv.value.as_ref().and_then(|v| v.value.as_ref()) {
                    Some(Value::StringValue(s)) => Some((kv.key.as_str(), s.as_str())),
                    _ => None,
                },
            )
            .collect();
        assert_eq!(
            service,
            [
                ("service.name", "support-triage"),
                ("service.version", "v1.9.0")
            ]
        );
        let records = &resource_logs.scope_logs[0].log_records;
        assert_eq!(records.len(), 2);

        let first = &records[0];
        assert_eq!(first.event_name, EVALUATION_RESULT_EVENT);
        assert_eq!(first.time_unix_nano, 1_000);
        assert_eq!(hex::encode(&first.trace_id), TRACE);
        assert_eq!(hex::encode(&first.span_id), SPAN);
        for (key, value) in [
            (EVALUATION_NAME, "Correctness"),
            (EVALUATION_SCORE_LABEL, "pass"),
            (EVALUATION_EXPLANATION, "fine"),
            (RUN_ID, "run-1"),
            (SET, "triage-golden"),
            (CASE_ID, "case-1"),
            (EVALUATOR, "judge@1"),
            (AGENT_NAME, "support-triage"),
            (AGENT_VERSION, "v1.9.0"),
        ] {
            assert_eq!(string(first, key), Some(value), "{key}");
        }
        assert_eq!(
            attr(first, EVALUATION_SCORE_VALUE),
            Some(&Value::DoubleValue(0.9))
        );
        assert_eq!(attr(first, TRIAL), Some(&Value::IntValue(3)));
        assert_eq!(string(first, ERROR_TYPE), None);

        let second = &records[1];
        assert_eq!(second.time_unix_nano, 1_001, "rows keep file order");
        assert!(second.trace_id.is_empty() && second.span_id.is_empty());
        assert_eq!(string(second, ERROR_TYPE), Some("timeout"));
        assert_eq!(attr(second, EVALUATION_SCORE_VALUE), None);
    }
}
