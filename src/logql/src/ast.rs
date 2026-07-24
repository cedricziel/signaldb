//! # LogQL AST
//!
//! Abstract syntax tree for parsed LogQL queries. The tree mirrors the
//! shape of the language: a log query is a stream selector followed by
//! an optional pipeline; metric queries (added with the metric-query
//! parser) wrap log queries in range and vector aggregations.

/// A parsed log query: stream selector plus pipeline stages.
#[derive(Debug, Clone, PartialEq)]
pub struct LogQuery {
    pub selector: StreamSelector,
    pub pipeline: Vec<PipelineStage>,
}

/// The `{...}` stream selector: a conjunction of label matchers.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct StreamSelector {
    pub matchers: Vec<LabelMatcher>,
}

/// One label matcher, e.g. `service_name=~"web-.*"`.
#[derive(Debug, Clone, PartialEq)]
pub struct LabelMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
}

/// Label matching operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchOp {
    /// `=`
    Eq,
    /// `!=`
    Neq,
    /// `=~`
    Re,
    /// `!~`
    Nre,
}

impl std::fmt::Display for MatchOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            MatchOp::Eq => "=",
            MatchOp::Neq => "!=",
            MatchOp::Re => "=~",
            MatchOp::Nre => "!~",
        })
    }
}

/// One stage of a log pipeline (`|= "err"`, `| json`, `| level="error"`).
#[derive(Debug, Clone, PartialEq)]
pub enum PipelineStage {
    /// Line filter: `|= "err"`, `!= "noise"`, `|~ "re"`, `!~ "re"`.
    LineFilter(LineFilter),
    /// `| json` with optional label-extraction expressions.
    Json(Vec<LabelExtraction>),
    /// `| logfmt` with optional label-extraction expressions.
    Logfmt(Vec<LabelExtraction>),
    /// `| regexp "(?P<name>re)"` — named-capture extraction.
    Regexp(String),
    /// `| pattern "<method> <path>"` — pattern extraction.
    Pattern(String),
    /// `| unpack` — promote a packed JSON entry's labels.
    Unpack,
    /// `| level="error"`, `| status >= 500 and duration > 1s`.
    LabelFilter(LabelFilterExpr),
    /// `| line_format "{{.msg}}"`.
    LineFormat(String),
    /// `| label_format dst="src", pretty=`{{.x}}``.
    LabelFormat(Vec<LabelFormat>),
    /// `| drop label1, label2`.
    Drop(Vec<String>),
    /// `| keep label1, label2`.
    Keep(Vec<String>),
    /// `| unwrap duration_ms` or `| unwrap duration(latency)`.
    Unwrap(Unwrap),
}

/// A line filter operator and its match string.
#[derive(Debug, Clone, PartialEq)]
pub struct LineFilter {
    pub op: LineFilterOp,
    pub value: String,
}

/// Line filter operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineFilterOp {
    /// `|=`
    Contains,
    /// `!=`
    NotContains,
    /// `|~`
    Regex,
    /// `!~`
    NotRegex,
}

/// One label extraction expression for `json`/`logfmt`, e.g.
/// `status="response.status"` or a bare `level`.
#[derive(Debug, Clone, PartialEq)]
pub struct LabelExtraction {
    /// Destination label name.
    pub name: String,
    /// Source expression (JSON path / logfmt key). `None` extracts the
    /// field whose name equals `name`.
    pub expr: Option<String>,
}

/// One `label_format` assignment.
#[derive(Debug, Clone, PartialEq)]
pub struct LabelFormat {
    /// Destination label.
    pub dst: String,
    /// Either a rename from another label or a Go-template string.
    pub value: LabelFormatValue,
}

/// The right-hand side of a `label_format` assignment.
#[derive(Debug, Clone, PartialEq)]
pub enum LabelFormatValue {
    /// `dst=src_label` — rename/copy from another label.
    Rename(String),
    /// `dst="template"` — render a Go template.
    Template(String),
}

/// A `by`/`without` grouping clause, shared by vector aggregations and
/// unwrapped range aggregations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grouping {
    /// `true` for `without (...)`, `false` for `by (...)`.
    pub without: bool,
    /// The labels listed in the clause (may be empty: `by ()`).
    pub labels: Vec<String>,
}

/// An `unwrap` stage: the label to unwrap plus an optional conversion.
#[derive(Debug, Clone, PartialEq)]
pub struct Unwrap {
    /// Label whose value is unwrapped into a sample value.
    pub label: String,
    /// Conversion function wrapping the label, e.g. `duration` or
    /// `bytes` in `unwrap duration(latency)`.
    pub conversion: Option<String>,
}

/// A label-filter expression: comparisons combined with `and`/`or`.
///
/// A comma between predicates is sugar for `and`. `and` binds tighter
/// than `or`.
#[derive(Debug, Clone, PartialEq)]
pub enum LabelFilterExpr {
    Pred(LabelFilterPred),
    And(Box<LabelFilterExpr>, Box<LabelFilterExpr>),
    Or(Box<LabelFilterExpr>, Box<LabelFilterExpr>),
}

/// A single label-filter comparison, e.g. `status >= 500`.
#[derive(Debug, Clone, PartialEq)]
pub struct LabelFilterPred {
    pub name: String,
    pub op: FilterOp,
    pub value: FilterValue,
}

/// Label-filter comparison operators. String matchers (`=`, `!=`, `=~`,
/// `!~`) and ordered comparisons (`==`, `>`, `>=`, `<`, `<=`) share this
/// enum; the parser rejects operator/value combinations that don't fit
/// (e.g. `=~` against a number).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOp {
    Eq,
    Neq,
    Re,
    Nre,
    CmpEq,
    Gt,
    Gte,
    Lt,
    Lte,
}

/// The right-hand side value of a label filter.
#[derive(Debug, Clone, PartialEq)]
pub enum FilterValue {
    /// A quoted string (used with `=`, `!=`, `=~`, `!~`, and `==`).
    String(String),
    /// A numeric literal.
    Number(f64),
    /// A duration literal such as `1s` or `500ms`.
    Duration(std::time::Duration),
}
