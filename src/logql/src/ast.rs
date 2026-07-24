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

/// One stage of a log pipeline (`|= "err"`, `| json`, ...).
///
/// Variants land with the pipeline-stage parser (#371); the enum exists
/// so [`LogQuery`] carries a stable shape from the start.
#[derive(Debug, Clone, PartialEq)]
pub enum PipelineStage {}
