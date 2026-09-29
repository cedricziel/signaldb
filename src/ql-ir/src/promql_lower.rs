//! PromQL → IR (design D11 of `otel-native-schema`).
//!
//! A PromQL expression lowers to one `metrics` document whose pipeline is the
//! series algebra of `irVersion` 10: a selector is a `where` over the point
//! stream plus a `sample`, and every operator on top of it is a series stage.
//!
//! Every document declares `irVersion` 10 and a document `step`. A range query
//! evaluates at `start + k·step`; an instant query at a single instant `t`
//! (`start = end = t`), with a nominal positive step.
//!
//! Label names map at this boundary and nowhere else: `__name__` is
//! `metric.name`; `job`, `service` and `service_name` are `service.name`; any
//! other label, dotted UTF-8 names included, passes through as spelled.

use promql_parser::label::{MatchOp, Matcher};
use promql_parser::parser::{self, Expr, VectorSelector};
use query_ir::{
    ComparisonOp, Document, Leaf, Predicate, Range, ResultEnvelope, Sample, SampleFn, SampleOf,
    Stage,
};

use crate::LowerError;

/// The version every PromQL document declares: the series algebra.
const IR_VERSION: i64 = 10;

/// How long an instant `sample` looks back for a series' latest point —
/// Prometheus's default lookback delta.
const LOOKBACK: &str = "5m";

/// The step an instant query declares when the caller gives none. An instant
/// query evaluates once, so any positive step denotes the same thing.
const INSTANT_STEP_NS: i64 = 1_000_000_000;

/// When and how often a PromQL query is evaluated, in nanoseconds since the
/// epoch.
///
/// A range query evaluates at `start_ns + k·step_ns` up to `end_ns`. An
/// instant query (`instant: true`) evaluates once, at `end_ns`; `start_ns` is
/// ignored and `step_ns` may be zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PromqlParams {
    pub start_ns: i64,
    pub end_ns: i64,
    pub step_ns: i64,
    pub instant: bool,
}

impl PromqlParams {
    /// A range query over `[start_ns, end_ns]` every `step_ns`.
    pub fn range(start_ns: i64, end_ns: i64, step_ns: i64) -> Self {
        PromqlParams {
            start_ns,
            end_ns,
            step_ns,
            instant: false,
        }
    }

    /// An instant query at `t_ns`.
    pub fn instant(t_ns: i64) -> Self {
        PromqlParams {
            start_ns: t_ns,
            end_ns: t_ns,
            step_ns: 0,
            instant: true,
        }
    }

    /// The evaluation window: `(start, end)`, collapsed to `(t, t)` for an
    /// instant query.
    fn window(&self) -> (i64, i64) {
        if self.instant {
            (self.end_ns, self.end_ns)
        } else {
            (self.start_ns, self.end_ns)
        }
    }
}

/// Lower a PromQL expression into an IR document over `metrics`.
///
/// # Errors
///
/// [`LowerError::InvalidPromql`] when the text does not parse or the
/// parameters describe no evaluation; [`LowerError::Inexpressible`], naming
/// the construct, when it parses but has no IR equivalent.
///
/// The document is not validated here: the caller validates it against its
/// source registry and field resolver (`query_ir::validate`).
pub fn promql_to_ir(query: &str, params: &PromqlParams) -> Result<Document, LowerError> {
    let expr = parser::parse(query).map_err(|e| LowerError::InvalidPromql(e.to_string()))?;
    let (start, end) = params.window();
    if start > end {
        return Err(LowerError::InvalidPromql(
            "the query range ends before it starts".to_string(),
        ));
    }
    let step_ns = match (params.instant, params.step_ns) {
        (_, ns) if ns > 0 => ns,
        (true, _) => INSTANT_STEP_NS,
        (false, _) => {
            return Err(LowerError::InvalidPromql(
                "a range query needs a positive step".to_string(),
            ));
        }
    };
    let lowerer = Lowerer;
    let operand = lowerer.lower(&expr)?;
    let pipe = operand.into_pipe();
    Ok(Document {
        ir_version: IR_VERSION,
        from: pipe.from.to_string(),
        range: Range {
            from: start.into(),
            to: end.into(),
        },
        result: match pipe.shape {
            Shape::Series => ResultEnvelope::Series,
            Shape::Scalar => ResultEnvelope::Scalar,
        },
        fields: None,
        pipeline: pipe.pipeline,
        focus: None,
        depth: None,
        trace_id: None,
        step: Some(duration_ns(step_ns)),
        constant: pipe.constant,
    })
}

/// The logical field a PromQL label name addresses.
pub fn promql_label_field(label: &str) -> String {
    match label {
        "__name__" => "metric.name",
        "job" | "service" | "service_name" => "service.name",
        other => other,
    }
    .to_string()
}

/// What an IR pipeline yields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    Series,
    Scalar,
}

/// A lowered sub-expression: an IR source plus the pipeline over it.
#[derive(Debug, Clone)]
struct Pipe {
    from: &'static str,
    constant: Option<f64>,
    pipeline: Vec<Stage>,
    shape: Shape,
}

/// A lowered expression. Number literals stay unmaterialized so arithmetic on
/// them folds and a stage can take them as an operand.
#[derive(Debug, Clone)]
enum Operand {
    Number(f64),
    Pipe(Pipe),
}

impl Operand {
    fn into_pipe(self) -> Pipe {
        match self {
            Operand::Number(n) => Pipe {
                from: "constant",
                constant: Some(n),
                pipeline: Vec::new(),
                shape: Shape::Scalar,
            },
            Operand::Pipe(p) => p,
        }
    }
}

struct Lowerer;

impl Lowerer {
    fn lower(&self, expr: &Expr) -> Result<Operand, LowerError> {
        match expr {
            Expr::Paren(p) => self.lower(&p.expr),
            Expr::NumberLiteral(n) => Ok(Operand::Number(n.val)),
            Expr::VectorSelector(vs) => Ok(Operand::Pipe(self.select(
                vs,
                Sample {
                    lookback: Some(LOOKBACK.to_string()),
                    ..sample(SampleFn::Latest)
                },
            )?)),
            Expr::StringLiteral(_) => Err(inexpressible("a string literal as a value")),
            Expr::MatrixSelector(_) => Err(inexpressible(
                "a range vector outside a range function (a range-vector result)",
            )),
            other => Err(inexpressible(&format!("the PromQL expression {other}"))),
        }
    }

    /// A selector read by one `sample`: `where` over the point stream, then
    /// the sample, carrying the selector's `offset` and `@`.
    fn select(&self, vs: &VectorSelector, sample: Sample) -> Result<Pipe, LowerError> {
        if vs.offset.is_some() || vs.at.is_some() {
            return Err(inexpressible("offset and @ modifiers"));
        }
        if !vs.matchers.or_matchers.is_empty() {
            return Err(inexpressible("`or` between selector matchers"));
        }
        let mut pipeline = Vec::new();
        if let Some(p) = selector_predicate(vs) {
            pipeline.push(Stage::Where(p));
        }
        pipeline.push(Stage::Sample(sample));
        Ok(Pipe {
            from: "metrics",
            constant: None,
            pipeline,
            shape: Shape::Series,
        })
    }
}

/// A `sample` of `func` with every optional operand unset.
fn sample(func: SampleFn) -> Sample {
    Sample {
        func,
        of: SampleOf::Value,
        window: None,
        lookback: None,
        step: None,
        offset: None,
        at: None,
        arg: None,
        as_name: None,
    }
}

/// A selector's matchers as one predicate, or `None` when it has none.
fn selector_predicate(vs: &VectorSelector) -> Option<Predicate> {
    let mut parts = Vec::new();
    if let Some(name) = &vs.name {
        parts.push(leaf("metric.name", ComparisonOp::Eq, name));
    }
    parts.extend(vs.matchers.matchers.iter().map(matcher));
    conjoin(parts)
}

/// One label matcher, with Prometheus's absent-is-empty semantics.
///
/// Prometheus treats a missing label as the empty string, while the IR's
/// comparisons are Kleene: absent satisfies neither a comparison nor its
/// negation. So a matcher that the empty string satisfies also ORs in "the
/// field does not exist" (as the LogQL lowering does, design D9 of
/// `ir-single-lowering`). The parser has already compiled each regex fully
/// anchored; that pattern is emitted with `.` matching newlines, as Prometheus
/// matches, and whether it matches `""` is decided exactly.
fn matcher(m: &Matcher) -> Predicate {
    let field = promql_label_field(&m.name);
    let (positive, empty_matches) = match &m.op {
        MatchOp::Equal => (leaf(&field, ComparisonOp::Eq, &m.value), m.value.is_empty()),
        MatchOp::NotEqual => (
            leaf(&field, ComparisonOp::Ne, &m.value),
            !m.value.is_empty(),
        ),
        MatchOp::Re(re) => (
            leaf(&field, ComparisonOp::Regex, &dotall(re.as_str())),
            re.is_match(""),
        ),
        MatchOp::NotRe(re) => (
            Predicate::Not(Box::new(leaf(
                &field,
                ComparisonOp::Regex,
                &dotall(re.as_str()),
            ))),
            !re.is_match(""),
        ),
    };
    if !empty_matches {
        return positive;
    }
    let absent = Predicate::Not(Box::new(Predicate::Leaf(Leaf {
        field,
        op: ComparisonOp::Exists,
        value: None,
    })));
    Predicate::Or(vec![positive, absent])
}

/// The parser's anchored `^(?:…)$` pattern as Prometheus matches it: `.`
/// also matches a newline.
fn dotall(anchored: &str) -> String {
    let inner = anchored
        .strip_prefix("^(?:")
        .and_then(|p| p.strip_suffix(")$"))
        .unwrap_or(anchored);
    format!("^(?s:{inner})$")
}

fn leaf(field: &str, op: ComparisonOp, value: &str) -> Predicate {
    Predicate::Leaf(Leaf {
        field: field.to_string(),
        op,
        value: Some(serde_json::Value::String(value.to_string())),
    })
}

fn conjoin(mut parts: Vec<Predicate>) -> Option<Predicate> {
    match parts.len() {
        0 => None,
        1 => parts.pop(),
        _ => Some(Predicate::And(parts)),
    }
}

/// A duration as the IR spells it, in the largest unit that divides it.
fn duration_ns(ns: i64) -> String {
    const UNITS: [(i64, &str); 5] = [
        (3_600_000_000_000, "h"),
        (60_000_000_000, "m"),
        (1_000_000_000, "s"),
        (1_000_000, "ms"),
        (1_000, "us"),
    ];
    UNITS.iter().find(|(size, _)| ns % size == 0).map_or_else(
        || format!("{ns}ns"),
        |(size, unit)| format!("{}{unit}", ns / size),
    )
}

fn inexpressible(what: &str) -> LowerError {
    LowerError::Inexpressible(what.to_string())
}
