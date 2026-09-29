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

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use promql_parser::label::{MatchOp, Matcher};
use promql_parser::parser::{
    self, AggregateExpr, AtModifier, Call, Expr, LabelModifier, Offset, VectorSelector, token,
};
use query_ir::{
    ComparisonOp, Document, Leaf, Predicate, Range, Reduce, ReduceFn, ResultEnvelope, Sample,
    SampleFn, SampleOf, Stage,
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
    let lowerer = Lowerer { params };
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

impl Pipe {
    fn push(mut self, stage: Stage) -> Self {
        self.pipeline.push(stage);
        self
    }
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

struct Lowerer<'a> {
    params: &'a PromqlParams,
}

impl Lowerer<'_> {
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
            Expr::Call(call) => self.call(call),
            Expr::Aggregate(agg) => self.aggregate(agg),
            Expr::StringLiteral(_) => Err(inexpressible("a string literal as a value")),
            Expr::MatrixSelector(_) => Err(inexpressible(
                "a range vector outside a range function (a range-vector result)",
            )),
            other => Err(inexpressible(&format!("PromQL {}", expr_kind(other)))),
        }
    }

    /// A selector read by one `sample`: `where` over the point stream, then
    /// the sample, carrying the selector's `offset` and `@`.
    fn select(&self, vs: &VectorSelector, mut sample: Sample) -> Result<Pipe, LowerError> {
        sample.offset = offset(vs.offset.as_ref())?;
        sample.at = vs.at.as_ref().map(|at| self.at(at)).transpose()?;
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

    /// `@ <t>`, `@ start()`, `@ end()` as a timestamp literal in nanoseconds.
    fn at(&self, at: &AtModifier) -> Result<serde_json::Value, LowerError> {
        let (start, end) = self.params.window();
        Ok(match at {
            AtModifier::Start => start.into(),
            AtModifier::End => end.into(),
            AtModifier::At(t) => system_time_ns(*t)
                .ok_or_else(|| {
                    LowerError::InvalidPromql(
                        "an `@` timestamp outside the nanosecond range".to_string(),
                    )
                })?
                .into(),
        })
    }

    fn call(&self, call: &Call) -> Result<Operand, LowerError> {
        let name = call.func.name;
        if let Some(func) = range_function(name) {
            let (arg, window_arg) = match func {
                SampleFn::QuantileOverTime => (Some(quantile(name, self.number_arg(call, 0)?)?), 1),
                _ => (None, 0),
            };
            return self.range_call(call, func, arg, window_arg);
        }
        Err(inexpressible(&format!("the PromQL function {name}()")))
    }

    /// `f(m[w])`: the range function samples the selector's window.
    fn range_call(
        &self,
        call: &Call,
        func: SampleFn,
        arg: Option<f64>,
        index: usize,
    ) -> Result<Operand, LowerError> {
        let name = call.func.name;
        match call.args.args.get(index).map(|a| unparen(a)) {
            Some(Expr::MatrixSelector(ms)) => Ok(Operand::Pipe(self.select(
                &ms.vs,
                Sample {
                    window: Some(duration(ms.range)),
                    arg,
                    ..sample(func)
                },
            )?)),
            Some(Expr::Subquery(_)) => Err(inexpressible(&format!("{name}() over a subquery"))),
            _ => Err(inexpressible(&format!(
                "{name}() over anything but a range selector"
            ))),
        }
    }

    /// The `i`-th call argument, which must fold to a number.
    fn number_arg(&self, call: &Call, i: usize) -> Result<f64, LowerError> {
        match call.args.args.get(i).map(|a| self.lower(a)).transpose()? {
            Some(Operand::Number(n)) if !n.is_finite() => Err(non_finite(n)),
            Some(Operand::Number(n)) => Ok(n),
            _ => Err(inexpressible(&format!(
                "{}() with a non-literal argument {}",
                call.func.name,
                i + 1
            ))),
        }
    }

    /// A vector aggregation: `reduce` over the lowered operand.
    fn aggregate(&self, agg: &AggregateExpr) -> Result<Operand, LowerError> {
        let op = agg.op.to_string();
        let func = match agg.op.id() {
            token::T_SUM => ReduceFn::Sum,
            token::T_AVG => ReduceFn::Avg,
            token::T_MIN => ReduceFn::Min,
            token::T_MAX => ReduceFn::Max,
            token::T_COUNT => ReduceFn::Count,
            token::T_GROUP => ReduceFn::Group,
            token::T_STDDEV => ReduceFn::Stddev,
            token::T_STDVAR => ReduceFn::Stdvar,
            token::T_QUANTILE => ReduceFn::Quantile,
            token::T_TOPK => ReduceFn::Topk,
            token::T_BOTTOMK => ReduceFn::Bottomk,
            token::T_COUNT_VALUES => ReduceFn::CountValues,
            _ => return Err(inexpressible(&format!("the PromQL aggregation {op}"))),
        };
        let (arg, label) = match (func, agg.param.as_deref().map(unparen)) {
            (ReduceFn::CountValues, Some(Expr::StringLiteral(s))) => {
                (None, Some(promql_label_field(&s.val)))
            }
            (ReduceFn::Quantile | ReduceFn::Topk | ReduceFn::Bottomk, Some(param)) => {
                match self.lower(param)? {
                    Operand::Number(q) if func == ReduceFn::Quantile => {
                        (Some(quantile(&op, q)?), None)
                    }
                    // Prometheus truncates k; below 1 it selects nothing.
                    Operand::Number(k) if k.trunc() >= 1.0 => (Some(k.trunc()), None),
                    Operand::Number(k) => {
                        return Err(inexpressible(&format!("{op} with k = {k}, below 1")));
                    }
                    Operand::Pipe(_) => {
                        return Err(inexpressible(&format!("{op} with a non-literal parameter")));
                    }
                }
            }
            (_, None) => (None, None),
            (_, Some(_)) => return Err(inexpressible(&format!("{op} with this parameter"))),
        };
        let (by, without) = match &agg.modifier {
            None => (None, None),
            Some(LabelModifier::Include(labels)) => (Some(label_fields(&labels.labels)), None),
            // `without` drops the metric name in the IR too, as in Prometheus.
            Some(LabelModifier::Exclude(labels)) => (None, Some(label_fields(&labels.labels))),
        };
        let inner = self.series(&agg.expr, &op)?;
        Ok(Operand::Pipe(inner.push(Stage::Reduce(Reduce {
            func,
            by,
            without,
            arg,
            label,
        }))))
    }

    /// Lower an operand that must be a series.
    fn series(&self, expr: &Expr, what: &str) -> Result<Pipe, LowerError> {
        match self.lower(expr)? {
            Operand::Pipe(p) if p.shape == Shape::Series => Ok(p),
            _ => Err(inexpressible(&format!("{what} over a scalar"))),
        }
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

/// The `sample` function a PromQL range function is.
fn range_function(name: &str) -> Option<SampleFn> {
    Some(match name {
        "rate" => SampleFn::Rate,
        "increase" => SampleFn::Increase,
        "irate" => SampleFn::Irate,
        "delta" => SampleFn::Delta,
        "idelta" => SampleFn::Idelta,
        "deriv" => SampleFn::Deriv,
        "resets" => SampleFn::Resets,
        "changes" => SampleFn::Changes,
        "avg_over_time" => SampleFn::AvgOverTime,
        "min_over_time" => SampleFn::MinOverTime,
        "max_over_time" => SampleFn::MaxOverTime,
        "sum_over_time" => SampleFn::SumOverTime,
        "count_over_time" => SampleFn::CountOverTime,
        "last_over_time" => SampleFn::LastOverTime,
        "stddev_over_time" => SampleFn::StddevOverTime,
        "stdvar_over_time" => SampleFn::StdvarOverTime,
        "present_over_time" => SampleFn::PresentOverTime,
        "quantile_over_time" => SampleFn::QuantileOverTime,
        _ => return None,
    })
}

/// A selector's matchers as one predicate, or `None` when it has none.
///
/// `{a="1" or b="2"}` is a disjunction of conjunctions.
fn selector_predicate(vs: &VectorSelector) -> Option<Predicate> {
    let mut parts = Vec::new();
    if let Some(name) = &vs.name {
        parts.push(leaf("metric.name", ComparisonOp::Eq, name));
    }
    parts.extend(vs.matchers.matchers.iter().map(matcher));
    let alternatives: Vec<Predicate> = vs
        .matchers
        .or_matchers
        .iter()
        .filter_map(|group| conjoin(group.iter().map(matcher).collect()))
        .collect();
    if !alternatives.is_empty() {
        parts.push(disjoin(alternatives));
    }
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

fn disjoin(mut parts: Vec<Predicate>) -> Predicate {
    if parts.len() == 1
        && let Some(only) = parts.pop()
    {
        return only;
    }
    Predicate::Or(parts)
}

fn label_fields(labels: &[String]) -> Vec<String> {
    labels.iter().map(|l| promql_label_field(l)).collect()
}

/// A positive `offset` as a duration; zero is no offset.
fn offset(offset: Option<&Offset>) -> Result<Option<String>, LowerError> {
    match offset {
        None => Ok(None),
        Some(Offset::Pos(d)) if d.is_zero() => Ok(None),
        Some(Offset::Pos(d)) => Ok(Some(duration(*d))),
        Some(Offset::Neg(d)) if d.is_zero() => Ok(None),
        Some(Offset::Neg(_)) => Err(inexpressible("a negative offset")),
    }
}

/// Nanoseconds since the epoch, negative before it.
fn system_time_ns(t: SystemTime) -> Option<i64> {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_nanos()).ok(),
        Err(e) => i64::try_from(e.duration().as_nanos()).ok().map(|n| -n),
    }
}

fn duration(d: Duration) -> String {
    duration_ns(i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
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

fn unparen(expr: &Expr) -> &Expr {
    match expr {
        Expr::Paren(p) => unparen(&p.expr),
        other => other,
    }
}

/// A quantile, which the IR carries only within `[0, 1]`. (Prometheus
/// answers ±Inf outside it.)
fn quantile(what: &str, q: f64) -> Result<f64, LowerError> {
    if (0.0..=1.0).contains(&q) {
        Ok(q)
    } else {
        Err(inexpressible(&format!(
            "{what} with the quantile {q} outside [0, 1]"
        )))
    }
}

/// JSON has no infinity or NaN, so the IR carries finite numbers only.
fn non_finite(n: f64) -> LowerError {
    inexpressible(&format!("the non-finite number {n} as an operand"))
}

fn inexpressible(what: &str) -> LowerError {
    LowerError::Inexpressible(what.to_string())
}

fn expr_kind(expr: &Expr) -> &'static str {
    match expr {
        Expr::Aggregate(_) => "aggregation",
        Expr::Unary(_) => "unary expression",
        Expr::Binary(_) => "binary expression",
        Expr::Paren(_) => "parenthesized expression",
        Expr::Subquery(_) => "subquery",
        Expr::NumberLiteral(_) => "number literal",
        Expr::StringLiteral(_) => "string literal",
        Expr::VectorSelector(_) => "vector selector",
        Expr::MatrixSelector(_) => "range-vector selector",
        Expr::Call(_) => "function call",
        Expr::Extension(_) => "extension expression",
    }
}
