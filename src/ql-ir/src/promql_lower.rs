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

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use promql_parser::label::{MatchOp, Matcher};
use promql_parser::parser::token::TokenId;
use promql_parser::parser::{
    self, AggregateExpr, AtModifier, BinaryExpr, Call, Expr, LabelModifier, Offset, SubqueryExpr,
    VectorMatchCardinality, VectorSelector, token,
};
use promql_parser::util::{ExprVisitor, walk_expr};
use query_ir::{
    Absent, Binop, BinopGroup, BinopOp, BinopOperand, CompareOp, ComparisonOp, Direction, Document,
    Filter, GroupSide, HistogramFraction, HistogramMode, HistogramQuantile, LabelJoin,
    LabelReplace, Labels, Leaf, Map, MapFn, NoOperands, OverTime, OverTimeFn, Predicate, Range,
    Reduce, ReduceFn, ResultEnvelope, Sample, SampleFn, SampleOf, Stage, SubDocument,
    is_pseudo_source,
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

/// Prometheus's default evaluation interval: the resolution of a subquery
/// that states none.
const DEFAULT_RESOLUTION_NS: i64 = 60_000_000_000;

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
    // An instant query evaluates once, so its step is free: it is raised to
    // the coarsest subquery resolution, which an `over_time` may not undercut.
    let step_ns = if params.instant {
        step_ns.max(max_subquery_step(&expr))
    } else {
        step_ns
    };
    let lowerer = Lowerer {
        params,
        doc_step_ns: step_ns,
        step_ns,
    };
    let operand = lowerer.lower(&expr)?;
    let pipe = operand.into_pipe()?;
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
    /// The operand as a pipeline: a number becomes the `constant`
    /// pseudo-source, which carries finite values only.
    fn into_pipe(self) -> Result<Pipe, LowerError> {
        match self {
            Operand::Number(n) if !n.is_finite() => Err(non_finite(n)),
            Operand::Number(n) => Ok(Pipe {
                from: "constant",
                constant: Some(n),
                pipeline: Vec::new(),
                shape: Shape::Scalar,
            }),
            Operand::Pipe(p) => Ok(p),
        }
    }
}

#[derive(Clone, Copy)]
struct Lowerer<'a> {
    params: &'a PromqlParams,
    /// The document step.
    doc_step_ns: i64,
    /// The step this sub-expression is evaluated at: a subquery's resolution
    /// inside one, else the document step.
    step_ns: i64,
}

impl Lowerer<'_> {
    fn lower(&self, expr: &Expr) -> Result<Operand, LowerError> {
        match expr {
            Expr::Paren(p) => self.lower(&p.expr),
            Expr::NumberLiteral(n) => Ok(Operand::Number(n.val)),
            Expr::VectorSelector(vs) => Ok(Operand::Pipe(self.select(vs, latest())?)),
            Expr::Call(call) => self.call(call),
            Expr::Aggregate(agg) => self.aggregate(agg),
            Expr::Binary(bin) => self.binary(bin),
            // Unary minus: `-v` is `v * -1`, which drops the metric name as
            // Prometheus does.
            Expr::Unary(u) => Ok(match self.lower(&u.expr)? {
                Operand::Number(n) => Operand::Number(-n),
                Operand::Pipe(p) => Operand::Pipe(p.push(Stage::Binop(number_binop(
                    BinopOp::Mul,
                    -1.0,
                    false,
                    false,
                )))),
            }),
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
        sample.step = self.stage_step();
        sample.offset = offset(vs.offset.as_ref())?;
        sample.at = vs.at.as_ref().map(|at| self.at(at)).transpose()?;
        Ok(where_pipe(vs).push(Stage::Sample(sample)))
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
        let series = |i: usize| self.series(arg(call, i)?, name);
        let map = |func, args| Stage::Map(Map { func, args });
        let stage = match name {
            "pi" => return Ok(Operand::Number(std::f64::consts::PI)),
            "histogram_quantile" | "histogram_fraction" => return self.histogram(call),
            "histogram_count" => {
                return Ok(Operand::Pipe(self.histogram_value(call, SampleOf::Count)?));
            }
            "histogram_sum" => {
                return Ok(Operand::Pipe(self.histogram_value(call, SampleOf::Sum)?));
            }
            // The mean observation: sum over count, series by series.
            "histogram_avg" => {
                let count = self.histogram_value(call, SampleOf::Count)?;
                let sum = self.histogram_value(call, SampleOf::Sum)?;
                return Ok(Operand::Pipe(sum.push(Stage::Binop(Binop {
                    right: BinopOperand::Document(Box::new(sub_document(count))),
                    ..number_binop(BinopOp::Div, 0.0, false, false)
                }))));
            }
            "time" => return Ok(Operand::Pipe(time())),
            "vector" => {
                let scalar = self.lower(arg(call, 0)?)?.into_pipe()?;
                return Ok(Operand::Pipe(Pipe {
                    shape: Shape::Series,
                    ..scalar.push(Stage::Vector(NoOperands {}))
                }));
            }
            "scalar" => {
                return Ok(Operand::Pipe(Pipe {
                    shape: Shape::Scalar,
                    ..series(0)?.push(Stage::Scalar(NoOperands {}))
                }));
            }
            "absent" => {
                let labels = match unparen(arg(call, 0)?) {
                    Expr::VectorSelector(vs) => absent_labels(vs),
                    _ => BTreeMap::new(),
                };
                return Ok(Operand::Pipe(
                    series(0)?.push(Stage::Absent(Absent { labels })),
                ));
            }
            "absent_over_time" => {
                let Expr::MatrixSelector(ms) = unparen(arg(call, 0)?) else {
                    return Err(inexpressible("absent_over_time() over a subquery"));
                };
                let labels = absent_labels(&ms.vs);
                let present = self.range_call(call, SampleFn::CountOverTime, None, 0)?;
                return Ok(Operand::Pipe(
                    present.into_pipe()?.push(Stage::Absent(Absent { labels })),
                ));
            }
            "sort" => Stage::Sort(Direction::Asc),
            "sort_desc" => Stage::Sort(Direction::Desc),
            "label_replace" => Stage::Labels(Labels::Replace(LabelReplace {
                dst: promql_label_field(&string_arg(call, 1)?),
                replacement: string_arg(call, 2)?,
                src: promql_label_field(&string_arg(call, 3)?),
                // Prometheus anchors the regex to the whole value, and `.`
                // matches a newline.
                regex: format!("^(?s:{})$", string_arg(call, 4)?),
            })),
            "label_join" => Stage::Labels(Labels::Join(LabelJoin {
                dst: promql_label_field(&string_arg(call, 1)?),
                separator: string_arg(call, 2)?,
                src: (3..call.args.args.len())
                    .map(|i| string_arg(call, i).map(|l| promql_label_field(&l)))
                    .collect::<Result<_, _>>()?,
            })),
            "round" if call.args.args.len() > 1 => {
                map(MapFn::Round, vec![self.number_arg(call, 1)?])
            }
            "clamp" => map(
                MapFn::Clamp,
                vec![self.number_arg(call, 1)?, self.number_arg(call, 2)?],
            ),
            "clamp_min" => map(MapFn::ClampMin, vec![self.number_arg(call, 1)?]),
            "clamp_max" => map(MapFn::ClampMax, vec![self.number_arg(call, 1)?]),
            other => match map_function(other) {
                // A calendar function without an argument reads `vector(time())`.
                Some(func) if call.args.args.is_empty() => {
                    let now = time().push(Stage::Vector(NoOperands {}));
                    return Ok(Operand::Pipe(Pipe {
                        shape: Shape::Series,
                        ..now.push(map(func, Vec::new()))
                    }));
                }
                Some(func) => map(func, Vec::new()),
                None => return Err(inexpressible(&format!("the PromQL function {name}()"))),
            },
        };
        Ok(Operand::Pipe(series(0)?.push(stage)))
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
            Some(Expr::Subquery(sq)) => self.subquery(name, sq, arg),
            _ => Err(inexpressible(&format!(
                "{name}() over anything but a range selector"
            ))),
        }
    }

    /// The step a stage states: none (the document's) outside a subquery.
    fn stage_step(&self) -> Option<String> {
        (self.step_ns != self.doc_step_ns).then(|| duration_ns(self.step_ns))
    }

    /// `f(expr[range:res])`: `expr` evaluated every `res`, re-windowed by an
    /// `over_time` stage at this sub-expression's own step.
    fn subquery(
        &self,
        name: &str,
        sq: &SubqueryExpr,
        arg: Option<f64>,
    ) -> Result<Operand, LowerError> {
        let func = over_time_function(name)
            .ok_or_else(|| inexpressible(&format!("{name}() over a subquery")))?;
        if sq.offset.is_some() || sq.at.is_some() {
            return Err(inexpressible("offset or @ on a subquery"));
        }
        let res_ns = subquery_resolution(sq);
        if res_ns > self.step_ns {
            return Err(inexpressible(&format!(
                "a subquery resolution ({}) coarser than its evaluation step ({})",
                duration_ns(res_ns),
                duration_ns(self.step_ns)
            )));
        }
        let inner = Lowerer {
            step_ns: res_ns,
            ..*self
        }
        .series(&sq.expr, "a subquery")?;
        // A pseudo-source always evaluates at the document step.
        if res_ns != self.doc_step_ns && reads_pseudo_source(inner.from, &inner.pipeline) {
            return Err(inexpressible(
                "time(), vector() or scalar arithmetic inside a subquery at its own resolution",
            ));
        }
        Ok(Operand::Pipe(inner.push(Stage::OverTime(OverTime {
            func,
            window: duration(sq.range),
            step: self.stage_step(),
            arg,
        }))))
    }

    /// `histogram_quantile`/`histogram_fraction` over SignalDB's whole stored
    /// histograms.
    fn histogram(&self, call: &Call) -> Result<Operand, LowerError> {
        let quantile = call.func.name == "histogram_quantile";
        let input = self.histogram_input(call, if quantile { 1 } else { 2 })?;
        let step = duration_ns(self.step_ns);
        let (by, per_series, mode, window) = (input.by, input.per_series, input.mode, input.window);
        // Instant mode reads each series' latest point, as an instant vector.
        let lookback = (mode == HistogramMode::Instant).then(|| LOOKBACK.to_string());
        let stage = if quantile {
            Stage::HistogramQuantile(HistogramQuantile {
                q: self.number_arg(call, 0)?,
                by,
                per_series,
                step,
                mode,
                window,
                lookback,
                as_name: "quantile".to_string(),
            })
        } else {
            Stage::HistogramFraction(HistogramFraction {
                lower: self.number_arg(call, 0)?,
                upper: self.number_arg(call, 1)?,
                by,
                per_series,
                step,
                mode,
                window,
                lookback,
                as_name: "fraction".to_string(),
            })
        };
        Ok(Operand::Pipe(input.pipe.push(stage)))
    }

    /// A histogram function's operand: a selector, optionally rated
    /// (`rate`/`increase`, whose scale a quantile or fraction ignores) and
    /// summed `by` labels. SignalDB stores each histogram whole rather than
    /// as `le`-labelled bucket series, so `le` is implicit. Without a `sum`
    /// the function applies to each series on its own, as in Prometheus.
    fn histogram_input(&self, call: &Call, i: usize) -> Result<HistogramInput, LowerError> {
        let name = call.func.name;
        let (expr, by, per_series) = match unparen(arg(call, i)?) {
            Expr::Aggregate(agg) if agg.op.id() == token::T_SUM => {
                // The output carries neither the name nor the implicit `le`.
                let by = match &agg.modifier {
                    None => Vec::new(),
                    Some(LabelModifier::Include(ls)) => label_fields(&ls.labels)
                        .into_iter()
                        .filter(|l| l != "le" && l != "metric.name")
                        .collect(),
                    Some(LabelModifier::Exclude(_)) => {
                        return Err(inexpressible(&format!("{name}() over `sum without`")));
                    }
                };
                (unparen(&agg.expr), by, false)
            }
            other => (other, Vec::new(), true),
        };
        let (vs, mode, window) = match expr {
            Expr::VectorSelector(vs) => (vs, HistogramMode::Instant, None),
            Expr::Call(c) if matches!(c.func.name, "rate" | "increase") => {
                match c.args.args.first().map(|a| unparen(a)) {
                    Some(Expr::MatrixSelector(ms)) => {
                        (&ms.vs, HistogramMode::Rate, Some(duration(ms.range)))
                    }
                    _ => return Err(inexpressible(&format!("{name}() over a rated subquery"))),
                }
            }
            other => {
                return Err(inexpressible(&format!(
                    "{name}() over {}",
                    describe_histogram_operand(other)
                )));
            }
        };
        if vs.offset.is_some() || vs.at.is_some() {
            return Err(inexpressible(&format!("{name}() with offset or @")));
        }
        Ok(HistogramInput {
            pipe: where_pipe(vs),
            by,
            per_series,
            mode,
            window,
        })
    }

    /// `histogram_count`/`histogram_sum`: a histogram's count or sum as a
    /// plain value, of a selector or through a range function.
    fn histogram_value(&self, call: &Call, of: SampleOf) -> Result<Pipe, LowerError> {
        let name = call.func.name;
        match unparen(arg(call, 0)?) {
            Expr::VectorSelector(vs) => self.select(vs, Sample { of, ..latest() }),
            Expr::Call(c) => match (
                range_function(c.func.name),
                c.args.args.first().map(|a| unparen(a)),
            ) {
                (Some(func), Some(Expr::MatrixSelector(ms)))
                    if func != SampleFn::QuantileOverTime =>
                {
                    let window = Some(duration(ms.range));
                    self.select(
                        &ms.vs,
                        Sample {
                            of,
                            window,
                            ..sample(func)
                        },
                    )
                }
                _ => Err(inexpressible(&format!("{name}() over {}()", c.func.name))),
            },
            other => Err(inexpressible(&format!(
                "{name}() over {}",
                describe_histogram_operand(other)
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

    /// A binary operator. Number operands fold or become the stage's number
    /// operand; two pipelines become a `binop` over a sub-document.
    fn binary(&self, bin: &BinaryExpr) -> Result<Operand, LowerError> {
        let op = binop_op(bin.op.id())
            .ok_or_else(|| inexpressible(&format!("the binary operator {}", bin.op)))?;
        let is_bool = bin.return_bool();
        match (self.lower(&bin.lhs)?, self.lower(&bin.rhs)?) {
            (Operand::Number(a), Operand::Number(b)) => fold(op, a, b)
                .map(Operand::Number)
                .ok_or_else(|| inexpressible(&format!("{} between two numbers", bin.op))),
            (Operand::Pipe(p), Operand::Number(n)) => with_number(p, op, n, false, is_bool),
            (Operand::Number(n), Operand::Pipe(p)) => with_number(p, op, n, true, is_bool),
            (Operand::Pipe(l), Operand::Pipe(r)) => self.vector_binop(bin, op, l, r),
        }
    }

    /// `l op r` between two pipelines, with PromQL's vector matching.
    fn vector_binop(
        &self,
        bin: &BinaryExpr,
        mut op: BinopOp,
        mut l: Pipe,
        mut r: Pipe,
    ) -> Result<Operand, LowerError> {
        let modifier = bin.modifier.clone().unwrap_or_default();
        let fill = &modifier.fill_values;
        if fill.lhs.is_some() || fill.rhs.is_some() {
            return Err(inexpressible("fill() vector-matching modifiers"));
        }
        let (on, ignoring) = match &modifier.matching {
            Some(LabelModifier::Include(ls)) => (Some(label_fields(&ls.labels)), None),
            Some(LabelModifier::Exclude(ls)) => (None, Some(label_fields(&ls.labels))),
            None => (None, None),
        };
        let group = match &modifier.card {
            VectorMatchCardinality::ManyToOne(ls) => Some((GroupSide::Left, ls)),
            VectorMatchCardinality::OneToMany(ls) => Some((GroupSide::Right, ls)),
            _ => None,
        }
        .map(|(side, ls)| BinopGroup {
            side,
            include: label_fields(&ls.labels),
        });
        let shape = if l.shape == Shape::Scalar && r.shape == Shape::Scalar {
            Shape::Scalar
        } else {
            Shape::Series
        };
        // A scalar-vector comparison keeps the vector's values, so a scalar
        // on the left swaps sides with the comparison flipped:
        // `scalar(y) < x` is `x > scalar(y)`.
        if l.shape == Shape::Scalar && r.shape == Shape::Series && compare_op(op).is_some() {
            (l, r, op) = (r, l, flip(op));
        }
        // A sub-document reads the document's own source or a
        // pseudo-source, so a pseudo-source left operand swaps sides:
        // `reverse` keeps the operator's (and `group`'s) sides as written.
        let reverse = is_pseudo_source(l.from) && !is_pseudo_source(r.from);
        let (pipe, right) = if reverse { (r, l) } else { (l, r) };
        let binop = Binop {
            op,
            right: BinopOperand::Document(Box::new(sub_document(right))),
            reverse,
            on,
            ignoring,
            group,
            bool: bin.return_bool(),
        };
        Ok(Operand::Pipe(Pipe {
            shape,
            ..pipe.push(Stage::Binop(binop))
        }))
    }

    /// Lower an operand that must be a series.
    fn series(&self, expr: &Expr, what: &str) -> Result<Pipe, LowerError> {
        match self.lower(expr)? {
            Operand::Pipe(p) if p.shape == Shape::Series => Ok(p),
            _ => Err(inexpressible(&format!("{what} over a scalar"))),
        }
    }
}

/// The metrics point stream narrowed by a selector's matchers.
fn where_pipe(vs: &VectorSelector) -> Pipe {
    Pipe {
        from: "metrics",
        constant: None,
        pipeline: selector_predicate(vs)
            .map(Stage::Where)
            .into_iter()
            .collect(),
        shape: Shape::Series,
    }
}

/// The `i`-th argument of a call.
fn arg(call: &Call, i: usize) -> Result<&Expr, LowerError> {
    call.args.args.get(i).map(|a| &**a).ok_or_else(|| {
        LowerError::InvalidPromql(format!(
            "{}() is missing argument {}",
            call.func.name,
            i + 1
        ))
    })
}

/// The `i`-th argument of a call, which must be a string literal.
fn string_arg(call: &Call, i: usize) -> Result<String, LowerError> {
    match unparen(arg(call, i)?) {
        Expr::StringLiteral(s) => Ok(s.val.clone()),
        _ => Err(inexpressible(&format!(
            "{}() with a non-literal argument {}",
            call.func.name,
            i + 1
        ))),
    }
}

/// The `time` pseudo-source: the evaluation instant, in seconds.
fn time() -> Pipe {
    Pipe {
        from: "time",
        constant: None,
        pipeline: Vec::new(),
        shape: Shape::Scalar,
    }
}

/// The labels `absent()` gives its series, by Prometheus's rule: each
/// label's first equality matcher, less the metric name, any label another
/// matcher also constrains, and empty values (which are no label).
fn absent_labels(vs: &VectorSelector) -> BTreeMap<String, String> {
    if !vs.matchers.or_matchers.is_empty() {
        return BTreeMap::new();
    }
    let mut labels = BTreeMap::new();
    let mut unknown = Vec::new();
    for m in &vs.matchers.matchers {
        let field = promql_label_field(&m.name);
        if field == "metric.name" {
            continue;
        }
        if m.op == MatchOp::Equal && !labels.contains_key(&field) {
            if !m.value.is_empty() {
                labels.insert(field, m.value.clone());
            }
        } else {
            unknown.push(field);
        }
    }
    for field in unknown {
        labels.remove(&field);
    }
    labels
}

/// The operand-free `map` function a PromQL function is.
fn map_function(name: &str) -> Option<MapFn> {
    Some(match name {
        "abs" => MapFn::Abs,
        "ceil" => MapFn::Ceil,
        "floor" => MapFn::Floor,
        "round" => MapFn::Round,
        "sqrt" => MapFn::Sqrt,
        "exp" => MapFn::Exp,
        "ln" => MapFn::Ln,
        "log2" => MapFn::Log2,
        "log10" => MapFn::Log10,
        "sgn" => MapFn::Sgn,
        "day_of_month" => MapFn::DayOfMonth,
        "day_of_week" => MapFn::DayOfWeek,
        "day_of_year" => MapFn::DayOfYear,
        "days_in_month" => MapFn::DaysInMonth,
        "hour" => MapFn::Hour,
        "minute" => MapFn::Minute,
        "month" => MapFn::Month,
        "year" => MapFn::Year,
        _ => return None,
    })
}

/// `pipe op n` (or `n op pipe` when `reversed`).
///
/// A series compared with a finite number is a `filter`, which keeps the
/// series that compare true (or yields 0/1 with `bool`); everything else is a
/// `binop` with a number operand.
fn with_number(
    pipe: Pipe,
    op: BinopOp,
    n: f64,
    reversed: bool,
    is_bool: bool,
) -> Result<Operand, LowerError> {
    if !n.is_finite() {
        return Err(non_finite(n));
    }
    let compare = compare_op(if reversed { flip(op) } else { op });
    let stage = match compare {
        Some(op) if pipe.shape == Shape::Series => Stage::Filter(Filter {
            op,
            value: n,
            bool: is_bool,
        }),
        _ => Stage::Binop(number_binop(op, n, reversed, is_bool)),
    };
    Ok(Operand::Pipe(pipe.push(stage)))
}

fn number_binop(op: BinopOp, n: f64, reverse: bool, is_bool: bool) -> Binop {
    Binop {
        op,
        right: BinopOperand::Number(n),
        reverse,
        on: None,
        ignoring: None,
        group: None,
        bool: is_bool,
    }
}

fn binop_op(id: TokenId) -> Option<BinopOp> {
    Some(match id {
        token::T_ADD => BinopOp::Add,
        token::T_SUB => BinopOp::Sub,
        token::T_MUL => BinopOp::Mul,
        token::T_DIV => BinopOp::Div,
        token::T_MOD => BinopOp::Mod,
        token::T_POW => BinopOp::Pow,
        token::T_ATAN2 => BinopOp::Atan2,
        token::T_EQLC => BinopOp::Eq,
        token::T_NEQ => BinopOp::Ne,
        token::T_GTR => BinopOp::Gt,
        token::T_GTE => BinopOp::Ge,
        token::T_LSS => BinopOp::Lt,
        token::T_LTE => BinopOp::Le,
        token::T_LAND => BinopOp::And,
        token::T_LOR => BinopOp::Or,
        token::T_LUNLESS => BinopOp::Unless,
        _ => return None,
    })
}

fn compare_op(op: BinopOp) -> Option<CompareOp> {
    Some(match op {
        BinopOp::Eq => CompareOp::Eq,
        BinopOp::Ne => CompareOp::Ne,
        BinopOp::Gt => CompareOp::Gt,
        BinopOp::Ge => CompareOp::Ge,
        BinopOp::Lt => CompareOp::Lt,
        BinopOp::Le => CompareOp::Le,
        _ => return None,
    })
}

/// The operator with its operands swapped, for a comparison: `5 < v` is
/// `v > 5`.
fn flip(op: BinopOp) -> BinopOp {
    match op {
        BinopOp::Gt => BinopOp::Lt,
        BinopOp::Ge => BinopOp::Le,
        BinopOp::Lt => BinopOp::Gt,
        BinopOp::Le => BinopOp::Ge,
        same => same,
    }
}

/// `a op b` over two numbers, as Prometheus evaluates it. A comparison
/// between scalars always carries `bool` (the parser insists), so it is 0/1.
/// Set operators take vectors, so they have no value here.
fn fold(op: BinopOp, a: f64, b: f64) -> Option<f64> {
    let truth = |t: bool| if t { 1.0 } else { 0.0 };
    Some(match op {
        BinopOp::Add => a + b,
        BinopOp::Sub => a - b,
        BinopOp::Mul => a * b,
        BinopOp::Div => a / b,
        BinopOp::Mod => a % b,
        BinopOp::Pow => a.powf(b),
        BinopOp::Atan2 => a.atan2(b),
        BinopOp::Eq => truth(a == b),
        BinopOp::Ne => truth(a != b),
        BinopOp::Gt => truth(a > b),
        BinopOp::Ge => truth(a >= b),
        BinopOp::Lt => truth(a < b),
        BinopOp::Le => truth(a <= b),
        BinopOp::And | BinopOp::Or | BinopOp::Unless => return None,
    })
}

/// A histogram function's operand, before its histogram stage.
struct HistogramInput {
    pipe: Pipe,
    by: Vec<String>,
    per_series: bool,
    mode: HistogramMode,
    window: Option<String>,
}

fn describe_histogram_operand(expr: &Expr) -> String {
    match expr {
        Expr::Call(c) => format!("{}()", c.func.name),
        Expr::Aggregate(a) => format!("{}()", a.op),
        other => format!("a {}", expr_kind(other)),
    }
}

fn sub_document(pipe: Pipe) -> SubDocument {
    SubDocument {
        from: pipe.from.to_string(),
        pipeline: pipe.pipeline,
        constant: pipe.constant,
    }
}

/// Whether a pipeline, or a sub-document it combines with, reads a
/// pseudo-source.
fn reads_pseudo_source(from: &str, pipeline: &[Stage]) -> bool {
    is_pseudo_source(from)
        || pipeline.iter().any(|stage| {
            matches!(stage, Stage::Binop(Binop { right: BinopOperand::Document(d), .. })
                if reads_pseudo_source(&d.from, &d.pipeline))
        })
}

/// The coarsest subquery resolution in an instant query, or 0.
fn max_subquery_step(expr: &Expr) -> i64 {
    struct Max(i64);
    impl ExprVisitor for Max {
        type Error = std::convert::Infallible;
        fn pre_visit(&mut self, expr: &Expr) -> Result<bool, Self::Error> {
            if let Expr::Subquery(sq) = expr {
                self.0 = self.0.max(subquery_resolution(sq));
            }
            Ok(true)
        }
    }
    let mut max = Max(0);
    let Ok(_) = walk_expr(&mut max, expr);
    max.0
}

/// A subquery's resolution, Prometheus's default evaluation interval when it
/// states none.
fn subquery_resolution(sq: &SubqueryExpr) -> i64 {
    sq.step
        .map(duration_i64)
        .filter(|&ns| ns > 0)
        .unwrap_or(DEFAULT_RESOLUTION_NS)
}

/// The `over_time` function a PromQL range function is over a subquery.
fn over_time_function(name: &str) -> Option<OverTimeFn> {
    Some(match name {
        "avg_over_time" => OverTimeFn::Avg,
        "min_over_time" => OverTimeFn::Min,
        "max_over_time" => OverTimeFn::Max,
        "sum_over_time" => OverTimeFn::Sum,
        "count_over_time" => OverTimeFn::Count,
        "last_over_time" => OverTimeFn::Last,
        "stddev_over_time" => OverTimeFn::Stddev,
        "stdvar_over_time" => OverTimeFn::Stdvar,
        "present_over_time" => OverTimeFn::Present,
        "quantile_over_time" => OverTimeFn::Quantile,
        "delta" => OverTimeFn::Delta,
        "deriv" => OverTimeFn::Deriv,
        "changes" => OverTimeFn::Changes,
        "resets" => OverTimeFn::Resets,
        _ => return None,
    })
}

/// An instant `sample`: the latest point within the lookback.
fn latest() -> Sample {
    Sample {
        lookback: Some(LOOKBACK.to_string()),
        ..sample(SampleFn::Latest)
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
    duration_ns(duration_i64(d))
}

fn duration_i64(d: Duration) -> i64 {
    i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
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
