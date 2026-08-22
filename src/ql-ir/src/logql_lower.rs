//! LogQL → IR.
//!
//! Two shapes. A **log query** is a stream selector plus a filter pipeline,
//! which is a `where` stage. A **metric query** wraps one in a range and/or
//! vector aggregation, which is a `where` followed by an `aggregate`.
//!
//! The version a document declares is derived from what it uses rather than
//! stamped at the maximum: a query needing nothing from `irVersion` 5 claims
//! v1 and stays executable by an older server.

use logql::{
    AggregationFunction, Expr, LabelMatcher, LineFilter, LineFilterOp, LogQuery, MatchOp,
    MetricQuery, PipelineStage, RangeFunction, StreamSelector,
};
use query_ir::{
    Agg, AggFn, Aggregate, ComparisonOp, Document, Leaf, Predicate, ResultEnvelope, Stage,
};

use crate::LowerError;

/// Lower a LogQL query — either form — into an IR document over `logs`.
pub fn logql_to_ir(query: &str, from: &str, to: &str) -> Result<Document, LowerError> {
    match logql::parse(query)? {
        Expr::Log(log) => lower_log_query(&log, from, to),
        Expr::Metric(metric) => lower_metric_query(&metric, from, to),
        // `Expr` is #[non_exhaustive]: the parser releases independently and
        // may recognise a query form this build cannot lower.
        ref other => Err(LowerError::Inexpressible(format!(
            "LogQL query form {other:?}"
        ))),
    }
}

/// A stream selector plus a filter pipeline: rows, one `where`.
fn lower_log_query(q: &LogQuery, from: &str, to: &str) -> Result<Document, LowerError> {
    let predicate = log_predicate(q)?;
    Ok(Document {
        ir_version: 1,
        from: "logs".to_string(),
        range: crate::ir_range(from, to),
        result: ResultEnvelope::Rows,
        fields: None,
        pipeline: predicate.into_iter().map(Stage::Where).collect(),
    })
}

/// The selector's matchers and the pipeline's filters, conjoined. `None` when
/// the query filters nothing — an empty `and` would be a claim about the rows.
fn log_predicate(q: &LogQuery) -> Result<Option<Predicate>, LowerError> {
    let mut parts = selector_predicates(&q.selector)?;
    for stage in &q.pipeline {
        match stage {
            PipelineStage::LineFilter(f) => parts.push(line_filter(f)?),
            // Parser stages, formatters and drop/keep reshape the result
            // without narrowing it, so they contribute no predicate. The IR
            // expresses parsing as an `extract` stage; wiring that up is
            // separate work, and silently ignoring a *filtering* stage would
            // be the bug this crate exists to avoid.
            PipelineStage::Json(_)
            | PipelineStage::Logfmt(_)
            | PipelineStage::LineFormat(_)
            | PipelineStage::LabelFormat(_)
            | PipelineStage::Decolorize
            | PipelineStage::Unpack => {}
            other => {
                return Err(LowerError::Inexpressible(format!(
                    "LogQL pipeline stage {other:?}"
                )));
            }
        }
    }
    Ok(match parts.len() {
        0 => None,
        1 => Some(parts.remove(0)),
        _ => Some(Predicate::And(parts)),
    })
}

fn selector_predicates(s: &StreamSelector) -> Result<Vec<Predicate>, LowerError> {
    s.matchers.iter().map(matcher).collect()
}

/// One label matcher.
///
/// D9 (`ir-single-lowering`): Loki (and Prometheus) treat a missing label as
/// the empty string — `{foo!="bar"}` matches a stream without `foo` at all,
/// and `{foo=""}` matches absent the same way a present empty value would.
/// The IR's own `not` is Kleene: absent satisfies neither a comparison nor
/// its negation, which is *not* what the compat surface promises here. So a
/// negative matcher — `!=`, `!~`, or `=` against `""` — explicitly ORs in
/// "the field does not exist" rather than relying on `not` alone:
/// `!=` → `or[ne, not(exists)]`, `!~` → `or[not(regex), not(exists)]`,
/// `=""` → `or[eq "", not(exists)]`. `=~` stays a plain `regex` — a pattern
/// that matches the empty string would match absent in real Loki too, but
/// that corner is recorded here, not emulated.
fn matcher(m: &LabelMatcher) -> Result<Predicate, LowerError> {
    let field = label_field(&m.name);
    let leaf = |op: ComparisonOp| {
        Predicate::Leaf(Leaf {
            field: field.clone(),
            op,
            value: Some(serde_json::Value::String(m.value.clone())),
        })
    };
    let not_exists = || {
        Predicate::Not(Box::new(Predicate::Leaf(Leaf {
            field: field.clone(),
            op: ComparisonOp::Exists,
            value: None,
        })))
    };
    Ok(match m.op {
        MatchOp::Eq if m.value.is_empty() => {
            Predicate::Or(vec![leaf(ComparisonOp::Eq), not_exists()])
        }
        MatchOp::Eq => leaf(ComparisonOp::Eq),
        MatchOp::Neq => Predicate::Or(vec![leaf(ComparisonOp::Ne), not_exists()]),
        MatchOp::Re => leaf(ComparisonOp::Regex),
        MatchOp::Nre => Predicate::Or(vec![
            Predicate::Not(Box::new(leaf(ComparisonOp::Regex))),
            not_exists(),
        ]),
        // `MatchOp` is `#[non_exhaustive]`; a newer parser may know an
        // operator this build cannot name.
        ref other => {
            return Err(LowerError::Inexpressible(format!(
                "LogQL matcher operator {other:?}"
            )));
        }
    })
}

/// The logical field a LogQL label addresses.
///
/// Loki's stream labels are flat strings; SignalDB's logical namespace is
/// OTel-dotted. The well-known ones are renamed, and anything else passes
/// through bare so the resolver coalesces it across attribute containers —
/// which is what Loki's own semantics imply.
/// The same aliases `querier::query::logql::column_for_label` recognises —
/// kept in step with it deliberately. That function maps to *physical*
/// columns; this one maps to *logical* fields, so the targets differ by layer,
/// but the set of labels being special-cased must not. `trace_id`/`span_id`
/// are listed even though they pass through unchanged, so a future alias added
/// on either side is visibly missing from the other.
fn label_field(label: &str) -> String {
    match label {
        "service_name" | "service" | "job" => "service.name",
        "level" | "severity" | "detected_level" => "severity_text",
        "trace_id" => "trace_id",
        "span_id" => "span_id",
        other => other,
    }
    .to_string()
}

/// A line filter matches the log body.
fn line_filter(f: &LineFilter) -> Result<Predicate, LowerError> {
    if f.is_ip {
        return Err(LowerError::Inexpressible(
            "LogQL ip() line filter".to_string(),
        ));
    }
    let leaf = |op| {
        Predicate::Leaf(Leaf {
            field: "body".to_string(),
            op,
            value: Some(serde_json::Value::String(f.value.clone())),
        })
    };
    Ok(match f.op {
        LineFilterOp::Contains => leaf(ComparisonOp::Contains),
        LineFilterOp::NotContains => Predicate::Not(Box::new(leaf(ComparisonOp::Contains))),
        LineFilterOp::Regex => leaf(ComparisonOp::Regex),
        LineFilterOp::NotRegex => Predicate::Not(Box::new(leaf(ComparisonOp::Regex))),
        ref other => {
            return Err(LowerError::Inexpressible(format!(
                "LogQL line-filter operator {other:?}"
            )));
        }
    })
}

/// The default grouping for a range aggregation with no outer `by` at all
/// (design D7 of `ir-single-lowering`): in real Loki, `count_over_time(...)`
/// alone is one series per matching *stream*, not one series total. The old
/// querier path implemented that by defaulting to these two dedicated
/// columns when ungrouped (`logs.rs::SERIES_COLUMNS`); the querier pins its
/// physical constant against this one (this crate has no access to the real
/// `LogicalSchema`, so the mapping is asserted, not derived, on that side).
pub const STREAM_IDENTITY: &[&str] = &["service.name", "severity_text"];

/// A metric query: the inner log query's filter, then one aggregate.
fn lower_metric_query(q: &MetricQuery, from: &str, to: &str) -> Result<Document, LowerError> {
    let (range_agg, grouping) = match q {
        // D7: a *bare* range aggregation — no outer vector aggregation at
        // all — defaults to the stream identity rather than collapsing
        // every matching row into one series.
        MetricQuery::Range(r) => (r, STREAM_IDENTITY.iter().map(|s| s.to_string()).collect()),
        // An *explicit* vector aggregation with no `by()` means something
        // different: `sum(count_over_time(...))` collapses every matching
        // stream into one series (real Loki's behavior, and the old path's
        // `logs.rs::execute_plan` — its `Some(_)` outer_agg branch reduces
        // across every series when `out_group_cols` is empty). The D7
        // default must not apply here; `vector_grouping` already returns an
        // empty grouping for exactly this case, and it means "collapse",
        // not "no grouping specified yet".
        MetricQuery::Vector(v) => match &v.inner {
            MetricQuery::Range(r) => (r, vector_grouping(v, range_function(r)?.0)?),
            _ => {
                return Err(LowerError::Inexpressible(
                    "a LogQL vector aggregation over anything but a range aggregation".to_string(),
                ));
            }
        },
        other => {
            return Err(LowerError::Inexpressible(format!(
                "LogQL metric query {}",
                metric_kind(other)
            )));
        }
    };

    let (func, divisor) = range_function(range_agg)?;

    // Field-taking aggregates need an operand. LogQL supplies it through the
    // `unwrap` stage, which it requires for exactly these functions; without
    // one there is nothing to aggregate and the IR would reject the document.
    let of = if func.needs_field() {
        let unwrap = range_agg.unwrap.as_ref().ok_or_else(|| {
            LowerError::Inexpressible(format!(
                "LogQL {:?} without an `unwrap` stage",
                range_agg.function
            ))
        })?;
        Some(label_field(&unwrap.label))
    } else {
        None
    };

    let step = humanize(range_agg.range);

    let mut pipeline = Vec::new();
    if let Some(p) = log_predicate(&range_agg.log_query)? {
        pipeline.push(Stage::Where(p));
    }
    pipeline.push(Stage::Aggregate(Aggregate {
        by: grouping,
        step: Some(step),
        aggs: vec![Agg {
            func,
            of,
            arg: None,
            divisor,
            as_name: "value".to_string(),
            scope: None,
        }],
    }));

    let mut doc = Document {
        // Filled in below from the pipeline itself rather than guessed here:
        // which version a shape needs is the IR's fact to state, not ours.
        ir_version: 1,
        from: "logs".to_string(),
        range: crate::ir_range(from, to),
        result: ResultEnvelope::Series,
        fields: None,
        pipeline,
    };
    doc.ir_version = doc.minimum_ir_version();
    Ok(doc)
}

/// The aggregate a range function reduces to, and its divisor if it reports
/// per unit time. `rate` is exactly `count` divided by the window.
fn range_function(r: &logql::RangeAggregation) -> Result<(AggFn, Option<f64>), LowerError> {
    let seconds = r.range.as_secs_f64();
    Ok(match r.function {
        RangeFunction::CountOverTime => (AggFn::Count, None),
        RangeFunction::Rate => (AggFn::Count, Some(seconds)),
        RangeFunction::SumOverTime => (AggFn::Sum, None),
        RangeFunction::AvgOverTime => (AggFn::Avg, None),
        RangeFunction::MinOverTime => (AggFn::Min, None),
        RangeFunction::MaxOverTime => (AggFn::Max, None),
        RangeFunction::StddevOverTime => (AggFn::Stddev, None),
        RangeFunction::StdvarOverTime => (AggFn::Stdvar, None),
        RangeFunction::FirstOverTime => (AggFn::First, None),
        RangeFunction::LastOverTime => (AggFn::Last, None),
        ref other => {
            return Err(LowerError::Inexpressible(format!(
                "LogQL range function {other:?}"
            )));
        }
    })
}

/// A vector aggregation's grouping labels, once the pairing is checked.
///
/// LogQL aggregates twice — the range function per stream, then the vector
/// function across streams — while the IR aggregates once. Collapsing the two
/// is only sound when applying the outer function to partial results
/// reproduces the inner one over their union:
///
/// | outer | inner | sound because |
/// |-------|-------|---------------|
/// | `sum` | `count_over_time`, `rate` | counts over disjoint streams add |
/// | `sum` | `sum_over_time` | sums add |
/// | `min` | `min_over_time` | the min of minima is the minimum |
/// | `max` | `max_over_time` | likewise |
///
/// Anything else means something different collapsed than spelled: the average
/// of per-stream counts is not a count. Those are refused, because answering a
/// neighbouring question silently is worse than refusing this one.
fn vector_grouping(v: &logql::VectorAggregation, inner: AggFn) -> Result<Vec<String>, LowerError> {
    let collapses = matches!(
        (v.function, inner),
        (AggregationFunction::Sum, AggFn::Count | AggFn::Sum)
            | (AggregationFunction::Min, AggFn::Min)
            | (AggregationFunction::Max, AggFn::Max)
    );
    if !collapses {
        return Err(LowerError::Inexpressible(format!(
            "LogQL {:?} over {:?} — the IR aggregates once, and this pairing \
             does not collapse into a single aggregate",
            v.function, inner
        )));
    }
    match &v.grouping {
        None => Ok(Vec::new()),
        // `without` names the complement of a label set, and the IR groups by
        // naming fields — the complement is not knowable until the data is
        // read, so there is nothing honest to emit.
        Some(g) if g.without => Err(LowerError::Inexpressible(
            "LogQL `without` grouping".to_string(),
        )),
        Some(g) => Ok(g.labels.iter().map(|l| label_field(l)).collect()),
    }
}

fn metric_kind(q: &MetricQuery) -> &'static str {
    match q {
        MetricQuery::Range(_) => "range aggregation",
        MetricQuery::Vector(_) => "vector aggregation",
        MetricQuery::Binary(_) => "binary operation",
        MetricQuery::LabelReplace(_) => "label_replace",
        _ => "construct",
    }
}

/// A duration as the IR spells a step. Whole units where possible, so a 5m
/// window reads `5m` rather than `300s`.
///
/// Sub-second windows are spelled in milliseconds. Truncating to whole seconds
/// turned `[500ms]` into a `0s` step, which is not a window at all.
fn humanize(d: std::time::Duration) -> String {
    let millis = d.as_millis();
    if !millis.is_multiple_of(1000) {
        return format!("{millis}ms");
    }
    let secs = d.as_secs();
    if secs > 0 && secs.is_multiple_of(3600) {
        format!("{}h", secs / 3600)
    } else if secs > 0 && secs.is_multiple_of(60) {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    }
}
