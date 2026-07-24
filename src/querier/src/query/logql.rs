//! # LogQL → DataFusion filter lowering (log/stream queries)
//!
//! Lowers a parsed [`logql::LogQuery`] — a stream selector plus a log
//! pipeline — into a DataFusion filter [`Expr`] over the logs table,
//! mirroring how [`super::search_filter`] lowers trace search conditions.
//! The querier feeds the result to `df.filter(expr)`, so LogQL values
//! never enter a SQL string.
//!
//! ## Column mapping
//!
//! A small set of well-known LogQL labels map to dedicated logs columns;
//! any other label is matched against the flat-JSON attribute columns
//! (`log_attributes` / `resource_attributes`) by the serialized
//! `"key":"value"` fragment — the same substring approximation the trace
//! search path uses (see [`super::search_filter`]).
//!
//! | LogQL label | Column |
//! |-------------|--------|
//! | `service_name`, `service`, `job` | `service_name` |
//! | `level`, `severity`, `detected_level` | `severity_text` |
//! | `trace_id` | `trace_id` |
//! | `span_id` | `span_id` |
//! | anything else | attribute JSON |
//!
//! Line filters (`|=`, `!=`, `|~`, `!~`) match the `body` column.

use datafusion::functions::regex::expr_fn::regexp_like;
use datafusion::functions::string::expr_fn::contains;
use datafusion::logical_expr::{Expr, col, lit, not};
use logql::{
    FilterOp, FilterValue, LabelFilterExpr, LabelMatcher, LineFilter, LineFilterOp, LogQuery,
    MatchOp, PipelineStage,
};

use super::error::QuerierError;

/// Attribute columns searched for a label that is not a dedicated column.
const LOG_ATTRIBUTES: &str = "log_attributes";
const RESOURCE_ATTRIBUTES: &str = "resource_attributes";

/// Lower a LogQL log query to a combined filter expression over the logs
/// table, or `None` when the query selects everything in range (`{}`).
///
/// The caller ANDs in the request's time-range predicate.
pub fn log_query_filter(query: &LogQuery) -> Result<Option<Expr>, QuerierError> {
    let mut exprs: Vec<Expr> = Vec::new();

    for matcher in &query.selector.matchers {
        exprs.push(matcher_expr(matcher)?);
    }
    for stage in &query.pipeline {
        if let Some(expr) = stage_expr(stage)? {
            exprs.push(expr);
        }
    }

    Ok(exprs.into_iter().reduce(Expr::and))
}

/// Lower a stream-selector matcher.
fn matcher_expr(matcher: &LabelMatcher) -> Result<Expr, QuerierError> {
    let op = match matcher.op {
        MatchOp::Eq => FilterOp::Eq,
        MatchOp::Neq => FilterOp::Neq,
        MatchOp::Re => FilterOp::Re,
        MatchOp::Nre => FilterOp::Nre,
    };
    label_expr(
        &matcher.name,
        op,
        &FilterValue::String(matcher.value.clone()),
    )
}

/// Lower a pipeline stage to an optional filter. Parser stages,
/// formatters, and drop/keep reshape results without filtering rows.
fn stage_expr(stage: &PipelineStage) -> Result<Option<Expr>, QuerierError> {
    match stage {
        PipelineStage::LineFilter(f) => Ok(Some(line_filter_expr(f)?)),
        PipelineStage::LabelFilter(expr) => Ok(Some(label_filter_expr(expr)?)),
        PipelineStage::Json(_)
        | PipelineStage::Logfmt(_)
        | PipelineStage::Regexp(_)
        | PipelineStage::Pattern(_)
        | PipelineStage::Unpack
        | PipelineStage::Decolorize
        | PipelineStage::LineFormat(_)
        | PipelineStage::LabelFormat(_)
        | PipelineStage::Drop(_)
        | PipelineStage::Keep(_)
        | PipelineStage::Distinct(_) => Ok(None),
        PipelineStage::Unwrap(_) => Err(QuerierError::Unsupported(
            "unwrap outside a metric query".to_string(),
        )),
    }
}

/// Lower a line filter to a predicate over the `body` column.
fn line_filter_expr(f: &LineFilter) -> Result<Expr, QuerierError> {
    if f.is_ip {
        return Err(QuerierError::Unsupported(
            "ip() line filter is not supported yet".to_string(),
        ));
    }
    let body = col("body");
    Ok(match f.op {
        LineFilterOp::Contains => contains(body, lit(f.value.clone())),
        LineFilterOp::NotContains => not(contains(body, lit(f.value.clone()))),
        LineFilterOp::Regex => regexp_like(body, lit(f.value.clone()), None),
        LineFilterOp::NotRegex => not(regexp_like(body, lit(f.value.clone()), None)),
    })
}

/// Lower a label-filter expression, preserving its `and`/`or` structure.
fn label_filter_expr(expr: &LabelFilterExpr) -> Result<Expr, QuerierError> {
    match expr {
        LabelFilterExpr::Pred(p) => label_expr(&p.name, p.op, &p.value),
        LabelFilterExpr::And(a, b) => Ok(label_filter_expr(a)?.and(label_filter_expr(b)?)),
        LabelFilterExpr::Or(a, b) => Ok(label_filter_expr(a)?.or(label_filter_expr(b)?)),
    }
}

/// A well-known LogQL label mapped to its dedicated column name.
fn column_for_label(label: &str) -> Option<&'static str> {
    match label {
        "service_name" | "service" | "job" => Some("service_name"),
        "level" | "severity" | "detected_level" => Some("severity_text"),
        "trace_id" => Some("trace_id"),
        "span_id" => Some("span_id"),
        _ => None,
    }
}

/// Lower a `name op value` predicate against a column or the attribute
/// JSON columns.
fn label_expr(name: &str, op: FilterOp, value: &FilterValue) -> Result<Expr, QuerierError> {
    match column_for_label(name) {
        Some(column) => column_expr(column, op, value),
        None => attribute_expr(name, op, value),
    }
}

/// Predicate against a dedicated string column.
fn column_expr(column: &str, op: FilterOp, value: &FilterValue) -> Result<Expr, QuerierError> {
    let c = col(column);
    let v = string_value(value)?;
    Ok(match op {
        FilterOp::Eq | FilterOp::CmpEq => c.eq(lit(v)),
        FilterOp::Neq => c.not_eq(lit(v)),
        FilterOp::Re => regexp_like(c, lit(v), None),
        FilterOp::Nre => not(regexp_like(c, lit(v), None)),
        FilterOp::Gt | FilterOp::Gte | FilterOp::Lt | FilterOp::Lte => {
            return Err(QuerierError::Unsupported(format!(
                "ordered comparison on log label column '{column}'"
            )));
        }
    })
}

/// Predicate against the flat-JSON attribute columns, matching the
/// serialized `"key":value` fragment in either attribute column.
fn attribute_expr(key: &str, op: FilterOp, value: &FilterValue) -> Result<Expr, QuerierError> {
    let fragment = attribute_fragment(key, value)?;
    let in_log = contains(col(LOG_ATTRIBUTES), lit(fragment.clone()));
    let in_resource = contains(col(RESOURCE_ATTRIBUTES), lit(fragment));
    match op {
        // Present with this value in either attribute column.
        FilterOp::Eq | FilterOp::CmpEq => Ok(in_log.or(in_resource)),
        // Absent from both — pushed in via De Morgan so the negation is
        // unambiguous (`NOT a AND NOT b`, not `NOT (a OR b)`).
        FilterOp::Neq => Ok(not(in_log).and(not(in_resource))),
        _ => Err(QuerierError::Unsupported(format!(
            "operator on attribute label '{key}' (attributes support only = and !=)"
        ))),
    }
}

/// Build the `"key":value` JSON fragment for attribute matching.
fn attribute_fragment(key: &str, value: &FilterValue) -> Result<String, QuerierError> {
    let json_key = serde_json::to_string(key).unwrap_or_else(|_| format!("\"{key}\""));
    let json_value = match value {
        FilterValue::String(s) => serde_json::to_string(s).unwrap_or_else(|_| format!("\"{s}\"")),
        FilterValue::Number(n) => n.to_string(),
        FilterValue::Bytes(b) => b.to_string(),
        FilterValue::Duration(d) => d.as_nanos().to_string(),
        FilterValue::Ip(_) => {
            return Err(QuerierError::Unsupported(
                "ip() label filter is not supported yet".to_string(),
            ));
        }
    };
    Ok(format!("{json_key}:{json_value}"))
}

/// The comparison string for a column predicate.
fn string_value(value: &FilterValue) -> Result<String, QuerierError> {
    Ok(match value {
        FilterValue::String(s) => s.clone(),
        FilterValue::Number(n) => n.to_string(),
        FilterValue::Bytes(b) => b.to_string(),
        FilterValue::Duration(d) => d.as_nanos().to_string(),
        FilterValue::Ip(_) => {
            return Err(QuerierError::Unsupported(
                "ip() label filter is not supported yet".to_string(),
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use logql::parse_query;

    /// Lower a query and render the filter as DataFusion's SQL-like
    /// `Display`, which is stable and readable — so these assertions pin
    /// the exact transpilation outcome, not just fragments of it.
    fn sql(query: &str) -> String {
        let q = parse_query(query).expect("parse");
        let expr = log_query_filter(&q).expect("lower").expect("some filter");
        format!("{expr}")
    }

    fn lower(query: &str) -> Result<Option<Expr>, QuerierError> {
        log_query_filter(&parse_query(query).expect("parse"))
    }

    #[test]
    fn service_name_selector() {
        assert_eq!(
            sql(r#"{service_name="api"}"#),
            r#"service_name = Utf8("api")"#
        );
    }

    #[test]
    fn well_known_label_aliases() {
        assert_eq!(sql(r#"{job="api"}"#), r#"service_name = Utf8("api")"#);
        assert_eq!(sql(r#"{service="api"}"#), r#"service_name = Utf8("api")"#);
        assert_eq!(
            sql(r#"{level="error"}"#),
            r#"severity_text = Utf8("error")"#
        );
        assert_eq!(
            sql(r#"{detected_level="warn"}"#),
            r#"severity_text = Utf8("warn")"#
        );
        assert_eq!(sql(r#"{trace_id="abc"}"#), r#"trace_id = Utf8("abc")"#);
        assert_eq!(sql(r#"{span_id="def"}"#), r#"span_id = Utf8("def")"#);
    }

    #[test]
    fn column_matcher_operators() {
        assert_eq!(
            sql(r#"{service_name!="api"}"#),
            r#"service_name != Utf8("api")"#
        );
        assert_eq!(
            sql(r#"{service_name=~"ap.*"}"#),
            r#"regexp_like(service_name, Utf8("ap.*"))"#
        );
        assert_eq!(
            sql(r#"{service_name!~"ap.*"}"#),
            r#"NOT regexp_like(service_name, Utf8("ap.*"))"#
        );
    }

    #[test]
    fn unknown_label_matches_both_attribute_columns() {
        assert_eq!(
            sql(r#"{namespace="prod"}"#),
            r#"contains(log_attributes, Utf8(""namespace":"prod"")) OR contains(resource_attributes, Utf8(""namespace":"prod""))"#
        );
    }

    #[test]
    fn attribute_neq_uses_de_morgan() {
        // `NOT a AND NOT b`, never the ambiguous `NOT (a OR b)`.
        assert_eq!(
            sql(r#"{namespace!="prod"}"#),
            r#"NOT contains(log_attributes, Utf8(""namespace":"prod"")) AND NOT contains(resource_attributes, Utf8(""namespace":"prod""))"#
        );
    }

    #[test]
    fn line_filters() {
        assert_eq!(
            sql(r#"{service_name="s"} |= "boom""#),
            r#"service_name = Utf8("s") AND contains(body, Utf8("boom"))"#
        );
        assert_eq!(
            sql(r#"{service_name="s"} != "x""#),
            r#"service_name = Utf8("s") AND NOT contains(body, Utf8("x"))"#
        );
        assert_eq!(
            sql(r#"{service_name="s"} |~ "e.*r""#),
            r#"service_name = Utf8("s") AND regexp_like(body, Utf8("e.*r"))"#
        );
        assert_eq!(
            sql(r#"{service_name="s"} !~ "e.*r""#),
            r#"service_name = Utf8("s") AND NOT regexp_like(body, Utf8("e.*r"))"#
        );
    }

    #[test]
    fn label_filter_after_parser_stage_uses_known_column() {
        assert_eq!(
            sql(r#"{service_name="s"} | json | level="error""#),
            r#"service_name = Utf8("s") AND severity_text = Utf8("error")"#
        );
    }

    #[test]
    fn label_filter_or_is_parenthesized_under_the_conjunction() {
        assert_eq!(
            sql(r#"{service_name="s"} | logfmt | service="x" or service="y""#),
            r#"service_name = Utf8("s") AND (service_name = Utf8("x") OR service_name = Utf8("y"))"#
        );
    }

    #[test]
    fn full_query_folds_left_associatively() {
        assert_eq!(
            sql(r#"{service_name="api", env="prod"} |= "error""#),
            r#"service_name = Utf8("api") AND (contains(log_attributes, Utf8(""env":"prod"")) OR contains(resource_attributes, Utf8(""env":"prod""))) AND contains(body, Utf8("error"))"#
        );
    }

    #[test]
    fn empty_selector_yields_no_filter() {
        assert_eq!(lower("{}").unwrap(), None);
    }

    #[test]
    fn ip_and_attribute_regex_are_unsupported() {
        assert!(matches!(
            lower(r#"{a="b"} |= ip("10.0.0.0/8")"#),
            Err(QuerierError::Unsupported(_))
        ));
        assert!(matches!(
            lower(r#"{a="b"} | logfmt | foo=~"bar.*""#),
            Err(QuerierError::Unsupported(_))
        ));
    }
}
