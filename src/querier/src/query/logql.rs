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

use std::collections::HashSet;

use common::schema::materialized_column_name;
use datafusion::arrow::datatypes::DataType;
use datafusion::functions::regex::expr_fn::regexp_like;
use datafusion::functions::string::expr_fn::contains;
use datafusion::logical_expr::{Expr, cast, col, lit, not};
use logql::{
    FilterOp, FilterValue, LabelFilterExpr, LabelMatcher, LineFilter, LineFilterOp, LogQuery,
    MatchOp, PipelineStage,
};

use super::error::QuerierError;

/// The error for a construct the `logql` crate parsed but this build does not
/// lower.
///
/// The parser is published and released independently of the querier
/// (openspec design D9), so its AST types are `#[non_exhaustive]` and a
/// newer parser can hand us a variant this build predates. Every wildcard arm
/// in this module and in [`super::logql_metric`] funnels here rather than
/// guessing at a filter — a wrong filter returns wrong rows silently, while
/// this returns an honest "not implemented".
pub(super) fn unlowerable(what: &str, construct: &impl std::fmt::Debug) -> QuerierError {
    QuerierError::Unsupported(format!(
        "LogQL {what} {construct:?} is recognised by the parser but not lowered by this build"
    ))
}

/// The set of materialized `label_<key>` columns present in the table being
/// queried. A label in this set routes to its dedicated column (exact,
/// regex, and ordered comparisons); anything else falls back to the
/// attribute-JSON substring match.
pub type MaterializedColumns = HashSet<String>;

/// What the target table offers for attribute matching: which materialized
/// `label_<key>` columns exist, whether the attribute columns are typed
/// maps (new tables) or JSON strings (legacy tables), and whether the
/// derived `attr_tokens` column exists for bloom-backed containment checks.
#[derive(Debug, Default, Clone)]
pub struct AttrContext {
    pub materialized: MaterializedColumns,
    pub map_attrs: bool,
    pub attr_tokens: bool,
}

/// Attribute columns searched for a label that is not a dedicated column.
const LOG_ATTRIBUTES: &str = "log_attributes";
const RESOURCE_ATTRIBUTES: &str = "resource_attributes";

/// Lower a LogQL log query to a combined filter expression over the logs
/// table, or `None` when the query selects everything in range (`{}`).
///
/// The caller ANDs in the request's time-range predicate.
pub fn log_query_filter(query: &LogQuery) -> Result<Option<Expr>, QuerierError> {
    log_query_filter_with_columns(query, &AttrContext::default())
}

/// Like [`log_query_filter`], but aware of which materialized `label_<key>`
/// columns exist in the target table, so attribute labels backed by a
/// column are matched exactly instead of by JSON substring.
pub fn log_query_filter_with_columns(
    query: &LogQuery,
    ctx: &AttrContext,
) -> Result<Option<Expr>, QuerierError> {
    let mut exprs: Vec<Expr> = Vec::new();

    for matcher in &query.selector.matchers {
        exprs.push(matcher_expr(matcher, ctx)?);
    }
    for stage in &query.pipeline {
        if let Some(expr) = stage_expr(stage, ctx)? {
            exprs.push(expr);
        }
    }

    Ok(exprs.into_iter().reduce(Expr::and))
}

/// Lower a stream-selector matcher.
fn matcher_expr(matcher: &LabelMatcher, ctx: &AttrContext) -> Result<Expr, QuerierError> {
    let op = match matcher.op {
        MatchOp::Eq => FilterOp::Eq,
        MatchOp::Neq => FilterOp::Neq,
        MatchOp::Re => FilterOp::Re,
        MatchOp::Nre => FilterOp::Nre,
        // The logql crate releases on its own cadence (openspec D9), so it can
        // parse a construct this build does not lower. Reporting that beats
        // filtering on a guess. Same for every other wildcard in this module.
        ref other => return Err(unlowerable("stream matcher operator", other)),
    };
    label_expr(
        &matcher.name,
        op,
        &FilterValue::String(matcher.value.clone()),
        ctx,
    )
}

/// Lower a pipeline stage to an optional filter. Parser stages,
/// formatters, and drop/keep reshape results without filtering rows.
fn stage_expr(stage: &PipelineStage, ctx: &AttrContext) -> Result<Option<Expr>, QuerierError> {
    match stage {
        PipelineStage::LineFilter(f) => Ok(Some(line_filter_expr(f)?)),
        PipelineStage::LabelFilter(expr) => Ok(Some(label_filter_expr(expr, ctx)?)),
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
        other => Err(unlowerable("pipeline stage", other)),
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
        ref other => return Err(unlowerable("line-filter operator", other)),
    })
}

/// Lower a label-filter expression, preserving its `and`/`or` structure.
fn label_filter_expr(expr: &LabelFilterExpr, ctx: &AttrContext) -> Result<Expr, QuerierError> {
    match expr {
        LabelFilterExpr::Pred(p) => label_expr(&p.name, p.op, &p.value, ctx),
        LabelFilterExpr::And(a, b) => {
            Ok(label_filter_expr(a, ctx)?.and(label_filter_expr(b, ctx)?))
        }
        LabelFilterExpr::Or(a, b) => Ok(label_filter_expr(a, ctx)?.or(label_filter_expr(b, ctx)?)),
        other => Err(unlowerable("label-filter expression", other)),
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

/// Lower a `name op value` predicate. Well-known labels resolve to their
/// dedicated column; other labels resolve to a materialized `label_<key>`
/// column when the table has one, else to the attribute-JSON substring
/// match.
fn label_expr(
    name: &str,
    op: FilterOp,
    value: &FilterValue,
    ctx: &AttrContext,
) -> Result<Expr, QuerierError> {
    if let Some(column) = column_for_label(name) {
        return column_expr(column, op, value);
    }
    let materialized = materialized_column_name(name);
    if ctx.materialized.contains(&materialized) {
        return materialized_label_expr(&materialized, op, value);
    }
    let base = if ctx.map_attrs {
        map_attribute_expr(name, op, value)?
    } else {
        attribute_expr(name, op, value)?
    };
    // Tables with the derived `attr_tokens` column get an extra exact
    // containment conjunct on equality filters: it never changes the
    // result (tokens are a superset of the attribute-column contents) but
    // gives the Parquet layer a bloom-filtered column to prune with.
    if ctx.attr_tokens && matches!(op, FilterOp::Eq | FilterOp::CmpEq) {
        let token = format!("{name}={}", string_value(value)?);
        return Ok(base.and(datafusion::functions_nested::expr_fn::array_has(
            col(common::schema::ATTR_TOKENS_COLUMN),
            lit(token),
        )));
    }
    Ok(base)
}

/// Predicate against typed `Map<Utf8, Utf8>` attribute columns: the value
/// is extracted per key with `get_field` and compared exactly — no
/// substring approximation. Regex and ordered comparisons work on any
/// attribute (ordered casts the value to `Float64`); negations also match
/// rows where the key is absent (NULL), mirroring the JSON path.
fn map_attribute_expr(key: &str, op: FilterOp, value: &FilterValue) -> Result<Expr, QuerierError> {
    use datafusion::functions::core::expr_fn::get_field;

    let in_log = get_field(col(LOG_ATTRIBUTES), key);
    let in_resource = get_field(col(RESOURCE_ATTRIBUTES), key);
    let both = |f: &dyn Fn(Expr) -> Expr| f(in_log.clone()).or(f(in_resource.clone()));
    let both_and = |f: &dyn Fn(Expr) -> Expr| f(in_log.clone()).and(f(in_resource.clone()));

    Ok(match op {
        FilterOp::Eq | FilterOp::CmpEq => {
            let v = string_value(value)?;
            both(&|e: Expr| e.eq(lit(v.clone())))
        }
        // Absent-or-different in *both* attribute maps.
        FilterOp::Neq => {
            let v = string_value(value)?;
            both_and(&|e: Expr| e.clone().is_null().or(e.not_eq(lit(v.clone()))))
        }
        FilterOp::Re => {
            let v = string_value(value)?;
            both(&|e: Expr| regexp_like(e, lit(v.clone()), None))
        }
        FilterOp::Nre => {
            let v = string_value(value)?;
            both_and(&|e: Expr| {
                e.clone()
                    .is_null()
                    .or(not(regexp_like(e, lit(v.clone()), None)))
            })
        }
        FilterOp::Gt | FilterOp::Gte | FilterOp::Lt | FilterOp::Lte => {
            let n = numeric_value(value)?;
            let cmp = move |e: Expr| {
                let casted = cast(e, DataType::Float64);
                match op {
                    FilterOp::Gt => casted.gt(lit(n)),
                    FilterOp::Gte => casted.gt_eq(lit(n)),
                    FilterOp::Lt => casted.lt(lit(n)),
                    FilterOp::Lte => casted.lt_eq(lit(n)),
                    _ => unreachable!(),
                }
            };
            both(&cmp)
        }
        ref other => return Err(unlowerable("label-filter operator", other)),
    })
}

/// Predicate against a materialized attribute column (nullable `Utf8`).
/// Exact/regex comparisons match the string value; ordered comparisons cast
/// the value to `Float64`. Negations also match rows where the label is
/// absent (NULL), mirroring the JSON path where an absent key satisfies
/// `!=`.
fn materialized_label_expr(
    column: &str,
    op: FilterOp,
    value: &FilterValue,
) -> Result<Expr, QuerierError> {
    let c = col(column);
    Ok(match op {
        FilterOp::Eq | FilterOp::CmpEq => c.eq(lit(string_value(value)?)),
        FilterOp::Neq => c.clone().is_null().or(c.not_eq(lit(string_value(value)?))),
        FilterOp::Re => regexp_like(c, lit(string_value(value)?), None),
        FilterOp::Nre => {
            c.clone()
                .is_null()
                .or(not(regexp_like(c, lit(string_value(value)?), None)))
        }
        FilterOp::Gt | FilterOp::Gte | FilterOp::Lt | FilterOp::Lte => {
            let n = numeric_value(value)?;
            let casted = cast(c, DataType::Float64);
            match op {
                FilterOp::Gt => casted.gt(lit(n)),
                FilterOp::Gte => casted.gt_eq(lit(n)),
                FilterOp::Lt => casted.lt(lit(n)),
                FilterOp::Lte => casted.lt_eq(lit(n)),
                _ => unreachable!(),
            }
        }
        ref other => return Err(unlowerable("label-filter operator", other)),
    })
}

/// The numeric form of a filter value for ordered comparisons. Durations
/// use nanoseconds and bytes their raw count, matching how the write path
/// serializes them.
fn numeric_value(value: &FilterValue) -> Result<f64, QuerierError> {
    Ok(match value {
        FilterValue::Number(n) => *n,
        FilterValue::Bytes(b) => *b as f64,
        FilterValue::Duration(d) => d.as_nanos() as f64,
        FilterValue::String(s) => s.parse::<f64>().map_err(|_| {
            QuerierError::Unsupported(format!(
                "ordered comparison needs a numeric value, got '{s}'"
            ))
        })?,
        FilterValue::Ip(_) => {
            return Err(QuerierError::Unsupported(
                "ordered comparison on an ip() value".to_string(),
            ));
        }
        other => return Err(unlowerable("filter value", other)),
    })
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
        ref other => return Err(unlowerable("label-filter operator", other)),
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
        other => return Err(unlowerable("filter value", other)),
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
        other => return Err(unlowerable("filter value", other)),
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

    /// Lower with a set of materialized `label_<key>` columns present.
    fn sql_with(query: &str, columns: &[&str]) -> String {
        let q = parse_query(query).expect("parse");
        let ctx = AttrContext {
            materialized: columns.iter().map(|c| c.to_string()).collect(),
            ..Default::default()
        };
        let expr = log_query_filter_with_columns(&q, &ctx)
            .expect("lower")
            .expect("some filter");
        format!("{expr}")
    }

    /// Lower against a map-typed attribute table.
    fn sql_map(query: &str) -> String {
        let q = parse_query(query).expect("parse");
        let ctx = AttrContext {
            materialized: MaterializedColumns::new(),
            map_attrs: true,
            ..Default::default()
        };
        let expr = log_query_filter_with_columns(&q, &ctx)
            .expect("lower")
            .expect("some filter");
        format!("{expr}")
    }

    /// Lower against a table that has the derived `attr_tokens` column.
    fn sql_tokens(query: &str, map_attrs: bool) -> String {
        let q = parse_query(query).expect("parse");
        let ctx = AttrContext {
            materialized: MaterializedColumns::new(),
            map_attrs,
            attr_tokens: true,
        };
        let expr = log_query_filter_with_columns(&q, &ctx)
            .expect("lower")
            .expect("some filter");
        format!("{expr}")
    }

    #[test]
    fn map_attribute_tables_get_exact_regex_and_ordered_matching() {
        // Exact equality per key via get_field on both attribute maps.
        let eq = sql_map(r#"{namespace="prod"}"#);
        assert!(
            eq.contains("get_field(log_attributes") && eq.contains(r#"= Utf8("prod")"#),
            "{eq}"
        );
        assert!(eq.contains("get_field(resource_attributes"), "{eq}");

        // Regex on any attribute — impossible on the JSON path.
        let re = sql_map(r#"{namespace=~"pro.*"}"#);
        assert!(re.contains("regexp_like(get_field(log_attributes"), "{re}");

        // Ordered comparison on any attribute, cast to Float64.
        let gt = sql_map(r#"{service_name="api"} | logfmt | status > 500"#);
        assert!(
            gt.contains("CAST(get_field(log_attributes") && gt.contains("Float64(500)"),
            "{gt}"
        );

        // != matches absent keys too (NULL-or-different in both maps).
        let ne = sql_map(r#"{namespace!="prod"}"#);
        assert!(ne.contains("IS NULL OR"), "{ne}");

        // The JSON path still rejects ordered/regex on attributes.
        let q = parse_query(r#"{service_name="api"} | logfmt | status > 500"#).expect("parse");
        assert!(matches!(
            log_query_filter_with_columns(&q, &AttrContext::default()),
            Err(QuerierError::Unsupported(_))
        ));
    }

    #[test]
    fn attr_tokens_adds_containment_conjunct_on_equality_only() {
        // Map-typed table with attr_tokens: the map predicate keeps the
        // exact semantics, the array_has conjunct adds bloom prunability.
        let eq = sql_tokens(r#"{namespace="prod"}"#, true);
        assert!(
            eq.contains("get_field(log_attributes") && eq.contains(r#"= Utf8("prod")"#),
            "{eq}"
        );
        assert!(
            eq.contains(r#"array_has(attr_tokens, Utf8("namespace=prod"))"#),
            "{eq}"
        );

        // Legacy JSON table with attr_tokens: substring predicate + conjunct.
        let eq_json = sql_tokens(r#"{namespace="prod"}"#, false);
        assert!(eq_json.contains("contains(log_attributes"), "{eq_json}");
        assert!(
            eq_json.contains(r#"array_has(attr_tokens, Utf8("namespace=prod"))"#),
            "{eq_json}"
        );

        // Non-equality operators stay untouched: no token conjunct.
        for query in [
            r#"{namespace!="prod"}"#,
            r#"{namespace=~"pro.*"}"#,
            r#"{namespace!~"pro.*"}"#,
        ] {
            let rendered = sql_tokens(query, true);
            assert!(!rendered.contains("array_has"), "{query} -> {rendered}");
        }

        // Well-known labels route to dedicated columns, never to tokens.
        let svc = sql_tokens(r#"{service_name="api"}"#, true);
        assert_eq!(svc, r#"service_name = Utf8("api")"#);

        // Materialized label columns also skip the token conjunct.
        let q = parse_query(r#"{namespace="prod"}"#).expect("parse");
        let ctx = AttrContext {
            materialized: ["label_namespace".to_string()].into_iter().collect(),
            map_attrs: true,
            attr_tokens: true,
        };
        let expr = format!(
            "{}",
            log_query_filter_with_columns(&q, &ctx)
                .expect("lower")
                .expect("some filter")
        );
        assert_eq!(expr, r#"label_namespace = Utf8("prod")"#);

        // Tables without the column are unchanged.
        assert!(!sql_map(r#"{namespace="prod"}"#).contains("array_has"));
    }

    #[test]
    fn materialized_label_routes_to_its_column() {
        // Without the column: attribute JSON substring match.
        assert_eq!(
            sql(r#"{namespace="prod"}"#),
            r#"contains(log_attributes, Utf8(""namespace":"prod"")) OR contains(resource_attributes, Utf8(""namespace":"prod""))"#
        );
        // With the column: exact equality on the dedicated column.
        assert_eq!(
            sql_with(r#"{namespace="prod"}"#, &["label_namespace"]),
            r#"label_namespace = Utf8("prod")"#
        );
        // `!=` also matches rows where the label is absent (NULL).
        assert_eq!(
            sql_with(r#"{namespace!="prod"}"#, &["label_namespace"]),
            r#"label_namespace IS NULL OR label_namespace != Utf8("prod")"#
        );
        // Regex against a materialized column.
        assert_eq!(
            sql_with(r#"{namespace=~"pr.*"}"#, &["label_namespace"]),
            r#"regexp_like(label_namespace, Utf8("pr.*"))"#
        );
    }

    #[test]
    fn materialized_label_supports_ordered_comparison() {
        // `| status > 500` on a materialized column casts to Float64.
        assert_eq!(
            sql_with(
                r#"{service_name="api"} | logfmt | status > 500"#,
                &["label_status"]
            ),
            r#"service_name = Utf8("api") AND CAST(label_status AS Float64) > Float64(500)"#
        );
        // Without the column, ordered comparison on an attribute is rejected.
        let q = parse_query(r#"{service_name="api"} | logfmt | status > 500"#).expect("parse");
        assert!(matches!(
            log_query_filter_with_columns(&q, &AttrContext::default()),
            Err(QuerierError::Unsupported(_))
        ));
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
