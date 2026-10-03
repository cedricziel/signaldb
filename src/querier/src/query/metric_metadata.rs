//! # Metric metadata service
//!
//! The Prometheus metadata reads (`labels`, `label/{name}/values`, `series`)
//! over the wide `metrics` table. Sample queries do not live here: they run
//! on the Query IR (`ir_planner`).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::functions::regex::expr_fn::regexp_like;
use datafusion::functions::string::expr_fn::contains;
use datafusion::logical_expr::{Expr, col, lit, not};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::scalar::ScalarValue;
use promql_parser::parser::{self, Expr as PromExpr};

use super::error::QuerierError;
use super::table_lookup::{
    LABEL_SCAN_LIMIT, distinct_non_empty, metric_type_filter, optional_table, string_column,
    time_window,
};

const GAUGE_SUM_TYPES: &[&str] = &["gauge", "sum"];

const LOG_ATTRIBUTES: &str = "attributes";
const RESOURCE_ATTRIBUTES: &str = "resource_attributes";

/// Label matcher operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MatchKind {
    Eq,
    Neq,
    Re,
    Nre,
}

/// A label matcher on a selector (excluding `__name__`).
#[derive(Debug, Clone, PartialEq, Eq)]
struct LabelMatch {
    name: String,
    op: MatchKind,
    value: String,
}

/// A parsed series selector: the metric name comparison plus label matchers.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Selector {
    metric_name: String,
    metric_name_op: MatchKind,
    matchers: Vec<LabelMatch>,
}

fn match_kind(op: &promql_parser::label::MatchOp) -> MatchKind {
    use promql_parser::label::MatchOp;
    match op {
        MatchOp::Equal => MatchKind::Eq,
        MatchOp::NotEqual => MatchKind::Neq,
        MatchOp::Re(_) => MatchKind::Re,
        MatchOp::NotRe(_) => MatchKind::Nre,
    }
}

/// Parse a `series` selector such as `up{job="api"}` or `{__name__=~"up.*"}`.
/// Dotted OTel metric names are accepted bare, as on the query path.
fn parse_selector(selector: &str) -> Result<Selector, QuerierError> {
    let rewritten = ql_ir::quote_dotted_metric_names(selector);
    let expr = parser::parse(&rewritten)
        .map_err(|e| QuerierError::InvalidInput(format!("invalid PromQL: {e}")))?;
    let mut expr = &expr;
    while let PromExpr::Paren(p) = expr {
        expr = &p.expr;
    }
    let PromExpr::VectorSelector(vs) = expr else {
        return Err(QuerierError::InvalidInput(
            "series selector must be an instant vector selector".to_string(),
        ));
    };
    // The metric name may be given directly or via a `__name__` matcher; a
    // matcher other than `=` (regex, negated) is kept as-is.
    let mut metric_name = vs.name.clone();
    let mut metric_name_op = MatchKind::Eq;
    let mut matchers = Vec::new();
    for m in &vs.matchers.matchers {
        if m.name == "__name__" {
            if metric_name.is_none() {
                metric_name = Some(m.value.clone());
                metric_name_op = match_kind(&m.op);
            }
            continue;
        }
        matchers.push(LabelMatch {
            name: m.name.clone(),
            op: match_kind(&m.op),
            value: m.value.clone(),
        });
    }
    let metric_name = metric_name
        .ok_or_else(|| QuerierError::InvalidInput("selector has no metric name".to_string()))?;
    Ok(Selector {
        metric_name,
        metric_name_op,
        matchers,
    })
}

/// Answers the Prometheus metadata endpoints from the `metrics` table.
#[derive(Clone)]
pub struct MetricMetadataService {
    session_context: Arc<SessionContext>,
}

// `SessionContext` isn't `Debug`, so `#[derive(Debug)]` doesn't apply here.
impl Debug for MetricMetadataService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricMetadataService")
            .field("session_context", &"set")
            .finish()
    }
}

impl MetricMetadataService {
    pub fn new(session_context: SessionContext) -> Self {
        Self {
            session_context: Arc::new(session_context),
        }
    }

    /// Scans the wide `metrics` table (D10) filtered to `metric_type IN
    /// (metric_types)`. `None` when the dataset has no `metrics` table.
    async fn scan_metrics(
        &self,
        tenant_slug: &str,
        dataset_slug: &str,
        metric_types: &[&str],
    ) -> Result<Option<DataFrame>, QuerierError> {
        let Some(df) =
            optional_table(&self.session_context, tenant_slug, dataset_slug, "metrics").await?
        else {
            return Ok(None);
        };
        Ok(Some(
            df.filter(metric_type_filter(metric_types))
                .map_err(QuerierError::QueryFailed)?,
        ))
    }

    /// List the Prometheus label names present in the window: the
    /// well-known ones (`__name__`, `job`) plus attribute keys discovered
    /// in the `attributes`/`resource_attributes` documents.
    pub async fn get_labels(
        &self,
        start: i64,
        end: i64,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<String>, QuerierError> {
        let mut labels: BTreeSet<String> =
            ["__name__", "job"].iter().map(|s| s.to_string()).collect();
        let Some(df) = self
            .scan_metrics(tenant_slug, dataset_slug, GAUGE_SUM_TYPES)
            .await?
        else {
            return Ok(Vec::new());
        };
        let df = time_window(df, start, end)?;
        let df =
            common::attrs::expr::select_attr_columns(df, &[LOG_ATTRIBUTES, RESOURCE_ATTRIBUTES])
                .map_err(QuerierError::QueryFailed)?;
        // Arrow's row format cannot sort Map columns; skip the dedup there.
        let attrs_are_map = df.schema().fields().iter().any(|f| {
            matches!(
                f.data_type(),
                datafusion::arrow::datatypes::DataType::Map(_, _)
            )
        });
        let df = if attrs_are_map {
            df
        } else {
            df.distinct().map_err(QuerierError::QueryFailed)?
        };
        let batches = df
            .limit(0, Some(LABEL_SCAN_LIMIT))
            .map_err(QuerierError::QueryFailed)?
            .collect()
            .await
            .map_err(QuerierError::QueryFailed)?;
        for batch in &batches {
            for column in [LOG_ATTRIBUTES, RESOURCE_ATTRIBUTES] {
                collect_attribute_keys(batch, column, &mut labels)?;
            }
        }
        Ok(labels.into_iter().collect())
    }

    /// List the distinct values of one Prometheus label in the window.
    pub async fn get_label_values(
        &self,
        label: &str,
        start: i64,
        end: i64,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<String>, QuerierError> {
        if label.is_empty() {
            return Err(QuerierError::InvalidInput(
                "label name must not be empty".to_string(),
            ));
        }
        let Some(df) = self
            .scan_metrics(tenant_slug, dataset_slug, GAUGE_SUM_TYPES)
            .await?
        else {
            return Ok(Vec::new());
        };
        let df = time_window(df, start, end)?;

        // `__name__` → metric_name; other known labels → their column.
        let column = match label {
            "__name__" => Some("metric_name"),
            _ => column_for_label(label),
        };
        if let Some(column) = column {
            let batches = df
                .select_columns(&[column])
                .map_err(QuerierError::QueryFailed)?
                .distinct()
                .map_err(QuerierError::QueryFailed)?
                .collect()
                .await
                .map_err(QuerierError::QueryFailed)?;
            return distinct_non_empty(&batches, column);
        }

        // Otherwise pull the value out of the attribute documents.
        let df =
            common::attrs::expr::select_attr_columns(df, &[LOG_ATTRIBUTES, RESOURCE_ATTRIBUTES])
                .map_err(QuerierError::QueryFailed)?;
        // Arrow's row format cannot sort Map columns; skip the dedup there.
        let attrs_are_map = df.schema().fields().iter().any(|f| {
            matches!(
                f.data_type(),
                datafusion::arrow::datatypes::DataType::Map(_, _)
            )
        });
        let df = if attrs_are_map {
            df
        } else {
            df.distinct().map_err(QuerierError::QueryFailed)?
        };
        let batches = df
            .limit(0, Some(LABEL_SCAN_LIMIT))
            .map_err(QuerierError::QueryFailed)?
            .collect()
            .await
            .map_err(QuerierError::QueryFailed)?;
        let mut values = BTreeSet::new();
        for batch in &batches {
            for column in [LOG_ATTRIBUTES, RESOURCE_ATTRIBUTES] {
                collect_attribute_values(batch, column, label, &mut values)?;
            }
        }
        Ok(values.into_iter().collect())
    }

    /// List the distinct series (label sets) matching a PromQL selector.
    /// Series identity is `__name__` (metric_name) and `job` (service_name).
    pub async fn get_series(
        &self,
        selector: &str,
        start: i64,
        end: i64,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<BTreeMap<String, String>>, QuerierError> {
        let plan = parse_selector(selector.trim())?;
        let Some(df) = self
            .scan_metrics(tenant_slug, dataset_slug, GAUGE_SUM_TYPES)
            .await?
        else {
            return Ok(Vec::new());
        };
        let df = apply_filters(df, &plan, start, end)?;

        let batches = df
            .select_columns(&["metric_name", "service_name"])
            .map_err(QuerierError::QueryFailed)?
            .distinct()
            .map_err(QuerierError::QueryFailed)?
            .limit(0, Some(LABEL_SCAN_LIMIT))
            .map_err(QuerierError::QueryFailed)?
            .collect()
            .await
            .map_err(QuerierError::QueryFailed)?;

        let mut series = BTreeSet::new();
        for batch in &batches {
            let name = string_column(batch, "metric_name")?;
            let service = string_column(batch, "service_name")?;
            for i in 0..batch.num_rows() {
                let mut labels = BTreeMap::new();
                if !name.is_null(i) && !name.value(i).is_empty() {
                    labels.insert("__name__".to_string(), name.value(i).to_string());
                }
                if !service.is_null(i) && !service.value(i).is_empty() {
                    labels.insert("job".to_string(), service.value(i).to_string());
                }
                if !labels.is_empty() {
                    series.insert(labels);
                }
            }
        }
        Ok(series.into_iter().collect())
    }
}

/// Add every attribute key from an attribute column (JSON-string or
/// map-typed) to `keys`.
fn collect_attribute_keys(
    batch: &RecordBatch,
    column: &str,
    keys: &mut BTreeSet<String>,
) -> Result<(), QuerierError> {
    for doc in super::logs::attr_documents(batch, column)?
        .into_iter()
        .flatten()
    {
        keys.extend(doc.into_keys());
    }
    Ok(())
}

/// Add the value of `label` from each attribute document (JSON-string or
/// map-typed) to `values`.
fn collect_attribute_values(
    batch: &RecordBatch,
    column: &str,
    label: &str,
    values: &mut BTreeSet<String>,
) -> Result<(), QuerierError> {
    for mut doc in super::logs::attr_documents(batch, column)?
        .into_iter()
        .flatten()
    {
        if let Some(value) = doc.remove(label) {
            values.insert(value);
        }
    }
    Ok(())
}

/// Apply the metric-name filter, label matchers, and time window.
fn apply_filters(
    df: DataFrame,
    plan: &Selector,
    start: i64,
    end: i64,
) -> Result<DataFrame, QuerierError> {
    // Materialized `label_<key>` columns present in this metrics table, so
    // label matchers on them are exact instead of JSON substring.
    let attr_ctx = super::logql::AttrContext {
        materialized: super::logs::materialized_columns_of(&df),
        map_attrs: common::attrs::expr::is_typed_layout(df.schema().as_arrow(), LOG_ATTRIBUTES),
        schema: Some(df.schema().inner().clone()),
    };
    let mut predicate = metric_name_expr(plan);
    for m in &plan.matchers {
        predicate = predicate.and(matcher_expr(m, &attr_ctx)?);
    }
    df.filter(
        col("timestamp")
            .gt_eq(lit(ScalarValue::TimestampNanosecond(Some(start), None)))
            .and(col("timestamp").lt_eq(lit(ScalarValue::TimestampNanosecond(Some(end), None))))
            .and(predicate),
    )
    .map_err(QuerierError::QueryFailed)
}

/// Build an exact/negated/regex predicate against a non-nullable column.
/// `anchored` fully anchors the regex pattern (`^(?:pattern)$`), matching
/// Prometheus' `__name__` matcher semantics; other labels keep the
/// pre-existing unanchored (substring) regex behavior.
fn column_op_expr(column: Expr, op: MatchKind, value: &str, anchored: bool) -> Expr {
    let pattern = |p: &str| {
        if anchored {
            anchor_regex(p)
        } else {
            p.to_string()
        }
    };
    match op {
        MatchKind::Eq => column.eq(lit(value.to_string())),
        MatchKind::Neq => column.not_eq(lit(value.to_string())),
        MatchKind::Re => regexp_like(column, lit(pattern(value)), None),
        MatchKind::Nre => not(regexp_like(column, lit(pattern(value)), None)),
    }
}

/// Anchor a PromQL regex pattern to the whole value, matching Prometheus'
/// `^(?:pattern)$` matcher semantics.
fn anchor_regex(pattern: &str) -> String {
    format!("^(?:{pattern})$")
}

/// Build the predicate for the `__name__` selector against the `metric_name`
/// column, anchoring `=~`/`!~` patterns to the whole name per Prometheus
/// semantics (`{__name__=~"wal"}` does not match `"signaldb.wal.count"`).
fn metric_name_expr(plan: &Selector) -> Expr {
    column_op_expr(
        col("metric_name"),
        plan.metric_name_op,
        &plan.metric_name,
        true,
    )
}

/// Lower one label matcher to a filter expression, mapping well-known
/// labels to columns and others to the attribute JSON.
fn matcher_expr(m: &LabelMatch, ctx: &super::logql::AttrContext) -> Result<Expr, QuerierError> {
    let materialized = &ctx.materialized;
    // A well-known or materialized label matches its dedicated column
    // exactly (and supports regex); other labels fall back to the JSON
    // substring match. A materialized column is nullable, so `!=`/`!~` also
    // keeps rows where the label is absent.
    let column_match = |column: String| match m.op {
        MatchKind::Eq => col(&column).eq(lit(m.value.clone())),
        MatchKind::Neq => col(&column)
            .clone()
            .is_null()
            .or(col(&column).not_eq(lit(m.value.clone()))),
        MatchKind::Re => regexp_like(col(&column), lit(m.value.clone()), None),
        MatchKind::Nre => col(&column).clone().is_null().or(not(regexp_like(
            col(&column),
            lit(m.value.clone()),
            None,
        ))),
    };
    match column_for_label(&m.name) {
        Some(column) => Ok(column_op_expr(col(column), m.op, &m.value, false)),
        None if let Some(column) = materialized.column_for(&m.name) => {
            Ok(column_match(column.to_string()))
        }
        // Map-typed attribute tables: per-key extraction, all four
        // operators, on both attribute columns.
        None if ctx.map_attrs => {
            let per = |column: &str| {
                let e =
                    common::attrs::expr::compat_attr_expr(ctx.schema.as_deref(), column, &m.name);
                match m.op {
                    MatchKind::Eq => e.eq(lit(m.value.clone())),
                    MatchKind::Neq => e.clone().is_null().or(e.not_eq(lit(m.value.clone()))),
                    MatchKind::Re => regexp_like(e, lit(m.value.clone()), None),
                    MatchKind::Nre => {
                        e.clone()
                            .is_null()
                            .or(not(regexp_like(e, lit(m.value.clone()), None)))
                    }
                }
            };
            Ok(match m.op {
                MatchKind::Eq | MatchKind::Re => per(LOG_ATTRIBUTES).or(per(RESOURCE_ATTRIBUTES)),
                MatchKind::Neq | MatchKind::Nre => {
                    per(LOG_ATTRIBUTES).and(per(RESOURCE_ATTRIBUTES))
                }
            })
        }
        None => {
            let fragment = attribute_fragment(&m.name, &m.value);
            let present = contains(col(LOG_ATTRIBUTES), lit(fragment.clone()))
                .or(contains(col(RESOURCE_ATTRIBUTES), lit(fragment.clone())));
            match m.op {
                MatchKind::Eq => Ok(present),
                MatchKind::Neq => Ok(not(present)),
                _ => Err(QuerierError::Unsupported(format!(
                    "regex matcher on attribute label '{}'",
                    m.name
                ))),
            }
        }
    }
}

/// Well-known PromQL labels mapped to dedicated columns.
fn column_for_label(label: &str) -> Option<&'static str> {
    match label {
        "job" | "service" | "service_name" => Some("service_name"),
        _ => None,
    }
}

fn attribute_fragment(key: &str, value: &str) -> String {
    let json_key = serde_json::to_string(key).unwrap_or_else(|_| format!("\"{key}\""));
    let json_value = serde_json::to_string(value).unwrap_or_else(|_| format!("\"{value}\""));
    format!("{json_key}:{json_value}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, Float64Array, StringArray, TimestampNanosecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::catalog::memory::MemTable;
    use datafusion::catalog::{
        CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
    };

    fn selector(q: &str) -> Selector {
        parse_selector(q).unwrap()
    }

    #[test]
    fn selector_parses_name_and_label_matchers() {
        let s = selector(r#"reqs{code="200", env!~"dev.*"}"#);
        assert_eq!(
            (s.metric_name.as_str(), s.metric_name_op),
            ("reqs", MatchKind::Eq)
        );
        assert_eq!(s.matchers.len(), 2);
        assert_eq!(s.matchers[1].op, MatchKind::Nre);

        let s = selector(r#"{__name__=~"req.*"}"#);
        assert_eq!(
            (s.metric_name.as_str(), s.metric_name_op),
            ("req.*", MatchKind::Re)
        );
    }

    #[test]
    fn selector_accepts_bare_dotted_names() {
        assert_eq!(
            selector("signaldb.wal.count"),
            selector(r#"{"signaldb.wal.count"}"#)
        );
    }

    #[test]
    fn selector_rejects_non_selectors() {
        for q in [
            "this is ((not promql",
            "sum(reqs)",
            "reqs[5m]",
            r#"{job="x"}"#,
        ] {
            assert!(
                matches!(parse_selector(q), Err(QuerierError::InvalidInput(_))),
                "{q}"
            );
        }
    }

    #[test]
    fn matcher_routes_materialized_label_to_column() {
        let m = LabelMatch {
            name: "namespace".to_string(),
            op: MatchKind::Eq,
            value: "prod".to_string(),
        };
        // No materialized column → attribute-JSON substring match.
        let ctx = super::super::logql::AttrContext::default();
        let json = format!("{:?}", matcher_expr(&m, &ctx).unwrap());
        assert!(json.contains(r#""namespace":"prod""#), "{json}");
        // With the column → exact equality on `label_namespace`.
        let ctx = super::super::logql::AttrContext {
            materialized: common::schema::MaterializedLabels::from_names(["label_namespace"]),
            ..Default::default()
        };
        let rendered = format!("{:?}", matcher_expr(&m, &ctx).unwrap());
        assert!(rendered.contains("label_namespace"), "{rendered}");
        assert!(!rendered.contains("attributes"), "{rendered}");
    }

    fn register(ctx: &SessionContext, table: Option<Arc<MemTable>>) {
        let schema_provider = Arc::new(MemorySchemaProvider::new());
        if let Some(table) = table {
            schema_provider
                .register_table("metrics".to_string(), table)
                .unwrap();
        }
        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog.register_schema("d", schema_provider).unwrap();
        ctx.register_catalog("t", catalog);
    }

    /// Three samples over `reqs`: two `api`, one `web`, with a `code`
    /// attribute and an empty `resource_attributes` container each.
    fn service_with_data() -> MetricMetadataService {
        let mut fields: Vec<Field> = vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampNanosecondArray::from(vec![100, 200, 300])),
            Arc::new(StringArray::from(vec!["api", "api", "web"])),
            Arc::new(StringArray::from(vec!["reqs", "reqs", "reqs"])),
            Arc::new(Float64Array::from(vec![1.0, 3.0, 5.0])),
        ];
        let code = |v: &str| Some(serde_json::Map::from_iter([("code".into(), v.into())]));
        let attr_rows = [code("200"), code("500"), code("200")];
        let resource_rows = [
            Some(serde_json::Map::new()),
            Some(serde_json::Map::new()),
            Some(serde_json::Map::new()),
        ];
        for (name, rows) in [
            (LOG_ATTRIBUTES, &attr_rows),
            (RESOURCE_ATTRIBUTES, &resource_rows),
        ] {
            let (typed_fields, typed_arrays) =
                common::testing::typed_attribute_columns_from("metrics", "physical-v4", name, rows);
            fields.extend(typed_fields);
            columns.extend(typed_arrays);
        }
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        let batch = common::testing::to_wide(&batch, "gauge");
        let ctx = SessionContext::new();
        register(
            &ctx,
            Some(Arc::new(
                MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap(),
            )),
        );
        MetricMetadataService::new(ctx)
    }

    /// A `t.d` dataset registered in the catalog but holding no tables.
    fn service_without_metrics_tables() -> MetricMetadataService {
        let ctx = SessionContext::new();
        register(&ctx, None);
        MetricMetadataService::new(ctx)
    }

    #[tokio::test]
    async fn label_names_include_known_and_attribute_keys() {
        let labels = service_with_data()
            .get_labels(0, 1000, "t", "d")
            .await
            .unwrap();
        assert!(labels.contains(&"__name__".to_string()));
        assert!(labels.contains(&"job".to_string()));
        assert!(labels.contains(&"code".to_string()));
    }

    #[tokio::test]
    async fn label_values_for_name_job_and_attribute() {
        let service = service_with_data();
        for (label, want) in [
            ("__name__", vec!["reqs"]),
            ("job", vec!["api", "web"]),
            ("code", vec!["200", "500"]),
        ] {
            assert_eq!(
                service
                    .get_label_values(label, 0, 1000, "t", "d")
                    .await
                    .unwrap(),
                want,
                "{label}"
            );
        }
    }

    #[tokio::test]
    async fn series_returns_name_and_job_sets() {
        let series = service_with_data()
            .get_series("reqs", 0, 1000, "t", "d")
            .await
            .unwrap();
        assert_eq!(series.len(), 2);
        assert!(
            series
                .iter()
                .all(|s| s.get("__name__") == Some(&"reqs".to_string()))
        );
        let jobs: BTreeSet<_> = series
            .iter()
            .filter_map(|s| s.get("job").cloned())
            .collect();
        assert_eq!(jobs, BTreeSet::from(["api".to_string(), "web".to_string()]));
    }

    #[tokio::test]
    async fn series_honours_label_matchers() {
        let series = service_with_data()
            .get_series(r#"reqs{job="web"}"#, 0, 1000, "t", "d")
            .await
            .unwrap();
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].get("job"), Some(&"web".to_string()));
    }

    #[tokio::test]
    async fn series_matchers_filter_as_prometheus_does() {
        let service = service_with_data();
        for (query, want) in [
            (r#"{__name__=~"re"}"#, 0),
            (r#"{__name__=~"req.*"}"#, 2),
            (r#"{__name__!~"reqs", job="web"}"#, 0),
            (r#"{__name__!~"re", job="web"}"#, 1),
            (r#"reqs{job!="web"}"#, 1),
        ] {
            let series = service.get_series(query, 0, 1000, "t", "d").await.unwrap();
            assert_eq!(series.len(), want, "{query}");
        }
    }

    #[tokio::test]
    async fn reads_on_absent_tables_are_empty() {
        let service = service_without_metrics_tables();
        assert!(
            service
                .get_labels(0, i64::MAX, "t", "d")
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            service
                .get_label_values("__name__", 0, i64::MAX, "t", "d")
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            service
                .get_series("up", 0, i64::MAX, "t", "d")
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn absence_does_not_swallow_real_errors() {
        let service = service_without_metrics_tables();
        assert!(
            service
                .get_labels(0, i64::MAX, "nosuchtenant", "d")
                .await
                .is_err(),
            "unknown tenant must not read as empty"
        );
        assert!(matches!(
            service
                .get_series("this is ((not promql", 0, 10, "t", "d")
                .await,
            Err(QuerierError::InvalidInput(_))
        ));
        assert!(matches!(
            service.get_label_values("", 0, 10, "t", "d").await,
            Err(QuerierError::InvalidInput(_))
        ));
    }
}
