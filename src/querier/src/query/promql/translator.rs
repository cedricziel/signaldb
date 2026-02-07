//! PromQL AST to DataFusion query translation
//!
//! Translates parsed PromQL expressions into DataFusion DataFrame operations
//! that can be executed against Iceberg metrics tables.
//!
//! This module is designed to be used by the metrics query service (Issue #335)
//! and the Prometheus HTTP API endpoints (Issues #332-#334).

// Allow dead code for now - this module is designed for future issues
#![allow(dead_code)]
//!
//! # Architecture
//!
//! The translator converts PromQL AST nodes (from promql-parser) into
//! DataFusion queries following the SignalDB multi-tenant model:
//!
//! - **Catalog**: `{tenant_slug}` (tenant isolation)
//! - **Schema**: `{dataset_slug}` (dataset isolation)
//! - **Table**: `metrics_gauge`, `metrics_sum`, `metrics_histogram`, etc.
//!
//! # Label Mapping
//!
//! PromQL labels are mapped to OTEL/SignalDB columns:
//! - `__name__` → `metric_name` column
//! - `job` → `resource_attributes.service.name`
//! - `instance` → `resource_attributes.service.instance.id`
//! - Other labels → `attributes.{label_name}`

use std::sync::Arc;

use datafusion::logical_expr::{col, lit};
use datafusion::prelude::{DataFrame, SessionContext};
use promql_parser::label::{MatchOp, Matchers};
use promql_parser::parser::{Expr as PromExpr, VectorSelector};

use super::error::PromQLError;
use super::types::{InstantQueryParams, RangeQueryParams};
use crate::query::table_ref::build_table_reference;

/// Default lookback delta for instant queries (5 minutes in milliseconds)
const DEFAULT_LOOKBACK_DELTA_MS: i64 = 5 * 60 * 1000;

/// Metrics table types in SignalDB
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsTableType {
    /// Gauge metrics (current value at a point in time)
    Gauge,
    /// Sum/counter metrics (monotonically increasing values)
    Sum,
    /// Histogram metrics (distribution of values)
    Histogram,
    /// Summary metrics (pre-computed quantiles)
    Summary,
    /// Exponential histogram metrics
    ExponentialHistogram,
}

impl MetricsTableType {
    /// Get the table name for this metrics type
    pub fn table_name(&self) -> &'static str {
        match self {
            Self::Gauge => "metrics_gauge",
            Self::Sum => "metrics_sum",
            Self::Histogram => "metrics_histogram",
            Self::Summary => "metrics_summary",
            Self::ExponentialHistogram => "metrics_exponential_histogram",
        }
    }
}

/// Context for PromQL to DataFusion translation
#[derive(Debug, Clone)]
pub struct TranslationContext {
    /// Tenant slug for multi-tenant isolation
    pub tenant_slug: String,
    /// Dataset slug within the tenant
    pub dataset_slug: String,
    /// Evaluation timestamp (milliseconds since epoch)
    pub eval_time: i64,
    /// Start time for range queries (milliseconds)
    pub start_time: Option<i64>,
    /// End time for range queries (milliseconds)
    pub end_time: Option<i64>,
    /// Step for range queries (milliseconds)
    pub step: Option<i64>,
    /// Lookback delta for instant queries (milliseconds)
    pub lookback_delta: i64,
}

impl TranslationContext {
    /// Create a context for an instant query
    pub fn for_instant_query(params: &InstantQueryParams, eval_time: i64) -> Self {
        Self {
            tenant_slug: params.tenant_slug.clone(),
            dataset_slug: params.dataset_slug.clone(),
            eval_time,
            start_time: None,
            end_time: None,
            step: None,
            lookback_delta: DEFAULT_LOOKBACK_DELTA_MS,
        }
    }

    /// Create a context for a range query
    pub fn for_range_query(params: &RangeQueryParams) -> Self {
        Self {
            tenant_slug: params.tenant_slug.clone(),
            dataset_slug: params.dataset_slug.clone(),
            eval_time: params.end, // Use end time as eval time
            start_time: Some(params.start),
            end_time: Some(params.end),
            step: Some(params.step),
            lookback_delta: DEFAULT_LOOKBACK_DELTA_MS,
        }
    }
}

/// Result of translating a PromQL expression
#[derive(Debug)]
pub struct TranslatedQuery {
    /// The metrics table type being queried
    pub table_type: MetricsTableType,
    /// The DataFusion DataFrame with all filters applied
    pub dataframe: DataFrame,
}

/// PromQL to DataFusion translator
pub struct PromQLTranslator {
    session_context: Arc<SessionContext>,
}

impl PromQLTranslator {
    /// Create a new translator with the given session context
    pub fn new(session_context: Arc<SessionContext>) -> Self {
        Self { session_context }
    }

    /// Translate a PromQL instant query to a DataFusion query
    ///
    /// # Arguments
    /// * `expr` - The parsed PromQL expression
    /// * `ctx` - Translation context with tenant/time information
    ///
    /// # Returns
    /// A translated query ready for execution, or an error
    pub async fn translate_instant_query(
        &self,
        expr: &PromExpr,
        ctx: &TranslationContext,
    ) -> Result<TranslatedQuery, PromQLError> {
        self.translate_expr(expr, ctx, false).await
    }

    /// Translate a PromQL range query to a DataFusion query
    ///
    /// # Arguments
    /// * `expr` - The parsed PromQL expression
    /// * `ctx` - Translation context with tenant/time information
    ///
    /// # Returns
    /// A translated query ready for execution, or an error
    pub async fn translate_range_query(
        &self,
        expr: &PromExpr,
        ctx: &TranslationContext,
    ) -> Result<TranslatedQuery, PromQLError> {
        self.translate_expr(expr, ctx, true).await
    }

    /// Translate a PromQL expression to a DataFusion query
    ///
    /// Uses `Box::pin` internally to handle recursive Paren expressions.
    fn translate_expr<'a>(
        &'a self,
        expr: &'a PromExpr,
        ctx: &'a TranslationContext,
        is_range_query: bool,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<TranslatedQuery, PromQLError>> + Send + 'a>,
    > {
        Box::pin(async move {
            match expr {
                PromExpr::VectorSelector(vs) => {
                    self.translate_vector_selector(vs, ctx, is_range_query)
                        .await
                }
                PromExpr::MatrixSelector(ms) => {
                    // Matrix selectors are used with range functions (rate, etc.)
                    // For now, translate the inner vector selector with the range applied
                    self.translate_vector_selector(&ms.vs, ctx, true).await
                }
                PromExpr::Paren(paren) => {
                    self.translate_expr(&paren.expr, ctx, is_range_query).await
                }
                PromExpr::NumberLiteral(_) => Err(PromQLError::UnsupportedFeature(
                    "Standalone number literals are not yet supported".to_string(),
                )),
                PromExpr::StringLiteral(_) => Err(PromQLError::UnsupportedFeature(
                    "Standalone string literals are not yet supported".to_string(),
                )),
                PromExpr::Call(_) => Err(PromQLError::UnsupportedFeature(
                    "Function calls will be implemented in Issue #334".to_string(),
                )),
                PromExpr::Aggregate(_) => Err(PromQLError::UnsupportedFeature(
                    "Aggregations will be implemented in Issue #333".to_string(),
                )),
                PromExpr::Binary(_) => Err(PromQLError::UnsupportedFeature(
                    "Binary operations are not yet supported".to_string(),
                )),
                PromExpr::Unary(_) => Err(PromQLError::UnsupportedFeature(
                    "Unary operations are not yet supported".to_string(),
                )),
                PromExpr::Subquery(_) => Err(PromQLError::UnsupportedFeature(
                    "Subqueries are not yet supported".to_string(),
                )),
                PromExpr::Extension(_) => Err(PromQLError::UnsupportedFeature(
                    "Extension expressions are not supported".to_string(),
                )),
            }
        })
    }

    /// Translate a vector selector to a DataFusion query
    ///
    /// A vector selector like `http_requests_total{job="api"}` becomes:
    /// ```sql
    /// SELECT * FROM tenant.dataset.metrics_sum
    /// WHERE metric_name = 'http_requests_total'
    ///   AND json_extract(attributes, '$.job') = 'api'
    ///   AND timestamp BETWEEN (eval_time - lookback) AND eval_time
    /// ```
    async fn translate_vector_selector(
        &self,
        vs: &VectorSelector,
        ctx: &TranslationContext,
        is_range_query: bool,
    ) -> Result<TranslatedQuery, PromQLError> {
        // 1. Determine table type from metric name
        let metric_name = vs.name.as_deref();
        let table_type = infer_table_type(metric_name);

        // 2. Build table reference
        let table_ref =
            build_table_reference(&ctx.tenant_slug, &ctx.dataset_slug, table_type.table_name())
                .map_err(|e| PromQLError::InvalidMatcher(e.to_string()))?;

        // 3. Get the DataFrame for the table
        let df =
            self.session_context.table(table_ref).await.map_err(|e| {
                PromQLError::EvaluationError(format!("Failed to access table: {e}"))
            })?;

        // 4. Apply metric name filter if specified
        let df = if let Some(name) = metric_name {
            df.filter(col("metric_name").eq(lit(name))).map_err(|e| {
                PromQLError::EvaluationError(format!("Failed to filter by name: {e}"))
            })?
        } else {
            df
        };

        // 5. Apply label matchers
        let df = apply_label_matchers(df, &vs.matchers)?;

        // 6. Apply time filter
        let df = apply_time_filter(df, ctx, is_range_query)?;

        Ok(TranslatedQuery {
            table_type,
            dataframe: df,
        })
    }
}

/// Infer the metrics table type from the metric name
///
/// Uses Prometheus naming conventions:
/// - `*_total` → Sum (counter)
/// - `*_bucket`, `*_sum`, `*_count` → Histogram
/// - Default → Gauge
pub fn infer_table_type(metric_name: Option<&str>) -> MetricsTableType {
    match metric_name {
        Some(name) if name.ends_with("_total") => MetricsTableType::Sum,
        Some(name) if name.ends_with("_bucket") => MetricsTableType::Histogram,
        Some(name) if name.ends_with("_sum") && !name.ends_with("_histogram_sum") => {
            // Could be histogram or sum - histogram takes precedence in context
            MetricsTableType::Histogram
        }
        Some(name) if name.ends_with("_count") => MetricsTableType::Histogram,
        _ => MetricsTableType::Gauge, // Default to gauge
    }
}

/// Determine which column to query for a given PromQL label name
///
/// Maps PromQL label conventions to OTEL/SignalDB schema:
/// - `__name__` → metric_name (direct column)
/// - `job` → resource_attributes JSON with key "service.name"
/// - `instance` → resource_attributes JSON with key "service.instance.id"
/// - Other labels → attributes JSON with the label name as key
///
/// Returns (column_name, json_key) where json_key is empty for direct columns
fn label_to_column(label_name: &str) -> (&'static str, &str) {
    match label_name {
        // Special Prometheus labels that map to resource attributes
        "job" => ("resource_attributes", "service.name"),
        "instance" => ("resource_attributes", "service.instance.id"),
        // Metric name is a direct column
        "__name__" => ("metric_name", ""),
        // All other labels are in the attributes JSON
        _ => ("attributes", label_name),
    }
}

/// Apply label matchers to a DataFrame
///
/// Translates PromQL label matchers to DataFusion filter expressions.
/// For JSON columns (attributes, resource_attributes), uses SQL JSON extraction.
fn apply_label_matchers(df: DataFrame, matchers: &Matchers) -> Result<DataFrame, PromQLError> {
    let mut result = df;

    for matcher in matchers.matchers.iter() {
        let (column_name, json_key) = label_to_column(&matcher.name);

        // Build the expression to get the label value
        let label_value_expr = if column_name == "metric_name" {
            // metric_name is a direct column, not JSON
            col("metric_name")
        } else {
            // For JSON columns, we need to extract the value
            // Using SQL JSON path syntax: json_extract_scalar(column, '$.key')
            // DataFusion 51 supports get_field for struct types, but for JSON strings
            // we need to use a different approach.
            //
            // For now, use a LIKE-based approach for simple equality matching
            // which works on JSON string columns. Full JSON support will be added
            // when we integrate datafusion-functions-json.
            col(column_name)
        };

        let filter = match &matcher.op {
            MatchOp::Equal => {
                if column_name == "metric_name" {
                    label_value_expr.eq(lit(&matcher.value))
                } else {
                    // For JSON columns, use LIKE pattern for simple matching
                    // Pattern: *"key":"value"* or *"key": "value"*
                    let pattern = format!("%\"{json_key}\":\"{}\"%", matcher.value);
                    let pattern_with_space = format!("%\"{json_key}\": \"{}\"%", matcher.value);
                    label_value_expr
                        .clone()
                        .like(lit(&pattern))
                        .or(label_value_expr.like(lit(&pattern_with_space)))
                }
            }
            MatchOp::NotEqual => {
                if column_name == "metric_name" {
                    label_value_expr.not_eq(lit(&matcher.value))
                } else {
                    // For JSON columns, NOT LIKE pattern
                    let pattern = format!("%\"{json_key}\":\"{}\"%", matcher.value);
                    let pattern_with_space = format!("%\"{json_key}\": \"{}\"%", matcher.value);
                    label_value_expr
                        .clone()
                        .not_like(lit(&pattern))
                        .and(label_value_expr.not_like(lit(&pattern_with_space)))
                }
            }
            MatchOp::Re(regex) => {
                if column_name == "metric_name" {
                    // Use regexp_match for direct columns (3rd arg is flags, None = default)
                    datafusion::functions::regex::expr_fn::regexp_match(
                        label_value_expr,
                        lit(regex.as_str()),
                        None,
                    )
                    .is_not_null()
                } else {
                    // For JSON columns, regex matching is complex
                    // For now, fall back to checking if the key exists with a pattern
                    let pattern = format!("%\"{json_key}\":%");
                    label_value_expr.like(lit(&pattern))
                }
            }
            MatchOp::NotRe(regex) => {
                if column_name == "metric_name" {
                    // Use regexp_match for direct columns (3rd arg is flags, None = default)
                    datafusion::functions::regex::expr_fn::regexp_match(
                        label_value_expr,
                        lit(regex.as_str()),
                        None,
                    )
                    .is_null()
                } else {
                    // For JSON columns, negative regex is complex
                    let pattern = format!("%\"{json_key}\":%");
                    label_value_expr.not_like(lit(&pattern))
                }
            }
        };

        result = result
            .filter(filter)
            .map_err(|e| PromQLError::EvaluationError(format!("Failed to apply filter: {e}")))?;
    }

    Ok(result)
}

/// Apply time range filter based on query type
///
/// For instant queries: timestamp BETWEEN (eval_time - lookback) AND eval_time
/// For range queries: timestamp BETWEEN start_time AND end_time
fn apply_time_filter(
    df: DataFrame,
    ctx: &TranslationContext,
    is_range_query: bool,
) -> Result<DataFrame, PromQLError> {
    let (start_ts, end_ts) = if is_range_query {
        // Range query: use explicit start/end times
        let start = ctx
            .start_time
            .ok_or_else(|| PromQLError::InvalidTimeRange {
                start: 0,
                end: ctx.eval_time,
                reason: "Range query requires start_time".to_string(),
            })?;
        let end = ctx.end_time.ok_or_else(|| PromQLError::InvalidTimeRange {
            start: ctx.eval_time,
            end: 0,
            reason: "Range query requires end_time".to_string(),
        })?;
        (start, end)
    } else {
        // Instant query: lookback window from eval_time
        let start = ctx.eval_time - ctx.lookback_delta;
        (start, ctx.eval_time)
    };

    // Convert milliseconds to nanoseconds for timestamp comparison
    // SignalDB stores timestamps in nanoseconds (TimestampNanosecond)
    let start_ns = start_ts * 1_000_000;
    let end_ns = end_ts * 1_000_000;

    df.filter(
        col("timestamp")
            .gt_eq(lit(start_ns))
            .and(col("timestamp").lt_eq(lit(end_ns))),
    )
    .map_err(|e| PromQLError::EvaluationError(format!("Failed to apply time filter: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_table_type_counter() {
        assert_eq!(
            infer_table_type(Some("http_requests_total")),
            MetricsTableType::Sum
        );
        assert_eq!(
            infer_table_type(Some("process_cpu_seconds_total")),
            MetricsTableType::Sum
        );
    }

    #[test]
    fn test_infer_table_type_histogram() {
        assert_eq!(
            infer_table_type(Some("http_request_duration_bucket")),
            MetricsTableType::Histogram
        );
        assert_eq!(
            infer_table_type(Some("http_request_duration_sum")),
            MetricsTableType::Histogram
        );
        assert_eq!(
            infer_table_type(Some("http_request_duration_count")),
            MetricsTableType::Histogram
        );
    }

    #[test]
    fn test_infer_table_type_gauge() {
        assert_eq!(
            infer_table_type(Some("process_resident_memory_bytes")),
            MetricsTableType::Gauge
        );
        assert_eq!(infer_table_type(Some("up")), MetricsTableType::Gauge);
        assert_eq!(infer_table_type(None), MetricsTableType::Gauge);
    }

    #[test]
    fn test_label_to_column_special_labels() {
        assert_eq!(
            label_to_column("job"),
            ("resource_attributes", "service.name")
        );
        assert_eq!(
            label_to_column("instance"),
            ("resource_attributes", "service.instance.id")
        );
        assert_eq!(label_to_column("__name__"), ("metric_name", ""));
    }

    #[test]
    fn test_label_to_column_regular_labels() {
        assert_eq!(label_to_column("method"), ("attributes", "method"));
        assert_eq!(
            label_to_column("status_code"),
            ("attributes", "status_code")
        );
        assert_eq!(label_to_column("handler"), ("attributes", "handler"));
    }

    // Note: Table reference validation tests are in crate::query::table_ref::tests

    #[test]
    fn test_metrics_table_type_names() {
        assert_eq!(MetricsTableType::Gauge.table_name(), "metrics_gauge");
        assert_eq!(MetricsTableType::Sum.table_name(), "metrics_sum");
        assert_eq!(
            MetricsTableType::Histogram.table_name(),
            "metrics_histogram"
        );
        assert_eq!(MetricsTableType::Summary.table_name(), "metrics_summary");
        assert_eq!(
            MetricsTableType::ExponentialHistogram.table_name(),
            "metrics_exponential_histogram"
        );
    }

    #[test]
    fn test_translation_context_instant() {
        let params = InstantQueryParams {
            query: "up".to_string(),
            time: None,
            timeout: None,
            tenant_slug: "test-tenant".to_string(),
            dataset_slug: "production".to_string(),
        };
        let ctx = TranslationContext::for_instant_query(&params, 1000000);

        assert_eq!(ctx.tenant_slug, "test-tenant");
        assert_eq!(ctx.dataset_slug, "production");
        assert_eq!(ctx.eval_time, 1000000);
        assert!(ctx.start_time.is_none());
        assert!(ctx.end_time.is_none());
        assert_eq!(ctx.lookback_delta, DEFAULT_LOOKBACK_DELTA_MS);
    }

    #[test]
    fn test_translation_context_range() {
        let params = RangeQueryParams {
            query: "up".to_string(),
            start: 1000000,
            end: 2000000,
            step: 15000,
            timeout: None,
            tenant_slug: "test-tenant".to_string(),
            dataset_slug: "production".to_string(),
        };
        let ctx = TranslationContext::for_range_query(&params);

        assert_eq!(ctx.tenant_slug, "test-tenant");
        assert_eq!(ctx.dataset_slug, "production");
        assert_eq!(ctx.eval_time, 2000000);
        assert_eq!(ctx.start_time, Some(1000000));
        assert_eq!(ctx.end_time, Some(2000000));
        assert_eq!(ctx.step, Some(15000));
    }
}
