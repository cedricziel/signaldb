use std::{collections::HashMap, fmt::Debug, str::FromStr, sync::Arc};

use async_trait::async_trait;

use common::model::{
    self,
    span::{Span, SpanKind, SpanStatus},
};
use datafusion::{
    arrow::array::{Array, BooleanArray, Int64Array, StringArray},
    logical_expr::{col, lit},
    prelude::SessionContext,
};

use super::{
    FindTraceByIdParams, SearchQueryParams, TraceQuerier, error::QuerierError, search_filter,
    table_ref::build_table_reference,
};

#[allow(dead_code)]
const SHALLOW_TRACE_BY_ID_QUERY: &str = "SELECT * FROM traces WHERE trace_id = '{trace_id}';";

#[allow(dead_code)]
const TRACE_BY_ID_QUERY: &str = "WITH RECURSIVE trace_hierarchy AS (
    SELECT *, ARRAY[span_id] AS path FROM traces WHERE trace_id = '{trace_id}'
    UNION ALL
    SELECT t.*, th.path || t.span_id
    FROM traces t
    INNER JOIN trace_hierarchy th ON t.parent_span_id = th.span_id OR th.parent_span_id = t.span_id
    WHERE NOT t.span_id = ANY(th.path)  -- Prevent revisiting nodes
)
SELECT DISTINCT * FROM trace_hierarchy;";

#[allow(dead_code)]
const TRACES_BY_QUERY: &str = "WITH RECURSIVE trace_hierarchy AS (
    SELECT t.* FROM traces t
    INNER JOIN trace_hierarchy th ON t.parent_span_id = th.span_id

    UNION ALL

    SELECT t.* FROM traces t
    INNER JOIN trace_hierarchy th ON th.parent_span_id = t.span_id
)
SELECT * FROM trace_hierarchy;";

#[allow(dead_code)]
pub struct TraceService {
    // skip debug on session_context
    session_context: Arc<SessionContext>,
    traces_path: String,
    /// Upper bound for the client-supplied `limit` (trace count) on search.
    max_search_limit: usize,
}

impl Debug for TraceService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraceService")
            .field("session_context", &"set")
            .field("traces_path", &self.traces_path)
            .field("max_search_limit", &self.max_search_limit)
            .finish()
    }
}

impl Clone for TraceService {
    fn clone(&self) -> Self {
        Self {
            session_context: Arc::clone(&self.session_context),
            traces_path: self.traces_path.clone(),
            max_search_limit: self.max_search_limit,
        }
    }
}

impl TraceService {
    #[allow(dead_code)]
    pub fn new(session_context: SessionContext, traces_path: String) -> Self {
        Self {
            session_context: Arc::new(session_context),
            traces_path,
            max_search_limit: common::config::QuerierConfig::default().max_search_limit,
        }
    }

    /// Override the clamp applied to client-supplied search limits.
    pub fn with_max_search_limit(mut self, max_search_limit: usize) -> Self {
        self.max_search_limit = max_search_limit;
        self
    }

    /// Find a trace by ID with tenant isolation
    pub async fn find_by_id_with_tenant(
        &self,
        params: FindTraceByIdParams,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Option<model::trace::Trace>, QuerierError> {
        log::info!(
            "Querying for trace_id={} in tenant_slug={}, dataset_slug={}",
            params.trace_id,
            tenant_slug,
            dataset_slug
        );

        // Build safe table reference with tenant and dataset isolation
        let table_ref = build_table_reference(tenant_slug, dataset_slug, "traces")
            .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;

        // Use DataFrame API with parameterized filter (prevents SQL injection)
        let df = self
            .session_context
            .table(table_ref)
            .await
            .map_err(|e| {
                log::error!(
                    "Failed to access table for tenant_slug={}, dataset_slug={}: {}",
                    tenant_slug,
                    dataset_slug,
                    e
                );
                QuerierError::QueryFailed(e)
            })?
            .filter(col("trace_id").eq(lit(&params.trace_id)))
            .map_err(|e| {
                log::error!(
                    "Failed to apply filter for trace_id={}: {}",
                    params.trace_id,
                    e
                );
                QuerierError::QueryFailed(e)
            })?;

        let results = df.collect().await.map_err(|e| {
            log::error!(
                "Failed to collect query results for trace_id={}, tenant_slug={}, dataset_slug={}: {}",
                params.trace_id,
                tenant_slug,
                dataset_slug,
                e
            );
            QuerierError::QueryFailed(e)
        })?;

        log::info!(
            "Query returned {} rows for trace_id={}, tenant_slug={}, dataset_slug={}",
            results.len(),
            params.trace_id,
            tenant_slug,
            dataset_slug
        );

        // bail if no results were found
        if results.is_empty() {
            return Ok(None);
        }

        // Create a map to store all spans by their span_id for easy lookup
        let mut span_map: HashMap<String, Span> = HashMap::new();
        let mut trace_id = String::new();

        for batch in results {
            for row_index in 0..batch.num_rows() {
                // Use named column access instead of positions
                let current_trace_id = batch
                    .column_by_name("trace_id")
                    .ok_or_else(|| {
                        QuerierError::InvalidInput("Missing required column 'trace_id'".to_string())
                    })?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        QuerierError::InvalidInput("Column 'trace_id' has wrong type".to_string())
                    })?
                    .value(row_index)
                    .to_string();

                if trace_id.is_empty() {
                    trace_id = current_trace_id.clone();
                }

                let span_id = batch
                    .column_by_name("span_id")
                    .ok_or_else(|| {
                        QuerierError::InvalidInput("Missing required column 'span_id'".to_string())
                    })?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        QuerierError::InvalidInput("Column 'span_id' has wrong type".to_string())
                    })?
                    .value(row_index)
                    .to_string();

                let parent_span_id = batch
                    .column_by_name("parent_span_id")
                    .ok_or_else(|| {
                        QuerierError::InvalidInput(
                            "Missing required column 'parent_span_id'".to_string(),
                        )
                    })?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        QuerierError::InvalidInput(
                            "Column 'parent_span_id' has wrong type".to_string(),
                        )
                    })?
                    .value(row_index)
                    .to_string();

                let attributes = batch
                    .column_by_name("span_attributes")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                    .and_then(|arr| {
                        if arr.is_null(row_index) {
                            None
                        } else {
                            serde_json::from_str(arr.value(row_index)).ok()
                        }
                    })
                    .unwrap_or_default();

                let resource = batch
                    .column_by_name("resource_attributes")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                    .and_then(|arr| {
                        if arr.is_null(row_index) {
                            None
                        } else {
                            serde_json::from_str(arr.value(row_index)).ok()
                        }
                    })
                    .unwrap_or_default();

                let span = Span {
                    span_id: span_id.clone(),
                    parent_span_id: parent_span_id.clone(),
                    children: Vec::new(),
                    trace_id: trace_id.clone(),
                    status: SpanStatus::from_str(
                        batch
                            .column_by_name("status_code")
                            .ok_or_else(|| {
                                QuerierError::InvalidInput(
                                    "Missing required column 'status_code'".to_string(),
                                )
                            })?
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| {
                                QuerierError::InvalidInput(
                                    "Column 'status_code' has wrong type".to_string(),
                                )
                            })?
                            .value(row_index),
                    )
                    .unwrap_or(SpanStatus::Unspecified),
                    is_root: batch
                        .column_by_name("is_root")
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Missing required column 'is_root'".to_string(),
                            )
                        })?
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Column 'is_root' has wrong type".to_string(),
                            )
                        })?
                        .value(row_index),
                    name: batch
                        .column_by_name("span_name")
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Missing required column 'span_name'".to_string(),
                            )
                        })?
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Column 'span_name' has wrong type".to_string(),
                            )
                        })?
                        .value(row_index)
                        .to_string(),
                    service_name: batch
                        .column_by_name("service_name")
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Missing required column 'service_name'".to_string(),
                            )
                        })?
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Column 'service_name' has wrong type".to_string(),
                            )
                        })?
                        .value(row_index)
                        .to_string(),
                    span_kind: SpanKind::from_str(
                        batch
                            .column_by_name("span_kind")
                            .ok_or_else(|| {
                                QuerierError::InvalidInput(
                                    "Missing required column 'span_kind'".to_string(),
                                )
                            })?
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| {
                                QuerierError::InvalidInput(
                                    "Column 'span_kind' has wrong type".to_string(),
                                )
                            })?
                            .value(row_index),
                    )
                    .unwrap_or(SpanKind::Internal),
                    start_time_unix_nano: batch
                        .column_by_name("start_time_unix_nano")
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Missing required column 'start_time_unix_nano'".to_string(),
                            )
                        })?
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Column 'start_time_unix_nano' has wrong type".to_string(),
                            )
                        })?
                        .value(row_index) as u64,
                    duration_nano: batch
                        .column_by_name("duration_nanos")
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Missing required column 'duration_nanos'".to_string(),
                            )
                        })?
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Column 'duration_nanos' has wrong type".to_string(),
                            )
                        })?
                        .value(row_index) as u64,
                    attributes,
                    resource,
                };

                span_map.insert(span_id, span);
            }
        }

        let root_spans = model::span::build_span_hierarchy(span_map);

        Ok(Some(model::trace::Trace {
            trace_id,
            spans: root_spans,
        }))
    }

    /// Find traces with tenant isolation
    pub async fn find_traces_with_tenant(
        &self,
        query: SearchQueryParams,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<model::trace::Trace>, QuerierError> {
        log::info!(
            "Searching traces in tenant_slug={}, dataset_slug={}",
            tenant_slug,
            dataset_slug
        );

        // Build safe table reference with tenant and dataset isolation
        let table_ref = build_table_reference(tenant_slug, dataset_slug, "traces")
            .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;

        // Use DataFrame API (prevents SQL injection)
        let mut df = self.session_context.table(table_ref).await.map_err(|e| {
            log::error!(
                "Failed to access table for tenant_slug={}, dataset_slug={}: {}",
                tenant_slug,
                dataset_slug,
                e
            );
            QuerierError::QueryFailed(e)
        })?;

        // Apply time range filters if provided
        if let Some(start) = query.start {
            let start_nanos = (start as i64) * 1_000_000_000;
            df = df
                .filter(col("start_time_unix_nano").gt_eq(lit(start_nanos)))
                .map_err(|e| {
                    log::error!("Failed to apply start time filter: {e}");
                    QuerierError::QueryFailed(e)
                })?;
        }
        if let Some(end) = query.end {
            let end_nanos = (end as i64) * 1_000_000_000;
            df = df
                .filter(col("start_time_unix_nano").lt_eq(lit(end_nanos)))
                .map_err(|e| {
                    log::error!("Failed to apply end time filter: {e}");
                    QuerierError::QueryFailed(e)
                })?;
        }

        // Apply duration filters
        if let Some(min_dur) = query.min_duration {
            df = df
                .filter(col("duration_nanos").gt_eq(lit(min_dur as i64)))
                .map_err(|e| {
                    log::error!("Failed to apply min duration filter: {e}");
                    QuerierError::QueryFailed(e)
                })?;
        }
        if let Some(max_dur) = query.max_duration {
            df = df
                .filter(col("duration_nanos").lt_eq(lit(max_dur as i64)))
                .map_err(|e| {
                    log::error!("Failed to apply max duration filter: {e}");
                    QuerierError::QueryFailed(e)
                })?;
        }

        // Apply the `q` (TraceQL subset) and `tags` (logfmt) selectors.
        // Unsupported constructs error out instead of silently returning
        // unfiltered results (issue #551).
        let mut conditions = Vec::new();
        if let Some(q) = query.q.as_deref().filter(|s| !s.trim().is_empty()) {
            conditions.extend(search_filter::parse_traceql(q)?);
        }
        if let Some(tags) = query.tags.as_deref().filter(|s| !s.trim().is_empty()) {
            conditions.extend(search_filter::parse_tags(tags)?);
        }
        for condition in &conditions {
            df = df.filter(condition.to_expr()?).map_err(|e| {
                log::error!("Failed to apply search filter {condition:?}: {e}");
                QuerierError::QueryFailed(e)
            })?;
        }

        // Apply limit — we query for more spans than the requested trace count because
        // each trace typically contains many spans. This estimate avoids truncating traces.
        let (limit, span_limit) = clamped_limits(query.limit, self.max_search_limit)?;
        df = df.limit(0, Some(span_limit)).map_err(|e| {
            log::error!("Failed to apply limit: {e}");
            QuerierError::QueryFailed(e)
        })?;

        let results = df.collect().await.map_err(|e| {
            log::error!(
                "Failed to collect query results for tenant_slug={}, dataset_slug={}: {}",
                tenant_slug,
                dataset_slug,
                e
            );
            QuerierError::QueryFailed(e)
        })?;

        log::info!(
            "Query returned {} batches for tenant_slug={}, dataset_slug={}",
            results.len(),
            tenant_slug,
            dataset_slug
        );

        // Group spans by trace_id
        let mut traces_map: HashMap<String, HashMap<String, Span>> = HashMap::new();

        for batch in results {
            for row_index in 0..batch.num_rows() {
                let current_trace_id = batch
                    .column_by_name("trace_id")
                    .ok_or_else(|| {
                        QuerierError::InvalidInput("Missing required column 'trace_id'".to_string())
                    })?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        QuerierError::InvalidInput("Column 'trace_id' has wrong type".to_string())
                    })?
                    .value(row_index)
                    .to_string();

                let span_id = batch
                    .column_by_name("span_id")
                    .ok_or_else(|| {
                        QuerierError::InvalidInput("Missing required column 'span_id'".to_string())
                    })?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        QuerierError::InvalidInput("Column 'span_id' has wrong type".to_string())
                    })?
                    .value(row_index)
                    .to_string();

                let parent_span_id = batch
                    .column_by_name("parent_span_id")
                    .ok_or_else(|| {
                        QuerierError::InvalidInput(
                            "Missing required column 'parent_span_id'".to_string(),
                        )
                    })?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        QuerierError::InvalidInput(
                            "Column 'parent_span_id' has wrong type".to_string(),
                        )
                    })?
                    .value(row_index)
                    .to_string();

                let attributes = batch
                    .column_by_name("span_attributes")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                    .and_then(|arr| {
                        if arr.is_null(row_index) {
                            None
                        } else {
                            serde_json::from_str(arr.value(row_index)).ok()
                        }
                    })
                    .unwrap_or_default();

                let resource = batch
                    .column_by_name("resource_attributes")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                    .and_then(|arr| {
                        if arr.is_null(row_index) {
                            None
                        } else {
                            serde_json::from_str(arr.value(row_index)).ok()
                        }
                    })
                    .unwrap_or_default();

                let span = Span {
                    span_id: span_id.clone(),
                    parent_span_id: parent_span_id.clone(),
                    children: Vec::new(),
                    trace_id: current_trace_id.clone(),
                    status: SpanStatus::from_str(
                        batch
                            .column_by_name("status_code")
                            .ok_or_else(|| {
                                QuerierError::InvalidInput(
                                    "Missing required column 'status_code'".to_string(),
                                )
                            })?
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| {
                                QuerierError::InvalidInput(
                                    "Column 'status_code' has wrong type".to_string(),
                                )
                            })?
                            .value(row_index),
                    )
                    .unwrap_or(SpanStatus::Unspecified),
                    is_root: batch
                        .column_by_name("is_root")
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Missing required column 'is_root'".to_string(),
                            )
                        })?
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Column 'is_root' has wrong type".to_string(),
                            )
                        })?
                        .value(row_index),
                    name: batch
                        .column_by_name("span_name")
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Missing required column 'span_name'".to_string(),
                            )
                        })?
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Column 'span_name' has wrong type".to_string(),
                            )
                        })?
                        .value(row_index)
                        .to_string(),
                    service_name: batch
                        .column_by_name("service_name")
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Missing required column 'service_name'".to_string(),
                            )
                        })?
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Column 'service_name' has wrong type".to_string(),
                            )
                        })?
                        .value(row_index)
                        .to_string(),
                    span_kind: SpanKind::from_str(
                        batch
                            .column_by_name("span_kind")
                            .ok_or_else(|| {
                                QuerierError::InvalidInput(
                                    "Missing required column 'span_kind'".to_string(),
                                )
                            })?
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| {
                                QuerierError::InvalidInput(
                                    "Column 'span_kind' has wrong type".to_string(),
                                )
                            })?
                            .value(row_index),
                    )
                    .unwrap_or(SpanKind::Internal),
                    start_time_unix_nano: batch
                        .column_by_name("start_time_unix_nano")
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Missing required column 'start_time_unix_nano'".to_string(),
                            )
                        })?
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Column 'start_time_unix_nano' has wrong type".to_string(),
                            )
                        })?
                        .value(row_index) as u64,
                    duration_nano: batch
                        .column_by_name("duration_nanos")
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Missing required column 'duration_nanos'".to_string(),
                            )
                        })?
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Column 'duration_nanos' has wrong type".to_string(),
                            )
                        })?
                        .value(row_index) as u64,
                    attributes,
                    resource,
                };

                traces_map
                    .entry(current_trace_id)
                    .or_default()
                    .insert(span_id, span);
            }
        }

        // Build trace hierarchies, limited to requested count
        let mut traces = Vec::new();
        for (trace_id, span_map) in traces_map {
            if traces.len() >= limit {
                break;
            }

            let root_spans = model::span::build_span_hierarchy(span_map);

            traces.push(model::trace::Trace {
                trace_id,
                spans: root_spans,
            });
        }

        Ok(traces)
    }
}

#[async_trait]
impl TraceQuerier for TraceService {
    #[tracing::instrument]
    async fn find_shallow_by_id(
        &self,
        params: FindTraceByIdParams,
    ) -> Result<Option<model::trace::Trace>, QuerierError> {
        self.find_by_id(params).await
    }

    #[tracing::instrument]
    async fn find_by_id(
        &self,
        params: FindTraceByIdParams,
    ) -> Result<Option<model::trace::Trace>, QuerierError> {
        // Use default tenant and dataset for non-tenant-aware queries
        let tenant_id = "default";
        let dataset_id = "default";

        log::info!(
            "Querying for trace_id={} in tenant={}, dataset={}",
            params.trace_id,
            tenant_id,
            dataset_id
        );

        // Build safe table reference
        let table_ref = build_table_reference(tenant_id, dataset_id, "traces")
            .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;

        // Use DataFrame API with parameterized filter (prevents SQL injection)
        let df = self
            .session_context
            .table(table_ref)
            .await
            .map_err(|e| {
                log::error!(
                    "Failed to access table for trace_id={}: {}",
                    params.trace_id,
                    e
                );
                QuerierError::QueryFailed(e)
            })?
            .filter(col("trace_id").eq(lit(&params.trace_id)))
            .map_err(|e| {
                log::error!(
                    "Failed to apply filter for trace_id={}: {}",
                    params.trace_id,
                    e
                );
                QuerierError::QueryFailed(e)
            })?;

        let results = df.collect().await.map_err(|e| {
            log::error!(
                "Failed to collect query results for trace_id={}: {}",
                params.trace_id,
                e
            );
            QuerierError::QueryFailed(e)
        })?;

        log::info!(
            "Query returned {} rows for trace_id={}",
            results.len(),
            params.trace_id
        );

        // bail if no results were found
        if results.is_empty() {
            return Ok(None);
        }

        // Create a map to store all spans by their span_id for easy lookup
        let mut span_map: HashMap<String, Span> = HashMap::new();
        let mut trace_id = String::new();

        for batch in results {
            for row_index in 0..batch.num_rows() {
                let current_trace_id = batch
                    .column_by_name("trace_id")
                    .ok_or_else(|| {
                        QuerierError::InvalidInput("Missing required column 'trace_id'".to_string())
                    })?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        QuerierError::InvalidInput("Column 'trace_id' has wrong type".to_string())
                    })?
                    .value(row_index)
                    .to_string();
                if trace_id.is_empty() {
                    trace_id = current_trace_id.clone();
                }

                let span_id = batch
                    .column_by_name("span_id")
                    .ok_or_else(|| {
                        QuerierError::InvalidInput("Missing required column 'span_id'".to_string())
                    })?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        QuerierError::InvalidInput("Column 'span_id' has wrong type".to_string())
                    })?
                    .value(row_index)
                    .to_string();
                let parent_span_id = batch
                    .column_by_name("parent_span_id")
                    .ok_or_else(|| {
                        QuerierError::InvalidInput(
                            "Missing required column 'parent_span_id'".to_string(),
                        )
                    })?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        QuerierError::InvalidInput(
                            "Column 'parent_span_id' has wrong type".to_string(),
                        )
                    })?
                    .value(row_index)
                    .to_string();

                let attributes = batch
                    .column_by_name("span_attributes")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                    .and_then(|arr| {
                        if arr.is_null(row_index) {
                            None
                        } else {
                            serde_json::from_str(arr.value(row_index)).ok()
                        }
                    })
                    .unwrap_or_default();

                let resource = batch
                    .column_by_name("resource_attributes")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                    .and_then(|arr| {
                        if arr.is_null(row_index) {
                            None
                        } else {
                            serde_json::from_str(arr.value(row_index)).ok()
                        }
                    })
                    .unwrap_or_default();

                let span = Span {
                    span_id: span_id.clone(),
                    parent_span_id: parent_span_id.clone(),
                    children: Vec::new(),
                    trace_id: trace_id.clone(),
                    status: SpanStatus::from_str(
                        batch
                            .column_by_name("status_code")
                            .ok_or_else(|| {
                                QuerierError::InvalidInput(
                                    "Missing required column 'status_code'".to_string(),
                                )
                            })?
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| {
                                QuerierError::InvalidInput(
                                    "Column 'status_code' has wrong type".to_string(),
                                )
                            })?
                            .value(row_index),
                    )
                    .unwrap_or(SpanStatus::Unspecified),
                    is_root: batch
                        .column_by_name("is_root")
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Missing required column 'is_root'".to_string(),
                            )
                        })?
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Column 'is_root' has wrong type".to_string(),
                            )
                        })?
                        .value(row_index),
                    name: batch
                        .column_by_name("span_name")
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Missing required column 'span_name'".to_string(),
                            )
                        })?
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Column 'span_name' has wrong type".to_string(),
                            )
                        })?
                        .value(row_index)
                        .to_string(),
                    service_name: batch
                        .column_by_name("service_name")
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Missing required column 'service_name'".to_string(),
                            )
                        })?
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Column 'service_name' has wrong type".to_string(),
                            )
                        })?
                        .value(row_index)
                        .to_string(),
                    span_kind: SpanKind::from_str(
                        batch
                            .column_by_name("span_kind")
                            .ok_or_else(|| {
                                QuerierError::InvalidInput(
                                    "Missing required column 'span_kind'".to_string(),
                                )
                            })?
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| {
                                QuerierError::InvalidInput(
                                    "Column 'span_kind' has wrong type".to_string(),
                                )
                            })?
                            .value(row_index),
                    )
                    .unwrap_or(SpanKind::Internal),
                    attributes,
                    resource,
                    start_time_unix_nano: batch
                        .column_by_name("start_time_unix_nano")
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Missing required column 'start_time_unix_nano'".to_string(),
                            )
                        })?
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Column 'start_time_unix_nano' has wrong type".to_string(),
                            )
                        })?
                        .value(row_index) as u64,
                    duration_nano: batch
                        .column_by_name("duration_nanos")
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Missing required column 'duration_nanos'".to_string(),
                            )
                        })?
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Column 'duration_nanos' has wrong type".to_string(),
                            )
                        })?
                        .value(row_index) as u64,
                };

                span_map.insert(span_id.clone(), span);
            }
        }

        let root_spans = model::span::build_span_hierarchy(span_map);

        Ok(Some(model::trace::Trace {
            spans: root_spans,
            trace_id,
        }))
    }

    #[tracing::instrument]
    async fn find_traces(
        &self,
        query: SearchQueryParams,
    ) -> Result<Vec<model::trace::Trace>, QuerierError> {
        self.find_traces_with_tenant(query, "default", "default")
            .await
    }
}

/// Each trace typically contains many spans, so search fetches more spans
/// than the requested trace count to avoid truncating traces.
const SPANS_PER_TRACE_ESTIMATE: usize = 50;

/// Compute the effective (trace, span-row) limits for a search: validate
/// the client-supplied trace `limit`, clamp it to `max_search_limit` (the
/// client fully controls the value, so without a clamp `limit=40000000`
/// would materialize ~2e9 rows), and scale by the spans-per-trace estimate.
fn clamped_limits(
    client_limit: Option<i32>,
    max_search_limit: usize,
) -> Result<(usize, usize), QuerierError> {
    let raw_limit = client_limit.unwrap_or(20);
    let limit: usize = usize::try_from(raw_limit).map_err(|_| {
        QuerierError::InvalidInput(format!(
            "Invalid limit '{raw_limit}': must be a non-negative integer"
        ))
    })?;
    let limit = if limit > max_search_limit {
        log::warn!(
            "Clamping client-supplied search limit {limit} to the configured maximum {max_search_limit}"
        );
        max_search_limit
    } else {
        limit
    };
    let span_limit = limit.checked_mul(SPANS_PER_TRACE_ESTIMATE).ok_or_else(|| {
        QuerierError::InvalidInput(format!(
            "Limit {limit} * {SPANS_PER_TRACE_ESTIMATE} overflows"
        ))
    })?;
    Ok((limit, span_limit))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    #[test]
    fn span_limit_uses_default_when_absent() {
        assert_eq!(clamped_limits(None, 1000).unwrap(), (20, 20 * 50));
    }

    #[test]
    fn span_limit_respects_client_limit_below_max() {
        assert_eq!(clamped_limits(Some(100), 1000).unwrap(), (100, 100 * 50));
    }

    #[test]
    fn span_limit_clamps_excessive_client_limit() {
        // The issue's example: limit=40000000 must not produce ~2e9 rows.
        assert_eq!(
            clamped_limits(Some(40_000_000), 1000).unwrap(),
            (1000, 50_000)
        );
    }

    #[test]
    fn span_limit_rejects_negative_limit() {
        assert!(clamped_limits(Some(-1), 1000).is_err());
    }

    #[tokio::test]
    #[ignore = "Superseded by integration tests in tests-integration/tests/router_tempo_endpoints.rs. \
                This unit test would require complex Iceberg catalog setup for multi-tenancy."]
    async fn test_find_by_id() {
        let session_context = SessionContext::new();

        // Create tenant-scoped table for multi-tenancy testing
        let tenant_id = "test_tenant";
        let create_namespace = format!("CREATE SCHEMA IF NOT EXISTS iceberg.{tenant_id}");
        session_context
            .sql(&create_namespace)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let create_table = format!(
            "CREATE TABLE iceberg.{tenant_id}.traces (
                trace_id VARCHAR,
                span_id VARCHAR,
                parent_span_id VARCHAR,
                span_name VARCHAR,
                span_kind VARCHAR,
                start_time_unix_nano BIGINT,
                duration_nanos BIGINT,
                status_code VARCHAR,
                is_root BOOLEAN,
                service_name VARCHAR
            )"
        );
        session_context
            .sql(&create_table)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Insert test data
        let insert_data = format!(
            "INSERT INTO iceberg.{tenant_id}.traces VALUES (
                '1234',
                'span1',
                '',
                'test-span',
                'Server',
                1640995200000000000,
                100000000,
                'Ok',
                true,
                'test-service'
            )"
        );
        session_context
            .sql(&insert_data)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let service = TraceService::new(session_context, "test_traces".to_string());
        let params = FindTraceByIdParams {
            trace_id: "1234".to_string(),
            start: None,
            end: None,
        };

        // Use the tenant-aware method
        let trace = service
            .find_by_id_with_tenant(params, tenant_id, "production")
            .await
            .expect("Query failed")
            .expect("Trace not found");

        assert_eq!(trace.trace_id, "1234");
        assert_eq!(trace.spans.len(), 1);

        let span = &trace.spans[0];
        assert_eq!(span.span_id, "span1");
        assert_eq!(span.name, "test-span");
        assert_eq!(span.span_kind, SpanKind::Server);
    }
}
