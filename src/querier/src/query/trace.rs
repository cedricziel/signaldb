use std::{collections::HashMap, fmt::Debug, str::FromStr, sync::Arc};

use async_trait::async_trait;

use common::model::{
    self,
    span::{Span, SpanKind, SpanStatus},
};
use datafusion::{
    arrow::array::{BooleanArray, Int64Array, StringArray, UInt64Array},
    logical_expr::{col, lit},
    prelude::SessionContext,
};

use super::{FindTraceByIdParams, SearchQueryParams, TraceQuerier, error::QuerierError};

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
}

impl Debug for TraceService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraceService")
            .field("session_context", &"set")
            .field("traces_path", &self.traces_path)
            .finish()
    }
}

impl Clone for TraceService {
    fn clone(&self) -> Self {
        Self {
            session_context: Arc::clone(&self.session_context),
            traces_path: self.traces_path.clone(),
        }
    }
}

impl TraceService {
    #[allow(dead_code)]
    pub fn new(session_context: SessionContext, traces_path: String) -> Self {
        Self {
            session_context: Arc::new(session_context),
            traces_path,
        }
    }

    /// Validate and construct a safe table reference for multi-tenant queries
    ///
    /// # Security
    /// This function validates tenant_id and dataset_id to prevent SQL injection.
    /// Only alphanumeric characters, underscores, and hyphens are allowed.
    fn build_table_ref(
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<String, QuerierError> {
        // Validate tenant_id and dataset_id contain only safe characters
        let is_valid_identifier = |s: &str| {
            !s.is_empty()
                && s.chars()
                    .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        };

        if !is_valid_identifier(tenant_id) {
            return Err(QuerierError::InvalidInput(format!(
                "Invalid tenant_id '{tenant_id}': must contain only alphanumeric, underscore, or hyphen characters"
            )));
        }

        if !is_valid_identifier(dataset_id) {
            return Err(QuerierError::InvalidInput(format!(
                "Invalid dataset_id '{dataset_id}': must contain only alphanumeric, underscore, or hyphen characters"
            )));
        }

        if !is_valid_identifier(table_name) {
            return Err(QuerierError::InvalidInput(format!(
                "Invalid table_name '{table_name}': must contain only alphanumeric, underscore, or hyphen characters"
            )));
        }

        // Build the fully qualified table name: iceberg.{tenant_id}.{dataset_id}.{table_name}
        Ok(format!("iceberg.{tenant_id}.{dataset_id}.{table_name}"))
    }

    /// Find a trace by ID with tenant isolation
    pub async fn find_by_id_with_tenant(
        &self,
        params: FindTraceByIdParams,
        tenant_id: &str,
        dataset_id: &str,
    ) -> Result<Option<model::trace::Trace>, QuerierError> {
        log::info!(
            "Querying for trace_id={} in tenant={}, dataset={}",
            params.trace_id,
            tenant_id,
            dataset_id
        );

        // Build safe table reference with tenant and dataset isolation
        let table_ref = Self::build_table_ref(tenant_id, dataset_id, "traces")?;

        // Use DataFrame API with parameterized filter (prevents SQL injection)
        let df = self
            .session_context
            .table(&table_ref)
            .await
            .map_err(|e| {
                log::error!(
                    "Failed to access table '{}' for tenant={}, dataset={}: {}",
                    table_ref,
                    tenant_id,
                    dataset_id,
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
                "Failed to collect query results for trace_id={}, tenant={}, dataset={}: {}",
                params.trace_id,
                tenant_id,
                dataset_id,
                e
            );
            QuerierError::QueryFailed(e)
        })?;

        log::info!(
            "Query returned {} rows for trace_id={}, tenant={}, dataset={}",
            results.len(),
            params.trace_id,
            tenant_id,
            dataset_id
        );

        // bail if no results were found
        if results.is_empty() {
            return Ok(None);
        }

        // Create a map to store all spans by their span_id for easy lookup
        let mut span_map: HashMap<String, Span> = HashMap::new();
        let mut root_spans = Vec::new();
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
                        .column_by_name("name")
                        .ok_or_else(|| {
                            QuerierError::InvalidInput("Missing required column 'name'".to_string())
                        })?
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            QuerierError::InvalidInput("Column 'name' has wrong type".to_string())
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
                        .downcast_ref::<UInt64Array>()
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Column 'start_time_unix_nano' has wrong type".to_string(),
                            )
                        })?
                        .value(row_index),
                    duration_nano: batch
                        .column_by_name("duration_nano")
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Missing required column 'duration_nano'".to_string(),
                            )
                        })?
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or_else(|| {
                            QuerierError::InvalidInput(
                                "Column 'duration_nano' has wrong type".to_string(),
                            )
                        })?
                        .value(row_index),
                    attributes: HashMap::new(),
                    resource: HashMap::new(),
                };

                if span.is_root {
                    root_spans.push(span.clone());
                }
                span_map.insert(span_id, span);
            }
        }

        Ok(Some(model::trace::Trace {
            trace_id,
            spans: root_spans,
        }))
    }

    /// Find traces with tenant isolation
    pub async fn find_traces_with_tenant(
        &self,
        _query: SearchQueryParams,
        tenant_id: &str,
        dataset_id: &str,
    ) -> Result<Vec<model::trace::Trace>, QuerierError> {
        log::info!(
            "Searching traces in tenant={}, dataset={}",
            tenant_id,
            dataset_id
        );

        // Build safe table reference with tenant and dataset isolation
        let table_ref = Self::build_table_ref(tenant_id, dataset_id, "traces")?;

        // Use DataFrame API (prevents SQL injection)
        let df = self.session_context.table(&table_ref).await.map_err(|e| {
            log::error!(
                "Failed to access table '{}' for tenant={}, dataset={}: {}",
                table_ref,
                tenant_id,
                dataset_id,
                e
            );
            QuerierError::QueryFailed(e)
        })?;

        let results = df.collect().await.map_err(|e| {
            log::error!(
                "Failed to collect query results for tenant={}, dataset={}: {}",
                tenant_id,
                dataset_id,
                e
            );
            QuerierError::QueryFailed(e)
        })?;

        log::info!(
            "Query returned {} rows for tenant={}, dataset={}",
            results.len(),
            tenant_id,
            dataset_id
        );

        let traces = Vec::new();

        // For now, just return empty traces
        // Full implementation would group spans by trace_id and build trace hierarchy
        Ok(traces)
    }
}

#[async_trait]
impl TraceQuerier for TraceService {
    #[tracing::instrument]
    async fn find_shallow_by_id(
        &self,
        _params: FindTraceByIdParams,
    ) -> Result<Option<model::trace::Trace>, QuerierError> {
        unimplemented!()
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
        let table_ref = Self::build_table_ref(tenant_id, dataset_id, "traces")?;

        // Use DataFrame API with parameterized filter (prevents SQL injection)
        let df = self
            .session_context
            .table(&table_ref)
            .await
            .map_err(|e| {
                log::error!(
                    "Failed to access table '{}' for trace_id={}: {}",
                    table_ref,
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
        let mut root_spans = Vec::new();
        let mut trace_id = String::new();

        for batch in results {
            for row_index in 0..batch.num_rows() {
                let current_trace_id = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row_index)
                    .to_string();
                if trace_id.is_empty() {
                    trace_id = current_trace_id.clone();
                }

                let span_id = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row_index)
                    .to_string();
                let parent_span_id = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row_index)
                    .to_string();

                let span = Span {
                    span_id: span_id.clone(),
                    parent_span_id: parent_span_id.clone(),
                    children: Vec::new(),
                    trace_id: trace_id.clone(),
                    status: SpanStatus::from_str(
                        batch
                            .column_by_name("status")
                            .expect("unable to find column status")
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(row_index),
                    )
                    .unwrap_or(SpanStatus::Unspecified),
                    is_root: batch
                        .column_by_name("is_root")
                        .expect("unable to find column 'is_root'")
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .value(row_index),
                    name: batch
                        .column_by_name("span_name")
                        .expect("unable to find column 'span_name'")
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(row_index)
                        .to_string(),
                    service_name: batch
                        .column_by_name("service_name")
                        .expect("unable to find column 'service_name'")
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(row_index)
                        .to_string(),
                    span_kind: SpanKind::from_str(
                        batch
                            .column_by_name("span_kind")
                            .expect("unable to find column 'span_kind'")
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(row_index),
                    )
                    .unwrap_or(SpanKind::Internal),
                    attributes: HashMap::new(),
                    resource: HashMap::new(),
                    start_time_unix_nano: batch
                        .column_by_name("start_time_unix_nano")
                        .expect("unable to find column 'start_time_unix_nano'")
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row_index) as u64,
                    duration_nano: batch
                        .column_by_name("duration_nanos")
                        .expect("unable to find column 'duration_nanos'")
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row_index) as u64,
                };

                span_map.insert(span_id.clone(), span);
            }
        }

        // Collect parent-child relationships first
        let mut parent_child_pairs = Vec::new();
        for span in span_map.values() {
            if span.parent_span_id != "00000000" {
                parent_child_pairs.push((span.parent_span_id.clone(), span.span_id.clone()));
            } else {
                root_spans.push(span.clone());
            }
        }

        // Collect child spans first
        let mut child_spans: HashMap<String, Vec<Span>> = HashMap::new();
        for (parent_span_id, span_id) in parent_child_pairs {
            if let Some(child_span) = span_map.get(&span_id) {
                child_spans
                    .entry(parent_span_id)
                    .or_default()
                    .push(child_span.clone());
            }
        }

        // Build the hierarchy by linking child spans to their parents
        for (parent_span_id, children) in child_spans {
            if let Some(parent_span) = span_map.get_mut(&parent_span_id) {
                parent_span.children.extend(children);
            }
        }

        // Create the final trace structure
        let trace = model::trace::Trace {
            spans: root_spans,
            trace_id,
        };

        Ok(Some(trace))
    }

    #[tracing::instrument]
    async fn find_traces(
        &self,
        _query: SearchQueryParams,
    ) -> Result<Vec<model::trace::Trace>, QuerierError> {
        // Use the existing session context that should have Iceberg catalog registered
        let query = "SELECT * FROM iceberg.default.traces;";

        let df = self
            .session_context
            .sql(query)
            .await
            .map_err(QuerierError::QueryFailed)?;

        let results = df.collect().await.map_err(QuerierError::QueryFailed)?;

        log::info!("Query returned {} rows", results.len());
        log::info!("Results: {:?}", results);

        let traces = Vec::new();

        for batch in results {
            for row_index in 0..batch.num_rows() {
                let trace_id = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row_index)
                    .to_string();
                let span_id = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row_index)
                    .to_string();
                let parent_span_id = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row_index)
                    .to_string();

                let _span = Span {
                    span_id: span_id.clone(),
                    parent_span_id: parent_span_id.clone(),
                    children: Vec::new(),
                    trace_id: trace_id.clone(),
                    status: SpanStatus::Unspecified,
                    is_root: batch
                        .column_by_name("is_root")
                        .expect("unable to find column 'is_root'")
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .value(row_index),
                    name: batch
                        .column_by_name("span_name")
                        .expect("unable to find column 'span_name'")
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(row_index)
                        .to_string(),
                    service_name: batch
                        .column_by_name("service_name")
                        .expect("unable to find column 'service_name'")
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(row_index)
                        .to_string(),
                    span_kind: SpanKind::from_str(
                        batch
                            .column_by_name("span_kind")
                            .expect("unable to find column 'span_kind'")
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(row_index),
                    )
                    .unwrap_or(SpanKind::Internal),
                    start_time_unix_nano: batch
                        .column_by_name("start_time_unix_nano")
                        .expect("unable to find column 'start_time_unix_nano'")
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row_index) as u64,
                    duration_nano: batch
                        .column_by_name("duration_nanos")
                        .expect("unable to find column 'duration_nanos'")
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row_index) as u64,
                    attributes: HashMap::new(),
                    resource: HashMap::new(),
                };
            }
        }

        Ok(traces)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

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
                duration_nano BIGINT,
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
