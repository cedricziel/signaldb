use std::{collections::HashMap, fmt::Debug, sync::Arc};

use async_trait::async_trait;

use common::model::{
    self,
    span::{Span, SpanKind, SpanStatus},
};
use datafusion::{
    arrow::array::{BooleanArray, StringArray},
    prelude::{ParquetReadOptions, SessionContext},
};

use super::{error::QuerierError, FindTraceByIdParams, SearchQueryParams, TraceQuerier};

const SHALLOW_TRACE_BY_ID_QUERY: &str = "SELECT * FROM traces WHERE trace_id = '{trace_id}';";

const TRACE_BY_ID_QUERY: &str = "WITH RECURSIVE trace_hierarchy AS (
    SELECT *, ARRAY[span_id] AS path FROM traces WHERE trace_id = '{trace_id}'
    UNION ALL
    SELECT t.*, th.path || t.span_id
    FROM traces t
    INNER JOIN trace_hierarchy th ON t.parent_span_id = th.span_id OR th.parent_span_id = t.span_id
    WHERE NOT t.span_id = ANY(th.path)  -- Prevent revisiting nodes
)
SELECT DISTINCT * FROM trace_hierarchy;";

const TRACES_BY_QUERY: &str = "WITH RECURSIVE trace_hierarchy AS (
    SELECT t.* FROM traces t
    INNER JOIN trace_hierarchy th ON t.parent_span_id = th.span_id

    UNION ALL

    SELECT t.* FROM traces t
    INNER JOIN trace_hierarchy th ON th.parent_span_id = t.span_id
)
SELECT * FROM trace_hierarchy;";

pub struct TraceService {
    // skip debug on session_context
    session_context: Arc<SessionContext>,
}

impl Debug for TraceService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraceService")
            .field("session_context", &"set")
            .finish()
    }
}

impl Clone for TraceService {
    fn clone(&self) -> Self {
        Self {
            session_context: Arc::clone(&self.session_context),
        }
    }
}

impl TraceService {
    pub fn new(session_context: SessionContext) -> Self {
        Self {
            session_context: Arc::new(session_context),
        }
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
        log::info!("Querying for trace_id: {}", params.trace_id);

        let query = TRACE_BY_ID_QUERY.replace("{trace_id}", &params.trace_id);

        let df = self.session_context.sql(&query).await.map_err(|e| {
            log::error!("Failed to execute query: {:?}, {:?}", query, e);

            QuerierError::QueryFailed(e)
        })?;

        let results = df
            .collect()
            .await
            .map_err(|e: datafusion::error::DataFusionError| {
                log::error!("Failed to collect results: {:?}", e);

                QuerierError::QueryFailed(e)
            })?;

        log::info!("Query returned {} rows", results.len());
        log::info!("Results: {:?}", results);

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
                    .into(),
                    is_root: batch
                        .column_by_name("is_root")
                        .expect("unable to find column 'is_root'")
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .value(row_index),
                    name: batch
                        .column_by_name("name")
                        .expect("unable to find column 'name'")
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
                    ),
                    attributes: HashMap::new(),
                    resource: HashMap::new(),
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
                    .or_insert_with(Vec::new)
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
        query: SearchQueryParams,
    ) -> Result<Vec<model::trace::Trace>, QuerierError> {
        let ctx = SessionContext::new();
        ctx.register_parquet("traces", ".data/ds/traces", ParquetReadOptions::default())
            .await
            .map_err(|e| QuerierError::FailedToRegisterParquet(e))?;

        let query = "SELECT * FROM traces;";

        let df = ctx
            .sql(query)
            .await
            .map_err(|e| QuerierError::QueryFailed(e))?;

        let results = df
            .collect()
            .await
            .map_err(|e| QuerierError::QueryFailed(e))?;

        log::info!("Query returned {} rows", results.len());
        log::info!("Results: {:?}", results);

        let mut traces = Vec::new();

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
                    .into(),
                    is_root: batch
                        .column_by_name("is_root")
                        .expect("unable to find column 'is_root'")
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .value(row_index),
                    name: batch
                        .column_by_name("name")
                        .expect("unable to find column 'name'")
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
                    ),
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

    #[tokio::test]
    async fn test_find_by_id() {
        let service = TraceService::new();
        let params = FindTraceByIdParams {
            trace_id: "1234".to_string(),
            start: None,
            end: None,
        };

        let result = service.find_by_id(params).await.unwrap();
        assert!(result.is_none());
    }
}
