use std::{collections::HashMap, fmt::Debug, str::FromStr, sync::Arc};

use common::model::{
    self,
    span::{Span, SpanEvent, SpanKind, SpanStatus, parse_span_events},
};
use datafusion::{
    arrow::{
        array::{Array, BooleanArray, Int64Array, MapArray, RecordBatch, StringArray},
        datatypes::{DataType, TimeUnit},
    },
    logical_expr::{Expr, col, lit},
    prelude::SessionContext,
    scalar::ScalarValue,
};

use super::{
    FindTraceByIdParams, SearchQueryParams, error::QuerierError, search_filter,
    table_ref::build_table_reference,
};

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
        let mut df = self
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

        // Apply the Tempo time hints as span-start bounds. Callers are
        // expected to pass a window bracketing the whole trace (Grafana's
        // Tempo datasource pads it by 30 minutes on each side), so this
        // prunes the scanned time range without truncating traces.
        //
        // Each hint is applied twice: a precise `start_time_unix_nano` row
        // filter, plus an equivalent (widened) predicate on the `timestamp`
        // partition column so that Iceberg can prune whole hour partitions
        // (the partition transform is `Hour(timestamp)`, so a filter on
        // `start_time_unix_nano` alone never engages partition pruning).
        let timestamp_type = df
            .schema()
            .fields()
            .iter()
            .find(|f| f.name() == "timestamp")
            .map(|f| f.data_type().clone());
        if let Some(start) = params.start {
            let start_nanos = unix_seconds_to_nanos("start", start)?;
            df = df
                .filter(col("start_time_unix_nano").gt_eq(lit(start_nanos)))
                .map_err(|e| {
                    log::error!(
                        "Failed to apply start hint for trace_id={}: {e}",
                        params.trace_id
                    );
                    QuerierError::QueryFailed(e)
                })?;
            if let Some(ts_type) = &timestamp_type {
                df = df
                    .filter(timestamp_bound_expr(start_nanos, ts_type, false)?)
                    .map_err(|e| {
                        log::error!(
                            "Failed to apply start partition bound for trace_id={}: {e}",
                            params.trace_id
                        );
                        QuerierError::QueryFailed(e)
                    })?;
            }
        }
        if let Some(end) = params.end {
            let end_nanos = unix_seconds_to_nanos("end", end)?;
            df = df
                .filter(col("start_time_unix_nano").lt_eq(lit(end_nanos)))
                .map_err(|e| {
                    log::error!(
                        "Failed to apply end hint for trace_id={}: {e}",
                        params.trace_id
                    );
                    QuerierError::QueryFailed(e)
                })?;
            if let Some(ts_type) = &timestamp_type {
                df = df
                    .filter(timestamp_bound_expr(end_nanos, ts_type, true)?)
                    .map_err(|e| {
                        log::error!(
                            "Failed to apply end partition bound for trace_id={}: {e}",
                            params.trace_id
                        );
                        QuerierError::QueryFailed(e)
                    })?;
            }
        }

        // Projection pushdown: only read the columns needed to reconstruct the
        // trace, so the scan skips the fat `events` / `links` / `scope_*`
        // columns entirely.
        df = df.select_columns(&TRACE_LOOKUP_COLUMNS).map_err(|e| {
            log::error!(
                "Failed to project trace lookup columns for trace_id={}: {e}",
                params.trace_id
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

                let attributes = attribute_map(&batch, "span_attributes", row_index);
                let resource = attribute_map(&batch, "resource_attributes", row_index);
                let events = span_events(&batch, row_index);

                let span = Span {
                    span_id: span_id.clone(),
                    parent_span_id: parent_span_id.clone(),
                    children: Vec::new(),
                    events,
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
            let start_nanos = unix_seconds_to_nanos("start", start)?;
            df = df
                .filter(col("start_time_unix_nano").gt_eq(lit(start_nanos)))
                .map_err(|e| {
                    log::error!("Failed to apply start time filter: {e}");
                    QuerierError::QueryFailed(e)
                })?;
        }
        if let Some(end) = query.end {
            let end_nanos = unix_seconds_to_nanos("end", end)?;
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
                .filter(col("duration_nanos").gt_eq(lit(min_dur)))
                .map_err(|e| {
                    log::error!("Failed to apply min duration filter: {e}");
                    QuerierError::QueryFailed(e)
                })?;
        }
        if let Some(max_dur) = query.max_duration {
            df = df
                .filter(col("duration_nanos").lt_eq(lit(max_dur)))
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
        let attr_ctx = super::logql::AttrContext {
            materialized: df
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .filter(|n| n.starts_with("label_"))
                .collect(),
            map_attrs: df.schema().fields().iter().any(|f| {
                f.name() == "span_attributes"
                    && matches!(
                        f.data_type(),
                        datafusion::arrow::datatypes::DataType::Map(_, _)
                    )
            }),
            // Traces tables carry no derived token column (logs only).
            attr_tokens: false,
        };
        // Query demand (epic #737, #733): attribute conditions are
        // materialization candidates.
        for condition in &conditions {
            if let search_filter::Selector::SpanAttribute(key)
            | search_filter::Selector::ResourceAttribute(key)
            | search_filter::Selector::AnyAttribute(key) = &condition.selector
            {
                common::attr_demand::record(tenant_slug, dataset_slug, "traces", key);
            }
        }
        for condition in &conditions {
            df = df.filter(condition.to_expr(&attr_ctx)?).map_err(|e| {
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

                let attributes = attribute_map(&batch, "span_attributes", row_index);
                let resource = attribute_map(&batch, "resource_attributes", row_index);

                let span = Span {
                    span_id: span_id.clone(),
                    parent_span_id: parent_span_id.clone(),
                    children: Vec::new(),
                    events: Vec::new(),
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

/// Columns required to reconstruct a trace in [`TraceService::find_by_id_with_tenant`].
/// Restricting the scan to these via projection pushdown avoids materializing the
/// large `links` list column and the `scope_*` maps, which are never consumed on
/// the single-trace path. `events` is included so span exceptions/annotations
/// survive to the trace view (it is a JSON string, not the fat list column).
const TRACE_LOOKUP_COLUMNS: [&str; 13] = [
    "trace_id",
    "span_id",
    "parent_span_id",
    "span_attributes",
    "resource_attributes",
    "status_code",
    "is_root",
    "span_name",
    "service_name",
    "span_kind",
    "start_time_unix_nano",
    "duration_nanos",
    "events",
];

/// Build a literal matching the on-disk `timestamp` partition column so a
/// time-range filter engages Iceberg hour-partition pruning (the partition
/// transform is `Hour(timestamp)`, whereas the precise row filter targets
/// `start_time_unix_nano`).
///
/// `nanos` is a unix-epoch nanosecond value. `col_type` is the Arrow type of
/// the `timestamp` column as reported by the table schema (Iceberg timestamps
/// commonly materialize as microseconds, but we adapt to whatever unit the
/// catalog reports). When the column unit is coarser than nanoseconds we widen
/// the bound — floor for a lower bound, ceil for an upper bound — so this
/// pruning predicate never excludes a row the precise `start_time_unix_nano`
/// filter would keep.
/// Convert a caller-supplied unix-**second** timestamp into nanoseconds.
///
/// Tempo's `start`/`end` query parameters are unix seconds. Anything that
/// does not survive the conversion is not a unix-second timestamp — most
/// often milliseconds from a client that guessed the unit. Saturating such a
/// value produced an `i64::MAX` sentinel that downstream predicate handling
/// could not represent, so the query silently matched nothing and the caller
/// saw a plausible-looking "trace not found". Reject it instead, so the
/// router can answer 400 and name the problem.
fn unix_seconds_to_nanos(name: &str, seconds: i64) -> Result<i64, QuerierError> {
    seconds.checked_mul(1_000_000_000).ok_or_else(|| {
        QuerierError::InvalidInput(format!(
            "`{name}` ({seconds}) is out of range: expected a unix timestamp in seconds \
             (did you send milliseconds?)"
        ))
    })
}

fn timestamp_bound_scalar(
    nanos: i64,
    col_type: &DataType,
    round_up: bool,
) -> Result<ScalarValue, QuerierError> {
    // `nanos` is a unix-epoch value (non-negative in practice); floor for a
    // lower bound, ceil for an upper bound (signed `i64::div_ceil` is still
    // unstable). Deriving the ceiling from the remainder rather than
    // `nanos + divisor - 1` keeps it overflow-free at the top of the range,
    // where the addition would otherwise wrap to a negative bound that
    // excludes every row.
    let scale = |divisor: i64| {
        let quotient = nanos.div_euclid(divisor);
        if round_up && nanos.rem_euclid(divisor) != 0 {
            quotient.saturating_add(1)
        } else {
            quotient
        }
    };
    Ok(match col_type {
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            ScalarValue::TimestampNanosecond(Some(nanos), tz.clone())
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            ScalarValue::TimestampMicrosecond(Some(scale(1_000)), tz.clone())
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            ScalarValue::TimestampMillisecond(Some(scale(1_000_000)), tz.clone())
        }
        DataType::Timestamp(TimeUnit::Second, tz) => {
            ScalarValue::TimestampSecond(Some(scale(1_000_000_000)), tz.clone())
        }
        other => {
            return Err(QuerierError::InvalidInput(format!(
                "`timestamp` column has unexpected type {other:?}; cannot build partition-pruning bound"
            )));
        }
    })
}

/// Wrap [`timestamp_bound_scalar`] into a `col("timestamp") >=/<= <literal>` expression.
fn timestamp_bound_expr(
    nanos: i64,
    col_type: &DataType,
    round_up: bool,
) -> Result<Expr, QuerierError> {
    let bound = lit(timestamp_bound_scalar(nanos, col_type, round_up)?);
    Ok(if round_up {
        col("timestamp").lt_eq(bound)
    } else {
        col("timestamp").gt_eq(bound)
    })
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

/// Read one row of a trace attribute column into a `serde_json` map,
/// handling both storage forms: a typed `Map<Utf8, Utf8>` column (current
/// tables, written by the writer's schema coercion) and a legacy `Utf8`
/// column holding a flat JSON object. An absent column, a null row, or
/// unparseable content yields an empty map.
///
/// The map form stores every value as a string, so its values come back as
/// `Value::String`; the legacy JSON form preserves the original scalar type.
fn attribute_map(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> HashMap<String, serde_json::Value> {
    let Some(column) = batch.column_by_name(name) else {
        return HashMap::new();
    };

    if let Some(map) = column.as_any().downcast_ref::<MapArray>() {
        if map.is_null(row) {
            return HashMap::new();
        }
        let entries = map.value(row);
        let (Some(keys), Some(values)) = (
            entries.column(0).as_any().downcast_ref::<StringArray>(),
            entries.column(1).as_any().downcast_ref::<StringArray>(),
        ) else {
            return HashMap::new();
        };
        let mut out = HashMap::with_capacity(entries.len());
        for j in 0..entries.len() {
            if !keys.is_null(j) && !values.is_null(j) {
                out.insert(
                    keys.value(j).to_string(),
                    serde_json::Value::String(values.value(j).to_string()),
                );
            }
        }
        return out;
    }

    if let Some(arr) = column.as_any().downcast_ref::<StringArray>()
        && !arr.is_null(row)
        && let Ok(serde_json::Value::Object(map)) =
            serde_json::from_str::<serde_json::Value>(arr.value(row))
    {
        return map.into_iter().collect();
    }

    HashMap::new()
}

/// Read the stored `events` JSON-string column for one row into span events.
/// Absent column or null row yields no events.
fn span_events(batch: &RecordBatch, row: usize) -> Vec<SpanEvent> {
    batch
        .column_by_name("events")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .filter(|arr| !arr.is_null(row))
        .map(|arr| parse_span_events(arr.value(row)))
        .unwrap_or_default()
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
    fn attribute_map_reads_typed_map_columns() {
        use datafusion::arrow::array::{ArrayRef, MapBuilder, StringBuilder};
        use datafusion::arrow::record_batch::RecordBatch;

        // Build a Map<Utf8, Utf8> column, the form the writer stores today.
        let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        builder.keys().append_value("http.method");
        builder.values().append_value("POST");
        builder.keys().append_value("http.status_code");
        builder.values().append_value("200");
        builder.append(true).unwrap();
        let column: ArrayRef = Arc::new(builder.finish());
        let batch = RecordBatch::try_from_iter([("span_attributes", column)]).unwrap();

        let attrs = attribute_map(&batch, "span_attributes", 0);
        assert_eq!(
            attrs.get("http.method"),
            Some(&serde_json::Value::String("POST".to_string()))
        );
        assert_eq!(
            attrs.get("http.status_code"),
            Some(&serde_json::Value::String("200".to_string()))
        );
    }

    #[test]
    fn attribute_map_reads_legacy_json_columns() {
        use datafusion::arrow::record_batch::RecordBatch;

        let column: datafusion::arrow::array::ArrayRef = Arc::new(StringArray::from(vec![Some(
            r#"{"db.system":"postgresql"}"#,
        )]));
        let batch = RecordBatch::try_from_iter([("span_attributes", column)]).unwrap();

        let attrs = attribute_map(&batch, "span_attributes", 0);
        assert_eq!(
            attrs.get("db.system"),
            Some(&serde_json::Value::String("postgresql".to_string()))
        );
    }

    #[test]
    fn attribute_map_is_empty_for_absent_column_or_null_row() {
        use datafusion::arrow::array::ArrayRef;
        use datafusion::arrow::record_batch::RecordBatch;

        let column: ArrayRef = Arc::new(StringArray::from(vec![Option::<&str>::None]));
        let batch = RecordBatch::try_from_iter([("span_attributes", column)]).unwrap();
        assert!(attribute_map(&batch, "span_attributes", 0).is_empty());
        assert!(attribute_map(&batch, "resource_attributes", 0).is_empty());
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

    #[test]
    fn timestamp_bound_nanosecond_is_exact() {
        let nanos = 1_700_000_000_123_456_789i64;
        let lower = timestamp_bound_scalar(
            nanos,
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )
        .unwrap();
        assert_eq!(lower, ScalarValue::TimestampNanosecond(Some(nanos), None));
    }

    #[test]
    fn timestamp_bound_microsecond_widens_outward() {
        let nanos = 1_700_000_000_123_456_789i64; // not micro-aligned
        let ty = DataType::Timestamp(TimeUnit::Microsecond, None);
        let lower = timestamp_bound_scalar(nanos, &ty, false).unwrap();
        let upper = timestamp_bound_scalar(nanos, &ty, true).unwrap();
        // Lower bound floors, upper bound ceils, so the [lower, upper] micro
        // window always contains the exact nanosecond instant.
        assert_eq!(
            lower,
            ScalarValue::TimestampMicrosecond(Some(1_700_000_000_123_456), None)
        );
        assert_eq!(
            upper,
            ScalarValue::TimestampMicrosecond(Some(1_700_000_000_123_457), None)
        );
    }

    #[test]
    fn timestamp_bound_preserves_timezone() {
        let ty = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let bound = timestamp_bound_scalar(1_700_000_000_000_000_000, &ty, false).unwrap();
        assert_eq!(
            bound,
            ScalarValue::TimestampMicrosecond(Some(1_700_000_000_000_000), Some("UTC".into()))
        );
    }

    #[test]
    fn timestamp_bound_rejects_non_timestamp() {
        assert!(timestamp_bound_scalar(0, &DataType::Int64, false).is_err());
    }

    /// The ceiling used for upper bounds must not overflow at the top of the
    /// i64 range. `nanos + divisor - 1` panics there in debug and wraps to a
    /// negative bound in release, which silently excludes every row.
    #[test]
    fn timestamp_bound_ceiling_survives_i64_max() {
        for unit in [
            TimeUnit::Nanosecond,
            TimeUnit::Microsecond,
            TimeUnit::Millisecond,
            TimeUnit::Second,
        ] {
            let ty = DataType::Timestamp(unit, None);
            let bound = timestamp_bound_scalar(i64::MAX, &ty, true)
                .unwrap_or_else(|e| panic!("{unit:?} upper bound must build: {e}"));
            let value = match bound {
                ScalarValue::TimestampNanosecond(Some(v), _)
                | ScalarValue::TimestampMicrosecond(Some(v), _)
                | ScalarValue::TimestampMillisecond(Some(v), _)
                | ScalarValue::TimestampSecond(Some(v), _) => v,
                other => panic!("{unit:?} produced unexpected scalar {other:?}"),
            };
            assert!(value > 0, "{unit:?} upper bound wrapped negative: {value}");
        }
    }

    #[test]
    fn timestamp_bound_rounds_outward() {
        let ty = DataType::Timestamp(TimeUnit::Microsecond, None);
        // 1_500ns is 1.5us: floor to 1, ceil to 2, so neither bound can
        // exclude a row the precise nanosecond filter would keep.
        assert_eq!(
            timestamp_bound_scalar(1_500, &ty, false).unwrap(),
            ScalarValue::TimestampMicrosecond(Some(1), None)
        );
        assert_eq!(
            timestamp_bound_scalar(1_500, &ty, true).unwrap(),
            ScalarValue::TimestampMicrosecond(Some(2), None)
        );
    }

    /// Tempo's `start`/`end` are unix *seconds*. A caller that sends
    /// milliseconds used to saturate to an `i64::MAX` nanosecond sentinel
    /// and silently return "not found"; it must be rejected instead.
    #[test]
    fn unix_seconds_to_nanos_rejects_out_of_range() {
        assert!(matches!(
            unix_seconds_to_nanos("end", 1_785_829_987_000),
            Err(QuerierError::InvalidInput(_))
        ));
        assert!(matches!(
            unix_seconds_to_nanos("start", i64::MAX),
            Err(QuerierError::InvalidInput(_))
        ));
    }

    #[test]
    fn unix_seconds_to_nanos_accepts_representable_instants() {
        // Now, and the largest second that still fits in i64 nanoseconds.
        assert_eq!(
            unix_seconds_to_nanos("end", 1_785_829_987).unwrap(),
            1_785_829_987_000_000_000
        );
        assert_eq!(
            unix_seconds_to_nanos("end", 9_223_372_036).unwrap(),
            9_223_372_036_000_000_000
        );
        assert_eq!(unix_seconds_to_nanos("start", 0).unwrap(), 0);
    }

    #[test]
    fn timestamp_bound_expr_direction() {
        let ty = DataType::Timestamp(TimeUnit::Nanosecond, None);
        // Just ensure both directions build without error and differ.
        let lower = timestamp_bound_expr(1_000, &ty, false).unwrap();
        let upper = timestamp_bound_expr(1_000, &ty, true).unwrap();
        assert_ne!(format!("{lower:?}"), format!("{upper:?}"));
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
