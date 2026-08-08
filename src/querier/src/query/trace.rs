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
    prelude::{DataFrame, SessionContext},
    scalar::ScalarValue,
};

use super::{
    FindTraceByIdParams, SearchQueryParams, error::QuerierError, search_filter,
    table_lookup::optional_table,
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
        tracing::info!(
            "Querying for trace_id={} in tenant_slug={}, dataset_slug={}",
            params.trace_id,
            tenant_slug,
            dataset_slug
        );

        // Convert the client-supplied time hints before touching the catalog:
        // the range check needs no schema, so a malformed hint must be
        // reported as invalid input even when the dataset has no `traces`
        // table yet (mirrors `build_search_dataframe`).
        let start_bound = params
            .start
            .map(|start| unix_seconds_to_nanos("start", start))
            .transpose()?;
        let end_bound = params
            .end
            .map(|end| unix_seconds_to_nanos("end", end))
            .transpose()?;

        // A dataset with no `traces` table holds no trace to find.
        let Some(df) =
            optional_table(&self.session_context, tenant_slug, dataset_slug, "traces").await?
        else {
            return Ok(None);
        };

        // Use DataFrame API with parameterized filter (prevents SQL injection)
        let mut df = df
            .filter(col("trace_id").eq(lit(&params.trace_id)))
            .map_err(|e| {
                tracing::error!(
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
        if let Some(start_nanos) = start_bound {
            df = df
                .filter(col("start_time_unix_nano").gt_eq(lit(start_nanos)))
                .map_err(|e| {
                    tracing::error!(
                        "Failed to apply start hint for trace_id={}: {e}",
                        params.trace_id
                    );
                    QuerierError::QueryFailed(e)
                })?;
            if let Some(ts_type) = &timestamp_type {
                df = df
                    .filter(timestamp_bound_expr(start_nanos, ts_type, false)?)
                    .map_err(|e| {
                        tracing::error!(
                            "Failed to apply start partition bound for trace_id={}: {e}",
                            params.trace_id
                        );
                        QuerierError::QueryFailed(e)
                    })?;
            }
        }
        if let Some(end_nanos) = end_bound {
            df = df
                .filter(col("start_time_unix_nano").lt_eq(lit(end_nanos)))
                .map_err(|e| {
                    tracing::error!(
                        "Failed to apply end hint for trace_id={}: {e}",
                        params.trace_id
                    );
                    QuerierError::QueryFailed(e)
                })?;
            if let Some(ts_type) = &timestamp_type {
                df = df
                    .filter(timestamp_bound_expr(end_nanos, ts_type, true)?)
                    .map_err(|e| {
                        tracing::error!(
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
            tracing::error!(
                "Failed to project trace lookup columns for trace_id={}: {e}",
                params.trace_id
            );
            QuerierError::QueryFailed(e)
        })?;

        let results = df.collect().await.map_err(|e| {
            tracing::error!(
                "Failed to collect query results for trace_id={}, tenant_slug={}, dataset_slug={}: {}",
                params.trace_id,
                tenant_slug,
                dataset_slug,
                e
            );
            QuerierError::QueryFailed(e)
        })?;

        tracing::info!(
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
            // Downcast every column consumed by this batch's row loop once,
            // instead of re-resolving `column_by_name` + `downcast_ref` on
            // every row (previously ~13 downcasts per row).
            let trace_ids = required_string_column(&batch, "trace_id")?;
            let span_ids = required_string_column(&batch, "span_id")?;
            let parent_span_ids = required_string_column(&batch, "parent_span_id")?;
            let status_codes = required_string_column(&batch, "status_code")?;
            let is_roots = required_bool_column(&batch, "is_root")?;
            let span_names = required_string_column(&batch, "span_name")?;
            let service_names = required_string_column(&batch, "service_name")?;
            let span_kinds = required_string_column(&batch, "span_kind")?;
            let start_times = required_i64_column(&batch, "start_time_unix_nano")?;
            let durations = required_i64_column(&batch, "duration_nanos")?;
            let span_attrs = resolve_attribute_column(&batch, "span_attributes");
            let resource_attrs = resolve_attribute_column(&batch, "resource_attributes");
            let events_col = resolve_events_column(&batch);

            for row_index in 0..batch.num_rows() {
                // Use named column access instead of positions
                let current_trace_id = trace_ids.value(row_index).to_string();

                if trace_id.is_empty() {
                    trace_id = current_trace_id.clone();
                }

                let span_id = span_ids.value(row_index).to_string();
                let parent_span_id = parent_span_ids.value(row_index).to_string();
                let attributes = attribute_map_from(&span_attrs, row_index);
                let resource = attribute_map_from(&resource_attrs, row_index);
                let events = span_events_from(events_col, row_index);

                let span = Span {
                    span_id: span_id.clone(),
                    parent_span_id: parent_span_id.clone(),
                    children: Vec::new(),
                    events,
                    trace_id: trace_id.clone(),
                    status: SpanStatus::from_str(status_codes.value(row_index))
                        .unwrap_or(SpanStatus::Unspecified),
                    is_root: is_roots.value(row_index),
                    name: span_names.value(row_index).to_string(),
                    service_name: service_names.value(row_index).to_string(),
                    span_kind: SpanKind::from_str(span_kinds.value(row_index))
                        .unwrap_or(SpanKind::Internal),
                    start_time_unix_nano: start_times.value(row_index) as u64,
                    duration_nano: durations.value(row_index) as u64,
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
        tracing::info!(
            "Searching traces in tenant_slug={}, dataset_slug={}",
            tenant_slug,
            dataset_slug
        );

        let Some((df, limit)) = self
            .build_search_dataframe(&query, tenant_slug, dataset_slug)
            .await?
        else {
            return Ok(Vec::new());
        };

        let results = df.collect().await.map_err(|e| {
            tracing::error!(
                "Failed to collect query results for tenant_slug={}, dataset_slug={}: {}",
                tenant_slug,
                dataset_slug,
                e
            );
            QuerierError::QueryFailed(e)
        })?;

        tracing::info!(
            "Query returned {} batches for tenant_slug={}, dataset_slug={}",
            results.len(),
            tenant_slug,
            dataset_slug
        );

        // Group spans by trace_id
        let mut traces_map: HashMap<String, HashMap<String, Span>> = HashMap::new();

        for batch in results {
            // Downcast every column consumed by this batch's row loop once,
            // instead of re-resolving `column_by_name` + `downcast_ref` on
            // every row (previously ~12 downcasts per row).
            let trace_ids = required_string_column(&batch, "trace_id")?;
            let span_ids = required_string_column(&batch, "span_id")?;
            let parent_span_ids = required_string_column(&batch, "parent_span_id")?;
            let status_codes = required_string_column(&batch, "status_code")?;
            let is_roots = required_bool_column(&batch, "is_root")?;
            let span_names = required_string_column(&batch, "span_name")?;
            let service_names = required_string_column(&batch, "service_name")?;
            let span_kinds = required_string_column(&batch, "span_kind")?;
            let start_times = required_i64_column(&batch, "start_time_unix_nano")?;
            let durations = required_i64_column(&batch, "duration_nanos")?;
            let span_attrs = resolve_attribute_column(&batch, "span_attributes");
            let resource_attrs = resolve_attribute_column(&batch, "resource_attributes");

            for row_index in 0..batch.num_rows() {
                let current_trace_id = trace_ids.value(row_index).to_string();
                let span_id = span_ids.value(row_index).to_string();
                let parent_span_id = parent_span_ids.value(row_index).to_string();
                let attributes = attribute_map_from(&span_attrs, row_index);
                let resource = attribute_map_from(&resource_attrs, row_index);

                let span = Span {
                    span_id: span_id.clone(),
                    parent_span_id: parent_span_id.clone(),
                    children: Vec::new(),
                    events: Vec::new(),
                    trace_id: current_trace_id.clone(),
                    status: SpanStatus::from_str(status_codes.value(row_index))
                        .unwrap_or(SpanStatus::Unspecified),
                    is_root: is_roots.value(row_index),
                    name: span_names.value(row_index).to_string(),
                    service_name: service_names.value(row_index).to_string(),
                    span_kind: SpanKind::from_str(span_kinds.value(row_index))
                        .unwrap_or(SpanKind::Internal),
                    start_time_unix_nano: start_times.value(row_index) as u64,
                    duration_nano: durations.value(row_index) as u64,
                    attributes,
                    resource,
                };

                traces_map
                    .entry(current_trace_id)
                    .or_default()
                    .insert(span_id, span);
            }
        }

        // Build trace hierarchies, truncated to the requested count
        // deterministically (newest first) rather than by HashMap iteration
        // order.
        let traces = order_traces_for_truncation(traces_map, limit)
            .into_iter()
            .map(|(trace_id, span_map)| model::trace::Trace {
                trace_id,
                spans: model::span::build_span_hierarchy(span_map),
            })
            .collect();

        Ok(traces)
    }

    /// Build the search scan for [`TraceService::find_traces_with_tenant`],
    /// returning the `DataFrame` plus the effective trace-count limit.
    ///
    /// Split out from the collect/assembly step so tests can assert on the
    /// logical plan. The plan shape addresses issue #928:
    /// - every time bound lands on `start_time_unix_nano` (precise row
    ///   filter) *and* on the `timestamp` partition column, so Iceberg
    ///   hour-partition pruning engages (transform: `Hour(timestamp)`);
    /// - spans are sorted `start_time_unix_nano DESC` before the span limit,
    ///   so "most recent N traces" keeps the newest spans instead of N
    ///   arbitrary rows;
    /// - only [`TRACE_SEARCH_COLUMNS`] are projected, skipping the fat
    ///   `events`/`links`/`scope_*` columns the search assembly never reads.
    async fn build_search_dataframe(
        &self,
        query: &SearchQueryParams,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Option<(DataFrame, usize)>, QuerierError> {
        // Validate the client-supplied selectors and window bounds before
        // touching the catalog: a malformed query must still be reported as
        // such on a dataset whose `traces` table does not exist yet.
        //
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
        let start_bound = query
            .start
            .map(|start| unix_seconds_to_nanos("start", start))
            .transpose()?;
        let end_bound = query
            .end
            .map(|end| unix_seconds_to_nanos("end", end))
            .transpose()?;
        let (limit, span_limit) = clamped_limits(query.limit, self.max_search_limit)?;

        // A dataset with no `traces` table has no traces to search.
        let Some(mut df) =
            optional_table(&self.session_context, tenant_slug, dataset_slug, "traces").await?
        else {
            return Ok(None);
        };

        // Apply time range filters if provided. Each bound is applied twice:
        // a precise `start_time_unix_nano` row filter, plus an equivalent
        // (widened) predicate on the `timestamp` partition column so Iceberg
        // can prune whole hour partitions — same dual-bound scheme as
        // `find_by_id_with_tenant`.
        let timestamp_type = df
            .schema()
            .fields()
            .iter()
            .find(|f| f.name() == "timestamp")
            .map(|f| f.data_type().clone());
        if let Some(start_nanos) = start_bound {
            df = df
                .filter(col("start_time_unix_nano").gt_eq(lit(start_nanos)))
                .map_err(|e| {
                    tracing::error!("Failed to apply start time filter: {e}");
                    QuerierError::QueryFailed(e)
                })?;
            if let Some(ts_type) = &timestamp_type {
                df = df
                    .filter(timestamp_bound_expr(start_nanos, ts_type, false)?)
                    .map_err(|e| {
                        tracing::error!("Failed to apply start partition bound: {e}");
                        QuerierError::QueryFailed(e)
                    })?;
            }
        }
        if let Some(end_nanos) = end_bound {
            df = df
                .filter(col("start_time_unix_nano").lt_eq(lit(end_nanos)))
                .map_err(|e| {
                    tracing::error!("Failed to apply end time filter: {e}");
                    QuerierError::QueryFailed(e)
                })?;
            if let Some(ts_type) = &timestamp_type {
                df = df
                    .filter(timestamp_bound_expr(end_nanos, ts_type, true)?)
                    .map_err(|e| {
                        tracing::error!("Failed to apply end partition bound: {e}");
                        QuerierError::QueryFailed(e)
                    })?;
            }
        }

        // Apply duration filters
        if let Some(min_dur) = query.min_duration {
            df = df
                .filter(col("duration_nanos").gt_eq(lit(min_dur)))
                .map_err(|e| {
                    tracing::error!("Failed to apply min duration filter: {e}");
                    QuerierError::QueryFailed(e)
                })?;
        }
        if let Some(max_dur) = query.max_duration {
            df = df
                .filter(col("duration_nanos").lt_eq(lit(max_dur)))
                .map_err(|e| {
                    tracing::error!("Failed to apply max duration filter: {e}");
                    QuerierError::QueryFailed(e)
                })?;
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
                tracing::error!("Failed to apply search filter {condition:?}: {e}");
                QuerierError::QueryFailed(e)
            })?;
        }

        // Projection pushdown: only read the columns the search assembly
        // consumes, so the scan skips the fat `events` / `links` / `scope_*`
        // columns entirely. Applied after the filters, which may reference
        // columns outside the projection (e.g. `label_*`).
        df = df.select_columns(&TRACE_SEARCH_COLUMNS).map_err(|e| {
            tracing::error!("Failed to project trace search columns: {e}");
            QuerierError::QueryFailed(e)
        })?;

        // Order newest-first before limiting: LIMIT without ORDER BY would
        // return arbitrary spans, so "most recent N traces" was N arbitrary
        // traces.
        df = df
            .sort(vec![col("start_time_unix_nano").sort(false, false)])
            .map_err(|e| {
                tracing::error!("Failed to apply search ordering: {e}");
                QuerierError::QueryFailed(e)
            })?;

        // Apply limit — we query for more spans than the requested trace count because
        // each trace typically contains many spans. This estimate avoids truncating traces.
        df = df.limit(0, Some(span_limit)).map_err(|e| {
            tracing::error!("Failed to apply limit: {e}");
            QuerierError::QueryFailed(e)
        })?;

        Ok(Some((df, limit)))
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

/// Columns the search result assembly in
/// [`TraceService::find_traces_with_tenant`] actually reads (persisted traces
/// v2 names). [`TRACE_LOOKUP_COLUMNS`] minus `events`: search builds span
/// summaries without events, so projecting the JSON `events` string away
/// keeps the scan lean.
const TRACE_SEARCH_COLUMNS: [&str; 12] = [
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
];

/// Order assembled traces for truncation: most-recent span start descending,
/// with `trace_id` as the tie-break, then keep the first `limit`.
///
/// Search fetches spans newest-first, so this keeps the traces whose spans
/// topped that stream. Without it, truncation followed `HashMap` iteration
/// order and "most recent N traces" returned N arbitrary traces (issue #928).
fn order_traces_for_truncation(
    traces_map: HashMap<String, HashMap<String, Span>>,
    limit: usize,
) -> Vec<(String, HashMap<String, Span>)> {
    let mut traces: Vec<(u64, String, HashMap<String, Span>)> = traces_map
        .into_iter()
        .map(|(trace_id, spans)| {
            let newest_start = spans
                .values()
                .map(|s| s.start_time_unix_nano)
                .max()
                .unwrap_or(0);
            (newest_start, trace_id, spans)
        })
        .collect();
    traces.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    traces.truncate(limit);
    traces
        .into_iter()
        .map(|(_, trace_id, spans)| (trace_id, spans))
        .collect()
}

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
///
/// Shared with the IR planner (`super::ir_planner`), which has the same
/// partition-pruning obligation for trace scans.
pub(super) fn timestamp_bound_expr(
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
        tracing::warn!(
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

/// Fetch a required `Utf8` column by name. Resolving this once per batch
/// (outside the row loop) avoids re-running `column_by_name` +
/// `downcast_ref` on every row.
fn required_string_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a StringArray, QuerierError> {
    let column = batch
        .column_by_name(name)
        .ok_or_else(|| QuerierError::InvalidInput(format!("Missing required column '{name}'")))?;
    column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| QuerierError::InvalidInput(format!("Column '{name}' has wrong type")))
}

/// Fetch a required `Boolean` column by name; see [`required_string_column`].
fn required_bool_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a BooleanArray, QuerierError> {
    let column = batch
        .column_by_name(name)
        .ok_or_else(|| QuerierError::InvalidInput(format!("Missing required column '{name}'")))?;
    column
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| QuerierError::InvalidInput(format!("Column '{name}' has wrong type")))
}

/// Fetch a required `Int64` column by name; see [`required_string_column`].
fn required_i64_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a Int64Array, QuerierError> {
    let column = batch
        .column_by_name(name)
        .ok_or_else(|| QuerierError::InvalidInput(format!("Missing required column '{name}'")))?;
    column
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| QuerierError::InvalidInput(format!("Column '{name}' has wrong type")))
}

/// A trace attribute column resolved once per batch (outside the row loop),
/// mirroring the two storage forms [`attribute_map_from`] reads: a typed
/// `Map<Utf8, Utf8>` column (current tables, written by the writer's schema
/// coercion) and a legacy `Utf8` column holding a flat JSON object.
enum AttributeColumn<'a> {
    Map(&'a MapArray),
    Json(&'a StringArray),
    Absent,
}

/// Resolve the attribute column named `name` for a batch once; see
/// [`AttributeColumn`].
fn resolve_attribute_column<'a>(batch: &'a RecordBatch, name: &str) -> AttributeColumn<'a> {
    let Some(column) = batch.column_by_name(name) else {
        return AttributeColumn::Absent;
    };
    if let Some(map) = column.as_any().downcast_ref::<MapArray>() {
        return AttributeColumn::Map(map);
    }
    if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
        return AttributeColumn::Json(arr);
    }
    AttributeColumn::Absent
}

/// Read one row of an [`AttributeColumn`] resolved by
/// [`resolve_attribute_column`] into a `serde_json` map. An absent column, a
/// null row, or unparseable content yields an empty map.
///
/// The map form stores every value as a string, so its values come back as
/// `Value::String`; the legacy JSON form preserves the original scalar type.
fn attribute_map_from(
    column: &AttributeColumn<'_>,
    row: usize,
) -> HashMap<String, serde_json::Value> {
    match column {
        AttributeColumn::Map(map) => {
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
            out
        }
        AttributeColumn::Json(arr) => {
            if arr.is_null(row) {
                return HashMap::new();
            }
            match serde_json::from_str::<serde_json::Value>(arr.value(row)) {
                Ok(serde_json::Value::Object(map)) => map.into_iter().collect(),
                _ => HashMap::new(),
            }
        }
        AttributeColumn::Absent => HashMap::new(),
    }
}

/// Resolve the `events` JSON-string column for a batch once; see
/// [`span_events_from`].
fn resolve_events_column(batch: &RecordBatch) -> Option<&StringArray> {
    batch
        .column_by_name("events")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
}

/// Read one row of the `events` column resolved by [`resolve_events_column`]
/// into span events. Absent column or null row yields no events.
fn span_events_from(events: Option<&StringArray>, row: usize) -> Vec<SpanEvent> {
    events
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

    /// Register a traces table with the persisted v2 column names (including
    /// the `timestamp` partition column and the fat `links`/`scope_*` columns
    /// that search must never materialize) under `t.d.traces`.
    fn search_session() -> SessionContext {
        use datafusion::arrow::array::{
            ArrayRef, BooleanArray, MapBuilder, MapFieldNames, StringBuilder,
            TimestampNanosecondArray,
        };
        use datafusion::arrow::datatypes::{Field, Fields, Schema};
        use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
        use datafusion::catalog::{CatalogProvider, MemTable, SchemaProvider};

        fn map_field(name: &str) -> Field {
            let entries = Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("keys", DataType::Utf8, false),
                    Field::new("values", DataType::Utf8, true),
                ])),
                false,
            );
            Field::new(name, DataType::Map(Arc::new(entries), false), true)
        }

        fn empty_maps(rows: usize) -> ArrayRef {
            let names = MapFieldNames {
                entry: "entries".to_string(),
                key: "keys".to_string(),
                value: "values".to_string(),
            };
            let mut b = MapBuilder::new(Some(names), StringBuilder::new(), StringBuilder::new());
            for _ in 0..rows {
                b.append(true).unwrap();
            }
            Arc::new(b.finish())
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("span_name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("span_kind", DataType::Utf8, false),
            Field::new("status_code", DataType::Utf8, true),
            Field::new("is_root", DataType::Boolean, false),
            Field::new("start_time_unix_nano", DataType::Int64, false),
            Field::new("duration_nanos", DataType::Int64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            map_field("span_attributes"),
            map_field("resource_attributes"),
            Field::new("events", DataType::Utf8, true),
            Field::new("links", DataType::Utf8, true),
            Field::new("scope_name", DataType::Utf8, true),
        ]));

        // Three single-span traces at distinct times (seconds 1, 2, 3).
        let starts: Vec<i64> = vec![1_000_000_000, 2_000_000_000, 3_000_000_000];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["t-old", "t-mid", "t-new"])),
                Arc::new(StringArray::from(vec!["s1", "s2", "s3"])),
                Arc::new(StringArray::from(vec![Some(""), Some(""), Some("")])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(StringArray::from(vec!["api", "api", "api"])),
                Arc::new(StringArray::from(vec!["Server", "Server", "Server"])),
                Arc::new(StringArray::from(vec![Some("Ok"), Some("Ok"), Some("Ok")])),
                Arc::new(BooleanArray::from(vec![true, true, true])),
                Arc::new(Int64Array::from(starts.clone())),
                Arc::new(Int64Array::from(vec![100_i64, 100, 100])),
                Arc::new(TimestampNanosecondArray::from(starts)),
                empty_maps(3),
                empty_maps(3),
                Arc::new(StringArray::from(vec![Option::<&str>::None; 3])),
                Arc::new(StringArray::from(vec![Option::<&str>::None; 3])),
                Arc::new(StringArray::from(vec![Option::<&str>::None; 3])),
            ],
        )
        .unwrap();

        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("traces".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    fn search_service() -> TraceService {
        TraceService::new(search_session(), "traces".to_string())
    }

    fn search_params() -> SearchQueryParams {
        SearchQueryParams {
            q: None,
            tags: None,
            min_duration: None,
            max_duration: None,
            limit: None,
            start: None,
            end: None,
        }
    }

    /// Issue #928 defect 1: the search scan must mirror its
    /// `start_time_unix_nano` bounds onto the `timestamp` partition column so
    /// Iceberg hour-partition pruning engages (the partition transform is
    /// `Hour(timestamp)`).
    #[tokio::test]
    async fn search_plan_bounds_partition_column() {
        let service = search_service();
        let query = SearchQueryParams {
            start: Some(1),
            end: Some(3),
            ..search_params()
        };
        let (df, _) = service
            .build_search_dataframe(&query, "t", "d")
            .await
            .unwrap()
            .expect("traces table is registered");
        let plan = format!("{}", df.logical_plan().display_indent());
        assert!(
            plan.contains("start_time_unix_nano >="),
            "missing precise lower row bound:\n{plan}"
        );
        assert!(
            plan.contains("start_time_unix_nano <="),
            "missing precise upper row bound:\n{plan}"
        );
        assert!(
            plan.contains(".timestamp >="),
            "missing partition-pruning lower bound on `timestamp`:\n{plan}"
        );
        assert!(
            plan.contains(".timestamp <="),
            "missing partition-pruning upper bound on `timestamp`:\n{plan}"
        );
    }

    /// Issue #928 defect 2 (query shape): LIMIT without ORDER BY returns
    /// arbitrary spans; the plan must sort newest-first before limiting.
    #[tokio::test]
    async fn search_plan_orders_newest_first_before_limit() {
        let service = search_service();
        let (df, _) = service
            .build_search_dataframe(&search_params(), "t", "d")
            .await
            .unwrap()
            .expect("traces table is registered");
        let plan = format!("{}", df.logical_plan().display_indent());
        let sort_pos = plan
            .find("Sort:")
            .unwrap_or_else(|| panic!("no Sort in plan:\n{plan}"));
        let limit_pos = plan
            .find("Limit:")
            .unwrap_or_else(|| panic!("no Limit in plan:\n{plan}"));
        assert!(
            plan.contains("start_time_unix_nano DESC"),
            "sort key must be start_time_unix_nano DESC:\n{plan}"
        );
        // display_indent prints root-first, so the Limit node (root side)
        // must appear before the Sort node it wraps.
        assert!(
            limit_pos < sort_pos,
            "Limit must apply on top of Sort:\n{plan}"
        );
    }

    /// Issue #928 defect 3: search must project only the columns its result
    /// assembly reads, skipping the fat `links`/`events`/`scope_*` columns.
    #[tokio::test]
    async fn search_plan_projects_only_consumed_columns() {
        let service = search_service();
        let (df, _) = service
            .build_search_dataframe(&search_params(), "t", "d")
            .await
            .unwrap()
            .expect("traces table is registered");
        let names: Vec<&str> = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(
            names,
            TRACE_SEARCH_COLUMNS.to_vec(),
            "search projection drifted"
        );
        for fat in ["links", "events", "scope_name"] {
            assert!(!names.contains(&fat), "search must not materialize `{fat}`");
        }
    }

    /// End to end: the "most recent N traces" contract — newest traces, in
    /// deterministic newest-first order, not N arbitrary HashMap entries.
    #[tokio::test]
    async fn search_returns_most_recent_traces_newest_first() {
        let service = search_service();
        let query = SearchQueryParams {
            limit: Some(2),
            ..search_params()
        };
        let traces = service
            .find_traces_with_tenant(query, "t", "d")
            .await
            .unwrap();
        let ids: Vec<&str> = traces.iter().map(|t| t.trace_id.as_str()).collect();
        assert_eq!(ids, vec!["t-new", "t-mid"]);
    }

    fn test_span(trace_id: &str, span_id: &str, start: u64) -> Span {
        Span {
            span_id: span_id.to_string(),
            parent_span_id: String::new(),
            children: Vec::new(),
            events: Vec::new(),
            trace_id: trace_id.to_string(),
            status: SpanStatus::Unspecified,
            is_root: true,
            name: "span".to_string(),
            service_name: "svc".to_string(),
            span_kind: SpanKind::Internal,
            start_time_unix_nano: start,
            duration_nano: 1,
            attributes: HashMap::new(),
            resource: HashMap::new(),
        }
    }

    /// Issue #928 defect 2 (truncation): trace truncation must be
    /// deterministic — most-recent span start descending, trace_id as the
    /// tie-break — regardless of HashMap iteration order.
    #[test]
    fn trace_truncation_is_deterministic_newest_first() {
        let mut traces_map: HashMap<String, HashMap<String, Span>> = HashMap::new();
        for (trace_id, starts) in [
            ("t-old", vec![10_u64, 20]),
            ("t-new", vec![15, 400]),
            ("t-mid", vec![300]),
            ("t-tie", vec![300]),
        ] {
            let spans: HashMap<String, Span> = starts
                .into_iter()
                .enumerate()
                .map(|(i, s)| {
                    let span_id = format!("{trace_id}-{i}");
                    (span_id.clone(), test_span(trace_id, &span_id, s))
                })
                .collect();
            traces_map.insert(trace_id.to_string(), spans);
        }

        let ordered = order_traces_for_truncation(traces_map.clone(), 3);
        let ids: Vec<&str> = ordered.iter().map(|(id, _)| id.as_str()).collect();
        // t-new has the most recent span (400); the 300-tie breaks on
        // trace_id; t-old (newest span 20) is truncated away.
        assert_eq!(ids, vec!["t-new", "t-mid", "t-tie"]);

        // Stable across repeated invocations (HashMap order must not leak).
        for _ in 0..10 {
            let again = order_traces_for_truncation(traces_map.clone(), 3);
            let again_ids: Vec<&str> = again.iter().map(|(id, _)| id.as_str()).collect();
            assert_eq!(again_ids, ids);
        }
    }

    #[test]
    fn trace_truncation_keeps_everything_under_limit() {
        let mut traces_map: HashMap<String, HashMap<String, Span>> = HashMap::new();
        traces_map.insert(
            "only".to_string(),
            HashMap::from([("s".to_string(), test_span("only", "s", 1))]),
        );
        let ordered = order_traces_for_truncation(traces_map, 20);
        assert_eq!(ordered.len(), 1);
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

        let resolved = resolve_attribute_column(&batch, "span_attributes");
        let attrs = attribute_map_from(&resolved, 0);
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

        let resolved = resolve_attribute_column(&batch, "span_attributes");
        let attrs = attribute_map_from(&resolved, 0);
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
        assert!(
            attribute_map_from(&resolve_attribute_column(&batch, "span_attributes"), 0).is_empty()
        );
        assert!(
            attribute_map_from(&resolve_attribute_column(&batch, "resource_attributes"), 0)
                .is_empty()
        );
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

    // ---- Absent `traces` table reads as empty (issue #972) ----

    /// A `t.d` dataset registered in the catalog but holding no tables.
    fn service_without_traces_table() -> TraceService {
        use datafusion::catalog::CatalogProvider;
        use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};

        let ctx = SessionContext::new();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", Arc::new(MemorySchemaProvider::new()))
            .unwrap();
        ctx.register_catalog("t", cat);
        TraceService::new(ctx, "traces".to_string())
    }

    #[tokio::test]
    async fn find_by_id_on_absent_table_is_not_found() {
        let service = service_without_traces_table();
        let trace = service
            .find_by_id_with_tenant(
                FindTraceByIdParams {
                    trace_id: "abc123".to_string(),
                    start: None,
                    end: None,
                },
                "t",
                "d",
            )
            .await
            .expect("absent table must not error");
        assert!(trace.is_none());
    }

    #[tokio::test]
    async fn search_on_absent_table_is_empty() {
        let service = service_without_traces_table();
        let traces = service
            .find_traces_with_tenant(search_params(), "t", "d")
            .await
            .expect("absent table must not error");
        assert!(traces.is_empty());
    }

    #[tokio::test]
    async fn unknown_tenant_still_errors_on_search() {
        let service = service_without_traces_table();
        assert!(
            service
                .find_traces_with_tenant(search_params(), "nosuchtenant", "d")
                .await
                .is_err(),
            "unknown tenant must not read as empty"
        );
    }

    #[tokio::test]
    async fn invalid_search_query_still_errors_when_table_is_absent() {
        let service = service_without_traces_table();
        let mut query = search_params();
        query.q = Some("{ not valid traceql ((".to_string());
        assert!(
            service
                .find_traces_with_tenant(query, "t", "d")
                .await
                .is_err(),
            "a malformed query must not read as empty"
        );
    }

    /// Schema-independent input validation must run *before* the table
    /// lookup, so a malformed time hint still errors on a dataset whose
    /// `traces` table has not been provisioned yet rather than silently
    /// reading as "not found".
    #[tokio::test]
    async fn out_of_range_time_hint_still_errors_when_table_is_absent() {
        let service = service_without_traces_table();
        assert!(matches!(
            service
                .find_by_id_with_tenant(
                    FindTraceByIdParams {
                        trace_id: "abc123".to_string(),
                        start: Some(i64::MAX),
                        end: None,
                    },
                    "t",
                    "d",
                )
                .await,
            Err(QuerierError::InvalidInput(_))
        ));
        assert!(matches!(
            service
                .find_by_id_with_tenant(
                    FindTraceByIdParams {
                        trace_id: "abc123".to_string(),
                        start: None,
                        end: Some(1_785_829_987_000),
                    },
                    "t",
                    "d",
                )
                .await,
            Err(QuerierError::InvalidInput(_))
        ));
    }
}
