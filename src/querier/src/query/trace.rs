use std::{
    collections::{BTreeSet, HashMap},
    fmt::Debug,
    str::FromStr,
    sync::Arc,
};

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
    FindTraceByIdParams, SearchQueryParams, TraceTagNames, TraceTagsParams,
    error::QuerierError,
    search_filter,
    table_lookup::{
        LABEL_SCAN_LIMIT, distinct_non_empty, optional_table, string_column, time_window,
    },
};

/// Fixed intrinsic tag names: fields Tempo derives per-span/per-trace
/// rather than reading from resource/span attributes.
pub const INTRINSIC_TAGS: &[&str] = &[
    "name",
    "status",
    "kind",
    "duration",
    "rootServiceName",
    "rootName",
];

/// Static enumeration values for the `status` intrinsic tag.
const STATUS_VALUES: &[&str] = &["ok", "error", "unset"];

/// Static enumeration values for the `kind` intrinsic tag — the span kinds
/// accepted by [`search_filter`]'s `kind` selector.
const KIND_VALUES: &[&str] = &["internal", "server", "client", "producer", "consumer"];

/// Upper bound on rows sampled for tag/tag-value discovery — shares
/// `table_lookup::LABEL_SCAN_LIMIT` with the other signals' discovery paths.
const TAG_SCAN_LIMIT: usize = LABEL_SCAN_LIMIT;

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
            let trace_ids = string_column(&batch, "trace_id")?;
            let span_ids = string_column(&batch, "span_id")?;
            let parent_span_ids = string_column(&batch, "parent_span_id")?;
            let status_codes = string_column(&batch, "status_code")?;
            let is_roots = required_bool_column(&batch, "is_root")?;
            let span_names = string_column(&batch, "span_name")?;
            let service_names = string_column(&batch, "service_name")?;
            let span_kinds = string_column(&batch, "span_kind")?;
            let start_times = required_i64_column(&batch, "start_time_unix_nano")?;
            let durations = required_i64_column(&batch, "duration_nanos")?;
            let span_attrs = resolve_attribute_column(&batch, "span_attributes");
            let resource_attrs = resolve_attribute_column(&batch, "resource_attributes");
            let events_col = resolve_events_column(&batch);

            for row_index in 0..batch.num_rows() {
                // Use named column access instead of positions
                if trace_id.is_empty() {
                    trace_id = trace_ids.value(row_index).to_string();
                }

                let span_id = span_ids.value(row_index).to_string();
                let parent_span_id = parent_span_ids.value(row_index).to_string();
                let attributes = attribute_map_from(&span_attrs, row_index);
                let resource = attribute_map_from(&resource_attrs, row_index);
                let events = span_events_from(events_col, row_index);

                let span = Span {
                    span_id: span_id.clone(),
                    parent_span_id,
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
            let trace_ids = string_column(&batch, "trace_id")?;
            let span_ids = string_column(&batch, "span_id")?;
            let parent_span_ids = string_column(&batch, "parent_span_id")?;
            let status_codes = string_column(&batch, "status_code")?;
            let is_roots = required_bool_column(&batch, "is_root")?;
            let span_names = string_column(&batch, "span_name")?;
            let service_names = string_column(&batch, "service_name")?;
            let span_kinds = string_column(&batch, "span_kind")?;
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
                    parent_span_id,
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
    /// Builds **one** IR document for the whole search filter — `q`, `tags`,
    /// and the duration bounds all become predicates conjoined in the same
    /// `where` stage — then plans it through
    /// [`super::ir_planner::plan_document`], the single planner entry point
    /// (D1 of `ir-single-lowering`). Split out from the collect/assembly step
    /// so tests can assert on the logical plan.
    ///
    /// The plan shape addresses issue #928:
    /// - every time bound lands on `start_time_unix_nano` (precise row
    ///   filter) *and* on the `timestamp` partition column, so Iceberg
    ///   hour-partition pruning engages (transform: `Hour(timestamp)`);
    /// - spans are sorted `start_time_unix_nano DESC` before the span limit,
    ///   so "most recent N traces" keeps the newest spans instead of N
    ///   arbitrary rows;
    /// - the document's `fields` names the logical fields that resolve to
    ///   exactly [`TRACE_SEARCH_COLUMNS`], in the same order — a `fields`
    ///   entry that resolves to a `Column` projects the physical name
    ///   unchanged (`ir_planner::Lowering::apply_projection`), skipping the
    ///   fat `events`/`links`/`scope_*` columns the search assembly never
    ///   reads.
    ///
    /// **Time range**: the IR always plans over a range, but a client may
    /// send no `start`/`end` at all. An absent bound becomes
    /// `UNBOUNDED_SEARCH_START_NS`/`_END_NS` here rather than e.g. `0`, so it
    /// excludes nothing — a span with a negative or far-future
    /// `start_time_unix_nano` still matches (see
    /// `unbounded_search_keeps_far_past_and_far_future_spans` below; issue
    /// #920's unix-seconds-overflow lesson applies to a *supplied* bound,
    /// which still goes through `unix_seconds_to_nanos` unchanged).
    async fn build_search_dataframe(
        &self,
        query: &SearchQueryParams,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Option<(DataFrame, usize)>, QuerierError> {
        use common::query_ir::{
            ComparisonOp, Direction, Document, Leaf, Order, Predicate, Range, ResultEnvelope, Stage,
        };

        let (limit, span_limit) = clamped_limits(query.limit, self.max_search_limit)?;

        // Conditions contribute predicates in the same order the old path
        // applies them: duration bounds, then `q`, then `tags`. Query
        // demand (epic #737, #733) is recorded for every attribute selector
        // either carries, exactly like the old path's combined loop.
        let mut predicates: Vec<Predicate> = Vec::new();
        if let Some(min_dur) = query.min_duration {
            predicates.push(Predicate::Leaf(Leaf {
                field: "duration".to_string(),
                op: ComparisonOp::Gte,
                value: Some(serde_json::json!(min_dur)),
            }));
        }
        if let Some(max_dur) = query.max_duration {
            predicates.push(Predicate::Leaf(Leaf {
                field: "duration".to_string(),
                op: ComparisonOp::Lte,
                value: Some(serde_json::json!(max_dur)),
            }));
        }
        if let Some(q) = query.q.as_deref().filter(|s| !s.trim().is_empty()) {
            // Parsed once more for its conditions' attribute keys (demand
            // recording matches the old path exactly); the predicate itself
            // comes from `ql_ir::traceql_to_ir`, the same lowering `q` alone
            // already uses, so the two can't disagree on what `q` means.
            for condition in &traceql::parse(q)? {
                record_attribute_demand(tenant_slug, dataset_slug, condition);
            }
            let doc = ql_ir::traceql_to_ir(q, "0", "0").map_err(QuerierError::from)?;
            if let Some(Stage::Where(predicate)) = doc.pipeline.into_iter().next() {
                predicates.push(predicate);
            }
        }
        if let Some(tags) = query.tags.as_deref().filter(|s| !s.trim().is_empty()) {
            let conditions = search_filter::parse_tags(tags)?;
            for condition in &conditions {
                record_attribute_demand(tenant_slug, dataset_slug, condition);
            }
            predicates.push(super::tags_to_ir::conditions_to_predicate(&conditions)?);
        }
        let where_stage = match predicates.len() {
            0 => None,
            1 => Some(Stage::Where(predicates.remove(0))),
            _ => Some(Stage::Where(Predicate::And(predicates))),
        };

        // An absent bound spans (almost) the full representable range rather
        // than a narrower placeholder such as `0` — see this method's doc
        // comment. Exactly `[i64::MIN, i64::MAX]` is one value too wide:
        // DataFusion's own interval-cardinality arithmetic
        // (`Interval::cardinality`, used during optimization) computes
        // `upper - lower + 1` and panics on overflow for that exact span.
        // One step in from each end still covers every representable
        // timestamp any real ingest could produce.
        let start_ns = match query.start {
            Some(start) => unix_seconds_to_nanos("start", start)?,
            None => UNBOUNDED_SEARCH_START_NS,
        };
        let end_ns = match query.end {
            Some(end) => unix_seconds_to_nanos("end", end)?,
            None => UNBOUNDED_SEARCH_END_NS,
        };

        let mut pipeline: Vec<Stage> = Vec::new();
        pipeline.extend(where_stage);
        pipeline.push(Stage::Order(vec![Order {
            of: "start_time_unix_nano".to_string(),
            dir: Direction::Desc,
        }]));
        pipeline.push(Stage::Limit(span_limit as u64));

        let doc = Document {
            ir_version: 1,
            from: "traces".to_string(),
            range: Range {
                from: serde_json::json!(start_ns),
                to: serde_json::json!(end_ns),
            },
            result: ResultEnvelope::Rows,
            fields: Some(
                TRACE_SEARCH_IR_FIELDS
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            ),
            pipeline,
        };

        let Some((df, _window)) = super::ir_planner::plan_document(
            &self.session_context,
            &doc,
            tenant_slug,
            dataset_slug,
            0,
        )
        .await?
        else {
            return Ok(None);
        };
        Ok(Some((df, limit)))
    }

    /// Discover trace attribute tag names in a window, grouped by scope
    /// (resource, span, intrinsic). Mirrors `LogsService::get_labels`: a
    /// bounded sample of the window's `resource_attributes` /
    /// `span_attributes` documents, unioned with the dedicated-column and
    /// intrinsic tags that are always queryable. `params.scope` narrows the
    /// scan to just the requested group; omitted, all three are populated.
    ///
    /// A dataset with no `traces` table returns only the intrinsics — there
    /// is nothing to discover, not an error.
    pub async fn get_tags(
        &self,
        params: &TraceTagsParams,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<TraceTagNames, QuerierError> {
        let (want_resource, want_span, want_intrinsic) = match params.scope {
            None => (true, true, true),
            Some(tempo_api::TagScope::Resource) => (true, false, false),
            Some(tempo_api::TagScope::Span) => (false, true, false),
            Some(tempo_api::TagScope::Intrinsic) => (false, false, true),
        };

        let mut names = TraceTagNames::default();
        if want_intrinsic {
            names.intrinsic = INTRINSIC_TAGS.iter().map(|s| s.to_string()).collect();
        }

        let Some(df) =
            optional_table(&self.session_context, tenant_slug, dataset_slug, "traces").await?
        else {
            return Ok(names);
        };

        if !(want_resource || want_span) {
            return Ok(names);
        }

        let mut cols: Vec<&str> = Vec::new();
        if want_resource {
            cols.push("resource_attributes");
        }
        if want_span {
            cols.push("span_attributes");
        }

        let scan = time_window(df, params.start, params.end)?
            .select_columns(&cols)
            .map_err(QuerierError::QueryFailed)?;
        // Arrow's row format cannot sort Map columns, so the JSON-era
        // `distinct()` dedup is skipped for map-typed attribute tables.
        let map_typed = cols.iter().any(|c| is_map_column(&scan, c));
        let scan = if map_typed {
            scan
        } else {
            scan.distinct().map_err(QuerierError::QueryFailed)?
        };
        let batches = scan
            .limit(0, Some(TAG_SCAN_LIMIT))
            .map_err(QuerierError::QueryFailed)?
            .collect()
            .await
            .map_err(QuerierError::QueryFailed)?;
        let has_rows = batches.iter().any(|b| b.num_rows() > 0);

        let mut resource_keys: BTreeSet<String> = BTreeSet::new();
        let mut span_keys: BTreeSet<String> = BTreeSet::new();
        if want_resource && has_rows {
            // `service.name` is a dedicated column, but it is the OTel
            // resource attribute of the same name, so it belongs in the
            // resource scope alongside the ones read from the map.
            resource_keys.insert("service.name".to_string());
        }
        for batch in &batches {
            if want_resource {
                for doc in super::logs::attr_documents(batch, "resource_attributes")?
                    .into_iter()
                    .flatten()
                {
                    resource_keys.extend(doc.into_keys());
                }
            }
            if want_span {
                for doc in super::logs::attr_documents(batch, "span_attributes")?
                    .into_iter()
                    .flatten()
                {
                    span_keys.extend(doc.into_keys());
                }
            }
        }

        if want_resource {
            names.resource = resource_keys.into_iter().collect();
        }
        if want_span {
            names.span = span_keys.into_iter().collect();
        }

        Ok(names)
    }

    /// List the distinct values of one trace tag in a window. `tag` is the
    /// unscoped attribute name (callers strip any `resource.`/`span.`/`.`
    /// scope prefix before calling, since the same key resolves the same
    /// way regardless of scope). The intrinsics `status` and `kind` return
    /// their fixed enumeration; `rootServiceName`/`rootName` resolve to the
    /// dedicated column restricted to root spans; an unknown or unobserved
    /// tag returns an empty list, never an error.
    pub async fn get_tag_values(
        &self,
        tag: &str,
        start: i64,
        end: i64,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<String>, QuerierError> {
        if tag.is_empty() {
            return Err(QuerierError::InvalidInput(
                "tag name must not be empty".to_string(),
            ));
        }

        match tag {
            "status" => return Ok(STATUS_VALUES.iter().map(|s| s.to_string()).collect()),
            "kind" => return Ok(KIND_VALUES.iter().map(|s| s.to_string()).collect()),
            _ => {}
        }

        let Some(df) =
            optional_table(&self.session_context, tenant_slug, dataset_slug, "traces").await?
        else {
            return Ok(Vec::new());
        };
        let df = time_window(df, start, end)?;

        if let Some((column, root_only)) = dedicated_tag_column(tag) {
            let df = if root_only {
                df.filter(col("is_root").eq(lit(true)))
                    .map_err(QuerierError::QueryFailed)?
            } else {
                df
            };
            let batches = df
                .select_columns(&[column])
                .map_err(QuerierError::QueryFailed)?
                .distinct()
                .map_err(QuerierError::QueryFailed)?
                .limit(0, Some(TAG_SCAN_LIMIT))
                .map_err(QuerierError::QueryFailed)?
                .collect()
                .await
                .map_err(QuerierError::QueryFailed)?;
            return distinct_non_empty(&batches, column);
        }

        // Otherwise pull the value out of the resource/span attribute
        // documents — covers map-stored attributes and unknown tags alike
        // (an unknown key simply is never present, so the result is empty).
        let cols = ["resource_attributes", "span_attributes"];
        let scan = df
            .select_columns(&cols)
            .map_err(QuerierError::QueryFailed)?;
        let map_typed = cols.iter().any(|c| is_map_column(&scan, c));
        let scan = if map_typed {
            scan
        } else {
            scan.distinct().map_err(QuerierError::QueryFailed)?
        };
        let batches = scan
            .limit(0, Some(TAG_SCAN_LIMIT))
            .map_err(QuerierError::QueryFailed)?
            .collect()
            .await
            .map_err(QuerierError::QueryFailed)?;

        let mut values = BTreeSet::new();
        for batch in &batches {
            for column in cols {
                for mut doc in super::logs::attr_documents(batch, column)?
                    .into_iter()
                    .flatten()
                {
                    if let Some(value) = doc.remove(tag) {
                        values.insert(value);
                    }
                }
            }
        }
        Ok(values.into_iter().collect())
    }
}

/// Map an unscoped intrinsic/dedicated tag name to the traces column that
/// backs it, and whether the scan must be restricted to root spans
/// (`rootServiceName`/`rootName` describe the trace's root, not every span).
fn dedicated_tag_column(tag: &str) -> Option<(&'static str, bool)> {
    match tag {
        "service.name" => Some(("service_name", false)),
        "name" => Some(("span_name", false)),
        "rootServiceName" => Some(("service_name", true)),
        "rootName" => Some(("span_name", true)),
        _ => None,
    }
}

/// Whether `column` is a typed `Map` column in `df`'s schema — Arrow's row
/// format cannot sort Map columns, so callers must skip `distinct()` on
/// them (see [`TraceService::get_tags`]).
fn is_map_column(df: &DataFrame, column: &str) -> bool {
    df.schema()
        .fields()
        .iter()
        .any(|f| f.name() == column && matches!(f.data_type(), DataType::Map(_, _)))
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
///
/// Test-only since §5 of `ir-single-lowering`: production code names the
/// projection through [`TRACE_SEARCH_IR_FIELDS`] (the document's `fields`)
/// rather than selecting these physical columns directly — this list now
/// exists solely as the expected physical shape the tests below assert
/// `TRACE_SEARCH_IR_FIELDS` resolves to.
#[cfg(test)]
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

/// The IR document `fields` used by [`TraceService::build_search_dataframe`]:
/// the logical name that resolves to each of [`TRACE_SEARCH_COLUMNS`], in the
/// same order — a `fields` entry that resolves to a `Column` projects the
/// physical name unchanged (`ir_planner::Lowering::apply_projection`), so the
/// resulting DataFrame's column names and order match `TRACE_SEARCH_COLUMNS`
/// exactly, and the assembly in `find_traces_with_tenant` needs no branch on
/// which path built the DataFrame.
const TRACE_SEARCH_IR_FIELDS: [&str; 12] = [
    "trace_id",
    "span_id",
    "parent_span_id",
    "span.attributes",
    "resource.attributes",
    "status.code",
    "is_root",
    "span.name",
    "service.name",
    "span_kind",
    "start_time_unix_nano",
    "duration",
];

/// The absolute-nanosecond bounds an unbounded search (no `start`/`end`)
/// plans over, one step in from `i64::MIN`/`i64::MAX` — see
/// [`TraceService::build_search_dataframe`]'s doc comment for why the
/// exact extremes are unusable.
const UNBOUNDED_SEARCH_START_NS: i64 = i64::MIN + 1;
const UNBOUNDED_SEARCH_END_NS: i64 = i64::MAX - 1;

/// Record query demand (epic #737, #733) for the attribute key an
/// attribute-selector condition carries — a materialization candidate.
/// Shared by [`TraceService::build_search_dataframe`]'s `q` and
/// `tags` branches, matching the old path's combined demand-recording loop
/// in [`TraceService::build_search_dataframe`].
fn record_attribute_demand(tenant_slug: &str, dataset_slug: &str, condition: &traceql::Condition) {
    if let traceql::Selector::SpanAttribute(key)
    | traceql::Selector::ResourceAttribute(key)
    | traceql::Selector::AnyAttribute(key) = &condition.selector
    {
        common::attr_demand::record(tenant_slug, dataset_slug, "traces", key);
    }
}

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

/// Fetch a required `Boolean` column by name; mirrors `table_lookup::string_column`
/// (imported above as `string_column`) for the `Utf8` case — resolving a
/// column once per batch, outside the row loop, avoids re-running
/// `column_by_name` + `downcast_ref` on every row.
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

/// Fetch a required `Int64` column by name; see [`required_bool_column`].
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

    /// Like [`search_session`], but `span_attributes` carries an
    /// `http.method` value on every span and a `label_http_method` column
    /// mirrors the compactor's promotion of that same key. Used to prove a
    /// TraceQL search whose result *depends on* attribute promotion still
    /// filters correctly (task 3.1 of `ir-single-lowering`, D10's regression
    /// net for task 3.3).
    fn search_session_with_promoted_attribute() -> SessionContext {
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

        fn attr_maps(pairs: &[&[(&str, &str)]]) -> ArrayRef {
            let names = MapFieldNames {
                entry: "entries".to_string(),
                key: "keys".to_string(),
                value: "values".to_string(),
            };
            let mut b = MapBuilder::new(Some(names), StringBuilder::new(), StringBuilder::new());
            for row in pairs {
                for (k, v) in *row {
                    b.keys().append_value(k);
                    b.values().append_value(v);
                }
                b.append(true).unwrap();
            }
            Arc::new(b.finish())
        }

        let mut fields = vec![
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
        ];
        // The promoted column: keyed off the bare attribute key, never the
        // TraceQL-scoped spelling — see `attr_promotion::materialized_keys_of`.
        fields.push(Field::new("label_http_method", DataType::Utf8, true));
        let schema = Arc::new(Schema::new(fields));

        // Two single-span traces: one GET, one POST.
        let starts: Vec<i64> = vec![1_000_000_000, 2_000_000_000];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["t-get", "t-post"])),
                Arc::new(StringArray::from(vec!["s1", "s2"])),
                Arc::new(StringArray::from(vec![Some(""), Some("")])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(StringArray::from(vec!["api", "api"])),
                Arc::new(StringArray::from(vec!["Server", "Server"])),
                Arc::new(StringArray::from(vec![Some("Ok"), Some("Ok")])),
                Arc::new(BooleanArray::from(vec![true, true])),
                Arc::new(Int64Array::from(starts.clone())),
                Arc::new(Int64Array::from(vec![100_i64, 100])),
                Arc::new(TimestampNanosecondArray::from(starts)),
                attr_maps(&[&[("http.method", "GET")], &[("http.method", "POST")]]),
                attr_maps(&[&[], &[]]),
                Arc::new(StringArray::from(vec![Option::<&str>::None; 2])),
                Arc::new(StringArray::from(vec![Option::<&str>::None; 2])),
                Arc::new(StringArray::from(vec![Option::<&str>::None; 2])),
                Arc::new(StringArray::from(vec![Some("GET"), Some("POST")])),
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

    /// A search whose *result* depends on attribute promotion (only the GET
    /// span should match): a scope-qualified attribute (`span.http.method`)
    /// must resolve to the promoted `label_http_method` column (D10, fixed
    /// in task 3.0 of `ir-single-lowering`) rather than the map-extraction
    /// path.
    #[tokio::test]
    async fn search_filters_on_a_promoted_attribute() {
        let service = TraceService::new(
            search_session_with_promoted_attribute(),
            "traces".to_string(),
        );
        let query = SearchQueryParams {
            q: Some(r#"{ span.http.method = "GET" }"#.to_string()),
            ..search_params()
        };
        let traces = service
            .find_traces_with_tenant(query, "t", "d")
            .await
            .unwrap();
        let ids: Vec<&str> = traces.iter().map(|t| t.trace_id.as_str()).collect();
        assert_eq!(ids, vec!["t-get"]);
    }

    /// `tags` alone: the same bare (unscoped) attribute key resolves to the
    /// promoted column through the [`super::tags_to_ir`] shim (task 3.2).
    #[tokio::test]
    async fn search_filters_on_a_promoted_attribute_via_tags() {
        let service = TraceService::new(
            search_session_with_promoted_attribute(),
            "traces".to_string(),
        );
        let query = SearchQueryParams {
            tags: Some("http.method=GET".to_string()),
            ..search_params()
        };
        let ids: Vec<String> = service
            .find_traces_with_tenant(query, "t", "d")
            .await
            .unwrap()
            .into_iter()
            .map(|t| t.trace_id)
            .collect();
        assert_eq!(ids, vec!["t-get"]);
    }

    /// `q` and `tags` together become one conjoined IR document (task 3.3);
    /// no existing test covered the combination before this one (task 3.4).
    /// `q` narrows to `service.name = "api"` (both spans match); `tags`
    /// narrows further to `http.method = "POST"` (only `t-post`), so the
    /// combination is the only way to reach a single-trace result.
    #[tokio::test]
    async fn search_filters_on_q_and_tags_together() {
        let service = TraceService::new(
            search_session_with_promoted_attribute(),
            "traces".to_string(),
        );
        let query = SearchQueryParams {
            q: Some(r#"{ resource.service.name = "api" }"#.to_string()),
            tags: Some("http.method=POST".to_string()),
            ..search_params()
        };
        let ids: Vec<String> = service
            .find_traces_with_tenant(query, "t", "d")
            .await
            .unwrap()
            .into_iter()
            .map(|t| t.trace_id)
            .collect();
        assert_eq!(ids, vec!["t-post"]);
    }

    /// Neither `q` nor `tags`: no filter stage at all.
    #[tokio::test]
    async fn search_without_q_or_tags_returns_every_matching_trace() {
        let service = TraceService::new(
            search_session_with_promoted_attribute(),
            "traces".to_string(),
        );
        let ids: Vec<String> = service
            .find_traces_with_tenant(search_params(), "t", "d")
            .await
            .unwrap()
            .into_iter()
            .map(|t| t.trace_id)
            .collect();
        assert_eq!(ids.len(), 2);
    }

    /// A traces table with one span far in the past (a negative
    /// `start_time_unix_nano`, same shape as the existing `traces_ctx`
    /// fixture in `ir_planner.rs`) and one far in the future, for the
    /// unbounded-search time-range warning (task 3.3's doc comment on
    /// `build_search_dataframe`).
    fn search_session_with_extreme_timestamps() -> SessionContext {
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

        // Extreme, but safely inside `UNBOUNDED_SEARCH_START_NS` /
        // `UNBOUNDED_SEARCH_END_NS` — the point of this fixture is a span an
        // unbounded search must still keep, not the exact boundary.
        let starts: Vec<i64> = vec![i64::MIN + 1_000_000, i64::MAX - 1_000_000];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["t-past", "t-future"])),
                Arc::new(StringArray::from(vec!["s1", "s2"])),
                Arc::new(StringArray::from(vec![Some(""), Some("")])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(StringArray::from(vec!["api", "api"])),
                Arc::new(StringArray::from(vec!["Server", "Server"])),
                Arc::new(StringArray::from(vec![Some("Ok"), Some("Ok")])),
                Arc::new(BooleanArray::from(vec![true, true])),
                Arc::new(Int64Array::from(starts.clone())),
                Arc::new(Int64Array::from(vec![100_i64, 100])),
                Arc::new(TimestampNanosecondArray::from(starts)),
                empty_maps(2),
                empty_maps(2),
                Arc::new(StringArray::from(vec![Option::<&str>::None; 2])),
                Arc::new(StringArray::from(vec![Option::<&str>::None; 2])),
                Arc::new(StringArray::from(vec![Option::<&str>::None; 2])),
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

    /// The time-range warning in `build_search_dataframe`'s doc comment: an
    /// unbounded search (no `start`/`end`) must exclude nothing and must not
    /// overflow converting the range to a document. A far-past (negative)
    /// and a far-future `start_time_unix_nano` both survive, since no time
    /// filter at all applies when neither bound is given.
    #[tokio::test]
    async fn unbounded_search_keeps_far_past_and_far_future_spans() {
        let service = TraceService::new(
            search_session_with_extreme_timestamps(),
            "traces".to_string(),
        );
        let mut ids: Vec<String> = service
            .find_traces_with_tenant(search_params(), "t", "d")
            .await
            .unwrap()
            .into_iter()
            .map(|t| t.trace_id)
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["t-future", "t-past"]);
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

    // ---- Tag discovery (#1073) ----

    /// Register a `t.d.traces` table with map-typed attribute columns and
    /// three spans: one outside the `[1_000, 3_000]` test window carrying
    /// attribute keys unique to it (to prove window exclusion), and two
    /// inside it with distinct resource/span attribute keys and values, one
    /// root and one not (to prove `is_root`-filtered intrinsics).
    fn tags_session() -> SessionContext {
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

        fn maps(rows: &[&[(&str, &str)]]) -> ArrayRef {
            let names = MapFieldNames {
                entry: "entries".to_string(),
                key: "keys".to_string(),
                value: "values".to_string(),
            };
            let mut b = MapBuilder::new(Some(names), StringBuilder::new(), StringBuilder::new());
            for row in rows {
                for (k, v) in *row {
                    b.keys().append_value(k);
                    b.values().append_value(v);
                }
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
        ]));

        let starts: Vec<i64> = vec![100, 1_000, 2_000];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["t-old", "t-mid", "t-new"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["s0", "s1", "s2"])),
                Arc::new(StringArray::from(vec![Some(""), Some(""), Some("")])),
                Arc::new(StringArray::from(vec![
                    "LEGACY",
                    "GET /orders",
                    "ProcessQueue",
                ])),
                Arc::new(StringArray::from(vec![
                    "legacy-svc",
                    "checkout",
                    "checkout-worker",
                ])),
                Arc::new(StringArray::from(vec!["Internal", "Server", "Internal"])),
                Arc::new(StringArray::from(vec![
                    Some("Ok"),
                    Some("Ok"),
                    Some("Error"),
                ])),
                Arc::new(BooleanArray::from(vec![true, true, false])),
                Arc::new(Int64Array::from(starts.clone())),
                Arc::new(Int64Array::from(vec![100_i64, 200, 300])),
                Arc::new(TimestampNanosecondArray::from(starts)),
                maps(&[
                    &[("legacy.route", "/old")],
                    &[("http.route", "/api/orders")],
                    &[("http.route", "/api/users")],
                ]),
                maps(&[
                    &[("legacy.only", "x")],
                    &[("deployment.environment.name", "prod")],
                    &[("deployment.environment.name", "staging")],
                ]),
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

    fn tags_service() -> TraceService {
        TraceService::new(tags_session(), "traces".to_string())
    }

    fn empty_tags_service() -> TraceService {
        // No `traces` table registered at all: `optional_table` sees `None`.
        use datafusion::catalog::CatalogProvider;

        let ctx = SessionContext::new();
        let cat = Arc::new(datafusion::catalog::memory::MemoryCatalogProvider::new());
        cat.register_schema(
            "d",
            Arc::new(datafusion::catalog::memory::MemorySchemaProvider::new()),
        )
        .unwrap();
        ctx.register_catalog("t", cat);
        TraceService::new(ctx, "traces".to_string())
    }

    fn tags_params(start: i64, end: i64, scope: Option<tempo_api::TagScope>) -> TraceTagsParams {
        TraceTagsParams { start, end, scope }
    }

    #[tokio::test]
    async fn get_tags_returns_resource_span_and_intrinsic_names() {
        let service = tags_service();
        let tags = service
            .get_tags(&tags_params(0, 3_000, None), "t", "d")
            .await
            .unwrap();
        assert!(tags.resource.contains(&"service.name".to_string()));
        assert!(
            tags.resource
                .contains(&"deployment.environment.name".to_string())
        );
        assert!(tags.span.contains(&"http.route".to_string()));
        assert_eq!(
            tags.intrinsic,
            INTRINSIC_TAGS
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn get_tags_window_excludes_older_spans() {
        let service = tags_service();
        let tags = service
            .get_tags(&tags_params(1_000, 3_000, None), "t", "d")
            .await
            .unwrap();
        assert!(!tags.resource.contains(&"legacy.only".to_string()));
        assert!(!tags.span.contains(&"legacy.route".to_string()));
        assert!(
            tags.resource
                .contains(&"deployment.environment.name".to_string())
        );
        assert!(tags.span.contains(&"http.route".to_string()));
    }

    #[tokio::test]
    async fn get_tags_scope_filter_narrows_v2_response() {
        let service = tags_service();
        let tags = service
            .get_tags(
                &tags_params(0, 3_000, Some(tempo_api::TagScope::Span)),
                "t",
                "d",
            )
            .await
            .unwrap();
        assert!(tags.resource.is_empty());
        assert!(tags.intrinsic.is_empty());
        assert!(tags.span.contains(&"http.route".to_string()));
    }

    #[tokio::test]
    async fn get_tags_on_absent_table_is_intrinsics_only() {
        let service = empty_tags_service();
        let tags = service
            .get_tags(&tags_params(0, i64::MAX, None), "t", "d")
            .await
            .unwrap();
        assert!(tags.resource.is_empty());
        assert!(tags.span.is_empty());
        assert_eq!(
            tags.intrinsic,
            INTRINSIC_TAGS
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn get_tag_values_for_a_map_attribute() {
        let service = tags_service();
        let values = service
            .get_tag_values("http.route", 0, 3_000, "t", "d")
            .await
            .unwrap();
        // Row 0 (t=100) carries `legacy.route`, not `http.route` — only the
        // two in-window rows that actually have the key contribute a value.
        assert_eq!(
            values,
            vec!["/api/orders".to_string(), "/api/users".to_string()]
        );
    }

    #[tokio::test]
    async fn get_tag_values_for_a_dedicated_column() {
        let service = tags_service();
        let values = service
            .get_tag_values("service.name", 0, 3_000, "t", "d")
            .await
            .unwrap();
        assert_eq!(
            values,
            vec![
                "checkout".to_string(),
                "checkout-worker".to_string(),
                "legacy-svc".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn get_tag_values_root_intrinsics_are_filtered_by_is_root() {
        let service = tags_service();
        let root_services = service
            .get_tag_values("rootServiceName", 0, 3_000, "t", "d")
            .await
            .unwrap();
        // `checkout-worker` (span 2) is not a root span, so it is excluded
        // even though it is a valid `service.name` value.
        assert_eq!(
            root_services,
            vec!["checkout".to_string(), "legacy-svc".to_string()]
        );

        let root_names = service
            .get_tag_values("rootName", 0, 3_000, "t", "d")
            .await
            .unwrap();
        assert_eq!(
            root_names,
            vec!["GET /orders".to_string(), "LEGACY".to_string()]
        );
    }

    #[tokio::test]
    async fn get_tag_values_intrinsic_enums_are_static() {
        let service = tags_service();
        assert_eq!(
            service
                .get_tag_values("status", 0, 3_000, "t", "d")
                .await
                .unwrap(),
            STATUS_VALUES
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            service
                .get_tag_values("kind", 0, 3_000, "t", "d")
                .await
                .unwrap(),
            KIND_VALUES
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn get_tag_values_unknown_tag_is_empty_not_an_error() {
        let service = tags_service();
        let values = service
            .get_tag_values("no.such.attribute", 0, 3_000, "t", "d")
            .await
            .unwrap();
        assert!(values.is_empty());
    }

    #[tokio::test]
    async fn get_tag_values_on_absent_table_is_empty() {
        let service = empty_tags_service();
        let values = service
            .get_tag_values("http.route", 0, i64::MAX, "t", "d")
            .await
            .unwrap();
        assert!(values.is_empty());
    }

    #[tokio::test]
    async fn get_tag_values_rejects_empty_tag_name() {
        let service = tags_service();
        assert!(matches!(
            service.get_tag_values("", 0, 3_000, "t", "d").await,
            Err(QuerierError::InvalidInput(_))
        ));
    }
}
