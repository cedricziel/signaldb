//! # Native Query IR HTTP API
//!
//! `POST /api/v1/query` — the first-party, structured query surface. A client
//! posts a versioned IR document (see the `query-ir-core` capability); the
//! router stamps the server clock, forwards it to a querier as a
//! `query_ir:{tenant}:{dataset}:{json}` Flight ticket, and shapes the returned
//! RecordBatches into the declared result envelope
//! (`rows` | `series` | `table` | `heatmap` | `flamegraph`).
//!
//! Auth and tenant scoping are identical to the Tempo/LogQL/Prometheus
//! surfaces: the endpoint sits behind the auth middleware and derives the
//! tenant/dataset from the authenticated request context, never from the
//! document body.

use std::collections::{BTreeMap, HashMap};

use tracing::Instrument;

use arrow_flight::Ticket;
use axum::{
    Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use common::auth::{TenantContext, TenantContextExtractor};
use common::flight::transport::ServiceCapability;
use common::query_ir::{Literal, ValueType, coerce};
use common::schema::typed_attributes::{IR_TYPE_METADATA_KEY, RAW_ATTRIBUTE_BAG_IR_TYPE};
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, MapArray, RecordBatch,
    StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::api_error::ApiError;
use crate::RouterAppState;

pub fn router() -> Router<RouterAppState> {
    Router::new()
        .route("/query", post(query_ir))
        .route("/query/sources", get(super::discovery::query_sources))
}

/// The query time range. `from`/`to` are timestamp literal **strings**: RFC3339,
/// a relative anchor (`now-1h`), or a nanosecond integer as a numeric string
/// (`"1700000000000000000"`). Kept a `String` so the emitted schema and the
/// generated clients match exactly what the endpoint accepts.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct QueryRange {
    #[schema(example = "now-1h")]
    pub from: String,
    #[schema(example = "now")]
    pub to: String,
}

/// A versioned Query IR request document.
///
/// The `pipeline` stages are opaque JSON objects at the HTTP boundary — the
/// querier validates and lowers them per the versioned IR contract. See the
/// `query-ir-core` capability for the full stage/predicate grammar.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct QueryIrRequest {
    /// IR document version (the server accepts a bounded range).
    #[serde(rename = "irVersion")]
    pub ir_version: i64,
    /// The registered signal source: `logs`, `traces`, or profile-summary `profiles`.
    #[schema(example = "logs")]
    pub from: String,
    pub range: QueryRange,
    /// Declared result envelope: `rows`, `series`, `table`, `heatmap`,
    /// (for the `profiles` source only) `flamegraph`, or (for the `traces`
    /// source, irVersion 8+) `graph`.
    #[schema(example = "rows")]
    pub result: String,
    /// Curated projection (logical field names) for `rows`/`table`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
    /// Ordered transform stages (opaque objects; see the IR spec).
    #[serde(default)]
    #[schema(value_type = Vec<Object>)]
    pub pipeline: Vec<serde_json::Value>,
    /// `graph` only: restrict to this service's neighbourhood.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub focus: Option<String>,
    /// `graph` only: hops from `focus` (1-3, default 1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth: Option<i64>,
    /// `graph` only: restrict to the services and calls of one trace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
}

/// One named formula in a [`MultiQueryIrRequest`] (D5): arithmetic
/// (`+ - * /`, numeric constants, parentheses) over the request's own query
/// names, e.g. `"errors / total"`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct QueryFormula {
    /// The formula's identity: tags each output series' `labels` under the
    /// `formula` key, so a request with several formulas stays distinguishable.
    pub name: String,
    #[schema(example = "errors / total")]
    pub expr: String,
}

/// A multi-query document (D5): several named IR queries — each required to
/// declare `result: "series"` — plus formulas evaluated over their results
/// after every inner query has run. Series join on an identical label set
/// and timestamp; a formula input missing a series present in another
/// contributes nothing to the join, and a zero divisor drops the point,
/// rather than either erroring.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MultiQueryIrRequest {
    pub queries: BTreeMap<String, QueryIrRequest>,
    pub formulas: Vec<QueryFormula>,
    /// Always `"series"` — a formula document has no other shape.
    #[schema(example = "series")]
    pub result: String,
}

/// The `POST /api/v1/query` request body: either a single IR document or a
/// [`MultiQueryIrRequest`], discriminated by the presence of `queries` — a
/// document without it is a single [`QueryIrRequest`], so an ordinary
/// request needs no wrapper key.
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum QueryIrRequestBody {
    Multi(MultiQueryIrRequest),
    Single(QueryIrRequest),
}

impl QueryIrResponse {
    /// The `metadata` envelope: an answer about the source rather than its
    /// records. Every record-shaped field stays empty.
    pub(super) fn metadata(
        window: ResolvedWindow,
        metadata: common::discovery::MetadataResult,
        warnings: Vec<QueryWarning>,
    ) -> Self {
        QueryIrResponse {
            result: common::query_ir::ResultEnvelope::Metadata
                .as_str()
                .to_string(),
            window,
            columns: Vec::new(),
            rows: Vec::new(),
            series: Vec::new(),
            step_ns: None,
            heatmap: HeatmapResult::default(),
            flamegraph: None,
            graph: None,
            metadata: Some(metadata),
            warnings,
        }
    }
}

/// The resolved absolute time window, echoed for reproducibility/replay.
#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
pub struct ResolvedWindow {
    pub start_ns: i64,
    pub end_ns: i64,
}

/// A named, typed result column.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ResultColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub value_type: String,
}

/// One time series in a `series` result.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ResultSeries {
    /// The grouping label set.
    pub labels: BTreeMap<String, String>,
    /// `[t_ns, value]` points.
    #[schema(value_type = Vec<Vec<serde_json::Value>>)]
    pub points: Vec<[serde_json::Value; 2]>,
}

/// Epoch-aligned time axis with a fixed nanosecond step.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct HeatmapAxisX {
    pub step_ns: i64,
    pub align: String,
}
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct HeatmapAxisY {
    pub of: String,
    #[serde(rename = "type")]
    pub value_type: String,
    pub bounds: Vec<i64>,
    pub overflow: bool,
}
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct HeatmapCell {
    pub time_bucket_ns: i64,
    pub duration_bucket: i64,
    pub count: i64,
}
/// Complete axes plus one non-zero heatmap cell. Missing declared coordinates
/// represent zero, making the Flight payload sparse without hiding the window.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct HeatmapResult {
    pub x: HeatmapAxisX,
    pub y: HeatmapAxisY,
    pub value: String,
    pub cells: Vec<HeatmapCell>,
}

impl Default for HeatmapResult {
    fn default() -> Self {
        Self {
            x: HeatmapAxisX {
                step_ns: 0,
                align: String::new(),
            },
            y: HeatmapAxisY {
                of: String::new(),
                value_type: String::new(),
                bounds: Vec::new(),
                overflow: false,
            },
            value: String::new(),
            cells: Vec::new(),
        }
    }
}

impl HeatmapResult {
    fn is_empty(&self) -> bool {
        self.x.step_ns == 0
    }
}

/// A flamegraph in Pyroscope flamebearer encoding — the same shape and
/// aggregation `/pyroscope/render` returns, reused here so the native Query
/// IR surface can retrieve an actual profile payload (bounded, aggregated)
/// rather than raw `samples_json`/`stacktraces_json`. See `query-ir-core`'s
/// "Profile flamegraph retrieval" requirement.
#[derive(Debug, Clone, Default, Serialize, ToSchema)]
pub struct FlamegraphResult {
    /// Function name table referenced by the blocks' name indices.
    pub names: Vec<String>,
    /// One entry per depth level; each level is a flat sequence of
    /// `[offset_delta, total, self, name_index]` quadruples.
    #[schema(value_type = Vec<Vec<i64>>)]
    pub levels: Vec<Vec<i64>>,
    /// Total value of the root (sum of all samples).
    pub total: i64,
    /// Largest self value of any block, used for color scaling.
    pub max_self: i64,
    /// `true` when more than `FLAMEGRAPH_PROFILE_CAP` (1,000) profile rows
    /// matched — a row-count cap, not a byte-size one — and the flamegraph
    /// was aggregated over only the first 1,000 of them.
    pub truncated: bool,
    /// Source location for each entry in `names`, aligned by index; `None`
    /// (or the array is shorter than `names`) where unknown. See
    /// `common::profile::Flamegraph::locations`.
    pub locations: Vec<Option<common::profile::FrameLocation>>,
}

/// A non-fatal diagnostic about a query that still produced a result. A
/// warning never changes the result: it explains something the caller
/// probably did not intend, so a client can surface it next to the data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct QueryWarning {
    /// Stable machine-readable identifier — clients branch on this, not on
    /// `message`. Today `unknown_group_by_field` and
    /// `no_attribute_statistics`.
    #[schema(example = "unknown_group_by_field")]
    pub code: String,
    /// Human-readable explanation, safe to show verbatim.
    pub message: String,
    /// The document field the warning is about, when it names one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    /// Field names close to `field` that the source does declare, best first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub suggestions: Vec<String>,
}

/// The single canonical response contract. `result` discriminates which fields
/// are populated: `rows`/`table` fill `columns` + `rows`; `series` fills
/// `series` + `step_ns`; `heatmap` fills `heatmap`; `flamegraph` fills
/// `flamegraph`; `graph` fills `graph`.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct QueryIrResponse {
    /// The result envelope: `rows`, `series`, `table`, `heatmap`, `flamegraph`, or `graph`.
    pub result: String,
    /// The resolved absolute window the query ran over.
    pub window: ResolvedWindow,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<ResultColumn>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schema(value_type = Vec<Vec<serde_json::Value>>)]
    pub rows: Vec<Vec<serde_json::Value>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub series: Vec<ResultSeries>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_ns: Option<i64>,
    #[serde(default, skip_serializing_if = "HeatmapResult::is_empty")]
    pub heatmap: HeatmapResult,
    /// Present iff `result == "flamegraph"` — `Some` even when zero profiles
    /// matched, so an empty match set stays distinguishable from "this
    /// response has no flamegraph at all" (i.e. a different envelope).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flamegraph: Option<FlamegraphResult>,
    /// Present iff `result == "graph"` — the service dependency graph.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph: Option<common::service_graph::ServiceGraph>,
    /// Present iff `result == "metadata"` — what a `describe` document asked
    /// about, with the provenance and cost of the answer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<common::discovery::MetadataResult>,
    /// Non-fatal diagnostics about this query. Empty (and omitted) when the
    /// server has nothing to report; a warning never suppresses the result.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<QueryWarning>,
}

/// Submit a native Query IR document — either a single query or a
/// multi-query formula document (D5, [`MultiQueryIrRequest`]), discriminated
/// by the presence of `queries`.
#[utoipa::path(
    post,
    path = "/api/v1/query",
    tag = "query",
    security(("bearerAuth" = [])),
    request_body = QueryIrRequestBody,
    responses(
        (status = 200, description = "The enveloped query result", body = QueryIrResponse),
        (status = 400, description = "Invalid IR document", body = crate::endpoints::api_error::ApiErrorBody),
        (status = 401, description = "Missing or invalid credentials", body = crate::endpoints::api_error::ApiErrorBody),
        (status = 403, description = "Missing read scope for a queried source", body = crate::endpoints::api_error::ApiErrorBody),
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 503, description = "No querier service available", body = crate::endpoints::api_error::ApiErrorBody),
    )
)]
pub async fn query_ir(
    state: State<RouterAppState>,
    tenant_ctx: TenantContextExtractor,
    axum::Json(body): axum::Json<QueryIrRequestBody>,
) -> Result<axum::Json<QueryIrResponse>, ApiError> {
    match body {
        QueryIrRequestBody::Multi(req) => query_ir_multi(state, tenant_ctx, req).await,
        QueryIrRequestBody::Single(req) => query_ir_single(state, tenant_ctx, req).await,
    }
}

#[tracing::instrument(skip(state, tenant_ctx, req), fields(
    signaldb.tenant.id = %tenant_ctx.0.tenant_id,
    signaldb.dataset.id = %tenant_ctx.0.dataset_id,
    source = %req.from,
    result = %req.result,
))]
async fn query_ir_single(
    State(state): State<RouterAppState>,
    tenant_ctx: TenantContextExtractor,
    req: QueryIrRequest,
) -> Result<axum::Json<QueryIrResponse>, ApiError> {
    let ctx = &tenant_ctx.0;

    // Query IR covers several signal tables, so its authorization must be
    // selected from the source before the ticket can reach a querier.
    source_read_scope(ctx, &req.from)?;

    // Stamp the server clock once, at the ticket boundary, so relative anchors
    // resolve to a single absolute window every stage of the plan sees.
    let now = super::now_ns();
    let window = resolve_window(&req.range, now)?;

    // The IR document is the request re-serialized; the querier validates it.
    let document = serde_json::to_value(&req)
        .map_err(|e| ApiError::bad_request(format!("invalid IR document: {e}")))?;

    // An introspection document is answered here, from the registry and the
    // catalog. It never becomes a ticket, so discovery does not depend on
    // query execution being available.
    if is_introspection(&req) {
        let doc: common::query_ir::Document = serde_json::from_value(document)
            .map_err(|e| ApiError::bad_request(format!("invalid IR document: {e}")))?;
        let describe =
            common::query_ir::validate_describe(&doc, &common::query_ir::SourceRegistry::core())
                .map_err(|e| ApiError::bad_request(e.to_string()))?;
        return super::discovery::answer_describe(&state, ctx, &doc, describe, window, now)
            .await
            .map(axum::Json);
    }
    let payload = serde_json::json!({ "document": document.clone(), "now_ns": now });
    let payload = serde_json::to_string(&payload)
        .map_err(|e| ApiError::bad_request(format!("invalid IR document: {e}")))?;
    let ticket = format!(
        "query_ir:{}:{}:{}",
        ctx.tenant_slug, ctx.dataset_slug, payload
    );

    let (batches, correlate_truncated) = execute_ticket(&state, ticket).await?;
    let mut response = build_envelope(&req.result, window, &batches, &document)?;
    response
        .warnings
        .extend(unknown_group_by_warnings(&req.from, &document, &batches));
    response
        .warnings
        .extend(correlate_truncation_warning(correlate_truncated));
    Ok(axum::Json(response))
}

/// D5: submit a [`MultiQueryIrRequest`] — several named queries plus
/// formulas over their `series` results. Every inner query's source is
/// authorized before any of them run; each then executes exactly the way a
/// standalone single-query request would (its own Flight ticket), and the
/// formulas are evaluated once every inner query has returned.
#[tracing::instrument(skip(state, tenant_ctx, req), fields(
    signaldb.tenant.id = %tenant_ctx.0.tenant_id,
    signaldb.dataset.id = %tenant_ctx.0.dataset_id,
    query_count = req.queries.len(),
    formula_count = req.formulas.len(),
))]
async fn query_ir_multi(
    State(state): State<RouterAppState>,
    tenant_ctx: TenantContextExtractor,
    req: MultiQueryIrRequest,
) -> Result<axum::Json<QueryIrResponse>, ApiError> {
    let ctx = &tenant_ctx.0;

    // Authorize every inner query's source before any of them run — a
    // partially-authorized multi-query request must fail closed, not spend
    // work on the queries it was allowed to run before rejecting the rest.
    check_multi_source_scopes(ctx, &req.queries)?;

    let multi_doc = to_multi_document(&req)?;
    let inner_envelopes: HashMap<String, common::query_ir::ResultEnvelope> = req
        .queries
        .iter()
        .map(|(name, inner)| Ok((name.clone(), parse_envelope(&inner.result)?)))
        .collect::<Result<_, ApiError>>()?;
    common::query_ir::validate_multi(&multi_doc, &inner_envelopes)
        .map_err(|e| ApiError::bad_request(e.to_string()))?;

    // One clock stamp for every inner query, same as a single request stamps
    // it once at the ticket boundary.
    let now = super::now_ns();
    let mut inputs: HashMap<String, Vec<common::query_ir::EvalSeries>> = HashMap::new();
    let mut window = None;
    for (name, inner) in &req.queries {
        let (inner_window, series) = execute_inner_series_query(&state, ctx, inner, now).await?;
        window.get_or_insert(inner_window);
        inputs.insert(name.clone(), series);
    }
    let window = window.ok_or_else(|| ApiError::bad_request("`queries` must not be empty"))?;

    let mut series = Vec::new();
    for formula in &req.formulas {
        let expr = common::query_ir::parse_formula_expr(&formula.expr)
            .map_err(|e| ApiError::bad_request(format!("formula '{}': {e}", formula.name)))?;
        for out in common::query_ir::evaluate_formula(&expr, &inputs) {
            // Tag the formula's identity onto its output series' labels, so
            // a request with several formulas stays distinguishable.
            let mut labels = out.labels;
            labels.insert("formula".to_string(), formula.name.clone());
            series.push(ResultSeries {
                labels,
                points: out
                    .points
                    .into_iter()
                    .map(|(t, v)| [serde_json::Value::from(t), serde_json::Value::from(v)])
                    .collect(),
            });
        }
    }

    Ok(axum::Json(QueryIrResponse {
        result: common::query_ir::ResultEnvelope::Series
            .as_str()
            .to_string(),
        window,
        columns: Vec::new(),
        rows: Vec::new(),
        series,
        step_ns: None,
        heatmap: HeatmapResult::default(),
        flamegraph: None,
        graph: None,
        metadata: None,
        warnings: Vec::new(),
    }))
}

/// Require the read scope for every inner query's source before any of them
/// run — the same [`source_read_scope`] check a single-query request goes
/// through, applied per named query.
fn check_multi_source_scopes(
    ctx: &TenantContext,
    queries: &BTreeMap<String, QueryIrRequest>,
) -> Result<(), ApiError> {
    for inner in queries.values() {
        source_read_scope(ctx, &inner.from)?;
    }
    Ok(())
}

/// Parse an HTTP result-envelope string into the IR's typed
/// [`common::query_ir::ResultEnvelope`].
fn parse_envelope(s: &str) -> Result<common::query_ir::ResultEnvelope, ApiError> {
    use common::query_ir::ResultEnvelope::*;
    Ok(match s {
        "rows" => Rows,
        "series" => Series,
        "table" => Table,
        "heatmap" => Heatmap,
        "flamegraph" => Flamegraph,
        "metadata" => Metadata,
        "graph" => Graph,
        other => {
            return Err(ApiError::bad_request(format!(
                "unknown result envelope '{other}'"
            )));
        }
    })
}

/// Build the [`common::query_ir::MultiDocument`] a [`MultiQueryIrRequest`]
/// denotes, re-serializing each inner request the same way a single-query
/// request's document is built.
fn to_multi_document(
    req: &MultiQueryIrRequest,
) -> Result<common::query_ir::MultiDocument, ApiError> {
    let mut queries = BTreeMap::new();
    for (name, inner) in &req.queries {
        let value = serde_json::to_value(inner)
            .map_err(|e| ApiError::bad_request(format!("invalid IR document '{name}': {e}")))?;
        let doc: common::query_ir::Document = serde_json::from_value(value)
            .map_err(|e| ApiError::bad_request(format!("invalid IR document '{name}': {e}")))?;
        queries.insert(name.clone(), doc);
    }
    let formulas = req
        .formulas
        .iter()
        .map(|f| common::query_ir::Formula {
            name: f.name.clone(),
            expr: f.expr.clone(),
        })
        .collect();
    let result = parse_envelope(&req.result)?;
    Ok(common::query_ir::MultiDocument {
        queries,
        formulas,
        result,
    })
}

/// Execute one inner query of a multi-query document exactly the way a
/// standalone single-query request would (its own `query_ir:` Flight
/// ticket), decoded straight to [`common::query_ir::EvalSeries`] — the
/// formula evaluator's input shape — rather than the HTTP `ResultSeries`
/// envelope. `validate_multi` already required this query's declared result
/// to be `series`.
async fn execute_inner_series_query(
    state: &RouterAppState,
    ctx: &TenantContext,
    req: &QueryIrRequest,
    now_ns: i64,
) -> Result<(ResolvedWindow, Vec<common::query_ir::EvalSeries>), ApiError> {
    let window = resolve_window(&req.range, now_ns)?;
    let document = serde_json::to_value(req)
        .map_err(|e| ApiError::bad_request(format!("invalid IR document: {e}")))?;
    let payload = serde_json::json!({ "document": document, "now_ns": now_ns });
    let payload = serde_json::to_string(&payload)
        .map_err(|e| ApiError::bad_request(format!("invalid IR document: {e}")))?;
    let ticket = format!(
        "query_ir:{}:{}:{}",
        ctx.tenant_slug, ctx.dataset_slug, payload
    );
    let (batches, _correlate_truncated) = execute_ticket(state, ticket).await?;
    let (series, _step_ns) = to_series(&batches);
    let eval_series = series
        .into_iter()
        .map(|s| common::query_ir::EvalSeries {
            labels: s.labels,
            points: s
                .points
                .into_iter()
                .filter_map(|[t, v]| Some((t.as_i64()?, v.as_f64()?)))
                .collect(),
        })
        .collect();
    Ok((window, eval_series))
}

/// Whether the request asks about the source rather than its records.
/// Deliberately syntactic: the document's own validator decides whether such a
/// request is well formed, and reports why when it is not.
fn is_introspection(req: &QueryIrRequest) -> bool {
    req.result == common::query_ir::ResultEnvelope::Metadata.as_str()
        || req
            .pipeline
            .iter()
            .any(|stage| stage.get("describe").is_some())
}

/// The `code` of the warning raised for a group key that labelled nothing.
const UNKNOWN_GROUP_BY_FIELD: &str = "unknown_group_by_field";

/// Warn about an `aggregate.by` field that put every row in one null group.
///
/// Field resolution is deliberately permissive: unpromoted attributes cannot
/// be enumerated while planning — there is no attribute registry yet
/// (#811/#813) — and bare Prometheus-style label names (`job`, `status`) are
/// legitimate group keys, so a name the source does not declare resolves to
/// an attribute lookup rather than a rejection. A name that is neither a
/// logical field nor carried by any record in the window therefore yields a
/// single null-labelled group instead of an error (#1070). Rejecting it while
/// planning would break a legitimate query over an attribute that is merely
/// absent from a short window (a quiet facet panel, a narrow dashboard
/// refresh), so the result stands and the caller gets this warning next to it.
fn unknown_group_by_warnings(
    source: &str,
    document: &serde_json::Value,
    batches: &[RecordBatch],
) -> Vec<QueryWarning> {
    let Ok(doc) = serde_json::from_value::<common::query_ir::Document>(document.clone()) else {
        return Vec::new();
    };
    // Query-local `extract` outputs are real columns of this document, not
    // fields of the source — an all-null one means the parser matched
    // nothing, which is a different (and expected) story.
    let derived: std::collections::HashSet<&str> = doc
        .pipeline
        .iter()
        .filter_map(|stage| match stage {
            common::query_ir::Stage::Extract(extract) => Some(extract),
            _ => None,
        })
        .flat_map(|extract| extract.as_fields.iter().map(|f| f.name.as_str()))
        .collect();

    let schema = common::schema::logical::LogicalSchema::core();
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut warnings = Vec::new();
    for by in doc
        .pipeline
        .iter()
        .filter_map(|stage| match stage {
            common::query_ir::Stage::Aggregate(agg) => Some(agg),
            _ => None,
        })
        .flat_map(|agg| agg.by.iter())
    {
        if !seen.insert(by.as_str())
            || derived.contains(by.as_str())
            || schema.resolve(source, by).is_some()
        {
            continue;
        }
        if !column_is_all_null(batches, &common::query_ir::safe_ident(by)) {
            continue;
        }
        warnings.push(QueryWarning {
            code: UNKNOWN_GROUP_BY_FIELD.to_string(),
            message: format!(
                "'{by}' is not a logical field of '{source}' and no record in the queried \
                 window carries an attribute named '{by}'; every row was grouped under a \
                 null label"
            ),
            field: Some(by.clone()),
            suggestions: closest_fields(&schema, source, by),
        });
    }
    warnings
}

const CORRELATE_ROW_LIMIT: &str = "correlate_row_limit";

/// A `correlate` stage's row cap (`[querier].correlate_max_rows`) was
/// reached. Ground truth, not a heuristic: the querier's `CorrelateCapExec`
/// operator detects the overflow at the join itself, streaming, before any
/// `aggregate`/`where`/`limit` stage can shrink or hide the row count, and
/// [`execute_ticket`] reads it back from the querier's Flight trailer
/// message (see `common::flight::correlate_truncated_trailer`).
fn correlate_truncation_warning(truncated: bool) -> Option<QueryWarning> {
    if !truncated {
        return None;
    }
    Some(QueryWarning {
        code: CORRELATE_ROW_LIMIT.to_string(),
        message: "a correlate stage's joined row count reached the server limit \
                   ([querier].correlate_max_rows); the result was truncated"
            .to_string(),
        field: None,
        suggestions: Vec::new(),
    })
}

/// Whether `column` exists in every batch and is null on every row of a
/// non-empty result. A result with no rows says nothing about the field.
fn column_is_all_null(batches: &[RecordBatch], column: &str) -> bool {
    let mut rows = 0usize;
    let mut nulls = 0usize;
    for batch in batches {
        let Some(array) = batch.column_by_name(column) else {
            return false;
        };
        rows += batch.num_rows();
        nulls += array.null_count();
    }
    rows > 0 && rows == nulls
}

/// Up to three logical field names of `source` closest to `field`: an exact
/// match once punctuation and case are ignored first (`statusCode` →
/// `status.code`), then near-misses by edit distance.
fn closest_fields(
    schema: &common::schema::logical::LogicalSchema,
    source: &str,
    field: &str,
) -> Vec<String> {
    let normalize = |name: &str| -> String {
        name.chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .map(|c| c.to_ascii_lowercase())
            .collect()
    };
    let target = normalize(field);
    let mut scored: Vec<(usize, String)> = schema
        .fields()
        .filter(|f| f.id.source == source && f.id.name != field)
        .map(|f| f.id.name.clone())
        .map(|name| (edit_distance(&target, &normalize(&name)), name))
        // A distance beyond a third of the name is a different word, not a
        // typo — suggesting it would be noise.
        .filter(|(distance, name)| *distance <= (name.len() / 3).max(2))
        .collect();
    scored.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    scored.dedup_by(|a, b| a.1 == b.1);
    scored.into_iter().take(3).map(|(_, name)| name).collect()
}

/// Levenshtein distance, iterative with a single row of state.
fn edit_distance(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut current = vec![0usize; b.len() + 1];
    for (i, ca) in a.chars().enumerate() {
        current[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let substitution = prev[j] + usize::from(ca != *cb);
            current[j + 1] = substitution.min(prev[j + 1] + 1).min(current[j] + 1);
        }
        std::mem::swap(&mut prev, &mut current);
    }
    prev[b.len()]
}

/// Require the read scope associated with a registered Query IR source.
fn source_read_scope(ctx: &TenantContext, source: &str) -> Result<(), ApiError> {
    let signal = match source {
        "logs" | "traces" | "profiles" | "metrics" => source,
        // metrics_histogram is a distinct IR source (bucketed rows, not a
        // scalar value — see ir_planner.rs) but the same signal for scoping
        // purposes; there is no separate metrics_histogram:read scope.
        "metrics_histogram" => "metrics",
        _ => {
            return Err(ApiError::bad_request(format!(
                "unknown query source '{source}'"
            )));
        }
    };
    if ctx.can_read(signal) {
        Ok(())
    } else {
        Err(ApiError::new(
            StatusCode::FORBIDDEN,
            format!("missing {signal}:read scope"),
        ))
    }
}

/// Resolve a range to an absolute window using the server-stamped clock.
fn resolve_window(range: &QueryRange, now_ns: i64) -> Result<ResolvedWindow, ApiError> {
    let resolve = |s: &str| -> Result<i64, ApiError> {
        match coerce(
            &serde_json::Value::String(s.to_string()),
            &ValueType::TimestampNs,
        ) {
            Ok(Literal::Timestamp(ts)) => Ok(ts.resolve(now_ns)),
            _ => Err(ApiError::bad_request(format!("invalid time bound: {s}"))),
        }
    };
    Ok(ResolvedWindow {
        start_ns: resolve(&range.from)?,
        end_ns: resolve(&range.to)?,
    })
}

/// Send a `query_ir` Flight ticket to a querier and collect the result
/// batches, alongside whether a `correlate` stage's join was truncated by
/// `[querier].correlate_max_rows` (see [`correlate_truncation_warning`]).
pub(super) async fn execute_ticket(
    state: &RouterAppState,
    ticket_content: String,
) -> Result<(Vec<RecordBatch>, bool), ApiError> {
    let (mut client, server_address) = state
        .service_registry()
        .get_flight_client_and_address_for_capability(ServiceCapability::QueryExecution)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to get Flight client for IR query");
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "no querier service available",
            )
        })?;

    let verb = common::self_monitoring::spans::ticket_verb(&ticket_content).map(str::to_owned);
    let ticket = Ticket::new(ticket_content);
    let mut flight_request = tonic::Request::new(ticket);
    let rpc_span = common::flight::trace_context::do_get_client_span(
        verb.as_deref(),
        &mut flight_request,
        Some(&server_address),
    );
    if let Some(key) = &state.config().auth.internal_service_key {
        common::flight::auth::attach_internal_auth(&mut flight_request, key);
    }

    // Bound the whole querier round-trip + drain with a deadline so a stalled
    // querier cannot hold the HTTP request (and connection) open indefinitely.
    let record_span = rpc_span.clone();
    tokio::time::timeout(
        IR_QUERY_TIMEOUT,
        async move {
            let mut stream = client
                .do_get(flight_request)
                .await
                .map_err(|e| ApiError::from_flight(&e, "query_ir"))?
                .into_inner();

            // Bound the buffered result size as well as the time — the deadline
            // alone would still let one uncapped query (no `limit` stage) buffer an
            // unbounded result set for up to the timeout.
            let mut data = Vec::new();
            let mut bytes: usize = 0;
            let mut correlate_truncated = false;
            while let Some(flight_data) = stream.next().await {
                let fd = flight_data.map_err(|e| ApiError::from_flight(&e, "query_ir"))?;
                // The trailer the querier appends after a truncated `correlate`
                // join (see `common::flight::correlate_truncated_trailer`) is a
                // data-free message: recognized and dropped here rather than
                // handed to `decode_flight_batches`, which expects only schema
                // and record-batch messages.
                if fd.app_metadata.as_ref() == common::flight::CORRELATE_TRUNCATED_APP_METADATA {
                    correlate_truncated = true;
                    continue;
                }
                bytes = bytes.saturating_add(fd.data_body.len());
                if bytes > MAX_IR_RESULT_BYTES {
                    return Err(ApiError::new(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "IR query result too large; add a `limit` stage or narrow the range",
                    ));
                }
                data.push(fd);
            }
            common::self_monitoring::spans::record_rpc_result(
                &record_span,
                common::self_monitoring::spans::RpcBoundary::Client,
                tonic::Code::Ok,
            );
            let batches = super::flight_decode::decode_flight_batches(data, "query_ir")
                .await
                .map_err(ApiError::from)?;
            Ok((batches, correlate_truncated))
        }
        .instrument(rpc_span),
    )
    .await
    .map_err(|_| ApiError::new(StatusCode::GATEWAY_TIMEOUT, "IR query timed out"))?
}

/// Upper bound on a single IR query's querier round-trip and result drain.
const IR_QUERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Upper bound on the encoded Flight result a single IR query may buffer, so an
/// uncapped query cannot exhaust router memory before the deadline fires.
const MAX_IR_RESULT_BYTES: usize = 256 * 1024 * 1024;

/// Shape RecordBatches into the declared result envelope.
fn build_envelope(
    result: &str,
    window: ResolvedWindow,
    batches: &[RecordBatch],
    document: &serde_json::Value,
) -> Result<QueryIrResponse, ApiError> {
    match result {
        "series" => {
            let (series, step_ns) = to_series(batches);
            Ok(QueryIrResponse {
                result: result.to_string(),
                window,
                columns: Vec::new(),
                rows: Vec::new(),
                series,
                step_ns,
                heatmap: HeatmapResult::default(),
                flamegraph: None,
                graph: None,
                metadata: None,
                warnings: Vec::new(),
            })
        }
        "rows" | "table" => {
            let (columns, rows) = ir_table(batches);
            Ok(QueryIrResponse {
                result: result.to_string(),
                window,
                columns,
                rows,
                series: Vec::new(),
                step_ns: None,
                heatmap: HeatmapResult::default(),
                flamegraph: None,
                graph: None,
                metadata: None,
                warnings: Vec::new(),
            })
        }
        "heatmap" => {
            let doc: common::query_ir::Document = serde_json::from_value(document.clone())
                .map_err(|e| ApiError::bad_request(format!("invalid heatmap document: {e}")))?;
            let stage = doc
                .pipeline
                .iter()
                .find_map(|stage| match stage {
                    common::query_ir::Stage::Heatmap(stage) => Some(stage),
                    _ => None,
                })
                .ok_or_else(|| ApiError::bad_request("heatmap result requires a heatmap stage"))?;
            let step_ns = common::query_ir::parse_duration_ns(&stage.x.step)
                .ok_or_else(|| ApiError::bad_request("invalid heatmap step"))?;
            let bounds = stage
                .y
                .bounds
                .iter()
                .map(
                    |bound| match common::query_ir::coerce(bound, &ValueType::DurationNs) {
                        Ok(Literal::Duration(value)) => Ok(value),
                        _ => Err(ApiError::bad_request("invalid heatmap duration bound")),
                    },
                )
                .collect::<Result<Vec<_>, _>>()?;
            Ok(QueryIrResponse {
                result: result.to_string(),
                window,
                columns: Vec::new(),
                rows: Vec::new(),
                series: Vec::new(),
                step_ns: None,
                heatmap: HeatmapResult {
                    x: HeatmapAxisX {
                        step_ns,
                        align: stage.x.align.clone(),
                    },
                    y: HeatmapAxisY {
                        of: stage.y.of.clone(),
                        value_type: "duration_ns".into(),
                        bounds,
                        overflow: stage.y.overflow,
                    },
                    value: stage.value.as_name.clone(),
                    cells: to_heatmap_cells(batches)?,
                },
                flamegraph: None,
                graph: None,
                metadata: None,
                warnings: Vec::new(),
            })
        }
        "flamegraph" => Ok(QueryIrResponse {
            result: result.to_string(),
            window,
            columns: Vec::new(),
            rows: Vec::new(),
            series: Vec::new(),
            step_ns: None,
            heatmap: HeatmapResult::default(),
            flamegraph: Some(to_flamegraph_result(batches)?),
            graph: None,
            metadata: None,
            warnings: Vec::new(),
        }),
        "graph" => {
            let graph = to_graph(batches)?;
            Ok(QueryIrResponse {
                result: result.to_string(),
                window,
                columns: Vec::new(),
                rows: Vec::new(),
                series: Vec::new(),
                step_ns: None,
                heatmap: HeatmapResult::default(),
                flamegraph: None,
                metadata: None,
                warnings: graph_node_limit_warning(graph.dropped_nodes)
                    .into_iter()
                    .collect(),
                graph: Some(graph),
            })
        }
        other => Err(ApiError::bad_request(format!(
            "unsupported result envelope '{other}'"
        ))),
    }
}

/// Decode the querier's one-row `graph_json` batch (see
/// `querier::query::graph::encode_graph_batch`).
fn to_graph(batches: &[RecordBatch]) -> Result<common::service_graph::ServiceGraph, ApiError> {
    let Some(batch) = batches.iter().find(|b| b.num_rows() > 0) else {
        return Ok(Default::default());
    };
    let json = batch
        .column_by_name(common::service_graph::GRAPH_JSON_COLUMN)
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "graph result is missing graph_json",
            )
        })?;
    serde_json::from_str(json.value(0)).map_err(|e| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("invalid graph_json: {e}"),
        )
    })
}

/// The `graph` node cap (`[querier].graph_max_nodes`) dropped nodes.
const GRAPH_NODE_LIMIT: &str = "graph_node_limit";

fn graph_node_limit_warning(dropped_nodes: u64) -> Option<QueryWarning> {
    (dropped_nodes > 0).then(|| QueryWarning {
        code: GRAPH_NODE_LIMIT.to_string(),
        message: format!(
            "the graph reached the server node limit ([querier].graph_max_nodes); \
             {dropped_nodes} lower-traffic nodes were dropped"
        ),
        field: None,
        suggestions: Vec::new(),
    })
}

/// Decode the querier's single-row flamegraph batch
/// (`flamegraph_json: Utf8`, `truncated: Boolean` — see
/// `ir_planner::encode_flamegraph_batch`) into the HTTP response shape.
fn to_flamegraph_result(batches: &[RecordBatch]) -> Result<FlamegraphResult, ApiError> {
    let Some(batch) = batches.iter().find(|b| b.num_rows() > 0) else {
        return Ok(FlamegraphResult::default());
    };
    let json = batch
        .column_by_name("flamegraph_json")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "flamegraph result is missing flamegraph_json",
            )
        })?;
    let truncated = batch
        .column_by_name("truncated")
        .and_then(|array| array.as_any().downcast_ref::<BooleanArray>())
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "flamegraph result is missing truncated",
            )
        })?;
    let decoded: common::profile::Flamegraph =
        serde_json::from_str(json.value(0)).map_err(|e| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("invalid flamegraph_json: {e}"),
            )
        })?;
    Ok(FlamegraphResult {
        names: decoded.names,
        levels: decoded.levels,
        total: decoded.total,
        max_self: decoded.max_self,
        truncated: truncated.value(0),
        locations: decoded.locations,
    })
}

fn to_heatmap_cells(batches: &[RecordBatch]) -> Result<Vec<HeatmapCell>, ApiError> {
    let mut cells = Vec::new();
    for batch in batches {
        let time = batch
            .column_by_name("time_bucket_ns")
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| {
                ApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "heatmap result is missing time_bucket_ns",
                )
            })?;
        let duration = batch
            .column_by_name("duration_bucket")
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| {
                ApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "heatmap result is missing duration_bucket",
                )
            })?;
        let count = batch
            .column_by_name("count")
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| {
                ApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "heatmap result is missing count",
                )
            })?;
        for row in 0..batch.num_rows() {
            cells.push(HeatmapCell {
                time_bucket_ns: time.value(row),
                duration_bucket: duration.value(row),
                count: count.value(row),
            });
        }
    }
    Ok(cells)
}

/// Column name + IR value type for a batch field. A field's own metadata
/// (set by the querier for a type Arrow can't express on its own, e.g.
/// [`RAW_ATTRIBUTE_BAG_IR_TYPE`]) takes precedence over the Arrow-type
/// inference below.
fn column_meta(field: &datafusion::arrow::datatypes::Field) -> ResultColumn {
    if let Some(ir_type) = field.metadata().get(IR_TYPE_METADATA_KEY) {
        return ResultColumn {
            name: field.name().clone(),
            value_type: ir_type.clone(),
        };
    }
    let value_type = match field.data_type() {
        DataType::Boolean => "bool",
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => "int64",
        DataType::Float16 | DataType::Float32 | DataType::Float64 => "float64",
        DataType::Timestamp(_, _) => "timestamp_ns",
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "bytes",
        // Attribute containers (`resource_attributes`, `scope_attributes`,
        // `log_attributes`, `span_attributes`). Not an IR `ValueType`: a
        // container is a result column, never a predicate operand — a query
        // addresses a key inside it, not the container itself.
        DataType::Map(_, _) => MAP_TYPE,
        _ => "string",
    };
    ResultColumn {
        name: field.name().clone(),
        value_type: value_type.to_string(),
    }
}

/// The canonical Arrow type a column is normalized to before extraction, keyed
/// by the IR value type `column_meta` declares. Casting once here means `cell`
/// only handles a fixed set — so DataFusion's `Utf8View`, dictionary, wider
/// integer, and non-nanosecond timestamp encodings never fall through to null.
/// The result-metadata type name for an attribute container column.
const MAP_TYPE: &str = "map<string,string>";

/// The Arrow type a result column is canonicalized to before encoding, or
/// `None` for a column that must be encoded from its original array — a Map
/// has no canonical scalar form, and casting it would either fail (leaving the
/// original silently) or flatten it to a display string.
fn canonical_arrow_type(ir_type: &str) -> Option<DataType> {
    Some(match ir_type {
        "int64" => DataType::Int64,
        "float64" => DataType::Float64,
        "bool" => DataType::Boolean,
        "timestamp_ns" => DataType::Timestamp(TimeUnit::Nanosecond, None),
        "bytes" => DataType::Binary,
        MAP_TYPE | RAW_ATTRIBUTE_BAG_IR_TYPE => return None,
        _ => DataType::Utf8,
    })
}

/// Decodes an attribute-bag struct column (the querier's `attribute_bag_expr`
/// — five typed-layout columns as a struct's children, in that fixed order)
/// into one JSON object per row, via `common::attrs::typed::decode_typed_arrays`
/// — once per column, not once per cell. A malformed struct (never produced
/// by the querier, but not a `panic!`) decodes every row as `null`.
fn attribute_bag_cells(array: &dyn Array) -> Vec<serde_json::Value> {
    use common::attrs::typed::decode_typed_arrays;
    use datafusion::arrow::array::{BinaryArray, MapArray, StructArray};

    fn typed_children(
        cols: &[ArrayRef],
    ) -> Option<(&MapArray, &MapArray, &MapArray, &MapArray, &BinaryArray)> {
        Some((
            cols.first()?.as_any().downcast_ref()?,
            cols.get(1)?.as_any().downcast_ref()?,
            cols.get(2)?.as_any().downcast_ref()?,
            cols.get(3)?.as_any().downcast_ref()?,
            cols.get(4)?.as_any().downcast_ref()?,
        ))
    }

    let len = array.len();
    let decoded = array
        .as_any()
        .downcast_ref::<StructArray>()
        .and_then(|s| typed_children(s.columns()))
        .and_then(|(str_map, int_map, double_map, bool_map, residue)| {
            decode_typed_arrays(str_map, int_map, double_map, bool_map, residue).ok()
        });
    match decoded {
        Some(rows) => rows
            .into_iter()
            .map(|row| {
                row.map(serde_json::Value::Object)
                    .unwrap_or(serde_json::Value::Null)
            })
            .collect(),
        None => vec![serde_json::Value::Null; len],
    }
}

/// Extract one cell of an already-canonicalized array as JSON, following the IR
/// value encoding (timestamps as integer nanoseconds, bytes as base64, others
/// JSON-native).
fn cell(array: &dyn Array, row: usize) -> serde_json::Value {
    use serde_json::Value;
    if array.is_null(row) {
        return Value::Null;
    }
    macro_rules! downcast {
        ($t:ty) => {
            array.as_any().downcast_ref::<$t>()
        };
    }
    if let Some(a) = downcast!(StringArray) {
        return Value::String(a.value(row).to_string());
    }
    if let Some(a) = downcast!(Int64Array) {
        return Value::from(a.value(row));
    }
    if let Some(a) = downcast!(Float64Array) {
        return serde_json::Number::from_f64(a.value(row))
            .map(Value::Number)
            .unwrap_or(Value::Null);
    }
    if let Some(a) = downcast!(BooleanArray) {
        return Value::Bool(a.value(row));
    }
    if let Some(a) = downcast!(TimestampNanosecondArray) {
        return Value::from(a.value(row));
    }
    if let Some(a) = downcast!(BinaryArray) {
        use base64::Engine as _;
        return Value::String(base64::engine::general_purpose::STANDARD.encode(a.value(row)));
    }
    // Attribute containers arrive as `Map<Utf8, Utf8>` and encode as a JSON
    // object, so the client can index a key rather than parse a rendering.
    // A container whose keys or values are not strings falls through to the
    // formatter below rather than being dropped.
    if let Some(a) = downcast!(MapArray) {
        let entries = a.value(row);
        if let (Some(keys), Some(values)) = (
            entries.column(0).as_any().downcast_ref::<StringArray>(),
            entries.column(1).as_any().downcast_ref::<StringArray>(),
        ) {
            let mut object = serde_json::Map::with_capacity(entries.len());
            for i in 0..entries.len() {
                if !keys.is_null(i) && !values.is_null(i) {
                    object.insert(
                        keys.value(i).to_string(),
                        Value::String(values.value(i).to_string()),
                    );
                }
            }
            return Value::Object(object);
        }
    }
    // Last resort (an un-castable type, e.g. a struct/list left as-is): a string
    // rendering, so the column's data is never silently dropped as null.
    ArrayFormatter::try_new(array, &FormatOptions::default())
        .map(|f| Value::String(f.value(row).to_string()))
        .unwrap_or(Value::Null)
}

pub(super) fn ir_table(
    batches: &[RecordBatch],
) -> (Vec<ResultColumn>, Vec<Vec<serde_json::Value>>) {
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    let Some(first) = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .or(batches.first())
    else {
        return (columns, rows);
    };
    columns = first
        .schema()
        .fields()
        .iter()
        .map(|f| column_meta(f))
        .collect();
    let targets: Vec<Option<DataType>> = columns
        .iter()
        .map(|c| canonical_arrow_type(&c.value_type))
        .collect();
    for batch in batches {
        // Normalize each column to the canonical Arrow type its declared IR type
        // maps to; keep the original array if a cast is unsupported, or if the
        // column has no canonical form (an attribute container).
        let casted: Vec<ArrayRef> = (0..batch.num_columns())
            .map(|c| match &targets[c] {
                Some(target) => {
                    cast(batch.column(c), target).unwrap_or_else(|_| batch.column(c).clone())
                }
                None => batch.column(c).clone(),
            })
            .collect();
        // An attribute-bag column decodes once per column here, not once per
        // cell (see `attribute_bag_cells`).
        let bag_cells: Vec<Option<Vec<serde_json::Value>>> = columns
            .iter()
            .zip(&casted)
            .map(|(meta, array)| {
                (meta.value_type == RAW_ATTRIBUTE_BAG_IR_TYPE)
                    .then(|| attribute_bag_cells(array.as_ref()))
            })
            .collect();
        for r in 0..batch.num_rows() {
            let row = casted
                .iter()
                .enumerate()
                .map(|(c, a)| match &bag_cells[c] {
                    Some(decoded) => decoded[r].clone(),
                    None => cell(a.as_ref(), r),
                })
                .collect();
            rows.push(row);
        }
    }
    (columns, rows)
}

/// Reshape step-aggregate batches (`[bucket, labels…, value]`) into series.
fn to_series(batches: &[RecordBatch]) -> (Vec<ResultSeries>, Option<i64>) {
    let mut order: Vec<String> = Vec::new();
    let mut series: BTreeMap<String, ResultSeries> = BTreeMap::new();

    for batch in batches {
        let ncols = batch.num_columns();
        if ncols < 2 {
            continue;
        }
        let schema = batch.schema();
        // Column 0 is `bucket` (the time axis); the last column is the value;
        // the columns between are the grouping labels.
        let label_cols: Vec<usize> = (1..ncols - 1).collect();
        let value_col = ncols - 1;
        // Normalize every column to its declared canonical Arrow type first, so
        // narrow-int / view / dictionary encodings serialize as the right JSON
        // (same as `ir_table`).
        let casted: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .enumerate()
            .map(
                |(c, f)| match canonical_arrow_type(&column_meta(f).value_type) {
                    Some(target) => {
                        cast(batch.column(c), &target).unwrap_or_else(|_| batch.column(c).clone())
                    }
                    None => batch.column(c).clone(),
                },
            )
            .collect();
        for r in 0..batch.num_rows() {
            let mut labels = BTreeMap::new();
            for &c in &label_cols {
                let name = schema.field(c).name().clone();
                let v = match cell(casted[c].as_ref(), r) {
                    serde_json::Value::String(s) => s,
                    other => other.to_string(),
                };
                labels.insert(name, v);
            }
            let key = labels
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(",");
            let t = cell(casted[0].as_ref(), r);
            let value = cell(casted[value_col].as_ref(), r);
            let entry = series.entry(key.clone()).or_insert_with(|| {
                order.push(key.clone());
                ResultSeries {
                    labels,
                    points: Vec::new(),
                }
            });
            entry.points.push([t, value]);
        }
    }

    let ordered = order
        .into_iter()
        .filter_map(|k| series.remove(&k))
        .collect();
    (ordered, None)
}

#[cfg(test)]
mod row_encoding {
    use super::{column_meta, ir_table};
    use datafusion::arrow::array::{ArrayRef, MapBuilder, RecordBatch, StringBuilder};
    use datafusion::arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    /// A `Map<Utf8, Utf8>` column named `name`; `None` is a null row.
    fn map_batch(name: &str, rows: Vec<Option<Vec<(&str, &str)>>>) -> RecordBatch {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for row in rows {
            match row {
                Some(pairs) => {
                    for (k, v) in pairs {
                        builder.keys().append_value(k);
                        builder.values().append_value(v);
                    }
                    builder.append(true).unwrap();
                }
                None => builder.append(false).unwrap(),
            }
        }
        let array: ArrayRef = Arc::new(builder.finish());
        let schema = Arc::new(Schema::new(vec![Field::new(
            name,
            array.data_type().clone(),
            true,
        )]));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    /// Attribute containers are Arrow `Map` columns. Without a Map branch the
    /// canonicalizing cast to Utf8 fails, the original array survives, and the
    /// cell falls through to `ArrayFormatter` — so the client receives the
    /// string `"{http.method: GET}"` instead of an object it can index.
    #[test]
    fn map_columns_encode_as_json_objects() {
        let batch = map_batch(
            "log_attributes",
            vec![Some(vec![("http.method", "GET"), ("user.id", "u-1")]), None],
        );
        let (columns, rows) = ir_table(&[batch]);

        assert_eq!(columns[0].value_type, "map<string,string>");
        assert_eq!(
            rows[0][0],
            serde_json::json!({ "http.method": "GET", "user.id": "u-1" }),
            "a map cell must be a JSON object, not a rendered string"
        );
        assert_eq!(
            rows[1][0],
            serde_json::Value::Null,
            "a null map row stays null rather than becoming an empty object"
        );
    }

    /// An empty map is distinct from a null one: the row carried the container
    /// but it held no attributes.
    #[test]
    fn empty_map_encodes_as_an_empty_object() {
        let batch = map_batch("scope_attributes", vec![Some(vec![])]);
        let (_, rows) = ir_table(&[batch]);
        assert_eq!(rows[0][0], serde_json::json!({}));
    }

    #[test]
    fn map_column_metadata_declares_the_map_type() {
        let batch = map_batch("resource_attributes", vec![Some(vec![("a", "b")])]);
        let field = batch.schema();
        let meta = column_meta(field.field(0));
        assert_eq!(meta.name, "resource_attributes");
        assert_eq!(meta.value_type, "map<string,string>");
    }

    /// A struct column tagged with
    /// `common::schema::typed_attributes::IR_TYPE_METADATA_KEY` — the shape
    /// the querier's `attribute_bag_expr` produces for a typed-layout
    /// container's raw accessor — declares `map<string,any>` and decodes as
    /// the JSON object its typed children encode, not the legacy layout's
    /// flat string map.
    #[test]
    fn metadata_tagged_struct_column_decodes_as_a_json_object() {
        use common::schema::typed_attributes::{IR_TYPE_METADATA_KEY, RAW_ATTRIBUTE_BAG_IR_TYPE};
        use datafusion::arrow::array::{
            BinaryArray, BooleanBuilder, Float64Builder, Int64Builder, StructArray,
        };
        use std::collections::HashMap;

        // Row 0: `http.method` in its `str` home. Row 1: no attributes at
        // all (every typed column null) — the container-absent case.
        macro_rules! empty_map {
            ($value_builder:expr) => {{
                let mut b = MapBuilder::new(None, StringBuilder::new(), $value_builder);
                b.append(false).unwrap();
                b.append(false).unwrap();
                Arc::new(b.finish()) as ArrayRef
            }};
        }
        let mut str_builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        str_builder.keys().append_value("http.method");
        str_builder.values().append_value("GET");
        str_builder.append(true).unwrap();
        str_builder.append(false).unwrap();
        let children: Vec<(&str, ArrayRef)> = vec![
            ("str", Arc::new(str_builder.finish())),
            ("int", empty_map!(Int64Builder::new())),
            ("double", empty_map!(Float64Builder::new())),
            ("bool", empty_map!(BooleanBuilder::new())),
            ("residue", Arc::new(BinaryArray::from(vec![None, None]))),
        ];
        let children: Vec<(Arc<Field>, ArrayRef)> = children
            .into_iter()
            .map(|(name, arr)| {
                (
                    Arc::new(Field::new(name, arr.data_type().clone(), true)),
                    arr,
                )
            })
            .collect();
        let array: ArrayRef = Arc::new(StructArray::from(children));
        let field = Field::new("log_attributes", array.data_type().clone(), true).with_metadata(
            HashMap::from([(
                IR_TYPE_METADATA_KEY.to_string(),
                RAW_ATTRIBUTE_BAG_IR_TYPE.to_string(),
            )]),
        );
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

        let meta = column_meta(batch.schema().field(0));
        assert_eq!(meta.value_type, RAW_ATTRIBUTE_BAG_IR_TYPE);

        let (columns, rows) = ir_table(&[batch]);
        assert_eq!(columns[0].value_type, RAW_ATTRIBUTE_BAG_IR_TYPE);
        assert_eq!(
            rows[0][0],
            serde_json::json!({ "http.method": "GET" }),
            "a map<string,any> cell must decode the struct into a JSON object"
        );
        assert_eq!(
            rows[1][0],
            serde_json::Value::Null,
            "a row with no attributes in any typed column stays null"
        );
    }
}

/// An `aggregate.by` field nothing in the window carries (#1070). The
/// grouping stays as the query asked for it — one null-labelled group — and
/// the envelope explains why, because a plan-time rejection would also
/// reject the legitimate case of a real attribute absent from a short window.
#[cfg(test)]
mod group_by_warnings {
    use super::{UNKNOWN_GROUP_BY_FIELD, closest_fields, unknown_group_by_warnings};
    use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// A one-group aggregate result: the `by` column plus a count.
    fn grouped(column: &str, labels: Vec<Option<&str>>) -> RecordBatch {
        let rows = labels.len();
        let label: ArrayRef = Arc::new(StringArray::from(labels));
        let count: ArrayRef = Arc::new(Int64Array::from(vec![1_i64; rows]));
        let schema = Arc::new(Schema::new(vec![
            Field::new(column, DataType::Utf8, true),
            Field::new("n", DataType::Int64, false),
        ]));
        RecordBatch::try_new(schema, vec![label, count]).unwrap()
    }

    fn document(by: &str) -> serde_json::Value {
        serde_json::json!({
            "irVersion": 1, "from": "traces",
            "range": { "from": "now-1h", "to": "now" },
            "result": "table",
            "pipeline": [{ "aggregate": { "by": [by], "aggs": [{ "fn": "count", "as": "n" }] } }]
        })
    }

    #[test]
    fn an_all_null_group_key_warns_and_names_the_field() {
        let batches = [grouped("bogus_field_xyz", vec![None, None])];
        let warnings = unknown_group_by_warnings("traces", &document("bogus_field_xyz"), &batches);

        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert_eq!(warnings[0].code, UNKNOWN_GROUP_BY_FIELD);
        assert_eq!(warnings[0].field.as_deref(), Some("bogus_field_xyz"));
        assert!(
            warnings[0].message.contains("bogus_field_xyz")
                && warnings[0].message.contains("traces"),
            "{}",
            warnings[0].message
        );
    }

    /// The camelCase spelling of a real field is the motivating typo: it must
    /// point at the field the caller meant.
    #[test]
    fn a_near_miss_spelling_suggests_the_real_field() {
        let batches = [grouped("statusCode", vec![None])];
        let warnings = unknown_group_by_warnings("traces", &document("statusCode"), &batches);

        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].suggestions.contains(&"status.code".to_string()),
            "{:?}",
            warnings[0].suggestions
        );
    }

    #[test]
    fn a_logical_field_never_warns_even_when_every_row_is_null() {
        let batches = [grouped("service_name", vec![None, None])];
        let warnings = unknown_group_by_warnings("traces", &document("service.name"), &batches);

        assert!(warnings.is_empty(), "{warnings:?}");
    }

    /// #1340: `resource.identity` is declared on every source but used to be
    /// unresolvable (a `LogicalSchema::resolve` bug, fixed independently in
    /// `schema/logical.rs`), so grouping by it always warned — even
    /// self-contradictorily suggesting the very field the caller used. Now
    /// that it resolves, an all-null result (e.g. every row predates the
    /// column) is unremarkable: no warning, same as any other logical field.
    #[test]
    fn resource_identity_group_key_never_warns_even_when_every_row_is_null() {
        let batches = [grouped("resource_identity", vec![None, None])];
        let warnings =
            unknown_group_by_warnings("traces", &document("resource.identity"), &batches);

        assert!(warnings.is_empty(), "{warnings:?}");
    }

    /// The bug the issue reported by name: a field that discovery
    /// advertises but that fails to resolve must never suggest itself as
    /// its own fix.
    /// `resource.identity` was the motivating case before the `resolve` fix
    /// above made it resolvable — `closest_fields` itself still must not
    /// self-suggest for any field, resolvable or not.
    #[test]
    fn closest_fields_never_suggests_the_queried_field_itself() {
        let schema = common::schema::logical::LogicalSchema::core();
        for source in ["logs", "traces", "metrics", "profiles"] {
            let suggestions = closest_fields(&schema, source, "resource.identity");
            assert!(
                !suggestions.contains(&"resource.identity".to_string()),
                "{source}: {suggestions:?}"
            );
        }
    }

    /// A real attribute that is simply absent from *this* window still
    /// warns — but one the window does carry must not, even partially.
    #[test]
    fn a_group_key_with_any_value_never_warns() {
        let batches = [grouped("job", vec![Some("api"), None])];
        let warnings = unknown_group_by_warnings("traces", &document("job"), &batches);

        assert!(warnings.is_empty(), "{warnings:?}");
    }

    /// An empty result is not evidence: the window held no records at all.
    #[test]
    fn an_empty_result_never_warns() {
        let batches = [grouped("bogus_field_xyz", vec![])];
        let warnings = unknown_group_by_warnings("traces", &document("bogus_field_xyz"), &batches);

        assert!(warnings.is_empty(), "{warnings:?}");
    }

    /// An `extract`-derived column belongs to the document, not the source:
    /// an all-null one means the parser matched nothing, a different story.
    #[test]
    fn an_extract_derived_group_key_never_warns() {
        let batches = [grouped("level", vec![None])];
        let doc = serde_json::json!({
            "irVersion": 1, "from": "logs",
            "range": { "from": "now-1h", "to": "now" },
            "result": "table",
            "pipeline": [
                { "extract": { "parser": "json", "as": [{ "name": "level", "type": "string" }] } },
                { "aggregate": { "by": ["level"], "aggs": [{ "fn": "count", "as": "n" }] } }
            ]
        });
        let warnings = unknown_group_by_warnings("logs", &doc, &batches);

        assert!(warnings.is_empty(), "{warnings:?}");
    }
}

/// The `correlate` stage's row cap (`[querier].correlate_max_rows`) reached
/// during the join — the querier truncates rather than fails, and this
/// warning is the caller's only signal that it happened.
#[cfg(test)]
mod correlate_warnings {
    use super::{CORRELATE_ROW_LIMIT, correlate_truncation_warning};

    #[test]
    fn truncated_flag_warns() {
        let warnings = correlate_truncation_warning(true);
        assert_eq!(
            warnings.map(|w| w.code),
            Some(CORRELATE_ROW_LIMIT.to_string())
        );
    }

    #[test]
    fn untruncated_flag_does_not_warn() {
        assert!(correlate_truncation_warning(false).is_none());
    }
}

#[cfg(test)]
mod tests {
    use super::{
        GRAPH_NODE_LIMIT, MultiQueryIrRequest, QueryFormula, QueryIrRequest, QueryRange,
        ResolvedWindow, build_envelope, check_multi_source_scopes, parse_envelope,
        source_read_scope, to_multi_document,
    };
    use crate::{RouterAppState, create_router};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use common::auth::{TenantContext, TenantSource};
    use common::catalog::Catalog;
    use common::catalog::MembershipRole;
    use common::config::{ApiKeyConfig, Configuration, DatasetConfig, TenantConfig};
    use tower::ServiceExt;

    fn test_config() -> Configuration {
        let mut config = Configuration::default();
        config.auth = common::config::AuthConfig {
            tenants: vec![
                TenantConfig {
                    id: "acme".to_string(),
                    slug: "acme".to_string(),
                    name: "Acme".to_string(),
                    default_dataset: Some("default".to_string()),
                    datasets: vec![DatasetConfig {
                        id: "alternate".into(),
                        slug: "alternate".into(),
                        is_default: false,
                        storage: None,
                    }],
                    api_keys: vec![ApiKeyConfig {
                        key: "sk-test-key".to_string(),
                        name: Some("test".to_string()),
                    }],
                    schema_config: None,
                    limits: None,
                },
                TenantConfig {
                    id: "other".into(),
                    slug: "other".into(),
                    name: "Other".into(),
                    default_dataset: Some("default".into()),
                    datasets: vec![],
                    api_keys: vec![ApiKeyConfig {
                        key: "sk-other-key".into(),
                        name: Some("other".into()),
                    }],
                    schema_config: None,
                    limits: None,
                },
            ],
            ..Default::default()
        };
        config
    }

    async fn test_app() -> axum::Router {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        create_router(RouterAppState::new(catalog, test_config()))
    }

    fn ir_body() -> Body {
        ir_body_for("logs")
    }

    fn ir_body_for(source: &str) -> Body {
        Body::from(
            serde_json::to_vec(&serde_json::json!({
                "irVersion": 1,
                "from": source,
                "range": { "from": "now-1h", "to": "now" },
                "result": "rows",
                "pipeline": []
            }))
            .unwrap(),
        )
    }

    fn post(uri: &str, auth: bool, body: Body) -> Request<Body> {
        let mut b = Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json");
        if auth {
            b = b
                .header("authorization", "Bearer sk-test-key")
                .header("x-tenant-id", "acme");
        }
        b.body(body).unwrap()
    }

    fn scoped_context(scopes: Vec<&str>) -> TenantContext {
        TenantContext::new(
            "acme".into(),
            "default".into(),
            "acme".into(),
            "default".into(),
            None,
            TenantSource::Database,
        )
        .with_user("u1".into(), MembershipRole::Member, false, None)
        .with_api_key_restrictions(
            Some(scopes.into_iter().map(str::to_string).collect()),
            None,
            None,
        )
    }

    #[test]
    fn ir_source_scopes_are_checked_before_dispatch() {
        let profiles = scoped_context(vec!["profiles:read"]);
        assert!(source_read_scope(&profiles, "profiles").is_ok());
        assert!(source_read_scope(&profiles, "logs").is_err());
        assert!(source_read_scope(&profiles, "traces").is_err());
    }

    // The `metrics` Query IR source (PR #1138) 400'd end-to-end through the
    // router even with a valid metrics:read scope, because this gate never
    // matched "metrics" — the querier's planner tests never caught it since
    // they call the planner directly, bypassing this scope check entirely.
    #[test]
    fn metrics_read_scope_grants_the_metrics_ir_source() {
        let metrics = scoped_context(vec!["metrics:read"]);
        assert!(source_read_scope(&metrics, "metrics").is_ok());
        assert!(source_read_scope(&metrics, "logs").is_err());
        assert!(source_read_scope(&metrics, "traces").is_err());
        assert!(source_read_scope(&metrics, "profiles").is_err());

        let profiles = scoped_context(vec!["profiles:read"]);
        assert!(source_read_scope(&profiles, "metrics").is_err());
    }

    #[test]
    fn metrics_read_scope_also_grants_the_metrics_histogram_ir_source() {
        let metrics = scoped_context(vec!["metrics:read"]);
        assert!(source_read_scope(&metrics, "metrics_histogram").is_ok());

        let profiles = scoped_context(vec!["profiles:read"]);
        assert!(source_read_scope(&profiles, "metrics_histogram").is_err());
    }

    // Task 6.1 — unauthenticated requests are rejected.
    #[tokio::test]
    async fn ir_query_requires_authentication() {
        let app = test_app().await;
        let resp = app
            .clone()
            .oneshot(post("/api/v1/query", false, ir_body()))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn heatmap_query_authenticates_each_tenant_dataset_context() {
        let app = test_app().await;
        let heatmap = || {
            Body::from(serde_json::to_vec(&serde_json::json!({
            "irVersion": 2, "from": "traces", "range": { "from": "now-1h", "to": "now" }, "result": "heatmap",
            "pipeline": [{ "heatmap": { "x": { "step": "1m", "align": "epoch" }, "y": { "of": "duration", "bounds": ["1ms"], "overflow": true }, "value": { "fn": "count", "as": "count" } }}]
        })).unwrap())
        };
        for (key, tenant, dataset) in [
            ("sk-test-key", "acme", Some("alternate")),
            ("sk-other-key", "other", None),
        ] {
            let mut request = Request::builder()
                .method("POST")
                .uri("/api/v1/query")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {key}"))
                .header("x-tenant-id", tenant);
            if let Some(dataset) = dataset {
                request = request.header("x-dataset-id", dataset);
            }
            let response = app
                .clone()
                .oneshot(request.body(heatmap()).unwrap())
                .await
                .unwrap();
            // Both independently authenticated contexts reach the native query
            // boundary. The no-querier fixture stops before execution.
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        }
    }

    // Task 6.1 — a valid request with no querier surfaces 503, not 200.
    #[tokio::test]
    async fn ir_query_without_a_querier_is_service_unavailable() {
        let app = test_app().await;
        let resp = app
            .clone()
            .oneshot(post("/api/v1/query", true, ir_body()))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // Regression for the `metrics_read_scope_grants_the_metrics_ir_source`
    // gap: that test calls `source_read_scope` directly, so it couldn't have
    // caught the router previously 400ing "unknown query source 'metrics'"
    // before reaching this fixture at all. This drives a real request
    // through the full `/api/v1/query` handler, so a regression here fails
    // as 400 (unknown source), not 503 (no querier — the boundary this test
    // expects to reach).
    #[tokio::test]
    async fn metrics_source_reaches_the_query_boundary() {
        let app = test_app().await;
        let resp = app
            .clone()
            .oneshot(post("/api/v1/query", true, ir_body_for("metrics")))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // Task 6.1 — a malformed IR body is a client error, not a 500.
    #[tokio::test]
    async fn ir_query_with_malformed_body_is_client_error() {
        let app = test_app().await;
        let resp = app
            .clone()
            .oneshot(post("/api/v1/query", true, Body::from("{ not valid json")))
            .await
            .unwrap();
        assert!(resp.status().is_client_error(), "got {}", resp.status());
    }

    #[test]
    fn heatmap_envelope_shapes_sparse_flight_cells() {
        use datafusion::arrow::array::{Int64Array, RecordBatch};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("time_bucket_ns", DataType::Int64, false),
                Field::new("duration_bucket", DataType::Int64, false),
                Field::new("count", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![0])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![2])),
            ],
        )
        .unwrap();
        let document = serde_json::json!({
            "irVersion": 2, "from": "traces", "range": { "from": "0", "to": "60" }, "result": "heatmap",
            "pipeline": [{ "heatmap": { "x": { "step": "1m", "align": "epoch" }, "y": { "of": "duration", "bounds": ["1ms"], "overflow": true }, "value": { "fn": "count", "as": "count" } }}]
        });
        let response = build_envelope(
            "heatmap",
            ResolvedWindow {
                start_ns: 0,
                end_ns: 60,
            },
            &[batch],
            &document,
        )
        .unwrap();
        assert_eq!(response.heatmap.cells[0].count, 2);
        assert_eq!(response.heatmap.y.bounds, vec![1_000_000]);
    }

    // profile-payload-access task 3.1 — flamegraph envelope.

    /// Mirrors `metrics_source_reaches_the_query_boundary`: a real request
    /// through the full `/api/v1/query` handler with `profiles:read` fails
    /// as 503 (no querier in this fixture — the boundary this test expects
    /// to reach), not 400/403, proving the flamegraph envelope is accepted
    /// and dispatched for the `profiles` source like any other envelope.
    #[tokio::test]
    async fn flamegraph_on_profiles_reaches_the_query_boundary() {
        let app = test_app().await;
        let body = Body::from(
            serde_json::to_vec(&serde_json::json!({
                "irVersion": 1, "from": "profiles", "range": { "from": "now-1h", "to": "now" },
                "result": "flamegraph",
                "pipeline": [{ "where": { "field": "profile.id", "op": "eq", "value": "abc" } }]
            }))
            .unwrap(),
        );
        let resp = app
            .clone()
            .oneshot(post("/api/v1/query", true, body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn flamegraph_without_profiles_scope_is_rejected_before_dispatch() {
        let no_scope = scoped_context(vec!["logs:read"]);
        assert!(source_read_scope(&no_scope, "profiles").is_err());
    }

    #[test]
    fn graph_envelope_decodes_the_querier_batch_and_warns_on_dropped_nodes() {
        use datafusion::arrow::array::{RecordBatch, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;
        let graph_json = serde_json::json!({
            "nodes": [
                { "id": "service:orders", "name": "orders", "kind": "service", "request_rate": 1.5,
                  "error_rate": 0.0, "p95_ns": 100 },
                { "id": "external:database:orders-db", "name": "orders-db", "kind": "external",
                  "dependency_kind": "database" }
            ],
            "edges": [{ "source": "service:orders", "target": "external:database:orders-db", "count": 3,
                        "rate": 0.05, "error_rate": 0.0, "p95_ns": 50 }],
            "dropped_nodes": 3
        })
        .to_string();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                common::service_graph::GRAPH_JSON_COLUMN,
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec![graph_json]))],
        )
        .unwrap();
        let document = serde_json::json!({
            "irVersion": 8, "from": "traces", "range": { "from": "0", "to": "60" },
            "result": "graph", "pipeline": []
        });
        let response = build_envelope(
            "graph",
            ResolvedWindow {
                start_ns: 0,
                end_ns: 60,
            },
            &[batch],
            &document,
        )
        .unwrap();
        let graph = response.graph.expect("graph envelope is present");
        assert_eq!(graph.nodes.len(), 2);
        assert_eq!(graph.edges[0].count, 3);
        assert_eq!(
            response
                .warnings
                .iter()
                .map(|w| w.code.as_str())
                .collect::<Vec<_>>(),
            vec![GRAPH_NODE_LIMIT]
        );
    }

    #[test]
    fn graph_scoping_fields_reach_the_querier_document() {
        let req: QueryIrRequest = serde_json::from_value(serde_json::json!({
            "irVersion": 8, "from": "traces", "range": { "from": "now-1h", "to": "now" },
            "result": "graph", "focus": "orders", "depth": 2, "pipeline": []
        }))
        .unwrap();
        let doc = serde_json::to_value(&req).unwrap();
        assert_eq!(doc["focus"], "orders");
        assert_eq!(doc["depth"], 2);
        assert!(doc.get("trace_id").is_none());
    }

    #[test]
    fn flamegraph_envelope_decodes_the_querier_batch() {
        use datafusion::arrow::array::{BooleanArray, RecordBatch, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;
        let flamegraph_json = serde_json::json!({
            "names": ["total", "main", "foo"],
            "levels": [[0, 100, 0, 0], [0, 100, 30, 1, 0, 70, 70, 2]],
            "total": 100,
            "max_self": 70,
            "locations": [null, {"file": "src/main.rs", "line": 12}, null]
        })
        .to_string();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("flamegraph_json", DataType::Utf8, false),
                Field::new("truncated", DataType::Boolean, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec![flamegraph_json])),
                Arc::new(BooleanArray::from(vec![true])),
            ],
        )
        .unwrap();
        let document = serde_json::json!({
            "irVersion": 1, "from": "profiles", "range": { "from": "0", "to": "60" },
            "result": "flamegraph", "pipeline": []
        });
        let response = build_envelope(
            "flamegraph",
            ResolvedWindow {
                start_ns: 0,
                end_ns: 60,
            },
            &[batch],
            &document,
        )
        .unwrap();
        let flamegraph = response.flamegraph.expect("flamegraph envelope is present");
        assert_eq!(flamegraph.names, vec!["total", "main", "foo"]);
        assert_eq!(flamegraph.total, 100);
        assert_eq!(flamegraph.max_self, 70);
        assert!(flamegraph.truncated);
        assert_eq!(
            flamegraph.locations,
            vec![
                None,
                Some(common::profile::FrameLocation {
                    file: "src/main.rs".to_string(),
                    line: 12,
                }),
                None,
            ]
        );
    }

    /// A `flamegraph_json` batch encoded before `locations` existed (no such
    /// key at all) decodes to an empty `Vec`, not an error — the field is
    /// additive per the "Flamegraph envelope carries per-name locations"
    /// decision.
    #[test]
    fn flamegraph_envelope_without_locations_field_decodes_to_empty_vec() {
        use datafusion::arrow::array::{BooleanArray, RecordBatch, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;
        let flamegraph_json = serde_json::json!({
            "names": ["total"],
            "levels": [[0, 0, 0, 0]],
            "total": 0,
            "max_self": 0
        })
        .to_string();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("flamegraph_json", DataType::Utf8, false),
                Field::new("truncated", DataType::Boolean, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec![flamegraph_json])),
                Arc::new(BooleanArray::from(vec![false])),
            ],
        )
        .unwrap();
        let document = serde_json::json!({
            "irVersion": 1, "from": "profiles", "range": { "from": "0", "to": "60" },
            "result": "flamegraph", "pipeline": []
        });
        let response = build_envelope(
            "flamegraph",
            ResolvedWindow {
                start_ns: 0,
                end_ns: 60,
            },
            &[batch],
            &document,
        )
        .unwrap();
        let flamegraph = response.flamegraph.expect("flamegraph envelope is present");
        assert_eq!(flamegraph.locations, Vec::new());
    }

    /// A flamegraph query that matches zero profile rows still carries
    /// `Some(FlamegraphResult)` — never `None`. `None` on this field means
    /// "not a flamegraph response", not "zero matches"; conflating the two
    /// would make an empty match set indistinguishable from a wrong envelope
    /// to any consumer that branches on presence (e.g. the MCP `get_profile`
    /// tool's not-found check).
    #[test]
    fn flamegraph_envelope_is_some_even_when_no_rows_matched() {
        let document = serde_json::json!({
            "irVersion": 1, "from": "profiles", "range": { "from": "0", "to": "60" },
            "result": "flamegraph", "pipeline": []
        });
        let response = build_envelope(
            "flamegraph",
            ResolvedWindow {
                start_ns: 0,
                end_ns: 60,
            },
            &[],
            &document,
        )
        .unwrap();
        let flamegraph = response
            .flamegraph
            .expect("flamegraph envelope is present even with zero matches");
        assert!(flamegraph.names.is_empty());
        assert_eq!(flamegraph.total, 0);
        assert!(!flamegraph.truncated);
    }

    /// Every non-flamegraph envelope carries `flamegraph: None` — the field
    /// only ever appears for `result == "flamegraph"`.
    #[test]
    fn non_flamegraph_envelopes_carry_no_flamegraph_field() {
        let document = serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": "0", "to": "60" },
            "result": "rows", "pipeline": []
        });
        let response = build_envelope(
            "rows",
            ResolvedWindow {
                start_ns: 0,
                end_ns: 60,
            },
            &[],
            &document,
        )
        .unwrap();
        assert!(response.flamegraph.is_none());
    }

    // Task 5.2 — formulas wired into POST /api/v1/query.

    /// A multi-query formula body (the error-ratio scenario from D5) is
    /// recognized by the `queries` key, authorized, and reaches the query
    /// boundary — same fixture pattern as `metrics_source_reaches_the_query_boundary`:
    /// this fixture has no querier, so a correctly-routed, correctly-authorized
    /// request fails as 503 (no querier), never 400/403.
    #[tokio::test]
    async fn formula_request_reaches_the_query_boundary() {
        let app = test_app().await;
        let body = Body::from(
            serde_json::to_vec(&serde_json::json!({
                "queries": {
                    "errors": {
                        "irVersion": 1, "from": "traces",
                        "range": { "from": "now-1h", "to": "now" }, "result": "series",
                        "pipeline": [{ "aggregate": {
                            "by": ["service.name"],
                            "aggs": [{ "fn": "count", "as": "n" }],
                            "step": "1m"
                        } }]
                    },
                    "total": {
                        "irVersion": 1, "from": "traces",
                        "range": { "from": "now-1h", "to": "now" }, "result": "series",
                        "pipeline": [{ "aggregate": {
                            "by": ["service.name"],
                            "aggs": [{ "fn": "count", "as": "n" }],
                            "step": "1m"
                        } }]
                    }
                },
                "formulas": [{ "name": "error_ratio", "expr": "errors / total" }],
                "result": "series"
            }))
            .unwrap(),
        );
        let resp = app
            .clone()
            .oneshot(post("/api/v1/query", true, body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    /// A malformed/incomplete formula document (no `formulas`) is rejected as
    /// a client error before it ever reaches the query boundary — proof the
    /// `queries` key alone routes into the multi-query path rather than
    /// silently falling back to the single-query shape (which has no `from`
    /// here and would 400 for a different reason).
    #[tokio::test]
    async fn formula_request_without_formulas_is_a_client_error() {
        let app = test_app().await;
        let body = Body::from(
            serde_json::to_vec(&serde_json::json!({
                "queries": {
                    "a": {
                        "irVersion": 1, "from": "traces",
                        "range": { "from": "now-1h", "to": "now" }, "result": "series",
                        "pipeline": [{ "aggregate": {
                            "by": [], "aggs": [{ "fn": "count", "as": "n" }], "step": "1m"
                        } }]
                    }
                },
                "formulas": [],
                "result": "series"
            }))
            .unwrap(),
        );
        let resp = app
            .clone()
            .oneshot(post("/api/v1/query", true, body))
            .await
            .unwrap();
        assert!(resp.status().is_client_error(), "got {}", resp.status());
    }

    /// Every inner query's source is authorized before any of them run — the
    /// same [`source_read_scope`] check a single-query request goes through,
    /// applied per named query. A request holding one authorized and one
    /// unauthorized source must reject as a whole, not run the authorized
    /// half first.
    #[test]
    fn multi_query_rejects_when_one_inner_source_is_unauthorized() {
        let scoped = scoped_context(vec!["traces:read"]);
        let mut queries = std::collections::BTreeMap::new();
        queries.insert(
            "a".to_string(),
            QueryIrRequest {
                ir_version: 1,
                from: "traces".to_string(),
                range: QueryRange {
                    from: "now-1h".to_string(),
                    to: "now".to_string(),
                },
                result: "series".to_string(),
                fields: None,
                pipeline: Vec::new(),
                focus: None,
                depth: None,
                trace_id: None,
            },
        );
        queries.insert(
            "b".to_string(),
            QueryIrRequest {
                ir_version: 1,
                from: "logs".to_string(),
                range: QueryRange {
                    from: "now-1h".to_string(),
                    to: "now".to_string(),
                },
                result: "series".to_string(),
                fields: None,
                pipeline: Vec::new(),
                focus: None,
                depth: None,
                trace_id: None,
            },
        );
        assert!(check_multi_source_scopes(&scoped, &queries).is_err());

        // The all-authorized case (both `traces`) is accepted.
        let mut both_traces = std::collections::BTreeMap::new();
        both_traces.insert("a".to_string(), queries["a"].clone());
        let mut c = queries["a"].clone();
        c.from = "traces".to_string();
        both_traces.insert("c".to_string(), c);
        assert!(check_multi_source_scopes(&scoped, &both_traces).is_ok());
    }

    // Task 5.1/5.2 — the formula evaluator's HTTP wiring: `to_multi_document`
    // builds the same IR the evaluator runs against, and `parse_envelope`
    // rejects anything but the six declared envelopes.
    #[test]
    fn to_multi_document_builds_the_declared_queries_and_formulas() {
        let mut queries = std::collections::BTreeMap::new();
        queries.insert(
            "a".to_string(),
            QueryIrRequest {
                ir_version: 1,
                from: "traces".to_string(),
                range: QueryRange {
                    from: "now-1h".to_string(),
                    to: "now".to_string(),
                },
                result: "series".to_string(),
                fields: None,
                pipeline: vec![serde_json::json!({
                    "aggregate": { "by": [], "aggs": [{ "fn": "count", "as": "n" }], "step": "1m" }
                })],
                focus: None,
                depth: None,
                trace_id: None,
            },
        );
        let req = MultiQueryIrRequest {
            queries,
            formulas: vec![QueryFormula {
                name: "f".to_string(),
                expr: "a * 2".to_string(),
            }],
            result: "series".to_string(),
        };
        let multi = to_multi_document(&req).unwrap();
        assert_eq!(multi.queries.len(), 1);
        assert_eq!(multi.formulas.len(), 1);
        assert_eq!(multi.formulas[0].expr, "a * 2");
        assert_eq!(multi.result, common::query_ir::ResultEnvelope::Series);
    }

    #[test]
    fn parse_envelope_rejects_an_unknown_result() {
        assert!(parse_envelope("bogus").is_err());
        assert_eq!(
            parse_envelope("series").unwrap(),
            common::query_ir::ResultEnvelope::Series
        );
    }
}
