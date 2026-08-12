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

use std::collections::BTreeMap;

use tracing::Instrument;

use arrow_flight::Ticket;
use axum::{Router, extract::State, http::StatusCode, routing::post};
use common::auth::{TenantContext, TenantContextExtractor};
use common::flight::transport::ServiceCapability;
use common::query_ir::{Literal, ValueType, coerce};
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
use crate::RouterState;

pub fn router<S: RouterState>() -> Router<S> {
    Router::new().route("/query", post(query_ir::<S>))
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
    /// Declared result envelope: `rows`, `series`, `table`, `heatmap`, or
    /// (for the `profiles` source only) `flamegraph`.
    #[schema(example = "rows")]
    pub result: String,
    /// Curated projection (logical field names) for `rows`/`table`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
    /// Ordered transform stages (opaque objects; see the IR spec).
    #[serde(default)]
    #[schema(value_type = Vec<Object>)]
    pub pipeline: Vec<serde_json::Value>,
}

/// The resolved absolute time window, echoed for reproducibility/replay.
#[derive(Debug, Clone, Serialize, ToSchema)]
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
    /// `true` when the matched profile rows exceeded the server's payload
    /// cap and the flamegraph was aggregated over only the first
    /// `max_search_limit` of them.
    pub truncated: bool,
}

impl FlamegraphResult {
    fn is_empty(&self) -> bool {
        self.names.is_empty() && self.levels.is_empty()
    }
}

/// The single canonical response contract. `result` discriminates which fields
/// are populated: `rows`/`table` fill `columns` + `rows`; `series` fills
/// `series` + `step_ns`; `heatmap` fills `heatmap`; `flamegraph` fills
/// `flamegraph`.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct QueryIrResponse {
    /// The result envelope: `rows`, `series`, `table`, `heatmap`, or `flamegraph`.
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
    #[serde(default, skip_serializing_if = "FlamegraphResult::is_empty")]
    pub flamegraph: FlamegraphResult,
}

/// Submit a native Query IR document.
#[utoipa::path(
    post,
    path = "/api/v1/query",
    tag = "query",
    security(("bearerAuth" = [])),
    request_body = QueryIrRequest,
    responses(
        (status = 200, description = "The enveloped query result", body = QueryIrResponse),
        (status = 400, description = "Invalid IR document"),
        (status = 401, description = "Missing or invalid credentials"),
        (status = 503, description = "No querier service available"),
    )
)]
#[tracing::instrument(skip(state, tenant_ctx, req), fields(
    signaldb.tenant.id = %tenant_ctx.0.tenant_id,
    signaldb.dataset.id = %tenant_ctx.0.dataset_id,
    source = %req.from,
    result = %req.result,
))]
pub async fn query_ir<S: RouterState>(
    State(state): State<S>,
    tenant_ctx: TenantContextExtractor,
    axum::Json(req): axum::Json<QueryIrRequest>,
) -> Result<axum::Json<QueryIrResponse>, ApiError> {
    let ctx = &tenant_ctx.0;

    // Query IR covers several signal tables, so its authorization must be
    // selected from the source before the ticket can reach a querier.
    source_read_scope(ctx, &req.from)?;

    // Stamp the server clock once, at the ticket boundary, so relative anchors
    // resolve to a single absolute window every stage of the plan sees.
    let now = now_ns();
    let window = resolve_window(&req.range, now)?;

    // The IR document is the request re-serialized; the querier validates it.
    let document = serde_json::to_value(&req)
        .map_err(|e| ApiError::bad_request(format!("invalid IR document: {e}")))?;
    let payload = serde_json::json!({ "document": document.clone(), "now_ns": now });
    let payload = serde_json::to_string(&payload)
        .map_err(|e| ApiError::bad_request(format!("invalid IR document: {e}")))?;
    let ticket = format!(
        "query_ir:{}:{}:{}",
        ctx.tenant_slug, ctx.dataset_slug, payload
    );

    let batches = execute_ticket(&state, ticket).await?;
    let response = build_envelope(&req.result, window, &batches, &document)?;
    Ok(axum::Json(response))
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

/// Send a `query_ir` Flight ticket to a querier and collect the result batches.
async fn execute_ticket<S: RouterState>(
    state: &S,
    ticket_content: String,
) -> Result<Vec<RecordBatch>, ApiError> {
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
            while let Some(flight_data) = stream.next().await {
                let fd = flight_data.map_err(|e| ApiError::from_flight(&e, "query_ir"))?;
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
            super::flight_decode::decode_flight_batches(data, "query_ir")
                .await
                .map_err(ApiError::from)
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
                flamegraph: FlamegraphResult::default(),
            })
        }
        "rows" | "table" => {
            let (columns, rows) = to_rows(batches);
            Ok(QueryIrResponse {
                result: result.to_string(),
                window,
                columns,
                rows,
                series: Vec::new(),
                step_ns: None,
                heatmap: HeatmapResult::default(),
                flamegraph: FlamegraphResult::default(),
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
                flamegraph: FlamegraphResult::default(),
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
            flamegraph: to_flamegraph_result(batches)?,
        }),
        other => Err(ApiError::bad_request(format!(
            "unsupported result envelope '{other}'"
        ))),
    }
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
    #[derive(Deserialize)]
    struct Decoded {
        names: Vec<String>,
        levels: Vec<Vec<i64>>,
        total: i64,
        max_self: i64,
    }
    let decoded: Decoded = serde_json::from_str(json.value(0)).map_err(|e| {
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

/// Column name + IR value type for a batch field.
fn column_meta(field: &datafusion::arrow::datatypes::Field) -> ResultColumn {
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
        MAP_TYPE => return None,
        _ => DataType::Utf8,
    })
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

fn to_rows(batches: &[RecordBatch]) -> (Vec<ResultColumn>, Vec<Vec<serde_json::Value>>) {
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
        for r in 0..batch.num_rows() {
            let row = casted.iter().map(|a| cell(a.as_ref(), r)).collect();
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
        // (same as `to_rows`).
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

/// Current time as unix-epoch nanoseconds.
fn now_ns() -> i64 {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis() * 1_000_000)
}

#[cfg(test)]
mod row_encoding {
    use super::{column_meta, to_rows};
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
        let (columns, rows) = to_rows(&[batch]);

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
        let (_, rows) = to_rows(&[batch]);
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
}

#[cfg(test)]
mod tests {
    use super::{ResolvedWindow, build_envelope, source_read_scope};
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
        .with_api_key_restrictions(Some(scopes.into_iter().map(str::to_string).collect()), None)
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
    fn flamegraph_envelope_decodes_the_querier_batch() {
        use datafusion::arrow::array::{BooleanArray, RecordBatch, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;
        let flamegraph_json = serde_json::json!({
            "names": ["total", "main", "foo"],
            "levels": [[0, 100, 0, 0], [0, 100, 30, 1, 0, 70, 70, 2]],
            "total": 100,
            "max_self": 70
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
        assert_eq!(response.flamegraph.names, vec!["total", "main", "foo"]);
        assert_eq!(response.flamegraph.total, 100);
        assert_eq!(response.flamegraph.max_self, 70);
        assert!(response.flamegraph.truncated);
    }

    #[test]
    fn flamegraph_envelope_defaults_when_no_rows_matched() {
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
        assert!(response.flamegraph.names.is_empty());
        assert_eq!(response.flamegraph.total, 0);
        assert!(!response.flamegraph.truncated);
    }
}
