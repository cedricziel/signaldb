//! # Native Query IR HTTP API
//!
//! `POST /api/v1/query` — the first-party, structured query surface. A client
//! posts a versioned IR document (see the `query-ir-core` capability); the
//! router stamps the server clock, forwards it to a querier as a
//! `query_ir:{tenant}:{dataset}:{json}` Flight ticket, and shapes the returned
//! RecordBatches into the declared result envelope (`rows` | `series` | `table`).
//!
//! Auth and tenant scoping are identical to the Tempo/LogQL/Prometheus
//! surfaces: the endpoint sits behind the auth middleware and derives the
//! tenant/dataset from the authenticated request context, never from the
//! document body.

use std::collections::BTreeMap;

use tracing::Instrument;

use arrow_flight::Ticket;
use axum::{Router, extract::State, http::StatusCode, routing::post};
use common::auth::TenantContextExtractor;
use common::flight::transport::ServiceCapability;
use common::query_ir::{Literal, ValueType, coerce};
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray,
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
    /// The registered signal source: `logs` or `traces`.
    #[schema(example = "logs")]
    pub from: String,
    pub range: QueryRange,
    /// Declared result envelope: `rows`, `series`, or `table`.
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

/// The single canonical response contract. `result` discriminates which fields
/// are populated: `rows`/`table` fill `columns` + `rows`; `series` fills
/// `series` + `step_ns`.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct QueryIrResponse {
    /// The result envelope: `rows`, `series`, or `table`.
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

    // Stamp the server clock once, at the ticket boundary, so relative anchors
    // resolve to a single absolute window every stage of the plan sees.
    let now = now_ns();
    let window = resolve_window(&req.range, now)?;

    // The IR document is the request re-serialized; the querier validates it.
    let document = serde_json::to_value(&req)
        .map_err(|e| ApiError::bad_request(format!("invalid IR document: {e}")))?;
    let payload = serde_json::json!({ "document": document, "now_ns": now });
    let payload = serde_json::to_string(&payload)
        .map_err(|e| ApiError::bad_request(format!("invalid IR document: {e}")))?;
    let ticket = format!(
        "query_ir:{}:{}:{}",
        ctx.tenant_slug, ctx.dataset_slug, payload
    );

    let batches = execute_ticket(&state, ticket).await?;
    let response = build_envelope(&req.result, window, &batches)?;
    Ok(axum::Json(response))
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
    let mut client = state
        .service_registry()
        .get_flight_client_for_capability(ServiceCapability::QueryExecution)
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
    let rpc_span =
        common::flight::trace_context::do_get_client_span(verb.as_deref(), &mut flight_request);
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
            })
        }
        other => Err(ApiError::bad_request(format!(
            "unsupported result envelope '{other}'"
        ))),
    }
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
fn canonical_arrow_type(ir_type: &str) -> DataType {
    match ir_type {
        "int64" => DataType::Int64,
        "float64" => DataType::Float64,
        "bool" => DataType::Boolean,
        "timestamp_ns" => DataType::Timestamp(TimeUnit::Nanosecond, None),
        "bytes" => DataType::Binary,
        _ => DataType::Utf8,
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
    let targets: Vec<DataType> = columns
        .iter()
        .map(|c| canonical_arrow_type(&c.value_type))
        .collect();
    for batch in batches {
        // Normalize each column to the canonical Arrow type its declared IR type
        // maps to; keep the original array if a cast is unsupported.
        let casted: Vec<ArrayRef> = (0..batch.num_columns())
            .map(|c| cast(batch.column(c), &targets[c]).unwrap_or_else(|_| batch.column(c).clone()))
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
            .map(|(c, f)| {
                let target = canonical_arrow_type(&column_meta(f).value_type);
                cast(batch.column(c), &target).unwrap_or_else(|_| batch.column(c).clone())
            })
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
mod tests {
    use crate::{RouterAppState, create_router};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use common::catalog::Catalog;
    use common::config::{ApiKeyConfig, Configuration, TenantConfig};
    use tower::ServiceExt;

    fn test_config() -> Configuration {
        let mut config = Configuration::default();
        config.auth = common::config::AuthConfig {
            tenants: vec![TenantConfig {
                id: "acme".to_string(),
                slug: "acme".to_string(),
                name: "Acme".to_string(),
                default_dataset: Some("default".to_string()),
                datasets: vec![],
                api_keys: vec![ApiKeyConfig {
                    key: "sk-test-key".to_string(),
                    name: Some("test".to_string()),
                }],
                schema_config: None,
                limits: None,
            }],
            ..Default::default()
        };
        config
    }

    async fn test_app() -> axum::Router {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        create_router(RouterAppState::new(catalog, test_config()))
    }

    fn ir_body() -> Body {
        Body::from(
            serde_json::to_vec(&serde_json::json!({
                "irVersion": 1,
                "from": "logs",
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
}
