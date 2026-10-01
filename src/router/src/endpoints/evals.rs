//! # Eval results upload (`POST /api/v1/evals/results`)
//!
//! Validates a JSONL/CSV results file ([`common::evals::upload`]) and writes
//! it as `gen_ai.evaluation.result` log records through the logs ingest path;
//! behaviour and guarantees are in docs/users/evaluations.md.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    Json, Router,
    body::Bytes,
    extract::{
        DefaultBodyLimit, Query, State,
        rejection::{BytesRejection, QueryRejection},
    },
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use common::evals::upload::{
    FileErrors, ResultsFormat, RunMetadata, UploadSummary, parse_results, summarize,
    to_logs_request,
};
use common::processors::CompiledProcessor;
use common::processors::apply::{self, ApplyError};
use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::RouterAppState;
use crate::endpoints::api_error::{ApiError, ApiErrorBody, ApiErrorDetail};
use crate::endpoints::eval_sets::{EVAL_SET_BODY_LIMIT, EvalsWrite};
use crate::endpoints::links::{API_V1, Link};

/// How long the writer may take to make an upload durable.
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

pub fn router() -> Router<RouterAppState> {
    Router::new()
        .route("/evals/results", post(upload_eval_results))
        .layer(DefaultBodyLimit::max(EVAL_SET_BODY_LIMIT))
}

/// Query parameters of `POST /api/v1/evals/results`: the run the file
/// belongs to.
#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct UploadEvalResultsParams {
    /// The agent the run evaluated (`gen_ai.agent.name`, and the records'
    /// `service.name`).
    pub agent: String,
    /// The agent version (`gen_ai.agent.version`, and `service.version`).
    pub version: String,
    /// The eval set the run replayed: a valid eval set name; the set need
    /// not exist.
    pub set: String,
    /// Run id (`signaldb.eval.run_id`); a UUID is generated when absent.
    /// Re-using a run id adds the file's results to that run.
    pub run_id: Option<String>,
    /// File format. Overrides the `Content-Type` (`text/csv` for CSV,
    /// `application/x-ndjson` or `application/jsonl` for JSONL); one of the
    /// two must name the format.
    pub format: Option<ResultsFormat>,
}

/// Links on an upload response.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct EvalResultsUploadLinks {
    /// The Query IR endpoint to read the run back: `logs` where
    /// `signaldb.eval.run_id` is the run id.
    pub query: Link,
    /// The Explore UI's Runs page (a UI path, not an API resource).
    pub runs: Link,
}

/// The run an upload wrote, with its per-evaluator summary.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct EvalResultsUploadResponse {
    pub run_id: String,
    pub agent: String,
    pub version: String,
    pub set: String,
    #[serde(flatten)]
    pub summary: UploadSummary,
    #[serde(rename = "_links")]
    pub links: EvalResultsUploadLinks,
}

fn bad_file(errors: FileErrors) -> ApiError {
    let details = errors
        .errors
        .iter()
        .map(|e| ApiErrorDetail {
            row: e.row,
            column: e.column.clone(),
            reason: e.reason.clone(),
        })
        .collect();
    ApiError::bad_request(errors.to_string()).with_details(details)
}

fn format_of(param: Option<ResultsFormat>, headers: &HeaderMap) -> Result<ResultsFormat, ApiError> {
    if let Some(format) = param {
        return Ok(format);
    }
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(ResultsFormat::from_content_type)
        .ok_or_else(|| {
            ApiError::bad_request(
                "cannot tell the file format: pass `format=csv|jsonl`, or send it as \
                 `text/csv` or `application/x-ndjson`",
            )
        })
}

fn processors_error(err: ApplyError) -> ApiError {
    match err {
        ApplyError::Unavailable { .. } => {
            tracing::error!(error = %err, "failed to load processors");
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "failed to load the tenant's processors",
            )
        }
        ApplyError::Rejected { processor, reason } => ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("processor `{processor}` rejected the results: {reason}"),
        ),
    }
}

/// The upload's `ingest_id`: a fingerprint of the tenant, dataset, run and
/// file bytes. A resend of the same file under the same run id carries the
/// same id, so the writer's ingest-id dedup drops it if the first copy
/// landed (the records' timestamps differ between attempts, so a batch
/// fingerprint would not match).
fn upload_ingest_id(
    tenant_id: &str,
    dataset_id: &str,
    run: &RunMetadata,
    format: ResultsFormat,
    body: &[u8],
) -> Uuid {
    let format: &[u8] = match format {
        ResultsFormat::Csv => b"csv",
        ResultsFormat::Jsonl => b"jsonl",
    };
    common::ingest_dedup::fingerprint(&[
        b"evals.results.upload",
        tenant_id.as_bytes(),
        dataset_id.as_bytes(),
        run.agent.as_bytes(),
        run.version.as_bytes(),
        run.set.as_bytes(),
        run.run_id.as_bytes(),
        format,
        body,
    ])
}

/// An upload ready to send to a writer.
struct Prepared {
    summary: UploadSummary,
    batch: RecordBatch,
    ingest_id: Uuid,
}

/// Fingerprint, parse, validate, summarize, apply the tenant's log
/// processors and convert to the logs Arrow batch. CPU-bound: runs on the
/// blocking pool. Each representation is dropped once the next is built, so
/// peak memory holds two of them, not all four.
fn prepare(
    body: Bytes,
    format: ResultsFormat,
    run: &RunMetadata,
    processors: &[Arc<CompiledProcessor>],
    tenant_id: &str,
    dataset_id: &str,
) -> Result<Prepared, ApiError> {
    let ingest_id = upload_ingest_id(tenant_id, dataset_id, run, format, &body);
    let text = std::str::from_utf8(&body)
        .map_err(|_| ApiError::bad_request("the results file must be UTF-8 text"))?;
    let rows = parse_results(text, format).map_err(bad_file)?;
    drop(body);
    let summary = summarize(&rows);
    let now = u64::try_from(super::now_ns()).unwrap_or_default();
    let mut request = to_logs_request(&rows, run, now);
    drop(rows);
    apply::run(processors, tenant_id, dataset_id, &mut request).map_err(processors_error)?;
    let batch = common::flight::conversion::otlp_logs_to_arrow(&request).map_err(|e| {
        tracing::error!(error = %e, "eval results conversion failed");
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to convert the results",
        )
    })?;
    Ok(Prepared {
        summary,
        batch,
        ingest_id,
    })
}

/// Sends the batch to a writer, which acks once it is in its WAL.
async fn write_logs(
    state: &RouterAppState,
    tenant_id: &str,
    dataset_id: &str,
    batch: RecordBatch,
    ingest_id: Uuid,
) -> Result<(), ApiError> {
    let metadata = serde_json::json!({
        "schema_version": "v1",
        "signal_type": "logs",
        "tenant_id": tenant_id,
        "dataset_id": dataset_id,
    })
    .to_string();
    let forward = state
        .service_registry()
        .forward_batch_to_writer(batch, &metadata, ingest_id);
    match tokio::time::timeout(WRITE_TIMEOUT, forward).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => match e.root_cause().downcast_ref::<tonic::Status>() {
            Some(status) => Err(ApiError::from_flight(status, "eval_results_upload")),
            None => {
                tracing::warn!(error = %e, "no writer reachable for eval results");
                Err(ApiError::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("no writer service is reachable: {e}"),
                ))
            }
        },
        Err(_) => Err(ApiError::new(
            StatusCode::GATEWAY_TIMEOUT,
            "the writer did not accept the results in time",
        )),
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/evals/results",
    tag = "evals",
    operation_id = "upload_eval_results",
    summary = "Upload a JSONL or CSV file of evaluator results as one offline run",
    description = "One row per evaluator result: `case_id` and `name` required; `score`, `label`, \
`explanation`, `trace_id`, `span_id`, `evaluator`, `error` and `trial` optional. The whole file is \
validated first: any invalid row rejects it with a `400` listing every problem in `details` \
(at most 100), and nothing is written. Each row becomes a `gen_ai.evaluation.result` log record \
carrying the run attributes, written through the log ingest path. See docs/users/evaluations.md \
(\"Upload a results file\").",
    params(UploadEvalResultsParams),
    request_body(
        content = String,
        content_type = "text/plain",
        description = "The results file: JSONL (one JSON object per line) or CSV with a header \
row. Sent as `text/csv` or `application/x-ndjson`, or with any type plus the `format` parameter."
    ),
    responses(
        (status = 201, description = "Results written; the run and its per-evaluator summary", body = EvalResultsUploadResponse),
        (status = 400, description = "Invalid run metadata, unknown format, a file that is not UTF-8, or invalid rows (listed in `details`)", body = ApiErrorBody),
        (status = 403, description = "Missing evals:write scope, or a session without the tenant-admin role", body = ApiErrorBody),
        (status = 413, description = "Body exceeds the 32 MiB limit", body = ApiErrorBody),
        (status = 422, description = "A tenant log processor rejected the results", body = ApiErrorBody),
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 500, description = "Internal error", body = ApiErrorBody),
        (status = 503, description = "No writer service available", body = ApiErrorBody),
        (status = 504, description = "The writer did not accept the results in time", body = ApiErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn upload_eval_results(
    State(state): State<RouterAppState>,
    EvalsWrite(ctx): EvalsWrite,
    params: Result<Query<UploadEvalResultsParams>, QueryRejection>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Result<Response, ApiError> {
    let Query(params) = params.map_err(|e| ApiError::bad_request(e.body_text()))?;
    let body = body.map_err(|e| ApiError::new(e.status(), e.body_text()))?;
    let format = format_of(params.format, &headers)?;
    let run = RunMetadata::new(
        &params.agent,
        &params.version,
        &params.set,
        params.run_id.as_deref(),
    )
    .map_err(ApiError::bad_request)?;
    let processors = apply::load(
        &state.processor_registry(),
        &ctx.tenant_id,
        &ctx.dataset_id,
        "logs",
    )
    .await
    .map_err(processors_error)?;

    let span = tracing::Span::current();
    let (tenant_id, dataset_id) = (ctx.tenant_id.clone(), ctx.dataset_id.clone());
    let (run, prepared) = tokio::task::spawn_blocking(move || {
        let _entered = span.enter();
        let prepared = prepare(body, format, &run, &processors, &tenant_id, &dataset_id);
        (run, prepared)
    })
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "eval results preparation task failed");
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to process the results",
        )
    })?;
    let Prepared {
        summary,
        batch,
        ingest_id,
    } = prepared?;
    write_logs(&state, &ctx.tenant_id, &ctx.dataset_id, batch, ingest_id).await?;
    tracing::info!(
        tenant_id = %ctx.tenant_id,
        dataset = %ctx.dataset_id,
        run_id = %run.run_id,
        set = %run.set,
        rows = summary.rows,
        evaluators = summary.evaluators.len(),
        "eval results uploaded"
    );
    let RunMetadata {
        agent,
        version,
        set,
        run_id,
    } = run;
    Ok((
        StatusCode::CREATED,
        Json(EvalResultsUploadResponse {
            run_id,
            agent,
            version,
            set,
            summary,
            links: EvalResultsUploadLinks {
                query: Link::with_method(format!("{API_V1}/query"), "POST"),
                runs: Link::get("/evals/runs".to_string()),
            },
        }),
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_flight::FlightData;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use common::auth::Authenticator;
    use common::catalog::Catalog;
    use common::config::{ApiKeyConfig, AuthConfig, Configuration, DatasetConfig, TenantConfig};
    use common::flight::transport::InMemoryFlightTransport;
    use common::service_bootstrap::{ServiceBootstrap, ServiceType};
    use datafusion::arrow::array::{Array, StringArray};
    use futures::StreamExt;
    use serde_json::Value;
    use tokio::sync::Mutex;
    use tower::ServiceExt;

    use crate::{RouterAppState, create_router};

    type Puts = Arc<Mutex<Vec<Vec<FlightData>>>>;

    /// A writer stand-in that accepts every `do_put` and keeps its messages.
    #[derive(Clone)]
    struct CapturingWriter {
        puts: Puts,
    }

    type Stream<T> = futures::stream::BoxStream<'static, Result<T, tonic::Status>>;

    #[tonic::async_trait]
    impl arrow_flight::flight_service_server::FlightService for CapturingWriter {
        type HandshakeStream = Stream<arrow_flight::HandshakeResponse>;
        type ListFlightsStream = Stream<arrow_flight::FlightInfo>;
        type DoGetStream = Stream<FlightData>;
        type DoPutStream = Stream<arrow_flight::PutResult>;
        type DoExchangeStream = Stream<FlightData>;
        type DoActionStream = Stream<arrow_flight::Result>;
        type ListActionsStream = Stream<arrow_flight::ActionType>;

        async fn handshake(
            &self,
            _: tonic::Request<tonic::Streaming<arrow_flight::HandshakeRequest>>,
        ) -> Result<tonic::Response<Self::HandshakeStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("handshake"))
        }
        async fn list_flights(
            &self,
            _: tonic::Request<arrow_flight::Criteria>,
        ) -> Result<tonic::Response<Self::ListFlightsStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("list_flights"))
        }
        async fn get_flight_info(
            &self,
            _: tonic::Request<arrow_flight::FlightDescriptor>,
        ) -> Result<tonic::Response<arrow_flight::FlightInfo>, tonic::Status> {
            Err(tonic::Status::unimplemented("get_flight_info"))
        }
        async fn poll_flight_info(
            &self,
            _: tonic::Request<arrow_flight::FlightDescriptor>,
        ) -> Result<tonic::Response<arrow_flight::PollInfo>, tonic::Status> {
            Err(tonic::Status::unimplemented("poll_flight_info"))
        }
        async fn get_schema(
            &self,
            _: tonic::Request<arrow_flight::FlightDescriptor>,
        ) -> Result<tonic::Response<arrow_flight::SchemaResult>, tonic::Status> {
            Err(tonic::Status::unimplemented("get_schema"))
        }
        async fn do_get(
            &self,
            _: tonic::Request<arrow_flight::Ticket>,
        ) -> Result<tonic::Response<Self::DoGetStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("do_get"))
        }
        async fn do_put(
            &self,
            request: tonic::Request<tonic::Streaming<FlightData>>,
        ) -> Result<tonic::Response<Self::DoPutStream>, tonic::Status> {
            let mut stream = request.into_inner();
            let mut messages = Vec::new();
            while let Some(message) = stream.next().await {
                messages.push(message?);
            }
            self.puts.lock().await.push(messages);
            Ok(tonic::Response::new(futures::stream::empty().boxed()))
        }
        async fn do_exchange(
            &self,
            _: tonic::Request<tonic::Streaming<FlightData>>,
        ) -> Result<tonic::Response<Self::DoExchangeStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("do_exchange"))
        }
        async fn do_action(
            &self,
            _: tonic::Request<arrow_flight::Action>,
        ) -> Result<tonic::Response<Self::DoActionStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("do_action"))
        }
        async fn list_actions(
            &self,
            _: tonic::Request<arrow_flight::Empty>,
        ) -> Result<tonic::Response<Self::ListActionsStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("list_actions"))
        }
    }

    /// App for tenant `acme` (default dataset `production`) with a capturing
    /// writer. Scoped keys: `sk-read` (evals:read), `sk-write` (evals:read +
    /// evals:write).
    async fn app() -> (axum::Router, Puts) {
        let catalog = Catalog::new_in_memory().await.unwrap();
        let config = Configuration {
            auth: AuthConfig {
                tenants: vec![TenantConfig {
                    id: "acme".to_string(),
                    slug: "acme".to_string(),
                    name: "Acme Inc".to_string(),
                    default_dataset: Some("production".to_string()),
                    datasets: vec![DatasetConfig {
                        id: "production".to_string(),
                        slug: "production".to_string(),
                        is_default: true,
                        storage: None,
                    }],
                    api_keys: vec![ApiKeyConfig {
                        key: "acme-key".to_string(),
                        name: Some("legacy".to_string()),
                    }],
                    schema_config: None,
                    limits: None,
                }],
                ..Default::default()
            },
            ..Default::default()
        };
        catalog.sync_config_tenants(&config.auth).await.unwrap();
        for (key, scopes) in [
            ("sk-read", vec!["evals:read"]),
            ("sk-write", vec!["evals:read", "evals:write"]),
        ] {
            let scopes: Vec<String> = scopes.into_iter().map(String::from).collect();
            catalog
                .upsert_scoped_api_key(
                    "acme",
                    &Authenticator::hash_api_key(key),
                    Some(key),
                    None,
                    None,
                    Some(&scopes),
                    None,
                )
                .await
                .unwrap();
        }

        let puts = Puts::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let writer_addr = listener.local_addr().unwrap();
        let writer = CapturingWriter { puts: puts.clone() };
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(common::flight::flight_service_server(writer))
                .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
        );
        ServiceBootstrap::new_for_test_with_catalog(
            catalog.clone(),
            ServiceType::Writer,
            &writer_addr.to_string(),
        )
        .await
        .unwrap();
        let router_bootstrap = ServiceBootstrap::new_for_test_with_catalog(
            catalog.clone(),
            ServiceType::Router,
            "127.0.0.1:0",
        )
        .await
        .unwrap();
        let transport = InMemoryFlightTransport::new(router_bootstrap);
        let state = RouterAppState::new_with_flight_transport(catalog, config, transport);
        (create_router(state), puts)
    }

    async fn upload(
        app: &axum::Router,
        key: &str,
        query: &str,
        content_type: &str,
        body: &str,
    ) -> (StatusCode, Value) {
        let request = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/evals/results?{query}"))
            .header("authorization", format!("Bearer {key}"))
            .header("x-tenant-id", "acme")
            .header("content-type", content_type)
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    const RUN: &str = "agent=support-triage&version=v1.9.0&set=triage-golden&run_id=run-42";
    const TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";

    fn csv() -> String {
        format!(
            "case_id,name,score,label,trace_id,error\n\
             case-1,Correctness,0.75,,{TRACE},\n\
             case-2,Correctness,0.25,,{TRACE},\n\
             case-3,Correctness,,,,timeout\n\
             case-1,Tone,,friendly,,\n"
        )
    }

    #[tokio::test]
    async fn a_read_only_key_is_forbidden_and_nothing_is_written() {
        let (app, puts) = app().await;
        let (status, body) = upload(&app, "sk-read", RUN, "text/csv", &csv()).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["errorType"], "forbidden");
        assert!(puts.lock().await.is_empty());
    }

    #[tokio::test]
    async fn a_missing_column_is_a_400_naming_it_and_nothing_is_written() {
        let (app, puts) = app().await;
        let (status, body) = upload(
            &app,
            "sk-write",
            RUN,
            "text/csv",
            "name,score\nCorrectness,0.5\n",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["errorType"], "bad_data");
        assert!(
            body["error"].as_str().unwrap().contains("case_id"),
            "{body}"
        );
        assert_eq!(body["details"][0]["column"], "case_id");
        assert!(body["details"][0].get("row").is_none(), "{body}");
        assert!(puts.lock().await.is_empty());
    }

    #[tokio::test]
    async fn invalid_rows_are_listed_in_details() {
        let (app, puts) = app().await;
        let jsonl = "{\"case_id\": \"a\", \"name\": \"C\", \"score\": \"high\"}\n\
                     {\"case_id\": \"b\", \"name\": \"C\", \"score\": 1, \"trace_id\": \"nope\"}\n";
        let (status, body) = upload(
            &app,
            "sk-write",
            &format!("{RUN}&format=jsonl"),
            "text/plain",
            jsonl,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        let details = body["details"].as_array().expect("details");
        assert_eq!(details.len(), 2, "{body}");
        assert_eq!(details[0]["row"], 1);
        assert_eq!(details[0]["column"], "score");
        assert_eq!(details[1]["row"], 2);
        assert_eq!(details[1]["column"], "trace_id");
        assert!(puts.lock().await.is_empty());
    }

    #[tokio::test]
    async fn bad_metadata_or_an_unknown_format_is_a_400() {
        let (app, _) = app().await;
        let (status, body) = upload(&app, "sk-write", RUN, "text/plain", &csv()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body["error"].as_str().unwrap().contains("format"), "{body}");

        let (status, body) = upload(
            &app,
            "sk-write",
            "agent=a&version=v1&set=Not%20A%20Slug",
            "text/csv",
            &csv(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body["error"].as_str().unwrap().contains("`set`"), "{body}");

        let (status, body) = upload(&app, "sk-write", "agent=a", "text/csv", &csv()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["errorType"], "bad_data");
    }

    #[tokio::test]
    async fn a_valid_file_is_written_as_log_records_and_summarized() {
        let (app, puts) = app().await;
        let (status, body) = upload(&app, "sk-write", RUN, "text/csv", &csv()).await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert_eq!(body["run_id"], "run-42");
        assert_eq!(body["agent"], "support-triage");
        assert_eq!(body["set"], "triage-golden");
        assert_eq!(body["rows"], 4);
        assert_eq!(body["cases"], 3);
        assert_eq!(body["span_linked"], 2);
        assert_eq!(body["run_level"], 2);
        assert_eq!(
            body["evaluators"][0],
            serde_json::json!({"name": "Correctness", "results": 3, "errors": 1,
                               "mean": 0.5, "pass_rate": 0.5})
        );
        assert_eq!(body["evaluators"][1]["pass_rate"], Value::Null);
        assert_eq!(body["_links"]["runs"]["href"], "/evals/runs");
        assert_eq!(body["_links"]["query"]["method"], "POST");

        let puts = puts.lock().await;
        assert_eq!(puts.len(), 1, "one DoPut");
        let metadata: Value = serde_json::from_slice(&puts[0][0].app_metadata).unwrap();
        assert_eq!(metadata["signal_type"], "logs");
        assert_eq!(metadata["tenant_id"], "acme");
        assert_eq!(metadata["dataset_id"], "production");
        let batches = common::flight::decode::flight_data_vec_to_batches(puts[0].clone())
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 4);
        let events = batches[0]
            .column_by_name("event_name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("event_name column");
        assert!(
            (0..events.len()).all(|i| events.value(i) == "gen_ai.evaluation.result"),
            "{events:?}"
        );
    }

    fn ingest_id(put: &[FlightData]) -> String {
        let metadata: Value = serde_json::from_slice(&put[0].app_metadata).unwrap();
        metadata["ingest_id"]
            .as_str()
            .expect("ingest_id")
            .to_string()
    }

    #[tokio::test]
    async fn a_resend_of_the_same_file_and_run_carries_the_same_ingest_id() {
        let (app, puts) = app().await;
        for (query, body) in [
            (RUN.to_string(), csv()),
            (RUN.to_string(), csv()),
            (RUN.replace("run-42", "run-43"), csv()),
            (RUN.to_string(), csv().replace("0.75", "0.8")),
        ] {
            let (status, body) = upload(&app, "sk-write", &query, "text/csv", &body).await;
            assert_eq!(status, StatusCode::CREATED, "{body}");
        }
        let puts = puts.lock().await;
        let ids: Vec<String> = puts.iter().map(|put| ingest_id(put)).collect();
        assert_eq!(ids[0], ids[1], "a retry must be deduplicated by the writer");
        assert_ne!(ids[0], ids[2], "another run id is another write");
        assert_ne!(ids[0], ids[3], "another file is another write");
    }

    #[tokio::test]
    async fn without_a_run_id_one_is_generated() {
        let (app, _) = app().await;
        let (status, body) = upload(
            &app,
            "sk-write",
            "agent=a&version=v1&set=golden",
            "application/x-ndjson",
            "{\"case_id\": \"a\", \"name\": \"C\", \"label\": \"pass\"}\n",
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert!(uuid::Uuid::parse_str(body["run_id"].as_str().unwrap()).is_ok());
        assert_eq!(body["evaluators"][0]["pass_rate"], 1.0);
    }
}
