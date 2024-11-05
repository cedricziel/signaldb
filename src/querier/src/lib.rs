use axum::{
    extract::{Path, Query},
    routing::{get, post},
    Router,
};
use common::model::span::SpanBatch;
use datafusion::prelude::*;
use tokio::sync::oneshot;

pub async fn query() -> &'static str {
    "query"
}

/// GET /api/traces/<traceid>?start=<start>&end=<end>
#[tracing::instrument]
pub async fn query_single_trace(
    Path(trace_id): Path<String>,
    _start: Option<Query<String>>,
    _end: Option<Query<String>>,
) -> Result<axum::Json<serde_json::Value>, axum::http::StatusCode> {
    log::info!("Querying for trace_id: {}", trace_id);

    let ctx = SessionContext::new();
    ctx.register_parquet("traces", ".data/ds/traces", ParquetReadOptions::default())
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let query = format!("SELECT * FROM traces WHERE trace_id = '{}'", trace_id).to_string();

    let df = ctx
        .sql(&query)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let schema = df.schema().clone();
    let results = df
        .collect()
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    log::info!("Query returned {} rows", results.len());
    log::info!("Schema: {:?}", schema);
    log::info!("Results: {:?}", results);

    // return the results as JSON
    let json_results: Vec<serde_json::Value> = results
        .iter()
        .map(|batch| {
            let span_batch = SpanBatch::from_record_batch(batch);
            serde_json::to_value(span_batch).unwrap()
        })
        .collect();

    Ok(axum::Json(serde_json::Value::Array(json_results)))
}

pub fn query_router() -> Router {
    Router::new()
        .route("/", post(query))
        .route("/api/traces/:trace_id", get(query_single_trace))
}

pub async fn serve_querier_http(
    init_tx: oneshot::Sender<()>,
    shutdown_rx: oneshot::Receiver<()>,
    stopped_tx: oneshot::Sender<()>,
) -> Result<(), anyhow::Error> {
    let addr = "0.0.0.0:9000";

    log::info!("Starting querier on {}", addr);

    let app = query_router();

    init_tx
        .send(())
        .expect("Unable to send init signal for querier http server");

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            shutdown_rx.await.ok();

            log::info!("Shutting down querier http server");
        })
        .await
        .unwrap();
    stopped_tx
        .send(())
        .expect("Unable to send stopped signal for querier http server");

    Ok(())
}
