use axum::{extract::State, http::StatusCode, response::Response};
use prometheus::{Encoder, TextEncoder};

use super::HttpState;

pub async fn metrics_handler(
    State(state): State<HttpState>,
) -> Result<Response<String>, StatusCode> {
    let encoder = TextEncoder::new();
    let metric_families = state.metrics_registry.gather();

    let mut buffer = vec![];
    encoder
        .encode(&metric_families, &mut buffer)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let response = String::from_utf8(buffer).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", encoder.format_type())
        .body(response)
        .unwrap())
}
