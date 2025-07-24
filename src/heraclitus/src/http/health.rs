use axum::{extract::State, http::StatusCode, response::Json};
use serde::{Deserialize, Serialize};

use super::HttpState;

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub uptime_seconds: u64,
    pub checks: HealthChecks,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthChecks {
    pub storage: bool,
    pub service_discovery: bool,
}

pub async fn health_handler(State(state): State<HttpState>) -> (StatusCode, Json<HealthResponse>) {
    let uptime = state.service_start_time.elapsed().as_secs();

    // TODO: Add actual health checks for storage and service discovery
    let response = HealthResponse {
        status: "healthy".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_seconds: uptime,
        checks: HealthChecks {
            storage: true,
            service_discovery: true,
        },
    };

    (StatusCode::OK, Json(response))
}
