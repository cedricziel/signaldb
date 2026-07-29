//! JSON error responses for the query HTTP surfaces.
//!
//! Loki- and Prometheus-style clients expect failures as
//! `{"status":"error","errorType":"...","error":"..."}`; a bare status code
//! with an empty body leaves UIs nothing to display.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// An HTTP error with a client-visible message.
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
}

impl ApiError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }

    /// Map a querier Flight status to an HTTP error, preserving its message.
    pub fn from_flight(status: &tonic::Status, what: &str) -> Self {
        let code = match status.code() {
            tonic::Code::NotFound => StatusCode::NOT_FOUND,
            tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
            tonic::Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
            tonic::Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
            tonic::Code::PermissionDenied => StatusCode::FORBIDDEN,
            tonic::Code::Unimplemented => StatusCode::NOT_IMPLEMENTED,
            _ => {
                tracing::error!(error = %status, query_kind = what, "Flight query failed");
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        Self::new(code, status.message().to_string())
    }

    fn error_type(&self) -> &'static str {
        match self.status {
            StatusCode::BAD_REQUEST => "bad_data",
            StatusCode::NOT_FOUND => "not_found",
            StatusCode::TOO_MANY_REQUESTS => "rate_limited",
            StatusCode::GATEWAY_TIMEOUT => "timeout",
            StatusCode::SERVICE_UNAVAILABLE => "unavailable",
            StatusCode::NOT_IMPLEMENTED => "not_implemented",
            _ => "internal",
        }
    }
}

/// Statuses raised without further context keep their canonical reason as
/// the message, so `?` on existing `StatusCode` sites stays valid.
impl From<StatusCode> for ApiError {
    fn from(status: StatusCode) -> Self {
        let message = status
            .canonical_reason()
            .unwrap_or("request failed")
            .to_string();
        Self { status, message }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(json!({
            "status": "error",
            "errorType": self.error_type(),
            "error": self.message,
        }));
        (self.status, body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn carries_message_and_error_type() {
        let err = ApiError::bad_request("parse error at line 1");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert_eq!(err.error_type(), "bad_data");
        assert_eq!(err.message, "parse error at line 1");
    }

    #[test]
    fn from_flight_preserves_the_querier_message() {
        let status = tonic::Status::invalid_argument("unknown label foo");
        let err = ApiError::from_flight(&status, "logs");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert_eq!(err.message, "unknown label foo");
    }

    #[test]
    fn from_status_code_uses_the_canonical_reason() {
        let err = ApiError::from(StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(err.message, "Service Unavailable");
        assert_eq!(err.error_type(), "unavailable");
    }
}
