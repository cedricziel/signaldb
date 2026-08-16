//! JSON error responses for the query HTTP surfaces.
//!
//! Loki- and Prometheus-style clients expect failures as
//! `{"status":"error","errorType":"...","error":"..."}`; a bare status code
//! with an empty body leaves UIs nothing to display. Rate-limit rejections
//! carry the same envelope plus `retryAfterMs` and the `Retry-After` /
//! `X-RateLimit-*` header trio, computed from the token bucket's actual
//! state (see [`common::ratelimit`]).

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use common::ratelimit::RateLimitExceeded;
use serde::Serialize;
use utoipa::ToSchema;

/// An HTTP error with a client-visible message.
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
    /// Set only by [`Self::rate_limited`]; carries the bucket state that
    /// [`IntoResponse`] renders as `retryAfterMs` in the body and the
    /// `Retry-After` / `X-RateLimit-*` headers, via
    /// [`common::ratelimit::retry_headers`] — the same helper the acceptor
    /// uses, so every SignalDB 429 answers identically.
    rate_limit: Option<RateLimitExceeded>,
}

impl ApiError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            rate_limit: None,
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }

    /// Build a `429` from a rejected [`RateLimitExceeded`]: status,
    /// message, `retryAfterMs` in the body, and the `Retry-After` /
    /// `X-RateLimit-Limit` / `X-RateLimit-Burst` headers on the response —
    /// the single shape every SignalDB rate-limit rejection uses.
    pub fn rate_limited(err: &RateLimitExceeded) -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            message: err.to_string(),
            rate_limit: Some(err.clone()),
        }
    }

    /// Map a querier Flight status to an HTTP error, preserving its message.
    pub fn from_flight(status: &tonic::Status, what: &str) -> Self {
        // When the caller runs inside a Flight CLIENT span
        // (`do_get_client_span`), record the failure on it per RPC semconv
        // (clients treat every non-OK code as an error). A no-op when the
        // current span is not an rpc.client span — the fields don't exist.
        common::self_monitoring::spans::record_rpc_result(
            &tracing::Span::current(),
            common::self_monitoring::spans::RpcBoundary::Client,
            status.code(),
        );
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
        Self {
            status,
            message,
            rate_limit: None,
        }
    }
}

/// The JSON envelope every query-surface error responds with: `status` is
/// always `"error"`, `errorType` a stable low-cardinality code, `error` a
/// human-readable message, and `retryAfterMs` present only on rate-limit
/// rejections. Exists as a real (rather than `serde_json::json!`-built)
/// type so the OpenAPI document can declare its schema on the `429`
/// response of every rate-limited operation.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ApiErrorBody {
    /// Always `"error"`.
    pub status: &'static str,
    #[serde(rename = "errorType")]
    pub error_type: String,
    pub error: String,
    /// Milliseconds until the request would be admitted; present only when
    /// `errorType` is `"rate_limited"`.
    #[serde(rename = "retryAfterMs", skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

/// Shared OpenAPI `429` response for every rate-limited query operation:
/// the [`ApiErrorBody`] envelope plus the `Retry-After` / `X-RateLimit-Limit`
/// / `X-RateLimit-Burst` header trio. Referenced via `response = RateLimited`
/// in each operation's `#[utoipa::path(responses(...))]` so the shape is
/// declared once instead of drifting per endpoint (admin's count-quota `429`
/// has no burst allowance and documents its own two-header shape inline).
pub struct RateLimited;

impl<'r> utoipa::ToResponse<'r> for RateLimited {
    fn response() -> (
        &'r str,
        utoipa::openapi::RefOr<utoipa::openapi::response::Response>,
    ) {
        use utoipa::openapi::{ContentBuilder, HeaderBuilder, Object, ResponseBuilder, Type};

        let response = ResponseBuilder::new()
            .description(
                "Rate limited: the tenant's request budget for this dimension is exhausted. \
                 `Retry-After` states whole seconds (rounded up, at least 1) until the request \
                 would be admitted, computed from the token bucket's actual state.",
            )
            .content(
                "application/json",
                ContentBuilder::new()
                    .schema(Some(<ApiErrorBody as utoipa::PartialSchema>::schema()))
                    .build(),
            )
            .header(
                "Retry-After",
                HeaderBuilder::new()
                    .schema(Object::with_type(Type::Integer))
                    .description(Some(
                        "Whole seconds (rounded up, at least 1) until the request would be admitted",
                    ))
                    .build(),
            )
            .header(
                "X-RateLimit-Limit",
                HeaderBuilder::new()
                    .schema(Object::with_type(Type::Integer))
                    .description(Some(
                        "The tenant's per-second budget for the rejected dimension",
                    ))
                    .build(),
            )
            .header(
                "X-RateLimit-Burst",
                HeaderBuilder::new()
                    .schema(Object::with_type(Type::Integer))
                    .description(Some(
                        "The tenant's burst allowance for the rejected dimension",
                    ))
                    .build(),
            )
            .build();
        ("RateLimited", response.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = ApiErrorBody {
            status: "error",
            error_type: self.error_type().to_string(),
            error: self.message,
            retry_after_ms: self
                .rate_limit
                .as_ref()
                .map(|err| err.retry_after_secs().saturating_mul(1_000)),
        };
        let mut response = (self.status, Json(body)).into_response();
        if let Some(err) = &self.rate_limit {
            let headers = response.headers_mut();
            for (name, value) in common::ratelimit::retry_headers(err) {
                headers.insert(name, value);
            }
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::ratelimit::RateLimitKind;
    use std::time::Duration;

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

    fn sample_rate_limit_exceeded() -> RateLimitExceeded {
        RateLimitExceeded {
            tenant_id: "acme".to_string(),
            kind: RateLimitKind::QueryRequests,
            retry_after: Duration::from_millis(2_500),
            limit: 100.0,
            burst: 1_000.0,
        }
    }

    #[tokio::test]
    async fn rate_limited_renders_429_envelope_with_retry_after_ms_and_headers() {
        use axum::body::to_bytes;

        let response = ApiError::rate_limited(&sample_rate_limit_exceeded()).into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("application/json")
        );
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
            Some("3")
        );
        assert_eq!(
            response
                .headers()
                .get("x-ratelimit-limit")
                .and_then(|v| v.to_str().ok()),
            Some("100")
        );
        assert_eq!(
            response
                .headers()
                .get("x-ratelimit-burst")
                .and_then(|v| v.to_str().ok()),
            Some("1000")
        );

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "error");
        assert_eq!(json["errorType"], "rate_limited");
        assert_eq!(json["retryAfterMs"], 3_000);
    }

    #[tokio::test]
    async fn non_rate_limit_errors_carry_no_rate_limit_headers_or_field() {
        use axum::body::to_bytes;

        let response = ApiError::bad_request("nope").into_response();
        assert!(
            response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .is_none()
        );
        assert!(response.headers().get("x-ratelimit-limit").is_none());
        assert!(response.headers().get("x-ratelimit-burst").is_none());

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json.get("retryAfterMs").is_none());
    }
}
