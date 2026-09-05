//! # Tool-call audit, span, metrics, and concurrency bound
//!
//! Everything the dispatch wrapper around `tools/call` needs (issue #629):
//! the identity a call is audited under, the outcome classification, the one
//! structured audit event per call, the metrics, and the per-session permit
//! that bounds in-flight calls. `server.rs` owns the wrapper itself
//! (`ServerHandler::call_tool`); this module owns the policy so it is
//! unit-testable without a transport.
//!
//! Audit events name the tool, tenant, dataset, session, outcome, and
//! duration — never arguments, query text, or results. Denied calls log at
//! `warn` (probing must be visible), failures at `error`, everything else at
//! `info`.

use std::time::Duration;

use axum::http::request::Parts;
use rmcp::ErrorData;
use rmcp::RoleServer;
use rmcp::model::{CallToolRequestParams, CallToolResponse, CallToolResult};
use rmcp::service::RequestContext;

/// Default bound on tool calls in flight per session
/// (`[mcp].max_concurrent_tool_calls`).
pub const DEFAULT_MAX_CONCURRENT_TOOL_CALLS: usize = 8;

/// How long a call at the concurrency bound waits for a permit before it
/// fails distinctly instead of queueing indefinitely.
pub const PERMIT_WAIT: Duration = Duration::from_secs(2);

/// Session id recorded for the stdio transport, which has no MCP session.
pub const STDIO_SESSION_ID: &str = "stdio";

/// Key under `ErrorData::data` carrying the router's HTTP status when a tool
/// error came from a downstream response — the classification source for
/// `denied` (401/403) and `throttled` (429).
pub const HTTP_STATUS_DATA_KEY: &str = "http_status";

/// Key under `ErrorData::data` naming a locally classified error type (e.g.
/// `concurrency_limit`), taking precedence over the JSON-RPC code.
pub const ERROR_TYPE_DATA_KEY: &str = "error_type";

/// `error.type` for a call refused at the per-session concurrency bound.
pub const CONCURRENCY_LIMIT_ERROR_TYPE: &str = "concurrency_limit";

/// `error.type` for a call still running past its total per-`tools/call`
/// deadline (see `crate::tool_call_deadline`).
pub const DEADLINE_ERROR_TYPE: &str = "deadline";

/// `_meta` key marking a result whose payload was cut at the size cap, so the
/// wrapper can classify it `truncated` without re-parsing the content.
pub const TRUNCATED_META_KEY: &str = "dev.signaldb/truncated";

/// Message prefix a later change uses for a call that failed after the router
/// throttled it past the SDK's retry budget.
const THROTTLED_MESSAGE_PREFIX: &str = "throttled:";

/// The tenant the router resolved the caller to, inserted into the request
/// extensions by the auth middleware so the audit names the tenant even for
/// OAuth credentials (which carry no `X-Tenant-ID` header).
#[derive(Clone, Debug)]
pub struct CallerTenant(pub String);

/// The credential's own dataset-set restriction, inserted into the request
/// extensions by the auth middleware alongside [`CallerTenant`] (from the
/// same `whoami()` call, `WhoamiIdentityResponse::dataset_ids`). `None`
/// means the credential is unrestricted. Tools that list datasets/tables
/// (`discover_datasets`, `tenant_list_tables`) read this to filter out any
/// dataset the caller's credential cannot reach, per design D10 —
/// unlisted datasets must not be visible even by name.
#[derive(Clone, Debug, Default)]
pub struct CallerDatasetIds(pub Option<Vec<String>>);

/// The distinct error a call receives when its session is at the concurrency
/// bound and no permit frees up within [`PERMIT_WAIT`].
pub fn concurrency_limit_error(limit: usize) -> ErrorData {
    ErrorData::invalid_request(
        format!(
            "too many concurrent tool calls (limit {limit}); wait for in-flight calls to finish"
        ),
        Some(serde_json::json!({
            "limit": limit,
            ERROR_TYPE_DATA_KEY: CONCURRENCY_LIMIT_ERROR_TYPE,
        })),
    )
}

/// The distinct error a call receives when it is still running after the
/// total per-`tools/call` deadline (`crate::tool_call_deadline`) elapses:
/// the per-attempt `.timeout` in `sdk_client_for` bounds one HTTP attempt,
/// but up to `max_attempts` attempts plus the retry policy's `total_cap` of
/// sleeps between them can otherwise keep a single tool call alive far
/// longer than any one attempt's timeout suggests.
pub fn deadline_exceeded_error(deadline: Duration) -> ErrorData {
    ErrorData::internal_error(
        format!(
            "tool call exceeded the {}s deadline",
            deadline.as_secs_f64()
        ),
        Some(serde_json::json!({ ERROR_TYPE_DATA_KEY: DEADLINE_ERROR_TYPE })),
    )
}

/// Attach the downstream HTTP status to a tool error's `data` so the audit
/// wrapper can classify it (`denied`, `throttled`) without matching message
/// text.
pub fn with_http_status(mut err: ErrorData, status: u16) -> ErrorData {
    let mut data = match err.data.take() {
        Some(serde_json::Value::Object(map)) => map,
        _ => serde_json::Map::new(),
    };
    data.insert(HTTP_STATUS_DATA_KEY.to_string(), status.into());
    err.data = Some(serde_json::Value::Object(data));
    err
}

/// Mark a result as truncated at the size cap (see [`TRUNCATED_META_KEY`]).
pub fn mark_truncated(mut result: CallToolResult) -> CallToolResult {
    let mut meta = result.meta.take().unwrap_or_default();
    meta.0.insert(TRUNCATED_META_KEY.to_string(), true.into());
    result.meta = Some(meta);
    result
}

/// The audit classification of one tool call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    Truncated,
    Denied,
    Throttled,
    Error { error_type: String },
}

impl Outcome {
    /// Classify a finished call. Errors carry the classification hints
    /// [`with_http_status`] / [`concurrency_limit_error`] attach; a
    /// `CallToolResult` with `isError` is a `tool_error` per the MCP
    /// conventions.
    pub fn of(result: &Result<CallToolResponse, ErrorData>) -> Self {
        match result {
            Ok(CallToolResponse::Complete(result)) => Self::of_result(result),
            Ok(_) => Self::Ok,
            Err(err) => Self::of_error(err),
        }
    }

    fn of_result(result: &CallToolResult) -> Self {
        if result.is_error == Some(true) {
            return Self::Error {
                error_type: "tool_error".to_string(),
            };
        }
        let truncated = result
            .meta
            .as_ref()
            .and_then(|m| m.0.get(TRUNCATED_META_KEY))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if truncated { Self::Truncated } else { Self::Ok }
    }

    fn of_error(err: &ErrorData) -> Self {
        let data = err.data.as_ref().and_then(|v| v.as_object());
        if let Some(error_type) = data
            .and_then(|d| d.get(ERROR_TYPE_DATA_KEY))
            .and_then(|v| v.as_str())
        {
            return Self::Error {
                error_type: error_type.to_string(),
            };
        }
        let http_status = data
            .and_then(|d| d.get(HTTP_STATUS_DATA_KEY))
            .and_then(|v| v.as_u64());
        match http_status {
            Some(401) | Some(403) => return Self::Denied,
            Some(429) => return Self::Throttled,
            Some(status) => {
                return Self::Error {
                    error_type: status.to_string(),
                };
            }
            None => {}
        }
        if err.message.starts_with(THROTTLED_MESSAGE_PREFIX) || err.message.contains("rate limited")
        {
            return Self::Throttled;
        }
        // Per the MCP conventions, `error.type` is the JSON-RPC error code.
        Self::Error {
            error_type: err.code.0.to_string(),
        }
    }

    /// The bounded label used on the audit event and metrics.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Truncated => "truncated",
            Self::Denied => "denied",
            Self::Throttled => "throttled",
            Self::Error { .. } => "error",
        }
    }

    /// The classified `error.type`, present only for `error`.
    pub fn error_type(&self) -> Option<&str> {
        match self {
            Self::Error { error_type } => Some(error_type),
            _ => None,
        }
    }
}

/// The identity one tool call is audited under, captured before dispatch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditContext {
    pub tool: String,
    pub tenant_id: String,
    pub dataset: Option<String>,
    pub session_id: String,
}

impl AuditContext {
    /// Capture tool, tenant, dataset, and session from the request and its
    /// HTTP `Parts` (absent on stdio). The dataset is the tool's explicit
    /// `dataset` argument when given, else the caller's `X-Dataset-ID`.
    pub fn capture(request: &CallToolRequestParams, context: &RequestContext<RoleServer>) -> Self {
        let parts = context.extensions.get::<Parts>();
        let header = |name: &str| -> Option<String> {
            parts
                .and_then(|p| p.headers.get(name))
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
        };
        let tenant_id = context
            .extensions
            .get::<CallerTenant>()
            .map(|t| t.0.clone())
            .or_else(|| parts.and_then(|p| p.extensions.get::<CallerTenant>().map(|t| t.0.clone())))
            .or_else(|| header("x-tenant-id"))
            .unwrap_or_else(|| "unknown".to_string());
        let dataset = request
            .arguments
            .as_ref()
            .and_then(|args| args.get("dataset"))
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .or_else(|| header("x-dataset-id"));
        let session_id = match parts {
            None => STDIO_SESSION_ID.to_string(),
            Some(_) => header("mcp-session-id").unwrap_or_else(|| "none".to_string()),
        };
        Self {
            tool: request.name.to_string(),
            tenant_id,
            dataset,
            session_id,
        }
    }

    /// Emit the one audit event for this call and record the metrics.
    /// `info` for `ok`/`truncated`/`throttled`, `warn` for `denied`, `error`
    /// for `error` (with `error.type`).
    pub fn finish(&self, outcome: &Outcome, duration: Duration) {
        let duration_ms = duration.as_millis() as u64;
        let tool = self.tool.as_str();
        let tenant_id = self.tenant_id.as_str();
        let dataset = self.dataset.as_deref();
        let session_id = self.session_id.as_str();
        match outcome {
            Outcome::Denied => tracing::warn!(
                target: "signaldb_mcp::audit",
                tool,
                tenant_id,
                dataset,
                session_id,
                outcome = outcome.as_str(),
                duration_ms,
                "MCP tool call denied"
            ),
            Outcome::Error { error_type } => tracing::error!(
                target: "signaldb_mcp::audit",
                tool,
                tenant_id,
                dataset,
                session_id,
                outcome = outcome.as_str(),
                duration_ms,
                error.r#type = error_type.as_str(),
                "MCP tool call failed"
            ),
            Outcome::Ok | Outcome::Truncated | Outcome::Throttled => tracing::info!(
                target: "signaldb_mcp::audit",
                tool,
                tenant_id,
                dataset,
                session_id,
                outcome = outcome.as_str(),
                duration_ms,
                "MCP tool call"
            ),
        }
        common::self_monitoring::app_metrics().record_mcp_tool_call(
            tool,
            outcome.as_str(),
            duration,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::model::ContentBlock;

    fn ok_result() -> Result<CallToolResponse, ErrorData> {
        Ok(CallToolResult::success(vec![ContentBlock::text("{}")]).into())
    }

    #[test]
    fn successful_result_is_ok() {
        assert_eq!(Outcome::of(&ok_result()), Outcome::Ok);
    }

    #[test]
    fn truncated_marker_classifies_as_truncated() {
        let result = mark_truncated(CallToolResult::success(vec![ContentBlock::text(
            r#"{"truncated":true}"#,
        )]));
        assert_eq!(Outcome::of(&Ok(result.into())), Outcome::Truncated);
    }

    #[test]
    fn tool_error_result_is_an_error() {
        let result = CallToolResult::error(vec![ContentBlock::text("boom")]);
        assert_eq!(
            Outcome::of(&Ok(result.into())),
            Outcome::Error {
                error_type: "tool_error".to_string()
            }
        );
    }

    #[test]
    fn router_401_and_403_are_denied() {
        for status in [401, 403] {
            let err = with_http_status(ErrorData::invalid_request("nope", None), status);
            assert_eq!(Outcome::of(&Err(err)), Outcome::Denied, "{status}");
        }
    }

    #[test]
    fn router_429_and_throttled_prefix_are_throttled() {
        let err = with_http_status(
            ErrorData::internal_error("search_logs: rate limited, retry shortly", None),
            429,
        );
        assert_eq!(Outcome::of(&Err(err)), Outcome::Throttled);
        let err = ErrorData::internal_error("throttled: retry budget exhausted", None);
        assert_eq!(Outcome::of(&Err(err)), Outcome::Throttled);
    }

    #[test]
    fn router_500_is_an_error_typed_by_status() {
        let err = with_http_status(ErrorData::internal_error("boom", None), 500);
        assert_eq!(
            Outcome::of(&Err(err)),
            Outcome::Error {
                error_type: "500".to_string()
            }
        );
    }

    #[test]
    fn concurrency_limit_error_is_typed() {
        let err = concurrency_limit_error(8);
        assert!(
            err.message
                .starts_with("too many concurrent tool calls (limit 8)"),
            "{}",
            err.message
        );
        assert_eq!(
            Outcome::of(&Err(err)),
            Outcome::Error {
                error_type: CONCURRENCY_LIMIT_ERROR_TYPE.to_string()
            }
        );
    }

    #[test]
    fn untyped_error_falls_back_to_the_jsonrpc_code() {
        let err = ErrorData::invalid_params("bad", None);
        assert_eq!(
            Outcome::of(&Err(err)),
            Outcome::Error {
                error_type: "-32602".to_string()
            }
        );
    }

    #[test]
    fn with_http_status_keeps_existing_data() {
        let err = with_http_status(
            ErrorData::internal_error("x", Some(serde_json::json!({"a": 1}))),
            503,
        );
        let data = err.data.unwrap();
        assert_eq!(data["a"], 1);
        assert_eq!(data[HTTP_STATUS_DATA_KEY], 503);
    }
}
