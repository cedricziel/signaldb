//! # Semconv span factories
//!
//! The single sanctioned construction path for boundary spans. Each factory
//! writes the OpenTelemetry semantic-convention attribute names exactly once
//! (as the literal field names `tracing` macros require) and encodes the
//! convention's span-name, kind, and status rules, so a call site cannot get
//! them wrong. Conformance is pinned by
//! `tests/span_factories_semconv.rs`; the literals are pinned against the
//! `opentelemetry-semantic-conventions` crate constants by unit tests below.
//!
//! Boundary spans (RPC server/client, DB client, background jobs) MUST be
//! opened through these factories; free-form `#[instrument(skip_all, ...)]`
//! spans remain fine for in-process (INTERNAL) work.

use tracing::Span;
use tracing::field::Empty;

/// Fully-qualified logical method names for the Arrow Flight protocol.
/// Flight has no semantic convention of its own; it is modeled as plain gRPC.
pub const FLIGHT_DO_GET: &str = "arrow.flight.protocol.FlightService/DoGet";
pub const FLIGHT_DO_PUT: &str = "arrow.flight.protocol.FlightService/DoPut";
pub const FLIGHT_DO_ACTION: &str = "arrow.flight.protocol.FlightService/DoAction";
pub const FLIGHT_GET_FLIGHT_INFO: &str = "arrow.flight.protocol.FlightService/GetFlightInfo";

/// Which side of an RPC a span describes — determines the status mapping.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcBoundary {
    Server,
    Client,
}

/// SERVER span for an inbound gRPC/Flight call, per the RPC semantic
/// conventions (post-1.39 names): named by the fully-qualified logical
/// method, optionally disambiguated by a low-cardinality detail such as the
/// Flight ticket verb (the RPC naming rules explicitly permit
/// system-specific formats).
///
/// The caller is responsible for parent adoption
/// ([`crate::flight::trace_context`], before first enter) and for recording
/// the outcome via [`record_rpc_result`].
pub fn rpc_server_span(rpc_method: &str, detail: Option<&str>) -> Span {
    let span = tracing::info_span!(
        "rpc.server",
        otel.name = Empty,
        otel.kind = "server",
        otel.status_code = Empty,
        otel.status_message = Empty,
        rpc.system.name = "grpc",
        rpc.method = %rpc_method,
        rpc.response.status_code = Empty,
        error.r#type = Empty,
        signaldb.flight.ticket_verb = Empty,
    );
    match detail {
        Some(detail) => {
            span.record("otel.name", format!("{rpc_method} {detail}").as_str());
            span.record("signaldb.flight.ticket_verb", detail);
        }
        None => {
            span.record("otel.name", rpc_method);
        }
    }
    span
}

/// CLIENT span wrapping an outbound gRPC/Flight call (the whole logical
/// call, retries included). `server_address` is `host:port` when known.
///
/// Record the outcome via [`record_rpc_result`]; per RPC semconv, clients
/// treat every non-OK status as an error (unlike servers).
pub fn rpc_client_span(rpc_method: &str, server_address: Option<&str>) -> Span {
    let span = tracing::info_span!(
        "rpc.client",
        otel.name = %rpc_method,
        otel.kind = "client",
        otel.status_code = Empty,
        otel.status_message = Empty,
        rpc.system.name = "grpc",
        rpc.method = %rpc_method,
        rpc.response.status_code = Empty,
        error.r#type = Empty,
        server.address = Empty,
        server.port = Empty,
    );
    if let Some(addr) = server_address {
        match addr.rsplit_once(':') {
            Some((host, port)) if port.chars().all(|c| c.is_ascii_digit()) => {
                span.record("server.address", host);
                span.record("server.port", port);
            }
            _ => {
                span.record("server.address", addr);
            }
        }
    }
    span
}

/// Record an RPC outcome on a factory span: the string gRPC status code
/// always, and — per the RPC semconv asymmetry — span status Error plus
/// `error.type` only when the code is a failure *for this boundary*:
/// servers fail only on server-fault codes (a `NOT_FOUND` is the caller's
/// problem), clients fail on every non-OK code.
pub fn record_rpc_result(span: &Span, boundary: RpcBoundary, code: tonic::Code) {
    let code_str = grpc_code_name(code);
    span.record("rpc.response.status_code", code_str);

    let is_error = match boundary {
        RpcBoundary::Server => matches!(
            code,
            tonic::Code::Unknown
                | tonic::Code::DeadlineExceeded
                | tonic::Code::Unimplemented
                | tonic::Code::Internal
                | tonic::Code::Unavailable
                | tonic::Code::DataLoss
        ),
        RpcBoundary::Client => code != tonic::Code::Ok,
    };
    if is_error {
        span.record("otel.status_code", "ERROR");
        span.record("error.type", code_str);
    }
}

/// Canonical gRPC status-code name, as `rpc.response.status_code` wants it.
pub fn grpc_code_name(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Ok => "OK",
        tonic::Code::Cancelled => "CANCELLED",
        tonic::Code::Unknown => "UNKNOWN",
        tonic::Code::InvalidArgument => "INVALID_ARGUMENT",
        tonic::Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        tonic::Code::NotFound => "NOT_FOUND",
        tonic::Code::AlreadyExists => "ALREADY_EXISTS",
        tonic::Code::PermissionDenied => "PERMISSION_DENIED",
        tonic::Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        tonic::Code::FailedPrecondition => "FAILED_PRECONDITION",
        tonic::Code::Aborted => "ABORTED",
        tonic::Code::OutOfRange => "OUT_OF_RANGE",
        tonic::Code::Unimplemented => "UNIMPLEMENTED",
        tonic::Code::Internal => "INTERNAL",
        tonic::Code::Unavailable => "UNAVAILABLE",
        tonic::Code::DataLoss => "DATA_LOSS",
        tonic::Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

/// CLIENT span for a SQL-catalog operation, per the stable database
/// semantic conventions. Named `{db.operation.name} {db.namespace}` — never
/// raw SQL (query text, if recorded at all, must be sanitized first).
pub fn db_client_span(system: &str, operation: &str, namespace: &str) -> Span {
    tracing::info_span!(
        "db.client",
        otel.name = %format!("{operation} {namespace}"),
        otel.kind = "client",
        otel.status_code = Empty,
        otel.status_message = Empty,
        db.system.name = %system,
        db.operation.name = %operation,
        db.namespace = %namespace,
        error.r#type = Empty,
    )
}

/// Root INTERNAL span for a background job run (compactor lifecycle, writer
/// batch processing). Named by the low-cardinality job kind; tenancy and
/// affected-object detail live in `signaldb.*` attributes. Callers add
/// WAL-origin links via [`crate::flight::trace_context::add_link_from_fields`].
pub fn job_span(job_kind: &str, tenant_id: &str, dataset_id: &str, table: Option<&str>) -> Span {
    let span = tracing::info_span!(
        "signaldb.job",
        otel.name = %job_kind,
        otel.kind = "internal",
        otel.status_code = Empty,
        otel.status_message = Empty,
        error.r#type = Empty,
        signaldb.job.kind = %job_kind,
        signaldb.tenant.id = %tenant_id,
        signaldb.dataset.id = %dataset_id,
        signaldb.table = Empty,
    );
    if let Some(table) = table {
        span.record("signaldb.table", table);
    }
    span
}

#[cfg(test)]
mod tests {
    use opentelemetry_semantic_conventions::attribute;

    /// The macro field names above must be literals; these pins keep them
    /// from drifting apart from the semconv crate's constants.
    #[test]
    fn literal_field_names_match_semconv_constants() {
        assert_eq!("rpc.system.name", attribute::RPC_SYSTEM_NAME);
        assert_eq!("rpc.method", attribute::RPC_METHOD);
        assert_eq!(
            "rpc.response.status_code",
            attribute::RPC_RESPONSE_STATUS_CODE
        );
        assert_eq!("error.type", attribute::ERROR_TYPE);
        assert_eq!("server.address", attribute::SERVER_ADDRESS);
        assert_eq!("server.port", attribute::SERVER_PORT);
        assert_eq!("db.system.name", attribute::DB_SYSTEM_NAME);
        assert_eq!("db.operation.name", attribute::DB_OPERATION_NAME);
        assert_eq!("db.namespace", attribute::DB_NAMESPACE);
    }

    #[test]
    fn flight_methods_are_fully_qualified() {
        for m in [
            super::FLIGHT_DO_GET,
            super::FLIGHT_DO_PUT,
            super::FLIGHT_DO_ACTION,
            super::FLIGHT_GET_FLIGHT_INFO,
        ] {
            assert!(m.starts_with("arrow.flight.protocol.FlightService/"));
        }
    }
}
