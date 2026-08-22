//! # OTLP gRPC Services
//!
//! The four generated `tonic` service implementations
//! (`{Trace,Log,Metric,Profile}Service`) the acceptor's gRPC server
//! exposes. Each is a thin wrapper: extract the [`crate::handler`]-injected
//! `TenantContext` and scope from the request extensions (added by
//! `crate::middleware::GrpcAuthLayer`), enforce per-tenant rate limits and
//! storage quotas, delegate to the matching `crate::handler` for WAL
//! durability + Flight forward, classify the result into the right gRPC
//! status (`INVALID_ARGUMENT` vs `UNAVAILABLE`, see
//! `crate::handler::IngestError`), and record self-monitoring RPC metrics.
//! Each `*HandlerTrait` exists so tests can substitute a fake handler
//! without a real WAL/Flight transport.

pub mod otlp_log_service;
pub mod otlp_metric_service;
pub mod otlp_profile_service;
pub mod otlp_trace_service;
