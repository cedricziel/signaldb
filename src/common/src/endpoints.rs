//! Shared constants for SignalDB's public-facing network surface.
//!
//! Default ports and OTLP/HTTP paths, reused by `[public]` config defaults
//! (`config::PublicEndpointsConfig`), the acceptor's route registrations and
//! CLI defaults, and the router's `GET /api/v1/connection` response, so the
//! wire contract is defined exactly once.

/// Default OTLP/gRPC ingest port.
pub const DEFAULT_OTLP_GRPC_PORT: u16 = 4317;
/// Default OTLP/HTTP ingest port.
pub const DEFAULT_OTLP_HTTP_PORT: u16 = 4318;
/// Default router HTTP API port.
pub const DEFAULT_ROUTER_HTTP_PORT: u16 = 3000;

/// OTLP/HTTP traces export path.
pub const OTLP_HTTP_TRACES_PATH: &str = "/v1/traces";
/// OTLP/HTTP logs export path.
pub const OTLP_HTTP_LOGS_PATH: &str = "/v1/logs";
/// OTLP/HTTP metrics export path.
pub const OTLP_HTTP_METRICS_PATH: &str = "/v1/metrics";
/// OTLP/HTTP profiles export path — the OTLP development endpoint for the
/// profiles signal, which has no stable OTLP/HTTP path yet.
pub const OTLP_HTTP_PROFILES_PATH: &str = "/v1development/profiles";

/// Prometheus remote-write ingest path.
pub const PROMETHEUS_REMOTE_WRITE_PATH: &str = "/api/v1/write";
