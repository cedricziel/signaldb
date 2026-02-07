//! Data model types for TUI client responses.

/// Parameters for searching traces.
#[derive(Debug, Clone, Default)]
pub struct TraceSearchParams {
    /// Tag filter expression (e.g. `service.name=frontend`)
    pub tags: Option<String>,
    /// Minimum span duration filter (e.g. `100ms`)
    pub min_duration: Option<String>,
    /// Maximum span duration filter (e.g. `5s`)
    pub max_duration: Option<String>,
    /// Maximum number of traces to return
    pub limit: Option<u32>,
    /// Start of time range as nanoseconds since epoch.
    pub start_time_nanos: Option<u64>,
    /// End of time range as nanoseconds since epoch.
    pub end_time_nanos: Option<u64>,
}

/// A single trace result from a search query.
#[derive(Debug, Clone)]
pub struct TraceResult {
    /// Hex-encoded trace ID
    pub trace_id: String,
    /// Service name of the root span
    pub root_service: String,
    /// Operation name of the root span
    pub root_operation: String,
    /// Total trace duration in milliseconds
    pub duration_ms: f64,
    /// Number of spans in the trace
    pub span_count: u32,
    /// Human-readable start time
    pub start_time: String,
}

/// Information about a single span within a trace.
#[derive(Debug, Clone)]
pub struct SpanInfo {
    /// Hex-encoded span ID
    pub span_id: String,
    /// Parent span ID, if any
    pub parent_span_id: Option<String>,
    /// Span operation name
    pub operation: String,
    /// Service that produced this span
    pub service: String,
    /// Start time in milliseconds since trace start
    pub start_time_ms: f64,
    /// Span duration in milliseconds
    pub duration_ms: f64,
    /// Span status (e.g. `Ok`, `Error`)
    pub status: String,
    /// Span attributes as a JSON value
    pub attributes: serde_json::Value,
}

/// Full detail for a single trace, including all spans.
#[derive(Debug, Clone)]
pub struct TraceDetail {
    /// Hex-encoded trace ID
    pub trace_id: String,
    /// All spans belonging to this trace
    pub spans: Vec<SpanInfo>,
}

/// A system-level metric data point.
#[derive(Debug, Clone)]
pub struct SystemMetric {
    /// Metric name
    pub name: String,
    /// Metric value
    pub value: f64,
    /// Unit of measurement
    pub unit: String,
    /// ISO-8601 timestamp
    pub timestamp: String,
}

/// Health status of a discovered service.
#[derive(Debug, Clone)]
pub struct ServiceHealth {
    /// Service identifier
    pub service_id: String,
    /// Service type (e.g. `writer`, `querier`)
    pub service_type: String,
    /// Network address
    pub address: String,
    /// Whether the service is healthy
    pub healthy: bool,
    /// Last heartbeat timestamp
    pub last_heartbeat: String,
}

/// Write-Ahead Log status.
#[derive(Debug, Clone)]
pub struct WalStatus {
    /// WAL directory path
    pub wal_dir: String,
    /// Number of pending (unprocessed) entries
    pub pending_entries: u64,
    /// Total WAL size in bytes
    pub total_size_bytes: u64,
    /// Oldest unprocessed entry timestamp
    pub oldest_entry: Option<String>,
}

/// Connection pool statistics.
#[derive(Debug, Clone)]
pub struct PoolStats {
    /// Number of active connections
    pub active_connections: u32,
    /// Number of idle connections
    pub idle_connections: u32,
    /// Maximum pool size
    pub max_connections: u32,
    /// Total connections created since startup
    pub total_connections: u64,
}
