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
    /// Span kind of the root span (e.g. `Server`, `Client`)
    pub root_span_kind: String,
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
    /// Span kind (e.g. `Server`, `Client`, `Internal`)
    pub kind: String,
    /// Span attributes as a JSON value
    pub attributes: serde_json::Value,
    /// Resource attributes as a JSON value (e.g. service.name, deployment.environment)
    pub resource_attributes: serde_json::Value,
    /// Span events (e.g., exceptions, annotations) as a JSON value
    pub events: serde_json::Value,
    /// Span links (cross-trace references) as a JSON value
    pub links: serde_json::Value,
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

/// Metric type enumeration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricType {
    Gauge,
    Sum,
    Histogram,
    ExponentialHistogram,
    Summary,
}

impl MetricType {
    /// Get the Iceberg table name for this metric type.
    pub fn table_name(&self) -> &str {
        match self {
            Self::Gauge => "metrics_gauge",
            Self::Sum => "metrics_sum",
            Self::Histogram => "metrics_histogram",
            Self::ExponentialHistogram => "metrics_exponential_histogram",
            Self::Summary => "metrics_summary",
        }
    }

    /// Get the human-readable label for this metric type.
    pub fn label(&self) -> &str {
        match self {
            Self::Gauge => "Gauge",
            Self::Sum => "Sum",
            Self::Histogram => "Histogram",
            Self::ExponentialHistogram => "Exp. Histogram",
            Self::Summary => "Summary",
        }
    }

    /// Get the single-character badge for this metric type.
    pub fn badge(&self) -> &str {
        match self {
            Self::Gauge => "G",
            Self::Sum => "S",
            Self::Histogram => "H",
            Self::ExponentialHistogram => "E",
            Self::Summary => "Q",
        }
    }

    /// All metric types in discovery order.
    pub fn all() -> &'static [MetricType] {
        &[
            MetricType::Gauge,
            MetricType::Sum,
            MetricType::Histogram,
            MetricType::ExponentialHistogram,
            MetricType::Summary,
        ]
    }
}

/// Information about a metric name.
#[derive(Debug, Clone)]
pub struct MetricNameInfo {
    /// Metric name
    pub name: String,
    /// Metric description
    pub description: String,
    /// Unit of measurement
    pub unit: String,
    /// Metric type
    pub metric_type: MetricType,
}

/// Filters for metric queries.
#[derive(Debug, Clone)]
pub struct MetricFilters {
    /// Filter by service name
    pub service_name: Option<String>,
    /// Maximum number of results
    pub limit: u32,
}

impl Default for MetricFilters {
    fn default() -> Self {
        Self {
            service_name: None,
            limit: 500,
        }
    }
}

/// Grouping strategy for metric results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricGroupBy {
    None,
    Service,
    MetricType,
    ScopeName,
}

impl MetricGroupBy {
    /// Get the human-readable label for this grouping strategy.
    pub fn label(&self) -> &str {
        match self {
            Self::None => "None",
            Self::Service => "Service",
            Self::MetricType => "Metric Type",
            Self::ScopeName => "Scope Name",
        }
    }

    /// Extract grouping key from a row.
    /// `columns` is the list of column names, `row` is the list of cell values as strings.
    pub fn key(&self, columns: &[String], row: &[String]) -> String {
        match self {
            Self::None => String::new(),
            Self::Service => {
                let idx = columns.iter().position(|c| c == "service_name");
                idx.and_then(|i| row.get(i))
                    .cloned()
                    .unwrap_or_else(|| "(unknown)".to_string())
            }
            Self::MetricType => {
                let idx = columns.iter().position(|c| c == "metric_type");
                idx.and_then(|i| row.get(i))
                    .cloned()
                    .unwrap_or_else(|| "(unknown)".to_string())
            }
            Self::ScopeName => {
                let idx = columns.iter().position(|c| c == "scope_name");
                idx.and_then(|i| row.get(i))
                    .cloned()
                    .unwrap_or_else(|| "(unknown)".to_string())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_type_table_names() {
        assert_eq!(MetricType::Gauge.table_name(), "metrics_gauge");
        assert_eq!(MetricType::Sum.table_name(), "metrics_sum");
        assert_eq!(MetricType::Histogram.table_name(), "metrics_histogram");
        assert_eq!(
            MetricType::ExponentialHistogram.table_name(),
            "metrics_exponential_histogram"
        );
        assert_eq!(MetricType::Summary.table_name(), "metrics_summary");
    }

    #[test]
    fn metric_type_labels() {
        assert_eq!(MetricType::Gauge.label(), "Gauge");
        assert_eq!(MetricType::Sum.label(), "Sum");
        assert_eq!(MetricType::Histogram.label(), "Histogram");
        assert_eq!(MetricType::ExponentialHistogram.label(), "Exp. Histogram");
        assert_eq!(MetricType::Summary.label(), "Summary");
    }

    #[test]
    fn metric_type_badges() {
        assert_eq!(MetricType::Gauge.badge(), "G");
        assert_eq!(MetricType::Sum.badge(), "S");
        assert_eq!(MetricType::Histogram.badge(), "H");
        assert_eq!(MetricType::ExponentialHistogram.badge(), "E");
        assert_eq!(MetricType::Summary.badge(), "Q");
    }

    #[test]
    fn metric_type_all() {
        let all = MetricType::all();
        assert_eq!(all.len(), 5);
        assert_eq!(all[0], MetricType::Gauge);
        assert_eq!(all[1], MetricType::Sum);
        assert_eq!(all[2], MetricType::Histogram);
        assert_eq!(all[3], MetricType::ExponentialHistogram);
        assert_eq!(all[4], MetricType::Summary);
    }

    #[test]
    fn metric_filters_default() {
        let filters = MetricFilters::default();
        assert_eq!(filters.limit, 500);
        assert_eq!(filters.service_name, None);
    }

    #[test]
    fn metric_group_by_labels() {
        assert_eq!(MetricGroupBy::None.label(), "None");
        assert_eq!(MetricGroupBy::Service.label(), "Service");
        assert_eq!(MetricGroupBy::MetricType.label(), "Metric Type");
        assert_eq!(MetricGroupBy::ScopeName.label(), "Scope Name");
    }

    #[test]
    fn metric_group_by_key_service() {
        let columns = vec!["service_name".to_string(), "value".to_string()];
        let row = vec!["my-svc".to_string(), "42".to_string()];
        let key = MetricGroupBy::Service.key(&columns, &row);
        assert_eq!(key, "my-svc");
    }

    #[test]
    fn metric_group_by_key_missing_column() {
        let columns = vec!["value".to_string()];
        let row = vec!["42".to_string()];
        let key = MetricGroupBy::Service.key(&columns, &row);
        assert_eq!(key, "(unknown)");
    }
}
