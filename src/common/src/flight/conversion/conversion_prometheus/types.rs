//! Internal types for Prometheus ↔ OTEL conversion
//!
//! These types represent Prometheus time series data in a format that can be
//! converted to/from OTEL metrics.

/// Prometheus stale marker - special NaN value indicating a stale sample
/// Bit pattern: 0x7ff0000000000002
pub(crate) const STALE_NAN_BITS: u64 = 0x7ff0000000000002;

/// Check if a sample value is a Prometheus stale marker
pub(crate) fn is_stale_marker(value: f64) -> bool {
    value.to_bits() == STALE_NAN_BITS
}

/// Prefix for OTEL scope labels in Prometheus format
pub(crate) const OTEL_SCOPE_PREFIX: &str = "otel_scope_";

/// Prometheus TimeSeries represents a single time series with labels and samples
#[derive(Debug, Clone)]
pub struct PrometheusTimeSeries {
    /// Labels identifying this time series (includes __name__)
    pub labels: Vec<PrometheusLabel>,
    /// Sample values with timestamps
    pub samples: Vec<PrometheusSample>,
    /// Histogram samples (for native histograms in remote_write v2)
    pub histograms: Vec<PrometheusHistogram>,
}

/// A Prometheus label key-value pair
#[derive(Debug, Clone)]
pub struct PrometheusLabel {
    pub name: String,
    pub value: String,
}

/// A Prometheus sample (timestamp + value)
#[derive(Debug, Clone)]
pub struct PrometheusSample {
    pub value: f64,
    pub timestamp: i64, // milliseconds since epoch
}

/// A Prometheus native histogram (remote_write v2)
#[derive(Debug, Clone)]
pub struct PrometheusHistogram {
    pub count: u64,
    pub sum: f64,
    pub schema: i32,
    pub zero_threshold: f64,
    pub zero_count: u64,
    pub negative_spans: Vec<BucketSpan>,
    pub negative_deltas: Vec<i64>,
    pub positive_spans: Vec<BucketSpan>,
    pub positive_deltas: Vec<i64>,
    pub timestamp: i64,
}

/// Bucket span for native histograms
#[derive(Debug, Clone)]
pub struct BucketSpan {
    pub offset: i32,
    pub length: u32,
}

/// Prometheus WriteRequest contains multiple time series
#[derive(Debug, Clone, Default)]
pub struct PrometheusWriteRequest {
    pub timeseries: Vec<PrometheusTimeSeries>,
    /// Metadata about metrics (type, help, unit) - optional in remote_write v2
    pub metadata: Vec<PrometheusMetricMetadata>,
}

/// Metric metadata from Prometheus
#[derive(Debug, Clone)]
pub struct PrometheusMetricMetadata {
    pub metric_family_name: String,
    pub metric_type: PrometheusMetricType,
    pub help: String,
    pub unit: String,
}

/// Prometheus metric types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PrometheusMetricType {
    #[default]
    Unknown,
    Counter,
    Gauge,
    Summary,
    Histogram,
    GaugeHistogram,
    Info,
    StateSet,
}

/// Detected metric information from naming conventions
#[derive(Debug)]
pub(crate) struct DetectedMetricInfo {
    pub base_name: String,
    pub metric_type: PrometheusMetricType,
    /// Suffix detected from metric name (e.g., "total", "bucket", "count")
    #[allow(dead_code)]
    pub suffix: Option<String>,
}

// ============================================================================
// Helper functions
// ============================================================================

/// Get a label value by name from a slice of labels
pub(crate) fn get_label(labels: &[PrometheusLabel], name: &str) -> Option<String> {
    labels
        .iter()
        .find(|l| l.name == name)
        .map(|l| l.value.clone())
}

/// Create a key for grouping series by their attribute set (excluding specified labels)
pub(crate) fn get_attributes_key(labels: &[PrometheusLabel], exclude: &[&str]) -> String {
    let mut key_parts: Vec<String> = labels
        .iter()
        .filter(|l| !exclude.contains(&l.name.as_str()))
        .map(|l| format!("{}={}", l.name, l.value))
        .collect();
    key_parts.sort();
    key_parts.join(",")
}

/// Parse the 'le' (less than or equal) bucket bound from histogram series
pub(crate) fn parse_le_bound(le: &str) -> f64 {
    if le == "+Inf" {
        f64::INFINITY
    } else {
        le.parse().unwrap_or(0.0)
    }
}

/// Get string representation of a metric type for metadata
pub(crate) fn prometheus_type_to_string(metric_type: PrometheusMetricType) -> &'static str {
    match metric_type {
        PrometheusMetricType::Counter => "counter",
        PrometheusMetricType::Gauge => "gauge",
        PrometheusMetricType::Histogram => "histogram",
        PrometheusMetricType::Summary => "summary",
        PrometheusMetricType::GaugeHistogram => "gaugehistogram",
        PrometheusMetricType::Info => "info",
        PrometheusMetricType::StateSet => "stateset",
        PrometheusMetricType::Unknown => "unknown",
    }
}
