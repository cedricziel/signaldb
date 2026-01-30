//! Bidirectional Prometheus ↔ OTEL metrics conversion
//!
//! This module provides conversion between Prometheus remote_write format and OTEL
//! ExportMetricsServiceRequest.
//!
//! ## Prometheus → OTEL (for ingestion via remote_write)
//!
//! - Metric type detection from naming conventions (_total, _bucket, quantile)
//! - Label to attribute mapping (job → service.name, instance → service.instance.id)
//! - UTF-8 metric names (Prometheus 3.0+ compatibility)
//! - otel_scope_* labels for instrumentation scope
//! - _created suffix for cumulative start time
//! - Stale marker detection (NaN with specific bit pattern)
//! - prometheus.type metadata preservation
//!
//! ## OTEL → Prometheus (for PromQL query results, remote_read)
//!
//! - Metric type mapping (Sum→Counter, Gauge→Gauge, Histogram→bucket/count/sum)
//! - Metric name normalization (_total suffix for counters)
//! - Unit suffix handling
//! - Resource attributes → target_info metric + job/instance labels
//! - Instrumentation scope → otel_scope_* labels
//! - Start time → _created metric generation
//!
//! Based on:
//! - OpenTelemetry Prometheus Compatibility Spec
//! - Prometheus Remote Write 1.0/2.0 Specification

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint,
    ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint, metric::Data, number_data_point,
    summary_data_point::ValueAtQuantile,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use std::collections::HashMap;
use tracing;

/// Prometheus stale marker - special NaN value indicating a stale sample
/// Bit pattern: 0x7ff0000000000002
const STALE_NAN_BITS: u64 = 0x7ff0000000000002;

/// Check if a sample value is a Prometheus stale marker
fn is_stale_marker(value: f64) -> bool {
    value.to_bits() == STALE_NAN_BITS
}

/// Prefix for OTEL scope labels in Prometheus format
const OTEL_SCOPE_PREFIX: &str = "otel_scope_";

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
struct DetectedMetricInfo {
    base_name: String,
    metric_type: PrometheusMetricType,
    /// Suffix detected from metric name (e.g., "total", "bucket", "count")
    /// Reserved for future use in metric reconstruction logic
    #[allow(dead_code)]
    suffix: Option<String>,
}

// ============================================================================
// Prometheus Remote Write Protobuf Types (Wire Format)
// ============================================================================

/// Module containing prost-derived types matching the Prometheus remote_write protobuf spec.
/// These types are used for deserializing the wire format, then converted to our internal types.
pub mod proto {
    use prost::Message;

    /// Prometheus WriteRequest - the top-level message in remote_write protocol
    #[derive(Clone, PartialEq, Message)]
    pub struct WriteRequest {
        #[prost(message, repeated, tag = "1")]
        pub timeseries: Vec<TimeSeries>,
        // Field 2 is reserved (used by Cortex)
        #[prost(message, repeated, tag = "3")]
        pub metadata: Vec<MetricMetadata>,
    }

    /// A single time series with labels, samples, and optional histograms
    #[derive(Clone, PartialEq, Message)]
    pub struct TimeSeries {
        #[prost(message, repeated, tag = "1")]
        pub labels: Vec<Label>,
        #[prost(message, repeated, tag = "2")]
        pub samples: Vec<Sample>,
        #[prost(message, repeated, tag = "3")]
        pub exemplars: Vec<Exemplar>,
        #[prost(message, repeated, tag = "4")]
        pub histograms: Vec<Histogram>,
    }

    /// A label key-value pair
    #[derive(Clone, PartialEq, Message)]
    pub struct Label {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(string, tag = "2")]
        pub value: String,
    }

    /// A sample value with timestamp
    #[derive(Clone, PartialEq, Message)]
    pub struct Sample {
        #[prost(double, tag = "1")]
        pub value: f64,
        #[prost(int64, tag = "2")]
        pub timestamp: i64,
    }

    /// Exemplar for trace correlation
    #[derive(Clone, PartialEq, Message)]
    pub struct Exemplar {
        #[prost(message, repeated, tag = "1")]
        pub labels: Vec<Label>,
        #[prost(double, tag = "2")]
        pub value: f64,
        #[prost(int64, tag = "3")]
        pub timestamp: i64,
    }

    /// Native histogram (remote_write v2)
    #[derive(Clone, PartialEq, Message)]
    pub struct Histogram {
        #[prost(oneof = "histogram::Count", tags = "1, 2")]
        pub count: Option<histogram::Count>,
        #[prost(double, tag = "3")]
        pub sum: f64,
        #[prost(sint32, tag = "4")]
        pub schema: i32,
        #[prost(double, tag = "5")]
        pub zero_threshold: f64,
        #[prost(oneof = "histogram::ZeroCount", tags = "6, 7")]
        pub zero_count: Option<histogram::ZeroCount>,
        #[prost(message, repeated, tag = "8")]
        pub negative_spans: Vec<BucketSpan>,
        #[prost(sint64, repeated, tag = "9")]
        pub negative_deltas: Vec<i64>,
        #[prost(double, repeated, tag = "10")]
        pub negative_counts: Vec<f64>,
        #[prost(message, repeated, tag = "11")]
        pub positive_spans: Vec<BucketSpan>,
        #[prost(sint64, repeated, tag = "12")]
        pub positive_deltas: Vec<i64>,
        #[prost(double, repeated, tag = "13")]
        pub positive_counts: Vec<f64>,
        #[prost(enumeration = "histogram::ResetHint", tag = "14")]
        pub reset_hint: i32,
        #[prost(int64, tag = "15")]
        pub timestamp: i64,
    }

    pub mod histogram {
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum Count {
            #[prost(uint64, tag = "1")]
            CountInt(u64),
            #[prost(double, tag = "2")]
            CountFloat(f64),
        }

        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum ZeroCount {
            #[prost(uint64, tag = "6")]
            ZeroCountInt(u64),
            #[prost(double, tag = "7")]
            ZeroCountFloat(f64),
        }

        #[derive(
            Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration,
        )]
        #[repr(i32)]
        pub enum ResetHint {
            Unknown = 0,
            Yes = 1,
            No = 2,
            Gauge = 3,
        }
    }

    /// Bucket span for native histograms
    #[derive(Clone, PartialEq, Message)]
    pub struct BucketSpan {
        #[prost(sint32, tag = "1")]
        pub offset: i32,
        #[prost(uint32, tag = "2")]
        pub length: u32,
    }

    /// Metric metadata
    #[derive(Clone, PartialEq, Message)]
    pub struct MetricMetadata {
        #[prost(enumeration = "MetricType", tag = "1")]
        pub r#type: i32,
        #[prost(string, tag = "2")]
        pub metric_family_name: String,
        #[prost(string, tag = "4")]
        pub help: String,
        #[prost(string, tag = "5")]
        pub unit: String,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum MetricType {
        Unknown = 0,
        Counter = 1,
        Gauge = 2,
        Summary = 3,
        Histogram = 4,
        GaugeHistogram = 5,
        Info = 6,
        Stateset = 7,
    }
}

/// Decode a snappy-compressed protobuf WriteRequest from raw bytes.
///
/// This is the entry point for Prometheus remote_write ingestion:
/// 1. Decompress snappy block format
/// 2. Decode protobuf
/// 3. Convert to internal PrometheusWriteRequest format
pub fn decode_prometheus_remote_write(data: &[u8]) -> anyhow::Result<PrometheusWriteRequest> {
    use prost::Message;

    // Decompress snappy (block format, not framed)
    let decompressed = snap::raw::Decoder::new()
        .decompress_vec(data)
        .map_err(|e| anyhow::anyhow!("Snappy decompression failed: {e}"))?;

    // Decode protobuf
    let proto_request = proto::WriteRequest::decode(decompressed.as_slice())
        .map_err(|e| anyhow::anyhow!("Protobuf decode failed: {e}"))?;

    // Convert to internal format
    Ok(proto_to_internal(proto_request))
}

/// Convert protobuf WriteRequest to internal PrometheusWriteRequest
fn proto_to_internal(proto: proto::WriteRequest) -> PrometheusWriteRequest {
    PrometheusWriteRequest {
        timeseries: proto
            .timeseries
            .into_iter()
            .map(|ts| PrometheusTimeSeries {
                labels: ts
                    .labels
                    .into_iter()
                    .map(|l| PrometheusLabel {
                        name: l.name,
                        value: l.value,
                    })
                    .collect(),
                samples: ts
                    .samples
                    .into_iter()
                    .map(|s| PrometheusSample {
                        value: s.value,
                        timestamp: s.timestamp,
                    })
                    .collect(),
                histograms: ts
                    .histograms
                    .into_iter()
                    .map(|h| PrometheusHistogram {
                        count: match h.count {
                            Some(proto::histogram::Count::CountInt(c)) => c,
                            Some(proto::histogram::Count::CountFloat(c)) => c as u64,
                            None => 0,
                        },
                        sum: h.sum,
                        schema: h.schema,
                        zero_threshold: h.zero_threshold,
                        zero_count: match h.zero_count {
                            Some(proto::histogram::ZeroCount::ZeroCountInt(c)) => c,
                            Some(proto::histogram::ZeroCount::ZeroCountFloat(c)) => c as u64,
                            None => 0,
                        },
                        negative_spans: h
                            .negative_spans
                            .into_iter()
                            .map(|s| BucketSpan {
                                offset: s.offset,
                                length: s.length,
                            })
                            .collect(),
                        negative_deltas: h.negative_deltas,
                        positive_spans: h
                            .positive_spans
                            .into_iter()
                            .map(|s| BucketSpan {
                                offset: s.offset,
                                length: s.length,
                            })
                            .collect(),
                        positive_deltas: h.positive_deltas,
                        timestamp: h.timestamp,
                    })
                    .collect(),
            })
            .collect(),
        metadata: proto
            .metadata
            .into_iter()
            .map(|m| PrometheusMetricMetadata {
                metric_family_name: m.metric_family_name,
                metric_type: match m.r#type {
                    1 => PrometheusMetricType::Counter,
                    2 => PrometheusMetricType::Gauge,
                    3 => PrometheusMetricType::Summary,
                    4 => PrometheusMetricType::Histogram,
                    5 => PrometheusMetricType::GaugeHistogram,
                    6 => PrometheusMetricType::Info,
                    7 => PrometheusMetricType::StateSet,
                    _ => PrometheusMetricType::Unknown,
                },
                help: m.help,
                unit: m.unit,
            })
            .collect(),
    }
}

/// Convert Prometheus WriteRequest to OTEL ExportMetricsServiceRequest
///
/// This function:
/// 1. Groups time series by resource (job/instance combination)
/// 2. Detects metric types from naming conventions
/// 3. Reconstructs histograms and summaries from their component series
/// 4. Maps Prometheus labels to OTEL attributes
pub fn prometheus_to_otel_metrics(request: &PrometheusWriteRequest) -> ExportMetricsServiceRequest {
    // Group time series by resource (job + instance)
    let mut resource_groups: HashMap<(String, String), Vec<&PrometheusTimeSeries>> = HashMap::new();

    for ts in &request.timeseries {
        let job = get_label(&ts.labels, "job").unwrap_or_default();
        let instance = get_label(&ts.labels, "instance").unwrap_or_default();
        resource_groups.entry((job, instance)).or_default().push(ts);
    }

    // Build metadata lookup map
    let metadata_map: HashMap<&str, &PrometheusMetricMetadata> = request
        .metadata
        .iter()
        .map(|m| (m.metric_family_name.as_str(), m))
        .collect();

    // Convert each resource group
    let resource_metrics: Vec<ResourceMetrics> = resource_groups
        .into_iter()
        .map(|((job, instance), timeseries)| {
            convert_resource_group(&job, &instance, &timeseries, &metadata_map)
        })
        .collect();

    ExportMetricsServiceRequest { resource_metrics }
}

/// Convert a group of time series sharing the same resource (job/instance)
fn convert_resource_group(
    job: &str,
    instance: &str,
    timeseries: &[&PrometheusTimeSeries],
    metadata_map: &HashMap<&str, &PrometheusMetricMetadata>,
) -> ResourceMetrics {
    // Build resource attributes
    let mut resource_attributes = Vec::new();

    if !job.is_empty() {
        resource_attributes.push(KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(job.to_string())),
            }),
        });
    }

    if !instance.is_empty() {
        resource_attributes.push(KeyValue {
            key: "service.instance.id".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(instance.to_string())),
            }),
        });
    }

    // Extract instrumentation scope from otel_scope_* labels
    let scope = extract_instrumentation_scope(timeseries);

    // Group time series by metric base name for histogram/summary reconstruction
    let mut metric_groups: HashMap<String, Vec<&PrometheusTimeSeries>> = HashMap::new();

    for ts in timeseries {
        let metric_name = get_label(&ts.labels, "__name__").unwrap_or_default();
        let detected = detect_metric_type(&metric_name, metadata_map);
        metric_groups
            .entry(detected.base_name)
            .or_default()
            .push(ts);
    }

    // Convert each metric group
    let mut metrics = Vec::new();

    for (base_name, series) in metric_groups {
        if let Some(metric) = convert_metric_group(&base_name, &series, metadata_map) {
            metrics.push(metric);
        }
    }

    ResourceMetrics {
        resource: Some(Resource {
            attributes: resource_attributes,
            dropped_attributes_count: 0,
            entity_refs: vec![],
        }),
        scope_metrics: vec![ScopeMetrics {
            scope,
            metrics,
            schema_url: String::new(),
        }],
        schema_url: String::new(),
    }
}

/// Extract instrumentation scope from otel_scope_* labels
fn extract_instrumentation_scope(
    timeseries: &[&PrometheusTimeSeries],
) -> Option<InstrumentationScope> {
    // Look for otel_scope_name and otel_scope_version labels in any time series
    let mut scope_name: Option<String> = None;
    let mut scope_version: Option<String> = None;
    let mut scope_attributes: Vec<KeyValue> = Vec::new();

    for ts in timeseries {
        for label in &ts.labels {
            if label.name == "otel_scope_name" {
                scope_name = Some(label.value.clone());
            } else if label.name == "otel_scope_version" {
                scope_version = Some(label.value.clone());
            } else if let Some(attr_name) = label.name.strip_prefix(OTEL_SCOPE_PREFIX) {
                // Other otel_scope_* labels become scope attributes
                if attr_name != "name" && attr_name != "version" {
                    scope_attributes.push(KeyValue {
                        key: attr_name.to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(label.value.clone())),
                        }),
                    });
                }
            }
        }
    }

    // Only create scope if we have at least a name
    scope_name.map(|name| InstrumentationScope {
        name,
        version: scope_version.unwrap_or_default(),
        attributes: scope_attributes,
        dropped_attributes_count: 0,
    })
}

/// Convert a group of time series that belong to the same metric
fn convert_metric_group(
    base_name: &str,
    series: &[&PrometheusTimeSeries],
    metadata_map: &HashMap<&str, &PrometheusMetricMetadata>,
) -> Option<Metric> {
    if series.is_empty() {
        return None;
    }

    // Determine metric type from metadata or naming conventions
    let metric_type = if let Some(metadata) = metadata_map.get(base_name) {
        metadata.metric_type
    } else {
        // Check first series name for suffix-based detection
        let first_name = get_label(&series[0].labels, "__name__").unwrap_or_default();
        detect_metric_type(&first_name, metadata_map).metric_type
    };

    // Get metadata info
    let (description, unit) = if let Some(metadata) = metadata_map.get(base_name) {
        (metadata.help.clone(), metadata.unit.clone())
    } else {
        (String::new(), String::new())
    };

    // For histograms and summaries, validate that _count series exists
    // Per OTEL spec: histograms/summaries without _count MUST be dropped
    if matches!(
        metric_type,
        PrometheusMetricType::Histogram
            | PrometheusMetricType::GaugeHistogram
            | PrometheusMetricType::Summary
    ) && !has_count_series(base_name, series)
    {
        tracing::warn!(
            metric = base_name,
            "Dropping metric: histogram/summary missing _count series"
        );
        return None;
    }

    // Extract _created timestamp for cumulative metrics start time
    let created_timestamp = extract_created_timestamp(base_name, series);

    let data = match metric_type {
        PrometheusMetricType::Counter => convert_to_sum(series, true, created_timestamp),
        PrometheusMetricType::Gauge => convert_to_gauge(series),
        PrometheusMetricType::Histogram | PrometheusMetricType::GaugeHistogram => {
            convert_to_histogram(series, created_timestamp)
        }
        PrometheusMetricType::Summary => convert_to_summary(series, created_timestamp),
        _ => {
            // Default to gauge for unknown types
            convert_to_gauge(series)
        }
    };

    // Build prometheus.type metadata
    let mut metric_metadata = Vec::new();
    if metric_type != PrometheusMetricType::Unknown {
        metric_metadata.push(KeyValue {
            key: "prometheus.type".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(
                    prometheus_type_to_string(metric_type).to_string(),
                )),
            }),
        });
    }

    Some(Metric {
        name: base_name.to_string(),
        description,
        unit,
        data: Some(data),
        metadata: metric_metadata,
    })
}

/// Check if a metric group has a _count series
fn has_count_series(base_name: &str, series: &[&PrometheusTimeSeries]) -> bool {
    let count_name = format!("{base_name}_count");
    series.iter().any(|ts| {
        get_label(&ts.labels, "__name__")
            .map(|n| n == count_name)
            .unwrap_or(false)
    })
}

/// Extract _created timestamp from a metric group
fn extract_created_timestamp(base_name: &str, series: &[&PrometheusTimeSeries]) -> Option<u64> {
    let created_name = format!("{base_name}_created");
    for ts in series {
        if let Some(name) = get_label(&ts.labels, "__name__")
            && name == created_name
            && let Some(sample) = ts.samples.last()
        {
            // _created value is a Unix timestamp in seconds
            // Convert from seconds to nanoseconds
            return Some((sample.value as u64) * 1_000_000_000);
        }
    }
    None
}

/// Convert Prometheus metric type to string representation
fn prometheus_type_to_string(metric_type: PrometheusMetricType) -> &'static str {
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

/// Convert time series to OTEL Gauge
fn convert_to_gauge(series: &[&PrometheusTimeSeries]) -> Data {
    let data_points: Vec<NumberDataPoint> = series
        .iter()
        .flat_map(|ts| {
            ts.samples
                .iter()
                .filter(|sample| !is_stale_marker(sample.value))
                .map(|sample| NumberDataPoint {
                    attributes: convert_labels_to_attributes(&ts.labels),
                    start_time_unix_nano: 0, // Gauges don't have start time
                    time_unix_nano: (sample.timestamp as u64) * 1_000_000, // ms to ns
                    value: Some(number_data_point::Value::AsDouble(sample.value)),
                    exemplars: vec![],
                    flags: 0,
                })
        })
        .collect();

    Data::Gauge(Gauge { data_points })
}

/// Convert time series to OTEL Sum (counter)
fn convert_to_sum(
    series: &[&PrometheusTimeSeries],
    is_monotonic: bool,
    created_timestamp: Option<u64>,
) -> Data {
    let data_points: Vec<NumberDataPoint> = series
        .iter()
        .flat_map(|ts| {
            // Skip _total suffix series names (use base name only) and stale markers
            let metric_name = get_label(&ts.labels, "__name__").unwrap_or_default();
            if metric_name.ends_with("_created") {
                return vec![]; // Skip _created series
            }

            // Use _created timestamp if available, otherwise use first sample timestamp
            let start_time = created_timestamp.unwrap_or_else(|| {
                ts.samples
                    .first()
                    .map(|s| (s.timestamp as u64) * 1_000_000)
                    .unwrap_or(0)
            });

            ts.samples
                .iter()
                .filter(|sample| !is_stale_marker(sample.value))
                .map(move |sample| NumberDataPoint {
                    attributes: convert_labels_to_attributes(&ts.labels),
                    start_time_unix_nano: start_time,
                    time_unix_nano: (sample.timestamp as u64) * 1_000_000,
                    value: Some(number_data_point::Value::AsDouble(sample.value)),
                    exemplars: vec![],
                    flags: 0,
                })
                .collect::<Vec<_>>()
        })
        .collect();

    Data::Sum(Sum {
        data_points,
        aggregation_temporality: AggregationTemporality::Cumulative as i32,
        is_monotonic,
    })
}

/// Convert histogram component series to OTEL Histogram
fn convert_to_histogram(series: &[&PrometheusTimeSeries], created_timestamp: Option<u64>) -> Data {
    // Group series by their attributes (excluding le label)
    let mut bucket_groups: HashMap<String, HistogramBucketGroup> = HashMap::new();

    for ts in series {
        let metric_name = get_label(&ts.labels, "__name__").unwrap_or_default();

        // Skip _created series (handled separately)
        if metric_name.ends_with("_created") {
            continue;
        }

        // Get attributes without le and __name__
        let attrs_key = get_attributes_key(&ts.labels, &["__name__", "le", "job", "instance"]);

        let group =
            bucket_groups
                .entry(attrs_key.clone())
                .or_insert_with(|| HistogramBucketGroup {
                    attributes: convert_labels_to_attributes(&ts.labels),
                    buckets: HashMap::new(),
                    count: 0,
                    sum: 0.0,
                    timestamps: vec![],
                });

        if metric_name.ends_with("_bucket")
            && let Some(le) = get_label(&ts.labels, "le")
            && let Some(sample) = ts.samples.last()
            && !is_stale_marker(sample.value)
        {
            // Bucket count
            let bound = parse_le_bound(&le);
            group
                .buckets
                .insert(OrderedFloat(bound), sample.value as u64);
            if !group.timestamps.contains(&sample.timestamp) {
                group.timestamps.push(sample.timestamp);
            }
        } else if metric_name.ends_with("_count")
            && let Some(sample) = ts.samples.last()
            && !is_stale_marker(sample.value)
        {
            group.count = sample.value as u64;
            if !group.timestamps.contains(&sample.timestamp) {
                group.timestamps.push(sample.timestamp);
            }
        } else if metric_name.ends_with("_sum")
            && let Some(sample) = ts.samples.last()
            && !is_stale_marker(sample.value)
        {
            group.sum = sample.value;
            if !group.timestamps.contains(&sample.timestamp) {
                group.timestamps.push(sample.timestamp);
            }
        }
    }

    // Convert bucket groups to OTEL HistogramDataPoints
    let data_points: Vec<HistogramDataPoint> = bucket_groups
        .into_values()
        .map(|group| {
            // Sort bucket bounds and create arrays
            let mut sorted_bounds: Vec<_> = group
                .buckets
                .keys()
                .filter(|b| !b.0.is_infinite())
                .copied()
                .collect();
            sorted_bounds.sort();

            // Calculate bucket counts (delta from cumulative)
            let mut bucket_counts = Vec::new();
            let mut prev_count = 0u64;
            for bound in &sorted_bounds {
                let cumulative = group.buckets.get(bound).copied().unwrap_or(0);
                bucket_counts.push(cumulative.saturating_sub(prev_count));
                prev_count = cumulative;
            }
            // Add +Inf bucket
            let inf_count = group
                .buckets
                .get(&OrderedFloat(f64::INFINITY))
                .copied()
                .unwrap_or(group.count);
            bucket_counts.push(inf_count.saturating_sub(prev_count));

            let timestamp = group.timestamps.last().copied().unwrap_or(0);

            // Use _created timestamp if available, otherwise use first sample timestamp
            let start_time = created_timestamp.unwrap_or_else(|| {
                group
                    .timestamps
                    .first()
                    .map(|t| (*t as u64) * 1_000_000)
                    .unwrap_or(0)
            });

            HistogramDataPoint {
                attributes: group.attributes,
                start_time_unix_nano: start_time,
                time_unix_nano: (timestamp as u64) * 1_000_000,
                count: group.count,
                sum: Some(group.sum),
                bucket_counts,
                explicit_bounds: sorted_bounds.into_iter().map(|f| f.0).collect(),
                exemplars: vec![],
                flags: 0,
                min: None,
                max: None,
            }
        })
        .collect();

    Data::Histogram(Histogram {
        data_points,
        aggregation_temporality: AggregationTemporality::Cumulative as i32,
    })
}

/// Helper struct for grouping histogram buckets
struct HistogramBucketGroup {
    attributes: Vec<KeyValue>,
    buckets: HashMap<OrderedFloat, u64>, // le bound -> cumulative count
    count: u64,
    sum: f64,
    timestamps: Vec<i64>,
}

/// Wrapper for f64 that implements Ord for use as HashMap key
#[derive(Debug, Clone, Copy, PartialEq)]
struct OrderedFloat(f64);

impl Eq for OrderedFloat {}

impl std::hash::Hash for OrderedFloat {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Convert summary component series to OTEL Summary
fn convert_to_summary(series: &[&PrometheusTimeSeries], created_timestamp: Option<u64>) -> Data {
    // Group series by their attributes (excluding quantile label)
    let mut summary_groups: HashMap<String, SummaryGroup> = HashMap::new();

    for ts in series {
        let metric_name = get_label(&ts.labels, "__name__").unwrap_or_default();

        // Skip _created series (handled separately)
        if metric_name.ends_with("_created") {
            continue;
        }

        // Get attributes without quantile and __name__
        let attrs_key =
            get_attributes_key(&ts.labels, &["__name__", "quantile", "job", "instance"]);

        let group = summary_groups
            .entry(attrs_key.clone())
            .or_insert_with(|| SummaryGroup {
                attributes: convert_labels_to_attributes(&ts.labels),
                quantiles: HashMap::new(),
                count: 0,
                sum: 0.0,
                timestamps: vec![],
            });

        if metric_name.ends_with("_count")
            && let Some(sample) = ts.samples.last()
            && !is_stale_marker(sample.value)
        {
            group.count = sample.value as u64;
            if !group.timestamps.contains(&sample.timestamp) {
                group.timestamps.push(sample.timestamp);
            }
        } else if metric_name.ends_with("_sum")
            && let Some(sample) = ts.samples.last()
            && !is_stale_marker(sample.value)
        {
            group.sum = sample.value;
            if !group.timestamps.contains(&sample.timestamp) {
                group.timestamps.push(sample.timestamp);
            }
        } else if let Some(quantile) = get_label(&ts.labels, "quantile")
            && let Ok(q) = quantile.parse::<f64>()
            && let Some(sample) = ts.samples.last()
            && !is_stale_marker(sample.value)
        {
            // Quantile value
            group.quantiles.insert(
                quantile.to_string(),
                ValueAtQuantile {
                    quantile: q,
                    value: sample.value,
                },
            );
            if !group.timestamps.contains(&sample.timestamp) {
                group.timestamps.push(sample.timestamp);
            }
        }
    }

    // Convert summary groups to OTEL SummaryDataPoints
    let data_points: Vec<SummaryDataPoint> = summary_groups
        .into_values()
        .map(|group| {
            let mut quantile_values: Vec<ValueAtQuantile> = group.quantiles.into_values().collect();
            quantile_values.sort_by(|a, b| {
                a.quantile
                    .partial_cmp(&b.quantile)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            let timestamp = group.timestamps.last().copied().unwrap_or(0);

            // Use _created timestamp if available, otherwise use first sample timestamp
            let start_time = created_timestamp.unwrap_or_else(|| {
                group
                    .timestamps
                    .first()
                    .map(|t| (*t as u64) * 1_000_000)
                    .unwrap_or(0)
            });

            SummaryDataPoint {
                attributes: group.attributes,
                start_time_unix_nano: start_time,
                time_unix_nano: (timestamp as u64) * 1_000_000,
                count: group.count,
                sum: group.sum,
                quantile_values,
                flags: 0,
            }
        })
        .collect();

    Data::Summary(Summary { data_points })
}

/// Helper struct for grouping summary quantiles
struct SummaryGroup {
    attributes: Vec<KeyValue>,
    quantiles: HashMap<String, ValueAtQuantile>,
    count: u64,
    sum: f64,
    timestamps: Vec<i64>,
}

/// Detect metric type from naming conventions
fn detect_metric_type(
    metric_name: &str,
    metadata_map: &HashMap<&str, &PrometheusMetricMetadata>,
) -> DetectedMetricInfo {
    // Check for histogram suffixes
    if metric_name.ends_with("_bucket") {
        let base = metric_name.strip_suffix("_bucket").unwrap_or(metric_name);
        return DetectedMetricInfo {
            base_name: base.to_string(),
            metric_type: PrometheusMetricType::Histogram,
            suffix: Some("bucket".to_string()),
        };
    }

    // Check for counter suffix
    if metric_name.ends_with("_total") {
        let base = metric_name.strip_suffix("_total").unwrap_or(metric_name);
        return DetectedMetricInfo {
            base_name: base.to_string(),
            metric_type: PrometheusMetricType::Counter,
            suffix: Some("total".to_string()),
        };
    }

    // Check for _count or _sum (could be histogram or summary)
    if metric_name.ends_with("_count") || metric_name.ends_with("_sum") {
        let suffix = if metric_name.ends_with("_count") {
            "_count"
        } else {
            "_sum"
        };
        let base = metric_name.strip_suffix(suffix).unwrap_or(metric_name);

        // Check metadata for type
        if let Some(metadata) = metadata_map.get(base) {
            return DetectedMetricInfo {
                base_name: base.to_string(),
                metric_type: metadata.metric_type,
                suffix: Some(suffix.trim_start_matches('_').to_string()),
            };
        }

        // Default to histogram for _count/_sum without metadata
        return DetectedMetricInfo {
            base_name: base.to_string(),
            metric_type: PrometheusMetricType::Histogram,
            suffix: Some(suffix.trim_start_matches('_').to_string()),
        };
    }

    // Check for created timestamp suffix
    if metric_name.ends_with("_created") {
        let base = metric_name.strip_suffix("_created").unwrap_or(metric_name);
        return DetectedMetricInfo {
            base_name: base.to_string(),
            metric_type: PrometheusMetricType::Counter,
            suffix: Some("created".to_string()),
        };
    }

    // Check for info suffix
    if metric_name.ends_with("_info") {
        return DetectedMetricInfo {
            base_name: metric_name.to_string(),
            metric_type: PrometheusMetricType::Info,
            suffix: None,
        };
    }

    // Default to gauge
    DetectedMetricInfo {
        base_name: metric_name.to_string(),
        metric_type: PrometheusMetricType::Gauge,
        suffix: None,
    }
}

/// Get a label value by name
fn get_label(labels: &[PrometheusLabel], name: &str) -> Option<String> {
    labels
        .iter()
        .find(|l| l.name == name)
        .map(|l| l.value.clone())
}

/// Convert Prometheus labels to OTEL KeyValue attributes
/// Excludes:
/// - __name__: metric name (goes to Metric.name)
/// - job, instance: resource attributes (go to Resource)
/// - le, quantile: histogram/summary specific labels
/// - otel_scope_*: instrumentation scope (go to InstrumentationScope)
fn convert_labels_to_attributes(labels: &[PrometheusLabel]) -> Vec<KeyValue> {
    labels
        .iter()
        .filter(|l| {
            l.name != "__name__"
                && l.name != "job"
                && l.name != "instance"
                && l.name != "le"
                && l.name != "quantile"
                && !l.name.starts_with(OTEL_SCOPE_PREFIX)
        })
        .map(|l| KeyValue {
            key: l.name.clone(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(l.value.clone())),
            }),
        })
        .collect()
}

/// Create a key for grouping series by attributes (excluding specified labels)
fn get_attributes_key(labels: &[PrometheusLabel], exclude: &[&str]) -> String {
    let mut key_parts: Vec<String> = labels
        .iter()
        .filter(|l| !exclude.contains(&l.name.as_str()))
        .map(|l| format!("{}={}", l.name, l.value))
        .collect();
    key_parts.sort();
    key_parts.join(",")
}

/// Parse a histogram bucket bound (le label value)
fn parse_le_bound(le: &str) -> f64 {
    if le == "+Inf" {
        f64::INFINITY
    } else {
        le.parse().unwrap_or(f64::NAN)
    }
}

// ============================================================
// OTEL to Prometheus Conversion
// ============================================================

/// Configuration options for OTEL to Prometheus conversion
#[derive(Debug, Clone, Default)]
pub struct OtelToPrometheusConfig {
    /// Add _total suffix to monotonic counters (default: true per spec)
    pub add_total_suffix: bool,
    /// Generate _created metrics for cumulative types (default: true)
    pub generate_created_metrics: bool,
    /// Generate target_info metric from resource attributes (default: true)
    pub generate_target_info: bool,
    /// Add otel_scope_* labels (default: true)
    pub add_scope_labels: bool,
}

impl OtelToPrometheusConfig {
    /// Create config with spec-compliant defaults
    pub fn new() -> Self {
        Self {
            add_total_suffix: true,
            generate_created_metrics: true,
            generate_target_info: true,
            add_scope_labels: true,
        }
    }
}

/// Convert OTEL ExportMetricsServiceRequest to Prometheus WriteRequest
///
/// This function implements the OTEL → Prometheus conversion per the
/// OpenTelemetry Prometheus Compatibility specification.
///
/// # Mapping Rules
///
/// | OTEL Type | Prometheus Type |
/// |-----------|-----------------|
/// | Sum (monotonic) | Counter (with _total suffix) |
/// | Sum (non-monotonic) | Gauge |
/// | Gauge | Gauge |
/// | Histogram | _bucket, _count, _sum series |
/// | Summary | quantile, _count, _sum series |
///
/// # Resource Handling
///
/// - `service.name` → `job` label
/// - `service.instance.id` → `instance` label
/// - Other resource attributes → `target_info` metric (if enabled)
///
/// # Instrumentation Scope Handling
///
/// - Scope name → `otel_scope_name` label
/// - Scope version → `otel_scope_version` label
/// - Scope attributes → `otel_scope_{attr}` labels
pub fn otel_to_prometheus_metrics(
    request: &ExportMetricsServiceRequest,
    config: &OtelToPrometheusConfig,
) -> PrometheusWriteRequest {
    let mut timeseries = Vec::new();
    let mut metadata = Vec::new();

    for resource_metrics in &request.resource_metrics {
        let resource = resource_metrics.resource.as_ref();

        // Extract job and instance from resource attributes
        let job = resource
            .and_then(|r| get_resource_attr(&r.attributes, "service.name"))
            .unwrap_or_default();
        let instance = resource
            .and_then(|r| get_resource_attr(&r.attributes, "service.instance.id"))
            .unwrap_or_default();

        // Generate target_info metric from resource attributes
        if config.generate_target_info
            && let Some(target_info) = generate_target_info_metric(resource, &job, &instance)
        {
            timeseries.push(target_info);
        }

        for scope_metrics in &resource_metrics.scope_metrics {
            let scope = scope_metrics.scope.as_ref();

            // Build base labels (job, instance, otel_scope_*)
            let base_labels = build_base_labels(&job, &instance, scope, config);

            for metric in &scope_metrics.metrics {
                // Add metric metadata
                if let Some(metric_metadata) = build_metric_metadata(metric) {
                    metadata.push(metric_metadata);
                }

                // Convert metric to time series
                let metric_timeseries =
                    convert_otel_metric_to_prometheus(metric, &base_labels, config);
                timeseries.extend(metric_timeseries);
            }
        }
    }

    PrometheusWriteRequest {
        timeseries,
        metadata,
    }
}

/// Get a string attribute value from resource attributes
fn get_resource_attr(attrs: &[KeyValue], key: &str) -> Option<String> {
    attrs.iter().find(|kv| kv.key == key).and_then(|kv| {
        if let Some(any_value::Value::StringValue(v)) = &kv.value.as_ref()?.value {
            Some(v.clone())
        } else {
            None
        }
    })
}

/// Generate target_info metric from resource attributes
fn generate_target_info_metric(
    resource: Option<&Resource>,
    job: &str,
    instance: &str,
) -> Option<PrometheusTimeSeries> {
    let resource = resource?;

    // Skip if resource has no attributes beyond job/instance
    if resource.attributes.len() <= 2 {
        return None;
    }

    let mut labels = vec![
        PrometheusLabel {
            name: "__name__".to_string(),
            value: "target_info".to_string(),
        },
        PrometheusLabel {
            name: "job".to_string(),
            value: job.to_string(),
        },
    ];

    if !instance.is_empty() {
        labels.push(PrometheusLabel {
            name: "instance".to_string(),
            value: instance.to_string(),
        });
    }

    // Add other resource attributes as labels (excluding service.name and service.instance.id)
    for kv in &resource.attributes {
        if kv.key != "service.name"
            && kv.key != "service.instance.id"
            && let Some(value) = attribute_to_string(&kv.value)
        {
            labels.push(PrometheusLabel {
                name: normalize_label_name(&kv.key),
                value,
            });
        }
    }

    Some(PrometheusTimeSeries {
        labels,
        samples: vec![PrometheusSample {
            value: 1.0, // Info metrics always have value 1
            timestamp: chrono::Utc::now().timestamp_millis(),
        }],
        histograms: vec![],
    })
}

/// Build base labels for all metrics in a scope
fn build_base_labels(
    job: &str,
    instance: &str,
    scope: Option<&InstrumentationScope>,
    config: &OtelToPrometheusConfig,
) -> Vec<PrometheusLabel> {
    let mut labels = Vec::new();

    if !job.is_empty() {
        labels.push(PrometheusLabel {
            name: "job".to_string(),
            value: job.to_string(),
        });
    }

    if !instance.is_empty() {
        labels.push(PrometheusLabel {
            name: "instance".to_string(),
            value: instance.to_string(),
        });
    }

    if config.add_scope_labels
        && let Some(scope) = scope
    {
        if !scope.name.is_empty() {
            labels.push(PrometheusLabel {
                name: "otel_scope_name".to_string(),
                value: scope.name.clone(),
            });
        }
        if !scope.version.is_empty() {
            labels.push(PrometheusLabel {
                name: "otel_scope_version".to_string(),
                value: scope.version.clone(),
            });
        }
        // Add scope attributes (excluding reserved names)
        for kv in &scope.attributes {
            if kv.key != "name"
                && kv.key != "version"
                && kv.key != "schema_url"
                && let Some(value) = attribute_to_string(&kv.value)
            {
                labels.push(PrometheusLabel {
                    name: format!("otel_scope_{}", normalize_label_name(&kv.key)),
                    value,
                });
            }
        }
    }

    labels
}

/// Convert OTEL metric to Prometheus time series
fn convert_otel_metric_to_prometheus(
    metric: &Metric,
    base_labels: &[PrometheusLabel],
    config: &OtelToPrometheusConfig,
) -> Vec<PrometheusTimeSeries> {
    let mut result = Vec::new();

    let metric_name = normalize_metric_name(&metric.name);
    let unit_suffix = if !metric.unit.is_empty() {
        Some(normalize_unit(&metric.unit))
    } else {
        None
    };

    match &metric.data {
        Some(Data::Gauge(gauge)) => {
            result.extend(convert_gauge_to_prometheus(
                &metric_name,
                unit_suffix.as_deref(),
                gauge,
                base_labels,
            ));
        }
        Some(Data::Sum(sum)) => {
            result.extend(convert_sum_to_prometheus(
                &metric_name,
                unit_suffix.as_deref(),
                sum,
                base_labels,
                config,
            ));
        }
        Some(Data::Histogram(histogram)) => {
            result.extend(convert_histogram_to_prometheus(
                &metric_name,
                unit_suffix.as_deref(),
                histogram,
                base_labels,
                config,
            ));
        }
        Some(Data::Summary(summary)) => {
            result.extend(convert_summary_to_prometheus(
                &metric_name,
                unit_suffix.as_deref(),
                summary,
                base_labels,
                config,
            ));
        }
        Some(Data::ExponentialHistogram(_)) => {
            // Exponential histograms require native histogram support in Prometheus
            // For now, we skip them (could convert to fixed-bucket histogram)
            tracing::debug!(
                metric = metric_name,
                "Skipping exponential histogram (native histogram conversion not implemented)"
            );
        }
        None => {}
    }

    result
}

/// Convert OTEL Gauge to Prometheus time series
fn convert_gauge_to_prometheus(
    metric_name: &str,
    unit_suffix: Option<&str>,
    gauge: &Gauge,
    base_labels: &[PrometheusLabel],
) -> Vec<PrometheusTimeSeries> {
    let full_name = build_metric_name(metric_name, unit_suffix, None);

    gauge
        .data_points
        .iter()
        .map(|dp| {
            let mut labels = vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: full_name.clone(),
            }];
            labels.extend(base_labels.iter().cloned());
            labels.extend(attributes_to_labels(&dp.attributes));

            PrometheusTimeSeries {
                labels,
                samples: vec![PrometheusSample {
                    value: extract_number_value(dp),
                    timestamp: (dp.time_unix_nano / 1_000_000) as i64,
                }],
                histograms: vec![],
            }
        })
        .collect()
}

/// Convert OTEL Sum to Prometheus time series (Counter or Gauge)
fn convert_sum_to_prometheus(
    metric_name: &str,
    unit_suffix: Option<&str>,
    sum: &Sum,
    base_labels: &[PrometheusLabel],
    config: &OtelToPrometheusConfig,
) -> Vec<PrometheusTimeSeries> {
    let mut result = Vec::new();

    // Determine if this is a counter (add _total suffix)
    let type_suffix = if sum.is_monotonic && config.add_total_suffix {
        Some("total")
    } else {
        None
    };

    let full_name = build_metric_name(metric_name, unit_suffix, type_suffix);

    for dp in &sum.data_points {
        let mut labels = vec![PrometheusLabel {
            name: "__name__".to_string(),
            value: full_name.clone(),
        }];
        labels.extend(base_labels.iter().cloned());
        labels.extend(attributes_to_labels(&dp.attributes));

        result.push(PrometheusTimeSeries {
            labels: labels.clone(),
            samples: vec![PrometheusSample {
                value: extract_number_value(dp),
                timestamp: (dp.time_unix_nano / 1_000_000) as i64,
            }],
            histograms: vec![],
        });

        // Generate _created metric for cumulative counters
        if config.generate_created_metrics
            && sum.is_monotonic
            && sum.aggregation_temporality == AggregationTemporality::Cumulative as i32
            && dp.start_time_unix_nano > 0
        {
            let created_name = build_metric_name(metric_name, unit_suffix, Some("created"));
            let mut created_labels = vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: created_name,
            }];
            created_labels.extend(base_labels.iter().cloned());
            created_labels.extend(attributes_to_labels(&dp.attributes));

            result.push(PrometheusTimeSeries {
                labels: created_labels,
                samples: vec![PrometheusSample {
                    // _created value is Unix timestamp in seconds
                    value: (dp.start_time_unix_nano as f64) / 1_000_000_000.0,
                    timestamp: (dp.time_unix_nano / 1_000_000) as i64,
                }],
                histograms: vec![],
            });
        }
    }

    result
}

/// Convert OTEL Histogram to Prometheus time series (_bucket, _count, _sum)
fn convert_histogram_to_prometheus(
    metric_name: &str,
    unit_suffix: Option<&str>,
    histogram: &Histogram,
    base_labels: &[PrometheusLabel],
    config: &OtelToPrometheusConfig,
) -> Vec<PrometheusTimeSeries> {
    let mut result = Vec::new();

    for dp in &histogram.data_points {
        let timestamp = (dp.time_unix_nano / 1_000_000) as i64;
        let attr_labels = attributes_to_labels(&dp.attributes);

        // Generate _bucket series
        let bucket_name = build_metric_name(metric_name, unit_suffix, Some("bucket"));
        let mut cumulative_count = 0u64;

        for (i, bound) in dp.explicit_bounds.iter().enumerate() {
            cumulative_count += dp.bucket_counts.get(i).copied().unwrap_or(0);

            let mut labels = vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: bucket_name.clone(),
                },
                PrometheusLabel {
                    name: "le".to_string(),
                    value: bound.to_string(),
                },
            ];
            labels.extend(base_labels.iter().cloned());
            labels.extend(attr_labels.iter().cloned());

            result.push(PrometheusTimeSeries {
                labels,
                samples: vec![PrometheusSample {
                    value: cumulative_count as f64,
                    timestamp,
                }],
                histograms: vec![],
            });
        }

        // +Inf bucket (total count)
        let mut inf_labels = vec![
            PrometheusLabel {
                name: "__name__".to_string(),
                value: bucket_name.clone(),
            },
            PrometheusLabel {
                name: "le".to_string(),
                value: "+Inf".to_string(),
            },
        ];
        inf_labels.extend(base_labels.iter().cloned());
        inf_labels.extend(attr_labels.iter().cloned());

        result.push(PrometheusTimeSeries {
            labels: inf_labels,
            samples: vec![PrometheusSample {
                value: dp.count as f64,
                timestamp,
            }],
            histograms: vec![],
        });

        // Generate _count series
        let count_name = build_metric_name(metric_name, unit_suffix, Some("count"));
        let mut count_labels = vec![PrometheusLabel {
            name: "__name__".to_string(),
            value: count_name,
        }];
        count_labels.extend(base_labels.iter().cloned());
        count_labels.extend(attr_labels.iter().cloned());

        result.push(PrometheusTimeSeries {
            labels: count_labels,
            samples: vec![PrometheusSample {
                value: dp.count as f64,
                timestamp,
            }],
            histograms: vec![],
        });

        // Generate _sum series (if available)
        if let Some(sum) = dp.sum {
            let sum_name = build_metric_name(metric_name, unit_suffix, Some("sum"));
            let mut sum_labels = vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: sum_name,
            }];
            sum_labels.extend(base_labels.iter().cloned());
            sum_labels.extend(attr_labels.iter().cloned());

            result.push(PrometheusTimeSeries {
                labels: sum_labels,
                samples: vec![PrometheusSample {
                    value: sum,
                    timestamp,
                }],
                histograms: vec![],
            });
        }

        // Generate _created metric
        if config.generate_created_metrics
            && histogram.aggregation_temporality == AggregationTemporality::Cumulative as i32
            && dp.start_time_unix_nano > 0
        {
            let created_name = build_metric_name(metric_name, unit_suffix, Some("created"));
            let mut created_labels = vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: created_name,
            }];
            created_labels.extend(base_labels.iter().cloned());
            created_labels.extend(attr_labels.iter().cloned());

            result.push(PrometheusTimeSeries {
                labels: created_labels,
                samples: vec![PrometheusSample {
                    value: (dp.start_time_unix_nano as f64) / 1_000_000_000.0,
                    timestamp,
                }],
                histograms: vec![],
            });
        }
    }

    result
}

/// Convert OTEL Summary to Prometheus time series (quantile, _count, _sum)
fn convert_summary_to_prometheus(
    metric_name: &str,
    unit_suffix: Option<&str>,
    summary: &Summary,
    base_labels: &[PrometheusLabel],
    config: &OtelToPrometheusConfig,
) -> Vec<PrometheusTimeSeries> {
    let mut result = Vec::new();
    let base_name = build_metric_name(metric_name, unit_suffix, None);

    for dp in &summary.data_points {
        let timestamp = (dp.time_unix_nano / 1_000_000) as i64;
        let attr_labels = attributes_to_labels(&dp.attributes);

        // Generate quantile series
        for qv in &dp.quantile_values {
            let mut labels = vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: base_name.clone(),
                },
                PrometheusLabel {
                    name: "quantile".to_string(),
                    value: qv.quantile.to_string(),
                },
            ];
            labels.extend(base_labels.iter().cloned());
            labels.extend(attr_labels.iter().cloned());

            result.push(PrometheusTimeSeries {
                labels,
                samples: vec![PrometheusSample {
                    value: qv.value,
                    timestamp,
                }],
                histograms: vec![],
            });
        }

        // Generate _count series
        let count_name = build_metric_name(metric_name, unit_suffix, Some("count"));
        let mut count_labels = vec![PrometheusLabel {
            name: "__name__".to_string(),
            value: count_name,
        }];
        count_labels.extend(base_labels.iter().cloned());
        count_labels.extend(attr_labels.iter().cloned());

        result.push(PrometheusTimeSeries {
            labels: count_labels,
            samples: vec![PrometheusSample {
                value: dp.count as f64,
                timestamp,
            }],
            histograms: vec![],
        });

        // Generate _sum series
        let sum_name = build_metric_name(metric_name, unit_suffix, Some("sum"));
        let mut sum_labels = vec![PrometheusLabel {
            name: "__name__".to_string(),
            value: sum_name,
        }];
        sum_labels.extend(base_labels.iter().cloned());
        sum_labels.extend(attr_labels.iter().cloned());

        result.push(PrometheusTimeSeries {
            labels: sum_labels,
            samples: vec![PrometheusSample {
                value: dp.sum,
                timestamp,
            }],
            histograms: vec![],
        });

        // Generate _created metric
        if config.generate_created_metrics && dp.start_time_unix_nano > 0 {
            let created_name = build_metric_name(metric_name, unit_suffix, Some("created"));
            let mut created_labels = vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: created_name,
            }];
            created_labels.extend(base_labels.iter().cloned());
            created_labels.extend(attr_labels.iter().cloned());

            result.push(PrometheusTimeSeries {
                labels: created_labels,
                samples: vec![PrometheusSample {
                    value: (dp.start_time_unix_nano as f64) / 1_000_000_000.0,
                    timestamp,
                }],
                histograms: vec![],
            });
        }
    }

    result
}

/// Build full metric name with optional unit and type suffixes
fn build_metric_name(
    base_name: &str,
    unit_suffix: Option<&str>,
    type_suffix: Option<&str>,
) -> String {
    let mut name = base_name.to_string();

    if let Some(unit) = unit_suffix {
        // Don't add unit if base name already ends with it
        if !name.ends_with(&format!("_{unit}")) {
            name = format!("{name}_{unit}");
        }
    }

    if let Some(suffix) = type_suffix {
        // Don't add suffix if already present
        let suffix_with_underscore = format!("_{suffix}");
        if !name.ends_with(&suffix_with_underscore) {
            name = format!("{name}_{suffix}");
        }
    }

    name
}

/// Normalize metric name to Prometheus conventions
/// Replaces disallowed characters with underscores
fn normalize_metric_name(name: &str) -> String {
    let mut result = String::with_capacity(name.len());
    let mut last_was_underscore = false;

    for c in name.chars() {
        if c.is_ascii_alphanumeric() || c == '_' || c == ':' {
            result.push(c);
            last_was_underscore = c == '_';
        } else {
            // Replace disallowed characters with underscore
            if !last_was_underscore {
                result.push('_');
                last_was_underscore = true;
            }
        }
    }

    // Remove trailing underscore
    if result.ends_with('_') {
        result.pop();
    }

    result
}

/// Normalize label name to Prometheus conventions
fn normalize_label_name(name: &str) -> String {
    let mut result = String::with_capacity(name.len());
    let mut last_was_underscore = false;

    for c in name.chars() {
        if c.is_ascii_alphanumeric() || c == '_' {
            result.push(c);
            last_was_underscore = c == '_';
        } else if !last_was_underscore {
            result.push('_');
            last_was_underscore = true;
        }
    }

    // Remove trailing underscore
    if result.ends_with('_') {
        result.pop();
    }

    result
}

/// Normalize OTEL unit to Prometheus unit suffix
fn normalize_unit(unit: &str) -> String {
    // Common unit conversions per OTEL spec
    match unit.to_lowercase().as_str() {
        "1" => "ratio".to_string(),
        "s" | "sec" | "second" | "seconds" => "seconds".to_string(),
        "ms" | "millisecond" | "milliseconds" => "milliseconds".to_string(),
        "us" | "microsecond" | "microseconds" => "microseconds".to_string(),
        "ns" | "nanosecond" | "nanoseconds" => "nanoseconds".to_string(),
        "by" | "byte" | "bytes" => "bytes".to_string(),
        "kib" | "kibibyte" | "kibibytes" => "kibibytes".to_string(),
        "mib" | "mebibyte" | "mebibytes" => "mebibytes".to_string(),
        "gib" | "gibibyte" | "gibibytes" => "gibibytes".to_string(),
        "kb" | "kilobyte" | "kilobytes" => "kilobytes".to_string(),
        "mb" | "megabyte" | "megabytes" => "megabytes".to_string(),
        "gb" | "gigabyte" | "gigabytes" => "gigabytes".to_string(),
        _ => {
            // Handle foo/bar → foo_per_bar
            if unit.contains('/') {
                unit.replace('/', "_per_")
            } else {
                // Remove brackets and special characters
                let cleaned = unit
                    .chars()
                    .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
                    .collect::<String>();
                if cleaned.is_empty() {
                    String::new()
                } else {
                    cleaned
                }
            }
        }
    }
}

/// Convert OTEL attributes to Prometheus labels
fn attributes_to_labels(attrs: &[KeyValue]) -> Vec<PrometheusLabel> {
    attrs
        .iter()
        .filter_map(|kv| {
            attribute_to_string(&kv.value).map(|value| PrometheusLabel {
                name: normalize_label_name(&kv.key),
                value,
            })
        })
        .collect()
}

/// Convert OTEL AnyValue to string
fn attribute_to_string(value: &Option<AnyValue>) -> Option<String> {
    value.as_ref().and_then(|v| match &v.value {
        Some(any_value::Value::StringValue(s)) => Some(s.clone()),
        Some(any_value::Value::BoolValue(b)) => Some(b.to_string()),
        Some(any_value::Value::IntValue(i)) => Some(i.to_string()),
        Some(any_value::Value::DoubleValue(d)) => Some(d.to_string()),
        _ => None,
    })
}

/// Extract numeric value from NumberDataPoint
fn extract_number_value(dp: &NumberDataPoint) -> f64 {
    match dp.value {
        Some(number_data_point::Value::AsDouble(v)) => v,
        Some(number_data_point::Value::AsInt(v)) => v as f64,
        None => 0.0,
    }
}

/// Build metric metadata from OTEL metric
fn build_metric_metadata(metric: &Metric) -> Option<PrometheusMetricMetadata> {
    let metric_type = match &metric.data {
        Some(Data::Gauge(_)) => PrometheusMetricType::Gauge,
        Some(Data::Sum(sum)) => {
            if sum.is_monotonic {
                PrometheusMetricType::Counter
            } else {
                PrometheusMetricType::Gauge
            }
        }
        Some(Data::Histogram(_)) => PrometheusMetricType::Histogram,
        Some(Data::Summary(_)) => PrometheusMetricType::Summary,
        Some(Data::ExponentialHistogram(_)) => PrometheusMetricType::Histogram,
        None => return None,
    };

    Some(PrometheusMetricMetadata {
        metric_family_name: normalize_metric_name(&metric.name),
        metric_type,
        help: metric.description.clone(),
        unit: metric.unit.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_counter_type() {
        let metadata_map = HashMap::new();

        let info = detect_metric_type("http_requests_total", &metadata_map);
        assert_eq!(info.base_name, "http_requests");
        assert_eq!(info.metric_type, PrometheusMetricType::Counter);
        assert_eq!(info.suffix, Some("total".to_string()));
    }

    #[test]
    fn test_detect_histogram_type() {
        let metadata_map = HashMap::new();

        let info = detect_metric_type("http_request_duration_seconds_bucket", &metadata_map);
        assert_eq!(info.base_name, "http_request_duration_seconds");
        assert_eq!(info.metric_type, PrometheusMetricType::Histogram);
        assert_eq!(info.suffix, Some("bucket".to_string()));
    }

    #[test]
    fn test_detect_gauge_type() {
        let metadata_map = HashMap::new();

        let info = detect_metric_type("process_resident_memory_bytes", &metadata_map);
        assert_eq!(info.base_name, "process_resident_memory_bytes");
        assert_eq!(info.metric_type, PrometheusMetricType::Gauge);
        assert_eq!(info.suffix, None);
    }

    #[test]
    fn test_convert_gauge() {
        let ts = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "temperature".to_string(),
                },
                PrometheusLabel {
                    name: "location".to_string(),
                    value: "room1".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 23.5,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![ts],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);

        assert_eq!(result.resource_metrics.len(), 1);
        let rm = &result.resource_metrics[0];
        assert_eq!(rm.scope_metrics.len(), 1);
        assert_eq!(rm.scope_metrics[0].metrics.len(), 1);

        let metric = &rm.scope_metrics[0].metrics[0];
        assert_eq!(metric.name, "temperature");

        if let Some(Data::Gauge(gauge)) = &metric.data {
            assert_eq!(gauge.data_points.len(), 1);
            if let Some(number_data_point::Value::AsDouble(v)) = gauge.data_points[0].value {
                assert!((v - 23.5).abs() < f64::EPSILON);
            } else {
                panic!("Expected double value");
            }
        } else {
            panic!("Expected gauge data");
        }
    }

    #[test]
    fn test_convert_counter() {
        let ts = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "http_requests_total".to_string(),
                },
                PrometheusLabel {
                    name: "method".to_string(),
                    value: "GET".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 100.0,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![ts],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);

        let metric = &result.resource_metrics[0].scope_metrics[0].metrics[0];
        assert_eq!(metric.name, "http_requests");

        if let Some(Data::Sum(sum)) = &metric.data {
            assert!(sum.is_monotonic);
            assert_eq!(
                sum.aggregation_temporality,
                AggregationTemporality::Cumulative as i32
            );
        } else {
            panic!("Expected sum data");
        }
    }

    #[test]
    fn test_resource_attributes_from_job_instance() {
        let ts = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "up".to_string(),
                },
                PrometheusLabel {
                    name: "job".to_string(),
                    value: "api-server".to_string(),
                },
                PrometheusLabel {
                    name: "instance".to_string(),
                    value: "localhost:8080".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 1.0,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![ts],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);

        let resource = result.resource_metrics[0].resource.as_ref().unwrap();
        assert_eq!(resource.attributes.len(), 2);

        let service_name = resource
            .attributes
            .iter()
            .find(|a| a.key == "service.name")
            .unwrap();
        if let Some(any_value::Value::StringValue(v)) = &service_name.value.as_ref().unwrap().value
        {
            assert_eq!(v, "api-server");
        }

        let instance_id = resource
            .attributes
            .iter()
            .find(|a| a.key == "service.instance.id")
            .unwrap();
        if let Some(any_value::Value::StringValue(v)) = &instance_id.value.as_ref().unwrap().value {
            assert_eq!(v, "localhost:8080");
        }
    }

    #[test]
    fn test_histogram_reconstruction() {
        // Simulate a histogram with 3 buckets: le=0.1, le=0.5, le=1.0, le=+Inf
        let bucket_01 = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "request_duration_seconds_bucket".to_string(),
                },
                PrometheusLabel {
                    name: "le".to_string(),
                    value: "0.1".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 10.0, // 10 requests <= 0.1s
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let bucket_05 = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "request_duration_seconds_bucket".to_string(),
                },
                PrometheusLabel {
                    name: "le".to_string(),
                    value: "0.5".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 25.0, // 25 requests <= 0.5s
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let bucket_inf = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "request_duration_seconds_bucket".to_string(),
                },
                PrometheusLabel {
                    name: "le".to_string(),
                    value: "+Inf".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 30.0, // 30 total requests
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let count = PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "request_duration_seconds_count".to_string(),
            }],
            samples: vec![PrometheusSample {
                value: 30.0,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let sum = PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "request_duration_seconds_sum".to_string(),
            }],
            samples: vec![PrometheusSample {
                value: 5.5,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![bucket_01, bucket_05, bucket_inf, count, sum],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);

        let metric = &result.resource_metrics[0].scope_metrics[0].metrics[0];
        assert_eq!(metric.name, "request_duration_seconds");

        if let Some(Data::Histogram(histogram)) = &metric.data {
            assert_eq!(histogram.data_points.len(), 1);
            let dp = &histogram.data_points[0];
            assert_eq!(dp.count, 30);
            assert!((dp.sum.unwrap() - 5.5).abs() < f64::EPSILON);
            assert_eq!(dp.explicit_bounds, vec![0.1, 0.5]);
            // Bucket counts: [10, 15, 5] (deltas from cumulative)
            assert_eq!(dp.bucket_counts, vec![10, 15, 5]);
        } else {
            panic!("Expected histogram data");
        }
    }

    #[test]
    fn test_summary_reconstruction() {
        let quantile_50 = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "request_latency".to_string(),
                },
                PrometheusLabel {
                    name: "quantile".to_string(),
                    value: "0.5".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 0.1,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let quantile_99 = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "request_latency".to_string(),
                },
                PrometheusLabel {
                    name: "quantile".to_string(),
                    value: "0.99".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 0.5,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let count = PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "request_latency_count".to_string(),
            }],
            samples: vec![PrometheusSample {
                value: 100.0,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let sum = PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "request_latency_sum".to_string(),
            }],
            samples: vec![PrometheusSample {
                value: 12.5,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let metadata = PrometheusMetricMetadata {
            metric_family_name: "request_latency".to_string(),
            metric_type: PrometheusMetricType::Summary,
            help: "Request latency summary".to_string(),
            unit: "seconds".to_string(),
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![quantile_50, quantile_99, count, sum],
            metadata: vec![metadata],
        };

        let result = prometheus_to_otel_metrics(&request);

        let metric = &result.resource_metrics[0].scope_metrics[0].metrics[0];
        assert_eq!(metric.name, "request_latency");
        assert_eq!(metric.description, "Request latency summary");
        assert_eq!(metric.unit, "seconds");

        if let Some(Data::Summary(summary)) = &metric.data {
            assert_eq!(summary.data_points.len(), 1);
            let dp = &summary.data_points[0];
            assert_eq!(dp.count, 100);
            assert!((dp.sum - 12.5).abs() < f64::EPSILON);
            assert_eq!(dp.quantile_values.len(), 2);
            assert!((dp.quantile_values[0].quantile - 0.5).abs() < f64::EPSILON);
            assert!((dp.quantile_values[0].value - 0.1).abs() < f64::EPSILON);
            assert!((dp.quantile_values[1].quantile - 0.99).abs() < f64::EPSILON);
            assert!((dp.quantile_values[1].value - 0.5).abs() < f64::EPSILON);
        } else {
            panic!("Expected summary data");
        }
    }

    #[test]
    fn test_utf8_metric_names_preserved() {
        // Test that UTF-8 metric names (Prometheus 3.0+) pass through unchanged
        let ts = PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "http.server.request.duration".to_string(), // OTEL-style with dots
            }],
            samples: vec![PrometheusSample {
                value: 0.5,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![ts],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);
        let metric = &result.resource_metrics[0].scope_metrics[0].metrics[0];

        // Metric name should be preserved exactly
        assert_eq!(metric.name, "http.server.request.duration");
    }

    #[test]
    fn test_stale_marker_filtered() {
        // Test that stale markers (special NaN value) are filtered out
        let stale_nan = f64::from_bits(STALE_NAN_BITS);

        let ts = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "temperature".to_string(),
                },
                PrometheusLabel {
                    name: "location".to_string(),
                    value: "room1".to_string(),
                },
            ],
            samples: vec![
                PrometheusSample {
                    value: 23.5,
                    timestamp: 1700000000000,
                },
                PrometheusSample {
                    value: stale_nan, // Stale marker
                    timestamp: 1700000001000,
                },
                PrometheusSample {
                    value: 24.0,
                    timestamp: 1700000002000,
                },
            ],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![ts],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);
        let metric = &result.resource_metrics[0].scope_metrics[0].metrics[0];

        if let Some(Data::Gauge(gauge)) = &metric.data {
            // Stale marker should be filtered out, leaving 2 data points
            assert_eq!(gauge.data_points.len(), 2);
            if let Some(number_data_point::Value::AsDouble(v)) = gauge.data_points[0].value {
                assert!((v - 23.5).abs() < f64::EPSILON);
            }
            if let Some(number_data_point::Value::AsDouble(v)) = gauge.data_points[1].value {
                assert!((v - 24.0).abs() < f64::EPSILON);
            }
        } else {
            panic!("Expected gauge data");
        }
    }

    #[test]
    fn test_is_stale_marker() {
        // Test stale marker detection
        let stale_nan = f64::from_bits(STALE_NAN_BITS);
        assert!(is_stale_marker(stale_nan));

        // Regular NaN should NOT be detected as stale
        assert!(!is_stale_marker(f64::NAN));

        // Regular values should NOT be detected as stale
        assert!(!is_stale_marker(0.0));
        assert!(!is_stale_marker(42.5));
        assert!(!is_stale_marker(f64::INFINITY));
        assert!(!is_stale_marker(f64::NEG_INFINITY));
    }

    #[test]
    fn test_otel_scope_labels_to_instrumentation_scope() {
        // Test that otel_scope_* labels are extracted to InstrumentationScope
        let ts = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "my_metric".to_string(),
                },
                PrometheusLabel {
                    name: "otel_scope_name".to_string(),
                    value: "my.instrumentation.library".to_string(),
                },
                PrometheusLabel {
                    name: "otel_scope_version".to_string(),
                    value: "1.2.3".to_string(),
                },
                PrometheusLabel {
                    name: "otel_scope_custom_attr".to_string(),
                    value: "custom_value".to_string(),
                },
                PrometheusLabel {
                    name: "regular_label".to_string(),
                    value: "label_value".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 42.0,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![ts],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);
        let rm = &result.resource_metrics[0];

        // Check instrumentation scope was created
        let scope = rm.scope_metrics[0].scope.as_ref().unwrap();
        assert_eq!(scope.name, "my.instrumentation.library");
        assert_eq!(scope.version, "1.2.3");
        assert_eq!(scope.attributes.len(), 1);
        assert_eq!(scope.attributes[0].key, "custom_attr");

        // Check that otel_scope_* labels are NOT in metric attributes
        let metric = &rm.scope_metrics[0].metrics[0];
        if let Some(Data::Gauge(gauge)) = &metric.data {
            let attrs = &gauge.data_points[0].attributes;
            // Should only have regular_label, not otel_scope_* labels
            assert_eq!(attrs.len(), 1);
            assert_eq!(attrs[0].key, "regular_label");
        } else {
            panic!("Expected gauge data");
        }
    }

    #[test]
    fn test_created_timestamp_for_counter() {
        // Test that _created suffix is used for start time
        let counter = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "http_requests_total".to_string(),
                },
                PrometheusLabel {
                    name: "method".to_string(),
                    value: "GET".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 100.0,
                timestamp: 1700000100000, // 100 seconds after created
            }],
            histograms: vec![],
        };

        let created = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "http_requests_created".to_string(),
                },
                PrometheusLabel {
                    name: "method".to_string(),
                    value: "GET".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 1700000000.0, // Unix timestamp in seconds
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![counter, created],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);
        let metric = &result.resource_metrics[0].scope_metrics[0].metrics[0];

        if let Some(Data::Sum(sum)) = &metric.data {
            assert_eq!(sum.data_points.len(), 1);
            let dp = &sum.data_points[0];
            // start_time should be from _created (1700000000 seconds in nanos)
            assert_eq!(dp.start_time_unix_nano, 1_700_000_000_000_000_000);
            // time should be from the sample timestamp
            assert_eq!(dp.time_unix_nano, 1700000100000 * 1_000_000);
        } else {
            panic!("Expected sum data");
        }
    }

    #[test]
    fn test_prometheus_type_metadata() {
        // Test that prometheus.type metadata is added
        let ts = PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "http_requests_total".to_string(),
            }],
            samples: vec![PrometheusSample {
                value: 100.0,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![ts],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);
        let metric = &result.resource_metrics[0].scope_metrics[0].metrics[0];

        // Check prometheus.type metadata
        assert!(!metric.metadata.is_empty());
        let prom_type = metric
            .metadata
            .iter()
            .find(|kv| kv.key == "prometheus.type")
            .unwrap();
        if let Some(any_value::Value::StringValue(v)) = &prom_type.value.as_ref().unwrap().value {
            assert_eq!(v, "counter");
        } else {
            panic!("Expected string value for prometheus.type");
        }
    }

    #[test]
    fn test_histogram_without_count_dropped() {
        // Test that histograms without _count series are dropped
        // Per OTEL spec: "Prometheus Histograms and Summaries without _count MUST be dropped"
        let bucket_01 = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "request_duration_seconds_bucket".to_string(),
                },
                PrometheusLabel {
                    name: "le".to_string(),
                    value: "0.1".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 10.0,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let bucket_inf = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "request_duration_seconds_bucket".to_string(),
                },
                PrometheusLabel {
                    name: "le".to_string(),
                    value: "+Inf".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 30.0,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        // Missing _count series!
        let sum = PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "request_duration_seconds_sum".to_string(),
            }],
            samples: vec![PrometheusSample {
                value: 5.5,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![bucket_01, bucket_inf, sum],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);

        // Metric should be dropped (no metrics in output)
        assert!(
            result.resource_metrics.is_empty()
                || result.resource_metrics[0].scope_metrics[0]
                    .metrics
                    .is_empty()
        );
    }

    #[test]
    fn test_summary_without_count_dropped() {
        // Test that summaries without _count series are dropped
        let quantile_50 = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "request_latency".to_string(),
                },
                PrometheusLabel {
                    name: "quantile".to_string(),
                    value: "0.5".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 0.1,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        // Only _sum, missing _count!
        let sum = PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "request_latency_sum".to_string(),
            }],
            samples: vec![PrometheusSample {
                value: 12.5,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let metadata = PrometheusMetricMetadata {
            metric_family_name: "request_latency".to_string(),
            metric_type: PrometheusMetricType::Summary,
            help: "Request latency summary".to_string(),
            unit: "seconds".to_string(),
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![quantile_50, sum],
            metadata: vec![metadata],
        };

        let result = prometheus_to_otel_metrics(&request);

        // Summary should be dropped (no metrics in output)
        assert!(
            result.resource_metrics.is_empty()
                || result.resource_metrics[0].scope_metrics[0]
                    .metrics
                    .is_empty()
        );
    }

    #[test]
    fn test_created_timestamp_for_histogram() {
        // Test that _created suffix is used for histogram start time
        let bucket_01 = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "request_duration_seconds_bucket".to_string(),
                },
                PrometheusLabel {
                    name: "le".to_string(),
                    value: "0.1".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 10.0,
                timestamp: 1700000100000,
            }],
            histograms: vec![],
        };

        let bucket_inf = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "request_duration_seconds_bucket".to_string(),
                },
                PrometheusLabel {
                    name: "le".to_string(),
                    value: "+Inf".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 30.0,
                timestamp: 1700000100000,
            }],
            histograms: vec![],
        };

        let count = PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "request_duration_seconds_count".to_string(),
            }],
            samples: vec![PrometheusSample {
                value: 30.0,
                timestamp: 1700000100000,
            }],
            histograms: vec![],
        };

        let sum = PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "request_duration_seconds_sum".to_string(),
            }],
            samples: vec![PrometheusSample {
                value: 5.5,
                timestamp: 1700000100000,
            }],
            histograms: vec![],
        };

        let created = PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "request_duration_seconds_created".to_string(),
            }],
            samples: vec![PrometheusSample {
                value: 1700000000.0, // Unix timestamp in seconds
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![bucket_01, bucket_inf, count, sum, created],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);
        let metric = &result.resource_metrics[0].scope_metrics[0].metrics[0];

        if let Some(Data::Histogram(histogram)) = &metric.data {
            assert_eq!(histogram.data_points.len(), 1);
            let dp = &histogram.data_points[0];
            // start_time should be from _created (1700000000 seconds in nanos)
            assert_eq!(dp.start_time_unix_nano, 1_700_000_000_000_000_000);
        } else {
            panic!("Expected histogram data");
        }
    }

    #[test]
    fn test_gauge_metadata_has_prometheus_type() {
        let ts = PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "temperature".to_string(),
            }],
            samples: vec![PrometheusSample {
                value: 23.5,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![ts],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);
        let metric = &result.resource_metrics[0].scope_metrics[0].metrics[0];

        // Check prometheus.type metadata is "gauge"
        let prom_type = metric
            .metadata
            .iter()
            .find(|kv| kv.key == "prometheus.type")
            .unwrap();
        if let Some(any_value::Value::StringValue(v)) = &prom_type.value.as_ref().unwrap().value {
            assert_eq!(v, "gauge");
        } else {
            panic!("Expected string value for prometheus.type");
        }
    }

    #[test]
    fn test_otel_scope_labels_excluded_from_attributes() {
        // Test that otel_scope_* labels are NOT included in metric attributes
        let labels = vec![
            PrometheusLabel {
                name: "__name__".to_string(),
                value: "test".to_string(),
            },
            PrometheusLabel {
                name: "otel_scope_name".to_string(),
                value: "my-lib".to_string(),
            },
            PrometheusLabel {
                name: "otel_scope_version".to_string(),
                value: "1.0".to_string(),
            },
            PrometheusLabel {
                name: "regular".to_string(),
                value: "value".to_string(),
            },
        ];

        let attrs = convert_labels_to_attributes(&labels);

        // Should only have 1 attribute (regular), otel_scope_* should be excluded
        assert_eq!(attrs.len(), 1);
        assert_eq!(attrs[0].key, "regular");
    }

    // ============================================================
    // Additional Conversion Tests
    // ============================================================

    #[test]
    fn test_multiple_resources_grouped_separately() {
        // Test that time series with different job/instance are grouped into separate resources
        let ts1 = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "cpu_usage".to_string(),
                },
                PrometheusLabel {
                    name: "job".to_string(),
                    value: "service-a".to_string(),
                },
                PrometheusLabel {
                    name: "instance".to_string(),
                    value: "host1:8080".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 0.75,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let ts2 = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "cpu_usage".to_string(),
                },
                PrometheusLabel {
                    name: "job".to_string(),
                    value: "service-b".to_string(),
                },
                PrometheusLabel {
                    name: "instance".to_string(),
                    value: "host2:8080".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 0.50,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![ts1, ts2],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);

        // Should have 2 resource metrics (one per job/instance combo)
        assert_eq!(result.resource_metrics.len(), 2);

        // Verify each resource has correct attributes
        for rm in &result.resource_metrics {
            let resource = rm.resource.as_ref().unwrap();
            let service_name = resource
                .attributes
                .iter()
                .find(|a| a.key == "service.name")
                .unwrap();
            let instance_id = resource
                .attributes
                .iter()
                .find(|a| a.key == "service.instance.id")
                .unwrap();

            if let (
                Some(any_value::Value::StringValue(job)),
                Some(any_value::Value::StringValue(inst)),
            ) = (
                &service_name.value.as_ref().unwrap().value,
                &instance_id.value.as_ref().unwrap().value,
            ) {
                assert!(
                    (job == "service-a" && inst == "host1:8080")
                        || (job == "service-b" && inst == "host2:8080")
                );
            }
        }
    }

    #[test]
    fn test_multiple_samples_in_time_series() {
        // Test that multiple samples in a time series are all converted
        let ts = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "temperature".to_string(),
                },
                PrometheusLabel {
                    name: "location".to_string(),
                    value: "room1".to_string(),
                },
            ],
            samples: vec![
                PrometheusSample {
                    value: 20.0,
                    timestamp: 1700000000000,
                },
                PrometheusSample {
                    value: 21.5,
                    timestamp: 1700000001000,
                },
                PrometheusSample {
                    value: 22.0,
                    timestamp: 1700000002000,
                },
            ],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![ts],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);
        let metric = &result.resource_metrics[0].scope_metrics[0].metrics[0];

        if let Some(Data::Gauge(gauge)) = &metric.data {
            assert_eq!(gauge.data_points.len(), 3);

            // Verify timestamps are correctly converted (ms to ns)
            assert_eq!(
                gauge.data_points[0].time_unix_nano,
                1700000000000 * 1_000_000
            );
            assert_eq!(
                gauge.data_points[1].time_unix_nano,
                1700000001000 * 1_000_000
            );
            assert_eq!(
                gauge.data_points[2].time_unix_nano,
                1700000002000 * 1_000_000
            );

            // Verify values
            if let Some(number_data_point::Value::AsDouble(v)) = gauge.data_points[0].value {
                assert!((v - 20.0).abs() < f64::EPSILON);
            }
            if let Some(number_data_point::Value::AsDouble(v)) = gauge.data_points[1].value {
                assert!((v - 21.5).abs() < f64::EPSILON);
            }
            if let Some(number_data_point::Value::AsDouble(v)) = gauge.data_points[2].value {
                assert!((v - 22.0).abs() < f64::EPSILON);
            }
        } else {
            panic!("Expected gauge data");
        }
    }

    #[test]
    fn test_empty_request() {
        // Test that empty request produces empty result
        let request = PrometheusWriteRequest {
            timeseries: vec![],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);
        assert!(result.resource_metrics.is_empty());
    }

    #[test]
    fn test_metadata_propagation() {
        // Test that help and unit from metadata are propagated to OTEL metric
        let ts = PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "http_request_duration_seconds".to_string(),
            }],
            samples: vec![PrometheusSample {
                value: 0.5,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let metadata = PrometheusMetricMetadata {
            metric_family_name: "http_request_duration_seconds".to_string(),
            metric_type: PrometheusMetricType::Gauge,
            help: "Duration of HTTP requests in seconds".to_string(),
            unit: "seconds".to_string(),
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![ts],
            metadata: vec![metadata],
        };

        let result = prometheus_to_otel_metrics(&request);
        let metric = &result.resource_metrics[0].scope_metrics[0].metrics[0];

        assert_eq!(metric.name, "http_request_duration_seconds");
        assert_eq!(metric.description, "Duration of HTTP requests in seconds");
        assert_eq!(metric.unit, "seconds");
    }

    #[test]
    fn test_mixed_metric_types_in_request() {
        // Test that a request with different metric types is correctly converted
        let gauge_ts = PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "temperature".to_string(),
            }],
            samples: vec![PrometheusSample {
                value: 23.5,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let counter_ts = PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "requests_total".to_string(),
            }],
            samples: vec![PrometheusSample {
                value: 100.0,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![gauge_ts, counter_ts],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);
        let metrics = &result.resource_metrics[0].scope_metrics[0].metrics;

        assert_eq!(metrics.len(), 2);

        // Find gauge and counter
        let gauge = metrics.iter().find(|m| m.name == "temperature");
        let counter = metrics.iter().find(|m| m.name == "requests");

        assert!(gauge.is_some());
        assert!(counter.is_some());

        // Verify types
        assert!(matches!(gauge.unwrap().data, Some(Data::Gauge(_))));
        assert!(matches!(counter.unwrap().data, Some(Data::Sum(_))));
    }

    #[test]
    fn test_timestamp_conversion_accuracy() {
        // Test that timestamps are accurately converted from ms to ns
        let ts = PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "test_metric".to_string(),
            }],
            samples: vec![PrometheusSample {
                value: 1.0,
                timestamp: 1609459200000, // 2021-01-01 00:00:00 UTC in ms
            }],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![ts],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);
        let metric = &result.resource_metrics[0].scope_metrics[0].metrics[0];

        if let Some(Data::Gauge(gauge)) = &metric.data {
            // 1609459200000 ms = 1609459200000000000000 ns
            assert_eq!(
                gauge.data_points[0].time_unix_nano,
                1609459200000 * 1_000_000
            );
        } else {
            panic!("Expected gauge data");
        }
    }

    #[test]
    fn test_histogram_with_multiple_label_dimensions() {
        // Test histogram with additional labels (e.g., method, status)
        let make_bucket = |le: &str, method: &str, value: f64| PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "http_duration_bucket".to_string(),
                },
                PrometheusLabel {
                    name: "le".to_string(),
                    value: le.to_string(),
                },
                PrometheusLabel {
                    name: "method".to_string(),
                    value: method.to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let make_count = |method: &str, value: f64| PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "http_duration_count".to_string(),
                },
                PrometheusLabel {
                    name: "method".to_string(),
                    value: method.to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let make_sum = |method: &str, value: f64| PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "http_duration_sum".to_string(),
                },
                PrometheusLabel {
                    name: "method".to_string(),
                    value: method.to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![
                // GET method histogram
                make_bucket("0.1", "GET", 5.0),
                make_bucket("+Inf", "GET", 10.0),
                make_count("GET", 10.0),
                make_sum("GET", 1.5),
                // POST method histogram
                make_bucket("0.1", "POST", 2.0),
                make_bucket("+Inf", "POST", 5.0),
                make_count("POST", 5.0),
                make_sum("POST", 0.8),
            ],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);
        let metric = &result.resource_metrics[0].scope_metrics[0].metrics[0];

        if let Some(Data::Histogram(histogram)) = &metric.data {
            // Should have 2 data points (one for GET, one for POST)
            assert_eq!(histogram.data_points.len(), 2);

            // Find GET and POST data points
            let get_dp = histogram
                .data_points
                .iter()
                .find(|dp| {
                    dp.attributes.iter().any(|a| {
                        a.key == "method" && {
                            matches!(
                                &a.value.as_ref().unwrap().value,
                                Some(any_value::Value::StringValue(v)) if v == "GET"
                            )
                        }
                    })
                })
                .unwrap();
            let post_dp = histogram
                .data_points
                .iter()
                .find(|dp| {
                    dp.attributes.iter().any(|a| {
                        a.key == "method" && {
                            matches!(
                                &a.value.as_ref().unwrap().value,
                                Some(any_value::Value::StringValue(v)) if v == "POST"
                            )
                        }
                    })
                })
                .unwrap();

            assert_eq!(get_dp.count, 10);
            assert!((get_dp.sum.unwrap() - 1.5).abs() < f64::EPSILON);

            assert_eq!(post_dp.count, 5);
            assert!((post_dp.sum.unwrap() - 0.8).abs() < f64::EPSILON);
        } else {
            panic!("Expected histogram data");
        }
    }

    #[test]
    fn test_counter_aggregation_temporality() {
        // Test that counters have CUMULATIVE aggregation temporality
        let ts = PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "events_total".to_string(),
            }],
            samples: vec![PrometheusSample {
                value: 500.0,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![ts],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);
        let metric = &result.resource_metrics[0].scope_metrics[0].metrics[0];

        if let Some(Data::Sum(sum)) = &metric.data {
            assert!(sum.is_monotonic);
            assert_eq!(
                sum.aggregation_temporality,
                AggregationTemporality::Cumulative as i32
            );
        } else {
            panic!("Expected sum data");
        }
    }

    #[test]
    fn test_label_attributes_preserved() {
        // Test that custom labels become metric attributes
        let ts = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "api_calls".to_string(),
                },
                PrometheusLabel {
                    name: "endpoint".to_string(),
                    value: "/users".to_string(),
                },
                PrometheusLabel {
                    name: "method".to_string(),
                    value: "GET".to_string(),
                },
                PrometheusLabel {
                    name: "status".to_string(),
                    value: "200".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 42.0,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![ts],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);
        let metric = &result.resource_metrics[0].scope_metrics[0].metrics[0];

        if let Some(Data::Gauge(gauge)) = &metric.data {
            let attrs = &gauge.data_points[0].attributes;
            assert_eq!(attrs.len(), 3); // endpoint, method, status

            let find_attr = |key: &str| -> Option<&str> {
                attrs.iter().find(|a| a.key == key).and_then(|a| {
                    if let Some(any_value::Value::StringValue(v)) = &a.value.as_ref()?.value {
                        Some(v.as_str())
                    } else {
                        None
                    }
                })
            };

            assert_eq!(find_attr("endpoint"), Some("/users"));
            assert_eq!(find_attr("method"), Some("GET"));
            assert_eq!(find_attr("status"), Some("200"));
        } else {
            panic!("Expected gauge data");
        }
    }

    #[test]
    fn test_info_metric_type() {
        // Test that _info suffix metrics are handled
        let ts = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "build_info".to_string(),
                },
                PrometheusLabel {
                    name: "version".to_string(),
                    value: "1.2.3".to_string(),
                },
                PrometheusLabel {
                    name: "commit".to_string(),
                    value: "abc123".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 1.0, // Info metrics always have value 1
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![ts],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);
        let metric = &result.resource_metrics[0].scope_metrics[0].metrics[0];

        // Info metrics should be preserved (converted to gauge by default)
        assert_eq!(metric.name, "build_info");

        // Check prometheus.type metadata
        let prom_type = metric
            .metadata
            .iter()
            .find(|kv| kv.key == "prometheus.type")
            .unwrap();
        if let Some(any_value::Value::StringValue(v)) = &prom_type.value.as_ref().unwrap().value {
            assert_eq!(v, "info");
        }
    }

    #[test]
    fn test_empty_job_and_instance() {
        // Test time series without job/instance labels
        let ts = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "standalone_metric".to_string(),
                },
                PrometheusLabel {
                    name: "env".to_string(),
                    value: "prod".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 1.0,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![ts],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);
        let resource = result.resource_metrics[0].resource.as_ref().unwrap();

        // Resource should have no attributes (job/instance are empty)
        assert!(resource.attributes.is_empty());
    }

    #[test]
    fn test_summary_with_multiple_quantiles() {
        // Test summary with various quantile values
        let make_quantile = |q: &str, value: f64| PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "request_latency".to_string(),
                },
                PrometheusLabel {
                    name: "quantile".to_string(),
                    value: q.to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let count = PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "request_latency_count".to_string(),
            }],
            samples: vec![PrometheusSample {
                value: 1000.0,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let sum = PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "request_latency_sum".to_string(),
            }],
            samples: vec![PrometheusSample {
                value: 150.0,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let metadata = PrometheusMetricMetadata {
            metric_family_name: "request_latency".to_string(),
            metric_type: PrometheusMetricType::Summary,
            help: "Request latency".to_string(),
            unit: "seconds".to_string(),
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![
                make_quantile("0.5", 0.05),
                make_quantile("0.9", 0.1),
                make_quantile("0.95", 0.15),
                make_quantile("0.99", 0.3),
                count,
                sum,
            ],
            metadata: vec![metadata],
        };

        let result = prometheus_to_otel_metrics(&request);
        let metric = &result.resource_metrics[0].scope_metrics[0].metrics[0];

        if let Some(Data::Summary(summary)) = &metric.data {
            assert_eq!(summary.data_points.len(), 1);
            let dp = &summary.data_points[0];

            assert_eq!(dp.count, 1000);
            assert!((dp.sum - 150.0).abs() < f64::EPSILON);
            assert_eq!(dp.quantile_values.len(), 4);

            // Quantiles should be sorted
            assert!((dp.quantile_values[0].quantile - 0.5).abs() < f64::EPSILON);
            assert!((dp.quantile_values[0].value - 0.05).abs() < f64::EPSILON);

            assert!((dp.quantile_values[1].quantile - 0.9).abs() < f64::EPSILON);
            assert!((dp.quantile_values[1].value - 0.1).abs() < f64::EPSILON);

            assert!((dp.quantile_values[2].quantile - 0.95).abs() < f64::EPSILON);
            assert!((dp.quantile_values[2].value - 0.15).abs() < f64::EPSILON);

            assert!((dp.quantile_values[3].quantile - 0.99).abs() < f64::EPSILON);
            assert!((dp.quantile_values[3].value - 0.3).abs() < f64::EPSILON);
        } else {
            panic!("Expected summary data");
        }
    }

    #[test]
    fn test_histogram_bucket_delta_calculation() {
        // Test that histogram bucket counts are correctly converted from cumulative to delta
        let make_bucket = |le: &str, cumulative: f64| PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "response_size_bucket".to_string(),
                },
                PrometheusLabel {
                    name: "le".to_string(),
                    value: le.to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: cumulative,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let count = PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "response_size_count".to_string(),
            }],
            samples: vec![PrometheusSample {
                value: 100.0,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let sum = PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "response_size_sum".to_string(),
            }],
            samples: vec![PrometheusSample {
                value: 50000.0,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        // Cumulative buckets: <=100: 20, <=500: 50, <=1000: 80, +Inf: 100
        // Delta counts should be: 20, 30, 30, 20
        let request = PrometheusWriteRequest {
            timeseries: vec![
                make_bucket("100", 20.0),
                make_bucket("500", 50.0),
                make_bucket("1000", 80.0),
                make_bucket("+Inf", 100.0),
                count,
                sum,
            ],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);
        let metric = &result.resource_metrics[0].scope_metrics[0].metrics[0];

        if let Some(Data::Histogram(histogram)) = &metric.data {
            let dp = &histogram.data_points[0];

            // Explicit bounds should not include +Inf
            assert_eq!(dp.explicit_bounds, vec![100.0, 500.0, 1000.0]);

            // Bucket counts should be deltas
            assert_eq!(dp.bucket_counts, vec![20, 30, 30, 20]);
        } else {
            panic!("Expected histogram data");
        }
    }

    #[test]
    fn test_metadata_type_overrides_name_detection() {
        // Test that explicit metadata type takes precedence over name-based detection
        let ts = PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "custom_metric".to_string(), // No suffix, would be detected as gauge
            }],
            samples: vec![PrometheusSample {
                value: 100.0,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let metadata = PrometheusMetricMetadata {
            metric_family_name: "custom_metric".to_string(),
            metric_type: PrometheusMetricType::Counter, // Explicit counter type
            help: "A custom counter".to_string(),
            unit: String::new(),
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![ts],
            metadata: vec![metadata],
        };

        let result = prometheus_to_otel_metrics(&request);
        let metric = &result.resource_metrics[0].scope_metrics[0].metrics[0];

        // Should be Sum (counter) not Gauge despite no _total suffix
        if let Some(Data::Sum(sum)) = &metric.data {
            assert!(sum.is_monotonic);
        } else {
            panic!("Expected sum data based on metadata type");
        }

        // Check prometheus.type metadata
        let prom_type = metric
            .metadata
            .iter()
            .find(|kv| kv.key == "prometheus.type")
            .unwrap();
        if let Some(any_value::Value::StringValue(v)) = &prom_type.value.as_ref().unwrap().value {
            assert_eq!(v, "counter");
        }
    }

    #[test]
    fn test_special_float_values() {
        // Test handling of special float values (infinity, negative values)
        let ts = PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "special_values".to_string(),
            }],
            samples: vec![
                PrometheusSample {
                    value: f64::INFINITY,
                    timestamp: 1700000000000,
                },
                PrometheusSample {
                    value: f64::NEG_INFINITY,
                    timestamp: 1700000001000,
                },
                PrometheusSample {
                    value: -42.5,
                    timestamp: 1700000002000,
                },
                PrometheusSample {
                    value: 0.0,
                    timestamp: 1700000003000,
                },
            ],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![ts],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);
        let metric = &result.resource_metrics[0].scope_metrics[0].metrics[0];

        if let Some(Data::Gauge(gauge)) = &metric.data {
            assert_eq!(gauge.data_points.len(), 4);

            // Check special values are preserved
            if let Some(number_data_point::Value::AsDouble(v)) = gauge.data_points[0].value {
                assert!(v.is_infinite() && v.is_sign_positive());
            }
            if let Some(number_data_point::Value::AsDouble(v)) = gauge.data_points[1].value {
                assert!(v.is_infinite() && v.is_sign_negative());
            }
            if let Some(number_data_point::Value::AsDouble(v)) = gauge.data_points[2].value {
                assert!((v - (-42.5)).abs() < f64::EPSILON);
            }
            if let Some(number_data_point::Value::AsDouble(v)) = gauge.data_points[3].value {
                assert!((v - 0.0).abs() < f64::EPSILON);
            }
        } else {
            panic!("Expected gauge data");
        }
    }

    #[test]
    fn test_counter_multiple_label_combinations() {
        // Test counter with multiple label combinations creates separate data points
        let make_counter = |method: &str, status: &str, value: f64| PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "http_requests_total".to_string(),
                },
                PrometheusLabel {
                    name: "method".to_string(),
                    value: method.to_string(),
                },
                PrometheusLabel {
                    name: "status".to_string(),
                    value: status.to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value,
                timestamp: 1700000000000,
            }],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![
                make_counter("GET", "200", 100.0),
                make_counter("GET", "404", 10.0),
                make_counter("POST", "200", 50.0),
                make_counter("POST", "500", 5.0),
            ],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);
        let metric = &result.resource_metrics[0].scope_metrics[0].metrics[0];

        if let Some(Data::Sum(sum)) = &metric.data {
            assert_eq!(sum.data_points.len(), 4);
            assert!(sum.is_monotonic);

            // Each data point should have method and status attributes
            for dp in &sum.data_points {
                assert_eq!(dp.attributes.len(), 2);
                let has_method = dp.attributes.iter().any(|a| a.key == "method");
                let has_status = dp.attributes.iter().any(|a| a.key == "status");
                assert!(has_method && has_status);
            }
        } else {
            panic!("Expected sum data");
        }
    }

    // ============================================================
    // OTEL to Prometheus Conversion Tests
    // ============================================================

    fn make_otel_gauge(name: &str, value: f64, timestamp_ns: u64) -> Metric {
        Metric {
            name: name.to_string(),
            description: String::new(),
            unit: String::new(),
            data: Some(Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 0,
                    time_unix_nano: timestamp_ns,
                    value: Some(number_data_point::Value::AsDouble(value)),
                    exemplars: vec![],
                    flags: 0,
                }],
            })),
            metadata: vec![],
        }
    }

    fn make_otel_counter(name: &str, value: f64, start_ns: u64, timestamp_ns: u64) -> Metric {
        Metric {
            name: name.to_string(),
            description: String::new(),
            unit: String::new(),
            data: Some(Data::Sum(Sum {
                data_points: vec![NumberDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: start_ns,
                    time_unix_nano: timestamp_ns,
                    value: Some(number_data_point::Value::AsDouble(value)),
                    exemplars: vec![],
                    flags: 0,
                }],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
            })),
            metadata: vec![],
        }
    }

    #[test]
    fn test_otel_to_prometheus_gauge() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![make_otel_gauge(
                        "temperature",
                        23.5,
                        1_700_000_000_000_000_000,
                    )],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let config = OtelToPrometheusConfig::new();
        let result = otel_to_prometheus_metrics(&request, &config);

        assert!(!result.timeseries.is_empty());

        let ts = &result.timeseries[0];
        let name = get_label(&ts.labels, "__name__").unwrap();
        assert_eq!(name, "temperature");
        assert!((ts.samples[0].value - 23.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_otel_to_prometheus_counter_adds_total_suffix() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![make_otel_counter(
                        "http_requests",
                        100.0,
                        1_700_000_000_000_000_000,
                        1_700_000_100_000_000_000,
                    )],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let config = OtelToPrometheusConfig::new();
        let result = otel_to_prometheus_metrics(&request, &config);

        // Should have counter + _created metric
        assert!(!result.timeseries.is_empty());

        // Find the counter time series
        let counter_ts = result
            .timeseries
            .iter()
            .find(|ts| {
                get_label(&ts.labels, "__name__")
                    .map(|n| n == "http_requests_total")
                    .unwrap_or(false)
            })
            .expect("Counter with _total suffix should exist");

        assert!((counter_ts.samples[0].value - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_otel_to_prometheus_counter_generates_created_metric() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![make_otel_counter(
                        "events",
                        500.0,
                        1_700_000_000_000_000_000,
                        1_700_000_100_000_000_000,
                    )],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let config = OtelToPrometheusConfig::new();
        let result = otel_to_prometheus_metrics(&request, &config);

        // Find the _created time series
        let created_ts = result
            .timeseries
            .iter()
            .find(|ts| {
                get_label(&ts.labels, "__name__")
                    .map(|n| n == "events_created")
                    .unwrap_or(false)
            })
            .expect("_created metric should exist");

        // _created value should be start time in seconds
        assert!((created_ts.samples[0].value - 1700000000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_otel_to_prometheus_histogram() {
        let histogram = Metric {
            name: "request_duration".to_string(),
            description: "Request duration".to_string(),
            unit: "seconds".to_string(),
            data: Some(Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 1_700_000_000_000_000_000,
                    time_unix_nano: 1_700_000_100_000_000_000,
                    count: 100,
                    sum: Some(15.5),
                    bucket_counts: vec![10, 30, 40, 20], // Delta counts
                    explicit_bounds: vec![0.1, 0.5, 1.0],
                    exemplars: vec![],
                    flags: 0,
                    min: None,
                    max: None,
                }],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            })),
            metadata: vec![],
        };

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![histogram],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let config = OtelToPrometheusConfig::new();
        let result = otel_to_prometheus_metrics(&request, &config);

        // Should have _bucket, _count, _sum, _created series
        let bucket_series: Vec<_> = result
            .timeseries
            .iter()
            .filter(|ts| {
                get_label(&ts.labels, "__name__")
                    .map(|n| n.contains("_bucket"))
                    .unwrap_or(false)
            })
            .collect();

        // 3 buckets + +Inf bucket = 4 bucket series
        assert_eq!(bucket_series.len(), 4);

        // Check _count exists
        let count_ts = result
            .timeseries
            .iter()
            .find(|ts| {
                get_label(&ts.labels, "__name__")
                    .map(|n| n.contains("_count"))
                    .unwrap_or(false)
            })
            .expect("_count should exist");
        assert!((count_ts.samples[0].value - 100.0).abs() < f64::EPSILON);

        // Check _sum exists
        let sum_ts = result
            .timeseries
            .iter()
            .find(|ts| {
                get_label(&ts.labels, "__name__")
                    .map(|n| n.contains("_sum"))
                    .unwrap_or(false)
            })
            .expect("_sum should exist");
        assert!((sum_ts.samples[0].value - 15.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_otel_to_prometheus_resource_to_labels() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![
                        KeyValue {
                            key: "service.name".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(
                                    "my-service".to_string(),
                                )),
                            }),
                        },
                        KeyValue {
                            key: "service.instance.id".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(
                                    "host1:8080".to_string(),
                                )),
                            }),
                        },
                    ],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![make_otel_gauge(
                        "cpu_usage",
                        0.75,
                        1_700_000_000_000_000_000,
                    )],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let config = OtelToPrometheusConfig::new();
        let result = otel_to_prometheus_metrics(&request, &config);

        let metric_ts = result
            .timeseries
            .iter()
            .find(|ts| {
                get_label(&ts.labels, "__name__")
                    .map(|n| n == "cpu_usage")
                    .unwrap_or(false)
            })
            .expect("cpu_usage should exist");

        // Should have job and instance labels
        assert_eq!(
            get_label(&metric_ts.labels, "job"),
            Some("my-service".to_string())
        );
        assert_eq!(
            get_label(&metric_ts.labels, "instance"),
            Some("host1:8080".to_string())
        );
    }

    #[test]
    fn test_otel_to_prometheus_scope_labels() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "my.library".to_string(),
                        version: "1.0.0".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    metrics: vec![make_otel_gauge("metric", 1.0, 1_700_000_000_000_000_000)],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let config = OtelToPrometheusConfig::new();
        let result = otel_to_prometheus_metrics(&request, &config);

        let ts = &result.timeseries[0];
        assert_eq!(
            get_label(&ts.labels, "otel_scope_name"),
            Some("my.library".to_string())
        );
        assert_eq!(
            get_label(&ts.labels, "otel_scope_version"),
            Some("1.0.0".to_string())
        );
    }

    #[test]
    fn test_otel_to_prometheus_config_disable_total_suffix() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![make_otel_counter(
                        "events",
                        100.0,
                        1_700_000_000_000_000_000,
                        1_700_000_100_000_000_000,
                    )],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let mut config = OtelToPrometheusConfig::new();
        config.add_total_suffix = false;

        let result = otel_to_prometheus_metrics(&request, &config);

        // Counter should NOT have _total suffix
        let counter_ts = result
            .timeseries
            .iter()
            .find(|ts| {
                get_label(&ts.labels, "__name__")
                    .map(|n| n == "events")
                    .unwrap_or(false)
            })
            .expect("Counter without _total suffix should exist");

        assert!((counter_ts.samples[0].value - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_normalize_metric_name() {
        // Replace dots with underscores
        assert_eq!(
            normalize_metric_name("http.request.count"),
            "http_request_count"
        );

        // Keep valid characters
        assert_eq!(
            normalize_metric_name("valid_name:suffix"),
            "valid_name:suffix"
        );

        // Multiple invalid chars become single underscore
        assert_eq!(normalize_metric_name("a..b--c"), "a_b_c");

        // Remove trailing underscore
        assert_eq!(normalize_metric_name("test."), "test");
    }

    #[test]
    fn test_normalize_unit() {
        assert_eq!(normalize_unit("1"), "ratio");
        assert_eq!(normalize_unit("s"), "seconds");
        assert_eq!(normalize_unit("ms"), "milliseconds");
        assert_eq!(normalize_unit("By"), "bytes");
        assert_eq!(normalize_unit("requests/second"), "requests_per_second");
    }

    #[test]
    fn test_build_metric_name() {
        // Base name only
        assert_eq!(build_metric_name("metric", None, None), "metric");

        // With unit suffix
        assert_eq!(
            build_metric_name("request_duration", Some("seconds"), None),
            "request_duration_seconds"
        );

        // With type suffix
        assert_eq!(
            build_metric_name("events", None, Some("total")),
            "events_total"
        );

        // With both
        assert_eq!(
            build_metric_name("events", Some("bytes"), Some("total")),
            "events_bytes_total"
        );

        // Don't duplicate existing suffix
        assert_eq!(
            build_metric_name("events_total", None, Some("total")),
            "events_total"
        );
    }

    #[test]
    fn test_otel_to_prometheus_summary() {
        let summary = Metric {
            name: "request_latency".to_string(),
            description: "Request latency".to_string(),
            unit: String::new(),
            data: Some(Data::Summary(Summary {
                data_points: vec![SummaryDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 1_700_000_000_000_000_000,
                    time_unix_nano: 1_700_000_100_000_000_000,
                    count: 100,
                    sum: 15.5,
                    quantile_values: vec![
                        ValueAtQuantile {
                            quantile: 0.5,
                            value: 0.1,
                        },
                        ValueAtQuantile {
                            quantile: 0.99,
                            value: 0.5,
                        },
                    ],
                    flags: 0,
                }],
            })),
            metadata: vec![],
        };

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![summary],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let config = OtelToPrometheusConfig::new();
        let result = otel_to_prometheus_metrics(&request, &config);

        // Should have quantile series + _count + _sum + _created
        let quantile_series: Vec<_> = result
            .timeseries
            .iter()
            .filter(|ts| ts.labels.iter().any(|l| l.name == "quantile"))
            .collect();

        assert_eq!(quantile_series.len(), 2); // 0.5 and 0.99 quantiles

        // Check _count and _sum
        assert!(result.timeseries.iter().any(|ts| {
            get_label(&ts.labels, "__name__")
                .map(|n| n == "request_latency_count")
                .unwrap_or(false)
        }));
        assert!(result.timeseries.iter().any(|ts| {
            get_label(&ts.labels, "__name__")
                .map(|n| n == "request_latency_sum")
                .unwrap_or(false)
        }));
    }

    #[test]
    fn test_otel_to_prometheus_round_trip_gauge() {
        // Create OTEL gauge
        let original = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("test-service".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "temperature".to_string(),
                        description: "Current temperature".to_string(),
                        unit: String::new(),
                        data: Some(Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![KeyValue {
                                    key: "location".to_string(),
                                    value: Some(AnyValue {
                                        value: Some(any_value::Value::StringValue(
                                            "room1".to_string(),
                                        )),
                                    }),
                                }],
                                start_time_unix_nano: 0,
                                time_unix_nano: 1_700_000_000_000_000_000,
                                value: Some(number_data_point::Value::AsDouble(23.5)),
                                exemplars: vec![],
                                flags: 0,
                            }],
                        })),
                        metadata: vec![],
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        // Convert OTEL → Prometheus
        let config = OtelToPrometheusConfig::new();
        let prom = otel_to_prometheus_metrics(&original, &config);

        // Convert Prometheus → OTEL
        let back_to_otel = prometheus_to_otel_metrics(&prom);

        // Verify key properties preserved
        assert!(!back_to_otel.resource_metrics.is_empty());
        let rm = &back_to_otel.resource_metrics[0];

        // Check resource attributes
        let resource = rm.resource.as_ref().unwrap();
        let service_name = resource
            .attributes
            .iter()
            .find(|a| a.key == "service.name")
            .unwrap();
        if let Some(any_value::Value::StringValue(v)) = &service_name.value.as_ref().unwrap().value
        {
            assert_eq!(v, "test-service");
        }

        // Check metric
        assert!(!rm.scope_metrics[0].metrics.is_empty());
        let metric = &rm.scope_metrics[0].metrics[0];
        assert_eq!(metric.name, "temperature");

        if let Some(Data::Gauge(gauge)) = &metric.data {
            assert!(!gauge.data_points.is_empty());
            if let Some(number_data_point::Value::AsDouble(v)) = gauge.data_points[0].value {
                assert!((v - 23.5).abs() < f64::EPSILON);
            }
        } else {
            panic!("Expected gauge data after round-trip");
        }
    }
}
