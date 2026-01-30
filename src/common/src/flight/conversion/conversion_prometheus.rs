//! Prometheus to OTEL metrics conversion
//!
//! This module provides conversion from Prometheus remote_write format to OTEL
//! ExportMetricsServiceRequest. It handles:
//! - Metric type detection from naming conventions (_total, _bucket, quantile)
//! - Label to attribute mapping (job -> service.name, instance -> service.instance.id)
//! - UTF-8 metric names (Prometheus 3.0+ compatibility)

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint,
    ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint, metric::Data, number_data_point,
    summary_data_point::ValueAtQuantile,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use std::collections::HashMap;

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
            scope: None,
            metrics,
            schema_url: String::new(),
        }],
        schema_url: String::new(),
    }
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

    let data = match metric_type {
        PrometheusMetricType::Counter => convert_to_sum(series, true),
        PrometheusMetricType::Gauge => convert_to_gauge(series),
        PrometheusMetricType::Histogram | PrometheusMetricType::GaugeHistogram => {
            convert_to_histogram(series)
        }
        PrometheusMetricType::Summary => convert_to_summary(series),
        _ => {
            // Default to gauge for unknown types
            convert_to_gauge(series)
        }
    };

    Some(Metric {
        name: base_name.to_string(),
        description,
        unit,
        data: Some(data),
        metadata: vec![],
    })
}

/// Convert time series to OTEL Gauge
fn convert_to_gauge(series: &[&PrometheusTimeSeries]) -> Data {
    let data_points: Vec<NumberDataPoint> = series
        .iter()
        .flat_map(|ts| {
            ts.samples.iter().map(|sample| NumberDataPoint {
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
fn convert_to_sum(series: &[&PrometheusTimeSeries], is_monotonic: bool) -> Data {
    let data_points: Vec<NumberDataPoint> = series
        .iter()
        .flat_map(|ts| {
            // For cumulative counters, we need start_time from the first sample
            let start_time = ts
                .samples
                .first()
                .map(|s| (s.timestamp as u64) * 1_000_000)
                .unwrap_or(0);

            ts.samples.iter().map(move |sample| NumberDataPoint {
                attributes: convert_labels_to_attributes(&ts.labels),
                start_time_unix_nano: start_time,
                time_unix_nano: (sample.timestamp as u64) * 1_000_000,
                value: Some(number_data_point::Value::AsDouble(sample.value)),
                exemplars: vec![],
                flags: 0,
            })
        })
        .collect();

    Data::Sum(Sum {
        data_points,
        aggregation_temporality: AggregationTemporality::Cumulative as i32,
        is_monotonic,
    })
}

/// Convert histogram component series to OTEL Histogram
fn convert_to_histogram(series: &[&PrometheusTimeSeries]) -> Data {
    // Group series by their attributes (excluding le label)
    let mut bucket_groups: HashMap<String, HistogramBucketGroup> = HashMap::new();

    for ts in series {
        let metric_name = get_label(&ts.labels, "__name__").unwrap_or_default();

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
        {
            group.count = sample.value as u64;
            if !group.timestamps.contains(&sample.timestamp) {
                group.timestamps.push(sample.timestamp);
            }
        } else if metric_name.ends_with("_sum")
            && let Some(sample) = ts.samples.last()
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

            HistogramDataPoint {
                attributes: group.attributes,
                start_time_unix_nano: group
                    .timestamps
                    .first()
                    .map(|t| (*t as u64) * 1_000_000)
                    .unwrap_or(0),
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
fn convert_to_summary(series: &[&PrometheusTimeSeries]) -> Data {
    // Group series by their attributes (excluding quantile label)
    let mut summary_groups: HashMap<String, SummaryGroup> = HashMap::new();

    for ts in series {
        let metric_name = get_label(&ts.labels, "__name__").unwrap_or_default();

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
        {
            group.count = sample.value as u64;
            if !group.timestamps.contains(&sample.timestamp) {
                group.timestamps.push(sample.timestamp);
            }
        } else if metric_name.ends_with("_sum")
            && let Some(sample) = ts.samples.last()
        {
            group.sum = sample.value;
            if !group.timestamps.contains(&sample.timestamp) {
                group.timestamps.push(sample.timestamp);
            }
        } else if let Some(quantile) = get_label(&ts.labels, "quantile")
            && let Ok(q) = quantile.parse::<f64>()
            && let Some(sample) = ts.samples.last()
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

            SummaryDataPoint {
                attributes: group.attributes,
                start_time_unix_nano: group
                    .timestamps
                    .first()
                    .map(|t| (*t as u64) * 1_000_000)
                    .unwrap_or(0),
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
/// Excludes __name__, job, and instance (which go to resource)
fn convert_labels_to_attributes(labels: &[PrometheusLabel]) -> Vec<KeyValue> {
    labels
        .iter()
        .filter(|l| {
            l.name != "__name__"
                && l.name != "job"
                && l.name != "instance"
                && l.name != "le"
                && l.name != "quantile"
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
}
