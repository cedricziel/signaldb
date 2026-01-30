//! Prometheus → OTEL metrics conversion
//!
//! Converts Prometheus remote_write data to OTEL ExportMetricsServiceRequest for
//! unified storage in SignalDB.

use std::collections::HashMap;

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint,
    ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint, metric::Data, number_data_point,
    summary_data_point::ValueAtQuantile,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use tracing;

use super::proto;
use super::types::{
    BucketSpan, DetectedMetricInfo, OTEL_SCOPE_PREFIX, PrometheusHistogram, PrometheusLabel,
    PrometheusMetricMetadata, PrometheusMetricType, PrometheusSample, PrometheusTimeSeries,
    PrometheusWriteRequest, get_attributes_key, get_label, is_stale_marker, parse_le_bound,
    prometheus_type_to_string,
};

/// Decode a snappy-compressed protobuf WriteRequest from raw bytes.
///
/// This is the entry point for Prometheus remote_write ingestion:
/// 1. Decompress snappy block format
/// 2. Decode protobuf
/// 3. Convert to internal PrometheusWriteRequest format
pub fn decode_prometheus_remote_write(data: &[u8]) -> anyhow::Result<PrometheusWriteRequest> {
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
pub(crate) fn detect_metric_type(
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
