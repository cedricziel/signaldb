//! Prometheus → OTEL metrics conversion
//!
//! Converts Prometheus remote_write data to OTEL ExportMetricsServiceRequest for
//! unified storage in SignalDB.

use std::collections::HashMap;

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
    HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary,
    SummaryDataPoint, exponential_histogram_data_point::Buckets, metric::Data, number_data_point,
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
                        negative_counts: h.negative_counts,
                        positive_spans: h
                            .positive_spans
                            .into_iter()
                            .map(|s| BucketSpan {
                                offset: s.offset,
                                length: s.length,
                            })
                            .collect(),
                        positive_deltas: h.positive_deltas,
                        positive_counts: h.positive_counts,
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
            key_strindex: 0,
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(job.to_string())),
            }),
        });
    }

    if !instance.is_empty() {
        resource_attributes.push(KeyValue {
            key_strindex: 0,
            key: "service.instance.id".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(instance.to_string())),
            }),
        });
    }

    // Extract instrumentation scope from otel_scope_* labels
    let scope = extract_instrumentation_scope(timeseries);

    // Native histogram series (remote_write v2) map directly onto OTLP
    // exponential histograms and bypass the classic suffix-based
    // _bucket/_count/_sum reconstruction.
    let mut native_histogram_groups: HashMap<String, Vec<&PrometheusTimeSeries>> = HashMap::new();

    // Group classic time series by metric base name for histogram/summary reconstruction
    let mut metric_groups: HashMap<String, Vec<&PrometheusTimeSeries>> = HashMap::new();

    for ts in timeseries {
        let metric_name = get_label(&ts.labels, "__name__").unwrap_or_default();

        if !ts.histograms.is_empty() {
            native_histogram_groups
                .entry(metric_name.clone())
                .or_default()
                .push(ts);
            // A native histogram series carries no classic samples; only route it
            // through the classic path if it also has float samples.
            if ts.samples.is_empty() {
                continue;
            }
        }

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

    for (base_name, series) in native_histogram_groups {
        if let Some(metric) = convert_native_histogram_metric(&base_name, &series, metadata_map) {
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
                        key_strindex: 0,
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
            key_strindex: 0,
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

/// Valid Prometheus native histogram schema range for standard base-2
/// exponential buckets. Values outside this range (e.g. -53/127 used for
/// custom-bucket NHCB histograms) cannot be represented as OTLP exponential
/// histograms.
const NATIVE_HISTOGRAM_SCHEMA_RANGE: std::ops::RangeInclusive<i32> = -4..=8;

/// Convert native histogram series (remote_write v2) to an OTLP
/// ExponentialHistogram metric.
///
/// Prometheus native histograms and OTLP exponential histograms share the same
/// base-2 bucket boundaries (`base = 2^(2^-schema)`), so `schema` maps directly
/// onto `scale`. The span/delta encoding is decoded into OTLP's dense per-sign
/// bucket arrays by [`decode_native_buckets`].
///
/// Unconvertible samples (unsupported schema, inconsistent span/delta encoding)
/// are counted and dropped with a warning so the loss is observable.
fn convert_native_histogram_metric(
    base_name: &str,
    series: &[&PrometheusTimeSeries],
    metadata_map: &HashMap<&str, &PrometheusMetricMetadata>,
) -> Option<Metric> {
    let mut data_points = Vec::new();
    let mut dropped_samples = 0usize;

    for ts in series {
        let attributes = convert_labels_to_attributes(&ts.labels);
        let start_time = ts
            .histograms
            .first()
            .map(|h| (h.timestamp as u64) * 1_000_000)
            .unwrap_or(0);

        for histogram in &ts.histograms {
            // Stale markers signal series staleness, not data; skip silently
            // like the classic sample path does.
            if is_stale_marker(histogram.sum) {
                continue;
            }
            match convert_native_histogram_sample(histogram, attributes.clone(), start_time) {
                Some(dp) => data_points.push(dp),
                None => dropped_samples += 1,
            }
        }
    }

    if dropped_samples > 0 {
        tracing::warn!(
            metric = base_name,
            dropped_samples,
            "Dropped native histogram samples that cannot be converted to OTLP exponential histograms"
        );
    }

    if data_points.is_empty() {
        return None;
    }

    let (description, unit) = if let Some(metadata) = metadata_map.get(base_name) {
        (metadata.help.clone(), metadata.unit.clone())
    } else {
        (String::new(), String::new())
    };

    Some(Metric {
        name: base_name.to_string(),
        description,
        unit,
        data: Some(Data::ExponentialHistogram(ExponentialHistogram {
            data_points,
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
        })),
        metadata: vec![KeyValue {
            key_strindex: 0,
            key: "prometheus.type".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(
                    prometheus_type_to_string(PrometheusMetricType::Histogram).to_string(),
                )),
            }),
        }],
    })
}

/// Convert a single native histogram sample to an OTLP
/// ExponentialHistogramDataPoint. Returns `None` (after logging) for samples
/// that cannot be represented.
fn convert_native_histogram_sample(
    histogram: &PrometheusHistogram,
    attributes: Vec<KeyValue>,
    start_time_unix_nano: u64,
) -> Option<ExponentialHistogramDataPoint> {
    if !NATIVE_HISTOGRAM_SCHEMA_RANGE.contains(&histogram.schema) {
        tracing::warn!(
            schema = histogram.schema,
            "Dropping native histogram sample with unsupported schema (custom-bucket histograms cannot map to exponential buckets)"
        );
        return None;
    }

    let positive = match decode_native_buckets(
        &histogram.positive_spans,
        &histogram.positive_deltas,
        &histogram.positive_counts,
    ) {
        Ok(buckets) => buckets,
        Err(e) => {
            tracing::warn!(error = %e, "Dropping native histogram sample with invalid positive buckets");
            return None;
        }
    };
    let negative = match decode_native_buckets(
        &histogram.negative_spans,
        &histogram.negative_deltas,
        &histogram.negative_counts,
    ) {
        Ok(buckets) => buckets,
        Err(e) => {
            tracing::warn!(error = %e, "Dropping native histogram sample with invalid negative buckets");
            return None;
        }
    };

    Some(ExponentialHistogramDataPoint {
        attributes,
        start_time_unix_nano,
        time_unix_nano: (histogram.timestamp as u64) * 1_000_000, // ms to ns
        count: histogram.count,
        sum: Some(histogram.sum),
        scale: histogram.schema,
        zero_count: histogram.zero_count,
        positive,
        negative,
        flags: 0,
        exemplars: vec![],
        min: None,
        max: None,
        zero_threshold: histogram.zero_threshold,
    })
}

/// Decode the Prometheus native histogram span/delta bucket encoding into
/// OTLP's dense bucket encoding.
///
/// Prometheus encodes buckets as spans (`offset` + `length`) over a shared
/// value array. The first span's offset is the absolute bucket index of its
/// first bucket; subsequent spans are offset relative to the bucket after the
/// previous span's last bucket. Integer histograms delta-encode counts (each
/// value relative to the previous bucket's count, starting from 0); float
/// histograms carry absolute counts.
///
/// OTLP uses a single dense `bucket_counts` array starting at `offset`, with
/// gaps filled by zero counts. Prometheus bucket index `i` covers
/// `(base^(i-1), base^i]` while OTLP index `i` covers `(base^i, base^(i+1)]`,
/// so OTLP indexes are shifted down by one.
///
/// Returns `Ok(None)` for histograms without buckets on this sign, and an
/// error for inconsistent encodings (span/count length mismatch, negative
/// counts, overlapping spans).
fn decode_native_buckets(
    spans: &[BucketSpan],
    deltas: &[i64],
    counts: &[f64],
) -> anyhow::Result<Option<Buckets>> {
    let total_buckets: usize = spans.iter().map(|s| s.length as usize).sum();
    let use_float_counts = deltas.is_empty() && !counts.is_empty();
    let value_count = if use_float_counts {
        counts.len()
    } else {
        deltas.len()
    };

    if total_buckets != value_count {
        anyhow::bail!(
            "span lengths sum to {total_buckets} buckets but {value_count} bucket values present"
        );
    }
    if total_buckets == 0 {
        return Ok(None);
    }

    let mut bucket_counts: Vec<u64> = Vec::with_capacity(total_buckets);
    let mut running_count: i64 = 0;
    let mut value_idx = 0usize;
    let mut prom_idx: i64 = 0;
    let mut first_index: Option<i64> = None;

    for (span_idx, span) in spans.iter().enumerate() {
        prom_idx = if span_idx == 0 {
            i64::from(span.offset)
        } else {
            prom_idx + i64::from(span.offset)
        };

        for _ in 0..span.length {
            if let Some(first) = first_index {
                let dense_pos = usize::try_from(prom_idx - first)
                    .map_err(|_| anyhow::anyhow!("out-of-order bucket spans"))?;
                if dense_pos < bucket_counts.len() {
                    anyhow::bail!("overlapping bucket spans");
                }
                // Fill gaps between spans with empty buckets
                bucket_counts.resize(dense_pos, 0);
            } else {
                first_index = Some(prom_idx);
            }

            let count = if use_float_counts {
                let c = counts[value_idx];
                if !c.is_finite() || c < 0.0 {
                    anyhow::bail!("negative or non-finite float bucket count {c}");
                }
                c.round() as u64
            } else {
                running_count = running_count
                    .checked_add(deltas[value_idx])
                    .ok_or_else(|| anyhow::anyhow!("bucket count delta overflow"))?;
                if running_count < 0 {
                    anyhow::bail!("negative cumulative bucket count {running_count}");
                }
                running_count as u64
            };
            bucket_counts.push(count);
            value_idx += 1;
            prom_idx += 1;
        }
    }

    let first = first_index
        .ok_or_else(|| anyhow::anyhow!("no buckets decoded despite non-zero span lengths"))?;
    // OTLP bucket index = Prometheus bucket index - 1 (upper-bound vs
    // lower-bound based indexing).
    let offset = i32::try_from(first - 1)
        .map_err(|_| anyhow::anyhow!("bucket offset {first} out of range"))?;

    Ok(Some(Buckets {
        offset,
        bucket_counts,
    }))
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
            key_strindex: 0,
            key: l.name.clone(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(l.value.clone())),
            }),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spans(spec: &[(i32, u32)]) -> Vec<BucketSpan> {
        spec.iter()
            .map(|(offset, length)| BucketSpan {
                offset: *offset,
                length: *length,
            })
            .collect()
    }

    #[test]
    fn decode_native_buckets_single_span_shifts_index_down_by_one() {
        // Prometheus indexes 1..=3 → OTLP offset 0, cumulative deltas resolved
        let buckets = decode_native_buckets(&spans(&[(1, 3)]), &[2, 1, -1], &[])
            .unwrap()
            .expect("buckets");
        assert_eq!(buckets.offset, 0);
        assert_eq!(buckets.bucket_counts, vec![2, 3, 2]);
    }

    #[test]
    fn decode_native_buckets_fills_gaps_between_spans_with_zeros() {
        // Span 1: prometheus indexes 0,1; span 2 starts at 1 + 1 + 2 = 4
        let buckets = decode_native_buckets(&spans(&[(0, 2), (2, 2)]), &[1, 1, -1, 2], &[])
            .unwrap()
            .expect("buckets");
        assert_eq!(buckets.offset, -1);
        assert_eq!(buckets.bucket_counts, vec![1, 2, 0, 0, 1, 3]);
    }

    #[test]
    fn decode_native_buckets_negative_first_offset() {
        // Prometheus indexes -2,-1 → OTLP offset -3
        let buckets = decode_native_buckets(&spans(&[(-2, 2)]), &[4, 4], &[])
            .unwrap()
            .expect("buckets");
        assert_eq!(buckets.offset, -3);
        assert_eq!(buckets.bucket_counts, vec![4, 8]);
    }

    #[test]
    fn decode_native_buckets_empty_returns_none() {
        assert!(decode_native_buckets(&[], &[], &[]).unwrap().is_none());
        // Zero-length spans with no values are also empty
        assert!(
            decode_native_buckets(&spans(&[(1, 0)]), &[], &[])
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn decode_native_buckets_length_mismatch_is_error() {
        assert!(decode_native_buckets(&spans(&[(0, 2)]), &[1], &[]).is_err());
        assert!(decode_native_buckets(&spans(&[(0, 1)]), &[], &[1.0, 2.0]).is_err());
    }

    #[test]
    fn decode_native_buckets_negative_cumulative_count_is_error() {
        assert!(decode_native_buckets(&spans(&[(0, 2)]), &[1, -2], &[]).is_err());
    }

    #[test]
    fn decode_native_buckets_overlapping_spans_are_error() {
        // Second span jumps backwards over the first
        assert!(decode_native_buckets(&spans(&[(0, 2), (-4, 2)]), &[1, 1, 1, 1], &[]).is_err());
    }

    #[test]
    fn decode_native_buckets_float_counts_are_absolute() {
        let buckets = decode_native_buckets(&spans(&[(2, 2)]), &[], &[1.4, 2.6])
            .unwrap()
            .expect("buckets");
        assert_eq!(buckets.offset, 1);
        assert_eq!(buckets.bucket_counts, vec![1, 3]);
    }

    #[test]
    fn convert_native_histogram_sample_maps_negative_buckets() {
        let histogram = PrometheusHistogram {
            count: 4,
            sum: -3.5,
            schema: 1,
            zero_threshold: 0.001,
            zero_count: 1,
            negative_spans: spans(&[(0, 2)]),
            negative_deltas: vec![2, -1],
            timestamp: 1_700_000_000_000,
            ..Default::default()
        };

        let dp = convert_native_histogram_sample(&histogram, vec![], 0).expect("data point");
        assert_eq!(dp.scale, 1);
        assert_eq!(dp.zero_count, 1);
        assert_eq!(dp.zero_threshold, 0.001);
        assert!(dp.positive.is_none());
        let negative = dp.negative.expect("negative buckets");
        assert_eq!(negative.offset, -1);
        assert_eq!(negative.bucket_counts, vec![2, 1]);
    }

    #[test]
    fn convert_native_histogram_sample_rejects_unsupported_schema() {
        for schema in [-53, -5, 9, 127] {
            let histogram = PrometheusHistogram {
                schema,
                ..Default::default()
            };
            assert!(
                convert_native_histogram_sample(&histogram, vec![], 0).is_none(),
                "schema {schema} should be rejected"
            );
        }
    }
}
