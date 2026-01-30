//! OTEL → Prometheus metrics conversion
//!
//! Converts OTEL ExportMetricsServiceRequest to Prometheus WriteRequest for
//! PromQL query results and remote_read responses.

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, Gauge, Histogram, Metric, NumberDataPoint, Sum, Summary, metric::Data,
    number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use tracing;

use super::types::{
    PrometheusLabel, PrometheusMetricMetadata, PrometheusMetricType, PrometheusSample,
    PrometheusTimeSeries, PrometheusWriteRequest,
};

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

    // Count attributes excluding service.name and service.instance.id
    // (these are already mapped to job/instance labels)
    let meaningful_attributes_count = resource
        .attributes
        .iter()
        .filter(|kv| kv.key != "service.name" && kv.key != "service.instance.id")
        .count();

    // Skip if no meaningful attributes beyond job/instance mapping
    if meaningful_attributes_count == 0 {
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
pub(crate) fn normalize_metric_name(name: &str) -> String {
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
pub(crate) fn normalize_label_name(name: &str) -> String {
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
pub(crate) fn normalize_unit(unit: &str) -> String {
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
