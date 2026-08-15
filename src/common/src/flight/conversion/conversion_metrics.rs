use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray, UInt64Array,
};
use datafusion::arrow::error::ArrowError;
use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::KeyValue,
    metrics::v1::{ResourceMetrics, ScopeMetrics},
    resource::v1::Resource,
};
use serde_json::{Map, Value as JsonValue};
use std::sync::Arc;

use opentelemetry_proto::tonic::metrics::v1::Exemplar;
use opentelemetry_proto::tonic::metrics::v1::ExponentialHistogramDataPoint;
use opentelemetry_proto::tonic::metrics::v1::HistogramDataPoint;
use opentelemetry_proto::tonic::metrics::v1::NumberDataPoint;
use opentelemetry_proto::tonic::metrics::v1::SummaryDataPoint;
use opentelemetry_proto::tonic::metrics::v1::exemplar;
use opentelemetry_proto::tonic::metrics::v1::exponential_histogram_data_point::Buckets;
use opentelemetry_proto::tonic::metrics::v1::number_data_point;
use opentelemetry_proto::tonic::metrics::v1::{
    ExponentialHistogram, Gauge, Histogram, Metric, Sum, Summary, metric::Data,
};
use std::collections::HashMap;

use crate::flight::conversion::conversion_common::{
    extract_resource_json, extract_value, f64_to_json, json_to_f64, json_value_to_any_value,
};
use crate::flight::schema::FlightSchemas;

use super::extract_scope_json;

/// Convert OTLP metric data to Arrow RecordBatch using the Flight metric schema
///
/// # Errors
///
/// Returns an error if the Arrow arrays cannot be assembled into a
/// `RecordBatch`. Callers must reject the export instead of acknowledging
/// it, otherwise the data would be silently lost.
pub fn otlp_metrics_to_arrow(
    request: &ExportMetricsServiceRequest,
) -> Result<datafusion::arrow::record_batch::RecordBatch, ArrowError> {
    let schemas = FlightSchemas::new();
    let schema = schemas.metric_schema.clone();

    // Extract metrics from the request
    let mut names = Vec::new();
    let mut descriptions = Vec::new();
    let mut units = Vec::new();
    let mut start_times = Vec::new();
    let mut times = Vec::new();
    let mut attributes_jsons = Vec::new();
    let mut resource_jsons = Vec::new();
    let mut scope_jsons = Vec::new();
    let mut metric_types = Vec::new();
    let mut data_jsons = Vec::new();
    let mut aggregation_temporities = Vec::new();
    let mut is_monotonics = Vec::new();

    for resource_metrics in &request.resource_metrics {
        // Extract resource attributes as JSON
        let resource_json = extract_resource_json(&resource_metrics.resource);

        for scope_metrics in &resource_metrics.scope_metrics {
            // Extract scope attributes as JSON
            let scope_json = if let Some(scope) = &scope_metrics.scope {
                extract_scope_json(&Some(scope.clone()))
            } else {
                "{}".to_string()
            };

            for metric in &scope_metrics.metrics {
                // Extract common metric fields
                let name = metric.name.clone();
                let description = metric.description.clone();
                let unit = metric.unit.clone();

                // Extract metric-specific fields based on data type
                let (metric_type, data_json, start_time, time, agg_temporality, is_monotonic) = match &metric.data {
                    Some(data) => match data {
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(gauge) => {
                            let data_points = extract_number_data_points(&gauge.data_points);
                            let data_json = serde_json::to_string(&data_points).unwrap_or_else(|_| "[]".to_string());

                            // Get time from the first data point if available
                            let (start_time, time) = if !gauge.data_points.is_empty() {
                                (
                                    gauge.data_points[0].start_time_unix_nano,
                                    gauge.data_points[0].time_unix_nano,
                                )
                            } else {
                                (0, 0)
                            };

                            ("gauge".to_string(), data_json, start_time, time, 0, false)
                        },
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(sum) => {
                            let data_points = extract_number_data_points(&sum.data_points);
                            let data_json = serde_json::to_string(&data_points).unwrap_or_else(|_| "[]".to_string());

                            // Get time from the first data point if available
                            let (start_time, time) = if !sum.data_points.is_empty() {
                                (
                                    sum.data_points[0].start_time_unix_nano,
                                    sum.data_points[0].time_unix_nano,
                                )
                            } else {
                                (0, 0)
                            };

                            (
                                "sum".to_string(),
                                data_json,
                                start_time,
                                time,
                                sum.aggregation_temporality,
                                sum.is_monotonic,
                            )
                        },
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::Histogram(histogram) => {
                            let data_points = extract_histogram_data_points(&histogram.data_points);
                            let data_json = serde_json::to_string(&data_points).unwrap_or_else(|_| "[]".to_string());

                            // Get time from the first data point if available
                            let (start_time, time) = if !histogram.data_points.is_empty() {
                                (
                                    histogram.data_points[0].start_time_unix_nano,
                                    histogram.data_points[0].time_unix_nano,
                                )
                            } else {
                                (0, 0)
                            };

                            (
                                "histogram".to_string(),
                                data_json,
                                start_time,
                                time,
                                histogram.aggregation_temporality,
                                false,
                            )
                        },
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::ExponentialHistogram(exp_histogram) => {
                            let data_points = extract_exponential_histogram_data_points(&exp_histogram.data_points);
                            let data_json = serde_json::to_string(&data_points).unwrap_or_else(|_| "[]".to_string());

                            // Get time from the first data point if available
                            let (start_time, time) = if !exp_histogram.data_points.is_empty() {
                                (
                                    exp_histogram.data_points[0].start_time_unix_nano,
                                    exp_histogram.data_points[0].time_unix_nano,
                                )
                            } else {
                                (0, 0)
                            };

                            (
                                "exponential_histogram".to_string(),
                                data_json,
                                start_time,
                                time,
                                exp_histogram.aggregation_temporality,
                                false,
                            )
                        },
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::Summary(summary) => {
                            let data_points = extract_summary_data_points(&summary.data_points);
                            let data_json = serde_json::to_string(&data_points).unwrap_or_else(|_| "[]".to_string());

                            // Get time from the first data point if available
                            let (start_time, time) = if !summary.data_points.is_empty() {
                                (
                                    summary.data_points[0].start_time_unix_nano,
                                    summary.data_points[0].time_unix_nano,
                                )
                            } else {
                                (0, 0)
                            };

                            ("summary".to_string(), data_json, start_time, time, 0, false)
                        },
                    },
                    None => ("unknown".to_string(), "{}".to_string(), 0, 0, 0, false),
                };

                // Extract metadata attributes as JSON
                let mut attr_map = Map::new();
                for attr in &metric.metadata {
                    attr_map.insert(attr.key.clone(), extract_value(&attr.value));
                }
                let attributes_json =
                    serde_json::to_string(&attr_map).unwrap_or_else(|_| "{}".to_string());

                // Add to arrays
                names.push(name);
                descriptions.push(description);
                units.push(unit);
                start_times.push(start_time);
                times.push(time);
                attributes_jsons.push(attributes_json);
                resource_jsons.push(resource_json.clone());
                scope_jsons.push(scope_json.clone());
                metric_types.push(metric_type);
                data_jsons.push(data_json);
                aggregation_temporities.push(agg_temporality);
                is_monotonics.push(is_monotonic);
            }
        }
    }

    // Create Arrow arrays from the extracted data
    let name_array: ArrayRef = Arc::new(StringArray::from(names));
    let description_array: ArrayRef = Arc::new(StringArray::from(descriptions));
    let unit_array: ArrayRef = Arc::new(StringArray::from(units));
    let start_time_array: ArrayRef = Arc::new(UInt64Array::from(start_times));
    let time_array: ArrayRef = Arc::new(UInt64Array::from(times));
    let attributes_json_array: ArrayRef = Arc::new(StringArray::from(attributes_jsons));
    let resource_json_array: ArrayRef = Arc::new(StringArray::from(resource_jsons));
    let scope_json_array: ArrayRef = Arc::new(StringArray::from(scope_jsons));
    let metric_type_array: ArrayRef = Arc::new(StringArray::from(metric_types));
    let data_json_array: ArrayRef = Arc::new(StringArray::from(data_jsons));
    let aggregation_temporality_array: ArrayRef =
        Arc::new(Int32Array::from(aggregation_temporities));
    let is_monotonic_array: ArrayRef = Arc::new(BooleanArray::from(is_monotonics));

    // Create and return the RecordBatch. Assembly failures must propagate:
    // swallowing them into an empty batch would ACK data that was never
    // stored (silent data loss, issue #926).
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            name_array,
            description_array,
            unit_array,
            start_time_array,
            time_array,
            attributes_json_array,
            resource_json_array,
            scope_json_array,
            metric_type_array,
            data_json_array,
            aggregation_temporality_array,
            is_monotonic_array,
        ],
    )
}

/// Extract number data points from OTLP NumberDataPoint
fn extract_number_data_points(
    data_points: &[opentelemetry_proto::tonic::metrics::v1::NumberDataPoint],
) -> Vec<JsonValue> {
    let mut result = Vec::new();

    for point in data_points {
        let mut point_map = Map::new();

        // Add timestamps
        point_map.insert(
            "start_time_unix_nano".to_string(),
            JsonValue::Number(serde_json::Number::from(point.start_time_unix_nano)),
        );
        point_map.insert(
            "time_unix_nano".to_string(),
            JsonValue::Number(serde_json::Number::from(point.time_unix_nano)),
        );

        // Add value
        match &point.value {
            Some(value) => match value {
                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(v) => {
                    // NaN/±Inf are spelled out, never `null` (#1061).
                    point_map.insert("value".to_string(), f64_to_json(*v));
                }
                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(v) => {
                    point_map.insert(
                        "value".to_string(),
                        JsonValue::Number(serde_json::Number::from(*v)),
                    );
                }
            },
            None => {
                point_map.insert("value".to_string(), JsonValue::Null);
            }
        }

        // Add attributes
        let mut attr_map = Map::new();
        for attr in &point.attributes {
            attr_map.insert(attr.key.clone(), extract_value(&attr.value));
        }
        point_map.insert("attributes".to_string(), JsonValue::Object(attr_map));

        // Add flags
        point_map.insert(
            "flags".to_string(),
            JsonValue::Number(serde_json::Number::from(point.flags)),
        );

        // Add exemplars if present
        if !point.exemplars.is_empty() {
            let exemplars = extract_exemplars(&point.exemplars);
            point_map.insert("exemplars".to_string(), JsonValue::Array(exemplars));
        }

        result.push(JsonValue::Object(point_map));
    }

    result
}

/// Convert Arrow RecordBatch to OTLP ExportMetricsServiceRequest
pub fn arrow_to_otlp_metrics(batch: &RecordBatch) -> ExportMetricsServiceRequest {
    let columns = batch.columns();

    // Extract columns by index based on the schema order in otlp_metrics_to_arrow
    let name_array = columns[0]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name column should be StringArray");
    let description_array = columns[1]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("description column should be StringArray");
    let unit_array = columns[2]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("unit column should be StringArray");
    let start_time_array = columns[3]
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("start_time column should be UInt64Array");
    let time_array = columns[4]
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("time column should be UInt64Array");
    let attributes_json_array = columns[5]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("attributes_json column should be StringArray");
    let resource_json_array = columns[6]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("resource_json column should be StringArray");
    let scope_json_array = columns[7]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("scope_json column should be StringArray");
    let metric_type_array = columns[8]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("metric_type column should be StringArray");
    let data_json_array = columns[9]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("data_json column should be StringArray");
    let aggregation_temporality_array = columns[10]
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("aggregation_temporality column should be Int32Array");
    let is_monotonic_array = columns[11]
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("is_monotonic column should be BooleanArray");

    // Group metrics by resource and scope
    let mut resource_scope_metrics_map: HashMap<String, HashMap<String, Vec<Metric>>> =
        HashMap::new();
    let mut resource_attributes_map: HashMap<String, Vec<KeyValue>> = HashMap::new();
    let mut scope_attributes_map: HashMap<
        String,
        opentelemetry_proto::tonic::common::v1::InstrumentationScope,
    > = HashMap::new();

    for row in 0..batch.num_rows() {
        let name = name_array.value(row).to_string();
        let description = description_array.value(row).to_string();
        let unit = unit_array.value(row).to_string();
        let start_time = start_time_array.value(row);
        let time = time_array.value(row);
        let attributes_json_str = attributes_json_array.value(row);
        let resource_json_str = resource_json_array.value(row);
        let scope_json_str = scope_json_array.value(row);
        let metric_type = metric_type_array.value(row);
        let data_json_str = data_json_array.value(row);
        let aggregation_temporality = aggregation_temporality_array.value(row);
        let is_monotonic = is_monotonic_array.value(row);

        // Parse attributes JSON string to KeyValue vector
        let metadata: Vec<KeyValue> = if let Ok(serde_json::Value::Object(map)) =
            serde_json::from_str::<serde_json::Value>(attributes_json_str)
        {
            map.into_iter()
                .map(|(k, v)| KeyValue {
                    key_strindex: 0,
                    key: k,
                    value: Some(json_value_to_any_value(&v)),
                })
                .collect()
        } else {
            vec![]
        };

        // Parse resource JSON string to KeyValue vector
        let resource_attributes: Vec<KeyValue> = if let Ok(serde_json::Value::Object(map)) =
            serde_json::from_str::<serde_json::Value>(resource_json_str)
        {
            map.into_iter()
                .map(|(k, v)| KeyValue {
                    key_strindex: 0,
                    key: k,
                    value: Some(json_value_to_any_value(&v)),
                })
                .collect()
        } else {
            vec![]
        };

        // Parse scope JSON string to InstrumentationScope
        let scope = if let Ok(serde_json::Value::Object(map)) =
            serde_json::from_str::<serde_json::Value>(scope_json_str)
        {
            let name = map
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let version = map
                .get("version")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let mut scope_attributes = Vec::new();
            if let Some(serde_json::Value::Object(attrs_map)) = map.get("attributes") {
                for (k, v) in attrs_map {
                    scope_attributes.push(KeyValue {
                        key_strindex: 0,
                        key: k.clone(),
                        value: Some(json_value_to_any_value(v)),
                    });
                }
            }

            opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                name,
                version,
                attributes: scope_attributes,
                dropped_attributes_count: 0,
            }
        } else {
            opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                name: "".to_string(),
                version: "".to_string(),
                attributes: vec![],
                dropped_attributes_count: 0,
            }
        };

        // Parse data JSON string to appropriate metric data
        let data = match metric_type {
            "gauge" => {
                let data_points = parse_number_data_points(data_json_str, start_time, time);
                Some(Data::Gauge(Gauge { data_points }))
            }
            "sum" => {
                let data_points = parse_number_data_points(data_json_str, start_time, time);
                Some(Data::Sum(Sum {
                    data_points,
                    aggregation_temporality,
                    is_monotonic,
                }))
            }
            "histogram" => {
                let data_points = parse_histogram_data_points(data_json_str, start_time, time);
                Some(Data::Histogram(Histogram {
                    data_points,
                    aggregation_temporality,
                }))
            }
            "exponential_histogram" => {
                let data_points =
                    parse_exponential_histogram_data_points(data_json_str, start_time, time);
                Some(Data::ExponentialHistogram(ExponentialHistogram {
                    data_points,
                    aggregation_temporality,
                }))
            }
            "summary" => {
                let data_points = parse_summary_data_points(data_json_str, start_time, time);
                Some(Data::Summary(Summary { data_points }))
            }
            _ => None,
        };

        // Construct the Metric
        let metric = Metric {
            name,
            description,
            unit,
            data,
            metadata,
        };

        // Use resource_json_str and scope_json_str as keys for grouping
        let scope_metrics_map = resource_scope_metrics_map
            .entry(resource_json_str.to_string())
            .or_default();

        let metrics = scope_metrics_map
            .entry(scope_json_str.to_string())
            .or_default();

        metrics.push(metric);

        // Store resource attributes for this resource
        resource_attributes_map.insert(resource_json_str.to_string(), resource_attributes);

        // Store scope for this scope
        scope_attributes_map.insert(scope_json_str.to_string(), scope);
    }

    // Construct ResourceMetrics
    let mut resource_metrics = Vec::new();
    for (resource_key, scope_metrics_map) in resource_scope_metrics_map {
        let mut scope_metrics_vec = Vec::new();

        for (scope_key, metrics) in scope_metrics_map {
            let scope_metrics = ScopeMetrics {
                scope: Some(scope_attributes_map.get(&scope_key).unwrap().clone()),
                metrics,
                schema_url: "".to_string(),
            };

            scope_metrics_vec.push(scope_metrics);
        }

        let resource_metrics_entry = ResourceMetrics {
            resource: Some(Resource {
                attributes: resource_attributes_map.get(&resource_key).unwrap().clone(),
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_metrics: scope_metrics_vec,
            schema_url: "".to_string(),
        };

        resource_metrics.push(resource_metrics_entry);
    }

    ExportMetricsServiceRequest { resource_metrics }
}

/// Parse number data points from JSON string
fn parse_number_data_points(
    json_str: &str,
    default_start_time: u64,
    default_time: u64,
) -> Vec<opentelemetry_proto::tonic::metrics::v1::NumberDataPoint> {
    let mut data_points = Vec::new();

    if let Ok(serde_json::Value::Array(points)) =
        serde_json::from_str::<serde_json::Value>(json_str)
    {
        for point_val in points {
            if let serde_json::Value::Object(point_map) = point_val {
                // Extract timestamps
                let start_time_unix_nano = point_map
                    .get("start_time_unix_nano")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(default_start_time);
                let time_unix_nano = point_map
                    .get("time_unix_nano")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(default_time);

                // Extract value
                let value = if let Some(val) = point_map.get("value") {
                    if val.is_i64() {
                        Some(number_data_point::Value::AsInt(val.as_i64().unwrap()))
                    } else if val.is_f64() {
                        Some(number_data_point::Value::AsDouble(val.as_f64().unwrap()))
                    } else if let Some(v) = val.as_str().and(json_to_f64(val)) {
                        // "NaN" / "+Inf" / "-Inf" sentinels (see f64_to_json).
                        Some(number_data_point::Value::AsDouble(v))
                    } else if val.is_u64() {
                        let u_val = val.as_u64().unwrap();
                        if u_val <= i64::MAX as u64 {
                            Some(number_data_point::Value::AsInt(u_val as i64))
                        } else {
                            Some(number_data_point::Value::AsDouble(u_val as f64))
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                // Extract attributes
                let attributes = if let Some(serde_json::Value::Object(attrs_map)) =
                    point_map.get("attributes")
                {
                    attrs_map
                        .iter()
                        .map(|(k, v)| KeyValue {
                            key_strindex: 0,
                            key: k.clone(),
                            value: Some(json_value_to_any_value(v)),
                        })
                        .collect()
                } else {
                    vec![]
                };

                // Extract flags
                let flags = point_map.get("flags").and_then(|v| v.as_u64()).unwrap_or(0) as u32;

                // Extract exemplars
                let exemplars = if let Some(exemplars_val) = point_map.get("exemplars") {
                    parse_exemplars(exemplars_val)
                } else {
                    vec![]
                };

                // Create NumberDataPoint
                let data_point = NumberDataPoint {
                    attributes,
                    start_time_unix_nano,
                    time_unix_nano,
                    value,
                    exemplars,
                    flags,
                };

                data_points.push(data_point);
            }
        }
    }

    data_points
}

/// Parse histogram data points from JSON string
fn parse_histogram_data_points(
    json_str: &str,
    default_start_time: u64,
    default_time: u64,
) -> Vec<HistogramDataPoint> {
    let mut data_points = Vec::new();

    if let Ok(serde_json::Value::Array(points)) =
        serde_json::from_str::<serde_json::Value>(json_str)
    {
        for point_val in points {
            if let serde_json::Value::Object(point_map) = point_val {
                // Extract timestamps
                let start_time_unix_nano = point_map
                    .get("start_time_unix_nano")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(default_start_time);
                let time_unix_nano = point_map
                    .get("time_unix_nano")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(default_time);

                // Extract count and sum
                let count = point_map.get("count").and_then(|v| v.as_u64()).unwrap_or(0);
                let sum = point_map.get("sum").and_then(json_to_f64);

                // Extract min/max
                let min = point_map.get("min").and_then(json_to_f64);
                let max = point_map.get("max").and_then(json_to_f64);

                // Extract bucket boundaries and counts
                let explicit_bounds = if let Some(serde_json::Value::Array(bounds_arr)) =
                    point_map.get("explicit_bounds")
                {
                    bounds_arr.iter().filter_map(json_to_f64).collect()
                } else {
                    vec![]
                };

                let bucket_counts = if let Some(serde_json::Value::Array(counts_arr)) =
                    point_map.get("bucket_counts")
                {
                    counts_arr.iter().filter_map(|v| v.as_u64()).collect()
                } else {
                    vec![]
                };

                // Extract attributes
                let attributes = if let Some(serde_json::Value::Object(attrs_map)) =
                    point_map.get("attributes")
                {
                    attrs_map
                        .iter()
                        .map(|(k, v)| KeyValue {
                            key_strindex: 0,
                            key: k.clone(),
                            value: Some(json_value_to_any_value(v)),
                        })
                        .collect()
                } else {
                    vec![]
                };

                // Extract flags
                let flags = point_map.get("flags").and_then(|v| v.as_u64()).unwrap_or(0) as u32;

                // Extract exemplars
                let exemplars = if let Some(exemplars_val) = point_map.get("exemplars") {
                    parse_exemplars(exemplars_val)
                } else {
                    vec![]
                };

                // Create HistogramDataPoint
                let data_point = HistogramDataPoint {
                    attributes,
                    start_time_unix_nano,
                    time_unix_nano,
                    count,
                    sum,
                    bucket_counts,
                    explicit_bounds,
                    exemplars,
                    flags,
                    min,
                    max,
                };

                data_points.push(data_point);
            }
        }
    }

    data_points
}

/// Parse exponential histogram data points from JSON string
fn parse_exponential_histogram_data_points(
    json_str: &str,
    default_start_time: u64,
    default_time: u64,
) -> Vec<ExponentialHistogramDataPoint> {
    let mut data_points = Vec::new();

    if let Ok(serde_json::Value::Array(points)) =
        serde_json::from_str::<serde_json::Value>(json_str)
    {
        for point_val in points {
            if let serde_json::Value::Object(point_map) = point_val {
                // Extract timestamps
                let start_time_unix_nano = point_map
                    .get("start_time_unix_nano")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(default_start_time);
                let time_unix_nano = point_map
                    .get("time_unix_nano")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(default_time);

                // Extract count, sum, and scale
                let count = point_map.get("count").and_then(|v| v.as_u64()).unwrap_or(0);
                let sum = point_map.get("sum").and_then(json_to_f64);
                let scale = point_map.get("scale").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                let zero_count = point_map
                    .get("zero_count")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);

                // Extract min/max
                let min = point_map.get("min").and_then(json_to_f64);
                let max = point_map.get("max").and_then(json_to_f64);

                // Extract positive and negative buckets
                let positive = if let Some(serde_json::Value::Object(pos_map)) =
                    point_map.get("positive")
                {
                    let offset = pos_map.get("offset").and_then(|v| v.as_i64()).unwrap_or(0) as i32;

                    let bucket_counts = if let Some(serde_json::Value::Array(counts_arr)) =
                        pos_map.get("bucket_counts")
                    {
                        counts_arr.iter().filter_map(|v| v.as_u64()).collect()
                    } else {
                        vec![]
                    };

                    Some(Buckets {
                        offset,
                        bucket_counts,
                    })
                } else {
                    None
                };

                let negative = if let Some(serde_json::Value::Object(neg_map)) =
                    point_map.get("negative")
                {
                    let offset = neg_map.get("offset").and_then(|v| v.as_i64()).unwrap_or(0) as i32;

                    let bucket_counts = if let Some(serde_json::Value::Array(counts_arr)) =
                        neg_map.get("bucket_counts")
                    {
                        counts_arr.iter().filter_map(|v| v.as_u64()).collect()
                    } else {
                        vec![]
                    };

                    Some(Buckets {
                        offset,
                        bucket_counts,
                    })
                } else {
                    None
                };

                // Extract zero threshold
                let zero_threshold = point_map
                    .get("zero_threshold")
                    .and_then(json_to_f64)
                    .unwrap_or(0.0);

                // Extract attributes
                let attributes = if let Some(serde_json::Value::Object(attrs_map)) =
                    point_map.get("attributes")
                {
                    attrs_map
                        .iter()
                        .map(|(k, v)| KeyValue {
                            key_strindex: 0,
                            key: k.clone(),
                            value: Some(json_value_to_any_value(v)),
                        })
                        .collect()
                } else {
                    vec![]
                };

                // Extract flags
                let flags = point_map.get("flags").and_then(|v| v.as_u64()).unwrap_or(0) as u32;

                // Extract exemplars
                let exemplars = if let Some(exemplars_val) = point_map.get("exemplars") {
                    parse_exemplars(exemplars_val)
                } else {
                    vec![]
                };

                // Create ExponentialHistogramDataPoint
                let data_point = ExponentialHistogramDataPoint {
                    attributes,
                    start_time_unix_nano,
                    time_unix_nano,
                    count,
                    sum,
                    scale,
                    zero_count,
                    positive,
                    negative,
                    flags,
                    exemplars,
                    zero_threshold,
                    min,
                    max,
                };

                data_points.push(data_point);
            }
        }
    }

    data_points
}

/// Parse summary data points from JSON string
fn parse_summary_data_points(
    json_str: &str,
    default_start_time: u64,
    default_time: u64,
) -> Vec<SummaryDataPoint> {
    let mut data_points = Vec::new();

    if let Ok(serde_json::Value::Array(points)) =
        serde_json::from_str::<serde_json::Value>(json_str)
    {
        for point_val in points {
            if let serde_json::Value::Object(point_map) = point_val {
                // Extract timestamps
                let start_time_unix_nano = point_map
                    .get("start_time_unix_nano")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(default_start_time);
                let time_unix_nano = point_map
                    .get("time_unix_nano")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(default_time);

                // Extract count and sum
                let count = point_map.get("count").and_then(|v| v.as_u64()).unwrap_or(0);
                let sum = point_map.get("sum").and_then(json_to_f64).unwrap_or(0.0);

                // Extract quantile values
                let quantile_values = if let Some(serde_json::Value::Array(quantiles_arr)) =
                    point_map.get("quantile_values")
                {
                    quantiles_arr.iter()
                            .filter_map(|q| {
                                if let serde_json::Value::Object(q_map) = q {
                                    let quantile = q_map.get("quantile")
                                        .and_then(json_to_f64)
                                        .unwrap_or(0.0);
                                    let value = q_map.get("value")
                                        .and_then(json_to_f64)
                                        .unwrap_or(0.0);

                                    Some(opentelemetry_proto::tonic::metrics::v1::summary_data_point::ValueAtQuantile {
                                        quantile,
                                        value,
                                    })
                                } else {
                                    None
                                }
                            })
                            .collect()
                } else {
                    vec![]
                };

                // Extract attributes
                let attributes = if let Some(serde_json::Value::Object(attrs_map)) =
                    point_map.get("attributes")
                {
                    attrs_map
                        .iter()
                        .map(|(k, v)| KeyValue {
                            key_strindex: 0,
                            key: k.clone(),
                            value: Some(json_value_to_any_value(v)),
                        })
                        .collect()
                } else {
                    vec![]
                };

                // Extract flags
                let flags = point_map.get("flags").and_then(|v| v.as_u64()).unwrap_or(0) as u32;

                // Create SummaryDataPoint
                let data_point = SummaryDataPoint {
                    attributes,
                    start_time_unix_nano,
                    time_unix_nano,
                    count,
                    sum,
                    quantile_values,
                    flags,
                };

                data_points.push(data_point);
            }
        }
    }

    data_points
}

/// Parse exemplars from JSON value
fn parse_exemplars(exemplars_val: &serde_json::Value) -> Vec<Exemplar> {
    let mut exemplars = Vec::new();

    if let serde_json::Value::Array(exemplars_arr) = exemplars_val {
        for exemplar_val in exemplars_arr {
            if let serde_json::Value::Object(exemplar_map) = exemplar_val {
                // Extract timestamp
                let time_unix_nano = exemplar_map
                    .get("time_unix_nano")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);

                // Extract value
                let value = if let Some(val) = exemplar_map.get("value") {
                    if val.is_i64() {
                        Some(exemplar::Value::AsInt(val.as_i64().unwrap()))
                    } else if val.is_f64() {
                        Some(exemplar::Value::AsDouble(val.as_f64().unwrap()))
                    } else if let Some(v) = val.as_str().and(json_to_f64(val)) {
                        Some(exemplar::Value::AsDouble(v))
                    } else if val.is_u64() {
                        let u_val = val.as_u64().unwrap();
                        if u_val <= i64::MAX as u64 {
                            Some(exemplar::Value::AsInt(u_val as i64))
                        } else {
                            Some(exemplar::Value::AsDouble(u_val as f64))
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                // Extract filtered attributes
                let filtered_attributes = if let Some(serde_json::Value::Object(attrs_map)) =
                    exemplar_map.get("filtered_attributes")
                {
                    attrs_map
                        .iter()
                        .map(|(k, v)| KeyValue {
                            key_strindex: 0,
                            key: k.clone(),
                            value: Some(json_value_to_any_value(v)),
                        })
                        .collect()
                } else {
                    vec![]
                };

                // Extract trace and span IDs
                let trace_id = exemplar_map
                    .get("trace_id")
                    .and_then(|v| v.as_str())
                    .map(|s| hex::decode(s).unwrap_or_default())
                    .unwrap_or_default();

                let span_id = exemplar_map
                    .get("span_id")
                    .and_then(|v| v.as_str())
                    .map(|s| hex::decode(s).unwrap_or_default())
                    .unwrap_or_default();

                // Create Exemplar
                let exemplar = Exemplar {
                    filtered_attributes,
                    time_unix_nano,
                    value,
                    span_id,
                    trace_id,
                };

                exemplars.push(exemplar);
            }
        }
    }

    exemplars
}

/// Extract histogram data points from OTLP HistogramDataPoint
fn extract_histogram_data_points(
    data_points: &[opentelemetry_proto::tonic::metrics::v1::HistogramDataPoint],
) -> Vec<JsonValue> {
    let mut result = Vec::new();

    for point in data_points {
        let mut point_map = Map::new();

        // Add timestamps
        point_map.insert(
            "start_time_unix_nano".to_string(),
            JsonValue::Number(serde_json::Number::from(point.start_time_unix_nano)),
        );
        point_map.insert(
            "time_unix_nano".to_string(),
            JsonValue::Number(serde_json::Number::from(point.time_unix_nano)),
        );

        // Add count and sum
        point_map.insert(
            "count".to_string(),
            JsonValue::Number(serde_json::Number::from(point.count)),
        );
        if let Some(sum) = point.sum {
            point_map.insert("sum".to_string(), f64_to_json(sum));
        }

        // Add min/max if present
        if let Some(min) = point.min {
            point_map.insert("min".to_string(), f64_to_json(min));
        }

        if let Some(max) = point.max {
            point_map.insert("max".to_string(), f64_to_json(max));
        }

        // Add bucket boundaries and counts
        let mut bounds = Vec::new();
        for bound in &point.explicit_bounds {
            // A `+Inf` upper bound is common (Prometheus-style histograms);
            // dropping it would misalign the bucket counts.
            bounds.push(f64_to_json(*bound));
        }
        point_map.insert("explicit_bounds".to_string(), JsonValue::Array(bounds));

        let mut counts = Vec::new();
        for count in &point.bucket_counts {
            counts.push(JsonValue::Number(serde_json::Number::from(*count)));
        }
        point_map.insert("bucket_counts".to_string(), JsonValue::Array(counts));

        // Add attributes
        let mut attr_map = Map::new();
        for attr in &point.attributes {
            attr_map.insert(attr.key.clone(), extract_value(&attr.value));
        }
        point_map.insert("attributes".to_string(), JsonValue::Object(attr_map));

        // Add flags
        point_map.insert(
            "flags".to_string(),
            JsonValue::Number(serde_json::Number::from(point.flags)),
        );

        // Add exemplars if present
        if !point.exemplars.is_empty() {
            let exemplars = extract_exemplars(&point.exemplars);
            point_map.insert("exemplars".to_string(), JsonValue::Array(exemplars));
        }

        result.push(JsonValue::Object(point_map));
    }

    result
}

/// Extract exponential histogram data points from OTLP ExponentialHistogramDataPoint
fn extract_exponential_histogram_data_points(
    data_points: &[opentelemetry_proto::tonic::metrics::v1::ExponentialHistogramDataPoint],
) -> Vec<JsonValue> {
    let mut result = Vec::new();

    for point in data_points {
        let mut point_map = Map::new();

        // Add timestamps
        point_map.insert(
            "start_time_unix_nano".to_string(),
            JsonValue::Number(serde_json::Number::from(point.start_time_unix_nano)),
        );
        point_map.insert(
            "time_unix_nano".to_string(),
            JsonValue::Number(serde_json::Number::from(point.time_unix_nano)),
        );

        // Add count, sum, and scale
        point_map.insert(
            "count".to_string(),
            JsonValue::Number(serde_json::Number::from(point.count)),
        );
        if let Some(sum) = point.sum {
            point_map.insert("sum".to_string(), f64_to_json(sum));
        }
        point_map.insert(
            "scale".to_string(),
            JsonValue::Number(serde_json::Number::from(point.scale)),
        );
        point_map.insert(
            "zero_count".to_string(),
            JsonValue::Number(serde_json::Number::from(point.zero_count)),
        );

        // Add min/max if present
        if let Some(min) = point.min {
            point_map.insert("min".to_string(), f64_to_json(min));
        }

        if let Some(max) = point.max {
            point_map.insert("max".to_string(), f64_to_json(max));
        }

        // Add positive and negative buckets
        if let Some(positive) = &point.positive {
            let mut pos_map = Map::new();
            pos_map.insert(
                "offset".to_string(),
                JsonValue::Number(serde_json::Number::from(positive.offset)),
            );

            let mut counts = Vec::new();
            for count in &positive.bucket_counts {
                counts.push(JsonValue::Number(serde_json::Number::from(*count)));
            }
            pos_map.insert("bucket_counts".to_string(), JsonValue::Array(counts));

            point_map.insert("positive".to_string(), JsonValue::Object(pos_map));
        }

        if let Some(negative) = &point.negative {
            let mut neg_map = Map::new();
            neg_map.insert(
                "offset".to_string(),
                JsonValue::Number(serde_json::Number::from(negative.offset)),
            );

            let mut counts = Vec::new();
            for count in &negative.bucket_counts {
                counts.push(JsonValue::Number(serde_json::Number::from(*count)));
            }
            neg_map.insert("bucket_counts".to_string(), JsonValue::Array(counts));

            point_map.insert("negative".to_string(), JsonValue::Object(neg_map));
        }

        // Add zero threshold
        point_map.insert(
            "zero_threshold".to_string(),
            f64_to_json(point.zero_threshold),
        );

        // Add attributes
        let mut attr_map = Map::new();
        for attr in &point.attributes {
            attr_map.insert(attr.key.clone(), extract_value(&attr.value));
        }
        point_map.insert("attributes".to_string(), JsonValue::Object(attr_map));

        // Add flags
        point_map.insert(
            "flags".to_string(),
            JsonValue::Number(serde_json::Number::from(point.flags)),
        );

        // Add exemplars if present
        if !point.exemplars.is_empty() {
            let exemplars = extract_exemplars(&point.exemplars);
            point_map.insert("exemplars".to_string(), JsonValue::Array(exemplars));
        }

        result.push(JsonValue::Object(point_map));
    }

    result
}

/// Extract summary data points from OTLP SummaryDataPoint
fn extract_summary_data_points(
    data_points: &[opentelemetry_proto::tonic::metrics::v1::SummaryDataPoint],
) -> Vec<JsonValue> {
    let mut result = Vec::new();

    for point in data_points {
        let mut point_map = Map::new();

        // Add timestamps
        point_map.insert(
            "start_time_unix_nano".to_string(),
            JsonValue::Number(serde_json::Number::from(point.start_time_unix_nano)),
        );
        point_map.insert(
            "time_unix_nano".to_string(),
            JsonValue::Number(serde_json::Number::from(point.time_unix_nano)),
        );

        // Add count and sum
        point_map.insert(
            "count".to_string(),
            JsonValue::Number(serde_json::Number::from(point.count)),
        );
        point_map.insert("sum".to_string(), f64_to_json(point.sum));

        // Add quantile values
        let mut quantiles = Vec::new();
        for q in &point.quantile_values {
            let mut q_map = Map::new();
            q_map.insert("quantile".to_string(), f64_to_json(q.quantile));
            q_map.insert("value".to_string(), f64_to_json(q.value));
            quantiles.push(JsonValue::Object(q_map));
        }
        point_map.insert("quantile_values".to_string(), JsonValue::Array(quantiles));

        // Add attributes
        let mut attr_map = Map::new();
        for attr in &point.attributes {
            attr_map.insert(attr.key.clone(), extract_value(&attr.value));
        }
        point_map.insert("attributes".to_string(), JsonValue::Object(attr_map));

        // Add flags
        point_map.insert(
            "flags".to_string(),
            JsonValue::Number(serde_json::Number::from(point.flags)),
        );

        result.push(JsonValue::Object(point_map));
    }

    result
}

/// Extract exemplars from OTLP Exemplar
fn extract_exemplars(
    exemplars: &[opentelemetry_proto::tonic::metrics::v1::Exemplar],
) -> Vec<JsonValue> {
    let mut result = Vec::new();

    for exemplar in exemplars {
        let mut exemplar_map = Map::new();

        // Add timestamp
        exemplar_map.insert(
            "time_unix_nano".to_string(),
            JsonValue::Number(serde_json::Number::from(exemplar.time_unix_nano)),
        );

        // Add value
        match &exemplar.value {
            Some(value) => match value {
                opentelemetry_proto::tonic::metrics::v1::exemplar::Value::AsDouble(v) => {
                    exemplar_map.insert("value".to_string(), f64_to_json(*v));
                }
                opentelemetry_proto::tonic::metrics::v1::exemplar::Value::AsInt(v) => {
                    exemplar_map.insert(
                        "value".to_string(),
                        JsonValue::Number(serde_json::Number::from(*v)),
                    );
                }
            },
            None => {
                exemplar_map.insert("value".to_string(), JsonValue::Null);
            }
        }

        // Add filtered attributes
        let mut attr_map = Map::new();
        for attr in &exemplar.filtered_attributes {
            attr_map.insert(attr.key.clone(), extract_value(&attr.value));
        }
        exemplar_map.insert(
            "filtered_attributes".to_string(),
            JsonValue::Object(attr_map),
        );

        // Add trace and span IDs if present
        if !exemplar.trace_id.is_empty() {
            exemplar_map.insert(
                "trace_id".to_string(),
                JsonValue::String(hex::encode(&exemplar.trace_id)),
            );
        }

        if !exemplar.span_id.is_empty() {
            exemplar_map.insert(
                "span_id".to_string(),
                JsonValue::String(hex::encode(&exemplar.span_id)),
            );
        }

        result.push(JsonValue::Object(exemplar_map));
    }

    result
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use std::sync::Arc;

    #[test]
    fn otlp_metrics_to_arrow_propagates_conversion_errors_via_result() {
        // The conversion is fallible: a RecordBatch assembly failure must
        // surface as Err so the acceptor rejects the export instead of
        // ACKing an empty batch (issue #926). Pin the Result contract and
        // that an empty request converts to an empty batch (the only way a
        // zero-row batch may legitimately be produced).
        let request = ExportMetricsServiceRequest::default();
        let result: Result<RecordBatch, ArrowError> = otlp_metrics_to_arrow(&request);
        let batch = result.expect("empty request must convert to an empty batch");
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_arrow_to_otlp_metrics_gauge() {
        // Create a simple gauge metric in Arrow format
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("unit", DataType::Utf8, true),
            Field::new("start_time_unix_nano", DataType::UInt64, true),
            Field::new("time_unix_nano", DataType::UInt64, false),
            Field::new("attributes_json", DataType::Utf8, true),
            Field::new("resource_json", DataType::Utf8, true),
            Field::new("scope_json", DataType::Utf8, true),
            Field::new("metric_type", DataType::Utf8, false),
            Field::new("data_json", DataType::Utf8, false),
            Field::new("aggregation_temporality", DataType::Int32, true),
            Field::new("is_monotonic", DataType::Boolean, true),
        ]));

        // Sample data for a gauge metric
        let name_array = StringArray::from(vec!["test_gauge"]);
        let description_array = StringArray::from(vec!["Test gauge metric"]);
        let unit_array = StringArray::from(vec!["ms"]);
        let start_time_array = UInt64Array::from(vec![1000000000]);
        let time_array = UInt64Array::from(vec![2000000000]);
        let attributes_json_array = StringArray::from(vec!["{\"attr1\":\"value1\"}"]);
        let resource_json_array = StringArray::from(vec!["{\"service.name\":\"test_service\"}"]);
        let scope_json_array =
            StringArray::from(vec!["{\"name\":\"test_scope\",\"version\":\"1.0\"}"]);
        let metric_type_array = StringArray::from(vec!["gauge"]);

        // Create a gauge data point
        let data_json = r#"[{"start_time_unix_nano":1000000000,"time_unix_nano":2000000000,"value":42.5,"attributes":{"label":"value"},"flags":0}]"#;
        let data_json_array = StringArray::from(vec![data_json]);

        let aggregation_temporality_array = Int32Array::from(vec![0]);
        let is_monotonic_array = BooleanArray::from(vec![false]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(name_array),
                Arc::new(description_array),
                Arc::new(unit_array),
                Arc::new(start_time_array),
                Arc::new(time_array),
                Arc::new(attributes_json_array),
                Arc::new(resource_json_array),
                Arc::new(scope_json_array),
                Arc::new(metric_type_array),
                Arc::new(data_json_array),
                Arc::new(aggregation_temporality_array),
                Arc::new(is_monotonic_array),
            ],
        )
        .unwrap();

        // Convert Arrow to OTLP
        let result = arrow_to_otlp_metrics(&batch);

        // Verify the result
        assert_eq!(result.resource_metrics.len(), 1);
        let resource_metrics = &result.resource_metrics[0];

        // Verify resource
        assert!(resource_metrics.resource.is_some());
        let resource = resource_metrics.resource.as_ref().unwrap();
        assert_eq!(resource.attributes.len(), 1);
        assert_eq!(resource.attributes[0].key, "service.name");

        // Verify scope metrics
        assert_eq!(resource_metrics.scope_metrics.len(), 1);
        let scope_metrics = &resource_metrics.scope_metrics[0];

        // Verify scope
        assert!(scope_metrics.scope.is_some());
        let scope = scope_metrics.scope.as_ref().unwrap();
        assert_eq!(scope.name, "test_scope");
        assert_eq!(scope.version, "1.0");

        // Verify metrics
        assert_eq!(scope_metrics.metrics.len(), 1);
        let metric = &scope_metrics.metrics[0];
        assert_eq!(metric.name, "test_gauge");
        assert_eq!(metric.description, "Test gauge metric");
        assert_eq!(metric.unit, "ms");

        // Verify metric data
        assert!(metric.data.is_some());
        if let Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(gauge)) =
            &metric.data
        {
            assert_eq!(gauge.data_points.len(), 1);
            let data_point = &gauge.data_points[0];
            assert_eq!(data_point.start_time_unix_nano, 1000000000);
            assert_eq!(data_point.time_unix_nano, 2000000000);

            // Verify value
            assert!(data_point.value.is_some());
            if let Some(
                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(value),
            ) = data_point.value
            {
                assert_eq!(value, 42.5);
            } else {
                panic!("Expected double value");
            }

            // Verify attributes
            assert_eq!(data_point.attributes.len(), 1);
            assert_eq!(data_point.attributes[0].key, "label");
        } else {
            panic!("Expected gauge data");
        }
    }

    #[test]
    fn test_arrow_to_otlp_metrics_sum() {
        // Create a simple sum metric in Arrow format
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("unit", DataType::Utf8, true),
            Field::new("start_time_unix_nano", DataType::UInt64, true),
            Field::new("time_unix_nano", DataType::UInt64, false),
            Field::new("attributes_json", DataType::Utf8, true),
            Field::new("resource_json", DataType::Utf8, true),
            Field::new("scope_json", DataType::Utf8, true),
            Field::new("metric_type", DataType::Utf8, false),
            Field::new("data_json", DataType::Utf8, false),
            Field::new("aggregation_temporality", DataType::Int32, true),
            Field::new("is_monotonic", DataType::Boolean, true),
        ]));

        // Sample data for a sum metric
        let name_array = StringArray::from(vec!["test_sum"]);
        let description_array = StringArray::from(vec!["Test sum metric"]);
        let unit_array = StringArray::from(vec!["count"]);
        let start_time_array = UInt64Array::from(vec![1000000000]);
        let time_array = UInt64Array::from(vec![2000000000]);
        let attributes_json_array = StringArray::from(vec!["{\"attr1\":\"value1\"}"]);
        let resource_json_array = StringArray::from(vec!["{\"service.name\":\"test_service\"}"]);
        let scope_json_array =
            StringArray::from(vec!["{\"name\":\"test_scope\",\"version\":\"1.0\"}"]);
        let metric_type_array = StringArray::from(vec!["sum"]);

        // Create a sum data point
        let data_json = r#"[{"start_time_unix_nano":1000000000,"time_unix_nano":2000000000,"value":100,"attributes":{"counter":"total"},"flags":0}]"#;
        let data_json_array = StringArray::from(vec![data_json]);

        let aggregation_temporality_array = Int32Array::from(vec![2]); // Cumulative
        let is_monotonic_array = BooleanArray::from(vec![true]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(name_array),
                Arc::new(description_array),
                Arc::new(unit_array),
                Arc::new(start_time_array),
                Arc::new(time_array),
                Arc::new(attributes_json_array),
                Arc::new(resource_json_array),
                Arc::new(scope_json_array),
                Arc::new(metric_type_array),
                Arc::new(data_json_array),
                Arc::new(aggregation_temporality_array),
                Arc::new(is_monotonic_array),
            ],
        )
        .unwrap();

        // Convert Arrow to OTLP
        let result = arrow_to_otlp_metrics(&batch);

        // Verify the result
        assert_eq!(result.resource_metrics.len(), 1);
        let resource_metrics = &result.resource_metrics[0];

        // Verify scope metrics
        assert_eq!(resource_metrics.scope_metrics.len(), 1);
        let scope_metrics = &resource_metrics.scope_metrics[0];

        // Verify metrics
        assert_eq!(scope_metrics.metrics.len(), 1);
        let metric = &scope_metrics.metrics[0];
        assert_eq!(metric.name, "test_sum");

        // Verify metric data
        assert!(metric.data.is_some());
        if let Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(sum)) = &metric.data
        {
            assert_eq!(sum.data_points.len(), 1);
            let data_point = &sum.data_points[0];

            // Verify value
            assert!(data_point.value.is_some());
            if let Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(
                value,
            )) = data_point.value
            {
                assert_eq!(value, 100);
            } else {
                panic!("Expected integer value");
            }

            // Verify aggregation temporality and monotonicity
            assert_eq!(sum.aggregation_temporality, 2); // Cumulative
            assert!(sum.is_monotonic);
        } else {
            panic!("Expected sum data");
        }
    }

    #[test]
    fn test_arrow_to_otlp_metrics_histogram() {
        // Create a simple histogram metric in Arrow format
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("unit", DataType::Utf8, true),
            Field::new("start_time_unix_nano", DataType::UInt64, true),
            Field::new("time_unix_nano", DataType::UInt64, false),
            Field::new("attributes_json", DataType::Utf8, true),
            Field::new("resource_json", DataType::Utf8, true),
            Field::new("scope_json", DataType::Utf8, true),
            Field::new("metric_type", DataType::Utf8, false),
            Field::new("data_json", DataType::Utf8, false),
            Field::new("aggregation_temporality", DataType::Int32, true),
            Field::new("is_monotonic", DataType::Boolean, true),
        ]));

        // Sample data for a histogram metric
        let name_array = StringArray::from(vec!["test_histogram"]);
        let description_array = StringArray::from(vec!["Test histogram metric"]);
        let unit_array = StringArray::from(vec!["ms"]);
        let start_time_array = UInt64Array::from(vec![1000000000]);
        let time_array = UInt64Array::from(vec![2000000000]);
        let attributes_json_array = StringArray::from(vec!["{\"attr1\":\"value1\"}"]);
        let resource_json_array = StringArray::from(vec!["{\"service.name\":\"test_service\"}"]);
        let scope_json_array =
            StringArray::from(vec!["{\"name\":\"test_scope\",\"version\":\"1.0\"}"]);
        let metric_type_array = StringArray::from(vec!["histogram"]);

        // Create a histogram data point
        let data_json = r#"[{"start_time_unix_nano":1000000000,"time_unix_nano":2000000000,"count":10,"sum":100.5,"min":5.0,"max":50.0,"explicit_bounds":[10.0,20.0,30.0,40.0],"bucket_counts":[2,3,3,1,1],"attributes":{},"flags":0}]"#;
        let data_json_array = StringArray::from(vec![data_json]);

        let aggregation_temporality_array = Int32Array::from(vec![2]); // Cumulative
        let is_monotonic_array = BooleanArray::from(vec![false]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(name_array),
                Arc::new(description_array),
                Arc::new(unit_array),
                Arc::new(start_time_array),
                Arc::new(time_array),
                Arc::new(attributes_json_array),
                Arc::new(resource_json_array),
                Arc::new(scope_json_array),
                Arc::new(metric_type_array),
                Arc::new(data_json_array),
                Arc::new(aggregation_temporality_array),
                Arc::new(is_monotonic_array),
            ],
        )
        .unwrap();

        // Convert Arrow to OTLP
        let result = arrow_to_otlp_metrics(&batch);

        // Verify the result
        assert_eq!(result.resource_metrics.len(), 1);
        let resource_metrics = &result.resource_metrics[0];

        // Verify scope metrics
        assert_eq!(resource_metrics.scope_metrics.len(), 1);
        let scope_metrics = &resource_metrics.scope_metrics[0];

        // Verify metrics
        assert_eq!(scope_metrics.metrics.len(), 1);
        let metric = &scope_metrics.metrics[0];
        assert_eq!(metric.name, "test_histogram");

        // Verify metric data
        assert!(metric.data.is_some());
        if let Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Histogram(histogram)) =
            &metric.data
        {
            assert_eq!(histogram.data_points.len(), 1);
            let data_point = &histogram.data_points[0];

            // Verify histogram properties
            assert_eq!(data_point.count, 10);
            assert_eq!(data_point.sum.unwrap(), 100.5);
            assert_eq!(data_point.min.unwrap(), 5.0);
            assert_eq!(data_point.max.unwrap(), 50.0);

            // Verify bucket bounds and counts
            assert_eq!(data_point.explicit_bounds, vec![10.0, 20.0, 30.0, 40.0]);
            assert_eq!(data_point.bucket_counts, vec![2, 3, 3, 1, 1]);

            // Verify aggregation temporality
            assert_eq!(histogram.aggregation_temporality, 2); // Cumulative
        } else {
            panic!("Expected histogram data");
        }
    }

    /// NaN and ±Inf are ordinary metric values (Prometheus staleness marker,
    /// `0/0` rates, unbounded divisions) but JSON cannot carry them. They must
    /// survive the v1 wire format as explicit sentinels and round-trip back
    /// to the same non-finite double — never collapse to `null`, which the
    /// writer's non-nullable `value` column then rejects, poisoning the WAL
    /// entry forever (#1061).
    #[test]
    fn non_finite_number_values_round_trip_through_arrow() {
        fn point(v: f64) -> NumberDataPoint {
            NumberDataPoint {
                start_time_unix_nano: 1,
                time_unix_nano: 2,
                value: Some(number_data_point::Value::AsDouble(v)),
                ..Default::default()
            }
        }
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource::default()),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![
                        Metric {
                            name: "g".into(),
                            data: Some(Data::Gauge(Gauge {
                                data_points: vec![
                                    point(f64::NAN),
                                    point(f64::INFINITY),
                                    point(f64::NEG_INFINITY),
                                    point(1.5),
                                ],
                            })),
                            ..Default::default()
                        },
                        Metric {
                            name: "s".into(),
                            data: Some(Data::Sum(Sum {
                                data_points: vec![point(f64::NAN)],
                                aggregation_temporality: 2,
                                is_monotonic: true,
                            })),
                            ..Default::default()
                        },
                    ],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let batch = otlp_metrics_to_arrow(&request).expect("convert");
        let data_json = batch
            .column_by_name("data_json")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // The wire format spells them out; nothing is null.
        assert!(
            !data_json.value(0).contains("null"),
            "{}",
            data_json.value(0)
        );
        assert!(data_json.value(0).contains("\"NaN\""));
        assert!(data_json.value(0).contains("\"+Inf\""));
        assert!(data_json.value(0).contains("\"-Inf\""));

        let back = arrow_to_otlp_metrics(&batch);
        let mut values = Vec::new();
        for rm in &back.resource_metrics {
            for sm in &rm.scope_metrics {
                for m in &sm.metrics {
                    let points = match &m.data {
                        Some(Data::Gauge(g)) => &g.data_points,
                        Some(Data::Sum(s)) => &s.data_points,
                        _ => continue,
                    };
                    for p in points {
                        match p.value {
                            Some(number_data_point::Value::AsDouble(v)) => values.push(v),
                            other => panic!("unexpected value {other:?}"),
                        }
                    }
                }
            }
        }
        assert_eq!(values.len(), 5);
        assert_eq!(values.iter().filter(|v| v.is_nan()).count(), 2);
        assert!(values.contains(&f64::INFINITY));
        assert!(values.contains(&f64::NEG_INFINITY));
        assert!(values.contains(&1.5));
    }
}
