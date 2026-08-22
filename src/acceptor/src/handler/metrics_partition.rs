//! Shared metric-type partitioning logic for OTLP and Prometheus remote-write ingestion.
//!
//! Both `MetricsHandler` (OTLP gRPC/HTTP) and `PrometheusHandler` (remote_write, converted
//! to OTEL metrics) need to split a mixed-type `ExportMetricsServiceRequest` into one request
//! per metric type so each type lands in its own table with its own schema.

use std::collections::HashMap;

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::{
    Metric, ResourceMetrics, ScopeMetrics, metric::Data,
};

/// Partition metrics by type to avoid schema conflicts.
/// Returns: HashMap<metric_type, (table_name, partitioned_request)>
pub(crate) fn partition_metrics_by_type(
    request: &ExportMetricsServiceRequest,
) -> HashMap<String, (String, ExportMetricsServiceRequest)> {
    // Single pass: group metrics by type, then by the (resource, scope)
    // they came from. Grouping by index (rather than cloning
    // ResourceMetrics/ScopeMetrics templates up front) keeps this a plain
    // append, with no state to reconcile across iterations.
    let mut by_type: HashMap<&'static str, HashMap<usize, HashMap<usize, Vec<Metric>>>> =
        HashMap::new();

    for (res_idx, resource_metrics) in request.resource_metrics.iter().enumerate() {
        for (scope_idx, scope_metrics) in resource_metrics.scope_metrics.iter().enumerate() {
            for metric in &scope_metrics.metrics {
                let Some(data) = &metric.data else {
                    continue;
                };
                let metric_type = match data {
                    Data::Gauge(_) => "gauge",
                    Data::Sum(_) => "sum",
                    Data::Histogram(_) => "histogram",
                    Data::ExponentialHistogram(_) => {
                        tracing::debug!(
                            metric_name = %metric.name,
                            "Processing ExponentialHistogram metric with full exponential metadata (scale, zero_count, positive/negative buckets)"
                        );
                        "exponential_histogram"
                    }
                    Data::Summary(_) => {
                        tracing::debug!(
                            metric_name = %metric.name,
                            "Processing Summary metric with quantile values"
                        );
                        "summary"
                    }
                };

                by_type
                    .entry(metric_type)
                    .or_default()
                    .entry(res_idx)
                    .or_default()
                    .entry(scope_idx)
                    .or_default()
                    .push(metric.clone());
            }
        }
    }

    // Second pass: assemble one ExportMetricsServiceRequest per type. Walk
    // `request.resource_metrics` in its original order (not `by_type`'s
    // HashMap order) so the output structure is deterministic and every
    // lookup below is a plain `.get()` — res_idx/scope_idx always came
    // from `enumerate()` over this exact `request`, so there is no index
    // to be missing or out of bounds, and therefore nothing to `.unwrap()`.
    let mut result = HashMap::new();

    for (metric_type, by_resource) in by_type {
        let table_name = match metric_type {
            "gauge" => "metrics_gauge",
            "sum" => "metrics_sum",
            "histogram" => "metrics_histogram",
            "exponential_histogram" => "metrics_exponential_histogram",
            "summary" => "metrics_summary",
            other => {
                // Defensive fallback, not expected to be reachable: every
                // value pushed into `by_type` above came from one of the
                // five match arms in the first pass.
                tracing::warn!(
                    metric_type = %other,
                    "Unknown metric type, falling back to metrics_gauge table"
                );
                "metrics_gauge"
            }
        };

        let mut partitioned_resource_metrics = Vec::new();
        for (res_idx, resource_metrics) in request.resource_metrics.iter().enumerate() {
            let Some(by_scope) = by_resource.get(&res_idx) else {
                continue;
            };

            let mut partitioned_scope_metrics = Vec::new();
            for (scope_idx, scope_metrics) in resource_metrics.scope_metrics.iter().enumerate() {
                let Some(metrics) = by_scope.get(&scope_idx) else {
                    continue;
                };
                partitioned_scope_metrics.push(ScopeMetrics {
                    scope: scope_metrics.scope.clone(),
                    metrics: metrics.clone(),
                    schema_url: scope_metrics.schema_url.clone(),
                });
            }

            partitioned_resource_metrics.push(ResourceMetrics {
                resource: resource_metrics.resource.clone(),
                scope_metrics: partitioned_scope_metrics,
                schema_url: resource_metrics.schema_url.clone(),
            });
        }

        result.insert(
            metric_type.to_string(),
            (
                table_name.to_string(),
                ExportMetricsServiceRequest {
                    resource_metrics: partitioned_resource_metrics,
                },
            ),
        );
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::metrics::v1::{Gauge, NumberDataPoint, Sum, number_data_point};
    use opentelemetry_proto::tonic::resource::v1::Resource;

    fn gauge(name: &str, value: f64) -> Metric {
        Metric {
            name: name.to_string(),
            description: String::new(),
            unit: "1".to_string(),
            data: Some(Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 1000,
                    time_unix_nano: 2000,
                    value: Some(number_data_point::Value::AsDouble(value)),
                    exemplars: vec![],
                    flags: 0,
                }],
            })),
            metadata: vec![],
        }
    }

    fn sum(name: &str, value: i64) -> Metric {
        Metric {
            name: name.to_string(),
            description: String::new(),
            unit: "1".to_string(),
            data: Some(Data::Sum(Sum {
                data_points: vec![NumberDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 1000,
                    time_unix_nano: 2000,
                    value: Some(number_data_point::Value::AsInt(value)),
                    exemplars: vec![],
                    flags: 0,
                }],
                aggregation_temporality: 0,
                is_monotonic: false,
            })),
            metadata: vec![],
        }
    }

    fn resource(service_name: &str) -> Resource {
        Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(service_name.to_string())),
                }),
                ..Default::default()
            }],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        }
    }

    /// Two resources, each with two scopes, each with a mix of gauge/sum
    /// metrics. Every (resource, scope) pair must land in the right place
    /// in each type's partitioned output, in the original order — the
    /// regrouping case the old index-walk state machine never had a
    /// dedicated test for.
    #[test]
    fn regroups_multiple_resources_and_scopes_by_type() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![
                ResourceMetrics {
                    resource: Some(resource("svc-a")),
                    scope_metrics: vec![
                        ScopeMetrics {
                            scope: None,
                            metrics: vec![gauge("a.scope0.gauge", 1.0), sum("a.scope0.sum", 1)],
                            schema_url: "a-scope0".to_string(),
                        },
                        ScopeMetrics {
                            scope: None,
                            metrics: vec![gauge("a.scope1.gauge", 2.0)],
                            schema_url: "a-scope1".to_string(),
                        },
                    ],
                    schema_url: "resource-a".to_string(),
                },
                ResourceMetrics {
                    resource: Some(resource("svc-b")),
                    scope_metrics: vec![
                        ScopeMetrics {
                            scope: None,
                            metrics: vec![sum("b.scope0.sum", 2)],
                            schema_url: "b-scope0".to_string(),
                        },
                        ScopeMetrics {
                            scope: None,
                            metrics: vec![gauge("b.scope1.gauge", 3.0), sum("b.scope1.sum", 3)],
                            schema_url: "b-scope1".to_string(),
                        },
                    ],
                    schema_url: "resource-b".to_string(),
                },
            ],
        };

        let partitions = partition_metrics_by_type(&request);
        assert_eq!(partitions.len(), 2, "expected gauge and sum partitions");

        // Gauge partition: resource-a/scope0, resource-a/scope1, resource-b/scope1
        // (resource-b/scope0 has no gauge metrics, so it must be absent).
        let (_, gauge_req) = &partitions["gauge"];
        assert_eq!(
            gauge_req.resource_metrics.len(),
            2,
            "gauge partition must have both resources (b/scope0-only is absent)"
        );
        assert_eq!(gauge_req.resource_metrics[0].schema_url, "resource-a");
        assert_eq!(gauge_req.resource_metrics[0].scope_metrics.len(), 2);
        assert_eq!(
            gauge_req.resource_metrics[0].scope_metrics[0].metrics[0].name,
            "a.scope0.gauge"
        );
        assert_eq!(
            gauge_req.resource_metrics[0].scope_metrics[1].metrics[0].name,
            "a.scope1.gauge"
        );
        assert_eq!(gauge_req.resource_metrics[1].schema_url, "resource-b");
        assert_eq!(
            gauge_req.resource_metrics[1].scope_metrics.len(),
            1,
            "resource-b's gauge-less scope0 must not produce an empty ScopeMetrics"
        );
        assert_eq!(
            gauge_req.resource_metrics[1].scope_metrics[0].metrics[0].name,
            "b.scope1.gauge"
        );

        // Sum partition: resource-a/scope0, resource-b/scope0, resource-b/scope1
        // (resource-a/scope1 has no sum metrics).
        let (_, sum_req) = &partitions["sum"];
        assert_eq!(sum_req.resource_metrics.len(), 2);
        assert_eq!(
            sum_req.resource_metrics[0].scope_metrics.len(),
            1,
            "resource-a's sum-less scope1 must not produce an empty ScopeMetrics"
        );
        assert_eq!(
            sum_req.resource_metrics[0].scope_metrics[0].metrics[0].name,
            "a.scope0.sum"
        );
        assert_eq!(sum_req.resource_metrics[1].scope_metrics.len(), 2);
        assert_eq!(
            sum_req.resource_metrics[1].scope_metrics[0].metrics[0].name,
            "b.scope0.sum"
        );
        assert_eq!(
            sum_req.resource_metrics[1].scope_metrics[1].metrics[0].name,
            "b.scope1.sum"
        );
    }

    /// A resource whose scopes contain only one metric type each must not
    /// leak an empty ScopeMetrics into a different type's partition.
    #[test]
    fn scope_with_single_type_does_not_leak_empty_group_into_other_types() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(resource("svc")),
                scope_metrics: vec![
                    ScopeMetrics {
                        scope: None,
                        metrics: vec![gauge("only.gauge", 1.0)],
                        schema_url: "gauge-only".to_string(),
                    },
                    ScopeMetrics {
                        scope: None,
                        metrics: vec![sum("only.sum", 1)],
                        schema_url: "sum-only".to_string(),
                    },
                ],
                schema_url: "resource".to_string(),
            }],
        };

        let partitions = partition_metrics_by_type(&request);

        let (_, gauge_req) = &partitions["gauge"];
        assert_eq!(gauge_req.resource_metrics.len(), 1);
        assert_eq!(
            gauge_req.resource_metrics[0].scope_metrics.len(),
            1,
            "gauge partition must not include the sum-only scope"
        );

        let (_, sum_req) = &partitions["sum"];
        assert_eq!(sum_req.resource_metrics.len(), 1);
        assert_eq!(
            sum_req.resource_metrics[0].scope_metrics.len(),
            1,
            "sum partition must not include the gauge-only scope"
        );
    }
}
