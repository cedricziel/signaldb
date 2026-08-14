//! Shared metric-type partitioning logic for OTLP and Prometheus remote-write ingestion.
//!
//! Both `MetricsHandler` (OTLP gRPC/HTTP) and `PrometheusHandler` (remote_write, converted
//! to OTEL metrics) need to split a mixed-type `ExportMetricsServiceRequest` into one request
//! per metric type so each type lands in its own table with its own schema.

use std::collections::HashMap;

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;

/// Partition metrics by type to avoid schema conflicts.
/// Returns: HashMap<metric_type, (table_name, partitioned_request)>
pub(crate) fn partition_metrics_by_type(
    request: &ExportMetricsServiceRequest,
) -> HashMap<String, (String, ExportMetricsServiceRequest)> {
    // Track metric types: gauge, sum, histogram, exponential_histogram, summary
    // Each type maps to its corresponding table for proper schema handling
    let mut partitions: HashMap<String, Vec<(usize, usize, usize)>> = HashMap::new(); // type -> Vec<(res_idx, scope_idx, metric_idx)>

    // First pass: detect all types and collect indices
    for (res_idx, resource_metrics) in request.resource_metrics.iter().enumerate() {
        for (scope_idx, scope_metrics) in resource_metrics.scope_metrics.iter().enumerate() {
            for (metric_idx, metric) in scope_metrics.metrics.iter().enumerate() {
                if let Some(data) = &metric.data {
                    let metric_type = match data {
                        Data::Gauge(_) => "gauge",
                        Data::Sum(_) => "sum",
                        Data::Histogram(_) => "histogram",
                        Data::ExponentialHistogram(_) => {
                            tracing::info!(
                                metric_name = %metric.name,
                                "Processing ExponentialHistogram metric with full exponential metadata (scale, zero_count, positive/negative buckets)"
                            );
                            "exponential_histogram"
                        }
                        Data::Summary(_) => {
                            tracing::info!(
                                metric_name = %metric.name,
                                "Processing Summary metric with quantile values"
                            );
                            "summary"
                        }
                    };

                    partitions
                        .entry(metric_type.to_string())
                        .or_default()
                        .push((res_idx, scope_idx, metric_idx));
                }
            }
        }
    }

    // Second pass: build separate requests for each type
    let mut result = HashMap::new();

    for (metric_type, indices) in partitions {
        let table_name = match metric_type.as_str() {
            "gauge" => "metrics_gauge",
            "sum" => "metrics_sum",
            "histogram" => "metrics_histogram",
            "exponential_histogram" => "metrics_exponential_histogram",
            "summary" => "metrics_summary",
            _ => {
                tracing::warn!(
                    metric_type = %metric_type,
                    "Unknown metric type, falling back to metrics_gauge table"
                );
                "metrics_gauge"
            }
        };

        // Build new request with only metrics of this type
        let mut partitioned_resource_metrics = vec![];
        let mut current_resource_idx = None;
        let mut current_scope_idx = None;
        let mut current_scope_metrics = vec![];
        let mut current_resource_scope_metrics = vec![];

        for (res_idx, scope_idx, metric_idx) in indices {
            let resource_metrics = &request.resource_metrics[res_idx];
            let scope_metrics = &resource_metrics.scope_metrics[scope_idx];
            let metric = scope_metrics.metrics[metric_idx].clone();

            // Check if we need to start a new resource or scope
            if current_resource_idx != Some(res_idx) {
                // Finalize previous scope and resource if any
                if !current_scope_metrics.is_empty() {
                    let src_scope_metrics: &opentelemetry_proto::tonic::metrics::v1::ScopeMetrics =
                        &request.resource_metrics[current_resource_idx.unwrap()].scope_metrics
                            [current_scope_idx.unwrap()];
                    current_resource_scope_metrics.push(
                        opentelemetry_proto::tonic::metrics::v1::ScopeMetrics {
                            scope: src_scope_metrics.scope.clone(),
                            metrics: current_scope_metrics,
                            schema_url: src_scope_metrics.schema_url.clone(),
                        },
                    );
                    current_scope_metrics = vec![];
                }

                if let Some(res_idx) = current_resource_idx {
                    let src_resource_metrics = &request.resource_metrics[res_idx];
                    partitioned_resource_metrics.push(
                        opentelemetry_proto::tonic::metrics::v1::ResourceMetrics {
                            resource: src_resource_metrics.resource.clone(),
                            scope_metrics: current_resource_scope_metrics,
                            schema_url: src_resource_metrics.schema_url.clone(),
                        },
                    );
                    current_resource_scope_metrics = vec![];
                }

                current_resource_idx = Some(res_idx);
                current_scope_idx = Some(scope_idx);
                current_scope_metrics.push(metric);
            } else if current_scope_idx != Some(scope_idx) {
                // Finalize previous scope
                if !current_scope_metrics.is_empty() {
                    let src_scope_metrics: &opentelemetry_proto::tonic::metrics::v1::ScopeMetrics =
                        &request.resource_metrics[current_resource_idx.unwrap()].scope_metrics
                            [current_scope_idx.unwrap()];
                    current_resource_scope_metrics.push(
                        opentelemetry_proto::tonic::metrics::v1::ScopeMetrics {
                            scope: src_scope_metrics.scope.clone(),
                            metrics: current_scope_metrics,
                            schema_url: src_scope_metrics.schema_url.clone(),
                        },
                    );
                    current_scope_metrics = vec![];
                }

                current_scope_idx = Some(scope_idx);
                current_scope_metrics.push(metric);
            } else {
                // Same resource and scope, just add metric
                current_scope_metrics.push(metric);
            }
        }

        // Finalize last scope and resource
        if !current_scope_metrics.is_empty() {
            let src_scope_metrics: &opentelemetry_proto::tonic::metrics::v1::ScopeMetrics =
                &request.resource_metrics[current_resource_idx.unwrap()].scope_metrics
                    [current_scope_idx.unwrap()];
            current_resource_scope_metrics.push(
                opentelemetry_proto::tonic::metrics::v1::ScopeMetrics {
                    scope: src_scope_metrics.scope.clone(),
                    metrics: current_scope_metrics,
                    schema_url: src_scope_metrics.schema_url.clone(),
                },
            );
        }

        if !current_resource_scope_metrics.is_empty() {
            partitioned_resource_metrics.push(
                opentelemetry_proto::tonic::metrics::v1::ResourceMetrics {
                    resource: request.resource_metrics[current_resource_idx.unwrap()]
                        .resource
                        .clone(),
                    scope_metrics: current_resource_scope_metrics,
                    schema_url: request.resource_metrics[current_resource_idx.unwrap()]
                        .schema_url
                        .clone(),
                },
            );
        }

        let partitioned_request = ExportMetricsServiceRequest {
            resource_metrics: partitioned_resource_metrics,
        };

        result.insert(metric_type, (table_name.to_string(), partitioned_request));
    }

    result
}
