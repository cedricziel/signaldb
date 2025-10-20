use std::sync::Arc;

use common::auth::TenantContext;
use common::flight::conversion::otlp_metrics_to_arrow;
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use common::wal::{WalOperation, record_batch_to_bytes};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;

use super::WalManager;
// Flight protocol imports
use arrow_flight::utils::batches_to_flight_data;
use bytes::Bytes;
use futures::{StreamExt, stream};

pub struct MetricsHandler {
    /// Flight transport for forwarding telemetry
    flight_transport: Arc<InMemoryFlightTransport>,
    /// WAL manager for multi-tenant WAL isolation
    wal_manager: Arc<WalManager>,
}

#[cfg(any(test, feature = "testing"))]
pub struct MockMetricsHandler {
    pub handle_grpc_otlp_metrics_calls: tokio::sync::Mutex<Vec<ExportMetricsServiceRequest>>,
}

#[cfg(any(test, feature = "testing"))]
impl Default for MockMetricsHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(test, feature = "testing"))]
impl MockMetricsHandler {
    pub fn new() -> Self {
        Self {
            handle_grpc_otlp_metrics_calls: tokio::sync::Mutex::new(Vec::new()),
        }
    }

    pub async fn handle_grpc_otlp_metrics(
        &self,
        _tenant_context: &TenantContext,
        request: ExportMetricsServiceRequest,
    ) {
        self.handle_grpc_otlp_metrics_calls
            .lock()
            .await
            .push(request);
    }

    pub fn expect_handle_grpc_otlp_metrics(&mut self) -> &mut Self {
        self
    }
}

impl MetricsHandler {
    /// Create a new handler with Flight transport and WAL manager
    pub fn new(
        flight_transport: Arc<InMemoryFlightTransport>,
        wal_manager: Arc<WalManager>,
    ) -> Self {
        Self {
            flight_transport,
            wal_manager,
        }
    }

    /// Partition metrics by type to avoid schema conflicts
    /// Returns: HashMap<metric_type, (table_name, partitioned_request)>
    fn partition_metrics_by_type(
        request: &ExportMetricsServiceRequest,
    ) -> std::collections::HashMap<String, (String, ExportMetricsServiceRequest)> {
        use std::collections::HashMap;

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
                                log::info!(
                                    "Processing ExponentialHistogram metric '{}' with full exponential metadata (scale, zero_count, positive/negative buckets)",
                                    metric.name
                                );
                                "exponential_histogram"
                            }
                            Data::Summary(_) => {
                                log::info!(
                                    "Processing Summary metric '{}' with quantile values",
                                    metric.name
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
                    log::warn!(
                        "Unknown metric type '{metric_type}', falling back to metrics_gauge table"
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
                            &request.resource_metrics[current_resource_idx.unwrap()]
                            .scope_metrics[current_scope_idx.unwrap()];
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
                            &request.resource_metrics[current_resource_idx.unwrap()]
                            .scope_metrics[current_scope_idx.unwrap()];
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

    pub async fn handle_grpc_otlp_metrics(
        &self,
        tenant_context: &TenantContext,
        request: ExportMetricsServiceRequest,
    ) {
        log::info!(
            "Handling OTLP metrics request for tenant='{}', dataset='{}'",
            tenant_context.tenant_id,
            tenant_context.dataset_id
        );

        // Get tenant/dataset-specific WAL
        let wal = match self
            .wal_manager
            .get_wal(
                &tenant_context.tenant_id,
                &tenant_context.dataset_id,
                "metrics",
            )
            .await
        {
            Ok(wal) => wal,
            Err(e) => {
                log::error!(
                    "Failed to get WAL for tenant='{}', dataset='{}': {e}",
                    tenant_context.tenant_id,
                    tenant_context.dataset_id
                );
                return;
            }
        };

        // Partition metrics by type to prevent schema conflicts
        let partitions = Self::partition_metrics_by_type(&request);

        if partitions.is_empty() {
            log::warn!("No metrics found in request");
            return;
        }

        log::info!(
            "Partitioned metrics into {} type(s): {}",
            partitions.len(),
            partitions.keys().cloned().collect::<Vec<_>>().join(", ")
        );

        // Process each partition separately
        for (metric_type, (target_table, partitioned_request)) in partitions {
            log::debug!(
                "Processing {} metric type -> table: {}",
                metric_type,
                target_table
            );

            // Convert OTLP metrics to Arrow RecordBatch
            let record_batch = otlp_metrics_to_arrow(&partitioned_request);

            // Step 1: Write to WAL first for durability
            let batch_bytes = match record_batch_to_bytes(&record_batch) {
                Ok(bytes) => bytes,
                Err(e) => {
                    log::error!("Failed to serialize record batch for {metric_type}: {e}");
                    continue; // Skip this partition but continue with others
                }
            };

            let wal_entry_id = match wal
                .append(WalOperation::WriteMetrics, batch_bytes.clone())
                .await
            {
                Ok(id) => id,
                Err(e) => {
                    log::error!("Failed to write {metric_type} metrics to WAL: {e}");
                    continue;
                }
            };

            // Flush WAL to ensure durability
            if let Err(e) = wal.flush().await {
                log::error!("Failed to flush WAL for {metric_type}: {e}");
                continue;
            }

            log::debug!(
                "{} metrics written to WAL with entry ID: {wal_entry_id}",
                metric_type
            );

            // Build metadata after WAL append to include wal_entry_id, tenant_id, and dataset_id
            // This enables writer routing and idempotency checks
            let metadata = serde_json::json!({
                "schema_version": "v1",
                "signal_type": "metrics",
                "metric_type": metric_type,
                "target_table": target_table,
                "tenant_id": tenant_context.tenant_id,
                "dataset_id": tenant_context.dataset_id,
                "wal_entry_id": wal_entry_id
            });

            // Step 2: Forward from WAL to writer via Flight
            // Get a Flight client for a writer service with storage capability
            let mut client = match self
                .flight_transport
                .get_client_for_capability(ServiceCapability::Storage)
                .await
            {
                Ok(client) => client,
                Err(e) => {
                    log::error!("Failed to get Flight client for {metric_type} metrics: {e}");
                    // Data remains in WAL for retry by background processor
                    continue;
                }
            };

            let schema = record_batch.schema();
            let mut flight_data = match batches_to_flight_data(&schema, vec![record_batch]) {
                Ok(data) => data,
                Err(e) => {
                    log::error!("Failed to convert {metric_type} batch to flight data: {e}");
                    // Data remains in WAL for retry
                    continue;
                }
            };

            // Add metadata to the first FlightData message (which contains the schema)
            if !flight_data.is_empty() {
                let metadata_bytes = metadata.to_string().into_bytes();
                flight_data[0].app_metadata = Bytes::from(metadata_bytes);
            }

            let flight_stream = stream::iter(flight_data);

            match client.do_put(flight_stream).await {
                Ok(response) => {
                    let mut response_stream = response.into_inner();
                    let mut success = true;
                    while let Some(result) = response_stream.next().await {
                        match result {
                            Ok(put_result) => {
                                log::debug!(
                                    "Flight put response for {metric_type}: {put_result:?}"
                                );
                            }
                            Err(e) => {
                                log::error!("Flight put error for {metric_type}: {e}");
                                success = false;
                                break;
                            }
                        }
                    }

                    if success {
                        log::debug!(
                            "Successfully forwarded {metric_type} metrics to {} via Flight",
                            target_table
                        );
                        // Mark WAL entry as processed after successful forwarding
                        if let Err(e) = wal.mark_processed(wal_entry_id).await {
                            log::warn!("Failed to mark WAL entry {wal_entry_id} as processed: {e}");
                        }
                    } else {
                        log::error!(
                            "Failed to forward {metric_type} metrics - data remains in WAL for retry"
                        );
                    }
                }
                Err(e) => {
                    log::error!("Failed to forward {metric_type} metrics via Flight: {e}");
                    // Data remains in WAL for retry by background processor
                }
            }
        }

        log::info!("Completed processing metrics request for all types");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::metrics::v1::{
        AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint,
        ResourceMetrics, ScopeMetrics, Sum, metric::Data, number_data_point,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;

    #[test]
    fn test_partition_metrics_by_type_mixed_types() {
        // Create a request with mixed metric types: gauge, sum, and histogram
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![
                        // Gauge metric
                        Metric {
                            name: "gauge_metric".to_string(),
                            description: "A gauge metric".to_string(),
                            unit: "1".to_string(),
                            data: Some(Data::Gauge(Gauge {
                                data_points: vec![NumberDataPoint {
                                    attributes: vec![],
                                    start_time_unix_nano: 1000,
                                    time_unix_nano: 2000,
                                    value: Some(number_data_point::Value::AsDouble(42.0)),
                                    exemplars: vec![],
                                    flags: 0,
                                }],
                            })),
                            metadata: vec![],
                        },
                        // Sum metric
                        Metric {
                            name: "sum_metric".to_string(),
                            description: "A sum metric".to_string(),
                            unit: "1".to_string(),
                            data: Some(Data::Sum(Sum {
                                data_points: vec![NumberDataPoint {
                                    attributes: vec![],
                                    start_time_unix_nano: 1000,
                                    time_unix_nano: 2000,
                                    value: Some(number_data_point::Value::AsInt(100)),
                                    exemplars: vec![],
                                    flags: 0,
                                }],
                                aggregation_temporality: AggregationTemporality::Cumulative.into(),
                                is_monotonic: true,
                            })),
                            metadata: vec![],
                        },
                        // Histogram metric
                        Metric {
                            name: "histogram_metric".to_string(),
                            description: "A histogram metric".to_string(),
                            unit: "ms".to_string(),
                            data: Some(Data::Histogram(Histogram {
                                data_points: vec![HistogramDataPoint {
                                    attributes: vec![],
                                    start_time_unix_nano: 1000,
                                    time_unix_nano: 2000,
                                    count: 5,
                                    sum: Some(250.0),
                                    bucket_counts: vec![1, 2, 2],
                                    explicit_bounds: vec![10.0, 50.0],
                                    exemplars: vec![],
                                    flags: 0,
                                    min: Some(5.0),
                                    max: Some(100.0),
                                }],
                                aggregation_temporality: AggregationTemporality::Cumulative.into(),
                            })),
                            metadata: vec![],
                        },
                    ],
                    schema_url: "".to_string(),
                }],
                schema_url: "".to_string(),
            }],
        };

        // Call the partition function
        let partitions = MetricsHandler::partition_metrics_by_type(&request);

        // Verify we got 3 partitions: gauge, sum, histogram
        assert_eq!(
            partitions.len(),
            3,
            "Should have 3 partitions for gauge, sum, and histogram"
        );

        // Verify gauge partition
        assert!(
            partitions.contains_key("gauge"),
            "Should have gauge partition"
        );
        let (gauge_table, gauge_request) = &partitions["gauge"];
        assert_eq!(
            gauge_table, "metrics_gauge",
            "Gauge should map to metrics_gauge table"
        );
        assert_eq!(
            gauge_request.resource_metrics[0].scope_metrics[0]
                .metrics
                .len(),
            1,
            "Gauge partition should have 1 metric"
        );
        assert_eq!(
            gauge_request.resource_metrics[0].scope_metrics[0].metrics[0].name,
            "gauge_metric"
        );

        // Verify sum partition
        assert!(partitions.contains_key("sum"), "Should have sum partition");
        let (sum_table, sum_request) = &partitions["sum"];
        assert_eq!(
            sum_table, "metrics_sum",
            "Sum should map to metrics_sum table"
        );
        assert_eq!(
            sum_request.resource_metrics[0].scope_metrics[0]
                .metrics
                .len(),
            1,
            "Sum partition should have 1 metric"
        );
        assert_eq!(
            sum_request.resource_metrics[0].scope_metrics[0].metrics[0].name,
            "sum_metric"
        );

        // Verify histogram partition
        assert!(
            partitions.contains_key("histogram"),
            "Should have histogram partition"
        );
        let (histogram_table, histogram_request) = &partitions["histogram"];
        assert_eq!(
            histogram_table, "metrics_histogram",
            "Histogram should map to metrics_histogram table"
        );
        assert_eq!(
            histogram_request.resource_metrics[0].scope_metrics[0]
                .metrics
                .len(),
            1,
            "Histogram partition should have 1 metric"
        );
        assert_eq!(
            histogram_request.resource_metrics[0].scope_metrics[0].metrics[0].name,
            "histogram_metric"
        );
    }

    #[test]
    fn test_partition_metrics_by_type_single_type() {
        // Create a request with only gauge metrics
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "only_gauge".to_string(),
                        description: "Single gauge".to_string(),
                        unit: "1".to_string(),
                        data: Some(Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![],
                                start_time_unix_nano: 1000,
                                time_unix_nano: 2000,
                                value: Some(number_data_point::Value::AsDouble(1.0)),
                                exemplars: vec![],
                                flags: 0,
                            }],
                        })),
                        metadata: vec![],
                    }],
                    schema_url: "".to_string(),
                }],
                schema_url: "".to_string(),
            }],
        };

        let partitions = MetricsHandler::partition_metrics_by_type(&request);

        // Should only have 1 partition
        assert_eq!(partitions.len(), 1, "Should have only 1 partition");
        assert!(
            partitions.contains_key("gauge"),
            "Should only have gauge partition"
        );
    }

    #[test]
    fn test_partition_metrics_by_type_empty() {
        // Create an empty request
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![],
        };

        let partitions = MetricsHandler::partition_metrics_by_type(&request);

        // Should have no partitions
        assert_eq!(
            partitions.len(),
            0,
            "Should have 0 partitions for empty request"
        );
    }

    #[test]
    fn test_partition_metrics_exponential_histogram() {
        use opentelemetry_proto::tonic::metrics::v1::{
            ExponentialHistogram, ExponentialHistogramDataPoint,
        };

        // Create a request with an exponential histogram metric
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "exp_histogram_metric".to_string(),
                        description: "An exponential histogram".to_string(),
                        unit: "ms".to_string(),
                        data: Some(Data::ExponentialHistogram(ExponentialHistogram {
                            data_points: vec![ExponentialHistogramDataPoint {
                                attributes: vec![],
                                start_time_unix_nano: 1000,
                                time_unix_nano: 2000,
                                count: 10,
                                sum: Some(100.0),
                                scale: 2,
                                zero_count: 1,
                                positive: None,
                                negative: None,
                                flags: 0,
                                exemplars: vec![],
                                zero_threshold: 0.0,
                                min: Some(1.0),
                                max: Some(50.0),
                            }],
                            aggregation_temporality: AggregationTemporality::Cumulative.into(),
                        })),
                        metadata: vec![],
                    }],
                    schema_url: "".to_string(),
                }],
                schema_url: "".to_string(),
            }],
        };

        let partitions = MetricsHandler::partition_metrics_by_type(&request);

        // Verify we got the exponential_histogram partition
        assert_eq!(
            partitions.len(),
            1,
            "Should have 1 partition for exponential histogram"
        );
        assert!(
            partitions.contains_key("exponential_histogram"),
            "Should have exponential_histogram partition"
        );

        let (table_name, exp_hist_request) = &partitions["exponential_histogram"];
        assert_eq!(
            table_name, "metrics_exponential_histogram",
            "ExponentialHistogram should map to metrics_exponential_histogram table"
        );
        assert_eq!(
            exp_hist_request.resource_metrics[0].scope_metrics[0]
                .metrics
                .len(),
            1,
            "ExponentialHistogram partition should have 1 metric"
        );
        assert_eq!(
            exp_hist_request.resource_metrics[0].scope_metrics[0].metrics[0].name,
            "exp_histogram_metric"
        );
    }

    #[test]
    fn test_partition_metrics_summary() {
        use opentelemetry_proto::tonic::metrics::v1::{
            Summary, SummaryDataPoint, summary_data_point::ValueAtQuantile,
        };

        // Create a request with a summary metric
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "summary_metric".to_string(),
                        description: "A summary metric".to_string(),
                        unit: "ms".to_string(),
                        data: Some(Data::Summary(Summary {
                            data_points: vec![SummaryDataPoint {
                                attributes: vec![],
                                start_time_unix_nano: 1000,
                                time_unix_nano: 2000,
                                count: 100,
                                sum: 500.0,
                                quantile_values: vec![
                                    ValueAtQuantile {
                                        quantile: 0.5,
                                        value: 4.5,
                                    },
                                    ValueAtQuantile {
                                        quantile: 0.95,
                                        value: 9.5,
                                    },
                                    ValueAtQuantile {
                                        quantile: 0.99,
                                        value: 10.0,
                                    },
                                ],
                                flags: 0,
                            }],
                        })),
                        metadata: vec![],
                    }],
                    schema_url: "".to_string(),
                }],
                schema_url: "".to_string(),
            }],
        };

        let partitions = MetricsHandler::partition_metrics_by_type(&request);

        // Verify we got the summary partition
        assert_eq!(partitions.len(), 1, "Should have 1 partition for summary");
        assert!(
            partitions.contains_key("summary"),
            "Should have summary partition"
        );

        let (table_name, summary_request) = &partitions["summary"];
        assert_eq!(
            table_name, "metrics_summary",
            "Summary should map to metrics_summary table"
        );
        assert_eq!(
            summary_request.resource_metrics[0].scope_metrics[0]
                .metrics
                .len(),
            1,
            "Summary partition should have 1 metric"
        );
        assert_eq!(
            summary_request.resource_metrics[0].scope_metrics[0].metrics[0].name,
            "summary_metric"
        );
    }

    #[test]
    fn test_partition_metrics_all_types() {
        use opentelemetry_proto::tonic::metrics::v1::{
            ExponentialHistogram, ExponentialHistogramDataPoint, Summary, SummaryDataPoint,
            summary_data_point::ValueAtQuantile,
        };

        // Create a request with all metric types
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![
                        // Gauge
                        Metric {
                            name: "gauge_metric".to_string(),
                            description: "A gauge".to_string(),
                            unit: "1".to_string(),
                            data: Some(Data::Gauge(Gauge {
                                data_points: vec![NumberDataPoint {
                                    attributes: vec![],
                                    start_time_unix_nano: 1000,
                                    time_unix_nano: 2000,
                                    value: Some(number_data_point::Value::AsDouble(42.0)),
                                    exemplars: vec![],
                                    flags: 0,
                                }],
                            })),
                            metadata: vec![],
                        },
                        // Sum
                        Metric {
                            name: "sum_metric".to_string(),
                            description: "A sum".to_string(),
                            unit: "1".to_string(),
                            data: Some(Data::Sum(Sum {
                                data_points: vec![NumberDataPoint {
                                    attributes: vec![],
                                    start_time_unix_nano: 1000,
                                    time_unix_nano: 2000,
                                    value: Some(number_data_point::Value::AsInt(100)),
                                    exemplars: vec![],
                                    flags: 0,
                                }],
                                aggregation_temporality: AggregationTemporality::Cumulative.into(),
                                is_monotonic: true,
                            })),
                            metadata: vec![],
                        },
                        // Histogram
                        Metric {
                            name: "histogram_metric".to_string(),
                            description: "A histogram".to_string(),
                            unit: "ms".to_string(),
                            data: Some(Data::Histogram(Histogram {
                                data_points: vec![HistogramDataPoint {
                                    attributes: vec![],
                                    start_time_unix_nano: 1000,
                                    time_unix_nano: 2000,
                                    count: 5,
                                    sum: Some(250.0),
                                    bucket_counts: vec![1, 2, 2],
                                    explicit_bounds: vec![10.0, 50.0],
                                    exemplars: vec![],
                                    flags: 0,
                                    min: Some(5.0),
                                    max: Some(100.0),
                                }],
                                aggregation_temporality: AggregationTemporality::Cumulative.into(),
                            })),
                            metadata: vec![],
                        },
                        // ExponentialHistogram
                        Metric {
                            name: "exp_histogram_metric".to_string(),
                            description: "An exponential histogram".to_string(),
                            unit: "ms".to_string(),
                            data: Some(Data::ExponentialHistogram(ExponentialHistogram {
                                data_points: vec![ExponentialHistogramDataPoint {
                                    attributes: vec![],
                                    start_time_unix_nano: 1000,
                                    time_unix_nano: 2000,
                                    count: 10,
                                    sum: Some(100.0),
                                    scale: 2,
                                    zero_count: 1,
                                    positive: None,
                                    negative: None,
                                    flags: 0,
                                    exemplars: vec![],
                                    zero_threshold: 0.0,
                                    min: Some(1.0),
                                    max: Some(50.0),
                                }],
                                aggregation_temporality: AggregationTemporality::Cumulative.into(),
                            })),
                            metadata: vec![],
                        },
                        // Summary
                        Metric {
                            name: "summary_metric".to_string(),
                            description: "A summary".to_string(),
                            unit: "ms".to_string(),
                            data: Some(Data::Summary(Summary {
                                data_points: vec![SummaryDataPoint {
                                    attributes: vec![],
                                    start_time_unix_nano: 1000,
                                    time_unix_nano: 2000,
                                    count: 100,
                                    sum: 500.0,
                                    quantile_values: vec![ValueAtQuantile {
                                        quantile: 0.5,
                                        value: 4.5,
                                    }],
                                    flags: 0,
                                }],
                            })),
                            metadata: vec![],
                        },
                    ],
                    schema_url: "".to_string(),
                }],
                schema_url: "".to_string(),
            }],
        };

        let partitions = MetricsHandler::partition_metrics_by_type(&request);

        // Verify we got 5 partitions for all types
        assert_eq!(
            partitions.len(),
            5,
            "Should have 5 partitions for all metric types"
        );

        // Verify each type
        assert!(partitions.contains_key("gauge"));
        assert!(partitions.contains_key("sum"));
        assert!(partitions.contains_key("histogram"));
        assert!(partitions.contains_key("exponential_histogram"));
        assert!(partitions.contains_key("summary"));

        // Verify table names
        assert_eq!(partitions["gauge"].0, "metrics_gauge");
        assert_eq!(partitions["sum"].0, "metrics_sum");
        assert_eq!(partitions["histogram"].0, "metrics_histogram");
        assert_eq!(
            partitions["exponential_histogram"].0,
            "metrics_exponential_histogram"
        );
        assert_eq!(partitions["summary"].0, "metrics_summary");
    }
}
