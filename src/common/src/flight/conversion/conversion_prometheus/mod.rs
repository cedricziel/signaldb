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

mod from_otel;
pub mod proto;
mod to_otel;
mod types;

// Re-export public API

// Types
pub use types::{
    BucketSpan, PrometheusHistogram, PrometheusLabel, PrometheusMetricMetadata,
    PrometheusMetricType, PrometheusSample, PrometheusTimeSeries, PrometheusWriteRequest,
};

// Prometheus → OTEL conversion
pub use to_otel::{decode_prometheus_remote_write, prometheus_to_otel_metrics};

// OTEL → Prometheus conversion
pub use from_otel::{OtelToPrometheusConfig, otel_to_prometheus_metrics};

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::metrics::v1::{
        AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint,
        ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint, metric::Data,
        number_data_point, summary_data_point::ValueAtQuantile,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use std::collections::HashMap;

    use to_otel::detect_metric_type;

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
                timestamp: 1_700_000_000_000,
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
            assert_eq!(
                gauge.data_points[0].time_unix_nano,
                1_700_000_000_000 * 1_000_000
            );
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
                timestamp: 1_700_000_000_000,
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
    fn test_convert_histogram() {
        let timeseries = vec![
            PrometheusTimeSeries {
                labels: vec![
                    PrometheusLabel {
                        name: "__name__".to_string(),
                        value: "http_request_duration_bucket".to_string(),
                    },
                    PrometheusLabel {
                        name: "le".to_string(),
                        value: "0.1".to_string(),
                    },
                ],
                samples: vec![PrometheusSample {
                    value: 10.0,
                    timestamp: 1_700_000_000_000,
                }],
                histograms: vec![],
            },
            PrometheusTimeSeries {
                labels: vec![
                    PrometheusLabel {
                        name: "__name__".to_string(),
                        value: "http_request_duration_bucket".to_string(),
                    },
                    PrometheusLabel {
                        name: "le".to_string(),
                        value: "0.5".to_string(),
                    },
                ],
                samples: vec![PrometheusSample {
                    value: 25.0,
                    timestamp: 1_700_000_000_000,
                }],
                histograms: vec![],
            },
            PrometheusTimeSeries {
                labels: vec![
                    PrometheusLabel {
                        name: "__name__".to_string(),
                        value: "http_request_duration_bucket".to_string(),
                    },
                    PrometheusLabel {
                        name: "le".to_string(),
                        value: "+Inf".to_string(),
                    },
                ],
                samples: vec![PrometheusSample {
                    value: 30.0,
                    timestamp: 1_700_000_000_000,
                }],
                histograms: vec![],
            },
            PrometheusTimeSeries {
                labels: vec![PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "http_request_duration_count".to_string(),
                }],
                samples: vec![PrometheusSample {
                    value: 30.0,
                    timestamp: 1_700_000_000_000,
                }],
                histograms: vec![],
            },
            PrometheusTimeSeries {
                labels: vec![PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "http_request_duration_sum".to_string(),
                }],
                samples: vec![PrometheusSample {
                    value: 5.5,
                    timestamp: 1_700_000_000_000,
                }],
                histograms: vec![],
            },
        ];

        let request = PrometheusWriteRequest {
            timeseries,
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);

        let metric = &result.resource_metrics[0].scope_metrics[0].metrics[0];
        assert_eq!(metric.name, "http_request_duration");

        if let Some(Data::Histogram(histogram)) = &metric.data {
            assert_eq!(histogram.data_points.len(), 1);
            let dp = &histogram.data_points[0];
            assert_eq!(dp.count, 30);
            assert_eq!(dp.sum, Some(5.5));
            assert_eq!(dp.explicit_bounds.len(), 2);
            assert_eq!(dp.bucket_counts.len(), 3);
        } else {
            panic!("Expected histogram data");
        }
    }

    #[test]
    fn test_resource_attributes() {
        let ts = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "up".to_string(),
                },
                PrometheusLabel {
                    name: "job".to_string(),
                    value: "prometheus".to_string(),
                },
                PrometheusLabel {
                    name: "instance".to_string(),
                    value: "localhost:9090".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 1.0,
                timestamp: 1_700_000_000_000,
            }],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![ts],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);

        let resource = result.resource_metrics[0].resource.as_ref().unwrap();

        let service_name = resource
            .attributes
            .iter()
            .find(|kv| kv.key == "service.name")
            .unwrap();
        assert_eq!(
            service_name.value,
            Some(AnyValue {
                value: Some(any_value::Value::StringValue("prometheus".to_string()))
            })
        );

        let instance_id = resource
            .attributes
            .iter()
            .find(|kv| kv.key == "service.instance.id")
            .unwrap();
        assert_eq!(
            instance_id.value,
            Some(AnyValue {
                value: Some(any_value::Value::StringValue("localhost:9090".to_string()))
            })
        );
    }

    #[test]
    fn test_otel_scope_labels() {
        let ts = PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "custom_metric".to_string(),
                },
                PrometheusLabel {
                    name: "otel_scope_name".to_string(),
                    value: "my-library".to_string(),
                },
                PrometheusLabel {
                    name: "otel_scope_version".to_string(),
                    value: "1.0.0".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 42.0,
                timestamp: 1_700_000_000_000,
            }],
            histograms: vec![],
        };

        let request = PrometheusWriteRequest {
            timeseries: vec![ts],
            metadata: vec![],
        };

        let result = prometheus_to_otel_metrics(&request);

        let scope = result.resource_metrics[0].scope_metrics[0]
            .scope
            .as_ref()
            .unwrap();
        assert_eq!(scope.name, "my-library");
        assert_eq!(scope.version, "1.0.0");
    }

    // OTEL → Prometheus tests

    #[test]
    fn test_otel_gauge_to_prometheus() {
        let request = ExportMetricsServiceRequest {
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
                        unit: "celsius".to_string(),
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

        let config = OtelToPrometheusConfig::new();
        let result = otel_to_prometheus_metrics(&request, &config);

        // Should have target_info + temperature metric
        assert!(!result.timeseries.is_empty());

        // Find the temperature metric
        let temp_ts = result
            .timeseries
            .iter()
            .find(|ts| {
                ts.labels
                    .iter()
                    .any(|l| l.name == "__name__" && l.value.contains("temperature"))
            })
            .expect("Temperature metric not found");

        assert_eq!(temp_ts.samples[0].value, 23.5);
        assert!(temp_ts.labels.iter().any(|l| l.name == "job"));
    }

    #[test]
    fn test_otel_counter_to_prometheus() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "http_requests".to_string(),
                        description: String::new(),
                        unit: String::new(),
                        data: Some(Data::Sum(Sum {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![],
                                start_time_unix_nano: 1_699_000_000_000_000_000,
                                time_unix_nano: 1_700_000_000_000_000_000,
                                value: Some(number_data_point::Value::AsDouble(100.0)),
                                exemplars: vec![],
                                flags: 0,
                            }],
                            aggregation_temporality: AggregationTemporality::Cumulative as i32,
                            is_monotonic: true,
                        })),
                        metadata: vec![],
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let config = OtelToPrometheusConfig::new();
        let result = otel_to_prometheus_metrics(&request, &config);

        // Should have http_requests_total and http_requests_created
        let total_ts = result
            .timeseries
            .iter()
            .find(|ts| {
                ts.labels
                    .iter()
                    .any(|l| l.name == "__name__" && l.value == "http_requests_total")
            })
            .expect("Total metric not found");

        assert_eq!(total_ts.samples[0].value, 100.0);

        let created_ts = result
            .timeseries
            .iter()
            .find(|ts| {
                ts.labels
                    .iter()
                    .any(|l| l.name == "__name__" && l.value == "http_requests_created")
            })
            .expect("Created metric not found");

        // _created value should be start time in seconds
        assert!((created_ts.samples[0].value - 1_699_000_000.0).abs() < 1.0);
    }

    #[test]
    fn test_otel_histogram_to_prometheus() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "request_duration".to_string(),
                        description: String::new(),
                        unit: "s".to_string(),
                        data: Some(Data::Histogram(Histogram {
                            data_points: vec![HistogramDataPoint {
                                attributes: vec![],
                                start_time_unix_nano: 0,
                                time_unix_nano: 1_700_000_000_000_000_000,
                                count: 30,
                                sum: Some(5.5),
                                bucket_counts: vec![10, 15, 5],
                                explicit_bounds: vec![0.1, 0.5],
                                exemplars: vec![],
                                flags: 0,
                                min: None,
                                max: None,
                            }],
                            aggregation_temporality: AggregationTemporality::Cumulative as i32,
                        })),
                        metadata: vec![],
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let config = OtelToPrometheusConfig::new();
        let result = otel_to_prometheus_metrics(&request, &config);

        // Should have bucket series, count, and sum
        let bucket_series: Vec<_> = result
            .timeseries
            .iter()
            .filter(|ts| {
                ts.labels
                    .iter()
                    .any(|l| l.name == "__name__" && l.value.contains("_bucket"))
            })
            .collect();

        assert_eq!(bucket_series.len(), 3); // 0.1, 0.5, +Inf

        // Verify cumulative counts
        let le_01 = bucket_series
            .iter()
            .find(|ts| ts.labels.iter().any(|l| l.name == "le" && l.value == "0.1"))
            .unwrap();
        assert_eq!(le_01.samples[0].value, 10.0); // First bucket

        let le_05 = bucket_series
            .iter()
            .find(|ts| ts.labels.iter().any(|l| l.name == "le" && l.value == "0.5"))
            .unwrap();
        assert_eq!(le_05.samples[0].value, 25.0); // 10 + 15

        let le_inf = bucket_series
            .iter()
            .find(|ts| {
                ts.labels
                    .iter()
                    .any(|l| l.name == "le" && l.value == "+Inf")
            })
            .unwrap();
        assert_eq!(le_inf.samples[0].value, 30.0); // Total count
    }

    #[test]
    fn test_otel_summary_to_prometheus() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "request_latency".to_string(),
                        description: String::new(),
                        unit: String::new(),
                        data: Some(Data::Summary(Summary {
                            data_points: vec![SummaryDataPoint {
                                attributes: vec![],
                                start_time_unix_nano: 0,
                                time_unix_nano: 1_700_000_000_000_000_000,
                                count: 100,
                                sum: 50.0,
                                quantile_values: vec![
                                    ValueAtQuantile {
                                        quantile: 0.5,
                                        value: 0.4,
                                    },
                                    ValueAtQuantile {
                                        quantile: 0.99,
                                        value: 1.2,
                                    },
                                ],
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

        let config = OtelToPrometheusConfig::new();
        let result = otel_to_prometheus_metrics(&request, &config);

        // Should have quantile series, count, and sum
        let quantile_series: Vec<_> = result
            .timeseries
            .iter()
            .filter(|ts| ts.labels.iter().any(|l| l.name == "quantile"))
            .collect();

        assert_eq!(quantile_series.len(), 2);

        let count_ts = result
            .timeseries
            .iter()
            .find(|ts| {
                ts.labels
                    .iter()
                    .any(|l| l.name == "__name__" && l.value.ends_with("_count"))
            })
            .unwrap();
        assert_eq!(count_ts.samples[0].value, 100.0);
    }

    #[test]
    fn test_round_trip_gauge() {
        // Start with Prometheus data
        let original = PrometheusWriteRequest {
            timeseries: vec![PrometheusTimeSeries {
                labels: vec![
                    PrometheusLabel {
                        name: "__name__".to_string(),
                        value: "temperature".to_string(),
                    },
                    PrometheusLabel {
                        name: "job".to_string(),
                        value: "sensors".to_string(),
                    },
                    PrometheusLabel {
                        name: "location".to_string(),
                        value: "room1".to_string(),
                    },
                ],
                samples: vec![PrometheusSample {
                    value: 23.5,
                    timestamp: 1_700_000_000_000,
                }],
                histograms: vec![],
            }],
            metadata: vec![],
        };

        // Convert to OTEL
        let otel = prometheus_to_otel_metrics(&original);

        // Convert back to Prometheus
        let config = OtelToPrometheusConfig::new();
        let round_tripped = otel_to_prometheus_metrics(&otel, &config);

        // Find the temperature metric
        let temp_ts = round_tripped
            .timeseries
            .iter()
            .find(|ts| {
                ts.labels
                    .iter()
                    .any(|l| l.name == "__name__" && l.value == "temperature")
            })
            .expect("Temperature metric not found after round-trip");

        assert_eq!(temp_ts.samples[0].value, 23.5);
        assert!(
            temp_ts
                .labels
                .iter()
                .any(|l| l.name == "job" && l.value == "sensors")
        );
    }

    #[test]
    fn test_metadata_preservation() {
        let request = PrometheusWriteRequest {
            timeseries: vec![PrometheusTimeSeries {
                labels: vec![PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "http_requests_total".to_string(),
                }],
                samples: vec![PrometheusSample {
                    value: 100.0,
                    timestamp: 1_700_000_000_000,
                }],
                histograms: vec![],
            }],
            metadata: vec![PrometheusMetricMetadata {
                metric_family_name: "http_requests".to_string(),
                metric_type: PrometheusMetricType::Counter,
                help: "Total HTTP requests".to_string(),
                unit: String::new(),
            }],
        };

        let result = prometheus_to_otel_metrics(&request);

        let metric = &result.resource_metrics[0].scope_metrics[0].metrics[0];

        // Check prometheus.type metadata
        let prom_type = metric
            .metadata
            .iter()
            .find(|kv| kv.key == "prometheus.type")
            .unwrap();
        assert_eq!(
            prom_type.value,
            Some(AnyValue {
                value: Some(any_value::Value::StringValue("counter".to_string()))
            })
        );
    }

    #[test]
    fn test_unit_normalization() {
        use from_otel::normalize_unit;

        assert_eq!(normalize_unit("s"), "seconds");
        assert_eq!(normalize_unit("ms"), "milliseconds");
        assert_eq!(normalize_unit("By"), "bytes");
        assert_eq!(normalize_unit("1"), "ratio");
        assert_eq!(normalize_unit("requests/second"), "requests_per_second");
    }

    #[test]
    fn test_metric_name_normalization() {
        use from_otel::normalize_metric_name;

        assert_eq!(
            normalize_metric_name("http.server.requests"),
            "http_server_requests"
        );
        assert_eq!(normalize_metric_name("cpu-usage"), "cpu_usage");
        assert_eq!(normalize_metric_name("valid_name"), "valid_name");
    }
}
