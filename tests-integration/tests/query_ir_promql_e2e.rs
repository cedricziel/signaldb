//! PromQL lowered to the IR (`ql_ir::promql_to_ir`) and run end to end
//! through `POST /api/v1/query`: ingest → WAL → writer → Iceberg → querier
//! → router, over OTLP counters.

use axum::{Router, http::StatusCode};
use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::KeyValue,
    metrics::v1::{
        AggregationTemporality, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
        metric::Data, number_data_point,
    },
    resource::v1::Resource,
};

use super::query_ir_e2e::{
    TestServices, build_router, post_ir, setup, string_value, test_tenant_context,
};

const T0: u64 = 1_700_000_000_000_000_000;
const MINUTE: u64 = 60_000_000_000;

/// A cumulative counter `name` of `service` with attribute `inst`, started
/// at `T0` and valued `values[i]` at `T0 + i·1m`.
fn counter(name: &str, service: &str, inst: &str, values: &[f64]) -> ExportMetricsServiceRequest {
    let kv = |key: &str, value: &str| KeyValue {
        key: key.to_string(),
        value: Some(string_value(value)),
        ..Default::default()
    };
    let data_points = values
        .iter()
        .enumerate()
        .map(|(i, v)| NumberDataPoint {
            attributes: vec![kv("inst", inst)],
            start_time_unix_nano: T0,
            time_unix_nano: T0 + i as u64 * MINUTE,
            value: Some(number_data_point::Value::AsDouble(*v)),
            ..Default::default()
        })
        .collect();
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![kv("service.name", service)],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: name.to_string(),
                    unit: "1".to_string(),
                    data: Some(Data::Sum(Sum {
                        data_points,
                        aggregation_temporality: AggregationTemporality::Cumulative.into(),
                        is_monotonic: true,
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// Lower `promql` at the instant `T0 + 1m`, run it, and return each
/// series' labels and value.
async fn promql(app: &Router, promql: &str) -> Vec<(serde_json::Value, f64)> {
    let params = ql_ir::PromqlParams::instant((T0 + MINUTE) as i64);
    let doc = ql_ir::promql_to_ir(promql, &params).expect("lowers");
    let mut doc = serde_json::to_value(doc).unwrap();
    // The wire range carries nanoseconds as numeric strings.
    for bound in ["from", "to"] {
        let ns = doc["range"][bound].to_string();
        doc["range"][bound] = serde_json::Value::String(ns);
    }
    let (status, body) = post_ir(app, doc).await;
    assert_eq!(status, StatusCode::OK, "{promql}: {body}");
    body["series"]
        .as_array()
        .unwrap_or_else(|| panic!("{promql}: {body}"))
        .iter()
        .map(|s| {
            let points = s["points"].as_array().expect("points");
            assert_eq!(points.len(), 1, "{promql}: {s}");
            let value = points[0][1].as_f64().expect("a numeric value");
            (s["labels"].clone(), value)
        })
        .collect()
}

/// Boot the stack, ingest `counters` and flush them to storage. The
/// services own the temp storage, so the caller keeps them alive.
async fn ingest(counters: Vec<ExportMetricsServiceRequest>) -> (TestServices, Router) {
    let services = setup().await;
    let ctx = test_tenant_context();
    for request in counters {
        services
            .metrics_handler
            .handle_grpc_otlp_metrics(&ctx, request)
            .await
            .expect("ingest counter");
    }
    common::testing::flush_storage_writers(&services.flight_transport, "test-tenant", None)
        .await
        .expect("flush writer");
    let app = build_router(&services).await;
    (services, app)
}

/// A read window opening in the hour of the points it reads: a strict
/// lower bound on `timestamp` used to prune that hour's partition.
#[tokio::test]
async fn a_sample_reads_points_in_the_hour_its_window_opens_in() {
    let (_services, app) = ingest(vec![counter("x", "api", "1", &[0.0, 60.0])]).await;
    let got = promql(&app, "x").await;
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(got[0].1, 60.0);
}

#[tokio::test]
async fn lowered_promql_reduces_and_matches_series_end_to_end() {
    let (_services, app) = ingest(vec![
        counter("x", "api", "1", &[0.0, 60.0]),
        counter("x", "api", "2", &[0.0, 120.0]),
        counter("x", "web", "1", &[0.0, 300.0]),
        counter("y", "api", "1", &[0.0, 30.0]),
        counter("y", "web", "1", &[0.0, 150.0]),
    ])
    .await;

    // Rates over 5m: x api 0.2 + 0.4, x web 1.0; y api 0.1, y web 0.5.
    let ratio = promql(
        &app,
        "sum by (job) (rate(x[5m])) / on(job) group_left sum by (job) (rate(y[5m]))",
    )
    .await;
    let ratio: Vec<_> = ratio
        .into_iter()
        .map(|(labels, v)| (labels.to_string(), (v * 1e9).round() / 1e9))
        .collect();
    assert_eq!(
        ratio,
        [
            (r#"{"service.name":"api"}"#.to_string(), 6.0),
            (r#"{"service.name":"web"}"#.to_string(), 2.0),
        ]
    );

    let top = promql(&app, "topk(1, x)").await;
    assert_eq!(top.len(), 1, "{top:?}");
    let (labels, value) = &top[0];
    assert_eq!(*value, 300.0);
    assert_eq!(labels["metric.name"], "x");
    assert_eq!(labels["service.name"], "web");
    assert_eq!(labels["inst"], "1");
}
