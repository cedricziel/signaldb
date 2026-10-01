use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::{
    ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram, HistogramDataPoint,
    Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Summary, SummaryDataPoint,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use ottl::{ErrorMode, Limits, Signal, compile};

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

fn request_with_metric(name: &str, data: Data) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource::default()),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope::default()),
                metrics: vec![Metric {
                    name: name.to_string(),
                    data: Some(data),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn attrs(req: &ExportMetricsServiceRequest) -> &[KeyValue] {
    match req.resource_metrics[0].scope_metrics[0].metrics[0]
        .data
        .as_ref()
        .unwrap()
    {
        Data::Gauge(g) => &g.data_points[0].attributes,
        Data::Sum(s) => &s.data_points[0].attributes,
        Data::Histogram(h) => &h.data_points[0].attributes,
        Data::ExponentialHistogram(h) => &h.data_points[0].attributes,
        Data::Summary(s) => &s.data_points[0].attributes,
    }
}

fn run_rename_and_attr_set(mut req: ExportMetricsServiceRequest) -> ExportMetricsServiceRequest {
    let program = compile(
        Signal::Metrics,
        &[
            r#"set(name, "renamed")"#.to_string(),
            r#"set(attributes["k"], "v")"#.to_string(),
        ],
        &Limits::default(),
    )
    .expect("compiles");
    program
        .apply_metrics(&mut req, ErrorMode::Propagate)
        .expect("applies");
    req
}

#[test]
fn gauge_data_point() {
    let req = request_with_metric(
        "orig",
        Data::Gauge(Gauge {
            data_points: vec![NumberDataPoint::default()],
        }),
    );
    let req = run_rename_and_attr_set(req);
    assert_eq!(
        req.resource_metrics[0].scope_metrics[0].metrics[0].name,
        "renamed"
    );
    assert!(attrs(&req).iter().any(|kv| kv.key == "k"));
}

#[test]
fn sum_data_point() {
    let req = request_with_metric(
        "orig",
        Data::Sum(opentelemetry_proto::tonic::metrics::v1::Sum {
            data_points: vec![NumberDataPoint::default()],
            ..Default::default()
        }),
    );
    let req = run_rename_and_attr_set(req);
    assert_eq!(
        req.resource_metrics[0].scope_metrics[0].metrics[0].name,
        "renamed"
    );
}

#[test]
fn histogram_data_point() {
    let req = request_with_metric(
        "orig",
        Data::Histogram(Histogram {
            data_points: vec![HistogramDataPoint::default()],
            ..Default::default()
        }),
    );
    let req = run_rename_and_attr_set(req);
    assert_eq!(
        req.resource_metrics[0].scope_metrics[0].metrics[0].name,
        "renamed"
    );
}

#[test]
fn exponential_histogram_data_point() {
    let req = request_with_metric(
        "orig",
        Data::ExponentialHistogram(ExponentialHistogram {
            data_points: vec![ExponentialHistogramDataPoint::default()],
            ..Default::default()
        }),
    );
    let req = run_rename_and_attr_set(req);
    assert_eq!(
        req.resource_metrics[0].scope_metrics[0].metrics[0].name,
        "renamed"
    );
}

#[test]
fn summary_data_point() {
    let req = request_with_metric(
        "orig",
        Data::Summary(Summary {
            data_points: vec![SummaryDataPoint::default()],
        }),
    );
    let req = run_rename_and_attr_set(req);
    assert_eq!(
        req.resource_metrics[0].scope_metrics[0].metrics[0].name,
        "renamed"
    );
}

#[test]
fn resource_edit_runs_once_per_data_point_and_is_idempotent() {
    let program = compile(
        Signal::Metrics,
        &[r#"set(resource.attributes["env"], "prod")"#.to_string()],
        &Limits::default(),
    )
    .expect("compiles");
    let mut req = request_with_metric(
        "m",
        Data::Gauge(Gauge {
            data_points: vec![NumberDataPoint::default(), NumberDataPoint::default()],
        }),
    );
    let report = program
        .apply_metrics(&mut req, ErrorMode::Propagate)
        .expect("applies");
    // Ran once per point (2 points), each application is a no-op idempotently.
    assert_eq!(report.statements[0].matched, 2);
    assert_eq!(
        req.resource_metrics[0]
            .resource
            .as_ref()
            .unwrap()
            .attributes
            .iter()
            .filter(|kv| kv.key == "env")
            .count(),
        1
    );
}

#[test]
fn datapoint_attributes_path_is_equivalent_to_bare_attributes() {
    let program = compile(
        Signal::Metrics,
        &[r#"set(datapoint.attributes["k"], "v")"#.to_string()],
        &Limits::default(),
    )
    .expect("compiles");
    let mut req = request_with_metric(
        "m",
        Data::Gauge(Gauge {
            data_points: vec![NumberDataPoint {
                attributes: vec![kv("existing", "x")],
                ..Default::default()
            }],
        }),
    );
    program
        .apply_metrics(&mut req, ErrorMode::Propagate)
        .expect("applies");
    assert!(attrs(&req).iter().any(|kv| kv.key == "k"));
}
