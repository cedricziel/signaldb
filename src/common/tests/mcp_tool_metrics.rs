//! `signaldb.mcp.tool_calls` / `signaldb.mcp.tool_call.duration` record one
//! point per MCP tool call, keyed by tool and audit outcome.
//!
//! One test per binary on purpose: `app_metrics()` binds to the global meter
//! provider exactly once per process, so the in-memory provider must be
//! installed before anything else touches it.

use common::self_monitoring::app_metrics::{MCP_OUTCOME_ATTR, MCP_TOOL_ATTR, app_metrics};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};

fn attr_value(attrs: &[opentelemetry::KeyValue], key: &str) -> Option<String> {
    attrs
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| kv.value.as_str().to_string())
}

#[test]
fn mcp_tool_call_metrics_record_by_tool_and_outcome() {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_meter_provider(provider.clone());

    let metrics = app_metrics();
    metrics.record_mcp_tool_call("search_traces", "ok", std::time::Duration::from_millis(40));
    metrics.record_mcp_tool_call("search_traces", "ok", std::time::Duration::from_millis(60));
    metrics.record_mcp_tool_call("get_trace", "denied", std::time::Duration::from_millis(5));
    provider.force_flush().unwrap();

    let finished = exporter.get_finished_metrics().unwrap();
    let mut counter_points: Vec<(String, String, u64)> = Vec::new();
    let mut histogram_points: Vec<(String, u64)> = Vec::new();
    for rm in &finished {
        for sm in rm.scope_metrics() {
            for metric in sm.metrics() {
                match (metric.name(), metric.data()) {
                    ("signaldb.mcp.tool_calls", AggregatedMetrics::U64(MetricData::Sum(sum))) => {
                        for p in sum.data_points() {
                            let attrs: Vec<_> = p.attributes().cloned().collect();
                            counter_points.push((
                                attr_value(&attrs, MCP_TOOL_ATTR).unwrap_or_default(),
                                attr_value(&attrs, MCP_OUTCOME_ATTR).unwrap_or_default(),
                                p.value(),
                            ));
                        }
                    }
                    (
                        "signaldb.mcp.tool_call.duration",
                        AggregatedMetrics::F64(MetricData::Histogram(h)),
                    ) => {
                        for p in h.data_points() {
                            let attrs: Vec<_> = p.attributes().cloned().collect();
                            histogram_points.push((
                                attr_value(&attrs, MCP_TOOL_ATTR).unwrap_or_default(),
                                p.count(),
                            ));
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    counter_points.sort();
    assert_eq!(
        counter_points,
        vec![
            ("get_trace".to_string(), "denied".to_string(), 1),
            ("search_traces".to_string(), "ok".to_string(), 2),
        ]
    );
    histogram_points.sort();
    assert_eq!(
        histogram_points,
        vec![
            ("get_trace".to_string(), 1),
            ("search_traces".to_string(), 2)
        ]
    );
}
