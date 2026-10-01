//! Read-only off-type attribute warnings surfaced to OTLP senders via
//! `partial_success`. The acceptor never places values into a typed home
//! or writes `attribute_types` — only the writer establishes canonical
//! types; this module just looks it up (read-only, via [`TypeSnapshots`])
//! and, on a mismatch, builds a bounded, deterministic message. Nothing is
//! rejected and no metric is emitted here (the writer counts; counting
//! here would double count and include resends the writer drops).
//!
//! [`OffTypeAttributes`] (per request type) and [`WithOffTypeWarning`]
//! (per response type) let the gRPC services and the OTLP/HTTP path share
//! one [`off_type_warning`] instead of a function pair per signal.

use std::collections::BTreeMap;
use std::sync::Arc;

use common::auth::TenantContext;
use common::schema::logical::AttributeLevel;
use common::schema::type_authority::{CanonicalType, ObservedKind, TypeSnapshots, off_type_keys};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsPartialSuccess, ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::profiles::v1development::{
    ExportProfilesPartialSuccess, ExportProfilesServiceRequest, ExportProfilesServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, InstrumentationScope, KeyValue, any_value::Value,
};
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::resource::v1::Resource;

/// At most this many offending keys are named in the warning message; the
/// rest are folded into "and N more".
const MAX_LISTED_KEYS: usize = 10;

fn observed(value: &Option<AnyValue>) -> ObservedKind {
    match value.as_ref().and_then(|v| v.value.as_ref()) {
        Some(Value::StringValue(_) | Value::StringValueStrindex(_)) => ObservedKind::String,
        Some(Value::BoolValue(_)) => ObservedKind::Bool,
        Some(Value::IntValue(_)) => ObservedKind::Int64,
        Some(Value::DoubleValue(_)) => ObservedKind::Float64,
        Some(Value::ArrayValue(_)) => ObservedKind::Array,
        Some(Value::KvlistValue(_)) => ObservedKind::KvList,
        Some(Value::BytesValue(_)) => ObservedKind::Bytes,
        None => ObservedKind::Empty,
    }
}

fn attrs_at(
    level: AttributeLevel,
    attrs: &[KeyValue],
) -> impl Iterator<Item = (AttributeLevel, &str, ObservedKind)> {
    attrs
        .iter()
        .map(move |kv| (level, kv.key.as_str(), observed(&kv.value)))
}

fn resource_attrs(
    resource: &Option<Resource>,
) -> impl Iterator<Item = (AttributeLevel, &str, ObservedKind)> {
    resource
        .iter()
        .flat_map(|r| attrs_at(AttributeLevel::Resource, &r.attributes))
}

fn scope_attrs(
    scope: &Option<InstrumentationScope>,
) -> impl Iterator<Item = (AttributeLevel, &str, ObservedKind)> {
    scope
        .iter()
        .flat_map(|s| attrs_at(AttributeLevel::Scope, &s.attributes))
}

fn record_attrs(attrs: &[KeyValue]) -> impl Iterator<Item = (AttributeLevel, &str, ObservedKind)> {
    attrs_at(AttributeLevel::Record, attrs)
}

/// Every data point's attributes, whichever `Metric::Data` variant.
fn metric_data_point_attrs(data: &Option<MetricData>) -> Vec<&[KeyValue]> {
    fn points<T>(points: &[T], attrs: impl Fn(&T) -> &[KeyValue]) -> Vec<&[KeyValue]> {
        points.iter().map(attrs).collect()
    }
    match data {
        Some(MetricData::Gauge(g)) => points(&g.data_points, |dp| &dp.attributes),
        Some(MetricData::Sum(s)) => points(&s.data_points, |dp| &dp.attributes),
        Some(MetricData::Histogram(h)) => points(&h.data_points, |dp| &dp.attributes),
        Some(MetricData::ExponentialHistogram(e)) => points(&e.data_points, |dp| &dp.attributes),
        Some(MetricData::Summary(s)) => points(&s.data_points, |dp| &dp.attributes),
        None => Vec::new(),
    }
}

/// Walks one OTLP export request's attributes for the off-type check.
pub trait OffTypeAttributes {
    fn off_type_attrs(&self) -> impl Iterator<Item = (AttributeLevel, &str, ObservedKind)>;
}

impl OffTypeAttributes for ExportTraceServiceRequest {
    fn off_type_attrs(&self) -> impl Iterator<Item = (AttributeLevel, &str, ObservedKind)> {
        self.resource_spans.iter().flat_map(|rs| {
            resource_attrs(&rs.resource).chain(rs.scope_spans.iter().flat_map(|ss| {
                scope_attrs(&ss.scope)
                    .chain(ss.spans.iter().flat_map(|s| record_attrs(&s.attributes)))
            }))
        })
    }
}

impl OffTypeAttributes for ExportLogsServiceRequest {
    fn off_type_attrs(&self) -> impl Iterator<Item = (AttributeLevel, &str, ObservedKind)> {
        self.resource_logs.iter().flat_map(|rl| {
            resource_attrs(&rl.resource).chain(rl.scope_logs.iter().flat_map(|sl| {
                scope_attrs(&sl.scope).chain(
                    sl.log_records
                        .iter()
                        .flat_map(|lr| record_attrs(&lr.attributes)),
                )
            }))
        })
    }
}

impl OffTypeAttributes for ExportMetricsServiceRequest {
    fn off_type_attrs(&self) -> impl Iterator<Item = (AttributeLevel, &str, ObservedKind)> {
        self.resource_metrics.iter().flat_map(|rm| {
            resource_attrs(&rm.resource).chain(rm.scope_metrics.iter().flat_map(|sm| {
                scope_attrs(&sm.scope).chain(sm.metrics.iter().flat_map(|m| {
                    metric_data_point_attrs(&m.data)
                        .into_iter()
                        .flat_map(record_attrs)
                }))
            }))
        })
    }
}

impl OffTypeAttributes for ExportProfilesServiceRequest {
    /// Sample attributes are interned through the profile dictionary, so
    /// only resource and scope attributes are checked.
    fn off_type_attrs(&self) -> impl Iterator<Item = (AttributeLevel, &str, ObservedKind)> {
        self.resource_profiles.iter().flat_map(|rp| {
            resource_attrs(&rp.resource).chain(
                rp.scope_profiles
                    .iter()
                    .flat_map(|sp| scope_attrs(&sp.scope)),
            )
        })
    }
}

/// Carries an off-type warning message into an `Export*ServiceResponse`'s
/// `partial_success`, or `Default::default()` when there is none —
/// byte-identical to today's response for existing clients.
pub trait WithOffTypeWarning: prost::Message + Default {
    fn with_off_type_warning(warning: Option<String>) -> Self;
}

macro_rules! impl_with_off_type_warning {
    ($response:ty, $partial:ident, $rejected_field:ident) => {
        impl WithOffTypeWarning for $response {
            fn with_off_type_warning(warning: Option<String>) -> Self {
                Self {
                    partial_success: warning.map(|error_message| $partial {
                        $rejected_field: 0,
                        error_message,
                    }),
                }
            }
        }
    };
}

impl_with_off_type_warning!(
    ExportTraceServiceResponse,
    ExportTracePartialSuccess,
    rejected_spans
);
impl_with_off_type_warning!(
    ExportLogsServiceResponse,
    ExportLogsPartialSuccess,
    rejected_log_records
);
impl_with_off_type_warning!(
    ExportMetricsServiceResponse,
    ExportMetricsPartialSuccess,
    rejected_data_points
);
impl_with_off_type_warning!(
    ExportProfilesServiceResponse,
    ExportProfilesPartialSuccess,
    rejected_profiles
);

fn observed_str(observed: ObservedKind) -> &'static str {
    match observed {
        ObservedKind::String => "string",
        ObservedKind::Int64 => "int64",
        ObservedKind::Float64 => "float64",
        ObservedKind::Bool => "bool",
        ObservedKind::Bytes => "bytes",
        ObservedKind::Array => "array",
        ObservedKind::KvList => "kvlist",
        ObservedKind::Empty => "empty",
    }
}

/// Builds the bounded, deterministic warning message for `off_type`, or
/// `None` when nothing was off-type.
fn build_message(
    off_type: BTreeMap<(AttributeLevel, &str), (CanonicalType, ObservedKind)>,
) -> Option<String> {
    if off_type.is_empty() {
        return None;
    }

    let total = off_type.len();
    let mut entries = off_type.into_iter();
    let listed: Vec<String> = entries
        .by_ref()
        .take(MAX_LISTED_KEYS)
        .map(|((level, key), (canonical, observed))| {
            format!(
                "{key} ({}, canonical {}, sent {})",
                level.as_str(),
                canonical.as_str(),
                observed_str(observed)
            )
        })
        .collect();
    let remaining = total.saturating_sub(listed.len());
    let suffix = if remaining > 0 {
        format!(", and {remaining} more")
    } else {
        String::new()
    };

    Some(format!(
        "{total} attribute value(s) sent with a type other than the field's canonical type \
         were stored as sent and are not filterable by type: {}{suffix}",
        listed.join(", ")
    ))
}

/// The off-type warning for one export request, or `None` when: the
/// tenant is the `_system` self-monitoring tenant, `type_snapshots` is
/// `None` (no cache attached — never blocks on the catalog to get one),
/// the (tenant, dataset, signal) has no cached snapshot yet, or every
/// attribute matched its canonical type.
pub fn off_type_warning<Req: OffTypeAttributes>(
    type_snapshots: Option<&Arc<TypeSnapshots>>,
    tenant_context: &TenantContext,
    signal: &str,
    request: &Req,
) -> Option<String> {
    if common::self_monitoring::is_self_monitoring_tenant(&tenant_context.tenant_id) {
        return None;
    }
    let snapshot = type_snapshots?.get(
        &tenant_context.tenant_id,
        &tenant_context.dataset_id,
        signal,
    )?;
    if snapshot.is_empty() {
        return None;
    }
    build_message(off_type_keys(&snapshot, request.off_type_attrs()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::catalog::Catalog;
    use common::schema::logical::LogicalFieldId;
    use common::schema::type_authority::{Resolution, TypeSource};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
    use std::time::Duration;

    fn test_tenant_context(tenant_id: &str) -> TenantContext {
        TenantContext {
            tenant_id: tenant_id.to_string(),
            dataset_id: "prod".to_string(),
            ..crate::handler::test_support::test_tenant_context()
        }
    }

    fn any_value(value: Value) -> Option<AnyValue> {
        Some(AnyValue { value: Some(value) })
    }

    /// Seeds `signal`'s `http.status_code` (record level) as Int64 and
    /// returns a primed `TypeSnapshots`.
    async fn snapshots_with_int64_status_code(signal: &str) -> Arc<TypeSnapshots> {
        let catalog = Catalog::new_in_memory().await.unwrap();
        let field = LogicalFieldId {
            source: signal.to_string(),
            level: Some(AttributeLevel::Record),
            name: "http.status_code".to_string(),
        };
        catalog
            .establish_attribute_type(
                "acme",
                "prod",
                &field,
                Resolution {
                    canonical: CanonicalType::Int64,
                    source: TypeSource::Observed,
                    hint_schema_url: None,
                },
            )
            .await
            .unwrap();
        let snapshots = Arc::new(TypeSnapshots::new(catalog, Duration::from_secs(30)));
        snapshots.refresh("acme", "prod", signal).await;
        snapshots
    }

    fn status_code_kv(status_code: Option<AnyValue>) -> KeyValue {
        KeyValue {
            key: "http.status_code".to_string(),
            value: status_code,
            ..Default::default()
        }
    }

    fn trace_request(status_code: Option<AnyValue>) -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        attributes: vec![status_code_kv(status_code)],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn log_request(status_code: Option<AnyValue>) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        attributes: vec![status_code_kv(status_code)],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    /// The message names the key, its canonical type, and what was sent.
    fn assert_names_status_code_as_off_type(warning: Option<String>) {
        let warning = warning.expect("off-type warning");
        assert!(warning.contains("http.status_code"));
        assert!(warning.contains("canonical int64"));
        assert!(warning.contains("sent string"));
    }

    #[tokio::test]
    async fn off_type_string_sent_for_an_int64_field_names_the_key() {
        let snapshots = snapshots_with_int64_status_code("traces").await;
        let request = trace_request(any_value(Value::StringValue("200".to_string())));

        assert_names_status_code_as_off_type(off_type_warning(
            Some(&snapshots),
            &test_tenant_context("acme"),
            "traces",
            &request,
        ));
    }

    /// Same off-type check, logs signal: `OffTypeAttributes` walks
    /// resource → scope → log record attributes just like traces walks
    /// resource → scope → span attributes.
    #[tokio::test]
    async fn off_type_string_sent_for_an_int64_log_field_names_the_key() {
        let snapshots = snapshots_with_int64_status_code("logs").await;
        let request = log_request(any_value(Value::StringValue("200".to_string())));

        assert_names_status_code_as_off_type(off_type_warning(
            Some(&snapshots),
            &test_tenant_context("acme"),
            "logs",
            &request,
        ));
    }

    /// No warning when: the type matches, there's no snapshot yet, or the
    /// tenant is `_system` (skipped before any snapshot lookup).
    #[tokio::test]
    async fn matching_type_missing_snapshot_and_self_monitoring_tenant_produce_no_warning() {
        let snapshots = snapshots_with_int64_status_code("traces").await;
        let matching = trace_request(any_value(Value::IntValue(200)));
        let off_type = trace_request(any_value(Value::StringValue("200".to_string())));
        let ctx = test_tenant_context("acme");

        assert_eq!(
            off_type_warning(Some(&snapshots), &ctx, "traces", &matching),
            None
        );
        assert_eq!(off_type_warning(None, &ctx, "traces", &off_type), None);
        assert_eq!(
            off_type_warning(
                Some(&snapshots),
                &test_tenant_context("_system"),
                "traces",
                &off_type
            ),
            None
        );
    }
}
