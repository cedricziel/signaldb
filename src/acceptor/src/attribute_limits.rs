//! Per-record attribute guardrails applied at ingest, for all four signals
//! on both OTLP transports (the handlers call these once per export).
//!
//! An attribute whose key or value exceeds its byte limit is dropped, then
//! any attributes past `max_attributes` are dropped, keeping the sender's
//! order. The number dropped is added to the owning message's
//! `dropped_attributes_count` (never overwriting the sender's value) where
//! the proto has one, and is always counted in
//! `signaldb.ingest.attributes_dropped`.

use std::collections::HashMap;

use common::config::{AcceptorConfig, AttributeLimits, AuthConfig};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::profiles::v1development::ExportProfilesServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{InstrumentationScope, KeyValue, any_value::Value};
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;

/// Attribute limits resolved per tenant: the tenant's own
/// `limits.attribute_limits`, else `[auth].default_limits.attribute_limits`,
/// else `[acceptor.attribute_limits]`. An override table replaces the
/// fallback wholesale; fields it omits take the [`AttributeLimits`]
/// defaults. Tenants provisioned at runtime have no config entry and get
/// the fallback.
#[derive(Debug, Default)]
pub struct TenantAttributeLimits {
    fallback: AttributeLimits,
    overrides: HashMap<String, AttributeLimits>,
}

impl TenantAttributeLimits {
    pub fn new(acceptor: &AcceptorConfig, auth: &AuthConfig) -> Self {
        let fallback = auth
            .default_limits
            .attribute_limits
            .clone()
            .unwrap_or_else(|| acceptor.attribute_limits.clone());
        let overrides = auth
            .tenants
            .iter()
            .filter_map(|t| {
                let limits = t.limits.as_ref()?.attribute_limits.clone()?;
                Some((t.id.clone(), limits))
            })
            .collect();
        Self {
            fallback,
            overrides,
        }
    }

    /// The same limits for every tenant.
    pub fn uniform(limits: AttributeLimits) -> Self {
        Self {
            fallback: limits,
            overrides: HashMap::new(),
        }
    }

    pub fn for_tenant(&self, tenant_id: &str) -> &AttributeLimits {
        self.overrides.get(tenant_id).unwrap_or(&self.fallback)
    }
}

/// Attributes dropped from one export request, by the level of the list
/// they were dropped from.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Dropped {
    pub resource: u64,
    pub scope: u64,
    /// Span, span event, span link, log record, metric data point and
    /// profile/sample attributes.
    pub record: u64,
}

impl Dropped {
    pub fn total(&self) -> u64 {
        self.resource + self.scope + self.record
    }
}

fn value_bytes(kv: &KeyValue) -> usize {
    match kv.value.as_ref().and_then(|v| v.value.as_ref()) {
        Some(Value::StringValue(s)) => s.len(),
        Some(Value::BytesValue(b)) => b.len(),
        Some(Value::ArrayValue(a)) => a.encoded_len(),
        Some(Value::KvlistValue(k)) => k.encoded_len(),
        _ => 0,
    }
}

/// Applies `limits` to one attribute list and returns how many attributes
/// were dropped.
pub fn cap_attrs(attrs: &mut Vec<KeyValue>, limits: &AttributeLimits) -> u32 {
    let before = attrs.len();
    attrs.retain(|kv| {
        kv.key.len() <= limits.max_key_bytes && value_bytes(kv) <= limits.max_value_bytes
    });
    attrs.truncate(limits.max_attributes);
    (before - attrs.len()) as u32
}

/// Caps `attrs`, adding the drops to the owner's `dropped_attributes_count`
/// and to the level's running total.
fn cap_into(
    attrs: &mut Vec<KeyValue>,
    owner_count: &mut u32,
    level_total: &mut u64,
    limits: &AttributeLimits,
) {
    let n = cap_attrs(attrs, limits);
    *owner_count = owner_count.saturating_add(n);
    *level_total += u64::from(n);
}

fn cap_resource(resource: &mut Option<Resource>, dropped: &mut Dropped, limits: &AttributeLimits) {
    if let Some(r) = resource {
        cap_into(
            &mut r.attributes,
            &mut r.dropped_attributes_count,
            &mut dropped.resource,
            limits,
        );
    }
}

fn cap_scope(
    scope: &mut Option<InstrumentationScope>,
    dropped: &mut Dropped,
    limits: &AttributeLimits,
) {
    if let Some(s) = scope {
        cap_into(
            &mut s.attributes,
            &mut s.dropped_attributes_count,
            &mut dropped.scope,
            limits,
        );
    }
}

pub fn cap_traces(request: &mut ExportTraceServiceRequest, limits: &AttributeLimits) -> Dropped {
    let mut d = Dropped::default();
    for rs in &mut request.resource_spans {
        cap_resource(&mut rs.resource, &mut d, limits);
        for ss in &mut rs.scope_spans {
            cap_scope(&mut ss.scope, &mut d, limits);
            for span in &mut ss.spans {
                cap_into(
                    &mut span.attributes,
                    &mut span.dropped_attributes_count,
                    &mut d.record,
                    limits,
                );
                for event in &mut span.events {
                    cap_into(
                        &mut event.attributes,
                        &mut event.dropped_attributes_count,
                        &mut d.record,
                        limits,
                    );
                }
                for link in &mut span.links {
                    cap_into(
                        &mut link.attributes,
                        &mut link.dropped_attributes_count,
                        &mut d.record,
                        limits,
                    );
                }
            }
        }
    }
    d
}

pub fn cap_logs(request: &mut ExportLogsServiceRequest, limits: &AttributeLimits) -> Dropped {
    let mut d = Dropped::default();
    for rl in &mut request.resource_logs {
        cap_resource(&mut rl.resource, &mut d, limits);
        for sl in &mut rl.scope_logs {
            cap_scope(&mut sl.scope, &mut d, limits);
            for record in &mut sl.log_records {
                cap_into(
                    &mut record.attributes,
                    &mut record.dropped_attributes_count,
                    &mut d.record,
                    limits,
                );
            }
        }
    }
    d
}

/// Metric data points carry no `dropped_attributes_count`, so their drops
/// are only counted.
pub fn cap_metrics(request: &mut ExportMetricsServiceRequest, limits: &AttributeLimits) -> Dropped {
    let mut d = Dropped::default();
    for rm in &mut request.resource_metrics {
        cap_resource(&mut rm.resource, &mut d, limits);
        for sm in &mut rm.scope_metrics {
            cap_scope(&mut sm.scope, &mut d, limits);
            for metric in &mut sm.metrics {
                let mut cap = |attrs: &mut Vec<KeyValue>| {
                    d.record += u64::from(cap_attrs(attrs, limits));
                };
                match &mut metric.data {
                    Some(MetricData::Gauge(g)) => g
                        .data_points
                        .iter_mut()
                        .for_each(|p| cap(&mut p.attributes)),
                    Some(MetricData::Sum(s)) => s
                        .data_points
                        .iter_mut()
                        .for_each(|p| cap(&mut p.attributes)),
                    Some(MetricData::Histogram(h)) => h
                        .data_points
                        .iter_mut()
                        .for_each(|p| cap(&mut p.attributes)),
                    Some(MetricData::ExponentialHistogram(e)) => e
                        .data_points
                        .iter_mut()
                        .for_each(|p| cap(&mut p.attributes)),
                    Some(MetricData::Summary(s)) => s
                        .data_points
                        .iter_mut()
                        .for_each(|p| cap(&mut p.attributes)),
                    None => {}
                }
            }
        }
    }
    d
}

/// Profile attributes are interned: a profile or sample lists indices into
/// the dictionary's attribute table, so only the number of indices can be
/// capped (key and value sizes are not checked). A profile's drops are
/// added to its `dropped_attributes_count`; a sample has none.
pub fn cap_profiles(
    request: &mut ExportProfilesServiceRequest,
    limits: &AttributeLimits,
) -> Dropped {
    fn truncate(indices: &mut Vec<i32>, limits: &AttributeLimits) -> u32 {
        let before = indices.len();
        indices.truncate(limits.max_attributes);
        (before - indices.len()) as u32
    }
    let mut d = Dropped::default();
    for rp in &mut request.resource_profiles {
        cap_resource(&mut rp.resource, &mut d, limits);
        for sp in &mut rp.scope_profiles {
            cap_scope(&mut sp.scope, &mut d, limits);
            for profile in &mut sp.profiles {
                let n = truncate(&mut profile.attribute_indices, limits);
                profile.dropped_attributes_count =
                    profile.dropped_attributes_count.saturating_add(n);
                d.record += u64::from(n);
                for sample in &mut profile.samples {
                    d.record += u64::from(truncate(&mut sample.attribute_indices, limits));
                }
            }
        }
    }
    d
}

/// Counts `dropped` in `signaldb.ingest.attributes_dropped`, one data
/// point per level. `_system` traffic is not measured (anti-loop guard).
pub fn record_drops(tenant_id: &str, signal: &'static str, dropped: &Dropped) {
    if dropped.total() == 0 || !common::self_monitoring::should_count_tenant(tenant_id) {
        return;
    }
    let counter = &common::self_monitoring::app_metrics().ingest_attributes_dropped;
    for (level, n) in [
        ("resource", dropped.resource),
        ("scope", dropped.scope),
        ("record", dropped.record),
    ] {
        if n > 0 {
            counter.add(
                n,
                &[
                    opentelemetry::KeyValue::new("tenant_id", tenant_id.to_string()),
                    opentelemetry::KeyValue::new("signal", signal),
                    opentelemetry::KeyValue::new("level", level),
                ],
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::collector::profiles::v1development::ExportProfilesServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::metrics::v1::{
        ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram, HistogramDataPoint,
        Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint,
    };
    use opentelemetry_proto::tonic::profiles::v1development::{
        Profile, ResourceProfiles, Sample, ScopeProfiles,
    };
    use opentelemetry_proto::tonic::trace::v1::{
        ResourceSpans, ScopeSpans, Span, span::Event, span::Link,
    };

    fn kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(value.to_string())),
            }),
            ..Default::default()
        }
    }

    fn kvs(n: usize) -> Vec<KeyValue> {
        (0..n).map(|i| kv(&format!("k{i}"), "v")).collect()
    }

    fn limits(max_attributes: usize) -> AttributeLimits {
        AttributeLimits {
            max_attributes,
            ..AttributeLimits::default()
        }
    }

    fn keys(attrs: &[KeyValue]) -> Vec<&str> {
        attrs.iter().map(|a| a.key.as_str()).collect()
    }

    #[test]
    fn cap_attrs_keeps_sender_order_and_truncates() {
        let mut attrs = kvs(5);
        assert_eq!(cap_attrs(&mut attrs, &limits(2)), 3);
        assert_eq!(keys(&attrs), ["k0", "k1"]);
    }

    #[test]
    fn cap_attrs_drops_long_keys_and_values_but_keeps_the_rest() {
        let limits = AttributeLimits {
            max_attributes: 10,
            max_key_bytes: 4,
            max_value_bytes: 4,
        };
        let array = KeyValue {
            key: "arr".into(),
            value: Some(AnyValue {
                value: Some(Value::ArrayValue(ArrayValue {
                    values: vec![AnyValue {
                        value: Some(Value::StringValue("abcdefgh".into())),
                    }],
                })),
            }),
            ..Default::default()
        };
        let mut attrs = vec![
            kv("ok", "fine"),
            kv("toolong", "v"),
            kv("k", "too long a value"),
            array,
            kv("last", "v"),
        ];
        assert_eq!(cap_attrs(&mut attrs, &limits), 3);
        assert_eq!(keys(&attrs), ["ok", "last"]);
    }

    fn span_request(attrs: usize) -> ExportTraceServiceRequest {
        let span = Span {
            attributes: kvs(attrs),
            dropped_attributes_count: 1,
            events: vec![Event {
                attributes: kvs(attrs),
                ..Default::default()
            }],
            links: vec![Link {
                attributes: kvs(attrs),
                dropped_attributes_count: 7,
                ..Default::default()
            }],
            ..Default::default()
        };
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: kvs(attrs),
                    dropped_attributes_count: 2,
                    entity_refs: vec![],
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        attributes: kvs(attrs),
                        ..Default::default()
                    }),
                    spans: vec![span],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    #[test]
    fn traces_over_limit_are_capped_and_counted_on_every_level() {
        let mut request = span_request(5);
        let dropped = cap_traces(&mut request, &limits(2));
        assert_eq!(
            dropped,
            Dropped {
                resource: 3,
                scope: 3,
                record: 9
            }
        );
        let rs = &request.resource_spans[0];
        let resource = rs.resource.as_ref().unwrap();
        assert_eq!(keys(&resource.attributes), ["k0", "k1"]);
        assert_eq!(resource.dropped_attributes_count, 2 + 3);
        let ss = &rs.scope_spans[0];
        assert_eq!(ss.scope.as_ref().unwrap().dropped_attributes_count, 3);
        let span = &ss.spans[0];
        assert_eq!(keys(&span.attributes), ["k0", "k1"]);
        assert_eq!(span.dropped_attributes_count, 1 + 3);
        assert_eq!(span.events[0].attributes.len(), 2);
        assert_eq!(span.events[0].dropped_attributes_count, 3);
        assert_eq!(span.links[0].dropped_attributes_count, 7 + 3);
    }

    #[test]
    fn traces_under_limit_are_unchanged() {
        let mut request = span_request(2);
        let original = request.clone();
        assert_eq!(cap_traces(&mut request, &limits(2)), Dropped::default());
        assert_eq!(request, original);
    }

    #[test]
    fn logs_over_limit_are_capped_and_under_limit_unchanged() {
        let build = |n| ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: None,
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        attributes: kvs(n),
                        dropped_attributes_count: 1,
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let mut over = build(5);
        let dropped = cap_logs(&mut over, &limits(2));
        assert_eq!(dropped.record, 3);
        let record = &over.resource_logs[0].scope_logs[0].log_records[0];
        assert_eq!(keys(&record.attributes), ["k0", "k1"]);
        assert_eq!(record.dropped_attributes_count, 4);

        let mut under = build(2);
        let original = under.clone();
        assert_eq!(cap_logs(&mut under, &limits(2)), Dropped::default());
        assert_eq!(under, original);
    }

    #[test]
    fn metric_data_points_are_capped_for_every_data_variant() {
        let metric = |data| Metric {
            data: Some(data),
            ..Default::default()
        };
        let mut request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![
                        metric(MetricData::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: kvs(3),
                                ..Default::default()
                            }],
                        })),
                        metric(MetricData::Sum(Sum {
                            data_points: vec![NumberDataPoint {
                                attributes: kvs(3),
                                ..Default::default()
                            }],
                            ..Default::default()
                        })),
                        metric(MetricData::Histogram(Histogram {
                            data_points: vec![HistogramDataPoint {
                                attributes: kvs(3),
                                ..Default::default()
                            }],
                            ..Default::default()
                        })),
                        metric(MetricData::ExponentialHistogram(ExponentialHistogram {
                            data_points: vec![ExponentialHistogramDataPoint {
                                attributes: kvs(3),
                                ..Default::default()
                            }],
                            ..Default::default()
                        })),
                        metric(MetricData::Summary(Summary {
                            data_points: vec![SummaryDataPoint {
                                attributes: kvs(3),
                                ..Default::default()
                            }],
                        })),
                    ],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let dropped = cap_metrics(&mut request, &limits(1));
        assert_eq!(dropped.record, 10);
        let metrics = &request.resource_metrics[0].scope_metrics[0].metrics;
        let lens: Vec<usize> = metrics
            .iter()
            .map(|m| match m.data.as_ref().unwrap() {
                MetricData::Gauge(g) => g.data_points[0].attributes.len(),
                MetricData::Sum(s) => s.data_points[0].attributes.len(),
                MetricData::Histogram(h) => h.data_points[0].attributes.len(),
                MetricData::ExponentialHistogram(e) => e.data_points[0].attributes.len(),
                MetricData::Summary(s) => s.data_points[0].attributes.len(),
            })
            .collect();
        assert_eq!(lens, [1; 5]);

        let original = request.clone();
        assert_eq!(cap_metrics(&mut request, &limits(1)), Dropped::default());
        assert_eq!(request, original);
    }

    #[test]
    fn profile_attribute_indices_are_truncated_and_counted() {
        let mut request = ExportProfilesServiceRequest {
            resource_profiles: vec![ResourceProfiles {
                resource: None,
                scope_profiles: vec![ScopeProfiles {
                    scope: None,
                    profiles: vec![Profile {
                        attribute_indices: vec![4, 3, 2, 1],
                        dropped_attributes_count: 5,
                        samples: vec![Sample {
                            attribute_indices: vec![9, 8, 7],
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
            dictionary: None,
        };
        let dropped = cap_profiles(&mut request, &limits(2));
        assert_eq!(dropped.record, 3);
        let profile = &request.resource_profiles[0].scope_profiles[0].profiles[0];
        assert_eq!(profile.attribute_indices, [4, 3]);
        assert_eq!(profile.dropped_attributes_count, 7);
        assert_eq!(profile.samples[0].attribute_indices, [9, 8]);

        let original = request.clone();
        assert_eq!(cap_profiles(&mut request, &limits(2)), Dropped::default());
        assert_eq!(request, original);
    }
}

#[cfg(test)]
mod tenant_limits_tests {
    use common::config::{TenantConfig, TenantLimits};

    use super::*;

    fn limits(max_attributes: usize) -> AttributeLimits {
        AttributeLimits {
            max_attributes,
            ..AttributeLimits::default()
        }
    }

    fn tenant(id: &str, attribute_limits: Option<AttributeLimits>) -> TenantConfig {
        TenantConfig {
            id: id.to_string(),
            limits: Some(TenantLimits {
                attribute_limits,
                ..TenantLimits::default()
            }),
            ..TenantConfig::default()
        }
    }

    fn acceptor(max_attributes: usize) -> AcceptorConfig {
        AcceptorConfig {
            attribute_limits: limits(max_attributes),
            ..AcceptorConfig::default()
        }
    }

    #[test]
    fn tenant_override_wins_over_default_limits_and_acceptor() {
        let auth = AuthConfig {
            default_limits: TenantLimits {
                attribute_limits: Some(limits(20)),
                ..TenantLimits::default()
            },
            tenants: vec![tenant("acme", Some(limits(5)))],
            ..AuthConfig::default()
        };
        let resolved = TenantAttributeLimits::new(&acceptor(30), &auth);
        assert_eq!(resolved.for_tenant("acme").max_attributes, 5);
    }

    #[test]
    fn default_limits_apply_before_the_acceptor_config() {
        let auth = AuthConfig {
            default_limits: TenantLimits {
                attribute_limits: Some(limits(20)),
                ..TenantLimits::default()
            },
            tenants: vec![tenant("acme", None)],
            ..AuthConfig::default()
        };
        let resolved = TenantAttributeLimits::new(&acceptor(30), &auth);
        assert_eq!(resolved.for_tenant("acme").max_attributes, 20);
        assert_eq!(resolved.for_tenant("other").max_attributes, 20);
    }

    #[test]
    fn acceptor_config_is_the_last_resort_for_known_and_unknown_tenants() {
        let auth = AuthConfig {
            tenants: vec![tenant("acme", None)],
            ..AuthConfig::default()
        };
        let resolved = TenantAttributeLimits::new(&acceptor(30), &auth);
        assert_eq!(resolved.for_tenant("acme").max_attributes, 30);
        assert_eq!(resolved.for_tenant("unknown").max_attributes, 30);
    }

    #[test]
    fn uniform_applies_to_every_tenant() {
        let resolved = TenantAttributeLimits::uniform(limits(7));
        assert_eq!(resolved.for_tenant("anyone").max_attributes, 7);
    }
}
