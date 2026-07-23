//! # Tempo gRPC Querier Protocol
//!
//! Implements Tempo's internal `tempopb.Querier` gRPC service backed by
//! [`TraceService`], allowing SignalDB to answer trace-by-id and search
//! requests from a Tempo query-frontend.
//!
//! Tenant resolution mirrors the Flight service: an authenticated
//! `TenantContext` request extension wins; otherwise the Tempo-conventional
//! `X-Scope-OrgID` header selects the tenant (with the `default` dataset);
//! otherwise `default`/`default`.
//!
//! `SearchBlock` is answered with `Unimplemented`: SignalDB stores data in
//! Iceberg tables, not Tempo blocks, so block-scoped search has no meaning
//! here. Tag endpoints return the statically queryable tag set (the same
//! set the HTTP API advertises) rather than fabricated values.

use std::collections::HashMap;

use tempo_api::tempopb::{
    SearchBlockRequest, SearchRequest, SearchResponse, SearchTagValuesRequest,
    SearchTagValuesResponse, SearchTagValuesV2Response, SearchTagsRequest, SearchTagsResponse,
    SearchTagsV2Response, SearchTagsV2Scope, SpanSet, TraceByIdRequest, TraceByIdResponse,
    TraceSearchMetadata, common as otlp_common, querier_server::Querier, resource as otlp_resource,
    trace as otlp_trace, trace_by_id_response::Status as TraceByIdStatus,
};
use tonic::{Request, Response, Status};

use crate::query::error::QuerierError;
use crate::query::trace::TraceService;
use crate::query::{FindTraceByIdParams, SearchQueryParams};
use common::model::span::{Span as ModelSpan, SpanKind, SpanStatus};

// Keep in sync with the router's HTTP tag endpoints: these are the tags
// search can actually filter on today.
const RESOURCE_TAGS: &[&str] = &["service.name"];
const INTRINSIC_TAGS: &[&str] = &["name", "status"];

/// Tempo gRPC querier backed by SignalDB's trace query service.
#[derive(Debug, Clone)]
pub struct SignalDBQuerier {
    trace_service: TraceService,
}

impl SignalDBQuerier {
    pub fn new(trace_service: TraceService) -> Self {
        Self { trace_service }
    }

    /// Resolve (tenant_slug, dataset_slug) for a request. An authenticated
    /// `TenantContext` extension (inserted by the Flight auth interceptor)
    /// takes precedence over Tempo's `X-Scope-OrgID` header.
    fn resolve_tenant<T>(request: &Request<T>) -> (String, String) {
        if let Some(ctx) = request.extensions().get::<common::auth::TenantContext>() {
            return (ctx.tenant_slug.clone(), ctx.dataset_slug.clone());
        }
        let tenant = request
            .metadata()
            .get("x-scope-orgid")
            .and_then(|v| v.to_str().ok())
            .filter(|s| !s.is_empty())
            .unwrap_or("default")
            .to_string();
        (tenant, "default".to_string())
    }
}

fn querier_error_to_status(e: QuerierError) -> Status {
    match e {
        QuerierError::TraceNotFound => Status::not_found(e.to_string()),
        QuerierError::InvalidInput(msg) => Status::invalid_argument(msg),
        QuerierError::Unsupported(msg) => Status::unimplemented(msg),
        other => Status::internal(format!("Query failed: {other:?}")),
    }
}

/// Collect a trace's spans depth-first, flattening the child hierarchy.
fn collect_spans<'a>(spans: &'a [ModelSpan], out: &mut Vec<&'a ModelSpan>) {
    for span in spans {
        out.push(span);
        collect_spans(&span.children, out);
    }
}

fn json_to_any_value(value: &serde_json::Value) -> otlp_common::v1::AnyValue {
    use otlp_common::v1::any_value::Value;
    let inner = match value {
        serde_json::Value::String(s) => Value::StringValue(s.clone()),
        serde_json::Value::Bool(b) => Value::BoolValue(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::IntValue(i)
            } else if let Some(f) = n.as_f64() {
                Value::DoubleValue(f)
            } else {
                Value::StringValue(n.to_string())
            }
        }
        other => Value::StringValue(other.to_string()),
    };
    otlp_common::v1::AnyValue { value: Some(inner) }
}

fn attrs_to_key_values(
    attrs: &HashMap<String, serde_json::Value>,
) -> Vec<otlp_common::v1::KeyValue> {
    let mut kvs: Vec<otlp_common::v1::KeyValue> = attrs
        .iter()
        .map(|(key, value)| otlp_common::v1::KeyValue {
            key: key.clone(),
            value: Some(json_to_any_value(value)),
        })
        .collect();
    // Deterministic output for consumers and tests
    kvs.sort_by(|a, b| a.key.cmp(&b.key));
    kvs
}

fn span_kind_to_proto(kind: &SpanKind) -> i32 {
    use otlp_trace::v1::span::SpanKind as ProtoKind;
    let kind = match kind {
        SpanKind::Internal => ProtoKind::Internal,
        SpanKind::Server => ProtoKind::Server,
        SpanKind::Client => ProtoKind::Client,
        SpanKind::Producer => ProtoKind::Producer,
        SpanKind::Consumer => ProtoKind::Consumer,
    };
    kind as i32
}

fn span_status_to_proto(status: &SpanStatus) -> otlp_trace::v1::Status {
    use otlp_trace::v1::status::StatusCode;
    let code = match status {
        SpanStatus::Unspecified => StatusCode::Unset,
        SpanStatus::Ok => StatusCode::Ok,
        SpanStatus::Error => StatusCode::Error,
    };
    otlp_trace::v1::Status {
        message: String::new(),
        code: code as i32,
    }
}

/// Convert the internal trace model to the OTLP-shaped `tempopb.Trace`,
/// grouping spans into one `ResourceSpans` per service.
fn model_trace_to_proto(trace: &common::model::trace::Trace) -> tempo_api::tempopb::Trace {
    let mut all_spans = Vec::new();
    collect_spans(&trace.spans, &mut all_spans);

    let mut by_service: HashMap<&str, Vec<&ModelSpan>> = HashMap::new();
    for span in all_spans {
        by_service.entry(&span.service_name).or_default().push(span);
    }

    let mut services: Vec<&str> = by_service.keys().copied().collect();
    services.sort_unstable();

    let resource_spans = services
        .into_iter()
        .map(|service_name| {
            let spans = &by_service[service_name];
            let proto_spans = spans
                .iter()
                .map(|span| otlp_trace::v1::Span {
                    trace_id: hex::decode(&span.trace_id).unwrap_or_default(),
                    span_id: hex::decode(&span.span_id).unwrap_or_default(),
                    parent_span_id: hex::decode(&span.parent_span_id).unwrap_or_default(),
                    name: span.name.clone(),
                    kind: span_kind_to_proto(&span.span_kind),
                    start_time_unix_nano: span.start_time_unix_nano,
                    end_time_unix_nano: span
                        .start_time_unix_nano
                        .saturating_add(span.duration_nano),
                    attributes: attrs_to_key_values(&span.attributes),
                    status: Some(span_status_to_proto(&span.status)),
                    ..Default::default()
                })
                .collect();

            let mut resource_attrs = spans
                .first()
                .map(|s| attrs_to_key_values(&s.resource))
                .unwrap_or_default();
            if !resource_attrs.iter().any(|kv| kv.key == "service.name") {
                resource_attrs.push(otlp_common::v1::KeyValue {
                    key: "service.name".to_string(),
                    value: Some(otlp_common::v1::AnyValue {
                        value: Some(otlp_common::v1::any_value::Value::StringValue(
                            service_name.to_string(),
                        )),
                    }),
                });
            }

            otlp_trace::v1::ResourceSpans {
                resource: Some(otlp_resource::v1::Resource {
                    attributes: resource_attrs,
                    dropped_attributes_count: 0,
                }),
                scope_spans: vec![otlp_trace::v1::ScopeSpans {
                    scope: None,
                    spans: proto_spans,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }
        })
        .collect();

    tempo_api::tempopb::Trace { resource_spans }
}

/// Convert a trace to Tempo search metadata, capping the span set at
/// `span_cap` spans (`matched` always reports the full count).
fn model_trace_to_search_metadata(
    trace: &common::model::trace::Trace,
    span_cap: Option<usize>,
) -> TraceSearchMetadata {
    let mut all_spans = Vec::new();
    collect_spans(&trace.spans, &mut all_spans);

    let mut earliest_start = u64::MAX;
    let mut latest_end = 0u64;
    let mut root_service_name = "unknown".to_string();
    let mut root_trace_name = "unknown".to_string();
    for span in &all_spans {
        if span.is_root {
            root_service_name = span.service_name.clone();
            root_trace_name = span.name.clone();
        }
        earliest_start = earliest_start.min(span.start_time_unix_nano);
        latest_end = latest_end.max(span.start_time_unix_nano.saturating_add(span.duration_nano));
    }
    let duration_ms = if earliest_start != u64::MAX && latest_end > earliest_start {
        ((latest_end - earliest_start) / 1_000_000) as u32
    } else {
        0
    };

    let spans = all_spans
        .iter()
        .take(span_cap.unwrap_or(usize::MAX))
        .map(|span| tempo_api::tempopb::Span {
            span_id: span.span_id.clone(),
            name: span.name.clone(),
            start_time_unix_nano: span.start_time_unix_nano,
            duration_nanos: span.duration_nano,
            attributes: attrs_to_key_values(&span.attributes),
        })
        .collect();

    TraceSearchMetadata {
        trace_id: trace.trace_id.clone(),
        root_service_name,
        root_trace_name,
        start_time_unix_nano: if earliest_start == u64::MAX {
            0
        } else {
            earliest_start
        },
        duration_ms,
        span_set: None,
        span_sets: vec![SpanSet {
            spans,
            matched: all_spans.len() as u32,
            attributes: Vec::new(),
        }],
        service_stats: HashMap::new(),
    }
}

/// Map a Tempo `SearchRequest` onto SignalDB search parameters.
fn search_request_to_params(req: &SearchRequest) -> SearchQueryParams {
    let tags = if req.tags.is_empty() {
        None
    } else {
        let mut pairs: Vec<String> = req.tags.iter().map(|(k, v)| format!("{k}={v}")).collect();
        pairs.sort_unstable();
        Some(pairs.join(" "))
    };
    SearchQueryParams {
        q: (!req.query.is_empty()).then(|| req.query.clone()),
        tags,
        min_duration: (req.min_duration_ms > 0)
            .then(|| i64::from(req.min_duration_ms).saturating_mul(1_000_000)),
        max_duration: (req.max_duration_ms > 0)
            .then(|| i64::from(req.max_duration_ms).saturating_mul(1_000_000)),
        limit: (req.limit > 0).then(|| i32::try_from(req.limit).unwrap_or(i32::MAX)),
        start: (req.start > 0).then_some(i64::from(req.start)),
        end: (req.end > 0).then_some(i64::from(req.end)),
    }
}

#[tonic::async_trait]
impl Querier for SignalDBQuerier {
    #[tracing::instrument(skip_all)]
    async fn find_trace_by_id(
        &self,
        request: tonic::Request<TraceByIdRequest>,
    ) -> Result<tonic::Response<TraceByIdResponse>, tonic::Status> {
        let (tenant_slug, dataset_slug) = Self::resolve_tenant(&request);
        let req = request.into_inner();
        let trace_id = hex::encode(&req.trace_id);
        tracing::info!(
            trace_id = %trace_id,
            tenant_slug = %tenant_slug,
            dataset_slug = %dataset_slug,
            "Tempo gRPC trace lookup"
        );

        let params = FindTraceByIdParams {
            trace_id,
            start: None,
            end: None,
        };
        let trace = self
            .trace_service
            .find_by_id_with_tenant(params, &tenant_slug, &dataset_slug)
            .await
            .map_err(querier_error_to_status)?;

        // Tempo's protocol reports absence as a complete response without a
        // trace; the query-frontend merges partial responses across queriers.
        let response = TraceByIdResponse {
            trace: trace.as_ref().map(model_trace_to_proto),
            metrics: None,
            status: TraceByIdStatus::Complete as i32,
            message: String::new(),
        };
        Ok(Response::new(response))
    }

    #[tracing::instrument(skip_all)]
    async fn search_recent(
        &self,
        request: tonic::Request<SearchRequest>,
    ) -> Result<tonic::Response<SearchResponse>, tonic::Status> {
        let (tenant_slug, dataset_slug) = Self::resolve_tenant(&request);
        let req = request.into_inner();
        let span_cap = usize::try_from(req.spans_per_span_set)
            .ok()
            .filter(|v| *v > 0);
        let params = search_request_to_params(&req);
        tracing::info!(
            tenant_slug = %tenant_slug,
            dataset_slug = %dataset_slug,
            params = ?params,
            "Tempo gRPC trace search"
        );

        let traces = self
            .trace_service
            .find_traces_with_tenant(params, &tenant_slug, &dataset_slug)
            .await
            .map_err(querier_error_to_status)?;

        let response = SearchResponse {
            traces: traces
                .iter()
                .map(|t| model_trace_to_search_metadata(t, span_cap))
                .collect(),
            metrics: None,
        };
        Ok(Response::new(response))
    }

    #[tracing::instrument(skip_all)]
    async fn search_block(
        &self,
        _request: tonic::Request<SearchBlockRequest>,
    ) -> Result<tonic::Response<SearchResponse>, tonic::Status> {
        Err(Status::unimplemented(
            "SignalDB stores data in Iceberg tables, not Tempo blocks; block-scoped search is not supported",
        ))
    }

    #[tracing::instrument(skip_all)]
    async fn search_tags(
        &self,
        _request: tonic::Request<SearchTagsRequest>,
    ) -> Result<tonic::Response<SearchTagsResponse>, tonic::Status> {
        let response = SearchTagsResponse {
            metrics: None,
            tag_names: RESOURCE_TAGS
                .iter()
                .chain(INTRINSIC_TAGS)
                .map(|t| t.to_string())
                .collect(),
        };
        Ok(Response::new(response))
    }

    #[tracing::instrument(skip_all)]
    async fn search_tags_v2(
        &self,
        _request: tonic::Request<SearchTagsRequest>,
    ) -> Result<tonic::Response<SearchTagsV2Response>, tonic::Status> {
        let response = SearchTagsV2Response {
            metrics: None,
            scopes: vec![
                SearchTagsV2Scope {
                    name: "resource".to_string(),
                    tags: RESOURCE_TAGS.iter().map(|t| t.to_string()).collect(),
                },
                SearchTagsV2Scope {
                    name: "intrinsic".to_string(),
                    tags: INTRINSIC_TAGS.iter().map(|t| t.to_string()).collect(),
                },
            ],
        };
        Ok(Response::new(response))
    }

    #[tracing::instrument(skip_all)]
    async fn search_tag_values(
        &self,
        _request: tonic::Request<SearchTagValuesRequest>,
    ) -> Result<tonic::Response<SearchTagValuesResponse>, tonic::Status> {
        // Value enumeration is served by the HTTP API via Flight SQL; here we
        // return an empty set rather than fabricated values.
        Ok(Response::new(SearchTagValuesResponse {
            tag_values: Vec::new(),
            metrics: None,
        }))
    }

    #[tracing::instrument(skip_all)]
    async fn search_tag_values_v2(
        &self,
        _request: tonic::Request<SearchTagValuesRequest>,
    ) -> Result<tonic::Response<SearchTagValuesV2Response>, tonic::Status> {
        Ok(Response::new(SearchTagValuesV2Response {
            tag_values: Vec::new(),
            metrics: None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_span(span_id: &str, service: &str, is_root: bool) -> ModelSpan {
        ModelSpan {
            trace_id: "0af7651916cd43dd8448eb211c80319c".to_string(),
            span_id: span_id.to_string(),
            parent_span_id: String::new(),
            status: SpanStatus::Ok,
            is_root,
            name: format!("op-{span_id}"),
            service_name: service.to_string(),
            span_kind: SpanKind::Server,
            start_time_unix_nano: 1_700_000_000_000_000_000,
            duration_nano: 5_000_000,
            attributes: HashMap::new(),
            resource: HashMap::new(),
            children: Vec::new(),
        }
    }

    fn make_trace(spans: Vec<ModelSpan>) -> common::model::trace::Trace {
        common::model::trace::Trace {
            trace_id: "0af7651916cd43dd8448eb211c80319c".to_string(),
            spans,
        }
    }

    #[test]
    fn trace_converts_to_resource_spans_grouped_by_service() {
        let trace = make_trace(vec![
            make_span("b7ad6b7169203331", "svc-a", true),
            make_span("00f067aa0ba902b7", "svc-b", false),
        ]);

        let proto = model_trace_to_proto(&trace);
        assert_eq!(proto.resource_spans.len(), 2);

        let first = &proto.resource_spans[0];
        let service_attr = &first.resource.as_ref().unwrap().attributes[0];
        assert_eq!(service_attr.key, "service.name");
        let span = &first.scope_spans[0].spans[0];
        assert_eq!(span.span_id, hex::decode("b7ad6b7169203331").unwrap());
        assert_eq!(
            span.end_time_unix_nano,
            1_700_000_000_000_000_000 + 5_000_000
        );
        assert_eq!(span.kind, otlp_trace::v1::span::SpanKind::Server as i32);
        assert_eq!(
            span.status.as_ref().unwrap().code,
            otlp_trace::v1::status::StatusCode::Ok as i32
        );
    }

    #[test]
    fn search_metadata_caps_spans_but_reports_full_match_count() {
        let spans = (0..5)
            .map(|i| make_span(&format!("{i:016x}"), "svc", i == 0))
            .collect();
        let metadata = model_trace_to_search_metadata(&make_trace(spans), Some(2));

        assert_eq!(metadata.span_sets.len(), 1);
        assert_eq!(metadata.span_sets[0].spans.len(), 2);
        assert_eq!(metadata.span_sets[0].matched, 5);
        assert_eq!(metadata.root_service_name, "svc");
        assert_eq!(metadata.duration_ms, 5);
    }

    #[test]
    fn search_request_maps_onto_search_params() {
        let mut tags = HashMap::new();
        tags.insert("service.name".to_string(), "api".to_string());
        let req = SearchRequest {
            tags,
            min_duration_ms: 100,
            max_duration_ms: 5_000,
            limit: 25,
            start: 1_700_000_000,
            end: 1_700_003_600,
            query: String::new(),
            spans_per_span_set: 3,
        };

        let params = search_request_to_params(&req);
        assert_eq!(params.q, None);
        assert_eq!(params.tags.as_deref(), Some("service.name=api"));
        assert_eq!(params.min_duration, Some(100_000_000));
        assert_eq!(params.max_duration, Some(5_000_000_000));
        assert_eq!(params.limit, Some(25));
        assert_eq!(params.start, Some(1_700_000_000));
        assert_eq!(params.end, Some(1_700_003_600));
    }

    #[test]
    fn tenant_resolution_prefers_context_then_header() {
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("x-scope-orgid", "acme".parse().unwrap());
        assert_eq!(
            SignalDBQuerier::resolve_tenant(&request),
            ("acme".to_string(), "default".to_string())
        );

        request
            .extensions_mut()
            .insert(common::auth::TenantContext::new(
                "tenant-x".to_string(),
                "prod".to_string(),
                "tenant-x".to_string(),
                "prod".to_string(),
                None,
                common::auth::TenantSource::Config,
            ));
        assert_eq!(
            SignalDBQuerier::resolve_tenant(&request),
            ("tenant-x".to_string(), "prod".to_string())
        );

        assert_eq!(
            SignalDBQuerier::resolve_tenant(&Request::new(())),
            ("default".to_string(), "default".to_string())
        );
    }
}
