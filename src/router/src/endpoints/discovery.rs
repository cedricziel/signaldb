//! # Query discovery — answered from metadata, not from data
//!
//! Serves the `describe` documents of the native query surface (`POST
//! /api/v1/query` with the `metadata` envelope) and the source listing
//! (`GET /api/v1/query/sources`).
//!
//! These requests never reach a querier. They are assembled here from the
//! canonical logical schema (in process), the tenant's schema registries, and
//! the compactor's `attribute_stats` — one indexed catalog read, the same
//! pattern `/prometheus/api/v1/label_stats` already uses. A field picker
//! therefore stays responsive while query execution is saturated, which is
//! exactly when someone is most likely to be building a query.
//!
//! The one exception is explicit: a `values` request carrying `"sample": true`
//! runs the ordinary Query IR aggregation named in the response's `hint`, and
//! the response says it read data. Without that flag no discovery request
//! reads signal data — see `openspec/changes/query-field-discovery`.

use axum::extract::State;
use common::auth::TenantContext;
use common::auth::TenantContextExtractor;
use common::discovery::{
    DEFAULT_FIELD_LIMIT, DEFAULT_VALUE_LIMIT, DiscoveredSource, DiscoveredValue, DiscoveryCost,
    MetadataKind, MetadataResult, ValueOrigin, intrinsic_values, latest_observation, merge_fields,
    registry_values, signal_for_source,
};
use common::query_ir::{Describe, DescribeTarget, Document, SourceRegistry};
use common::schema::logical::LogicalSchema;
use common::tenant_api::TenantApi;

use super::api_error::ApiError;
use super::query::{QueryIrResponse, QueryWarning, ResolvedWindow, execute_ticket, ir_table};
use crate::RouterState;

/// Answer a `describe` document. The caller has already authorized the source
/// and resolved the window.
pub(super) async fn answer_describe<S: RouterState>(
    state: &S,
    ctx: &TenantContext,
    doc: &Document,
    describe: &Describe,
    window: ResolvedWindow,
    now_ns: i64,
) -> Result<QueryIrResponse, ApiError> {
    let metadata = match describe.target {
        DescribeTarget::Fields => fields(state, ctx, &doc.from, describe).await?,
        DescribeTarget::Values => values(state, ctx, doc, describe, window, now_ns).await?,
    };
    let warnings = warnings_for(&metadata);
    Ok(QueryIrResponse::metadata(window, metadata, warnings))
}

/// The `code` of the warning raised when no statistics back a field answer.
pub(super) const NO_ATTRIBUTE_STATISTICS: &str = "no_attribute_statistics";

/// Warn when a field answer rests on declared schema alone. Without
/// statistics the response is not wrong — every field in it is real and
/// queryable — but it is not the tenant's complete field set either, and a
/// client that presented it as one would mislead its user.
fn warnings_for(metadata: &MetadataResult) -> Vec<QueryWarning> {
    if metadata.kind != MetadataKind::Fields || metadata.cost.as_of.is_some() {
        return Vec::new();
    }
    vec![QueryWarning {
        code: NO_ATTRIBUTE_STATISTICS.to_string(),
        message: "no attribute statistics exist for this tenant and signal yet, so this lists \
                  only the fields the schema declares; attribute keys this tenant emits appear \
                  once the compactor's analyzer has run over the data"
            .to_string(),
        field: None,
        suggestions: Vec::new(),
    }]
}

/// `GET /api/v1/query/sources` — the signal sources this tenant can query.
///
/// A deliberate exception to "a first-party read is a `POST /api/v1/query`
/// document": an IR document must name a `from`, and this is the one question
/// that has no source to name.
#[utoipa::path(
    get,
    path = "/api/v1/query/sources",
    tag = "query",
    operation_id = "query_sources",
    description = "List the signal sources available to the authenticated tenant and dataset, each with whether it is currently queryable. A registered signal whose table exists but holds no data is reported as available and empty, never omitted.\n\nThis is the one discovery call that is a GET rather than a `POST /api/v1/query` document: an IR document must name the source it queries (`from`), and \"which sources exist\" is the question that has no source to name. Every other discovery request — fields and values — is a document with a terminal `describe` stage and the `metadata` envelope, so this exception is bounded to this one route rather than a pattern to copy.\n\nThe answer comes from tenant metadata; it reads no signal data. The response is the same `metadata` envelope the `describe` documents return.",
    security(("bearerAuth" = [])),
    responses(
        (status = 200, description = "Available signal sources", body = QueryIrResponse),
        (status = 401, description = "Unauthorized"),
        (status = 429, response = crate::endpoints::api_error::RateLimited),
        (status = 503, description = "Catalog unavailable"),
    )
)]
#[tracing::instrument(
    skip(state, tenant_ctx),
    fields(
        signaldb.tenant.id = %tenant_ctx.0.tenant_id,
        signaldb.dataset.id = %tenant_ctx.0.dataset_id
    )
)]
pub async fn query_sources<S: RouterState>(
    State(state): State<S>,
    tenant_ctx: TenantContextExtractor,
) -> Result<axum::Json<QueryIrResponse>, ApiError> {
    let ctx = &tenant_ctx.0;
    let tables = tenant_tables(&state, ctx).await?;
    let registry = SourceRegistry::core();
    let sources: Vec<DiscoveredSource> = registry
        .names()
        .into_iter()
        .map(|name| DiscoveredSource {
            available: source_tables(&name)
                .iter()
                .any(|table| tables.iter().any(|t| t == table)),
            name,
        })
        .collect();

    let window = ResolvedWindow {
        start_ns: 0,
        end_ns: 0,
    };
    Ok(axum::Json(QueryIrResponse::metadata(
        window,
        MetadataResult {
            kind: MetadataKind::Sources,
            sources,
            fields: Vec::new(),
            values: Vec::new(),
            truncated: false,
            cost: DiscoveryCost::metadata(None),
            hint: None,
        },
        Vec::new(),
    )))
}

/// The physical tables that make a source queryable. A source with none of
/// them is reported unavailable; a source whose table exists but holds no data
/// is available and empty, never omitted.
fn source_tables(source: &str) -> &'static [&'static str] {
    match source {
        "logs" => &["logs"],
        "traces" => &["traces"],
        "profiles" => &["profiles"],
        "metrics" => &["metrics_gauge", "metrics_sum"],
        "metrics_histogram" => &["metrics_histogram"],
        _ => &[],
    }
}

/// The tenant's table names in the authenticated dataset.
async fn tenant_tables<S: RouterState>(
    state: &S,
    ctx: &TenantContext,
) -> Result<Vec<String>, ApiError> {
    let mut api = TenantApi::new(state.config().clone())
        .with_tenant_source(std::sync::Arc::new(state.catalog().clone()));
    let listing = api.list_tables(&ctx.tenant_id).await.map_err(|error| {
        tracing::error!(?error, "failed to list tenant tables for source discovery");
        ApiError::new(
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "failed to list tenant tables",
        )
    })?;
    Ok(listing
        .tables
        .into_iter()
        .filter(|table| table.dataset == ctx.dataset_slug || table.dataset == ctx.dataset_id)
        .map(|table| table.name)
        .collect())
}

/// The queryable fields of a source: declared schema, enriched by the tenant's
/// registries, plus the attribute keys the statistics observed.
async fn fields<S: RouterState>(
    state: &S,
    ctx: &TenantContext,
    source: &str,
    describe: &Describe,
) -> Result<MetadataResult, ApiError> {
    let signal = signal_for_source(source)
        .ok_or_else(|| ApiError::bad_request(format!("unknown query source '{source}'")))?;
    let stats = state
        .catalog()
        .get_attribute_stats(&ctx.tenant_slug, &ctx.dataset_slug, signal)
        .await
        .map_err(|error| {
            tracing::error!(?error, "failed to read attribute stats for discovery");
            ApiError::new(
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "failed to read attribute statistics",
            )
        })?;

    let schema = LogicalSchema::core();
    let keys: Vec<String> = schema
        .fields()
        .filter(|field| field.id.source == source)
        .map(|field| field.id.name.clone())
        .chain(stats.iter().map(|record| record.attr_key.clone()))
        .collect();
    let registry = state
        .schema_resolver()
        .resolve_attributes(&ctx.tenant_id, keys)
        .await
        .unwrap_or_else(|error| {
            // The registries only enrich: without them the fields are still
            // correct, just untyped and undescribed. Degrade, do not fail.
            tracing::warn!(?error, "schema registry unavailable for discovery");
            Default::default()
        });

    let limit = bounded(describe.limit, DEFAULT_FIELD_LIMIT);
    let (fields, truncated) = merge_fields(source, &schema, &stats, &registry, limit);
    Ok(MetadataResult {
        kind: MetadataKind::Fields,
        sources: Vec::new(),
        fields,
        values: Vec::new(),
        truncated,
        cost: DiscoveryCost::metadata(latest_observation(&stats)),
        hint: None,
    })
}

/// Value suggestions for one field, in tier order: a declared value set, then
/// — only when the client explicitly opted in — the ordinary IR aggregation
/// over the window. Otherwise no values, and the reason why.
async fn values<S: RouterState>(
    state: &S,
    ctx: &TenantContext,
    doc: &Document,
    describe: &Describe,
    window: ResolvedWindow,
    now_ns: i64,
) -> Result<MetadataResult, ApiError> {
    let source = doc.from.as_str();
    let field = describe
        .field
        .as_deref()
        .ok_or_else(|| ApiError::bad_request("describe target `values` requires a `field`"))?;
    let limit = bounded(describe.limit, DEFAULT_VALUE_LIMIT);

    // 1. A value set SignalDB itself writes, or one a registry declares:
    // exact, and free.
    let declared = match intrinsic_values(source, field) {
        Some(values) => Some(values),
        None => state
            .schema_resolver()
            .resolve_attribute(&ctx.tenant_id, field)
            .await
            .ok()
            .and_then(|resolution| resolution.primary)
            .as_ref()
            .and_then(registry_values),
    };
    if let Some(mut values) = declared {
        let truncated = values.len() > limit;
        values.truncate(limit);
        return Ok(MetadataResult {
            kind: MetadataKind::Values,
            sources: Vec::new(),
            fields: Vec::new(),
            values,
            truncated,
            cost: DiscoveryCost::metadata(None),
            hint: None,
        });
    }

    // 2. Nothing declares this field's values, and no maintained value
    // statistics exist yet (see the sketch task in the change). Say so, and
    // name the query that computes the answer — never scan behind the
    // client's back.
    let hint = value_query_hint(source, field, window, limit);
    if !describe.sample {
        return Ok(MetadataResult {
            kind: MetadataKind::Values,
            sources: Vec::new(),
            fields: Vec::new(),
            values: Vec::new(),
            truncated: false,
            cost: DiscoveryCost::none(),
            hint: Some(hint),
        });
    }

    // 3. The client asked for the data-derived answer: run exactly the query
    // the hint names, bounded by the window and the limit, and say that the
    // answer came from reading data.
    let values = sampled_values(state, ctx, source, field, window, limit, now_ns).await?;
    let truncated = values.len() >= limit;
    Ok(MetadataResult {
        kind: MetadataKind::Values,
        sources: Vec::new(),
        fields: Vec::new(),
        values,
        truncated,
        cost: DiscoveryCost::sampled_scan(),
        hint: Some(hint),
    })
}

/// Run the counted top-values aggregation over the window, through the same
/// IR path as any other query.
async fn sampled_values<S: RouterState>(
    state: &S,
    ctx: &TenantContext,
    source: &str,
    field: &str,
    window: ResolvedWindow,
    limit: usize,
    now_ns: i64,
) -> Result<Vec<DiscoveredValue>, ApiError> {
    let document = value_query_document(source, field, window, limit);
    let payload = serde_json::json!({ "document": document, "now_ns": now_ns });
    let ticket = format!(
        "query_ir:{}:{}:{payload}",
        ctx.tenant_slug, ctx.dataset_slug
    );
    let batches = execute_ticket(state, ticket).await?;
    let (columns, rows) = ir_table(&batches);

    let key = common::query_ir::safe_ident(field);
    let value_index = columns.iter().position(|column| column.name == key);
    let count_index = columns
        .iter()
        .position(|column| column.name == COUNT_OUTPUT);
    let Some(value_index) = value_index else {
        return Ok(Vec::new());
    };
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            let value = match row.get(value_index) {
                Some(serde_json::Value::String(s)) => s.clone(),
                Some(serde_json::Value::Null) | None => return None,
                Some(other) => other.to_string(),
            };
            Some(DiscoveredValue {
                value,
                count: count_index
                    .and_then(|index| row.get(index))
                    .and_then(serde_json::Value::as_i64),
                origin: ValueOrigin::Sampled,
            })
        })
        .collect())
}

/// The aggregate output name the value query declares.
const COUNT_OUTPUT: &str = "count";

/// The IR document that computes a field's top values by reading data.
fn value_query_document(
    source: &str,
    field: &str,
    window: ResolvedWindow,
    limit: usize,
) -> serde_json::Value {
    serde_json::json!({
        "irVersion": common::query_ir::MAX_IR_VERSION,
        "from": source,
        "range": { "from": window.start_ns.to_string(), "to": window.end_ns.to_string() },
        "result": "table",
        "pipeline": [
            { "aggregate": { "by": [field], "aggs": [{ "fn": "count", "as": COUNT_OUTPUT }] } },
            { "topk": { "of": COUNT_OUTPUT, "n": limit } }
        ]
    })
}

/// The same document as text, for the response's `hint`: what was run, or —
/// when nothing covers the field — what the client can run itself.
fn value_query_hint(source: &str, field: &str, window: ResolvedWindow, limit: usize) -> String {
    format!(
        "no declared value set or maintained statistics cover '{field}'. \
         Reading data answers it: POST /api/v1/query {}",
        value_query_document(source, field, window, limit)
    )
}

/// Clamp a client-supplied limit to the server's cap.
fn bounded(requested: Option<u64>, cap: usize) -> usize {
    requested
        .map(|value| (value as usize).min(cap))
        .unwrap_or(cap)
        .max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RouterAppState;
    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
    };
    use common::auth::TenantSource;
    use common::catalog::Catalog;
    use common::config::Configuration;
    use tower::ServiceExt;

    /// A router serving the discovery surface with `ctx` as the authenticated
    /// principal, over an empty in-memory catalog.
    ///
    /// Its service registry holds **no querier**, which is the point: any
    /// request that builds a Flight ticket fails with `503`. A `200` from these
    /// tests is therefore positive evidence that discovery answered from
    /// metadata alone, and a `403` is evidence that authorization was decided
    /// before any ticket was attempted rather than after the work was done.
    async fn app_with(catalog: Catalog, ctx: TenantContext) -> Router {
        let state = RouterAppState::new(catalog, Configuration::default());
        super::super::query::router()
            .with_state(state)
            .layer(axum::middleware::from_fn(
                move |mut req: Request<Body>, next: axum::middleware::Next| {
                    let ctx = ctx.clone();
                    async move {
                        req.extensions_mut().insert(ctx);
                        next.run(req).await
                    }
                },
            ))
    }

    fn ctx_for(tenant: &str, scopes: Option<Vec<String>>) -> TenantContext {
        TenantContext::new(
            tenant.to_string(),
            "default".to_string(),
            tenant.to_string(),
            "default".to_string(),
            Some("test".to_string()),
            TenantSource::Config,
        )
        .with_api_key_restrictions(scopes, None)
    }

    fn describe(source: &str, stage: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "irVersion": 4, "from": source,
            "range": { "from": "now-1h", "to": "now" },
            "result": "metadata",
            "pipeline": [ { "describe": stage } ]
        })
    }

    async fn post(app: &Router, doc: serde_json::Value) -> (StatusCode, serde_json::Value) {
        let request = Request::builder()
            .method("POST")
            .uri("/query")
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&doc).unwrap()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null),
        )
    }

    #[tokio::test]
    async fn fields_are_answered_without_a_querier() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let app = app_with(catalog, ctx_for("acme", None)).await;

        let (status, body) = post(
            &app,
            describe("logs", serde_json::json!({"target": "fields"})),
        )
        .await;

        // No querier is registered: a Flight ticket would have produced 503.
        assert_eq!(status, StatusCode::OK, "body: {body}");
        assert_eq!(body["result"], "metadata");
        assert_eq!(body["metadata"]["kind"], "fields");
        let names: Vec<&str> = body["metadata"]["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"service.name"), "got {names:?}");
        assert_eq!(body["metadata"]["cost"]["mode"], "metadata");
        assert_eq!(
            body["metadata"]["cost"]["window_scoped"], false,
            "statistics carry no time dimension, so the range narrowed nothing"
        );
    }

    #[tokio::test]
    async fn missing_statistics_are_reported_not_hidden() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let app = app_with(catalog, ctx_for("acme", None)).await;

        let (status, body) = post(
            &app,
            describe("logs", serde_json::json!({"target": "fields"})),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert!(
            body["metadata"]["cost"]["as_of"].is_null(),
            "no statistics exist, so the answer has no age to report"
        );
        assert!(
            !body["metadata"]["fields"].as_array().unwrap().is_empty(),
            "the declared fields are still returned"
        );
        let codes: Vec<&str> = body["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .map(|w| w["code"].as_str().unwrap())
            .collect();
        assert!(
            codes.contains(&NO_ATTRIBUTE_STATISTICS),
            "the client must be told this is the declared set, not the tenant's whole field set: {codes:?}"
        );
    }

    #[tokio::test]
    async fn discovery_sees_only_the_authenticated_tenant() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        catalog
            .upsert_attribute_scan_stats("acme", "default", "logs", "acme.only", 5, 10, 3, false)
            .await
            .unwrap();
        catalog
            .upsert_attribute_scan_stats("other", "default", "logs", "other.only", 5, 10, 3, false)
            .await
            .unwrap();
        let app = app_with(catalog, ctx_for("acme", None)).await;

        let (status, body) = post(
            &app,
            describe("logs", serde_json::json!({"target": "fields"})),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        let names: Vec<&str> = body["metadata"]["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"acme.only"), "got {names:?}");
        assert!(
            !names.contains(&"other.only"),
            "another tenant's observed keys must never appear: {names:?}"
        );
    }

    #[tokio::test]
    async fn an_uncovered_field_reads_nothing_and_names_the_query_that_would() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let app = app_with(catalog, ctx_for("acme", None)).await;

        let (status, body) = post(
            &app,
            describe(
                "logs",
                serde_json::json!({"target": "values", "field": "http.route"}),
            ),
        )
        .await;

        // Again: 200 rather than 503 proves no ticket was built.
        assert_eq!(status, StatusCode::OK, "body: {body}");
        assert!(
            body["metadata"]["values"]
                .as_array()
                .is_none_or(|v| v.is_empty()),
            "nothing covers this field, so there is nothing honest to suggest"
        );
        assert_eq!(body["metadata"]["cost"]["mode"], "none");
        let hint = body["metadata"]["hint"].as_str().unwrap_or_default();
        assert!(
            hint.contains("aggregate") && hint.contains("topk") && hint.contains("http.route"),
            "the client must be told what would answer it: {hint}"
        );
    }

    #[tokio::test]
    async fn a_declared_value_set_is_answered_exactly_and_for_free() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let app = app_with(catalog, ctx_for("acme", None)).await;

        let (status, body) = post(
            &app,
            describe(
                "traces",
                serde_json::json!({"target": "values", "field": "span.kind"}),
            ),
        )
        .await;

        assert_eq!(status, StatusCode::OK, "body: {body}");
        let values: Vec<&str> = body["metadata"]["values"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["value"].as_str().unwrap())
            .collect();
        assert!(values.contains(&"Server"), "got {values:?}");
        assert_eq!(body["metadata"]["cost"]["mode"], "metadata");
    }

    #[tokio::test]
    async fn the_sampled_path_is_refused_before_it_can_read_anything() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        // A key scoped to logs only, asking to sample trace values.
        let ctx = ctx_for("acme", Some(vec!["logs:read".to_string()]));
        let app = app_with(catalog, ctx).await;

        let (status, body) = post(
            &app,
            describe(
                "traces",
                serde_json::json!({"target": "values", "field": "http.route", "sample": true}),
            ),
        )
        .await;

        // The distinction that matters: 403 (refused at the authorization
        // boundary) rather than 503 (refused only after trying to reach a
        // querier). A 503 here would mean the request was authorized, did the
        // work, and failed for an unrelated reason.
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "the missing scope must be decided before any read is attempted: {body}"
        );
    }

    #[tokio::test]
    async fn a_predicate_before_describe_is_rejected_with_the_query_that_answers_it() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let app = app_with(catalog, ctx_for("acme", None)).await;

        let (status, body) = post(
            &app,
            serde_json::json!({
                "irVersion": 4, "from": "logs",
                "range": { "from": "now-1h", "to": "now" },
                "result": "metadata",
                "pipeline": [
                    { "where": { "field": "service.name", "op": "eq", "value": "checkout" } },
                    { "describe": { "target": "fields" } }
                ]
            }),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        let message = body.to_string();
        assert!(
            message.contains("aggregate") && message.contains("topk"),
            "the rejection must name the query that computes the scoped answer: {message}"
        );
    }

    #[test]
    fn a_limit_is_clamped_to_the_cap_and_never_zero() {
        assert_eq!(bounded(None, 200), 200);
        assert_eq!(bounded(Some(10), 200), 10);
        assert_eq!(bounded(Some(10_000), 200), 200);
    }

    #[test]
    fn every_registered_source_declares_its_tables() {
        for name in SourceRegistry::core().names() {
            assert!(
                !source_tables(&name).is_empty(),
                "source {name} has no table mapping, so it would always look unavailable"
            );
        }
    }

    #[test]
    fn the_hint_names_a_runnable_query_for_the_field() {
        let window = ResolvedWindow {
            start_ns: 1,
            end_ns: 2,
        };
        let hint = value_query_hint("logs", "http.route", window, 50);
        assert!(hint.contains("http.route"), "{hint}");
        assert!(
            hint.contains("aggregate") && hint.contains("topk"),
            "{hint}"
        );
        assert!(hint.contains("/api/v1/query"), "{hint}");
    }

    #[test]
    fn the_value_query_is_a_bounded_aggregation_over_the_window() {
        let window = ResolvedWindow {
            start_ns: 100,
            end_ns: 200,
        };
        let doc = value_query_document("traces", "http.route", window, 25);
        assert_eq!(doc["from"], "traces");
        assert_eq!(doc["range"]["from"], "100");
        assert_eq!(doc["range"]["to"], "200");
        assert_eq!(doc["pipeline"][1]["topk"]["n"], 25);
        // It must be a document the server itself accepts.
        let parsed: Document =
            serde_json::from_value(doc).expect("the sampled path runs a valid IR document");
        common::query_ir::validate(
            &parsed,
            &SourceRegistry::core(),
            &common::query_ir::InMemoryResolver::new().with_attribute(
                "traces",
                "http.route",
                "span_attributes",
                common::query_ir::ValueType::String,
                false,
            ),
        )
        .expect("the sampled value query validates");
    }
}
