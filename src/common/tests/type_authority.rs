//! [`TypeAuthority`]/[`SignalScope`]: the cached read of the type-authority
//! precedence (config > semconv > observed) tying the catalog, config, and
//! semconv resolver together.

use std::sync::Arc;

use common::catalog::Catalog;
use common::config::{AttributeTypeOverride, AttributeTypeSignal, Configuration};
use common::schema::logical::{AttributeLevel, LogicalFieldId};
use common::schema::type_authority::{
    CanonicalType, ObservedKind, Placement, Resolution, SchemaUrls, SignalScope, TypeAuthority,
    TypeSource, place,
};
use common::schema_registry::SchemaResolver;
use common::self_monitoring::SEMCONV_SCHEMA_URL;

const STATUS_CODE: &str = "http.response.status_code";
const NO_URL: SchemaUrls<'static> = SchemaUrls {
    resource: None,
    scope: None,
};

fn urls_for(schema_url: &str) -> SchemaUrls<'static> {
    // Leak once per test process; fine for a handful of short-lived test urls.
    let leaked: &'static str = Box::leak(schema_url.to_string().into_boxed_str());
    SchemaUrls {
        resource: None,
        scope: Some(leaked),
    }
}

fn record_field(source: &str, key: &str) -> LogicalFieldId {
    LogicalFieldId {
        source: source.to_string(),
        level: Some(AttributeLevel::Record),
        name: key.to_string(),
    }
}

fn override_for(
    signal: AttributeTypeSignal,
    key: &str,
    canonical: CanonicalType,
) -> AttributeTypeOverride {
    AttributeTypeOverride {
        signal,
        level: AttributeLevel::Record,
        key: key.to_string(),
        canonical_type: canonical,
        dataset: None,
    }
}

async fn authority(config: Configuration) -> TypeAuthority {
    authority_with_catalog(config).await.0
}

async fn authority_with_catalog(config: Configuration) -> (TypeAuthority, Catalog) {
    let catalog = Catalog::new_in_memory().await.expect("catalog");
    let resolver = SchemaResolver::new(catalog.clone());
    (
        TypeAuthority::new(catalog.clone(), resolver, Arc::new(config)),
        catalog,
    )
}

async fn canon(
    scope: &SignalScope,
    key: &str,
    urls: SchemaUrls<'_>,
    observed: ObservedKind,
) -> Option<CanonicalType> {
    scope
        .canonical(AttributeLevel::Record, key, urls, observed)
        .await
        .expect("canonical")
}

#[tokio::test]
async fn precedence_end_to_end_through_the_authority() {
    let mut config = Configuration::default();
    config.schema.attribute_types.push(override_for(
        AttributeTypeSignal::Traces,
        "retry.count",
        CanonicalType::String,
    ));
    let authority = authority(config).await;
    let scope = authority.scope("t1", "d1", "traces").await.expect("scope");
    let semconv_url = urls_for(SEMCONV_SCHEMA_URL);

    let cases = [
        // A real OTel int-typed key resolves from semconv over the observed kind.
        (
            STATUS_CODE,
            semconv_url,
            ObservedKind::String,
            CanonicalType::Int64,
        ),
        // A config override wins over the same semconv hint for a different key.
        (
            "retry.count",
            semconv_url,
            ObservedKind::String,
            CanonicalType::String,
        ),
        // A key unknown to semconv falls through to the observed kind.
        (
            "acme.widget.spin",
            semconv_url,
            ObservedKind::Bool,
            CanonicalType::Bool,
        ),
        // A missing schema_url still resolves from the observed kind, no error.
        (
            "no.schema.url",
            NO_URL,
            ObservedKind::Float64,
            CanonicalType::Float64,
        ),
    ];
    for (key, urls, observed, expected) in cases {
        assert_eq!(
            canon(&scope, key, urls, observed).await,
            Some(expected),
            "{key}"
        );
    }
}

#[tokio::test]
async fn tenants_are_isolated_through_the_authority() {
    let authority = authority(Configuration::default()).await;
    let a = authority
        .scope("tenant-a", "d", "logs")
        .await
        .expect("scope a");
    let b = authority
        .scope("tenant-b", "d", "logs")
        .await
        .expect("scope b");

    assert_eq!(
        canon(&a, "custom.new.key", NO_URL, ObservedKind::Int64).await,
        Some(CanonicalType::Int64)
    );
    assert_eq!(
        canon(&b, "custom.new.key", NO_URL, ObservedKind::String).await,
        Some(CanonicalType::String)
    );
}

#[tokio::test]
async fn monotonic_through_the_authority() {
    let authority = authority(Configuration::default()).await;
    let scope = authority.scope("t", "d", "metrics").await.expect("scope");

    assert_eq!(
        canon(&scope, "queue.depth", NO_URL, ObservedKind::Int64).await,
        Some(CanonicalType::Int64)
    );
    let second = canon(&scope, "queue.depth", NO_URL, ObservedKind::String).await;
    assert_eq!(
        second,
        Some(CanonicalType::Int64),
        "monotonic: first seen wins"
    );
    assert_eq!(
        place(second, ObservedKind::String),
        Placement::Residue { off_type: true }
    );
}

#[tokio::test]
async fn first_observed_non_scalar_establishes_nothing_later_scalar_wins() {
    let (authority, catalog) = authority_with_catalog(Configuration::default()).await;
    let scope = authority.scope("t", "d", "logs").await.expect("scope");

    assert_eq!(
        canon(&scope, "body.attrs", NO_URL, ObservedKind::KvList).await,
        None
    );

    let stored = catalog
        .get_attribute_type("t", "d", &record_field("logs", "body.attrs"))
        .await
        .expect("get");
    assert!(
        stored.is_none(),
        "non-scalar first observation stores nothing"
    );

    assert_eq!(
        canon(&scope, "body.attrs", NO_URL, ObservedKind::Int64).await,
        Some(CanonicalType::Int64)
    );
}

#[tokio::test]
async fn cache_survives_a_direct_retype_until_invalidated() {
    let (authority, catalog) = authority_with_catalog(Configuration::default()).await;
    let scope = authority.scope("t", "d", "traces").await.expect("scope");
    let field = record_field("traces", "span.attempt");

    assert_eq!(
        canon(&scope, "span.attempt", NO_URL, ObservedKind::Int64).await,
        Some(CanonicalType::Int64)
    );

    // Simulates another process/operator retyping the row directly.
    catalog
        .override_attribute_type("t", "d", &field, CanonicalType::String)
        .await
        .expect("direct override");

    // The already-cached scope still serves the old, now-stale value.
    assert_eq!(
        canon(&scope, "span.attempt", NO_URL, ObservedKind::Int64).await,
        Some(CanonicalType::Int64)
    );

    authority.invalidate();
    let fresh_scope = authority.scope("t", "d", "traces").await.expect("scope");
    let fresh = canon(&fresh_scope, "span.attempt", NO_URL, ObservedKind::Int64).await;
    assert_eq!(
        fresh,
        Some(CanonicalType::String),
        "invalidate picks up the retype"
    );
}

#[tokio::test]
async fn config_pins_an_already_established_field_when_the_scope_is_built() {
    let field = record_field("logs", "retry.count");
    let seed_catalog = Catalog::new_in_memory().await.expect("catalog");
    seed_catalog
        .establish_attribute_type(
            "t",
            "d",
            &field,
            Resolution {
                canonical: CanonicalType::Int64,
                source: TypeSource::Observed,
                hint_schema_url: None,
            },
        )
        .await
        .expect("establish");

    let mut config = Configuration::default();
    config.schema.attribute_types.push(override_for(
        AttributeTypeSignal::Logs,
        "retry.count",
        CanonicalType::String,
    ));
    let authority = TypeAuthority::new(
        seed_catalog.clone(),
        SchemaResolver::new(seed_catalog),
        Arc::new(config),
    );

    let scope = authority.scope("t", "d", "logs").await.expect("scope");
    let pinned = canon(&scope, "retry.count", NO_URL, ObservedKind::Int64).await;
    assert_eq!(pinned, Some(CanonicalType::String));
}
