//! `SchemaResolver::type_hints`: a sync [`TypeHints`] snapshot over the
//! bundled `otel` semconv snapshot and a tenant's custom registries.

use common::catalog::Catalog;
use common::schema::type_authority::{CanonicalType, TypeHints};
use common::schema_registry::SchemaResolver;
use schema_model::RegistryDocument;

const OTEL_URL: &str = common::self_monitoring::SEMCONV_SCHEMA_URL;
const OTHER_OTEL_VERSION_URL: &str = "https://opentelemetry.io/schemas/1.20.0";
const UNKNOWN_URL: &str = "https://unknown.example.com/v1";

const CUSTOM_YAML: &str = r#"
name: acme
version: 1.0.0
schema_url: https://acme.example/schemas/1.0.0
description: Acme custom registry for type-hint tests.
dependencies:
  - name: otel
    registry_path: https://github.com/open-telemetry/semantic-conventions@v1.43.0[model]
groups:
  - id: registry.acme.thing
    type: attribute_group
    display_name: Acme Thing Attributes
    brief: Attributes describing an Acme thing.
    attributes:
      - id: acme.thing.count
        type: int
        stability: development
        brief: Number of things.
"#;

async fn resolver() -> SchemaResolver {
    let catalog = Catalog::new_in_memory().await.expect("catalog");
    SchemaResolver::new(catalog)
}

fn custom_doc() -> RegistryDocument {
    RegistryDocument::from_yaml(CUSTOM_YAML).expect("custom doc parses")
}

#[tokio::test]
async fn otel_url_hints_are_pinned_regardless_of_declared_version() {
    let r = resolver().await;
    let hints = r.type_hints("t1").await.expect("type hints");

    assert_eq!(
        hints.hint(OTEL_URL, "http.response.status_code"),
        Some(CanonicalType::Int64)
    );
    assert_eq!(
        hints.hint(OTHER_OTEL_VERSION_URL, "http.response.status_code"),
        Some(CanonicalType::Int64)
    );
    assert_eq!(
        hints.hint(OTEL_URL, "url.full"),
        Some(CanonicalType::String)
    );
    assert_eq!(
        hints.hint(OTEL_URL, "geo.location.lon"),
        Some(CanonicalType::Float64)
    );
    assert_eq!(
        hints.hint(OTEL_URL, "k8s.service.publish_not_ready_addresses"),
        Some(CanonicalType::Bool)
    );
    assert_eq!(
        hints.hint(OTEL_URL, "k8s.pod.status.phase"),
        Some(CanonicalType::String)
    );
}

#[tokio::test]
async fn unknown_url_or_undeclared_key_gives_no_hint() {
    let r = resolver().await;
    let hints = r.type_hints("t1").await.expect("type hints");

    assert_eq!(hints.hint(UNKNOWN_URL, "http.response.status_code"), None);
    assert_eq!(hints.hint(OTEL_URL, "no.such.key"), None);
}

#[tokio::test]
async fn array_template_and_any_types_give_no_hint() {
    let r = resolver().await;
    let hints = r.type_hints("t1").await.expect("type hints");

    assert_eq!(hints.hint(OTEL_URL, "host.ip"), None);
}

#[tokio::test]
async fn custom_registry_is_selected_by_its_exact_schema_url_and_does_not_see_otel() {
    let r = resolver().await;
    r.create("t1", &custom_doc()).await.expect("create custom");
    let hints = r.type_hints("t1").await.expect("type hints");

    const CUSTOM_URL: &str = "https://acme.example/schemas/1.0.0";
    assert_eq!(
        hints.hint(CUSTOM_URL, "acme.thing.count"),
        Some(CanonicalType::Int64)
    );
    // The custom registry's own url never falls back to otel, even for a key
    // only otel defines.
    assert_eq!(hints.hint(CUSTOM_URL, "http.response.status_code"), None);
}
