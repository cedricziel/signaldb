//! Verifies the generated client exposes the schema-registry operations the
//! CLI `schema` / `admin schema` commands and the MCP `*_schema*` tools wrap
//! (openspec change `schema-registry`, task 6.1).
//!
//! Compile-and-construct assertions: the surface is generated from
//! `api/signaldb-api.json`, so if a schema endpoint drops out of the OpenAPI
//! document this test stops compiling.

use signaldb_sdk::Client;

#[test]
fn client_exposes_registry_management_builders() {
    let client = Client::new(
        "http://localhost:8080",
        signaldb_sdk::RetryPolicy::default(),
    );

    let _list = client.schema_list_registries();
    let _get = client
        .schema_get_registry()
        .namespace("acme")
        .version("1.0.0");
    let _create = client.schema_create_registry();
    let _replace = client
        .schema_replace_registry()
        .namespace("acme")
        .version("1.0.0");
    let _delete = client
        .schema_delete_registry()
        .namespace("acme")
        .version("1.0.0");
    let _validate = client.schema_validate_registry();
}

#[test]
fn client_exposes_resolve_and_search_builders() {
    let client = Client::new(
        "http://localhost:8080",
        signaldb_sdk::RetryPolicy::default(),
    );

    let _attr = client.schema_resolve_attribute().key("k8s.pod.uid");
    let _attrs = client
        .schema_search_attributes()
        .prefix("k8s.")
        .limit(10_u64);
    let _batch = client
        .schema_search_attributes()
        .keys("service.name,k8s.pod.uid");
    let _entity = client.schema_resolve_entity().name("k8s.pod");
    let _entities = client.schema_search_entities().prefix("k8s.");
    let _metric = client.schema_resolve_metric().name("k8s.pod.cpu.time");
    let _metrics = client.schema_search_metrics().prefix("k8s.");
}

/// The mutation builders take the raw registry document as a JSON object, so
/// a YAML upload converts to `serde_json::Value` first (the CLI does this).
#[test]
fn registry_document_body_is_a_json_object() {
    let client = Client::new(
        "http://localhost:8080",
        signaldb_sdk::RetryPolicy::default(),
    );
    let document = serde_json::json!({
        "name": "acme",
        "version": "1.0.0",
        "groups": []
    });
    let object = document.as_object().expect("document is an object").clone();
    let _create = client.schema_create_registry().body(object.clone());
    let _validate = client.schema_validate_registry().body(object);
}

/// The resolve envelope (`{ key, primary, hits }`) round-trips through the
/// generated types with namespace-tagged hits in precedence order.
#[test]
fn attribute_resolution_round_trips_precedence_envelope() {
    use signaldb_sdk::types::AttributeResolution;

    let json = serde_json::json!({
        "key": "service.name",
        "primary": hit("acme", "1.0.0", "custom"),
        "hits": [hit("acme", "1.0.0", "custom"), hit("otel", "1.43.0", "bundled")]
    });
    let resolution: AttributeResolution =
        serde_json::from_value(json).expect("resolution deserializes");
    assert_eq!(resolution.key, "service.name");
    assert_eq!(
        resolution.primary.as_ref().map(|h| h.namespace.as_str()),
        Some("acme")
    );
    let namespaces: Vec<&str> = resolution
        .hits
        .iter()
        .map(|h| h.namespace.as_str())
        .collect();
    assert_eq!(namespaces, ["acme", "otel"]);
}

fn hit(namespace: &str, version: &str, source: &str) -> serde_json::Value {
    serde_json::json!({
        "key": "service.name",
        "namespace": namespace,
        "version": version,
        "source": source,
        "group_id": "registry.service",
        "type": "string",
        "brief": "Logical name of the service.",
    })
}
