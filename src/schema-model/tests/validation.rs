//! Validator behaviour on custom registries: every rule the spec names is
//! rejected with a path that points at the offending group/attribute, and a
//! well-formed Weaver-style file (fixtures/acme.yaml) resolves against otel.

use std::path::{Path, PathBuf};

use schema_model::{Registry, RegistryDocument, ResolvedRegistry, Role, is_reserved_namespace};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn otel() -> ResolvedRegistry {
    let version = std::fs::read_to_string(repo_root().join("vendor/otel-semconv/VERSION"))
        .expect("VERSION")
        .trim()
        .to_string();
    let doc = RegistryDocument::from_dir(
        "otel",
        &version,
        &repo_root().join(format!("vendor/otel-semconv/{version}/model")),
    )
    .expect("parse otel");
    Registry::resolve(&doc, &[]).expect("otel resolves")
}

fn fixture(name: &str) -> RegistryDocument {
    let text = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("tests/fixtures/{name}")),
    )
    .unwrap_or_else(|e| panic!("fixture {name}: {e}"));
    RegistryDocument::from_yaml(&text).unwrap_or_else(|e| panic!("parse {name}: {e}"))
}

fn errors_of(yaml: &str) -> Vec<String> {
    let doc = RegistryDocument::from_yaml(yaml).expect("yaml parses");
    let otel = otel();
    match Registry::resolve(&doc, &[&otel]) {
        Ok(_) => Vec::new(),
        Err(errs) => errs.iter().map(|e| e.to_string()).collect(),
    }
}

#[test]
fn acme_fixture_resolves_against_otel_and_yields_all_kinds() {
    let otel = otel();
    let doc = fixture("acme.yaml");
    let resolved = Registry::resolve(&doc, &[&otel]).unwrap_or_else(|errs| {
        panic!(
            "{:?}",
            errs.iter().map(|e| e.to_string()).collect::<Vec<_>>()
        )
    });
    assert_eq!(resolved.namespace, "acme");
    assert_eq!(resolved.version, "1.0.0");
    let order_id = &resolved.attributes["acme.order.id"];
    assert_eq!(
        order_id.group_display_name.as_deref(),
        Some("Acme Order Attributes")
    );
    // A custom registry may re-describe an upstream key under its own namespace.
    assert!(resolved.attributes.contains_key("service.name"));
    let order = &resolved.entities["acme.order"];
    assert_eq!(order.identifying.len(), 1);
    assert!(
        order
            .descriptive
            .iter()
            .any(|a| a.key == "acme.order.total")
    );
    // Extension of an otel entity: inherits identifying attrs, adds descriptive.
    let rack_pod = &resolved.entities["acme.k8s.pod"];
    assert_eq!(rack_pod.extends.as_deref(), Some("k8s.pod"));
    assert!(rack_pod.identifying.iter().any(|a| a.key == "k8s.pod.uid"));
    assert!(rack_pod.descriptive.iter().any(|a| a.key == "acme.rack.id"));
    let latency = &resolved.metrics["acme.checkout.latency"];
    assert_eq!(latency.instrument, "histogram");
    assert_eq!(latency.entity_associations, vec!["acme.order".to_string()]);
    assert!(
        resolved
            .metrics_for_entity("acme.order")
            .any(|m| m == "acme.checkout.latency")
    );
    assert!(
        resolved
            .entity_roles_for_attribute("acme.order.id")
            .any(|r| r.entity == "acme.order" && r.role == Role::Identifying)
    );
}

#[test]
fn weaver_checked_fixtures_resolve() {
    let otel = otel();
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/weaver");
    for entry in std::fs::read_dir(&dir).expect("fixtures/weaver") {
        let path = entry.expect("entry").path();
        let text = std::fs::read_to_string(&path).expect("read fixture");
        let doc = RegistryDocument::from_yaml(&text)
            .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        Registry::resolve(&doc, &[&otel]).unwrap_or_else(|errs| {
            panic!(
                "{}: {:?}",
                path.display(),
                errs.iter().map(|e| e.to_string()).collect::<Vec<_>>()
            )
        });
    }
}

#[test]
fn json_and_yaml_forms_are_equivalent() {
    let yaml = fixture("acme.yaml");
    let json = serde_json::to_string(&yaml).expect("serialize");
    let back = RegistryDocument::from_json(&json).expect("json parses");
    assert_eq!(yaml, back);
}

#[test]
fn unknown_fields_round_trip() {
    let doc = RegistryDocument::from_yaml(
        r#"
name: x
version: 0.1.0
custom_top: 1
groups:
  - id: span.thing
    type: span
    span_kind: client
    attributes:
      - ref: server.address
        sampling_relevant: true
"#,
    )
    .expect("parses");
    assert_eq!(doc.extra["custom_top"], serde_json::json!(1));
    assert_eq!(
        doc.groups[0].extra["span_kind"],
        serde_json::json!("client")
    );
    assert_eq!(
        doc.groups[0].attributes[0].extra["sampling_relevant"],
        serde_json::json!(true)
    );
    let json = serde_json::to_value(&doc).expect("serialize");
    assert_eq!(json["groups"][0]["span_kind"], "client");
}

#[test]
fn dangling_ref_is_rejected_with_path() {
    let errs = errors_of(
        r#"
name: acme
version: 1.0.0
groups:
  - id: registry.acme
    type: attribute_group
    attributes:
      - id: acme.a
        type: string
        brief: a
        stability: development
      - ref: acme.nonexistent
"#,
    );
    assert_eq!(
        errs,
        vec!["groups[0].attributes[1].ref: unresolved ref `acme.nonexistent`"]
    );
}

#[test]
fn duplicate_ids_are_rejected() {
    let errs = errors_of(
        r#"
name: acme
version: 1.0.0
groups:
  - id: registry.acme
    type: attribute_group
    attributes:
      - id: acme.a
        type: string
        brief: a
      - id: acme.a
        type: string
        brief: again
  - id: registry.acme
    type: attribute_group
"#,
    );
    assert!(
        errs.iter()
            .any(|e| e == "groups[0].attributes[1].id: duplicate attribute id `acme.a`"),
        "{errs:?}"
    );
    assert!(
        errs.iter()
            .any(|e| e == "groups[1].id: duplicate group id `registry.acme`"),
        "{errs:?}"
    );
}

#[test]
fn invalid_type_is_rejected() {
    let errs = errors_of(
        r#"
name: acme
version: 1.0.0
groups:
  - id: registry.acme
    type: attribute_group
    attributes:
      - id: acme.a
        type: varchar
        brief: a
"#,
    );
    assert_eq!(
        errs,
        vec!["groups[0].attributes[0].type: unknown attribute type `varchar`"]
    );
}

#[test]
fn unknown_role_fails_to_parse() {
    let res = RegistryDocument::from_yaml(
        r#"
name: acme
version: 1.0.0
groups:
  - id: entity.acme.thing
    type: entity
    name: acme.thing
    attributes:
      - ref: service.name
        role: primary
"#,
    );
    assert!(res.is_err());
}

#[test]
fn metric_missing_instrument_or_unit_is_rejected() {
    let errs = errors_of(
        r#"
name: acme
version: 1.0.0
groups:
  - id: metric.acme.a
    type: metric
    metric_name: acme.a
    brief: a
    unit: "s"
  - id: metric.acme.b
    type: metric
    metric_name: acme.b
    brief: b
    instrument: gauge
  - id: metric.acme.c
    type: metric
    metric_name: acme.c
    brief: c
    instrument: thermometer
    unit: "s"
"#,
    );
    assert!(
        errs.iter()
            .any(|e| e == "groups[0].instrument: metric `acme.a` needs `instrument`"),
        "{errs:?}"
    );
    assert!(
        errs.iter()
            .any(|e| e == "groups[1].unit: metric `acme.b` needs `unit`"),
        "{errs:?}"
    );
    assert!(
        errs.iter()
            .any(|e| e == "groups[2].instrument: unknown instrument `thermometer`"),
        "{errs:?}"
    );
}

#[test]
fn association_to_unknown_entity_is_rejected() {
    let errs = errors_of(
        r#"
name: acme
version: 1.0.0
groups:
  - id: metric.acme.a
    type: metric
    metric_name: acme.a
    brief: a
    instrument: gauge
    unit: "1"
    entity_associations:
      - k8s.pod
      - acme.rack
"#,
    );
    assert_eq!(
        errs,
        vec![
            "groups[0].entity_associations[1]: metric `acme.a` is associated with unknown entity `acme.rack`"
        ]
    );
}

#[test]
fn extension_cannot_add_identifying_attribute() {
    let errs = errors_of(
        r#"
name: acme
version: 1.0.0
groups:
  - id: registry.acme
    type: attribute_group
    attributes:
      - id: acme.rack.id
        type: string
        brief: rack
  - id: entity.acme.k8s.pod
    type: entity
    name: acme.k8s.pod
    extends: entity.k8s.pod
    attributes:
      - ref: acme.rack.id
        role: identifying
"#,
    );
    assert_eq!(
        errs,
        vec![
            "groups[1].attributes[0].role: entity `acme.k8s.pod` extends `k8s.pod` and may not add identifying attribute `acme.rack.id`"
        ]
    );
}

#[test]
fn reserved_namespaces() {
    assert!(is_reserved_namespace("otel"));
    assert!(is_reserved_namespace("signaldb"));
    assert!(!is_reserved_namespace("acme"));
}

#[test]
fn missing_name_or_version_is_rejected() {
    let errs = errors_of("name: ''\nversion: ''\ngroups: []\n");
    assert!(errs.iter().any(|e| e.starts_with("name:")), "{errs:?}");
    assert!(errs.iter().any(|e| e.starts_with("version:")), "{errs:?}");
}

#[test]
fn legacy_string_deprecated_form_is_accepted() {
    let doc = RegistryDocument::from_yaml(
        r#"
name: acme
version: 1.0.0
groups:
  - id: registry.acme
    type: attribute_group
    attributes:
      - id: acme.old
        type: string
        brief: old
        deprecated: "Use acme.new instead."
"#,
    )
    .expect("parses");
    let resolved = Registry::resolve(&doc, &[]).expect("resolves");
    let dep = resolved.attributes["acme.old"]
        .deprecated
        .as_ref()
        .expect("deprecated");
    assert_eq!(dep.reason, None);
    assert_eq!(dep.note.as_deref(), Some("Use acme.new instead."));
}
