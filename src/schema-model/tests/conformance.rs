//! Conformance suite: the parser, resolver, and validator must accept the
//! entire vendored OpenTelemetry semantic-conventions model (the source of the
//! bundled `otel` registry) and SignalDB's own registry, with zero errors, and
//! reproduce a handful of known upstream facts. This is what proves the
//! subset validator against ~900 real definitions before it ever sees a
//! tenant's custom registry.

use std::path::{Path, PathBuf};

use schema_model::{Registry, RegistryDocument, ResolvedRegistry};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn vendored_version() -> String {
    std::fs::read_to_string(repo_root().join("vendor/otel-semconv/VERSION"))
        .expect("vendor/otel-semconv/VERSION")
        .trim()
        .to_string()
}

fn otel_document() -> RegistryDocument {
    let version = vendored_version();
    let dir = repo_root().join(format!("vendor/otel-semconv/{version}/model"));
    RegistryDocument::from_dir("otel", &version, &dir).expect("parse vendored semconv model")
}

fn otel_resolved() -> ResolvedRegistry {
    Registry::resolve(&otel_document(), &[]).unwrap_or_else(|errs| {
        panic!(
            "vendored semconv must resolve cleanly, got {} errors:\n{}",
            errs.len(),
            errs.iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("\n")
        )
    })
}

#[test]
fn every_vendored_model_file_parses() {
    let doc = otel_document();
    assert!(
        doc.groups.len() > 900,
        "expected >900 groups, got {}",
        doc.groups.len()
    );
}

#[test]
fn vendored_semconv_resolves_with_zero_errors() {
    let resolved = otel_resolved();
    assert!(
        resolved.attributes.len() > 800,
        "expected >800 attributes, got {}",
        resolved.attributes.len()
    );
    assert!(
        resolved.metrics.len() > 500,
        "expected >500 metrics, got {}",
        resolved.metrics.len()
    );
    assert_eq!(
        resolved.entities.len(),
        64,
        "semconv 1.43 declares 64 entities"
    );
}

#[test]
fn known_upstream_facts_hold() {
    let resolved = otel_resolved();

    let uid = &resolved.attributes["k8s.pod.uid"];
    assert_eq!(uid.brief.trim(), "The UID of the Pod.");
    assert_eq!(uid.r#type, "string");
    assert_eq!(uid.stability.as_deref(), Some("stable"));
    assert_eq!(
        uid.group_display_name.as_deref(),
        Some("Kubernetes Attributes")
    );

    let pod = &resolved.entities["k8s.pod"];
    assert!(pod.identifying.iter().any(|a| a.key == "k8s.pod.uid"));
    assert!(pod.descriptive.iter().any(|a| a.key == "k8s.pod.name"));

    let cpu = &resolved.metrics["k8s.pod.cpu.time"];
    assert_eq!(cpu.instrument, "counter");
    assert_eq!(cpu.unit, "s");
    assert_eq!(cpu.entity_associations, vec!["k8s.pod".to_string()]);
    assert!(
        resolved
            .metrics_for_entity("k8s.pod")
            .any(|m| m == "k8s.pod.cpu.time"),
        "reverse index entity -> metrics"
    );
    assert!(
        resolved
            .entity_roles_for_attribute("k8s.pod.uid")
            .any(|r| r.entity == "k8s.pod" && r.role == schema_model::Role::Identifying),
        "reverse index attribute -> entity roles"
    );

    let old = &resolved.attributes["http.status_code"];
    let dep = old
        .deprecated
        .as_ref()
        .expect("http.status_code is deprecated");
    assert_eq!(dep.reason.as_deref(), Some("renamed"));
    assert_eq!(dep.renamed_to.as_deref(), Some("http.response.status_code"));

    let method = &resolved.attributes["http.request.method"];
    assert_eq!(method.r#type, "enum");
    assert!(method.enum_members.iter().any(|m| m.value == "GET"));

    let service = &resolved.entities["service"];
    assert_eq!(service.stability.as_deref(), Some("stable"));
    assert!(service.identifying.iter().any(|a| a.key == "service.name"));
}

#[test]
fn signaldb_registry_resolves_against_otel() {
    let otel = otel_resolved();
    let dir = repo_root().join("otel/registry");
    let doc = RegistryDocument::from_dir("signaldb", "0.1.0", &dir).expect("parse otel/registry");
    let resolved = Registry::resolve(&doc, &[&otel]).unwrap_or_else(|errs| {
        panic!(
            "signaldb registry must resolve: {:?}",
            errs.iter().map(|e| e.to_string()).collect::<Vec<_>>()
        )
    });
    assert!(resolved.attributes.contains_key("signaldb.tenant.id"));
}
