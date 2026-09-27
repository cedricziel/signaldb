//! `file_format: definition/2` support: the parser must lower Weaver v2
//! model files (registry `attributes`, `attribute_groups`, `metrics`,
//! `spans`, `events`, `entities`, and `span_refinements`/`metric_refinements`)
//! into the same v1 `Group`/`AttributeSpec` shape the resolver already knows,
//! and reject files it cannot make sense of.

mod common;

use common::{fixtures_dir, genai_document, otel_resolved, resolve};
use schema_model::{Dependency, RegistryDocument};

#[test]
fn definition_v2_and_v1_lower_to_the_same_resolved_facts() {
    let v2_dir = fixtures_dir().join("definition-v2/model");
    let v1_dir = fixtures_dir().join("definition-v1/model");

    let v2_doc = RegistryDocument::from_dir("widgets", "1.0.0", &v2_dir)
        .expect("parse definition/2 fixture");
    let v1_doc = RegistryDocument::from_dir("widgets", "1.0.0", &v1_dir).expect("parse v1 fixture");

    let v2_resolved = resolve(&v2_doc, &[]);
    let v1_resolved = resolve(&v1_doc, &[]);

    for key in ["widget.id", "widget.color"] {
        let a = &v2_resolved.attributes[key];
        let b = &v1_resolved.attributes[key];
        assert_eq!(a.key, b.key);
        assert_eq!(a.brief, b.brief);
        assert_eq!(a.r#type, b.r#type);
        assert_eq!(a.enum_members, b.enum_members);
        assert_eq!(a.deprecated, b.deprecated);
    }

    let a = &v2_resolved.metrics["widget.production.count"];
    let b = &v1_resolved.metrics["widget.production.count"];
    assert_eq!(a.name, b.name);
    assert_eq!(a.instrument, b.instrument);
    assert_eq!(a.unit, b.unit);
    assert_eq!(a.entity_associations, b.entity_associations);
    let mut a_attrs: Vec<_> = a.attributes.iter().map(|x| x.key.clone()).collect();
    let mut b_attrs: Vec<_> = b.attributes.iter().map(|x| x.key.clone()).collect();
    a_attrs.sort();
    b_attrs.sort();
    assert_eq!(a_attrs, b_attrs);

    let a = &v2_resolved.entities["widget"];
    let b = &v1_resolved.entities["widget"];
    assert_eq!(a.name, b.name);
    assert_eq!(a.brief, b.brief);
    assert_eq!(
        a.identifying.iter().map(|x| &x.key).collect::<Vec<_>>(),
        b.identifying.iter().map(|x| &x.key).collect::<Vec<_>>()
    );
    assert_eq!(
        a.descriptive.iter().map(|x| &x.key).collect::<Vec<_>>(),
        b.descriptive.iter().map(|x| &x.key).collect::<Vec<_>>()
    );
}

#[test]
fn unknown_file_format_is_rejected() {
    let dir = fixtures_dir().join("bad-file-format/model");
    let err = RegistryDocument::from_dir("bad", "1.0.0", &dir)
        .expect_err("unknown file_format must be rejected");
    let message = err.to_string();
    assert!(message.contains("bad.yaml"), "{message}");
    assert!(message.contains("definition/3"), "{message}");
}

#[test]
fn non_manifest_v1_file_without_groups_is_rejected() {
    let dir = fixtures_dir().join("missing-groups/model");
    let err = RegistryDocument::from_dir("bad", "1.0.0", &dir)
        .expect_err("a non-manifest file with no `groups:` key must be rejected");
    let message = err.to_string();
    assert!(message.contains("nogroups.yaml"), "{message}");
    assert!(message.contains("groups"), "{message}");
}

#[test]
fn genai_dependency_maps_to_otel_genai_namespace() {
    let genai_dep = Dependency {
        schema_url: Some("https://opentelemetry.io/schemas/gen-ai-dev/1.42.0-dev".to_string()),
        registry_path: Some(
            "https://github.com/open-telemetry/semantic-conventions-genai.git@abc123[model]"
                .to_string(),
        ),
        ..Default::default()
    };
    assert_eq!(genai_dep.namespace(), Some("otel-genai".to_string()));

    let core_dep = Dependency {
        schema_url: Some("https://opentelemetry.io/schemas/1.44.0".to_string()),
        registry_path: Some(
            "https://github.com/open-telemetry/semantic-conventions.git@v1.44.0[model]".to_string(),
        ),
        ..Default::default()
    };
    assert_eq!(core_dep.namespace(), Some("otel".to_string()));
}

#[test]
fn vendored_genai_model_resolves_against_core_otel() {
    let resolved = resolve(&genai_document(), &[&otel_resolved()]);
    for key in [
        "gen_ai.agent.id",
        "gen_ai.agent.name",
        "gen_ai.agent.description",
        "gen_ai.agent.version",
    ] {
        let attr = &resolved.attributes[key];
        assert_eq!(attr.r#type, "string", "{key}");
        assert!(attr.deprecated.is_none(), "{key}");
    }
    assert!(resolved.attributes.len() > 60);
    assert!(resolved.metrics.len() > 10);
    assert_eq!(resolved.dependencies, ["otel"]);
}

#[test]
fn lowered_genai_document_round_trips_through_json() {
    let doc = genai_document();
    let json = serde_json::to_string(&doc).expect("serialize");
    let back = RegistryDocument::from_json(&json).expect("lowered document re-reads");
    assert_eq!(back, doc);
}
