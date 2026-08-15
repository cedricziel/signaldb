//! Schema registry in `common`: bundled `otel` + `signaldb` registries
//! embedded at build time, tenant custom registries persisted in the catalog,
//! and the resolver that merges them with precedence.

use common::catalog::Catalog;
use common::schema_registry::{
    RegistrySource, SchemaResolver, StoreError, bundled_registries, is_bundled,
};
use schema_model::{RegistryDocument, Role};

const ACME: &str = include_str!("../../schema-model/tests/fixtures/acme.yaml");

async fn resolver() -> SchemaResolver {
    let catalog = Catalog::new_in_memory().await.expect("catalog");
    SchemaResolver::new(catalog)
}

fn acme(version: &str) -> RegistryDocument {
    let mut doc = RegistryDocument::from_yaml(ACME).expect("acme parses");
    doc.version = version.to_string();
    doc
}

// ---- 3.1 / 3.3 bundled --------------------------------------------------

#[test]
fn bundled_registries_are_otel_and_signaldb_at_the_pin() {
    let bundled = bundled_registries();
    let names: Vec<(&str, &str)> = bundled
        .iter()
        .map(|r| (r.resolved.namespace.as_str(), r.resolved.version.as_str()))
        .collect();
    let pinned = common::self_monitoring::SEMCONV_SCHEMA_URL
        .rsplit('/')
        .next()
        .expect("version");
    assert!(names.contains(&("otel", pinned)), "{names:?}");
    assert!(names.iter().any(|(ns, _)| *ns == "signaldb"), "{names:?}");
    assert!(is_bundled("otel"));
    assert!(is_bundled("signaldb"));
    assert!(!is_bundled("acme"));

    let otel = bundled
        .iter()
        .find(|r| r.resolved.namespace == "otel")
        .expect("otel");
    assert!(otel.resolved.attributes.contains_key("k8s.pod.uid"));
    assert!(otel.resolved.entities.contains_key("k8s.pod"));
    assert!(otel.resolved.metrics.contains_key("k8s.pod.cpu.time"));
    assert!(otel.document.groups.len() > 900, "document is embedded too");

    let signaldb = bundled
        .iter()
        .find(|r| r.resolved.namespace == "signaldb")
        .expect("signaldb");
    assert!(
        signaldb
            .resolved
            .attributes
            .contains_key("signaldb.tenant.id")
    );
}

#[tokio::test]
async fn bundled_registries_reject_mutation() {
    let r = resolver().await;
    let mut doc = acme("1.0.0");
    doc.name = "otel".to_string();
    let err = r.create("acme-tenant", &doc).await.unwrap_err();
    assert!(
        matches!(&err, StoreError::ReservedNamespace(ns) if ns == "otel"),
        "{err:?}"
    );
    let err = r.delete("acme-tenant", "otel", "1.43.0").await.unwrap_err();
    assert!(matches!(err, StoreError::ReadOnly { .. }), "{err:?}");
    let err = r
        .replace("acme-tenant", "signaldb", "0.1.0", &doc)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            StoreError::ReadOnly { .. } | StoreError::ReservedNamespace(_)
        ),
        "{err:?}"
    );
}

// ---- 4.1 catalog persistence ------------------------------------------

#[tokio::test]
async fn custom_registry_crud_round_trips_and_is_tenant_scoped() {
    let r = resolver().await;
    let created = r.create("t1", &acme("1.0.0")).await.expect("create");
    assert_eq!(created.namespace, "acme");
    assert_eq!(created.version, "1.0.0");
    assert_eq!(created.source, RegistrySource::Custom);
    assert!(created.attribute_count >= 5);
    assert_eq!(created.entity_count, 2);
    assert_eq!(created.metric_count, 1);

    // list: bundled + custom, custom first
    let list = r.list("t1").await.expect("list");
    assert_eq!(list[0].namespace, "acme");
    assert!(
        list.iter()
            .any(|s| s.namespace == "otel" && s.source == RegistrySource::Bundled)
    );
    assert!(
        list.iter()
            .all(|s| s.source != RegistrySource::Bundled || s.read_only)
    );

    // get returns the document verbatim (including unknown fields)
    let (summary, doc) = r
        .get("t1", "acme", "1.0.0")
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(summary.namespace, "acme");
    assert_eq!(doc, acme("1.0.0"));

    // duplicate create is a conflict
    let err = r.create("t1", &acme("1.0.0")).await.unwrap_err();
    assert!(matches!(err, StoreError::AlreadyExists { .. }), "{err:?}");

    // another tenant sees neither the registry nor its definitions
    assert!(r.get("t2", "acme", "1.0.0").await.expect("get").is_none());
    assert!(
        !r.list("t2")
            .await
            .expect("list")
            .iter()
            .any(|s| s.namespace == "acme")
    );
    let res = r
        .resolve_attribute("t2", "acme.order.id")
        .await
        .expect("resolve");
    assert!(res.hits.is_empty());

    // replace is all-or-nothing: an invalid document leaves the old one served
    let mut bad = acme("1.0.0");
    bad.groups[0].attributes.push(schema_model::AttributeSpec {
        r#ref: Some("acme.nonexistent".into()),
        ..Default::default()
    });
    let err = r.replace("t1", "acme", "1.0.0", &bad).await.unwrap_err();
    assert!(
        matches!(err, StoreError::Invalid(ref errs) if errs.iter().any(|e| e.path == "groups[0].attributes[3].ref")),
        "{err:?}"
    );
    let (_, still) = r
        .get("t1", "acme", "1.0.0")
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(still, acme("1.0.0"));

    // replace with a valid change takes effect
    let mut v2 = acme("1.0.0");
    v2.description = Some("edited".into());
    r.replace("t1", "acme", "1.0.0", &v2)
        .await
        .expect("replace");
    let (s, d) = r
        .get("t1", "acme", "1.0.0")
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(d.description.as_deref(), Some("edited"));
    assert_eq!(s.description.as_deref(), Some("edited"));

    // path/document identity mismatch is rejected
    let err = r.replace("t1", "acme", "9.9.9", &v2).await.unwrap_err();
    assert!(
        matches!(err, StoreError::IdentityMismatch { .. }),
        "{err:?}"
    );

    // delete removes definitions
    assert!(r.delete("t1", "acme", "1.0.0").await.expect("delete"));
    assert!(r.get("t1", "acme", "1.0.0").await.expect("get").is_none());
    let res = r
        .resolve_attribute("t1", "acme.order.id")
        .await
        .expect("resolve");
    assert!(res.hits.is_empty());
    assert!(!r.delete("t1", "acme", "1.0.0").await.expect("delete"));
}

#[tokio::test]
async fn validate_reports_without_storing() {
    let r = resolver().await;
    let ok = r.validate("t1", &acme("1.0.0")).await.expect("validate");
    assert!(ok.errors.is_empty());
    assert_eq!(ok.entity_count, 2);
    assert!(r.get("t1", "acme", "1.0.0").await.expect("get").is_none());

    let mut bad = acme("1.0.0");
    bad.groups.retain(|g| g.r#type != "attribute_group");
    let res = r.validate("t1", &bad).await.expect("validate");
    assert!(!res.errors.is_empty());
    assert!(res.errors.iter().all(|e| e.path.starts_with("groups[")));
}

#[tokio::test]
async fn unknown_dependency_is_a_validation_error() {
    let r = resolver().await;
    let mut doc = acme("1.0.0");
    doc.dependencies = vec![schema_model::Dependency {
        name: Some("nope".into()),
        ..Default::default()
    }];
    let err = r.create("t1", &doc).await.unwrap_err();
    assert!(
        matches!(err, StoreError::Invalid(ref errs) if errs[0].path == "dependencies[0]"),
        "{err:?}"
    );
}

// ---- 4.3 resolver -------------------------------------------------------

#[tokio::test]
async fn resolution_precedence_and_alternatives() {
    let r = resolver().await;
    // no custom: otel only
    let res = r
        .resolve_attribute("t1", "http.response.status_code")
        .await
        .expect("resolve");
    assert_eq!(res.hits.len(), 1);
    let hit = &res.hits[0];
    assert_eq!(hit.namespace, "otel");
    assert_eq!(hit.source, RegistrySource::Bundled);
    assert_eq!(hit.def.r#type, "int");
    assert_eq!(
        res.primary.as_ref().map(|h| h.namespace.as_str()),
        Some("otel")
    );

    // custom re-description wins but does not hide otel
    r.create("t1", &acme("1.0.0")).await.expect("create");
    let res = r
        .resolve_attribute("t1", "service.name")
        .await
        .expect("resolve");
    assert_eq!(res.hits.len(), 2);
    assert_eq!(res.hits[0].namespace, "acme");
    assert_eq!(res.hits[1].namespace, "otel");
    assert!(res.hits[0].def.brief.starts_with("Our service naming"));
    // entity roles come from every registry that gives the key a role
    assert!(
        res.hits[1]
            .entity_roles
            .iter()
            .any(|e| e.entity == "service" && e.role == Role::Identifying)
    );

    // deprecated carries rename
    let res = r
        .resolve_attribute("t1", "http.status_code")
        .await
        .expect("resolve");
    let dep = res.hits[0].def.deprecated.as_ref().expect("deprecated");
    assert_eq!(dep.renamed_to.as_deref(), Some("http.response.status_code"));

    // entity: roles, metrics, extensions
    let res = r.resolve_entity("t1", "k8s.pod").await.expect("resolve");
    let pod = &res.hits[0];
    assert_eq!(pod.namespace, "otel");
    assert!(pod.def.identifying.iter().any(|a| a.key == "k8s.pod.uid"));
    assert!(pod.metrics.iter().any(|m| m == "k8s.pod.cpu.time"));
    assert!(pod.extended_by.iter().any(|e| e == "acme/acme.k8s.pod"));

    // metric names its entities
    let res = r
        .resolve_metric("t1", "acme.checkout.latency")
        .await
        .expect("resolve");
    assert_eq!(res.hits[0].namespace, "acme");
    assert_eq!(
        res.hits[0].def.entity_associations,
        vec!["acme.order".to_string()]
    );

    // unknown key: empty, not an error
    let res = r
        .resolve_attribute("t1", "no.such.key")
        .await
        .expect("resolve");
    assert!(res.hits.is_empty());
    assert!(res.primary.is_none());
}

#[tokio::test]
async fn prefix_search_is_bounded_and_deduplicated() {
    let r = resolver().await;
    r.create("t1", &acme("1.0.0")).await.expect("create");
    let hits = r
        .search_attributes("t1", "k8s.pod.", 5)
        .await
        .expect("search");
    assert_eq!(hits.len(), 5);
    assert!(hits.iter().all(|h| h.def.key.starts_with("k8s.pod.")));
    let all = r
        .search_attributes("t1", "k8s.pod.", 500)
        .await
        .expect("search");
    assert!(all.len() > 5 && all.len() < 500);
    let mut keys: Vec<(String, String)> = all
        .iter()
        .map(|h| (h.namespace.clone(), h.def.key.clone()))
        .collect();
    keys.dedup();
    assert_eq!(keys.len(), all.len(), "no duplicate (namespace, key)");
    // custom hits sort first
    let svc = r
        .search_attributes("t1", "service.", 50)
        .await
        .expect("search");
    assert_eq!(svc[0].namespace, "acme");
    assert_eq!(svc[0].def.key, "service.name");
    let ents = r.search_entities("t1", "acme.", 10).await.expect("search");
    assert_eq!(ents.len(), 2);
    let mets = r
        .search_metrics("t1", "k8s.pod.c", 10)
        .await
        .expect("search");
    assert!(mets.iter().any(|m| m.def.name == "k8s.pod.cpu.time"));
    // empty prefix lists from the top, still bounded
    let any = r.search_attributes("t1", "", 3).await.expect("search");
    assert_eq!(any.len(), 3);
}

#[tokio::test]
async fn cache_invalidates_on_write_and_multiple_versions_order_desc() {
    let r = resolver().await;
    r.create("t1", &acme("1.0.0")).await.expect("create");
    let res = r
        .resolve_attribute("t1", "acme.order.id")
        .await
        .expect("resolve");
    assert_eq!(res.hits.len(), 1);
    r.create("t1", &acme("1.1.0")).await.expect("create v1.1");
    let res = r
        .resolve_attribute("t1", "acme.order.id")
        .await
        .expect("resolve");
    assert_eq!(res.hits.len(), 2);
    assert_eq!(res.hits[0].version, "1.1.0", "newest version first");
    assert_eq!(res.hits[1].version, "1.0.0");
    r.delete("t1", "acme", "1.1.0").await.expect("delete");
    let res = r
        .resolve_attribute("t1", "acme.order.id")
        .await
        .expect("resolve");
    assert_eq!(res.hits.len(), 1);
    assert_eq!(res.hits[0].version, "1.0.0");
}

#[tokio::test]
async fn a_fresh_resolver_reloads_custom_registries_from_the_catalog() {
    let catalog = Catalog::new_in_memory().await.expect("catalog");
    let r1 = SchemaResolver::new(catalog.clone());
    r1.create("t1", &acme("1.0.0")).await.expect("create");
    let r2 = SchemaResolver::new(catalog);
    let res = r2
        .resolve_metric("t1", "acme.checkout.latency")
        .await
        .expect("resolve");
    assert_eq!(res.hits.len(), 1);
}
