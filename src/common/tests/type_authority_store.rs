use common::catalog::Catalog;
use common::schema::logical::{AttributeLevel, LogicalFieldId, LogicalSchema};
use common::schema::type_authority::{CanonicalType, Placement, Resolution, TypeSource, place};

async fn catalog() -> Catalog {
    Catalog::new_in_memory().await.expect("catalog")
}

fn field(source: &str, level: Option<AttributeLevel>, name: &str) -> LogicalFieldId {
    LogicalFieldId {
        source: source.to_string(),
        level,
        name: name.to_string(),
    }
}

fn observed(canonical: CanonicalType) -> Resolution<'static> {
    Resolution {
        canonical,
        source: TypeSource::Observed,
        hint_schema_url: None,
    }
}

/// Each row establishes the same wire key `http.status` / `service.name`
/// independently: distinct (tenant, dataset, level) triples must not share a
/// canonical type, whether the axis that varies is tenant, dataset, or level.
#[tokio::test]
async fn canonical_type_is_scoped_per_tenant_dataset_and_level() {
    let catalog = catalog().await;
    use AttributeLevel as L;
    use CanonicalType as C;
    let cases = [
        ("tenant-a", "default", L::Record, C::Int64),
        ("tenant-b", "default", L::Record, C::String),
        ("tenant-a", "other", L::Record, C::Bool),
        // Same tenant+dataset, different level: no shared home.
        ("t", "d", L::Resource, C::String),
        ("t", "d", L::Record, C::Int64),
    ];

    for (tenant, dataset, level, canonical) in cases {
        let f = field("traces", Some(level), "http.status");
        let stored = catalog
            .establish_attribute_type(tenant, dataset, &f, observed(canonical))
            .await
            .expect("establish");
        assert_eq!(stored.canonical, canonical, "{tenant}/{dataset}/{level:?}");
    }
}

#[tokio::test]
async fn establish_is_monotonic_first_seen_wins() {
    let catalog = catalog().await;
    let f = field("logs", Some(AttributeLevel::Record), "retry.count");

    let first = catalog
        .establish_attribute_type("t", "d", &f, observed(CanonicalType::Int64))
        .await
        .expect("first establish");
    assert_eq!(first.canonical, CanonicalType::Int64);
    assert_eq!(first.source, TypeSource::Observed);

    let second = catalog
        .establish_attribute_type("t", "d", &f, observed(CanonicalType::String))
        .await
        .expect("second establish");
    assert_eq!(second.canonical, CanonicalType::Int64);
    assert_eq!(second.source, TypeSource::Observed);
    assert_eq!(second.hint_schema_url, None);

    let stored = catalog
        .get_attribute_type("t", "d", &f)
        .await
        .expect("get")
        .expect("row exists");
    assert_eq!(stored.canonical, CanonicalType::Int64);
    assert_eq!(stored.schema_version, LogicalSchema::VERSION);
}

#[tokio::test]
async fn concurrent_first_writers_converge_on_one_home() {
    use common::schema::type_authority::ObservedKind;

    let catalog = std::sync::Arc::new(catalog().await);
    let f = std::sync::Arc::new(field(
        "metrics",
        Some(AttributeLevel::Record),
        "queue.depth",
    ));
    let kinds = [ObservedKind::Int64, ObservedKind::String].repeat(8);

    let mut handles = Vec::new();
    for kind in &kinds {
        let catalog = catalog.clone();
        let f = f.clone();
        let canonical = kind.canonical().expect("scalar kind");
        handles.push(tokio::spawn(async move {
            catalog
                .establish_attribute_type("t", "d", &f, observed(canonical))
                .await
                .expect("establish")
        }));
    }

    let mut winners = Vec::new();
    for handle in handles {
        winners.push(handle.await.expect("task").canonical);
    }

    let winner = winners[0];
    assert!(winners.iter().all(|w| *w == winner));

    for kind in kinds {
        if kind.canonical() != Some(winner) {
            assert_eq!(
                place(Some(winner), kind),
                Placement::Residue { off_type: true }
            );
        }
    }
}

#[tokio::test]
async fn record_off_type_accumulates_and_is_noop_when_missing() {
    let catalog = catalog().await;
    let f = field("logs", Some(AttributeLevel::Record), "body.size");

    catalog
        .record_off_type("t", "d", &f, 3)
        .await
        .expect("no-op record_off_type");
    assert!(
        catalog
            .get_attribute_type("t", "d", &f)
            .await
            .unwrap()
            .is_none()
    );

    catalog
        .establish_attribute_type("t", "d", &f, observed(CanonicalType::Int64))
        .await
        .expect("establish");

    catalog.record_off_type("t", "d", &f, 3).await.unwrap();
    catalog.record_off_type("t", "d", &f, 4).await.unwrap();

    let stored = catalog
        .get_attribute_type("t", "d", &f)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.off_type_count, 7);
}

#[tokio::test]
async fn establish_rejects_fields_without_a_level() {
    let catalog = catalog().await;
    let f = field("traces", None, "trace.id");

    let err = catalog
        .establish_attribute_type("t", "d", &f, observed(CanonicalType::String))
        .await
        .expect_err("no level should be rejected");
    assert!(matches!(
        err,
        common::schema::type_authority::StoreError::NoLevel
    ));
}

/// A config override is the one path that may retype an already-established
/// row, and does so permanently: a later observed establish must not undo it.
#[tokio::test]
async fn override_retypes_an_established_row_and_stays_monotonic_afterward() {
    let catalog = catalog().await;
    let f = field("logs", Some(AttributeLevel::Record), "retry.count");

    let established = catalog
        .establish_attribute_type("t", "d", &f, observed(CanonicalType::Int64))
        .await
        .expect("establish");
    assert_eq!(established.canonical, CanonicalType::Int64);
    catalog.record_off_type("t", "d", &f, 5).await.unwrap();

    let overridden = catalog
        .override_attribute_type("t", "d", &f, CanonicalType::String)
        .await
        .expect("override");
    assert_eq!(overridden.canonical, CanonicalType::String);
    assert_eq!(overridden.source, TypeSource::Config);
    assert_eq!(overridden.hint_schema_url, None);
    assert_eq!(overridden.schema_version, LogicalSchema::VERSION);
    assert_eq!(overridden.off_type_count, 5, "off-type count is preserved");

    // A later observed establish must not undo the config override: the
    // canonical type is monotonic once set, whichever path set it.
    let after = catalog
        .establish_attribute_type("t", "d", &f, observed(CanonicalType::Bool))
        .await
        .expect("establish after override");
    assert_eq!(after.canonical, CanonicalType::String);
    assert_eq!(after.source, TypeSource::Config);

    let stored = catalog
        .get_attribute_type("t", "d", &f)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.canonical, CanonicalType::String);
    assert_eq!(stored.source, TypeSource::Config);
    assert_eq!(stored.off_type_count, 5);
}

#[tokio::test]
async fn override_on_a_missing_row_creates_it() {
    let catalog = catalog().await;
    let f = field("traces", Some(AttributeLevel::Resource), "service.tier");

    let overridden = catalog
        .override_attribute_type("t", "d", &f, CanonicalType::Bool)
        .await
        .expect("override");
    assert_eq!(overridden.canonical, CanonicalType::Bool);
    assert_eq!(overridden.source, TypeSource::Config);
    assert_eq!(overridden.off_type_count, 0);

    let stored = catalog
        .get_attribute_type("t", "d", &f)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.canonical, CanonicalType::Bool);
    assert_eq!(stored.source, TypeSource::Config);
}
