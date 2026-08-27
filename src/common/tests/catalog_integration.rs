use common::catalog::{Catalog, GrantSource, MembershipRole};
use common::flight::transport::ServiceCapability;
use common::service_bootstrap::ServiceType;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

/// Testcontainers' Postgres wait strategy already blocks `start()` until the
/// container reports itself ready, but the mapped port can still refuse the
/// very first connection for a few milliseconds afterward. Retry the actual
/// connection attempt with a short, bounded backoff instead of guessing at a
/// fixed startup delay.
async fn connect_catalog_with_retry(dsn: &str) -> Catalog {
    const MAX_ATTEMPTS: u32 = 20;
    const RETRY_DELAY: Duration = Duration::from_millis(100);

    let mut last_err = None;
    for _ in 0..MAX_ATTEMPTS {
        match Catalog::new(dsn).await {
            Ok(catalog) => return catalog,
            Err(err) => {
                last_err = Some(err);
                sleep(RETRY_DELAY).await;
            }
        }
    }
    panic!(
        "Failed to create Catalog after {MAX_ATTEMPTS} attempts: {}",
        last_err.expect("at least one connection attempt was made")
    );
}

#[tokio::test]
async fn test_ingester_operations() {
    let container = Postgres::default()
        .start()
        .await
        .expect("Failed to start database");

    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let dsn = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let catalog = connect_catalog_with_retry(&dsn).await;

    let id = Uuid::new_v4();
    catalog
        .register_ingester(
            id,
            "127.0.0.1:8080",
            ServiceType::Writer,
            &[
                ServiceCapability::TraceIngestion,
                ServiceCapability::Storage,
            ],
        )
        .await
        .expect("Failed to register ingester");

    let ingesters = catalog
        .list_ingesters()
        .await
        .expect("Failed to list ingesters");
    assert_eq!(ingesters.len(), 1);
    assert_eq!(ingesters[0].id, id);
    assert_eq!(ingesters[0].address, "127.0.0.1:8080");
    assert_eq!(ingesters[0].service_type, ServiceType::Writer);
    assert_eq!(ingesters[0].capabilities.len(), 2);
    assert!(
        ingesters[0]
            .capabilities
            .contains(&ServiceCapability::TraceIngestion)
    );
    assert!(
        ingesters[0]
            .capabilities
            .contains(&ServiceCapability::Storage)
    );

    // Test heartbeat does not error
    catalog.heartbeat(id).await.expect("Failed to heartbeat");
}

#[tokio::test]
async fn test_shard_operations() {
    let container = Postgres::default()
        .start()
        .await
        .expect("Failed to start database");

    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let dsn = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let catalog = connect_catalog_with_retry(&dsn).await;

    // Initially no shards
    let shards = catalog.list_shards().await.expect("Failed to list shards");
    assert!(shards.is_empty());

    // Add a shard
    catalog
        .add_shard(1, 0, 100)
        .await
        .expect("Failed to add shard");
    let shards = catalog.list_shards().await.expect("Failed to list shards");
    assert_eq!(shards.len(), 1);
    let shard = &shards[0];
    assert_eq!(shard.id, 1);
    assert_eq!(shard.start_range, 0);
    assert_eq!(shard.end_range, 100);

    // Duplicate insertion is a no-op
    catalog
        .add_shard(1, 0, 100)
        .await
        .expect("Failed to add shard duplicate");
    let shards = catalog.list_shards().await.expect("Failed to list shards");
    assert_eq!(shards.len(), 1);

    // Test shard owners mapping
    let id = Uuid::new_v4();
    catalog
        .register_ingester(
            id,
            "127.0.0.1:8081",
            ServiceType::Writer,
            &[
                ServiceCapability::TraceIngestion,
                ServiceCapability::Storage,
            ],
        )
        .await
        .expect("Failed to register ingester");
    catalog
        .assign_shard(1, id)
        .await
        .expect("Failed to assign shard");
    let owners = catalog
        .list_shard_owners()
        .await
        .expect("Failed to list shard owners");
    assert_eq!(owners.len(), 1);
    let owner = &owners[0];
    assert_eq!(owner.shard_id, 1);
    assert_eq!(owner.ingester_id, id);

    // Duplicate assignment is a no-op
    catalog
        .assign_shard(1, id)
        .await
        .expect("Failed to assign shard duplicate");
    let owners = catalog
        .list_shard_owners()
        .await
        .expect("Failed to list shard owners");
    assert_eq!(owners.len(), 1);
}

/// Custom schema registries persist and round-trip on Postgres exactly as on
/// SQLite (the in-memory suite lives in tests/schema_registry.rs).
#[tokio::test]
async fn schema_registry_round_trips_on_postgres() {
    use common::schema_registry::{RegistrySource, SchemaResolver};
    use schema_model::RegistryDocument;

    let container = Postgres::default()
        .start()
        .await
        .expect("Failed to start database");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let dsn = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let catalog = connect_catalog_with_retry(&dsn).await;

    let doc =
        RegistryDocument::from_yaml(include_str!("../../schema-model/tests/fixtures/acme.yaml"))
            .expect("acme parses");
    let resolver = SchemaResolver::new(catalog.clone());
    let created = resolver.create("t1", &doc).await.expect("create");
    assert_eq!(created.source, RegistrySource::Custom);
    assert_eq!(created.entity_count, 2);

    let (summary, stored) = resolver
        .get("t1", "acme", "1.0.0")
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(stored, doc);
    assert!(summary.updated_at.is_some());

    let mut edited = doc.clone();
    edited.description = Some("edited".into());
    resolver
        .replace("t1", "acme", "1.0.0", &edited)
        .await
        .expect("replace");
    let fresh = SchemaResolver::new(catalog);
    let res = fresh
        .resolve_attribute("t1", "acme.order.id")
        .await
        .expect("resolve");
    assert_eq!(res.hits.len(), 1);
    assert!(fresh.delete("t1", "acme", "1.0.0").await.expect("delete"));
    assert!(
        fresh
            .get("t2", "acme", "1.0.0")
            .await
            .expect("get")
            .is_none()
    );
}

/// `users` carries a nullable `password_hash` and the linked-OIDC-identity
/// columns on Postgres, mirroring the SQLite suite in `src/catalog.rs`
/// (change: oidc-login, task 1.1/1.2).
#[tokio::test]
async fn users_support_nullable_password_and_oidc_identity_on_postgres() {
    let container = Postgres::default()
        .start()
        .await
        .expect("Failed to start database");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let dsn = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let catalog = connect_catalog_with_retry(&dsn).await;

    let sso_user = catalog
        .create_user("sso@example.com", None, None, false)
        .await
        .expect("create SSO-only user");
    assert!(sso_user.password_hash.is_none());
    assert!(sso_user.oidc_issuer.is_none());

    assert!(
        catalog
            .find_user_by_oidc_identity("https://idp.example.com", "subject-1")
            .await
            .expect("lookup")
            .is_none()
    );

    if let Catalog::Postgres(pool) = &catalog {
        sqlx::query("UPDATE users SET oidc_issuer = $1, oidc_subject = $2 WHERE id = $3")
            .bind("https://idp.example.com")
            .bind("subject-1")
            .bind(&sso_user.id)
            .execute(pool)
            .await
            .expect("link identity");
    }
    let found = catalog
        .find_user_by_oidc_identity("https://idp.example.com", "subject-1")
        .await
        .expect("lookup")
        .expect("linked user found");
    assert_eq!(found.id, sso_user.id);
}

/// Source-keyed memberships on Postgres: `upsert_tenant_membership` and
/// `remove_tenant_membership` only ever touch `granted_by = 'local'` rows,
/// `sync_oidc_memberships` only touches `oidc_mapping` rows, a local and a
/// mapped row coexist for the same `(user_id, tenant_id)`, and
/// `get_tenant_membership` resolves the higher role (change: oidc-login,
/// task 1.3/1.4).
#[tokio::test]
async fn tenant_memberships_are_source_aware_on_postgres() {
    let container = Postgres::default()
        .start()
        .await
        .expect("Failed to start database");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let dsn = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let catalog = connect_catalog_with_retry(&dsn).await;

    catalog
        .upsert_tenant("acme", "Acme Corp", None, "config")
        .await
        .unwrap();
    let user = catalog
        .create_user("member@example.com", None, Some("hash"), false)
        .await
        .unwrap();

    catalog
        .upsert_tenant_membership(&user.id, "acme", MembershipRole::Viewer)
        .await
        .unwrap();
    catalog
        .sync_oidc_memberships(&user.id, &[("acme".to_string(), MembershipRole::Admin)])
        .await
        .unwrap();

    let rows = catalog.list_memberships_for_user(&user.id).await.unwrap();
    assert_eq!(rows.len(), 2, "local and mapped rows coexist");
    assert!(rows.iter().any(|m| m.granted_by == GrantSource::Local));
    assert!(
        rows.iter()
            .any(|m| m.granted_by == GrantSource::OidcMapping)
    );

    let effective = catalog
        .get_tenant_membership(&user.id, "acme")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(effective.role, MembershipRole::Admin);
    assert_eq!(effective.granted_by, GrantSource::OidcMapping);

    // remove_tenant_membership only removes the local row.
    catalog
        .remove_tenant_membership(&user.id, "acme")
        .await
        .unwrap();
    let rows = catalog.list_memberships_for_user(&user.id).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].granted_by, GrantSource::OidcMapping);

    // sync_oidc_memberships with an empty desired set clears the mapped row.
    catalog.sync_oidc_memberships(&user.id, &[]).await.unwrap();
    assert!(
        catalog
            .list_memberships_for_user(&user.id)
            .await
            .unwrap()
            .is_empty()
    );

    let for_tenant = catalog.list_members_for_tenant("acme").await.unwrap();
    assert!(for_tenant.is_empty());
}

/// A pre-existing (pre-oidc-login) Postgres catalog is migrated in place:
/// `password_hash` becomes nullable, the OIDC columns and unique index
/// appear, and every pre-existing membership row becomes `granted_by =
/// 'local'` under the re-keyed primary key — all without losing data
/// (change: oidc-login migration plan).
#[tokio::test]
async fn postgres_migration_from_pre_oidc_schema_preserves_data() {
    let container = Postgres::default()
        .start()
        .await
        .expect("Failed to start database");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let dsn = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    // Build the pre-oidc-login schema directly and seed it, exactly as an
    // already-deployed instance would have it.
    {
        let pool = connect_pg_with_retry(&dsn).await;
        sqlx::query(
            r#"CREATE TABLE tenants (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, default_dataset TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                source TEXT NOT NULL CHECK(source IN ('config', 'database'))
            )"#,
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            r#"CREATE TABLE users (
                id TEXT PRIMARY KEY, email TEXT NOT NULL UNIQUE, display_name TEXT,
                password_hash TEXT NOT NULL, is_instance_admin BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), disabled_at TIMESTAMPTZ
            )"#,
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            r#"CREATE TABLE tenant_memberships (
                user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
                role TEXT NOT NULL CHECK(role IN ('admin', 'member', 'viewer')),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (user_id, tenant_id)
            )"#,
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO tenants (id, name, source) VALUES ('acme', 'Acme', 'config')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO users (id, email, password_hash) VALUES ('u1', 'legacy@example.com', 'phc-legacy')",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO tenant_memberships (user_id, tenant_id, role) VALUES ('u1', 'acme', 'admin')",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;
    }

    // Opening it through the real Catalog runs the migration.
    let catalog = connect_catalog_with_retry(&dsn).await;

    let user = catalog.get_user("u1").await.unwrap().unwrap();
    assert_eq!(user.password_hash.as_deref(), Some("phc-legacy"));
    assert!(user.oidc_issuer.is_none());

    let membership = catalog
        .get_tenant_membership("u1", "acme")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(membership.role, MembershipRole::Admin);
    assert_eq!(membership.granted_by, GrantSource::Local);

    // The migrated schema now accepts a nullable password_hash and a second
    // membership row for the same (user, tenant).
    let sso_user = catalog
        .create_user("sso@example.com", None, None, false)
        .await
        .unwrap();
    assert!(sso_user.password_hash.is_none());
    catalog
        .sync_oidc_memberships(
            &sso_user.id,
            &[("acme".to_string(), MembershipRole::Viewer)],
        )
        .await
        .unwrap();

    // Re-running the migration (as every startup does) is a no-op.
    drop(catalog);
    let catalog_again = connect_catalog_with_retry(&dsn).await;
    let membership = catalog_again
        .get_tenant_membership("u1", "acme")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(membership.role, MembershipRole::Admin);

    // The `granted_by` CHECK constraint must also survive migrating an
    // existing installation, not just appear on a fresh install (change:
    // oidc-login, code review Group 1 should-fix 3).
    let Catalog::Postgres(pool) = &catalog_again else {
        unreachable!()
    };
    let invalid_source = sqlx::query(
        "INSERT INTO tenant_memberships (user_id, tenant_id, role, granted_by) \
         VALUES ('u1', 'acme', 'admin', 'bogus')",
    )
    .execute(pool)
    .await;
    assert!(
        invalid_source.is_err(),
        "granted_by CHECK constraint must be present after migrating a pre-oidc-login installation"
    );
}

/// Connects a raw Postgres pool with the same startup retry as
/// [`connect_catalog_with_retry`], for tests that need to seed a schema
/// before `Catalog::new` runs its migrations against it.
async fn connect_pg_with_retry(dsn: &str) -> sqlx::PgPool {
    const MAX_ATTEMPTS: u32 = 20;
    const RETRY_DELAY: Duration = Duration::from_millis(100);

    let mut last_err = None;
    for _ in 0..MAX_ATTEMPTS {
        match sqlx::PgPool::connect(dsn).await {
            Ok(pool) => return pool,
            Err(err) => {
                last_err = Some(err);
                sleep(RETRY_DELAY).await;
            }
        }
    }
    panic!(
        "Failed to connect Postgres pool after {MAX_ATTEMPTS} attempts: {}",
        last_err.expect("at least one connection attempt was made")
    );
}
