//! Consolidated Iceberg integration for SignalDB.
//!
//! This module contains all Iceberg-specific code: catalog creation,
//! table schema definitions, and naming utilities.

use crate::config::{Configuration, SchemaConfig, StorageConfig};
use anyhow::Result;
use iceberg_rust::catalog::Catalog as IcebergCatalog;
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_sql_catalog::{SqlCatalog, SqlCatalogOptions};
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use std::sync::Arc;
use url::Url;

pub mod evolution;
pub mod names;
pub mod schemas;
pub mod sort;
pub mod table_manager;

/// Create an ObjectStoreBuilder from storage configuration
pub(crate) fn create_object_store_builder_from_config(
    storage_config: &StorageConfig,
) -> Result<ObjectStoreBuilder> {
    let url = Url::parse(&storage_config.dsn)
        .map_err(|e| anyhow::anyhow!("Invalid storage DSN '{}': {}", storage_config.dsn, e))?;

    match url.scheme() {
        "file" => {
            // Pre-create the directory: iceberg-rust unwraps
            // LocalFileSystem::new_with_prefix internally and would panic on a
            // missing path. Also applies the /.foo -> relative normalization.
            let path = crate::storage::ensure_file_dsn_dir(&url)?;
            Ok(ObjectStoreBuilder::filesystem(path))
        }
        "memory" => Ok(ObjectStoreBuilder::memory()),
        "s3" => Ok(create_s3_object_store_builder(&url)),
        scheme => Err(anyhow::anyhow!(
            "Unsupported storage scheme for catalog: {}. Supported: file, memory, s3",
            scheme
        )),
    }
}

/// Build an S3-backed [`ObjectStoreBuilder`] from an `s3://` storage DSN.
///
/// DSN format: `s3://[access_key:secret_key@]host[:port]/bucket`.
///
/// The builder starts from the real process environment
/// ([`AmazonS3Builder::from_env`]), so standard AWS variables
/// (`AWS_ACCESS_KEY_ID`, `AWS_ENDPOINT_URL`, `AWS_REGION`,
/// `AWS_SESSION_TOKEN`, ...) keep working whenever the DSN does not carry the
/// corresponding value. DSN-provided values are then applied on top and win
/// over the environment. The process environment is never read *mutably*:
/// configuration flows into the builder instance only, so concurrent catalog
/// constructions (e.g. per-tenant stores with different credentials) cannot
/// race each other through global state.
fn create_s3_object_store_builder(url: &Url) -> ObjectStoreBuilder {
    let mut builder = AmazonS3Builder::from_env();

    // Credentials from the DSN override the environment.
    let access_key = url.username();
    if !access_key.is_empty() {
        builder = builder
            .with_access_key_id(access_key)
            .with_secret_access_key(url.password().unwrap_or(""));
    }

    // Non-AWS hosts are MinIO or another S3-compatible store: derive an
    // explicit endpoint from the DSN (default MinIO port 9000) and use
    // path-style requests over plain HTTP.
    let host = url.host_str().unwrap_or("localhost");
    if !host.contains("amazonaws.com") {
        let port = url.port().unwrap_or(9000);
        let endpoint = format!("http://{host}:{port}");
        tracing::info!("Using S3-compatible endpoint from storage DSN: {endpoint}");
        builder = builder
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false);
    }

    // Default region when neither DSN nor environment provided one.
    if builder
        .get_config_value(&AmazonS3ConfigKey::Region)
        .is_none()
    {
        builder = builder.with_region("us-east-1");
    }

    // Bucket from the DSN path. The catalog re-derives the bucket from each
    // table location at build time, but setting it keeps the builder usable
    // as-is and preserves the previous behavior.
    let bucket = url.path().trim_start_matches('/');
    if !bucket.is_empty() {
        builder = builder.with_bucket_name(bucket);
    }

    ObjectStoreBuilder::S3(Box::new(builder))
}

/// Create an Iceberg catalog from full configuration
pub async fn create_catalog_with_config(config: &Configuration) -> Result<Arc<dyn IcebergCatalog>> {
    let object_store_builder = create_object_store_builder_from_config(&config.storage)?;

    create_sql_catalog_with_builder(&config.schema.catalog_uri, "signaldb", object_store_builder)
        .await
}

/// Create an Iceberg catalog with explicit object store
/// Note: This function is limited by the current catalog implementation which
/// doesn't support injecting external object stores. The object_store parameter
/// is currently ignored. Use create_catalog_with_config instead.
pub async fn create_catalog_with_object_store(
    schema_config: &SchemaConfig,
    _object_store: Arc<dyn object_store::ObjectStore>,
) -> Result<Arc<dyn IcebergCatalog>> {
    // TODO: Find a way to inject a custom object store into the catalog
    // For now, we create a memory object store builder
    tracing::warn!(
        "create_catalog_with_object_store: Cannot inject provided object store into catalog, using memory store"
    );

    create_sql_catalog_with_builder(
        &schema_config.catalog_uri,
        "signaldb",
        ObjectStoreBuilder::memory(),
    )
    .await
}

/// Create a SQL catalog with in-memory object store
pub async fn create_sql_catalog(
    catalog_uri: &str,
    catalog_name: &str,
) -> Result<Arc<dyn IcebergCatalog>> {
    // Create an in-memory object store builder
    let object_store_builder = ObjectStoreBuilder::memory();

    create_sql_catalog_with_builder(catalog_uri, catalog_name, object_store_builder).await
}

/// Extra pragmas applied to every connection the Iceberg catalog's pool opens.
///
/// The catalog itself already sets `journal_mode = wal` and
/// `busy_timeout = 30000` on every SQLite connection
/// ([JanKaul/iceberg-rust#381](https://github.com/JanKaul/iceberg-rust/pull/381)),
/// which is what keeps concurrent trace/log commits from serializing behind an
/// exclusive rollback-journal lock and stalling first-time table creation. This
/// adds the one setting it does not: `synchronous = normal`, which under WAL
/// skips an fsync per commit while still being crash-safe, and matches what
/// `src/common/src/catalog.rs` sets on the service discovery catalog.
///
/// Pragmas cannot be carried on the DSN — sqlx's SQLite URL parser rejects them
/// as query parameters — so they have to be set on the connection. Caller
/// statements run after the catalog's own, so this could also override a
/// default if we ever needed to.
fn sqlite_session_statements() -> Vec<String> {
    vec!["pragma synchronous = normal".to_string()]
}

/// Connection options for a SQLite-backed Iceberg catalog.
fn sqlite_catalog_options() -> SqlCatalogOptions {
    SqlCatalogOptions::new().with_session_statements(sqlite_session_statements())
}

/// Connection options for a PostgreSQL-backed Iceberg catalog.
///
/// PostgreSQL needs none of `sqlite_catalog_options`'s pragmas: `journal_mode`
/// and `busy_timeout` are SQLite-only concepts, and the fork's
/// `pool_options_with_setup` already skips them for non-SQLite URLs. The
/// defaults (sqlx's own pool sizing, no extra per-session statements) match
/// what `src/common/src/catalog.rs`'s service-discovery catalog uses for its
/// PostgreSQL connections.
///
/// Empty is a deliberate starting point, not an oversight: a PostgreSQL
/// equivalent of the SQLite tuning (e.g. a `set statement_timeout = '...'`
/// session statement, mirroring `busy_timeout`) is a plausible future
/// addition once we have production signal that the default is too
/// permissive, the same way the SQLite pragmas above were added in response
/// to observed contention rather than upfront.
fn postgres_catalog_options() -> SqlCatalogOptions {
    SqlCatalogOptions::new()
}

/// Internal helper to create catalog with ObjectStoreBuilder
pub(crate) async fn create_sql_catalog_with_builder(
    catalog_uri: &str,
    catalog_name: &str,
    object_store_builder: ObjectStoreBuilder,
) -> Result<Arc<dyn IcebergCatalog>> {
    let catalog = if catalog_uri.starts_with("sqlite://") && catalog_uri != "sqlite://" {
        let uri = if catalog_uri.contains('?') {
            if catalog_uri.contains("mode=") {
                catalog_uri.to_string()
            } else {
                format!("{catalog_uri}&mode=rwc")
            }
        } else {
            format!("{catalog_uri}?mode=rwc")
        };

        if let Some(path) = uri
            .split('?')
            .next()
            .and_then(|u| u.strip_prefix("sqlite:"))
        {
            let path = path.trim_start_matches('/');
            if let Some(parent) = std::path::Path::new(path).parent()
                && !parent.as_os_str().is_empty()
            {
                std::fs::create_dir_all(parent).ok();
            }
        }

        let catalog = SqlCatalog::new_with_options(
            &uri,
            catalog_name,
            object_store_builder,
            sqlite_catalog_options(),
        )
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to create SQLite catalog at '{}': {}",
                crate::config::redact_dsn(&uri),
                e
            )
        })?;
        Arc::new(catalog) as Arc<dyn IcebergCatalog>
    } else if catalog_uri.starts_with("sqlite:file:") {
        // Named in-memory or file-URI SQLite (e.g. sqlite:file:mydb?mode=memory&cache=shared).
        // An on-disk file URI needs `mode=rwc` so the pool creates the database
        // rather than failing to open it; an in-memory one already carries its
        // own mode and is passed through untouched.
        let is_memory = catalog_uri.contains("mode=memory") || catalog_uri.contains(":memory:");
        let uri = if is_memory || catalog_uri.contains("mode=") {
            catalog_uri.to_string()
        } else if catalog_uri.contains('?') {
            format!("{catalog_uri}&mode=rwc")
        } else {
            format!("{catalog_uri}?mode=rwc")
        };

        let catalog = SqlCatalog::new_with_options(
            &uri,
            catalog_name,
            object_store_builder,
            sqlite_catalog_options(),
        )
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to create SQLite catalog '{}': {}",
                crate::config::redact_dsn(&uri),
                e
            )
        })?;
        Arc::new(catalog) as Arc<dyn IcebergCatalog>
    } else if catalog_uri == "sqlite://"
        || catalog_uri.contains(":memory:")
        || catalog_uri == "memory://"
    {
        // In-memory SQLite catalog (also handle memory:// for compatibility).
        // Use a unique named database per instance so that concurrent test runs
        // don't share catalog state (which causes UNIQUE constraint conflicts).
        use std::sync::atomic::{AtomicU64, Ordering};
        static MEMORY_CATALOG_COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = MEMORY_CATALOG_COUNTER.fetch_add(1, Ordering::Relaxed);
        let unique_uri = format!("sqlite:file:signaldb_mem_{id}?mode=memory&cache=shared");
        let catalog = SqlCatalog::new_with_options(
            &unique_uri,
            catalog_name,
            object_store_builder,
            sqlite_catalog_options(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create in-memory SQLite catalog: {}", e))?;
        Arc::new(catalog) as Arc<dyn IcebergCatalog>
    } else if catalog_uri.starts_with("postgres://") || catalog_uri.starts_with("postgresql://") {
        // sqlx's `Any` driver connects lazily (`connect_lazy`, inside
        // `SqlCatalog::new_with_options`), so this does not touch the network:
        // the pool is only opened, and the catalog's tables only created, on
        // first use.
        let catalog = SqlCatalog::new_with_options(
            catalog_uri,
            catalog_name,
            object_store_builder,
            postgres_catalog_options(),
        )
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to create PostgreSQL catalog at '{}': {}",
                crate::config::redact_dsn(catalog_uri),
                e
            )
        })?;
        Arc::new(catalog) as Arc<dyn IcebergCatalog>
    } else {
        return Err(anyhow::anyhow!(
            "Unsupported catalog URI: {}. Supported: sqlite://, sqlite:file:, postgres://, postgresql://.",
            crate::config::redact_dsn(catalog_uri)
        ));
    };

    Ok(catalog)
}

/// Create an Iceberg catalog from schema config with default storage
/// This is a convenience function for tests and simple use cases
pub async fn create_catalog(schema_config: SchemaConfig) -> Result<Arc<dyn IcebergCatalog>> {
    let default_storage = StorageConfig::default();
    let object_store_builder = create_object_store_builder_from_config(&default_storage)?;

    create_sql_catalog_with_builder(&schema_config.catalog_uri, "signaldb", object_store_builder)
        .await
}

/// Create an Iceberg catalog with default configuration
/// Uses default schema config and in-memory storage
pub async fn create_default_catalog() -> Result<Arc<dyn IcebergCatalog>> {
    create_catalog(SchemaConfig::default()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg_rust::catalog::namespace::Namespace;
    use sqlx::sqlite::SqliteConnectOptions;
    use sqlx::{ConnectOptions, Row};
    use std::str::FromStr;

    /// Force the catalog's lazy pool to open a connection, which is when the
    /// session pragmas are applied.
    async fn use_catalog(catalog: &Arc<dyn IcebergCatalog>) {
        catalog
            .create_namespace(&Namespace::try_new(&["ns".to_string()]).unwrap(), None)
            .await
            .expect("namespace creation should succeed");
    }

    /// Read a pragma back over an independent connection to the same file.
    async fn pragma(uri: &str, pragma: &str) -> String {
        let mut conn = SqliteConnectOptions::from_str(uri)
            .unwrap()
            .connect()
            .await
            .unwrap();
        sqlx::query(&format!("PRAGMA {pragma}"))
            .fetch_one(&mut conn)
            .await
            .unwrap()
            .get::<String, _>(0)
    }

    /// An on-disk SQLite Iceberg catalog must end up in WAL journal mode so that
    /// concurrent trace/log commits don't serialize behind an exclusive rollback
    /// lock and time out first-time metric-table creation.
    ///
    /// The pool is lazy, so the pragmas land when it first connects rather than
    /// at construction; nothing touches the database before then.
    #[tokio::test]
    async fn on_disk_sqlite_catalog_uses_wal_journal_mode() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("catalog.db");
        let uri = format!("sqlite://{}", db_path.display());

        let catalog = create_sql_catalog_with_builder(&uri, "test", ObjectStoreBuilder::memory())
            .await
            .expect("catalog creation should succeed");
        use_catalog(&catalog).await;

        assert_eq!(pragma(&uri, "journal_mode").await.to_lowercase(), "wal");
    }

    /// The `sqlite:file:` URI form gets the same treatment.
    #[tokio::test]
    async fn on_disk_sqlite_file_uri_catalog_uses_wal_journal_mode() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("catalog.db");
        let uri = format!("sqlite:file:{}", db_path.display());

        let catalog = create_sql_catalog_with_builder(&uri, "test", ObjectStoreBuilder::memory())
            .await
            .expect("catalog creation should succeed");
        use_catalog(&catalog).await;

        assert_eq!(pragma(&uri, "journal_mode").await.to_lowercase(), "wal");
    }

    /// We add only what the catalog does not already set for itself. Repeating
    /// its `journal_mode`/`busy_timeout` would be harmless but would hide which
    /// component owns the setting, and would drift silently if it ever changed
    /// them.
    #[test]
    fn sqlite_session_statements_add_only_what_the_catalog_does_not_set() {
        let statements = sqlite_session_statements();

        assert!(
            statements
                .iter()
                .any(|s| s.contains("synchronous") && s.contains("normal")),
            "synchronous must be relaxed to NORMAL: {statements:?}"
        );
        assert!(
            !statements
                .iter()
                .any(|s| s.contains("journal_mode") || s.contains("busy_timeout")),
            "the catalog sets these itself; repeating them muddies ownership: {statements:?}"
        );
    }

    /// A `postgres://` catalog URI must be accepted, not rejected as
    /// "Unsupported catalog URI". The pool connects lazily (`connect_lazy`),
    /// so this does not require a running PostgreSQL server -- it only
    /// proves the URI reaches `SqlCatalog::new_with_options` instead of the
    /// SQLite-only error branch. `postgres_catalog_tests` below covers the
    /// real connection, table creation, and CAS commit path against a
    /// testcontainers-backed PostgreSQL instance.
    #[tokio::test]
    async fn postgres_catalog_uri_is_accepted() {
        let uri = "postgres://user:pass@localhost:5432/signaldb_catalog";

        let result =
            create_sql_catalog_with_builder(uri, "test", ObjectStoreBuilder::memory()).await;

        assert!(
            result.is_ok(),
            "expected a postgres:// catalog URI to be accepted, got {:?}",
            result.err()
        );
    }

    /// A password in the catalog URI must never reach the "Unsupported
    /// catalog URI" error message. A typo'd scheme (`postgre://` instead of
    /// `postgres://`) is a realistic way to land here with credentials still
    /// attached, since every other branch matches on scheme prefix first.
    #[tokio::test]
    async fn unsupported_catalog_uri_error_redacts_credentials() {
        let uri = "postgre://user:hunter2@db/iceberg";

        let err = create_sql_catalog_with_builder(uri, "test", ObjectStoreBuilder::memory())
            .await
            .expect_err("a typo'd scheme must be rejected");

        let message = err.to_string();
        assert!(
            !message.contains("hunter2"),
            "error message leaked the catalog URI password: {message}"
        );
        assert!(
            message.contains("***"),
            "error message should carry the redacted marker: {message}"
        );
    }

    /// The `postgresql://` scheme alias must work identically to `postgres://`.
    #[tokio::test]
    async fn postgresql_scheme_alias_catalog_uri_is_accepted() {
        let uri = "postgresql://user:pass@localhost:5432/signaldb_catalog";

        let result =
            create_sql_catalog_with_builder(uri, "test", ObjectStoreBuilder::memory()).await;

        assert!(
            result.is_ok(),
            "expected a postgresql:// catalog URI to be accepted, got {:?}",
            result.err()
        );
    }

    fn s3_builder_from_dsn(dsn: &str) -> Box<AmazonS3Builder> {
        let config = StorageConfig {
            dsn: dsn.to_string(),
        };
        match create_object_store_builder_from_config(&config).expect("s3 DSN should build") {
            ObjectStoreBuilder::S3(builder) => builder,
            other => panic!("expected S3 object store builder, got {other:?}"),
        }
    }

    /// S3 settings from the DSN must be applied to the returned builder
    /// directly — not routed through process-global environment variables,
    /// which is racy across threads and prevents per-tenant credentials.
    #[test]
    fn s3_dsn_configures_builder_without_mutating_process_env() {
        // Sentinel values that cannot legitimately exist in the environment.
        let builder = s3_builder_from_dsn(
            "s3://sentinel-key-948:sentinel-secret-948@minio.sentinel.internal:9123/sentinel-bucket-948",
        );

        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::AccessKeyId),
            Some("sentinel-key-948".to_string())
        );
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::SecretAccessKey),
            Some("sentinel-secret-948".to_string())
        );
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Endpoint),
            Some("http://minio.sentinel.internal:9123".to_string())
        );
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Bucket),
            Some("sentinel-bucket-948".to_string())
        );

        // The DSN values must never leak into the process environment.
        assert_ne!(
            std::env::var("AWS_ACCESS_KEY_ID").ok().as_deref(),
            Some("sentinel-key-948")
        );
        assert_ne!(
            std::env::var("AWS_SECRET_ACCESS_KEY").ok().as_deref(),
            Some("sentinel-secret-948")
        );
        assert_ne!(
            std::env::var("AWS_ENDPOINT_URL").ok().as_deref(),
            Some("http://minio.sentinel.internal:9123")
        );
        assert_ne!(
            std::env::var("AWS_BUCKET").ok().as_deref(),
            Some("sentinel-bucket-948")
        );
        assert_ne!(
            std::env::var("AWS_BUCKET_NAME").ok().as_deref(),
            Some("sentinel-bucket-948")
        );
    }

    /// A non-AWS host without an explicit port gets the MinIO default (9000).
    #[test]
    fn s3_dsn_defaults_custom_endpoint_port_to_9000() {
        let builder = s3_builder_from_dsn("s3://minio.sentinel.internal/bucket");
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Endpoint),
            Some("http://minio.sentinel.internal:9000".to_string())
        );
    }

    /// A real AWS host must not get a DSN-derived custom endpoint.
    #[test]
    fn s3_dsn_with_amazonaws_host_gets_no_custom_endpoint() {
        let builder = s3_builder_from_dsn("s3://s3.amazonaws.com/bucket");
        let endpoint = builder.get_config_value(&AmazonS3ConfigKey::Endpoint);
        assert!(
            endpoint
                .as_deref()
                .is_none_or(|e| !e.contains("s3.amazonaws.com")),
            "amazonaws.com host must not become a custom endpoint, got {endpoint:?}"
        );
    }

    /// When the DSN carries no credentials, the environment remains the
    /// fallback (matching the previous `from_env`-based behavior).
    #[test]
    fn s3_dsn_without_credentials_falls_back_to_environment() {
        let builder = s3_builder_from_dsn("s3://minio.sentinel.internal:9000/bucket");
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::AccessKeyId),
            std::env::var("AWS_ACCESS_KEY_ID").ok()
        );
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::SecretAccessKey),
            std::env::var("AWS_SECRET_ACCESS_KEY").ok()
        );
        // A region is always resolved (env value or the us-east-1 default).
        assert!(
            builder
                .get_config_value(&AmazonS3ConfigKey::Region)
                .is_some()
        );
    }

    /// file:// DSNs keep producing filesystem-backed builders.
    #[test]
    fn file_dsn_builds_filesystem_object_store_builder() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageConfig {
            dsn: format!("file://{}", dir.path().display()),
        };
        let builder = create_object_store_builder_from_config(&config).unwrap();
        assert!(matches!(builder, ObjectStoreBuilder::Filesystem(_)));
    }

    /// memory:// DSNs keep producing in-memory builders.
    #[test]
    fn memory_dsn_builds_memory_object_store_builder() {
        let config = StorageConfig {
            dsn: "memory://".to_string(),
        };
        let builder = create_object_store_builder_from_config(&config).unwrap();
        assert!(matches!(builder, ObjectStoreBuilder::Memory(_)));
    }
}

/// Exercises a PostgreSQL-backed Iceberg catalog against a real server: table
/// creation, then the compare-and-swap commit path that
/// `[schema].catalog_uri` is meant to guarantee across writer, querier, and
/// compactor processes racing the same catalog row.
#[cfg(test)]
mod postgres_catalog_tests {
    use super::*;
    use crate::testing::start_container_with_retry;
    use iceberg_rust::catalog::identifier::Identifier;
    use iceberg_rust::catalog::namespace::Namespace;
    use iceberg_rust::catalog::tabular::Tabular;
    use iceberg_rust::error::Error as IcebergError;
    use iceberg_rust::spec::schema::Schema;
    use iceberg_rust::spec::types::{PrimitiveType, StructField, Type};
    use iceberg_rust::table::Table;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::ContainerAsync;

    /// Start a Postgres testcontainer and return its connection DSN alongside
    /// the container handle, which must be kept alive for the DSN to remain
    /// reachable.
    async fn start_postgres_container() -> (String, ContainerAsync<Postgres>) {
        let container = start_container_with_retry(Postgres::default).await;
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let dsn = format!("postgres://postgres:postgres@{host}:{port}/postgres");
        (dsn, container)
    }

    fn test_schema() -> Schema {
        Schema::builder()
            .with_struct_field(StructField {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: None,
                write_default: None,
            })
            .build()
            .unwrap()
    }

    /// A `postgres://` catalog URI must produce a working catalog: creating a
    /// namespace and a table succeeds against a real server, not just a
    /// lazily-connected pool.
    #[tokio::test]
    async fn postgres_catalog_creates_namespace_and_table() {
        let (dsn, _container) = start_postgres_container().await;

        let catalog =
            create_sql_catalog_with_builder(&dsn, "warehouse", ObjectStoreBuilder::memory())
                .await
                .expect("postgres catalog creation should succeed");

        catalog
            .create_namespace(&Namespace::try_new(&["ns".to_string()]).unwrap(), None)
            .await
            .expect("namespace creation should succeed");

        Table::builder()
            .with_name("t")
            .with_location("/warehouse/ns/t")
            .with_schema(test_schema())
            .build(&["ns".to_string()], catalog.clone())
            .await
            .expect("table creation should succeed");

        let identifier = Identifier::new(&["ns".to_string()], "t");
        let tabular = catalog
            .load_tabular(&identifier)
            .await
            .expect("table should be loadable back from postgres");
        assert!(matches!(tabular, Tabular::Table(_)));
    }

    /// Two catalog handles over the same PostgreSQL database -- the shape of
    /// a distributed deployment where writer, querier, and compactor all CAS
    /// against the same catalog -- must not both report success for commits
    /// built on the same base metadata. This is the guarantee SQLite-on-a-
    /// single-node can provide but SQLite-on-shared-storage cannot; wiring
    /// Postgres through gives the CAS guarantee an actual multi-process
    /// backend.
    #[tokio::test]
    async fn concurrent_commit_from_a_second_catalog_reports_conflict_on_postgres() {
        let (dsn, _container) = start_postgres_container().await;
        let object_store = ObjectStoreBuilder::memory();

        let catalog_a = create_sql_catalog_with_builder(&dsn, "warehouse", object_store.clone())
            .await
            .expect("postgres catalog creation should succeed");
        let catalog_b = create_sql_catalog_with_builder(&dsn, "warehouse", object_store)
            .await
            .expect("postgres catalog creation should succeed");

        catalog_a
            .create_namespace(&Namespace::try_new(&["ns".to_string()]).unwrap(), None)
            .await
            .expect("namespace creation should succeed");

        let mut table_a = Table::builder()
            .with_name("t")
            .with_location("/warehouse/ns/t")
            .with_schema(test_schema())
            .build(&["ns".to_string()], catalog_a.clone())
            .await
            .expect("table creation should succeed");

        // Catalog B loads the table, caching the same base metadata location
        // that catalog A is about to supersede.
        let identifier = Identifier::new(&["ns".to_string()], "t");
        let Tabular::Table(mut table_b) =
            catalog_b.clone().load_tabular(&identifier).await.unwrap()
        else {
            panic!("expected a table");
        };

        table_a
            .new_transaction(None)
            .update_properties(vec![("owner".to_string(), "a".to_string())])
            .commit()
            .await
            .expect("first commit should succeed");

        // B's commit is built on metadata A already superseded: it must fail
        // rather than report a success the catalog never recorded.
        let result = table_b
            .new_transaction(None)
            .update_properties(vec![("owner".to_string(), "b".to_string())])
            .commit()
            .await;

        assert!(
            matches!(result, Err(IcebergError::CommitConflict(_))),
            "expected a commit conflict, got {result:?}"
        );

        // And the losing commit must not have overwritten the winner.
        let Tabular::Table(reloaded) = catalog_a.clone().load_tabular(&identifier).await.unwrap()
        else {
            panic!("expected a table");
        };
        assert_eq!(
            reloaded.metadata().properties.get("owner"),
            Some(&"a".to_string())
        );
    }
}
