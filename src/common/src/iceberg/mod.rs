//! Consolidated Iceberg integration for SignalDB.
//!
//! This module contains all Iceberg-specific code: catalog creation,
//! table schema definitions, and naming utilities.

use crate::config::{Configuration, SchemaConfig, StorageConfig};
use anyhow::{Context, Result};
use iceberg_rust::catalog::Catalog as IcebergCatalog;
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_sql_catalog::SqlCatalog;
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous};
use sqlx::{ConnectOptions, Connection};
use std::str::FromStr;
use std::sync::Arc;
use url::Url;

pub mod evolution;
pub mod names;
pub mod schemas;
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

/// Enable WAL journaling on an on-disk SQLite Iceberg catalog before the
/// third-party [`SqlCatalog`] opens its own connection pool.
///
/// `iceberg-sql-catalog` connects through sqlx's `Any` pool and does not expose
/// its `SqliteConnectOptions`, so we cannot set these pragmas on its connections
/// directly — and sqlx 0.8's SQLite URL parser rejects `journal_mode`/
/// `busy_timeout` as query parameters, so they can't be carried on the DSN
/// either. `journal_mode = WAL`, however, is a *persistent* property of the
/// database file: once set here, every later connection (including the `Any`
/// pool's) inherits it. Under the site's trace+log commit volume the default
/// rollback journal serializes writers and blocks readers, which is what makes
/// first-time metric-table creation time out (see the `iceberg_tables`
/// slow-statement warnings). WAL lets readers proceed during a write and makes
/// each write cheaper, so the writer's `do_put` no longer exhausts its deadline.
///
/// `synchronous`/`busy_timeout` set here are per-connection and only tune this
/// one-shot connection; the `Any` pool re-applies its own defaults (a 5s
/// busy_timeout) but inherits the now-persistent WAL journal.
async fn enable_wal_on_sqlite_catalog(uri: &str) -> Result<()> {
    // Reuse sqlx's own URL parsing so the filename resolves identically to the
    // `Any` pool that SqlCatalog opens against the same `uri`.
    let options = SqliteConnectOptions::from_str(uri)
        .with_context(|| {
            format!(
                "Failed to parse SQLite catalog URI '{}'",
                crate::config::redact_dsn(uri)
            )
        })?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal);

    let conn = options.connect().await.with_context(|| {
        format!(
            "Failed to open SQLite catalog '{}' to enable WAL mode",
            crate::config::redact_dsn(uri)
        )
    })?;
    conn.close().await.with_context(|| {
        format!(
            "Failed to close SQLite catalog connection '{}'",
            crate::config::redact_dsn(uri)
        )
    })?;

    Ok(())
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

        // Set WAL journaling on the file before the Any pool connects; WAL is a
        // persistent property, so the pool inherits it. See the fn docs for why
        // this must be done out-of-band rather than via the DSN or pool options.
        enable_wal_on_sqlite_catalog(&uri).await?;

        let catalog = SqlCatalog::new(&uri, catalog_name, object_store_builder)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create SQLite catalog at '{}': {}", uri, e))?;
        Arc::new(catalog) as Arc<dyn IcebergCatalog>
    } else if catalog_uri.starts_with("sqlite:file:") {
        // Named in-memory or file-URI SQLite (e.g. sqlite:file:mydb?mode=memory&cache=shared).
        // Passed through directly — the caller is responsible for supplying a valid SQLite URI.
        // On-disk file URIs still need WAL, for the same reason as the sqlite://
        // branch above. In-memory ones must be left alone: opening a second
        // connection to a shared-cache in-memory database and closing it could
        // tear the database down before the pool ever connects.
        let is_memory = catalog_uri.contains("mode=memory") || catalog_uri.contains(":memory:");
        if !is_memory {
            enable_wal_on_sqlite_catalog(catalog_uri).await?;
        }
        let catalog = SqlCatalog::new(catalog_uri, catalog_name, object_store_builder)
            .await
            .map_err(|e| {
                anyhow::anyhow!("Failed to create SQLite catalog '{}': {}", catalog_uri, e)
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
        let catalog = SqlCatalog::new(&unique_uri, catalog_name, object_store_builder)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create in-memory SQLite catalog: {}", e))?;
        Arc::new(catalog) as Arc<dyn IcebergCatalog>
    } else {
        return Err(anyhow::anyhow!(
            "Unsupported catalog URI: {}. Only SQLite is supported.",
            catalog_uri
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
    use sqlx::Row;

    /// An on-disk SQLite Iceberg catalog must end up in WAL journal mode so that
    /// concurrent trace/log commits don't serialize behind an exclusive rollback
    /// lock and time out first-time metric-table creation.
    #[tokio::test]
    async fn on_disk_sqlite_catalog_uses_wal_journal_mode() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("catalog.db");
        let uri = format!("sqlite://{}", db_path.display());

        let _catalog = create_sql_catalog_with_builder(&uri, "test", ObjectStoreBuilder::memory())
            .await
            .expect("catalog creation should succeed");

        // Open an independent connection and confirm the persisted journal mode.
        let mut conn = SqliteConnectOptions::from_str(&uri)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mode: String = sqlx::query("PRAGMA journal_mode")
            .fetch_one(&mut conn)
            .await
            .unwrap()
            .get(0);
        assert_eq!(mode.to_lowercase(), "wal");
    }

    /// The `sqlite:file:` URI form also gets WAL when it points at an on-disk
    /// file (only in-memory `sqlite:file:...mode=memory` URIs are left alone).
    #[tokio::test]
    async fn on_disk_sqlite_file_uri_catalog_uses_wal_journal_mode() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("catalog.db");
        let uri = format!("sqlite:file:{}", db_path.display());

        let _catalog = create_sql_catalog_with_builder(&uri, "test", ObjectStoreBuilder::memory())
            .await
            .expect("catalog creation should succeed");

        let mut conn = SqliteConnectOptions::from_str(&uri)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mode: String = sqlx::query("PRAGMA journal_mode")
            .fetch_one(&mut conn)
            .await
            .unwrap()
            .get(0);
        assert_eq!(mode.to_lowercase(), "wal");
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
