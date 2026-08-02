//! Consolidated Iceberg integration for SignalDB.
//!
//! This module contains all Iceberg-specific code: catalog creation,
//! table schema definitions, and naming utilities.

use crate::config::{Configuration, SchemaConfig, StorageConfig};
use anyhow::{Context, Result};
use iceberg_rust::catalog::Catalog as IcebergCatalog;
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_sql_catalog::SqlCatalog;
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
        "s3" => {
            // ObjectStoreBuilder has limited S3 configurability
            // It reads from environment variables, so we need to set them
            // based on the DSN before creating the builder

            // Extract credentials from DSN
            let access_key = url.username();
            let secret_key = url.password().unwrap_or("");

            if !access_key.is_empty() {
                unsafe {
                    std::env::set_var("AWS_ACCESS_KEY_ID", access_key);
                    std::env::set_var("AWS_SECRET_ACCESS_KEY", secret_key);
                }
            }

            // For MinIO, we'd need to set the endpoint URL via env var
            let host = url.host_str().unwrap_or("localhost");
            if !host.contains("amazonaws.com") {
                // This is MinIO or S3-compatible
                let port = url.port().unwrap_or(9000);
                let endpoint = format!("http://{host}:{port}");
                log::info!("Setting AWS_ENDPOINT_URL for MinIO: {endpoint}");
                unsafe {
                    std::env::set_var("AWS_ENDPOINT_URL", endpoint);
                }
            }

            // Set region
            unsafe {
                std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");
            }

            // Set bucket name - extract from DSN path
            let bucket = url.path().trim_start_matches('/');
            if !bucket.is_empty() {
                log::info!("Setting AWS bucket from DSN: {bucket}");
                unsafe {
                    std::env::set_var("AWS_BUCKET", bucket);
                    std::env::set_var("AWS_BUCKET_NAME", bucket);
                }
            }

            Ok(ObjectStoreBuilder::s3())
        }
        scheme => Err(anyhow::anyhow!(
            "Unsupported storage scheme for catalog: {}. Supported: file, memory, s3",
            scheme
        )),
    }
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
    log::warn!(
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
}
