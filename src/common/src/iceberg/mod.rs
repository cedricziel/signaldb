//! Consolidated Iceberg integration for SignalDB.
//!
//! This module contains all Iceberg-specific code: catalog creation,
//! table schema definitions, and naming utilities.

use crate::config::{Configuration, SchemaConfig, StorageConfig};
use anyhow::Result;
use iceberg_rust::catalog::Catalog as IcebergCatalog;
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_sql_catalog::SqlCatalog;
use std::sync::Arc;
use url::Url;

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
            let path = url.path();
            if path.is_empty() || path == "/" {
                return Err(anyhow::anyhow!(
                    "File DSN must specify a path: file:///path/to/storage"
                ));
            }
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

        let catalog = SqlCatalog::new(&uri, catalog_name, object_store_builder)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create SQLite catalog at '{}': {}", uri, e))?;
        Arc::new(catalog) as Arc<dyn IcebergCatalog>
    } else if catalog_uri == "sqlite://"
        || catalog_uri.contains(":memory:")
        || catalog_uri == "memory://"
    {
        // In-memory SQLite catalog (also handle memory:// for compatibility)
        let catalog = SqlCatalog::new("sqlite::memory:", catalog_name, object_store_builder)
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
