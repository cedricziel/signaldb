use anyhow::Result;

use iceberg_rust::catalog::Catalog as IcebergRustCatalog;
use std::sync::Arc;

/// Connection pool configuration for catalog operations
#[derive(Debug, Clone)]
pub struct CatalogPoolConfig {
    /// Minimum number of connections in pool
    pub min_connections: u32,
    /// Maximum number of connections in pool
    pub max_connections: u32,
    /// Connection timeout in milliseconds
    pub connection_timeout_ms: u64,
    /// Idle timeout in seconds
    pub idle_timeout_seconds: u64,
    /// Maximum lifetime of connections in seconds
    pub max_lifetime_seconds: u64,
}

impl Default for CatalogPoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 2,
            max_connections: 10,
            connection_timeout_ms: 5000,
            idle_timeout_seconds: 300,  // 5 minutes
            max_lifetime_seconds: 1800, // 30 minutes
        }
    }
}

/// Create a JanKaul SQL catalog for use with DataFusion
/// This replaces the complex catalog adapter approach with a simpler pattern
pub async fn create_jankaul_sql_catalog(
    catalog_uri: &str,
    catalog_name: &str,
) -> Result<Arc<dyn IcebergRustCatalog>> {
    create_jankaul_sql_catalog_with_pool(catalog_uri, catalog_name, None).await
}

/// Create a JanKaul SQL catalog with connection pooling for production performance
pub async fn create_jankaul_sql_catalog_with_pool(
    catalog_uri: &str,
    catalog_name: &str,
    pool_config: Option<CatalogPoolConfig>,
) -> Result<Arc<dyn IcebergRustCatalog>> {
    log::info!("Creating JanKaul SQL catalog with URI: {catalog_uri}");

    use iceberg_rust::object_store::ObjectStoreBuilder;
    use iceberg_sql_catalog::SqlCatalog;

    // Create an in-memory object store builder
    let object_store_builder = ObjectStoreBuilder::memory();

    let catalog = if catalog_uri.starts_with("sqlite://") && catalog_uri != "sqlite://" {
        // Use connection pooling for persistent SQLite databases
        let pool_config = pool_config.unwrap_or_default();

        log::info!(
            "Creating pooled SQL catalog with config: min={}, max={}, timeout={}ms",
            pool_config.min_connections,
            pool_config.max_connections,
            pool_config.connection_timeout_ms
        );

        create_pooled_sql_catalog(catalog_uri, catalog_name, object_store_builder, pool_config)
            .await?
    } else {
        // Use direct connection for in-memory databases
        log::info!("Creating direct SQL catalog for in-memory database");

        SqlCatalog::new(catalog_uri, catalog_name, object_store_builder)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create SQL catalog: {}", e))?
    };

    let catalog_arc: Arc<dyn IcebergRustCatalog> = Arc::new(catalog);

    log::info!("Successfully created JanKaul SQL catalog: {catalog_name}");
    Ok(catalog_arc)
}

/// Create a pooled SQL catalog for production workloads
async fn create_pooled_sql_catalog(
    catalog_uri: &str,
    catalog_name: &str,
    object_store_builder: iceberg_rust::object_store::ObjectStoreBuilder,
    pool_config: CatalogPoolConfig,
) -> Result<iceberg_sql_catalog::SqlCatalog> {
    use iceberg_sql_catalog::SqlCatalog;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use std::time::Duration;

    // Parse SQLite connection options
    let connect_options = catalog_uri
        .parse::<SqliteConnectOptions>()
        .map_err(|e| anyhow::anyhow!("Invalid SQLite connection URI: {}", e))?;

    // TODO: Connection pooling implementation pending iceberg_sql_catalog support
    // The iceberg_sql_catalog (v0.8.0) currently doesn't expose a way to use an external
    // sqlx::Pool. The pool creation code below is ready for when this functionality
    // is added to the library. For now, we create the pool to validate configuration
    // but cannot use it with SqlCatalog::new().
    //
    // Future implementation should:
    // 1. Check if iceberg_sql_catalog has added pool support
    // 2. Use a constructor like SqlCatalog::with_pool(pool, catalog_name, object_store_builder)
    // 3. Remove the direct SqlCatalog::new() call below

    // Create connection pool with optimized settings (currently unused due to library limitations)
    let _pool = SqlitePoolOptions::new()
        .min_connections(pool_config.min_connections)
        .max_connections(pool_config.max_connections)
        .acquire_timeout(Duration::from_millis(pool_config.connection_timeout_ms))
        .idle_timeout(Duration::from_secs(pool_config.idle_timeout_seconds))
        .max_lifetime(Duration::from_secs(pool_config.max_lifetime_seconds))
        .test_before_acquire(true) // Test connections before use
        .connect_with(connect_options)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create connection pool: {}", e))?;

    log::info!(
        "Created SQLite connection pool with {} connections for catalog '{}' (awaiting library support)",
        pool_config.max_connections,
        catalog_name
    );

    // FIXME: Replace with pooled catalog creation once iceberg_sql_catalog supports it
    // Currently falls back to non-pooled connection
    SqlCatalog::new(catalog_uri, catalog_name, object_store_builder)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create SQL catalog: {}", e))
}
