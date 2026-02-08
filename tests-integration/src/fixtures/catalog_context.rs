//! Catalog test context for integration testing
//!
//! Provides both PostgreSQL-backed and in-memory catalog contexts for tests.

use anyhow::Result;
use common::catalog_manager::CatalogManager;
use std::sync::Arc;

/// Test context for catalog operations
pub struct CatalogTestContext {
    pub catalog_manager: Arc<CatalogManager>,
    #[allow(dead_code)] // Used to keep container alive during test
    _storage_mode: StorageMode,
}

enum StorageMode {
    InMemory,
    // Future: add PostgreSQL testcontainer support
    // Postgres { container: ContainerAsync<Postgres>, dsn: String },
}

impl CatalogTestContext {
    /// Creates an in-memory catalog for fast tests
    ///
    /// This is the preferred mode for unit-style integration tests as it's fast
    /// and doesn't require external dependencies.
    pub async fn new_in_memory() -> Result<Self> {
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await?);

        Ok(Self {
            catalog_manager,
            _storage_mode: StorageMode::InMemory,
        })
    }

    /// Creates a new catalog test context (defaults to in-memory)
    ///
    /// For now, this is an alias for `new_in_memory`. In the future,
    /// this may be extended to support PostgreSQL via testcontainers.
    pub async fn new() -> Result<Self> {
        Self::new_in_memory().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_in_memory_catalog() -> Result<()> {
        let ctx = CatalogTestContext::new_in_memory().await?;
        assert!(Arc::strong_count(&ctx.catalog_manager) >= 1);
        Ok(())
    }
}
