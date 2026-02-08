//! Storage test context for integration testing
//!
//! Provides both MinIO (S3-compatible) and in-memory storage contexts for tests.

use anyhow::Result;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use std::sync::Arc;

/// Test context for object storage operations
pub struct StorageTestContext {
    pub object_store: Arc<dyn ObjectStore>,
    #[allow(dead_code)] // Used to keep container alive during test
    _storage_mode: StorageMode,
}

enum StorageMode {
    InMemory,
    // Future: add MinIO testcontainer support
    // MinIO { container: ContainerAsync<MinIO>, dsn: Url },
}

impl StorageTestContext {
    /// Creates in-memory storage for fast tests
    ///
    /// This is the preferred mode for unit-style integration tests.
    pub async fn new_in_memory() -> Result<Self> {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        Ok(Self {
            object_store,
            _storage_mode: StorageMode::InMemory,
        })
    }

    /// Creates a new storage test context (defaults to in-memory)
    ///
    /// For now, this is an alias for `new_in_memory`. In the future,
    /// this may be extended to support MinIO via testcontainers.
    pub async fn new() -> Result<Self> {
        Self::new_in_memory().await
    }

    /// Lists all objects in storage (for verification)
    pub async fn list_all_objects(&self) -> Result<Vec<String>> {
        let mut paths = Vec::new();
        let list_result = self.object_store.list(None);

        use futures::StreamExt;
        let mut stream = Box::pin(list_result);
        while let Some(meta_result) = stream.next().await {
            let meta = meta_result?;
            paths.push(meta.location.to_string());
        }

        Ok(paths)
    }

    /// Gets object count for a specific prefix
    pub async fn count_objects(&self, prefix: &str) -> Result<usize> {
        let prefix_path = object_store::path::Path::from(prefix);
        let list_result = self.object_store.list(Some(&prefix_path));

        use futures::StreamExt;
        let mut stream = Box::pin(list_result);
        let mut count = 0;
        while let Some(meta_result) = stream.next().await {
            meta_result?;
            count += 1;
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use object_store::path::Path;

    #[tokio::test]
    async fn test_create_in_memory_storage() -> Result<()> {
        let ctx = StorageTestContext::new_in_memory().await?;

        // Write a test object
        let path = Path::from("test/file.txt");
        let data = Bytes::from("test data");
        ctx.object_store.put(&path, data.into()).await?;

        // Verify we can list it
        let objects = ctx.list_all_objects().await?;
        assert_eq!(objects.len(), 1);
        assert!(objects[0].contains("test/file.txt"));

        Ok(())
    }

    #[tokio::test]
    async fn test_count_objects_with_prefix() -> Result<()> {
        let ctx = StorageTestContext::new_in_memory().await?;

        // Write multiple objects with different prefixes
        for i in 0..5 {
            let path = Path::from(format!("tenant1/data/{}.parquet", i));
            ctx.object_store
                .put(&path, Bytes::from("data").into())
                .await?;
        }

        for i in 0..3 {
            let path = Path::from(format!("tenant2/data/{}.parquet", i));
            ctx.object_store
                .put(&path, Bytes::from("data").into())
                .await?;
        }

        // Count objects for tenant1
        let tenant1_count = ctx.count_objects("tenant1/").await?;
        assert_eq!(tenant1_count, 5);

        // Count objects for tenant2
        let tenant2_count = ctx.count_objects("tenant2/").await?;
        assert_eq!(tenant2_count, 3);

        Ok(())
    }
}
