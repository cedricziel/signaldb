use anyhow::Result;
use object_store::{local::LocalFileSystem, memory::InMemory, ObjectStore};
use std::sync::Arc;
use url::Url;

use crate::config::StorageConfig;

/// Create an object store from storage configuration
pub fn create_object_store(storage_config: &StorageConfig) -> Result<Arc<dyn ObjectStore>> {
    create_object_store_from_dsn(&storage_config.dsn)
}

/// Create an object store from a DSN string
pub fn create_object_store_from_dsn(dsn: &str) -> Result<Arc<dyn ObjectStore>> {
    let url =
        Url::parse(dsn).map_err(|e| anyhow::anyhow!("Invalid storage DSN '{}': {}", dsn, e))?;

    match url.scheme() {
        "file" => {
            // Extract path from file:// URL
            let path = url.path();
            if path.is_empty() || path == "/" {
                return Err(anyhow::anyhow!(
                    "File DSN must specify a path: file:///path/to/storage"
                ));
            }
            Ok(Arc::new(LocalFileSystem::new_with_prefix(path)?))
        }
        "memory" => Ok(Arc::new(InMemory::new())),
        scheme => Err(anyhow::anyhow!(
            "Unsupported storage scheme: {}. Supported: file, memory",
            scheme
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_memory_object_store() {
        let object_store = create_object_store_from_dsn("memory://").unwrap();
        assert!(Arc::strong_count(&object_store) == 1);
    }

    #[test]
    fn test_create_filesystem_object_store() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy();
        let dsn = format!("file://{}", path);

        let object_store = create_object_store_from_dsn(&dsn).unwrap();
        assert!(Arc::strong_count(&object_store) == 1);
    }

    #[test]
    fn test_create_object_store_from_config() {
        let storage_config = StorageConfig {
            dsn: "memory://".to_string(),
        };

        let object_store = create_object_store(&storage_config).unwrap();
        assert!(Arc::strong_count(&object_store) == 1);
    }

    #[test]
    fn test_invalid_dsn() {
        let result = create_object_store_from_dsn("not-a-url");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid storage DSN"));
    }

    #[test]
    fn test_unsupported_scheme() {
        let result = create_object_store_from_dsn("s3://bucket/prefix");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported storage scheme"));
    }

    #[test]
    fn test_file_dsn_without_path() {
        let result = create_object_store_from_dsn("file://");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("File DSN must specify a path"));
    }
}
