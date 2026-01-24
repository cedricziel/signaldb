use anyhow::Result;
use object_store::{ObjectStore, aws::AmazonS3Builder, local::LocalFileSystem, memory::InMemory};
use std::sync::Arc;
use url::Url;

use crate::config::StorageConfig;

/// Create an object store from storage configuration
pub fn create_object_store(storage_config: &StorageConfig) -> Result<Arc<dyn ObjectStore>> {
    create_object_store_from_dsn(&storage_config.dsn)
}

/// Extract the filesystem path from a storage DSN
/// Returns the path component without the URL scheme for file:// URLs,
/// or the original DSN for other schemes
///
/// # Examples
/// ```
/// use common::storage::storage_dsn_to_path;
///
/// assert_eq!(storage_dsn_to_path("file:///.data/storage").unwrap(), ".data/storage");
/// assert_eq!(storage_dsn_to_path("file:///tmp/data").unwrap(), "/tmp/data");
/// assert_eq!(storage_dsn_to_path("memory://").unwrap(), "memory://");
/// ```
pub fn storage_dsn_to_path(dsn: &str) -> Result<String> {
    let url =
        Url::parse(dsn).map_err(|e| anyhow::anyhow!("Invalid storage DSN '{}': {}", dsn, e))?;

    match url.scheme() {
        "file" => {
            let path = url.path();
            if path.is_empty() || path == "/" {
                return Err(anyhow::anyhow!(
                    "File DSN must specify a path: file:///path/to/storage"
                ));
            }
            // Remove leading slash for relative paths like /.data/storage -> .data/storage
            // Keep leading slash for absolute paths like /tmp/data -> /tmp/data
            let path = if path.starts_with("/.") {
                &path[1..]
            } else {
                path
            };
            Ok(path.to_string())
        }
        "memory" => Ok("memory://".to_string()),
        "s3" => Ok(dsn.to_string()), // Keep S3 URLs as-is
        scheme => Err(anyhow::anyhow!(
            "Unsupported storage scheme: {}. Supported: file, memory, s3",
            scheme
        )),
    }
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
        "s3" => {
            let builder = create_s3_builder_from_dsn(&url)?;
            Ok(Arc::new(builder.build()?))
        }
        scheme => Err(anyhow::anyhow!(
            "Unsupported storage scheme: {}. Supported: file, memory, s3",
            scheme
        )),
    }
}

/// Create an S3 builder from a DSN
/// DSN format: s3://[access_key:secret_key@]host[:port]/bucket
pub fn create_s3_builder_from_dsn(dsn: &Url) -> Result<AmazonS3Builder> {
    let host = dsn
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("Missing S3 host in DSN"))?;
    let port = dsn.port();
    let bucket = dsn.path().trim_start_matches('/');

    if bucket.is_empty() {
        return Err(anyhow::anyhow!(
            "S3 DSN must specify a bucket: s3://host/bucket"
        ));
    }

    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region("us-east-1"); // Default region

    // Extract credentials from DSN if present
    let access_key = dsn.username();
    let secret_key = dsn.password().unwrap_or("");

    if !access_key.is_empty() {
        builder = builder
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key);
    }

    // Determine if this is real S3 or S3-compatible (MinIO, etc)
    let endpoint = if host.contains("amazonaws.com") {
        // Real S3 - no custom endpoint needed
        None
    } else {
        // S3-compatible - need custom endpoint
        let scheme = if port == Some(443) { "https" } else { "http" };
        Some(match port {
            Some(p) => format!("{scheme}://{host}:{p}"),
            None => format!("{scheme}://{host}"),
        })
    };

    if let Some(endpoint) = endpoint {
        builder = builder
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false); // MinIO requires path-style URLs
    }

    // Check environment for AWS credentials if not in DSN
    if access_key.is_empty() {
        if let Ok(env_key) = std::env::var("AWS_ACCESS_KEY_ID") {
            builder = builder.with_access_key_id(env_key);
        }
        if let Ok(env_secret) = std::env::var("AWS_SECRET_ACCESS_KEY") {
            builder = builder.with_secret_access_key(env_secret);
        }
        if let Ok(env_region) = std::env::var("AWS_DEFAULT_REGION") {
            builder = builder.with_region(env_region);
        }
    }

    Ok(builder)
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
        let dsn = format!("file://{path}");

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
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid storage DSN")
        );
    }

    #[test]
    fn test_unsupported_scheme() {
        let result = create_object_store_from_dsn("gcs://bucket/prefix");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unsupported storage scheme")
        );
    }

    #[test]
    fn test_s3_dsn_parsing() {
        // Test basic S3 DSN
        let result = create_s3_builder_from_dsn(
            &Url::parse("s3://mybucket.s3.amazonaws.com/prefix").unwrap(),
        );
        assert!(result.is_ok());

        // Test S3-compatible DSN with credentials
        let result = create_s3_builder_from_dsn(
            &Url::parse("s3://access:secret@localhost:9000/bucket").unwrap(),
        );
        assert!(result.is_ok());

        // Test missing bucket
        let result = create_s3_builder_from_dsn(&Url::parse("s3://localhost:9000/").unwrap());
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("must specify a bucket")
        );
    }

    #[test]
    fn test_file_dsn_without_path() {
        let result = create_object_store_from_dsn("file://");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("File DSN must specify a path")
        );
    }

    #[test]
    fn test_storage_dsn_to_path_relative() {
        // Relative path starting with dot
        let result = storage_dsn_to_path("file:///.data/storage").unwrap();
        assert_eq!(result, ".data/storage");
    }

    #[test]
    fn test_storage_dsn_to_path_absolute() {
        // Absolute path
        let result = storage_dsn_to_path("file:///tmp/data").unwrap();
        assert_eq!(result, "/tmp/data");
    }

    #[test]
    fn test_storage_dsn_to_path_memory() {
        // Memory scheme
        let result = storage_dsn_to_path("memory://").unwrap();
        assert_eq!(result, "memory://");
    }

    #[test]
    fn test_storage_dsn_to_path_s3() {
        // S3 scheme - keep as-is
        let dsn = "s3://bucket/prefix";
        let result = storage_dsn_to_path(dsn).unwrap();
        assert_eq!(result, dsn);
    }

    #[test]
    fn test_storage_dsn_to_path_invalid() {
        // Invalid DSN
        let result = storage_dsn_to_path("not-a-url");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid storage DSN")
        );
    }

    #[test]
    fn test_storage_dsn_to_path_no_path() {
        // File URL without path
        let result = storage_dsn_to_path("file://");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("File DSN must specify a path")
        );
    }
}
