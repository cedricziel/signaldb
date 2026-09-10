/// Common test utilities and helpers for integration tests
use common::config::Configuration;
use std::sync::Arc;
use tempfile::TempDir;

pub mod compaction_helpers;
pub mod fixtures;
pub mod generators;
pub mod ordering;
pub mod test_helpers;

/// Create a test configuration with temporary directories
pub fn create_test_config() -> (Configuration, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let mut config = Configuration::default();

    // Use memory storage for tests
    config.storage.dsn = "memory://".to_string();
    config.schema.catalog_type = "memory".to_string();
    config.schema.catalog_uri = "memory://".to_string();

    (config, temp_dir)
}

/// Initialize test logging
pub fn init_test_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

/// Common test fixture for object store
pub fn create_test_object_store() -> Arc<dyn object_store::ObjectStore> {
    Arc::new(object_store::memory::InMemory::new())
}
