//! WAL Manager for multi-tenant write-ahead log isolation
//!
//! This module provides WalManager which creates and caches WAL instances
//! per tenant/dataset/signal type combination, ensuring data isolation.

use common::wal::{Wal, WalConfig};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Key for WAL cache: (tenant_id, dataset_id, signal_type)
type WalKey = (String, String, String);

/// Manager for creating and caching per-tenant/dataset WAL instances
///
/// The WalManager ensures that each unique combination of tenant_id, dataset_id,
/// and signal_type gets its own isolated WAL instance. WALs are created lazily
/// on first access and cached for reuse.
///
/// WAL paths follow the pattern: `.wal/{tenant}/{dataset}/{signal}/`
pub struct WalManager {
    /// Cache of WAL instances keyed by (tenant_id, dataset_id, signal_type)
    wals: Arc<Mutex<HashMap<WalKey, Arc<Wal>>>>,
    /// Base configuration template for trace WALs
    traces_config: WalConfig,
    /// Base configuration template for log WALs
    logs_config: WalConfig,
    /// Base configuration template for metrics WALs
    metrics_config: WalConfig,
}

impl WalManager {
    /// Create a new WalManager with base configurations for each signal type
    ///
    /// # Arguments
    ///
    /// * `traces_config` - Base WAL configuration for traces
    /// * `logs_config` - Base WAL configuration for logs
    /// * `metrics_config` - Base WAL configuration for metrics
    ///
    /// The `wal_dir` in each config should point to the base directory (e.g., `.wal`).
    /// The manager will create subdirectories per tenant/dataset/signal.
    pub fn new(
        traces_config: WalConfig,
        logs_config: WalConfig,
        metrics_config: WalConfig,
    ) -> Self {
        Self {
            wals: Arc::new(Mutex::new(HashMap::new())),
            traces_config,
            logs_config,
            metrics_config,
        }
    }

    /// Get or create a WAL for the given tenant, dataset, and signal type
    ///
    /// # Arguments
    ///
    /// * `tenant_id` - The tenant identifier
    /// * `dataset_id` - The dataset identifier
    /// * `signal_type` - The signal type ("traces", "logs", or "metrics")
    ///
    /// # Returns
    ///
    /// An Arc<Wal> for the specified tenant/dataset/signal combination.
    /// The WAL is created if it doesn't exist, otherwise the cached instance is returned.
    ///
    /// # Errors
    ///
    /// Returns an error if WAL initialization fails.
    pub async fn get_wal(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        signal_type: &str,
    ) -> Result<Arc<Wal>, anyhow::Error> {
        let key = (
            tenant_id.to_string(),
            dataset_id.to_string(),
            signal_type.to_string(),
        );

        // Check cache first
        {
            let wals = self.wals.lock().await;
            if let Some(wal) = wals.get(&key) {
                log::debug!(
                    "Reusing existing WAL for tenant='{}', dataset='{}', signal='{}'",
                    tenant_id,
                    dataset_id,
                    signal_type
                );
                return Ok(wal.clone());
            }
        }

        // Create new WAL if not in cache
        log::info!(
            "Creating new WAL for tenant='{}', dataset='{}', signal='{}'",
            tenant_id,
            dataset_id,
            signal_type
        );

        // Get appropriate base config for this signal type
        let base_config = match signal_type {
            "traces" => &self.traces_config,
            "logs" => &self.logs_config,
            "metrics" => &self.metrics_config,
            _ => {
                return Err(anyhow::anyhow!(
                    "Unknown signal type: {}. Must be 'traces', 'logs', or 'metrics'",
                    signal_type
                ));
            }
        };

        // Create tenant/dataset-specific config
        let wal_config = base_config.for_tenant_dataset(tenant_id, dataset_id, signal_type);

        // Initialize WAL
        let mut wal = Wal::new(wal_config).await?;

        // Start background flush for this WAL
        wal.start_background_flush();

        let wal = Arc::new(wal);

        // Cache the WAL
        let mut wals = self.wals.lock().await;
        wals.insert(key, wal.clone());

        log::info!(
            "Successfully created WAL for tenant='{}', dataset='{}', signal='{}'",
            tenant_id,
            dataset_id,
            signal_type
        );

        Ok(wal)
    }

    /// Get the number of cached WAL instances
    ///
    /// Useful for monitoring and debugging.
    pub async fn wal_count(&self) -> usize {
        self.wals.lock().await.len()
    }

    /// Clear all cached WAL instances
    ///
    /// This will drop all WAL references. WALs will be recreated on next access.
    /// Note: This doesn't delete WAL files on disk, just clears the in-memory cache.
    #[allow(dead_code)]
    pub async fn clear_cache(&self) {
        let mut wals = self.wals.lock().await;
        wals.clear();
        log::info!("Cleared all cached WAL instances");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::TempDir;

    fn create_test_config(base_dir: &Path) -> WalConfig {
        let mut config = WalConfig::with_defaults(base_dir.to_path_buf());
        config.max_segment_size = 1024 * 1024; // 1MB for tests
        config.max_buffer_entries = 100;
        config.flush_interval_secs = 60;
        config
    }

    #[tokio::test]
    async fn test_wal_manager_creates_wal() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let manager = WalManager::new(
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
        );

        let _wal = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();

        // Verify WAL was created successfully
        // We can't access the config directly since it's private,
        // but we can verify the path was created on disk
        let expected_path = base_path.join("acme").join("production").join("traces");
        assert!(expected_path.exists());
    }

    #[tokio::test]
    async fn test_wal_manager_caches_wals() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let manager = WalManager::new(
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
        );

        // Get WAL first time
        let wal1 = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();

        // Get WAL second time - should be cached
        let wal2 = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();

        // Should be the same instance
        assert!(Arc::ptr_eq(&wal1, &wal2));

        // WAL count should be 1
        assert_eq!(manager.wal_count().await, 1);
    }

    #[tokio::test]
    async fn test_wal_manager_isolates_tenants() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let manager = WalManager::new(
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
        );

        let wal_acme = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();

        let wal_globex = manager
            .get_wal("globex", "production", "traces")
            .await
            .unwrap();

        // Different tenants should have different WALs
        assert!(!Arc::ptr_eq(&wal_acme, &wal_globex));

        // Verify different paths were created
        let path_acme = base_path.join("acme").join("production").join("traces");
        let path_globex = base_path.join("globex").join("production").join("traces");
        assert!(path_acme.exists());
        assert!(path_globex.exists());

        // Should have 2 WALs cached
        assert_eq!(manager.wal_count().await, 2);
    }

    #[tokio::test]
    async fn test_wal_manager_isolates_datasets() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let manager = WalManager::new(
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
        );

        let wal_prod = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();

        let wal_staging = manager.get_wal("acme", "staging", "traces").await.unwrap();

        // Different datasets should have different WALs
        assert!(!Arc::ptr_eq(&wal_prod, &wal_staging));

        // Verify different paths were created
        let path_prod = base_path.join("acme").join("production").join("traces");
        let path_staging = base_path.join("acme").join("staging").join("traces");
        assert!(path_prod.exists());
        assert!(path_staging.exists());

        // Should have 2 WALs cached
        assert_eq!(manager.wal_count().await, 2);
    }

    #[tokio::test]
    async fn test_wal_manager_isolates_signals() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let manager = WalManager::new(
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
        );

        let wal_traces = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();

        let wal_logs = manager.get_wal("acme", "production", "logs").await.unwrap();

        // Different signal types should have different WALs
        assert!(!Arc::ptr_eq(&wal_traces, &wal_logs));

        // Verify different paths were created
        let path_traces = base_path.join("acme").join("production").join("traces");
        let path_logs = base_path.join("acme").join("production").join("logs");
        assert!(path_traces.exists());
        assert!(path_logs.exists());

        // Should have 2 WALs cached
        assert_eq!(manager.wal_count().await, 2);
    }

    #[tokio::test]
    async fn test_wal_manager_invalid_signal_type() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let manager = WalManager::new(
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
        );

        let result = manager.get_wal("acme", "production", "invalid").await;

        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("Unknown signal type")
        );
    }

    #[tokio::test]
    async fn test_wal_manager_clear_cache() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let manager = WalManager::new(
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
        );

        // Create some WALs
        manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();
        manager.get_wal("acme", "production", "logs").await.unwrap();

        assert_eq!(manager.wal_count().await, 2);

        // Clear cache
        manager.clear_cache().await;

        assert_eq!(manager.wal_count().await, 0);

        // Getting WAL again should create new instance
        manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();
        assert_eq!(manager.wal_count().await, 1);
    }
}
