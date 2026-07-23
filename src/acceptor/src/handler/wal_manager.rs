//! WAL Manager for multi-tenant write-ahead log isolation
//!
//! This module provides WalManager which creates and caches WAL instances
//! per tenant/dataset/signal type combination, ensuring data isolation.

use common::wal::{Wal, WalConfig};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Key for WAL cache: (tenant_id, dataset_id, signal_type)
pub type WalKey = (String, String, String);

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
    /// Per-key initialization guards to prevent duplicate WAL creation
    init_guards: Arc<Mutex<HashMap<WalKey, Arc<Mutex<()>>>>>,
    /// Base configuration template for trace WALs
    traces_config: WalConfig,
    /// Base configuration template for log WALs
    logs_config: WalConfig,
    /// Base configuration template for metrics WALs
    metrics_config: WalConfig,
    /// Base configuration template for profile WALs
    profiles_config: WalConfig,
}

impl WalManager {
    /// Create a new WalManager with base configurations for each signal type
    ///
    /// # Arguments
    ///
    /// * `traces_config` - Base WAL configuration for traces
    /// * `logs_config` - Base WAL configuration for logs
    /// * `metrics_config` - Base WAL configuration for metrics
    /// * `profiles_config` - Base WAL configuration for profiles
    ///
    /// The `wal_dir` in each config should point to the base directory (e.g., `.wal`).
    /// The manager will create subdirectories per tenant/dataset/signal.
    pub fn new(
        traces_config: WalConfig,
        logs_config: WalConfig,
        metrics_config: WalConfig,
        profiles_config: WalConfig,
    ) -> Self {
        Self {
            wals: Arc::new(Mutex::new(HashMap::new())),
            init_guards: Arc::new(Mutex::new(HashMap::new())),
            traces_config,
            logs_config,
            metrics_config,
            profiles_config,
        }
    }

    /// Get or create a WAL for the given tenant, dataset, and signal type
    ///
    /// # Arguments
    ///
    /// * `tenant_id` - The tenant identifier
    /// * `dataset_id` - The dataset identifier
    /// * `signal_type` - The signal type ("traces", "logs", "metrics", or "profiles")
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

        // Fast path: check cache first without per-key guard
        {
            let wals = self.wals.lock().await;
            if let Some(wal) = wals.get(&key) {
                log::debug!(
                    "Reusing existing WAL for tenant='{tenant_id}', dataset='{dataset_id}', signal='{signal_type}'"
                );
                return Ok(wal.clone());
            }
        }

        // Get or create per-key initialization guard
        let init_guard = {
            let mut guards = self.init_guards.lock().await;
            guards
                .entry(key.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };

        // Acquire per-key lock to serialize initialization for this specific key
        let _guard = init_guard.lock().await;

        // Double-check cache after acquiring per-key lock (another thread might have created it)
        {
            let wals = self.wals.lock().await;
            if let Some(wal) = wals.get(&key) {
                log::debug!(
                    "WAL created by concurrent thread for tenant='{tenant_id}', dataset='{dataset_id}', signal='{signal_type}'"
                );
                return Ok(wal.clone());
            }
        }

        // Create new WAL - we now have exclusive access for this key
        log::info!(
            "Creating new WAL for tenant='{tenant_id}', dataset='{dataset_id}', signal='{signal_type}'"
        );

        // Get appropriate base config for this signal type
        let base_config = match signal_type {
            "traces" => &self.traces_config,
            "logs" => &self.logs_config,
            "metrics" => &self.metrics_config,
            "profiles" => &self.profiles_config,
            _ => {
                // Remove guard on failure
                self.init_guards.lock().await.remove(&key);
                return Err(anyhow::anyhow!(
                    "Unknown signal type: {signal_type}. Must be 'traces', 'logs', 'metrics', or 'profiles'"
                ));
            }
        };

        // Create tenant/dataset-specific config
        let wal_config = base_config.for_tenant_dataset(tenant_id, dataset_id, signal_type);

        // Initialize WAL
        let mut wal = match Wal::new(wal_config).await {
            Ok(wal) => wal,
            Err(e) => {
                // Remove guard on failure
                self.init_guards.lock().await.remove(&key);
                return Err(e);
            }
        };

        // Start background flush for this WAL
        wal.start_background_flush();

        let wal = Arc::new(wal);

        // Cache the WAL
        {
            let mut wals = self.wals.lock().await;
            wals.insert(key.clone(), wal.clone());
        }

        // Clean up the per-key guard (optional, but prevents unbounded growth)
        self.init_guards.lock().await.remove(&key);

        log::info!(
            "Successfully created WAL for tenant='{tenant_id}', dataset='{dataset_id}', signal='{signal_type}'"
        );

        Ok(wal)
    }

    /// Get the number of cached WAL instances
    ///
    /// Useful for monitoring and debugging.
    pub async fn wal_count(&self) -> usize {
        self.wals.lock().await.len()
    }

    /// Snapshot of all cached WAL instances with their keys
    ///
    /// Used by the WAL retry consumer to scan every tenant/dataset/signal
    /// WAL for unprocessed entries.
    pub async fn all_wals(&self) -> Vec<(WalKey, Arc<Wal>)> {
        self.wals
            .lock()
            .await
            .iter()
            .map(|(key, wal)| (key.clone(), wal.clone()))
            .collect()
    }

    /// Discover WAL directories left on disk by previous runs and open them
    ///
    /// WALs are created lazily on first write, so after a restart a WAL with
    /// pending entries would not be in the cache until new traffic arrives
    /// for that tenant/dataset/signal — and entries from the previous run
    /// would never be retried. This scans the base WAL directories for
    /// `{tenant}/{dataset}/{signal}` layouts and opens each one.
    ///
    /// Returns the number of newly opened WAL instances.
    pub async fn discover_existing_wals(&self) -> Result<usize, anyhow::Error> {
        let mut base_dirs = Vec::new();
        for config in [
            &self.traces_config,
            &self.logs_config,
            &self.metrics_config,
            &self.profiles_config,
        ] {
            if !base_dirs.contains(&config.wal_dir) {
                base_dirs.push(config.wal_dir.clone());
            }
        }

        let mut opened = 0;
        for base_dir in base_dirs {
            if !base_dir.is_dir() {
                continue;
            }
            for (tenant, dataset, signal) in Self::scan_wal_layout(&base_dir).await? {
                let already_cached = {
                    let wals = self.wals.lock().await;
                    wals.contains_key(&(tenant.clone(), dataset.clone(), signal.clone()))
                };
                if already_cached {
                    continue;
                }
                match self.get_wal(&tenant, &dataset, &signal).await {
                    Ok(_) => {
                        opened += 1;
                        log::info!(
                            "Discovered existing WAL for tenant='{tenant}', dataset='{dataset}', signal='{signal}'"
                        );
                    }
                    Err(e) => {
                        log::warn!(
                            "Failed to open discovered WAL for tenant='{tenant}', dataset='{dataset}', signal='{signal}': {e}"
                        );
                    }
                }
            }
        }

        Ok(opened)
    }

    /// Scan a base WAL directory for `{tenant}/{dataset}/{signal}` triples
    /// that contain WAL segment files.
    async fn scan_wal_layout(
        base_dir: &std::path::Path,
    ) -> Result<Vec<(String, String, String)>, anyhow::Error> {
        const SIGNALS: [&str; 4] = ["traces", "logs", "metrics", "profiles"];
        let mut found = Vec::new();

        let mut tenants = tokio::fs::read_dir(base_dir).await?;
        while let Some(tenant_entry) = tenants.next_entry().await? {
            if !tenant_entry.file_type().await?.is_dir() {
                continue;
            }
            let Some(tenant) = tenant_entry.file_name().to_str().map(String::from) else {
                continue;
            };

            let mut datasets = tokio::fs::read_dir(tenant_entry.path()).await?;
            while let Some(dataset_entry) = datasets.next_entry().await? {
                if !dataset_entry.file_type().await?.is_dir() {
                    continue;
                }
                let Some(dataset) = dataset_entry.file_name().to_str().map(String::from) else {
                    continue;
                };

                for signal in SIGNALS {
                    let signal_dir = dataset_entry.path().join(signal);
                    if !signal_dir.is_dir() {
                        continue;
                    }
                    // Only open directories that actually contain WAL segments
                    let mut has_segments = false;
                    let mut files = tokio::fs::read_dir(&signal_dir).await?;
                    while let Some(file) = files.next_entry().await? {
                        if let Some(name) = file.file_name().to_str()
                            && name.starts_with("wal-")
                            && name.ends_with(".log")
                        {
                            has_segments = true;
                            break;
                        }
                    }
                    if has_segments {
                        found.push((tenant.clone(), dataset.clone(), signal.to_string()));
                    }
                }
            }
        }

        Ok(found)
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
    async fn test_wal_manager_creates_profiles_wal() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let manager = WalManager::new(
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
        );

        let _wal = manager
            .get_wal("acme", "production", "profiles")
            .await
            .unwrap();

        let expected_path = base_path.join("acme").join("production").join("profiles");
        assert!(expected_path.exists());
    }

    #[tokio::test]
    async fn test_wal_manager_invalid_signal_type() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let manager = WalManager::new(
            create_test_config(&base_path),
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

    #[tokio::test]
    async fn test_wal_manager_concurrent_initialization_no_duplicates() {
        use std::sync::Arc;
        use tokio::task::JoinSet;

        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let manager = Arc::new(WalManager::new(
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
        ));

        // Spawn 10 concurrent tasks all trying to get the same WAL
        let mut join_set = JoinSet::new();

        for i in 0..10 {
            let manager_clone = Arc::clone(&manager);
            join_set.spawn(async move {
                let wal = manager_clone
                    .get_wal("acme", "production", "traces")
                    .await
                    .unwrap();
                (i, wal)
            });
        }

        // Collect all results
        let mut wals = Vec::new();
        while let Some(result) = join_set.join_next().await {
            let (_task_id, wal) = result.unwrap();
            wals.push(wal);
        }

        // All 10 tasks should have gotten the same WAL instance
        assert_eq!(wals.len(), 10);

        // Verify all WALs are the same instance (same Arc pointer)
        for i in 1..wals.len() {
            assert!(
                Arc::ptr_eq(&wals[0], &wals[i]),
                "WAL at index {i} is not the same instance as WAL at index 0"
            );
        }

        // Verify only 1 WAL was created
        assert_eq!(manager.wal_count().await, 1);
    }
}
