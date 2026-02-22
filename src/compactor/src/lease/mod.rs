//! Distributed lease management for compactor coordination.
//!
//! When multiple compactor instances run simultaneously, leases prevent duplicate
//! work by ensuring only one instance compacts a given partition at a time.
//!
//! ## Algorithm
//!
//! Leases are stored in the shared SQL catalog (`compactor_leases` table).
//! Acquisition uses an atomic `INSERT … ON CONFLICT DO UPDATE WHERE expires_at < now`
//! so that:
//!
//! - Only one instance holds a non-expired lease at a time
//! - Expired leases (from crashed instances) are automatically taken over
//! - All operations are single round-trips to the database (no two-phase locking)
//!
//! ## Usage
//!
//! ```no_run
//! use std::sync::Arc;
//! use std::time::Duration;
//! use uuid::Uuid;
//! use compactor::lease::LeaseManager;
//!
//! # async fn example() -> anyhow::Result<()> {
//! # let catalog = todo!();
//! # let candidate = todo!();
//! let manager = LeaseManager::new(catalog, Uuid::new_v4(), Duration::from_secs(300));
//!
//! if let Some(lease) = manager.try_acquire(&candidate, Duration::from_secs(300)).await? {
//!     // do compaction work …
//!     manager.release(&lease).await?;
//! }
//! # Ok(())
//! # }
//! ```

use crate::planner::CompactionCandidate;
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use common::catalog::{Catalog, CompactorLease};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// An active lease on a compaction work unit.
///
/// Drop (or explicitly call `LeaseManager::release`) when work is complete.
#[derive(Debug, Clone)]
pub struct Lease {
    /// Tenant identifier
    pub tenant_id: String,
    /// Dataset identifier
    pub dataset_id: String,
    /// Table name
    pub table_name: String,
    /// Partition identifier
    pub partition_id: String,
    /// UUID string of the compactor instance that holds this lease
    pub holder_id: String,
    /// When this lease was acquired
    pub acquired_at: DateTime<Utc>,
    /// When this lease expires (can be extended via `renew`)
    pub expires_at: DateTime<Utc>,
}

impl From<CompactorLease> for Lease {
    fn from(l: CompactorLease) -> Self {
        Self {
            tenant_id: l.tenant_id,
            dataset_id: l.dataset_id,
            table_name: l.table_name,
            partition_id: l.partition_id,
            holder_id: l.holder_id,
            acquired_at: l.acquired_at,
            expires_at: l.expires_at,
        }
    }
}

/// Manages distributed compaction leases via the shared SQL catalog.
///
/// Create one `LeaseManager` per compactor instance and reuse it across
/// compaction cycles. All methods are async and safe to call concurrently.
#[derive(Clone)]
pub struct LeaseManager {
    catalog: Arc<Catalog>,
    /// This compactor instance's UUID (from `ServiceBootstrap::service_id`)
    instance_id: Uuid,
    /// Default TTL if no TTL is provided to individual calls
    default_ttl: Duration,
}

impl LeaseManager {
    /// Create a new `LeaseManager`.
    ///
    /// # Arguments
    ///
    /// * `catalog`     – The SQL catalog (from `ServiceBootstrap::catalog()`)
    /// * `instance_id` – This compactor's unique UUID
    /// * `default_ttl` – How long leases are valid before expiring
    pub fn new(catalog: Arc<Catalog>, instance_id: Uuid, default_ttl: Duration) -> Self {
        Self {
            catalog,
            instance_id,
            default_ttl,
        }
    }

    /// Attempt to acquire a lease for a compaction candidate.
    ///
    /// Returns `Some(Lease)` if this instance now holds the lease,
    /// or `None` if another live instance already holds it.
    ///
    /// Expired leases are automatically taken over.
    pub async fn try_acquire(
        &self,
        candidate: &CompactionCandidate,
        ttl: Duration,
    ) -> Result<Option<Lease>> {
        let holder_id = self.instance_id.to_string();
        let ttl_secs = ttl.as_secs() as i64;

        let acquired = self
            .catalog
            .try_acquire_compaction_lease(
                &candidate.tenant_id,
                &candidate.dataset_id,
                &candidate.table_name,
                &candidate.partition_id,
                &holder_id,
                ttl_secs,
            )
            .await
            .with_context(|| {
                format!(
                    "Failed to acquire lease for {}/{}/{}/{}",
                    candidate.tenant_id,
                    candidate.dataset_id,
                    candidate.table_name,
                    candidate.partition_id
                )
            })?;

        if acquired {
            let now = Utc::now();
            let expires_at =
                now + chrono::Duration::from_std(ttl).context("TTL duration overflow")?;
            Ok(Some(Lease {
                tenant_id: candidate.tenant_id.clone(),
                dataset_id: candidate.dataset_id.clone(),
                table_name: candidate.table_name.clone(),
                partition_id: candidate.partition_id.clone(),
                holder_id,
                acquired_at: now,
                expires_at,
            }))
        } else {
            Ok(None)
        }
    }

    /// Try to acquire using the default TTL.
    pub async fn try_acquire_default(
        &self,
        candidate: &CompactionCandidate,
    ) -> Result<Option<Lease>> {
        self.try_acquire(candidate, self.default_ttl).await
    }

    /// Renew an existing lease, extending the expiry by `ttl` from now.
    ///
    /// Call this periodically during long-running compaction jobs to prevent
    /// lease expiry. Fails with an error if the lease was stolen.
    pub async fn renew(&self, lease: &Lease, ttl: Duration) -> Result<()> {
        let ttl_secs = ttl.as_secs() as i64;

        let renewed = self
            .catalog
            .renew_compaction_lease(
                &lease.tenant_id,
                &lease.dataset_id,
                &lease.table_name,
                &lease.partition_id,
                &lease.holder_id,
                ttl_secs,
            )
            .await
            .with_context(|| {
                format!(
                    "Failed to renew lease for {}/{}/{}/{}",
                    lease.tenant_id, lease.dataset_id, lease.table_name, lease.partition_id
                )
            })?;

        if !renewed {
            Err(anyhow!(
                "Lease for {}/{}/{}/{} no longer owned by this instance — may have been stolen",
                lease.tenant_id,
                lease.dataset_id,
                lease.table_name,
                lease.partition_id
            ))
        } else {
            Ok(())
        }
    }

    /// Release a lease explicitly after compaction completes.
    ///
    /// If the lease was stolen (another instance took it over), this is a no-op
    /// rather than an error — the lease is already gone.
    pub async fn release(&self, lease: &Lease) -> Result<()> {
        self.catalog
            .release_compaction_lease(
                &lease.tenant_id,
                &lease.dataset_id,
                &lease.table_name,
                &lease.partition_id,
                &lease.holder_id,
            )
            .await
            .with_context(|| {
                format!(
                    "Failed to release lease for {}/{}/{}/{}",
                    lease.tenant_id, lease.dataset_id, lease.table_name, lease.partition_id
                )
            })?;
        Ok(())
    }

    /// Expire all stale leases (where `expires_at < now`).
    ///
    /// Should be called periodically (e.g. every 30s) to garbage-collect
    /// leases from crashed compactor instances.
    ///
    /// Returns the number of leases cleaned up.
    pub async fn expire_stale(&self) -> Result<u64> {
        self.catalog
            .expire_stale_compaction_leases()
            .await
            .context("Failed to expire stale compaction leases")
    }

    /// List all currently active (non-expired) leases.
    ///
    /// Useful for the `compact_status` Flight DoAction endpoint.
    pub async fn list_active(&self) -> Result<Vec<Lease>> {
        let leases = self
            .catalog
            .list_active_compaction_leases()
            .await
            .context("Failed to list active compaction leases")?;
        Ok(leases.into_iter().map(Lease::from).collect())
    }

    /// Count leases held by this instance (non-expired).
    pub async fn count_held(&self) -> Result<usize> {
        let all = self.list_active().await?;
        Ok(all
            .into_iter()
            .filter(|l| l.holder_id == self.instance_id.to_string())
            .count())
    }

    /// Return this instance's UUID.
    pub fn instance_id(&self) -> Uuid {
        self.instance_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{CompactionCandidate, PartitionStats};
    use common::catalog::Catalog;

    async fn make_catalog() -> Catalog {
        Catalog::new_in_memory().await.expect("in-memory catalog")
    }

    fn make_candidate(partition_id: &str) -> CompactionCandidate {
        CompactionCandidate {
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            table_name: "traces".to_string(),
            partition_id: partition_id.to_string(),
            stats: PartitionStats {
                file_count: 5,
                total_size_bytes: 1024 * 1024,
                avg_file_size_bytes: 204_800,
            },
        }
    }

    #[tokio::test]
    async fn test_acquire_and_release() {
        let catalog = make_catalog().await;
        let id = Uuid::new_v4();
        let mgr = LeaseManager::new(Arc::new(catalog), id, Duration::from_secs(300));
        let cand = make_candidate("2024-01-01");

        let lease = mgr.try_acquire_default(&cand).await.unwrap();
        assert!(lease.is_some(), "Should acquire lease on first attempt");

        let lease = lease.unwrap();
        assert_eq!(lease.holder_id, id.to_string());

        mgr.release(&lease).await.unwrap();
    }

    #[tokio::test]
    async fn test_second_instance_cannot_acquire_live_lease() {
        let catalog = Arc::new(make_catalog().await);
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let mgr1 = LeaseManager::new(catalog.clone(), id1, Duration::from_secs(300));
        let mgr2 = LeaseManager::new(catalog.clone(), id2, Duration::from_secs(300));

        let cand = make_candidate("2024-01-01");

        let lease1 = mgr1.try_acquire_default(&cand).await.unwrap();
        assert!(lease1.is_some(), "Instance 1 should acquire lease");

        let lease2 = mgr2.try_acquire_default(&cand).await.unwrap();
        assert!(
            lease2.is_none(),
            "Instance 2 should not acquire lease held by instance 1"
        );

        // After release, instance 2 can acquire
        mgr1.release(&lease1.unwrap()).await.unwrap();
        let lease2 = mgr2.try_acquire_default(&cand).await.unwrap();
        assert!(lease2.is_some(), "Instance 2 should acquire after release");
    }

    #[tokio::test]
    async fn test_expired_lease_can_be_taken_over() {
        let catalog = Arc::new(make_catalog().await);
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        // Instance 1 acquires with very short TTL
        let mgr1 = LeaseManager::new(catalog.clone(), id1, Duration::from_millis(1));
        let mgr2 = LeaseManager::new(catalog.clone(), id2, Duration::from_secs(300));

        let cand = make_candidate("2024-01-02");

        let lease1 = mgr1
            .try_acquire(&cand, Duration::from_millis(1))
            .await
            .unwrap();
        assert!(lease1.is_some());

        // Wait for expiry
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Instance 2 should now be able to take over the expired lease
        let lease2 = mgr2.try_acquire_default(&cand).await.unwrap();
        assert!(
            lease2.is_some(),
            "Instance 2 should be able to take over expired lease"
        );
    }

    #[tokio::test]
    async fn test_different_partitions_independent() {
        let catalog = Arc::new(make_catalog().await);
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let mgr1 = LeaseManager::new(catalog.clone(), id1, Duration::from_secs(300));
        let mgr2 = LeaseManager::new(catalog.clone(), id2, Duration::from_secs(300));

        let cand1 = make_candidate("partition-a");
        let cand2 = make_candidate("partition-b");

        let l1 = mgr1.try_acquire_default(&cand1).await.unwrap();
        let l2 = mgr2.try_acquire_default(&cand2).await.unwrap();

        assert!(l1.is_some(), "Instance 1 should hold lease on partition-a");
        assert!(l2.is_some(), "Instance 2 should hold lease on partition-b");
    }

    #[tokio::test]
    async fn test_expire_stale_removes_expired() {
        let catalog = Arc::new(make_catalog().await);
        let id = Uuid::new_v4();
        let mgr = LeaseManager::new(catalog.clone(), id, Duration::from_millis(1));

        let cand = make_candidate("2024-01-03");
        let _lease = mgr
            .try_acquire(&cand, Duration::from_millis(1))
            .await
            .unwrap();

        // Wait for expiry then run cleanup
        tokio::time::sleep(Duration::from_millis(10)).await;

        let removed = mgr.expire_stale().await.unwrap();
        assert!(
            removed >= 1,
            "Should have removed at least one expired lease"
        );

        let active = mgr.list_active().await.unwrap();
        assert!(
            active.is_empty(),
            "No active leases should remain after expiry"
        );
    }

    #[tokio::test]
    async fn test_renew_updates_expiry() {
        let catalog = Arc::new(make_catalog().await);
        let id = Uuid::new_v4();
        let mgr = LeaseManager::new(catalog.clone(), id, Duration::from_secs(300));

        let cand = make_candidate("2024-01-04");
        let lease = mgr.try_acquire_default(&cand).await.unwrap().unwrap();

        let original_expires = lease.expires_at;

        // Small delay so renewal time is measurably different
        tokio::time::sleep(Duration::from_millis(5)).await;

        mgr.renew(&lease, Duration::from_secs(300)).await.unwrap();

        // Verify via list_active
        let active = mgr.list_active().await.unwrap();
        let updated = active
            .iter()
            .find(|l| l.partition_id == "2024-01-04")
            .expect("Lease should still be active after renewal");

        assert!(
            updated.expires_at >= original_expires,
            "Expiry should be extended after renewal"
        );
    }
}
