//! # Per-Tenant Storage Usage Accounting
//!
//! Tracks how many bytes of live Iceberg data files each tenant holds and
//! enforces the optional `max_storage_bytes` quota on the ingest paths.
//!
//! Usage is computed from table metadata (manifest entries carry
//! `file_size_in_bytes`), so replaced or deleted files never double-count:
//! compaction and retention shrink usage as soon as the next refresh runs.
//!
//! Enforcement is deliberately eventually consistent. A periodic refresher
//! ([`spawn_usage_refresher`]) recomputes usage off the hot path and swaps
//! it into the [`StorageUsageTracker`]; ingest checks only compare the
//! cached value against the tenant's quota. A tenant may therefore
//! overshoot its cap by up to one refresh interval of ingest — that lag is
//! accepted by design (issue #610).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use dashmap::DashMap;
use futures::StreamExt;
use iceberg_rust::spec::manifest::Status;

use crate::catalog_manager::CatalogManager;
use crate::config::{AuthConfig, TenantLimits};

/// Error returned when a tenant is at or over its storage quota.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageQuotaExceeded {
    pub tenant_id: String,
    pub usage_bytes: u64,
    pub limit_bytes: u64,
}

impl std::fmt::Display for StorageQuotaExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "quota_exceeded: tenant '{}' uses {} bytes of storage, at or over its {} byte quota; \
             delete data, lower retention, or raise the tenant's max_storage_bytes",
            self.tenant_id, self.usage_bytes, self.limit_bytes
        )
    }
}

impl std::error::Error for StorageQuotaExceeded {}

/// Cached per-tenant storage usage with quota checks for the ingest paths.
///
/// Cheap to check on the hot path: one `DashMap` read. Usage values are
/// written by the periodic refresher, not by ingest itself.
pub struct StorageUsageTracker {
    defaults: TenantLimits,
    overrides: HashMap<String, TenantLimits>,
    usage: DashMap<String, u64>,
}

impl StorageUsageTracker {
    /// Build a tracker from auth configuration: global `default_limits`
    /// plus per-tenant overrides.
    pub fn from_auth_config(auth: &AuthConfig) -> Self {
        let overrides = auth
            .tenants
            .iter()
            .filter_map(|t| t.limits.clone().map(|l| (t.id.clone(), l)))
            .collect();
        Self {
            defaults: auth.default_limits.clone(),
            overrides,
            usage: DashMap::new(),
        }
    }

    /// Whether any storage quota is configured at all. When false, no
    /// usage accounting is needed and the refresher can be skipped.
    pub fn quotas_configured(&self) -> bool {
        self.defaults.max_storage_bytes.is_some()
            || self
                .overrides
                .values()
                .any(|l| l.max_storage_bytes.is_some())
    }

    /// The storage quota that applies to `tenant_id`, if any.
    fn limit_for(&self, tenant_id: &str) -> Option<u64> {
        self.overrides
            .get(tenant_id)
            .unwrap_or(&self.defaults)
            .max_storage_bytes
    }

    /// Reject ingest for a tenant whose known usage is at or over its
    /// quota. Tenants without a quota, or whose usage has not been
    /// computed yet, always pass — accounting lag must not block ingest.
    pub fn check_ingest(&self, tenant_id: &str) -> Result<(), StorageQuotaExceeded> {
        let Some(limit_bytes) = self.limit_for(tenant_id) else {
            return Ok(());
        };
        let Some(usage_bytes) = self.usage.get(tenant_id).map(|u| *u) else {
            return Ok(());
        };
        if usage_bytes >= limit_bytes {
            return Err(StorageQuotaExceeded {
                tenant_id: tenant_id.to_string(),
                usage_bytes,
                limit_bytes,
            });
        }
        Ok(())
    }

    /// Replace the cached usage for all tenants with a fresh computation.
    /// Tenants absent from `usage` are removed (their tables are gone).
    pub fn replace_all(&self, usage: HashMap<String, u64>) {
        self.usage
            .retain(|tenant_id, _| usage.contains_key(tenant_id));
        for (tenant_id, bytes) in usage {
            self.usage.insert(tenant_id, bytes);
        }
    }

    /// Known usage for one tenant, if computed.
    pub fn usage_bytes(&self, tenant_id: &str) -> Option<u64> {
        self.usage.get(tenant_id).map(|u| *u)
    }

    /// Snapshot of all known tenant usages, for metrics and operators.
    pub fn snapshot(&self) -> Vec<(String, u64)> {
        self.usage
            .iter()
            .map(|e| (e.key().clone(), *e.value()))
            .collect()
    }
}

/// Compute per-tenant storage usage from Iceberg table metadata.
///
/// Walks every namespace in the catalog (namespaces are
/// `[tenant_slug, dataset_slug]`), sums `file_size_in_bytes` over the live
/// (non-deleted) data files of every table, and groups the totals by tenant
/// id. Tenant slugs are mapped back to tenant ids via configuration;
/// runtime-provisioned tenants (no config entry) key by their slug, which
/// equals their id.
///
/// Per-table and per-namespace failures are logged and skipped rather than
/// failing the whole pass: a partial (under-counted) refresh is more useful
/// than none, and the next pass retries.
pub async fn compute_usage(catalog_manager: &CatalogManager) -> Result<HashMap<String, u64>> {
    let catalog = catalog_manager.catalog();
    let namespaces = catalog
        .list_namespaces(None)
        .await
        .context("Failed to list namespaces for storage usage accounting")?;

    let slug_to_tenant_id: HashMap<String, String> = catalog_manager
        .config()
        .auth
        .tenants
        .iter()
        .map(|t| (t.slug.clone(), t.id.clone()))
        .collect();

    let mut usage: HashMap<String, u64> = HashMap::new();
    for namespace in &namespaces {
        let Some(tenant_slug) = namespace.iter().next() else {
            continue;
        };
        let tenant_id = slug_to_tenant_id
            .get(tenant_slug.as_str())
            .cloned()
            .unwrap_or_else(|| tenant_slug.clone());
        // A tenant with namespaces but no readable files still gets an
        // entry, so stale cached usage is replaced by the fresh count.
        let tenant_usage = usage.entry(tenant_id).or_default();

        let identifiers = match catalog.list_tabulars(namespace).await {
            Ok(identifiers) => identifiers,
            Err(e) => {
                tracing::warn!(
                    namespace = ?namespace,
                    error = %e,
                    "Skipping namespace in storage usage accounting: listing tables failed"
                );
                continue;
            }
        };
        for identifier in identifiers {
            match table_live_bytes(catalog.clone(), &identifier).await {
                Ok(bytes) => *tenant_usage += bytes,
                Err(e) => {
                    tracing::warn!(
                        table = %identifier,
                        error = %e,
                        "Skipping table in storage usage accounting; usage is under-counted \
                         until the next successful refresh"
                    );
                }
            }
        }
    }
    Ok(usage)
}

/// Sum of `file_size_in_bytes` over the live data files in the table's
/// current snapshot.
async fn table_live_bytes(
    catalog: Arc<dyn iceberg_rust::catalog::Catalog>,
    identifier: &iceberg_rust::catalog::identifier::Identifier,
) -> Result<u64> {
    let tabular = catalog
        .load_tabular(identifier)
        .await
        .context("Failed to load table")?;
    let iceberg_rust::catalog::tabular::Tabular::Table(table) = tabular else {
        // Views and materialized views hold no data files of their own.
        return Ok(0);
    };

    let manifests = table
        .manifests(None, None)
        .await
        .context("Failed to read manifest list from current snapshot")?;
    if manifests.is_empty() {
        return Ok(0);
    }

    let file_iter = table
        .datafiles(&manifests, None, (None, None))
        .await
        .context("Failed to read data files from manifests")?;

    let mut total = 0u64;
    let mut file_iter = std::pin::pin!(file_iter);
    while let Some(result) = file_iter.next().await {
        let (_, entry) = result.context("Failed to read manifest entry")?;
        if *entry.status() != Status::Deleted {
            total += *entry.data_file().file_size_in_bytes() as u64;
        }
    }
    Ok(total)
}

/// Spawn the periodic usage refresher: recompute per-tenant usage every
/// `interval`, publish it to the tracker, and export it as the
/// `signaldb.tenant.storage_usage` gauge.
///
/// The first pass runs immediately so quotas take effect shortly after
/// startup rather than one full interval later.
pub fn spawn_usage_refresher(
    catalog_manager: Arc<CatalogManager>,
    tracker: Arc<StorageUsageTracker>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            match compute_usage(&catalog_manager).await {
                Ok(usage) => {
                    let metrics = crate::self_monitoring::app_metrics();
                    for (tenant_id, bytes) in &usage {
                        metrics.tenant_storage_usage_bytes.record(
                            *bytes,
                            &[opentelemetry::KeyValue::new("tenant_id", tenant_id.clone())],
                        );
                    }
                    tracing::debug!(tenants = usage.len(), "Refreshed per-tenant storage usage");
                    tracker.replace_all(usage);
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Failed to refresh per-tenant storage usage; keeping previous values"
                    );
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TenantConfig;

    fn tenant_with_limits(id: &str, limits: TenantLimits) -> TenantConfig {
        TenantConfig {
            id: id.to_string(),
            slug: id.to_string(),
            name: id.to_string(),
            default_dataset: None,
            datasets: vec![],
            api_keys: vec![],
            schema_config: None,
            limits: Some(limits),
        }
    }

    fn tracker(defaults: TenantLimits, tenants: Vec<TenantConfig>) -> StorageUsageTracker {
        let auth = AuthConfig {
            default_limits: defaults,
            tenants,
            ..Default::default()
        };
        StorageUsageTracker::from_auth_config(&auth)
    }

    #[test]
    fn unlimited_without_quota_configuration() {
        let tracker = tracker(TenantLimits::default(), vec![]);
        assert!(!tracker.quotas_configured());
        tracker.replace_all(HashMap::from([("acme".to_string(), u64::MAX)]));
        assert!(tracker.check_ingest("acme").is_ok());
    }

    #[test]
    fn unknown_usage_passes_even_with_quota() {
        let tracker = tracker(
            TenantLimits {
                max_storage_bytes: Some(1_000),
                ..Default::default()
            },
            vec![],
        );
        assert!(tracker.quotas_configured());
        // No usage computed yet: accounting lag must not block ingest.
        assert!(tracker.check_ingest("acme").is_ok());
    }

    #[test]
    fn usage_at_or_over_quota_rejects() {
        let tracker = tracker(
            TenantLimits {
                max_storage_bytes: Some(1_000),
                ..Default::default()
            },
            vec![],
        );
        tracker.replace_all(HashMap::from([
            ("under".to_string(), 999),
            ("at".to_string(), 1_000),
            ("over".to_string(), 1_001),
        ]));

        assert!(tracker.check_ingest("under").is_ok());
        let at = tracker.check_ingest("at").unwrap_err();
        assert_eq!(at.usage_bytes, 1_000);
        assert_eq!(at.limit_bytes, 1_000);
        let over = tracker.check_ingest("over").unwrap_err();
        assert_eq!(over.usage_bytes, 1_001);
        assert!(over.to_string().contains("quota_exceeded"));
        assert!(over.to_string().contains("over"));
    }

    #[test]
    fn per_tenant_override_beats_default() {
        let tracker = tracker(
            TenantLimits {
                max_storage_bytes: Some(1_000),
                ..Default::default()
            },
            vec![tenant_with_limits(
                "vip",
                TenantLimits {
                    max_storage_bytes: Some(1_000_000),
                    ..Default::default()
                },
            )],
        );
        tracker.replace_all(HashMap::from([
            ("vip".to_string(), 500_000),
            ("acme".to_string(), 500_000),
        ]));

        assert!(tracker.check_ingest("vip").is_ok());
        assert!(tracker.check_ingest("acme").is_err());
    }

    #[test]
    fn override_without_storage_quota_is_unlimited() {
        // A tenant override replaces the defaults wholesale, matching the
        // rate limiter's semantics: an override that leaves
        // max_storage_bytes unset means unlimited for that tenant.
        let tracker = tracker(
            TenantLimits {
                max_storage_bytes: Some(1_000),
                ..Default::default()
            },
            vec![tenant_with_limits(
                "special",
                TenantLimits {
                    max_ingest_requests_per_sec: Some(10),
                    ..Default::default()
                },
            )],
        );
        tracker.replace_all(HashMap::from([("special".to_string(), 5_000)]));
        assert!(tracker.check_ingest("special").is_ok());
    }

    #[test]
    fn replace_all_drops_stale_tenants() {
        let tracker = tracker(
            TenantLimits {
                max_storage_bytes: Some(1_000),
                ..Default::default()
            },
            vec![],
        );
        tracker.replace_all(HashMap::from([("gone".to_string(), 2_000)]));
        assert!(tracker.check_ingest("gone").is_err());

        // The tenant's tables were removed; the next refresh no longer
        // reports it and ingest unblocks.
        tracker.replace_all(HashMap::new());
        assert_eq!(tracker.usage_bytes("gone"), None);
        assert!(tracker.check_ingest("gone").is_ok());
    }

    #[tokio::test]
    async fn compute_usage_empty_catalog_reports_nothing() {
        let manager = CatalogManager::new_in_memory().await.unwrap();
        let usage = compute_usage(&manager).await.unwrap();
        assert!(usage.is_empty());
    }

    #[tokio::test]
    async fn compute_usage_counts_empty_tables_as_zero() {
        let manager = CatalogManager::new_in_memory().await.unwrap();
        // Creates the namespace and an empty traces table for the default
        // tenant/dataset.
        manager
            .ensure_table("default", "default", "traces")
            .await
            .unwrap();
        let usage = compute_usage(&manager).await.unwrap();
        assert_eq!(usage.get("default"), Some(&0));
    }
}
