//! # Signal-table reconciler
//!
//! Converges every registered tenant/dataset on the set of signal tables
//! enabled for its tenant, so a dataset is queryable from the moment it exists
//! rather than only after telemetry arrives for each signal.
//!
//! Convergence, not a one-shot creation hook: a pass runs at writer startup and
//! then periodically, so datasets created while the writer was down, datasets
//! predating this capability, and datasets added at runtime all converge
//! without operator action or a restart.
//!
//! Failures are never fatal. A table that cannot be provisioned is logged with
//! its tenant/dataset/table and retried next pass; because the ingest path
//! independently creates any table it needs, a persistently failing reconciler
//! degrades to the prior create-on-first-write behavior.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context, Result};
use common::CatalogManager;
use common::self_monitoring::app_metrics;
use opentelemetry::KeyValue;
use tokio::sync::Mutex;

/// What one reconcile pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReconcilePassSummary {
    /// Datasets whose tables were checked against the catalog.
    pub datasets_checked: usize,
    /// Datasets skipped because every enabled table was already confirmed by
    /// an earlier pass in this process — these cost no catalog traffic.
    pub datasets_skipped: usize,
    /// Tables created by this pass.
    pub tables_created: usize,
    /// Tables that could not be provisioned; retried on the next pass.
    pub tables_failed: usize,
}

/// Converges datasets on their enabled signal tables.
///
/// Keeps a process-local set of `(tenant, dataset, table)` triples it has
/// confirmed, so a converged deployment issues no catalog traffic per pass.
/// The set is rebuilt on restart, making the startup pass always a real check.
/// Staleness is benign: the worst case is skipping a table deleted
/// out-of-band, which the write path recreates and a restart re-checks.
pub struct TableReconciler {
    catalog_manager: Arc<CatalogManager>,
    ensured: Mutex<HashSet<(String, String, String)>>,
}

impl TableReconciler {
    pub fn new(catalog_manager: Arc<CatalogManager>) -> Self {
        Self {
            catalog_manager,
            ensured: Mutex::new(HashSet::new()),
        }
    }

    /// Run one pass over the tenant registry.
    ///
    /// Returns an error only when the registry itself cannot be enumerated —
    /// per-dataset and per-table failures are counted in the summary so one
    /// tenant's failure never aborts the rest of the pass.
    pub async fn run_pass(&self) -> Result<ReconcilePassSummary> {
        let tenants = self
            .catalog_manager
            .list_active_tenants()
            .await
            .context("Failed to enumerate tenants for table reconciliation")?;

        let mut summary = ReconcilePassSummary::default();
        for tenant in tenants {
            let expected = self.catalog_manager.enabled_table_names(&tenant.id);

            for dataset in datasets_of(&tenant) {
                if self.is_converged(&tenant.id, &dataset, &expected).await {
                    summary.datasets_skipped += 1;
                    continue;
                }

                summary.datasets_checked += 1;
                let report = self
                    .catalog_manager
                    .ensure_dataset_tables(&tenant.id, &dataset)
                    .await;

                summary.tables_created += report.created.len();
                summary.tables_failed += report.failed.len();
                record_provisioning_metrics(&tenant.id, &dataset, &report);

                let mut ensured = self.ensured.lock().await;
                for table in report.created.iter().chain(report.already_present.iter()) {
                    ensured.insert((tenant.id.clone(), dataset.clone(), table.clone()));
                }
            }
        }

        Ok(summary)
    }

    /// Whether every enabled table for this dataset was already confirmed.
    async fn is_converged(&self, tenant_id: &str, dataset_id: &str, expected: &[String]) -> bool {
        if expected.is_empty() {
            return true;
        }
        let ensured = self.ensured.lock().await;
        expected.iter().all(|table| {
            ensured.contains(&(tenant_id.to_string(), dataset_id.to_string(), table.clone()))
        })
    }
}

/// The datasets to provision for a tenant.
///
/// The registry guarantees a tenant's `default_dataset` is among its resolved
/// datasets even when no dataset row names it — the state admin-API tenant
/// creation leaves behind — so this is a plain projection.
fn datasets_of(tenant: &common::catalog_manager::ResolvedTenant) -> Vec<String> {
    tenant.datasets.iter().map(|d| d.id.clone()).collect()
}

fn record_provisioning_metrics(
    tenant_id: &str,
    dataset_id: &str,
    report: &common::catalog_manager::DatasetProvisioningReport,
) {
    let attrs = [
        KeyValue::new("tenant", tenant_id.to_string()),
        KeyValue::new("dataset", dataset_id.to_string()),
    ];
    if !report.created.is_empty() {
        app_metrics()
            .writer_tables_provisioned
            .add(report.created.len() as u64, &attrs);
    }
    if !report.failed.is_empty() {
        app_metrics()
            .writer_table_provisioning_failures
            .add(report.failed.len() as u64, &attrs);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::catalog::Catalog;
    use common::config::{Configuration, DatasetConfig, TenantConfig};

    fn config_with(tenants: Vec<TenantConfig>) -> Configuration {
        let mut config = Configuration::default();
        config.schema.catalog_uri = format!(
            "sqlite:file:signaldb_reconcile_{}?mode=memory&cache=shared",
            uuid::Uuid::new_v4()
        );
        config.auth.tenants = tenants;
        config
    }

    fn config_tenant(id: &str, datasets: &[&str]) -> TenantConfig {
        TenantConfig {
            id: id.to_string(),
            slug: id.to_string(),
            name: id.to_string(),
            default_dataset: datasets.first().map(|d| (*d).to_string()),
            datasets: datasets
                .iter()
                .enumerate()
                .map(|(i, d)| DatasetConfig {
                    id: (*d).to_string(),
                    slug: (*d).to_string(),
                    is_default: i == 0,
                    storage: None,
                })
                .collect(),
            api_keys: vec![],
            schema_config: None,
            limits: None,
        }
    }

    async fn tables_in(manager: &CatalogManager, tenant_id: &str, dataset_id: &str) -> Vec<String> {
        let Ok(namespace) = manager.build_namespace(tenant_id, dataset_id) else {
            return Vec::new();
        };
        let mut names: Vec<String> = manager
            .catalog()
            .list_tabulars(&namespace)
            .await
            .unwrap_or_default()
            .iter()
            .map(|identifier| identifier.name().to_string())
            .collect();
        names.sort();
        names
    }

    fn all_signal_tables() -> usize {
        8
    }

    // Task 4.1 — the reconciler must see database-created tenants, not only
    // config-defined ones. Without a tenant source attached to the writer's
    // CatalogManager, `list_active_tenants` returns config tenants only —
    // i.e. exactly not the population this change is for.
    #[tokio::test]
    async fn sees_database_created_tenants() {
        let source = Arc::new(Catalog::new_in_memory().await.unwrap());
        let manager = Arc::new(
            CatalogManager::new(config_with(vec![]))
                .await
                .unwrap()
                .with_tenant_source(source.clone()),
        );

        source
            .upsert_tenant(
                "matter-survey",
                "Matter Survey",
                Some("production"),
                "database",
            )
            .await
            .unwrap();
        source
            .create_dataset("matter-survey", "production")
            .await
            .unwrap();

        let summary = TableReconciler::new(manager.clone())
            .run_pass()
            .await
            .unwrap();

        assert_eq!(summary.tables_failed, 0);
        assert_eq!(summary.tables_created, all_signal_tables());
        assert_eq!(
            tables_in(&manager, "matter-survey", "production")
                .await
                .len(),
            all_signal_tables()
        );
    }

    // Task 4.3 — admin-API tenant creation writes `default_dataset` as a
    // column on the tenant row and no dataset row at all.
    #[tokio::test]
    async fn provisions_a_default_dataset_with_no_dataset_row() {
        let source = Arc::new(Catalog::new_in_memory().await.unwrap());
        let manager = Arc::new(
            CatalogManager::new(config_with(vec![]))
                .await
                .unwrap()
                .with_tenant_source(source.clone()),
        );

        // Tenant only — deliberately no `create_dataset`.
        source
            .upsert_tenant("fresh", "Fresh", Some("production"), "database")
            .await
            .unwrap();

        TableReconciler::new(manager.clone())
            .run_pass()
            .await
            .unwrap();

        assert_eq!(
            tables_in(&manager, "fresh", "production").await.len(),
            all_signal_tables()
        );
    }

    #[tokio::test]
    async fn recorded_datasets_are_provisioned_without_duplicating_the_default() {
        let source = Arc::new(Catalog::new_in_memory().await.unwrap());
        let manager = Arc::new(
            CatalogManager::new(config_with(vec![]))
                .await
                .unwrap()
                .with_tenant_source(source.clone()),
        );
        source
            .upsert_tenant("both", "Both", Some("production"), "database")
            .await
            .unwrap();
        source.create_dataset("both", "production").await.unwrap();
        source.create_dataset("both", "staging").await.unwrap();

        let summary = TableReconciler::new(manager.clone())
            .run_pass()
            .await
            .unwrap();

        // Two datasets, each checked exactly once.
        assert_eq!(summary.datasets_checked, 2);
        assert_eq!(summary.tables_created, 2 * all_signal_tables());
    }

    // Task 4.4 — a dataset that appears after the first pass converges on a
    // later pass, with no restart.
    #[tokio::test]
    async fn dataset_added_after_startup_converges_on_a_later_pass() {
        let source = Arc::new(Catalog::new_in_memory().await.unwrap());
        let manager = Arc::new(
            CatalogManager::new(config_with(vec![]))
                .await
                .unwrap()
                .with_tenant_source(source.clone()),
        );
        source
            .upsert_tenant("growing", "Growing", Some("production"), "database")
            .await
            .unwrap();
        source
            .create_dataset("growing", "production")
            .await
            .unwrap();

        let reconciler = TableReconciler::new(manager.clone());
        reconciler.run_pass().await.unwrap();
        assert!(tables_in(&manager, "growing", "staging").await.is_empty());

        source.create_dataset("growing", "staging").await.unwrap();

        let summary = reconciler.run_pass().await.unwrap();
        assert_eq!(summary.datasets_checked, 1, "only the new dataset");
        assert_eq!(summary.tables_created, all_signal_tables());
        assert_eq!(
            tables_in(&manager, "growing", "staging").await.len(),
            all_signal_tables()
        );
    }

    // Task 4.5 — an unenumerable registry surfaces as an error the caller
    // logs, never a panic; and a per-dataset failure does not abort the
    // remaining tenants, which are retried next pass.
    #[tokio::test]
    async fn one_datasets_failure_does_not_abort_the_rest_of_the_pass() {
        // An empty dataset name cannot form an Iceberg namespace, so every
        // table of that dataset fails — deterministically, per table.
        let manager = Arc::new(
            CatalogManager::new(config_with(vec![
                config_tenant("broken", &[""]),
                config_tenant("healthy", &["production"]),
            ]))
            .await
            .unwrap(),
        );

        let reconciler = TableReconciler::new(manager.clone());
        let summary = reconciler.run_pass().await.expect("pass must not fail");

        assert_eq!(summary.tables_failed, all_signal_tables());
        assert_eq!(summary.tables_created, all_signal_tables());
        assert_eq!(
            tables_in(&manager, "healthy", "production").await.len(),
            all_signal_tables(),
            "a sibling tenant must still be provisioned"
        );

        // The failed dataset is not marked converged, so the next pass retries it.
        let retry = reconciler.run_pass().await.expect("retry must not fail");
        assert_eq!(retry.datasets_checked, 1, "the failed dataset is retried");
        assert_eq!(retry.tables_failed, all_signal_tables());
    }

    // Task 4.8 — once converged, a pass issues no catalog traffic.
    #[tokio::test]
    async fn a_converged_deployment_checks_nothing() {
        let manager = Arc::new(
            CatalogManager::new(config_with(vec![config_tenant("acme", &["production"])]))
                .await
                .unwrap(),
        );
        let reconciler = TableReconciler::new(manager.clone());

        let first = reconciler.run_pass().await.unwrap();
        assert_eq!(first.datasets_checked, 1);
        assert_eq!(first.tables_created, all_signal_tables());

        let second = reconciler.run_pass().await.unwrap();
        assert_eq!(
            second,
            ReconcilePassSummary {
                datasets_checked: 0,
                datasets_skipped: 1,
                tables_created: 0,
                tables_failed: 0,
            },
            "a converged dataset must not be re-checked against the catalog"
        );
    }
}
