pub mod admin;
pub mod api_error;
pub mod discovery;
pub mod flight;
mod flight_decode;
pub mod logql;
pub mod management;
pub mod oauth;
pub mod ops;
pub mod promql;
pub mod pyroscope;
pub mod query;
pub mod schema;
pub mod session;
pub mod tempo;
pub mod tenant;

/// Current time as unix-epoch nanoseconds.
pub(crate) fn now_ns() -> i64 {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis() * 1_000_000)
}

/// Provision a newly created dataset's enabled signal tables synchronously,
/// best-effort.
///
/// Dataset creation must succeed regardless of the outcome here: on any
/// failure this only logs a warning and returns. The writer's periodic table
/// reconciler (`[writer].table_reconcile_interval`) is the convergence
/// backstop, and the ingest path still load-or-creates tables on first write,
/// so a failure here just means the dataset is not immediately queryable
/// rather than broken.
pub(crate) async fn provision_dataset_tables(
    config: &common::config::Configuration,
    tenant_source: &common::catalog::Catalog,
    tenant_id: &str,
    dataset_id: &str,
) {
    let manager = match common::CatalogManager::new(config.clone()).await {
        Ok(manager) => manager.with_tenant_source(std::sync::Arc::new(tenant_source.clone())),
        Err(error) => {
            tracing::warn!(
                %error, tenant_id, dataset_id,
                "failed to build catalog manager to provision tables for new dataset; \
                 the writer's reconciler will retry"
            );
            return;
        }
    };
    let report = manager.ensure_dataset_tables(tenant_id, dataset_id).await;
    if !report.failed.is_empty() {
        let failures: Vec<String> = report
            .failed
            .iter()
            .map(|(table, reason)| format!("{table}: {reason}"))
            .collect();
        tracing::warn!(
            tenant_id,
            dataset_id,
            failures = %failures.join("; "),
            "failed to provision some tables for new dataset; the writer's reconciler will retry"
        );
    }
}

/// The names of the tables actually registered for a tenant/dataset
/// namespace, read straight from the Iceberg catalog.
///
/// Test-only: shared by the `management` and `admin` dataset-provisioning
/// tests so both assert against the same query rather than each
/// reimplementing it.
#[cfg(test)]
pub(crate) async fn tabular_names_in(
    manager: &common::CatalogManager,
    tenant_id: &str,
    dataset_id: &str,
) -> Vec<String> {
    let namespace = manager
        .build_namespace(tenant_id, dataset_id)
        .expect("valid tenant/dataset namespace");
    manager
        .catalog()
        .list_tabulars(&namespace)
        .await
        .expect("catalog must list tabulars")
        .iter()
        .map(|identifier| identifier.name().to_string())
        .collect()
}
