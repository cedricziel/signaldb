//! Manages Iceberg table lifecycle -- loading and creating tables.

use std::sync::Arc;

use anyhow::Result;
use iceberg_rust::catalog::Catalog as IcebergCatalog;
use iceberg_rust::catalog::create::CreateTableBuilder;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::table::Table;

use super::names;
use super::schemas;

/// Manages the lifecycle of Iceberg tables.
///
/// Provides `ensure_table()` which loads an existing table or creates it
/// if it doesn't exist. Every call loads fresh metadata from the catalog:
/// a `Table` handle carries table metadata as of load time, so handing out
/// long-lived cached handles would hide snapshots committed by other
/// writers (issue #537). Callers that need a current view must call
/// `ensure_table` again rather than hold on to an old handle.
pub struct IcebergTableManager {
    catalog: Arc<dyn IcebergCatalog>,
}

impl IcebergTableManager {
    /// Create from catalog reference.
    pub fn new(catalog: Arc<dyn IcebergCatalog>) -> Self {
        Self { catalog }
    }

    /// Load an existing table or create it if it doesn't exist.
    ///
    /// This method:
    /// 1. Tries `load_tabular()` -- single catalog round-trip, fresh metadata
    /// 2. On NotFound -> creates the table with schema from `schemas::TableSchema`
    /// 3. Handles `AlreadyExists` gracefully (concurrent callers)
    pub async fn ensure_table(
        &self,
        tenant_slug: &str,
        dataset_slug: &str,
        table_name: &str,
    ) -> Result<Table> {
        let ident = names::build_table_identifier(tenant_slug, dataset_slug, table_name);
        if let Ok(tabular) = self.catalog.clone().load_tabular(&ident).await {
            let table = match tabular {
                Tabular::Table(table) => table,
                _ => {
                    return Err(anyhow::anyhow!(
                        "Expected table but found different tabular type for {}",
                        ident
                    ));
                }
            };

            return Ok(table);
        }

        let table_schema = match table_name {
            "traces" => schemas::TableSchema::Traces,
            "logs" => schemas::TableSchema::Logs,
            "metrics_gauge" => schemas::TableSchema::MetricsGauge,
            "metrics_sum" => schemas::TableSchema::MetricsSum,
            "metrics_histogram" => schemas::TableSchema::MetricsHistogram,
            "metrics_exponential_histogram" => schemas::TableSchema::MetricsExponentialHistogram,
            "metrics_summary" => schemas::TableSchema::MetricsSummary,
            "profiles" => schemas::TableSchema::Profiles,
            _ => return Err(anyhow::anyhow!("Unknown table name: {table_name}")),
        };

        // Ensure namespace exists before creating table
        let namespace = names::build_namespace(tenant_slug, dataset_slug)?;
        // Try to create namespace - idempotent, will succeed if already exists
        match self
            .catalog
            .clone()
            .create_namespace(&namespace, None)
            .await
        {
            Ok(_) => {
                // Namespace created successfully
            }
            Err(e) => {
                let message = e.to_string().to_lowercase();
                // Ignore "already exists" errors from concurrent creation attempts.
                // SQLite surfaces this as "unique constraint failed" rather than "already exists".
                if !message.contains("already exists")
                    && !message.contains("conflict")
                    && !message.contains("unique constraint")
                {
                    return Err(anyhow::anyhow!(
                        "Failed to create namespace {namespace}: {e}"
                    ));
                }
            }
        }

        let table_create = CreateTableBuilder::default()
            .with_name(table_name.to_string())
            .with_schema(table_schema.schema()?)
            .with_partition_spec(table_schema.partition_spec()?)
            .with_location(names::build_table_location(
                tenant_slug,
                dataset_slug,
                table_name,
            ))
            .create()
            .map_err(|e| anyhow::anyhow!("Failed to build CreateTable for {ident}: {e}"))?;

        let table = match self
            .catalog
            .clone()
            .create_table(ident.clone(), table_create)
            .await
        {
            Ok(table) => table,
            Err(create_err) => {
                let message = create_err.to_string().to_lowercase();
                // SQLite surfaces concurrent duplicate-create as "unique constraint failed"
                // rather than "already exists", so treat both the same way.
                if message.contains("already exists") || message.contains("unique constraint") {
                    let tabular = self
                        .catalog
                        .clone()
                        .load_tabular(&ident)
                        .await
                        .map_err(|load_err| {
                        anyhow::anyhow!(
                            "Table {} already existed but reload failed: {}; original create error: {}",
                            ident,
                            load_err,
                            create_err
                        )
                    })?;

                    match tabular {
                        Tabular::Table(table) => table,
                        _ => {
                            return Err(anyhow::anyhow!(
                                "Expected table but found different tabular type for {}",
                                ident
                            ));
                        }
                    }
                } else {
                    return Err(create_err.into());
                }
            }
        };

        Ok(table)
    }
}
