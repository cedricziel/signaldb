//! Manages Iceberg table lifecycle -- loading, creating, and caching tables.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use iceberg_rust::catalog::Catalog as IcebergCatalog;
use iceberg_rust::catalog::create::CreateTableBuilder;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::table::Table;
use tokio::sync::RwLock;

use super::names;
use super::schemas;

/// Manages the lifecycle of Iceberg tables with caching.
///
/// Provides `ensure_table()` which loads an existing table or creates it
/// if it doesn't exist. Tables are cached by (tenant_id, dataset_id, table_name).
pub struct IcebergTableManager {
    catalog: Arc<dyn IcebergCatalog>,
    cache: RwLock<HashMap<(String, String, String), Table>>,
}

impl IcebergTableManager {
    /// Create from catalog reference.
    pub fn new(catalog: Arc<dyn IcebergCatalog>) -> Self {
        Self {
            catalog,
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Load an existing table or create it if it doesn't exist.
    ///
    /// This method:
    /// 1. Checks the in-memory cache
    /// 2. Tries `load_tabular()` -- single catalog round-trip
    /// 3. On NotFound -> creates the table with schema from `schemas::TableSchema`
    /// 4. Handles `AlreadyExists` gracefully (concurrent callers)
    /// 5. Caches the result
    pub async fn ensure_table(
        &self,
        tenant_slug: &str,
        dataset_slug: &str,
        table_name: &str,
    ) -> Result<Table> {
        let cache_key = (
            tenant_slug.to_string(),
            dataset_slug.to_string(),
            table_name.to_string(),
        );

        if let Some(table) = self.cache.read().await.get(&cache_key).cloned() {
            return Ok(table);
        }

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

            self.cache.write().await.insert(cache_key, table.clone());
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
            _ => return Err(anyhow::anyhow!("Unknown table name: {table_name}")),
        };

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
                if message.contains("already exists") {
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

        self.cache.write().await.insert(cache_key, table.clone());
        Ok(table)
    }

    /// Invalidate a cached table, forcing reload on next access.
    pub async fn invalidate(&self, tenant_id: &str, dataset_id: &str, table_name: &str) {
        self.cache.write().await.remove(&(
            tenant_id.to_string(),
            dataset_id.to_string(),
            table_name.to_string(),
        ));
    }
}
