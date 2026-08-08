//! Absence-aware signal table resolution.
//!
//! A registered tenant/dataset does not necessarily hold a table for every
//! signal: the table may not have been provisioned yet, or the signal type may
//! be disabled for that deployment forever. Reads must tell that apart from a
//! genuine failure — an absent table is "no data", a broken catalog is an
//! error.
//!
//! [`optional_table`] keys on the async schema lookup DataFusion itself turns
//! into `No table named 'logs'`, so absence is caught at its source:
//!
//! - `schema_for_ref` failing means the tenant catalog or dataset schema could
//!   not be resolved — an unknown tenant, which stays an error.
//! - `SchemaProvider::table` returning `Ok(None)` means the table is absent.
//! - `SchemaProvider::table` returning `Err` means the catalog itself failed.
//!
//! Deliberately *not* keyed on [`SessionContext::table_exist`], which is sync
//! and delegates to the schema provider's sync `table_exist` — a hardcoded
//! `true` for the querier's `LiveIcebergSchema` — nor on matching DataFusion
//! error text, which is brittle across upgrades and would swallow genuine
//! planning failures.

use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::prelude::{DataFrame, SessionContext};

use super::error::QuerierError;
use super::table_ref::build_table_reference;

/// Resolve `{tenant_slug}.{dataset_slug}.{table_name}` to a [`DataFrame`],
/// reporting an absent table as `Ok(None)` rather than as an error.
///
/// The returned frame is identical to [`SessionContext::table`]'s — the same
/// qualified scan plan — so callers keep column qualification unchanged.
///
/// # Errors
///
/// Returns an error if a slug is malformed, if the tenant catalog or dataset
/// schema cannot be resolved, or if the catalog fails while looking the table
/// up. Those are all distinguishable from the absent case.
pub async fn optional_table(
    ctx: &SessionContext,
    tenant_slug: &str,
    dataset_slug: &str,
    table_name: &str,
) -> Result<Option<DataFrame>, QuerierError> {
    let table_ref = build_table_reference(tenant_slug, dataset_slug, table_name)
        .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
    let table = table_ref.table().to_string();

    let schema = ctx
        .state()
        .schema_for_ref(table_ref.clone())
        .map_err(QuerierError::QueryFailed)?;

    let Some(provider) = schema
        .table(&table)
        .await
        .map_err(QuerierError::QueryFailed)?
    else {
        tracing::debug!(
            tenant_id = %tenant_slug,
            dataset = %dataset_slug,
            table = %table,
            "Signal table absent; reading as empty"
        );
        return Ok(None);
    };

    let plan = LogicalPlanBuilder::scan(table_ref, provider_as_source(provider), None)
        .map_err(QuerierError::QueryFailed)?
        .build()
        .map_err(QuerierError::QueryFailed)?;
    Ok(Some(DataFrame::new(ctx.state(), plan)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::memory::MemTable;
    use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
    use std::sync::Arc;

    /// A context with `t.d` registered but holding no tables.
    fn empty_dataset_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog
            .register_schema("d", Arc::new(MemorySchemaProvider::new()))
            .unwrap();
        ctx.register_catalog("t", catalog);
        ctx
    }

    #[tokio::test]
    async fn absent_table_resolves_to_none() {
        let ctx = empty_dataset_ctx();
        let df = optional_table(&ctx, "t", "d", "logs").await.unwrap();
        assert!(df.is_none());
    }

    #[tokio::test]
    async fn present_table_resolves_to_a_qualified_scan() {
        let ctx = empty_dataset_ctx();
        let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
        let table = MemTable::try_new(schema, vec![vec![]]).unwrap();
        ctx.catalog("t")
            .unwrap()
            .schema("d")
            .unwrap()
            .register_table("logs".to_string(), Arc::new(table))
            .unwrap();

        let df = optional_table(&ctx, "t", "d", "logs")
            .await
            .unwrap()
            .expect("table is present");
        // Same qualification `SessionContext::table` produces.
        assert_eq!(df.schema().field_names(), vec!["t.d.logs.body".to_string()]);
    }

    #[tokio::test]
    async fn unknown_tenant_is_an_error_not_an_absence() {
        let ctx = empty_dataset_ctx();
        let err = optional_table(&ctx, "nosuchtenant", "d", "logs")
            .await
            .unwrap_err();
        assert!(matches!(err, QuerierError::QueryFailed(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn unknown_dataset_is_an_error_not_an_absence() {
        let ctx = empty_dataset_ctx();
        let err = optional_table(&ctx, "t", "nosuchdataset", "logs")
            .await
            .unwrap_err();
        assert!(matches!(err, QuerierError::QueryFailed(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn malformed_slug_is_rejected() {
        let ctx = empty_dataset_ctx();
        let err = optional_table(&ctx, "t; DROP TABLE", "d", "logs")
            .await
            .unwrap_err();
        assert!(matches!(err, QuerierError::InvalidInput(_)), "got {err:?}");
    }
}
