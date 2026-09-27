//! Test-only helpers for constructing a writer that can actually commit,
//! independent of whether the target table is in the legacy or the typed
//! attribute layout.
//!
//! Every signal table's current `schemas.toml` version is the typed
//! attribute layout (`otel-native-schema` layer 4, one-shot cutover), so any
//! [`IcebergTableWriter`] that commits a batch containing an attribute
//! container needs a `TypeAuthority` attached -- see
//! [`writer::storage::IcebergTableWriter::append_batches_with_marker`]'s
//! hard error for a typed table with no configured authority. Against a
//! table still in the legacy layout this authority is simply never
//! consulted, so this constructor is safe to use unconditionally.

use anyhow::Result;
use common::CatalogManager;
use common::config::{Configuration, WriterConfig};
use common::schema::type_authority::TypeAuthority;
use common::schema_registry::SchemaResolver;
use common::wal::manager::WalManager;
use std::sync::Arc;
use writer::{IcebergTableWriter, IcebergWriterFlightService, WalProcessor};

/// Same signature as [`IcebergTableWriter::new`], but with a fresh
/// in-memory-catalog-backed `TypeAuthority` attached.
pub async fn writer_with_type_authority(
    catalog_manager: &CatalogManager,
    tenant_id: String,
    dataset_id: String,
    table_name: String,
) -> Result<IcebergTableWriter> {
    let writer =
        IcebergTableWriter::new(catalog_manager, tenant_id, dataset_id, table_name).await?;
    Ok(writer.with_type_authority(test_type_authority().await?))
}

/// Same signature as `WalProcessor::new`, but with a fresh
/// in-memory-catalog-backed `TypeAuthority` attached.
pub async fn processor_with_type_authority(
    wal_manager: Arc<WalManager>,
    catalog_manager: Arc<CatalogManager>,
) -> Result<WalProcessor> {
    Ok(WalProcessor::new(wal_manager, catalog_manager)
        .with_type_authority(test_type_authority().await?))
}

/// Same signature as `IcebergWriterFlightService::new`, but with a fresh
/// in-memory-catalog-backed `TypeAuthority` attached.
pub async fn writer_service_with_type_authority(
    catalog_manager: Arc<CatalogManager>,
    wal_manager: Arc<WalManager>,
    writer_config: &WriterConfig,
) -> Result<IcebergWriterFlightService> {
    Ok(IcebergWriterFlightService::with_type_authority(
        catalog_manager,
        wal_manager,
        writer_config,
        test_type_authority().await?,
    ))
}

/// A `TypeAuthority` backed by a fresh in-memory SQL catalog -- independent
/// of the target table's data catalog, since it only tracks canonical
/// attribute types, not table data.
pub async fn test_type_authority() -> Result<Arc<TypeAuthority>> {
    let sql_catalog = common::catalog::Catalog::new_in_memory().await?;
    let resolver = SchemaResolver::new(sql_catalog.clone());
    Ok(Arc::new(TypeAuthority::new(
        sql_catalog,
        resolver,
        Arc::new(Configuration::default()),
    )))
}
