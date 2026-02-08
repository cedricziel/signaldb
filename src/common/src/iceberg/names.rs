//! Iceberg naming utilities for constructing table identifiers and namespaces.

use anyhow::Result;
use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::namespace::Namespace;

/// Build an Iceberg table Identifier from tenant/dataset slugs and table name.
///
/// The namespace path is `[tenant_slug, dataset_slug]` and the table name is appended.
pub fn build_table_identifier(
    tenant_slug: &str,
    dataset_slug: &str,
    table_name: &str,
) -> Identifier {
    Identifier::new(
        &[tenant_slug.to_string(), dataset_slug.to_string()],
        table_name,
    )
}

/// Build an Iceberg Namespace from tenant/dataset slugs.
pub fn build_namespace(tenant_slug: &str, dataset_slug: &str) -> Result<Namespace> {
    Namespace::try_new(&[tenant_slug.to_string(), dataset_slug.to_string()])
        .map_err(|e| anyhow::anyhow!("Failed to create namespace: {e}"))
}

/// Build a table location path relative to the object store root.
///
/// Format: `{tenant_slug}/{dataset_slug}/{table_name}`
pub fn build_table_location(tenant_slug: &str, dataset_slug: &str, table_name: &str) -> String {
    format!("{tenant_slug}/{dataset_slug}/{table_name}")
}
