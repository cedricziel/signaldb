//! Canonical attribute types for the typed attribute layout (design
//! `otel-native-schema`, task 4.4).
//!
//! A typed-layout table has one canonical type per (tenant, dataset, signal,
//! level, key), committed by the writer's type authority into the
//! `attribute_types` catalog table. [`CanonicalTypeLookup`] fetches that map
//! for one query's tenant/dataset/signal scope; [`CatalogCanonicalTypes`] is
//! the production implementation, resolving the query's slugs to the ids the
//! writer recorded types under. Nothing in this PR resolves a field against
//! the fetched map yet — that lands with the resolver in a later PR of this
//! design.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use common::CatalogManager;
use common::catalog::Catalog;
use common::schema::logical::AttributeLevel;
use common::schema::type_authority::{AttributeKeyType, CanonicalType};

use super::error::QuerierError;

/// Every canonical type committed for one (tenant, dataset, signal) table,
/// keyed by (level, key) since the same key name can be recorded at more
/// than one attribute level (e.g. both resource- and record-scoped).
#[derive(Debug, Clone, Default)]
pub(crate) struct CanonicalTypes(HashMap<(AttributeLevel, String), CanonicalType>);

impl CanonicalTypes {
    /// The canonical type committed for `key` at `level`, if any.
    // Consumed by the typed attribute resolver (otel-native-schema task 4.4,
    // not yet added).
    #[allow(dead_code)]
    pub(crate) fn get(&self, level: AttributeLevel, key: &str) -> Option<CanonicalType> {
        self.0.get(&(level, key.to_string())).copied()
    }
}

impl FromIterator<AttributeKeyType> for CanonicalTypes {
    fn from_iter<I: IntoIterator<Item = AttributeKeyType>>(iter: I) -> Self {
        Self(
            iter.into_iter()
                .map(|t| ((t.level, t.attr_key), t.canonical_type))
                .collect(),
        )
    }
}

/// Resolves the canonical attribute types committed for one tenant/dataset/
/// signal scope.
#[async_trait]
pub(crate) trait CanonicalTypeLookup: Send + Sync {
    async fn canonical_types(
        &self,
        tenant_slug: &str,
        dataset_slug: &str,
        signal: &str,
    ) -> Result<CanonicalTypes, QuerierError>;
}

/// The production [`CanonicalTypeLookup`]: resolves the query's tenant/
/// dataset slugs to the ids the writer's type authority recorded types
/// under (via the tenant registry), then fetches the committed types.
pub(crate) struct CatalogCanonicalTypes {
    pub(crate) catalog_manager: Arc<CatalogManager>,
    pub(crate) catalog: Arc<Catalog>,
}

#[async_trait]
impl CanonicalTypeLookup for CatalogCanonicalTypes {
    async fn canonical_types(
        &self,
        tenant_slug: &str,
        dataset_slug: &str,
        signal: &str,
    ) -> Result<CanonicalTypes, QuerierError> {
        let tenant = self
            .catalog_manager
            .resolve_tenant_by_slug(tenant_slug)
            .await
            .map_err(|e| {
                QuerierError::QueryFailed(datafusion::error::DataFusionError::External(e.into()))
            })?
            .ok_or_else(|| QuerierError::InvalidInput(format!("unknown tenant '{tenant_slug}'")))?;
        let dataset = tenant
            .datasets
            .iter()
            .find(|d| d.slug == dataset_slug)
            .ok_or_else(|| {
                QuerierError::InvalidInput(format!(
                    "unknown dataset '{dataset_slug}' for tenant '{tenant_slug}'"
                ))
            })?;
        let types = self
            .catalog
            .list_attribute_types_for_table(&tenant.id, &dataset.id, signal)
            .await
            .map_err(|e| {
                QuerierError::QueryFailed(datafusion::error::DataFusionError::External(Box::new(e)))
            })?;
        Ok(types.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::config::{AuthConfig, Configuration, DatasetConfig, StorageConfig, TenantConfig};
    use common::schema::logical::LogicalFieldId;
    use common::schema::type_authority::{Resolution, TypeSource};

    fn test_config() -> Configuration {
        Configuration {
            auth: AuthConfig {
                tenants: vec![TenantConfig {
                    id: "acme".to_string(),
                    slug: "acme".to_string(),
                    name: "Acme Corp".to_string(),
                    default_dataset: Some("production".to_string()),
                    datasets: vec![DatasetConfig {
                        id: "production".to_string(),
                        slug: "prod".to_string(),
                        is_default: true,
                        storage: None,
                    }],
                    api_keys: vec![],
                    schema_config: None,
                    limits: None,
                }],
                ..Default::default()
            },
            storage: StorageConfig {
                dsn: "memory://".to_string(),
            },
            ..Configuration::default()
        }
    }

    #[tokio::test]
    async fn resolves_slugs_to_ids_and_fetches_types() {
        let catalog = Arc::new(Catalog::new_in_memory().await.unwrap());
        catalog
            .establish_attribute_type(
                "acme",
                "production",
                &LogicalFieldId {
                    source: "traces".to_string(),
                    level: Some(AttributeLevel::Record),
                    name: "http.status_code".to_string(),
                },
                Resolution {
                    canonical: CanonicalType::Int64,
                    source: TypeSource::Observed,
                    hint_schema_url: None,
                },
            )
            .await
            .unwrap();

        let manager = Arc::new(
            CatalogManager::new(test_config())
                .await
                .unwrap()
                .with_tenant_source(catalog.clone()),
        );
        let lookup = CatalogCanonicalTypes {
            catalog_manager: manager,
            catalog: catalog.clone(),
        };

        // The query addresses the tenant by its slug ("acme", same as its
        // id here) and the dataset by its slug ("prod"), distinct from the
        // id ("production") the writer recorded types under.
        let types = lookup
            .canonical_types("acme", "prod", "traces")
            .await
            .unwrap();
        assert_eq!(
            types.get(AttributeLevel::Record, "http.status_code"),
            Some(CanonicalType::Int64)
        );
        assert_eq!(types.get(AttributeLevel::Record, "unknown.key"), None);
    }

    #[tokio::test]
    async fn unknown_tenant_slug_is_an_error() {
        let catalog = Arc::new(Catalog::new_in_memory().await.unwrap());
        let manager = Arc::new(
            CatalogManager::new(test_config())
                .await
                .unwrap()
                .with_tenant_source(catalog.clone()),
        );
        let lookup = CatalogCanonicalTypes {
            catalog_manager: manager,
            catalog,
        };

        let err = lookup
            .canonical_types("nope", "prod", "traces")
            .await
            .unwrap_err();
        assert!(matches!(err, QuerierError::InvalidInput(_)));
    }
}
