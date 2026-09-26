//! [`TypeAuthority`]: the cached tie-together of the catalog, the semconv
//! resolver, and config overrides. A writer resolves one [`SignalScope`] per
//! (tenant, dataset, signal) once per batch rather than per attribute.

use std::collections::HashSet;
use std::sync::Arc;

use dashmap::DashMap;

use super::{CanonicalType, ObservedKind, SchemaUrls, resolve};
use crate::catalog::Catalog;
use crate::config::{Configuration, SchemaConfig};
use crate::schema::logical::{AttributeLevel, LogicalFieldId};
use crate::schema_registry::StoreError as RegistryStoreError;
use crate::schema_registry::{SchemaResolver, SemconvTypeHints};

/// Errors surfaced while resolving or caching a [`SignalScope`].
#[derive(Debug, thiserror::Error)]
pub enum AuthorityError {
    #[error(transparent)]
    Store(#[from] super::StoreError),
    #[error(transparent)]
    Registry(#[from] RegistryStoreError),
}

type ScopeKey = (String, String, String);

/// Cached [`SignalScope`]s keyed by (tenant, dataset, signal).
pub struct TypeAuthority {
    catalog: Catalog,
    resolver: SchemaResolver,
    config: Arc<Configuration>,
    scopes: DashMap<ScopeKey, Arc<SignalScope>>,
}

impl TypeAuthority {
    pub fn new(catalog: Catalog, resolver: SchemaResolver, config: Arc<Configuration>) -> Self {
        Self {
            catalog,
            resolver,
            config,
            scopes: DashMap::new(),
        }
    }

    /// The scope for (`tenant_id`, `dataset_id`, `signal`), building and
    /// caching it on first use. Concurrent first calls may each build their
    /// own scope; the caller keeps whichever `Arc` it already holds, and the
    /// cache converges on the last one inserted.
    pub async fn scope(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        signal: &str,
    ) -> Result<Arc<SignalScope>, AuthorityError> {
        let key = (
            tenant_id.to_string(),
            dataset_id.to_string(),
            signal.to_string(),
        );
        if let Some(scope) = self.scopes.get(&key) {
            return Ok(Arc::clone(&scope));
        }

        let scope = Arc::new(self.build_scope(tenant_id, dataset_id, signal).await?);
        self.scopes.insert(key, Arc::clone(&scope));
        Ok(scope)
    }

    /// Drop every cached scope. A schema-version bump ships as a new binary
    /// (a fresh process already starts with an empty cache), so the only
    /// caller that matters here is a config reload within one running
    /// process: it must see retyped or newly pinned fields on the next
    /// `scope` call.
    pub fn invalidate(&self) {
        self.scopes.clear();
    }

    async fn build_scope(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        signal: &str,
    ) -> Result<SignalScope, AuthorityError> {
        let hints = self.resolver.type_hints(tenant_id).await?;
        let schema = self.config.get_tenant_schema_config(tenant_id);

        let keys: HashSet<(AttributeLevel, &str)> = schema
            .attribute_types
            .iter()
            .filter(|o| o.signal.as_str() == signal)
            .map(|o| (o.level, o.key.as_str()))
            .collect();

        for (level, key) in keys {
            let field = LogicalFieldId {
                source: signal.to_string(),
                level: Some(level),
                name: key.to_string(),
            };
            let Some(canonical) = schema.attribute_type_override(dataset_id, &field) else {
                continue;
            };
            let current = self
                .catalog
                .get_attribute_type(tenant_id, dataset_id, &field)
                .await?;
            if current.map(|stored| stored.canonical) != Some(canonical) {
                self.catalog
                    .override_attribute_type(tenant_id, dataset_id, &field, canonical)
                    .await?;
            }
        }

        Ok(SignalScope {
            catalog: self.catalog.clone(),
            hints,
            schema,
            tenant_id: tenant_id.to_string(),
            dataset_id: dataset_id.to_string(),
            signal: signal.to_string(),
            resource: DashMap::new(),
            scope_level: DashMap::new(),
            record: DashMap::new(),
        })
    }
}

/// One (tenant, dataset, signal)'s resolved semconv hints, config overrides,
/// and a cache of already-committed canonical types. Cheap to hold across a
/// batch; `canonical` hits are a `&str` lookup with no allocation.
pub struct SignalScope {
    catalog: Catalog,
    hints: SemconvTypeHints,
    schema: SchemaConfig,
    tenant_id: String,
    dataset_id: String,
    signal: String,
    resource: DashMap<String, CanonicalType>,
    scope_level: DashMap<String, CanonicalType>,
    record: DashMap<String, CanonicalType>,
}

impl SignalScope {
    fn cache_for(&self, level: AttributeLevel) -> &DashMap<String, CanonicalType> {
        match level {
            AttributeLevel::Resource => &self.resource,
            AttributeLevel::Scope => &self.scope_level,
            AttributeLevel::Record => &self.record,
        }
    }

    /// The canonical type for `key` at `level`, resolving and committing it
    /// on first sight. `None` means the first observation was a non-scalar
    /// (array/kvlist/bytes/empty), which establishes nothing.
    pub async fn canonical(
        &self,
        level: AttributeLevel,
        key: &str,
        urls: SchemaUrls<'_>,
        observed: ObservedKind,
    ) -> Result<Option<CanonicalType>, AuthorityError> {
        let cache = self.cache_for(level);
        if let Some(canonical) = cache.get(key) {
            return Ok(Some(*canonical));
        }

        let field = LogicalFieldId {
            source: self.signal.clone(),
            level: Some(level),
            name: key.to_string(),
        };

        if let Some(stored) = self
            .catalog
            .get_attribute_type(&self.tenant_id, &self.dataset_id, &field)
            .await?
        {
            cache.insert(key.to_string(), stored.canonical);
            return Ok(Some(stored.canonical));
        }

        let config_override = self
            .schema
            .attribute_type_override(&self.dataset_id, &field);
        let Some(resolution) = resolve(config_override, &self.hints, urls, level, key, observed)
        else {
            return Ok(None);
        };

        let stored = self
            .catalog
            .establish_attribute_type(&self.tenant_id, &self.dataset_id, &field, resolution)
            .await?;
        cache.insert(key.to_string(), stored.canonical);
        Ok(Some(stored.canonical))
    }
}
