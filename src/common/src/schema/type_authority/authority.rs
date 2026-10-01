//! [`TypeAuthority`]: the cached tie-together of the catalog, the semconv
//! resolver, and config overrides. A writer resolves one [`SignalScope`] per
//! (tenant, dataset, signal) once per batch rather than per attribute.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::mapref::entry::Entry;
use dashmap::{DashMap, DashSet};

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

/// How long a [`SignalScope`] is served before the next [`TypeAuthority::scope`]
/// call rebuilds it. Schema-registry writes land in the router, whose
/// `SchemaResolver` is never this one -- not even in the monolith -- so
/// expiry is how a created, replaced or deleted registry's type hints reach
/// ingest. Each (tenant, dataset, signal) rebuild costs one registry listing
/// for the tenant, one catalog read per config-pinned key, and one per
/// attribute key as the new scope's per-key cache refills.
pub const DEFAULT_SCOPE_TTL: Duration = Duration::from_secs(30);

struct CachedScope {
    scope: Arc<SignalScope>,
    built_at: Instant,
}

enum Cached {
    /// Within the TTL: serve as is.
    Fresh(Arc<SignalScope>),
    /// Expired, and this caller claimed the rebuild; served if it fails.
    Claimed(Arc<SignalScope>),
    Missing,
}

/// Cached [`SignalScope`]s keyed by (tenant, dataset, signal), each served
/// for at most the scope TTL ([`DEFAULT_SCOPE_TTL`]).
pub struct TypeAuthority {
    catalog: Catalog,
    resolver: SchemaResolver,
    config: Arc<Configuration>,
    scopes: DashMap<ScopeKey, CachedScope>,
    scope_ttl: Duration,
}

impl TypeAuthority {
    pub fn new(catalog: Catalog, resolver: SchemaResolver, config: Arc<Configuration>) -> Self {
        Self {
            catalog,
            resolver,
            config,
            scopes: DashMap::new(),
            scope_ttl: DEFAULT_SCOPE_TTL,
        }
    }

    /// Overrides [`DEFAULT_SCOPE_TTL`]; `Duration::ZERO` rebuilds on every
    /// call.
    pub fn with_scope_ttl(mut self, ttl: Duration) -> Self {
        self.scope_ttl = ttl;
        self
    }

    /// The scope for (`tenant_id`, `dataset_id`, `signal`), building and
    /// caching it on first use or once the cached one is older than the scope
    /// TTL.
    ///
    /// Only the first caller to see an expired entry rebuilds it; it restarts
    /// the entry's clock, so concurrent callers keep serving the old scope
    /// meanwhile. A failed rebuild also serves the old scope (and retries one
    /// TTL later); only a first build with nothing cached returns the error.
    /// Concurrent first builds may each produce their own scope; the newest
    /// build wins the cache.
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
        let stale = match self.lookup(&key) {
            Cached::Fresh(scope) => return Ok(scope),
            Cached::Claimed(stale) => Some(stale),
            Cached::Missing => None,
        };
        let started = Instant::now();
        // A rebuilt scope keeps its predecessor's warned set, so an off-type
        // key still logs once per process rather than once per TTL.
        let off_type_warned = stale
            .as_ref()
            .map(|stale| Arc::clone(&stale.off_type_warned))
            .unwrap_or_default();

        let scope = match self
            .build_scope(tenant_id, dataset_id, signal, off_type_warned)
            .await
        {
            Ok(scope) => Arc::new(scope),
            Err(error) => {
                let Some(stale) = stale else {
                    return Err(error);
                };
                tracing::warn!(
                    tenant_id,
                    dataset_id,
                    signal,
                    error = %error,
                    "failed to rebuild the attribute type scope; serving the previous one"
                );
                return Ok(stale);
            }
        };

        // A slower build that started earlier must not replace a newer one.
        match self.scopes.entry(key) {
            Entry::Occupied(cached) if cached.get().built_at > started => {}
            entry => {
                entry.insert(CachedScope {
                    scope: Arc::clone(&scope),
                    built_at: started,
                });
            }
        }
        Ok(scope)
    }

    fn lookup(&self, key: &ScopeKey) -> Cached {
        let fresh = self
            .scopes
            .get(key)
            .filter(|cached| cached.built_at.elapsed() < self.scope_ttl)
            .map(|cached| Arc::clone(&cached.scope));
        if let Some(scope) = fresh {
            return Cached::Fresh(scope);
        }
        // Re-checked under the write lock: another caller may have claimed
        // the rebuild since the read above.
        match self.scopes.get_mut(key) {
            Some(cached) if cached.built_at.elapsed() < self.scope_ttl => {
                Cached::Fresh(Arc::clone(&cached.scope))
            }
            Some(mut cached) => {
                cached.built_at = Instant::now();
                Cached::Claimed(Arc::clone(&cached.scope))
            }
            None => Cached::Missing,
        }
    }

    async fn build_scope(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        signal: &str,
        off_type_warned: Arc<DashSet<(AttributeLevel, String)>>,
    ) -> Result<SignalScope, AuthorityError> {
        // Registry writes go through another resolver, so this one's cache
        // would never notice them.
        let hints = self.resolver.fresh_type_hints(tenant_id).await?;
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
            let stored_canonical = current.map(|stored| stored.canonical);
            if stored_canonical != Some(canonical) {
                self.catalog
                    .override_attribute_type(tenant_id, dataset_id, &field, canonical)
                    .await?;
                if let Some(from) = stored_canonical {
                    tracing::warn!(
                        tenant_id,
                        dataset_id,
                        signal,
                        level = level.as_str(),
                        key,
                        from = ?from,
                        to = ?canonical,
                        "config pin retyped an already-established attribute field"
                    );
                    crate::self_monitoring::app_metrics().record_attribute_type_mismatches(
                        tenant_id,
                        signal,
                        level.as_str(),
                        "pin_conflict",
                        1,
                    );
                }
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
            off_type_warned,
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
    /// (level, key) pairs already warned about for an off-type value, so a
    /// hot key logs once per process rather than once per batch.
    off_type_warned: Arc<DashSet<(AttributeLevel, String)>>,
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

    /// Records `count` off-type placements observed for `key` at `level`
    /// during one write: increments the catalog's `off_type_count` and the
    /// `signaldb.writer.attribute_type_mismatches` metric, and logs once per
    /// (level, key) per process. A catalog failure is logged and does not
    /// suppress the metric or the warning.
    pub async fn record_off_type(
        &self,
        level: AttributeLevel,
        key: &str,
        canonical: CanonicalType,
        observed: ObservedKind,
        count: i64,
    ) {
        let field = LogicalFieldId {
            source: self.signal.clone(),
            level: Some(level),
            name: key.to_string(),
        };
        if let Err(e) = self
            .catalog
            .record_off_type(&self.tenant_id, &self.dataset_id, &field, count)
            .await
        {
            tracing::warn!(
                tenant_id = %self.tenant_id,
                dataset_id = %self.dataset_id,
                signal = %self.signal,
                level = level.as_str(),
                key,
                error = %e,
                "failed to record off-type attribute occurrences in the catalog"
            );
        }

        crate::self_monitoring::app_metrics().record_attribute_type_mismatches(
            &self.tenant_id,
            &self.signal,
            level.as_str(),
            "off_type",
            count.max(0) as u64,
        );

        if self.off_type_warned.insert((level, key.to_string())) {
            tracing::warn!(
                tenant_id = %self.tenant_id,
                dataset_id = %self.dataset_id,
                signal = %self.signal,
                level = level.as_str(),
                key,
                canonical_type = ?canonical,
                observed_kind = ?observed,
                "attribute value sent under a different type than the field's canonical \
                 type; kept as sent in the residue"
            );
        }
    }
}
