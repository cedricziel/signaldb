//! A read-only, non-blocking cache of each (tenant, dataset, signal)'s
//! established canonical types, for callers (the acceptor) that must never
//! await the catalog on the ingest hot path and never place values or
//! establish types themselves — only the writer does that.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::{DashMap, DashSet};

use super::{CanonicalType, ObservedKind, Placement, place};
use crate::catalog::Catalog;
use crate::schema::logical::AttributeLevel;

type Key = (String, String, String);

/// One (tenant, dataset, signal)'s established canonical types, keyed by
/// attribute level and key.
#[derive(Debug, Default)]
pub struct TypeSnapshot {
    by_level: HashMap<AttributeLevel, HashMap<String, CanonicalType>>,
}

impl TypeSnapshot {
    /// The established canonical type for `key` at `level`, if any.
    pub fn get(&self, level: AttributeLevel, key: &str) -> Option<CanonicalType> {
        self.by_level.get(&level)?.get(key).copied()
    }

    pub fn is_empty(&self) -> bool {
        self.by_level.is_empty()
    }
}

struct CacheEntry {
    snapshot: Arc<TypeSnapshot>,
    fetched_at: Instant,
}

/// Read-only cache of established canonical types. [`get`](Self::get)
/// never awaits the catalog: it returns whatever is cached (possibly stale,
/// possibly absent) and, when the entry is missing or older than the TTL,
/// spawns at most one background refresh per key.
pub struct TypeSnapshots {
    catalog: Catalog,
    ttl: Duration,
    cache: DashMap<Key, CacheEntry>,
    in_flight: DashSet<Key>,
}

impl TypeSnapshots {
    pub fn new(catalog: Catalog, ttl: Duration) -> Self {
        Self {
            catalog,
            ttl,
            cache: DashMap::new(),
            in_flight: DashSet::new(),
        }
    }

    /// The cached snapshot for (`tenant_id`, `dataset_id`, `signal`), or
    /// `None` if it has never been fetched.
    pub fn get(
        self: &Arc<Self>,
        tenant_id: &str,
        dataset_id: &str,
        signal: &str,
    ) -> Option<Arc<TypeSnapshot>> {
        let key = (
            tenant_id.to_string(),
            dataset_id.to_string(),
            signal.to_string(),
        );
        let (snapshot, stale) = match self.cache.get(&key) {
            Some(entry) => (
                Some(Arc::clone(&entry.snapshot)),
                entry.fetched_at.elapsed() > self.ttl,
            ),
            None => (None, true),
        };

        if stale && self.in_flight.insert(key.clone()) {
            let this = Arc::clone(self);
            tokio::spawn(async move {
                this.refresh(&key.0, &key.1, &key.2).await;
                this.in_flight.remove(&key);
            });
        }
        snapshot
    }

    /// Fetches (`tenant_id`, `dataset_id`, `signal`) from the catalog. On
    /// error the previous snapshot is kept.
    pub async fn refresh(&self, tenant_id: &str, dataset_id: &str, signal: &str) {
        let rows = match self
            .catalog
            .list_attribute_types_for_table(tenant_id, dataset_id, signal)
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                tracing::debug!(
                    tenant_id,
                    dataset_id,
                    signal,
                    %error,
                    "failed to refresh canonical-type snapshot; keeping the stale snapshot"
                );
                return;
            }
        };

        let mut by_level: HashMap<AttributeLevel, HashMap<String, CanonicalType>> = HashMap::new();
        for row in rows {
            by_level
                .entry(row.level)
                .or_default()
                .insert(row.attr_key, row.canonical_type);
        }

        self.cache.insert(
            (
                tenant_id.to_string(),
                dataset_id.to_string(),
                signal.to_string(),
            ),
            CacheEntry {
                snapshot: Arc::new(TypeSnapshot { by_level }),
                fetched_at: Instant::now(),
            },
        );
    }
}

/// The (level, key) pairs among `attrs` whose observed kind differs from
/// the snapshot's established canonical type — i.e. [`Placement::Residue`]
/// with `off_type: true` — mapped to the (canonical, observed) pair that
/// made them off-type. A key absent from the snapshot, or observed as a
/// non-scalar, is never reported. A `BTreeMap` keyed by (level, key) gives
/// callers a deterministic order without requiring [`CanonicalType`] or
/// [`ObservedKind`] to be [`Ord`] themselves — only [`AttributeLevel`] and
/// `&str` need to be, and both already are.
pub fn off_type_keys<'a>(
    snapshot: &TypeSnapshot,
    attrs: impl Iterator<Item = (AttributeLevel, &'a str, ObservedKind)>,
) -> BTreeMap<(AttributeLevel, &'a str), (CanonicalType, ObservedKind)> {
    attrs
        .filter_map(|(level, key, observed)| {
            let canonical = snapshot.get(level, key)?;
            match place(Some(canonical), observed) {
                Placement::Residue { off_type: true } => {
                    Some(((level, key), (canonical, observed)))
                }
                _ => None,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::logical::LogicalFieldId;
    use crate::schema::type_authority::{Resolution, TypeSource};

    async fn seeded_catalog(
        tenant_id: &str,
        dataset_id: &str,
        signal: &str,
        level: AttributeLevel,
        key: &str,
        canonical: CanonicalType,
    ) -> Catalog {
        let catalog = Catalog::new_in_memory().await.unwrap();
        let field = LogicalFieldId {
            source: signal.to_string(),
            level: Some(level),
            name: key.to_string(),
        };
        catalog
            .establish_attribute_type(
                tenant_id,
                dataset_id,
                &field,
                Resolution {
                    canonical,
                    source: TypeSource::Observed,
                    hint_schema_url: None,
                },
            )
            .await
            .unwrap();
        catalog
    }

    #[tokio::test]
    async fn established_int64_key_sent_as_string_is_reported() {
        let catalog = seeded_catalog(
            "acme",
            "prod",
            "traces",
            AttributeLevel::Record,
            "http.status_code",
            CanonicalType::Int64,
        )
        .await;
        let snapshots = Arc::new(TypeSnapshots::new(catalog, Duration::from_secs(30)));
        snapshots.refresh("acme", "prod", "traces").await;
        let snapshot = snapshots.get("acme", "prod", "traces").unwrap();

        let off_type = off_type_keys(
            &snapshot,
            std::iter::once((
                AttributeLevel::Record,
                "http.status_code",
                ObservedKind::String,
            )),
        );

        assert_eq!(
            off_type,
            [(
                (AttributeLevel::Record, "http.status_code"),
                (CanonicalType::Int64, ObservedKind::String)
            )]
            .into_iter()
            .collect()
        );
    }

    #[tokio::test]
    async fn matching_int64_is_not_reported() {
        let catalog = seeded_catalog(
            "acme",
            "prod",
            "traces",
            AttributeLevel::Record,
            "http.status_code",
            CanonicalType::Int64,
        )
        .await;
        let snapshots = Arc::new(TypeSnapshots::new(catalog, Duration::from_secs(30)));
        snapshots.refresh("acme", "prod", "traces").await;
        let snapshot = snapshots.get("acme", "prod", "traces").unwrap();

        let off_type = off_type_keys(
            &snapshot,
            std::iter::once((
                AttributeLevel::Record,
                "http.status_code",
                ObservedKind::Int64,
            )),
        );

        assert!(off_type.is_empty());
    }

    #[tokio::test]
    async fn array_value_is_never_reported() {
        let catalog = seeded_catalog(
            "acme",
            "prod",
            "traces",
            AttributeLevel::Record,
            "http.status_code",
            CanonicalType::Int64,
        )
        .await;
        let snapshots = Arc::new(TypeSnapshots::new(catalog, Duration::from_secs(30)));
        snapshots.refresh("acme", "prod", "traces").await;
        let snapshot = snapshots.get("acme", "prod", "traces").unwrap();

        let off_type = off_type_keys(
            &snapshot,
            std::iter::once((
                AttributeLevel::Record,
                "http.status_code",
                ObservedKind::Array,
            )),
        );

        assert!(off_type.is_empty());
    }

    #[tokio::test]
    async fn unknown_key_is_never_reported() {
        let catalog = seeded_catalog(
            "acme",
            "prod",
            "traces",
            AttributeLevel::Record,
            "http.status_code",
            CanonicalType::Int64,
        )
        .await;
        let snapshots = Arc::new(TypeSnapshots::new(catalog, Duration::from_secs(30)));
        snapshots.refresh("acme", "prod", "traces").await;
        let snapshot = snapshots.get("acme", "prod", "traces").unwrap();

        let off_type = off_type_keys(
            &snapshot,
            std::iter::once((AttributeLevel::Record, "unknown.key", ObservedKind::String)),
        );

        assert!(off_type.is_empty());
    }

    #[tokio::test]
    async fn cold_cache_returns_none_without_blocking_then_the_snapshot_after_refresh() {
        let catalog = seeded_catalog(
            "acme",
            "prod",
            "traces",
            AttributeLevel::Record,
            "http.status_code",
            CanonicalType::Int64,
        )
        .await;
        let snapshots = Arc::new(TypeSnapshots::new(catalog, Duration::from_secs(30)));

        assert!(snapshots.get("acme", "prod", "traces").is_none());

        snapshots.refresh("acme", "prod", "traces").await;

        let snapshot = snapshots.get("acme", "prod", "traces").unwrap();
        assert_eq!(
            snapshot.get(AttributeLevel::Record, "http.status_code"),
            Some(CanonicalType::Int64)
        );
    }
}
