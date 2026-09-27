//! # Attribute Query-Demand Registry
//!
//! In-process counters for "this attribute key was used in a query filter"
//! (epic #737, #733). The querier records a hit at its query entrypoints —
//! LogQL/PromQL/trace-search — for every label that is *not* backed by a
//! dedicated column, i.e. the ones that would benefit from materialization.
//! A background task periodically drains the registry into the service
//! catalog's `attribute_stats` table, where the compactor's advisory
//! analyzer joins demand with scan-side presence/cardinality.
//!
//! The registry is a process-global so the deeply nested query lowering
//! does not need a counter handle threaded through it; the entrypoints
//! (which know the tenant/dataset/signal) do the recording.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use crate::schema::logical::AttributeLevel;

/// One demand counter key: (tenant, dataset, signal, attribute key).
pub type DemandKey = (String, String, String, String);

/// One per-level demand counter key: (tenant, dataset, signal, level,
/// attribute key). Kept in a separate registry from [`DemandKey`] so the
/// unqualified `record`/`drain` pair (still flushed into `attribute_stats`)
/// is unaffected by level-aware callers (change: otel-native-schema layer 6).
pub type LevelDemandKey = (String, String, String, AttributeLevel, String);

fn registry() -> &'static Mutex<HashMap<DemandKey, u64>> {
    static REGISTRY: OnceLock<Mutex<HashMap<DemandKey, u64>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn level_registry() -> &'static Mutex<HashMap<LevelDemandKey, u64>> {
    static REGISTRY: OnceLock<Mutex<HashMap<LevelDemandKey, u64>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Record one query-filter hit for an attribute key.
pub fn record(tenant_id: &str, dataset_id: &str, signal: &str, attr_key: &str) {
    let key = (
        tenant_id.to_string(),
        dataset_id.to_string(),
        signal.to_string(),
        attr_key.to_string(),
    );
    if let Ok(mut map) = registry().lock() {
        *map.entry(key).or_insert(0) += 1;
    }
}

/// Record one query-filter hit for an attribute key at a specific
/// [`AttributeLevel`], flushed into `attribute_level_stats` (layer 6's
/// per-level promotion demand, separate from the flat `record` above).
pub fn record_level(
    tenant_id: &str,
    dataset_id: &str,
    signal: &str,
    level: AttributeLevel,
    attr_key: &str,
) {
    let key = (
        tenant_id.to_string(),
        dataset_id.to_string(),
        signal.to_string(),
        level,
        attr_key.to_string(),
    );
    if let Ok(mut map) = level_registry().lock() {
        *map.entry(key).or_insert(0) += 1;
    }
}

/// Take all accumulated counters, leaving the registry empty. Callers flush
/// the result into the catalog's `attribute_stats` table.
pub fn drain() -> Vec<(DemandKey, u64)> {
    match registry().lock() {
        Ok(mut map) => map.drain().collect(),
        Err(_) => Vec::new(),
    }
}

/// Take all accumulated per-level counters, leaving the registry empty.
/// Callers flush the result into the catalog's `attribute_level_stats` table.
pub fn drain_level() -> Vec<(LevelDemandKey, u64)> {
    match level_registry().lock() {
        Ok(mut map) => map.drain().collect(),
        Err(_) => Vec::new(),
    }
}

/// Spawn a background task that periodically drains both registries into the
/// catalog's `attribute_stats`/`attribute_level_stats` tables. Flush failures
/// are logged and the counters are dropped (advisory data — losing a window
/// is acceptable).
pub fn spawn_flusher(
    catalog: std::sync::Arc<crate::catalog::Catalog>,
    interval: std::time::Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            for ((tenant, dataset, signal, key), hits) in drain() {
                if let Err(e) = catalog
                    .add_attribute_query_hits(&tenant, &dataset, &signal, &key, hits as i64)
                    .await
                {
                    tracing::warn!(error = %e, attr_key = %key, "Failed to flush attribute query-demand counter");
                }
            }
            let queried_at = chrono::Utc::now();
            for ((tenant, dataset, signal, level, key), hits) in drain_level() {
                if let Err(e) = catalog
                    .add_attribute_level_query_hits(
                        &tenant,
                        &dataset,
                        &signal,
                        level,
                        &key,
                        hits as i64,
                        queried_at,
                    )
                    .await
                {
                    tracing::warn!(error = %e, attr_key = %key, level = level.as_str(), "Failed to flush per-level attribute query-demand counter");
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    /// A single test covers record+drain because the registry is
    /// process-global — parallel tests would observe each other's counts.
    #[test]
    fn record_accumulates_and_drain_empties() {
        super::record("t", "d", "logs", "namespace");
        super::record("t", "d", "logs", "namespace");
        super::record("t", "d", "traces", "http.method");
        let mut drained = super::drain();
        drained.sort();
        let ns = drained
            .iter()
            .find(|((_, _, s, k), _)| s == "logs" && k == "namespace")
            .expect("namespace counter");
        assert_eq!(ns.1, 2);
        assert!(
            drained
                .iter()
                .any(|((_, _, s, k), _)| s == "traces" && k == "http.method")
        );
        assert!(super::drain().is_empty());
    }

    /// Same coverage as `record_accumulates_and_drain_empties`, for the
    /// per-level registry — level-qualified hits must not collide with the
    /// flat registry or with a different level of the same key.
    #[test]
    fn record_level_accumulates_and_drain_level_empties() {
        use crate::schema::logical::AttributeLevel;

        super::record_level("t", "d", "logs", AttributeLevel::Record, "namespace");
        super::record_level("t", "d", "logs", AttributeLevel::Record, "namespace");
        super::record_level("t", "d", "logs", AttributeLevel::Resource, "namespace");
        let mut drained = super::drain_level();
        drained.sort();
        let record_level_hits = drained
            .iter()
            .find(|((_, _, _, level, k), _)| *level == AttributeLevel::Record && k == "namespace")
            .expect("record-level namespace counter");
        assert_eq!(record_level_hits.1, 2);
        let resource_level_hits = drained
            .iter()
            .find(|((_, _, _, level, k), _)| *level == AttributeLevel::Resource && k == "namespace")
            .expect("resource-level namespace counter");
        assert_eq!(resource_level_hits.1, 1);
        assert!(super::drain_level().is_empty());
    }
}
