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

/// One demand counter key: (tenant, dataset, signal, attribute key).
pub type DemandKey = (String, String, String, String);

fn registry() -> &'static Mutex<HashMap<DemandKey, u64>> {
    static REGISTRY: OnceLock<Mutex<HashMap<DemandKey, u64>>> = OnceLock::new();
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

/// Take all accumulated counters, leaving the registry empty. Callers flush
/// the result into the catalog's `attribute_stats` table.
pub fn drain() -> Vec<(DemandKey, u64)> {
    match registry().lock() {
        Ok(mut map) => map.drain().collect(),
        Err(_) => Vec::new(),
    }
}

/// Spawn a background task that periodically drains the registry into the
/// catalog's `attribute_stats` table. Flush failures are logged and the
/// counters are dropped (advisory data — losing a window is acceptable).
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
}
