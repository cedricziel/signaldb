//! `oldest_data_ns` on a single-document Query IR answer: the earliest event
//! the queried signal's table holds for the caller's tenant and dataset, read
//! from Iceberg file statistics. A best-effort fact: any failure or a slow
//! catalog omits it, so it never fails the query and delays it by at most
//! [`LOOKUP_TIMEOUT`], once per [`CACHE_TTL`].

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use common::auth::TenantContext;
use common::retention::SignalType;
use tracing::debug;

use crate::RouterAppState;

/// How long the metadata lookup may take before the answer goes out without it.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(2);

/// How long a looked-up value is reused. The oldest event only moves when
/// retention or compaction drops files, so a short lag is harmless and spares
/// every query (and every tail poll) the catalog and manifest reads.
const CACHE_TTL: Duration = Duration::from_secs(60);

type CacheKey = (String, String, &'static str);

/// Looked-up values per tenant, dataset and table, held on the router state.
pub(crate) type OldestDataCache = Mutex<HashMap<CacheKey, (Instant, Option<i64>)>>;

/// The signal an IR source reads and its Iceberg table, for signal sources
/// only: the signal tables by name, plus `exemplars`, which reads the metric
/// exemplars table under the metrics signal.
pub(super) fn signal_source(source: &str) -> Option<(SignalType, &'static str)> {
    if source == "exemplars" {
        return Some((SignalType::Metrics, "metric_exemplars"));
    }
    let signal = SignalType::from_table_name(source).ok()?;
    let table = signal.table_name();
    (table == source).then_some((signal, table))
}

/// The earliest event timestamp, in unix nanoseconds, the source's table
/// holds for the caller. `None` for a non-signal source, an empty or missing
/// table, or a lookup that failed or timed out.
pub(super) async fn oldest_data_ns(
    state: &RouterAppState,
    ctx: &TenantContext,
    source: &str,
) -> Option<i64> {
    let (_, table) = signal_source(source)?;
    let key = (ctx.tenant_id.clone(), ctx.dataset_id.clone(), table);
    if let Some(&(at, oldest)) = state.oldest_data_cache.lock().ok()?.get(&key)
        && at.elapsed() < CACHE_TTL
    {
        return oldest;
    }
    let lookup = async {
        let manager = crate::catalog_manager(state).await?;
        manager
            .oldest_event_ns(&ctx.tenant_id, &ctx.dataset_id, table)
            .await
    };
    let (oldest, error) = match tokio::time::timeout(LOOKUP_TIMEOUT, lookup).await {
        Ok(Ok(oldest)) => (oldest, None),
        Ok(Err(error)) => (None, Some(error.to_string())),
        Err(_) => (
            None,
            Some(format!("timed out after {}ms", LOOKUP_TIMEOUT.as_millis())),
        ),
    };
    if let Ok(mut cache) = state.oldest_data_cache.lock() {
        cache.insert(key, (Instant::now(), oldest));
    }
    let Some(error) = error else {
        return oldest;
    };
    debug!(
        signaldb.tenant.id = %ctx.tenant_id,
        signaldb.dataset.id = %ctx.dataset_id,
        table,
        error = %error,
        "oldest_data_ns lookup failed; omitting it"
    );
    None
}

#[cfg(test)]
mod tests {
    use super::signal_source;

    #[test]
    fn signal_sources_map_to_their_tables() {
        let table = |source| signal_source(source).map(|(_, table)| table);
        assert_eq!(table("traces"), Some("traces"));
        assert_eq!(table("logs"), Some("logs"));
        assert_eq!(table("metrics"), Some("metrics"));
        assert_eq!(table("profiles"), Some("profiles"));
        assert_eq!(table("exemplars"), Some("metric_exemplars"));
    }

    #[test]
    fn other_sources_have_no_table() {
        for source in ["scalar", "service_graph", "metrics_gauge", ""] {
            assert!(signal_source(source).is_none(), "{source}");
        }
    }
}
