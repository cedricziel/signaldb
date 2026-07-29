//! # Attribute statistics analyzer (read-only)
//!
//! Computes per-key statistics over a table's attribute columns while the
//! compactor is already scanning the data for a rewrite: presence (how many
//! rows carry the key) and an approximate distinct-value count. The
//! analyzer then *logs* which keys would be promoted to materialized
//! `label_<key>` columns under a schema-width budget — it changes nothing.
//!
//! This is the de-risking first half of auto-materialization (epic #737,
//! Layer 4a): validating the guardrails — especially the cardinality
//! estimator — against real data before any rewrite-coupled promotion.
//! Persisting stats to a catalog table and folding in query-demand
//! counters are follow-ups tracked on the issue.

use std::collections::{BTreeMap, BTreeSet};

use datafusion::arrow::array::{Array, MapArray, RecordBatch, StringArray};

/// Attribute columns recognized across the signal tables.
const ATTR_COLUMNS: &[&str] = &[
    "log_attributes",
    "span_attributes",
    "resource_attributes",
    "scope_attributes",
    "attributes",
    "profile_attributes",
];

/// The signal a table's statistics are recorded under.
pub fn signal_of_table(table_name: &str) -> &'static str {
    match table_name {
        "traces" => "traces",
        "logs" => "logs",
        "profiles" => "profiles",
        t if t.starts_with("metrics_") => "metrics",
        _ => "unknown",
    }
}

/// Cap on the tracked distinct values per key. Keys that exceed it are
/// reported as `>= CARDINALITY_CAP` and are never promotion candidates.
const CARDINALITY_CAP: usize = 10_000;

/// The maximum number of keys the analyzer would promote (schema-width
/// budget, minus whatever is already materialized — the log is advisory).
const PROMOTION_BUDGET: usize = 32;

/// Minimum fraction of rows that must carry a key for it to be a
/// promotion candidate.
const MIN_PRESENCE: f64 = 0.005;

/// Per-key statistics over the scanned rows.
#[derive(Debug, Default, Clone)]
pub struct AttrFieldStats {
    /// Rows in which the key appeared (across all attribute columns).
    pub present_rows: u64,
    /// Distinct values observed, capped at [`CARDINALITY_CAP`].
    pub distinct: usize,
    /// Whether the distinct tracking hit the cap (true cardinality is
    /// at least [`CARDINALITY_CAP`]).
    pub capped: bool,
}

/// Analyze the attribute columns of the given batches, returning per-key
/// statistics plus the total row count scanned.
pub fn analyze_batches(batches: &[RecordBatch]) -> (BTreeMap<String, AttrFieldStats>, u64) {
    let mut stats: BTreeMap<String, AttrFieldStats> = BTreeMap::new();
    let mut values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut total_rows: u64 = 0;

    for batch in batches {
        total_rows += batch.num_rows() as u64;
        for column in ATTR_COLUMNS {
            let Some(array) = batch.column_by_name(column) else {
                continue;
            };
            for doc in attr_documents(array.as_ref()) {
                let Some(doc) = doc else { continue };
                for (key, value) in doc {
                    let entry = stats.entry(key.clone()).or_default();
                    entry.present_rows += 1;
                    let set = values.entry(key).or_default();
                    if entry.capped {
                        continue;
                    }
                    set.insert(value);
                    if set.len() >= CARDINALITY_CAP {
                        entry.capped = true;
                    }
                }
            }
        }
    }
    for (key, set) in values {
        if let Some(entry) = stats.get_mut(&key) {
            entry.distinct = set.len();
        }
    }
    (stats, total_rows)
}

/// Log the promotion candidates for a table: keys that clear the presence
/// floor and the cardinality cap, ranked by presence, truncated to the
/// budget. Purely advisory — nothing is changed.
pub fn log_promotion_candidates(
    table_name: &str,
    stats: &BTreeMap<String, AttrFieldStats>,
    total_rows: u64,
) {
    if total_rows == 0 || stats.is_empty() {
        return;
    }
    let mut candidates: Vec<(&String, &AttrFieldStats)> = stats
        .iter()
        .filter(|(_, s)| !s.capped && (s.present_rows as f64 / total_rows as f64) >= MIN_PRESENCE)
        .collect();
    candidates.sort_by_key(|(_, s)| std::cmp::Reverse(s.present_rows));
    candidates.truncate(PROMOTION_BUDGET);

    let rejected_cardinality = stats.values().filter(|s| s.capped).count();
    let summary: Vec<String> = candidates
        .iter()
        .map(|(k, s)| {
            format!(
                "{k} (presence {:.1}%, distinct {})",
                100.0 * s.present_rows as f64 / total_rows as f64,
                s.distinct
            )
        })
        .collect();
    tracing::info!(
        table = %table_name,
        total_rows,
        keys_seen = stats.len(),
        rejected_high_cardinality = rejected_cardinality,
        candidates = %summary.join(", "),
        "Attribute-stats analyzer: promotion candidates (advisory)"
    );
}

/// Iterate an attribute column's per-row documents as key/value pairs,
/// handling both storage forms: `Map<Utf8, Utf8>` (new tables) and Utf8
/// columns holding flat JSON objects (legacy tables). Also used by the
/// rewrite-coupled promotion backfill in [`crate::attr_promotion`].
pub(crate) fn attr_documents(array: &dyn Array) -> Vec<Option<Vec<(String, String)>>> {
    if let Some(map) = array.as_any().downcast_ref::<MapArray>() {
        return (0..map.len())
            .map(|i| {
                if map.is_null(i) {
                    return None;
                }
                let entries = map.value(i);
                let keys = entries.column(0).as_any().downcast_ref::<StringArray>()?;
                let vals = entries.column(1).as_any().downcast_ref::<StringArray>()?;
                let mut doc = Vec::with_capacity(entries.len());
                for j in 0..entries.len() {
                    if !keys.is_null(j) && !vals.is_null(j) {
                        doc.push((keys.value(j).to_string(), vals.value(j).to_string()));
                    }
                }
                Some(doc)
            })
            .collect();
    }
    if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
        return (0..strings.len())
            .map(|i| {
                if strings.is_null(i) {
                    return None;
                }
                match serde_json::from_str::<serde_json::Value>(strings.value(i)) {
                    Ok(serde_json::Value::Object(map)) => Some(
                        map.into_iter()
                            .map(|(k, v)| {
                                let rendered = match v {
                                    serde_json::Value::String(s) => s,
                                    other => other.to_string(),
                                };
                                (k, rendered)
                            })
                            .collect(),
                    ),
                    _ => None,
                }
            })
            .collect();
    }
    Vec::new()
}

/// Persist the analyzer's per-key statistics into the service catalog's
/// `attribute_stats` table (epic #737, #733), keyed by
/// (tenant, dataset, signal, key). Failures are logged and swallowed —
/// the stats are advisory and must never fail a compaction.
pub async fn persist_stats(
    catalog: &common::catalog::Catalog,
    tenant_id: &str,
    dataset_id: &str,
    table_name: &str,
    stats: &BTreeMap<String, AttrFieldStats>,
    total_rows: u64,
) {
    let signal = signal_of_table(table_name);
    for (key, s) in stats {
        if let Err(e) = catalog
            .upsert_attribute_scan_stats(
                tenant_id,
                dataset_id,
                signal,
                key,
                s.present_rows as i64,
                total_rows as i64,
                s.distinct as i64,
                s.capped,
            )
            .await
        {
            tracing::warn!(error = %e, attr_key = %key, "Failed to persist attribute scan stats");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn analyzer_computes_presence_and_cardinality_and_ranks_candidates() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "log_attributes",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                Some(r#"{"namespace":"prod","pod":"a"}"#),
                Some(r#"{"namespace":"prod","pod":"b"}"#),
                Some(r#"{"namespace":"staging"}"#),
                None,
            ]))],
        )
        .unwrap();

        let (stats, total) = analyze_batches(&[batch]);
        assert_eq!(total, 4);
        let ns = &stats["namespace"];
        assert_eq!(ns.present_rows, 3);
        assert_eq!(ns.distinct, 2);
        assert!(!ns.capped);
        assert_eq!(stats["pod"].present_rows, 2);
        assert_eq!(stats["pod"].distinct, 2);

        // Advisory logging must not panic on real stats or empty input.
        log_promotion_candidates("logs", &stats, total);
        log_promotion_candidates("logs", &BTreeMap::new(), 0);
    }

    #[tokio::test]
    async fn persist_stats_writes_scan_rows_under_the_signal() {
        let catalog = common::catalog::Catalog::new("sqlite::memory:")
            .await
            .unwrap();
        let mut stats = BTreeMap::new();
        stats.insert(
            "namespace".to_string(),
            AttrFieldStats {
                present_rows: 80,
                distinct: 5,
                capped: false,
            },
        );
        super::persist_stats(&catalog, "t", "d", "metrics_gauge", &stats, 100).await;

        let rows = catalog
            .get_attribute_stats("t", "d", "metrics")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].attr_key, "namespace");
        assert_eq!(rows[0].present_rows, 80);
        assert_eq!(rows[0].total_rows, 100);
        assert_eq!(rows[0].distinct_estimate, 5);
        assert!(!rows[0].capped);
    }

    #[test]
    fn signal_of_table_maps_all_tables() {
        assert_eq!(super::signal_of_table("traces"), "traces");
        assert_eq!(super::signal_of_table("logs"), "logs");
        assert_eq!(super::signal_of_table("metrics_histogram"), "metrics");
        assert_eq!(super::signal_of_table("profiles"), "profiles");
    }
}
