//! # Attribute statistics analyzer (read-only)
//!
//! Computes per-key statistics over a table's attribute columns while the
//! compactor is already scanning the data for a rewrite: presence (how many
//! rows carry the key), an approximate distinct-value count, and a bounded
//! sketch of the key's most frequent values. The
//! analyzer then *logs* which keys would be promoted to materialized
//! `label_<key>` columns under a schema-width budget — it changes nothing.
//!
//! This is the de-risking first half of auto-materialization (epic #737,
//! Layer 4a): validating the guardrails — especially the cardinality
//! estimator — against real data before any rewrite-coupled promotion.
//! Persisting stats to a catalog table and folding in query-demand
//! counters are follow-ups tracked on the issue.

use std::collections::BTreeMap;

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
///
/// Classified through [`crate::retention::SignalType::from_table_name`], the
/// crate's single table->signal predicate, rather than a second hand-rolled
/// match that could silently drift from it.
pub fn signal_of_table(table_name: &str) -> &'static str {
    crate::retention::SignalType::from_table_name(table_name)
        .map(|s| s.table_name())
        .unwrap_or("unknown")
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

/// The default number of values kept per key as a suggestion sketch. The
/// compactor's `value_sketch_size` overrides it.
pub const DEFAULT_VALUE_SKETCH_SIZE: usize = 100;

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
    /// The key's most frequent values with their counts, most frequent first,
    /// bounded by the configured sketch size — what query discovery suggests
    /// without reading data.
    ///
    /// Empty for a key that hit [`CARDINALITY_CAP`]: once value tracking
    /// stops, the counts held are whatever happened to arrive first, and
    /// suggesting those as "the top values" would be a confident wrong answer.
    /// Discovery reports such a key as uncovered instead.
    pub top_values: Vec<(String, u64)>,
}

/// Incremental accumulator for the attribute-statistics pass.
///
/// The rewrite streams its partition rather than collecting it, so the
/// stats pass has to fold over batches as they go by instead of taking a
/// materialized slice. State is per-key and bounded by
/// [`CARDINALITY_CAP`], so it does not grow with the partition's size.
#[derive(Debug)]
pub struct AttrStatsAccumulator {
    stats: BTreeMap<String, AttrFieldStats>,
    /// Per-key value counts, bounded by [`CARDINALITY_CAP`] distinct values.
    values: BTreeMap<String, BTreeMap<String, u64>>,
    total_rows: u64,
    sketch_size: usize,
}

impl Default for AttrStatsAccumulator {
    fn default() -> Self {
        Self {
            stats: BTreeMap::new(),
            values: BTreeMap::new(),
            total_rows: 0,
            sketch_size: DEFAULT_VALUE_SKETCH_SIZE,
        }
    }
}

impl AttrStatsAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    /// How many values per key to keep as a suggestion sketch. `0` keeps none.
    pub fn with_sketch_size(mut self, sketch_size: usize) -> Self {
        self.sketch_size = sketch_size;
        self
    }

    /// Fold one batch into the running statistics.
    pub fn push_batch(&mut self, batch: &RecordBatch) {
        self.total_rows += batch.num_rows() as u64;
        for column in ATTR_COLUMNS {
            let Some(array) = batch.column_by_name(column) else {
                continue;
            };
            for doc in attr_documents(array.as_ref()) {
                let Some(doc) = doc else { continue };
                for (key, value) in doc {
                    let entry = self.stats.entry(key.clone()).or_default();
                    entry.present_rows += 1;
                    let counts = self.values.entry(key).or_default();
                    if entry.capped {
                        continue;
                    }
                    // Count an existing value always; admit a new one only
                    // while under the cap, so state stays bounded by
                    // CARDINALITY_CAP per key regardless of partition size.
                    match counts.get_mut(&value) {
                        Some(count) => *count += 1,
                        None => {
                            counts.insert(value, 1);
                            if counts.len() >= CARDINALITY_CAP {
                                entry.capped = true;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Finalize into per-key statistics plus the total row count scanned.
    pub fn finish(mut self) -> (BTreeMap<String, AttrFieldStats>, u64) {
        for (key, counts) in self.values {
            let Some(entry) = self.stats.get_mut(&key) else {
                continue;
            };
            entry.distinct = counts.len();
            if entry.capped || self.sketch_size == 0 {
                continue;
            }
            let mut ranked: Vec<(String, u64)> = counts.into_iter().collect();
            // Frequency first, then name, so the sketch is deterministic for
            // the same data rather than dependent on iteration order.
            ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            ranked.truncate(self.sketch_size);
            entry.top_values = ranked;
        }
        (self.stats, self.total_rows)
    }
}

/// Analyze the attribute columns of the given batches, returning per-key
/// statistics plus the total row count scanned.
pub fn analyze_batches(batches: &[RecordBatch]) -> (BTreeMap<String, AttrFieldStats>, u64) {
    let mut acc = AttrStatsAccumulator::new();
    for batch in batches {
        acc.push_batch(batch);
    }
    acc.finish()
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
        // The sketch is replaced wholesale, including with nothing: a key
        // that grew past the cardinality cap since the last pass must stop
        // being suggested rather than keep serving a stale list.
        let values: Vec<(String, i64)> = s
            .top_values
            .iter()
            .map(|(value, count)| (value.clone(), *count as i64))
            .collect();
        if let Err(e) = catalog
            .replace_attribute_value_stats(tenant_id, dataset_id, signal, key, &values)
            .await
        {
            tracing::warn!(error = %e, attr_key = %key, "Failed to persist attribute value sketch");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Build a batch whose single `log_attributes` column holds one JSON
    /// document per row.
    fn attr_batch(docs: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "log_attributes",
            DataType::Utf8,
            true,
        )]));
        let column = datafusion::arrow::array::StringArray::from(
            docs.iter().map(|d| Some(*d)).collect::<Vec<_>>(),
        );
        RecordBatch::try_new(schema, vec![Arc::new(column)]).unwrap()
    }

    #[test]
    fn the_sketch_ranks_values_by_frequency_and_is_bounded() {
        let mut acc = AttrStatsAccumulator::new().with_sketch_size(2);
        acc.push_batch(&attr_batch(&[
            r#"{"http.route":"/orders"}"#,
            r#"{"http.route":"/orders"}"#,
            r#"{"http.route":"/orders"}"#,
            r#"{"http.route":"/users"}"#,
            r#"{"http.route":"/users"}"#,
            r#"{"http.route":"/health"}"#,
        ]));
        let (stats, rows) = acc.finish();
        assert_eq!(rows, 6);
        let route = &stats["http.route"];
        assert_eq!(route.distinct, 3);
        assert_eq!(
            route.top_values,
            vec![("/orders".to_string(), 3), ("/users".to_string(), 2)],
            "most frequent first, bounded by the sketch size"
        );
    }

    #[test]
    fn the_sketch_is_deterministic_for_equal_counts() {
        let mut acc = AttrStatsAccumulator::new();
        acc.push_batch(&attr_batch(&[
            r#"{"k":"b"}"#,
            r#"{"k":"a"}"#,
            r#"{"k":"c"}"#,
        ]));
        let (stats, _) = acc.finish();
        let values: Vec<&str> = stats["k"]
            .top_values
            .iter()
            .map(|(v, _)| v.as_str())
            .collect();
        assert_eq!(
            values,
            vec!["a", "b", "c"],
            "ties break by value, not by insertion order"
        );
    }

    #[test]
    fn a_disabled_sketch_still_counts_presence_and_cardinality() {
        let mut acc = AttrStatsAccumulator::new().with_sketch_size(0);
        acc.push_batch(&attr_batch(&[r#"{"k":"a"}"#, r#"{"k":"b"}"#]));
        let (stats, _) = acc.finish();
        assert_eq!(stats["k"].present_rows, 2);
        assert_eq!(stats["k"].distinct, 2);
        assert!(
            stats["k"].top_values.is_empty(),
            "a disabled sketch keeps nothing"
        );
    }

    #[test]
    fn a_key_past_the_cardinality_cap_keeps_no_sketch() {
        // Once value tracking stops, the counts held are whatever arrived
        // first; presenting those as "the top values" would be a confident
        // wrong answer, so the key must carry no sketch at all.
        let mut acc = AttrStatsAccumulator::new();
        let docs: Vec<String> = (0..CARDINALITY_CAP + 10)
            .map(|i| format!(r#"{{"id":"v{i}"}}"#))
            .collect();
        let refs: Vec<&str> = docs.iter().map(String::as_str).collect();
        acc.push_batch(&attr_batch(&refs));
        let (stats, _) = acc.finish();
        assert!(stats["id"].capped);
        assert!(
            stats["id"].top_values.is_empty(),
            "a runaway key is reported as uncovered, not partially suggested"
        );
    }

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
    }

    /// Folding batch-by-batch must produce exactly what analyzing them all
    /// at once does — otherwise streaming the rewrite would silently
    /// change which attributes get promoted.
    #[test]
    fn accumulating_batch_by_batch_matches_analyzing_them_together() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "log_attributes",
            DataType::Utf8,
            true,
        )]));
        let make = |rows: Vec<Option<&str>>| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from(
                    rows.into_iter()
                        .map(|r| r.map(|s| s.to_string()))
                        .collect::<Vec<_>>(),
                ))],
            )
            .unwrap()
        };

        let first = make(vec![
            Some(r#"{"namespace":"prod","pod":"a"}"#),
            Some(r#"{"namespace":"prod","pod":"b"}"#),
        ]);
        let second = make(vec![Some(r#"{"namespace":"staging"}"#), None]);

        let (batched, batched_rows) = analyze_batches(&[first.clone(), second.clone()]);

        let mut acc = AttrStatsAccumulator::new();
        acc.push_batch(&first);
        acc.push_batch(&second);
        let (streamed, streamed_rows) = acc.finish();

        assert_eq!(streamed_rows, batched_rows);
        assert_eq!(streamed.len(), batched.len());
        for (key, expected) in &batched {
            let actual = &streamed[key];
            assert_eq!(actual.present_rows, expected.present_rows, "key {key}");
            assert_eq!(actual.distinct, expected.distinct, "key {key}");
            assert_eq!(actual.capped, expected.capped, "key {key}");
        }
    }

    #[test]
    fn log_promotion_candidates_does_not_panic_on_real_or_empty_stats() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "log_attributes",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![Some(
                r#"{"namespace":"prod"}"#,
            )]))],
        )
        .unwrap();
        let (stats, total) = analyze_batches(&[batch]);

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
                top_values: vec![("prod".to_string(), 60), ("staging".to_string(), 20)],
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
