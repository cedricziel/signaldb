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

use datafusion::arrow::array::RecordBatch;

use common::schema::logical::AttributeLevel;
use common::schema::type_authority::CanonicalType;
use common::schema::typed_attributes::{container_level, home_column};

/// The four typed-home canonical types every attribute container carries.
const HOME_CANONICAL_TYPES: [CanonicalType; 4] = [
    CanonicalType::String,
    CanonicalType::Int64,
    CanonicalType::Float64,
    CanonicalType::Bool,
];

/// Rows carrying each key at a given [`AttributeLevel`], counted from the
/// typed homes only (never the residue — see [`AttrStatsAccumulator`]).
pub type AttrLevelPresence = BTreeMap<(AttributeLevel, String), u64>;

/// Attribute columns recognized across the signal tables.
const ATTR_COLUMNS: &[&str] = &[
    "log_attributes",
    "span_attributes",
    "resource_attributes",
    "scope_attributes",
    "attributes",
    "profile_attributes",
];

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
    /// Rows carrying each key, split by the [`AttributeLevel`] its container
    /// implies (change: otel-native-schema layer 6, D4/D5) — only keys in a
    /// typed home count; the residue (off-type/array/kvlist/bytes) is never
    /// promotable, so it is excluded here even though the flat `stats` above
    /// counts it.
    level_presence: AttrLevelPresence,
    total_rows: u64,
    sketch_size: usize,
}

impl Default for AttrStatsAccumulator {
    fn default() -> Self {
        Self {
            stats: BTreeMap::new(),
            values: BTreeMap::new(),
            level_presence: BTreeMap::new(),
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
            let Ok(docs) = common::attrs::attr_documents(batch, column) else {
                continue;
            };
            for doc in docs {
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
            self.push_level_presence(batch, column);
        }
    }

    /// Fold one container's typed homes into `level_presence`, skipping the
    /// residue — a key that only appears off-type is never a promotion
    /// candidate, so it must not count toward per-level presence.
    ///
    /// Reads keys only (`map_key_documents`), not values: the `_int`,
    /// `_double`, and `_bool` homes don't carry `Utf8` values, so
    /// `string_map_documents` would reject them as non-string.
    fn push_level_presence(&mut self, batch: &RecordBatch, container: &str) {
        let level = container_level(container);
        for canonical in HOME_CANONICAL_TYPES {
            let home = home_column(container, canonical);
            let Ok(docs) = common::attrs::map_key_documents(batch, &home) else {
                continue;
            };
            for keys in docs.into_iter().flatten() {
                for key in keys {
                    *self.level_presence.entry((level, key)).or_insert(0) += 1;
                }
            }
        }
    }

    /// Finalize into per-key statistics, the per-(level, key) presence
    /// counts, and the total row count scanned.
    pub fn finish(mut self) -> (BTreeMap<String, AttrFieldStats>, u64, AttrLevelPresence) {
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
        (self.stats, self.total_rows, self.level_presence)
    }
}

/// Analyze the attribute columns of the given batches, returning per-key
/// statistics, per-(level, key) presence, and the total row count scanned.
pub fn analyze_batches(
    batches: &[RecordBatch],
) -> (BTreeMap<String, AttrFieldStats>, u64, AttrLevelPresence) {
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

/// Persist the analyzer's per-key statistics into the service catalog's
/// `attribute_stats` table (epic #737, #733), keyed by
/// (tenant, dataset, signal, key), and the per-(level, key) presence into
/// `attribute_level_stats` (change: otel-native-schema layer 6, D4/D5).
/// Failures are logged and swallowed — the stats are advisory and must never
/// fail a compaction.
#[allow(clippy::too_many_arguments)]
pub async fn persist_stats(
    catalog: &common::catalog::Catalog,
    tenant_id: &str,
    dataset_id: &str,
    table_name: &str,
    stats: &BTreeMap<String, AttrFieldStats>,
    level_presence: &AttrLevelPresence,
    total_rows: u64,
    analyzed_span: common::catalog::AnalyzedSpan,
) {
    let signal = common::catalog::attribute_stats_signal(table_name);
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
                // The span lands with the sketch below, in one transaction;
                // until then the key reads as not covering any window.
                None,
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
            .replace_attribute_value_sketch(
                tenant_id,
                dataset_id,
                signal,
                key,
                &values,
                Some(analyzed_span),
            )
            .await
        {
            tracing::warn!(error = %e, attr_key = %key, "Failed to persist attribute value sketch");
        }
    }
    for ((level, key), present_rows) in level_presence {
        if let Err(e) = catalog
            .upsert_attribute_level_scan_stats(
                tenant_id,
                dataset_id,
                signal,
                *level,
                key,
                *present_rows as i64,
                total_rows as i64,
            )
            .await
        {
            tracing::warn!(error = %e, attr_key = %key, level = level.as_str(), "Failed to persist per-level attribute scan stats");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use std::sync::Arc;

    /// Build a typed-layout `log_attributes` batch, one row per JSON
    /// document string (all object-shaped in these tests).
    fn attr_batch(docs: &[&str]) -> RecordBatch {
        let rows: Vec<Option<serde_json::Map<String, serde_json::Value>>> = docs
            .iter()
            .map(
                |d| match serde_json::from_str(d).expect("valid JSON fixture") {
                    serde_json::Value::Object(map) => Some(map),
                    other => panic!("expected a JSON object fixture, got {other}"),
                },
            )
            .collect();
        let (fields, arrays) = common::testing::typed_attribute_columns_from(
            "logs",
            "physical-v4",
            "log_attributes",
            &rows,
        );
        RecordBatch::try_new(Arc::new(Schema::new(fields.to_vec())), arrays.to_vec()).unwrap()
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
        let (stats, rows, _levels) = acc.finish();
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
        let (stats, _, _levels) = acc.finish();
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
        let (stats, _, _levels) = acc.finish();
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
        let (stats, _, _levels) = acc.finish();
        assert!(stats["id"].capped);
        assert!(
            stats["id"].top_values.is_empty(),
            "a runaway key is reported as uncovered, not partially suggested"
        );
    }

    #[test]
    fn analyzer_computes_presence_and_cardinality_and_ranks_candidates() {
        let batch = attr_batch(&[
            r#"{"namespace":"prod","pod":"a"}"#,
            r#"{"namespace":"prod","pod":"b"}"#,
            r#"{"namespace":"staging"}"#,
        ]);

        let (stats, total, _levels) = analyze_batches(&[batch]);
        assert_eq!(total, 3);
        let ns = &stats["namespace"];
        assert_eq!(ns.present_rows, 3);
        assert_eq!(ns.distinct, 2);
        assert!(!ns.capped);
        assert_eq!(stats["pod"].present_rows, 2);
        assert_eq!(stats["pod"].distinct, 2);
    }

    /// A typed-layout container (four typed maps + CBOR residue) feeds
    /// per-key stats across a string key, an int key, and a residue (array)
    /// key.
    #[test]
    fn stats_see_every_key_over_a_typed_batch() {
        let row = serde_json::Map::from_iter([
            ("namespace".to_string(), serde_json::json!("prod")),
            ("retries".to_string(), serde_json::json!(3)),
            ("tags".to_string(), serde_json::json!(["a", "b"])),
        ]);
        let (fields, arrays) =
            common::testing::typed_attribute_columns("span_attributes", &[Some(row)]);
        let schema = Arc::new(Schema::new(fields.to_vec()));
        let batch = RecordBatch::try_new(schema, arrays.to_vec()).unwrap();

        let (stats, total, _levels) = analyze_batches(&[batch]);
        assert_eq!(total, 1);
        assert_eq!(
            stats.keys().collect::<Vec<_>>(),
            vec!["namespace", "retries", "tags"]
        );
        for key in ["namespace", "retries", "tags"] {
            assert_eq!(stats[key].present_rows, 1, "key {key}");
            assert_eq!(stats[key].distinct, 1, "key {key}");
        }
    }

    /// Folding batch-by-batch must produce exactly what analyzing them all
    /// at once does — otherwise streaming the rewrite would silently
    /// change which attributes get promoted.
    #[test]
    fn accumulating_batch_by_batch_matches_analyzing_them_together() {
        let json_row = |s: &str| match serde_json::from_str(s).expect("valid JSON fixture") {
            serde_json::Value::Object(map) => Some(map),
            other => panic!("expected a JSON object fixture, got {other}"),
        };
        let make = |rows: Vec<Option<serde_json::Map<String, serde_json::Value>>>| {
            let (fields, arrays) = common::testing::typed_attribute_columns_from(
                "logs",
                "physical-v4",
                "log_attributes",
                &rows,
            );
            RecordBatch::try_new(Arc::new(Schema::new(fields.to_vec())), arrays.to_vec()).unwrap()
        };

        let first = make(vec![
            json_row(r#"{"namespace":"prod","pod":"a"}"#),
            json_row(r#"{"namespace":"prod","pod":"b"}"#),
        ]);
        let second = make(vec![json_row(r#"{"namespace":"staging"}"#), None]);

        let (batched, batched_rows, _batched_levels) =
            analyze_batches(&[first.clone(), second.clone()]);

        let mut acc = AttrStatsAccumulator::new();
        acc.push_batch(&first);
        acc.push_batch(&second);
        let (streamed, streamed_rows, _streamed_levels) = acc.finish();

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
        let batch = attr_batch(&[r#"{"namespace":"prod"}"#]);
        let (stats, total, _levels) = analyze_batches(&[batch]);

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
        let mut level_presence = BTreeMap::new();
        level_presence.insert((AttributeLevel::Resource, "namespace".to_string()), 80);
        super::persist_stats(
            &catalog,
            "t",
            "d",
            "metrics_gauge",
            &stats,
            &level_presence,
            100,
            common::catalog::AnalyzedSpan {
                start_ns: 0,
                end_ns: 3_600_000_000_000,
            },
        )
        .await;

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
        assert_eq!(
            rows[0].analyzed_span,
            Some(common::catalog::AnalyzedSpan {
                start_ns: 0,
                end_ns: 3_600_000_000_000,
            })
        );

        let level_rows = catalog
            .list_attribute_level_stats("t", "d", "metrics")
            .await
            .unwrap();
        assert_eq!(level_rows.len(), 1);
        assert_eq!(level_rows[0].level, AttributeLevel::Resource);
        assert_eq!(level_rows[0].attr_key, "namespace");
        assert_eq!(level_rows[0].present_rows, 80);
        assert_eq!(level_rows[0].total_rows, 100);
    }

    /// A key present at both resource and record level (e.g. the same name
    /// used in `resource_attributes` and `span_attributes`) counts as two
    /// separate level entries, each with its own presence.
    #[test]
    fn level_presence_counts_separately_per_level_for_the_same_key() {
        let resource_row =
            serde_json::Map::from_iter([("environment".to_string(), serde_json::json!("prod"))]);
        let (resource_fields, resource_arrays) = common::testing::typed_attribute_columns(
            "resource_attributes",
            &[Some(resource_row.clone()), Some(resource_row)],
        );
        let resource_batch = RecordBatch::try_new(
            Arc::new(Schema::new(resource_fields.to_vec())),
            resource_arrays.to_vec(),
        )
        .unwrap();

        let span_row =
            serde_json::Map::from_iter([("environment".to_string(), serde_json::json!("prod"))]);
        let (span_fields, span_arrays) =
            common::testing::typed_attribute_columns("span_attributes", &[Some(span_row)]);
        let span_batch = RecordBatch::try_new(
            Arc::new(Schema::new(span_fields.to_vec())),
            span_arrays.to_vec(),
        )
        .unwrap();

        let mut acc = AttrStatsAccumulator::new();
        acc.push_batch(&resource_batch);
        acc.push_batch(&span_batch);
        let (_stats, _total, levels) = acc.finish();

        assert_eq!(
            levels[&(AttributeLevel::Resource, "environment".to_string())],
            2
        );
        assert_eq!(
            levels[&(AttributeLevel::Record, "environment".to_string())],
            1
        );
    }

    /// A key that only appears off-type (residue, e.g. an array value) is
    /// never a promotion candidate, so it must not appear in per-level
    /// presence even though the flat per-key stats still count it.
    #[test]
    fn residue_only_keys_are_excluded_from_level_presence() {
        let row = serde_json::Map::from_iter([("tags".to_string(), serde_json::json!(["a", "b"]))]);
        let (fields, arrays) =
            common::testing::typed_attribute_columns("span_attributes", &[Some(row)]);
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(fields.to_vec())), arrays.to_vec()).unwrap();

        let (stats, _total, levels) = analyze_batches(&[batch]);
        assert_eq!(
            stats["tags"].present_rows, 1,
            "the flat stats still count it"
        );
        assert!(
            !levels.contains_key(&(AttributeLevel::Record, "tags".to_string())),
            "residue keys are never promotable, so they carry no level presence"
        );
    }

    /// A non-string-valued typed home (int, double, bool) must still yield
    /// level presence for its keys, and must not warn — a regression test
    /// for `push_level_presence` once calling `string_map_documents`
    /// (Utf8-valued only) on these homes, which silently dropped every
    /// non-string key and spammed a warning per batch per home.
    #[test]
    fn level_presence_counts_non_string_valued_homes_without_warning() {
        let (warnings, _guard) =
            common::testing::WarnCapture::install("map attribute column has non-string keys");

        let row = serde_json::Map::from_iter([
            ("retries".to_string(), serde_json::json!(3)),
            ("ratio".to_string(), serde_json::json!(1.5)),
            ("ok".to_string(), serde_json::json!(true)),
        ]);
        let (fields, arrays) =
            common::testing::typed_attribute_columns("span_attributes", &[Some(row)]);
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(fields.to_vec())), arrays.to_vec()).unwrap();

        let (_stats, _total, levels) = analyze_batches(&[batch]);
        assert_eq!(levels[&(AttributeLevel::Record, "retries".to_string())], 1);
        assert_eq!(levels[&(AttributeLevel::Record, "ratio".to_string())], 1);
        assert_eq!(levels[&(AttributeLevel::Record, "ok".to_string())], 1);
        assert!(
            warnings.messages().is_empty(),
            "non-string-valued homes must not be reported as malformed: {:?}",
            warnings.messages()
        );
    }

    #[test]
    fn attribute_stats_signal_maps_all_tables() {
        assert_eq!(common::catalog::attribute_stats_signal("traces"), "traces");
        assert_eq!(common::catalog::attribute_stats_signal("logs"), "logs");
        assert_eq!(
            common::catalog::attribute_stats_signal("metrics_histogram"),
            "metrics"
        );
        assert_eq!(
            common::catalog::attribute_stats_signal("profiles"),
            "profiles"
        );
        // otel-native-schema layer 7 (D10) cutover prep.
        assert_eq!(
            common::catalog::attribute_stats_signal("metrics"),
            "metrics"
        );
        assert_eq!(
            common::catalog::attribute_stats_signal("metric_exemplars"),
            "metrics"
        );
    }
}
