//! # Attribute Auto-Promotion Decisions
//!
//! Layer 4b of the attribute-explorability plan (epic #737, #734): decide —
//! from the persisted `attribute_stats` (scan-side presence/cardinality
//! joined with query demand) — which attribute keys should be promoted to
//! materialized `label_<key>` columns at the next rewrite, and which
//! existing auto-promoted columns should be demoted.
//!
//! The decision is pure; acting on it (rewriting files with the new
//! columns and committing the schema change) is the rewrite-coupled half.
//! Guardrails (Honeycomb/ClickHouse-derived):
//!
//! - **Dimensionality budget**, not value-cardinality limits: at most
//!   `max_labels_per_table` materialized columns, pinned config entries
//!   included. Keys whose distinct tracking hit the analyzer cap are
//!   rejected outright.
//! - **Hysteresis**: a key must score above threshold for
//!   `promote_streak` consecutive cycles before promotion; a bounded
//!   number of promotions per cycle.
//! - **Generated-key rejection**: keys that embed UUIDs, long hex/numeric
//!   runs, or timestamps would create runaway schemas and are never
//!   promoted.
//! - **Pinned labels** (from `[schema.materialized_labels]`) are never
//!   demoted.

use anyhow::{Context, Result};
use common::catalog::AttributeStatsRecord;
use common::config::AttrPromotionConfig;
use common::schema::materialized_column_name;
use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// The outcome of a promotion pass over one table's statistics.
#[derive(Debug, Default, PartialEq)]
pub struct PromotionDecision {
    /// Attribute keys to extract into `label_<key>` columns at the next
    /// rewrite, highest score first.
    pub promote: Vec<String>,
    /// Currently materialized keys (not pinned) whose demand has dropped
    /// to zero — candidates for dropping at a later rewrite.
    pub demote: Vec<String>,
    /// Keys currently over threshold but still building their streak
    /// (`streak` cycles observed so far, promotion at
    /// `config.promote_streak`).
    pub building: Vec<(String, i64)>,
}

/// A key that looks machine-generated: promotion would grow the schema
/// without reusable query value. Rejects UUID segments, runs of 12+ hex
/// chars, and runs of 8+ digits anywhere in the key.
pub fn looks_generated(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut hex_run = 0usize;
    let mut digit_run = 0usize;
    for &b in bytes {
        if b.is_ascii_digit() {
            digit_run += 1;
            hex_run += 1;
        } else if b.is_ascii_hexdigit() {
            hex_run += 1;
            digit_run = 0;
        } else {
            hex_run = 0;
            digit_run = 0;
        }
        if digit_run >= 8 || hex_run >= 12 {
            return true;
        }
    }
    false
}

/// Compute the promotion/demotion decision for one table.
///
/// `materialized` is the table's current set of materialized label
/// *attribute keys* (column names minus the `label_` prefix); `pinned` is
/// the configured allowlist for the signal (never demoted). Returns the
/// decision and the new streak value per key so the caller can persist the
/// hysteresis state.
pub fn decide(
    stats: &[AttributeStatsRecord],
    materialized: &[String],
    pinned: &[String],
    config: &AttrPromotionConfig,
) -> (PromotionDecision, Vec<(String, i64)>) {
    let mut decision = PromotionDecision::default();
    let mut new_streaks: Vec<(String, i64)> = Vec::new();

    // Score every not-yet-materialized key against the guardrails.
    let mut eligible: Vec<(&AttributeStatsRecord, f64)> = Vec::new();
    for record in stats {
        if materialized.contains(&record.attr_key) || pinned.contains(&record.attr_key) {
            continue;
        }
        let over_threshold = !record.capped
            && !looks_generated(&record.attr_key)
            && record.total_rows > 0
            && record.query_hits >= config.min_query_hits
            && (record.present_rows as f64 / record.total_rows as f64) >= config.min_presence;
        let streak = if over_threshold {
            record.promote_streak + 1
        } else {
            0
        };
        if streak != record.promote_streak {
            new_streaks.push((record.attr_key.clone(), streak));
        }
        if over_threshold && streak >= config.promote_streak {
            let presence = record.present_rows as f64 / record.total_rows as f64;
            eligible.push((record, record.query_hits as f64 * presence));
        } else if over_threshold {
            decision.building.push((record.attr_key.clone(), streak));
        }
    }

    // Budget: pinned labels plus what is already materialized count
    // against the width; promote highest score first into what remains.
    let width: usize = materialized
        .iter()
        .chain(pinned.iter().filter(|p| !materialized.contains(p)))
        .count();
    let headroom = config
        .max_labels_per_table
        .saturating_sub(width)
        .min(config.max_promotions_per_cycle);
    eligible.sort_by(|a, b| b.1.total_cmp(&a.1));
    decision.promote = eligible
        .iter()
        .take(headroom)
        .map(|(r, _)| r.attr_key.clone())
        .collect();

    // Demotion: an auto-promoted (not pinned) column with zero recorded
    // demand is a candidate for dropping.
    for key in materialized {
        if pinned.contains(key) {
            continue;
        }
        let unqueried = stats
            .iter()
            .find(|r| &r.attr_key == key)
            .is_none_or(|r| r.query_hits == 0);
        if unqueried {
            decision.demote.push(key.clone());
        }
    }

    (decision, new_streaks)
}

/// Log the decision for one table (the advisory face of the pass; the
/// rewrite-coupled half acts on it when `dry_run` is off).
pub fn log_decision(table_name: &str, decision: &PromotionDecision, dry_run: bool) {
    if decision.promote.is_empty() && decision.demote.is_empty() && decision.building.is_empty() {
        return;
    }
    tracing::info!(
        table = %table_name,
        dry_run,
        promote = ?decision.promote,
        demote = ?decision.demote,
        building = ?decision.building,
        "Attribute promotion decision"
    );
}

/// The materialized attribute keys of a table, recovered from its
/// `label_<key>` column names. The column name is a lossy encoding
/// (non-alphanumerics become `_`), so keys are matched by re-encoding the
/// stats keys rather than decoding column names.
pub fn materialized_keys_of(
    label_columns: &[String],
    stats: &[AttributeStatsRecord],
) -> Vec<String> {
    stats
        .iter()
        .map(|r| r.attr_key.clone())
        .filter(|key| label_columns.contains(&materialized_column_name(key)))
        .collect()
}

/// Attribute source columns for the backfill, in the writer's value
/// precedence order: resource, then scope, then the record-level column
/// (only one of the record-level names exists per table).
const BACKFILL_SOURCE_COLUMNS: &[&str] = &[
    "resource_attributes",
    "scope_attributes",
    "span_attributes",
    "log_attributes",
    "attributes",
    "profile_attributes",
];

/// Recompute the materialized `label_<column>` value columns of the given
/// batches from their attribute source columns.
///
/// `pairs` maps attribute keys to their target column names (deduplicated
/// by column — see [`materialized_column_name`]). Existing Utf8 columns
/// are overwritten in place (healing rows the writer left null, e.g. data
/// ingested after an auto-promotion); missing columns are appended in
/// `pairs` order, matching the evolved schema which appends promoted
/// columns at the end. Values follow the writer's source precedence
/// (resource → scope → record attributes) and rows without the key stay
/// null — old rows are backfilled by construction since every row is
/// rewritten.
pub(crate) fn backfill_label_columns(
    batches: Vec<RecordBatch>,
    pairs: &[(String, String)],
) -> Result<Vec<RecordBatch>> {
    if pairs.is_empty() {
        return Ok(batches);
    }
    batches
        .into_iter()
        .map(|batch| backfill_batch(batch, pairs))
        .collect()
}

/// One attribute source column parsed into per-row key/value documents.
type AttrDocuments = Vec<Option<Vec<(String, String)>>>;

fn backfill_batch(batch: RecordBatch, pairs: &[(String, String)]) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();

    // Parse each attribute source column present in the batch once, in
    // precedence order.
    let sources: Vec<AttrDocuments> = BACKFILL_SOURCE_COLUMNS
        .iter()
        .filter_map(|column| batch.column_by_name(column))
        .map(|array| crate::attr_stats::attr_documents(array.as_ref()))
        .filter(|docs| docs.len() == num_rows)
        .collect();

    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();

    for (key, column) in pairs {
        let values: StringArray = (0..num_rows)
            .map(|row| {
                sources.iter().find_map(|docs| {
                    docs[row]
                        .as_ref()
                        .and_then(|doc| doc.iter().find(|(k, _)| k == key))
                        .map(|(_, v)| v.clone())
                })
            })
            .collect();
        match fields.iter().position(|f| f.name() == column) {
            Some(idx) if fields[idx].data_type() == &DataType::Utf8 => {
                if !fields[idx].is_nullable() {
                    fields[idx] = fields[idx].clone().with_nullable(true);
                }
                columns[idx] = Arc::new(values);
            }
            Some(_) => {
                tracing::warn!(
                    column = %column,
                    attr_key = %key,
                    "Materialized label column has a non-string type; skipping backfill"
                );
            }
            None => {
                fields.push(Field::new(column, DataType::Utf8, true));
                columns.push(Arc::new(values));
            }
        }
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
        .context("Failed to rebuild batch with backfilled label columns")
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Array;

    fn record(key: &str, present: i64, total: i64, hits: i64, streak: i64) -> AttributeStatsRecord {
        AttributeStatsRecord {
            tenant_id: "t".into(),
            dataset_id: "d".into(),
            signal: "logs".into(),
            attr_key: key.into(),
            present_rows: present,
            total_rows: total,
            distinct_estimate: 10,
            capped: false,
            query_hits: hits,
            promote_streak: streak,
        }
    }

    fn config() -> AttrPromotionConfig {
        AttrPromotionConfig {
            enabled: true,
            dry_run: true,
            max_labels_per_table: 4,
            min_presence: 0.01,
            min_query_hits: 1,
            promote_streak: 3,
            max_promotions_per_cycle: 4,
        }
    }

    #[test]
    fn promotes_only_after_the_streak_builds() {
        let cfg = config();
        // Two prior over-threshold cycles: this one completes the streak.
        let ready = record("namespace", 90, 100, 50, 2);
        // First over-threshold cycle: streak starts building.
        let fresh = record("pod", 90, 100, 50, 0);
        let (decision, streaks) = decide(&[ready, fresh], &[], &[], &cfg);
        assert_eq!(decision.promote, vec!["namespace".to_string()]);
        assert_eq!(decision.building, vec![("pod".to_string(), 1)]);
        assert!(streaks.contains(&("namespace".to_string(), 3)));
        assert!(streaks.contains(&("pod".to_string(), 1)));
    }

    #[test]
    fn streak_resets_when_demand_disappears() {
        let cfg = config();
        let cooled = record("namespace", 90, 100, 0, 2);
        let (decision, streaks) = decide(&[cooled], &[], &[], &cfg);
        assert!(decision.promote.is_empty());
        assert_eq!(streaks, vec![("namespace".to_string(), 0)]);
    }

    #[test]
    fn rejects_capped_cardinality_generated_keys_and_low_presence() {
        let cfg = config();
        let mut capped = record("request_id", 90, 100, 50, 5);
        capped.capped = true;
        let generated = record("span.0123456789abcdef", 90, 100, 50, 5);
        let sparse = record("rare", 1, 10_000, 50, 5);
        let (decision, _) = decide(&[capped, generated, sparse], &[], &[], &cfg);
        assert!(decision.promote.is_empty());
        assert!(decision.building.is_empty());
    }

    #[test]
    fn budget_counts_pinned_and_materialized_and_ranks_by_score() {
        let mut cfg = config();
        cfg.max_labels_per_table = 3;
        // Width 2 (one pinned + one auto-materialized) leaves headroom 1.
        let a = record("a", 100, 100, 10, 5); // score 10
        let b = record("b", 50, 100, 30, 5); // score 15 — wins
        let (decision, _) = decide(
            &[a, b],
            &["auto".to_string()],
            &["pinned".to_string()],
            &cfg,
        );
        assert_eq!(decision.promote, vec!["b".to_string()]);
    }

    #[test]
    fn demotes_unqueried_auto_columns_but_never_pinned() {
        let cfg = config();
        let stats = vec![record("auto_cold", 90, 100, 0, 0)];
        let materialized = vec!["auto_cold".to_string(), "pinned_cold".to_string()];
        let pinned = vec!["pinned_cold".to_string()];
        let (decision, _) = decide(&stats, &materialized, &pinned, &cfg);
        assert_eq!(decision.demote, vec!["auto_cold".to_string()]);
    }

    #[test]
    fn generated_key_detection() {
        assert!(looks_generated("trace.4bf92f3577b34da6a3ce929d0e0e4736"));
        assert!(looks_generated("session_12345678"));
        assert!(!looks_generated("http.method"));
        assert!(!looks_generated("k8s.pod.name"));
    }

    fn json_utf8_batch() -> RecordBatch {
        // Two source columns: resource attrs win over record attrs.
        let schema = Arc::new(Schema::new(vec![
            Field::new("body", DataType::Utf8, true),
            Field::new("resource_attributes", DataType::Utf8, true),
            Field::new("log_attributes", DataType::Utf8, true),
            Field::new("label_env", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
                Arc::new(StringArray::from(vec![
                    Some(r#"{"env":"prod"}"#),
                    None,
                    Some("{}"),
                ])),
                Arc::new(StringArray::from(vec![
                    Some(r#"{"env":"record-level","pod":"api-1"}"#),
                    Some(r#"{"env":"staging"}"#),
                    Some("{}"),
                ])),
                // Existing column with writer-left nulls to be healed.
                Arc::new(StringArray::from(vec![None::<&str>, None, None])),
            ],
        )
        .unwrap()
    }

    fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
    }

    #[test]
    fn backfill_overwrites_existing_column_with_source_precedence() {
        let pairs = vec![("env".to_string(), "label_env".to_string())];
        let out = backfill_label_columns(vec![json_utf8_batch()], &pairs).unwrap();
        assert_eq!(out.len(), 1);
        let env = string_column(&out[0], "label_env");
        // Row 0: resource wins over the record-level value.
        assert_eq!(env.value(0), "prod");
        // Row 1: only the record-level source carries the key.
        assert_eq!(env.value(1), "staging");
        // Row 2: key absent everywhere -> null.
        assert!(env.is_null(2));
        // Column count unchanged: the existing column was replaced.
        assert_eq!(out[0].num_columns(), 4);
    }

    #[test]
    fn backfill_appends_missing_columns_in_pair_order() {
        let pairs = vec![
            ("pod".to_string(), "label_pod".to_string()),
            ("env".to_string(), "label_env".to_string()),
        ];
        let out = backfill_label_columns(vec![json_utf8_batch()], &pairs).unwrap();
        let batch = &out[0];
        // `label_pod` is new and appended before the (replaced) `label_env`.
        assert_eq!(batch.num_columns(), 5);
        assert_eq!(batch.schema().field(4).name(), "label_pod");
        let pod = string_column(batch, "label_pod");
        assert_eq!(pod.value(0), "api-1");
        assert!(pod.is_null(1));
        assert!(pod.is_null(2));
    }

    #[test]
    fn backfill_without_pairs_is_a_no_op() {
        let batch = json_utf8_batch();
        let out = backfill_label_columns(vec![batch.clone()], &[]).unwrap();
        assert_eq!(out[0], batch);
    }

    #[test]
    fn materialized_keys_match_via_column_encoding() {
        let stats = vec![
            record("http.method", 1, 1, 1, 0),
            record("other", 1, 1, 1, 0),
        ];
        let cols = vec!["label_http_method".to_string()];
        assert_eq!(
            materialized_keys_of(&cols, &stats),
            vec!["http.method".to_string()]
        );
    }
}
