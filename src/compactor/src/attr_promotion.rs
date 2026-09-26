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
use common::attrs::AttrDocument;
use common::catalog::AttributeStatsRecord;
use common::config::AttrPromotionConfig;
use common::iceberg::evolution;
use common::schema::type_authority::{AttributeKeyType, CanonicalType};
use common::schema::typed_attributes::{home_column, is_typed_layout};
use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::collections::{HashMap, HashSet};
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
/// the configured allowlist for the signal (never demoted). `typed_string_keys`
/// is `Some` on a typed-layout table: only keys in the set (their canonical
/// type authority home is `String` at every level it's recorded) are
/// eligible for promotion, since any other key's typed home isn't a string
/// and a `label_<key>` column can't safely stringify it (see
/// [`string_only_keys`]). `None` means legacy layout, where every key is
/// eligible as before. Returns the decision and the new streak value per key
/// so the caller can persist the hysteresis state.
pub fn decide(
    stats: &[AttributeStatsRecord],
    materialized: &[String],
    pinned: &[String],
    config: &AttrPromotionConfig,
    typed_string_keys: Option<&HashSet<String>>,
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
            && (record.present_rows as f64 / record.total_rows as f64) >= config.min_presence
            && typed_string_keys.is_none_or(|allowed| allowed.contains(&record.attr_key));
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

/// Whether `schema` is the typed attribute layout (four typed maps plus a
/// CBOR residue column per container) rather than the legacy single map/JSON
/// column.
pub fn schema_is_typed(schema: &iceberg_rust::spec::schema::Schema) -> bool {
    is_typed_layout(schema.fields().iter().map(|f| f.name.as_str()))
}

/// The keys eligible for promotion on a typed-layout table: those whose
/// canonical type is recorded as [`CanonicalType::String`] at every
/// attribute level the type authority has seen them at for this table. A key
/// recorded at more than one level (e.g. both resource- and record-scoped)
/// with disagreeing types is excluded rather than guessed at — the
/// per-attribute-stats-key candidacy check has no level of its own to match
/// against (`attribute_stats` folds every container's keys into one flat
/// per-key map; see [`crate::attr_stats::push_batch`]).
pub fn string_only_keys(types: &[AttributeKeyType]) -> HashSet<String> {
    let mut by_key: HashMap<&str, HashSet<CanonicalType>> = HashMap::new();
    for row in types {
        by_key
            .entry(row.attr_key.as_str())
            .or_default()
            .insert(row.canonical_type);
    }
    by_key
        .into_iter()
        .filter(|(_, canonicals)| {
            canonicals.len() == 1 && canonicals.contains(&CanonicalType::String)
        })
        .map(|(key, _)| key.to_string())
        .collect()
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

/// The materialized attribute keys of a table, recovered from its label
/// columns' recorded origin-key `doc` metadata (#814) rather than by
/// re-encoding each stats key and checking the candidate name against the
/// table's `label_` columns -- that would misreport a key as already
/// materialized whenever it sanitizes to the same column name as a
/// different key that actually owns that column.
///
/// Origin keys are collected into a set once, up front, rather than
/// re-scanning every schema field (base columns included) per stats key.
pub fn materialized_keys_of(
    schema: &iceberg_rust::spec::schema::Schema,
    stats: &[AttributeStatsRecord],
) -> Vec<String> {
    let origin_keys: std::collections::HashSet<&str> = schema
        .fields()
        .iter()
        .filter_map(|f| evolution::origin_key_of(f.doc.as_deref()))
        .collect();
    stats
        .iter()
        .map(|r| r.attr_key.clone())
        .filter(|key| origin_keys.contains(key.as_str()))
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
/// by column — see [`common::schema::materialized_column_name`]). Existing
/// Utf8 columns are overwritten in place (healing rows the writer left
/// null, e.g. data ingested after an auto-promotion); missing columns are
/// appended in `pairs` order, matching the evolved schema which appends
/// promoted columns at the end. Values follow the writer's source precedence
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
type AttrDocuments = Vec<Option<AttrDocument>>;

/// One attribute source column's per-row documents. On the typed layout,
/// only the key's string home backs a label column — coercing the
/// int/double/bool homes would give the promoted field a type other than
/// its canonical, typed one.
fn label_source_documents(batch: &RecordBatch, container: &str) -> AttrDocuments {
    let source = if batch.column_by_name(container).is_some() {
        container.to_string()
    } else {
        home_column(container, CanonicalType::String)
    };
    common::attrs::attr_documents(batch, &source).unwrap_or_default()
}

fn backfill_batch(batch: RecordBatch, pairs: &[(String, String)]) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();

    // Parse each attribute source column present in the batch once, in
    // precedence order.
    let sources: Vec<AttrDocuments> = BACKFILL_SOURCE_COLUMNS
        .iter()
        .map(|column| label_source_documents(&batch, column))
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
                sources
                    .iter()
                    .find_map(|docs| docs[row].as_ref().and_then(|doc| doc.get(key)).cloned())
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
    use common::schema::logical::AttributeLevel;
    use datafusion::arrow::array::Array;
    use iceberg_rust::spec::schema::Schema as IcebergSchema;
    use iceberg_rust::spec::types::{PrimitiveType, StructField, StructType, Type};

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
            updated_at: "2026-08-17 09:00:00".into(),
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
        let (decision, streaks) = decide(&[ready, fresh], &[], &[], &cfg, None);
        assert_eq!(decision.promote, vec!["namespace".to_string()]);
        assert_eq!(decision.building, vec![("pod".to_string(), 1)]);
        assert!(streaks.contains(&("namespace".to_string(), 3)));
        assert!(streaks.contains(&("pod".to_string(), 1)));
    }

    #[test]
    fn streak_resets_when_demand_disappears() {
        let cfg = config();
        let cooled = record("namespace", 90, 100, 0, 2);
        let (decision, streaks) = decide(&[cooled], &[], &[], &cfg, None);
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
        let (decision, _) = decide(&[capped, generated, sparse], &[], &[], &cfg, None);
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
            None,
        );
        assert_eq!(decision.promote, vec!["b".to_string()]);
    }

    #[test]
    fn demotes_unqueried_auto_columns_but_never_pinned() {
        let cfg = config();
        let stats = vec![record("auto_cold", 90, 100, 0, 0)];
        let materialized = vec!["auto_cold".to_string(), "pinned_cold".to_string()];
        let pinned = vec!["pinned_cold".to_string()];
        let (decision, _) = decide(&stats, &materialized, &pinned, &cfg, None);
        assert_eq!(decision.demote, vec!["auto_cold".to_string()]);
    }

    /// On a typed table, a key whose type-authority home is `String` is
    /// still promotable; a key whose home is `Int64` is not, even though
    /// both clear every other guardrail -- its typed home isn't a string,
    /// so a `label_<key>` column can't safely carry it.
    #[test]
    fn typed_table_promotes_string_keys_and_rejects_non_string_keys() {
        let cfg = config();
        let env = record("env", 90, 100, 50, 2);
        let retries = record("retries", 90, 100, 50, 2);
        let allowed = string_only_keys(&[
            AttributeKeyType {
                attr_key: "env".to_string(),
                level: AttributeLevel::Record,
                canonical_type: CanonicalType::String,
            },
            AttributeKeyType {
                attr_key: "retries".to_string(),
                level: AttributeLevel::Record,
                canonical_type: CanonicalType::Int64,
            },
        ]);
        let (decision, _) = decide(&[env, retries], &[], &[], &cfg, Some(&allowed));
        assert_eq!(decision.promote, vec!["env".to_string()]);
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

    /// On a typed-layout batch, backfill fills a label from the key's string
    /// home but leaves it null for a key whose canonical home is int64 --
    /// stringifying that value would give the label a type other than its
    /// typed home's.
    #[test]
    fn backfill_over_a_typed_batch_fills_string_keys_and_skips_int_keys() {
        let row = serde_json::Map::from_iter([
            ("env".to_string(), serde_json::json!("prod")),
            ("retries".to_string(), serde_json::json!(3)),
        ]);
        let (fields, arrays) =
            common::testing::typed_attribute_columns("span_attributes", &[Some(row)]);
        let schema = Arc::new(Schema::new(fields.to_vec()));
        let batch = RecordBatch::try_new(schema, arrays.to_vec()).unwrap();

        let pairs = vec![
            ("env".to_string(), "label_env".to_string()),
            ("retries".to_string(), "label_retries".to_string()),
        ];
        let out = backfill_label_columns(vec![batch], &pairs).unwrap();
        let env = string_column(&out[0], "label_env");
        assert_eq!(env.value(0), "prod");
        let retries = string_column(&out[0], "label_retries");
        assert!(
            retries.is_null(0),
            "an int-home key must not be stringified into a label column"
        );
    }

    /// A single-field schema with one materialized label column,
    /// carrying `origin_key`'s [`evolution::label_doc`].
    fn schema_with_label(origin_key: &str, column: &str) -> IcebergSchema {
        let field = StructField {
            id: 1,
            name: column.to_string(),
            required: false,
            field_type: Type::Primitive(PrimitiveType::String),
            doc: Some(evolution::label_doc(origin_key)),
            initial_default: None,
            write_default: None,
        };
        IcebergSchema::from_struct_type(StructType::new(vec![field]), 0, None)
    }

    #[test]
    fn schema_is_typed_detects_a_residue_column() {
        assert!(!schema_is_typed(&schema_with_label(
            "http.method",
            "label_http_method"
        )));
        assert!(schema_is_typed(&schema_with_label(
            "http.method",
            "span_attributes_residue"
        )));
    }

    #[test]
    fn materialized_keys_match_via_origin_key_doc() {
        let stats = vec![
            record("http.method", 1, 1, 1, 0),
            record("other", 1, 1, 1, 0),
        ];
        let schema = schema_with_label("http.method", "label_http_method");
        assert_eq!(
            materialized_keys_of(&schema, &stats),
            vec!["http.method".to_string()]
        );
    }

    #[test]
    fn colliding_key_is_not_misreported_as_already_materialized() {
        // `label_http_method` materializes `http.method`; `http_method`
        // sanitizes to the same column name but has no column of its own
        // (#814) -- it must not be reported as already materialized.
        let stats = vec![record("http_method", 1, 1, 1, 0)];
        let schema = schema_with_label("http.method", "label_http_method");
        assert!(materialized_keys_of(&schema, &stats).is_empty());
    }
}
