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
use chrono::{DateTime, Utc};
use common::attrs::AttrDocument;
use common::catalog::{AttributeLevelStatsRecord, AttributeStatsRecord};
use common::config::AttrPromotionConfig;
use common::iceberg::evolution;
use common::schema::logical::AttributeLevel;
use common::schema::type_authority::{AttributeKeyType, CanonicalType};
use common::schema::typed_attributes::{container_level, home_column, residue_column};
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, MapArray, RecordBatch, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// The outcome of a demotion pass over one table's already-materialized
/// `label_<key>` columns. Legacy label promotion no longer decides new
/// columns (layer 6 promotes typed `attr_<level>_<key>` columns instead);
/// this keeps only the demotion half so an already-existing label column
/// that has gone cold can still be dropped.
#[derive(Debug, Default, PartialEq)]
pub struct PromotionDecision {
    /// Currently materialized keys (not pinned) whose demand has dropped
    /// to zero — candidates for dropping at a later rewrite.
    pub demote: Vec<String>,
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

/// Compute the demotion decision for one table's already-materialized
/// `label_<key>` columns: an auto-promoted (not pinned) column with zero
/// recorded query demand is a candidate for dropping. `materialized` is the
/// table's current set of materialized label *attribute keys* (column names
/// minus the `label_` prefix); `pinned` is the configured allowlist for the
/// signal (never demoted).
pub fn decide(
    stats: &[AttributeStatsRecord],
    materialized: &[String],
    pinned: &[String],
) -> PromotionDecision {
    let mut decision = PromotionDecision::default();
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
    decision
}

/// A canonical-type lookup keyed by `(level, key)`, built from the type
/// authority's per-table rows, used by the typed promoted-attribute
/// backfill to detect a repinned key (its stored column type no longer
/// matches the type authority's current one for that `(level, key)`).
pub fn canonical_types_by_level(
    types: &[AttributeKeyType],
) -> HashMap<(AttributeLevel, String), CanonicalType> {
    types
        .iter()
        .map(|t| ((t.level, t.attr_key.clone()), t.canonical_type))
        .collect()
}

/// Log the demotion decision for one table (the advisory face of the pass;
/// the rewrite-coupled half acts on it when `dry_run` is off).
pub fn log_decision(table_name: &str, decision: &PromotionDecision, dry_run: bool) {
    if decision.demote.is_empty() {
        return;
    }
    tracing::info!(
        table = %table_name,
        dry_run,
        demote = ?decision.demote,
        "Attribute label demotion decision"
    );
}

/// Which `(level, key)` pairs get a typed `attr_<level>_<key>` column at the next rewrite.
#[derive(Debug, Default, PartialEq)]
pub struct TypedPromotionDecision {
    /// `(level, key)` pairs to promote, highest score first.
    pub promote: Vec<(AttributeLevel, String)>,
    /// `(level, key, streak)` over threshold but still building toward `promote_streak`.
    pub building: Vec<(AttributeLevel, String, i64)>,
}

/// A new promotion streak for one `(level, key)`, persisted by the caller.
pub type LevelStreak = (AttributeLevel, String, i64);

/// The [`AttributeLevel`]s with a typed-layout container, found by its `_residue` column.
fn levels_with_container<'a>(
    field_names: impl IntoIterator<Item = &'a str>,
) -> HashSet<AttributeLevel> {
    let names: HashSet<&str> = field_names.into_iter().collect();
    BACKFILL_SOURCE_COLUMNS
        .iter()
        .filter(|container| names.contains(residue_column(container).as_str()))
        .map(|container| container_level(container))
        .collect()
}

/// The [`AttributeLevel`]s this Iceberg schema has a typed-layout container for.
pub fn available_attribute_levels(
    schema: &iceberg_rust::spec::schema::Schema,
) -> HashSet<AttributeLevel> {
    levels_with_container(schema.fields().iter().map(|f| f.name.as_str()))
}

/// Parse a `last_queried_at` timestamp, accepting both storage dialects:
/// SQLite's RFC 3339 (`chrono::DateTime::to_rfc3339()`) and Postgres's
/// `CAST(timestamptz AS TEXT)` (e.g. `2026-09-28 01:02:03.456+00`, a space
/// separator and no minutes in the offset). `None` for anything else.
fn parse_last_queried_at(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .or_else(|_| DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z"))
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
}

/// The idle-demotion cutoff: a promoted column last queried before this
/// instant is stale. `None` when `demote_after_idle` is zero, meaning idle
/// demotion (and the promotion side's anti-flapping guard) is disabled.
fn idle_cutoff(
    now: DateTime<Utc>,
    demote_after_idle: std::time::Duration,
) -> Option<DateTime<Utc>> {
    if demote_after_idle.is_zero() {
        return None;
    }
    chrono::Duration::from_std(demote_after_idle)
        .ok()
        .map(|idle| now - idle)
}

/// The per-level typed promotion decision for one table: the legacy label guardrails
/// (cardinality cap, generated keys, presence/demand thresholds, hysteresis) keyed by
/// `(level, key)`, with `max_labels_per_table` shared with the `label_columns_used`
/// label columns. `capped_keys` comes from the flat, level-less `attribute_stats`.
///
/// `demoted` is this cycle's just-decided demotions (see
/// [`decide_typed_demotions`]): those pairs are excluded from promotion
/// candidates so a column isn't demoted and re-promoted in the same cycle,
/// while the promotion headroom still counts them as freed (not part of
/// `promoted` for budget purposes once the caller demotes them). A
/// candidate must also have been queried within `config.demote_after_idle`
/// of `now` (when non-zero) — otherwise it would qualify for idle demotion
/// on the very next cycle.
#[allow(clippy::too_many_arguments)]
pub fn decide_typed_promotions(
    level_stats: &[AttributeLevelStatsRecord],
    canonical_types: &HashMap<(AttributeLevel, String), CanonicalType>,
    promoted: &HashSet<(AttributeLevel, String)>,
    demoted: &HashSet<(AttributeLevel, String)>,
    available_levels: &HashSet<AttributeLevel>,
    capped_keys: &HashSet<String>,
    label_columns_used: usize,
    now: DateTime<Utc>,
    config: &AttrPromotionConfig,
) -> (TypedPromotionDecision, Vec<LevelStreak>) {
    let mut decision = TypedPromotionDecision::default();
    let mut new_streaks: Vec<LevelStreak> = Vec::new();
    let flapping_cutoff = idle_cutoff(now, config.demote_after_idle);

    let mut eligible: Vec<(&AttributeLevelStatsRecord, f64)> = Vec::new();
    for record in level_stats {
        let level_key = (record.level, record.attr_key.clone());
        if promoted.contains(&level_key) || demoted.contains(&level_key) {
            continue;
        }
        let recently_queried = flapping_cutoff.is_none_or(|cutoff| {
            record
                .last_queried_at
                .as_deref()
                .and_then(parse_last_queried_at)
                .is_some_and(|ts| ts >= cutoff)
        });
        let over_threshold = canonical_types.contains_key(&level_key)
            && available_levels.contains(&record.level)
            && !capped_keys.contains(&record.attr_key)
            && !looks_generated(&record.attr_key)
            && record.total_rows > 0
            && record.query_hits >= config.min_query_hits
            && (record.present_rows as f64 / record.total_rows as f64) >= config.min_presence
            && recently_queried;
        let streak = if over_threshold {
            record.promote_streak + 1
        } else {
            0
        };
        if streak != record.promote_streak {
            new_streaks.push((record.level, record.attr_key.clone(), streak));
        }
        if over_threshold && streak >= config.promote_streak {
            let presence = record.present_rows as f64 / record.total_rows as f64;
            eligible.push((record, record.query_hits as f64 * presence));
        } else if over_threshold {
            decision
                .building
                .push((record.level, record.attr_key.clone(), streak));
        }
    }

    let promoted_after_demotion = promoted.len().saturating_sub(demoted.len());
    let headroom = config
        .max_labels_per_table
        .saturating_sub(label_columns_used + promoted_after_demotion)
        .min(config.max_promotions_per_cycle);
    eligible.sort_by(|a, b| b.1.total_cmp(&a.1));
    decision.promote = eligible
        .iter()
        .take(headroom)
        .map(|(r, _)| (r.level, r.attr_key.clone()))
        .collect();

    (decision, new_streaks)
}

/// Which already-promoted `(level, key)` typed columns should be dropped at
/// the next rewrite (design D4: budgeted LRU demotion — a cold column folds
/// back into the typed map, which still holds every value, so nothing is
/// lost). Two independent reasons, applied in order:
///
/// - **Idle**: `last_queried_at` missing, unparsable, or older than
///   `now - config.demote_after_idle`. Disabled when `demote_after_idle` is
///   zero.
/// - **Over budget**: if, after the idle demotions, `label_columns_used +
///   promoted.len()` still exceeds `max_labels_per_table`, the
///   least-recently-queried remaining columns are demoted until back
///   within budget — missing timestamp sorts oldest, ties broken by fewer
///   `query_hits`, then by `(level, key)` for determinism.
pub fn decide_typed_demotions(
    promoted: &[(AttributeLevel, String)],
    level_stats: &[AttributeLevelStatsRecord],
    now: DateTime<Utc>,
    label_columns_used: usize,
    config: &AttrPromotionConfig,
) -> Vec<(AttributeLevel, String)> {
    let stats_by_key: HashMap<(AttributeLevel, String), &AttributeLevelStatsRecord> = level_stats
        .iter()
        .map(|r| ((r.level, r.attr_key.clone()), r))
        .collect();
    let last_queried = |pair: &(AttributeLevel, String)| -> Option<DateTime<Utc>> {
        stats_by_key
            .get(pair)
            .and_then(|r| r.last_queried_at.as_deref())
            .and_then(parse_last_queried_at)
    };

    let cutoff = idle_cutoff(now, config.demote_after_idle);
    let mut demote: Vec<(AttributeLevel, String)> = Vec::new();
    if let Some(cutoff) = cutoff {
        for pair in promoted {
            let stale = last_queried(pair).is_none_or(|ts| ts < cutoff);
            if stale {
                demote.push(pair.clone());
            }
        }
    }
    let demoted_set: HashSet<&(AttributeLevel, String)> = demote.iter().collect();

    let remaining_used = label_columns_used + promoted.len() - demote.len();
    if remaining_used > config.max_labels_per_table {
        let over = remaining_used - config.max_labels_per_table;
        type Candidate<'a> = (&'a (AttributeLevel, String), Option<DateTime<Utc>>, i64);
        let mut candidates: Vec<Candidate> = promoted
            .iter()
            .filter(|pair| !demoted_set.contains(pair))
            .map(|pair| {
                let record = stats_by_key.get(pair);
                let hits = record.map(|r| r.query_hits).unwrap_or(0);
                (pair, last_queried(pair), hits)
            })
            .collect();
        candidates.sort_by(|a, b| {
            let by_recency = match (a.1, b.1) {
                (None, None) => std::cmp::Ordering::Equal,
                (None, Some(_)) => std::cmp::Ordering::Less,
                (Some(_), None) => std::cmp::Ordering::Greater,
                (Some(x), Some(y)) => x.cmp(&y),
            };
            by_recency.then(a.2.cmp(&b.2)).then(a.0.cmp(b.0))
        });
        demote.extend(
            candidates
                .into_iter()
                .take(over)
                .map(|(pair, _, _)| pair.clone()),
        );
    }

    demote
}

/// Log the typed-attribute promotion decision for one table.
pub fn log_typed_decision(table_name: &str, decision: &TypedPromotionDecision, dry_run: bool) {
    if decision.promote.is_empty() && decision.building.is_empty() {
        return;
    }
    tracing::info!(
        table = %table_name,
        dry_run,
        promote = ?decision.promote,
        building = ?decision.building,
        "Typed attribute promotion decision"
    );
}

/// Log the typed-attribute demotion decision for one table (see
/// [`decide_typed_demotions`]).
pub fn log_typed_demotion(table_name: &str, demote: &[(AttributeLevel, String)], dry_run: bool) {
    if demote.is_empty() {
        return;
    }
    tracing::info!(
        table = %table_name,
        dry_run,
        demote = ?demote,
        "Typed attribute demotion decision"
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

/// One attribute source column's per-row documents, read from the key's
/// string home — coercing the int/double/bool homes would give the
/// promoted field a type other than its canonical, typed one.
fn label_source_documents(batch: &RecordBatch, container: &str) -> AttrDocuments {
    let source = home_column(container, CanonicalType::String);
    common::attrs::string_map_documents(batch, &source).unwrap_or_default()
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

/// The typed-layout container backing `level` in this batch: the fixed
/// names for resource/scope, and whichever one record-level container
/// (`span_attributes`, `log_attributes`, `attributes`, or
/// `profile_attributes`) the batch actually carries — a table has at most
/// one. `None` if the batch has no typed container for this level.
fn container_for_level(batch: &RecordBatch, level: AttributeLevel) -> Option<&'static str> {
    let schema = batch.schema();
    let names: HashSet<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    BACKFILL_SOURCE_COLUMNS.iter().copied().find(|container| {
        container_level(container) == level && names.contains(residue_column(container).as_str())
    })
}

/// A null array of the Arrow type backing `canonical`, `num_rows` long.
fn null_typed_array(canonical: CanonicalType, num_rows: usize) -> ArrayRef {
    match canonical {
        CanonicalType::String => Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
        CanonicalType::Int64 => Arc::new(Int64Array::from(vec![None::<i64>; num_rows])),
        CanonicalType::Float64 => Arc::new(Float64Array::from(vec![None::<f64>; num_rows])),
        CanonicalType::Bool => Arc::new(BooleanArray::from(vec![None::<bool>; num_rows])),
    }
}

/// The Arrow type backing a promoted column of canonical type `canonical`.
fn arrow_type_for_canonical(canonical: CanonicalType) -> DataType {
    match canonical {
        CanonicalType::String => DataType::Utf8,
        CanonicalType::Int64 => DataType::Int64,
        CanonicalType::Float64 => DataType::Float64,
        CanonicalType::Bool => DataType::Boolean,
    }
}

/// One row's value for `key` in a `Map<Utf8, V>` home, read as its native
/// type via `extract` rather than stringified — `None` when the row lacks
/// the key, the map is null, or the entries aren't shaped as expected.
fn find_value<V: Array + 'static, T>(
    map: &MapArray,
    key: &str,
    row: usize,
    extract: impl Fn(&V, usize) -> T,
) -> Option<T> {
    if map.is_null(row) {
        return None;
    }
    let entries = map.value(row);
    let keys = entries.column(0).as_any().downcast_ref::<StringArray>()?;
    let values = entries.column(1).as_any().downcast_ref::<V>()?;
    (0..entries.len())
        .find(|&j| !keys.is_null(j) && keys.value(j) == key && !values.is_null(j))
        .map(|j| extract(values, j))
}

/// `key`'s value from `home` (a `Map<Utf8, T>` typed-home column), one Arrow
/// array of `canonical`'s type. All-null when `home` is absent from the
/// batch or isn't a map — never coerced from a different type's home.
fn typed_home_array(
    batch: &RecordBatch,
    home: &str,
    key: &str,
    canonical: CanonicalType,
) -> ArrayRef {
    let num_rows = batch.num_rows();
    let Some(map) = batch
        .column_by_name(home)
        .and_then(|c| c.as_any().downcast_ref::<MapArray>())
    else {
        return null_typed_array(canonical, num_rows);
    };
    match canonical {
        CanonicalType::String => Arc::new(StringArray::from(
            (0..num_rows)
                .map(|i| find_value::<StringArray, _>(map, key, i, |v, j| v.value(j).to_string()))
                .collect::<Vec<_>>(),
        )),
        CanonicalType::Int64 => Arc::new(Int64Array::from(
            (0..num_rows)
                .map(|i| find_value::<Int64Array, _>(map, key, i, |v, j| v.value(j)))
                .collect::<Vec<_>>(),
        )),
        CanonicalType::Float64 => Arc::new(Float64Array::from(
            (0..num_rows)
                .map(|i| find_value::<Float64Array, _>(map, key, i, |v, j| v.value(j)))
                .collect::<Vec<_>>(),
        )),
        CanonicalType::Bool => Arc::new(BooleanArray::from(
            (0..num_rows)
                .map(|i| find_value::<BooleanArray, _>(map, key, i, |v, j| v.value(j)))
                .collect::<Vec<_>>(),
        )),
    }
}

/// Recompute the typed promoted `attr_<level>_<key>` columns of the given
/// batches from exactly their level's container typed home — never the
/// residue, and never another canonical type's home.
///
/// `attrs` is `(level, key, column, canonical type)` per
/// [`common::iceberg::evolution::promoted_attrs_of`]. Rows without the key
/// stay null; a level whose container isn't present in this batch also
/// stays null (defensive — the caller only passes attrs the current schema
/// actually carries). Callers filter out an entry whose stored canonical
/// type no longer matches the key's current type authority canonical type
/// (a repin) before calling this — such an entry is left null rather than
/// backfilled from a home that no longer matches it.
pub(crate) fn backfill_promoted_attr_columns(
    batches: Vec<RecordBatch>,
    attrs: &[(AttributeLevel, String, String, CanonicalType)],
) -> Result<Vec<RecordBatch>> {
    if attrs.is_empty() {
        return Ok(batches);
    }
    batches
        .into_iter()
        .map(|batch| backfill_promoted_attr_batch(batch, attrs))
        .collect()
}

fn backfill_promoted_attr_batch(
    batch: RecordBatch,
    attrs: &[(AttributeLevel, String, String, CanonicalType)],
) -> Result<RecordBatch> {
    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();

    for (level, key, column, canonical) in attrs {
        let arrow_type = arrow_type_for_canonical(*canonical);
        let values = match container_for_level(&batch, *level) {
            Some(container) => {
                typed_home_array(&batch, &home_column(container, *canonical), key, *canonical)
            }
            None => null_typed_array(*canonical, batch.num_rows()),
        };
        match fields.iter().position(|f| f.name() == column) {
            Some(idx) if fields[idx].data_type() == &arrow_type => {
                columns[idx] = values;
            }
            Some(_) => {
                tracing::warn!(
                    column = %column,
                    attr_key = %key,
                    "Promoted attribute column has an unexpected Arrow type; skipping backfill"
                );
            }
            None => {
                fields.push(Field::new(column, arrow_type, true));
                columns.push(values);
            }
        }
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
        .context("Failed to rebuild batch with backfilled promoted attribute columns")
}

#[cfg(test)]
mod tests {
    use super::*;
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
            analyzed_span: None,
            updated_at: "2026-08-17 09:00:00".into(),
        }
    }

    #[test]
    fn demotes_unqueried_auto_columns_but_never_pinned() {
        let stats = vec![record("auto_cold", 90, 100, 0, 0)];
        let materialized = vec!["auto_cold".to_string(), "pinned_cold".to_string()];
        let pinned = vec!["pinned_cold".to_string()];
        let decision = decide(&stats, &materialized, &pinned);
        assert_eq!(decision.demote, vec!["auto_cold".to_string()]);
    }

    /// Legacy label promotion no longer decides new columns: `decide` only
    /// has a `demote` field to populate, so it structurally cannot promote
    /// a key regardless of demand.
    #[test]
    fn decide_never_promotes_new_label_columns() {
        let hot = record("namespace", 90, 100, 50, 5);
        let decision = decide(&[hot], &[], &[]);
        assert_eq!(decision, PromotionDecision::default());
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
            // Existing promotion-only tests predate idle demotion and
            // don't set `last_queried_at`; disabled here so the new
            // anti-flapping guard doesn't reject them. Idle/demotion
            // behavior gets its own tests with a non-zero value.
            demote_after_idle: std::time::Duration::ZERO,
        }
    }

    fn level_record(
        level: AttributeLevel,
        key: &str,
        present: i64,
        total: i64,
        hits: i64,
        streak: i64,
    ) -> AttributeLevelStatsRecord {
        AttributeLevelStatsRecord {
            tenant_id: "t".into(),
            dataset_id: "d".into(),
            signal: "logs".into(),
            level,
            attr_key: key.into(),
            present_rows: present,
            total_rows: total,
            query_hits: hits,
            last_queried_at: None,
            promote_streak: streak,
            updated_at: "2026-08-17 09:00:00".into(),
        }
    }

    fn types(
        entries: &[(AttributeLevel, &str, CanonicalType)],
    ) -> HashMap<(AttributeLevel, String), CanonicalType> {
        entries
            .iter()
            .map(|(level, key, canonical)| ((*level, key.to_string()), *canonical))
            .collect()
    }

    fn all_levels() -> HashSet<AttributeLevel> {
        [
            AttributeLevel::Resource,
            AttributeLevel::Scope,
            AttributeLevel::Record,
        ]
        .into_iter()
        .collect()
    }

    #[test]
    fn typed_promotes_only_after_the_streak_builds() {
        let cfg = config();
        let ready = level_record(AttributeLevel::Record, "namespace", 90, 100, 50, 2);
        let fresh = level_record(AttributeLevel::Record, "pod", 90, 100, 50, 0);
        let types = types(&[
            (AttributeLevel::Record, "namespace", CanonicalType::String),
            (AttributeLevel::Record, "pod", CanonicalType::String),
        ]);
        let (decision, streaks) = decide_typed_promotions(
            &[ready, fresh],
            &types,
            &HashSet::new(),
            &HashSet::new(),
            &all_levels(),
            &HashSet::new(),
            0,
            Utc::now(),
            &cfg,
        );
        assert_eq!(
            decision.promote,
            vec![(AttributeLevel::Record, "namespace".to_string())]
        );
        assert_eq!(
            decision.building,
            vec![(AttributeLevel::Record, "pod".to_string(), 1)]
        );
        assert!(streaks.contains(&(AttributeLevel::Record, "namespace".to_string(), 3)));
        assert!(streaks.contains(&(AttributeLevel::Record, "pod".to_string(), 1)));
    }

    #[test]
    fn typed_streak_resets_when_demand_disappears() {
        let cfg = config();
        let cooled = level_record(AttributeLevel::Record, "namespace", 90, 100, 0, 2);
        let types = types(&[(AttributeLevel::Record, "namespace", CanonicalType::String)]);
        let (decision, streaks) = decide_typed_promotions(
            &[cooled],
            &types,
            &HashSet::new(),
            &HashSet::new(),
            &all_levels(),
            &HashSet::new(),
            0,
            Utc::now(),
            &cfg,
        );
        assert!(decision.promote.is_empty());
        assert_eq!(
            streaks,
            vec![(AttributeLevel::Record, "namespace".to_string(), 0)]
        );
    }

    /// Each eligibility rule alone rejects an otherwise-eligible key.
    #[test]
    fn typed_rejects_each_guardrail_independently() {
        let cfg = config();
        let no_type = level_record(AttributeLevel::Record, "no_type", 90, 100, 50, 5);
        let no_container = level_record(AttributeLevel::Scope, "no_container", 90, 100, 50, 5);
        let capped = level_record(AttributeLevel::Record, "request_id", 90, 100, 50, 5);
        let generated = level_record(
            AttributeLevel::Record,
            "span.0123456789abcdef",
            90,
            100,
            50,
            5,
        );
        let sparse = level_record(AttributeLevel::Record, "rare", 1, 10_000, 50, 5);
        let types = types(&[
            (AttributeLevel::Scope, "no_container", CanonicalType::String),
            (AttributeLevel::Record, "request_id", CanonicalType::String),
            (
                AttributeLevel::Record,
                "span.0123456789abcdef",
                CanonicalType::String,
            ),
            (AttributeLevel::Record, "rare", CanonicalType::String),
        ]);
        let available: HashSet<AttributeLevel> = [AttributeLevel::Resource, AttributeLevel::Record]
            .into_iter()
            .collect();
        let capped_keys: HashSet<String> = ["request_id".to_string()].into_iter().collect();
        let (decision, _) = decide_typed_promotions(
            &[no_type, no_container, capped, generated, sparse],
            &types,
            &HashSet::new(),
            &HashSet::new(),
            &available,
            &capped_keys,
            0,
            Utc::now(),
            &cfg,
        );
        assert!(decision.promote.is_empty());
        assert!(decision.building.is_empty());
    }

    #[test]
    fn typed_rejects_low_query_hits() {
        let cfg = config();
        let quiet = level_record(AttributeLevel::Record, "quiet", 90, 100, 0, 5);
        let types = types(&[(AttributeLevel::Record, "quiet", CanonicalType::String)]);
        let (decision, _) = decide_typed_promotions(
            &[quiet],
            &types,
            &HashSet::new(),
            &HashSet::new(),
            &all_levels(),
            &HashSet::new(),
            0,
            Utc::now(),
            &cfg,
        );
        assert!(decision.promote.is_empty());
    }

    #[test]
    fn typed_budget_is_shared_with_labels_and_ranks_by_score() {
        let mut cfg = config();
        cfg.max_labels_per_table = 3;
        // 2 labels + 1 promoted attr exhaust a budget of 3.
        let a = level_record(AttributeLevel::Record, "a", 100, 100, 10, 5); // score 10
        let b = level_record(AttributeLevel::Record, "b", 50, 100, 30, 5); // score 15
        let types = types(&[
            (AttributeLevel::Record, "a", CanonicalType::String),
            (AttributeLevel::Record, "b", CanonicalType::String),
        ]);
        let promoted: HashSet<(AttributeLevel, String)> =
            [(AttributeLevel::Resource, "already".to_string())]
                .into_iter()
                .collect();
        let (decision, _) = decide_typed_promotions(
            &[a, b],
            &types,
            &promoted,
            &HashSet::new(),
            &all_levels(),
            &HashSet::new(),
            2,
            Utc::now(),
            &cfg,
        );
        assert!(decision.promote.is_empty());

        cfg.max_labels_per_table = 4;
        let a = level_record(AttributeLevel::Record, "a", 100, 100, 10, 5);
        let b = level_record(AttributeLevel::Record, "b", 50, 100, 30, 5);
        let (decision, _) = decide_typed_promotions(
            &[a, b],
            &types,
            &promoted,
            &HashSet::new(),
            &all_levels(),
            &HashSet::new(),
            2,
            Utc::now(),
            &cfg,
        );
        assert_eq!(
            decision.promote,
            vec![(AttributeLevel::Record, "b".to_string())]
        );
    }

    /// The same key at two levels is decided independently.
    #[test]
    fn typed_decides_the_same_key_independently_per_level() {
        let cfg = config();
        let record_row = level_record(AttributeLevel::Record, "env", 90, 100, 50, 2);
        let resource_row = level_record(AttributeLevel::Resource, "env", 90, 100, 50, 2);
        let types = types(&[(AttributeLevel::Record, "env", CanonicalType::String)]);
        let (decision, _) = decide_typed_promotions(
            &[record_row, resource_row],
            &types,
            &HashSet::new(),
            &HashSet::new(),
            &all_levels(),
            &HashSet::new(),
            0,
            Utc::now(),
            &cfg,
        );
        assert_eq!(
            decision.promote,
            vec![(AttributeLevel::Record, "env".to_string())]
        );
    }

    #[test]
    fn generated_key_detection() {
        assert!(looks_generated("trace.4bf92f3577b34da6a3ce929d0e0e4736"));
        assert!(looks_generated("session_12345678"));
        assert!(!looks_generated("http.method"));
        assert!(!looks_generated("k8s.pod.name"));
    }

    /// Two typed-layout source containers over three rows: resource attrs
    /// win over record attrs for the shared `env` key (row 0), only the
    /// record-level source carries the key (row 1), and the key is absent
    /// everywhere (row 2, both containers present but empty).
    fn typed_source_batch() -> RecordBatch {
        let json_map = |pairs: &[(&str, &str)]| {
            Some(serde_json::Map::from_iter(pairs.iter().map(|(k, v)| {
                (k.to_string(), serde_json::Value::String(v.to_string()))
            })))
        };
        let resource_rows = [json_map(&[("env", "prod")]), None, json_map(&[])];
        let log_rows = [
            json_map(&[("env", "record-level"), ("pod", "api-1")]),
            json_map(&[("env", "staging")]),
            json_map(&[]),
        ];
        let (resource_fields, resource_arrays) = common::testing::typed_attribute_columns_from(
            "logs",
            "physical-v4",
            "resource_attributes",
            &resource_rows,
        );
        let (log_fields, log_arrays) = common::testing::typed_attribute_columns_from(
            "logs",
            "physical-v4",
            "log_attributes",
            &log_rows,
        );

        let mut fields = vec![Field::new("body", DataType::Utf8, true)];
        fields.extend(resource_fields);
        fields.extend(log_fields);
        fields.push(Field::new("label_env", DataType::Utf8, true));

        let mut columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("c"),
        ]))];
        columns.extend(resource_arrays);
        columns.extend(log_arrays);
        // Existing column with writer-left nulls to be healed.
        columns.push(Arc::new(StringArray::from(vec![None::<&str>, None, None])));

        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
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
        let out = backfill_label_columns(vec![typed_source_batch()], &pairs).unwrap();
        assert_eq!(out.len(), 1);
        let env = string_column(&out[0], "label_env");
        // Row 0: resource wins over the record-level value.
        assert_eq!(env.value(0), "prod");
        // Row 1: only the record-level source carries the key.
        assert_eq!(env.value(1), "staging");
        // Row 2: key absent everywhere -> null.
        assert!(env.is_null(2));
        // Column count unchanged: the existing column was replaced.
        assert_eq!(out[0].num_columns(), typed_source_batch().num_columns());
    }

    #[test]
    fn backfill_appends_missing_columns_in_pair_order() {
        let pairs = vec![
            ("pod".to_string(), "label_pod".to_string()),
            ("env".to_string(), "label_env".to_string()),
        ];
        let source = typed_source_batch();
        let source_columns = source.num_columns();
        let out = backfill_label_columns(vec![source], &pairs).unwrap();
        let batch = &out[0];
        // `label_pod` is new and appended before the (replaced) `label_env`.
        assert_eq!(batch.num_columns(), source_columns + 1);
        assert_eq!(batch.schema().field(source_columns).name(), "label_pod");
        let pod = string_column(batch, "label_pod");
        assert_eq!(pod.value(0), "api-1");
        assert!(pod.is_null(1));
        assert!(pod.is_null(2));
    }

    #[test]
    fn backfill_without_pairs_is_a_no_op() {
        let batch = typed_source_batch();
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

    /// A typed promoted-attribute column is recomputed from exactly its
    /// key's canonical home: `retries` (int) reads its native `Int64`
    /// value rather than a stringified one, and a row missing the key
    /// stays null.
    #[test]
    fn backfill_promoted_attr_columns_reads_the_canonical_home() {
        let row = serde_json::Map::from_iter([
            ("env".to_string(), serde_json::json!("prod")),
            ("retries".to_string(), serde_json::json!(3)),
        ]);
        let empty_row = serde_json::Map::new();
        let (fields, arrays) = common::testing::typed_attribute_columns(
            "span_attributes",
            &[Some(row), Some(empty_row)],
        );
        let schema = Arc::new(Schema::new(fields.to_vec()));
        let batch = RecordBatch::try_new(schema, arrays.to_vec()).unwrap();

        let attrs = vec![(
            AttributeLevel::Record,
            "retries".to_string(),
            "attr_record_retries".to_string(),
            CanonicalType::Int64,
        )];
        let out = backfill_promoted_attr_columns(vec![batch], &attrs).unwrap();
        let retries = out[0]
            .column_by_name("attr_record_retries")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(retries.value(0), 3);
        assert!(retries.is_null(1), "a row without the key stays null");
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

    fn now() -> DateTime<Utc> {
        chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 9, 28, 12, 0, 0).unwrap()
    }

    fn idle_config(max_labels: usize) -> AttrPromotionConfig {
        AttrPromotionConfig {
            demote_after_idle: std::time::Duration::from_secs(7 * 24 * 3600),
            max_labels_per_table: max_labels,
            ..config()
        }
    }

    fn level_record_with_demand(
        level: AttributeLevel,
        key: &str,
        hits: i64,
        last_queried_at: Option<&str>,
    ) -> AttributeLevelStatsRecord {
        AttributeLevelStatsRecord {
            last_queried_at: last_queried_at.map(str::to_string),
            ..level_record(level, key, 90, 100, hits, 5)
        }
    }

    #[test]
    fn idle_demotes_a_column_with_no_recorded_demand() {
        let promoted = vec![(AttributeLevel::Record, "cold".to_string())];
        let stats = vec![level_record_with_demand(
            AttributeLevel::Record,
            "cold",
            0,
            None,
        )];
        let demote = decide_typed_demotions(&promoted, &stats, now(), 0, &idle_config(10));
        assert_eq!(demote, promoted);
    }

    #[test]
    fn idle_demotes_a_column_stale_in_either_storage_format() {
        let promoted = vec![
            (AttributeLevel::Record, "rfc3339".to_string()),
            (AttributeLevel::Record, "postgres".to_string()),
        ];
        let stats = vec![
            level_record_with_demand(
                AttributeLevel::Record,
                "rfc3339",
                5,
                Some("2026-01-01T00:00:00Z"),
            ),
            level_record_with_demand(
                AttributeLevel::Record,
                "postgres",
                5,
                Some("2026-01-01 00:00:00.000+00"),
            ),
        ];
        let demote = decide_typed_demotions(&promoted, &stats, now(), 0, &idle_config(10));
        assert_eq!(demote.len(), 2);
        assert!(demote.contains(&(AttributeLevel::Record, "rfc3339".to_string())));
        assert!(demote.contains(&(AttributeLevel::Record, "postgres".to_string())));
    }

    #[test]
    fn idle_keeps_a_column_queried_recently_in_either_storage_format() {
        let promoted = vec![
            (AttributeLevel::Record, "rfc3339".to_string()),
            (AttributeLevel::Record, "postgres".to_string()),
        ];
        let stats = vec![
            level_record_with_demand(
                AttributeLevel::Record,
                "rfc3339",
                5,
                Some("2026-09-28T00:00:00Z"),
            ),
            level_record_with_demand(
                AttributeLevel::Record,
                "postgres",
                5,
                Some("2026-09-28 00:00:00.000+00"),
            ),
        ];
        let demote = decide_typed_demotions(&promoted, &stats, now(), 0, &idle_config(10));
        assert!(demote.is_empty());
    }

    #[test]
    fn zero_demote_after_idle_disables_idle_demotion() {
        let promoted = vec![(AttributeLevel::Record, "cold".to_string())];
        let stats = vec![level_record_with_demand(
            AttributeLevel::Record,
            "cold",
            0,
            None,
        )];
        let demote = decide_typed_demotions(&promoted, &stats, now(), 0, &config());
        assert!(demote.is_empty());
    }

    #[test]
    fn over_budget_demotes_least_recently_queried_first_with_tie_breaks() {
        // All four are recently queried (no idle demotion applies); the
        // budget of 2 forces demoting the two least-recently-queried ones.
        let promoted = vec![
            (AttributeLevel::Record, "oldest".to_string()),
            (AttributeLevel::Record, "missing_ts".to_string()),
            (AttributeLevel::Record, "tie_low_hits".to_string()),
            (AttributeLevel::Record, "tie_high_hits".to_string()),
        ];
        let stats = vec![
            level_record_with_demand(
                AttributeLevel::Record,
                "oldest",
                5,
                Some("2026-09-25T00:00:00Z"),
            ),
            level_record_with_demand(AttributeLevel::Record, "missing_ts", 5, None),
            level_record_with_demand(
                AttributeLevel::Record,
                "tie_low_hits",
                1,
                Some("2026-09-27T00:00:00Z"),
            ),
            level_record_with_demand(
                AttributeLevel::Record,
                "tie_high_hits",
                9,
                Some("2026-09-27T00:00:00Z"),
            ),
        ];
        let demote = decide_typed_demotions(&promoted, &stats, now(), 0, &idle_config(2));
        // Missing timestamp sorts oldest; then the older explicit
        // timestamp; the two 2026-09-27 entries would tie on recency, so
        // "tie_low_hits" (fewer query_hits) demotes before
        // "tie_high_hits" survives within budget.
        assert_eq!(
            demote,
            vec![
                (AttributeLevel::Record, "missing_ts".to_string()),
                (AttributeLevel::Record, "oldest".to_string()),
            ]
        );
    }

    #[test]
    fn idle_demotions_reduce_budget_pressure_before_lru_kicks_in() {
        // "cold" is idle-demoted outright; that alone brings the count to
        // budget, so no additional LRU demotion is needed even though
        // "warm" would otherwise be the least-recently-queried survivor.
        let promoted = vec![
            (AttributeLevel::Record, "cold".to_string()),
            (AttributeLevel::Record, "warm".to_string()),
        ];
        let stats = vec![
            level_record_with_demand(AttributeLevel::Record, "cold", 0, None),
            level_record_with_demand(
                AttributeLevel::Record,
                "warm",
                5,
                Some("2026-09-28T00:00:00Z"),
            ),
        ];
        let demote = decide_typed_demotions(&promoted, &stats, now(), 0, &idle_config(1));
        assert_eq!(demote, vec![(AttributeLevel::Record, "cold".to_string())]);
    }

    #[test]
    fn a_column_demoted_this_cycle_is_not_re_promoted_in_the_same_cycle() {
        let cfg = idle_config(10);
        let demoted: HashSet<(AttributeLevel, String)> =
            [(AttributeLevel::Record, "flappy".to_string())]
                .into_iter()
                .collect();
        // Otherwise fully eligible: typed, available, uncapped, not
        // generated, high presence/hits, streak already built, and
        // recently queried (so the anti-flapping guard alone wouldn't
        // reject it).
        let record = level_record_with_demand(
            AttributeLevel::Record,
            "flappy",
            50,
            Some("2026-09-28T00:00:00Z"),
        );
        let types = types(&[(AttributeLevel::Record, "flappy", CanonicalType::String)]);
        let (decision, _) = decide_typed_promotions(
            &[record],
            &types,
            &HashSet::new(),
            &demoted,
            &all_levels(),
            &HashSet::new(),
            0,
            now(),
            &cfg,
        );
        assert!(decision.promote.is_empty());
    }

    #[test]
    fn a_promotion_candidate_not_recently_queried_is_rejected_to_avoid_flapping() {
        // Otherwise-eligible, but its last query demand is older than
        // `demote_after_idle` -- promoting it now would only have it
        // demoted again next cycle.
        let cfg = idle_config(10);
        let record = level_record_with_demand(
            AttributeLevel::Record,
            "stale_demand",
            50,
            Some("2026-01-01T00:00:00Z"),
        );
        let types = types(&[(
            AttributeLevel::Record,
            "stale_demand",
            CanonicalType::String,
        )]);
        let (decision, _) = decide_typed_promotions(
            &[record],
            &types,
            &HashSet::new(),
            &HashSet::new(),
            &all_levels(),
            &HashSet::new(),
            0,
            now(),
            &cfg,
        );
        assert!(decision.promote.is_empty());
    }
}
