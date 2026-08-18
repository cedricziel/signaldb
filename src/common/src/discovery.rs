//! # Query discovery — what a tenant can query, without scanning it
//!
//! Assembles the answer to "what can I filter on" from three metadata tiers,
//! none of which reads signal data:
//!
//! 1. **declared** — [`LogicalSchema`], the canonical client-visible field
//!    catalog: membership, canonical type, attribute level, filterability;
//! 2. **registry** — the tenant's schema registries, which give an observed key
//!    a type, a description and its declared value set;
//! 3. **observed** — the compactor's `attribute_stats`, which is the only thing
//!    that knows which attribute keys a tenant actually emits (until the
//!    attribute registry, issue #813, exists).
//!
//! Membership is `declared ∪ observed`. A semantic-convention registry knows
//! thousands of keys a tenant has never sent; listing them would bury the
//! tenant's real fields, so the registry enriches and never contributes
//! membership.
//!
//! This module is deliberately I/O-free: callers fetch the three inputs and
//! pass them in, so the merge rules are unit-testable without a catalog. See
//! `openspec/changes/query-field-discovery`.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::catalog::{AttributeStatsRecord, AttributeValueStat};
use crate::model::span::{SpanKind, SpanStatus};
use crate::schema::logical::{AttributeLevel, Filterability, LogicalSchema, LogicalType};
use crate::schema_registry::AttributeHit;

/// Which metadata tier a discovered item came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FieldOrigin {
    /// The canonical logical schema declares it.
    Declared,
    /// A schema registry defines it and statistics observed it.
    Registry,
    /// Statistics observed it; nothing defines it.
    Observed,
}

/// Where a suggested value came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ValueOrigin {
    /// A declared value set — a registry enumeration, or one of the
    /// enumerations SignalDB itself writes (span kind, status code).
    Registry,
    /// Maintained value statistics.
    Statistics,
    /// A bounded, explicitly requested read of signal data.
    Sampled,
}

/// Which tier answered a discovery request, and therefore what it cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum CostMode {
    /// Answered from declared schema, registries and statistics. No signal
    /// data was read.
    Metadata,
    /// Answered by an explicitly requested, bounded read of signal data.
    SampledScan,
    /// Not answered: no metadata covers the request and no read was requested.
    None,
}

/// An approximate distinct-value count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct CardinalityEstimate {
    /// The estimated number of distinct values.
    pub estimate: i64,
    /// When true the collector hit its cap: the true count is at least
    /// `estimate`, not equal to it.
    pub at_least: bool,
}

/// One queryable field.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct DiscoveredField {
    /// The logical, dotted OTel-native name — directly usable in a predicate.
    pub name: String,
    /// The canonical value type a literal is coerced to.
    #[serde(rename = "type")]
    pub value_type: LogicalType,
    /// The OTel attribute level, when known. Statistics carry no level, so an
    /// observed key reports `null` rather than a guess.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub level: Option<AttributeLevel>,
    /// Whether a predicate may address it (retrieval-only fields are listed,
    /// not hidden).
    pub filterable: bool,
    /// The tier this item came from.
    pub origin: FieldOrigin,
    /// The fraction of the tenant's records carrying it, when statistics
    /// exist. Absent means unknown — never defaulted to a number that could be
    /// mistaken for a measurement.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coverage: Option<f64>,
    /// An approximate distinct-value count, when statistics exist.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cardinality: Option<CardinalityEstimate>,
    /// The registry's one-line description, when a registry defines it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub brief: Option<String>,
    /// Whether a registry marks it deprecated.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub deprecated: bool,
}

/// One suggested value.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct DiscoveredValue {
    pub value: String,
    /// How often it was observed, when the tier that produced it counts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<i64>,
    pub origin: ValueOrigin,
}

/// One signal source available to the tenant.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct DiscoveredSource {
    /// The name an IR document's `from` names.
    pub name: String,
    /// Whether the tenant can query it. A registered signal with no data is
    /// available and empty, never omitted.
    pub available: bool,
}

/// What a discovery answer cost and how far it can be trusted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct DiscoveryCost {
    pub mode: CostMode,
    /// Whether the answer is scoped to the requested time window. Maintained
    /// statistics carry no time dimension, so a metadata answer says `false`
    /// rather than implying the range narrowed it.
    pub window_scoped: bool,
    /// Whether the answer is sampled, and therefore possibly incomplete.
    pub sampled: bool,
    /// Whether the answer is approximate — a bounded sketch of the most
    /// frequent values rather than the exact set. A declared value set is
    /// exact; a statistics- or scan-derived one is not, and saying so is the
    /// difference between a suggestion and a claim.
    pub approximate: bool,
    /// How recent the statistics behind the answer are (as the catalog stores
    /// it). `null` means no statistics exist yet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub as_of: Option<String>,
}

impl DiscoveryCost {
    /// A free, exact answer from declared schema and registries.
    pub fn metadata(as_of: Option<String>) -> Self {
        DiscoveryCost {
            mode: CostMode::Metadata,
            window_scoped: false,
            sampled: false,
            approximate: false,
            as_of,
        }
    }

    /// A free answer from maintained statistics: no data read, but a bounded
    /// sketch rather than the exact set.
    pub fn statistics(as_of: Option<String>) -> Self {
        DiscoveryCost {
            mode: CostMode::Metadata,
            window_scoped: false,
            sampled: false,
            approximate: true,
            as_of,
        }
    }

    /// No answer: nothing covers the request and no read was requested.
    pub fn none() -> Self {
        DiscoveryCost {
            mode: CostMode::None,
            window_scoped: false,
            sampled: false,
            approximate: false,
            as_of: None,
        }
    }

    /// A bounded read of signal data, explicitly requested. The query that
    /// read it is named in the result's `hint`.
    pub fn sampled_scan() -> Self {
        DiscoveryCost {
            mode: CostMode::SampledScan,
            window_scoped: true,
            sampled: true,
            approximate: true,
            as_of: None,
        }
    }
}

/// What a discovery answer is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MetadataKind {
    Sources,
    Fields,
    Values,
}

/// The payload of a `metadata` result envelope.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct MetadataResult {
    pub kind: MetadataKind,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<DiscoveredSource>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<DiscoveredField>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<DiscoveredValue>,
    /// Whether a documented limit cut the list short.
    pub truncated: bool,
    /// Which tier answered, and how far the answer can be trusted.
    pub cost: DiscoveryCost,
    /// The Query IR request that produced this answer by reading data, or —
    /// when nothing covers the request — the one that would compute it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

/// The default cap on fields in one discovery answer.
pub const DEFAULT_FIELD_LIMIT: usize = 1_000;
/// The default cap on suggested values in one discovery answer.
pub const DEFAULT_VALUE_LIMIT: usize = 200;

/// The signal a source's statistics are recorded under. `metrics_histogram`
/// is a distinct IR source but the same signal, exactly as it is for read
/// authorization.
pub fn signal_for_source(source: &str) -> Option<&'static str> {
    match source {
        "logs" => Some("logs"),
        "traces" => Some("traces"),
        "profiles" => Some("profiles"),
        "metrics" | "metrics_histogram" => Some("metrics"),
        _ => None,
    }
}

/// The most recent observation time across a set of statistics rows — how
/// stale the answer built from them is.
pub fn latest_observation(stats: &[AttributeStatsRecord]) -> Option<String> {
    stats
        .iter()
        .map(|record| record.updated_at.as_str())
        .max()
        .map(str::to_string)
}

/// The declared value set for a field, if SignalDB itself determines it.
///
/// These are enumerations because the ingest path maps an OTel proto enum onto
/// them, so the values come from that mapping's own list rather than a second
/// copy here. `severity_text`, by contrast, is whatever an SDK sent: free-form,
/// and therefore not a declared set.
pub fn intrinsic_values(source: &str, field: &str) -> Option<Vec<DiscoveredValue>> {
    if source != "traces" {
        return None;
    }
    let values: Vec<&str> = match field {
        "span.kind" | "span_kind" => SpanKind::ALL.iter().map(SpanKind::to_str).collect(),
        "status.code" => SpanStatus::ALL.iter().map(SpanStatus::to_str).collect(),
        _ => return None,
    };
    Some(
        values
            .into_iter()
            .map(|value| DiscoveredValue {
                value: value.to_string(),
                count: None,
                origin: ValueOrigin::Registry,
            })
            .collect(),
    )
}

/// The registry-declared value set for a field, if one is declared.
pub fn registry_values(hit: &AttributeHit) -> Option<Vec<DiscoveredValue>> {
    if hit.def.enum_members.is_empty() {
        return None;
    }
    let values: Vec<DiscoveredValue> = hit
        .def
        .enum_members
        .iter()
        .map(|member| DiscoveredValue {
            value: match &member.value {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            },
            count: None,
            origin: ValueOrigin::Registry,
        })
        .collect();
    Some(values)
}

/// The value sketch for a field, if the analyzer maintains one.
///
/// An empty sketch is not an empty value set: it means no sketch covers the
/// key — because the analyzer has not run, or because the key's cardinality
/// exceeded the cap and a partial list would mislead. The caller reports that
/// as "nothing covers this field", never as "this field has no values".
pub fn sketch_values(stats: &[AttributeValueStat], limit: usize) -> Vec<DiscoveredValue> {
    stats
        .iter()
        .take(limit)
        .map(|stat| DiscoveredValue {
            value: stat.value.clone(),
            count: Some(stat.count),
            origin: ValueOrigin::Statistics,
        })
        .collect()
}

/// Map a registry's canonical type name onto the IR's client-visible type.
/// Anything structured (arrays, templates) is presented as a string: the
/// predicate grammar addresses it as one, and inventing a richer type here
/// would promise coercion the IR does not perform.
fn logical_type_of(registry_type: &str) -> LogicalType {
    match registry_type {
        "int" => LogicalType::Int64,
        "double" => LogicalType::Float64,
        "boolean" => LogicalType::Bool,
        _ => LogicalType::String,
    }
}

/// The fields of `source`, merged across the tiers and ordered for a picker.
///
/// `registry` maps a key to its registry definition (the caller resolves the
/// keys it cares about in one pass); `stats` is the tenant's statistics for
/// this source's signal. Returns the fields and whether `limit` truncated them.
pub fn merge_fields(
    source: &str,
    schema: &LogicalSchema,
    stats: &[AttributeStatsRecord],
    registry: &BTreeMap<String, AttributeHit>,
    limit: usize,
) -> (Vec<DiscoveredField>, bool) {
    let stats_by_key: BTreeMap<&str, &AttributeStatsRecord> = stats
        .iter()
        .map(|record| (record.attr_key.as_str(), record))
        .collect();

    // Declared fields. A name declared at two attribute levels (the
    // `attributes` bags) is emitted level-qualified, which is exactly how a
    // client addresses it.
    let declared: Vec<_> = schema
        .fields()
        .filter(|field| field.id.source == source)
        .collect();
    let mut name_counts: BTreeMap<&str, usize> = BTreeMap::new();
    for field in &declared {
        *name_counts.entry(field.id.name.as_str()).or_default() += 1;
    }

    let mut out: Vec<DiscoveredField> = Vec::with_capacity(declared.len() + stats.len());
    for field in &declared {
        let ambiguous = name_counts
            .get(field.id.name.as_str())
            .is_some_and(|count| *count > 1);
        let name = match (ambiguous, field.id.level) {
            (true, Some(level)) => format!("{}.{}", level_prefix(level), field.id.name),
            _ => field.id.name.clone(),
        };
        let hit = registry.get(&name);
        let stat = stats_by_key.get(name.as_str());
        out.push(DiscoveredField {
            value_type: field.value_type,
            level: field.id.level,
            filterable: field.filterability == Filterability::Filterable,
            origin: FieldOrigin::Declared,
            coverage: stat.and_then(|r| r.coverage()),
            cardinality: stat.and_then(|r| cardinality(r)),
            brief: hit.map(|h| h.def.brief.clone()),
            deprecated: hit.is_some_and(|h| h.def.deprecated.is_some()),
            name,
        });
    }

    let declared_names: std::collections::BTreeSet<&str> =
        out.iter().map(|f| f.name.as_str()).collect();

    // Observed keys: everything the statistics saw that the schema does not
    // declare. This is the only tier that knows a tenant's own attribute keys
    // until #813 lands.
    let mut observed: Vec<DiscoveredField> = stats
        .iter()
        .filter(|record| !declared_names.contains(record.attr_key.as_str()))
        .map(|record| {
            let hit = registry.get(&record.attr_key);
            DiscoveredField {
                name: record.attr_key.clone(),
                value_type: hit
                    .map(|h| logical_type_of(&h.def.r#type))
                    .unwrap_or(LogicalType::String),
                level: None,
                filterable: true,
                origin: if hit.is_some() {
                    FieldOrigin::Registry
                } else {
                    FieldOrigin::Observed
                },
                coverage: record.coverage(),
                cardinality: cardinality(record),
                brief: hit.map(|h| h.def.brief.clone()),
                deprecated: hit.is_some_and(|h| h.def.deprecated.is_some()),
            }
        })
        .collect();

    // Declared fields first (they are always valid and always few), then the
    // observed keys most records actually carry. `out` holds only declared
    // fields at this point, so name order is the whole ordering here.
    out.sort_by(|a, b| a.name.cmp(&b.name));
    observed.sort_by(|a, b| {
        coverage_rank(b)
            .cmp(&coverage_rank(a))
            .then_with(|| a.name.cmp(&b.name))
    });
    out.extend(observed);

    let truncated = out.len() > limit;
    out.truncate(limit);
    (out, truncated)
}

/// How a client qualifies a name that exists at more than one OTel attribute
/// level — the inverse of the prefix `LogicalSchema::resolve` strips.
fn level_prefix(level: AttributeLevel) -> &'static str {
    match level {
        AttributeLevel::Resource => "resource",
        AttributeLevel::Scope => "scope",
        AttributeLevel::Record => "record",
    }
}

/// Coverage as an integer rank so the ordering is total and deterministic
/// (floats have no `Ord`, and an unknown coverage must sort last, not first).
fn coverage_rank(field: &DiscoveredField) -> i64 {
    field
        .coverage
        .map(|c| (c * 1_000_000.0) as i64)
        .unwrap_or(-1)
}

fn cardinality(record: &AttributeStatsRecord) -> Option<CardinalityEstimate> {
    (record.distinct_estimate > 0).then_some(CardinalityEstimate {
        estimate: record.distinct_estimate,
        at_least: record.capped,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::logical::LogicalField;

    fn stat(
        key: &str,
        present: i64,
        total: i64,
        distinct: i64,
        capped: bool,
    ) -> AttributeStatsRecord {
        AttributeStatsRecord {
            tenant_id: "t".to_string(),
            dataset_id: "d".to_string(),
            signal: "logs".to_string(),
            attr_key: key.to_string(),
            present_rows: present,
            total_rows: total,
            distinct_estimate: distinct,
            capped,
            query_hits: 0,
            promote_streak: 0,
            updated_at: "2026-08-17 09:00:00".to_string(),
        }
    }

    fn schema() -> LogicalSchema {
        LogicalSchema::new(vec![
            LogicalField::record_metadata("logs", "severity_text", LogicalType::String),
            LogicalField::record_metadata("logs", "body", LogicalType::AnyValue).retrieval_only(),
            LogicalField::attribute(
                "logs",
                AttributeLevel::Resource,
                "service.name",
                LogicalType::String,
            ),
            LogicalField::record_metadata("traces", "span.kind", LogicalType::String),
        ])
    }

    #[test]
    fn declared_fields_come_first_and_carry_their_type_and_filterability() {
        let (fields, truncated) = merge_fields("logs", &schema(), &[], &BTreeMap::new(), 100);
        assert!(!truncated);
        let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, vec!["body", "service.name", "severity_text"]);
        assert!(fields.iter().all(|f| f.origin == FieldOrigin::Declared));
        let body = fields.iter().find(|f| f.name == "body").unwrap();
        assert!(
            !body.filterable,
            "retrieval-only fields are listed, not hidden"
        );
        let service = fields.iter().find(|f| f.name == "service.name").unwrap();
        assert_eq!(service.level, Some(AttributeLevel::Resource));
    }

    #[test]
    fn a_source_only_sees_its_own_declared_fields() {
        let (fields, _) = merge_fields("logs", &schema(), &[], &BTreeMap::new(), 100);
        assert!(fields.iter().all(|f| f.name != "span.kind"));
    }

    #[test]
    fn observed_keys_are_ordered_by_coverage_and_carry_hints() {
        let stats = vec![
            stat("rare.key", 1, 1000, 3, false),
            stat("http.route", 800, 1000, 42, false),
            stat("trace.sampler", 500, 1000, 0, false),
        ];
        let (fields, _) = merge_fields("logs", &schema(), &stats, &BTreeMap::new(), 100);
        let observed: Vec<&str> = fields
            .iter()
            .filter(|f| f.origin != FieldOrigin::Declared)
            .map(|f| f.name.as_str())
            .collect();
        assert_eq!(observed, vec!["http.route", "trace.sampler", "rare.key"]);

        let route = fields.iter().find(|f| f.name == "http.route").unwrap();
        assert_eq!(route.coverage, Some(0.8));
        assert_eq!(
            route.cardinality,
            Some(CardinalityEstimate {
                estimate: 42,
                at_least: false
            })
        );
        // No statistics for a distinct count means no claim about it.
        let sampler = fields.iter().find(|f| f.name == "trace.sampler").unwrap();
        assert_eq!(sampler.cardinality, None);
    }

    #[test]
    fn a_capped_distinct_count_is_a_lower_bound() {
        let stats = vec![stat("user.id", 900, 1000, 10_000, true)];
        let (fields, _) = merge_fields("logs", &schema(), &stats, &BTreeMap::new(), 100);
        let field = fields.iter().find(|f| f.name == "user.id").unwrap();
        assert_eq!(
            field.cardinality,
            Some(CardinalityEstimate {
                estimate: 10_000,
                at_least: true
            })
        );
    }

    #[test]
    fn a_declared_field_without_statistics_reports_unknown_hints() {
        let (fields, _) = merge_fields("logs", &schema(), &[], &BTreeMap::new(), 100);
        let severity = fields.iter().find(|f| f.name == "severity_text").unwrap();
        assert_eq!(severity.coverage, None);
        assert_eq!(severity.cardinality, None);
    }

    #[test]
    fn a_declared_field_is_never_duplicated_by_an_observed_key() {
        let stats = vec![stat("service.name", 1000, 1000, 7, false)];
        let (fields, _) = merge_fields("logs", &schema(), &stats, &BTreeMap::new(), 100);
        let matching: Vec<_> = fields.iter().filter(|f| f.name == "service.name").collect();
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].origin, FieldOrigin::Declared);
        // ... but the statistics still enrich it.
        assert_eq!(matching[0].coverage, Some(1.0));
    }

    #[test]
    fn the_limit_truncates_and_says_so() {
        let stats: Vec<AttributeStatsRecord> = (0..10)
            .map(|i| stat(&format!("k{i}"), 1, 10, 1, false))
            .collect();
        let (fields, truncated) = merge_fields("logs", &schema(), &stats, &BTreeMap::new(), 5);
        assert_eq!(fields.len(), 5);
        assert!(truncated);
    }

    #[test]
    fn repeated_merges_produce_the_same_order() {
        let stats = vec![
            stat("b.key", 5, 10, 1, false),
            stat("a.key", 5, 10, 1, false),
            stat("c.key", 9, 10, 1, false),
        ];
        let first = merge_fields("logs", &schema(), &stats, &BTreeMap::new(), 100).0;
        let second = merge_fields("logs", &schema(), &stats, &BTreeMap::new(), 100).0;
        assert_eq!(first, second);
        let observed: Vec<&str> = first
            .iter()
            .filter(|f| f.origin != FieldOrigin::Declared)
            .map(|f| f.name.as_str())
            .collect();
        assert_eq!(observed, vec!["c.key", "a.key", "b.key"]);
    }

    #[test]
    fn intrinsic_value_sets_are_the_ones_signaldb_writes() {
        let kinds = intrinsic_values("traces", "span.kind").expect("span kind is declared");
        assert_eq!(kinds.len(), 5);
        assert!(kinds.iter().all(|v| v.origin == ValueOrigin::Registry));
        assert!(kinds.iter().any(|v| v.value == "Server"));
        assert!(intrinsic_values("traces", "status.code").is_some());
        // Free-form: an SDK writes whatever it likes, so it is not a declared set.
        assert!(intrinsic_values("logs", "severity_text").is_none());
        assert!(intrinsic_values("logs", "http.route").is_none());
    }

    #[test]
    fn the_answers_age_is_the_newest_observation() {
        let mut older = stat("a", 1, 10, 1, false);
        older.updated_at = "2026-08-01 00:00:00".to_string();
        let newer = stat("b", 1, 10, 1, false);
        assert_eq!(
            latest_observation(&[older, newer]).as_deref(),
            Some("2026-08-17 09:00:00")
        );
        assert_eq!(latest_observation(&[]), None);
    }

    #[test]
    fn every_ir_source_maps_to_a_statistics_signal() {
        for source in ["logs", "traces", "profiles", "metrics", "metrics_histogram"] {
            assert!(signal_for_source(source).is_some(), "{source}");
        }
        assert_eq!(signal_for_source("metrics_histogram"), Some("metrics"));
        assert_eq!(signal_for_source("nope"), None);
    }

    #[test]
    fn a_sketch_answer_is_labelled_approximate_and_a_declared_one_is_not() {
        assert!(!DiscoveryCost::metadata(None).approximate);
        assert!(DiscoveryCost::statistics(None).approximate);
        assert!(DiscoveryCost::sampled_scan().approximate);
        // Both statistics and declared answers are free; only the flag differs.
        assert_eq!(DiscoveryCost::statistics(None).mode, CostMode::Metadata);
    }

    #[test]
    fn sketch_values_carry_their_counts_and_respect_the_limit() {
        let stats = vec![
            AttributeValueStat {
                value: "/api/orders".to_string(),
                count: 900,
                updated_at: "2026-08-17 09:00:00".to_string(),
            },
            AttributeValueStat {
                value: "/api/users".to_string(),
                count: 100,
                updated_at: "2026-08-17 09:00:00".to_string(),
            },
        ];
        let values = sketch_values(&stats, 10);
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].value, "/api/orders");
        assert_eq!(values[0].count, Some(900));
        assert!(values.iter().all(|v| v.origin == ValueOrigin::Statistics));
        assert_eq!(sketch_values(&stats, 1).len(), 1);
        assert!(sketch_values(&[], 10).is_empty());
    }

    #[test]
    fn a_zero_row_statistic_makes_no_coverage_claim() {
        let stats = vec![stat("empty.key", 0, 0, 0, false)];
        let (fields, _) = merge_fields("logs", &schema(), &stats, &BTreeMap::new(), 100);
        let field = fields.iter().find(|f| f.name == "empty.key").unwrap();
        assert_eq!(field.coverage, None);
    }
}
