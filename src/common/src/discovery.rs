//! # Query discovery — what a tenant can query, without scanning it
//!
//! Assembles the answer to "what can I filter on" from four metadata tiers,
//! none of which reads signal data:
//!
//! 1. **declared** — [`LogicalSchema`], the canonical client-visible field
//!    catalog: membership, canonical type, attribute level, filterability;
//! 2. **authority** — the layer-3 type authority's committed canonical types,
//!    the same ones the query planner enforces; they win over any other type
//!    and list a typed key even before statistics observe it;
//! 3. **registry** — the tenant's schema registries, which give a key a
//!    fallback type, a description and its declared value set;
//! 4. **observed** — the compactor's `attribute_stats`, which knows which
//!    attribute keys a tenant actually emits.
//!
//! Membership is `declared ∪ authority ∪ observed`. A semantic-convention
//! registry knows thousands of keys a tenant has never sent; listing them
//! would bury the tenant's real fields, so the registry enriches and never
//! contributes membership.
//!
//! This module is deliberately I/O-free: callers fetch the inputs and pass
//! them in, so the merge rules are unit-testable without a catalog. See
//! `openspec/changes/archive/2026-09-22-query-field-discovery`.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::catalog::{AttributeStatsRecord, AttributeValueStat};
use crate::model::span::{SpanKind, SpanStatus};
use crate::schema::logical::{
    AttributeLevel, Filterability, LogicalSchema, LogicalType, attribute_qualifier,
    level_is_addressable,
};
use crate::schema::type_authority::AttributeKeyType;
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
    /// The type authority committed a canonical type for it at ingest,
    /// whether or not statistics have observed it yet.
    Authority,
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
    /// The canonical value type a literal is coerced to: the type
    /// authority's for an attribute it has typed, else the registry's, else
    /// string.
    #[serde(rename = "type")]
    pub value_type: LogicalType,
    /// The OTel attribute level, when known. Statistics carry no level, so an
    /// observed key the type authority has not typed reports `null` rather
    /// than a guess.
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
    /// What the statistics behind the answer were built from, when
    /// statistics contributed to it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub analyzed: Option<StatisticsCoverage>,
    /// Whether the statistics behind the answer cover less than the
    /// requested window. The analyzer observes one compacted partition at a
    /// time, so a statistics answer over a longer window can miss values and
    /// fields that only occur elsewhere, and its cardinalities are lower
    /// bounds. Pass `sample: true` to read the window instead.
    pub partial: bool,
}

/// What a statistics-derived answer was built from: the rows the analyzer
/// last read and the event-time span they came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct StatisticsCoverage {
    /// Rows the analyzer read for the most recent observation.
    pub rows_analyzed: i64,
    /// Start of the event-time span those rows came from, in Unix
    /// nanoseconds. Absent for statistics written before spans were recorded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_ns: Option<i64>,
    /// End (exclusive) of that span, in Unix nanoseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_ns: Option<i64>,
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
            analyzed: None,
            partial: false,
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
            analyzed: None,
            partial: false,
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
            analyzed: None,
            partial: false,
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
            analyzed: None,
            partial: false,
        }
    }

    /// Record what the statistics rows behind this answer cover, and whether
    /// that falls short of the requested `[start_ns, end_ns)` window. No rows
    /// is no coverage at all, so it is partial too.
    pub fn with_statistics<'r>(
        mut self,
        stats: impl IntoIterator<Item = &'r AttributeStatsRecord>,
        start_ns: i64,
        end_ns: i64,
    ) -> Self {
        // Rows the querier created for demand counters alone carry no
        // observation.
        let stats: Vec<&AttributeStatsRecord> = stats
            .into_iter()
            .filter(|record| record.total_rows > 0)
            .collect();
        let newest = stats.iter().max_by(|a, b| a.updated_at.cmp(&b.updated_at));
        self.analyzed = newest.map(|record| StatisticsCoverage {
            rows_analyzed: record.total_rows,
            start_ns: record.analyzed_span.map(|span| span.start_ns),
            end_ns: record.analyzed_span.map(|span| span.end_ns),
        });
        self.partial = stats.is_empty()
            || !stats.iter().all(|record| {
                record
                    .analyzed_span
                    .is_some_and(|span| span.covers(start_ns, end_ns))
            });
        self
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

/// The signal a source's statistics are recorded under. Every `metrics_*`
/// physical table (`metrics_histogram`, `metrics_gauge`, ...) is a distinct
/// IR source but the same signal, exactly as it is for read authorization.
pub fn signal_for_source(source: &str) -> Option<&'static str> {
    match source {
        "logs" => Some("logs"),
        "traces" => Some("traces"),
        "profiles" => Some("profiles"),
        "metrics" | "metric_exemplars" | "exemplars" => Some("metrics"),
        name if name.starts_with("metrics_") => Some("metrics"),
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

/// `key` with one leading source qualifier removed (`span.http.method` on
/// traces is the attribute `http.method`; `log.log.file.path` is
/// `log.file.path`), or `None` when it starts with none. The inverse of
/// [`listed_name`]'s qualification: statistics and registries are keyed by the
/// bare key, so a qualified name is stripped before those lookups.
pub fn strip_qualifier<'k>(source: &str, key: &'k str) -> Option<&'k str> {
    [
        AttributeLevel::Record,
        AttributeLevel::Scope,
        AttributeLevel::Resource,
    ]
    .into_iter()
    .filter_map(|level| attribute_qualifier(source, level))
    .find_map(|q| key.strip_prefix(q)?.strip_prefix('.'))
}

/// The name a client writes to address `key` at `level` on `source`, or
/// `None` when no name reaches it.
///
/// A name is level-qualified when `qualify` says the key exists at several
/// levels, and also when the bare key starts with one of the source's own
/// qualifiers (`log.file.path` on logs is addressed `log.log.file.path`, or the
/// planner would read the key `file.path`). A level the source has no qualifier
/// for can't be qualified.
fn listed_name(
    source: &str,
    key: &str,
    level: Option<AttributeLevel>,
    qualify: bool,
) -> Option<String> {
    let starts_with_qualifier = strip_qualifier(source, key).is_some();
    if !qualify && !starts_with_qualifier {
        return Some(key.to_string());
    }
    let qualifier = attribute_qualifier(source, level?)?;
    Some(format!("{qualifier}.{key}"))
}

/// The fields of `source`, merged across the tiers and ordered for a picker.
///
/// `registry` maps a key to its registry definition (the caller resolves the
/// keys it cares about in one pass); it supplies descriptions and deprecation,
/// never a type or membership. `stats` is the tenant's statistics for this
/// source's signal; `types` is the type authority's committed canonical types
/// for the table. An attribute's type is the authority's, or `string` — what
/// the planner enforces — and every listed name resolves to that type.
/// Returns the fields and whether `limit` truncated them.
pub fn merge_fields(
    source: &str,
    schema: &LogicalSchema,
    stats: &[AttributeStatsRecord],
    registry: &BTreeMap<String, AttributeHit>,
    types: &[AttributeKeyType],
    limit: usize,
) -> (Vec<DiscoveredField>, bool) {
    let stats_by_key: BTreeMap<&str, &AttributeStatsRecord> = stats
        .iter()
        .map(|record| (record.attr_key.as_str(), record))
        .collect();

    // Declared fields. A name declared at two attribute levels, and every
    // scope-level name (bare `name` on traces is the span's), is emitted
    // level-qualified, which is exactly how a client addresses it.
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
        let qualifier = field
            .id
            .level
            .filter(|level| ambiguous || *level == AttributeLevel::Scope)
            .and_then(|level| attribute_qualifier(source, level));
        let name = match qualifier {
            Some(q) => format!("{q}.{}", field.id.name),
            None => field.id.name.clone(),
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

    // Levels the type authority has committed a type at, per key — only the
    // levels this source can address.
    let mut typed: BTreeMap<&str, Vec<(AttributeLevel, LogicalType)>> = BTreeMap::new();
    for row in types
        .iter()
        .filter(|row| level_is_addressable(source, row.level))
    {
        typed
            .entry(row.attr_key.as_str())
            .or_default()
            .push((row.level, row.canonical_type.into()));
    }
    for levels in typed.values_mut() {
        levels.sort_by_key(|(level, _)| *level);
    }

    // Observed keys: everything the statistics saw that the schema does not
    // declare, plus every key the type authority typed that statistics have
    // not seen yet. A key typed at two levels is level-qualified, as a
    // declared ambiguous name is. A name that would read a declared field (an
    // attribute called `name` at scope level is `scope.name`, the column) is
    // not listed under it.
    let keys: std::collections::BTreeSet<&str> = stats
        .iter()
        .map(|record| record.attr_key.as_str())
        .chain(typed.keys().copied())
        .collect();
    // Each entry carries the key's coverage rank so a level-qualified entry,
    // which reports no coverage, still sorts with its key.
    let mut observed: Vec<(i64, DiscoveredField)> = Vec::new();
    for key in keys {
        let hit = registry.get(key);
        let levels = typed.get(key).map(Vec::as_slice).unwrap_or_default();
        let origin = match (levels.is_empty(), hit) {
            (false, _) => FieldOrigin::Authority,
            (true, Some(_)) => FieldOrigin::Registry,
            (true, None) => FieldOrigin::Observed,
        };
        let qualify = levels.len() > 1;
        let stat = stats_by_key.get(key);
        let rank = coverage_rank(stat.and_then(|r| r.coverage()));
        // Statistics are per key, not per level.
        let stat = stat.filter(|_| !qualify);

        // An untyped key has no level: one entry, `string`, as the planner
        // reads it.
        let entries: Vec<(Option<AttributeLevel>, LogicalType)> = if levels.is_empty() {
            vec![(None, LogicalType::String)]
        } else {
            levels
                .iter()
                .map(|&(level, value_type)| (Some(level), value_type))
                .collect()
        };
        for (level, value_type) in entries {
            let Some(name) = listed_name(source, key, level, qualify) else {
                continue;
            };
            if schema.resolve(source, &name).is_some() {
                continue;
            }
            observed.push((
                rank,
                DiscoveredField {
                    name,
                    value_type,
                    level,
                    filterable: true,
                    origin,
                    coverage: stat.and_then(|r| r.coverage()),
                    cardinality: stat.and_then(|r| cardinality(r)),
                    brief: hit.map(|h| h.def.brief.clone()),
                    deprecated: hit.is_some_and(|h| h.def.deprecated.is_some()),
                },
            ));
        }
    }

    // Declared fields first (they are always valid and always few), then the
    // observed keys most records actually carry.
    out.sort_by(|a, b| a.name.cmp(&b.name));
    observed
        .sort_by(|(rank_a, a), (rank_b, b)| rank_b.cmp(rank_a).then_with(|| a.name.cmp(&b.name)));
    out.extend(observed.into_iter().map(|(_, field)| field));

    let truncated = out.len() > limit;
    out.truncate(limit);
    (out, truncated)
}

/// Coverage as an integer rank so the ordering is total and deterministic
/// (floats have no `Ord`, and an unknown coverage must sort last, not first).
fn coverage_rank(coverage: Option<f64>) -> i64 {
    coverage.map(|c| (c * 1_000_000.0) as i64).unwrap_or(-1)
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
    use crate::schema::type_authority::CanonicalType;

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
            analyzed_span: None,
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
        let (fields, truncated) = merge_fields("logs", &schema(), &[], &BTreeMap::new(), &[], 100);
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
        let (fields, _) = merge_fields("logs", &schema(), &[], &BTreeMap::new(), &[], 100);
        assert!(fields.iter().all(|f| f.name != "span.kind"));
    }

    #[test]
    fn observed_keys_are_ordered_by_coverage_and_carry_hints() {
        let stats = vec![
            stat("rare.key", 1, 1000, 3, false),
            stat("http.route", 800, 1000, 42, false),
            stat("trace.sampler", 500, 1000, 0, false),
        ];
        let (fields, _) = merge_fields("logs", &schema(), &stats, &BTreeMap::new(), &[], 100);
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
        let (fields, _) = merge_fields("logs", &schema(), &stats, &BTreeMap::new(), &[], 100);
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
        let (fields, _) = merge_fields("logs", &schema(), &[], &BTreeMap::new(), &[], 100);
        let severity = fields.iter().find(|f| f.name == "severity_text").unwrap();
        assert_eq!(severity.coverage, None);
        assert_eq!(severity.cardinality, None);
    }

    #[test]
    fn a_declared_field_is_never_duplicated_by_an_observed_key() {
        let stats = vec![stat("service.name", 1000, 1000, 7, false)];
        let (fields, _) = merge_fields("logs", &schema(), &stats, &BTreeMap::new(), &[], 100);
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
        let (fields, truncated) = merge_fields("logs", &schema(), &stats, &BTreeMap::new(), &[], 5);
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
        let first = merge_fields("logs", &schema(), &stats, &BTreeMap::new(), &[], 100).0;
        let second = merge_fields("logs", &schema(), &stats, &BTreeMap::new(), &[], 100).0;
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
        for source in [
            "logs",
            "traces",
            "profiles",
            "metrics",
            "metrics_histogram",
            "metrics_gauge",
        ] {
            assert!(signal_for_source(source).is_some(), "{source}");
        }
        assert_eq!(signal_for_source("metrics_histogram"), Some("metrics"));
        assert_eq!(signal_for_source("metrics_gauge"), Some("metrics"));
        assert_eq!(signal_for_source("exemplars"), Some("metrics"));
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

    fn spanning(start_ns: i64, end_ns: i64) -> AttributeStatsRecord {
        AttributeStatsRecord {
            analyzed_span: Some(crate::catalog::AnalyzedSpan { start_ns, end_ns }),
            ..stat("k", 10, 10, 1, false)
        }
    }

    #[test]
    fn statistics_covering_the_window_are_not_partial() {
        let record = spanning(0, 100);
        let cost = DiscoveryCost::metadata(None).with_statistics([&record], 10, 90);
        assert!(!cost.partial);
        assert_eq!(
            cost.analyzed,
            Some(StatisticsCoverage {
                rows_analyzed: 10,
                start_ns: Some(0),
                end_ns: Some(100),
            })
        );
    }

    #[test]
    fn statistics_short_of_the_window_are_partial() {
        let record = spanning(50, 100);
        assert!(
            DiscoveryCost::metadata(None)
                .with_statistics([&record], 0, 100)
                .partial
        );
    }

    #[test]
    fn statistics_without_a_recorded_span_or_rows_are_partial() {
        let unspanned = stat("k", 10, 10, 1, false);
        let cost = DiscoveryCost::metadata(None).with_statistics([&unspanned], 0, 1);
        assert!(cost.partial);
        assert_eq!(cost.analyzed.and_then(|a| a.start_ns), None);

        // A demand-only row (no rows analyzed) is no observation at all.
        let demand_only = stat("k", 0, 0, 0, false);
        let cost = DiscoveryCost::metadata(None).with_statistics([&demand_only], 0, 1);
        assert!(cost.partial);
        assert_eq!(cost.analyzed, None);
    }

    #[test]
    fn a_zero_row_statistic_makes_no_coverage_claim() {
        let stats = vec![stat("empty.key", 0, 0, 0, false)];
        let (fields, _) = merge_fields("logs", &schema(), &stats, &BTreeMap::new(), &[], 100);
        let field = fields.iter().find(|f| f.name == "empty.key").unwrap();
        assert_eq!(field.coverage, None);
    }

    fn typed(key: &str, level: AttributeLevel, canonical: CanonicalType) -> AttributeKeyType {
        AttributeKeyType {
            attr_key: key.to_string(),
            level,
            canonical_type: canonical,
        }
    }

    fn registry_hit(key: &str, registry_type: &str) -> BTreeMap<String, AttributeHit> {
        let hit: AttributeHit = serde_json::from_value(serde_json::json!({
            "namespace": "otel", "version": "1", "source": "bundled",
            "key": key, "group_id": "g", "brief": "a registry brief",
            "type": registry_type,
        }))
        .unwrap();
        BTreeMap::from([(key.to_string(), hit)])
    }

    fn field<'a>(fields: &'a [DiscoveredField], name: &str) -> &'a DiscoveredField {
        fields.iter().find(|f| f.name == name).unwrap()
    }

    #[test]
    fn a_qualified_name_strips_one_source_qualifier() {
        assert_eq!(
            strip_qualifier("traces", "span.http.method"),
            Some("http.method")
        );
        assert_eq!(strip_qualifier("logs", "resource.env"), Some("env"));
        assert_eq!(
            strip_qualifier("logs", "log.log.file.path"),
            Some("log.file.path")
        );
        assert_eq!(strip_qualifier("logs", "http.method"), None);
        assert_eq!(strip_qualifier("logs", "logger"), None);
        // Metrics address record attributes as `point.`, traces as `span.`.
        assert_eq!(strip_qualifier("metrics", "span.x"), None);
        assert_eq!(strip_qualifier("exemplars", "resource.x"), None);
    }

    #[test]
    fn the_authoritys_type_is_listed_and_the_registrys_is_not() {
        let stats = vec![
            stat("http.status_code", 9, 10, 4, false),
            stat("retries", 9, 10, 4, false),
            stat("untyped", 9, 10, 4, false),
        ];
        let types = vec![
            typed(
                "http.status_code",
                AttributeLevel::Record,
                CanonicalType::Int64,
            ),
            typed("retries", AttributeLevel::Record, CanonicalType::Int64),
        ];
        // A registry that says `string` for one key and `int` for the untyped
        // one: neither decides the type, only the authority does.
        let mut registry = registry_hit("http.status_code", "string");
        registry.extend(registry_hit("untyped", "int"));
        let (fields, _) = merge_fields("logs", &schema(), &stats, &registry, &types, 100);

        let status = field(&fields, "http.status_code");
        assert_eq!(status.value_type, LogicalType::Int64);
        assert_eq!(status.brief.as_deref(), Some("a registry brief"));
        assert_eq!(status.level, Some(AttributeLevel::Record));
        assert_eq!(status.coverage, Some(0.9), "statistics still enrich it");
        assert_eq!(field(&fields, "retries").value_type, LogicalType::Int64);
        let untyped = field(&fields, "untyped");
        assert_eq!(
            untyped.value_type,
            LogicalType::String,
            "what the planner reads"
        );
        assert_eq!(untyped.origin, FieldOrigin::Registry);
        assert_eq!(untyped.brief.as_deref(), Some("a registry brief"));
    }

    #[test]
    fn a_typed_key_no_statistics_have_seen_is_still_listed() {
        let types = vec![typed(
            "queue.depth",
            AttributeLevel::Record,
            CanonicalType::Float64,
        )];
        let (fields, _) = merge_fields("logs", &schema(), &[], &BTreeMap::new(), &types, 100);
        let depth = field(&fields, "queue.depth");
        assert_eq!(depth.value_type, LogicalType::Float64);
        assert_eq!(depth.origin, FieldOrigin::Authority);
        assert_eq!(depth.coverage, None);
        assert!(depth.filterable);
    }

    #[test]
    fn a_key_typed_at_two_levels_is_qualified_the_way_the_source_addresses_each_level() {
        let stats = vec![stat("region", 5, 10, 2, false)];
        let types = vec![
            typed("region", AttributeLevel::Resource, CanonicalType::String),
            typed("region", AttributeLevel::Record, CanonicalType::Int64),
        ];
        for (source, record_name) in [
            ("logs", "log.region"),
            ("traces", "span.region"),
            ("profiles", "profile.region"),
            ("metrics", "point.region"),
        ] {
            let (fields, _) =
                merge_fields(source, &schema(), &stats, &BTreeMap::new(), &types, 100);
            assert!(fields.iter().all(|f| f.name != "region"), "{source}");
            let resource = field(&fields, "resource.region");
            assert_eq!(resource.value_type, LogicalType::String);
            assert_eq!(resource.level, Some(AttributeLevel::Resource));
            let record = field(&fields, record_name);
            assert_eq!(record.value_type, LogicalType::Int64, "{source}");
            assert_eq!(record.level, Some(AttributeLevel::Record));
            assert_eq!(
                record.coverage, None,
                "statistics are per key, not per level"
            );
        }
    }

    #[test]
    fn levels_a_source_cannot_address_are_not_listed() {
        let types = vec![
            typed("k", AttributeLevel::Record, CanonicalType::Int64),
            typed("k", AttributeLevel::Scope, CanonicalType::String),
            typed("only.scope", AttributeLevel::Scope, CanonicalType::Int64),
            typed(
                "only.resource",
                AttributeLevel::Resource,
                CanonicalType::Int64,
            ),
        ];
        // Metrics have no scope level: `k` is record-only, so unqualified.
        let (fields, _) = merge_fields("metrics", &schema(), &[], &BTreeMap::new(), &types, 100);
        assert_eq!(field(&fields, "k").value_type, LogicalType::Int64);
        assert!(fields.iter().all(|f| f.name != "only.scope"));
        // Exemplars have one, record-level, bare-keyed container.
        let (fields, _) = merge_fields("exemplars", &schema(), &[], &BTreeMap::new(), &types, 100);
        let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, vec!["k"]);
    }

    #[test]
    fn a_bare_key_that_starts_with_a_source_qualifier_is_escaped() {
        let types = vec![
            typed(
                "log.file.path",
                AttributeLevel::Record,
                CanonicalType::String,
            ),
            typed(
                "resource.foo",
                AttributeLevel::Resource,
                CanonicalType::Int64,
            ),
            typed("logger", AttributeLevel::Record, CanonicalType::String),
        ];
        let stats = vec![stat("scope.stray", 1, 10, 1, false)];
        let (fields, _) = merge_fields("logs", &schema(), &stats, &BTreeMap::new(), &types, 100);
        assert_eq!(
            field(&fields, "log.log.file.path").level,
            Some(AttributeLevel::Record)
        );
        assert_eq!(
            field(&fields, "resource.resource.foo").value_type,
            LogicalType::Int64
        );
        // `logger` only shares letters with the `log` qualifier.
        field(&fields, "logger");
        // An untyped key has no known level to escape under, so it is not listed.
        assert!(fields.iter().all(|f| !f.name.contains("stray")));
    }

    #[test]
    fn scope_level_declared_names_are_qualified_and_attributes_cannot_shadow_them() {
        let schema = LogicalSchema::new(vec![
            LogicalField::attribute("logs", AttributeLevel::Scope, "name", LogicalType::String),
            LogicalField::record_metadata("traces", "name", LogicalType::String),
            LogicalField::attribute("traces", AttributeLevel::Scope, "name", LogicalType::String),
        ]);
        let (logs, _) = merge_fields("logs", &schema, &[], &BTreeMap::new(), &[], 100);
        assert_eq!(
            logs.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
            vec!["scope.name"]
        );
        // On traces bare `name` is the span's, scope's is qualified.
        let (traces, _) = merge_fields("traces", &schema, &[], &BTreeMap::new(), &[], 100);
        let names: Vec<&str> = traces.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, vec!["name", "scope.name"]);

        // An attribute *named* `name` at scope level would be listed as
        // `scope.name` and read the column, so that entry is dropped; the
        // record-level one is listed as `log.name`.
        let types = vec![
            typed("name", AttributeLevel::Scope, CanonicalType::Int64),
            typed("name", AttributeLevel::Record, CanonicalType::Int64),
        ];
        let (logs, _) = merge_fields("logs", &schema, &[], &BTreeMap::new(), &types, 100);
        let listed: Vec<&str> = logs.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(listed, vec!["scope.name", "log.name"], "{listed:?}");
        assert_eq!(field(&logs, "log.name").value_type, LogicalType::Int64);
        assert_eq!(field(&logs, "scope.name").origin, FieldOrigin::Declared);
    }

    #[test]
    fn qualified_entries_sort_with_their_keys_coverage_so_limit_keeps_them() {
        let stats = vec![
            stat("popular", 9, 10, 1, false),
            stat("rare", 1, 10, 1, false),
        ];
        let types = vec![
            typed("popular", AttributeLevel::Resource, CanonicalType::String),
            typed("popular", AttributeLevel::Record, CanonicalType::Int64),
        ];
        let declared = schema().fields().filter(|f| f.id.source == "logs").count();
        let (fields, truncated) = merge_fields(
            "logs",
            &schema(),
            &stats,
            &BTreeMap::new(),
            &types,
            declared + 2,
        );
        assert!(truncated);
        assert!(fields.iter().any(|f| f.name == "log.popular"));
        assert!(fields.iter().all(|f| f.name != "rare"));
    }

    #[test]
    fn a_declared_field_is_not_retyped_or_duplicated_by_the_authority() {
        let types = vec![typed(
            "service.name",
            AttributeLevel::Resource,
            CanonicalType::Int64,
        )];
        let (fields, _) = merge_fields("logs", &schema(), &[], &BTreeMap::new(), &types, 100);
        let matching: Vec<_> = fields.iter().filter(|f| f.name == "service.name").collect();
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].origin, FieldOrigin::Declared);
        assert_eq!(matching[0].value_type, LogicalType::String);
    }
}
