//! otel-native-schema layer 2 (task 2.3): checks that the physical schema in
//! `schemas.toml` is the declared realization of the logical schema in
//! `LogicalSchema::core()`, for every signal's *current* physical version.
//!
//! Every physical column must be exactly one of:
//!   (a) the realization of a logical field of that signal's source, found
//!       either under its own name or via the alias convention the querier's
//!       `SourcePlan::aliases` (src/querier/src/query/ir_planner.rs) uses,
//!   (b) an attribute container (map column holding a resource/scope/record
//!       attribute bag), or
//!   (c) marked `physical_only` (or `computed`) in schemas.toml.
//!
//! A handful of pre-existing gaps don't fit that yet; each is called out
//! below with why, rather than silently special-cased.

use common::schema::SCHEMA_DEFINITIONS;
use common::schema::logical::LogicalSchema;
use common::schema::schema_parser::{ResolvedField, TableSchemaDefinition};
use common::schema::typed_attributes;
use std::collections::HashMap;

/// Physical column name -> logical field name(s), mirroring
/// `SourcePlan::aliases` in `src/querier/src/query/ir_planner.rs`. Kept as an
/// explicit, independent table (rather than importing the querier's private
/// `SourcePlan`) so this test can't silently pass just because the querier's
/// own table changed. A physical column may realize more than one logical
/// name (e.g. `duration_nanos` realizes both `duration` and `duration_nano`).
fn alias_table(source: &str) -> &'static [(&'static str, &'static str)] {
    match source {
        "logs" => &[
            ("service_name", "service.name"),
            ("resource_schema_url", "resource.schema_url"),
            ("scope_name", "scope.name"),
            ("scope_version", "scope.version"),
            ("scope_schema_url", "scope.schema_url"),
            ("resource_identity", "resource.identity"),
        ],
        "traces" => &[
            ("service_name", "service.name"),
            ("span_name", "name"),
            ("span_name", "span.name"),
            ("duration_nanos", "duration"),
            ("duration_nanos", "duration_nano"),
            ("status_code", "status.code"),
            ("resource_schema_url", "resource.schema_url"),
            ("scope_name", "scope.name"),
            ("scope_version", "scope.version"),
            ("scope_schema_url", "scope.schema_url"),
            ("resource_identity", "resource.identity"),
            // `events` has no alias entry in `SourcePlan::aliases` itself --
            // it's resolved through a dedicated special case in
            // `SchemaResolver::column_for` (field `"events"` or
            // `"span_events"` both hit `Resolved::SpanEvents { events_column:
            // "events" }`) rather than the generic alias table. Recorded here
            // as the logical name it actually realizes.
            ("events", "span_events"),
        ],
        "metrics_gauge" | "metrics_sum" => &[
            ("service_name", "service.name"),
            ("metric_name", "metric.name"),
            ("value", "metric.value"),
            ("resource_identity", "resource.identity"),
        ],
        "metrics_histogram" => &[
            ("service_name", "service.name"),
            ("metric_name", "metric.name"),
        ],
        // Profiles: only `timestamp` and `resource.identity` are registered
        // in `LogicalSchema::core()` today (see the comment on
        // `record_metadata("profiles", "timestamp", ...)` -- "the remaining
        // profile scalars still resolve through the planner's alias table").
        // Everything else profile-scalar-shaped is a `known_gap` below, not
        // an alias, since there is no logical field for it to realize yet.
        "profiles" => &[("resource_identity", "resource.identity")],
        _ => &[],
    }
}

/// The first logical name a physical column realizes, used for the forward
/// (physical -> logical) direction -- any one match is enough to prove the
/// column has logical meaning.
fn expected_alias(source: &str, physical: &str) -> Option<&'static str> {
    alias_table(source)
        .iter()
        .find(|(p, _)| *p == physical)
        .map(|(_, logical)| *logical)
}

/// Whether some physical column realizes exactly this logical name, used for
/// the reverse (logical -> physical) direction.
fn realizes(source: &str, physical: &str, logical_name: &str) -> bool {
    physical == logical_name
        || alias_table(source)
            .iter()
            .any(|(p, l)| *p == physical && *l == logical_name)
}

/// Attribute-container base names per source, mirroring
/// `SourcePlan::containers`. A container is realized either as the legacy
/// single `map<string,string>` column named exactly this, or -- in the
/// typed-attribute layout (otel-native-schema layer 4) -- as the five
/// columns `typed_attributes::typed_columns` names for it.
fn containers(source: &str) -> &'static [&'static str] {
    match source {
        "logs" => &["log_attributes", "scope_attributes", "resource_attributes"],
        "traces" => &["span_attributes", "scope_attributes", "resource_attributes"],
        "profiles" => &[
            "profile_attributes",
            "scope_attributes",
            "resource_attributes",
        ],
        "metrics_gauge" | "metrics_sum" | "metrics_histogram" => {
            &["attributes", "resource_attributes", "scope_attributes"]
        }
        _ => &[],
    }
}

/// Whether `physical` is an attribute-container column for `source` -- the
/// legacy map column itself, or one of its five typed-layout columns.
fn is_attribute_container_column(source: &str, physical: &str) -> bool {
    containers(source).iter().any(|base| {
        physical == *base
            || typed_attributes::typed_columns(base)
                .iter()
                .any(|c| c == physical)
    })
}

/// Physical columns that carry no logical meaning today and aren't
/// `physical_only`/`computed` in `schemas.toml` either -- pre-existing gaps
/// this test surfaces rather than papering over. Each is a genuine finding
/// to fix by either declaring the logical field or marking the column
/// `physical_only`, not something this test should silently accept forever.
///
/// - `traces.links`: the span's links list. No logical field exists for it
///   (only `span_events` was modeled, #1280); `get_trace` reads it directly
///   by physical name, bypassing the logical schema entirely.
/// - metrics gauge/sum/histogram columns not yet covered by the (deferred)
///   one-metric-model logical schema: `start_timestamp`, `metric_description`,
///   `metric_unit`, `flags`, `resource_schema_url`, `scope_name`,
///   `scope_version`, `scope_schema_url`, `scope_dropped_attr_count`,
///   `exemplars`, plus sum's `aggregation_temporality`/`is_monotonic` and
///   histogram's `count`/`sum`/`min`/`max`/`bucket_counts`/`explicit_bounds`/
///   `aggregation_temporality`. These carry real meaning; they're just not
///   modeled in the logical schema yet (deferred to the one-metric-model
///   layer per the task), so marking them `physical_only` would misstate
///   that they're computed/partition artifacts.
fn known_gap(source: &str, physical: &str) -> bool {
    let names: &[&str] = match source {
        "traces" => &["links"],
        // Profile scalars beyond `timestamp`/`resource.identity` aren't
        // modeled in the logical schema yet -- see the comment on
        // `alias_table`'s "profiles" arm.
        "profiles" => &[
            "profile_id",
            "duration_nano",
            "sample_type",
            "sample_unit",
            "period_type",
            "period_unit",
            "period",
            "service_name",
            "stacktraces_json",
            "samples_json",
            "trace_id",
            "span_id",
        ],
        "metrics_gauge" | "metrics_sum" => &[
            "start_timestamp",
            "metric_description",
            "metric_unit",
            "flags",
            "resource_schema_url",
            "scope_name",
            "scope_version",
            "scope_schema_url",
            "scope_dropped_attr_count",
            "exemplars",
            "aggregation_temporality",
            "is_monotonic",
        ],
        "metrics_histogram" => &[
            "start_timestamp",
            "metric_description",
            "metric_unit",
            "flags",
            "resource_schema_url",
            "scope_name",
            "scope_version",
            "scope_schema_url",
            "scope_dropped_attr_count",
            "exemplars",
            "aggregation_temporality",
            "count",
            "sum",
            "min",
            "max",
            "bucket_counts",
            "explicit_bounds",
            // `resource.identity` has no `SourcePlan` entry at all for
            // `metrics_histogram` (see `LogicalSchema::core()`'s comment: "not
            // on metrics_histogram, which has no alias table at all") -- the
            // column exists (schemas.toml added it uniformly across every
            // metrics_* table) but isn't wired up as a logical field for this
            // source.
            "resource_identity",
        ],
        _ => &[],
    };
    names.contains(&physical)
}

fn schemas_for(
    source: &str,
) -> (
    &'static HashMap<String, TableSchemaDefinition>,
    &'static str,
) {
    let d = &*SCHEMA_DEFINITIONS;
    let m = &d.metadata;
    match source {
        "logs" => (&d.logs, &m.current_log_version),
        "traces" => (&d.traces, &m.current_trace_version),
        "profiles" => (&d.profiles, &m.current_profile_version),
        "metrics_gauge" => (&d.metrics_gauge, &m.current_metric_version),
        "metrics_sum" => (&d.metrics_sum, &m.current_metric_version),
        "metrics_histogram" => (&d.metrics_histogram, &m.current_metric_version),
        other => panic!("unhandled source {other}"),
    }
}

/// The typed attribute layout's version per source; declared but not yet
/// current until the one-shot cutover makes it so.
fn typed_layout_version(source: &str) -> &'static str {
    match source {
        "traces" => "physical-v5",
        "logs" => "physical-v4",
        _ => "physical-v3",
    }
}

/// The resolved fields of `source`'s current version and of its typed
/// attribute layout version.
fn layouts_of(source: &str) -> [(&'static str, Vec<ResolvedField>); 2] {
    let (schemas, current) = schemas_for(source);
    [current, typed_layout_version(source)].map(|version| {
        let fields = SCHEMA_DEFINITIONS
            .resolve_table_schema(schemas, version)
            .unwrap_or_else(|e| panic!("{source}.{version}: {e}"))
            .fields;
        (version, fields)
    })
}

/// Every physical table whose current version this test checks.
const PHYSICAL_SOURCES: [&str; 6] = [
    "logs",
    "traces",
    "profiles",
    "metrics_gauge",
    "metrics_sum",
    "metrics_histogram",
];

/// The logical source name a physical table resolves fields against.
/// `metrics_gauge`/`metrics_sum` share the `metrics` logical source (see
/// `SourcePlan::for_source("metrics")`, which unions both tables);
/// `metrics_histogram` is its own logical source.
fn logical_source_for(physical_source: &str) -> &'static str {
    match physical_source {
        "metrics_gauge" | "metrics_sum" => "metrics",
        "logs" => "logs",
        "traces" => "traces",
        "profiles" => "profiles",
        "metrics_histogram" => "metrics_histogram",
        other => panic!("unhandled source {other}"),
    }
}

#[test]
fn every_physical_column_realizes_a_logical_field_container_or_is_physical_only() {
    let logical = LogicalSchema::core();

    for physical_source in PHYSICAL_SOURCES {
        let logical_source = logical_source_for(physical_source);
        for (version, fields) in layouts_of(physical_source) {
            for field in fields {
                if field.physical_only
                    || is_attribute_container_column(physical_source, &field.name)
                    || known_gap(physical_source, &field.name)
                {
                    continue;
                }
                let logical_name =
                    expected_alias(physical_source, &field.name).unwrap_or(&field.name);
                assert!(
                    logical.resolve(logical_source, logical_name).is_some(),
                    "{physical_source}.{version}.{}: not a physical_only/computed column, not \
                     an attribute container, and no logical field {logical_source}.{logical_name} \
                     exists -- declare it in LogicalSchema::core() (src/common/src/schema/logical.rs) \
                     or mark it physical_only in schemas.toml",
                    field.name
                );
            }
        }
    }
}

#[test]
fn typed_layout_replaces_each_container_with_its_five_typed_columns() {
    for physical_source in PHYSICAL_SOURCES {
        let [_, (version, fields)] = layouts_of(physical_source);
        let names: Vec<String> = fields.into_iter().map(|f| f.name).collect();
        for container in containers(physical_source) {
            assert!(
                !names.iter().any(|n| n == container),
                "{physical_source}.{version}: legacy container {container} must be removed"
            );
            for column in typed_attributes::typed_columns(container) {
                assert!(
                    names.contains(&column),
                    "{physical_source}.{version}: missing {column}"
                );
            }
        }
    }
}

#[test]
fn every_filterable_non_attribute_logical_field_has_a_physical_realization() {
    use common::schema::logical::{Filterability, LogicalFieldKind};

    let logical = LogicalSchema::core();

    for physical_source in PHYSICAL_SOURCES {
        let logical_source = logical_source_for(physical_source);
        for (version, fields) in layouts_of(physical_source) {
            let physical_names: Vec<String> = fields.into_iter().map(|f| f.name).collect();

            for field in logical.fields() {
                if field.id.source != logical_source {
                    continue;
                }
                if field.kind == LogicalFieldKind::Attribute
                    || field.kind == LogicalFieldKind::SignalDbDefined
                    || field.filterability == Filterability::RetrievalOnly
                {
                    continue;
                }
                let realized = physical_names
                    .iter()
                    .any(|physical| realizes(physical_source, physical, &field.id.name));
                assert!(
                    realized,
                    "{logical_source}.{}: declared in LogicalSchema::core() but no physical \
                 column on {physical_source}.{version} realizes it -- add the column \
                 to schemas.toml or mark the logical field non-native/retrieval-only if it \
                 was never meant to be stored",
                    field.id.name
                );
            }
        }
    }
}
