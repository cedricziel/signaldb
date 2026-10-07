//! # IR → DataFusion planner (single-signal)
//!
//! Lowers a validated [`Document`](common::query_ir::Document) over a single
//! signal (`logs`/`traces`/`profiles`) to a DataFusion `DataFrame`, satisfying the IR's
//! denotational semantics. The DataFrame API is used throughout (as in the
//! LogQL/trace planners), so user-controlled query values never enter a SQL
//! string.
//!
//! ## Correctness properties this planner upholds
//!
//! - **Promotion invariance.** Field resolution goes through a
//!   [`SchemaResolver`] built from the *scanned table's* Arrow schema: a
//!   promoted attribute appears as a physical column and lowers to a column
//!   reference; an unpromoted one lowers to a `get_field` extraction from its
//!   attribute-map container. Same IR, same result.
//! - **Absent-value semantics.** Comparisons lower to DataFusion expressions
//!   whose NULL-in-`WHERE` behaviour coincides with the IR's Kleene semantics:
//!   a row where the field is absent (NULL) satisfies neither `field = x` nor
//!   `not(field = x)`, and only `exists`/`not(exists)` observe absence.
//! - **Deterministic relative time.** Relative anchors resolve once against the
//!   server-stamped clock (`now_ns`) carried in the ticket; every stage sees
//!   the same absolute `[t0, t1]`.
//! - **Curated projection.** A `rows`/`table` result returns only the `fields`
//!   set (or a bounded per-source default) — never `SELECT *`.
//! - **Bounded regex.** A predicate `regex` pattern is compiled behind a size
//!   limit before it is lowered, so a pathological pattern is rejected rather
//!   than executed.

use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, PoisonError};

use common::attrs::expr::typed_compat_attr_expr;
use common::attrs::expr::typed_home_expr;
use common::attrs::expr::typed_home_filter_expr;
use common::flight::{CorrelateReport, CorrelateWindowReport};
use common::profile::{aggregate_profiles_to_diff_flamegraph, aggregate_profiles_to_flamegraph};
use common::query_cursor::{PageReport, PageRequest};
use common::query_ir::{
    Aggregate, BinopOperand, ComparisonOp, Correlate, CorrelateTarget, Document, Extract,
    FieldResolver, Heatmap, HistogramMode, HistogramMoment, JoinKind, Leaf, Literal, Match, Parser,
    Predicate, Resolved, ResultEnvelope, SourceRegistry, SpanListField, Stage, TimestampLiteral,
    ValueType, coerce, parse_duration_ns, safe_ident, validate,
};
use common::query_ir::{PageUnit, page::BODY_HASH};
use common::schema::logical::{AttributeLevel, Filterability, LogicalSchema, LogicalType};
use common::schema::type_authority::CanonicalType;
use common::schema::typed_attributes::{self, has_typed_container, home_column, typed_columns};
use datafusion::arrow::array::{
    Array, AsArray, BooleanArray, LargeStringArray, StringArray, StringBuilder, StringViewArray,
    UInt64Array,
};
use datafusion::arrow::datatypes::Int64Type;
use datafusion::arrow::datatypes::{DataType, Field, IntervalMonthDayNano, Schema, TimeUnit};
use datafusion::catalog::MemTable;
use datafusion::functions::core::expr_fn::{coalesce, named_struct, nullif, with_metadata};
use datafusion::functions::datetime::expr_fn::date_bin;
use datafusion::functions::encoding::expr_fn::encode;
use datafusion::functions::regex::expr_fn::regexp_like;
use datafusion::functions::string::expr_fn::contains;
use datafusion::functions::string::expr_fn::lower;
use datafusion::functions_aggregate::expr_fn::{
    approx_distinct, approx_percentile_cont, avg, count, first_value, last_value, max, min,
    stddev_pop, sum, var_pop,
};
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::SortExpr;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprFunctionExt, ExprSchemable, JoinType, Operator, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility, cast, col, lit, not, try_cast,
};
use datafusion::prelude::{DataFrame, SessionContext, ident};
use datafusion::scalar::ScalarValue;

use super::IrQueryParams;
use super::error::QuerierError;
use super::metric_ops::hist::HistStat;
use super::metric_ops::hist_math::Mode;
use super::metric_ops::hist_plan::{HistEval, histogram_series};
use super::metric_ops::instants::{check_instants, check_positive};
use super::metric_ops::range_math::RangeFn;
use super::metric_ops::range_plan::{RangeEval, range_series};
use super::metric_series;
use super::page_cut::{self, CutLimits};
use super::profile::batch_to_models;
use super::structural_match::{self, MatchLimits};
use super::table_lookup::{optional_table_provider, scan_provider};
use super::typed_attrs::{CanonicalTypeLookup, CanonicalTypes};
use datafusion::common::TableReference;

/// Upper bound on profile rows aggregated into one `flamegraph` result.
/// Matches `QuerierConfig::max_search_limit`'s default — the same cap the
/// Pyroscope render path applies via `ProfileService::fetch_models`.
const FLAMEGRAPH_PROFILE_CAP: usize = 1_000;

/// Per-source planning facts: the physical table, its time column, and the
/// attribute-map containers a `get_field` extraction targets.
///
/// The physical column names here are validated against the canonical persisted
/// Iceberg schema (`common::schema::SCHEMA_DEFINITIONS`) by a unit test — the
/// traces v2 schema renames `name`→`span_name` and `duration_nano`→
/// `duration_nanos`, so those idiosyncratic renames live in `aliases`.
pub(crate) struct SourcePlan {
    /// The logical source name (as written in a document's `from`).
    name: &'static str,
    /// The physical table scanned for this source.
    table: &'static str,
    /// The column carrying the row's primary timestamp.
    time_col: &'static str,
    /// Whether `time_col` is a real `Timestamp` (compare with a timestamp
    /// literal) or an integer nanosecond column (compare with an `i64`).
    time_is_timestamp: bool,
    /// Attribute-map containers, in resolution/coalesce order.
    containers: &'static [&'static str],
    /// Logical prefixes that address one container explicitly, longest-first
    /// at match time. `resource.deployment.environment` reads only the
    /// resource container, where the bare name coalesces across all of them.
    attr_prefixes: &'static [(&'static str, &'static str)],
    /// The default projection for a `rows` result (intersected with the schema).
    row_defaults: &'static [&'static str],
    /// Logical field name → physical column, for OTel-native names and the
    /// schema's idiosyncratic renames that a plain dot→underscore mapping does
    /// not cover.
    aliases: &'static [(&'static str, &'static str)],
}

impl SourcePlan {
    pub(crate) fn for_source(source: &str) -> Option<SourcePlan> {
        match source {
            "logs" => Some(SourcePlan {
                name: "logs",
                table: "logs",
                time_col: "timestamp",
                time_is_timestamp: true,
                containers: &["log_attributes", "scope_attributes", "resource_attributes"],
                attr_prefixes: &[
                    ("log.", "log_attributes"),
                    ("scope.", "scope_attributes"),
                    ("resource.", "resource_attributes"),
                ],
                // The OTel LogRecord: trace context (including the sampled
                // flag), severity as text *and* number, the instrumentation
                // scope's identity, and all three attribute containers kept
                // separate. A client rendering a log line needs every one of
                // these, and merging the containers would erase the scope
                // distinction OTel draws between them.
                row_defaults: &[
                    "timestamp",
                    "observed_timestamp",
                    "body",
                    "service_name",
                    "severity_text",
                    "severity_number",
                    "trace_id",
                    "span_id",
                    "trace_flags",
                    "scope_name",
                    "scope_version",
                    "scope_schema_url",
                    "resource_schema_url",
                    "log_attributes",
                    "scope_attributes",
                    "resource_attributes",
                ],
                aliases: &[
                    ("service.name", "service_name"),
                    ("trace.id", "trace_id"),
                    ("span.id", "span_id"),
                    ("resource.schema_url", "resource_schema_url"),
                    ("scope.name", "scope_name"),
                    ("scope.version", "scope_version"),
                    ("scope.schema_url", "scope_schema_url"),
                    ("log.attributes", "log_attributes"),
                    ("scope.attributes", "scope_attributes"),
                    ("resource.attributes", "resource_attributes"),
                    ("resource.identity", "resource_identity"),
                ],
            }),
            "traces" => Some(SourcePlan {
                name: "traces",
                table: "traces",
                time_col: "start_time_unix_nano",
                time_is_timestamp: false,
                containers: &["span_attributes", "scope_attributes", "resource_attributes"],
                attr_prefixes: &[
                    ("span.", "span_attributes"),
                    ("scope.", "scope_attributes"),
                    ("resource.", "resource_attributes"),
                ],
                row_defaults: &[
                    "trace_id",
                    "span_id",
                    "parent_span_id",
                    "span_name",
                    "service_name",
                    "start_time_unix_nano",
                    "duration_nanos",
                    "status_code",
                ],
                aliases: &[
                    ("service.name", "service_name"),
                    ("trace.id", "trace_id"),
                    ("span.id", "span_id"),
                    ("name", "span_name"),
                    ("span.name", "span_name"),
                    ("duration", "duration_nanos"),
                    ("duration_nano", "duration_nanos"),
                    ("status.code", "status_code"),
                    ("resource.schema_url", "resource_schema_url"),
                    ("scope.name", "scope_name"),
                    ("scope.version", "scope_version"),
                    ("scope.schema_url", "scope_schema_url"),
                    ("span.attributes", "span_attributes"),
                    ("scope.attributes", "scope_attributes"),
                    ("resource.attributes", "resource_attributes"),
                    ("resource.identity", "resource_identity"),
                ],
            }),
            "profiles" => Some(SourcePlan {
                name: "profiles",
                table: "profiles",
                time_col: "timestamp",
                time_is_timestamp: true,
                containers: &[
                    "profile_attributes",
                    "scope_attributes",
                    "resource_attributes",
                ],
                attr_prefixes: &[
                    ("profile.", "profile_attributes"),
                    ("scope.", "scope_attributes"),
                    ("resource.", "resource_attributes"),
                ],
                // Summary rows intentionally omit profile payload columns
                // (`samples_json`/`stacktraces_json`) and attribute containers.
                row_defaults: &[
                    "profile_id",
                    "timestamp",
                    "duration_nano",
                    "sample_type",
                    "sample_unit",
                    "period_type",
                    "period_unit",
                    "period",
                    "service_name",
                    "trace_id",
                    "span_id",
                ],
                aliases: &[
                    ("profile.attributes", "profile_attributes"),
                    ("scope.attributes", "scope_attributes"),
                    ("resource.attributes", "resource_attributes"),
                    ("profile.id", "profile_id"),
                    ("duration", "duration_nano"),
                    ("sample.type", "sample_type"),
                    ("sample.unit", "sample_unit"),
                    ("period.type", "period_type"),
                    ("period.unit", "period_unit"),
                    ("service.name", "service_name"),
                    ("trace.id", "trace_id"),
                    ("span.id", "span_id"),
                    ("resource.identity", "resource_identity"),
                ],
            }),
            "metrics" => Some(SourcePlan {
                name: "metrics",
                table: "metrics",
                time_col: "timestamp",
                time_is_timestamp: true,
                containers: &["attributes", "resource_attributes"],
                // `point.` addresses a data-point attribute whose key a
                // Series label set qualifies (`metric_series::labels`).
                attr_prefixes: &[
                    ("point.", "attributes"),
                    ("resource.", "resource_attributes"),
                ],
                row_defaults: &[
                    "timestamp",
                    "service_name",
                    "metric_name",
                    "metric_type",
                    "value",
                    "attributes",
                    "resource_attributes",
                ],
                aliases: &[
                    ("service.name", "service_name"),
                    ("metric.name", "metric_name"),
                    // "value" is itself the physical column name, which the
                    // resolver rejects as a bare reference (a document must
                    // name a *logical* field, not storage directly, even
                    // when the two spellings coincide) — so it needs a
                    // distinct logical name, same reasoning as traces'
                    // `duration` → `duration_nanos`.
                    ("metric.value", "value"),
                    ("metric.type", "metric_type"),
                    ("metric.temporality", "aggregation_temporality"),
                    ("metric.monotonic", "is_monotonic"),
                    ("metric.count", "count"),
                    ("metric.sum", "sum"),
                    ("metric.min", "min"),
                    ("metric.max", "max"),
                    ("metric.explicit_bounds", "explicit_bounds"),
                    ("metric.bucket_counts", "bucket_counts"),
                    ("metric.quantiles", "quantiles"),
                    ("metric.quantile_values", "quantile_values"),
                    ("series.id", "series_id"),
                    ("resource.identity", "resource_identity"),
                ],
            }),
            "exemplars" => Some(SourcePlan {
                name: "exemplars",
                table: "metric_exemplars",
                time_col: "timestamp",
                time_is_timestamp: true,
                containers: &["filtered_attributes"],
                attr_prefixes: &[],
                row_defaults: &[
                    "timestamp",
                    "service_name",
                    "metric_name",
                    "metric_type",
                    "value",
                    "trace_id",
                    "span_id",
                    "filtered_attributes",
                ],
                aliases: &[
                    ("service.name", "service_name"),
                    ("trace.id", "trace_id"),
                    ("span.id", "span_id"),
                    ("metric.name", "metric_name"),
                    ("metric.type", "metric_type"),
                    ("series.id", "series_id"),
                    ("exemplar.value", "value"),
                    ("exemplar.filtered_attributes", "filtered_attributes"),
                    ("resource.identity", "resource_identity"),
                ],
            }),
            _ => None,
        }
    }
}

fn internal(msg: String) -> QuerierError {
    QuerierError::QueryFailed(datafusion::error::DataFusionError::Internal(msg))
}

/// Scans this source's table. The scan keeps the table's full raw schema —
/// `SchemaResolver`'s promoted-attribute discovery depends on seeing every
/// column the table actually has, not just `row_defaults`.
///
/// A missing `metrics` table reads as an empty one with the canonical
/// schema, so a Series pipeline still answers what PromQL answers from
/// nothing (`sum(x) or vector(0)`, `absent(x)`, `scalar(x)`).
async fn scan_source(
    ctx: &SessionContext,
    tenant_slug: &str,
    dataset_slug: &str,
    source: &SourcePlan,
) -> Result<Option<DataFrame>, QuerierError> {
    let Some((table_ref, provider)) =
        optional_table_provider(ctx, tenant_slug, dataset_slug, source.table).await?
    else {
        if source.table != "metrics" {
            return Ok(None);
        }
        return empty_canonical_scan(ctx, source);
    };
    Ok(Some(scan_provider(ctx, table_ref, provider)?))
}

/// An empty frame with `source`'s canonical table schema, standing in for a
/// table that does not exist yet.
fn empty_canonical_scan(
    ctx: &SessionContext,
    source: &SourcePlan,
) -> Result<Option<DataFrame>, QuerierError> {
    let Some(table) = common::iceberg::schemas::TableSchema::from_table_name(source.table) else {
        return Ok(None);
    };
    let schema = table
        .schema()
        .map_err(|e| internal(format!("{} schema: {e}", source.table)))?;
    let schema: Schema = schema
        .fields()
        .try_into()
        .map_err(|e| internal(format!("{} schema as Arrow: {e:?}", source.table)))?;
    Ok(Some(
        ctx.read_batch(RecordBatch::new_empty(Arc::new(schema)))?,
    ))
}

/// Scan the `traces` table a second time for a `correlate` stage's parent
/// side, under a synthetic table reference distinct from the child scan's —
/// both read the identical provider (same data, same schema), but giving
/// each scan its own [`TableReference`] keeps DataFusion's optimizer from
/// conflating their column provenance while pushing projections through the
/// join. Without this, two scans sharing one qualified name produce a
/// `DuplicateQualifiedField` error once a `Map`-typed attribute container
/// column (e.g. `span_attributes`) is involved — plain scalar columns
/// happen not to trip it, which is why the child side needs no such trick.
///
/// The synthetic reference is safe to build against the shared, long-lived
/// `SessionContext` every `IrService` call reuses: [`scan_provider`] builds
/// a bare `LogicalPlanBuilder::scan` over the reference, never calling
/// `SessionContext::register_table`, so it never mutates the shared
/// context's catalog — concurrent queries (including concurrent
/// `correlate`s) never observe or collide with each other's synthetic name.
async fn scan_parent_traces(
    ctx: &SessionContext,
    tenant_slug: &str,
    dataset_slug: &str,
    source: &SourcePlan,
) -> Result<Option<DataFrame>, QuerierError> {
    let Some((table_ref, provider)) =
        optional_table_provider(ctx, tenant_slug, dataset_slug, source.table).await?
    else {
        return Ok(None);
    };
    let parent_ref = match table_ref {
        TableReference::Bare { table } => {
            TableReference::bare(format!("{table}__correlate_parent"))
        }
        TableReference::Partial { schema, table } => {
            TableReference::partial(schema, format!("{table}__correlate_parent"))
        }
        TableReference::Full {
            catalog,
            schema,
            table,
        } => TableReference::full(catalog, schema, format!("{table}__correlate_parent")),
    };
    Ok(Some(scan_provider(ctx, parent_ref, provider)?))
}

/// Map an Arrow data type to the IR canonical [`ValueType`], or `None` for a
/// container/struct type that is not directly referenceable as a scalar field.
fn arrow_to_value_type(dt: &DataType) -> Option<ValueType> {
    Some(match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => ValueType::Int64,
        DataType::Float16 | DataType::Float32 | DataType::Float64 => ValueType::Float64,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => ValueType::String,
        DataType::Boolean => ValueType::Bool,
        DataType::Timestamp(_, _) => ValueType::TimestampNs,
        DataType::Duration(_) => ValueType::DurationNs,
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => ValueType::Bytes,
        _ => return None,
    })
}

/// Split a container-qualified field into `(container, bare key)`, or
/// `None` for an already-bare name (no known qualifier). Shared by
/// `SchemaResolver::column_for` (only needs the bare key, to materialize a
/// promoted column's name the way the compactor does) and
/// `Lowering::qualified_attr` (additionally needs the container, for the
/// map-extraction fallback).
fn strip_scope_qualifier<'f>(
    attr_prefixes: &'static [(&'static str, &'static str)],
    field: &'f str,
) -> Option<(&'static str, &'f str)> {
    attr_prefixes.iter().find_map(|(prefix, container)| {
        field
            .strip_prefix(prefix)
            // A bare prefix with nothing after it is not a field.
            .filter(|rest| !rest.is_empty())
            .map(|rest| (*container, rest))
    })
}

/// A [`FieldResolver`] whose client-visible built-ins come from the canonical
/// logical schema. The scanned Arrow schema only verifies a logical field's
/// current physical realization and discovers promoted attributes.
pub(crate) struct SchemaResolver {
    columns: HashMap<String, ValueType>,
    /// Key -> `label_<key>` column resolution, exact where the scanned
    /// schema carries each column's origin key (#1533).
    labels: common::schema::MaterializedLabels,
    physical_names: std::collections::HashSet<String>,
    container: String,
    /// Every attribute container this source coalesces over, in resolution
    /// order — see `typed_attribute`'s unqualified branch, which walks these
    /// to pick the first-recorded canonical type.
    containers: &'static [&'static str],
    aliases: &'static [(&'static str, &'static str)],
    /// Logical scope qualifiers (`span.`, `resource.`, ...) this source
    /// recognizes — see `column_for`'s use, which strips one before
    /// materializing a promoted column's name.
    attr_prefixes: &'static [(&'static str, &'static str)],
    source: String,
    logical_schema: LogicalSchema,
    /// The committed canonical attribute types for this scan, when
    /// `plan_document` resolved them (`otel-native-schema` task 4.4) — set
    /// via [`Self::with_typed`]. `None` for every compat lowering and most
    /// tests, which read a typed-layout table (if any) through the legacy
    /// coalesce instead (see [`AttributeTypeRequest`]).
    typed: Option<CanonicalTypes>,
}

impl SchemaResolver {
    pub(crate) fn new(schema: &datafusion::common::DFSchema, source: &SourcePlan) -> Self {
        let mut columns = HashMap::new();
        let mut physical_names = std::collections::HashSet::new();
        for field in schema.fields() {
            physical_names.insert(field.name().to_string());
            if let Some(vt) = arrow_to_value_type(field.data_type()) {
                columns.insert(field.name().to_string(), vt);
            }
        }
        SchemaResolver {
            columns,
            labels: common::schema::MaterializedLabels::from_fields(schema.inner().fields()),
            physical_names,
            container: source.containers[0].to_string(),
            containers: source.containers,
            aliases: source.aliases,
            attr_prefixes: source.attr_prefixes,
            source: source.name.to_string(),
            logical_schema: LogicalSchema::core(),
            typed: None,
        }
    }

    /// Resolve unpromoted attributes against `types`'s committed canonical
    /// homes instead of the legacy JSON-path coalesce (task 4.4). Only ever
    /// set by [`plan_document`] for `IrService::query`'s typed-resolve path,
    /// and only over a table `is_typed_layout` reports as typed — see
    /// [`Self::typed_attribute`].
    pub(crate) fn with_typed(mut self, types: CanonicalTypes) -> Self {
        self.typed = Some(types);
        self
    }

    /// Resolve a declared logical field to its current physical realization:
    /// a physical column addressed directly or via a declared alias (e.g.
    /// `trace_id`, always fully populated, no backfill concern) resolves to
    /// [`Resolved::Column`]; a promoted attribute column (`label_<key>`, may
    /// still be NULL in files the compactor hasn't backfilled since
    /// promotion, #816) resolves to [`Resolved::PromotedColumn`].
    fn column_for(&self, field: &str, value_type: ValueType) -> Option<Resolved> {
        // An alias may target a column with no scalar value type: an
        // attribute-container Map (`Resolved::Column`, same as any other
        // physical column), or — on the typed layout — a container whose
        // five typed columns stand in for it (`Resolved::AttributeBag`).
        if let Some((_, physical)) = self.aliases.iter().find(|(logical, _)| *logical == field) {
            if self.physical_names.contains(*physical) {
                return Some(Resolved::Column {
                    name: physical.to_string(),
                    value_type,
                });
            }
            if has_typed_container(self.physical_names.iter().map(String::as_str), physical) {
                return Some(Resolved::AttributeBag {
                    container: physical.to_string(),
                });
            }
        }
        // A promoted attribute column is materialized from the *bare*
        // attribute key (`attr_promotion::materialized_keys_of` keys off the
        // raw `attr_key` the compactor sees, never a TraceQL-scoped
        // spelling) — strip a `span.`/`resource.`/... qualifier first, the
        // same way `Lowering::qualified_attr` does for the unpromoted
        // extraction path, or a scope-qualified field would never find its
        // promoted column (D10 of `ir-single-lowering`).
        let bare = strip_scope_qualifier(self.attr_prefixes, field).map_or(field, |(_, bare)| bare);
        let materialized = self.labels.column_for(bare);
        // In typed mode, a `label_<key>` column only ever shadows a
        // `String`-canonical attribute — `typed_attribute` makes that call
        // itself, having checked the committed type first; this legacy,
        // type-blind lookup would otherwise promote a non-`String` key too
        // (task 4.4's "a stray legacy label is ignored" rule).
        if self.typed.is_none()
            && let Some(materialized) = materialized
            && let Some(vt) = self.columns.get(materialized)
        {
            // Matched via the materialized-label lookup, not a direct
            // physical alias — #816: this column may still be NULL in files
            // the compactor hasn't backfilled since promotion, so the caller
            // must keep the JSON fallback alive rather than trusting it
            // exclusively.
            return Some(Resolved::PromotedColumn {
                name: materialized.to_string(),
                value_type: vt.clone(),
                key: field.to_string(),
            });
        }
        self.columns.contains_key(field).then(|| Resolved::Column {
            name: field.to_string(),
            value_type,
        })
    }

    /// Whether the logical schema itself declares a type for `field` —
    /// unlike [`FieldResolver::is_known`], this ignores a promoted-but-
    /// undeclared `label_*` column. Used to decide whether an attribute is
    /// "untyped" for the numeric-ordered-comparison rule (`Lowering::
    /// ordered`): a materialized column being present says nothing about
    /// the field's declared type, only that it has been promoted.
    fn has_declared_type(&self, field: &str) -> bool {
        self.logical_schema.resolve(&self.source, field).is_some()
    }

    /// The distinct attribute levels (among this source's containers) that
    /// have a committed canonical type for `key` — the "exactly one level"
    /// test a legacy `label_<key>` column must pass before it's trusted (see
    /// [`Self::promoted_for`]): the compactor backfills `label_<key>`
    /// resource→scope→record while an unqualified read coalesces
    /// record→scope→resource, so a `label_<key>` shared by more than one
    /// level can silently hold a different level's value than the one being
    /// read.
    fn attribute_levels_with_type(
        &self,
        types: &CanonicalTypes,
        key: &str,
    ) -> std::collections::HashSet<AttributeLevel> {
        self.containers
            .iter()
            .map(|c| typed_attributes::container_level(c))
            .filter(|level| types.get(*level, key).is_some())
            .collect()
    }

    /// The promoted column backing one typed home (`container` at `level`,
    /// canonical type `canonical`, attribute `key`), if any: a per-level
    /// `attr_<level>_<key>` column, when it's present in the scanned schema
    /// with the canonical type's own Arrow type — a mismatched-type column
    /// (e.g. left over from a repinned type) is never trusted. Falling that,
    /// a legacy `label_<key>` column stands in for a `String`-canonical home
    /// but only when `single_level` (the key is recorded at exactly one
    /// level) — see [`Self::attribute_levels_with_type`].
    fn promoted_for(
        &self,
        level: AttributeLevel,
        canonical: CanonicalType,
        key: &str,
        single_level: bool,
    ) -> Option<String> {
        let column = common::schema::promoted_attr_column(level, key);
        let expected = logical_to_value_type(canonical.into());
        if self.columns.get(&column) == Some(&expected) {
            return Some(column);
        }
        if canonical != CanonicalType::String || !single_level {
            return None;
        }
        let materialized = self.labels.column_for(key)?;
        self.columns
            .contains_key(materialized)
            .then(|| materialized.to_string())
    }

    /// Resolve an unpromoted attribute against the typed layout's committed
    /// canonical homes (task 4.4), or `None` when there's nothing typed to
    /// resolve against — no `with_typed` types, or `field`'s container isn't
    /// itself on the typed layout — so the caller falls back to the legacy
    /// `JsonPath` coalesce.
    fn typed_attribute(&self, field: &str) -> Option<Resolved> {
        let types = self.typed.as_ref()?;

        if let Some((container, bare)) = strip_scope_qualifier(self.attr_prefixes, field) {
            if !has_typed_container(self.physical_names.iter().map(String::as_str), container) {
                return None;
            }
            let level = typed_attributes::container_level(container);
            return Some(match types.get(level, bare) {
                Some(canonical) => {
                    let single_level = self.attribute_levels_with_type(types, bare).len() == 1;
                    Resolved::TypedAttribute {
                        homes: vec![home_column(container, canonical)],
                        promoted: vec![self.promoted_for(level, canonical, bare, single_level)],
                        key: bare.to_string(),
                        value_type: logical_to_value_type(canonical.into()),
                    }
                }
                None => Resolved::TypedAttribute {
                    homes: Vec::new(),
                    promoted: Vec::new(),
                    key: bare.to_string(),
                    value_type: ValueType::String,
                },
            });
        }

        // Unqualified: T is the canonical type of the first container (in
        // resolution order) with a recorded type; every other container
        // whose recorded type is also T joins the coalesce, in order — a
        // differently-typed container is excluded (reachable via a qualified
        // name or the raw bag), matching the legacy coalesce's own scoping.
        let by_level: Vec<(&'static str, AttributeLevel)> = self
            .containers
            .iter()
            .map(|c| (*c, typed_attributes::container_level(c)))
            .collect();
        let Some(canonical) = by_level
            .iter()
            .find_map(|(_, level)| types.get(*level, field))
        else {
            return Some(Resolved::TypedAttribute {
                homes: Vec::new(),
                promoted: Vec::new(),
                key: field.to_string(),
                value_type: ValueType::String,
            });
        };
        let single_level = self.attribute_levels_with_type(types, field).len() == 1;
        let (homes, promoted): (Vec<String>, Vec<Option<String>>) = by_level
            .iter()
            .filter(|(_, level)| types.get(*level, field) == Some(canonical))
            .map(|(container, level)| {
                (
                    home_column(container, canonical),
                    self.promoted_for(*level, canonical, field, single_level),
                )
            })
            .unzip();
        Some(Resolved::TypedAttribute {
            homes,
            promoted,
            key: field.to_string(),
            value_type: logical_to_value_type(canonical.into()),
        })
    }
}

/// Exception attributes per the OTel exception semantic conventions
/// (https://opentelemetry.io/docs/specs/semconv/exceptions/exceptions-spans/):
/// captured on the span event named `exception`, not as ordinary span
/// attributes. Logs need no such special-casing — the same attribute names
/// on a LogRecord (exceptions-logs.md) are already ordinary record
/// attributes, resolved by the generic fallback below.
const EXCEPTION_EVENT_ATTRIBUTES: [&str; 4] = [
    "exception.type",
    "exception.message",
    "exception.stacktrace",
    "exception.escaped",
];

impl FieldResolver for SchemaResolver {
    fn resolve(&self, _source: &str, field: &str) -> Option<Resolved> {
        if self.source == "traces"
            && EXCEPTION_EVENT_ATTRIBUTES.contains(&field)
            && self.physical_names.contains("events")
        {
            return Some(Resolved::EventAttribute {
                events_column: "events".to_string(),
                event_name: "exception".to_string(),
                key: field.to_string(),
                value_type: ValueType::String,
            });
        }
        if self.source == "traces"
            && field == "span_events"
            && self.physical_names.contains("events")
        {
            return Some(Resolved::SpanEvents {
                events_column: "events".to_string(),
            });
        }
        if self.source == "traces" && field == "span_links" && self.physical_names.contains("links")
        {
            return Some(Resolved::SpanLinks {
                links_column: "links".to_string(),
            });
        }
        if self.source == "traces"
            && let Some(f) = SpanListField::parse(field)
            && self.physical_names.contains(f.column())
        {
            return Some(Resolved::SpanList(f));
        }
        if let Some(logical) = self.logical_schema.resolve(&self.source, field) {
            let value_type = logical_to_value_type(logical.value_type);
            return match self.column_for(field, value_type.clone()) {
                Some(resolved) => Some(resolved),
                None => self.typed_attribute(field).or(Some(Resolved::JsonPath {
                    container: self.container.clone(),
                    key: field.to_string(),
                    value_type,
                })),
            };
        }
        if self.physical_names.contains(field) {
            return None;
        }
        match self.column_for(field, ValueType::String) {
            Some(resolved) => Some(resolved),
            // An unpromoted attribute: a typed-home read when this table's
            // types are known, else a String extraction from the container.
            None => self.typed_attribute(field).or(Some(Resolved::JsonPath {
                container: self.container.clone(),
                key: field.to_string(),
                value_type: ValueType::String,
            })),
        }
    }

    fn is_known(&self, _source: &str, field: &str) -> bool {
        // Only physical / promoted columns are "known" — the permissive String
        // attribute fallback must not spuriously collide with derived/output
        // names. (Without #811 the resolver cannot enumerate real attributes.)
        self.logical_schema.resolve(&self.source, field).is_some()
            || self
                .labels
                .column_for(field)
                .is_some_and(|c| self.columns.contains_key(c))
    }

    fn is_physical_name(&self, _source: &str, field: &str) -> bool {
        self.physical_names.contains(field)
            && self.logical_schema.resolve(&self.source, field).is_none()
    }

    /// Maps the logical schema's filterability vocabulary onto the IR's yes/no
    /// question. The IR deliberately does not know this enum — it asks whether
    /// a field may be addressed, and the schema layer decides what that means.
    fn is_filterable(&self, _source: &str, field: &str) -> bool {
        self.logical_schema
            .resolve(&self.source, field)
            .is_none_or(|logical| logical.filterability != Filterability::RetrievalOnly)
    }
}

fn logical_to_value_type(value_type: LogicalType) -> ValueType {
    match value_type {
        LogicalType::String | LogicalType::AnyValue => ValueType::String,
        LogicalType::Bool => ValueType::Bool,
        LogicalType::Int64 => ValueType::Int64,
        LogicalType::Float64 => ValueType::Float64,
        LogicalType::TimestampNs => ValueType::TimestampNs,
        LogicalType::DurationNs => ValueType::DurationNs,
        LogicalType::Bytes => ValueType::Bytes,
    }
}

/// The IR query service. Mirrors the other single-signal services: constructed
/// with a shared [`SessionContext`], one method per ticket.
#[derive(Clone)]
pub struct IrService {
    session_context: Arc<SessionContext>,
    /// Row cap on a `correlate` stage's joined output
    /// (`[querier].correlate_max_rows`, default [`DEFAULT_CORRELATE_MAX_ROWS`]).
    /// Set via [`Self::with_correlate_max_rows`]; the production Flight
    /// service sets it from config, every other caller (compat lowerings,
    /// tests) keeps the default — none of those can ever reach a `correlate`
    /// stage, so the value is moot for them.
    correlate_max_rows: usize,
    /// Source-row cap of a signal-target `correlate`
    /// (`[querier].correlate_max_source_rows`).
    correlate_max_source_rows: usize,
    /// `[querier].match_max_trace_spans` / `match_max_trace_bytes`.
    match_limits: MatchLimits,
    /// Node cap on a `graph` result (`[querier].graph_max_nodes`).
    graph_max_nodes: usize,
    /// `[querier].page_max_tie_rows` / `page_max_bytes`.
    page_max_tie_rows: usize,
    page_max_bytes: usize,
    /// Fetches committed canonical attribute types for a typed-layout table
    /// (`otel-native-schema` task 4.4). Set via [`Self::with_canonical_types`]
    /// by the production Flight service; `None` in every other caller
    /// (compat lowerings, most tests), which never reach a typed table.
    canonical_type_lookup: Option<Arc<dyn CanonicalTypeLookup>>,
}

/// The resolved absolute time window `[t0, t1]` (unix epoch nanoseconds),
/// carried through the plan and echoed back to the caller for reproducibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedWindow {
    pub start_ns: i64,
    pub end_ns: i64,
}

impl IrService {
    pub fn new(session_context: SessionContext) -> Self {
        Self {
            session_context: Arc::new(session_context),
            correlate_max_rows: DEFAULT_CORRELATE_MAX_ROWS,
            correlate_max_source_rows: DEFAULT_CORRELATE_MAX_SOURCE_ROWS,
            match_limits: MatchLimits::default(),
            graph_max_nodes: common::config::QuerierConfig::default().graph_max_nodes,
            page_max_tie_rows: common::config::QuerierConfig::default().page_max_tie_rows,
            page_max_bytes: common::config::QuerierConfig::default().page_max_bytes,
            canonical_type_lookup: None,
        }
    }

    /// Attach the canonical-type-authority lookup used by `query`'s
    /// `POST /api/v1/query` path to resolve a typed-layout source's
    /// attribute types before planning.
    pub fn with_canonical_types(mut self, lookup: Arc<dyn CanonicalTypeLookup>) -> Self {
        self.canonical_type_lookup = Some(lookup);
        self
    }

    /// Override the `graph` node cap, from `[querier].graph_max_nodes`.
    pub fn with_graph_max_nodes(mut self, graph_max_nodes: usize) -> Self {
        self.graph_max_nodes = graph_max_nodes;
        self
    }

    /// Override the `correlate` row cap (default [`DEFAULT_CORRELATE_MAX_ROWS`]),
    /// from `[querier].correlate_max_rows`.
    pub fn with_correlate_max_rows(mut self, correlate_max_rows: usize) -> Self {
        self.correlate_max_rows = correlate_max_rows;
        self
    }

    /// Override the signal-target `correlate` source-row cap, from
    /// `[querier].correlate_max_source_rows`.
    pub fn with_correlate_max_source_rows(mut self, correlate_max_source_rows: usize) -> Self {
        self.correlate_max_source_rows = correlate_max_source_rows;
        self
    }

    /// Override the page bounds, from `[querier].page_max_tie_rows` /
    /// `page_max_bytes`.
    pub fn with_page_limits(mut self, max_tie_rows: usize, max_bytes: usize) -> Self {
        self.page_max_tie_rows = max_tie_rows;
        self.page_max_bytes = max_bytes;
        self
    }

    /// Override the `match` stage's per-trace bounds, from
    /// `[querier].match_max_trace_spans` / `match_max_trace_bytes`.
    pub fn with_match_limits(mut self, max_spans: usize, max_bytes: usize) -> Self {
        self.match_limits = MatchLimits {
            max_spans,
            max_bytes,
        };
        self
    }

    /// Executes an IR query ticket, returning the projected RecordBatches,
    /// the resolved window, and a [`CorrelateReport`] of what a `correlate`
    /// stage's join did. The row-limit flag is only known once `.collect()`
    /// below has actually run the stream to completion — it is an
    /// [`AtomicBool`] flipped by `CorrelateCapExec` (`correlate_cap`) as it
    /// streams, not something plan-time can predict.
    pub async fn query(
        &self,
        params: &IrQueryParams,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<(Vec<RecordBatch>, ResolvedWindow, CorrelateReport), QuerierError> {
        use tracing::Instrument;

        let doc: Document = serde_json::from_value(params.document.clone())
            .map_err(|e| QuerierError::InvalidInput(format!("invalid IR document: {e}")))?;
        if doc.result == ResultEnvelope::Graph {
            return self
                .query_graph(&doc, params.now_ns, tenant_slug, dataset_slug)
                .await;
        }
        if doc.result == ResultEnvelope::Flamegraph {
            return self
                .query_flamegraph(&doc, params.now_ns, tenant_slug, dataset_slug)
                .await;
        }
        // Stage spans (INTERNAL) under the Flight SERVER span, so a slow
        // query is attributable to planning vs execution.
        let Some((df, window, outcome)) = self
            .plan_with_correlate_truncation(
                &doc,
                tenant_slug,
                dataset_slug,
                params.now_ns,
                AttributeTypeRequest::Resolve(self.canonical_type_lookup.clone()),
                params.page.as_ref().map(|p| (p, p.size as usize + 1)),
            )
            .instrument(tracing::info_span!("signaldb.query.plan"))
            .await?
        else {
            // No storage for this source in this dataset: no rows, but the
            // window is still resolved so the caller can echo it back.
            return Ok((
                Vec::new(),
                resolve_window(&doc, params.now_ns)?,
                CorrelateReport {
                    page: params.page.as_ref().map(|_| PageReport::default()),
                    ..CorrelateReport::default()
                },
            ));
        };
        let exec_span = tracing::info_span!(
            "signaldb.query.execute",
            signaldb.query.rows = tracing::field::Empty,
            signaldb.query.batches = tracing::field::Empty,
        );
        let batches = df
            .collect()
            .instrument(exec_span.clone())
            .await
            .map_err(QuerierError::from)?;
        // A page's sort fetches one row past `size`. When that row extends the
        // tie group at the boundary, the group is read again in full, bounded
        // by the tie limit, rather than over-fetching every page by it.
        let crossing = match &params.page {
            Some(page)
                if page.unit == PageUnit::Rows
                    && page.ceiling.is_none_or(|c| c > page.size)
                    && !page.tail.is_some_and(|t| t.newest) =>
            {
                page_cut::crossing_group(&batches, &page.order, page.size as usize)?
                    .map(|crossing| (page, crossing))
            }
            _ => None,
        };
        let batches = match crossing {
            Some((page, crossing)) => {
                let retry = PageRequest {
                    after: crossing.after.or_else(|| page.after.clone()),
                    ..page.clone()
                };
                let fetch = self.page_max_tie_rows.saturating_add(2);
                let group = match self
                    .plan_with_correlate_truncation(
                        &doc,
                        tenant_slug,
                        dataset_slug,
                        params.now_ns,
                        AttributeTypeRequest::Resolve(self.canonical_type_lookup.clone()),
                        Some((&retry, fetch)),
                    )
                    .await?
                {
                    Some((df, ..)) => df
                        .collect()
                        .instrument(exec_span.clone())
                        .await
                        .map_err(QuerierError::from)?,
                    None => Vec::new(),
                };
                [crossing.head, group].concat()
            }
            None => batches,
        };
        let (batches, page) = match &params.page {
            Some(page) => {
                let newest = page.tail.is_some_and(|t| t.newest);
                let limits = CutLimits {
                    size: page.size as usize,
                    unit: page.unit,
                    exact: newest,
                    ceiling: page.ceiling.map(|c| c as usize),
                    reverse: newest,
                    // A trace page bounds each trace's spans as `match` does.
                    max_tie_rows: match page.unit {
                        PageUnit::Rows => self.page_max_tie_rows,
                        PageUnit::Traces => self.match_limits.max_spans,
                    },
                    max_bytes: self.page_max_bytes,
                };
                let (batches, report) = page_cut::cut_page(&batches, &page.order, limits)?;
                (batches, Some(report))
            }
            None => (batches, None),
        };
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        exec_span.record("signaldb.query.rows", rows as i64);
        exec_span.record("signaldb.query.batches", batches.len() as i64);
        let report = CorrelateReport {
            row_limit: outcome
                .truncated
                .is_some_and(|flag| flag.load(AtomicOrdering::Relaxed)),
            fanout_limit: outcome
                .fanout_limit
                .is_some_and(|flag| flag.load(AtomicOrdering::Relaxed)),
            window: outcome.window.map(|w| CorrelateWindowReport {
                start_ns: w.start_ns,
                end_ns: w.end_ns,
            }),
            match_incomplete: outcome.match_incomplete.and_then(|m| m.report()),
            page,
        };
        Ok((batches, window, report))
    }

    /// A `flamegraph` document. The aggregation happens in Rust, not
    /// DataFusion (see `apply_projection`'s flamegraph carve-out). With a
    /// `baseline` the same pipeline is read over both windows, one after the
    /// other so a query never holds two scans at once, and merged into one
    /// differential flamegraph; the reported window is `range`'s.
    async fn query_flamegraph(
        &self,
        doc: &Document,
        now_ns: i64,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<(Vec<RecordBatch>, ResolvedWindow, CorrelateReport), QuerierError> {
        let cap = FLAMEGRAPH_PROFILE_CAP;
        let (comparison, window) = self
            .flamegraph_rows(doc, now_ns, tenant_slug, dataset_slug, cap)
            .await?;
        let batch = match &doc.baseline {
            None => encode_flamegraph_batch(&comparison, cap)?,
            Some(baseline) => {
                resolve_range("baseline", baseline, now_ns)?;
                let baseline_doc = Document {
                    range: baseline.clone(),
                    baseline: None,
                    ..doc.clone()
                };
                let (baseline, _) = self
                    .flamegraph_rows(&baseline_doc, now_ns, tenant_slug, dataset_slug, cap)
                    .await?;
                encode_diff_flamegraph_batch(&baseline, &comparison, cap)?
            }
        };
        Ok((vec![batch], window, CorrelateReport::default()))
    }

    /// The newest profile rows one `flamegraph` window matches, one row past
    /// `cap` so truncation is exact.
    async fn flamegraph_rows(
        &self,
        doc: &Document,
        now_ns: i64,
        tenant_slug: &str,
        dataset_slug: &str,
        cap: usize,
    ) -> Result<(Vec<RecordBatch>, ResolvedWindow), QuerierError> {
        use tracing::Instrument;

        let Some((df, window, _)) = self
            .plan_with_correlate_truncation(
                doc,
                tenant_slug,
                dataset_slug,
                now_ns,
                AttributeTypeRequest::Resolve(self.canonical_type_lookup.clone()),
                None,
            )
            .instrument(tracing::info_span!("signaldb.query.plan"))
            .await?
        else {
            return Ok((Vec::new(), resolve_window(doc, now_ns)?));
        };
        let batches = df
            .sort(vec![ident("timestamp").sort(false, true)])
            .and_then(|df| df.limit(0, Some(cap + 1)))
            .map_err(QuerierError::QueryFailed)?
            .collect()
            .instrument(tracing::info_span!("signaldb.query.execute"))
            .await
            .map_err(QuerierError::from)?;
        Ok((batches, window))
    }

    /// A `graph` document: assembled from fixed internal pipelines (see
    /// `super::graph`) and shipped as one `graph_json` cell.
    async fn query_graph(
        &self,
        doc: &Document,
        now_ns: i64,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<(Vec<RecordBatch>, ResolvedWindow, CorrelateReport), QuerierError> {
        use tracing::Instrument;

        let limits = super::graph::GraphLimits {
            correlate_max_rows: self.correlate_max_rows,
            max_nodes: self.graph_max_nodes,
        };
        let built = super::graph::build_graph(
            &self.session_context,
            doc,
            tenant_slug,
            dataset_slug,
            now_ns,
            limits,
        )
        .instrument(tracing::info_span!("signaldb.query.graph"))
        .await?;
        let (graph, window, truncated) = match built {
            Some(built) => built,
            None => (
                common::service_graph::ServiceGraph::default(),
                resolve_window(doc, now_ns)?,
                false,
            ),
        };
        let report = CorrelateReport {
            row_limit: truncated,
            ..Default::default()
        };
        Ok((
            vec![super::graph::encode_graph_batch(&graph)?],
            window,
            report,
        ))
    }

    /// Build the `DataFrame` for a document (split out for planner tests).
    ///
    /// Delegates to [`plan_document`], the planner's single entry point —
    /// `IrService`'s Flight-ticket path and any other caller (the compat
    /// lowerings, once they route through this planner) build the same plan
    /// the same way. `IrService::query` itself now calls
    /// [`Self::plan_with_correlate_truncation`] directly instead, so this
    /// two-tuple form is exercised by the planner's own tests only —
    /// `#[cfg_attr(not(test), allow(dead_code))]` says exactly that, rather
    /// than a blanket allow.
    ///
    /// A signal-target `correlate` executes its source pipeline while
    /// planning (see [`plan_document`]).
    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn plan(
        &self,
        doc: &Document,
        tenant_slug: &str,
        dataset_slug: &str,
        now_ns: i64,
    ) -> Result<Option<(DataFrame, ResolvedWindow)>, QuerierError> {
        Ok(self
            .plan_with_correlate_truncation(
                doc,
                tenant_slug,
                dataset_slug,
                now_ns,
                AttributeTypeRequest::CompatOnly,
                None,
            )
            .await?
            .map(|(df, window, _outcome)| (df, window)))
    }

    /// Like [`Self::plan`], but also reports whether a `correlate` stage's
    /// join was truncated by `correlate_max_rows` — ground truth captured
    /// at the join itself, needed by [`Self::query`] to stamp the final
    /// result's Arrow schema metadata for the router. Kept private to
    /// [`Self::plan`]'s public two-tuple shape: the dozens of existing
    /// planner-only tests and compat lowerings that call `plan` never
    /// reach a `correlate` stage, so they don't need to spell out a third
    /// element they'd only discard.
    async fn plan_with_correlate_truncation(
        &self,
        doc: &Document,
        tenant_slug: &str,
        dataset_slug: &str,
        now_ns: i64,
        attribute_type_request: AttributeTypeRequest,
        page: Option<(&PageRequest, usize)>,
    ) -> Result<Option<(DataFrame, ResolvedWindow, CorrelateOutcome)>, QuerierError> {
        plan_document(
            &self.session_context,
            doc,
            PlanRequest::new(tenant_slug, dataset_slug, now_ns)
                .with_correlate_max_rows(self.correlate_max_rows)
                .with_correlate_max_source_rows(self.correlate_max_source_rows)
                .with_match_limits(self.match_limits)
                .with_attribute_type_request(attribute_type_request)
                .with_page(page),
        )
        .await
    }
}

/// A pseudo-source below `irVersion` 10 is invalid (400); it has no table to
/// scan, so this is checked before the scan.
fn reject_pseudo_source(doc: &Document) -> Result<(), QuerierError> {
    if common::query_ir::is_pseudo_source(&doc.from) && doc.ir_version < 10 {
        return Err(QuerierError::InvalidInput(format!(
            "the {} source requires irVersion 10 (document declares {})",
            doc.from, doc.ir_version
        )));
    }
    Ok(())
}

fn unsupported_stage(stage: &Stage) -> QuerierError {
    QuerierError::Unsupported(format!("{} stage is not supported yet", stage.name()))
}

/// Lower a validated [`Document`] to a `DataFrame`, over the given session
/// context and tenant/dataset scope. The planner's one entry point (D1 of
/// `ir-single-lowering`): [`IrService::plan`] calls this, and so will every
/// compat lowering that adopts the IR, so there is one planner rather than a
/// planner and a compat-planner.
///
/// The [`SchemaResolver`] is deliberately not a parameter — see D1's note:
/// it is built here from the *scanned table's* Arrow schema, which is what
/// keeps field resolution promotion-invariant (a promoted attribute is
/// discovered from the schema DataFusion actually returned, not from a
/// resolver a caller could hand in stale or wrong) and is exactly the
/// coupling `pub(crate)`-only visibility (task 1.2) exists to prevent a
/// caller from second-guessing.
///
/// Not pure planning: a signal-target `correlate` executes its source
/// pipeline here (bounded by `correlate_max_source_rows`) to learn the key
/// set and time envelope the target scan is built from.
pub(crate) async fn plan_document(
    ctx: &SessionContext,
    doc: &Document,
    request: PlanRequest<'_>,
) -> Result<Option<(DataFrame, ResolvedWindow, CorrelateOutcome)>, QuerierError> {
    if let Ok(window) = resolve_window(doc, request.now_ns) {
        metric_series::check_document_steps(doc, window)?;
    }
    plan_operand(ctx, doc, request).await
}

/// [`plan_document`] without the document-level step limit, for a `binop`
/// operand, whose range a subquery may have widened.
async fn plan_operand(
    ctx: &SessionContext,
    doc: &Document,
    request: PlanRequest<'_>,
) -> Result<Option<(DataFrame, ResolvedWindow, CorrelateOutcome)>, QuerierError> {
    let operand_request = request.clone();
    let PlanRequest {
        tenant_slug,
        dataset_slug,
        now_ns,
        correlate_max_rows,
        correlate_max_source_rows,
        match_limits,
        attribute_type_request,
        page,
    } = request;
    reject_pseudo_source(doc)?;
    // Before the missing-table shortcut below skips `validate`.
    common::query_ir::check_structure(doc)
        .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
    if common::query_ir::is_pseudo_source(&doc.from) {
        let window = resolve_window(doc, now_ns)?;
        let df = plan_pseudo_document(ctx, doc, window, &operand_request).await?;
        return Ok(Some((df, window, CorrelateOutcome::default())));
    }
    let source = SourcePlan::for_source(&doc.from)
        .ok_or_else(|| QuerierError::InvalidInput(format!("unknown source '{}'", doc.from)))?;

    // A dataset with none of this source's tables (but `metrics`) has no
    // rows to plan over. The document's schema-dependent validation is
    // skipped along with the scan — there is no schema to validate against.
    let Some(base) = scan_source(ctx, tenant_slug, dataset_slug, &source).await? else {
        return Ok(None);
    };

    // Build the resolver from the actual scanned schema and validate the
    // document against it (envelope, coercibility, references, guards).
    let resolver = schema_resolver(
        base.schema(),
        &source,
        &attribute_type_request,
        tenant_slug,
        dataset_slug,
    )
    .await?;
    let target = match signal_target(doc).and_then(SourcePlan::for_source) {
        Some(plan) => Some(
            CorrelateTargetSide::scan(
                ctx,
                plan,
                &attribute_type_request,
                tenant_slug,
                dataset_slug,
            )
            .await?,
        ),
        None => None,
    };
    let resolvers = SourceResolvers {
        from: &resolver,
        target: target.as_ref().map(|t| &t.resolver),
    };
    validate(doc, &SourceRegistry::core(), &resolvers)
        .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;

    // Resolve the time window once against the injected clock.
    let window = resolve_window(doc, now_ns)?;
    validate_heatmap_window(doc, &window)?;

    let mut lowering = Lowering {
        source: &source,
        resolver: &resolver,
        now_ns,
        aggregated: false,
        series_shaped: false,
        demand: common::discovery::signal_for_source(source.name)
            .map(|signal| AttrDemandScope::new(tenant_slug, dataset_slug, signal)),
        col_of: HashMap::new(),
        derived_types: HashMap::new(),
        schema_cols: base
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect(),
        scope: None,
        correlate_truncated: None,
        correlate_fanout: None,
        correlate_window: None,
    };

    let doc_step = doc.step.as_deref();
    let windows = metric_series::stage_windows(&doc.pipeline, window, None, doc_step);
    let sample_window = doc
        .pipeline
        .iter()
        .position(|stage| matches!(stage, Stage::Sample(_)))
        .map_or(window, |i| windows[i]);
    // An operator at the first instant reads the window before it.
    let mut lookback = 0;
    for stage in &doc.pipeline {
        match stage {
            _ if let Some(h) = HistStage::of(stage) => {
                lookback = lookback.max(histogram_step_window(&h)?.1);
            }
            Stage::Aggregate(agg) if let Some(a) = range_agg(agg) => {
                lookback = lookback.max(range_step_window(agg, a, &window)?.1);
            }
            Stage::Aggregate(agg) if let Some(step) = instant_grid_step(agg, &source) => {
                lookback = lookback.max(parse_step(step)?);
            }
            _ => {}
        }
    }
    let scan = metric_series::sample::scan_window(doc, sample_window, now_ns)?;
    // A subquery widens the window of the stages before it.
    let earliest = windows
        .iter()
        .map(|w| w.start_ns)
        .fold(window.start_ns, i64::min);
    let scan = ResolvedWindow {
        start_ns: scan.start_ns.min(earliest.saturating_sub(lookback)),
        ..scan
    };
    let mut df = lowering.apply_time_window(base, &scan)?;
    let mut metric_frame = false;
    let mut series_step = None;
    let mut match_incomplete = None;
    // A paged walk caps a trailing `limit` across pages itself.
    let pipeline = match (page, doc.pipeline.split_last()) {
        (Some(_), Some((Stage::Limit(_), rest))) => rest,
        _ => &doc.pipeline[..],
    };
    for (i, (stage, &stage_window)) in pipeline.iter().zip(&windows).enumerate() {
        // A limit keeps the first rows, so it needs the frame's final order.
        if metric_frame && matches!(stage, Stage::Limit(_)) {
            df = metric_series::sort_frame(df, None)?;
        }
        df = match stage {
            Stage::Sample(sample) => {
                lowering.series_shaped = true;
                metric_frame = true;
                let env = metric_series::sample::SampleEnv {
                    window: stage_window,
                    doc_step,
                    now_ns,
                    schema_cols: &lowering.schema_cols,
                };
                metric_series::sample::lower_sample(df, sample, &env)?
            }
            Stage::Scalar(_)
            | Stage::Vector(_)
            | Stage::Reduce(_)
            | Stage::Labels(_)
            | Stage::Map(_)
            | Stage::Filter(_)
            | Stage::Sort(_)
            | Stage::Absent(_)
            | Stage::OverTime(_)
            | Stage::Binop(_) => {
                let env = metric_series::FrameEnv {
                    ctx,
                    window: stage_window,
                    step_ns: series_step,
                    doc_step,
                };
                lower_frame_stage(df, stage, &env, doc, &operand_request).await?
            }
            // Needs its stage's window for its evaluation instants.
            _ if let Some(h) = HistStage::of(stage) => {
                let df = lowering.lower_histogram(df, &h, &stage_window)?;
                // Series stages after it read it as a Series.
                if h.per_series {
                    metric_frame = true;
                    df
                } else if doc.result == ResultEnvelope::Series
                    || doc.pipeline[i + 1..]
                        .iter()
                        .any(metric_series::is_frame_stage)
                {
                    metric_frame = true;
                    metric_series::histogram_as_series(df, h.by, h.as_name)?
                } else {
                    df
                }
            }
            Stage::Aggregate(agg) if let Some(a) = range_agg(agg) => {
                lowering.lower_rate_aggregate(df, agg, a, &window)?
            }
            Stage::Aggregate(agg) if instant_grid_step(agg, &source).is_some() => {
                lowering.lower_aggregate(df, agg, Some(&window))?
            }
            // Needs its own scan of the target table (the parent span side
            // or another signal) and the resolved window, only available here.
            Stage::Correlate(correlate) => {
                let scan = CorrelateScan {
                    tenant_slug,
                    dataset_slug,
                    window: &window,
                    correlate_max_rows,
                    correlate_max_source_rows,
                };
                match (&correlate.to, &target) {
                    (CorrelateTarget::Signal(_), Some(target)) => {
                        lowering
                            .lower_signal_correlate(ctx, df, correlate, target, scan)
                            .await?
                    }
                    _ => lowering.lower_correlate(ctx, df, correlate, scan).await?,
                }
            }
            Stage::Match(stage) => {
                let (df, incomplete) = lowering.lower_match(df, stage, &scan, match_limits)?;
                match_incomplete = Some(incomplete);
                df
            }
            other => lowering.lower_stage(df, other)?,
        };
        series_step = metric_series::output_step(stage, series_step, doc_step);
    }
    if metric_frame {
        df = metric_series::sort_frame(df, metric_series::terminal_order(doc, window))?;
    }
    let mut page_keys = Vec::new();
    if let Some((page, fetch)) = page {
        for (i, key) in page.order.iter().enumerate() {
            let value = if key.field == BODY_HASH {
                page_cut::body_hash(lowering.value_expr("body")?)
            } else {
                lowering.record_field_demand(&key.field);
                lowering.value_expr(&key.field)?
            };
            let name = page_cut::key_column(i);
            df = df
                .with_column(&name, value)
                .map_err(QuerierError::QueryFailed)?;
            page_keys.push(name);
        }
        df = page_cut::bound_to_page(df, page, fetch)?;
    }
    df = lowering.apply_projection(df, doc, &page_keys)?;
    let outcome = CorrelateOutcome {
        truncated: lowering.correlate_truncated,
        fanout_limit: lowering.correlate_fanout,
        window: lowering.correlate_window,
        match_incomplete,
    };
    Ok(Some((df, window, outcome)))
}

/// Plan a document over the `time`/`constant` pseudo-source: its Scalar,
/// then its Series/Scalar stages.
async fn plan_pseudo_document(
    ctx: &SessionContext,
    doc: &Document,
    window: ResolvedWindow,
    request: &PlanRequest<'_>,
) -> Result<DataFrame, QuerierError> {
    let (mut df, step_ns) = metric_series::scalar::pseudo_source_frame(ctx, doc, window)?;
    let doc_step = doc.step.as_deref();
    let windows = metric_series::stage_windows(&doc.pipeline, window, Some(step_ns), doc_step);
    let mut step = Some(step_ns);
    for (stage, &stage_window) in doc.pipeline.iter().zip(&windows) {
        let env = metric_series::FrameEnv {
            ctx,
            window: stage_window,
            step_ns: step,
            doc_step,
        };
        df = lower_frame_stage(df, stage, &env, doc, request).await?;
        step = metric_series::output_step(stage, step, doc_step);
    }
    metric_series::sort_frame(df, metric_series::terminal_order(doc, window))
}

/// Lower one Series/Scalar stage; a `binop` with a sub-document first plans
/// that document over the stage's window and matches the two frames.
async fn lower_frame_stage(
    df: DataFrame,
    stage: &Stage,
    env: &metric_series::FrameEnv<'_>,
    doc: &Document,
    request: &PlanRequest<'_>,
) -> Result<DataFrame, QuerierError> {
    let Stage::Binop(binop) = stage else {
        return metric_series::lower_stage(df, stage, env);
    };
    let BinopOperand::Document(sub) = &binop.right else {
        return metric_series::lower_stage(df, stage, env);
    };
    let Some(step_ns) = env.step_ns else {
        return Err(QuerierError::Unsupported(
            "binop over a non-sampled Series".to_string(),
        ));
    };
    let sub_step = metric_series::pipeline_step(&sub.from, &sub.pipeline, doc.step.as_deref());
    if let Some(sub_step) = sub_step
        && sub_step != step_ns
    {
        return Err(QuerierError::InvalidInput(format!(
            "binop operand evaluates every {sub_step}ns but its input every {step_ns}ns; \
             give both the same step"
        )));
    }
    let child = Document {
        ir_version: doc.ir_version,
        from: sub.from.clone(),
        range: common::query_ir::Range {
            from: env.window.start_ns.into(),
            to: env.window.end_ns.into(),
        },
        result: if metric_series::yields_scalar(&sub.from, &sub.pipeline) {
            ResultEnvelope::Scalar
        } else {
            ResultEnvelope::Series
        },
        fields: None,
        pipeline: sub.pipeline.clone(),
        focus: None,
        depth: None,
        trace_id: None,
        baseline: None,
        step: doc.step.clone(),
        constant: sub.constant,
        page: None,
        tail: None,
    };
    let right = match Box::pin(plan_operand(env.ctx, &child, request.clone())).await? {
        Some((right, _, _)) => metric_series::operand(right),
        None => metric_series::empty_series(env.ctx)?,
    };
    metric_series::vector_match::vector_match(metric_series::operand(df), right, binop)
}

/// Decode full-payload profile rows, aggregate them into one flamegraph, and
/// encode the result as a single-row RecordBatch (`flamegraph_json: Utf8`,
/// `truncated: Boolean`) so it can cross the Flight wire like every other
/// envelope — the router decodes it back into the HTTP response shape.
///
/// Aggregation itself runs in Rust (`aggregate_profiles_to_flamegraph`), not
/// DataFusion — see `apply_projection`'s flamegraph carve-out — so this is
/// the terminal step for a `flamegraph`-enveloped pipeline, mirroring how
/// `ProfileService::flamegraph_with_tenant` serves the Pyroscope render path
/// over the same profile rows.
fn encode_flamegraph_batch(
    batches: &[RecordBatch],
    cap: usize,
) -> Result<RecordBatch, QuerierError> {
    let (profiles, truncated) = capped_profiles(batches, cap);
    flamegraph_batch(&aggregate_profiles_to_flamegraph(&profiles), truncated)
}

/// [`encode_flamegraph_batch`] for a `baseline` document: the cap applies to
/// each side, and `truncated` is set when either side hit it.
fn encode_diff_flamegraph_batch(
    baseline: &[RecordBatch],
    comparison: &[RecordBatch],
    cap: usize,
) -> Result<RecordBatch, QuerierError> {
    let (baseline, baseline_truncated) = capped_profiles(baseline, cap);
    let (comparison, comparison_truncated) = capped_profiles(comparison, cap);
    flamegraph_batch(
        &aggregate_profiles_to_diff_flamegraph(&baseline, &comparison),
        baseline_truncated || comparison_truncated,
    )
}

fn capped_profiles(
    batches: &[RecordBatch],
    cap: usize,
) -> (Vec<common::model::profile::Profile>, bool) {
    let mut profiles: Vec<_> = batches.iter().flat_map(batch_to_models).collect();
    let truncated = profiles.len() > cap;
    profiles.truncate(cap);
    (profiles, truncated)
}

fn flamegraph_batch(
    flamegraph: &impl serde::Serialize,
    truncated: bool,
) -> Result<RecordBatch, QuerierError> {
    let flamegraph_json = serde_json::to_string(flamegraph).map_err(|e| {
        QuerierError::QueryFailed(datafusion::error::DataFusionError::Execution(format!(
            "failed to encode flamegraph: {e}"
        )))
    })?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("flamegraph_json", DataType::Utf8, false),
        Field::new("truncated", DataType::Boolean, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![flamegraph_json])),
            Arc::new(BooleanArray::from(vec![truncated])),
        ],
    )
    .map_err(|e| {
        QuerierError::QueryFailed(datafusion::error::DataFusionError::ArrowError(
            Box::new(e),
            None,
        ))
    })
}

fn validate_heatmap_window(doc: &Document, window: &ResolvedWindow) -> Result<(), QuerierError> {
    const MAX_TIME_BUCKETS: i64 = 512;
    for stage in &doc.pipeline {
        let Stage::Heatmap(heatmap) = stage else {
            continue;
        };
        let step = common::query_ir::parse_duration_ns(&heatmap.x.step)
            .ok_or_else(|| QuerierError::InvalidInput("invalid heatmap step".into()))?;
        if window.start_ns >= window.end_ns || step <= 0 {
            return Err(QuerierError::InvalidInput(
                "heatmap requires a positive step and non-empty range".into(),
            ));
        }
        let buckets = heatmap_bucket_count(window.start_ns, window.end_ns, step)?;
        if buckets > MAX_TIME_BUCKETS {
            return Err(QuerierError::InvalidInput(format!(
                "heatmap has {buckets} time buckets; maximum is {MAX_TIME_BUCKETS}"
            )));
        }
    }
    Ok(())
}

/// Count epoch-aligned buckets that intersect the inclusive query window.
fn heatmap_bucket_count(start_ns: i64, end_ns: i64, step_ns: i64) -> Result<i64, QuerierError> {
    if start_ns >= end_ns || step_ns <= 0 {
        return Err(QuerierError::InvalidInput(
            "heatmap requires a positive step and non-empty range".into(),
        ));
    }
    let first = start_ns.div_euclid(step_ns) as i128;
    let last = end_ns.div_euclid(step_ns) as i128;
    i64::try_from(last - first + 1)
        .map_err(|_| QuerierError::InvalidInput("heatmap time bucket count overflows i64".into()))
}

/// The one range function of a stepped `aggregate` (`validate` makes it the only output).
fn range_agg(agg: &Aggregate) -> Option<&common::query_ir::Agg> {
    match (&agg.step, agg.aggs.as_slice()) {
        (Some(_), [a]) if a.func.is_range_fn() => Some(a),
        _ => None,
    }
}

/// A stepped aggregate over metrics buckets on the evaluation-instant grid
/// (D11); other sources keep epoch-aligned `date_bin` buckets.
fn instant_grid_step<'a>(agg: &'a Aggregate, source: &SourcePlan) -> Option<&'a str> {
    agg.step.as_deref().filter(|_| source.name == "metrics")
}

fn parse_step(step: &str) -> Result<i64, QuerierError> {
    common::query_ir::parse_duration_ns(step)
        .ok_or_else(|| QuerierError::InvalidInput(format!("invalid step duration '{step}'")))
}

/// A range aggregate's step and window (the window defaults to the step),
/// checked against the query's evaluation instants.
fn range_step_window(
    agg: &Aggregate,
    a: &common::query_ir::Agg,
    range: &ResolvedWindow,
) -> Result<(i64, i64), QuerierError> {
    let parse = |d: &str, what: &str| {
        common::query_ir::parse_duration_ns(d)
            .ok_or_else(|| QuerierError::InvalidInput(format!("invalid {what} duration '{d}'")))
    };
    let step_ns = parse(agg.step.as_deref().unwrap_or_default(), "step")?;
    let window_ns = a
        .window
        .as_deref()
        .map_or(Ok(step_ns), |w| parse(w, "window"))?;
    check_instants(range.start_ns, range.end_ns, step_ns, window_ns)?;
    Ok((step_ns, window_ns))
}

/// The operands `histogram_quantile` and `histogram_fraction` share, and the
/// statistic each computes.
struct HistStage<'a> {
    name: &'static str,
    stat: HistStat,
    by: &'a [String],
    per_series: bool,
    step: &'a str,
    mode: HistogramMode,
    window: Option<&'a str>,
    lookback: Option<&'a str>,
    as_name: &'a str,
}

impl<'a> HistStage<'a> {
    fn of(stage: &'a Stage) -> Option<Self> {
        Some(match stage {
            Stage::HistogramQuantile(hq) => Self {
                name: "histogram_quantile",
                stat: HistStat::Quantile(hq.q),
                by: &hq.by,
                per_series: hq.per_series,
                step: &hq.step,
                mode: hq.mode,
                window: hq.window.as_deref(),
                lookback: hq.lookback.as_deref(),
                as_name: &hq.as_name,
            },
            Stage::HistogramFraction(hf) => Self {
                name: "histogram_fraction",
                stat: HistStat::Fraction(hf.lower, hf.upper),
                by: &hf.by,
                per_series: hf.per_series,
                step: &hf.step,
                mode: hf.mode,
                window: hf.window.as_deref(),
                lookback: hf.lookback.as_deref(),
                as_name: &hf.as_name,
            },
            Stage::HistogramAvg(m) => Self::moment("histogram_avg", HistStat::Avg, m),
            Stage::HistogramStddev(m) => Self::moment("histogram_stddev", HistStat::Stddev, m),
            Stage::HistogramStdvar(m) => Self::moment("histogram_stdvar", HistStat::Stdvar, m),
            _ => return None,
        })
    }

    fn moment(name: &'static str, stat: HistStat, m: &'a HistogramMoment) -> Self {
        Self {
            name,
            stat,
            by: &m.by,
            per_series: m.per_series,
            step: &m.step,
            mode: m.mode,
            window: m.window.as_deref(),
            lookback: m.lookback.as_deref(),
            as_name: &m.as_name,
        }
    }
}

/// A histogram stage's step and the window each instant reads: `window`
/// in rate mode, `lookback` in instant mode, the step when unset. Its
/// instants are bounded with the document's range
/// ([`metric_series::check_document_steps`]).
fn histogram_step_window(h: &HistStage<'_>) -> Result<(i64, i64), QuerierError> {
    let parse = |d: &str| {
        common::query_ir::parse_duration_ns(d)
            .ok_or_else(|| QuerierError::InvalidInput(format!("invalid {} duration '{d}'", h.name)))
    };
    let step_ns = parse(h.step)?;
    let window = match h.mode {
        HistogramMode::Rate => h.window,
        HistogramMode::Instant => h.lookback,
    };
    let window_ns = window.map_or(Ok(step_ns), parse)?;
    check_positive(step_ns, window_ns)?;
    Ok((step_ns, window_ns))
}

/// Resolve the document's range to an absolute window.
fn resolve_window(doc: &Document, now_ns: i64) -> Result<ResolvedWindow, QuerierError> {
    resolve_range("range", &doc.range, now_ns)
}

/// Resolve one window, rejecting a `from` after its `to`.
fn resolve_range(
    name: &str,
    range: &common::query_ir::Range,
    now_ns: i64,
) -> Result<ResolvedWindow, QuerierError> {
    let start = resolve_instant(&range.from, now_ns)?;
    let end = resolve_instant(&range.to, now_ns)?;
    if start > end {
        return Err(QuerierError::InvalidInput(format!(
            "{name}.from must not be after {name}.to"
        )));
    }
    Ok(ResolvedWindow {
        start_ns: start,
        end_ns: end,
    })
}

fn resolve_instant(value: &serde_json::Value, now_ns: i64) -> Result<i64, QuerierError> {
    match coerce(value, &ValueType::TimestampNs) {
        Ok(Literal::Timestamp(ts)) => Ok(ts.resolve(now_ns)),
        _ => Err(QuerierError::InvalidInput(format!(
            "invalid time bound: {value}"
        ))),
    }
}

use datafusion::arrow::array::RecordBatch;

/// The output physical column name for a parent-side field after
/// `correlate`: `parent.<child physical name>` — a literal dot, matching the
/// IR's `parent.` field scope directly and never colliding with a real
/// child column (`parent_span_id`, with an underscore, is one).
/// Referenced via `ident()`, never `col()`, since the dot must not be
/// parsed as a qualifier.
const PARENT_COLUMN_PREFIX: &str = "parent.";

/// A `TypedAttribute`'s `(homes, promoted, key, prefix)` — the arguments
/// `typed_home_filter_expr` needs to lower a filter comparison through the
/// OR-rewrite (see `promoted_typed_attribute`, `ordered`).
type TypedAttrFilterParts = (Vec<String>, Vec<Option<String>>, String, String);

/// A collision-free rename applied to the parent-side scan before the join —
/// see [`Lowering::lower_correlate`] for why a flat rename is used instead
/// of a `DataFrame::alias` table qualifier.
const PARENT_JOIN_TMP_PREFIX: &str = "__correlate_parent__";

/// Default row cap on a `correlate` stage's joined output
/// (`openspec/changes/query-ir-span-join`'s design), also
/// `QuerierConfig::correlate_max_rows`'s default. [`IrService::query`]'s real
/// callers use [`IrService::with_correlate_max_rows`] to override it from
/// config; every other caller of `plan_document` (compat lowerings, tests)
/// keeps this default, which is moot for them since none can reach a
/// `correlate` stage.
pub(crate) const DEFAULT_CORRELATE_MAX_ROWS: usize = 5_000_000;

/// Default source-row cap of a signal-target `correlate`, also
/// `QuerierConfig::correlate_max_source_rows`'s default.
pub(crate) const DEFAULT_CORRELATE_MAX_SOURCE_ROWS: usize = 10_000;

/// What a `correlate` stage reports once the plan has run: the streaming
/// row-cap and fan-out-cap flags and the target scan window (signal target),
/// plus the traces a `match` stage found cut by the range.
#[derive(Debug, Default)]
pub(crate) struct CorrelateOutcome {
    pub truncated: Option<Arc<AtomicBool>>,
    pub fanout_limit: Option<Arc<AtomicBool>>,
    pub window: Option<ResolvedWindow>,
    pub match_incomplete: Option<structural_match::IncompleteTraces>,
}

/// Helper columns of a signal-target `correlate`: the canonical key columns
/// (suffixed by key-field index), the time envelope inputs and the source
/// row ordinal. A source column under [`CORRELATE_HELPER_PREFIX`] is
/// rejected so a helper can never shadow it.
const CORRELATE_HELPER_PREFIX: &str = "__correlate_";
const CORRELATE_KEY_PREFIX: &str = "__correlate_key_";
const CORRELATE_TARGET_KEY_PREFIX: &str = "__correlate_target_key_";
const CORRELATE_START: &str = "__correlate_start";
const CORRELATE_DURATION: &str = "__correlate_duration";
const CORRELATE_ROW: &str = "__correlate_row";
/// A target row's fan-out rank within its key.
const CORRELATE_RANK: &str = "__correlate_rank";
/// Default per-source-row match cap of an inner/left signal `correlate`.
const DEFAULT_CORRELATE_FANOUT: u64 = 100;

/// How long before the target window a `traces` target's span may start and
/// still overlap it. Spans are stored by start time, so the overlap test
/// needs a prunable lower bound; a span longer than this matches only when
/// `window.before` covers the difference.
const TRACE_TARGET_LOOKBACK_NS: i64 = 3_600 * 1_000_000_000;

/// The `[min start, max end]` of the materialized source rows, each row
/// ending at `start + duration` when a [`CORRELATE_DURATION`] column is
/// present (negative durations count as zero; the end saturates).
fn source_envelope(batches: &[RecordBatch]) -> Option<(i64, i64)> {
    let mut envelope: Option<(i64, i64)> = None;
    for batch in batches {
        let Some(starts) = batch
            .column_by_name(CORRELATE_START)
            .and_then(|c| c.as_primitive_opt::<Int64Type>())
        else {
            continue;
        };
        let durations = batch
            .column_by_name(CORRELATE_DURATION)
            .and_then(|c| c.as_primitive_opt::<Int64Type>());
        for (row, start) in starts.iter().enumerate() {
            let Some(start) = start else { continue };
            let duration = durations
                .filter(|d| d.is_valid(row))
                .map_or(0, |d| d.value(row).max(0));
            let end = start.saturating_add(duration);
            envelope = Some(envelope.map_or((start, end), |(lo, hi)| (lo.min(start), hi.max(end))));
        }
    }
    envelope
}

/// Whether the writer stores `column` of `source` in the canonical key form
/// (trace/span ids as lowercase hex, identity/series digests), so the key
/// set can bound the raw column directly.
fn writer_stores_canonical_key(source: &str, column: &str) -> bool {
    match column {
        "trace_id" | "span_id" => matches!(source, "traces" | "logs" | "exemplars" | "profiles"),
        "resource_identity" | "series_id" => true,
        _ => false,
    }
}

/// The physical column `field` reads from, or `None` when it resolves to
/// anything else (an attribute read, a derived value) or not at all.
fn stored_column(resolver: &SchemaResolver, field: &str) -> Option<String> {
    match resolver.resolve("", field) {
        Some(Resolved::Column { name, .. }) => Some(name),
        _ => None,
    }
}

/// The stored duration column of a `traces` relation. Only a span covers
/// an interval; other signals' durations (a profile's) describe something
/// else.
fn span_duration_column(plan: &SourcePlan, resolver: &SchemaResolver) -> Option<String> {
    (plan.name == "traces")
        .then(|| stored_column(resolver, "duration"))
        .flatten()
}

/// Reject a correlate key that does not read a stored column on `side`:
/// an attribute fallback (e.g. an older table without the column) would
/// silently match nothing.
fn require_stored_key(
    resolver: &SchemaResolver,
    fields: &[&str],
    key: &str,
    side: &str,
    source: &str,
) -> Result<(), QuerierError> {
    match fields.iter().find(|f| stored_column(resolver, f).is_none()) {
        Some(field) => Err(QuerierError::InvalidInput(format!(
            "correlate key `{key}`: field `{field}` has no stored column on the {side} \
             `{source}`, so it cannot be joined"
        ))),
        None => Ok(()),
    }
}

/// Append a [`CORRELATE_ROW`] ordinal to the materialized source batches so
/// the join result can be put back in source order.
fn with_row_ordinal(
    schema: &Schema,
    batches: Vec<RecordBatch>,
) -> Result<(Arc<Schema>, Vec<RecordBatch>), QuerierError> {
    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new(CORRELATE_ROW, DataType::UInt64, false)));
    let schema = Arc::new(Schema::new(fields));
    let mut offset = 0_u64;
    let batches = batches
        .into_iter()
        .map(|batch| {
            let rows = batch.num_rows() as u64;
            let mut columns = batch.columns().to_vec();
            columns.push(Arc::new(UInt64Array::from_iter_values(
                offset..offset + rows,
            )));
            offset += rows;
            RecordBatch::try_new(Arc::clone(&schema), columns)
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| QuerierError::QueryFailed(e.into()))?;
    Ok((schema, batches))
}

/// The signal source a document's `correlate` stage targets, if any.
fn signal_target(doc: &Document) -> Option<&str> {
    doc.pipeline.iter().find_map(|stage| match stage {
        Stage::Correlate(Correlate {
            to: CorrelateTarget::Signal(name),
            ..
        }) => Some(name.as_str()),
        _ => None,
    })
}

/// Build the [`SchemaResolver`] for a scanned table, resolving a
/// typed-layout table's committed canonical attribute types when `request`
/// asks for them.
async fn schema_resolver(
    schema: &datafusion::common::DFSchema,
    source: &SourcePlan,
    request: &AttributeTypeRequest,
    tenant_slug: &str,
    dataset_slug: &str,
) -> Result<SchemaResolver, QuerierError> {
    let resolver = SchemaResolver::new(schema, source);
    let AttributeTypeRequest::Resolve(lookup) = request else {
        return Ok(resolver);
    };
    if !common::schema::typed_attributes::is_typed_layout(
        schema.fields().iter().map(|f| f.name().as_str()),
    ) {
        return Ok(resolver);
    }
    let Some(lookup) = lookup else {
        return Err(QuerierError::QueryFailed(
            datafusion::error::DataFusionError::Execution(
                "attribute type registry not configured".to_string(),
            ),
        ));
    };
    let signal = common::discovery::signal_for_source(source.name).unwrap_or(source.name);
    let types = lookup
        .canonical_types(tenant_slug, dataset_slug, signal)
        .await?;
    Ok(resolver.with_typed(types))
}

/// The target side of a signal-target `correlate`: its scan (`None` when the
/// table does not exist) and the resolver its sub-pipeline validates and
/// lowers against.
struct CorrelateTargetSide {
    plan: SourcePlan,
    base: Option<DataFrame>,
    resolver: SchemaResolver,
}

impl CorrelateTargetSide {
    async fn scan(
        ctx: &SessionContext,
        plan: SourcePlan,
        request: &AttributeTypeRequest,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Self, QuerierError> {
        let (base, resolver) = match scan_source(ctx, tenant_slug, dataset_slug, &plan).await? {
            Some(base) => {
                let resolver =
                    schema_resolver(base.schema(), &plan, request, tenant_slug, dataset_slug)
                        .await?;
                (Some(base), resolver)
            }
            // A missing table joins as an empty one of the canonical schema,
            // so the output has the same target columns either way. The typed
            // registry is consulted for it like a present table's, so
            // `<target>.x` keeps its canonical type.
            None => match empty_canonical_scan(ctx, &plan)? {
                Some(empty) => {
                    let resolver =
                        schema_resolver(empty.schema(), &plan, request, tenant_slug, dataset_slug)
                            .await?;
                    (Some(empty), resolver)
                }
                None => (
                    None,
                    SchemaResolver::new(&datafusion::common::DFSchema::empty(), &plan),
                ),
            },
        };
        Ok(Self {
            plan,
            base,
            resolver,
        })
    }
}

/// Resolves each field against its own source: the document's `from`, or a
/// signal-target `correlate`'s target.
struct SourceResolvers<'a> {
    from: &'a SchemaResolver,
    target: Option<&'a SchemaResolver>,
}

impl SourceResolvers<'_> {
    fn pick(&self, source: &str) -> &SchemaResolver {
        self.target
            .filter(|target| target.source == source)
            .unwrap_or(self.from)
    }
}

impl FieldResolver for SourceResolvers<'_> {
    fn resolve(&self, source: &str, field: &str) -> Option<Resolved> {
        self.pick(source).resolve(source, field)
    }

    fn is_known(&self, source: &str, field: &str) -> bool {
        self.pick(source).is_known(source, field)
    }

    fn is_physical_name(&self, source: &str, field: &str) -> bool {
        self.pick(source).is_physical_name(source, field)
    }

    fn is_filterable(&self, source: &str, field: &str) -> bool {
        self.pick(source).is_filterable(source, field)
    }
}

/// A signal `correlate`'s target rows within `window`, restricted to `keys`
/// and narrowed by the target `pipeline`, with each canonical key as a
/// `CORRELATE_TARGET_KEY_PREFIX` column.
///
/// Only a Utf8 column the writer is known to store canonically (see
/// [`writer_stores_canonical_key`]) gets a literal IN-list on the raw column
/// (prunable); anything else is compared through its canonical form,
/// correct but without pushdown.
///
/// A `traces` target matches spans *overlapping* `window` (a log emitted
/// mid-span belongs to a span that started earlier), with the start bounded
/// below by [`TRACE_TARGET_LOOKBACK_NS`] so the scan stays prunable.
fn target_frame(
    target: &CorrelateTargetSide,
    base: DataFrame,
    field_keys: &[(&str, &BTreeSet<String>)],
    window: &ResolvedWindow,
    correlate: &Correlate,
    now_ns: i64,
    demand: Option<&AttrDemandScope<'_>>,
) -> Result<DataFrame, QuerierError> {
    let mut lowering = Lowering {
        source: &target.plan,
        resolver: &target.resolver,
        now_ns,
        demand: demand.and_then(|scope| {
            common::discovery::signal_for_source(target.plan.name)
                .map(|signal| scope.for_signal(signal))
        }),
        aggregated: false,
        series_shaped: false,
        col_of: HashMap::new(),
        derived_types: HashMap::new(),
        schema_cols: base
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect(),
        scope: None,
        correlate_truncated: None,
        correlate_fanout: None,
        correlate_window: None,
    };
    let mut df = match span_duration_column(&target.plan, &target.resolver) {
        Some(duration) => {
            let scan = ResolvedWindow {
                start_ns: window.start_ns.saturating_sub(TRACE_TARGET_LOOKBACK_NS),
                end_ns: window.end_ns,
            };
            let start = cast(col(target.plan.time_col), DataType::Int64);
            let duration = coalesce(vec![cast(ident(duration), DataType::Int64), lit(0_i64)]);
            // `start + max(duration, 0) >= window.start`, rearranged so the
            // arithmetic cannot overflow.
            let overlaps = start
                .clone()
                .gt_eq(lit(window.start_ns))
                .or(duration.gt_eq(lit(window.start_ns) - start));
            lowering.apply_time_window(base, &scan)?.filter(overlaps)?
        }
        None => lowering.apply_time_window(base, window)?,
    };
    for (i, (field, keys)) in field_keys.iter().enumerate() {
        let expr = lowering.value_expr(field)?;
        let data_type = expr.get_type(df.schema())?;
        let stored_canonical = stored_column(lowering.resolver, field)
            .is_some_and(|column| writer_stores_canonical_key(target.plan.name, &column))
            && matches!(
                data_type,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            );
        let key = if stored_canonical {
            expr
        } else {
            canonical_key(expr, &data_type)
        };
        let list = keys.iter().map(|k| lit(k.as_str())).collect();
        df = df
            .filter(key.clone().in_list(list, false))?
            .with_column(&format!("{CORRELATE_TARGET_KEY_PREFIX}{i}"), key)?;
    }
    for stage in &correlate.pipeline {
        df = lowering.lower_stage(df, stage)?;
    }
    Ok(df)
}

/// `frame`'s target columns renamed to `<target>.<physical>` (a logs `body`
/// decoded) before the join, so none collides with a source column (see
/// [`Lowering::lower_correlate`] for why not a table qualifier), keeping at
/// most `fanout` rows per target key: ranked by target time ascending, then
/// every other non-nested target column ascending. Rows equal in all of
/// those (differing only in attribute containers or other nested columns)
/// tie, and which of them is kept is unspecified. The frame is already
/// bounded to the source keys, so every key ranked here matches a source
/// row and a dropped row is a real overflow, flagged on `overflow`.
fn prefixed_target(
    frame: DataFrame,
    plan: &SourcePlan,
    fanout: u64,
    overflow: Arc<AtomicBool>,
) -> Result<(DataFrame, Vec<String>), QuerierError> {
    let mut select = Vec::new();
    let mut names = Vec::new();
    let mut keys = Vec::new();
    let mut order = Vec::new();
    for field in frame.schema().fields() {
        let physical = field.name();
        if physical.starts_with(CORRELATE_TARGET_KEY_PREFIX) {
            select.push(ident(physical));
            keys.push(ident(physical));
            continue;
        }
        let name = format!("{}.{physical}", plan.name);
        let value = if is_body_column(physical) {
            body_decode_expr(physical)
        } else {
            ident(physical)
        };
        select.push(value.alias(&name));
        if !field.data_type().is_nested() {
            let key = ident(&name).sort(true, false);
            if physical == plan.time_col {
                order.insert(0, key);
            } else {
                order.push(key);
            }
        }
        names.push(name);
    }
    let rank = row_number().partition_by(keys).order_by(order).build()?;
    let ranked = frame.select(select)?.with_column(CORRELATE_RANK, rank)?;
    let capped =
        super::correlate_cap::wrap_with_rank_cap(ranked, CORRELATE_RANK, fanout, overflow)?;
    Ok((capped, names))
}

/// A join key's canonical text form: lowercase text, or lowercase hex for a
/// binary key; empty keys become NULL so they never match.
fn canonical_key(expr: Expr, data_type: &DataType) -> Expr {
    let text = match data_type {
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => encode(cast(expr, DataType::Binary), lit("hex")),
        _ => lower(cast(expr, DataType::Utf8)),
    };
    nullif(text, lit(""))
}

/// Whether a [`PlanRequest`] wants a typed-layout table's committed
/// attribute types resolved before planning. Every compat lowering
/// (LogQL/TraceQL) and every planner test — anything built via
/// [`PlanRequest::new`] — stays `CompatOnly`: they already read a typed
/// table's columns directly (see `has_typed_container`) without needing the
/// per-key canonical types. Only [`IrService::query`]'s `POST /api/v1/query`
/// path opts into `Resolve`, and only it can hit the "typed table, no
/// lookup attached" error — a compat caller over the same typed table stays
/// on `CompatString` reads instead.
#[derive(Clone, Default)]
pub(crate) enum AttributeTypeRequest {
    #[default]
    CompatOnly,
    Resolve(Option<Arc<dyn CanonicalTypeLookup>>),
}

/// [`plan_document`]'s request-scoped parameters — tenant/dataset scope, the
/// query clock, the `correlate` row cap, and the attribute-type request —
/// bundled so a caller that never reaches a `correlate` stage or a typed
/// table (every compat lowering, every planner test) can
/// build one with [`PlanRequest::new`] and not spell out either default at
/// every call site.
#[derive(Clone)]
pub(crate) struct PlanRequest<'a> {
    pub tenant_slug: &'a str,
    pub dataset_slug: &'a str,
    pub now_ns: i64,
    pub correlate_max_rows: usize,
    pub correlate_max_source_rows: usize,
    pub match_limits: MatchLimits,
    pub attribute_type_request: AttributeTypeRequest,
    /// Sort, resume and bound the result to one page, and the rows a
    /// `rows` page's sort fetches.
    pub page: Option<(&'a PageRequest, usize)>,
}

impl<'a> PlanRequest<'a> {
    pub(crate) fn new(tenant_slug: &'a str, dataset_slug: &'a str, now_ns: i64) -> Self {
        Self {
            tenant_slug,
            dataset_slug,
            now_ns,
            correlate_max_rows: DEFAULT_CORRELATE_MAX_ROWS,
            correlate_max_source_rows: DEFAULT_CORRELATE_MAX_SOURCE_ROWS,
            match_limits: MatchLimits::default(),
            attribute_type_request: AttributeTypeRequest::CompatOnly,
            page: None,
        }
    }

    pub(crate) fn with_page(mut self, page: Option<(&'a PageRequest, usize)>) -> Self {
        self.page = page;
        self
    }

    pub(crate) fn with_correlate_max_rows(mut self, correlate_max_rows: usize) -> Self {
        self.correlate_max_rows = correlate_max_rows;
        self
    }

    pub(crate) fn with_correlate_max_source_rows(
        mut self,
        correlate_max_source_rows: usize,
    ) -> Self {
        self.correlate_max_source_rows = correlate_max_source_rows;
        self
    }

    pub(crate) fn with_match_limits(mut self, match_limits: MatchLimits) -> Self {
        self.match_limits = match_limits;
        self
    }

    pub(crate) fn with_attribute_type_request(
        mut self,
        attribute_type_request: AttributeTypeRequest,
    ) -> Self {
        self.attribute_type_request = attribute_type_request;
        self
    }
}

/// The request-scoped context [`Lowering::lower_correlate`] needs beyond
/// what it already carries — bundled to keep the method's argument count
/// down, not a reusable abstraction.
struct CorrelateScan<'a> {
    tenant_slug: &'a str,
    dataset_slug: &'a str,
    window: &'a ResolvedWindow,
    correlate_max_rows: usize,
    correlate_max_source_rows: usize,
}

/// One recorded demand hit: (signal, level, key).
type DemandKey = (&'static str, AttributeLevel, String);

/// Where to record per-level attribute-promotion demand for one document
/// (change: otel-native-schema layer 6) — the tenant/dataset slugs and
/// signal the compactor's analyzer keys `attribute_level_stats` by, plus
/// this document's own dedup set so a key hit more than once (e.g. the same
/// filter repeated, or a key used in both a filter and a group-by) counts
/// once, mirroring the compat paths' flat demand counters. The set is keyed
/// per (signal, level, key) and shared with a `correlate` target's
/// sub-pipeline ([`Self::for_signal`]), so one document counts a key once per
/// signal however many pipelines reference it.
struct AttrDemandScope<'a> {
    tenant_slug: &'a str,
    dataset_slug: &'a str,
    signal: &'static str,
    seen: Arc<Mutex<HashSet<DemandKey>>>,
}

impl<'a> AttrDemandScope<'a> {
    fn new(tenant_slug: &'a str, dataset_slug: &'a str, signal: &'static str) -> Self {
        Self {
            tenant_slug,
            dataset_slug,
            signal,
            seen: Arc::default(),
        }
    }

    fn for_signal(&self, signal: &'static str) -> Self {
        Self {
            tenant_slug: self.tenant_slug,
            dataset_slug: self.dataset_slug,
            signal,
            seen: Arc::clone(&self.seen),
        }
    }
}

struct Lowering<'a> {
    source: &'a SourcePlan,
    resolver: &'a SchemaResolver,
    now_ns: i64,
    /// Tenant/dataset slugs — the same ones the compactor's analyzer keys
    /// `attribute_level_stats` by — for recording per-level attribute
    /// demand (change: otel-native-schema layer 6). `None` for a caller
    /// that constructs a `Lowering` directly rather than through
    /// `plan_document` (a `correlate` unit test): demand is simply not
    /// recorded then.
    demand: Option<AttrDemandScope<'a>>,
    aggregated: bool,
    series_shaped: bool,
    /// Logical name → current DataFrame column name (extract-derived and
    /// post-aggregate output columns).
    col_of: HashMap<String, String>,
    /// Declared types of extract-derived fields, for literal coercion.
    derived_types: HashMap<String, ValueType>,
    /// The current base-table physical column names.
    schema_cols: Vec<String>,
    /// The far side of a `correlate` join, once one has run: its
    /// `<prefix><field>` references resolve through [`Self::scoped_field`].
    scope: Option<CorrelateScope<'a>>,
    /// `Some` once a `correlate` stage has streamed into `CorrelateCapExec`
    /// (`correlate_cap`) — the shared flag it flips if the join's row count
    /// crosses `correlate_max_rows`. Ground truth captured *during*
    /// execution, only readable after `.collect()` finishes
    /// ([`IrService::query`] does the read); `None` when the pipeline never
    /// reached `correlate`.
    correlate_truncated: Option<Arc<AtomicBool>>,
    /// Like `correlate_truncated`, for an inner/left signal `correlate`'s
    /// per-source-row `fanout` cap.
    correlate_fanout: Option<Arc<AtomicBool>>,
    /// The target scan window a signal-target `correlate` used.
    correlate_window: Option<ResolvedWindow>,
}

/// The far side of a `correlate` join: the parent span (`parent.`) or an
/// inner/left signal target (`<target>.`). A `<prefix><field>` reference
/// resolves through `resolver` against `plan`, reading the
/// `<prefix><physical>` columns the join produced.
struct CorrelateScope<'a> {
    prefix: String,
    plan: &'a SourcePlan,
    resolver: &'a SchemaResolver,
    /// The signal attribute demand for this scope's references records
    /// under; `None` when the far side has no signal.
    signal: Option<&'static str>,
}

impl<'a> Lowering<'a> {
    fn apply_time_window(
        &self,
        df: DataFrame,
        window: &ResolvedWindow,
    ) -> Result<DataFrame, QuerierError> {
        let (lo, hi) = if self.source.time_is_timestamp {
            (
                lit(ScalarValue::TimestampNanosecond(
                    Some(window.start_ns),
                    None,
                )),
                lit(ScalarValue::TimestampNanosecond(Some(window.end_ns), None)),
            )
        } else {
            (lit(window.start_ns), lit(window.end_ns))
        };
        let time = col(self.source.time_col);
        let mut df = df
            .filter(time.clone().gt_eq(lo))
            .map_err(QuerierError::QueryFailed)?
            .filter(time.lt_eq(hi))
            .map_err(QuerierError::QueryFailed)?;

        // When the time column is an integer nanosecond column (traces), the
        // window filter alone never engages Iceberg partition pruning: the
        // partition transform is `Hour(timestamp)`. Mirror the bounds onto
        // the `timestamp` partition column (widened outward, so they never
        // exclude a row the precise filter keeps) — issue #928.
        if !self.source.time_is_timestamp {
            let ts_type = df
                .schema()
                .fields()
                .iter()
                .find(|f| f.name() == "timestamp")
                .map(|f| f.data_type().clone());
            if let Some(ts_type) = ts_type {
                df = df
                    .filter(super::trace::timestamp_bound_expr(
                        window.start_ns,
                        &ts_type,
                        false,
                    )?)
                    .map_err(QuerierError::QueryFailed)?
                    .filter(super::trace::timestamp_bound_expr(
                        window.end_ns,
                        &ts_type,
                        true,
                    )?)
                    .map_err(QuerierError::QueryFailed)?;
            }
        }
        Ok(df)
    }

    /// `time_col <op> ns` over the bare column, for an inclusive `op`. The
    /// Iceberg provider pushes down any filter that reads only the partition
    /// source column, but rewrites it onto the `Hour(timestamp)` partition
    /// only when the column itself is an operand: a filter over an
    /// expression of the column (including one pushed through a projection)
    /// fails the scan with "No field named timestamp" (#2122). A strict
    /// bound would become a strict bound on the hour and prune the hour it
    /// falls in. The literal is in the column's own unit, rounded inward
    /// (up for `>=`, down for `<=`), so a coarser column keeps the bound
    /// exact instead of letting the planner truncate it.
    fn time_bound(&self, df: &DataFrame, ns: i64, op: Operator) -> Result<Expr, QuerierError> {
        let bound = if self.source.time_is_timestamp {
            let field = df
                .schema()
                .field_with_unqualified_name(self.source.time_col)
                .map_err(QuerierError::QueryFailed)?;
            let round_up = op == Operator::GtEq;
            lit(super::trace::timestamp_bound_scalar(
                ns,
                field.data_type(),
                round_up,
            )?)
        } else {
            lit(ns)
        };
        Ok(datafusion::logical_expr::binary_expr(
            col(self.source.time_col),
            op,
            bound,
        ))
    }

    fn lower_stage(&mut self, df: DataFrame, stage: &Stage) -> Result<DataFrame, QuerierError> {
        match stage {
            Stage::Where(pred) => {
                let expr = self.lower_predicate(pred)?;
                df.filter(expr).map_err(QuerierError::QueryFailed)
            }
            Stage::Aggregate(agg) => self.lower_aggregate(df, agg, None),
            Stage::Topk(rank) => self.lower_rank(df, &rank.of, rank.n, false),
            Stage::Bottomk(rank) => self.lower_rank(df, &rank.of, rank.n, true),
            Stage::Order(keys) => {
                // `value_expr`, not `df_col`: a promoted-but-not-yet-backfilled
                // attribute must sort by its coalesced value (the same
                // COALESCE(column, attribute-map fallback) WHERE/SELECT use),
                // not the bare column alone, or rows in unrewritten files sort
                // by NULL instead of their true value (#816 follow-up).
                let sort = keys
                    .iter()
                    .map(|k| {
                        self.record_field_demand(&k.of);
                        let ascending = matches!(k.dir, common::query_ir::Direction::Asc);
                        Ok(self.value_expr(&k.of)?.sort(ascending, true))
                    })
                    .collect::<Result<Vec<_>, QuerierError>>()?;
                df.sort(sort).map_err(QuerierError::QueryFailed)
            }
            Stage::Limit(n) => df
                .limit(0, Some(*n as usize))
                .map_err(QuerierError::QueryFailed),
            Stage::Extract(extract) => self.lower_extract(df, extract),
            Stage::Heatmap(heatmap) => self.lower_heatmap(df, heatmap),
            // Lowered by `plan_operand`'s stage loop through `lower_histogram`
            // (it needs the stage's resolved window) — never reached.
            Stage::HistogramQuantile(_)
            | Stage::HistogramFraction(_)
            | Stage::HistogramAvg(_)
            | Stage::HistogramStddev(_)
            | Stage::HistogramStdvar(_) => Err(QuerierError::InvalidInput(format!(
                "{} requires async lowering",
                stage.name()
            ))),
            // Discovery is answered from the registry and maintained
            // statistics in the router; a `describe` document never becomes a
            // plan, so reaching here means one was routed to a querier by
            // mistake. Fail loudly rather than lowering something.
            Stage::Describe(_) => Err(QuerierError::InvalidInput(
                "describe introspects a source and is not executable as a query".into(),
            )),
            // Handled directly in `plan_document`'s stage loop (needs its own
            // async scan of the traces table) — never reached.
            Stage::Correlate(_) => Err(QuerierError::InvalidInput(
                "correlate requires async lowering".into(),
            )),
            // Lowered in `plan_operand`, which holds the request's bounds.
            Stage::Match(_) => Err(internal("match reached lower_stage".into())),
            Stage::Sample(_)
            | Stage::Scalar(_)
            | Stage::Vector(_)
            | Stage::Reduce(_)
            | Stage::Map(_)
            | Stage::Labels(_)
            | Stage::Filter(_)
            | Stage::Sort(_)
            | Stage::Absent(_)
            | Stage::OverTime(_)
            | Stage::Binop(_) => Err(unsupported_stage(stage)),
        }
    }

    /// Lower the `correlate` stage (`irVersion` 8): join the traces relation
    /// to the span in the same trace whose `span_id` equals this row's
    /// `parent_span_id`, per `openspec/changes/query-ir-span-join`.
    ///
    /// The parent side is a second, independent scan of the same table,
    /// bounded by the same time window as the child side (D of the design:
    /// "both sides bounded by the outer range") and renamed column-by-column
    /// (`PARENT_JOIN_TMP_PREFIX`) before the join, so nothing on that side
    /// collides with the child's own column names — a `DataFrame::alias`
    /// table qualifier would do the same for scalar columns, but DataFusion's
    /// optimizer reports a spurious "ambiguous reference" once a `Map`-typed
    /// attribute container column is involved. The final output carries
    /// every parent column under `parent.<physical name>`
    /// (`PARENT_COLUMN_PREFIX`). The joined output is capped by
    /// `[querier].correlate_max_rows` — a plain row limit, since a span has
    /// at most one parent and the only fan-out risk is a duplicate `span_id`.
    async fn lower_correlate(
        &mut self,
        ctx: &SessionContext,
        df: DataFrame,
        correlate: &Correlate,
        scan: CorrelateScan<'_>,
    ) -> Result<DataFrame, QuerierError> {
        let CorrelateScan {
            tenant_slug,
            dataset_slug,
            window,
            correlate_max_rows,
            ..
        } = scan;
        let join_type = match (&correlate.to, correlate.kind) {
            (CorrelateTarget::Parent, JoinKind::Inner) => JoinType::Inner,
            (CorrelateTarget::Parent, JoinKind::Left) => JoinType::Left,
            _ => {
                return Err(internal(
                    "a signal or semi/anti correlate reached the parent lowering".into(),
                ));
            }
        };

        let Some(parent_base) =
            scan_parent_traces(ctx, tenant_slug, dataset_slug, self.source).await?
        else {
            // No traces table to join against: every child is parentless.
            // Still mark the relation correlated, so a later `parent.*`
            // reference resolves (to an always-empty/always-null column)
            // rather than erroring as if `correlate` had never run.
            self.scope = Some(self.parent_scope());
            return match join_type {
                JoinType::Inner => df.limit(0, Some(0)).map_err(QuerierError::QueryFailed),
                _ => Ok(df),
            };
        };
        let parent_windowed = self.apply_time_window(parent_base, window)?;
        let parent_cols: Vec<String> = parent_windowed
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        // Both sides scan the same physical table, so every column name
        // (`trace_id`, `service_name`, ...) exists identically on both. A
        // `DataFrame::alias` table qualifier disambiguates scalar columns
        // fine, but produces a spurious "ambiguous reference" from
        // DataFusion's optimizer once a `Map`-typed attribute container
        // column (`span_attributes`) is involved — so instead, rename every
        // parent-side column to a unique flat name up front, before the
        // join, leaving the child side untouched. With no overlapping names
        // left, the join predicate and every later reference need no
        // qualifier at all.
        let parent_renamed: Vec<Expr> = parent_cols
            .iter()
            .map(|c| col(c.as_str()).alias(format!("{PARENT_JOIN_TMP_PREFIX}{c}")))
            .collect();
        let parent_scan = parent_windowed
            .select(parent_renamed)
            .map_err(QuerierError::QueryFailed)?;

        let on = vec![
            col("trace_id").eq(col(format!("{PARENT_JOIN_TMP_PREFIX}trace_id"))),
            col("parent_span_id").eq(col(format!("{PARENT_JOIN_TMP_PREFIX}span_id"))),
        ];
        let joined = df
            .join_on(parent_scan, join_type, on)
            .map_err(QuerierError::QueryFailed)?;

        let mut select_exprs: Vec<Expr> =
            self.schema_cols.iter().map(|c| col(c.as_str())).collect();
        let mut schema_cols = self.schema_cols.clone();
        for c in &parent_cols {
            let out_name = format!("{PARENT_COLUMN_PREFIX}{c}");
            select_exprs.push(col(format!("{PARENT_JOIN_TMP_PREFIX}{c}")).alias(out_name.clone()));
            schema_cols.push(out_name);
        }
        let joined = joined
            .select(select_exprs)
            .map_err(QuerierError::QueryFailed)?;
        // The cap is enforced *inside* the plan, streaming — `CorrelateCapExec`
        // passes batches through unchanged up to `correlate_max_rows` and, on
        // the batch that would cross it, slices off the excess, flags
        // `truncated`, and ends its stream. This must be resolved at the join
        // itself, not left for `IrService::query`'s final `.collect()`: a
        // later `aggregate`/`where`/`limit` stage can shrink or hide the row
        // count, but the join's own overflow already happened and must still
        // be reported — without ever materializing the join's full output
        // first (see `correlate_cap`'s module doc comment).
        let truncated = Arc::new(AtomicBool::new(false));
        let joined = super::correlate_cap::wrap_with_cap(
            joined,
            correlate_max_rows,
            Arc::clone(&truncated),
        )?;

        self.schema_cols = schema_cols;
        self.scope = Some(self.parent_scope());
        self.correlate_truncated = Some(truncated);
        Ok(joined)
    }

    /// Lower the `match` stage (`irVersion` 12) through the per-trace
    /// evaluator in [`structural_match`].
    fn lower_match(
        &mut self,
        df: DataFrame,
        stage: &Match,
        scan: &ResolvedWindow,
        limits: MatchLimits,
    ) -> Result<(DataFrame, structural_match::IncompleteTraces), QuerierError> {
        if let Some(clash) = self
            .schema_cols
            .iter()
            .find(|c| c.starts_with(structural_match::FLAG_PREFIX) || *c == Match::SPANSETS)
        {
            return Err(QuerierError::InvalidInput(format!(
                "column '{clash}' collides with a name the match stage reserves"
            )));
        }
        let flags = stage
            .spansets
            .0
            .iter()
            .map(|(_, pred)| Ok(coalesce(vec![self.lower_predicate(pred)?, lit(false)])))
            .collect::<Result<Vec<_>, QuerierError>>()?;
        let time_col = self.source.time_col;
        let lowered = structural_match::lower(df, stage, flags, time_col, scan.end_ns, limits)?;
        self.col_of
            .insert(Match::SPANSETS.to_string(), Match::SPANSETS.to_string());
        self.schema_cols.push(Match::SPANSETS.to_string());
        Ok(lowered)
    }

    fn parent_scope(&self) -> CorrelateScope<'a> {
        CorrelateScope {
            prefix: PARENT_COLUMN_PREFIX.to_string(),
            plan: self.source,
            resolver: self.resolver,
            signal: self.demand.as_ref().map(|demand| demand.signal),
        }
    }

    /// Lower a signal-target `correlate` in two phases, never as a free
    /// join over both full tables. Phase 1 materializes the source relation
    /// (at most `correlate_max_source_rows` rows, else a resource error) and
    /// takes its canonical key set and time envelope. Phase 2 scans the
    /// target only within that envelope (widened by `window`) and for those
    /// keys, then semi/anti-joins the source rows against the target's
    /// distinct keys, or inner/left-joins them to the target rows (see
    /// [`Self::join_target_rows`]). A missing target table matches nothing.
    ///
    /// A source row whose key is null or empty never matches: semi drops it,
    /// anti keeps it. The result keeps the source row order, so a preceding
    /// `topk`/`order` survives the join.
    async fn lower_signal_correlate(
        &mut self,
        ctx: &SessionContext,
        df: DataFrame,
        correlate: &Correlate,
        target: &'a CorrelateTargetSide,
        scan: CorrelateScan<'_>,
    ) -> Result<DataFrame, QuerierError> {
        let doc_window = scan.window;
        let max_source_rows = scan.correlate_max_source_rows;
        let join_type = match correlate.kind {
            JoinKind::Semi => JoinType::LeftSemi,
            JoinKind::Anti => JoinType::LeftAnti,
            JoinKind::Inner => JoinType::Inner,
            JoinKind::Left => JoinType::Left,
        };
        let enrich = matches!(join_type, JoinType::Inner | JoinType::Left);
        let Some((key, source_fields, target_fields)) = correlate.on.and_then(|key| {
            Some((
                key.as_str(),
                key.fields(self.source.name)?,
                key.fields(target.plan.name)?,
            ))
        }) else {
            return Err(internal("correlate key missing after validation".into()));
        };
        require_stored_key(
            self.resolver,
            source_fields,
            key,
            "source",
            self.source.name,
        )?;
        require_stored_key(
            &target.resolver,
            target_fields,
            key,
            "target",
            target.plan.name,
        )?;
        if let Some(reserved) = df
            .schema()
            .fields()
            .iter()
            .find(|f| f.name().starts_with(CORRELATE_HELPER_PREFIX))
        {
            return Err(QuerierError::InvalidInput(format!(
                "column `{}` uses the reserved `{CORRELATE_HELPER_PREFIX}` prefix; rename it \
                 before correlate",
                reserved.name()
            )));
        }

        let output: Vec<Expr> = df
            .schema()
            .fields()
            .iter()
            .map(|f| ident(f.name()))
            .collect();
        let mut src = df;
        for (i, field) in source_fields.iter().enumerate() {
            let expr = self.value_expr(field)?;
            let data_type = expr.get_type(src.schema())?;
            src = src.with_column(
                &format!("{CORRELATE_KEY_PREFIX}{i}"),
                canonical_key(expr, &data_type),
            )?;
        }
        if !self.aggregated
            && src
                .schema()
                .has_column_with_unqualified_name(self.source.time_col)
        {
            let ts_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
            let start = cast(cast(col(self.source.time_col), ts_type), DataType::Int64);
            src = src.with_column(CORRELATE_START, start)?;
            let span_duration = span_duration_column(self.source, self.resolver)
                .filter(|name| src.schema().has_column_with_unqualified_name(name));
            if let Some(duration) = span_duration {
                src =
                    src.with_column(CORRELATE_DURATION, cast(ident(duration), DataType::Int64))?;
            }
        }
        let schema = src.schema().as_arrow().clone();
        let batches = src
            .limit(0, Some(max_source_rows.saturating_add(1)))?
            .collect()
            .await?;
        if batches.iter().map(RecordBatch::num_rows).sum::<usize>() > max_source_rows {
            return Err(QuerierError::ResourceExhausted(format!(
                "the correlate source relation has more than {max_source_rows} rows \
                 ([querier].correlate_max_source_rows); narrow the source with topk, limit or \
                 where"
            )));
        }

        let mut keys = vec![BTreeSet::new(); source_fields.len()];
        for batch in &batches {
            for (i, set) in keys.iter_mut().enumerate() {
                let values = batch
                    .column_by_name(&format!("{CORRELATE_KEY_PREFIX}{i}"))
                    .and_then(|c| c.as_string_opt::<i32>())
                    .ok_or_else(|| internal("correlate key column is not Utf8".into()))?;
                set.extend(values.iter().flatten().map(str::to_string));
            }
        }
        let (start, end) =
            source_envelope(&batches).unwrap_or((doc_window.start_ns, doc_window.end_ns));
        let widening = correlate.window.clone().unwrap_or_default();
        let widen = |d: Option<String>| d.as_deref().and_then(parse_duration_ns).unwrap_or(0);
        let target_window = ResolvedWindow {
            start_ns: start.saturating_sub(widen(widening.before)),
            end_ns: end.saturating_add(widen(widening.after)),
        };
        self.correlate_window = Some(target_window);

        if enrich {
            self.scope = Some(CorrelateScope {
                prefix: format!("{}.", target.plan.name),
                plan: &target.plan,
                resolver: &target.resolver,
                signal: common::discovery::signal_for_source(target.plan.name),
            });
        }
        let (schema, batches) = with_row_ordinal(&schema, batches)?;
        let source_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        let source = ctx.read_table(Arc::new(MemTable::try_new(schema, vec![batches])?))?;
        let frame = match &target.base {
            Some(base) if enrich || keys.iter().all(|set| !set.is_empty()) => Some(target_frame(
                target,
                base.clone(),
                &target_fields.iter().copied().zip(&keys).collect::<Vec<_>>(),
                &target_window,
                correlate,
                self.now_ns,
                self.demand.as_ref(),
            )?),
            _ => None,
        };
        let target_keys: Vec<Expr> = (0..keys.len())
            .map(|i| ident(format!("{CORRELATE_TARGET_KEY_PREFIX}{i}")))
            .collect();
        let on: Vec<Expr> = target_keys
            .iter()
            .enumerate()
            .map(|(i, key)| ident(format!("{CORRELATE_KEY_PREFIX}{i}")).eq(key.clone()))
            .collect();
        let joined = match frame {
            Some(frame) if enrich => {
                let fanout = correlate
                    .fanout
                    .and_then(|n| u64::try_from(n).ok())
                    .unwrap_or(DEFAULT_CORRELATE_FANOUT);
                // The key bound is per field, so a multi-field key can
                // still hold tuples no source row has; drop them before
                // ranking so an overflow is always a real one.
                let source_keys = source
                    .clone()
                    .select(
                        (0..keys.len())
                            .map(|i| ident(format!("{CORRELATE_KEY_PREFIX}{i}")))
                            .collect::<Vec<_>>(),
                    )?
                    .distinct()?;
                let frame = frame.join_on(source_keys, JoinType::LeftSemi, on.clone())?;
                let overflow = Arc::new(AtomicBool::new(false));
                let (frame, target_cols) =
                    prefixed_target(frame, &target.plan, fanout, Arc::clone(&overflow))?;
                self.correlate_fanout = Some(overflow);
                let joined = source.join_on(frame, join_type, on)?;
                let max_rows = scan.correlate_max_rows;
                // Each source row keeps at most `fanout` matches.
                let can_truncate = u64::try_from(source_rows)
                    .unwrap_or(u64::MAX)
                    .saturating_mul(fanout)
                    > u64::try_from(max_rows).unwrap_or(u64::MAX);
                return self.join_target_rows(joined, output, target_cols, max_rows, can_truncate);
            }
            Some(frame) => source.join_on(frame.select(target_keys)?.distinct()?, join_type, on)?,
            None if matches!(join_type, JoinType::LeftSemi | JoinType::Inner) => {
                source.limit(0, Some(0))?
            }
            None => source,
        };
        Ok(joined
            .sort(vec![ident(CORRELATE_ROW).sort(true, false)])?
            .select(output)?)
    }

    /// Finish an inner/left signal `correlate` over `joined` (the source rows
    /// joined to the [`prefixed_target`] frame): the source's `output`
    /// columns plus `target_cols`, in source row order then match order,
    /// under the `correlate_max_rows` cap.
    ///
    /// The join holds at most source rows × `fanout` rows. When that can
    /// exceed `max_rows` (`can_truncate`), the sort is limited to
    /// `max_rows + 1` rows, a TopK that keeps up to that many joined rows in
    /// memory without spilling; otherwise the sort runs unlimited (and may
    /// spill), since the cap can never trigger.
    fn join_target_rows(
        &mut self,
        joined: DataFrame,
        mut output: Vec<Expr>,
        target_cols: Vec<String>,
        max_rows: usize,
        can_truncate: bool,
    ) -> Result<DataFrame, QuerierError> {
        let mut sorted = joined.sort(vec![
            ident(CORRELATE_ROW).sort(true, false),
            ident(CORRELATE_RANK).sort(true, false),
        ])?;
        if can_truncate {
            sorted = sorted.limit(0, Some(max_rows.saturating_add(1)))?;
        }
        output.extend(target_cols.iter().map(ident));
        let truncated = Arc::new(AtomicBool::new(false));
        let out = super::correlate_cap::wrap_with_cap(sorted, max_rows, Arc::clone(&truncated))?
            .select(output)?;
        self.schema_cols.extend(target_cols);
        self.correlate_truncated = Some(truncated);
        Ok(out)
    }

    /// Split a reference under the correlate scope into the scope and the
    /// field name there; `None` for an unscoped name, or before `correlate`.
    /// Once a scope exists it shadows any source attribute whose key starts
    /// with its prefix (`parent.`, `<target>.`).
    fn scoped<'n>(&self, logical: &'n str) -> Option<(&CorrelateScope<'a>, &'n str)> {
        let scope = self.scope.as_ref()?;
        logical
            .strip_prefix(scope.prefix.as_str())
            .map(|field| (scope, field))
    }

    /// Resolve a scoped reference (see [`Self::scoped`]) to the expression
    /// that reads its value, once `correlate` has joined the relation — a
    /// physical column, a promoted column (coalesced with its attribute
    /// fallback, same as [`Self::promoted_column_expr`]), or an
    /// attribute-container extraction, all read against the
    /// `<prefix><physical>` columns the join produced rather than the
    /// source's own. Returns the expression plus the field's canonical type
    /// and whether that type is advisory (mirrors
    /// [`Resolved::is_advisory_type`]).
    fn scoped_field(
        &self,
        scope: &CorrelateScope<'_>,
        field: &str,
    ) -> Result<(Expr, ValueType, bool), QuerierError> {
        let prefix = scope.prefix.as_str();
        match scope.resolver.resolve("", field) {
            Some(Resolved::Column { name, value_type }) => {
                Ok((ident(format!("{prefix}{name}")), value_type, false))
            }
            Some(Resolved::JsonPath {
                key, value_type, ..
            }) => Ok((
                self.attr_expr_in(scope.plan, &key, prefix),
                value_type,
                true,
            )),
            Some(Resolved::PromotedColumn {
                name,
                key,
                value_type,
            }) => Ok((
                coalesce(vec![
                    ident(format!("{prefix}{name}")),
                    self.attr_expr_in(scope.plan, &key, prefix),
                ]),
                value_type,
                true,
            )),
            Some(Resolved::TypedAttribute {
                homes,
                promoted,
                key,
                value_type,
            }) => Ok((
                self.typed_attribute_expr(&homes, &promoted, &key, prefix),
                value_type,
                false,
            )),
            _ => Err(QuerierError::InvalidInput(format!(
                "field '{prefix}{field}' is not yet supported by correlate"
            ))),
        }
    }

    /// Lower an `extract` stage: derive typed, query-local columns from the log
    /// `body` via the bounded `ir_extract` UDF, one `with_column` per field.
    fn lower_extract(
        &mut self,
        df: DataFrame,
        extract: &Extract,
    ) -> Result<DataFrame, QuerierError> {
        let parser = match extract.parser {
            Parser::Json => "json",
            Parser::Logfmt => "logfmt",
        };
        let udf = ScalarUDF::from(ExtractUdf::new());
        // `body` is ingest's JSON-encoded form (issue #1410): decode it so
        // `json`/`logfmt` parse the actual log text, not a JSON string
        // literal wrapping it. Built once and cloned into each field's
        // `ir_extract` call as an *unmaterialized* expression rather than a
        // named hidden column deliberately: `as_fields` is query-document
        // input, and a document naming a field the same as a hidden column
        // would silently collide with it (`DataFrame::with_column`
        // overwrites in place), corrupting every later field in this stage.
        // Threading the `Expr` instead removes that name from the namespace
        // entirely, at the cost of redoing the decode once per field.
        let decoded_body = body_decode_expr("body");
        let mut df = df;
        for f in &extract.as_fields {
            let raw = udf.call(vec![decoded_body.clone(), lit(parser), lit(f.name.clone())]);
            let typed = cast(raw, arrow_type_for(&f.value_type));
            let alias = safe_ident(&f.name);
            df = df
                .with_column(&alias, typed)
                .map_err(QuerierError::QueryFailed)?;
            // Later stages resolve the logical name to this derived column.
            self.col_of.insert(f.name.clone(), alias);
            self.derived_types
                .insert(f.name.clone(), f.value_type.clone());
        }
        Ok(df)
    }

    /// The current DataFrame column name for a logical reference.
    ///
    /// Exhaustive over `Resolved` on purpose, with no wildcard arm: a
    /// promoted-attribute variant silently falling through to
    /// `safe_ident(logical)` (a name that doesn't exist in the scanned
    /// schema) is exactly the regression a wildcard here produced once
    /// already, when `Resolved::PromotedColumn` was added and every other
    /// match site over `Resolved` in this file caught the gap at compile
    /// time except this one. Adding a future variant must force a decision
    /// here too.
    fn df_col(&self, logical: &str) -> String {
        self.col_of.get(logical).cloned().unwrap_or_else(|| {
            match self.resolver.resolve("", logical) {
                Some(Resolved::Column { name, .. } | Resolved::PromotedColumn { name, .. }) => name,
                Some(
                    Resolved::JsonPath { .. }
                    | Resolved::EventAttribute { .. }
                    | Resolved::SpanEvents { .. }
                    | Resolved::SpanLinks { .. }
                    | Resolved::SpanList(_)
                    | Resolved::AttributeBag { .. }
                    | Resolved::TypedAttribute { .. },
                )
                | None => safe_ident(logical),
            }
        })
    }

    fn lower_rank(
        &mut self,
        df: DataFrame,
        of: &str,
        n: i64,
        ascending: bool,
    ) -> Result<DataFrame, QuerierError> {
        self.record_field_demand(of);
        // `value_expr`, not `df_col` — see `Stage::Order`'s comment above.
        df.sort(vec![self.value_expr(of)?.sort(ascending, false)])
            .map_err(QuerierError::QueryFailed)?
            .limit(0, Some(n.max(0) as usize))
            .map_err(QuerierError::QueryFailed)
    }

    /// A stepped aggregate buckets on `instants`' evaluation grid when given
    /// (metric series, D11), else epoch-aligned `date_bin` buckets.
    fn lower_aggregate(
        &mut self,
        mut df: DataFrame,
        agg: &Aggregate,
        instants: Option<&ResolvedWindow>,
    ) -> Result<DataFrame, QuerierError> {
        // Group expressions: each `by` field, aliased to a safe identifier.
        let mut group_exprs = Vec::new();
        let mut new_col_of = HashMap::new();
        if let Some(step) = &agg.step {
            let step_ns = parse_step(step)?;
            let ts_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
            let bucket = if let Some(w) = instants {
                // The one instant `t = from + k·step` whose `(t - step, t]`
                // holds the point: `k` is the ceiling of `(ts - from) / step`,
                // which integer division gives for every `ts > from - step`.
                // The last instant at or before `to` bounds `ts` from above.
                check_instants(w.start_ns, w.end_ns, step_ns, step_ns)?;
                let span = w.end_ns.saturating_sub(w.start_ns);
                let last = w
                    .start_ns
                    .saturating_add(span.div_euclid(step_ns) * step_ns);
                let ts = cast(
                    cast(col(self.source.time_col), ts_type.clone()),
                    DataType::Int64,
                );
                let at = lit(w.start_ns)
                    + (ts - lit(w.start_ns) + lit(step_ns - 1)) / lit(step_ns) * lit(step_ns);
                let lower = w.start_ns.saturating_sub(step_ns).saturating_add(1);
                let lower = self.time_bound(&df, lower, Operator::GtEq)?;
                let upper = self.time_bound(&df, last, Operator::LtEq)?;
                df = df
                    .filter(lower.and(upper))
                    .map_err(QuerierError::QueryFailed)?;
                cast(at, ts_type)
            } else {
                let stride = lit(ScalarValue::IntervalMonthDayNano(Some(
                    IntervalMonthDayNano::new(0, 0, step_ns),
                )));
                let origin = lit(ScalarValue::TimestampNanosecond(Some(0), None));
                date_bin(stride, cast(col(self.source.time_col), ts_type), origin)
            };
            group_exprs.push(bucket.alias("bucket"));
        }
        for by in &agg.by {
            self.record_field_demand(by);
            let alias = safe_ident(by);
            group_exprs.push(self.value_expr(by)?.alias(alias.clone()));
            new_col_of.insert(by.clone(), alias);
        }

        // Aggregate expressions.
        let mut agg_exprs = Vec::new();
        for a in &agg.aggs {
            let expr = self.agg_expr(a)?.alias(a.as_name.clone());
            agg_exprs.push(expr);
            new_col_of.insert(a.as_name.clone(), a.as_name.clone());
        }

        let df = df
            .aggregate(group_exprs, agg_exprs)
            .map_err(QuerierError::QueryFailed)?;

        self.aggregated = true;
        self.col_of = new_col_of;
        if agg.step.is_some() {
            self.series_shaped = true;
            // Deterministic order: bucket then labels.
            let mut sort = vec![col("bucket").sort(true, false)];
            for by in &agg.by {
                sort.push(ident(safe_ident(by)).sort(true, false));
            }
            return df.sort(sort).map_err(QuerierError::QueryFailed);
        }
        Ok(df)
    }

    /// `rate`/`increase`/`irate`/`*_over_time` at each evaluation instant
    /// `t = from + k·step`: every series (`series_id`) is reduced over
    /// `(t - window, t]` (`window` defaults to `step`) by the range UDAF, then
    /// the per-series values sharing a `by` group are folded by the `across`
    /// reducer (default `sum`).
    fn lower_rate_aggregate(
        &mut self,
        df: DataFrame,
        agg: &Aggregate,
        a: &common::query_ir::Agg,
        window: &ResolvedWindow,
    ) -> Result<DataFrame, QuerierError> {
        use common::query_ir::AggFn;
        let (step_ns, window_ns) = range_step_window(agg, a, window)?;
        let of = a.of.as_deref().ok_or_else(|| {
            QuerierError::InvalidInput(format!(
                "aggregate '{}' requires an `of` field",
                a.func.as_str()
            ))
        })?;
        let f = match a.func {
            AggFn::Rate => RangeFn::Rate,
            AggFn::Increase => RangeFn::Increase,
            AggFn::Irate => RangeFn::Irate,
            AggFn::AvgOverTime => RangeFn::AvgOverTime,
            AggFn::MinOverTime => RangeFn::MinOverTime,
            AggFn::MaxOverTime => RangeFn::MaxOverTime,
            AggFn::SumOverTime => RangeFn::SumOverTime,
            AggFn::CountOverTime => RangeFn::CountOverTime,
            other => {
                return Err(QuerierError::Unsupported(format!(
                    "'{}' is not a range function",
                    other.as_str()
                )));
            }
        };
        let mut groups = Vec::new();
        let mut new_col_of = HashMap::new();
        for by in &agg.by {
            self.record_field_demand(by);
            let alias = safe_ident(by);
            groups.push((self.value_expr(by)?, alias.clone()));
            new_col_of.insert(by.clone(), alias);
        }
        let eval = RangeEval {
            f,
            first_ns: window.start_ns,
            last_ns: window.end_ns,
            step_ns,
            window_ns,
        };
        const PER_SERIES: &str = "__per_series";
        let df = range_series(df, self.value_expr(of)?, &groups, &eval, PER_SERIES)?;

        let v = || ident(PER_SERIES);
        let across_expr = match a.across.unwrap_or(AggFn::Sum) {
            AggFn::Avg => avg(v()),
            AggFn::Min => min(v()),
            AggFn::Max => max(v()),
            AggFn::Count => count(v()),
            AggFn::Sum => sum(v()),
            other => {
                return Err(QuerierError::Unsupported(format!(
                    "`across` does not support '{}'",
                    other.as_str()
                )));
            }
        };
        let labels = || groups.iter().map(|(_, alias)| ident(alias));
        let keys: Vec<Expr> = std::iter::once(col("bucket")).chain(labels()).collect();
        let df = df
            .aggregate(keys, vec![across_expr.alias(a.as_name.clone())])
            .map_err(QuerierError::QueryFailed)?;

        new_col_of.insert(a.as_name.clone(), a.as_name.clone());
        self.aggregated = true;
        self.col_of = new_col_of;
        self.series_shaped = true;
        let mut sort = vec![col("bucket").sort(true, false)];
        sort.extend(labels().map(|l| l.sort(true, false)));
        df.sort(sort).map_err(QuerierError::QueryFailed)
    }

    fn lower_heatmap(
        &mut self,
        df: DataFrame,
        heatmap: &Heatmap,
    ) -> Result<DataFrame, QuerierError> {
        let step_ns = common::query_ir::parse_duration_ns(&heatmap.x.step).ok_or_else(|| {
            QuerierError::InvalidInput(format!("invalid heatmap step '{}'", heatmap.x.step))
        })?;
        let y_type = self
            .resolver
            .resolve("", &heatmap.y.of)
            .ok_or_else(|| {
                QuerierError::InvalidInput(format!("unknown heatmap field '{}'", heatmap.y.of))
            })?
            .value_type()
            .clone();
        let bounds = heatmap
            .y
            .bounds
            .iter()
            .map(|bound| {
                common::query_ir::coerce(bound, &y_type)
                    .map_err(|e| QuerierError::InvalidInput(e.to_string()))
                    .and_then(|value| match value {
                        Literal::Duration(ns) => Ok(ns),
                        _ => Err(QuerierError::InvalidInput(
                            "heatmap bounds must be duration values".into(),
                        )),
                    })
            })
            .collect::<Result<Vec<_>, QuerierError>>()?;
        // DataFusion integer division truncates negative values toward zero.
        // Offset negative timestamps before division to retain epoch floor semantics.
        let time = col(self.source.time_col);
        let time_bucket = datafusion::logical_expr::when(
            time.clone().lt(lit(0i64)),
            ((time.clone() + lit(1i64)) / lit(step_ns) - lit(1i64)) * lit(step_ns),
        )
        .otherwise((time / lit(step_ns)) * lit(step_ns))
        .map_err(QuerierError::QueryFailed)?;
        let mut duration_bucket = lit(bounds.len() as i64);
        for (index, bound) in bounds.iter().enumerate().rev() {
            duration_bucket = datafusion::logical_expr::when(
                self.value_expr(&heatmap.y.of)?.lt(lit(*bound)),
                lit(index as i64),
            )
            .otherwise(duration_bucket)
            .map_err(QuerierError::QueryFailed)?;
        }
        self.aggregated = true;
        self.col_of = HashMap::from([
            ("time_bucket_ns".into(), "time_bucket_ns".into()),
            ("duration_bucket".into(), "duration_bucket".into()),
            ("count".into(), "count".into()),
        ]);
        df.aggregate(
            vec![
                time_bucket.alias("time_bucket_ns"),
                duration_bucket.alias("duration_bucket"),
            ],
            vec![count(lit(1i64)).alias("count")],
        )
        .map_err(QuerierError::QueryFailed)?
        .sort(vec![
            col("time_bucket_ns").sort(true, false),
            col("duration_bucket").sort(true, false),
        ])
        .map_err(QuerierError::QueryFailed)
    }

    /// Lower a `histogram_quantile` or `histogram_fraction` stage: the
    /// statistic of each group's merged histogram at every evaluation
    /// instant `from + k·step`, each series reduced over `(t - window, t]`
    /// first (`window` defaults to `step`; its latest point, or in rate mode
    /// its increase). The output is shaped like `lower_aggregate`'s `step`
    /// output (`bucket`, label columns, one value).
    /// Groups stay per metric, but the Series is labelled by `by` alone.
    /// `per_series` groups by each series' label set instead and yields the
    /// Series frame (`bucket`, `__labels`, `value`).
    fn lower_histogram(
        &mut self,
        df: DataFrame,
        h: &HistStage<'_>,
        window: &ResolvedWindow,
    ) -> Result<DataFrame, QuerierError> {
        let (step_ns, window_ns) = histogram_step_window(h)?;
        let by_aliases: Vec<String> = h.by.iter().map(|by| safe_ident(by)).collect();
        let mut groups = vec![(col("metric_name"), "metric_name".to_string())];
        for (by, alias) in h.by.iter().zip(&by_aliases) {
            groups.push((self.value_expr(by)?, alias.clone()));
        }
        let eval = HistEval {
            stat: h.stat,
            mode: match h.mode {
                HistogramMode::Rate => Mode::Rate,
                HistogramMode::Instant => Mode::Instant,
            },
            first_ns: window.start_ns,
            last_ns: window.end_ns,
            step_ns,
            window_ns,
            offset_ns: 0,
        };
        self.aggregated = true;
        self.series_shaped = true;
        if h.per_series {
            let labels = metric_series::labels::series_labels_of(&self.schema_cols);
            let groups = [(labels, metric_series::labels::LABELS_COLUMN.to_string())];
            self.col_of = HashMap::from([(h.as_name.to_string(), "value".to_string())]);
            let df = histogram_series(df, &groups, &eval, "value")?;
            return metric_series::histogram_per_series(df);
        }
        let df = histogram_series(df, &groups, &eval, h.as_name)?;
        let mut new_col_of = HashMap::new();
        for (by, alias) in h.by.iter().zip(&by_aliases) {
            new_col_of.insert(by.clone(), alias.clone());
        }
        new_col_of.insert(h.as_name.to_string(), h.as_name.to_string());
        self.col_of = new_col_of;

        let mut sort = vec![
            col("bucket").sort(true, false),
            col("metric_name").sort(true, false),
        ];
        for alias in &by_aliases {
            sort.push(ident(alias.clone()).sort(true, false));
        }
        df.sort(sort)
            .and_then(|df| df.drop_columns(&["metric_name"]))
            .map_err(QuerierError::QueryFailed)
    }

    fn agg_expr(&self, a: &common::query_ir::Agg) -> Result<Expr, QuerierError> {
        use common::query_ir::AggFn;
        // The rate family's `of` is a metric value column, never a typed
        // attribute: `lower_rate_aggregate` deliberately records no demand
        // for it.
        if let Some(of) = a.of.as_deref() {
            self.record_field_demand(of);
        }
        let expr = match a.func {
            AggFn::Count => count(lit(1i64)),
            AggFn::Sum => sum(self.numeric_of(a)?),
            AggFn::Avg => avg(self.numeric_of(a)?),
            AggFn::Min => min(self.value_expr(a.of.as_deref().unwrap_or_default())?),
            AggFn::Max => max(self.value_expr(a.of.as_deref().unwrap_or_default())?),
            AggFn::Quantile => {
                let q = a.arg.unwrap_or(0.5);
                approx_percentile_cont(self.numeric_of(a)?.sort(true, false), lit(q), None)
            }
            AggFn::Stddev => stddev_pop(self.numeric_of(a)?),
            AggFn::Stdvar => var_pop(self.numeric_of(a)?),
            // HyperLogLog; ignores nulls. Cast to `Int64` below.
            AggFn::CountDistinct => {
                approx_distinct(self.value_expr(a.of.as_deref().unwrap_or_default())?)
            }
            // Population, not sample: LogQL's stddev_over_time/stdvar_over_time
            // describe the window they were given rather than estimating a
            // wider distribution from it, and the compat path already uses the
            // population form.
            // Ordered by the source's own time column — `timestamp` on most
            // sources, `start_time_unix_nano` on traces — so "first"/"last"
            // mean earliest/latest rather than whatever order the scan
            // produced. Both order *ascending*: `first_value`/`last_value`
            // pick the value at the start/end of the ordered frame, so
            // ascending time puts the earliest row first and the latest row
            // last — the same order both functions share, only which end of
            // it they read differs. (`last_value` ordered *descending* —
            // the bug this fixes — put the earliest row last too, so `last`
            // silently returned the same value `first` did.)
            AggFn::First => first_value(
                self.value_expr(a.of.as_deref().unwrap_or_default())?,
                vec![SortExpr::new(col(self.source.time_col), true, true)],
            ),
            AggFn::Last => last_value(
                self.value_expr(a.of.as_deref().unwrap_or_default())?,
                vec![SortExpr::new(col(self.source.time_col), true, true)],
            ),
            // Intercepted in `lower_aggregate` before reaching `agg_expr` —
            // computed from a window over the raw samples, not a plain
            // aggregate expression.
            AggFn::Rate
            | AggFn::Increase
            | AggFn::Irate
            | AggFn::AvgOverTime
            | AggFn::MinOverTime
            | AggFn::MaxOverTime
            | AggFn::SumOverTime
            | AggFn::CountOverTime => {
                return Err(QuerierError::InvalidInput(format!(
                    "aggregate '{}' must be lowered by lower_rate_aggregate",
                    a.func.as_str()
                )));
            }
        };

        // A scoping predicate narrows this aggregate alone, as a per-aggregate
        // FILTER on the one grouping — not a `Filter` node, which would narrow
        // every aggregate in the stage and drop groups with no matching row.
        // The scope lowers through `lower_predicate`, so it inherits the same
        // Kleene absent-value semantics a `where` stage has: a row whose field
        // is NULL satisfies neither the scope nor its negation.
        //
        // This must happen before the divisor: `.filter()` builds on an
        // aggregate function expression, and dividing first hands it an
        // arithmetic node it cannot attach to.
        let expr = match &a.scope {
            None => expr,
            Some(scope) => expr
                .filter(self.lower_predicate(scope)?)
                .build()
                .map_err(QuerierError::QueryFailed)?,
        };
        // `approx_distinct` returns `UInt64`; `validate` declares `Int64`.
        // The cast wraps the filtered aggregate because `.filter()` only
        // builds on a bare `Expr::AggregateFunction`, not on a `Cast`.
        let expr = if a.func == AggFn::CountDistinct {
            cast(expr, DataType::Int64)
        } else {
            expr
        };
        // `divisor` reports the aggregate per unit rather than absolute — the
        // whole of what a rate is. It divides whatever the aggregate produced,
        // scoped or not.
        Ok(match a.divisor {
            None => expr,
            Some(d) => expr / lit(d),
        })
    }

    /// The `of` field of an aggregate as a numeric expression. A legacy
    /// resolution (`JsonPath`/`Column`/...) has no canonical-type guarantee,
    /// so it casts to `Float64` — a non-numeric-looking value becomes NULL
    /// rather than failing the whole aggregate. A `TypedAttribute`'s home
    /// column is already the writer-committed numeric Arrow type: no cast,
    /// same "no cast off a typed home" rule as every predicate operator in
    /// `lower_leaf`. A non-numeric `TypedAttribute` (e.g. `String`) never
    /// reaches here — `validate`'s numeric-operand check already rejects it,
    /// since `TypedAttribute` is never advisory (see `Resolved::
    /// is_advisory_type`).
    fn numeric_of(&self, a: &common::query_ir::Agg) -> Result<Expr, QuerierError> {
        let of = a.of.as_deref().ok_or_else(|| {
            QuerierError::InvalidInput(format!("aggregate '{}' requires a field", a.func.as_str()))
        })?;
        let resolved = match self.scoped(of) {
            Some((scope, field)) => scope.resolver.resolve("", field),
            None => self.resolver.resolve("", of),
        };
        if matches!(resolved, Some(Resolved::TypedAttribute { .. })) {
            return self.value_expr(of);
        }
        Ok(cast(self.value_expr(of)?, DataType::Float64))
    }

    /// Lower a logical field to the expression that reads its value.
    fn value_expr(&self, logical: &str) -> Result<Expr, QuerierError> {
        // Post-aggregate references address the current DataFrame column.
        if let Some(c) = self.col_of.get(logical) {
            return Ok(ident(c.clone()));
        }
        if let Some((scope, field)) = self.scoped(logical) {
            let (expr, ..) = self.scoped_field(scope, field)?;
            return Ok(expr);
        }
        match self.resolver.resolve("", logical) {
            // `body` decodes the same way the projection does (issue #1433):
            // grouping, ordering, and an aggregate operand must agree with
            // what a `rows` result of the same field shows.
            Some(Resolved::Column { name, .. }) if is_body_column(&name) => {
                Ok(body_decode_expr(&name))
            }
            Some(Resolved::Column { name, .. }) => Ok(ident(name)),
            Some(Resolved::JsonPath { key, .. }) => Ok(self.attr_expr(&key)),
            Some(Resolved::EventAttribute {
                events_column,
                event_name,
                key,
                ..
            }) => Ok(self.event_attr_expr(&events_column, &event_name, &key)),
            Some(Resolved::SpanEvents { events_column }) => Ok(span_events_expr(&events_column)),
            Some(Resolved::SpanLinks { links_column }) => Ok(span_links_expr(&links_column)),
            Some(Resolved::SpanList(_)) => Err(span_list_filter_only(logical)),
            Some(Resolved::PromotedColumn { name, key, .. }) => {
                Ok(self.promoted_column_expr(&name, &key))
            }
            Some(Resolved::TypedAttribute {
                homes,
                promoted,
                key,
                ..
            }) => Ok(self.typed_attribute_expr(&homes, &promoted, &key, "")),
            // Retrieval-only, like `SpanEvents`; `is_filterable` rejects it
            // as a value position at validation time, so unreachable here.
            Some(Resolved::AttributeBag { container }) => Err(QuerierError::InvalidInput(format!(
                "field '{logical}' (container '{container}') is retrieval-only"
            ))),
            None => Err(QuerierError::InvalidInput(format!(
                "field '{logical}' has no canonical type"
            ))),
        }
    }

    /// Extract an attribute value, coalescing over the source's containers that
    /// are present in the scanned schema.
    fn attr_expr(&self, key: &str) -> Expr {
        self.attr_expr_in(self.source, key, "")
    }

    /// Shared implementation of [`Self::attr_expr`]/[`Self::scoped_field`]:
    /// extract an attribute value, coalescing over `plan`'s containers
    /// present in the scanned schema. `prefix` is empty for the source side
    /// and the correlate scope's prefix for a scoped reference — the
    /// container column addressed is `<prefix><container>` either way.
    /// Always built with `ident()`, never `col()`: a prefixed container
    /// name contains a `.` that must not be parsed as a qualifier, and an
    /// unprefixed name has no qualifier to parse regardless.
    fn attr_expr_in(&self, plan: &SourcePlan, key: &str, prefix: &str) -> Expr {
        // An explicit container qualifier reads that container only. Checked
        // before the coalesce so `resource.x` and `log.x` stay distinguishable
        // when the same key exists at both scopes.
        if let Some((container, bare)) = strip_scope_qualifier(plan.attr_prefixes, key) {
            let container = format!("{prefix}{container}");
            return self.attr_expr_for_container(&container, bare);
        }
        let mut parts: Vec<Expr> = plan
            .containers
            .iter()
            .map(|c| format!("{prefix}{c}"))
            .filter(|c| self.is_typed_container(c))
            .map(|c| self.attr_expr_for_container(&c, key))
            .collect();
        match parts.len() {
            0 => lit(ScalarValue::Utf8(None)),
            1 => parts.remove(0),
            _ => coalesce(parts),
        }
    }

    /// Read `key` from `container_col` (already prefixed for the parent
    /// side, if applicable) through the typed-layout coalesce
    /// (`typed_compat_attr_expr`), or a NULL literal when the scanned schema
    /// has no such container (a qualifier for a container this source
    /// doesn't have).
    fn attr_expr_for_container(&self, container_col: &str, key: &str) -> Expr {
        if self.is_typed_container(container_col) {
            typed_compat_attr_expr(container_col, key)
        } else {
            lit(ScalarValue::Utf8(None))
        }
    }

    /// Whether `container_col` is on the typed layout in the scanned
    /// schema — its residue column is present among `schema_cols`.
    fn is_typed_container(&self, container_col: &str) -> bool {
        has_typed_container(self.schema_cols.iter().map(String::as_str), container_col)
    }

    /// The projection expression for a resolved physical column: `body` is
    /// decoded (see [`body_decode_expr`]), everything else projected as-is.
    /// A typed-layout attribute container never reaches here as a
    /// `Resolved::Column` — it resolves to `Resolved::AttributeBag` instead
    /// (see [`attribute_bag_expr`]).
    fn column_projection_expr(&self, physical: &str) -> Expr {
        if is_body_column(physical) {
            body_decode_expr(physical).alias(physical)
        } else {
            ident(physical)
        }
    }

    /// Read a [`Resolved::PromotedColumn`]: `name` may still be NULL in a
    /// file the compactor hasn't backfilled since promotion (#816), so
    /// coalesce it with the plain JSON-path extraction of `key` rather than
    /// trusting the column alone.
    fn promoted_column_expr(&self, name: &str, key: &str) -> Expr {
        coalesce(vec![ident(name), self.attr_expr(key)])
    }

    /// Read a [`Resolved::TypedAttribute`]: `prefix` is `""` for the child
    /// side and [`PARENT_COLUMN_PREFIX`] for a `parent.`-scoped reference —
    /// see [`typed_home_expr`] for the expression itself.
    fn typed_attribute_expr(
        &self,
        homes: &[String],
        promoted: &[Option<String>],
        key: &str,
        prefix: &str,
    ) -> Expr {
        typed_home_expr(homes, promoted, key, prefix)
    }

    /// `resolved`'s typed-attribute parts, when it has at least one promoted
    /// column — the shape [`lower_leaf`] needs to rewrite a filter
    /// comparison through [`typed_home_filter_expr`] instead of the plain
    /// coalesce. `None` for every other resolution, or a promoted-free
    /// `TypedAttribute` (the coalesce form is already pushdown-friendly
    /// there).
    fn promoted_typed_attribute(
        resolved: Option<Resolved>,
        prefix: &str,
    ) -> Option<TypedAttrFilterParts> {
        match resolved {
            Some(Resolved::TypedAttribute {
                homes,
                promoted,
                key,
                ..
            }) if promoted.iter().any(Option::is_some) => {
                Some((homes, promoted, key, prefix.to_string()))
            }
            _ => None,
        }
    }

    /// [`typed_home_filter_expr`] over `parts`' unpacked homes/promoted/key/
    /// prefix — the one-line call every OR-rewrite site in [`Self::lower_leaf`]
    /// and [`Self::ordered`] shares.
    fn typed_attr_filter_expr(parts: &TypedAttrFilterParts, op: Operator, literal: Expr) -> Expr {
        let (homes, promoted, key, prefix) = parts;
        typed_home_filter_expr(homes, promoted, key, prefix, op, literal)
    }

    /// Record per-level attribute-promotion demand for a field used in a
    /// filter, grouping, ordering, aggregate-operand or `fields` position
    /// (change: otel-native-schema layer 6): only a
    /// [`Resolved::TypedAttribute`] with at least one committed home counts
    /// — a key with no committed type has nothing to promote. One hit per
    /// (signal, level, key) per document, whichever level(s) the resolved
    /// homes actually read (an unqualified reference coalescing more than
    /// one level counts each of them).
    fn record_attr_demand(&self, signal: &'static str, resolved: &Resolved) {
        let Some(scope) = &self.demand else { return };
        let Resolved::TypedAttribute { homes, key, .. } = resolved else {
            return;
        };
        for home in homes {
            let Some((container, _)) = typed_attributes::canonical_of_home_column(home) else {
                continue;
            };
            let level = typed_attributes::container_level(container);
            let first_hit = scope
                .seen
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert((signal, level, key.clone()));
            if first_hit {
                common::attr_demand::record_level(
                    scope.tenant_slug,
                    scope.dataset_slug,
                    signal,
                    level,
                    key,
                );
            }
        }
    }

    /// Resolve `logical` and record its attribute demand, for a filter,
    /// grouping, ordering, aggregate-operand or `fields` position. A name
    /// under the correlate scope records under the scope's signal (the
    /// target's for `<target>.`, the source's for `parent.`); any other name
    /// under the source signal. Extract-derived fields and aggregate aliases
    /// ([`Self::col_of`]) are not attributes and record nothing.
    fn record_field_demand(&self, logical: &str) {
        if self.col_of.contains_key(logical) {
            return;
        }
        let (signal, resolved) = match self.scoped(logical) {
            Some((scope, field)) => (scope.signal, scope.resolver.resolve("", field)),
            None => (
                self.demand.as_ref().map(|demand| demand.signal),
                self.resolver.resolve("", logical),
            ),
        };
        if let (Some(signal), Some(resolved)) = (signal, resolved) {
            self.record_attr_demand(signal, &resolved);
        }
    }

    /// Extract one attribute from a named span event (see
    /// `Resolved::EventAttribute`), via the `ir_event_attr` UDF.
    fn event_attr_expr(&self, events_column: &str, event_name: &str, key: &str) -> Expr {
        let udf = ScalarUDF::from(EventAttrUdf::new());
        udf.call(vec![
            col(events_column),
            lit(event_name.to_string()),
            lit(key.to_string()),
        ])
    }

    fn lower_predicate(&self, pred: &Predicate) -> Result<Expr, QuerierError> {
        match pred {
            Predicate::Leaf(leaf) => self.lower_leaf(leaf),
            Predicate::Not(p) => Ok(not(self.lower_predicate(p)?)),
            Predicate::And(preds) => {
                let exprs = preds
                    .iter()
                    .map(|p| self.lower_predicate(p))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(exprs
                    .into_iter()
                    .reduce(Expr::and)
                    .unwrap_or_else(|| lit(true)))
            }
            Predicate::Or(preds) => {
                let exprs = preds
                    .iter()
                    .map(|p| self.lower_predicate(p))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(exprs
                    .into_iter()
                    .reduce(Expr::or)
                    .unwrap_or_else(|| lit(false)))
            }
        }
    }

    fn lower_leaf(&self, leaf: &Leaf) -> Result<Expr, QuerierError> {
        // An extract-derived or aggregate-output column takes precedence over
        // registry resolution (it is a real DataFrame column now).
        let (is_json, value_type, field_expr, is_body, untyped, typed_attr) = if let Some(alias) =
            self.col_of.get(&leaf.field)
        {
            let ty = self
                .derived_types
                .get(&leaf.field)
                .cloned()
                .unwrap_or(ValueType::String);
            (false, ty, ident(alias.clone()), false, false, None)
        } else if let Some((scope, field)) = self.scoped(&leaf.field) {
            let (expr, ty, advisory) = self.scoped_field(scope, field)?;
            let scope_resolved = scope.resolver.resolve("", field);
            if let (Some(signal), Some(resolved)) = (scope.signal, &scope_resolved) {
                self.record_attr_demand(signal, resolved);
            }
            let typed_attr = Self::promoted_typed_attribute(scope_resolved, &scope.prefix);
            (advisory, ty, expr, false, advisory, typed_attr)
        } else {
            let resolved = self.resolver.resolve("", &leaf.field).ok_or_else(|| {
                QuerierError::InvalidInput(format!("unknown field '{}'", leaf.field))
            })?;
            let is_json = resolved.is_advisory_type();
            let ty = resolved.value_type().clone();
            // `has_declared_type` is false when the logical schema has no
            // entry for the field — whether or not a promoted `label_*`
            // column exists (unlike `is_known`, which treats a promoted
            // column as "known"). No declared type means its `String` type
            // is a hardcoded default, not a declared one. That is the
            // "untyped" case `ordered` needs, distinct from a field the
            // schema registry explicitly declares as `String`. A
            // `TypedAttribute` is never "untyped" this way even when the
            // logical schema doesn't declare it — its type is the writer's
            // committed canonical type, not a permissive-fallback default.
            let untyped = !self.resolver.has_declared_type(&leaf.field)
                && !matches!(&resolved, Resolved::TypedAttribute { .. });
            if let Some(demand) = &self.demand {
                self.record_attr_demand(demand.signal, &resolved);
            }
            // The physical `body` column is JSON-encoded at ingest (issue
            // #1410): a plain-string body is stored quoted. `eq`/`ne`/`in`
            // stay pushdown-friendly by JSON-encoding the *literal* instead
            // (see `body_eq_candidates`, below); every other operator that
            // touches `body` decodes the column instead (`Exists` is the
            // one exception, using raw `is_not_null`, which is equivalent
            // since the decode UDF preserves nulls), because none of them
            // generalise to literal-encoding (issue #1433).
            let is_body =
                matches!(&resolved, Resolved::Column { name, .. } if is_body_column(name));
            let expr = match &resolved {
                Resolved::Column { name, .. } => ident(name.clone()),
                Resolved::JsonPath { key, .. } => self.attr_expr(key),
                Resolved::EventAttribute {
                    events_column,
                    event_name,
                    key,
                    ..
                } => self.event_attr_expr(events_column, event_name, key),
                Resolved::SpanEvents { events_column } => span_events_expr(events_column),
                Resolved::SpanLinks { links_column } => span_links_expr(links_column),
                Resolved::SpanList(f) => return self.lower_span_list_leaf(leaf, f),
                Resolved::PromotedColumn { name, key, .. } => self.promoted_column_expr(name, key),
                Resolved::TypedAttribute {
                    homes,
                    promoted,
                    key,
                    ..
                } => self.typed_attribute_expr(homes, promoted, key, ""),
                // Retrieval-only, unreachable for a validated document (see
                // the `value_expr` arm above); a NULL literal, not a panic.
                Resolved::AttributeBag { .. } => lit(ScalarValue::Utf8(None)),
            };
            let typed_attr = Self::promoted_typed_attribute(Some(resolved), "");
            (is_json, ty, expr, is_body, untyped, typed_attr)
        };
        // The decoded form of `field_expr`, used by every operator except
        // `eq`/`ne`/`in` (which compare against the encoded literal instead,
        // so Parquet predicate pushdown still applies to the hot path).
        let decoded_field_expr = || {
            if is_body {
                body_decode_expr("body")
            } else {
                field_expr.clone()
            }
        };

        let coerce_val = |v: &serde_json::Value, ty: &ValueType| -> Result<Literal, QuerierError> {
            coerce(v, ty)
                .map_err(|e| QuerierError::InvalidInput(format!("field '{}': {e}", leaf.field)))
        };

        Ok(match leaf.op {
            ComparisonOp::Exists => field_expr.is_not_null(),
            ComparisonOp::Eq => {
                let v = self.require_value(leaf)?;
                let literal = coerce_val(v, &value_type)?;
                if is_body {
                    field_expr.in_list(body_eq_candidates(&string_of(&literal)), false)
                } else if let Some(parts) = &typed_attr {
                    Self::typed_attr_filter_expr(
                        parts,
                        Operator::Eq,
                        self.value_lit(&literal, is_json),
                    )
                } else {
                    field_expr.eq(self.value_lit(&literal, is_json))
                }
            }
            ComparisonOp::Ne => {
                let v = self.require_value(leaf)?;
                let literal = coerce_val(v, &value_type)?;
                if is_body {
                    field_expr.in_list(body_eq_candidates(&string_of(&literal)), true)
                } else if let Some(parts) = &typed_attr {
                    Self::typed_attr_filter_expr(
                        parts,
                        Operator::NotEq,
                        self.value_lit(&literal, is_json),
                    )
                } else {
                    field_expr.not_eq(self.value_lit(&literal, is_json))
                }
            }
            ComparisonOp::Gt | ComparisonOp::Gte | ComparisonOp::Lt | ComparisonOp::Lte => self
                .ordered(
                    leaf,
                    decoded_field_expr(),
                    &value_type,
                    is_json,
                    untyped,
                    typed_attr.as_ref(),
                )?,
            ComparisonOp::Contains => {
                let v = self.require_value(leaf)?;
                let s = coerce_val(v, &ValueType::String)?;
                contains(decoded_field_expr(), self.value_lit(&s, true))
            }
            ComparisonOp::Regex => {
                let v = self.require_value(leaf)?;
                let s = string_of(&coerce_val(v, &ValueType::String)?);
                compile_regex_guard(&s)?;
                regexp_like(decoded_field_expr(), lit(s), None)
            }
            ComparisonOp::In => {
                let arr = leaf
                    .value
                    .as_ref()
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| QuerierError::InvalidInput("`in` needs an array".to_string()))?;
                let list = if is_body {
                    arr.iter()
                        .map(|item| {
                            Ok(body_eq_candidates(&string_of(&coerce_val(
                                item,
                                &value_type,
                            )?)))
                        })
                        .collect::<Result<Vec<Vec<Expr>>, QuerierError>>()?
                        .into_iter()
                        .flatten()
                        .collect()
                } else {
                    arr.iter()
                        .map(|item| Ok(self.value_lit(&coerce_val(item, &value_type)?, is_json)))
                        .collect::<Result<Vec<_>, QuerierError>>()?
                };
                field_expr.in_list(list, false)
            }
            ComparisonOp::Between => {
                let arr = leaf
                    .value
                    .as_ref()
                    .and_then(|v| v.as_array())
                    .filter(|a| a.len() == 2)
                    .ok_or_else(|| {
                        QuerierError::InvalidInput("`between` needs a 2-element array".to_string())
                    })?;
                let lo = self.value_lit(&coerce_val(&arr[0], &value_type)?, is_json);
                let hi = self.value_lit(&coerce_val(&arr[1], &value_type)?, is_json);
                let field_expr = decoded_field_expr();
                field_expr.clone().gt_eq(lo).and(field_expr.lt_eq(hi))
            }
        })
    }

    /// Lower a predicate on an `events`/`links` element field to a
    /// [`SpanListMatchUdf`] call: true when any element satisfies the leaf,
    /// false (never NULL) for NULL or malformed JSON.
    fn lower_span_list_leaf(
        &self,
        leaf: &Leaf,
        field: &SpanListField,
    ) -> Result<Expr, QuerierError> {
        // Link ids are stored as lowercase hex.
        let lower = matches!(
            field,
            SpanListField::LinkTraceId | SpanListField::LinkSpanId
        );
        let raw_text = |v: &serde_json::Value| {
            coerce(v, &ValueType::String)
                .map(|l| string_of(&l))
                .map_err(|e| QuerierError::InvalidInput(format!("field '{}': {e}", leaf.field)))
        };
        let text =
            |v: &serde_json::Value| raw_text(v).map(|t| if lower { t.to_lowercase() } else { t });
        let op = match leaf.op {
            ComparisonOp::Exists => SpanListOp::Exists,
            ComparisonOp::Eq => SpanListOp::Eq(text(self.require_value(leaf)?)?),
            ComparisonOp::Contains => SpanListOp::Contains(text(self.require_value(leaf)?)?),
            ComparisonOp::Regex => {
                let pattern = raw_text(self.require_value(leaf)?)?;
                SpanListOp::Regex(CompiledRegex(compile_regex_guard(&pattern)?, pattern))
            }
            ComparisonOp::In => {
                let items = leaf
                    .value
                    .as_ref()
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| QuerierError::InvalidInput("`in` needs an array".to_string()))?;
                SpanListOp::In(items.iter().map(text).collect::<Result<_, _>>()?)
            }
            _ => {
                return Err(QuerierError::InvalidInput(format!(
                    "operator '{}' is not supported on list field '{}'",
                    leaf.op.as_str(),
                    leaf.field
                )));
            }
        };
        let udf = SpanListMatchUdf::new(field.clone(), op);
        Ok(ScalarUDF::from(udf).call(vec![col(field.column())]))
    }

    fn require_value<'v>(&self, leaf: &'v Leaf) -> Result<&'v serde_json::Value, QuerierError> {
        leaf.value.as_ref().ok_or_else(|| {
            QuerierError::InvalidInput(format!("operator '{}' requires a value", leaf.op.as_str()))
        })
    }

    fn ordered(
        &self,
        leaf: &Leaf,
        field_expr: Expr,
        value_type: &ValueType,
        is_json: bool,
        untyped: bool,
        typed_attr: Option<&TypedAttrFilterParts>,
    ) -> Result<Expr, QuerierError> {
        let field = leaf.field.as_str();
        let op = leaf.op;
        let value = self.require_value(leaf)?;
        // An untyped attribute (no declared logical type — resolved as
        // `String` only by the resolver's permissive fallback, see
        // `lower_leaf`) compared against a JSON *number* literal: comparing
        // lexicographically would silently misorder ("9" > "10") since the
        // attribute's actual type is unknown. Route it through a numeric
        // `TRY_CAST` instead — a non-numeric-looking value casts to NULL and
        // so never matches, same as `Predicate::evaluate`'s unparsable-string
        // case (see `query_ir::predicate::eval_leaf`). A declared `String`
        // field keeps the lexicographic comparison: its type was a choice,
        // not a fallback default.
        // An untyped attribute (no declared logical type — resolved as
        // `String` only by the resolver's permissive fallback, see
        // `lower_leaf`) compared against a JSON *number* literal: comparing
        // lexicographically would silently misorder ("9" > "10") since the
        // attribute's actual type is unknown. Route it through a numeric
        // `TRY_CAST` instead — a non-numeric-looking value casts to NULL and
        // so never matches, same as `Predicate::evaluate`'s unparsable-string
        // case (see `query_ir::predicate::eval_leaf`). A declared `String`
        // field keeps the lexicographic comparison below: its type was a
        // choice, not a fallback default.
        let (lhs, rhs) = if !is_numeric(value_type)
            && untyped
            && let Some(f) = value.as_f64()
        {
            (try_cast(field_expr, DataType::Float64), lit(f))
        } else if is_numeric(value_type) {
            // Route on the resolved ValueType, not the storage form, so a
            // field compares the same whether promoted (typed column) or
            // unpromoted (Utf8 attribute extraction) — promotion invariance.
            // A numeric type compares numerically in both cases (an
            // attribute's Utf8 value is cast to Float64); a string type
            // compares lexically in both.
            let literal = coerce(value, value_type)
                .map_err(|e| QuerierError::InvalidInput(format!("field '{field}': {e}")))?;
            if is_json {
                (
                    cast(field_expr, DataType::Float64),
                    lit(literal_as_f64(&literal)),
                )
            } else {
                (field_expr, self.value_lit(&literal, false))
            }
        } else {
            let literal = coerce(value, value_type)
                .map_err(|e| QuerierError::InvalidInput(format!("field '{field}': {e}")))?;
            (field_expr, self.value_lit(&literal, is_json))
        };
        // A promoted `TypedAttribute` is never `is_json`/`untyped` (see
        // `lower_leaf`), so `lhs` above is always the plain `field_expr`
        // (the coalesce `typed_attribute_expr` built) — safe to discard in
        // favor of the homes-based rewrite, which gives DataFusion a
        // prunable disjunct on the promoted column instead.
        if let Some(parts) = typed_attr {
            let df_op = match op {
                ComparisonOp::Gt => Operator::Gt,
                ComparisonOp::Gte => Operator::GtEq,
                ComparisonOp::Lt => Operator::Lt,
                ComparisonOp::Lte => Operator::LtEq,
                _ => unreachable!(),
            };
            return Ok(Self::typed_attr_filter_expr(parts, df_op, rhs));
        }
        Ok(match op {
            ComparisonOp::Gt => lhs.gt(rhs),
            ComparisonOp::Gte => lhs.gt_eq(rhs),
            ComparisonOp::Lt => lhs.lt(rhs),
            ComparisonOp::Lte => lhs.lt_eq(rhs),
            _ => unreachable!(),
        })
    }

    /// A DataFusion literal for a coerced value. `as_string` forces the string
    /// form (attribute-map values are `Utf8`).
    fn value_lit(&self, literal: &Literal, as_string: bool) -> Expr {
        if as_string {
            return lit(string_of(literal));
        }
        match literal {
            Literal::String(s) => lit(s.clone()),
            Literal::Int64(i) => lit(*i),
            Literal::Float64(f) => lit(*f),
            Literal::Bool(b) => lit(*b),
            Literal::Duration(ns) => lit(*ns),
            Literal::Timestamp(ts) => lit(ScalarValue::TimestampNanosecond(
                Some(ts.resolve(self.now_ns)),
                None,
            )),
            Literal::Bytes(b) => lit(ScalarValue::Binary(Some(b.clone()))),
            Literal::Array(_) => lit(string_of(literal)),
        }
    }

    /// `plan`'s default `rows` columns present as `<prefix><column>`. A typed
    /// container has no scanned column under its own name (it's five typed
    /// columns instead) — the same `attribute_bag_expr` an explicit
    /// `{scope}.attributes` projection uses, aliased back to the container's
    /// usual name.
    fn row_default_exprs(&self, plan: &SourcePlan, prefix: &str) -> Vec<Expr> {
        plan.row_defaults
            .iter()
            .map(|c| format!("{prefix}{c}"))
            .filter(|c| self.schema_cols.contains(c) || self.is_typed_container(c))
            .map(|c| {
                if self.is_typed_container(&c) {
                    attribute_bag_expr(&c).alias(c)
                } else {
                    self.column_projection_expr(&c)
                }
            })
            .collect()
    }

    /// `keep` names columns carried past the projection (a page's sort keys).
    fn apply_projection(
        &self,
        df: DataFrame,
        doc: &Document,
        keep: &[String],
    ) -> Result<DataFrame, QuerierError> {
        // Series results are already shaped by the step aggregate. Flamegraph
        // is decoded from the full unprojected row set (samples_json/
        // stacktraces_json included) by the caller, not curated here.
        if matches!(
            doc.result,
            ResultEnvelope::Series | ResultEnvelope::Heatmap | ResultEnvelope::Flamegraph
        ) || self.series_shaped
        {
            return Ok(df);
        }
        let mut projection: Vec<Expr> = match &doc.fields {
            Some(fields) => fields
                .iter()
                .map(|f| {
                    if !self.aggregated || self.scoped(f).is_some() {
                        self.record_field_demand(f);
                    }
                    Ok(if self.col_of.contains_key(f) {
                        // Aggregate output or extract-derived column.
                        ident(self.df_col(f))
                    } else if let Some((scope, field)) = self.scoped(f) {
                        let (expr, ..) = self.scoped_field(scope, field)?;
                        expr.alias(safe_ident(f))
                    } else if self.aggregated {
                        ident(self.df_col(f))
                    } else {
                        match self.resolver.resolve("", f) {
                            Some(Resolved::Column { name, .. }) => {
                                self.column_projection_expr(&name)
                            }
                            Some(Resolved::JsonPath { key, .. }) => {
                                self.attr_expr(&key).alias(safe_ident(f))
                            }
                            Some(Resolved::EventAttribute {
                                events_column,
                                event_name,
                                key,
                                ..
                            }) => self
                                .event_attr_expr(&events_column, &event_name, &key)
                                .alias(safe_ident(f)),
                            Some(Resolved::SpanEvents { events_column }) => {
                                span_events_expr(&events_column).alias(safe_ident(f))
                            }
                            Some(Resolved::SpanLinks { links_column }) => {
                                span_links_expr(&links_column).alias(safe_ident(f))
                            }
                            Some(Resolved::SpanList(_)) => {
                                return Err(span_list_filter_only(f));
                            }
                            // #816: same treatment as `JsonPath` — the
                            // column alone isn't trustworthy until backfilled.
                            Some(Resolved::PromotedColumn { name, key, .. }) => {
                                self.promoted_column_expr(&name, &key).alias(safe_ident(f))
                            }
                            Some(Resolved::AttributeBag { container }) => {
                                attribute_bag_expr(&container).alias(safe_ident(f))
                            }
                            Some(Resolved::TypedAttribute {
                                homes,
                                promoted,
                                key,
                                ..
                            }) => self
                                .typed_attribute_expr(&homes, &promoted, &key, "")
                                .alias(safe_ident(f)),
                            None => ident(safe_ident(f)),
                        }
                    })
                })
                .collect::<Result<_, QuerierError>>()?,
            // A `table` default is the (already-curated) aggregate output,
            // plus a signal target's row defaults when a correlate joined
            // it after the aggregate.
            None if self.aggregated => {
                let Some(scope) = self.scope.as_ref().filter(|scope| {
                    scope.prefix != PARENT_COLUMN_PREFIX
                        && df
                            .schema()
                            .fields()
                            .iter()
                            .any(|f| f.name().starts_with(&scope.prefix))
                }) else {
                    return Ok(df);
                };
                let mut projection: Vec<Expr> = df
                    .schema()
                    .fields()
                    .iter()
                    .filter(|f| !f.name().starts_with(&scope.prefix))
                    .map(|f| ident(f.name()))
                    .collect();
                projection.extend(self.row_default_exprs(scope.plan, &scope.prefix));
                projection
            }
            None => {
                let mut projection = self.row_default_exprs(self.source, "");
                // A `correlate` join adds the far side's columns to the
                // default `rows` projection too — otherwise a client that
                // never named `fields` would see only the source: every
                // parent column, or a signal target's own row defaults.
                match &self.scope {
                    Some(scope) if scope.prefix == PARENT_COLUMN_PREFIX => projection.extend(
                        self.schema_cols
                            .iter()
                            .filter(|c| c.starts_with(PARENT_COLUMN_PREFIX))
                            .map(|c| ident(c.clone())),
                    ),
                    Some(scope) => {
                        projection.extend(self.row_default_exprs(scope.plan, &scope.prefix))
                    }
                    None => {}
                }
                if self.schema_cols.iter().any(|c| c == Match::SPANSETS) {
                    projection.push(ident(Match::SPANSETS));
                }
                projection
            }
        };
        projection.extend(keep.iter().map(ident));
        df.select(projection).map_err(QuerierError::QueryFailed)
    }
}

/// The `body` column (logs only), decoded through [`BodyDecodeUdf`] so a
/// plain string body comes back as the actual text rather than
/// JSON-string-quoted (issue #1410); a structured body round-trips as-is
/// (`BodyDecodeUdf`'s job, not this function's).
///
/// `pub(crate)` so `logs.rs`'s `shape_log_query` (the LogQL-compat fallback
/// path, used when the IR lowering declines) can decode `body` the same way
/// this file's own IR-path projection does — both querier-side projections
/// of `body` must decode exactly once; nothing downstream of either (the
/// Loki serializer included) may decode again.
pub(crate) fn body_decode_expr(physical: &str) -> Expr {
    ScalarUDF::from(BodyDecodeUdf::new()).call(vec![col(physical)])
}

/// Whether a resolved physical column name is the log `body` column —
/// the one column ingest JSON-encodes (issue #1410) and every read site
/// (projection, filter, ordering, grouping, aggregate operand) must
/// therefore treat specially. The single check backing all of those sites,
/// so they cannot drift out of agreement with each other (issue #1433).
fn is_body_column(physical: &str) -> bool {
    physical == "body"
}

/// The raw-storage candidates whose decode equals `text`, for `body`'s
/// `eq`/`ne`/`in`: JSON-encoding the literal (see
/// [`common::flight::conversion::encode_log_body`]) keeps these
/// pushdown-friendly (a constant `IN (...)` list, never a per-row UDF eval)
/// instead of decoding the column, but the encoded form alone is only *one*
/// raw value that decodes to `text` — [`common::flight::conversion::decode_log_body`]
/// is the identity outside a JSON string scalar, so a body whose raw text
/// already equals `text` verbatim (an object, array, number, bool, `null`,
/// or a pre-#1410 legacy non-JSON value) decodes to `text` too, without
/// ever being encoded. Include that second candidate unless `text` itself
/// looks like a JSON string (starts with `"` and parses as one): a raw
/// column equal to `text` in that case decodes to something *other* than
/// `text` (its own quotes would be stripped), so it is never a genuine
/// candidate (issue #1433 review).
fn body_eq_candidates(text: &str) -> Vec<Expr> {
    let mut candidates = vec![lit(common::flight::conversion::encode_log_body(text))];
    if common::flight::conversion::decode_log_body(text) == text {
        candidates.push(lit(text.to_string()));
    }
    candidates
}

/// [`attribute_bag_expr`]'s `named_struct` field names, in
/// [`typed_attributes::typed_fields`] order. Read positionally on the router
/// side (`common::attrs::typed::decode_typed_arrays`), so the spelling here
/// is internal — it never reaches a client.
const ATTRIBUTE_BAG_FIELD_NAMES: [&str; 5] = ["str", "int", "double", "bool", "residue"];

/// The `{scope}.attributes` raw accessor for a container on the typed
/// layout: an Arrow struct of its five typed columns
/// (`common::schema::typed_attributes::typed_columns`) — native scalars from
/// their typed home, plus the residue column for values with no typed home.
/// Tagged via `with_metadata` with `IR_TYPE_METADATA_KEY` =
/// `RAW_ATTRIBUTE_BAG_IR_TYPE`, so the router renders it as a JSON object,
/// not the legacy layout's `map<string,string>`. Built from column
/// identifiers, not `col()`, so a `parent.`-prefixed container name (a `.`
/// that must not parse as a table qualifier) still resolves. Unaliased —
/// the caller names the result column.
fn attribute_bag_expr(container_col: &str) -> Expr {
    let mut struct_args = Vec::with_capacity(2 * ATTRIBUTE_BAG_FIELD_NAMES.len());
    for (field_name, column_name) in ATTRIBUTE_BAG_FIELD_NAMES
        .iter()
        .zip(typed_columns(container_col))
    {
        struct_args.push(lit(*field_name));
        struct_args.push(ident(column_name));
    }
    with_metadata(vec![
        named_struct(struct_args),
        lit(typed_attributes::IR_TYPE_METADATA_KEY),
        lit(typed_attributes::RAW_ATTRIBUTE_BAG_IR_TYPE),
    ])
}

/// The Arrow data type an extracted field is cast to.
fn arrow_type_for(vt: &ValueType) -> DataType {
    match vt {
        ValueType::Int64 | ValueType::DurationNs => DataType::Int64,
        ValueType::Float64 => DataType::Float64,
        ValueType::Bool => DataType::Boolean,
        ValueType::TimestampNs => DataType::Timestamp(TimeUnit::Nanosecond, None),
        _ => DataType::Utf8,
    }
}

/// A scalar UDF that extracts a field from a log body string, `ir_extract(body,
/// parser, key) -> Utf8`. `body` accepts any of `Utf8`, `LargeUtf8`, or
/// `Utf8View` (DataFusion's string-view optimization). `extract` v1 supports
/// the `json` and `logfmt` parsers. Extraction is bounded per row (no
/// backtracking); a missing field yields NULL, which the IR's absent-value
/// semantics then handle.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ExtractUdf {
    signature: Signature,
}

impl ExtractUdf {
    fn new() -> Self {
        ExtractUdf {
            // `body` may arrive as any of the three UTF-8 encodings DataFusion
            // uses (plain, large-offset, or the German-string-style `Utf8View`
            // introduced for zero-copy string scans); `parser`/`key` are
            // always literal `Utf8` in practice (see `lower_extract`), so a
            // single type suffices there.
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8, DataType::Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ExtractUdf {
    fn name(&self) -> &str {
        "ir_extract"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let num_rows = args.number_rows;
        let body = BodyArg::try_from(&args.args[0])?;
        let parser = StrArg::try_from(&args.args[1])?;
        let key = StrArg::try_from(&args.args[2])?;

        // Bodies average well under 1KiB in practice; 16 bytes/row is a cheap
        // starting estimate that avoids most reallocation without over-committing.
        let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
        for i in 0..num_rows {
            let value = match (body.value_at(i), parser.value_at(i), key.value_at(i)) {
                (Some(b), Some(p), Some(k)) => extract_field(b, p, k),
                _ => None,
            };
            builder.append_option(value.as_deref());
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// A scalar UDF, `ir_body_decode(body) -> Utf8`, that undoes ingest's
/// JSON-encoding of the log `body` value (issue #1410): a stored value that
/// is a JSON string scalar decodes to its inner text; a JSON object, array,
/// number, boolean, `null`, or anything that fails to parse as JSON passes
/// through unchanged. See [`common::flight::conversion::decode_log_body`],
/// which does the actual decode — kept in `common` next to the encode it
/// inverts so the two read sites here (this UDF and, independently, the Loki
/// line serializer in `router`) share one implementation and cannot drift.
#[derive(Debug, PartialEq, Eq, Hash)]
struct BodyDecodeUdf {
    signature: Signature,
}

impl BodyDecodeUdf {
    fn new() -> Self {
        BodyDecodeUdf {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8]),
                    TypeSignature::Exact(vec![DataType::Utf8View]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for BodyDecodeUdf {
    fn name(&self) -> &str {
        "ir_body_decode"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let num_rows = args.number_rows;
        let body = BodyArg::try_from(&args.args[0])?;
        let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
        for i in 0..num_rows {
            match body.value_at(i) {
                Some(raw) => builder.append_value(common::flight::conversion::decode_log_body(raw)),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// A per-row accessor over `ir_extract`'s log-body argument, which DataFusion
/// may hand us as a scalar (constant-folded) or as any of the three UTF-8
/// array encodings its `signature()` accepts. Resolving the variant once
/// up front — instead of per row — keeps the extraction loop a single match.
enum BodyArg<'a> {
    Scalar(Option<&'a str>),
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
}

impl<'a> BodyArg<'a> {
    fn value_at(&self, i: usize) -> Option<&'a str> {
        match self {
            BodyArg::Scalar(s) => *s,
            BodyArg::Utf8(a) => (!a.is_null(i)).then(|| a.value(i)),
            BodyArg::LargeUtf8(a) => (!a.is_null(i)).then(|| a.value(i)),
            BodyArg::Utf8View(a) => (!a.is_null(i)).then(|| a.value(i)),
        }
    }
}

impl<'a> TryFrom<&'a ColumnarValue> for BodyArg<'a> {
    type Error = datafusion::error::DataFusionError;

    fn try_from(cv: &'a ColumnarValue) -> Result<Self, Self::Error> {
        match cv {
            ColumnarValue::Scalar(
                ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) | ScalarValue::Utf8View(s),
            ) => Ok(BodyArg::Scalar(s.as_deref())),
            ColumnarValue::Scalar(other) => {
                Err(datafusion::error::DataFusionError::Internal(format!(
                    "ir_extract: unsupported body scalar type {:?}",
                    other.data_type()
                )))
            }
            ColumnarValue::Array(arr) => match arr.data_type() {
                DataType::Utf8 => Ok(BodyArg::Utf8(
                    arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                        datafusion::error::DataFusionError::Internal(
                            "ir_extract: body array not Utf8".into(),
                        )
                    })?,
                )),
                DataType::LargeUtf8 => Ok(BodyArg::LargeUtf8(
                    arr.as_any()
                        .downcast_ref::<LargeStringArray>()
                        .ok_or_else(|| {
                            datafusion::error::DataFusionError::Internal(
                                "ir_extract: body array not LargeUtf8".into(),
                            )
                        })?,
                )),
                DataType::Utf8View => Ok(BodyArg::Utf8View(
                    arr.as_any()
                        .downcast_ref::<StringViewArray>()
                        .ok_or_else(|| {
                            datafusion::error::DataFusionError::Internal(
                                "ir_extract: body array not Utf8View".into(),
                            )
                        })?,
                )),
                other => Err(datafusion::error::DataFusionError::Internal(format!(
                    "ir_extract: unsupported body array type {other:?}"
                ))),
            },
        }
    }
}

/// A per-row accessor over `ir_extract`'s `parser`/`key` arguments. These are
/// always `Utf8` literals in the one call site (`lower_extract`), so the
/// scalar branch is the hot path — extracted once, with no per-row or
/// full-array allocation. The array branch exists for correctness (a
/// hypothetical column-valued parser/key) and DataFusion's `signature()`
/// coercion guarantees it arrives as plain `Utf8`.
enum StrArg<'a> {
    Scalar(Option<&'a str>),
    Array(&'a StringArray),
}

impl<'a> StrArg<'a> {
    fn value_at(&self, i: usize) -> Option<&'a str> {
        match self {
            StrArg::Scalar(s) => *s,
            StrArg::Array(a) => (!a.is_null(i)).then(|| a.value(i)),
        }
    }
}

impl<'a> TryFrom<&'a ColumnarValue> for StrArg<'a> {
    type Error = datafusion::error::DataFusionError;

    fn try_from(cv: &'a ColumnarValue) -> Result<Self, Self::Error> {
        match cv {
            ColumnarValue::Scalar(
                ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) | ScalarValue::Utf8View(s),
            ) => Ok(StrArg::Scalar(s.as_deref())),
            ColumnarValue::Scalar(other) => {
                Err(datafusion::error::DataFusionError::Internal(format!(
                    "ir_extract: expected Utf8 scalar, got {:?}",
                    other.data_type()
                )))
            }
            ColumnarValue::Array(arr) => Ok(StrArg::Array(
                arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    datafusion::error::DataFusionError::Internal(
                        "ir_extract: parser/key array not Utf8".into(),
                    )
                })?,
            )),
        }
    }
}

/// Extract a single field from a log body by `parser`. Bounded, allocation-light.
fn extract_field(body: &str, parser: &str, key: &str) -> Option<String> {
    match parser {
        "json" => {
            let v: serde_json::Value = serde_json::from_str(body).ok()?;
            json_attr_text(v.get(key)?)
        }
        "logfmt" => {
            for token in body.split_whitespace() {
                if let Some((k, val)) = token.split_once('=')
                    && k == key
                {
                    return Some(val.trim_matches('"').to_string());
                }
            }
            None
        }
        _ => None,
    }
}

/// A scalar UDF that extracts one attribute from a named span event,
/// `ir_event_attr(events, event_name, key) -> Utf8` — the mechanism behind
/// `exception.type`/`.message`/`.stacktrace`/`.escaped` resolution on the
/// `traces` source (see `EXCEPTION_EVENT_ATTRIBUTES`). `events` is the
/// stored per-span JSON array of `{name, timestamp_unix_nano,
/// attributes_json}` objects; NULL/empty/no-match all yield NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
struct EventAttrUdf {
    signature: Signature,
}

impl EventAttrUdf {
    fn new() -> Self {
        EventAttrUdf {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for EventAttrUdf {
    fn name(&self) -> &str {
        "ir_event_attr"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let num_rows = args.number_rows;
        let events = StrArg::try_from(&args.args[0])?;
        let event_name = StrArg::try_from(&args.args[1])?;
        let key = StrArg::try_from(&args.args[2])?;

        let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
        for i in 0..num_rows {
            let value = match (events.value_at(i), event_name.value_at(i), key.value_at(i)) {
                (Some(e), Some(n), Some(k)) => extract_event_attr(e, n, k),
                _ => None,
            };
            builder.append_option(value.as_deref());
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Find the first event named `event_name` in the stored `events` JSON array
/// and extract `key` from its own attributes. Tolerant by design, matching
/// `common::model::span::parse_span_events`: malformed JSON, an absent
/// event, or a missing/null attribute all yield `None` rather than an error.
fn extract_event_attr(events_json: &str, event_name: &str, key: &str) -> Option<String> {
    let events = common::model::span::parse_span_events(events_json);
    let event = events.into_iter().find(|e| e.name == event_name)?;
    json_attr_text(event.attributes.get(key)?)
}

/// A JSON attribute value as text: strings verbatim, `null` as absent,
/// anything else as its JSON text (`3`, `false`).
fn json_attr_text(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Null => None,
        other => Some(other.to_string()),
    }
}

fn span_list_filter_only(field: &str) -> QuerierError {
    QuerierError::InvalidInput(Resolved::filter_only_message(field))
}

/// The per-element matcher baked into a [`SpanListMatchUdf`] at plan time.
#[derive(Debug, PartialEq, Eq, Hash)]
enum SpanListOp {
    Exists,
    Eq(String),
    In(Vec<String>),
    Contains(String),
    Regex(CompiledRegex),
}

/// A compiled regex compared and hashed by its pattern text.
#[derive(Debug)]
struct CompiledRegex(regex::Regex, String);

impl PartialEq for CompiledRegex {
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1
    }
}
impl Eq for CompiledRegex {}
impl std::hash::Hash for CompiledRegex {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.1.hash(state);
    }
}

/// One element of a stored `events`/`links` JSON array, borrowing from the
/// row's text; only the fields a [`SpanListField`] can read are decoded.
#[derive(serde::Deserialize)]
struct SpanListElement<'a> {
    #[serde(borrow, default)]
    name: Cow<'a, str>,
    #[serde(borrow, default)]
    trace_id: Cow<'a, str>,
    #[serde(borrow, default)]
    span_id: Cow<'a, str>,
    #[serde(borrow, default)]
    attributes_json: Option<Cow<'a, str>>,
}

/// A scalar UDF, `ir_span_list_match(list_json) -> Boolean`, behind
/// `events.*`/`links.*` predicates (see `Resolved::SpanList`): true when any
/// element of the stored list satisfies the baked-in matcher on `field`.
/// NULL or malformed JSON yields false, never NULL, so `not` reads "no
/// element matches".
#[derive(Debug, PartialEq, Eq, Hash)]
struct SpanListMatchUdf {
    signature: Signature,
    field: SpanListField,
    op: SpanListOp,
}

impl SpanListMatchUdf {
    fn new(field: SpanListField, op: SpanListOp) -> Self {
        SpanListMatchUdf {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
            field,
            op,
        }
    }

    fn element_matches(&self, el: &SpanListElement<'_>) -> bool {
        let attr = |key: &str| {
            let attrs: serde_json::Value =
                serde_json::from_str(el.attributes_json.as_deref()?).ok()?;
            json_attr_text(attrs.get(key)?).map(Cow::Owned)
        };
        let text = match &self.field {
            SpanListField::EventName => Some(Cow::Borrowed(&*el.name)),
            SpanListField::LinkTraceId => Some(Cow::Borrowed(&*el.trace_id)),
            SpanListField::LinkSpanId => Some(Cow::Borrowed(&*el.span_id)),
            SpanListField::EventAttribute(k) | SpanListField::LinkAttribute(k) => attr(k),
        };
        let Some(x) = text.as_deref() else {
            return false;
        };
        match &self.op {
            SpanListOp::Exists => true,
            SpanListOp::Eq(v) => x == v,
            SpanListOp::In(vs) => vs.iter().any(|v| x == v),
            SpanListOp::Contains(v) => x.contains(v.as_str()),
            SpanListOp::Regex(re) => re.0.is_match(x),
        }
    }

    /// Whether any element of `list_json` matches, stopping at the first hit.
    fn any_match(&self, list_json: &str) -> bool {
        struct AnyMatch<'m>(&'m SpanListMatchUdf, &'m mut bool);
        impl<'de> serde::de::Visitor<'de> for AnyMatch<'_> {
            type Value = ();
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON array")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
                while let Some(raw) = seq.next_element::<&'de serde_json::value::RawValue>()? {
                    // A malformed element is skipped, so it cannot hide its
                    // siblings whatever their order.
                    let Ok(el) = serde_json::from_str::<SpanListElement<'de>>(raw.get()) else {
                        continue;
                    };
                    if self.0.element_matches(&el) {
                        // Stop reading; the unread tail makes serde_json
                        // report an error we deliberately ignore.
                        *self.1 = true;
                        return Ok(());
                    }
                }
                Ok(())
            }
        }
        if list_json.is_empty() || list_json == "[]" {
            return false;
        }
        use serde::Deserializer as _;
        let mut hit = false;
        let _ =
            serde_json::Deserializer::from_str(list_json).deserialize_seq(AnyMatch(self, &mut hit));
        hit
    }
}

impl ScalarUDFImpl for SpanListMatchUdf {
    fn name(&self) -> &str {
        "ir_span_list_match"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let list = StrArg::try_from(&args.args[0])?;
        let out: BooleanArray = (0..args.number_rows)
            .map(|i| Some(list.value_at(i).is_some_and(|l| self.any_match(l))))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

/// The `span_events` logical field: the events column through the
/// `ir_span_events` UDF (see [`SpanListJsonUdf`]).
fn span_events_expr(events_column: &str) -> Expr {
    ScalarUDF::from(SpanListJsonUdf::new(
        "ir_span_events",
        normalize_span_events,
    ))
    .call(vec![col(events_column)])
}

/// The `span_links` logical field: the links column through the
/// `ir_span_links` UDF (see [`SpanListJsonUdf`]).
fn span_links_expr(links_column: &str) -> Expr {
    ScalarUDF::from(SpanListJsonUdf::new("ir_span_links", normalize_span_links))
        .call(vec![col(links_column)])
}

/// A scalar UDF, `<name>(list) -> Utf8`, that normalizes a stored per-span
/// events or links JSON array (each element's attributes double-encoded as
/// an `attributes_json` string by the writer) into the client shape, with
/// each element's attributes as an object. NULL stays NULL; the normalizer
/// decides what an empty or malformed list becomes.
#[derive(Debug)]
struct SpanListJsonUdf {
    name: &'static str,
    normalize: fn(&str) -> Option<String>,
    signature: Signature,
}

// Identity is the UDF name: each name is built with exactly one normalizer,
// and function pointers have no meaningful equality.
impl PartialEq for SpanListJsonUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Eq for SpanListJsonUdf {}
impl std::hash::Hash for SpanListJsonUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl SpanListJsonUdf {
    fn new(name: &'static str, normalize: fn(&str) -> Option<String>) -> Self {
        SpanListJsonUdf {
            name,
            normalize,
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SpanListJsonUdf {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let num_rows = args.number_rows;
        let list = StrArg::try_from(&args.args[0])?;
        let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        for i in 0..num_rows {
            builder.append_option(list.value_at(i).and_then(self.normalize));
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// `[{name, timestamp_unix_nano, attributes}]`; malformed input yields `[]`,
/// matching `parse_span_events`' tolerance.
fn normalize_span_events(events_json: &str) -> Option<String> {
    let events: Vec<serde_json::Value> = common::model::span::parse_span_events(events_json)
        .into_iter()
        .map(|e| {
            serde_json::json!({
                "name": e.name,
                "timestamp_unix_nano": e.timestamp_unix_nano,
                "attributes": e.attributes,
            })
        })
        .collect();
    Some(serde_json::to_string(&events).unwrap_or_else(|_| "[]".to_string()))
}

/// `[{trace_id, span_id, attributes}]`; an empty or malformed list yields
/// NULL, so "has links" is a plain not-null check.
fn normalize_span_links(links_json: &str) -> Option<String> {
    let links: Vec<SpanListElement<'_>> = serde_json::from_str(links_json).ok()?;
    if links.is_empty() {
        return None;
    }
    let links: Vec<serde_json::Value> = links
        .into_iter()
        .map(|link| {
            let attributes = link
                .attributes_json
                .and_then(|a| serde_json::from_str::<serde_json::Value>(&a).ok())
                .filter(serde_json::Value::is_object)
                .unwrap_or_else(|| serde_json::json!({}));
            serde_json::json!({
                "trace_id": link.trace_id,
                "span_id": link.span_id,
                "attributes": attributes,
            })
        })
        .collect();
    serde_json::to_string(&links).ok()
}

/// Whether a value type compares numerically.
fn is_numeric(t: &ValueType) -> bool {
    matches!(
        t,
        ValueType::Int64 | ValueType::Float64 | ValueType::DurationNs | ValueType::TimestampNs
    )
}

/// A coerced literal as `f64`, for numeric comparison against a `Utf8`-stored
/// (unpromoted) attribute cast to `Float64`.
fn literal_as_f64(literal: &Literal) -> f64 {
    match literal {
        Literal::Int64(i) => *i as f64,
        Literal::Float64(f) => *f,
        Literal::Duration(ns) => *ns as f64,
        Literal::Timestamp(TimestampLiteral::Absolute(ns)) => *ns as f64,
        _ => 0.0,
    }
}

/// The string form of a coerced literal (for `Utf8` attribute comparison).
fn string_of(literal: &Literal) -> String {
    match literal {
        Literal::String(s) => s.clone(),
        Literal::Int64(i) => i.to_string(),
        Literal::Float64(f) => f.to_string(),
        Literal::Bool(b) => b.to_string(),
        Literal::Duration(ns) => ns.to_string(),
        Literal::Timestamp(TimestampLiteral::Absolute(ns)) => ns.to_string(),
        Literal::Timestamp(TimestampLiteral::Relative(r)) => r.offset_ns.to_string(),
        Literal::Bytes(_) => String::new(),
        Literal::Array(_) => String::new(),
    }
}

/// Compile a predicate `regex` pattern behind a size limit, so a pathological
/// pattern is rejected at plan time rather than executed. (Rust's `regex` is
/// already immune to catastrophic backtracking; the size limit bounds
/// compilation blow-up.)
fn compile_regex_guard(pattern: &str) -> Result<regex::Regex, QuerierError> {
    const SIZE_LIMIT: usize = 1 << 20;
    regex::RegexBuilder::new(pattern)
        .size_limit(SIZE_LIMIT)
        .dfa_size_limit(SIZE_LIMIT)
        .build()
        .map_err(|e| QuerierError::InvalidInput(format!("invalid or oversized regex: {e}")))
}

#[cfg(test)]
#[path = "ir_planner_page_tests.rs"]
mod page_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::metric_ops::fixtures::{
        HIVE_MERGED_P50, HIVE_SERIES, counter_points, histogram_points, with_series_id, with_sum,
    };
    use common::schema::type_authority::{ObservedKind, Placement};
    use datafusion::arrow::array::{
        ArrayRef, Float64Array, Int64Array, MapBuilder, MapFieldNames, StringArray, StringBuilder,
        TimestampMicrosecondArray, TimestampNanosecondArray,
    };
    use datafusion::arrow::datatypes::{Field, Fields, Schema};
    use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
    use datafusion::catalog::{CatalogProvider, MemTable, SchemaProvider};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    /// `FLAMEGRAPH_PROFILE_CAP`'s doc comment claims it matches
    /// `QuerierConfig::max_search_limit`'s default — nothing else enforces
    /// that, so a future change to either value would silently drift them
    /// apart (the flamegraph cap would then disagree with the cap the
    /// Pyroscope render path applies over the same profile data).
    #[test]
    fn flamegraph_profile_cap_matches_max_search_limit_default() {
        assert_eq!(
            FLAMEGRAPH_PROFILE_CAP,
            common::config::QuerierConfig::default().max_search_limit
        );
    }

    fn map_field_named(name: &str) -> Field {
        let entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::Utf8, true),
            ])),
            false,
        );
        Field::new(name, DataType::Map(Arc::new(entries), false), true)
    }

    fn map_field() -> Field {
        map_field_named("log_attributes")
    }

    fn build_map(pairs: &[&[(&str, &str)]]) -> ArrayRef {
        let names = MapFieldNames {
            entry: "entries".to_string(),
            key: "keys".to_string(),
            value: "values".to_string(),
        };
        let mut b = MapBuilder::new(Some(names), StringBuilder::new(), StringBuilder::new());
        for row in pairs {
            for (k, v) in *row {
                b.keys().append_value(k);
                b.values().append_value(v);
            }
            b.append(true).unwrap();
        }
        Arc::new(b.finish())
    }

    /// Registers one `(schema, batch)` as `table_name` under catalog `t`,
    /// schema `d` — the common tail of every single-table fixture below.
    pub(super) fn single_table_ctx(
        table_name: &str,
        schema: Arc<Schema>,
        batch: RecordBatch,
    ) -> SessionContext {
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table(table_name.to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    /// A logs table with a promoted `severity_number` column, a `label_env`
    /// materialized column, and a `log_attributes` map.
    fn logs_batch() -> (Arc<Schema>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("body", DataType::Utf8, true),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("severity_text", DataType::Utf8, true),
            Field::new("severity_number", DataType::Int64, true),
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("span_id", DataType::Utf8, true),
            Field::new("label_env", DataType::Utf8, true),
            // A genuine physical column `LogicalSchema::core()` registers
            // only for `traces`, not `logs` (#1395) — used by
            // `aggregate_of_field_naming_a_physical_column_is_rejected` to
            // pin that physical addressing stays rejected for an aggregate
            // operand too.
            Field::new("duration", DataType::Float64, true),
            // The OTel LogRecord fields the explore UI renders.
            Field::new("trace_flags", DataType::Int64, true),
            Field::new("scope_name", DataType::Utf8, true),
            Field::new("scope_version", DataType::Utf8, true),
            map_field(),
            map_field_named("resource_attributes"),
            map_field_named("scope_attributes"),
        ]));

        let ts = TimestampNanosecondArray::from(vec![10_i64, 20, 30, 40]);
        let body = StringArray::from(vec![Some("a"), Some("b"), Some("c"), Some("d")]);
        let service = StringArray::from(vec![Some("api"), Some("api"), Some("web"), Some("web")]);
        let sev_text = StringArray::from(vec![
            Some("ERROR"),
            Some("INFO"),
            Some("ERROR"),
            Some("ERROR"),
        ]);
        let sev_num = Int64Array::from(vec![Some(17), Some(9), Some(17), Some(21)]);
        let trace = StringArray::from(vec![Some("t1"), Some("t2"), Some("t3"), Some("t4")]);
        let span = StringArray::from(vec![Some("s1"), Some("s2"), Some("s3"), Some("s4")]);
        // `env` promoted into label_env for two rows; the third row has no env.
        let env = StringArray::from(vec![Some("prod"), Some("prod"), None, Some("prod")]);
        // Unused by any passing test — `duration` is a physical column no
        // logical field registers for `logs`, so it's addressable only to
        // confirm that stays rejected (`aggregate_of_field_naming_a_physical_column_is_rejected`).
        let duration = Float64Array::from(vec![Some(1.5), Some(2.5), Some(3.5), Some(4.5)]);
        let flags = Int64Array::from(vec![Some(1), Some(0), Some(1), Some(1)]);
        let scope_name = StringArray::from(vec![
            Some("app.http"),
            Some("app.http"),
            Some("app.db"),
            Some("app.db"),
        ]);
        let scope_version = StringArray::from(vec![Some("1.0"); 4]);
        let log_attrs = build_map(&[
            &[("deployment.environment", "prod")],
            &[("deployment.environment", "prod")],
            &[("other", "x")],
            &[("deployment.environment", "prod")],
        ]);
        // `deployment.environment` also exists at resource scope, with a
        // different value — the two containers must stay distinguishable.
        let res_attrs = build_map(&[
            &[("deployment.environment", "resource-prod")],
            &[("deployment.environment", "resource-prod")],
            &[("deployment.environment", "resource-prod")],
            &[("deployment.environment", "resource-prod")],
        ]);
        let scope_attrs = build_map(&[
            &[("otel.scope.flavor", "sync")],
            &[("otel.scope.flavor", "sync")],
            &[("otel.scope.flavor", "async")],
            &[("otel.scope.flavor", "async")],
        ]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(ts),
                Arc::new(body),
                Arc::new(service),
                Arc::new(sev_text),
                Arc::new(sev_num),
                Arc::new(trace),
                Arc::new(span),
                Arc::new(env),
                Arc::new(duration),
                Arc::new(flags),
                Arc::new(scope_name),
                Arc::new(scope_version),
                log_attrs,
                res_attrs,
                scope_attrs,
            ],
        )
        .unwrap();

        (schema, batch)
    }

    fn logs_ctx() -> SessionContext {
        let (_, batch) = logs_batch();
        let batch = common::testing::to_typed_layout(
            "logs",
            "physical-v4",
            &batch,
            &["log_attributes", "resource_attributes", "scope_attributes"],
        );
        single_table_ctx("logs", batch.schema(), batch.clone())
    }

    fn profiles_ctx() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("profile_id", DataType::Utf8, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("duration_nano", DataType::Int64, false),
            Field::new("sample_type", DataType::Utf8, false),
            Field::new("sample_unit", DataType::Utf8, false),
            Field::new("period_type", DataType::Utf8, true),
            Field::new("period_unit", DataType::Utf8, true),
            Field::new("period", DataType::Int64, true),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("stacktraces_json", DataType::Utf8, false),
            Field::new("samples_json", DataType::Utf8, false),
            map_field_named("profile_attributes"),
            map_field_named("scope_attributes"),
            map_field_named("resource_attributes"),
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("span_id", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["p1", "p2", "p3"])),
                Arc::new(TimestampNanosecondArray::from(vec![10_i64, 20, 30])),
                Arc::new(Int64Array::from(vec![100_i64, 200, 300])),
                Arc::new(StringArray::from(vec!["cpu", "cpu", "heap"])),
                Arc::new(StringArray::from(vec!["nanoseconds"; 3])),
                Arc::new(StringArray::from(vec![Some("cpu"); 3])),
                Arc::new(StringArray::from(vec![Some("nanoseconds"); 3])),
                Arc::new(Int64Array::from(vec![Some(10_i64); 3])),
                Arc::new(StringArray::from(vec!["api", "api", "web"])),
                Arc::new(StringArray::from(vec![
                    r#"[{"frames":[{"function_name":"main"},{"function_name":"foo"}]}]"#,
                    r#"[{"frames":[{"function_name":"main"},{"function_name":"bar"}]}]"#,
                    r#"[{"frames":[{"function_name":"main"},{"function_name":"baz"}]}]"#,
                ])),
                Arc::new(StringArray::from(vec![
                    r#"[{"stacktrace_index":0,"values":[100]}]"#,
                    r#"[{"stacktrace_index":0,"values":[50]}]"#,
                    r#"[{"stacktrace_index":0,"values":[30]}]"#,
                ])),
                build_map(&[
                    &[("profile.kind", "cpu")],
                    &[("profile.kind", "cpu")],
                    &[("profile.kind", "heap")],
                ]),
                build_map(&[
                    &[("otel.scope.name", "profiler")],
                    &[("otel.scope.name", "profiler")],
                    &[("otel.scope.name", "profiler")],
                ]),
                build_map(&[
                    &[("deployment.environment", "prod")],
                    &[("deployment.environment", "prod")],
                    &[("deployment.environment", "staging")],
                ]),
                Arc::new(StringArray::from(vec![Some("t1"), Some("t2"), None])),
                Arc::new(StringArray::from(vec![Some("s1"), Some("s2"), None])),
            ],
        )
        .unwrap();
        let batch = common::testing::to_typed_layout(
            "profiles",
            "physical-v3",
            &batch,
            &[
                "profile_attributes",
                "scope_attributes",
                "resource_attributes",
            ],
        );
        let ctx = SessionContext::new();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("profiles".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    /// Gauge and sum points in the `metrics` table, over the persisted
    /// schema's common columns (`timestamp`/`service_name`/`metric_name`/
    /// `value`/`attributes`/`resource_attributes`).
    fn metrics_ctx() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            map_field_named("attributes"),
            map_field_named("resource_attributes"),
        ]));

        let gauge_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![10_i64, 20])),
                Arc::new(StringArray::from(vec!["signaldb", "signaldb"])),
                Arc::new(StringArray::from(vec![
                    "signaldb.wal.entries_processed",
                    "signaldb.wal.entries_processed",
                ])),
                Arc::new(Float64Array::from(vec![5.0, 7.0])),
                build_map(&[&[], &[]]),
                build_map(&[&[], &[]]),
            ],
        )
        .unwrap();
        let sum_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![15_i64])),
                Arc::new(StringArray::from(vec!["signaldb"])),
                Arc::new(StringArray::from(vec!["signaldb.wal.entries_processed"])),
                Arc::new(Float64Array::from(vec![3.0])),
                build_map(&[&[]]),
                build_map(&[&[]]),
            ],
        )
        .unwrap();

        // Both batches share `schema` unchanged (no per-table typed-attribute
        // conversion), so their `to_wide` results line up onto one schema —
        // required for two `metric_type`s to coexist in the same `metrics`
        // table, unlike the legacy per-type tables this fixture used to build.
        let gauge_batch = common::testing::to_wide(&gauge_batch, "gauge");
        let sum_batch = common::testing::to_wide(&sum_batch, "sum");
        let ctx = SessionContext::new();
        let table =
            MemTable::try_new(gauge_batch.schema(), vec![vec![gauge_batch, sum_batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("metrics".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    /// A `metrics` table of histogram rows shaped for each
    /// `histogram_quantile` scenario, distinguished by `metric_name`:
    /// - `latency` (svcA, svcB): two points per service in one step bucket —
    ///   instant-mode merge `[2,2,0,0]`+`[0,2,2,0]`=`[2,4,2,0]`, q=0.5 → 0.3
    ///   (bounds `[0.1,0.5,1.0]`), for both the merge math and `by` grouping.
    /// - `reset` (svcD): rate-mode two points, `first=[5,0]`,`last=[3,2]` —
    ///   the first bucket decreases (a counter reset, clamped to 0) and the
    ///   rank ends up in the `+Inf` bucket, clamped to the top finite bound
    ///   `1.0` (bounds `[1.0]`).
    /// - `solo` (svcC): a single rate-mode point — delta is all-zero → NaN.
    /// - `zero` (svcA): a single instant-mode point with all-zero counts →
    ///   NaN.
    /// - `malformed` (svcE): `bucket_counts` has one more entry than the
    ///   OTLP invariant allows — skipped, contributing no output row.
    fn histogram_ctx() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("count", DataType::Int64, true),
            Field::new("sum", DataType::Float64, true),
            Field::new("min", DataType::Float64, true),
            Field::new("max", DataType::Float64, true),
            Field::new("bucket_counts", DataType::Utf8, true),
            Field::new("explicit_bounds", DataType::Utf8, true),
            Field::new("aggregation_temporality", DataType::Int32, true),
            map_field_named("attributes"),
            map_field_named("resource_attributes"),
        ]));

        let rows: &[(i64, &str, &str, &str, &str)] = &[
            (0, "svcA", "latency", "[2,2,0,0]", "[0.1,0.5,1.0]"),
            (50, "svcA", "latency", "[0,2,2,0]", "[0.1,0.5,1.0]"),
            (0, "svcB", "latency", "[2,2,0,0]", "[0.1,0.5,1.0]"),
            (50, "svcB", "latency", "[0,2,2,0]", "[0.1,0.5,1.0]"),
            (0, "svcC", "solo", "[1,1,0,0]", "[0.1,0.5,1.0]"),
            (0, "svcD", "reset", "[5,0]", "[1.0]"),
            (99, "svcD", "reset", "[3,2]", "[1.0]"),
            (0, "svcE", "malformed", "[1,1,1]", "[1.0]"),
            (0, "svcA", "zero", "[0,0,0,0]", "[0.1,0.5,1.0]"),
            // Two raw series (svcX, svcY) interleaved in time, queried with
            // no `by` grouping. Correct rate-mode delta is per-service
            // ([2,0,0,0] and [0,2,0,0]) summed to [2,2,0,0] before
            // interpolation. Picking one first/last pair across both
            // services' points (the pre-fix bug) instead sees first=t0
            // (svcX, [10,0,0,0]) and last=t99 (svcX, [12,0,0,0]), silently
            // dropping svcY and producing a different, wrong value.
            (0, "svcX", "multiservice", "[10,0,0,0]", "[0.1,0.5,1.0]"),
            (10, "svcY", "multiservice", "[0,10,0,0]", "[0.1,0.5,1.0]"),
            (90, "svcY", "multiservice", "[0,12,0,0]", "[0.1,0.5,1.0]"),
            (99, "svcX", "multiservice", "[12,0,0,0]", "[0.1,0.5,1.0]"),
        ];
        let n = rows.len();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
                Arc::new(datafusion::arrow::array::Int64Array::from(vec![1i64; n])),
                Arc::new(Float64Array::from(vec![0.0f64; n])),
                Arc::new(Float64Array::from(vec![0.0f64; n])),
                Arc::new(Float64Array::from(vec![0.0f64; n])),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.3).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.4).collect::<Vec<_>>(),
                )),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![2i32; n])),
                build_map(&vec![&[] as &[(&str, &str)]; n]),
                build_map(&vec![&[] as &[(&str, &str)]; n]),
            ],
        )
        .unwrap();

        let batch = with_series_id(common::testing::to_wide(&batch, "histogram"));
        let ctx = SessionContext::new();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("metrics".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    /// One counter series (`requests`) at 10, 20, 5, 15, all inside one 30s
    /// step bucket: 20-10=10, 5-20 is a reset (contributes 5), 15-5=10 — the
    /// D4 design scenario (`increase` 25, `rate` 25/30 per second).
    fn rate_ctx() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            map_field_named("attributes"),
            map_field_named("resource_attributes"),
        ]));
        let n = 4;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    0i64,
                    10_000_000_000,
                    20_000_000_000,
                    29_000_000_000,
                ])),
                Arc::new(StringArray::from(vec!["svc"; n])),
                Arc::new(StringArray::from(vec!["requests"; n])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 5.0, 15.0])),
                build_map(&vec![&[] as &[(&str, &str)]; n]),
                build_map(&vec![&[] as &[(&str, &str)]; n]),
            ],
        )
        .unwrap();

        let batch = with_series_id(common::testing::to_wide(&batch, "sum"));
        let ctx = SessionContext::new();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("metrics".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    /// Two distinct series (different `service_name`, same `metric_name`)
    /// interleaved in time within one step bucket: svcA at t=0,10,20s
    /// (10,20,30 — increase 20), svcB at t=1000,1010,1020s (5,15,25 —
    /// increase 20). Grouping by `metric.name` alone (no `service.name` in
    /// `by`) must still compute each series' delta independently before
    /// summing — the correct group increase is 40. Interleaving the raw
    /// samples by time and taking `lag` across both series instead would see
    /// svcA's last sample (30) followed by svcB's first (5) as a spurious
    /// counter reset, inflating the total.
    fn rate_two_series_ctx() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            map_field_named("attributes"),
            map_field_named("resource_attributes"),
        ]));
        let rows: &[(i64, &str, f64)] = &[
            (0, "svcA", 10.0),
            (10_000_000_000, "svcA", 20.0),
            (20_000_000_000, "svcA", 30.0),
            (1_000_000_000_000, "svcB", 5.0),
            (1_010_000_000_000, "svcB", 15.0),
            (1_020_000_000_000, "svcB", 25.0),
        ];
        let n = rows.len();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(vec!["requests"; n])),
                Arc::new(Float64Array::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
                build_map(&vec![&[] as &[(&str, &str)]; n]),
                build_map(&vec![&[] as &[(&str, &str)]; n]),
            ],
        )
        .unwrap();

        let batch = with_series_id(common::testing::to_wide(&batch, "sum"));
        let ctx = SessionContext::new();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("metrics".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    fn doc(v: serde_json::Value) -> Document {
        serde_json::from_value(v).unwrap()
    }

    // Rate/increase must be computed per individual series (all `service`/
    // label identity), not just per `by` group — otherwise two series
    // sharing a `by` value interleave and their deltas cross-contaminate.
    #[tokio::test]
    async fn increase_sums_independent_per_series_deltas_not_interleaved_raw_samples() {
        let svc = IrService::new(rate_two_series_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 6, "from": "metrics",
            "range": { "from": 1_020_000_000_000i64, "to": 2_000_000_000_000i64 },
            "result": "series",
            "pipeline": [{ "aggregate": {
                "by": ["metric.name"],
                "aggs": [{ "fn": "increase", "of": "metric.value", "as": "r" }],
                "step": "2000s"
            } }]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let batches = df.collect().await.unwrap();
        let batch = batches.iter().find(|b| b.num_rows() > 0).expect("one row");
        assert_eq!(batch.num_rows(), 1, "one bucket, one metric.name group");
        let value = batch
            .column_by_name("r")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!(
            (value - 40.0).abs() < 1e-9,
            "expected 40 (20 + 20, each series' own delta), got {value}"
        );
    }

    // Task 4.1 — counter rate (D4): reset scenario from the design doc.
    #[tokio::test]
    async fn increase_and_rate_across_a_reset() {
        for (func, expected) in [("increase", 25.0), ("rate", 25.0 / 30.0)] {
            let svc = IrService::new(rate_ctx());
            let d = doc(serde_json::json!({
                "irVersion": 6, "from": "metrics",
                "range": { "from": 29_000_000_000i64, "to": 30_000_000_000i64 },
                "result": "series",
                "pipeline": [{ "aggregate": {
                    "by": ["metric.name"],
                    "aggs": [{ "fn": func, "of": "metric.value", "as": "r" }],
                    "step": "30s"
                } }]
            }));
            let (df, _) = svc
                .plan(&d, "t", "d", 0)
                .await
                .unwrap()
                .expect("source table is registered");
            let batches = df.collect().await.unwrap();
            let batch = batches.iter().find(|b| b.num_rows() > 0).expect("one row");
            assert_eq!(batch.num_rows(), 1, "{func}: one bucket, one series");
            let value = batch
                .column_by_name("r")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0);
            assert!(
                (value - expected).abs() < 1e-9,
                "{func}: expected {expected}, got {value}"
            );
        }
    }

    async fn bucket_points(svc: &IrService, d: &Document, value: &str) -> BTreeMap<i64, f64> {
        let (df, _) = svc
            .plan(d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let mut out = BTreeMap::new();
        for b in df.collect().await.unwrap() {
            let t = b
                .column_by_name("bucket")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let v = b
                .column_by_name(value)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                out.insert(t.value(i), v.value(i));
            }
        }
        out
    }

    // D11: a plain stepped metric aggregate and a range function share the
    // evaluation-instant grid `from + k·step`, each instant reading
    // `(t - step, t]`, so a formula over both joins on their timestamps.
    #[tokio::test]
    async fn plain_and_range_metric_aggregates_share_the_instant_grid() {
        const S: i64 = 1_000_000_000;
        let svc = IrService::new(rate_ctx());
        let stepped = |agg: serde_json::Value| {
            doc(serde_json::json!({
                "irVersion": 6, "from": "metrics",
                "range": { "from": 5 * S, "to": 65 * S },
                "result": "series",
                "pipeline": [{ "aggregate": {
                    "by": ["metric.name"], "aggs": [agg], "step": "30s"
                } }]
            }))
        };
        let plain = bucket_points(
            &svc,
            &stepped(serde_json::json!({ "fn": "sum", "of": "metric.value", "as": "r" })),
            "r",
        )
        .await;
        let range = bucket_points(
            &svc,
            &stepped(serde_json::json!({ "fn": "increase", "of": "metric.value", "as": "r" })),
            "r",
        )
        .await;
        // Points 0,10,20,29s valued 10,20,5,15: t=5s reads (-25s, 5s], t=35s
        // reads (5s, 35s]; t=65s is empty.
        assert_eq!(
            plain,
            BTreeMap::from([(5 * S, 10.0), (35 * S, 40.0)]),
            "plain"
        );
        assert_eq!(range.keys().copied().collect::<Vec<_>>(), vec![35 * S]);

        let series = |points: BTreeMap<i64, f64>| {
            vec![common::query_ir::EvalSeries {
                labels: BTreeMap::new(),
                points,
            }]
        };
        let inputs = HashMap::from([
            ("A".to_string(), series(range.clone())),
            ("B".to_string(), series(plain)),
        ]);
        let expr = common::query_ir::parse_formula_expr("A / B").unwrap();
        let out = common::query_ir::evaluate_formula(&expr, &inputs);
        assert_eq!(out.len(), 1, "A / B joins on the shared instant");
        assert_eq!(
            out[0].points,
            BTreeMap::from([(35 * S, range[&(35 * S)] / 40.0)])
        );
    }

    #[tokio::test]
    async fn rate_rejects_a_document_declaring_less_than_ir_version_6() {
        let svc = IrService::new(rate_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 5, "from": "metrics", "range": { "from": 0, "to": 30_000_000_000i64 },
            "result": "series",
            "pipeline": [{ "aggregate": {
                "by": ["metric.name"],
                "aggs": [{ "fn": "rate", "of": "metric.value", "as": "r" }],
                "step": "30s"
            } }]
        }));
        let err = svc.plan(&d, "t", "d", 0).await.expect_err("rejected");
        assert!(format!("{err}").contains("irVersion 6"), "{err}");
    }

    #[tokio::test]
    async fn rate_rejects_a_non_metric_source() {
        let svc = IrService::new(logs_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 6, "from": "logs", "range": { "from": 0, "to": 30_000_000_000i64 },
            "result": "series",
            "pipeline": [{ "aggregate": {
                "by": [],
                "aggs": [{ "fn": "rate", "of": "severity_number", "as": "r" }],
                "step": "30s"
            } }]
        }));
        let err = svc.plan(&d, "t", "d", 0).await.expect_err("rejected");
        assert!(format!("{err}").contains("metrics"), "{err}");
    }

    #[tokio::test]
    async fn rate_rejects_missing_step() {
        let svc = IrService::new(rate_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 6, "from": "metrics", "range": { "from": 0, "to": 30_000_000_000i64 },
            "result": "table",
            "pipeline": [{ "aggregate": {
                "by": ["metric.name"],
                "aggs": [{ "fn": "rate", "of": "metric.value", "as": "r" }]
            } }]
        }));
        let err = svc.plan(&d, "t", "d", 0).await.expect_err("rejected");
        assert!(format!("{err}").contains("requires `step`"), "{err}");
    }

    // irVersion 7 — irate/*_over_time, `across`, `window`.

    async fn one_row_f64(svc: &IrService, d: &Document) -> f64 {
        let (df, _) = svc
            .plan(d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let batches = df.collect().await.unwrap();
        let batch = batches.iter().find(|b| b.num_rows() > 0).expect("one row");
        assert_eq!(batch.num_rows(), 1, "expected exactly one output row");
        batch
            .column_by_name("r")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
    }

    /// `irate` looks only at the last two samples in the window — the reset
    /// at t=20 (30 → 5) that `increase_and_rate_across_a_reset` sees earlier
    /// in the series is irrelevant here; only the last pair (5 at t=20, 15
    /// at t=29) matters: delta 10 over dt 9s.
    #[tokio::test]
    async fn irate_uses_only_the_last_two_samples() {
        let svc = IrService::new(rate_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 7, "from": "metrics",
            "range": { "from": 29_000_000_000i64, "to": 30_000_000_000i64 },
            "result": "series",
            "pipeline": [{ "aggregate": {
                "by": ["metric.name"],
                "aggs": [{ "fn": "irate", "of": "metric.value", "as": "r" }],
                "step": "30s"
            } }]
        }));
        let value = one_row_f64(&svc, &d).await;
        let expected = 10.0 / 9.0;
        assert!(
            (value - expected).abs() < 1e-9,
            "expected {expected}, got {value}"
        );
    }

    #[tokio::test]
    async fn avg_over_time_averages_the_raw_samples_in_the_window() {
        let svc = IrService::new(rate_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 7, "from": "metrics",
            "range": { "from": 29_000_000_000i64, "to": 30_000_000_000i64 },
            "result": "series",
            "pipeline": [{ "aggregate": {
                "by": ["metric.name"],
                "aggs": [{ "fn": "avg_over_time", "of": "metric.value", "as": "r" }],
                "step": "30s"
            } }]
        }));
        let value = one_row_f64(&svc, &d).await;
        // (10 + 20 + 5 + 15) / 4
        assert!((value - 12.5).abs() < 1e-9, "got {value}");
    }

    /// `across: "avg"` folds the two series' per-series `avg_over_time`
    /// values (20 for svcA, 15 for svcB) with `avg` instead of the default
    /// `sum` — 17.5, not 35.
    #[tokio::test]
    async fn across_avg_reduces_per_series_values_with_avg_not_sum() {
        let svc = IrService::new(rate_two_series_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 7, "from": "metrics",
            "range": { "from": 1_020_000_000_000i64, "to": 2_000_000_000_000i64 },
            "result": "series",
            "pipeline": [{ "aggregate": {
                "by": ["metric.name"],
                "aggs": [{
                    "fn": "avg_over_time", "of": "metric.value", "as": "r", "across": "avg"
                }],
                "step": "2000s"
            } }]
        }));
        let value = one_row_f64(&svc, &d).await;
        assert!((value - 17.5).abs() < 1e-9, "got {value}");
    }

    /// Five evenly-spaced samples 10s apart (`t=0,10,20,30,40s`, values
    /// `1,2,3,4,5`), `step: "10s"` (one raw sample per bucket) but
    /// `window: "25s"` — each bucket's `sum_over_time` must include every
    /// sample within 25s of its own timestamp, not just its own bucket's,
    /// which is the whole point of a lookback window wider than the step.
    /// Each instant `t` reads `(t - 25s, t]`: 0 → {0} = 1; 10 → {0,10} = 3;
    /// 20 → {0,10,20} = 6; 30 → {10,20,30} = 9; 40 → {20,30,40} = 12.
    #[tokio::test]
    async fn window_wider_than_step_overlaps_buckets() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            map_field_named("attributes"),
            map_field_named("resource_attributes"),
        ]));
        let ts: Vec<i64> = (0..5).map(|i| i * 10_000_000_000).collect();
        let n = ts.len();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(ts)),
                Arc::new(StringArray::from(vec!["svc"; n])),
                Arc::new(StringArray::from(vec!["requests"; n])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0])),
                build_map(&vec![&[] as &[(&str, &str)]; n]),
                build_map(&vec![&[] as &[(&str, &str)]; n]),
            ],
        )
        .unwrap();
        let batch = with_series_id(common::testing::to_wide(&batch, "gauge"));
        let ctx = SessionContext::new();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("metrics".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);

        let svc = IrService::new(ctx);
        let d = doc(serde_json::json!({
            "irVersion": 7, "from": "metrics",
            "range": { "from": 0, "to": 40_000_000_000i64 },
            "result": "series",
            "pipeline": [{ "aggregate": {
                "by": ["metric.name"],
                "aggs": [{
                    "fn": "sum_over_time", "of": "metric.value", "as": "r", "window": "25s"
                }],
                "step": "10s"
            } }]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let batches = df.collect().await.unwrap();
        let mut values = Vec::new();
        for batch in &batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let col = batch
                .column_by_name("r")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                values.push(col.value(i));
            }
        }
        assert_eq!(values, vec![1.0, 3.0, 6.0, 9.0, 12.0], "got {values:?}");
    }

    /// Seven evenly-spaced samples 10s apart (`t=0..60s`, values `0..60`
    /// counting by 10 — a steady counter, no reset), `step: "30s"`,
    /// `window: "30s"`. The bucket ending at 60s must see only the delta
    /// *within* the window (30 → 60, i.e. 30), not the delta from the
    /// sample immediately before the window too (0 → 60, i.e. 60 minus a
    /// double-counted first interval, 40): the first in-window sample's own
    /// per-row delta is against a sample *outside* the window and must not
    /// be counted, or the window total is inflated by one interval whenever
    /// the series has history before the window.
    fn increase_window_edge_ctx() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            map_field_named("attributes"),
            map_field_named("resource_attributes"),
        ]));
        let ts: Vec<i64> = (0..7).map(|i| i * 10_000_000_000).collect();
        let n = ts.len();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(ts)),
                Arc::new(StringArray::from(vec!["svc"; n])),
                Arc::new(StringArray::from(vec!["requests"; n])),
                Arc::new(Float64Array::from(vec![
                    0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0,
                ])),
                build_map(&vec![&[] as &[(&str, &str)]; n]),
                build_map(&vec![&[] as &[(&str, &str)]; n]),
            ],
        )
        .unwrap();
        let batch = with_series_id(common::testing::to_wide(&batch, "sum"));
        let ctx = SessionContext::new();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("metrics".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    #[tokio::test]
    async fn increase_window_excludes_the_delta_from_before_the_window() {
        let svc = IrService::new(increase_window_edge_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 7, "from": "metrics",
            "range": { "from": 0, "to": 60_000_000_000i64 },
            "result": "series",
            "pipeline": [{ "aggregate": {
                "by": ["metric.name"],
                "aggs": [{
                    "fn": "increase", "of": "metric.value", "as": "r", "window": "30s"
                }],
                "step": "30s"
            } }]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let batches = df.collect().await.unwrap();
        let mut values = Vec::new();
        for batch in &batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let col = batch
                .column_by_name("r")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                values.push(col.value(i));
            }
        }
        // Instants 0, 30s, 60s read (t - 30s, t]. t=0 holds one point (a
        // baseline only, no sample); t=30 holds 10,20,30 and t=60 holds
        // 40,50,60: 20 each, never the 30→40 delta from before the window.
        assert_eq!(values, vec![20.0, 20.0], "got {values:?}");
    }

    /// Same shape, but with a counter reset inside the window (30 → 5 at
    /// t=30) — the corrected per-row delta at the reset sample is its own
    /// value (5, counted from zero), and the fix must still only subtract
    /// the true first-in-frame point, not silently reintroduce the dropped
    /// pre-window interval.
    #[tokio::test]
    async fn increase_window_edge_fix_still_honors_a_reset_inside_the_window() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            map_field_named("attributes"),
            map_field_named("resource_attributes"),
        ]));
        let ts: Vec<i64> = (0..4).map(|i| i * 10_000_000_000).collect();
        let n = ts.len();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(ts)),
                Arc::new(StringArray::from(vec!["svc"; n])),
                Arc::new(StringArray::from(vec!["requests"; n])),
                // t=0:10, t=10:20, t=20:30 (pre-window), t=30: reset to 5.
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 5.0])),
                build_map(&vec![&[] as &[(&str, &str)]; n]),
                build_map(&vec![&[] as &[(&str, &str)]; n]),
            ],
        )
        .unwrap();
        let batch = with_series_id(common::testing::to_wide(&batch, "sum"));
        let ctx = SessionContext::new();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("metrics".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);

        let svc = IrService::new(ctx);
        // The instant t=30s with `window: "11s"` reads (19s, 30s] — only the
        // 20→30s interval (the reset, contributing 5).
        let d = doc(serde_json::json!({
            "irVersion": 7, "from": "metrics",
            "range": { "from": 30_000_000_000i64, "to": 30_000_000_000i64 },
            "result": "series",
            "pipeline": [{ "aggregate": {
                "by": ["metric.name"],
                "aggs": [{
                    "fn": "increase", "of": "metric.value", "as": "r", "window": "11s"
                }],
                "step": "40s"
            } }]
        }));
        let value = one_row_f64(&svc, &d).await;
        assert!((value - 5.0).abs() < 1e-9, "got {value}");
    }

    fn increase_json(func: &str) -> serde_json::Value {
        serde_json::json!({
            "irVersion": 7, "from": "metrics",
            "range": { "from": 35_000_000_000i64, "to": 35_000_000_000i64 },
            "result": "series",
            "pipeline": [{ "aggregate": {
                "by": ["metric.name"],
                "aggs": [{ "fn": func, "of": "metric.value", "as": "r" }],
                "step": "30s"
            } }]
        })
    }

    fn increase_doc(func: &str) -> Document {
        doc(increase_json(func))
    }

    /// One service emitting two series of one counter (distinct `series_id`):
    /// each is differenced against itself, 20 + 20 — never 10 → 100 → 20 ….
    #[tokio::test]
    async fn increase_differences_each_series_of_one_service() {
        const S: i64 = 1_000_000_000;
        let rows: &[(&str, i64, f64)] = &[
            ("s1", 10 * S, 10.0),
            ("s2", 15 * S, 100.0),
            ("s1", 20 * S, 20.0),
            ("s2", 25 * S, 110.0),
            ("s1", 30 * S, 30.0),
            ("s2", 35 * S, 120.0),
        ];
        let svc = IrService::new(points_ctx(counter_points("sum", rows)));
        let value = one_row_f64(&svc, &increase_doc("increase")).await;
        assert!((value - 40.0).abs() < 1e-9, "got {value}");
    }

    #[tokio::test]
    async fn rate_over_a_gauge_is_invalid_input() {
        let rows: &[(&str, i64, f64)] = &[("g", 10_000_000_000, 1.0), ("g", 20_000_000_000, 2.0)];
        let svc = IrService::new(points_ctx(counter_points("gauge", rows)));
        let params = IrQueryParams {
            document: increase_json("rate"),
            now_ns: 0,
            page: None,
        };
        let err = svc.query(&params, "t", "d").await.unwrap_err();
        assert!(
            matches!(&err, QuerierError::InvalidInput(m) if m.contains("use delta or deriv")),
            "{err}"
        );
    }

    /// Step, window and the instant cap are checked before execution.
    #[tokio::test]
    async fn metric_operators_reject_too_many_instants_at_plan_time() {
        let rows: &[(&str, i64, f64)] = &[("c", 10, 1.0)];
        let svc = IrService::new(points_ctx(counter_points("sum", rows)));
        let range = serde_json::json!({ "from": 0, "to": 100_000_000_000_000i64 });
        let hq = serde_json::json!({ "histogram_quantile": { "q": 0.5, "step": "1s", "as": "p" } });
        let hf = serde_json::json!({ "histogram_fraction": {
            "lower": 0.0, "upper": 1.0, "step": "1s", "as": "f"
        } });
        let rate = serde_json::json!({ "aggregate": {
            "aggs": [{ "fn": "rate", "of": "metric.value", "as": "r" }], "step": "1s"
        } });
        for stage in [hq, hf, rate] {
            let d = doc(serde_json::json!({
                "irVersion": 10, "from": "metrics", "range": range, "result": "series",
                "pipeline": [stage]
            }));
            let err = svc.plan(&d, "t", "d", 0).await.map(|_| ()).unwrap_err();
            assert!(
                matches!(&err, QuerierError::InvalidInput(m) if m.contains("11000")),
                "{err}"
            );
        }
    }

    // Task 4.1 — from(logs)+where+aggregate(step) lowers to the expected plan.
    #[tokio::test]
    async fn logs_where_aggregate_step_lowers_and_executes() {
        let svc = IrService::new(logs_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "series",
            "pipeline": [
                { "where": { "and": [
                    { "field": "severity_number", "op": "gte", "value": 17 },
                    { "field": "deployment.environment", "op": "eq", "value": "prod" }
                ]}},
                { "aggregate": { "by": ["service.name"], "aggs": [{ "fn": "count", "as": "n" }], "step": "1ms" } }
            ]
        }));
        let (df, window) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        assert_eq!(
            window,
            ResolvedWindow {
                start_ns: 0,
                end_ns: 1000
            }
        );
        let plan = format!("{}", df.logical_plan().display_indent());
        assert!(plan.contains("Aggregate"), "plan:\n{plan}");
        assert!(plan.contains("Filter"), "plan:\n{plan}");
        assert!(plan.contains("date_bin"), "plan:\n{plan}");
        // Executes.
        let _ = df.collect().await.unwrap();
    }

    /// Gauge (2 rows) and sum (1 row) points in the `metrics` table, plus an
    /// unrelated `summary`-typed row sharing the queried `metric_name` (but a
    /// different `service_name`) so only a `metric.type` filter can exclude
    /// it from a `metrics` query.
    fn metrics_ctx_with_summary_leak() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            map_field_named("attributes"),
            map_field_named("resource_attributes"),
        ]));
        let gauge_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![10_i64, 20])),
                Arc::new(StringArray::from(vec!["signaldb", "signaldb"])),
                Arc::new(StringArray::from(vec![
                    "signaldb.wal.entries_processed",
                    "signaldb.wal.entries_processed",
                ])),
                Arc::new(Float64Array::from(vec![5.0, 7.0])),
                build_map(&[&[], &[]]),
                build_map(&[&[], &[]]),
            ],
        )
        .unwrap();
        let sum_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![15_i64])),
                Arc::new(StringArray::from(vec!["signaldb"])),
                Arc::new(StringArray::from(vec!["signaldb.wal.entries_processed"])),
                Arc::new(Float64Array::from(vec![3.0])),
                build_map(&[&[]]),
                build_map(&[&[]]),
            ],
        )
        .unwrap();

        let leak = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![25_i64])),
                Arc::new(StringArray::from(vec!["leak"])),
                Arc::new(StringArray::from(vec!["signaldb.wal.entries_processed"])),
                Arc::new(Float64Array::from(vec![999.0])),
                build_map(&[&[]]),
                build_map(&[&[]]),
            ],
        )
        .unwrap();
        let gauge = common::testing::to_wide(&gauge_batch, "gauge");
        let sum = common::testing::to_wide(&sum_batch, "sum");
        let leak = common::testing::to_wide(&leak, "summary");
        let table = MemTable::try_new(gauge.schema(), vec![vec![gauge, sum, leak]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("metrics".to_string(), Arc::new(table))
            .unwrap();
        let ctx = SessionContext::new();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    #[tokio::test]
    async fn metrics_filtered_to_gauge_and_sum_filters_by_name_and_aggregates() {
        let svc = IrService::new(metrics_ctx_with_summary_leak());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "metrics", "range": { "from": 0, "to": 1_000_000 },
            "result": "series",
            "pipeline": [
                { "where": { "and": [
                    { "field": "metric.name", "op": "eq", "value": "signaldb.wal.entries_processed" },
                    { "field": "metric.type", "op": "in", "value": ["gauge", "sum"] }
                ] } },
                { "aggregate": { "by": ["service.name"], "aggs": [{ "fn": "sum", "of": "metric.value", "as": "v" }], "step": "1ms" } }
            ]
        }));
        let (df, window) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("the metrics source is registered");
        assert_eq!(
            window,
            ResolvedWindow {
                start_ns: 0,
                end_ns: 1_000_000
            }
        );
        let plan = format!("{}", df.logical_plan().display_indent());
        assert!(plan.contains("Aggregate"), "plan:\n{plan}");
        assert!(plan.contains("Filter"), "plan:\n{plan}");
        assert!(!plan.contains("date_bin"), "plan:\n{plan}");
        // 5 + 7 (gauge) + 3 (sum) = 15. The `summary` row's 999.0 would
        // corrupt this total if the `metric.type` filter failed.
        let batches = df.collect().await.unwrap();
        let total: f64 = batches
            .iter()
            .map(|b| {
                let col = b
                    .column_by_name("v")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Float64Array>()
                    .unwrap();
                col.values().iter().sum::<f64>()
            })
            .sum();
        assert_eq!(total, 15.0);
    }

    /// One point per metric type in the wide `metrics` table, named after
    /// its type: `sum` is cumulative (temporality 2) and monotonic, the
    /// histogram and summary carry their typed bucket/quantile lists.
    fn metric_model_ctx() -> SessionContext {
        use datafusion::arrow::array::{BooleanArray, Int32Array, ListArray};
        use datafusion::arrow::datatypes::{Float64Type, Int64Type};

        const N: Option<f64> = None;
        let f64s = |v: [Option<f64>; 5]| -> ArrayRef { Arc::new(Float64Array::from(v.to_vec())) };
        let f64_list = |at: usize, v: &[f64]| -> ArrayRef {
            let rows =
                (0..5).map(|i| (i == at).then(|| v.iter().copied().map(Some).collect::<Vec<_>>()));
            Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(rows))
        };
        let types = [
            "gauge",
            "sum",
            "histogram",
            "exponential_histogram",
            "summary",
        ];
        let no_attrs = || build_map(&[&[] as &[(&str, &str)]; 5]);
        let columns: Vec<(&str, ArrayRef)> = vec![
            (
                "timestamp",
                Arc::new(TimestampNanosecondArray::from(vec![10_i64, 20, 30, 40, 50])),
            ),
            ("service_name", Arc::new(StringArray::from(vec!["svc"; 5]))),
            ("metric_name", Arc::new(StringArray::from(types.to_vec()))),
            ("metric_type", Arc::new(StringArray::from(types.to_vec()))),
            ("value", f64s([Some(0.5), Some(42.0), N, N, N])),
            (
                "count",
                Arc::new(Int64Array::from(vec![
                    None,
                    None,
                    Some(4),
                    Some(3),
                    Some(10),
                ])),
            ),
            ("sum", f64s([N, N, Some(1.2), Some(3.0), Some(5.0)])),
            ("min", f64s([N, N, Some(0.1), N, N])),
            ("max", f64s([N, N, Some(0.9), N, N])),
            ("explicit_bounds", f64_list(2, &[0.1, 0.5, 1.0])),
            (
                "bucket_counts",
                Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
                    (0..5).map(|i| (i == 2).then(|| vec![Some(1), Some(2), Some(1), Some(0)])),
                )),
            ),
            ("quantiles", f64_list(4, &[0.5, 0.99])),
            ("quantile_values", f64_list(4, &[0.4, 0.9])),
            (
                "aggregation_temporality",
                Arc::new(Int32Array::from(vec![
                    None,
                    Some(2),
                    Some(2),
                    Some(1),
                    None,
                ])),
            ),
            (
                "is_monotonic",
                Arc::new(BooleanArray::from(vec![None, Some(true), None, None, None])),
            ),
            ("attributes", no_attrs()),
            ("resource_attributes", no_attrs()),
        ];
        let batch = RecordBatch::try_from_iter(columns).unwrap();
        single_table_ctx("metrics", batch.schema(), batch)
    }

    async fn metric_model_query(
        result: &str,
        fields: Option<&[&str]>,
        pipeline: serde_json::Value,
    ) -> Result<RecordBatch, QuerierError> {
        let svc = IrService::new(metric_model_ctx());
        let mut d = serde_json::json!({
            "irVersion": 1, "from": "metrics", "range": { "from": 0, "to": 1000 },
            "result": result, "pipeline": pipeline
        });
        if let Some(fields) = fields {
            d["fields"] = serde_json::json!(fields);
        }
        let (df, _) = svc
            .plan(&doc(d), "t", "d", 0)
            .await?
            .expect("the metrics table is registered");
        let batches = df.collect().await.unwrap();
        Ok(datafusion::arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap())
    }

    fn strings_of(batch: &RecordBatch, name: &str) -> Vec<String> {
        let col =
            datafusion::arrow::compute::cast(batch.column_by_name(name).unwrap(), &DataType::Utf8)
                .unwrap();
        let col = col.as_any().downcast_ref::<StringArray>().unwrap();
        col.iter()
            .map(|v| v.unwrap_or("null").to_string())
            .collect()
    }

    #[tokio::test]
    async fn metrics_source_covers_every_metric_type_as_a_groupable_field() {
        let batch = metric_model_query(
            "table",
            None,
            serde_json::json!([
                { "aggregate": { "by": ["metric.type"], "aggs": [{ "fn": "count", "as": "n" }] } },
                { "order": [{ "of": "metric.type", "dir": "asc" }] }
            ]),
        )
        .await
        .unwrap();
        assert_eq!(
            strings_of(&batch, "metric_type"),
            vec![
                "exponential_histogram",
                "gauge",
                "histogram",
                "sum",
                "summary"
            ]
        );
        assert_eq!(strings_of(&batch, "n"), vec!["1"; 5]);
    }

    #[tokio::test]
    async fn unfiltered_metrics_rows_include_non_scalar_types_with_null_value() {
        let batch = metric_model_query(
            "rows",
            Some(&["metric.name", "metric.value"]),
            serde_json::json!([{ "order": [{ "of": "timestamp", "dir": "asc" }] }]),
        )
        .await
        .unwrap();
        assert_eq!(
            strings_of(&batch, "value"),
            vec!["0.5", "42.0", "null", "null", "null"]
        );
    }

    #[tokio::test]
    async fn metrics_filter_by_metric_type_returns_histogram_fields() {
        let batch = metric_model_query(
            "rows",
            Some(&[
                "metric.count",
                "metric.sum",
                "metric.min",
                "metric.max",
                "metric.explicit_bounds",
                "metric.bucket_counts",
            ]),
            serde_json::json!([{ "where": { "field": "metric.type", "op": "eq", "value": "histogram" } }]),
        )
        .await
        .unwrap();
        assert_eq!(batch.num_rows(), 1);
        for (column, expected) in [
            ("count", "4"),
            ("sum", "1.2"),
            ("min", "0.1"),
            ("max", "0.9"),
            ("explicit_bounds", "[0.1, 0.5, 1.0]"),
            ("bucket_counts", "[1, 2, 1, 0]"),
        ] {
            assert_eq!(strings_of(&batch, column), vec![expected], "{column}");
        }
    }

    #[tokio::test]
    async fn summary_quantiles_are_returned_as_stored() {
        let batch = metric_model_query(
            "rows",
            Some(&["metric.name", "metric.quantiles", "metric.quantile_values"]),
            serde_json::json!([{ "where": { "field": "metric.type", "op": "eq", "value": "summary" } }]),
        )
        .await
        .unwrap();
        assert_eq!(strings_of(&batch, "quantiles"), vec!["[0.5, 0.99]"]);
        assert_eq!(strings_of(&batch, "quantile_values"), vec!["[0.4, 0.9]"]);
    }

    #[tokio::test]
    async fn temporality_and_monotonic_are_typed_filterable_fields() {
        let batch = metric_model_query(
            "rows",
            Some(&["metric.name", "metric.temporality", "metric.monotonic"]),
            serde_json::json!([{ "where": { "and": [
                { "field": "metric.temporality", "op": "eq", "value": 2 },
                { "field": "metric.monotonic", "op": "eq", "value": true }
            ] } }]),
        )
        .await
        .unwrap();
        assert_eq!(strings_of(&batch, "metric_name"), vec!["sum"]);

        let err = metric_model_query(
            "rows",
            None,
            serde_json::json!([{ "where": { "field": "metric.temporality", "op": "eq", "value": "cumulative" } }]),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, QuerierError::InvalidInput(_)), "{err}");
    }

    /// Two exemplars of one histogram series, on different traces.
    fn exemplars_ctx() -> SessionContext {
        let columns: Vec<(&str, ArrayRef)> = vec![
            (
                "timestamp",
                Arc::new(TimestampNanosecondArray::from(vec![10_i64, 20])),
            ),
            (
                "point_timestamp",
                Arc::new(TimestampNanosecondArray::from(vec![30_i64, 30])),
            ),
            (
                "service_name",
                Arc::new(StringArray::from(vec!["checkout"; 2])),
            ),
            (
                "metric_name",
                Arc::new(StringArray::from(vec!["http.server.duration"; 2])),
            ),
            (
                "metric_type",
                Arc::new(StringArray::from(vec!["histogram"; 2])),
            ),
            ("series_id", Arc::new(StringArray::from(vec!["s1"; 2]))),
            ("value", Arc::new(Float64Array::from(vec![0.25, 0.75]))),
            (
                "trace_id",
                Arc::new(StringArray::from(vec!["aaaa", "bbbb"])),
            ),
            ("span_id", Arc::new(StringArray::from(vec!["01", "02"]))),
            (
                "filtered_attributes",
                build_map(&[&[("http.route", "/cart")], &[]]),
            ),
            (
                "resource_identity",
                Arc::new(StringArray::from(vec!["r1"; 2])),
            ),
        ];
        let batch = common::testing::to_typed_layout(
            "metric_exemplars",
            "physical-v4",
            &RecordBatch::try_from_iter(columns).unwrap(),
            &["filtered_attributes"],
        );
        single_table_ctx("metric_exemplars", batch.schema(), batch)
    }

    #[tokio::test]
    async fn exemplars_are_queryable_by_trace_id() {
        let svc = IrService::new(exemplars_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "exemplars", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": [
                "timestamp", "span.id", "exemplar.value", "exemplar.filtered_attributes",
                "metric.name", "metric.type"
            ],
            "pipeline": [{ "where": { "field": "trace.id", "op": "eq", "value": "aaaa" } }]
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let batches = df.collect().await.unwrap();
        let batch =
            datafusion::arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        assert_eq!(batch.num_rows(), 1);
        for (column, expected) in [
            ("span_id", "01"),
            ("value", "0.25"),
            ("metric_name", "http.server.duration"),
            ("metric_type", "histogram"),
        ] {
            assert_eq!(strings_of(&batch, column), vec![expected], "{column}");
        }
        assert_eq!(strings_of(&batch, "timestamp").len(), 1);
        let attrs = datafusion::arrow::util::display::array_value_to_string(
            batch
                .column_by_name("exemplar_filtered_attributes")
                .unwrap(),
            0,
        )
        .unwrap();
        assert!(attrs.contains("/cart"), "{attrs}");
    }

    #[tokio::test]
    async fn bucket_and_quantile_lists_are_retrieval_only() {
        for field in [
            "metric.quantiles",
            "metric.quantile_values",
            "metric.explicit_bounds",
            "metric.bucket_counts",
        ] {
            let err = metric_model_query(
                "rows",
                None,
                serde_json::json!([{ "where": { "field": field, "op": "exists" } }]),
            )
            .await
            .unwrap_err();
            assert!(format!("{err}").contains(field), "{field}: {err}");
        }
    }

    /// Two gauge points and one sum point in the typed-layout `metrics` table,
    /// every one carrying the resource attribute `container.name`, which is
    /// not a registered logical field and so resolves through the
    /// attribute-map fallback. The typed container columns come from
    /// `metrics_gauge` `physical-v3`, the fixture helper having no `metrics`
    /// schema; the container shape is the same.
    fn fallback_attr_metrics_ctx() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            map_field_named("attributes"),
            map_field_named("resource_attributes"),
        ]));
        let batch = |n: usize| {
            let containers = vec![&[("container.name", "ix-signaldb-mcp-1")] as &[_]; n];
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampNanosecondArray::from(vec![10_i64; n])),
                    Arc::new(StringArray::from(vec!["signaldb"; n])),
                    Arc::new(StringArray::from(vec!["m"; n])),
                    Arc::new(Float64Array::from(vec![5.0; n])),
                    build_map(&vec![&[] as &[(&str, &str)]; n]),
                    build_map(&containers),
                ],
            )
            .unwrap()
        };
        let typed = |b: RecordBatch| {
            common::testing::to_typed_layout(
                "metrics",
                "physical-v4",
                &b,
                &["attributes", "resource_attributes"],
            )
        };
        let gauge = common::testing::to_wide(&typed(batch(2)), "gauge");
        let sum = common::testing::to_wide(&typed(batch(1)), "sum");
        let table = MemTable::try_new(gauge.schema(), vec![vec![gauge, sum]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("metrics".to_string(), Arc::new(table))
            .unwrap();
        let ctx = SessionContext::new();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    /// Runs `pipeline` over [`fallback_attr_metrics_ctx`] and returns the
    /// result's row count and the sum of its `n` column.
    async fn fallback_attr_count(pipeline: serde_json::Value) -> (usize, i64) {
        let svc = IrService::new(fallback_attr_metrics_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "metrics", "range": { "from": 0, "to": 1000 },
            "result": "table",
            "pipeline": pipeline
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("the metrics table is registered");
        let batches = df.collect().await.unwrap();
        let rows = batches.iter().map(|b| b.num_rows()).sum();
        let n = batches
            .iter()
            .map(|b| {
                b.column_by_name("n")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .sum::<i64>()
            })
            .sum();
        (rows, n)
    }

    /// #1206: grouping by a resource attribute that is not a registered
    /// logical field (`host.name`, absent on every row) resolves through the
    /// generic `resource.`/map fallback.
    #[tokio::test]
    async fn metrics_groups_by_a_fallback_resource_attribute() {
        let (_, n) = fallback_attr_count(serde_json::json!([
            { "aggregate": { "by": ["host.name"], "aggs": [{ "fn": "count", "as": "n" }] } },
            { "limit": 501 }
        ]))
        .await;
        // Every gauge and sum row lands in the one (null host.name) group.
        assert_eq!(n, 3);
    }

    /// #1348: every predicate shape the resolver can put on an attribute-map
    /// column must apply, matching all three gauge and sum rows. The ordered
    /// comparisons are covered by `between`, which lowers to `>= lo AND <= hi`.
    #[tokio::test]
    async fn metrics_filters_by_a_fallback_attribute_with_every_operator() {
        let cases: Vec<(&str, Option<serde_json::Value>)> = vec![
            ("eq", Some(serde_json::json!("ix-signaldb-mcp-1"))),
            ("ne", Some(serde_json::json!("something-else"))),
            ("contains", Some(serde_json::json!("signaldb"))),
            ("regex", Some(serde_json::json!("(?i)^ix-signaldb"))),
            ("exists", None),
            (
                "in",
                Some(serde_json::json!(["ix-signaldb-mcp-1", "not-this-one"])),
            ),
            ("between", Some(serde_json::json!(["ix-a", "ix-z"]))),
        ];
        for (op, value) in cases {
            let mut predicate = serde_json::json!({ "field": "container.name", "op": op });
            if let Some(v) = value {
                predicate["value"] = v;
            }
            let (_, n) = fallback_attr_count(serde_json::json!([
                { "where": predicate },
                { "aggregate": { "by": ["container.name"], "aggs": [{ "fn": "count", "as": "n" }] } }
            ]))
            .await;
            assert_eq!(n, 3, "op '{op}' matched the wrong number of rows");
        }
    }

    /// A predicate that plans but matches everything would pass the test
    /// above while ignoring the filter; these cases must exclude every row.
    #[tokio::test]
    async fn metrics_filter_by_a_fallback_attribute_excludes_as_well_as_matches() {
        let cases: Vec<(&str, serde_json::Value)> = vec![
            ("eq", serde_json::json!("not-a-container")),
            ("ne", serde_json::json!("ix-signaldb-mcp-1")),
            ("contains", serde_json::json!("nothing-like-this")),
            ("regex", serde_json::json!("^zzz")),
            ("in", serde_json::json!(["neither", "nor"])),
            ("between", serde_json::json!(["aa", "ab"])),
        ];
        for (op, value) in cases {
            let (rows, _) = fallback_attr_count(serde_json::json!([
                { "where": { "field": "container.name", "op": op, "value": value } },
                { "aggregate": { "by": ["container.name"], "aggs": [{ "fn": "count", "as": "n" }] } }
            ]))
            .await;
            assert_eq!(rows, 0, "op '{op}' should have excluded every row");
        }
    }

    /// Every scalar source registers a logical `timestamp` (#1205): metrics
    /// and profiles used to lack it, so `max(timestamp)` — the "last seen"
    /// column entity discovery needs across signals — was rejected as
    /// physical addressing on those two sources.
    async fn max_timestamp_ns(ctx: SessionContext, source: &str) -> i64 {
        let svc = IrService::new(ctx);
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": source, "range": { "from": 0, "to": 1000 },
            "result": "table",
            "pipeline": [
                { "aggregate": { "by": ["service.name"], "aggs": [
                    { "fn": "count", "as": "n" },
                    { "fn": "max", "of": "timestamp", "as": "last" }
                ] } },
                { "order": [{ "of": "last", "dir": "desc" }] },
                { "limit": 10 }
            ]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap_or_else(|e| panic!("{source}: {e}"))
            .expect("source table is registered");
        let batches = df.collect().await.unwrap();
        let first = batches.iter().find(|b| b.num_rows() > 0).expect("a row");
        let col = first
            .column_by_name("last")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("max(timestamp) keeps the timestamp type");
        col.value(0)
    }

    /// A `metrics` table whose `timestamp` scans as `Timestamp(µs)`, as the
    /// Iceberg `timestamp` type does, holding one point per `micros`.
    fn metrics_us_ctx(micros: &[i64]) -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            map_field_named("attributes"),
            map_field_named("resource_attributes"),
        ]));
        let n = micros.len();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMicrosecondArray::from(micros.to_vec())),
                Arc::new(StringArray::from(vec!["svc"; n])),
                Arc::new(StringArray::from(vec!["m"; n])),
                Arc::new(Float64Array::from(vec![1.0; n])),
                build_map(&vec![&[][..]; n]),
                build_map(&vec![&[][..]; n]),
            ],
        )
        .unwrap();
        let batch = common::testing::to_wide(&batch, "gauge");
        let ctx = SessionContext::new();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("metrics".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    /// Issue #2122: a stepped `metrics` aggregate bounds its input on the
    /// bare `timestamp` column (the Iceberg scan cannot prune on a cast of
    /// it), and the bounds stay exact over a microsecond column: a point at
    /// `from - step` belongs to no instant, one 1µs later to `from`.
    #[tokio::test]
    async fn stepped_metrics_aggregate_bounds_the_bare_timestamp_exactly() {
        const S: i64 = 1_000_000;
        let (from, step) = (120 * S, 60 * S);
        let points = [from - step, from - step + 1, from, from + 2 * step];
        let svc = IrService::new(metrics_us_ctx(&points));
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "metrics", "result": "series",
            "range": { "from": from * 1_000, "to": (from + 2 * step) * 1_000 + 999 },
            "pipeline": [
                { "aggregate": { "by": [], "aggs": [{ "fn": "count", "as": "n" }], "step": "60s" } }
            ]
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let plan = df.clone().into_optimized_plan().unwrap();
        let text = plan.display_indent().to_string();
        let filters: Vec<&str> = text.lines().filter(|l| l.contains("Filter:")).collect();
        assert!(
            !filters.is_empty() && filters.iter().all(|f| !f.contains("CAST(")),
            "time bounds must compare the bare column: {filters:#?}"
        );
        let batches = df.collect().await.unwrap();
        let got: Vec<(i64, i64)> = batches
            .iter()
            .flat_map(|b| {
                let at = datafusion::arrow::compute::cast(
                    b.column_by_name("bucket").unwrap(),
                    &DataType::Int64,
                )
                .unwrap();
                let at = at.as_any().downcast_ref::<Int64Array>().unwrap().clone();
                let n = b
                    .column_by_name("n")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .clone();
                (0..b.num_rows()).map(move |i| (at.value(i), n.value(i)))
            })
            .collect();
        assert_eq!(got, [(from * 1_000, 2), ((from + 2 * step) * 1_000, 1)]);
    }

    #[tokio::test]
    async fn metrics_max_timestamp_aggregate_executes() {
        // gauge points at 10/20, sum point at 15 — the max spans the union.
        assert_eq!(max_timestamp_ns(metrics_ctx(), "metrics").await, 20);
    }

    #[tokio::test]
    async fn profiles_max_timestamp_aggregate_executes() {
        assert_eq!(max_timestamp_ns(profiles_ctx(), "profiles").await, 30);
    }

    async fn ordered_by_timestamp(ctx: SessionContext, source: &str) -> Vec<i64> {
        let svc = IrService::new(ctx);
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": source, "range": { "from": 0, "to": 1000 },
            "result": "rows", "fields": ["timestamp", "service.name"],
            "pipeline": [
                { "where": { "field": "timestamp", "op": "gte", "value": 15 } },
                { "order": [{ "of": "timestamp", "dir": "desc" }] }
            ]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap_or_else(|e| panic!("{source}: {e}"))
            .expect("source table is registered");
        let batches = df.collect().await.unwrap();
        batches
            .iter()
            .flat_map(|b| {
                let col = b
                    .column_by_name("timestamp")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                col.values().iter().copied().collect::<Vec<_>>()
            })
            .collect()
    }

    #[tokio::test]
    async fn metrics_filter_and_order_by_timestamp_execute() {
        assert_eq!(
            ordered_by_timestamp(metrics_ctx(), "metrics").await,
            vec![20, 15]
        );
    }

    #[tokio::test]
    async fn profiles_filter_and_order_by_timestamp_execute() {
        assert_eq!(
            ordered_by_timestamp(profiles_ctx(), "profiles").await,
            vec![30, 20]
        );
    }

    /// The `service.name` of row `i`, from the Series frame's `__labels`.
    fn service_of(b: &RecordBatch, i: usize) -> String {
        use datafusion::arrow::array::AsArray;
        let labels = b.column_by_name("__labels").unwrap();
        let set: serde_json::Value =
            serde_json::from_str(labels.as_string::<i32>().value(i)).unwrap();
        set["service.name"].as_str().unwrap_or_default().to_string()
    }

    /// The histogram values of `batches`, all of them or those of the
    /// `service.name` series `label` names.
    fn histogram_value(batches: &[RecordBatch], _as_name: &str, label: Option<&str>) -> Vec<f64> {
        let mut out = Vec::new();
        for b in batches {
            let values = b
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Float64Array>()
                .unwrap();
            match label {
                None => out.extend(values.iter().map(|v| v.unwrap_or(f64::NAN))),
                Some(want) => {
                    for i in 0..b.num_rows() {
                        if service_of(b, i) == want {
                            out.push(values.value(i));
                        }
                    }
                }
            }
        }
        out
    }

    /// The `latency` (svcA/svcB)/`reset` (svcD) slice of
    /// [`histogram_ctx`], built via [`common::testing::to_wide`],
    /// plus one `leak_type` row sharing `latency`'s `metric_name`.
    fn histogram_ctx_with_leak(leak_type: &str) -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("bucket_counts", DataType::Utf8, true),
            Field::new("explicit_bounds", DataType::Utf8, true),
            map_field_named("attributes"),
            map_field_named("resource_attributes"),
        ]));
        let rows: &[(i64, &str, &str, &str, &str)] = &[
            (0, "svcA", "latency", "[2,2,0,0]", "[0.1,0.5,1.0]"),
            (50, "svcA", "latency", "[0,2,2,0]", "[0.1,0.5,1.0]"),
            (0, "svcB", "latency", "[2,2,0,0]", "[0.1,0.5,1.0]"),
            (50, "svcB", "latency", "[0,2,2,0]", "[0.1,0.5,1.0]"),
            (0, "svcD", "reset", "[5,0]", "[1.0]"),
            (99, "svcD", "reset", "[3,2]", "[1.0]"),
        ];
        let n = rows.len();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.3).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.4).collect::<Vec<_>>(),
                )),
                build_map(&vec![&[] as &[(&str, &str)]; n]),
                build_map(&vec![&[] as &[(&str, &str)]; n]),
            ],
        )
        .unwrap();

        let leak = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![0])),
                Arc::new(StringArray::from(vec!["leak"])),
                Arc::new(StringArray::from(vec!["latency"])),
                Arc::new(StringArray::from(vec!["[100,100,100,100]"])),
                Arc::new(StringArray::from(vec!["[0.1,0.5,1.0]"])),
                build_map(&[&[] as &[(&str, &str)]]),
                build_map(&[&[] as &[(&str, &str)]]),
            ],
        )
        .unwrap();
        let main = with_series_id(common::testing::to_wide(&batch, "histogram"));
        let leak = with_series_id(common::testing::to_wide(&leak, leak_type));
        let table = MemTable::try_new(main.schema(), vec![vec![main, leak]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("metrics".to_string(), Arc::new(table))
            .unwrap();
        let ctx = SessionContext::new();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    #[tokio::test]
    async fn histogram_quantile_instant_mode_takes_the_latest_point_per_service() {
        let svc = IrService::new(histogram_ctx_with_leak("gauge"));
        let d = doc(serde_json::json!({
            "irVersion": 3, "from": "metrics", "range": { "from": 100, "to": 1000 },
            "result": "series",
            "pipeline": [
                { "where": { "field": "metric.name", "op": "eq", "value": "latency" } },
                { "histogram_quantile": { "q": 0.5, "by": ["service.name"], "step": "1000ms", "mode": "instant", "as": "p50" } }
            ]
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2, "one row per service");
        for label in ["svcA", "svcB"] {
            let vs = histogram_value(&batches, "p50", Some(label));
            assert_eq!(vs.len(), 1, "{label}");
            // The latest point [0,2,2,0]: rank 2 tops out the (0.1, 0.5] bucket.
            assert!((vs[0] - 0.5).abs() < 1e-9, "{label}: got {:?}", vs[0]);
        }
        assert!(
            histogram_value(&batches, "p50", Some("leak")).is_empty(),
            "{batches:?}"
        );
    }

    #[tokio::test]
    async fn histogram_quantile_rate_mode_counts_a_reset_series_whole() {
        let svc = IrService::new(histogram_ctx_with_leak("gauge"));
        let d = doc(serde_json::json!({
            "irVersion": 3, "from": "metrics", "range": { "from": 100, "to": 1000 },
            "result": "series",
            "pipeline": [
                { "where": { "field": "metric.name", "op": "eq", "value": "reset" } },
                { "histogram_quantile": { "q": 0.5, "step": "1000ms", "mode": "rate", "as": "p50" } }
            ]
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let batches = df.collect().await.unwrap();
        let vs = histogram_value(&batches, "p50", None);
        assert_eq!(vs.len(), 1);
        // [5,0] → [3,2]: a bucket decreased, so the series reset and [3,2]
        // counts whole; rank 2.5 of 5 interpolates to 2.5/3 in (0, 1].
        assert!((vs[0] - 2.5 / 3.0).abs() < 1e-9, "got {:?}", vs[0]);
    }

    /// Regression: rate mode must compute each raw series' delta
    /// independently before merging into the requested `by` group — two
    /// interleaved services under an empty `by` must not have their points
    /// paired into a single, meaningless first/last delta.
    #[tokio::test]
    async fn histogram_quantile_rate_mode_sums_per_series_deltas_across_an_empty_by_group() {
        let svc = IrService::new(histogram_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 3, "from": "metrics", "range": { "from": 100, "to": 1000 },
            "result": "series",
            "pipeline": [
                { "where": { "field": "metric.name", "op": "eq", "value": "multiservice" } },
                { "histogram_quantile": { "q": 0.5, "step": "1000ms", "mode": "rate", "as": "p50" } }
            ]
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let batches = df.collect().await.unwrap();
        let vs = histogram_value(&batches, "p50", None);
        assert_eq!(vs.len(), 1);
        assert!((vs[0] - 0.1).abs() < 1e-9, "got {:?}", vs[0]);
    }

    #[tokio::test]
    async fn histogram_quantile_single_point_in_rate_mode_has_no_sample() {
        let svc = IrService::new(histogram_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 3, "from": "metrics", "range": { "from": 100, "to": 1000 },
            "result": "series",
            "pipeline": [
                { "where": { "field": "metric.name", "op": "eq", "value": "solo" } },
                { "histogram_quantile": { "q": 0.5, "step": "1000ms", "mode": "rate", "as": "p50" } }
            ]
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let batches = df.collect().await.unwrap();
        assert!(histogram_value(&batches, "p50", None).is_empty());
    }

    #[tokio::test]
    async fn histogram_quantile_all_zero_buckets_in_instant_mode_is_nan() {
        let svc = IrService::new(histogram_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 3, "from": "metrics", "range": { "from": 100, "to": 1000 },
            "result": "series",
            "pipeline": [
                { "where": { "field": "metric.name", "op": "eq", "value": "zero" } },
                { "histogram_quantile": { "q": 0.5, "step": "1000ms", "mode": "instant", "as": "p50" } }
            ]
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let batches = df.collect().await.unwrap();
        let vs = histogram_value(&batches, "p50", None);
        assert_eq!(vs.len(), 1);
        assert!(vs[0].is_nan(), "got {:?}", vs[0]);
    }

    #[tokio::test]
    async fn histogram_quantile_skips_malformed_bucket_rows() {
        let svc = IrService::new(histogram_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 3, "from": "metrics", "range": { "from": 100, "to": 1000 },
            "result": "series",
            "pipeline": [
                { "where": { "field": "metric.name", "op": "eq", "value": "malformed" } },
                { "histogram_quantile": { "q": 0.5, "step": "1000ms", "mode": "instant", "as": "p50" } }
            ]
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 0,
            "the only row is malformed, so no group forms"
        );
    }

    #[tokio::test]
    async fn histogram_quantile_limit_stage_executes_on_reinjected_dataframe() {
        let svc = IrService::new(histogram_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 3, "from": "metrics", "range": { "from": 100, "to": 1000 },
            "result": "series",
            "pipeline": [
                { "where": { "field": "metric.name", "op": "eq", "value": "latency" } },
                { "histogram_quantile": { "q": 0.5, "by": ["service.name"], "step": "1000ms", "mode": "instant", "as": "p50" } },
                { "limit": 1 }
            ]
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1, "limit narrows the 2-service result to 1");
    }

    #[tokio::test]
    async fn series_stages_read_a_histogram_quantile_as_a_series() {
        use datafusion::arrow::array::AsArray;
        use datafusion::arrow::compute::concat_batches;
        use datafusion::arrow::datatypes::Float64Type;

        // `sum(histogram_quantile(0.5, sum by (service.name) (latency)))`
        let svc = IrService::new(histogram_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 10, "from": "metrics", "range": { "from": 100, "to": 1000 },
            "step": "1000ms", "result": "series",
            "pipeline": [
                { "where": { "field": "metric.name", "op": "eq", "value": "latency" } },
                { "histogram_quantile": { "q": 0.5, "by": ["service.name"], "step": "1000ms", "mode": "instant", "as": "p50" } },
                { "filter": { "op": "gt", "value": 0.0 } },
                { "reduce": { "fn": "sum" } }
            ]
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let batches = df.collect().await.unwrap();
        let batch = concat_batches(&batches[0].schema(), &batches).unwrap();
        let labels = batch.column_by_name("__labels").unwrap().as_string::<i32>();
        let values = batch
            .column_by_name("value")
            .unwrap()
            .as_primitive::<Float64Type>();
        assert_eq!(batch.num_rows(), 1, "{batch:?}");
        assert_eq!(labels.value(0), "{}");
        // Both services' p50 is 0.5.
        assert!((values.value(0) - 1.0).abs() < 1e-9, "{batch:?}");
    }

    #[tokio::test]
    async fn a_subquery_over_a_histogram_quantile_reads_its_widened_window() {
        use datafusion::arrow::array::AsArray;
        use datafusion::arrow::datatypes::Float64Type;

        // `min_over_time(histogram_quantile(0.5, …)[70ns:10ns])` at 60ns:
        // the inner instant 0ns sees the 0ns point (p50 0.1), 50ns the 50ns
        // point (0.5), which lie before the range.
        let svc = IrService::new(histogram_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 10, "from": "metrics", "range": { "from": 60, "to": 60 },
            "step": "10ns", "result": "series",
            "pipeline": [
                { "where": { "field": "metric.name", "op": "eq", "value": "latency" } },
                { "histogram_quantile": { "q": 0.5, "by": ["service.name"], "step": "10ns", "mode": "instant", "as": "p50" } },
                { "over_time": { "fn": "min", "window": "70ns" } }
            ]
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let batches = df.collect().await.unwrap();
        let values: Vec<f64> = batches
            .iter()
            .flat_map(|b| {
                let v = b
                    .column_by_name("value")
                    .unwrap()
                    .as_primitive::<Float64Type>();
                v.values().to_vec()
            })
            .collect();
        assert_eq!(values.len(), 2, "{batches:?}");
        assert!(values.iter().all(|v| (v - 0.1).abs() < 1e-9), "{values:?}");
    }

    #[tokio::test]
    async fn two_metrics_quantiles_with_one_label_set_are_invalid_input() {
        let svc = IrService::new(histogram_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 10, "from": "metrics", "range": { "from": 100, "to": 1000 },
            "step": "1000ms", "result": "series",
            "pipeline": [
                { "where": { "field": "metric.name", "op": "in", "value": ["latency", "solo"] } },
                { "histogram_quantile": { "q": 0.5, "step": "1000ms", "mode": "instant", "as": "p50" } },
                { "filter": { "op": "ge", "value": 0.0 } }
            ]
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let err = QuerierError::from(df.collect().await.unwrap_err());
        assert!(
            matches!(&err, QuerierError::InvalidInput(m) if m.contains("same labelset")),
            "{err}"
        );
    }

    fn histogram_points_ctx(kind: &str, rows: &[(&str, i64, &[i64])]) -> SessionContext {
        points_ctx(histogram_points(kind, rows))
    }

    fn points_ctx(batch: RecordBatch) -> SessionContext {
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("metrics".to_string(), Arc::new(table))
            .unwrap();
        let ctx = SessionContext::new();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    async fn p50(ctx: SessionContext, mode: &str) -> Vec<f64> {
        let d = doc(serde_json::json!({
            "irVersion": 3, "from": "metrics", "range": { "from": 100, "to": 1000 },
            "result": "series",
            "pipeline": [
                { "histogram_quantile": { "q": 0.5, "step": "1000ms", "mode": mode, "as": "p50" } }
            ]
        }));
        let (df, _) = IrService::new(ctx)
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .unwrap();
        histogram_value(&df.collect().await.unwrap(), "p50", None)
    }

    /// Regression (hive NaN): one service, two cumulative series of one metric,
    /// each differenced against itself before the merge (see [`HIVE_SERIES`]).
    #[tokio::test]
    async fn histogram_quantile_rate_mode_differences_each_series_of_one_service() {
        let vs = p50(histogram_points_ctx("histogram", HIVE_SERIES), "rate").await;
        assert_eq!(vs, vec![HIVE_MERGED_P50]);
    }

    /// The same through `IrService::query`, the path `POST /api/v1/query` takes.
    #[tokio::test]
    async fn histogram_quantile_rate_mode_query_merges_per_series_increases() {
        let svc = IrService::new(histogram_points_ctx("histogram", HIVE_SERIES));
        let params = IrQueryParams {
            document: serde_json::json!({
                "irVersion": 3, "from": "metrics", "range": { "from": 100, "to": 1000 },
                "result": "series",
                "pipeline": [{ "histogram_quantile": {
                    "q": 0.5, "by": ["service.name"], "step": "1000ms", "mode": "rate", "as": "p50"
                } }]
            }),
            now_ns: 0,
            page: None,
        };
        let (batches, _, _) = svc.query(&params, "t", "d").await.unwrap();
        assert_eq!(
            histogram_value(&batches, "p50", Some("svc")),
            vec![HIVE_MERGED_P50]
        );
    }

    /// `metric.name` is not a label of the stage's output: a later stage
    /// naming it is refused by the validator (400), not the planner.
    #[tokio::test]
    async fn a_stage_after_histogram_quantile_cannot_read_metric_name() {
        let svc = IrService::new(histogram_points_ctx("histogram", HIVE_SERIES));
        let d = doc(serde_json::json!({
            "irVersion": 3, "from": "metrics", "range": { "from": 100, "to": 1000 },
            "result": "series",
            "pipeline": [
                { "histogram_quantile": { "q": 0.5, "step": "1000ms", "as": "p50" } },
                { "where": { "field": "metric.name", "op": "eq", "value": "lat" } }
            ]
        }));
        let err = svc.plan(&d, "t", "d", 0).await.map(|_| ()).unwrap_err();
        assert!(matches!(&err, QuerierError::InvalidInput(_)), "{err}");
    }

    /// The output labels are the `by` fields; `metric.name` only keeps
    /// groups of different metrics apart.
    #[tokio::test]
    async fn histogram_quantile_series_labels_are_the_by_fields() {
        let d = doc(serde_json::json!({
            "irVersion": 3, "from": "metrics", "range": { "from": 100, "to": 1000 },
            "result": "series",
            "pipeline": [{ "histogram_quantile": {
                "q": 0.5, "by": ["service.name"], "step": "1000ms", "as": "p50"
            } }]
        }));
        let svc = IrService::new(histogram_ctx());
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let got: Vec<&str> = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(got, ["bucket", "__labels", "value"]);
    }

    /// Instant mode reads each series' latest point in `(t - lookback, t]`;
    /// without `lookback` the window is the step.
    #[tokio::test]
    async fn histogram_quantile_instant_lookback_reaches_past_the_step() {
        const S: i64 = 1_000_000_000;
        let rows: &[(&str, i64, &[i64])] =
            &[("s", 10 * S, &[0, 0, 4, 0]), ("s", 20 * S, &[0, 4, 0, 0])];
        for (lookback, want) in [(None, vec![]), (Some("15s"), vec![1.5])] {
            let mut hq =
                serde_json::json!({ "q": 0.5, "step": "5s", "mode": "instant", "as": "p50" });
            if let Some(l) = lookback {
                hq["lookback"] = l.into();
            }
            let d = doc(serde_json::json!({
                "irVersion": 10, "from": "metrics", "result": "series",
                "range": { "from": 30 * S, "to": 30 * S },
                "pipeline": [{ "histogram_quantile": hq }]
            }));
            let svc = IrService::new(histogram_points_ctx("histogram", rows));
            let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
            // The 20s point [0,4,0,0]: rank 2 of 4 halfway into (1, 2].
            assert_eq!(
                histogram_value(&df.collect().await.unwrap(), "p50", None),
                want
            );
        }
    }

    #[tokio::test]
    async fn histogram_quantile_rate_window_widens_the_step() {
        const S: i64 = 1_000_000_000;
        let rows: &[(&str, i64, &[i64])] = &[
            ("s", 10 * S, &[1, 0, 0, 0]),
            ("s", 20 * S, &[1, 1, 0, 0]),
            ("s", 30 * S, &[1, 1, 5, 0]),
        ];
        for (window, want) in [(None, vec![]), (Some("20s"), vec![3.0])] {
            let mut hq = serde_json::json!({ "q": 0.5, "step": "10s", "as": "p50" });
            if let Some(w) = window {
                hq["window"] = w.into();
            }
            let d = doc(serde_json::json!({
                "irVersion": 10, "from": "metrics", "result": "series",
                "range": { "from": 30 * S, "to": 30 * S },
                "pipeline": [{ "histogram_quantile": hq }]
            }));
            let svc = IrService::new(histogram_points_ctx("histogram", rows));
            let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
            // (20s, 30s] holds one point; (10s, 30s] adds [0,0,5,0]: median 2 + 2·2.5/5.
            assert_eq!(
                histogram_value(&df.collect().await.unwrap(), "p50", None),
                want
            );
        }
    }

    #[tokio::test]
    async fn histogram_quantile_over_an_exponential_histogram_uses_its_buckets() {
        // Latest point: 1 in (1, 2] and 3 in (2, 4]; the median lies in (2, 4].
        let rows: &[(&str, i64, &[i64])] = &[("x", 10, &[1, 3])];
        let vs = p50(
            histogram_points_ctx("exponential_histogram", rows),
            "instant",
        )
        .await;
        assert!(vs.len() == 1 && vs[0] > 2.0 && vs[0] <= 4.0, "{vs:?}");
    }

    /// `histogram_fraction(0, 3, …)` per service at the one instant 1000,
    /// reading the 5m before it, through the IR.
    async fn fraction_ir(ctx: SessionContext, mode: &str) -> Vec<f64> {
        fraction_ir_between(ctx, mode, 0.0, 3.0).await
    }

    async fn fraction_ir_between(
        ctx: SessionContext,
        mode: &str,
        lower: f64,
        upper: f64,
    ) -> Vec<f64> {
        let mut hf = serde_json::json!({
            "lower": lower, "upper": upper, "by": ["service.name"], "step": "1us", "mode": mode, "as": "f"
        });
        hf[if mode == "rate" { "window" } else { "lookback" }] = "5m".into();
        let d = doc(serde_json::json!({
            "irVersion": 10, "from": "metrics", "result": "series",
            "range": { "from": 1000, "to": 1000 },
            "pipeline": [{ "histogram_fraction": hf }]
        }));
        let (df, _) = IrService::new(ctx)
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .unwrap();
        histogram_value(&df.collect().await.unwrap(), "f", Some("svc"))
    }

    /// One of `histogram_avg`/`histogram_stddev`/`histogram_stdvar` per
    /// service at the instant 1000, reading each series' latest point in the
    /// 5m before it, through the IR.
    async fn moment_ir(ctx: SessionContext, stage: &str) -> Vec<f64> {
        let mut stage_doc = serde_json::Map::new();
        stage_doc.insert(
            stage.to_string(),
            serde_json::json!({
                "by": ["service.name"], "step": "1us", "mode": "instant", "lookback": "5m", "as": "v"
            }),
        );
        let d = doc(serde_json::json!({
            "irVersion": 16, "from": "metrics", "result": "series",
            "range": { "from": 1000, "to": 1000 },
            "pipeline": [stage_doc]
        }));
        let (df, _) = IrService::new(ctx)
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .unwrap();
        histogram_value(&df.collect().await.unwrap(), "v", Some("svc"))
    }

    /// Scale-0 buckets (1, 2] and (2, 4] hold two observations each, with a
    /// recorded sum of 6 (mean 1.5): the bucket representatives are sqrt(2)
    /// and 2 sqrt(2).
    fn moment_ctx(kind: &str) -> SessionContext {
        let counts: &[i64] = if kind == "exponential_histogram" {
            &[2, 2]
        } else {
            &[2, 2, 0, 0]
        };
        points_ctx(with_sum(
            histogram_points(kind, &[("x", 10, counts)]),
            &[6.0],
        ))
    }

    #[tokio::test]
    async fn histogram_avg_is_sum_over_count() {
        let got = moment_ir(moment_ctx("exponential_histogram"), "histogram_avg").await;
        assert_close(&got, &[1.5], "exponential avg");
        let got = moment_ir(moment_ctx("histogram"), "histogram_avg").await;
        assert_close(&got, &[1.5], "explicit avg");
    }

    #[tokio::test]
    async fn histogram_stdvar_and_stddev_use_geometric_bucket_midpoints() {
        let sq2 = std::f64::consts::SQRT_2;
        let var = ((sq2 - 1.5).powi(2) + (2.0 * sq2 - 1.5).powi(2)) / 2.0;
        let got = moment_ir(moment_ctx("exponential_histogram"), "histogram_stdvar").await;
        assert_close(&got, &[var], "stdvar");
        let got = moment_ir(moment_ctx("exponential_histogram"), "histogram_stddev").await;
        assert_close(&got, &[var.sqrt()], "stddev");
    }

    #[tokio::test]
    async fn histogram_stddev_of_explicit_buckets_has_no_sample() {
        let got = moment_ir(moment_ctx("histogram"), "histogram_stddev").await;
        assert!(got.is_empty(), "{got:?}");
    }

    /// `got` equals `want` element-wise, to floating-point noise.
    fn assert_close(got: &[f64], want: &[f64], what: &str) {
        assert!(
            got.len() == want.len() && got.iter().zip(want).all(|(g, w)| (g - w).abs() < 1e-12),
            "{what}: got {got:?}, want {want:?}"
        );
    }

    /// Fractions over [`HIVE_SERIES`] in `(0, 3]`. The latest points are
    /// a = [3, 4, 2, 0] and b = [0, 2, 6, 0] (merged [3, 6, 8, 0], total 17);
    /// the merged increase is [2, 5, 7, 0] (total 14).
    ///
    /// - Explicit bounds [1, 2, 4] interpolate linearly, so (0, 3] holds all of
    ///   the first two buckets and half of (2, 4]: instant (3 + 6 + 8/2) / 17 =
    ///   13/17, rate (2 + 5 + 7/2) / 14 = 3/4.
    /// - Exponential buckets at scale 0 are (1, 2], (2, 4], (4, 8], (8, 16] and
    ///   interpolate geometrically, so (0, 3] holds the first bucket and
    ///   log2(3) - 1 of (2, 4]: instant (3 + 6 * (log2 3 - 1)) / 17, rate
    ///   (2 + 5 * (log2 3 - 1)) / 14.
    #[tokio::test]
    async fn histogram_fraction_over_the_hive_series() {
        let share_of_2_4 = 3f64.log2() - 1.0;
        let want = [
            ("histogram", 13.0 / 17.0, 0.75),
            (
                "exponential_histogram",
                (3.0 + 6.0 * share_of_2_4) / 17.0,
                (2.0 + 5.0 * share_of_2_4) / 14.0,
            ),
        ];
        for (kind, instant, rate) in want {
            let ctx = || histogram_points_ctx(kind, HIVE_SERIES);
            assert_close(&fraction_ir(ctx(), "instant").await, &[instant], kind);
            assert_close(&fraction_ir(ctx(), "rate").await, &[rate], kind);
        }
    }

    /// Exponential buckets interpolate on a log scale, as Prometheus'
    /// `Bucket.FractionBelow` does. The fixture is scale 0, offset 0, so
    /// bucket k is (2^k, 2^(k+1)]; `[0, 3]` takes all of (1, 2] and
    /// log2(3/2) / log2(4/2) = log2(1.5) of (2, 4]:
    /// - instant, a@30 + b@35 = [3, 6, 8, 0]: (3 + 6·log2 1.5) / 17 ≈ 0.382928
    /// - rate, merged increase [2, 5, 7, 0]: (2 + 5·log2 1.5) / 14 ≈ 0.351772
    #[tokio::test]
    async fn histogram_fraction_interpolates_exponential_buckets_on_a_log_scale() {
        let l = 1.5f64.log2();
        for (mode, want) in [
            ("instant", (3.0 + 6.0 * l) / 17.0),
            ("rate", (2.0 + 5.0 * l) / 14.0),
        ] {
            let got = fraction_ir(
                histogram_points_ctx("exponential_histogram", HIVE_SERIES),
                mode,
            )
            .await;
            assert!(
                got.len() == 1 && (got[0] - want).abs() < 1e-12,
                "{mode}: {got:?} != {want}"
            );
        }
    }

    #[tokio::test]
    async fn histogram_fraction_of_an_empty_interval_is_zero() {
        for kind in ["histogram", "exponential_histogram"] {
            for bound in [0.0, 1.5, 3.0, 100.0] {
                let got = fraction_ir_between(
                    histogram_points_ctx(kind, HIVE_SERIES),
                    "instant",
                    bound,
                    bound,
                )
                .await;
                assert_eq!(got, vec![0.0], "{kind} at {bound}");
            }
        }
    }

    /// A group mixing explicit and exponential histograms cannot merge; the
    /// `InvalidInput` raised inside execution stays a caller error (400).
    #[tokio::test]
    async fn mixing_explicit_and_exponential_histograms_is_invalid_input() {
        let rows: &[(&str, i64, &[i64])] = &[("a", 10, &[1, 1, 0, 0])];
        let explicit = histogram_points("histogram", rows);
        let exp = histogram_points("exponential_histogram", &[("b", 10, &[1, 1])]);
        let batch =
            datafusion::arrow::compute::concat_batches(&explicit.schema(), [&explicit, &exp])
                .unwrap();
        let params = IrQueryParams {
            document: serde_json::json!({
                "irVersion": 10, "from": "metrics", "result": "series",
                "range": { "from": 1000, "to": 1000 },
                "pipeline": [{ "histogram_fraction": {
                    "lower": 0, "upper": 3, "step": "1us", "mode": "instant",
                    "lookback": "5m", "as": "f"
                } }]
            }),
            now_ns: 0,
            page: None,
        };
        let err = IrService::new(points_ctx(batch))
            .query(&params, "t", "d")
            .await
            .map(|_| ())
            .unwrap_err();
        assert!(
            matches!(&err, QuerierError::InvalidInput(m) if m.contains("cannot be merged")),
            "{err:?}"
        );
    }

    /// [`HIVE_SERIES`] with `column` set to `values[0]` on series `a` and
    /// `values[1]` on series `b`.
    fn hive_per_series_ctx(column: &str, values: [&str; 2]) -> SessionContext {
        let batch = histogram_points("histogram", HIVE_SERIES);
        let mut cols = batch.columns().to_vec();
        cols[batch.schema().index_of(column).unwrap()] = Arc::new(StringArray::from_iter_values(
            HIVE_SERIES.iter().map(|r| values[usize::from(r.0 == "b")]),
        ));
        points_ctx(RecordBatch::try_new(batch.schema(), cols).unwrap())
    }

    /// A per-series histogram stage at the one instant 40, reading every
    /// point: the output columns and its `(bucket, __labels, value)` rows.
    async fn per_series_rows(
        ctx: SessionContext,
        stage: &str,
        mut body: serde_json::Value,
    ) -> Result<(Vec<String>, Vec<(i64, String, f64)>), QuerierError> {
        use datafusion::arrow::array::AsArray;
        use datafusion::arrow::datatypes::{Float64Type, TimestampNanosecondType};

        body["per_series"] = true.into();
        body["as"] = "v".into();
        let d = doc(serde_json::json!({
            "irVersion": 10, "from": "metrics", "result": "series",
            "range": { "from": 40, "to": 40 },
            "pipeline": [{ stage: body }]
        }));
        let (df, _) = IrService::new(ctx).plan(&d, "t", "d", 0).await?.unwrap();
        let names = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let mut rows = Vec::new();
        for b in df.collect().await? {
            let t = b.column(0).as_primitive::<TimestampNanosecondType>();
            let labels = b.column(1).as_string::<i32>();
            let v = b.column(2).as_primitive::<Float64Type>();
            rows.extend(
                (0..b.num_rows()).map(|i| (t.value(i), labels.value(i).to_string(), v.value(i))),
            );
        }
        Ok((names, rows))
    }

    /// `per_series` evaluates each series on its own and labels it by its own
    /// label set less `metric.name`, where the merged path would merge them.
    #[tokio::test]
    async fn histogram_stages_per_series_keep_each_series_labels() {
        let (a, b) = (r#"{"service.name":"svc-a"}"#, r#"{"service.name":"svc-b"}"#);
        // a's increase is [2,3,2,0] and b's [0,2,5,0] (see [`HIVE_SERIES`]).
        for (stage, body, want) in [
            (
                "histogram_quantile",
                serde_json::json!({ "q": 0.5, "step": "10ns", "window": "40ns" }),
                [1.5, 2.6],
            ),
            (
                "histogram_fraction",
                serde_json::json!({ "lower": 0, "upper": 2, "step": "10ns", "window": "40ns" }),
                [5.0 / 7.0, 2.0 / 7.0],
            ),
        ] {
            let ctx = hive_per_series_ctx("service_name", ["svc-a", "svc-b"]);
            let (names, rows) = per_series_rows(ctx, stage, body).await.unwrap();
            assert_eq!(names, ["bucket", "__labels", "value"], "{stage}");
            assert_eq!(rows.len(), 2, "{stage}: {rows:?}");
            for ((t, labels, v), (want_labels, want)) in
                rows.iter().zip([(a, want[0]), (b, want[1])])
            {
                assert_eq!((*t, labels.as_str()), (40, want_labels), "{stage}");
                assert!((v - want).abs() < 1e-9, "{stage}: {v} != {want}");
            }
        }
    }

    /// A merged `histogram_quantile` over `ctx` at the instant 40: the label
    /// sets of the Series it yields.
    async fn merged_series_labels(
        ctx: SessionContext,
        by: serde_json::Value,
    ) -> Result<Vec<String>, QuerierError> {
        use datafusion::arrow::array::AsArray;

        let d = doc(serde_json::json!({
            "irVersion": 10, "from": "metrics", "result": "series",
            "range": { "from": 40, "to": 40 },
            "pipeline": [{ "histogram_quantile": {
                "q": 0.5, "by": by, "step": "10ns", "window": "40ns", "mode": "rate", "as": "v"
            } }]
        }));
        let (df, _) = IrService::new(ctx).plan(&d, "t", "d", 0).await?.unwrap();
        let mut out = Vec::new();
        for b in df.collect().await? {
            let labels = b.column_by_name("__labels").unwrap().as_string::<i32>();
            out.extend(labels.iter().flatten().map(str::to_string));
        }
        Ok(out)
    }

    /// A series whose `by` column is null has no such label, as in Prometheus:
    /// never the string "null".
    #[tokio::test]
    async fn merged_histogram_series_omit_a_null_label() {
        let batch = histogram_points("histogram", HIVE_SERIES);
        let mut cols = batch.columns().to_vec();
        cols[batch.schema().index_of("service_name").unwrap()] = Arc::new(StringArray::from_iter(
            HIVE_SERIES.iter().map(|r| (r.0 == "a").then_some("svc-a")),
        ));
        let ctx = points_ctx(RecordBatch::try_new(batch.schema(), cols).unwrap());
        let labels = merged_series_labels(ctx, serde_json::json!(["service.name"]))
            .await
            .unwrap();
        assert_eq!(labels, [r#"{"service.name":"svc-a"}"#, "{}"]);
    }

    /// The labels carry the SignalDB name of a `by` field, not the column
    /// alias it is grouped under.
    #[tokio::test]
    async fn merged_histogram_series_keep_the_signaldb_label_name() {
        let ctx = hive_per_series_ctx("service_name", ["svc-a", "svc-b"]);
        let labels = merged_series_labels(ctx, serde_json::json!(["service.name"]))
            .await
            .unwrap();
        assert_eq!(
            labels,
            [r#"{"service.name":"svc-a"}"#, r#"{"service.name":"svc-b"}"#]
        );
    }

    /// Two metrics grouped to one label set are two series with it: a 400.
    #[tokio::test]
    async fn merged_histogram_series_with_one_label_set_are_invalid_input() {
        let ctx = hive_per_series_ctx("metric_name", ["lat", "lat2"]);
        let err = merged_series_labels(ctx, serde_json::json!([]))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, QuerierError::InvalidInput(m) if m.contains("same labelset")),
            "{err}"
        );
    }

    /// Two metrics whose series share every label but the name collide once
    /// the name is dropped: a 400, as in Prometheus.
    #[tokio::test]
    async fn per_series_histograms_with_one_label_set_are_invalid_input() {
        let ctx = hive_per_series_ctx("metric_name", ["lat", "lat2"]);
        let body = serde_json::json!({ "q": 0.5, "step": "10ns", "window": "40ns" });
        let err = per_series_rows(ctx, "histogram_quantile", body)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, QuerierError::InvalidInput(m) if m.contains("same labelset")),
            "{err}"
        );
    }

    /// Series stages read a per-series histogram as the Series it is.
    #[tokio::test]
    async fn series_stages_read_a_per_series_histogram() {
        use datafusion::arrow::array::AsArray;
        use datafusion::arrow::datatypes::Float64Type;

        let d = doc(serde_json::json!({
            "irVersion": 10, "from": "metrics", "result": "series",
            "range": { "from": 40, "to": 40 },
            "pipeline": [
                { "histogram_quantile": { "q": 0.5, "step": "10ns", "window": "40ns", "per_series": true, "as": "v" } },
                { "filter": { "op": "gt", "value": 2.0 } }
            ]
        }));
        let ctx = hive_per_series_ctx("service_name", ["svc-a", "svc-b"]);
        let (df, _) = IrService::new(ctx)
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .unwrap();
        let batches = df.collect().await.unwrap();
        let rows: Vec<(String, f64)> = batches
            .iter()
            .flat_map(|b| {
                let l = b.column_by_name("__labels").unwrap().as_string::<i32>();
                let v = b
                    .column_by_name("value")
                    .unwrap()
                    .as_primitive::<Float64Type>();
                (0..b.num_rows())
                    .map(|i| (l.value(i).to_string(), v.value(i)))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(rows, [(r#"{"service.name":"svc-b"}"#.to_string(), 2.6)]);
    }

    /// Per series, instant mode answers `histogram_fraction(0, 3, …)` from each
    /// series' latest point over bounds [1, 2, 4]: a = [3, 4, 2, 0] holds
    /// (3 + 4 + 2/2) / 9 = 8/9 in (0, 3], b = [0, 2, 6, 0] holds
    /// (0 + 2 + 6/2) / 8 = 5/8.
    #[tokio::test]
    async fn per_series_histogram_fraction_reads_each_latest_point() {
        let ctx = hive_per_series_ctx("service_name", ["svc-a", "svc-b"]);
        let d = doc(serde_json::json!({
            "irVersion": 10, "from": "metrics", "result": "series",
            "range": { "from": 1000, "to": 1000 },
            "pipeline": [{ "histogram_fraction": {
                "lower": 0, "upper": 3, "step": "1us", "mode": "instant", "lookback": "5m",
                "per_series": true, "as": "f" } }]
        }));
        let (df, _) = IrService::new(ctx)
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .unwrap();
        let ir: Vec<f64> = df
            .collect()
            .await
            .unwrap()
            .iter()
            .flat_map(|b| {
                use datafusion::arrow::array::AsArray;
                b.column(2)
                    .as_primitive::<datafusion::arrow::datatypes::Float64Type>()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_close(&ir, &[8.0 / 9.0, 5.0 / 8.0], "per series");
    }

    /// `irVersion` 10 per-series histogram shapes execute: each yields a
    /// Series frame.
    #[tokio::test]
    async fn per_series_histogram_shapes_execute() {
        let svc = IrService::new(histogram_ctx_with_leak("gauge"));
        for pipeline in [
            serde_json::json!([{ "histogram_quantile": { "q": 0.5, "step": "1m", "per_series": true, "as": "p" } }]),
            serde_json::json!([{ "histogram_fraction": {
                "lower": 0.0, "upper": 1.0, "step": "1m", "per_series": true, "as": "f" } }]),
        ] {
            let d = doc(serde_json::json!({
                "irVersion": 10, "from": "metrics", "step": "1m", "range": { "from": 0, "to": 1000 },
                "result": "series", "pipeline": pipeline
            }));
            let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
            let names: Vec<String> = df
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            assert_eq!(names, ["bucket", "__labels", "value"]);
            df.collect().await.unwrap();
        }
    }

    #[tokio::test]
    async fn histogram_quantile_on_metrics_ignores_gauge_and_sum_rows() {
        for leak in ["gauge", "sum"] {
            histogram_quantile_ignores_leak(leak).await;
        }
    }

    async fn histogram_quantile_ignores_leak(leak: &str) {
        let svc = IrService::new(histogram_ctx_with_leak(leak));
        let d = doc(serde_json::json!({
            "irVersion": 3, "from": "metrics", "range": { "from": 100, "to": 1000 },
            "result": "series",
            "pipeline": [
                { "where": { "field": "metric.name", "op": "eq", "value": "latency" } },
                { "histogram_quantile": { "q": 0.5, "by": ["service.name"], "step": "1000ms", "mode": "instant", "as": "p50" } }
            ]
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let batches = df.collect().await.unwrap();
        assert!(histogram_value(&batches, "p50", Some("leak")).is_empty());
        let svc_a = histogram_value(&batches, "p50", Some("svcA"));
        assert!(
            svc_a.len() == 1 && (svc_a[0] - 0.5).abs() < 1e-9,
            "{svc_a:?}"
        );
    }

    /// A summary in the selection is a 400, whichever statistic reads it and
    /// however the result is grouped: ungrouped, `by` (the summary is its own
    /// group) or `per_series`.
    #[tokio::test]
    async fn histogram_statistics_over_a_summary_are_invalid_input() {
        let svc = IrService::new(histogram_ctx_with_leak("summary"));
        let shapes = [
            serde_json::json!({}),
            serde_json::json!({ "by": ["service.name"] }),
            serde_json::json!({ "per_series": true }),
        ];
        for (stat, name) in [
            (serde_json::json!({ "q": 0.5 }), "histogram_quantile"),
            (
                serde_json::json!({ "lower": 0.0, "upper": 1.0 }),
                "histogram_fraction",
            ),
        ] {
            for shape in &shapes {
                let mut args = stat.clone();
                args["step"] = "1000ms".into();
                args["as"] = "v".into();
                args.as_object_mut()
                    .unwrap()
                    .extend(shape.as_object().unwrap().clone());
                let d = doc(serde_json::json!({
                    "irVersion": 10, "from": "metrics", "range": { "from": 100, "to": 1000 },
                    "result": "series",
                    "pipeline": [
                        { "where": { "field": "metric.name", "op": "eq", "value": "latency" } },
                        { name: args }
                    ]
                }));
                let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
                let err = QuerierError::from(df.collect().await.unwrap_err());
                assert!(
                    matches!(&err, QuerierError::InvalidInput(m)
                        if m.contains(&format!("{name} is not supported on summary metrics"))),
                    "{name} {shape}: {err:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn histogram_quantile_missing_table_is_empty() {
        let ctx = SessionContext::new();
        let sp = Arc::new(MemorySchemaProvider::new());
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);

        let svc = IrService::new(ctx);
        let d = doc(serde_json::json!({
            "irVersion": 3, "from": "metrics", "range": { "from": 0, "to": 1000 },
            "result": "series",
            "pipeline": [
                { "histogram_quantile": { "q": 0.5, "step": "1000ms", "as": "p50" } }
            ]
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let rows: usize = df
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 0);
    }

    #[tokio::test]
    async fn profiles_summary_rows_grouped_tables_and_series_execute() {
        let svc = IrService::new(profiles_ctx());
        let rows = doc(serde_json::json!({
            "irVersion": 1, "from": "profiles", "range": { "from": 0, "to": 1000 },
            "result": "rows", "fields": ["profile.id", "duration", "sample.type", "service.name"],
            "pipeline": [{ "where": { "field": "resource.deployment.environment", "op": "eq", "value": "prod" } }]
        }));
        let (df, _) = svc.plan(&rows, "t", "d", 0).await.unwrap().unwrap();
        assert_eq!(
            df.collect()
                .await
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>(),
            2
        );

        let table = doc(serde_json::json!({
            "irVersion": 1, "from": "profiles", "range": { "from": 0, "to": 1000 },
            "result": "table", "pipeline": [{ "aggregate": { "by": ["service.name"], "aggs": [{ "fn": "count", "as": "n" }] } }]
        }));
        let (df, _) = svc.plan(&table, "t", "d", 0).await.unwrap().unwrap();
        assert_eq!(
            df.collect()
                .await
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>(),
            2
        );

        let series = doc(serde_json::json!({
            "irVersion": 1, "from": "profiles", "range": { "from": 0, "to": 1000 },
            "result": "series", "pipeline": [{ "aggregate": { "by": ["service.name"], "aggs": [{ "fn": "count", "as": "n" }], "step": "1ms" } }]
        }));
        let (df, _) = svc.plan(&series, "t", "d", 0).await.unwrap().unwrap();
        assert_eq!(
            df.collect()
                .await
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>(),
            2
        );
    }

    /// profile-payload-access task 2.1 — flamegraph envelope end-to-end.
    #[tokio::test]
    async fn flamegraph_over_single_profile_id_returns_its_own_flamegraph() {
        let svc = IrService::new(profiles_ctx())
            .with_canonical_types(Arc::new(StaticLookup(canonical_types(&[]))));
        let params = IrQueryParams {
            document: serde_json::json!({
                "irVersion": 1, "from": "profiles", "range": { "from": 0, "to": 1000 },
                "result": "flamegraph",
                "pipeline": [{ "where": { "field": "profile.id", "op": "eq", "value": "p1" } }]
            }),
            now_ns: 0,
            page: None,
        };
        let (batches, _, _) = svc.query(&params, "t", "d").await.unwrap();
        assert_eq!(batches.len(), 1);
        let flamegraph = flamegraph_from_batch(&batches[0]);
        assert_eq!(flamegraph.total, 100);
        assert!(flamegraph.names.contains(&"main".to_string()));
        assert!(flamegraph.names.contains(&"foo".to_string()));
        assert!(!flamegraph.names.contains(&"bar".to_string()));
        assert!(!truncated_from_batch(&batches[0]));
    }

    #[tokio::test]
    async fn flamegraph_over_service_filter_aggregates_matching_profiles() {
        let svc = IrService::new(profiles_ctx())
            .with_canonical_types(Arc::new(StaticLookup(canonical_types(&[]))));
        let params = IrQueryParams {
            document: serde_json::json!({
                "irVersion": 1, "from": "profiles", "range": { "from": 0, "to": 1000 },
                "result": "flamegraph",
                "pipeline": [{ "where": { "field": "service.name", "op": "eq", "value": "api" } }]
            }),
            now_ns: 0,
            page: None,
        };
        let (batches, _, _) = svc.query(&params, "t", "d").await.unwrap();
        let flamegraph = flamegraph_from_batch(&batches[0]);
        // p1 (main/foo, 100) + p2 (main/bar, 50) match service=api; p3 (web) does not.
        assert_eq!(flamegraph.total, 150);
        assert!(flamegraph.names.contains(&"foo".to_string()));
        assert!(flamegraph.names.contains(&"bar".to_string()));
        assert!(!flamegraph.names.contains(&"baz".to_string()));
    }

    /// A `baseline` reads the same `where` over a second window: p1 (t=10,
    /// main/foo 100) is the baseline, p2 (t=20, main/bar 50) the comparison.
    #[tokio::test]
    async fn flamegraph_with_a_baseline_diffs_the_two_windows() {
        let svc = IrService::new(profiles_ctx())
            .with_canonical_types(Arc::new(StaticLookup(canonical_types(&[]))));
        let params = IrQueryParams {
            document: serde_json::json!({
                "irVersion": 13, "from": "profiles", "range": { "from": 15, "to": 1000 },
                "baseline": { "from": 0, "to": 15 },
                "result": "flamegraph",
                "pipeline": [{ "where": { "field": "service.name", "op": "eq", "value": "api" } }]
            }),
            now_ns: 0,
            page: None,
        };
        let (batches, window, _) = svc.query(&params, "t", "d").await.unwrap();
        assert_eq!((window.start_ns, window.end_ns), (15, 1000));
        let json = batches[0]
            .column_by_name("flamegraph_json")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .unwrap();
        let diff: common::profile::DiffFlamegraph = serde_json::from_str(json.value(0)).unwrap();
        assert_eq!((diff.left_ticks, diff.right_ticks), (100, 50));
        assert_eq!(diff.total, 150);
        assert!(diff.names.contains(&"foo".to_string()));
        assert!(diff.names.contains(&"bar".to_string()));
        assert!(!truncated_from_batch(&batches[0]));
    }

    /// The cap keeps the newest profiles: p2 (t=20) over p1 (t=10).
    #[tokio::test]
    async fn flamegraph_rows_keep_the_newest_profiles() {
        let svc = IrService::new(profiles_ctx())
            .with_canonical_types(Arc::new(StaticLookup(canonical_types(&[]))));
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "profiles", "range": { "from": 0, "to": 1000 },
            "result": "flamegraph",
            "pipeline": [{ "where": { "field": "service.name", "op": "eq", "value": "api" } }]
        }));
        let (batches, _) = svc.flamegraph_rows(&d, 0, "t", "d", 1).await.unwrap();
        let encoded = encode_flamegraph_batch(&batches, 1).unwrap();
        assert!(truncated_from_batch(&encoded));
        let flamegraph = flamegraph_from_batch(&encoded);
        assert_eq!(flamegraph.total, 50, "p2 (main/bar 50) is the newest");
        assert!(flamegraph.names.contains(&"bar".to_string()));
    }

    /// A mixed absolute/relative pair passes validation and is caught once
    /// resolved.
    #[tokio::test]
    async fn an_inverted_resolved_window_is_invalid_input() {
        let svc = IrService::new(profiles_ctx())
            .with_canonical_types(Arc::new(StaticLookup(canonical_types(&[]))));
        let params = IrQueryParams {
            document: serde_json::json!({
                "irVersion": 1, "from": "profiles", "range": { "from": "now", "to": 0 },
                "result": "flamegraph", "pipeline": []
            }),
            now_ns: 1_000,
            page: None,
        };
        let err = svc.query(&params, "t", "d").await.unwrap_err();
        assert!(
            matches!(err, QuerierError::InvalidInput(ref m) if m.contains("range.from")),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn an_inverted_baseline_window_is_invalid_input() {
        let svc = IrService::new(profiles_ctx())
            .with_canonical_types(Arc::new(StaticLookup(canonical_types(&[]))));
        let params = IrQueryParams {
            document: serde_json::json!({
                "irVersion": 13, "from": "profiles", "range": { "from": "now-1h", "to": "now" },
                "baseline": { "from": "now", "to": 0 },
                "result": "flamegraph", "pipeline": []
            }),
            now_ns: 1_000,
            page: None,
        };
        let err = svc.query(&params, "t", "d").await.unwrap_err();
        assert!(
            matches!(err, QuerierError::InvalidInput(ref m) if m.contains("baseline.from")),
            "got {err:?}"
        );
    }

    /// A cap-exceeding match set is aggregated up to the cap and flagged
    /// `truncated: true`, not returned unbounded or failed.
    #[test]
    fn flamegraph_batch_is_capped_and_flagged_truncated() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("profile_id", DataType::Utf8, false),
            Field::new("stacktraces_json", DataType::Utf8, false),
            Field::new("samples_json", DataType::Utf8, false),
        ]));
        let n: usize = 5;
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    (0..n).map(|i| format!("p{i}")).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(vec![
                    r#"[{"frames":[{"function_name":"main"}]}]"#;
                    n
                ])),
                Arc::new(StringArray::from(vec![
                    r#"[{"stacktrace_index":0,"values":[10]}]"#;
                    n
                ])),
            ],
        )
        .unwrap();

        let capped = encode_flamegraph_batch(&[batch], 2).unwrap();
        assert!(truncated_from_batch(&capped));
        let flamegraph = flamegraph_from_batch(&capped);
        assert_eq!(flamegraph.total, 20, "only the first 2 of 5 profiles kept");
    }

    fn flamegraph_from_batch(batch: &RecordBatch) -> common::profile::Flamegraph {
        let json = batch
            .column_by_name("flamegraph_json")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .unwrap();
        serde_json::from_str(json.value(0)).unwrap()
    }

    fn truncated_from_batch(batch: &RecordBatch) -> bool {
        batch
            .column_by_name("truncated")
            .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())
            .unwrap()
            .value(0)
    }

    // ---- OTel-native attribute scopes ----
    //
    // A LogRecord carries three attribute containers with different meanings:
    // resource (the emitting entity), scope (the instrumentation library), and
    // the record's own attributes. They must stay separately addressable, or a
    // UI cannot render them as the distinct things they are.

    /// Collect one string column's values, in row order.
    async fn column_values(df: DataFrame, column: &str) -> Vec<Option<String>> {
        let batches = df.collect().await.unwrap();
        let mut out = Vec::new();
        for batch in &batches {
            let idx = batch.schema().index_of(column).unwrap();
            let col = batch
                .column(idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap_or_else(|| panic!("column '{column}' is not a string column"));
            for i in 0..batch.num_rows() {
                out.push((!col.is_null(i)).then(|| col.value(i).to_string()));
            }
        }
        out
    }

    /// [`column_values`]'s `Int64` twin.
    async fn column_values_i64(df: DataFrame, column: &str) -> Vec<Option<i64>> {
        let batches = df.collect().await.unwrap();
        let mut out = Vec::new();
        for batch in &batches {
            let idx = batch.schema().index_of(column).unwrap();
            let col = batch
                .column(idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap_or_else(|| panic!("column '{column}' is not an int64 column"));
            for i in 0..batch.num_rows() {
                out.push((!col.is_null(i)).then(|| col.value(i)));
            }
        }
        out
    }

    /// A scope attribute is only reachable if `scope_attributes` is one of the
    /// source's containers. It was omitted, so grouping by an instrumentation
    /// scope attribute silently produced nothing.
    #[tokio::test]
    async fn scope_attributes_are_a_resolvable_container() {
        let svc = IrService::new(logs_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "table",
            "pipeline": [
                { "aggregate": { "by": ["otel.scope.flavor"], "aggs": [{ "fn": "count", "as": "n" }] } },
                { "order": [{ "of": "n", "dir": "desc" }] }
            ]
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let values = column_values(df, &safe_ident("otel.scope.flavor")).await;
        let mut found: Vec<String> = values.into_iter().flatten().collect();
        found.sort();
        assert_eq!(
            found,
            vec!["async".to_string(), "sync".to_string()],
            "scope attributes must be groupable"
        );
    }

    /// A qualified name addresses exactly one container. `deployment.environment`
    /// exists at both log and resource scope with different values; without
    /// qualification the coalesce hides which one answered.
    #[tokio::test]
    async fn a_container_qualifier_selects_one_scope() {
        let svc = IrService::new(logs_ctx());
        for (field, expected) in [
            ("resource.deployment.environment", "resource-prod"),
            ("log.deployment.environment", "prod"),
        ] {
            let d = doc(serde_json::json!({
                "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
                "result": "rows",
                "fields": [field],
                "pipeline": [{ "where": { "field": "service.name", "op": "eq", "value": "api" } }]
            }));
            let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
            let values = column_values(df, &safe_ident(field)).await;
            assert!(
                values.iter().all(|v| v.as_deref() == Some(expected)),
                "{field} must read only its own container, got {values:?}"
            );
        }
    }

    /// The qualifier must not shadow a physical column: `scope.name` is the
    /// `scope_name` column, not the key `name` inside `scope_attributes`.
    #[tokio::test]
    async fn a_physical_column_wins_over_a_container_qualifier() {
        let svc = IrService::new(logs_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["scope.name"],
            "pipeline": [{ "where": { "field": "service.name", "op": "eq", "value": "api" } }]
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let values = column_values(df, "scope_name").await;
        assert!(
            values.iter().all(|v| v.as_deref() == Some("app.http")),
            "scope.name must resolve to the scope_name column, got {values:?}"
        );
    }

    /// An unqualified name keeps coalescing across containers, so no document
    /// written before qualification existed changes meaning.
    #[tokio::test]
    async fn an_unqualified_name_still_coalesces_across_containers() {
        let svc = IrService::new(logs_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "table",
            "pipeline": [
                { "where": { "field": "deployment.environment", "op": "eq", "value": "prod" } },
                { "aggregate": { "by": ["service.name"], "aggs": [{ "fn": "count", "as": "n" }] } }
            ]
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total > 0, "the unqualified predicate must still match");
    }

    /// The default `rows` projection is the OTel LogRecord: trace context,
    /// severity (text *and* number), scope identity, and all three attribute
    /// containers — everything the explore UI renders per line.
    #[tokio::test]
    async fn logs_row_defaults_are_the_otel_log_record() {
        let svc = IrService::new(logs_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "pipeline": []
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let names: Vec<String> = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        for expected in [
            "timestamp",
            "body",
            "service_name",
            "severity_text",
            "severity_number",
            "trace_id",
            "span_id",
            "trace_flags",
            "scope_name",
            "scope_version",
            "log_attributes",
            "resource_attributes",
            "scope_attributes",
        ] {
            assert!(
                names.iter().any(|n| n == expected),
                "row defaults must include '{expected}', got {names:?}"
            );
        }
    }

    // --- IR-4: typed attribute resolution and retrieval (`otel-native-schema`
    // task 4.4) -----------------------------------------------------------

    fn canonical_types(entries: &[(&str, AttributeLevel, CanonicalType)]) -> CanonicalTypes {
        entries
            .iter()
            .map(
                |(key, level, canonical)| common::schema::type_authority::AttributeKeyType {
                    attr_key: (*key).to_string(),
                    level: *level,
                    canonical_type: *canonical,
                },
            )
            .collect()
    }

    /// A [`CanonicalTypeLookup`] that always returns the same fixed map,
    /// standing in for a fetched catalog result in a test.
    struct StaticLookup(CanonicalTypes);

    #[async_trait::async_trait]
    impl CanonicalTypeLookup for StaticLookup {
        async fn canonical_types(
            &self,
            _tenant_slug: &str,
            _dataset_slug: &str,
            _signal: &str,
        ) -> Result<CanonicalTypes, QuerierError> {
            Ok(self.0.clone())
        }
    }

    /// Plans `d` over `ctx` with `types` resolved as `IrService::query`'s
    /// typed-resolve path would (`AttributeTypeRequest::Resolve`), unlike
    /// every `svc.plan(...)` call elsewhere in this module, which stays on
    /// `AttributeTypeRequest::CompatOnly`.
    async fn plan_typed(ctx: &SessionContext, d: &Document, types: CanonicalTypes) -> DataFrame {
        let lookup: Arc<dyn CanonicalTypeLookup> = Arc::new(StaticLookup(types));
        plan_document(
            ctx,
            d,
            PlanRequest::new("t", "d", 0)
                .with_attribute_type_request(AttributeTypeRequest::Resolve(Some(lookup))),
        )
        .await
        .unwrap()
        .expect("typed table scans")
        .0
    }

    /// [`plan_typed`], collected — the plan-then-execute boilerplate every
    /// test that only needs the final rows (not the pre-collect plan text or
    /// schema) shares.
    async fn plan_typed_rows(
        ctx: &SessionContext,
        d: &Document,
        types: CanonicalTypes,
    ) -> Vec<RecordBatch> {
        plan_typed(ctx, d, types).await.collect().await.unwrap()
    }

    /// A typed-attribute-row builder: `row(&[("k", json!(1))])` is
    /// `Some({"k": 1})` — the shape every typed fixture's rows are built
    /// from.
    fn row(
        pairs: &[(&str, serde_json::Value)],
    ) -> Option<serde_json::Map<String, serde_json::Value>> {
        Some(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        )
    }

    /// The default typed-attribute placement every fixture in this module
    /// shares unless it overrides a specific key: a scalar goes to its
    /// canonical-type home, everything else (arrays, objects, nulls) falls
    /// to residue.
    fn standard_placement(observed: ObservedKind) -> Placement {
        match observed {
            ObservedKind::String => Placement::Home(CanonicalType::String),
            ObservedKind::Int64 => Placement::Home(CanonicalType::Int64),
            ObservedKind::Float64 => Placement::Home(CanonicalType::Float64),
            ObservedKind::Bool => Placement::Home(CanonicalType::Bool),
            _ => Placement::Residue { off_type: false },
        }
    }

    /// Appends `container`'s five typed columns (from `table`'s `version`
    /// schema), built from `rows` with `place`, onto `fields`/`columns` — the
    /// field/array assembly every typed fixture in this module repeats.
    fn extend_typed_container(
        fields: &mut Vec<Field>,
        columns: &mut Vec<ArrayRef>,
        table: &str,
        version: &str,
        container: &str,
        rows: &[Option<serde_json::Map<String, serde_json::Value>>],
        place: impl FnMut(&str, ObservedKind) -> Placement,
    ) {
        let (typed_fields, typed_arrays) =
            common::testing::typed_attribute_columns_from_with_placement(
                table, version, container, rows, place,
            );
        fields.extend(typed_fields);
        columns.extend(typed_arrays);
    }

    /// `common::schema::attribute_qualifier` is what discovery lists names
    /// with; the planner's `attr_prefixes` is what resolves them. They must
    /// say the same thing for every source and level.
    #[test]
    fn discovery_qualifiers_agree_with_the_planners_prefixes() {
        for source in ["logs", "traces", "profiles", "metrics", "exemplars"] {
            let plan = SourcePlan::for_source(source).expect("source");
            for level in [
                AttributeLevel::Record,
                AttributeLevel::Scope,
                AttributeLevel::Resource,
            ] {
                let prefixed: Vec<&str> = plan
                    .attr_prefixes
                    .iter()
                    .filter(|(_, container)| typed_attributes::container_level(container) == level)
                    .map(|(prefix, _)| *prefix)
                    .collect();
                let expected = common::schema::logical::attribute_qualifier(source, level)
                    .map(|q| format!("{q}."));
                assert_eq!(
                    prefixed,
                    expected.iter().map(String::as_str).collect::<Vec<_>>(),
                    "{source} {level:?}"
                );
                let has_container = plan
                    .containers
                    .iter()
                    .any(|c| typed_attributes::container_level(c) == level);
                assert_eq!(
                    common::schema::logical::level_is_addressable(source, level),
                    has_container,
                    "{source} {level:?} addressable iff it has a container"
                );
            }
        }
    }

    /// The invariant discovery exists to keep: every field `describe` lists
    /// is valid to reference by its listed name, with its listed type. Feeds
    /// every name `merge_fields` emits, per source and with types committed at
    /// several levels, through the planner's typed resolver.
    #[test]
    fn every_discovered_field_resolves_with_its_listed_type() {
        use common::catalog::AttributeStatsRecord;
        use common::discovery::{FieldOrigin, merge_fields};
        use std::collections::BTreeMap;

        let authority =
            |key: &str, level, canonical| common::schema::type_authority::AttributeKeyType {
                attr_key: key.to_string(),
                level,
                canonical_type: canonical,
            };
        let rows = vec![
            authority("k.int", AttributeLevel::Record, CanonicalType::Int64),
            authority("k.float", AttributeLevel::Record, CanonicalType::Float64),
            authority("k.res", AttributeLevel::Resource, CanonicalType::Bool),
            authority("k.scope", AttributeLevel::Scope, CanonicalType::Int64),
            authority("k.multi", AttributeLevel::Resource, CanonicalType::String),
            authority("k.multi", AttributeLevel::Scope, CanonicalType::Float64),
            authority("k.multi", AttributeLevel::Record, CanonicalType::Int64),
            // Names that begin with a source qualifier, and keys that collide
            // with a declared field at a level.
            authority(
                "log.file.path",
                AttributeLevel::Record,
                CanonicalType::Int64,
            ),
            authority("span.kind.x", AttributeLevel::Record, CanonicalType::Int64),
            authority("point.idx", AttributeLevel::Record, CanonicalType::Int64),
            authority(
                "resource.foo",
                AttributeLevel::Resource,
                CanonicalType::Int64,
            ),
            authority("name", AttributeLevel::Scope, CanonicalType::Int64),
            authority("name", AttributeLevel::Record, CanonicalType::Int64),
            authority("schema_url", AttributeLevel::Resource, CanonicalType::Int64),
        ];
        let stats = vec![AttributeStatsRecord {
            tenant_id: "t".to_string(),
            dataset_id: "d".to_string(),
            signal: "x".to_string(),
            attr_key: "plain.untyped".to_string(),
            present_rows: 1,
            total_rows: 1,
            distinct_estimate: 1,
            capped: false,
            query_hits: 0,
            promote_streak: 0,
            analyzed_span: None,
            updated_at: "2026-01-01 00:00:00".to_string(),
        }];

        let schema = LogicalSchema::core();
        for source in ["logs", "traces", "profiles", "metrics", "exemplars"] {
            let plan = SourcePlan::for_source(source).expect("source");
            let mut fields = vec![Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )];
            for container in plan.containers {
                fields.extend(
                    typed_columns(container)
                        .into_iter()
                        .map(|name| Field::new(name, DataType::Utf8, true)),
                );
            }
            for column in [
                "span_name",
                "scope_name",
                "scope_version",
                "scope_schema_url",
                "resource_schema_url",
            ] {
                fields.push(Field::new(column, DataType::Utf8, true));
            }
            let df_schema =
                datafusion::common::DFSchema::try_from(Schema::new(fields)).expect("schema");
            let resolver = SchemaResolver::new(&df_schema, &plan).with_typed(canonical_types(
                &rows
                    .iter()
                    .map(|r| (r.attr_key.as_str(), r.level, r.canonical_type))
                    .collect::<Vec<_>>(),
            ));

            let (listed, _) =
                merge_fields(source, &schema, &stats, &BTreeMap::new(), &rows, 10_000);
            let mut checked = 0;
            for field in &listed {
                let resolved = resolver
                    .resolve(source, &field.name)
                    .unwrap_or_else(|| panic!("{source}: `{}` does not resolve", field.name));
                if field.origin == FieldOrigin::Declared {
                    continue;
                }
                let Resolved::TypedAttribute {
                    homes, value_type, ..
                } = resolved
                else {
                    panic!(
                        "{source}: `{}` is not a typed attribute: {resolved:?}",
                        field.name
                    );
                };
                assert_eq!(
                    value_type,
                    logical_to_value_type(field.value_type),
                    "{source}: `{}` is listed as {:?}",
                    field.name,
                    field.value_type
                );
                assert_eq!(
                    homes.is_empty(),
                    field.origin != FieldOrigin::Authority,
                    "{source}: `{}` reads a typed home exactly when the authority typed it",
                    field.name
                );
                checked += 1;
            }
            assert!(
                checked >= 2,
                "{source}: only {checked} attribute fields listed"
            );

            // The scope-level columns are listed under the names that read them.
            if matches!(source, "logs" | "traces") {
                for (listed_name, column) in [
                    ("scope.name", "scope_name"),
                    ("scope.version", "scope_version"),
                ] {
                    assert!(
                        listed.iter().any(|f| f.name == listed_name),
                        "{source}: {listed_name}"
                    );
                    assert!(
                        matches!(resolver.resolve(source, listed_name), Some(Resolved::Column { name, .. }) if name == column),
                        "{source}: {listed_name} reads {column}"
                    );
                }
            }
            if source == "traces" {
                assert!(
                    matches!(resolver.resolve(source, "name"), Some(Resolved::Column { name, .. }) if name == "span_name"),
                    "bare `name` on traces is the span's"
                );
            }
        }
    }

    /// A `SchemaResolver` for `logs` over an all-typed, empty (no rows)
    /// schema: task 4.4's homes/promotion/exclusion rules are schema and
    /// type-map facts, so asserting `resolve()` directly is cheaper and more
    /// precise than round-tripping through a full plan and its data.
    fn typed_logs_resolver(types: CanonicalTypes) -> SchemaResolver {
        let mut fields = vec![Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )];
        for container in ["log_attributes", "scope_attributes", "resource_attributes"] {
            let (typed_fields, _) = common::testing::typed_attribute_columns_from(
                "logs",
                "physical-v4",
                container,
                &[],
            );
            fields.extend(typed_fields);
        }
        let df_schema =
            datafusion::common::DFSchema::try_from(Schema::new(fields)).expect("valid schema");
        let source = SourcePlan::for_source("logs").expect("logs source");
        SchemaResolver::new(&df_schema, &source).with_typed(types)
    }

    /// Unqualified resolution coalesces only the levels whose committed
    /// canonical type agrees with the first-recorded one, in container
    /// order; a differently-typed level is excluded from the coalesce but
    /// still reachable qualified.
    #[test]
    fn typed_attribute_unqualified_coalesces_same_typed_levels_and_excludes_others() {
        let types = canonical_types(&[
            ("priority", AttributeLevel::Record, CanonicalType::Int64),
            ("priority", AttributeLevel::Scope, CanonicalType::Int64),
            ("priority", AttributeLevel::Resource, CanonicalType::String),
        ]);
        let resolver = typed_logs_resolver(types);

        match resolver.resolve("", "priority") {
            Some(Resolved::TypedAttribute {
                homes,
                promoted,
                key,
                value_type,
            }) => {
                assert_eq!(
                    homes,
                    vec![
                        "log_attributes_int".to_string(),
                        "scope_attributes_int".to_string(),
                    ],
                    "same-typed levels coalesce in container order, the \
                     differently-typed resource level is excluded"
                );
                assert_eq!(promoted, vec![None, None]);
                assert_eq!(key, "priority");
                assert_eq!(value_type, ValueType::Int64);
            }
            other => panic!("expected a TypedAttribute, got {other:?}"),
        }

        match resolver.resolve("", "resource.priority") {
            Some(Resolved::TypedAttribute {
                homes, value_type, ..
            }) => {
                assert_eq!(homes, vec!["resource_attributes_str".to_string()]);
                assert_eq!(value_type, ValueType::String);
            }
            other => panic!("expected a qualified TypedAttribute, got {other:?}"),
        }
    }

    /// A key with no committed canonical type at any level resolves as a
    /// typed NULL rather than an error or a JSON-path fallback.
    #[test]
    fn typed_attribute_with_no_recorded_type_is_a_typed_null() {
        let resolver = typed_logs_resolver(CanonicalTypes::default());
        match resolver.resolve("", "unknown.attr") {
            Some(Resolved::TypedAttribute {
                homes,
                promoted,
                value_type,
                ..
            }) => {
                assert!(homes.is_empty());
                assert!(promoted.is_empty());
                assert_eq!(value_type, ValueType::String);
            }
            other => panic!("expected a typed null, got {other:?}"),
        }
    }

    /// A `logs` table on the typed layout with `http.status_code` recorded
    /// as `Int64` — row 0 holds a genuine int, row 1 an off-type string
    /// (forced to residue, mimicking what the writer's type authority does
    /// for a value that doesn't match the committed type).
    fn typed_attribute_logs_ctx() -> SessionContext {
        let mut fields = vec![Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )];
        let mut columns: Vec<ArrayRef> =
            vec![Arc::new(TimestampNanosecondArray::from(vec![10_i64, 20]))];

        extend_typed_container(
            &mut fields,
            &mut columns,
            "logs",
            "physical-v4",
            "log_attributes",
            &[
                row(&[
                    ("http.status_code", serde_json::json!(200)),
                    ("priority", serde_json::json!(7)),
                ]),
                row(&[("http.status_code", serde_json::json!("pending"))]),
            ],
            |key, observed| {
                if key == "http.status_code" && observed == ObservedKind::String {
                    Placement::Residue { off_type: true }
                } else {
                    standard_placement(observed)
                }
            },
        );

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        single_table_ctx("logs", schema, batch)
    }

    /// An `Int64`-canonical attribute reads as a real `Int64` column, with no
    /// `CAST` in the lowered plan — same-typed homes need no coercion.
    #[tokio::test]
    async fn typed_int64_attribute_reads_with_no_cast() {
        let ctx = typed_attribute_logs_ctx();
        let types = canonical_types(&[(
            "http.status_code",
            AttributeLevel::Record,
            CanonicalType::Int64,
        )]);
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows", "fields": ["http.status_code"],
            "pipeline": [{ "where": { "field": "http.status_code", "op": "eq", "value": 200 } }]
        }));
        let df = plan_typed(&ctx, &d, types).await;
        let plan_text = df.logical_plan().to_string();
        assert!(
            !plan_text.contains("CAST"),
            "a same-typed int64 read must not cast: {plan_text}"
        );
        let field_name = safe_ident("http.status_code");
        assert_eq!(
            df.schema()
                .field_with_unqualified_name(&field_name)
                .unwrap()
                .data_type(),
            &DataType::Int64
        );
        let values = column_values_i64(df, &field_name).await;
        assert_eq!(
            values,
            vec![Some(200)],
            "only the int64-typed row matches eq 200"
        );
    }

    /// An `eq` filter on a typed attribute with no promoted column keeps the
    /// plain coalesce shape (a bare `get_field(...) = <literal>`, the shape
    /// the warm-index probe recognizes); one with a promoted column lowers
    /// to the `IS NOT NULL`/`OR` disjunction that gives DataFusion a
    /// prunable predicate on it instead.
    #[tokio::test]
    async fn eq_filter_lowers_to_the_or_form_only_when_promoted() {
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows", "fields": ["service.tier"],
            "pipeline": [{ "where": { "field": "service.tier", "op": "eq", "value": "gold" } }]
        }));

        let unpromoted = typed_attribute_logs_ctx();
        let unpromoted_types = canonical_types(&[(
            "service.tier",
            AttributeLevel::Record,
            CanonicalType::String,
        )]);
        let plan_text = plan_typed(&unpromoted, &d, unpromoted_types)
            .await
            .logical_plan()
            .to_string();
        assert!(!plan_text.contains("IS NOT NULL"), "{plan_text}");
        assert!(!plan_text.contains(" OR "), "{plan_text}");

        let promoted = typed_promotion_logs_ctx([
            vec![Some("gold"), Some("silver")],
            vec![Some("legacy"), Some("legacy")],
        ]);
        let promoted_types = canonical_types(&[(
            "service.tier",
            AttributeLevel::Record,
            CanonicalType::String,
        )]);
        let plan_text = plan_typed(&promoted, &d, promoted_types)
            .await
            .logical_plan()
            .to_string();
        assert!(
            plan_text.contains("label_service_tier IS NOT NULL"),
            "{plan_text}"
        );
        assert!(plan_text.contains(" OR "), "{plan_text}");
    }

    /// An off-type value (a string on an `Int64`-declared key) reads as NULL
    /// through the typed home — `exists` is false for that row — but the
    /// value itself is never lost: the raw `log.attributes` bag still shows
    /// it, in the residue.
    #[tokio::test]
    async fn typed_attribute_off_type_value_reads_null_but_survives_in_the_raw_bag() {
        use datafusion::arrow::array::{BinaryArray, StructArray};

        let ctx = typed_attribute_logs_ctx();
        let types = canonical_types(&[(
            "http.status_code",
            AttributeLevel::Record,
            CanonicalType::Int64,
        )]);

        let exists_doc = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows", "fields": ["http.status_code"],
            "pipeline": [{ "where": { "field": "http.status_code", "op": "exists" } }]
        }));
        let batches = plan_typed_rows(&ctx, &exists_doc, types.clone()).await;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total, 1,
            "the off-type row must not satisfy `exists` on the typed read"
        );

        let bag_doc = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows", "fields": ["log.attributes"],
            "pipeline": []
        }));
        let batches = plan_typed_rows(&ctx, &bag_doc, types).await;
        let batch = &batches[0];
        let bag = batch
            .column_by_name(&safe_ident("log.attributes"))
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let residue = bag
            .column_by_name("residue")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert!(
            residue.is_null(0),
            "row 0's value went to its typed home, not residue"
        );
        assert!(
            !residue.is_null(1),
            "row 1's off-type value must survive in the residue"
        );
        let decoded = common::attrs::typed::decode_residue(residue.value(1)).unwrap();
        assert_eq!(
            decoded.get("http.status_code"),
            Some(&serde_json::json!("pending"))
        );
    }

    /// `drain_level` empties one process-global registry, so tests that
    /// drain it must not run concurrently.
    static ATTR_DEMAND_DRAIN: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// A `logs` table on the typed layout with all three attribute
    /// containers (`log_attributes`, `scope_attributes`,
    /// `resource_attributes`) so a per-level attribute-demand test can
    /// exercise a record-level filter, a `resource.`-qualified filter, and
    /// an unqualified key recorded at two levels — registered under
    /// `tenant`/`dataset` rather than the fixed `"t"`/`"d"` most tests share,
    /// so a parallel test's demand hits never land in this one's drain.
    fn attr_demand_logs_ctx(tenant: &str, dataset: &str) -> SessionContext {
        let mut fields = vec![Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )];
        let mut columns: Vec<ArrayRef> =
            vec![Arc::new(TimestampNanosecondArray::from(vec![10_i64]))];

        for (container, row_pairs) in [
            (
                "log_attributes",
                vec![
                    ("http.status_code", serde_json::json!(200)),
                    ("priority", serde_json::json!(7)),
                    ("region", serde_json::json!("eu")),
                ],
            ),
            ("scope_attributes", vec![("priority", serde_json::json!(7))]),
            (
                "resource_attributes",
                vec![("environment", serde_json::json!("prod"))],
            ),
        ] {
            extend_typed_container(
                &mut fields,
                &mut columns,
                "logs",
                "physical-v4",
                container,
                &[row(&row_pairs)],
                |_, observed| standard_placement(observed),
            );
        }

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("logs".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema(dataset, sp).unwrap();
        ctx.register_catalog(tenant, cat);
        ctx
    }

    /// A document filtering on a duplicated record-level attribute, a
    /// `resource.`-qualified one negated (`not`), an unqualified key
    /// committed at two levels, and a key with no committed type at all —
    /// plus a `group by` on a fifth key — must record exactly the (level,
    /// key) pairs that have a committed type, once each, no matter how many
    /// times a document repeats them (change: otel-native-schema layer 6).
    #[tokio::test]
    async fn ir_query_records_per_level_attribute_demand_from_filters_and_grouping() {
        let _drain = ATTR_DEMAND_DRAIN.lock().await;
        let tenant = "attr-demand-tenant";
        let dataset = "attr-demand-dataset";
        let ctx = attr_demand_logs_ctx(tenant, dataset);
        let types = canonical_types(&[
            (
                "http.status_code",
                AttributeLevel::Record,
                CanonicalType::Int64,
            ),
            ("priority", AttributeLevel::Record, CanonicalType::Int64),
            ("priority", AttributeLevel::Scope, CanonicalType::Int64),
            (
                "environment",
                AttributeLevel::Resource,
                CanonicalType::String,
            ),
            ("region", AttributeLevel::Record, CanonicalType::String),
        ]);
        let lookup: Arc<dyn CanonicalTypeLookup> = Arc::new(StaticLookup(types));

        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "table", "fields": ["region", "n"],
            "pipeline": [
                { "where": { "and": [
                    { "field": "http.status_code", "op": "eq", "value": 200 },
                    { "field": "http.status_code", "op": "eq", "value": 200 },
                    { "not": { "field": "resource.environment", "op": "eq", "value": "staging" } },
                    { "field": "priority", "op": "eq", "value": 7 },
                    { "field": "nope.attr", "op": "exists" }
                ]}},
                { "aggregate": { "by": ["region"], "aggs": [{ "fn": "count", "as": "n" }] } }
            ]
        }));

        let (df, ..) = plan_document(
            &ctx,
            &d,
            PlanRequest::new(tenant, dataset, 0)
                .with_attribute_type_request(AttributeTypeRequest::Resolve(Some(lookup))),
        )
        .await
        .unwrap()
        .expect("typed table scans");
        df.collect().await.unwrap();

        let drained: Vec<((AttributeLevel, String), u64)> = common::attr_demand::drain_level()
            .into_iter()
            .filter(|((t, d, s, _, _), _)| t == tenant && d == dataset && s == "logs")
            .map(|((_, _, _, level, key), count)| ((level, key), count))
            .collect();
        let recorded: HashMap<(AttributeLevel, String), u64> = drained.into_iter().collect();

        for (level, key) in [
            (AttributeLevel::Record, "http.status_code"),
            (AttributeLevel::Resource, "environment"),
            (AttributeLevel::Record, "priority"),
            (AttributeLevel::Scope, "priority"),
            (AttributeLevel::Record, "region"),
        ] {
            assert_eq!(
                recorded.get(&(level, key.to_string())),
                Some(&1),
                "expected exactly one hit for {level:?}/{key}, got {recorded:?}"
            );
        }
        assert!(
            !recorded.keys().any(|(_, key)| key == "nope.attr"),
            "a key with no committed type has nothing to promote: {recorded:?}"
        );
        assert_eq!(
            recorded.len(),
            5,
            "no unexpected demand entries: {recorded:?}"
        );
    }

    #[tokio::test]
    async fn correlate_target_filter_records_attribute_demand_under_target_signal() {
        let _drain = ATTR_DEMAND_DRAIN.lock().await;
        let tenant = "attr-demand-correlate-tenant";
        let dataset = "attr-demand-correlate-dataset";
        let mut fields = vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("trace_id", DataType::Utf8, true),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampNanosecondArray::from(vec![120_i64])),
            Arc::new(StringArray::from(vec![hex_id(1)])),
        ];
        extend_typed_container(
            &mut fields,
            &mut columns,
            "logs",
            "physical-v4",
            "log_attributes",
            &[row(&[("priority", serde_json::json!(7))])],
            |_, observed| standard_placement(observed),
        );
        let schema = Arc::new(Schema::new(fields));
        let logs = RecordBatch::try_new(schema, columns).unwrap();
        let ctx = SessionContext::new();
        let sp = Arc::new(MemorySchemaProvider::new());
        for (name, batch) in [("traces", signal_traces(false)), ("logs", logs)] {
            let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
            sp.register_table(name.to_string(), Arc::new(table))
                .unwrap();
        }
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema(dataset, sp).unwrap();
        ctx.register_catalog(tenant, cat);
        let lookup: Arc<dyn CanonicalTypeLookup> = Arc::new(StaticLookup(canonical_types(&[(
            "priority",
            AttributeLevel::Record,
            CanonicalType::Int64,
        )])));

        let d = doc(serde_json::json!({
            "irVersion": 11, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "pipeline": [{ "correlate": {
                "to": "logs", "on": "trace_id", "kind": "semi",
                "pipeline": [{ "where": { "field": "priority", "op": "eq", "value": 7 } }]
            }}]
        }));
        let (df, ..) = plan_document(
            &ctx,
            &d,
            PlanRequest::new(tenant, dataset, 0)
                .with_attribute_type_request(AttributeTypeRequest::Resolve(Some(lookup))),
        )
        .await
        .unwrap()
        .expect("typed table scans");
        df.collect().await.unwrap();

        let recorded: Vec<(String, AttributeLevel, String)> = common::attr_demand::drain_level()
            .into_iter()
            .filter(|((t, d, _, _, _), _)| t == tenant && d == dataset)
            .map(|((_, _, signal, level, key), _)| (signal, level, key))
            .collect();
        assert_eq!(
            recorded,
            vec![(
                "logs".to_string(),
                AttributeLevel::Record,
                "priority".to_string()
            )]
        );
    }

    fn catalog_under(
        tenant: &str,
        dataset: &str,
        tables: Vec<(&str, RecordBatch)>,
    ) -> SessionContext {
        let sp = Arc::new(MemorySchemaProvider::new());
        for (name, batch) in tables {
            let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
            sp.register_table(name.to_string(), Arc::new(table))
                .unwrap();
        }
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema(dataset, sp).unwrap();
        let ctx = SessionContext::new();
        ctx.register_catalog(tenant, cat);
        ctx
    }

    /// `traces` plus a one-row typed `logs` table carrying record-level
    /// `keys` (each `(key, value)`), under its own tenant/dataset.
    fn traces_and_typed_logs_ctx(
        tenant: &str,
        dataset: &str,
        keys: &[(&str, serde_json::Value)],
    ) -> SessionContext {
        let mut fields = vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("trace_id", DataType::Utf8, true),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampNanosecondArray::from(vec![120_i64])),
            Arc::new(StringArray::from(vec![hex_id(1)])),
        ];
        extend_typed_container(
            &mut fields,
            &mut columns,
            "logs",
            "physical-v4",
            "log_attributes",
            &[row(keys)],
            |_, observed| standard_placement(observed),
        );
        let logs = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        catalog_under(
            tenant,
            dataset,
            vec![("traces", signal_traces(false)), ("logs", logs)],
        )
    }

    fn record_lookup(keys: &[(&str, CanonicalType)]) -> Arc<dyn CanonicalTypeLookup> {
        let entries: Vec<_> = keys
            .iter()
            .map(|(key, ty)| (*key, AttributeLevel::Record, *ty))
            .collect();
        Arc::new(StaticLookup(canonical_types(&entries)))
    }

    async fn collect_documents(
        ctx: &SessionContext,
        tenant: &str,
        dataset: &str,
        lookup: &Arc<dyn CanonicalTypeLookup>,
        docs: impl IntoIterator<Item = serde_json::Value>,
    ) {
        for d in docs {
            let (df, ..) = plan_document(
                ctx,
                &doc(d),
                PlanRequest::new(tenant, dataset, 0).with_attribute_type_request(
                    AttributeTypeRequest::Resolve(Some(lookup.clone())),
                ),
            )
            .await
            .unwrap()
            .expect("typed table scans");
            df.collect().await.unwrap();
        }
    }

    /// Drains the recorded demand for `tenant`/`dataset` as
    /// `(signal, level, key) -> hits`.
    fn drain_demand(tenant: &str, dataset: &str) -> HashMap<(String, AttributeLevel, String), u64> {
        common::attr_demand::drain_level()
            .into_iter()
            .filter(|((t, d, _, _, _), _)| t == tenant && d == dataset)
            .map(|((_, _, signal, level, key), count)| ((signal, level, key), count))
            .collect()
    }

    fn demand_once(signal: &str, keys: &[&str]) -> HashMap<(String, AttributeLevel, String), u64> {
        keys.iter()
            .map(|key| {
                (
                    (signal.to_string(), AttributeLevel::Record, key.to_string()),
                    1,
                )
            })
            .collect()
    }

    fn inner_logs_correlate() -> serde_json::Value {
        serde_json::json!({ "correlate": {
            "to": "logs", "on": "trace_id", "kind": "inner", "pipeline": []
        }})
    }

    fn traces_document(
        result: &str,
        fields: &[&str],
        pipeline: Vec<serde_json::Value>,
    ) -> serde_json::Value {
        serde_json::json!({
            "irVersion": 11, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": result, "fields": fields, "pipeline": pipeline
        })
    }

    /// Every typed-attribute reference under a correlate target prefix
    /// (where, aggregate by/of, order, topk, fields, also after an
    /// aggregate) records demand once, under the target signal.
    #[tokio::test]
    async fn correlate_target_prefixed_references_record_demand_under_target_signal() {
        let _drain = ATTR_DEMAND_DRAIN.lock().await;
        let tenant = "attr-demand-prefixed-tenant";
        let dataset = "attr-demand-prefixed-dataset";
        let keys = [
            ("priority", serde_json::json!(7)),
            ("region", serde_json::json!("eu")),
            ("weight", serde_json::json!(1.5)),
            ("note", serde_json::json!("x")),
            ("rank", serde_json::json!(2)),
            ("score", serde_json::json!(3)),
            ("tag", serde_json::json!("t")),
        ];
        let ctx = traces_and_typed_logs_ctx(tenant, dataset, &keys);
        let lookup = record_lookup(&[
            ("priority", CanonicalType::Int64),
            ("region", CanonicalType::String),
            ("weight", CanonicalType::Float64),
            ("note", CanonicalType::String),
            ("rank", CanonicalType::Int64),
            ("score", CanonicalType::Int64),
            ("tag", CanonicalType::String),
        ]);
        let docs = [
            traces_document(
                "table",
                &["logs.region", "total"],
                vec![
                    inner_logs_correlate(),
                    serde_json::json!({ "where": { "field": "logs.priority", "op": "eq", "value": 7 } }),
                    serde_json::json!({ "aggregate": {
                        "by": ["logs.region"],
                        "aggs": [{ "fn": "sum", "of": "logs.weight", "as": "total" }]
                    }}),
                ],
            ),
            traces_document(
                "rows",
                &["trace_id", "logs.note"],
                vec![inner_logs_correlate()],
            ),
            traces_document(
                "rows",
                &["trace_id"],
                vec![
                    inner_logs_correlate(),
                    serde_json::json!({ "order": [{ "of": "logs.rank", "dir": "desc" }] }),
                ],
            ),
            traces_document(
                "rows",
                &["trace_id"],
                vec![
                    inner_logs_correlate(),
                    serde_json::json!({ "topk": { "n": 1, "of": "logs.score" } }),
                ],
            ),
            traces_document(
                "table",
                &["trace_id", "n", "logs.tag"],
                vec![
                    serde_json::json!({ "aggregate": {
                        "by": ["trace_id"], "aggs": [{ "fn": "count", "as": "n" }]
                    }}),
                    inner_logs_correlate(),
                ],
            ),
        ];
        collect_documents(&ctx, tenant, dataset, &lookup, docs).await;

        assert_eq!(
            drain_demand(tenant, dataset),
            demand_once(
                "logs",
                &[
                    "priority", "region", "weight", "note", "rank", "score", "tag"
                ]
            )
        );
    }

    /// A key used in the target sub-pipeline and again as `<target>.x` in
    /// the outer pipeline counts once, under the target signal.
    #[tokio::test]
    async fn correlate_target_and_prefixed_references_count_the_key_once() {
        let _drain = ATTR_DEMAND_DRAIN.lock().await;
        let tenant = "attr-demand-once-tenant";
        let dataset = "attr-demand-once-dataset";
        let ctx = traces_and_typed_logs_ctx(tenant, dataset, &[("priority", serde_json::json!(7))]);
        let lookup = record_lookup(&[("priority", CanonicalType::Int64)]);
        let d = traces_document(
            "rows",
            &["trace_id"],
            vec![
                serde_json::json!({ "correlate": {
                    "to": "logs", "on": "trace_id", "kind": "inner",
                    "pipeline": [{ "where": { "field": "priority", "op": "eq", "value": 7 } }]
                }}),
                serde_json::json!({ "where": { "field": "logs.priority", "op": "eq", "value": 7 } }),
            ],
        );
        collect_documents(&ctx, tenant, dataset, &lookup, [d]).await;

        assert_eq!(
            drain_demand(tenant, dataset),
            demand_once("logs", &["priority"])
        );
    }

    /// `parent.x` and `x` in one document are the same traces key, counted
    /// once under traces.
    #[tokio::test]
    async fn parent_prefixed_and_source_references_count_once_under_traces() {
        let _drain = ATTR_DEMAND_DRAIN.lock().await;
        let tenant = "attr-demand-parent-tenant";
        let dataset = "attr-demand-parent-dataset";
        let (_, batch) = correlate_attrs_batch();
        let batch =
            common::testing::to_typed_layout("traces", "physical-v5", &batch, &["span_attributes"]);
        let ctx = catalog_under(tenant, dataset, vec![("traces", batch)]);
        let lookup = record_lookup(&[("http.route", CanonicalType::String)]);
        let d = serde_json::json!({
            "irVersion": 8, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "rows", "fields": ["span_id"],
            "pipeline": [
                { "correlate": { "to": "parent", "kind": "inner" } },
                { "where": { "field": "parent.http.route", "op": "exists" } },
                { "where": { "field": "http.route", "op": "exists" } }
            ]
        });
        collect_documents(&ctx, tenant, dataset, &lookup, [d]).await;

        assert_eq!(
            drain_demand(tenant, dataset),
            demand_once("traces", &["http.route"])
        );
    }

    /// `fields` and an aggregate operand on the source signal record demand,
    /// while an extract-derived field and an aggregate alias record none.
    #[tokio::test]
    async fn source_fields_and_aggregate_operands_record_attribute_demand() {
        let _drain = ATTR_DEMAND_DRAIN.lock().await;
        let tenant = "attr-demand-source-ops-tenant";
        let dataset = "attr-demand-source-ops-dataset";
        let ctx = attr_demand_logs_ctx(tenant, dataset);
        let lookup = record_lookup(&[
            ("http.status_code", CanonicalType::Int64),
            ("region", CanonicalType::String),
        ]);
        let docs = [
            serde_json::json!({
                "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
                "result": "rows", "fields": ["region"]
            }),
            serde_json::json!({
                "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
                "result": "table", "fields": ["total"],
                "pipeline": [{ "aggregate": {
                    "aggs": [{ "fn": "sum", "of": "http.status_code", "as": "total" }]
                }}]
            }),
            serde_json::json!({
                "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
                "result": "table", "fields": ["total"],
                "pipeline": [
                    { "aggregate": { "aggs": [{ "fn": "count", "as": "total" }] }},
                    { "order": [{ "of": "total", "dir": "desc" }] }
                ]
            }),
        ];
        collect_documents(&ctx, tenant, dataset, &lookup, docs).await;

        assert_eq!(
            drain_demand(tenant, dataset),
            demand_once("logs", &["http.status_code", "region"])
        );
    }

    /// A `logs` table with `service.tier` (`String`-canonical) and
    /// `retry.count` (`Int64`-canonical), plus a `label_<key>` column for
    /// each — `label_service_tier` (relevant, since its type is `String`)
    /// and `label_retry_count` (a stray legacy label on a non-`String` key,
    /// which must be ignored). `labels` sets both label columns' values.
    fn typed_promotion_logs_ctx(labels: [Vec<Option<&str>>; 2]) -> SessionContext {
        let [label_service_tier, label_retry_count] = labels;
        let mut fields = vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("label_service_tier", DataType::Utf8, true),
            Field::new("label_retry_count", DataType::Utf8, true),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampNanosecondArray::from(vec![10_i64, 20])),
            Arc::new(StringArray::from(label_service_tier)),
            Arc::new(StringArray::from(label_retry_count)),
        ];
        extend_typed_container(
            &mut fields,
            &mut columns,
            "logs",
            "physical-v4",
            "log_attributes",
            &[
                row(&[
                    ("service.tier", serde_json::json!("gold")),
                    ("retry.count", serde_json::json!(3)),
                ]),
                row(&[
                    ("service.tier", serde_json::json!("silver")),
                    ("retry.count", serde_json::json!(5)),
                ]),
            ],
            |_, observed| standard_placement(observed),
        );

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        single_table_ctx("logs", schema, batch)
    }

    /// Promotion invariance on the typed layout: a `String` field reads
    /// identically whether its `label_<key>` column is all-NULL (not yet
    /// backfilled) or fully backfilled, and an `Int64` field's read ignores
    /// a stray `label_<key>` column entirely, since promotion only ever
    /// shadows a `String`-canonical home.
    #[tokio::test]
    async fn typed_attribute_promotion_is_invariant_and_ignored_off_type() {
        async fn tier_and_retry(
            ctx: &SessionContext,
            types: CanonicalTypes,
        ) -> (Vec<Option<String>>, Vec<Option<i64>>) {
            let d = doc(serde_json::json!({
                "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
                "result": "rows", "fields": ["service.tier", "retry.count"],
                "pipeline": []
            }));
            let tiers = column_values(
                plan_typed(ctx, &d, types.clone()).await,
                &safe_ident("service.tier"),
            )
            .await;
            let retries =
                column_values_i64(plan_typed(ctx, &d, types).await, &safe_ident("retry.count"))
                    .await;
            (tiers, retries)
        }

        let types = canonical_types(&[
            (
                "service.tier",
                AttributeLevel::Record,
                CanonicalType::String,
            ),
            ("retry.count", AttributeLevel::Record, CanonicalType::Int64),
        ]);
        let not_backfilled = typed_promotion_logs_ctx([vec![None, None], vec![None, None]]);
        let backfilled = typed_promotion_logs_ctx([
            vec![Some("gold"), Some("silver")],
            vec![Some("legacy"), Some("legacy")],
        ]);

        let (tiers_not_backfilled, retries_not_backfilled) =
            tier_and_retry(&not_backfilled, types.clone()).await;
        let (tiers_backfilled, retries_backfilled) = tier_and_retry(&backfilled, types).await;

        assert_eq!(
            tiers_not_backfilled,
            vec![Some("gold".to_string()), Some("silver".to_string())]
        );
        assert_eq!(
            tiers_not_backfilled, tiers_backfilled,
            "a String field reads identically whether or not label_service_tier is backfilled"
        );
        assert_eq!(retries_not_backfilled, vec![Some(3), Some(5)]);
        assert_eq!(
            retries_not_backfilled, retries_backfilled,
            "an Int64 field must ignore a stray label_retry_count column entirely"
        );
    }

    /// A `parent.`-scoped typed attribute reads through `correlate` the same
    /// way the child side does, against the `parent.<home>` columns the join
    /// produced — reuses [`correlate_attrs_batch`]'s root/child trace pair
    /// rewritten onto the typed layout, rather than a dedicated fixture.
    #[tokio::test]
    async fn typed_attribute_reads_through_a_parent_correlate_reference() {
        let (_, batch) = correlate_attrs_batch();
        let batch =
            common::testing::to_typed_layout("traces", "physical-v5", &batch, &["span_attributes"]);
        let ctx = single_table_ctx("traces", batch.schema(), batch.clone());
        let types =
            canonical_types(&[("http.route", AttributeLevel::Record, CanonicalType::String)]);
        let d = doc(serde_json::json!({
            "irVersion": 8, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "rows", "fields": ["span_id", "parent.http.route"],
            "pipeline": [{ "correlate": { "to": "parent", "kind": "inner" } }]
        }));
        let span_ids = column_values(plan_typed(&ctx, &d, types.clone()).await, "span_id").await;
        let parent_routes = column_values(
            plan_typed(&ctx, &d, types).await,
            &safe_ident("parent.http.route"),
        )
        .await;
        assert_eq!(span_ids, vec![Some("c0".to_string())]);
        assert_eq!(parent_routes, vec![Some("/checkout".to_string())]);
    }

    // otel-native-schema layer 6, task 6.1: promotion demote-and-still-correct
    // invariant, extended to per-level `attr_<level>_<key>` promoted columns
    // (D5). `Row` = (str_field, int_field, double_field, bool_field,
    // env_record, env_resource, tier): every scalar type at record level,
    // `env` at both record and resource level (row 3 has no record-level
    // `env`, falling through to resource), `tier` — single-level, only
    // promoted via the legacy `label_tier` column.
    mod typed_promotion_invariant {
        use super::*;
        use AttributeLevel::{Record, Resource};
        use common::schema::promoted_attr_column;
        use datafusion::arrow::array::{BooleanArray, Float64Array};

        type Row = (
            &'static str,
            i64,
            f64,
            bool,
            Option<&'static str>,
            &'static str,
            Option<&'static str>,
        );

        const ROWS: [Row; 4] = [
            ("apple", 1, 1.0, true, Some("prod"), "us", Some("gold")),
            ("apple", 2, 2.0, false, Some("stg"), "us", Some("silver")),
            ("banana", 3, 3.0, true, Some("prod"), "eu", None),
            ("banana", 4, 4.0, false, None, "eu", Some("bronze")),
        ];

        fn fixture_types() -> CanonicalTypes {
            canonical_types(&[
                ("str_field", Record, CanonicalType::String),
                ("int_field", Record, CanonicalType::Int64),
                ("double_field", Record, CanonicalType::Float64),
                ("bool_field", Record, CanonicalType::Bool),
                ("env", Record, CanonicalType::String),
                ("env", Resource, CanonicalType::String),
                ("tier", Record, CanonicalType::String),
            ])
        }

        /// `timestamp` plus the three typed attribute containers. `offset`
        /// is `rows`'s starting position within [`ROWS`], so a `timestamp`
        /// derived from it stays the same whether `rows` is the whole
        /// fixture or one slice of a multi-batch table (otherwise a split
        /// batch would repeat `timestamp` values a single-batch one never
        /// does, making the two layouts hold different data).
        fn typed_container_fields_and_columns(
            rows: &[Row],
            offset: usize,
        ) -> (Vec<Field>, Vec<ArrayRef>) {
            let n = rows.len();
            let mut fields = vec![Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )];
            let mut columns: Vec<ArrayRef> = vec![Arc::new(TimestampNanosecondArray::from(
                (0..n)
                    .map(|i| 10 * (offset + i + 1) as i64)
                    .collect::<Vec<_>>(),
            ))];
            let record_rows = rows
                .iter()
                .map(|&(s, i, d, b, env, _, tier)| {
                    let mut m = serde_json::json!({
                        "str_field": s, "int_field": i, "double_field": d, "bool_field": b,
                    });
                    let obj = m.as_object_mut().unwrap();
                    if let Some(env) = env {
                        obj.insert("env".to_string(), serde_json::json!(env));
                    }
                    if let Some(tier) = tier {
                        obj.insert("tier".to_string(), serde_json::json!(tier));
                    }
                    Some(obj.clone())
                })
                .collect::<Vec<_>>();
            let resource_rows = rows
                .iter()
                .map(|&(.., env_resource, _)| {
                    serde_json::json!({ "env": env_resource })
                        .as_object()
                        .cloned()
                })
                .collect::<Vec<_>>();
            for (container, container_rows) in [
                ("log_attributes", record_rows),
                ("scope_attributes", vec![None; n]),
                ("resource_attributes", resource_rows),
            ] {
                extend_typed_container(
                    &mut fields,
                    &mut columns,
                    "logs",
                    "physical-v4",
                    container,
                    &container_rows,
                    |_, observed| standard_placement(observed),
                );
            }
            (fields, columns)
        }

        /// Every promoted column, plus legacy `label_tier` (single-level,
        /// trusted) and `label_env` (multi-level, must be ignored — WRONG).
        fn promoted_fields_and_columns(rows: &[Row]) -> (Vec<Field>, Vec<ArrayRef>) {
            let name = |level, key| promoted_attr_column(level, key);
            let fields = vec![
                Field::new(name(Record, "str_field"), DataType::Utf8, true),
                Field::new(name(Record, "int_field"), DataType::Int64, true),
                Field::new(name(Record, "double_field"), DataType::Float64, true),
                Field::new(name(Record, "bool_field"), DataType::Boolean, true),
                Field::new(name(Record, "env"), DataType::Utf8, true),
                Field::new(name(Resource, "env"), DataType::Utf8, true),
                Field::new("label_tier", DataType::Utf8, true),
                Field::new("label_env", DataType::Utf8, true),
            ];
            macro_rules! arr {
                ($ty:ty, $get:expr) => {
                    Arc::new(<$ty>::from(rows.iter().map($get).collect::<Vec<_>>())) as ArrayRef
                };
            }
            let columns: Vec<ArrayRef> = vec![
                arr!(StringArray, |r| Some(r.0)),
                arr!(Int64Array, |r| Some(r.1)),
                arr!(Float64Array, |r| Some(r.2)),
                arr!(BooleanArray, |r| Some(r.3)),
                arr!(StringArray, |r| r.4),
                arr!(StringArray, |r| Some(r.5)),
                arr!(StringArray, |r| r.6),
                arr!(StringArray, |_| Some("WRONG")),
            ];
            (fields, columns)
        }

        /// A batch's promoted-column shape: none; all-NULL (pre-backfill);
        /// populated; or only `env`'s record-level home promoted.
        enum Promotion {
            Off,
            Unbackfilled,
            Backfilled,
            RecordEnvOnly,
        }

        fn typed_batch(rows: &[Row], offset: usize, promotion: Promotion) -> RecordBatch {
            let (mut fields, mut columns) = typed_container_fields_and_columns(rows, offset);
            match promotion {
                Promotion::Off => {}
                Promotion::RecordEnvOnly => {
                    fields.push(Field::new(
                        promoted_attr_column(Record, "env"),
                        DataType::Utf8,
                        true,
                    ));
                    columns.push(Arc::new(StringArray::from(
                        rows.iter().map(|r| r.4).collect::<Vec<_>>(),
                    )) as ArrayRef);
                }
                Promotion::Backfilled | Promotion::Unbackfilled => {
                    let (pf, pc) = promoted_fields_and_columns(rows);
                    fields.extend(pf);
                    columns.extend(if matches!(promotion, Promotion::Unbackfilled) {
                        pc.iter()
                            .map(|c| {
                                datafusion::arrow::array::new_null_array(c.data_type(), rows.len())
                            })
                            .collect()
                    } else {
                        pc
                    });
                }
            }
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
        }

        fn ctx(rows: &[Row], promotion: Promotion) -> SessionContext {
            let batch = typed_batch(rows, 0, promotion);
            single_table_ctx("logs", batch.schema(), batch)
        }

        /// Registers `batches` as one table across several files/row-groups.
        fn multi_batch_table_ctx(schema: Arc<Schema>, batches: Vec<RecordBatch>) -> SessionContext {
            let ctx = SessionContext::new();
            let table = MemTable::try_new(schema, vec![batches]).unwrap();
            let sp = Arc::new(MemorySchemaProvider::new());
            sp.register_table("logs".to_string(), Arc::new(table))
                .unwrap();
            let cat = Arc::new(MemoryCatalogProvider::new());
            cat.register_schema("d", sp).unwrap();
            ctx.register_catalog("t", cat);
            ctx
        }

        /// Promotion on, over two files: rows 0-1 unbackfilled, 2-3 backfilled.
        fn promotion_on_ctx() -> SessionContext {
            let batch1 = typed_batch(&ROWS[0..2], 0, Promotion::Unbackfilled);
            let batch2 = typed_batch(&ROWS[2..4], 2, Promotion::Backfilled);
            multi_batch_table_ctx(batch1.schema(), vec![batch1, batch2])
        }

        /// Asserts `d` (identified by `name`) plans to identical schema
        /// (name/type/nullability, not incidental metadata) and rows with
        /// promotion off vs on.
        async fn assert_promotion_invariant(d: &Document, name: &str) {
            fn shape(schema: &Schema) -> Vec<(String, DataType, bool)> {
                schema
                    .fields()
                    .iter()
                    .map(|f| (f.name().clone(), f.data_type().clone(), f.is_nullable()))
                    .collect()
            }
            let off = plan_typed(&ctx(&ROWS, Promotion::Off), d, fixture_types()).await;
            let on = plan_typed(&promotion_on_ctx(), d, fixture_types()).await;
            assert_eq!(
                shape(off.schema().as_arrow()),
                shape(on.schema().as_arrow()),
                "'{name}': promotion must not change the result schema"
            );
            let off_rows = sorted_rows(off.collect().await.unwrap());
            let on_rows = sorted_rows(on.collect().await.unwrap());
            assert!(
                !off_rows.is_empty(),
                "'{name}': the fixture must actually return rows"
            );
            assert_eq!(off_rows, on_rows, "'{name}': promotion changed the rows");
        }

        /// Every row of `batches`, debug-rendered and sorted.
        fn sorted_rows(batches: Vec<RecordBatch>) -> Vec<String> {
            let mut rows: Vec<String> = batches
                .iter()
                .flat_map(|b| (0..b.num_rows()).map(move |i| row_debug(b, i)))
                .collect();
            rows.sort();
            rows
        }

        fn row_debug(batch: &RecordBatch, i: usize) -> String {
            (0..batch.num_columns())
                .map(|c| {
                    datafusion::arrow::util::display::array_value_to_string(batch.column(c), i)
                        .unwrap_or_else(|_| "<err>".to_string())
                })
                .collect::<Vec<_>>()
                .join("|")
        }

        fn rows_doc(fields: &[&str], pipeline: serde_json::Value) -> serde_json::Value {
            serde_json::json!({
                "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
                "result": "rows", "fields": fields, "pipeline": pipeline
            })
        }

        fn where_doc(field: &str, op: &str, value: serde_json::Value) -> serde_json::Value {
            rows_doc(
                &[field],
                serde_json::json!([{ "where": { "field": field, "op": op, "value": value } }]),
            )
        }

        fn not_where_doc(field: &str, op: &str, value: serde_json::Value) -> serde_json::Value {
            rows_doc(
                &[field],
                serde_json::json!([{
                    "where": { "not": { "field": field, "op": op, "value": value } }
                }]),
            )
        }

        fn table_doc(pipeline: serde_json::Value) -> serde_json::Value {
            serde_json::json!({
                "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
                "result": "table", "pipeline": pipeline
            })
        }

        /// `sum`/`avg`/`min`/`max` of `int_field` and `double_field`.
        fn scalar_aggs() -> Vec<serde_json::Value> {
            ["int_field", "double_field"]
                .iter()
                .flat_map(|field| {
                    let suffix = if *field == "int_field" { "i" } else { "d" };
                    ["sum", "avg", "min", "max"].iter().map(move |func| {
                        serde_json::json!({ "fn": func, "of": field, "as": format!("{func}_{suffix}") })
                    })
                })
                .collect()
        }

        /// Documents this invariant must hold over: projections, filters
        /// per scalar type, aggregates, and the multi-level `env` key.
        fn scenarios() -> Vec<(&'static str, serde_json::Value)> {
            let all_fields = [
                "str_field",
                "int_field",
                "double_field",
                "bool_field",
                "env",
                "log.env",
                "resource.env",
                "tier",
            ];
            let mut docs = vec![
                (
                    "project every field",
                    rows_doc(&all_fields, serde_json::json!([])),
                ),
                (
                    "count by str_field",
                    table_doc(serde_json::json!([{ "aggregate": {
                        "by": ["str_field"], "aggs": [{ "fn": "count", "as": "n" }]
                    } }])),
                ),
                (
                    "numeric aggregates",
                    table_doc(
                        serde_json::json!([{ "aggregate": { "by": [], "aggs": scalar_aggs() } }]),
                    ),
                ),
                (
                    "count by bool_field",
                    table_doc(serde_json::json!([{ "aggregate": {
                        "by": ["bool_field"], "aggs": [{ "fn": "count", "as": "n" }]
                    } }])),
                ),
                (
                    "multi-level env unqualified and qualified",
                    rows_doc(&["env", "log.env", "resource.env"], serde_json::json!([])),
                ),
                (
                    "ordered by timestamp",
                    rows_doc(
                        &["timestamp", "str_field"],
                        serde_json::json!([{ "order": [{ "of": "timestamp", "dir": "asc" }] }]),
                    ),
                ),
            ];
            for (name, field, op, value) in [
                ("eq string", "str_field", "eq", serde_json::json!("apple")),
                ("eq int", "int_field", "eq", serde_json::json!(3)),
                ("eq double", "double_field", "eq", serde_json::json!(3.0)),
                ("eq bool", "bool_field", "eq", serde_json::json!(true)),
                ("ne string", "str_field", "ne", serde_json::json!("apple")),
                ("gt int", "int_field", "gt", serde_json::json!(2)),
                ("lte double", "double_field", "lte", serde_json::json!(2.0)),
            ] {
                docs.push((name, where_doc(field, op, value)));
            }
            docs.push((
                "not eq string",
                not_where_doc("str_field", "eq", serde_json::json!("apple")),
            ));
            docs
        }

        #[tokio::test]
        async fn promotion_is_invariant_across_scenarios() {
            for (name, doc_json) in scenarios() {
                assert_promotion_invariant(&doc(doc_json), name).await;
            }
        }

        /// `env` stays invariant whether only its record-level home is
        /// promoted, or both are.
        #[tokio::test]
        async fn multi_level_key_partial_and_full_promotion_agree() {
            let d = doc(rows_doc(&["env"], serde_json::json!([])));
            let expected = vec![
                Some("prod".to_string()),
                Some("stg".to_string()),
                Some("prod".to_string()),
                Some("eu".to_string()),
            ];
            for session in [promotion_on_ctx(), ctx(&ROWS, Promotion::RecordEnvOnly)] {
                let values = column_values(
                    plan_typed(&session, &d, fixture_types()).await,
                    &safe_ident("env"),
                )
                .await;
                assert_eq!(values, expected);
            }
        }

        /// `env` is committed at both `Record` and `Resource`, so a legacy
        /// `label_env` column must never stand in for it — even when it's
        /// the only promoted column present, with no per-level
        /// `attr_*_env` column to make `promoted_for` return early. Proves
        /// the `single_level` guard actually rejects a multi-level key's
        /// legacy label, the counterpart to `label_tier`'s single-level
        /// key, which `promotion_on_ctx` already trusts throughout
        /// [`scenarios`].
        #[tokio::test]
        async fn multi_level_legacy_label_is_ignored_without_attr_columns() {
            let (mut fields, mut columns) = typed_container_fields_and_columns(&ROWS, 0);
            fields.push(Field::new("label_env", DataType::Utf8, true));
            columns.push(Arc::new(StringArray::from(vec![Some("WRONG"); ROWS.len()])) as ArrayRef);
            let schema = Arc::new(Schema::new(fields));
            let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
            let with_label = single_table_ctx("logs", schema, batch);

            let d = doc(rows_doc(&["env"], serde_json::json!([])));
            let with_label_values = column_values(
                plan_typed(&with_label, &d, fixture_types()).await,
                &safe_ident("env"),
            )
            .await;
            let off_values = column_values(
                plan_typed(&ctx(&ROWS, Promotion::Off), &d, fixture_types()).await,
                &safe_ident("env"),
            )
            .await;
            assert_eq!(
                with_label_values, off_values,
                "label_env must be ignored for a key recorded at more than one level"
            );
        }

        /// The invariant holds on genuinely redundant data, but that alone
        /// can't tell a promoted column that's actually read apart from one
        /// silently skipped (e.g. always NULL, or equal to the home by
        /// coincidence): assert the backfilled context's plan text
        /// references `attr_record_str_field` for both a projection and a
        /// filter, the way `promotion_invariance_same_result` and
        /// `scope_qualified_attribute_resolves_to_promoted_column` already
        /// do for the legacy `label_*` columns.
        #[tokio::test]
        async fn promoted_columns_are_referenced_in_the_plan() {
            let backfilled = ctx(&ROWS, Promotion::Backfilled);
            let promoted_str_field = promoted_attr_column(Record, "str_field");

            for (name, d) in [
                (
                    "projection",
                    doc(rows_doc(&["str_field"], serde_json::json!([]))),
                ),
                (
                    "filter",
                    doc(where_doc("str_field", "eq", serde_json::json!("apple"))),
                ),
            ] {
                let plan = format!(
                    "{}",
                    plan_typed(&backfilled, &d, fixture_types())
                        .await
                        .logical_plan()
                        .display_indent()
                );
                assert!(
                    plan.contains(&promoted_str_field),
                    "expected the promoted column in the {name} plan:\n{plan}"
                );
            }
        }

        /// A type-mismatched promoted column (`Utf8`, not `Int64`) is ignored.
        #[tokio::test]
        async fn a_type_mismatched_promoted_column_is_ignored() {
            let (mut fields, mut columns) = typed_container_fields_and_columns(&ROWS, 0);
            let mismatched = promoted_attr_column(Record, "int_field");
            fields.push(Field::new(&mismatched, DataType::Utf8, true));
            columns.push(Arc::new(StringArray::from(vec![Some("999"); ROWS.len()])) as ArrayRef);
            let schema = Arc::new(Schema::new(fields));
            let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
            let ctx = single_table_ctx("logs", schema, batch);

            let d = doc(rows_doc(&["int_field"], serde_json::json!([])));
            let values =
                column_values_i64(plan_typed(&ctx, &d, fixture_types()).await, "int_field").await;
            assert_eq!(
                values,
                vec![Some(1), Some(2), Some(3), Some(4)],
                "the type-mismatched promoted column must be ignored"
            );
        }
    }

    // --- IR-5: typed predicates compare against the field's canonical type
    // (`otel-native-schema` task 4.4) ---------------------------------------

    /// Like [`plan_typed`], but for a document expected to fail at plan
    /// time: returns the `Err` instead of panicking.
    async fn plan_typed_err(
        ctx: &SessionContext,
        d: &Document,
        types: CanonicalTypes,
    ) -> QuerierError {
        let lookup: Arc<dyn CanonicalTypeLookup> = Arc::new(StaticLookup(types));
        plan_document(
            ctx,
            d,
            PlanRequest::new("t", "d", 0)
                .with_attribute_type_request(AttributeTypeRequest::Resolve(Some(lookup))),
        )
        .await
        .expect_err("expected the document to be rejected")
    }

    fn typed_predicate_types() -> CanonicalTypes {
        canonical_types(&[
            (
                "service.tier",
                AttributeLevel::Record,
                CanonicalType::String,
            ),
            ("retry.count", AttributeLevel::Record, CanonicalType::Int64),
        ])
    }

    /// Reuses [`typed_promotion_logs_ctx`] with no `label_*` backfill, so
    /// `service.tier`/`retry.count` are served purely from their typed
    /// homes: rows `("gold", 3)` and `("silver", 5)`.
    fn typed_predicate_ctx() -> SessionContext {
        typed_promotion_logs_ctx([vec![None, None], vec![None, None]])
    }

    /// An unpromoted `gt`/`between` on an `Int64`-canonical key compares the
    /// typed home column directly against an `Int64` literal — no `CAST`.
    #[tokio::test]
    async fn typed_attribute_gt_and_between_compare_typed_column_with_no_cast() {
        let ctx = typed_predicate_ctx();
        let types = typed_predicate_types();

        let gt_doc = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows", "fields": ["retry.count"],
            "pipeline": [{ "where": { "field": "retry.count", "op": "gt", "value": 3 } }]
        }));
        let df = plan_typed(&ctx, &gt_doc, types.clone()).await;
        assert!(
            !df.logical_plan().to_string().contains("CAST"),
            "an unpromoted numeric comparison must not cast the typed home"
        );
        let values = column_values_i64(df, &safe_ident("retry.count")).await;
        assert_eq!(values, vec![Some(5)]);

        let between_doc = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows", "fields": ["retry.count"],
            "pipeline": [{ "where": { "field": "retry.count", "op": "between", "value": [3, 5] } }]
        }));
        let values = column_values_i64(
            plan_typed(&ctx, &between_doc, types).await,
            &safe_ident("retry.count"),
        )
        .await;
        assert_eq!(values, vec![Some(3), Some(5)]);
    }

    /// `eq` with an int literal, and `in` with an int-literal list, both
    /// compare against the typed home directly.
    #[tokio::test]
    async fn typed_attribute_eq_and_in_use_typed_column() {
        let ctx = typed_predicate_ctx();
        let types = typed_predicate_types();

        let eq_doc = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows", "fields": ["retry.count"],
            "pipeline": [{ "where": { "field": "retry.count", "op": "eq", "value": 3 } }]
        }));
        let values = column_values_i64(
            plan_typed(&ctx, &eq_doc, types.clone()).await,
            &safe_ident("retry.count"),
        )
        .await;
        assert_eq!(values, vec![Some(3)]);

        let in_doc = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows", "fields": ["retry.count"],
            "pipeline": [{ "where": { "field": "retry.count", "op": "in", "value": [3, 99] } }]
        }));
        let values = column_values_i64(
            plan_typed(&ctx, &in_doc, types).await,
            &safe_ident("retry.count"),
        )
        .await;
        assert_eq!(values, vec![Some(3)]);
    }

    /// A literal that can't be represented in the field's canonical type is
    /// a defined rejection naming the field and its type — never a silent
    /// cast (a fractional-vs-`Int64` case is `coerce`'s own concern, tested
    /// in `query-ir`'s `value.rs`; this only needs one representative case
    /// end to end through the planner).
    #[tokio::test]
    async fn typed_attribute_uncoercible_literal_is_rejected() {
        let ctx = typed_predicate_ctx();
        let types = typed_predicate_types();
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows", "fields": ["retry.count"],
            "pipeline": [{ "where": { "field": "retry.count", "op": "eq", "value": "abc" } }]
        }));
        let err = plan_typed_err(&ctx, &d, types).await.to_string();
        assert!(err.contains("retry.count"), "unexpected error: {err}");
        assert!(err.contains("int64"), "unexpected error: {err}");
    }

    /// `contains` needs a `String` field: a non-`String` `TypedAttribute` is
    /// a defined rejection rather than an implicit cast to string
    /// (`query-ir::validate`'s authoritative-type check — see its own unit
    /// tests for the isolated case; this exercises it end to end through
    /// the planner), but a `String`-canonical one works on its typed home.
    #[tokio::test]
    async fn typed_attribute_contains_rejects_non_string_and_works_on_string() {
        let ctx = typed_predicate_ctx();
        let types = typed_predicate_types();

        let contains_int = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows", "fields": ["retry.count"],
            "pipeline": [{ "where": { "field": "retry.count", "op": "contains", "value": "3" } }]
        }));
        let err = plan_typed_err(&ctx, &contains_int, types.clone())
            .await
            .to_string();
        assert!(err.contains("retry.count"), "unexpected error: {err}");

        let contains_str = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows", "fields": ["service.tier"],
            "pipeline": [{ "where": { "field": "service.tier", "op": "contains", "value": "gol" } }]
        }));
        let values = column_values(
            plan_typed(&ctx, &contains_str, types).await,
            &safe_ident("service.tier"),
        )
        .await;
        assert_eq!(values, vec![Some("gold".to_string())]);
    }

    /// `sum` over an `Int64`-canonical `TypedAttribute` reads the typed
    /// column with no cast; over a `String`-canonical one it is a defined
    /// rejection (`validate`'s numeric-operand check, since `TypedAttribute`
    /// is never advisory — never even reaches the planner).
    #[tokio::test]
    async fn typed_attribute_sum_uses_typed_column_and_rejects_string() {
        let ctx = typed_predicate_ctx();
        let types = typed_predicate_types();

        let sum_doc = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "table",
            "pipeline": [{ "aggregate": { "aggs": [
                { "fn": "sum", "of": "retry.count", "as": "total" }
            ] } }]
        }));
        let df = plan_typed(&ctx, &sum_doc, types.clone()).await;
        let plan_text = df.logical_plan().to_string();
        assert!(
            !plan_text.contains("CAST"),
            "a typed Int64 sum must not cast: {plan_text}"
        );
        let batches = df.collect().await.unwrap();
        let total = batches[0]
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("sum over an Int64 typed home stays Int64, uncast")
            .value(0);
        assert_eq!(total, 8);

        let sum_string_doc = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "table",
            "pipeline": [{ "aggregate": { "aggs": [
                { "fn": "sum", "of": "service.tier", "as": "total" }
            ] } }]
        }));
        let err = plan_typed_err(&ctx, &sum_string_doc, types)
            .await
            .to_string();
        assert!(
            err.contains("numeric") && err.contains("string"),
            "unexpected error: {err}"
        );
    }

    /// Compat mode (`IrService::plan`'s default `AttributeTypeRequest::
    /// CompatOnly`, no typed resolve) over the same typed table keeps
    /// accepting the legacy string comparison — task 4.4's typed predicates
    /// only apply once a table's types are actually resolved.
    #[tokio::test]
    async fn typed_attribute_compat_mode_still_accepts_string_literals() {
        let ctx = typed_predicate_ctx();
        let svc = IrService::new(ctx);
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows", "fields": ["retry.count"],
            "pipeline": [{ "where": { "field": "retry.count", "op": "eq", "value": "3" } }]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let values = column_values(df, &safe_ident("retry.count")).await;
        assert_eq!(values, vec![Some("3".to_string())]);
    }

    /// A `logs` table with `log_attributes` on the typed layout, holding one
    /// row whose attributes exercise every raw-accessor case: a key in its
    /// typed home (`http.method`), an off-type string on a key the type
    /// authority has declared `Int64` (`retries` — forced to residue despite
    /// its `String` observed kind, `off_type: true`), an array (`tags`), and
    /// a bytes carrier (`payload`) — the last three all land in the residue.
    fn logs_ctx_typed_residue() -> SessionContext {
        let mut fields = vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("trace_id", DataType::Utf8, true),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampNanosecondArray::from(vec![10_i64])),
            Arc::new(StringArray::from(vec![Some("t1")])),
        ];
        extend_typed_container(
            &mut fields,
            &mut columns,
            "logs",
            "physical-v4",
            "log_attributes",
            &[row(&[
                ("http.method", serde_json::json!("GET")),
                ("retries", serde_json::json!("three")),
                ("tags", serde_json::json!(["a", "b"])),
                (
                    "payload",
                    serde_json::json!({"$otlp_type": "bytes", "base64": "AP9h"}),
                ),
            ])],
            |key, observed| {
                if key == "retries" {
                    Placement::Residue { off_type: true }
                } else {
                    standard_placement(observed)
                }
            },
        );

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        single_table_ctx("logs", schema, batch)
    }

    /// Reads `log_attributes` off a batch as its decoded JSON value — the
    /// typed layout's raw-accessor column is a `Utf8` JSON object; a `rows`
    /// batch always has exactly one row in these tests.
    fn log_attributes_json(batch: &RecordBatch) -> serde_json::Value {
        use common::attrs::typed::decode_typed_arrays;
        use datafusion::arrow::array::{BinaryArray, MapArray, StructArray};

        let column = batch.column_by_name("log_attributes").unwrap();
        let s = column.as_any().downcast_ref::<StructArray>().unwrap();
        let map_child = |i: usize| s.column(i).as_any().downcast_ref::<MapArray>().unwrap();
        let rows = decode_typed_arrays(
            map_child(0),
            map_child(1),
            map_child(2),
            map_child(3),
            s.column(4).as_any().downcast_ref::<BinaryArray>().unwrap(),
        )
        .unwrap();
        match &rows[0] {
            Some(doc) => serde_json::Value::Object(doc.clone()),
            None => serde_json::Value::Null,
        }
    }

    /// The `rows` default projection returns `log_attributes` as a JSON
    /// object of the container's *original* values on the typed layout: the
    /// typed-home key untouched, the off-type residue key untouched (never
    /// coerced towards its declared type), the array, and the bytes carrier.
    #[tokio::test]
    async fn typed_logs_rows_default_returns_raw_attribute_values() {
        let svc = IrService::new(logs_ctx_typed_residue());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "pipeline": []
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let output_field = df
            .schema()
            .field_with_unqualified_name("log_attributes")
            .unwrap();
        assert_eq!(
            output_field
                .metadata()
                .get(typed_attributes::IR_TYPE_METADATA_KEY),
            Some(&typed_attributes::RAW_ATTRIBUTE_BAG_IR_TYPE.to_string()),
            "the output field must carry the IR type tag through the DataFrame plan"
        );
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
        assert_eq!(
            log_attributes_json(batch),
            serde_json::json!({
                "http.method": "GET",
                "retries": "three",
                "tags": ["a", "b"],
                "payload": {"$otlp_type": "bytes", "base64": "AP9h"},
            })
        );
    }

    /// The same raw-accessor result, from an explicit `log.attributes`
    /// projection rather than the `rows` default — both routes through
    /// [`Lowering::column_projection_expr`] must agree.
    #[tokio::test]
    async fn typed_logs_explicit_attributes_projection_matches_the_default() {
        let svc = IrService::new(logs_ctx_typed_residue());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows", "fields": ["log.attributes"],
            "pipeline": []
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let batches = df.collect().await.unwrap();
        let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
        assert_eq!(
            log_attributes_json(batch),
            serde_json::json!({
                "http.method": "GET",
                "retries": "three",
                "tags": ["a", "b"],
                "payload": {"$otlp_type": "bytes", "base64": "AP9h"},
            })
        );
    }

    /// Widening the row defaults must not open a back door to physical
    /// addressing: the server chooses the default projection, but a client
    /// still cannot name a storage column. Containers reach the client only
    /// through the defaults, under their OTel scope.
    #[tokio::test]
    async fn a_client_still_cannot_address_a_container_by_storage_name() {
        let svc = IrService::new(logs_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["log_attributes_str"],
            "pipeline": []
        }));
        let err = svc
            .plan(&d, "t", "d", 0)
            .await
            .expect_err("physical addressing must stay rejected");
        assert!(
            format!("{err}").contains("log_attributes_str"),
            "unexpected error: {err}"
        );
    }

    /// #1395's residual: an aggregate's `of` operand naming a genuine
    /// physical column `LogicalSchema::core()` does not register for this
    /// source (`duration` is registered only for `traces`, not `logs`) stays
    /// rejected, exactly like a `where`/`by`/`fields` reference to the same
    /// name (`a_client_still_cannot_address_a_container_by_storage_name`).
    /// Physical addressing is not a LogQL/TraceQL construct any compat
    /// surface can reach — `unwrap <label>` lowers a *logical* field (an
    /// attribute, resolved by name through the ordinary coalesce path; see
    /// `ql_ir::logql_lower`), never a literal storage column, so no working
    /// query is regressed by keeping this rejected. See design.md's
    /// fallback-set subsection for the resolution.
    #[tokio::test]
    async fn aggregate_of_field_naming_a_physical_column_is_rejected() {
        let svc = IrService::new(logs_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "table",
            "pipeline": [
                { "aggregate": { "by": ["service.name"], "aggs": [
                    { "fn": "sum", "of": "duration", "as": "total" }
                ] } }
            ]
        }));
        let err = svc
            .plan(&d, "t", "d", 0)
            .await
            .expect_err("an aggregate over an unregistered physical column must stay rejected");
        assert!(
            format!("{err}").contains("duration"),
            "unexpected error: {err}"
        );
    }

    /// The attribute containers are addressable under their OTel scope as
    /// retrieval-only logical fields (`log.attributes`, `scope.attributes`,
    /// `resource.attributes`; `span.attributes` / `profile.attributes` on the
    /// other sources), so a client can ask for a record's whole attribute
    /// bag by scope without naming a storage column.
    #[tokio::test]
    async fn attribute_containers_are_addressable_by_otel_scope() {
        let svc = IrService::new(logs_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["trace_id", "log.attributes", "scope.attributes", "resource.attributes"],
            "pipeline": [{ "where": { "field": "trace_id", "op": "eq", "value": "t1" } }]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let batches = df.collect().await.unwrap();
        let batch = batches.iter().find(|b| b.num_rows() > 0).expect("one row");
        assert_eq!(batch.num_rows(), 1);
        let schema = batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "trace_id",
                "log_attributes",
                "scope_attributes",
                "resource_attributes"
            ],
            "output columns keep the OTel-scope naming"
        );
        assert_eq!(
            log_attributes_json(batch),
            serde_json::json!({ "deployment.environment": "prod" })
        );
    }

    #[tokio::test]
    async fn attribute_containers_are_retrieval_only() {
        let svc = IrService::new(logs_ctx());
        for field in ["log.attributes", "scope.attributes", "resource.attributes"] {
            let d = doc(serde_json::json!({
                "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
                "result": "rows",
                "fields": ["trace_id"],
                "pipeline": [{ "where": { "field": field, "op": "exists" } }]
            }));
            let err = svc.plan(&d, "t", "d", 0).await.expect_err("rejected");
            assert!(format!("{err}").contains(field), "unexpected error: {err}");
        }
    }

    #[tokio::test]
    async fn span_attributes_container_is_addressable_on_traces() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("span_name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("start_time_unix_nano", DataType::Int64, false),
            Field::new("duration_nanos", DataType::Int64, false),
            map_field_named("span_attributes"),
            map_field_named("resource_attributes"),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["t0"])),
                Arc::new(StringArray::from(vec!["s0"])),
                Arc::new(StringArray::from(vec!["GET /a"])),
                Arc::new(StringArray::from(vec!["api"])),
                Arc::new(Int64Array::from(vec![10_i64])),
                Arc::new(Int64Array::from(vec![100_i64])),
                build_map(&[&[("http.request.method", "GET")]]),
                build_map(&[&[("service.version", "1.2.3")]]),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("traces".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);

        let svc = IrService::new(ctx);
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["span_id", "span.attributes", "resource.attributes"]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let batches = df.collect().await.unwrap();
        let batch = batches.iter().find(|b| b.num_rows() > 0).expect("one row");
        for (col_name, key, value) in [
            ("span_attributes", "http.request.method", "GET"),
            ("resource_attributes", "service.version", "1.2.3"),
        ] {
            let attrs = batch
                .column_by_name(col_name)
                .unwrap()
                .as_any()
                .downcast_ref::<datafusion::arrow::array::MapArray>()
                .unwrap();
            let entries = attrs.value(0);
            let keys = entries
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let values = entries
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(keys.value(0), key);
            assert_eq!(values.value(0), value);
        }
    }

    #[tokio::test]
    async fn a_where_clause_cannot_address_a_container_by_storage_name() {
        let svc = IrService::new(logs_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["trace_id"],
            "pipeline": [{ "where": { "field": "log_attributes_str", "op": "exists" } }]
        }));
        let err = svc
            .plan(&d, "t", "d", 0)
            .await
            .expect_err("physical addressing must stay rejected in a where clause");
        assert!(
            format!("{err}").contains("log_attributes_str"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn a_client_cannot_address_a_builtin_physical_alias() {
        let svc = IrService::new(logs_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["service_name"],
            "pipeline": []
        }));

        let err = svc
            .plan(&d, "t", "d", 0)
            .await
            .expect_err("physical aliases must stay private");
        assert!(
            format!("{err}").contains("service_name"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn retrieval_only_metadata_cannot_be_used_in_predicates() {
        // `body` moved off the retrieval-only list (`ir-single-lowering` D6);
        // `span_events` is the retrieval-only example now — see
        // `span_events_is_retrieval_only` for the dedicated coverage of that
        // field's own normalization behaviour, and
        // `body_is_filterable_for_string_operators`/
        // `body_ordered_operators_compare_lexically` below for `body`.
        let svc = IrService::new(traces_events_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "pipeline": [{ "where": { "field": "span_events", "op": "exists" } }]
        }));

        let err = svc
            .plan(&d, "t", "d", 0)
            .await
            .expect_err("retrieval-only metadata must not be filterable");
        assert!(
            format!("{err}").contains("span_events"),
            "unexpected error: {err}"
        );
    }

    /// Bodies exercising exactly the characters `serde_json::to_string`
    /// escapes on encode — an embedded quote, a backslash, a newline, and a
    /// non-ASCII codepoint — so an encode/decode mismatch in the `body`
    /// `eq`/`ne`/`in` literal-encoding path (issue #1433 review) would
    /// surface as a failed `eq` rather than passing by accident the way a
    /// single-character ASCII body could.
    const TRICKY_BODIES: [&str; 4] = ["say \"hi\"", "a\\b", "line1\nline2", "café"];

    /// The literal, ingest-order list of decoded bodies `logs_body_ctx()`
    /// stores — `"a"` first, [`TRICKY_BODIES`], then `"b"`/`"c"`/`"d"` — so
    /// both the fixture builder and the tests asserting against it derive
    /// their expectations from the one list instead of hand-maintaining a
    /// second copy that could silently drift from what the fixture actually
    /// holds.
    fn logs_body_ctx_bodies() -> Vec<&'static str> {
        ["a"]
            .into_iter()
            .chain(TRICKY_BODIES)
            .chain(["b", "c", "d"])
            .collect()
    }

    /// The subset of `logs_body_ctx_bodies()` matching `pred`, decoded and
    /// sorted — a test's expected row set, computed from the fixture
    /// definition rather than a separately hand-computed literal.
    fn expected_bodies(pred: impl Fn(&str) -> bool) -> Vec<String> {
        let mut out: Vec<String> = logs_body_ctx_bodies()
            .into_iter()
            .filter(|b| pred(b))
            .map(str::to_string)
            .collect();
        out.sort();
        out
    }

    /// Like `logs_ctx()`, but `body` is seeded through the real ingest
    /// encoding (`serde_json::to_string`, issue #1410) instead of bare
    /// single-character strings. Bare strings like `"a"` are not valid JSON
    /// string encodings of themselves (`serde_json::to_string("a")` is
    /// `"\"a\""`), so `logs_ctx()`'s fixture cannot distinguish a filter that
    /// compares against the decoded value from one that compares against the
    /// raw, still-encoded column — exactly the gap issue #1433 closes.
    fn logs_body_ctx() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("body", DataType::Utf8, true),
            Field::new("service_name", DataType::Utf8, true),
        ]));
        let encode = |s: &str| serde_json::to_string(s).unwrap();
        let bodies: Vec<Option<String>> = logs_body_ctx_bodies()
            .into_iter()
            .map(|s| Some(encode(s)))
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                // "a" at ts=10, the tricky bodies at ts=11..14 (between "a"
                // and "b"), then "b"/"c"/"d" at their original 20/30/40 —
                // `first`/`last`-by-time still pick "a"/"d".
                Arc::new(TimestampNanosecondArray::from(vec![
                    10_i64, 11, 12, 13, 14, 20, 30, 40,
                ])),
                Arc::new(StringArray::from(bodies)),
                Arc::new(StringArray::from(vec![
                    "api", "api", "api", "api", "api", "api", "web", "web",
                ])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("logs".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    /// Logs for `count_distinct`: two `service.name` groups, one
    /// `exception` row, and one row without `user.id`.
    fn count_distinct_ctx() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("event_name", DataType::Utf8, true),
            map_field_named("log_attributes"),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![10_i64, 20, 30, 40, 50])),
                Arc::new(StringArray::from(vec!["web", "web", "web", "api", "api"])),
                Arc::new(StringArray::from(vec![
                    "page_view",
                    "exception",
                    "page_view",
                    "page_view",
                    "page_view",
                ])),
                build_map(&[
                    &[("session.id", "s1"), ("user.id", "u1")],
                    &[("session.id", "s1"), ("user.id", "u1")],
                    &[("session.id", "s2")],
                    &[("session.id", "s3"), ("user.id", "u2")],
                    &[("session.id", "s3"), ("user.id", "u2")],
                ]),
            ],
        )
        .unwrap();
        // The querier's attribute reads only understand the typed layout
        // (the legacy map/JSON attribute paths were dropped) — rewrite the
        // legacy-shaped `log_attributes` map built above onto it, same as
        // `logs_ctx()`.
        let batch =
            common::testing::to_typed_layout("logs", "physical-v4", &batch, &["log_attributes"]);
        single_table_ctx("logs", batch.schema(), batch)
    }

    /// Plain `count_distinct` of a string attribute: within 2% of the true
    /// distinct count (exact here, since the cardinalities are tiny) —
    /// mirrors the spec's "Counting sessions" scenario.
    #[tokio::test]
    async fn count_distinct_counts_distinct_sessions_per_group() {
        let batches = collect_doc_over(
            count_distinct_ctx(),
            serde_json::json!({
                "irVersion": 9, "from": "logs", "range": { "from": 0, "to": 1000 },
                "result": "table",
                "pipeline": [ { "aggregate": { "by": ["service.name"], "aggs": [
                    { "fn": "count_distinct", "of": "session.id", "as": "sessions" }
                ] } } ]
            }),
        )
        .await;
        let sessions = counts_by_group(&batches, "service_name", "sessions")
            .into_iter()
            .collect::<HashMap<_, _>>();
        assert_eq!(sessions.get("web").copied(), Some(2), "s1, s2");
        assert_eq!(sessions.get("api").copied(), Some(1), "s3 only");
    }

    /// `trace.id`/`span.id` name the same columns on every source that
    /// carries trace context, so a document written for profiles or
    /// exemplars (or the MCP minimal example) counts traces rather than an
    /// absent attribute's zero (#2204).
    #[tokio::test]
    async fn count_distinct_of_trace_id_reads_the_trace_id_column() {
        let batches = collect_doc_over(
            traces_ctx(),
            serde_json::json!({
                "irVersion": 9, "from": "traces", "range": { "from": 0, "to": 1000 },
                "result": "table",
                "pipeline": [ { "aggregate": { "by": ["service.name"], "aggs": [
                    { "fn": "count_distinct", "of": "trace.id", "as": "traces" },
                    { "fn": "count_distinct", "of": "span.id", "as": "spans" }
                ] } } ]
            }),
        )
        .await;
        for measure in ["traces", "spans"] {
            let counts = counts_by_group(&batches, "service_name", measure)
                .into_iter()
                .collect::<HashMap<_, _>>();
            assert_eq!(counts.get("api").copied(), Some(2), "{measure}: t1, t2");
            assert_eq!(counts.get("web").copied(), Some(1), "{measure}: t3");
        }
    }

    /// A scoped `count_distinct` counts only the records the scope admits;
    /// a group with no matching record reports zero rather than being
    /// dropped — mirrors the spec's "Scoped distinct count" scenario.
    #[tokio::test]
    async fn a_scoped_count_distinct_counts_only_matching_records_and_zeros_the_rest() {
        let batches = collect_doc_over(
            count_distinct_ctx(),
            serde_json::json!({
                "irVersion": 9, "from": "logs", "range": { "from": 0, "to": 1000 },
                "result": "table",
                "pipeline": [ { "aggregate": { "by": ["service.name"], "aggs": [
                    { "fn": "count_distinct", "of": "session.id", "as": "sessions",
                      "where": { "field": "event_name", "op": "eq", "value": "exception" } }
                ] } } ]
            }),
        )
        .await;
        let sessions = counts_by_group(&batches, "service_name", "sessions")
            .into_iter()
            .collect::<HashMap<_, _>>();
        assert_eq!(
            sessions.get("web").copied(),
            Some(1),
            "only s1's exception row counts"
        );
        assert_eq!(
            sessions.get("api").copied(),
            Some(0),
            "no exception row in this group — zero, not dropped"
        );
    }

    /// A record with no `user.id` is not counted — mirrors the spec's
    /// "Nulls are not a value" scenario.
    #[tokio::test]
    async fn count_distinct_does_not_count_nulls() {
        let batches = collect_doc_over(
            count_distinct_ctx(),
            serde_json::json!({
                "irVersion": 9, "from": "logs", "range": { "from": 0, "to": 1000 },
                "result": "table",
                "pipeline": [ { "aggregate": { "by": ["service.name"], "aggs": [
                    { "fn": "count_distinct", "of": "user.id", "as": "users" }
                ] } } ]
            }),
        )
        .await;
        let users = counts_by_group(&batches, "service_name", "users")
            .into_iter()
            .collect::<HashMap<_, _>>();
        // `web` has u1 (rows 1-2) and a null (row 3) — the null must not
        // inflate the count to 2.
        assert_eq!(users.get("web").copied(), Some(1));
        assert_eq!(users.get("api").copied(), Some(1));
    }

    /// Like `collect_doc`, but over a caller-supplied context rather than
    /// the shared `logs_ctx()` fixture.
    async fn collect_doc_over(ctx: SessionContext, v: serde_json::Value) -> Vec<RecordBatch> {
        let svc = IrService::new(ctx);
        let d = doc(v);
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("logs table is registered");
        df.collect().await.unwrap()
    }

    /// A string column's values across every batch, sorted — the shared
    /// tail end of "run a query, check which rows/keys came back" used by
    /// both `plan_body_rows` (a `rows` projection) and
    /// `group_by_body_decodes_the_group_key` (an `aggregate` group key),
    /// so the two don't hand-maintain identical extraction loops.
    fn collect_sorted_string_column(batches: &[RecordBatch], name: &str) -> Vec<String> {
        let mut values: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                let col = b
                    .column_by_name(name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..b.num_rows()).map(|i| col.value(i).to_string())
            })
            .collect();
        values.sort();
        values
    }

    /// Collect a single-column `body` projection's decoded text values.
    async fn plan_body_rows(svc: &IrService, doc: &Document) -> Vec<String> {
        let (df, _) = svc
            .plan(doc, "t", "d", 0)
            .await
            .unwrap_or_else(|e| panic!("plan failed: {e}"))
            .expect("logs table is registered");
        collect_sorted_string_column(&df.collect().await.unwrap(), "body")
    }

    /// D6 (`ir-single-lowering`): the harness found every LogQL line filter
    /// (`|=`/`!=`/`|~`/`!~`) lowers to a predicate on `body`, which
    /// `LogicalSchema::core()` used to mark retrieval-only — rejecting every
    /// one of those documents outright. `body` is filterable for string
    /// operators now.
    ///
    /// #1433: `body` is JSON-encoded at rest (issue #1410), so each of these
    /// operators must resolve `where body = "<text>"` against the *decoded*
    /// text a `rows` result of the same field shows — an anchored `regex`
    /// especially, since the raw column's surrounding quotes would defeat a
    /// `^...$` anchor even though the substring survives an unanchored
    /// `contains`.
    #[tokio::test]
    async fn body_is_filterable_for_string_operators() {
        let svc = IrService::new(logs_body_ctx());
        let where_body = |op: &str, value: serde_json::Value| {
            doc(serde_json::json!({
                "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
                "result": "rows",
                "fields": ["body"],
                "pipeline": [{ "where": { "field": "body", "op": op, "value": value } }]
            }))
        };

        assert_eq!(
            plan_body_rows(&svc, &where_body("eq", serde_json::json!("a"))).await,
            vec!["a".to_string()],
        );
        // #1433 review: `eq`/`ne`/`in` on `body` compare against a
        // JSON-encoded literal for pushdown, so every character
        // `serde_json::to_string` escapes must round-trip through that
        // encoding exactly — a single-character ASCII body wouldn't catch a
        // mismatch here.
        for tricky in TRICKY_BODIES {
            assert_eq!(
                plan_body_rows(&svc, &where_body("eq", serde_json::json!(tricky))).await,
                vec![tricky.to_string()],
                "eq should match the decoded body exactly for {tricky:?}"
            );
        }
        assert_eq!(
            plan_body_rows(&svc, &where_body("ne", serde_json::json!("a"))).await,
            expected_bodies(|b| b != "a"),
        );
        assert_eq!(
            plan_body_rows(&svc, &where_body("contains", serde_json::json!("a"))).await,
            expected_bodies(|b| b.contains('a')),
        );
        // Anchored: would fail against the raw, quote-wrapped column even
        // though an unanchored `contains` happens to still find "a" inside
        // the quotes.
        assert_eq!(
            plan_body_rows(&svc, &where_body("regex", serde_json::json!("^a$"))).await,
            vec!["a".to_string()],
        );

        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["body"],
            "pipeline": [{ "where": { "field": "body", "op": "exists" } }]
        }));
        assert_eq!(plan_body_rows(&svc, &d).await, expected_bodies(|_| true));
    }

    /// #1433: `in` on `body` must decide membership against the decoded
    /// text, the same encoded-literal-candidates approach `eq` uses (see
    /// `body_eq_candidates`), for each element of the array.
    #[tokio::test]
    async fn body_in_matches_decoded_values() {
        let svc = IrService::new(logs_body_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["body"],
            "pipeline": [{ "where": { "field": "body", "op": "in", "value": ["a", "café"] } }]
        }));
        assert_eq!(
            plan_body_rows(&svc, &d).await,
            expected_bodies(|b| b == "a" || b == "café"),
        );
    }

    /// #1433: `between` on `body` must decode the column before comparing,
    /// like every other ordered operator (`body_ordered_operators_compare_lexically`).
    #[tokio::test]
    async fn body_between_compares_lexically_decoded() {
        let svc = IrService::new(logs_body_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["body"],
            "pipeline": [{ "where": { "field": "body", "op": "between", "value": ["a", "c"] } }]
        }));
        assert_eq!(
            plan_body_rows(&svc, &d).await,
            expected_bodies(|b| ("a"..="c").contains(&b)),
        );
    }

    /// #1433 review: `eq`/`ne`/`in` on `body` must also match a structured
    /// (or legacy-bare) body, not only a plain string. `decode_log_body` is
    /// the identity for anything that isn't a JSON string scalar, so a raw
    /// column that already equals the query literal verbatim — never
    /// JSON-string-encoded, since only a *string* `AnyValue` body is
    /// wrapped in quotes at ingest — decodes to that literal too, and `eq`
    /// must recognise that candidate as well as the encoded form a
    /// plain-string body takes (`body_eq_candidates`). `logs_json_ctx`'s
    /// bodies are exactly this shape: raw JSON object text, unquoted.
    #[tokio::test]
    async fn body_eq_matches_a_structured_body_verbatim() {
        let svc = IrService::new(logs_json_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["body"],
            "pipeline": [{ "where": {
                "field": "body", "op": "eq", "value": r#"{"level":"info","code":200}"#
            } }]
        }));
        assert_eq!(
            plan_body_rows(&svc, &d).await,
            vec![r#"{"level":"info","code":200}"#.to_string()],
        );
    }

    /// D6 (`ir-single-lowering`), review finding on #1393: `body` gets no
    /// special-cased operator allowlist — an ordered operator (`gt`) on
    /// `body` is not rejected, it compares *lexically*, the same as any
    /// other string field (`Lowering::ordered` only casts to a number when
    /// the field's resolved `ValueType` is numeric; `body` resolves to
    /// `ValueType::String`). This is not a numeric-comparison capability
    /// for `body` — it is the absence of a special case, in either
    /// direction.
    ///
    /// #1433: ordering must compare the *decoded* text, not the raw,
    /// JSON-encoded column — the surrounding `"` (0x22) sorts ahead of every
    /// letter, so comparing the raw bytes would make every row fail `gt`.
    #[tokio::test]
    async fn body_ordered_operators_compare_lexically() {
        let svc = IrService::new(logs_body_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["body"],
            "pipeline": [{ "where": { "field": "body", "op": "gt", "value": "a" } }]
        }));
        assert_eq!(plan_body_rows(&svc, &d).await, expected_bodies(|b| b > "a"));
    }

    /// Collect the LogicalPlan node types, root-first, following each node's
    /// input(s). Used to assert plan *shape* (not a brittle golden string).
    fn plan_node_types(plan: &datafusion::logical_expr::LogicalPlan, out: &mut Vec<&'static str>) {
        use datafusion::logical_expr::LogicalPlan as LP;
        out.push(match plan {
            LP::Projection(_) => "Projection",
            LP::Filter(_) => "Filter",
            LP::Aggregate(_) => "Aggregate",
            LP::Sort(_) => "Sort",
            LP::Limit(_) => "Limit",
            LP::TableScan(_) => "TableScan",
            LP::SubqueryAlias(_) => "SubqueryAlias",
            _ => "Other",
        });
        for input in plan.inputs() {
            plan_node_types(input, out);
        }
    }

    // Task 4.1 (deep) — assert the *shape* of the lowered plan, not just that
    // substrings appear: Sort at the root, one Aggregate (bucketed by date_bin)
    // above the Filters, promoted + unpromoted predicates in the same Filter,
    // and a TableScan leaf. Catches lowering regressions (dropped stage, lost
    // bucketing, reordering) that an execution result on a tiny fixture would
    // not. (Pushdown depends on the TableProvider — Iceberg pushes, MemTable
    // does not — so it is covered by execution/E2E, not plan shape.)
    #[tokio::test]
    async fn logs_aggregate_step_lowers_to_expected_plan_shape() {
        let svc = IrService::new(logs_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "series",
            "pipeline": [
                { "where": { "and": [
                    { "field": "severity_number", "op": "gte", "value": 17 },
                    { "field": "deployment.environment", "op": "eq", "value": "prod" }
                ]}},
                { "aggregate": { "by": ["service.name"], "aggs": [{ "fn": "count", "as": "n" }], "step": "1ms" } }
            ]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let plan = df.logical_plan();

        let mut types = Vec::new();
        plan_node_types(plan, &mut types);

        // Root is the deterministic Sort; leaf is the TableScan.
        assert_eq!(types.first(), Some(&"Sort"), "node types: {types:?}");
        assert_eq!(types.last(), Some(&"TableScan"), "node types: {types:?}");
        // Exactly one Aggregate, sitting above every Filter, above the scan.
        assert_eq!(
            types.iter().filter(|t| **t == "Aggregate").count(),
            1,
            "node types: {types:?}"
        );
        let agg = types.iter().position(|t| *t == "Aggregate").unwrap();
        let first_filter = types.iter().position(|t| *t == "Filter").unwrap();
        let scan = types.iter().position(|t| *t == "TableScan").unwrap();
        assert!(
            agg < first_filter && first_filter < scan,
            "node types: {types:?}"
        );

        // The Aggregate buckets by date_bin; the Filter carries BOTH the promoted
        // column predicate and the unpromoted get_field extraction, proving
        // promotion-aware lowering in one plan.
        let text = format!("{}", plan.display_indent_schema());
        assert!(text.contains("date_bin"), "plan:\n{text}");
        assert!(text.contains("severity_number"), "plan:\n{text}");
        assert!(text.contains("get_field"), "plan:\n{text}");
    }

    // Task 4.2 — promotion invariance: promoted column vs json-path, same result.
    #[tokio::test]
    async fn promotion_invariance_same_result() {
        let svc = IrService::new(logs_ctx());
        let promoted = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "table",
            "pipeline": [
                { "where": { "field": "env", "op": "eq", "value": "prod" } },
                { "aggregate": { "aggs": [{ "fn": "count", "as": "n" }] } }
            ]
        }));
        // `env` is promoted (label_env exists) → resolves to the column.
        let (df_p, _) = svc
            .plan(&promoted, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let plan_p = format!("{}", df_p.logical_plan().display_indent());
        assert!(
            plan_p.contains("label_env"),
            "expected column ref:\n{plan_p}"
        );
        let batches_p = df_p.collect().await.unwrap();

        // Same query on the unpromoted attribute → json-path extraction.
        let unpromoted = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "table",
            "pipeline": [
                { "where": { "field": "deployment.environment", "op": "eq", "value": "prod" } },
                { "aggregate": { "aggs": [{ "fn": "count", "as": "n" }] } }
            ]
        }));
        let (df_u, _) = svc
            .plan(&unpromoted, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let plan_u = format!("{}", df_u.logical_plan().display_indent());
        assert!(
            plan_u.contains("get_field"),
            "expected json-path:\n{plan_u}"
        );
        let batches_u = df_u.collect().await.unwrap();

        let count_p = batches_p[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let count_u = batches_u[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count_p, count_u, "promotion changed the result");
        assert_eq!(count_p, 3); // three rows have env=prod
    }

    // Task 3.0 (D10, `ir-single-lowering`) — a *scope-qualified* attribute
    // field must resolve to its promoted column exactly like the unscoped
    // case `promotion_invariance_same_result` already covers. Before the
    // fix, `SchemaResolver::column_for` computed `materialized_column_name`
    // from the scope-qualified field itself (`"span.http.method"` →
    // `label_span_http_method`), which no real promoted column is ever
    // named — the compactor promotes off the bare attribute key
    // (`attr_promotion::materialized_keys_of`), never the TraceQL-scoped
    // spelling — so the resolver always took the `get_field` extraction
    // path for a scope-qualified field, even when `label_http_method`
    // existed. Fixed by stripping the scope prefix first, the way
    // `Lowering::qualified_attr` already does for the unpromoted path.
    #[tokio::test]
    async fn scope_qualified_attribute_resolves_to_promoted_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("span_name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("start_time_unix_nano", DataType::Int64, false),
            Field::new("duration_nanos", DataType::Int64, false),
            Field::new("status_code", DataType::Utf8, true),
            map_field_named("span_attributes"),
            // The promoted column: keyed off the *bare* attribute key
            // (`http.method`), never the scope-qualified spelling.
            Field::new("label_http_method", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["t0", "t1"])),
                Arc::new(StringArray::from(vec!["s0", "s1"])),
                Arc::new(StringArray::from(vec![None::<&str>, None])),
                Arc::new(StringArray::from(vec!["GET /a", "POST /b"])),
                Arc::new(StringArray::from(vec!["api", "api"])),
                Arc::new(Int64Array::from(vec![10_i64, 20])),
                Arc::new(Int64Array::from(vec![100_i64, 100])),
                Arc::new(StringArray::from(vec![Some("OK"), Some("OK")])),
                build_map(&[&[("http.method", "GET")], &[("http.method", "POST")]]),
                Arc::new(StringArray::from(vec![Some("GET"), Some("POST")])),
            ],
        )
        .unwrap();
        let batch =
            common::testing::to_typed_layout("traces", "physical-v5", &batch, &["span_attributes"]);
        let ctx = SessionContext::new();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("traces".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);

        let svc = IrService::new(ctx);
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["trace_id"],
            "pipeline": [{ "where": { "field": "span.http.method", "op": "eq", "value": "GET" } }]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let plan = format!("{}", df.logical_plan().display_indent());
        assert!(
            plan.contains("label_http_method"),
            "expected the promoted column to be referenced:\n{plan}"
        );
        // #816: the promoted column alone isn't trustworthy until the
        // compactor backfills every file — the plan must keep the
        // `get_field` JSON fallback alive (coalesced with the column), not
        // drop it once a promoted column exists.
        assert!(
            plan.contains("get_field"),
            "expected the json-path fallback to stay coalesced with the promoted column (#816):\n{plan}"
        );
        let batches = df.collect().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "only the GET row should match");
    }

    // #1533 stopgap: two distinct label keys can sanitize to the same
    // `materialized_column_name` (`materialized_column_name` turns every
    // non-alphanumeric into `_`), so the writer resolves the collision by
    // suffixing the later key's column (`label_http_method_2`). Before this
    // fix, `SchemaResolver::column_for` matched `http.method` to
    // `label_http_method` regardless of a `label_http_method_2` sibling, so
    // a query for the *second* key's spelling would silently read the
    // first key's column. The resolver must instead treat `label_http_method`
    // as ambiguous whenever a `<base>_<n>` sibling exists and fall back to
    // the JSON/attribute-map extraction path, exactly as it does for an
    // unpromoted key.
    #[tokio::test]
    async fn colliding_materialized_columns_fall_back_to_extraction() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("span_name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("start_time_unix_nano", DataType::Int64, false),
            Field::new("duration_nanos", DataType::Int64, false),
            Field::new("status_code", DataType::Utf8, true),
            map_field_named("span_attributes"),
            // A decoy: if the resolver trusted the materialized fast path
            // it would read this column's ("wrong") values instead of the
            // attribute map.
            Field::new("label_http_method", DataType::Utf8, true),
            Field::new("label_http_method_2", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["t0", "t1"])),
                Arc::new(StringArray::from(vec!["s0", "s1"])),
                Arc::new(StringArray::from(vec![None::<&str>, None])),
                Arc::new(StringArray::from(vec!["GET /a", "POST /b"])),
                Arc::new(StringArray::from(vec!["api", "api"])),
                Arc::new(Int64Array::from(vec![10_i64, 20])),
                Arc::new(Int64Array::from(vec![100_i64, 100])),
                Arc::new(StringArray::from(vec![Some("OK"), Some("OK")])),
                build_map(&[&[("http.method", "GET")], &[("http.method", "POST")]]),
                Arc::new(StringArray::from(vec![Some("WRONG"), Some("WRONG")])),
                Arc::new(StringArray::from(vec![Some("GET"), Some("POST")])),
            ],
        )
        .unwrap();
        let batch =
            common::testing::to_typed_layout("traces", "physical-v5", &batch, &["span_attributes"]);
        let ctx = SessionContext::new();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("traces".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);

        let svc = IrService::new(ctx);
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["trace_id"],
            "pipeline": [{ "where": { "field": "http.method", "op": "eq", "value": "GET" } }]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let plan = format!("{}", df.logical_plan().display_indent());
        assert!(
            !plan.contains("label_http_method"),
            "an ambiguous materialized column must not be referenced:\n{plan}"
        );
        assert!(
            plan.contains("get_field"),
            "expected the json-path fallback, not the materialized fast path:\n{plan}"
        );
        let batches = df.collect().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            rows, 1,
            "only the row whose attribute map has GET should match"
        );
    }

    // Companion to `colliding_materialized_columns_fall_back_to_extraction`:
    // with no suffixed sibling, the fast path stays in effect.
    #[tokio::test]
    async fn uncontested_materialized_column_uses_fast_path() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("span_name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("start_time_unix_nano", DataType::Int64, false),
            Field::new("duration_nanos", DataType::Int64, false),
            Field::new("status_code", DataType::Utf8, true),
            map_field_named("span_attributes"),
            Field::new("label_http_method", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["t0", "t1"])),
                Arc::new(StringArray::from(vec!["s0", "s1"])),
                Arc::new(StringArray::from(vec![None::<&str>, None])),
                Arc::new(StringArray::from(vec!["GET /a", "POST /b"])),
                Arc::new(StringArray::from(vec!["api", "api"])),
                Arc::new(Int64Array::from(vec![10_i64, 20])),
                Arc::new(Int64Array::from(vec![100_i64, 100])),
                Arc::new(StringArray::from(vec![Some("OK"), Some("OK")])),
                build_map(&[&[("http.method", "GET")], &[("http.method", "POST")]]),
                Arc::new(StringArray::from(vec![Some("GET"), Some("POST")])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("traces".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);

        let svc = IrService::new(ctx);
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["trace_id"],
            "pipeline": [{ "where": { "field": "http.method", "op": "eq", "value": "GET" } }]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let plan = format!("{}", df.logical_plan().display_indent());
        assert!(
            plan.contains("label_http_method"),
            "expected the promoted column fast path with no collision:\n{plan}"
        );
    }

    // #1533: when each colliding column carries its origin key, each key
    // resolves to its own column and the fast path stays on for both.
    #[tokio::test]
    async fn documented_colliding_columns_each_resolve_to_their_own_key() {
        let origin = |key: &str| {
            HashMap::from([(
                common::schema::LABEL_ORIGIN_KEY_METADATA.to_string(),
                key.to_string(),
            )])
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("span_name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("start_time_unix_nano", DataType::Int64, false),
            Field::new("duration_nanos", DataType::Int64, false),
            Field::new("status_code", DataType::Utf8, true),
            map_field_named("span_attributes"),
            Field::new("label_http_method", DataType::Utf8, true)
                .with_metadata(origin("http.method")),
            Field::new("label_http_method_2", DataType::Utf8, true)
                .with_metadata(origin("http_method")),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["t0", "t1"])),
                Arc::new(StringArray::from(vec!["s0", "s1"])),
                Arc::new(StringArray::from(vec![None::<&str>, None])),
                Arc::new(StringArray::from(vec!["GET /a", "POST /b"])),
                Arc::new(StringArray::from(vec!["api", "api"])),
                Arc::new(Int64Array::from(vec![10_i64, 20])),
                Arc::new(Int64Array::from(vec![100_i64, 100])),
                Arc::new(StringArray::from(vec![Some("OK"), Some("OK")])),
                build_map(&[&[], &[]]),
                Arc::new(StringArray::from(vec![Some("GET"), Some("POST")])),
                Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("traces".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        let svc = IrService::new(ctx);

        for (field, value, column, trace) in [
            ("http.method", "POST", "label_http_method", "t1"),
            ("http_method", "alpha", "label_http_method_2", "t0"),
        ] {
            let d = doc(serde_json::json!({
                "irVersion": 1, "from": "traces", "range": { "from": 0, "to": 1000 },
                "result": "rows",
                "fields": ["trace_id"],
                "pipeline": [{ "where": { "field": field, "op": "eq", "value": value } }]
            }));
            let (df, _) = svc
                .plan(&d, "t", "d", 0)
                .await
                .unwrap()
                .expect("source table is registered");
            let plan = format!("{}", df.logical_plan().display_indent());
            assert!(plan.contains(column), "{field} must read {column}:\n{plan}");
            let batches = df.collect().await.unwrap();
            let ids = batches[0]
                .column_by_name("trace_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
            assert_eq!(ids.value(0), trace, "{field}={value}");
        }
    }

    /// A traces table with the real v2 column names, for the single-signal
    /// trace query path (task 4.3).
    fn traces_ctx() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("span_name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("start_time_unix_nano", DataType::Int64, false),
            Field::new("duration_nanos", DataType::Int64, false),
            Field::new("status_code", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["t0", "t1", "t2", "t3"])),
                Arc::new(StringArray::from(vec!["s0", "s1", "s2", "s3"])),
                Arc::new(StringArray::from(vec![None, Some("p1"), None, Some("p3")])),
                Arc::new(StringArray::from(vec![
                    "GET /before",
                    "GET /a",
                    "GET /b",
                    "POST /c",
                ])),
                Arc::new(StringArray::from(vec!["api", "api", "api", "web"])),
                Arc::new(Int64Array::from(vec![-1_i64, 10, 20, 30])),
                Arc::new(Int64Array::from(vec![100_i64, 100, 900, 500])),
                Arc::new(StringArray::from(vec![
                    Some("OK"),
                    Some("OK"),
                    Some("ERROR"),
                    Some("OK"),
                ])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("traces".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    /// Three traces with real parent/child span pairs, for the `correlate`
    /// stage (`irVersion` 8): trace `t0` has root `r0` (service `web`, no
    /// parent) calling child `c0` (service `api`, parent `r0`); trace `t1`
    /// has a lone root `r1` (service `web`, no parent, no children); trace
    /// `t2` has root `r2` (service `web`) calling child `c2` (service
    /// `api`), a second joinable pair for the row-cap test.
    fn correlate_ctx() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("span_name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("start_time_unix_nano", DataType::Int64, false),
            Field::new("duration_nanos", DataType::Int64, false),
            Field::new("status_code", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["t0", "t0", "t1", "t2", "t2"])),
                Arc::new(StringArray::from(vec!["r0", "c0", "r1", "r2", "c2"])),
                Arc::new(StringArray::from(vec![
                    None,
                    Some("r0"),
                    None,
                    None,
                    Some("r2"),
                ])),
                Arc::new(StringArray::from(vec![
                    "GET /a", "GET /b", "GET /c", "GET /d", "GET /e",
                ])),
                Arc::new(StringArray::from(vec!["web", "api", "web", "web", "api"])),
                Arc::new(Int64Array::from(vec![10_i64, 20, 10, 10, 20])),
                Arc::new(Int64Array::from(vec![100_i64, 50, 10, 100, 50])),
                Arc::new(StringArray::from(vec![
                    Some("OK"),
                    Some("OK"),
                    Some("OK"),
                    Some("OK"),
                    Some("OK"),
                ])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("traces".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    fn correlate_doc(kind: &str, extra: Vec<serde_json::Value>) -> Document {
        let mut pipeline =
            vec![serde_json::json!({ "correlate": { "to": "parent", "kind": kind } })];
        pipeline.extend(extra);
        doc(serde_json::json!({
            "irVersion": 8, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "pipeline": pipeline
        }))
    }

    #[tokio::test]
    async fn correlate_inner_pairs_child_and_parent_and_drops_the_root() {
        let svc = IrService::new(correlate_ctx());
        let d = correlate_doc("inner", vec![]);
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "c0 and c2 each have a parent in the window");
        for batch in &batches {
            let child_service = batch
                .column_by_name("service_name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let parent_service = batch
                .column_by_name("parent.service_name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                assert_eq!(child_service.value(i), "api");
                assert_eq!(parent_service.value(i), "web");
            }
        }
    }

    #[tokio::test]
    async fn correlate_left_keeps_roots_with_null_parent_fields() {
        let svc = IrService::new(correlate_ctx());
        let d = correlate_doc("left", vec![]);
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 5, "every child row survives a left join");
        let mut null_parent_rows = 0usize;
        for batch in &batches {
            let parent_service = batch
                .column_by_name("parent.service_name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                if parent_service.is_null(i) {
                    null_parent_rows += 1;
                }
            }
        }
        assert_eq!(null_parent_rows, 3, "the three roots have no parent");
    }

    #[tokio::test]
    async fn correlate_row_cap_bounds_the_joined_output() {
        let svc = IrService::new(correlate_ctx()).with_correlate_max_rows(1);
        let d = correlate_doc("inner", vec![]);
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total, 1,
            "two rows join, but correlate_max_rows=1 bounds the output"
        );
    }

    fn correlate_ir_params(
        kind: &str,
        extra: Vec<serde_json::Value>,
        result: &str,
    ) -> IrQueryParams {
        let mut pipeline =
            vec![serde_json::json!({ "correlate": { "to": "parent", "kind": kind } })];
        pipeline.extend(extra);
        IrQueryParams {
            document: serde_json::json!({
                "irVersion": 8, "from": "traces", "range": { "from": 0, "to": 1000 },
                "result": result,
                "pipeline": pipeline
            }),
            now_ns: 0,
            page: None,
        }
    }

    /// A ground-truth truncation signal (task 2's "critical" fix): the join
    /// itself is capped to `correlate_max_rows`, detected before an
    /// `aggregate` reduces the row count to something that can no longer
    /// prove anything about the join's own size.
    #[tokio::test]
    async fn correlate_truncation_survives_a_following_aggregate() {
        let svc = IrService::new(correlate_ctx()).with_correlate_max_rows(1);
        let params = correlate_ir_params(
            "inner",
            vec![serde_json::json!({ "aggregate": {
                "by": ["parent.service.name", "service.name"],
                "aggs": [{ "fn": "count", "as": "n" }]
            } })],
            "table",
        );
        let (_, _, report) = svc.query(&params, "t", "d").await.unwrap();
        assert!(
            report.row_limit,
            "two rows joined but the cap is 1; the aggregate must not hide the truncation"
        );
    }

    /// The same cap, but `correlate_max_rows` covers every row the join
    /// actually produces — no truncation happened, so no flag.
    #[tokio::test]
    async fn no_truncation_flag_when_the_cap_is_not_reached() {
        let svc = IrService::new(correlate_ctx()).with_correlate_max_rows(2);
        let params = correlate_ir_params("inner", vec![], "rows");
        let (batches, _, report) = svc.query(&params, "t", "d").await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "exactly the cap, but not beyond it");
        assert!(
            !report.row_limit,
            "the join produced exactly the cap without overflowing it"
        );
    }

    /// A `where`/`limit` after `correlate` can shrink the final row count
    /// well below the cap — truncation must still be reported, since it
    /// already happened at the join.
    #[tokio::test]
    async fn correlate_truncation_survives_a_following_where_and_limit() {
        let svc = IrService::new(correlate_ctx()).with_correlate_max_rows(1);
        let params = correlate_ir_params(
            "inner",
            vec![
                serde_json::json!({ "where": { "field": "service.name", "op": "eq", "value": "api" } }),
                serde_json::json!({ "limit": 1 }),
            ],
            "rows",
        );
        let (batches, _, report) = svc.query(&params, "t", "d").await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "limit narrows the final result to 1 row");
        assert!(
            report.row_limit,
            "the join itself was already truncated by the cap before where/limit ran"
        );
    }

    /// Task 5 — the parent-side scan finds no traces table at all (a new
    /// tenant/dataset with no data yet), exercised directly against
    /// `Lowering::lower_correlate` since `plan_document` itself would
    /// already have returned `None` before reaching any stage if the
    /// *child* side's table were equally absent — this is specifically the
    /// "child data exists, parent scan targets an empty context" case.
    #[tokio::test]
    async fn correlate_with_missing_parent_table_inner_is_empty_left_keeps_children_with_nulls() {
        let source = SourcePlan::for_source("traces").unwrap();
        let child_ctx = correlate_ctx();
        let child_table = child_ctx.table("t.d.traces").await.unwrap();
        let resolver = SchemaResolver::new(child_table.schema(), &source);
        let child_batches = child_table.collect().await.unwrap();
        let schema_cols: Vec<String> = child_batches[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        let window = ResolvedWindow {
            start_ns: 0,
            end_ns: 1000,
        };
        // The tenant/dataset catalog exists (as it would for a real,
        // freshly-provisioned tenant), but no `traces` table is registered
        // in it yet.
        let no_parent_ctx = SessionContext::new();
        let empty_schema = Arc::new(MemorySchemaProvider::new());
        let empty_catalog = Arc::new(MemoryCatalogProvider::new());
        empty_catalog.register_schema("d", empty_schema).unwrap();
        no_parent_ctx.register_catalog("t", empty_catalog);

        let run = |kind: JoinKind| {
            let resolver = &resolver;
            let source = &source;
            let child_batches = child_batches.clone();
            let no_parent_ctx = no_parent_ctx.clone();
            let schema_cols = schema_cols.clone();
            async move {
                let mut lowering = Lowering {
                    source,
                    resolver,
                    now_ns: 0,
                    demand: None,
                    aggregated: false,
                    series_shaped: false,
                    col_of: HashMap::new(),
                    derived_types: HashMap::new(),
                    schema_cols,
                    scope: None,
                    correlate_truncated: None,
                    correlate_fanout: None,
                    correlate_window: None,
                };
                let child_df = no_parent_ctx.read_batches(child_batches).unwrap();
                let correlate = Correlate {
                    to: CorrelateTarget::Parent,
                    on: None,
                    kind,
                    pipeline: Vec::new(),
                    window: None,
                    fanout: None,
                };
                let scan = CorrelateScan {
                    tenant_slug: "t",
                    dataset_slug: "d",
                    window: &window,
                    correlate_max_rows: 100,
                    correlate_max_source_rows: 100,
                };
                let out = lowering
                    .lower_correlate(&no_parent_ctx, child_df, &correlate, scan)
                    .await
                    .unwrap();
                out.collect().await.unwrap()
            }
        };

        let inner = run(JoinKind::Inner).await;
        let inner_rows: usize = inner.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            inner_rows, 0,
            "inner join against a missing parent table returns nothing"
        );

        let left = run(JoinKind::Left).await;
        let left_rows: usize = left.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            left_rows, 5,
            "left join keeps every child row when the parent table is missing"
        );
    }

    /// One trace: root `r0` (`span.http.route = "/checkout"`) calls child
    /// `c0`. For the `parent.span.<key>` attribute-scope test.
    fn correlate_attrs_batch() -> (Arc<Schema>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("span_name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("start_time_unix_nano", DataType::Int64, false),
            Field::new("duration_nanos", DataType::Int64, false),
            map_field_named("span_attributes"),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["t0", "t0"])),
                Arc::new(StringArray::from(vec!["r0", "c0"])),
                Arc::new(StringArray::from(vec![None, Some("r0")])),
                Arc::new(StringArray::from(vec!["GET /a", "GET /b"])),
                Arc::new(StringArray::from(vec!["web", "api"])),
                Arc::new(Int64Array::from(vec![10_i64, 20])),
                Arc::new(Int64Array::from(vec![100_i64, 50])),
                build_map(&[&[("http.route", "/checkout")], &[]]),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    fn correlate_attrs_ctx() -> SessionContext {
        let (_, batch) = correlate_attrs_batch();
        let batch =
            common::testing::to_typed_layout("traces", "physical-v5", &batch, &["span_attributes"]);
        single_table_ctx("traces", batch.schema(), batch.clone())
    }

    #[tokio::test]
    async fn group_by_parent_attribute_scope() {
        let svc = IrService::new(correlate_attrs_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 8, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "table",
            "pipeline": [
                { "correlate": { "to": "parent", "kind": "inner" } },
                { "aggregate": {
                    "by": ["parent.span.http.route"],
                    "aggs": [{ "fn": "count", "as": "n" }]
                } }
            ]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "one distinct parent route");
        let batch = &batches[0];
        let route = batch
            .column_by_name("parent_span_http_route")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(route, "/checkout");
        let n = batch
            .column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 1);
    }

    #[tokio::test]
    async fn group_by_caller_and_callee_gives_per_pair_counts() {
        let svc = IrService::new(correlate_ctx());
        let d = correlate_doc(
            "inner",
            vec![serde_json::json!({ "aggregate": {
                "by": ["parent.service.name", "service.name"],
                "aggs": [{ "fn": "count", "as": "n" }]
            } })],
        );
        let mut d = d;
        d.result = ResultEnvelope::Table;
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "one caller/callee pair: web -> api");
        let batch = &batches[0];
        let n = batch
            .column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 2);
    }

    #[tokio::test]
    async fn correlate_parent_outside_window_is_missing() {
        let svc = IrService::new(correlate_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 8, "from": "traces", "range": { "from": 15, "to": 1000 },
            "result": "rows",
            "pipeline": [{ "correlate": { "to": "parent", "kind": "inner" } }]
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total, 0,
            "r0 (start_time 10) is outside the window, so c0 has no joinable parent"
        );
    }

    // otel-native-schema layer 9 — cross-signal `correlate` (semi/anti).

    fn hex_id(n: u8) -> String {
        format!("{n:032x}")
    }

    /// Several partitions and tiny batches, so a plan that loses its row
    /// order shows it.
    fn catalog_ctx(tables: Vec<(&str, RecordBatch)>) -> SessionContext {
        let config = datafusion::prelude::SessionConfig::new()
            .with_target_partitions(4)
            .with_batch_size(2);
        register_catalog(SessionContext::new_with_config(config), tables)
    }

    fn register_catalog(ctx: SessionContext, tables: Vec<(&str, RecordBatch)>) -> SessionContext {
        let sp = Arc::new(MemorySchemaProvider::new());
        for (name, batch) in tables {
            let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
            sp.register_table(name.to_string(), Arc::new(table))
                .unwrap();
        }
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    /// Traces with ids 1..=4 starting at 100/200/300/400; trace 4 lasts
    /// 100ns, so the source envelope is `[100, 500]`.
    fn signal_traces(uppercase: bool) -> RecordBatch {
        let ids: Vec<String> = (1..=4)
            .map(|n| {
                let id = hex_id(n);
                if uppercase { id.to_uppercase() } else { id }
            })
            .collect();
        RecordBatch::try_from_iter(vec![
            ("trace_id", Arc::new(StringArray::from(ids)) as ArrayRef),
            (
                "span_id",
                Arc::new(StringArray::from(vec!["s1", "s2", "s3", "s4"])),
            ),
            ("span_name", Arc::new(StringArray::from(vec!["op"; 4]))),
            ("service_name", Arc::new(StringArray::from(vec!["web"; 4]))),
            (
                "start_time_unix_nano",
                Arc::new(Int64Array::from(vec![100_i64, 200, 300, 400])),
            ),
            (
                "duration_nanos",
                Arc::new(Int64Array::from(vec![50_i64, 10, 10, 100])),
            ),
        ])
        .unwrap()
    }

    /// Logs: an error for trace 1 (t=120), an info line for trace 2, an
    /// error for trace 4 at t=700 (past the envelope), and an error for a
    /// trace that is not in the source. `binary` stores `trace_id` as raw
    /// 16 bytes instead of lowercase hex.
    fn signal_logs(binary: bool) -> RecordBatch {
        let ids = [1u8, 2, 4, 9];
        let trace_id: ArrayRef = if binary {
            let raw: Vec<Vec<u8>> = ids
                .iter()
                .map(|n| {
                    let mut b = vec![0u8; 16];
                    b[15] = *n;
                    b
                })
                .collect();
            Arc::new(datafusion::arrow::array::BinaryArray::from_iter_values(raw))
        } else {
            Arc::new(StringArray::from(ids.map(hex_id).to_vec()))
        };
        RecordBatch::try_from_iter(vec![
            (
                "timestamp",
                Arc::new(TimestampNanosecondArray::from(vec![120_i64, 210, 700, 150])) as ArrayRef,
            ),
            ("trace_id", trace_id),
            (
                "severity_number",
                Arc::new(Int64Array::from(vec![17_i64, 9, 17, 17])),
            ),
            ("service_name", Arc::new(StringArray::from(vec!["web"; 4]))),
        ])
        .unwrap()
    }

    fn signal_ctx(uppercase_traces: bool, binary_logs: bool) -> SessionContext {
        catalog_ctx(vec![
            ("traces", signal_traces(uppercase_traces)),
            ("logs", signal_logs(binary_logs)),
        ])
    }

    fn error_logs_correlate(kind: &str) -> serde_json::Value {
        serde_json::json!({
            "to": "logs", "on": "trace_id", "kind": kind,
            "pipeline": [{ "where": { "field": "severity_number", "op": "gte", "value": 17 } }]
        })
    }

    fn signal_params(from: &str, correlate: serde_json::Value) -> IrQueryParams {
        IrQueryParams {
            document: serde_json::json!({
                "irVersion": 11, "from": from, "range": { "from": 0, "to": 1000 },
                "result": "rows",
                "pipeline": [{ "correlate": correlate }]
            }),
            now_ns: 0,
            page: None,
        }
    }

    /// Runs `params` and returns the (lowercased, sorted) `trace_id`s it kept.
    async fn correlated_trace_ids(
        ctx: SessionContext,
        params: &IrQueryParams,
    ) -> (Vec<String>, CorrelateReport) {
        let (batches, _, report) = with_empty_lookup(IrService::new(ctx))
            .query(params, "t", "d")
            .await
            .unwrap();
        let mut ids = Vec::new();
        for batch in &batches {
            let col = batch.column_by_name("trace_id").unwrap();
            let col = datafusion::arrow::compute::cast(col, &DataType::Utf8).unwrap();
            let col = col.as_any().downcast_ref::<StringArray>().unwrap();
            ids.extend(col.iter().flatten().map(str::to_lowercase));
        }
        ids.sort();
        (ids, report)
    }

    #[tokio::test]
    async fn semi_keeps_exactly_the_traces_with_an_error_log() {
        let params = signal_params("traces", error_logs_correlate("semi"));
        let (ids, report) = correlated_trace_ids(signal_ctx(false, false), &params).await;
        assert_eq!(ids, vec![hex_id(1)]);
        assert_eq!(
            report.window,
            Some(CorrelateWindowReport {
                start_ns: 100,
                end_ns: 500
            })
        );
    }

    #[tokio::test]
    async fn anti_keeps_exactly_the_traces_without_an_error_log() {
        let params = signal_params("traces", error_logs_correlate("anti"));
        let (ids, _) = correlated_trace_ids(signal_ctx(false, false), &params).await;
        assert_eq!(ids, vec![hex_id(2), hex_id(3), hex_id(4)]);
    }

    /// Trace 4's only error log (t=700) lies past the envelope `[100, 500]`:
    /// it counts as "no match" until `window.after` widens the scan.
    #[tokio::test]
    async fn anti_join_truth_is_scoped_to_the_widenable_window() {
        let mut correlate = error_logs_correlate("anti");
        correlate["window"] = serde_json::json!({ "after": "1000" });
        let params = signal_params("traces", correlate);
        let (ids, report) = correlated_trace_ids(signal_ctx(false, false), &params).await;
        assert_eq!(ids, vec![hex_id(2), hex_id(3)]);
        assert_eq!(
            report.window,
            Some(CorrelateWindowReport {
                start_ns: 100,
                end_ns: 1500
            })
        );
    }

    #[tokio::test]
    async fn differing_key_encodings_correlate_through_the_canonical_key() {
        for (uppercase, binary) in [(false, true), (true, false), (true, true)] {
            let params = signal_params("traces", error_logs_correlate("semi"));
            let (ids, _) = correlated_trace_ids(signal_ctx(uppercase, binary), &params).await;
            assert_eq!(
                ids,
                vec![hex_id(1)],
                "uppercase={uppercase} binary={binary}"
            );
        }
    }

    /// Pushdown only where the canonical key equals the stored encoding: a
    /// Utf8 `trace_id` gets a literal IN-list on the raw column, a Binary
    /// one is compared through its canonical (hex) form instead.
    #[tokio::test]
    async fn wide_side_pushdown_only_when_stored_encoding_is_canonical() {
        let d = doc(signal_params("traces", error_logs_correlate("semi")).document);
        for (binary, pushdown) in [(false, true), (true, false)] {
            let (df, _) = IrService::new(signal_ctx(false, binary))
                .plan(&d, "t", "d", 0)
                .await
                .unwrap()
                .unwrap();
            let plan = df.logical_plan().display_indent().to_string();
            assert_eq!(plan.contains("trace_id IN ([Utf8"), pushdown, "{plan}");
            assert_eq!(plan.contains("encode("), !pushdown, "{plan}");
        }
    }

    #[tokio::test]
    async fn exemplars_correlate_to_traces_on_trace_id() {
        let exemplars = RecordBatch::try_from_iter(vec![
            (
                "timestamp",
                Arc::new(TimestampNanosecondArray::from(vec![100_i64, 120])) as ArrayRef,
            ),
            (
                "trace_id",
                Arc::new(StringArray::from(vec![hex_id(1), hex_id(7)])),
            ),
            (
                "metric_name",
                Arc::new(StringArray::from(vec!["latency"; 2])),
            ),
            ("value", Arc::new(Float64Array::from(vec![1.0, 2.0]))),
        ])
        .unwrap();
        let ctx = catalog_ctx(vec![
            ("metric_exemplars", exemplars),
            ("traces", signal_traces(false)),
        ]);
        let params = signal_params(
            "exemplars",
            serde_json::json!({ "to": "traces", "on": "trace_id", "kind": "semi" }),
        );
        let (ids, _) = correlated_trace_ids(ctx, &params).await;
        assert_eq!(ids, vec![hex_id(1)]);
    }

    #[tokio::test]
    async fn metrics_correlate_to_logs_on_resource_identity() {
        let metrics = RecordBatch::try_from_iter(vec![
            (
                "timestamp",
                Arc::new(TimestampNanosecondArray::from(vec![100_i64, 100])) as ArrayRef,
            ),
            (
                "metric_name",
                Arc::new(StringArray::from(vec!["up", "down"])),
            ),
            ("value", Arc::new(Float64Array::from(vec![1.0, 0.0]))),
            (
                "resource_identity",
                Arc::new(StringArray::from(vec!["r1", "r2"])),
            ),
        ])
        .unwrap();
        let logs = RecordBatch::try_from_iter(vec![
            (
                "timestamp",
                Arc::new(TimestampNanosecondArray::from(vec![100_i64])) as ArrayRef,
            ),
            ("resource_identity", Arc::new(StringArray::from(vec!["r1"]))),
        ])
        .unwrap();
        let params = signal_params(
            "metrics",
            serde_json::json!({ "to": "logs", "on": "resource_identity", "kind": "semi" }),
        );
        let (batches, _, _) =
            IrService::new(catalog_ctx(vec![("metrics", metrics), ("logs", logs)]))
                .query(&params, "t", "d")
                .await
                .unwrap();
        let names: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                let c = b.column_by_name("metric_name").unwrap();
                let c = c.as_any().downcast_ref::<StringArray>().unwrap();
                c.iter().flatten().map(str::to_string).collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(names, vec!["up"]);
    }

    #[tokio::test]
    async fn a_source_over_the_bound_is_a_resource_error_for_semi_and_anti() {
        for kind in ["semi", "anti"] {
            let err = IrService::new(signal_ctx(false, false))
                .with_correlate_max_source_rows(3)
                .query(
                    &signal_params("traces", error_logs_correlate(kind)),
                    "t",
                    "d",
                )
                .await
                .unwrap_err();
            assert!(
                matches!(err, QuerierError::ResourceExhausted(ref m)
                    if m.contains("[querier].correlate_max_source_rows")),
                "{kind}: {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_missing_target_table_is_no_match() {
        let ctx = || catalog_ctx(vec![("traces", signal_traces(false))]);
        let semi = signal_params("traces", error_logs_correlate("semi"));
        assert!(correlated_trace_ids(ctx(), &semi).await.0.is_empty());
        let anti = signal_params("traces", error_logs_correlate("anti"));
        assert_eq!(correlated_trace_ids(ctx(), &anti).await.0.len(), 4);
    }

    /// Spans as `(trace_id, span_id, start, duration)`.
    fn spans(rows: &[(Option<&str>, &str, i64, i64)]) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            (
                "trace_id",
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            (
                "span_id",
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
            ),
            (
                "span_name",
                Arc::new(StringArray::from(vec!["op"; rows.len()])),
            ),
            (
                "start_time_unix_nano",
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ),
            (
                "duration_nanos",
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.3).collect::<Vec<_>>(),
                )),
            ),
        ])
        .unwrap()
    }

    /// Log records as `(timestamp, trace_id, span_id)`.
    fn span_logs(rows: &[(i64, &str, &str)]) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            (
                "timestamp",
                Arc::new(TimestampNanosecondArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            (
                "trace_id",
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
            ),
            (
                "span_id",
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ),
        ])
        .unwrap()
    }

    /// The `trace_id` column of every returned row, in result order.
    async fn trace_id_rows(ctx: SessionContext, params: &IrQueryParams) -> Vec<Option<String>> {
        let (batches, _, _) = with_empty_lookup(IrService::new(ctx))
            .query(params, "t", "d")
            .await
            .unwrap();
        batches
            .iter()
            .flat_map(|batch| {
                let col = batch.column_by_name("trace_id").unwrap();
                let col = col.as_any().downcast_ref::<StringArray>().unwrap();
                col.iter()
                    .map(|v| v.map(str::to_string))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// A log emitted mid-span (t=150, span `[100, 200]`) belongs to that
    /// span even though the span started before the log's envelope; a span
    /// that ended before the log (`[10, 30]`) does not match.
    #[tokio::test]
    async fn a_traces_target_matches_spans_overlapping_the_envelope() {
        let (t1, t2) = (hex_id(1), hex_id(2));
        let ctx = || {
            catalog_ctx(vec![
                (
                    "traces",
                    spans(&[(Some(&t1), "s1", 100, 100), (Some(&t2), "s2", 10, 20)]),
                ),
                ("logs", span_logs(&[(150, &t1, "s1"), (150, &t2, "s2")])),
            ])
        };
        let correlate = |kind: &str| {
            signal_params(
                "logs",
                serde_json::json!({ "to": "traces", "on": "trace_id", "kind": kind }),
            )
        };
        let (semi, _) = correlated_trace_ids(ctx(), &correlate("semi")).await;
        assert_eq!(semi, vec![t1.clone()]);
        let (anti, _) = correlated_trace_ids(ctx(), &correlate("anti")).await;
        assert_eq!(anti, vec![t2]);
    }

    /// A span that started more than [`TRACE_TARGET_LOOKBACK_NS`] before the
    /// envelope is out of the default scan; `window.before` brings it back.
    #[tokio::test]
    async fn a_traces_target_looks_back_a_bounded_span_length() {
        let hour = TRACE_TARGET_LOOKBACK_NS;
        let t1 = hex_id(1);
        let ctx = || {
            catalog_ctx(vec![
                ("traces", spans(&[(Some(&t1), "s1", 0, 3 * hour)])),
                ("logs", span_logs(&[(2 * hour, &t1, "s1")])),
            ])
        };
        let params = |correlate: serde_json::Value| IrQueryParams {
            document: serde_json::json!({
                "irVersion": 11, "from": "logs", "range": { "from": 0, "to": 3 * hour },
                "result": "rows", "pipeline": [{ "correlate": correlate }]
            }),
            now_ns: 0,
            page: None,
        };
        let default =
            params(serde_json::json!({ "to": "traces", "on": "trace_id", "kind": "semi" }));
        assert!(correlated_trace_ids(ctx(), &default).await.0.is_empty());
        let widened = params(serde_json::json!({
            "to": "traces", "on": "trace_id", "kind": "semi", "window": { "before": "1h" }
        }));
        assert_eq!(correlated_trace_ids(ctx(), &widened).await.0, vec![t1]);
    }

    /// `aggregate → topk → correlate` keeps the topk order through the join.
    #[tokio::test]
    async fn a_signal_correlate_keeps_the_source_row_order() {
        let ids: Vec<String> = (1..=12).map(hex_id).collect();
        // Durations interleave so topk order differs from id and hash order.
        let durations = [5_i64, 90, 20, 70, 10, 110, 40, 60, 30, 100, 50, 80];
        let span_rows: Vec<_> = ids
            .iter()
            .zip(durations)
            .map(|(id, d)| (Some(id.as_str()), "s", 100, d))
            .collect();
        let logs: Vec<_> = ids.iter().map(|id| (150, id.as_str(), "s")).collect();
        // A partitioned hash join, as over large tables, hash-repartitions
        // both sides and scrambles the source order.
        let config = datafusion::prelude::SessionConfig::new()
            .with_target_partitions(4)
            .set_usize(
                "datafusion.optimizer.hash_join_single_partition_threshold",
                0,
            )
            .set_usize(
                "datafusion.optimizer.hash_join_single_partition_threshold_rows",
                0,
            );
        let ctx = register_catalog(
            SessionContext::new_with_config(config),
            vec![("traces", spans(&span_rows)), ("logs", span_logs(&logs))],
        );
        let params = IrQueryParams {
            document: serde_json::json!({
                "irVersion": 11, "from": "traces", "range": { "from": 0, "to": 1000 },
                "result": "table",
                "pipeline": [
                    { "aggregate": { "by": ["trace_id"],
                        "aggs": [{ "fn": "max", "of": "duration", "as": "d" }] } },
                    { "topk": { "n": 6, "of": "d" } },
                    { "correlate": { "to": "logs", "on": "trace_id", "kind": "semi" } }
                ]
            }),
            now_ns: 0,
            page: None,
        };
        let expected: Vec<Option<String>> = [6, 10, 2, 12, 4, 8]
            .into_iter()
            .map(|n| Some(hex_id(n)))
            .collect();
        for _ in 0..5 {
            assert_eq!(trace_id_rows(ctx.clone(), &params).await, expected);
        }
    }

    /// A null or empty source key never matches: semi drops the row, anti
    /// keeps it.
    #[tokio::test]
    async fn null_and_empty_source_keys_never_match() {
        let t1 = hex_id(1);
        let ctx = || {
            catalog_ctx(vec![
                (
                    "traces",
                    spans(&[
                        (Some(&t1), "s1", 100, 10),
                        (None, "s2", 100, 10),
                        (Some(""), "s3", 100, 10),
                    ]),
                ),
                ("logs", span_logs(&[(105, &t1, "s1"), (105, "", "s3")])),
            ])
        };
        let correlate = |kind: &str| {
            signal_params(
                "traces",
                serde_json::json!({ "to": "logs", "on": "trace_id", "kind": kind }),
            )
        };
        assert_eq!(
            trace_id_rows(ctx(), &correlate("semi")).await,
            vec![Some(t1.clone())]
        );
        let mut anti = trace_id_rows(ctx(), &correlate("anti")).await;
        anti.sort();
        assert_eq!(anti, vec![None, Some(String::new())]);
    }

    /// `on: span_id` matches the (trace_id, span_id) pair, not either alone.
    #[tokio::test]
    async fn the_span_id_key_matches_the_trace_and_span_pair() {
        let (t1, t2) = (hex_id(1), hex_id(2));
        let ctx = catalog_ctx(vec![
            (
                "traces",
                spans(&[(Some(&t1), "s1", 100, 10), (Some(&t2), "s2", 100, 10)]),
            ),
            // t2's log names t1's span id: same span id, different trace.
            ("logs", span_logs(&[(105, &t1, "s1"), (105, &t2, "s1")])),
        ]);
        let params = signal_params(
            "traces",
            serde_json::json!({ "to": "logs", "on": "span_id", "kind": "semi" }),
        );
        assert_eq!(correlated_trace_ids(ctx, &params).await.0, vec![t1]);
    }

    #[tokio::test]
    async fn window_before_widens_the_target_scan_backwards() {
        let t1 = hex_id(1);
        let ctx = || {
            catalog_ctx(vec![
                ("traces", spans(&[(Some(&t1), "s1", 100, 10)])),
                ("logs", span_logs(&[(40, &t1, "s1")])),
            ])
        };
        let mut correlate = serde_json::json!({ "to": "logs", "on": "trace_id", "kind": "semi" });
        let (ids, _) =
            correlated_trace_ids(ctx(), &signal_params("traces", correlate.clone())).await;
        assert!(ids.is_empty());
        correlate["window"] = serde_json::json!({ "before": "60" });
        let (ids, report) = correlated_trace_ids(ctx(), &signal_params("traces", correlate)).await;
        assert_eq!(ids, vec![t1]);
        assert_eq!(
            report.window,
            Some(CorrelateWindowReport {
                start_ns: 40,
                end_ns: 110
            })
        );
    }

    /// An aggregate drops the source time column, so the target scan falls
    /// back to the document window: trace 4's error log at t=700, past the
    /// row envelope, now matches.
    #[tokio::test]
    async fn an_aggregated_source_scans_the_target_over_the_document_window() {
        let params = IrQueryParams {
            document: serde_json::json!({
                "irVersion": 11, "from": "traces", "range": { "from": 0, "to": 1000 },
                "result": "table",
                "pipeline": [
                    { "aggregate": { "by": ["trace_id"], "aggs": [{ "fn": "count", "as": "n" }] } },
                    { "correlate": error_logs_correlate("semi") }
                ]
            }),
            now_ns: 0,
            page: None,
        };
        let (ids, report) = correlated_trace_ids(signal_ctx(false, false), &params).await;
        assert_eq!(ids, vec![hex_id(1), hex_id(4)]);
        assert_eq!(
            report.window,
            Some(CorrelateWindowReport {
                start_ns: 0,
                end_ns: 1000
            })
        );
    }

    /// Only a traces source's rows end at `start + duration`; a profile's
    /// duration does not stretch the envelope.
    #[tokio::test]
    async fn only_a_traces_source_extends_the_envelope_by_duration() {
        let profiles = RecordBatch::try_from_iter(vec![
            (
                "timestamp",
                Arc::new(TimestampNanosecondArray::from(vec![100_i64, 200])) as ArrayRef,
            ),
            (
                "trace_id",
                Arc::new(StringArray::from(vec![hex_id(1), hex_id(2)])),
            ),
            (
                "duration_nano",
                Arc::new(Int64Array::from(vec![10_000_i64, 10_000])),
            ),
        ])
        .unwrap();
        let ctx = catalog_ctx(vec![
            ("profiles", profiles),
            ("traces", signal_traces(false)),
        ]);
        let params = signal_params(
            "profiles",
            serde_json::json!({ "to": "traces", "on": "trace_id", "kind": "semi" }),
        );
        let (_, report) = correlated_trace_ids(ctx, &params).await;
        assert_eq!(
            report.window,
            Some(CorrelateWindowReport {
                start_ns: 100,
                end_ns: 200
            })
        );
    }

    #[test]
    fn the_source_envelope_clamps_negative_durations_and_saturates() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                CORRELATE_START,
                Arc::new(Int64Array::from(vec![
                    Some(100_i64),
                    Some(i64::MAX - 5),
                    Some(50),
                ])) as ArrayRef,
            ),
            (
                CORRELATE_DURATION,
                Arc::new(Int64Array::from(vec![Some(-1_000_i64), Some(100), None])),
            ),
        ])
        .unwrap();
        assert_eq!(
            source_envelope(std::slice::from_ref(&batch)),
            Some((50, i64::MAX))
        );
        assert_eq!(source_envelope(&[batch.slice(0, 1)]), Some((100, 100)));
        let starts_only = batch.project(&[0]).unwrap();
        assert_eq!(source_envelope(&[starts_only]), Some((50, i64::MAX - 5)));
    }

    #[test]
    fn only_writer_canonical_key_columns_get_the_raw_in_list() {
        for source in ["traces", "logs", "exemplars", "profiles"] {
            assert!(writer_stores_canonical_key(source, "trace_id"));
            assert!(writer_stores_canonical_key(source, "span_id"));
        }
        assert!(writer_stores_canonical_key("metrics", "resource_identity"));
        assert!(writer_stores_canonical_key("exemplars", "series_id"));
        assert!(!writer_stores_canonical_key("metrics", "trace_id"));
        assert!(!writer_stores_canonical_key("logs", "service_name"));
    }

    /// A key that would fall back to an attribute read (an older logs table
    /// without a `trace_id` column) is a caller error, not a silent no-match.
    #[tokio::test]
    async fn an_unstored_key_column_is_rejected_naming_key_and_side() {
        let logs = RecordBatch::try_from_iter(vec![
            (
                "timestamp",
                Arc::new(TimestampNanosecondArray::from(vec![120_i64])) as ArrayRef,
            ),
            ("severity_number", Arc::new(Int64Array::from(vec![17_i64]))),
            (
                "log_attributes",
                Arc::new(StringArray::from(vec![r#"{"trace_id":"x"}"#])),
            ),
        ])
        .unwrap();
        let ctx = catalog_ctx(vec![("traces", signal_traces(false)), ("logs", logs)]);
        let err = IrService::new(ctx)
            .query(
                &signal_params("traces", error_logs_correlate("semi")),
                "t",
                "d",
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, QuerierError::InvalidInput(ref m)
                if m.contains("trace_id") && m.contains("target `logs`")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_source_column_named_like_a_correlate_helper_is_rejected() {
        let params = IrQueryParams {
            document: serde_json::json!({
                "irVersion": 11, "from": "traces", "range": { "from": 0, "to": 1000 },
                "result": "table",
                "pipeline": [
                    { "aggregate": { "by": ["trace_id"],
                        "aggs": [{ "fn": "count", "as": "__correlate_key_0" }] } },
                    { "correlate": error_logs_correlate("semi") }
                ]
            }),
            now_ns: 0,
            page: None,
        };
        let err = IrService::new(signal_ctx(false, false))
            .query(&params, "t", "d")
            .await
            .unwrap_err();
        assert!(
            matches!(err, QuerierError::InvalidInput(ref m) if m.contains("__correlate_key_0")),
            "{err:?}"
        );
    }

    // otel-native-schema layer 9 — inner/left correlate to a signal target.

    type LogRow = (u8, i64, i64, &'static str, &'static str);

    /// Logs of traces 1, 2 and 4 (five for trace 4, none for trace 3) plus
    /// one for trace 9, which is not in the source. Unsorted on purpose.
    const TRACE_LOGS: &[LogRow] = &[
        (1, 120, 17, "ERROR", "t1 boom"),
        (1, 110, 9, "INFO", "t1 start"),
        (2, 210, 9, "INFO", "t2 ok"),
        (4, 450, 9, "INFO", "t4 e"),
        (4, 410, 17, "ERROR", "t4 a"),
        (4, 430, 9, "INFO", "t4 c"),
        (4, 420, 9, "INFO", "t4 b"),
        (4, 440, 17, "ERROR", "t4 d"),
        (9, 150, 17, "ERROR", "t9"),
    ];

    /// `rows` as a logs table; `body` is stored JSON-encoded, as ingest
    /// writes it, and `binary` stores `trace_id` as raw bytes.
    fn enriched_ctx(rows: &[LogRow], binary: bool) -> SessionContext {
        catalog_ctx(vec![
            ("traces", signal_traces(false)),
            ("logs", enriched_logs(rows, binary)),
        ])
    }

    fn enriched_logs(rows: &[LogRow], binary: bool) -> RecordBatch {
        let trace_id: ArrayRef = if binary {
            Arc::new(datafusion::arrow::array::BinaryArray::from_iter_values(
                rows.iter().map(|r| {
                    let mut b = vec![0u8; 16];
                    b[15] = r.0;
                    b
                }),
            ))
        } else {
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| hex_id(r.0)),
            ))
        };
        RecordBatch::try_from_iter(vec![
            (
                "timestamp",
                Arc::new(TimestampNanosecondArray::from_iter_values(
                    rows.iter().map(|r| r.1),
                )) as ArrayRef,
            ),
            ("trace_id", trace_id),
            (
                "severity_number",
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))),
            ),
            (
                "severity_text",
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.3))),
            ),
            (
                "body",
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|r| serde_json::json!(r.4).to_string()),
                )),
            ),
            (
                "resource_identity",
                Arc::new(StringArray::from(vec!["r1"; rows.len()])),
            ),
        ])
        .unwrap()
    }

    fn traces_params(result: &str, pipeline: serde_json::Value) -> IrQueryParams {
        IrQueryParams {
            document: serde_json::json!({
                "irVersion": 11, "from": "traces", "range": { "from": 0, "to": 1000 },
                "result": result, "pipeline": pipeline
            }),
            now_ns: 0,
            page: None,
        }
    }

    /// Runs `params` and returns `columns` of every row as text, sorted.
    async fn correlated_rows(
        ctx: SessionContext,
        params: &IrQueryParams,
        columns: &[&str],
    ) -> (Vec<Vec<Option<String>>>, CorrelateReport) {
        let (mut rows, report) = ordered_rows(ctx, params, columns).await;
        rows.sort();
        (rows, report)
    }

    /// Like [`correlated_rows`], in result order.
    async fn ordered_rows(
        ctx: SessionContext,
        params: &IrQueryParams,
        columns: &[&str],
    ) -> (Vec<Vec<Option<String>>>, CorrelateReport) {
        let (batches, _, report) = with_empty_lookup(IrService::new(ctx))
            .query(params, "t", "d")
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in &batches {
            let cols: Vec<StringArray> = columns
                .iter()
                .map(|name| {
                    let col = batch
                        .column_by_name(name)
                        .unwrap_or_else(|| panic!("no column {name}: {:?}", batch.schema()));
                    let col = datafusion::arrow::compute::cast(col, &DataType::Utf8).unwrap();
                    col.as_string::<i32>().clone()
                })
                .collect();
            for i in 0..batch.num_rows() {
                rows.push(
                    cols.iter()
                        .map(|c| c.is_valid(i).then(|| c.value(i).to_string()))
                        .collect(),
                );
            }
        }
        (rows, report)
    }

    fn log_row(trace: u8, body: Option<&str>) -> Vec<Option<String>> {
        vec![Some(hex_id(trace)), body.map(str::to_string)]
    }

    fn inner_logs(extra: serde_json::Value) -> serde_json::Value {
        let mut correlate = serde_json::json!({ "to": "logs", "on": "trace_id", "kind": "inner" });
        correlate
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        serde_json::json!({ "correlate": correlate })
    }

    fn trace_is(n: u8) -> serde_json::Value {
        serde_json::json!({ "where": { "field": "trace_id", "op": "eq", "value": hex_id(n) } })
    }

    #[tokio::test]
    async fn inner_returns_the_logs_of_the_slowest_traces() {
        let params = traces_params(
            "table",
            serde_json::json!([
                { "aggregate": { "by": ["trace_id"], "aggs": [{ "fn": "max", "of": "duration", "as": "slowest" }] } },
                { "topk": { "n": 2, "of": "slowest" } },
                inner_logs(serde_json::json!({}))
            ]),
        );
        let (rows, report) = correlated_rows(
            enriched_ctx(TRACE_LOGS, false),
            &params,
            &["trace_id", "logs.body"],
        )
        .await;
        let expected: Vec<_> = [
            (1, "t1 boom"),
            (1, "t1 start"),
            (4, "t4 a"),
            (4, "t4 b"),
            (4, "t4 c"),
            (4, "t4 d"),
            (4, "t4 e"),
        ]
        .map(|(t, b)| log_row(t, Some(b)))
        .to_vec();
        assert_eq!(rows, expected);
        assert!(!report.fanout_limit);
    }

    /// Matches tied on time are cut by the remaining scalar columns; a full
    /// tie (identical scalar columns) keeps `fanout` of the identical rows.
    #[tokio::test]
    async fn fanout_ties_are_cut_deterministically() {
        let on_time: &[LogRow] = &[
            (4, 410, 9, "INFO", "c"),
            (4, 410, 9, "INFO", "a"),
            (4, 410, 9, "INFO", "b"),
        ];
        let full: &[LogRow] = &[(4, 410, 9, "INFO", "x"); 3];
        let params = traces_params(
            "rows",
            serde_json::json!([trace_is(4), inner_logs(serde_json::json!({ "fanout": 2 }))]),
        );
        for (logs, kept) in [(on_time, ["a", "b"]), (full, ["x", "x"])] {
            for _ in 0..2 {
                let (rows, report) = correlated_rows(
                    enriched_ctx(logs, false),
                    &params,
                    &["trace_id", "logs.body"],
                )
                .await;
                assert_eq!(rows, kept.map(|b| log_row(4, Some(b))).to_vec());
                assert!(report.fanout_limit);
            }
        }
    }

    /// Output follows the source order, then each row's match order, across
    /// partitions and a later `where`.
    #[tokio::test]
    async fn output_keeps_source_order_then_match_order() {
        let params = traces_params(
            "rows",
            serde_json::json!([
                { "order": [{ "of": "trace_id", "dir": "asc" }] },
                inner_logs(serde_json::json!({ "kind": "left" })),
                { "where": { "field": "service.name", "op": "eq", "value": "web" } }
            ]),
        );
        let (rows, _) = ordered_rows(
            enriched_ctx(TRACE_LOGS, false),
            &params,
            &["trace_id", "logs.body"],
        )
        .await;
        let expected = [
            (1, Some("t1 start")),
            (1, Some("t1 boom")),
            (2, Some("t2 ok")),
            (3, None),
            (4, Some("t4 a")),
            (4, Some("t4 b")),
            (4, Some("t4 c")),
            (4, Some("t4 d")),
            (4, Some("t4 e")),
        ]
        .map(|(t, b)| log_row(t, b))
        .to_vec();
        assert_eq!(rows, expected);
    }

    /// A `table` after aggregate → correlate shows the aggregate output plus
    /// the target's row defaults, not every raw target column.
    #[tokio::test]
    async fn aggregated_source_default_projection_uses_target_row_defaults() {
        let params = traces_params(
            "table",
            serde_json::json!([
                { "aggregate": { "by": ["trace_id"], "aggs": [{ "fn": "max", "of": "duration", "as": "slowest" }] } },
                inner_logs(serde_json::json!({}))
            ]),
        );
        let (batches, _, _) = IrService::new(enriched_ctx(TRACE_LOGS, false))
            .query(&params, "t", "d")
            .await
            .unwrap();
        let schema = batches[0].schema();
        let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            [
                "trace_id",
                "slowest",
                "logs.timestamp",
                "logs.body",
                "logs.severity_text",
                "logs.severity_number",
                "logs.trace_id"
            ]
        );
    }

    /// A missing target table yields the same target columns, all null: a
    /// left join keeps the source rows, an inner join none.
    #[tokio::test]
    async fn a_missing_target_table_keeps_the_target_columns() {
        let ctx = || catalog_ctx(vec![("traces", signal_traces(false))]);
        let left = traces_params(
            "rows",
            serde_json::json!([
                trace_is(3),
                inner_logs(serde_json::json!({ "kind": "left" }))
            ]),
        );
        let (rows, _) = correlated_rows(ctx(), &left, &["trace_id", "logs.body"]).await;
        assert_eq!(rows, vec![log_row(3, None)]);
        let inner = doc(traces_params(
            "rows",
            serde_json::json!([inner_logs(serde_json::json!({}))]),
        )
        .document);
        let (df, _) = with_empty_lookup(IrService::new(ctx()))
            .plan(&inner, "t", "d", 0)
            .await
            .unwrap()
            .unwrap();
        assert!(df.schema().has_column_with_unqualified_name("logs.body"));
        assert_eq!(df.count().await.unwrap(), 0);
    }

    /// An `IrService` whose registry holds no attribute types, so a typed
    /// empty target frame resolves the way a present one does.
    fn with_empty_lookup(service: IrService) -> IrService {
        service.with_canonical_types(Arc::new(StaticLookup(canonical_types(&[]))))
    }

    /// A left correlate to a missing `logs` table reading `logs.priority`,
    /// followed by `extra` stages.
    fn missing_target_doc(extra: serde_json::Value) -> Document {
        let mut d = serde_json::json!({
            "irVersion": 11, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "rows", "fields": ["trace_id", "logs.priority"],
            "pipeline": [inner_logs(serde_json::json!({ "kind": "left" }))]
        });
        d["pipeline"]
            .as_array_mut()
            .unwrap()
            .extend(extra.as_array().unwrap().clone());
        doc(d)
    }

    fn missing_target_types() -> CanonicalTypes {
        canonical_types(&[("priority", AttributeLevel::Record, CanonicalType::Int64)])
    }

    /// `<target>.x` on a missing target table keeps its canonical type: a
    /// typed Int64 null on the left-joined row, not a Utf8 null.
    #[tokio::test]
    async fn a_missing_target_table_resolves_target_prefixed_typed_attributes() {
        let ctx = catalog_ctx(vec![("traces", signal_traces(false))]);
        let batches = plan_typed_rows(
            &ctx,
            &missing_target_doc(serde_json::json!([])),
            missing_target_types(),
        )
        .await;
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 4);
        for batch in &batches {
            let column = batch
                .column_by_name(&safe_ident("logs.priority"))
                .expect("target column present");
            assert_eq!(column.data_type(), &DataType::Int64);
            assert_eq!(column.null_count(), column.len());
        }
    }

    /// A literal the canonical type cannot represent is rejected for a
    /// target-prefixed field even when the target table is missing.
    #[tokio::test]
    async fn a_missing_target_table_rejects_an_uncoercible_target_prefixed_literal() {
        let ctx = catalog_ctx(vec![("traces", signal_traces(false))]);
        let d = missing_target_doc(serde_json::json!([
            { "where": { "field": "logs.priority", "op": "eq", "value": "abc" } }
        ]));
        let err = plan_typed_err(&ctx, &d, missing_target_types()).await;
        assert!(
            matches!(err, QuerierError::InvalidInput(ref m) if m.contains("logs.priority")),
            "{err:?}"
        );
    }

    /// With no attribute registry attached, a missing target table errors the
    /// way a present typed one does.
    #[tokio::test]
    async fn a_missing_target_table_without_a_registry_errors_like_a_present_one() {
        let ctx = catalog_ctx(vec![("traces", signal_traces(false))]);
        let err = plan_document(
            &ctx,
            &missing_target_doc(serde_json::json!([])),
            PlanRequest::new("t", "d", 0)
                .with_attribute_type_request(AttributeTypeRequest::Resolve(None)),
        )
        .await
        .expect_err("a typed target frame needs the registry");
        assert!(
            err.to_string()
                .contains("attribute type registry not configured"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn an_unsupported_target_field_kind_is_invalid_input() {
        let traces = signal_traces(false);
        let events: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>; 4]));
        let mut columns = traces.columns().to_vec();
        columns.push(events);
        let mut fields = traces.schema().fields().to_vec();
        fields.push(Arc::new(Field::new("events", DataType::Utf8, true)));
        let traces = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        let logs = enriched_ctx(TRACE_LOGS, false)
            .table("t.d.logs")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .remove(0);
        let ctx = catalog_ctx(vec![("traces", traces), ("logs", logs)]);
        let params = IrQueryParams {
            document: serde_json::json!({
                "irVersion": 11, "from": "logs", "range": { "from": 0, "to": 1000 },
                "result": "rows", "fields": ["traces.span_events"],
                "pipeline": [{ "correlate": { "to": "traces", "on": "trace_id", "kind": "inner" } }]
            }),
            now_ns: 0,
            page: None,
        };
        let err = IrService::new(ctx)
            .query(&params, "t", "d")
            .await
            .unwrap_err();
        assert!(
            matches!(err, QuerierError::InvalidInput(ref m) if m.contains("traces.span_events")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn left_fanout_never_drops_an_unmatched_source_row() {
        let params = traces_params(
            "rows",
            serde_json::json!([inner_logs(
                serde_json::json!({ "kind": "left", "fanout": 1 })
            )]),
        );
        let (rows, report) = correlated_rows(
            enriched_ctx(TRACE_LOGS, false),
            &params,
            &["trace_id", "logs.body"],
        )
        .await;
        assert_eq!(
            rows,
            vec![
                log_row(1, Some("t1 start")),
                log_row(2, Some("t2 ok")),
                log_row(3, None),
                log_row(4, Some("t4 a")),
            ]
        );
        assert!(report.fanout_limit);
    }

    #[tokio::test]
    async fn where_on_a_target_field_after_the_join() {
        let params = traces_params(
            "rows",
            serde_json::json!([
                inner_logs(serde_json::json!({})),
                { "where": { "field": "logs.severity_number", "op": "gte", "value": 17 } }
            ]),
        );
        let (rows, _) = correlated_rows(
            enriched_ctx(TRACE_LOGS, false),
            &params,
            &["trace_id", "logs.body"],
        )
        .await;
        assert_eq!(
            rows,
            vec![
                log_row(1, Some("t1 boom")),
                log_row(4, Some("t4 a")),
                log_row(4, Some("t4 d")),
            ]
        );
    }

    #[tokio::test]
    async fn aggregate_by_a_target_field_after_the_join() {
        let params = traces_params(
            "table",
            serde_json::json!([
                inner_logs(serde_json::json!({})),
                { "aggregate": { "by": ["logs.severity_text"], "aggs": [{ "fn": "count", "as": "n" }] } }
            ]),
        );
        let (rows, _) = correlated_rows(
            enriched_ctx(TRACE_LOGS, false),
            &params,
            &["logs_severity_text", "n"],
        )
        .await;
        let expected: Vec<Vec<Option<String>>> = [["ERROR", "3"], ["INFO", "5"]]
            .map(|r| r.map(|v| Some(v.to_string())).to_vec())
            .to_vec();
        assert_eq!(rows, expected);
    }

    /// One source trace joined to its logs: `left` keeps a trace without
    /// logs with null target fields, `fanout` keeps the earliest matches and
    /// reports only a real overflow, and a Binary-encoded target key joins.
    #[tokio::test]
    async fn one_trace_joined_to_its_logs() {
        let cases: [(u8, bool, serde_json::Value, &[&str], bool); 5] = [
            (3, false, serde_json::json!({ "kind": "left" }), &[], false),
            (
                4,
                false,
                serde_json::json!({ "fanout": 2 }),
                &["t4 a", "t4 b"],
                true,
            ),
            (
                1,
                false,
                serde_json::json!({ "fanout": 2 }),
                &["t1 boom", "t1 start"],
                false,
            ),
            (
                1,
                true,
                serde_json::json!({}),
                &["t1 boom", "t1 start"],
                false,
            ),
            (4, true, serde_json::json!({ "fanout": 1 }), &["t4 a"], true),
        ];
        for (trace, binary, extra, bodies, fanout_limit) in cases {
            let params = traces_params(
                "rows",
                serde_json::json!([trace_is(trace), inner_logs(extra)]),
            );
            let (rows, report) = correlated_rows(
                enriched_ctx(TRACE_LOGS, binary),
                &params,
                &["trace_id", "logs.body"],
            )
            .await;
            let expected: Vec<_> = if bodies.is_empty() {
                vec![log_row(trace, None)]
            } else {
                bodies.iter().map(|b| log_row(trace, Some(b))).collect()
            };
            assert_eq!(rows, expected, "trace {trace} binary={binary}");
            assert_eq!(report.fanout_limit, fanout_limit, "trace {trace}");
        }
    }

    /// With the (trace_id, span_id) key, a target pair no source row holds
    /// still passes the per-field key bound; it must not count toward the
    /// fan-out overflow.
    #[tokio::test]
    async fn fanout_on_the_pair_key_counts_only_source_pairs() {
        let logs = RecordBatch::try_from_iter(vec![
            (
                "timestamp",
                Arc::new(TimestampNanosecondArray::from(vec![150_i64, 160, 170])) as ArrayRef,
            ),
            ("trace_id", Arc::new(StringArray::from(vec![hex_id(1); 3]))),
            (
                "span_id",
                Arc::new(StringArray::from(vec!["s1", "s2", "s2"])),
            ),
            (
                "body",
                Arc::new(StringArray::from(vec!["\"a\"", "\"b\"", "\"c\""])),
            ),
        ])
        .unwrap();
        let ctx = catalog_ctx(vec![("traces", signal_traces(false)), ("logs", logs)]);
        let params = traces_params(
            "rows",
            serde_json::json!([
                { "where": { "field": "trace_id", "op": "in", "value": [hex_id(1), hex_id(2)] } },
                inner_logs(serde_json::json!({ "on": "span_id", "fanout": 1 }))
            ]),
        );
        let (rows, report) = correlated_rows(ctx, &params, &["trace_id", "logs.body"]).await;
        assert_eq!(rows, vec![log_row(1, Some("a"))]);
        assert!(!report.fanout_limit);
    }

    /// Source rows whose key is null or empty never match: left keeps them
    /// with null target fields, inner drops them.
    #[tokio::test]
    async fn inner_and_left_with_only_null_or_empty_source_keys() {
        let ctx = || {
            catalog_ctx(vec![
                (
                    "traces",
                    spans(&[(None, "s1", 100, 10), (Some(""), "s2", 200, 10)]),
                ),
                ("logs", enriched_logs(TRACE_LOGS, false)),
            ])
        };
        let left = traces_params(
            "rows",
            serde_json::json!([inner_logs(serde_json::json!({ "kind": "left" }))]),
        );
        let (rows, _) = correlated_rows(ctx(), &left, &["trace_id", "logs.body"]).await;
        assert_eq!(
            rows,
            vec![vec![None, None], vec![Some(String::new()), None]]
        );
        let inner = traces_params(
            "rows",
            serde_json::json!([inner_logs(serde_json::json!({}))]),
        );
        let (rows, _) = correlated_rows(ctx(), &inner, &["trace_id", "logs.body"]).await;
        assert!(rows.is_empty(), "{rows:?}");
    }

    /// A missing target table on the `resource_identity` key: the key's
    /// stored column comes from the canonical schema, so left keeps every
    /// source row and anti keeps them all.
    #[tokio::test]
    async fn a_missing_target_table_on_resource_identity() {
        let ctx = || catalog_ctx(vec![("logs", enriched_logs(TRACE_LOGS, false))]);
        for kind in ["left", "anti"] {
            let params = IrQueryParams {
                document: serde_json::json!({
                    "irVersion": 11, "from": "logs", "range": { "from": 0, "to": 1000 },
                    "result": "rows",
                    "pipeline": [{ "correlate": { "to": "traces", "on": "resource_identity", "kind": kind } }]
                }),
                now_ns: 0,
                page: None,
            };
            let (batches, _, _) = with_empty_lookup(IrService::new(ctx()))
                .query(&params, "t", "d")
                .await
                .unwrap();
            let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(rows, TRACE_LOGS.len(), "{kind}");
        }
    }

    /// Like [`traces_ctx`], plus the `events` column: three spans, one
    /// clean, one erroring with a captured `exception` event, one erroring
    /// with no event at all (an error status set without a captured
    /// exception, e.g. a hand-set span status).
    fn traces_events_ctx() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("span_name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("start_time_unix_nano", DataType::Int64, false),
            Field::new("duration_nanos", DataType::Int64, false),
            Field::new("status_code", DataType::Utf8, true),
            Field::new("events", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["t0", "t1", "t2"])),
                Arc::new(StringArray::from(vec!["s0", "s1", "s2"])),
                Arc::new(StringArray::from(vec!["GET /a", "GET /b", "GET /c"])),
                Arc::new(StringArray::from(vec!["api", "api", "api"])),
                Arc::new(Int64Array::from(vec![10_i64, 20, 30])),
                Arc::new(Int64Array::from(vec![100_i64, 200, 300])),
                Arc::new(StringArray::from(vec![
                    Some("OK"),
                    Some("ERROR"),
                    Some("ERROR"),
                ])),
                Arc::new(StringArray::from(vec![
                    None,
                    Some(
                        r#"[{"name":"exception","timestamp_unix_nano":1700000000000000000,"attributes_json":"{\"exception.type\":\"std::io::Error\",\"exception.message\":\"boom\",\"exception.stacktrace\":\"at foo\"}"}]"#,
                    ),
                    Some("[]"),
                ])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("traces".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    /// Nine spans (`s0`..`s8`) with `events`/`links` JSON: s0 has events
    /// `start` and `retry` (attempt 3), s1 an `exception` event, s2 a link to
    /// `aaaa`, s3 NULL events/links, s4 malformed JSON, s5 empty lists, s6/s7
    /// a `retry` event and an event with a null name (both orders), s8 an
    /// event named `say "hi" é`.
    fn traces_lists_ctx() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("span_id", DataType::Utf8, false),
            Field::new("start_time_unix_nano", DataType::Int64, false),
            Field::new("events", DataType::Utf8, true),
            Field::new("links", DataType::Utf8, true),
        ]));
        let events0 = r#"[{"name":"start"},{"name":"retry","attributes_json":"{\"attempt\":3,\"reason\":\"timeout\",\"fatal\":false}"}]"#;
        let events1 =
            r#"[{"name":"exception","attributes_json":"{\"exception.type\":\"IoError\"}"}]"#;
        let links2 = r#"[{"trace_id":"aaaa","span_id":"bbbb","attributes_json":"{\"kind\":\"follows\"}"},{"trace_id":"cccc","span_id":"dddd"}]"#;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![
                    "s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8",
                ])),
                Arc::new(Int64Array::from(vec![1_i64; 9])),
                Arc::new(StringArray::from(vec![
                    Some(events0),
                    Some(events1),
                    None,
                    None,
                    Some("not json"),
                    Some("[]"),
                    Some(r#"[{"name":"retry"},{"name":null}]"#),
                    Some(r#"[{"name":null},{"name":"retry"}]"#),
                    Some(r#"[{"name":"say \"hi\" é"}]"#),
                ])),
                Arc::new(StringArray::from(vec![
                    None,
                    None,
                    Some(links2),
                    None,
                    Some("{{"),
                    Some("[]"),
                    None,
                    None,
                    None,
                ])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("traces".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    async fn span_ids_where(predicate: serde_json::Value) -> Vec<String> {
        let svc = IrService::new(traces_lists_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["span_id"],
            "pipeline": [ { "where": predicate }, { "order": [{ "of": "span_id", "dir": "asc" }] } ]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let batches = df.collect().await.unwrap();
        batches
            .iter()
            .flat_map(|b| strings_of(b, "span_id"))
            .collect()
    }

    /// `span_links` is the whole links list of a span (#1802), normalized
    /// like `span_events` so each link's attributes are a JSON object. A span
    /// with no links, or unreadable links, yields NULL.
    #[tokio::test]
    async fn span_links_returns_the_normalized_links_list() {
        let svc = IrService::new(traces_lists_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["span_id", "span_links"]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let mut by_span: HashMap<String, Option<serde_json::Value>> = HashMap::new();
        for batch in df.collect().await.unwrap() {
            let ids = strings_of(&batch, "span_id");
            let links = batch
                .column_by_name("span_links")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for (i, id) in ids.into_iter().enumerate() {
                by_span.insert(
                    id,
                    (!links.is_null(i)).then(|| serde_json::from_str(links.value(i)).unwrap()),
                );
            }
        }
        assert_eq!(
            by_span.get("s2"),
            Some(&Some(serde_json::json!([
                { "trace_id": "aaaa", "span_id": "bbbb", "attributes": { "kind": "follows" } },
                { "trace_id": "cccc", "span_id": "dddd", "attributes": {} }
            ])))
        );
        for span in ["s0", "s4", "s5"] {
            assert_eq!(by_span.get(span), Some(&None), "{span} has no links");
        }
    }

    #[tokio::test]
    async fn span_links_is_retrieval_only() {
        let svc = IrService::new(traces_lists_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["span_id"],
            "pipeline": [{ "where": { "field": "span_links", "op": "exists" } }]
        }));
        let err = svc.plan(&d, "t", "d", 0).await.expect_err("rejected");
        assert!(
            format!("{err}").contains("span_links"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn span_list_predicates_match_any_element() {
        use serde_json::json;
        // (field, op, value — null for `exists`, expected span ids)
        let cases = [
            ("events.name", "eq", json!("nope"), vec![]),
            ("events.name", "eq", json!("retry"), vec!["s0", "s6", "s7"]),
            ("events.name", "eq", json!("say \"hi\" é"), vec!["s8"]),
            ("links.trace_id", "eq", json!("CCCC"), vec!["s2"]),
            ("links.span_id", "in", json!(["BBBB"]), vec!["s2"]),
            ("links.trace_id", "contains", json!("AAA"), vec!["s2"]),
            (
                "events.attributes.reason",
                "eq",
                json!("timeout"),
                vec!["s0"],
            ),
            ("events.attributes.attempt", "eq", json!(3), vec!["s0"]),
            ("events.attributes.attempt", "eq", json!("3"), vec!["s0"]),
            ("events.attributes.fatal", "eq", json!(false), vec!["s0"]),
            ("links.trace_id", "eq", json!("cccc"), vec!["s2"]),
            ("links.span_id", "eq", json!("bbbb"), vec!["s2"]),
            ("links.attributes.kind", "eq", json!("follows"), vec!["s2"]),
            (
                "events.name",
                "in",
                json!(["exception", "start"]),
                vec!["s0", "s1"],
            ),
            (
                "events.attributes.reason",
                "contains",
                json!("time"),
                vec!["s0"],
            ),
            ("events.name", "regex", json!("^exc.*n$"), vec!["s1"]),
            (
                "events.name",
                "exists",
                json!(null),
                vec!["s0", "s1", "s6", "s7", "s8"],
            ),
            (
                "events.attributes.attempt",
                "exists",
                json!(null),
                vec!["s0"],
            ),
            ("links.trace_id", "exists", json!(null), vec!["s2"]),
        ];
        for (field, op, value, expected) in cases {
            let mut leaf = json!({"field": field, "op": op});
            if !value.is_null() {
                leaf["value"] = value;
            }
            let not = json!({"not": leaf});
            assert_eq!(span_ids_where(leaf.clone()).await, expected, "{leaf}");
            // `not` keeps exactly the complement: NULL/malformed/empty rows
            // never match a leaf, so they survive the negation.
            let complement: Vec<_> = ["s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"]
                .into_iter()
                .filter(|s| !expected.contains(s))
                .collect();
            assert_eq!(span_ids_where(not.clone()).await, complement, "{not}");
        }
    }

    #[tokio::test]
    async fn span_list_leaves_in_one_and_match_independently() {
        use serde_json::json;
        let both = |a: &str, b: &str| {
            json!({"and": [
                {"field": "events.name", "op": "eq", "value": a},
                {"field": "events.name", "op": "eq", "value": b},
            ]})
        };
        // s0 has `start` and `retry`; s1 has only `exception`.
        assert_eq!(span_ids_where(both("start", "retry")).await, vec!["s0"]);
        assert!(span_ids_where(both("start", "exception")).await.is_empty());
        assert!(span_ids_where(both("exception", "retry")).await.is_empty());
    }

    #[tokio::test]
    async fn span_list_rejects_unsupported_ops_at_plan_time() {
        for op in ["ne", "gt"] {
            let svc = IrService::new(traces_lists_ctx());
            let d = doc(serde_json::json!({
                "irVersion": 1, "from": "traces", "range": { "from": 0, "to": 1000 },
                "result": "rows",
                "pipeline": [ { "where": {"field": "events.name", "op": op, "value": "x"} } ]
            }));
            assert!(svc.plan(&d, "t", "d", 0).await.is_err(), "{op}");
        }
    }

    // exception.type/message/stacktrace are not stored as their own columns —
    // they live inside the `exception` span event's own attributes. A span
    // with no exception event (t0: clean, t2: error with no captured
    // exception) must resolve to NULL rather than erroring or matching.
    #[tokio::test]
    async fn exception_attributes_resolve_from_the_exception_event() {
        let svc = IrService::new(traces_events_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["span.name", "exception.type", "exception.message", "exception.stacktrace"],
            "pipeline": [
                { "where": { "field": "exception.type", "op": "exists" } }
            ]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total, 1,
            "only the span with a captured exception event survives the exists filter"
        );
        let batch = &batches[0];
        let name = batch
            .column_by_name("span_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(name, "GET /b");
        let ty = batch
            .column_by_name("exception_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(ty, "std::io::Error");
        let message = batch
            .column_by_name("exception_message")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(message, "boom");
        let stacktrace = batch
            .column_by_name("exception_stacktrace")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(stacktrace, "at foo");
    }

    // A span with an error status but no captured exception (t2) must not be
    // mistaken for one that has an exception — `exists` must see NULL, not
    // an empty string or a spurious match.
    #[tokio::test]
    async fn exception_type_is_null_without_a_captured_exception_event() {
        let svc = IrService::new(traces_events_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["span.name", "exception.type"],
            "pipeline": [
                { "where": { "field": "status.code", "op": "eq", "value": "ERROR" } }
            ]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total, 2,
            "both error spans (t1, t2) match the status filter"
        );
        let mut by_name: HashMap<String, Option<String>> = HashMap::new();
        for batch in &batches {
            let names = batch
                .column_by_name("span_name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let types = batch
                .column_by_name("exception_type")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                by_name.insert(
                    names.value(i).to_string(),
                    (!types.is_null(i)).then(|| types.value(i).to_string()),
                );
            }
        }
        assert_eq!(
            by_name.get("GET /b"),
            Some(&Some("std::io::Error".to_string()))
        );
        assert_eq!(by_name.get("GET /c"), Some(&None));
    }

    // `span_events` is the whole events list of a span (issue #1280): the
    // stored `events` column, normalized so each event's attributes are a
    // JSON object rather than the writer's double-encoded `attributes_json`
    // string. A span without events yields NULL; the field is retrieval-only.
    #[tokio::test]
    async fn span_events_returns_the_normalized_events_list() {
        let svc = IrService::new(traces_events_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["span_id", "span_events"]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let batches = df.collect().await.unwrap();
        let mut by_span: HashMap<String, Option<serde_json::Value>> = HashMap::new();
        for batch in &batches {
            let ids = batch
                .column_by_name("span_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let events = batch
                .column_by_name("span_events")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                by_span.insert(
                    ids.value(i).to_string(),
                    (!events.is_null(i)).then(|| serde_json::from_str(events.value(i)).unwrap()),
                );
            }
        }
        assert_eq!(by_span.get("s0"), Some(&None), "no events column value");
        assert_eq!(
            by_span.get("s1"),
            Some(&Some(serde_json::json!([{
                "name": "exception",
                "timestamp_unix_nano": 1700000000000000000_u64,
                "attributes": {
                    "exception.type": "std::io::Error",
                    "exception.message": "boom",
                    "exception.stacktrace": "at foo"
                }
            }])))
        );
        assert_eq!(by_span.get("s2"), Some(&Some(serde_json::json!([]))));
    }

    #[tokio::test]
    async fn span_events_is_retrieval_only() {
        let svc = IrService::new(traces_events_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["span_id"],
            "pipeline": [{ "where": { "field": "span_events", "op": "exists" } }]
        }));
        let err = svc.plan(&d, "t", "d", 0).await.expect_err("rejected");
        assert!(
            format!("{err}").contains("span_events"),
            "unexpected error: {err}"
        );
    }

    // The Errors & Exceptions UI groups spans by exception.type to count
    // occurrences per exception — grouping by a UDF-computed expression
    // (not a physical column) must work through DataFusion's aggregate().
    #[tokio::test]
    async fn spans_group_by_exception_type_with_counts() {
        let svc = IrService::new(traces_events_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "table",
            "pipeline": [
                { "where": { "field": "exception.type", "op": "exists" } },
                { "aggregate": {
                    "by": ["exception.type"],
                    "aggs": [{ "fn": "count", "as": "count" }]
                }}
            ]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1, "one distinct exception.type group");
        let batch = &batches[0];
        let ty = batch
            .column_by_name("exception_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(ty, "std::io::Error");
        let count = batch
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 1);
    }

    /// A `by` field whose name carries uppercase letters used to fail with a
    /// DataFusion `FieldNotFound { name: "statuscode" }` (HTTP 500): the
    /// group column is aliased verbatim, but the follow-up references went
    /// through `col()`, which parses its argument as a SQL identifier and
    /// normalizes an unquoted one to lowercase. Reference the alias as a
    /// literal identifier instead (#1070).
    #[tokio::test]
    async fn aggregate_by_a_mixed_case_field_keeps_its_alias() {
        let svc = IrService::new(traces_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "series",
            "pipeline": [
                { "aggregate": {
                    "by": ["statusCode"],
                    "aggs": [{ "fn": "count", "as": "n" }],
                    "step": "1ms"
                }}
            ]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .expect("planning a mixed-case group key succeeds")
            .expect("source table is registered");
        let batches = df.collect().await.expect("the plan executes");
        let batch = batches.iter().find(|b| b.num_rows() > 0).expect("a row");
        assert!(
            batch.column_by_name("statusCode").is_some(),
            "the group column keeps the spelling the document used: {:?}",
            batch.schema().fields().iter().collect::<Vec<_>>()
        );
    }

    /// The same normalization broke ordering by a mixed-case aggregate
    /// output as well — `order` resolves through the same alias table.
    #[tokio::test]
    async fn order_by_a_mixed_case_aggregate_output_executes() {
        let svc = IrService::new(traces_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "table",
            "pipeline": [
                { "aggregate": {
                    "by": ["service.name"],
                    "aggs": [{ "fn": "count", "as": "spanCount" }]
                }},
                { "order": [{ "of": "spanCount", "dir": "desc" }] }
            ]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .expect("planning a mixed-case aggregate output succeeds")
            .expect("source table is registered");
        let batches = df.collect().await.expect("the plan executes");
        let counts: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column_by_name("spanCount")
                    .expect("the aggregate output keeps its spelling")
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .expect("count is an i64")
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(counts, vec![2, 1], "descending by span count");
    }

    /// A minimal table for one logical source, carrying only its time
    /// column and a nullable `resource_identity` column: two rows share one
    /// digest, a third carries a different one. Enough to exercise
    /// `resource.identity` as a `rows` projection target, a `where`
    /// operand, and an `aggregate.by` key without pulling in every other
    /// physical column that source's real schema has (#1340).
    fn resource_identity_ctx(
        table: &str,
        time_col: &str,
        time_is_timestamp: bool,
        metric_type: Option<&str>,
    ) -> SessionContext {
        let time_field = if time_is_timestamp {
            Field::new(
                time_col,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )
        } else {
            Field::new(time_col, DataType::Int64, false)
        };
        let mut fields = vec![
            time_field,
            Field::new("resource_identity", DataType::Utf8, true),
        ];
        let identities = StringArray::from(vec![
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        ]);
        let time_array: ArrayRef = if time_is_timestamp {
            Arc::new(TimestampNanosecondArray::from(vec![10_i64, 20, 30]))
        } else {
            Arc::new(Int64Array::from(vec![10_i64, 20, 30]))
        };
        let mut columns: Vec<ArrayRef> = vec![time_array, Arc::new(identities)];
        // The wide `metrics` table's scan filters on `metric_type` even for a
        // plain rows/where/aggregate query, so the `metrics` case needs it.
        if let Some(mt) = metric_type {
            fields.push(Field::new("metric_type", DataType::Utf8, false));
            columns.push(Arc::new(StringArray::from(vec![mt; 3])));
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let ctx = SessionContext::new();
        let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table(table.to_string(), Arc::new(mem)).unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    #[tokio::test]
    async fn resource_identity_projects_filters_and_groups_by_the_physical_column() {
        for (source, table, time_col, time_is_timestamp, metric_type) in [
            ("logs", "logs", "timestamp", true, None),
            ("traces", "traces", "start_time_unix_nano", false, None),
            ("profiles", "profiles", "timestamp", true, None),
            ("metrics", "metrics", "timestamp", true, Some("gauge")),
        ] {
            let svc = IrService::new(resource_identity_ctx(
                table,
                time_col,
                time_is_timestamp,
                metric_type,
            ));

            // `rows`: the projected column is the physical `resource_identity`.
            let d = doc(serde_json::json!({
                "irVersion": 1, "from": source, "range": { "from": 0, "to": 1000 },
                "result": "rows",
                "fields": ["resource.identity"],
                "pipeline": []
            }));
            let (df, _) = svc
                .plan(&d, "t", "d", 0)
                .await
                .unwrap_or_else(|e| panic!("{source}: rows projection should plan: {e}"))
                .expect("source table is registered");
            let batches = df.collect().await.unwrap();
            assert_eq!(
                batches[0].schema().field(0).name(),
                "resource_identity",
                "{source}: projects the physical column"
            );

            // `where`: filtering on resource.identity narrows to the two
            // rows sharing that digest.
            let d = doc(serde_json::json!({
                "irVersion": 1, "from": source, "range": { "from": 0, "to": 1000 },
                "result": "rows",
                "fields": ["resource.identity"],
                "pipeline": [{ "where": { "field": "resource.identity", "op": "eq",
                                           "value": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" } }]
            }));
            let (df, _) = svc
                .plan(&d, "t", "d", 0)
                .await
                .unwrap_or_else(|e| panic!("{source}: filter should plan: {e}"))
                .expect("source table is registered");
            let batches = df.collect().await.unwrap();
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(
                total, 2,
                "{source}: filter matches both rows of one identity"
            );

            // `aggregate.by`: grouping by resource.identity yields the two
            // distinct digests, never a null group.
            let d = doc(serde_json::json!({
                "irVersion": 1, "from": source, "range": { "from": 0, "to": 1000 },
                "result": "table",
                "pipeline": [{ "aggregate": { "by": ["resource.identity"],
                                               "aggs": [{ "fn": "count", "as": "n" }] } }]
            }));
            let (df, _) = svc
                .plan(&d, "t", "d", 0)
                .await
                .unwrap_or_else(|e| panic!("{source}: aggregate should plan: {e}"))
                .expect("source table is registered");
            let batches = df.collect().await.unwrap();
            let mut groups: Vec<(Option<String>, i64)> = Vec::new();
            for b in &batches {
                let keys = b
                    .column_by_name("resource_identity")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let ns = b
                    .column_by_name("n")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..b.num_rows() {
                    groups.push((
                        (!keys.is_null(i)).then(|| keys.value(i).to_string()),
                        ns.value(i),
                    ));
                }
            }
            groups.sort();
            assert_eq!(
                groups,
                vec![
                    (Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()), 2),
                    (Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()), 1),
                ],
                "{source}: two non-null groups, never a null bucket"
            );
        }
    }

    /// Like [`traces_ctx`], plus the `timestamp` partition column the real v2
    /// table carries (partition transform: `Hour(timestamp)`).
    fn traces_partitioned_ctx() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("span_name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("start_time_unix_nano", DataType::Int64, false),
            Field::new("duration_nanos", DataType::Int64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["t1", "t2"])),
                Arc::new(StringArray::from(vec!["s1", "s2"])),
                Arc::new(StringArray::from(vec!["GET /a", "GET /b"])),
                Arc::new(StringArray::from(vec!["api", "api"])),
                Arc::new(Int64Array::from(vec![10_i64, 20])),
                Arc::new(Int64Array::from(vec![100_i64, 900])),
                Arc::new(TimestampNanosecondArray::from(vec![10_i64, 20])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("traces".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    // Issue #928: the IR trace window filters only start_time_unix_nano while
    // the Iceberg partition transform is Hour(timestamp) — the plan must also
    // bound the `timestamp` partition column so pruning engages.
    #[tokio::test]
    async fn traces_time_window_bounds_partition_column() {
        let svc = IrService::new(traces_partitioned_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["trace_id", "start_time_unix_nano"],
            "pipeline": []
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let plan = format!("{}", df.logical_plan().display_indent());
        assert!(
            plan.contains("start_time_unix_nano >="),
            "missing precise lower row bound:\n{plan}"
        );
        assert!(
            plan.contains(".timestamp >="),
            "missing partition-pruning lower bound on `timestamp`:\n{plan}"
        );
        assert!(
            plan.contains(".timestamp <="),
            "missing partition-pruning upper bound on `timestamp`:\n{plan}"
        );
        // Still executes: both in-window rows survive the widened bound.
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    // Task 4.3 — single-signal trace query: filter + topk lowers and executes.
    #[tokio::test]
    async fn traces_where_topk_lowers_and_executes() {
        let svc = IrService::new(traces_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["span.name", "duration"],
            "pipeline": [
                { "where": { "field": "service.name", "op": "eq", "value": "api" } },
                { "topk": { "n": 1, "of": "duration" } }
            ]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        // `service.name` aliases to the physical `service_name` column.
        let plan = format!("{}", df.logical_plan().display_indent());
        assert!(plan.contains("service_name"), "plan:\n{plan}");
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "topk(1) returns one span");
        // The slowest `api` span is t2 (900ns).
        let dur = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(dur, 900);
    }

    #[tokio::test]
    async fn trace_heatmap_uses_epoch_buckets_and_duration_boundary_overflow_bins() {
        let svc = IrService::new(traces_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 2, "from": "traces", "range": { "from": -10, "to": 40 },
            "result": "heatmap", "pipeline": [{ "heatmap": {
                "x": { "step": "10ns", "align": "epoch" },
                "y": { "of": "duration", "bounds": [100, 500, 900], "overflow": true },
                "value": { "fn": "count", "as": "count" }
            }}]
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let plan = format!("{}", df.logical_plan().display_indent());
        assert!(
            plan.contains("start_time_unix_nano"),
            "precise range predicate is retained: {plan}"
        );
        let batches = df.collect().await.unwrap();
        let mut cells = Vec::new();
        for batch in batches {
            let time = batch
                .column_by_name("time_bucket_ns")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let bucket = batch
                .column_by_name("duration_bucket")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let count = batch
                .column_by_name("count")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                cells.push((time.value(row), bucket.value(row), count.value(row)));
            }
        }
        assert_eq!(cells, vec![(-10, 1, 1), (10, 1, 1), (20, 3, 1), (30, 2, 1)]);
    }

    #[test]
    fn heatmap_window_counts_aligned_buckets_inclusive_of_end() {
        assert_eq!(heatmap_bucket_count(1, 5121, 10).unwrap(), 513);
    }

    #[test]
    fn heatmap_window_rejects_extreme_timestamp_ranges() {
        let err = heatmap_bucket_count(i64::MIN, i64::MAX, 1).unwrap_err();
        assert!(err.to_string().contains("overflows"));
    }

    #[tokio::test]
    async fn trace_heatmap_keeps_partition_pruning_predicates() {
        let svc = IrService::new(traces_partitioned_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 2, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "heatmap", "pipeline": [{ "heatmap": {
                "x": { "step": "1us", "align": "epoch" },
                "y": { "of": "duration", "bounds": ["1ns"], "overflow": true },
                "value": { "fn": "count", "as": "count" }
            }}]
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let plan = format!("{}", df.logical_plan().display_indent());
        assert!(
            plan.contains(".timestamp >=") && plan.contains(".timestamp <="),
            "partition bounds missing: {plan}"
        );
    }

    /// A logs table whose `body` holds JSON documents, for `extract`.
    fn logs_json_ctx() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("body", DataType::Utf8, true),
            Field::new("service_name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![10_i64, 20, 30])),
                Arc::new(StringArray::from(vec![
                    Some(r#"{"level":"error","code":500}"#),
                    Some(r#"{"level":"info","code":200}"#),
                    Some(r#"{"level":"error","code":503}"#),
                ])),
                Arc::new(StringArray::from(vec!["api", "api", "web"])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("logs".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    // Task 4.8 — extract (json) derives a typed field usable by a later stage.
    #[tokio::test]
    async fn extract_json_derives_usable_field() {
        let svc = IrService::new(logs_json_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["level"],
            "pipeline": [
                { "extract": { "parser": "json", "as": [{ "name": "level", "type": "string" }] } },
                { "where": { "field": "level", "op": "eq", "value": "error" } }
            ]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "two rows have level=error");
        // The projected column is the extracted `level`.
        for b in &batches {
            let col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..b.num_rows() {
                assert_eq!(col.value(i), "error");
            }
        }
    }

    // Regression (promotion invariance): an ordered comparison on an unpromoted
    // String attribute must lower (lexically), not reject with "needs a number".
    #[tokio::test]
    async fn ordered_comparison_on_string_attribute_lowers() {
        let svc = IrService::new(logs_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["service.name"],
            "pipeline": [
                { "where": { "field": "deployment.environment", "op": "gte", "value": "prod" } }
            ]
        }));
        // Plans and executes; the Utf8 attribute compares lexically, no error.
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let _ = df.collect().await.unwrap();
    }

    /// A logs table whose `body` holds JSON-string-*encoded* documents — the
    /// form ingest actually writes (`serde_json::to_string` over the log
    /// text, issue #1410), unlike `logs_json_ctx`'s bare JSON object text.
    /// One row's body text is itself a self-contained JSON string literal
    /// (`"quoted already"` — starts *and* ends with a quote, nothing
    /// trailing) — the one shape that actually distinguishes "decoded once"
    /// from "decoded twice": a body with trailing text after an embedded
    /// quote (e.g. `"foo" bar`) fails to re-parse as JSON on a second decode
    /// and would pass either way, proving nothing (review finding on
    /// #1432). This one's second decode would visibly strip the quotes.
    fn logs_json_encoded_ctx() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("body", DataType::Utf8, true),
            Field::new("service_name", DataType::Utf8, true),
        ]));
        let encode = |s: &str| serde_json::to_string(s).unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![10_i64, 20, 30])),
                Arc::new(StringArray::from(vec![
                    Some(encode(
                        r#"{"level":"error","code":500,"__ir_decoded_body":"nope"}"#,
                    )),
                    Some(encode("level=info dur=5ms")),
                    Some(encode(r#""quoted already""#)),
                ])),
                Arc::new(StringArray::from(vec!["api", "api", "web"])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("logs".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    // Issue #1410: `extract_json_derives_usable_field` seeds `body` with
    // already-unwrapped JSON object text, which `decode_log_body` already
    // leaves alone — it passes identically with or without the body-decode
    // fix. This seeds `body` the way ingest actually encodes it
    // (`serde_json::to_string` over the log text) and proves `json`/`logfmt`
    // extraction operates on the real text, not the JSON string literal.
    #[tokio::test]
    async fn extract_json_parses_ingest_encoded_body_not_the_json_string_literal() {
        let svc = IrService::new(logs_json_encoded_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["level"],
            "pipeline": [
                { "extract": { "parser": "json", "as": [{ "name": "level", "type": "string" }] } },
                { "where": { "field": "level", "op": "eq", "value": "error" } }
            ]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "only the JSON-object-body row has level=error");
    }

    #[tokio::test]
    async fn extract_logfmt_parses_ingest_encoded_body_not_the_json_string_literal() {
        let svc = IrService::new(logs_json_encoded_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["service.name", "dur"],
            "pipeline": [
                { "extract": { "parser": "logfmt", "as": [{ "name": "dur", "type": "string" }] } },
                { "where": { "field": "dur", "op": "exists" } }
            ]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "only the logfmt-body row has a dur field");
        let col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "5ms");
    }

    // Issue #1410: a body whose text is itself a self-contained JSON string
    // literal must decode exactly once. A second (accidental) decode pass
    // would strip that quoting and lose data — this fixture shape is what
    // makes the test able to tell the difference (see `logs_json_encoded_ctx`).
    #[tokio::test]
    async fn body_field_decodes_exactly_once_for_a_message_that_is_itself_quoted() {
        let svc = IrService::new(logs_json_encoded_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["body"],
            "pipeline": [
                { "where": { "field": "service.name", "op": "eq", "value": "web" } }
            ]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), r#""quoted already""#);
    }

    // Issue #1410 review fix: `extract.as_fields` is query-document input and
    // must not be able to collide with any querier-internal name. Before this
    // fix, `lower_extract` decoded `body` into a *named* hidden column and
    // re-resolved it per field — an `as_fields` entry that happened to share
    // that name would silently overwrite it (`DataFrame::with_column`
    // overwrites in place), corrupting every later field in the stage. The
    // fix threads the decode as an unmaterialized `Expr` instead, so no such
    // name exists to collide with; this pins that multiple fields — one
    // deliberately named after the old hidden column — still each resolve
    // independently from the real decoded body.
    #[tokio::test]
    async fn extract_as_field_named_like_the_old_hidden_column_does_not_corrupt_other_fields() {
        let svc = IrService::new(logs_json_encoded_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["__ir_decoded_body", "level"],
            "pipeline": [
                { "extract": { "parser": "json", "as": [
                    { "name": "__ir_decoded_body", "type": "string" },
                    { "name": "level", "type": "string" }
                ] } },
                { "where": { "field": "level", "op": "eq", "value": "error" } }
            ]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "only the JSON-object-body row has level=error");
        let collider = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // The field literally named `__ir_decoded_body` extracts the JSON
        // key of the same name from the body (unrelated to the decode
        // mechanism) and must resolve to that value rather than error or
        // silently become the raw decoded body.
        assert_eq!(collider.value(0), "nope");
        let level = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(level.value(0), "error");
    }

    #[test]
    fn extract_field_parses_json_and_logfmt() {
        assert_eq!(
            extract_field(r#"{"level":"error"}"#, "json", "level"),
            Some("error".to_string())
        );
        assert_eq!(
            extract_field("level=warn dur=5ms", "logfmt", "dur"),
            Some("5ms".to_string())
        );
        assert_eq!(extract_field("no match here", "logfmt", "level"), None);
    }

    /// Build a minimal `ScalarFunctionArgs` for direct `ExtractUdf` unit
    /// tests, bypassing the planner/DataFrame machinery.
    fn extract_args(args: Vec<ColumnarValue>, number_rows: usize) -> ScalarFunctionArgs {
        let arg_fields = args
            .iter()
            .map(|a| Arc::new(Field::new("arg", a.data_type(), true)))
            .collect();
        ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field: Arc::new(Field::new("ir_extract", DataType::Utf8, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        }
    }

    fn extract_output_values(cv: ColumnarValue, len: usize) -> Vec<Option<String>> {
        let arrays = ColumnarValue::values_to_arrays(&[cv]).unwrap();
        let out = arrays[0].as_any().downcast_ref::<StringArray>().unwrap();
        (0..len)
            .map(|i| (!out.is_null(i)).then(|| out.value(i).to_string()))
            .collect()
    }

    // ir_extract must not materialize the (always-scalar-in-practice) parser
    // and key arguments into full-length arrays — this exercises the
    // ColumnarValue::Scalar branch directly.
    #[test]
    fn ir_extract_udf_handles_scalar_parser_and_key_args() {
        let udf = ExtractUdf::new();
        let bodies: ArrayRef = Arc::new(StringArray::from(vec![
            Some(r#"{"level":"error"}"#),
            None,
            Some(r#"{"level":"info"}"#),
        ]));
        let args = extract_args(
            vec![
                ColumnarValue::Array(bodies),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("json".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("level".to_string()))),
            ],
            3,
        );
        let out = udf.invoke_with_args(args).unwrap();
        assert_eq!(
            extract_output_values(out, 3),
            vec![Some("error".to_string()), None, Some("info".to_string())]
        );
    }

    // The signature must accept a Utf8View body (e.g. after DataFusion's
    // string-view optimizations), not just plain Utf8.
    #[test]
    fn ir_extract_udf_handles_utf8view_body() {
        let udf = ExtractUdf::new();
        let bodies: ArrayRef = Arc::new(datafusion::arrow::array::StringViewArray::from(vec![
            Some(r#"{"level":"warn"}"#),
            Some(r#"{"other":"field"}"#),
        ]));
        let args = extract_args(
            vec![
                ColumnarValue::Array(bodies),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("json".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("level".to_string()))),
            ],
            2,
        );
        let out = udf.invoke_with_args(args).unwrap();
        assert_eq!(
            extract_output_values(out, 2),
            vec![Some("warn".to_string()), None]
        );
    }

    // LargeUtf8 body is accepted too, rounding out the three UTF-8 encodings
    // the signature declares.
    #[test]
    fn ir_extract_udf_handles_large_utf8_body() {
        let udf = ExtractUdf::new();
        let bodies: ArrayRef = Arc::new(datafusion::arrow::array::LargeStringArray::from(vec![
            Some("level=error dur=5ms"),
        ]));
        let args = extract_args(
            vec![
                ColumnarValue::Array(bodies),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("logfmt".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("dur".to_string()))),
            ],
            1,
        );
        let out = udf.invoke_with_args(args).unwrap();
        assert_eq!(extract_output_values(out, 1), vec![Some("5ms".to_string())]);
    }

    // A genuine Utf8View `body` column, run through the exact call shape
    // `lower_extract` builds (`ir_extract(col("body"), lit(parser),
    // lit(key))`), end to end through DataFusion's real expression
    // evaluation (not just a hand-built `ScalarFunctionArgs`) — this proves
    // the signature's coercion/dispatch, not just the invoke body.
    #[tokio::test]
    async fn ir_extract_expr_runs_against_utf8view_column_via_dataframe() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "body",
            DataType::Utf8View,
            true,
        )]));
        let bodies: ArrayRef = Arc::new(datafusion::arrow::array::StringViewArray::from(vec![
            Some(r#"{"level":"error"}"#),
            Some(r#"{"level":"info"}"#),
            None,
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![bodies]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_batch("logs_view", batch).unwrap();
        let df = ctx.table("logs_view").await.unwrap();

        let udf = ScalarUDF::from(ExtractUdf::new());
        let df = df
            .with_column(
                "level",
                udf.call(vec![col("body"), lit("json"), lit("level")]),
            )
            .unwrap()
            .select(vec![col("level")])
            .unwrap();
        let batches = df.collect().await.unwrap();
        let mut values: Vec<Option<String>> = Vec::new();
        for b in &batches {
            let arr = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..b.num_rows() {
                values.push((!arr.is_null(i)).then(|| arr.value(i).to_string()));
            }
        }
        assert_eq!(
            values,
            vec![Some("error".to_string()), Some("info".to_string()), None]
        );
    }

    // Task 4.4 — absent-value semantics in the lowered plan.
    #[tokio::test]
    async fn negated_equality_excludes_absent_rows() {
        let svc = IrService::new(logs_ctx());
        // Row 3 (service=web) has no `env` (label_env NULL). not(env=prod)
        // must exclude it, not include it.
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["service.name"],
            "pipeline": [
                { "where": { "not": { "field": "env", "op": "eq", "value": "prod" } } }
            ]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // All present env values are "prod", and the one absent row is excluded.
        assert_eq!(total, 0, "absent row must be excluded by not(field = x)");
    }

    // Task 4.5 — curated projection: rows returns only the fields set.
    #[tokio::test]
    async fn rows_projection_is_curated() {
        let svc = IrService::new(logs_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["service.name", "severity_number"],
            "pipeline": []
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        let batches = df.collect().await.unwrap();
        assert_eq!(
            batches[0].num_columns(),
            2,
            "only the fields set is projected"
        );
        let names: Vec<_> = batches[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(
            names,
            vec!["service_name".to_string(), "severity_number".to_string()]
        );
    }

    // Task 4.6 — relative-time determinism.
    #[tokio::test]
    async fn relative_time_resolves_once_deterministically() {
        let svc = IrService::new(logs_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": "now-1h", "to": "now" },
            "result": "rows", "pipeline": []
        }));
        let now = 3_600_000_000_000_i64; // 1h in ns
        let (_, w1) = svc
            .plan(&d, "t", "d", now)
            .await
            .unwrap()
            .expect("source table is registered");
        let (_, w2) = svc
            .plan(&d, "t", "d", now)
            .await
            .unwrap()
            .expect("source table is registered");
        assert_eq!(w1, w2);
        assert_eq!(w1.end_ns, now);
        assert_eq!(w1.start_ns, 0);
    }

    // Task 4.7 — regex safety guard.
    #[test]
    fn regex_guard_bounds_pathological_patterns() {
        assert!(compile_regex_guard("^GET /api").is_ok());
        // A pattern whose compiled size explodes past the limit is rejected.
        let adversarial =
            "((((((((((a{1000}){1000}){1000}){1000}){1000}){1000}){1000}){1000}){1000}){1000})";
        assert!(compile_regex_guard(adversarial).is_err());
    }

    // The planner's per-source physical column assumptions MUST match the
    // canonical persisted Iceberg schema — the traces v2 renames (`name` →
    // `span_name`, `duration_nano` → `duration_nanos`) are the trap this guards.
    #[test]
    fn source_plan_columns_match_real_persisted_schema() {
        use common::schema::SCHEMA_DEFINITIONS;
        use std::collections::HashSet;

        // A name is realized either as the legacy single map column, or
        // (typed layout, e.g. every signal's current version) as its
        // `{name}_residue` column among the five typed-home columns --
        // either is a valid realization, since `SourcePlan`'s container
        // names are logical container identities, not literal column names.
        let realized = |cols: &HashSet<String>, name: &str| {
            cols.contains(name)
                || common::schema::typed_attributes::has_typed_container(
                    cols.iter().map(String::as_str),
                    name,
                )
        };

        let check = |sp: &SourcePlan, cols: &HashSet<String>, sig: &str| {
            assert!(
                cols.contains(sp.time_col),
                "{sig} time_col '{}' not in schema",
                sp.time_col
            );
            for c in sp.containers {
                assert!(realized(cols, c), "{sig} container '{c}' not in schema");
            }
            for c in sp.row_defaults {
                assert!(realized(cols, c), "{sig} row default '{c}' not in schema");
            }
            for (_, physical) in sp.aliases {
                assert!(
                    realized(cols, physical),
                    "{sig} alias target '{physical}' not in schema"
                );
            }
        };

        let logs = SCHEMA_DEFINITIONS
            .resolve_log_schema(&SCHEMA_DEFINITIONS.metadata.current_log_version)
            .unwrap();
        let log_cols: HashSet<String> = logs.fields.iter().map(|f| f.name.clone()).collect();
        check(&SourcePlan::for_source("logs").unwrap(), &log_cols, "logs");

        let traces = SCHEMA_DEFINITIONS
            .resolve_trace_schema(&SCHEMA_DEFINITIONS.metadata.current_trace_version)
            .unwrap();
        let trace_cols: HashSet<String> = traces.fields.iter().map(|f| f.name.clone()).collect();
        check(
            &SourcePlan::for_source("traces").unwrap(),
            &trace_cols,
            "traces",
        );

        let profiles = common::iceberg::schemas::create_profiles_schema().unwrap();
        let profile_cols: HashSet<String> = profiles
            .fields()
            .iter()
            .map(|field| field.name.to_string())
            .collect();
        check(
            &SourcePlan::for_source("profiles").unwrap(),
            &profile_cols,
            "profiles",
        );

        let metrics = common::iceberg::schemas::create_metrics_schema().unwrap();
        let metrics_cols: HashSet<String> = metrics
            .fields()
            .iter()
            .map(|f| f.name.to_string())
            .collect();
        check(
            &SourcePlan::for_source("metrics").unwrap(),
            &metrics_cols,
            "metrics",
        );
        let exemplars = common::iceberg::schemas::create_metric_exemplars_schema().unwrap();
        let exemplar_cols: HashSet<String> = exemplars
            .fields()
            .iter()
            .map(|f| f.name.to_string())
            .collect();
        check(
            &SourcePlan::for_source("exemplars").unwrap(),
            &exemplar_cols,
            "exemplars",
        );
        assert!(SourcePlan::for_source("metrics_histogram").is_none());
    }

    /// Group 7 (`otel-compliant-self-tracing`): query execution decomposes
    /// into plan/execute stage spans carrying result-size attributes.
    #[tokio::test]
    async fn query_emits_stage_spans_with_row_counts() {
        use opentelemetry::trace::TracerProvider as _;
        use tracing::instrument::WithSubscriber;
        use tracing_subscriber::prelude::*;

        let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));

        async {
            let svc = IrService::new(logs_ctx())
                .with_canonical_types(Arc::new(StaticLookup(canonical_types(&[]))));
            let params = crate::query::IrQueryParams {
                document: serde_json::json!({
                    "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
                    "result": "rows",
                    "pipeline": []
                }),
                now_ns: 0,
                page: None,
            };
            let _ = svc.query(&params, "t", "d").await.unwrap();
        }
        .with_subscriber(subscriber)
        .await;

        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        let names: Vec<_> = spans.iter().map(|s| s.name.to_string()).collect();
        assert!(
            names.iter().any(|n| n == "signaldb.query.plan"),
            "no plan stage span; exported = {names:?}"
        );
        let exec = spans
            .iter()
            .find(|s| s.name == "signaldb.query.execute")
            .unwrap_or_else(|| panic!("no execute stage span; exported = {names:?}"));
        let rows = exec
            .attributes
            .iter()
            .find(|kv| kv.key.as_str() == "signaldb.query.rows")
            .map(|kv| kv.value.clone())
            .unwrap_or_else(|| {
                panic!(
                    "execute span carries signaldb.query.rows; attrs = {:?}",
                    exec.attributes
                )
            });
        assert!(matches!(rows, opentelemetry::Value::I64(_)));
    }

    // ---- Absent source table reads as empty (issue #972) ----

    /// A `t.d` dataset registered in the catalog but holding no tables.
    fn empty_dataset_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", Arc::new(MemorySchemaProvider::new()))
            .unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    fn logs_ir_params() -> IrQueryParams {
        IrQueryParams {
            document: serde_json::json!({
                "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
                "result": "rows",
                "pipeline": []
            }),
            now_ns: 0,
            page: None,
        }
    }

    #[tokio::test]
    async fn query_on_absent_source_table_is_empty() {
        let svc = IrService::new(empty_dataset_ctx());
        let (batches, window, _) = svc
            .query(&logs_ir_params(), "t", "d")
            .await
            .expect("absent table must not error");
        assert!(batches.is_empty());
        // The window is still resolved so the caller can echo it back.
        assert_eq!(
            window,
            ResolvedWindow {
                start_ns: 0,
                end_ns: 1000
            }
        );
    }

    #[tokio::test]
    async fn unknown_tenant_still_errors_on_ir_query() {
        let svc = IrService::new(empty_dataset_ctx());
        assert!(
            svc.query(&logs_ir_params(), "nosuchtenant", "d")
                .await
                .is_err(),
            "unknown tenant must not read as empty"
        );
    }

    #[tokio::test]
    async fn malformed_ir_document_still_errors_when_table_is_absent() {
        let svc = IrService::new(empty_dataset_ctx());
        let params = IrQueryParams {
            document: serde_json::json!({
                "irVersion": 1, "from": "nosuchsource", "range": { "from": 0, "to": 1 },
                "result": "rows", "pipeline": []
            }),
            now_ns: 0,
            page: None,
        };
        assert!(matches!(
            svc.query(&params, "t", "d").await,
            Err(QuerierError::InvalidInput(_))
        ));
    }

    #[tokio::test]
    async fn invalid_time_bound_still_errors_when_table_is_absent() {
        let svc = IrService::new(empty_dataset_ctx());
        let params = IrQueryParams {
            document: serde_json::json!({
                "irVersion": 1, "from": "logs", "range": { "from": "not-a-time", "to": 1 },
                "result": "rows", "pipeline": []
            }),
            now_ns: 0,
            page: None,
        };
        assert!(matches!(
            svc.query(&params, "t", "d").await,
            Err(QuerierError::InvalidInput(_))
        ));
    }

    // Task 2 — scoped aggregates. The `logs_ctx` fixture holds four rows:
    // (api, sev 17), (api, sev 9), (web, sev 17), (web, sev 21) — so `api` has
    // one row at/above 17 out of two, and `web` has two out of two.

    /// Run a document against `logs_ctx` and return its rows.
    async fn collect_doc(v: serde_json::Value) -> Vec<RecordBatch> {
        let svc = IrService::new(logs_ctx());
        let (df, _) = svc
            .plan(&doc(v), "t", "d", 0)
            .await
            .unwrap()
            .expect("source table is registered");
        df.collect().await.unwrap()
    }

    /// The `n`-th Int64 column of the single returned batch, keyed by the
    /// group column so the assertion does not depend on group ordering.
    fn counts_by_group(batches: &[RecordBatch], group: &str, measure: &str) -> Vec<(String, i64)> {
        let b = &batches[0];
        let g = b
            .column_by_name(group)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let m = b.column_by_name(measure).unwrap();
        let m = m.as_any().downcast_ref::<Int64Array>().unwrap();
        let mut out: Vec<(String, i64)> = (0..b.num_rows())
            .map(|i| (g.value(i).to_string(), m.value(i)))
            .collect();
        out.sort();
        out
    }

    #[tokio::test]
    async fn a_scoped_count_measures_only_matching_rows() {
        let batches = collect_doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "table",
            "pipeline": [ { "aggregate": { "by": ["service.name"], "aggs": [
                { "fn": "count", "as": "n" },
                { "fn": "count", "as": "errors",
                  "where": { "field": "severity_number", "op": "gte", "value": 17 } }
            ] } } ]
        }))
        .await;
        assert_eq!(
            counts_by_group(&batches, "service_name", "n"),
            vec![("api".to_string(), 2), ("web".to_string(), 2)],
            "the unscoped count covers every row in the group"
        );
        assert_eq!(
            counts_by_group(&batches, "service_name", "errors"),
            vec![("api".to_string(), 1), ("web".to_string(), 2)],
            "the scoped count covers only matching rows"
        );
    }

    #[tokio::test]
    async fn a_group_with_no_matching_row_reports_zero_and_is_kept() {
        let batches = collect_doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "table",
            "pipeline": [ { "aggregate": { "by": ["service.name"], "aggs": [
                { "fn": "count", "as": "n" },
                // No `api` row reaches 21; `web` has exactly one.
                { "fn": "count", "as": "worst",
                  "where": { "field": "severity_number", "op": "gte", "value": 21 } }
            ] } } ]
        }))
        .await;
        assert_eq!(
            counts_by_group(&batches, "service_name", "worst"),
            vec![("api".to_string(), 0), ("web".to_string(), 1)],
            "a group with no matching row is kept, reporting zero"
        );
    }

    #[tokio::test]
    async fn scoping_does_not_change_the_group_set() {
        let unscoped = collect_doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "table",
            "pipeline": [ { "aggregate": { "by": ["service.name"],
                "aggs": [ { "fn": "count", "as": "n" } ] } } ]
        }))
        .await;
        let scoped = collect_doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "table",
            "pipeline": [ { "aggregate": { "by": ["service.name"], "aggs": [
                { "fn": "count", "as": "n" },
                { "fn": "count", "as": "errors",
                  "where": { "field": "severity_number", "op": "gte", "value": 17 } }
            ] } } ]
        }))
        .await;
        assert_eq!(
            counts_by_group(&unscoped, "service_name", "n"),
            counts_by_group(&scoped, "service_name", "n"),
            "adding a scoped aggregate leaves the groups and their totals alone"
        );
    }

    #[tokio::test]
    async fn a_scoped_aggregate_lowers_to_one_aggregate_node() {
        let svc = IrService::new(logs_ctx());
        let (df, _) = svc
            .plan(
                &doc(serde_json::json!({
                        "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
                        "result": "table",
                "pipeline": [ { "aggregate": { "by": ["service.name"], "aggs": [
                            { "fn": "count", "as": "n" },
                            { "fn": "count", "as": "errors",
                              "where": { "field": "severity_number", "op": "gte", "value": 17 } }
                        ] } } ]
                    })),
                "t",
                "d",
                0,
            )
            .await
            .unwrap()
            .expect("source table is registered");
        let mut nodes = Vec::new();
        plan_node_types(df.logical_plan(), &mut nodes);
        assert_eq!(
            nodes.iter().filter(|n| **n == "Aggregate").count(),
            1,
            "one grouping, not one per aggregate: {nodes:?}"
        );
    }

    #[tokio::test]
    async fn a_scoped_quantile_measures_only_matching_rows() {
        let batches = collect_doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "table",
                    "pipeline": [ { "aggregate": { "by": ["service.name"], "aggs": [
                // `web` holds severities 17 and 21; scoped to >= 21 the median
                // must be 21, not 19.
                { "fn": "quantile", "of": "severity_number", "arg": 0.5, "as": "p50",
                  "where": { "field": "severity_number", "op": "gte", "value": 21 } }
            ] } } ]
        }))
        .await;
        let b = &batches[0];
        let g = b
            .column_by_name("service_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let p = b.column_by_name("p50").unwrap();
        let p = p
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap();
        let web = (0..b.num_rows())
            .find(|i| g.value(*i) == "web")
            .expect("web group present");
        assert_eq!(
            p.value(web),
            21.0,
            "the quantile sees only the rows the scope admits"
        );
    }

    // -----------------------------------------------------------------
    // `first`/`last` aggregate ordering (found while removing
    // `ir-single-lowering`'s rollout switches): `AggFn::Last` ordered by
    // `time_col` *descending*, so "last" in that order picked the row with
    // the *smallest* time — the earliest sample, the same one `first`
    // already picked. Fixed by ordering both ascending and letting
    // `last_value`'s own semantics (the value at the end of the frame) pick
    // the true latest row.
    // -----------------------------------------------------------------

    /// `logs_ctx`'s four rows carry `severity_number` 17, 9, 17, 21 at
    /// `timestamp` 10, 20, 30, 40 — `first` (earliest) must be 17 (row 1),
    /// `last` (latest) must be 21 (row 4), never the same value as `first`.
    #[tokio::test]
    async fn first_and_last_aggregates_order_by_time() {
        let svc = IrService::new(logs_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 5, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "table",
            "pipeline": [
                { "aggregate": { "by": [], "aggs": [
                    { "fn": "first", "of": "severity_number", "as": "first_sev" },
                    { "fn": "last", "of": "severity_number", "as": "last_sev" }
                ] } }
            ]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("logs table is registered");
        let batches = df.collect().await.unwrap();
        let batch = &batches[0];
        let first = batch
            .column_by_name("first_sev")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let last = batch
            .column_by_name("last_sev")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(
            first, 17,
            "first (earliest, ts=10) should be row 1's severity_number"
        );
        assert_eq!(
            last, 21,
            "last (latest, ts=40) should be row 4's severity_number, not row 1's again"
        );
    }

    /// #1433: `first_value(body)`/`last_value(body)` must return the same
    /// decoded text a `rows` result of `body` shows, not the raw
    /// JSON-encoded column.
    #[tokio::test]
    async fn first_and_last_aggregates_decode_body() {
        let svc = IrService::new(logs_body_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 5, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "table",
            "pipeline": [
                { "aggregate": { "by": [], "aggs": [
                    { "fn": "first", "of": "body", "as": "first_body" },
                    { "fn": "last", "of": "body", "as": "last_body" }
                ] } }
            ]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("logs table is registered");
        let batches = df.collect().await.unwrap();
        let batch = &batches[0];
        let col = |name: &str| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .to_string()
        };
        assert_eq!(col("first_body"), "a", "first (earliest, ts=10) decoded");
        assert_eq!(col("last_body"), "d", "last (latest, ts=40) decoded");
    }

    /// #1433: grouping by `body` must use the same decoded value the
    /// projection returns, so a `table` result's group key text matches what
    /// a `rows` result of the same field shows for the same log.
    #[tokio::test]
    async fn group_by_body_decodes_the_group_key() {
        let svc = IrService::new(logs_body_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 5, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "table",
            "pipeline": [
                { "aggregate": { "by": ["body"], "aggs": [
                    { "fn": "count", "as": "n" }
                ] } }
            ]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("logs table is registered");
        let keys = collect_sorted_string_column(&df.collect().await.unwrap(), "body");
        assert_eq!(
            keys,
            expected_bodies(|_| true),
            "group keys are the decoded text, not the raw quoted column"
        );
    }

    // -----------------------------------------------------------------
    // #816 — a promoted attribute column may still be NULL in files the
    // compactor hasn't rewritten since promotion (Iceberg schema evolution
    // null-fills new columns in pre-existing files); the querier must keep
    // coalescing with the JSON fallback, never trust the column alone.
    // -----------------------------------------------------------------

    /// Three rows over `log_attributes`' `x` key: row 1 has `x=v`, row 2 has
    /// `x=other`, row 3 has no `x` key at all (genuinely absent, not merely
    /// empty — for the Kleene absent-key case).
    ///
    /// `promoted == false` models the table before `x` is ever promoted (no
    /// `label_x` column at all — resolves through the plain `JsonPath`
    /// path). `promoted == true` models the table immediately after
    /// promotion, before the compactor has rewritten a single file: schema
    /// evolution (`common::iceberg::evolution::add_label_columns`) adds
    /// `label_x` to the *schema*, but every existing file null-fills it on
    /// read — so `label_x` is NULL for every row here, exactly like a real
    /// unrewritten file would read back.
    fn logs_attr_x_ctx(promoted: bool) -> SessionContext {
        let mut fields = vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, true),
            map_field(),
        ];
        if promoted {
            fields.push(Field::new("label_x", DataType::Utf8, true));
        }
        let schema = Arc::new(Schema::new(fields));

        let ts = TimestampNanosecondArray::from(vec![10_i64, 20, 30]);
        let service = StringArray::from(vec![Some("svc1"), Some("svc2"), Some("svc3")]);
        let log_attrs = build_map(&[&[("x", "v")], &[("x", "other")], &[]]);

        let mut columns: Vec<ArrayRef> = vec![Arc::new(ts), Arc::new(service), log_attrs];
        if promoted {
            // Every row null-filled — no file has been rewritten yet.
            columns.push(Arc::new(StringArray::from(
                vec![None, None, None] as Vec<Option<&str>>
            )));
        }

        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let batch =
            common::testing::to_typed_layout("logs", "physical-v4", &batch, &["log_attributes"]);
        let ctx = SessionContext::new();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("logs".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    /// `service_name` of rows matching `pred` against `logs_attr_x_ctx`,
    /// sorted for a deterministic comparison.
    async fn matching_services(ctx: &SessionContext, pred: serde_json::Value) -> Vec<String> {
        let svc = IrService::new(ctx.clone());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["service.name"],
            "pipeline": [{ "where": pred }]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap_or_else(|e| panic!("plan failed: {e}"))
            .expect("table is registered");
        let batches = df
            .collect()
            .await
            .unwrap_or_else(|e| panic!("execute failed: {e}"));
        let mut out = Vec::new();
        for b in &batches {
            let col = b
                .column_by_name("service_name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..b.num_rows() {
                out.push(col.value(i).to_string());
            }
        }
        out.sort();
        out
    }

    /// The issue's own acceptance scenario: query results for `{x="v"}`
    /// must be identical before promotion (plain `JsonPath`) and right
    /// after promotion with zero files backfilled (`label_x` all-NULL,
    /// `PromotedColumn` coalescing with the JSON fallback).
    #[tokio::test]
    async fn promoted_column_matches_baseline_before_backfill() {
        let pred = serde_json::json!({ "field": "x", "op": "eq", "value": "v" });
        let baseline = matching_services(&logs_attr_x_ctx(false), pred.clone()).await;
        let promoted = matching_services(&logs_attr_x_ctx(true), pred).await;
        assert_eq!(
            baseline,
            vec!["svc1".to_string()],
            "only svc1 has x=v in log_attributes"
        );
        assert_eq!(
            promoted, baseline,
            "promotion with zero files backfilled must not change the result (#816)"
        );
    }

    /// Negation through the promoted-column fallback keeps Kleene absent-key
    /// semantics: row 3 has no `x` key in `label_x` (NULL, unbackfilled) or
    /// in `log_attributes` (genuinely absent) — `x != "v"` must exclude it,
    /// matching `negated_equality_excludes_absent_rows`'s pattern for the
    /// unpromoted path.
    #[tokio::test]
    async fn promoted_column_negation_excludes_absent_key_before_backfill() {
        let pred = serde_json::json!({ "field": "x", "op": "ne", "value": "v" });
        let promoted = matching_services(&logs_attr_x_ctx(true), pred.clone()).await;
        assert_eq!(
            promoted,
            vec!["svc2".to_string()],
            "svc2 (x=other) matches != v; svc1 (x=v) doesn't; svc3's absent key matches neither = nor !="
        );
        // Same result whether or not `x` was ever promoted.
        let baseline = matching_services(&logs_attr_x_ctx(false), pred).await;
        assert_eq!(promoted, baseline);
    }

    /// Regression for the gap `728113e` left in `SchemaResolver::df_col`:
    /// it only matched `Resolved::Column`, so a promoted attribute fell
    /// through to `safe_ident(logical)` (`"x"`) instead of the real
    /// materialized column (`"label_x"`) — a column that doesn't exist in
    /// the scanned schema, so `ORDER BY` on a promoted attribute broke.
    /// `df_col` must resolve `Resolved::PromotedColumn` to its `name` just
    /// like it already does for `Resolved::Column`.
    #[tokio::test]
    async fn order_by_promoted_attribute_resolves_to_materialized_column() {
        let ctx = logs_attr_x_ctx(true);
        let svc = IrService::new(ctx);
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "fields": ["service.name"],
            "pipeline": [{ "order": [{ "of": "x", "dir": "desc" }] }]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap_or_else(|e| panic!("plan failed: {e}"))
            .expect("table is registered");
        let plan = format!("{}", df.logical_plan().display_indent());
        assert!(
            plan.contains("coalesce") && plan.contains("label_x"),
            "ORDER BY on a promoted-but-unbackfilled attribute must sort by the coalesced \
             value (column, then attribute-map fallback), not the bare null column alone, got:\n{plan}"
        );
        let batches = df
            .collect()
            .await
            .unwrap_or_else(|e| panic!("execute failed: {e}"));
        let services: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                let col = b
                    .column_by_name("service_name")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .clone();
                (0..b.num_rows()).map(move |i| col.value(i).to_string())
            })
            .collect();
        // `label_x` is NULL in every row (nothing backfilled yet); the
        // fixture's log_attributes hold the real values: svc1 has x="v",
        // svc2 has x="other", svc3 has no `x` at all. If ordering still
        // trusted the bare null column, every row would tie and come back
        // in scan order (svc1, svc2, svc3) regardless of `desc` -- the
        // exact bug this test guards against. Sorting the coalesced value
        // descending with nulls first instead gives: absent (svc3), then
        // "v" > "other".
        assert_eq!(
            services,
            vec!["svc3", "svc1", "svc2"],
            "order must follow the attribute-map fallback value, not the null column"
        );
    }

    /// Regression (#1672): a promoted attribute with no declared logical
    /// type is still "untyped" for the numeric-ordered-comparison rule in
    /// `Lowering::lower_leaf`. `SchemaResolver::is_known` also returns `true`
    /// once a materialized `label_*` column exists for the field, so using
    /// it to decide "untyped" wrongly treated a promoted-but-undeclared
    /// `num` as typed `String` and compared lexicographically ("9" > "50"
    /// as strings), keeping the wrong rows. `num` here is fully promoted
    /// (`label_num` populated, no JSON fallback needed) and never declared
    /// in the logical schema, so `num > 10` must still route through the
    /// numeric `TRY_CAST` path and keep only `"50"`.
    #[tokio::test]
    async fn promoted_untyped_attribute_compares_numerically() {
        let fields = vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, true),
            map_field(),
            Field::new("label_num", DataType::Utf8, true),
        ];
        let schema = Arc::new(Schema::new(fields));

        let ts = TimestampNanosecondArray::from(vec![10_i64, 20, 30]);
        let service = StringArray::from(vec![Some("svc1"), Some("svc2"), Some("svc3")]);
        let log_attrs = build_map(&[&[("num", "9")], &[("num", "50")], &[("num", "abc")]]);
        let label_num = StringArray::from(vec![Some("9"), Some("50"), Some("abc")]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(ts),
                Arc::new(service),
                log_attrs,
                Arc::new(label_num),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("logs".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);

        let pred = serde_json::json!({ "field": "num", "op": "gt", "value": 10 });
        let services = matching_services(&ctx, pred).await;
        assert_eq!(
            services,
            vec!["svc2".to_string()],
            "num > 10 must keep only svc2 (num=\"50\"), not svc1 (num=\"9\" lexicographically \
             greater than \"10\") or svc3 (num=\"abc\", non-numeric)"
        );
    }

    /// A [`CanonicalTypeLookup`] that counts its own calls, standing in for
    /// `CatalogCanonicalTypes` so `plan_document`'s fetch-only-if-typed gate
    /// can be checked end-to-end (via `IrService::query`) without a real
    /// catalog.
    struct CountingLookup {
        calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl CanonicalTypeLookup for CountingLookup {
        async fn canonical_types(
            &self,
            _tenant_slug: &str,
            _dataset_slug: &str,
            _signal: &str,
        ) -> Result<CanonicalTypes, QuerierError> {
            self.calls.fetch_add(1, AtomicOrdering::Relaxed);
            Ok(CanonicalTypes::default())
        }
    }

    fn residue_only_table_ctx(table_name: &str) -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("log_attributes_residue", DataType::Binary, true),
        ]));
        let ts = TimestampNanosecondArray::from(vec![10_i64]);
        let residue = datafusion::arrow::array::BinaryArray::from(vec![None::<&[u8]>]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ts), Arc::new(residue)]).unwrap();
        single_table_ctx(table_name, schema, batch)
    }

    fn rows_query_params() -> IrQueryParams {
        IrQueryParams {
            document: serde_json::json!({
                "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
                "result": "rows", "fields": ["timestamp"]
            }),
            now_ns: 0,
            page: None,
        }
    }

    #[tokio::test]
    async fn typed_table_fetches_canonical_types_once() {
        let ctx = residue_only_table_ctx("logs");
        let lookup = Arc::new(CountingLookup {
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let svc = IrService::new(ctx).with_canonical_types(lookup.clone());

        svc.query(&rows_query_params(), "t", "d").await.unwrap();
        assert_eq!(lookup.calls.load(AtomicOrdering::Relaxed), 1);
    }

    #[tokio::test]
    async fn typed_table_without_lookup_is_an_error() {
        let ctx = residue_only_table_ctx("logs");
        let svc = IrService::new(ctx);

        let err = svc.query(&rows_query_params(), "t", "d").await.unwrap_err();
        assert!(matches!(err, QuerierError::QueryFailed(_)));
    }
}
