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

use std::collections::HashMap;
use std::sync::Arc;

use common::profile::aggregate_profiles_to_flamegraph;
use common::query_ir::{
    Aggregate, ComparisonOp, Document, Extract, FieldResolver, Heatmap, HistogramMode,
    HistogramQuantile, Leaf, Literal, Parser, Predicate, Resolved, ResultEnvelope, SourceRegistry,
    Stage, TimestampLiteral, ValueType, coerce, validate,
};
use common::schema::logical::{Filterability, LogicalSchema, LogicalType};
use datafusion::arrow::array::{
    Array, BooleanArray, Float64Array, LargeStringArray, StringArray, StringBuilder,
    StringViewArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::datatypes::{DataType, Field, IntervalMonthDayNano, Schema, TimeUnit};
use datafusion::datasource::TableProvider;
use datafusion::functions::core::expr_fn::{coalesce, get_field};
use datafusion::functions::datetime::expr_fn::date_bin;
use datafusion::functions::regex::expr_fn::regexp_like;
use datafusion::functions::string::expr_fn::contains;
use datafusion::functions_aggregate::expr_fn::{approx_percentile_cont, avg, count, max, min, sum};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprFunctionExt, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility, cast, col, lit, not,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr::expressions::{CastExpr, Column as PhysicalColumn};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::scalar::ScalarValue;

use super::IrQueryParams;
use super::error::QuerierError;
use super::histogram::{
    HistogramAcc, RateHistAcc, histogram_quantile, parse_bounds_cached, parse_f64_array,
};
use super::profile::batch_to_models;
use super::table_lookup::{optional_table_provider, scan_provider};

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
struct SourcePlan {
    /// The logical source name (as written in a document's `from`) — used for
    /// display/error messages, distinct from `tables` below since a source
    /// can scan more than one physical table (metrics unions gauge + sum).
    name: &'static str,
    /// Physical tables scanned for this source. A dataset missing one of
    /// several (e.g. no sum metrics ingested yet) still scans the rest; only
    /// a dataset missing *all* of them has no rows to plan over.
    tables: &'static [&'static str],
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
    fn for_source(source: &str) -> Option<SourcePlan> {
        match source {
            "logs" => Some(SourcePlan {
                name: "logs",
                tables: &["logs"],
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
                    ("resource.schema_url", "resource_schema_url"),
                    ("scope.name", "scope_name"),
                    ("scope.version", "scope_version"),
                    ("scope.schema_url", "scope_schema_url"),
                ],
            }),
            "traces" => Some(SourcePlan {
                name: "traces",
                tables: &["traces"],
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
                    ("name", "span_name"),
                    ("span.name", "span_name"),
                    ("duration", "duration_nanos"),
                    ("duration_nano", "duration_nanos"),
                    ("status.code", "status_code"),
                    ("resource.schema_url", "resource_schema_url"),
                    ("scope.name", "scope_name"),
                    ("scope.version", "scope_version"),
                    ("scope.schema_url", "scope_schema_url"),
                ],
            }),
            "profiles" => Some(SourcePlan {
                name: "profiles",
                tables: &["profiles"],
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
                    ("profile.id", "profile_id"),
                    ("duration", "duration_nano"),
                    ("sample.type", "sample_type"),
                    ("sample.unit", "sample_unit"),
                    ("period.type", "period_type"),
                    ("period.unit", "period_unit"),
                    ("service.name", "service_name"),
                    ("trace.id", "trace_id"),
                    ("span.id", "span_id"),
                ],
            }),
            "metrics" => Some(SourcePlan {
                name: "metrics",
                // Gauge + sum only — both share the same scalar `value`
                // column shape. Histograms have a bucketed row shape with no
                // IR aggregate equivalent yet (no `histogram_quantile`
                // stage), so they're deliberately excluded here rather than
                // scanned and misinterpreted as plain values.
                tables: &["metrics_gauge", "metrics_sum"],
                time_col: "timestamp",
                time_is_timestamp: true,
                containers: &["attributes", "resource_attributes"],
                attr_prefixes: &[("resource.", "resource_attributes")],
                row_defaults: &[
                    "timestamp",
                    "service_name",
                    "metric_name",
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
                ],
            }),
            "metrics_histogram" => Some(SourcePlan {
                name: "metrics_histogram",
                // One whole histogram (bucket_counts/explicit_bounds) per
                // row, not a scalar value — a separate source from `metrics`
                // since the row shape differs. Only reachable via the
                // `histogram_quantile` stage, which reads the bucket columns
                // by physical name directly (see `lower_histogram_quantile`)
                // rather than through the resolver — comparing a JSON-array
                // string in a `where` is meaningless, so they get no alias.
                tables: &["metrics_histogram"],
                time_col: "timestamp",
                time_is_timestamp: true,
                containers: &["attributes", "resource_attributes"],
                attr_prefixes: &[("resource.", "resource_attributes")],
                row_defaults: &[
                    "timestamp",
                    "service_name",
                    "metric_name",
                    "count",
                    "sum",
                    "min",
                    "max",
                    "bucket_counts",
                    "explicit_bounds",
                    "aggregation_temporality",
                    "attributes",
                    "resource_attributes",
                ],
                aliases: &[
                    ("service.name", "service_name"),
                    ("metric.name", "metric_name"),
                ],
            }),
            _ => None,
        }
    }
}

/// Scans every table in `source.tables`, unioning them when there's more
/// than one (metrics: gauge + sum). A single-table source's scan keeps its
/// full raw schema unchanged — `SchemaResolver`'s promoted-attribute
/// discovery depends on seeing every column the table actually has, not
/// just `row_defaults` — so the projection-then-union step only runs when
/// there's more than one table to reconcile onto a common schema.
async fn scan_source_tables(
    ctx: &SessionContext,
    tenant_slug: &str,
    dataset_slug: &str,
    source: &SourcePlan,
) -> Result<Option<DataFrame>, QuerierError> {
    let mut providers = Vec::with_capacity(source.tables.len());
    for table in source.tables {
        // A missing table (e.g. no sum metrics ingested yet) is not an
        // error — skip it. A catalog failure still is.
        if let Some(found) = optional_table_provider(ctx, tenant_slug, dataset_slug, table).await? {
            providers.push(found);
        }
    }
    match providers.len() {
        0 => Ok(None),
        1 => {
            let (table_ref, provider) = providers.remove(0);
            Ok(Some(scan_provider(ctx, table_ref, provider)?))
        }
        _ => {
            // Tables created at different times can disagree on a column's
            // physical type — most commonly an attribute container that is
            // a legacy JSON string on one table and a typed
            // `Map<Utf8,Utf8>` on the other. UNION requires identical types
            // per position, so pick one target type per column and coerce
            // each mismatching table's *scan* to it (a wrapping provider,
            // not a projection expression: DataFusion 54's
            // `optimize_projections` mis-orders the pushed-down projections
            // of a UNION whose inputs mix columns and expressions) (#1206).
            let targets = union_target_types(&providers, source.row_defaults);
            let mut union: Option<DataFrame> = None;
            for (table_ref, provider) in providers {
                let provider = CoercedTableProvider::wrap(provider, source.row_defaults, &targets);
                let df = scan_provider(ctx, table_ref, provider)?;
                let proj: Vec<Expr> = source.row_defaults.iter().map(|c| col(*c)).collect();
                let projected = df.select(proj).map_err(QuerierError::QueryFailed)?;
                union = Some(match union {
                    None => projected,
                    Some(existing) => existing
                        .union(projected)
                        .map_err(QuerierError::QueryFailed)?,
                });
            }
            Ok(union)
        }
    }
}

/// The type each unioned column should have: the first `Map` seen when any
/// table stores the column as a map (so legacy JSON-string tables coerce up
/// to the typed form), otherwise the first table's type.
fn union_target_types(
    providers: &[(datafusion::common::TableReference, Arc<dyn TableProvider>)],
    columns: &[&str],
) -> Vec<Option<DataType>> {
    columns
        .iter()
        .map(|c| {
            let types: Vec<DataType> = providers
                .iter()
                .filter_map(|(_, p)| {
                    p.schema()
                        .field_with_name(c)
                        .ok()
                        .map(|f| f.data_type().clone())
                })
                .collect();
            types
                .iter()
                .find(|t| matches!(t, DataType::Map(_, _)))
                .or(types.first())
                .cloned()
        })
        .collect()
}

/// A [`TableProvider`] that presents `inner` with some columns coerced to a
/// different type: a legacy JSON-string attribute container becomes a typed
/// `Map<Utf8,Utf8>` (via [`JsonToMapUdf`]), anything else is cast. Used to
/// give the tables of a multi-table source identical schemas before they are
/// unioned. Filters that only touch un-coerced columns are still offered to
/// the inner provider (so time-range pruning survives); projections and
/// limits pass straight through.
#[derive(Debug)]
struct CoercedTableProvider {
    inner: Arc<dyn TableProvider>,
    schema: SchemaRef,
    /// Column index → target type, for the columns that differ from `inner`.
    coerced: Vec<(usize, DataType)>,
}

impl CoercedTableProvider {
    /// Wrap `inner` if any of `columns` needs coercion to its target type;
    /// return `inner` untouched otherwise.
    fn wrap(
        inner: Arc<dyn TableProvider>,
        columns: &[&str],
        targets: &[Option<DataType>],
    ) -> Arc<dyn TableProvider> {
        let inner_schema = inner.schema();
        let mut coerced = Vec::new();
        for (name, target) in columns.iter().zip(targets) {
            let (Some(target), Ok(idx)) = (target, inner_schema.index_of(name)) else {
                continue;
            };
            if inner_schema.field(idx).data_type() != target {
                coerced.push((idx, target.clone()));
            }
        }
        if coerced.is_empty() {
            return inner;
        }
        let fields: Vec<Field> = inner_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| match coerced.iter().find(|(idx, _)| *idx == i) {
                Some((_, target)) => Field::new(f.name(), target.clone(), true),
                None => f.as_ref().clone(),
            })
            .collect();
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            inner_schema.metadata().clone(),
        ));
        Arc::new(Self {
            inner,
            schema,
            coerced,
        })
    }

    fn is_coerced(&self, idx: usize) -> bool {
        self.coerced.iter().any(|(i, _)| *i == idx)
    }
}

#[async_trait::async_trait]
impl TableProvider for CoercedTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> datafusion::datasource::TableType {
        self.inner.table_type()
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        // A filter over a coerced column must run against the coerced
        // values, i.e. above this provider; everything else may go down.
        let coerced_names: Vec<&str> = self
            .coerced
            .iter()
            .map(|(i, _)| self.schema.field(*i).name().as_str())
            .collect();
        let (down, down_idx): (Vec<&Expr>, Vec<usize>) = filters
            .iter()
            .enumerate()
            .filter(|(_, f)| {
                f.column_refs()
                    .iter()
                    .all(|c| !coerced_names.contains(&c.name.as_str()))
            })
            .map(|(i, f)| (*f, i))
            .unzip();
        let inner = self.inner.supports_filters_pushdown(&down)?;
        let mut out = vec![TableProviderFilterPushDown::Unsupported; filters.len()];
        for (i, support) in down_idx.into_iter().zip(inner) {
            out[i] = support;
        }
        Ok(out)
    }
    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // The inner scan sees the same indices (we never reorder columns)
        // and only the filters that survived `supports_filters_pushdown`.
        let inner_plan = self.inner.scan(state, projection, filters, limit).await?;
        let inner_schema = inner_plan.schema();
        let selected: Vec<usize> = match projection {
            Some(p) => p.clone(),
            None => (0..self.schema.fields().len()).collect(),
        };
        if !selected.iter().any(|i| self.is_coerced(*i)) {
            return Ok(inner_plan);
        }
        let mut exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::with_capacity(selected.len());
        for (out_idx, src_idx) in selected.iter().enumerate() {
            let name = self.schema.field(*src_idx).name().clone();
            let column: Arc<dyn PhysicalExpr> = Arc::new(PhysicalColumn::new(&name, out_idx));
            let expr: Arc<dyn PhysicalExpr> = match self.coerced.iter().find(|(i, _)| i == src_idx)
            {
                None => column,
                Some((_, target)) => {
                    let actual = inner_schema.field(out_idx).data_type();
                    let is_string = matches!(
                        actual,
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                    );
                    if is_string && matches!(target, DataType::Map(_, _)) {
                        let udf = Arc::new(ScalarUDF::from(JsonToMapUdf::new(target.clone())));
                        Arc::new(ScalarFunctionExpr::try_new(
                            udf,
                            vec![column],
                            &inner_schema,
                            Arc::new(state.config_options().clone()),
                        )?)
                    } else {
                        Arc::new(CastExpr::new(column, target.clone(), None))
                    }
                }
            };
            exprs.push((expr, name));
        }
        Ok(Arc::new(ProjectionExec::try_new(exprs, inner_plan)?))
    }
}

/// `ir_json_to_map(Utf8) -> Map<Utf8,Utf8>`: decodes a legacy JSON-string
/// attribute document into the typed map form (non-string JSON values are
/// stringified, as the writer's `json_strings_to_map_array` does), producing
/// exactly the `target` map type so it can sit under a UNION next to a table
/// that already stores the map.
#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonToMapUdf {
    signature: Signature,
    target: DataType,
}

impl JsonToMapUdf {
    fn new(target: DataType) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8]),
                    TypeSignature::Exact(vec![DataType::Utf8View]),
                ],
                Volatility::Immutable,
            ),
            target,
        }
    }
}

impl ScalarUDFImpl for JsonToMapUdf {
    fn name(&self) -> &str {
        "ir_json_to_map"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(self.target.clone())
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{MapBuilder, MapFieldNames};
        let num_rows = args.number_rows;
        let docs = BodyArg::try_from(&args.args[0])?;
        let DataType::Map(entry_field, _) = &self.target else {
            return Err(datafusion::error::DataFusionError::Internal(
                "ir_json_to_map target is not a map".into(),
            ));
        };
        let DataType::Struct(kv) = entry_field.data_type() else {
            return Err(datafusion::error::DataFusionError::Internal(
                "ir_json_to_map map entries are not a struct".into(),
            ));
        };
        let names = MapFieldNames {
            entry: entry_field.name().clone(),
            key: kv[0].name().clone(),
            value: kv[1].name().clone(),
        };
        let mut builder = MapBuilder::new(Some(names), StringBuilder::new(), StringBuilder::new());
        for i in 0..num_rows {
            match docs
                .value_at(i)
                .map(serde_json::from_str::<serde_json::Value>)
            {
                Some(Ok(serde_json::Value::Object(map))) => {
                    for (k, v) in map {
                        builder.keys().append_value(k);
                        match v {
                            serde_json::Value::String(s) => builder.values().append_value(s),
                            other => builder.values().append_value(other.to_string()),
                        }
                    }
                    builder.append(true)?;
                }
                _ => builder.append(false)?,
            }
        }
        let built = builder.finish();
        // MapBuilder fixes its own entry-struct nullability; rebuild against
        // the exact target field so the UNION sees identical types.
        let (_, offsets, entries, nulls, ordered) = built.into_parts();
        let entries = datafusion::arrow::array::StructArray::try_new(
            kv.clone(),
            entries.columns().to_vec(),
            None,
        )?;
        let array = datafusion::arrow::array::MapArray::try_new(
            entry_field.clone(),
            offsets,
            entries,
            nulls,
            ordered,
        )?;
        Ok(ColumnarValue::Array(Arc::new(array)))
    }
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

/// A [`FieldResolver`] whose client-visible built-ins come from the canonical
/// logical schema. The scanned Arrow schema only verifies a logical field's
/// current physical realization and discovers promoted attributes.
struct SchemaResolver {
    columns: HashMap<String, ValueType>,
    physical_names: std::collections::HashSet<String>,
    container: String,
    aliases: &'static [(&'static str, &'static str)],
    source: String,
    logical_schema: LogicalSchema,
}

impl SchemaResolver {
    fn new(schema: &datafusion::common::DFSchema, source: &SourcePlan) -> Self {
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
            physical_names,
            container: source.containers[0].to_string(),
            aliases: source.aliases,
            source: source.name.to_string(),
            logical_schema: LogicalSchema::core(),
        }
    }

    /// Resolve a declared logical field to its current physical realization.
    fn column_for(&self, field: &str, value_type: ValueType) -> Option<(String, ValueType)> {
        if let Some((_, physical)) = self.aliases.iter().find(|(logical, _)| *logical == field)
            && self.columns.contains_key(*physical)
        {
            return Some((physical.to_string(), value_type));
        }
        let materialized = common::schema::materialized_column_name(field);
        if let Some(vt) = self.columns.get(&materialized) {
            return Some((materialized, vt.clone()));
        }
        self.columns
            .contains_key(field)
            .then(|| (field.to_string(), value_type))
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
        if let Some(logical) = self.logical_schema.resolve(&self.source, field) {
            let value_type = logical_to_value_type(logical.value_type);
            return match self.column_for(field, value_type.clone()) {
                Some((name, value_type)) => Some(Resolved::Column { name, value_type }),
                None => Some(Resolved::JsonPath {
                    container: self.container.clone(),
                    key: field.to_string(),
                    value_type,
                }),
            };
        }
        if self.physical_names.contains(field) {
            return None;
        }
        match self.column_for(field, ValueType::String) {
            Some((name, value_type)) => Some(Resolved::Column { name, value_type }),
            // An unpromoted attribute: a String extraction from the container.
            None => Some(Resolved::JsonPath {
                container: self.container.clone(),
                key: field.to_string(),
                value_type: ValueType::String,
            }),
        }
    }

    fn is_known(&self, _source: &str, field: &str) -> bool {
        // Only physical / promoted columns are "known" — the permissive String
        // attribute fallback must not spuriously collide with derived/output
        // names. (Without #811 the resolver cannot enumerate real attributes.)
        self.logical_schema.resolve(&self.source, field).is_some()
            || self
                .columns
                .contains_key(&common::schema::materialized_column_name(field))
    }

    fn is_physical_name(&self, _source: &str, field: &str) -> bool {
        self.physical_names.contains(field)
            && self.logical_schema.resolve(&self.source, field).is_none()
    }

    fn filterability(&self, _source: &str, field: &str) -> Filterability {
        self.logical_schema
            .resolve(&self.source, field)
            .map_or(Filterability::Filterable, |logical| logical.filterability)
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
pub struct IrService {
    session_context: Arc<SessionContext>,
}

impl Clone for IrService {
    fn clone(&self) -> Self {
        Self {
            session_context: Arc::clone(&self.session_context),
        }
    }
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
        }
    }

    /// Execute an IR query ticket, returning the projected RecordBatches. The
    /// resolved window is echoed via the returned [`ResolvedWindow`].
    pub async fn query(
        &self,
        params: &IrQueryParams,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<(Vec<RecordBatch>, ResolvedWindow), QuerierError> {
        use tracing::Instrument;

        let doc: Document = serde_json::from_value(params.document.clone())
            .map_err(|e| QuerierError::InvalidInput(format!("invalid IR document: {e}")))?;
        // Stage spans (INTERNAL) under the Flight SERVER span, so a slow
        // query is attributable to planning vs execution.
        let Some((mut df, window)) = self
            .plan(&doc, tenant_slug, dataset_slug, params.now_ns)
            .instrument(tracing::info_span!("signaldb.query.plan"))
            .await?
        else {
            // No storage for this source in this dataset: no rows, but the
            // window is still resolved so the caller can echo it back.
            return Ok((Vec::new(), resolve_window(&doc, params.now_ns)?));
        };
        // The flamegraph aggregation happens in Rust, not DataFusion (see
        // `plan`'s doc comment on `apply_projection`'s flamegraph carve-out);
        // request one row past the cap so truncation is exact, not a guess.
        if doc.result == ResultEnvelope::Flamegraph {
            df = df
                .limit(0, Some(FLAMEGRAPH_PROFILE_CAP + 1))
                .map_err(QuerierError::QueryFailed)?;
        }
        let exec_span = tracing::info_span!(
            "signaldb.query.execute",
            signaldb.query.rows = tracing::field::Empty,
            signaldb.query.batches = tracing::field::Empty,
        );
        let batches = df
            .collect()
            .instrument(exec_span.clone())
            .await
            .map_err(QuerierError::QueryFailed)?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        exec_span.record("signaldb.query.rows", rows as i64);
        exec_span.record("signaldb.query.batches", batches.len() as i64);
        if doc.result == ResultEnvelope::Flamegraph {
            return Ok((
                vec![encode_flamegraph_batch(&batches, FLAMEGRAPH_PROFILE_CAP)?],
                window,
            ));
        }
        Ok((batches, window))
    }

    /// Build the `DataFrame` for a document (split out for planner tests).
    pub async fn plan(
        &self,
        doc: &Document,
        tenant_slug: &str,
        dataset_slug: &str,
        now_ns: i64,
    ) -> Result<Option<(DataFrame, ResolvedWindow)>, QuerierError> {
        let source = SourcePlan::for_source(&doc.from)
            .ok_or_else(|| QuerierError::InvalidInput(format!("unknown source '{}'", doc.from)))?;

        // A dataset with none of this source's tables has no rows to plan
        // over. The document's schema-dependent validation is skipped along
        // with the scan — there is no schema to validate against.
        let Some(base) =
            scan_source_tables(&self.session_context, tenant_slug, dataset_slug, &source).await?
        else {
            return Ok(None);
        };

        // Build the resolver from the actual scanned schema and validate the
        // document against it (envelope, coercibility, references, guards).
        let resolver = SchemaResolver::new(base.schema(), &source);
        validate(doc, &SourceRegistry::core(), &resolver)
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
            col_of: HashMap::new(),
            derived_types: HashMap::new(),
            schema_cols: base
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect(),
        };

        let mut df = lowering.apply_time_window(base, &window)?;
        for stage in &doc.pipeline {
            df = match stage {
                // The only stage that needs to break the lazy DataFrame chain
                // (merging bucket-array columns element-wise isn't expressible
                // as a DataFusion aggregate) — see lower_histogram_quantile.
                Stage::HistogramQuantile(hq) => {
                    lowering
                        .lower_histogram_quantile(&self.session_context, df, hq)
                        .await?
                }
                other => lowering.lower_stage(df, other)?,
            };
        }
        df = lowering.apply_projection(df, doc)?;
        Ok(Some((df, window)))
    }
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
    let mut profiles: Vec<_> = batches.iter().flat_map(batch_to_models).collect();
    let truncated = profiles.len() > cap;
    profiles.truncate(cap);

    let flamegraph = aggregate_profiles_to_flamegraph(&profiles);
    let flamegraph_json = serde_json::to_string(&flamegraph).map_err(|e| {
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

/// Resolve the document's range to an absolute window.
fn resolve_window(doc: &Document, now_ns: i64) -> Result<ResolvedWindow, QuerierError> {
    let start = resolve_instant(&doc.range.from, now_ns)?;
    let end = resolve_instant(&doc.range.to, now_ns)?;
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

struct Lowering<'a> {
    source: &'a SourcePlan,
    resolver: &'a SchemaResolver,
    now_ns: i64,
    aggregated: bool,
    series_shaped: bool,
    /// Logical name → current DataFrame column name (extract-derived and
    /// post-aggregate output columns).
    col_of: HashMap<String, String>,
    /// Declared types of extract-derived fields, for literal coercion.
    derived_types: HashMap<String, ValueType>,
    /// The current base-table physical column names.
    schema_cols: Vec<String>,
}

impl Lowering<'_> {
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

    fn lower_stage(&mut self, df: DataFrame, stage: &Stage) -> Result<DataFrame, QuerierError> {
        match stage {
            Stage::Where(pred) => {
                let expr = self.lower_predicate(pred)?;
                df.filter(expr).map_err(QuerierError::QueryFailed)
            }
            Stage::Aggregate(agg) => self.lower_aggregate(df, agg),
            Stage::Topk(rank) => self.lower_rank(df, &rank.of, rank.n, false),
            Stage::Bottomk(rank) => self.lower_rank(df, &rank.of, rank.n, true),
            Stage::Order(keys) => {
                let sort: Vec<_> = keys
                    .iter()
                    .map(|k| {
                        let ascending = matches!(k.dir, common::query_ir::Direction::Asc);
                        col(self.df_col(&k.of)).sort(ascending, true)
                    })
                    .collect();
                df.sort(sort).map_err(QuerierError::QueryFailed)
            }
            Stage::Limit(n) => df
                .limit(0, Some(*n as usize))
                .map_err(QuerierError::QueryFailed),
            Stage::Extract(extract) => self.lower_extract(df, extract),
            Stage::Heatmap(heatmap) => self.lower_heatmap(df, heatmap),
            // Handled directly in `plan()`'s stage loop (needs an async
            // `.collect()` this sync method can't perform) — never reached.
            Stage::HistogramQuantile(_) => Err(QuerierError::InvalidInput(
                "histogram_quantile requires async lowering".into(),
            )),
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
        let mut df = df;
        for f in &extract.as_fields {
            let raw = udf.call(vec![col("body"), lit(parser), lit(f.name.clone())]);
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
    fn df_col(&self, logical: &str) -> String {
        self.col_of.get(logical).cloned().unwrap_or_else(|| {
            match self.resolver.resolve("", logical) {
                Some(Resolved::Column { name, .. }) => name,
                _ => safe_ident(logical),
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
        df.sort(vec![col(self.df_col(of)).sort(ascending, false)])
            .map_err(QuerierError::QueryFailed)?
            .limit(0, Some(n.max(0) as usize))
            .map_err(QuerierError::QueryFailed)
    }

    fn lower_aggregate(
        &mut self,
        df: DataFrame,
        agg: &Aggregate,
    ) -> Result<DataFrame, QuerierError> {
        // Group expressions: each `by` field, aliased to a safe identifier.
        let mut group_exprs = Vec::new();
        let mut new_col_of = HashMap::new();
        if let Some(step) = &agg.step {
            let step_ns = common::query_ir::parse_duration_ns(step).ok_or_else(|| {
                QuerierError::InvalidInput(format!("invalid step duration '{step}'"))
            })?;
            let stride = lit(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNano::new(0, 0, step_ns),
            )));
            let origin = lit(ScalarValue::TimestampNanosecond(Some(0), None));
            let ts_ns = cast(
                col(self.source.time_col),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            );
            group_exprs.push(date_bin(stride, ts_ns, origin).alias("bucket"));
        }
        for by in &agg.by {
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
                sort.push(col(safe_ident(by)).sort(true, false));
            }
            return df.sort(sort).map_err(QuerierError::QueryFailed);
        }
        Ok(df)
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

    /// Lower a `histogram_quantile` stage. Unlike every other stage, this
    /// breaks the lazy DataFrame chain: merging `bucket_counts`/
    /// `explicit_bounds` element-wise across grouped rows (and, in rate mode,
    /// tracking each group's first/last-by-timestamp counts) isn't expressible
    /// as a DataFusion aggregate the way `count()`/`sum()` are. Instead this
    /// collects the filtered, time-bucketed rows (the same shape
    /// `metrics.rs`'s PromQL `histogram_query` already collects) and reuses
    /// its exact bucket-merge/interpolation functions
    /// ([`HistogramAcc`]/[`RateHistAcc`]/[`histogram_quantile`]) so the two
    /// surfaces compute identical percentiles from identical data — then
    /// reinjects the small, already-aggregated result as a fresh in-memory
    /// DataFrame shaped exactly like `lower_aggregate`'s `step` output
    /// (`bucket`, label columns, one value column), so every downstream stage
    /// and the response builder need no histogram-specific handling.
    async fn lower_histogram_quantile(
        &mut self,
        ctx: &SessionContext,
        df: DataFrame,
        hq: &HistogramQuantile,
    ) -> Result<DataFrame, QuerierError> {
        let step_ns = common::query_ir::parse_duration_ns(&hq.step).ok_or_else(|| {
            QuerierError::InvalidInput(format!("invalid histogram_quantile step '{}'", hq.step))
        })?;
        let stride = lit(ScalarValue::IntervalMonthDayNano(Some(
            IntervalMonthDayNano::new(0, 0, step_ns),
        )));
        let origin = lit(ScalarValue::TimestampNanosecond(Some(0), None));
        let ts_ns = cast(
            col(self.source.time_col),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        );
        // Every column the Rust-side row walk parses gets an explicit Utf8
        // cast: a Parquet-backed scan under DataFusion 54 can yield `Utf8View`
        // for string columns (a zero-copy optimization), which `downcast_string`
        // below — deliberately narrow, see its doc comment — would otherwise
        // reject. `by` aliases already go through `value_expr` + this same cast.
        let utf8 = |e: Expr| cast(e, DataType::Utf8);
        let mut projection = vec![
            date_bin(stride, ts_ns, origin).alias("bucket"),
            utf8(col("metric_name")).alias("metric_name"),
            // The raw-series discriminator for rate-mode delta tracking,
            // independent of `by` — see the comment on `RawKey` below.
            utf8(col("service_name")).alias("__raw_service"),
        ];
        let by_aliases: Vec<String> = hq.by.iter().map(|by| safe_ident(by)).collect();
        for (by, alias) in hq.by.iter().zip(&by_aliases) {
            projection.push(utf8(self.value_expr(by)?).alias(alias.clone()));
        }
        projection.push(utf8(col("bucket_counts")).alias("bucket_counts"));
        projection.push(utf8(col("explicit_bounds")).alias("explicit_bounds"));
        projection.push(
            cast(
                col(self.source.time_col),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            )
            .alias("__ts"),
        );

        let batches = df
            .select(projection)
            .map_err(QuerierError::QueryFailed)?
            .collect()
            .await
            .map_err(QuerierError::QueryFailed)?;

        // The final output grouping: bucket, metric, and the query's
        // requested `by` labels. A `None` element is a genuinely absent
        // label, kept distinct from an empty-string value.
        type GroupKey = (i64, String, Vec<Option<String>>);
        // The raw-series identity used ONLY for rate-mode delta tracking:
        // (bucket, metric, service_name) — the same granularity the PromQL
        // `histogram_quantile(phi, rate(metric[range]))` surface already uses
        // (it has no way to group any finer). Computing each raw series'
        // first/last-by-timestamp delta independently, THEN summing those
        // deltas into the requested `by` groups, is what makes rate mode
        // correct when `by` differs from `["service.name"]`: merging points
        // from two different services into one first/last pair before
        // computing a delta would pair up unrelated observations.
        type RawKey = (i64, String, String);
        let rate_mode = matches!(hq.mode, HistogramMode::Rate);
        let mut merged: std::collections::BTreeMap<GroupKey, HistogramAcc> =
            std::collections::BTreeMap::new();
        let mut rated: std::collections::BTreeMap<RawKey, (RateHistAcc, Vec<Option<String>>)> =
            std::collections::BTreeMap::new();
        // `explicit_bounds` is typically identical across every row of a
        // series — cache its parse (see histogram::parse_bounds_cached).
        let mut bounds_cache: HashMap<String, Vec<f64>> = HashMap::new();

        for batch in &batches {
            let bucket = batch
                .column_by_name("bucket")
                .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
                .ok_or_else(|| {
                    QuerierError::InvalidInput("bucket column is not a timestamp".to_string())
                })?;
            let metric = downcast_string(batch, "metric_name")?;
            let raw_service = downcast_string(batch, "__raw_service")?;
            let by_cols: Vec<&StringArray> = by_aliases
                .iter()
                .map(|alias| downcast_string(batch, alias))
                .collect::<Result<_, _>>()?;
            let counts = downcast_string(batch, "bucket_counts")?;
            let bounds = downcast_string(batch, "explicit_bounds")?;
            let ts = batch
                .column_by_name("__ts")
                .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
                .ok_or_else(|| {
                    QuerierError::InvalidInput("__ts column is not a timestamp".to_string())
                })?;

            for i in 0..batch.num_rows() {
                if bucket.is_null(i) || counts.is_null(i) || bounds.is_null(i) {
                    continue;
                }
                let (Some(row_counts), Some(row_bounds)) = (
                    parse_f64_array(counts.value(i)),
                    parse_bounds_cached(&mut bounds_cache, bounds.value(i)),
                ) else {
                    continue;
                };
                // OTLP invariant: one more bucket count than bound.
                if row_counts.len() != row_bounds.len() + 1 || row_bounds.is_empty() {
                    continue;
                }
                let by_values: Vec<Option<String>> = by_cols
                    .iter()
                    .map(|c| (!c.is_null(i)).then(|| c.value(i).to_string()))
                    .collect();
                let bucket_ns = bucket.value(i);
                let metric_name = metric.value(i).to_string();
                if rate_mode {
                    let t = if ts.is_null(i) { 0 } else { ts.value(i) };
                    let raw_key = (
                        bucket_ns,
                        metric_name,
                        if raw_service.is_null(i) {
                            String::new()
                        } else {
                            raw_service.value(i).to_string()
                        },
                    );
                    rated
                        .entry(raw_key)
                        .or_insert_with(|| {
                            (
                                RateHistAcc::new(row_bounds, t, row_counts.clone()),
                                by_values,
                            )
                        })
                        .0
                        .observe(t, &row_counts);
                } else {
                    let key = (bucket_ns, metric_name, by_values);
                    merged
                        .entry(key)
                        .or_insert_with(|| HistogramAcc::new(row_bounds, row_counts.len()))
                        .merge(&row_counts);
                }
            }
        }

        // Rate mode: sum each raw series' independently-computed delta into
        // its requested `by` group — the second half of the two-pass
        // approach described above.
        if rate_mode {
            for ((bucket_ns, metric, _raw_service), (acc, by_values)) in rated {
                let delta = acc.delta();
                let key = (bucket_ns, metric, by_values);
                merged
                    .entry(key)
                    .or_insert_with(|| HistogramAcc::new(acc.bounds.clone(), delta.len()))
                    .merge(&delta);
            }
        }

        let groups: Vec<(GroupKey, Vec<f64>, Vec<f64>)> = merged
            .into_iter()
            .map(|(k, acc)| (k, acc.bounds, acc.counts))
            .collect();

        let mut bucket_out = Vec::with_capacity(groups.len());
        let mut metric_out = Vec::with_capacity(groups.len());
        let mut by_out: Vec<Vec<Option<String>>> =
            vec![Vec::with_capacity(groups.len()); hq.by.len()];
        let mut value_out = Vec::with_capacity(groups.len());
        for ((bucket_ns, metric, by_values), bounds, counts) in groups {
            let q = histogram_quantile(hq.q, &bounds, &counts);
            bucket_out.push(bucket_ns);
            metric_out.push(metric);
            for (idx, v) in by_values.into_iter().enumerate() {
                by_out[idx].push(v);
            }
            value_out.push(q);
        }

        let mut fields = vec![
            Field::new(
                "bucket",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("metric_name", DataType::Utf8, false),
        ];
        let mut arrays: Vec<datafusion::arrow::array::ArrayRef> = vec![
            Arc::new(TimestampNanosecondArray::from(bucket_out)),
            Arc::new(StringArray::from(metric_out)),
        ];
        for (alias, values) in by_aliases.iter().zip(by_out) {
            fields.push(Field::new(alias, DataType::Utf8, true));
            arrays.push(Arc::new(StringArray::from(values)));
        }
        fields.push(Field::new(&hq.as_name, DataType::Float64, true));
        arrays.push(Arc::new(Float64Array::from(value_out)));

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|e| QuerierError::QueryFailed(e.into()))?;
        let new_df = ctx.read_batch(batch).map_err(QuerierError::QueryFailed)?;

        self.aggregated = true;
        self.series_shaped = true;
        let mut new_col_of = HashMap::new();
        new_col_of.insert("metric.name".to_string(), "metric_name".to_string());
        for (by, alias) in hq.by.iter().zip(&by_aliases) {
            new_col_of.insert(by.clone(), alias.clone());
        }
        new_col_of.insert(hq.as_name.clone(), hq.as_name.clone());
        self.col_of = new_col_of;

        let mut sort = vec![
            col("bucket").sort(true, false),
            col("metric_name").sort(true, false),
        ];
        for alias in &by_aliases {
            sort.push(col(alias.clone()).sort(true, false));
        }
        new_df.sort(sort).map_err(QuerierError::QueryFailed)
    }

    fn agg_expr(&self, a: &common::query_ir::Agg) -> Result<Expr, QuerierError> {
        use common::query_ir::AggFn;
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
        };
        // A scoping predicate narrows this aggregate alone, as a per-aggregate
        // FILTER on the one grouping — not a `Filter` node, which would narrow
        // every aggregate in the stage and drop groups with no matching row.
        // The scope lowers through `lower_predicate`, so it inherits the same
        // Kleene absent-value semantics a `where` stage has: a row whose field
        // is NULL satisfies neither the scope nor its negation.
        match &a.scope {
            None => Ok(expr),
            Some(scope) => expr
                .filter(self.lower_predicate(scope)?)
                .build()
                .map_err(QuerierError::QueryFailed),
        }
    }

    /// The `of` field of an aggregate as a numeric (Float64) expression.
    fn numeric_of(&self, a: &common::query_ir::Agg) -> Result<Expr, QuerierError> {
        let of = a.of.as_deref().ok_or_else(|| {
            QuerierError::InvalidInput(format!("aggregate '{}' requires a field", a.func.as_str()))
        })?;
        Ok(cast(self.value_expr(of)?, DataType::Float64))
    }

    /// Lower a logical field to the expression that reads its value.
    fn value_expr(&self, logical: &str) -> Result<Expr, QuerierError> {
        // Post-aggregate references address the current DataFrame column.
        if let Some(c) = self.col_of.get(logical) {
            return Ok(col(c.clone()));
        }
        match self.resolver.resolve("", logical) {
            Some(Resolved::Column { name, .. }) => Ok(col(name)),
            Some(Resolved::JsonPath { key, .. }) => Ok(self.attr_expr(&key)),
            Some(Resolved::EventAttribute {
                events_column,
                event_name,
                key,
                ..
            }) => Ok(self.event_attr_expr(&events_column, &event_name, &key)),
            None => Err(QuerierError::InvalidInput(format!(
                "field '{logical}' has no canonical type"
            ))),
        }
    }

    /// Extract an attribute value, coalescing over the source's containers that
    /// are present in the scanned schema.
    fn attr_expr(&self, key: &str) -> Expr {
        // An explicit container qualifier reads that container only. Checked
        // before the coalesce so `resource.x` and `log.x` stay distinguishable
        // when the same key exists at both scopes.
        if let Some((container, bare)) = self.qualified_attr(key) {
            return if self.schema_cols.iter().any(|s| s == container) {
                get_field(col(container), bare)
            } else {
                lit(ScalarValue::Utf8(None))
            };
        }
        let mut parts: Vec<Expr> = self
            .source
            .containers
            .iter()
            .filter(|c| self.schema_cols.iter().any(|s| s == *c))
            .map(|c| get_field(col(*c), key))
            .collect();
        match parts.len() {
            0 => lit(ScalarValue::Utf8(None)),
            1 => parts.remove(0),
            _ => coalesce(parts),
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

    /// Split a container-qualified field into `(container column, bare key)`.
    /// Returns `None` for an unqualified name, which coalesces instead.
    fn qualified_attr<'f>(&self, field: &'f str) -> Option<(&'static str, &'f str)> {
        self.source
            .attr_prefixes
            .iter()
            .find_map(|(prefix, container)| {
                field
                    .strip_prefix(prefix)
                    // A bare prefix with nothing after it is not a field.
                    .filter(|rest| !rest.is_empty())
                    .map(|rest| (*container, rest))
            })
    }

    fn lower_predicate(&self, pred: &Predicate) -> Result<Expr, QuerierError> {
        match pred {
            Predicate::Leaf(leaf) => self.lower_leaf(leaf),
            Predicate::Not(p) => Ok(not(self.lower_predicate(p)?)),
            Predicate::And(preds) => {
                let mut acc = lit(true);
                for (i, p) in preds.iter().enumerate() {
                    let e = self.lower_predicate(p)?;
                    acc = if i == 0 { e } else { acc.and(e) };
                }
                Ok(acc)
            }
            Predicate::Or(preds) => {
                let mut acc = lit(false);
                for (i, p) in preds.iter().enumerate() {
                    let e = self.lower_predicate(p)?;
                    acc = if i == 0 { e } else { acc.or(e) };
                }
                Ok(acc)
            }
        }
    }

    fn lower_leaf(&self, leaf: &Leaf) -> Result<Expr, QuerierError> {
        // An extract-derived or aggregate-output column takes precedence over
        // registry resolution (it is a real DataFrame column now).
        let (is_json, value_type, field_expr) = if let Some(alias) = self.col_of.get(&leaf.field) {
            let ty = self
                .derived_types
                .get(&leaf.field)
                .cloned()
                .unwrap_or(ValueType::String);
            (false, ty, col(alias.clone()))
        } else {
            let resolved = self.resolver.resolve("", &leaf.field).ok_or_else(|| {
                QuerierError::InvalidInput(format!("unknown field '{}'", leaf.field))
            })?;
            let is_json = matches!(
                resolved,
                Resolved::JsonPath { .. } | Resolved::EventAttribute { .. }
            );
            let ty = resolved.value_type().clone();
            let expr = match &resolved {
                Resolved::Column { name, .. } => col(name.clone()),
                Resolved::JsonPath { key, .. } => self.attr_expr(key),
                Resolved::EventAttribute {
                    events_column,
                    event_name,
                    key,
                    ..
                } => self.event_attr_expr(events_column, event_name, key),
            };
            (is_json, ty, expr)
        };

        let coerce_val = |v: &serde_json::Value, ty: &ValueType| -> Result<Literal, QuerierError> {
            coerce(v, ty).map_err(|e| QuerierError::InvalidInput(e.to_string()))
        };

        Ok(match leaf.op {
            ComparisonOp::Exists => field_expr.is_not_null(),
            ComparisonOp::Eq => {
                let v = self.require_value(leaf)?;
                field_expr.eq(self.value_lit(&coerce_val(v, &value_type)?, is_json))
            }
            ComparisonOp::Ne => {
                let v = self.require_value(leaf)?;
                field_expr.not_eq(self.value_lit(&coerce_val(v, &value_type)?, is_json))
            }
            ComparisonOp::Gt | ComparisonOp::Gte | ComparisonOp::Lt | ComparisonOp::Lte => {
                let v = self.require_value(leaf)?;
                self.ordered(field_expr, leaf.op, v, &value_type, is_json)?
            }
            ComparisonOp::Contains => {
                let v = self.require_value(leaf)?;
                let s = coerce_val(v, &ValueType::String)?;
                contains(field_expr, self.value_lit(&s, true))
            }
            ComparisonOp::Regex => {
                let v = self.require_value(leaf)?;
                let s = string_of(&coerce_val(v, &ValueType::String)?);
                compile_regex_guard(&s)?;
                regexp_like(field_expr, lit(s), None)
            }
            ComparisonOp::In => {
                let arr = leaf
                    .value
                    .as_ref()
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| QuerierError::InvalidInput("`in` needs an array".to_string()))?;
                let list = arr
                    .iter()
                    .map(|item| Ok(self.value_lit(&coerce_val(item, &value_type)?, is_json)))
                    .collect::<Result<Vec<_>, QuerierError>>()?;
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
                field_expr.clone().gt_eq(lo).and(field_expr.lt_eq(hi))
            }
        })
    }

    fn require_value<'v>(&self, leaf: &'v Leaf) -> Result<&'v serde_json::Value, QuerierError> {
        leaf.value.as_ref().ok_or_else(|| {
            QuerierError::InvalidInput(format!("operator '{}' requires a value", leaf.op.as_str()))
        })
    }

    fn ordered(
        &self,
        field_expr: Expr,
        op: ComparisonOp,
        value: &serde_json::Value,
        value_type: &ValueType,
        is_json: bool,
    ) -> Result<Expr, QuerierError> {
        // Route on the resolved ValueType, not the storage form, so a field
        // compares the same whether promoted (typed column) or unpromoted
        // (Utf8 attribute extraction) — promotion invariance. A numeric type
        // compares numerically in both cases (an attribute's Utf8 value is cast
        // to Float64); a string type compares lexically in both.
        let literal =
            coerce(value, value_type).map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
        let (lhs, rhs) = if is_numeric(value_type) {
            if is_json {
                (
                    cast(field_expr, DataType::Float64),
                    lit(literal_as_f64(&literal)),
                )
            } else {
                (field_expr, self.value_lit(&literal, false))
            }
        } else {
            (field_expr, self.value_lit(&literal, is_json))
        };
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

    fn apply_projection(&self, df: DataFrame, doc: &Document) -> Result<DataFrame, QuerierError> {
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
        let projection: Vec<Expr> = match &doc.fields {
            Some(fields) => fields
                .iter()
                .map(|f| {
                    if self.aggregated || self.col_of.contains_key(f) {
                        // Aggregate output or extract-derived column.
                        col(self.df_col(f))
                    } else {
                        match self.resolver.resolve("", f) {
                            Some(Resolved::Column { name, .. }) => col(name),
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
                            None => col(safe_ident(f)),
                        }
                    }
                })
                .collect(),
            None if self.aggregated => {
                // A `table` default is the (already-curated) aggregate output.
                return Ok(df);
            }
            None => self
                .source
                .row_defaults
                .iter()
                .filter(|c| self.schema_cols.iter().any(|s| s == *c))
                .map(|c| col(*c))
                .collect(),
        };
        df.select(projection).map_err(QuerierError::QueryFailed)
    }
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
            let field = v.get(key)?;
            Some(match field {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Null => return None,
                other => other.to_string(),
            })
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
    match event.attributes.get(key)? {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Null => None,
        other => Some(other.to_string()),
    }
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

/// Downcast a named `RecordBatch` column to `StringArray`, or a clear error —
/// used by `lower_histogram_quantile`'s post-collect row walk.
fn downcast_string<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a StringArray, QuerierError> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| QuerierError::InvalidInput(format!("{name} column is not a string")))
}

/// Sanitize a logical name into a safe DataFrame column identifier (no dots,
/// which DataFusion would read as a table qualifier).
fn safe_ident(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
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
fn compile_regex_guard(pattern: &str) -> Result<(), QuerierError> {
    const SIZE_LIMIT: usize = 1 << 20;
    regex::RegexBuilder::new(pattern)
        .size_limit(SIZE_LIMIT)
        .dfa_size_limit(SIZE_LIMIT)
        .build()
        .map(|_| ())
        .map_err(|e| QuerierError::InvalidInput(format!("invalid or oversized regex: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        ArrayRef, Float64Array, Int64Array, MapBuilder, MapFieldNames, StringArray, StringBuilder,
        TimestampNanosecondArray,
    };
    use datafusion::arrow::datatypes::{Field, Fields, Schema};
    use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
    use datafusion::catalog::{CatalogProvider, MemTable, SchemaProvider};
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

    /// A logs table with a promoted `severity_number` column, a `label_env`
    /// materialized column, and a `log_attributes` map.
    fn logs_ctx() -> SessionContext {
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
                Arc::new(flags),
                Arc::new(scope_name),
                Arc::new(scope_version),
                log_attrs,
                res_attrs,
                scope_attrs,
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
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("profiles".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    /// Two metrics tables (gauge + sum), each shaped like the real
    /// persisted schema's common columns (`timestamp`/`service_name`/
    /// `metric_name`/`value`/`attributes`/`resource_attributes`) — exercises
    /// `scan_source_tables`'s union path, unlike every other fixture here
    /// (one table each).
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

        let ctx = SessionContext::new();
        let gauge = MemTable::try_new(schema.clone(), vec![vec![gauge_batch]]).unwrap();
        let sum = MemTable::try_new(schema, vec![vec![sum_batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("metrics_gauge".to_string(), Arc::new(gauge))
            .unwrap();
        sp.register_table("metrics_sum".to_string(), Arc::new(sum))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    /// A `metrics_histogram` table with rows shaped for each
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
    fn metrics_histogram_ctx() -> SessionContext {
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

        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("metrics_histogram".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    fn doc(v: serde_json::Value) -> Document {
        serde_json::from_value(v).unwrap()
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

    /// The metrics source unions metrics_gauge + metrics_sum (2 + 1 rows
    /// here) rather than scanning one table — the case every other source's
    /// fixture doesn't exercise.
    #[tokio::test]
    async fn metrics_unions_gauge_and_sum_filters_by_name_and_aggregates() {
        let svc = IrService::new(metrics_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "metrics", "range": { "from": 0, "to": 1000 },
            "result": "series",
            "pipeline": [
                { "where": { "field": "metric.name", "op": "eq", "value": "signaldb.wal.entries_processed" } },
                { "aggregate": { "by": ["service.name"], "aggs": [{ "fn": "sum", "of": "metric.value", "as": "v" }], "step": "1ms" } }
            ]
        }));
        let (df, window) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("both metrics tables are registered");
        assert_eq!(
            window,
            ResolvedWindow {
                start_ns: 0,
                end_ns: 1000
            }
        );
        let plan = format!("{}", df.logical_plan().display_indent());
        assert!(plan.contains("Union"), "plan:\n{plan}");
        assert!(plan.contains("Aggregate"), "plan:\n{plan}");
        assert!(plan.contains("Filter"), "plan:\n{plan}");
        assert!(plan.contains("date_bin"), "plan:\n{plan}");
        // Executes, and sums across both tables: 5 + 7 (gauge) + 3 (sum) = 15.
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

    /// Grouping the gauge+sum union by a resource attribute that is *not* an
    /// explicitly registered logical field (`host.name` resolves through the
    /// generic `resource.`/map fallback) used to fail in the optimizer with
    /// "UNION field 0 have different type in inputs" (#1206). The identical
    /// document works on single-table sources; the union must too.
    #[tokio::test]
    async fn metrics_union_groups_by_a_fallback_resource_attribute() {
        let svc = IrService::new(metrics_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "metrics", "range": { "from": 0, "to": 1000 },
            "result": "table",
            "pipeline": [
                { "aggregate": { "by": ["host.name"], "aggs": [{ "fn": "count", "as": "n" }] } },
                { "limit": 501 }
            ]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("both metrics tables are registered");
        let plan = format!("{}", df.logical_plan().display_indent());
        assert!(plan.contains("Union"), "plan:\n{plan}");
        let batches = df
            .collect()
            .await
            .expect("union grouped by a fallback attribute executes");
        let n: i64 = batches
            .iter()
            .map(|b| {
                b.column_by_name("n")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .sum::<i64>()
            })
            .sum();
        // Every row of both tables lands in the one (null host.name) group.
        assert_eq!(n, 3);
    }

    /// Same document as above, but the two tables disagree on the attribute
    /// container type — `metrics_gauge` created after the typed-attribute
    /// change (Map), `metrics_sum` a legacy table (JSON string) — which is
    /// what a long-lived deployment actually has (hive's `_system`). This is
    /// the shape that produced the optimizer's UNION type mismatch.
    #[tokio::test]
    async fn metrics_union_groups_by_a_fallback_attribute_across_mixed_container_types() {
        let map_schema = Arc::new(Schema::new(vec![
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
        let json_schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            Field::new("attributes", DataType::Utf8, true),
            Field::new("resource_attributes", DataType::Utf8, true),
        ]));
        let gauge_batch = RecordBatch::try_new(
            map_schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![10_i64, 20])),
                Arc::new(StringArray::from(vec!["signaldb", "signaldb"])),
                Arc::new(StringArray::from(vec!["m", "m"])),
                Arc::new(Float64Array::from(vec![5.0, 7.0])),
                build_map(&[&[], &[]]),
                build_map(&[&[("host.name", "hive")], &[("host.name", "hive")]]),
            ],
        )
        .unwrap();
        let sum_batch = RecordBatch::try_new(
            json_schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![15_i64])),
                Arc::new(StringArray::from(vec!["signaldb"])),
                Arc::new(StringArray::from(vec!["m"])),
                Arc::new(Float64Array::from(vec![3.0])),
                Arc::new(StringArray::from(vec![Some("{}")])),
                Arc::new(StringArray::from(vec![Some(r#"{"host.name":"other"}"#)])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let gauge = MemTable::try_new(map_schema, vec![vec![gauge_batch]]).unwrap();
        let sum = MemTable::try_new(json_schema, vec![vec![sum_batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("metrics_gauge".to_string(), Arc::new(gauge))
            .unwrap();
        sp.register_table("metrics_sum".to_string(), Arc::new(sum))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);

        let svc = IrService::new(ctx);
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "metrics", "range": { "from": 0, "to": 1000 },
            "result": "table",
            "pipeline": [
                { "aggregate": { "by": ["host.name"], "aggs": [{ "fn": "count", "as": "n" }] } },
                { "limit": 501 }
            ]
        }));
        let (df, _) = svc
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .expect("both metrics tables are registered");
        let batches = df
            .collect()
            .await
            .expect("mixed-container union grouped by a fallback attribute executes");
        // hive: 2 rows, other: 1 row.
        let mut groups: Vec<(String, i64)> = Vec::new();
        for b in &batches {
            let keys = b
                .column_by_name("host_name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let ns = b
                .column_by_name("n")
                .unwrap()
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                groups.push((keys.value(i).to_string(), ns.value(i)));
            }
        }
        groups.sort();
        assert_eq!(groups, vec![("hive".into(), 2), ("other".into(), 1)]);
    }

    /// A metrics document still executes when only one of the two tables
    /// exists — e.g. a dataset that has only ever received gauge metrics.
    #[tokio::test]
    async fn metrics_scans_whichever_table_exists_when_only_one_is_registered() {
        let ctx = SessionContext::new();
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
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![10_i64])),
                Arc::new(StringArray::from(vec!["signaldb"])),
                Arc::new(StringArray::from(vec!["signaldb.wal.entries_processed"])),
                Arc::new(Float64Array::from(vec![5.0])),
                build_map(&[&[]]),
                build_map(&[&[]]),
            ],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("metrics_gauge".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);

        let svc = IrService::new(ctx);
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "metrics", "range": { "from": 0, "to": 1000 },
            "result": "table",
            "pipeline": [{ "aggregate": { "by": ["metric.name"], "aggs": [{ "fn": "sum", "of": "metric.value", "as": "v" }] } }]
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        assert_eq!(
            df.collect()
                .await
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>(),
            1
        );
    }

    fn histogram_value(batches: &[RecordBatch], as_name: &str, label: Option<&str>) -> Vec<f64> {
        let mut out = Vec::new();
        for b in batches {
            let values = b
                .column_by_name(as_name)
                .unwrap()
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Float64Array>()
                .unwrap();
            match label {
                None => out.extend(values.iter().map(|v| v.unwrap_or(f64::NAN))),
                Some(want) => {
                    let labels = b
                        .column_by_name("service_name")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    for i in 0..b.num_rows() {
                        if labels.value(i) == want {
                            out.push(values.value(i));
                        }
                    }
                }
            }
        }
        out
    }

    #[tokio::test]
    async fn histogram_quantile_instant_mode_merges_and_groups_by_service() {
        let svc = IrService::new(metrics_histogram_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 3, "from": "metrics_histogram", "range": { "from": 0, "to": 1000 },
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
            assert!((vs[0] - 0.3).abs() < 1e-9, "{label}: got {:?}", vs[0]);
        }
    }

    #[tokio::test]
    async fn histogram_quantile_rate_mode_clamps_counter_reset_and_inf_overflow() {
        let svc = IrService::new(metrics_histogram_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 3, "from": "metrics_histogram", "range": { "from": 0, "to": 1000 },
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
        // first=[5,0] last=[3,2]: bucket 0 decreases (reset, clamped to 0),
        // delta=[0,2] → rank lands in the +Inf bucket → clamps to bound 1.0.
        assert!((vs[0] - 1.0).abs() < 1e-9, "got {:?}", vs[0]);
    }

    /// Regression: rate mode must compute each raw series' delta
    /// independently before merging into the requested `by` group — two
    /// interleaved services under an empty `by` must not have their points
    /// paired into a single, meaningless first/last delta.
    #[tokio::test]
    async fn histogram_quantile_rate_mode_sums_per_series_deltas_across_an_empty_by_group() {
        let svc = IrService::new(metrics_histogram_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 3, "from": "metrics_histogram", "range": { "from": 0, "to": 1000 },
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
    async fn histogram_quantile_single_point_in_rate_mode_is_nan() {
        let svc = IrService::new(metrics_histogram_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 3, "from": "metrics_histogram", "range": { "from": 0, "to": 1000 },
            "result": "series",
            "pipeline": [
                { "where": { "field": "metric.name", "op": "eq", "value": "solo" } },
                { "histogram_quantile": { "q": 0.5, "step": "1000ms", "mode": "rate", "as": "p50" } }
            ]
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let batches = df.collect().await.unwrap();
        let vs = histogram_value(&batches, "p50", None);
        assert_eq!(vs.len(), 1);
        assert!(vs[0].is_nan(), "got {:?}", vs[0]);
    }

    #[tokio::test]
    async fn histogram_quantile_all_zero_buckets_in_instant_mode_is_nan() {
        let svc = IrService::new(metrics_histogram_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 3, "from": "metrics_histogram", "range": { "from": 0, "to": 1000 },
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
        let svc = IrService::new(metrics_histogram_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 3, "from": "metrics_histogram", "range": { "from": 0, "to": 1000 },
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
        let svc = IrService::new(metrics_histogram_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 3, "from": "metrics_histogram", "range": { "from": 0, "to": 1000 },
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

    /// A Parquet-backed scan under DataFusion 54 can yield `Utf8View` for
    /// string columns (a zero-copy optimization) rather than plain `Utf8`.
    /// `downcast_string` only accepts `StringArray`, so `metric_name`/
    /// `bucket_counts`/`explicit_bounds`/`service_name` must be cast to
    /// `Utf8` in the projection before `.collect()` — this fixture proves
    /// that cast makes the stage source-encoding-agnostic.
    #[tokio::test]
    async fn histogram_quantile_reads_utf8view_encoded_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8View, false),
            Field::new("metric_name", DataType::Utf8View, false),
            Field::new("count", DataType::Int64, true),
            Field::new("sum", DataType::Float64, true),
            Field::new("min", DataType::Float64, true),
            Field::new("max", DataType::Float64, true),
            Field::new("bucket_counts", DataType::Utf8View, true),
            Field::new("explicit_bounds", DataType::Utf8View, true),
            Field::new("aggregation_temporality", DataType::Int32, true),
            map_field_named("attributes"),
            map_field_named("resource_attributes"),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![0_i64])),
                Arc::new(StringViewArray::from(vec!["svcV"])),
                Arc::new(StringViewArray::from(vec!["viewmetric"])),
                Arc::new(datafusion::arrow::array::Int64Array::from(vec![1i64])),
                Arc::new(Float64Array::from(vec![0.0f64])),
                Arc::new(Float64Array::from(vec![0.0f64])),
                Arc::new(Float64Array::from(vec![0.0f64])),
                Arc::new(StringViewArray::from(vec!["[2,2,0,0]"])),
                Arc::new(StringViewArray::from(vec!["[0.1,0.5,1.0]"])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![2i32])),
                build_map(&[&[] as &[(&str, &str)]]),
                build_map(&[&[] as &[(&str, &str)]]),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("metrics_histogram".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);

        let svc = IrService::new(ctx);
        let d = doc(serde_json::json!({
            "irVersion": 3, "from": "metrics_histogram", "range": { "from": 0, "to": 1000 },
            "result": "series",
            "pipeline": [
                { "where": { "field": "metric.name", "op": "eq", "value": "viewmetric" } },
                { "histogram_quantile": { "q": 0.5, "step": "1000ms", "mode": "instant", "as": "p50" } }
            ]
        }));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let batches = df.collect().await.unwrap();
        let vs = histogram_value(&batches, "p50", None);
        assert_eq!(vs.len(), 1);
        assert!(vs[0].is_finite(), "got {:?}", vs[0]);
    }

    #[tokio::test]
    async fn metrics_histogram_missing_table_returns_none() {
        let ctx = SessionContext::new();
        let sp = Arc::new(MemorySchemaProvider::new());
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);

        let svc = IrService::new(ctx);
        let d = doc(serde_json::json!({
            "irVersion": 3, "from": "metrics_histogram", "range": { "from": 0, "to": 1000 },
            "result": "series",
            "pipeline": [
                { "histogram_quantile": { "q": 0.5, "step": "1000ms", "as": "p50" } }
            ]
        }));
        assert!(svc.plan(&d, "t", "d", 0).await.unwrap().is_none());
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
        let svc = IrService::new(profiles_ctx());
        let params = IrQueryParams {
            document: serde_json::json!({
                "irVersion": 1, "from": "profiles", "range": { "from": 0, "to": 1000 },
                "result": "flamegraph",
                "pipeline": [{ "where": { "field": "profile.id", "op": "eq", "value": "p1" } }]
            }),
            now_ns: 0,
        };
        let (batches, _) = svc.query(&params, "t", "d").await.unwrap();
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
        let svc = IrService::new(profiles_ctx());
        let params = IrQueryParams {
            document: serde_json::json!({
                "irVersion": 1, "from": "profiles", "range": { "from": 0, "to": 1000 },
                "result": "flamegraph",
                "pipeline": [{ "where": { "field": "service.name", "op": "eq", "value": "api" } }]
            }),
            now_ns: 0,
        };
        let (batches, _) = svc.query(&params, "t", "d").await.unwrap();
        let flamegraph = flamegraph_from_batch(&batches[0]);
        // p1 (main/foo, 100) + p2 (main/bar, 50) match service=api; p3 (web) does not.
        assert_eq!(flamegraph.total, 150);
        assert!(flamegraph.names.contains(&"foo".to_string()));
        assert!(flamegraph.names.contains(&"bar".to_string()));
        assert!(!flamegraph.names.contains(&"baz".to_string()));
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
            "fields": ["log_attributes"],
            "pipeline": []
        }));
        let err = svc
            .plan(&d, "t", "d", 0)
            .await
            .expect_err("physical addressing must stay rejected");
        assert!(
            format!("{err}").contains("log_attributes"),
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
        let svc = IrService::new(logs_ctx());
        let d = doc(serde_json::json!({
            "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
            "result": "rows",
            "pipeline": [{ "where": { "field": "body", "op": "eq", "value": "a" } }]
        }));

        let err = svc
            .plan(&d, "t", "d", 0)
            .await
            .expect_err("retrieval-only metadata must not be filterable");
        assert!(format!("{err}").contains("body"), "unexpected error: {err}");
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

        let check = |sp: &SourcePlan, cols: &HashSet<String>, sig: &str| {
            assert!(
                cols.contains(sp.time_col),
                "{sig} time_col '{}' not in schema",
                sp.time_col
            );
            for c in sp.containers {
                assert!(cols.contains(*c), "{sig} container '{c}' not in schema");
            }
            for c in sp.row_defaults {
                assert!(cols.contains(*c), "{sig} row default '{c}' not in schema");
            }
            for (_, physical) in sp.aliases {
                assert!(
                    cols.contains(*physical),
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

        // Only row_defaults needs checking against both tables — it's the
        // common-denominator column set scan_source_tables projects onto
        // before unioning gauge + sum, so it must exist in each.
        let gauge = common::iceberg::schemas::create_metrics_gauge_schema().unwrap();
        let gauge_cols: HashSet<String> =
            gauge.fields().iter().map(|f| f.name.to_string()).collect();
        let sum = common::iceberg::schemas::create_metrics_sum_schema().unwrap();
        let sum_cols: HashSet<String> = sum.fields().iter().map(|f| f.name.to_string()).collect();
        let metrics = SourcePlan::for_source("metrics").unwrap();
        for c in metrics.row_defaults {
            assert!(gauge_cols.contains(*c), "metrics_gauge missing '{c}'");
            assert!(sum_cols.contains(*c), "metrics_sum missing '{c}'");
        }
        check(&metrics, &gauge_cols, "metrics (vs. gauge)");

        let histogram = common::iceberg::schemas::create_metrics_histogram_schema().unwrap();
        let histogram_cols: HashSet<String> = histogram
            .fields()
            .iter()
            .map(|f| f.name.to_string())
            .collect();
        check(
            &SourcePlan::for_source("metrics_histogram").unwrap(),
            &histogram_cols,
            "metrics_histogram",
        );
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
            let svc = IrService::new(logs_ctx());
            let params = crate::query::IrQueryParams {
                document: serde_json::json!({
                    "irVersion": 1, "from": "logs", "range": { "from": 0, "to": 1000 },
                    "result": "rows",
                    "pipeline": []
                }),
                now_ns: 0,
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
        }
    }

    #[tokio::test]
    async fn query_on_absent_source_table_is_empty() {
        let svc = IrService::new(empty_dataset_ctx());
        let (batches, window) = svc
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
}
