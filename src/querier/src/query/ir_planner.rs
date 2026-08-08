//! # IR → DataFusion planner (single-signal)
//!
//! Lowers a validated [`Document`](common::query_ir::Document) over a single
//! signal (`logs`/`traces`) to a DataFusion `DataFrame`, satisfying the IR's
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

use common::query_ir::{
    Aggregate, ComparisonOp, Document, Extract, FieldResolver, Leaf, Literal, Parser, Predicate,
    Resolved, ResultEnvelope, SourceRegistry, Stage, TimestampLiteral, ValueType, coerce, validate,
};
use datafusion::arrow::array::{
    Array, LargeStringArray, StringArray, StringBuilder, StringViewArray,
};
use datafusion::arrow::datatypes::{DataType, IntervalMonthDayNano, TimeUnit};
use datafusion::functions::core::expr_fn::{coalesce, get_field};
use datafusion::functions::datetime::expr_fn::date_bin;
use datafusion::functions::regex::expr_fn::regexp_like;
use datafusion::functions::string::expr_fn::contains;
use datafusion::functions_aggregate::expr_fn::{approx_percentile_cont, avg, count, max, min, sum};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility, cast, col, lit, not,
};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::scalar::ScalarValue;

use super::IrQueryParams;
use super::error::QuerierError;
use super::table_lookup::optional_table;

/// Per-source planning facts: the physical table, its time column, and the
/// attribute-map containers a `get_field` extraction targets.
///
/// The physical column names here are validated against the canonical persisted
/// Iceberg schema (`common::schema::SCHEMA_DEFINITIONS`) by a unit test — the
/// traces v2 schema renames `name`→`span_name` and `duration_nano`→
/// `duration_nanos`, so those idiosyncratic renames live in `aliases`.
struct SourcePlan {
    table: &'static str,
    /// The column carrying the row's primary timestamp.
    time_col: &'static str,
    /// Whether `time_col` is a real `Timestamp` (compare with a timestamp
    /// literal) or an integer nanosecond column (compare with an `i64`).
    time_is_timestamp: bool,
    /// Attribute-map containers, in resolution/coalesce order.
    containers: &'static [&'static str],
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
                table: "logs",
                time_col: "timestamp",
                time_is_timestamp: true,
                containers: &["log_attributes", "resource_attributes"],
                row_defaults: &[
                    "timestamp",
                    "body",
                    "service_name",
                    "severity_text",
                    "trace_id",
                    "span_id",
                ],
                aliases: &[("service.name", "service_name")],
            }),
            "traces" => Some(SourcePlan {
                table: "traces",
                time_col: "start_time_unix_nano",
                time_is_timestamp: false,
                containers: &["span_attributes", "resource_attributes"],
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
                ],
            }),
            _ => None,
        }
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

/// A [`FieldResolver`] that types physical columns from the scanned table's
/// Arrow schema and treats every other logical name as an unpromoted attribute
/// (a `String` extraction from the signal's attribute container). This is the
/// promotion-invariant production view — a consumer of the attribute registry
/// (#811); until #811 supplies canonical attribute types, unpromoted attributes
/// are `String`.
struct SchemaResolver {
    columns: HashMap<String, ValueType>,
    container: String,
    aliases: &'static [(&'static str, &'static str)],
}

impl SchemaResolver {
    fn new(schema: &datafusion::common::DFSchema, source: &SourcePlan) -> Self {
        let mut columns = HashMap::new();
        for field in schema.fields() {
            if let Some(vt) = arrow_to_value_type(field.data_type()) {
                columns.insert(field.name().to_string(), vt);
            }
        }
        SchemaResolver {
            columns,
            container: source.containers[0].to_string(),
            aliases: source.aliases,
        }
    }

    /// Resolve a logical field to a physical column + type, if one exists.
    /// Tries, in order: an exact column name, a declared alias, the
    /// dot→underscore form of the name, and the promoted `label_<sanitized>`
    /// materialization. `None` means the field is an unpromoted attribute.
    fn column_for(&self, field: &str) -> Option<(String, ValueType)> {
        if let Some(vt) = self.columns.get(field) {
            return Some((field.to_string(), vt.clone()));
        }
        if let Some((_, physical)) = self.aliases.iter().find(|(logical, _)| *logical == field)
            && let Some(vt) = self.columns.get(*physical)
        {
            return Some((physical.to_string(), vt.clone()));
        }
        let underscored = safe_ident(field);
        if underscored != field
            && let Some(vt) = self.columns.get(&underscored)
        {
            return Some((underscored, vt.clone()));
        }
        let materialized = common::schema::materialized_column_name(field);
        if let Some(vt) = self.columns.get(&materialized) {
            return Some((materialized, vt.clone()));
        }
        None
    }
}

impl FieldResolver for SchemaResolver {
    fn resolve(&self, _source: &str, field: &str) -> Option<Resolved> {
        match self.column_for(field) {
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
        self.column_for(field).is_some()
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
        let Some((df, window)) = self
            .plan(&doc, tenant_slug, dataset_slug, params.now_ns)
            .instrument(tracing::info_span!("signaldb.query.plan"))
            .await?
        else {
            // No storage for this source in this dataset: no rows, but the
            // window is still resolved so the caller can echo it back.
            return Ok((Vec::new(), resolve_window(&doc, params.now_ns)?));
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
            .map_err(QuerierError::QueryFailed)?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        exec_span.record("signaldb.query.rows", rows as i64);
        exec_span.record("signaldb.query.batches", batches.len() as i64);
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

        // A dataset with no table for this source has no rows to plan over.
        // The document's schema-dependent validation is skipped along with
        // the scan — there is no schema to validate against.
        let Some(base) = optional_table(
            &self.session_context,
            tenant_slug,
            dataset_slug,
            source.table,
        )
        .await?
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
            df = lowering.lower_stage(df, stage)?;
        }
        df = lowering.apply_projection(df, doc)?;
        Ok(Some((df, window)))
    }
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

    fn agg_expr(&self, a: &common::query_ir::Agg) -> Result<Expr, QuerierError> {
        use common::query_ir::AggFn;
        Ok(match a.func {
            AggFn::Count => count(lit(1i64)),
            AggFn::Sum => sum(self.numeric_of(a)?),
            AggFn::Avg => avg(self.numeric_of(a)?),
            AggFn::Min => min(self.value_expr(a.of.as_deref().unwrap_or_default())?),
            AggFn::Max => max(self.value_expr(a.of.as_deref().unwrap_or_default())?),
            AggFn::Quantile => {
                let q = a.arg.unwrap_or(0.5);
                approx_percentile_cont(self.numeric_of(a)?.sort(true, false), lit(q), None)
            }
        })
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
            None => Err(QuerierError::InvalidInput(format!(
                "field '{logical}' has no canonical type"
            ))),
        }
    }

    /// Extract an attribute value, coalescing over the source's containers that
    /// are present in the scanned schema.
    fn attr_expr(&self, key: &str) -> Expr {
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
            let is_json = matches!(resolved, Resolved::JsonPath { .. });
            let ty = resolved.value_type().clone();
            let expr = match &resolved {
                Resolved::Column { name, .. } => col(name.clone()),
                Resolved::JsonPath { key, .. } => self.attr_expr(key),
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
        // Series results are already shaped by the step aggregate.
        if doc.result == ResultEnvelope::Series || self.series_shaped {
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
        ArrayRef, Int64Array, MapBuilder, MapFieldNames, StringArray, StringBuilder,
        TimestampNanosecondArray,
    };
    use datafusion::arrow::datatypes::{Field, Fields, Schema};
    use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
    use datafusion::catalog::{CatalogProvider, MemTable, SchemaProvider};
    use std::sync::Arc;

    fn map_field() -> Field {
        let entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::Utf8, true),
            ])),
            false,
        );
        Field::new(
            "log_attributes",
            DataType::Map(Arc::new(entries), false),
            true,
        )
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
            map_field(),
            Field::new(
                "resource_attributes",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("keys", DataType::Utf8, false),
                            Field::new("values", DataType::Utf8, true),
                        ])),
                        false,
                    )),
                    false,
                ),
                true,
            ),
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
        let log_attrs = build_map(&[
            &[("deployment.environment", "prod")],
            &[("deployment.environment", "prod")],
            &[("other", "x")],
            &[("deployment.environment", "prod")],
        ]);
        let res_attrs = build_map(&[&[], &[], &[], &[]]);

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
                log_attrs,
                res_attrs,
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
                { "aggregate": { "by": ["service_name"], "aggs": [{ "fn": "count", "as": "n" }], "step": "1ms" } }
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
                { "aggregate": { "by": ["service_name"], "aggs": [{ "fn": "count", "as": "n" }], "step": "1ms" } }
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
                Arc::new(StringArray::from(vec!["t1", "t2", "t3"])),
                Arc::new(StringArray::from(vec!["s1", "s2", "s3"])),
                Arc::new(StringArray::from(vec![Some("p1"), None, Some("p3")])),
                Arc::new(StringArray::from(vec!["GET /a", "GET /b", "POST /c"])),
                Arc::new(StringArray::from(vec!["api", "api", "web"])),
                Arc::new(Int64Array::from(vec![10_i64, 20, 30])),
                Arc::new(Int64Array::from(vec![100_i64, 900, 500])),
                Arc::new(StringArray::from(vec![
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
            "fields": ["span_name", "duration_nanos"],
            "pipeline": [
                { "where": { "field": "service.name", "op": "eq", "value": "api" } },
                { "topk": { "n": 1, "of": "duration_nanos" } }
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
            "fields": ["service_name"],
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
            "fields": ["service_name"],
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
            "fields": ["service_name", "severity_number"],
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
}
