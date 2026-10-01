//! # Pipeline stages and structured operands
//!
//! Every stage operand is a **structured value**, never an embedded
//! mini-expression string. Aggregate outputs are uniquely named (`as`) and are
//! the only thing an `AggRef`, `order`, or `topk`/`bottomk` may reference.

use serde::{Deserialize, Serialize};

use super::predicate::Predicate;
use super::value::ValueType;
use super::version::Feature;

/// An aggregate function. Member of the versioned function registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrAggFn))]
#[serde(rename_all = "snake_case")]
pub enum AggFn {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Quantile,
    /// Population standard deviation (`irVersion` 5).
    Stddev,
    /// Population variance (`irVersion` 5).
    Stdvar,
    /// The earliest value in the group by the source's time column
    /// (`irVersion` 5).
    First,
    /// The latest value in the group by the source's time column
    /// (`irVersion` 5).
    Last,
    /// Per-second rate of a monotonic counter over the `step` window,
    /// counter-reset aware. Only legal on `metrics` with `step` set
    /// (`irVersion` 6).
    Rate,
    /// The counter's total increase over the `step` window, counter-reset
    /// aware — `rate` without the division by the window width (`irVersion`
    /// 6).
    Increase,
    /// Instantaneous per-second rate from the last two samples in the
    /// window, counter-reset aware (`irVersion` 7). Unlike `rate`, which
    /// averages over the whole window, `irate` reacts to the most recent
    /// pair of samples — PromQL's `irate()`.
    Irate,
    /// The average of the raw values seen in the window (`irVersion` 7).
    AvgOverTime,
    /// The minimum of the raw values seen in the window (`irVersion` 7).
    MinOverTime,
    /// The maximum of the raw values seen in the window (`irVersion` 7).
    MaxOverTime,
    /// The sum of the raw values seen in the window (`irVersion` 7).
    SumOverTime,
    /// The count of raw samples seen in the window (`irVersion` 7).
    CountOverTime,
    /// An approximate count of distinct non-null values of the field in the
    /// group, computed with a bounded-memory sketch (DataFusion
    /// `approx_distinct`, HyperLogLog) rather than an exact distinct count —
    /// its cost does not grow with the number of distinct values. Accepts
    /// `string`/`int64`/`bool`/`timestamp` operands; `float64` is rejected at
    /// validation (`irVersion` 9).
    CountDistinct,
}

impl AggFn {
    /// Whether the function needs an `of` field operand. `count` does not.
    pub fn needs_field(self) -> bool {
        !matches!(self, AggFn::Count)
    }

    /// Whether the function needs a numeric `arg` (e.g. the quantile).
    pub fn needs_arg(self) -> bool {
        matches!(self, AggFn::Quantile)
    }

    /// Whether this is a per-series range function (`rate`/`increase`/`irate`/
    /// `*_over_time`) — computed per series from ordered raw samples rather
    /// than a plain grouped reduction, and legal only on `metrics` with
    /// `step` set.
    pub fn is_range_fn(self) -> bool {
        matches!(
            self,
            AggFn::Rate
                | AggFn::Increase
                | AggFn::Irate
                | AggFn::AvgOverTime
                | AggFn::MinOverTime
                | AggFn::MaxOverTime
                | AggFn::SumOverTime
                | AggFn::CountOverTime
        )
    }

    /// Whether this function may reduce several series into one output group
    /// per step (the `aggregate` stage's `across` reducer, or a range
    /// function's implicit `sum` default).
    pub fn is_across_reducer(self) -> bool {
        matches!(
            self,
            AggFn::Sum | AggFn::Avg | AggFn::Min | AggFn::Max | AggFn::Count
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            AggFn::Count => "count",
            AggFn::Sum => "sum",
            AggFn::Avg => "avg",
            AggFn::Min => "min",
            AggFn::Max => "max",
            AggFn::Quantile => "quantile",
            AggFn::Stddev => "stddev",
            AggFn::Stdvar => "stdvar",
            AggFn::First => "first",
            AggFn::Last => "last",
            AggFn::Rate => "rate",
            AggFn::Increase => "increase",
            AggFn::Irate => "irate",
            AggFn::AvgOverTime => "avg_over_time",
            AggFn::MinOverTime => "min_over_time",
            AggFn::MaxOverTime => "max_over_time",
            AggFn::SumOverTime => "sum_over_time",
            AggFn::CountOverTime => "count_over_time",
            AggFn::CountDistinct => "count_distinct",
        }
    }
}

/// A single named aggregate output.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrAgg))]
#[serde(deny_unknown_fields)]
pub struct Agg {
    #[serde(rename = "fn")]
    pub func: AggFn,
    /// The field being aggregated (a logical name). Omitted for `count`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub of: Option<String>,
    /// A numeric argument (e.g. the quantile in `[0,1]`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arg: Option<f64>,
    /// Divide the aggregate's value by this scalar, so a measure can be
    /// reported per unit rather than absolute (`irVersion` 5).
    ///
    /// This is what a rate is: a count over a window, divided by the window.
    /// Named for the operation rather than for time — dividing an aggregate
    /// by a scalar is not inherently temporal, and this IR is
    /// signal-agnostic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub divisor: Option<f64>,
    /// The output column name — the only thing later stages may reference.
    #[serde(rename = "as")]
    pub as_name: String,
    /// An optional predicate scoping which records this aggregate consumes,
    /// so one grouped query can report a total and a subset measure over the
    /// same groups (`count` of everything beside `count` of just the errors).
    ///
    /// It is the same grammar, resolver and evaluation semantics as the `where`
    /// stage — deliberately the shared [`Predicate`], not a second grammar —
    /// and it narrows only this aggregate. Grouping happens once regardless.
    #[serde(rename = "where", default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<Predicate>,
    /// For a per-series range function (`rate`/`increase`/`irate`/
    /// `*_over_time`), the reducer that folds each `by` group's per-series
    /// values into one value per step. Defaults to `sum` (the historical
    /// `rate`/`increase` behaviour). `irVersion` 7.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub across: Option<AggFn>,
    /// For a per-series range function, the lookback window: each step's
    /// value uses samples in `(t - window, t]`. A duration string like
    /// `step`. Defaults to `step` (the historical behaviour). `irVersion` 7.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<String>,
}

/// The `aggregate` stage: group-reduce, optionally time-bucketed by `step`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrAggregate))]
#[serde(deny_unknown_fields)]
pub struct Aggregate {
    /// Grouping fields (logical names).
    #[serde(default)]
    pub by: Vec<String>,
    /// The named aggregate outputs.
    pub aggs: Vec<Agg>,
    /// A time-bucket width (`"1m"`). Present → the result is a `series`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,
}

/// A sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrDirection))]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Asc,
    Desc,
}

/// An `order` key: a field or aggregate name plus a direction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrOrder))]
#[serde(deny_unknown_fields)]
pub struct Order {
    /// A `FieldRef` or `AggRef` (a name), never an expression string.
    pub of: String,
    pub dir: Direction,
}

/// A `topk`/`bottomk` rank stage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrRank))]
#[serde(deny_unknown_fields)]
pub struct Rank {
    /// Must be an integer `> 0` (validated).
    pub n: i64,
    /// The `AggRef` or `FieldRef` to rank by (a name).
    pub of: String,
}

/// A parser for the `extract` stage. `regex` is deferred (registry-gated).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrParser))]
#[serde(rename_all = "snake_case")]
pub enum Parser {
    Json,
    Logfmt,
}

/// A field derived by an `extract` stage, with its declared type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrDerivedField))]
#[serde(deny_unknown_fields)]
pub struct DerivedField {
    pub name: String,
    #[serde(rename = "type")]
    pub value_type: ValueType,
}

/// The `extract` stage: derive typed, query-local fields from log content.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrExtract))]
#[serde(deny_unknown_fields)]
pub struct Extract {
    pub parser: Parser,
    #[serde(rename = "as")]
    pub as_fields: Vec<DerivedField>,
}

/// A terminal two-dimensional count aggregate, available in IR v2.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrHeatmapAxisX))]
#[serde(deny_unknown_fields)]
pub struct HeatmapAxisX {
    pub step: String,
    pub align: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrHeatmapAxisY))]
#[serde(deny_unknown_fields)]
pub struct HeatmapAxisY {
    pub of: String,
    pub bounds: Vec<serde_json::Value>,
    #[serde(default)]
    pub overflow: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrHeatmapValue))]
#[serde(deny_unknown_fields)]
pub struct HeatmapValue {
    #[serde(rename = "fn")]
    pub func: AggFn,
    #[serde(rename = "as")]
    pub as_name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrHeatmap))]
#[serde(deny_unknown_fields)]
pub struct Heatmap {
    pub x: HeatmapAxisX,
    pub y: HeatmapAxisY,
    pub value: HeatmapValue,
}

/// How data points sharing a `histogram_quantile` step bucket combine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrHistogramMode))]
#[serde(rename_all = "snake_case")]
pub enum HistogramMode {
    /// Each series' increase over `(t - window, t]` (cumulative points
    /// differenced against the series' own previous point, delta points
    /// summed), merged across series — `histogram_quantile(q, rate(x[w]))`.
    #[default]
    Rate,
    /// Each series' latest point in `(t - lookback, t]`, merged across
    /// series; `lookback` defaults to `step`.
    Instant,
}

/// A terminal quantile-over-buckets stage, available in IR v3. Only legal on
/// the `metrics` source, over its histogram rows: interpolates a percentile
/// from OTLP classic-histogram bucket data, distinct from the `aggregate` stage's
/// `fn: "quantile"` (`approx_percentile_cont` over independent scalar
/// values — a different algorithm entirely, over a different source shape).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrHistogramQuantile))]
#[serde(deny_unknown_fields)]
pub struct HistogramQuantile {
    /// The quantile, in `[0, 1]`.
    pub q: f64,
    /// Grouping labels (logical names), and the output's labels. Grouping
    /// also separates metrics internally — merging bucket data across
    /// different metrics is meaningless, since different metrics carry
    /// different bucket bounds — but, as in Prometheus, the output does not
    /// carry `metric.name`.
    #[serde(default)]
    pub by: Vec<String>,
    /// One result per stored series instead of merging them (`irVersion`
    /// 10). Excludes `by`; the output keeps each series' labels less
    /// `metric.name`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub per_series: bool,
    /// Evaluation step: the stage is evaluated at `t = from + k·step` and
    /// each value labelled `t`. The result is always a `series`.
    pub step: String,
    #[serde(default)]
    pub mode: HistogramMode,
    /// Rate mode's window: each instant reads `(t - window, t]` (default:
    /// `step`), `irVersion` 10.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<String>,
    /// Instant mode only: at each evaluation instant `t`, read each series'
    /// latest point within `(t - lookback, t]`, as a PromQL instant vector
    /// does (`irVersion` 10). Without it the lookback is `step`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lookback: Option<String>,
    /// The output value column name.
    #[serde(rename = "as")]
    pub as_name: String,
}

/// The estimated fraction of histogram observations in `(lower, upper]`,
/// cumulative(`upper`) − cumulative(`lower`): the `histogram_quantile`
/// sibling (`irVersion` 10). A bound inside a bucket is interpolated, so the
/// result is an estimate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrHistogramFraction))]
#[serde(deny_unknown_fields)]
pub struct HistogramFraction {
    pub lower: f64,
    pub upper: f64,
    #[serde(default)]
    pub by: Vec<String>,
    /// One result per stored series instead of merging them (`irVersion`
    /// 10). Excludes `by`; the output keeps each series' labels less
    /// `metric.name`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub per_series: bool,
    pub step: String,
    #[serde(default)]
    pub mode: HistogramMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<String>,
    /// As on `histogram_quantile`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lookback: Option<String>,
    #[serde(rename = "as")]
    pub as_name: String,
}

/// What a `describe` stage introspects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrDescribeTarget))]
#[serde(rename_all = "snake_case")]
pub enum DescribeTarget {
    /// The queryable fields of the source.
    Fields,
    /// The values one named field takes.
    Values,
}

impl DescribeTarget {
    pub fn as_str(self) -> &'static str {
        match self {
            DescribeTarget::Fields => "fields",
            DescribeTarget::Values => "values",
        }
    }
}

/// The `describe` stage: introspect the source instead of reading its records.
///
/// Terminal, and legal only with the `metadata` result envelope. It is answered
/// from declared schema, the type authority, the tenant's schema registries
/// and maintained statistics — not by lowering to a query plan — so it carries
/// no predicate: see `openspec/changes/archive/2026-09-22-query-field-discovery`
/// (design D6) for why a predicate-scoped answer is refused rather than approximated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrDescribe))]
#[serde(deny_unknown_fields)]
pub struct Describe {
    pub target: DescribeTarget,
    /// The logical field whose values to suggest. Required by `values`,
    /// rejected by `fields`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    /// Maximum items to return. Bounded by the server's own cap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    /// Opt in to reading signal data when no metadata covers the request.
    /// Without it, an uncovered request is answered with an explanation
    /// rather than a scan.
    #[serde(default)]
    pub sample: bool,
}

/// What a `correlate` stage joins the current relation to, written as a
/// plain string: `"parent"` (one hop within `traces`, `irVersion` 8) or the
/// name of another signal source (`irVersion` 11). An unregistered source
/// name parses and is rejected by validation as an unknown source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "String", into = "String")]
pub enum CorrelateTarget {
    /// The span in the same trace whose `span_id` equals this row's
    /// `parent_span_id`.
    Parent,
    /// Another signal source, joined on a logical [`CorrelateKey`].
    Signal(String),
}

impl From<String> for CorrelateTarget {
    fn from(name: String) -> Self {
        if name == "parent" {
            CorrelateTarget::Parent
        } else {
            CorrelateTarget::Signal(name)
        }
    }
}

impl From<CorrelateTarget> for String {
    fn from(target: CorrelateTarget) -> Self {
        match target {
            CorrelateTarget::Parent => "parent".to_string(),
            CorrelateTarget::Signal(name) => name,
        }
    }
}

/// A join kind for a `correlate` stage. `semi`/`anti` need a signal target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrJoinKind))]
#[serde(rename_all = "snake_case")]
pub enum JoinKind {
    Inner,
    Left,
    Semi,
    Anti,
}

impl JoinKind {
    pub fn as_str(self) -> &'static str {
        match self {
            JoinKind::Inner => "inner",
            JoinKind::Left => "left",
            JoinKind::Semi => "semi",
            JoinKind::Anti => "anti",
        }
    }
}

/// A logical join key a signal `correlate` matches on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrCorrelateKey))]
#[serde(rename_all = "snake_case")]
pub enum CorrelateKey {
    TraceId,
    /// The `(trace_id, span_id)` pair.
    SpanId,
    ResourceIdentity,
    SeriesId,
}

impl CorrelateKey {
    pub fn as_str(self) -> &'static str {
        match self {
            CorrelateKey::TraceId => "trace_id",
            CorrelateKey::SpanId => "span_id",
            CorrelateKey::ResourceIdentity => "resource_identity",
            CorrelateKey::SeriesId => "series_id",
        }
    }

    /// The logical fields that carry this key on `source`, or `None` when
    /// the source has no such key.
    pub fn fields(self, source: &str) -> Option<&'static [&'static str]> {
        Some(match (self, source) {
            (CorrelateKey::TraceId, "traces" | "logs") => &["trace_id"],
            (CorrelateKey::TraceId, "profiles" | "exemplars") => &["trace.id"],
            (CorrelateKey::SpanId, "traces" | "logs") => &["trace_id", "span_id"],
            (CorrelateKey::SpanId, "profiles" | "exemplars") => &["trace.id", "span.id"],
            (
                CorrelateKey::ResourceIdentity,
                "traces" | "logs" | "profiles" | "metrics" | "exemplars",
            ) => &["resource.identity"],
            (CorrelateKey::SeriesId, "metrics" | "exemplars") => &["series.id"],
            _ => return None,
        })
    }
}

/// How far a signal `correlate` widens its target scan beyond the source
/// rows' time envelope. Both default to zero.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrCorrelateWindow))]
#[serde(deny_unknown_fields)]
pub struct CorrelateWindow {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
}

/// The `correlate` stage. With `to: "parent"` (`irVersion` 8) it joins each
/// span to its parent span, whose columns come back under a fixed `parent.`
/// prefix. With a signal target (`irVersion` 11) it joins the relation to
/// that source `on` a logical key; `pipeline` (`where` stages only) narrows
/// the target side.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrCorrelate))]
#[serde(deny_unknown_fields)]
pub struct Correlate {
    #[cfg_attr(feature = "openapi", schema(value_type = String))]
    pub to: CorrelateTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on: Option<CorrelateKey>,
    pub kind: JoinKind,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[cfg_attr(feature = "openapi", schema(no_recursion))]
    pub pipeline: Vec<Stage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<CorrelateWindow>,
    /// inner/left only: the most target rows kept per source row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fanout: Option<i64>,
}

/// How a `match` relation relates its two span-sets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrMatchOp))]
#[serde(rename_all = "snake_case")]
pub enum MatchOp {
    /// `right`'s parent is `left`.
    Child,
    /// `right` is a descendant of `left` at any depth.
    Descendant,
    /// `right` is an ancestor of `left`.
    Ancestor,
    /// `right` and `left` share a non-empty parent and are different spans.
    Sibling,
}

/// One structural relation of a `match` stage, between two declared span-sets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrMatchRelation))]
#[serde(deny_unknown_fields)]
pub struct MatchRelation {
    pub left: String,
    pub op: MatchOp,
    pub right: String,
}

/// A `match` stage's named span-set predicates, in declaration order.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SpanSets(pub Vec<(String, Predicate)>);

impl Serialize for SpanSets {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(self.0.iter().map(|(name, pred)| (name, pred)))
    }
}

impl<'de> Deserialize<'de> for SpanSets {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = SpanSets;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an object of named span-set predicates")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<SpanSets, A::Error> {
                let mut sets = Vec::new();
                while let Some(entry) = map.next_entry()? {
                    sets.push(entry);
                }
                Ok(SpanSets(sets))
            }
        }
        deserializer.deserialize_map(Visitor)
    }
}

/// The `match` stage (`irVersion` 12): keep the traces in which every
/// span-set has a matching span and every relation holds, returning the
/// witnessing spans.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrMatch))]
#[serde(deny_unknown_fields)]
pub struct Match {
    #[cfg_attr(
        feature = "openapi",
        schema(value_type = std::collections::BTreeMap<String, Predicate>)
    )]
    pub spansets: SpanSets,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relations: Vec<MatchRelation>,
}

impl Match {
    /// The most span-sets one stage may declare.
    pub const MAX_SPANSETS: usize = 8;
    /// The output column naming the span-sets each row witnesses.
    pub const SPANSETS: &'static str = "spansets";
}

/// A function a `sample` stage evaluates over each series' point stream
/// (`irVersion` 10).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrSampleFn))]
#[serde(rename_all = "snake_case")]
pub enum SampleFn {
    /// The latest point in `(t - lookback, t]`.
    Latest,
    Rate,
    Increase,
    Irate,
    Delta,
    Idelta,
    Deriv,
    Resets,
    Changes,
    AvgOverTime,
    MinOverTime,
    MaxOverTime,
    SumOverTime,
    CountOverTime,
    LastOverTime,
    StddevOverTime,
    StdvarOverTime,
    PresentOverTime,
    QuantileOverTime,
}

/// Which value of a metric point a `sample` reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrSampleOf))]
pub enum SampleOf {
    #[default]
    #[serde(rename = "metric.value")]
    Value,
    #[serde(rename = "metric.count")]
    Count,
    #[serde(rename = "metric.sum")]
    Sum,
}

/// The `sample` stage: evaluate a metric point stream into a `Series` at
/// every evaluation instant (`irVersion` 10).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrSample))]
#[serde(deny_unknown_fields)]
pub struct Sample {
    #[serde(rename = "fn")]
    pub func: SampleFn,
    #[serde(default)]
    pub of: SampleOf,
    /// The range read by every function but `latest`: `(t - window, t]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<String>,
    /// `latest` only: how far back a point still counts (default `5m`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lookback: Option<String>,
    /// The evaluation step; defaults to the document `step`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,
    /// Shift the read window back by this non-negative duration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<String>,
    /// Pin every evaluation instant to this timestamp literal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<serde_json::Value>,
    /// `quantile_over_time` only: the quantile in `[0, 1]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arg: Option<f64>,
}

/// A `reduce` function: folds series into groups at every instant
/// (`irVersion` 10).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrReduceFn))]
#[serde(rename_all = "snake_case")]
pub enum ReduceFn {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Group,
    Stddev,
    Stdvar,
    Quantile,
    Topk,
    Bottomk,
    CountValues,
}

/// The `reduce` stage: Series → Series, grouped `by` or `without` labels.
///
/// `without` also drops `metric.name`, as Prometheus does; `by` keeps
/// exactly the listed labels (so `by (metric.name)` keeps the name).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrReduce))]
#[serde(deny_unknown_fields)]
pub struct Reduce {
    #[serde(rename = "fn")]
    pub func: ReduceFn,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub without: Option<Vec<String>>,
    /// `topk`/`bottomk`: the integer k; `quantile`: the quantile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arg: Option<f64>,
    /// `count_values`: the label that carries each value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

/// A per-value `map` function (`irVersion` 10).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrMapFn))]
#[serde(rename_all = "snake_case")]
pub enum MapFn {
    Abs,
    Ceil,
    Floor,
    Round,
    Sqrt,
    Exp,
    Ln,
    Log2,
    Log10,
    Sgn,
    Clamp,
    ClampMin,
    ClampMax,
    Timestamp,
    DayOfMonth,
    DayOfWeek,
    DayOfYear,
    DaysInMonth,
    Hour,
    Minute,
    Month,
    Year,
}

impl MapFn {
    /// The accepted number of `args`.
    pub fn arity(self) -> std::ops::RangeInclusive<usize> {
        match self {
            MapFn::Round => 0..=1,
            MapFn::Clamp => 2..=2,
            MapFn::ClampMin | MapFn::ClampMax => 1..=1,
            _ => 0..=0,
        }
    }

    /// A pure math function, legal on a Scalar as well as a Series.
    pub fn is_math(self) -> bool {
        !matches!(
            self,
            MapFn::Timestamp
                | MapFn::DayOfMonth
                | MapFn::DayOfWeek
                | MapFn::DayOfYear
                | MapFn::DaysInMonth
                | MapFn::Hour
                | MapFn::Minute
                | MapFn::Month
                | MapFn::Year
        )
    }
}

/// The `map` stage: apply a function to every value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrMap))]
#[serde(deny_unknown_fields)]
pub struct Map {
    #[serde(rename = "fn")]
    pub func: MapFn,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<f64>,
}

/// `labels.replace`: PromQL's `label_replace`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrLabelReplace))]
#[serde(deny_unknown_fields)]
pub struct LabelReplace {
    pub dst: String,
    pub replacement: String,
    pub src: String,
    pub regex: String,
}

/// `labels.join`: PromQL's `label_join`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrLabelJoin))]
#[serde(deny_unknown_fields)]
pub struct LabelJoin {
    pub dst: String,
    pub separator: String,
    pub src: Vec<String>,
}

/// The `labels` stage: rewrite one label of every series.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrLabels))]
#[serde(rename_all = "snake_case")]
pub enum Labels {
    Replace(LabelReplace),
    Join(LabelJoin),
}

/// A comparison against a number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrCompareOp))]
#[serde(rename_all = "snake_case")]
pub enum CompareOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

/// The `filter` stage: keep the values that compare true, or with `bool`
/// replace every value by 0/1.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrFilter))]
#[serde(deny_unknown_fields)]
pub struct Filter {
    pub op: CompareOp,
    pub value: f64,
    #[serde(default)]
    pub bool: bool,
}

/// The `absent` stage: one series valued 1 where the input has none.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrAbsent))]
#[serde(deny_unknown_fields)]
pub struct Absent {
    #[serde(default)]
    pub labels: std::collections::BTreeMap<String, String>,
}

/// An `over_time` function (`irVersion` 10).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrOverTimeFn))]
#[serde(rename_all = "snake_case")]
pub enum OverTimeFn {
    Avg,
    Min,
    Max,
    Sum,
    Count,
    Last,
    Stddev,
    Stdvar,
    Present,
    Quantile,
    Delta,
    Deriv,
    Changes,
    Resets,
}

/// The `over_time` stage: re-window a Series evaluated at its own step (a
/// subquery).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrOverTime))]
#[serde(deny_unknown_fields)]
pub struct OverTime {
    #[serde(rename = "fn")]
    pub func: OverTimeFn,
    pub window: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arg: Option<f64>,
}
/// A `binop` operator (`irVersion` 10).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrBinopOp))]
#[serde(rename_all = "snake_case")]
pub enum BinopOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Atan2,
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    And,
    Or,
    Unless,
}

impl BinopOp {
    pub fn is_comparison(self) -> bool {
        matches!(
            self,
            BinopOp::Eq | BinopOp::Ne | BinopOp::Gt | BinopOp::Ge | BinopOp::Lt | BinopOp::Le
        )
    }

    /// `and`/`or`/`unless`: set operations on label sets.
    pub fn is_set(self) -> bool {
        matches!(self, BinopOp::And | BinopOp::Or | BinopOp::Unless)
    }
}

/// Which operand of a `binop` holds many series per match.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrGroupSide))]
#[serde(rename_all = "snake_case")]
pub enum GroupSide {
    Left,
    Right,
}

/// A `binop`'s one-to-many (`group_left`/`group_right`) match.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrBinopGroup))]
#[serde(deny_unknown_fields)]
pub struct BinopGroup {
    pub side: GroupSide,
    /// Labels copied from the "one" side onto the result.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<String>,
}

/// A `binop`'s right operand as a sub-document: it inherits `irVersion`,
/// `range` and `step` from the enclosing document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrSubDocument))]
#[serde(deny_unknown_fields)]
pub struct SubDocument {
    pub from: String,
    #[serde(default)]
    #[cfg_attr(feature = "openapi", schema(no_recursion))]
    pub pipeline: Vec<Stage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub constant: Option<f64>,
}

/// A `binop`'s right operand: a number or a sub-document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrBinopOperand))]
#[serde(untagged)]
pub enum BinopOperand {
    Number(f64),
    Document(Box<SubDocument>),
}

/// The `binop` stage: combine the pipeline (left) with `right`.
///
/// Two series match on a key of their labels, as in PromQL: by default every
/// label but `metric.name`; with `ignoring`, every label but `metric.name`
/// and the listed ones; with `on`, exactly the listed labels.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrBinop))]
#[serde(deny_unknown_fields)]
pub struct Binop {
    pub op: BinopOp,
    pub right: BinopOperand,
    /// Evaluate `right op left` (e.g. `2 - series`).
    #[serde(default)]
    pub reverse: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignoring: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<BinopGroup>,
    /// Comparison ops only: yield 0/1 instead of filtering.
    #[serde(default)]
    pub bool: bool,
}

/// The operand of a stage that takes none (`{"scalar": {}}`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrNoOperands))]
#[serde(deny_unknown_fields)]
pub struct NoOperands {}

/// A transform stage in the pipeline. Externally tagged: a single-key object
/// whose key names the stage. An unknown key is an unsupported stage and is
/// rejected by name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrStage))]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    Where(Predicate),
    Extract(Extract),
    Aggregate(Aggregate),
    Topk(Rank),
    Bottomk(Rank),
    Order(Vec<Order>),
    Limit(u64),
    Heatmap(Heatmap),
    HistogramQuantile(HistogramQuantile),
    Describe(Describe),
    Correlate(Correlate),
    Sample(Sample),
    Scalar(NoOperands),
    Vector(NoOperands),
    Reduce(Reduce),
    Map(Map),
    Labels(Labels),
    Filter(Filter),
    Sort(Direction),
    Absent(Absent),
    OverTime(OverTime),
    Binop(Binop),
    HistogramFraction(HistogramFraction),
    Match(Match),
}

impl Stage {
    /// The stage's name, for error messages.
    pub fn name(&self) -> &'static str {
        match self {
            Stage::Where(_) => "where",
            Stage::Extract(_) => "extract",
            Stage::Aggregate(_) => "aggregate",
            Stage::Topk(_) => "topk",
            Stage::Bottomk(_) => "bottomk",
            Stage::Order(_) => "order",
            Stage::Limit(_) => "limit",
            Stage::Heatmap(_) => "heatmap",
            Stage::HistogramQuantile(_) => "histogram_quantile",
            Stage::Describe(_) => "describe",
            Stage::Correlate(_) => "correlate",
            Stage::Sample(_) => "sample",
            Stage::Scalar(_) => "scalar",
            Stage::Vector(_) => "vector",
            Stage::Reduce(_) => "reduce",
            Stage::Map(_) => "map",
            Stage::Labels(_) => "labels",
            Stage::Filter(_) => "filter",
            Stage::Sort(_) => "sort",
            Stage::Absent(_) => "absent",
            Stage::OverTime(_) => "over_time",
            Stage::Binop(_) => "binop",
            Stage::HistogramFraction(_) => "histogram_fraction",
            Stage::Match(_) => "match",
        }
    }

    /// The versioned feature this stage kind is, if it is gated.
    pub fn feature(&self) -> Option<Feature> {
        Some(match self {
            Stage::Heatmap(_) => Feature::Heatmap,
            Stage::HistogramQuantile(_) => Feature::HistogramQuantile,
            Stage::Describe(_) => Feature::Describe,
            Stage::Correlate(Correlate {
                to: CorrelateTarget::Parent,
                ..
            }) => Feature::SpanCorrelate,
            Stage::Correlate(_) => Feature::SignalCorrelate,
            Stage::Sample(_) => Feature::Sample,
            Stage::Scalar(_) => Feature::ScalarStage,
            Stage::Vector(_) => Feature::VectorStage,
            Stage::Reduce(_) => Feature::Reduce,
            Stage::Map(_) => Feature::Map,
            Stage::Labels(_) => Feature::Labels,
            Stage::Filter(_) => Feature::Filter,
            Stage::Sort(_) => Feature::Sort,
            Stage::Absent(_) => Feature::Absent,
            Stage::OverTime(_) => Feature::OverTime,
            Stage::Binop(_) => Feature::Binop,
            Stage::HistogramFraction(_) => Feature::HistogramFraction,
            Stage::Match(_) => Feature::Match,
            _ => return None,
        })
    }
}

/// Whether a reference name is actually an embedded expression string rather
/// than a structured operand (e.g. `"max(duration)"`). Used to reject the
/// pre-restructure `topk:{of:"max(duration)"}` form.
pub fn is_expression_string(name: &str) -> bool {
    name.chars()
        .any(|c| matches!(c, '(' | ')' | '+' | '-' | '*' | '/' | ' ' | ','))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ComparisonOp, Leaf};
    use serde_json::json;

    #[test]
    fn aggregate_with_step_and_named_output_parses() {
        let s: Stage = serde_json::from_value(json!({
            "aggregate": { "by": ["service.name"], "aggs": [{ "fn": "count", "as": "n" }], "step": "1m" }
        }))
        .unwrap();
        assert!(matches!(s, Stage::Aggregate(_)));
    }

    #[test]
    fn unknown_stage_key_is_rejected_by_name() {
        let err = serde_json::from_value::<Stage>(json!({ "frobnicate": {} })).unwrap_err();
        assert!(err.to_string().contains("frobnicate"), "got: {err}");
    }

    #[test]
    fn correlate_parent_inner_parses() {
        let s: Stage =
            serde_json::from_value(json!({ "correlate": { "to": "parent", "kind": "inner" } }))
                .unwrap();
        let Stage::Correlate(c) = s else {
            panic!("expected a correlate stage");
        };
        assert_eq!(c.to, CorrelateTarget::Parent);
        assert_eq!(c.kind, JoinKind::Inner);
        assert_eq!(Stage::Correlate(c).name(), "correlate");
    }

    #[test]
    fn correlate_left_parses() {
        let s: Stage =
            serde_json::from_value(json!({ "correlate": { "to": "parent", "kind": "left" } }))
                .unwrap();
        assert!(matches!(
            s,
            Stage::Correlate(Correlate {
                kind: JoinKind::Left,
                ..
            })
        ));
    }

    #[test]
    fn correlate_rejects_unknown_inner_key() {
        assert!(
            serde_json::from_value::<Stage>(json!({
                "correlate": { "to": "parent", "kind": "inner", "bogus": 1 }
            }))
            .is_err()
        );
    }

    /// An unregistered target name parses (as a signal target) and is left
    /// to validation to reject as an unknown source.
    #[test]
    fn correlate_unknown_target_parses_as_a_signal() {
        let s: Stage = serde_json::from_value(json!({
            "correlate": { "to": "grandparent", "kind": "inner" }
        }))
        .unwrap();
        let Stage::Correlate(c) = s else {
            panic!("expected a correlate stage");
        };
        assert_eq!(c.to, CorrelateTarget::Signal("grandparent".to_string()));
    }

    #[test]
    fn signal_correlate_round_trips() {
        let v = json!({ "correlate": {
            "to": "logs", "on": "span_id", "kind": "anti",
            "pipeline": [{ "where": { "field": "severity_number", "op": "gte", "value": 17 } }],
            "window": { "after": "10m" }
        } });
        let s: Stage = serde_json::from_value(v.clone()).unwrap();
        let Stage::Correlate(c) = &s else {
            panic!("expected a correlate stage");
        };
        assert_eq!(c.on, Some(CorrelateKey::SpanId));
        assert_eq!(c.kind, JoinKind::Anti);
        assert_eq!(serde_json::to_value(&s).unwrap(), v);
    }

    #[test]
    fn unknown_inner_key_is_rejected() {
        assert!(
            serde_json::from_value::<Stage>(json!({
                "aggregate": { "aggs": [{ "fn": "count", "as": "n" }], "bogus": 1 }
            }))
            .is_err()
        );
    }

    #[test]
    fn expression_string_operand_is_detected() {
        assert!(is_expression_string("max(duration)"));
        assert!(!is_expression_string("max_dur"));
    }

    #[test]
    fn aggregate_scope_parses_into_a_predicate() {
        let s: Stage = serde_json::from_value(json!({
            "aggregate": { "by": ["span.name"], "aggs": [
                { "fn": "count", "as": "n" },
                { "fn": "count", "as": "errors",
                  "where": { "field": "status.code", "op": "eq", "value": "Error" } }
            ] }
        }))
        .unwrap();
        let Stage::Aggregate(agg) = s else {
            panic!("expected an aggregate stage");
        };
        assert_eq!(agg.aggs[0].scope, None, "unscoped aggregate keeps no scope");
        let scope = agg.aggs[1].scope.as_ref().expect("scope parses");
        assert!(matches!(scope, Predicate::Leaf(l) if l.field == "status.code"));
    }

    #[test]
    fn aggregate_scope_round_trips_and_is_omitted_when_absent() {
        let scoped = Agg {
            func: AggFn::Count,
            of: None,
            arg: None,
            divisor: None,
            as_name: "errors".to_string(),
            scope: Some(Predicate::Leaf(Leaf {
                field: "status.code".to_string(),
                op: ComparisonOp::Eq,
                value: Some(json!("Error")),
            })),
            across: None,
            window: None,
        };
        let encoded = serde_json::to_value(&scoped).unwrap();
        assert!(encoded.get("where").is_some());
        assert_eq!(
            serde_json::from_value::<Agg>(encoded).unwrap(),
            scoped,
            "a scoped aggregate round-trips"
        );

        let unscoped = Agg {
            scope: None,
            ..scoped
        };
        let encoded = serde_json::to_value(&unscoped).unwrap();
        assert!(
            encoded.get("where").is_none(),
            "an unscoped aggregate emits no `where` key: {encoded}"
        );
    }

    #[test]
    fn unknown_key_on_an_aggregate_is_still_rejected() {
        assert!(
            serde_json::from_value::<Agg>(json!({
                "fn": "count", "as": "n", "filter": { "field": "x", "op": "exists" }
            }))
            .is_err(),
            "`deny_unknown_fields` still guards the aggregate"
        );
    }

    #[test]
    fn a_scope_may_compose_with_and_or_not() {
        let agg: Agg = serde_json::from_value(json!({
            "fn": "count", "as": "n",
            "where": { "and": [
                { "field": "status.code", "op": "eq", "value": "Error" },
                { "not": { "field": "http.route", "op": "exists" } }
            ] }
        }))
        .unwrap();
        assert!(matches!(agg.scope, Some(Predicate::And(ref v)) if v.len() == 2));
    }

    #[test]
    fn histogram_quantile_parses_with_default_mode_and_empty_by() {
        let s: Stage = serde_json::from_value(json!({
            "histogram_quantile": { "q": 0.95, "step": "1m", "as": "p95" }
        }))
        .unwrap();
        let Stage::HistogramQuantile(hq) = s else {
            panic!("expected a histogram_quantile stage");
        };
        assert_eq!(hq.q, 0.95);
        assert_eq!(hq.by, Vec::<String>::new());
        assert_eq!(hq.step, "1m");
        assert_eq!(hq.mode, HistogramMode::Rate);
        assert_eq!(hq.as_name, "p95");
        assert_eq!(Stage::HistogramQuantile(hq).name(), "histogram_quantile");
    }

    #[test]
    fn histogram_quantile_parses_explicit_by_and_instant_mode() {
        let s: Stage = serde_json::from_value(json!({
            "histogram_quantile": {
                "q": 0.5, "by": ["service.name"], "step": "30s",
                "mode": "instant", "as": "p50"
            }
        }))
        .unwrap();
        let Stage::HistogramQuantile(hq) = s else {
            panic!("expected a histogram_quantile stage");
        };
        assert_eq!(hq.by, vec!["service.name".to_string()]);
        assert_eq!(hq.mode, HistogramMode::Instant);
    }

    #[test]
    fn histogram_quantile_rejects_unknown_inner_key() {
        assert!(
            serde_json::from_value::<Stage>(json!({
                "histogram_quantile": { "q": 0.95, "step": "1m", "as": "p95", "bogus": 1 }
            }))
            .is_err()
        );
    }

    #[test]
    fn histogram_quantile_round_trips() {
        let hq = HistogramQuantile {
            q: 0.99,
            by: vec!["service.name".to_string()],
            per_series: false,
            step: "5m".to_string(),
            mode: HistogramMode::Instant,
            window: None,
            lookback: None,
            as_name: "p99".to_string(),
        };
        let encoded = serde_json::to_value(Stage::HistogramQuantile(hq.clone())).unwrap();
        assert_eq!(
            serde_json::from_value::<Stage>(encoded).unwrap(),
            Stage::HistogramQuantile(hq)
        );
    }

    #[test]
    fn topk_parses_as_structured_operand() {
        let s: Stage =
            serde_json::from_value(json!({ "topk": { "n": 10, "of": "max_dur" } })).unwrap();
        match s {
            Stage::Topk(r) => {
                assert_eq!(r.n, 10);
                assert_eq!(r.of, "max_dur");
            }
            _ => panic!("expected topk"),
        }
    }
}
