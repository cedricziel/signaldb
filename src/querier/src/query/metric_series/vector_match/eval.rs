//! Per-bucket matching over operands folded from their input batches.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::mem::discriminant;
use std::sync::Arc;

use common::query_ir::{BinopGroup, BinopOp, GroupSide};
use datafusion::arrow::array::{ArrayRef, Float64Array, StringArray, TimestampNanosecondArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DFResult};

use super::LABELS;
use crate::query::error::QuerierError;
use crate::query::metric_series::value_fn::{arithmetic, compare};

pub(super) const NAME_LABEL: &str = "metric.name";
/// Distinct series one operand may hold, the PromQL evaluator's row-wise
/// group bound.
pub(super) const MAX_MATCH_SERIES: usize = 100_000;

#[derive(Debug, Clone, Copy)]
pub(super) enum Kind {
    Series,
    Scalar,
    Number(f64),
}

impl PartialEq for Kind {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Kind::Number(a), Kind::Number(b)) => a.to_bits() == b.to_bits(),
            _ => discriminant(self) == discriminant(other),
        }
    }
}
impl Eq for Kind {}

/// The matching parameters, with operands already swapped for `reverse`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MatchSpec {
    pub(super) op: BinopOp,
    pub(super) on: Option<Vec<String>>,
    pub(super) ignoring: Option<Vec<String>>,
    pub(super) group: Option<BinopGroup>,
    pub(super) bool: bool,
    pub(super) left: Kind,
    pub(super) right: Kind,
}

/// `BinopOp` and `GroupSide` are not `Hash`; equal specs print alike.
impl Hash for MatchSpec {
    fn hash<H: Hasher>(&self, state: &mut H) {
        format!("{self:?}").hash(state);
    }
}

impl MatchSpec {
    pub(super) fn op_name(&self) -> String {
        format!("{:?}", self.op).to_lowercase()
    }

    /// Arithmetic and `bool` comparisons drop the metric name.
    fn drops_name(&self) -> bool {
        !self.op.is_set() && (!self.op.is_comparison() || self.bool)
    }

    /// The label set two series match on; an empty value is an absent label.
    pub(super) fn key(&self, labels: &BTreeMap<String, String>) -> DFResult<String> {
        let kept: BTreeMap<&String, &String> = labels
            .iter()
            .filter(|(k, v)| {
                !v.is_empty()
                    && match &self.on {
                        Some(on) => on.contains(k),
                        None => {
                            k.as_str() != NAME_LABEL
                                && !self.ignoring.iter().flatten().any(|i| i == *k)
                        }
                    }
            })
            .collect();
        canonical(&kept)
    }

    /// `value` for one pair, or `None` when a filtering comparison drops it.
    fn combine(&self, l: f64, r: f64) -> Option<f64> {
        if let Some(value) = arithmetic(self.op, l, r) {
            return Some(value);
        }
        match (self.bool, compare(self.op, l, r)?) {
            (true, holds) => Some(if holds { 1.0 } else { 0.0 }),
            (false, true) => Some(l),
            (false, false) => None,
        }
    }

    /// Output labels of a matched vector pair.
    fn result_labels(
        &self,
        many: &BTreeMap<String, String>,
        one: &BTreeMap<String, String>,
    ) -> DFResult<Arc<str>> {
        let keep = |k: &String| match (&self.group, &self.on) {
            (Some(_), _) => true,
            (None, Some(on)) => on.contains(k),
            (None, None) => !self.ignoring.iter().flatten().any(|i| i == k),
        };
        let mut out: BTreeMap<&str, &str> = many
            .iter()
            .filter(|(k, _)| keep(k))
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        if self.drops_name() {
            out.remove(NAME_LABEL);
        }
        for label in self.group.iter().flat_map(|g| &g.include) {
            match one.get(label).filter(|v| !v.is_empty()) {
                Some(v) => out.insert(label, v),
                None => out.remove(label.as_str()),
            };
        }
        Ok(canonical(&out)?.into())
    }
}

pub(super) fn canonical<K: serde::Serialize + Ord, V: serde::Serialize>(
    labels: &BTreeMap<K, V>,
) -> DFResult<String> {
    serde_json::to_string(labels).map_err(|e| DataFusionError::External(Box::new(e)))
}

pub(super) fn invalid(msg: String) -> DataFusionError {
    DataFusionError::External(Box::new(QuerierError::InvalidInput(msg)))
}

pub(super) struct SeriesMeta {
    pub(super) raw: Arc<str>,
    /// `raw` without the metric name.
    pub(super) unnamed: Arc<str>,
    pub(super) labels: BTreeMap<String, String>,
    pub(super) key: String,
}

/// One Series operand: its series and, per bucket, `(series index, value)`
/// sorted by series index.
#[derive(Default)]
pub(super) struct Side {
    pub(super) series: Vec<SeriesMeta>,
    pub(super) samples: BTreeMap<i64, Vec<(usize, f64)>>,
}

impl Side {
    fn at(&self, bucket: i64) -> &[(usize, f64)] {
        self.samples.get(&bucket).map_or(&[], Vec::as_slice)
    }
}

pub(super) enum Input {
    Series(Side),
    Scalar(BTreeMap<i64, f64>),
    Number(f64),
}

impl Input {
    fn scalar_at(&self, bucket: i64) -> f64 {
        match self {
            Input::Number(n) => *n,
            Input::Scalar(values) => values.get(&bucket).copied().unwrap_or(f64::NAN),
            Input::Series(_) => f64::NAN,
        }
    }
}

type Row = (i64, Arc<str>, f64);

/// Rows arrive bucket by bucket; this orders one bucket's rows by labels.
fn sort_bucket(rows: &mut [Row]) {
    rows.sort_unstable_by(|a, b| a.1.cmp(&b.1));
}

pub(super) fn evaluate(
    spec: &MatchSpec,
    left: &Input,
    right: &Input,
    schema: SchemaRef,
) -> DFResult<RecordBatch> {
    let rows = match (left, right) {
        (Input::Series(l), Input::Series(r)) if spec.op.is_set() => set_ops(spec, l, r),
        (Input::Series(l), Input::Series(r)) => match_vectors(spec, l, r)?,
        (Input::Series(series), scalar) => broadcast(spec, series, scalar, true)?,
        (scalar, Input::Series(series)) => broadcast(spec, series, scalar, false)?,
        _ => scalars(spec, left, right),
    };
    let buckets = TimestampNanosecondArray::from_iter_values(rows.iter().map(|r| r.0));
    let values = Float64Array::from_iter_values(rows.iter().map(|r| r.2));
    let mut columns: Vec<ArrayRef> = vec![Arc::new(buckets)];
    if schema.column_with_name(LABELS).is_some() {
        columns.push(Arc::new(StringArray::from_iter_values(
            rows.iter().map(|r| &*r.1),
        )));
    }
    columns.push(Arc::new(values));
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn scalars(spec: &MatchSpec, left: &Input, right: &Input) -> Vec<Row> {
    let buckets: BTreeSet<i64> = [left, right]
        .into_iter()
        .filter_map(|input| match input {
            Input::Scalar(values) => Some(values.keys().copied()),
            _ => None,
        })
        .flatten()
        .collect();
    let empty: Arc<str> = Arc::from("");
    buckets
        .into_iter()
        .filter_map(|b| {
            let value = spec.combine(left.scalar_at(b), right.scalar_at(b))?;
            Some((b, Arc::clone(&empty), value))
        })
        .collect()
}

/// A Series against a Scalar: the scalar applies to every series.
fn broadcast(
    spec: &MatchSpec,
    series: &Side,
    scalar: &Input,
    series_left: bool,
) -> DFResult<Vec<Row>> {
    let mut rows = Vec::new();
    for (&bucket, samples) in &series.samples {
        let s = scalar.scalar_at(bucket);
        let mut seen = HashSet::new();
        let start = rows.len();
        for &(idx, v) in samples {
            let (l, r) = if series_left { (v, s) } else { (s, v) };
            let Some(mut value) = spec.combine(l, r) else {
                continue;
            };
            if spec.op.is_comparison() && !spec.bool {
                value = v;
            }
            let meta = &series.series[idx];
            let labels = if spec.drops_name() {
                &meta.unnamed
            } else {
                &meta.raw
            };
            if !seen.insert(&**labels) {
                return Err(duplicate_output(spec, labels));
            }
            rows.push((bucket, Arc::clone(labels), value));
        }
        sort_bucket(&mut rows[start..]);
    }
    Ok(rows)
}

fn duplicate_output(spec: &MatchSpec, labels: &str) -> DataFusionError {
    invalid(format!(
        "binop `{}` produced the label set {labels} more than once; \
         the matching labels must keep output series unique",
        spec.op_name()
    ))
}

/// Arithmetic and comparisons: one-to-one, or many-to-one on the `group`
/// side. Only pairs that survive a filtering comparison count as matches.
fn match_vectors(spec: &MatchSpec, left: &Side, right: &Side) -> DFResult<Vec<Row>> {
    let swapped = matches!(&spec.group, Some(g) if g.side == GroupSide::Right);
    let (many, one, one_name) = if swapped {
        (right, left, "left")
    } else {
        (left, right, "right")
    };
    let mut rows = Vec::new();
    let mut labels_of: HashMap<(usize, usize), Arc<str>> = HashMap::new();
    for (&bucket, one_rows) in &one.samples {
        let many_rows = many.at(bucket);
        if many_rows.is_empty() {
            continue;
        }
        let mut index: HashMap<&str, (usize, f64)> = HashMap::new();
        for &(idx, v) in one_rows {
            let key = one.series[idx].key.as_str();
            if index.insert(key, (idx, v)).is_some() {
                return Err(invalid(format!(
                    "binop `{}`: several {one_name} series match the label set {key}; \
                     many-to-many matching is not allowed: matching labels must be unique \
                     on one side",
                    spec.op_name()
                )));
            }
        }
        let mut matched = HashSet::new();
        let mut seen = HashSet::new();
        let start = rows.len();
        for &(idx, v) in many_rows {
            let key = many.series[idx].key.as_str();
            let Some(&(one_idx, one_v)) = index.get(key) else {
                continue;
            };
            let (lv, rv) = if swapped { (one_v, v) } else { (v, one_v) };
            let Some(value) = spec.combine(lv, rv) else {
                continue;
            };
            if spec.group.is_none() && !matched.insert(key) {
                return Err(invalid(format!(
                    "binop `{}`: several left series match the label set {key}; \
                     declare `group` for a one-to-many match",
                    spec.op_name()
                )));
            }
            let labels = match labels_of.get(&(idx, one_idx)) {
                Some(labels) => Arc::clone(labels),
                None => {
                    let labels =
                        spec.result_labels(&many.series[idx].labels, &one.series[one_idx].labels)?;
                    labels_of.insert((idx, one_idx), Arc::clone(&labels));
                    labels
                }
            };
            if !seen.insert(Arc::clone(&labels)) {
                return Err(duplicate_output(spec, &labels));
            }
            rows.push((bucket, labels, value));
        }
        sort_bucket(&mut rows[start..]);
    }
    Ok(rows)
}

fn keys<'a>(side: &'a Side, samples: &[(usize, f64)]) -> HashSet<&'a str> {
    samples
        .iter()
        .map(|(idx, _)| side.series[*idx].key.as_str())
        .collect()
}

/// `and`/`or`/`unless`: keep series by whether their key exists on the
/// other side; labels and values pass through.
fn set_ops(spec: &MatchSpec, left: &Side, right: &Side) -> Vec<Row> {
    let buckets: BTreeSet<i64> = left
        .samples
        .keys()
        .chain(right.samples.keys())
        .copied()
        .collect();
    let mut rows = Vec::new();
    for bucket in buckets {
        let (l, r) = (left.at(bucket), right.at(bucket));
        let right_keys = keys(right, r);
        let start = rows.len();
        for &(idx, v) in l {
            let found = right_keys.contains(left.series[idx].key.as_str());
            let keep = match spec.op {
                BinopOp::And => found,
                BinopOp::Unless => !found,
                _ => true,
            };
            if keep {
                rows.push((bucket, Arc::clone(&left.series[idx].raw), v));
            }
        }
        if spec.op == BinopOp::Or {
            let left_keys = keys(left, l);
            for &(idx, v) in r {
                if !left_keys.contains(right.series[idx].key.as_str()) {
                    rows.push((bucket, Arc::clone(&right.series[idx].raw), v));
                }
            }
        }
        sort_bucket(&mut rows[start..]);
    }
    rows
}
