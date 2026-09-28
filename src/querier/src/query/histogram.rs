//! # Classic-histogram bucket math
//!
//! Shared by the PromQL `histogram_quantile`/`histogram_fraction` functions
//! ([`metrics`](super::metrics)) and the Query IR `histogram_quantile` stage
//! ([`ir_planner`](super::ir_planner)) — both surfaces must compute the same
//! percentile from the same `metrics_histogram` bucket data, so the
//! interpolation and accumulation logic lives here once rather than being
//! reimplemented per surface.

use std::collections::HashMap;

use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, ListArray, StringArray, StringViewArray,
};

/// Merges OTLP histogram data points that share a step bucket and series.
pub(crate) struct HistogramAcc {
    /// Upper bounds of the finite buckets (`explicit_bounds`).
    pub(crate) bounds: Vec<f64>,
    /// Running element-wise sum of `bucket_counts` (one more than `bounds`).
    pub(crate) counts: Vec<f64>,
}

impl HistogramAcc {
    pub(crate) fn new(bounds: Vec<f64>, count_len: usize) -> Self {
        Self {
            bounds,
            counts: vec![0.0; count_len],
        }
    }

    pub(crate) fn merge(&mut self, row_counts: &[f64]) {
        if row_counts.len() == self.counts.len() {
            for (acc, add) in self.counts.iter_mut().zip(row_counts) {
                *acc += add;
            }
        }
    }
}

/// Tracks the first and last histogram data points (by timestamp) for a
/// series in a bucket, so `histogram_quantile(phi, rate(metric[range]))` can
/// interpolate over the per-bucket count delta.
pub(crate) struct RateHistAcc {
    pub(crate) bounds: Vec<f64>,
    first_ts: i64,
    first: Vec<f64>,
    last_ts: i64,
    last: Vec<f64>,
}

impl RateHistAcc {
    pub(crate) fn new(bounds: Vec<f64>, ts: i64, counts: Vec<f64>) -> Self {
        Self {
            bounds,
            first_ts: ts,
            first: counts.clone(),
            last_ts: ts,
            last: counts,
        }
    }

    pub(crate) fn observe(&mut self, ts: i64, counts: &[f64]) {
        if counts.len() != self.first.len() {
            return;
        }
        if ts <= self.first_ts {
            self.first_ts = ts;
            self.first = counts.to_vec();
        }
        if ts >= self.last_ts {
            self.last_ts = ts;
            self.last = counts.to_vec();
        }
    }

    /// Per-bucket increase, clamped to ≥ 0 (a decrease means a counter reset).
    pub(crate) fn delta(&self) -> Vec<f64> {
        self.last
            .iter()
            .zip(&self.first)
            .map(|(l, f)| (l - f).max(0.0))
            .collect()
    }
}

/// Parse a JSON numeric array (`"[1,2,3]"`) into `f64`s.
fn parse_f64_array(raw: &str) -> Option<Vec<f64>> {
    let value: serde_json::Value = serde_json::from_str(raw).ok()?;
    let array = value.as_array()?;
    // Bounds may carry the acceptor's non-finite sentinels ("+Inf" for a
    // Prometheus-style overflow bucket) — see common::flight::conversion.
    array
        .iter()
        .map(common::flight::conversion::json_to_f64)
        .collect()
}

/// Parses `explicit_bounds` via [`parse_f64_array`], memoizing on the raw
/// JSON string in `cache`. `explicit_bounds` is fixed at instrumentation
/// time, so every data point of a given histogram series carries the same
/// bounds array — caching avoids re-parsing identical JSON once per row of
/// a potentially large per-bucket scan.
fn parse_bounds_cached(cache: &mut HashMap<String, Vec<f64>>, raw: &str) -> Option<Vec<f64>> {
    if let Some(cached) = cache.get(raw) {
        return Some(cached.clone());
    }
    let parsed = parse_f64_array(raw)?;
    cache.insert(raw.to_string(), parsed.clone());
    Some(parsed)
}

/// A resolved `bucket_counts`/`explicit_bounds` column: either the wide
/// `metrics` table's typed `List<Int64>`/`List<Float64>` shape, or the
/// legacy per-type table's JSON-array string shape — resolved once per batch
/// via [`Self::resolve`], not per row, since a column's physical type is
/// fixed for the whole batch.
pub(crate) enum BucketCol<'a> {
    F64List(&'a ListArray, &'a Float64Array),
    I64List(&'a ListArray, &'a Int64Array),
    Utf8(&'a StringArray),
    Utf8View(&'a StringViewArray),
}

impl<'a> BucketCol<'a> {
    pub(crate) fn resolve(column: &'a dyn Array) -> Option<Self> {
        if let Some(list) = column.as_any().downcast_ref::<ListArray>() {
            return match list.values().as_any().downcast_ref::<Float64Array>() {
                Some(values) => Some(BucketCol::F64List(list, values)),
                None => list
                    .values()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|values| BucketCol::I64List(list, values)),
            };
        }
        if let Some(a) = column.as_any().downcast_ref::<StringArray>() {
            return Some(BucketCol::Utf8(a));
        }
        column
            .as_any()
            .downcast_ref::<StringViewArray>()
            .map(BucketCol::Utf8View)
    }

    pub(crate) fn is_null(&self, row: usize) -> bool {
        match self {
            BucketCol::F64List(list, _) | BucketCol::I64List(list, _) => list.is_null(row),
            BucketCol::Utf8(a) => a.is_null(row),
            BucketCol::Utf8View(a) => a.is_null(row),
        }
    }

    /// Decodes row `row` as `f64`s. `cache` memoizes the legacy JSON parse
    /// (used for `explicit_bounds`, fixed per series); the typed-list
    /// variants need no parsing and ignore it.
    pub(crate) fn decode(
        &self,
        row: usize,
        cache: Option<&mut HashMap<String, Vec<f64>>>,
    ) -> Option<Vec<f64>> {
        match self {
            BucketCol::F64List(list, values) => {
                let (start, len) = Self::row_range(list, row);
                Some(
                    (start..start + len)
                        .map(|i| {
                            if values.is_null(i) {
                                0.0
                            } else {
                                values.value(i)
                            }
                        })
                        .collect(),
                )
            }
            BucketCol::I64List(list, values) => {
                let (start, len) = Self::row_range(list, row);
                Some(
                    (start..start + len)
                        .map(|i| if values.is_null(i) { 0 } else { values.value(i) } as f64)
                        .collect(),
                )
            }
            BucketCol::Utf8(a) => Self::parse(a.value(row), cache),
            BucketCol::Utf8View(a) => Self::parse(a.value(row), cache),
        }
    }

    fn row_range(list: &ListArray, row: usize) -> (usize, usize) {
        let offsets = list.value_offsets();
        let start = offsets[row] as usize;
        (start, offsets[row + 1] as usize - start)
    }

    fn parse(raw: &str, cache: Option<&mut HashMap<String, Vec<f64>>>) -> Option<Vec<f64>> {
        match cache {
            Some(cache) => parse_bounds_cached(cache, raw),
            None => parse_f64_array(raw),
        }
    }
}

/// Decodes one row's `bucket_counts`/`explicit_bounds` pair via
/// [`BucketCol::resolve`] — either the wide `metrics` table's typed
/// `List<Int64>`/`List<Float64>` columns, or the legacy per-type table's
/// JSON-array string columns. `None` for a null row or an unrecognized
/// column type.
pub(crate) fn decode_bucket_row(
    counts: &ArrayRef,
    bounds: &ArrayRef,
    row: usize,
    bounds_cache: &mut HashMap<String, Vec<f64>>,
) -> Option<(Vec<f64>, Vec<f64>)> {
    let counts = BucketCol::resolve(counts.as_ref())?;
    let bounds = BucketCol::resolve(bounds.as_ref())?;
    if counts.is_null(row) || bounds.is_null(row) {
        return None;
    }
    Some((
        counts.decode(row, None)?,
        bounds.decode(row, Some(bounds_cache))?,
    ))
}

/// Interpolate the `phi`-quantile of a classic histogram, following
/// Prometheus's `bucketQuantile`: locate the bucket the rank falls in and
/// linearly interpolate within it, assuming a uniform spread.
///
/// `bounds` are the finite bucket upper bounds; `counts` are the
/// non-cumulative per-bucket counts with one extra `+Inf` bucket
/// (`counts.len() == bounds.len() + 1`).
pub(crate) fn histogram_quantile(phi: f64, bounds: &[f64], counts: &[f64]) -> f64 {
    if phi.is_nan() {
        return f64::NAN;
    }
    if phi < 0.0 {
        return f64::NEG_INFINITY;
    }
    if phi > 1.0 {
        return f64::INFINITY;
    }
    if bounds.is_empty() || counts.len() != bounds.len() + 1 {
        return f64::NAN;
    }
    let total: f64 = counts.iter().sum();
    if total <= 0.0 {
        return f64::NAN;
    }

    // Cumulative counts across buckets.
    let mut cumulative = Vec::with_capacity(counts.len());
    let mut running = 0.0;
    for &c in counts {
        running += c;
        cumulative.push(running);
    }

    let rank = phi * total;
    let last = counts.len() - 1;
    let b = cumulative.iter().position(|&c| c >= rank).unwrap_or(last);

    // Rank lands in the open-ended `+Inf` bucket: clamp to the top finite
    // bound (we can't extrapolate beyond it).
    if b == last {
        return bounds[bounds.len() - 1];
    }

    let bucket_end = bounds[b];
    let (bucket_start, rank_in_bucket, count_in_bucket) = if b == 0 {
        // Prometheus: a non-positive first bound is returned as-is.
        if bucket_end <= 0.0 {
            return bucket_end;
        }
        (0.0, rank, cumulative[0])
    } else {
        (bounds[b - 1], rank - cumulative[b - 1], counts[b])
    };
    if count_in_bucket <= 0.0 {
        return bucket_start;
    }
    bucket_start + (bucket_end - bucket_start) * (rank_in_bucket / count_in_bucket)
}

/// The fraction of a classic histogram's observations in `(lower, upper]`,
/// via the cumulative distribution (the inverse of [`histogram_quantile`]'s
/// interpolation): buckets span `(prev_bound, bound]` with the first starting
/// at 0, observations spread uniformly, and the open `+Inf` bucket sits above
/// the last finite bound.
pub(crate) fn histogram_fraction(lower: f64, upper: f64, bounds: &[f64], counts: &[f64]) -> f64 {
    if bounds.is_empty() || counts.len() != bounds.len() + 1 {
        return f64::NAN;
    }
    let total: f64 = counts.iter().sum();
    if total <= 0.0 {
        return f64::NAN;
    }
    (hist_cumulative(upper, bounds, counts) - hist_cumulative(lower, bounds, counts)) / total
}

/// Cumulative count of observations `<= x`, interpolated within the bucket.
pub(crate) fn hist_cumulative(x: f64, bounds: &[f64], counts: &[f64]) -> f64 {
    let n = bounds.len();
    // At or above the top finite bound: all finite-bucket observations count;
    // the `+Inf` bucket's observations are strictly greater.
    if x >= bounds[n - 1] {
        return counts[..n].iter().sum();
    }
    // First finite bucket whose upper bound is >= x contains x.
    let b = bounds.iter().position(|&bd| bd >= x).unwrap_or(0);
    let bucket_start = if b == 0 { 0.0 } else { bounds[b - 1] };
    let before: f64 = counts[..b].iter().sum();
    if x <= bucket_start {
        return before;
    }
    let width = bounds[b] - bucket_start;
    if width <= 0.0 {
        return before;
    }
    let frac = ((x - bucket_start) / width).clamp(0.0, 1.0);
    before + counts[b] * frac
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- histogram_quantile ----

    #[test]
    fn histogram_quantile_interpolates_within_bucket() {
        // bounds [1,2,4], counts [1,2,3,4] (incl. +Inf), total 10.
        // rank 5 lands in the (2,4] bucket: 2 + 2*(5-3)/3 = 3.333…
        let bounds = [1.0, 2.0, 4.0];
        let counts = [1.0, 2.0, 3.0, 4.0];
        assert!((histogram_quantile(0.5, &bounds, &counts) - (2.0 + 2.0 * 2.0 / 3.0)).abs() < 1e-9);
        // rank 1 lands at the top of the first bucket: exactly its bound.
        assert!((histogram_quantile(0.1, &bounds, &counts) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn histogram_quantile_in_inf_bucket_clamps_to_top_bound() {
        let bounds = [1.0, 2.0, 4.0];
        let counts = [1.0, 2.0, 3.0, 4.0];
        // rank 9.5 falls in the +Inf bucket → clamp to the highest bound.
        assert_eq!(histogram_quantile(0.95, &bounds, &counts), 4.0);
    }

    #[test]
    fn histogram_quantile_edge_cases() {
        let bounds = [1.0, 2.0];
        let counts = [1.0, 1.0, 1.0];
        assert!(histogram_quantile(f64::NAN, &bounds, &counts).is_nan());
        assert_eq!(
            histogram_quantile(-0.1, &bounds, &counts),
            f64::NEG_INFINITY
        );
        assert_eq!(histogram_quantile(1.5, &bounds, &counts), f64::INFINITY);
        // No observations → NaN.
        assert!(histogram_quantile(0.5, &bounds, &[0.0, 0.0, 0.0]).is_nan());
        // Malformed (counts length != bounds+1) → NaN.
        assert!(histogram_quantile(0.5, &bounds, &[1.0, 1.0]).is_nan());
    }

    #[test]
    fn parse_f64_array_handles_json_and_junk() {
        assert_eq!(parse_f64_array("[1, 2.5, 3]"), Some(vec![1.0, 2.5, 3.0]));
        assert_eq!(parse_f64_array("[]"), Some(vec![]));
        assert_eq!(
            parse_f64_array(r#"[1, 2.5, "+Inf"]"#),
            Some(vec![1.0, 2.5, f64::INFINITY])
        );
        assert_eq!(parse_f64_array("not json"), None);
        assert_eq!(parse_f64_array(r#"{"a":1}"#), None);
    }

    #[test]
    fn bucket_col_reads_typed_list_columns() {
        use datafusion::arrow::datatypes::Float64Type;

        let counts = ListArray::from_iter_primitive::<Float64Type, _, _>(vec![Some(vec![
            Some(1.0),
            Some(2.0),
            Some(3.0),
        ])]);
        let bounds = ListArray::from_iter_primitive::<Float64Type, _, _>(vec![Some(vec![
            Some(1.0),
            Some(f64::INFINITY),
        ])]);
        let counts = BucketCol::resolve(&counts).expect("typed list resolves");
        let bounds = BucketCol::resolve(&bounds).expect("typed list resolves");
        assert_eq!(counts.decode(0, None), Some(vec![1.0, 2.0, 3.0]));
        assert_eq!(bounds.decode(0, None), Some(vec![1.0, f64::INFINITY]));
    }

    #[test]
    fn bucket_col_reads_legacy_json_columns() {
        let counts = StringArray::from(vec!["[1,2,3]"]);
        let bounds = StringArray::from(vec![r#"[1,"+Inf"]"#]);
        let counts = BucketCol::resolve(&counts).expect("utf8 resolves");
        let bounds = BucketCol::resolve(&bounds).expect("utf8 resolves");
        let mut cache = HashMap::new();
        assert_eq!(counts.decode(0, None), Some(vec![1.0, 2.0, 3.0]));
        assert_eq!(
            bounds.decode(0, Some(&mut cache)),
            Some(vec![1.0, f64::INFINITY])
        );
        assert_eq!(cache.len(), 1, "bounds parse should be cached");
    }

    #[test]
    fn bucket_col_reads_utf8view_columns() {
        // A Parquet-backed scan under DataFusion 54 can yield `Utf8View` for
        // string columns (a zero-copy optimization) instead of `Utf8`.
        let counts = StringViewArray::from(vec!["[1,2,3]"]);
        let bounds = StringViewArray::from(vec![r#"[1,"+Inf"]"#]);
        let counts = BucketCol::resolve(&counts).expect("utf8view resolves");
        let bounds = BucketCol::resolve(&bounds).expect("utf8view resolves");
        assert_eq!(counts.decode(0, None), Some(vec![1.0, 2.0, 3.0]));
        assert_eq!(bounds.decode(0, None), Some(vec![1.0, f64::INFINITY]));
    }

    #[test]
    fn decode_bucket_row_reads_typed_list_and_legacy_json_columns() {
        use datafusion::arrow::datatypes::Float64Type;
        use std::sync::Arc;

        let typed_counts: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
                Some(vec![Some(1.0), Some(2.0), Some(3.0)]),
            ]));
        let typed_bounds: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
                Some(vec![Some(1.0), Some(f64::INFINITY)]),
            ]));
        let mut cache = HashMap::new();
        assert_eq!(
            decode_bucket_row(&typed_counts, &typed_bounds, 0, &mut cache),
            Some((vec![1.0, 2.0, 3.0], vec![1.0, f64::INFINITY]))
        );

        let json_counts: ArrayRef = Arc::new(StringArray::from(vec!["[1,2,3]"]));
        let json_bounds: ArrayRef = Arc::new(StringArray::from(vec![r#"[1,"+Inf"]"#]));
        assert_eq!(
            decode_bucket_row(&json_counts, &json_bounds, 0, &mut cache),
            Some((vec![1.0, 2.0, 3.0], vec![1.0, f64::INFINITY]))
        );
    }

    #[test]
    fn bounds_parse_cache_matches_uncached_parsing() {
        let raw_bounds = ["[1,2,4]", "[1,2,4]", "[0.5,1.5]", "not json", "[1,2,4]"];
        let mut cache: HashMap<String, Vec<f64>> = HashMap::new();
        for raw in raw_bounds {
            let cached = parse_bounds_cached(&mut cache, raw);
            let uncached = parse_f64_array(raw);
            assert_eq!(cached, uncached, "mismatch for {raw:?}");
        }
        // Only the two distinct, parseable bounds strings should be cached.
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get("[1,2,4]"), Some(&vec![1.0, 2.0, 4.0]));
        assert_eq!(cache.get("[0.5,1.5]"), Some(&vec![0.5, 1.5]));
    }
}
