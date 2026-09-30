//! # Classic-histogram bucket math
//!
//! Quantile and fraction interpolation over explicit bucket bounds, used by
//! the histogram operators in [`metric_ops`](super::metric_ops).

pub(crate) const NON_SCALAR_METRIC_TYPES: &[&str] =
    &["histogram", "exponential_histogram", "summary"];

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
/// interpolation), as Prometheus' `BucketFraction`: buckets span
/// `(prev_bound, bound]` with observations spread uniformly. The first bucket
/// starts at 0 when its bound is positive and at -Inf otherwise; the open
/// `+Inf` bucket sits above the last finite bound. An infinite-width bucket's
/// observations sit at its infinite edge.
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
    if b == 0 && bounds[0] <= 0.0 {
        // (-Inf, bounds[0]]: its observations lie below any finite `x`.
        return counts[0];
    }
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

    // ---- histogram_fraction ----

    /// Prometheus' `BucketFraction`: a first bound <= 0 makes the first
    /// bucket (-Inf, bound], whose observations count as below any finite
    /// value; they are never inside `(lower, upper]`.
    #[test]
    fn histogram_fraction_first_bucket_reaches_down_to_minus_inf_below_zero() {
        let bounds = [-5.0, -1.0, 0.0, 1.0];
        let counts = [1.0, 2.0, 3.0, 4.0, 5.0];
        // (-5, 0] holds the (-5, -1] and (-1, 0] buckets: 2 + 3 of 15.
        assert!((histogram_fraction(-5.0, 0.0, &bounds, &counts) - 5.0 / 15.0).abs() < 1e-12);
        assert!((histogram_fraction(-10.0, 0.0, &bounds, &counts) - 5.0 / 15.0).abs() < 1e-12);
        assert_eq!(histogram_fraction(-10.0, -5.0, &bounds, &counts), 0.0);
        // A positive first bound still starts the first bucket at 0.
        assert_eq!(histogram_fraction(-1.0, 0.5, &[1.0, 2.0], &[2.0, 2.0, 0.0]), 0.25);
    }
}
