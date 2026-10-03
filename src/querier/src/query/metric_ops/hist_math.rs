//! Per-series histogram rate/instant values and cross-series merging, independent of DataFusion.

use super::exp_histogram::{Buckets, ExpHistogram, bounds_at};
use crate::query::error::QuerierError;
use crate::query::histogram::{histogram_fraction, histogram_quantile};

const DELTA: i32 = 1;

/// How one series is reduced over the window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Mode {
    /// The latest point in the window.
    Instant,
    /// The increase over the window (not per second: callers divide by the window seconds).
    Rate,
}

/// Bucket data of one point; `counts` has `bounds.len() + 1` entries (last is `+Inf`).
#[derive(Debug, Clone, PartialEq)]
pub enum HistPoint {
    Explicit {
        bounds: Vec<f64>,
        counts: Vec<u64>,
        sum: Option<f64>,
        count: u64,
    },
    Exp(ExpHistogram, Option<f64>),
}

/// One stored point of a series.
#[derive(Debug, Clone, PartialEq)]
pub struct HistPt {
    pub ts: i64,
    pub start: i64,
    pub temporality: Option<i32>,
    pub h: HistPoint,
}

impl HistPoint {
    pub fn count(&self) -> u64 {
        match self {
            HistPoint::Explicit { count, .. } => *count,
            HistPoint::Exp(h, _) => h.count(),
        }
    }

    pub fn sum(&self) -> Option<f64> {
        match self {
            HistPoint::Explicit { sum, .. } | HistPoint::Exp(_, sum) => *sum,
        }
    }

    /// `self - earlier`; `None` when the shapes differ or any count would go negative.
    fn checked_sub(&self, earlier: &HistPoint) -> Option<HistPoint> {
        let sub_sum = |a: Option<f64>, b: Option<f64>| Some(a? - b?);
        match (self, earlier) {
            (
                HistPoint::Explicit {
                    bounds,
                    counts,
                    sum,
                    count,
                },
                HistPoint::Explicit {
                    bounds: pb,
                    counts: pc,
                    sum: ps,
                    count: pn,
                },
            ) if bounds == pb && counts.len() == pc.len() => Some(HistPoint::Explicit {
                bounds: bounds.clone(),
                counts: counts
                    .iter()
                    .zip(pc)
                    .map(|(a, b)| a.checked_sub(*b))
                    .collect::<Option<_>>()?,
                sum: sub_sum(*sum, *ps),
                count: count.checked_sub(*pn)?,
            }),
            (HistPoint::Exp(a, sa), HistPoint::Exp(b, sb)) => {
                Some(HistPoint::Exp(a.checked_sub(b)?, sub_sum(*sa, *sb)))
            }
            _ => None,
        }
    }
}

fn is_reset(prev: &HistPt, cur: &HistPt) -> bool {
    if prev.start > 0 && cur.start > 0 {
        cur.start > prev.start
    } else {
        cur.h.count() < prev.h.count()
    }
}

/// The increase from `prev` to `cur`: `cur` whole on a reset, `None` (a new baseline) when
/// the layout changed without a new start.
fn step(prev: &HistPt, cur: &HistPt) -> Option<HistPoint> {
    let restarted = prev.start > 0 && cur.start > 0 && cur.start > prev.start;
    if !restarted && !same_shape(&prev.h, &cur.h) {
        return None;
    }
    let diff = (!restarted && !is_reset(prev, cur)).then(|| cur.h.checked_sub(&prev.h));
    Some(diff.flatten().unwrap_or_else(|| cur.h.clone()))
}

fn invalid(msg: &str) -> QuerierError {
    QuerierError::InvalidInput(msg.into())
}

fn merge_two(a: HistPoint, b: &HistPoint) -> Result<HistPoint, QuerierError> {
    let sum = |a: Option<f64>, b: Option<f64>| Some(a? + b?);
    match (a, b) {
        (
            HistPoint::Explicit {
                bounds,
                mut counts,
                sum: sa,
                count,
            },
            HistPoint::Explicit {
                bounds: bb,
                counts: bc,
                sum: sb,
                count: bn,
            },
        ) => {
            let (bounds, counts) = if &bounds == bb && counts.len() == bc.len() {
                for (x, y) in counts.iter_mut().zip(bc) {
                    *x = x.saturating_add(*y);
                }
                (bounds, counts)
            } else {
                merge_bounds(&bounds, &counts, bb, bc)
            };
            Ok(HistPoint::Explicit {
                bounds,
                counts,
                sum: sum(sa, *sb),
                count: count.saturating_add(*bn),
            })
        }
        (HistPoint::Exp(mut a, sa), HistPoint::Exp(b, sb)) => {
            a.merge(b);
            Ok(HistPoint::Exp(a, sum(sa, *sb)))
        }
        _ => Err(invalid(
            "explicit-bucket and exponential histograms cannot be merged",
        )),
    }
}

/// Two explicit layouts on the union of their bounds: at each bound `le`, a
/// histogram contributes its cumulative count at its largest own bound
/// `<= le` (a step function), as Prometheus' `sum by (le)` does; the summed
/// cumulative counts are kept non-decreasing.
fn merge_bounds(ab: &[f64], ac: &[u64], bb: &[f64], bc: &[u64]) -> (Vec<f64>, Vec<u64>) {
    let mut bounds: Vec<f64> = ab.iter().chain(bb).copied().collect();
    bounds.sort_by(f64::total_cmp);
    bounds.dedup();
    let cumulative_at = |own: &[f64], counts: &[u64], le: f64| -> u64 {
        let n = own.partition_point(|&b| b <= le);
        counts[..n].iter().fold(0u64, |a, &c| a.saturating_add(c))
    };
    let total = |counts: &[u64]| counts.iter().fold(0u64, |a, &c| a.saturating_add(c));
    let mut merged = Vec::with_capacity(bounds.len() + 1);
    let mut prev = 0u64;
    for &le in &bounds {
        let cum = cumulative_at(ab, ac, le)
            .saturating_add(cumulative_at(bb, bc, le))
            .max(prev);
        merged.push(cum - prev);
        prev = cum;
    }
    let all = total(ac).saturating_add(total(bc)).max(prev);
    merged.push(all - prev);
    (bounds, merged)
}

/// Merge the values of several series into one histogram (`None` when empty).
pub fn merge_across(values: Vec<HistPoint>) -> Result<Option<HistPoint>, QuerierError> {
    let mut it = values.into_iter();
    let Some(first) = it.next() else {
        return Ok(None);
    };
    it.try_fold(first, |acc, v| merge_two(acc, &v)).map(Some)
}

/// Whether two increments describe the same bucket layout and can be summed.
fn same_shape(a: &HistPoint, b: &HistPoint) -> bool {
    match (a, b) {
        (
            HistPoint::Explicit {
                bounds: x,
                counts: xc,
                ..
            },
            HistPoint::Explicit {
                bounds: y,
                counts: yc,
                ..
            },
        ) => x == y && xc.len() == yc.len(),
        (HistPoint::Exp(..), HistPoint::Exp(..)) => true,
        _ => false,
    }
}

/// Add `inc` into `acc`. A layout change inside one series (new bounds, or explicit
/// to exponential) is a reset boundary: the earlier increments cannot be summed with
/// the new layout, so they are discarded and `inc` starts over.
fn accumulate(acc: Option<HistPoint>, inc: HistPoint) -> Result<HistPoint, QuerierError> {
    match acc {
        Some(a) if same_shape(&a, &inc) => merge_two(a, &inc),
        _ => Ok(inc),
    }
}

/// One series' histogram over `(t - window_ns, t]`; points may be in any order.
///
/// Rate mode is point-wise: a delta point contributes itself, a cumulative point
/// contributes its increase over the previous cumulative point (or in full when
/// its series began inside the window).
pub fn series_value(
    points: &[HistPt],
    mode: Mode,
    t: i64,
    window_ns: i64,
) -> Result<Option<HistPoint>, QuerierError> {
    let lo = t.saturating_sub(window_ns);
    let mut pts: Vec<&HistPt> = points.iter().filter(|p| p.ts > lo && p.ts <= t).collect();
    pts.sort_by_key(|p| (p.ts, p.start));
    let Some(last) = pts.last().copied() else {
        return Ok(None);
    };
    if mode == Mode::Instant {
        return Ok(Some(last.h.clone()));
    }
    let mut acc: Option<HistPoint> = None;
    let mut prev: Option<&HistPt> = None;
    for p in pts {
        let inc = if p.temporality == Some(DELTA) {
            p.h.clone()
        } else {
            let prior = prev.replace(p);
            match prior {
                Some(pr) => match step(pr, p) {
                    Some(inc) => inc,
                    None => {
                        acc = None;
                        continue;
                    }
                },
                None if p.start > 0 && p.start > lo && p.start <= t => p.h.clone(),
                None => continue,
            }
        };
        acc = Some(accumulate(acc, inc)?);
    }
    Ok(acc)
}

pub fn quantile(h: &HistPoint, q: f64) -> f64 {
    match h {
        HistPoint::Explicit { bounds, counts, .. } => {
            histogram_quantile(q, bounds, &to_f64(counts))
        }
        HistPoint::Exp(e, _) => e.quantile(q),
    }
}

fn to_f64(counts: &[u64]) -> Vec<f64> {
    counts.iter().map(|&c| c as f64).collect()
}

/// Fraction of observations in `(lo, hi]`; NaN when the histogram is empty
/// or a bound is NaN, 0 when `lo >= hi`, as in Prometheus.
pub fn fraction(h: &HistPoint, lo: f64, hi: f64) -> f64 {
    if lo.is_nan() || hi.is_nan() {
        return f64::NAN;
    }
    let f = match h {
        HistPoint::Explicit { bounds, counts, .. } => {
            histogram_fraction(lo, hi, bounds, &to_f64(counts))
        }
        HistPoint::Exp(e, _) => {
            let e = e.normalised();
            let total = e.count() as f64;
            if total == 0.0 {
                return f64::NAN;
            }
            (exp_cumulative(&e, hi) - exp_cumulative(&e, lo)) / total
        }
    };
    if lo >= hi && !f.is_nan() { 0.0 } else { f }
}

/// `sum / count`, NaN for an empty histogram; `None` when no sum was recorded.
pub fn avg(h: &HistPoint) -> Option<f64> {
    Some(h.sum()? / h.count() as f64)
}

/// Population variance of an exponential histogram, as Prometheus'
/// `histogram_stdvar` estimates it: the mean is the recorded `sum / count`, and
/// each bucket's observations sit at one representative value, the geometric
/// mean of its bounds (negated for a negative bucket, 0 for the zero bucket).
/// NaN when empty; `None` without a recorded sum or for explicit buckets, which
/// are not native histograms.
pub fn variance(h: &HistPoint) -> Option<f64> {
    let HistPoint::Exp(e, Some(sum)) = h else {
        return None;
    };
    let e = e.normalised();
    let total = e.count() as f64;
    if total == 0.0 {
        return Some(f64::NAN);
    }
    let mean = sum / total;
    let spread = |value: f64, count: u64| {
        let d = value - mean;
        d * d * count as f64
    };
    let mut acc = spread(0.0, e.zero_count);
    for (b, sign) in [(&e.positive, 1.0), (&e.negative, -1.0)] {
        for (k, &c) in b.counts.iter().enumerate() {
            let (l, u) = bounds_at(e.scale, i64::from(b.offset) + k as i64);
            acc += spread(sign * (l * u).sqrt(), c);
        }
    }
    Some(acc / total)
}

/// Square root of [`variance`].
pub fn stddev(h: &HistPoint) -> Option<f64> {
    variance(h).map(f64::sqrt)
}

/// Observations `<= x`: exponential interpolation inside a boundary bucket, linear across zero.
/// `min`/`max` play no part, as in Prometheus (a rate-mode increase has none).
fn exp_cumulative(e: &ExpHistogram, x: f64) -> f64 {
    let zt = e.zero_threshold;
    // Same zero-bucket span as `ExpHistogram::quantile`.
    let any = |b: &Buckets| b.counts.iter().any(|&c| c > 0);
    let (has_neg, has_pos) = (any(&e.negative), any(&e.positive));
    let zero_lo = if has_pos && !has_neg { 0.0 } else { -zt };
    let zero_hi = if has_neg && !has_pos { 0.0 } else { zt };
    let mut segs: Vec<(u64, f64, f64)> = Vec::new();
    for (k, &c) in e.negative.counts.iter().enumerate().rev() {
        let (l, h) = bounds_at(e.scale, i64::from(e.negative.offset) + k as i64);
        segs.push((c, -h, -l));
    }
    segs.push((e.zero_count, zero_lo, zero_hi));
    for (k, &c) in e.positive.counts.iter().enumerate() {
        let (l, h) = bounds_at(e.scale, i64::from(e.positive.offset) + k as i64);
        segs.push((c, l, h));
    }
    segs.into_iter()
        .map(|(c, a, b)| {
            let c = c as f64;
            if x >= b {
                c
            } else if x <= a {
                0.0
            } else if a * b > 0.0 {
                let f = (x.abs().ln() - a.abs().ln()) / (b.abs().ln() - a.abs().ln());
                c * if f.is_nan() { 0.0 } else { f.clamp(0.0, 1.0) }
            } else {
                c * (x - a) / (b - a)
            }
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exp_full(zero: u64, pos: (i32, &[u64]), neg: (i32, &[u64]), sum: Option<f64>) -> HistPoint {
        let h = ExpHistogram {
            scale: 0,
            zero_count: zero,
            positive: Buckets {
                offset: pos.0,
                counts: pos.1.to_vec(),
            },
            negative: Buckets {
                offset: neg.0,
                counts: neg.1.to_vec(),
            },
            ..Default::default()
        };
        HistPoint::Exp(h, sum)
    }

    const SQ2: f64 = std::f64::consts::SQRT_2;

    #[test]
    fn avg_is_sum_over_count_for_both_layouts() {
        assert_eq!(avg(&ex(&[2, 2], 6.0)), Some(1.5));
        assert_eq!(avg(&eb(&[1, 1, 0, 0], 3.0)), Some(1.5));
        assert!(avg(&ex(&[0, 0], 0.0)).unwrap().is_nan());
        assert_eq!(avg(&exp_full(0, (0, &[2]), (0, &[]), None)), None);
    }

    #[test]
    fn variance_uses_geometric_bucket_midpoints() {
        // (1, 2] and (2, 4] hold two observations each: representatives are
        // sqrt(2) and 2 sqrt(2); the reported sum 6 gives mean 1.5.
        let want = ((SQ2 - 1.5).powi(2) + (2.0 * SQ2 - 1.5).powi(2)) / 2.0;
        let h = ex(&[2, 2], 6.0);
        assert!((variance(&h).unwrap() - want).abs() < 1e-12);
        assert!((stddev(&h).unwrap() - want.sqrt()).abs() < 1e-12);
    }

    #[test]
    fn variance_counts_the_zero_bucket_as_zero_and_negatives_as_negative() {
        // Two zeros, one in (2, 4] (-> 2 sqrt 2), one in [-2, -1) (-> -sqrt 2); sum 0.
        let h = exp_full(2, (1, &[1]), (0, &[1]), Some(0.0));
        let want = (8.0 + 2.0) / 4.0;
        assert!((variance(&h).unwrap() - want).abs() < 1e-12);
        assert!((stddev(&h).unwrap() - want.sqrt()).abs() < 1e-12);
        // A mean of 1 shifts every delta: (0-1)^2*2 + (2 sqrt 2 - 1)^2 + (-sqrt 2 - 1)^2.
        let h = exp_full(2, (1, &[1]), (0, &[1]), Some(4.0));
        let want = (2.0 + (2.0 * SQ2 - 1.0).powi(2) + (SQ2 + 1.0).powi(2)) / 4.0;
        assert!((variance(&h).unwrap() - want).abs() < 1e-12);
    }

    #[test]
    fn variance_edge_cases() {
        assert!(variance(&ex(&[0, 0], 0.0)).unwrap().is_nan());
        assert_eq!(variance(&exp_full(0, (0, &[2]), (0, &[]), None)), None);
        // Explicit-bucket histograms carry no native-histogram sample.
        assert_eq!(variance(&eb(&[1, 1, 0, 0], 3.0)), None);
        assert_eq!(stddev(&eb(&[1, 1, 0, 0], 3.0)), None);
        // One bucket's worth of identical observations at its representative: no spread.
        let h = ex(&[0, 3], 3.0 * 2.0 * SQ2);
        assert!(variance(&h).unwrap().abs() < 1e-12);
    }

    const S: i64 = 1_000_000_000;

    fn ex(counts: &[u64], sum: f64) -> HistPoint {
        let h = ExpHistogram {
            scale: 0,
            positive: Buckets {
                offset: 0,
                counts: counts.to_vec(),
            },
            ..Default::default()
        };
        HistPoint::Exp(h, Some(sum))
    }

    fn eb(counts: &[u64], sum: f64) -> HistPoint {
        HistPoint::Explicit {
            bounds: vec![1.0, 2.0, 4.0],
            counts: counts.to_vec(),
            sum: Some(sum),
            count: counts.iter().sum(),
        }
    }

    fn pt(ts: i64, start: i64, temporality: i32, h: HistPoint) -> HistPt {
        HistPt {
            ts: ts * S,
            start: start * S,
            temporality: Some(temporality),
            h,
        }
    }

    fn val(pts: &[HistPt], mode: Mode, t: i64, w: i64) -> Option<HistPoint> {
        series_value(pts, mode, t * S, w * S).unwrap()
    }

    #[test]
    fn cumulative_rate_is_latest_minus_previous() {
        let pts = [
            pt(120, 5, 2, eb(&[1, 2, 3, 0], 10.0)),
            pt(150, 5, 2, eb(&[2, 4, 6, 1], 25.0)),
        ];
        assert_eq!(
            val(&pts, Mode::Rate, 150, 60),
            Some(eb(&[1, 2, 3, 1], 15.0))
        );
        // Only the baseline in the window.
        assert_eq!(val(&pts[..1], Mode::Rate, 150, 60), None);
    }

    #[test]
    fn start_inside_window_counts_first_point_in_full() {
        let pts = [pt(110, 100, 2, eb(&[1, 1, 0, 0], 3.0))];
        assert_eq!(val(&pts, Mode::Rate, 120, 60), Some(eb(&[1, 1, 0, 0], 3.0)));
    }

    #[test]
    fn start_moving_or_count_decrease_is_a_reset() {
        let pts = [
            pt(110, 5, 2, eb(&[5, 5, 0, 0], 10.0)),
            pt(120, 115, 2, eb(&[1, 0, 0, 0], 1.0)),
        ];
        assert_eq!(val(&pts, Mode::Rate, 130, 60), Some(eb(&[1, 0, 0, 0], 1.0)));
        let unknown = [
            pt(110, 0, 2, eb(&[5, 5, 0, 0], 10.0)),
            pt(120, 0, 2, eb(&[1, 0, 0, 0], 1.0)),
        ];
        assert_eq!(
            val(&unknown, Mode::Rate, 130, 60),
            Some(eb(&[1, 0, 0, 0], 1.0))
        );
    }

    #[test]
    fn delta_sums_and_instant_takes_latest() {
        let pts = [
            pt(20, 0, 1, eb(&[0, 2, 0, 0], 3.0)),
            pt(10, 0, 1, eb(&[1, 0, 0, 0], 1.0)),
        ];
        assert_eq!(val(&pts, Mode::Rate, 30, 60), Some(eb(&[1, 2, 0, 0], 4.0)));
        assert_eq!(
            val(&pts, Mode::Instant, 30, 60),
            Some(eb(&[0, 2, 0, 0], 3.0))
        );
        assert_eq!(val(&pts, Mode::Instant, 30, 5), None);
    }

    #[test]
    fn merge_across_series_and_mismatch() {
        let m = merge_across(vec![eb(&[1, 0, 0, 0], 1.0), eb(&[0, 2, 0, 0], 3.0)]).unwrap();
        assert_eq!(m, Some(eb(&[1, 2, 0, 0], 4.0)));
        let mixed = merge_across(vec![eb(&[1, 0, 0, 0], 1.0), ex(&[1], 1.0)]);
        assert!(matches!(mixed, Err(QuerierError::InvalidInput(_))));
        assert_eq!(merge_across(vec![]).unwrap(), None);
    }

    /// Different bounds merge on their union, each series read as a step
    /// function of its own cumulative counts (Prometheus' `sum by (le)`).
    #[test]
    fn explicit_bounds_merge_on_their_union() {
        let a = eb(&[1, 1, 1, 1], 2.0);
        let b = HistPoint::Explicit {
            bounds: vec![1.0, 5.0],
            counts: vec![2, 2, 2],
            sum: Some(3.0),
            count: 6,
        };
        let want = HistPoint::Explicit {
            bounds: vec![1.0, 2.0, 4.0, 5.0],
            counts: vec![3, 1, 1, 2, 3],
            sum: Some(5.0),
            count: 10,
        };
        assert_eq!(
            merge_across(vec![a.clone(), b.clone()]).unwrap(),
            Some(want.clone())
        );
        assert_eq!(merge_across(vec![b, a]).unwrap(), Some(want));
    }

    #[test]
    fn exp_rate_delta_and_quantile() {
        let pts = [
            pt(120, 5, 2, ex(&[1, 1], 3.0)),
            pt(150, 5, 2, ex(&[1, 3], 9.0)),
        ];
        let d = val(&pts, Mode::Rate, 150, 60).unwrap();
        assert_eq!(d.count(), 2);
        assert_eq!(d.sum(), Some(6.0));
        // Both observations sit in bucket (2, 4]; the median interpolates inside it.
        let q = quantile(&d, 0.5);
        assert!(q > 2.0 && q < 4.0, "{q}");
    }

    #[test]
    fn fractions() {
        let h = eb(&[2, 2, 0, 0], 1.0);
        assert_eq!(fraction(&h, 0.0, 1.0), 0.5);
        assert!(quantile(&h, 0.5).is_finite());
        // Bucket 0 is (1, 2], bucket 1 is (2, 4]: 1 observation each.
        let e = ex(&[0, 1, 1], 1.0);
        assert!((fraction(&e, 1.0, 2.0) - 0.0).abs() < 1e-12);
        assert!((fraction(&e, 2.0, 4.0) - 0.5).abs() < 1e-12);
        assert!((fraction(&e, 0.0, 100.0) - 1.0).abs() < 1e-12);
        let mid = fraction(&e, 2.0, 2f64.powf(1.5));
        assert!((mid - 0.25).abs() < 1e-9, "{mid}");
    }

    /// PromQL passes these bounds through; Prometheus' `BucketFraction` and
    /// `HistogramFraction` treat them this way.
    #[test]
    fn fraction_bounds_follow_prometheus() {
        // Bounds [1, 2, 4], counts [1, 2, 3, 4] incl. +Inf: 10 observations.
        let h = eb(&[1, 2, 3, 4], 1.0);
        // `+Inf` includes the +Inf bucket: (2, +Inf] holds 3 + 4 of 10.
        assert_eq!(fraction(&h, 2.0, f64::INFINITY), 0.7);
        assert_eq!(fraction(&h, f64::NEG_INFINITY, f64::INFINITY), 1.0);
        let below_zero = HistPoint::Explicit {
            bounds: vec![-5.0, 1.0],
            counts: vec![1, 1, 0],
            sum: None,
            count: 2,
        };
        // `-Inf` includes the (-Inf, -5] bucket.
        assert_eq!(fraction(&below_zero, f64::NEG_INFINITY, 1.0), 1.0);
        let e = ex(&[1, 1], 1.0);
        for h in [&h, &e] {
            assert_eq!(fraction(h, 3.0, 2.0), 0.0);
            assert!(fraction(h, f64::NAN, 2.0).is_nan());
            assert!(fraction(h, 0.0, f64::NAN).is_nan());
        }
    }

    #[test]
    fn start_after_the_instant_is_not_a_new_series() {
        let pts = [pt(110, 125, 2, eb(&[1, 0, 0, 0], 1.0))];
        assert_eq!(val(&pts, Mode::Rate, 120, 60), None);
    }

    #[test]
    fn exp_fraction_respects_zero_threshold() {
        let h = ExpHistogram {
            zero_count: 2,
            zero_threshold: 0.5,
            positive: Buckets {
                offset: 0,
                counts: vec![0, 2],
            },
            ..Default::default()
        };
        let p = HistPoint::Exp(h, None);
        // Half the observations are at or below the zero threshold.
        assert!((fraction(&p, -1.0, 0.5) - 0.5).abs() < 1e-12);
        assert!(fraction(&p, f64::NEG_INFINITY, f64::INFINITY).is_finite());
    }

    /// Prometheus never cuts the interpolation off at a point's min/max, and
    /// a rate-mode increase has none, so instant and rate mode would disagree.
    #[test]
    fn exp_fraction_ignores_min_max() {
        let plain = ExpHistogram {
            zero_count: 2,
            zero_threshold: 0.5,
            positive: Buckets {
                offset: 0,
                counts: vec![0, 2],
            },
            ..Default::default()
        };
        let bounded = ExpHistogram {
            min: Some(2.5),
            max: Some(3.0),
            ..plain.clone()
        };
        let (plain, bounded) = (HistPoint::Exp(plain, None), HistPoint::Exp(bounded, None));
        // (3, 100] takes the part of (2, 4] above 3: 2·(1 - log2 1.5) of 4.
        let want = 2.0 * (1.0 - 1.5f64.log2()) / 4.0;
        assert!((fraction(&bounded, 3.0, 100.0) - want).abs() < 1e-12);
        for (lo, hi) in [(3.0, 100.0), (0.0, 2.5), (2.0, 4.0)] {
            assert_eq!(
                fraction(&bounded, lo, hi),
                fraction(&plain, lo, hi),
                "({lo}, {hi}]"
            );
        }
    }

    #[test]
    fn exp_fraction_uses_the_quantile_zero_span() {
        let h = ExpHistogram {
            zero_count: 2,
            zero_threshold: 1.0,
            positive: Buckets {
                offset: 0,
                counts: vec![2],
            },
            ..Default::default()
        };
        let q = h.quantile(0.25);
        let f = fraction(&HistPoint::Exp(h, None), f64::NEG_INFINITY, q);
        assert!((f - 0.25).abs() < 1e-12, "{f}");
    }

    #[test]
    fn layout_change_discards_earlier_increments() {
        let wide = |c: &[u64]| HistPoint::Explicit {
            bounds: vec![1.0, 2.0, 4.0, 8.0],
            counts: c.to_vec(),
            sum: None,
            count: c.iter().sum(),
        };
        let pts = [
            pt(100, 5, 2, eb(&[1, 0, 0, 0], 1.0)),
            pt(110, 5, 2, eb(&[2, 0, 0, 0], 2.0)),
            pt(120, 5, 2, wide(&[3, 0, 0, 0, 0])),
            pt(130, 5, 2, wide(&[4, 1, 0, 0, 0])),
        ];
        // The layout change at 120 is a new baseline (contributes 0) and discards the
        // 100 -> 110 increment, which cannot merge with it; only 120 -> 130 counts.
        assert_eq!(val(&pts, Mode::Rate, 130, 60), Some(wide(&[1, 1, 0, 0, 0])));
    }

    #[test]
    fn mixed_temporality_is_pointwise() {
        let pts = [
            pt(100, 5, 2, eb(&[1, 0, 0, 0], 1.0)),
            pt(110, 5, 2, eb(&[3, 0, 0, 0], 3.0)),
            pt(120, 0, 1, eb(&[0, 4, 0, 0], 4.0)),
        ];
        assert_eq!(val(&pts, Mode::Rate, 130, 60), Some(eb(&[2, 4, 0, 0], 6.0)));
    }

    #[test]
    fn layout_change_then_comparable_increment() {
        let wide = |c: &[u64]| HistPoint::Explicit {
            bounds: vec![1.0, 2.0, 4.0, 8.0],
            counts: c.to_vec(),
            sum: None,
            count: c.iter().sum(),
        };
        let pts = [
            pt(120, 5, 2, eb(&[10, 0, 0, 0], 1.0)),
            pt(130, 5, 2, wide(&[12, 0, 0, 0, 0])),
            pt(140, 5, 2, wide(&[15, 0, 0, 0, 0])),
        ];
        assert_eq!(val(&pts, Mode::Rate, 140, 50), Some(wide(&[3, 0, 0, 0, 0])));
        // A start moving forward together with the layout change is a genuine reset.
        let reset = [
            pt(120, 5, 2, eb(&[10, 0, 0, 0], 1.0)),
            pt(130, 125, 2, wide(&[12, 0, 0, 0, 0])),
        ];
        assert_eq!(
            val(&reset, Mode::Rate, 140, 50),
            Some(wide(&[12, 0, 0, 0, 0]))
        );
    }
}
