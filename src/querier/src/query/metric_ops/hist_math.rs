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
            if &bounds != bb || counts.len() != bc.len() {
                return Err(invalid(
                    "histograms with different bucket bounds cannot be merged; group by the series attributes that distinguish them",
                ));
            }
            for (x, y) in counts.iter_mut().zip(bc) {
                *x = x.saturating_add(*y);
            }
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

/// Fraction of observations in `(lo, hi]`; NaN when the histogram is empty.
pub fn fraction(h: &HistPoint, lo: f64, hi: f64) -> f64 {
    match h {
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
    }
}

/// Observations `<= x`: exponential interpolation inside a boundary bucket, linear across zero.
fn exp_cumulative(e: &ExpHistogram, x: f64) -> f64 {
    if e.max.is_some_and(|m| x >= m) {
        return e.count() as f64;
    }
    if e.min.is_some_and(|m| x < m) {
        return 0.0;
    }
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
        let other = HistPoint::Explicit {
            bounds: vec![1.0, 5.0],
            counts: vec![1, 0, 0],
            sum: None,
            count: 1,
        };
        let err = merge_across(vec![eb(&[1, 0, 0, 0], 1.0), other]).unwrap_err();
        assert!(
            matches!(err, QuerierError::InvalidInput(m) if m.contains("different bucket bounds"))
        );
        let mixed = merge_across(vec![eb(&[1, 0, 0, 0], 1.0), ex(&[1], 1.0)]);
        assert!(matches!(mixed, Err(QuerierError::InvalidInput(_))));
        assert_eq!(merge_across(vec![]).unwrap(), None);
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

    #[test]
    fn start_after_the_instant_is_not_a_new_series() {
        let pts = [pt(110, 125, 2, eb(&[1, 0, 0, 0], 1.0))];
        assert_eq!(val(&pts, Mode::Rate, 120, 60), None);
    }

    #[test]
    fn exp_fraction_respects_zero_threshold_and_min_max() {
        let mut h = ExpHistogram {
            zero_count: 2,
            zero_threshold: 0.5,
            positive: Buckets {
                offset: 0,
                counts: vec![0, 2],
            },
            ..Default::default()
        };
        h.max = Some(3.0);
        let p = HistPoint::Exp(h, None);
        // Half the observations are at or below the zero threshold.
        assert!((fraction(&p, -1.0, 0.5) - 0.5).abs() < 1e-12);
        // Nothing lies above max.
        assert!((fraction(&p, 3.0, 100.0) - 0.0).abs() < 1e-12);
        assert!(fraction(&p, f64::NEG_INFINITY, f64::INFINITY).is_finite());
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
