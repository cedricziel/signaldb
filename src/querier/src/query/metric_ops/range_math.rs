//! Range-function math (rate, increase, resets, `*_over_time`) over the points of one series.

use std::hash::{Hash, Hasher};

use crate::query::error::QuerierError;

pub const NS_PER_SEC: f64 = 1e9;
const DELTA: i32 = 1;

/// A windowed function over the points of one series at one evaluation instant.
#[derive(Debug, Clone, Copy)]
pub enum RangeFn {
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
    QuantileOverTime(f64),
}

impl PartialEq for RangeFn {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (RangeFn::QuantileOverTime(a), RangeFn::QuantileOverTime(b)) => {
                a.to_bits() == b.to_bits()
            }
            _ => std::mem::discriminant(self) == std::mem::discriminant(other),
        }
    }
}

impl Eq for RangeFn {}

impl Hash for RangeFn {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        if let RangeFn::QuantileOverTime(q) = self {
            q.to_bits().hash(state);
        }
    }
}

impl RangeFn {
    pub fn name(&self) -> &'static str {
        match self {
            RangeFn::Latest => "latest",
            RangeFn::Rate => "rate",
            RangeFn::Increase => "increase",
            RangeFn::Irate => "irate",
            RangeFn::Delta => "delta",
            RangeFn::Idelta => "idelta",
            RangeFn::Deriv => "deriv",
            RangeFn::Resets => "resets",
            RangeFn::Changes => "changes",
            RangeFn::AvgOverTime => "avg_over_time",
            RangeFn::MinOverTime => "min_over_time",
            RangeFn::MaxOverTime => "max_over_time",
            RangeFn::SumOverTime => "sum_over_time",
            RangeFn::CountOverTime => "count_over_time",
            RangeFn::LastOverTime => "last_over_time",
            RangeFn::StddevOverTime => "stddev_over_time",
            RangeFn::StdvarOverTime => "stdvar_over_time",
            RangeFn::PresentOverTime => "present_over_time",
            RangeFn::QuantileOverTime(_) => "quantile_over_time",
        }
    }

    fn needs_counter(&self) -> bool {
        matches!(self, RangeFn::Rate | RangeFn::Increase | RangeFn::Irate)
    }
}

/// One sample of a series. `temporality` and `monotonic` are read from the
/// latest point of the window.
#[derive(Debug, Clone, Copy)]
pub struct Pt {
    pub ts: i64,
    pub v: f64,
    pub start: i64,
    pub temporality: Option<i32>,
    pub monotonic: Option<bool>,
}

/// A cumulative counter restarted between two consecutive points.
fn is_reset(prev: &Pt, cur: &Pt) -> bool {
    (prev.start > 0 && cur.start > 0 && cur.start > prev.start) || cur.v < prev.v
}

fn step(prev: &Pt, cur: &Pt) -> f64 {
    if is_reset(prev, cur) {
        cur.v
    } else {
        cur.v - prev.v
    }
}

/// Evaluate `f` at instant `t` over the points of one series (any order).
/// `kind` is the OTLP metric type; counter functions reject gauges and
/// non-monotonic sums with `InvalidInput`.
pub fn eval_points(
    f: RangeFn,
    points: &[Pt],
    kind: Option<&str>,
    t: i64,
    window_ns: i64,
) -> Result<Option<f64>, QuerierError> {
    let lo = t.saturating_sub(window_ns);
    let mut pts: Vec<Pt> = points
        .iter()
        .filter(|p| p.ts > lo && p.ts <= t)
        .copied()
        .collect();
    pts.sort_by(|a, b| {
        (a.ts, a.start)
            .cmp(&(b.ts, b.start))
            .then(a.v.total_cmp(&b.v))
    });
    let (Some(first), Some(last)) = (pts.first().copied(), pts.last().copied()) else {
        return Ok(None);
    };
    if f.needs_counter() {
        let what = match (kind, last.monotonic) {
            (Some("gauge"), _) => Some("gauge"),
            (Some("sum"), Some(false)) => Some("non-monotonic sum"),
            _ => None,
        };
        if let Some(what) = what {
            return Err(QuerierError::InvalidInput(format!(
                "{} is not defined on {what} metrics; use delta or deriv",
                f.name()
            )));
        }
    }
    let is_delta = last.temporality == Some(DELTA);
    let n = pts.len();
    let window_secs = window_ns as f64 / NS_PER_SEC;
    let vals = || pts.iter().map(|p| p.v);
    let prev = (n >= 2).then(|| pts[n - 2]);
    // Pointwise temporality: a delta point adds its value; a cumulative one
    // adds its step from the previous cumulative point, or its whole value
    // when it is the first and its series began inside the window.
    let increase = || -> Option<f64> {
        let mut total = None;
        let mut prev: Option<&Pt> = None;
        for p in &pts {
            let inc = if p.temporality == Some(DELTA) {
                p.v
            } else {
                match prev.replace(p) {
                    Some(pr) => step(pr, p),
                    None if p.start > 0 && p.start > lo && p.start <= t => p.v,
                    None => continue,
                }
            };
            *total.get_or_insert(0.0) += inc;
        }
        total
    };
    Ok(match f {
        RangeFn::Latest | RangeFn::LastOverTime => Some(last.v),
        RangeFn::Increase => increase(),
        RangeFn::Rate => increase().map(|i| i / window_secs),
        RangeFn::Irate => prev.and_then(|p| {
            let dt = last.ts.saturating_sub(p.ts) as f64 / NS_PER_SEC;
            let d = if is_delta { last.v } else { step(&p, &last) };
            (dt > 0.0).then(|| d / dt)
        }),
        RangeFn::Delta => prev.map(|_| last.v - first.v),
        RangeFn::Idelta => prev.map(|p| last.v - p.v),
        RangeFn::Deriv => deriv(&pts),
        RangeFn::Resets if is_delta => Some(0.0),
        RangeFn::Resets => Some(pts.windows(2).filter(|w| is_reset(&w[0], &w[1])).count() as f64),
        RangeFn::Changes => Some(
            pts.windows(2)
                .filter(|w| w[0].v != w[1].v && !(w[0].v.is_nan() && w[1].v.is_nan()))
                .count() as f64,
        ),
        RangeFn::AvgOverTime => Some(vals().sum::<f64>() / n as f64),
        RangeFn::MinOverTime => vals().reduce(f64::min),
        RangeFn::MaxOverTime => vals().reduce(f64::max),
        RangeFn::SumOverTime => Some(vals().sum()),
        RangeFn::CountOverTime => Some(n as f64),
        RangeFn::PresentOverTime => Some(1.0),
        RangeFn::StdvarOverTime => Some(variance(vals(), n)),
        RangeFn::StddevOverTime => Some(variance(vals(), n).sqrt()),
        RangeFn::QuantileOverTime(q) => Some(quantile(q, vals().collect())),
    })
}

fn variance(vals: impl Iterator<Item = f64> + Clone, n: usize) -> f64 {
    let mean = vals.clone().sum::<f64>() / n as f64;
    vals.map(|v| (v - mean).powi(2)).sum::<f64>() / n as f64
}

pub(crate) fn quantile(q: f64, mut vals: Vec<f64>) -> f64 {
    if q.is_nan() {
        return f64::NAN;
    }
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    // Prometheus ranks NaN below every number: NaNs first, then the rest.
    let mut nans = 0;
    for i in 0..vals.len() {
        if vals[i].is_nan() {
            vals.swap(i, nans);
            nans += 1;
        }
    }
    vals[nans..].sort_unstable_by(f64::total_cmp);
    // Prometheus's interpolation, so an infinite bound stays infinite.
    let rank = q * (vals.len() - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = (lo + 1).min(vals.len() - 1);
    let weight = rank - rank.floor();
    vals[lo] * (1.0 - weight) + vals[hi] * weight
}

/// Least-squares slope in units per second.
fn deriv(pts: &[Pt]) -> Option<f64> {
    if pts.len() < 2 {
        return None;
    }
    let t0 = pts[0].ts;
    let n = pts.len() as f64;
    let xs = |p: &Pt| (p.ts - t0) as f64 / NS_PER_SEC;
    let (sx, sy) = pts
        .iter()
        .fold((0.0, 0.0), |(a, b), p| (a + xs(p), b + p.v));
    let (mx, my) = (sx / n, sy / n);
    let sxx: f64 = pts.iter().map(|p| (xs(p) - mx).powi(2)).sum();
    let sxy: f64 = pts.iter().map(|p| (xs(p) - mx) * (p.v - my)).sum();
    (sxx > 0.0).then(|| sxy / sxx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantile_ranks_nan_lowest_as_prometheus_does() {
        assert_eq!(quantile(1.0, vec![1.0, f64::NAN, 2.0]), 2.0);
        assert_eq!(quantile(0.5, vec![3.0, f64::NAN, 1.0]), 1.0);
        assert!(quantile(0.0, vec![1.0, -f64::NAN, 2.0]).is_nan());
    }

    #[test]
    fn quantile_interpolates_as_prometheus_does() {
        assert_eq!(
            quantile(0.0, vec![f64::NEG_INFINITY, 1.0]),
            f64::NEG_INFINITY
        );
        assert_eq!(quantile(1.0, vec![1.0, 2.0]), 2.0);
        assert_eq!(quantile(0.25, vec![1.0, 2.0, 3.0]), 1.5);
    }

    const S: i64 = 1_000_000_000;

    #[derive(Clone, Copy)]
    struct Meta {
        temporality: Option<i32>,
        monotonic: Option<bool>,
        kind: &'static str,
    }

    const CUM: Meta = Meta {
        temporality: Some(2),
        monotonic: Some(true),
        kind: "sum",
    };
    const DEL: Meta = Meta {
        temporality: Some(1),
        ..CUM
    };
    const GAUGE: Meta = Meta {
        temporality: None,
        monotonic: None,
        kind: "gauge",
    };
    const NON_MONO: Meta = Meta {
        monotonic: Some(false),
        ..CUM
    };

    /// `(ts_s, value, start_s)` points, evaluated at `t` seconds; input is reversed to
    /// exercise sorting.
    fn run(
        f: RangeFn,
        m: Meta,
        pts: &[(i64, f64, i64)],
        t: i64,
        w: i64,
    ) -> Result<Option<f64>, QuerierError> {
        let pts: Vec<Pt> = pts
            .iter()
            .rev()
            .map(|&(ts, v, start)| Pt {
                ts: ts * S,
                v,
                start: start * S,
                temporality: m.temporality,
                monotonic: m.monotonic,
            })
            .collect();
        eval_points(f, &pts, Some(m.kind), t * S, w * S)
    }

    fn eval(f: RangeFn, vals: &[(i64, f64)], t: i64, w: i64) -> Option<f64> {
        let pts: Vec<_> = vals.iter().map(|&(ts, v)| (ts, v, 0)).collect();
        run(f, CUM, &pts, t, w).unwrap()
    }

    type Case = (RangeFn, Meta, Vec<(i64, f64, i64)>, i64, i64, Option<f64>);

    #[test]
    fn resets_on_delta_temporality_is_zero() {
        let pts = [(10, 5.0, 0), (20, 1.0, 0)];
        assert_eq!(run(RangeFn::Resets, DEL, &pts, 30, 60).unwrap(), Some(0.0));
        assert_eq!(run(RangeFn::Resets, CUM, &pts, 30, 60).unwrap(), Some(1.0));
    }

    #[test]
    fn counter_rules() {
        use RangeFn::*;
        let ramp = vec![(121, 0.0, 5), (150, 60.0, 5), (180, 120.0, 5)];
        let moved = vec![(110, 10.0, 5), (120, 20.0, 5), (130, 25.0, 125)];
        let unknown = vec![(10, 50.0, 0), (20, 60.0, 0), (30, 5.0, 0), (40, 8.0, 0)];
        let cases: Vec<Case> = vec![
            (Increase, CUM, ramp.clone(), 180, 60, Some(120.0)),
            (Rate, CUM, ramp, 180, 60, Some(2.0)),
            (
                Increase,
                DEL,
                vec![(10, 3.0, 0), (20, 4.0, 0), (30, 5.0, 0)],
                30,
                60,
                Some(12.0),
            ),
            // reset seen only in start_timestamp, value does not decrease
            (Increase, CUM, moved.clone(), 140, 60, Some(35.0)),
            (Resets, CUM, moved, 140, 60, Some(1.0)),
            // first point began inside the window counts in full
            (
                Increase,
                CUM,
                vec![(110, 7.0, 100), (120, 10.0, 100)],
                120,
                60,
                Some(10.0),
            ),
            // start exactly at the (exclusive) window start is the baseline; at t it began inside
            (
                Increase,
                CUM,
                vec![(110, 7.0, 60), (120, 10.0, 60)],
                120,
                60,
                Some(3.0),
            ),
            (Increase, CUM, vec![(120, 7.0, 120)], 120, 60, Some(7.0)),
            (
                Increase,
                CUM,
                vec![(110, 7.0, 5), (120, 10.0, 5)],
                120,
                60,
                Some(3.0),
            ),
            (Increase, CUM, vec![(110, 7.0, 5)], 120, 60, None),
            // equal starts but a lower value: still a reset
            (
                Increase,
                CUM,
                vec![(110, 20.0, 5), (120, 5.0, 5)],
                120,
                60,
                Some(5.0),
            ),
            (
                Resets,
                CUM,
                vec![(110, 20.0, 5), (120, 5.0, 5)],
                120,
                60,
                Some(1.0),
            ),
            // a start after the instant is not a series that began in the window
            (
                Increase,
                CUM,
                vec![(110, 7.0, 130), (120, 10.0, 130)],
                120,
                60,
                Some(3.0),
            ),
            (Increase, CUM, unknown.clone(), 40, 60, Some(18.0)),
            (Resets, CUM, unknown, 40, 60, Some(1.0)),
            (
                Irate,
                CUM,
                vec![(10, 10.0, 0), (20, 20.0, 0), (30, 5.0, 0), (40, 9.0, 0)],
                40,
                60,
                Some(0.4),
            ),
            (
                Irate,
                DEL,
                vec![(10, 3.0, 0), (20, 5.0, 0)],
                20,
                60,
                Some(0.5),
            ),
            (Irate, CUM, vec![(10, 10.0, 0)], 40, 60, None),
            // duplicate timestamps order by (start, value), not by arrival
            (
                Increase,
                CUM,
                vec![(110, 5.0, 5), (120, 9.0, 5), (120, 7.0, 5)],
                120,
                60,
                Some(4.0),
            ),
        ];
        for (f, m, pts, t, w, want) in cases {
            assert_eq!(run(f, m, &pts, t, w).unwrap(), want, "{f:?} {pts:?}");
        }
    }

    #[test]
    fn counter_functions_reject_gauges_and_non_monotonic_sums() {
        for f in [RangeFn::Rate, RangeFn::Increase, RangeFn::Irate] {
            let msg = run(f, GAUGE, &[(10, 1.0, 0)], 30, 60)
                .unwrap_err()
                .to_string();
            assert!(
                msg.contains("gauge") && msg.contains("delta or deriv"),
                "{msg}"
            );
            let err = run(f, NON_MONO, &[(10, 1.0, 0)], 30, 60).unwrap_err();
            assert!(
                matches!(&err, QuerierError::InvalidInput(m) if m.contains("non-monotonic sum"))
            );
        }
        let pts = [(10, 1.0, 0), (20, 5.0, 0)];
        assert_eq!(run(RangeFn::Delta, GAUGE, &pts, 30, 60).unwrap(), Some(4.0));
        assert_eq!(run(RangeFn::Deriv, GAUGE, &pts, 30, 60).unwrap(), Some(0.4));
    }

    #[test]
    fn temporality_is_pointwise() {
        let mk = |ts, v, temporality| Pt {
            ts: ts * S,
            v,
            start: 0,
            temporality,
            monotonic: Some(true),
        };
        let inc =
            |pts: &[Pt]| eval_points(RangeFn::Increase, pts, Some("sum"), 40 * S, 60 * S).unwrap();
        // The cumulative 3 is a baseline; the delta 4 counts.
        assert_eq!(
            inc(&[mk(10, 3.0, Some(2)), mk(20, 4.0, Some(1))]),
            Some(4.0)
        );
        // Cumulative points difference against each other across a delta point.
        let mixed = [
            mk(10, 3.0, Some(2)),
            mk(20, 4.0, Some(1)),
            mk(30, 5.0, Some(2)),
        ];
        assert_eq!(inc(&mixed), Some(6.0));
    }

    #[test]
    fn idelta_deriv_changes() {
        let v = [(10, 10.0), (20, 20.0), (30, 5.0), (40, 9.0)];
        assert_eq!(eval(RangeFn::Idelta, &v, 40, 60), Some(4.0));
        assert_eq!(eval(RangeFn::Delta, &v, 40, 60), Some(-1.0));
        assert_eq!(eval(RangeFn::Changes, &v, 40, 60), Some(3.0));
        assert_eq!(
            eval(RangeFn::Changes, &[(1, f64::NAN), (2, f64::NAN)], 40, 60),
            Some(0.0)
        );
        assert_eq!(
            eval(RangeFn::Deriv, &[(0, 1.0), (10, 3.0), (20, 5.0)], 20, 60),
            Some(0.2)
        );
        assert_eq!(eval(RangeFn::Deriv, &v[..1], 40, 60), None);
    }

    #[test]
    fn over_time_functions() {
        let v = [(10, 2.0), (20, 4.0), (30, 4.0), (40, 6.0)];
        let f = |f| eval(f, &v, 40, 60);
        assert_eq!(f(RangeFn::AvgOverTime), Some(4.0));
        assert_eq!(f(RangeFn::MinOverTime), Some(2.0));
        assert_eq!(f(RangeFn::MaxOverTime), Some(6.0));
        assert_eq!(f(RangeFn::SumOverTime), Some(16.0));
        assert_eq!(f(RangeFn::CountOverTime), Some(4.0));
        assert_eq!(f(RangeFn::LastOverTime), Some(6.0));
        assert_eq!(f(RangeFn::PresentOverTime), Some(1.0));
        assert_eq!(f(RangeFn::StdvarOverTime), Some(2.0));
        assert_eq!(f(RangeFn::StddevOverTime), Some(2f64.sqrt()));
        assert_eq!(f(RangeFn::QuantileOverTime(0.5)), Some(4.0));
        assert_eq!(f(RangeFn::QuantileOverTime(0.25)), Some(3.5));
        assert_eq!(f(RangeFn::QuantileOverTime(2.0)), Some(f64::INFINITY));
    }

    #[test]
    fn latest_takes_newest_point_in_window() {
        let v = [(10, 1.0), (100, 2.0), (250, 3.0)];
        assert_eq!(eval(RangeFn::Latest, &v, 240, 300), Some(2.0));
        assert_eq!(eval(RangeFn::Latest, &v, 250, 300), Some(3.0));
        assert_eq!(eval(RangeFn::Latest, &v, 310, 300), Some(3.0));
        assert_eq!(eval(RangeFn::Latest, &v, 310, 60), None);
    }

    #[test]
    fn window_is_open_at_the_start_and_closed_at_the_end() {
        let v = [(60, 1.0), (120, 2.0)];
        assert_eq!(eval(RangeFn::CountOverTime, &v, 120, 60), Some(1.0));
        assert_eq!(eval(RangeFn::CountOverTime, &v, 120, 61), Some(2.0));
    }
}
