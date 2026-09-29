//! # OTel exponential-histogram math
//!
//! At `scale`, `base = 2^(2^-scale)` and positive bucket `i` covers
//! `(base^i, base^(i+1)]`; negative buckets mirror it for values below zero.
//! `Buckets::offset` is the index of `counts[0]`.

#![cfg_attr(
    not(test),
    expect(dead_code, reason = "consumed by the UDF wiring in a follow-up")
)]

/// Largest dense bucket span the math will allocate.
const MAX_SPAN: usize = 1 << 16;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Buckets {
    pub offset: i32,
    pub counts: Vec<u64>,
}

/// A single exponential-histogram point. `min`/`max` are `None` when unknown.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ExpHistogram {
    pub scale: i32,
    pub zero_count: u64,
    pub zero_threshold: f64,
    pub positive: Buckets,
    pub negative: Buckets,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

pub(super) fn bounds_at(scale: i32, index: i64) -> (f64, f64) {
    let step = 2f64.powi(scale.saturating_neg());
    (
        (index as f64 * step).exp2(),
        ((index + 1) as f64 * step).exp2(),
    )
}

type Bound = (Option<f64>, u64);

fn merge_bound((a, an): Bound, (b, bn): Bound, pick: fn(f64, f64) -> f64) -> Option<f64> {
    match (an > 0, bn > 0) {
        (true, true) => Some(pick(a?, b?)),
        (true, false) => a,
        (false, true) => b,
        (false, false) => None,
    }
}

impl Buckets {
    /// `None` for a negative count or a span too long for `MAX_SPAN` / `i32`.
    pub fn from_stored(offset: i32, counts: &[i64]) -> Option<Self> {
        if counts.len() > MAX_SPAN || i64::from(offset) + counts.len() as i64 > i64::from(i32::MAX)
        {
            return None;
        }
        let counts = counts
            .iter()
            .map(|&c| u64::try_from(c).ok())
            .collect::<Option<Vec<_>>>()?;
        Some(Self { offset, counts })
    }

    fn end(&self) -> i64 {
        i64::from(self.offset) + self.counts.len() as i64
    }

    fn downscale(&mut self, by: u32) {
        let by = by.min(31);
        if by == 0 || self.counts.is_empty() {
            self.offset >>= by;
            return;
        }
        let first = i64::from(self.offset) >> by;
        let last = (self.end() - 1) >> by;
        let mut merged = vec![0u64; (last - first + 1) as usize];
        for (k, &c) in self.counts.iter().enumerate() {
            let slot = ((i64::from(self.offset) + k as i64) >> by) - first;
            let dst = &mut merged[slot as usize];
            *dst = dst.saturating_add(c);
        }
        self.offset = first as i32;
        self.counts = merged;
    }

    /// Drops leading buckets starting below `threshold`; returns their count
    /// and the largest populated upper bound.
    fn fold_within(&mut self, scale: i32, threshold: f64) -> (u64, f64) {
        let (mut folded, mut upper, mut n) = (0u64, threshold, 0);
        for (k, &c) in self.counts.iter().enumerate() {
            let (lo, hi) = bounds_at(scale, i64::from(self.offset) + k as i64);
            if lo >= threshold {
                break;
            }
            if c > 0 {
                folded = folded.saturating_add(c);
                upper = upper.max(hi);
            }
            n = k + 1;
        }
        self.counts.drain(..n);
        self.offset += n as i32;
        (folded, upper)
    }

    fn checked_sub(&self, earlier: &Buckets) -> Option<Buckets> {
        let mut out = self.clone();
        for (k, &c) in earlier.counts.iter().enumerate() {
            if c == 0 {
                continue;
            }
            let slot = i64::from(earlier.offset) + k as i64 - i64::from(self.offset);
            let dst = out.counts.get_mut(usize::try_from(slot).ok()?)?;
            *dst = dst.checked_sub(c)?;
        }
        Some(out)
    }

    fn add(&mut self, other: &Buckets) {
        if other.counts.is_empty() {
            return;
        }
        let (lo, hi) = union_range(self, other);
        let mut merged = vec![0u64; (hi - lo) as usize];
        for b in [&*self, other] {
            for (k, &c) in b.counts.iter().enumerate() {
                let dst = &mut merged[(i64::from(b.offset) + k as i64 - lo) as usize];
                *dst = dst.saturating_add(c);
            }
        }
        self.offset = lo as i32;
        self.counts = merged;
    }
}

/// `[lo, hi)` index range covering the non-empty bucket lists.
fn union_range(a: &Buckets, b: &Buckets) -> (i64, i64) {
    let (lo, hi) = [a, b]
        .iter()
        .filter(|x| !x.counts.is_empty())
        .fold((i64::MAX, i64::MIN), |(lo, hi), x| {
            (lo.min(i64::from(x.offset)), hi.max(x.end()))
        });
    if lo > hi { (0, 0) } else { (lo, hi) }
}

fn union_span(a: &Buckets, b: &Buckets) -> usize {
    let (lo, hi) = union_range(a, b);
    (hi - lo) as usize
}

impl ExpHistogram {
    /// `None` for a scale outside [-10, 20], a negative count, or a threshold
    /// that is not finite and non-negative.
    pub fn from_stored(
        scale: i32,
        zero_count: i64,
        zero_threshold: f64,
        positive: Buckets,
        negative: Buckets,
        min: Option<f64>,
        max: Option<f64>,
    ) -> Option<Self> {
        if !(-10..=20).contains(&scale) || !zero_threshold.is_finite() || zero_threshold < 0.0 {
            return None;
        }
        Some(Self {
            scale,
            zero_count: u64::try_from(zero_count).ok()?,
            zero_threshold,
            positive,
            negative,
            min,
            max,
        })
    }

    pub fn count(&self) -> u64 {
        self.positive
            .counts
            .iter()
            .chain(&self.negative.counts)
            .fold(self.zero_count, |a, &c| a.saturating_add(c))
    }

    pub fn downscale(&mut self, by: u32) {
        let by = by.min(31);
        self.positive.downscale(by);
        self.negative.downscale(by);
        self.scale = self.scale.saturating_sub(by as i32);
    }

    fn downscale_to(&mut self, scale: i32) {
        while self.scale > scale {
            self.downscale((i64::from(self.scale) - i64::from(scale)).min(31) as u32);
        }
    }

    /// Folds buckets reaching into `[-t, t]` (`t` = max of `threshold` and the
    /// current one) into zero; a `t` inside a populated bucket rises to its bound.
    fn widen_zero(&mut self, threshold: f64) {
        let t = threshold.max(self.zero_threshold);
        let (pc, pu) = self.positive.fold_within(self.scale, t);
        let (nc, nu) = self.negative.fold_within(self.scale, t);
        self.zero_count = self.zero_count.saturating_add(pc).saturating_add(nc);
        self.zero_threshold = pu.max(nu);
    }

    /// A copy with the zero bucket widened over any bucket inside its threshold.
    pub(super) fn normalised(&self) -> ExpHistogram {
        let mut h = self.clone();
        h.widen_zero(0.0);
        h
    }

    fn aligned(&self, other: &ExpHistogram) -> (ExpHistogram, ExpHistogram) {
        let scale = self.scale.min(other.scale);
        let (mut a, mut b) = (self.clone(), other.clone());
        a.downscale_to(scale);
        b.downscale_to(scale);
        while union_span(&a.positive, &b.positive).max(union_span(&a.negative, &b.negative))
            > MAX_SPAN
        {
            a.downscale(1);
            b.downscale(1);
        }
        for _ in 0..2 {
            let t = a.zero_threshold.max(b.zero_threshold);
            a.widen_zero(t);
            b.widen_zero(t);
        }
        (a, b)
    }

    pub fn merge(&mut self, other: &ExpHistogram) {
        let (mut a, b) = self.aligned(other);
        a.zero_count = a.zero_count.saturating_add(b.zero_count);
        a.positive.add(&b.positive);
        a.negative.add(&b.negative);
        let (sn, on) = (self.count(), other.count());
        a.min = merge_bound((self.min, sn), (other.min, on), f64::min);
        a.max = merge_bound((self.max, sn), (other.max, on), f64::max);
        *self = a;
    }

    /// Cumulative delta `self - earlier`, `None` when any count would go
    /// negative (a reset). The result has no `min`/`max`.
    pub fn checked_sub(&self, earlier: &ExpHistogram) -> Option<ExpHistogram> {
        let (a, b) = self.aligned(earlier);
        Some(ExpHistogram {
            zero_count: a.zero_count.checked_sub(b.zero_count)?,
            positive: a.positive.checked_sub(&b.positive)?,
            negative: a.negative.checked_sub(&b.negative)?,
            min: None,
            max: None,
            ..a
        })
    }

    pub fn quantile(&self, q: f64) -> f64 {
        if q.is_nan() {
            return f64::NAN;
        }
        if !(0.0..=1.0).contains(&q) {
            return if q < 0.0 {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            };
        }
        let mut h = self.clone();
        h.widen_zero(0.0);
        let total = h.count();
        if total == 0 {
            return f64::NAN;
        }
        let rank = q * total as f64;

        let has_neg = h.negative.counts.iter().any(|&c| c > 0);
        let has_pos = h.positive.counts.iter().any(|&c| c > 0);
        let zt = h.zero_threshold;
        let zero_lo = if has_pos && !has_neg { 0.0 } else { -zt };
        let zero_hi = if has_neg && !has_pos { 0.0 } else { zt };

        // (count, value at fraction 0, value at fraction 1), ascending in value.
        let mut segments = Vec::new();
        for (k, &c) in h.negative.counts.iter().enumerate().rev() {
            let (lo, hi) = bounds_at(h.scale, i64::from(h.negative.offset) + k as i64);
            segments.push((c, -hi, -lo));
        }
        segments.push((h.zero_count, zero_lo, zero_hi));
        for (k, &c) in h.positive.counts.iter().enumerate() {
            let (lo, hi) = bounds_at(h.scale, i64::from(h.positive.offset) + k as i64);
            segments.push((c, lo, hi));
        }

        let mut seen = 0.0;
        let mut value = f64::NAN;
        for (c, a, b) in segments.into_iter().filter(|s| s.0 > 0) {
            let c = c as f64;
            value = interpolate(a, b, ((rank - seen) / c).clamp(0.0, 1.0));
            if rank <= seen + c {
                break;
            }
            seen += c;
        }
        value
            .max(self.min.unwrap_or(f64::NEG_INFINITY))
            .min(self.max.unwrap_or(f64::INFINITY))
    }
}

fn interpolate(a: f64, b: f64, f: f64) -> f64 {
    if a.is_finite() && b.is_finite() && a * b > 0.0 {
        let (la, lb) = (a.abs().log2(), b.abs().log2());
        let v = a.signum() * (la + (lb - la) * f).exp2();
        if v.is_finite() {
            return v;
        }
    }
    a + (b - a) * f
}

#[cfg(test)]
mod tests {
    use super::*;

    const NONE: (i32, &[u64]) = (0, &[]);

    fn hist(scale: i32, pos: (i32, &[u64]), neg: (i32, &[u64])) -> ExpHistogram {
        ExpHistogram {
            scale,
            positive: buckets(pos.0, pos.1),
            negative: buckets(neg.0, neg.1),
            ..Default::default()
        }
    }

    fn zero(mut h: ExpHistogram, count: u64, threshold: f64) -> ExpHistogram {
        h.zero_count = count;
        h.zero_threshold = threshold;
        h
    }

    fn buckets(offset: i32, counts: &[u64]) -> Buckets {
        let counts = counts.to_vec();
        Buckets { offset, counts }
    }

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9 * b.abs().max(1.0)
    }

    #[test]
    fn bounds_scale_0_and_3() {
        assert_eq!(bounds_at(0, 1), (2.0, 4.0));
        let (lo, hi) = bounds_at(3, 8);
        assert!(close(lo, 2.0) && close(hi, 2f64.powf(9.0 / 8.0)));
    }

    #[test]
    fn quantile_cases() {
        let single = hist(0, (1, &[10]), NONE);
        let only_zero = zero(hist(0, NONE, NONE), 4, 2.0);
        let zero_pos = zero(hist(0, (1, &[4]), NONE), 4, 2.0);
        let zero_neg = zero(hist(0, NONE, (1, &[4])), 4, 2.0);
        let cases = [
            (&single, 0.5, 2.0 * 2f64.sqrt()),
            (&single, 1.0, 4.0),
            (&single, 0.0, 2.0),
            (&only_zero, 0.75, 1.0),
            (&zero_pos, 0.1, 0.4),
            (&zero_neg, 0.9, -0.4),
            (&zero(hist(0, NONE, NONE), 3, 0.0), 0.3, 0.0),
        ];
        for (h, q, want) in cases {
            assert!(close(h.quantile(q), want), "q={q}: {}", h.quantile(q));
        }
    }

    #[test]
    fn quantile_is_monotone() {
        let mixed = zero(hist(1, (-2, &[3, 1, 2]), (0, &[2, 5])), 3, 0.1);
        let cut = zero(hist(0, (0, &[6, 2]), NONE), 4, 1.5);
        for h in [mixed, cut] {
            let mut prev = f64::NEG_INFINITY;
            for i in 0..=100 {
                let v = h.quantile(f64::from(i) / 100.0);
                assert!(v >= prev, "q={i}: {v} < {prev}");
                prev = v;
            }
        }
    }

    #[test]
    fn min_max_clamp_and_range() {
        let h = ExpHistogram {
            min: Some(2.5),
            max: Some(3.0),
            ..hist(0, (1, &[10]), NONE)
        };
        assert_eq!((h.quantile(0.0), h.quantile(1.0)), (2.5, 3.0));
        assert!(hist(0, NONE, NONE).quantile(0.5).is_nan());
        let h = hist(0, (1, &[1]), NONE);
        assert_eq!(h.quantile(-0.1), f64::NEG_INFINITY);
        assert_eq!(h.quantile(1.1), f64::INFINITY);
        assert!(h.quantile(f64::NAN).is_nan());
    }

    #[test]
    fn downscale_merges_buckets() {
        let mut h = hist(2, (-3, &[1, 2, 3, 4]), (-1, &[5, 6]));
        h.downscale(1);
        assert_eq!(h.scale, 1);
        assert_eq!(h.positive, buckets(-2, &[1, 5, 4]));
        h.downscale(1);
        assert_eq!(h.scale, 0);
        assert_eq!(h.positive, buckets(-1, &[6, 4]));
        assert_eq!(h.negative, buckets(-1, &[5, 6]));
    }

    #[test]
    fn merge_different_scales() {
        let mut a = hist(2, (-3, &[1, 2, 3]), (0, &[1]));
        a.min = Some(0.5);
        a.max = Some(3.0);
        let mut b = hist(1, (0, &[4, 5]), (-1, &[2]));
        b.max = Some(9.0);
        a.merge(&b);
        assert_eq!(a.scale, 1);
        assert_eq!(a.positive, buckets(-2, &[1, 5, 4, 5]));
        assert_eq!(a.negative, buckets(-1, &[2, 1]));
        assert_eq!((a.min, a.max), (None, Some(9.0)));

        let mut c = hist(0, (0, &[1]), NONE);
        c.min = Some(1.5);
        c.merge(&hist(0, NONE, NONE));
        assert_eq!(c.min, Some(1.5));
    }

    #[test]
    fn merge_zero_threshold_folding() {
        let mut a = zero(hist(0, (0, &[1, 2, 4]), (0, &[3])), 1, 0.0);
        a.merge(&zero(hist(0, (2, &[5]), NONE), 0, 3.0));
        assert_eq!((a.zero_count, a.zero_threshold), (7, 4.0));
        assert_eq!(a.positive, buckets(2, &[9]));
        assert_eq!(a.count(), 16);
        let mut a = zero(hist(0, (1, &[2, 5]), NONE), 1, 3.0);
        a.merge(&zero(hist(0, (1, &[1, 1]), NONE), 2, 3.0));
        assert_eq!((a.zero_count, a.zero_threshold), (6, 4.0));
        assert_eq!(a.positive, buckets(2, &[6]));
        let mut a = hist(0, (0, &[0, 7]), NONE);
        a.merge(&zero(hist(0, NONE, NONE), 0, 1.5));
        assert_eq!((a.zero_count, a.zero_threshold), (0, 1.5));
        assert_eq!(a.positive, buckets(1, &[7]));
    }

    #[test]
    fn merge_bounds_span_and_equalises_scale() {
        let mut a = hist(0, (-40_000, &[1]), NONE);
        a.merge(&hist(0, (40_000, &[1]), NONE));
        assert!(a.count() == 2 && a.positive.counts.len() <= MAX_SPAN && a.scale < 0);

        let mut a = hist(100, (5, &[1]), NONE);
        a.merge(&hist(0, (1, &[1]), NONE));
        assert_eq!((a.scale, a.count()), (0, 2));
    }

    #[test]
    fn checked_sub_delta_and_reset() {
        let later = zero(hist(1, (0, &[5, 6, 7]), (0, &[2])), 4, 0.0);
        let mut earlier = zero(hist(1, (1, &[2, 3]), (0, &[1])), 1, 0.0);
        let d = later.checked_sub(&earlier).expect("delta");
        assert_eq!(d.positive.counts, vec![5, 4, 4]);
        assert_eq!((d.negative.counts, d.zero_count), (vec![1], 3));
        assert_eq!((d.min, d.max), (None, None));

        earlier.positive.counts[0] = 7;
        assert!(later.checked_sub(&earlier).is_none());
        assert!(later.checked_sub(&hist(1, (5, &[1]), NONE)).is_none());
    }

    #[test]
    fn from_stored_validates() {
        let bad = [
            Buckets::from_stored(2, &[1, -3]),
            Buckets::from_stored(i32::MAX, &[1, 1]),
        ];
        assert!(bad.iter().all(Option::is_none));
        let hs = |scale, zc, zt| {
            let e = Buckets::default;
            ExpHistogram::from_stored(scale, zc, zt, e(), e(), None, None).is_some()
        };
        assert!(hs(20, 1, 0.0) && hs(-10, 0, 0.5));
        assert!(!hs(21, 1, 0.0) && !hs(-11, 1, 0.0));
        assert!(!hs(0, -1, 0.0) && !hs(0, 0, f64::NAN));
    }
}
