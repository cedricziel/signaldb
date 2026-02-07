//! Sparkline wrapper with f64-to-u64 scaling for metrics visualization.
//!
//! Ratatui's [`Sparkline`] widget requires `&[u64]` data. This module provides
//! [`scale_to_u64`] to proportionally map arbitrary `f64` values into the
//! `0..=100` range suitable for sparkline rendering.

/// Scale a slice of `f64` values proportionally into the `0..=100` `u64` range.
///
/// # Edge cases
///
/// - **Empty input** returns an empty `Vec`.
/// - **All identical values** returns a flat line at 50.
/// - **Negative values** are handled correctly — the minimum is shifted to 0.
///
/// # Example
///
/// ```ignore
/// let scaled = scale_to_u64(&[10.0, 20.0, 30.0, 40.0, 50.0]);
/// assert_eq!(scaled, vec![0, 25, 50, 75, 100]);
/// ```
pub fn scale_to_u64(values: &[f64]) -> Vec<u64> {
    if values.is_empty() {
        return Vec::new();
    }

    let min = values.iter().copied().fold(f64::INFINITY, f64::min);
    let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);

    let range = max - min;

    if range == 0.0 || !range.is_finite() {
        // All values are the same (or NaN/Inf edge case) — flat line at 50.
        return vec![50; values.len()];
    }

    values
        .iter()
        .map(|&v| {
            let normalized = (v - min) / range; // 0.0 .. 1.0
            (normalized * 100.0).round() as u64
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_returns_empty() {
        assert_eq!(scale_to_u64(&[]), Vec::<u64>::new());
    }

    #[test]
    fn single_value_returns_flat_50() {
        assert_eq!(scale_to_u64(&[42.0]), vec![50]);
    }

    #[test]
    fn all_same_values_return_flat_50() {
        assert_eq!(scale_to_u64(&[7.0, 7.0, 7.0]), vec![50, 50, 50]);
    }

    #[test]
    fn linear_ramp_scales_correctly() {
        let scaled = scale_to_u64(&[0.0, 25.0, 50.0, 75.0, 100.0]);
        assert_eq!(scaled, vec![0, 25, 50, 75, 100]);
    }

    #[test]
    fn negative_values_shift_to_zero() {
        let scaled = scale_to_u64(&[-50.0, 0.0, 50.0]);
        assert_eq!(scaled, vec![0, 50, 100]);
    }

    #[test]
    fn fractional_values_round() {
        let scaled = scale_to_u64(&[0.0, 0.333, 0.666, 1.0]);
        // 0 -> 0, 0.333 -> 33, 0.666 -> 67, 1.0 -> 100
        assert_eq!(scaled, vec![0, 33, 67, 100]);
    }

    #[test]
    fn two_values_min_and_max() {
        let scaled = scale_to_u64(&[10.0, 20.0]);
        assert_eq!(scaled, vec![0, 100]);
    }

    #[test]
    fn large_values_scale_correctly() {
        let scaled = scale_to_u64(&[1_000_000.0, 2_000_000.0, 3_000_000.0]);
        assert_eq!(scaled, vec![0, 50, 100]);
    }

    #[test]
    fn nan_values_return_flat() {
        let scaled = scale_to_u64(&[f64::NAN, f64::NAN]);
        assert_eq!(scaled, vec![50, 50]);
    }
}
