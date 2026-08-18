//! Shared formatting and Arrow-value-extraction helpers for TUI widgets and
//! components.

use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, UInt64Array};

/// Render a duration in milliseconds as a human-readable string, choosing
/// microseconds, milliseconds, or seconds depending on magnitude.
pub(crate) fn format_duration_ms(duration_ms: f64) -> String {
    if duration_ms < 1.0 {
        format!("{:.0}us", duration_ms * 1000.0)
    } else if duration_ms < 1000.0 {
        format!("{duration_ms:.1}ms")
    } else {
        format!("{:.2}s", duration_ms / 1000.0)
    }
}

/// Read a numeric Arrow column value as `f64`, returning `None` for nulls
/// or unsupported array types. Accepts `Float64`, `Int64`, and `UInt64`
/// arrays, which cover the metric/dashboard columns the TUI queries.
pub(crate) fn extract_f64(col: &Arc<dyn Array>, row: usize) -> Option<f64> {
    if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
        if arr.is_null(row) {
            None
        } else {
            Some(arr.value(row))
        }
    } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        if arr.is_null(row) {
            None
        } else {
            Some(arr.value(row) as f64)
        }
    } else if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
        if arr.is_null(row) {
            None
        } else {
            Some(arr.value(row) as f64)
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_duration_us() {
        assert_eq!(format_duration_ms(0.5), "500us");
    }

    #[test]
    fn format_duration_ms_range() {
        assert_eq!(format_duration_ms(42.0), "42.0ms");
    }

    #[test]
    fn format_duration_seconds() {
        assert_eq!(format_duration_ms(2500.0), "2.50s");
    }

    #[test]
    fn extract_f64_reads_float64_int64_and_uint64() {
        let floats: Arc<dyn Array> = Arc::new(Float64Array::from(vec![Some(1.5), None]));
        assert_eq!(extract_f64(&floats, 0), Some(1.5));
        assert_eq!(extract_f64(&floats, 1), None);

        let ints: Arc<dyn Array> = Arc::new(Int64Array::from(vec![Some(3), None]));
        assert_eq!(extract_f64(&ints, 0), Some(3.0));
        assert_eq!(extract_f64(&ints, 1), None);

        let uints: Arc<dyn Array> = Arc::new(UInt64Array::from(vec![Some(7), None]));
        assert_eq!(extract_f64(&uints, 0), Some(7.0));
        assert_eq!(extract_f64(&uints, 1), None);
    }

    #[test]
    fn extract_f64_returns_none_for_unsupported_array_type() {
        let strings: Arc<dyn Array> = Arc::new(arrow::array::StringArray::from(vec!["x"]));
        assert_eq!(extract_f64(&strings, 0), None);
    }
}
