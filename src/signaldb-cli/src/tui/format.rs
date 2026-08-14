//! Shared formatting helpers for TUI widgets and components.

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
}
