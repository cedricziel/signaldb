//! Waterfall widget for trace span visualization.
//!
//! Renders spans as horizontal bars using Unicode block characters,
//! positioned by start time relative to trace start, with indentation
//! by depth in the span hierarchy.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};

use crate::tui::client::models::SpanInfo;

/// Colors cycled through for different service names.
const SERVICE_COLORS: &[Color] = &[
    Color::Cyan,
    Color::Magenta,
    Color::Green,
    Color::Yellow,
    Color::Blue,
    Color::Red,
    Color::LightCyan,
    Color::LightMagenta,
];

/// A span entry prepared for waterfall rendering.
#[derive(Debug, Clone)]
pub struct WaterfallSpan {
    /// Display label (operation name).
    pub label: String,
    /// Service name (used for color coding).
    pub service: String,
    /// Depth in the span hierarchy (0 = root).
    pub depth: usize,
    /// Start time as fraction of total trace duration (0.0 - 1.0).
    pub start_fraction: f64,
    /// Duration as fraction of total trace duration (0.0 - 1.0).
    pub duration_fraction: f64,
    /// Duration in milliseconds (for label).
    pub duration_ms: f64,
    /// Status code string.
    pub status: String,
}

/// Build waterfall spans from a list of `SpanInfo`, computing depth via parent relationships.
pub fn build_waterfall_spans(spans: &[SpanInfo]) -> Vec<WaterfallSpan> {
    if spans.is_empty() {
        return Vec::new();
    }

    let trace_duration_ms = spans
        .iter()
        .map(|s| s.start_time_ms + s.duration_ms)
        .fold(0.0f64, f64::max);
    let trace_duration_ms = if trace_duration_ms <= 0.0 {
        1.0
    } else {
        trace_duration_ms
    };

    // Build parent -> depth map
    let mut depth_map: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

    // First pass: find roots (no parent)
    for span in spans {
        if span.parent_span_id.is_none() {
            depth_map.insert(span.span_id.clone(), 0);
        }
    }

    // Multi-pass to resolve depths (handles out-of-order spans)
    for _ in 0..spans.len() {
        let mut changed = false;
        for span in spans {
            if depth_map.contains_key(&span.span_id) {
                continue;
            }
            if let Some(parent_id) = &span.parent_span_id
                && let Some(&parent_depth) = depth_map.get(parent_id)
            {
                depth_map.insert(span.span_id.clone(), parent_depth + 1);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    spans
        .iter()
        .map(|span| {
            let depth = depth_map.get(&span.span_id).copied().unwrap_or(0);
            let start_fraction = span.start_time_ms / trace_duration_ms;
            let duration_fraction = span.duration_ms / trace_duration_ms;

            WaterfallSpan {
                label: span.operation.clone(),
                service: span.service.clone(),
                depth,
                start_fraction: start_fraction.clamp(0.0, 1.0),
                duration_fraction: duration_fraction.clamp(0.0, 1.0),
                duration_ms: span.duration_ms,
                status: span.status.clone(),
            }
        })
        .collect()
}

/// Get a color for a service name by hashing it into the color palette.
fn service_color(service: &str) -> Color {
    let hash: usize = service.bytes().map(|b| b as usize).sum();
    SERVICE_COLORS[hash % SERVICE_COLORS.len()]
}

/// Render the waterfall visualization into the given area.
///
/// `selected` is the index of the currently highlighted span.
pub fn render_waterfall(
    frame: &mut Frame,
    area: Rect,
    spans: &[WaterfallSpan],
    selected: Option<usize>,
    scroll_offset: usize,
) {
    let block = Block::default()
        .title(" Waterfall ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue));

    if spans.is_empty() {
        let empty = Paragraph::new("No spans to display")
            .style(Style::default().fg(Color::DarkGray))
            .block(block);
        frame.render_widget(empty, area);
        return;
    }

    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.height == 0 || inner.width < 10 {
        return;
    }

    let visible_rows = inner.height as usize;
    let total_width = inner.width as usize;
    let label_width = (total_width * 30 / 100).clamp(10, 30);
    let duration_col_width = 10; // e.g. " 100.0ms" + margin
    let bar_width = total_width
        .saturating_sub(label_width)
        .saturating_sub(duration_col_width)
        .saturating_sub(1); // separator space

    let mut lines = Vec::new();

    for (i, span) in spans.iter().enumerate().skip(scroll_offset) {
        if lines.len() >= visible_rows {
            break;
        }

        let is_selected = selected == Some(i);
        let color = service_color(&span.service);

        // Build label with indentation
        let indent = "  ".repeat(span.depth.min(5));
        let status_indicator = if span.status.contains("Error") || span.status.contains("ERROR") {
            "! "
        } else {
            ""
        };
        let raw_label = format!("{indent}{status_indicator}{}", span.label);
        let label: String = if raw_label.len() > label_width {
            format!("{}~", &raw_label[..label_width - 1])
        } else {
            format!("{:<width$}", raw_label, width = label_width)
        };

        // Build the bar
        let bar_start = (span.start_fraction * bar_width as f64) as usize;
        let bar_len = ((span.duration_fraction * bar_width as f64) as usize).max(1);
        let bar_end = (bar_start + bar_len).min(bar_width);

        let duration_label = format_duration(span.duration_ms);

        let mut bar_chars = String::new();
        for pos in 0..bar_width {
            if pos >= bar_start && pos < bar_end {
                bar_chars.push('\u{2588}'); // Full block █
            } else {
                bar_chars.push(' ');
            }
        }

        let label_style = if is_selected {
            Style::default()
                .fg(Color::White)
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::White)
        };

        let bar_style = if is_selected {
            Style::default().fg(color).bg(Color::DarkGray)
        } else {
            Style::default().fg(color)
        };

        let dur_style = if is_selected {
            Style::default()
                .fg(Color::Gray)
                .bg(Color::DarkGray)
                .add_modifier(Modifier::DIM)
        } else {
            Style::default().fg(Color::Gray).add_modifier(Modifier::DIM)
        };

        let line = Line::from(vec![
            Span::styled(label, label_style),
            Span::raw(" "),
            Span::styled(bar_chars, bar_style),
            Span::styled(format!(" {duration_label}"), dur_style),
        ]);

        lines.push(line);
    }

    let paragraph = Paragraph::new(lines);
    frame.render_widget(paragraph, inner);
}

/// Format duration in milliseconds to a human-readable string.
fn format_duration(ms: f64) -> String {
    if ms < 1.0 {
        format!("{:.0}us", ms * 1000.0)
    } else if ms < 1000.0 {
        format!("{:.1}ms", ms)
    } else {
        format!("{:.2}s", ms / 1000.0)
    }
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::tui::test_helpers::assert_buffer_contains;

    fn make_spans() -> Vec<SpanInfo> {
        vec![
            SpanInfo {
                span_id: "span-1".into(),
                parent_span_id: None,
                operation: "GET /api/users".into(),
                service: "frontend".into(),
                start_time_ms: 0.0,
                duration_ms: 100.0,
                status: "Ok".into(),
                attributes: serde_json::json!({}),
            },
            SpanInfo {
                span_id: "span-2".into(),
                parent_span_id: Some("span-1".into()),
                operation: "SELECT users".into(),
                service: "backend".into(),
                start_time_ms: 10.0,
                duration_ms: 50.0,
                status: "Ok".into(),
                attributes: serde_json::json!({}),
            },
            SpanInfo {
                span_id: "span-3".into(),
                parent_span_id: Some("span-2".into()),
                operation: "db.query".into(),
                service: "database".into(),
                start_time_ms: 15.0,
                duration_ms: 30.0,
                status: "Error".into(),
                attributes: serde_json::json!({"error": true}),
            },
        ]
    }

    #[test]
    fn build_waterfall_spans_empty() {
        let result = build_waterfall_spans(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn build_waterfall_spans_computes_depth() {
        let spans = make_spans();
        let waterfall = build_waterfall_spans(&spans);
        assert_eq!(waterfall.len(), 3);
        assert_eq!(waterfall[0].depth, 0); // root
        assert_eq!(waterfall[1].depth, 1); // child of root
        assert_eq!(waterfall[2].depth, 2); // grandchild
    }

    #[test]
    fn build_waterfall_spans_computes_fractions() {
        let spans = make_spans();
        let waterfall = build_waterfall_spans(&spans);
        // trace_duration = max(0+100, 10+50, 15+30) = 100
        assert!((waterfall[0].start_fraction - 0.0).abs() < 0.01);
        assert!((waterfall[0].duration_fraction - 1.0).abs() < 0.01);
        assert!((waterfall[1].start_fraction - 0.1).abs() < 0.01);
        assert!((waterfall[1].duration_fraction - 0.5).abs() < 0.01);
    }

    #[test]
    fn format_duration_us() {
        assert_eq!(format_duration(0.5), "500us");
    }

    #[test]
    fn format_duration_ms() {
        assert_eq!(format_duration(42.0), "42.0ms");
    }

    #[test]
    fn format_duration_seconds() {
        assert_eq!(format_duration(2500.0), "2.50s");
    }

    #[test]
    fn service_color_deterministic() {
        let c1 = service_color("frontend");
        let c2 = service_color("frontend");
        assert_eq!(c1, c2);
    }

    #[test]
    fn service_color_varies() {
        let c1 = service_color("frontend");
        let c2 = service_color("backend");
        // Different services should (usually) get different colors
        let _ = (c1, c2); // Just check no panic
    }

    #[test]
    fn render_waterfall_empty() {
        let mut terminal = Terminal::new(TestBackend::new(80, 10)).unwrap();
        terminal
            .draw(|frame| render_waterfall(frame, frame.area(), &[], None, 0))
            .unwrap();
        assert_buffer_contains(&terminal, "No spans to display");
    }

    #[test]
    fn render_waterfall_with_spans() {
        let mut terminal = Terminal::new(TestBackend::new(100, 10)).unwrap();
        let spans = make_spans();
        let waterfall = build_waterfall_spans(&spans);
        terminal
            .draw(|frame| render_waterfall(frame, frame.area(), &waterfall, Some(0), 0))
            .unwrap();
        assert_buffer_contains(&terminal, "GET /api/users");
        assert_buffer_contains(&terminal, "SELECT users");
        assert_buffer_contains(&terminal, "db.query");
    }

    #[test]
    fn render_waterfall_shows_error_indicator() {
        let mut terminal = Terminal::new(TestBackend::new(100, 10)).unwrap();
        let spans = make_spans();
        let waterfall = build_waterfall_spans(&spans);
        terminal
            .draw(|frame| render_waterfall(frame, frame.area(), &waterfall, None, 0))
            .unwrap();
        assert_buffer_contains(&terminal, "!");
    }

    #[test]
    fn render_waterfall_shows_duration() {
        let mut terminal = Terminal::new(TestBackend::new(140, 10)).unwrap();
        let spans = make_spans();
        let waterfall = build_waterfall_spans(&spans);
        terminal
            .draw(|frame| render_waterfall(frame, frame.area(), &waterfall, None, 0))
            .unwrap();
        assert_buffer_contains(&terminal, "100.0ms");
        assert_buffer_contains(&terminal, "50.0ms");
        assert_buffer_contains(&terminal, "30.0ms");
    }

    #[test]
    fn snapshot_waterfall_with_spans() {
        let mut terminal = Terminal::new(TestBackend::new(100, 10)).unwrap();
        let spans = make_spans();
        let waterfall = build_waterfall_spans(&spans);
        terminal
            .draw(|frame| render_waterfall(frame, frame.area(), &waterfall, Some(1), 0))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("waterfall_with_spans", content);
    }

    #[test]
    fn snapshot_waterfall_empty() {
        let mut terminal = Terminal::new(TestBackend::new(60, 6)).unwrap();
        terminal
            .draw(|frame| render_waterfall(frame, frame.area(), &[], None, 0))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("waterfall_empty", content);
    }
}
