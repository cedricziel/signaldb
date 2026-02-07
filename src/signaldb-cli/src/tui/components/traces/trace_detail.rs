//! Trace detail view: waterfall visualization + JSON attribute viewer.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use tui_tree_widget::TreeState;

use crate::tui::client::models::{SpanInfo, TraceDetail};
use crate::tui::widgets::json_viewer::render_json_tree;
use crate::tui::widgets::waterfall::{WaterfallSpan, build_waterfall_spans, render_waterfall};

/// Focus within the detail view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DetailFocus {
    Waterfall,
    JsonViewer,
}

/// Two-section trace detail: waterfall (top) + JSON attributes (bottom).
pub struct TraceDetailView {
    pub detail: Option<TraceDetail>,
    pub waterfall_spans: Vec<WaterfallSpan>,
    pub selected_span: usize,
    pub scroll_offset: usize,
    pub focus: DetailFocus,
    pub json_state: TreeState<String>,
}

impl TraceDetailView {
    pub fn new() -> Self {
        Self {
            detail: None,
            waterfall_spans: Vec::new(),
            selected_span: 0,
            scroll_offset: 0,
            focus: DetailFocus::Waterfall,
            json_state: TreeState::default(),
        }
    }

    pub fn set_detail(&mut self, detail: TraceDetail) {
        self.waterfall_spans = build_waterfall_spans(&detail.spans);
        self.detail = Some(detail);
        self.selected_span = 0;
        self.scroll_offset = 0;
    }

    pub fn clear(&mut self) {
        self.detail = None;
        self.waterfall_spans.clear();
        self.selected_span = 0;
        self.scroll_offset = 0;
    }

    pub fn select_prev_span(&mut self) {
        if self.waterfall_spans.is_empty() {
            return;
        }
        if self.selected_span > 0 {
            self.selected_span -= 1;
            if self.selected_span < self.scroll_offset {
                self.scroll_offset = self.selected_span;
            }
        }
    }

    pub fn select_next_span(&mut self, visible_height: usize) {
        if self.waterfall_spans.is_empty() {
            return;
        }
        if self.selected_span < self.waterfall_spans.len() - 1 {
            self.selected_span += 1;
            if self.selected_span >= self.scroll_offset + visible_height {
                self.scroll_offset = self.selected_span.saturating_sub(visible_height - 1);
            }
        }
    }

    pub fn toggle_focus(&mut self) {
        self.focus = match self.focus {
            DetailFocus::Waterfall => DetailFocus::JsonViewer,
            DetailFocus::JsonViewer => DetailFocus::Waterfall,
        };
    }

    fn selected_span_info(&self) -> Option<&SpanInfo> {
        self.detail
            .as_ref()
            .and_then(|d| d.spans.get(self.selected_span))
    }

    pub fn render(&mut self, frame: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
            .split(area);

        render_waterfall(
            frame,
            chunks[0],
            &self.waterfall_spans,
            Some(self.selected_span),
            self.scroll_offset,
        );

        let attrs = self
            .selected_span_info()
            .map(|s| s.attributes.clone())
            .unwrap_or(serde_json::Value::Null);

        let title = self
            .selected_span_info()
            .map(|s| format!("Span: {} ({})", s.operation, s.service))
            .unwrap_or_else(|| "Span Attributes".to_string());

        render_json_tree(frame, chunks[1], &attrs, &mut self.json_state, &title);
    }
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::tui::client::models::SpanInfo;
    use crate::tui::test_helpers::assert_buffer_contains;

    fn make_detail() -> TraceDetail {
        TraceDetail {
            trace_id: "abc123".into(),
            spans: vec![
                SpanInfo {
                    span_id: "span-1".into(),
                    parent_span_id: None,
                    operation: "GET /api".into(),
                    service: "frontend".into(),
                    start_time_ms: 0.0,
                    duration_ms: 100.0,
                    status: "Ok".into(),
                    attributes: serde_json::json!({"http.method": "GET", "http.status_code": 200}),
                },
                SpanInfo {
                    span_id: "span-2".into(),
                    parent_span_id: Some("span-1".into()),
                    operation: "db.query".into(),
                    service: "backend".into(),
                    start_time_ms: 10.0,
                    duration_ms: 50.0,
                    status: "Ok".into(),
                    attributes: serde_json::json!({"db.system": "postgresql"}),
                },
            ],
        }
    }

    #[test]
    fn new_has_no_detail() {
        let view = TraceDetailView::new();
        assert!(view.detail.is_none());
        assert!(view.waterfall_spans.is_empty());
        assert_eq!(view.selected_span, 0);
    }

    #[test]
    fn set_detail_builds_waterfall() {
        let mut view = TraceDetailView::new();
        view.set_detail(make_detail());
        assert_eq!(view.waterfall_spans.len(), 2);
        assert_eq!(view.selected_span, 0);
    }

    #[test]
    fn clear_resets() {
        let mut view = TraceDetailView::new();
        view.set_detail(make_detail());
        view.clear();
        assert!(view.detail.is_none());
        assert!(view.waterfall_spans.is_empty());
    }

    #[test]
    fn select_next_prev_span() {
        let mut view = TraceDetailView::new();
        view.set_detail(make_detail());

        view.select_next_span(10);
        assert_eq!(view.selected_span, 1);

        view.select_next_span(10);
        assert_eq!(view.selected_span, 1); // Can't go past last

        view.select_prev_span();
        assert_eq!(view.selected_span, 0);

        view.select_prev_span();
        assert_eq!(view.selected_span, 0); // Can't go before 0
    }

    #[test]
    fn toggle_focus() {
        let mut view = TraceDetailView::new();
        assert_eq!(view.focus, DetailFocus::Waterfall);
        view.toggle_focus();
        assert_eq!(view.focus, DetailFocus::JsonViewer);
        view.toggle_focus();
        assert_eq!(view.focus, DetailFocus::Waterfall);
    }

    #[test]
    fn render_with_detail() {
        let mut terminal = Terminal::new(TestBackend::new(100, 20)).unwrap();
        let mut view = TraceDetailView::new();
        view.set_detail(make_detail());
        terminal
            .draw(|frame| view.render(frame, frame.area()))
            .unwrap();
        assert_buffer_contains(&terminal, "Waterfall");
        assert_buffer_contains(&terminal, "GET /api");
    }

    #[test]
    fn render_empty() {
        let mut terminal = Terminal::new(TestBackend::new(100, 20)).unwrap();
        let mut view = TraceDetailView::new();
        terminal
            .draw(|frame| view.render(frame, frame.area()))
            .unwrap();
        assert_buffer_contains(&terminal, "No spans to display");
    }

    #[test]
    fn snapshot_trace_detail_with_data() {
        let mut terminal = Terminal::new(TestBackend::new(100, 20)).unwrap();
        let mut view = TraceDetailView::new();
        view.set_detail(make_detail());
        terminal
            .draw(|frame| view.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("trace_detail_with_data", content);
    }

    #[test]
    fn snapshot_trace_detail_empty() {
        let mut terminal = Terminal::new(TestBackend::new(80, 15)).unwrap();
        let mut view = TraceDetailView::new();
        terminal
            .draw(|frame| view.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("trace_detail_empty", content);
    }
}
