//! Trace list table displaying search results.

use ratatui::Frame;
use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Row, Table, TableState};

use crate::tui::client::models::TraceResult;

/// Tri-state data model for the trace list.
#[derive(Debug, Clone)]
pub enum TraceData {
    Loading,
    Loaded(Vec<TraceResult>),
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupBy {
    None,
    Service,
    Operation,
    SpanKind,
}

impl GroupBy {
    pub fn cycle(&self) -> Self {
        match self {
            Self::None => Self::Service,
            Self::Service => Self::Operation,
            Self::Operation => Self::SpanKind,
            Self::SpanKind => Self::None,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::None => "None",
            Self::Service => "Service",
            Self::Operation => "Operation",
            Self::SpanKind => "Span Kind",
        }
    }

    fn key(&self, trace: &TraceResult) -> String {
        match self {
            Self::None => String::new(),
            Self::Service => trace.root_service.clone(),
            Self::Operation => trace.root_operation.clone(),
            Self::SpanKind => trace.root_span_kind.clone(),
        }
    }
}

/// Trace search results table with scrollable selection.
pub struct TraceList {
    pub data: TraceData,
    pub table_state: TableState,
    pub group_by: GroupBy,
}

impl TraceList {
    pub fn new() -> Self {
        Self {
            data: TraceData::Loading,
            table_state: TableState::default(),
            group_by: GroupBy::None,
        }
    }

    pub fn cycle_group_by(&mut self) {
        self.group_by = self.group_by.cycle();
    }

    pub fn set_data(&mut self, results: Vec<TraceResult>) {
        let has_results = !results.is_empty();
        self.data = TraceData::Loaded(results);
        if self.table_state.selected().is_none() && has_results {
            self.table_state.select(Some(0));
        }
    }

    pub fn set_error(&mut self, msg: String) {
        self.data = TraceData::Error(msg);
    }

    pub fn scroll_up(&mut self) {
        if let TraceData::Loaded(results) = &self.data {
            if results.is_empty() {
                return;
            }
            let current = self.table_state.selected().unwrap_or(0);
            let next = if current == 0 {
                results.len() - 1
            } else {
                current - 1
            };
            self.table_state.select(Some(next));
        }
    }

    pub fn scroll_down(&mut self) {
        if let TraceData::Loaded(results) = &self.data {
            if results.is_empty() {
                return;
            }
            let current = self.table_state.selected().unwrap_or(0);
            let next = (current + 1) % results.len();
            self.table_state.select(Some(next));
        }
    }

    #[allow(dead_code)]
    pub fn selected_index(&self) -> Option<usize> {
        self.table_state.selected()
    }

    pub fn selected_trace(&self) -> Option<&TraceResult> {
        if let TraceData::Loaded(results) = &self.data {
            self.table_state.selected().and_then(|idx| results.get(idx))
        } else {
            None
        }
    }

    #[allow(dead_code)]
    pub fn row_count(&self) -> usize {
        match &self.data {
            TraceData::Loaded(results) => results.len(),
            _ => 0,
        }
    }

    #[cfg(test)]
    pub fn render(&mut self, frame: &mut Frame, area: Rect, focused: bool) {
        self.render_with_spinner(frame, area, focused, None);
    }

    pub fn render_with_spinner(
        &mut self,
        frame: &mut Frame,
        area: Rect,
        focused: bool,
        spinner: Option<char>,
    ) {
        let border_color = if focused {
            Color::Cyan
        } else {
            Color::DarkGray
        };

        let row_info = match &self.data {
            TraceData::Loaded(results) => {
                let sel = self.table_state.selected().map(|s| s + 1).unwrap_or(0);
                let group_label = match &self.group_by {
                    GroupBy::None => String::new(),
                    g => format!(" (g: {}) ", g.label()),
                };
                format!(" Traces [{sel}/{}]{group_label}", results.len())
            }
            _ => " Traces ".to_string(),
        };

        let block = Block::default()
            .title(row_info)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(border_color));

        match &self.data {
            TraceData::Loading => {
                let text = if let Some(s) = spinner {
                    format!("{s} Searching traces...")
                } else {
                    "Searching traces...".to_string()
                };
                let loading = Paragraph::new(text)
                    .style(Style::default().fg(Color::DarkGray))
                    .block(block);
                frame.render_widget(loading, area);
            }
            TraceData::Error(msg) => {
                let error = Paragraph::new(format!("Search failed: {msg}"))
                    .style(Style::default().fg(Color::Red))
                    .block(block);
                frame.render_widget(error, area);
            }
            TraceData::Loaded(results) => {
                if results.is_empty() {
                    let empty = Paragraph::new("No traces found")
                        .style(Style::default().fg(Color::DarkGray))
                        .block(block);
                    frame.render_widget(empty, area);
                    return;
                }

                let header = Row::new(vec![
                    "Trace ID",
                    "Service",
                    "Operation",
                    "Duration",
                    "Spans",
                    "Start Time",
                ])
                .style(
                    Style::default()
                        .fg(Color::White)
                        .add_modifier(Modifier::BOLD),
                )
                .bottom_margin(1);

                let sorted_results: Vec<&TraceResult> = if self.group_by != GroupBy::None {
                    let mut sorted: Vec<&TraceResult> = results.iter().collect();
                    sorted.sort_by(|a, b| {
                        self.group_by
                            .key(a)
                            .cmp(&self.group_by.key(b))
                            .then(b.start_time.cmp(&a.start_time))
                    });
                    sorted
                } else {
                    results.iter().collect()
                };

                let mut rows: Vec<Row> = Vec::new();
                let mut last_group: Option<String> = std::option::Option::None;

                for t in &sorted_results {
                    if self.group_by != GroupBy::None {
                        let group_key = self.group_by.key(t);
                        if last_group.as_ref() != Some(&group_key) {
                            let header_text = format!(
                                "── {} ──",
                                if group_key.is_empty() {
                                    "(unknown)"
                                } else {
                                    &group_key
                                }
                            );
                            rows.push(
                                Row::new(vec![Line::from(vec![Span::styled(
                                    header_text,
                                    Style::default()
                                        .fg(Color::Cyan)
                                        .add_modifier(Modifier::BOLD),
                                )])])
                                .height(1),
                            );
                            last_group = Some(group_key);
                        }
                    }

                    let duration = if t.duration_ms < 1.0 {
                        format!("{:.0}us", t.duration_ms * 1000.0)
                    } else if t.duration_ms < 1000.0 {
                        format!("{:.1}ms", t.duration_ms)
                    } else {
                        format!("{:.2}s", t.duration_ms / 1000.0)
                    };

                    let style = if t.duration_ms > 1000.0 {
                        Style::default().fg(Color::Red)
                    } else if t.duration_ms > 500.0 {
                        Style::default().fg(Color::Yellow)
                    } else {
                        Style::default()
                    };

                    rows.push(
                        Row::new(vec![
                            truncate_trace_id(&t.trace_id),
                            t.root_service.clone(),
                            t.root_operation.clone(),
                            duration,
                            t.span_count.to_string(),
                            t.start_time.clone(),
                        ])
                        .style(style),
                    );
                }

                let widths = [
                    Constraint::Length(18),
                    Constraint::Length(16),
                    Constraint::Min(20),
                    Constraint::Length(10),
                    Constraint::Length(6),
                    Constraint::Length(28),
                ];

                let table = Table::new(rows, widths)
                    .header(header)
                    .block(block)
                    .row_highlight_style(
                        Style::default()
                            .bg(Color::DarkGray)
                            .add_modifier(Modifier::BOLD),
                    );

                frame.render_stateful_widget(table, area, &mut self.table_state);
            }
        }
    }
}

fn truncate_trace_id(id: &str) -> String {
    if id.len() > 16 {
        format!("{}...", &id[..13])
    } else {
        id.to_string()
    }
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::tui::test_helpers::assert_buffer_contains;

    fn make_results() -> Vec<TraceResult> {
        vec![
            TraceResult {
                trace_id: "abc123def456789012345678".into(),
                root_service: "frontend".into(),
                root_operation: "GET /api/users".into(),
                duration_ms: 150.0,
                span_count: 5,
                start_time: "2025-01-15 10:30:00 UTC".into(),
                root_span_kind: "Server".into(),
            },
            TraceResult {
                trace_id: "def456abc789012345678901".into(),
                root_service: "backend".into(),
                root_operation: "POST /api/orders".into(),
                duration_ms: 2500.0,
                span_count: 12,
                start_time: "2025-01-15 10:30:01 UTC".into(),
                root_span_kind: "Client".into(),
            },
        ]
    }

    #[test]
    fn renders_loading_state() {
        let mut terminal = Terminal::new(TestBackend::new(100, 10)).unwrap();
        let mut list = TraceList::new();
        terminal
            .draw(|frame| list.render(frame, frame.area(), false))
            .unwrap();
        assert_buffer_contains(&terminal, "Searching traces...");
    }

    #[test]
    fn renders_error_state() {
        let mut terminal = Terminal::new(TestBackend::new(100, 10)).unwrap();
        let mut list = TraceList::new();
        list.set_error("connection refused".into());
        terminal
            .draw(|frame| list.render(frame, frame.area(), false))
            .unwrap();
        assert_buffer_contains(&terminal, "Search failed: connection refused");
    }

    #[test]
    fn renders_empty_results() {
        let mut terminal = Terminal::new(TestBackend::new(100, 10)).unwrap();
        let mut list = TraceList::new();
        list.set_data(vec![]);
        terminal
            .draw(|frame| list.render(frame, frame.area(), false))
            .unwrap();
        assert_buffer_contains(&terminal, "No traces found");
    }

    #[test]
    fn renders_results() {
        let mut terminal = Terminal::new(TestBackend::new(120, 12)).unwrap();
        let mut list = TraceList::new();
        list.set_data(make_results());
        terminal
            .draw(|frame| list.render(frame, frame.area(), true))
            .unwrap();
        assert_buffer_contains(&terminal, "Trace ID");
        assert_buffer_contains(&terminal, "frontend");
        assert_buffer_contains(&terminal, "GET /api/users");
    }

    #[test]
    fn scroll_up_down_wraps() {
        let mut list = TraceList::new();
        list.set_data(make_results());
        assert_eq!(list.selected_index(), Some(0));

        list.scroll_down();
        assert_eq!(list.selected_index(), Some(1));

        list.scroll_down();
        assert_eq!(list.selected_index(), Some(0));

        list.scroll_up();
        assert_eq!(list.selected_index(), Some(1));
    }

    #[test]
    fn selected_trace_returns_data() {
        let mut list = TraceList::new();
        list.set_data(make_results());
        let trace = list.selected_trace().unwrap();
        assert_eq!(trace.root_service, "frontend");
    }

    #[test]
    fn truncate_trace_id_long() {
        let id = "abcdef1234567890abcdef1234567890";
        assert_eq!(truncate_trace_id(id), "abcdef1234567...");
    }

    #[test]
    fn truncate_trace_id_short() {
        assert_eq!(truncate_trace_id("abc123"), "abc123");
    }

    #[test]
    fn cycle_group_by() {
        let mut list = TraceList::new();
        assert_eq!(list.group_by, GroupBy::None);
        list.cycle_group_by();
        assert_eq!(list.group_by, GroupBy::Service);
        list.cycle_group_by();
        assert_eq!(list.group_by, GroupBy::Operation);
        list.cycle_group_by();
        assert_eq!(list.group_by, GroupBy::SpanKind);
        list.cycle_group_by();
        assert_eq!(list.group_by, GroupBy::None);
    }

    #[test]
    fn renders_grouped_by_service() {
        let mut terminal = Terminal::new(TestBackend::new(120, 15)).unwrap();
        let mut list = TraceList::new();
        list.set_data(make_results());
        list.group_by = GroupBy::Service;
        terminal
            .draw(|frame| list.render(frame, frame.area(), true))
            .unwrap();
        assert_buffer_contains(&terminal, "── backend ──");
        assert_buffer_contains(&terminal, "── frontend ──");
        assert_buffer_contains(&terminal, "(g: Service)");
    }

    #[test]
    fn renders_grouped_by_span_kind() {
        let mut terminal = Terminal::new(TestBackend::new(120, 15)).unwrap();
        let mut list = TraceList::new();
        list.set_data(make_results());
        list.group_by = GroupBy::SpanKind;
        terminal
            .draw(|frame| list.render(frame, frame.area(), true))
            .unwrap();
        assert_buffer_contains(&terminal, "── Server ──");
        assert_buffer_contains(&terminal, "── Client ──");
    }

    #[test]
    fn snapshot_trace_list_with_results() {
        let mut terminal = Terminal::new(TestBackend::new(120, 10)).unwrap();
        let mut list = TraceList::new();
        list.set_data(make_results());
        terminal
            .draw(|frame| list.render(frame, frame.area(), true))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("trace_list_with_results", content);
    }

    #[test]
    fn snapshot_trace_list_empty() {
        let mut terminal = Terminal::new(TestBackend::new(100, 6)).unwrap();
        let mut list = TraceList::new();
        list.set_data(vec![]);
        terminal
            .draw(|frame| list.render(frame, frame.area(), false))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("trace_list_empty", content);
    }

    #[test]
    fn snapshot_trace_list_loading() {
        let mut terminal = Terminal::new(TestBackend::new(80, 6)).unwrap();
        let mut list = TraceList::new();
        terminal
            .draw(|frame| list.render(frame, frame.area(), false))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("trace_list_loading", content);
    }
}
