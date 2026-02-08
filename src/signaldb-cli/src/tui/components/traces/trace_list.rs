//! Trace list table displaying search results.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
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

/// Summary of a group of traces.
#[derive(Debug, Clone)]
pub struct GroupSummary {
    pub key: String,
    pub trace_count: usize,
    pub avg_duration_ms: f64,
    pub p99_duration_ms: f64,
    pub traces: Vec<usize>,
}

/// View mode for the trace list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceViewMode {
    /// Flat list of all traces (no grouping).
    Flat,
    /// Grouped summary view (bar chart + table).
    GroupSummary,
    /// Drilled into a specific group, showing its traces.
    DrillIn { group_key: String },
}

/// Trace search results table with scrollable selection.
pub struct TraceList {
    pub data: TraceData,
    pub table_state: TableState,
    pub group_by: GroupBy,
    pub view_mode: TraceViewMode,
    pub group_summaries: Vec<GroupSummary>,
    pub group_table_state: TableState,
}

impl TraceList {
    pub fn new() -> Self {
        Self {
            data: TraceData::Loading,
            table_state: TableState::default(),
            group_by: GroupBy::None,
            view_mode: TraceViewMode::Flat,
            group_summaries: Vec::new(),
            group_table_state: TableState::default(),
        }
    }

    pub fn set_group_by(&mut self, group_by: GroupBy) {
        self.group_by = group_by;
        self.view_mode = if self.group_by == GroupBy::None {
            TraceViewMode::Flat
        } else {
            TraceViewMode::GroupSummary
        };
        self.rebuild_group_summaries();
    }

    pub fn set_data(&mut self, results: Vec<TraceResult>) {
        self.data = TraceData::Loaded(results);
        self.rebuild_group_summaries();
    }

    pub fn set_error(&mut self, msg: String) {
        self.data = TraceData::Error(msg);
    }

    pub fn scroll_up(&mut self) {
        let row_count = self.row_count();
        if row_count == 0 {
            return;
        }

        if self.view_mode == TraceViewMode::GroupSummary {
            let current = self.group_table_state.selected().unwrap_or(0);
            let next = if current == 0 {
                row_count - 1
            } else {
                current - 1
            };
            self.group_table_state.select(Some(next));
            return;
        }

        let current = self.table_state.selected().unwrap_or(0);
        let next = if current == 0 {
            row_count - 1
        } else {
            current - 1
        };
        self.table_state.select(Some(next));
    }

    pub fn scroll_down(&mut self) {
        let row_count = self.row_count();
        if row_count == 0 {
            return;
        }

        if self.view_mode == TraceViewMode::GroupSummary {
            let current = self.group_table_state.selected().unwrap_or(0);
            self.group_table_state
                .select(Some((current + 1) % row_count));
            return;
        }

        let current = self.table_state.selected().unwrap_or(0);
        self.table_state.select(Some((current + 1) % row_count));
    }

    pub fn is_drill_in_mode(&self) -> bool {
        matches!(self.view_mode, TraceViewMode::DrillIn { .. })
    }

    pub fn enter_selected_group(&mut self) -> bool {
        if self.view_mode != TraceViewMode::GroupSummary {
            return false;
        }

        let Some(selected) = self.group_table_state.selected() else {
            return false;
        };
        let Some(group) = self.group_summaries.get(selected) else {
            return false;
        };

        self.view_mode = TraceViewMode::DrillIn {
            group_key: group.key.clone(),
        };
        if group.trace_count > 0 {
            self.table_state.select(Some(0));
        } else {
            self.table_state.select(None);
        }
        true
    }

    pub fn exit_drill_in(&mut self) -> bool {
        if self.is_drill_in_mode() {
            self.view_mode = TraceViewMode::GroupSummary;
            return true;
        }
        false
    }

    #[allow(dead_code)]
    pub fn selected_index(&self) -> Option<usize> {
        match self.view_mode {
            TraceViewMode::GroupSummary => self.group_table_state.selected(),
            TraceViewMode::Flat | TraceViewMode::DrillIn { .. } => self.table_state.selected(),
        }
    }

    pub fn selected_trace(&self) -> Option<&TraceResult> {
        let TraceData::Loaded(results) = &self.data else {
            return None;
        };

        match &self.view_mode {
            TraceViewMode::GroupSummary => None,
            TraceViewMode::Flat => self.table_state.selected().and_then(|idx| results.get(idx)),
            TraceViewMode::DrillIn { group_key } => {
                let selected = self.table_state.selected()?;
                let group = self.group_summaries.iter().find(|g| &g.key == group_key)?;
                let trace_idx = *group.traces.get(selected)?;
                results.get(trace_idx)
            }
        }
    }

    #[allow(dead_code)]
    pub fn row_count(&self) -> usize {
        let TraceData::Loaded(results) = &self.data else {
            return 0;
        };

        match &self.view_mode {
            TraceViewMode::Flat => results.len(),
            TraceViewMode::GroupSummary => self.group_summaries.len(),
            TraceViewMode::DrillIn { group_key } => self
                .group_summaries
                .iter()
                .find(|g| &g.key == group_key)
                .map(|g| g.trace_count)
                .unwrap_or(0),
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
            TraceData::Loaded(results) => match &self.view_mode {
                TraceViewMode::Flat => {
                    let sel = self.table_state.selected().map(|s| s + 1).unwrap_or(0);
                    format!(" Traces [{sel}/{}]", results.len())
                }
                TraceViewMode::GroupSummary => {
                    format!(" Traces [Grouped by {}] ", self.group_by.label())
                }
                TraceViewMode::DrillIn { group_key } => {
                    format!(
                        " Traces [{}: {}] (Esc to go back) ",
                        self.group_by.label(),
                        group_key
                    )
                }
            },
            _ => " Traces ".to_string(),
        };

        let block = Block::default()
            .title(row_info)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(border_color));

        match self.data.clone() {
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

                match self.view_mode {
                    TraceViewMode::GroupSummary => self.render_group_summary(frame, area, block),
                    TraceViewMode::Flat | TraceViewMode::DrillIn { .. } => {
                        self.render_trace_table(frame, area, block, &results)
                    }
                }
            }
        }
    }

    fn render_trace_table(
        &mut self,
        frame: &mut Frame,
        area: Rect,
        block: Block,
        results: &[TraceResult],
    ) {
        let indices: Vec<usize> = match &self.view_mode {
            TraceViewMode::Flat => (0..results.len()).collect(),
            TraceViewMode::DrillIn { group_key } => self
                .group_summaries
                .iter()
                .find(|g| &g.key == group_key)
                .map(|g| g.traces.clone())
                .unwrap_or_default(),
            TraceViewMode::GroupSummary => Vec::new(),
        };

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

        let rows: Vec<Row> = indices
            .iter()
            .filter_map(|&idx| results.get(idx))
            .map(|trace| {
                let style = if trace.duration_ms > 1000.0 {
                    Style::default().fg(Color::Red)
                } else if trace.duration_ms > 500.0 {
                    Style::default().fg(Color::Yellow)
                } else {
                    Style::default()
                };

                Row::new(vec![
                    truncate_trace_id(&trace.trace_id),
                    trace.root_service.clone(),
                    trace.root_operation.clone(),
                    format_duration(trace.duration_ms),
                    trace.span_count.to_string(),
                    trace.start_time.clone(),
                ])
                .style(style)
            })
            .collect();

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

    fn render_group_summary(&mut self, frame: &mut Frame, area: Rect, block: Block) {
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let bar_height = (self.group_summaries.len() as u16)
            .saturating_add(2)
            .clamp(4, 8);
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(bar_height), Constraint::Min(6)])
            .split(inner);

        let bar_block = Block::default()
            .title(" Distribution ")
            .borders(Borders::ALL);
        let bar_inner = bar_block.inner(chunks[0]);
        frame.render_widget(bar_block, chunks[0]);

        if self.group_summaries.is_empty() {
            frame.render_widget(Paragraph::new("No groups"), bar_inner);
        } else {
            let palette = [
                Color::Cyan,
                Color::Green,
                Color::Yellow,
                Color::Magenta,
                Color::Blue,
                Color::Red,
            ];
            let max_count = self
                .group_summaries
                .iter()
                .map(|g| g.trace_count)
                .max()
                .unwrap_or(1);
            let label_space = 24usize;
            let max_bar_width = usize::from(bar_inner.width)
                .saturating_sub(label_space)
                .max(1);

            let lines: Vec<Line> = self
                .group_summaries
                .iter()
                .enumerate()
                .take(usize::from(bar_inner.height))
                .map(|(idx, group)| {
                    let color = palette[idx % palette.len()];
                    let ratio = if max_count == 0 {
                        0.0
                    } else {
                        group.trace_count as f64 / max_count as f64
                    };
                    let bar_len = ((ratio * max_bar_width as f64).round() as usize).max(1);
                    let bar = "█".repeat(bar_len);
                    Line::from(vec![
                        Span::styled(bar, Style::default().fg(color)),
                        Span::raw(format!(" {} ({})", group.key, group.trace_count)),
                    ])
                })
                .collect();

            frame.render_widget(Paragraph::new(lines), bar_inner);
        }

        let header = Row::new(vec!["Group", "Traces", "Avg Duration", "P99 Duration"]).style(
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        );

        let rows: Vec<Row> = self
            .group_summaries
            .iter()
            .map(|group| {
                Row::new(vec![
                    group.key.clone(),
                    group.trace_count.to_string(),
                    format_duration(group.avg_duration_ms),
                    format_duration(group.p99_duration_ms),
                ])
            })
            .collect();

        let widths = [
            Constraint::Min(18),
            Constraint::Length(8),
            Constraint::Length(14),
            Constraint::Length(14),
        ];

        let table = Table::new(rows, widths)
            .header(header)
            .block(Block::default().title(" Groups ").borders(Borders::ALL))
            .row_highlight_style(
                Style::default()
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("> ");

        frame.render_stateful_widget(table, chunks[1], &mut self.group_table_state);
    }

    fn rebuild_group_summaries(&mut self) {
        let TraceData::Loaded(results) = &self.data else {
            self.group_summaries.clear();
            self.table_state.select(None);
            self.group_table_state.select(None);
            return;
        };

        if self.group_by == GroupBy::None {
            self.group_summaries.clear();
            self.view_mode = TraceViewMode::Flat;
            if results.is_empty() {
                self.table_state.select(None);
            } else {
                let selected = self
                    .table_state
                    .selected()
                    .unwrap_or(0)
                    .min(results.len() - 1);
                self.table_state.select(Some(selected));
            }
            self.group_table_state.select(None);
            return;
        }

        self.group_summaries = compute_group_summaries(results, &self.group_by);
        if self.group_summaries.is_empty() {
            self.group_table_state.select(None);
        } else {
            let selected = self
                .group_table_state
                .selected()
                .unwrap_or(0)
                .min(self.group_summaries.len() - 1);
            self.group_table_state.select(Some(selected));
        }

        match &self.view_mode {
            TraceViewMode::DrillIn { group_key } => {
                if let Some(group) = self.group_summaries.iter().find(|g| &g.key == group_key) {
                    if group.trace_count == 0 {
                        self.table_state.select(None);
                    } else {
                        let selected = self
                            .table_state
                            .selected()
                            .unwrap_or(0)
                            .min(group.trace_count - 1);
                        self.table_state.select(Some(selected));
                    }
                } else {
                    self.view_mode = TraceViewMode::GroupSummary;
                    self.table_state.select(None);
                }
            }
            TraceViewMode::Flat | TraceViewMode::GroupSummary => {
                self.view_mode = TraceViewMode::GroupSummary;
                self.table_state.select(None);
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

fn group_key_display(raw: String) -> String {
    if raw.trim().is_empty() {
        "(unknown)".to_string()
    } else {
        raw
    }
}

fn format_duration(duration_ms: f64) -> String {
    if duration_ms < 1.0 {
        format!("{:.0}us", duration_ms * 1000.0)
    } else if duration_ms < 1000.0 {
        format!("{duration_ms:.1}ms")
    } else {
        format!("{:.2}s", duration_ms / 1000.0)
    }
}

fn p99_duration_ms(durations: &[f64]) -> f64 {
    if durations.is_empty() {
        return 0.0;
    }

    let mut sorted = durations.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let percentile_idx = ((sorted.len() as f64 * 0.99).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[percentile_idx]
}

fn compute_group_summaries(results: &[TraceResult], group_by: &GroupBy) -> Vec<GroupSummary> {
    if *group_by == GroupBy::None {
        return Vec::new();
    }

    let mut grouped: std::collections::BTreeMap<String, Vec<usize>> =
        std::collections::BTreeMap::new();
    for (idx, trace) in results.iter().enumerate() {
        grouped
            .entry(group_key_display(group_by.key(trace)))
            .or_default()
            .push(idx);
    }

    grouped
        .into_iter()
        .map(|(key, traces)| {
            let durations: Vec<f64> = traces
                .iter()
                .filter_map(|idx| results.get(*idx).map(|t| t.duration_ms))
                .collect();
            let total: f64 = durations.iter().sum();
            let trace_count = traces.len();
            let avg_duration_ms = if trace_count == 0 {
                0.0
            } else {
                total / trace_count as f64
            };
            let p99_duration_ms = p99_duration_ms(&durations);

            GroupSummary {
                key,
                trace_count,
                avg_duration_ms,
                p99_duration_ms,
                traces,
            }
        })
        .collect()
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
    fn set_group_by_switches_modes() {
        let mut list = TraceList::new();
        assert_eq!(list.group_by, GroupBy::None);
        list.set_group_by(GroupBy::Service);
        assert_eq!(list.group_by, GroupBy::Service);
        assert_eq!(list.view_mode, TraceViewMode::GroupSummary);
        list.set_group_by(GroupBy::None);
        assert_eq!(list.group_by, GroupBy::None);
        assert_eq!(list.view_mode, TraceViewMode::Flat);
    }

    #[test]
    fn computes_group_summary_stats() {
        let mut results = make_results();
        results.push(TraceResult {
            trace_id: "extra123".into(),
            root_service: "frontend".into(),
            root_operation: "GET /api/health".into(),
            duration_ms: 50.0,
            span_count: 3,
            start_time: "2025-01-15 10:30:02 UTC".into(),
            root_span_kind: "Server".into(),
        });

        let summaries = compute_group_summaries(&results, &GroupBy::Service);
        assert_eq!(summaries.len(), 2);

        let frontend = summaries.iter().find(|g| g.key == "frontend").unwrap();
        assert_eq!(frontend.trace_count, 2);
        assert!((frontend.avg_duration_ms - 100.0).abs() < f64::EPSILON);
        assert!((frontend.p99_duration_ms - 150.0).abs() < f64::EPSILON);
    }

    #[test]
    fn drill_in_and_back_preserves_group_mode() {
        let mut list = TraceList::new();
        list.set_data(make_results());
        list.set_group_by(GroupBy::Service);
        assert_eq!(list.view_mode, TraceViewMode::GroupSummary);

        assert!(list.enter_selected_group());
        assert!(matches!(list.view_mode, TraceViewMode::DrillIn { .. }));
        assert!(list.selected_trace().is_some());

        assert!(list.exit_drill_in());
        assert_eq!(list.view_mode, TraceViewMode::GroupSummary);
    }

    #[test]
    fn grouped_scroll_uses_group_table_state() {
        let mut list = TraceList::new();
        list.set_data(make_results());
        list.set_group_by(GroupBy::Service);

        assert_eq!(list.selected_index(), Some(0));
        list.scroll_down();
        assert_eq!(list.selected_index(), Some(1));
        list.scroll_down();
        assert_eq!(list.selected_index(), Some(0));
    }

    #[test]
    fn renders_grouped_summary_with_chart_and_table() {
        let mut terminal = Terminal::new(TestBackend::new(120, 15)).unwrap();
        let mut list = TraceList::new();
        list.set_data(make_results());
        list.set_group_by(GroupBy::Service);
        terminal
            .draw(|frame| list.render(frame, frame.area(), true))
            .unwrap();
        assert_buffer_contains(&terminal, "Grouped by Service");
        assert_buffer_contains(&terminal, "Distribution");
        assert_buffer_contains(&terminal, "P99 Duration");
        assert_buffer_contains(&terminal, "frontend");
    }

    #[test]
    fn renders_drill_in_title() {
        let mut terminal = Terminal::new(TestBackend::new(120, 15)).unwrap();
        let mut list = TraceList::new();
        list.set_data(make_results());
        list.set_group_by(GroupBy::Service);
        list.enter_selected_group();
        terminal
            .draw(|frame| list.render(frame, frame.area(), true))
            .unwrap();
        assert_buffer_contains(&terminal, "Esc to go back");
        assert_buffer_contains(&terminal, "Trace ID");
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
