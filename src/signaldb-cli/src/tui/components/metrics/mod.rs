//! Metrics tab: Flight SQL query interface with sparkline visualization.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::widgets::{Block, Borders, Paragraph, Sparkline};

use super::Component;
use crate::tui::action::Action;
use crate::tui::components::logs::log_table::LogTable;
use crate::tui::components::logs::query_bar::{QueryBar, QueryBarAction};
use crate::tui::state::AppState;
use crate::tui::widgets::sparkline::scale_to_u64;

const DEFAULT_QUERY: &str = "SELECT timestamp, metric_name, metric_value \
                             FROM _signaldb_metrics ORDER BY timestamp DESC LIMIT 500";

const F1_QUERY: &str = "SELECT timestamp, metric_name, metric_value \
                         FROM _signaldb_metrics \
                         WHERE metric_name IN (\
                         'signaldb.system.cpu_usage_percent', \
                         'signaldb.system.memory_used_bytes') \
                         ORDER BY timestamp DESC LIMIT 500";

const F2_QUERY: &str = "SELECT timestamp, metric_name, metric_value \
                         FROM _signaldb_metrics \
                         WHERE metric_name LIKE 'signaldb.wal.%' \
                         ORDER BY timestamp DESC LIMIT 500";

const F3_QUERY: &str = "SELECT timestamp, metric_name, metric_value \
                         FROM _signaldb_metrics \
                         WHERE metric_name LIKE 'signaldb.flight.pool_%' \
                         ORDER BY timestamp DESC LIMIT 500";

const SPARKLINE_COLORS: &[Color] = &[
    Color::Cyan,
    Color::Magenta,
    Color::Green,
    Color::Yellow,
    Color::Blue,
    Color::Red,
];

#[derive(Debug, Clone, PartialEq, Eq)]
enum Focus {
    QueryBar,
    Table,
}

/// Metrics tab composing query bar, sparkline panel, and data table.
pub struct MetricsPanel {
    query_bar: QueryBar,
    data_table: LogTable,
    focus: Focus,
    /// Whether a query execution has been requested (polled by the app).
    pub pending_query: Option<String>,
    /// metric_name -> Vec<f64> values (oldest first).
    sparkline_data: BTreeMap<String, Vec<f64>>,
}

impl MetricsPanel {
    pub fn new() -> Self {
        let mut query_bar = QueryBar::new();
        query_bar.text = DEFAULT_QUERY.to_string();

        Self {
            query_bar,
            data_table: LogTable::new(),
            focus: Focus::Table,
            pending_query: Some(DEFAULT_QUERY.to_string()),
            sparkline_data: BTreeMap::new(),
        }
    }

    #[allow(dead_code)] // Used by focused tests and upcoming integration paths
    /// Return the current SQL query text.
    pub fn current_query(&self) -> &str {
        self.query_bar.query()
    }

    /// Feed query results into both sparklines and table.
    pub fn set_data(&mut self, batches: &[RecordBatch]) {
        self.data_table.set_data(batches);
        self.sparkline_data = extract_sparkline_data(batches);
    }

    /// Feed an error into the table and clear sparklines.
    pub fn set_error(&mut self, msg: String) {
        self.data_table.set_error(msg);
        self.sparkline_data.clear();
    }

    /// Take the pending query (if any), clearing it.
    pub fn take_pending_query(&mut self) -> Option<String> {
        self.pending_query.take()
    }

    /// Re-queue the current query for execution (used by tick-based refresh).
    pub fn refresh(&mut self) {
        self.pending_query = Some(self.query_bar.query().to_string());
    }

    fn apply_shortcut(&mut self, query: &str) {
        self.query_bar.text = query.to_string();
        self.pending_query = Some(query.to_string());
    }
}

impl Component for MetricsPanel {
    fn handle_key_event(&mut self, key: KeyEvent) -> Option<Action> {
        match self.focus {
            Focus::QueryBar => {
                match self.query_bar.handle_key(key) {
                    QueryBarAction::Execute => {
                        self.pending_query = Some(self.query_bar.query().to_string());
                    }
                    QueryBarAction::Blur => {
                        self.focus = Focus::Table;
                        self.query_bar.focused = false;
                    }
                    QueryBarAction::None => {}
                }
                Some(Action::None)
            }
            Focus::Table => match key.code {
                KeyCode::Char('/') => {
                    self.focus = Focus::QueryBar;
                    self.query_bar.focused = true;
                    Some(Action::None)
                }
                KeyCode::F(1) => {
                    self.apply_shortcut(F1_QUERY);
                    Some(Action::None)
                }
                KeyCode::F(2) => {
                    self.apply_shortcut(F2_QUERY);
                    Some(Action::None)
                }
                KeyCode::F(3) => {
                    self.apply_shortcut(F3_QUERY);
                    Some(Action::None)
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.data_table.scroll_up();
                    Some(Action::None)
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.data_table.scroll_down();
                    Some(Action::None)
                }
                _ => None,
            },
        }
    }

    fn update(&mut self, action: &Action, _state: &mut AppState) {
        if matches!(action, Action::Refresh) {
            self.refresh();
        }
    }

    fn render(&self, frame: &mut Frame, area: Rect, _state: &AppState) {
        let sparkline_height = if self.sparkline_data.is_empty() {
            Constraint::Length(3)
        } else {
            let rows = self.sparkline_data.len() as u16;
            Constraint::Min(rows.saturating_mul(3).max(10))
        };

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(3), sparkline_height, Constraint::Min(0)])
            .split(area);

        self.query_bar.render(frame, chunks[0]);
        render_sparkline_panel(frame, chunks[1], &self.sparkline_data);

        let mut table_clone = LogTable {
            data: self.data_table.data.clone(),
            table_state: self.data_table.table_state,
            severity_col_idx: self.data_table.severity_col_idx,
        };
        table_clone.render(frame, chunks[2], self.focus == Focus::Table);
    }
}

fn extract_sparkline_data(batches: &[RecordBatch]) -> BTreeMap<String, Vec<f64>> {
    let mut groups: BTreeMap<String, Vec<f64>> = BTreeMap::new();

    for batch in batches {
        let metric_names = batch
            .column_by_name("metric_name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let values_col = batch.column_by_name("metric_value");

        let metric_names = match metric_names {
            Some(m) => m,
            None => continue,
        };

        for i in 0..batch.num_rows() {
            let name = metric_names.value(i).to_string();
            let value = values_col.and_then(|v| extract_f64(v, i)).unwrap_or(0.0);
            groups.entry(name).or_default().push(value);
        }
    }

    for values in groups.values_mut() {
        values.reverse();
    }

    groups
}

fn extract_f64(col: &Arc<dyn Array>, row: usize) -> Option<f64> {
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

fn render_sparkline_panel(frame: &mut Frame, area: Rect, data: &BTreeMap<String, Vec<f64>>) {
    let block = Block::default()
        .title(" Sparklines (F1: CPU/Mem, F2: WAL, F3: Pool) ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue));

    if data.is_empty() {
        let empty =
            Paragraph::new("No metric data — run a query with metric_name + metric_value columns")
                .style(Style::default().fg(Color::DarkGray))
                .block(block);
        frame.render_widget(empty, area);
        return;
    }

    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.height < 2 {
        return;
    }

    let metrics: Vec<(&String, &Vec<f64>)> = data.iter().collect();
    let rows_per_metric = (inner.height as usize / metrics.len()).max(1);
    let constraints: Vec<Constraint> = metrics
        .iter()
        .map(|_| Constraint::Length(rows_per_metric as u16))
        .collect();

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(inner);

    for (i, (name, values)) in metrics.iter().enumerate() {
        if i >= rows.len() {
            break;
        }
        let color = SPARKLINE_COLORS[i % SPARKLINE_COLORS.len()];
        let scaled = scale_to_u64(values);
        let sparkline = Sparkline::default()
            .data(&scaled)
            .style(Style::default().fg(color))
            .block(Block::default().title(name.as_str()));
        frame.render_widget(sparkline, rows[i]);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::tui::state::AppState;
    use crate::tui::test_helpers::assert_buffer_contains;

    fn press(code: KeyCode) -> KeyEvent {
        KeyEvent {
            code,
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        }
    }

    fn make_state() -> AppState {
        AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            std::time::Duration::from_secs(5),
        )
    }

    fn make_metric_batches() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("metric_value", DataType::Float64, false),
        ]));
        vec![
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500, 600])),
                    Arc::new(StringArray::from(vec![
                        "cpu_usage",
                        "cpu_usage",
                        "cpu_usage",
                        "memory_mb",
                        "memory_mb",
                        "memory_mb",
                    ])),
                    Arc::new(Float64Array::from(vec![
                        10.0, 25.0, 40.0, 512.0, 520.0, 530.0,
                    ])),
                ],
            )
            .unwrap(),
        ]
    }

    #[test]
    fn new_panel_has_pending_default_query() {
        let panel = MetricsPanel::new();
        assert_eq!(panel.focus, Focus::Table);
        assert!(panel.pending_query.is_some());
        assert!(
            panel
                .pending_query
                .as_deref()
                .unwrap()
                .contains("_signaldb_metrics")
        );
    }

    #[test]
    fn slash_focuses_query_bar() {
        let mut panel = MetricsPanel::new();
        let action = panel.handle_key_event(press(KeyCode::Char('/')));
        assert_eq!(panel.focus, Focus::QueryBar);
        assert!(panel.query_bar.focused);
        assert_eq!(action, Some(Action::None));
    }

    #[test]
    fn esc_from_query_bar_returns_to_table() {
        let mut panel = MetricsPanel::new();
        panel.focus = Focus::QueryBar;
        panel.query_bar.focused = true;
        panel.handle_key_event(press(KeyCode::Esc));
        assert_eq!(panel.focus, Focus::Table);
        assert!(!panel.query_bar.focused);
    }

    #[test]
    fn enter_in_query_bar_queues_query() {
        let mut panel = MetricsPanel::new();
        panel.pending_query = None;
        panel.focus = Focus::QueryBar;
        panel.query_bar.focused = true;
        panel.handle_key_event(press(KeyCode::Enter));
        assert!(panel.pending_query.is_some());
    }

    #[test]
    fn f1_shortcut_sets_cpu_mem_query() {
        let mut panel = MetricsPanel::new();
        panel.pending_query = None;
        panel.handle_key_event(press(KeyCode::F(1)));
        assert!(panel.pending_query.is_some());
        let q = panel.pending_query.as_deref().unwrap();
        assert!(q.contains("cpu_usage_percent"));
        assert!(q.contains("memory_used_bytes"));
    }

    #[test]
    fn f2_shortcut_sets_wal_query() {
        let mut panel = MetricsPanel::new();
        panel.pending_query = None;
        panel.handle_key_event(press(KeyCode::F(2)));
        assert!(panel.pending_query.is_some());
        let q = panel.pending_query.as_deref().unwrap();
        assert!(q.contains("signaldb.wal.%"));
    }

    #[test]
    fn f3_shortcut_sets_pool_query() {
        let mut panel = MetricsPanel::new();
        panel.pending_query = None;
        panel.handle_key_event(press(KeyCode::F(3)));
        assert!(panel.pending_query.is_some());
        let q = panel.pending_query.as_deref().unwrap();
        assert!(q.contains("signaldb.flight.pool_%"));
    }

    #[test]
    fn arrow_keys_scroll_table() {
        let mut panel = MetricsPanel::new();
        panel.set_data(&make_metric_batches());
        assert_eq!(panel.data_table.selected_index(), Some(0));

        panel.handle_key_event(press(KeyCode::Down));
        assert_eq!(panel.data_table.selected_index(), Some(1));

        panel.handle_key_event(press(KeyCode::Up));
        assert_eq!(panel.data_table.selected_index(), Some(0));
    }

    #[test]
    fn set_data_populates_sparkline_groups() {
        let mut panel = MetricsPanel::new();
        panel.set_data(&make_metric_batches());
        assert_eq!(panel.sparkline_data.len(), 2);
        assert!(panel.sparkline_data.contains_key("cpu_usage"));
        assert!(panel.sparkline_data.contains_key("memory_mb"));
    }

    #[test]
    fn set_error_clears_sparklines() {
        let mut panel = MetricsPanel::new();
        panel.set_data(&make_metric_batches());
        assert!(!panel.sparkline_data.is_empty());
        panel.set_error("timeout".into());
        assert!(panel.sparkline_data.is_empty());
    }

    #[test]
    fn refresh_requeues_query() {
        let mut panel = MetricsPanel::new();
        panel.pending_query = None;
        panel.refresh();
        assert!(panel.pending_query.is_some());
    }

    #[test]
    fn take_pending_clears() {
        let mut panel = MetricsPanel::new();
        let q = panel.take_pending_query();
        assert!(q.is_some());
        assert!(panel.pending_query.is_none());
    }

    #[test]
    fn update_refresh_action() {
        let mut panel = MetricsPanel::new();
        panel.pending_query = None;
        let mut state = make_state();
        panel.update(&Action::Refresh, &mut state);
        assert!(panel.pending_query.is_some());
    }

    #[test]
    fn unhandled_keys_in_table_bubble_up() {
        let mut panel = MetricsPanel::new();
        let action = panel.handle_key_event(press(KeyCode::Char('q')));
        assert!(action.is_none());
    }

    #[test]
    fn renders_empty_sparkline_panel() {
        let mut terminal = Terminal::new(TestBackend::new(100, 20)).unwrap();
        let panel = MetricsPanel::new();
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "Sparklines");
        assert_buffer_contains(&terminal, "No metric data");
    }

    #[test]
    fn renders_with_data() {
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        let mut panel = MetricsPanel::new();
        panel.set_data(&make_metric_batches());
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "cpu_usage");
        assert_buffer_contains(&terminal, "memory_mb");
    }

    #[test]
    fn renders_error_state() {
        let mut terminal = Terminal::new(TestBackend::new(80, 15)).unwrap();
        let mut panel = MetricsPanel::new();
        panel.set_error("table not found".into());
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "Query failed: table not found");
    }

    #[test]
    fn extract_sparkline_data_reverses_values() {
        let batches = make_metric_batches();
        let data = extract_sparkline_data(&batches);
        let cpu = &data["cpu_usage"];
        // Input was 10, 25, 40 (DESC order) -> reversed to 40, 25, 10 (oldest first)
        assert_eq!(cpu, &[40.0, 25.0, 10.0]);
    }

    #[test]
    fn snapshot_metrics_empty() {
        let mut terminal = Terminal::new(TestBackend::new(100, 20)).unwrap();
        let panel = MetricsPanel::new();
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("metrics_empty", content);
    }

    #[test]
    fn snapshot_metrics_with_data() {
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        let mut panel = MetricsPanel::new();
        panel.set_data(&make_metric_batches());
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("metrics_with_data", content);
    }

    #[test]
    fn snapshot_sparkline_panel_only() {
        let mut terminal = Terminal::new(TestBackend::new(80, 12)).unwrap();
        let batches = make_metric_batches();
        let data = extract_sparkline_data(&batches);
        let state = make_state();
        terminal
            .draw(|frame| {
                let _ = &state;
                render_sparkline_panel(frame, frame.area(), &data);
            })
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("sparkline_panel_only", content);
    }
}
