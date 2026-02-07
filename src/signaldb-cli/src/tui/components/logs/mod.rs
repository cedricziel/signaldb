//! Logs tab: Flight SQL query interface with dynamic table and detail viewer.

pub mod log_detail;
pub mod log_table;
pub mod query_bar;

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};

use self::log_detail::LogDetail;
use self::log_table::LogTable;
use self::query_bar::{QueryBar, QueryBarAction};
use super::Component;
use crate::tui::action::Action;
use crate::tui::state::AppState;

/// Focus state within the Logs tab.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Focus {
    QueryBar,
    Table,
}

/// Logs tab component composing query bar, log table, and detail viewer.
pub struct LogsPanel {
    query_bar: QueryBar,
    log_table: LogTable,
    log_detail: LogDetail,
    focus: Focus,
    /// Whether a query execution has been requested (polled by the app).
    pub pending_query: Option<String>,
    show_detail: bool,
}

impl LogsPanel {
    pub fn new() -> Self {
        Self {
            query_bar: QueryBar::new(),
            log_table: LogTable::new(),
            log_detail: LogDetail::new(),
            focus: Focus::Table,
            pending_query: Some(query_bar::DEFAULT_QUERY.to_string()),
            show_detail: false,
        }
    }

    /// Return the current SQL query text.
    pub fn current_query(&self) -> &str {
        self.query_bar.query()
    }

    /// Feed query results into the table.
    pub fn set_data(&mut self, batches: &[arrow::record_batch::RecordBatch]) {
        self.log_table.set_data(batches);
    }

    /// Feed an error into the table.
    pub fn set_error(&mut self, msg: String) {
        self.log_table.set_error(msg);
    }

    /// Take the pending query (if any), clearing it.
    pub fn take_pending_query(&mut self) -> Option<String> {
        self.pending_query.take()
    }

    /// Re-queue the current query for execution (used by tick-based refresh).
    pub fn refresh(&mut self) {
        self.pending_query = Some(self.query_bar.query().to_string());
    }
}

impl Component for LogsPanel {
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
                KeyCode::Up | KeyCode::Char('k') => {
                    self.log_table.scroll_up();
                    self.show_detail = false;
                    Some(Action::None)
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.log_table.scroll_down();
                    self.show_detail = false;
                    Some(Action::None)
                }
                KeyCode::Enter => {
                    self.show_detail = !self.show_detail;
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
        let detail_height = if self.show_detail {
            Constraint::Length(10)
        } else {
            Constraint::Length(0)
        };

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(3), Constraint::Min(0), detail_height])
            .split(area);

        self.query_bar.render(frame, chunks[0]);

        let mut table_clone = LogTable {
            data: self.log_table.data.clone(),
            table_state: self.log_table.table_state,
            severity_col_idx: self.log_table.severity_col_idx,
        };
        table_clone.render(frame, chunks[1], self.focus == Focus::Table);

        if self.show_detail {
            let selected = self.log_table.selected_row();
            self.log_detail.render(frame, chunks[2], selected);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
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

    fn make_batches() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("severity_text", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, false),
        ]));
        vec![
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![100, 200, 300])),
                    Arc::new(StringArray::from(vec!["ERROR", "WARN", "INFO"])),
                    Arc::new(StringArray::from(vec![
                        "disk full",
                        "high latency",
                        "request ok",
                    ])),
                ],
            )
            .unwrap(),
        ]
    }

    #[test]
    fn new_panel_has_pending_default_query() {
        let panel = LogsPanel::new();
        assert_eq!(panel.focus, Focus::Table);
        assert!(panel.pending_query.is_some());
        assert!(panel.pending_query.as_deref().unwrap().contains("SELECT"));
    }

    #[test]
    fn slash_focuses_query_bar() {
        let mut panel = LogsPanel::new();
        let action = panel.handle_key_event(press(KeyCode::Char('/')));
        assert_eq!(panel.focus, Focus::QueryBar);
        assert!(panel.query_bar.focused);
        assert_eq!(action, Some(Action::None));
    }

    #[test]
    fn esc_from_query_bar_returns_to_table() {
        let mut panel = LogsPanel::new();
        panel.focus = Focus::QueryBar;
        panel.query_bar.focused = true;
        panel.handle_key_event(press(KeyCode::Esc));
        assert_eq!(panel.focus, Focus::Table);
        assert!(!panel.query_bar.focused);
    }

    #[test]
    fn enter_in_query_bar_queues_query() {
        let mut panel = LogsPanel::new();
        panel.pending_query = None;
        panel.focus = Focus::QueryBar;
        panel.query_bar.focused = true;
        panel.handle_key_event(press(KeyCode::Enter));
        assert!(panel.pending_query.is_some());
    }

    #[test]
    fn arrow_keys_scroll_table() {
        let mut panel = LogsPanel::new();
        panel.set_data(&make_batches());
        assert_eq!(panel.log_table.selected_index(), Some(0));

        panel.handle_key_event(press(KeyCode::Down));
        assert_eq!(panel.log_table.selected_index(), Some(1));

        panel.handle_key_event(press(KeyCode::Up));
        assert_eq!(panel.log_table.selected_index(), Some(0));
    }

    #[test]
    fn enter_toggles_detail() {
        let mut panel = LogsPanel::new();
        panel.set_data(&make_batches());
        assert!(!panel.show_detail);

        panel.handle_key_event(press(KeyCode::Enter));
        assert!(panel.show_detail);

        panel.handle_key_event(press(KeyCode::Enter));
        assert!(!panel.show_detail);
    }

    #[test]
    fn refresh_requeues_query() {
        let mut panel = LogsPanel::new();
        panel.pending_query = None;
        panel.refresh();
        assert!(panel.pending_query.is_some());
    }

    #[test]
    fn take_pending_clears() {
        let mut panel = LogsPanel::new();
        let q = panel.take_pending_query();
        assert!(q.is_some());
        assert!(panel.pending_query.is_none());
    }

    #[test]
    fn renders_full_layout() {
        let mut terminal = Terminal::new(TestBackend::new(100, 20)).unwrap();
        let mut panel = LogsPanel::new();
        panel.set_data(&make_batches());
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "SQL Query");
        assert_buffer_contains(&terminal, "Logs");
        assert_buffer_contains(&terminal, "disk full");
    }

    #[test]
    fn renders_with_detail_panel() {
        let mut terminal = Terminal::new(TestBackend::new(100, 25)).unwrap();
        let mut panel = LogsPanel::new();
        panel.set_data(&make_batches());
        panel.show_detail = true;
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "Log Detail");
        assert_buffer_contains(&terminal, "severity_text:");
    }

    #[test]
    fn renders_error_state() {
        let mut terminal = Terminal::new(TestBackend::new(80, 15)).unwrap();
        let mut panel = LogsPanel::new();
        panel.set_error("table 'logs' not found".into());
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "Query failed: table 'logs' not found");
    }

    #[test]
    fn update_refresh_action() {
        let mut panel = LogsPanel::new();
        panel.pending_query = None;
        let mut state = make_state();
        panel.update(&Action::Refresh, &mut state);
        assert!(panel.pending_query.is_some());
    }

    #[test]
    fn unhandled_keys_in_table_bubble_up() {
        let mut panel = LogsPanel::new();
        let action = panel.handle_key_event(press(KeyCode::Char('q')));
        assert!(action.is_none());
    }

    #[test]
    fn snapshot_logs_full_layout() {
        let mut terminal = Terminal::new(TestBackend::new(100, 20)).unwrap();
        let mut panel = LogsPanel::new();
        panel.set_data(&make_batches());
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("logs_full_layout", content);
    }

    #[test]
    fn snapshot_logs_with_detail() {
        let mut terminal = Terminal::new(TestBackend::new(100, 25)).unwrap();
        let mut panel = LogsPanel::new();
        panel.set_data(&make_batches());
        panel.show_detail = true;
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("logs_with_detail", content);
    }
}
