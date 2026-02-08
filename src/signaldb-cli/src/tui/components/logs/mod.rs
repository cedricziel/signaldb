//! Logs tab: Flight SQL query interface with dynamic table and detail viewer.

pub mod log_detail;
pub mod log_table;
pub mod query_bar;

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use tui_tree_widget::TreeState;

use self::log_detail::LogDetail;
use self::log_table::{LogGroupBy, LogTable, LogViewMode};
use self::query_bar::{QueryBar, QueryBarAction};
use super::Component;
use super::selector::{SelectorAction, SelectorItem, SelectorPopup};
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
    show_group_selector: bool,
    group_selector: SelectorPopup,
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
            show_group_selector: false,
            group_selector: build_group_selector(),
        }
    }

    #[allow(dead_code)] // Used by focused tests and upcoming integration paths
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

    pub fn set_group_by(&mut self, group_by: LogGroupBy) {
        self.log_table.set_group_by(group_by);
    }

    pub fn has_pending(&self) -> bool {
        self.pending_query.is_some()
    }

    pub fn take_pending_query(&mut self) -> Option<String> {
        self.pending_query.take()
    }

    pub fn refresh(&mut self) {
        self.pending_query = Some(self.query_bar.query().to_string());
    }
}

impl Component for LogsPanel {
    fn handle_key_event(&mut self, key: KeyEvent) -> Option<Action> {
        if self.show_group_selector {
            match self.group_selector.handle_key(key) {
                SelectorAction::Selected { id, .. } => {
                    self.log_table.set_group_by(group_by_from_selector(&id));
                    self.show_group_selector = false;
                    self.group_selector.reset();
                    return Some(Action::None);
                }
                SelectorAction::Cancelled => {
                    self.show_group_selector = false;
                    self.group_selector.reset();
                    return Some(Action::None);
                }
                SelectorAction::None => return Some(Action::None),
            }
        }

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
                    if self.log_table.view_mode == LogViewMode::GroupSummary {
                        self.log_table.enter_selected_group();
                    } else {
                        self.show_detail = !self.show_detail;
                    }
                    Some(Action::None)
                }
                KeyCode::Esc => {
                    if self.log_table.exit_drill_in() {
                        return Some(Action::None);
                    }
                    if self.log_table.view_mode == LogViewMode::GroupSummary {
                        self.log_table.set_group_by(LogGroupBy::None);
                        return Some(Action::None);
                    }
                    None
                }
                KeyCode::Char('g') => {
                    self.show_group_selector = true;
                    self.group_selector.reset();
                    Some(Action::None)
                }
                KeyCode::Char('r') => {
                    if self.show_detail {
                        self.log_detail.cycle_attribute_tab();
                        Some(Action::None)
                    } else {
                        None
                    }
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

    fn render(&self, frame: &mut Frame, area: Rect, state: &AppState) {
        let detail_height = if self.show_detail {
            Constraint::Length(10)
        } else {
            Constraint::Length(0)
        };

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(3), Constraint::Min(0), detail_height])
            .split(area);

        self.query_bar
            .render_with_title(frame, chunks[0], Some(&state.time_range.label()));

        let mut table_clone = LogTable {
            data: self.log_table.data.clone(),
            table_state: self.log_table.table_state,
            severity_col_idx: self.log_table.severity_col_idx,
            group_by: self.log_table.group_by.clone(),
            view_mode: self.log_table.view_mode.clone(),
            group_summaries: self.log_table.group_summaries.clone(),
            group_table_state: self.log_table.group_table_state,
        };
        let spinner = if state.loading {
            Some(state.spinner_char())
        } else {
            None
        };
        table_clone.render_with_spinner(frame, chunks[1], self.focus == Focus::Table, spinner);

        if self.show_group_selector {
            self.group_selector.render(frame, chunks[1]);
        }

        if self.show_detail {
            let (columns, values) = self
                .log_table
                .selected_row()
                .map(|(c, v)| (c.to_vec(), v.to_vec()))
                .unwrap_or_default();
            let mut detail_clone = LogDetail {
                attribute_tab: self.log_detail.attribute_tab.clone(),
                json_state: TreeState::default(),
            };
            detail_clone.render(frame, chunks[2], &columns, &values);
        }
    }
}

fn build_group_selector() -> SelectorPopup {
    let mut selector = SelectorPopup::new("Group Logs By");
    selector.set_items(vec![
        SelectorItem {
            id: "none".to_string(),
            label: "None".to_string(),
            depth: 0,
            parent_id: None,
        },
        SelectorItem {
            id: "service".to_string(),
            label: "Service".to_string(),
            depth: 0,
            parent_id: None,
        },
        SelectorItem {
            id: "severity".to_string(),
            label: "Severity".to_string(),
            depth: 0,
            parent_id: None,
        },
        SelectorItem {
            id: "scope_name".to_string(),
            label: "Scope Name".to_string(),
            depth: 0,
            parent_id: None,
        },
    ]);
    selector
}

fn group_by_from_selector(id: &str) -> LogGroupBy {
    match id {
        "service" => LogGroupBy::Service,
        "severity" => LogGroupBy::Severity,
        "scope" | "scope_name" => LogGroupBy::ScopeName,
        _ => LogGroupBy::None,
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
            Field::new("service_name", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, false),
            Field::new("log_attributes", DataType::Utf8, false),
            Field::new("resource_attributes", DataType::Utf8, false),
            Field::new("scope_name", DataType::Utf8, false),
            Field::new("scope_attributes", DataType::Utf8, false),
        ]));
        vec![
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![100, 200, 300])),
                    Arc::new(StringArray::from(vec!["ERROR", "WARN", "INFO"])),
                    Arc::new(StringArray::from(vec!["checkout", "checkout", "worker"])),
                    Arc::new(StringArray::from(vec![
                        "disk full",
                        "high latency",
                        "request ok",
                    ])),
                    Arc::new(StringArray::from(vec![
                        "{\"error\":true}",
                        "{\"latency\":120}",
                        "{\"ok\":true}",
                    ])),
                    Arc::new(StringArray::from(vec![
                        "{\"service.name\":\"checkout\"}",
                        "{\"service.name\":\"checkout\"}",
                        "{\"service.name\":\"worker\"}",
                    ])),
                    Arc::new(StringArray::from(vec!["ingest", "ingest", "scheduler"])),
                    Arc::new(StringArray::from(vec![
                        "{\"scope.version\":\"1.0\"}",
                        "{\"scope.version\":\"1.0\"}",
                        "{\"scope.version\":\"1.1\"}",
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
        assert!(
            panel
                .pending_query
                .as_deref()
                .unwrap()
                .contains("service_name")
        );
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
    fn g_key_opens_group_selector_and_selects_group() {
        let mut panel = LogsPanel::new();
        panel.set_data(&make_batches());
        assert_eq!(panel.log_table.group_by, LogGroupBy::None);

        panel.handle_key_event(press(KeyCode::Char('g')));
        assert!(panel.show_group_selector);

        panel.handle_key_event(press(KeyCode::Down));
        panel.handle_key_event(press(KeyCode::Down));
        panel.handle_key_event(press(KeyCode::Enter));
        assert_eq!(panel.log_table.group_by, LogGroupBy::Severity);
        assert!(!panel.show_group_selector);
    }

    #[test]
    fn enter_on_group_summary_drills_and_esc_returns() {
        let mut panel = LogsPanel::new();
        panel.set_data(&make_batches());
        panel.log_table.set_group_by(LogGroupBy::Service);
        assert_eq!(panel.log_table.view_mode, LogViewMode::GroupSummary);

        panel.handle_key_event(press(KeyCode::Enter));
        assert!(matches!(
            panel.log_table.view_mode,
            LogViewMode::DrillIn { .. }
        ));

        panel.handle_key_event(press(KeyCode::Esc));
        assert_eq!(panel.log_table.view_mode, LogViewMode::GroupSummary);
    }

    #[test]
    fn r_key_cycles_attribute_tab_in_log_detail() {
        let mut panel = LogsPanel::new();
        panel.set_data(&make_batches());
        panel.show_detail = true;

        assert_eq!(
            panel.log_detail.attribute_tab,
            log_detail::LogAttributeTab::LogAttributes
        );
        panel.handle_key_event(press(KeyCode::Char('r')));
        assert_eq!(
            panel.log_detail.attribute_tab,
            log_detail::LogAttributeTab::ResourceAttributes
        );
        panel.handle_key_event(press(KeyCode::Char('r')));
        assert_eq!(
            panel.log_detail.attribute_tab,
            log_detail::LogAttributeTab::Scope
        );
    }

    #[test]
    fn enter_toggles_detail_in_flat_mode() {
        let mut panel = LogsPanel::new();
        panel.set_data(&make_batches());
        assert!(!panel.show_detail);

        panel.handle_key_event(press(KeyCode::Enter));
        assert!(panel.show_detail);

        panel.handle_key_event(press(KeyCode::Enter));
        assert!(!panel.show_detail);
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
        assert_buffer_contains(&terminal, "Log Attributes");
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
