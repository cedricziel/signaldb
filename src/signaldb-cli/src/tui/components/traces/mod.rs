//! Traces tab: search, list, waterfall visualization, and JSON detail viewer.

pub mod search_bar;
pub mod trace_detail;
pub mod trace_list;

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};

use tui_tree_widget::TreeState;

use self::search_bar::{SearchBarAction, TraceSearchBar};
use self::trace_detail::TraceDetailView;
use self::trace_list::TraceList;
use super::Component;
use crate::tui::action::Action;
use crate::tui::client::models::{TraceResult, TraceSearchParams};
use crate::tui::state::AppState;

/// Focus state within the Traces tab.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Focus {
    SearchBar,
    TraceList,
    TraceDetail,
}

/// Traces tab composing search bar, trace list, and detail viewer.
pub struct TracesPanel {
    search_bar: TraceSearchBar,
    trace_list: TraceList,
    trace_detail: TraceDetailView,
    focus: Focus,
    /// Pending search parameters (polled by the app).
    pub pending_search: Option<TraceSearchParams>,
    /// Pending trace ID to fetch detail for.
    pub pending_trace_id: Option<String>,
    show_detail: bool,
}

impl TracesPanel {
    pub fn new() -> Self {
        Self {
            search_bar: TraceSearchBar::new(),
            trace_list: TraceList::new(),
            trace_detail: TraceDetailView::new(),
            focus: Focus::TraceList,
            pending_search: Some(TraceSearchParams::default()),
            pending_trace_id: None,
            show_detail: false,
        }
    }

    pub fn set_search_results(&mut self, results: Vec<TraceResult>) {
        self.trace_list.set_data(results);
    }

    pub fn set_trace_detail(&mut self, detail: crate::tui::client::models::TraceDetail) {
        self.trace_detail.set_detail(detail);
        self.show_detail = true;
        self.focus = Focus::TraceDetail;
    }

    pub fn set_error(&mut self, msg: String) {
        self.trace_list.set_error(msg);
    }

    pub fn take_pending_search(&mut self) -> Option<TraceSearchParams> {
        self.pending_search.take()
    }

    pub fn take_pending_trace_id(&mut self) -> Option<String> {
        self.pending_trace_id.take()
    }

    pub fn refresh(&mut self) {
        self.pending_search = Some(self.search_bar.parse_params());
    }
}

impl Component for TracesPanel {
    fn handle_key_event(&mut self, key: KeyEvent) -> Option<Action> {
        match self.focus {
            Focus::SearchBar => {
                match self.search_bar.handle_key(key) {
                    SearchBarAction::Execute => {
                        self.pending_search = Some(self.search_bar.parse_params());
                    }
                    SearchBarAction::Blur => {
                        self.focus = Focus::TraceList;
                        self.search_bar.focused = false;
                    }
                    SearchBarAction::None => {}
                }
                Some(Action::None)
            }
            Focus::TraceList => match key.code {
                KeyCode::Char('/') => {
                    self.focus = Focus::SearchBar;
                    self.search_bar.focused = true;
                    Some(Action::None)
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.trace_list.scroll_up();
                    Some(Action::None)
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.trace_list.scroll_down();
                    Some(Action::None)
                }
                KeyCode::Enter => {
                    if let Some(trace) = self.trace_list.selected_trace() {
                        self.pending_trace_id = Some(trace.trace_id.clone());
                    }
                    Some(Action::None)
                }
                _ => None,
            },
            Focus::TraceDetail => match key.code {
                KeyCode::Esc => {
                    self.focus = Focus::TraceList;
                    self.show_detail = false;
                    self.trace_detail.clear();
                    Some(Action::None)
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.trace_detail.select_prev_span();
                    Some(Action::None)
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.trace_detail.select_next_span(20);
                    Some(Action::None)
                }
                KeyCode::Tab => {
                    self.trace_detail.toggle_focus();
                    Some(Action::None)
                }
                KeyCode::Char('/') => {
                    self.focus = Focus::SearchBar;
                    self.search_bar.focused = true;
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
        if self.show_detail {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(3), Constraint::Min(0)])
                .split(area);

            self.search_bar.render(frame, chunks[0]);

            let mut detail_clone = TraceDetailView {
                detail: self.trace_detail.detail.clone(),
                waterfall_spans: self.trace_detail.waterfall_spans.clone(),
                selected_span: self.trace_detail.selected_span,
                scroll_offset: self.trace_detail.scroll_offset,
                focus: self.trace_detail.focus.clone(),
                json_state: TreeState::default(),
            };
            detail_clone.render(frame, chunks[1]);
        } else {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(3), Constraint::Min(0)])
                .split(area);

            self.search_bar.render(frame, chunks[0]);

            let mut list_clone = TraceList {
                data: self.trace_list.data.clone(),
                table_state: self.trace_list.table_state,
            };
            list_clone.render(frame, chunks[1], self.focus == Focus::TraceList);
        }
    }
}

#[cfg(test)]
mod tests {
    use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::tui::client::models::{SpanInfo, TraceDetail, TraceResult};
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

    fn make_results() -> Vec<TraceResult> {
        vec![
            TraceResult {
                trace_id: "abc123".into(),
                root_service: "frontend".into(),
                root_operation: "GET /api/users".into(),
                duration_ms: 150.0,
                span_count: 5,
                start_time: "2025-01-15 10:30:00 UTC".into(),
            },
            TraceResult {
                trace_id: "def456".into(),
                root_service: "backend".into(),
                root_operation: "POST /api/orders".into(),
                duration_ms: 2500.0,
                span_count: 12,
                start_time: "2025-01-15 10:30:01 UTC".into(),
            },
        ]
    }

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
                    attributes: serde_json::json!({"http.method": "GET"}),
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
    fn new_panel_has_pending_default_search() {
        let panel = TracesPanel::new();
        assert_eq!(panel.focus, Focus::TraceList);
        assert!(panel.pending_search.is_some());
    }

    #[test]
    fn slash_focuses_search_bar() {
        let mut panel = TracesPanel::new();
        let action = panel.handle_key_event(press(KeyCode::Char('/')));
        assert_eq!(panel.focus, Focus::SearchBar);
        assert!(panel.search_bar.focused);
        assert_eq!(action, Some(Action::None));
    }

    #[test]
    fn esc_from_search_bar_returns_to_list() {
        let mut panel = TracesPanel::new();
        panel.focus = Focus::SearchBar;
        panel.search_bar.focused = true;
        panel.handle_key_event(press(KeyCode::Esc));
        assert_eq!(panel.focus, Focus::TraceList);
        assert!(!panel.search_bar.focused);
    }

    #[test]
    fn enter_in_search_bar_queues_search() {
        let mut panel = TracesPanel::new();
        panel.pending_search = None;
        panel.focus = Focus::SearchBar;
        panel.search_bar.focused = true;
        panel.handle_key_event(press(KeyCode::Enter));
        assert!(panel.pending_search.is_some());
    }

    #[test]
    fn arrow_keys_scroll_trace_list() {
        let mut panel = TracesPanel::new();
        panel.set_search_results(make_results());
        assert_eq!(panel.trace_list.selected_index(), Some(0));

        panel.handle_key_event(press(KeyCode::Down));
        assert_eq!(panel.trace_list.selected_index(), Some(1));

        panel.handle_key_event(press(KeyCode::Up));
        assert_eq!(panel.trace_list.selected_index(), Some(0));
    }

    #[test]
    fn enter_on_trace_queues_detail_fetch() {
        let mut panel = TracesPanel::new();
        panel.set_search_results(make_results());
        panel.handle_key_event(press(KeyCode::Enter));
        assert_eq!(panel.pending_trace_id.as_deref(), Some("abc123"));
    }

    #[test]
    fn set_trace_detail_shows_detail() {
        let mut panel = TracesPanel::new();
        panel.set_trace_detail(make_detail());
        assert!(panel.show_detail);
        assert_eq!(panel.focus, Focus::TraceDetail);
    }

    #[test]
    fn esc_from_detail_returns_to_list() {
        let mut panel = TracesPanel::new();
        panel.set_trace_detail(make_detail());
        panel.handle_key_event(press(KeyCode::Esc));
        assert_eq!(panel.focus, Focus::TraceList);
        assert!(!panel.show_detail);
    }

    #[test]
    fn tab_toggles_focus_in_detail() {
        let mut panel = TracesPanel::new();
        panel.set_trace_detail(make_detail());
        assert_eq!(
            panel.trace_detail.focus,
            trace_detail::DetailFocus::Waterfall
        );
        panel.handle_key_event(press(KeyCode::Tab));
        assert_eq!(
            panel.trace_detail.focus,
            trace_detail::DetailFocus::JsonViewer
        );
    }

    #[test]
    fn refresh_requeues_search() {
        let mut panel = TracesPanel::new();
        panel.pending_search = None;
        panel.refresh();
        assert!(panel.pending_search.is_some());
    }

    #[test]
    fn take_pending_search_clears() {
        let mut panel = TracesPanel::new();
        let params = panel.take_pending_search();
        assert!(params.is_some());
        assert!(panel.pending_search.is_none());
    }

    #[test]
    fn renders_list_view() {
        let mut terminal = Terminal::new(TestBackend::new(120, 20)).unwrap();
        let mut panel = TracesPanel::new();
        panel.set_search_results(make_results());
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "Trace Search");
        assert_buffer_contains(&terminal, "frontend");
        assert_buffer_contains(&terminal, "GET /api/users");
    }

    #[test]
    fn renders_detail_view() {
        let mut terminal = Terminal::new(TestBackend::new(120, 25)).unwrap();
        let mut panel = TracesPanel::new();
        panel.set_trace_detail(make_detail());
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "Waterfall");
        assert_buffer_contains(&terminal, "GET /api");
    }

    #[test]
    fn renders_error_state() {
        let mut terminal = Terminal::new(TestBackend::new(100, 15)).unwrap();
        let mut panel = TracesPanel::new();
        panel.set_error("table 'traces' not found".into());
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "Search failed: table 'traces' not found");
    }

    #[test]
    fn update_refresh_action() {
        let mut panel = TracesPanel::new();
        panel.pending_search = None;
        let mut state = make_state();
        panel.update(&Action::Refresh, &mut state);
        assert!(panel.pending_search.is_some());
    }

    #[test]
    fn unhandled_keys_in_list_bubble_up() {
        let mut panel = TracesPanel::new();
        let action = panel.handle_key_event(press(KeyCode::Char('q')));
        assert!(action.is_none());
    }

    #[test]
    fn snapshot_traces_list_view() {
        let mut terminal = Terminal::new(TestBackend::new(120, 15)).unwrap();
        let mut panel = TracesPanel::new();
        panel.set_search_results(make_results());
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("traces_list_view", content);
    }

    #[test]
    fn snapshot_traces_detail_view() {
        let mut terminal = Terminal::new(TestBackend::new(120, 25)).unwrap();
        let mut panel = TracesPanel::new();
        panel.set_trace_detail(make_detail());
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("traces_detail_view", content);
    }
}
