//! Search bar for trace queries using Tempo API parameters.

use crossterm::event::KeyEvent;
use ratatui::Frame;
use ratatui::layout::Rect;

use crate::tui::client::models::TraceSearchParams;
use crate::tui::widgets::text_input::{TextInput, TextInputAction};

/// Actions produced by the search bar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchBarAction {
    Execute,
    Blur,
    None,
}

/// Search bar for Tempo trace search parameters.
///
/// Input format: `tags=service.name=frontend minDuration=100ms maxDuration=5s limit=20`
pub struct TraceSearchBar {
    input: TextInput,
    pub focused: bool,
}

impl TraceSearchBar {
    pub fn new() -> Self {
        Self {
            input: TextInput::with_placeholder(
                "tags=key=val minDuration=100ms maxDuration=5s limit=20",
            ),
            focused: false,
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> SearchBarAction {
        match self.input.handle_key(key) {
            TextInputAction::Submit => SearchBarAction::Execute,
            TextInputAction::Cancel => SearchBarAction::Blur,
            TextInputAction::Changed | TextInputAction::Unhandled => SearchBarAction::None,
        }
    }

    /// Parse the current input text into `TraceSearchParams`.
    pub fn parse_params(&self) -> TraceSearchParams {
        let text = self.input.text();
        let mut params = TraceSearchParams::default();

        for part in text.split_whitespace() {
            if let Some(val) = part.strip_prefix("tags=") {
                params.tags = Some(val.to_string());
            } else if let Some(val) = part.strip_prefix("minDuration=") {
                params.min_duration = Some(val.to_string());
            } else if let Some(val) = part.strip_prefix("maxDuration=") {
                params.max_duration = Some(val.to_string());
            } else if let Some(val) = part.strip_prefix("limit=") {
                params.limit = val.parse().ok();
            } else if !part.is_empty() {
                // Treat bare text as tags filter
                let existing = params.tags.get_or_insert_with(String::new);
                if !existing.is_empty() {
                    existing.push(' ');
                }
                existing.push_str(part);
            }
        }

        params
    }

    pub fn render(&self, frame: &mut Frame, area: Rect) {
        self.input.render(
            frame,
            area,
            "Trace Search (Enter: search, /: focus)",
            self.focused,
        );
    }
}

#[cfg(test)]
mod tests {
    use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::tui::test_helpers::assert_buffer_contains;

    fn press(code: KeyCode) -> KeyEvent {
        KeyEvent {
            code,
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        }
    }

    #[test]
    fn new_bar_is_empty() {
        let bar = TraceSearchBar::new();
        assert!(!bar.focused);
        let params = bar.parse_params();
        assert!(params.tags.is_none());
        assert!(params.min_duration.is_none());
        assert!(params.max_duration.is_none());
        assert!(params.limit.is_none());
    }

    #[test]
    fn enter_returns_execute() {
        let mut bar = TraceSearchBar::new();
        assert_eq!(
            bar.handle_key(press(KeyCode::Enter)),
            SearchBarAction::Execute
        );
    }

    #[test]
    fn esc_returns_blur() {
        let mut bar = TraceSearchBar::new();
        assert_eq!(bar.handle_key(press(KeyCode::Esc)), SearchBarAction::Blur);
    }

    #[test]
    fn parse_tags() {
        let mut bar = TraceSearchBar::new();
        bar.input.set_text("tags=service.name=frontend");
        let params = bar.parse_params();
        assert_eq!(params.tags.as_deref(), Some("service.name=frontend"));
    }

    #[test]
    fn parse_duration_filters() {
        let mut bar = TraceSearchBar::new();
        bar.input.set_text("minDuration=100ms maxDuration=5s");
        let params = bar.parse_params();
        assert_eq!(params.min_duration.as_deref(), Some("100ms"));
        assert_eq!(params.max_duration.as_deref(), Some("5s"));
    }

    #[test]
    fn parse_limit() {
        let mut bar = TraceSearchBar::new();
        bar.input.set_text("limit=50");
        let params = bar.parse_params();
        assert_eq!(params.limit, Some(50));
    }

    #[test]
    fn parse_combined() {
        let mut bar = TraceSearchBar::new();
        bar.input
            .set_text("tags=http.method=GET minDuration=10ms limit=25");
        let params = bar.parse_params();
        assert_eq!(params.tags.as_deref(), Some("http.method=GET"));
        assert_eq!(params.min_duration.as_deref(), Some("10ms"));
        assert_eq!(params.limit, Some(25));
    }

    #[test]
    fn parse_bare_text_as_tags() {
        let mut bar = TraceSearchBar::new();
        bar.input.set_text("service.name=frontend");
        let params = bar.parse_params();
        assert_eq!(params.tags.as_deref(), Some("service.name=frontend"));
    }

    #[test]
    fn render_unfocused() {
        let mut terminal = Terminal::new(TestBackend::new(80, 3)).unwrap();
        let bar = TraceSearchBar::new();
        terminal
            .draw(|frame| bar.render(frame, frame.area()))
            .unwrap();
        assert_buffer_contains(&terminal, "Trace Search");
    }

    #[test]
    fn render_focused() {
        let mut terminal = Terminal::new(TestBackend::new(80, 3)).unwrap();
        let mut bar = TraceSearchBar::new();
        bar.focused = true;
        terminal
            .draw(|frame| bar.render(frame, frame.area()))
            .unwrap();
        assert_buffer_contains(&terminal, "Trace Search");
    }

    #[test]
    fn snapshot_search_bar_unfocused() {
        let mut terminal = Terminal::new(TestBackend::new(80, 3)).unwrap();
        let bar = TraceSearchBar::new();
        terminal
            .draw(|frame| bar.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("trace_search_bar_unfocused", content);
    }
}
