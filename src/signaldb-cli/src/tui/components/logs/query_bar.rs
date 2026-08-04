//! Editable SQL text input for the Logs tab.
//!
//! Provides a single-line text editor with cursor support. Enter executes
//! the current query, Esc returns focus to the log table.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::widgets::{Block, Borders, Paragraph};

/// Default SQL query shown when the Logs tab first loads.
pub const DEFAULT_QUERY: &str = "SELECT timestamp, severity_text, service_name, body, log_attributes, resource_attributes, scope_name, scope_attributes FROM logs ORDER BY timestamp DESC LIMIT 100";

/// Editable query bar with cursor position tracking.
#[derive(Debug, Clone)]
pub struct QueryBar {
    /// Current text content.
    pub text: String,
    /// Cursor byte-offset within `text`.
    cursor: usize,
    /// Whether the query bar currently has input focus.
    pub focused: bool,
    /// Query history (max 50 entries).
    history: Vec<String>,
    /// Current position in history (None = editing current, Some(n) = viewing history[n]).
    history_index: Option<usize>,
    /// Saved draft text when browsing history.
    draft: String,
}

#[allow(dead_code)]
impl QueryBar {
    /// Create a new query bar pre-filled with the default query.
    pub fn new() -> Self {
        let text = DEFAULT_QUERY.to_string();
        let cursor = text.len();
        Self {
            text,
            cursor,
            focused: false,
            history: Vec::new(),
            history_index: None,
            draft: String::new(),
        }
    }

    /// Handle a key event while the query bar is focused.
    ///
    /// Returns `true` if the key was consumed, `false` if it should bubble up.
    pub fn handle_key(&mut self, key: KeyEvent) -> QueryBarAction {
        match key.code {
            KeyCode::Enter => {
                let query = self.text.trim();
                if !query.is_empty()
                    && (self.history.is_empty() || self.history.last() != Some(&self.text))
                {
                    self.history.push(self.text.clone());
                    if self.history.len() > 50 {
                        self.history.remove(0);
                    }
                }
                self.history_index = None;
                self.draft.clear();
                QueryBarAction::Execute
            }
            KeyCode::Up => {
                if !self.history.is_empty() {
                    match self.history_index {
                        None => {
                            self.draft = self.text.clone();
                            self.history_index = Some(self.history.len() - 1);
                            self.text = self.history[self.history.len() - 1].clone();
                            self.cursor = self.text.len();
                        }
                        Some(idx) if idx > 0 => {
                            let new_idx = idx - 1;
                            self.history_index = Some(new_idx);
                            self.text = self.history[new_idx].clone();
                            self.cursor = self.text.len();
                        }
                        _ => {}
                    }
                }
                QueryBarAction::None
            }
            KeyCode::Down => {
                if let Some(idx) = self.history_index {
                    if idx < self.history.len() - 1 {
                        self.history_index = Some(idx + 1);
                        self.text = self.history[idx + 1].clone();
                        self.cursor = self.text.len();
                    } else {
                        self.history_index = None;
                        self.text = self.draft.clone();
                        self.cursor = self.text.len();
                    }
                }
                QueryBarAction::None
            }
            KeyCode::Esc => QueryBarAction::Blur,
            KeyCode::Char(c) => {
                if key.modifiers.contains(KeyModifiers::CONTROL) {
                    match c {
                        'a' => self.cursor = 0,
                        'e' => self.cursor = self.text.len(),
                        'u' => {
                            self.text.drain(..self.cursor);
                            self.cursor = 0;
                            self.history_index = None;
                        }
                        'k' => {
                            self.text.truncate(self.cursor);
                            self.history_index = None;
                        }
                        _ => {}
                    }
                } else {
                    self.text.insert(self.cursor, c);
                    self.cursor += c.len_utf8();
                    self.history_index = None;
                }
                QueryBarAction::None
            }
            KeyCode::Backspace => {
                if self.cursor > 0 {
                    let prev = self.text[..self.cursor]
                        .char_indices()
                        .next_back()
                        .map(|(i, _)| i)
                        .unwrap_or(0);
                    self.text.drain(prev..self.cursor);
                    self.cursor = prev;
                }
                self.history_index = None;
                QueryBarAction::None
            }
            KeyCode::Delete => {
                if self.cursor < self.text.len() {
                    let next = self.text[self.cursor..]
                        .char_indices()
                        .nth(1)
                        .map(|(i, _)| self.cursor + i)
                        .unwrap_or(self.text.len());
                    self.text.drain(self.cursor..next);
                }
                self.history_index = None;
                QueryBarAction::None
            }
            KeyCode::Left => {
                if self.cursor > 0 {
                    self.cursor = self.text[..self.cursor]
                        .char_indices()
                        .next_back()
                        .map(|(i, _)| i)
                        .unwrap_or(0);
                }
                QueryBarAction::None
            }
            KeyCode::Right => {
                if self.cursor < self.text.len() {
                    self.cursor = self.text[self.cursor..]
                        .char_indices()
                        .nth(1)
                        .map(|(i, _)| self.cursor + i)
                        .unwrap_or(self.text.len());
                }
                QueryBarAction::None
            }
            KeyCode::Home => {
                self.cursor = 0;
                QueryBarAction::None
            }
            KeyCode::End => {
                self.cursor = self.text.len();
                QueryBarAction::None
            }
            _ => QueryBarAction::None,
        }
    }

    /// Render the query bar into the given area.
    pub fn render(&self, frame: &mut Frame, area: Rect) {
        self.render_with_title(frame, area, None);
    }

    pub fn render_with_title(&self, frame: &mut Frame, area: Rect, time_hint: Option<&str>) {
        let border_color = if self.focused {
            Color::Yellow
        } else {
            Color::DarkGray
        };

        let title = match time_hint {
            Some(hint) => format!(" SQL Query (Enter: run, /: focus) [{hint}] "),
            None => " SQL Query (Enter: run, /: focus) ".to_string(),
        };

        let block = Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(border_color));

        // Show cursor indicator when focused.
        let display_text = if self.focused {
            // Insert a visible cursor character for rendering.
            let (before, after) = self.text.split_at(self.cursor);
            format!("{before}\u{2588}{after}")
        } else {
            self.text.clone()
        };

        let style = if self.focused {
            Style::default().fg(Color::White)
        } else {
            Style::default().fg(Color::Gray)
        };

        let paragraph = Paragraph::new(display_text).style(style).block(block);

        frame.render_widget(paragraph, area);
    }

    /// Return the current query text.
    pub fn query(&self) -> &str {
        &self.text
    }
}

/// Actions produced by the query bar's key handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryBarAction {
    /// Execute the current query.
    Execute,
    /// Return focus to the table.
    Blur,
    /// Key was consumed but no special action needed.
    None,
}

#[cfg(test)]
mod tests {
    use crossterm::event::{KeyEventKind, KeyEventState};

    use super::*;

    fn press(code: KeyCode) -> KeyEvent {
        KeyEvent {
            code,
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        }
    }

    fn press_ctrl(c: char) -> KeyEvent {
        KeyEvent {
            code: KeyCode::Char(c),
            modifiers: KeyModifiers::CONTROL,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        }
    }

    /// Clear the bar's text via the public key-handling API (Home moves the
    /// cursor to 0, Ctrl-K truncates from the cursor onward) and optionally
    /// type replacement text, leaving the cursor at the end of it.
    fn set_text(bar: &mut QueryBar, text: &str) {
        bar.handle_key(press(KeyCode::Home));
        bar.handle_key(press_ctrl('k'));
        for c in text.chars() {
            bar.handle_key(press(KeyCode::Char(c)));
        }
    }

    #[test]
    fn new_has_default_query() {
        let bar = QueryBar::new();
        assert_eq!(bar.text, DEFAULT_QUERY);
        assert!(!bar.focused);
    }

    #[test]
    fn typing_inserts_chars() {
        let mut bar = QueryBar::new();
        set_text(&mut bar, "SEL");
        assert_eq!(bar.text, "SEL");
    }

    #[test]
    fn backspace_deletes_char_before_cursor() {
        let mut bar = QueryBar::new();
        set_text(&mut bar, "abc");
        bar.handle_key(press(KeyCode::Backspace));
        assert_eq!(bar.text, "ab");
    }

    #[test]
    fn backspace_at_start_is_noop() {
        let mut bar = QueryBar::new();
        set_text(&mut bar, "abc");
        bar.handle_key(press(KeyCode::Home));
        bar.handle_key(press(KeyCode::Backspace));
        assert_eq!(bar.text, "abc");
    }

    #[test]
    fn left_then_right_returns_cursor_to_original_position() {
        let mut bar = QueryBar::new();
        set_text(&mut bar, "abc");
        bar.handle_key(press(KeyCode::Home));
        bar.handle_key(press(KeyCode::Right)); // cursor between 'a' and 'b'
        bar.handle_key(press(KeyCode::Left)); // cursor back to start
        bar.handle_key(press(KeyCode::Right)); // cursor between 'a' and 'b' again
        bar.handle_key(press(KeyCode::Char('X')));
        assert_eq!(bar.text, "aXbc");
    }

    #[test]
    fn home_end_keys_move_cursor_to_boundaries() {
        let mut bar = QueryBar::new();
        set_text(&mut bar, "SELECT 1");
        bar.handle_key(press(KeyCode::Home));
        bar.handle_key(press(KeyCode::Char('X')));
        assert_eq!(bar.text, "XSELECT 1");

        bar.handle_key(press(KeyCode::End));
        bar.handle_key(press(KeyCode::Char('Y')));
        assert_eq!(bar.text, "XSELECT 1Y");
    }

    #[test]
    fn ctrl_a_moves_cursor_to_start() {
        let mut bar = QueryBar::new();
        set_text(&mut bar, "abc");
        bar.handle_key(press_ctrl('a'));
        bar.handle_key(press(KeyCode::Char('X')));
        assert_eq!(bar.text, "Xabc");
    }

    #[test]
    fn ctrl_e_moves_cursor_to_end() {
        let mut bar = QueryBar::new();
        set_text(&mut bar, "abc");
        bar.handle_key(press(KeyCode::Home));
        bar.handle_key(press_ctrl('e'));
        bar.handle_key(press(KeyCode::Char('X')));
        assert_eq!(bar.text, "abcX");
    }

    #[test]
    fn ctrl_u_clears_text_before_cursor() {
        let mut bar = QueryBar::new();
        set_text(&mut bar, "abcdef");
        bar.handle_key(press(KeyCode::Home));
        bar.handle_key(press(KeyCode::Right));
        bar.handle_key(press(KeyCode::Right));
        bar.handle_key(press(KeyCode::Right)); // cursor between 'c' and 'd'
        bar.handle_key(press_ctrl('u'));
        assert_eq!(bar.text, "def");
        // Cursor should now be at the start of the remaining text.
        bar.handle_key(press(KeyCode::Char('X')));
        assert_eq!(bar.text, "Xdef");
    }

    #[test]
    fn ctrl_k_clears_text_after_cursor() {
        let mut bar = QueryBar::new();
        set_text(&mut bar, "abcdef");
        bar.handle_key(press(KeyCode::Home));
        bar.handle_key(press(KeyCode::Right));
        bar.handle_key(press(KeyCode::Right));
        bar.handle_key(press(KeyCode::Right)); // cursor between 'c' and 'd'
        bar.handle_key(press_ctrl('k'));
        assert_eq!(bar.text, "abc");
    }

    #[test]
    fn enter_returns_execute() {
        let mut bar = QueryBar::new();
        assert_eq!(
            bar.handle_key(press(KeyCode::Enter)),
            QueryBarAction::Execute
        );
    }

    #[test]
    fn esc_returns_blur() {
        let mut bar = QueryBar::new();
        assert_eq!(bar.handle_key(press(KeyCode::Esc)), QueryBarAction::Blur);
    }

    #[test]
    fn delete_removes_char_after_cursor() {
        let mut bar = QueryBar::new();
        set_text(&mut bar, "abc");
        bar.handle_key(press(KeyCode::Home));
        bar.handle_key(press(KeyCode::Right)); // cursor between 'a' and 'b'
        bar.handle_key(press(KeyCode::Delete));
        assert_eq!(bar.text, "ac");
    }

    #[test]
    fn render_focused_shows_cursor() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let mut terminal = Terminal::new(TestBackend::new(80, 3)).unwrap();
        let mut bar = QueryBar::new();
        bar.focused = true;
        set_text(&mut bar, "SELECT 1");
        terminal
            .draw(|frame| bar.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        assert!(content.contains("SELECT 1"));
        assert!(content.contains("SQL Query"));
    }

    #[test]
    fn render_unfocused() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let mut terminal = Terminal::new(TestBackend::new(120, 3)).unwrap();
        let bar = QueryBar::new();
        terminal
            .draw(|frame| bar.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        assert!(content.contains("SELECT timestamp, severity_text, service_name"));
    }

    #[test]
    fn history_navigation_cycles_through_entries_and_restores_draft() {
        let mut bar = QueryBar::new();

        for query in ["Q1", "Q2", "Q3"] {
            set_text(&mut bar, query);
            assert_eq!(
                bar.handle_key(press(KeyCode::Enter)),
                QueryBarAction::Execute
            );
        }

        set_text(&mut bar, "current");

        bar.handle_key(press(KeyCode::Up));
        assert_eq!(bar.text, "Q3");

        bar.handle_key(press(KeyCode::Up));
        assert_eq!(bar.text, "Q2");

        bar.handle_key(press(KeyCode::Up));
        assert_eq!(bar.text, "Q1");

        bar.handle_key(press(KeyCode::Down));
        assert_eq!(bar.text, "Q2");

        bar.handle_key(press(KeyCode::Down));
        assert_eq!(bar.text, "Q3");

        // One Down past the newest entry restores the pre-browsing draft.
        bar.handle_key(press(KeyCode::Down));
        assert_eq!(bar.text, "current");
    }

    #[test]
    fn history_skips_consecutive_duplicate_entries() {
        let mut bar = QueryBar::new();
        set_text(&mut bar, "Q");
        assert_eq!(
            bar.handle_key(press(KeyCode::Enter)),
            QueryBarAction::Execute
        );

        // Submitting the exact same text again must not add a duplicate
        // history entry.
        assert_eq!(
            bar.handle_key(press(KeyCode::Enter)),
            QueryBarAction::Execute
        );

        set_text(&mut bar, "Q2");
        assert_eq!(
            bar.handle_key(press(KeyCode::Enter)),
            QueryBarAction::Execute
        );

        // History should contain exactly two entries ("Q", "Q2"): Up twice
        // reaches the oldest one, and a third Up is a no-op.
        bar.handle_key(press(KeyCode::Up));
        bar.handle_key(press(KeyCode::Up));
        assert_eq!(bar.text, "Q");

        bar.handle_key(press(KeyCode::Up));
        assert_eq!(bar.text, "Q");
    }

    #[test]
    fn history_caps_at_50_entries_evicting_oldest() {
        let mut bar = QueryBar::new();

        for i in 0..60 {
            set_text(&mut bar, &format!("Q{i}"));
            bar.handle_key(press(KeyCode::Enter));
        }

        // Navigate to the oldest surviving entry (Up 50 times from the
        // most recently submitted query).
        for _ in 0..50 {
            bar.handle_key(press(KeyCode::Up));
        }
        assert_eq!(bar.text, "Q10", "oldest entries Q0..Q9 should be evicted");

        // A 51st Up is a no-op: there is no earlier entry than the 50 kept.
        bar.handle_key(press(KeyCode::Up));
        assert_eq!(bar.text, "Q10");
    }

    #[test]
    fn editing_while_browsing_history_exits_history_mode() {
        let mut bar = QueryBar::new();
        set_text(&mut bar, "Q1");
        bar.handle_key(press(KeyCode::Enter));

        set_text(&mut bar, "current");
        bar.handle_key(press(KeyCode::Up));
        assert_eq!(bar.text, "Q1");

        // Typing exits history-browsing mode: Down (which only acts while
        // browsing) becomes a no-op afterward instead of restoring a draft.
        bar.handle_key(press(KeyCode::Char('X')));
        assert_eq!(bar.text, "Q1X");
        bar.handle_key(press(KeyCode::Down));
        assert_eq!(bar.text, "Q1X");

        // Backspace also exits history-browsing mode.
        set_text(&mut bar, "Q1");
        bar.handle_key(press(KeyCode::Up));
        assert_eq!(bar.text, "Q1");
        bar.handle_key(press(KeyCode::Backspace));
        bar.handle_key(press(KeyCode::Down));
        assert_eq!(bar.text, "Q");

        // Delete also exits history-browsing mode.
        set_text(&mut bar, "Q1");
        bar.handle_key(press(KeyCode::Up));
        assert_eq!(bar.text, "Q1");
        bar.handle_key(press(KeyCode::Home));
        bar.handle_key(press(KeyCode::Delete));
        bar.handle_key(press(KeyCode::Down));
        assert_eq!(bar.text, "1");
    }

    #[test]
    fn history_skips_empty_and_whitespace_only_entries() {
        let mut bar = QueryBar::new();
        set_text(&mut bar, "");
        bar.handle_key(press(KeyCode::Enter));
        set_text(&mut bar, "draft");
        bar.handle_key(press(KeyCode::Up));
        assert_eq!(
            bar.text, "draft",
            "empty text must not become a history entry"
        );

        set_text(&mut bar, "   ");
        bar.handle_key(press(KeyCode::Enter));
        set_text(&mut bar, "draft2");
        bar.handle_key(press(KeyCode::Up));
        assert_eq!(
            bar.text, "draft2",
            "whitespace-only text must not become a history entry"
        );

        set_text(&mut bar, "Q1");
        bar.handle_key(press(KeyCode::Enter));
        set_text(&mut bar, "draft3");
        bar.handle_key(press(KeyCode::Up));
        assert_eq!(bar.text, "Q1");
    }
}
