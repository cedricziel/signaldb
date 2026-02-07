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
pub const DEFAULT_QUERY: &str =
    "SELECT timestamp, severity_text, body FROM logs ORDER BY timestamp DESC LIMIT 100";

/// Editable query bar with cursor position tracking.
#[derive(Debug, Clone)]
pub struct QueryBar {
    /// Current text content.
    pub text: String,
    /// Cursor byte-offset within `text`.
    cursor: usize,
    /// Whether the query bar currently has input focus.
    pub focused: bool,
}

impl QueryBar {
    /// Create a new query bar pre-filled with the default query.
    pub fn new() -> Self {
        let text = DEFAULT_QUERY.to_string();
        let cursor = text.len();
        Self {
            text,
            cursor,
            focused: false,
        }
    }

    /// Handle a key event while the query bar is focused.
    ///
    /// Returns `true` if the key was consumed, `false` if it should bubble up.
    pub fn handle_key(&mut self, key: KeyEvent) -> QueryBarAction {
        match key.code {
            KeyCode::Enter => QueryBarAction::Execute,
            KeyCode::Esc => QueryBarAction::Blur,
            KeyCode::Char(c) => {
                if key.modifiers.contains(KeyModifiers::CONTROL) {
                    match c {
                        'a' => self.cursor = 0,
                        'e' => self.cursor = self.text.len(),
                        'u' => {
                            self.text.drain(..self.cursor);
                            self.cursor = 0;
                        }
                        'k' => {
                            self.text.truncate(self.cursor);
                        }
                        _ => {}
                    }
                } else {
                    self.text.insert(self.cursor, c);
                    self.cursor += c.len_utf8();
                }
                QueryBarAction::None
            }
            KeyCode::Backspace => {
                if self.cursor > 0 {
                    // Find the previous char boundary.
                    let prev = self.text[..self.cursor]
                        .char_indices()
                        .next_back()
                        .map(|(i, _)| i)
                        .unwrap_or(0);
                    self.text.drain(prev..self.cursor);
                    self.cursor = prev;
                }
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
        let border_color = if self.focused {
            Color::Yellow
        } else {
            Color::DarkGray
        };

        let block = Block::default()
            .title(" SQL Query (Enter: run, /: focus) ")
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

    #[test]
    fn new_has_default_query() {
        let bar = QueryBar::new();
        assert_eq!(bar.text, DEFAULT_QUERY);
        assert_eq!(bar.cursor, DEFAULT_QUERY.len());
        assert!(!bar.focused);
    }

    #[test]
    fn typing_inserts_chars() {
        let mut bar = QueryBar::new();
        bar.text.clear();
        bar.cursor = 0;
        bar.handle_key(press(KeyCode::Char('S')));
        bar.handle_key(press(KeyCode::Char('E')));
        bar.handle_key(press(KeyCode::Char('L')));
        assert_eq!(bar.text, "SEL");
        assert_eq!(bar.cursor, 3);
    }

    #[test]
    fn backspace_deletes_char() {
        let mut bar = QueryBar::new();
        bar.text = "abc".into();
        bar.cursor = 3;
        bar.handle_key(press(KeyCode::Backspace));
        assert_eq!(bar.text, "ab");
        assert_eq!(bar.cursor, 2);
    }

    #[test]
    fn backspace_at_start_is_noop() {
        let mut bar = QueryBar::new();
        bar.text = "abc".into();
        bar.cursor = 0;
        bar.handle_key(press(KeyCode::Backspace));
        assert_eq!(bar.text, "abc");
        assert_eq!(bar.cursor, 0);
    }

    #[test]
    fn left_right_movement() {
        let mut bar = QueryBar::new();
        bar.text = "abc".into();
        bar.cursor = 1;
        bar.handle_key(press(KeyCode::Left));
        assert_eq!(bar.cursor, 0);
        bar.handle_key(press(KeyCode::Right));
        assert_eq!(bar.cursor, 1);
    }

    #[test]
    fn home_end_keys() {
        let mut bar = QueryBar::new();
        bar.text = "SELECT 1".into();
        bar.cursor = 4;
        bar.handle_key(press(KeyCode::Home));
        assert_eq!(bar.cursor, 0);
        bar.handle_key(press(KeyCode::End));
        assert_eq!(bar.cursor, 8);
    }

    #[test]
    fn ctrl_a_goes_to_start() {
        let mut bar = QueryBar::new();
        bar.text = "abc".into();
        bar.cursor = 3;
        bar.handle_key(press_ctrl('a'));
        assert_eq!(bar.cursor, 0);
    }

    #[test]
    fn ctrl_e_goes_to_end() {
        let mut bar = QueryBar::new();
        bar.text = "abc".into();
        bar.cursor = 0;
        bar.handle_key(press_ctrl('e'));
        assert_eq!(bar.cursor, 3);
    }

    #[test]
    fn ctrl_u_clears_before_cursor() {
        let mut bar = QueryBar::new();
        bar.text = "abcdef".into();
        bar.cursor = 3;
        bar.handle_key(press_ctrl('u'));
        assert_eq!(bar.text, "def");
        assert_eq!(bar.cursor, 0);
    }

    #[test]
    fn ctrl_k_clears_after_cursor() {
        let mut bar = QueryBar::new();
        bar.text = "abcdef".into();
        bar.cursor = 3;
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
    fn delete_removes_char_at_cursor() {
        let mut bar = QueryBar::new();
        bar.text = "abc".into();
        bar.cursor = 1;
        bar.handle_key(press(KeyCode::Delete));
        assert_eq!(bar.text, "ac");
        assert_eq!(bar.cursor, 1);
    }

    #[test]
    fn render_focused_shows_cursor() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let mut terminal = Terminal::new(TestBackend::new(80, 3)).unwrap();
        let mut bar = QueryBar::new();
        bar.focused = true;
        bar.text = "SELECT 1".into();
        bar.cursor = 8;
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
        assert!(content.contains(DEFAULT_QUERY));
    }
}
