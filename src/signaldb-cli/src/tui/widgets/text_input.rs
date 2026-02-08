//! Reusable text input widget with cursor tracking.
//!
//! Supports character insertion, backspace, left/right cursor movement,
//! Ctrl-A (start), Ctrl-E (end), Home/End keys, and visible cursor indicator.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::widgets::{Block, Borders, Paragraph};

/// A single-line text input with cursor position tracking.
#[derive(Debug, Clone)]
pub struct TextInput {
    /// Current text content.
    pub text: String,
    /// Cursor byte-offset within `text`.
    cursor: usize,
    /// Optional placeholder text shown when input is empty.
    placeholder: String,
}

/// Actions produced by the text input's key handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TextInputAction {
    /// User pressed Enter — submit the input.
    Submit,
    /// User pressed Esc — cancel the input.
    Cancel,
    /// Key was consumed, no special action needed.
    Changed,
    /// Key was not handled by the text input.
    Unhandled,
}

impl TextInput {
    /// Create a new empty text input.
    pub fn new() -> Self {
        Self {
            text: String::new(),
            cursor: 0,
            placeholder: String::new(),
        }
    }

    /// Create a text input with a placeholder.
    pub fn with_placeholder(placeholder: &str) -> Self {
        Self {
            text: String::new(),
            cursor: 0,
            placeholder: placeholder.to_string(),
        }
    }

    /// Create a text input with initial text.
    pub fn with_text(text: &str) -> Self {
        let len = text.len();
        Self {
            text: text.to_string(),
            cursor: len,
            placeholder: String::new(),
        }
    }

    /// Get the current text content.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Clear the text and reset cursor.
    pub fn clear(&mut self) {
        self.text.clear();
        self.cursor = 0;
    }

    /// Set the text and move cursor to end.
    pub fn set_text(&mut self, text: &str) {
        self.text = text.to_string();
        self.cursor = self.text.len();
    }

    /// Handle a key event. Returns an action indicating what happened.
    pub fn handle_key(&mut self, key: KeyEvent) -> TextInputAction {
        match key.code {
            KeyCode::Enter => TextInputAction::Submit,
            KeyCode::Esc => TextInputAction::Cancel,
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
                        _ => return TextInputAction::Unhandled,
                    }
                } else {
                    self.text.insert(self.cursor, c);
                    self.cursor += c.len_utf8();
                }
                TextInputAction::Changed
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
                TextInputAction::Changed
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
                TextInputAction::Changed
            }
            KeyCode::Left => {
                if self.cursor > 0 {
                    self.cursor = self.text[..self.cursor]
                        .char_indices()
                        .next_back()
                        .map(|(i, _)| i)
                        .unwrap_or(0);
                }
                TextInputAction::Changed
            }
            KeyCode::Right => {
                if self.cursor < self.text.len() {
                    self.cursor = self.text[self.cursor..]
                        .char_indices()
                        .nth(1)
                        .map(|(i, _)| self.cursor + i)
                        .unwrap_or(self.text.len());
                }
                TextInputAction::Changed
            }
            KeyCode::Home => {
                self.cursor = 0;
                TextInputAction::Changed
            }
            KeyCode::End => {
                self.cursor = self.text.len();
                TextInputAction::Changed
            }
            _ => TextInputAction::Unhandled,
        }
    }

    /// Render the text input into the given area.
    pub fn render(&self, frame: &mut Frame, area: Rect, title: &str, focused: bool) {
        let border_color = if focused {
            Color::Yellow
        } else {
            Color::DarkGray
        };

        let block = Block::default()
            .title(format!(" {title} "))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(border_color));

        let display_text = if self.text.is_empty() && !focused {
            self.placeholder.clone()
        } else if focused {
            let (before, after) = self.text.split_at(self.cursor);
            format!("{before}\u{2588}{after}")
        } else {
            self.text.clone()
        };

        let style = if self.text.is_empty() && !focused {
            Style::default().fg(Color::DarkGray)
        } else if focused {
            Style::default().fg(Color::White)
        } else {
            Style::default().fg(Color::Gray)
        };

        let paragraph = Paragraph::new(display_text).style(style).block(block);
        frame.render_widget(paragraph, area);
    }
}

#[cfg(test)]
mod tests {
    use crossterm::event::{KeyEventKind, KeyEventState};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

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
    fn new_is_empty() {
        let input = TextInput::new();
        assert!(input.text().is_empty());
        assert_eq!(input.cursor, 0);
    }

    #[test]
    fn with_text_sets_cursor_to_end() {
        let input = TextInput::with_text("hello");
        assert_eq!(input.text(), "hello");
        assert_eq!(input.cursor, 5);
    }

    #[test]
    fn typing_inserts_chars() {
        let mut input = TextInput::new();
        input.handle_key(press(KeyCode::Char('a')));
        input.handle_key(press(KeyCode::Char('b')));
        input.handle_key(press(KeyCode::Char('c')));
        assert_eq!(input.text(), "abc");
        assert_eq!(input.cursor, 3);
    }

    #[test]
    fn backspace_deletes_before_cursor() {
        let mut input = TextInput::with_text("abc");
        assert_eq!(
            input.handle_key(press(KeyCode::Backspace)),
            TextInputAction::Changed
        );
        assert_eq!(input.text(), "ab");
        assert_eq!(input.cursor, 2);
    }

    #[test]
    fn backspace_at_start_is_noop() {
        let mut input = TextInput::new();
        input.handle_key(press(KeyCode::Backspace));
        assert!(input.text().is_empty());
        assert_eq!(input.cursor, 0);
    }

    #[test]
    fn delete_removes_char_at_cursor() {
        let mut input = TextInput::with_text("abc");
        input.cursor = 1;
        input.handle_key(press(KeyCode::Delete));
        assert_eq!(input.text(), "ac");
        assert_eq!(input.cursor, 1);
    }

    #[test]
    fn left_right_movement() {
        let mut input = TextInput::with_text("abc");
        input.handle_key(press(KeyCode::Left));
        assert_eq!(input.cursor, 2);
        input.handle_key(press(KeyCode::Left));
        assert_eq!(input.cursor, 1);
        input.handle_key(press(KeyCode::Right));
        assert_eq!(input.cursor, 2);
    }

    #[test]
    fn left_at_start_stays() {
        let mut input = TextInput::with_text("abc");
        input.cursor = 0;
        input.handle_key(press(KeyCode::Left));
        assert_eq!(input.cursor, 0);
    }

    #[test]
    fn right_at_end_stays() {
        let mut input = TextInput::with_text("abc");
        input.handle_key(press(KeyCode::Right));
        assert_eq!(input.cursor, 3);
    }

    #[test]
    fn home_end_keys() {
        let mut input = TextInput::with_text("hello world");
        input.handle_key(press(KeyCode::Home));
        assert_eq!(input.cursor, 0);
        input.handle_key(press(KeyCode::End));
        assert_eq!(input.cursor, 11);
    }

    #[test]
    fn ctrl_a_goes_to_start() {
        let mut input = TextInput::with_text("abc");
        input.handle_key(press_ctrl('a'));
        assert_eq!(input.cursor, 0);
    }

    #[test]
    fn ctrl_e_goes_to_end() {
        let mut input = TextInput::with_text("abc");
        input.cursor = 0;
        input.handle_key(press_ctrl('e'));
        assert_eq!(input.cursor, 3);
    }

    #[test]
    fn ctrl_u_clears_before_cursor() {
        let mut input = TextInput::with_text("abcdef");
        input.cursor = 3;
        input.handle_key(press_ctrl('u'));
        assert_eq!(input.text(), "def");
        assert_eq!(input.cursor, 0);
    }

    #[test]
    fn ctrl_k_clears_after_cursor() {
        let mut input = TextInput::with_text("abcdef");
        input.cursor = 3;
        input.handle_key(press_ctrl('k'));
        assert_eq!(input.text(), "abc");
    }

    #[test]
    fn enter_returns_submit() {
        let mut input = TextInput::new();
        assert_eq!(
            input.handle_key(press(KeyCode::Enter)),
            TextInputAction::Submit
        );
    }

    #[test]
    fn esc_returns_cancel() {
        let mut input = TextInput::new();
        assert_eq!(
            input.handle_key(press(KeyCode::Esc)),
            TextInputAction::Cancel
        );
    }

    #[test]
    fn clear_resets() {
        let mut input = TextInput::with_text("hello");
        input.clear();
        assert!(input.text().is_empty());
        assert_eq!(input.cursor, 0);
    }

    #[test]
    fn set_text_moves_cursor_to_end() {
        let mut input = TextInput::new();
        input.set_text("new text");
        assert_eq!(input.text(), "new text");
        assert_eq!(input.cursor, 8);
    }

    #[test]
    fn insert_at_cursor_middle() {
        let mut input = TextInput::with_text("ac");
        input.cursor = 1;
        input.handle_key(press(KeyCode::Char('b')));
        assert_eq!(input.text(), "abc");
        assert_eq!(input.cursor, 2);
    }

    #[test]
    fn render_focused_shows_cursor() {
        let mut terminal = Terminal::new(TestBackend::new(40, 3)).unwrap();
        let input = TextInput::with_text("hello");
        terminal
            .draw(|frame| input.render(frame, frame.area(), "Name", true))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        assert!(content.contains("hello"));
        assert!(content.contains("Name"));
    }

    #[test]
    fn render_placeholder_when_empty() {
        let mut terminal = Terminal::new(TestBackend::new(40, 3)).unwrap();
        let input = TextInput::with_placeholder("Enter name...");
        terminal
            .draw(|frame| input.render(frame, frame.area(), "Name", false))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        assert!(content.contains("Enter name..."));
    }

    #[test]
    fn snapshot_text_input_focused() {
        let mut terminal = Terminal::new(TestBackend::new(40, 3)).unwrap();
        let input = TextInput::with_text("acme-corp");
        terminal
            .draw(|frame| input.render(frame, frame.area(), "Tenant ID", true))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("text_input_focused", content);
    }

    #[test]
    fn snapshot_text_input_empty() {
        let mut terminal = Terminal::new(TestBackend::new(40, 3)).unwrap();
        let input = TextInput::with_placeholder("Enter value...");
        terminal
            .draw(|frame| input.render(frame, frame.area(), "Value", false))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("text_input_empty", content);
    }
}
