//! Type-to-confirm deletion modal dialog.

use crossterm::event::KeyEvent;
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};

use crate::tui::widgets::text_input::{TextInput, TextInputAction};

/// Result of handling a key in the confirm dialog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfirmResult {
    /// User confirmed deletion (input matched resource name).
    Confirmed,
    /// User cancelled (pressed Esc).
    Cancelled,
    /// Key was consumed, dialog still open.
    Pending,
}

/// Modal dialog requiring the user to type a resource name to confirm deletion.
pub struct ConfirmDialog {
    resource_name: String,
    input: TextInput,
    visible: bool,
}

impl ConfirmDialog {
    pub fn new() -> Self {
        Self {
            resource_name: String::new(),
            input: TextInput::new(),
            visible: false,
        }
    }

    /// Show the dialog for deleting the given resource.
    pub fn show(&mut self, resource_name: &str) {
        self.resource_name = resource_name.to_string();
        self.input.clear();
        self.visible = true;
    }

    /// Hide the dialog.
    pub fn hide(&mut self) {
        self.visible = false;
        self.input.clear();
    }

    /// Whether the dialog is currently visible.
    pub fn is_visible(&self) -> bool {
        self.visible
    }

    /// Handle a key event. Returns `None` if dialog is not visible.
    pub fn handle_key(&mut self, key: KeyEvent) -> Option<ConfirmResult> {
        if !self.visible {
            return None;
        }

        match self.input.handle_key(key) {
            TextInputAction::Submit => {
                if self.input.text() == self.resource_name {
                    self.hide();
                    Some(ConfirmResult::Confirmed)
                } else {
                    Some(ConfirmResult::Pending)
                }
            }
            TextInputAction::Cancel => {
                self.hide();
                Some(ConfirmResult::Cancelled)
            }
            TextInputAction::Changed | TextInputAction::Unhandled => Some(ConfirmResult::Pending),
        }
    }

    /// Render the dialog as a centered modal overlay.
    pub fn render(&self, frame: &mut Frame, area: Rect) {
        if !self.visible {
            return;
        }

        let dialog_width = 60u16.min(area.width.saturating_sub(4));
        let dialog_height = 7u16.min(area.height.saturating_sub(2));

        let x = area.x + (area.width.saturating_sub(dialog_width)) / 2;
        let y = area.y + (area.height.saturating_sub(dialog_height)) / 2;

        let dialog_area = Rect::new(x, y, dialog_width, dialog_height);

        frame.render_widget(Clear, dialog_area);

        let block = Block::default()
            .title(" Confirm Deletion ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Red));

        let inner = block.inner(dialog_area);
        frame.render_widget(block, dialog_area);

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(2), Constraint::Length(3)])
            .split(inner);

        let prompt = Paragraph::new(format!(
            "Type '{}' to confirm deletion:",
            self.resource_name
        ))
        .style(Style::default().fg(Color::Yellow));
        frame.render_widget(prompt, chunks[0]);

        self.input.render(frame, chunks[1], "Confirm", true);
    }
}

#[cfg(test)]
mod tests {
    use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers};
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

    #[test]
    fn initially_hidden() {
        let dialog = ConfirmDialog::new();
        assert!(!dialog.is_visible());
    }

    #[test]
    fn show_makes_visible() {
        let mut dialog = ConfirmDialog::new();
        dialog.show("acme");
        assert!(dialog.is_visible());
    }

    #[test]
    fn hide_clears() {
        let mut dialog = ConfirmDialog::new();
        dialog.show("acme");
        dialog.hide();
        assert!(!dialog.is_visible());
    }

    #[test]
    fn handle_key_when_hidden_returns_none() {
        let mut dialog = ConfirmDialog::new();
        assert!(dialog.handle_key(press(KeyCode::Enter)).is_none());
    }

    #[test]
    fn esc_cancels() {
        let mut dialog = ConfirmDialog::new();
        dialog.show("acme");
        let result = dialog.handle_key(press(KeyCode::Esc));
        assert_eq!(result, Some(ConfirmResult::Cancelled));
        assert!(!dialog.is_visible());
    }

    #[test]
    fn wrong_text_stays_pending() {
        let mut dialog = ConfirmDialog::new();
        dialog.show("acme");
        dialog.handle_key(press(KeyCode::Char('x')));
        let result = dialog.handle_key(press(KeyCode::Enter));
        assert_eq!(result, Some(ConfirmResult::Pending));
        assert!(dialog.is_visible());
    }

    #[test]
    fn correct_text_confirms() {
        let mut dialog = ConfirmDialog::new();
        dialog.show("acme");
        for c in "acme".chars() {
            dialog.handle_key(press(KeyCode::Char(c)));
        }
        let result = dialog.handle_key(press(KeyCode::Enter));
        assert_eq!(result, Some(ConfirmResult::Confirmed));
        assert!(!dialog.is_visible());
    }

    #[test]
    fn render_hidden_does_nothing() {
        let mut terminal = Terminal::new(TestBackend::new(80, 20)).unwrap();
        let dialog = ConfirmDialog::new();
        terminal
            .draw(|frame| dialog.render(frame, frame.area()))
            .unwrap();
        // Should render empty space (no dialog visible)
    }

    #[test]
    fn render_visible_shows_prompt() {
        let mut terminal = Terminal::new(TestBackend::new(80, 20)).unwrap();
        let mut dialog = ConfirmDialog::new();
        dialog.show("acme-tenant");
        terminal
            .draw(|frame| dialog.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        assert!(content.contains("Confirm Deletion"));
        assert!(content.contains("acme-tenant"));
    }

    #[test]
    fn snapshot_confirm_dialog() {
        let mut terminal = Terminal::new(TestBackend::new(80, 20)).unwrap();
        let mut dialog = ConfirmDialog::new();
        dialog.show("acme-tenant");
        // Type partial text
        dialog.handle_key(press(KeyCode::Char('a')));
        dialog.handle_key(press(KeyCode::Char('c')));
        terminal
            .draw(|frame| dialog.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("confirm_dialog_partial", content);
    }
}
