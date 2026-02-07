//! Help overlay modal listing keyboard shortcuts.

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::{Constraint, Flex, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};

use super::Component;
use crate::tui::action::Action;
use crate::tui::state::AppState;

/// Modal help overlay with keybinding reference.
pub struct HelpOverlay;

impl HelpOverlay {
    pub fn new() -> Self {
        Self
    }

    fn modal_area(area: Rect) -> Rect {
        let horizontal = Layout::default()
            .direction(ratatui::layout::Direction::Horizontal)
            .constraints([Constraint::Length(60)])
            .flex(Flex::Center)
            .split(area);

        Layout::default()
            .direction(ratatui::layout::Direction::Vertical)
            .constraints([Constraint::Length(23)])
            .flex(Flex::Center)
            .split(horizontal[0])[0]
    }
}

impl Component for HelpOverlay {
    fn handle_key_event(&mut self, key: KeyEvent) -> Option<Action> {
        match key.code {
            KeyCode::Esc | KeyCode::Char('?') => Some(Action::ToggleHelp),
            _ => Some(Action::None),
        }
    }

    fn update(&mut self, _action: &Action, _state: &mut AppState) {}

    fn render(&self, frame: &mut Frame, area: Rect, _state: &AppState) {
        let modal = Self::modal_area(area);
        frame.render_widget(Clear, modal);

        let lines = vec![
            Line::from(vec![Span::styled(
                "Navigation",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )]),
            Line::from("  Tab / Shift+Tab : Next / Previous tab"),
            Line::from("  1-5             : Jump to tab"),
            Line::from(""),
            Line::from(vec![Span::styled(
                "Tabs",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )]),
            Line::from("  /               : Focus query input"),
            Line::from("  Enter           : Execute or select"),
            Line::from("  Up/Down, j/k    : Move selection"),
            Line::from("  Esc             : Back / close dialog"),
            Line::from("  F1/F2/F3        : Metrics query shortcuts"),
            Line::from("  Ctrl+T          : Select tenant (admin)"),
            Line::from("  Ctrl+D          : Select dataset"),
            Line::from("  :               : Command palette"),
            Line::from("  T / K / D       : Admin sub-tabs"),
            Line::from("  c / e / d       : Admin CRUD actions"),
            Line::from("  y / n           : Confirm / cancel"),
            Line::from(""),
            Line::from(vec![Span::styled(
                "Global",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )]),
            Line::from("  r               : Refresh active tab"),
            Line::from("  ?               : Toggle this help"),
            Line::from("  q / Ctrl-C      : Quit"),
        ];

        let paragraph = Paragraph::new(lines).block(
            Block::default()
                .title(" Help ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Yellow)),
        );
        frame.render_widget(paragraph, modal);
    }
}

#[cfg(test)]
mod tests {
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;

    fn press(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn make_state() -> AppState {
        AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            std::time::Duration::from_secs(5),
        )
    }

    #[test]
    fn esc_or_question_mark_close_overlay() {
        let mut help = HelpOverlay::new();
        assert_eq!(
            help.handle_key_event(press(KeyCode::Esc)),
            Some(Action::ToggleHelp)
        );
        assert_eq!(
            help.handle_key_event(press(KeyCode::Char('?'))),
            Some(Action::ToggleHelp)
        );
    }

    #[test]
    fn renders_modal_with_keybinds() {
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        let help = HelpOverlay::new();
        let state = make_state();
        terminal
            .draw(|frame| help.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        assert!(content.contains("Help"));
        assert!(content.contains("Navigation"));
        assert!(content.contains("Toggle this help"));
    }
}
