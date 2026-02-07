//! Bottom status bar component

use crossterm::event::KeyEvent;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::Span;
use ratatui::widgets::Paragraph;

use super::Component;
use crate::tui::action::Action;
use crate::tui::state::{AppState, ConnectionStatus, Permission};

/// Bottom status bar with connection state, permission level, and keybind hints.
pub struct StatusBar;

impl StatusBar {
    pub fn new() -> Self {
        Self
    }
}

impl Component for StatusBar {
    fn handle_key_event(&mut self, _key: KeyEvent) -> Option<Action> {
        None
    }

    fn update(&mut self, _action: &Action, _state: &mut AppState) {}

    fn render(&self, frame: &mut Frame, area: Rect, state: &AppState) {
        let chunks = Layout::horizontal([
            Constraint::Percentage(40),
            Constraint::Percentage(20),
            Constraint::Percentage(40),
        ])
        .split(area);

        let (indicator, color) = match &state.connection_status {
            ConnectionStatus::Connected => ("Connected", Color::Green),
            ConnectionStatus::Disconnected => ("Disconnected", Color::Red),
            ConnectionStatus::Connecting => ("Connecting", Color::Yellow),
        };

        let left_text = if let Some(ref err) = state.last_error {
            let when = state
                .last_error_at
                .map(|ts| {
                    chrono::DateTime::<chrono::Local>::from(ts)
                        .format("%H:%M:%S")
                        .to_string()
                })
                .unwrap_or_else(|| "--:--:--".to_string());
            format!("{indicator} {url} | [{when}] {err}", url = state.url)
        } else {
            format!("{indicator} {url}", url = state.url)
        };

        let left = Paragraph::new(Span::styled(left_text, Style::default().fg(color)));
        frame.render_widget(left, chunks[0]);

        let perm_text = match &state.permission {
            Permission::Admin { .. } => "Admin".to_string(),
            Permission::Tenant { tenant_id, .. } => format!("Tenant: {tenant_id}"),
            Permission::Unknown => "Unknown".to_string(),
        };
        let center = Paragraph::new(perm_text)
            .style(Style::default().fg(Color::White))
            .centered();
        frame.render_widget(center, chunks[1]);

        let right = Paragraph::new("q: Quit  r: Refresh  /: Search  ?: Help")
            .style(Style::default().fg(Color::DarkGray))
            .right_aligned();
        frame.render_widget(right, chunks[2]);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::tui::state::AppState;
    use crate::tui::test_helpers::assert_buffer_contains;

    fn render_status_bar(state: &AppState) -> Terminal<TestBackend> {
        let mut terminal = Terminal::new(TestBackend::new(140, 1)).unwrap();
        let bar = StatusBar::new();
        terminal
            .draw(|frame| {
                bar.render(frame, frame.area(), state);
            })
            .unwrap();
        terminal
    }

    #[test]
    fn status_bar_connected() {
        let mut state = AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            Duration::from_secs(5),
        );
        state.connection_status = ConnectionStatus::Connected;

        let terminal = render_status_bar(&state);
        assert_buffer_contains(&terminal, "Connected");
        assert_buffer_contains(&terminal, "http://localhost:3000");
        assert_buffer_contains(&terminal, "q: Quit");
    }

    #[test]
    fn status_bar_disconnected() {
        let state = AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            Duration::from_secs(5),
        );

        let terminal = render_status_bar(&state);
        assert_buffer_contains(&terminal, "Disconnected");
    }

    #[test]
    fn status_bar_with_error() {
        let mut state = AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            Duration::from_secs(5),
        );
        state.last_error = Some("timeout".into());

        let terminal = render_status_bar(&state);
        assert_buffer_contains(&terminal, "timeout");
    }

    #[test]
    fn status_bar_admin_permission() {
        let mut state = AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            Duration::from_secs(5),
        );
        state.set_permission(Permission::Admin {
            admin_key: "key".into(),
        });
        state.connection_status = ConnectionStatus::Connected;

        let terminal = render_status_bar(&state);
        assert_buffer_contains(&terminal, "Admin");
    }

    #[test]
    fn status_bar_tenant_permission() {
        let mut state = AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            Duration::from_secs(5),
        );
        state.set_permission(Permission::Tenant {
            api_key: "key".into(),
            tenant_id: "acme".into(),
            dataset_id: None,
        });
        state.connection_status = ConnectionStatus::Connected;

        let terminal = render_status_bar(&state);
        assert_buffer_contains(&terminal, "Tenant: acme");
    }

    #[test]
    fn status_bar_snapshot_connected() {
        let mut state = AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            Duration::from_secs(5),
        );
        state.connection_status = ConnectionStatus::Connected;
        state.set_permission(Permission::Admin {
            admin_key: "key".into(),
        });

        let terminal = render_status_bar(&state);
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("status_bar_connected", content);
    }

    #[test]
    fn status_bar_snapshot_disconnected() {
        let state = AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            Duration::from_secs(5),
        );

        let terminal = render_status_bar(&state);
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("status_bar_disconnected", content);
    }

    #[test]
    fn status_bar_snapshot_error() {
        let mut state = AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            Duration::from_secs(5),
        );
        state.last_error = Some("connection refused".into());

        let terminal = render_status_bar(&state);
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("status_bar_error", content);
    }
}
