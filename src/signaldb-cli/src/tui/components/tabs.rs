//! Tab bar component

use crossterm::event::KeyEvent;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Tabs as RatatuiTabs};

use super::Component;
use crate::tui::action::Action;
use crate::tui::state::AppState;

/// Top-of-screen tab bar showing available tabs with keyboard shortcuts.
pub struct TabBar;

impl TabBar {
    pub fn new() -> Self {
        Self
    }
}

impl Component for TabBar {
    fn handle_key_event(&mut self, _key: KeyEvent) -> Option<Action> {
        None
    }

    fn update(&mut self, _action: &Action, _state: &mut AppState) {}

    fn render(&self, frame: &mut Frame, area: Rect, state: &AppState) {
        let titles: Vec<Line<'_>> = state
            .available_tabs
            .iter()
            .map(|tab| Line::from(format!("[{}] {}", tab.shortcut(), tab.label())))
            .collect();

        let selected = state
            .available_tabs
            .iter()
            .position(|t| t == &state.active_tab)
            .unwrap_or(0);

        let tabs = RatatuiTabs::new(titles)
            .block(Block::default().borders(Borders::ALL).title("SignalDB"))
            .select(selected)
            .style(Style::default().fg(Color::White))
            .highlight_style(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )
            .divider("|");

        frame.render_widget(tabs, area);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::tui::state::{AppState, Permission, Tab};
    use crate::tui::test_helpers::assert_buffer_contains;

    fn render_tab_bar(state: &AppState) -> Terminal<TestBackend> {
        let mut terminal = Terminal::new(TestBackend::new(80, 3)).unwrap();
        let tab_bar = TabBar::new();
        terminal
            .draw(|frame| {
                tab_bar.render(frame, frame.area(), state);
            })
            .unwrap();
        terminal
    }

    #[test]
    fn tab_bar_admin_shows_all_five_tabs() {
        let mut state = AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            Duration::from_secs(5),
        );
        state.set_permission(Permission::Admin {
            admin_key: "key".into(),
        });

        let terminal = render_tab_bar(&state);
        assert_buffer_contains(&terminal, "[1] Dashboard");
        assert_buffer_contains(&terminal, "[2] Traces");
        assert_buffer_contains(&terminal, "[3] Logs");
        assert_buffer_contains(&terminal, "[4] Metrics");
        assert_buffer_contains(&terminal, "[5] Admin");
    }

    #[test]
    fn tab_bar_tenant_hides_admin() {
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

        let terminal = render_tab_bar(&state);
        assert_buffer_contains(&terminal, "[1] Dashboard");
        assert_buffer_contains(&terminal, "[2] Traces");
        assert_buffer_contains(&terminal, "[3] Logs");
        assert_buffer_contains(&terminal, "[4] Metrics");

        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        assert!(
            !content.contains("[5] Admin"),
            "Admin tab should be hidden for tenant permission"
        );
    }

    #[test]
    fn tab_bar_highlights_active_tab() {
        let mut state = AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            Duration::from_secs(5),
        );
        state.active_tab = Tab::Logs;

        let terminal = render_tab_bar(&state);
        assert_buffer_contains(&terminal, "[3] Logs");
    }

    #[test]
    fn tab_bar_snapshot_admin() {
        let mut state = AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            Duration::from_secs(5),
        );
        state.set_permission(Permission::Admin {
            admin_key: "key".into(),
        });

        let terminal = render_tab_bar(&state);
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("tab_bar_admin", content);
    }

    #[test]
    fn tab_bar_snapshot_tenant() {
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

        let terminal = render_tab_bar(&state);
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("tab_bar_tenant", content);
    }
}
