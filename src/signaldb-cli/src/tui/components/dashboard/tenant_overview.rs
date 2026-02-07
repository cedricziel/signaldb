//! Tenant overview panel for the dashboard (admin-only).

use crossterm::event::KeyEvent;
use ratatui::Frame;
use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Paragraph, Row, Table};

use crate::tui::action::Action;
use crate::tui::components::Component;
use crate::tui::state::{AppState, Permission};

#[derive(Debug, Clone)]
enum PanelData {
    Loading,
    Loaded(Vec<TenantRow>),
    Error(String),
}

#[derive(Debug, Clone)]
struct TenantRow {
    id: String,
    name: String,
}

/// Tenant overview panel, visible only to admin users.
pub struct TenantOverviewPanel {
    data: PanelData,
}

impl TenantOverviewPanel {
    pub fn new() -> Self {
        Self {
            data: PanelData::Loading,
        }
    }

    /// Populate panel from admin API tenant listing.
    pub fn set_data(&mut self, tenants: &[serde_json::Value]) {
        let rows: Vec<TenantRow> = tenants
            .iter()
            .map(|t| TenantRow {
                id: t["id"].as_str().unwrap_or("-").to_string(),
                name: t["name"].as_str().unwrap_or("-").to_string(),
            })
            .collect();
        self.data = PanelData::Loaded(rows);
    }

    /// Set an error on the panel.
    pub fn set_error(&mut self, msg: String) {
        self.data = PanelData::Error(msg);
    }

    /// Whether this panel should be visible for the given permission.
    #[allow(dead_code)] // Used by targeted panel tests and future app-level gating
    pub fn is_visible(state: &AppState) -> bool {
        matches!(state.permission, Permission::Admin { .. })
    }
}

impl Component for TenantOverviewPanel {
    fn handle_key_event(&mut self, _key: KeyEvent) -> Option<Action> {
        None
    }

    fn update(&mut self, _action: &Action, _state: &mut AppState) {}

    fn render(&self, frame: &mut Frame, area: Rect, _state: &AppState) {
        let block = Block::default()
            .title(" Tenant Overview ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Green));

        match &self.data {
            PanelData::Loading => {
                let loading = Paragraph::new("Loading...")
                    .style(Style::default().fg(Color::DarkGray))
                    .block(block);
                frame.render_widget(loading, area);
            }
            PanelData::Error(msg) => {
                let error = Paragraph::new(format!("Failed to load: {msg}\n(r to retry)"))
                    .style(Style::default().fg(Color::Red))
                    .block(block);
                frame.render_widget(error, area);
            }
            PanelData::Loaded(rows) => {
                if rows.is_empty() {
                    let empty = Paragraph::new("No tenants")
                        .style(Style::default().fg(Color::DarkGray))
                        .block(block);
                    frame.render_widget(empty, area);
                    return;
                }

                let header = Row::new(vec!["ID", "Name"])
                    .style(Style::default().add_modifier(Modifier::BOLD))
                    .bottom_margin(1);

                let table_rows: Vec<Row> = rows
                    .iter()
                    .map(|r| Row::new(vec![r.id.clone(), r.name.clone()]))
                    .collect();

                let widths = [Constraint::Percentage(40), Constraint::Percentage(60)];
                let table = Table::new(table_rows, widths).header(header).block(block);
                frame.render_widget(table, area);
            }
        }
    }
}

/// Placeholder for non-admin users.
pub struct TenantPlaceholderPanel;

impl TenantPlaceholderPanel {
    pub fn new() -> Self {
        Self
    }
}

impl Component for TenantPlaceholderPanel {
    fn handle_key_event(&mut self, _key: KeyEvent) -> Option<Action> {
        None
    }

    fn update(&mut self, _action: &Action, _state: &mut AppState) {}

    fn render(&self, frame: &mut Frame, area: Rect, _state: &AppState) {
        let block = Block::default()
            .title(" Tenant Overview ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray));
        let content = Paragraph::new("Admin access required")
            .style(Style::default().fg(Color::DarkGray))
            .block(block);
        frame.render_widget(content, area);
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

    fn make_state() -> AppState {
        AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            Duration::from_secs(5),
        )
    }

    fn make_admin_state() -> AppState {
        let mut state = make_state();
        state.set_permission(Permission::Admin {
            admin_key: "test-key".into(),
        });
        state
    }

    #[test]
    fn renders_loading_state() {
        let mut terminal = Terminal::new(TestBackend::new(40, 10)).unwrap();
        let panel = TenantOverviewPanel::new();
        let state = make_admin_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "Loading...");
        assert_buffer_contains(&terminal, "Tenant Overview");
    }

    #[test]
    fn renders_error_state() {
        let mut terminal = Terminal::new(TestBackend::new(50, 10)).unwrap();
        let mut panel = TenantOverviewPanel::new();
        panel.set_error("unauthorized".into());
        let state = make_admin_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "Failed to load: unauthorized");
    }

    #[test]
    fn renders_loaded_tenants() {
        let mut terminal = Terminal::new(TestBackend::new(50, 10)).unwrap();
        let mut panel = TenantOverviewPanel::new();
        panel.set_data(&[
            serde_json::json!({"id": "acme", "name": "Acme Corp"}),
            serde_json::json!({"id": "globex", "name": "Globex Inc"}),
        ]);
        let state = make_admin_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "acme");
        assert_buffer_contains(&terminal, "Acme Corp");
        assert_buffer_contains(&terminal, "globex");
    }

    #[test]
    fn is_visible_for_admin() {
        let state = make_admin_state();
        assert!(TenantOverviewPanel::is_visible(&state));
    }

    #[test]
    fn is_hidden_for_non_admin() {
        let state = make_state();
        assert!(!TenantOverviewPanel::is_visible(&state));
    }

    #[test]
    fn placeholder_renders_message() {
        let mut terminal = Terminal::new(TestBackend::new(40, 10)).unwrap();
        let panel = TenantPlaceholderPanel::new();
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "Admin access required");
    }

    #[test]
    fn snapshot_loading() {
        let mut terminal = Terminal::new(TestBackend::new(40, 8)).unwrap();
        let panel = TenantOverviewPanel::new();
        let state = make_admin_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("tenant_overview_loading", content);
    }

    #[test]
    fn snapshot_loaded() {
        let mut terminal = Terminal::new(TestBackend::new(50, 8)).unwrap();
        let mut panel = TenantOverviewPanel::new();
        panel.set_data(&[serde_json::json!({"id": "acme", "name": "Acme Corp"})]);
        let state = make_admin_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("tenant_overview_loaded", content);
    }
}
