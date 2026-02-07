//! Context bar component showing active tenant, dataset, time range, and key hints

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::tui::state::{AppState, Permission};

/// Display-only context bar showing active tenant, dataset, time range, and key hints.
/// Renders as a single line with colored segments.
pub struct ContextBar;

impl ContextBar {
    /// Create a new ContextBar
    pub fn new() -> Self {
        Self
    }

    /// Render the context bar into the given area
    pub fn render(&self, frame: &mut Frame, area: Rect, state: &AppState) {
        let mut spans = Vec::new();

        // Tenant segment
        let tenant_name = state.active_tenant.as_deref().unwrap_or("(none)");
        let tenant_color = if state.active_tenant.is_some() {
            Color::Cyan
        } else {
            Color::DarkGray
        };
        spans.push(Span::styled(
            format!("Tenant: {tenant_name}"),
            Style::default()
                .fg(tenant_color)
                .add_modifier(Modifier::BOLD),
        ));

        // Separator
        spans.push(Span::styled(" │ ", Style::default().fg(Color::DarkGray)));

        // Dataset segment
        let dataset_name = state.active_dataset.as_deref().unwrap_or("(none)");
        let dataset_color = if state.active_dataset.is_some() {
            Color::Cyan
        } else {
            Color::DarkGray
        };
        spans.push(Span::styled(
            format!("Dataset: {dataset_name}"),
            Style::default()
                .fg(dataset_color)
                .add_modifier(Modifier::BOLD),
        ));

        // Separator
        spans.push(Span::styled(" │ ", Style::default().fg(Color::DarkGray)));

        // Time range segment
        spans.push(Span::styled(
            format!("⏱ {}", state.time_range.label()),
            Style::default().fg(Color::Green),
        ));

        // Separator
        spans.push(Span::styled(" │ ", Style::default().fg(Color::DarkGray)));

        // Key hints segment
        let hints = match &state.permission {
            Permission::Admin { .. } => "Ctrl+T Ctrl+D : Cmd".to_string(),
            Permission::Tenant { .. } => "Ctrl+D : Cmd".to_string(),
            Permission::Unknown => "(not connected)".to_string(),
        };
        spans.push(Span::styled(hints, Style::default().fg(Color::Gray)));

        let line = Line::from(spans);
        let paragraph = Paragraph::new(line);
        frame.render_widget(paragraph, area);
    }
}

impl Default for ContextBar {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::tui::state::ConnectionStatus;
    use crate::tui::test_helpers::assert_buffer_contains;

    fn render_context_bar(state: &AppState) -> Terminal<TestBackend> {
        let mut terminal = Terminal::new(TestBackend::new(140, 1)).unwrap();
        let bar = ContextBar::new();
        terminal
            .draw(|frame| {
                bar.render(frame, frame.area(), state);
            })
            .unwrap();
        terminal
    }

    #[test]
    fn test_context_bar_renders_tenant_and_dataset() {
        let mut state = AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            Duration::from_secs(5),
        );
        state.active_tenant = Some("acme".to_string());
        state.active_dataset = Some("production".to_string());

        let terminal = render_context_bar(&state);
        assert_buffer_contains(&terminal, "acme");
        assert_buffer_contains(&terminal, "production");
    }

    #[test]
    fn test_context_bar_renders_none_when_no_context() {
        let state = AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            Duration::from_secs(5),
        );

        let terminal = render_context_bar(&state);
        assert_buffer_contains(&terminal, "(none)");
    }

    #[test]
    fn test_context_bar_renders_time_range() {
        let state = AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            Duration::from_secs(5),
        );

        let terminal = render_context_bar(&state);
        assert_buffer_contains(&terminal, "Last 15m");
    }

    #[test]
    fn test_context_bar_no_selector_hint_for_tenant_user() {
        let mut state = AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            Duration::from_secs(5),
        );
        state.set_permission(Permission::Tenant {
            api_key: "key".into(),
            tenant_id: "acme".into(),
            dataset_id: Some("production".into()),
        });

        let terminal = render_context_bar(&state);
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        assert!(
            !content.contains("Ctrl+T"),
            "Tenant user should not see Ctrl+T hint"
        );
        assert!(
            content.contains("Ctrl+D"),
            "Tenant user should see Ctrl+D hint"
        );
    }

    #[test]
    fn test_context_bar_admin_shows_all_hints() {
        let mut state = AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            Duration::from_secs(5),
        );
        state.set_permission(Permission::Admin {
            admin_key: "key".into(),
        });

        let terminal = render_context_bar(&state);
        assert_buffer_contains(&terminal, "Ctrl+T");
        assert_buffer_contains(&terminal, "Ctrl+D");
    }

    #[test]
    fn test_context_bar_unknown_shows_not_connected() {
        let state = AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            Duration::from_secs(5),
        );

        let terminal = render_context_bar(&state);
        assert_buffer_contains(&terminal, "(not connected)");
    }

    #[test]
    fn test_context_bar_snapshot_admin() {
        let mut state = AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            Duration::from_secs(5),
        );
        state.set_permission(Permission::Admin {
            admin_key: "key".into(),
        });
        state.active_tenant = Some("acme".to_string());
        state.active_dataset = Some("production".to_string());
        state.connection_status = ConnectionStatus::Connected;

        let terminal = render_context_bar(&state);
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("context_bar_admin", content);
    }

    #[test]
    fn test_context_bar_snapshot_tenant() {
        let mut state = AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            Duration::from_secs(5),
        );
        state.set_permission(Permission::Tenant {
            api_key: "key".into(),
            tenant_id: "acme".into(),
            dataset_id: Some("production".into()),
        });
        state.connection_status = ConnectionStatus::Connected;

        let terminal = render_context_bar(&state);
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("context_bar_tenant", content);
    }

    #[test]
    fn test_context_bar_snapshot_unknown() {
        let state = AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            Duration::from_secs(5),
        );

        let terminal = render_context_bar(&state);
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("context_bar_unknown", content);
    }
}
