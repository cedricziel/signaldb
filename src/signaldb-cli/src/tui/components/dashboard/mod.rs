//! Dashboard component with 2x2+1 grid layout for monitoring panels.

pub mod pool_stats;
pub mod service_health;
pub mod system_resources;
pub mod tenant_overview;
pub mod wal_status;

use crossterm::event::KeyEvent;
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};

use self::pool_stats::PoolStatsPanel;
use self::service_health::ServiceHealthPanel;
use self::system_resources::SystemResourcesPanel;
use self::tenant_overview::{TenantOverviewPanel, TenantPlaceholderPanel};
use self::wal_status::WalStatusPanel;
use super::Component;
use crate::tui::action::Action;
use crate::tui::state::{AppState, Permission};

/// Dashboard tab component with 5 monitoring panels arranged in a 2x2+1 grid.
///
/// ```text
/// +------------------+------------------+
/// | Service Health   | WAL Status       |
/// +------------------+------------------+
/// | Connection Pool  | Tenant Overview  |
/// +------------------+------------------+
/// | System Resources                    |
/// +-------------------------------------+
/// ```
pub struct Dashboard {
    service_health: ServiceHealthPanel,
    wal_status: WalStatusPanel,
    pool_stats: PoolStatsPanel,
    tenant_overview: TenantOverviewPanel,
    tenant_placeholder: TenantPlaceholderPanel,
    system_resources: SystemResourcesPanel,
}

impl Dashboard {
    pub fn new() -> Self {
        Self {
            service_health: ServiceHealthPanel::new(),
            wal_status: WalStatusPanel::new(),
            pool_stats: PoolStatsPanel::new(),
            tenant_overview: TenantOverviewPanel::new(),
            tenant_placeholder: TenantPlaceholderPanel::new(),
            system_resources: SystemResourcesPanel::new(),
        }
    }

    /// Access the service health panel for data updates.
    pub fn service_health_mut(&mut self) -> &mut ServiceHealthPanel {
        &mut self.service_health
    }

    /// Access the WAL status panel for data updates.
    pub fn wal_status_mut(&mut self) -> &mut WalStatusPanel {
        &mut self.wal_status
    }

    /// Access the pool stats panel for data updates.
    pub fn pool_stats_mut(&mut self) -> &mut PoolStatsPanel {
        &mut self.pool_stats
    }

    /// Access the tenant overview panel for data updates.
    pub fn tenant_overview_mut(&mut self) -> &mut TenantOverviewPanel {
        &mut self.tenant_overview
    }

    /// Access the system resources panel for data updates.
    pub fn system_resources_mut(&mut self) -> &mut SystemResourcesPanel {
        &mut self.system_resources
    }
}

impl Component for Dashboard {
    fn handle_key_event(&mut self, key: KeyEvent) -> Option<Action> {
        self.service_health
            .handle_key_event(key)
            .or_else(|| self.wal_status.handle_key_event(key))
            .or_else(|| self.pool_stats.handle_key_event(key))
            .or_else(|| self.tenant_overview.handle_key_event(key))
            .or_else(|| self.system_resources.handle_key_event(key))
    }

    fn update(&mut self, action: &Action, state: &mut AppState) {
        self.service_health.update(action, state);
        self.wal_status.update(action, state);
        self.pool_stats.update(action, state);
        self.tenant_overview.update(action, state);
        self.system_resources.update(action, state);
    }

    fn render(&self, frame: &mut Frame, area: Rect, state: &AppState) {
        let vertical = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Percentage(40),
                Constraint::Percentage(40),
                Constraint::Percentage(20),
            ])
            .split(area);

        let top_row = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(vertical[0]);

        let mid_row = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(vertical[1]);

        self.service_health.render(frame, top_row[0], state);
        self.wal_status.render(frame, top_row[1], state);
        self.pool_stats.render(frame, mid_row[0], state);

        let is_admin = matches!(state.permission, Permission::Admin { .. });
        if is_admin {
            self.tenant_overview.render(frame, mid_row[1], state);
        } else {
            self.tenant_placeholder.render(frame, mid_row[1], state);
        }

        self.system_resources.render(frame, vertical[2], state);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::tui::state::{AppState, Permission};
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
    fn renders_all_panels_loading() {
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        let dashboard = Dashboard::new();
        let state = make_state();
        terminal
            .draw(|frame| dashboard.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "Service Health");
        assert_buffer_contains(&terminal, "WAL Status");
        assert_buffer_contains(&terminal, "Connection Pool");
        assert_buffer_contains(&terminal, "Tenant Overview");
        assert_buffer_contains(&terminal, "System Resources");
    }

    #[test]
    fn shows_admin_required_for_non_admin() {
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        let dashboard = Dashboard::new();
        let state = make_state();
        terminal
            .draw(|frame| dashboard.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "Admin access required");
    }

    #[test]
    fn shows_tenant_panel_for_admin() {
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        let dashboard = Dashboard::new();
        let state = make_admin_state();
        terminal
            .draw(|frame| dashboard.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "Tenant Overview");
        assert_buffer_contains(&terminal, "Loading...");
    }

    #[test]
    fn snapshot_dashboard_loading() {
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        let dashboard = Dashboard::new();
        let state = make_state();
        terminal
            .draw(|frame| dashboard.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("dashboard_loading", content);
    }

    #[test]
    fn snapshot_dashboard_admin_loading() {
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        let dashboard = Dashboard::new();
        let state = make_admin_state();
        terminal
            .draw(|frame| dashboard.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("dashboard_admin_loading", content);
    }

    #[test]
    fn handle_key_event_returns_none() {
        let mut dashboard = Dashboard::new();
        let key = crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Char('x'),
            crossterm::event::KeyModifiers::NONE,
        );
        assert!(dashboard.handle_key_event(key).is_none());
    }
}
