//! Main TUI application: owns the event loop, terminal, and render cycle.

use std::time::Duration;

use crossterm::event::KeyEvent;
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};

use super::action::{Action, map_key_to_action};
use super::client::admin::{AdminClient, AdminClientError};
use super::client::flight::{FlightClientError, FlightSqlClient};
use super::components::Component;
use super::components::admin::AdminPanel;
use super::components::dashboard::Dashboard;
use super::components::help::HelpOverlay;
use super::components::logs::LogsPanel;
use super::components::metrics::MetricsPanel;
use super::components::status_bar::StatusBar;
use super::components::tabs::TabBar;
use super::components::traces::TracesPanel;
use super::event::{Event, EventHandler};
use super::state::{AppState, ConnectionStatus, Permission, Tab};
use super::terminal::Tui;

/// Top-level TUI application.
pub struct App {
    pub refresh_rate: Duration,
    running: bool,
    pub state: AppState,
    tab_bar: TabBar,
    status_bar: StatusBar,
    dashboard: Dashboard,
    traces: TracesPanel,
    logs: LogsPanel,
    metrics: MetricsPanel,
    admin: AdminPanel,
    help: HelpOverlay,
    show_help: bool,
    flight_client: FlightSqlClient,
    admin_client: Option<AdminClient>,
}

impl App {
    /// Create a new `App` with the given configuration.
    pub fn new(
        url: String,
        flight_url: String,
        api_key: Option<String>,
        admin_key: Option<String>,
        refresh_rate: Duration,
        tenant_id: Option<String>,
        dataset_id: Option<String>,
    ) -> Self {
        let mut state = AppState::new(url.clone(), flight_url.clone(), refresh_rate);
        let permission = if let Some(admin_key_value) = admin_key.clone() {
            Permission::Admin {
                admin_key: admin_key_value,
            }
        } else if let (Some(api_key_value), Some(tenant_id_value)) =
            (api_key.clone(), tenant_id.clone())
        {
            Permission::Tenant {
                api_key: api_key_value,
                tenant_id: tenant_id_value,
                dataset_id: dataset_id.clone(),
            }
        } else {
            Permission::Unknown
        };
        state.set_permission(permission);

        let admin_client = admin_key
            .as_deref()
            .and_then(|key| AdminClient::new(&url, key).ok());

        let flight_client = FlightSqlClient::new(
            flight_url.clone(),
            api_key.clone(),
            tenant_id.clone(),
            dataset_id.clone(),
        );

        Self {
            refresh_rate,
            running: true,
            state,
            tab_bar: TabBar::new(),
            status_bar: StatusBar::new(),
            dashboard: Dashboard::new(),
            traces: TracesPanel::new(),
            logs: LogsPanel::new(),
            metrics: MetricsPanel::new(),
            admin: AdminPanel::new(),
            help: HelpOverlay::new(),
            show_help: false,
            flight_client,
            admin_client,
        }
    }

    /// Run the main event loop until quit.
    pub async fn run(&mut self) -> anyhow::Result<()> {
        let mut tui = Tui::new()?;
        tui.init()?;

        let mut events = EventHandler::new(self.refresh_rate);

        while self.running {
            let event = events.next().await?;
            match event {
                Event::Key(key) => {
                    self.handle_key_event(key);
                }
                Event::Tick => {
                    self.state.loading = true;
                    self.refresh_active_tab().await;
                    self.state.loading = false;
                }
                Event::Render => {
                    self.state.tick_spinner();
                    tui.terminal.draw(|frame| self.render(frame))?;
                }
            }
        }

        tui.exit()?;
        Ok(())
    }

    fn handle_key_event(&mut self, key: KeyEvent) {
        if self.show_help {
            if let Some(action) = self.help.handle_key_event(key) {
                self.handle_action(action);
            }
            return;
        }

        if let Some(action) = self.active_component_handle_key(key) {
            self.handle_action(action);
            return;
        }

        let action = map_key_to_action(key);
        self.handle_action(action);
    }

    fn active_component_handle_key(&mut self, key: KeyEvent) -> Option<Action> {
        match self.state.active_tab {
            Tab::Dashboard => self.dashboard.handle_key_event(key),
            Tab::Traces => self.traces.handle_key_event(key),
            Tab::Logs => self.logs.handle_key_event(key),
            Tab::Metrics => self.metrics.handle_key_event(key),
            Tab::Admin => self.admin.handle_key_event(key),
        }
    }

    fn handle_action(&mut self, action: Action) {
        match action {
            Action::Quit => self.running = false,
            Action::SwitchTab(idx) => self.state.switch_tab(idx),
            Action::NextTab => self.state.next_tab(),
            Action::PrevTab => self.state.prev_tab(),
            Action::ToggleHelp => self.show_help = !self.show_help,
            Action::Refresh
            | Action::ScrollUp
            | Action::ScrollDown
            | Action::Select
            | Action::Back
            | Action::Search(_)
            | Action::Confirm
            | Action::Cancel
            | Action::None => {}
        }

        if !self.show_help {
            self.update_active_tab(&action);
        }
    }

    fn update_active_tab(&mut self, action: &Action) {
        match self.state.active_tab {
            Tab::Dashboard => self.dashboard.update(action, &mut self.state),
            Tab::Traces => self.traces.update(action, &mut self.state),
            Tab::Logs => self.logs.update(action, &mut self.state),
            Tab::Metrics => self.metrics.update(action, &mut self.state),
            Tab::Admin => self.admin.update(action, &mut self.state),
        }
    }

    async fn refresh_active_tab(&mut self) {
        if self.show_help {
            return;
        }

        match self.state.active_tab {
            Tab::Dashboard => self.refresh_dashboard().await,
            Tab::Traces => self.refresh_traces().await,
            Tab::Logs => self.refresh_logs().await,
            Tab::Metrics => self.refresh_metrics().await,
            Tab::Admin => self.refresh_admin().await,
        }
    }

    async fn refresh_dashboard(&mut self) {
        match self
            .flight_client
            .query_system_metrics(
                super::components::dashboard::service_health::ServiceHealthPanel::query(),
            )
            .await
        {
            Ok(batches) => {
                self.dashboard.service_health_mut().set_data(&batches);
                self.mark_connected();
            }
            Err(err) => {
                let msg = self.flight_error_message(&err);
                self.dashboard.service_health_mut().set_error(msg);
                self.handle_flight_error(err);
                return;
            }
        }

        match self
            .flight_client
            .query_system_metrics(super::components::dashboard::wal_status::WalStatusPanel::query())
            .await
        {
            Ok(batches) => {
                self.dashboard.wal_status_mut().set_data(&batches);
                self.mark_connected();
            }
            Err(err) => {
                let msg = self.flight_error_message(&err);
                self.dashboard.wal_status_mut().set_error(msg);
                self.handle_flight_error(err);
            }
        }

        match self
            .flight_client
            .query_system_metrics(super::components::dashboard::pool_stats::PoolStatsPanel::query())
            .await
        {
            Ok(batches) => {
                self.dashboard.pool_stats_mut().set_data(&batches);
                self.mark_connected();
            }
            Err(err) => {
                let msg = self.flight_error_message(&err);
                self.dashboard.pool_stats_mut().set_error(msg);
                self.handle_flight_error(err);
            }
        }

        match self
            .flight_client
            .query_system_metrics(
                super::components::dashboard::system_resources::SystemResourcesPanel::query(),
            )
            .await
        {
            Ok(batches) => {
                self.dashboard.system_resources_mut().set_data(&batches);
                self.mark_connected();
            }
            Err(err) => {
                let msg = self.flight_error_message(&err);
                self.dashboard.system_resources_mut().set_error(msg);
                self.handle_flight_error(err);
            }
        }

        if matches!(self.state.permission, Permission::Admin { .. }) {
            match &self.admin_client {
                Some(client) => match client.list_tenants().await {
                    Ok(tenants) => {
                        self.dashboard.tenant_overview_mut().set_data(&tenants);
                        self.mark_connected();
                    }
                    Err(err) => {
                        let msg = self.admin_error_message(&err);
                        self.dashboard.tenant_overview_mut().set_error(msg);
                        self.handle_admin_error(err);
                    }
                },
                None => {
                    self.dashboard
                        .tenant_overview_mut()
                        .set_error("Authentication failed - check API key".to_string());
                    self.state
                        .set_error("Authentication failed - check API key");
                }
            }
        }
    }

    async fn refresh_traces(&mut self) {
        if let Some(trace_id) = self.traces.take_pending_trace_id() {
            match self.flight_client.get_trace(&trace_id).await {
                Ok(detail) => {
                    self.traces.set_trace_detail(detail);
                    self.mark_connected();
                }
                Err(err) => {
                    let msg = self.flight_error_message(&err);
                    self.traces.set_error(msg);
                    self.handle_flight_error(err);
                }
            }
            return;
        }

        if let Some(params) = self.traces.take_pending_search() {
            match self.flight_client.search_traces(&params).await {
                Ok(results) => {
                    self.traces.set_search_results(results);
                    self.mark_connected();
                }
                Err(err) => {
                    let msg = self.flight_error_message(&err);
                    self.traces.set_error(msg);
                    self.handle_flight_error(err);
                }
            }
        }
    }

    async fn refresh_logs(&mut self) {
        self.logs.refresh();
        if let Some(query) = self.logs.take_pending_query() {
            match self.flight_client.query_sql(&query).await {
                Ok(batches) => {
                    self.logs.set_data(&batches);
                    self.mark_connected();
                }
                Err(err) => {
                    let msg = self.flight_error_message(&err);
                    self.logs.set_error(msg);
                    self.handle_flight_error(err);
                }
            }
        }
    }

    async fn refresh_metrics(&mut self) {
        self.metrics.refresh();
        if let Some(query) = self.metrics.take_pending_query() {
            match self.flight_client.query_sql(&query).await {
                Ok(batches) => {
                    self.metrics.set_data(&batches);
                    self.mark_connected();
                }
                Err(err) => {
                    let msg = self.flight_error_message(&err);
                    self.metrics.set_error(msg);
                    self.handle_flight_error(err);
                }
            }
        }
    }

    async fn refresh_admin(&mut self) {
        let Some(client) = &self.admin_client else {
            self.admin
                .set_error("Authentication failed - check API key".to_string());
            self.state
                .set_error("Authentication failed - check API key");
            return;
        };

        match self.admin.refresh(client).await {
            Ok(()) => {
                self.admin.clear_error();
                self.mark_connected();
            }
            Err(err) => {
                self.admin.set_error(self.admin_error_message(&err));
                self.handle_admin_error(err);
            }
        }
    }

    fn mark_connected(&mut self) {
        self.state.connection_status = ConnectionStatus::Connected;
        self.state.clear_error();
    }

    fn handle_flight_error(&mut self, err: FlightClientError) {
        match err {
            FlightClientError::Auth(_) => {
                self.state
                    .set_error("Authentication failed - check API key");
            }
            FlightClientError::Connection(_) => {
                self.state.connection_status = ConnectionStatus::Disconnected;
                self.state.set_error("Disconnected - retrying on next tick");
                self.set_active_tab_error("Disconnected - retrying on next tick");
            }
            FlightClientError::Query(msg) => {
                self.state.set_error(msg);
            }
        }
    }

    fn handle_admin_error(&mut self, err: AdminClientError) {
        match err {
            AdminClientError::Unauthorized => {
                self.state
                    .set_error("Authentication failed - check API key");
            }
            AdminClientError::ConnectionError(msg) => {
                self.state.connection_status = ConnectionStatus::Disconnected;
                self.state
                    .set_error(format!("Disconnected - retrying on next tick ({msg})"));
                self.set_active_tab_error("Disconnected - retrying on next tick");
            }
            other => {
                self.state.set_error(other.to_string());
            }
        }
    }

    fn set_active_tab_error(&mut self, message: &str) {
        match self.state.active_tab {
            Tab::Dashboard => {
                self.dashboard
                    .service_health_mut()
                    .set_error(message.to_string());
                self.dashboard
                    .wal_status_mut()
                    .set_error(message.to_string());
                self.dashboard
                    .pool_stats_mut()
                    .set_error(message.to_string());
                self.dashboard
                    .tenant_overview_mut()
                    .set_error(message.to_string());
                self.dashboard
                    .system_resources_mut()
                    .set_error(message.to_string());
            }
            Tab::Traces => self.traces.set_error(message.to_string()),
            Tab::Logs => self.logs.set_error(message.to_string()),
            Tab::Metrics => self.metrics.set_error(message.to_string()),
            Tab::Admin => self.admin.set_error(message.to_string()),
        }
    }

    fn flight_error_message(&self, err: &FlightClientError) -> String {
        match err {
            FlightClientError::Auth(_) => "Authentication failed - check API key".to_string(),
            FlightClientError::Connection(_) => "Disconnected - retrying on next tick".to_string(),
            FlightClientError::Query(msg) => msg.clone(),
        }
    }

    fn admin_error_message(&self, err: &AdminClientError) -> String {
        match err {
            AdminClientError::Unauthorized => "Authentication failed - check API key".to_string(),
            AdminClientError::ConnectionError(_) => {
                "Disconnected - retrying on next tick".to_string()
            }
            other => other.to_string(),
        }
    }

    fn render(&self, frame: &mut Frame) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(0),
                Constraint::Length(1),
            ])
            .split(frame.area());

        self.tab_bar.render(frame, chunks[0], &self.state);

        match self.state.active_tab {
            Tab::Dashboard => self.dashboard.render(frame, chunks[1], &self.state),
            Tab::Traces => self.traces.render(frame, chunks[1], &self.state),
            Tab::Logs => self.logs.render(frame, chunks[1], &self.state),
            Tab::Metrics => self.metrics.render(frame, chunks[1], &self.state),
            Tab::Admin => self.admin.render(frame, chunks[1], &self.state),
        }

        self.status_bar.render(frame, chunks[2], &self.state);

        if self.show_help {
            self.help.render(frame, frame.area(), &self.state);
        }
    }
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::tui::state::{ConnectionStatus, Permission, Tab};
    use crate::tui::test_helpers::assert_buffer_contains;

    fn make_app() -> App {
        App::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            None,
            None,
            Duration::from_secs(5),
            None,
            None,
        )
    }

    #[test]
    fn app_new_sets_running() {
        let app = make_app();
        assert!(app.running);
    }

    #[test]
    fn quit_action_stops_app() {
        let mut app = make_app();
        app.handle_action(Action::Quit);
        assert!(!app.running);
    }

    #[test]
    fn switch_tab_action_updates_state() {
        let mut app = make_app();
        app.handle_action(Action::SwitchTab(2));
        assert_eq!(app.state.active_tab, Tab::Logs);
    }

    #[test]
    fn toggle_help_action_opens_overlay() {
        let mut app = make_app();
        assert!(!app.show_help);
        app.handle_action(Action::ToggleHelp);
        assert!(app.show_help);
    }

    #[test]
    fn next_tab_cycles_forward() {
        let mut app = make_app();
        assert_eq!(app.state.active_tab, Tab::Dashboard);
        app.handle_action(Action::NextTab);
        assert_eq!(app.state.active_tab, Tab::Traces);
    }

    #[test]
    fn prev_tab_cycles_backward() {
        let mut app = make_app();
        assert_eq!(app.state.active_tab, Tab::Dashboard);
        app.handle_action(Action::PrevTab);
        assert_eq!(app.state.active_tab, Tab::Metrics);
    }

    #[test]
    fn render_shows_tab_bar_and_status_bar() {
        let mut terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
        let app = make_app();
        terminal.draw(|frame| app.render(frame)).unwrap();
        assert_buffer_contains(&terminal, "[1] Dashboard");
        assert_buffer_contains(&terminal, "Service Health");
        assert_buffer_contains(&terminal, "q: Quit");
    }

    #[test]
    fn render_three_row_layout() {
        let mut terminal = Terminal::new(TestBackend::new(120, 10)).unwrap();
        let mut app = make_app();
        app.state.connection_status = ConnectionStatus::Connected;
        app.state.set_permission(Permission::Admin {
            admin_key: "key".into(),
        });
        terminal.draw(|frame| app.render(frame)).unwrap();
        assert_buffer_contains(&terminal, "SignalDB");
        assert_buffer_contains(&terminal, "[5] Admin");
        assert_buffer_contains(&terminal, "Connected");
        assert_buffer_contains(&terminal, "Admin");
    }

    #[test]
    fn render_full_layout_snapshot() {
        let mut terminal = Terminal::new(TestBackend::new(100, 10)).unwrap();
        let mut app = make_app();
        app.state.connection_status = ConnectionStatus::Connected;
        app.state.set_permission(Permission::Admin {
            admin_key: "key".into(),
        });
        terminal.draw(|frame| app.render(frame)).unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("app_full_layout", content);
    }
}
