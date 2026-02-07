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
use super::components::command_palette::{CommandPalette, PaletteAction, PaletteCommand};
use super::components::context_bar::ContextBar;
use super::components::dashboard::Dashboard;
use super::components::help::HelpOverlay;
use super::components::logs::LogsPanel;
use super::components::metrics::MetricsPanel;
use super::components::selector::{SelectorAction, SelectorPopup};
use super::components::status_bar::StatusBar;
use super::components::tabs::TabBar;
use super::components::time_range::{TimeRangeAction, TimeRangeSelector};
use super::components::traces::TracesPanel;
use super::event::{Event, EventHandler};
use super::state::{ActiveOverlay, AppState, ConnectionStatus, Permission, Tab, TimeRange};
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
    context_bar: ContextBar,
    context_selector: SelectorPopup,
    command_palette: CommandPalette,
    time_range_selector: TimeRangeSelector,
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
            context_bar: ContextBar::new(),
            context_selector: SelectorPopup::new("Select Context"),
            command_palette: CommandPalette::new(),
            time_range_selector: TimeRangeSelector::new(),
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
                    self.populate_selectors().await;
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
        if self.state.active_overlay != ActiveOverlay::None {
            if let Some(action) = self.handle_overlay_key(key) {
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

    fn handle_overlay_key(&mut self, key: KeyEvent) -> Option<Action> {
        match self.state.active_overlay {
            ActiveOverlay::Help => self.help.handle_key_event(key),
            ActiveOverlay::ContextSelector => match self.context_selector.handle_key(key) {
                SelectorAction::Selected {
                    id,
                    parent_id: None,
                } => Some(Action::SetTenant(id)),
                SelectorAction::Selected {
                    id,
                    parent_id: Some(tenant),
                } => {
                    self.handle_action(Action::SetTenant(tenant));
                    Some(Action::SetDataset(id))
                }
                SelectorAction::Cancelled => Some(Action::CloseOverlay),
                SelectorAction::None => None,
            },
            ActiveOverlay::CommandPalette => match self.command_palette.handle_key(key) {
                PaletteAction::Execute(cmd) => match cmd {
                    PaletteCommand::SetTenant(name) => Some(Action::SetTenant(name)),
                    PaletteCommand::SetDataset(name) => Some(Action::SetDataset(name)),
                    PaletteCommand::Refresh => {
                        self.command_palette.reset();
                        Some(Action::CloseOverlay)
                    }
                    PaletteCommand::Quit => Some(Action::Quit),
                    PaletteCommand::SetTimeRange(label) => {
                        Some(Action::ExecuteCommand(format!("time {label}")))
                    }
                },
                PaletteAction::Cancelled => Some(Action::CloseOverlay),
                PaletteAction::None => None,
            },
            ActiveOverlay::TimeRangeSelector => match self.time_range_selector.handle_key(key) {
                TimeRangeAction::Selected(tr) => Some(Action::SetTimeRange(tr)),
                TimeRangeAction::Cancelled => Some(Action::CloseOverlay),
                TimeRangeAction::None => None,
            },
            ActiveOverlay::None => None,
        }
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
            Action::ToggleHelp => {
                if self.state.active_overlay == ActiveOverlay::Help {
                    self.state.active_overlay = ActiveOverlay::None;
                } else {
                    self.state.active_overlay = ActiveOverlay::Help;
                }
            }
            Action::CloseOverlay => {
                self.state.active_overlay = ActiveOverlay::None;
            }
            Action::OpenContextSelector => {
                self.context_selector.set_loading(true);
                self.state.active_overlay = ActiveOverlay::ContextSelector;
            }
            Action::OpenCommandPalette => {
                self.state.active_overlay = ActiveOverlay::CommandPalette;
            }
            Action::OpenTimeRangeSelector => {
                self.state.active_overlay = ActiveOverlay::TimeRangeSelector;
            }
            Action::SetTenant(ref tenant) => {
                self.state.active_tenant = Some(tenant.clone());
                self.state.active_dataset = None;
                self.flight_client.set_tenant_id(Some(tenant.clone()));
                self.flight_client.set_dataset_id(None);
                self.state.pending_context_refresh = true;
                self.state.loading = true;
                self.state.active_overlay = ActiveOverlay::None;
                self.context_selector.reset();
                self.command_palette.reset();
            }
            Action::SetDataset(ref dataset) => {
                self.state.active_dataset = Some(dataset.clone());
                self.flight_client.set_dataset_id(Some(dataset.clone()));
                self.state.pending_context_refresh = true;
                self.state.loading = true;
                self.state.active_overlay = ActiveOverlay::None;
                self.context_selector.reset();
                self.command_palette.reset();
            }
            Action::SetTimeRange(ref tr) => {
                self.state.time_range = tr.clone();
                self.state.pending_context_refresh = true;
                self.state.loading = true;
                self.state.active_overlay = ActiveOverlay::None;
                self.time_range_selector.reset();
            }
            Action::ExecuteCommand(ref cmd) => {
                if let Some(label) = cmd.strip_prefix("time ") {
                    let matched = TimeRange::presets()
                        .into_iter()
                        .find(|(name, _)| name.to_lowercase().contains(&label.to_lowercase()))
                        .map(|(_, tr)| tr);
                    if let Some(tr) = matched {
                        self.state.time_range = tr;
                        self.state.pending_context_refresh = true;
                    }
                }
                self.state.active_overlay = ActiveOverlay::None;
                self.command_palette.reset();
            }
            Action::Refresh => {
                self.state.pending_context_refresh = true;
                self.state.loading = true;
            }
            Action::ScrollUp
            | Action::ScrollDown
            | Action::Select
            | Action::Back
            | Action::Search(_)
            | Action::Confirm
            | Action::Cancel
            | Action::None => {}
        }

        if self.state.active_overlay == ActiveOverlay::None {
            self.update_active_tab(&action);
            self.sync_loading_state();
        }
    }

    /// Set `loading` whenever any panel has a pending data operation.
    fn sync_loading_state(&mut self) {
        let has_pending =
            self.traces.has_pending() || self.logs.has_pending() || self.metrics.has_pending();
        if has_pending {
            self.state.loading = true;
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

    async fn populate_selectors(&mut self) {
        use super::components::selector::SelectorItem;

        match self.state.active_overlay {
            ActiveOverlay::ContextSelector if self.context_selector.is_loading() => {
                let mut selector_items = Vec::new();
                let mut tenant_names = Vec::new();
                let mut dataset_names = Vec::new();

                if let Some(client) = &self.admin_client {
                    match client.list_tenants().await {
                        Ok(tenants) => {
                            for tenant in &tenants {
                                let Some(tenant_id) = tenant
                                    .get("id")
                                    .and_then(|value| value.as_str())
                                    .map(ToString::to_string)
                                else {
                                    continue;
                                };

                                let tenant_name = tenant
                                    .get("name")
                                    .and_then(|value| value.as_str())
                                    .unwrap_or(&tenant_id);
                                let tenant_label = if tenant_name == tenant_id {
                                    tenant_id.clone()
                                } else {
                                    format!("{tenant_name} ({tenant_id})")
                                };
                                selector_items.push(SelectorItem {
                                    id: tenant_id.clone(),
                                    label: tenant_label,
                                    depth: 0,
                                    parent_id: None,
                                });
                                tenant_names.push(tenant_id.clone());

                                if let Ok(datasets) = client.list_datasets(&tenant_id).await {
                                    for dataset in datasets {
                                        if let Some(dataset_name) = dataset
                                            .get("name")
                                            .and_then(|value| value.as_str())
                                            .map(ToString::to_string)
                                        {
                                            selector_items.push(SelectorItem {
                                                id: dataset_name.clone(),
                                                label: dataset_name.clone(),
                                                depth: 1,
                                                parent_id: Some(tenant_id.clone()),
                                            });
                                            dataset_names.push(dataset_name);
                                        }
                                    }
                                }
                            }

                            self.context_selector.set_items(selector_items);
                            self.command_palette.set_available_tenants(tenant_names);
                            self.command_palette.set_available_datasets(dataset_names);
                        }
                        Err(_) => {
                            self.context_selector.set_items(vec![]);
                        }
                    }
                } else if let Permission::Tenant {
                    tenant_id,
                    dataset_id,
                    ..
                } = &self.state.permission
                {
                    selector_items.push(SelectorItem {
                        id: tenant_id.clone(),
                        label: tenant_id.clone(),
                        depth: 0,
                        parent_id: None,
                    });
                    tenant_names.push(tenant_id.clone());

                    if let Some(dataset) = dataset_id {
                        selector_items.push(SelectorItem {
                            id: dataset.clone(),
                            label: dataset.clone(),
                            depth: 1,
                            parent_id: Some(tenant_id.clone()),
                        });
                        dataset_names.push(dataset.clone());
                    }

                    self.context_selector.set_items(selector_items);
                    self.command_palette.set_available_tenants(tenant_names);
                    self.command_palette.set_available_datasets(dataset_names);
                } else {
                    self.context_selector.set_items(vec![]);
                }
            }
            _ => {}
        }
    }

    async fn refresh_active_tab(&mut self) {
        if self.state.active_overlay != ActiveOverlay::None && !self.state.pending_context_refresh {
            return;
        }

        self.state.pending_context_refresh = false;

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
                Constraint::Length(1),
                Constraint::Min(0),
                Constraint::Length(1),
            ])
            .split(frame.area());

        self.tab_bar.render(frame, chunks[0], &self.state);
        self.context_bar.render(frame, chunks[1], &self.state);

        match self.state.active_tab {
            Tab::Dashboard => self.dashboard.render(frame, chunks[2], &self.state),
            Tab::Traces => self.traces.render(frame, chunks[2], &self.state),
            Tab::Logs => self.logs.render(frame, chunks[2], &self.state),
            Tab::Metrics => self.metrics.render(frame, chunks[2], &self.state),
            Tab::Admin => self.admin.render(frame, chunks[2], &self.state),
        }

        self.status_bar.render(frame, chunks[3], &self.state);

        match self.state.active_overlay {
            ActiveOverlay::Help => self.help.render(frame, frame.area(), &self.state),
            ActiveOverlay::ContextSelector => self.context_selector.render(frame, frame.area()),
            ActiveOverlay::CommandPalette => self.command_palette.render(frame, chunks[2]),
            ActiveOverlay::TimeRangeSelector => {
                self.time_range_selector.render(frame, frame.area())
            }
            ActiveOverlay::None => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::tui::state::{ActiveOverlay, ConnectionStatus, Permission, Tab};
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
        assert_eq!(app.state.active_overlay, ActiveOverlay::None);
        app.handle_action(Action::ToggleHelp);
        assert_eq!(app.state.active_overlay, ActiveOverlay::Help);
    }

    #[test]
    fn toggle_help_closes_when_already_open() {
        let mut app = make_app();
        app.handle_action(Action::ToggleHelp);
        assert_eq!(app.state.active_overlay, ActiveOverlay::Help);
        app.handle_action(Action::ToggleHelp);
        assert_eq!(app.state.active_overlay, ActiveOverlay::None);
    }

    #[test]
    fn overlay_exclusivity_replaces_previous() {
        let mut app = make_app();
        app.state.set_permission(Permission::Admin {
            admin_key: "key".into(),
        });
        app.handle_action(Action::ToggleHelp);
        assert_eq!(app.state.active_overlay, ActiveOverlay::Help);
        app.handle_action(Action::OpenContextSelector);
        assert_eq!(app.state.active_overlay, ActiveOverlay::ContextSelector);
    }

    #[test]
    fn close_overlay_action_clears_overlay() {
        let mut app = make_app();
        app.handle_action(Action::ToggleHelp);
        app.handle_action(Action::CloseOverlay);
        assert_eq!(app.state.active_overlay, ActiveOverlay::None);
    }

    #[test]
    fn open_context_selector_available_for_all_users() {
        let mut app = make_app();
        app.handle_action(Action::OpenContextSelector);
        assert_eq!(app.state.active_overlay, ActiveOverlay::ContextSelector);

        app.state.set_permission(Permission::Admin {
            admin_key: "key".into(),
        });
        app.handle_action(Action::OpenContextSelector);
        assert_eq!(app.state.active_overlay, ActiveOverlay::ContextSelector);
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

    #[test]
    fn set_tenant_updates_state_and_closes_overlay() {
        let mut app = make_app();
        app.state.active_overlay = ActiveOverlay::ContextSelector;
        app.handle_action(Action::SetTenant("acme".into()));

        assert_eq!(app.state.active_tenant, Some("acme".into()));
        assert_eq!(app.state.active_dataset, None);
        assert!(app.state.pending_context_refresh);
        assert_eq!(app.state.active_overlay, ActiveOverlay::None);
    }

    #[test]
    fn set_dataset_updates_state_and_closes_overlay() {
        let mut app = make_app();
        app.state.active_overlay = ActiveOverlay::ContextSelector;
        app.handle_action(Action::SetDataset("prod".into()));

        assert_eq!(app.state.active_dataset, Some("prod".into()));
        assert!(app.state.pending_context_refresh);
        assert_eq!(app.state.active_overlay, ActiveOverlay::None);
    }

    #[test]
    fn set_time_range_updates_state() {
        let mut app = make_app();
        let tr = TimeRange::Relative(Duration::from_secs(3600));
        app.state.active_overlay = ActiveOverlay::TimeRangeSelector;
        app.handle_action(Action::SetTimeRange(tr.clone()));

        assert_eq!(app.state.time_range, tr);
        assert!(app.state.pending_context_refresh);
        assert_eq!(app.state.active_overlay, ActiveOverlay::None);
    }

    #[test]
    fn execute_command_time_matches_preset() {
        let mut app = make_app();
        app.state.active_overlay = ActiveOverlay::CommandPalette;
        app.handle_action(Action::ExecuteCommand("time 1h".into()));

        assert_eq!(
            app.state.time_range,
            TimeRange::Relative(Duration::from_secs(3600))
        );
        assert!(app.state.pending_context_refresh);
        assert_eq!(app.state.active_overlay, ActiveOverlay::None);
    }

    #[test]
    fn execute_command_time_no_match_keeps_default() {
        let mut app = make_app();
        let default_tr = app.state.time_range.clone();
        app.handle_action(Action::ExecuteCommand("time xyz".into()));

        assert_eq!(app.state.time_range, default_tr);
    }

    #[test]
    fn set_tenant_resets_dataset() {
        let mut app = make_app();
        app.state.active_dataset = Some("staging".into());

        app.handle_action(Action::SetTenant("globex".into()));
        assert_eq!(app.state.active_tenant, Some("globex".into()));
        assert_eq!(app.state.active_dataset, None);
    }

    #[test]
    fn open_time_range_selector_sets_overlay() {
        let mut app = make_app();
        app.handle_action(Action::OpenTimeRangeSelector);
        assert_eq!(app.state.active_overlay, ActiveOverlay::TimeRangeSelector);
    }
}
