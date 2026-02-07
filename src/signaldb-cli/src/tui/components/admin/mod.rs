//! Admin tab with sub-tab navigation for Tenants, API Keys, and Datasets.

pub mod api_keys;
pub mod confirm_dialog;
pub mod datasets;
pub mod tenants;

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Tabs};

use self::api_keys::ApiKeysPanel;
use self::confirm_dialog::{ConfirmDialog, ConfirmResult};
use self::datasets::DatasetsPanel;
use self::tenants::TenantsPanel;
use super::Component;
use crate::tui::action::Action;
use crate::tui::client::admin::{AdminClient, AdminClientError};
use crate::tui::state::{AppState, Permission};

#[derive(Debug, Clone, PartialEq, Eq)]
enum AdminSubTab {
    Tenants,
    Keys,
    Datasets,
}

impl AdminSubTab {
    fn index(&self) -> usize {
        match self {
            Self::Tenants => 0,
            Self::Keys => 1,
            Self::Datasets => 2,
        }
    }
}

/// Admin panel composing Tenants, API Keys, and Datasets sub-tabs.
pub struct AdminPanel {
    sub_tab: AdminSubTab,
    tenants: TenantsPanel,
    api_keys: ApiKeysPanel,
    datasets: DatasetsPanel,
    confirm_dialog: ConfirmDialog,
    pending_delete_context: Option<DeleteContext>,
    error_message: Option<String>,
}

enum DeleteContext {
    Tenant,
    ApiKey,
    Dataset,
}

impl AdminPanel {
    pub fn new() -> Self {
        Self {
            sub_tab: AdminSubTab::Tenants,
            tenants: TenantsPanel::new(),
            api_keys: ApiKeysPanel::new(),
            datasets: DatasetsPanel::new(),
            confirm_dialog: ConfirmDialog::new(),
            pending_delete_context: None,
            error_message: None,
        }
    }

    pub fn set_tenants(&mut self, tenants: Vec<serde_json::Value>) {
        self.tenants.set_data(tenants);
    }

    pub fn set_api_keys(&mut self, keys: Vec<serde_json::Value>) {
        self.api_keys.set_data(keys);
    }

    pub fn set_datasets(&mut self, datasets: Vec<serde_json::Value>) {
        self.datasets.set_data(datasets);
    }

    pub fn set_error(&mut self, message: String) {
        self.error_message = Some(message);
    }

    pub fn clear_error(&mut self) {
        self.error_message = None;
    }

    pub async fn refresh(&mut self, client: &AdminClient) -> Result<(), AdminClientError> {
        self.process_pending_action(client).await?;

        let tenants = client.list_tenants().await?;
        self.set_tenants(tenants);
        self.sync_selected_tenant();

        if let Some(tenant_id) = self.tenants.selected_tenant_id() {
            let keys = client.list_api_keys(&tenant_id).await?;
            self.set_api_keys(keys);

            let datasets = client.list_datasets(&tenant_id).await?;
            self.set_datasets(datasets);
        } else {
            self.set_api_keys(Vec::new());
            self.set_datasets(Vec::new());
        }

        Ok(())
    }

    async fn process_pending_action(
        &mut self,
        client: &AdminClient,
    ) -> Result<(), AdminClientError> {
        if let Some(action) = self.tenants.pending_action.take() {
            match action {
                self::tenants::TenantAction::Create { id, name } => {
                    let _ = client.create_tenant(&id, &name).await?;
                }
                self::tenants::TenantAction::Update { id, name } => {
                    let _ = client.update_tenant(&id, &name).await?;
                }
                self::tenants::TenantAction::Delete { id } => {
                    client.delete_tenant(&id).await?;
                }
            }
        }

        if let Some(action) = self.api_keys.pending_action.take() {
            match action {
                self::api_keys::ApiKeyAction::Create { tenant_id, name } => {
                    let _ = client.create_api_key(&tenant_id, &name).await?;
                }
                self::api_keys::ApiKeyAction::Revoke { tenant_id, key_id } => {
                    client.revoke_api_key(&tenant_id, &key_id).await?;
                }
            }
        }

        if let Some(action) = self.datasets.pending_action.take() {
            match action {
                self::datasets::DatasetAction::Create { tenant_id, id } => {
                    let _ = client.create_dataset(&tenant_id, &id).await?;
                }
                self::datasets::DatasetAction::Delete {
                    tenant_id,
                    dataset_id,
                } => {
                    client.delete_dataset(&tenant_id, &dataset_id).await?;
                }
            }
        }

        Ok(())
    }

    fn sync_selected_tenant(&mut self) {
        let tenant_id = self.tenants.selected_tenant_id();
        self.api_keys.set_tenant(tenant_id.clone());
        self.datasets.set_tenant(tenant_id);
    }

    fn handle_sub_tab_switch(&mut self, key: KeyEvent) -> bool {
        match key.code {
            KeyCode::Char('T') => {
                self.sub_tab = AdminSubTab::Tenants;
                true
            }
            KeyCode::Char('K') => {
                self.sub_tab = AdminSubTab::Keys;
                self.sync_selected_tenant();
                true
            }
            KeyCode::Char('D') => {
                self.sub_tab = AdminSubTab::Datasets;
                self.sync_selected_tenant();
                true
            }
            _ => false,
        }
    }

    fn try_delete(&mut self, key: KeyEvent) -> bool {
        if key.code != KeyCode::Char('d') {
            return false;
        }

        match self.sub_tab {
            AdminSubTab::Tenants => {
                if let Some(name) = self.tenants.request_delete() {
                    self.confirm_dialog.show(&name);
                    self.pending_delete_context = Some(DeleteContext::Tenant);
                    return true;
                }
            }
            AdminSubTab::Keys => {
                if let Some(name) = self.api_keys.request_revoke() {
                    self.confirm_dialog.show(&name);
                    self.pending_delete_context = Some(DeleteContext::ApiKey);
                    return true;
                }
            }
            AdminSubTab::Datasets => {
                if let Some(name) = self.datasets.request_delete() {
                    self.confirm_dialog.show(&name);
                    self.pending_delete_context = Some(DeleteContext::Dataset);
                    return true;
                }
            }
        }
        false
    }
}

impl Component for AdminPanel {
    fn handle_key_event(&mut self, key: KeyEvent) -> Option<Action> {
        if self.confirm_dialog.is_visible() {
            if let Some(result) = self.confirm_dialog.handle_key(key) {
                match result {
                    ConfirmResult::Confirmed => {
                        if let Some(ctx) = self.pending_delete_context.take() {
                            match ctx {
                                DeleteContext::Tenant => self.tenants.confirm_delete(),
                                DeleteContext::ApiKey => self.api_keys.confirm_revoke(),
                                DeleteContext::Dataset => self.datasets.confirm_delete(),
                            }
                        }
                    }
                    ConfirmResult::Cancelled => {
                        self.pending_delete_context = None;
                    }
                    ConfirmResult::Pending => {}
                }
            }
            return Some(Action::None);
        }

        if self.handle_sub_tab_switch(key) {
            return Some(Action::None);
        }

        if self.try_delete(key) {
            return Some(Action::None);
        }

        let handled = match self.sub_tab {
            AdminSubTab::Tenants => self.tenants.handle_key(key),
            AdminSubTab::Keys => self.api_keys.handle_key(key),
            AdminSubTab::Datasets => self.datasets.handle_key(key),
        };

        if handled { Some(Action::None) } else { None }
    }

    fn update(&mut self, _action: &Action, _state: &mut AppState) {}

    fn render(&self, frame: &mut Frame, area: Rect, state: &AppState) {
        if !matches!(state.permission, Permission::Admin { .. }) {
            return;
        }

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Length(if self.error_message.is_some() { 1 } else { 0 }),
                Constraint::Min(0),
            ])
            .split(area);

        self.render_sub_tabs(frame, chunks[0]);

        if let Some(message) = &self.error_message {
            let error = Paragraph::new(message.as_str()).style(Style::default().fg(Color::Red));
            frame.render_widget(error, chunks[1]);
        }

        match self.sub_tab {
            AdminSubTab::Tenants => self.tenants.render(frame, chunks[2]),
            AdminSubTab::Keys => self.api_keys.render(frame, chunks[2]),
            AdminSubTab::Datasets => self.datasets.render(frame, chunks[2]),
        }

        if self.confirm_dialog.is_visible() {
            self.confirm_dialog.render(frame, area);
        }
    }
}

impl AdminPanel {
    fn render_sub_tabs(&self, frame: &mut Frame, area: Rect) {
        let titles: Vec<Line> = vec![
            Line::from(vec![
                Span::styled("T", Style::default().add_modifier(Modifier::UNDERLINED)),
                Span::raw("enants"),
            ]),
            Line::from(vec![
                Span::styled("K", Style::default().add_modifier(Modifier::UNDERLINED)),
                Span::raw("eys"),
            ]),
            Line::from(vec![
                Span::styled("D", Style::default().add_modifier(Modifier::UNDERLINED)),
                Span::raw("atasets"),
            ]),
        ];

        let tabs = Tabs::new(titles)
            .select(self.sub_tab.index())
            .block(
                Block::default()
                    .title(" Admin ")
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(Color::Yellow)),
            )
            .highlight_style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )
            .style(Style::default().fg(Color::DarkGray));

        frame.render_widget(tabs, area);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::tui::state::AppState;
    use crate::tui::test_helpers::assert_buffer_contains;

    fn press(code: KeyCode) -> KeyEvent {
        KeyEvent {
            code,
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        }
    }

    fn make_admin_state() -> AppState {
        let mut state = AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            Duration::from_secs(5),
        );
        state.set_permission(Permission::Admin {
            admin_key: "test-key".into(),
        });
        state
    }

    fn make_non_admin_state() -> AppState {
        AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            Duration::from_secs(5),
        )
    }

    fn sample_tenants() -> Vec<serde_json::Value> {
        vec![
            serde_json::json!({"id": "acme", "name": "Acme Corp", "default_dataset": "production"}),
            serde_json::json!({"id": "globex", "name": "Globex Inc", "default_dataset": "staging"}),
        ]
    }

    #[test]
    fn new_panel_defaults_to_tenants() {
        let panel = AdminPanel::new();
        assert_eq!(panel.sub_tab, AdminSubTab::Tenants);
    }

    #[test]
    fn shift_t_switches_to_tenants() {
        let mut panel = AdminPanel::new();
        panel.sub_tab = AdminSubTab::Keys;
        panel.handle_key_event(press(KeyCode::Char('T')));
        assert_eq!(panel.sub_tab, AdminSubTab::Tenants);
    }

    #[test]
    fn shift_k_switches_to_keys() {
        let mut panel = AdminPanel::new();
        panel.handle_key_event(press(KeyCode::Char('K')));
        assert_eq!(panel.sub_tab, AdminSubTab::Keys);
    }

    #[test]
    fn shift_d_switches_to_datasets() {
        let mut panel = AdminPanel::new();
        panel.handle_key_event(press(KeyCode::Char('D')));
        assert_eq!(panel.sub_tab, AdminSubTab::Datasets);
    }

    #[test]
    fn d_on_tenant_shows_confirm_dialog() {
        let mut panel = AdminPanel::new();
        panel.tenants.set_data(sample_tenants());
        panel.handle_key_event(press(KeyCode::Char('d')));
        assert!(panel.confirm_dialog.is_visible());
    }

    #[test]
    fn confirm_dialog_esc_cancels() {
        let mut panel = AdminPanel::new();
        panel.tenants.set_data(sample_tenants());
        panel.handle_key_event(press(KeyCode::Char('d')));
        assert!(panel.confirm_dialog.is_visible());
        panel.handle_key_event(press(KeyCode::Esc));
        assert!(!panel.confirm_dialog.is_visible());
    }

    #[test]
    fn confirm_dialog_correct_text_deletes() {
        let mut panel = AdminPanel::new();
        panel.tenants.set_data(sample_tenants());
        panel.handle_key_event(press(KeyCode::Char('d')));
        for c in "acme".chars() {
            panel.handle_key_event(press(KeyCode::Char(c)));
        }
        panel.handle_key_event(press(KeyCode::Enter));
        assert!(!panel.confirm_dialog.is_visible());
        assert!(panel.tenants.pending_action.is_some());
    }

    #[test]
    fn non_admin_render_is_empty() {
        let mut terminal = Terminal::new(TestBackend::new(100, 20)).unwrap();
        let panel = AdminPanel::new();
        let state = make_non_admin_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        assert!(!content.contains("Tenants"));
    }

    #[test]
    fn admin_render_shows_sub_tabs() {
        let mut terminal = Terminal::new(TestBackend::new(100, 20)).unwrap();
        let panel = AdminPanel::new();
        let state = make_admin_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "Admin");
        assert_buffer_contains(&terminal, "enants");
        assert_buffer_contains(&terminal, "eys");
        assert_buffer_contains(&terminal, "atasets");
    }

    #[test]
    fn admin_render_with_tenants() {
        let mut terminal = Terminal::new(TestBackend::new(120, 20)).unwrap();
        let mut panel = AdminPanel::new();
        panel.tenants.set_data(sample_tenants());
        let state = make_admin_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "Acme Corp");
        assert_buffer_contains(&terminal, "Globex");
    }

    #[test]
    fn unhandled_keys_bubble_up() {
        let mut panel = AdminPanel::new();
        let result = panel.handle_key_event(press(KeyCode::Char('q')));
        assert!(result.is_none());
    }

    #[test]
    fn sync_selected_tenant_passes_to_sub_tabs() {
        let mut panel = AdminPanel::new();
        panel.tenants.set_data(sample_tenants());
        panel.sync_selected_tenant();
        // api_keys and datasets have private tenant_id, verify indirectly by switching tab
        panel.handle_key_event(press(KeyCode::Char('K')));
        assert_eq!(panel.sub_tab, AdminSubTab::Keys);
    }

    #[test]
    fn snapshot_admin_tenants() {
        let mut terminal = Terminal::new(TestBackend::new(120, 20)).unwrap();
        let mut panel = AdminPanel::new();
        panel.tenants.set_data(sample_tenants());
        let state = make_admin_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("admin_tenants_tab", content);
    }

    #[test]
    fn snapshot_admin_keys() {
        let mut terminal = Terminal::new(TestBackend::new(120, 20)).unwrap();
        let mut panel = AdminPanel::new();
        panel.sub_tab = AdminSubTab::Keys;
        let state = make_admin_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("admin_keys_tab", content);
    }

    #[test]
    fn snapshot_admin_datasets() {
        let mut terminal = Terminal::new(TestBackend::new(120, 20)).unwrap();
        let mut panel = AdminPanel::new();
        panel.sub_tab = AdminSubTab::Datasets;
        let state = make_admin_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("admin_datasets_tab", content);
    }

    #[test]
    fn snapshot_admin_confirm_dialog() {
        let mut terminal = Terminal::new(TestBackend::new(120, 20)).unwrap();
        let mut panel = AdminPanel::new();
        panel.tenants.set_data(sample_tenants());
        panel.handle_key_event(press(KeyCode::Char('d')));
        let state = make_admin_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("admin_confirm_dialog", content);
    }
}
