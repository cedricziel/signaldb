//! API key CRUD sub-tab for the Admin panel.

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use crate::tui::widgets::text_input::TextInput;

/// Mask an API key for display: `sk-****-{last4}`.
pub fn mask_api_key(key: &str) -> String {
    if key.len() <= 4 {
        return "sk-****".to_string();
    }
    let last4 = &key[key.len() - 4..];
    format!("sk-****-{last4}")
}

/// API key list sub-tab with create/revoke operations.
pub struct ApiKeysPanel {
    keys: Vec<serde_json::Value>,
    selected: usize,
    tenant_id: Option<String>,
    creating: bool,
    name_input: TextInput,
    pub pending_action: Option<ApiKeyAction>,
}

/// Actions that the parent admin panel should handle.
#[derive(Debug, Clone)]
pub enum ApiKeyAction {
    Create { tenant_id: String, name: String },
    Revoke { tenant_id: String, key_id: String },
    Refresh,
}

impl ApiKeysPanel {
    pub fn new() -> Self {
        Self {
            keys: Vec::new(),
            selected: 0,
            tenant_id: None,
            creating: false,
            name_input: TextInput::with_placeholder("Key name"),
            pending_action: None,
        }
    }

    pub fn set_tenant(&mut self, tenant_id: Option<String>) {
        if self.tenant_id != tenant_id {
            self.tenant_id = tenant_id;
            self.keys.clear();
            self.selected = 0;
            self.creating = false;
        }
    }

    pub fn set_data(&mut self, keys: Vec<serde_json::Value>) {
        self.keys = keys;
        if self.selected >= self.keys.len() && !self.keys.is_empty() {
            self.selected = self.keys.len() - 1;
        }
    }

    pub fn selected_key_name(&self) -> Option<String> {
        self.keys.get(self.selected).and_then(|k| {
            k.get("name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
    }

    fn selected_key_id(&self) -> Option<String> {
        self.keys.get(self.selected).and_then(|k| {
            k.get("id")
                .or_else(|| k.get("key_id"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
    }

    /// Request revocation of selected key (caller shows confirm dialog).
    pub fn request_revoke(&self) -> Option<String> {
        self.selected_key_name()
    }

    pub fn confirm_revoke(&mut self) {
        if let (Some(tenant_id), Some(key_id)) = (&self.tenant_id, self.selected_key_id()) {
            self.pending_action = Some(ApiKeyAction::Revoke {
                tenant_id: tenant_id.clone(),
                key_id,
            });
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        if self.creating {
            return self.handle_create_key(key);
        }
        self.handle_list_key(key)
    }

    fn handle_list_key(&mut self, key: KeyEvent) -> bool {
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                if self.selected > 0 {
                    self.selected -= 1;
                }
                true
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if self.selected + 1 < self.keys.len() {
                    self.selected += 1;
                }
                true
            }
            KeyCode::Char('c') => {
                if self.tenant_id.is_some() {
                    self.creating = true;
                    self.name_input.clear();
                }
                true
            }
            KeyCode::Char('d') => false, // Parent shows confirm dialog
            _ => false,
        }
    }

    fn handle_create_key(&mut self, key: KeyEvent) -> bool {
        match key.code {
            KeyCode::Esc => {
                self.creating = false;
                true
            }
            KeyCode::Enter => {
                let name = self.name_input.text().to_string();
                if let Some(tenant_id) = &self.tenant_id
                    && !name.is_empty()
                {
                    self.pending_action = Some(ApiKeyAction::Create {
                        tenant_id: tenant_id.clone(),
                        name,
                    });
                    self.creating = false;
                }
                true
            }
            _ => {
                self.name_input.handle_key(key);
                true
            }
        }
    }

    pub fn render(&self, frame: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(area);

        self.render_list(frame, chunks[0]);
        self.render_right_pane(frame, chunks[1]);
    }

    fn render_list(&self, frame: &mut Frame, area: Rect) {
        let tenant_label = self.tenant_id.as_deref().unwrap_or("none");

        let header = Row::new(vec![
            Cell::from("Name").style(Style::default().add_modifier(Modifier::BOLD)),
            Cell::from("Key").style(Style::default().add_modifier(Modifier::BOLD)),
            Cell::from("Created").style(Style::default().add_modifier(Modifier::BOLD)),
        ])
        .height(1);

        let rows: Vec<Row> = self
            .keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                let name = k.get("name").and_then(|v| v.as_str()).unwrap_or("-");
                let key_val = k
                    .get("key")
                    .or_else(|| k.get("api_key"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let masked = mask_api_key(key_val);
                let created = k.get("created_at").and_then(|v| v.as_str()).unwrap_or("-");

                let style = if i == self.selected {
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Magenta)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };

                Row::new(vec![
                    Cell::from(name.to_string()),
                    Cell::from(masked),
                    Cell::from(created.to_string()),
                ])
                .style(style)
            })
            .collect();

        let table = Table::new(
            rows,
            [
                Constraint::Percentage(35),
                Constraint::Percentage(35),
                Constraint::Percentage(30),
            ],
        )
        .header(header)
        .block(
            Block::default()
                .title(format!(
                    " API Keys [{tenant_label}] (c: create, d: revoke) "
                ))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Magenta)),
        );

        frame.render_widget(table, area);
    }

    fn render_right_pane(&self, frame: &mut Frame, area: Rect) {
        if self.creating {
            let block = Block::default()
                .title(" Create API Key (Enter: submit, Esc: cancel) ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Yellow));

            let inner = block.inner(area);
            frame.render_widget(block, area);

            let form_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(3), Constraint::Min(0)])
                .split(inner);

            self.name_input
                .render(frame, form_chunks[0], "Key Name", true);
        } else if self.tenant_id.is_none() {
            let msg = Paragraph::new("Select a tenant first (switch to Tenants tab)")
                .style(Style::default().fg(Color::DarkGray))
                .block(
                    Block::default()
                        .title(" Info ")
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(Color::DarkGray)),
                );
            frame.render_widget(msg, area);
        } else {
            let msg = Paragraph::new("Press 'c' to create a new API key")
                .style(Style::default().fg(Color::DarkGray))
                .block(
                    Block::default()
                        .title(" Info ")
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(Color::DarkGray)),
                );
            frame.render_widget(msg, area);
        }
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

    fn sample_keys() -> Vec<serde_json::Value> {
        vec![
            serde_json::json!({"id": "key-1", "name": "Production Key", "key": "sk-acme-prod-key-123", "created_at": "2025-01-01"}),
            serde_json::json!({"id": "key-2", "name": "Staging Key", "key": "sk-acme-staging-456", "created_at": "2025-01-15"}),
        ]
    }

    #[test]
    fn mask_short_key() {
        assert_eq!(mask_api_key("abc"), "sk-****");
    }

    #[test]
    fn mask_normal_key() {
        assert_eq!(mask_api_key("sk-acme-prod-key-123"), "sk-****--123");
    }

    #[test]
    fn mask_another_key() {
        assert_eq!(mask_api_key("sk-acme-staging-456"), "sk-****--456");
    }

    #[test]
    fn new_panel_is_empty() {
        let panel = ApiKeysPanel::new();
        assert!(panel.keys.is_empty());
        assert!(panel.tenant_id.is_none());
    }

    #[test]
    fn set_tenant_clears_keys() {
        let mut panel = ApiKeysPanel::new();
        panel.set_data(sample_keys());
        panel.set_tenant(Some("acme".to_string()));
        assert!(panel.keys.is_empty());
    }

    #[test]
    fn set_data_populates_keys() {
        let mut panel = ApiKeysPanel::new();
        panel.set_data(sample_keys());
        assert_eq!(panel.keys.len(), 2);
    }

    #[test]
    fn arrow_keys_navigate() {
        let mut panel = ApiKeysPanel::new();
        panel.set_data(sample_keys());
        panel.handle_key(press(KeyCode::Down));
        assert_eq!(panel.selected, 1);
        panel.handle_key(press(KeyCode::Up));
        assert_eq!(panel.selected, 0);
    }

    #[test]
    fn c_opens_create_when_tenant_set() {
        let mut panel = ApiKeysPanel::new();
        panel.tenant_id = Some("acme".to_string());
        panel.handle_key(press(KeyCode::Char('c')));
        assert!(panel.creating);
    }

    #[test]
    fn c_does_nothing_without_tenant() {
        let mut panel = ApiKeysPanel::new();
        panel.handle_key(press(KeyCode::Char('c')));
        assert!(!panel.creating);
    }

    #[test]
    fn create_form_submit() {
        let mut panel = ApiKeysPanel::new();
        panel.tenant_id = Some("acme".to_string());
        panel.handle_key(press(KeyCode::Char('c')));

        for c in "new-key".chars() {
            panel.handle_key(press(KeyCode::Char(c)));
        }
        panel.handle_key(press(KeyCode::Enter));

        assert!(!panel.creating);
        assert!(panel.pending_action.is_some());
    }

    #[test]
    fn esc_cancels_create() {
        let mut panel = ApiKeysPanel::new();
        panel.tenant_id = Some("acme".to_string());
        panel.handle_key(press(KeyCode::Char('c')));
        panel.handle_key(press(KeyCode::Esc));
        assert!(!panel.creating);
    }

    #[test]
    fn render_no_tenant() {
        let mut terminal = Terminal::new(TestBackend::new(100, 15)).unwrap();
        let panel = ApiKeysPanel::new();
        terminal
            .draw(|frame| panel.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        assert!(content.contains("API Keys"));
        assert!(content.contains("Select a tenant first"));
    }

    #[test]
    fn render_with_keys() {
        let mut terminal = Terminal::new(TestBackend::new(100, 15)).unwrap();
        let mut panel = ApiKeysPanel::new();
        panel.tenant_id = Some("acme".to_string());
        panel.set_data(sample_keys());
        terminal
            .draw(|frame| panel.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        assert!(content.contains("Production Key"));
        assert!(content.contains("sk-****"));
    }

    #[test]
    fn snapshot_api_keys_list() {
        let mut terminal = Terminal::new(TestBackend::new(100, 15)).unwrap();
        let mut panel = ApiKeysPanel::new();
        panel.tenant_id = Some("acme".to_string());
        panel.set_data(sample_keys());
        terminal
            .draw(|frame| panel.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("api_keys_list", content);
    }

    #[test]
    fn snapshot_api_keys_create() {
        let mut terminal = Terminal::new(TestBackend::new(100, 15)).unwrap();
        let mut panel = ApiKeysPanel::new();
        panel.tenant_id = Some("acme".to_string());
        panel.creating = true;
        terminal
            .draw(|frame| panel.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("api_keys_create", content);
    }
}
