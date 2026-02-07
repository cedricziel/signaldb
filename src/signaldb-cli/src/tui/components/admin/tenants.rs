//! Tenant CRUD sub-tab for the Admin panel.

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use crate::tui::widgets::text_input::TextInput;

/// View mode for the right pane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TenantView {
    /// No tenant selected — show instructions.
    Empty,
    /// Show tenant detail.
    Detail(usize),
    /// Create form.
    Create,
    /// Edit form for tenant at index.
    Edit(usize),
}

/// Tenant list sub-tab with CRUD operations.
pub struct TenantsPanel {
    tenants: Vec<serde_json::Value>,
    selected: usize,
    view: TenantView,
    id_input: TextInput,
    name_input: TextInput,
    form_focus: usize,
    pub pending_action: Option<TenantAction>,
}

/// Actions that the parent admin panel should handle.
#[derive(Debug, Clone)]
pub enum TenantAction {
    Create { id: String, name: String },
    Update { id: String, name: String },
    Delete { id: String },
    Refresh,
}

impl TenantsPanel {
    pub fn new() -> Self {
        Self {
            tenants: Vec::new(),
            selected: 0,
            view: TenantView::Empty,
            id_input: TextInput::with_placeholder("tenant-id"),
            name_input: TextInput::with_placeholder("Tenant Name"),
            form_focus: 0,
            pending_action: None,
        }
    }

    pub fn set_data(&mut self, tenants: Vec<serde_json::Value>) {
        self.tenants = tenants;
        if self.selected >= self.tenants.len() && !self.tenants.is_empty() {
            self.selected = self.tenants.len() - 1;
        }
    }

    pub fn selected_tenant(&self) -> Option<&serde_json::Value> {
        self.tenants.get(self.selected)
    }

    pub fn selected_tenant_id(&self) -> Option<String> {
        self.selected_tenant()
            .and_then(|t| t.get("id").and_then(|v| v.as_str()).map(|s| s.to_string()))
    }

    /// Request deletion of the selected tenant (caller shows confirm dialog).
    pub fn request_delete(&self) -> Option<String> {
        self.selected_tenant_id()
    }

    /// Confirm deletion was approved.
    pub fn confirm_delete(&mut self) {
        if let Some(id) = self.selected_tenant_id() {
            self.pending_action = Some(TenantAction::Delete { id });
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        match &self.view {
            TenantView::Create | TenantView::Edit(_) => self.handle_form_key(key),
            _ => self.handle_list_key(key),
        }
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
                if self.selected + 1 < self.tenants.len() {
                    self.selected += 1;
                }
                true
            }
            KeyCode::Enter => {
                if !self.tenants.is_empty() {
                    self.view = TenantView::Detail(self.selected);
                }
                true
            }
            KeyCode::Char('c') => {
                self.view = TenantView::Create;
                self.id_input.clear();
                self.name_input.clear();
                self.form_focus = 0;
                true
            }
            KeyCode::Char('e') => {
                if let Some(tenant) = self.selected_tenant() {
                    let id = tenant
                        .get("id")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string();
                    let name = tenant
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string();
                    self.id_input.set_text(&id);
                    self.name_input.set_text(&name);
                    self.view = TenantView::Edit(self.selected);
                    self.form_focus = 1;
                }
                true
            }
            KeyCode::Char('d') => {
                // Parent will show confirm dialog
                false
            }
            _ => false,
        }
    }

    fn handle_form_key(&mut self, key: KeyEvent) -> bool {
        match key.code {
            KeyCode::Esc => {
                self.view = TenantView::Empty;
                true
            }
            KeyCode::Tab => {
                self.form_focus = (self.form_focus + 1) % 2;
                true
            }
            KeyCode::Enter => {
                let id = self.id_input.text().to_string();
                let name = self.name_input.text().to_string();
                if !id.is_empty() && !name.is_empty() {
                    match &self.view {
                        TenantView::Create => {
                            self.pending_action = Some(TenantAction::Create {
                                id: id.clone(),
                                name: name.clone(),
                            });
                        }
                        TenantView::Edit(_) => {
                            self.pending_action = Some(TenantAction::Update {
                                id: id.clone(),
                                name: name.clone(),
                            });
                        }
                        _ => {}
                    }
                    self.view = TenantView::Empty;
                }
                true
            }
            _ => {
                if self.form_focus == 0 {
                    self.id_input.handle_key(key);
                } else {
                    self.name_input.handle_key(key);
                }
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
        let header = Row::new(vec![
            Cell::from("ID").style(Style::default().add_modifier(Modifier::BOLD)),
            Cell::from("Name").style(Style::default().add_modifier(Modifier::BOLD)),
            Cell::from("Default Dataset").style(Style::default().add_modifier(Modifier::BOLD)),
        ])
        .height(1);

        let rows: Vec<Row> = self
            .tenants
            .iter()
            .enumerate()
            .map(|(i, t)| {
                let id = t.get("id").and_then(|v| v.as_str()).unwrap_or("-");
                let name = t.get("name").and_then(|v| v.as_str()).unwrap_or("-");
                let default_ds = t
                    .get("default_dataset")
                    .and_then(|v| v.as_str())
                    .unwrap_or("-");

                let style = if i == self.selected {
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };

                Row::new(vec![
                    Cell::from(id.to_string()),
                    Cell::from(name.to_string()),
                    Cell::from(default_ds.to_string()),
                ])
                .style(style)
            })
            .collect();

        let table = Table::new(
            rows,
            [
                Constraint::Percentage(30),
                Constraint::Percentage(40),
                Constraint::Percentage(30),
            ],
        )
        .header(header)
        .block(
            Block::default()
                .title(" Tenants (c: create, e: edit, d: delete) ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan)),
        );

        frame.render_widget(table, area);
    }

    fn render_right_pane(&self, frame: &mut Frame, area: Rect) {
        match &self.view {
            TenantView::Empty => {
                let msg = Paragraph::new("Select a tenant or press 'c' to create")
                    .style(Style::default().fg(Color::DarkGray))
                    .block(
                        Block::default()
                            .title(" Detail ")
                            .borders(Borders::ALL)
                            .border_style(Style::default().fg(Color::DarkGray)),
                    );
                frame.render_widget(msg, area);
            }
            TenantView::Detail(idx) => {
                if let Some(tenant) = self.tenants.get(*idx) {
                    let id = tenant.get("id").and_then(|v| v.as_str()).unwrap_or("-");
                    let name = tenant.get("name").and_then(|v| v.as_str()).unwrap_or("-");
                    let default_ds = tenant
                        .get("default_dataset")
                        .and_then(|v| v.as_str())
                        .unwrap_or("-");

                    let detail = format!(
                        "ID:              {id}\nName:            {name}\nDefault Dataset: {default_ds}"
                    );
                    let widget = Paragraph::new(detail)
                        .style(Style::default().fg(Color::White))
                        .block(
                            Block::default()
                                .title(" Tenant Detail ")
                                .borders(Borders::ALL)
                                .border_style(Style::default().fg(Color::Green)),
                        );
                    frame.render_widget(widget, area);
                }
            }
            TenantView::Create | TenantView::Edit(_) => {
                let title = if matches!(self.view, TenantView::Create) {
                    " Create Tenant (Tab: next field, Enter: submit, Esc: cancel) "
                } else {
                    " Edit Tenant (Tab: next field, Enter: submit, Esc: cancel) "
                };

                let block = Block::default()
                    .title(title)
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(Color::Yellow));

                let inner = block.inner(area);
                frame.render_widget(block, area);

                let form_chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Length(3),
                        Constraint::Length(3),
                        Constraint::Min(0),
                    ])
                    .split(inner);

                let id_editable = matches!(self.view, TenantView::Create);
                self.id_input.render(
                    frame,
                    form_chunks[0],
                    "ID",
                    self.form_focus == 0 && id_editable,
                );
                self.name_input
                    .render(frame, form_chunks[1], "Name", self.form_focus == 1);
            }
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

    fn sample_tenants() -> Vec<serde_json::Value> {
        vec![
            serde_json::json!({"id": "acme", "name": "Acme Corp", "default_dataset": "production"}),
            serde_json::json!({"id": "globex", "name": "Globex Inc", "default_dataset": "staging"}),
        ]
    }

    #[test]
    fn new_panel_is_empty() {
        let panel = TenantsPanel::new();
        assert!(panel.tenants.is_empty());
        assert_eq!(panel.selected, 0);
        assert_eq!(panel.view, TenantView::Empty);
    }

    #[test]
    fn set_data_updates_tenants() {
        let mut panel = TenantsPanel::new();
        panel.set_data(sample_tenants());
        assert_eq!(panel.tenants.len(), 2);
    }

    #[test]
    fn set_data_clamps_selection() {
        let mut panel = TenantsPanel::new();
        panel.selected = 5;
        panel.set_data(sample_tenants());
        assert_eq!(panel.selected, 1);
    }

    #[test]
    fn arrow_keys_navigate() {
        let mut panel = TenantsPanel::new();
        panel.set_data(sample_tenants());
        assert_eq!(panel.selected, 0);

        panel.handle_key(press(KeyCode::Down));
        assert_eq!(panel.selected, 1);

        panel.handle_key(press(KeyCode::Down));
        assert_eq!(panel.selected, 1); // Clamped

        panel.handle_key(press(KeyCode::Up));
        assert_eq!(panel.selected, 0);

        panel.handle_key(press(KeyCode::Up));
        assert_eq!(panel.selected, 0); // Clamped
    }

    #[test]
    fn enter_shows_detail() {
        let mut panel = TenantsPanel::new();
        panel.set_data(sample_tenants());
        panel.handle_key(press(KeyCode::Enter));
        assert_eq!(panel.view, TenantView::Detail(0));
    }

    #[test]
    fn c_opens_create_form() {
        let mut panel = TenantsPanel::new();
        panel.handle_key(press(KeyCode::Char('c')));
        assert_eq!(panel.view, TenantView::Create);
    }

    #[test]
    fn e_opens_edit_form() {
        let mut panel = TenantsPanel::new();
        panel.set_data(sample_tenants());
        panel.handle_key(press(KeyCode::Char('e')));
        assert!(matches!(panel.view, TenantView::Edit(0)));
        assert_eq!(panel.id_input.text(), "acme");
        assert_eq!(panel.name_input.text(), "Acme Corp");
    }

    #[test]
    fn create_form_submit() {
        let mut panel = TenantsPanel::new();
        panel.handle_key(press(KeyCode::Char('c')));

        // Type ID
        for c in "test-id".chars() {
            panel.handle_key(press(KeyCode::Char(c)));
        }
        // Tab to name
        panel.handle_key(press(KeyCode::Tab));
        // Type name
        for c in "Test Name".chars() {
            panel.handle_key(press(KeyCode::Char(c)));
        }
        panel.handle_key(press(KeyCode::Enter));

        assert!(panel.pending_action.is_some());
        if let Some(TenantAction::Create { id, name }) = &panel.pending_action {
            assert_eq!(id, "test-id");
            assert_eq!(name, "Test Name");
        } else {
            panic!("Expected Create action");
        }
    }

    #[test]
    fn esc_cancels_form() {
        let mut panel = TenantsPanel::new();
        panel.handle_key(press(KeyCode::Char('c')));
        panel.handle_key(press(KeyCode::Esc));
        assert_eq!(panel.view, TenantView::Empty);
    }

    #[test]
    fn selected_tenant_id_returns_id() {
        let mut panel = TenantsPanel::new();
        panel.set_data(sample_tenants());
        assert_eq!(panel.selected_tenant_id(), Some("acme".to_string()));
    }

    #[test]
    fn render_empty_list() {
        let mut terminal = Terminal::new(TestBackend::new(100, 15)).unwrap();
        let panel = TenantsPanel::new();
        terminal
            .draw(|frame| panel.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        assert!(content.contains("Tenants"));
        assert!(content.contains("Detail"));
    }

    #[test]
    fn render_with_data() {
        let mut terminal = Terminal::new(TestBackend::new(100, 15)).unwrap();
        let mut panel = TenantsPanel::new();
        panel.set_data(sample_tenants());
        terminal
            .draw(|frame| panel.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        assert!(content.contains("acme"));
        assert!(content.contains("Globex"));
    }

    #[test]
    fn snapshot_tenants_list() {
        let mut terminal = Terminal::new(TestBackend::new(100, 15)).unwrap();
        let mut panel = TenantsPanel::new();
        panel.set_data(sample_tenants());
        terminal
            .draw(|frame| panel.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("tenants_list", content);
    }

    #[test]
    fn snapshot_tenant_detail() {
        let mut terminal = Terminal::new(TestBackend::new(100, 15)).unwrap();
        let mut panel = TenantsPanel::new();
        panel.set_data(sample_tenants());
        panel.view = TenantView::Detail(0);
        terminal
            .draw(|frame| panel.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("tenant_detail", content);
    }

    #[test]
    fn snapshot_tenant_create_form() {
        let mut terminal = Terminal::new(TestBackend::new(100, 15)).unwrap();
        let mut panel = TenantsPanel::new();
        panel.view = TenantView::Create;
        terminal
            .draw(|frame| panel.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("tenant_create_form", content);
    }
}
