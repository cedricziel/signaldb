//! Dataset CRUD sub-tab for the Admin panel.

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use crate::tui::widgets::text_input::TextInput;

/// Dataset list sub-tab with create/delete operations.
pub struct DatasetsPanel {
    datasets: Vec<serde_json::Value>,
    selected: usize,
    tenant_id: Option<String>,
    creating: bool,
    id_input: TextInput,
    pub pending_action: Option<DatasetAction>,
}

#[derive(Debug, Clone)]
pub enum DatasetAction {
    Create {
        tenant_id: String,
        id: String,
    },
    Delete {
        tenant_id: String,
        dataset_id: String,
    },
}

impl DatasetsPanel {
    pub fn new() -> Self {
        Self {
            datasets: Vec::new(),
            selected: 0,
            tenant_id: None,
            creating: false,
            id_input: TextInput::with_placeholder("dataset-id"),
            pending_action: None,
        }
    }

    pub fn set_tenant(&mut self, tenant_id: Option<String>) {
        if self.tenant_id != tenant_id {
            self.tenant_id = tenant_id;
            self.datasets.clear();
            self.selected = 0;
            self.creating = false;
        }
    }

    pub fn set_data(&mut self, datasets: Vec<serde_json::Value>) {
        self.datasets = datasets;
        if self.selected >= self.datasets.len() && !self.datasets.is_empty() {
            self.selected = self.datasets.len() - 1;
        }
    }

    pub fn selected_dataset_name(&self) -> Option<String> {
        self.datasets.get(self.selected).and_then(|d| {
            d.get("name")
                .or_else(|| d.get("id"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
    }

    fn selected_dataset_id(&self) -> Option<String> {
        self.datasets.get(self.selected).and_then(|d| {
            d.get("id")
                .or_else(|| d.get("name"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
    }

    pub fn request_delete(&self) -> Option<String> {
        self.selected_dataset_name()
    }

    pub fn confirm_delete(&mut self) {
        if let (Some(tenant_id), Some(dataset_id)) = (&self.tenant_id, self.selected_dataset_id()) {
            self.pending_action = Some(DatasetAction::Delete {
                tenant_id: tenant_id.clone(),
                dataset_id,
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
                if self.selected + 1 < self.datasets.len() {
                    self.selected += 1;
                }
                true
            }
            KeyCode::Char('c') => {
                if self.tenant_id.is_some() {
                    self.creating = true;
                    self.id_input.clear();
                }
                true
            }
            KeyCode::Char('d') => false,
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
                let id = self.id_input.text().to_string();
                if let Some(tenant_id) = &self.tenant_id
                    && !id.is_empty()
                {
                    self.pending_action = Some(DatasetAction::Create {
                        tenant_id: tenant_id.clone(),
                        id,
                    });
                    self.creating = false;
                }
                true
            }
            _ => {
                self.id_input.handle_key(key);
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
            Cell::from("ID").style(Style::default().add_modifier(Modifier::BOLD)),
            Cell::from("Is Default").style(Style::default().add_modifier(Modifier::BOLD)),
            Cell::from("Created").style(Style::default().add_modifier(Modifier::BOLD)),
        ])
        .height(1);

        let rows: Vec<Row> = self
            .datasets
            .iter()
            .enumerate()
            .map(|(i, d)| {
                let id = d
                    .get("name")
                    .or_else(|| d.get("id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("-");
                let is_default = d
                    .get("is_default")
                    .and_then(|v| v.as_bool())
                    .map(|b| if b { "Yes" } else { "No" })
                    .unwrap_or("-");
                let created = d.get("created_at").and_then(|v| v.as_str()).unwrap_or("-");

                let style = if i == self.selected {
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Green)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };

                Row::new(vec![
                    Cell::from(id.to_string()),
                    Cell::from(is_default.to_string()),
                    Cell::from(created.to_string()),
                ])
                .style(style)
            })
            .collect();

        let table = Table::new(
            rows,
            [
                Constraint::Percentage(40),
                Constraint::Percentage(30),
                Constraint::Percentage(30),
            ],
        )
        .header(header)
        .block(
            Block::default()
                .title(format!(
                    " Datasets [{tenant_label}] (c: create, d: delete) "
                ))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Green)),
        );

        frame.render_widget(table, area);
    }

    fn render_right_pane(&self, frame: &mut Frame, area: Rect) {
        if self.creating {
            let block = Block::default()
                .title(" Create Dataset (Enter: submit, Esc: cancel) ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Yellow));

            let inner = block.inner(area);
            frame.render_widget(block, area);

            let form_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(3), Constraint::Min(0)])
                .split(inner);

            self.id_input
                .render(frame, form_chunks[0], "Dataset ID", true);
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
            let msg = Paragraph::new("Press 'c' to create a new dataset")
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

    fn sample_datasets() -> Vec<serde_json::Value> {
        vec![
            serde_json::json!({"name": "production", "is_default": true, "created_at": "2025-01-01"}),
            serde_json::json!({"name": "staging", "is_default": false, "created_at": "2025-01-15"}),
        ]
    }

    #[test]
    fn new_panel_is_empty() {
        let panel = DatasetsPanel::new();
        assert!(panel.datasets.is_empty());
        assert!(panel.tenant_id.is_none());
    }

    #[test]
    fn set_tenant_clears_datasets() {
        let mut panel = DatasetsPanel::new();
        panel.set_data(sample_datasets());
        panel.set_tenant(Some("acme".to_string()));
        assert!(panel.datasets.is_empty());
    }

    #[test]
    fn arrow_keys_navigate() {
        let mut panel = DatasetsPanel::new();
        panel.set_data(sample_datasets());
        panel.handle_key(press(KeyCode::Down));
        assert_eq!(panel.selected, 1);
        panel.handle_key(press(KeyCode::Up));
        assert_eq!(panel.selected, 0);
    }

    #[test]
    fn c_opens_create_when_tenant_set() {
        let mut panel = DatasetsPanel::new();
        panel.tenant_id = Some("acme".to_string());
        panel.handle_key(press(KeyCode::Char('c')));
        assert!(panel.creating);
    }

    #[test]
    fn create_form_submit() {
        let mut panel = DatasetsPanel::new();
        panel.tenant_id = Some("acme".to_string());
        panel.handle_key(press(KeyCode::Char('c')));
        for c in "test-ds".chars() {
            panel.handle_key(press(KeyCode::Char(c)));
        }
        panel.handle_key(press(KeyCode::Enter));
        assert!(!panel.creating);
        assert!(panel.pending_action.is_some());
    }

    #[test]
    fn esc_cancels_create() {
        let mut panel = DatasetsPanel::new();
        panel.tenant_id = Some("acme".to_string());
        panel.handle_key(press(KeyCode::Char('c')));
        panel.handle_key(press(KeyCode::Esc));
        assert!(!panel.creating);
    }

    #[test]
    fn render_no_tenant() {
        let mut terminal = Terminal::new(TestBackend::new(100, 15)).unwrap();
        let panel = DatasetsPanel::new();
        terminal
            .draw(|frame| panel.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        assert!(content.contains("Datasets"));
        assert!(content.contains("Select a tenant first"));
    }

    #[test]
    fn render_with_data() {
        let mut terminal = Terminal::new(TestBackend::new(100, 15)).unwrap();
        let mut panel = DatasetsPanel::new();
        panel.tenant_id = Some("acme".to_string());
        panel.set_data(sample_datasets());
        terminal
            .draw(|frame| panel.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        assert!(content.contains("production"));
        assert!(content.contains("staging"));
    }

    #[test]
    fn snapshot_datasets_list() {
        let mut terminal = Terminal::new(TestBackend::new(100, 15)).unwrap();
        let mut panel = DatasetsPanel::new();
        panel.tenant_id = Some("acme".to_string());
        panel.set_data(sample_datasets());
        terminal
            .draw(|frame| panel.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("datasets_list", content);
    }

    #[test]
    fn snapshot_datasets_create() {
        let mut terminal = Terminal::new(TestBackend::new(100, 15)).unwrap();
        let mut panel = DatasetsPanel::new();
        panel.tenant_id = Some("acme".to_string());
        panel.creating = true;
        terminal
            .draw(|frame| panel.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("datasets_create", content);
    }
}
