//! Reusable selector popup modal with type-to-filter functionality.
//!
//! Provides a generic modal for selecting items from a list with real-time filtering.
//! Used for tenant/dataset selection and other list-based choices.

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::{Constraint, Flex, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph};

use crate::tui::widgets::text_input::{TextInput, TextInputAction};

/// A selectable item with an ID and display label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectorItem {
    pub id: String,
    pub label: String,
}

/// Actions returned by the selector popup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectorAction {
    /// User selected an item (returns the item's ID).
    Selected(String),
    /// User cancelled the selection (Esc).
    Cancelled,
    /// No action taken.
    None,
}

/// A modal popup for selecting items with type-to-filter support.
pub struct SelectorPopup {
    title: String,
    items: Vec<SelectorItem>,
    filtered_indices: Vec<usize>,
    list_state: ListState,
    filter_input: TextInput,
    loading: bool,
}

#[allow(dead_code)]
impl SelectorPopup {
    /// Create a new empty selector with the given title.
    pub fn new(title: &str) -> Self {
        Self {
            title: title.to_string(),
            items: Vec::new(),
            filtered_indices: Vec::new(),
            list_state: ListState::default(),
            filter_input: TextInput::new(),
            loading: false,
        }
    }

    /// Set the items to display and reset the filter.
    pub fn set_items(&mut self, items: Vec<SelectorItem>) {
        self.items = items;
        self.filter_input.clear();
        self.update_filter();
    }

    /// Set the loading state.
    pub fn set_loading(&mut self, loading: bool) {
        self.loading = loading;
    }

    /// Reset the selector (clear filter and selection).
    pub fn reset(&mut self) {
        self.filter_input.clear();
        self.update_filter();
    }

    fn update_filter(&mut self) {
        let filter_text = self.filter_input.text().to_lowercase();

        if filter_text.is_empty() {
            self.filtered_indices = (0..self.items.len()).collect();
        } else {
            self.filtered_indices = self
                .items
                .iter()
                .enumerate()
                .filter(|(_, item)| item.label.to_lowercase().contains(&filter_text))
                .map(|(idx, _)| idx)
                .collect();
        }

        if !self.filtered_indices.is_empty() {
            self.list_state.select(Some(0));
        } else {
            self.list_state.select(None);
        }
    }

    /// Handle keyboard input and return an action.
    pub fn handle_key(&mut self, key: KeyEvent) -> SelectorAction {
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                if !self.filtered_indices.is_empty() {
                    let current = self.list_state.selected().unwrap_or(0);
                    let new_idx = if current == 0 {
                        self.filtered_indices.len() - 1
                    } else {
                        current - 1
                    };
                    self.list_state.select(Some(new_idx));
                }
                SelectorAction::None
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if !self.filtered_indices.is_empty() {
                    let current = self.list_state.selected().unwrap_or(0);
                    let new_idx = if current >= self.filtered_indices.len() - 1 {
                        0
                    } else {
                        current + 1
                    };
                    self.list_state.select(Some(new_idx));
                }
                SelectorAction::None
            }
            KeyCode::Enter => {
                if let Some(selected_idx) = self.list_state.selected()
                    && let Some(&item_idx) = self.filtered_indices.get(selected_idx)
                    && let Some(item) = self.items.get(item_idx)
                {
                    return SelectorAction::Selected(item.id.clone());
                }
                SelectorAction::None
            }
            KeyCode::Esc => SelectorAction::Cancelled,
            _ => {
                let action = self.filter_input.handle_key(key);
                match action {
                    TextInputAction::Changed => {
                        self.update_filter();
                        SelectorAction::None
                    }
                    TextInputAction::Submit => {
                        if let Some(&item_idx) = self.filtered_indices.first()
                            && let Some(item) = self.items.get(item_idx)
                        {
                            return SelectorAction::Selected(item.id.clone());
                        }
                        SelectorAction::None
                    }
                    TextInputAction::Cancel => SelectorAction::Cancelled,
                    TextInputAction::Unhandled => SelectorAction::None,
                }
            }
        }
    }

    /// Render the selector popup as a centered modal.
    pub fn render(&self, frame: &mut Frame, area: Rect) {
        let modal = self.modal_area(area);
        frame.render_widget(Clear, modal);

        let block = Block::default()
            .title(format!(" {} ", self.title))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Yellow));

        if self.loading {
            let loading_text = Paragraph::new("Loading...")
                .style(Style::default().fg(Color::Gray))
                .block(block);
            frame.render_widget(loading_text, modal);
            return;
        }

        let inner = block.inner(modal);
        frame.render_widget(block, modal);

        let chunks = Layout::default()
            .direction(ratatui::layout::Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Min(1)])
            .split(inner);

        let filter_text = if self.filter_input.text().is_empty() {
            Line::from(vec![
                Span::styled("Filter: ", Style::default().fg(Color::Gray)),
                Span::styled("(type to search)", Style::default().fg(Color::DarkGray)),
            ])
        } else {
            Line::from(vec![
                Span::styled("Filter: ", Style::default().fg(Color::Gray)),
                Span::styled(self.filter_input.text(), Style::default().fg(Color::White)),
            ])
        };
        let filter_paragraph = Paragraph::new(filter_text);
        frame.render_widget(filter_paragraph, chunks[0]);

        let items: Vec<ListItem> = self
            .filtered_indices
            .iter()
            .filter_map(|&idx| self.items.get(idx))
            .map(|item| ListItem::new(item.label.clone()))
            .collect();

        let list = List::new(items)
            .highlight_style(
                Style::default()
                    .fg(Color::Yellow)
                    .bg(Color::Black)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("> ");

        frame.render_stateful_widget(list, chunks[1], &mut self.list_state.clone());
    }

    /// Calculate the centered modal area.
    fn modal_area(&self, area: Rect) -> Rect {
        let height = std::cmp::min(self.filtered_indices.len() + 4, 15) as u16;

        let horizontal = Layout::default()
            .direction(ratatui::layout::Direction::Horizontal)
            .constraints([Constraint::Length(50)])
            .flex(Flex::Center)
            .split(area);

        Layout::default()
            .direction(ratatui::layout::Direction::Vertical)
            .constraints([Constraint::Length(height)])
            .flex(Flex::Center)
            .split(horizontal[0])[0]
    }
}

#[cfg(test)]
mod tests {
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;

    fn press(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn make_items() -> Vec<SelectorItem> {
        vec![
            SelectorItem {
                id: "acme".to_string(),
                label: "Acme Corporation".to_string(),
            },
            SelectorItem {
                id: "globex".to_string(),
                label: "Globex Industries".to_string(),
            },
            SelectorItem {
                id: "initech".to_string(),
                label: "Initech LLC".to_string(),
            },
        ]
    }

    #[test]
    fn test_selector_new_empty() {
        let selector = SelectorPopup::new("Select Tenant");
        assert_eq!(selector.title, "Select Tenant");
        assert!(selector.items.is_empty());
        assert!(!selector.loading);
    }

    #[test]
    fn test_selector_set_items() {
        let mut selector = SelectorPopup::new("Select Tenant");
        let items = make_items();
        selector.set_items(items.clone());
        assert_eq!(selector.items.len(), 3);
        assert_eq!(selector.filtered_indices.len(), 3);
        assert_eq!(selector.items[0].id, "acme");
    }

    #[test]
    fn test_selector_navigate_down() {
        let mut selector = SelectorPopup::new("Select Tenant");
        selector.set_items(make_items());

        assert_eq!(selector.list_state.selected(), Some(0));

        let action = selector.handle_key(press(KeyCode::Down));
        assert_eq!(action, SelectorAction::None);
        assert_eq!(selector.list_state.selected(), Some(1));
    }

    #[test]
    fn test_selector_navigate_up() {
        let mut selector = SelectorPopup::new("Select Tenant");
        selector.set_items(make_items());

        let action = selector.handle_key(press(KeyCode::Up));
        assert_eq!(action, SelectorAction::None);
        assert_eq!(selector.list_state.selected(), Some(2));
    }

    #[test]
    fn test_selector_enter_selects() {
        let mut selector = SelectorPopup::new("Select Tenant");
        selector.set_items(make_items());

        let action = selector.handle_key(press(KeyCode::Enter));
        assert_eq!(action, SelectorAction::Selected("acme".to_string()));
    }

    #[test]
    fn test_selector_esc_cancels() {
        let mut selector = SelectorPopup::new("Select Tenant");
        selector.set_items(make_items());

        let action = selector.handle_key(press(KeyCode::Esc));
        assert_eq!(action, SelectorAction::Cancelled);
    }

    #[test]
    fn test_selector_type_to_filter() {
        let mut selector = SelectorPopup::new("Select Tenant");
        selector.set_items(make_items());

        selector.handle_key(press(KeyCode::Char('a')));
        selector.handle_key(press(KeyCode::Char('c')));

        assert_eq!(selector.filtered_indices.len(), 1);
        assert_eq!(selector.items[selector.filtered_indices[0]].id, "acme");
    }

    #[test]
    fn test_selector_filter_case_insensitive() {
        let mut selector = SelectorPopup::new("Select Tenant");
        selector.set_items(make_items());

        selector.handle_key(press(KeyCode::Char('A')));
        selector.handle_key(press(KeyCode::Char('C')));
        selector.handle_key(press(KeyCode::Char('M')));
        selector.handle_key(press(KeyCode::Char('E')));

        assert_eq!(selector.filtered_indices.len(), 1);
        assert_eq!(selector.items[selector.filtered_indices[0]].id, "acme");
    }

    #[test]
    fn test_selector_backspace_updates_filter() {
        let mut selector = SelectorPopup::new("Select Tenant");
        selector.set_items(make_items());

        selector.handle_key(press(KeyCode::Char('a')));
        selector.handle_key(press(KeyCode::Char('c')));
        assert_eq!(selector.filtered_indices.len(), 1);

        selector.handle_key(press(KeyCode::Backspace));
        assert_eq!(selector.filtered_indices.len(), 1);

        selector.handle_key(press(KeyCode::Backspace));
        assert_eq!(selector.filtered_indices.len(), 3);
    }

    #[test]
    fn test_selector_reset() {
        let mut selector = SelectorPopup::new("Select Tenant");
        selector.set_items(make_items());

        selector.handle_key(press(KeyCode::Char('a')));
        selector.handle_key(press(KeyCode::Char('c')));
        assert_eq!(selector.filtered_indices.len(), 1);

        selector.reset();
        assert_eq!(selector.filtered_indices.len(), 3);
        assert!(selector.filter_input.text().is_empty());
    }

    #[test]
    fn test_selector_j_k_navigation() {
        let mut selector = SelectorPopup::new("Select Tenant");
        selector.set_items(make_items());

        selector.handle_key(press(KeyCode::Char('j')));
        assert_eq!(selector.list_state.selected(), Some(1));

        selector.handle_key(press(KeyCode::Char('k')));
        assert_eq!(selector.list_state.selected(), Some(0));
    }

    #[test]
    fn snapshot_selector_with_items() {
        let mut terminal = Terminal::new(TestBackend::new(60, 20)).unwrap();
        let mut selector = SelectorPopup::new("Select Tenant");
        selector.set_items(make_items());

        terminal
            .draw(|frame| selector.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("selector_with_items", content);
    }

    #[test]
    fn snapshot_selector_filtered() {
        let mut terminal = Terminal::new(TestBackend::new(60, 20)).unwrap();
        let mut selector = SelectorPopup::new("Select Tenant");
        selector.set_items(make_items());

        selector.handle_key(press(KeyCode::Char('a')));
        selector.handle_key(press(KeyCode::Char('c')));

        terminal
            .draw(|frame| selector.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("selector_filtered", content);
    }

    #[test]
    fn snapshot_selector_loading() {
        let mut terminal = Terminal::new(TestBackend::new(60, 20)).unwrap();
        let mut selector = SelectorPopup::new("Select Tenant");
        selector.set_loading(true);

        terminal
            .draw(|frame| selector.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("selector_loading", content);
    }
}
