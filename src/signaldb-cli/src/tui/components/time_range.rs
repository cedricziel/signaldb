//! Time range selector popup modal with preset ranges and custom ISO 8601 input.
//!
//! Provides a modal for selecting predefined time ranges or entering custom
//! absolute time ranges in ISO 8601 format.

use chrono::{DateTime, Utc};
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::{Constraint, Flex, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState};

use crate::tui::state::TimeRange;
use crate::tui::widgets::text_input::{TextInput, TextInputAction};

/// Actions returned by the time range selector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeRangeAction {
    /// User selected a time range.
    Selected(TimeRange),
    /// User cancelled the selection (Esc).
    Cancelled,
    /// No action taken.
    None,
}

/// A modal popup for selecting time ranges with preset options and custom input.
pub struct TimeRangeSelector {
    /// List of preset time ranges (label, TimeRange).
    presets: Vec<(String, TimeRange)>,
    /// Current selection index in the list.
    list_state: ListState,
    /// Text input for custom ISO 8601 time range.
    custom_input: TextInput,
    /// Whether we're in custom input mode.
    custom_mode: bool,
}

impl TimeRangeSelector {
    /// Create a new time range selector with presets.
    pub fn new() -> Self {
        let mut presets = TimeRange::presets();
        presets.push(("Custom...".to_string(), TimeRange::default()));

        let mut list_state = ListState::default();
        list_state.select(Some(0));

        Self {
            presets,
            list_state,
            custom_input: TextInput::with_placeholder("2024-01-01T00:00:00Z/2024-01-02T00:00:00Z"),
            custom_mode: false,
        }
    }

    /// Handle keyboard input and return an action.
    pub fn handle_key(&mut self, key: KeyEvent) -> TimeRangeAction {
        if self.custom_mode {
            return self.handle_custom_mode_key(key);
        }

        match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                if !self.presets.is_empty() {
                    let current = self.list_state.selected().unwrap_or(0);
                    let new_idx = if current == 0 {
                        self.presets.len() - 1
                    } else {
                        current - 1
                    };
                    self.list_state.select(Some(new_idx));
                }
                TimeRangeAction::None
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if !self.presets.is_empty() {
                    let current = self.list_state.selected().unwrap_or(0);
                    let new_idx = if current >= self.presets.len() - 1 {
                        0
                    } else {
                        current + 1
                    };
                    self.list_state.select(Some(new_idx));
                }
                TimeRangeAction::None
            }
            KeyCode::Enter => {
                if let Some(selected_idx) = self.list_state.selected()
                    && let Some((label, _)) = self.presets.get(selected_idx)
                {
                    if label == "Custom..." {
                        self.custom_mode = true;
                        self.custom_input.clear();
                        return TimeRangeAction::None;
                    }

                    if let Some((_, time_range)) = self.presets.get(selected_idx) {
                        return TimeRangeAction::Selected(time_range.clone());
                    }
                }
                TimeRangeAction::None
            }
            KeyCode::Esc => TimeRangeAction::Cancelled,
            _ => TimeRangeAction::None,
        }
    }

    /// Handle key events in custom input mode.
    fn handle_custom_mode_key(&mut self, key: KeyEvent) -> TimeRangeAction {
        match key.code {
            KeyCode::Esc => {
                self.custom_mode = false;
                self.custom_input.clear();
                TimeRangeAction::None
            }
            KeyCode::Enter => {
                let input = self.custom_input.text().trim();
                if let Ok(time_range) = self.parse_iso8601_interval(input) {
                    self.custom_mode = false;
                    self.custom_input.clear();
                    return TimeRangeAction::Selected(time_range);
                }
                // Invalid input, stay in custom mode
                TimeRangeAction::None
            }
            _ => {
                let action = self.custom_input.handle_key(key);
                match action {
                    TextInputAction::Changed => TimeRangeAction::None,
                    TextInputAction::Submit => {
                        let input = self.custom_input.text().trim();
                        if let Ok(time_range) = self.parse_iso8601_interval(input) {
                            self.custom_mode = false;
                            self.custom_input.clear();
                            return TimeRangeAction::Selected(time_range);
                        }
                        TimeRangeAction::None
                    }
                    TextInputAction::Cancel => {
                        self.custom_mode = false;
                        self.custom_input.clear();
                        TimeRangeAction::None
                    }
                    TextInputAction::Unhandled => TimeRangeAction::None,
                }
            }
        }
    }

    /// Parse ISO 8601 interval format: "2024-01-01T00:00:00Z/2024-01-02T00:00:00Z"
    fn parse_iso8601_interval(&self, input: &str) -> Result<TimeRange, String> {
        let parts: Vec<&str> = input.split('/').collect();
        if parts.len() != 2 {
            return Err("Invalid format: expected start/end".to_string());
        }

        let start = DateTime::parse_from_rfc3339(parts[0])
            .map_err(|_| "Invalid start time".to_string())?
            .with_timezone(&Utc);

        let end = DateTime::parse_from_rfc3339(parts[1])
            .map_err(|_| "Invalid end time".to_string())?
            .with_timezone(&Utc);

        Ok(TimeRange::Absolute { start, end })
    }

    /// Render the time range selector as a centered modal.
    pub fn render(&self, frame: &mut Frame, area: Rect) {
        let modal = self.modal_area(area);
        frame.render_widget(Clear, modal);

        let block = Block::default()
            .title(" Time Range ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Yellow));

        let inner = block.inner(modal);
        frame.render_widget(block, modal);

        if self.custom_mode {
            self.render_custom_mode(frame, inner);
        } else {
            self.render_preset_mode(frame, inner);
        }
    }

    /// Render the preset selection mode.
    fn render_preset_mode(&self, frame: &mut Frame, area: Rect) {
        let items: Vec<ListItem> = self
            .presets
            .iter()
            .map(|(label, _)| ListItem::new(label.clone()))
            .collect();

        let list = List::new(items)
            .highlight_style(
                Style::default()
                    .fg(Color::Yellow)
                    .bg(Color::Black)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("> ");

        frame.render_stateful_widget(list, area, &mut self.list_state.clone());
    }

    /// Render the custom input mode.
    fn render_custom_mode(&self, frame: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(ratatui::layout::Direction::Vertical)
            .constraints([Constraint::Min(3), Constraint::Length(3)])
            .split(area);

        // Show presets above
        let items: Vec<ListItem> = self
            .presets
            .iter()
            .map(|(label, _)| ListItem::new(label.clone()))
            .collect();

        let list = List::new(items)
            .highlight_style(
                Style::default()
                    .fg(Color::Yellow)
                    .bg(Color::Black)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("> ");

        frame.render_stateful_widget(list, chunks[0], &mut self.list_state.clone());

        // Show custom input below
        self.custom_input
            .render(frame, chunks[1], "ISO 8601 Interval", true);
    }

    /// Reset the selector to normal mode.
    pub fn reset(&mut self) {
        self.custom_mode = false;
        self.custom_input.clear();
        self.list_state.select(Some(0));
    }

    /// Calculate the centered modal area.
    fn modal_area(&self, area: Rect) -> Rect {
        let height = if self.custom_mode { 15 } else { 12 } as u16;

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

impl Default for TimeRangeSelector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use chrono::Datelike;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use std::time::Duration;

    use super::*;

    fn press(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    #[test]
    fn test_time_range_selector_new() {
        let selector = TimeRangeSelector::new();
        assert!(!selector.presets.is_empty());
        assert!(!selector.custom_mode);
        assert_eq!(selector.list_state.selected(), Some(0));
    }

    #[test]
    fn test_time_range_selector_presets_listed() {
        let selector = TimeRangeSelector::new();
        let preset_count = TimeRange::presets().len();
        assert_eq!(selector.presets.len(), preset_count + 1); // +1 for "Custom..."
    }

    #[test]
    fn test_time_range_selector_navigate_down() {
        let mut selector = TimeRangeSelector::new();
        assert_eq!(selector.list_state.selected(), Some(0));

        let action = selector.handle_key(press(KeyCode::Down));
        assert_eq!(action, TimeRangeAction::None);
        assert_eq!(selector.list_state.selected(), Some(1));
    }

    #[test]
    fn test_time_range_selector_navigate_up() {
        let mut selector = TimeRangeSelector::new();
        assert_eq!(selector.list_state.selected(), Some(0));

        let action = selector.handle_key(press(KeyCode::Up));
        assert_eq!(action, TimeRangeAction::None);
        // Should wrap to last item
        assert_eq!(
            selector.list_state.selected(),
            Some(selector.presets.len() - 1)
        );
    }

    #[test]
    fn test_time_range_selector_select_preset() {
        let mut selector = TimeRangeSelector::new();
        let action = selector.handle_key(press(KeyCode::Enter));

        match action {
            TimeRangeAction::Selected(TimeRange::Relative(d)) => {
                assert_eq!(d, Duration::from_secs(900)); // First preset is 15m
            }
            _ => panic!("Expected Selected action with Relative time range"),
        }
    }

    #[test]
    fn test_time_range_selector_esc_cancels() {
        let mut selector = TimeRangeSelector::new();
        let action = selector.handle_key(press(KeyCode::Esc));
        assert_eq!(action, TimeRangeAction::Cancelled);
    }

    #[test]
    fn test_time_range_selector_custom_mode() {
        let mut selector = TimeRangeSelector::new();

        // Navigate to "Custom..." (last item)
        while selector.list_state.selected() != Some(selector.presets.len() - 1) {
            selector.handle_key(press(KeyCode::Down));
        }

        // Press Enter to enter custom mode
        let action = selector.handle_key(press(KeyCode::Enter));
        assert_eq!(action, TimeRangeAction::None);
        assert!(selector.custom_mode);
    }

    #[test]
    fn test_time_range_selector_custom_mode_esc() {
        let mut selector = TimeRangeSelector::new();

        // Navigate to "Custom..." and enter custom mode
        while selector.list_state.selected() != Some(selector.presets.len() - 1) {
            selector.handle_key(press(KeyCode::Down));
        }
        selector.handle_key(press(KeyCode::Enter));
        assert!(selector.custom_mode);

        // Press Esc to exit custom mode
        let action = selector.handle_key(press(KeyCode::Esc));
        assert_eq!(action, TimeRangeAction::None);
        assert!(!selector.custom_mode);
    }

    #[test]
    fn test_time_range_selector_reset() {
        let mut selector = TimeRangeSelector::new();

        // Enter custom mode
        while selector.list_state.selected() != Some(selector.presets.len() - 1) {
            selector.handle_key(press(KeyCode::Down));
        }
        selector.handle_key(press(KeyCode::Enter));
        assert!(selector.custom_mode);

        // Reset
        selector.reset();
        assert!(!selector.custom_mode);
        assert_eq!(selector.list_state.selected(), Some(0));
        assert!(selector.custom_input.text().is_empty());
    }

    #[test]
    fn test_time_range_selector_parse_iso8601_valid() {
        let selector = TimeRangeSelector::new();
        let result = selector.parse_iso8601_interval("2024-01-01T00:00:00Z/2024-01-02T00:00:00Z");
        assert!(result.is_ok());

        if let Ok(TimeRange::Absolute { start, end }) = result {
            assert_eq!(start.year(), 2024);
            assert_eq!(start.month(), 1);
            assert_eq!(start.day(), 1);
            assert_eq!(end.day(), 2);
        }
    }

    #[test]
    fn test_time_range_selector_parse_iso8601_invalid_format() {
        let selector = TimeRangeSelector::new();
        let result = selector.parse_iso8601_interval("invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_time_range_selector_parse_iso8601_invalid_date() {
        let selector = TimeRangeSelector::new();
        let result = selector.parse_iso8601_interval("2024-13-01T00:00:00Z/2024-01-02T00:00:00Z");
        assert!(result.is_err());
    }

    #[test]
    fn test_time_range_selector_navigate_j_k() {
        let mut selector = TimeRangeSelector::new();
        selector.handle_key(press(KeyCode::Char('j')));
        assert_eq!(selector.list_state.selected(), Some(1));

        selector.handle_key(press(KeyCode::Char('k')));
        assert_eq!(selector.list_state.selected(), Some(0));
    }

    #[test]
    fn snapshot_time_range_selector() {
        let mut terminal = Terminal::new(TestBackend::new(60, 20)).unwrap();
        let selector = TimeRangeSelector::new();

        terminal
            .draw(|frame| selector.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("time_range_selector", content);
    }

    #[test]
    fn snapshot_time_range_selector_custom_mode() {
        let mut terminal = Terminal::new(TestBackend::new(60, 20)).unwrap();
        let mut selector = TimeRangeSelector::new();

        // Navigate to "Custom..." and enter custom mode
        while selector.list_state.selected() != Some(selector.presets.len() - 1) {
            selector.handle_key(press(KeyCode::Down));
        }
        selector.handle_key(press(KeyCode::Enter));

        terminal
            .draw(|frame| selector.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("time_range_selector_custom_mode", content);
    }
}
