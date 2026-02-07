//! Vim-style command palette with prefix-matched commands and tab completion.

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph};

use crate::tui::widgets::text_input::{TextInput, TextInputAction};

/// Commands that can be executed from the palette.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum PaletteCommand {
    SetTenant(String),
    SetDataset(String),
    Refresh,
    Quit,
    SetTimeRange(String),
    SetGroupBy(String),
}

/// Actions produced by the command palette.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum PaletteAction {
    Execute(PaletteCommand),
    Cancelled,
    None,
}

/// Vim-style command palette with prefix matching and tab completion.
#[allow(dead_code)]
pub struct CommandPalette {
    input: TextInput,
    completions: Vec<String>,
    selected_completion: usize,
    available_tenants: Vec<String>,
    available_datasets: Vec<String>,
}

impl CommandPalette {
    /// Create a new command palette.
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            input: TextInput::new(),
            completions: Vec::new(),
            selected_completion: 0,
            available_tenants: Vec::new(),
            available_datasets: Vec::new(),
        }
    }

    /// Clear input and completions.
    #[allow(dead_code)]
    pub fn reset(&mut self) {
        self.input.clear();
        self.completions.clear();
        self.selected_completion = 0;
    }

    /// Set available tenants for completion.
    #[allow(dead_code)]
    pub fn set_available_tenants(&mut self, tenants: Vec<String>) {
        self.available_tenants = tenants;
    }

    /// Set available datasets for completion.
    #[allow(dead_code)]
    pub fn set_available_datasets(&mut self, datasets: Vec<String>) {
        self.available_datasets = datasets;
    }

    /// Handle key events and return action.
    #[allow(dead_code)]
    pub fn handle_key(&mut self, key: KeyEvent) -> PaletteAction {
        match key.code {
            KeyCode::Enter => {
                let text = self.input.text().trim();
                if let Some(cmd) = Self::parse_command(text) {
                    PaletteAction::Execute(cmd)
                } else {
                    PaletteAction::None
                }
            }
            KeyCode::Esc => PaletteAction::Cancelled,
            KeyCode::Tab => {
                if !self.completions.is_empty() {
                    // Cycle through completions
                    self.selected_completion =
                        (self.selected_completion + 1) % self.completions.len();
                    // Fill input with selected completion
                    self.input
                        .set_text(&self.completions[self.selected_completion]);
                    self.compute_and_update_completions();
                }
                PaletteAction::None
            }
            KeyCode::Up => {
                if !self.completions.is_empty() {
                    self.selected_completion = if self.selected_completion == 0 {
                        self.completions.len() - 1
                    } else {
                        self.selected_completion - 1
                    };
                }
                PaletteAction::None
            }
            KeyCode::Down => {
                if !self.completions.is_empty() {
                    self.selected_completion =
                        (self.selected_completion + 1) % self.completions.len();
                }
                PaletteAction::None
            }
            _ => {
                // Delegate to TextInput
                match self.input.handle_key(key) {
                    TextInputAction::Submit => {
                        let text = self.input.text().trim();
                        if let Some(cmd) = Self::parse_command(text) {
                            PaletteAction::Execute(cmd)
                        } else {
                            PaletteAction::None
                        }
                    }
                    TextInputAction::Cancel => PaletteAction::Cancelled,
                    TextInputAction::Changed => {
                        self.compute_and_update_completions();
                        PaletteAction::None
                    }
                    TextInputAction::Unhandled => PaletteAction::None,
                }
            }
        }
    }

    /// Render the command palette at the bottom of the area.
    #[allow(dead_code)]
    pub fn render(&self, frame: &mut Frame, area: Rect) {
        // Render completions popup above input if they exist
        if !self.completions.is_empty() {
            let completion_height = (self.completions.len() as u16).min(10) + 2; // +2 for borders
            let popup_area = Rect {
                x: area.x,
                y: area.y + area.height.saturating_sub(completion_height + 1), // +1 for input line
                width: area.width.min(60),
                height: completion_height,
            };

            frame.render_widget(Clear, popup_area);

            let items: Vec<ListItem> = self
                .completions
                .iter()
                .enumerate()
                .map(|(i, completion)| {
                    let style = if i == self.selected_completion {
                        Style::default()
                            .fg(Color::Yellow)
                            .add_modifier(Modifier::BOLD)
                    } else {
                        Style::default().fg(Color::White)
                    };
                    ListItem::new(Line::from(Span::styled(completion.clone(), style)))
                })
                .collect();

            let list = List::new(items).block(
                Block::default()
                    .title(" Completions ")
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(Color::Cyan)),
            );

            frame.render_widget(list, popup_area);
        }

        // Render input at bottom
        let input_area = Rect {
            x: area.x,
            y: area.y + area.height.saturating_sub(1),
            width: area.width,
            height: 1,
        };

        let text = self.input.text();
        let display_text = format!(":{}", text);
        let paragraph = Paragraph::new(display_text).style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        );

        frame.render_widget(paragraph, input_area);
    }

    /// Parse command text into a PaletteCommand.
    fn parse_command(input: &str) -> Option<PaletteCommand> {
        let input = input.trim();
        if input.is_empty() {
            return None;
        }

        let parts: Vec<&str> = input.split_whitespace().collect();
        if parts.is_empty() {
            return None;
        }

        match parts[0] {
            "tenant" | "t" => {
                if parts.len() > 1 {
                    Some(PaletteCommand::SetTenant(parts[1..].join(" ")))
                } else {
                    None
                }
            }
            "dataset" | "d" => {
                if parts.len() > 1 {
                    Some(PaletteCommand::SetDataset(parts[1..].join(" ")))
                } else {
                    None
                }
            }
            "refresh" => Some(PaletteCommand::Refresh),
            "quit" | "q" => Some(PaletteCommand::Quit),
            "time" => {
                if parts.len() > 1 {
                    Some(PaletteCommand::SetTimeRange(parts[1..].join(" ")))
                } else {
                    None
                }
            }
            "group" => {
                if parts.len() > 1 {
                    Some(PaletteCommand::SetGroupBy(parts[1..].join(" ")))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Compute completions based on current input.
    fn compute_completions(&self, input: &str) -> Vec<String> {
        let input = input.trim();
        if input.is_empty() {
            return vec![
                "tenant".to_string(),
                "dataset".to_string(),
                "refresh".to_string(),
                "quit".to_string(),
                "time".to_string(),
            ];
        }

        let parts: Vec<&str> = input.split_whitespace().collect();

        // If we have a space, we're completing arguments
        if input.ends_with(' ') || parts.len() > 1 {
            let cmd = parts[0];
            let arg_prefix = if input.ends_with(' ') {
                ""
            } else {
                parts.last().copied().unwrap_or("")
            };

            match cmd {
                "tenant" | "t" => {
                    return self
                        .available_tenants
                        .iter()
                        .filter(|t| t.starts_with(arg_prefix))
                        .map(|t| format!("{} {}", cmd, t))
                        .collect();
                }
                "dataset" | "d" => {
                    return self
                        .available_datasets
                        .iter()
                        .filter(|d| d.starts_with(arg_prefix))
                        .map(|d| format!("{} {}", cmd, d))
                        .collect();
                }
                "group" => {
                    return [
                        "none",
                        "service",
                        "operation",
                        "kind",
                        "span_kind",
                        "severity",
                        "scope",
                        "scope_name",
                    ]
                    .into_iter()
                    .filter(|value| value.starts_with(arg_prefix))
                    .map(|value| format!("group {value}"))
                    .collect();
                }
                _ => return Vec::new(),
            }
        }

        // Complete command names
        let base_commands = vec!["tenant", "dataset", "refresh", "quit", "time", "group"];
        base_commands
            .into_iter()
            .filter(|cmd| cmd.starts_with(input))
            .map(String::from)
            .collect()
    }

    /// Compute and update completions based on current input.
    fn compute_and_update_completions(&mut self) {
        self.completions = self.compute_completions(self.input.text());
        self.selected_completion = 0;
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

    #[test]
    fn test_palette_parse_tenant_command() {
        let cmd = CommandPalette::parse_command("tenant acme");
        assert_eq!(cmd, Some(PaletteCommand::SetTenant("acme".to_string())));
    }

    #[test]
    fn test_palette_parse_tenant_short() {
        let cmd = CommandPalette::parse_command("t acme");
        assert_eq!(cmd, Some(PaletteCommand::SetTenant("acme".to_string())));
    }

    #[test]
    fn test_palette_parse_dataset_command() {
        let cmd = CommandPalette::parse_command("dataset prod");
        assert_eq!(cmd, Some(PaletteCommand::SetDataset("prod".to_string())));
    }

    #[test]
    fn test_palette_parse_refresh() {
        let cmd = CommandPalette::parse_command("refresh");
        assert_eq!(cmd, Some(PaletteCommand::Refresh));
    }

    #[test]
    fn test_palette_parse_quit() {
        let cmd = CommandPalette::parse_command("quit");
        assert_eq!(cmd, Some(PaletteCommand::Quit));

        let cmd = CommandPalette::parse_command("q");
        assert_eq!(cmd, Some(PaletteCommand::Quit));
    }

    #[test]
    fn test_palette_parse_time() {
        let cmd = CommandPalette::parse_command("time 15m");
        assert_eq!(cmd, Some(PaletteCommand::SetTimeRange("15m".to_string())));
    }

    #[test]
    fn test_palette_parse_empty_returns_none() {
        let cmd = CommandPalette::parse_command("");
        assert_eq!(cmd, None);
    }

    #[test]
    fn test_palette_parse_group_command() {
        let cmd = CommandPalette::parse_command("group service");
        assert_eq!(cmd, Some(PaletteCommand::SetGroupBy("service".to_string())));

        let cmd = CommandPalette::parse_command("group kind");
        assert_eq!(cmd, Some(PaletteCommand::SetGroupBy("kind".to_string())));
    }

    #[test]
    fn test_palette_esc_cancels() {
        let mut palette = CommandPalette::new();
        let action = palette.handle_key(press(KeyCode::Esc));
        assert_eq!(action, PaletteAction::Cancelled);
    }

    #[test]
    fn test_palette_enter_executes() {
        let mut palette = CommandPalette::new();
        palette.input.set_text("refresh");
        let action = palette.handle_key(press(KeyCode::Enter));
        assert_eq!(action, PaletteAction::Execute(PaletteCommand::Refresh));
    }

    #[test]
    fn test_palette_completions_for_prefix() {
        let palette = CommandPalette::new();
        let completions = palette.compute_completions("g");
        assert!(completions.contains(&"group".to_string()));

        let completions = palette.compute_completions("t");
        assert!(completions.contains(&"tenant".to_string()));
        assert!(completions.contains(&"time".to_string()));
        assert!(!completions.contains(&"refresh".to_string()));
    }

    #[test]
    fn test_palette_group_argument_completion() {
        let palette = CommandPalette::new();
        let completions = palette.compute_completions("group s");
        assert!(completions.contains(&"group service".to_string()));
        assert!(completions.contains(&"group span_kind".to_string()));
        assert!(completions.contains(&"group severity".to_string()));
        assert!(completions.contains(&"group scope".to_string()));
        assert!(completions.contains(&"group scope_name".to_string()));
        assert!(!completions.contains(&"group operation".to_string()));
    }

    #[test]
    fn test_palette_tab_fills_completion() {
        let mut palette = CommandPalette::new();
        palette.input.set_text("t");
        palette.compute_and_update_completions();

        // Tab should fill with first completion
        let action = palette.handle_key(press(KeyCode::Tab));
        assert_eq!(action, PaletteAction::None);

        // Input should now contain the first completion
        let text = palette.input.text();
        assert!(text == "tenant" || text == "time");
    }

    #[test]
    fn snapshot_command_palette_empty() {
        let mut terminal = Terminal::new(TestBackend::new(80, 10)).unwrap();
        let palette = CommandPalette::new();
        terminal
            .draw(|frame| palette.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("command_palette_empty", content);
    }

    #[test]
    fn snapshot_command_palette_with_input() {
        let mut terminal = Terminal::new(TestBackend::new(80, 10)).unwrap();
        let mut palette = CommandPalette::new();
        palette.input.set_text("tenant acme");
        terminal
            .draw(|frame| palette.render(frame, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("command_palette_with_input", content);
    }
}
