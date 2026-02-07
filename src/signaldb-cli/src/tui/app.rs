//! Main TUI application: owns the event loop, terminal, and render cycle.

use std::time::Duration;

use ratatui::Frame;
use ratatui::layout::Alignment;
use ratatui::style::{Color, Style};
use ratatui::widgets::{Block, Borders, Paragraph};

use super::action::{Action, map_key_to_action};
use super::event::{Event, EventHandler};
use super::terminal::Tui;

/// Top-level TUI application.
#[allow(dead_code)] // Fields used in future tab/data-fetching tasks
pub struct App {
    pub url: String,
    pub flight_url: String,
    pub api_key: Option<String>,
    pub admin_key: Option<String>,
    pub refresh_rate: Duration,
    pub tenant_id: Option<String>,
    pub dataset_id: Option<String>,
    running: bool,
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
        Self {
            url,
            flight_url,
            api_key,
            admin_key,
            refresh_rate,
            tenant_id,
            dataset_id,
            running: true,
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
                    let action = map_key_to_action(key);
                    self.handle_action(action);
                }
                Event::Tick => {}
                Event::Render => {
                    tui.terminal.draw(|frame| self.render(frame))?;
                }
            }
        }

        tui.exit()?;
        Ok(())
    }

    fn handle_action(&mut self, action: Action) {
        match action {
            Action::Quit => self.running = false,
            Action::SwitchTab(_)
            | Action::Refresh
            | Action::ScrollUp
            | Action::ScrollDown
            | Action::Select
            | Action::Back
            | Action::Search(_)
            | Action::Confirm
            | Action::Cancel
            | Action::None => {}
        }
    }

    fn render(&self, frame: &mut Frame) {
        let area = frame.area();
        let text = Paragraph::new("SignalDB TUI — Press q to quit")
            .alignment(Alignment::Center)
            .style(Style::default().fg(Color::Cyan))
            .block(Block::default().borders(Borders::ALL).title("SignalDB"));
        frame.render_widget(text, area);
    }
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::tui::test_helpers::assert_buffer_contains;

    #[test]
    fn app_new_sets_running() {
        let app = App::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            None,
            None,
            Duration::from_secs(5),
            None,
            None,
        );
        assert!(app.running);
    }

    #[test]
    fn quit_action_stops_app() {
        let mut app = App::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            None,
            None,
            Duration::from_secs(5),
            None,
            None,
        );
        app.handle_action(Action::Quit);
        assert!(!app.running);
    }

    #[test]
    fn render_shows_placeholder_text() {
        let mut terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
        let app = App::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            None,
            None,
            Duration::from_secs(5),
            None,
            None,
        );
        terminal.draw(|frame| app.render(frame)).unwrap();
        assert_buffer_contains(&terminal, "SignalDB TUI");
    }
}
