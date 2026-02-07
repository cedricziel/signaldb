//! Main TUI application: owns the event loop, terminal, and render cycle.

use std::time::Duration;

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Style};
use ratatui::widgets::Paragraph;

use super::action::{Action, map_key_to_action};
use super::components::Component;
use super::components::status_bar::StatusBar;
use super::components::tabs::TabBar;
use super::event::{Event, EventHandler};
use super::state::AppState;
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
    pub state: AppState,
    tab_bar: TabBar,
    status_bar: StatusBar,
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
        let state = AppState::new(url.clone(), flight_url.clone(), refresh_rate);
        Self {
            url,
            flight_url,
            api_key,
            admin_key,
            refresh_rate,
            tenant_id,
            dataset_id,
            running: true,
            state,
            tab_bar: TabBar::new(),
            status_bar: StatusBar::new(),
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
            Action::SwitchTab(idx) => self.state.switch_tab(idx),
            Action::NextTab => self.state.next_tab(),
            Action::PrevTab => self.state.prev_tab(),
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

        let placeholder = Paragraph::new("Tab content coming soon")
            .style(Style::default().fg(Color::DarkGray))
            .centered();
        frame.render_widget(placeholder, chunks[1]);

        self.status_bar.render(frame, chunks[2], &self.state);
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
        assert_buffer_contains(&terminal, "Tab content coming soon");
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
