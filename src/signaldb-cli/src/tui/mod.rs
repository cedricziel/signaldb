//! Terminal User Interface module for SignalDB CLI

pub mod action;
pub mod app;
pub mod client;
#[allow(dead_code)] // Scaffolded for upcoming tab implementations
pub mod components;
pub mod event;
#[allow(dead_code)] // Scaffolded for upcoming state management tasks
pub mod state;
pub mod terminal;
#[allow(dead_code)] // Scaffolded for upcoming widget implementations
pub mod widgets;

#[cfg(test)]
pub mod test_helpers;

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    #[test]
    fn canary_test_backend_creates() {
        let terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
        assert_eq!(terminal.size().unwrap().width, 120);
        assert_eq!(terminal.size().unwrap().height, 40);
    }
}
