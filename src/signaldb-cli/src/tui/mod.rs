//! Terminal User Interface module for SignalDB CLI

pub mod action;
pub mod app;
pub mod client;
pub mod components;
pub mod event;
pub mod state;
pub mod terminal;
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
