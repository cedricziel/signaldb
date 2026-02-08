//! Terminal setup and management
//!
//! Provides a `Tui` wrapper around [`ratatui::Terminal`] with crossterm backend.
//! Handles entering/leaving alternate screen, raw mode, and panic-safe terminal restoration.

use std::io::{self, Stdout, stdout};

use crossterm::ExecutableCommand;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

/// TUI terminal wrapper that manages raw mode and alternate screen.
pub struct Tui {
    /// The underlying ratatui terminal.
    pub terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl Tui {
    /// Create a new `Tui` with a crossterm backend writing to stdout.
    pub fn new() -> anyhow::Result<Self> {
        let backend = CrosstermBackend::new(stdout());
        let terminal = Terminal::new(backend)?;
        Ok(Self { terminal })
    }

    /// Initialize the terminal: enable raw mode, enter alternate screen,
    /// hide cursor, clear screen, and install a panic hook that restores
    /// the terminal before printing the panic info.
    pub fn init(&mut self) -> anyhow::Result<()> {
        // Install panic hook *before* entering raw mode so it always restores.
        let original_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |panic_info| {
            // Best-effort terminal restoration on panic.
            let _ = disable_raw_mode();
            let _ = io::stdout().execute(LeaveAlternateScreen);
            let _ = io::stdout().execute(crossterm::cursor::Show);
            original_hook(panic_info);
        }));

        enable_raw_mode()?;
        io::stdout().execute(EnterAlternateScreen)?;
        self.terminal.hide_cursor()?;
        self.terminal.clear()?;
        Ok(())
    }

    /// Restore the terminal: leave alternate screen, disable raw mode,
    /// and show the cursor.
    pub fn exit(&mut self) -> anyhow::Result<()> {
        disable_raw_mode()?;
        io::stdout().execute(LeaveAlternateScreen)?;
        self.terminal.show_cursor()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    #[test]
    fn test_backend_can_be_created() {
        let terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
        let size = terminal.size().unwrap();
        assert_eq!(size.width, 80);
        assert_eq!(size.height, 24);
    }
}
