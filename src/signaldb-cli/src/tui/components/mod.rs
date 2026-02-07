//! Component trait and implementations

#[allow(dead_code)] // Wired into App render once Admin tab integration lands
pub mod admin;
#[allow(dead_code)] // Panels used once Dashboard tab is wired into App render
pub mod dashboard;
#[allow(dead_code)] // Wired into App render once Logs tab integration lands
pub mod logs;
#[allow(dead_code)] // Wired into App render once Metrics tab integration lands
pub mod metrics;
pub mod status_bar;
pub mod tabs;

use crossterm::event::KeyEvent;
use ratatui::Frame;
use ratatui::layout::Rect;

use super::action::Action;
use super::state::AppState;

/// Lifecycle trait for TUI components.
///
/// Each tab or UI panel implements this trait so the main [`App`] can
/// delegate key handling, state updates, and rendering uniformly.
#[allow(dead_code)] // Methods called once tab content components are implemented
pub trait Component {
    /// Translate a key press into an [`Action`], or `None` to let the
    /// parent handle it.
    fn handle_key_event(&mut self, key: KeyEvent) -> Option<Action>;

    /// React to an action that was dispatched globally.
    fn update(&mut self, action: &Action, state: &mut AppState);

    /// Draw the component into the given area.
    fn render(&self, frame: &mut Frame, area: Rect, state: &AppState);
}
