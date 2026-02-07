//! Component trait and implementations

pub mod admin;
pub mod command_palette;
pub mod context_bar;
pub mod dashboard;
pub mod help;
pub mod logs;
pub mod metrics;
pub mod selector;
pub mod status_bar;
pub mod tabs;
pub mod time_range;
pub mod traces;

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
