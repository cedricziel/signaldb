//! Actions that drive TUI state transitions.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::state::TimeRange;

/// Actions that can be triggered by user input or internal events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Quit,
    SwitchTab(usize),
    NextTab,
    PrevTab,
    ToggleHelp,
    Refresh,
    ScrollUp,
    ScrollDown,
    Select,
    Back,
    #[allow(dead_code)]
    Search(String),
    Confirm,
    Cancel,
    OpenTenantSelector,
    OpenDatasetSelector,
    OpenCommandPalette,
    #[allow(dead_code)]
    OpenTimeRangeSelector,
    #[allow(dead_code)]
    CloseOverlay,
    #[allow(dead_code)]
    SetTenant(String),
    #[allow(dead_code)]
    SetDataset(String),
    #[allow(dead_code)]
    SetTimeRange(TimeRange),
    #[allow(dead_code)]
    ExecuteCommand(String),
    None,
}

/// Map a key event to an [`Action`].
pub fn map_key_to_action(key: KeyEvent) -> Action {
    match key.code {
        KeyCode::Char('q') => Action::Quit,
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => Action::Quit,
        KeyCode::Char('t') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            Action::OpenTenantSelector
        }
        KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            Action::OpenDatasetSelector
        }
        KeyCode::Char(':') => Action::OpenCommandPalette,
        KeyCode::Char('1') => Action::SwitchTab(0),
        KeyCode::Char('2') => Action::SwitchTab(1),
        KeyCode::Char('3') => Action::SwitchTab(2),
        KeyCode::Char('4') => Action::SwitchTab(3),
        KeyCode::Char('5') => Action::SwitchTab(4),
        KeyCode::Tab => Action::NextTab,
        KeyCode::BackTab => Action::PrevTab,
        KeyCode::Char('?') => Action::ToggleHelp,
        KeyCode::Char('r') => Action::Refresh,
        KeyCode::Up | KeyCode::Char('k') => Action::ScrollUp,
        KeyCode::Down | KeyCode::Char('j') => Action::ScrollDown,
        KeyCode::Enter => Action::Select,
        KeyCode::Esc => Action::Back,
        KeyCode::Char('y') => Action::Confirm,
        KeyCode::Char('n') => Action::Cancel,
        _ => Action::None,
    }
}

#[cfg(test)]
mod tests {
    use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers};

    use super::*;

    fn press(code: KeyCode) -> KeyEvent {
        KeyEvent {
            code,
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        }
    }

    fn press_with(code: KeyCode, modifiers: KeyModifiers) -> KeyEvent {
        KeyEvent {
            code,
            modifiers,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        }
    }

    #[test]
    fn action_variants_exist() {
        use crate::tui::state::TimeRange;
        let actions = vec![
            Action::Quit,
            Action::SwitchTab(0),
            Action::NextTab,
            Action::PrevTab,
            Action::ToggleHelp,
            Action::Refresh,
            Action::ScrollUp,
            Action::ScrollDown,
            Action::Select,
            Action::Back,
            Action::Search("test".to_string()),
            Action::Confirm,
            Action::Cancel,
            Action::OpenTenantSelector,
            Action::OpenDatasetSelector,
            Action::OpenCommandPalette,
            Action::OpenTimeRangeSelector,
            Action::CloseOverlay,
            Action::SetTenant("acme".to_string()),
            Action::SetDataset("prod".to_string()),
            Action::SetTimeRange(TimeRange::default()),
            Action::ExecuteCommand("refresh".to_string()),
            Action::None,
        ];
        assert_eq!(actions.len(), 23);
    }

    #[test]
    fn q_maps_to_quit() {
        assert_eq!(map_key_to_action(press(KeyCode::Char('q'))), Action::Quit);
    }

    #[test]
    fn ctrl_c_maps_to_quit() {
        assert_eq!(
            map_key_to_action(press_with(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            Action::Quit
        );
    }

    #[test]
    fn number_keys_map_to_switch_tab() {
        for (ch, idx) in [('1', 0), ('2', 1), ('3', 2), ('4', 3), ('5', 4)] {
            assert_eq!(
                map_key_to_action(press(KeyCode::Char(ch))),
                Action::SwitchTab(idx)
            );
        }
    }

    #[test]
    fn navigation_keys() {
        assert_eq!(map_key_to_action(press(KeyCode::Up)), Action::ScrollUp);
        assert_eq!(
            map_key_to_action(press(KeyCode::Char('k'))),
            Action::ScrollUp
        );
        assert_eq!(map_key_to_action(press(KeyCode::Down)), Action::ScrollDown);
        assert_eq!(
            map_key_to_action(press(KeyCode::Char('j'))),
            Action::ScrollDown
        );
        assert_eq!(map_key_to_action(press(KeyCode::Enter)), Action::Select);
        assert_eq!(map_key_to_action(press(KeyCode::Esc)), Action::Back);
        assert_eq!(
            map_key_to_action(press(KeyCode::Char('?'))),
            Action::ToggleHelp
        );
    }

    #[test]
    fn tab_key_cycles_forward() {
        assert_eq!(map_key_to_action(press(KeyCode::Tab)), Action::NextTab);
    }

    #[test]
    fn backtab_key_cycles_backward() {
        assert_eq!(
            map_key_to_action(press_with(KeyCode::BackTab, KeyModifiers::SHIFT)),
            Action::PrevTab
        );
    }

    #[test]
    fn unknown_key_maps_to_none() {
        assert_eq!(map_key_to_action(press(KeyCode::Char('z'))), Action::None);
    }

    #[test]
    fn ctrl_t_maps_to_open_tenant_selector() {
        assert_eq!(
            map_key_to_action(press_with(KeyCode::Char('t'), KeyModifiers::CONTROL)),
            Action::OpenTenantSelector
        );
    }

    #[test]
    fn ctrl_d_maps_to_open_dataset_selector() {
        assert_eq!(
            map_key_to_action(press_with(KeyCode::Char('d'), KeyModifiers::CONTROL)),
            Action::OpenDatasetSelector
        );
    }

    #[test]
    fn colon_maps_to_open_command_palette() {
        assert_eq!(
            map_key_to_action(press(KeyCode::Char(':'))),
            Action::OpenCommandPalette
        );
    }
}
