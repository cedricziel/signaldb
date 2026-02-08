//! Log detail viewer showing selected log attributes as JSON trees.

use ratatui::Frame;
use ratatui::layout::Rect;
use tui_tree_widget::TreeState;

use crate::tui::widgets::json_viewer::render_json_tree;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogAttributeTab {
    LogAttributes,
    ResourceAttributes,
    Scope,
}

/// Log detail panel with attribute tab selection.
pub struct LogDetail {
    pub attribute_tab: LogAttributeTab,
    pub json_state: TreeState<String>,
}

impl LogDetail {
    pub fn new() -> Self {
        Self {
            attribute_tab: LogAttributeTab::LogAttributes,
            json_state: TreeState::default(),
        }
    }

    pub fn cycle_attribute_tab(&mut self) {
        self.attribute_tab = match self.attribute_tab {
            LogAttributeTab::LogAttributes => LogAttributeTab::ResourceAttributes,
            LogAttributeTab::ResourceAttributes => LogAttributeTab::Scope,
            LogAttributeTab::Scope => LogAttributeTab::LogAttributes,
        };
        self.json_state = TreeState::default();
    }

    /// Render the detail view for the currently selected log row.
    pub fn render(&mut self, frame: &mut Frame, area: Rect, columns: &[String], values: &[String]) {
        let (attrs, tab_label) = match self.attribute_tab {
            LogAttributeTab::LogAttributes => (
                json_column(columns, values, "log_attributes"),
                "Log Attributes",
            ),
            LogAttributeTab::ResourceAttributes => (
                json_column(columns, values, "resource_attributes"),
                "Resource Attributes",
            ),
            LogAttributeTab::Scope => (scope_json(columns, values), "Scope"),
        };

        let active_indicator = match self.attribute_tab {
            LogAttributeTab::LogAttributes => "[Log ● | Resource ○ | Scope ○] ",
            LogAttributeTab::ResourceAttributes => "[Log ○ | Resource ● | Scope ○] ",
            LogAttributeTab::Scope => "[Log ○ | Resource ○ | Scope ●] ",
        };

        let title = format!("{active_indicator}{tab_label} (r to switch)");
        render_json_tree(frame, area, &attrs, &mut self.json_state, &title);
    }
}

fn scope_json(columns: &[String], values: &[String]) -> serde_json::Value {
    let scope_name = string_column(columns, values, "scope_name")
        .map(serde_json::Value::String)
        .unwrap_or(serde_json::Value::Null);
    let scope_attributes = json_column(columns, values, "scope_attributes");
    serde_json::json!({
        "scope_name": scope_name,
        "scope_attributes": scope_attributes,
    })
}

fn string_column(columns: &[String], values: &[String], name: &str) -> Option<String> {
    let idx = columns.iter().position(|c| c == name)?;
    let raw = values.get(idx)?;
    if raw == "NULL" {
        None
    } else {
        Some(raw.clone())
    }
}

fn json_column(columns: &[String], values: &[String], name: &str) -> serde_json::Value {
    let Some(idx) = columns.iter().position(|c| c == name) else {
        return serde_json::Value::Null;
    };
    let Some(raw) = values.get(idx) else {
        return serde_json::Value::Null;
    };

    if raw == "NULL" || raw.trim().is_empty() {
        return serde_json::Value::Null;
    }

    serde_json::from_str::<serde_json::Value>(raw)
        .unwrap_or_else(|_| serde_json::Value::String(raw.clone()))
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::tui::test_helpers::assert_buffer_contains;

    fn selected_row() -> (Vec<String>, Vec<String>) {
        (
            vec![
                "timestamp".to_string(),
                "service_name".to_string(),
                "log_attributes".to_string(),
                "resource_attributes".to_string(),
                "scope_name".to_string(),
                "scope_attributes".to_string(),
            ],
            vec![
                "1234567890".to_string(),
                "checkout".to_string(),
                "{\"http.status_code\":500,\"error\":true}".to_string(),
                "{\"service.name\":\"checkout\"}".to_string(),
                "io.opentelemetry.rust".to_string(),
                "{\"scope.version\":\"1.0.0\"}".to_string(),
            ],
        )
    }

    #[test]
    fn cycles_attribute_tabs() {
        let mut detail = LogDetail::new();
        assert_eq!(detail.attribute_tab, LogAttributeTab::LogAttributes);
        detail.cycle_attribute_tab();
        assert_eq!(detail.attribute_tab, LogAttributeTab::ResourceAttributes);
        detail.cycle_attribute_tab();
        assert_eq!(detail.attribute_tab, LogAttributeTab::Scope);
        detail.cycle_attribute_tab();
        assert_eq!(detail.attribute_tab, LogAttributeTab::LogAttributes);
    }

    #[test]
    fn renders_placeholder_when_no_selection() {
        let mut terminal = Terminal::new(TestBackend::new(60, 8)).unwrap();
        let mut detail = LogDetail::new();
        terminal
            .draw(|frame| detail.render(frame, frame.area(), &[], &[]))
            .unwrap();
        assert_buffer_contains(&terminal, "No attributes");
        assert_buffer_contains(&terminal, "Log");
    }

    #[test]
    fn renders_log_attributes_tab() {
        let mut terminal = Terminal::new(TestBackend::new(80, 8)).unwrap();
        let mut detail = LogDetail::new();
        let (columns, values) = selected_row();
        terminal
            .draw(|frame| detail.render(frame, frame.area(), &columns, &values))
            .unwrap();
        assert_buffer_contains(&terminal, "http.status_code");
        assert_buffer_contains(&terminal, "500");
    }

    #[test]
    fn renders_resource_attributes_tab() {
        let mut terminal = Terminal::new(TestBackend::new(80, 8)).unwrap();
        let mut detail = LogDetail::new();
        let (columns, values) = selected_row();
        detail.cycle_attribute_tab();
        terminal
            .draw(|frame| detail.render(frame, frame.area(), &columns, &values))
            .unwrap();
        assert_buffer_contains(&terminal, "service.name");
        assert_buffer_contains(&terminal, "checkout");
    }

    #[test]
    fn renders_scope_tab() {
        let mut terminal = Terminal::new(TestBackend::new(80, 8)).unwrap();
        let mut detail = LogDetail::new();
        let (columns, values) = selected_row();
        detail.cycle_attribute_tab();
        detail.cycle_attribute_tab();
        terminal
            .draw(|frame| detail.render(frame, frame.area(), &columns, &values))
            .unwrap();
        assert_buffer_contains(&terminal, "scope_name");
        assert_buffer_contains(&terminal, "io.opentelemetry.rust");
        assert_buffer_contains(&terminal, "scope_attributes");
    }

    #[test]
    fn snapshot_detail_placeholder() {
        let mut terminal = Terminal::new(TestBackend::new(60, 6)).unwrap();
        let mut detail = LogDetail::new();
        terminal
            .draw(|frame| detail.render(frame, frame.area(), &[], &[]))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("log_detail_placeholder", content);
    }

    #[test]
    fn snapshot_detail_with_data() {
        let mut terminal = Terminal::new(TestBackend::new(80, 8)).unwrap();
        let mut detail = LogDetail::new();
        let (columns, values) = selected_row();
        terminal
            .draw(|frame| detail.render(frame, frame.area(), &columns, &values))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("log_detail_with_data", content);
    }
}
