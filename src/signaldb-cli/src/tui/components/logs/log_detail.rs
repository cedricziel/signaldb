//! Log detail viewer showing the full record of a selected log row.
//!
//! Renders log records as a collapsible JSON tree using the json_viewer widget.

use ratatui::Frame;
use ratatui::layout::Rect;
use tui_tree_widget::TreeState;

use crate::tui::widgets::json_viewer::render_json_tree;

/// Log detail panel showing the selected log row as a JSON tree.
pub struct LogDetail;

impl LogDetail {
    pub fn new() -> Self {
        Self
    }

    /// Render the detail view for the currently selected log row.
    ///
    /// `selected` is a tuple of (column_names, cell_values). If `None`,
    /// a placeholder is shown. The columns and values are converted to a
    /// JSON object and rendered as a collapsible tree.
    pub fn render(&self, frame: &mut Frame, area: Rect, selected: Option<(&[String], &[String])>) {
        let mut tree_state = TreeState::default();
        match selected {
            None => {
                // render_json_tree handles the placeholder case for null/empty objects
                render_json_tree(
                    frame,
                    area,
                    &serde_json::Value::Null,
                    &mut tree_state,
                    "Log Detail",
                );
            }
            Some((columns, values)) => {
                // Convert column/value pairs to a JSON object
                let mut obj = serde_json::Map::new();
                for (col, val) in columns.iter().zip(values.iter()) {
                    obj.insert(col.clone(), serde_json::Value::String(val.clone()));
                }
                let json_value = serde_json::Value::Object(obj);
                render_json_tree(frame, area, &json_value, &mut tree_state, "Log Detail");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::tui::test_helpers::assert_buffer_contains;

    #[test]
    fn renders_placeholder_when_no_selection() {
        let mut terminal = Terminal::new(TestBackend::new(60, 8)).unwrap();
        let detail = LogDetail::new();
        terminal
            .draw(|frame| detail.render(frame, frame.area(), None))
            .unwrap();
        assert_buffer_contains(&terminal, "No attributes");
        assert_buffer_contains(&terminal, "Log Detail");
    }

    #[test]
    fn renders_selected_row() {
        let mut terminal = Terminal::new(TestBackend::new(80, 8)).unwrap();
        let detail = LogDetail::new();
        let columns = vec![
            "timestamp".to_string(),
            "severity_text".to_string(),
            "body".to_string(),
        ];
        let values = vec![
            "1234567890".to_string(),
            "ERROR".to_string(),
            "disk is full".to_string(),
        ];
        terminal
            .draw(|frame| {
                detail.render(
                    frame,
                    frame.area(),
                    Some((columns.as_slice(), values.as_slice())),
                )
            })
            .unwrap();
        assert_buffer_contains(&terminal, "timestamp:");
        assert_buffer_contains(&terminal, "1234567890");
        assert_buffer_contains(&terminal, "severity_text:");
        assert_buffer_contains(&terminal, "ERROR");
        assert_buffer_contains(&terminal, "body:");
        assert_buffer_contains(&terminal, "disk is full");
    }

    #[test]
    fn snapshot_detail_placeholder() {
        let mut terminal = Terminal::new(TestBackend::new(60, 6)).unwrap();
        let detail = LogDetail::new();
        terminal
            .draw(|frame| detail.render(frame, frame.area(), None))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("log_detail_placeholder", content);
    }

    #[test]
    fn snapshot_detail_with_data() {
        let mut terminal = Terminal::new(TestBackend::new(80, 8)).unwrap();
        let detail = LogDetail::new();
        let columns = vec![
            "timestamp".to_string(),
            "severity_text".to_string(),
            "body".to_string(),
        ];
        let values = vec![
            "2025-01-15 10:30:00".to_string(),
            "WARN".to_string(),
            "connection pool nearly exhausted".to_string(),
        ];
        terminal
            .draw(|frame| {
                detail.render(
                    frame,
                    frame.area(),
                    Some((columns.as_slice(), values.as_slice())),
                )
            })
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("log_detail_with_data", content);
    }
}
