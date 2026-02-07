//! JSON tree viewer widget wrapping `tui_tree_widget::Tree`.
//!
//! Converts `serde_json::Value` into a collapsible tree structure
//! for displaying span attributes and other JSON data.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Paragraph};
use tui_tree_widget::{Tree, TreeItem, TreeState};

/// Render a JSON value as a collapsible tree.
pub fn render_json_tree(
    frame: &mut Frame,
    area: Rect,
    value: &serde_json::Value,
    state: &mut TreeState<String>,
    title: &str,
) {
    let block = Block::default()
        .title(format!(" {title} "))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));

    if value.is_null() || (value.is_object() && value.as_object().is_some_and(|o| o.is_empty())) {
        let empty = Paragraph::new("No attributes")
            .style(Style::default().fg(Color::DarkGray))
            .block(block);
        frame.render_widget(empty, area);
        return;
    }

    let items = json_to_tree_items(value);

    match Tree::new(&items) {
        Ok(tree) => {
            let tree = tree
                .block(block)
                .style(Style::default().fg(Color::White))
                .highlight_style(
                    Style::default()
                        .fg(Color::White)
                        .bg(Color::DarkGray)
                        .add_modifier(Modifier::BOLD),
                )
                .node_closed_symbol("\u{25B6} ") // ▶
                .node_open_symbol("\u{25BC} ") // ▼
                .node_no_children_symbol("  ");

            frame.render_stateful_widget(tree, area, state);
        }
        Err(_) => {
            let fallback = Paragraph::new(format!("{}", value))
                .style(Style::default().fg(Color::White))
                .block(block);
            frame.render_widget(fallback, area);
        }
    }
}

/// Convert a `serde_json::Value` to a vec of `TreeItem`s.
pub fn json_to_tree_items(value: &serde_json::Value) -> Vec<TreeItem<'static, String>> {
    match value {
        serde_json::Value::Object(map) => map
            .iter()
            .enumerate()
            .filter_map(|(idx, (key, val))| json_kv_to_item(key, val, idx))
            .collect(),
        serde_json::Value::Array(arr) => arr
            .iter()
            .enumerate()
            .filter_map(|(idx, val)| {
                let label = format!("[{idx}]: {}", value_preview(val));
                match val {
                    serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
                        let children = json_to_tree_items(val);
                        TreeItem::new(format!("arr_{idx}"), label, children).ok()
                    }
                    _ => Some(TreeItem::new_leaf(format!("arr_{idx}"), label)),
                }
            })
            .collect(),
        _ => vec![TreeItem::new_leaf("root".to_string(), format!("{value}"))],
    }
}

fn json_kv_to_item(
    key: &str,
    val: &serde_json::Value,
    idx: usize,
) -> Option<TreeItem<'static, String>> {
    let id = format!("{}_{idx}", key.replace('.', "_"));
    match val {
        serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
            let label = format!("{key}: {{{}}}", child_count(val));
            let children = json_to_tree_items(val);
            TreeItem::new(id, label, children).ok()
        }
        _ => {
            let label = format!("{key}: {}", format_value(val));
            Some(TreeItem::new_leaf(id, label))
        }
    }
}

fn format_value(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::String(s) => format!("\"{s}\""),
        serde_json::Value::Null => "null".into(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        _ => format!("{val}"),
    }
}

fn value_preview(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Object(m) => format!("{{{} keys}}", m.len()),
        serde_json::Value::Array(a) => format!("[{} items]", a.len()),
        _ => format_value(val),
    }
}

fn child_count(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Object(m) => format!("{} keys", m.len()),
        serde_json::Value::Array(a) => format!("{} items", a.len()),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::tui::test_helpers::assert_buffer_contains;

    #[test]
    fn json_to_tree_items_empty_object() {
        let val = serde_json::json!({});
        let items = json_to_tree_items(&val);
        assert!(items.is_empty());
    }

    #[test]
    fn json_to_tree_items_flat_object() {
        let val = serde_json::json!({"name": "test", "count": 42});
        let items = json_to_tree_items(&val);
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn json_to_tree_items_nested_object() {
        let val = serde_json::json!({
            "http": {
                "method": "GET",
                "status_code": 200
            },
            "db": {
                "type": "postgres"
            }
        });
        let items = json_to_tree_items(&val);
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn json_to_tree_items_array() {
        let val = serde_json::json!([1, 2, 3]);
        let items = json_to_tree_items(&val);
        assert_eq!(items.len(), 3);
    }

    #[test]
    fn json_to_tree_items_primitive() {
        let val = serde_json::json!("hello");
        let items = json_to_tree_items(&val);
        assert_eq!(items.len(), 1);
    }

    #[test]
    fn format_value_string() {
        assert_eq!(format_value(&serde_json::json!("hello")), "\"hello\"");
    }

    #[test]
    fn format_value_number() {
        assert_eq!(format_value(&serde_json::json!(42)), "42");
    }

    #[test]
    fn format_value_bool() {
        assert_eq!(format_value(&serde_json::json!(true)), "true");
    }

    #[test]
    fn format_value_null() {
        assert_eq!(format_value(&serde_json::Value::Null), "null");
    }

    #[test]
    fn render_json_tree_empty() {
        let mut terminal = Terminal::new(TestBackend::new(60, 8)).unwrap();
        let val = serde_json::json!({});
        let mut state = TreeState::default();
        terminal
            .draw(|frame| render_json_tree(frame, frame.area(), &val, &mut state, "Attributes"))
            .unwrap();
        assert_buffer_contains(&terminal, "No attributes");
    }

    #[test]
    fn render_json_tree_with_data() {
        let mut terminal = Terminal::new(TestBackend::new(80, 10)).unwrap();
        let val = serde_json::json!({
            "http.method": "GET",
            "http.status_code": 200,
            "db.system": "postgresql"
        });
        let mut state = TreeState::default();
        terminal
            .draw(|frame| render_json_tree(frame, frame.area(), &val, &mut state, "Attributes"))
            .unwrap();
        assert_buffer_contains(&terminal, "http.method");
        assert_buffer_contains(&terminal, "GET");
    }

    #[test]
    fn render_json_tree_null() {
        let mut terminal = Terminal::new(TestBackend::new(60, 8)).unwrap();
        let val = serde_json::Value::Null;
        let mut state = TreeState::default();
        terminal
            .draw(|frame| render_json_tree(frame, frame.area(), &val, &mut state, "Attributes"))
            .unwrap();
        assert_buffer_contains(&terminal, "No attributes");
    }

    #[test]
    fn snapshot_json_viewer_with_data() {
        let mut terminal = Terminal::new(TestBackend::new(80, 12)).unwrap();
        let val = serde_json::json!({
            "http.method": "POST",
            "http.status_code": 201,
            "http.url": "https://api.example.com/users",
            "net.peer.name": "api.example.com"
        });
        let mut state = TreeState::default();
        terminal
            .draw(|frame| {
                render_json_tree(frame, frame.area(), &val, &mut state, "Span Attributes")
            })
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("json_viewer_with_data", content);
    }

    #[test]
    fn snapshot_json_viewer_empty() {
        let mut terminal = Terminal::new(TestBackend::new(60, 6)).unwrap();
        let val = serde_json::json!({});
        let mut state = TreeState::default();
        terminal
            .draw(|frame| render_json_tree(frame, frame.area(), &val, &mut state, "Attributes"))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("json_viewer_empty", content);
    }

    #[test]
    fn snapshot_json_viewer_nested() {
        let mut terminal = Terminal::new(TestBackend::new(80, 15)).unwrap();
        let val = serde_json::json!({
            "http": {
                "method": "GET",
                "status_code": 200
            },
            "service": "frontend",
            "tags": ["important", "production"]
        });
        let mut state = TreeState::default();
        terminal
            .draw(|frame| render_json_tree(frame, frame.area(), &val, &mut state, "Nested JSON"))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("json_viewer_nested", content);
    }
}
