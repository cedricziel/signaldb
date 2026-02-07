//! Log detail viewer showing the full record of a selected log row.
//!
//! Renders column name/value pairs for the currently selected log entry.
//! Will integrate with the JSON viewer widget when available.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

/// Log detail panel showing the selected log row as key-value pairs.
pub struct LogDetail;

impl LogDetail {
    pub fn new() -> Self {
        Self
    }

    /// Render the detail view for the currently selected log row.
    ///
    /// `selected` is a tuple of (column_names, cell_values). If `None`,
    /// a placeholder is shown.
    pub fn render(&self, frame: &mut Frame, area: Rect, selected: Option<(&[String], &[String])>) {
        let block = Block::default()
            .title(" Log Detail ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray));

        match selected {
            None => {
                let placeholder = Paragraph::new("Select a log entry to view details")
                    .style(Style::default().fg(Color::DarkGray))
                    .block(block);
                frame.render_widget(placeholder, area);
            }
            Some((columns, values)) => {
                let lines: Vec<Line> = columns
                    .iter()
                    .zip(values.iter())
                    .map(|(col, val)| {
                        Line::from(vec![
                            Span::styled(
                                format!("{col}: "),
                                Style::default()
                                    .fg(Color::Cyan)
                                    .add_modifier(Modifier::BOLD),
                            ),
                            Span::styled(val.clone(), Style::default().fg(Color::White)),
                        ])
                    })
                    .collect();

                let paragraph = Paragraph::new(lines)
                    .block(block)
                    .wrap(Wrap { trim: false });
                frame.render_widget(paragraph, area);
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
        assert_buffer_contains(&terminal, "Select a log entry to view details");
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
