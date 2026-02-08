//! Service health panel for the dashboard.
//!
//! Queries `_signaldb_metrics` for `signaldb.service.healthy` metrics and
//! renders a table showing each service's health status.

use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use crossterm::event::KeyEvent;
use ratatui::Frame;
use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Paragraph, Row, Table};

use crate::tui::action::Action;
use crate::tui::components::Component;
use crate::tui::state::AppState;

/// Data state for the service health panel.
#[derive(Debug, Clone)]
enum PanelData {
    Loading,
    Loaded(Vec<ServiceRow>),
    Error(String),
}

/// A single row in the service health table.
#[derive(Debug, Clone)]
struct ServiceRow {
    service_name: String,
    healthy: bool,
}

/// Service health panel component.
pub struct ServiceHealthPanel {
    data: PanelData,
}

impl ServiceHealthPanel {
    pub fn new() -> Self {
        Self {
            data: PanelData::Loading,
        }
    }

    /// Update panel data from record batches returned by Flight SQL.
    pub fn set_data(&mut self, batches: &[RecordBatch]) {
        let mut rows = Vec::new();
        for batch in batches {
            let services = batch
                .column_by_name("service_name")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let values = batch.column_by_name("metric_value");

            let services = match services {
                Some(s) => s,
                None => continue,
            };

            for i in 0..batch.num_rows() {
                let service_name = services.value(i).to_string();
                let value = values.and_then(|v| extract_f64(v, i)).unwrap_or(0.0);
                rows.push(ServiceRow {
                    service_name,
                    healthy: value > 0.0,
                });
            }
        }
        self.data = PanelData::Loaded(rows);
    }

    /// Set an error message on the panel.
    pub fn set_error(&mut self, msg: String) {
        self.data = PanelData::Error(msg);
    }

    /// SQL query for service health metrics.
    pub fn query() -> &'static str {
        "SELECT service_name, metric_value FROM _signaldb_metrics \
         WHERE metric_name = 'signaldb.service.healthy' \
         ORDER BY timestamp DESC LIMIT 10"
    }
}

impl Component for ServiceHealthPanel {
    fn handle_key_event(&mut self, _key: KeyEvent) -> Option<Action> {
        None
    }

    fn update(&mut self, _action: &Action, _state: &mut AppState) {}

    fn render(&self, frame: &mut Frame, area: Rect, state: &AppState) {
        let block = Block::default()
            .title(" Service Health ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan));

        match &self.data {
            PanelData::Loading => {
                let spinner = state.spinner_char();
                let loading = Paragraph::new(format!("{spinner} Loading..."))
                    .style(Style::default().fg(Color::DarkGray))
                    .block(block);
                frame.render_widget(loading, area);
            }
            PanelData::Error(msg) => {
                let error = Paragraph::new(format!("Failed to load: {msg}\n(r to retry)"))
                    .style(Style::default().fg(Color::Red))
                    .block(block);
                frame.render_widget(error, area);
            }
            PanelData::Loaded(rows) => {
                if rows.is_empty() {
                    let empty = Paragraph::new("No services found")
                        .style(Style::default().fg(Color::DarkGray))
                        .block(block);
                    frame.render_widget(empty, area);
                    return;
                }

                let header = Row::new(vec!["Service", "Status"])
                    .style(Style::default().add_modifier(Modifier::BOLD))
                    .bottom_margin(1);

                let table_rows: Vec<Row> = rows
                    .iter()
                    .map(|r| {
                        let status = if r.healthy { "Healthy" } else { "Unhealthy" };
                        let style = if r.healthy {
                            Style::default().fg(Color::Green)
                        } else {
                            Style::default().fg(Color::Red)
                        };
                        Row::new(vec![r.service_name.clone(), status.to_string()]).style(style)
                    })
                    .collect();

                let widths = [Constraint::Percentage(60), Constraint::Percentage(40)];
                let table = Table::new(table_rows, widths).header(header).block(block);
                frame.render_widget(table, area);
            }
        }
    }
}

/// Extract an f64 from a column that may be Float64, Int64, or UInt64.
fn extract_f64(col: &Arc<dyn Array>, row: usize) -> Option<f64> {
    if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
        if arr.is_null(row) {
            None
        } else {
            Some(arr.value(row))
        }
    } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        if arr.is_null(row) {
            None
        } else {
            Some(arr.value(row) as f64)
        }
    } else if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
        if arr.is_null(row) {
            None
        } else {
            Some(arr.value(row) as f64)
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::tui::state::AppState;
    use crate::tui::test_helpers::assert_buffer_contains;

    fn make_state() -> AppState {
        AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            std::time::Duration::from_secs(5),
        )
    }

    #[test]
    fn renders_loading_state() {
        let mut terminal = Terminal::new(TestBackend::new(40, 10)).unwrap();
        let panel = ServiceHealthPanel::new();
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "Loading...");
        assert_buffer_contains(&terminal, "Service Health");
    }

    #[test]
    fn renders_error_state() {
        let mut terminal = Terminal::new(TestBackend::new(50, 10)).unwrap();
        let mut panel = ServiceHealthPanel::new();
        panel.set_error("connection refused".into());
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "Failed to load: connection refused");
    }

    #[test]
    fn renders_loaded_data() {
        let mut terminal = Terminal::new(TestBackend::new(50, 10)).unwrap();
        let mut panel = ServiceHealthPanel::new();
        panel.data = PanelData::Loaded(vec![
            ServiceRow {
                service_name: "writer-1".into(),
                healthy: true,
            },
            ServiceRow {
                service_name: "querier-1".into(),
                healthy: false,
            },
        ]);
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "writer-1");
        assert_buffer_contains(&terminal, "Healthy");
        assert_buffer_contains(&terminal, "querier-1");
        assert_buffer_contains(&terminal, "Unhealthy");
    }

    #[test]
    fn renders_empty_data() {
        let mut terminal = Terminal::new(TestBackend::new(40, 10)).unwrap();
        let mut panel = ServiceHealthPanel::new();
        panel.data = PanelData::Loaded(vec![]);
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "No services found");
    }

    #[test]
    fn snapshot_loading() {
        let mut terminal = Terminal::new(TestBackend::new(40, 8)).unwrap();
        let panel = ServiceHealthPanel::new();
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("service_health_loading", content);
    }

    #[test]
    fn snapshot_loaded() {
        let mut terminal = Terminal::new(TestBackend::new(50, 8)).unwrap();
        let mut panel = ServiceHealthPanel::new();
        panel.data = PanelData::Loaded(vec![
            ServiceRow {
                service_name: "writer-1".into(),
                healthy: true,
            },
            ServiceRow {
                service_name: "querier-1".into(),
                healthy: false,
            },
        ]);
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("service_health_loaded", content);
    }

    #[test]
    fn query_is_valid_sql() {
        let q = ServiceHealthPanel::query();
        assert!(q.contains("_signaldb_metrics"));
        assert!(q.contains("signaldb.service.healthy"));
    }
}
