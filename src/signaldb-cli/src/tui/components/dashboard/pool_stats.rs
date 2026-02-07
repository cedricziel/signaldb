//! Connection pool statistics panel for the dashboard.

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

#[derive(Debug, Clone)]
enum PanelData {
    Loading,
    Loaded(Vec<PoolRow>),
    Error(String),
}

#[derive(Debug, Clone)]
struct PoolRow {
    service_name: String,
    metric_name: String,
    value: f64,
}

/// Connection pool statistics panel.
pub struct PoolStatsPanel {
    data: PanelData,
}

impl PoolStatsPanel {
    pub fn new() -> Self {
        Self {
            data: PanelData::Loading,
        }
    }

    /// Populate panel from Flight SQL record batches.
    pub fn set_data(&mut self, batches: &[RecordBatch]) {
        let mut rows = Vec::new();
        for batch in batches {
            let services = batch
                .column_by_name("service_name")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let metric_names = batch
                .column_by_name("metric_name")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let values = batch.column_by_name("metric_value");

            let (services, metric_names) = match (services, metric_names) {
                (Some(s), Some(m)) => (s, m),
                _ => continue,
            };

            for i in 0..batch.num_rows() {
                rows.push(PoolRow {
                    service_name: services.value(i).to_string(),
                    metric_name: metric_names.value(i).to_string(),
                    value: values.and_then(|v| extract_f64(v, i)).unwrap_or(0.0),
                });
            }
        }
        self.data = PanelData::Loaded(rows);
    }

    /// Set an error on the panel.
    pub fn set_error(&mut self, msg: String) {
        self.data = PanelData::Error(msg);
    }

    /// SQL query for connection pool metrics.
    pub fn query() -> &'static str {
        "SELECT service_name, metric_value, metric_name FROM _signaldb_metrics \
         WHERE metric_name LIKE 'signaldb.flight.pool_%' \
         ORDER BY timestamp DESC LIMIT 20"
    }
}

impl Component for PoolStatsPanel {
    fn handle_key_event(&mut self, _key: KeyEvent) -> Option<Action> {
        None
    }

    fn update(&mut self, _action: &Action, _state: &mut AppState) {}

    fn render(&self, frame: &mut Frame, area: Rect, _state: &AppState) {
        let block = Block::default()
            .title(" Connection Pool ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Magenta));

        match &self.data {
            PanelData::Loading => {
                let loading = Paragraph::new("Loading...")
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
                    let empty = Paragraph::new("No pool data")
                        .style(Style::default().fg(Color::DarkGray))
                        .block(block);
                    frame.render_widget(empty, area);
                    return;
                }

                let header = Row::new(vec!["Service", "Metric", "Value"])
                    .style(Style::default().add_modifier(Modifier::BOLD))
                    .bottom_margin(1);

                let table_rows: Vec<Row> = rows
                    .iter()
                    .map(|r| {
                        let short_name = r
                            .metric_name
                            .strip_prefix("signaldb.flight.")
                            .unwrap_or(&r.metric_name);
                        Row::new(vec![
                            r.service_name.clone(),
                            short_name.to_string(),
                            format!("{}", r.value as u64),
                        ])
                    })
                    .collect();

                let widths = [
                    Constraint::Percentage(35),
                    Constraint::Percentage(35),
                    Constraint::Percentage(30),
                ];
                let table = Table::new(table_rows, widths).header(header).block(block);
                frame.render_widget(table, area);
            }
        }
    }
}

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
        let panel = PoolStatsPanel::new();
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "Loading...");
        assert_buffer_contains(&terminal, "Connection Pool");
    }

    #[test]
    fn renders_error_state() {
        let mut terminal = Terminal::new(TestBackend::new(50, 10)).unwrap();
        let mut panel = PoolStatsPanel::new();
        panel.set_error("query failed".into());
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "Failed to load: query failed");
    }

    #[test]
    fn renders_loaded_data() {
        let mut terminal = Terminal::new(TestBackend::new(60, 10)).unwrap();
        let mut panel = PoolStatsPanel::new();
        panel.data = PanelData::Loaded(vec![
            PoolRow {
                service_name: "router-1".into(),
                metric_name: "signaldb.flight.pool_active".into(),
                value: 5.0,
            },
            PoolRow {
                service_name: "router-1".into(),
                metric_name: "signaldb.flight.pool_idle".into(),
                value: 3.0,
            },
        ]);
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "router-1");
        assert_buffer_contains(&terminal, "pool_active");
        assert_buffer_contains(&terminal, "pool_idle");
    }

    #[test]
    fn snapshot_loading() {
        let mut terminal = Terminal::new(TestBackend::new(40, 8)).unwrap();
        let panel = PoolStatsPanel::new();
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("pool_stats_loading", content);
    }

    #[test]
    fn snapshot_loaded() {
        let mut terminal = Terminal::new(TestBackend::new(60, 8)).unwrap();
        let mut panel = PoolStatsPanel::new();
        panel.data = PanelData::Loaded(vec![PoolRow {
            service_name: "router-1".into(),
            metric_name: "signaldb.flight.pool_active".into(),
            value: 5.0,
        }]);
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("pool_stats_loaded", content);
    }

    #[test]
    fn query_is_valid_sql() {
        let q = PoolStatsPanel::query();
        assert!(q.contains("_signaldb_metrics"));
        assert!(q.contains("signaldb.flight.pool_%"));
    }
}
