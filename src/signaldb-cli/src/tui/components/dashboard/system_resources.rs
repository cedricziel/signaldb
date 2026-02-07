//! System resources panel with sparklines for CPU and memory.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use crossterm::event::KeyEvent;
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::widgets::{Block, Borders, Paragraph, Sparkline};

use crate::tui::action::Action;
use crate::tui::components::Component;
use crate::tui::state::AppState;

#[derive(Debug, Clone)]
enum PanelData {
    Loading,
    Loaded(ResourceData),
    Error(String),
}

#[derive(Debug, Clone, Default)]
struct ResourceData {
    /// service_name -> list of values (oldest first)
    cpu: HashMap<String, Vec<u64>>,
    /// service_name -> list of values in MB (oldest first)
    memory: HashMap<String, Vec<u64>>,
}

/// System resources panel with CPU and memory sparklines per service.
pub struct SystemResourcesPanel {
    data: PanelData,
}

impl SystemResourcesPanel {
    pub fn new() -> Self {
        Self {
            data: PanelData::Loading,
        }
    }

    /// Populate panel from Flight SQL record batches.
    pub fn set_data(&mut self, batches: &[RecordBatch]) {
        let mut resource_data = ResourceData::default();

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
                let service = services.value(i).to_string();
                let metric = metric_names.value(i);
                let value = values.and_then(|v| extract_f64(v, i)).unwrap_or(0.0);

                if metric.contains("cpu_usage_percent") {
                    resource_data
                        .cpu
                        .entry(service)
                        .or_default()
                        .push(value as u64);
                } else if metric.contains("memory_used_bytes") {
                    let mb = (value / 1_048_576.0) as u64;
                    resource_data.memory.entry(service).or_default().push(mb);
                }
            }
        }

        // Reverse so oldest is first (query returns DESC)
        for values in resource_data.cpu.values_mut() {
            values.reverse();
        }
        for values in resource_data.memory.values_mut() {
            values.reverse();
        }

        self.data = PanelData::Loaded(resource_data);
    }

    /// Set an error on the panel.
    pub fn set_error(&mut self, msg: String) {
        self.data = PanelData::Error(msg);
    }

    /// SQL query for system resource metrics.
    pub fn query() -> &'static str {
        "SELECT service_name, metric_name, metric_value, timestamp \
         FROM _signaldb_metrics \
         WHERE metric_name IN ('signaldb.system.cpu_usage_percent', \
         'signaldb.system.memory_used_bytes') \
         ORDER BY timestamp DESC LIMIT 500"
    }
}

impl Component for SystemResourcesPanel {
    fn handle_key_event(&mut self, _key: KeyEvent) -> Option<Action> {
        None
    }

    fn update(&mut self, _action: &Action, _state: &mut AppState) {}

    fn render(&self, frame: &mut Frame, area: Rect, state: &AppState) {
        let block = Block::default()
            .title(" System Resources ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Blue));

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
            PanelData::Loaded(data) => {
                if data.cpu.is_empty() && data.memory.is_empty() {
                    let empty = Paragraph::new("No resource data")
                        .style(Style::default().fg(Color::DarkGray))
                        .block(block);
                    frame.render_widget(empty, area);
                    return;
                }

                let inner = block.inner(area);
                frame.render_widget(block, area);

                let halves = Layout::default()
                    .direction(Direction::Horizontal)
                    .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                    .split(inner);

                render_sparkline_group(frame, halves[0], "CPU %", &data.cpu, Color::Cyan);
                render_sparkline_group(
                    frame,
                    halves[1],
                    "Memory (MB)",
                    &data.memory,
                    Color::Magenta,
                );
            }
        }
    }
}

fn render_sparkline_group(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    data: &HashMap<String, Vec<u64>>,
    color: Color,
) {
    if data.is_empty() || area.height < 2 {
        let empty =
            Paragraph::new(format!("{title}: no data")).style(Style::default().fg(Color::DarkGray));
        frame.render_widget(empty, area);
        return;
    }

    let mut services: Vec<&String> = data.keys().collect();
    services.sort();

    let rows_per_service = std::cmp::max(1, area.height as usize / services.len());
    let constraints: Vec<Constraint> = services
        .iter()
        .map(|_| Constraint::Length(rows_per_service as u16))
        .collect();

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(area);

    for (i, service) in services.iter().enumerate() {
        if i >= rows.len() {
            break;
        }
        let values = &data[*service];
        let label = format!("{title} [{service}]");
        let sparkline = Sparkline::default()
            .data(values)
            .style(Style::default().fg(color))
            .block(Block::default().title(label));
        frame.render_widget(sparkline, rows[i]);
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
        let mut terminal = Terminal::new(TestBackend::new(80, 10)).unwrap();
        let panel = SystemResourcesPanel::new();
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "Loading...");
        assert_buffer_contains(&terminal, "System Resources");
    }

    #[test]
    fn renders_error_state() {
        let mut terminal = Terminal::new(TestBackend::new(80, 10)).unwrap();
        let mut panel = SystemResourcesPanel::new();
        panel.set_error("timeout".into());
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "Failed to load: timeout");
    }

    #[test]
    fn renders_loaded_data() {
        let mut terminal = Terminal::new(TestBackend::new(80, 10)).unwrap();
        let mut panel = SystemResourcesPanel::new();
        let mut data = ResourceData::default();
        data.cpu.insert("writer-1".into(), vec![10, 20, 30, 40, 50]);
        data.memory
            .insert("writer-1".into(), vec![512, 520, 530, 540, 550]);
        panel.data = PanelData::Loaded(data);
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "CPU %");
        assert_buffer_contains(&terminal, "Memory (MB)");
    }

    #[test]
    fn renders_empty_data() {
        let mut terminal = Terminal::new(TestBackend::new(80, 10)).unwrap();
        let mut panel = SystemResourcesPanel::new();
        panel.data = PanelData::Loaded(ResourceData::default());
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "No resource data");
    }

    #[test]
    fn snapshot_loading() {
        let mut terminal = Terminal::new(TestBackend::new(80, 8)).unwrap();
        let panel = SystemResourcesPanel::new();
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("system_resources_loading", content);
    }

    #[test]
    fn snapshot_loaded() {
        let mut terminal = Terminal::new(TestBackend::new(80, 8)).unwrap();
        let mut panel = SystemResourcesPanel::new();
        let mut data = ResourceData::default();
        data.cpu.insert("writer-1".into(), vec![10, 20, 30, 40, 50]);
        data.memory.insert("writer-1".into(), vec![512, 520, 530]);
        panel.data = PanelData::Loaded(data);
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("system_resources_loaded", content);
    }

    #[test]
    fn query_is_valid_sql() {
        let q = SystemResourcesPanel::query();
        assert!(q.contains("_signaldb_metrics"));
        assert!(q.contains("cpu_usage_percent"));
        assert!(q.contains("memory_used_bytes"));
    }
}
