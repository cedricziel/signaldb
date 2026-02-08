//! Dynamic table rendering for log query results.

use std::sync::Arc;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Row, Table, TableState};

/// Tri-state data model for the log table.
#[derive(Debug, Clone)]
pub enum LogData {
    Loading,
    Loaded {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogGroupBy {
    None,
    Service,
    Severity,
    ScopeName,
}

impl LogGroupBy {
    pub fn label(&self) -> &'static str {
        match self {
            Self::None => "None",
            Self::Service => "Service",
            Self::Severity => "Severity",
            Self::ScopeName => "Scope Name",
        }
    }

    pub fn key(&self, columns: &[String], row: &[String]) -> String {
        let column_name = match self {
            Self::None => return String::new(),
            Self::Service => "service_name",
            Self::Severity => "severity_text",
            Self::ScopeName => "scope_name",
        };

        columns
            .iter()
            .position(|c| c == column_name)
            .and_then(|idx| row.get(idx))
            .cloned()
            .unwrap_or_default()
    }
}

/// Summary of a group of logs.
#[derive(Debug, Clone)]
pub struct LogGroupSummary {
    pub key: String,
    pub log_count: usize,
    pub traces: Vec<usize>,
}

/// View mode for logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogViewMode {
    Flat,
    GroupSummary,
    DrillIn { group_key: String },
}

/// Log table component with scrollable selection.
pub struct LogTable {
    pub data: LogData,
    pub table_state: TableState,
    pub(crate) severity_col_idx: Option<usize>,
    pub group_by: LogGroupBy,
    pub view_mode: LogViewMode,
    pub group_summaries: Vec<LogGroupSummary>,
    pub group_table_state: TableState,
}

impl LogTable {
    pub fn new() -> Self {
        Self {
            data: LogData::Loading,
            table_state: TableState::default(),
            severity_col_idx: None,
            group_by: LogGroupBy::None,
            view_mode: LogViewMode::Flat,
            group_summaries: Vec::new(),
            group_table_state: TableState::default(),
        }
    }

    pub fn set_group_by(&mut self, group_by: LogGroupBy) {
        self.group_by = group_by;
        self.view_mode = if self.group_by == LogGroupBy::None {
            LogViewMode::Flat
        } else {
            LogViewMode::GroupSummary
        };
        self.rebuild_group_summaries();
    }

    pub fn enter_selected_group(&mut self) -> bool {
        if self.view_mode != LogViewMode::GroupSummary {
            return false;
        }

        let Some(selected) = self.group_table_state.selected() else {
            return false;
        };
        let Some(group) = self.group_summaries.get(selected) else {
            return false;
        };

        self.view_mode = LogViewMode::DrillIn {
            group_key: group.key.clone(),
        };
        if group.log_count > 0 {
            self.table_state.select(Some(0));
        } else {
            self.table_state.select(None);
        }
        true
    }

    pub fn exit_drill_in(&mut self) -> bool {
        if self.is_drill_in_mode() {
            self.view_mode = LogViewMode::GroupSummary;
            return true;
        }
        false
    }

    pub fn is_drill_in_mode(&self) -> bool {
        matches!(self.view_mode, LogViewMode::DrillIn { .. })
    }

    /// Populate the table from Arrow `RecordBatch`es.
    pub fn set_data(&mut self, batches: &[RecordBatch]) {
        if batches.is_empty() {
            self.data = LogData::Loaded {
                columns: Vec::new(),
                rows: Vec::new(),
            };
            self.severity_col_idx = None;
            self.rebuild_group_summaries();
            return;
        }

        let schema = batches[0].schema();
        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

        self.severity_col_idx = columns.iter().position(|c| c == "severity_text");

        let mut rows = Vec::new();
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let row: Vec<String> = (0..batch.num_columns())
                    .map(|col_idx| format_cell(batch.column(col_idx), row_idx))
                    .collect();
                rows.push(row);
            }
        }

        self.data = LogData::Loaded { columns, rows };
        self.rebuild_group_summaries();
    }

    /// Set an error message.
    pub fn set_error(&mut self, msg: String) {
        self.data = LogData::Error(msg);
    }

    /// Move selection up by one row.
    pub fn scroll_up(&mut self) {
        let row_count = self.row_count();
        if row_count == 0 {
            return;
        }

        if self.view_mode == LogViewMode::GroupSummary {
            let current = self.group_table_state.selected().unwrap_or(0);
            let next = if current == 0 {
                row_count - 1
            } else {
                current - 1
            };
            self.group_table_state.select(Some(next));
            return;
        }

        let current = self.table_state.selected().unwrap_or(0);
        let next = if current == 0 {
            row_count - 1
        } else {
            current - 1
        };
        self.table_state.select(Some(next));
    }

    /// Move selection down by one row.
    pub fn scroll_down(&mut self) {
        let row_count = self.row_count();
        if row_count == 0 {
            return;
        }

        if self.view_mode == LogViewMode::GroupSummary {
            let current = self.group_table_state.selected().unwrap_or(0);
            self.group_table_state
                .select(Some((current + 1) % row_count));
            return;
        }

        let current = self.table_state.selected().unwrap_or(0);
        self.table_state.select(Some((current + 1) % row_count));
    }

    #[allow(dead_code)] // Used by table-focused unit tests
    /// Return the selected row index, if any.
    pub fn selected_index(&self) -> Option<usize> {
        match self.view_mode {
            LogViewMode::GroupSummary => self.group_table_state.selected(),
            LogViewMode::Flat | LogViewMode::DrillIn { .. } => self.table_state.selected(),
        }
    }

    /// Return the selected row data, if any.
    pub fn selected_row(&self) -> Option<(&[String], &[String])> {
        let LogData::Loaded { columns, rows } = &self.data else {
            return None;
        };

        match &self.view_mode {
            LogViewMode::GroupSummary => None,
            LogViewMode::Flat => self
                .table_state
                .selected()
                .and_then(|idx| rows.get(idx))
                .map(|row| (columns.as_slice(), row.as_slice())),
            LogViewMode::DrillIn { group_key } => {
                let selected = self.table_state.selected()?;
                let group = self.group_summaries.iter().find(|g| &g.key == group_key)?;
                let row_idx = *group.traces.get(selected)?;
                rows.get(row_idx)
                    .map(|row| (columns.as_slice(), row.as_slice()))
            }
        }
    }

    #[allow(dead_code)] // Used by table-focused unit tests
    /// Total row count for current view mode.
    pub fn row_count(&self) -> usize {
        let LogData::Loaded { rows, .. } = &self.data else {
            return 0;
        };

        match &self.view_mode {
            LogViewMode::Flat => rows.len(),
            LogViewMode::GroupSummary => self.group_summaries.len(),
            LogViewMode::DrillIn { group_key } => self
                .group_summaries
                .iter()
                .find(|g| &g.key == group_key)
                .map(|g| g.log_count)
                .unwrap_or(0),
        }
    }

    #[cfg(test)]
    pub fn render(&mut self, frame: &mut Frame, area: Rect, focused: bool) {
        self.render_with_spinner(frame, area, focused, None);
    }

    pub fn render_with_spinner(
        &mut self,
        frame: &mut Frame,
        area: Rect,
        focused: bool,
        spinner: Option<char>,
    ) {
        let border_color = if focused {
            Color::Cyan
        } else {
            Color::DarkGray
        };

        let row_info = match &self.data {
            LogData::Loaded { rows, .. } => match &self.view_mode {
                LogViewMode::Flat => {
                    let sel = self.table_state.selected().map(|s| s + 1).unwrap_or(0);
                    format!(" Logs [{sel}/{}] ", rows.len())
                }
                LogViewMode::GroupSummary => {
                    format!(" Logs [Grouped by {}] ", self.group_by.label())
                }
                LogViewMode::DrillIn { group_key } => {
                    format!(
                        " Logs [{}: {}] (Esc to go back) ",
                        self.group_by.label(),
                        group_key
                    )
                }
            },
            _ => " Logs ".to_string(),
        };

        let block = Block::default()
            .title(row_info)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(border_color));

        match self.data.clone() {
            LogData::Loading => {
                let text = if let Some(s) = spinner {
                    format!("{s} Loading...")
                } else {
                    "Loading...".to_string()
                };
                let loading = Paragraph::new(text)
                    .style(Style::default().fg(Color::DarkGray))
                    .block(block);
                frame.render_widget(loading, area);
            }
            LogData::Error(msg) => {
                let error = Paragraph::new(format!("Query failed: {msg}"))
                    .style(Style::default().fg(Color::Red))
                    .block(block);
                frame.render_widget(error, area);
            }
            LogData::Loaded { columns, rows } => {
                if rows.is_empty() {
                    let empty = Paragraph::new("No results")
                        .style(Style::default().fg(Color::DarkGray))
                        .block(block);
                    frame.render_widget(empty, area);
                    return;
                }

                match self.view_mode {
                    LogViewMode::GroupSummary => self.render_group_summary(frame, area, block),
                    LogViewMode::Flat => {
                        let indices: Vec<usize> = (0..rows.len()).collect();
                        self.render_log_table(frame, area, block, &columns, &rows, &indices);
                    }
                    LogViewMode::DrillIn { ref group_key } => {
                        let indices = self
                            .group_summaries
                            .iter()
                            .find(|g| &g.key == group_key)
                            .map(|g| g.traces.clone())
                            .unwrap_or_default();
                        self.render_log_table(frame, area, block, &columns, &rows, &indices);
                    }
                }
            }
        }
    }

    fn render_log_table(
        &mut self,
        frame: &mut Frame,
        area: Rect,
        block: Block,
        columns: &[String],
        rows: &[Vec<String>],
        indices: &[usize],
    ) {
        let header = Row::new(columns.to_vec())
            .style(
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            )
            .bottom_margin(1);

        let severity_idx = self.severity_col_idx;
        let table_rows: Vec<Row> = indices
            .iter()
            .filter_map(|&idx| rows.get(idx))
            .map(|row| {
                let severity = severity_idx.and_then(|i| row.get(i).map(|s| s.as_str()));
                let style = severity_style(severity);
                Row::new(row.clone()).style(style)
            })
            .collect();

        let widths = distribute_column_widths(columns.len());
        let table = Table::new(table_rows, widths)
            .header(header)
            .block(block)
            .row_highlight_style(
                Style::default()
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            );

        frame.render_stateful_widget(table, area, &mut self.table_state);
    }

    fn render_group_summary(&mut self, frame: &mut Frame, area: Rect, block: Block) {
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let bar_height = (self.group_summaries.len() as u16)
            .saturating_add(2)
            .clamp(4, 8);
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(bar_height), Constraint::Min(6)])
            .split(inner);

        let bar_block = Block::default()
            .title(" Distribution ")
            .borders(Borders::ALL);
        let bar_inner = bar_block.inner(chunks[0]);
        frame.render_widget(bar_block, chunks[0]);

        if self.group_summaries.is_empty() {
            frame.render_widget(Paragraph::new("No groups"), bar_inner);
        } else {
            let palette = [
                Color::Cyan,
                Color::Green,
                Color::Yellow,
                Color::Magenta,
                Color::Blue,
                Color::Red,
            ];
            let max_count = self
                .group_summaries
                .iter()
                .map(|g| g.log_count)
                .max()
                .unwrap_or(1);
            let label_space = 24usize;
            let max_bar_width = usize::from(bar_inner.width)
                .saturating_sub(label_space)
                .max(1);

            let lines: Vec<Line> = self
                .group_summaries
                .iter()
                .enumerate()
                .take(usize::from(bar_inner.height))
                .map(|(idx, group)| {
                    let color = palette[idx % palette.len()];
                    let ratio = if max_count == 0 {
                        0.0
                    } else {
                        group.log_count as f64 / max_count as f64
                    };
                    let bar_len = ((ratio * max_bar_width as f64).round() as usize).max(1);
                    let bar = "█".repeat(bar_len);
                    Line::from(vec![
                        Span::styled(bar, Style::default().fg(color)),
                        Span::raw(format!(" {} ({})", group.key, group.log_count)),
                    ])
                })
                .collect();

            frame.render_widget(Paragraph::new(lines), bar_inner);
        }

        let header = Row::new(vec!["Group", "Logs"]).style(
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        );
        let rows: Vec<Row> = self
            .group_summaries
            .iter()
            .map(|group| Row::new(vec![group.key.clone(), group.log_count.to_string()]))
            .collect();

        let table = Table::new(rows, [Constraint::Min(20), Constraint::Length(8)])
            .header(header)
            .block(Block::default().title(" Groups ").borders(Borders::ALL))
            .row_highlight_style(
                Style::default()
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("> ");

        frame.render_stateful_widget(table, chunks[1], &mut self.group_table_state);
    }

    fn rebuild_group_summaries(&mut self) {
        let LogData::Loaded { columns, rows } = &self.data else {
            self.group_summaries.clear();
            self.table_state.select(None);
            self.group_table_state.select(None);
            return;
        };

        if self.group_by == LogGroupBy::None {
            self.group_summaries.clear();
            self.view_mode = LogViewMode::Flat;
            if rows.is_empty() {
                self.table_state.select(None);
            } else {
                let selected = self
                    .table_state
                    .selected()
                    .unwrap_or(0)
                    .min(rows.len().saturating_sub(1));
                self.table_state.select(Some(selected));
            }
            self.group_table_state.select(None);
            return;
        }

        self.group_summaries = compute_group_summaries(columns, rows, &self.group_by);

        if self.group_summaries.is_empty() {
            self.group_table_state.select(None);
        } else {
            let selected = self
                .group_table_state
                .selected()
                .unwrap_or(0)
                .min(self.group_summaries.len() - 1);
            self.group_table_state.select(Some(selected));
        }

        match &self.view_mode {
            LogViewMode::DrillIn { group_key } => {
                if let Some(group) = self.group_summaries.iter().find(|g| &g.key == group_key) {
                    if group.log_count == 0 {
                        self.table_state.select(None);
                    } else {
                        let selected = self
                            .table_state
                            .selected()
                            .unwrap_or(0)
                            .min(group.log_count - 1);
                        self.table_state.select(Some(selected));
                    }
                } else {
                    self.view_mode = LogViewMode::GroupSummary;
                    self.table_state.select(None);
                }
            }
            LogViewMode::Flat | LogViewMode::GroupSummary => {
                self.view_mode = LogViewMode::GroupSummary;
                self.table_state.select(None);
            }
        }
    }
}

fn group_key_display(raw: String) -> String {
    if raw.trim().is_empty() {
        "(unknown)".to_string()
    } else {
        raw
    }
}

fn compute_group_summaries(
    columns: &[String],
    rows: &[Vec<String>],
    group_by: &LogGroupBy,
) -> Vec<LogGroupSummary> {
    if *group_by == LogGroupBy::None {
        return Vec::new();
    }

    let mut grouped: std::collections::BTreeMap<String, Vec<usize>> =
        std::collections::BTreeMap::new();
    for (idx, row) in rows.iter().enumerate() {
        grouped
            .entry(group_key_display(group_by.key(columns, row)))
            .or_default()
            .push(idx);
    }

    grouped
        .into_iter()
        .map(|(key, traces)| LogGroupSummary {
            key,
            log_count: traces.len(),
            traces,
        })
        .collect()
}

/// Map severity level to a row style.
fn severity_style(severity: Option<&str>) -> Style {
    match severity.map(|s| s.to_uppercase()).as_deref() {
        Some("ERROR" | "FATAL" | "CRITICAL") => Style::default().fg(Color::Red),
        Some("WARN" | "WARNING") => Style::default().fg(Color::Yellow),
        Some("DEBUG" | "TRACE") => Style::default().fg(Color::DarkGray),
        _ => Style::default(),
    }
}

/// Distribute column widths evenly across `n` columns.
fn distribute_column_widths(n: usize) -> Vec<Constraint> {
    if n == 0 {
        return vec![];
    }
    let pct = 100u16.saturating_div(n as u16).max(5);
    (0..n).map(|_| Constraint::Percentage(pct)).collect()
}

/// Format a single Arrow array cell as a display string.
fn format_cell(col: &Arc<dyn Array>, row: usize) -> String {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if col.is_null(row) {
        return "NULL".to_string();
    }

    match col.data_type() {
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::LargeUtf8 => col
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::UInt64 => col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| format!("{:.3}", a.value(row)))
            .unwrap_or_default(),
        DataType::Boolean => col
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Int32 => col
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::UInt32 => col
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Timestamp(_, _) => {
            if let Some(a) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
                let nanos = a.value(row);
                let secs = nanos / 1_000_000_000;
                let nsec = (nanos % 1_000_000_000) as u32;
                chrono::DateTime::from_timestamp(secs, nsec)
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
                    .unwrap_or_else(|| nanos.to_string())
            } else if let Some(a) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                let micros = a.value(row);
                let secs = micros / 1_000_000;
                let nsec = ((micros % 1_000_000) * 1_000) as u32;
                chrono::DateTime::from_timestamp(secs, nsec)
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
                    .unwrap_or_else(|| micros.to_string())
            } else {
                format!("{:?}", col)
            }
        }
        DataType::Date32 => col
            .as_any()
            .downcast_ref::<Date32Array>()
            .map(|a| {
                a.value_as_date(row)
                    .map(|d| d.to_string())
                    .unwrap_or_default()
            })
            .unwrap_or_default(),
        _ => {
            let formatter =
                arrow::util::display::ArrayFormatter::try_new(col.as_ref(), &Default::default());
            match formatter {
                Ok(f) => f.value(row).to_string(),
                Err(_) => format!("<{}>", col.data_type()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::tui::test_helpers::assert_buffer_contains;

    fn make_batches() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("severity_text", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("scope_name", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 2, 3])),
                Arc::new(StringArray::from(vec!["INFO", "ERROR", "WARN", "ERROR"])),
                Arc::new(StringArray::from(vec!["api", "api", "worker", "worker"])),
                Arc::new(StringArray::from(vec![
                    "ingest",
                    "ingest",
                    "scheduler",
                    "scheduler",
                ])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            ],
        )
        .unwrap();
        vec![batch]
    }

    #[test]
    fn log_group_by_key_reads_columns() {
        let columns = vec![
            "service_name".to_string(),
            "severity_text".to_string(),
            "scope_name".to_string(),
        ];
        let row = vec!["api".to_string(), "ERROR".to_string(), "ingest".to_string()];
        assert_eq!(LogGroupBy::Service.key(&columns, &row), "api");
        assert_eq!(LogGroupBy::Severity.key(&columns, &row), "ERROR");
        assert_eq!(LogGroupBy::ScopeName.key(&columns, &row), "ingest");
    }

    #[test]
    fn computes_group_summaries() {
        let batches = make_batches();
        let mut table = LogTable::new();
        table.set_data(&batches);
        table.set_group_by(LogGroupBy::Service);

        assert_eq!(table.group_summaries.len(), 2);
        let api = table
            .group_summaries
            .iter()
            .find(|g| g.key == "api")
            .unwrap();
        assert_eq!(api.log_count, 2);
        assert_eq!(api.traces, vec![0, 1]);
    }

    #[test]
    fn drill_in_and_back_preserves_group_mode() {
        let mut table = LogTable::new();
        table.set_data(&make_batches());
        table.set_group_by(LogGroupBy::Service);
        assert_eq!(table.view_mode, LogViewMode::GroupSummary);

        assert!(table.enter_selected_group());
        assert!(matches!(table.view_mode, LogViewMode::DrillIn { .. }));
        assert!(table.selected_row().is_some());

        assert!(table.exit_drill_in());
        assert_eq!(table.view_mode, LogViewMode::GroupSummary);
    }

    #[test]
    fn grouped_scroll_uses_group_table_state() {
        let mut table = LogTable::new();
        table.set_data(&make_batches());
        table.set_group_by(LogGroupBy::Service);

        assert_eq!(table.selected_index(), Some(0));
        table.scroll_down();
        assert_eq!(table.selected_index(), Some(1));
        table.scroll_down();
        assert_eq!(table.selected_index(), Some(0));
    }

    #[test]
    fn renders_grouped_summary_with_chart_and_table() {
        let mut terminal = Terminal::new(TestBackend::new(100, 14)).unwrap();
        let mut table = LogTable::new();
        table.set_data(&make_batches());
        table.set_group_by(LogGroupBy::Service);
        terminal
            .draw(|frame| table.render(frame, frame.area(), true))
            .unwrap();
        assert_buffer_contains(&terminal, "Grouped by Service");
        assert_buffer_contains(&terminal, "Distribution");
        assert_buffer_contains(&terminal, "Groups");
    }

    #[test]
    fn renders_loading_state() {
        let mut terminal = Terminal::new(TestBackend::new(60, 10)).unwrap();
        let mut table = LogTable::new();
        terminal
            .draw(|frame| table.render(frame, frame.area(), false))
            .unwrap();
        assert_buffer_contains(&terminal, "Loading...");
    }

    #[test]
    fn renders_error_state() {
        let mut terminal = Terminal::new(TestBackend::new(60, 10)).unwrap();
        let mut table = LogTable::new();
        table.set_error("syntax error near SELECT".into());
        terminal
            .draw(|frame| table.render(frame, frame.area(), false))
            .unwrap();
        assert_buffer_contains(&terminal, "Query failed: syntax error near SELECT");
    }

    #[test]
    fn severity_style_mapping() {
        assert_eq!(severity_style(Some("ERROR")).fg, Some(Color::Red));
        assert_eq!(severity_style(Some("WARN")).fg, Some(Color::Yellow));
        assert_eq!(severity_style(Some("DEBUG")).fg, Some(Color::DarkGray));
        assert_eq!(severity_style(Some("INFO")).fg, None);
        assert_eq!(severity_style(None).fg, None);
    }

    #[test]
    fn snapshot_log_table_with_severities() {
        let mut terminal = Terminal::new(TestBackend::new(80, 10)).unwrap();
        let mut table = LogTable::new();
        table.set_data(&make_batches());
        terminal
            .draw(|frame| table.render(frame, frame.area(), true))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("log_table_severities", content);
    }

    #[test]
    fn snapshot_log_table_loading() {
        let mut terminal = Terminal::new(TestBackend::new(60, 6)).unwrap();
        let mut table = LogTable::new();
        terminal
            .draw(|frame| table.render(frame, frame.area(), false))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("log_table_loading", content);
    }

    #[test]
    fn snapshot_log_table_error() {
        let mut terminal = Terminal::new(TestBackend::new(60, 6)).unwrap();
        let mut table = LogTable::new();
        table.set_error("table 'logs' not found".into());
        terminal
            .draw(|frame| table.render(frame, frame.area(), false))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("log_table_error", content);
    }
}
