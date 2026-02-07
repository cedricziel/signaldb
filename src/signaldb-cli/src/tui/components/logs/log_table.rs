//! Dynamic table rendering for log query results.
//!
//! Renders a `ratatui::widgets::Table` from `Vec<RecordBatch>` with dynamic
//! columns derived from the query's schema. Rows with a `severity_text`
//! column are color-coded by severity level.

use std::sync::Arc;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use ratatui::Frame;
use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Color, Modifier, Style};
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

/// Log table component with scrollable selection.
pub struct LogTable {
    pub data: LogData,
    pub table_state: TableState,
    pub(crate) severity_col_idx: Option<usize>,
}

impl LogTable {
    pub fn new() -> Self {
        Self {
            data: LogData::Loading,
            table_state: TableState::default(),
            severity_col_idx: None,
        }
    }

    /// Populate the table from Arrow `RecordBatch`es.
    pub fn set_data(&mut self, batches: &[RecordBatch]) {
        if batches.is_empty() {
            self.data = LogData::Loaded {
                columns: Vec::new(),
                rows: Vec::new(),
            };
            self.severity_col_idx = None;
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

        let has_rows = !rows.is_empty();
        self.data = LogData::Loaded { columns, rows };

        if self.table_state.selected().is_none() && has_rows {
            self.table_state.select(Some(0));
        }
    }

    /// Set an error message.
    pub fn set_error(&mut self, msg: String) {
        self.data = LogData::Error(msg);
    }

    /// Move selection up by one row.
    pub fn scroll_up(&mut self) {
        if let LogData::Loaded { rows, .. } = &self.data {
            if rows.is_empty() {
                return;
            }
            let current = self.table_state.selected().unwrap_or(0);
            let next = if current == 0 {
                rows.len() - 1
            } else {
                current - 1
            };
            self.table_state.select(Some(next));
        }
    }

    /// Move selection down by one row.
    pub fn scroll_down(&mut self) {
        if let LogData::Loaded { rows, .. } = &self.data {
            if rows.is_empty() {
                return;
            }
            let current = self.table_state.selected().unwrap_or(0);
            let next = (current + 1) % rows.len();
            self.table_state.select(Some(next));
        }
    }

    /// Return the selected row index, if any.
    pub fn selected_index(&self) -> Option<usize> {
        self.table_state.selected()
    }

    /// Return the selected row data, if any.
    pub fn selected_row(&self) -> Option<(&[String], &[String])> {
        if let LogData::Loaded { columns, rows } = &self.data {
            self.table_state
                .selected()
                .and_then(|idx| rows.get(idx))
                .map(|row| (columns.as_slice(), row.as_slice()))
        } else {
            None
        }
    }

    /// Total row count.
    pub fn row_count(&self) -> usize {
        match &self.data {
            LogData::Loaded { rows, .. } => rows.len(),
            _ => 0,
        }
    }

    /// Render the log table.
    pub fn render(&mut self, frame: &mut Frame, area: Rect, focused: bool) {
        let border_color = if focused {
            Color::Cyan
        } else {
            Color::DarkGray
        };

        let row_info = match &self.data {
            LogData::Loaded { rows, .. } => {
                let sel = self.table_state.selected().map(|s| s + 1).unwrap_or(0);
                format!(" Logs [{sel}/{}] ", rows.len())
            }
            _ => " Logs ".to_string(),
        };

        let block = Block::default()
            .title(row_info)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(border_color));

        match &self.data {
            LogData::Loading => {
                let loading = Paragraph::new("Loading...")
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

                let header = Row::new(columns.clone())
                    .style(
                        Style::default()
                            .fg(Color::White)
                            .add_modifier(Modifier::BOLD),
                    )
                    .bottom_margin(1);

                let severity_idx = self.severity_col_idx;

                let table_rows: Vec<Row> = rows
                    .iter()
                    .map(|row| {
                        let severity = severity_idx.and_then(|i| row.get(i).map(|s| s.as_str()));
                        let style = severity_style(severity);
                        Row::new(row.clone()).style(style)
                    })
                    .collect();

                let col_count = columns.len();
                let widths: Vec<Constraint> = distribute_column_widths(col_count);

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
        }
    }
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
            // Fallback: use Arrow's display formatting
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

    fn make_batches(severities: &[&str], bodies: &[&str]) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("severity_text", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, false),
        ]));
        let timestamps: Vec<i64> = (0..severities.len() as i64).collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(timestamps)),
                Arc::new(StringArray::from(severities.to_vec())),
                Arc::new(StringArray::from(bodies.to_vec())),
            ],
        )
        .unwrap();
        vec![batch]
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
    fn renders_empty_results() {
        let mut terminal = Terminal::new(TestBackend::new(60, 10)).unwrap();
        let mut table = LogTable::new();
        table.set_data(&[]);
        terminal
            .draw(|frame| table.render(frame, frame.area(), false))
            .unwrap();
        assert_buffer_contains(&terminal, "No results");
    }

    #[test]
    fn renders_rows_with_columns() {
        let mut terminal = Terminal::new(TestBackend::new(80, 12)).unwrap();
        let mut table = LogTable::new();
        let batches = make_batches(&["INFO", "ERROR"], &["request started", "disk full"]);
        table.set_data(&batches);
        terminal
            .draw(|frame| table.render(frame, frame.area(), true))
            .unwrap();
        assert_buffer_contains(&terminal, "timestamp");
        assert_buffer_contains(&terminal, "severity_text");
        assert_buffer_contains(&terminal, "body");
        assert_buffer_contains(&terminal, "request started");
        assert_buffer_contains(&terminal, "disk full");
    }

    #[test]
    fn scroll_up_down_wraps() {
        let mut table = LogTable::new();
        let batches = make_batches(&["INFO", "WARN", "ERROR"], &["a", "b", "c"]);
        table.set_data(&batches);

        assert_eq!(table.selected_index(), Some(0));
        table.scroll_down();
        assert_eq!(table.selected_index(), Some(1));
        table.scroll_down();
        assert_eq!(table.selected_index(), Some(2));
        table.scroll_down();
        assert_eq!(table.selected_index(), Some(0));

        table.scroll_up();
        assert_eq!(table.selected_index(), Some(2));
    }

    #[test]
    fn selected_row_returns_data() {
        let mut table = LogTable::new();
        let batches = make_batches(&["ERROR"], &["disk full"]);
        table.set_data(&batches);

        let (cols, row) = table.selected_row().unwrap();
        assert_eq!(cols, &["timestamp", "severity_text", "body"]);
        assert_eq!(row[1], "ERROR");
        assert_eq!(row[2], "disk full");
    }

    #[test]
    fn severity_style_mapping() {
        assert_eq!(severity_style(Some("ERROR")).fg, Some(Color::Red));
        assert_eq!(severity_style(Some("error")).fg, Some(Color::Red));
        assert_eq!(severity_style(Some("FATAL")).fg, Some(Color::Red));
        assert_eq!(severity_style(Some("WARN")).fg, Some(Color::Yellow));
        assert_eq!(severity_style(Some("WARNING")).fg, Some(Color::Yellow));
        assert_eq!(severity_style(Some("DEBUG")).fg, Some(Color::DarkGray));
        assert_eq!(severity_style(Some("TRACE")).fg, Some(Color::DarkGray));
        assert_eq!(severity_style(Some("INFO")).fg, None);
        assert_eq!(severity_style(None).fg, None);
    }

    #[test]
    fn format_cell_null_returns_null() {
        use arrow::array::StringArray;
        let arr: Arc<dyn Array> = Arc::new(StringArray::from(vec![None as Option<&str>]));
        assert_eq!(format_cell(&arr, 0), "NULL");
    }

    #[test]
    fn format_cell_string() {
        let arr: Arc<dyn Array> = Arc::new(StringArray::from(vec!["hello"]));
        assert_eq!(format_cell(&arr, 0), "hello");
    }

    #[test]
    fn format_cell_int64() {
        let arr: Arc<dyn Array> = Arc::new(Int64Array::from(vec![42]));
        assert_eq!(format_cell(&arr, 0), "42");
    }

    #[test]
    fn row_count_reflects_data() {
        let mut table = LogTable::new();
        assert_eq!(table.row_count(), 0);
        let batches = make_batches(&["INFO", "WARN"], &["a", "b"]);
        table.set_data(&batches);
        assert_eq!(table.row_count(), 2);
    }

    #[test]
    fn snapshot_log_table_with_severities() {
        let mut terminal = Terminal::new(TestBackend::new(80, 10)).unwrap();
        let mut table = LogTable::new();
        let batches = make_batches(
            &["ERROR", "WARN", "INFO", "DEBUG"],
            &["disk full", "high latency", "request ok", "parsing input"],
        );
        table.set_data(&batches);
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
