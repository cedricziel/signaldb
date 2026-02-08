//! Metrics tab: structured metric explorer with sparkline visualization.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Row, Table, TableState};
use tui_tree_widget::TreeState;

use super::Component;
use crate::tui::action::Action;
use crate::tui::client::models::{MetricFilters, MetricGroupBy, MetricNameInfo, MetricType};
use crate::tui::components::logs::log_table::{LogData, LogTable};
use crate::tui::components::logs::query_bar::{QueryBar, QueryBarAction};
use crate::tui::components::selector::{SelectorAction, SelectorItem, SelectorPopup};
use crate::tui::state::AppState;
use crate::tui::widgets::json_viewer::render_json_tree;
use crate::tui::widgets::sparkline::scale_to_u64;

const SPARKLINE_COLORS: &[Color] = &[
    Color::Cyan,
    Color::Magenta,
    Color::Green,
    Color::Yellow,
    Color::Blue,
    Color::Red,
];

#[derive(Debug, Clone, PartialEq, Eq)]
enum Focus {
    MetricList,
    DataTable,
    QueryBar,
}

struct MetricSelector {
    items: Vec<MetricNameInfo>,
    filter_text: String,
    selected: usize,
}

impl MetricSelector {
    fn new() -> Self {
        Self {
            items: Vec::new(),
            filter_text: String::new(),
            selected: 0,
        }
    }

    fn set_items(&mut self, items: Vec<MetricNameInfo>) {
        self.items = items;
        self.clamp_selection();
    }

    fn filtered_items(&self) -> Vec<&MetricNameInfo> {
        if self.filter_text.is_empty() {
            return self.items.iter().collect();
        }

        let needle = self.filter_text.to_lowercase();
        self.items
            .iter()
            .filter(|m| m.name.to_lowercase().contains(&needle))
            .collect()
    }

    fn scroll_up(&mut self) {
        let len = self.filtered_items().len();
        if len == 0 {
            self.selected = 0;
            return;
        }
        self.selected = if self.selected == 0 {
            len - 1
        } else {
            self.selected - 1
        };
    }

    fn scroll_down(&mut self) {
        let len = self.filtered_items().len();
        if len == 0 {
            self.selected = 0;
            return;
        }
        self.selected = (self.selected + 1) % len;
    }

    fn selected_item(&self) -> Option<&MetricNameInfo> {
        let filtered = self.filtered_items();
        filtered.get(self.selected).copied()
    }

    fn handle_char(&mut self, c: char) {
        self.filter_text.push(c);
        self.clamp_selection();
    }

    fn handle_backspace(&mut self) {
        self.filter_text.pop();
        self.clamp_selection();
    }

    fn clear_filter(&mut self) {
        self.filter_text.clear();
        self.selected = 0;
    }

    fn clamp_selection(&mut self) {
        let len = self.filtered_items().len();
        if len == 0 {
            self.selected = 0;
        } else if self.selected >= len {
            self.selected = len - 1;
        }
    }

    fn render(&self, frame: &mut Frame, area: Rect, focused: bool) {
        let title = if self.filter_text.is_empty() {
            " Metrics ".to_string()
        } else {
            format!(" Metrics (filter: {}) ", self.filter_text)
        };

        let border_color = if focused {
            Color::Cyan
        } else {
            Color::DarkGray
        };

        let block = Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(border_color));

        let filtered = self.filtered_items();
        if filtered.is_empty() {
            frame.render_widget(
                Paragraph::new("No metrics found")
                    .style(Style::default().fg(Color::DarkGray))
                    .block(block),
                area,
            );
            return;
        }

        let items: Vec<ListItem> = filtered
            .iter()
            .map(|metric| {
                let badge = format!("[{}]", metric.metric_type.badge());
                let desc = if metric.description.is_empty() {
                    "(no description)".to_string()
                } else {
                    metric.description.clone()
                };
                let unit = if metric.unit.is_empty() {
                    "-".to_string()
                } else {
                    metric.unit.clone()
                };

                let line = Line::from(vec![
                    Span::styled(
                        badge,
                        Style::default()
                            .fg(metric_type_color(&metric.metric_type))
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(" "),
                    Span::raw(format!("{} -- {} ({})", metric.name, desc, unit)),
                ]);
                ListItem::new(line)
            })
            .collect();

        let mut list_state = ratatui::widgets::ListState::default();
        list_state.select(Some(self.selected));

        let list = List::new(items)
            .block(block)
            .highlight_symbol("> ")
            .highlight_style(
                Style::default()
                    .fg(Color::White)
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            );

        frame.render_stateful_widget(list, area, &mut list_state);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MetricAttributeTab {
    MetricAttributes,
    ResourceAttributes,
    Scope,
}

struct MetricDetail {
    attribute_tab: MetricAttributeTab,
    json_state: TreeState<String>,
}

impl MetricDetail {
    fn new() -> Self {
        Self {
            attribute_tab: MetricAttributeTab::MetricAttributes,
            json_state: TreeState::default(),
        }
    }

    fn cycle_attribute_tab(&mut self) {
        self.attribute_tab = match self.attribute_tab {
            MetricAttributeTab::MetricAttributes => MetricAttributeTab::ResourceAttributes,
            MetricAttributeTab::ResourceAttributes => MetricAttributeTab::Scope,
            MetricAttributeTab::Scope => MetricAttributeTab::MetricAttributes,
        };
        self.json_state = TreeState::default();
    }

    fn render(&mut self, frame: &mut Frame, area: Rect, columns: &[String], values: &[String]) {
        let (attrs, tab_label) = match self.attribute_tab {
            MetricAttributeTab::MetricAttributes => (
                json_column(columns, values, "attributes"),
                "Metric Attributes",
            ),
            MetricAttributeTab::ResourceAttributes => (
                json_column(columns, values, "resource_attributes"),
                "Resource Attributes",
            ),
            MetricAttributeTab::Scope => (scope_json(columns, values), "Scope"),
        };

        let active_indicator = match self.attribute_tab {
            MetricAttributeTab::MetricAttributes => "[Attrs ● | Resource ○ | Scope ○] ",
            MetricAttributeTab::ResourceAttributes => "[Attrs ○ | Resource ● | Scope ○] ",
            MetricAttributeTab::Scope => "[Attrs ○ | Resource ○ | Scope ●] ",
        };

        let title = format!("{active_indicator}{tab_label} (r to switch)");
        render_json_tree(frame, area, &attrs, &mut self.json_state, &title);
    }
}

#[derive(Debug, Clone)]
struct MetricGroupSummary {
    key: String,
    count: usize,
    indices: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MetricViewMode {
    Flat,
    GroupSummary,
    DrillIn { group_key: String },
}

/// Metrics tab composing selector, table, detail viewer, and SQL fallback.
pub struct MetricsPanel {
    metric_selector: MetricSelector,
    selected_metric: Option<MetricNameInfo>,
    data_table: LogTable,
    metric_detail: MetricDetail,
    query_bar: QueryBar,
    raw_sql_mode: bool,
    focus: Focus,
    show_detail: bool,
    show_group_selector: bool,
    group_selector: SelectorPopup,
    group_by: MetricGroupBy,
    view_mode: MetricViewMode,
    group_summaries: Vec<MetricGroupSummary>,
    group_table_state: TableState,
    sparkline_data: BTreeMap<String, Vec<f64>>,
    pub pending_discovery: bool,
    pub pending_metric_query: Option<(String, MetricType, MetricFilters)>,
    pub pending_query: Option<String>,
}

impl MetricsPanel {
    pub fn new() -> Self {
        Self {
            metric_selector: MetricSelector::new(),
            selected_metric: None,
            data_table: LogTable::new(),
            metric_detail: MetricDetail::new(),
            query_bar: QueryBar::new(),
            raw_sql_mode: false,
            focus: Focus::MetricList,
            show_detail: false,
            show_group_selector: false,
            group_selector: build_metric_group_selector(),
            group_by: MetricGroupBy::None,
            view_mode: MetricViewMode::Flat,
            group_summaries: Vec::new(),
            group_table_state: TableState::default(),
            sparkline_data: BTreeMap::new(),
            pending_discovery: true,
            pending_metric_query: None,
            pending_query: None,
        }
    }

    pub fn set_metric_names(&mut self, names: Vec<MetricNameInfo>) {
        self.metric_selector.set_items(names);
    }

    pub fn set_data(&mut self, batches: &[RecordBatch]) {
        self.data_table.set_data(batches);
        self.sparkline_data = extract_sparkline_data(batches);
        self.rebuild_group_summaries();
    }

    pub fn set_error(&mut self, msg: String) {
        self.data_table.set_error(msg);
        self.sparkline_data.clear();
        self.rebuild_group_summaries();
    }

    pub fn set_group_by(&mut self, group_by: MetricGroupBy) {
        self.group_by = group_by;
        self.view_mode = if self.group_by == MetricGroupBy::None {
            MetricViewMode::Flat
        } else {
            MetricViewMode::GroupSummary
        };
        self.rebuild_group_summaries();
    }

    pub fn take_pending_discovery(&mut self) -> bool {
        let val = self.pending_discovery;
        self.pending_discovery = false;
        val
    }

    pub fn take_pending_metric_query(&mut self) -> Option<(String, MetricType, MetricFilters)> {
        self.pending_metric_query.take()
    }

    pub fn take_pending_query(&mut self) -> Option<String> {
        self.pending_query.take()
    }

    pub fn has_pending(&self) -> bool {
        self.pending_discovery
            || self.pending_metric_query.is_some()
            || self.pending_query.is_some()
    }

    pub fn refresh(&mut self) {
        if self.raw_sql_mode {
            self.pending_query = Some(self.query_bar.query().to_string());
        } else if let Some(ref metric) = self.selected_metric {
            self.pending_metric_query = Some((
                metric.name.clone(),
                metric.metric_type.clone(),
                MetricFilters::default(),
            ));
        } else {
            self.pending_discovery = true;
        }
    }

    fn scroll_up(&mut self) {
        let row_count = self.row_count();
        if row_count == 0 {
            return;
        }

        if self.view_mode == MetricViewMode::GroupSummary {
            let current = self.group_table_state.selected().unwrap_or(0);
            let next = if current == 0 {
                row_count - 1
            } else {
                current - 1
            };
            self.group_table_state.select(Some(next));
            return;
        }

        let current = self.data_table.table_state.selected().unwrap_or(0);
        let next = if current == 0 {
            row_count - 1
        } else {
            current - 1
        };
        self.data_table.table_state.select(Some(next));
    }

    fn scroll_down(&mut self) {
        let row_count = self.row_count();
        if row_count == 0 {
            return;
        }

        if self.view_mode == MetricViewMode::GroupSummary {
            let current = self.group_table_state.selected().unwrap_or(0);
            self.group_table_state
                .select(Some((current + 1) % row_count));
            return;
        }

        let current = self.data_table.table_state.selected().unwrap_or(0);
        self.data_table
            .table_state
            .select(Some((current + 1) % row_count));
    }

    fn row_count(&self) -> usize {
        let LogData::Loaded { rows, .. } = &self.data_table.data else {
            return 0;
        };

        match &self.view_mode {
            MetricViewMode::Flat => rows.len(),
            MetricViewMode::GroupSummary => self.group_summaries.len(),
            MetricViewMode::DrillIn { group_key } => self
                .group_summaries
                .iter()
                .find(|g| &g.key == group_key)
                .map(|g| g.count)
                .unwrap_or(0),
        }
    }

    fn selected_row(&self) -> Option<(&[String], &[String])> {
        let LogData::Loaded { columns, rows } = &self.data_table.data else {
            return None;
        };

        match &self.view_mode {
            MetricViewMode::GroupSummary => None,
            MetricViewMode::Flat => self
                .data_table
                .table_state
                .selected()
                .and_then(|idx| rows.get(idx))
                .map(|row| (columns.as_slice(), row.as_slice())),
            MetricViewMode::DrillIn { group_key } => {
                let selected = self.data_table.table_state.selected()?;
                let group = self.group_summaries.iter().find(|g| &g.key == group_key)?;
                let row_idx = *group.indices.get(selected)?;
                rows.get(row_idx)
                    .map(|row| (columns.as_slice(), row.as_slice()))
            }
        }
    }

    fn rebuild_group_summaries(&mut self) {
        let LogData::Loaded { columns, rows } = &self.data_table.data else {
            self.group_summaries.clear();
            self.data_table.table_state.select(None);
            self.group_table_state.select(None);
            return;
        };

        if self.group_by == MetricGroupBy::None {
            self.group_summaries.clear();
            self.view_mode = MetricViewMode::Flat;
            if rows.is_empty() {
                self.data_table.table_state.select(None);
            } else {
                let selected = self
                    .data_table
                    .table_state
                    .selected()
                    .unwrap_or(0)
                    .min(rows.len().saturating_sub(1));
                self.data_table.table_state.select(Some(selected));
            }
            self.group_table_state.select(None);
            return;
        }

        let mut grouped: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for (idx, row) in rows.iter().enumerate() {
            let raw_key = self.group_by.key(columns, row);
            let key = if raw_key.trim().is_empty() {
                "(unknown)".to_string()
            } else {
                raw_key
            };
            grouped.entry(key).or_default().push(idx);
        }

        self.group_summaries = grouped
            .into_iter()
            .map(|(key, indices)| MetricGroupSummary {
                key,
                count: indices.len(),
                indices,
            })
            .collect();

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
            MetricViewMode::DrillIn { group_key } => {
                if let Some(group) = self.group_summaries.iter().find(|g| &g.key == group_key) {
                    if group.count == 0 {
                        self.data_table.table_state.select(None);
                    } else {
                        let selected = self
                            .data_table
                            .table_state
                            .selected()
                            .unwrap_or(0)
                            .min(group.count - 1);
                        self.data_table.table_state.select(Some(selected));
                    }
                } else {
                    self.view_mode = MetricViewMode::GroupSummary;
                    self.data_table.table_state.select(None);
                }
            }
            MetricViewMode::Flat | MetricViewMode::GroupSummary => {
                self.view_mode = MetricViewMode::GroupSummary;
                self.data_table.table_state.select(None);
            }
        }
    }

    fn enter_selected_group(&mut self) -> bool {
        if self.view_mode != MetricViewMode::GroupSummary {
            return false;
        }

        let Some(selected) = self.group_table_state.selected() else {
            return false;
        };
        let Some(group) = self.group_summaries.get(selected) else {
            return false;
        };

        self.view_mode = MetricViewMode::DrillIn {
            group_key: group.key.clone(),
        };
        if group.count > 0 {
            self.data_table.table_state.select(Some(0));
        } else {
            self.data_table.table_state.select(None);
        }
        true
    }

    fn exit_drill_in(&mut self) -> bool {
        if matches!(self.view_mode, MetricViewMode::DrillIn { .. }) {
            self.view_mode = MetricViewMode::GroupSummary;
            return true;
        }
        false
    }

    fn render_data_table(
        &self,
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

        let row_info = match &self.data_table.data {
            LogData::Loaded { rows, .. } => match &self.view_mode {
                MetricViewMode::Flat => {
                    let sel = self
                        .data_table
                        .table_state
                        .selected()
                        .map(|s| s + 1)
                        .unwrap_or(0);
                    format!(" Metrics [{sel}/{}] ", rows.len())
                }
                MetricViewMode::GroupSummary => {
                    format!(" Metrics [Grouped by {}] ", self.group_by.label())
                }
                MetricViewMode::DrillIn { group_key } => {
                    format!(
                        " Metrics [{}: {}] (Esc to go back) ",
                        self.group_by.label(),
                        group_key
                    )
                }
            },
            _ => " Metrics ".to_string(),
        };

        let block = Block::default()
            .title(row_info)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(border_color));

        match self.data_table.data.clone() {
            LogData::Loading => {
                let text = if let Some(s) = spinner {
                    format!("{s} Loading...")
                } else {
                    "Loading...".to_string()
                };
                frame.render_widget(
                    Paragraph::new(text)
                        .style(Style::default().fg(Color::DarkGray))
                        .block(block),
                    area,
                );
            }
            LogData::Error(msg) => {
                frame.render_widget(
                    Paragraph::new(format!("Query failed: {msg}"))
                        .style(Style::default().fg(Color::Red))
                        .block(block),
                    area,
                );
            }
            LogData::Loaded { columns, rows } => {
                if rows.is_empty() {
                    frame.render_widget(
                        Paragraph::new("No results")
                            .style(Style::default().fg(Color::DarkGray))
                            .block(block),
                        area,
                    );
                    return;
                }

                match self.view_mode {
                    MetricViewMode::GroupSummary => {
                        self.render_group_summary(frame, area, block);
                    }
                    MetricViewMode::Flat => {
                        let indices: Vec<usize> = (0..rows.len()).collect();
                        self.render_metric_rows(frame, area, block, &columns, &rows, &indices);
                    }
                    MetricViewMode::DrillIn { ref group_key } => {
                        let indices = self
                            .group_summaries
                            .iter()
                            .find(|g| &g.key == group_key)
                            .map(|g| g.indices.clone())
                            .unwrap_or_default();
                        self.render_metric_rows(frame, area, block, &columns, &rows, &indices);
                    }
                }
            }
        }
    }

    fn render_metric_rows(
        &self,
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

        let table_rows: Vec<Row> = indices
            .iter()
            .filter_map(|&idx| rows.get(idx))
            .map(|row| Row::new(row.clone()))
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

        let mut table_state = self.data_table.table_state;
        frame.render_stateful_widget(table, area, &mut table_state);
    }

    fn render_group_summary(&self, frame: &mut Frame, area: Rect, block: Block) {
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
                .map(|g| g.count)
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
                        group.count as f64 / max_count as f64
                    };
                    let bar_len = ((ratio * max_bar_width as f64).round() as usize).max(1);
                    let bar = "█".repeat(bar_len);
                    Line::from(vec![
                        Span::styled(bar, Style::default().fg(color)),
                        Span::raw(format!(" {} ({})", group.key, group.count)),
                    ])
                })
                .collect();

            frame.render_widget(Paragraph::new(lines), bar_inner);
        }

        let header = Row::new(vec!["Group", "Count"]).style(
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        );
        let rows: Vec<Row> = self
            .group_summaries
            .iter()
            .map(|group| Row::new(vec![group.key.clone(), group.count.to_string()]))
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

        let mut group_table_state = self.group_table_state;
        frame.render_stateful_widget(table, chunks[1], &mut group_table_state);
    }
}

impl Component for MetricsPanel {
    fn handle_key_event(&mut self, key: KeyEvent) -> Option<Action> {
        if matches!(key.code, KeyCode::F(1) | KeyCode::F(2) | KeyCode::F(3)) {
            return None;
        }

        if self.show_group_selector {
            match self.group_selector.handle_key(key) {
                SelectorAction::Selected { id, .. } => {
                    self.set_group_by(metric_group_by_from_selector(&id));
                    self.show_group_selector = false;
                    self.group_selector.reset();
                    return Some(Action::None);
                }
                SelectorAction::Cancelled => {
                    self.show_group_selector = false;
                    self.group_selector.reset();
                    return Some(Action::None);
                }
                SelectorAction::None => return Some(Action::None),
            }
        }

        match self.focus {
            Focus::QueryBar => {
                match self.query_bar.handle_key(key) {
                    QueryBarAction::Execute => {
                        self.pending_query = Some(self.query_bar.query().to_string());
                    }
                    QueryBarAction::Blur => {
                        self.query_bar.focused = false;
                        self.raw_sql_mode = false;
                        self.focus = if self.selected_metric.is_some() {
                            Focus::DataTable
                        } else {
                            Focus::MetricList
                        };
                    }
                    QueryBarAction::None => {}
                }
                Some(Action::None)
            }
            Focus::MetricList => match key.code {
                KeyCode::Up | KeyCode::Char('k') => {
                    self.metric_selector.scroll_up();
                    Some(Action::None)
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.metric_selector.scroll_down();
                    Some(Action::None)
                }
                KeyCode::Enter => {
                    if let Some(metric) = self.metric_selector.selected_item().cloned() {
                        self.selected_metric = Some(metric.clone());
                        self.pending_metric_query =
                            Some((metric.name, metric.metric_type, MetricFilters::default()));
                        self.focus = Focus::DataTable;
                        self.raw_sql_mode = false;
                        self.show_detail = false;
                    }
                    Some(Action::None)
                }
                KeyCode::Char('/') => {
                    self.raw_sql_mode = true;
                    self.focus = Focus::QueryBar;
                    self.query_bar.focused = true;
                    Some(Action::None)
                }
                KeyCode::Esc => {
                    if self.metric_selector.filter_text.is_empty() {
                        None
                    } else {
                        self.metric_selector.clear_filter();
                        Some(Action::None)
                    }
                }
                KeyCode::Backspace => {
                    self.metric_selector.handle_backspace();
                    Some(Action::None)
                }
                KeyCode::Char('g') => {
                    self.show_group_selector = true;
                    self.group_selector.reset();
                    Some(Action::None)
                }
                KeyCode::Char(':') => Some(Action::None),
                KeyCode::Char(c) => {
                    self.metric_selector.handle_char(c);
                    Some(Action::None)
                }
                _ => None,
            },
            Focus::DataTable => match key.code {
                KeyCode::Up | KeyCode::Char('k') => {
                    self.scroll_up();
                    self.show_detail = false;
                    Some(Action::None)
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.scroll_down();
                    self.show_detail = false;
                    Some(Action::None)
                }
                KeyCode::Enter => {
                    if self.view_mode == MetricViewMode::GroupSummary {
                        self.enter_selected_group();
                    } else {
                        self.show_detail = !self.show_detail;
                    }
                    Some(Action::None)
                }
                KeyCode::Esc => {
                    if self.show_detail {
                        self.show_detail = false;
                        return Some(Action::None);
                    }
                    if self.exit_drill_in() {
                        return Some(Action::None);
                    }
                    if self.view_mode == MetricViewMode::GroupSummary {
                        self.set_group_by(MetricGroupBy::None);
                        return Some(Action::None);
                    }
                    self.selected_metric = None;
                    self.focus = Focus::MetricList;
                    Some(Action::None)
                }
                KeyCode::Char('g') => {
                    self.show_group_selector = true;
                    self.group_selector.reset();
                    Some(Action::None)
                }
                KeyCode::Char('r') => {
                    if self.show_detail {
                        self.metric_detail.cycle_attribute_tab();
                        Some(Action::None)
                    } else {
                        None
                    }
                }
                KeyCode::Char('/') => {
                    self.raw_sql_mode = true;
                    self.focus = Focus::QueryBar;
                    self.query_bar.focused = true;
                    Some(Action::None)
                }
                KeyCode::Char(':') => {
                    if self.show_detail {
                        Some(Action::None)
                    } else {
                        None
                    }
                }
                _ => None,
            },
        }
    }

    fn update(&mut self, action: &Action, _state: &mut AppState) {
        if matches!(action, Action::Refresh) {
            self.refresh();
        }
    }

    fn render(&self, frame: &mut Frame, area: Rect, state: &AppState) {
        if self.raw_sql_mode {
            let sparkline_height = if self.sparkline_data.is_empty() {
                Constraint::Length(3)
            } else {
                let rows = self.sparkline_data.len() as u16;
                Constraint::Min(rows.saturating_mul(3).max(10))
            };

            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(3), sparkline_height, Constraint::Min(0)])
                .split(area);

            self.query_bar
                .render_with_title(frame, chunks[0], Some(&state.time_range.label()));
            render_sparkline_panel(frame, chunks[1], &self.sparkline_data);

            let spinner = if state.loading {
                Some(state.spinner_char())
            } else {
                None
            };
            self.render_data_table(frame, chunks[2], self.focus == Focus::DataTable, spinner);
            return;
        }

        if self.selected_metric.is_none() {
            self.metric_selector
                .render(frame, area, self.focus == Focus::MetricList);
            if self.show_group_selector {
                self.group_selector.render(frame, area);
            }
            return;
        }

        let sparkline_height = if self.sparkline_data.is_empty() {
            Constraint::Length(3)
        } else {
            let rows = self.sparkline_data.len() as u16;
            Constraint::Min(rows.saturating_mul(3).max(8))
        };
        let detail_height = if self.show_detail {
            Constraint::Length(10)
        } else {
            Constraint::Length(0)
        };

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                sparkline_height,
                Constraint::Min(0),
                detail_height,
            ])
            .split(area);

        if let Some(metric) = &self.selected_metric {
            let desc = if metric.description.is_empty() {
                "(no description)"
            } else {
                metric.description.as_str()
            };
            let unit = if metric.unit.is_empty() {
                "-"
            } else {
                metric.unit.as_str()
            };

            let title = format!(
                " {} [{}] {} ({}) -- {} ",
                metric.name,
                metric.metric_type.badge(),
                metric.metric_type.label(),
                unit,
                desc
            );
            frame.render_widget(
                Block::default()
                    .title(title)
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(Color::DarkGray)),
                chunks[0],
            );
        }

        render_sparkline_panel(frame, chunks[1], &self.sparkline_data);

        let spinner = if state.loading {
            Some(state.spinner_char())
        } else {
            None
        };
        self.render_data_table(frame, chunks[2], self.focus == Focus::DataTable, spinner);

        if self.show_group_selector {
            self.group_selector.render(frame, chunks[2]);
        }

        if self.show_detail {
            let (columns, values) = self
                .selected_row()
                .map(|(c, v)| (c.to_vec(), v.to_vec()))
                .unwrap_or_default();
            let mut detail_clone = MetricDetail {
                attribute_tab: self.metric_detail.attribute_tab.clone(),
                json_state: TreeState::default(),
            };
            detail_clone.render(frame, chunks[3], &columns, &values);
        }
    }
}

fn metric_type_color(metric_type: &MetricType) -> Color {
    match metric_type {
        MetricType::Gauge => Color::Cyan,
        MetricType::Sum => Color::Green,
        MetricType::Histogram => Color::Yellow,
        MetricType::ExponentialHistogram => Color::Magenta,
        MetricType::Summary => Color::Blue,
    }
}

fn distribute_column_widths(n: usize) -> Vec<Constraint> {
    if n == 0 {
        return vec![];
    }
    let pct = 100u16.saturating_div(n as u16).max(5);
    (0..n).map(|_| Constraint::Percentage(pct)).collect()
}

fn build_metric_group_selector() -> SelectorPopup {
    let mut selector = SelectorPopup::new("Group Metrics By");
    selector.set_items(vec![
        SelectorItem {
            id: "none".to_string(),
            label: "None".to_string(),
            depth: 0,
            parent_id: None,
        },
        SelectorItem {
            id: "service".to_string(),
            label: "Service".to_string(),
            depth: 0,
            parent_id: None,
        },
        SelectorItem {
            id: "metric_type".to_string(),
            label: "Metric Type".to_string(),
            depth: 0,
            parent_id: None,
        },
        SelectorItem {
            id: "scope_name".to_string(),
            label: "Scope Name".to_string(),
            depth: 0,
            parent_id: None,
        },
    ]);
    selector
}

fn metric_group_by_from_selector(id: &str) -> MetricGroupBy {
    match id {
        "service" => MetricGroupBy::Service,
        "type" | "metric_type" => MetricGroupBy::MetricType,
        "scope" | "scope_name" => MetricGroupBy::ScopeName,
        _ => MetricGroupBy::None,
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

fn extract_sparkline_data(batches: &[RecordBatch]) -> BTreeMap<String, Vec<f64>> {
    let mut groups: BTreeMap<String, Vec<f64>> = BTreeMap::new();

    for batch in batches {
        let metric_names = batch
            .column_by_name("metric_name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let values_col = batch.column_by_name("metric_value");

        let metric_names = match metric_names {
            Some(m) => m,
            None => continue,
        };

        for i in 0..batch.num_rows() {
            let name = metric_names.value(i).to_string();
            let value = values_col.and_then(|v| extract_f64(v, i)).unwrap_or(0.0);
            groups.entry(name).or_default().push(value);
        }
    }

    for values in groups.values_mut() {
        values.reverse();
    }

    groups
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

fn render_sparkline_panel(frame: &mut Frame, area: Rect, data: &BTreeMap<String, Vec<f64>>) {
    let block = Block::default()
        .title(" Sparklines ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue));

    if data.is_empty() {
        let empty =
            Paragraph::new("No metric data -- run a query with metric_name + metric_value columns")
                .style(Style::default().fg(Color::DarkGray))
                .block(block);
        frame.render_widget(empty, area);
        return;
    }

    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.height < 2 {
        return;
    }

    let metrics: Vec<(&String, &Vec<f64>)> = data.iter().collect();
    let rows_per_metric = (inner.height as usize / metrics.len()).max(1);
    let constraints: Vec<Constraint> = metrics
        .iter()
        .map(|_| Constraint::Length(rows_per_metric as u16))
        .collect();

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(inner);

    for (i, (name, values)) in metrics.iter().enumerate() {
        if i >= rows.len() {
            break;
        }
        let color = SPARKLINE_COLORS[i % SPARKLINE_COLORS.len()];
        let scaled = scale_to_u64(values);
        let sparkline = ratatui::widgets::Sparkline::default()
            .data(&scaled)
            .style(Style::default().fg(color))
            .block(Block::default().title(name.as_str()));
        frame.render_widget(sparkline, rows[i]);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::tui::state::AppState;
    use crate::tui::test_helpers::assert_buffer_contains;

    fn press(code: KeyCode) -> KeyEvent {
        KeyEvent {
            code,
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        }
    }

    fn make_state() -> AppState {
        AppState::new(
            "http://localhost:3000".into(),
            "http://localhost:50053".into(),
            std::time::Duration::from_secs(5),
        )
    }

    fn make_metric_batches() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("metric_value", DataType::Float64, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("scope_name", DataType::Utf8, false),
            Field::new("metric_type", DataType::Utf8, false),
            Field::new("attributes", DataType::Utf8, false),
            Field::new("resource_attributes", DataType::Utf8, false),
            Field::new("scope_attributes", DataType::Utf8, false),
        ]));
        vec![
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500, 600])),
                    Arc::new(StringArray::from(vec![
                        "cpu_usage",
                        "cpu_usage",
                        "cpu_usage",
                        "memory_mb",
                        "memory_mb",
                        "memory_mb",
                    ])),
                    Arc::new(Float64Array::from(vec![
                        10.0, 25.0, 40.0, 512.0, 520.0, 530.0,
                    ])),
                    Arc::new(StringArray::from(vec![
                        "checkout", "checkout", "checkout", "worker", "worker", "worker",
                    ])),
                    Arc::new(StringArray::from(vec![
                        "runtime", "runtime", "runtime", "runtime", "runtime", "runtime",
                    ])),
                    Arc::new(StringArray::from(vec![
                        "gauge", "gauge", "gauge", "gauge", "gauge", "gauge",
                    ])),
                    Arc::new(StringArray::from(vec![
                        "{\"k\":1}",
                        "{\"k\":2}",
                        "{\"k\":3}",
                        "{\"k\":4}",
                        "{\"k\":5}",
                        "{\"k\":6}",
                    ])),
                    Arc::new(StringArray::from(vec![
                        "{\"service.name\":\"checkout\"}",
                        "{\"service.name\":\"checkout\"}",
                        "{\"service.name\":\"checkout\"}",
                        "{\"service.name\":\"worker\"}",
                        "{\"service.name\":\"worker\"}",
                        "{\"service.name\":\"worker\"}",
                    ])),
                    Arc::new(StringArray::from(vec![
                        "{\"scope.version\":\"1.0\"}",
                        "{\"scope.version\":\"1.0\"}",
                        "{\"scope.version\":\"1.0\"}",
                        "{\"scope.version\":\"1.0\"}",
                        "{\"scope.version\":\"1.0\"}",
                        "{\"scope.version\":\"1.0\"}",
                    ])),
                ],
            )
            .unwrap(),
        ]
    }

    fn metric(name: &str, metric_type: MetricType) -> MetricNameInfo {
        MetricNameInfo {
            name: name.to_string(),
            description: "desc".to_string(),
            unit: "ms".to_string(),
            metric_type,
        }
    }

    #[test]
    fn new_panel_starts_with_discovery_pending() {
        let panel = MetricsPanel::new();
        assert_eq!(panel.focus, Focus::MetricList);
        assert!(panel.pending_discovery);
        assert!(panel.pending_metric_query.is_none());
        assert!(panel.pending_query.is_none());
    }

    #[test]
    fn slash_focuses_query_bar() {
        let mut panel = MetricsPanel::new();
        let action = panel.handle_key_event(press(KeyCode::Char('/')));
        assert_eq!(panel.focus, Focus::QueryBar);
        assert!(panel.query_bar.focused);
        assert!(panel.raw_sql_mode);
        assert_eq!(action, Some(Action::None));
    }

    #[test]
    fn esc_from_query_bar_returns_to_metric_list_or_table() {
        let mut panel = MetricsPanel::new();
        panel.focus = Focus::QueryBar;
        panel.query_bar.focused = true;
        panel.raw_sql_mode = true;
        panel.handle_key_event(press(KeyCode::Esc));
        assert_eq!(panel.focus, Focus::MetricList);
        assert!(!panel.query_bar.focused);
        assert!(!panel.raw_sql_mode);

        panel.selected_metric = Some(metric("cpu_usage", MetricType::Gauge));
        panel.focus = Focus::QueryBar;
        panel.query_bar.focused = true;
        panel.raw_sql_mode = true;
        panel.handle_key_event(press(KeyCode::Esc));
        assert_eq!(panel.focus, Focus::DataTable);
    }

    #[test]
    fn enter_in_query_bar_queues_query() {
        let mut panel = MetricsPanel::new();
        panel.pending_query = None;
        panel.focus = Focus::QueryBar;
        panel.query_bar.focused = true;
        panel.handle_key_event(press(KeyCode::Enter));
        assert!(panel.pending_query.is_some());
    }

    #[test]
    fn enter_on_metric_selector_queues_metric_query() {
        let mut panel = MetricsPanel::new();
        panel.set_metric_names(vec![metric("cpu_usage", MetricType::Gauge)]);
        panel.handle_key_event(press(KeyCode::Enter));
        assert!(panel.pending_metric_query.is_some());
        assert_eq!(panel.focus, Focus::DataTable);
        assert!(panel.selected_metric.is_some());
    }

    #[test]
    fn arrow_keys_scroll_table() {
        let mut panel = MetricsPanel::new();
        panel.selected_metric = Some(metric("cpu_usage", MetricType::Gauge));
        panel.focus = Focus::DataTable;
        panel.set_data(&make_metric_batches());
        assert_eq!(panel.data_table.table_state.selected(), Some(0));

        panel.handle_key_event(press(KeyCode::Down));
        assert_eq!(panel.data_table.table_state.selected(), Some(1));

        panel.handle_key_event(press(KeyCode::Up));
        assert_eq!(panel.data_table.table_state.selected(), Some(0));
    }

    #[test]
    fn g_key_opens_group_selector() {
        let mut panel = MetricsPanel::new();
        panel.selected_metric = Some(metric("cpu_usage", MetricType::Gauge));
        panel.focus = Focus::DataTable;
        panel.handle_key_event(press(KeyCode::Char('g')));
        assert!(panel.show_group_selector);
    }

    #[test]
    fn enter_toggles_detail_in_data_table() {
        let mut panel = MetricsPanel::new();
        panel.selected_metric = Some(metric("cpu_usage", MetricType::Gauge));
        panel.focus = Focus::DataTable;
        panel.set_data(&make_metric_batches());
        assert!(!panel.show_detail);
        panel.handle_key_event(press(KeyCode::Enter));
        assert!(panel.show_detail);
    }

    #[test]
    fn r_key_cycles_detail_tabs() {
        let mut panel = MetricsPanel::new();
        panel.selected_metric = Some(metric("cpu_usage", MetricType::Gauge));
        panel.focus = Focus::DataTable;
        panel.show_detail = true;
        panel.set_data(&make_metric_batches());
        assert_eq!(
            panel.metric_detail.attribute_tab,
            MetricAttributeTab::MetricAttributes
        );
        panel.handle_key_event(press(KeyCode::Char('r')));
        assert_eq!(
            panel.metric_detail.attribute_tab,
            MetricAttributeTab::ResourceAttributes
        );
    }

    #[test]
    fn esc_from_detail_closes_it() {
        let mut panel = MetricsPanel::new();
        panel.selected_metric = Some(metric("cpu_usage", MetricType::Gauge));
        panel.focus = Focus::DataTable;
        panel.show_detail = true;
        panel.handle_key_event(press(KeyCode::Esc));
        assert!(!panel.show_detail);
    }

    #[test]
    fn f1_f2_f3_keys_are_not_handled() {
        let mut panel = MetricsPanel::new();
        assert!(panel.handle_key_event(press(KeyCode::F(1))).is_none());
        assert!(panel.handle_key_event(press(KeyCode::F(2))).is_none());
        assert!(panel.handle_key_event(press(KeyCode::F(3))).is_none());
    }

    #[test]
    fn colon_key_consumed_in_metric_selector() {
        let mut panel = MetricsPanel::new();
        let action = panel.handle_key_event(press(KeyCode::Char(':')));
        assert_eq!(action, Some(Action::None));
    }

    #[test]
    fn typing_filters_metric_list() {
        let mut panel = MetricsPanel::new();
        panel.set_metric_names(vec![
            metric("cpu_usage", MetricType::Gauge),
            metric("memory_usage", MetricType::Gauge),
        ]);
        assert_eq!(panel.metric_selector.filtered_items().len(), 2);
        panel.handle_key_event(press(KeyCode::Char('c')));
        panel.handle_key_event(press(KeyCode::Char('p')));
        assert_eq!(panel.metric_selector.filtered_items().len(), 1);
    }

    #[test]
    fn metric_detail_cycles_tabs() {
        let mut detail = MetricDetail::new();
        assert_eq!(detail.attribute_tab, MetricAttributeTab::MetricAttributes);
        detail.cycle_attribute_tab();
        assert_eq!(detail.attribute_tab, MetricAttributeTab::ResourceAttributes);
        detail.cycle_attribute_tab();
        assert_eq!(detail.attribute_tab, MetricAttributeTab::Scope);
        detail.cycle_attribute_tab();
        assert_eq!(detail.attribute_tab, MetricAttributeTab::MetricAttributes);
    }

    #[test]
    fn set_data_populates_sparkline_groups() {
        let mut panel = MetricsPanel::new();
        panel.set_data(&make_metric_batches());
        assert_eq!(panel.sparkline_data.len(), 2);
        assert!(panel.sparkline_data.contains_key("cpu_usage"));
        assert!(panel.sparkline_data.contains_key("memory_mb"));
    }

    #[test]
    fn set_error_clears_sparklines() {
        let mut panel = MetricsPanel::new();
        panel.set_data(&make_metric_batches());
        assert!(!panel.sparkline_data.is_empty());
        panel.set_error("timeout".into());
        assert!(panel.sparkline_data.is_empty());
    }

    #[test]
    fn refresh_requeues_query() {
        let mut panel = MetricsPanel::new();
        panel.pending_discovery = false;
        panel.refresh();
        assert!(panel.pending_discovery);
    }

    #[test]
    fn take_pending_clears() {
        let mut panel = MetricsPanel::new();
        assert!(panel.take_pending_discovery());
        assert!(!panel.pending_discovery);
    }

    #[test]
    fn update_refresh_action() {
        let mut panel = MetricsPanel::new();
        panel.pending_discovery = false;
        let mut state = make_state();
        panel.update(&Action::Refresh, &mut state);
        assert!(panel.pending_discovery);
    }

    #[test]
    fn unhandled_keys_in_table_bubble_up() {
        let mut panel = MetricsPanel::new();
        panel.selected_metric = Some(metric("cpu_usage", MetricType::Gauge));
        panel.focus = Focus::DataTable;
        let action = panel.handle_key_event(press(KeyCode::Char('q')));
        assert!(action.is_none());
    }

    #[test]
    fn renders_metric_selector() {
        let mut terminal = Terminal::new(TestBackend::new(100, 20)).unwrap();
        let mut panel = MetricsPanel::new();
        panel.set_metric_names(vec![metric("cpu_usage", MetricType::Gauge)]);
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "Metrics");
        assert_buffer_contains(&terminal, "cpu_usage");
    }

    #[test]
    fn renders_with_data() {
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        let mut panel = MetricsPanel::new();
        panel.selected_metric = Some(metric("cpu_usage", MetricType::Gauge));
        panel.focus = Focus::DataTable;
        panel.set_data(&make_metric_batches());
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "cpu_usage");
        assert_buffer_contains(&terminal, "memory_mb");
    }

    #[test]
    fn renders_error_state() {
        let mut terminal = Terminal::new(TestBackend::new(80, 15)).unwrap();
        let mut panel = MetricsPanel::new();
        panel.selected_metric = Some(metric("cpu_usage", MetricType::Gauge));
        panel.focus = Focus::DataTable;
        panel.set_error("table not found".into());
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        assert_buffer_contains(&terminal, "Query failed: table not found");
    }

    #[test]
    fn extract_sparkline_data_reverses_values() {
        let batches = make_metric_batches();
        let data = extract_sparkline_data(&batches);
        let cpu = &data["cpu_usage"];
        assert_eq!(cpu, &[40.0, 25.0, 10.0]);
    }

    #[test]
    fn snapshot_metrics_empty() {
        let mut terminal = Terminal::new(TestBackend::new(100, 20)).unwrap();
        let panel = MetricsPanel::new();
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("metrics_empty", content);
    }

    #[test]
    fn snapshot_metrics_with_data() {
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        let mut panel = MetricsPanel::new();
        panel.selected_metric = Some(metric("cpu_usage", MetricType::Gauge));
        panel.focus = Focus::DataTable;
        panel.set_data(&make_metric_batches());
        let state = make_state();
        terminal
            .draw(|frame| panel.render(frame, frame.area(), &state))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("metrics_with_data", content);
    }

    #[test]
    fn snapshot_sparkline_panel_only() {
        let mut terminal = Terminal::new(TestBackend::new(80, 12)).unwrap();
        let batches = make_metric_batches();
        let data = extract_sparkline_data(&batches);
        let state = make_state();
        terminal
            .draw(|frame| {
                let _ = &state;
                render_sparkline_panel(frame, frame.area(), &data);
            })
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        insta::assert_snapshot!("sparkline_panel_only", content);
    }
}
