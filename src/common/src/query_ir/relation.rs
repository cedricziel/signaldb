//! # Relation types — what flows between IR stages
//!
//! Every stage consumes and produces a typed **relation**. Legality and
//! result-envelope validation are functions of [`RelationType`], not prose:
//! the validator infers the relation type stage-by-stage and rejects a stage
//! whose input constraint is unmet.

use serde::{Deserialize, Serialize};

use super::stage::DescribeTarget;
use super::value::ValueType;

/// What one row of a [`RowSet`] denotes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Grain {
    /// A single log event.
    Event,
    /// A single span.
    Span,
    /// A whole trace.
    Trace,
    /// One group produced by an aggregate.
    Group,
}

/// A named, typed column in an inferred relation schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub value_type: ValueType,
}

impl Column {
    pub fn new(name: impl Into<String>, value_type: ValueType) -> Self {
        Column {
            name: name.into(),
            value_type,
        }
    }
}

/// A row-shaped relation.
///
/// `aggregated` is the discriminator that makes the two RowSet-terminal
/// envelopes distinguishable: a raw scan/filter yields `aggregated=false` →
/// `rows`; a grouped `aggregate` (no `step`) yields `aggregated=true` → `table`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowSet {
    /// The registered source this relation ultimately derives from.
    pub source: String,
    /// The explicitly-referenceable columns (derived/extracted columns, and —
    /// once the schema is closed — the exact aggregate/group outputs).
    pub columns: Vec<Column>,
    /// What one row denotes.
    pub grain: Grain,
    /// `true` once a non-`step` aggregate has grouped the rows.
    pub aggregated: bool,
    /// While `open`, a reference may resolve to any registry field of `source`
    /// in addition to `columns`. An `aggregate` closes the schema so only
    /// `columns` are referenceable thereafter.
    pub open: bool,
}

/// A time-series relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Series {
    /// The grouping labels (the aggregate's `by` fields).
    pub labels: Vec<String>,
    /// The value type of the single series value (the aggregate output).
    pub value: ValueType,
    /// The bucket width in nanoseconds.
    pub step_ns: i64,
}

/// A sparse two-dimensional count relation with declared axes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Heatmap {
    pub x_step_ns: i64,
    pub y_of: String,
    pub y_type: ValueType,
    pub y_bounds: Vec<i64>,
    pub value: String,
}

/// An introspection relation: what a terminal `describe` stage produces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metadata {
    /// What the stage introspects.
    pub target: DescribeTarget,
}

/// The type of a relation flowing between stages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationType {
    RowSet(RowSet),
    Series(Series),
    Heatmap(Heatmap),
    /// Introspection about a source rather than its records: the relation a
    /// terminal `describe` stage produces. It carries no columns — the answer
    /// is assembled from declared schema, schema registries and maintained
    /// statistics, not from a scan.
    Metadata(Metadata),
}

impl RelationType {
    /// A short human label for error messages.
    pub fn describe(&self) -> String {
        match self {
            RelationType::RowSet(rs) if rs.aggregated => "table (aggregated row-set)".to_string(),
            RelationType::RowSet(_) => "rows (row-set)".to_string(),
            RelationType::Series(_) => "series".to_string(),
            RelationType::Heatmap(_) => "heatmap".to_string(),
            RelationType::Metadata(m) => format!("metadata ({})", m.target.as_str()),
        }
    }

    /// Look up a named column in the currently-referenceable schema.
    pub fn column(&self, name: &str) -> Option<&Column> {
        match self {
            RelationType::RowSet(rs) => rs.columns.iter().find(|c| c.name == name),
            RelationType::Series(_) => None,
            RelationType::Heatmap(_) => None,
            RelationType::Metadata(_) => None,
        }
    }
}
