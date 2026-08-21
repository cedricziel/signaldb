//! # The IR document shape
//!
//! ```text
//!   Document = { irVersion, from: Source, range, result, fields?, pipeline: [Stage] }
//! ```
//!
//! `from` is a **document-level field** (not a pipeline stage) that selects the
//! source and seeds the initial `RowSet`. The document tolerates unknown
//! optional top-level keys (additive forward-compatibility); strictness is
//! enforced at the stage level via `deny_unknown_fields`.

use serde::{Deserialize, Serialize};

use super::stage::Stage;

/// The declared result envelope. Validated against the inferred terminal
/// relation type. (`trace`/`scalar` arrive with their owning sibling changes.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultEnvelope {
    Rows,
    Series,
    Table,
    Heatmap,
    /// A bounded, aggregated flamegraph over matched `profiles` rows. Legal
    /// only for the `profiles` source; see `query_ir::validate`.
    Flamegraph,
    /// Introspection about the source rather than its records. Legal only for
    /// a pipeline whose terminal stage is `describe`; see `query_ir::validate`.
    Metadata,
}

impl ResultEnvelope {
    pub fn as_str(self) -> &'static str {
        match self {
            ResultEnvelope::Rows => "rows",
            ResultEnvelope::Series => "series",
            ResultEnvelope::Table => "table",
            ResultEnvelope::Heatmap => "heatmap",
            ResultEnvelope::Flamegraph => "flamegraph",
            ResultEnvelope::Metadata => "metadata",
        }
    }
}

/// The query time range. `from`/`to` are timestamp literals — RFC3339, a
/// relative anchor (`now-1h`), or integer nanoseconds. Only coercibility is
/// checked at validation; the router resolves relative anchors to one absolute
/// window at the ticket boundary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Range {
    pub from: serde_json::Value,
    pub to: serde_json::Value,
}

/// A structured, versioned query document.
///
/// The top-level struct deliberately does **not** use `deny_unknown_fields`: an
/// older stored query gains forward-compatibility with unknown optional
/// envelope-level keys. Stage objects, by contrast, are strict.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Document {
    #[serde(rename = "irVersion")]
    pub ir_version: i64,
    /// The registered source name (resolved against the source registry).
    pub from: String,
    pub range: Range,
    pub result: ResultEnvelope,
    /// Curated projection for `rows`/`table` (logical field names). When
    /// omitted, the server applies a bounded default set — never `SELECT *`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
    #[serde(default)]
    pub pipeline: Vec<Stage>,
}

impl Document {
    /// The lowest `irVersion` that can carry this document's features.
    ///
    /// A builder should declare *this* rather than the server's maximum, so a
    /// document needing nothing recent stays executable by an older server.
    ///
    /// The rule lives here because it is a property of the IR, not of whoever
    /// constructs one: `ql-ir` previously asserted "a divisor means 5" itself,
    /// which made the same fact true in three places and free to drift apart.
    /// `validate` rejects a document declaring less than this.
    pub fn minimum_ir_version(&self) -> i64 {
        use super::stage::Stage;

        let mut needed = 1;
        if self.result == ResultEnvelope::Heatmap {
            needed = needed.max(2);
        }
        if self.result == ResultEnvelope::Metadata {
            needed = needed.max(4);
        }
        for stage in &self.pipeline {
            needed = needed.max(match stage {
                Stage::Heatmap(_) => 2,
                Stage::HistogramQuantile(_) => 3,
                Stage::Describe(_) => 4,
                Stage::Aggregate(a) => a
                    .aggs
                    .iter()
                    .map(|agg| {
                        agg.func
                            .min_ir_version()
                            .max(if agg.divisor.is_some() { 5 } else { 1 })
                    })
                    .max()
                    .unwrap_or(1),
                _ => 1,
            });
        }
        needed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn document_tolerates_unknown_optional_top_level_key() {
        // Additive forward-compatibility: an unknown optional key at the
        // document level does not fail parsing.
        let doc: Document = serde_json::from_value(json!({
            "irVersion": 1,
            "from": "logs",
            "range": { "from": "now-1h", "to": "now" },
            "result": "rows",
            "pipeline": [],
            "someFutureOptionalKey": { "x": 1 }
        }))
        .expect("unknown optional top-level key is tolerated");
        assert_eq!(doc.ir_version, 1);
        assert_eq!(doc.from, "logs");
    }

    #[test]
    fn range_rejects_unknown_key() {
        assert!(
            serde_json::from_value::<Range>(json!({ "from": "now-1h", "to": "now", "x": 1 }))
                .is_err()
        );
    }
}
