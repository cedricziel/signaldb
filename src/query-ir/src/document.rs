//! # The IR document shape
//!
//! ```text
//!   Document = { irVersion, from: Source, range, result, fields?, pipeline: [Stage], page?, tail? }
//! ```
//!
//! `from` is a **document-level field** (not a pipeline stage) that selects the
//! source and seeds the initial `RowSet`. The document tolerates unknown
//! optional top-level keys (additive forward-compatibility); strictness is
//! enforced at the stage level via `deny_unknown_fields`.

use serde::{Deserialize, Serialize};

use super::page::{Page, Tail};
use super::stage::Stage;

/// The declared result envelope. Validated against the inferred terminal
/// relation type.
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
    /// A service dependency graph (nodes and edges) over `traces`. Legal only
    /// for the `traces` source at IR version 8 or later; see
    /// `query_ir::validate`.
    Graph,
    /// One value per evaluation instant, no labels: a terminal `Scalar`
    /// relation (`irVersion` 10).
    Scalar,
    /// Rows of a `traces` source grouped per trace: `traceId` plus its
    /// spans (`irVersion` 12); see `query_ir::validate`.
    Trace,
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
            ResultEnvelope::Graph => "graph",
            ResultEnvelope::Scalar => "scalar",
            ResultEnvelope::Trace => "trace",
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
    /// `graph` scoping: restrict to the neighbourhood of this service (see
    /// `depth`). Legal only with `result: graph`; mutually exclusive with
    /// `trace_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub focus: Option<String>,
    /// `graph` scoping: hop count from `focus`, 1 to 3, default 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth: Option<i64>,
    /// `graph` scoping: restrict to the services and calls in this trace.
    /// Mutually exclusive with `focus`/`depth`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    /// The default evaluation step of the series-algebra stages; required by
    /// the `time`/`constant` pseudo-sources (`irVersion` 10).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,
    /// The value of the `constant` pseudo-source, and legal only there
    /// (`irVersion` 10).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub constant: Option<f64>,
    /// Walk a `rows`/`trace` result in pages (`irVersion` 14).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page: Option<Page>,
    /// Follow a `rows`/`trace` result forward in time (`irVersion` 15).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail: Option<Tail>,
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
        use super::version::{Feature, OperatorRegistry};

        let mut needed = 1;
        if self.result == ResultEnvelope::Heatmap {
            needed = needed.max(OperatorRegistry::feature_min_version(Feature::Heatmap));
        }
        if self.result == ResultEnvelope::Metadata {
            needed = needed.max(OperatorRegistry::feature_min_version(Feature::Describe));
        }
        if self.result == ResultEnvelope::Scalar {
            needed = needed.max(OperatorRegistry::feature_min_version(
                Feature::ScalarEnvelope,
            ));
        }
        if self.result == ResultEnvelope::Trace {
            needed = needed.max(OperatorRegistry::feature_min_version(
                Feature::TraceEnvelope,
            ));
        }
        if self.step.is_some() || self.constant.is_some() {
            needed = needed.max(OperatorRegistry::feature_min_version(Feature::DocumentStep));
        }
        if self.page.is_some() {
            needed = needed.max(OperatorRegistry::feature_min_version(Feature::Page));
        }
        if self.tail.is_some() {
            needed = needed.max(OperatorRegistry::feature_min_version(Feature::Tail));
        }
        if super::source::is_pseudo_source(&self.from) {
            needed = needed.max(OperatorRegistry::feature_min_version(Feature::PseudoSource));
        }
        for stage in &self.pipeline {
            needed = needed.max(match stage {
                Stage::HistogramQuantile(hq) if hq.window.is_some() => {
                    OperatorRegistry::feature_min_version(Feature::HistogramWindow)
                }
                Stage::HistogramQuantile(hq) if hq.per_series => {
                    OperatorRegistry::feature_min_version(Feature::HistogramPerSeries)
                }
                Stage::Aggregate(a) => a
                    .aggs
                    .iter()
                    .map(|agg| {
                        let mut agg_needed = OperatorRegistry::agg_min_version(agg.func);
                        if agg.divisor.is_some() {
                            agg_needed = agg_needed.max(OperatorRegistry::feature_min_version(
                                Feature::AggregateDivisor,
                            ));
                        }
                        if agg.across.is_some() {
                            agg_needed = agg_needed.max(OperatorRegistry::feature_min_version(
                                Feature::AggregateAcross,
                            ));
                        }
                        if agg.window.is_some() {
                            agg_needed = agg_needed.max(OperatorRegistry::feature_min_version(
                                Feature::AggregateWindow,
                            ));
                        }
                        agg_needed
                    })
                    .max()
                    .unwrap_or(1),
                other => other
                    .feature()
                    .map_or(1, OperatorRegistry::feature_min_version),
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
