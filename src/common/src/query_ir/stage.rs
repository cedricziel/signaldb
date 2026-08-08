//! # Pipeline stages and structured operands
//!
//! Every stage operand is a **structured value**, never an embedded
//! mini-expression string. Aggregate outputs are uniquely named (`as`) and are
//! the only thing an `AggRef`, `order`, or `topk`/`bottomk` may reference.

use serde::{Deserialize, Serialize};

use super::predicate::Predicate;
use super::value::ValueType;

/// An aggregate function. Member of the versioned function registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggFn {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Quantile,
}

impl AggFn {
    /// Whether the function needs an `of` field operand. `count` does not.
    pub fn needs_field(self) -> bool {
        !matches!(self, AggFn::Count)
    }

    /// Whether the function needs a numeric `arg` (e.g. the quantile).
    pub fn needs_arg(self) -> bool {
        matches!(self, AggFn::Quantile)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            AggFn::Count => "count",
            AggFn::Sum => "sum",
            AggFn::Avg => "avg",
            AggFn::Min => "min",
            AggFn::Max => "max",
            AggFn::Quantile => "quantile",
        }
    }
}

/// A single named aggregate output.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Agg {
    #[serde(rename = "fn")]
    pub func: AggFn,
    /// The field being aggregated (a logical name). Omitted for `count`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub of: Option<String>,
    /// A numeric argument (e.g. the quantile in `[0,1]`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arg: Option<f64>,
    /// The output column name — the only thing later stages may reference.
    #[serde(rename = "as")]
    pub as_name: String,
    /// An optional predicate scoping which records this aggregate consumes,
    /// so one grouped query can report a total and a subset measure over the
    /// same groups (`count` of everything beside `count` of just the errors).
    ///
    /// It is the same grammar, resolver and evaluation semantics as the `where`
    /// stage — deliberately the shared [`Predicate`], not a second grammar —
    /// and it narrows only this aggregate. Grouping happens once regardless.
    #[serde(rename = "where", default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<Predicate>,
}

/// The `aggregate` stage: group-reduce, optionally time-bucketed by `step`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Aggregate {
    /// Grouping fields (logical names).
    #[serde(default)]
    pub by: Vec<String>,
    /// The named aggregate outputs.
    pub aggs: Vec<Agg>,
    /// A time-bucket width (`"1m"`). Present → the result is a `series`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,
}

/// A sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Asc,
    Desc,
}

/// An `order` key: a field or aggregate name plus a direction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Order {
    /// A `FieldRef` or `AggRef` (a name), never an expression string.
    pub of: String,
    pub dir: Direction,
}

/// A `topk`/`bottomk` rank stage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rank {
    /// Must be an integer `> 0` (validated).
    pub n: i64,
    /// The `AggRef` or `FieldRef` to rank by (a name).
    pub of: String,
}

/// A parser for the `extract` stage. `regex` is deferred (registry-gated).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Parser {
    Json,
    Logfmt,
}

/// A field derived by an `extract` stage, with its declared type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DerivedField {
    pub name: String,
    #[serde(rename = "type")]
    pub value_type: ValueType,
}

/// The `extract` stage: derive typed, query-local fields from log content.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Extract {
    pub parser: Parser,
    #[serde(rename = "as")]
    pub as_fields: Vec<DerivedField>,
}

/// A transform stage in the pipeline. Externally tagged: a single-key object
/// whose key names the stage. An unknown key is an unsupported stage and is
/// rejected by name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    Where(Predicate),
    Extract(Extract),
    Aggregate(Aggregate),
    Topk(Rank),
    Bottomk(Rank),
    Order(Vec<Order>),
    Limit(u64),
}

impl Stage {
    /// The stage's name, for error messages.
    pub fn name(&self) -> &'static str {
        match self {
            Stage::Where(_) => "where",
            Stage::Extract(_) => "extract",
            Stage::Aggregate(_) => "aggregate",
            Stage::Topk(_) => "topk",
            Stage::Bottomk(_) => "bottomk",
            Stage::Order(_) => "order",
            Stage::Limit(_) => "limit",
        }
    }
}

/// Whether a reference name is actually an embedded expression string rather
/// than a structured operand (e.g. `"max(duration)"`). Used to reject the
/// pre-restructure `topk:{of:"max(duration)"}` form.
pub fn is_expression_string(name: &str) -> bool {
    name.chars()
        .any(|c| matches!(c, '(' | ')' | '+' | '-' | '*' | '/' | ' ' | ','))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_ir::{ComparisonOp, Leaf};
    use serde_json::json;

    #[test]
    fn aggregate_with_step_and_named_output_parses() {
        let s: Stage = serde_json::from_value(json!({
            "aggregate": { "by": ["service.name"], "aggs": [{ "fn": "count", "as": "n" }], "step": "1m" }
        }))
        .unwrap();
        assert!(matches!(s, Stage::Aggregate(_)));
    }

    #[test]
    fn unknown_stage_key_is_rejected_by_name() {
        let err = serde_json::from_value::<Stage>(json!({ "correlate": {} })).unwrap_err();
        assert!(err.to_string().contains("correlate"), "got: {err}");
    }

    #[test]
    fn unknown_inner_key_is_rejected() {
        assert!(
            serde_json::from_value::<Stage>(json!({
                "aggregate": { "aggs": [{ "fn": "count", "as": "n" }], "bogus": 1 }
            }))
            .is_err()
        );
    }

    #[test]
    fn expression_string_operand_is_detected() {
        assert!(is_expression_string("max(duration)"));
        assert!(!is_expression_string("max_dur"));
    }

    #[test]
    fn aggregate_scope_parses_into_a_predicate() {
        let s: Stage = serde_json::from_value(json!({
            "aggregate": { "by": ["span.name"], "aggs": [
                { "fn": "count", "as": "n" },
                { "fn": "count", "as": "errors",
                  "where": { "field": "status.code", "op": "eq", "value": "Error" } }
            ] }
        }))
        .unwrap();
        let Stage::Aggregate(agg) = s else {
            panic!("expected an aggregate stage");
        };
        assert_eq!(agg.aggs[0].scope, None, "unscoped aggregate keeps no scope");
        let scope = agg.aggs[1].scope.as_ref().expect("scope parses");
        assert!(matches!(scope, Predicate::Leaf(l) if l.field == "status.code"));
    }

    #[test]
    fn aggregate_scope_round_trips_and_is_omitted_when_absent() {
        let scoped = Agg {
            func: AggFn::Count,
            of: None,
            arg: None,
            as_name: "errors".to_string(),
            scope: Some(Predicate::Leaf(Leaf {
                field: "status.code".to_string(),
                op: ComparisonOp::Eq,
                value: Some(json!("Error")),
            })),
        };
        let encoded = serde_json::to_value(&scoped).unwrap();
        assert!(encoded.get("where").is_some());
        assert_eq!(
            serde_json::from_value::<Agg>(encoded).unwrap(),
            scoped,
            "a scoped aggregate round-trips"
        );

        let unscoped = Agg {
            scope: None,
            ..scoped
        };
        let encoded = serde_json::to_value(&unscoped).unwrap();
        assert!(
            encoded.get("where").is_none(),
            "an unscoped aggregate emits no `where` key: {encoded}"
        );
    }

    #[test]
    fn unknown_key_on_an_aggregate_is_still_rejected() {
        assert!(
            serde_json::from_value::<Agg>(json!({
                "fn": "count", "as": "n", "filter": { "field": "x", "op": "exists" }
            }))
            .is_err(),
            "`deny_unknown_fields` still guards the aggregate"
        );
    }

    #[test]
    fn a_scope_may_compose_with_and_or_not() {
        let agg: Agg = serde_json::from_value(json!({
            "fn": "count", "as": "n",
            "where": { "and": [
                { "field": "status.code", "op": "eq", "value": "Error" },
                { "not": { "field": "http.route", "op": "exists" } }
            ] }
        }))
        .unwrap();
        assert!(matches!(agg.scope, Some(Predicate::And(ref v)) if v.len() == 2));
    }

    #[test]
    fn topk_parses_as_structured_operand() {
        let s: Stage =
            serde_json::from_value(json!({ "topk": { "n": 10, "of": "max_dur" } })).unwrap();
        match s {
            Stage::Topk(r) => {
                assert_eq!(r.n, 10);
                assert_eq!(r.of, "max_dur");
            }
            _ => panic!("expected topk"),
        }
    }
}
