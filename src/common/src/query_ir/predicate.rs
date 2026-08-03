//! # Shared predicate grammar
//!
//! One predicate grammar for every `where`: comparison leaves `{field, op,
//! value}` composed with `and`/`or`/`not`, where `field` is a logical, dotted
//! OTel-native attribute name. A leaf naming a physical column, the attribute
//! blob, or any storage detail is rejected by the validator's logical-namespace
//! guard.
//!
//! This module also carries the **denotational** evaluator ([`Predicate::evaluate`]):
//! the reference semantics, in three-valued [`Truth`], that the lowered plan
//! must satisfy. It is defined here so a query's result never depends on the
//! execution engine's own NULL handling.

use serde::{Deserialize, Serialize};

use super::value::Truth;

/// A comparison operator. Members of the versioned operator registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    #[serde(rename = "in")]
    In,
    Between,
    Contains,
    Regex,
    Exists,
}

impl ComparisonOp {
    /// Whether this operator carries a `value` operand. `exists` does not.
    pub fn takes_value(self) -> bool {
        !matches!(self, ComparisonOp::Exists)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ComparisonOp::Eq => "eq",
            ComparisonOp::Ne => "ne",
            ComparisonOp::Gt => "gt",
            ComparisonOp::Gte => "gte",
            ComparisonOp::Lt => "lt",
            ComparisonOp::Lte => "lte",
            ComparisonOp::In => "in",
            ComparisonOp::Between => "between",
            ComparisonOp::Contains => "contains",
            ComparisonOp::Regex => "regex",
            ComparisonOp::Exists => "exists",
        }
    }
}

/// A comparison leaf.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Leaf {
    pub field: String,
    pub op: ComparisonOp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
}

/// A predicate tree.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    Leaf(Leaf),
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
}

/// Wire representation used to enforce "exactly one form" plus
/// `deny_unknown_fields` (the physical-addressing guard) at parse time.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PredicateRepr {
    field: Option<String>,
    op: Option<ComparisonOp>,
    value: Option<serde_json::Value>,
    and: Option<Vec<Predicate>>,
    or: Option<Vec<Predicate>>,
    not: Option<Box<Predicate>>,
}

impl<'de> Deserialize<'de> for Predicate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let repr = PredicateRepr::deserialize(deserializer)?;
        // Exactly one of the four forms must be present.
        let forms = [
            repr.field.is_some() || repr.op.is_some() || repr.value.is_some(),
            repr.and.is_some(),
            repr.or.is_some(),
            repr.not.is_some(),
        ];
        let count = forms.iter().filter(|p| **p).count();
        if count != 1 {
            return Err(serde::de::Error::custom(
                "predicate must be exactly one of a comparison leaf, `and`, `or`, or `not`",
            ));
        }
        if let Some(and) = repr.and {
            return Ok(Predicate::And(and));
        }
        if let Some(or) = repr.or {
            return Ok(Predicate::Or(or));
        }
        if let Some(not) = repr.not {
            return Ok(Predicate::Not(not));
        }
        let field = repr
            .field
            .ok_or_else(|| serde::de::Error::custom("comparison leaf requires `field`"))?;
        let op = repr
            .op
            .ok_or_else(|| serde::de::Error::custom("comparison leaf requires `op`"))?;
        Ok(Predicate::Leaf(Leaf {
            field,
            op,
            value: repr.value,
        }))
    }
}

impl Serialize for Predicate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        match self {
            Predicate::Leaf(leaf) => leaf.serialize(serializer),
            Predicate::And(preds) => {
                let mut m = serializer.serialize_map(Some(1))?;
                m.serialize_entry("and", preds)?;
                m.end()
            }
            Predicate::Or(preds) => {
                let mut m = serializer.serialize_map(Some(1))?;
                m.serialize_entry("or", preds)?;
                m.end()
            }
            Predicate::Not(pred) => {
                let mut m = serializer.serialize_map(Some(1))?;
                m.serialize_entry("not", pred)?;
                m.end()
            }
        }
    }
}

impl Predicate {
    /// Visit every comparison leaf in the tree.
    pub fn walk_leaves<'a>(&'a self, f: &mut impl FnMut(&'a Leaf)) {
        match self {
            Predicate::Leaf(leaf) => f(leaf),
            Predicate::And(preds) | Predicate::Or(preds) => {
                for p in preds {
                    p.walk_leaves(f);
                }
            }
            Predicate::Not(p) => p.walk_leaves(f),
        }
    }

    /// Evaluate the predicate against a record — the **denotational** reference
    /// semantics in three-valued [`Truth`]. A field missing from `record`, or
    /// present with a JSON `null`, is *absent*.
    pub fn evaluate(&self, record: &Record) -> Truth {
        match self {
            Predicate::Not(p) => !p.evaluate(record),
            Predicate::And(preds) => preds
                .iter()
                .map(|p| p.evaluate(record))
                .fold(Truth::True, Truth::and),
            Predicate::Or(preds) => preds
                .iter()
                .map(|p| p.evaluate(record))
                .fold(Truth::False, Truth::or),
            Predicate::Leaf(leaf) => eval_leaf(leaf, record),
        }
    }
}

/// A record for the reference evaluator: field name → value, where a missing
/// entry (or a JSON `null`) denotes an absent field.
pub type Record = std::collections::HashMap<String, serde_json::Value>;

fn eval_leaf(leaf: &Leaf, record: &Record) -> Truth {
    let present = record.get(&leaf.field).filter(|v| !v.is_null()).cloned();

    // `exists` is total: it observes absence, never returns Absent.
    if leaf.op == ComparisonOp::Exists {
        return Truth::from_bool(present.is_some());
    }

    let Some(actual) = present else {
        // Every comparison against an absent field is Absent (propagates).
        return Truth::Absent;
    };
    let Some(expected) = &leaf.value else {
        return Truth::Absent;
    };

    match leaf.op {
        ComparisonOp::Eq => Truth::from_bool(json_eq(&actual, expected)),
        ComparisonOp::Ne => Truth::from_bool(!json_eq(&actual, expected)),
        ComparisonOp::Gt => cmp_truth(&actual, expected, |o| o == std::cmp::Ordering::Greater),
        ComparisonOp::Gte => cmp_truth(&actual, expected, |o| o != std::cmp::Ordering::Less),
        ComparisonOp::Lt => cmp_truth(&actual, expected, |o| o == std::cmp::Ordering::Less),
        ComparisonOp::Lte => cmp_truth(&actual, expected, |o| o != std::cmp::Ordering::Greater),
        ComparisonOp::Contains => match (&actual, expected) {
            (serde_json::Value::String(h), serde_json::Value::String(n)) => {
                Truth::from_bool(h.contains(n.as_str()))
            }
            _ => Truth::False,
        },
        ComparisonOp::In => match expected {
            serde_json::Value::Array(items) => {
                Truth::from_bool(items.iter().any(|i| json_eq(&actual, i)))
            }
            _ => Truth::False,
        },
        ComparisonOp::Between => match expected {
            serde_json::Value::Array(bounds) if bounds.len() == 2 => {
                let lo = cmp_json(&actual, &bounds[0]).map(|o| o != std::cmp::Ordering::Less);
                let hi = cmp_json(&actual, &bounds[1]).map(|o| o != std::cmp::Ordering::Greater);
                match (lo, hi) {
                    (Some(a), Some(b)) => Truth::from_bool(a && b),
                    _ => Truth::False,
                }
            }
            _ => Truth::False,
        },
        ComparisonOp::Regex => match (&actual, expected) {
            (serde_json::Value::String(_h), serde_json::Value::String(_p)) => {
                // The reference evaluator does not run regexes (the engine does,
                // behind the bounded matcher). Left as False for the reference;
                // real matching happens in the lowered plan.
                Truth::False
            }
            _ => Truth::False,
        },
        ComparisonOp::Exists => unreachable!("handled above"),
    }
}

fn json_eq(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    match (a, b) {
        (serde_json::Value::Number(x), serde_json::Value::Number(y)) => {
            match (x.as_f64(), y.as_f64()) {
                (Some(xf), Some(yf)) => xf == yf,
                _ => x == y,
            }
        }
        _ => a == b,
    }
}

fn cmp_json(a: &serde_json::Value, b: &serde_json::Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (serde_json::Value::Number(x), serde_json::Value::Number(y)) => {
            x.as_f64()?.partial_cmp(&y.as_f64()?)
        }
        (serde_json::Value::String(x), serde_json::Value::String(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

fn cmp_truth(
    a: &serde_json::Value,
    b: &serde_json::Value,
    pred: impl Fn(std::cmp::Ordering) -> bool,
) -> Truth {
    match cmp_json(a, b) {
        Some(o) => Truth::from_bool(pred(o)),
        None => Truth::False,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn record(pairs: &[(&str, serde_json::Value)]) -> Record {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    // Task 2.1 — nested and/or/not with all operators parses.
    #[test]
    fn nested_predicate_with_all_ops_parses() {
        let doc = json!({
            "and": [
                { "field": "service.name", "op": "eq", "value": "checkout" },
                { "or": [
                    { "field": "http.status_code", "op": "gte", "value": 500 },
                    { "not": { "field": "deployment.environment", "op": "exists" } }
                ]},
                { "field": "duration", "op": "between", "value": [1, 10] },
                { "field": "path", "op": "in", "value": ["/a", "/b"] },
                { "field": "body", "op": "contains", "value": "error" },
                { "field": "name", "op": "regex", "value": "^GET" }
            ]
        });
        let p: Predicate = serde_json::from_value(doc).expect("parses");
        // Round-trips.
        let _ = serde_json::to_value(&p).unwrap();
        assert!(matches!(p, Predicate::And(_)));
    }

    #[test]
    fn mixed_form_predicate_is_rejected() {
        let doc = json!({ "and": [], "field": "x", "op": "eq", "value": 1 });
        assert!(serde_json::from_value::<Predicate>(doc).is_err());
    }

    // Task 1.2 (denotational) — absent-value semantics via the evaluator.
    #[test]
    fn eq_and_negated_eq_both_exclude_absent_rows() {
        let p_eq: Predicate =
            serde_json::from_value(json!({ "field": "env", "op": "eq", "value": "prod" })).unwrap();
        let p_neq = Predicate::Not(Box::new(p_eq.clone()));
        let absent = record(&[("other", json!(1))]);
        assert_eq!(p_eq.evaluate(&absent), Truth::Absent);
        assert_eq!(p_neq.evaluate(&absent), Truth::Absent);
        assert!(!p_eq.evaluate(&absent).matches());
        assert!(!p_neq.evaluate(&absent).matches());
    }

    #[test]
    fn exists_observes_absence() {
        let exists: Predicate =
            serde_json::from_value(json!({ "field": "env", "op": "exists" })).unwrap();
        let not_exists = Predicate::Not(Box::new(exists.clone()));
        let absent = record(&[("other", json!(1))]);
        let present = record(&[("env", json!("prod"))]);
        assert_eq!(exists.evaluate(&absent), Truth::False);
        assert_eq!(not_exists.evaluate(&absent), Truth::True);
        assert!(not_exists.evaluate(&absent).matches()); // matches absent rows
        assert_eq!(exists.evaluate(&present), Truth::True);
        assert!(exists.evaluate(&present).matches());
    }

    #[test]
    fn null_value_is_treated_as_absent() {
        let p: Predicate =
            serde_json::from_value(json!({ "field": "env", "op": "eq", "value": "prod" })).unwrap();
        let null_row = record(&[("env", json!(null))]);
        assert_eq!(p.evaluate(&null_row), Truth::Absent);
    }
}
