//! PromQL lowered onto the query IR's series algebra (`irVersion` 10).
//!
//! Table-driven: each case is a PromQL expression and the pipeline it must
//! lower to, and every lowered document must also pass `query_ir::validate`
//! against the `metrics` source. What the IR cannot express is an
//! `Inexpressible` error naming the construct.

use ql_ir::{LowerError, PromqlParams};
use query_ir::{FieldResolver, Resolved, SourceRegistry, ValueType};
use serde_json::{Value, json};

const START: i64 = 1_700_000_000_000_000_000;
const END: i64 = START + 3_600_000_000_000;
const STEP: i64 = 60_000_000_000;

/// Resolves any field of `metrics`: the identity fields as columns, every
/// other label as an untyped attribute, as the production resolver does for a
/// label it has no declaration for.
struct Permissive;

impl FieldResolver for Permissive {
    fn resolve(&self, source: &str, field: &str) -> Option<Resolved> {
        let value_type = ValueType::String;
        (source == "metrics").then(|| match field {
            "metric.name" | "service.name" => Resolved::Column {
                name: field.replace('.', "_"),
                value_type,
            },
            _ => Resolved::JsonPath {
                container: "attributes".to_string(),
                key: field.to_string(),
                value_type,
            },
        })
    }
}

/// Lower, validate, and return the document as JSON.
fn lower_with(q: &str, params: &PromqlParams) -> Value {
    let doc = ql_ir::promql_to_ir(q, params).unwrap_or_else(|e| panic!("{q} should lower: {e}"));
    if let Err(e) = query_ir::validate(&doc, &SourceRegistry::core(), &Permissive) {
        panic!("{q} lowered to an invalid document: {e}\n{doc:#?}");
    }
    serde_json::to_value(&doc).expect("a document serializes")
}

fn lower(q: &str) -> Value {
    lower_with(q, &PromqlParams::range(START, END, STEP))
}

fn inexpressible(q: &str) -> String {
    match ql_ir::promql_to_ir(q, &PromqlParams::range(START, END, STEP)) {
        Err(LowerError::Inexpressible(msg)) => msg,
        other => panic!("{q}: expected Inexpressible, got {other:?}"),
    }
}

fn cases(table: &[(&str, Value)]) {
    for (q, expected) in table {
        assert_eq!(&lower(q)["pipeline"], expected, "{q}");
    }
}

fn leaf(field: &str, op: &str, value: &str) -> Value {
    json!({ "field": field, "op": op, "value": value })
}

fn absent(field: &str) -> Value {
    json!({ "not": { "field": field, "op": "exists" } })
}

fn name(m: &str) -> Value {
    json!({ "where": leaf("metric.name", "eq", m) })
}

fn latest() -> Value {
    json!({ "sample": { "fn": "latest", "of": "metric.value", "lookback": "5m" } })
}

/// `up{…}` with one extra matcher predicate.
fn up_and(pred: Value) -> Value {
    json!([{ "where": { "and": [leaf("metric.name", "eq", "up"), pred] } }, latest()])
}

#[test]
fn a_range_query_document() {
    assert_eq!(
        lower("up"),
        json!({
            "irVersion": 10, "from": "metrics", "range": { "from": START, "to": END },
            "result": "series", "step": "1m", "pipeline": [name("up"), latest()],
        })
    );
}

/// Matchers keep Prometheus's absent-is-empty semantics (a matcher `""`
/// satisfies also matches a missing label); label names map here only.
#[test]
fn selectors() {
    let or = |a: Value, b: Value| json!({ "or": [a, b] });
    let not = |a: Value| json!({ "not": a });
    cases(&[
        (
            r#"up{job="api"}"#,
            up_and(leaf("service.name", "eq", "api")),
        ),
        (
            r#"{__name__="up", service_name!="api"}"#,
            up_and(or(
                leaf("service.name", "ne", "api"),
                absent("service.name"),
            )),
        ),
        (r#"up{r=""}"#, up_and(or(leaf("r", "eq", ""), absent("r")))),
        (r#"up{r!=""}"#, up_and(leaf("r", "ne", ""))),
        // Regexes are fully anchored, as the parser compiled them.
        (
            r#"up{service=~"api-.*"}"#,
            up_and(leaf("service.name", "regex", "^(?s:api-.*)$")),
        ),
        (
            r#"up{r=~"eu|"}"#,
            up_and(or(leaf("r", "regex", "^(?s:eu|)$"), absent("r"))),
        ),
        (
            r#"up{r!~"eu|"}"#,
            up_and(not(leaf("r", "regex", "^(?s:eu|)$"))),
        ),
        (
            r#"up{r!~"eu"}"#,
            up_and(or(not(leaf("r", "regex", "^(?s:eu)$")), absent("r"))),
        ),
        (
            r#"{__name__=~"http_.*"}"#,
            json!([{ "where": leaf("metric.name", "regex", "^(?s:http_.*)$") }, latest()]),
        ),
    ]);
}

#[test]
fn constructs_the_ir_cannot_express_are_named() {
    for (q, needle) in [("x[5m]", "range vector"), (r#""text""#, "string literal")] {
        let msg = inexpressible(q);
        assert!(msg.contains(needle), "{q}: {msg}");
    }
}
