//! PromQL lowered onto the query IR's series algebra (`irVersion` 10).
//!
//! Table-driven: each case is a PromQL expression and the pipeline it must
//! lower to, and every lowered document must also pass `query_ir::validate`
//! against the `metrics` source. What the IR cannot express is an
//! `Inexpressible` error naming the construct.

use ql_ir::{LowerError, PromqlParams};
use query_ir::{FieldResolver, RelationType, Resolved, SourceRegistry, ValueType};
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

    /// Only the declared fields are known, so an output name may not collide
    /// with an attribute the permissive fallback merely resolves.
    fn is_known(&self, source: &str, field: &str) -> bool {
        source == "metrics" && matches!(field, "metric.name" | "service.name")
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

/// The labels the validator infers for the lowered document: the known
/// ones, and whether the set is open.
fn labels(q: &str) -> (Vec<String>, bool) {
    let doc = ql_ir::promql_to_ir(q, &PromqlParams::range(START, END, STEP)).expect("lowers");
    match query_ir::validate(&doc, &SourceRegistry::core(), &Permissive).map(|v| v.terminal) {
        Ok(RelationType::Series(s)) => (s.labels, s.open_labels),
        other => panic!("{q}: expected a series, got {other:?}"),
    }
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

fn ranged(f: &str, window: &str) -> Value {
    json!({ "sample": { "fn": f, "of": "metric.value", "window": window } })
}

/// `stages` appended to `[name(m), sample]`.
fn after(m: &str, sample: Value, stages: &[Value]) -> Value {
    let mut p = vec![name(m), sample];
    p.extend_from_slice(stages);
    Value::Array(p)
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
    // An instant selector keeps each series' labels, the name included.
    assert_eq!(labels("up"), (vec!["metric.name".to_string()], true));
}

/// An instant query evaluates once: `start = end = t`, any positive step.
#[test]
fn an_instant_query_document() {
    let doc = lower_with("up", &PromqlParams::instant(END));
    assert_eq!(doc["range"], json!({ "from": END, "to": END }));
    assert_eq!(doc["step"], json!("1s"));
    let explicit = PromqlParams {
        step_ns: 15_000_000_000,
        ..PromqlParams::instant(END)
    };
    assert_eq!(lower_with("up", &explicit)["step"], json!("15s"));
}

#[test]
fn bad_parameters_and_text_are_invalid_promql() {
    for (q, params) in [
        ("up", PromqlParams::range(START, END, 0)),
        ("up", PromqlParams::range(END, START, STEP)),
        ("sum(", PromqlParams::range(START, END, STEP)),
        ("topk(NaN, x)", PromqlParams::range(START, END, STEP)),
        ("bottomk(Inf, x)", PromqlParams::range(START, END, STEP)),
        ("topk(-Inf, x)", PromqlParams::range(START, END, STEP)),
    ] {
        let result = ql_ir::promql_to_ir(q, &params);
        assert!(matches!(result, Err(LowerError::InvalidPromql(_))), "{q}");
    }
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
        (
            r#"{"signaldb.wal.entries_pending", "k8s.pod.name"="p"}"#,
            json!([{ "where": { "and": [
                leaf("metric.name", "eq", "signaldb.wal.entries_pending"),
                leaf("k8s.pod.name", "eq", "p")
            ] } }, latest()]),
        ),
    ]);
}

/// `{a or b}` is a disjunction of matcher groups, under the metric name.
#[test]
fn or_selectors() {
    cases(&[(
        r#"up{a="1", b="2" or c="3"}"#,
        up_and(json!({ "or": [
            { "and": [leaf("a", "eq", "1"), leaf("b", "eq", "2")] },
            leaf("c", "eq", "3")
        ] })),
    )]);
}

#[test]
fn offset_and_at_modifiers() {
    let up = |extra: Value| {
        let mut s = latest();
        s["sample"]
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        json!([name("up"), s])
    };
    cases(&[
        ("up offset 5m", up(json!({ "offset": "5m" }))),
        ("up @ 1700000000", up(json!({ "at": START }))),
        (
            "up @ 1700000000.5",
            up(json!({ "at": START + 500_000_000 })),
        ),
        ("up @ start()", up(json!({ "at": START }))),
        (
            "up @ end() offset 1h",
            up(json!({ "offset": "1h", "at": END })),
        ),
        ("rate(up[5m] offset 1d)", {
            let mut s = ranged("rate", "5m");
            s["sample"]["offset"] = json!("24h");
            json!([name("up"), s])
        }),
    ]);
    assert!(inexpressible("up offset -5m").contains("negative offset"));
    let far = ql_ir::promql_to_ir("up @ 1e12", &PromqlParams::range(START, END, STEP));
    assert!(matches!(far, Err(LowerError::InvalidPromql(_))), "{far:?}");
}

#[test]
fn range_functions_sample_the_window() {
    for f in [
        "rate",
        "increase",
        "irate",
        "delta",
        "idelta",
        "deriv",
        "resets",
        "changes",
        "avg_over_time",
        "min_over_time",
        "max_over_time",
        "sum_over_time",
        "count_over_time",
        "last_over_time",
        "stddev_over_time",
        "stdvar_over_time",
        "present_over_time",
    ] {
        let q = format!("{f}(x[90s])");
        assert_eq!(
            lower(&q)["pipeline"],
            after("x", ranged(f, "90s"), &[]),
            "{q}"
        );
    }
    let mut quantile = ranged("quantile_over_time", "1h");
    quantile["sample"]["arg"] = json!(0.9);
    cases(&[
        ("quantile_over_time(0.9, x[1h])", after("x", quantile, &[])),
        ("rate(x[1500ms])", after("x", ranged("rate", "1500ms"), &[])),
    ]);
}

#[test]
fn aggregations_reduce() {
    let rate = |reduce: Value| after("x", ranged("rate", "5m"), &[json!({ "reduce": reduce })]);
    cases(&[
        ("sum(rate(x[5m]))", rate(json!({ "fn": "sum" }))),
        (
            "sum by (job, region) (rate(x[5m]))",
            rate(json!({ "fn": "sum", "by": ["service.name", "region"] })),
        ),
        (
            "avg by () (rate(x[5m]))",
            rate(json!({ "fn": "avg", "by": [] })),
        ),
        // Prometheus drops the metric name under `without`.
        (
            "max without (pod) (rate(x[5m]))",
            rate(json!({ "fn": "max", "without": ["pod"] })),
        ),
        (
            "min without (__name__) (rate(x[5m]))",
            rate(json!({ "fn": "min", "without": ["metric.name"] })),
        ),
        ("count(rate(x[5m]))", rate(json!({ "fn": "count" }))),
        ("group(rate(x[5m]))", rate(json!({ "fn": "group" }))),
        ("stddev(rate(x[5m]))", rate(json!({ "fn": "stddev" }))),
        ("stdvar(rate(x[5m]))", rate(json!({ "fn": "stdvar" }))),
        (
            "quantile by (job) (0.99, rate(x[5m]))",
            rate(json!({ "fn": "quantile", "by": ["service.name"], "arg": 0.99 })),
        ),
        (
            "topk(3, rate(x[5m]))",
            rate(json!({ "fn": "topk", "arg": 3.0 })),
        ),
        (
            "bottomk by (job) (2, rate(x[5m]))",
            rate(json!({ "fn": "bottomk", "by": ["service.name"], "arg": 2.0 })),
        ),
        (
            r#"count_values("v", rate(x[5m]))"#,
            rate(json!({ "fn": "count_values", "label": "v" })),
        ),
        (
            "max(sum by (job, pod) (x))",
            after(
                "x",
                latest(),
                &[
                    json!({ "reduce": { "fn": "sum", "by": ["service.name", "pod"] } }),
                    json!({ "reduce": { "fn": "max" } }),
                ],
            ),
        ),
        (
            "topk(2.7, rate(x[5m]))",
            rate(json!({ "fn": "topk", "arg": 2.0 })),
        ),
        // Below 1 selects nothing, as in Prometheus.
        (
            "topk(-5, rate(x[5m]))",
            rate(json!({ "fn": "topk", "arg": 0.0 })),
        ),
    ]);
    // A computed value drops the name; `without` drops it too, `by` keeps
    // exactly its labels.
    assert_eq!(labels("rate(x[5m])"), (vec![], true));
    assert_eq!(labels("last_over_time(x[5m])").0, ["metric.name"]);
    assert_eq!(labels("max without (pod) (x)"), (vec![], true));
    assert_eq!(
        labels("sum by (__name__) (x)"),
        (vec!["metric.name".into()], false)
    );
}

#[test]
fn constructs_the_ir_cannot_express_are_named() {
    for (q, needle) in [
        ("limitk(2, x)", "limitk"),
        ("limit_ratio(0.5, x)", "limit_ratio"),
        ("x[5m]", "range vector"),
        (r#""text""#, "string literal"),
        ("quantile(1.5, x)", "outside [0, 1]"),
        ("quantile_over_time(-1, x[5m])", "outside [0, 1]"),
        ("quantile_over_time(NaN, x[5m])", "non-finite"),
    ] {
        let msg = inexpressible(q);
        assert!(msg.contains(needle), "{q}: {msg}");
    }
}

fn binop(op: &str, right: Value, extra: Value) -> Value {
    let mut b = json!({ "op": op, "right": right, "reverse": false, "bool": false });
    b.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    json!({ "binop": b })
}

/// `m` as a sub-document: the selector and its sample.
fn sub(m: &str) -> Value {
    json!({ "from": "metrics", "pipeline": [name(m), latest()] })
}

#[test]
fn binary_operators_with_a_number() {
    let x = |stage: Value| after("x", latest(), &[stage]);
    let filter =
        |op: &str, bool: bool| json!({ "filter": { "op": op, "value": 5.0, "bool": bool } });
    cases(&[
        ("x * 100", x(binop("mul", json!(100.0), json!({})))),
        (
            "2 - x",
            x(binop("sub", json!(2.0), json!({ "reverse": true }))),
        ),
        ("x ^ 2", x(binop("pow", json!(2.0), json!({})))),
        ("x atan2 2", x(binop("atan2", json!(2.0), json!({})))),
        ("-x", x(binop("mul", json!(-1.0), json!({})))),
        ("x > 5", x(filter("gt", false))),
        ("5 < x", x(filter("gt", false))),
        ("5 >= x", x(filter("le", false))),
        ("x == bool 5", x(filter("eq", true))),
        ("x != (2 + 3)", x(filter("ne", false))),
    ]);
    assert!(inexpressible("x * (1 / 0)").contains("non-finite"));
}

/// Scalar-only expressions fold, or read a pseudo-source.
#[test]
fn scalar_expressions() {
    let constant = |c: f64| {
        let doc = lower(&format!("{c}"));
        (
            doc["from"].clone(),
            doc["constant"].clone(),
            doc["result"].clone(),
        )
    };
    assert_eq!(
        constant(3.0),
        (json!("constant"), json!(3.0), json!("scalar"))
    );
    for (q, value) in [
        ("1 + 2 * 3", 7.0),
        ("2 ^ 3 % 5", 3.0),
        ("1 > bool 2", 0.0),
        ("-(4)", -4.0),
    ] {
        let doc = lower(q);
        assert_eq!(
            (doc["from"].clone(), doc["constant"].clone()),
            (json!("constant"), json!(value)),
            "{q}"
        );
    }
    let doc = lower("time() * 2");
    assert_eq!(
        (doc["from"].clone(), doc["result"].clone()),
        (json!("time"), json!("scalar"))
    );
    assert_eq!(
        doc["pipeline"],
        json!([binop("mul", json!(2.0), json!({}))])
    );
    let doc = lower("time() - time()");
    let time = json!({ "from": "time", "pipeline": [] });
    assert_eq!(doc["pipeline"], json!([binop("sub", time, json!({}))]));
    assert!(inexpressible("1 / 0").contains("non-finite"));
}

#[test]
fn vector_matching() {
    let a = |b: Value| after("a", latest(), &[b]);
    cases(&[
        ("a / b", a(binop("div", sub("b"), json!({})))),
        (
            "a / on(job) group_left(team) b",
            a(binop(
                "div",
                sub("b"),
                json!({
                "on": ["service.name"], "group": { "side": "left", "include": ["team"] } }),
            )),
        ),
        (
            "a * ignoring(pod) group_right b",
            a(binop(
                "mul",
                sub("b"),
                json!({ "ignoring": ["pod"], "group": { "side": "right" } }),
            )),
        ),
        (
            "a > bool on() b",
            a(binop("gt", sub("b"), json!({ "on": [], "bool": true }))),
        ),
        ("a and b", a(binop("and", sub("b"), json!({})))),
        ("a or b", a(binop("or", sub("b"), json!({})))),
        (
            "a unless on(job) b",
            a(binop("unless", sub("b"), json!({ "on": ["service.name"] }))),
        ),
        // A pseudo-source left operand swaps sides; `reverse` keeps the order.
        (
            "time() - a",
            a(binop(
                "sub",
                json!({ "from": "time", "pipeline": [] }),
                json!({ "reverse": true }),
            )),
        ),
        // A scalar on the left of a comparison swaps sides with the operator
        // flipped, so the vector's values are the ones kept.
        ("scalar(y) < a", a(binop("gt", scalar_of("y"), json!({})))),
        (
            "time() > a",
            a(binop(
                "lt",
                json!({ "from": "time", "pipeline": [] }),
                json!({}),
            )),
        ),
        (
            "(a + b) / c",
            after(
                "a",
                latest(),
                &[
                    binop("add", sub("b"), json!({})),
                    binop("div", sub("c"), json!({})),
                ],
            ),
        ),
        (
            "a / (b + c)",
            a(binop(
                "div",
                json!({ "from": "metrics", "pipeline": [
                name("b"), latest(), binop("add", sub("c"), json!({}))
            ] }),
                json!({}),
            )),
        ),
    ]);
}

fn scalar_of(m: &str) -> Value {
    json!({ "from": "metrics", "pipeline": [name(m), latest(), { "scalar": {} }] })
}

#[test]
fn scalar_functions() {
    cases(&[
        ("vector(1)", json!([{ "vector": {} }])),
        (
            "x * scalar(y)",
            after("x", latest(), &[binop("mul", scalar_of("y"), json!({}))]),
        ),
    ]);
    let doc = lower("vector(1)");
    assert_eq!(
        (doc["from"].clone(), doc["constant"].clone()),
        (json!("constant"), json!(1.0))
    );
    assert_eq!(lower("scalar(x)")["result"], json!("scalar"));
    let doc = lower("vector(time())");
    assert_eq!(
        (&doc["from"], &doc["pipeline"]),
        (&json!("time"), &json!([{ "vector": {} }]))
    );
    let doc = lower("-time()");
    let minus = binop("mul", json!(-1.0), json!({}));
    assert_eq!(
        (&doc["from"], &doc["result"]),
        (&json!("time"), &json!("scalar"))
    );
    assert_eq!(doc["pipeline"], json!([minus]));
    // Arithmetic drops the metric name; a filtering comparison keeps it.
    assert_eq!(labels("x * 2"), (vec![], true));
    assert_eq!(labels("x > 2").0, ["metric.name"]);
    assert_eq!(lower("pi()")["constant"], json!(std::f64::consts::PI));
    assert!(inexpressible("sin(x)").contains("sin"));
}

#[test]
fn functions() {
    let x = |stages: &[Value]| after("x", latest(), stages);
    let map = |f: &str, args: Value| json!({ "map": { "fn": f, "args": args } });
    let map0 = |f: &str| json!({ "map": { "fn": f } });
    let vector = json!({ "vector": {} });
    cases(&[
        ("abs(x)", x(&[map0("abs")])),
        ("round(x)", x(&[map0("round")])),
        ("round(x, 0.5)", x(&[map("round", json!([0.5]))])),
        ("clamp(x, 0, 1)", x(&[map("clamp", json!([0.0, 1.0]))])),
        ("clamp_min(x, 0)", x(&[map("clamp_min", json!([0.0]))])),
        ("clamp_max(x, 1)", x(&[map("clamp_max", json!([1.0]))])),
        ("day_of_week(x)", x(&[map0("day_of_week")])),
        ("hour()", json!([vector, map0("hour")])),
        ("sort(x)", x(&[json!({ "sort": "asc" })])),
        ("sort_desc(x)", x(&[json!({ "sort": "desc" })])),
        (
            r#"label_replace(x, "svc", "$1", "job", "(.*)-prod")"#,
            x(&[json!({ "labels": { "replace": {
                "dst": "svc", "replacement": "$1", "src": "service.name", "regex": "^(?s:(.*)-prod)$"
            } } })]),
        ),
        // An empty source is Prometheus's constant-label idiom.
        (
            r#"label_replace(x, "a", "b", "", "")"#,
            x(&[json!({ "labels": { "replace": {
                "dst": "a", "replacement": "b", "src": "", "regex": "^(?s:)$" } } })]),
        ),
        (
            r#"label_join(x, "id", "/", "job", "pod")"#,
            x(&[json!({ "labels": { "join": {
                "dst": "id", "separator": "/", "src": ["service.name", "pod"] } } })]),
        ),
        (
            r#"absent(x{job="api", pod=~"p.*"})"#,
            json!([
                { "where": { "and": [
                    leaf("metric.name", "eq", "x"),
                    leaf("service.name", "eq", "api"),
                    leaf("pod", "regex", "^(?s:p.*)$")
                ] } },
                latest(),
                { "absent": { "labels": { "service.name": "api" } } }
            ]),
        ),
        (
            "absent_over_time(x[5m])",
            after(
                "x",
                ranged("count_over_time", "5m"),
                &[json!({ "absent": { "labels": {} } })],
            ),
        ),
    ]);
    // absent() labels as Prometheus does: in matcher order, a label's first
    // equality matcher sets it (the stage drops an empty value), any later
    // matcher on it removes it; `job` and `service` are one label.
    let absent = lower(r#"absent(x{a="1", a="2", b!="3", b="4", c="", d="5"})"#);
    assert_eq!(
        absent["pipeline"][2],
        json!({ "absent": { "labels": { "b": "4", "c": "", "d": "5" } } })
    );
    let absent = lower(r#"absent(x{job="api", service="api", e="", e="6"})"#);
    assert_eq!(absent["pipeline"][2], json!({ "absent": { "labels": {} } }));
    assert_eq!(labels(r#"absent(x{d="5"})"#), (vec!["d".into()], false));
    // topk keeps its input series whole; count_values labels by the value.
    cases(&[
        (
            "topk without (pod) (2, x)",
            x(&[json!({ "reduce": { "fn": "topk", "without": ["pod"], "arg": 2.0 } })]),
        ),
        (
            r#"count_values without (pod) ("v", x)"#,
            x(&[json!({ "reduce": { "fn": "count_values", "without": ["pod"], "label": "v" } })]),
        ),
    ]);
    assert_eq!(labels("topk without (pod) (2, x)").0, ["metric.name"]);
    assert_eq!(
        labels(r#"count_values without (pod) ("v", x)"#),
        (vec!["v".into()], true)
    );
    for (q, needle) in [
        ("timestamp(x)", "timestamp"),
        ("clamp(x, -Inf, Inf)", "non-finite"),
        ("predict_linear(x[5m], 60)", "predict_linear"),
        (r#"sort_by_label(x, "job")"#, "sort_by_label"),
        ("mad_over_time(x[5m])", "mad_over_time"),
    ] {
        let msg = inexpressible(q);
        assert!(msg.contains(needle), "{q}: {msg}");
    }
}

/// A `histogram_quantile` stage; `by: None` is the bare, per-series form.
/// Instant mode reads each series' latest point within the lookback.
fn hq(q: f64, by: Option<Value>, mode: &str, window: Option<&str>) -> Value {
    let mut h = json!({ "q": q, "step": "1m", "mode": mode, "as": "quantile" });
    if mode == "instant" {
        h["lookback"] = json!("5m");
    }
    match by {
        Some(by) => h["by"] = by,
        None => {
            h["by"] = json!([]);
            h["per_series"] = json!(true);
        }
    }
    if let Some(w) = window {
        h["window"] = json!(w);
    }
    json!({ "histogram_quantile": h })
}

#[test]
fn histograms() {
    cases(&[
        (
            "histogram_quantile(0.95, x)",
            json!([name("x"), hq(0.95, None, "instant", None)]),
        ),
        (
            "histogram_quantile(0.9, rate(x[5m]))",
            json!([name("x"), hq(0.9, None, "rate", Some("5m"))]),
        ),
        (
            "histogram_quantile(0.9, sum by (le, job) (rate(x[5m])))",
            json!([
                name("x"),
                hq(0.9, Some(json!(["service.name"])), "rate", Some("5m"))
            ]),
        ),
        // A quantile ignores the scale `increase` has over `rate`.
        (
            "histogram_quantile(0.9, sum(increase(x[10m])))",
            json!([name("x"), hq(0.9, Some(json!([])), "rate", Some("10m"))]),
        ),
        (
            "histogram_quantile(0.5, x) * 1000",
            json!([
                name("x"),
                hq(0.5, None, "instant", None),
                binop("mul", json!(1000.0), json!({}))
            ]),
        ),
        (
            "histogram_fraction(0, 1, x)",
            json!([name("x"), { "histogram_fraction": {
                "lower": 0.0, "upper": 1.0, "by": [], "per_series": true, "step": "1m",
                "mode": "instant", "lookback": "5m", "as": "fraction" } }]),
        ),
        (
            "histogram_fraction(0, 0.25, sum by (le) (rate(x[5m])))",
            json!([name("x"), { "histogram_fraction": {
                "lower": 0.0, "upper": 0.25, "by": [], "step": "1m", "mode": "rate",
                "window": "5m", "as": "fraction" } }]),
        ),
        // Neither the name nor `le` labels a merged histogram.
        (
            "histogram_quantile(0.9, sum by (le, __name__) (rate(x[5m])))",
            json!([name("x"), hq(0.9, Some(json!([])), "rate", Some("5m"))]),
        ),
    ]);
    let by_job = "histogram_quantile(0.9, sum by (le, job) (rate(x[5m])))";
    assert_eq!(labels(by_job), (vec!["service.name".into()], false));
    assert_eq!(labels("histogram_quantile(0.9, x)"), (vec![], true));
    // An instant query evaluates at one instant, at its nominal step.
    let instant = |q: &str| lower_with(q, &PromqlParams::instant(END))["pipeline"].clone();
    let mut bare = hq(0.9, None, "instant", None);
    bare["histogram_quantile"]["step"] = json!("1s");
    assert_eq!(
        instant("histogram_quantile(0.9, x)"),
        json!([name("x"), bare])
    );
    let mut rated = hq(0.9, Some(json!([])), "rate", Some("5m"));
    rated["histogram_quantile"]["step"] = json!("1s");
    assert_eq!(
        instant("histogram_quantile(0.9, sum by (le) (rate(x[5m])))"),
        json!([name("x"), rated])
    );
}

#[test]
fn subqueries() {
    let over = |f: &str, window: &str| json!({ "over_time": { "fn": f, "window": window } });
    let rate_at = |step: &str| {
        let mut s = ranged("rate", "5m");
        s["sample"]["step"] = json!(step);
        s
    };
    cases(&[
        (
            "max_over_time(rate(x[5m])[1h:30s])",
            after("x", rate_at("30s"), &[over("max", "1h")]),
        ),
        // The selector's own offset and `@` carry into the subquery.
        ("max_over_time((x offset 5m @ 1700000000)[1h:30s])", {
            let mut s = latest();
            s["sample"]["step"] = json!("30s");
            s["sample"]["offset"] = json!("5m");
            s["sample"]["at"] = json!(START);
            after("x", s, &[over("max", "1h")])
        }),
        ("quantile_over_time(0.5, x[1h:30s])", {
            let mut s = latest();
            s["sample"]["step"] = json!("30s");
            let mut o = over("quantile", "1h");
            o["over_time"]["arg"] = json!(0.5);
            after("x", s, &[o])
        }),
        (
            "max_over_time(avg_over_time(rate(x[5m])[10m:30s])[1h:1m])",
            after(
                "x",
                rate_at("30s"),
                &[over("avg", "10m"), over("max", "1h")],
            ),
        ),
        (
            "deriv(sum(rate(x[5m]))[30m:30s])",
            after(
                "x",
                rate_at("30s"),
                &[json!({ "reduce": { "fn": "sum" } }), over("deriv", "30m")],
            ),
        ),
    ]);
    // Coarser than the query step: fine where the step is, or for an
    // instant query, whose step rises to meet it.
    let coarse = "max_over_time(rate(x[5m])[1h:5m])";
    let every_5m = PromqlParams::range(START, END, 300_000_000_000);
    assert_eq!(
        lower_with(coarse, &every_5m)["pipeline"],
        after("x", ranged("rate", "5m"), &[over("max", "1h")])
    );
    // No resolution: 1m, Prometheus's default evaluation interval.
    let default_res = "max_over_time(rate(x[5m])[1h:])";
    assert_eq!(
        lower_with(default_res, &every_5m)["pipeline"],
        after("x", rate_at("1m"), &[over("max", "1h")])
    );
    let every_30s = PromqlParams::range(START, END, 30_000_000_000);
    assert!(matches!(
        ql_ir::promql_to_ir(default_res, &every_30s),
        Err(LowerError::Inexpressible(msg)) if msg.contains("coarser")
    ));
    assert_eq!(
        lower_with(coarse, &PromqlParams::instant(END))["step"],
        json!("5m")
    );
    for (q, needle) in [
        (coarse, "coarser"),
        ("rate(x[5m:1m])", "rate() over a subquery"),
        ("max_over_time((x - time())[1h:30s])", "time()"),
        ("max_over_time(x[1h:30s] offset 5m)", "offset"),
        ("max_over_time(x[1h:30s] @ 1700000000)", "@"),
    ] {
        let msg = inexpressible(q);
        assert!(msg.contains(needle), "{q}: {msg}");
    }
}

/// Bare dotted metric names read as their quoted form. The hive repro:
/// this query failed to parse before dotted names were rewritten.
#[test]
fn bare_dotted_metric_names() {
    for (bare, quoted) in [
        (
            "histogram_quantile(0.95, rate(signaldb.writer.commit_duration[5m]))",
            r#"histogram_quantile(0.95, rate({"signaldb.writer.commit_duration"}[5m]))"#,
        ),
        (
            r#"process.memory.usage{service_name="signaldb"}"#,
            r#"{__name__="process.memory.usage", service_name="signaldb"}"#,
        ),
        (
            "sum by (job) (rate(signaldb.ingest.spans_received[5m]))",
            r#"sum by (job) (rate({"signaldb.ingest.spans_received"}[5m]))"#,
        ),
    ] {
        assert_eq!(lower(bare), lower(quoted), "{bare}");
    }
    assert_eq!(
        lower("histogram_quantile(0.95, rate(signaldb.writer.commit_duration[5m]))")["pipeline"],
        json!([
            name("signaldb.writer.commit_duration"),
            hq(0.95, None, "rate", Some("5m"))
        ])
    );
    assert_eq!(
        lower(r#"a.b{x="1" or y="2"}"#),
        lower(r#"{"a.b", x="1" or "a.b", y="2"}"#)
    );
    // The quoting hint shows only where a dotted name is still bare.
    let invalid = |q: &str| match ql_ir::promql_to_ir(q, &PromqlParams::range(START, END, STEP)) {
        Err(LowerError::InvalidPromql(msg)) => msg,
        other => panic!("{q}: expected InvalidPromql, got {other:?}"),
    };
    assert!(invalid("sum by (http.method) (a.b)").contains(r#"("a.b")"#));
    assert!(!invalid("a.b.c{").contains("quoted"));
}

/// A histogram's count and sum read as plain values, and the histogram
/// operands the IR cannot express.
#[test]
fn histogram_values_and_refusals() {
    let of = |f: &str, of: &str, window: Option<&str>| {
        let mut s = match window {
            Some(w) => ranged(f, w),
            None => latest(),
        };
        s["sample"]["of"] = json!(of);
        s
    };
    cases(&[
        (
            "histogram_count(x)",
            json!([
                name("x"),
                of("latest", "metric.count", None),
                binop("mul", json!(1.0), json!({}))
            ]),
        ),
        (
            "histogram_sum(rate(x[5m]))",
            json!([name("x"), of("rate", "metric.sum", Some("5m"))]),
        ),
        (
            "histogram_avg(rate(x[5m]))",
            json!([
                name("x"),
                of("rate", "metric.sum", Some("5m")),
                binop(
                    "div",
                    json!({
                        "from": "metrics", "pipeline": [name("x"), of("rate", "metric.count", Some("5m"))]
                    }),
                    json!({})
                )
            ]),
        ),
    ]);
    for (q, needle) in [
        (
            "histogram_quantile(0.9, sum without (pod) (rate(x[5m])))",
            "sum without",
        ),
        ("histogram_quantile(0.9, max(x))", "max()"),
        ("histogram_quantile(0.9, x offset 5m)", "offset"),
        ("histogram_fraction(0, 1, irate(x[5m]))", "irate()"),
        ("histogram_count(sum(x))", "sum()"),
        ("histogram_stddev(x)", "histogram_stddev"),
        ("histogram_quantile(1.5, rate(x[5m]))", "outside [0, 1]"),
        ("histogram_quantile(-0.1, rate(x[5m]))", "outside [0, 1]"),
        ("histogram_fraction(2, 1, rate(x[5m]))", "lower above upper"),
        ("histogram_count(max_over_time(x[5m]))", "max_over_time()"),
        (
            "histogram_sum(stddev_over_time(x[5m]))",
            "stddev_over_time()",
        ),
        (
            r#"histogram_quantile(0.9, {__name__=~"x.*"})"#,
            "several metric names",
        ),
    ] {
        let msg = inexpressible(q);
        assert!(msg.contains(needle), "{q}: {msg}");
    }
}

/// Histogram outputs carry Prometheus's labels: no metric name, and each
/// grouping field once even when several PromQL labels map to it.
#[test]
fn histogram_output_labels() {
    let dup = lower("histogram_quantile(0.9, sum by (le, job, service_name) (rate(x[5m])))");
    assert_eq!(
        dup["pipeline"][1]["histogram_quantile"]["by"],
        json!(["service.name"])
    );
    for q in ["histogram_count(x)", "histogram_sum(last_over_time(x[5m]))"] {
        assert!(!labels(q).0.contains(&"metric.name".to_string()), "{q}");
    }
    assert_eq!(
        labels("histogram_fraction(0, 1, sum by (le, job) (rate(x[5m])))"),
        (vec!["service.name".to_string()], false)
    );
    assert_eq!(labels("histogram_fraction(0, 1, x)"), (vec![], true));
}
