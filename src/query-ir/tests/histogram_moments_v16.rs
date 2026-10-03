//! `irVersion` 16: `histogram_avg`, `histogram_stddev` and `histogram_stdvar`.

use query_ir::{Document, IrError, RelationType, SourceRegistry, ValueType, validate};
use serde_json::{Value, json};

fn resolver() -> query_ir::InMemoryResolver {
    query_ir::InMemoryResolver::new()
        .with_column("metrics", "metric.name", "metric_name", ValueType::String)
        .with_column("metrics", "service.name", "service_name", ValueType::String)
}

fn check(v: Value) -> Result<RelationType, IrError> {
    let d: Document = serde_json::from_value(v).expect("document parses");
    validate(&d, &SourceRegistry::core(), &resolver()).map(|v| v.terminal)
}

fn doc(version: i64, from: &str, stage: Value) -> Value {
    json!({
        "irVersion": version, "from": from,
        "range": { "from": "now-1h", "to": "now" },
        "result": "series", "pipeline": [stage],
    })
}

const STAGES: [&str; 3] = ["histogram_avg", "histogram_stddev", "histogram_stdvar"];

fn operands() -> Value {
    json!({ "by": ["service.name"], "step": "1m", "as": "v" })
}

#[test]
fn the_moment_stages_yield_a_series() {
    for name in STAGES {
        match check(doc(16, "metrics", json!({ name: operands() }))) {
            Ok(RelationType::Series(_)) => {}
            other => panic!("{name}: expected a series, got {other:?}"),
        }
    }
}

#[test]
fn the_moment_stages_require_irversion_16() {
    for name in STAGES {
        let e = check(doc(15, "metrics", json!({ name: operands() })))
            .expect_err("gated")
            .to_string();
        assert!(e.contains("irVersion 16"), "{name}: {e}");
    }
}

#[test]
fn the_moment_stages_are_metrics_only_and_closed() {
    for name in STAGES {
        assert!(check(doc(16, "logs", json!({ name: operands() }))).is_err());
        let mut unknown = operands();
        unknown["lower"] = json!(0);
        let parsed =
            serde_json::from_value::<Document>(doc(16, "metrics", json!({ name: unknown })));
        assert!(parsed.is_err(), "{name} accepted an unknown operand");
    }
}

#[test]
fn per_series_excludes_by() {
    let mut o = operands();
    o["per_series"] = json!(true);
    let e = check(doc(16, "metrics", json!({ "histogram_stdvar": o })))
        .expect_err("by with per_series")
        .to_string();
    assert!(e.contains("per_series"), "{e}");
}
