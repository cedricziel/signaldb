//! `list_services`: what is sending data into a dataset. One Query IR
//! aggregate per signal (service name, namespace and version, with row count
//! and first/last seen), merged into one entry per service.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use signaldb_sdk::types::{QueryIrResponse, QueryWarning, ResolvedWindow};

/// Every signal source, with the logical name of its primary time column.
pub(crate) const SIGNALS: &[(&str, &str)] = &[
    ("traces", "start_time_unix_nano"),
    ("logs", "timestamp"),
    ("metrics", "timestamp"),
    ("profiles", "timestamp"),
];

/// The grouping fields. A source with no such attribute in the window warns
/// `unknown_group_by_field` and groups under `null`, which is what
/// `list_services` wants, so that warning is dropped for these fields.
const GROUP_BY: [&str; 3] = ["service.name", "service.namespace", "service.version"];

/// The aggregate `list_services` runs against one signal.
pub(crate) fn document(signal: &str, time_field: &str, from: &str, to: &str) -> serde_json::Value {
    serde_json::json!({
        "irVersion": 1,
        "from": signal,
        "range": { "from": from, "to": to },
        "result": "table",
        "pipeline": [{
            "aggregate": {
                "by": GROUP_BY,
                "aggs": [
                    { "fn": "count", "as": "rows" },
                    { "fn": "min", "of": time_field, "as": "first_seen" },
                    { "fn": "max", "of": time_field, "as": "last_seen" }
                ]
            }
        }]
    })
}

#[derive(Debug, Serialize)]
pub(crate) struct ServiceList {
    /// The window every signal was read over.
    #[serde(skip_serializing_if = "Option::is_none")]
    window: Option<ResolvedWindow>,
    /// True when every signal answered: the counts and times are then exact
    /// for `window`. False when a signal failed (see `errors`).
    exact: bool,
    services: Vec<Service>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    errors: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<QueryWarning>,
}

#[derive(Debug, Default, Serialize)]
struct Service {
    name: Option<String>,
    namespaces: BTreeSet<String>,
    versions: BTreeSet<String>,
    #[serde(flatten)]
    seen: Seen,
    signals: BTreeMap<String, SignalSeen>,
}

#[derive(Debug, Default, Serialize)]
struct SignalSeen {
    versions: BTreeSet<String>,
    #[serde(flatten)]
    seen: Seen,
}

#[derive(Debug, Default, Serialize)]
struct Seen {
    rows: i64,
    first_seen_ns: Option<i64>,
    last_seen_ns: Option<i64>,
}

impl Seen {
    fn add(&mut self, rows: i64, first: Option<i64>, last: Option<i64>) {
        self.rows += rows;
        self.first_seen_ns = min_some(self.first_seen_ns, first);
        self.last_seen_ns = self.last_seen_ns.max(last);
    }
}

fn min_some(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

fn is_expected(warning: &QueryWarning) -> bool {
    warning.code == "unknown_group_by_field"
        && warning
            .field
            .as_deref()
            .is_some_and(|field| GROUP_BY.contains(&field))
}

/// Merge each signal's aggregate (or its error) into one entry per service,
/// most rows first.
pub(crate) fn merge(answers: Vec<(&str, Result<QueryIrResponse, String>)>) -> ServiceList {
    let mut window = None;
    let mut errors = BTreeMap::new();
    let mut warnings = Vec::new();
    let mut services: BTreeMap<Option<String>, Service> = BTreeMap::new();
    for (signal, answer) in answers {
        let response = match answer {
            Ok(response) => response,
            Err(error) => {
                errors.insert(signal.to_string(), error);
                continue;
            }
        };
        window.get_or_insert(response.window);
        warnings.extend(
            response
                .warnings
                .into_iter()
                .filter(|warning| !is_expected(warning)),
        );
        for row in response.rows {
            let text = |i: usize| row.get(i).and_then(|v| v.as_str()).map(str::to_string);
            let int = |i: usize| row.get(i).and_then(|v| v.as_i64());
            let (name, version) = (text(0), text(2));
            let (rows, first, last) = (int(3).unwrap_or(0), int(4), int(5));
            let service = services.entry(name.clone()).or_insert_with(|| Service {
                name,
                ..Service::default()
            });
            service.namespaces.extend(text(1));
            service.versions.extend(version.clone());
            service.seen.add(rows, first, last);
            let on_signal = service.signals.entry(signal.to_string()).or_default();
            on_signal.versions.extend(version);
            on_signal.seen.add(rows, first, last);
        }
    }
    let mut services: Vec<Service> = services.into_values().collect();
    services.sort_by(|a, b| b.seen.rows.cmp(&a.seen.rows).then(a.name.cmp(&b.name)));
    ServiceList {
        window,
        exact: errors.is_empty(),
        services,
        errors,
        warnings,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(rows: serde_json::Value) -> QueryIrResponse {
        serde_json::from_value(serde_json::json!({
            "result": "table",
            "window": { "start_ns": 10, "end_ns": 20 },
            "rows": rows,
            "warnings": [{
                "code": "unknown_group_by_field",
                "field": "service.namespace",
                "message": "not on profiles"
            }]
        }))
        .expect("a table response")
    }

    #[test]
    fn the_document_groups_by_service_and_reads_the_time_column() {
        let doc = document("traces", "start_time_unix_nano", "now-24h", "now");
        assert_eq!(doc["from"], "traces");
        let aggregate = &doc["pipeline"][0]["aggregate"];
        assert_eq!(
            aggregate["by"],
            serde_json::json!(["service.name", "service.namespace", "service.version"])
        );
        assert_eq!(aggregate["aggs"][2]["of"], "start_time_unix_nano");
        serde_json::from_value::<signaldb_sdk::types::QueryIrRequest>(doc)
            .expect("a valid IR document");
    }

    #[test]
    fn a_service_seen_on_two_signals_is_one_entry() {
        let list = merge(vec![
            (
                "traces",
                Ok(response(serde_json::json!([
                    ["api", "shop", "1.2", 40, 100, 900],
                    ["web", null, null, 5, 300, 400]
                ]))),
            ),
            (
                "logs",
                Ok(response(serde_json::json!([[
                    "api", null, "1.3", 60, 50, 800
                ]]))),
            ),
        ]);
        let value = serde_json::to_value(&list).expect("serializes");
        assert_eq!(value["exact"], true);
        assert!(
            value.get("warnings").is_none(),
            "expected warnings are dropped"
        );
        assert_eq!(value["window"]["start_ns"], 10);
        let api = &value["services"][0];
        assert_eq!(api["name"], "api");
        assert_eq!(api["namespaces"], serde_json::json!(["shop"]));
        assert_eq!(api["versions"], serde_json::json!(["1.2", "1.3"]));
        assert_eq!(api["rows"], 100);
        assert_eq!(api["first_seen_ns"], 50);
        assert_eq!(api["last_seen_ns"], 900);
        assert_eq!(api["signals"]["traces"]["rows"], 40);
        assert_eq!(
            api["signals"]["logs"]["versions"],
            serde_json::json!(["1.3"])
        );
        assert_eq!(value["services"][1]["name"], "web");
        assert_eq!(value["services"][1]["versions"], serde_json::json!([]));
    }

    #[test]
    fn a_failed_signal_makes_the_list_inexact() {
        let list = merge(vec![
            ("traces", Ok(response(serde_json::json!([])))),
            ("profiles", Err("access denied".to_string())),
        ]);
        let value = serde_json::to_value(&list).expect("serializes");
        assert_eq!(value["exact"], false);
        assert_eq!(value["errors"]["profiles"], "access denied");
    }
}
