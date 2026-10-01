//! `get_trace`'s Query IR document and response decoding: builds the `rows`
//! query over the `traces` source, and turns the response into the
//! Tempo-shaped payload the waterfall app (`src/mcp-server/ui/trace.html`)
//! and the plain-text tool result both read. Mirrors
//! `src/ui/src/api/traceDetail.ts`.

use std::collections::{HashMap, HashSet};

use serde::{Serialize, Serializer};
use serde_json::{Map, Value};

/// The fields `trace_document` projects. The response names each column
/// after its physical column (`span.name` arrives as `span_name`,
/// `duration` as `duration_nanos`), which is what `decode_span` reads.
const TRACE_SPAN_FIELDS: &[&str] = &[
    "span_id",
    "parent_span_id",
    "is_root",
    "span.name",
    "service.name",
    "status.code",
    "status_message",
    "start_time_unix_nano",
    "duration",
    "span_kind",
    "span.attributes",
    "scope.attributes",
    "resource.attributes",
    "span_events",
];

/// Build the Query IR document `get_trace` submits: every span of one trace,
/// defaulting to the last 30 days when no `start`/`end` hint is given (a
/// trace opened by pasting its ID may be much older than any short default
/// window).
pub(crate) fn trace_document(
    trace_id: &str,
    start: Option<i64>,
    end: Option<i64>,
) -> serde_json::Value {
    let (range_from, range_to) = crate::server::range_bounds_ns(start, end, "now-30d");
    serde_json::json!({
        "irVersion": 1,
        "from": "traces",
        "range": { "from": range_from, "to": range_to },
        "result": "rows",
        "fields": TRACE_SPAN_FIELDS,
        "pipeline": [
            { "where": { "field": "trace_id", "op": "eq", "value": trace_id } }
        ]
    })
}

fn u64_as_string<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(value)
}

#[derive(Serialize)]
pub(crate) struct EventPayload {
    name: String,
    #[serde(rename = "timeUnixNano")]
    time_unix_nano: String,
    attributes: Map<String, Value>,
}

#[derive(Serialize)]
pub(crate) struct SpanPayload {
    #[serde(rename = "spanID")]
    span_id: String,
    #[serde(rename = "parentSpanID", skip_serializing_if = "Option::is_none")]
    parent_span_id: Option<String>,
    #[serde(skip)]
    is_root: bool,
    name: String,
    #[serde(rename = "serviceName")]
    service_name: String,
    #[serde(rename = "startTimeUnixNano", serialize_with = "u64_as_string")]
    start_ns: u64,
    #[serde(rename = "durationNanos", serialize_with = "u64_as_string")]
    duration_ns: u64,
    status: String,
    #[serde(rename = "statusMessage", skip_serializing_if = "Option::is_none")]
    status_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kind: Option<String>,
    attributes: Map<String, Value>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    events: Vec<EventPayload>,
}

#[derive(Serialize)]
pub(crate) struct SpanSet {
    matched: usize,
    spans: Vec<SpanPayload>,
}

/// `get_trace`'s output payload — Tempo-shaped so the existing waterfall app
/// (`src/mcp-server/ui/trace.html`) keeps working unmodified.
#[derive(Serialize)]
pub(crate) struct TraceView {
    #[serde(rename = "traceID")]
    trace_id: String,
    #[serde(rename = "rootServiceName")]
    root_service_name: String,
    #[serde(rename = "rootTraceName")]
    root_trace_name: String,
    #[serde(rename = "startTimeUnixNano", serialize_with = "u64_as_string")]
    start_ns: u64,
    #[serde(rename = "durationMs")]
    duration_ms: u64,
    #[serde(rename = "spanSets")]
    span_sets: Vec<SpanSet>,
    services: Value,
}

/// Owned string of a cell; empty strings and non-string, non-number cells
/// are `None`.
fn as_string(v: Value) -> Option<String> {
    match v {
        Value::String(s) if !s.is_empty() => Some(s),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn as_u64(v: &Value) -> u64 {
    match v {
        Value::Number(n) => n.as_u64().unwrap_or(0),
        Value::String(s) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

fn status_of(v: Value) -> String {
    as_string(v)
        .map(|s| s.to_lowercase())
        .filter(|s| s != "unspecified")
        .unwrap_or_else(|| "unset".to_string())
}

/// A cell holding JSON, either inline or — from a legacy table — as a
/// JSON-encoded string.
fn parse_json_cell(v: Value) -> Value {
    match v {
        Value::String(s) => serde_json::from_str(&s).unwrap_or(Value::Null),
        other => other,
    }
}

/// An attribute container cell. Anything but an object decodes to empty;
/// null values are dropped and arrays/objects stringified, mirroring the
/// UI's `container`.
fn container(v: Value) -> Map<String, Value> {
    let Value::Object(map) = parse_json_cell(v) else {
        return Map::new();
    };
    map.into_iter()
        .filter(|(_, value)| !value.is_null())
        .map(|(k, value)| match value {
            Value::String(_) | Value::Number(_) | Value::Bool(_) => (k, value),
            other => (k, Value::String(other.to_string())),
        })
        .collect()
}

/// The `span_events` cell: `[{name, timestamp_unix_nano, attributes}]`.
fn decode_events(v: Value) -> Vec<EventPayload> {
    let Value::Array(items) = parse_json_cell(v) else {
        return Vec::new();
    };
    items
        .into_iter()
        .filter_map(|item| match item {
            Value::Object(mut obj) => Some(EventPayload {
                name: obj.remove("name").and_then(as_string).unwrap_or_default(),
                time_unix_nano: obj
                    .remove("timestamp_unix_nano")
                    .and_then(as_string)
                    .unwrap_or_default(),
                attributes: obj.remove("attributes").map(container).unwrap_or_default(),
            }),
            _ => None,
        })
        .collect()
}

fn decode_span(mut row: Vec<Value>, index: &HashMap<String, usize>) -> SpanPayload {
    let mut take = |name: &str| {
        index
            .get(name)
            .and_then(|&i| row.get_mut(i))
            .map(Value::take)
            .unwrap_or(Value::Null)
    };
    let is_root = take("is_root").as_bool().unwrap_or(false);
    let parent_span_id = as_string(take("parent_span_id")).filter(|_| !is_root);
    let mut attributes = container(take("span_attributes"));
    for (k, v) in container(take("scope_attributes")) {
        attributes.insert(format!("scope.{k}"), v);
    }
    for (k, v) in container(take("resource_attributes")) {
        attributes.insert(format!("resource.{k}"), v);
    }
    SpanPayload {
        span_id: as_string(take("span_id")).unwrap_or_default(),
        parent_span_id,
        is_root,
        name: as_string(take("span_name")).unwrap_or_default(),
        service_name: as_string(take("service_name")).unwrap_or_default(),
        start_ns: as_u64(&take("start_time_unix_nano")),
        duration_ns: as_u64(&take("duration_nanos")),
        status: status_of(take("status_code")),
        status_message: as_string(take("status_message")),
        kind: as_string(take("span_kind")),
        attributes,
        events: decode_events(take("span_events")),
    }
}

/// The root span: the first span the writer flagged `is_root` or that has no
/// parent; else the first orphan (a parent id not present in this trace —
/// the real root hasn't ended, or been ingested, yet); else the first span.
/// `spans` must be sorted by start time.
fn select_root(spans: &[SpanPayload]) -> Option<&SpanPayload> {
    let ids: HashSet<&str> = spans.iter().map(|s| s.span_id.as_str()).collect();
    spans
        .iter()
        .find(|s| s.is_root || s.parent_span_id.is_none())
        .or_else(|| {
            spans.iter().find(|s| {
                s.parent_span_id
                    .as_deref()
                    .is_some_and(|p| !ids.contains(p))
            })
        })
        .or_else(|| spans.first())
}

/// Decode `trace_document`'s response into `get_trace`'s output payload.
/// `None` when the response has no rows (the trace isn't in the queried
/// window).
pub(crate) fn trace_from_response(
    trace_id: &str,
    response: signaldb_sdk::types::QueryIrResponse,
) -> Option<TraceView> {
    let index: HashMap<String, usize> = response
        .columns
        .into_iter()
        .enumerate()
        .map(|(i, c)| (c.name, i))
        .collect();
    let mut spans: Vec<SpanPayload> = response
        .rows
        .into_iter()
        .map(|row| decode_span(row, &index))
        .collect();
    // The same span can come back from more than one storage tier.
    spans.sort_by(|a, b| (a.start_ns, &a.span_id).cmp(&(b.start_ns, &b.span_id)));
    spans.dedup_by(|a, b| a.span_id == b.span_id);

    let root = select_root(&spans)?;
    let root_service_name = root.service_name.clone();
    let root_trace_name = root.name.clone();

    let start_ns = spans.first().map_or(0, |s| s.start_ns);
    let end_ns = spans
        .iter()
        .map(|s| s.start_ns.saturating_add(s.duration_ns))
        .max()
        .unwrap_or(start_ns);

    Some(TraceView {
        trace_id: trace_id.to_string(),
        root_service_name,
        root_trace_name,
        start_ns,
        duration_ms: end_ns.saturating_sub(start_ns) / 1_000_000,
        services: trace_services_summary(&spans),
        span_sets: vec![SpanSet {
            matched: spans.len(),
            spans,
        }],
    })
}

/// Derive `get_trace`'s `services` summary from the trace's own spans:
/// nodes are services with total span duration and call count in this
/// trace, edges are calls between *different* services found by walking to
/// each span's direct parent (a span nested under its own service's parent
/// extends the caller rather than drawing a self-edge), failed when any
/// call or span reached error status. Mirrors
/// `src/ui/src/lib/traceToGraph.ts`'s `traceToGraph`.
fn trace_services_summary(spans: &[SpanPayload]) -> Value {
    let by_id: HashMap<&str, &SpanPayload> = spans
        .iter()
        .map(|span| (span.span_id.as_str(), span))
        .collect();

    #[derive(Default)]
    struct NodeAcc {
        duration_ms: f64,
        span_count: i64,
        failed: bool,
    }
    let mut nodes: HashMap<&str, NodeAcc> = HashMap::new();
    for span in spans {
        let acc = nodes.entry(span.service_name.as_str()).or_default();
        acc.duration_ms += span.duration_ns as f64 / 1_000_000.0;
        acc.span_count += 1;
        acc.failed |= span.status == "error";
    }

    #[derive(Default)]
    struct EdgeAcc {
        count: i64,
        failed: bool,
    }
    let mut edges: HashMap<(&str, &str), EdgeAcc> = HashMap::new();
    for span in spans {
        let Some(parent) = span.parent_span_id.as_deref().and_then(|id| by_id.get(id)) else {
            continue;
        };
        let (from, to) = (parent.service_name.as_str(), span.service_name.as_str());
        if from == to {
            continue;
        }
        let acc = edges.entry((from, to)).or_default();
        acc.count += 1;
        acc.failed |= span.status == "error";
    }

    let node_list: Vec<Value> = nodes
        .into_iter()
        .map(|(service, acc)| {
            serde_json::json!({
                "service": service,
                "durationMs": acc.duration_ms,
                "spanCount": acc.span_count,
                "failed": acc.failed,
            })
        })
        .collect();
    let edge_list: Vec<Value> = edges
        .into_iter()
        .map(|((from, to), acc)| {
            serde_json::json!({
                "from": from,
                "to": to,
                "count": acc.count,
                "failed": acc.failed,
            })
        })
        .collect();
    serde_json::json!({ "nodes": node_list, "edges": edge_list })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(
        columns: &[&str],
        rows: Vec<Vec<serde_json::Value>>,
    ) -> signaldb_sdk::types::QueryIrResponse {
        let columns: Vec<serde_json::Value> = columns
            .iter()
            .map(|name| serde_json::json!({ "name": name, "type": "utf8" }))
            .collect();
        serde_json::from_value(serde_json::json!({
            "result": "rows",
            "window": { "start_ns": 0, "end_ns": 1 },
            "columns": columns,
            "rows": rows,
        }))
        .expect("response parses")
    }

    #[test]
    fn trace_document_defaults_to_the_last_30_days() {
        let doc = trace_document("abc123", None, None);
        assert_eq!(doc["range"]["from"], "now-30d");
        assert_eq!(doc["range"]["to"], "now");
        assert_eq!(doc["from"], "traces");
        assert_eq!(doc["result"], "rows");
    }

    #[test]
    fn trace_document_converts_start_end_hints_to_nanoseconds() {
        let doc = trace_document("abc123", Some(10), Some(20));
        assert_eq!(doc["range"]["from"], "10000000000");
        assert_eq!(doc["range"]["to"], "20000000000");
    }

    #[test]
    fn trace_document_filters_by_trace_id() {
        let doc = trace_document("abc123", None, None);
        let leaf = &doc["pipeline"][0]["where"];
        assert_eq!(leaf["field"], "trace_id");
        assert_eq!(leaf["op"], "eq");
        assert_eq!(leaf["value"], "abc123");
    }

    #[test]
    fn trace_document_fields_include_span_kind() {
        let doc = trace_document("abc123", None, None);
        let fields: Vec<&str> = doc["fields"]
            .as_array()
            .expect("fields array")
            .iter()
            .map(|v| v.as_str().expect("field is a string"))
            .collect();
        assert!(fields.contains(&"span_kind"));
    }

    fn columns() -> &'static [&'static str] {
        &[
            "span_id",
            "parent_span_id",
            "is_root",
            "span_name",
            "service_name",
            "status_code",
            "status_message",
            "start_time_unix_nano",
            "duration_nanos",
            "span_kind",
            "span_attributes",
            "scope_attributes",
            "resource_attributes",
            "span_events",
        ]
    }

    fn col(name: &str) -> usize {
        columns()
            .iter()
            .position(|c| *c == name)
            .expect("fixture column exists")
    }

    fn span_row(
        span_id: &str,
        parent_span_id: serde_json::Value,
        start_ns: i64,
        service_name: &str,
    ) -> Vec<serde_json::Value> {
        vec![
            serde_json::json!(span_id),
            parent_span_id,
            serde_json::Value::Null,
            serde_json::json!("GET /"),
            serde_json::json!(service_name),
            serde_json::json!("ok"),
            serde_json::Value::Null,
            serde_json::json!(start_ns),
            serde_json::json!(1_000_000),
            serde_json::Value::Null,
            serde_json::json!("{}"),
            serde_json::json!("{}"),
            serde_json::json!("{}"),
            serde_json::Value::Null,
        ]
    }

    #[test]
    fn a_span_returned_twice_appears_once() {
        let rows = vec![
            span_row("1", serde_json::Value::Null, 0, "frontend"),
            span_row("2", serde_json::json!("1"), 0, "checkout"),
            span_row("1", serde_json::Value::Null, 0, "frontend"),
        ];
        let trace =
            trace_from_response("abc123", response(columns(), rows)).expect("trace decodes");
        assert_eq!(trace.span_sets[0].matched, 2);
        assert_eq!(trace.span_sets[0].spans.len(), 2);
    }

    #[test]
    fn empty_rows_decode_to_none() {
        let response = response(columns(), vec![]);
        assert!(trace_from_response("abc123", response).is_none());
    }

    #[test]
    fn span_kind_carries_through_to_the_payload() {
        let mut row = span_row("1", serde_json::Value::Null, 0, "checkout");
        let kind_idx = col("span_kind");
        row[kind_idx] = serde_json::json!("Server");

        let trace =
            trace_from_response("abc123", response(columns(), vec![row])).expect("trace decodes");
        let span = &trace.span_sets[0].spans[0];
        assert_eq!(span.kind.as_deref(), Some("Server"));
    }

    #[test]
    fn root_falls_back_to_the_earliest_orphan_when_no_span_has_no_parent() {
        let rows = vec![
            span_row("2", serde_json::json!("missing-parent-a"), 500, "checkout"),
            span_row("1", serde_json::json!("missing-parent-b"), 100, "frontend"),
        ];
        let trace =
            trace_from_response("abc123", response(columns(), rows)).expect("trace decodes");
        assert_eq!(trace.root_service_name, "frontend");
    }

    #[test]
    fn a_span_flagged_is_root_is_the_root_and_drops_its_sentinel_parent() {
        let rows = vec![
            span_row("orphan", serde_json::json!("missing-parent"), 0, "checkout"),
            span_row("1", serde_json::json!("0000000000000000"), 500, "frontend"),
        ];
        let mut rows = rows;
        rows[1][col("is_root")] = serde_json::json!(true);
        let trace =
            trace_from_response("abc123", response(columns(), rows)).expect("trace decodes");
        let span = &trace.span_sets[0].spans[1];
        assert!(span.parent_span_id.is_none());
        assert_eq!(trace.root_service_name, "frontend");
    }

    #[test]
    fn a_true_root_wins_over_an_earlier_orphan() {
        let rows = vec![
            span_row("orphan", serde_json::json!("missing-parent"), 0, "checkout"),
            span_row("root", serde_json::Value::Null, 500, "frontend"),
        ];
        let trace =
            trace_from_response("abc123", response(columns(), rows)).expect("trace decodes");
        assert_eq!(trace.root_service_name, "frontend");
    }

    #[test]
    fn attributes_flatten_scope_and_resource_prefixes_and_decode_legacy_json_strings() {
        let mut row = span_row("1", serde_json::Value::Null, 0, "frontend");
        let span_idx = col("span_attributes");
        row[span_idx] = serde_json::json!(r#"{"http.method":"GET"}"#);
        let scope_idx = col("scope_attributes");
        row[scope_idx] = serde_json::json!({"name": "otel"});
        let resource_idx = col("resource_attributes");
        row[resource_idx] = serde_json::json!({"service.version": "1.0"});

        let trace =
            trace_from_response("abc123", response(columns(), vec![row])).expect("trace decodes");
        let span = &trace.span_sets[0].spans[0];
        assert_eq!(span.attributes["http.method"], serde_json::json!("GET"));
        assert_eq!(span.attributes["scope.name"], serde_json::json!("otel"));
        assert_eq!(
            span.attributes["resource.service.version"],
            serde_json::json!("1.0")
        );
    }

    #[test]
    fn events_decode_from_the_json_string_cell() {
        let mut row = span_row("1", serde_json::Value::Null, 0, "frontend");
        let events_idx = col("span_events");
        row[events_idx] = serde_json::json!(
            r#"[{"name":"exception","timestamp_unix_nano":"100","attributes":{"level":"error"}}]"#
        );

        let trace =
            trace_from_response("abc123", response(columns(), vec![row])).expect("trace decodes");
        let span = &trace.span_sets[0].spans[0];
        assert_eq!(span.events.len(), 1);
        assert_eq!(span.events[0].name, "exception");
        assert_eq!(span.events[0].time_unix_nano, "100");
        assert_eq!(
            span.events[0].attributes["level"],
            serde_json::json!("error")
        );
    }

    #[test]
    fn status_lowercases_and_defaults_unset() {
        let mut row = span_row("1", serde_json::Value::Null, 0, "frontend");
        let status_idx = col("status_code");
        row[status_idx] = serde_json::json!("ERROR");
        let msg_idx = col("status_message");
        row[msg_idx] = serde_json::json!("boom");

        let trace =
            trace_from_response("abc123", response(columns(), vec![row])).expect("trace decodes");
        let span = &trace.span_sets[0].spans[0];
        assert_eq!(span.status, "error");
        assert_eq!(span.status_message.as_deref(), Some("boom"));

        let mut unset_row = span_row("2", serde_json::Value::Null, 0, "frontend");
        let status_idx = col("status_code");
        unset_row[status_idx] = serde_json::json!("unspecified");
        let trace = trace_from_response("abc123", response(columns(), vec![unset_row]))
            .expect("trace decodes");
        assert_eq!(trace.span_sets[0].spans[0].status, "unset");
        assert!(trace.span_sets[0].spans[0].status_message.is_none());
    }

    #[test]
    fn duration_ms_spans_the_earliest_start_to_the_latest_end() {
        let mut row_a = span_row("1", serde_json::Value::Null, 0, "frontend");
        let dur_idx = col("duration_nanos");
        row_a[dur_idx] = serde_json::json!(2_000_000);
        let row_b = span_row("2", serde_json::json!("1"), 1_000_000, "checkout");

        let trace = trace_from_response("abc123", response(columns(), vec![row_a, row_b]))
            .expect("trace decodes");
        // earliest start 0, latest end = 1_000_000 (start) + 1_000_000 (dur) = 2_000_000ns = 2ms
        assert_eq!(trace.duration_ms, 2);
    }

    fn span(
        span_id: &str,
        parent_span_id: Option<&str>,
        service_name: &str,
        duration_ns: u64,
        status: &str,
    ) -> SpanPayload {
        SpanPayload {
            span_id: span_id.to_string(),
            parent_span_id: parent_span_id.map(str::to_string),
            is_root: false,
            name: "op".to_string(),
            service_name: service_name.to_string(),
            status: status.to_string(),
            status_message: None,
            kind: None,
            start_ns: 0,
            duration_ns,
            attributes: serde_json::Map::new(),
            events: Vec::new(),
        }
    }

    #[test]
    fn trace_services_summary_is_empty_for_a_trace_with_no_spans() {
        assert_eq!(
            trace_services_summary(&[]),
            serde_json::json!({ "nodes": [], "edges": [] })
        );
    }

    #[test]
    fn trace_services_summary_sums_duration_per_service_and_marks_failed_nodes() {
        let spans = vec![
            span("1", None, "frontend", 1000000, "unset"),
            span("2", Some("1"), "checkout", 2000000, "error"),
        ];
        let summary = trace_services_summary(&spans);
        let nodes = summary["nodes"].as_array().expect("nodes array");
        let checkout = nodes
            .iter()
            .find(|n| n["service"] == "checkout")
            .expect("checkout node present");
        assert_eq!(checkout["durationMs"], 2.0);
        assert_eq!(checkout["spanCount"], 1);
        assert_eq!(checkout["failed"], true);
        let frontend = nodes
            .iter()
            .find(|n| n["service"] == "frontend")
            .expect("frontend node present");
        assert_eq!(frontend["failed"], false);
    }

    #[test]
    fn trace_services_summary_edges_use_the_direct_parents_service_and_mark_failed_calls() {
        let spans = vec![
            span("1", None, "frontend", 1000000, "unset"),
            span("2", Some("1"), "checkout", 1000000, "error"),
        ];
        let summary = trace_services_summary(&spans);
        let edges = summary["edges"].as_array().expect("edges array");
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0]["from"], "frontend");
        assert_eq!(edges[0]["to"], "checkout");
        assert_eq!(edges[0]["count"], 1);
        assert_eq!(edges[0]["failed"], true);
    }

    /// A span nested under a same-service parent extends the caller rather
    /// than drawing a self-edge — mirrors `traceToGraph.ts`'s
    /// `callerService`.
    #[test]
    fn trace_services_summary_skips_same_service_parent_child_edges() {
        let spans = vec![
            span("1", None, "checkout", 1000000, "unset"),
            span("2", Some("1"), "checkout", 500000, "unset"),
            span("3", Some("2"), "payments", 200000, "unset"),
        ];
        let summary = trace_services_summary(&spans);
        let edges = summary["edges"].as_array().expect("edges array");
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0]["from"], "checkout");
        assert_eq!(edges[0]["to"], "payments");
    }
}
