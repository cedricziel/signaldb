use std::{collections::HashMap, str::FromStr, sync::Arc};

use datafusion::arrow::{
    array::{Array, ArrayRef, BooleanArray, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};

/// 16-character hex sentinel for root/absent parent span IDs in OTLP traces.
/// OTLP span IDs are 8 bytes (16 hex characters); root spans encode the
/// parent span ID as all zeroes to indicate they have no parent.
pub const EMPTY_PARENT_SPAN_ID: &str = "0000000000000000";

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum SpanKind {
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

impl FromStr for SpanKind {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Internal" => Ok(SpanKind::Internal),
            "Server" => Ok(SpanKind::Server),
            "Client" => Ok(SpanKind::Client),
            "Producer" => Ok(SpanKind::Producer),
            "Consumer" => Ok(SpanKind::Consumer),
            _ => Ok(SpanKind::Internal),
        }
    }
}

impl SpanKind {
    pub fn to_str(&self) -> &str {
        match self {
            SpanKind::Internal => "Internal",
            SpanKind::Server => "Server",
            SpanKind::Client => "Client",
            SpanKind::Producer => "Producer",
            SpanKind::Consumer => "Consumer",
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum SpanStatus {
    Unspecified,
    Ok,
    Error,
}

impl FromStr for SpanStatus {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Unspecified" => Ok(SpanStatus::Unspecified),
            "Ok" => Ok(SpanStatus::Ok),
            "Error" => Ok(SpanStatus::Error),
            _ => Ok(SpanStatus::Unspecified),
        }
    }
}

impl SpanStatus {
    pub fn to_str(&self) -> &str {
        match self {
            SpanStatus::Unspecified => "Unspecified",
            SpanStatus::Ok => "Ok",
            SpanStatus::Error => "Error",
        }
    }
}

/// A single OpenTelemetry span event: a timestamped, named annotation with its
/// own attributes. An exception is the event named `exception`, carrying
/// `exception.message`/`exception.type`/`exception.stacktrace` in `attributes`.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct SpanEvent {
    pub name: String,
    pub timestamp_unix_nano: u64,
    pub attributes: HashMap<String, serde_json::Value>,
}

/// Parse the stored `events` column — a JSON string of
/// `[{name, timestamp_unix_nano, attributes_json}]` objects (the shape the
/// writer serializes the events list column to) — into [`SpanEvent`]s.
///
/// Tolerant by design: an empty, null, or malformed value yields no events
/// rather than an error, so a single bad row never fails a trace lookup.
pub fn parse_span_events(json: &str) -> Vec<SpanEvent> {
    #[derive(Deserialize)]
    struct RawEvent {
        #[serde(default)]
        name: String,
        #[serde(default)]
        timestamp_unix_nano: u64,
        #[serde(default)]
        attributes_json: Option<String>,
    }

    let raw: Vec<RawEvent> = serde_json::from_str(json).unwrap_or_default();
    raw.into_iter()
        .map(|e| SpanEvent {
            name: e.name,
            timestamp_unix_nano: e.timestamp_unix_nano,
            attributes: e
                .attributes_json
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_default(),
        })
        .collect()
}

/// Serialize [`SpanEvent`]s back to the stored/wire `events` column shape — a
/// JSON string of `[{name, timestamp_unix_nano, attributes_json}]` objects.
/// Inverse of [`parse_span_events`]; used to carry events across the
/// querier→router single-trace wire batch.
pub fn serialize_span_events(events: &[SpanEvent]) -> String {
    let raw: Vec<serde_json::Value> = events
        .iter()
        .map(|e| {
            serde_json::json!({
                "name": e.name,
                "timestamp_unix_nano": e.timestamp_unix_nano,
                "attributes_json": serde_json::to_string(&e.attributes)
                    .unwrap_or_else(|_| "{}".to_string()),
            })
        })
        .collect();
    serde_json::to_string(&raw).unwrap_or_else(|_| "[]".to_string())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Span {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,
    pub status: SpanStatus,

    pub is_root: bool,

    pub name: String,

    pub service_name: String,
    pub span_kind: SpanKind,

    pub start_time_unix_nano: u64,
    pub duration_nano: u64,

    pub attributes: HashMap<String, serde_json::Value>,
    pub resource: HashMap<String, serde_json::Value>,

    pub children: Vec<Span>,

    /// Span events (annotations, exceptions). Empty when none were recorded or
    /// when read on a path that does not project the `events` column.
    pub events: Vec<SpanEvent>,
}

impl Drop for Span {
    /// Drain the child subtree iteratively before the span is destroyed: the
    /// compiler-generated drop glue recurses through `children` and would
    /// overflow the stack on pathologically deep hierarchies, which the read
    /// path must survive (see [`build_span_hierarchy`]).
    fn drop(&mut self) {
        let mut stack = std::mem::take(&mut self.children);
        while let Some(mut span) = stack.pop() {
            stack.append(&mut span.children);
        }
    }
}

impl Span {
    /// Clone the span's own data with an empty `children` vec. A plain
    /// `clone()` copies the whole descendant subtree recursively, which both
    /// duplicates data and can overflow the stack on deep hierarchies.
    pub fn clone_without_children(&self) -> Span {
        Span {
            trace_id: self.trace_id.clone(),
            span_id: self.span_id.clone(),
            parent_span_id: self.parent_span_id.clone(),
            status: self.status.clone(),
            is_root: self.is_root,
            name: self.name.clone(),
            service_name: self.service_name.clone(),
            span_kind: self.span_kind.clone(),
            start_time_unix_nano: self.start_time_unix_nano,
            duration_nano: self.duration_nano,
            attributes: self.attributes.clone(),
            resource: self.resource.clone(),
            children: Vec::new(),
            events: self.events.clone(),
        }
    }

    pub fn to_schema() -> Schema {
        let fields = vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("status", DataType::Utf8, false),
            Field::new("is_root", DataType::Boolean, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("span_kind", DataType::Utf8, false),
            Field::new("start_time_unix_nano", DataType::UInt64, false),
            Field::new("duration_nano", DataType::UInt64, false),
            Field::new("span_attributes", DataType::Utf8, true),
            Field::new("resource_attributes", DataType::Utf8, true),
        ];
        Schema::new(fields)
    }

    pub fn to_record_batch(&self) -> RecordBatch {
        // create a new record batch
        let trace_id: ArrayRef = Arc::new(StringArray::from(vec![self.trace_id.clone()]));
        let span_id: ArrayRef = Arc::new(StringArray::from(vec![self.span_id.clone()]));
        let parent_span_id: ArrayRef =
            Arc::new(StringArray::from(vec![self.parent_span_id.clone()]));
        let status: ArrayRef = Arc::new(StringArray::from(vec![self.status.to_str()]));
        let is_root: ArrayRef = Arc::new(BooleanArray::from(vec![self.is_root]));
        let name: ArrayRef = Arc::new(StringArray::from(vec![self.name.clone()]));
        let service_name: ArrayRef = Arc::new(StringArray::from(vec![self.service_name.clone()]));
        let span_kind: ArrayRef = Arc::new(StringArray::from(vec![self.span_kind.to_str()]));
        let start_time_unix_nano: ArrayRef =
            Arc::new(UInt64Array::from(vec![self.start_time_unix_nano]));
        let duration_nano: ArrayRef = Arc::new(UInt64Array::from(vec![self.duration_nano]));
        let span_attributes: ArrayRef = Arc::new(StringArray::from(vec![
            serde_json::to_string(&self.attributes).unwrap_or_default(),
        ]));
        let resource_attributes: ArrayRef = Arc::new(StringArray::from(vec![
            serde_json::to_string(&self.resource).unwrap_or_default(),
        ]));

        RecordBatch::try_new(
            Arc::new(Self::to_schema()),
            vec![
                trace_id,
                span_id,
                parent_span_id,
                status,
                is_root,
                name,
                service_name,
                span_kind,
                start_time_unix_nano,
                duration_nano,
                span_attributes,
                resource_attributes,
            ],
        )
        .unwrap()
    }
}

/// Build a hierarchical span tree from a flat map of spans.
///
/// Links child spans to their parents and returns the root spans. A span is
/// considered a root if any of the following is true:
/// - `span.is_root` is set
/// - its `parent_span_id` is empty or the zero sentinel ([`EMPTY_PARENT_SPAN_ID`])
/// - its `parent_span_id` references a span not present in `span_map` (orphan)
///
/// The input `span_map` is consumed and mutated in place so that parent spans
/// contain their children.
pub fn build_span_hierarchy(span_map: HashMap<String, Span>) -> Vec<Span> {
    use std::collections::HashSet;

    // Adjacency list: parent span id -> child span ids, siblings ordered by
    // start time (span id as the tie-break) so the output is deterministic.
    let mut children_ids: HashMap<&str, Vec<&Span>> = HashMap::new();
    for span in span_map.values() {
        if !is_root_parent_id(&span.parent_span_id) {
            children_ids
                .entry(span.parent_span_id.as_str())
                .or_default()
                .push(span);
        }
    }
    for children in children_ids.values_mut() {
        children.sort_by(|a, b| {
            a.start_time_unix_nano
                .cmp(&b.start_time_unix_nano)
                .then_with(|| a.span_id.cmp(&b.span_id))
        });
    }

    // Copy a span with its whole descendant subtree attached, using an
    // explicit work stack so arbitrarily deep parent chains cannot overflow
    // the call stack. The visited set guards against parent-id cycles in
    // malformed data (and keeps a span claimed by several parents from being
    // duplicated).
    struct Frame<'a> {
        copy: Span,
        pending: Vec<&'a Span>,
        next: usize,
    }
    fn build_subtree<'a>(
        span: &'a Span,
        children_ids: &HashMap<&str, Vec<&'a Span>>,
        visited: &mut HashSet<&'a str>,
    ) -> Option<Span> {
        if !visited.insert(&span.span_id) {
            return None;
        }
        let mut stack = vec![Frame {
            copy: span.clone_without_children(),
            pending: children_ids
                .get(span.span_id.as_str())
                .cloned()
                .unwrap_or_default(),
            next: 0,
        }];
        let mut result = None;
        while let Some(frame) = stack.last_mut() {
            if frame.next < frame.pending.len() {
                let child = frame.pending[frame.next];
                frame.next += 1;
                if visited.insert(&child.span_id) {
                    stack.push(Frame {
                        copy: child.clone_without_children(),
                        pending: children_ids
                            .get(child.span_id.as_str())
                            .cloned()
                            .unwrap_or_default(),
                        next: 0,
                    });
                }
            } else if let Some(done) = stack.pop() {
                match stack.last_mut() {
                    Some(parent) => parent.copy.children.push(done.copy),
                    None => result = Some(done.copy),
                }
            }
        }
        result
    }

    // Roots: explicitly marked, zero-parent, or orphaned (parent id not in
    // the map). Ordered like siblings, for deterministic output.
    let mut root_spans: Vec<&Span> = span_map
        .values()
        .filter(|span| {
            span.is_root
                || is_root_parent_id(&span.parent_span_id)
                || !span_map.contains_key(&span.parent_span_id)
        })
        .collect();
    root_spans.sort_by(|a, b| {
        a.start_time_unix_nano
            .cmp(&b.start_time_unix_nano)
            .then_with(|| a.span_id.cmp(&b.span_id))
    });

    let mut visited: HashSet<&str> = HashSet::new();
    let mut roots: Vec<Span> = root_spans
        .into_iter()
        .filter_map(|span| build_subtree(span, &children_ids, &mut visited))
        .collect();

    // Any span still unvisited sits in, or hangs below, a parent-id cycle no
    // root reaches (its parent must also be unvisited, otherwise the parent's
    // subtree would have claimed it). Promote one span per cycle — the
    // earliest-starting cycle *member*, not merely the earliest residual span,
    // so descendants of the cycle keep their parent link instead of being
    // split off as separate roots — and build the full component from there.
    let mut unvisited: Vec<&Span> = span_map
        .values()
        .filter(|span| !visited.contains(span.span_id.as_str()))
        .collect();
    unvisited.sort_by(|a, b| {
        a.start_time_unix_nano
            .cmp(&b.start_time_unix_nano)
            .then_with(|| a.span_id.cmp(&b.span_id))
    });
    let unvisited_ids: HashSet<&str> = unvisited.iter().map(|s| s.span_id.as_str()).collect();
    let mut cycle_roots: Vec<&Span> = Vec::new();
    let mut walked: HashSet<&str> = HashSet::new();
    for span in &unvisited {
        if walked.contains(span.span_id.as_str()) {
            continue;
        }
        // Walk the parent chain until it revisits itself (the cycle) or joins
        // a chain walked earlier (that walk already recorded the cycle).
        let mut path: Vec<&Span> = Vec::new();
        let mut path_ids: HashSet<&str> = HashSet::new();
        let mut current: &Span = span;
        loop {
            if walked.contains(current.span_id.as_str()) {
                break;
            }
            if !path_ids.insert(&current.span_id) {
                let members = path
                    .iter()
                    .skip_while(|s| s.span_id != current.span_id)
                    .copied();
                if let Some(root) = members.min_by(|a, b| {
                    a.start_time_unix_nano
                        .cmp(&b.start_time_unix_nano)
                        .then_with(|| a.span_id.cmp(&b.span_id))
                }) {
                    cycle_roots.push(root);
                }
                break;
            }
            path.push(current);
            match span_map.get(&current.parent_span_id) {
                Some(parent) if unvisited_ids.contains(parent.span_id.as_str()) => {
                    current = parent;
                }
                _ => break,
            }
        }
        for s in &path {
            walked.insert(&s.span_id);
        }
    }
    cycle_roots.sort_by(|a, b| {
        a.start_time_unix_nano
            .cmp(&b.start_time_unix_nano)
            .then_with(|| a.span_id.cmp(&b.span_id))
    });
    for span in cycle_roots {
        if let Some(root) = build_subtree(span, &children_ids, &mut visited) {
            roots.push(root);
        }
    }
    // Safety net: surface anything a malformed chain left unreached rather
    // than dropping it (unreachable in theory, cheap to keep in practice).
    for span in unvisited {
        if !visited.contains(span.span_id.as_str())
            && let Some(root) = build_subtree(span, &children_ids, &mut visited)
        {
            roots.push(root);
        }
    }

    roots
}

/// Returns true if the parent span ID indicates a root span (empty or the zero sentinel).
fn is_root_parent_id(parent_span_id: &str) -> bool {
    parent_span_id.is_empty() || parent_span_id == EMPTY_PARENT_SPAN_ID
}

/// A batch of spans.
///
/// Supposedly making it easier to convert to a record batch.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpanBatch {
    pub spans: Vec<Span>,
}

impl Default for SpanBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl SpanBatch {
    pub fn new() -> Self {
        SpanBatch { spans: vec![] }
    }

    pub fn new_with_spans(spans: Vec<Span>) -> Self {
        SpanBatch { spans }
    }

    pub fn add_span(&mut self, span: Span) {
        self.spans.push(span);
    }

    /// Create a new span batch from a request.
    pub fn from_request(
        _request: &opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest,
    ) -> Self {
        SpanBatch { spans: vec![] }
    }

    /// Convert the span batch to a record batch.
    pub fn to_record_batch(&self) -> RecordBatch {
        let schema = Span::to_schema();
        let mut columns = vec![];

        for span in &self.spans {
            let record_batch = span.to_record_batch();

            for i in 0..record_batch.num_columns() {
                columns.push(record_batch.column(i).clone());
            }
        }

        RecordBatch::try_new(Arc::new(schema), columns).unwrap()
    }

    /// Convert an arrow batch to a span batch.
    pub fn from_record_batch(batch: &RecordBatch) -> Self {
        let mut span_batch = SpanBatch::new();

        for i in 0..batch.num_rows() {
            let trace_id = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let span_id = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let parent_span_id = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let status = batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let is_root = batch
                .column(4)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            let name = batch
                .column(5)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let service_name = batch
                .column(6)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let span_kind = batch
                .column(7)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let start_time_unix_nano = batch
                .column(8)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let duration_nano = batch
                .column(9)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            // Read attributes columns gracefully (may not exist in older data)
            let attributes: HashMap<String, serde_json::Value> = batch
                .column_by_name("span_attributes")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .and_then(|arr| {
                    if arr.is_null(i) {
                        None
                    } else {
                        serde_json::from_str(arr.value(i)).ok()
                    }
                })
                .unwrap_or_default();

            let resource: HashMap<String, serde_json::Value> = batch
                .column_by_name("resource_attributes")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .and_then(|arr| {
                    if arr.is_null(i) {
                        None
                    } else {
                        serde_json::from_str(arr.value(i)).ok()
                    }
                })
                .unwrap_or_default();

            // Span events are stored as a JSON string; absent on paths that
            // don't project the column, which parse_span_events treats as empty.
            let events: Vec<SpanEvent> = batch
                .column_by_name("events")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .filter(|arr| !arr.is_null(i))
                .map(|arr| parse_span_events(arr.value(i)))
                .unwrap_or_default();

            let span = Span {
                trace_id: trace_id.value(i).to_string(),
                span_id: span_id.value(i).to_string(),
                parent_span_id: parent_span_id.value(i).to_string(),
                status: status.value(i).parse().unwrap_or(SpanStatus::Unspecified),
                is_root: is_root.value(i),
                name: name.value(i).to_string(),
                service_name: service_name.value(i).to_string(),
                span_kind: span_kind.value(i).parse().unwrap_or(SpanKind::Internal),
                start_time_unix_nano: start_time_unix_nano.value(i),
                duration_nano: duration_nano.value(i),
                attributes,
                resource,
                children: vec![],
                events,
            };

            span_batch.add_span(span);
        }

        span_batch
    }
}

impl From<RecordBatch> for SpanBatch {
    fn from(batch: RecordBatch) -> Self {
        SpanBatch::from_record_batch(&batch)
    }
}

impl From<&RecordBatch> for SpanBatch {
    fn from(batch: &RecordBatch) -> Self {
        SpanBatch::from_record_batch(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chain_span(span_id: &str, parent_span_id: &str, start: u64) -> Span {
        Span {
            trace_id: "trace".to_string(),
            span_id: span_id.to_string(),
            parent_span_id: parent_span_id.to_string(),
            status: SpanStatus::Ok,
            is_root: parent_span_id.is_empty(),
            name: span_id.to_string(),
            service_name: "svc".to_string(),
            span_kind: SpanKind::Internal,
            start_time_unix_nano: start,
            duration_nano: 1,
            attributes: HashMap::new(),
            resource: HashMap::new(),
            children: vec![],
            events: vec![],
        }
    }

    fn span_map(spans: Vec<Span>) -> HashMap<String, Span> {
        spans
            .into_iter()
            .map(|span| (span.span_id.clone(), span))
            .collect()
    }

    /// Count every span reachable from the returned roots (iteratively, so
    /// the deep-hierarchy test cannot overflow the test's own stack).
    fn count_spans(spans: &[Span]) -> usize {
        let mut count = 0;
        let mut stack: Vec<&Span> = spans.iter().collect();
        while let Some(span) = stack.pop() {
            count += 1;
            stack.extend(span.children.iter());
        }
        count
    }

    #[test]
    fn hierarchy_preserves_spans_deeper_than_the_roots_children() {
        // root -> a -> b -> c: every span must be reachable from the root.
        let map = span_map(vec![
            chain_span("root", "", 0),
            chain_span("a", "root", 1),
            chain_span("b", "a", 2),
            chain_span("c", "b", 3),
        ]);

        let roots = build_span_hierarchy(map);
        assert_eq!(roots.len(), 1, "one root expected");
        assert_eq!(
            count_spans(&roots),
            4,
            "grandchildren must not be dropped from the hierarchy"
        );
        let a = &roots[0].children[0];
        assert_eq!(a.span_id, "a");
        let b = &a.children[0];
        assert_eq!(b.span_id, "b");
        assert_eq!(b.children[0].span_id, "c");
    }

    #[test]
    fn hierarchy_promotes_orphan_subtrees_to_roots() {
        // Parent "lost" is not in the map: its child becomes a root but
        // keeps its own descendants.
        let map = span_map(vec![
            chain_span("orphan", "lost", 5),
            chain_span("leaf", "orphan", 6),
        ]);

        let roots = build_span_hierarchy(map);
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].span_id, "orphan");
        assert_eq!(roots[0].children.len(), 1);
        assert_eq!(roots[0].children[0].span_id, "leaf");
    }

    #[test]
    fn hierarchy_orders_siblings_by_start_time() {
        let map = span_map(vec![
            chain_span("root", "", 0),
            chain_span("late", "root", 20),
            chain_span("early", "root", 10),
        ]);

        let roots = build_span_hierarchy(map);
        let children: Vec<&str> = roots[0]
            .children
            .iter()
            .map(|s| s.span_id.as_str())
            .collect();
        assert_eq!(children, vec!["early", "late"]);
    }

    #[test]
    fn hierarchy_survives_parent_cycles() {
        // Malformed data: x and y claim each other as parents. Both must
        // surface (as roots of broken subtrees) without infinite recursion.
        let map = span_map(vec![chain_span("x", "y", 1), chain_span("y", "x", 2)]);

        let roots = build_span_hierarchy(map);
        assert_eq!(count_spans(&roots), 2, "both spans must survive a cycle");
    }

    #[test]
    fn hierarchy_keeps_cycle_descendants_attached_to_the_cycle() {
        // z (earliest span overall) hangs below a cycle: the cycle must be
        // broken at its earliest *member* (x), keeping x -> z intact instead
        // of promoting z to a disconnected root.
        let map = span_map(vec![
            chain_span("x", "y", 10),
            chain_span("y", "x", 20),
            chain_span("z", "x", 1),
        ]);

        let roots = build_span_hierarchy(map);
        assert_eq!(roots.len(), 1, "the whole component shares one root");
        assert_eq!(roots[0].span_id, "x", "earliest cycle member is the root");
        let children: Vec<&str> = roots[0]
            .children
            .iter()
            .map(|s| s.span_id.as_str())
            .collect();
        assert_eq!(
            children,
            vec!["z", "y"],
            "cycle descendant must stay attached to its parent"
        );
        assert_eq!(count_spans(&roots), 3);
    }

    #[test]
    fn hierarchy_handles_very_deep_parent_chains_without_overflowing() {
        // A pathological 100k-deep parent chain must not overflow the stack
        // on the read path.
        const DEPTH: u64 = 100_000;
        let mut spans = vec![chain_span("s0", "", 0)];
        for i in 1..DEPTH {
            spans.push(chain_span(&format!("s{i}"), &format!("s{}", i - 1), i));
        }
        let map = span_map(spans);

        let roots = build_span_hierarchy(map);
        assert_eq!(roots.len(), 1);
        assert_eq!(count_spans(&roots), DEPTH as usize);
    }

    #[test]
    fn test_span() {
        let span = Span {
            trace_id: "trace_id".to_string(),
            span_id: "span_id".to_string(),
            parent_span_id: "parent_span_id".to_string(),
            status: SpanStatus::Ok,
            is_root: true,
            name: "name".to_string(),
            service_name: "service_name".to_string(),
            span_kind: SpanKind::Client,
            start_time_unix_nano: 0,
            duration_nano: 0,
            attributes: HashMap::new(),
            resource: HashMap::new(),
            children: vec![],
            events: vec![],
        };

        let record_batch = span.to_record_batch();
        assert_eq!(record_batch.num_columns(), 12);
        assert_eq!(record_batch.num_rows(), 1);
    }

    #[test]
    fn test_span_batch() {
        let mut span_batch = SpanBatch::new();
        span_batch.add_span(Span {
            trace_id: "trace_id".to_string(),
            span_id: "span_id".to_string(),
            parent_span_id: "parent_span_id".to_string(),
            status: SpanStatus::Ok,
            is_root: true,
            name: "name".to_string(),
            service_name: "service_name".to_string(),
            span_kind: SpanKind::Client,
            start_time_unix_nano: 0,
            duration_nano: 0,
            attributes: HashMap::new(),
            resource: HashMap::new(),
            children: vec![],
            events: vec![],
        });

        let record_batch = span_batch.to_record_batch();
        assert_eq!(record_batch.num_columns(), 12);
        assert_eq!(record_batch.num_rows(), 1);

        let span_batch = SpanBatch::from_record_batch(&record_batch);
        assert_eq!(span_batch.spans.len(), 1);
    }

    #[test]
    fn parse_span_events_reads_the_stored_exception_shape() {
        // The on-disk `events` column is a JSON string: an array of
        // {name, timestamp_unix_nano, attributes_json} objects, where
        // attributes_json is itself a JSON object of the event's attributes.
        let json = r#"[{"name":"exception","timestamp_unix_nano":1700000000000000000,"attributes_json":"{\"exception.message\":\"boom\",\"exception.type\":\"std::io::Error\"}"}]"#;
        let events = parse_span_events(json);
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.name, "exception");
        assert_eq!(event.timestamp_unix_nano, 1_700_000_000_000_000_000);
        assert_eq!(
            event
                .attributes
                .get("exception.message")
                .and_then(|v| v.as_str()),
            Some("boom")
        );
        assert_eq!(
            event
                .attributes
                .get("exception.type")
                .and_then(|v| v.as_str()),
            Some("std::io::Error")
        );
    }

    #[test]
    fn span_events_round_trip_through_serialize_and_parse() {
        let mut attributes = HashMap::new();
        attributes.insert(
            "exception.message".to_string(),
            serde_json::Value::String("boom".to_string()),
        );
        let events = vec![SpanEvent {
            name: "exception".to_string(),
            timestamp_unix_nano: 99,
            attributes,
        }];
        let json = serialize_span_events(&events);
        assert_eq!(parse_span_events(&json), events);
    }

    #[test]
    fn parse_span_events_tolerates_empty_and_malformed() {
        assert!(parse_span_events("[]").is_empty());
        assert!(parse_span_events("").is_empty());
        assert!(parse_span_events("not json").is_empty());
    }

    #[test]
    fn from_record_batch_populates_events_from_json_column() {
        // A trace-lookup batch: the base 12 columns plus the stored `events`
        // JSON-string column the query path projects.
        let mut fields = Span::to_schema().fields().to_vec();
        fields.push(Arc::new(Field::new("events", DataType::Utf8, true)));
        let schema = Arc::new(Schema::new(fields));

        let events_json = r#"[{"name":"exception","timestamp_unix_nano":42,"attributes_json":"{\"exception.message\":\"kaboom\"}"}]"#;
        let base = Span {
            trace_id: "t".to_string(),
            span_id: "s".to_string(),
            parent_span_id: "p".to_string(),
            status: SpanStatus::Error,
            is_root: true,
            name: "n".to_string(),
            service_name: "svc".to_string(),
            span_kind: SpanKind::Server,
            start_time_unix_nano: 0,
            duration_nano: 0,
            attributes: HashMap::new(),
            resource: HashMap::new(),
            children: vec![],
            events: vec![],
        };
        let base_batch = base.to_record_batch();
        let mut columns: Vec<ArrayRef> = base_batch.columns().to_vec();
        columns.push(Arc::new(StringArray::from(vec![events_json])));
        let batch = RecordBatch::try_new(schema, columns).unwrap();

        let parsed = SpanBatch::from_record_batch(&batch);
        assert_eq!(parsed.spans.len(), 1);
        let events = &parsed.spans[0].events;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].name, "exception");
        assert_eq!(events[0].timestamp_unix_nano, 42);
        assert_eq!(
            events[0]
                .attributes
                .get("exception.message")
                .and_then(|v| v.as_str()),
            Some("kaboom")
        );
    }

    #[test]
    fn test_span_kind() {
        assert_eq!("Internal".parse::<SpanKind>().unwrap(), SpanKind::Internal);
        assert_eq!("Server".parse::<SpanKind>().unwrap(), SpanKind::Server);
        assert_eq!("Client".parse::<SpanKind>().unwrap(), SpanKind::Client);
        assert_eq!("Producer".parse::<SpanKind>().unwrap(), SpanKind::Producer);
        assert_eq!("Consumer".parse::<SpanKind>().unwrap(), SpanKind::Consumer);

        assert_eq!(SpanKind::Internal.to_str(), "Internal");
        assert_eq!(SpanKind::Server.to_str(), "Server");
        assert_eq!(SpanKind::Client.to_str(), "Client");
        assert_eq!(SpanKind::Producer.to_str(), "Producer");
        assert_eq!(SpanKind::Consumer.to_str(), "Consumer");
    }

    #[test]
    fn test_span_status() {
        assert_eq!(
            "Unspecified".parse::<SpanStatus>().unwrap(),
            SpanStatus::Unspecified
        );
        assert_eq!("Ok".parse::<SpanStatus>().unwrap(), SpanStatus::Ok);
        assert_eq!("Error".parse::<SpanStatus>().unwrap(), SpanStatus::Error);

        assert_eq!(SpanStatus::Unspecified.to_str(), "Unspecified");
        assert_eq!(SpanStatus::Ok.to_str(), "Ok");
        assert_eq!(SpanStatus::Error.to_str(), "Error");
    }

    #[test]
    fn test_span_schema() {
        let schema = Span::to_schema();
        assert_eq!(schema.fields().len(), 12);
    }

    #[test]
    fn test_span_batch_from_request() {
        let request = opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest {
            resource_spans: vec![],
        };

        let span_batch = SpanBatch::from_request(&request);
        assert_eq!(span_batch.spans.len(), 0);
    }

    #[test]
    fn test_span_attributes_round_trip_via_record_batch() {
        let mut attributes = HashMap::new();
        attributes.insert(
            "http.method".to_string(),
            serde_json::Value::String("GET".to_string()),
        );
        attributes.insert(
            "http.status_code".to_string(),
            serde_json::Value::Number(200.into()),
        );

        let mut resource = HashMap::new();
        resource.insert(
            "service.name".to_string(),
            serde_json::Value::String("test-service".to_string()),
        );

        let span = Span {
            trace_id: "trace_id".to_string(),
            span_id: "span_id".to_string(),
            parent_span_id: "parent_span_id".to_string(),
            status: SpanStatus::Ok,
            is_root: true,
            name: "test-op".to_string(),
            service_name: "test-service".to_string(),
            span_kind: SpanKind::Server,
            start_time_unix_nano: 1_000_000_000,
            duration_nano: 500_000_000,
            attributes: attributes.clone(),
            resource: resource.clone(),
            children: vec![],
            events: vec![],
        };

        let record_batch = span.to_record_batch();
        assert_eq!(record_batch.num_columns(), 12);
        assert_eq!(record_batch.num_rows(), 1);

        let span_batch = SpanBatch::from_record_batch(&record_batch);
        assert_eq!(span_batch.spans.len(), 1);

        let recovered = &span_batch.spans[0];
        assert_eq!(recovered.attributes, attributes);
        assert_eq!(recovered.resource, resource);
        assert_eq!(
            recovered.attributes.get("http.method"),
            Some(&serde_json::Value::String("GET".to_string()))
        );
        assert_eq!(
            recovered.resource.get("service.name"),
            Some(&serde_json::Value::String("test-service".to_string()))
        );
    }
}
