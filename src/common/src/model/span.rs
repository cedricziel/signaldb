use std::collections::HashMap;

#[derive(Clone)]
pub enum SpanKind {
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

#[derive(Clone)]
pub enum SpanStatus {
    Unspecified,
    Ok,
    Error,
}

#[derive(Clone)]
pub struct Span {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,
    pub status: SpanStatus,

    pub is_root: bool,

    pub name: String,

    pub service_name: String,
    pub span_kind: SpanKind,

    pub attributes: HashMap<String, serde_json::Value>,
    pub resource: HashMap<String, serde_json::Value>,
}
