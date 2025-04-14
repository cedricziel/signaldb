use serde::{Deserialize, Serialize};

use super::span::Span;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Trace {
    pub trace_id: String,
    pub spans: Vec<Span>,
}

impl Trace {
    pub fn new(trace_id: impl Into<String>) -> Self {
        Self {
            trace_id: trace_id.into(),
            spans: Vec::new(),
        }
    }

    pub fn new_with_spans(trace_id: impl Into<String>, spans: Vec<Span>) -> Self {
        Self {
            trace_id: trace_id.into(),
            spans,
        }
    }

    pub fn add_span(&mut self, span: Span) {
        self.spans.push(span);
    }

    pub fn root_span(&self) -> Option<&Span> {
        self.spans.iter().find(|s| s.is_root)
    }

    pub fn duration_ms(&self) -> u64 {
        if let Some(root) = self.root_span() {
            root.duration_nano / 1_000_000
        } else if let Some(first) = self.spans.first() {
            first.duration_nano / 1_000_000
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::messages::span::{Span, SpanKind, SpanStatus};

    #[test]
    fn test_trace() {
        let mut trace = Trace::new("trace_id");
        assert_eq!(trace.trace_id, "trace_id");
        assert_eq!(trace.spans.len(), 0);

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
            duration_nano: 1_000_000_000, // 1 second
            attributes: HashMap::new(),
            resource: HashMap::new(),
            children: vec![],
        };

        trace.add_span(span);
        assert_eq!(trace.spans.len(), 1);
        assert_eq!(trace.duration_ms(), 1000); // 1 second = 1000 ms
    }

    #[test]
    fn test_trace_with_spans() {
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
            duration_nano: 1_000_000_000, // 1 second
            attributes: HashMap::new(),
            resource: HashMap::new(),
            children: vec![],
        };

        let trace = Trace::new_with_spans("trace_id", vec![span]);
        assert_eq!(trace.trace_id, "trace_id");
        assert_eq!(trace.spans.len(), 1);
        assert_eq!(trace.duration_ms(), 1000); // 1 second = 1000 ms
    }

    #[test]
    fn test_root_span() {
        let root_span = Span {
            trace_id: "trace_id".to_string(),
            span_id: "root_span_id".to_string(),
            parent_span_id: "0000000000000000".to_string(),
            status: SpanStatus::Ok,
            is_root: true,
            name: "root".to_string(),
            service_name: "service_name".to_string(),
            span_kind: SpanKind::Client,
            start_time_unix_nano: 0,
            duration_nano: 1_000_000_000, // 1 second
            attributes: HashMap::new(),
            resource: HashMap::new(),
            children: vec![],
        };

        let child_span = Span {
            trace_id: "trace_id".to_string(),
            span_id: "child_span_id".to_string(),
            parent_span_id: "root_span_id".to_string(),
            status: SpanStatus::Ok,
            is_root: false,
            name: "child".to_string(),
            service_name: "service_name".to_string(),
            span_kind: SpanKind::Client,
            start_time_unix_nano: 0,
            duration_nano: 500_000_000, // 0.5 seconds
            attributes: HashMap::new(),
            resource: HashMap::new(),
            children: vec![],
        };

        let trace = Trace::new_with_spans("trace_id", vec![child_span.clone(), root_span.clone()]);
        assert_eq!(trace.root_span(), Some(&root_span));
        assert_eq!(trace.duration_ms(), 1000); // 1 second = 1000 ms
    }
}
