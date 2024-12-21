use super::span::Span;

pub struct Trace {
    pub trace_id: String,
    pub spans: Vec<Span>,
}

impl Into<tempo_api::Trace> for Trace {
    fn into(self) -> tempo_api::Trace {
        // Find the root span (the one with is_root = true)
        let root_span = self.spans.iter().find(|s| s.is_root).unwrap_or(&self.spans[0]);
        
        // Calculate duration in milliseconds
        let duration_ms = root_span.duration_nano / 1_000_000;

        tempo_api::Trace {
            trace_id: self.trace_id,
            root_service_name: root_span.service_name.clone(),
            root_trace_name: root_span.name.clone(),
            start_time_unix_nano: root_span.start_time_unix_nano.to_string(),
            duration_ms,
            span_sets: vec![],
        }
    }
}
