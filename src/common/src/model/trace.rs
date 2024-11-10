use super::span::Span;

pub struct Trace {
    pub trace_id: String,
    pub spans: Vec<Span>,
}

impl Into<tempo_api::Trace> for Trace {
    fn into(self) -> tempo_api::Trace {
        tempo_api::Trace {
            trace_id: self.trace_id,
            // retrieve the service name from the first span or fall back to "unknown"
            root_service_name: self.spans[0].service_name.clone(),
            root_trace_name: self.spans[0].name.clone(),
            start_time_unix_nano: todo!(),
            duration_ms: todo!(),
            span_sets: vec![],
        }
    }
}
