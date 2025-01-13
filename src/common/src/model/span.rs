use std::{collections::HashMap, sync::Arc};

use arrow_array::{ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum SpanKind {
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

impl SpanKind {
    pub fn from_str(s: &str) -> Self {
        match s {
            "Internal" => SpanKind::Internal,
            "Server" => SpanKind::Server,
            "Client" => SpanKind::Client,
            "Producer" => SpanKind::Producer,
            "Consumer" => SpanKind::Consumer,
            _ => SpanKind::Internal,
        }
    }

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

impl SpanStatus {
    pub fn from_str(s: &str) -> Self {
        match s {
            "Unspecified" => SpanStatus::Unspecified,
            "Ok" => SpanStatus::Ok,
            "Error" => SpanStatus::Error,
            _ => SpanStatus::Unspecified,
        }
    }

    pub fn to_str(&self) -> &str {
        match self {
            SpanStatus::Unspecified => "Unspecified",
            SpanStatus::Ok => "Ok",
            SpanStatus::Error => "Error",
        }
    }
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
}

impl Span {
    pub fn to_schema() -> arrow_schema::Schema {
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
        let start_time_unix_nano: ArrayRef = Arc::new(arrow_array::UInt64Array::from(vec![
            self.start_time_unix_nano,
        ]));
        let duration_nano: ArrayRef =
            Arc::new(arrow_array::UInt64Array::from(vec![self.duration_nano]));

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
            ],
        )
        .unwrap()
    }
}

/// A batch of spans.
///
/// Supposedly making it easier to convert to a record batch.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpanBatch {
    pub spans: Vec<Span>,
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
                .downcast_ref::<arrow_array::UInt64Array>()
                .unwrap();
            let duration_nano = batch
                .column(9)
                .as_any()
                .downcast_ref::<arrow_array::UInt64Array>()
                .unwrap();

            let span = Span {
                trace_id: trace_id.value(i).to_string(),
                span_id: span_id.value(i).to_string(),
                parent_span_id: parent_span_id.value(i).to_string(),
                status: SpanStatus::from_str(status.value(i)),
                is_root: is_root.value(i),
                name: name.value(i).to_string(),
                service_name: service_name.value(i).to_string(),
                span_kind: SpanKind::from_str(span_kind.value(i)),
                start_time_unix_nano: start_time_unix_nano.value(i),
                duration_nano: duration_nano.value(i),
                attributes: HashMap::new(),
                resource: HashMap::new(),
                children: vec![],
            };

            span_batch.add_span(span);
        }

        span_batch
    }
}

impl From<arrow_array::RecordBatch> for SpanBatch {
    fn from(batch: RecordBatch) -> Self {
        SpanBatch::from_record_batch(&batch)
    }
}

impl From<&arrow_array::RecordBatch> for SpanBatch {
    fn from(batch: &RecordBatch) -> Self {
        SpanBatch::from_record_batch(&batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        };

        let record_batch = span.to_record_batch();
        assert_eq!(record_batch.num_columns(), 10);
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
        });

        let record_batch = span_batch.to_record_batch();
        assert_eq!(record_batch.num_columns(), 10);
        assert_eq!(record_batch.num_rows(), 1);

        let span_batch = SpanBatch::from_record_batch(&record_batch);
        assert_eq!(span_batch.spans.len(), 1);
    }

    #[test]
    fn test_span_kind() {
        assert_eq!(SpanKind::from_str("Internal"), SpanKind::Internal);
        assert_eq!(SpanKind::from_str("Server"), SpanKind::Server);
        assert_eq!(SpanKind::from_str("Client"), SpanKind::Client);
        assert_eq!(SpanKind::from_str("Producer"), SpanKind::Producer);
        assert_eq!(SpanKind::from_str("Consumer"), SpanKind::Consumer);

        assert_eq!(SpanKind::Internal.to_str(), "Internal");
        assert_eq!(SpanKind::Server.to_str(), "Server");
        assert_eq!(SpanKind::Client.to_str(), "Client");
        assert_eq!(SpanKind::Producer.to_str(), "Producer");
        assert_eq!(SpanKind::Consumer.to_str(), "Consumer");
    }

    #[test]
    fn test_span_status() {
        assert_eq!(SpanStatus::from_str("Unspecified"), SpanStatus::Unspecified);
        assert_eq!(SpanStatus::from_str("Ok"), SpanStatus::Ok);
        assert_eq!(SpanStatus::from_str("Error"), SpanStatus::Error);

        assert_eq!(SpanStatus::Unspecified.to_str(), "Unspecified");
        assert_eq!(SpanStatus::Ok.to_str(), "Ok");
        assert_eq!(SpanStatus::Error.to_str(), "Error");
    }

    #[test]
    fn test_span_schema() {
        let schema = Span::to_schema();
        assert_eq!(schema.fields().len(), 10);
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
    fn test_span_batch_from_record_batch() {
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
        };

        let record_batch = span.to_record_batch();
        let span_batch = SpanBatch::from_record_batch(&record_batch);
        assert_eq!(span_batch.spans.len(), 1);
    }
}
