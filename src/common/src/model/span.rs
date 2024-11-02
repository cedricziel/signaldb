use std::{collections::HashMap, ptr::null, sync::Arc};

use arrow_array::{ArrayRef, BooleanArray, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field, Fields, Schema};

#[derive(Clone)]
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

#[derive(Clone)]
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
            // Field::new(
            //     "attributes",
            //     DataType::Struct(Fields::from(Vec::<Field>::new())),
            //     false,
            // ),
            // Field::new(
            //     "resource",
            //     DataType::Struct(Fields::from(Vec::<Field>::new())),
            //     false,
            // ),
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
        // let attributes: ArrayRef = Arc::new(StructArray::from(vec![]));
        // let resource: ArrayRef = Arc::new(StructArray::from(vec![null()]));

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
                // attributes,
                // resource,
            ],
        )
        .unwrap()
    }
}

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
            attributes: HashMap::new(),
            resource: HashMap::new(),
        };

        let record_batch = span.to_record_batch();
        assert_eq!(record_batch.num_columns(), 8);
        assert_eq!(record_batch.num_rows(), 1);
    }
}
