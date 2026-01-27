use std::{collections::HashMap, str::FromStr, sync::Arc};

use datafusion::arrow::{
    array::{Array, ArrayRef, BooleanArray, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};

/// Sentinel value representing an empty/absent parent span ID in OTLP traces.
/// Root spans use this value (all zero bytes in the 8-byte span ID) to indicate
/// they have no parent.
pub const EMPTY_PARENT_SPAN_ID: &str = "00000000";

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
/// Links child spans to their parents and returns the root spans (those marked
/// `is_root` or with an empty/absent parent span ID). The input `span_map` is
/// consumed and mutated in place so that parent spans contain their children.
pub fn build_span_hierarchy(mut span_map: HashMap<String, Span>) -> Vec<Span> {
    // Collect parent-child relationships
    let mut parent_child_pairs = Vec::new();
    for span in span_map.values() {
        if !is_root_parent_id(&span.parent_span_id) {
            parent_child_pairs.push((span.parent_span_id.clone(), span.span_id.clone()));
        }
    }

    // Group children by parent
    let mut child_spans: HashMap<String, Vec<Span>> = HashMap::new();
    for (parent_span_id, span_id) in parent_child_pairs {
        if let Some(child_span) = span_map.get(&span_id) {
            child_spans
                .entry(parent_span_id)
                .or_default()
                .push(child_span.clone());
        }
    }

    // Attach children to parents
    for (parent_span_id, children) in child_spans {
        if let Some(parent_span) = span_map.get_mut(&parent_span_id) {
            parent_span.children.extend(children);
        }
    }

    // Collect root spans
    span_map
        .into_values()
        .filter(|span| span.is_root || is_root_parent_id(&span.parent_span_id))
        .collect()
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
        });

        let record_batch = span_batch.to_record_batch();
        assert_eq!(record_batch.num_columns(), 12);
        assert_eq!(record_batch.num_rows(), 1);

        let span_batch = SpanBatch::from_record_batch(&record_batch);
        assert_eq!(span_batch.spans.len(), 1);
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
