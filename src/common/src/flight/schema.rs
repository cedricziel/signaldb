use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};

/// Flight schemas for SignalDB
pub struct FlightSchemas {
    /// Schema for trace data
    pub trace_schema: Schema,
    /// Schema for log data
    pub log_schema: Schema,
    /// Schema for metric data
    pub metric_schema: Schema,
}

impl FlightSchemas {
    /// Create a new instance of FlightSchemas with all defined schemas
    pub fn new() -> Self {
        Self {
            trace_schema: Self::create_trace_schema(),
            log_schema: Self::create_log_schema(),
            metric_schema: Self::create_metric_schema(),
        }
    }

    /// Create the schema for trace data
    fn create_trace_schema() -> Schema {
        let fields = vec![
            // Core span fields
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            // Timing information
            Field::new("start_time_unix_nano", DataType::UInt64, false),
            Field::new("end_time_unix_nano", DataType::UInt64, false),
            Field::new("duration_nano", DataType::UInt64, false),
            // Span metadata
            Field::new("span_kind", DataType::Utf8, false),
            Field::new("status_code", DataType::Utf8, false),
            Field::new("status_message", DataType::Utf8, true),
            Field::new("is_root", DataType::Boolean, false),
            // Attributes and resources as JSON strings
            // Using JSON strings allows for flexible attribute storage
            // while maintaining compatibility with Arrow Flight
            Field::new("attributes_json", DataType::Utf8, true),
            Field::new("resource_json", DataType::Utf8, true),
            // Events as a nested list
            Field::new(
                "events",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(Fields::from(vec![
                        Field::new("name", DataType::Utf8, false),
                        Field::new("timestamp_unix_nano", DataType::UInt64, false),
                        Field::new("attributes_json", DataType::Utf8, true),
                    ])),
                    true,
                ))),
                true,
            ),
            // Links to other spans
            Field::new(
                "links",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(Fields::from(vec![
                        Field::new("trace_id", DataType::Utf8, false),
                        Field::new("span_id", DataType::Utf8, false),
                        Field::new("attributes_json", DataType::Utf8, true),
                    ])),
                    true,
                ))),
                true,
            ),
        ];

        Schema::new(Fields::from(fields))
    }

    /// Create the schema for log data based on OpenTelemetry log data model
    fn create_log_schema() -> Schema {
        let fields = vec![
            // Core log fields
            Field::new("time_unix_nano", DataType::UInt64, false),
            Field::new("observed_time_unix_nano", DataType::UInt64, false),
            Field::new("severity_number", DataType::Int32, true),
            Field::new("severity_text", DataType::Utf8, true),
            Field::new("body", DataType::Utf8, true), // JSON serialized AnyValue
            // Trace context
            Field::new("trace_id", DataType::Binary, true),
            Field::new("span_id", DataType::Binary, true),
            Field::new("flags", DataType::UInt32, true),
            // Attributes and resources
            Field::new("attributes_json", DataType::Utf8, true),
            Field::new("resource_json", DataType::Utf8, true),
            Field::new("scope_json", DataType::Utf8, true),
            Field::new("dropped_attributes_count", DataType::UInt32, true),
            // Service context
            Field::new("service_name", DataType::Utf8, true),
            // Event context (added in OpenTelemetry 0.29.0)
            Field::new("event_name", DataType::Utf8, true),
        ];

        Schema::new(Fields::from(fields))
    }

    /// Create the schema for metric data based on OpenTelemetry metric data model
    fn create_metric_schema() -> Schema {
        let fields = vec![
            // Core metric fields
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("unit", DataType::Utf8, true),
            // Common datapoint fields
            Field::new("start_time_unix_nano", DataType::UInt64, true),
            Field::new("time_unix_nano", DataType::UInt64, false),
            Field::new("attributes_json", DataType::Utf8, true),
            Field::new("resource_json", DataType::Utf8, true),
            Field::new("scope_json", DataType::Utf8, true),
            // Metric type
            Field::new("metric_type", DataType::Utf8, false), // "gauge", "sum", "histogram", "exponential_histogram", "summary"
            // Metric-specific fields (stored as JSON strings)
            Field::new("data_json", DataType::Utf8, false), // JSON serialized metric data
            // Aggregation temporality (for sum, histogram, exponential_histogram)
            Field::new("aggregation_temporality", DataType::Int32, true), // 0=unspecified, 1=delta, 2=cumulative
            // Monotonicity (for sum)
            Field::new("is_monotonic", DataType::Boolean, true),
        ];

        Schema::new(Fields::from(fields))
    }
}

/// Create a schema for a batch of spans
pub fn create_span_batch_schema() -> Schema {
    Schema::new(Fields::from(vec![
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
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flight_schemas() {
        let schemas = FlightSchemas::new();

        // Verify trace schema
        let trace_schema = schemas.trace_schema;
        assert!(trace_schema.field_with_name("trace_id").is_ok());
        assert!(trace_schema.field_with_name("span_id").is_ok());
        assert!(trace_schema.field_with_name("parent_span_id").is_ok());
        assert!(trace_schema.field_with_name("name").is_ok());
        assert!(trace_schema.field_with_name("service_name").is_ok());
        assert!(trace_schema.field_with_name("start_time_unix_nano").is_ok());
        assert!(trace_schema.field_with_name("end_time_unix_nano").is_ok());
        assert!(trace_schema.field_with_name("duration_nano").is_ok());
        assert!(trace_schema.field_with_name("span_kind").is_ok());
        assert!(trace_schema.field_with_name("status_code").is_ok());
        assert!(trace_schema.field_with_name("is_root").is_ok());
        assert!(trace_schema.field_with_name("attributes_json").is_ok());
        assert!(trace_schema.field_with_name("resource_json").is_ok());
        assert!(trace_schema.field_with_name("events").is_ok());
        assert!(trace_schema.field_with_name("links").is_ok());

        // Verify log schema
        let log_schema = schemas.log_schema;
        assert!(log_schema.field_with_name("time_unix_nano").is_ok());
        assert!(log_schema
            .field_with_name("observed_time_unix_nano")
            .is_ok());
        assert!(log_schema.field_with_name("severity_number").is_ok());
        assert!(log_schema.field_with_name("severity_text").is_ok());
        assert!(log_schema.field_with_name("body").is_ok());
        assert!(log_schema.field_with_name("trace_id").is_ok());
        assert!(log_schema.field_with_name("span_id").is_ok());
        assert!(log_schema.field_with_name("flags").is_ok());
        assert!(log_schema.field_with_name("attributes_json").is_ok());
        assert!(log_schema.field_with_name("resource_json").is_ok());
        assert!(log_schema.field_with_name("scope_json").is_ok());
        assert!(log_schema.field_with_name("service_name").is_ok());

        // Verify metric schema
        let metric_schema = schemas.metric_schema;
        assert!(metric_schema.field_with_name("name").is_ok());
        assert!(metric_schema.field_with_name("description").is_ok());
        assert!(metric_schema.field_with_name("unit").is_ok());
        assert!(metric_schema
            .field_with_name("start_time_unix_nano")
            .is_ok());
        assert!(metric_schema.field_with_name("time_unix_nano").is_ok());
        assert!(metric_schema.field_with_name("attributes_json").is_ok());
        assert!(metric_schema.field_with_name("resource_json").is_ok());
        assert!(metric_schema.field_with_name("scope_json").is_ok());
        assert!(metric_schema.field_with_name("metric_type").is_ok());
        assert!(metric_schema.field_with_name("data_json").is_ok());
        assert!(metric_schema
            .field_with_name("aggregation_temporality")
            .is_ok());
        assert!(metric_schema.field_with_name("is_monotonic").is_ok());
    }

    #[test]
    fn test_span_batch_schema() {
        let schema = create_span_batch_schema();

        assert!(schema.field_with_name("trace_id").is_ok());
        assert!(schema.field_with_name("span_id").is_ok());
        assert!(schema.field_with_name("parent_span_id").is_ok());
        assert!(schema.field_with_name("status").is_ok());
        assert!(schema.field_with_name("is_root").is_ok());
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("service_name").is_ok());
        assert!(schema.field_with_name("span_kind").is_ok());
        assert!(schema.field_with_name("start_time_unix_nano").is_ok());
        assert!(schema.field_with_name("duration_nano").is_ok());
    }
}
