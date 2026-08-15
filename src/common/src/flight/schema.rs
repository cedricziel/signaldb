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
    /// Schema for profile data
    pub profile_schema: Schema,
}

impl Default for FlightSchemas {
    fn default() -> Self {
        Self::new()
    }
}

impl FlightSchemas {
    /// Create a new instance of FlightSchemas with all defined schemas
    pub fn new() -> Self {
        Self {
            trace_schema: Self::create_trace_schema(),
            log_schema: Self::create_log_schema(),
            metric_schema: Self::create_metric_schema(),
            profile_schema: Self::create_profile_schema(),
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
            // Scope and resource metadata
            Field::new("trace_state", DataType::Utf8, true),
            Field::new("resource_schema_url", DataType::Utf8, true),
            Field::new("scope_name", DataType::Utf8, true),
            Field::new("scope_version", DataType::Utf8, true),
            Field::new("scope_schema_url", DataType::Utf8, true),
            Field::new("scope_attributes", DataType::Utf8, true),
            // Numeric OTel source of truth for span_kind/status_code
            // (issue #1208): span_kind/status_code above remain derived
            // display strings, computed from these at write time, never
            // the reverse.
            Field::new("span_kind_number", DataType::Int32, true),
            Field::new("status_code_number", DataType::Int32, true),
            // Preserved verbatim from the OTel span rather than discarded.
            Field::new("dropped_attributes_count", DataType::Int64, true),
            Field::new("dropped_events_count", DataType::Int64, true),
            Field::new("dropped_links_count", DataType::Int64, true),
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

    /// Create the schema for profile data based on the OpenTelemetry profiles
    /// data model (`v1development`), with the request-level dictionary resolved
    /// so each row is self-contained
    fn create_profile_schema() -> Schema {
        let fields = vec![
            // Identity and timing
            Field::new("profile_id", DataType::Binary, false),
            Field::new("time_unix_nano", DataType::UInt64, false),
            Field::new("duration_nano", DataType::UInt64, false),
            // Sample value type/unit (resolved from the string table)
            Field::new("sample_type_type", DataType::Utf8, false),
            Field::new("sample_type_unit", DataType::Utf8, false),
            // Sampling period
            Field::new("period", DataType::Int64, true),
            Field::new("period_type_type", DataType::Utf8, true),
            Field::new("period_type_unit", DataType::Utf8, true),
            // Service context
            Field::new("service_name", DataType::Utf8, false),
            // Resolved stack traces and samples as JSON documents
            Field::new("stacktraces_json", DataType::Utf8, false),
            Field::new("samples_json", DataType::Utf8, false),
            // Attributes and resources as JSON strings
            Field::new("resource_json", DataType::Utf8, true),
            Field::new("scope_json", DataType::Utf8, true),
            Field::new("attributes_json", DataType::Utf8, true),
            // Trace correlation (primary link)
            Field::new("trace_id", DataType::Binary, true),
            Field::new("span_id", DataType::Binary, true),
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
        Field::new("status_code", DataType::Utf8, false),
        Field::new("is_root", DataType::Boolean, false),
        Field::new("span_name", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("span_kind", DataType::Utf8, false),
        Field::new("start_time_unix_nano", DataType::UInt64, false),
        Field::new("duration_nano", DataType::UInt64, false),
        Field::new("span_attributes", DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        // Span events (annotations, exceptions) as a JSON string; carries the
        // reason for a failure on the single-trace path. Nullable/absent for
        // spans with no events or older payloads.
        Field::new("events", DataType::Utf8, true),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_schema_identity_fields_are_non_nullable_utf8() {
        let schema = FlightSchemas::new().trace_schema;

        let trace_id = schema.field_with_name("trace_id").unwrap();
        assert_eq!(trace_id.data_type(), &DataType::Utf8);
        assert!(!trace_id.is_nullable());

        let span_id = schema.field_with_name("span_id").unwrap();
        assert_eq!(span_id.data_type(), &DataType::Utf8);
        assert!(!span_id.is_nullable());
    }

    #[test]
    fn trace_schema_duration_is_non_nullable_uint64() {
        let schema = FlightSchemas::new().trace_schema;

        let duration = schema.field_with_name("duration_nano").unwrap();
        assert_eq!(duration.data_type(), &DataType::UInt64);
        assert!(!duration.is_nullable());
    }

    #[test]
    fn log_and_profile_schemas_encode_trace_ids_as_nullable_binary() {
        // Unlike trace_schema (trace_id: non-nullable Utf8), logs and
        // profiles carry the correlation id as raw, optional bytes --
        // conversion code must not assume one trace-id representation.
        let schemas = FlightSchemas::new();

        let log_trace_id = schemas.log_schema.field_with_name("trace_id").unwrap();
        assert_eq!(log_trace_id.data_type(), &DataType::Binary);
        assert!(log_trace_id.is_nullable());

        let profile_trace_id = schemas.profile_schema.field_with_name("trace_id").unwrap();
        assert_eq!(profile_trace_id.data_type(), &DataType::Binary);
        assert!(profile_trace_id.is_nullable());

        let log_span_id = schemas.log_schema.field_with_name("span_id").unwrap();
        assert_eq!(log_span_id.data_type(), &DataType::Binary);
        assert!(log_span_id.is_nullable());
    }

    #[test]
    fn profile_schema_identity_field_is_non_nullable_while_correlation_fields_are_nullable() {
        let schema = FlightSchemas::new().profile_schema;

        let profile_id = schema.field_with_name("profile_id").unwrap();
        assert_eq!(profile_id.data_type(), &DataType::Binary);
        assert!(!profile_id.is_nullable());
        assert!(schema.field_with_name("trace_id").unwrap().is_nullable());
        let span_id = schema.field_with_name("span_id").unwrap();
        assert_eq!(span_id.data_type(), &DataType::Binary);
        assert!(span_id.is_nullable());
    }

    #[test]
    fn metric_schema_data_json_is_non_nullable() {
        // data_json carries the metric's typed payload (gauge/sum/histogram);
        // it must always be present, unlike the optional attribute/resource JSON.
        let schema = FlightSchemas::new().metric_schema;

        let data_json = schema.field_with_name("data_json").unwrap();
        assert_eq!(data_json.data_type(), &DataType::Utf8);
        assert!(!data_json.is_nullable());
    }

    #[test]
    fn test_span_batch_schema() {
        let schema = create_span_batch_schema();

        assert!(schema.field_with_name("trace_id").is_ok());
        assert!(schema.field_with_name("span_id").is_ok());
        assert!(schema.field_with_name("parent_span_id").is_ok());
        assert!(schema.field_with_name("status_code").is_ok());
        assert!(schema.field_with_name("is_root").is_ok());
        assert!(schema.field_with_name("span_name").is_ok());
        assert!(schema.field_with_name("service_name").is_ok());
        assert!(schema.field_with_name("span_kind").is_ok());
        assert!(schema.field_with_name("start_time_unix_nano").is_ok());
        assert!(schema.field_with_name("duration_nano").is_ok());
        assert!(schema.field_with_name("span_attributes").is_ok());
        assert!(schema.field_with_name("resource_attributes").is_ok());
    }
}
