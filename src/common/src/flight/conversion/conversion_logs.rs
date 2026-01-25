use datafusion::arrow::array::{
    ArrayRef, BinaryArray, Int32Array, StringArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::KeyValue,
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use serde_json::Map;
use std::sync::Arc;

use crate::flight::conversion::conversion_common::{
    extract_resource_json, extract_service_name, extract_value, json_value_to_any_value,
};
use crate::flight::schema::FlightSchemas;

use super::extract_scope_json;

/// Convert OTLP log data to Arrow RecordBatch using the Flight log schema
pub fn otlp_logs_to_arrow(request: &ExportLogsServiceRequest) -> RecordBatch {
    let schemas = FlightSchemas::new();
    let schema = schemas.log_schema.clone();

    // Extract logs from the request
    let mut times = Vec::new();
    let mut observed_times = Vec::new();
    let mut severity_numbers = Vec::new();
    let mut severity_texts = Vec::new();
    let mut bodies = Vec::new();
    let mut trace_ids = Vec::new();
    let mut span_ids = Vec::new();
    let mut flags = Vec::new();
    let mut attributes_jsons = Vec::new();
    let mut resource_jsons = Vec::new();
    let mut scope_jsons = Vec::new();
    let mut dropped_attributes_counts = Vec::new();
    let mut service_names = Vec::new();
    let mut event_names = Vec::new();

    for resource_logs in &request.resource_logs {
        // Extract resource attributes as JSON
        let resource_json = extract_resource_json(&resource_logs.resource);

        // Extract service name from resource attributes
        let service_name = extract_service_name(&resource_logs.resource);

        for scope_logs in &resource_logs.scope_logs {
            // Extract scope attributes as JSON
            let scope_json = if let Some(scope) = &scope_logs.scope {
                extract_scope_json(&Some(scope.clone()))
            } else {
                "{}".to_string()
            };

            for log in &scope_logs.log_records {
                // Extract log attributes as JSON
                let mut attr_map = Map::new();
                for attr in &log.attributes {
                    attr_map.insert(attr.key.clone(), extract_value(&attr.value));
                }
                let attributes_json =
                    serde_json::to_string(&attr_map).unwrap_or_else(|_| "{}".to_string());

                // Extract body as JSON
                let body_json = if let Some(body) = &log.body {
                    serde_json::to_string(&extract_value(&Some(body.clone())))
                        .unwrap_or_else(|_| "null".to_string())
                } else {
                    "null".to_string()
                };

                // Extract severity - convert from SeverityNumber enum to i32
                let severity_number = log.severity_number as u32 as i32;
                let severity_text = log.severity_text.clone();

                // Extract event name
                let event_name = log.event_name.clone();

                // Add to arrays
                times.push(log.time_unix_nano);
                observed_times.push(log.observed_time_unix_nano);
                severity_numbers.push(severity_number);
                severity_texts.push(severity_text);
                bodies.push(body_json);
                trace_ids.push(log.trace_id.clone());
                span_ids.push(log.span_id.clone());
                flags.push(log.flags);
                attributes_jsons.push(attributes_json);
                resource_jsons.push(resource_json.clone());
                scope_jsons.push(scope_json.clone());
                dropped_attributes_counts.push(log.dropped_attributes_count);
                service_names.push(service_name.clone());
                event_names.push(event_name);
            }
        }
    }

    // Create Arrow arrays from the extracted data
    let time_array: ArrayRef = Arc::new(UInt64Array::from(times));
    let observed_time_array: ArrayRef = Arc::new(UInt64Array::from(observed_times));
    let severity_number_array: ArrayRef = Arc::new(Int32Array::from(severity_numbers));
    let severity_text_array: ArrayRef = Arc::new(StringArray::from(severity_texts));
    let body_array: ArrayRef = Arc::new(StringArray::from(bodies));

    // Create binary arrays for trace_id and span_id
    let trace_id_array: ArrayRef =
        Arc::new(BinaryArray::from_iter(trace_ids.into_iter().map(Some)));
    let span_id_array: ArrayRef = Arc::new(BinaryArray::from_iter(span_ids.into_iter().map(Some)));

    let flags_array: ArrayRef = Arc::new(UInt32Array::from(flags));
    let attributes_json_array: ArrayRef = Arc::new(StringArray::from(attributes_jsons));
    let resource_json_array: ArrayRef = Arc::new(StringArray::from(resource_jsons));
    let scope_json_array: ArrayRef = Arc::new(StringArray::from(scope_jsons));
    let dropped_attributes_count_array: ArrayRef =
        Arc::new(UInt32Array::from(dropped_attributes_counts));
    let service_name_array: ArrayRef = Arc::new(StringArray::from(service_names));
    let event_name_array: ArrayRef = Arc::new(StringArray::from(event_names));

    // Clone schema for potential error case
    let schema_clone = schema.clone();

    // Create and return the RecordBatch
    let result = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            time_array,
            observed_time_array,
            severity_number_array,
            severity_text_array,
            body_array,
            trace_id_array,
            span_id_array,
            flags_array,
            attributes_json_array,
            resource_json_array,
            scope_json_array,
            dropped_attributes_count_array,
            service_name_array,
            event_name_array,
        ],
    );

    result.unwrap_or_else(|_| RecordBatch::new_empty(Arc::new(schema_clone)))
}

/// Convert Arrow RecordBatch to OTLP ExportLogsServiceRequest
pub fn arrow_to_otlp_logs(batch: &RecordBatch) -> ExportLogsServiceRequest {
    use std::collections::HashMap;

    let columns = batch.columns();

    // Extract columns by index based on the schema order in otlp_logs_to_arrow
    let time_array = columns[0]
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("time_unix_nano column should be UInt64Array");
    let observed_time_array = columns[1]
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("observed_time_unix_nano column should be UInt64Array");
    let severity_number_array = columns[2]
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("severity_number column should be Int32Array");
    let severity_text_array = columns[3]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("severity_text column should be StringArray");
    let body_array = columns[4]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("body column should be StringArray");
    let trace_id_array = columns[5]
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("trace_id column should be BinaryArray");
    let span_id_array = columns[6]
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("span_id column should be BinaryArray");
    let flags_array = columns[7]
        .as_any()
        .downcast_ref::<UInt32Array>()
        .expect("flags column should be UInt32Array");
    let attributes_json_array = columns[8]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("attributes_json column should be StringArray");
    let resource_json_array = columns[9]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("resource_json column should be StringArray");
    let scope_json_array = columns[10]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("scope_json column should be StringArray");
    let dropped_attributes_count_array = columns[11]
        .as_any()
        .downcast_ref::<UInt32Array>()
        .expect("dropped_attributes_count column should be UInt32Array");
    let service_name_array = columns[12]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("service_name column should be StringArray");
    let event_name_array = columns[13]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("event_name column should be StringArray");

    // Group logs by resource and scope
    let mut resource_scope_logs_map: HashMap<String, HashMap<String, Vec<LogRecord>>> =
        HashMap::new();
    let mut resource_attributes_map: HashMap<String, Vec<KeyValue>> = HashMap::new();
    let mut scope_attributes_map: HashMap<
        String,
        opentelemetry_proto::tonic::common::v1::InstrumentationScope,
    > = HashMap::new();

    for row in 0..batch.num_rows() {
        let time_unix_nano = time_array.value(row);
        let observed_time_unix_nano = observed_time_array.value(row);
        let severity_number = severity_number_array.value(row);
        let severity_text = severity_text_array.value(row).to_string();
        let body_json = body_array.value(row);
        let trace_id = trace_id_array.value(row).to_vec();
        let span_id = span_id_array.value(row).to_vec();
        let flags = flags_array.value(row);
        let attributes_json_str = attributes_json_array.value(row);
        let resource_json_str = resource_json_array.value(row);
        let scope_json_str = scope_json_array.value(row);
        let dropped_attributes_count = dropped_attributes_count_array.value(row);
        let _service_name = service_name_array.value(row).to_string();
        let event_name = event_name_array.value(row).to_string();

        // Parse body JSON string to AnyValue
        let body = if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(body_json) {
            Some(json_value_to_any_value(&json_val))
        } else {
            None
        };

        // Parse attributes JSON string to KeyValue vector
        let attributes: Vec<KeyValue> = if let Ok(serde_json::Value::Object(map)) =
            serde_json::from_str::<serde_json::Value>(attributes_json_str)
        {
            map.into_iter()
                .map(|(k, v)| KeyValue {
                    key: k,
                    value: Some(json_value_to_any_value(&v)),
                })
                .collect()
        } else {
            vec![]
        };

        // Parse resource JSON string to KeyValue vector
        let resource_attributes: Vec<KeyValue> = if let Ok(serde_json::Value::Object(map)) =
            serde_json::from_str::<serde_json::Value>(resource_json_str)
        {
            map.into_iter()
                .map(|(k, v)| KeyValue {
                    key: k,
                    value: Some(json_value_to_any_value(&v)),
                })
                .collect()
        } else {
            vec![]
        };

        // Parse scope JSON string to InstrumentationScope
        let scope = if let Ok(serde_json::Value::Object(map)) =
            serde_json::from_str::<serde_json::Value>(scope_json_str)
        {
            let name = map
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let version = map
                .get("version")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let mut scope_attributes = Vec::new();
            if let Some(serde_json::Value::Object(attrs_map)) = map.get("attributes") {
                for (k, v) in attrs_map {
                    scope_attributes.push(KeyValue {
                        key: k.clone(),
                        value: Some(json_value_to_any_value(v)),
                    });
                }
            }

            opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                name,
                version,
                attributes: scope_attributes,
                dropped_attributes_count: 0,
            }
        } else {
            opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                name: "".to_string(),
                version: "".to_string(),
                attributes: vec![],
                dropped_attributes_count: 0,
            }
        };

        // Construct the LogRecord
        let log_record = LogRecord {
            time_unix_nano,
            observed_time_unix_nano,
            severity_number, // i32 to SeverityNumber enum
            severity_text,
            body,
            attributes,
            dropped_attributes_count,
            flags,
            trace_id,
            span_id,
            event_name,
        };

        // Use resource_json_str and scope_json_str as keys for grouping
        let scope_logs_map = resource_scope_logs_map
            .entry(resource_json_str.to_string())
            .or_default();

        let logs = scope_logs_map
            .entry(scope_json_str.to_string())
            .or_default();

        logs.push(log_record);

        // Store resource attributes for this resource
        resource_attributes_map.insert(resource_json_str.to_string(), resource_attributes);

        // Store scope for this scope
        scope_attributes_map.insert(scope_json_str.to_string(), scope);
    }

    // Construct ResourceLogs
    let mut resource_logs = Vec::new();
    for (resource_key, scope_logs_map) in resource_scope_logs_map {
        let mut scope_logs_vec = Vec::new();

        for (scope_key, logs) in scope_logs_map {
            let scope_logs = ScopeLogs {
                scope: Some(scope_attributes_map.get(&scope_key).unwrap().clone()),
                log_records: logs,
                schema_url: "".to_string(),
            };

            scope_logs_vec.push(scope_logs);
        }

        let resource_logs_entry = ResourceLogs {
            resource: Some(Resource {
                attributes: resource_attributes_map.get(&resource_key).unwrap().clone(),
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: scope_logs_vec,
            schema_url: "".to_string(),
        };

        resource_logs.push(resource_logs_entry);
    }

    ExportLogsServiceRequest { resource_logs }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        BinaryArray, Int32Array, StringArray, UInt32Array, UInt64Array,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use std::sync::Arc;

    #[test]
    fn test_arrow_to_otlp_logs() {
        // Create a simple log in Arrow format
        let schema = Arc::new(Schema::new(vec![
            Field::new("time_unix_nano", DataType::UInt64, false),
            Field::new("observed_time_unix_nano", DataType::UInt64, false),
            Field::new("severity_number", DataType::Int32, true),
            Field::new("severity_text", DataType::Utf8, true),
            Field::new("body", DataType::Utf8, true),
            Field::new("trace_id", DataType::Binary, true),
            Field::new("span_id", DataType::Binary, true),
            Field::new("flags", DataType::UInt32, true),
            Field::new("attributes_json", DataType::Utf8, true),
            Field::new("resource_json", DataType::Utf8, true),
            Field::new("scope_json", DataType::Utf8, true),
            Field::new("dropped_attributes_count", DataType::UInt32, true),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("event_name", DataType::Utf8, true),
        ]));

        // Sample data for a log
        let time_array = UInt64Array::from(vec![1000000000]);
        let observed_time_array = UInt64Array::from(vec![1100000000]);
        let severity_number_array = Int32Array::from(vec![9]); // ERROR
        let severity_text_array = StringArray::from(vec!["ERROR"]);
        let body_array = StringArray::from(vec![r#""This is a test log message""#]);

        // Create binary arrays for trace_id and span_id
        let trace_id = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let span_id = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let trace_id_array = BinaryArray::from_iter(vec![Some(trace_id.clone())]);
        let span_id_array = BinaryArray::from_iter(vec![Some(span_id.clone())]);

        let flags_array = UInt32Array::from(vec![1]);
        let attributes_json_array = StringArray::from(vec!["{\"attr1\":\"value1\"}"]);
        let resource_json_array = StringArray::from(vec!["{\"service.name\":\"test_service\"}"]);
        let scope_json_array =
            StringArray::from(vec!["{\"name\":\"test_scope\",\"version\":\"1.0\"}"]);
        let dropped_attributes_count_array = UInt32Array::from(vec![0]);
        let service_name_array = StringArray::from(vec!["test_service"]);
        let event_name_array = StringArray::from(vec![""]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(time_array),
                Arc::new(observed_time_array),
                Arc::new(severity_number_array),
                Arc::new(severity_text_array),
                Arc::new(body_array),
                Arc::new(trace_id_array),
                Arc::new(span_id_array),
                Arc::new(flags_array),
                Arc::new(attributes_json_array),
                Arc::new(resource_json_array),
                Arc::new(scope_json_array),
                Arc::new(dropped_attributes_count_array),
                Arc::new(service_name_array),
                Arc::new(event_name_array),
            ],
        )
        .unwrap();

        // Convert Arrow to OTLP
        let result = arrow_to_otlp_logs(&batch);

        // Verify the result
        assert_eq!(result.resource_logs.len(), 1);
        let resource_logs = &result.resource_logs[0];

        // Verify resource
        assert!(resource_logs.resource.is_some());
        let resource = resource_logs.resource.as_ref().unwrap();
        assert_eq!(resource.attributes.len(), 1);
        assert_eq!(resource.attributes[0].key, "service.name");

        // Verify scope logs
        assert_eq!(resource_logs.scope_logs.len(), 1);
        let scope_logs = &resource_logs.scope_logs[0];

        // Verify scope
        assert!(scope_logs.scope.is_some());
        let scope = scope_logs.scope.as_ref().unwrap();
        assert_eq!(scope.name, "test_scope");
        assert_eq!(scope.version, "1.0");

        // Verify logs
        assert_eq!(scope_logs.log_records.len(), 1);
        let log = &scope_logs.log_records[0];

        // Verify log properties
        assert_eq!(log.time_unix_nano, 1000000000);
        assert_eq!(log.observed_time_unix_nano, 1100000000);
        assert_eq!(log.severity_number as i32, 9); // ERROR
        assert_eq!(log.severity_text, "ERROR");
        assert_eq!(log.trace_id, trace_id);
        assert_eq!(log.span_id, span_id);
        assert_eq!(log.flags, 1);

        // Verify body
        assert!(log.body.is_some());
        if let Some(body) = &log.body {
            if let Some(Value::StringValue(value)) = &body.value {
                assert_eq!(value, "This is a test log message");
            } else {
                panic!("Expected string value for body");
            }
        } else {
            panic!("Expected body to be present");
        }

        // Verify attributes
        assert_eq!(log.attributes.len(), 1);
        assert_eq!(log.attributes[0].key, "attr1");
    }
}
