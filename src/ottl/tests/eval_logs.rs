use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use ottl::{ErrorMode, Limits, Signal, compile};

fn one_log_request(body: &str, attributes: Vec<KeyValue>) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource::default()),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope::default()),
                log_records: vec![LogRecord {
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(body.to_string())),
                    }),
                    severity_text: "INFO".to_string(),
                    attributes,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn body_str(record: &opentelemetry_proto::tonic::logs::v1::LogRecord) -> Option<&str> {
    record.body.as_ref().and_then(|v| match &v.value {
        Some(any_value::Value::StringValue(s)) => Some(s.as_str()),
        _ => None,
    })
}

#[test]
fn span_name_fails_to_compile_for_logs_naming_span_name() {
    let errors = compile(
        Signal::Logs,
        &[r#"set(span.name, "x")"#.to_string()],
        &Limits::default(),
    )
    .unwrap_err();
    assert!(!errors.is_empty());
    assert!(errors[0].message.to_lowercase().contains("span"));
}

#[test]
fn bare_body_editable_as_string() {
    let program = compile(
        Signal::Logs,
        &[r#"set(body, "redacted")"#.to_string()],
        &Limits::default(),
    )
    .expect("compiles");
    let mut req = one_log_request("secret payload", vec![]);
    program
        .apply_logs(&mut req, ErrorMode::Propagate)
        .expect("applies");
    let record = &req.resource_logs[0].scope_logs[0].log_records[0];
    assert_eq!(body_str(record), Some("redacted"));
}

#[test]
fn log_dot_body_and_severity_fields() {
    let program = compile(
        Signal::Logs,
        &[
            r#"set(log.severity_text, "DEBUG")"#.to_string(),
            r#"set(log.severity_number, 5)"#.to_string(),
        ],
        &Limits::default(),
    )
    .expect("compiles");
    let mut req = one_log_request("hello", vec![]);
    program
        .apply_logs(&mut req, ErrorMode::Propagate)
        .expect("applies");
    let record = &req.resource_logs[0].scope_logs[0].log_records[0];
    assert_eq!(record.severity_text, "DEBUG");
    assert_eq!(record.severity_number, 5);
}

#[test]
fn where_on_non_string_body_compares_as_non_equal() {
    // A non-string body (here: none set at all, so Nil) must never equal a string.
    let program = compile(
        Signal::Logs,
        &[r#"set(attributes["matched"], "yes") where body == "hello""#.to_string()],
        &Limits::default(),
    )
    .expect("compiles");
    let mut req = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource::default()),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope::default()),
                log_records: vec![LogRecord {
                    body: Some(AnyValue {
                        value: Some(any_value::Value::IntValue(42)),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let report = program
        .apply_logs(&mut req, ErrorMode::Propagate)
        .expect("applies");
    assert_eq!(report.statements[0].matched, 0);
}

#[test]
fn log_attributes_map_editors() {
    let program = compile(
        Signal::Logs,
        &[r#"delete_key(attributes, "secret")"#.to_string()],
        &Limits::default(),
    )
    .expect("compiles");
    let mut req = one_log_request(
        "hi",
        vec![KeyValue {
            key: "secret".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue("shh".to_string())),
            }),
            ..Default::default()
        }],
    );
    program
        .apply_logs(&mut req, ErrorMode::Propagate)
        .expect("applies");
    let record = &req.resource_logs[0].scope_logs[0].log_records[0];
    assert!(record.attributes.is_empty());
}
