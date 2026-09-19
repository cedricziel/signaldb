//! Verifies the generated client exposes the seven tenant OTTL processor
//! operations (openspec change `tenant-ottl-processors`, task 4.5).
//!
//! Compile-and-construct assertions: the surface is generated from
//! `api/signaldb-api.json`, so if a processors endpoint drops out of the
//! OpenAPI document this test stops compiling.

use signaldb_sdk::Client;

#[test]
fn client_exposes_crud_builders() {
    let client = Client::new(
        "http://localhost:8080",
        signaldb_sdk::RetryPolicy::default(),
    );

    let _list = client.processors_list();
    let _get = client.processors_get().name("redact");
    let _delete = client.processors_delete().name("redact");

    let spec = signaldb_sdk::types::ProcessorSpec {
        name: "redact".to_string(),
        dataset: None,
        signal: "traces".to_string(),
        enabled: None,
        priority: None,
        error_mode: None,
        description: None,
        statements: vec![r#"replace_pattern(attributes["url.full"], "\\?.*$", "")"#.to_string()],
    };
    let _create = client.processors_create().body(spec.clone());
    let _replace = client.processors_replace().name("redact").body(spec);
}

#[test]
fn client_exposes_validate_and_test_builders() {
    let client = Client::new(
        "http://localhost:8080",
        signaldb_sdk::RetryPolicy::default(),
    );

    let _validate = client
        .processors_validate()
        .body(signaldb_sdk::types::ValidateRequest {
            signal: "traces".to_string(),
            statements: vec![
                r#"replace_pattern(attributes["url.full"], "\\?.*$", "")"#.to_string(),
            ],
        });

    let _test = client
        .processors_test()
        .body(signaldb_sdk::types::TestRequest {
            signal: "traces".to_string(),
            dataset: None,
            processors: None,
            payload: serde_json::json!({"resourceSpans": []}),
        });
}

/// A write-response body round-trips the `applies_within_seconds` field on
/// the generated write-response type.
#[test]
fn write_response_round_trips_applies_within_seconds() {
    use signaldb_sdk::types::ProcessorWriteResponse;

    let json = serde_json::json!({
        "tenant_id": "acme",
        "name": "redact",
        "signal": "traces",
        "enabled": true,
        "priority": 100,
        "error_mode": "ignore",
        "statements": [],
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z",
        "status": "ok",
        "applies_within_seconds": 30,
    });
    let response: ProcessorWriteResponse =
        serde_json::from_value(json).expect("write response deserializes");
    assert_eq!(response.applies_within_seconds, 30);
    assert_eq!(response.status, "ok");
}
