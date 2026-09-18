use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use ottl::{ErrorMode, Limits, Signal, compile};

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

fn attr_str<'a>(attrs: &'a [KeyValue], key: &str) -> Option<&'a str> {
    attrs
        .iter()
        .find(|kv| kv.key == key)
        .and_then(|kv| kv.value.as_ref())
        .and_then(|v| match &v.value {
            Some(any_value::Value::StringValue(s)) => Some(s.as_str()),
            _ => None,
        })
}

fn one_span_request(attributes: Vec<KeyValue>) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope::default()),
                spans: vec![Span {
                    name: "GET /x".to_string(),
                    attributes,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

#[test]
fn spec_scenario_replace_pattern_strips_query_string() {
    let program = compile(
        Signal::Traces,
        &[r#"replace_pattern(attributes["url.full"], "\\?.*$", "")"#.to_string()],
        &Limits::default(),
    )
    .expect("compiles");
    let mut req = one_span_request(vec![kv("url.full", "https://example.com/x?token=secret")]);
    let report = program
        .apply_traces(&mut req, ErrorMode::Propagate)
        .expect("applies");
    let span = &req.resource_spans[0].scope_spans[0].spans[0];
    assert_eq!(
        attr_str(&span.attributes, "url.full"),
        Some("https://example.com/x")
    );
    assert_eq!(report.statements[0].matched, 1);
    assert_eq!(report.statements[0].errors, 0);
}

#[test]
fn spec_scenario_sha256_email_only_when_present() {
    let program = compile(
        Signal::Traces,
        &[
            r#"set(attributes["user.email"], SHA256(attributes["user.email"])) where attributes["user.email"] != nil"#
                .to_string(),
        ],
        &Limits::default(),
    )
    .expect("compiles");

    let mut with_email = one_span_request(vec![kv("user.email", "a@example.com")]);
    program
        .apply_traces(&mut with_email, ErrorMode::Propagate)
        .expect("applies");
    let span = &with_email.resource_spans[0].scope_spans[0].spans[0];
    let hashed = attr_str(&span.attributes, "user.email").expect("attribute kept");
    assert_ne!(hashed, "a@example.com");
    assert_eq!(hashed.len(), 64, "sha256 hex digest is 64 chars");

    let mut without_email = one_span_request(vec![]);
    let report = program
        .apply_traces(&mut without_email, ErrorMode::Propagate)
        .expect("applies");
    assert_eq!(report.statements[0].matched, 0);
}

#[test]
fn ordering_later_statement_sees_earlier_edit() {
    let program = compile(
        Signal::Traces,
        &[
            r#"set(attributes["k"], "first")"#.to_string(),
            r#"set(attributes["k2"], attributes["k"])"#.to_string(),
        ],
        &Limits::default(),
    )
    .expect("compiles");
    let mut req = one_span_request(vec![]);
    program
        .apply_traces(&mut req, ErrorMode::Propagate)
        .expect("applies");
    let span = &req.resource_spans[0].scope_spans[0].spans[0];
    assert_eq!(attr_str(&span.attributes, "k2"), Some("first"));
}

#[test]
fn int_conversion_error_propagates() {
    let program = compile(
        Signal::Traces,
        &[r#"set(attributes["n"], Int("not-a-number"))"#.to_string()],
        &Limits::default(),
    )
    .expect("compiles");
    let mut req = one_span_request(vec![]);
    let err = program
        .apply_traces(&mut req, ErrorMode::Propagate)
        .unwrap_err();
    assert_eq!(err.statement, 0);
}

#[test]
fn int_conversion_error_is_skipped_under_ignore() {
    let program = compile(
        Signal::Traces,
        &[
            r#"set(attributes["n"], Int("not-a-number"))"#.to_string(),
            r#"set(attributes["after"], "ran")"#.to_string(),
        ],
        &Limits::default(),
    )
    .expect("compiles");
    let mut req = one_span_request(vec![]);
    let report = program
        .apply_traces(&mut req, ErrorMode::Ignore)
        .expect("does not abort");
    assert_eq!(report.statements[0].errors, 1);
    assert_eq!(report.statements[1].matched, 1);
    let span = &req.resource_spans[0].scope_spans[0].spans[0];
    assert_eq!(attr_str(&span.attributes, "n"), None);
    assert_eq!(attr_str(&span.attributes, "after"), Some("ran"));
}

#[test]
fn keep_keys_and_delete_matching_keys_and_truncate_and_limit() {
    let program = compile(
        Signal::Traces,
        &[
            r#"delete_matching_keys(attributes, "^drop_")"#.to_string(),
            r#"keep_keys(attributes, ["a", "b", "long"])"#.to_string(),
            r#"truncate_all(attributes, 3)"#.to_string(),
        ],
        &Limits::default(),
    )
    .expect("compiles");
    let mut req = one_span_request(vec![
        kv("drop_me", "x"),
        kv("a", "1"),
        kv("b", "2"),
        kv("other", "3"),
        kv("long", "abcdef"),
    ]);
    program
        .apply_traces(&mut req, ErrorMode::Propagate)
        .expect("applies");
    let span = &req.resource_spans[0].scope_spans[0].spans[0];
    let mut keys: Vec<&str> = span.attributes.iter().map(|kv| kv.key.as_str()).collect();
    keys.sort_unstable();
    assert_eq!(keys, vec!["a", "b", "long"]);
    assert_eq!(attr_str(&span.attributes, "long"), Some("abc"));
}

#[test]
fn limit_keeps_priority_keys_first() {
    let program = compile(
        Signal::Traces,
        &[r#"limit(attributes, 2, ["important"])"#.to_string()],
        &Limits::default(),
    )
    .expect("compiles");
    let mut req = one_span_request(vec![kv("a", "1"), kv("b", "2"), kv("important", "keep-me")]);
    program
        .apply_traces(&mut req, ErrorMode::Propagate)
        .expect("applies");
    let span = &req.resource_spans[0].scope_spans[0].spans[0];
    assert_eq!(span.attributes.len(), 2);
    assert!(span.attributes.iter().any(|kv| kv.key == "important"));
}

#[test]
fn replace_all_patterns_on_keys_and_values() {
    let program = compile(
        Signal::Traces,
        &[r#"replace_all_patterns(attributes, "key", "^http\\.", "h.")"#.to_string()],
        &Limits::default(),
    )
    .expect("compiles");
    let mut req = one_span_request(vec![kv("http.method", "GET")]);
    program
        .apply_traces(&mut req, ErrorMode::Propagate)
        .expect("applies");
    let span = &req.resource_spans[0].scope_spans[0].spans[0];
    assert!(span.attributes.iter().any(|kv| kv.key == "h.method"));
}

#[test]
fn replace_match_and_replace_all_matches_use_glob() {
    let program = compile(
        Signal::Traces,
        &[
            r#"replace_match(name, "GET *", "GET <redacted>")"#.to_string(),
            r#"replace_all_matches(attributes, "secret-*", "***")"#.to_string(),
        ],
        &Limits::default(),
    )
    .expect("compiles");
    let mut req = one_span_request(vec![kv("token", "secret-abc")]);
    program
        .apply_traces(&mut req, ErrorMode::Propagate)
        .expect("applies");
    let span = &req.resource_spans[0].scope_spans[0].spans[0];
    assert_eq!(span.name, "GET <redacted>");
    assert_eq!(attr_str(&span.attributes, "token"), Some("***"));
}

#[test]
fn dollar_dollar_replacement_ports_verbatim_from_collector_configs() {
    let program = compile(
        Signal::Traces,
        &[r#"replace_pattern(attributes["v"], "(a)(b)", "$$1-$2")"#.to_string()],
        &Limits::default(),
    )
    .expect("compiles");
    let mut req = one_span_request(vec![kv("v", "ab")]);
    program
        .apply_traces(&mut req, ErrorMode::Propagate)
        .expect("applies");
    let span = &req.resource_spans[0].scope_spans[0].spans[0];
    // `$$` -> literal `$`; the following `1` is plain text, not part of a group ref.
    // `$2` is a real capture-group reference.
    assert_eq!(attr_str(&span.attributes, "v"), Some("$1-b"));
}

#[test]
fn concat_and_string_converters() {
    let program = compile(
        Signal::Traces,
        &[
            r#"set(attributes["combo"], Concat([attributes["a"], "-", attributes["b"]], ""))"#
                .to_string(),
        ],
        &Limits::default(),
    )
    .expect("compiles");
    let mut req = one_span_request(vec![kv("a", "x"), kv("b", "y")]);
    program
        .apply_traces(&mut req, ErrorMode::Propagate)
        .expect("applies");
    let span = &req.resource_spans[0].scope_spans[0].spans[0];
    assert_eq!(attr_str(&span.attributes, "combo"), Some("x-y"));
}
