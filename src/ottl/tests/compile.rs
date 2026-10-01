use ottl::{Limits, Signal, compile};

#[test]
fn spec_scenario_replace_pattern_url_full_compiles_for_traces() {
    let program = compile(
        Signal::Traces,
        &[r#"replace_pattern(attributes["url.full"], "\\?.*$", "")"#.to_string()],
        &Limits::default(),
    );
    assert!(program.is_ok(), "{:?}", program.err());
}

#[test]
fn spec_scenario_sha256_email_with_where_compiles_for_traces() {
    let program = compile(
        Signal::Traces,
        &[r#"set(attributes["user.email"], SHA256(attributes["user.email"])) where attributes["user.email"] != nil"#.to_string()],
        &Limits::default(),
    );
    assert!(program.is_ok(), "{:?}", program.err());
}

#[test]
fn unknown_editor_merge_maps_fails_naming_it() {
    let errors = compile(
        Signal::Traces,
        &[r#"merge_maps(attributes, resource.attributes, "upsert")"#.to_string()],
        &Limits::default(),
    )
    .unwrap_err();
    assert!(
        errors.iter().any(|e| e.message.contains("merge_maps")),
        "{errors:?}"
    );
}

#[test]
fn span_name_is_illegal_for_logs_and_names_the_token() {
    let errors = compile(
        Signal::Logs,
        &[r#"set(span.name, "x")"#.to_string()],
        &Limits::default(),
    )
    .unwrap_err();
    assert!(
        errors
            .iter()
            .any(|e| e.message.contains("span.name") || e.message.contains("span")),
        "{errors:?}"
    );
}

#[test]
fn resource_and_scope_paths_are_legal_for_every_signal() {
    for signal in [Signal::Traces, Signal::Logs, Signal::Metrics] {
        let program = compile(
            signal,
            &[
                r#"set(resource.attributes["k"], "v")"#.to_string(),
                r#"set(instrumentation_scope.attributes["k"], "v")"#.to_string(),
            ],
            &Limits::default(),
        );
        assert!(program.is_ok(), "{signal:?}: {:?}", program.err());
    }
}

#[test]
fn logs_bare_body_and_attributes_are_legal() {
    let program = compile(
        Signal::Logs,
        &[
            r#"set(body, "x")"#.to_string(),
            r#"set(attributes["k"], "v")"#.to_string(),
        ],
        &Limits::default(),
    );
    assert!(program.is_ok(), "{:?}", program.err());
}

#[test]
fn metrics_bare_name_is_metric_name_and_bare_attributes_is_datapoint() {
    let program = compile(
        Signal::Metrics,
        &[
            r#"set(name, "renamed")"#.to_string(),
            r#"set(attributes["k"], "v")"#.to_string(),
        ],
        &Limits::default(),
    );
    assert!(program.is_ok(), "{:?}", program.err());
}

#[test]
fn metrics_datapoint_attributes_path_is_legal() {
    let program = compile(
        Signal::Metrics,
        &[r#"set(datapoint.attributes["k"], "v")"#.to_string()],
        &Limits::default(),
    );
    assert!(program.is_ok(), "{:?}", program.err());
}

#[test]
fn statement_count_over_limit_is_a_compile_error() {
    let limits = Limits {
        max_statements: 1,
        ..Limits::default()
    };
    let statements = vec![
        r#"set(name, "a")"#.to_string(),
        r#"set(name, "b")"#.to_string(),
    ];
    let errors = compile(Signal::Traces, &statements, &limits).unwrap_err();
    assert!(!errors.is_empty());
}

#[test]
fn regex_over_length_limit_is_a_compile_error() {
    let limits = Limits {
        max_regex_len: 4,
        ..Limits::default()
    };
    let errors = compile(
        Signal::Traces,
        &[r#"replace_pattern(name, "abcdef", "x")"#.to_string()],
        &limits,
    )
    .unwrap_err();
    assert!(errors.iter().any(|e| e.message.contains("exceeds")));
}

#[test]
fn compile_error_carries_statement_index() {
    let statements = vec![
        r#"set(name, "ok")"#.to_string(),
        r#"nope(name, "x")"#.to_string(),
    ];
    let errors = compile(Signal::Traces, &statements, &Limits::default()).unwrap_err();
    assert!(errors.iter().any(|e| e.statement == 1));
}

#[test]
fn keep_keys_requires_a_list_of_strings() {
    let errors = compile(
        Signal::Traces,
        &[r#"keep_keys(attributes, "a")"#.to_string()],
        &Limits::default(),
    )
    .unwrap_err();
    assert!(!errors.is_empty());
}

#[test]
fn replace_all_patterns_requires_key_or_value_selector() {
    let errors = compile(
        Signal::Traces,
        &[r#"replace_all_patterns(attributes, "keys", "a", "b")"#.to_string()],
        &Limits::default(),
    )
    .unwrap_err();
    assert!(
        errors
            .iter()
            .any(|e| e.message.contains("key") || e.message.contains("value"))
    );
}
