use ottl::ast::{CmpOp, Condition, Expr, Literal, PathSegment};
use ottl::parse;

#[test]
fn parses_set_with_path_and_string_literal() {
    let stmt = parse(r#"set(attributes["url.full"], "x")"#).expect("parses");
    assert_eq!(stmt.call.name, "set");
    assert_eq!(stmt.call.args.len(), 2);
    match &stmt.call.args[0] {
        Expr::Path(p) => {
            assert_eq!(p.segments[0], PathSegment::Field("attributes".to_string()));
            assert_eq!(p.segments[1], PathSegment::Index("url.full".to_string()));
        }
        other => panic!("expected path, got {other:?}"),
    }
    assert_eq!(
        stmt.call.args[1],
        Expr::Literal(Literal::String("x".to_string()))
    );
}

#[test]
fn parses_nested_converter_call() {
    let stmt = parse(r#"set(attributes["user.email"], SHA256(attributes["user.email"]))"#)
        .expect("parses");
    match &stmt.call.args[1] {
        Expr::Call(call) => assert_eq!(call.name, "SHA256"),
        other => panic!("expected call, got {other:?}"),
    }
}

#[test]
fn parses_where_clause_with_ne_nil() {
    let stmt = parse(r#"set(attributes["a"], "b") where attributes["a"] != nil"#).expect("parses");
    match stmt.condition.expect("condition") {
        Condition::Compare(_, CmpOp::Ne, rhs) => assert_eq!(rhs, Expr::Literal(Literal::Nil)),
        other => panic!("expected compare, got {other:?}"),
    }
}

#[test]
fn parses_boolean_operators_and_parens() {
    let stmt = parse(r#"set(name, "x") where (attributes["a"] == "1" and attributes["b"] == "2") or not attributes["c"] != nil"#)
        .expect("parses");
    assert!(stmt.condition.is_some());
}

#[test]
fn parses_list_literal() {
    let stmt = parse(r#"keep_keys(attributes, ["a", "b", "c"])"#).expect("parses");
    match &stmt.call.args[1] {
        Expr::List(items) => assert_eq!(items.len(), 3),
        other => panic!("expected list, got {other:?}"),
    }
}

#[test]
fn parses_bare_attributes_map_path() {
    let stmt = parse(r#"delete_matching_keys(attributes, "^http\\.")"#).expect("parses");
    match &stmt.call.args[0] {
        Expr::Path(p) => assert_eq!(p.segments.len(), 1),
        other => panic!("expected path, got {other:?}"),
    }
}

#[test]
fn parses_escapes_backslash_quote_newline_tab() {
    let stmt = parse(r#"set(name, "a\\b\"c\nd\te")"#).expect("parses");
    assert_eq!(
        stmt.call.args[1],
        Expr::Literal(Literal::String("a\\b\"c\nd\te".to_string()))
    );
}

#[test]
fn unterminated_string_is_a_parse_error() {
    let err = parse(r#"set(name, "unterminated)"#).unwrap_err();
    assert!(err.column >= 1);
}

#[test]
fn unknown_editor_still_parses_a_call_and_fails_at_compile_time() {
    // The parser accepts any identifier as a call name; legality is a compile-time concern.
    let stmt = parse(r#"merge_maps(attributes, resource.attributes, "upsert")"#).expect("parses");
    assert_eq!(stmt.call.name, "merge_maps");
}

#[test]
fn bad_arity_is_still_syntactically_valid_but_flagged_at_compile() {
    let stmt = parse(r#"set(name)"#).expect("parses");
    assert_eq!(stmt.call.args.len(), 1);
}

#[test]
fn reports_column_and_token_on_syntax_error() {
    let err = parse(r#"set(name, )"#).unwrap_err();
    assert!(err.column > 1);
}

#[test]
fn where_clause_after_nested_call_and_after_list_literal_parses() {
    parse(r#"set(attributes["a"], SHA256(attributes["a"])) where attributes["a"] != nil"#)
        .expect("parses");
    parse(r#"keep_keys(attributes, ["a"]) where attributes["a"] == "1""#).expect("parses");
}
