//! Parser behaviour, ported from `querier::query::search_filter` when the
//! grammar moved out of the query engine. The lowering tests that lived
//! alongside them stayed behind: they assert DataFusion expressions, which is
//! the querier's concern, not the language's.

use traceql::{Condition, FilterValue, MatchOp, ParseError, Selector, parse};

#[test]
fn equality_subset_parses() {
    let conditions =
        parse(r#"{ resource.service.name = "api" && span.http.method = "GET" }"#).unwrap();
    assert_eq!(conditions.len(), 2);
    assert_eq!(conditions[0].selector, Selector::ServiceName);
    assert_eq!(
        conditions[1].selector,
        Selector::SpanAttribute("http.method".to_string())
    );
}

#[test]
fn intrinsics_and_numbers() {
    let conditions =
        parse(r#"{ name = "GET /api" && status = error && span.http.status_code = 500 }"#).unwrap();
    assert_eq!(conditions[0].selector, Selector::SpanName);
    assert_eq!(conditions[1].selector, Selector::Status);
    assert_eq!(conditions[2].value, FilterValue::Number("500".to_string()));
}

#[test]
fn empty_spanset_matches_everything() {
    assert!(parse("{}").unwrap().is_empty());
    assert!(parse("{   }").unwrap().is_empty());
}

#[test]
fn kind_intrinsic_parses() {
    let conditions = parse(r#"{ kind = server }"#).unwrap();
    assert_eq!(
        conditions,
        vec![Condition {
            selector: Selector::Kind,
            op: MatchOp::Eq,
            value: FilterValue::String("server".to_string()),
        }]
    );
}

#[test]
fn ampersand_inside_quotes_is_preserved() {
    let conditions = parse(r#"{ span.query = "a && b" }"#).unwrap();
    assert_eq!(conditions.len(), 1);
    assert_eq!(
        conditions[0].value,
        FilterValue::String("a && b".to_string())
    );
}

/// Well-formed TraceQL using a construct this parser does not implement.
/// A caller maps these to "not implemented", so they must not drift into
/// `Syntax` — that would tell a client its valid query was malformed.
#[test]
fn valid_but_unimplemented_constructs_are_unsupported() {
    for q in [
        r#"{ duration > 100ms }"#,
        r#"{ span.x > 1 }"#,
        r#"{ span.x <= 1 }"#,
        r#"{ span.a = "1" || span.b = "2" }"#,
    ] {
        assert!(
            matches!(parse(q), Err(ParseError::Unsupported(_))),
            "expected Unsupported for {q}, got {:?}",
            parse(q)
        );
    }
}

/// Input that is not TraceQL at all. Answering "not implemented" here would
/// leave a client unable to tell a wrong query from one SignalDB cannot run.
#[test]
fn unparseable_input_is_a_syntax_error() {
    for q in [
        r#"name = "no-braces""#,
        r#"{ foo }"#,
        r#"{ zzz = 1 }"#,
        r#"{ .a = "unterminated }"#,
        r#"{ .a = @@@ }"#,
    ] {
        assert!(
            matches!(parse(q), Err(ParseError::Syntax(_))),
            "expected Syntax for {q}, got {:?}",
            parse(q)
        );
    }
}

/// The documented carve-out: an escaped string literal is legal TraceQL this
/// lexer cannot handle, and is reported as `Syntax` rather than `Unsupported`
/// because it was already a client error before the parser was extracted.
#[test]
fn escaped_string_literal_stays_a_syntax_error() {
    let q = r#"{ .a = "he said "hi"" }"#;
    assert!(
        matches!(parse(q), Err(ParseError::Syntax(_))),
        "got {:?}",
        parse(q)
    );
}

/// Rejection text is carried verbatim so a consumer can surface it unchanged.
#[test]
fn rejection_messages_name_the_offending_construct() {
    let err = parse(r#"{ span.x >= 1 }"#).unwrap_err();
    assert!(err.to_string().contains("'>='"), "{err}");

    let err = parse(r#"{ zzz = 1 }"#).unwrap_err();
    assert!(err.to_string().contains("zzz"), "{err}");
}

/// The operators from #2171: `!=`, `=~` and `!~` parse on every scope.
#[test]
fn negation_and_regex_operators_parse() {
    let conditions = parse(
        r#"{ resource.service.name != "robot-notes-server" && resource.device.model.identifier =~ ".+" && span.http.route !~ "/health.*" }"#,
    )
    .unwrap();
    assert_eq!(
        conditions,
        vec![
            Condition {
                selector: Selector::ServiceName,
                op: MatchOp::Ne,
                value: FilterValue::String("robot-notes-server".to_string()),
            },
            Condition {
                selector: Selector::ResourceAttribute("device.model.identifier".to_string()),
                op: MatchOp::Regex,
                value: FilterValue::String(".+".to_string()),
            },
            Condition {
                selector: Selector::SpanAttribute("http.route".to_string()),
                op: MatchOp::NotRegex,
                value: FilterValue::String("/health.*".to_string()),
            },
        ]
    );
}

/// An operator character inside a quoted value is part of the value.
#[test]
fn operator_characters_inside_quotes_are_text() {
    let conditions = parse(r#"{ .expr = "a<b!=c=~d" }"#).unwrap();
    assert_eq!(conditions[0].op, MatchOp::Eq);
    assert_eq!(
        conditions[0].value,
        FilterValue::String("a<b!=c=~d".to_string())
    );
}

#[test]
fn a_regex_operand_must_be_a_string() {
    assert!(matches!(
        parse(r#"{ .a =~ 5 }"#),
        Err(ParseError::Syntax(_))
    ));
}
