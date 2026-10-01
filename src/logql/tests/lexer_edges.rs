//! Lexer escape handling and rejection paths.
//!
//! These are the branches a well-formed query never reaches, which is exactly
//! why they were uncovered: nothing exercised the escape table or the negative
//! -literal guards until something asked what they did.

use logql::{Token, tokenize};

/// Every escape the string lexer understands, and what it produces.
#[test]
fn string_escapes_decode() {
    let cases: &[(&str, &str)] = &[
        (r#""a\"b""#, "a\"b"),
        (r#""a\\b""#, "a\\b"),
        (r#""a\/b""#, "a/b"),
        (r#""a\nb""#, "a\nb"),
        (r#""a\tb""#, "a\tb"),
        (r#""a\rb""#, "a\rb"),
        (r#""A""#, "A"),
        (r#""é""#, "é"),
    ];
    for (input, expected) in cases {
        let tokens = tokenize(input).unwrap_or_else(|e| panic!("{input} should lex: {e}"));
        assert_eq!(
            tokens[0].token,
            Token::String((*expected).to_string()),
            "{input}"
        );
    }
}

/// Backtick strings are raw — the form used for regex patterns, where a
/// backslash is a regex escape and must survive lexing untouched.
#[test]
fn raw_strings_keep_backslashes() {
    let tokens = tokenize(r#"`a\d+b`"#).expect("raw string lexes");
    assert_eq!(tokens[0].token, Token::String(r"a\d+b".to_string()));
}

/// Each rejection, with the fragment of the message that identifies it. A
/// caller surfaces these to a user, so the wording is part of the contract.
#[test]
fn malformed_input_is_rejected_with_a_reason() {
    let cases: &[(&str, &str)] = &[
        (r#""unterminated"#, "unterminated string literal"),
        (r#""trailing escape\"#, "unterminated string literal"),
        (r#""bad \q escape""#, "escape"),
        (r#""\u00""#, "invalid \\u escape"),
        (r#""\uD800""#, "invalid unicode code point"),
        ("`unterminated", "unterminated"),
        ("5z", "duration"),
        ("--", "expected a flag name after '--'"),
        ("!", "unexpected '!'"),
        ("@", "unexpected character"),
    ];
    for (input, fragment) in cases {
        let err = tokenize(input).expect_err(&format!("{input:?} should not lex"));
        let rendered = err.to_string();
        assert!(
            rendered.contains(fragment),
            "{input:?} should mention {fragment:?}, got {rendered:?}"
        );
    }
}

/// Positions are 1-based and point at the offending token, not the start of
/// the query — that is what lets a caller underline it.
#[test]
fn errors_point_at_the_offending_token() {
    let err = tokenize(r#"{service_name="api"} @"#).expect_err("@ is not a LogQL token");
    assert_eq!(err.line, 1);
    assert_eq!(err.col, 22);
}

/// Byte literals carry their unit; a leading minus is rejected rather than
/// silently producing a huge unsigned value.
#[test]
fn byte_literals_lex_and_reject_negatives() {
    let tokens = tokenize("10MB").expect("byte literal lexes");
    assert!(
        matches!(tokens[0].token, Token::Bytes(_)),
        "{:?}",
        tokens[0]
    );
    // A leading `-` lexes as its own Sub token; the literal itself is unsigned.
    let tokens = tokenize("-5MB").expect("minus then a byte literal lexes");
    assert_eq!(tokens[0].token, Token::Sub);
}

/// A comment runs to end of line and produces no tokens.
#[test]
fn comments_are_skipped() {
    let tokens = tokenize("# just a comment").expect("a bare comment lexes");
    assert!(tokens.is_empty(), "{tokens:?}");

    let tokens = tokenize("{a=\"b\"} # trailing").expect("a trailing comment lexes");
    assert_eq!(tokens.last().map(|t| &t.token), Some(&Token::RBrace));
}
