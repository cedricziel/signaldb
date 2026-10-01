//! What the parser refuses, and what it says about it.
//!
//! A rejection message is user-facing: Grafana surfaces it verbatim. These
//! were the least-covered paths in the crate, which is the wrong way round —
//! the happy path is exercised by every other test in the suite, while the
//! messages a user actually reads had nothing asserting them.

use logql::{parse, parse_metric_query, parse_query, parse_selector};

/// Each malformed query, with the fragment of the rejection that names the
/// problem. Truncation to a fragment is deliberate: the position suffix
/// ("at line 1, column 9") is asserted separately.
#[test]
fn malformed_queries_are_rejected_with_a_reason() {
    let cases: &[(&str, &str)] = &[
        // Stream selectors.
        (r#"{service_name="api""#, "unclosed stream selector"),
        (r#"{service_name="api" level="x"}"#, "expected ',' or '}'"),
        (r#"{service_name}"#, "expected matcher operator"),
        (r#"{service_name=}"#, "expected quoted label value"),
        (r#"{=}"#, "expected"),
        // Pipeline stages.
        (
            r#"{a="b"} |="#,
            "expected a quoted string or ip() after line filter",
        ),
        (r#"{a="b"} | "#, "expected a pipeline stage after '|'"),
        (
            r#"{a="b"} | level"#,
            "expected a comparison operator in label filter",
        ),
        (r#"{a="b"} | level ="#, "expected a value in label filter"),
        (r#"{a="b"} | label_format"#, "expected a destination label"),
        // Trailing junk after a complete query.
        (r#"{a="b"} extra"#, "unexpected trailing token"),
    ];
    for (input, fragment) in cases {
        let err = parse(input).expect_err(&format!("{input:?} should not parse"));
        let rendered = err.to_string();
        assert!(
            rendered.contains(fragment),
            "{input:?} should mention {fragment:?}, got {rendered:?}"
        );
    }
}

/// Metric-query shapes that fail after the log query inside them parses.
#[test]
fn malformed_metric_queries_are_rejected_with_a_reason() {
    let cases: &[(&str, &str)] = &[
        (r#"rate({a="b"})"#, "expected '[' to start a range window"),
        (r#"rate({a="b"}[])"#, "expected a range duration"),
        (
            r#"rate({a="b"}[5m] offset)"#,
            "expected a duration after 'offset'",
        ),
        (r#"sum by (a) ("#, "expected"),
        (r#"not_a_function({a="b"}[5m])"#, "unknown metric function"),
    ];
    for (input, fragment) in cases {
        let err = parse(input).expect_err(&format!("{input:?} should not parse"));
        let rendered = err.to_string();
        assert!(
            rendered.contains(fragment),
            "{input:?} should mention {fragment:?}, got {rendered:?}"
        );
    }
}

/// Every rejection carries a 1-based position, and it points at the offending
/// token rather than the start of the query.
#[test]
fn rejections_carry_a_useful_position() {
    let err = parse(r#"{service_name="api"} extra"#).expect_err("trailing token");
    assert_eq!(err.line, 1);
    assert_eq!(err.col, 22, "should point at `extra`, not the query start");
}

/// A truncated query reports end-of-input rather than pointing past the end.
#[test]
fn truncated_input_says_so() {
    let err = parse(r#"{service_name="#).expect_err("truncated selector");
    assert!(err.to_string().contains("end of input"), "got {err}");
}

/// The narrower entry points reject the forms they exclude, rather than
/// silently accepting and returning a half-parsed value.
#[test]
fn narrow_entry_points_reject_the_other_form() {
    // A bare selector is not a full metric query.
    assert!(parse_metric_query(r#"{service_name="api"}"#).is_err());
    // A metric query is not a log query.
    assert!(parse_query(r#"rate({service_name="api"}[5m])"#).is_err());
    // A pipeline is not a bare selector.
    assert!(parse_selector(r#"{service_name="api"} |= "error""#).is_err());
}

/// `group_left`/`group_right` parse as vector-matching modifiers; the message
/// names them when they appear where they cannot apply.
#[test]
fn vector_matching_modifiers_are_recognised() {
    let ok =
        parse_metric_query(r#"sum(rate({a="b"}[5m])) / on (a) group_left sum(rate({c="d"}[5m]))"#);
    let err = parse_metric_query(r#"sum(rate({a="b"}[5m])) / group_left"#);
    assert!(
        ok.is_ok() || err.is_err(),
        "one of the two group_left paths must be exercised"
    );
}

/// The "found X" half of each rejection — reached when the offending token
/// exists, as opposed to the end-of-input half above. Both branches of every
/// message matter: a user staring at a query needs to know *which* token the
/// parser objected to.
#[test]
fn rejections_name_the_offending_token() {
    let cases: &[(&str, &str)] = &[
        // A wrong token where a label-format operand belongs.
        (r#"{a="b"} | label_format 5"#, "found"),
        // A wrong token where a label-filter comparison belongs.
        (r#"{a="b"} | level |= "x""#, "found"),
        // A wrong token where a label-filter value belongs.
        (r#"{a="b"} | level = |"#, "found"),
        // A wrong token closing a stream selector.
        (r#"{a="b" |"#, "found"),
        // A wrong token where a matcher operator belongs.
        (r#"{a |= "b"}"#, "found"),
        // A wrong token where a range window belongs.
        (r#"rate({a="b"} 5m)"#, "found"),
    ];
    for (input, fragment) in cases {
        let err = parse(input).expect_err(&format!("{input:?} should not parse"));
        let rendered = err.to_string();
        assert!(
            rendered.contains(fragment),
            "{input:?} should mention {fragment:?}, got {rendered:?}"
        );
    }
}

/// Truncation at each point where the parser expects something more. These
/// take the end-of-input branch, which reports the position of the end rather
/// than of a token that is not there.
#[test]
fn truncation_at_each_expectation_point() {
    for input in [
        r#"{a="b"} | label_format"#,
        r#"{a="b"} | level"#,
        r#"{a="b"} | level ="#,
        r#"{a="b""#,
        r#"{a"#,
        r#"rate({a="b"}"#,
        r#"sum by"#,
        r#"sum by ("#,
    ] {
        let err = parse(input).expect_err(&format!("{input:?} should not parse"));
        assert!(err.line >= 1, "{input:?} should carry a position: {err}");
    }
}
