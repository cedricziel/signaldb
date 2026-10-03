//! Recognising the supported TraceQL subset.

use crate::ast::{Condition, FilterValue, MatchOp, Selector, unscoped_selector};

/// Why a query was rejected.
///
/// The two variants exist because they mean different things to a caller, and
/// callers serving HTTP map them to different statuses: [`Self::Syntax`] is the
/// client's mistake, [`Self::Unsupported`] is ours.
///
/// Message text is carried verbatim so a consumer can surface it unchanged.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    /// The input is not TraceQL. No amount of implementing would make it
    /// parse.
    ///
    /// Carries one documented exception — escaped string literals. See the
    /// crate-level docs, which are the single statement of this contract.
    #[error("{0}")]
    Syntax(String),

    /// The input is well-formed TraceQL using a construct this parser does not
    /// implement.
    #[error("{0}")]
    Unsupported(String),
}

/// Parse the supported TraceQL subset:
/// `{ selector <op> value && selector <op> value ... }`, where `<op>` is one
/// of `=`, `!=`, `=~` or `!~`.
///
/// An empty spanset (`{}`) is valid and selects everything, so it returns no
/// conditions rather than an error.
///
/// # Examples
///
/// ```
/// use traceql::{FilterValue, Selector};
///
/// let conditions = traceql::parse(r#"{ resource.service.name = "api" }"#)?;
/// assert_eq!(conditions.len(), 1);
/// assert_eq!(conditions[0].selector, Selector::ServiceName);
/// assert_eq!(conditions[0].value, FilterValue::String("api".into()));
/// # Ok::<(), traceql::ParseError>(())
/// ```
///
/// An empty spanset selects everything:
///
/// ```
/// assert!(traceql::parse("{}")?.is_empty());
/// # Ok::<(), traceql::ParseError>(())
/// ```
///
/// The two rejection classes are distinguishable without reading the message
/// — a caller serving HTTP maps them to different statuses:
///
/// ```
/// use traceql::ParseError;
///
/// // Not TraceQL at all: the caller's mistake.
/// assert!(matches!(traceql::parse("notbraces"), Err(ParseError::Syntax(_))));
///
/// // Valid TraceQL this parser does not implement: ours.
/// assert!(matches!(
///     traceql::parse(r#"{ span.x > 1 }"#),
///     Err(ParseError::Unsupported(_)),
/// ));
/// ```
pub fn parse(q: &str) -> Result<Vec<Condition>, ParseError> {
    let trimmed = q.trim();
    let inner = trimmed
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or_else(|| {
            ParseError::Syntax(format!(
                "Unsupported TraceQL query '{q}': only a single {{ ... }} spanset is supported"
            ))
        })?;

    if inner.contains("||") {
        return Err(ParseError::Unsupported(
            "TraceQL '||' is not supported yet; only '&&' conjunctions of matchers".to_string(),
        ));
    }
    let inner = inner.trim();
    if inner.is_empty() {
        // `{}` selects everything — valid TraceQL, no filters.
        return Ok(Vec::new());
    }

    let mut conditions = Vec::new();
    for clause in split_top_level_and(inner) {
        conditions.push(parse_clause(clause.trim())?);
    }
    Ok(conditions)
}

/// Split on `&&` outside of double quotes.
fn split_top_level_and(input: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut in_quotes = false;
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => in_quotes = !in_quotes,
            b'&' if !in_quotes && i + 1 < bytes.len() && bytes[i + 1] == b'&' => {
                parts.push(&input[start..i]);
                i += 2;
                start = i;
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    parts.push(&input[start..]);
    parts
}

/// Split `clause` at its comparison operator, the first one outside double
/// quotes, so an operator character inside a string value is just text.
fn split_operator(clause: &str) -> Option<(&str, &str, &str)> {
    let bytes = clause.as_bytes();
    let mut in_quotes = false;
    for (i, &byte) in bytes.iter().enumerate() {
        match byte {
            b'"' => in_quotes = !in_quotes,
            b'=' | b'!' | b'<' | b'>' if !in_quotes => {
                let two = matches!(
                    (byte, bytes.get(i + 1)),
                    (b'!', Some(b'=' | b'~')) | (b'=', Some(b'~')) | (b'<' | b'>', Some(b'='))
                );
                let end = if two { i + 2 } else { i + 1 };
                return Some((&clause[..i], &clause[i..end], &clause[end..]));
            }
            _ => {}
        }
    }
    None
}

/// Parse one `selector <op> value` clause of the supported subset.
fn parse_clause(clause: &str) -> Result<Condition, ParseError> {
    let (lhs, op, rhs) = split_operator(clause).ok_or_else(|| {
        ParseError::Syntax(format!(
            "Unsupported TraceQL clause '{clause}': expected `selector <op> value`"
        ))
    })?;
    let op = match op {
        "=" => MatchOp::Eq,
        "!=" => MatchOp::Ne,
        "=~" => MatchOp::Regex,
        "!~" => MatchOp::NotRegex,
        ">" | "<" | ">=" | "<=" => {
            return Err(ParseError::Unsupported(format!(
                "TraceQL operator '{op}' is not supported yet; supported operators are \
                 '=', '!=', '=~' and '!~' (clause: '{clause}')"
            )));
        }
        _ => {
            return Err(ParseError::Syntax(format!(
                "Unsupported TraceQL operator '{op}' in clause '{clause}'"
            )));
        }
    };
    let lhs = lhs.trim();
    let rhs = rhs.trim();

    let selector = if lhs == "name" {
        Selector::SpanName
    } else if lhs == "status" {
        Selector::Status
    } else if lhs == "kind" {
        Selector::Kind
    } else if lhs == "duration" {
        return Err(ParseError::Unsupported(
            "TraceQL 'duration' matchers are not supported; use minDuration/maxDuration"
                .to_string(),
        ));
    } else if lhs == "resource.service.name" || lhs == ".service.name" {
        Selector::ServiceName
    } else if let Some(key) = lhs.strip_prefix("span.") {
        Selector::SpanAttribute(key.to_string())
    } else if let Some(key) = lhs.strip_prefix("resource.") {
        Selector::ResourceAttribute(key.to_string())
    } else if let Some(key) = lhs.strip_prefix('.') {
        unscoped_selector(key)
    } else {
        return Err(ParseError::Syntax(format!(
            "Unsupported TraceQL selector '{lhs}'"
        )));
    };

    let value = parse_value(rhs)?;
    if matches!(op, MatchOp::Regex | MatchOp::NotRegex) && !matches!(value, FilterValue::String(_))
    {
        return Err(ParseError::Syntax(format!(
            "TraceQL operator '{}' needs a quoted regular expression (clause: '{clause}')",
            op.as_str()
        )));
    }
    Ok(Condition {
        selector,
        op,
        value,
    })
}

fn parse_value(raw: &str) -> Result<FilterValue, ParseError> {
    if let Some(quoted) = raw.strip_prefix('"') {
        let inner = quoted
            .strip_suffix('"')
            .ok_or_else(|| ParseError::Syntax(format!("Unterminated string literal '{raw}'")))?;
        if inner.contains('"') {
            // Legal TraceQL this lexer does not handle — see ParseError::Syntax.
            return Err(ParseError::Syntax(format!(
                "Unsupported escaped string literal '{raw}'"
            )));
        }
        return Ok(FilterValue::String(inner.to_string()));
    }
    if raw == "true" || raw == "false" {
        return Ok(FilterValue::Bool(raw == "true"));
    }
    if !raw.is_empty()
        && raw
            .chars()
            .all(|c| c.is_ascii_digit() || c == '.' || c == '-')
    {
        return Ok(FilterValue::Number(raw.to_string()));
    }
    // Bare identifiers are only meaningful for `status`/`kind`.
    if !raw.is_empty() && raw.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Ok(FilterValue::String(raw.to_string()));
    }
    Err(ParseError::Syntax(format!(
        "Unsupported TraceQL value '{raw}'"
    )))
}
