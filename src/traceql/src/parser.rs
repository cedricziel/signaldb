//! Recognising the supported TraceQL subset.

use crate::ast::{Condition, FilterValue, Selector, unscoped_selector};

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
    /// The input is not TraceQL. Nothing SignalDB could implement would make
    /// it parse.
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
/// `{ selector = value && selector = value ... }`.
///
/// An empty spanset (`{}`) is valid and selects everything, so it returns no
/// conditions rather than an error.
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
            "TraceQL '||' is not supported yet; only '&&' conjunctions of equality matchers"
                .to_string(),
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

/// Parse one `selector = value` clause of the supported subset.
fn parse_clause(clause: &str) -> Result<Condition, ParseError> {
    for unsupported in ["!=", ">=", "<=", "=~", "!~", ">", "<"] {
        if clause.contains(unsupported) {
            return Err(ParseError::Unsupported(format!(
                "TraceQL operator '{unsupported}' is not supported yet (clause: '{clause}')"
            )));
        }
    }
    let (lhs, rhs) = clause.split_once('=').ok_or_else(|| {
        ParseError::Syntax(format!(
            "Unsupported TraceQL clause '{clause}': only equality matchers are supported"
        ))
    })?;
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
    Ok(Condition { selector, value })
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
