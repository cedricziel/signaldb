//! # Query-text sanitization
//!
//! Implements the DB-semconv sanitization rules for recording query text on
//! spans: every literal is replaced with a `?` placeholder before the text
//! leaves the process, so PII in string/numeric literals never lands in
//! telemetry. Parameterized values are never inlined by construction — this
//! helper is for the raw-SQL / dialect paths where user text is the query.

/// Replace SQL literals with `?` placeholders.
///
/// Covers the semconv literal classes that occur in SignalDB's dialects:
/// single-quoted strings (with `''` escapes), double-quoted strings that are
/// not identifiers in the dialect are left alone (SignalDB treats `"` as an
/// identifier quote), numeric literals (integer, decimal, exponent, hex),
/// and the boolean/NULL keywords. Adjacent placeholders inside an `IN`
/// list are collapsed to a single `?`.
pub fn sanitize_query_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let bytes = text.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        let c = bytes[i] as char;

        // Single-quoted string literal, honoring '' escapes.
        if c == '\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.push('?');
            continue;
        }

        // Identifier quotes pass through verbatim (not literals).
        if c == '"' {
            out.push(c);
            i += 1;
            while i < bytes.len() {
                out.push(bytes[i] as char);
                if bytes[i] == b'"' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }

        // Numeric literal — only when not part of an identifier.
        if c.is_ascii_digit()
            && !out
                .chars()
                .last()
                .is_some_and(|p| p.is_ascii_alphanumeric() || p == '_')
        {
            while i < bytes.len()
                && ((bytes[i] as char).is_ascii_alphanumeric()
                    || bytes[i] == b'.'
                    || ((bytes[i] == b'+' || bytes[i] == b'-')
                        && matches!(bytes[i - 1], b'e' | b'E')))
            {
                i += 1;
            }
            out.push('?');
            continue;
        }

        // Word: check for boolean/NULL keywords.
        if c.is_ascii_alphabetic() || c == '_' {
            let start = i;
            while i < bytes.len()
                && ((bytes[i] as char).is_ascii_alphanumeric() || bytes[i] == b'_')
            {
                i += 1;
            }
            let word = &text[start..i];
            if word.eq_ignore_ascii_case("true")
                || word.eq_ignore_ascii_case("false")
                || word.eq_ignore_ascii_case("null")
            {
                out.push('?');
            } else {
                out.push_str(word);
            }
            continue;
        }

        out.push(c);
        i += 1;
    }

    collapse_in_lists(&out)
}

/// Collapse `(?, ?, ?)` placeholder lists to `(?)` — semconv explicitly
/// permits this and it keeps long IN-lists from bloating the attribute.
fn collapse_in_lists(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find("(?") {
        let (head, tail) = rest.split_at(start);
        out.push_str(head);
        // Scan a run of `?` separated by commas/whitespace.
        let mut j = 1; // past '('
        let t = tail.as_bytes();
        let mut placeholders = 0;
        while j < t.len() {
            match t[j] as char {
                '?' => {
                    placeholders += 1;
                    j += 1;
                }
                ',' | ' ' => j += 1,
                ')' => break,
                _ => break,
            }
        }
        if placeholders > 1 && j < t.len() && t[j] == b')' {
            out.push_str("(?)");
            rest = &tail[j + 1..];
        } else {
            out.push('(');
            rest = &tail[1..];
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_literals_become_placeholders() {
        assert_eq!(
            sanitize_query_text("SELECT * FROM t WHERE name = 'alice@example.com'"),
            "SELECT * FROM t WHERE name = ?"
        );
        // '' escape stays inside one literal
        assert_eq!(
            sanitize_query_text("WHERE a = 'it''s' AND b = 'x'"),
            "WHERE a = ? AND b = ?"
        );
    }

    #[test]
    fn numeric_and_boolean_literals_become_placeholders() {
        assert_eq!(
            sanitize_query_text("WHERE ts > 1700000000000000000 AND ratio < 0.5e-3 AND ok = true"),
            "WHERE ts > ? AND ratio < ? AND ok = ?"
        );
    }

    #[test]
    fn identifiers_survive() {
        assert_eq!(
            sanitize_query_text("SELECT trace_id2 FROM \"acme\".\"prod\".\"traces\""),
            "SELECT trace_id2 FROM \"acme\".\"prod\".\"traces\""
        );
    }

    #[test]
    fn in_lists_collapse() {
        assert_eq!(
            sanitize_query_text("WHERE id IN (1, 2, 3, 4)"),
            "WHERE id IN (?)"
        );
    }
}
