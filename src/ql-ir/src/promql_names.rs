//! Bare dotted metric names in PromQL text.
//!
//! Copied from the querier's PromQL planner, whose copy goes when that planner
//! is deleted (design D11): this crate is where PromQL text becomes a query.

use std::borrow::Cow;

/// SignalDB stores metric names in OTel dotted form
/// (`signaldb.wal.entries_pending`) but a bare dotted identifier is not
/// valid PromQL — `promql-parser` correctly rejects it. This rewrites such
/// an identifier, wherever it sits in metric-name position, into the
/// quoted UTF-8 selector form the parser accepts: `a.b.c` becomes
/// `{"a.b.c"}`, `a.b.c{x="y"}` becomes `{"a.b.c", x="y"}`, and `a.b.c[5m]`
/// becomes `{"a.b.c"}[5m]`. Only identifiers containing a `.` are
/// touched; a dotted identifier inside a `by`/`without`/`on`/`ignoring`/
/// `group_left`/`group_right` label list is left alone too, since that
/// position takes a label name rather than a selector, and — like string
/// literals, numeric literals, and the contents of an existing `{...}`
/// block — passes through byte for byte.
///
/// This scans the raw text rather than going through `promql_parser`'s own
/// lexer because that lexer treats a bare `.` outside a number literal as a
/// hard error and stops there — it can't tokenize the very construct this
/// function exists to accept.
pub fn quote_dotted_metric_names(query: &str) -> Cow<'_, str> {
    if !query.contains('.') {
        return Cow::Borrowed(query);
    }

    let chars: Vec<(usize, char)> = query.char_indices().collect();
    let len = chars.len();

    let mut out = String::with_capacity(query.len() + 8);
    let mut pos = 0;
    let mut changed = false;
    // Whether the innermost open paren is a grouping/matching label list
    // (`by (…)`, `on (…)`, …), pushed/popped on `(`/`)`.
    let mut label_lists: Vec<bool> = Vec::new();
    // Set right after scanning a grouping keyword, so the `(` it's
    // immediately (whitespace aside) followed by is recognized as opening
    // a label list rather than a selector's argument list.
    let mut pending_label_list = false;

    while pos < len {
        let (byte_pos, c) = chars[pos];
        match c {
            '"' | '\'' | '`' => {
                let end = skip_string(&chars, pos);
                out.push_str(&query[byte_pos..byte_at(&chars, query, end)]);
                pos = end;
                pending_label_list = false;
            }
            // Content already inside a selector is left alone: a dotted
            // name there is either the quoted form the caller wants, or a
            // label matcher this extension does not touch.
            '{' => {
                // An unclosed block runs to the end of the input; copying
                // it through as-is still leaves the parser to reject it.
                let end = skip_brace_block(&chars, pos).unwrap_or(len);
                out.push_str(&query[byte_pos..byte_at(&chars, query, end)]);
                pos = end;
                pending_label_list = false;
            }
            '(' => {
                label_lists.push(pending_label_list);
                pending_label_list = false;
                out.push('(');
                pos += 1;
            }
            ')' => {
                label_lists.pop();
                pending_label_list = false;
                out.push(')');
                pos += 1;
            }
            _ if c.is_whitespace() => {
                out.push(c);
                pos += 1;
            }
            _ if is_ident_start(c) => {
                let end = skip_ident(&chars, pos);
                let ident = &query[byte_pos..byte_at(&chars, query, end)];
                let in_label_list = label_lists.last().copied().unwrap_or(false);
                if ident.contains('.') && !in_label_list {
                    changed = true;
                    pos = emit_dotted(&mut out, query, &chars, ident, end);
                } else {
                    out.push_str(ident);
                    pos = end;
                }
                pending_label_list = !ident.contains('.') && is_grouping_keyword(ident);
            }
            _ => {
                out.push(c);
                pos += 1;
                pending_label_list = false;
            }
        }
    }

    if changed {
        Cow::Owned(out)
    } else {
        Cow::Borrowed(query)
    }
}

/// The byte offset just past `chars[idx]`, or `query.len()` past the end —
/// shared by [`quote_dotted_metric_names`] and [`emit_dotted`] to turn a
/// `chars` index back into a slice bound on `query`.
fn byte_at(chars: &[(usize, char)], query: &str, idx: usize) -> usize {
    chars.get(idx).map_or(query.len(), |&(b, _)| b)
}

/// The clauses that take a label list rather than a selector, so a dotted
/// name inside them is a label name this extension must not touch.
fn is_grouping_keyword(ident: &str) -> bool {
    matches!(
        ident.to_ascii_lowercase().as_str(),
        "by" | "without" | "on" | "ignoring" | "group_left" | "group_right"
    )
}

/// Whether `query` still holds a dotted identifier outside string literals
/// and selector blocks: one [`quote_dotted_metric_names`] left alone, such as
/// a label in a `by (…)` list, which the parser rejects unquoted.
pub(crate) fn has_bare_dotted_name(query: &str) -> bool {
    let chars: Vec<(usize, char)> = query.char_indices().collect();
    let mut pos = 0;
    while pos < chars.len() {
        let c = chars[pos].1;
        pos = match c {
            '"' | '\'' | '`' => skip_string(&chars, pos),
            '{' => skip_brace_block(&chars, pos).unwrap_or(chars.len()),
            _ if is_ident_start(c) => {
                let end = skip_ident(&chars, pos);
                if chars[pos..end].iter().any(|&(_, c)| c == '.') {
                    return true;
                }
                end
            }
            _ => pos + 1,
        };
    }
    false
}

/// A selector block's matchers split at each `or` keyword: an `or` right
/// after a matcher's value, so a label named `or` is not one.
fn or_groups(inner: &str) -> Vec<&str> {
    let chars: Vec<(usize, char)> = inner.char_indices().collect();
    let mut groups = Vec::new();
    let (mut start, mut pos, mut after_value) = (0, 0, false);
    while pos < chars.len() {
        let (byte_pos, c) = chars[pos];
        match c {
            '"' | '\'' | '`' => {
                pos = skip_string(&chars, pos);
                after_value = true;
            }
            _ if c.is_whitespace() => pos += 1,
            _ if is_ident_start(c) => {
                let end = skip_ident(&chars, pos);
                let ident = &inner[byte_pos..byte_at(&chars, inner, end)];
                if after_value && ident.eq_ignore_ascii_case("or") {
                    groups.push(inner[start..byte_pos].trim());
                    start = byte_at(&chars, inner, end);
                }
                pos = end;
                after_value = false;
            }
            _ => {
                pos += 1;
                after_value = false;
            }
        }
    }
    groups.push(inner[start..].trim());
    groups
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_' || c == ':'
}

fn is_ident_continue(c: char) -> bool {
    is_ident_start(c) || c.is_ascii_digit() || c == '.'
}

/// Advances past the identifier run starting at `pos` (`chars[pos]` must
/// satisfy [`is_ident_start`]). Returns the index just past the run.
fn skip_ident(chars: &[(usize, char)], pos: usize) -> usize {
    let mut i = pos + 1;
    while i < chars.len() && is_ident_continue(chars[i].1) {
        i += 1;
    }
    i
}

/// Advances past a quoted string literal starting at `pos` (`chars[pos]`
/// is the opening quote). Backtick strings are raw, matching the
/// Prometheus lexer; `"`/`'` strings honor a backslash escaping the next
/// character.
fn skip_string(chars: &[(usize, char)], pos: usize) -> usize {
    let quote = chars[pos].1;
    let raw = quote == '`';
    let mut i = pos + 1;
    while i < chars.len() {
        let c = chars[i].1;
        if !raw && c == '\\' && i + 1 < chars.len() {
            i += 2;
            continue;
        }
        i += 1;
        if c == quote {
            break;
        }
    }
    i
}

/// Advances past a `{...}` block starting at `pos` (`chars[pos] == '{'`),
/// honoring string literals inside it so a label value's own `}` doesn't
/// end the block early. Returns `None` for an unclosed block, so a caller
/// merging into it doesn't compute a nonsensical inner range.
fn skip_brace_block(chars: &[(usize, char)], pos: usize) -> Option<usize> {
    let mut i = pos + 1;
    while i < chars.len() {
        match chars[i].1 {
            '"' | '\'' | '`' => i = skip_string(chars, i),
            '}' => return Some(i + 1),
            _ => i += 1,
        }
    }
    None
}

/// Emits the quoted-selector rewrite for a dotted identifier found in
/// metric-name position and returns the index to resume scanning from.
/// PromQL's grammar allows whitespace between a selector and its `{`, so
/// this looks past it to decide whether to merge into an existing matcher
/// list or wrap the identifier on its own.
fn emit_dotted(
    out: &mut String,
    query: &str,
    chars: &[(usize, char)],
    ident: &str,
    end: usize,
) -> usize {
    let len = chars.len();

    let mut lookahead = end;
    while lookahead < len && chars[lookahead].1.is_whitespace() {
        lookahead += 1;
    }

    // An unclosed `{` falls through to the plain wrap below: the parser
    // will reject the query regardless, and the main loop's own `{`
    // handling (which tolerates an unclosed block) takes over from `end`.
    if lookahead < len
        && chars[lookahead].1 == '{'
        && let Some(brace_end) = skip_brace_block(chars, lookahead)
    {
        let inner = query
            [byte_at(chars, query, lookahead) + 1..byte_at(chars, query, brace_end) - 1]
            .trim();
        // `a.b{x or y}` is `{"a.b", x or "a.b", y}`: each group names it.
        let groups: Vec<String> = or_groups(inner)
            .into_iter()
            .map(|group| match group {
                "" => format!("\"{ident}\""),
                matchers => format!("\"{ident}\", {matchers}"),
            })
            .collect();
        out.push('{');
        out.push_str(&groups.join(" or "));
        out.push('}');
        return brace_end;
    }

    out.push_str("{\"");
    out.push_str(ident);
    out.push_str("\"}");
    end
}

#[cfg(test)]
mod tests {
    use super::quote_dotted_metric_names as quote;

    #[test]
    fn a_dotted_name_becomes_the_quoted_selector() {
        for (bare, quoted) in [
            (
                "signaldb.wal.entries_pending",
                r#"{"signaldb.wal.entries_pending"}"#,
            ),
            (
                r#"process.memory.usage{service_name="signaldb"}"#,
                r#"{"process.memory.usage", service_name="signaldb"}"#,
            ),
            ("rate(a.b[5m])", r#"rate({"a.b"}[5m])"#),
            (
                "sum by (http.method) (some.metric)",
                r#"sum by (http.method) ({"some.metric"})"#,
            ),
            (
                "a.b / on (svc.name) group_left (extra.dim) c.d",
                r#"{"a.b"} / on (svc.name) group_left (extra.dim) {"c.d"}"#,
            ),
            // Keywords are case-insensitive.
            (
                "SUM BY (http.method) (some.metric)",
                r#"SUM BY (http.method) ({"some.metric"})"#,
            ),
            // Every `or` group is its own conjunction, so each takes the name;
            // a label called `or` is not the keyword.
            (
                r#"a.b{x="1", or="2" OR y=~"3"}"#,
                r#"{"a.b", x="1", or="2" or "a.b", y=~"3"}"#,
            ),
        ] {
            assert_eq!(quote(bare), quoted, "{bare}");
        }
    }

    #[test]
    fn strings_numbers_and_undotted_queries_are_untouched() {
        let replace = r#"label_replace(x, "a", "$1", "b", "(.*)")"#;
        assert_eq!(quote(replace), replace);
        assert_eq!(quote("x > 0.95"), "x > 0.95");
        assert!(matches!(
            quote("sum(rate(x[5m]))"),
            std::borrow::Cow::Borrowed(_)
        ));
    }

    #[test]
    fn a_bare_dotted_name_is_found_outside_strings_and_selectors() {
        assert!(super::has_bare_dotted_name("sum by (http.method) (x)"));
        for q in [
            r#"{"a.b"}"#,
            r#"x{"k8s.pod"="a.b"}"#,
            "x > 0.95",
            r#"f("a.b")"#,
        ] {
            assert!(!super::has_bare_dotted_name(q), "{q}");
        }
    }
}
