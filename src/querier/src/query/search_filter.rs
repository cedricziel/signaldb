//! # Tempo `tags` parameter parsing
//!
//! Parses Tempo's `tags` parameter (space-separated logfmt `key=value`
//! pairs, values optionally double-quoted) into [`traceql::Condition`]
//! values. That is an HTTP parameter encoding rather than a TraceQL
//! construct — it merely produces the same [`Condition`] shape TraceQL's own
//! parser does — so it stays out of the `traceql`/`ql-ir` crates, which know
//! nothing about HTTP query parameters (design D4 of `ir-single-lowering`).
//!
//! Lowering a [`Condition`] to a query — whether the old per-condition
//! DataFusion expression this module used to build (`to_expr`, deleted in
//! `ir-single-lowering` §5) or the IR predicate [`super::tags_to_ir`] builds
//! today — is not this module's concern; it only recognises the wire format.

use traceql::{Condition, FilterValue, Selector};

use super::error::QuerierError;

/// Which selector a Tempo `tags` key denotes.
///
/// Deliberately a copy of the vocabulary TraceQL uses for unscoped keys rather
/// than a call into `traceql`. The two agree today by coincidence: `tags` is a
/// frozen logfmt wire format, while TraceQL's intrinsics evolve with the
/// language on its own release cadence. Sharing one function would let a new
/// TraceQL intrinsic silently redefine what an existing `tags=` request means.
fn tags_selector(key: &str) -> Selector {
    match key {
        "service.name" => Selector::ServiceName,
        "name" => Selector::SpanName,
        "status" => Selector::Status,
        "kind" => Selector::Kind,
        _ => Selector::AnyAttribute(key.to_string()),
    }
}

/// Parse Tempo's logfmt `tags` parameter: space-separated `key=value`
/// pairs with optionally double-quoted values.
pub fn parse_tags(tags: &str) -> Result<Vec<Condition>, QuerierError> {
    let mut conditions = Vec::new();
    let mut rest = tags.trim();
    while !rest.is_empty() {
        let (key, after_key) = rest.split_once('=').ok_or_else(|| {
            QuerierError::InvalidInput(format!(
                "Invalid tags expression near '{rest}': expected key=value pairs"
            ))
        })?;
        let key = key.trim();
        if key.is_empty() {
            return Err(QuerierError::InvalidInput(
                "Invalid tags expression: empty key".to_string(),
            ));
        }
        let (raw_value, remainder) = take_value(after_key)?;
        conditions.push(Condition {
            selector: tags_selector(key),
            value: FilterValue::String(raw_value),
        });
        rest = remainder.trim_start();
    }
    if conditions.is_empty() {
        return Err(QuerierError::InvalidInput(
            "Empty tags expression".to_string(),
        ));
    }
    Ok(conditions)
}

/// Take one logfmt value (quoted or bare) from the front of `input`,
/// returning the value and the remainder.
fn take_value(input: &str) -> Result<(String, &str), QuerierError> {
    if let Some(quoted) = input.strip_prefix('"') {
        let end = quoted.find('"').ok_or_else(|| {
            QuerierError::InvalidInput("Unterminated quoted value in tags".to_string())
        })?;
        Ok((quoted[..end].to_string(), &quoted[end + 1..]))
    } else {
        let end = input.find(char::is_whitespace).unwrap_or(input.len());
        Ok((input[..end].to_string(), &input[end..]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tags_single_pair_maps_service_name() {
        let conditions = parse_tags("service.name=user-service").unwrap();
        assert_eq!(
            conditions,
            vec![Condition {
                selector: Selector::ServiceName,
                value: FilterValue::String("user-service".to_string()),
            }]
        );
    }

    #[test]
    fn tags_multiple_pairs_with_quotes() {
        let conditions = parse_tags(r#"http.method=GET env="prod east" name=checkout"#).unwrap();
        assert_eq!(conditions.len(), 3);
        assert_eq!(
            conditions[0].selector,
            Selector::AnyAttribute("http.method".to_string())
        );
        assert_eq!(
            conditions[1].value,
            FilterValue::String("prod east".to_string())
        );
        assert_eq!(conditions[2].selector, Selector::SpanName);
    }

    #[test]
    fn tags_without_equals_is_invalid_input() {
        assert!(matches!(
            parse_tags("justaword"),
            Err(QuerierError::InvalidInput(_))
        ));
        assert!(matches!(parse_tags(""), Err(QuerierError::InvalidInput(_))));
    }

    /// The parser's rejection classes must reach the caller as the statuses
    /// they imply: unparseable input is a client error, an unimplemented
    /// construct is not.
    #[test]
    fn parse_errors_map_to_their_status_class() {
        let syntax: QuerierError = traceql::parse(r#"name = "no-braces""#).unwrap_err().into();
        assert!(matches!(syntax, QuerierError::InvalidInput(_)), "{syntax}");

        let unsupported: QuerierError = traceql::parse(r#"{ span.x != "y" }"#).unwrap_err().into();
        assert!(
            matches!(unsupported, QuerierError::Unsupported(_)),
            "{unsupported}"
        );
    }
}
