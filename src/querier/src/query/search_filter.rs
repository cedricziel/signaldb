//! # Trace Search Filters
//!
//! Parses the Tempo search parameters `tags` (logfmt pairs) and `q`
//! (TraceQL) into DataFusion filter expressions over the traces table.
//!
//! Only an equality subset is supported; anything else is rejected with
//! an explicit error instead of silently returning unfiltered results
//! (issue #551):
//!
//! - `tags`: space-separated `key=value` pairs, values optionally
//!   double-quoted (logfmt).
//! - `q`: `{ selector = value && ... }` where a selector is an intrinsic
//!   (`name`, `status`, `.service.name`/`resource.service.name`), a span
//!   attribute (`span.key` / `.key`), or a resource attribute
//!   (`resource.key`).
//!
//! Intrinsics filter dedicated columns. Attributes are stored as flat
//! JSON objects in the `span_attributes` / `resource_attributes` string
//! columns, so attribute equality is implemented as a substring match on
//! the serialized `"key":value` fragment. That can over-match when
//! another attribute's serialized text embeds the same fragment — a
//! documented approximation until attributes are indexed.

use datafusion::functions::string::expr_fn::contains;
use datafusion::logical_expr::{Expr, col, lit};

use super::error::QuerierError;

/// Where a filter condition applies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selector {
    /// The dedicated `service_name` column.
    ServiceName,
    /// The dedicated `span_name` column.
    SpanName,
    /// The dedicated `status_code` column.
    Status,
    /// A span attribute (JSON column `span_attributes`).
    SpanAttribute(String),
    /// A resource attribute (JSON column `resource_attributes`).
    ResourceAttribute(String),
    /// An attribute that may live on the span or the resource.
    AnyAttribute(String),
}

/// A parsed equality condition.
#[derive(Debug, Clone, PartialEq)]
pub struct Condition {
    pub selector: Selector,
    pub value: FilterValue,
}

/// Supported comparison values.
#[derive(Debug, Clone, PartialEq)]
pub enum FilterValue {
    String(String),
    Number(String),
    Bool(bool),
}

impl Condition {
    /// Lower the condition to a DataFusion filter expression.
    pub fn to_expr(&self) -> Result<Expr, QuerierError> {
        match &self.selector {
            Selector::ServiceName => Ok(col("service_name").eq(lit(self.string_value()?))),
            Selector::SpanName => Ok(col("span_name").eq(lit(self.string_value()?))),
            Selector::Status => {
                let status = match self.string_value()?.to_ascii_lowercase().as_str() {
                    "ok" => "Ok",
                    "error" => "Error",
                    "unset" | "unspecified" => "Unspecified",
                    other => {
                        return Err(QuerierError::InvalidInput(format!(
                            "Unknown status value '{other}' (expected ok, error, or unset)"
                        )));
                    }
                };
                Ok(col("status_code").eq(lit(status)))
            }
            Selector::SpanAttribute(key) => Ok(attribute_expr("span_attributes", key, &self.value)),
            Selector::ResourceAttribute(key) => {
                Ok(attribute_expr("resource_attributes", key, &self.value))
            }
            Selector::AnyAttribute(key) => Ok(attribute_expr("span_attributes", key, &self.value)
                .or(attribute_expr("resource_attributes", key, &self.value))),
        }
    }

    fn string_value(&self) -> Result<String, QuerierError> {
        match &self.value {
            FilterValue::String(s) => Ok(s.clone()),
            other => Err(QuerierError::InvalidInput(format!(
                "Selector {:?} requires a string value, got {other:?}",
                self.selector
            ))),
        }
    }
}

/// Match the serialized `"key":value` fragment inside a flat-JSON
/// attribute column. Non-string OTLP values serialize unquoted
/// (`{"http.status_code":200}`), so numbers and bools match both the
/// bare and the quoted form.
fn attribute_expr(column: &str, key: &str, value: &FilterValue) -> Expr {
    // serde_json::to_string on &str cannot fail.
    let json_key = serde_json::to_string(key).unwrap_or_else(|_| format!("\"{key}\""));
    match value {
        FilterValue::String(s) => {
            let json_value = serde_json::to_string(s).unwrap_or_else(|_| format!("\"{s}\""));
            contains(col(column), lit(format!("{json_key}:{json_value}")))
        }
        FilterValue::Number(n) => contains(col(column), lit(format!("{json_key}:{n}")))
            .or(contains(col(column), lit(format!("{json_key}:\"{n}\"")))),
        FilterValue::Bool(b) => contains(col(column), lit(format!("{json_key}:{b}")))
            .or(contains(col(column), lit(format!("{json_key}:\"{b}\"")))),
    }
}

/// Map an attribute-style key that may name an intrinsic to its selector.
fn unscoped_selector(key: &str) -> Selector {
    match key {
        "service.name" => Selector::ServiceName,
        "name" => Selector::SpanName,
        "status" => Selector::Status,
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
            selector: unscoped_selector(key),
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

/// Parse the supported TraceQL subset:
/// `{ selector = value && selector = value ... }`.
///
/// Anything outside the subset (other operators, `||`, nesting, pipeline
/// stages, duration comparisons) returns [`QuerierError::Unsupported`] so
/// clients get an explicit 501 instead of silently unfiltered results.
pub fn parse_traceql(q: &str) -> Result<Vec<Condition>, QuerierError> {
    let trimmed = q.trim();
    let inner = trimmed
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or_else(|| {
            QuerierError::Unsupported(format!(
                "Unsupported TraceQL query '{q}': only a single {{ ... }} spanset is supported"
            ))
        })?;

    if inner.contains("||") {
        return Err(QuerierError::Unsupported(
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
        conditions.push(parse_traceql_clause(clause.trim())?);
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

/// Parse one `selector = value` clause of the TraceQL subset.
fn parse_traceql_clause(clause: &str) -> Result<Condition, QuerierError> {
    for unsupported in ["!=", ">=", "<=", "=~", "!~", ">", "<"] {
        if clause.contains(unsupported) {
            return Err(QuerierError::Unsupported(format!(
                "TraceQL operator '{unsupported}' is not supported yet (clause: '{clause}')"
            )));
        }
    }
    let (lhs, rhs) = clause.split_once('=').ok_or_else(|| {
        QuerierError::Unsupported(format!(
            "Unsupported TraceQL clause '{clause}': only equality matchers are supported"
        ))
    })?;
    let lhs = lhs.trim();
    let rhs = rhs.trim();

    let selector = if lhs == "name" {
        Selector::SpanName
    } else if lhs == "status" {
        Selector::Status
    } else if lhs == "duration" {
        return Err(QuerierError::Unsupported(
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
        return Err(QuerierError::Unsupported(format!(
            "Unsupported TraceQL selector '{lhs}'"
        )));
    };

    let value = parse_traceql_value(rhs)?;
    Ok(Condition { selector, value })
}

fn parse_traceql_value(raw: &str) -> Result<FilterValue, QuerierError> {
    if let Some(quoted) = raw.strip_prefix('"') {
        let inner = quoted.strip_suffix('"').ok_or_else(|| {
            QuerierError::InvalidInput(format!("Unterminated string literal '{raw}'"))
        })?;
        if inner.contains('"') {
            return Err(QuerierError::InvalidInput(format!(
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
    // Bare identifiers are only meaningful for `status`.
    if !raw.is_empty() && raw.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Ok(FilterValue::String(raw.to_string()));
    }
    Err(QuerierError::InvalidInput(format!(
        "Unsupported TraceQL value '{raw}'"
    )))
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

    #[test]
    fn traceql_equality_subset_parses() {
        let conditions =
            parse_traceql(r#"{ resource.service.name = "api" && span.http.method = "GET" }"#)
                .unwrap();
        assert_eq!(conditions.len(), 2);
        assert_eq!(conditions[0].selector, Selector::ServiceName);
        assert_eq!(
            conditions[1].selector,
            Selector::SpanAttribute("http.method".to_string())
        );
    }

    #[test]
    fn traceql_intrinsics_and_numbers() {
        let conditions = parse_traceql(
            r#"{ name = "GET /api" && status = error && span.http.status_code = 500 }"#,
        )
        .unwrap();
        assert_eq!(conditions[0].selector, Selector::SpanName);
        assert_eq!(conditions[1].selector, Selector::Status);
        assert_eq!(conditions[2].value, FilterValue::Number("500".to_string()));
    }

    #[test]
    fn traceql_empty_spanset_matches_everything() {
        assert!(parse_traceql("{}").unwrap().is_empty());
        assert!(parse_traceql("{   }").unwrap().is_empty());
    }

    #[test]
    fn traceql_unsupported_operators_are_rejected_as_unsupported() {
        for q in [
            r#"{ duration > 100ms }"#,
            r#"{ span.x != "y" }"#,
            r#"{ span.x =~ "y.*" }"#,
            r#"{ span.a = "1" || span.b = "2" }"#,
            r#"name = "no-braces""#,
        ] {
            assert!(
                matches!(parse_traceql(q), Err(QuerierError::Unsupported(_))),
                "expected Unsupported for {q}"
            );
        }
    }

    #[test]
    fn traceql_ampersand_inside_quotes_is_preserved() {
        let conditions = parse_traceql(r#"{ span.query = "a && b" }"#).unwrap();
        assert_eq!(conditions.len(), 1);
        assert_eq!(
            conditions[0].value,
            FilterValue::String("a && b".to_string())
        );
    }

    #[test]
    fn status_maps_to_storage_values() {
        let condition = Condition {
            selector: Selector::Status,
            value: FilterValue::String("error".to_string()),
        };
        let expr = condition.to_expr().unwrap();
        assert!(format!("{expr:?}").contains("Error"));

        let bad = Condition {
            selector: Selector::Status,
            value: FilterValue::String("bogus".to_string()),
        };
        assert!(matches!(bad.to_expr(), Err(QuerierError::InvalidInput(_))));
    }

    #[test]
    fn attribute_expr_matches_serialized_fragment() {
        let condition = Condition {
            selector: Selector::SpanAttribute("http.method".to_string()),
            value: FilterValue::String("GET".to_string()),
        };
        let rendered = format!("{:?}", condition.to_expr().unwrap());
        assert!(rendered.contains(r#""http.method":"GET""#), "{rendered}");

        // Numbers match both bare (real OTLP int) and quoted forms.
        let numeric = Condition {
            selector: Selector::SpanAttribute("http.status_code".to_string()),
            value: FilterValue::Number("200".to_string()),
        };
        let rendered = format!("{:?}", numeric.to_expr().unwrap());
        assert!(rendered.contains(r#""http.status_code":200"#), "{rendered}");
        assert!(
            rendered.contains(r#""http.status_code":"200""#),
            "{rendered}"
        );
    }
}
