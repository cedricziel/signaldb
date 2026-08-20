//! # Trace search filter lowering
//!
//! Turns parsed trace-search conditions into DataFusion filter expressions
//! over the traces table. Recognising TraceQL is not done here — that is the
//! [`traceql`] crate, which knows nothing about columns or storage. This
//! module owns the other half: which physical representation answers a given
//! [`traceql::Selector`].
//!
//! It also parses Tempo's `tags` parameter (space-separated logfmt
//! `key=value` pairs, values optionally double-quoted). That is an HTTP
//! parameter encoding rather than a TraceQL construct — it merely produces the
//! same [`traceql::Condition`] values — so it stays on this side of the
//! boundary.
//!
//! ## How an attribute is matched
//!
//! Three representations, in preference order:
//!
//! | Table shape | Expression |
//! |-------------|------------|
//! | attribute promoted to a column | equality on `label_<key>` |
//! | map-typed attribute columns | `get_field(col, key)` equality |
//! | legacy flat-JSON columns | substring match on the serialized `"key":value` fragment |
//!
//! The JSON fallback can over-match when another attribute's serialized text
//! embeds the same fragment — a documented approximation that disappears as
//! tables move to map-typed attributes.

use common::schema::materialized_column_name;
use datafusion::functions::string::expr_fn::contains;
use datafusion::logical_expr::{Expr, col, lit};
use traceql::{Condition, FilterValue, Selector};

use super::error::QuerierError;
use super::logql::{AttrContext, MaterializedColumns};

/// Lower a condition to a DataFusion filter expression, routing an attribute
/// key to its materialized `label_<key>` column (exact match) when the table
/// has one; otherwise to per-key `get_field` extraction on map-typed attribute
/// tables, else the attribute-JSON substring match (legacy tables).
///
/// A free function rather than a method: `Condition` is defined in `traceql`,
/// which must not know what a DataFusion expression is.
pub fn to_expr(condition: &Condition, ctx: &AttrContext) -> Result<Expr, QuerierError> {
    let columns: &MaterializedColumns = &ctx.materialized;
    match &condition.selector {
        Selector::ServiceName => Ok(col("service_name").eq(lit(string_value(condition)?))),
        Selector::SpanName => Ok(col("span_name").eq(lit(string_value(condition)?))),
        Selector::Status => {
            let status = match string_value(condition)?.to_ascii_lowercase().as_str() {
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
        Selector::Kind => {
            let kind = match string_value(condition)?.to_ascii_lowercase().as_str() {
                "internal" => "Internal",
                "server" => "Server",
                "client" => "Client",
                "producer" => "Producer",
                "consumer" => "Consumer",
                other => {
                    return Err(QuerierError::InvalidInput(format!(
                        "Unknown kind value '{other}' (expected internal, server, client, producer, or consumer)"
                    )));
                }
            };
            Ok(col("span_kind").eq(lit(kind)))
        }
        Selector::SpanAttribute(key)
        | Selector::ResourceAttribute(key)
        | Selector::AnyAttribute(key)
            if columns.contains(&materialized_column_name(key)) =>
        {
            materialized_expr(&materialized_column_name(key), &condition.value)
        }
        Selector::SpanAttribute(key) if ctx.map_attrs => {
            map_attribute_expr("span_attributes", key, &condition.value)
        }
        Selector::ResourceAttribute(key) if ctx.map_attrs => {
            map_attribute_expr("resource_attributes", key, &condition.value)
        }
        Selector::AnyAttribute(key) if ctx.map_attrs => {
            Ok(
                map_attribute_expr("span_attributes", key, &condition.value)?.or(
                    map_attribute_expr("resource_attributes", key, &condition.value)?,
                ),
            )
        }
        Selector::SpanAttribute(key) => attribute_expr("span_attributes", key, &condition.value),
        Selector::ResourceAttribute(key) => {
            attribute_expr("resource_attributes", key, &condition.value)
        }
        Selector::AnyAttribute(key) => {
            Ok(
                attribute_expr("span_attributes", key, &condition.value)?.or(attribute_expr(
                    "resource_attributes",
                    key,
                    &condition.value,
                )?),
            )
        }
        // `Selector` is `#[non_exhaustive]`: the parser releases on its own
        // cadence, so it can recognise a selector this build cannot lower.
        // Saying so beats filtering on the wrong column.
        other => Err(QuerierError::Unsupported(format!(
            "TraceQL selector {other:?} is recognised but not lowered by this build"
        ))),
    }
}

fn string_value(condition: &Condition) -> Result<String, QuerierError> {
    match &condition.value {
        FilterValue::String(s) => Ok(s.clone()),
        other => Err(QuerierError::InvalidInput(format!(
            "Selector {:?} requires a string value, got {other:?}",
            condition.selector
        ))),
    }
}

/// Exact equality against a materialized attribute column, comparing the
/// value as its stored string form.
fn materialized_expr(column: &str, value: &FilterValue) -> Result<Expr, QuerierError> {
    Ok(col(column).eq(lit(stored_string(value)?)))
}

/// Exact per-key equality on a map-typed attribute column. Values are
/// stored as strings (numbers/bools rendered bare at ingest), so every
/// filter value compares by its string form.
fn map_attribute_expr(column: &str, key: &str, value: &FilterValue) -> Result<Expr, QuerierError> {
    use datafusion::functions::core::expr_fn::get_field;
    Ok(get_field(col(column), key).eq(lit(stored_string(value)?)))
}

/// A filter value as ingest rendered it into a string-typed column.
///
/// Fallible for the same reason [`to_expr`] is: `FilterValue` is
/// `#[non_exhaustive]`, and a value kind this build cannot render must be
/// reported, not guessed at. Rendering an unknown variant via `Debug` would
/// produce a filter that silently matches nothing — the failure mode this
/// module exists to avoid.
fn stored_string(value: &FilterValue) -> Result<String, QuerierError> {
    match value {
        FilterValue::String(s) => Ok(s.clone()),
        FilterValue::Number(n) => Ok(n.clone()),
        FilterValue::Bool(b) => Ok(b.to_string()),
        other => Err(QuerierError::Unsupported(format!(
            "TraceQL value {other:?} is recognised but not lowered by this build"
        ))),
    }
}

/// Match the serialized `"key":value` fragment inside a flat-JSON
/// attribute column. Non-string OTLP values serialize unquoted
/// (`{"http.status_code":200}`), so numbers and bools match both the
/// bare and the quoted form.
fn attribute_expr(column: &str, key: &str, value: &FilterValue) -> Result<Expr, QuerierError> {
    // serde_json::to_string on &str cannot fail.
    let json_key = serde_json::to_string(key).unwrap_or_else(|_| format!("\"{key}\""));
    match value {
        FilterValue::String(s) => {
            let json_value = serde_json::to_string(s).unwrap_or_else(|_| format!("\"{s}\""));
            Ok(contains(
                col(column),
                lit(format!("{json_key}:{json_value}")),
            ))
        }
        other => {
            let bare = stored_string(other)?;
            Ok(contains(col(column), lit(format!("{json_key}:{bare}")))
                .or(contains(col(column), lit(format!("{json_key}:\"{bare}\"")))))
        }
    }
}

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

    #[test]
    fn status_maps_to_storage_values() {
        let condition = Condition {
            selector: Selector::Status,
            value: FilterValue::String("error".to_string()),
        };
        let expr = to_expr(&condition, &AttrContext::default()).unwrap();
        assert!(format!("{expr:?}").contains("Error"));

        let bad = Condition {
            selector: Selector::Status,
            value: FilterValue::String("bogus".to_string()),
        };
        assert!(matches!(
            to_expr(&bad, &AttrContext::default()),
            Err(QuerierError::InvalidInput(_))
        ));
    }

    #[test]
    fn kind_maps_to_storage_values() {
        let condition = Condition {
            selector: Selector::Kind,
            value: FilterValue::String("server".to_string()),
        };
        let expr = to_expr(&condition, &AttrContext::default()).unwrap();
        assert!(format!("{expr:?}").contains("Server"));

        let bad = Condition {
            selector: Selector::Kind,
            value: FilterValue::String("bogus".to_string()),
        };
        assert!(matches!(
            to_expr(&bad, &AttrContext::default()),
            Err(QuerierError::InvalidInput(_))
        ));
    }

    #[test]
    fn attribute_expr_matches_serialized_fragment() {
        let condition = Condition {
            selector: Selector::SpanAttribute("http.method".to_string()),
            value: FilterValue::String("GET".to_string()),
        };
        let rendered = format!(
            "{:?}",
            to_expr(&condition, &AttrContext::default()).unwrap()
        );
        assert!(rendered.contains(r#""http.method":"GET""#), "{rendered}");

        // Numbers match both bare (real OTLP int) and quoted forms.
        let numeric = Condition {
            selector: Selector::SpanAttribute("http.status_code".to_string()),
            value: FilterValue::Number("200".to_string()),
        };
        let rendered = format!("{:?}", to_expr(&numeric, &AttrContext::default()).unwrap());
        assert!(rendered.contains(r#""http.status_code":200"#), "{rendered}");
        assert!(
            rendered.contains(r#""http.status_code":"200""#),
            "{rendered}"
        );
    }

    #[test]
    fn map_attribute_tables_use_get_field_extraction() {
        let ctx = AttrContext {
            materialized: MaterializedColumns::new(),
            map_attrs: true,
            ..Default::default()
        };
        let condition = Condition {
            selector: Selector::AnyAttribute("namespace".to_string()),
            value: FilterValue::String("prod".to_string()),
        };
        let rendered = format!("{:?}", to_expr(&condition, &ctx).unwrap());
        assert!(rendered.contains("GetFieldFunc"), "{rendered}");
        assert!(!rendered.contains("ContainsFunc"), "{rendered}");

        // Numbers compare by their bare string form (ingest rendering).
        let numeric = Condition {
            selector: Selector::SpanAttribute("http.status_code".to_string()),
            value: FilterValue::Number("200".to_string()),
        };
        let rendered = format!("{:?}", to_expr(&numeric, &ctx).unwrap());
        assert!(rendered.contains(r#"Utf8("200")"#), "{rendered}");
    }

    #[test]
    fn materialized_attribute_routes_to_its_column() {
        let condition = Condition {
            selector: Selector::AnyAttribute("namespace".to_string()),
            value: FilterValue::String("prod".to_string()),
        };
        // Without the column: JSON substring across both attribute columns.
        let json = format!(
            "{:?}",
            to_expr(&condition, &AttrContext::default()).unwrap()
        );
        assert!(json.contains(r#""namespace":"prod""#), "{json}");

        // With the column: exact equality on `label_namespace`.
        let ctx = AttrContext {
            materialized: ["label_namespace".to_string()].into_iter().collect(),
            ..Default::default()
        };
        let rendered = format!("{:?}", to_expr(&condition, &ctx).unwrap());
        assert!(rendered.contains("label_namespace"), "{rendered}");
        assert!(!rendered.contains("span_attributes"), "{rendered}");
    }
}
