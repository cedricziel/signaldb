//! TraceQL → IR.
//!
//! The supported subset is a single spanset of `&&`-conjoined equality
//! matchers, which is exactly a `where` stage of conjoined equality leaves —
//! so this lowering is a rename of vocabulary, not a translation of structure.
//!
//! The one judgement it makes is **field naming**: TraceQL's spelling
//! (`name`, `status`, `span.<k>`) becomes the logical names the IR declares
//! for the `traces` source. TraceQL's scoping survives intact, because the
//! IR's container qualifiers mean the same thing TraceQL's scopes do —
//! `span.http.method` addresses the span container in both, and a bare name
//! coalesces in both.

use query_ir::{ComparisonOp, Document, Leaf, Predicate, ResultEnvelope, Stage};

use crate::LowerError;

/// The IR version this lowering targets. Bumped only when the emitted shape
/// needs a stage or envelope introduced by a later version.
const IR_VERSION: i64 = 1;

/// Lower a TraceQL query into an IR document over the `traces` source.
///
/// `from`/`to` are IR range literals — RFC-3339, a relative anchor such as
/// `now-1h`, or integer nanoseconds. They are passed through unresolved; the
/// router resolves relative anchors to one absolute window at the ticket
/// boundary, and duplicating that here would let the two disagree.
pub fn traceql_to_ir(query: &str, from: &str, to: &str) -> Result<Document, LowerError> {
    let conditions = traceql::parse(query)?;

    let pipeline = match conditions.len() {
        // `{}` selects everything. No predicate at all, rather than one that
        // is vacuously true: an empty `and` would be a claim about the rows.
        0 => Vec::new(),
        1 => vec![Stage::Where(traceql_condition_to_predicate(
            &conditions[0],
        )?)],
        _ => {
            let parts = conditions
                .iter()
                .map(traceql_condition_to_predicate)
                .collect::<Result<Vec<_>, _>>()?;
            vec![Stage::Where(Predicate::And(parts))]
        }
    };

    Ok(Document {
        ir_version: IR_VERSION,
        from: "traces".to_string(),
        range: crate::ir_range(from, to),
        result: ResultEnvelope::Rows,
        // No curated projection: the server's bounded default for `traces` is
        // what a Tempo-shaped caller wants, and naming columns here would
        // freeze a projection this crate has no basis to choose.
        fields: None,
        pipeline,
    })
}

/// Lower one TraceQL condition to an IR equality leaf.
///
/// Public so callers with a [`traceql::Condition`] but no TraceQL text of
/// their own — Tempo's `tags` HTTP parameter, lowered by the querier's
/// `tags_to_ir` shim (design D4 of `ir-single-lowering`) — can reuse this
/// exact field-naming and status/kind normalization instead of rendering a
/// condition back into TraceQL text and re-parsing it. A round trip through
/// text is unsound: TraceQL's own string-literal grammar has no escape
/// syntax (see `traceql::parser::parse_value`), so a value containing a
/// backslash, an embedded quote, or (via an unescaped key) `&&` either
/// silently changes value or corrupts the parsed structure. Lowering the
/// already-parsed `Condition` directly has no such step.
///
/// `status`/`kind` get their value normalized rather than passed through
/// [`literal`] — see [`normalize_status`]/[`normalize_kind`].
pub fn traceql_condition_to_predicate(
    condition: &traceql::Condition,
) -> Result<Predicate, LowerError> {
    let value = match condition.selector {
        traceql::Selector::Status => {
            serde_json::Value::String(normalize_status(&condition.value)?.to_string())
        }
        traceql::Selector::Kind => {
            serde_json::Value::String(normalize_kind(&condition.value)?.to_string())
        }
        _ => literal(&condition.value)?,
    };
    Ok(Predicate::Leaf(Leaf {
        field: field_name(&condition.selector)?,
        op: ComparisonOp::Eq,
        value: Some(value),
    }))
}

/// Canonicalize a `status` matcher's value to the form span status is
/// actually stored in (`status_code_to_str` in
/// `common::flight::conversion::conversion_traces`: `"Ok"`/`"Error"`/
/// `"Unspecified"`). TraceQL spells the value lowercase
/// (`status = error`); comparing that spelling against the stored column
/// would never match a real row. `search_filter::to_expr` (the compat
/// lowering this crate is meant to replace) already normalizes the same
/// way — this mirrors it so both paths agree, a gap the `ir-single-lowering`
/// differential harness found.
fn normalize_status(value: &traceql::FilterValue) -> Result<&'static str, LowerError> {
    let traceql::FilterValue::String(s) = value else {
        return Err(LowerError::Inexpressible(format!(
            "TraceQL status value {value:?}"
        )));
    };
    Ok(match s.to_ascii_lowercase().as_str() {
        "ok" => "Ok",
        "error" => "Error",
        "unset" | "unspecified" => "Unspecified",
        _ => {
            return Err(LowerError::Inexpressible(format!(
                "TraceQL status value '{s}'"
            )));
        }
    })
}

/// Canonicalize a `kind` matcher's value to the form span kind is actually
/// stored in (`span_kind_to_str`): `"Internal"`/`"Server"`/`"Client"`/
/// `"Producer"`/`"Consumer"`. See [`normalize_status`] for why this can't be
/// a plain [`literal`] passthrough.
fn normalize_kind(value: &traceql::FilterValue) -> Result<&'static str, LowerError> {
    let traceql::FilterValue::String(s) = value else {
        return Err(LowerError::Inexpressible(format!(
            "TraceQL kind value {value:?}"
        )));
    };
    Ok(match s.to_ascii_lowercase().as_str() {
        "internal" => "Internal",
        "server" => "Server",
        "client" => "Client",
        "producer" => "Producer",
        "consumer" => "Consumer",
        _ => {
            return Err(LowerError::Inexpressible(format!(
                "TraceQL kind value '{s}'"
            )));
        }
    })
}

/// The logical field a TraceQL selector addresses.
///
/// Intrinsics are renamed to the names `LogicalSchema::core()` declares for
/// `traces`; attributes keep their key and carry their scope as the IR's
/// container qualifier.
fn field_name(selector: &traceql::Selector) -> Result<String, LowerError> {
    Ok(match selector {
        traceql::Selector::ServiceName => "service.name".to_string(),
        traceql::Selector::SpanName => "span.name".to_string(),
        traceql::Selector::Status => "status.code".to_string(),
        traceql::Selector::Kind => "span_kind".to_string(),
        traceql::Selector::SpanAttribute(key) => format!("span.{key}"),
        traceql::Selector::ResourceAttribute(key) => format!("resource.{key}"),
        // Unscoped: a bare name, which the IR resolver coalesces across
        // containers — the same semantics as TraceQL's leading `.`.
        traceql::Selector::AnyAttribute(key) => key.clone(),
        // `Selector` is `#[non_exhaustive]`; the parser releases on its own
        // cadence and may recognise a selector this build cannot name.
        other => {
            return Err(LowerError::Inexpressible(format!(
                "TraceQL selector {other:?}"
            )));
        }
    })
}

/// A matcher's right-hand side as a JSON literal.
///
/// Types are preserved rather than stringified: the IR has a type system, and
/// `500` compared as text would not match a numeric field.
fn literal(value: &traceql::FilterValue) -> Result<serde_json::Value, LowerError> {
    Ok(match value {
        traceql::FilterValue::String(s) => serde_json::Value::String(s.clone()),
        traceql::FilterValue::Bool(b) => serde_json::Value::Bool(*b),
        traceql::FilterValue::Number(n) => n
            .parse::<serde_json::Number>()
            .map(serde_json::Value::Number)
            .map_err(|_| LowerError::Inexpressible(format!("TraceQL numeric literal '{n}'")))?,
        other => {
            return Err(LowerError::Inexpressible(format!(
                "TraceQL value {other:?}"
            )));
        }
    })
}
