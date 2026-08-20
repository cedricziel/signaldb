//! The parsed form of a TraceQL spanset.
//!
//! These types describe *what the query said*, never how it is answered. A
//! [`Selector`] names an intrinsic or an attribute by the spelling TraceQL
//! uses; mapping it onto a column, an attribute map, or a materialized
//! projection is the consumer's job.

/// Where a filter condition applies.
///
/// Non-exhaustive: TraceQL gains selectors over time, and this crate releases
/// independently of anything consuming it, so a consumer may be compiled
/// against a newer grammar than it lowers. Match with a fallback arm that
/// reports the selector as unsupported rather than assuming the list is closed.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selector {
    /// `resource.service.name` / `.service.name`.
    ServiceName,
    /// The `name` intrinsic.
    SpanName,
    /// The `status` intrinsic.
    Status,
    /// The `kind` intrinsic.
    Kind,
    /// `span.<key>` — scoped to span attributes.
    SpanAttribute(String),
    /// `resource.<key>` — scoped to resource attributes.
    ResourceAttribute(String),
    /// `.<key>` — unscoped; may resolve on the span or the resource.
    AnyAttribute(String),
}

/// The right-hand side of a matcher.
///
/// `Number` keeps the literal's source text rather than a parsed numeric type:
/// the consumer decides how to compare it, and round-tripping the spelling
/// avoids inventing a precision the query never had.
///
/// Non-exhaustive for the same reason as [`Selector`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterValue {
    String(String),
    Number(String),
    Bool(bool),
}

/// One `selector = value` equality matcher.
///
/// Deliberately *not* `#[non_exhaustive]`: a matcher is exactly a selector and
/// a value, and consumers both read and construct these.
#[derive(Debug, Clone, PartialEq)]
pub struct Condition {
    pub selector: Selector,
    pub value: FilterValue,
}

/// Which selector a `.`-prefixed (unscoped) key denotes.
///
/// Private on purpose. This is TraceQL's intrinsic vocabulary and evolves with
/// the language; a consumer that happens to share the spelling today — Tempo's
/// `tags` HTTP parameter does — must keep its own mapping, so a new TraceQL
/// intrinsic cannot silently redefine an unrelated wire format.
pub(crate) fn unscoped_selector(key: &str) -> Selector {
    match key {
        "service.name" => Selector::ServiceName,
        "name" => Selector::SpanName,
        "status" => Selector::Status,
        "kind" => Selector::Kind,
        _ => Selector::AnyAttribute(key.to_string()),
    }
}
