//! # Query IR value types, coercion, and three-valued logic
//!
//! The IR's meaning is defined here, independently of the DataFusion plan it
//! lowers to. This module owns:
//!
//! - [`ValueType`] — the canonical value types a logical field or literal can
//!   have. Every logical field has exactly one canonical `ValueType`, owned by
//!   the attribute registry (never inferred from an incidental physical column).
//! - [`Literal`] — a coerced literal value, produced by [`coerce`] against a
//!   target `ValueType`. Coercion is total or the query is rejected at
//!   validation; there is never a silent runtime cast.
//! - [`Truth`] — the three-valued (Kleene) logic in which `absent` is a first
//!   class truth value that propagates through the connectives, so a query's
//!   result never depends on the execution engine's own NULL handling.

use std::fmt;

use serde::{Deserialize, Serialize};

/// The canonical value types that flow through the IR.
///
/// Each logical field has exactly one canonical `ValueType`, owned by the
/// attribute registry. Literal coercion always targets a field's canonical
/// type — see [`coerce`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueType {
    String,
    Int64,
    Float64,
    Bool,
    /// Nanoseconds since the Unix epoch.
    TimestampNs,
    /// A duration in nanoseconds.
    DurationNs,
    /// Raw bytes, encoded as base64 on the wire.
    Bytes,
    /// A homogeneous array of a single element type.
    Array(Box<ValueType>),
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValueType::String => write!(f, "string"),
            ValueType::Int64 => write!(f, "int64"),
            ValueType::Float64 => write!(f, "float64"),
            ValueType::Bool => write!(f, "bool"),
            ValueType::TimestampNs => write!(f, "timestamp_ns"),
            ValueType::DurationNs => write!(f, "duration_ns"),
            ValueType::Bytes => write!(f, "bytes"),
            ValueType::Array(inner) => write!(f, "array<{inner}>"),
        }
    }
}

/// A relative time anchor such as `now`, `now-1h`, `now+30m`.
///
/// Relative anchoring is **not** resolved at validation time — only its syntax
/// is checked (coercibility). The router resolves every relative anchor to one
/// absolute nanosecond boundary at the ticket boundary, so `now-1h` means one
/// hour before a single server-stamped clock reading and every stage of the
/// plan sees identical bounds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelativeTime {
    /// Signed offset, in nanoseconds, applied to `now`. `now` itself is `0`.
    pub offset_ns: i64,
}

impl RelativeTime {
    /// Resolve against an absolute `now` (nanoseconds since epoch).
    pub fn resolve(&self, now_ns: i64) -> i64 {
        now_ns.saturating_add(self.offset_ns)
    }
}

/// A coerced timestamp literal: either an absolute instant or a relative anchor
/// that the planner resolves against an injected clock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampLiteral {
    Absolute(i64),
    Relative(RelativeTime),
}

impl TimestampLiteral {
    /// Resolve to an absolute instant. Absolute literals ignore `now_ns`.
    pub fn resolve(&self, now_ns: i64) -> i64 {
        match self {
            TimestampLiteral::Absolute(ns) => *ns,
            TimestampLiteral::Relative(rel) => rel.resolve(now_ns),
        }
    }
}

/// A coerced literal value. Produced by [`coerce`]; the shape mirrors
/// [`ValueType`]. Timestamps stay symbolic ([`TimestampLiteral`]) until the
/// planner resolves relative anchors against the injected clock.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Literal {
    String(String),
    Int64(i64),
    Float64(f64),
    Bool(bool),
    Timestamp(TimestampLiteral),
    /// Duration in nanoseconds.
    Duration(i64),
    Bytes(Vec<u8>),
    Array(Vec<Literal>),
}

impl Literal {
    /// The [`ValueType`] this literal inhabits.
    pub fn value_type(&self) -> ValueType {
        match self {
            Literal::String(_) => ValueType::String,
            Literal::Int64(_) => ValueType::Int64,
            Literal::Float64(_) => ValueType::Float64,
            Literal::Bool(_) => ValueType::Bool,
            Literal::Timestamp(_) => ValueType::TimestampNs,
            Literal::Duration(_) => ValueType::DurationNs,
            Literal::Bytes(_) => ValueType::Bytes,
            Literal::Array(items) => ValueType::Array(Box::new(
                items
                    .first()
                    .map(Literal::value_type)
                    .unwrap_or(ValueType::String),
            )),
        }
    }
}

/// Error returned when a literal cannot be coerced to a target [`ValueType`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("value {value} cannot be coerced to {target}")]
pub struct CoercionError {
    pub value: String,
    pub target: ValueType,
}

/// Coerce a JSON literal to a target [`ValueType`].
///
/// Coercion is **total or rejected** — a literal that cannot be represented in
/// the target type is an error at validation, never a silent runtime cast.
///
/// - `duration_ns` accepts a unit-suffixed string (`"500ms"`, `"2s"`) or an
///   integer number of nanoseconds.
/// - `int64`/`float64` accept a JSON number or a numeric string.
/// - `timestamp_ns` accepts RFC3339, a relative anchor (`"now-1h"`), or an
///   integer number of nanoseconds. Only well-formedness is checked here; a
///   relative anchor stays symbolic.
/// - `bytes` accepts a base64 string.
/// - `array<T>` accepts a JSON array whose elements each coerce to `T`.
pub fn coerce(value: &serde_json::Value, target: &ValueType) -> Result<Literal, CoercionError> {
    let err = || CoercionError {
        value: value.to_string(),
        target: target.clone(),
    };
    match target {
        ValueType::String => match value {
            serde_json::Value::String(s) => Ok(Literal::String(s.clone())),
            serde_json::Value::Number(n) => Ok(Literal::String(n.to_string())),
            serde_json::Value::Bool(b) => Ok(Literal::String(b.to_string())),
            _ => Err(err()),
        },
        ValueType::Int64 => match value {
            serde_json::Value::Number(n) => n.as_i64().map(Literal::Int64).ok_or_else(err),
            serde_json::Value::String(s) => s
                .trim()
                .parse::<i64>()
                .map(Literal::Int64)
                .map_err(|_| err()),
            _ => Err(err()),
        },
        ValueType::Float64 => match value {
            serde_json::Value::Number(n) => n.as_f64().map(Literal::Float64).ok_or_else(err),
            serde_json::Value::String(s) => s
                .trim()
                .parse::<f64>()
                .map(Literal::Float64)
                .map_err(|_| err()),
            _ => Err(err()),
        },
        ValueType::Bool => match value {
            serde_json::Value::Bool(b) => Ok(Literal::Bool(*b)),
            serde_json::Value::String(s) => match s.trim() {
                "true" => Ok(Literal::Bool(true)),
                "false" => Ok(Literal::Bool(false)),
                _ => Err(err()),
            },
            _ => Err(err()),
        },
        ValueType::DurationNs => match value {
            serde_json::Value::Number(n) => n.as_i64().map(Literal::Duration).ok_or_else(err),
            serde_json::Value::String(s) => {
                parse_duration_ns(s).map(Literal::Duration).ok_or_else(err)
            }
            _ => Err(err()),
        },
        ValueType::TimestampNs => match value {
            serde_json::Value::Number(n) => n
                .as_i64()
                .map(|ns| Literal::Timestamp(TimestampLiteral::Absolute(ns)))
                .ok_or_else(err),
            serde_json::Value::String(s) => parse_timestamp_literal(s).ok_or_else(err),
            _ => Err(err()),
        },
        ValueType::Bytes => match value {
            serde_json::Value::String(s) => {
                use base64::Engine as _;
                base64::engine::general_purpose::STANDARD
                    .decode(s)
                    .map(Literal::Bytes)
                    .map_err(|_| err())
            }
            _ => Err(err()),
        },
        ValueType::Array(inner) => match value {
            serde_json::Value::Array(items) => {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    out.push(coerce(item, inner)?);
                }
                Ok(Literal::Array(out))
            }
            _ => Err(err()),
        },
    }
}

/// Parse a unit-suffixed duration string (`"500ms"`, `"2s"`, `"1h30m"`) to
/// nanoseconds. Returns `None` for an unrecognised form.
pub fn parse_duration_ns(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    // Support a bare integer (interpreted as nanoseconds) for round-tripping.
    if let Ok(n) = s.parse::<i64>() {
        return Some(n);
    }
    let mut total: i64 = 0;
    let mut num = String::new();
    let mut chars = s.chars().peekable();
    let mut saw_unit = false;
    while let Some(&c) = chars.peek() {
        if c.is_ascii_digit() || c == '.' || (num.is_empty() && (c == '+' || c == '-')) {
            num.push(c);
            chars.next();
            continue;
        }
        // Accumulate the unit suffix (letters, incl. the "µ"/"u" microsecond forms).
        let mut unit = String::new();
        while let Some(&u) = chars.peek() {
            if u.is_ascii_alphabetic() || u == 'µ' {
                unit.push(u);
                chars.next();
            } else {
                break;
            }
        }
        if num.is_empty() || unit.is_empty() {
            return None;
        }
        let magnitude: f64 = num.parse().ok()?;
        let scale = unit_scale_ns(&unit)?;
        total = total.checked_add((magnitude * scale as f64).round() as i64)?;
        num.clear();
        saw_unit = true;
    }
    // A trailing bare number with no unit is not a valid duration string.
    if !num.is_empty() || !saw_unit {
        return None;
    }
    Some(total)
}

fn unit_scale_ns(unit: &str) -> Option<i64> {
    Some(match unit {
        "ns" => 1,
        "us" | "µs" | "μs" => 1_000,
        "ms" => 1_000_000,
        "s" => 1_000_000_000,
        "m" => 60 * 1_000_000_000,
        "h" => 3_600 * 1_000_000_000,
        "d" => 86_400 * 1_000_000_000,
        _ => return None,
    })
}

/// Parse a timestamp literal: RFC3339, a relative anchor (`now`, `now-1h`,
/// `now+30m`), or a bare integer of nanoseconds.
pub fn parse_timestamp_literal(s: &str) -> Option<Literal> {
    let s = s.trim();
    if let Some(rel) = parse_relative_time(s) {
        return Some(Literal::Timestamp(TimestampLiteral::Relative(rel)));
    }
    if let Ok(n) = s.parse::<i64>() {
        return Some(Literal::Timestamp(TimestampLiteral::Absolute(n)));
    }
    // RFC3339 / ISO8601 absolute instant.
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return dt
            .timestamp_nanos_opt()
            .map(|ns| Literal::Timestamp(TimestampLiteral::Absolute(ns)));
    }
    None
}

/// Parse a relative anchor of the form `now`, `now-<dur>`, or `now+<dur>`.
pub fn parse_relative_time(s: &str) -> Option<RelativeTime> {
    let s = s.trim();
    if s == "now" {
        return Some(RelativeTime { offset_ns: 0 });
    }
    let rest = s.strip_prefix("now")?;
    // Inspect the first *character* — `split_at(1)` would panic on a multi-byte
    // char (e.g. `now€1h`), and the input is client-supplied.
    let sign = match rest.chars().next()? {
        '-' => -1i64,
        '+' => 1i64,
        _ => return None,
    };
    let dur = &rest['+'.len_utf8()..]; // '+' and '-' are both 1 byte (ASCII)
    let magnitude = parse_duration_ns(dur)?;
    Some(RelativeTime {
        offset_ns: sign.checked_mul(magnitude)?,
    })
}

/// Kleene three-valued truth. `Absent` is a first-class truth value that
/// propagates through the connectives — defined here so the result of a query
/// never depends on the execution engine's own NULL handling.
///
/// ```text
///   not(absent) = absent
///   absent AND false = false     absent AND true = absent     absent AND absent = absent
///   absent OR  true  = true      absent OR  false = absent     absent OR  absent = absent
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Truth {
    True,
    False,
    Absent,
}

impl Truth {
    pub fn from_bool(b: bool) -> Self {
        if b { Truth::True } else { Truth::False }
    }

    /// Kleene negation. Also available via the `!` operator ([`std::ops::Not`]).
    pub fn negate(self) -> Self {
        match self {
            Truth::True => Truth::False,
            Truth::False => Truth::True,
            Truth::Absent => Truth::Absent,
        }
    }

    pub fn and(self, other: Truth) -> Self {
        match (self, other) {
            (Truth::False, _) | (_, Truth::False) => Truth::False,
            (Truth::True, Truth::True) => Truth::True,
            _ => Truth::Absent,
        }
    }

    pub fn or(self, other: Truth) -> Self {
        match (self, other) {
            (Truth::True, _) | (_, Truth::True) => Truth::True,
            (Truth::False, Truth::False) => Truth::False,
            _ => Truth::Absent,
        }
    }

    /// A `where` emits a row only when the top-level predicate is `True`;
    /// `absent` (like `false`) does not match.
    pub fn matches(self) -> bool {
        matches!(self, Truth::True)
    }
}

impl std::ops::Not for Truth {
    type Output = Truth;
    fn not(self) -> Truth {
        self.negate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Task 1.1 — value-type coercion.
    #[test]
    fn duration_suffix_coerces_to_duration_ns() {
        assert_eq!(
            coerce(&json!("500ms"), &ValueType::DurationNs).unwrap(),
            Literal::Duration(500_000_000)
        );
        assert_eq!(
            coerce(&json!("2s"), &ValueType::DurationNs).unwrap(),
            Literal::Duration(2_000_000_000)
        );
        assert_eq!(
            coerce(&json!("1h30m"), &ValueType::DurationNs).unwrap(),
            Literal::Duration(90 * 60 * 1_000_000_000)
        );
    }

    #[test]
    fn numeric_strings_coerce_to_number() {
        assert_eq!(
            coerce(&json!("17"), &ValueType::Int64).unwrap(),
            Literal::Int64(17)
        );
        assert_eq!(
            coerce(&json!("3.5"), &ValueType::Float64).unwrap(),
            Literal::Float64(3.5)
        );
    }

    #[test]
    fn rfc3339_and_relative_coerce_to_timestamp() {
        let abs = coerce(&json!("2026-01-02T03:04:05Z"), &ValueType::TimestampNs).unwrap();
        assert!(matches!(
            abs,
            Literal::Timestamp(TimestampLiteral::Absolute(_))
        ));
        let rel = coerce(&json!("now-1h"), &ValueType::TimestampNs).unwrap();
        match rel {
            Literal::Timestamp(TimestampLiteral::Relative(r)) => {
                assert_eq!(r.offset_ns, -3_600 * 1_000_000_000);
            }
            other => panic!("expected relative timestamp, got {other:?}"),
        }
    }

    #[test]
    fn uncoercible_literal_is_rejected_not_silently_cast() {
        assert!(coerce(&json!("not-a-number"), &ValueType::Int64).is_err());
        assert!(coerce(&json!("banana"), &ValueType::DurationNs).is_err());
        assert!(coerce(&json!("nope"), &ValueType::TimestampNs).is_err());
        assert!(coerce(&json!({}), &ValueType::String).is_err());
    }

    #[test]
    fn relative_time_resolves_against_injected_clock() {
        let rel = parse_relative_time("now-1h").unwrap();
        assert_eq!(
            rel.resolve(1_000_000_000_000),
            1_000_000_000_000 - 3_600_000_000_000
        );
        assert_eq!(parse_relative_time("now").unwrap().offset_ns, 0);
        assert_eq!(
            parse_relative_time("now+30m").unwrap().offset_ns,
            30 * 60 * 1_000_000_000
        );
        // A multi-byte char after `now` must not panic (client-supplied input).
        assert!(parse_relative_time("now€1h").is_none());
        assert!(parse_relative_time("nowユ").is_none());
        assert!(coerce(&json!("now€1h"), &ValueType::TimestampNs).is_err());
    }

    // Task 1.2 — absent-value semantics (the three-valued truth tables).
    #[test]
    fn not_absent_is_absent() {
        assert_eq!(Truth::Absent.negate(), Truth::Absent);
        assert_eq!(Truth::True.negate(), Truth::False);
        assert_eq!(Truth::False.negate(), Truth::True);
    }

    #[test]
    fn and_truth_table_propagates_absent() {
        assert_eq!(Truth::Absent.and(Truth::False), Truth::False);
        assert_eq!(Truth::Absent.and(Truth::True), Truth::Absent);
        assert_eq!(Truth::Absent.and(Truth::Absent), Truth::Absent);
        assert_eq!(Truth::True.and(Truth::True), Truth::True);
    }

    #[test]
    fn or_truth_table_propagates_absent() {
        assert_eq!(Truth::Absent.or(Truth::True), Truth::True);
        assert_eq!(Truth::Absent.or(Truth::False), Truth::Absent);
        assert_eq!(Truth::Absent.or(Truth::Absent), Truth::Absent);
        assert_eq!(Truth::False.or(Truth::False), Truth::False);
    }

    #[test]
    fn where_emits_only_on_true() {
        // Both `field = x` (Absent) and `not(field = x)` (not(Absent)=Absent)
        // exclude absent rows.
        assert!(!Truth::Absent.matches());
        assert!(!Truth::Absent.negate().matches());
        assert!(!Truth::False.matches());
        assert!(Truth::True.matches());
    }
}
