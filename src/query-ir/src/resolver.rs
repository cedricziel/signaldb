//! # Attribute-registry resolver interface
//!
//! Every logical field is resolved to its physical location — a promoted
//! column or an attribute-map extraction — plus its canonical [`ValueType`].
//! This is a **consumer** of the attribute-registry epic (#811): a query-facing
//! *view*, not a re-implementation of the registry.
//!
//! ## Production gating (#811)
//!
//! The registry today is advisory-only (`attribute_stats` carries no canonical
//! type; promoted attribute columns are `String`). Until #811 lands a canonical
//! type source, an attribute's type is `String` unless a column-typed source
//! declares otherwise, and a field the resolver cannot type at all is reported
//! as unresolved so the validator can raise a **defined rejection** rather than
//! letting an untyped literal reach the engine. See the module's
//! [`FieldResolver::resolve`] contract.

use std::collections::{HashMap, HashSet};

use super::value::ValueType;

/// Where a logical field physically lives, with its canonical type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolved {
    /// A promoted physical column, addressed by its physical column name.
    Column { name: String, value_type: ValueType },
    /// An unpromoted attribute served by extracting `key` from an attribute
    /// container column (an Arrow `Map`/JSON column such as `log_attributes`).
    JsonPath {
        container: String,
        key: String,
        value_type: ValueType,
    },
    /// An attribute captured on a named span event (e.g. the `exception`
    /// event's `exception.type`/`exception.message`/`exception.stacktrace`),
    /// extracted from an events JSON-array column — distinct from
    /// `JsonPath` because the value lives inside one matching array element's
    /// own nested attributes, not directly in a per-record attribute
    /// container.
    EventAttribute {
        events_column: String,
        event_name: String,
        key: String,
        value_type: ValueType,
    },
    /// The whole span-events list, read from an events JSON-array column and
    /// normalized to `[{name, timestamp_unix_nano, attributes}]` (String).
    SpanEvents { events_column: String },
    /// A promoted attribute column (`label_<key>`) that may still be NULL in
    /// files the compactor hasn't rewritten since promotion — Iceberg schema
    /// evolution null-fills new columns in pre-existing files, and the
    /// rewrite-coupled backfill only reaches a file when the compactor next
    /// compacts its partition (#816). Lowers to
    /// `COALESCE(<name>, <attribute-map/JSON extraction of key>)` so a query
    /// gets the same answer whether or not this row's file has been backfilled
    /// yet. Negation and absent-key (Kleene NULL) semantics fall out of the
    /// COALESCE automatically: the coalesced value is NULL exactly when the
    /// key is genuinely absent from both the column and the source containers,
    /// same as the plain `JsonPath` path today.
    PromotedColumn {
        name: String,
        value_type: ValueType,
        key: String,
    },
}

impl Resolved {
    /// The canonical [`ValueType`] of the resolved field.
    pub fn value_type(&self) -> &ValueType {
        match self {
            Resolved::Column { value_type, .. } => value_type,
            Resolved::JsonPath { value_type, .. } => value_type,
            Resolved::EventAttribute { value_type, .. } => value_type,
            Resolved::SpanEvents { .. } => &ValueType::String,
            Resolved::PromotedColumn { value_type, .. } => value_type,
        }
    }

    /// Whether this resolution's [`ValueType`] is *advisory* rather than
    /// authoritative: an unpromoted attribute (`JsonPath`) or an attribute
    /// captured on a span event (`EventAttribute`) has no canonical type of
    /// its own until the attribute-registry epic (#811) lands a real type
    /// source, so a caller that would otherwise coerce the value at plan
    /// time (a numeric aggregate operand casting to `Float64`, tolerating
    /// `NULL` for a non-numeric-looking value rather than erroring) may
    /// treat this resolution's advertised type as a hint, not a guarantee —
    /// whatever that type actually is. It is *not* always `String`: a
    /// resolver's unknown-name fallback (an unrecognized field, resolved
    /// permissively as a String attribute extraction) does hardcode
    /// `String`, but a declared-but-unpromoted logical field resolves
    /// through the same `JsonPath` shape carrying whatever type the logical
    /// schema declares for it — this method judges advisory-ness structurally,
    /// by resolution shape, not by inspecting the carried type. A `Column`
    /// (a real physical or promoted field) is authoritative: its type is
    /// this document's actual, load-bearing contract, and coercion there
    /// stays strict. A [`Resolved::PromotedColumn`] joins this set too: its
    /// physical column is always an optional `Utf8`/String (materialized
    /// labels are always created as optional String columns, see
    /// `evolution.rs`'s `add_label_columns`), and until the row's file is
    /// backfilled its value can still come from the untyped JSON fallback —
    /// no more of a canonical-type guarantee than `JsonPath` has.
    pub fn is_advisory_type(&self) -> bool {
        matches!(
            self,
            Resolved::JsonPath { .. }
                | Resolved::EventAttribute { .. }
                | Resolved::PromotedColumn { .. }
        )
    }
}

/// Resolves a logical, dotted OTel-native field name to its physical location.
///
/// A resolver returns `None` for a field it cannot resolve **or cannot assign a
/// canonical type to** — the validator turns that into a defined rejection.
/// Resolution MUST be promotion-invariant in meaning: a field resolves to a
/// [`Resolved::JsonPath`] when unpromoted and, once its attribute is
/// promoted to a materialized `label_<key>` column, either a
/// [`Resolved::Column`] (a first-class physical column, always fully
/// populated) or a [`Resolved::PromotedColumn`] (a promoted attribute column,
/// which may still be NULL in files the compactor hasn't backfilled yet and
/// so keeps coalescing with the JSON fallback — #816). All three denote the
/// same logical field with the same type.
pub trait FieldResolver: Send + Sync {
    fn resolve(&self, source: &str, field: &str) -> Option<Resolved>;

    /// Whether `field` is an actually-known field of `source` (a physical
    /// column or an explicitly-declared attribute) rather than a permissive
    /// fallback. Collision checks (extract shadowing, aggregate output names)
    /// use this so a production resolver that resolves *any* name to a String
    /// attribute does not spuriously flag every derived/output name as a
    /// collision. Defaults to "resolvable" for strict/in-memory resolvers.
    fn is_known(&self, source: &str, field: &str) -> bool {
        self.resolve(source, field).is_some()
    }

    /// Whether a name belongs to the physical realization of a source but is
    /// not necessarily a logical field. Validation uses this to reject clients
    /// addressing storage aliases without relying on name spelling.
    fn is_physical_name(&self, _source: &str, _field: &str) -> bool {
        false
    }

    /// Whether a predicate, grouping, or ordering may address this field.
    ///
    /// A `bool` rather than the schema layer's richer filterability vocabulary:
    /// the IR asks one yes/no question, and answering it must not require this
    /// crate to know how the caller classifies its fields. A caller with such a
    /// vocabulary maps it here.
    fn is_filterable(&self, _source: &str, _field: &str) -> bool {
        true
    }
}

/// A per-attribute entry in the [`InMemoryResolver`].
#[derive(Debug, Clone)]
enum Entry {
    /// A first-class physical column: resolves to a `Column` with `physical`.
    Column {
        physical: String,
        value_type: ValueType,
    },
    /// An attribute stored in `container`; resolves to a `Column` when the
    /// caller has named the physical column it was promoted to, else to a
    /// `JsonPath` extraction. Both denote the same logical field.
    Attribute {
        container: String,
        value_type: ValueType,
        promoted_column: Option<String>,
    },
}

/// A config/in-memory [`FieldResolver`] used for tests and as the fallback
/// before #811 provides a production registry view. Fully functional: it
/// captures the promoted-vs-unpromoted distinction that makes resolution
/// promotion-invariant.
#[derive(Debug, Clone, Default)]
pub struct InMemoryResolver {
    entries: HashMap<(String, String), Entry>,
    physical_names: HashMap<String, HashSet<String>>,
    retrieval_only: HashSet<(String, String)>,
}

impl InMemoryResolver {
    pub fn new() -> Self {
        Self::default()
    }

    /// Declare a first-class physical column for `source`. `logical` is the
    /// name a query writes; `physical` is the column the planner scans.
    pub fn with_column(
        mut self,
        source: &str,
        logical: &str,
        physical: &str,
        value_type: ValueType,
    ) -> Self {
        if logical != physical {
            self.physical_names
                .entry(source.to_string())
                .or_default()
                .insert(physical.to_string());
        }
        self.entries.insert(
            (source.to_string(), logical.to_string()),
            Entry::Column {
                physical: physical.to_string(),
                value_type,
            },
        );
        self
    }

    /// Declare an attribute for `source`, stored in `container`.
    ///
    /// `promoted_column` names the physical column the attribute currently
    /// materializes to, or `None` while it is served from `container`. The
    /// caller supplies the name rather than this crate deriving it: how a
    /// promoted attribute is spelled physically is the storage layer's
    /// convention, and the IR's promotion-invariance guarantee holds whatever
    /// that convention is.
    pub fn with_attribute(
        mut self,
        source: &str,
        logical: &str,
        container: &str,
        value_type: ValueType,
        promoted_column: Option<String>,
    ) -> Self {
        self.physical_names
            .entry(source.to_string())
            .or_default()
            .insert(container.to_string());
        self.entries.insert(
            (source.to_string(), logical.to_string()),
            Entry::Attribute {
                container: container.to_string(),
                value_type,
                promoted_column,
            },
        );
        self
    }

    /// Declare an implementation-only storage name with no logical field.
    pub fn with_physical_name(mut self, source: &str, name: &str) -> Self {
        self.physical_names
            .entry(source.to_string())
            .or_default()
            .insert(name.to_string());
        self
    }

    pub fn with_retrieval_only(mut self, source: &str, field: &str) -> Self {
        self.retrieval_only
            .insert((source.to_string(), field.to_string()));
        self
    }
}

impl FieldResolver for InMemoryResolver {
    fn resolve(&self, source: &str, field: &str) -> Option<Resolved> {
        match self.entries.get(&(source.to_string(), field.to_string()))? {
            Entry::Column {
                physical,
                value_type,
            } => Some(Resolved::Column {
                name: physical.clone(),
                value_type: value_type.clone(),
            }),
            Entry::Attribute {
                container,
                value_type,
                promoted_column,
            } => {
                if let Some(column) = promoted_column {
                    // Resolution shifts from JSON path to column while the
                    // meaning stays identical — promotion invariance.
                    Some(Resolved::Column {
                        name: column.clone(),
                        value_type: value_type.clone(),
                    })
                } else {
                    Some(Resolved::JsonPath {
                        container: container.clone(),
                        key: field.to_string(),
                        value_type: value_type.clone(),
                    })
                }
            }
        }
    }

    fn is_physical_name(&self, source: &str, field: &str) -> bool {
        self.physical_names
            .get(source)
            .is_some_and(|names| names.contains(field))
    }

    fn is_filterable(&self, source: &str, field: &str) -> bool {
        !self
            .retrieval_only
            .contains(&(source.to_string(), field.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Task 3.1 — resolving a logical field yields a column ref or a json-path
    // plus the canonical type; unpromoted → json path, promoted → column.
    #[test]
    fn physical_column_resolves_to_column_with_type() {
        let r = InMemoryResolver::new().with_column(
            "logs",
            "severity_number",
            "severity_number",
            ValueType::Int64,
        );
        assert_eq!(
            r.resolve("logs", "severity_number"),
            Some(Resolved::Column {
                name: "severity_number".to_string(),
                value_type: ValueType::Int64,
            })
        );
    }

    #[test]
    fn unpromoted_attribute_resolves_to_json_path() {
        let r = InMemoryResolver::new().with_attribute(
            "logs",
            "deployment.environment",
            "log_attributes",
            ValueType::String,
            None,
        );
        assert_eq!(
            r.resolve("logs", "deployment.environment"),
            Some(Resolved::JsonPath {
                container: "log_attributes".to_string(),
                key: "deployment.environment".to_string(),
                value_type: ValueType::String,
            })
        );
    }

    #[test]
    fn promoted_attribute_resolves_to_column() {
        let r = InMemoryResolver::new().with_attribute(
            "logs",
            "deployment.environment",
            "log_attributes",
            ValueType::String,
            Some("label_deployment_environment".to_string()),
        );
        match r.resolve("logs", "deployment.environment") {
            Some(Resolved::Column { name, value_type }) => {
                assert_eq!(value_type, ValueType::String);
                assert_eq!(name, "label_deployment_environment");
            }
            other => panic!("expected a column, got {other:?}"),
        }
    }

    #[test]
    fn unknown_field_is_unresolved() {
        let r = InMemoryResolver::new();
        assert_eq!(r.resolve("logs", "nope"), None);
    }
}
