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
use crate::schema::logical::Filterability;

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
}

impl Resolved {
    /// The canonical [`ValueType`] of the resolved field.
    pub fn value_type(&self) -> &ValueType {
        match self {
            Resolved::Column { value_type, .. } => value_type,
            Resolved::JsonPath { value_type, .. } => value_type,
            Resolved::EventAttribute { value_type, .. } => value_type,
            Resolved::SpanEvents { .. } => &ValueType::String,
        }
    }
}

/// Resolves a logical, dotted OTel-native field name to its physical location.
///
/// A resolver returns `None` for a field it cannot resolve **or cannot assign a
/// canonical type to** — the validator turns that into a defined rejection.
/// Resolution MUST be promotion-invariant in meaning: a field resolves to a
/// [`Resolved::JsonPath`] when unpromoted and a [`Resolved::Column`] when
/// promoted, but both denote the same logical field with the same type.
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

    fn filterability(&self, _source: &str, _field: &str) -> Filterability {
        Filterability::Filterable
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
    /// An attribute stored in `container`; resolves to a `Column` (the
    /// materialized name) when `promoted`, else to a `JsonPath` extraction.
    Attribute {
        container: String,
        value_type: ValueType,
        promoted: bool,
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

    /// Declare an attribute for `source`, stored in `container`. `promoted`
    /// selects whether it currently resolves to a materialized column or a
    /// JSON-path extraction — the two promotion states of the same field.
    pub fn with_attribute(
        mut self,
        source: &str,
        logical: &str,
        container: &str,
        value_type: ValueType,
        promoted: bool,
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
                promoted,
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
                promoted,
            } => {
                if *promoted {
                    // Promoted attributes materialize to `label_<sanitized>`
                    // columns — resolution shifts from JSON path to column,
                    // meaning unchanged (promotion invariance).
                    Some(Resolved::Column {
                        name: crate::schema::materialized_column_name(field),
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

    fn filterability(&self, source: &str, field: &str) -> Filterability {
        if self
            .retrieval_only
            .contains(&(source.to_string(), field.to_string()))
        {
            Filterability::RetrievalOnly
        } else {
            Filterability::Filterable
        }
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
            false,
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
            true,
        );
        match r.resolve("logs", "deployment.environment") {
            Some(Resolved::Column { name, value_type }) => {
                assert_eq!(value_type, ValueType::String);
                assert_eq!(
                    name,
                    crate::schema::materialized_column_name("deployment.environment")
                );
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
