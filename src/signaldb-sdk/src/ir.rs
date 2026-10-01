//! Hand-written Query IR types the generator cannot express faithfully.
//!
//! `cargo xtask generate` substitutes these for the generated ones (progenitor
//! `with_replacement`), so the generated stage types refer to them.

use std::fmt;

use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::types::{IrMatchRelation, IrPredicate};

/// The `match` stage (`irVersion` 12): keep the traces in which every
/// span-set has a matching span and every relation holds, returning the
/// witnessing spans.
///
/// Replaces the generated type, whose `HashMap` would lose the span-sets'
/// declaration order.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IrMatch {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relations: Vec<IrMatchRelation>,
    pub spansets: IrSpansets,
}

/// Named span-set predicates, in declaration order — the order of the names
/// in each result row's `spansets` column. Serialized as a JSON object.
#[derive(Clone, Debug, Default)]
pub struct IrSpansets(pub Vec<(String, IrPredicate)>);

impl Serialize for IrSpansets {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (name, predicate) in &self.0 {
            map.serialize_entry(name, predicate)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for IrSpansets {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct OrderedVisitor;

        impl<'de> Visitor<'de> for OrderedVisitor {
            type Value = IrSpansets;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a map of span-set names to predicates")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<IrSpansets, A::Error> {
                let mut entries = Vec::with_capacity(access.size_hint().unwrap_or(0));
                while let Some(entry) = access.next_entry()? {
                    entries.push(entry);
                }
                Ok(IrSpansets(entries))
            }
        }

        deserializer.deserialize_map(OrderedVisitor)
    }
}
