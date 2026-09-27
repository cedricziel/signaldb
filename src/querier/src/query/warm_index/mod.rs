//! Warm-tier containment-index probing (see `openspec/changes/otel-native-schema`,
//! spec `typed-attribute-storage`, "Warm tier"): [`probe::probe_clauses`]
//! recognizes an equality predicate over a typed-attribute home as a set of
//! candidate warm-index tokens for [`prefilter::prefilter_files`]'s Parquet
//! bloom-filter prefilter to check against each candidate file, and
//! [`table::WarmIndexTable`] wires that into a table's physical scan. Never a
//! false negative: an unrecognized predicate shape is dropped rather than
//! guessed at.

mod prefilter;
mod probe;
mod table;

pub(crate) use prefilter::WarmIndexGate;
pub(crate) use table::WarmIndexTable;
