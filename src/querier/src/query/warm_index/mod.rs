//! Warm-tier containment-index probing (see `openspec/changes/otel-native-schema`,
//! spec `typed-attribute-storage`, "Warm tier"): [`probe::probe_clauses`]
//! recognizes an equality predicate over a typed-attribute home as a set of
//! candidate warm-index tokens for a Parquet bloom-filter prefilter (added in
//! a follow-up commit) to check against each candidate file. Never a false
//! negative: an unrecognized predicate shape is dropped rather than guessed at.
// Not consumed until the prefilter (and later a `TableProvider`) wires it in.
#![allow(unused_imports)]

mod probe;

pub(crate) use probe::{ProbeClause, probe_clauses};
