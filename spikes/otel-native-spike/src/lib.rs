//! Spike for the `otel-native-schema` change — Layer 0 (blocking).
//!
//! Three workstreams, each with a demo binary under `src/bin/`:
//!
//! - [`warm_index`] (task 0.1): prototype the warm typed containment index
//!   (typed generalization of `attr_tokens`) and prove row-group/file pruning
//!   for an unpromoted `key = value` predicate.
//! - [`coexistence`] (task 0.2): prove the datafusion-iceberg provider handles
//!   the typed layout (per-type maps + binary residue) under one table scan,
//!   including field-id promotion evolution across file generations
//!   (pre-promotion files null-fill, no error). No legacy-map coexistence —
//!   the typed layout replaces the old layout in a one-shot breaking cutover.
//! - [`bench`] (task 0.3): benchmark string-map vs typed map vs promoted column
//!   vs warm-index on real data, including conflicted/off-type keys, files-pruned
//!   %, footer/metadata overhead on small flush files, residue parse cost, and
//!   per-attribute registry-lookup cost.
//!
//! Results are recorded in `openspec/changes/otel-native-schema/spike/`.

pub mod bench;
pub mod coexistence;
pub mod warm_index;
