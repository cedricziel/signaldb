//! Data-transfer types for the SignalDB admin HTTP API.
//!
//! The types in [`schemas`] are hand-written and derive [`utoipa::ToSchema`],
//! making them the source of truth for the admin API's OpenAPI component
//! schemas. The `router` crate annotates its handlers with `#[utoipa::path]`
//! and assembles these schemas into the emitted spec (`api/signaldb-api.json`).

mod extensions;
mod schemas;

pub use schemas::*;
