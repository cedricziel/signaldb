//! # SignalDB SDK
//!
//! The single client library for SignalDB. The CLI and the MCP server consume
//! only this crate (see `client-surface-parity`), so it must cover the full
//! API surface. It has two parts with different provenance:
//!
//! - [`Client`] and everything re-exported from [`generated`] is produced by
//!   `cargo xtask generate` (progenitor) from the code-first OpenAPI document.
//!   It covers admin/management and the PromQL/LogQL/TraceQL query-compat
//!   endpoints (`promql_query`, `logql_query`, `search`, …). **Do not edit
//!   `generated.rs` by hand** — change the router annotations and regenerate.
//! - [`query`] is hand-written. SQL is served over Arrow Flight (gRPC), which
//!   OpenAPI cannot describe, so [`query::QueryClient`] wraps that transport.
//!   It is the only non-generated code in this crate.

#[allow(clippy::all)]
mod generated;

pub mod query;

pub use generated::*;
pub use query::{QueryClient, QueryError};
