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
//! - [`retry`] is hand-written: the shared retry-on-throttle policy every
//!   generated operation runs through (`cargo xtask generate` installs it as
//!   the progenitor `ClientHooks::exec` override; see the note at the top of
//!   `generated.rs`).
//! - [`builder`] is hand-written: [`ClientBuilder`], the one way consumers
//!   construct a [`Client`] (headers, timeouts, retry policy).

#[allow(clippy::all)]
mod generated;

pub mod builder;
pub mod query;
pub mod retry;

pub use builder::{ClientBuildError, ClientBuilder};
pub use generated::*;
pub use query::{QueryClient, QueryError};
pub use retry::RetryPolicy;

#[cfg(test)]
mod tests {
    /// `generated::OPERATIONS` is emitted by `cargo xtask generate` in the
    /// same run that produces `generated.rs` itself, from the same
    /// `api/signaldb-api.json` this test reads directly — so this only
    /// catches a stale `generated.rs` checked in without rerunning
    /// generation (task 2.1). It intentionally does not re-derive the
    /// extraction logic; xtask is the source of truth for how an
    /// `operationId` is pulled out of the document.
    #[test]
    fn operations_matches_openapi_document_operation_ids() {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let spec_path = format!("{manifest_dir}/../../api/signaldb-api.json");
        let spec_json = std::fs::read_to_string(&spec_path)
            .unwrap_or_else(|e| panic!("reading {spec_path}: {e}"));
        let spec: serde_json::Value =
            serde_json::from_str(&spec_json).expect("api/signaldb-api.json must be valid JSON");

        const METHODS: &[&str] = &[
            "get", "post", "put", "delete", "patch", "head", "options", "trace",
        ];
        let mut expected: Vec<String> = Vec::new();
        if let Some(paths) = spec.get("paths").and_then(|p| p.as_object()) {
            for path_item in paths.values() {
                let Some(path_item) = path_item.as_object() else {
                    continue;
                };
                for method in METHODS {
                    if let Some(id) = path_item
                        .get(*method)
                        .and_then(|op| op.get("operationId"))
                        .and_then(|id| id.as_str())
                    {
                        expected.push(id.to_string());
                    }
                }
            }
        }
        expected.sort();
        expected.dedup();

        let actual: Vec<&str> = super::OPERATIONS.to_vec();
        assert_eq!(
            actual, expected,
            "signaldb_sdk::OPERATIONS is stale relative to api/signaldb-api.json — \
             run `cargo xtask generate`"
        );
    }
}
