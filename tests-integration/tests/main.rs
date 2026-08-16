//! Single integration-test binary.
//!
//! Every file under `tests/` is a module of this one crate instead of its own
//! `[[test]]` target. Each integration-test binary links the whole
//! DataFusion/Arrow/Iceberg dependency graph, so with ~40 files the test job
//! spent most of its compile time on linking rather than on the tests
//! themselves; one binary also lets the harness (and cargo-nextest) schedule
//! all tests together instead of running one binary at a time.
//!
//! Tests that install their own *capturing* global tracing subscriber
//! (`tracing::subscriber::set_global_default`) cannot share a process with
//! tests that call `try_init()`, so those stay separate `[[test]]` targets:
//! `http_response_headers` and `ops_endpoints_tracing`. Everything else — no
//! fixed ports (all binds are ephemeral), no `std::env` mutation, no shared
//! statics — is safe to run interleaved.
//!
//! Adding a test file: create it under the matching directory and add a
//! `mod name;` line to that directory's `mod.rs` (or below, for top-level
//! files). `autotests = false` in Cargo.toml means a file that is not
//! declared here is silently not compiled.

mod compactor;
mod e2e;
mod querier;
mod writer;

mod flight_message_limits;
mod logql_queries;
mod oauth_connector_flow;
mod ops_endpoints;
mod prometheus_remote_write_test;
mod promql_queries;
mod query_ir_e2e;
mod query_parity;
mod retry_on_throttle;
mod router_tempo_endpoints;
mod schema_registry_clients;
mod sdk_query_methods;
mod self_monitoring;
mod storage_quota;
mod tenant_manage_clients;
mod tenant_table_cli;
mod wal_retry_rejected_entries;
