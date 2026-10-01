//! A bounded, in-house subset of OpenTelemetry Transformation Language (OTTL),
//! implemented from scratch (no adoptable Rust OTTL crate exists — see
//! `openspec/changes/tenant-ottl-processors/design.md` D1).
//!
//! ```text
//! parse(source)                              -> Statement
//! compile(signal, statements, limits)        -> CompiledProgram
//! CompiledProgram::apply_traces/logs/metrics -> ApplyReport
//! ```
//!
//! See `README.md` for the supported grammar and a conformance table against
//! upstream OTTL.

mod apply;
pub mod ast;
mod compile;
mod engine;
mod error;
mod parser;
mod value;

pub use apply::{ApplyReport, ErrorMode, StatementStats};
pub use compile::{Limits, Signal, compile};
pub use error::{ApplyError, CompileError, ParseError};
pub use parser::parse;
pub use value::Value;

// Re-exported so downstream crates can name the type `CompiledProgram` without
// depending on the module layout.
pub use compile::CompiledProgram;
