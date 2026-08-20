//! # TraceQL Front-End
//!
//! Lexing, parsing, and syntactic validation for the subset of Grafana Tempo's
//! TraceQL that SignalDB answers. This crate recognises a query; it does not
//! execute one. It knows nothing about columns, catalogs, tenants, or storage,
//! and it has no dependency on any SignalDB component — the same query string
//! parses identically no matter which deployment it is aimed at.
//!
//! Published as **`traceql-parser`**; the library is imported as `traceql`.
//!
//! ## Supported subset
//!
//! A single spanset of `&&`-conjoined equality matchers:
//!
//! ```text
//! { resource.service.name = "api" && span.http.method = "GET" }
//! ```
//!
//! - **Intrinsics**: `name`, `status`, `kind`, and service name spelled either
//!   `resource.service.name` or `.service.name`.
//! - **Attributes**: `span.<key>`, `resource.<key>`, or unscoped `.<key>`.
//! - **Values**: double-quoted strings, bare numbers, `true`/`false`, and bare
//!   identifiers (which only mean anything for `status`/`kind`).
//! - **`{}`** is valid and selects everything.
//!
//! ## Rejection contract
//!
//! Two error classes, and the difference is the caller's to act on:
//!
//! | Input | Variant |
//! |-------|---------|
//! | not TraceQL at all (`notbraces`, `{ foo }`, `{ zzz = 1 }`) | [`ParseError::Syntax`] |
//! | valid TraceQL we do not implement (`\|\|`, `!=`, `=~`, `duration`) | [`ParseError::Unsupported`] |
//!
//! A caller serving HTTP maps `Syntax` to a client error and `Unsupported` to
//! not-implemented. Nothing outside the subset is silently ignored: a query is
//! never executed with a matcher dropped, because a partially-applied filter
//! returns *more* traces than asked for, which reads as a successful search.
//!
//! **One exception, deliberately.** An escaped string literal
//! (`{ .a = "he said \"hi\"" }`) is legal TraceQL that this lexer does not
//! handle, so the rule above would put it under [`ParseError::Unsupported`].
//! It is reported as [`ParseError::Syntax`] instead, because it was already a
//! client error before this parser was extracted and reclassifying a client
//! error as "not implemented" tells the caller less, not more.

pub mod ast;
pub mod parser;

pub use ast::{Condition, FilterValue, Selector};
pub use parser::{ParseError, parse};
