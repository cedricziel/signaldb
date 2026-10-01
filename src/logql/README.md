# logql-parser

A LogQL lexer, AST, and recursive-descent parser in Rust.

```toml
[dependencies]
logql-parser = "0.1"
```

The package is `logql-parser`; the library is imported as `logql`, because a
bare `logql` on crates.io was taken in 2022 by an unrelated project.

```rust
let query = logql::parse_query(r#"{service_name="api"} |= "error""#)?;
```

## What it does, and what it deliberately does not

It answers one question: **is this LogQL, and what does it say?** It lexes,
parses, and validates syntax, and its only dependency is `thiserror` — no
Arrow, no DataFusion, no database. Whether a query is valid is a property of
the query text, so answering it needs no catalog, no tenant, and no running
server.

It does **not** execute a query, plan one, or know anything about how logs are
stored. Translating an AST into a plan is the caller's job.

Both query forms are covered: log queries (a stream selector plus a pipeline of
line filters, label filters, parser stages, and formatters) and metric queries
(range and vector aggregations, binary operations, `label_replace`).

## Stability

Pre-1.0. The AST enums are `#[non_exhaustive]`, so a release that teaches the
parser a new construct is additive for consumers — match with a fallback arm.
The AST _structs_ are deliberately left constructible: building a query by hand
is a legitimate use of this crate.

## Provenance

Extracted from [SignalDB](https://github.com/cedricziel/signaldb), which uses it
for its Loki-compatible query interface. Licensed **AGPL-3.0**, matching
[Grafana Loki](https://github.com/grafana/loki), whose language it implements.

## Tests

```
cargo test -p logql-parser
```
