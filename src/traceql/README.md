# traceql-parser

A parser for the equality subset of Grafana Tempo's TraceQL, in Rust.

```toml
[dependencies]
traceql-parser = "0.1"
```

The package is `traceql-parser`; the library is imported as `traceql`.

```rust
let conditions = traceql::parse(r#"{ resource.service.name = "api" }"#)?;
```

## What it does, and what it deliberately does not

It answers one question: **is this TraceQL, and what does it say?** It parses
and validates syntax, and its only dependency is `thiserror` — no Arrow, no
DataFusion, no database. The same query parses identically no matter which
backend it is aimed at.

It does **not** execute a query or know how traces are stored.

## Supported subset

A single spanset of `&&`-conjoined equality matchers. `{}` is valid and selects
everything.

```
{ resource.service.name = "api" && span.http.method = "GET" }
```

- **Intrinsics**: `name`, `status`, `kind`, and service name as either
  `resource.service.name` or `.service.name`
- **Attributes**: `span.<key>`, `resource.<key>`, unscoped `.<key>`
- **Values**: quoted strings, bare numbers, `true`/`false`, bare identifiers

Everything else is rejected rather than ignored — a partially applied filter
returns _more_ traces than asked for while still looking like a successful
search.

## Two rejection classes

| Input                                                         | Variant                   |
| ------------------------------------------------------------- | ------------------------- |
| not TraceQL (`notbraces`, `{ foo }`, `{ zzz = 1 }`)           | `ParseError::Syntax`      |
| valid TraceQL, unimplemented (`\|\|`, `!=`, `=~`, `duration`) | `ParseError::Unsupported` |

The distinction is the point: a caller serving HTTP maps the first to a client
error and the second to not-implemented, so a user can tell a wrong query from
one the backend cannot yet run.

## Stability

Pre-1.0. `Selector`, `FilterValue`, and `ParseError` are `#[non_exhaustive]`;
`Condition` is not, since a matcher is exactly a selector and a value.

## Provenance

Extracted from [SignalDB](https://github.com/cedricziel/signaldb), which uses it
for its Tempo-compatible search API. Licensed **AGPL-3.0**, matching
[Grafana Tempo](https://github.com/grafana/tempo), whose language it implements.

## Tests

```
cargo test -p traceql-parser
```
