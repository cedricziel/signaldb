# ottl

A bounded, in-house subset of [OTTL](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/pkg/ottl/README.md)
(OpenTelemetry Transformation Language), implemented from scratch. No
adoptable Rust OTTL crate exists — see
`openspec/changes/tenant-ottl-processors/design.md` decision D1 — so this
crate parses (`pest`), compiles, and evaluates a deliberately small statement
grammar directly against `opentelemetry_proto` request structs, before
conversion to Arrow.

```
parse(source)                              -> Statement
compile(signal, statements, limits)        -> CompiledProgram
CompiledProgram::apply_traces/logs/metrics -> ApplyReport
```

## Grammar

```
statement  = call [ "where" condition ]
call       = ident "(" [ expr ("," expr)* ] ")"
expr       = call | list_literal | literal | path
list_literal = "[" [ expr ("," expr)* ] "]"
path       = ident ( "." ident | "[" string "]" )*
literal    = float | int | true | false | nil | string
condition  = or_expr
or_expr    = and_expr ( "or" and_expr )*
and_expr   = not_expr ( "and" not_expr )*
not_expr   = [ "not" ] ( "(" condition ")" | comparison )
comparison = expr [ ( "==" | "!=" | "<" | "<=" | ">" | ">=" ) expr ]
```

String literals support the escapes `\\`, `\"`, `\n`, `\t` (anything else
after a backslash is passed through literally, backslash dropped).

## Paths, per signal

| Signal  | Legal roots |
|---------|-------------|
| traces  | `resource.attributes[...]`, `instrumentation_scope.{name,version,attributes[...]}`, `span.{name,kind,status.code,status.message,attributes[...]}`; bare `attributes[...]`/`name` resolve to the span |
| logs    | `resource.*`, `instrumentation_scope.*`, `log.{body,severity_text,severity_number,attributes[...]}`; bare `attributes[...]`/`body` resolve to the log record |
| metrics | `resource.*`, `instrumentation_scope.*`, `metric.{name,description,unit}`, `datapoint.attributes[...]`; bare `attributes[...]` resolves to the data point, bare `name` resolves to `metric.name` |

`resource.attributes`, `instrumentation_scope.attributes`, and the signal's
leaf `attributes` (or bare `attributes`) may be used **with** a key
(`attributes["k"]`, an assignable attribute value) or **without** one
(`attributes`, a map — the only form editors like `delete_key` accept).

Metrics are evaluated once per data point (every kind: gauge, sum, histogram,
exponential histogram, summary); `metric.*` and `resource.*` edits therefore
run once per point too, matching the Collector's own `datapoint` context
semantics.

A `where` comparison against a missing path is `false` for every operator
except `!=`, which is exactly the presence test (`attributes["k"] != nil`). A
log body that isn't a string is `nil` for every purpose (comparisons,
`replace_pattern`, etc.) — only string bodies are editable/comparable as
strings.

## Editors

| Editor | Arity | Arg 1 | Arg 2 | Arg 3 | Arg 4 |
|---|---|---|---|---|---|
| `set` | 2 | target (scalar or `attrs["k"]`) | value (any expr) | | |
| `delete_key` | 2 | map | key (string literal) | | |
| `delete_matching_keys` | 2 | map | regex (string literal) | | |
| `keep_keys` | 2 | map | keys (`[string, ...]` literal) | | |
| `truncate_all` | 2 | map | limit (int literal, chars) | | |
| `limit` | 3 | map | limit (int literal) | priority keys (`[string, ...]`) | |
| `replace_pattern` | 3 | target (scalar or `attrs["k"]`) | regex (string literal) | replacement (string literal) | |
| `replace_all_patterns` | 4 | map | `"key"` or `"value"` (string literal) | regex (string literal) | replacement (string literal) |
| `replace_match` | 3 | target (scalar or `attrs["k"]`) | glob (string literal, `*`/`?` only) | replacement (string literal) | |
| `replace_all_matches` | 3 | map | glob (string literal, `*`/`?` only) | replacement (string literal) | |

Replacement strings use `regex`-crate syntax: `$1`/`${1}` reference capture
groups, `$$` is a literal `$`. Collector configs that use `$$1` to mean the
literal text `$1` port verbatim: this crate expands replacements itself
(`engine::expand_replacement`) rather than delegating to `regex::Regex`'s own
expansion, but implements the identical semantics.

## Converters

| Converter | Arity | Arg 1 | Arg 2 | Arg 3 | Result |
|---|---|---|---|---|---|
| `IsMatch` | 2 | value (any expr) | regex (string literal) | | bool |
| `IsString` | 1 | value | | | bool |
| `Concat` | 2 | values (`[expr, ...]` list) | delimiter (string literal) | | string |
| `String` | 1 | value | | | string (display form) |
| `Int` | 1 | value (int/double/bool/numeric string) | | | int (error if not convertible) |
| `Double` | 1 | value (double/int/numeric string) | | | double (error if not convertible) |
| `Len` | 1 | value (string/list/bytes) | | | int |
| `SHA256` | 1 | value | | | string (64-char lowercase hex) |
| `Substring` | 3 | value | start (int literal) | len (int literal) | string |
| `ToLowerCase` | 1 | value | | | string |
| `ToUpperCase` | 1 | value | | | string |
| `Truncate` | 2 | value | len (int literal) | | string, truncated to `len` chars |

## Conditions

`==`, `!=`, `<`, `<=`, `>`, `>=`; `and`, `or`, `not`; parentheses; literals
(string, int, float, `true`/`false`, `nil`). Numeric comparisons coerce
int/double to a common type; `nil` handling is described above.

## Limits (`Limits`)

| Field | Default | Enforced |
|---|---|---|
| `max_statements` | 200 | at `compile` |
| `max_regex_len` | 2048 bytes | at `compile`, per regex source |
| `regex_size_limit` | 1 MiB | `regex::RegexBuilder::size_limit`, at `compile` |

Every regex (and glob, translated to a regex) is compiled once at `compile`
time, never per item.

## `ErrorMode`

`Ignore` (default) and `Silent` record the error in `ApplyReport` and continue
with the next statement/item; `Ignore` additionally logs at
`tracing::debug!`. `Propagate` aborts on the first runtime error.

## Conformance vs upstream OTTL

Deliberately unsupported (a validation error naming the token, never a silent
no-op): `Cache`, enum symbols, `merge_maps`, `flatten`, `ParseJSON`,
`Time`/`Duration` converters, nested map/slice paths, `span.events[...]`,
`spanevent`/`scope` statement contexts, dropping items (`drop()`), any context
other than the signal's own leaf (e.g. no cross-signal statements).

| Area | Upstream OTTL | This crate |
|---|---|---|
| Statement shape | `editor(args) [where condition]`, multiple conditions (`and`/`or`), functions as values | same, minus function-as-bare-statement forms |
| Contexts | resource, scope, span, spanevent, log, metric, datapoint, profile | resource, scope, span (traces only), log (logs only), metric/datapoint (metrics only) |
| Paths | arbitrary nested maps/slices | one level of map indexing (`attributes["k"]`); no nested map/slice paths |
| Editors | ~20, including `merge_maps`, `flatten`, `set` with nested paths | the 10 listed above |
| Converters | ~40 | the 12 listed above |
| Replacement syntax | Go `regexp` `$1`/`$$` | same semantics, own implementation |
| Enums | symbolic (`SPAN_KIND_SERVER`) | not supported; use the raw int (e.g. `span.kind == 2`) |
