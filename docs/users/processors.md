---
audience: user
type: how-to
status: living
sources:
  - src/ottl/src/lib.rs
  - src/common/src/processors/mod.rs
  - src/acceptor/src/handler/otlp_grpc.rs
  - src/router/src/endpoints/processors.rs
  - src/ui/src/features/processors/routes.tsx
---

# Processors — redact and transform telemetry at ingest

A **processor** is a named, ordered set of [OTTL](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/pkg/ottl/README.md)
(OpenTelemetry Transformation Language) statements a tenant admin attaches to
one signal (`traces`, `logs`, or `metrics`). Every enabled processor matching
an export's tenant, dataset, and signal runs against the decoded OTLP request
**before** it is converted to Arrow and **before** anything reaches the WAL —
so a redaction rule is the only form of the data that is ever persisted,
forwarded to the writer, or retried. If you already run an OpenTelemetry
Collector `transform` processor in front of another vendor, the statements
below are the same language; most Collector configs paste in with no
changes.

## Scoping and ordering

A processor declares:

- `name` — a slug (`[a-z0-9][a-z0-9-]{0,62}`), unique per tenant, and the API
  identifier (`/api/v1/processors/{name}`).
- `signal` — `traces`, `logs`, or `metrics`. One processor applies to one
  signal.
- `dataset` (optional) — a dataset *name* (the same value carried in
  `X-Dataset-ID`). Unset means every dataset of the tenant.
- `enabled` (default `true`), `priority` (default `100`), `error_mode`
  (default `ignore`), an optional `description`, and an ordered list of
  `statements`.

For a given request (tenant, dataset, signal), the acceptor selects every
enabled processor whose signal matches and whose `dataset` is unset or equals
the request's dataset, and orders them **tenant-wide first, then
dataset-scoped**, each group by ascending `priority` then `name`. A
dataset-scoped rule therefore always has the last word over a tenant-wide
baseline — useful when one dataset needs a stricter or looser rule than the
tenant default.

Statements run once per leaf item of the decoded request: each span, each log
record, or — for metrics — each data point of every point type (including
summary and exponential histogram). `resource.*` edits are reachable from
every leaf and so run once per leaf too (matching Collector behavior); a
`where` guard keeps a resource-level `set` idempotent across repeated runs. A
metric with N data points evaluates `metric.*` paths N times against the same
`Metric`. Renaming `metric.name` takes effect before metric-type partitioning,
so the renamed metric partitions and stores under its new name.

A tenant with no matching processors sees byte-identical ingest behavior.

## The OTTL subset

SignalDB implements a bounded, in-house subset of OTTL (crate `src/ottl`) —
not the full Collector language. Every statement is `editor(args) [where
condition]`, compiled once at create/replace/validate time; nothing in the
table below is a silent no-op — an unsupported token is a validation error
naming it.

### Paths, per signal

| Signal    | Keyed paths                                                                                          | Bare map paths (target of map editors)                          |
| --------- | ----------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| `traces`  | `resource.attributes["k"]`, `instrumentation_scope.name\|version\|attributes["k"]`, `span.name\|kind\|status.code\|status.message\|attributes["k"]` | `attributes`, `resource.attributes`, `instrumentation_scope.attributes` |
| `logs`    | `resource.attributes["k"]`, `instrumentation_scope.name\|version\|attributes["k"]`, `log.body\|severity_text\|severity_number\|attributes["k"]` | `attributes`, `resource.attributes`, `instrumentation_scope.attributes` |
| `metrics` | `resource.attributes["k"]`, `instrumentation_scope.name\|version\|attributes["k"]`, `metric.name\|description\|unit`, `datapoint.attributes["k"]` | `datapoint.attributes`, `resource.attributes`, `instrumentation_scope.attributes` |

Unqualified `attributes[...]` resolves to the signal's leaf item (the span's,
the log record's, or — for metrics — the data point's attributes); unqualified
`body`/`name` resolve the same way (`body` = the log body, `name` = the
metric's name). A path from another signal (e.g. `span.name` in a `logs`
processor) is a validation error naming the path and the signal it isn't
available for.

### Editors

| Editor                                            | Arity / target                                          |
| -------------------------------------------------- | -------------------------------------------------------- |
| `set(target, value)`                               | keyed path                                                |
| `delete_key(map, "key")`                           | bare map path                                             |
| `delete_matching_keys(map, regex)`                 | bare map path                                             |
| `keep_keys(map, "key1", "key2", ...)`              | bare map path                                             |
| `truncate_all(map, max_len)`                       | bare map path                                             |
| `limit(map, max_keys)`                             | bare map path                                             |
| `replace_pattern(target, regex, replacement)`      | keyed path, 3 args only                                   |
| `replace_all_patterns(map, "key"\|"value", regex, replacement)` | bare map path; the mode string is required        |
| `replace_match(target, glob, replacement)`         | keyed path                                                 |
| `replace_all_matches(map, glob, replacement)`      | bare map path                                              |

### Converters

`IsMatch`, `IsString`, `Concat`, `String`, `Int`, `Double`, `Len`, `SHA256`,
`Substring`, `ToLowerCase`, `ToUpperCase`, `Truncate`.

### Conditions

`where` accepts comparison operators (`==`, `!=`, `<`, `<=`, `>`, `>=`),
`and`, `or`, `not`, and parentheses over string, integer, float, boolean
literals and `nil`. A condition referencing an absent key evaluates to
`false`, never an error.

### `$$` normalisation and string escapes

Replacement strings use `regex`-crate syntax (`$1`, `${1}`). A literal `$$` in
a replacement string is normalised to `$` at compile time, so a Collector
config written as `$$1` — to survive the Collector's own `${}` template
expansion — ports verbatim without editing. String literals accept the
escapes `\\`, `\"`, `\n`, `\t`.

### Not supported

Nested map/slice paths, `Cache`, enum symbols, `merge_maps`, `flatten`,
`ParseJSON`, `Time`/`Duration` converters, `span.events[...]`, and the
`spanevent`/`scope` statement contexts are all outside this subset and fail
validation by name. A dropping editor (the Collector's `filter` semantics) is
not implemented either — a processor can redact or rewrite a field, never
drop a whole span/log/point.

## Error modes

Every processor declares an `error_mode`, checked per statement at runtime
(type-conversion failures, an editor targeting a missing required path, or a
replacement-template error):

| Mode        | Behavior                                                                                  |
| ----------- | ------------------------------------------------------------------------------------------ |
| `ignore` (default) | Log the failure (rate-limited to once per minute per tenant+processor) and skip that statement for that item; the rest of the export proceeds |
| `silent`    | Skip the statement for that item without logging                                          |
| `propagate` | Abort the whole export with an invalid-argument error (HTTP 400 / gRPC `InvalidArgument`) before anything is written |

A `where` condition referencing an absent key is `false`, never a runtime
error, under any mode.

## Worked example: PII redaction

Hash an e-mail address, blank credit-card-shaped fragments in log bodies, and
strip every `user.*` attribute you don't otherwise need:

```
set(attributes["user.email"], SHA256(attributes["user.email"])) where attributes["user.email"] != nil
replace_pattern(body, "\\b(?:\\d[ -]*?){13,16}\\b", "[redacted-card]")
delete_matching_keys(attributes, "^user\\.")
```

The first statement only fires when the key is present (the `where` guard),
hashing rather than dropping the value so joins on "the same user redacted the
same way" still work. The second scans the log body for card-shaped digit runs
and blanks them. The third removes every remaining `user.*` attribute — apply
it last, after any statement that still needs to read one of those keys.

## Worked example: URL sanitization

Strip the query string from `url.full` and mask a `token` query parameter
before that, plus a generic attribute-value scrub:

```
replace_pattern(attributes["url.full"], "([?&]token=)[^&]*", "$${1}[redacted]")
replace_pattern(attributes["url.full"], "\\?.*$", "")
replace_all_patterns(attributes, "value", "(?i)password=[^&\\s]+", "password=[redacted]")
```

The first statement masks a `token=...` parameter in place (`$$` normalises
to `$`, so `$${1}` in the source becomes the replacement group `${1}`); the
second then drops the rest of the query string outright — order matters,
since a query-string strip alone would also have removed the token already
masked, but running the mask second would restore nothing (a masked token has
already lost its original value, so the two are independent — mask first,
strip second, or the strip alone makes masking moot). The third statement runs
over every attribute value map-wide, not just `url.full`, using
`replace_all_patterns`' `"value"` mode.

## Validate and dry-run

Prove a rule works before it runs against real traffic:

```bash
# Compile-check only — nothing stored
curl -X POST -H "Authorization: Bearer $KEY" -H "X-Tenant-ID: acme" \
  -H "Content-Type: application/json" \
  -d '{"signal":"logs","statements":["set(attributes[\"user.email\"], \"[redacted]\")"]}' \
  http://localhost:3000/api/v1/processors:validate

# Apply against a sample OTLP JSON payload — never written to the WAL,
# never forwarded, never counted against ingest quotas
curl -X POST -H "Authorization: Bearer $KEY" -H "X-Tenant-ID: acme" \
  -H "Content-Type: application/json" \
  -d '{"signal":"logs","payload":{...}}' \
  http://localhost:3000/api/v1/processors:test
```

`:validate` returns positional errors (statement index, column, message
naming the offending token) and never stores anything. `:test` accepts a
`signal`, an optional `dataset`, an optional inline `processors` list (omit it
to dry-run the tenant's *stored* processors for that signal/dataset), and an
OTLP JSON payload up to `[processors].test_payload_max_bytes` (default 1 MiB);
it returns the transformed payload plus, per statement, how many items it
matched and how many errored.

## HTTP API

All under `/api/v1`, tenant-credentialed:

| Method   | Path                       | Scope               | Notes                                    |
| -------- | -------------------------- | -------------------- | ----------------------------------------- |
| `GET`    | `/processors`              | `processors:read`    | List                                      |
| `POST`   | `/processors`              | `processors:write`   | Create → `201`                            |
| `GET`    | `/processors/{name}`       | `processors:read`    | Get                                       |
| `PUT`    | `/processors/{name}`       | `processors:write`   | Replace (full document; never upserts)    |
| `DELETE` | `/processors/{name}`       | `processors:write`   | Delete → `204`                            |
| `POST`   | `/processors:validate`     | `processors:read`    | Compile-check, nothing stored             |
| `POST`   | `/processors:test`         | `processors:read`    | Dry-run against a payload                 |

Errors: `404` unknown name (`GET`/`PUT`/`DELETE`), `409` create on an existing
name, `422` with `errors: [{statement, column, message}]` for compile
failures, `403` missing scope. A write response's `applies_within_seconds`
field equals the configured `[processors].reload_interval` — a hint the UI
also surfaces after save.

## CLI

```bash
signaldb-cli processors list
signaldb-cli processors get redact-emails
signaldb-cli processors validate --signal logs --statement 'set(attributes["a"], 1)'
signaldb-cli processors test --signal logs --payload sample.json

signaldb-cli admin processors create -f redact.yaml
signaldb-cli admin processors replace redact-emails -f redact.yaml
signaldb-cli admin processors delete redact-emails
```

`processors` (read) uses a tenant credential; `admin processors` (write)
needs a key holding `processors:write`. A processor spec can be given as a
JSON/YAML file (`-f`) or via flags: `--signal`, `--dataset`, `--statement`
(repeatable), `--priority`, `--error-mode`, `--disabled`.

## MCP tools

`list_processors`, `get_processor`, `validate_processor`, `test_processor`
(`processors:read`) and `create_processor`, `replace_processor`,
`delete_processor` (`processors:write`) — see [the MCP tool
catalogue](mcp.md#what-it-exposes).

## Explore UI

`/processors` (linked from the user menu) lists the tenant's processors —
name, signal, dataset, enabled, priority, status (`ok`/`invalid`), last
update — and, for tenant admins, an editor: one statement per line, validated
on blur against `:validate` with inline per-line errors, and a **Test** panel
preloaded with a sample OTLP payload for the selected signal that renders a
before/after diff plus per-statement match counts. Non-admin members see the
list read-only.

## Applies within `reload_interval`

Every service that applies processors (the acceptor, today) caches a
tenant's compiled processors and refreshes the cache on a TTL
(`[processors].reload_interval`, default 30s) plus immediate invalidation on
writes made through the same process. A write through the router therefore
reaches a separately-running acceptor process within one interval, never
instantly — the write response and the UI both surface this as "applies
within N seconds." A stored processor that fails to compile (for example
after a limit is lowered) is skipped at ingest, logged, and reported as
`status: "invalid"` on list/get; it never blocks ingest.

## What processors do not touch

- **Prometheus remote-write** (`POST /api/v1/prometheus/write`) builds its
  own `ExportMetricsServiceRequest` outside the OTLP handler path; metrics
  processors do not run against it.
- **Profiles** have no processor support — no OTTL context is defined for
  profiles upstream either.
- **Direct writer Flight writes** (`do_put` straight to the writer) bypass the
  acceptor entirely, so no processor ever sees that data.
- Query-time / read-path redaction is out of scope — processors run once, at
  ingest, before durability.

## Scopes

`processors:read` (list/get/validate/test) joins the OAuth-grantable default
read-scope set, same as `schema:read`. `processors:write`
(create/replace/delete) requires tenant-admin rights and is **not**
OAuth-grantable, same as `schema:write`. Both are selectable wherever API-key
scopes are chosen (Admin API, Management API, CLI `--scope`, MCP
`create_api_key`, the UI's key scope picker). See
[Authentication](authentication.md#api-key-scopes).
