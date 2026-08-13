## ADDED Requirements

### Requirement: Exception attributes resolve from the span event on traces

On the `traces` source, `exception.type`, `exception.message`,
`exception.stacktrace`, and `exception.escaped` SHALL resolve by reading the
span's first event named `exception`, per the OTel exceptions-on-spans
convention, rather than as span attributes. A span with no `exception` event
SHALL resolve each of these fields as absent, regardless of the span's
status. These fields SHALL be filterable, groupable, and projectable like
any other logical field.

#### Scenario: Filtering spans that captured an exception

- **WHEN** a `traces` query filters `{ "field": "exception.type", "op":
"exists" }`
- **THEN** only spans carrying an `exception` event with a `exception.type`
  attribute match, independent of the span's status

#### Scenario: An error span without a captured exception does not match

- **WHEN** a span has status `Error` but no `exception` event
- **THEN** `exception.type` resolves absent for that span, and it does not
  match `{ "field": "exception.type", "op": "exists" }`

#### Scenario: Grouping by exception type

- **WHEN** a `traces` query aggregates `{ "by": ["exception.type"], "aggs":
[{ "fn": "count", "as": "n" }] }`
- **THEN** spans are grouped by the value read from each one's `exception`
  event, not by a (nonexistent) `exception.type` span attribute

Logs need no equivalent resolution: per the exceptions-on-logs convention,
the same attribute names are ordinary LogRecord attributes on the `logs`
source, already served by the existing attribute resolution.
