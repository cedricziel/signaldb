## Purpose

Defines how a client follows new records matching a Query IR `rows` or `trace`
document as they become queryable. It covers the poll protocol and its cursor,
the settle delay and what happens to late data, how a lagging client is
handled, which documents can be tailed, and the tenancy rules every poll
obeys.

## ADDED Requirements

### Requirement: Live tail over the native query endpoint

SignalDB SHALL let a client tail a `rows` or `trace` document through
repeated calls to `POST /api/v1/query`. A call carries an optional
document-level `tail` (optional `cursor`, optional `settle`), available from
`irVersion` 15, on a `range` whose `to` is the relative anchor `now`. Each
response SHALL carry a `tail` member with the next `cursor`, the
`settled_through_ns` it read up to, the effective `settle_ns`, and
`caught_up`. The server SHALL hold no per-tail state between calls. Each call
SHALL be bounded like any query, and `page.size` SHALL bound the rows (or
traces) returned per call. Tailing SHALL be available to first parties
through the generated clients.

#### Scenario: The first call shows the latest rows

- **WHEN** a client submits a tailable document with `tail: {}` and
  `page.size` 200
- **THEN** the response holds the newest 200 matching rows whose tail-time is
  at or before `settled_through_ns`, ordered oldest first, and a `tail.cursor`
  positioned after the newest of them

#### Scenario: A follow-up call returns only new rows

- **WHEN** the client resubmits the document with the previous
  `tail.cursor`
- **THEN** the response holds only matching rows whose tail-time is after the
  previous call's position and at or before the new `settled_through_ns`,
  oldest first, and no row returned by an earlier call of the same tail

#### Scenario: Backlog is drained in bounded calls

- **WHEN** more new rows exist than `page.size`
- **THEN** the call returns `page.size` rows with `caught_up: false`, and the
  next call continues directly after them

#### Scenario: An idle tail advances

- **WHEN** a call finds no new rows
- **THEN** it returns no rows, `caught_up: true`, and a cursor positioned at
  `settled_through_ns`, so the next call does not rescan the idle interval

### Requirement: Tail ordering, settle delay and late data

A tail SHALL deliver rows in ascending order of a documented per-source
tail-time, then the source's tie-breakers. The tail-time SHALL be the span end
time for `traces` and the source time column for every other source. A call
at server time `T` SHALL read up to `T − settle`. The effective settle SHALL
be the requested `settle` clamped to server bounds whose minimum is at least
the deployment's ingest-to-queryable lag. The response SHALL echo the
effective value. A row that becomes queryable after the tail has passed its
tail-time SHALL NOT be delivered by that tail. This at-most-once behaviour
for late rows SHALL be documented, and so SHALL its causes: client export
delay beyond `settle`, an ingest backlog, and spans longer than the documented
tailable span duration.

#### Scenario: A span is tailed when it ends, not when it starts

- **WHEN** a 60-second span is ingested after it ends, while a `traces` tail
  is running
- **THEN** the span is delivered by the tail call whose window covers its end
  time, even though its start time is a minute older

#### Scenario: Settle is clamped and echoed

- **WHEN** a client requests `settle: "0s"`
- **THEN** the call uses the server minimum and reports it in
  `tail.settle_ns`

#### Scenario: A row later than settle is not delivered

- **WHEN** a row becomes queryable after a tail call has already read past
  its tail-time
- **THEN** no later call of that tail returns the row, and the documented
  late-data behaviour describes this

### Requirement: A lagging tail is moved forward explicitly

When a tail cursor's position is more than the server's maximum tail lag
behind the call's settle line (`T − settle`), the call SHALL skip forward to
that bound rather than scan the whole backlog, so a large `settle` is never
counted as lag. The settle line SHALL never move backwards, even when the
server clock is behind the cursor. The response SHALL carry a `tail_lagged`
warning naming the skipped interval. A tail SHALL never fail, and SHALL never
silently skip, because the client fell behind.

#### Scenario: A suspended client resumes with a lag warning

- **WHEN** a client resumes a tail whose cursor is older than the maximum tail
  lag
- **THEN** the call returns rows starting at the lag bound and a
  `tail_lagged` warning with the skipped interval's start and end

### Requirement: Only tailable documents can be tailed

A document carrying `tail` SHALL be paginatable (see
`query-result-pagination`), SHALL use the `rows` envelope (a `trace` envelope
cannot be tailed, since a tail delivers spans as they end), SHALL use a
relative `range.to` of `now`, and SHALL NOT contain an `order` stage, a
`match` stage, or a `limit` stage. A `tail` combined with `page.cursor` SHALL
be rejected. A violating document SHALL be rejected at validation with a 400
whose details give the reason `not_tailable` and name the offending stage,
envelope, or range bound.

#### Scenario: An absolute range cannot be tailed

- **WHEN** a document with an absolute `range.to` carries `tail`
- **THEN** it is rejected with `not_tailable` naming `range.to`

#### Scenario: A structural match cannot be tailed

- **WHEN** a document with a `match` stage carries `tail`
- **THEN** it is rejected with `not_tailable` naming the `match` stage

#### Scenario: A trace envelope cannot be tailed

- **WHEN** a document with `result: "trace"` carries `tail`
- **THEN** it is rejected with `not_tailable` naming `result`

#### Scenario: An aggregate cannot be tailed

- **WHEN** a document with an `aggregate` stage carries `tail`
- **THEN** it is rejected with `not_tailable` naming the aggregate stage

### Requirement: Tail calls are authenticated and tenant-scoped per call

Every tail call SHALL be authenticated and authorized like any query, with
read scopes checked for every source the document reads. Tenant and dataset
SHALL come from the authenticated request. A tail cursor SHALL be bound to the
tenant, dataset and document that produced it, under the same rules as a
pagination cursor. Revoking a key or scope SHALL take effect on the next call.
Tail calls SHALL count toward the tenant's query rate limits.

#### Scenario: A tail cursor is tenant-bound

- **WHEN** a tail cursor issued for one tenant is presented by another tenant
- **THEN** the call is rejected and no data is returned

#### Scenario: Revocation stops a running tail

- **WHEN** the API key used by a running tail is revoked
- **THEN** the next tail call fails with 401
