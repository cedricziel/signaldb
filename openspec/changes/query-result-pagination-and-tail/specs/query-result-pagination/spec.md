## Purpose

Defines how a large native query result is delivered in bounded, resumable
pages: the continuation cursor a client uses to walk a `rows` or `trace`
result, the ordering a cursor relies on, the bounds that keep paging from
becoming an unbounded export, and what a cursor guarantees when the underlying
data changes between pages.

## ADDED Requirements

### Requirement: Pagination of large results

SignalDB SHALL let a client walk a `rows` or `trace` Query IR result in
bounded pages. A document requests pagination with an optional
document-level `page` (`size`, optional `cursor`), available from `irVersion` 14. The response SHALL carry a `page` member whose `next_cursor` is present
exactly when more of the result exists. A client continues by resubmitting
the same document with `page.cursor` set to the previous `next_cursor`. A
cursor SHALL be opaque to clients, and clients SHALL NOT construct or modify
one. A document without `page` SHALL behave and respond exactly as before
this capability.

#### Scenario: A large result is walked in pages

- **WHEN** a paginatable query's result exceeds `page.size` and the client
  resubmits the document with the returned `next_cursor`
- **THEN** the next page continues directly after the last row of the
  previous page, and walking until no `next_cursor` is returned yields every
  row of the result exactly once

#### Scenario: The final page ends the walk

- **WHEN** the last page of a result is returned
- **THEN** the response carries a `page` member with no `next_cursor`, so
  completion is observable rather than inferred from an empty page

#### Scenario: Page size is bounded

- **WHEN** a document requests a `page.size` above the server maximum
- **THEN** it is rejected at validation, naming the maximum

#### Scenario: A page is under the version gate

- **WHEN** a document carries `page` with `irVersion` below 14
- **THEN** it is rejected as unsupported for that version

#### Scenario: A querier that cannot page is reported

- **WHEN** a paged document reaches a querier that does not report a page
  position (one not yet upgraded)
- **THEN** the request fails with 503, never with an unpaged result

### Requirement: Pagination requires a total order and never splits ties

A paginated result SHALL be ordered by a total order. The leading keys SHALL
be the document's `order` stage keys if it has one. Otherwise the server
SHALL apply a documented per-source default (the source time column
descending), and a `match` pipeline SHALL keep its native
`(trace_id, start time, span_id)` order. In both cases the server SHALL append
the source's documented tie-breaker columns. A page SHALL end only where the
full sort key changes, so rows with equal keys are always returned on the
same page. If such a tie group exceeds the server's tie bound, the request
SHALL fail with an explicit resource-limit error asking for a further `order`
key. For the `trace` envelope, the page unit SHALL be a whole trace:
`page.size` counts traces, and a trace SHALL NOT be split across pages.

#### Scenario: Default order without an order stage

- **WHEN** a paginated `logs` document has no `order` stage
- **THEN** pages are ordered newest first by the log timestamp, with the
  documented tie-breakers, and the same document walked twice over unchanged
  data yields the same pages

#### Scenario: Equal keys stay on one page

- **WHEN** the row at the page boundary shares its full sort key with
  following rows
- **THEN** those rows are returned on the same page, and the next page
  starts after them

#### Scenario: An oversized tie group is an explicit error

- **WHEN** more rows than the tie bound share one full sort key
- **THEN** the request fails with a `resource_limit` error asking the caller
  to add an `order` key, rather than returning a page that splits or drops
  them

#### Scenario: Trace envelope pages whole traces

- **WHEN** a paginated `match` document with `result: "trace"` is walked
- **THEN** each page holds at most `page.size` traces, each with all of its
  result rows, and no trace appears on two pages

### Requirement: Only non-aggregated row results can be paginated

Pagination SHALL be accepted only for single documents whose envelope is
`rows` or `trace`, whose pipeline has no `aggregate`, `topk`, `bottomk`,
`rank`, or `describe` stage, and whose `limit` stage, if any, is the last
stage. A trailing `limit` SHALL cap the whole walk, even inside a tie group.
For the `trace` envelope, the leading sort key SHALL be `trace_id`, and a
trailing `limit` SHALL be rejected, since it counts spans while a trace page
counts traces. Field names starting with `__sdb_` are reserved for the columns
paging adds, and a paginated document naming one SHALL be rejected with a 400.
Any other document carrying `page` SHALL be rejected at validation with a 400
whose details give the reason `not_paginatable` and name the offending stage
or envelope.

#### Scenario: An aggregate cannot be paginated

- **WHEN** a document with an `aggregate` stage, or a `series`, `table`,
  `heatmap`, `flamegraph`, `graph`, `metadata`, or `scalar` envelope,
  carries `page`
- **THEN** it is rejected with a 400 whose details carry `not_paginatable`
  and name the aggregate stage or the envelope

#### Scenario: A trailing limit caps the walk

- **WHEN** a paginated document ends in `limit: 2500` and is walked with
  `page.size` 1000
- **THEN** the walk yields three pages of 1000, 1000 and 500 rows, and the
  third carries no `next_cursor`

#### Scenario: A non-trailing limit is rejected

- **WHEN** a document carrying `page` has a `limit` stage followed by another
  stage
- **THEN** it is rejected with `not_paginatable` naming the `limit` stage

### Requirement: A cursor is bound to its tenant, dataset and document

A cursor SHALL be valid only for the tenant, dataset and document (ignoring
the cursor itself) that produced it, and SHALL be checked on every request
before any data is read. Tenant and dataset SHALL come from the authenticated
request, never from the cursor. A cursor SHALL be carried only in request and
response bodies.

#### Scenario: A token is tenant-bound

- **WHEN** a cursor issued for one tenant is presented by another tenant
- **THEN** the request is rejected, and no data is returned

#### Scenario: A cursor cannot be moved to another document

- **WHEN** a cursor is resubmitted with a document whose filter, source,
  range, or `irVersion` differs from the one that produced it
- **THEN** the request is rejected as a cursor/document mismatch

#### Scenario: A corrupted cursor is rejected

- **WHEN** a cursor's content does not match its checksum
- **THEN** the request is rejected with a 400, not executed with a guessed
  position

#### Scenario: An edited cursor is rejected when the server has a secret

- **WHEN** the server has a shared secret (`[auth].internal_service_key`) and
  a client edits a cursor and recomputes a plain checksum, or presents an
  oversized cursor or one issued in the future
- **THEN** the request is rejected with a 400

#### Scenario: Scopes are rechecked on every page

- **WHEN** the caller's read scope for the source is revoked between two
  pages
- **THEN** the next page request fails with 403

### Requirement: Paging consistency across data lifecycle events

The first page SHALL resolve the document's `range` to an absolute window,
and every later page SHALL use that same window. Within one walk, no row
SHALL be returned twice, and every row that exists for the whole walk SHALL
be returned. The answer SHALL be unaffected by compaction and by the
committed/unflushed boundary. A row added during the walk SHALL be returned
only if its sort key falls after the cursor of the page that reads it. A row
removed during the walk (retention, deletion) SHALL no longer be returned. A
cursor past its documented lifetime, or from an incompatible server version,
SHALL be reported as expired rather than silently skipping or repeating rows.

#### Scenario: Compaction between pages changes nothing

- **WHEN** the files holding a walked result are compacted between two pages
- **THEN** the remaining pages return exactly the rows they would have
  returned without compaction

#### Scenario: A relative range does not drift

- **WHEN** a document with `range` `now-1h`..`now` is walked over several
  minutes
- **THEN** every page reads the window resolved by the first page, not a
  window shifted to the current time

#### Scenario: An expired token is reported

- **WHEN** a client presents a cursor older than the documented lifetime, or
  one from an incompatible cursor version
- **THEN** the request fails with HTTP 410, not a partial or silently shifted
  page

### Requirement: Paging is bounded

Paging SHALL be bounded per page (rows and bytes) and per walk (total rows).
A page that reaches the byte bound SHALL end early at a key boundary and
still carry `next_cursor`; a page whose first key group (or, for the `trace`
envelope, first trace) alone exceeds the byte bound, or a trace with more
spans than the server's span bound, SHALL fail with a resource-limit error. A
page SHALL be clamped to the rows left in the walk bound, and a walk that
would exceed the total-row bound SHALL fail with an explicit resource-limit
error. Paging SHALL NOT be an unbounded
export path, and every page SHALL be subject to the same rate limits as any
query.

#### Scenario: The byte bound ends a page early

- **WHEN** a page's rows would exceed the per-page byte bound before
  reaching `page.size`
- **THEN** the page ends at the last complete key group within the bound and
  carries `next_cursor`

#### Scenario: The walk bound is explicit

- **WHEN** a walk reaches the total-row bound
- **THEN** the next page request fails with a `resource_limit` error naming
  the bound, rather than returning a truncated page without `next_cursor`
