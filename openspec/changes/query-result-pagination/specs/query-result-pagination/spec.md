## Purpose

Defines how a large native query result is delivered in bounded, resumable
pages: the continuation token a client uses to walk it, the bounds that keep
paging from becoming an unbounded export, and what a token guarantees when the
underlying data changes between pages. Stub scope; the requirements below are
the headline guarantees to be expanded when the change is picked up.

## ADDED Requirements

### Requirement: Pagination of large results

SignalDB SHALL support walking a large `rows`/`trace` result in bounded pages
via an opaque continuation token, so a client can retrieve the full result set
without a single unbounded response. The token SHALL be opaque to the client and
SHALL be valid only for the query that produced it and only for the
authenticated tenant.

#### Scenario: A large result is walked in pages

- **WHEN** a query result exceeds a single page and the client requests the next
  page with the returned continuation token
- **THEN** the subsequent page continues from where the previous one ended,
  within documented page-size and total-scan bounds

#### Scenario: The final page ends the walk

- **WHEN** the last page of a result is returned
- **THEN** the response carries no continuation token, so completion is
  observable rather than inferred from an empty page

#### Scenario: A token is tenant-bound

- **WHEN** a continuation token issued for one tenant is presented by another
- **THEN** the request is rejected, and no data is returned

### Requirement: Paging consistency across data lifecycle events

A continuation token SHALL carry a defined consistency guarantee across
compaction, retention expiry, and snapshot lifecycle, and a token that can no
longer honour it SHALL be reported as expired rather than silently skipping or
repeating rows.

#### Scenario: An expired token is reported

- **WHEN** a client presents a continuation token whose underlying snapshot is
  no longer available
- **THEN** the request fails with an explicit expiry error, not a partial or
  silently shifted page
