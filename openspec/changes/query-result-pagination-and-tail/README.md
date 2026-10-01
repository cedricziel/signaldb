# query-result-pagination-and-tail

Cursor pagination and live tail over Query IR `rows`/`trace` results, using
one keyset cursor: a `page` walks a bounded result, and a `tail` follows a
sliding window forward in time over plain HTTP polling.

> Renamed from `query-result-pagination` (2026-10-01): live tail moved back
> into scope after the streaming epic #437's sub-issues were closed as not
> planned. See proposal.md.
