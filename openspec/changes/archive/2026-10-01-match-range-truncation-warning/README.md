# match-range-truncation-warning

A `match` over traces cut by the query `range` returns partial witness rows
or misses traces without saying so. This change adds a result warning,
`match_incomplete_trace`, raised when an evaluated trace's hierarchy is
visibly broken inside the range.
