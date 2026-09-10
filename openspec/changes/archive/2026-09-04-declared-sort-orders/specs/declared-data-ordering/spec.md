# Declared Data Ordering Delta Spec

## Purpose

Defines the ordering contract between file producers (ingest, compaction) and the query engine: when a sort order is declared, it is physically true; queries stay correct over any mix of sorted and unsorted files; and time-ordered/limited queries get the acceleration the declared order enables.

## ADDED Requirements

### Requirement: Signal tables declare a canonical sort order

Every signal table SHALL declare a sort order in its table metadata at creation time, with a time-leading key per signal type (traces, logs, metrics, profiles each have one defined canonical key). The declared order SHALL be the single source of truth consumed by all file producers and by the query engine; no component may assume an ordering that is not declared.

#### Scenario: New table carries the declaration

- **WHEN** a signal table is created for a tenant/dataset
- **THEN** its table metadata contains the canonical sort order for that signal type, observable via the catalog

#### Scenario: One key per signal, everywhere

- **WHEN** ingest and compaction both write files for the same table
- **THEN** both produce files ordered by the same declared key (no producer-specific orderings)

### Requirement: Declared order is physically honest per file

A data file SHALL only be attributed the declared sort order (in file metadata and in scan-time ordering claims) if its rows are actually sorted by that key. Any producer that writes a file it cannot guarantee sorted MUST write it without the ordering attribution. Honesty SHALL be verifiable: an ordering-attributed file failing a sortedness check is a defect, not a tolerated state.

#### Scenario: Ingest batches are sorted before write

- **WHEN** the writer persists a batch group whose rows arrived out of time order
- **THEN** the written file's rows are sorted by the declared key and the file carries the ordering attribution

#### Scenario: Unsorted producer stays honest

- **WHEN** a code path writes rows whose order it cannot guarantee (e.g. a recovery/backfill path without a sort step)
- **THEN** the resulting file carries no ordering attribution, and queries over it remain correct (it is treated as unsorted)

#### Scenario: Sorted output without a declaration stays unattributed

- **WHEN** a producer sorts its rows by the canonical key for a table whose metadata does not (yet) declare that sort order
- **THEN** the resulting file carries no ordering attribution, even though its rows are in key order

Attribution is a claim about the table's _declared_ order, so there is nothing for such a file to attest. Sorting without claiming is the conservative direction and MUST NOT be "optimized" into attribution later: the physical layout is whatever the producer already produced, and no reader can be misled into eliding a sort it needed.

### Requirement: Queries are correct over mixed sorted and unsorted files

Query results SHALL be identical (up to result-set ordering guarantees actually requested by the query) regardless of whether the files scanned are ordering-attributed, unattributed, or a mixture. Sort elimination or ordered-scan optimizations MUST only be applied when every file relied upon carries an honest ordering attribution; otherwise the engine SHALL retain explicit sorts.

#### Scenario: Legacy files do not corrupt ordered results

- **WHEN** a table holds pre-change files without ordering attribution alongside new attributed files, and a query requests `ORDER BY timestamp DESC LIMIT n`
- **THEN** the result is exactly the true top-n rows, identical to the result computed with all optimizations disabled

#### Scenario: Fully attributed partitions skip redundant sorting

- **WHEN** every file in the scanned range is ordering-attributed
- **THEN** the query plan does not re-sort scan output that already satisfies the requested order (observable via plan inspection)

### Requirement: Time-ordered limited queries exploit declared order and statistics

For queries of the shape "time-range filter, order by time, limit n" on tables with declared sort order, the engine SHALL use per-file statistics and the declared ordering to prioritize and prune file reads, such that I/O scales with the files needed to satisfy the limit rather than with all files in the time range.

#### Scenario: Recent-first query reads recent files first

- **WHEN** a `ORDER BY timestamp DESC LIMIT 20` query spans a range containing many attributed files
- **THEN** scan metrics show files/row-groups skipped relative to a full-range scan

The skipping comes from per-file statistics: the engine reads files newest-first and its TopK's dynamic filter prunes the rest, which is available to attributed and unattributed files alike. The engine does not yet elide a sort for the _reverse_ of a declared order, so this shape keeps its `SortExec` and the benchmark suite shows no difference against the undeclared baseline for it. The measured numbers live in `docs/contributing/benchmarking.md`.

#### Scenario: Oldest-first query stops at the first files

- **WHEN** a `ORDER BY timestamp ASC LIMIT 20` query spans a range containing many attributed, non-overlapping files
- **THEN** the plan carries no `SortExec`, and scan metrics show that only the leading file of each file group was opened — the remaining files in range were never read

### Requirement: Compaction converges tables toward attributed files

Compaction SHALL write ordering-attributed output for tables with a declared sort order, so any partition it processes becomes fully attributed. Compaction MUST NOT regress attribution (rewriting attributed files into unattributed output is a defect).

#### Scenario: Old partition becomes fully attributed

- **WHEN** compaction rewrites a partition containing legacy unattributed files
- **THEN** the partition's live files afterwards are all ordering-attributed and sorted by the declared key
