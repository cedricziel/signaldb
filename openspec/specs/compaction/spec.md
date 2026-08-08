# compaction Specification

## Purpose
Defines what background compaction must guarantee: merging small data files into larger sorted ones, scoped so that cost and memory track the amount of new data, correct under concurrent ingest, and safe to run continuously on a live multi-tenant deployment.
## Requirements
### Requirement: Compaction operates on closed partitions, never whole tables

Compaction SHALL plan, rewrite, and commit at the granularity of a single time partition, and SHALL only select partitions that are closed (their time range lies entirely before now minus a configurable lateness window). Compaction MUST NOT rewrite files outside the selected partition, and the work performed by one compaction job MUST be bounded by the size of that partition, not the size of the table.

#### Scenario: Only the target partition is rewritten

- **WHEN** a table holds data across 30 days of hourly partitions and one recent closed partition qualifies for compaction
- **THEN** the resulting commit removes and adds files only within that partition, and files in all other partitions are byte-for-byte untouched

#### Scenario: Open partitions are not compacted

- **WHEN** a partition's time range overlaps now minus the configured lateness window
- **THEN** the planner does not select it, and the deferral is observable (metric or log), so ingest into the current hour never races compaction of the same partition

### Requirement: Small-file count triggers compaction candidacy

The planner SHALL treat a partition as a compaction candidate when its count of data files below the target file size meets a configured threshold. Small files MUST NOT be excluded from candidacy or from job inputs on account of being small; a minimum-input-size filter that skips small files SHALL NOT exist. Files already at or above the target size MAY be excluded from job inputs. Every configured planning limit SHALL either be enforced or not exist.

#### Scenario: A partition of tiny files qualifies

- **WHEN** frequent ingest commits produce a closed partition containing many files far below the target file size (e.g. hundreds of files under 100 KiB)
- **THEN** the partition is selected as a candidate and its small files are the job's inputs

#### Scenario: Already-compacted partitions are left alone

- **WHEN** a closed partition consists of files at or above the target file size
- **THEN** the planner does not select it, so compaction converges instead of rewriting indefinitely

### Requirement: Compaction commits are deltas that tolerate concurrent ingest

A compaction commit SHALL remove exactly the job's input files and add exactly its output files. A commit MUST fail only when a conflicting change touched the job's own input set (an input file was removed or the partition's contents changed); appends elsewhere in the table — including new files in other partitions committed while the job ran — MUST NOT invalidate the commit. Conflict classification SHALL be based on typed errors or snapshot inspection, not on matching error-message text.

#### Scenario: Ingest lands during a long rewrite

- **WHEN** a compaction job runs for minutes on a busy table while ingest commits new files to the current-hour partition every few seconds
- **THEN** the compaction commit succeeds without retry-until-starvation, and the resulting snapshot contains both the compacted files and all concurrently ingested files

#### Scenario: True conflict aborts cleanly

- **WHEN** retention drops the partition a compaction job was rewriting before the job commits
- **THEN** the commit fails as a conflict, no snapshot referencing the job's output is created, and the job's output files are eligible for physical cleanup

### Requirement: Compaction memory is bounded and configured

A compaction job SHALL execute with an explicit memory budget and stream its rewrite rather than materializing the full input in memory; peak memory MUST NOT scale with table size. Exceeding the budget SHALL fail the job with a resource error rather than exhausting host memory.

#### Scenario: Large partition, small budget

- **WHEN** a partition's decoded size exceeds the configured compaction memory budget
- **THEN** the job either completes by streaming/spilling within the budget or fails with an attributable resource error, and the process is not OOM-killed

### Requirement: Compacted output preserves rows and physical sort

Compaction output SHALL contain exactly the rows of its inputs (verified by count before commit), be written sorted by the table's per-signal sort key, and target the configured file size measured as encoded (compressed) bytes, not in-memory bytes.

#### Scenario: Row-count parity is enforced

- **WHEN** a rewrite produces an output row count different from its input row count
- **THEN** the job aborts before committing and reports the discrepancy

#### Scenario: Output files approximate the target size

- **WHEN** compaction merges 500 MiB (encoded) of small files with a 128 MiB target file size
- **THEN** the committed output is on the order of four files, not one file 5–10× under target due to in-memory size estimation

