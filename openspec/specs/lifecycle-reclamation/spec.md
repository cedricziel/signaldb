# lifecycle-reclamation Specification

## Purpose
Defines what retention enforcement and orphan-file cleanup must guarantee: logically expired or superseded data is eventually physically reclaimed, live data is never deleted, and both properties hold under default configuration.
## Requirements
### Requirement: Live-file determination is derived from current table metadata, never snapshot age

Orphan detection SHALL derive the live-file set from the manifests reachable from the table's current snapshot, unioned with the manifests of every retained snapshot. Snapshot creation time MUST NOT be used to exclude a reachable manifest or file from the live set. A file SHALL be classified an orphan candidate only if no retained snapshot references it.

#### Scenario: Reused manifests keep old files live

- **WHEN** the current snapshot references a manifest created long before any age threshold, containing EXISTING data files added by old snapshots
- **THEN** those files are in the live set and are never deletion candidates

#### Scenario: Idle table loses nothing

- **WHEN** a table has received no commits for longer than any configured age window and orphan cleanup runs
- **THEN** the live set equals the table's current content, zero files are classified as orphans, and the table remains fully queryable

#### Scenario: Genuinely orphaned files are found

- **WHEN** a failed ingest or compaction commit left Parquet files in the table's storage location that no retained snapshot references, and they are older than the grace period
- **THEN** they are classified as orphan candidates and reclaimed

### Requirement: Logical deletion is followed by physical reclamation under default configuration

A default deployment SHALL physically reclaim storage: retention enforcement, snapshot expiration, and compaction MAY defer physical deletion to orphan cleanup, but orphan cleanup SHALL be enabled (not dry-run) by default with a non-zero grace period, so that bytes freed logically are eventually freed physically without operator action. Disabling reclamation SHALL remain a supported explicit opt-out.

#### Scenario: Retention frees storage without extra configuration

- **WHEN** a deployment runs with default configuration and retention expires a partition
- **THEN** within the cleanup interval plus grace period, the partition's data files are physically deleted from object storage

#### Scenario: Superseded compaction inputs are reclaimed

- **WHEN** a compaction commit replaces small input files with merged output files
- **THEN** the input files, once unreferenced by all retained snapshots and past the grace period, are physically deleted

### Requirement: Partition identity comes from table metadata, and unclassifiable files are loud

Any component that needs a file's partition value (retention enforcement, compaction planning, cleanup) SHALL read it from the table's manifest entries, not by parsing the file's storage path. If a file's partition cannot be determined, the component SHALL surface an attributable error signal (metric and log naming the table and file) rather than silently skipping or silently retaining the file, and SHALL fail safe (retain, never delete).

#### Scenario: Storage layout changes do not break retention

- **WHEN** the table's file-location scheme changes (e.g. hashed object-storage prefixes) so paths no longer embed partition key-value strings
- **THEN** retention continues to classify and drop expired partitions correctly

#### Scenario: Unclassifiable file is reported, kept, and counted

- **WHEN** a manifest entry yields no usable partition value for a file
- **THEN** the file is retained, and an error metric/log identifies the table and file so the condition cannot persist unnoticed

### Requirement: Reclamation is safe against in-flight work

Physical deletion SHALL respect a configurable grace period covering query execution and commit publication: files newer than the grace period, and files referenced by any retained snapshot, MUST NOT be deleted. Immediately before deleting a candidate, cleanup SHALL re-validate it against freshly loaded current table metadata; this re-validation is a mandatory part of deletion, not an optional flag.

#### Scenario: Concurrent commit rescues a candidate

- **WHEN** a file classified as an orphan candidate becomes referenced by a snapshot committed between detection and deletion
- **THEN** re-validation removes it from the deletion set and the file survives

#### Scenario: Running query is not broken

- **WHEN** a query pinned to a retained snapshot is executing while cleanup runs
- **THEN** every file that query's snapshot references is excluded from deletion

