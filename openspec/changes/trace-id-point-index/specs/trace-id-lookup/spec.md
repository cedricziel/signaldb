# trace-id-lookup Specification

## ADDED Requirements

### Requirement: A bare trace-id lookup is complete

A trace lookup by id with no time range SHALL return every span the dataset
holds for that trace id, wherever in retention those spans fall, including
traces whose spans sit in hours days apart. The index SHALL NOT cause a span
to be omitted. When the index cannot answer (no index table, a read error, or
hours outside its coverage), the lookup SHALL fall back to scanning `traces`
directly for those hours rather than returning fewer spans.

#### Scenario: Trace spread across days is returned whole

- **WHEN** a trace has spans in three hours across two days and is looked up
  by id with no time range
- **THEN** all of its spans are returned

#### Scenario: Index commit lost between writer commits

- **WHEN** the writer committed index rows for a batch but the span data
  commit failed and was retried later
- **THEN** a lookup returns every span once the data commit lands, and before
  that it returns what `traces` holds, never an error

#### Scenario: Dataset without an index table

- **WHEN** a dataset has a `traces` table but no `trace_index` table
- **THEN** a bare-id lookup returns the same spans it returns today

### Requirement: A bare trace-id lookup is bounded by the hours the trace touches

For hours the index covers, a bare-id lookup SHALL read span data only from
the hour partitions the index lists for that id, plus the recent hot window.
Its cost SHALL NOT grow with the number of hours in retention beyond the
index lookup itself.

#### Scenario: Cold lookup opens only the trace's hours

- **WHEN** a dataset holds 30 days of indexed traces and a trace touching one
  hour is looked up by id on a cold cache
- **THEN** the `traces` scan opens files only from that hour and from the hot
  window, not from the other hours

#### Scenario: Unknown trace id

- **WHEN** a trace id with no spans in the dataset is looked up
- **THEN** the result is empty (not found), and no hour outside the hot
  window and the unindexed range is scanned

### Requirement: Lookups with a time range are unchanged

A trace lookup that carries a time range SHALL behave as it does without the
index: it is bounded by the given range and does not consult the index.

#### Scenario: Tempo lookup with start and end

- **WHEN** `/api/traces/{id}?start=..&end=..` is called
- **THEN** the querier applies the range as today and returns the same spans

### Requirement: The index is tenant- and dataset-scoped

Each dataset SHALL have its own `trace_index` table in its tenant's
namespace. A lookup SHALL consult only the index of the dataset it targets,
and the index SHALL never reveal ids or hours from another tenant or dataset.

#### Scenario: Same trace id in two tenants

- **WHEN** tenants A and B both hold spans for the same trace id and A looks
  it up
- **THEN** only A's spans and A's hours are used

### Requirement: Index lifecycle follows the traces table

The `trace_index` table SHALL be provisioned wherever the `traces` table is
provisioned, kept within the dataset's trace retention, and removed when its
dataset or tenant is removed.

#### Scenario: Reconciler provisions the index

- **WHEN** the writer's table reconciler ensures a dataset's `traces` table
- **THEN** the dataset's `trace_index` table exists too

#### Scenario: Retention bounds the index

- **WHEN** hours older than the dataset's trace retention have been dropped
  from `traces` and the index shard has been compacted since
- **THEN** the index holds no rows for those hours
