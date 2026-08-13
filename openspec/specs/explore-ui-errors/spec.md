# explore-ui-errors Specification

## Purpose

Defines the explore UI's Errors tab: grouping and ranking exceptions found
across traces and logs, and letting a user drill into a group's individual
occurrences.

## Requirements

### Requirement: Exceptions grouped across traces and logs

The Errors tab SHALL show exceptions grouped by (exception type, exception
message, service name), combining both places an exception can be recorded
— a span's `exception` event, and a log record's own exception attributes —
since neither source alone is complete. Each group SHALL show its
occurrence count and first/last-seen timestamps, ranked by count.

#### Scenario: A group appears once per distinct exception across both sources

- **WHEN** the same exception type/message/service occurs both as a captured
  span event and as a log record within the selected window
- **THEN** each source contributes its own group (traces and logs are not
  merged into one row), and both are shown ranked among all groups by count

#### Scenario: Ranked by frequency

- **WHEN** the Errors tab loads for a window with multiple distinct
  exceptions
- **THEN** the most frequently occurring exception group is listed first

### Requirement: Drilling into a group's occurrences

Selecting an exception group SHALL fetch its individual occurrences (up to
25, newest first), mirroring the group→instances pattern used for trace
groups and catalog entities elsewhere in the explore UI. Each occurrence
SHALL independently show its own timestamp and, when it carries a trace id,
a link that opens the trace waterfall for that exact trace; an occurrence
with no trace id (e.g. a log-only exception with no active trace) SHALL
offer no such link — occurrences of the same group are not assumed to share
one trace outcome. Selecting an occurrence SHALL show its own stacktrace.

#### Scenario: Occurrences of one group differ in trace linkage

- **WHEN** a user selects a group where some occurrences happened inside an
  active trace and others did not
- **THEN** the occurrence list shows a trace link only for the occurrences
  that carry a trace id, not for the group as a whole

#### Scenario: Viewing one occurrence's stacktrace

- **WHEN** a user selects an occurrence from the list
- **THEN** that occurrence's own stacktrace is shown, independent of any
  other occurrence in the same group

#### Scenario: Opening a trace does not also expand the stacktrace

- **WHEN** a user clicks an occurrence's trace link
- **THEN** the trace waterfall opens for that trace, and the occurrence's
  stacktrace does not also toggle open from the same click

### Requirement: Faceted narrowing of the exception group list

The Errors tab SHALL offer a facet sidebar over exception type, service, and
source, each showing its distinct values with the summed occurrence count of
the groups carrying that value. Selecting a value SHALL narrow the
displayed group list to groups matching every active filter; a facet's own
active filter SHALL NOT narrow its own value counts, so its alternatives
stay visible and selectable.

#### Scenario: Selecting a facet value narrows the list

- **WHEN** a user expands the Source facet and selects "logs"
- **THEN** only exception groups sourced from logs remain in the list, and a
  removable filter chip for the selection appears

#### Scenario: A facet's own filter does not narrow its own counts

- **WHEN** a filter on the Source facet is active
- **THEN** the Source facet itself still shows every source value with its
  full (unfiltered-by-itself) count, so the user can switch sources
