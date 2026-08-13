# explore-ui-errors Specification

## Purpose

Defines the explore UI's Errors tab: grouping and ranking exceptions found
across traces and logs, and letting a user drill into one concrete example.

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

### Requirement: Drilling into an example occurrence

Selecting an exception group SHALL fetch one concrete occurrence and show
its stacktrace. When that occurrence carries a trace id, a link SHALL open
the trace waterfall for that exact trace; when it does not (e.g. a log-only
exception with no active trace), no such link SHALL be offered.

#### Scenario: Viewing a traced exception's example

- **WHEN** a user selects a group whose example occurrence carries a trace
  id
- **THEN** the group's stacktrace is shown alongside a link that opens that
  trace's waterfall

#### Scenario: Viewing an untraced exception's example

- **WHEN** a user selects a group whose example occurrence carries no trace
  id
- **THEN** the group's stacktrace is shown with no trace link offered

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
