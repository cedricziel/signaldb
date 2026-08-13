## MODIFIED Requirements

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
