## ADDED Requirements

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
