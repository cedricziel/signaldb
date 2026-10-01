## ADDED Requirements

### Requirement: Catalog Map view

The Catalog SHALL offer a List | Map switch for the service entity type. The Map view SHALL draw the tenant's service graph for the selected time range and filters, with each service node showing request rate, error rate and p95, edge thickness scaled by call rate, and edges colored by error rate (neutral below 0.5%, warning from 0.5%, critical from 2%). External nodes SHALL be visually distinct from instrumented services and can be hidden. The chosen view SHALL be kept in the URL so a link reopens the map. Hovering a node or edge SHALL show its figures through the shared visualization tooltip.

#### Scenario: Open the map

- **WHEN** a user on the Catalog's service list selects Map
- **THEN** the URL records the map view and the page shows the service graph for the current time range

#### Scenario: Select a service

- **WHEN** the user clicks a service node
- **THEN** a side panel shows its rate, error rate, p95, callers and dependencies, with links to the service page, its traces and its errors

### Requirement: Service page neighbourhood map

A service's detail page SHALL show a one-hop map with the service in the middle, its callers on the left and its dependencies on the right, next to the time-by-dependency breakdown. Clicking a neighbouring service SHALL open that service's page. A Map | Table switch SHALL show the same edges as a table.

#### Scenario: Walk to a caller

- **WHEN** a user on the `orders` page clicks the caller node `api-gateway`
- **THEN** the `api-gateway` service page opens with the same time range

#### Scenario: Service with no callers

- **WHEN** the service has no incoming edges in the window
- **THEN** the callers column states that no callers were seen in the time range

### Requirement: Trace map beside the waterfall

The trace detail SHALL offer a Waterfall | Map | Both switch. The Map SHALL show the services involved in the trace, the time spent in each, and the calls between them, marking failed calls. It SHALL be built from the trace's already-loaded spans without another query. Clicking a service in the map SHALL filter the waterfall to that service's spans.

#### Scenario: Failed call is visible

- **WHEN** a trace contains an error span in `payments` called from `api-gateway`
- **THEN** the map marks the `api-gateway → payments` edge and the `payments` node as failed

#### Scenario: Filter waterfall by service

- **WHEN** the user clicks `payments` in the trace map
- **THEN** the waterfall shows only `payments` spans until the filter is cleared
