## MODIFIED Requirements

### Requirement: Signal selection via URL path

The explore UI SHALL expose each signal view at its own path — `/logs`,
`/traces`, `/metrics`, `/profiles`, `/query` — rather than behind a query
parameter on a single path. Selecting a different signal (from the
navigation sidebar, the mobile navigation drawer, or the command palette)
SHALL navigate to that signal's path.

#### Scenario: Switching signals from the sidebar updates the path

- **WHEN** a user on `/logs` clicks "Traces" in the navigation sidebar
- **THEN** the browser URL path becomes `/traces`

#### Scenario: Navigating directly to a signal path renders that view

- **WHEN** a user opens `/metrics` directly (fresh load or external link)
- **THEN** the metrics view renders with "Metrics" marked as the current
  page in the navigation

## ADDED Requirements

### Requirement: Navigation sidebar

Every page inside the app shell SHALL show a navigation sidebar listing the
pages grouped as Monitor (Errors, Catalog), Investigate (Logs, Traces,
Metrics, Profiles, Query) and Configure (Schema, Processors,
Instrumentation), with the current page marked `aria-current="page"`, and
Manage shown only to users who can manage the tenant. Links to explore
pages SHALL carry the time range and tenant/dataset context and drop
view-specific state.

#### Scenario: Deep links keep their section current

- **WHEN** a user opens `/traces/{traceId}`
- **THEN** "Traces" is the current page in the sidebar

#### Scenario: Collapsing is remembered

- **WHEN** a user collapses the sidebar and reloads the page
- **THEN** the sidebar is still collapsed, showing icons only

#### Scenario: Tablets start collapsed

- **WHEN** a user with no saved choice opens the app in a viewport
  narrower than 1024px
- **THEN** the sidebar starts collapsed

### Requirement: Command palette

The app SHALL offer a command palette, opened with ⌘K / Ctrl+K or from the
page header's search field, that searches pages, catalog services, recent
queries and actions, supports ↑/↓/Enter/Escape, and offers a direct jump
when the query is a 16- or 32-digit hexadecimal trace id.

#### Scenario: Keyboard navigation to a page

- **WHEN** a user presses ⌘K, types "tra" and presses Enter
- **THEN** the palette closes and the browser navigates to `/traces`

#### Scenario: Pasted trace id

- **WHEN** a user pastes `4bf92f3577b34da6a3ce929d0e0e4736` into the palette
  and presses Enter
- **THEN** the browser navigates to `/traces/4bf92f3577b34da6a3ce929d0e0e4736`

### Requirement: Mobile navigation drawer

Below a 720px-wide viewport the sidebar SHALL be replaced by a top bar with
a menu button that opens the navigation in a drawer; choosing a page SHALL
navigate and close the drawer.

#### Scenario: Navigating from the drawer

- **WHEN** a user on a 390px-wide viewport opens the menu and taps "Traces"
- **THEN** the browser navigates to `/traces` and the drawer closes
