# explore-ui-navigation Specification

## Purpose

Defines how the explore UI's client-side URLs map to on-screen views, so
every reachable screen — a signal view, the tenant management panel, the
OAuth consent screen — has a distinct, bookmarkable, shareable address and
participates correctly in browser back/forward history.

## Requirements

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

### Requirement: Unrecognized paths resolve to the logs view

A path whose signal segment is not one of the known signals SHALL redirect
to `/logs`, preserving any query string from the original URL.

#### Scenario: Unknown path redirects preserving query params

- **WHEN** a user opens `/bogus?range=15m`
- **THEN** the browser URL becomes `/logs?range=15m` and the logs view renders

### Requirement: Root path redirects to the overview

Navigating to the site root SHALL redirect to `/overview`, preserving any
query string from the original URL.

#### Scenario: Root redirects to the overview

- **WHEN** a user opens `/`
- **THEN** the browser URL becomes `/overview` and the overview renders

#### Scenario: Root redirect preserving query params

- **WHEN** a user opens `/?tenant=acme&dataset=prod`
- **THEN** the browser URL becomes `/overview?tenant=acme&dataset=prod`

### Requirement: Tenant management is reachable via a dedicated URL

The tenant/API-key management panel SHALL be reachable at its own URL,
`/manage`, rather than as component state with no URL representation. Only
users who are a tenant admin or instance admin SHALL see the panel;
everyone else navigating to `/manage` SHALL be redirected to `/logs`.
Because it is a real route, navigating to it creates browser history, so
using the browser back button after opening it SHALL return to the
previous view instead of leaving the panel open.

#### Scenario: Admin opens /manage directly

- **WHEN** a tenant admin or instance admin navigates to `/manage`
- **THEN** the management panel renders

#### Scenario: Non-admin is redirected away from /manage

- **WHEN** a user who is neither a tenant admin nor an instance admin
  navigates to `/manage`
- **THEN** the browser URL becomes `/logs` and the management panel does not
  render

#### Scenario: Back button closes the management panel

- **WHEN** a user opens `/manage` from a signal view and then uses the
  browser back button
- **THEN** the browser returns to the signal view they came from and the
  management panel is no longer shown

### Requirement: OAuth consent screen is a standalone route

`/oauth/consent` SHALL render the OAuth connector consent screen on its own,
outside the explore shell (no top bar, no signal tabs), independent of any
explore-view query state.

#### Scenario: Consent screen renders standalone

- **WHEN** a user is redirected to `/oauth/consent` with valid authorization
  parameters
- **THEN** the consent screen renders without the explore shell's top bar or
  signal tabs

### Requirement: Non-signal state stays in the query string

Time range, filters, search text, live-tail mode, trace/group selection,
grouping dimension, grouping grain, PromQL expression, profile type/service
selectors, and tenant/dataset context SHALL remain represented as URL query
parameters, independent of which signal path is active, so a view (including a
specific trace or a specific PromQL query) remains bookmarkable and shareable.

#### Scenario: Query parameters survive a signal switch

- **WHEN** a user on `/logs?tenant=acme&dataset=prod` switches to the
  traces signal
- **THEN** the resulting URL is `/traces?tenant=acme&dataset=prod`

#### Scenario: A shared link reproduces the grouping grain

- **WHEN** a user shares a traces view whose group table counts spans rather
  than traces
- **THEN** opening that link presents the table at the same grain

### Requirement: Catalog entity selection via URL path

The catalog SHALL address the selected entity type and any drilled-into entity
in the URL path, not in query parameters: `/catalog/:entity` shows the list for
entity type `:entity` (an entity type id such as `service`, `database`, `host`,
`k8s_pod`), `/catalog/:entity/:primary` shows that entity's detail, and
`/catalog/:entity/:primary/:secondary` the breakdown row drilled into within
it. `:primary` and `:secondary` SHALL encode their identity values as
comma-separated, percent-encoded segments (a value containing `,` or `/` is
percent-encoded, so the split is unambiguous), and a not-set identity value
SHALL round-trip. `/catalog` with no further segment SHALL show the default
entity type's list. Time range and tenant/dataset context SHALL remain query
parameters as on every other view.

#### Scenario: Drilling into an entity navigates to its route

- **WHEN** a user on `/catalog/service?tenant=acme` opens the entity whose
  `service.name` is `checkout` and `service.namespace` is `shop`
- **THEN** the URL becomes `/catalog/service/checkout,shop?tenant=acme` and
  the entity detail renders

#### Scenario: An entity route is directly addressable

- **WHEN** a user opens `/catalog/host/db-01?tenant=acme` directly
- **THEN** the catalog renders the `host` entity type with `db-01`'s detail
  view, and the browser back button returns to the previous view

#### Scenario: Identity values with reserved characters round-trip

- **WHEN** an entity's identity value is `a/b,c`
- **THEN** its route segment is `a%2Fb%2Cc` and opening that URL selects the
  same entity

#### Scenario: Legacy query parameters are not honoured

- **WHEN** a user opens `/catalog?entity=service&primary=x`
- **THEN** the default entity list renders (the query parameters are ignored)

### Requirement: Login screen is a standalone route

`/login` SHALL render the sign-in page on its own, outside the explore
shell (no top bar, no signal tabs) and without a modal dialog. It SHALL
accept an optional `?redirect=<path>` honoured only for a same-app relative
path (anything else falls back to `/logs`), and an optional `?error=<code>`
reported once as a generic alert. An already-authenticated visitor SHALL be
forwarded to the target without seeing the form. Signing out SHALL land
here.

#### Scenario: Login page renders standalone

- **WHEN** an unauthenticated visitor opens `/login`
- **THEN** the sign-in page renders without the explore shell's top bar or
  signal tabs and without a dialog

#### Scenario: Redirect target is honoured

- **WHEN** a visitor signs in from `/login?redirect=%2Ftraces%3Frange%3D15m`
- **THEN** the browser lands on `/traces?range=15m` with the resolved
  tenant and dataset appended

#### Scenario: Unsafe redirect falls back

- **WHEN** `redirect` is `//evil.com`, an absolute URL, or `/\evil.com`
- **THEN** the browser lands on `/logs`

#### Scenario: Already authenticated

- **WHEN** a visitor holding a valid session opens `/login?redirect=%2Ftraces`
- **THEN** they are forwarded to `/traces` without the form being shown

### Requirement: Evaluate navigation group

The navigation sidebar, mobile drawer and command palette SHALL include an
Evaluate group, placed between Investigate and Configure, with the pages
Agents & scores (`/evals`), Compare (`/evals/compare`), Runs
(`/evals/runs`) and Evaluators (`/evals/evaluators`); Eval sets
(`/evals/sets`) joins the group with the `agent-eval-sets` capability. The current page SHALL be the item
with the longest path prefix of the location, so nested Evaluate pages
keep their own item current. Evaluate links SHALL carry the time range and
tenant/dataset context.

#### Scenario: Nested paths highlight their own item

- **WHEN** a user opens `/evals/runs`
- **THEN** "Runs" is the current page in the sidebar, not "Agents & scores"

#### Scenario: The case drilldown belongs to Compare

- **WHEN** a user opens `/evals/compare/case?case=case-117`
- **THEN** "Compare" is the current page in the sidebar

### Requirement: Real users page is reachable from the sidebar

The sidebar's Monitor group SHALL list "Real users" after Catalog, linking
to `/rum` with the explore context (tenant, dataset, range) carried over.
Any `/rum/...` path SHALL highlight it and title the breadcrumb
"Monitor / Real users". An unknown tab segment SHALL resolve to
`/rum/overview`, preserving the query string.

#### Scenario: Sidebar link

- **WHEN** a user on `/logs?tenant=acme&dataset=prod` clicks "Real users"
- **THEN** the browser navigates to `/rum?tenant=acme&dataset=prod` (plus the
  current range) and the entry is highlighted

#### Scenario: Unknown tab

- **WHEN** a user opens `/rum/nope?app=storefront-web`
- **THEN** the URL becomes `/rum/overview?app=storefront-web`

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
