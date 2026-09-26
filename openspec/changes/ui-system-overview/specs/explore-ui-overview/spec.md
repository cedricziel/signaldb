## ADDED Requirements

### Requirement: System overview page

The explore UI SHALL serve a tenant-wide overview at `/overview` showing, for
the selected window: request rate, error rate and p95 latency over root
spans with the change against the previous equal-length window; the number
of services reporting and their health; the service graph; per-service rate,
error rate, p95 and last deploy; records ingested per signal; the top error
groups; and the slowest endpoints by p95. Every row SHALL link into the
existing view that explains it.

#### Scenario: Services sort by health

- **WHEN** one service has a 2.1% error rate and another a 0.1% error rate
  with a p95 under 500 ms
- **THEN** the first is listed first and marked critical, the second marked
  healthy

#### Scenario: A service links to its catalog entry

- **WHEN** a user clicks a service's name in the services table
- **THEN** the browser navigates to `/catalog/service/{identity}` carrying the
  tenant context

### Requirement: Environment scope

The overview SHALL offer the `deployment.environment.name` values seen on
spans in the window and, when one is picked, scope every query on the page
to it and record it in the URL as `env`.

#### Scenario: Picking an environment

- **WHEN** a user picks `staging`
- **THEN** the URL gains `env=staging` and the KPI, services, map, ingest,
  error and endpoint queries are filtered on
  `deployment.environment.name = staging`

### Requirement: Deploys inferred from service versions

The overview SHALL mark a deploy when a service reports a `service.version`
whose first span in the window arrives after another version of that service
was already reporting, and SHALL show deploys on a timeline and as markers on
the KPI sparklines.

#### Scenario: A version change in the window

- **WHEN** `checkout` reports `v1` from the start of the window and `v2` from
  16 minutes before its end
- **THEN** the deploys lane shows `checkout v2` and the services table shows
  `v2 · 16m ago` for checkout

### Requirement: Setup checklist

The overview SHALL offer a setup checklist listing traces, full service
coverage, logs, profiling, GitHub source links and (for admins) team
membership, with a call to action for each open step; `/overview?setup`
SHALL open it on load.

#### Scenario: Deep link opens the checklist

- **WHEN** a user opens `/overview?setup`
- **THEN** the "Setup checklist" dialog is shown and `setup` is removed from
  the URL
