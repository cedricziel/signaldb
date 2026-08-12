## MODIFIED Requirements

### Requirement: Extensible signal-source model

The IR source SHALL reference a registered signal source rather than a fixed,
hardcoded set. Logs, traces, and profiles SHALL be available as sources in this
capability; the model SHALL allow additional sources (e.g. metrics) to be added
by later changes without altering the IR document shape. A profile source query
SHALL operate on profile-summary rows and SHALL expose only registered scalar
profile metadata and registered resource or scope attributes; it SHALL NOT
expose sample, stacktrace, or attribute JSON payloads as logical fields.

#### Scenario: Logs and traces are queryable sources

- **WHEN** a client selects `logs` or `traces` as the query source
- **THEN** the query executes against that signal

#### Scenario: Profiles is a queryable source

- **WHEN** a client selects `profiles` as the query source
- **THEN** the query executes against that signal

#### Scenario: Profile summary query returns registered metadata

- **WHEN** a client requests profile fields such as `profile.id`, `timestamp`,
  `duration`, `sample.type`, or `service.name`
- **THEN** the result contains the requested typed metadata values without raw
  sample, stacktrace, or attribute JSON payloads

#### Scenario: Profile payload addressing is rejected

- **WHEN** a profile query references a raw sample, stacktrace, or attribute JSON
  storage payload
- **THEN** validation rejects the query as an unregistered logical field

#### Scenario: Adding a source does not reshape the IR

- **WHEN** a later change registers an additional signal source
- **THEN** existing IR documents remain valid and the document shape is unchanged

## ADDED Requirements

### Requirement: Source-specific read authorization

The native Query IR endpoint SHALL authorize a request for a registered signal
source using that signal's read scope before it dispatches the query. A request
for `profiles` SHALL require `profiles:read`; authorization SHALL remain bound
to the authenticated tenant and dataset rather than any client-supplied tenant
or dataset value.

#### Scenario: Profile scope permits profile IR query

- **WHEN** an authenticated request with `profiles:read` submits a profile IR
  document for its tenant and dataset
- **THEN** the endpoint dispatches the query using that authenticated context

#### Scenario: Missing profile scope is rejected

- **WHEN** an authenticated request without `profiles:read` submits a profile IR
  document
- **THEN** the endpoint rejects the request before dispatching it to a querier

#### Scenario: Other source scopes remain isolated

- **WHEN** an authenticated request with only `profiles:read` submits a logs or
  traces IR document
- **THEN** the endpoint rejects the request for lacking that source's read scope
