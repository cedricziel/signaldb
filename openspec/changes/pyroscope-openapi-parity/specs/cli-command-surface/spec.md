## ADDED Requirements

### Requirement: The profile compat surface lives under profiles

The CLI SHALL expose the Pyroscope-compatible profile surface as a
`profiles <verb>` group — `types`, `labels`, `label-values <label>`,
`render <selector> --from --until`, `diff <selector> --left-from --left-until
--right-from --right-until`, and `by-trace <trace_id>` — dispatched through the
SDK and printing the native Pyroscope JSON responses unchanged, consistent with
how the other compat surfaces are surfaced. It lives outside `query` because
Pyroscope has no single query-language flag; the selector and ranges are
per-verb parameters.

#### Scenario: Profile types are listed

- **WHEN** a user runs `signaldb profiles types`
- **THEN** the CLI prints the tenant's profile types with data as the native
  JSON response

#### Scenario: A flame graph is rendered

- **WHEN** a user runs `signaldb profiles render
'process_cpu:cpu:nanoseconds{service_name="checkout"}' --from now-1h`
- **THEN** the CLI prints the native flame-graph JSON returned by the render
  endpoint through the SDK

#### Scenario: Profiles for a trace

- **WHEN** a user runs `signaldb profiles by-trace <trace_id>`
- **THEN** the CLI prints the correlated profiles for that trace
