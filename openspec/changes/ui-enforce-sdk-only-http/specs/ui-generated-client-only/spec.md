## ADDED Requirements

### Requirement: Generated-client-only is enforced automatically

An automated lint check SHALL fail when application code under `src/ui/src`
(excluding the generated client at `src/api/gen/**`) contains a direct
`fetch()` call, so the requirement that the UI reach SignalDB exclusively
through the generated client cannot regress silently.

#### Scenario: A raw fetch call is introduced

- **WHEN** a contributor adds a direct `fetch()` call in a file under
  `src/ui/src` outside `src/api/gen/**`
- **THEN** `pnpm --filter signaldb-ui lint` fails and identifies the
  offending call

#### Scenario: Generated client code is exempt

- **WHEN** the lint check runs against `src/api/gen/**`
- **THEN** it does not flag the generated client's own `fetch()` usage
