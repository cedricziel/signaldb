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

#### Scenario: A transport opts out inline

- **WHEN** a `fetch()` call is the network transport itself rather than a
  SignalDB call site (the generated client's fetch, the service worker's
  app shell)
- **THEN** it disables the check on that line with a stated reason
- **AND** no file or directory outside `src/api/gen/**` is exempted wholesale
