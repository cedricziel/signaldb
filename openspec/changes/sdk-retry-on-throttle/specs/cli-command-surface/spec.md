## ADDED Requirements

### Requirement: Throttled commands retry, then exit distinctly

The CLI SHALL retry a throttled request through the SDK's shared retry policy before failing. When retries are exhausted it SHALL write a diagnostic to stderr stating the command was rate limited and, when the server stated one, how long it asked to wait, and SHALL exit with a dedicated exit code (`4`) distinct from generic failure so scripts can back off and re-run. `--no-retry` (or `SIGNALDB_NO_RETRY=1`) SHALL disable retry for scripting that prefers fail-fast. When stderr is a terminal the CLI SHALL print one short note per retry so an interactive user knows the command is waiting, not hung.

#### Scenario: Throttled command exits with the throttled code

- **WHEN** a command's request is throttled past the retry budget with `Retry-After: 5` on the last response
- **THEN** stderr reads that the command was rate limited and the server asked to retry in 5 seconds, stdout carries no partial result, and the exit code is `4`

#### Scenario: Fail-fast opt-out

- **WHEN** a command is run with `--no-retry` and its first request is throttled
- **THEN** the CLI exits with code `4` immediately without waiting

#### Scenario: Interactive retry is visible

- **WHEN** an interactive command is retried after throttling
- **THEN** stderr shows one line per retry naming the wait, and stdout is untouched
