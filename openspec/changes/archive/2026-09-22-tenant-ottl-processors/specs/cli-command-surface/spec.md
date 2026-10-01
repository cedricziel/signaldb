## MODIFIED Requirements

### Requirement: Command taxonomy covers every user-facing capability

The CLI SHALL additionally expose a `processors` group (`list`, `get`,
`validate`, `test`) backed by the tenant credential, and an `admin processors`
group (`create`, `replace`, `delete`) that uses a tenant key holding
`processors:write`. Processor specifications SHALL be read from a JSON or
YAML file or from flags (`--signal`, `--dataset`, `--statement` repeatable,
`--priority`, `--error-mode`, `--disabled`). All calls SHALL go through the
generated SDK.

#### Scenario: Validate from the CLI

- **WHEN** `signaldb-cli processors validate --signal logs --statement
  'set(attributes["a"], 1)'` is run
- **THEN** the CLI prints the validation result and exits non-zero on errors

#### Scenario: Create from a file

- **WHEN** `signaldb-cli admin processors create -f redact.yaml` is run with a
  key holding `processors:write`
- **THEN** the processor is created and its summary printed
