## Purpose

Provides users with guided, source-specific instructions for configuring their applications to send telemetry data (metrics, logs, traces, profiles) to SignalDB.

## ADDED Requirements

### Requirement: Source selector with multiple instrumentation options

The system SHALL display a sidebar with selectable instrumentation sources: OTel SDK, OTel Collector, Kubernetes, Docker, journald, and Prometheus.

#### Scenario: All sources visible

- **WHEN** the instrumentation page loads
- **THEN** all six sources are listed in the sidebar

#### Scenario: Default source selected

- **WHEN** the page loads
- **THEN** "OTel SDK" is selected by default

#### Scenario: Source selection switches content

- **WHEN** the user clicks a different source
- **THEN** the content area updates to show that source's instructions

### Requirement: Source-specific setup instructions

For each source, the system SHALL display step-by-step setup instructions with copyable code/config snippets that include the user's actual tenant and dataset from whoami data.

#### Scenario: Snippets include tenant context

- **WHEN** the user views any source's instructions
- **THEN** code snippets include the user's current tenant ID and dataset ID from the whoami response

#### Scenario: Snippets are copyable

- **WHEN** the user clicks a copy button on a code snippet
- **THEN** the snippet content is copied to the clipboard

#### Scenario: OTel SDK shows environment variables

- **WHEN** OTel SDK is selected
- **THEN** instructions show OTEL_EXPORTER_OTLP_ENDPOINT and OTEL_EXPORTER_OTLP_HEADERS with correct values

#### Scenario: OTel Collector shows YAML config

- **WHEN** OTel Collector is selected
- **THEN** instructions show a YAML exporter configuration

#### Scenario: Prometheus shows remote_write config

- **WHEN** Prometheus is selected
- **THEN** instructions show a remote_write configuration with the SignalDB endpoint

### Requirement: Verification status indicators

The system SHALL display a verification section showing the ingestion status for each signal type (metrics, logs, traces, profiles).

#### Scenario: Initial status shows waiting

- **WHEN** the page first loads
- **THEN** all signal types show "Waiting for data" status

#### Scenario: Status visually distinguishes active signals

- **WHEN** a signal type has received data (future implementation)
- **THEN** it shows "Receiving data" with a green checkmark
