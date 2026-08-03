## Purpose

Exposes operator-facing lifecycle control — compaction, retention, snapshot
expiration, orphan cleanup, and status — through the router so it is reachable
via the SDK and therefore uniformly available to the CLI and the MCP server.

## ADDED Requirements

### Requirement: Operational control is reachable through the API

The router SHALL expose operational control operations under a dedicated
`/api/v1/ops/*` surface: triggering compaction, triggering and inspecting
retention enforcement, expiring snapshots, running orphan cleanup, and reading
operational status/health. These operations SHALL forward to the compactor's
existing control surface.

#### Scenario: Trigger compaction via the API

- **WHEN** an authorized caller requests a compaction run through the ops surface
- **THEN** the router forwards the request to the compactor
- **AND** returns the outcome reported by the compactor

#### Scenario: Read operational status

- **WHEN** an authorized caller requests operational status
- **THEN** the router returns the compactor's current status

### Requirement: Operational endpoints are described for SDK generation

The `/api/v1/ops/*` endpoints SHALL be described in the code-first OpenAPI
surface so they generate into `signaldb-sdk`, making operational control a
first-class SDK capability with no hand-written client per consumer.

#### Scenario: Ops appears in the SDK

- **WHEN** the SDK is generated from the annotated router
- **THEN** the SDK exposes methods for the operational control operations

### Requirement: Destructive operations support dry-run

Operational actions that delete or expire data — retention enforcement, snapshot
expiration, and orphan cleanup — SHALL support a dry-run mode that reports what
would be affected without performing deletion, consistent with the compactor's
existing dry-run semantics.

#### Scenario: Retention dry-run reports without deleting

- **WHEN** a caller requests retention enforcement in dry-run mode
- **THEN** the response reports the partitions or files that would be removed
- **AND** no data is deleted

### Requirement: Operational control requires authorization

Operational endpoints SHALL require administrative authorization and SHALL
reject unauthenticated or non-administrative callers.

#### Scenario: Unauthorized ops call is rejected

- **WHEN** a caller without administrative authorization requests an operational
  action
- **THEN** the request is rejected and no action is performed
