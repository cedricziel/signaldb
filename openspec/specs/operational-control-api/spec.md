# operational-control-api Specification

## Purpose
Exposes operator-facing **compaction control** — trigger, status, and dry-run —
through the router so it is reachable via the SDK and therefore uniformly
available to the CLI and the MCP server. Retention enforcement, snapshot
expiration, and orphan cleanup run as compactor background loops with no control
surface today; exposing them here is future work that requires matching
compactor `do_action` commands, and is intentionally out of scope for this
capability's initial delivery.
## Requirements
### Requirement: Compaction control is reachable through the API

The router SHALL expose compaction control under a dedicated `/api/v1/ops/*`
surface: triggering a compaction pass, reading compaction status (active leases
and metrics), and planning candidates without executing (dry-run). These
operations SHALL forward to the compactor's Flight `do_action` control surface
and return its result.

#### Scenario: Trigger compaction via the API

- **WHEN** an authorized caller requests a compaction run through the ops surface
- **THEN** the router forwards the request to the compactor
- **AND** returns the outcome reported by the compactor

#### Scenario: Read compaction status

- **WHEN** an authorized caller requests compaction status
- **THEN** the router returns the compactor's active leases and metrics

#### Scenario: No compactor is reachable

- **WHEN** an authorized caller requests a compaction operation and no compactor
  service is registered
- **THEN** the router responds with a service-unavailable error rather than
  hanging or succeeding

### Requirement: Operational endpoints are described for SDK generation

The `/api/v1/ops/*` endpoints SHALL be described in the code-first OpenAPI
surface so they generate into `signaldb-sdk`, making operational control a
first-class SDK capability with no hand-written client per consumer.

#### Scenario: Ops appears in the SDK

- **WHEN** the SDK is generated from the annotated router
- **THEN** the SDK exposes methods for the compaction control operations

### Requirement: Compaction dry-run previews without executing

The compaction dry-run operation SHALL report the candidate partitions/files it
would compact without performing any compaction, giving operators a read-only
preview.

#### Scenario: Dry-run reports candidates without acting

- **WHEN** a caller requests a compaction dry-run
- **THEN** the response reports the compaction candidates
- **AND** no compaction is executed

### Requirement: Operational control requires authorization

Operational endpoints SHALL require administrative authorization and SHALL
reject unauthenticated or non-administrative callers.

#### Scenario: Unauthorized ops call is rejected

- **WHEN** a caller without administrative authorization requests an operational
  action
- **THEN** the request is rejected and no action is performed

