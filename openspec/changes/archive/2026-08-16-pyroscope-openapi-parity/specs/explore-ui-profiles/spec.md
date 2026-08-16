## ADDED Requirements

### Requirement: Pyroscope-compat calls go through the generated client

Wherever the Explore UI calls the Pyroscope-compatible endpoints (profile
types, label names/values, render, render-diff, profiles for a trace) it SHALL
do so through the generated TypeScript client, not a hand-written fetch, so the
UI, CLI, and MCP consume one contract.

#### Scenario: Profile types load through the generated client

- **WHEN** the profiles view loads the available profile types
- **THEN** the request is issued by the generated client's Pyroscope operation
  and the hand-written raw-fetch module for Pyroscope no longer exists
