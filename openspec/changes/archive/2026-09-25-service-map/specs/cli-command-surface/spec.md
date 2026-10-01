## ADDED Requirements

### Requirement: Service map command

The CLI SHALL provide `signaldb-cli services map` with optional `--service`, `--depth`, `--trace-id`, time-range and dataset flags, backed by the Query IR `graph` result. `--format` SHALL accept `table` (default: one row per edge with source, target, call rate, error rate and p95), `json` (the graph result unchanged), `dot` (Graphviz) and `mermaid`. An empty graph SHALL print nothing to stdout, a note on stderr, and exit 0.

#### Scenario: Default table

- **WHEN** a user runs `signaldb-cli services map --service orders`
- **THEN** stdout shows one row per edge touching `orders`

#### Scenario: Graphviz export

- **WHEN** a user runs `signaldb-cli services map --format dot`
- **THEN** stdout is a valid Graphviz digraph that `dot -Tsvg` renders
