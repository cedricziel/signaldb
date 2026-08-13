## 1. Query IR Source Contract

- [x] 1.1 Add failing common crate tests for profile source registration,
  supported metadata fields, payload-field rejection, and profile relation/stage
  validation.
- [x] 1.2 Register the typed `profiles` Query IR source and its logical
  profile-summary field mappings in common.
- [x] 1.3 Run `cargo test -p common query_ir`.

## 2. Profile Planning and Authorization

- [x] 2.1 Add failing querier tests that execute profile summary rows, grouped
  tables, and time series through Query IR against tenant-scoped profile data.
- [x] 2.2 Extend the Query IR planner to lower profile source fields and query
  the existing tenant/dataset profiles Iceberg table.
- [x] 2.3 Run `cargo test -p querier query_ir`.
- [x] 2.4 Add failing router tests for allowed `profiles:read` requests and
  rejected profile, logs, and traces requests lacking their respective scopes.
- [x] 2.5 Enforce source-specific read scopes before native Query IR Flight
  dispatch while retaining authenticated tenant/dataset derivation.
- [x] 2.6 Run `cargo test -p router query_ir`.

## 3. Client Surfaces

- [x] 3.1 Update the native Query IR OpenAPI documentation and regenerate or
  verify the Rust SDK and TypeScript generated clients.
- [x] 3.2 Add failing UI tests for selecting `profiles`, emitting a profile IR
  document, and rendering its generic envelopes.
- [x] 3.3 Add `profiles` to the Explore Query IR source selector and profile
  grouping defaults using the generated client path.
- [x] 3.4 Verify CLI and MCP generic Query IR commands accept and forward a
  `profiles` source without a dedicated profile transport.
- [x] 3.5 Run `pnpm --filter signaldb-ui test`.

## 4. Integration and Documentation

- [x] 4.1 Add integration coverage proving profile IR results are tenant/dataset
  isolated and require `profiles:read`.
- [x] 4.2 Run the targeted integration test in `tests-integration`.
- [x] 4.3 Document profile Query IR field and capability boundaries, including
  the retained Pyroscope-specific operations.
- [x] 4.4 Update the relevant SignalDB query/architecture skill reference.
- [x] 4.5 Run `openspec validate query-ir-profiles --strict` and the project
  formatting and lint checks.
