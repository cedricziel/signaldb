## 1. Query IR v2 Contract

- [x] 1.1 Add failing `common` tests for IR v2 heatmap parsing, v1 rejection,
  typed duration-bound validation, relation inference, and envelope matching.
- [x] 1.2 Add the v2 heatmap stage, relation metadata, result envelope, and
  explicit version gating in `common::query_ir`.
- [x] 1.3 Update native Query IR OpenAPI schemas and regenerate the Rust SDK and
  TypeScript client; verify v1 generated-client compatibility remains intact.

## 2. Server-Side Execution

- [x] 2.1 Add failing `querier` tests for epoch-aligned time buckets,
  lower-inclusive/upper-exclusive duration bounds, overflow bins, pruning, and
  tenant/dataset isolation.
- [x] 2.2 Lower the terminal heatmap relation into a DataFusion time-by-duration
  count aggregate over traces, using DataFusion Arrow reexports and existing
  timestamp partition pruning.
- [x] 2.3 Extend the existing Query IR Flight result path to encode and decode
  sparse heatmap axis metadata and cells without adding a dedicated ticket.
- [x] 2.4 Add router tests for authenticated native-query heatmap submission,
  validation failures, tenant scoping, and canonical HTTP response shaping.
- [x] 2.5 Add cross-service integration coverage in `tests-integration` proving
  duration-bound counts, list-limit independence, and tenant/dataset isolation.

## 3. Client Surfaces

- [x] 3.1 Add CLI coverage proving the existing generic Query IR command submits
  and displays a v2 heatmap envelope; no dedicated CLI command is introduced.
- [x] 3.2 Add MCP coverage proving the existing generic native-query tool accepts
  the v2 document; no dedicated MCP tool is introduced.
- [x] 3.3 Replace the trace UI average-latency heatmap with a generated-client
  Query IR v2 heatmap request and a time-by-duration count grid.
- [x] 3.4 Add UI tests for axes, sparse empty cells, count intensity,
  accessibility, and independence from the trace list limit.

## 4. Documentation And Validation

- [x] 4.1 Update `docs/users/querying-ir.md` with the v2 heatmap grammar,
  response envelope, axis/bound semantics, and limits.
- [x] 4.2 Update the Query IR architecture documentation or skill if source
  changes make its current guidance incomplete.
- [x] 4.3 Remove the uncommitted dedicated latency-heatmap endpoint spike and
  generated contracts before merge.
- [x] 4.4 Run `cargo fmt`, `cargo clippy --workspace --all-targets --all-features`,
  targeted crate tests, integration tests, generated-client checks, UI tests,
  and the UI production build.
