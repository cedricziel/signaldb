# Tasks

> **Sketch: this change is not scheduled for implementation.** It records the
> PR stack design.md's Migration Plan implies, so the plan can be reviewed.
> Each group is one PR under 500 changed lines that leaves `main` deployable.
> Groups land in order: reader before writer before acceptor.

## 1. Residue v2 reader (common, querier): nothing emits it yet

- [ ] 1.1 Write failing tests in `common::attrs::typed`: decode a
      `Tag(55801, [[k, v], …])` document; map-shaped access is last-wins;
      the list access returns every occurrence in order; the existing map
      form decodes unchanged; any other top-level shape is a corrupt-residue
      error. Verify with `cargo test -p common attrs::typed`
- [ ] 1.2 Implement the v2 decode path and the list accessor. 1.1 passes.
      Add a querier test that the raw bag over a v2 residue is last-wins.
      Verify with `cargo test --profile ci-test -p querier typed_attrs`

## 2. Wire v3 schemas, protobuf extraction, down-converter (common): unused

- [ ] 2.1 Write failing round-trip tests per signal: OTLP request → v3 batch →
      decode `*_pb` with `prost` → equals the original containers
      (duplicates, NaN, bytes-shaped kvlist, nesting at exactly the depth
      bound, absent vs empty) and an over-bound value rejected by the
      wire-byte depth scan without recursion; v3 → v1 down-conversion equals today's v1 conversion of the
      same request. Verify with `cargo test -p common flight::conversion`
- [ ] 2.2 Add the v3 Flight schemas (`flight/schema.rs`), the protobuf
      container extraction, and `downconvert_v3_to_v1`. 2.1 passes
- [ ] 2.3 Add the iterative protobuf wire-byte depth scan and
      `[acceptor].max_value_depth`; apply it to the acceptor's OTLP decode
      (pre-existing exposure: `prost` is built with `no-recursion-limit`)
      and to the writer's `*_pb` decode. Can ship ahead of v3

## 3. Writer accepts v3 and advertises `TypedWire` (writer, common): acceptors still send v1

- [ ] 3.1 Write failing writer tests: a v3 `do_put` is transformed (`*_pb`
      kept as `Binary`), written to the WAL, and committed with values placed
      from `AnyValue`s (NaN to the double home; a duplicated key's last
      occurrence in the home); version/column mismatch → `invalid_argument`
      naming both; v1 batches and legacy writer-WAL entries still commit
      unchanged (golden test against today's output).
      Verify with `cargo test --profile ci-test -p writer`
- [ ] 3.2 Implement the v3 transform plan, schema-detected commit dispatch,
      and `ServiceCapability::TypedWire` registration. 3.1 passes

## 4. Writer emits residue v2 for duplicate keys (writer)

- [ ] 4.1 Write failing tests: a container with a duplicate key writes the
      tagged form with every occurrence; a container without one is
      byte-identical to today's residue; a compactor rewrite preserves both
      forms byte-for-byte. Verify with
      `cargo test --profile ci-test -p writer -p compactor residue`
- [ ] 4.2 Implement. 4.1 passes

## 5. Acceptor negotiation and down-conversion (acceptor): default `json`

- [ ] 5.1 Write failing tests: `wire_format` `auto` picks v3 only when all
      Storage writers advertise `TypedWire`; `json`/`typed` pin the carrier;
      forwarding a v3 entry to a writer without the capability down-converts
      it, delivers it, and increments the metric; under `typed`, an
      incapable writer leaves the entry queued.
      Verify with `cargo test -p acceptor`
- [ ] 5.2 Implement for OTLP traces/logs/metrics/profiles and Prometheus
      remote-write. Add the `[acceptor].wire_format` config and dist TOML
      entry, defaulting to `json` in this PR. 5.1 passes
- [ ] 5.3 `tests-integration`: a mixed-version matrix (old/new acceptor ×
      old/new writer, simulated by capability and version), WAL replay across
      an upgrade, and a drain-then-downgrade run.
      Verify with `cargo test --profile ci-test -p tests-integration wire_format`

## 6. Turn it on; query-side exposure; docs (all surfaces)

- [ ] 6.1 Flip the default to `auto`. Add the
      `signaldb.acceptor.wire_format` gate metric
- [ ] 6.2 Write failing querier/router tests, then add the retrieval-only
      `{scope}.attribute_list` field and the non-finite-double and kvlist
      carriers under the next IR version; earlier versions render as today.
      Update the OpenAPI field docs and regenerate `signaldb-sdk` and the TS
      client. Make the UI attribute tables render the carriers (vitest). The
      CLI prints JSON as-is, so it needs no change
- [ ] 6.3 Docs and skills (route via the `docs` skill): `flight-schemas`
      (the wire axis gains v3), `storage-layout` (residue forms, WAL payload
      versions), an operations upgrade/rollback page (order,
      drain-before-downgrade, `wire_format`), and `docs/users/querying-ir.md`
      (`attribute_list`, carriers). Mark the Roadmap's "typed wire and WAL
      fidelity" entry as landed

## 7. Retire wire v1 (a later release, BREAKING)

- [ ] 7.1 Writers reject wire v1 and the legacy writer-WAL carrier. Add a
      startup check that reports legacy entries and leaves them unprocessed.
      Remove `extract_value`'s JSON path once no caller remains
