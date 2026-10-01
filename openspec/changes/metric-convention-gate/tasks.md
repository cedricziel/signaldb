# Tasks

## 1. Registry declarations (additive, no behavior change)

- [ ] 1.1 Inventory every instrument built in
      `common::self_monitoring::app_metrics` and
      `common::self_monitoring::metrics`, with its kind, unit, and the
      attribute keys its recording sites pass.
- [ ] 1.2 Add a `type: metric` group per instrument to
      `otel/registry/signaldb.yaml`, declaring the intended (post-rename)
      attribute set, alongside the three existing compactor entries.
- [ ] 1.3 Add the new attribute declarations the renames need:
      `signaldb.signal`, `signaldb.wal.record_type`, `signaldb.query.type`,
      `signaldb.ratelimit.surface`, `signaldb.ratelimit.dimension`.
- [ ] 1.4 Verify the registry still passes `weaver registry check`
      (`.github/workflows/weaver-live-check.yml` runs it).

## 2. Attribute vocabulary — tests first

- [ ] 2.1 Failing test (`cargo test -p common`): exported metrics carry no
      attribute named `tenant`, `tenant_id`, `record`, `signal`,
      `query_type`, `surface`, or `kind`, and no data point carries
      `service.name`. Uses an in-memory metric exporter; one test binary,
      per the `OnceLock` instrument binding.
- [ ] 2.2 Failing test (`cargo test -p common`): the WAL and storage-usage
      recording sites emit `signaldb.tenant.id`, `signaldb.signal`, and
      `signaldb.wal.record_type`.
- [ ] 2.3 Rename in `common`: `wal` (`record` → `signaldb.wal.record_type`,
      `signal` → `signaldb.signal`), `storage_usage` (`tenant_id` →
      `signaldb.tenant.id`), `app_metrics::record_rate_limit_rejection`
      (`surface`/`kind` → `signaldb.ratelimit.surface` /
      `signaldb.ratelimit.dimension`).
- [ ] 2.4 Drop the per-point `service.name` from the five observable
      callbacks in `self_monitoring::metrics`; confirm the resource still
      identifies the service.
- [ ] 2.5 Failing test (`cargo test -p writer`), then rename in
      `writer::processor` (`tenant` → `signaldb.tenant.id`, `signal` →
      `signaldb.signal`).
- [ ] 2.6 Failing test (`cargo test -p querier`), then rename in
      `querier::flight` (`query_type` → `signaldb.query.type`).
- [ ] 2.7 Failing test (`cargo test -p acceptor`), then rename in the four
      OTLP services (`tenant_id` → `signaldb.tenant.id`).

## 3. The gate

- [ ] 3.1 Failing test: extend `src/common/tests/registry_pins.rs` with a
      metric extractor — instrument names from the `AppMetrics` constructor
      and attribute keys from `KeyValue::new` sites — asserting each is
      declared in a registry metric group. Prove it fails on a deliberately
      undeclared instrument before the declarations from task 1 are read.
- [ ] 3.2 Pin convention-defined metric names against
      `opentelemetry_semantic_conventions::metric::*` constants, mirroring
      the attribute pins in `self_monitoring::spans`.
- [ ] 3.3 Add the CI guard rejecting instrument construction outside
      `src/common/src/self_monitoring/`, exempting `src/signal-producer/`;
      place it with the existing span-construction guards in
      `.github/workflows/ci.yml` and give it the same style of error message.
- [ ] 3.4 Verify the guard fires: add an instrument outside the module
      locally, confirm CI's grep rejects it, revert.

## 4. Verification

- [ ] 4.1 `cargo test -p common -p writer -p querier -p acceptor -p compactor -p mcp-server`.
- [ ] 4.2 `cargo fmt`, `cargo clippy --workspace --all-targets --all-features`,
      `cargo machete --with-metadata`.
- [ ] 4.3 Run the Weaver live-check workflow against a local deployment and
      confirm no metric attribute is reported as unregistered.

## 5. Documentation

- [ ] 5.1 Add a self-monitoring metrics reference under `docs/operations/`
      (route per the docs skill), listing the declared inventory and the
      cardinality rule, and cross-linking `self-monitoring-traces`.
- [ ] 5.2 Include an old → new label mapping table for the renamed
      attributes so operator dashboards can be updated mechanically.
- [ ] 5.3 Update any skill whose described behavior changed (the
      instrumentation/self-monitoring guidance naming metric attributes).
- [ ] 5.4 Run the docs-freshness gate after committing, and again after any
      follow-up fix.
