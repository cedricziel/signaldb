## 1. Registry type and source-agnostic descriptors (`common`)

- [ ] 1.1 Write a failing unit test (`cargo test -p common`) asserting that,
      given a `Catalog` seeded with one `source="config"` tenant and one
      `source="database"` tenant, the registry enumeration returns **both**,
      each with a resolved tenant slug, dataset slugs, default dataset, and an
      effective storage DSN.
- [ ] 1.2 Write a failing unit test asserting slug/storage resolution parity:
      a database-sourced dataset with no override resolves to the same
      tenant/dataset slug (via the existing `get_tenant_slug`/`get_dataset_slug`)
      and the same global-default storage DSN that the write path would use.
- [ ] 1.3 Write a failing unit test asserting config overlay precedence: a
      config tenant with an explicit slug / per-dataset `storage` override and a
      `schema_config.enabled = false` flag is resolved with its explicit values
      and excluded when disabled.
- [ ] 1.4 Introduce the registry (a `TenantRegistry { catalog: Arc<Catalog>,
config: Configuration }` in `common`, or an equivalent method on
      `CatalogManager`) returning source-agnostic descriptors
      `{ tenant_id, tenant_slug, datasets: [{ id, slug, storage_dsn,
is_default }], default_dataset, enabled }`. Make 1.1–1.3 pass.
- [ ] 1.5 Provide a config-only fallback path for contexts with no DB `Catalog`
      (in-memory/unit), preserving today's behavior; add a test that the
      fallback matches the current `get_enabled_tenants()` output for a
      config-only deployment.

## 2. Wire the DB catalog into the enumeration (`common`)

- [ ] 2.1 Give `CatalogManager` (or the registry) access to `Arc<Catalog>`;
      thread the existing auth-side `Catalog` handle through construction in
      `common` and update `CatalogManager::new`/`new_in_memory` call sites.
- [ ] 2.2 Deprecate/replace `get_enabled_tenants()` with the registry query;
      keep a thin shim only if needed for the fallback, and update its doc
      comment to state the registry is the source of truth.

## 3. Querier registration through the registry (`querier`)

- [ ] 3.1 Write a failing test (`cargo test -p querier`) that builds
      `QuerierFlightService::new_with_catalog_manager` against a registry
      containing a `source="database"` tenant and asserts a DataFusion catalog
      **and** object store are registered for that tenant/dataset (today only
      config tenants are).
- [ ] 3.2 Change `new_with_catalog_manager` (flight.rs) to iterate registry
      descriptors instead of `get_enabled_tenants()` for both object-store
      registration and per-slug catalog registration; keep the `registered_urls`
      dedup. Make 3.1 pass.
- [ ] 3.3 Ensure the querier bootstrap (standalone binary + monolithic) supplies
      the DB `Catalog` to the registry, failing fast with `anyhow::Context` if
      it is required but unavailable.

## 4. Lazy on-demand catalog registration in the querier (`querier`)

- [ ] 4.1 Write a failing test (`cargo test -p querier`) that constructs a
      querier with an **empty** startup registry, then adds a `source="database"`
      tenant to the registry after construction, and asserts an authenticated
      query for that tenant resolves the catalog (registered on demand) instead
      of failing with `failed to resolve catalog` — with no rebuild/restart of
      the service.
- [ ] 4.2 Write a failing concurrency test asserting that N simultaneous
      first-queries for the same not-yet-registered tenant register its catalog
      exactly once (no duplicate/object-store re-register panic).
- [ ] 4.3 Add an on-demand registration step on the querier's authenticated
      query path: when the resolved tenant/dataset has no registered DataFusion
      catalog/object store in the `SessionContext`, resolve it from the registry
      and register it idempotently (guarded check-then-insert) before executing.
      Make 4.1–4.2 pass.

## 5. Compactor lifecycle through the registry (`compactor`)

- [ ] 5.1 Write a failing test (`cargo test -p compactor`) asserting the planner
      considers a `source="database"` tenant's datasets as compaction candidates.
- [ ] 5.2 Route `CompactionPlanner::plan` (planner.rs) and the retention /
      orphan-cleanup loops (main.rs:468/514/527) through the registry. Make 5.1
      pass; add a retention test that a database tenant's over-age data is
      selected under the resolved policy.

## 6. Cross-service integration coverage (`tests-integration`)

- [ ] 6.1 Write a failing integration test: create a tenant purely via the admin
      API (no config block), ingest a trace for it, and assert — against the
      **already-running** querier, with no restart — that the Tempo/LogQL/
      Prometheus query paths resolve the catalog on demand and return the data
      instead of `failed to resolve catalog`.
- [ ] 6.2 Add an assertion that read and write namespaces match (the ingested
      data is the data returned), guarding against slug divergence.
- [ ] 6.3 Make 6.1–6.2 pass end to end.

## 7. Docs and provisioning guidance

- [ ] 7.1 Update the multi-tenancy / admin docs to state that admin-API tenants
      are usable for ingest and query immediately on creation — no
      `signaldb.toml` edit and no restart — and remove the "add a
      `[[auth.tenants]]` block or queries 500" requirement (keep it noted only as
      the historical pre-fix workaround).
- [ ] 7.2 Update the `signaldb-observe` skill's Step 2 gotcha and troubleshooting
      row (`failed to resolve catalog`) to reflect the fixed behavior.

## 8. Pre-commit gates

- [ ] 8.1 `cargo fmt`, `cargo clippy --workspace --all-targets --all-features`,
      `cargo machete --with-metadata`; run `openspec validate
unified-tenant-catalog-registry --strict` and the affected `cargo test -p`
      suites green.
