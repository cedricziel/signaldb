Three PRs, in order. Each is independently revertable and leaves the catalog
working. TDD throughout: the failing test comes first, and Vitest covers the
pure logic (identity-tuple resolution, presence intersection, overlay merge)
directly rather than through the components.

## 1. Split detection from measurement (PR 1)

Keeps the existing fixed entity-type list. No registry, no new sources. The
user-visible change is RED honesty; everything else is internal shape.

- [x] 1.1 Test: a group whose observations come only from a non-trace source
      reports its error rate and percentiles as unavailable, not `0`/`0ms`
- [x] 1.2 Introduce an explicit "measurement unavailable" state in the catalog
      row model, distinct from a zero measurement
- [x] 1.3 Split `buildEntitySourceDoc` into an instance-listing aggregate
      (identity tuple + last-seen) and a trace-only RED aggregate
- [x] 1.4 Test: merging sources does not sum volume from different signals into
      one figure presented as request volume
- [x] 1.5 Render unavailable measurements as such in `CatalogView` and
      `EntityDetail`; drop the summed-count column from the list view
- [ ] 1.6 Verify against hive: a service observed in traces still shows p50/p95;
      one observed only outside traces shows neither, and shows no fabricated
      zero

## 2. Registry-derived entity types (PR 2)

- [ ] 2.1 Test: entity types are built from a registry response, keeping only
      definitions with ≥1 identifying attribute, primary = first declared
- [ ] 2.2 Add a typed client call for `GET /api/v1/schema/entities` alongside the
      existing schema-feature API module
- [ ] 2.3 Build the entity-type model from the registry response, replacing the
      hand-written `ENTITY_TYPES` array
- [ ] 2.4 Test: an entity type with no presentation-overlay entry is still
      listed and is labelled from its registry name
- [ ] 2.5 Add the presentation overlay (label, plural, ordering, `breakdown`,
      `topValues`, `spanKindScope`) keyed by registry entity name, covering at
      minimum the eight types the current list covers so no existing detail-page
      behavior regresses
- [ ] 2.6 Test: identity ordering and `spanKindScope` still produce the same
      service query as before this change (regression guard on the one entity
      type with a kind scope)
- [ ] 2.7 Verify against hive: the eight previously-listed types still render
      with unchanged labels, columns, and drill-down URLs

## 3. All-signal detection (PR 3)

- [ ] 3.1 Test: an entity type is present in a source when its primary
      identifying attribute is present there, and absent when it is not
- [ ] 3.2 Add a per-source field-descriptor fetch using
      `describe { target: "fields" }` (irVersion 4), one call per source
- [ ] 3.3 Implement tier-1 presence: intersect each source's field set with each
      entity type's identifying attributes, across every queryable source
- [ ] 3.4 Test: identifying attributes absent from a source are dropped from the
      tuple; when the primary is absent the source contributes no instances
- [ ] 3.5 Apply per-source identity degradation to the tier-2 listing queries
- [ ] 3.6 Test: an entity type whose identity attributes have no covering
      statistics is reported unanalyzed, distinctly from analyzed-and-absent
- [ ] 3.7 Add the unanalyzed state and surface the statistics `as_of` stamp in
      the UI
- [ ] 3.8 Drive the nav from the observed set rather than the full registry list
- [ ] 3.9 Verify against hive: Processes lists `process.pid` from metrics
      (currently empty), Services includes `otelcol-contrib` (currently absent),
      and containers / service instances appear

## 4. Ship

- [ ] 4.1 `pnpm -C src/ui test` and `pnpm -C src/ui build` clean on each PR
- [ ] 4.2 Lint/format per the project, then `/simplify` on the changed code
- [ ] 4.3 Docs: update the catalog's user-facing documentation to describe
      registry-derived types and all-signal detection; run the docs-freshness
      gate after committing
- [ ] 4.4 Open the three PRs as a stack, each under the 500-line guideline, and
      act on automated review findings
- [ ] 4.5 After merge, sync the `explore-ui-catalog` delta into the main spec and
      archive the change
