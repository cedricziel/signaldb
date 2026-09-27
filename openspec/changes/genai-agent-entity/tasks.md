# Tasks

## 1. Registry entity

- [ ] 1.1 Add a failing test in `src/common/tests/schema_registry.rs`: resolving entity `gen_ai.agent` for a fresh tenant returns one `signaldb` bundled hit with `gen_ai.agent.id` identifying and name/description/version descriptive, and resolving attribute `gen_ai.agent.id` lists `gen_ai.agent` with role `identifying`. Verify it fails with `cargo test -p common --test schema_registry`
- [ ] 1.2 Add the `entity.gen_ai.agent` group to `otel/registry/signaldb.yaml` (refs to the four `otel` attributes, brief stating the hosted-agent meaning of `gen_ai.agent.id`). Verify with `cargo test -p common --test schema_registry`
- [ ] 1.3 Verify `weaver registry check` over `otel/registry/` passes with the same image and invocation as `.github/workflows/ci.yml`
- [ ] 1.4 Add a test that a tenant registry extending `gen_ai.agent` validates and appears in `extended_by`. Verify with `cargo test -p common --test schema_registry`

## 2. Docs and skills

- [ ] 2.1 Document the `gen_ai.agent` entity and the deprecated-attribute caveat in `docs/users/schema-registry.md` (route via the docs skill). Verify the `signaldb schema entity get gen_ai.agent` example in the doc runs as written against a local dev server
- [ ] 2.2 Mention the entity in `docs/users/mcp.md` where `resolve_entity` is described, if that section lists example entities. Verify with a read-through of the rendered doc

## 3. Integration check

- [ ] 3.1 Against `./scripts/run-dev.sh`, verify `resolve_entity gen_ai.agent` works through the HTTP API, `signaldb schema entity get`, the MCP tool, and the Explore UI schema view. No code changes are expected on these surfaces
