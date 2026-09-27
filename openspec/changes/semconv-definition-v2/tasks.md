# Tasks

## 1. Parse definition/2 in schema-model

- [x] 1.1 Add real upstream `definition/2` files (a registry, spans, metrics and events file from `semantic-conventions-genai`) plus hand-written v1 equivalents as fixtures under `src/schema-model/tests/fixtures/`, and a failing conformance test asserting both resolve to identical attributes, entities, metrics, spans and events. Verify it fails with `cargo test -p schema-model`
- [x] 1.2 Implement `file_format` dispatch and lowering of `definition/2` into `Group`s in `src/schema-model/src/model.rs`. Verify with `cargo test -p schema-model`
- [x] 1.3 Add failing tests where an unknown `file_format` and a non-manifest file without `groups` each produce a `ParseError` naming the file, then make them pass. Verify with `cargo test -p schema-model`
- [x] 1.4 Run Weaver ≥ v0.26 over `definition/2` content in CI. Covered by the `otel/registry-genai` check (task 4.4), which loads the vendored GenAI model as a dependency. `scripts/weaver-check-fixtures.sh` stays on v1 single-document upload fixtures. Verify the CI step passes locally

## 2. Custom registry uploads

- [x] 2.1 Add a failing test in `src/common/tests/schema_registry.rs`: a `definition/2` YAML upload creates a registry whose definitions resolve like the `groups` equivalent, and reading it back returns `groups`. Also test that an unsupported `file_format` upload is rejected and stores nothing. Verify with `cargo test -p common --test schema_registry`
- [x] 2.2 Route YAML uploads through the new parser in the router's schema-registry endpoint. Verify with the 2.1 tests and `cargo test -p router schema`
- [x] 2.3 Update the upload request description in the OpenAPI spec, regenerate `src/signaldb-sdk` and the UI client in `src/ui/src/api/gen`. Verify the diff is description-only and `pnpm --filter ./src/ui typecheck` passes
- [x] 2.4 Verify `signaldb schema` upload in the CLI accepts a `definition/2` file (add or extend a CLI test)
- [x] 2.5 Document `definition/2` uploads in `docs/users/schema-registry.md` (route via the docs skill). Verify the example uploads cleanly against a local dev server

## 3. Core semconv pin to 1.44.0 (optional; only if GenAI refs something 1.43.0 lacks)

- [ ] 3.1 Bump `common::self_monitoring::SEMCONV_SCHEMA_URL` and the `otel/registry/manifest.yaml` dependency to 1.44.0, run `cargo xtask vendor-semconv`, and fix what the bump breaks. Verify with `cargo test -p common` and the CI `weaver registry check`

## 4. Bundle otel-genai at a pinned commit

- [x] 4.1 Add `cargo xtask vendor-semconv-genai <sha>` that clones `semantic-conventions-genai` at a pinned commit into `vendor/otel-semconv-genai/<sha>/` with `VERSION`, `LICENSE` and a README, and vendor `e57c543`. Verify by running it and checking the tree
- [x] 4.2 Add failing tests in `src/common/tests/schema_registry.rs`: bundled registries list `otel`, `otel-genai` and `signaldb`; `otel-genai` rejects mutation; `gen_ai.agent.id` resolves primary `otel-genai` (not deprecated) with `otel` as an alternative; a custom registry named `otel-genai` is rejected. Verify with `cargo test -p common --test schema_registry`
- [x] 4.3 Bundle `otel-genai` in `src/common/build.rs` (resolved against `otel`), insert it into the precedence order and the reserved-namespace list in `common::schema_registry`, and fold `otel/registry-genai/` into the bundled `signaldb` registry (resolved against `otel` and `otel-genai`). Verify with the 4.2 tests
- [x] 4.4 Create `otel/registry-genai/` (manifest depending on GenAI at the pinned commit) and add a Weaver v0.26.1 `registry check` step for it in `.github/workflows/ci.yml`. Verify by running the same command locally
- [x] 4.5 Update `docs/users/schema-registry.md` and the `multi-tenancy`/`configuration` skills only if they list bundled or reserved namespaces (route via the docs skill). Verify with a grep for `otel` and `signaldb` namespace lists

## 5. Integration check

- [ ] 5.1 Against `./scripts/run-dev.sh`, verify registries list, `resolve_attribute gen_ai.agent.id` and a `definition/2` upload through the HTTP API, CLI, MCP tools and the Explore UI schema view
