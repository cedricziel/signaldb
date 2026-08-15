#!/usr/bin/env bash
# Run `weaver registry check` over the sample custom registries in
# src/schema-model/tests/fixtures/weaver/. Those fixtures are single-document
# registries (manifest fields + groups in one file, the shape SignalDB accepts
# on upload); Weaver wants a directory with registry_manifest.yaml + model/,
# so each fixture is split into that layout in a temp dir first. Keeps
# schema-model's validator accept-set a subset of Weaver's: a fixture our
# validator accepts must pass Weaver too.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FIXTURES="$ROOT/src/schema-model/tests/fixtures/weaver"
WEAVER_IMAGE="${WEAVER_IMAGE:-otel/weaver:v0.25.1}"
SEMCONV_VERSION="$(tr -d '[:space:]' < "$ROOT/vendor/otel-semconv/VERSION")"

status=0
for fixture in "$FIXTURES"/*.yaml; do
  name="$(basename "$fixture" .yaml)"
  tmp="$(mktemp -d)"
  mkdir -p "$tmp/model"
  python3 - "$fixture" "$tmp" "$SEMCONV_VERSION" <<'PY'
import sys, yaml
src, out, semconv = sys.argv[1:4]
doc = yaml.safe_load(open(src))
# Same manifest/2.0 shape as otel/registry/manifest.yaml (validated in CI).
manifest = {
    "name": doc["name"],
    "description": doc.get("description", ""),
    "schema_url": doc.get("schema_url", f"https://example.invalid/schemas/{doc['version']}"),
    "dependencies": [
        {
            "schema_url": f"https://opentelemetry.io/schemas/{semconv}",
            "registry_path": f"https://github.com/open-telemetry/semantic-conventions@v{semconv}[model]",
        }
    ],
}
yaml.safe_dump(manifest, open(f"{out}/manifest.yaml", "w"), sort_keys=False)
yaml.safe_dump({"groups": doc.get("groups", [])}, open(f"{out}/model/{doc['name']}.yaml", "w"), sort_keys=False)
PY
  # The weaver image runs unprivileged; mktemp dirs are 0700.
  chmod -R a+rX "$tmp"
  echo "== weaver registry check: $name"
  if ! docker run --rm -v "$tmp:/registry" "$WEAVER_IMAGE" registry check -r /registry; then
    echo "::error::fixture $name rejected by Weaver" >&2
    status=1
  fi
  rm -rf "$tmp"
done
exit $status
