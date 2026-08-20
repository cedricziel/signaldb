#!/usr/bin/env bash
# Assert that the leaf crates stayed leaves.
#
# `cargo publish --dry-run` does NOT do this, which is the whole reason this
# script exists. It checks that a crate *can* be published — metadata,
# packaging, no bare path dependency — and is perfectly happy with
# `datafusion = "54"`, a real and entirely publishable crate. Verified: adding
# it to src/traceql/Cargo.toml still reports "Packaged 9 files" and exits 0.
#
# Purity is what makes "answer whether a query is valid without linking the
# query engine" true, so it needs an assertion of its own.
#
# These crates may depend only on registry packages that are neither part of
# this workspace nor part of the query engine's stack.
set -euo pipefail

# The compatibility parsers, plus the query IR: all three exist so that a
# query can be parsed, built, or validated without the query engine. That is
# only true while none of them reaches into the workspace or the FDAP stack.
CRATES=("logql-parser" "traceql-parser" "query-ir")

exec python3 - "${CRATES[@]}" <<'PY'
import json
import re
import subprocess
import sys

targets = sys.argv[1:]

# The FDAP stack, including its satellite crates: `datafusion-common` and
# `parquet_derive` are as much the query engine as `datafusion` itself, and
# both are independently publishable — so nothing else would catch them.
FORBIDDEN = re.compile(
    r"^(datafusion|datafusion[-_].*|arrow|arrow[-_].*|parquet|parquet[-_].*"
    r"|iceberg.*|object_store)$"
)

# One resolution answers every question below, for every crate.
probe = subprocess.run(
    ["cargo", "metadata", "--format-version", "1"],
    capture_output=True, text=True,
)
if probe.returncode != 0:
    # Usually an unresolvable dependency someone just added. Say so plainly
    # rather than raising a traceback out of a CI step.
    print("cargo metadata failed, so purity could not be checked:", file=sys.stderr)
    print(probe.stderr.strip(), file=sys.stderr)
    sys.exit(1)
meta = json.loads(probe.stdout)
members = {
    p["name"]
    for p in meta["packages"]
    if p["id"] in meta["workspace_members"]
}

failures = []
for target in targets:
    pkg = next((p for p in meta["packages"] if p["name"] == target), None)
    if pkg is None:
        failures.append(f"{target}: not found in workspace metadata")
        continue

    bad = []
    for dep in pkg["dependencies"]:
        # dev-dependencies are test-only and never ship to a consumer.
        # build-dependencies DO get compiled, so they stay in scope.
        if dep.get("kind") == "dev":
            continue
        name, source = dep["name"], dep.get("source")
        if source is None:
            bad.append(f"'{name}' by path — a published parser must not")
        elif source.startswith("git+"):
            bad.append(f"'{name}' from git ({source}) — not a registry package")
        elif name in members:
            bad.append(f"workspace member '{name}'")
        elif FORBIDDEN.match(name):
            bad.append(f"'{name}', part of the query engine's stack")

    if bad:
        failures.extend(f"FAIL: {target} depends on {b}." for b in bad)
    else:
        print(f"ok: {target} is pure")

if failures:
    print()
    print("\n".join(failures))
    print()
    print("These crates must not know a column, a catalog, or a tenant — that is")
    print("what lets a caller use them without the query engine. See openspec")
    print("design D8 and docs/contributing/compat-crates.md.")
    sys.exit(1)
PY
