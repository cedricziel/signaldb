#!/usr/bin/env bash
# run-benches.sh — run every Criterion bench target in the workspace.
#
# Discovers `[[bench]]` targets via `cargo metadata` (so a new bench file
# only needs its Cargo.toml entry), enables each crate's `benchmarks`
# feature, and runs the targets one by one. Extra arguments after `--` are
# passed to Criterion, e.g.
#
#   scripts/run-benches.sh                              # full run, HTML report
#   scripts/run-benches.sh -- --save-baseline main      # record a baseline
#   scripts/run-benches.sh -- --baseline main           # compare against it
#   scripts/run-benches.sh -- --output-format bencher   # machine-readable
#
# The nightly workflow (.github/workflows/benchmarks.yml) uses the last form
# and feeds the output to github-action-benchmark. Every line Criterion
# prints is echoed AND appended to target/bench-output.txt.
#
# Usage: scripts/run-benches.sh [-p <crate>]... [-- <criterion args>...]
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

crates=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        -p | --package)
            crates+=("$2")
            shift 2
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "unknown argument: $1" >&2
            echo "usage: $0 [-p <crate>]... [-- <criterion args>...]" >&2
            exit 2
            ;;
    esac
done

# "<crate> <bench-target> [<bench-target>...]" per line — every [[bench]] in
# the workspace, grouped by crate so each crate is one cargo invocation.
targets=$(cargo metadata --format-version 1 --no-deps |
    jq -r '.packages[] | select(any(.targets[]; .kind | index("bench")))
           | .name + " " + ([.targets[] | select(.kind | index("bench")) | .name] | join(" "))')

mkdir -p target
: >target/bench-output.txt

while read -r crate benches; do
    if [[ ${#crates[@]} -gt 0 ]] && ! printf '%s\n' "${crates[@]}" | grep -qx "$crate"; then
        continue
    fi
    echo "==> $crate: $benches" >&2
    bench_flags=()
    for bench in $benches; do
        bench_flags+=(--bench "$bench")
    done
    cargo bench -p "$crate" --features benchmarks "${bench_flags[@]}" -- "$@" |
        tee -a target/bench-output.txt
done <<<"$targets"
