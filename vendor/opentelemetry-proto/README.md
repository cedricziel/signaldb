# Vendored: opentelemetry-proto 0.32.0

Vendored from crates.io (not the `open-telemetry/opentelemetry-rust` git
history) and patched in `src/proto.rs`'s `deserialize_from_value`: a JSON
object for an `AnyValue` with no field we recognize (empty `{}`, or only
unknown/future keys) now deserializes to `Ok(None)` instead of erroring.

Per the OTLP spec, `AnyValue` is explicitly allowed to have none of its
oneof fields set ("considered to be empty"), and unrecognized JSON fields
must be ignored for forward compatibility. Upstream's hand-rolled serde
`Deserialize` impl didn't handle either case and rejected the whole
request. Real clients hit the empty case — e.g. the OpenTelemetry JS
browser SDK's auto-instrumentation emits event-style log records with an
empty `body`, and can emit an empty `AnyValue` for an attribute whose
source value is absent (observed via `signaldb-ui`'s
`browser.resource_timing` events, SignalDB #1134).

This is already fixed upstream —
[open-telemetry/opentelemetry-rust#3595](https://github.com/open-telemetry/opentelemetry-rust/pull/3595),
merged 2026-07-21 — and the patch here applies that fix verbatim. It just
hasn't shipped in a crates.io release yet (0.32.0, 2026-05-08, remains the
latest). A straight `git` dependency pin at the merge commit was tried
first but doesn't work: `main` has since diverged from the 0.32.0 release
line with unrelated breaking changes in `opentelemetry_sdk`/`opentelemetry`
types that the crates.io-pinned `opentelemetry-otlp` 0.32.0 doesn't expect.

Remove this patch (and the `[patch.crates-io]` entry in the workspace
`Cargo.toml`) once a release ships with the fix; go back to the plain
version pin.
