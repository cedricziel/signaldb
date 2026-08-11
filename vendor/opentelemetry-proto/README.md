# Vendored: opentelemetry-proto 0.32.0

Vendored from crates.io (not the `open-telemetry/opentelemetry-rust` git
history) and patched in `src/proto.rs`'s `deserialize_from_value`: an empty
JSON object (`{}`) for an `AnyValue` now deserializes to `Ok(None)` instead
of erroring.

Per the OTLP spec, `AnyValue` is explicitly allowed to have none of its
oneof fields set ("considered to be empty"). Upstream's hand-rolled serde
`Deserialize` impl didn't handle that case and rejected the whole request.
Real clients hit this — e.g. the OpenTelemetry JS browser SDK's
auto-instrumentation emits event-style log records with an empty `body`,
and can emit an empty `AnyValue` for an attribute whose source value is
absent (observed via `signaldb-ui`'s `browser.resource_timing` events,
SignalDB #1134).

A map with unrecognized keys (garbage input, not an empty object) still
errors — only the zero-keys case was changed.

Remove this patch (and the `[patch.crates-io]` entry in the workspace
`Cargo.toml`) once fixed upstream and pull the version bump normally.
