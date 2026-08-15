# Vendored OpenTelemetry semantic conventions

`1.43.0/model/` is a verbatim copy of `model/` from
https://github.com/open-telemetry/semantic-conventions at tag `v1.43.0`
(Apache-2.0, see `1.43.0/LICENSE`). It is the source of the bundled
`otel` schema registry. Do not edit by hand — regenerate with
`cargo xtask vendor-semconv` after bumping
`common::self_monitoring::SEMCONV_SCHEMA_URL`.
