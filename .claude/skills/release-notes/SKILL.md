---
name: release-notes
description: SignalDB release-notes routing - which release carries the user-facing notes, how release-please tags and groups components, who reads them, and where to link. Use when drafting or editing the body of a SignalDB GitHub release or asked what changed for users between two versions.
---

# SignalDB release notes

The writing standard, output shape, input gathering and gates live in the
`oss:release-notes` skill. Load it first. This file holds only the repo facts.

## Which release gets the notes

release-please tags every component separately (`signaldb-bin-v0.3.0`,
`grafana-plugin-v1.3.0`, `signaldb-ui-v0.2.0`, ...). The core crates
(`signaldb-bin`, `signaldb-cli`, `acceptor`, `router`, `writer`, `querier`,
`compactor`, `common`) share one version through the `signaldb-core`
linked-versions group, and each generated body is scoped to its crate's path.

Users run the one `signaldb` binary, and the tarballs attach to the
`signaldb-bin-*` release only. Write the product notes there, covering the
whole `signaldb-core` range, and say so in the summary. The other core
releases keep their generated bodies. `grafana-plugin` (Grafana users),
`signaldb-sdk` and `mcp-server` (integrators) get their own curated notes.

## Audiences, in order

1. Operators of a homelab or single-node install: Docker, TrueNAS, Dokku, the
   raw binary. Config keys are `[section].key` in `signaldb.toml`, env vars
   `SIGNALDB__SECTION__KEY`.
2. People sending data or querying: OTLP, Grafana, the Explore UI, the CLI.
3. Integrators: SDK, MCP, HTTP API.

## Repo values

- Previous-tag match pattern: `signaldb-bin-v*` (or the component's prefix).
- Docs links: `https://cedricziel.github.io/signaldb/` mirrors `docs/`; the
  `docs` skill says which directory serves which audience.
- Curated notes go above the generated section of the published release.
