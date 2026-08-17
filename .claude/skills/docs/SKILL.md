---
name: docs
description: SignalDB documentation routing - where docs live, the audience taxonomy (users/operations/architecture/contributing), required frontmatter, and when a code change owes a doc update. Use when adding or editing documentation, deciding where a doc belongs, or settling doc debt after finishing a feature.
---

# SignalDB Documentation Guide

How documentation is organized in this repo and when a code change owes a doc
update.

## Taxonomy

Docs are organized by audience:

| Location                       | Audience                  | Contents                                                                                 |
| ------------------------------ | ------------------------- | ---------------------------------------------------------------------------------------- |
| `docs/users/`                  | Sends data / runs queries | OTLP setup, Tempo API usage, Grafana plugin, API keys from the client side               |
| `docs/operations/`             | Runs SignalDB             | Deployment, configuration, WAL persistence, compactor runbooks, troubleshooting          |
| `docs/architecture/`           | Changes SignalDB          | Architecture overview, storage layout, service discovery, Flight communication           |
| `docs/architecture/decisions/` | Changes SignalDB          | Point-in-time design records (ADR-like)                                                  |
| `docs/contributing/`           | Changes SignalDB          | Prescriptive standards (Rust coding rules); `@`-included from CLAUDE.md                  |
| `.claude/skills/`              | Agents                    | Thin routers into docs/ and code plus agent-facing gotchas; never a second copy of a doc |

`docs/users/` covers OTLP ingestion, Prometheus remote_write, SQL querying,
the Tempo API reference, the Grafana datasource, and client authentication.
When work touches user-visible behavior, update the matching doc there (or add
one) rather than bolting user guidance onto README.md.

## Routing a new or updated doc

- Explains how to **send data to or query** SignalDB → `docs/users/`
- Explains how to **deploy, configure, or operate** it → `docs/operations/`
- Explains **how the internals work** → `docs/architecture/`
- Proposes or records a **design decision** → `docs/architecture/decisions/`,
  dated, `status: record`, never edited to match later reality
- Prescribes **how contributors work** → `docs/contributing/` (and consider
  whether CLAUDE.md should `@`-include it)
- Crate `README.md`s stay short: what the crate is, how to run its tests, links
  into `docs/`. Substantive content belongs in `docs/`, not READMEs.

## Frontmatter (required on every doc in docs/)

```yaml
---
audience: user | operator | contributor
type: tutorial | how-to | reference | explanation | decision-record
status: living | record
sources: # code paths this doc describes; globs allowed
  - src/common/src/wal/**
---
```

- `status: living` — the doc claims to describe the present; it must be updated
  (or consciously waved off) when its `sources` change. Freshness tooling keys
  off this field.
- `status: record` — point-in-time document; exempt from freshness checks.
- `sources` — how tooling maps a code diff to the docs it may invalidate. Name
  the files whose _stated contract_ the doc describes (a CLI, a config struct,
  an endpoint router, a schema) — never a whole crate. Broad globs cause false
  nags, and false nags get ignored or padded away.

## Writing rules

Diátaxis, applied strictly — one mode per page, never mixed:

- **How-to** (`type: how-to`): use-case driven. Title is the job ("Send OTLP
  from the Collector", "Rotate an API key"); goal → prerequisites → steps →
  verify → troubleshooting. Minimal explanation inline; link to the concept
  page for the why. This is the default type for `users/` and `operations/`.
- **Reference** (`type: reference`): the manual. Exhaustive, ordered by
  structure (config section, endpoint, flag), no narrative, no steps. One
  page per surface (config, HTTP API, CLI, schema); prefer generating it from
  code (`signaldb.dist.toml`, OpenAPI) over hand-maintained tables. Never
  fold a parameter table into a how-to — link to it.
- **Explanation** (`type: explanation`): why it is built this way. Lives in
  `architecture/`; keep it short and rare.
- **Tutorial**: only for first-contact "get it running" flows; one or two.

Every page is page one: readers land from search, so each page states its
job in the first line, stands alone, and links out instead of recapping.

Concision: second person, active voice, present tense, one idea per sentence,
no marketing, no "simply/just", no preamble or "Overview" sections. Prefer a
runnable example to a paragraph. Cut before you add; a fact that already has
a home gets a link, not a second copy. Never restate what the code says
line-by-line — document intent, contracts, and gotchas.

## Diagrams and structure

- Docs use Mermaid for diagrams (renders on GitHub, shows in diffs, covered by
  the freshness check). Architecture docs that describe a flow or topology
  lead with a diagram of it.
- Decision records: date → context → decision → consequences.

## One home per fact

Every fact lives in exactly one document; everything else links to it. Before
adding content, check whether it already has a home — update that home instead
of writing a second copy. Duplicated facts are how docs go stale.

## When finishing code work

Before wrapping up a change, check what it invalidated:

1. Did it change user-visible behavior, config options, ports, endpoints, CLI
   flags, or metrics? → fix the matching `users/` or `operations/` doc.
2. Did it change internals that `docs/architecture/` describes? Check
   `sources` frontmatter against your diff.
3. No doc affected? Fine — but decide that consciously, don't default to it.

The `TaskCompleted` hook and CI (`scripts/check-doc-freshness.sh`) enforce
this: a living doc whose `sources` changed without the doc changing blocks
once. The right response is to _read the doc against the diff and edit only
what is now wrong_ — deleting a stale sentence counts; adding a paragraph to
satisfy the check does not. If the doc is still accurate, say so (locally: one
line; on the PR: the `docs-not-needed` label).
