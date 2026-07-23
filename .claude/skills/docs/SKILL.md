---
name: docs
description: SignalDB documentation routing - where docs live, the audience taxonomy (users/operations/architecture/contributing), required frontmatter, and when a code change owes a doc update. Use when adding or editing documentation, deciding where a doc belongs, or settling doc debt after finishing a feature.
---

# SignalDB Documentation Guide

How documentation is organized in this repo and when a code change owes a doc
update. For writing craft (style, structure, doc modes), also load the
`technical-writing` skill.

## Taxonomy

Docs are organized by audience:

| Location | Audience | Contents |
|----------|----------|----------|
| `docs/users/` | Sends data / runs queries | OTLP setup, Tempo API usage, Grafana plugin, API keys from the client side |
| `docs/operations/` | Runs SignalDB | Deployment, configuration, WAL persistence, compactor runbooks, troubleshooting |
| `docs/architecture/` | Changes SignalDB | Architecture overview, storage layout, service discovery, Flight communication |
| `docs/architecture/decisions/` | Changes SignalDB | Point-in-time design records (ADR-like) |
| `docs/contributing/` | Changes SignalDB | Prescriptive standards (Rust coding rules); `@`-included from CLAUDE.md |
| `.claude/skills/` | Agents | Condensed, derived views of docs + code; refreshed via `/refresh-skills` |

Known gap: `docs/users/` is largely unwritten. When work touches user-visible
behavior, prefer starting a doc there over bolting user guidance onto README.md.

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
sources:            # code paths this doc describes; globs allowed
  - src/common/src/wal/**
---
```

- `status: living` — the doc claims to describe the present; it must be updated
  (or consciously waved off) when its `sources` change. Freshness tooling keys
  off this field.
- `status: record` — point-in-time document; exempt from freshness checks.
- `sources` — how tooling maps a code diff to the docs it may invalidate. Keep
  globs tight: overly broad sources cause false nags, which get ignored.
- Knowledge skills in `.claude/skills/` carry the same `sources` field in their
  frontmatter and are held to the same freshness expectation.

## One home per fact

Every fact lives in exactly one document; everything else links to it. Before
adding content, check whether it already has a home — update that home instead
of writing a second copy. Duplicated facts are how docs go stale.

## When finishing code work

Before wrapping up a change, check what it invalidated:

1. Did it change user-visible behavior, config options, ports, endpoints, CLI
   flags, or metrics? → update the matching `users/` or `operations/` doc.
2. Did it change internals that `docs/architecture/` or a knowledge skill
   describes? Check `sources` frontmatter against your diff.
3. No doc affected? Fine — but decide that consciously, don't default to it.

A Stop hook enforces this mechanically: if your diff touches files matched by a
living doc's `sources` and that doc wasn't updated, you'll be asked once to
either update it or state why no update is needed.
