---
name: refresh-skills
description: Refresh all project knowledge skills by re-reading the codebase, docs, and source files to update stale SKILL.md content.
disable-model-invocation: true
---

# Refresh Project Knowledge Skills

Scan the codebase and update all knowledge skills in `.claude/skills/` with current information.

## Instructions

For each skill listed below, read the **source-of-truth files**, compare against the current SKILL.md content, and update anything that is stale, missing, or incorrect. Preserve the YAML frontmatter (`name`, `description`, `user-invocable`, `disable-model-invocation`) but update the `description` if the scope changed.

Do NOT rewrite skills that are already accurate. Only touch what changed.

### Skills and their source-of-truth files

Each knowledge skill declares its source-of-truth files in the `sources:` list
of its own frontmatter — read that list, not a central table. Additional notes:

- **crate-map**: after `Cargo.toml` (workspace members), read `src/*/src/lib.rs`
  or `src/*/src/main.rs` for each crate to check module structure
- **adding-new-signal**: has no `sources:`; cross-reference with the
  `architecture`, `flight-schemas`, and `storage-layout` skills for consistency
- **multi-tenancy** / **configuration**: also check the Multi-Tenancy and
  Configuration sections of `docs/architecture/overview.md`

### Refresh procedure

1. Read all source-of-truth files for a skill (batch reads where possible)
2. Read the current SKILL.md
3. Diff mentally: are there new fields, renamed modules, changed ports, new config options, new endpoints, removed features?
4. If changes found: edit the SKILL.md with minimal, targeted updates
5. If no changes: skip, report "up to date"
6. After all skills are checked, report a summary of what changed

### What to look for

- **New workspace members** added to `Cargo.toml`
- **New or renamed modules** in any crate
- **Schema changes** in `schemas.toml` (new fields, new versions, new signal types)
- **New config options** in `src/common/src/config/mod.rs`
- **New API endpoints** in router
- **Port changes** in any service
- **New dependencies** that affect patterns (e.g., new error handling crate)
- **Rust edition or MSRV changes** in `Cargo.toml`
- **New Flight RPC methods** or query types

### Output format

After refreshing, print a summary like:

```
Skills refresh complete:
- architecture: updated (added new Querier port, updated deployment notes)
- storage-layout: up to date
- flight-schemas: updated (new log schema v2 fields)
- ...
```
