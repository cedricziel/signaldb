#!/usr/bin/env bash
# TaskCompleted hook: block marking a task complete when the branch diff touches
# source files that living docs (per their `sources:` frontmatter) claim to
# describe, but the docs were not updated. This is the hard gate — it fires at
# milestone boundaries rather than every turn, so long-running goals are not
# interrupted at arbitrary points. Blocks at most once per distinct debt set per
# session: the agent may update the docs or consciously wave off, then re-mark
# the task complete.
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/doc-freshness-common.sh"

df_collect || exit 0

marker=$(df_marker block)
[[ -f "$marker" ]] && exit 0
touch "$marker"

reason="Doc-freshness check: this branch's changes touch files that the following living docs declare as sources, but the docs were not updated:

$DF_REPORT

Read each listed doc against the diff. Edit it only where a stated fact is now wrong or a new user-visible behavior has no home yet — deleting a stale sentence counts; do not add text to satisfy this check. If the doc is still accurate, say so in one line and complete the task. The 'docs' skill has the routing rules. This check will not repeat for the same set of findings."

python3 -c 'import json,sys; print(json.dumps({"decision":"block","reason":sys.argv[1]}))' "$reason"
exit 0
