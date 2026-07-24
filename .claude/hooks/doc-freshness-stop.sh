#!/usr/bin/env bash
# Stop hook: soft backstop for the doc-freshness check. Fires at every turn end,
# but never blocks — it only injects a non-blocking reminder so long-running
# goals are not interrupted mid-flight. The hard gate lives in the TaskCompleted
# hook; this catches debt that never passed through a task completion (a taskless
# session, or the un-tasked tail of a goal).
#
# Reminds at most once per distinct debt set per session, and stays silent for
# any debt set the TaskCompleted hook already hard-blocked (shared "block"
# marker), so the two hooks never nag about the same thing twice.
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/doc-freshness-common.sh"

df_collect || exit 0

# Already hard-blocked at a milestone this session — don't softly repeat it.
[[ -f "$(df_marker block)" ]] && exit 0

marker=$(df_marker soft)
[[ -f "$marker" ]] && exit 0
touch "$marker"

context="Doc-freshness reminder: this branch's changes touch files that the following living docs declare as sources, but the docs were not updated:

$DF_REPORT

Before wrapping up, invoke the 'docs' skill for routing guidance, then either update the affected docs (and .claude/skills entries) or note why no documentation change is needed. This reminder will not repeat for the same set of findings."

python3 -c 'import json,sys; print(json.dumps({"hookSpecificOutput":{"hookEventName":"Stop","additionalContext":sys.argv[1]}}))' "$context"
exit 0
