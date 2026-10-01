#!/usr/bin/env bash
# SessionStart hook (wired in ~/.claude/settings.json): tell the model to plan
# every code-changing task as a todo list whose final items are /simplify and
# squashing the work into semantic commits. No-op outside git repos.
set -u
git rev-parse --show-toplevel >/dev/null 2>&1 || exit 0

jq -n '{hookSpecificOutput: {hookEventName: "SessionStart", additionalContext:
"Session workflow rule: before starting any task that changes code, create a todo list (TodoWrite) for it. The LAST two items of that list must always be, in this order: (1) run the `simplify` skill (Skill tool, skill=\"simplify\") on the full diff and apply its findings; (2) squash the work into clean semantic commits (one concern per commit, split anything that needs \"and\"). Do not commit before item (1) is checked off, and treat the task as unfinished until both are done."}}'
