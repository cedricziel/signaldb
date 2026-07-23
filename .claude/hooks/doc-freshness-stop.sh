#!/usr/bin/env bash
# Stop hook: block ending the turn when the session's diff touches source files
# that living docs (per their `sources:` frontmatter) claim to describe, but the
# docs were not updated. Blocks at most once per distinct debt set per session —
# the agent may update the docs or consciously wave off, but not forget.
set -uo pipefail

input=$(cat)

# Never re-block while the agent is already continuing because of this hook.
if [[ "$input" == *'"stop_hook_active":true'* || "$input" == *'"stop_hook_active": true'* ]]; then
    exit 0
fi

session_id=$(printf '%s' "$input" | sed -n 's/.*"session_id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')
[[ -z "$session_id" ]] && session_id="unknown"

# Run against the repo the session is actually working in. In a linked worktree
# the session cwd is the worktree, while CLAUDE_PROJECT_DIR may point at the
# main checkout — prefer the cwd from the hook input.
session_cwd=$(printf '%s' "$input" | sed -n 's/.*"cwd"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')
if [[ -n "$session_cwd" && -d "$session_cwd" ]]; then
    cd "$session_cwd"
elif [[ -n "${CLAUDE_PROJECT_DIR:-}" ]]; then
    cd "$CLAUDE_PROJECT_DIR"
fi
repo_root=$(git rev-parse --show-toplevel 2>/dev/null) || exit 0

# The checker cds to the toplevel of the repo it runs in, so invoking the
# worktree's own copy (when present) checks the worktree's diff and docs.
checker="$repo_root/scripts/check-doc-freshness.sh"
[[ -x "$checker" ]] || checker="${CLAUDE_PROJECT_DIR:-$repo_root}/scripts/check-doc-freshness.sh"
[[ -x "$checker" ]] || exit 0

report=$("$checker" 2>/dev/null)
[[ -z "$report" ]] && exit 0

# One nag per distinct debt set: key marker on session + report content.
digest=$(printf '%s' "$report" | cksum | tr -d ' \t')
marker="${TMPDIR:-/tmp}/claude-doc-freshness-${session_id}-${digest}"
[[ -f "$marker" ]] && exit 0
touch "$marker"

reason="Doc-freshness check: this session's changes touch files that the following living docs declare as sources, but the docs were not updated:

$report

Invoke the 'docs' skill for routing guidance, then either update the affected docs (and .claude/skills entries) or state explicitly in your final message why no documentation change is needed. This check will not repeat for the same set of findings."

python3 -c 'import json,sys; print(json.dumps({"decision":"block","reason":sys.argv[1]}))' "$reason"
exit 0
