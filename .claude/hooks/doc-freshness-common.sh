#!/usr/bin/env bash
# Shared logic for the doc-freshness hooks, sourced by both wrappers:
#   doc-freshness-taskcompleted.sh — hard gate at milestone (task) completion
#   doc-freshness-stop.sh          — soft reminder at turn end, for debt that
#                                    never passed through a task completion
#
# Reads the hook JSON from stdin, locates the repo the session is working in,
# runs the freshness checker, and populates:
#   DF_REPORT     — the checker's findings (empty when docs are fresh)
#   DF_DIGEST     — stable digest of the findings, for per-debt-set dedup
#   DF_SESSION_ID — session id (or "unknown")
#
# df_collect returns 0 when there is doc debt to act on, non-zero when there is
# nothing to do (docs fresh, or the checker is unavailable) — callers should
# exit 0 in that case.
set -uo pipefail

df_collect() {
    local input session_cwd repo_root checker
    input=$(cat)

    DF_SESSION_ID=$(printf '%s' "$input" | sed -n 's/.*"session_id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')
    [[ -z "$DF_SESSION_ID" ]] && DF_SESSION_ID="unknown"

    # Prefer the cwd from the hook input: in a linked worktree the session cwd is
    # the worktree, while CLAUDE_PROJECT_DIR may point at the main checkout.
    session_cwd=$(printf '%s' "$input" | sed -n 's/.*"cwd"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')
    if [[ -n "$session_cwd" && -d "$session_cwd" ]]; then
        cd "$session_cwd" || return 1
    elif [[ -n "${CLAUDE_PROJECT_DIR:-}" ]]; then
        cd "$CLAUDE_PROJECT_DIR" || return 1
    fi
    repo_root=$(git rev-parse --show-toplevel 2>/dev/null) || return 1

    # The checker cds to the toplevel of the repo it runs in, so invoking the
    # worktree's own copy (when present) checks the worktree's diff and docs.
    checker="$repo_root/scripts/check-doc-freshness.sh"
    [[ -x "$checker" ]] || checker="${CLAUDE_PROJECT_DIR:-$repo_root}/scripts/check-doc-freshness.sh"
    [[ -x "$checker" ]] || return 1

    DF_REPORT=$("$checker" 2>/dev/null)
    [[ -z "$DF_REPORT" ]] && return 1

    DF_DIGEST=$(printf '%s' "$DF_REPORT" | cksum | tr -d ' \t')
    return 0
}

# Marker path for a given kind (block | soft) and the current debt digest.
# The block hook owns the "block" marker; the soft hook checks it too, so a debt
# set already hard-blocked at a milestone is not softly re-reported at turn end.
df_marker() {
    printf '%s/claude-doc-freshness-%s-%s-%s' "${TMPDIR:-/tmp}" "$1" "$DF_SESSION_ID" "$DF_DIGEST"
}
