#!/usr/bin/env bash
# check-doc-freshness.sh — map a code diff to the docs that claim to describe it.
#
# Scans docs/**/*.md and .claude/skills/*/SKILL.md for frontmatter with a
# `sources:` list. If a changed file matches one of a doc's source globs and the
# doc itself was not touched in the same diff, the doc is reported as possibly
# stale. Docs with `status: record` (decision records) are exempt.
#
# Usage:
#   check-doc-freshness.sh <diff-range>   # e.g. origin/main...HEAD (CI)
#   check-doc-freshness.sh                # working tree + commits since
#                                         # merge-base with origin/main
#
# Exit 0 if no doc debt found, 1 otherwise. Report lines go to stdout.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

if [[ $# -ge 1 && -n "$1" ]]; then
    changed=$(git diff --name-only "$1")
else
    base=$(git merge-base HEAD origin/main 2>/dev/null || echo HEAD)
    changed=$(
        {
            git diff --name-only "$base"
            git diff --name-only --cached
            git ls-files --others --exclude-standard
        } | sort -u
    )
fi
[[ -z "$changed" ]] && exit 0

# --- Frontmatter validation -------------------------------------------------
# Every doc under docs/ must declare audience/type/status; without valid
# frontmatter a doc would silently opt out of the freshness system.
errors=0
while IFS= read -r doc; do
    fm=$(awk 'NR==1 && $0!="---"{exit} /^---$/{n++; next} n==1{print} n>=2{exit}' "$doc")
    if [[ -z "$fm" ]]; then
        echo "INVALID $doc: missing frontmatter (audience, type, status required)"
        errors=1
        continue
    fi
    audience=$(sed -n 's/^audience:[[:space:]]*//p' <<<"$fm")
    doctype=$(sed -n 's/^type:[[:space:]]*//p' <<<"$fm")
    status=$(sed -n 's/^status:[[:space:]]*//p' <<<"$fm")
    case "$audience" in
        user | operator | contributor) ;;
        *)
            echo "INVALID $doc: audience '$audience' not one of user|operator|contributor"
            errors=1
            ;;
    esac
    case "$doctype" in
        tutorial | how-to | reference | explanation | decision-record) ;;
        *)
            echo "INVALID $doc: type '$doctype' not one of tutorial|how-to|reference|explanation|decision-record"
            errors=1
            ;;
    esac
    case "$status" in
        living | record) ;;
        *)
            echo "INVALID $doc: status '$status' not one of living|record"
            errors=1
            ;;
    esac
    case "$doc" in
        docs/users/*) want=user ;;
        docs/operations/*) want=operator ;;
        docs/architecture/* | docs/contributing/*) want=contributor ;;
        *) want="" ;;
    esac
    if [[ -n "$want" && -n "$audience" && "$audience" != "$want" ]]; then
        echo "INVALID $doc: audience '$audience' but its directory implies '$want'"
        errors=1
    fi
    if [[ "$doc" == docs/architecture/decisions/* && "$status" != "record" ]]; then
        echo "INVALID $doc: decision records must have status: record"
        errors=1
    fi
done < <(find docs -name '*.md' -type f 2>/dev/null | sort)

# --- Sources validation ------------------------------------------------------
# Every `sources:` glob in every doc/skill frontmatter must match at least one
# existing file; a dead glob silently opts the doc out of freshness tracking.
while IFS= read -r doc; do
    fm=$(awk 'NR==1 && $0!="---"{exit} /^---$/{n++; next} n==1{print} n>=2{exit}' "$doc")
    [[ -z "$fm" ]] && continue
    globs=$(awk '
        /^sources:/ {f=1; next}
        f && /^[[:space:]]+-[[:space:]]/ {sub(/^[[:space:]]+-[[:space:]]+/, ""); print; next}
        f {exit}
    ' <<<"$fm")
    [[ -z "$globs" ]] && continue
    while IFS= read -r glob; do
        [[ -z "$glob" ]] && continue
        pattern="${glob//\*\*/*}"
        if ! compgen -G "$pattern" > /dev/null; then
            echo "INVALID $doc: source glob $glob matches no files"
            errors=1
        fi
    done <<<"$globs"
done < <(find docs .claude/skills -name '*.md' -type f 2>/dev/null | sort)

# --- Freshness: changed sources without a doc update ------------------------
stale=0
while IFS= read -r doc; do
    # Frontmatter = lines between the first pair of --- markers.
    fm=$(awk 'NR==1 && $0!="---"{exit} /^---$/{n++; next} n==1{print} n>=2{exit}' "$doc")
    [[ -z "$fm" ]] && continue
    grep -q '^status: *record' <<<"$fm" && continue

    globs=$(awk '
        /^sources:/ {f=1; next}
        f && /^[[:space:]]+-[[:space:]]/ {sub(/^[[:space:]]+-[[:space:]]+/, ""); print; next}
        f {exit}
    ' <<<"$fm")
    [[ -z "$globs" ]] && continue

    # The doc settled its own debt if it changed too.
    grep -qxF "$doc" <<<"$changed" && continue

    hits=""
    while IFS= read -r glob; do
        [[ -z "$glob" ]] && continue
        pattern="${glob//\*\*/*}"
        while IFS= read -r file; do
            [[ -z "$file" || "$file" == "$doc" ]] && continue
            # shellcheck disable=SC2053  # unquoted RHS is intentional glob matching
            if [[ "$file" == $pattern ]]; then
                hits+="$file "
            fi
        done <<<"$changed"
    done <<<"$globs"

    if [[ -n "$hits" ]]; then
        echo "$doc — sources changed: $hits"
        stale=1
    fi
done < <(find docs .claude/skills -name '*.md' -type f 2>/dev/null | sort)

[[ $stale -eq 1 || $errors -eq 1 ]] && exit 1
exit 0
