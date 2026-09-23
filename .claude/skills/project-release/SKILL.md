---
name: project-release
description: SignalDB release procedure end to end - deciding whether to cut a release, checking the version bump, merging the release-please PR, curating the published notes, waiting for the binary build, and promoting the pre-releases with signaldb-bin as Latest. Use whenever someone asks "should we release", "cut a release", "ship it", "publish a new version", "mark as latest", or wants the release-please PR merged, even if they don't say "release procedure".
---

# Cutting a SignalDB release

release-please owns versions, tags and changelogs. The job here is the part
it gets wrong or leaves undone: a sane version, readable notes, and releases
that end up as full releases with the right one marked Latest.

Merging the release PR, editing a published release and promoting a
pre-release are all public and hard to take back. Get an explicit go from the
user before step 3. "Cut the release" is that go; "should we release?" is not.

## 1. Decide whether a release is due

```bash
gh release list -L 10
gh pr list --state open --search "release in:title" --json number,title,createdAt
git log --oneline "$(git describe --tags --abbrev=0 --match 'signaldb-bin-v*')"..origin/main | grep -E '^\w+ (feat|fix)'
gh run list -b main -L 5
```

Recommend a release when main carries an unreleased security fix (a
`fix(deps)` for a RUSTSEC/CVE is reason enough on its own) or user-visible
features. The main release PR is titled `chore: release main`. Component PRs
such as `chore(main): release logql-parser X` are separate; only merge those
when asked.

## 2. Check the version before anything merges

release-please bumps from commit markers. On 0.x, a breaking change only
bumps the minor when the commit carries `!` or `BREAKING CHANGE:`, and
contributors often forget. Read the PR bodies in range for removed config
keys, changed defaults, changed wire formats (timestamps, field types), and
removed UI features. If any exist and the proposed version is a patch, tell
the user and offer a `Release-As: 0.X.0` commit before merging. Once tagged,
the number is permanent.

## 3. Draft the notes, then merge

Load the `release-notes` project skill (which loads `oss:release-notes`) and
draft the notes before merging, so the user can review while CI finishes.
Delegating the draft to `oss:technical-writer` is fine. Check its output
against `oss:release-notes` yourself, and check any claim it flags as
inferred from a PR title against the diff. Two things it has gotten wrong
before: repeating one PR in both the warning block and a highlight, and
describing tools or parameters a PR never added.

Merge only when every check is SUCCESS or SKIPPED (CodeRabbit reports as a
status context, not a check run). Pin the head so a late push can't slip in:

```bash
gh pr view <N> --json mergeable,headRefOid,statusCheckRollup
gh pr merge <N> --squash --match-head-commit <sha>
```

Never edit the release PR body: release-please regenerates it on every push.

## 4. Publish the curated notes

The merge triggers the `Release Please` workflow, which creates one release
per component within a minute or two. Every one starts as a **pre-release**
(`"prerelease": true` in `release-please-config.json`).

For each release, put the curated notes on top and fold the generated body
underneath, so the commit list stays available without dominating:

```text
<curated notes>

---

<details><summary>Generated changelog</summary>

<generated body>

</details>
```

Apply with `gh release edit <tag> --notes-file <file>`. Skip a release whose
body already contains curated notes, so a rerun does not stack them.

## 5. Wait for the binaries, then promote

The same workflow then builds the `signaldb` tarballs and attaches them to
the `signaldb-bin-v*` release. This takes 15+ minutes. Watch it in the
background, not in a blocking call:

```bash
id=$(gh run list -w "Release Please" -L 1 --json databaseId -q '.[0].databaseId')
gh run watch "$id" --exit-status
gh release view signaldb-bin-v<X> --json assets -q '.assets|length'
```

GitHub rejects `--latest` on a pre-release (HTTP 422), and on its own it may
put Latest on whichever release was created last, often `grafana-plugin`.
Once the tarballs are attached, promote every release from this run and give
Latest to `signaldb-bin`, because that is where users download from:

```bash
for t in <other tags from this run>; do gh release edit "$t" --prerelease=false --latest=false; done
gh release edit signaldb-bin-v<X> --prerelease=false --latest
gh release list -L 10 --json tagName,isLatest,isPrerelease
```

If the build fails, leave everything as a pre-release and report the failing
job. Promoting a release without tarballs breaks `releases/latest/download`
links.

## 6. Report

Tell the user the release URL, what went out, and anything left open: an
unmerged component release PR, docs or sample config that still mention a
removed key, and claims in the notes you softened because the code didn't
back them.
