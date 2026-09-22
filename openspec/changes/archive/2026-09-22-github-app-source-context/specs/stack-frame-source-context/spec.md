## Purpose

Resolves a repo/ref/file/line reference from a trace exception frame or profile flame-graph frame to a bounded, cached source snippet, using the tenant's linked GitHub App installation, so the frame's code is visible without leaving SignalDB.

## ADDED Requirements

### Requirement: Bounded snippet lookup

Given a repo, a commit SHA or ref, a file path, and a line number, the system SHALL retrieve the file's content from GitHub's Contents API (using a token minted through the tenant's linked installation covering that repo, per `github-app-integration`) and SHALL return to the caller only a bounded window of source lines centered on that line (a fixed maximum line count) — never the full retrieved file content. The client SHALL base64-decode the returned content and strictly validate the decoded bytes as UTF-8 text. A decode failure, an invalid-UTF-8 result, an `encoding` value the client does not support (including `encoding: "none"`, which GitHub returns with empty content for files over its size threshold), or a decoded size over the fixed cap SHALL be treated as unavailable rather than sliced.

#### Scenario: Snippet fetched around a line

- **WHEN** a lookup requests file `src/main.rs` at line 42 for a repo covered by the tenant's linked installation
- **THEN** the response contains a bounded window of lines around line 42, not the entire file

#### Scenario: Undecodable, non-UTF-8, or oversized file is unavailable

- **WHEN** a lookup names a file whose content fails base64 decoding, decodes to bytes that are not valid UTF-8, arrives with an unsupported `encoding` (including `"none"` for an oversized file), or whose decoded size exceeds the fixed cap
- **THEN** the lookup returns "unavailable" rather than attempting to slice or return the content

### Requirement: Graceful unavailability

When no linked installation covers the requested repo, the ref or file does not exist, or the line number is out of range, the lookup SHALL return a distinguishable "unavailable" result rather than an error that fails the surrounding trace or profile query. The requesting UI SHALL render the frame without a source panel in this case. Any successful Contents API response whose effective type is not a regular file — a directory listing (a JSON array), a directory entry object, a `symlink` entry that does not itself resolve to file content, or a `submodule` entry — SHALL likewise map to "unavailable," checked before any base64 decoding or slicing is attempted. A `symlink` entry that GitHub has already resolved to file content follows the normal file path and is not special-cased.

#### Scenario: No installation covers the repo

- **WHEN** a lookup is requested for a repo with no linked installation for the caller's tenant
- **THEN** the lookup returns "unavailable" and the frame's trace or profile data is still returned in full

#### Scenario: Ref or file not found

- **WHEN** a lookup names a commit SHA or file path that does not exist in the repo
- **THEN** the lookup returns "unavailable" rather than a server error

#### Scenario: Non-file response is unavailable

- **WHEN** the requested path resolves to a directory, an unresolved `symlink` entry, or a `submodule` entry rather than a regular file
- **THEN** the lookup returns "unavailable" without attempting to decode or slice the response

### Requirement: Response caching

Repeated lookups for the same repo, ref, and file within a bounded cache period — whatever the line window — SHALL be served without a new GitHub API call; an "unavailable" outcome caused by the content itself (missing, not a file, too large, undecodable, line out of range) SHALL be cached the same way, while a transport or internal failure SHALL NOT be. Removing an installation link SHALL evict its cached entries immediately. The system, so that repeatedly viewing the same trace or profile does not consume GitHub API rate limit budget proportional to view count. The cache SHALL also enforce a fixed capacity and evict least-recently-used entries when that capacity is exceeded, independent of the cache period, so that a deployment fanning out across many distinct repo/ref/file/line-window combinations cannot grow the cache unbounded.

#### Scenario: Second lookup is cached

- **WHEN** the same repo/ref/file is requested twice within the cache period, for the same or a different line
- **THEN** only the first request calls GitHub; the second is served from cache

#### Scenario: Exceeding capacity evicts rather than growing unbounded

- **WHEN** distinct repo/ref/file lookups fill the cache to its configured capacity and another distinct lookup is then made
- **THEN** the least-recently-used entry is evicted to make room, rather than the cache growing past its configured capacity

### Requirement: Repository and ref resolution

A lookup MAY name the repository as `owner/name` or as a GitHub URL, which the system SHALL normalize before resolving it against the caller's tenant. A lookup MAY omit the repository, in which case the system SHALL probe the caller's tenant's linked repositories (bounded) for the path and serve the first match, and SHALL name the repository that served the snippet in the response. A lookup MAY omit the ref, in which case the system SHALL read the repository's default branch and SHALL report a null ref so the caller can label the snippet as unpinned.

#### Scenario: Lookup without a repository probes the tenant's repositories

- **WHEN** a lookup names only a path and line and the caller's tenant has an installation covering a repository that contains the path
- **THEN** the snippet is served from that repository and the response names it

#### Scenario: Lookup without a ref reads the default branch

- **WHEN** a lookup omits the ref
- **THEN** the file is read from the repository's default branch and the response carries a null ref

### Requirement: Availability probe for readers

The system SHALL expose, under the same authorization as the lookup, whether source context can be offered for the caller's tenant (`configured` and `linked`), so a read-only user's UI can decide to show the affordance without access to the tenant's management endpoints.

#### Scenario: Reader learns source context is available

- **WHEN** a principal with read access to a signal asks for the tenant's source-context availability after an installation was linked
- **THEN** the answer reports `configured: true` and `linked: true`

### Requirement: Flame-graph frame locations

The Query IR `flamegraph` envelope SHALL carry an optional `locations` array parallel to `names`, each entry either `{file, line}` for the first frame seen under that name or `null` when the frame carried no file, so the profile UI can offer a lookup for frames whose location is known. The Pyroscope-compatible render path is unchanged.

#### Scenario: Frame with a known file reports its location

- **WHEN** a profile's frames carry a source file and line for a function
- **THEN** the flamegraph envelope's `locations` entry for that name holds the file and line

### Requirement: Tenant-scoped authorization

A lookup SHALL be authorized against the caller's authenticated tenant, and SHALL only be able to resolve repos covered by that tenant's own linked installations, per `github-app-integration`'s tenant isolation.

#### Scenario: Lookup scoped to caller's tenant

- **WHEN** a lookup is made by a caller authenticated for tenant A
- **THEN** only installations linked to tenant A are considered when resolving the repo

### Requirement: Explore UI surfaces available snippets

The Explore UI's trace exception detail panel, the Errors view's stacktrace detail, and the profile flame-graph frame surface SHALL offer a source-context lookup only for a frame whose file path and line number are known — from `code.file.path`/`code.line.number` attributes, from a best-effort `path:line` extraction over a stacktrace line, or from the flamegraph's per-name `locations` — and SHALL display the returned snippet inline with the frame when available, naming the repository and ref it came from, without blocking the rest of the trace, error, or profile view when it is not.

#### Scenario: Exception frame with file/line shows source

- **WHEN** a trace's exception stacktrace frame includes a file path and line number and a snippet lookup succeeds
- **THEN** the source snippet is shown alongside that frame in the exception detail panel

#### Scenario: Frame without file/line requests nothing

- **WHEN** a frame carries no file path or line number
- **THEN** the UI does not attempt a source-context lookup for that frame
