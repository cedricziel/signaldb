## Purpose

Resolves a repo/ref/file/line reference from a trace exception frame or profile flame-graph frame to a bounded, cached source snippet, using the tenant's linked GitHub App installation, so the frame's code is visible without leaving SignalDB.

## ADDED Requirements

### Requirement: Bounded snippet lookup

Given a repo, a commit SHA or ref, a file path, and a line number, the system SHALL fetch a bounded window of source lines centered on that line (a fixed maximum line count, not the whole file) from GitHub's Contents API, using a token minted through the tenant's linked installation covering that repo, per `github-app-integration`.

#### Scenario: Snippet fetched around a line

- **WHEN** a lookup requests file `src/main.rs` at line 42 for a repo covered by the tenant's linked installation
- **THEN** the response contains a bounded window of lines around line 42, not the entire file

### Requirement: Graceful unavailability

When no linked installation covers the requested repo, the ref or file does not exist, or the line number is out of range, the lookup SHALL return a distinguishable "unavailable" result rather than an error that fails the surrounding trace or profile query. The requesting UI SHALL render the frame without a source panel in this case.

#### Scenario: No installation covers the repo

- **WHEN** a lookup is requested for a repo with no linked installation for the caller's tenant
- **THEN** the lookup returns "unavailable" and the frame's trace or profile data is still returned in full

#### Scenario: Ref or file not found

- **WHEN** a lookup names a commit SHA or file path that does not exist in the repo
- **THEN** the lookup returns "unavailable" rather than a server error

### Requirement: Response caching

Repeated lookups for the same repo, ref, file, and line window within a bounded cache period SHALL be served without a new GitHub API call, so that repeatedly viewing the same trace or profile does not consume GitHub API rate limit budget proportional to view count.

#### Scenario: Second lookup is cached

- **WHEN** the same repo/ref/file/line lookup is requested twice within the cache period
- **THEN** only the first request calls GitHub; the second is served from cache

### Requirement: Tenant-scoped authorization

A lookup SHALL be authorized against the caller's authenticated tenant, and SHALL only be able to resolve repos covered by that tenant's own linked installations, per `github-app-integration`'s tenant isolation.

#### Scenario: Lookup scoped to caller's tenant

- **WHEN** a lookup is made by a caller authenticated for tenant A
- **THEN** only installations linked to tenant A are considered when resolving the repo

### Requirement: Explore UI surfaces available snippets

The Explore UI's trace exception detail panel and profile flame-graph frame detail SHALL request a source-context lookup only when the frame already carries a file path and line number, and SHALL display the returned snippet inline with the frame when available, without blocking the rest of the trace or profile view when it is not.

#### Scenario: Exception frame with file/line shows source

- **WHEN** a trace's exception stacktrace frame includes a file path and line number and a snippet lookup succeeds
- **THEN** the source snippet is shown alongside that frame in the exception detail panel

#### Scenario: Frame without file/line requests nothing

- **WHEN** a frame carries no file path or line number
- **THEN** the UI does not attempt a source-context lookup for that frame
