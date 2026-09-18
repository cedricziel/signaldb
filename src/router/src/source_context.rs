//! Stack-frame source-context snippet lookup (change:
//! `github-app-source-context`).
//!
//! Resolves a repo/ref/file/line reference (typically from a trace
//! exception frame or a profile flame-graph frame) to a bounded, cached
//! source snippet, fetched through the tenant's linked GitHub App
//! installation (see `crate::github::GitHubApp` and
//! `common::catalog::Catalog`'s GitHub installation methods). This is a
//! plain internal service call, not a Query IR source — see the design's
//! "Source-context lookup is an internal service call, not a new Query IR
//! source" decision.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::catalog::Catalog;

use crate::github::{FileFetch, GitHubApp};

/// The number of lines of context requested when a caller doesn't specify
/// one explicitly (used by the endpoint layer, not by [`SourceContextService`]
/// itself, which always takes an explicit `context_lines`).
pub const DEFAULT_CONTEXT_LINES: u32 = 8;
/// The largest `context_lines` [`SourceRequest::new`] honors; a larger
/// request is silently clamped rather than rejected.
pub const MAX_CONTEXT_LINES: u32 = 25;
/// How many of a tenant's covered repositories a path-only lookup (no
/// `repository` given) may probe before giving up and reporting
/// [`UnavailableReason::NotFound`].
const MAX_REPOSITORY_PROBES: usize = 25;

/// Why [`SourceRequest::new`] refused to build a request. The endpoint
/// layer maps every variant straight to a `400`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidSourceRequest {
    /// `path` was empty (or all whitespace).
    #[error("path must not be empty")]
    EmptyPath,
    /// `line` was zero.
    #[error("line must be greater than zero")]
    LineIsZero,
    /// `path` contained a `..`, `.`, empty, or absolute segment — see
    /// [`is_plain_repository_path`].
    #[error("path must be a plain, relative, in-repository file path")]
    UnsafePath,
}

/// A resolved lookup request: what [`SourceContextService::lookup`] needs to
/// serve a snippet. The only way to build one is [`SourceRequest::new`],
/// which is the single point every field is validated at — `lookup` trusts
/// it never has to re-check `path` or `line`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceRequest {
    /// `owner/name`, a GitHub URL, or `None` to probe the tenant's linked
    /// repositories by path alone.
    repository: Option<String>,
    /// The ref (branch, tag, or SHA) to read at; `None` reads the
    /// repository's default branch.
    git_ref: Option<String>,
    /// The file path within the repository.
    path: String,
    /// The 1-based line number the snippet is centered on.
    line: u32,
    /// How many lines of context to include on each side of `line`, already
    /// clamped to [`MAX_CONTEXT_LINES`] by [`SourceRequest::new`].
    context_lines: u32,
}

impl SourceRequest {
    /// Build a validated request: rejects an empty or unsafe `path` and a
    /// zero `line`, and clamps `context_lines` to [`MAX_CONTEXT_LINES`].
    /// This is the request type's single validation point — every other
    /// method (`lookup`, `fetch_one`) trusts a `SourceRequest` that exists
    /// is already well-formed.
    pub fn new(
        repository: Option<String>,
        git_ref: Option<String>,
        path: String,
        line: u32,
        context_lines: u32,
    ) -> Result<Self, InvalidSourceRequest> {
        if path.trim().is_empty() {
            return Err(InvalidSourceRequest::EmptyPath);
        }
        if line == 0 {
            return Err(InvalidSourceRequest::LineIsZero);
        }
        if !is_plain_repository_path(&path) {
            // A `..`, `.` or empty segment could steer the Contents API
            // request at a different endpoint once GitHub normalizes the
            // URL; such a path can never name a file in a repository anyway.
            return Err(InvalidSourceRequest::UnsafePath);
        }
        Ok(Self {
            repository,
            git_ref,
            path,
            line,
            context_lines: context_lines.min(MAX_CONTEXT_LINES),
        })
    }
}

/// A bounded window of source lines around one line of one file, plus
/// enough metadata to render and link to it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
pub struct SourceSnippet {
    /// The `"owner/name"` repository that served the snippet.
    pub repository: String,
    /// The ref the caller asked for; `None` means the repository's default
    /// branch.
    #[serde(rename = "ref")]
    pub git_ref: Option<String>,
    /// The file path within the repository.
    pub path: String,
    /// The 1-based line number the snippet is centered on.
    pub line: u32,
    /// The 1-based line number `lines[0]` corresponds to.
    pub start_line: u32,
    /// The window of source lines, `start_line..=start_line + lines.len() - 1`.
    pub lines: Vec<String>,
    /// GitHub's `html_url` for the file, with a `#L{line}` fragment.
    pub html_url: String,
    /// The blob's `sha`, as GitHub reports it.
    pub sha: String,
}

/// Why a lookup could not serve a snippet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableReason {
    /// `[github]` is absent (or failed to build).
    NotConfigured,
    /// No installation linked to the caller's tenant covers the requested
    /// (or any) repository. Also returned for a repository covered only by
    /// another tenant's installation — indistinguishable from no
    /// installation at all, by design.
    NoInstallation,
    /// The path or ref does not exist in the repository.
    NotFound,
    /// The path resolved to something other than a regular file.
    NotAFile,
    /// The file exceeds the size cap GitHub or this client enforces.
    TooLarge,
    /// The file's content could not be decoded as UTF-8 text.
    Undecodable,
    /// `line` is past the end of the file.
    LineOutOfRange,
    /// The GitHub API call itself failed (transport or a non-404 status).
    /// Never cached.
    GithubError,
    /// The catalog lookup needed to resolve an installation failed. Never
    /// cached.
    Internal,
}

/// The outcome of a source-context lookup: either a snippet, or a reason it
/// could not be served. Never an error — a failed lookup degrades the
/// caller's frame to "no source panel", it never fails the surrounding
/// trace or profile query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceLookup {
    /// A snippet was fetched (or served from cache).
    Available(SourceSnippet),
    /// No snippet could be served, and why.
    Unavailable(UnavailableReason),
}

/// One cached file's identity: the exact (installation, repo, ref, file)
/// combination a fetch was made for. Not keyed on a line range — every
/// request against the same file shares one cache entry, and the requested
/// window is sliced out of it at lookup time (see [`CachedFile`]).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    installation_id: i64,
    repository: String,
    git_ref: String,
    path: String,
}

/// A cached file's content, or a reason inherent to the file/path/ref itself
/// (never a transient GitHub failure — see [`SourceContextService::fetch_one`])
/// that it could not be read.
#[derive(Debug, Clone)]
enum CachedFile {
    /// The file, decoded into lines, plus the metadata every window sliced
    /// from it needs. `Arc` so a cache hit clones cheaply regardless of file
    /// size.
    Available {
        lines: Arc<Vec<String>>,
        sha: String,
        html_url: String,
    },
    /// The path/ref could not be resolved to a servable file.
    Unavailable(UnavailableReason),
}

/// A cached [`CachedFile`] plus when it was inserted, for TTL expiry.
#[derive(Debug, Clone)]
struct CacheEntry {
    content: CachedFile,
    inserted: Instant,
}

/// Resolves source-context lookups against a tenant's linked GitHub
/// installations, backed by a TTL-and-capacity-bounded, LRU-evicting cache
/// keyed on (installation, repo, ref, file) — one entry per file, not per
/// requested line window, so every frame of a stacktrace sharing a file
/// costs at most one GitHub fetch — see the design's "Snippet cache"
/// decision.
pub struct SourceContextService {
    app: Arc<GitHubApp>,
    ttl: Duration,
    cache: tokio::sync::Mutex<lru::LruCache<CacheKey, CacheEntry>>,
}

impl SourceContextService {
    /// Build a service over `app`, reading `snippet_cache_ttl` and
    /// `snippet_cache_capacity` from `app.config()`.
    pub fn new(app: Arc<GitHubApp>) -> Self {
        let ttl = app.config().snippet_cache_ttl;
        let capacity =
            NonZeroUsize::new(app.config().snippet_cache_capacity).unwrap_or(NonZeroUsize::MIN);
        Self {
            app,
            ttl,
            cache: tokio::sync::Mutex::new(lru::LruCache::new(capacity)),
        }
    }

    /// Resolve `request` for `tenant_id`, per the module docs' flow: an
    /// explicit `repository` resolves through
    /// [`Catalog::find_github_installation_for_repository`]; an absent one
    /// probes every repository covered by the tenant's linked installations
    /// (capped at [`MAX_REPOSITORY_PROBES`]), returning the first available
    /// snippet.
    pub async fn lookup(
        &self,
        catalog: &Catalog,
        tenant_id: &str,
        request: &SourceRequest,
    ) -> SourceLookup {
        if let Some(raw_repository) = request.repository.as_deref() {
            let Some(normalized) = normalize_repository(raw_repository) else {
                return SourceLookup::Unavailable(UnavailableReason::NoInstallation);
            };
            let installation = match catalog
                .find_github_installation_for_repository(tenant_id, &normalized)
                .await
            {
                Ok(Some(installation)) => installation,
                Ok(None) => return SourceLookup::Unavailable(UnavailableReason::NoInstallation),
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        tenant_id,
                        "GitHub installation lookup failed during source-context resolution"
                    );
                    return SourceLookup::Unavailable(UnavailableReason::Internal);
                }
            };
            let canonical_repository = installation
                .repositories
                .iter()
                .find(|repo| repo.eq_ignore_ascii_case(&normalized))
                .cloned()
                .unwrap_or(normalized);
            return self
                .fetch_one(installation.installation_id, &canonical_repository, request)
                .await;
        }

        let installations = match catalog.list_github_installations(tenant_id).await {
            Ok(installations) => installations,
            Err(error) => {
                tracing::error!(
                    error = %error,
                    tenant_id,
                    "GitHub installation listing failed during source-context resolution"
                );
                return SourceLookup::Unavailable(UnavailableReason::Internal);
            }
        };
        let candidates: Vec<(i64, String)> = installations
            .into_iter()
            .flat_map(|installation| {
                let installation_id = installation.installation_id;
                installation
                    .repositories
                    .into_iter()
                    .map(move |repository| (installation_id, repository))
            })
            .take(MAX_REPOSITORY_PROBES)
            .collect();
        if candidates.is_empty() {
            return SourceLookup::Unavailable(UnavailableReason::NoInstallation);
        }
        for (installation_id, repository) in candidates {
            let outcome = self.fetch_one(installation_id, &repository, request).await;
            if matches!(outcome, SourceLookup::Available(_)) {
                return outcome;
            }
        }
        SourceLookup::Unavailable(UnavailableReason::NotFound)
    }

    /// Drop every cached entry belonging to `installation_id`, and the
    /// underlying [`GitHubApp`]'s cached installation token with it (called
    /// when a tenant removes a linked installation, so neither a snippet
    /// lookup nor a token mint immediately after removal can serve stale
    /// cached state — spec: "removal takes effect immediately"). The single
    /// place a caller needs to know what to flush on unlink.
    pub async fn forget_installation(&self, installation_id: i64) {
        let mut cache = self.cache.lock().await;
        let stale: Vec<CacheKey> = cache
            .iter()
            .filter(|(key, _)| key.installation_id == installation_id)
            .map(|(key, _)| key.clone())
            .collect();
        for key in stale {
            cache.pop(&key);
        }
        drop(cache);
        self.app.forget_installation(installation_id).await;
    }

    /// Resolve one (installation, repository) candidate: a cached file hit
    /// within `ttl` is served directly; a miss fetches the whole file via
    /// [`GitHubApp::file_content`] and caches it (every outcome except
    /// [`UnavailableReason::GithubError`]/[`UnavailableReason::Internal`] —
    /// never cache a transient failure). Either way, `request`'s window is
    /// then sliced out of the (cached or freshly fetched) file; an
    /// out-of-range `line` is computed against the cached file every call
    /// and is itself never cached.
    async fn fetch_one(
        &self,
        installation_id: i64,
        repository: &str,
        request: &SourceRequest,
    ) -> SourceLookup {
        let key = CacheKey {
            installation_id,
            repository: repository.to_string(),
            git_ref: request.git_ref.clone().unwrap_or_default(),
            path: request.path.clone(),
        };
        {
            let mut cache = self.cache.lock().await;
            if let Some(entry) = cache.get(&key)
                && entry.inserted.elapsed() < self.ttl
            {
                return Self::window(repository, request, entry.content.clone());
            }
        }

        let content = match self
            .app
            .file_content(
                installation_id,
                repository,
                request.git_ref.as_deref(),
                &request.path,
            )
            .await
        {
            Ok(FileFetch::File {
                text,
                sha,
                html_url,
            }) => CachedFile::Available {
                lines: Arc::new(text.lines().map(str::to_string).collect()),
                sha,
                html_url,
            },
            Ok(FileFetch::NotFound) => CachedFile::Unavailable(UnavailableReason::NotFound),
            Ok(FileFetch::NotAFile) => CachedFile::Unavailable(UnavailableReason::NotAFile),
            Ok(FileFetch::TooLarge) => CachedFile::Unavailable(UnavailableReason::TooLarge),
            Ok(FileFetch::Undecodable) => CachedFile::Unavailable(UnavailableReason::Undecodable),
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    installation_id,
                    repository,
                    "GitHub source-context fetch failed"
                );
                return SourceLookup::Unavailable(UnavailableReason::GithubError);
            }
        };

        {
            let mut cache = self.cache.lock().await;
            cache.put(
                key,
                CacheEntry {
                    content: content.clone(),
                    inserted: Instant::now(),
                },
            );
        }
        Self::window(repository, request, content)
    }

    /// Slice `request`'s `line`/`context_lines` window out of `content`,
    /// computing (never caching) [`UnavailableReason::LineOutOfRange`] when
    /// `line` is past the file's end.
    fn window(repository: &str, request: &SourceRequest, content: CachedFile) -> SourceLookup {
        let (lines, sha, html_url) = match content {
            CachedFile::Available {
                lines,
                sha,
                html_url,
            } => (lines, sha, html_url),
            CachedFile::Unavailable(reason) => return SourceLookup::Unavailable(reason),
        };
        if request.line as usize > lines.len() {
            return SourceLookup::Unavailable(UnavailableReason::LineOutOfRange);
        }
        let start_line = request.line.saturating_sub(request.context_lines).max(1);
        let end_line = request.line + request.context_lines;
        let slice_start = (start_line - 1) as usize;
        let slice_end = (end_line as usize).min(lines.len());
        SourceLookup::Available(SourceSnippet {
            repository: repository.to_string(),
            git_ref: request.git_ref.clone(),
            path: request.path.clone(),
            line: request.line,
            start_line,
            lines: lines[slice_start..slice_end].to_vec(),
            html_url: format!("{html_url}#L{}", request.line),
            sha,
        })
    }
}

/// Whether `path` is a plain, relative, in-repository file path: non-empty,
/// no leading slash, and no empty, `.` or `..` segment.
pub fn is_plain_repository_path(path: &str) -> bool {
    !path.is_empty()
        && !path.starts_with('/')
        && path
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
}

/// Read `"owner/name"` out of a plain `owner/name`, an `https://github.com/…`
/// URL (with or without a trailing `.git`), an `ssh` remote
/// (`git@github.com:owner/name.git`), or a bare `github.com/owner/name`.
/// Case-insensitive input, always lowercased on the way out; `None` when no
/// two-segment `owner/name` can be read.
pub fn normalize_repository(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    let body = raw
        .strip_prefix("git@github.com:")
        .or_else(|| raw.strip_prefix("https://github.com/"))
        .or_else(|| raw.strip_prefix("http://github.com/"))
        .or_else(|| raw.strip_prefix("github.com/"))
        .unwrap_or(raw);
    let body = body.strip_suffix(".git").unwrap_or(body);
    let body = body.trim_matches('/');

    let mut segments = body.split('/');
    let owner = segments.next()?;
    let name = segments.next()?;
    if owner.is_empty() || name.is_empty() || segments.next().is_some() {
        return None;
    }
    Some(format!("{owner}/{name}").to_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_repository_paths_reject_traversal_and_absolute_forms() {
        assert!(is_plain_repository_path("src/main.rs"));
        assert!(is_plain_repository_path("README"));
        assert!(is_plain_repository_path("dir with space/x.py"));
        for bad in [
            "",
            "/src/main.rs",
            "src/../main.rs",
            "../x",
            "./x",
            "src//x",
            "src/",
        ] {
            assert!(!is_plain_repository_path(bad), "{bad:?} must be rejected");
        }
    }

    #[test]
    fn new_rejects_empty_path_zero_line_and_unsafe_path_clamps_context() {
        assert_eq!(
            SourceRequest::new(None, None, "  ".to_string(), 1, 0).unwrap_err(),
            InvalidSourceRequest::EmptyPath
        );
        assert_eq!(
            SourceRequest::new(None, None, "f.rs".to_string(), 0, 0).unwrap_err(),
            InvalidSourceRequest::LineIsZero
        );
        assert_eq!(
            SourceRequest::new(None, None, "../x".to_string(), 1, 0).unwrap_err(),
            InvalidSourceRequest::UnsafePath
        );
        let request = SourceRequest::new(None, None, "f.rs".to_string(), 1, 9_999).unwrap();
        assert_eq!(request.context_lines, MAX_CONTEXT_LINES);
    }

    use common::catalog::{Catalog, GitHubLinkOutcome, NewGitHubInstallation};
    use common::config::GitHubAppConfig;
    use wiremock::matchers::{method, path};
    use wiremock::{MockServer, ResponseTemplate};

    use crate::github::test_support::{contents_file_body, mount_installation_token};

    /// A `req("f.rs", 3, 1)`-shaped [`SourceRequest`] against `octo/api`'s
    /// default branch, collapsing every test's request literal below.
    fn req(path: &str, line: u32, context_lines: u32) -> SourceRequest {
        SourceRequest::new(
            Some("octo/api".to_string()),
            None,
            path.to_string(),
            line,
            context_lines,
        )
        .expect("valid request")
    }

    /// Mounts a Contents API GET mock for `octo/api`'s `path_name`,
    /// returning `text` with a fixed sha/html_url.
    async fn mount_file(server: &MockServer, path_name: &str, text: &str) {
        wiremock::Mock::given(method("GET"))
            .and(path(format!("/repos/octo/api/contents/{path_name}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(contents_file_body(
                text,
                "sha-1",
                &format!("https://github.com/octo/api/blob/main/{path_name}"),
            )))
            .mount(server)
            .await;
    }

    /// A catalog with one tenant, one linked installation (id 42) covering
    /// `octo/api`, and an installation-token mock.
    async fn seed_service(
        server: &MockServer,
        ttl: Duration,
        capacity: usize,
    ) -> (Catalog, Arc<SourceContextService>) {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        catalog
            .upsert_tenant("acme", "Acme", Some("production"), "config")
            .await
            .unwrap();
        catalog
            .create_github_link_state("state-hash", "acme", None, Duration::from_secs(600))
            .await
            .unwrap();
        let outcome = catalog
            .complete_github_link(
                "state-hash",
                &NewGitHubInstallation {
                    installation_id: 42,
                    account_login: "octo".to_string(),
                    account_type: "Organization".to_string(),
                    account_id: 1,
                    repositories: vec!["octo/api".to_string()],
                    linked_by_user_id: None,
                    linked_by_github_login: None,
                },
            )
            .await
            .unwrap();
        assert!(matches!(outcome, GitHubLinkOutcome::Linked(_)));

        mount_installation_token(server, 42).await;

        let config = GitHubAppConfig {
            snippet_cache_ttl: ttl,
            snippet_cache_capacity: capacity,
            ..common::testing::github_test_config(&server.uri())
        };
        let app = Arc::new(GitHubApp::new(config).unwrap());
        (catalog, Arc::new(SourceContextService::new(app)))
    }

    #[test]
    fn normalize_repository_reads_all_supported_shapes() {
        for input in [
            "octo/api",
            "OCTO/API",
            "https://github.com/octo/api",
            "https://github.com/octo/api.git",
            "http://github.com/octo/api",
            "git@github.com:octo/api.git",
            "github.com/octo/api",
            "  octo/api  ",
        ] {
            assert_eq!(
                normalize_repository(input),
                Some("octo/api".to_string()),
                "input: {input}"
            );
        }
        for bad in ["", "octo", "octo/api/extra", "   ", "/"] {
            assert_eq!(normalize_repository(bad), None, "input: {bad}");
        }
    }

    #[tokio::test]
    async fn snippet_window_math_first_last_and_middle_lines() {
        let server = MockServer::start().await;
        let (catalog, service) = seed_service(&server, Duration::from_secs(60), 100).await;
        let text = (1..=20)
            .map(|n| format!("line{n}"))
            .collect::<Vec<_>>()
            .join("\n");
        mount_file(&server, "f.rs", &text).await;

        // Middle line, context 2: window [8, 12].
        let SourceLookup::Available(snippet) =
            service.lookup(&catalog, "acme", &req("f.rs", 10, 2)).await
        else {
            panic!("expected Available");
        };
        assert_eq!(snippet.start_line, 8);
        assert_eq!(
            snippet.lines,
            vec!["line8", "line9", "line10", "line11", "line12"]
        );
        assert_eq!(
            snippet.html_url,
            "https://github.com/octo/api/blob/main/f.rs#L10"
        );
        assert_eq!(snippet.sha, "sha-1");

        // First line, context 5: start_line clamps to 1.
        let SourceLookup::Available(snippet) =
            service.lookup(&catalog, "acme", &req("f.rs", 1, 5)).await
        else {
            panic!("expected Available");
        };
        assert_eq!(snippet.start_line, 1);
        assert_eq!(snippet.lines[0], "line1");
        assert_eq!(snippet.lines.last().unwrap(), "line6");

        // Last line, context 5: window clamps to the file's end.
        let SourceLookup::Available(snippet) =
            service.lookup(&catalog, "acme", &req("f.rs", 20, 5)).await
        else {
            panic!("expected Available");
        };
        assert_eq!(snippet.lines.last().unwrap(), "line20");
        assert_eq!(*snippet.lines.first().unwrap(), "line15");

        // Past the end of the file: out of range.
        assert_eq!(
            service.lookup(&catalog, "acme", &req("f.rs", 21, 2)).await,
            SourceLookup::Unavailable(UnavailableReason::LineOutOfRange)
        );
    }

    #[tokio::test]
    async fn a_second_lookup_for_another_line_in_the_same_file_is_cached() {
        let server = MockServer::start().await;
        let (catalog, service) = seed_service(&server, Duration::from_secs(60), 100).await;
        // `.expect(1)`, verified when `server` drops: a second fetch for
        // the same file (regardless of which line/context it asks for)
        // would fail this test.
        wiremock::Mock::given(method("GET"))
            .and(path("/repos/octo/api/contents/f.rs"))
            .respond_with(ResponseTemplate::new(200).set_body_json(contents_file_body(
                "a\nb\nc\nd\ne\n",
                "sha-1",
                "https://github.com/octo/api/blob/main/f.rs",
            )))
            .expect(1)
            .mount(&server)
            .await;

        assert!(matches!(
            service.lookup(&catalog, "acme", &req("f.rs", 3, 1)).await,
            SourceLookup::Available(_)
        ));
        // A different line, and a different context width, in the same
        // file: still served from the one cached file.
        assert!(matches!(
            service.lookup(&catalog, "acme", &req("f.rs", 5, 0)).await,
            SourceLookup::Available(_)
        ));
        assert!(matches!(
            service.lookup(&catalog, "acme", &req("f.rs", 1, 2)).await,
            SourceLookup::Available(_)
        ));
    }

    #[tokio::test]
    async fn capacity_two_evicts_least_recently_used() {
        let server = MockServer::start().await;
        let (catalog, service) = seed_service(&server, Duration::from_secs(60), 2).await;

        for path_name in ["a.rs", "b.rs", "c.rs"] {
            mount_file(&server, path_name, "x\n").await;
        }

        for path_name in ["a.rs", "b.rs", "c.rs"] {
            assert!(matches!(
                service
                    .lookup(&catalog, "acme", &req(path_name, 1, 0))
                    .await,
                SourceLookup::Available(_)
            ));
        }
        // Capacity 2: `a.rs` was evicted when `c.rs` was inserted, so
        // looking it up again is a fourth GitHub call.
        assert!(matches!(
            service.lookup(&catalog, "acme", &req("a.rs", 1, 0)).await,
            SourceLookup::Available(_)
        ));

        let calls = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.url.path().starts_with("/repos/octo/api/contents/"))
            .count();
        assert_eq!(calls, 4, "expected a.rs to be refetched after eviction");
    }

    #[tokio::test]
    async fn ttl_expiry_causes_a_refetch() {
        let server = MockServer::start().await;
        let (catalog, service) = seed_service(&server, Duration::from_millis(50), 100).await;
        wiremock::Mock::given(method("GET"))
            .and(path("/repos/octo/api/contents/f.rs"))
            .respond_with(ResponseTemplate::new(200).set_body_json(contents_file_body(
                "x\n",
                "sha",
                "https://github.com/octo/api/blob/main/f.rs",
            )))
            .expect(2)
            .mount(&server)
            .await;

        assert!(matches!(
            service.lookup(&catalog, "acme", &req("f.rs", 1, 0)).await,
            SourceLookup::Available(_)
        ));
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(matches!(
            service.lookup(&catalog, "acme", &req("f.rs", 1, 0)).await,
            SourceLookup::Available(_)
        ));
    }

    #[tokio::test]
    async fn unavailable_outcomes_are_cached_but_github_error_is_not() {
        let server = MockServer::start().await;
        let (catalog, service) = seed_service(&server, Duration::from_secs(60), 100).await;

        wiremock::Mock::given(method("GET"))
            .and(path("/repos/octo/api/contents/missing.rs"))
            .respond_with(
                ResponseTemplate::new(404)
                    .set_body_json(serde_json::json!({"message": "Not Found"})),
            )
            .expect(1)
            .mount(&server)
            .await;
        wiremock::Mock::given(method("GET"))
            .and(path("/repos/octo/api/contents/broken.rs"))
            .respond_with(ResponseTemplate::new(500))
            .expect(2)
            .mount(&server)
            .await;

        assert_eq!(
            service
                .lookup(&catalog, "acme", &req("missing.rs", 1, 0))
                .await,
            SourceLookup::Unavailable(UnavailableReason::NotFound)
        );
        // Cached: the 404 mock expects exactly one call.
        assert_eq!(
            service
                .lookup(&catalog, "acme", &req("missing.rs", 1, 0))
                .await,
            SourceLookup::Unavailable(UnavailableReason::NotFound)
        );

        assert_eq!(
            service
                .lookup(&catalog, "acme", &req("broken.rs", 1, 0))
                .await,
            SourceLookup::Unavailable(UnavailableReason::GithubError)
        );
        // Not cached: a second lookup calls GitHub again (the 500 mock
        // expects exactly two calls).
        assert_eq!(
            service
                .lookup(&catalog, "acme", &req("broken.rs", 1, 0))
                .await,
            SourceLookup::Unavailable(UnavailableReason::GithubError)
        );
    }

    #[tokio::test]
    async fn forget_installation_evicts_its_cached_entries() {
        let server = MockServer::start().await;
        let (catalog, service) = seed_service(&server, Duration::from_secs(60), 100).await;
        wiremock::Mock::given(method("GET"))
            .and(path("/repos/octo/api/contents/f.rs"))
            .respond_with(ResponseTemplate::new(200).set_body_json(contents_file_body(
                "x\n",
                "sha",
                "https://github.com/octo/api/blob/main/f.rs",
            )))
            .expect(2)
            .mount(&server)
            .await;

        assert!(matches!(
            service.lookup(&catalog, "acme", &req("f.rs", 1, 0)).await,
            SourceLookup::Available(_)
        ));
        service.forget_installation(42).await;
        assert!(matches!(
            service.lookup(&catalog, "acme", &req("f.rs", 1, 0)).await,
            SourceLookup::Available(_)
        ));
    }
}
