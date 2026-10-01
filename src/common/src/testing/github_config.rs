//! A ready-to-use `[github]` test configuration (change:
//! github-app-source-context), shared by the router crate's GitHub-App
//! tests and the `github_source_context_e2e` integration test so the
//! boilerplate struct literal lives in exactly one place.

use crate::config::GitHubAppConfig;
use crate::testing::GITHUB_TEST_PEM;

/// A `[github]` configuration pointed at `api_url` for both the REST API
/// and web base URLs (a `wiremock::MockServer`'s `.uri()` in every caller
/// today), with a fixed app id (`4242`), slug (`signaldb-test`), client
/// id/secret, and the shared [`GITHUB_TEST_PEM`] private key. Every other
/// field — TTLs, cache capacity, ... — keeps [`GitHubAppConfig::default`]'s
/// value; override one with struct-update syntax:
/// `GitHubAppConfig { snippet_cache_ttl: ttl, ..github_test_config(url) }`.
pub fn github_test_config(api_url: &str) -> GitHubAppConfig {
    GitHubAppConfig {
        app_id: 4242,
        app_slug: "signaldb-test".to_string(),
        private_key: GITHUB_TEST_PEM.to_string(),
        client_id: "test-client-id".to_string(),
        client_secret: "test-client-secret".to_string(),
        api_url: api_url.to_string(),
        web_url: api_url.to_string(),
        ..GitHubAppConfig::default()
    }
}
