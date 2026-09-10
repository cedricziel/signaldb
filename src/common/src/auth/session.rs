//! # UI session cookie
//!
//! Extraction for the opaque browser session cookie used by the embedded
//! explore UI. The cookie contains only a high-entropy token; its SHA-256
//! digest and session metadata live in the service catalog.

use axum::http::HeaderMap;

use super::SESSION_TOKEN_PREFIX;

/// Name of the session cookie set by `POST /ui/session`.
pub const SESSION_COOKIE: &str = "signaldb_session";

/// Build the `Set-Cookie` header value for a freshly issued session token.
/// The single construction site for the cookie every login path (password,
/// OIDC SSO — change: oidc-login) sets, so they stay byte-for-byte
/// identical: `HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=43200`
/// (12 hours, matching [`crate::catalog::Catalog::create_user_session`]'s
/// TTL).
pub fn session_cookie_header(token: &str) -> String {
    format!("{SESSION_COOKIE}={token}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=43200")
}

/// Extract an opaque server-side session token from the session cookie.
///
/// Requiring the session-token prefix deliberately rejects the legacy
/// base64-encoded API-key cookie format.
pub fn session_token_from_headers(headers: &HeaderMap) -> Option<String> {
    for header in headers.get_all(axum::http::header::COOKIE) {
        let Ok(cookies) = header.to_str() else {
            continue;
        };
        for pair in cookies.split(';') {
            let Some((name, value)) = pair.split_once('=') else {
                continue;
            };
            let value = value.trim();
            if name.trim() == SESSION_COOKIE && value.starts_with(SESSION_TOKEN_PREFIX) {
                return Some(value.to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn session_token_from_headers_finds_cookie_among_others() {
        let mut headers = HeaderMap::new();
        let token = crate::auth::generate_session_token();
        let value = format!("theme=dark; {SESSION_COOKIE}={}; other=1", token);
        headers.insert(
            axum::http::header::COOKIE,
            HeaderValue::from_str(&value).unwrap(),
        );
        assert_eq!(session_token_from_headers(&headers), Some(token));
    }

    #[test]
    fn session_cookie_header_carries_the_token_and_required_attributes() {
        let header = session_cookie_header("sdbs_abc123");
        assert!(header.starts_with("signaldb_session=sdbs_abc123;"));
        assert!(header.contains("HttpOnly"));
        assert!(header.contains("Secure"));
        assert!(header.contains("SameSite=Strict"));
        assert!(header.contains("Path=/"));
        assert!(header.contains("Max-Age=43200"));
    }

    #[test]
    fn session_token_from_headers_rejects_absent_or_legacy_cookie() {
        let headers = HeaderMap::new();
        assert_eq!(session_token_from_headers(&headers), None);

        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::COOKIE,
            HeaderValue::from_static("signaldb_session=eyJhcGlfa2V5IjoibGVnYWN5In0"),
        );
        assert_eq!(session_token_from_headers(&headers), None);
    }
}
