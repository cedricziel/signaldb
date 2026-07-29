//! # UI session cookie
//!
//! Encoding/decoding for the browser session cookie used by the embedded
//! explore UI. The cookie holds the same three values the auth headers
//! carry (API key, tenant, optional dataset) as base64-encoded JSON — it
//! is deliberately not encrypted: the key is client-held credential
//! material either way, and `HttpOnly` keeps it out of reach of page
//! scripts. The auth middleware falls back to this cookie when a request
//! carries no `Authorization` header.

use axum::http::HeaderMap;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

/// Name of the session cookie set by `POST /ui/session`.
pub const SESSION_COOKIE: &str = "signaldb_session";

/// Credential triple stored in the session cookie.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionData {
    /// Raw tenant API key (what `Authorization: Bearer` would carry).
    pub api_key: String,
    /// Tenant ID (what `X-Tenant-ID` would carry).
    pub tenant: String,
    /// Optional dataset ID (what `X-Dataset-ID` would carry).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset: Option<String>,
}

/// Encode session data into the cookie value (URL-safe base64 JSON).
pub fn encode_session(data: &SessionData) -> String {
    let json = serde_json::to_vec(data).expect("SessionData serializes to JSON");
    URL_SAFE_NO_PAD.encode(json)
}

/// Decode a cookie value produced by [`encode_session`].
///
/// Returns `None` for anything malformed — an invalid cookie is treated
/// as absent, never as an error the client must handle.
pub fn decode_session(value: &str) -> Option<SessionData> {
    let bytes = URL_SAFE_NO_PAD.decode(value.trim()).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Extract session data from a request's `Cookie` header(s), if present
/// and well-formed.
pub fn session_from_headers(headers: &HeaderMap) -> Option<SessionData> {
    for header in headers.get_all(axum::http::header::COOKIE) {
        let Ok(cookies) = header.to_str() else {
            continue;
        };
        for pair in cookies.split(';') {
            let Some((name, value)) = pair.split_once('=') else {
                continue;
            };
            if name.trim() == SESSION_COOKIE
                && let Some(session) = decode_session(value)
            {
                return Some(session);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn sample() -> SessionData {
        SessionData {
            api_key: "sk-test-key".to_string(),
            tenant: "acme".to_string(),
            dataset: Some("production".to_string()),
        }
    }

    #[test]
    fn encode_decode_round_trip() {
        let data = sample();
        assert_eq!(decode_session(&encode_session(&data)), Some(data));
    }

    #[test]
    fn round_trip_without_dataset() {
        let data = SessionData {
            dataset: None,
            ..sample()
        };
        assert_eq!(decode_session(&encode_session(&data)), Some(data));
    }

    #[test]
    fn decode_rejects_garbage() {
        assert_eq!(decode_session("not base64 at all!!"), None);
        // Valid base64, invalid JSON payload.
        assert_eq!(decode_session(&URL_SAFE_NO_PAD.encode("not json")), None);
    }

    #[test]
    fn session_from_headers_finds_cookie_among_others() {
        let mut headers = HeaderMap::new();
        let value = format!(
            "theme=dark; {SESSION_COOKIE}={}; other=1",
            encode_session(&sample())
        );
        headers.insert(
            axum::http::header::COOKIE,
            HeaderValue::from_str(&value).unwrap(),
        );
        assert_eq!(session_from_headers(&headers), Some(sample()));
    }

    #[test]
    fn session_from_headers_absent_or_malformed() {
        let headers = HeaderMap::new();
        assert_eq!(session_from_headers(&headers), None);

        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::COOKIE,
            HeaderValue::from_static("signaldb_session=%%%broken%%%"),
        );
        assert_eq!(session_from_headers(&headers), None);
    }
}
