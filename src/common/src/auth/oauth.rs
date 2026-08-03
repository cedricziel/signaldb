//! # OAuth 2.1 token primitives
//!
//! Opaque high-entropy values for SignalDB's OAuth authorization server:
//! access tokens, refresh tokens, and authorization codes. Every value is a
//! prefixed, URL-safe base64 random string; only its SHA-256 digest is stored
//! server-side (see [`hash_oauth_token`]), mirroring the browser session-token
//! pattern in [`crate::auth::password`]. The raw value is never reconstructable
//! from stored state.
//!
//! Also provides [`verify_pkce_s256`], the PKCE (RFC 7636) `S256` check the
//! token endpoint runs when redeeming an authorization code.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rand::TryRng;
use sha2::{Digest, Sha256};

/// Random bytes in an opaque value before base64 encoding (256 bits).
const TOKEN_BYTES: usize = 32;

/// Prefix for OAuth access tokens.
pub const ACCESS_TOKEN_PREFIX: &str = "sdb_at_";
/// Prefix for OAuth refresh tokens.
pub const REFRESH_TOKEN_PREFIX: &str = "sdb_rt_";
/// Prefix for OAuth authorization codes.
pub const AUTH_CODE_PREFIX: &str = "sdb_ac_";

/// Kind of opaque OAuth value to mint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
    /// A short-lived access token presented on each MCP request.
    Access,
    /// A long-lived refresh token exchanged for new access tokens.
    Refresh,
    /// A single-use authorization code exchanged at the token endpoint.
    AuthorizationCode,
}

impl TokenKind {
    /// The identifying prefix for this kind.
    pub fn prefix(self) -> &'static str {
        match self {
            TokenKind::Access => ACCESS_TOKEN_PREFIX,
            TokenKind::Refresh => REFRESH_TOKEN_PREFIX,
            TokenKind::AuthorizationCode => AUTH_CODE_PREFIX,
        }
    }
}

/// Mint a new opaque OAuth value of the given kind: its prefix followed by 32
/// bytes of OS randomness, URL-safe base64 without padding. Return the raw
/// value to hand to the client exactly once; store only [`hash_oauth_token`]
/// of it.
///
/// # Panics
///
/// Panics if the OS random number generator fails — a predictable token would
/// be a security vulnerability, and RNG failure is not recoverable here.
pub fn generate_oauth_token(kind: TokenKind) -> String {
    let mut bytes = [0u8; TOKEN_BYTES];
    if let Err(error) = rand::rngs::SysRng.try_fill_bytes(&mut bytes) {
        panic!("OS random number generator failed while generating OAuth token: {error}");
    }
    let encoded = URL_SAFE_NO_PAD.encode(bytes);
    format!("{}{encoded}", kind.prefix())
}

/// Hash an opaque OAuth value with SHA-256, returning lowercase hex.
///
/// Deterministic, so a presented value is looked up by hashing it. Same shape
/// as [`crate::auth::hash_session_token`].
pub fn hash_oauth_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}

/// Verify a PKCE `code_verifier` against a stored `S256` `code_challenge`
/// (RFC 7636): the challenge must equal `BASE64URL-NO-PAD(SHA256(verifier))`.
pub fn verify_pkce_s256(verifier: &str, challenge: &str) -> bool {
    let mut hasher = Sha256::new();
    hasher.update(verifier.as_bytes());
    let computed = URL_SAFE_NO_PAD.encode(hasher.finalize());
    // Constant-time-ish comparison is unnecessary here: the challenge is
    // public (sent in the clear during /authorize) and the verifier is the
    // caller's own secret, so there is no cross-party secret to leak by timing.
    computed == challenge
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn generated_tokens_carry_their_prefix_and_decode_to_32_bytes() {
        for kind in [
            TokenKind::Access,
            TokenKind::Refresh,
            TokenKind::AuthorizationCode,
        ] {
            let token = generate_oauth_token(kind);
            assert!(
                token.starts_with(kind.prefix()),
                "missing prefix for {kind:?}"
            );
            let raw = URL_SAFE_NO_PAD
                .decode(&token[kind.prefix().len()..])
                .expect("body is url-safe base64");
            assert_eq!(raw.len(), 32);
        }
    }

    #[test]
    fn generated_tokens_are_unique() {
        let tokens: HashSet<String> = (0..100)
            .map(|_| generate_oauth_token(TokenKind::Access))
            .collect();
        assert_eq!(tokens.len(), 100);
    }

    #[test]
    fn hash_is_deterministic_hex_sha256_and_differs_between_tokens() {
        let token = generate_oauth_token(TokenKind::Refresh);
        let h1 = hash_oauth_token(&token);
        let h2 = hash_oauth_token(&token);
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64);
        assert!(h1.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(
            hash_oauth_token(&generate_oauth_token(TokenKind::Refresh)),
            hash_oauth_token(&generate_oauth_token(TokenKind::Refresh))
        );
    }

    #[test]
    fn pkce_s256_accepts_a_matching_verifier() {
        // Canonical RFC 7636 Appendix B example.
        let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        let challenge = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";
        assert!(verify_pkce_s256(verifier, challenge));
    }

    #[test]
    fn pkce_s256_rejects_a_mismatched_verifier() {
        let challenge = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";
        assert!(!verify_pkce_s256("not-the-verifier", challenge));
    }
}
