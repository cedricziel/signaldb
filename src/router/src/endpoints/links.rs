//! Hypermedia links for first-party resources (http-api skill, rule 4).
//!
//! `href`s are built from [`API_V1`], the same constant `create_router`
//! nests the versioned API under, so a link can't drift from its route.

use serde::{Deserialize, Serialize};

/// The versioned API root every first-party resource is mounted under.
pub const API_V1: &str = "/api/v1";

/// A hypermedia link. `method` is omitted for `GET`.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct Link {
    pub href: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
}

impl Link {
    /// A `GET` link.
    pub fn get(href: String) -> Self {
        Self { href, method: None }
    }

    /// A link to call with `method` (`POST`, `PUT`, `DELETE`, ...).
    pub fn with_method(href: String, method: &str) -> Self {
        Self {
            href,
            method: Some(method.to_string()),
        }
    }
}
