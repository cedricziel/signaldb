//! # MCP Apps extension
//!
//! Serves interactive UI resources alongside the tool surface, per the
//! [MCP Apps extension] (SEP-1865, stable revision `2026-01-26`). A UI-capable
//! host renders the app in a sandboxed iframe instead of showing the tool's raw
//! JSON; every other client is unaffected and keeps reading the text result.
//!
//! The server side of the extension is deliberately small:
//!
//! - a tool carries `_meta.ui.resourceUri` pointing at a `ui://` resource
//! - that resource is served over `resources/read` as
//!   `text/html;profile=mcp-app`
//! - the host does the rest — it fetches the HTML, sandboxes it, and forwards
//!   the tool result to the iframe as a `ui/notifications/tool-result`
//!   notification
//!
//! The whole `ui/*` dialect runs between the host and the iframe; this server
//! never sees one of those methods.
//!
//! ## Negotiation
//!
//! There is no server-side capability to advertise. The *client* declares
//! support in its `initialize` request under the extension identifier
//! [`UI_EXTENSION_ID`], and the spec asks servers to check that before
//! attaching UI metadata. [`client_supports_ui`] performs that check, so a
//! client that never asked for apps sees plain tools.
//!
//! ## Self-containment
//!
//! Hosts apply a deny-by-default CSP (`default-src 'none'`, notably
//! `connect-src 'none'`) when a resource declares no `_meta.ui.csp`. Every app
//! here is therefore a single self-contained HTML document — inline CSS and JS,
//! no external fetches — which keeps the strictest possible sandbox and means
//! the app can never reach the router on its own. Its only data is the tool
//! result the host hands it.
//!
//! [MCP Apps extension]: https://github.com/modelcontextprotocol/ext-apps/blob/main/specification/2026-01-26/apps.mdx

use rmcp::model::{ClientCapabilities, MetaObject, Resource, ResourceContents};

/// Extension identifier a client declares in its `initialize` capabilities to
/// negotiate MCP Apps (SEP-1724 extensions map).
pub const UI_EXTENSION_ID: &str = "io.modelcontextprotocol/ui";

/// MIME type for MCP Apps UI resources. The `;profile=` parameter is part of
/// the type — hosts match on it exactly.
pub const UI_MIME_TYPE: &str = "text/html;profile=mcp-app";

/// URI of the single-trace waterfall app rendered for `get_trace`.
pub const TRACE_APP_URI: &str = "ui://signaldb/trace";

/// URI of the single-profile flamegraph app rendered for `get_profile`.
pub const PROFILE_APP_URI: &str = "ui://signaldb/profile";

/// A UI resource this server can serve.
struct UiApp {
    /// `ui://` URI the tool's `_meta.ui.resourceUri` points at.
    uri: &'static str,
    /// Programmatic resource name.
    name: &'static str,
    /// Human-readable title for hosts that list resources.
    title: &'static str,
    /// What the app renders.
    description: &'static str,
    /// The complete, self-contained HTML5 document.
    html: &'static str,
}

/// Every app this server serves. Compiled in, so the binary carries its UI and
/// there is no runtime asset path to configure or fail to find.
const APPS: &[UiApp] = &[
    UiApp {
        uri: TRACE_APP_URI,
        name: "trace-waterfall",
        title: "Trace waterfall",
        description: "Interactive waterfall for a single trace: span timings, self time vs. \
                      time in child spans, per-span attributes, and span events.",
        html: include_str!("../ui/trace.html"),
    },
    UiApp {
        uri: PROFILE_APP_URI,
        name: "profile-flamegraph",
        title: "Profile flamegraph",
        description: "Interactive flamegraph for a single profile: per-frame self/total sample \
                      values across the call stack.",
        html: include_str!("../ui/profile.html"),
    },
];

/// Whether the connected client negotiated the MCP Apps extension *and* can
/// render what this server serves.
///
/// `mimeTypes` is a required setting on the extension capability, and it is the
/// client's statement of what it can display. Declaring the extension without
/// naming [`UI_MIME_TYPE`] is not an offer this server can fill — handing such a
/// client a `ui://` resource it cannot render would leave it with a tool it
/// cannot display and no text to fall back on. So the check requires the MIME
/// type, and anything short of that keeps the plain-text tool surface.
///
/// Per the spec, servers SHOULD check this before registering UI-enabled tools.
pub fn client_supports_ui(capabilities: &ClientCapabilities) -> bool {
    capabilities
        .extensions
        .as_ref()
        .and_then(|extensions| extensions.get(UI_EXTENSION_ID))
        .and_then(|settings| settings.get("mimeTypes"))
        .and_then(|mime_types| mime_types.as_array())
        .is_some_and(|mime_types| {
            mime_types
                .iter()
                .any(|mime_type| mime_type.as_str() == Some(UI_MIME_TYPE))
        })
}

/// Build the `_meta` object linking a tool to its UI resource:
/// `{"ui": {"resourceUri": "ui://…"}}`.
///
/// The nested `ui.resourceUri` form is the current spec shape; the flat
/// `_meta["ui/resourceUri"]` key it replaced is deprecated and not emitted.
pub fn tool_ui_meta(resource_uri: &str) -> MetaObject {
    let mut meta = MetaObject::new();
    meta.insert(
        "ui".to_string(),
        serde_json::json!({ "resourceUri": resource_uri }),
    );
    meta
}

/// The UI resources this server exposes, for `resources/list`.
///
/// Listing is optional under the extension — hosts discover apps through tool
/// metadata — but advertising them lets a host review or prefetch the templates
/// at connect time.
pub fn ui_resources() -> Vec<Resource> {
    APPS.iter()
        .map(|app| {
            Resource::new(app.uri, app.name)
                .with_title(app.title)
                .with_description(app.description)
                .with_mime_type(UI_MIME_TYPE)
                .with_size(app.html.len() as u64)
        })
        .collect()
}

/// Resolve a `ui://` URI to its contents for `resources/read`, or `None` when
/// no app is registered under that URI.
pub fn read_ui_resource(uri: &str) -> Option<ResourceContents> {
    APPS.iter()
        .find(|app| app.uri == uri)
        .map(|app| ResourceContents::text(app.html, app.uri).with_mime_type(UI_MIME_TYPE))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::model::ExtensionCapabilities;

    /// Client capabilities declaring `extension` with the given settings.
    fn capabilities_with(
        extension: Option<&str>,
        settings: serde_json::Value,
    ) -> ClientCapabilities {
        let mut capabilities = ClientCapabilities::default();
        if let Some(id) = extension {
            let mut extensions = ExtensionCapabilities::new();
            extensions.insert(
                id.to_string(),
                settings
                    .as_object()
                    .expect("capability settings are an object")
                    .clone(),
            );
            capabilities.extensions = Some(extensions);
        }
        capabilities
    }

    /// Settings a conformant MCP Apps client sends.
    fn ui_settings() -> serde_json::Value {
        serde_json::json!({ "mimeTypes": [UI_MIME_TYPE] })
    }

    #[test]
    fn ui_support_follows_the_client_declaration() {
        assert!(client_supports_ui(&capabilities_with(
            Some(UI_EXTENSION_ID),
            ui_settings()
        )));
        assert!(!client_supports_ui(&capabilities_with(
            None,
            serde_json::json!({})
        )));
        assert!(!client_supports_ui(&capabilities_with(
            Some("io.modelcontextprotocol/tasks"),
            ui_settings()
        )));
    }

    /// Declaring the extension is not enough: the client must also say it can
    /// render the type this server serves.
    #[test]
    fn ui_support_requires_the_apps_mime_type() {
        // `mimeTypes` omitted entirely — the setting is required by the spec.
        assert!(!client_supports_ui(&capabilities_with(
            Some(UI_EXTENSION_ID),
            serde_json::json!({})
        )));
        // Declared but empty.
        assert!(!client_supports_ui(&capabilities_with(
            Some(UI_EXTENSION_ID),
            serde_json::json!({ "mimeTypes": [] })
        )));
        // Renders HTML, but not the MCP Apps profile.
        assert!(!client_supports_ui(&capabilities_with(
            Some(UI_EXTENSION_ID),
            serde_json::json!({ "mimeTypes": ["text/html"] })
        )));
        // Ours among others is a match.
        assert!(client_supports_ui(&capabilities_with(
            Some(UI_EXTENSION_ID),
            serde_json::json!({ "mimeTypes": ["text/plain", UI_MIME_TYPE] })
        )));
    }

    #[test]
    fn tool_meta_uses_the_nested_ui_shape() {
        let meta = tool_ui_meta(TRACE_APP_URI);
        let value = serde_json::to_value(&meta).expect("meta serializes");
        assert_eq!(value["ui"]["resourceUri"], TRACE_APP_URI);
        // The deprecated flat key must not be emitted.
        assert!(value.get("ui/resourceUri").is_none());
    }

    #[test]
    fn trace_app_is_listed_with_the_apps_mime_type() {
        let resources = ui_resources();
        let trace = resources
            .iter()
            .find(|r| r.uri == TRACE_APP_URI)
            .expect("trace app is listed");
        assert_eq!(trace.mime_type.as_deref(), Some(UI_MIME_TYPE));
        assert_eq!(trace.name, "trace-waterfall");
        assert!(trace.size.is_some_and(|size| size > 0));
    }

    #[test]
    fn reading_the_trace_app_returns_the_html_document() {
        let contents = read_ui_resource(TRACE_APP_URI).expect("trace app is readable");
        let ResourceContents::TextResourceContents {
            uri,
            mime_type,
            text,
            ..
        } = contents
        else {
            panic!("UI resources are served as text, not blobs");
        };
        assert_eq!(uri, TRACE_APP_URI);
        assert_eq!(mime_type.as_deref(), Some(UI_MIME_TYPE));
        assert!(
            text.starts_with("<!doctype html>"),
            "must be an HTML5 document"
        );
    }

    #[test]
    fn unknown_ui_uri_is_not_served() {
        assert!(read_ui_resource("ui://signaldb/nope").is_none());
        assert!(read_ui_resource("file:///etc/passwd").is_none());
    }

    /// The default host CSP is `default-src 'none'` with `connect-src 'none'`,
    /// so an app that reaches for an external origin silently renders blank.
    /// Apps must stay single-document and self-contained.
    #[test]
    fn apps_are_self_contained() {
        for app in APPS {
            for needle in [
                "src=\"http",
                "href=\"http",
                "@import",
                "fetch(",
                "XMLHttpRequest",
            ] {
                assert!(
                    !app.html.contains(needle),
                    "app `{}` references `{needle}`, which the host CSP blocks",
                    app.name
                );
            }
        }
    }

    /// The app is useless if it never completes the host handshake or never
    /// listens for the payload it renders.
    #[test]
    fn trace_app_speaks_the_ui_dialect() {
        let html = read_ui_resource(TRACE_APP_URI)
            .and_then(|contents| match contents {
                ResourceContents::TextResourceContents { text, .. } => Some(text),
                _ => None,
            })
            .expect("trace app is readable");
        for method in [
            "ui/initialize",
            "ui/notifications/initialized",
            "ui/notifications/tool-result",
            "ui/notifications/size-changed",
        ] {
            assert!(html.contains(method), "app must handle `{method}`");
        }
    }

    #[test]
    fn profile_app_is_listed_with_the_apps_mime_type() {
        let resources = ui_resources();
        let profile = resources
            .iter()
            .find(|r| r.uri == PROFILE_APP_URI)
            .expect("profile app is listed");
        assert_eq!(profile.mime_type.as_deref(), Some(UI_MIME_TYPE));
        assert_eq!(profile.name, "profile-flamegraph");
        assert!(profile.size.is_some_and(|size| size > 0));
    }

    #[test]
    fn reading_the_profile_app_returns_the_html_document() {
        let contents = read_ui_resource(PROFILE_APP_URI).expect("profile app is readable");
        let ResourceContents::TextResourceContents {
            uri,
            mime_type,
            text,
            ..
        } = contents
        else {
            panic!("UI resources are served as text, not blobs");
        };
        assert_eq!(uri, PROFILE_APP_URI);
        assert_eq!(mime_type.as_deref(), Some(UI_MIME_TYPE));
        assert!(
            text.starts_with("<!doctype html>"),
            "must be an HTML5 document"
        );
    }

    /// The app is useless if it never completes the host handshake or never
    /// listens for the payload it renders.
    #[test]
    fn profile_app_speaks_the_ui_dialect() {
        let html = read_ui_resource(PROFILE_APP_URI)
            .and_then(|contents| match contents {
                ResourceContents::TextResourceContents { text, .. } => Some(text),
                _ => None,
            })
            .expect("profile app is readable");
        for method in [
            "ui/initialize",
            "ui/notifications/initialized",
            "ui/notifications/tool-result",
            "ui/notifications/size-changed",
        ] {
            assert!(html.contains(method), "app must handle `{method}`");
        }
    }

    /// The flamegraph decoder must actually consume `names`/`levels`/`total`,
    /// not just parrot bridge boilerplate copied from trace.html.
    #[test]
    fn profile_app_renders_the_flamebearer_shape() {
        let html = read_ui_resource(PROFILE_APP_URI)
            .and_then(|contents| match contents {
                ResourceContents::TextResourceContents { text, .. } => Some(text),
                _ => None,
            })
            .expect("profile app is readable");
        for needle in ["flamegraph.names", "flamegraph.levels", "offsetDelta"] {
            assert!(html.contains(needle), "app must decode `{needle}`");
        }
    }
}
