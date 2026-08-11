//! # Explore UI serving
//!
//! Serves the built SignalDB UI (a static SPA from `src/ui`) under `/ui`.
//! The asset directory is provided at runtime via `SIGNALDB_UI_DIR` so cargo
//! builds never depend on a Node toolchain; container images copy the built
//! assets in and set the variable. An unset variable serves a short
//! placeholder page; a set-but-invalid directory fails startup so a broken
//! deployment cannot silently ship without its UI.
//!
//! ## Runtime configuration
//!
//! `GET /ui/runtime-config.js` sets `window.__SIGNALDB_RUNTIME_CONFIG__` from
//! `[self_monitoring.frontend]`, plus `service.namespace` / the running
//! router's own `service.version` / `deployment.environment.name` (mirroring
//! [`common::self_monitoring`]'s backend resource, so the UI's `Resource`
//! carries the same deployment identity, not just the tenant/dataset-ingest
//! bits `FrontendMonitoringConfig` covers). The UI's `index.html` loads it
//! (blocking, classic script) before the app boots, so browser telemetry
//! export can be enabled and pointed at any endpoint via config alone — one
//! container image serves every deployment without a UI rebuild.
//!
//! ## Trace context on the document response
//!
//! `index.html` is never served as a static file: [`serve_index_html`] reads
//! it once at startup and, on every request, injects the current server
//! span's context as `<meta name="traceparent" content="...">` before
//! `</head>`. The initial document request is the one call the browser can
//! never instrument (no JS has run yet), so this is the frontend's most
//! reliable way to link its `documentLoad` span to the server span that
//! served the page — no dependency on the Performance API surfacing
//! `Server-Timing` (browser/proxy support varies). See
//! `docs/users/response-trace-context.md`.

use anyhow::Context;
use axum::Router;
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use common::config::FrontendMonitoringConfig;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tower_http::services::ServeDir;

const UI_DIR_ENV: &str = "SIGNALDB_UI_DIR";

/// Build the `/ui` service from the `SIGNALDB_UI_DIR` environment variable.
///
/// # Panics
///
/// Panics at startup when the variable is set but the directory does not
/// contain a built UI (`index.html`). Misconfiguration must fail loudly
/// instead of degrading to the placeholder page.
pub fn service_from_env(frontend: &FrontendMonitoringConfig, environment: &str) -> Router {
    let dir = std::env::var(UI_DIR_ENV).ok().map(PathBuf::from);
    service_with_dir(dir, frontend, environment).unwrap_or_else(|e| panic!("{e}"))
}

/// Build the `/ui` service for an explicit asset directory.
///
/// `None` serves the placeholder page (UI explicitly not bundled). A
/// directory without an `index.html` is a configuration error. The
/// `runtime-config.js` route is served in all cases so the UI (or a dev
/// server proxying to it) can always read its runtime configuration.
/// `environment` is `[self_monitoring].environment`, the same
/// `deployment.environment.name` value the backend's own telemetry uses.
pub fn service_with_dir(
    dir: Option<PathBuf>,
    frontend: &FrontendMonitoringConfig,
    environment: &str,
) -> anyhow::Result<Router> {
    let assets = match dir {
        Some(dir) if has_ui_assets(&dir) => {
            tracing::info!(dir = %dir.display(), "Serving explore UI");
            let template: Arc<str> = Arc::from(
                std::fs::read_to_string(dir.join("index.html")).with_context(|| {
                    format!("Failed to read {}", dir.join("index.html").display())
                })?,
            );
            let index = get(move || {
                let template = template.clone();
                async move { serve_index_html(&template) }
            });
            // Unknown paths fall back to index.html so SPA deep links work.
            // Index-on-directory serving is disabled so every path that
            // resolves to index.html — including `/` — goes through our
            // handler and gets the trace-context meta tag, not ServeDir's
            // built-in static passthrough.
            Router::new().fallback_service(
                ServeDir::new(&dir)
                    .append_index_html_on_directories(false)
                    .fallback(index),
            )
        }
        Some(dir) => anyhow::bail!(
            "{UI_DIR_ENV} is set to {} but the directory contains no index.html; \
             build the UI first (pnpm ui:build) or unset {UI_DIR_ENV}",
            dir.display()
        ),
        None => Router::new().fallback(placeholder),
    };

    // The exact route wins over the static-asset fallback below it.
    let body: Arc<str> = Arc::from(runtime_config_js(frontend, environment));
    Ok(Router::new()
        .route(
            "/runtime-config.js",
            get(move || {
                let body = body.clone();
                async move {
                    (
                        [
                            (
                                header::CONTENT_TYPE,
                                "application/javascript; charset=utf-8",
                            ),
                            // Config may carry an ingest key and can change
                            // between deploys — never let a proxy cache it.
                            (header::CACHE_CONTROL, "no-store"),
                        ],
                        body.to_string(),
                    )
                }
            }),
        )
        .fallback_service(assets))
}

/// Render the runtime-config script from the frontend telemetry config.
///
/// The export-credential fields (`endpoint`/`apiKey`/`tenantId`/`datasetId`/
/// `serviceName`) are emitted only when export is enabled with an endpoint
/// set — `{ telemetry: { enabled: false } }` (and, crucially, no `apiKey`)
/// otherwise. `namespace`/`version`/`deploymentEnvironment` are deployment
/// identity, not export credentials, so they are always included: they mirror
/// the `(service.namespace, service.version, deployment.environment.name)`
/// facts [`common::self_monitoring`]'s backend resource carries, letting the
/// UI's `Resource` (see `telemetry/resource.ts`) describe the same
/// deployment even when browser export itself is off. Values are
/// JSON-encoded for safe escaping.
fn runtime_config_js(frontend: &FrontendMonitoringConfig, environment: &str) -> String {
    let mut telemetry = if frontend.enabled && !frontend.endpoint.is_empty() {
        serde_json::json!({
            "enabled": true,
            "endpoint": frontend.endpoint,
            "apiKey": frontend.api_key,
            "tenantId": frontend.tenant_id,
            "datasetId": frontend.dataset_id,
            "serviceName": frontend.service_name,
        })
    } else {
        serde_json::json!({ "enabled": false })
    };
    // Mirrors common::self_monitoring::build_resource's hardcoded
    // "signaldb" service.namespace literal.
    telemetry["namespace"] = serde_json::json!("signaldb");
    telemetry["version"] = serde_json::json!(env!("CARGO_PKG_VERSION"));
    telemetry["deploymentEnvironment"] = serde_json::json!(environment);
    let payload = serde_json::json!({ "telemetry": telemetry });
    format!("window.__SIGNALDB_RUNTIME_CONFIG__ = {payload};\n")
}

/// Render `index.html` with the current server span's trace context injected
/// as a `<meta name="traceparent">` tag. No-op (raw template) when
/// self-monitoring is disabled or the span was sampled out, matching
/// [`common::flight::trace_context::current_trace_context_fields`]'s
/// documented degrade-to-`None` behavior.
fn serve_index_html(template: &str) -> impl IntoResponse + use<> {
    let html = match common::flight::trace_context::current_trace_context_fields() {
        Some((traceparent, _tracestate)) => inject_traceparent_meta(template, &traceparent),
        None => template.to_string(),
    };
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], html)
}

/// Insert `<meta name="traceparent" content="<traceparent>">` immediately
/// before `</head>`. `traceparent` always comes from `format_traceparent`
/// (hex digits and hyphens only), so no HTML escaping is required. Returns
/// `html` unchanged if it has no `</head>` to anchor on.
fn inject_traceparent_meta(html: &str, traceparent: &str) -> String {
    let Some(pos) = html.find("</head>") else {
        return html.to_string();
    };
    let mut out = String::with_capacity(html.len() + traceparent.len() + 48);
    out.push_str(&html[..pos]);
    out.push_str("<meta name=\"traceparent\" content=\"");
    out.push_str(traceparent);
    out.push_str("\">\n");
    out.push_str(&html[pos..]);
    out
}

async fn placeholder() -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        Html(
            "<!doctype html><title>SignalDB UI</title>\
             <body style=\"font-family:system-ui;max-width:38rem;margin:4rem auto\">\
             <h1>UI not bundled</h1>\
             <p>This SignalDB build has no explore UI assets. Build them with\
             <code>pnpm ui:build</code> and point <code>SIGNALDB_UI_DIR</code>\
             at <code>src/ui/dist</code>, or use the container image, which\
             ships them preinstalled.</p></body>",
        ),
    )
}

/// True when `dir` looks like a built UI bundle (used by callers for logs).
pub fn has_ui_assets(dir: &Path) -> bool {
    dir.join("index.html").is_file()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    async fn body_string(res: axum::response::Response) -> String {
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .expect("read body");
        String::from_utf8_lossy(&bytes).to_string()
    }

    fn ui_fixture() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("index.html"), "<html>ui-index</html>")
            .expect("write index");
        std::fs::create_dir(dir.path().join("assets")).expect("mkdir assets");
        std::fs::write(dir.path().join("assets/app.js"), "console.log(1)").expect("write asset");
        dir
    }

    async fn get(router: Router, path: &str) -> axum::response::Response {
        router
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    fn disabled() -> FrontendMonitoringConfig {
        FrontendMonitoringConfig::default()
    }

    const ENV: &str = "test";

    #[tokio::test]
    async fn serves_index_and_assets_from_dir() {
        let dir = ui_fixture();
        let router =
            service_with_dir(Some(dir.path().to_path_buf()), &disabled(), ENV).expect("valid dir");

        let res = get(router.clone(), "/").await;
        assert_eq!(res.status(), StatusCode::OK);
        assert!(body_string(res).await.contains("ui-index"));

        let res = get(router, "/assets/app.js").await;
        assert_eq!(res.status(), StatusCode::OK);
        assert!(body_string(res).await.contains("console.log"));
    }

    #[tokio::test]
    async fn index_has_no_traceparent_meta_when_self_monitoring_disabled() {
        // No global OTel layer is installed in this test process, so
        // current_trace_context_fields() returns None regardless of the
        // FrontendMonitoringConfig passed in — mirrors
        // common::flight::trace_context's
        // current_trace_context_fields_is_none_without_otel_layer test.
        let dir = ui_fixture();
        let router =
            service_with_dir(Some(dir.path().to_path_buf()), &disabled(), ENV).expect("valid dir");
        let res = get(router, "/").await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/html; charset=utf-8"
        );
        let body = body_string(res).await;
        assert_eq!(body, "<html>ui-index</html>", "template served unchanged");
        assert!(!body.contains("traceparent"));
    }

    #[test]
    fn inject_traceparent_meta_inserts_before_head_close() {
        let html = "<html><head><title>t</title></head><body></body></html>";
        let out = inject_traceparent_meta(
            html,
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        );
        assert_eq!(
            out,
            "<html><head><title>t</title>\
             <meta name=\"traceparent\" content=\"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01\">\n\
             </head><body></body></html>"
        );
    }

    #[test]
    fn inject_traceparent_meta_leaves_html_unchanged_without_head_close() {
        let html = "<html><body>no head tag</body></html>";
        assert_eq!(
            inject_traceparent_meta(
                html,
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
            ),
            html
        );
    }

    #[tokio::test]
    async fn falls_back_to_index_for_unknown_paths() {
        let dir = ui_fixture();
        let router =
            service_with_dir(Some(dir.path().to_path_buf()), &disabled(), ENV).expect("valid dir");
        let res = get(router, "/some/deep/link").await;
        assert_eq!(res.status(), StatusCode::OK);
        assert!(body_string(res).await.contains("ui-index"));
    }

    #[tokio::test]
    async fn placeholder_without_assets() {
        let router = service_with_dir(None, &disabled(), ENV).expect("no dir is valid");
        let res = get(router, "/").await;
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        assert!(body_string(res).await.contains("UI not bundled"));
    }

    #[test]
    fn configured_dir_without_index_is_an_error() {
        let empty = tempfile::tempdir().expect("tempdir");
        let err = service_with_dir(Some(empty.path().to_path_buf()), &disabled(), ENV)
            .expect_err("missing index.html must fail");
        assert!(err.to_string().contains("index.html"));
    }

    #[tokio::test]
    async fn runtime_config_disabled_by_default_and_omits_key() {
        let dir = ui_fixture();
        let router =
            service_with_dir(Some(dir.path().to_path_buf()), &disabled(), ENV).expect("valid dir");
        let res = get(router, "/runtime-config.js").await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store"
        );
        let body = body_string(res).await;
        assert!(body.contains("window.__SIGNALDB_RUNTIME_CONFIG__"));
        assert!(body.contains("\"enabled\":false"));
        assert!(!body.contains("apiKey"), "no key when disabled: {body}");
    }

    #[tokio::test]
    async fn runtime_config_serves_endpoint_and_key_when_enabled() {
        let frontend = FrontendMonitoringConfig {
            enabled: true,
            endpoint: "http://signaldb.example:4318".to_string(),
            api_key: Some("sk-ingest".to_string()),
            ..FrontendMonitoringConfig::default()
        };
        // Enabled even with no UI bundle (None): dev proxies this route to a
        // live router while serving assets itself.
        let router = service_with_dir(None, &frontend, "staging").expect("no dir is valid");
        let body = body_string(get(router, "/runtime-config.js").await).await;
        assert!(body.contains("\"enabled\":true"));
        assert!(body.contains("http://signaldb.example:4318"));
        assert!(body.contains("sk-ingest"));
        assert!(body.contains("\"tenantId\":\"_system\""));
        assert!(body.contains("\"serviceName\":\"signaldb-ui\""));
        assert!(body.contains("\"namespace\":\"signaldb\""));
        assert!(body.contains(&format!("\"version\":\"{}\"", env!("CARGO_PKG_VERSION"))));
        assert!(body.contains("\"deploymentEnvironment\":\"staging\""));
    }

    #[test]
    fn runtime_config_stays_disabled_when_endpoint_missing() {
        // enabled but no endpoint => nothing to export to; treat as disabled.
        let frontend = FrontendMonitoringConfig {
            enabled: true,
            api_key: Some("sk-ingest".to_string()),
            ..FrontendMonitoringConfig::default()
        };
        let js = runtime_config_js(&frontend, ENV);
        assert!(js.contains("\"enabled\":false"));
        assert!(!js.contains("sk-ingest"), "no key leaked: {js}");
        // Deployment-identity fields are not export credentials — still
        // present even while telemetry export is disabled.
        assert!(js.contains("\"namespace\":\"signaldb\""));
        assert!(js.contains(&format!("\"version\":\"{}\"", env!("CARGO_PKG_VERSION"))));
        assert!(js.contains("\"deploymentEnvironment\":\"test\""));
    }

    #[test]
    fn has_ui_assets_checks_for_index() {
        let dir = ui_fixture();
        assert!(has_ui_assets(dir.path()));
        let empty = tempfile::tempdir().expect("tempdir");
        assert!(!has_ui_assets(empty.path()));
    }
}
