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
//! `[self_monitoring.frontend]`. The UI's `index.html` loads it (blocking,
//! classic script) before the app boots, so browser telemetry export can be
//! enabled and pointed at any endpoint via config alone — one container image
//! serves every deployment without a UI rebuild.

use axum::Router;
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use common::config::FrontendMonitoringConfig;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tower_http::services::{ServeDir, ServeFile};

const UI_DIR_ENV: &str = "SIGNALDB_UI_DIR";

/// Build the `/ui` service from the `SIGNALDB_UI_DIR` environment variable.
///
/// # Panics
///
/// Panics at startup when the variable is set but the directory does not
/// contain a built UI (`index.html`). Misconfiguration must fail loudly
/// instead of degrading to the placeholder page.
pub fn service_from_env(frontend: &FrontendMonitoringConfig) -> Router {
    let dir = std::env::var(UI_DIR_ENV).ok().map(PathBuf::from);
    service_with_dir(dir, frontend).unwrap_or_else(|e| panic!("{e}"))
}

/// Build the `/ui` service for an explicit asset directory.
///
/// `None` serves the placeholder page (UI explicitly not bundled). A
/// directory without an `index.html` is a configuration error. The
/// `runtime-config.js` route is served in all cases so the UI (or a dev
/// server proxying to it) can always read its runtime configuration.
pub fn service_with_dir(
    dir: Option<PathBuf>,
    frontend: &FrontendMonitoringConfig,
) -> anyhow::Result<Router> {
    let assets = match dir {
        Some(dir) if has_ui_assets(&dir) => {
            tracing::info!(dir = %dir.display(), "Serving explore UI");
            let index = ServeFile::new(dir.join("index.html"));
            // Unknown paths fall back to index.html so SPA deep links work.
            Router::new().fallback_service(ServeDir::new(&dir).fallback(index))
        }
        Some(dir) => anyhow::bail!(
            "{UI_DIR_ENV} is set to {} but the directory contains no index.html; \
             build the UI first (pnpm ui:build) or unset {UI_DIR_ENV}",
            dir.display()
        ),
        None => Router::new().fallback(placeholder),
    };

    // The exact route wins over the static-asset fallback below it.
    let body: Arc<str> = Arc::from(runtime_config_js(frontend));
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
/// Emits `{ telemetry: { enabled: false } }` (and, crucially, no `apiKey`)
/// unless export is enabled with an endpoint set. Values are JSON-encoded for
/// safe escaping.
fn runtime_config_js(frontend: &FrontendMonitoringConfig) -> String {
    let telemetry = if frontend.enabled && !frontend.endpoint.is_empty() {
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
    let payload = serde_json::json!({ "telemetry": telemetry });
    format!("window.__SIGNALDB_RUNTIME_CONFIG__ = {payload};\n")
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

    #[tokio::test]
    async fn serves_index_and_assets_from_dir() {
        let dir = ui_fixture();
        let router =
            service_with_dir(Some(dir.path().to_path_buf()), &disabled()).expect("valid dir");

        let res = get(router.clone(), "/").await;
        assert_eq!(res.status(), StatusCode::OK);
        assert!(body_string(res).await.contains("ui-index"));

        let res = get(router, "/assets/app.js").await;
        assert_eq!(res.status(), StatusCode::OK);
        assert!(body_string(res).await.contains("console.log"));
    }

    #[tokio::test]
    async fn falls_back_to_index_for_unknown_paths() {
        let dir = ui_fixture();
        let router =
            service_with_dir(Some(dir.path().to_path_buf()), &disabled()).expect("valid dir");
        let res = get(router, "/some/deep/link").await;
        assert_eq!(res.status(), StatusCode::OK);
        assert!(body_string(res).await.contains("ui-index"));
    }

    #[tokio::test]
    async fn placeholder_without_assets() {
        let router = service_with_dir(None, &disabled()).expect("no dir is valid");
        let res = get(router, "/").await;
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        assert!(body_string(res).await.contains("UI not bundled"));
    }

    #[test]
    fn configured_dir_without_index_is_an_error() {
        let empty = tempfile::tempdir().expect("tempdir");
        let err = service_with_dir(Some(empty.path().to_path_buf()), &disabled())
            .expect_err("missing index.html must fail");
        assert!(err.to_string().contains("index.html"));
    }

    #[tokio::test]
    async fn runtime_config_disabled_by_default_and_omits_key() {
        let dir = ui_fixture();
        let router =
            service_with_dir(Some(dir.path().to_path_buf()), &disabled()).expect("valid dir");
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
        let router = service_with_dir(None, &frontend).expect("no dir is valid");
        let body = body_string(get(router, "/runtime-config.js").await).await;
        assert!(body.contains("\"enabled\":true"));
        assert!(body.contains("http://signaldb.example:4318"));
        assert!(body.contains("sk-ingest"));
        assert!(body.contains("\"tenantId\":\"_system\""));
        assert!(body.contains("\"serviceName\":\"signaldb-ui\""));
    }

    #[test]
    fn runtime_config_stays_disabled_when_endpoint_missing() {
        // enabled but no endpoint => nothing to export to; treat as disabled.
        let frontend = FrontendMonitoringConfig {
            enabled: true,
            api_key: Some("sk-ingest".to_string()),
            ..FrontendMonitoringConfig::default()
        };
        let js = runtime_config_js(&frontend);
        assert!(js.contains("\"enabled\":false"));
        assert!(!js.contains("sk-ingest"), "no key leaked: {js}");
    }

    #[test]
    fn has_ui_assets_checks_for_index() {
        let dir = ui_fixture();
        assert!(has_ui_assets(dir.path()));
        let empty = tempfile::tempdir().expect("tempdir");
        assert!(!has_ui_assets(empty.path()));
    }
}
