//! # Explore UI serving
//!
//! Serves the built SignalDB UI (a static SPA from `src/ui`) under `/ui`.
//! The asset directory is provided at runtime via `SIGNALDB_UI_DIR` so cargo
//! builds never depend on a Node toolchain; container images copy the built
//! assets in and set the variable. An unset variable serves a short
//! placeholder page; a set-but-invalid directory fails startup so a broken
//! deployment cannot silently ship without its UI.

use axum::Router;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use std::path::{Path, PathBuf};
use tower_http::services::{ServeDir, ServeFile};

const UI_DIR_ENV: &str = "SIGNALDB_UI_DIR";

/// Build the `/ui` service from the `SIGNALDB_UI_DIR` environment variable.
///
/// # Panics
///
/// Panics at startup when the variable is set but the directory does not
/// contain a built UI (`index.html`). Misconfiguration must fail loudly
/// instead of degrading to the placeholder page.
pub fn service_from_env() -> Router {
    let dir = std::env::var(UI_DIR_ENV).ok().map(PathBuf::from);
    service_with_dir(dir).unwrap_or_else(|e| panic!("{e}"))
}

/// Build the `/ui` service for an explicit asset directory.
///
/// `None` serves the placeholder page (UI explicitly not bundled). A
/// directory without an `index.html` is a configuration error.
pub fn service_with_dir(dir: Option<PathBuf>) -> anyhow::Result<Router> {
    match dir {
        Some(dir) if has_ui_assets(&dir) => {
            tracing::info!(dir = %dir.display(), "Serving explore UI");
            let index = ServeFile::new(dir.join("index.html"));
            // Unknown paths fall back to index.html so SPA deep links work.
            Ok(Router::new().fallback_service(ServeDir::new(&dir).fallback(index)))
        }
        Some(dir) => anyhow::bail!(
            "{UI_DIR_ENV} is set to {} but the directory contains no index.html; \
             build the UI first (pnpm ui:build) or unset {UI_DIR_ENV}",
            dir.display()
        ),
        None => Ok(Router::new().fallback(placeholder)),
    }
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

    #[tokio::test]
    async fn serves_index_and_assets_from_dir() {
        let dir = ui_fixture();
        let router = service_with_dir(Some(dir.path().to_path_buf())).expect("valid dir");

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
        let router = service_with_dir(Some(dir.path().to_path_buf())).expect("valid dir");
        let res = get(router, "/some/deep/link").await;
        assert_eq!(res.status(), StatusCode::OK);
        assert!(body_string(res).await.contains("ui-index"));
    }

    #[tokio::test]
    async fn placeholder_without_assets() {
        let router = service_with_dir(None).expect("no dir is valid");
        let res = get(router, "/").await;
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        assert!(body_string(res).await.contains("UI not bundled"));
    }

    #[test]
    fn configured_dir_without_index_is_an_error() {
        let empty = tempfile::tempdir().expect("tempdir");
        let err = service_with_dir(Some(empty.path().to_path_buf()))
            .expect_err("missing index.html must fail");
        assert!(err.to_string().contains("index.html"));
    }

    #[test]
    fn has_ui_assets_checks_for_index() {
        let dir = ui_fixture();
        assert!(has_ui_assets(dir.path()));
        let empty = tempfile::tempdir().expect("tempdir");
        assert!(!has_ui_assets(empty.path()));
    }
}
