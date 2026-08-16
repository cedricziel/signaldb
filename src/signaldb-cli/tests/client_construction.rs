//! `client-surface-parity`: the CLI obtains every HTTP client from the SDK's
//! `ClientBuilder`, never by assembling a bare `reqwest` client, so the
//! shared retry-on-throttle policy (and timeouts/headers) cannot drift per
//! call site.

use std::path::Path;

const FORBIDDEN: &[&str] = &[
    "reqwest::Client::builder()",
    "reqwest::ClientBuilder::new()",
    "reqwest::Client::new()",
];

fn scan(dir: &Path, hits: &mut Vec<String>) {
    for entry in std::fs::read_dir(dir).expect("readable source dir") {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        if path.is_dir() {
            scan(&path, hits);
            continue;
        }
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let source = std::fs::read_to_string(&path).expect("readable source file");
        for (line_no, line) in source.lines().enumerate() {
            let code = line.split("//").next().unwrap_or(line);
            for pattern in FORBIDDEN {
                if code.contains(pattern) {
                    hits.push(format!(
                        "{}:{}: {}",
                        path.display(),
                        line_no + 1,
                        line.trim()
                    ));
                }
            }
        }
    }
}

#[test]
fn cli_builds_every_http_client_through_the_sdk_builder() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut hits = Vec::new();
    scan(&src, &mut hits);
    assert!(
        hits.is_empty(),
        "bare reqwest client construction in signaldb-cli; use signaldb_sdk::ClientBuilder \
         (crate::retry::client_builder) instead:\n{}",
        hits.join("\n")
    );
}

#[test]
fn cli_uses_the_sdk_builder_somewhere() {
    // Guard against the scan passing vacuously.
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/retry.rs");
    let source = std::fs::read_to_string(src).expect("retry.rs");
    assert!(source.contains("ClientBuilder::new("));
}
