use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "xtask", about = "SignalDB code generation tooling")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Generate code from the OpenAPI spec
    Generate,
    /// Check that generated code is up-to-date (for CI)
    Check,
    /// Vendor the OpenTelemetry semantic-conventions model at the version
    /// pinned by `common::self_monitoring::SEMCONV_SCHEMA_URL` into
    /// `vendor/otel-semconv/`.
    VendorSemconv {
        /// Override the semconv version (defaults to the self-monitoring pin).
        #[arg(long)]
        version: Option<String>,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Generate => generate(false),
        Command::Check => generate(true),
        Command::VendorSemconv { version } => vendor_semconv(version),
    }
}

/// The semconv version SignalDB's self-monitoring telemetry claims, read
/// textually from `common` so xtask does not have to link the workspace.
fn pinned_semconv_version(root: &Path) -> Result<String> {
    let path = root.join("src/common/src/self_monitoring/mod.rs");
    let src =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    const PREFIX: &str = "https://opentelemetry.io/schemas/";
    let line = src
        .lines()
        .find(|l| l.contains("SEMCONV_SCHEMA_URL") && l.contains(PREFIX))
        .context("SEMCONV_SCHEMA_URL not found in common::self_monitoring")?;
    let start = line.find(PREFIX).context("schema url prefix")? + PREFIX.len();
    let version: String = line[start..]
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    if version.is_empty() {
        anyhow::bail!("could not parse a version out of {line:?}");
    }
    Ok(version)
}

/// Clone `open-telemetry/semantic-conventions` at `v<version>` and copy its
/// `model/` tree (plus LICENSE) into `vendor/otel-semconv/<version>/`,
/// replacing whatever vintage was vendored before, and record the version in
/// `vendor/otel-semconv/VERSION`. The bundled `otel` schema registry is built
/// from this tree; a unit test in `common` keeps VERSION equal to the
/// self-monitoring pin.
fn vendor_semconv(version: Option<String>) -> Result<()> {
    let root = project_root();
    let version = match version {
        Some(v) => v,
        None => pinned_semconv_version(&root)?,
    };
    let dest_root = root.join("vendor/otel-semconv");
    let tmp = std::env::temp_dir().join(format!("signaldb-semconv-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    let status = std::process::Command::new("git")
        .args([
            "clone",
            "--quiet",
            "--depth",
            "1",
            "--branch",
            &format!("v{version}"),
            "https://github.com/open-telemetry/semantic-conventions",
        ])
        .arg(&tmp)
        .status()
        .context("running git clone")?;
    if !status.success() {
        anyhow::bail!("git clone of semantic-conventions v{version} failed");
    }

    // Replace any previously vendored vintage wholesale.
    if dest_root.exists() {
        std::fs::remove_dir_all(&dest_root).context("clearing vendor/otel-semconv")?;
    }
    let dest = dest_root.join(&version);
    copy_tree(&tmp.join("model"), &dest.join("model"))?;
    std::fs::copy(tmp.join("LICENSE"), dest.join("LICENSE")).context("copying LICENSE")?;
    std::fs::write(dest_root.join("VERSION"), format!("{version}\n"))?;
    std::fs::write(
        dest_root.join("README.md"),
        format!(
            "# Vendored OpenTelemetry semantic conventions\n\n\
             `{version}/model/` is a verbatim copy of `model/` from\n\
             https://github.com/open-telemetry/semantic-conventions at tag `v{version}`\n\
             (Apache-2.0, see `{version}/LICENSE`). It is the source of the bundled\n\
             `otel` schema registry. Do not edit by hand — regenerate with\n\
             `cargo xtask vendor-semconv` after bumping\n\
             `common::self_monitoring::SEMCONV_SCHEMA_URL`.\n"
        ),
    )?;
    let _ = std::fs::remove_dir_all(&tmp);
    let files = collect_files(&dest.join("model"))?.len();
    eprintln!("  VENDORED semconv v{version} ({files} model files)");
    Ok(())
}

/// Recursively copy `src` into `dst` (created if missing).
fn copy_tree(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src).with_context(|| format!("reading {}", src.display()))? {
        let entry = entry?;
        let target = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask must be inside the workspace")
        .to_path_buf()
}

fn generate(check_only: bool) -> Result<()> {
    let root = project_root();
    // The spec itself is emitted from the router's code-first annotations by the
    // golden test in `router::openapi` (UPDATE_OPENAPI=1 cargo test -p router).
    // xtask consumes that committed spec to generate the downstream clients.
    let spec_json = std::fs::read_to_string(root.join("api/signaldb-api.json"))
        .context("reading api/signaldb-api.json (run UPDATE_OPENAPI=1 cargo test -p router)")?;
    let json_value: serde_json::Value =
        serde_json::from_str(&spec_json).context("parsing api/signaldb-api.json")?;

    // --- signaldb-sdk client (progenitor, Rust) ---
    let sdk_client = generate_sdk_client(&json_value)?;
    write_or_check(
        &root.join("src/signaldb-sdk/src/generated.rs"),
        &sdk_client,
        check_only,
    )?;

    // --- UI TypeScript client (@hey-api/openapi-ts) ---
    generate_ts_client(&root, check_only)?;

    if check_only {
        eprintln!("All generated files are up-to-date.");
    } else {
        eprintln!("Code generation complete.");
    }
    Ok(())
}

/// Generate a full HTTP client from the OpenAPI spec using progenitor.
///
/// The spec is emitted by utoipa as OpenAPI 3.1, but progenitor parses via the
/// `openapiv3` crate, which targets 3.0. The only incompatibility our schemas
/// hit is 3.1's nullable encoding (`"type": ["string", "null"]`), so we
/// downconvert those to 3.0's `"type": "string", "nullable": true` before
/// handing the spec to progenitor. The served spec and the checked-in
/// `signaldb-api.json` stay 3.1; this rewrite is progenitor-input only.
fn generate_sdk_client(spec: &serde_json::Value) -> Result<String> {
    let mut spec = spec.clone();
    homogenize_error_response_bodies(&mut spec);
    downconvert_nullable_types(&mut spec);
    // progenitor/openapiv3 only accept a 3.0.x version string.
    if let Some(obj) = spec.as_object_mut() {
        obj.insert(
            "openapi".to_string(),
            serde_json::Value::String("3.0.3".to_string()),
        );
    }
    let openapi: openapiv3::OpenAPI =
        serde_json::from_value(spec).context("parsing spec as openapiv3::OpenAPI")?;

    let mut settings = progenitor::GenerationSettings::default();
    settings
        .with_interface(progenitor::InterfaceStyle::Builder)
        .with_tag(progenitor::TagStyle::Merged);

    let mut generator = progenitor::Generator::new(&settings);
    let tokens = generator
        .generate_tokens(&openapi)
        .context("progenitor generate_tokens")?;
    let ast: syn::File = syn::parse2(tokens).context("parsing progenitor output as syn::File")?;
    let code = prettyplease::unparse(&ast);

    let formatted = run_rustfmt(&code)?;
    Ok(format!(
        "// This file is generated by `cargo xtask generate`. Do not edit.\n\n{formatted}"
    ))
}

/// Progenitor cannot express an operation whose non-2xx responses use more
/// than one distinct body type (it asserts `response_types.len() <= 1` while
/// building the method signature — see `extract_responses` in
/// `progenitor-impl`, which has a `TODO` acknowledging this: a real fix needs
/// an enum type for the mixed case). SignalDB's rate-limit rejection
/// (`components.responses.RateLimited`, body `ApiErrorBody`) is injected by
/// router middleware ahead of the handler, so on an operation whose other
/// errors were untyped (or typed as something else, e.g. `ManageError`,
/// `SchemaError`) before this change, the newly declared `429` body would
/// either be the operation's first-ever typed error (flipping its SDK error
/// type from `()` to something concrete) or a second, different type — both
/// change the generated method's `Result<_, Error<E>>` signature in a way
/// that breaks existing hand-written callers (`mcp-server`, `signaldb-cli`)
/// built against the pre-existing `E`.
///
/// To keep every operation's progenitor-visible error type exactly what it
/// was before this change, this always strips the `429` response's `content`
/// (keeping `description`/`headers`) *unless* the operation's other non-2xx
/// responses already agree on one type — in which case the `429` is
/// retargeted to reuse that same type instead of being stripped, since
/// existing callers already handle it and adding another response that
/// produces the same `E` changes nothing about the method's signature.
/// Operations whose non-`429` errors are already heterogeneous (not
/// something this change introduces) fall back to stripping every non-2xx
/// body. Progenitor-input only; the served spec, `api/signaldb-api.json`,
/// and the TypeScript client (no such limitation) keep full fidelity.
fn homogenize_error_response_bodies(spec: &mut serde_json::Value) {
    let components_responses = spec
        .pointer("/components/responses")
        .cloned()
        .unwrap_or(serde_json::Value::Null);

    let Some(paths) = spec.get_mut("paths").and_then(|p| p.as_object_mut()) else {
        return;
    };
    for path_item in paths.values_mut() {
        let Some(path_obj) = path_item.as_object_mut() else {
            continue;
        };
        for operation in path_obj.values_mut() {
            let Some(responses) = operation
                .get_mut("responses")
                .and_then(|r| r.as_object_mut())
            else {
                continue;
            };
            if !responses.contains_key("429") {
                continue;
            }

            let other_statuses: Vec<String> = responses
                .keys()
                .filter(|status| {
                    *status != "429"
                        && status
                            .parse::<u16>()
                            .is_ok_and(|code| !(200..300).contains(&code))
                })
                .cloned()
                .collect();

            let other_keys: Vec<Option<String>> = other_statuses
                .iter()
                .map(|status| {
                    response_content_schema_key(&responses[status], &components_responses)
                })
                .collect();
            let other_homogeneous = other_keys.windows(2).all(|w| w[0] == w[1]);

            if !other_statuses.is_empty() && other_homogeneous {
                // Reuse the other errors' shared type for `429` too, so
                // existing callers of this operation (already handling that
                // `E`) see no signature change.
                let dominant_content = responses
                    .get(&other_statuses[0])
                    .and_then(|r| resolve_response(r, &components_responses))
                    .and_then(|resolved| resolved.get("content").cloned());
                let rl = responses.get_mut("429").expect("checked above");
                match dominant_content {
                    Some(content) => retarget_response_content(rl, content, &components_responses),
                    None => strip_response_body_keep_headers(rl, &components_responses),
                }
                continue;
            }

            if !other_statuses.is_empty() {
                // Heterogeneity this change didn't introduce; fall back to
                // the lossy-but-safe strip of every non-2xx body so
                // progenitor at least builds.
                for status in &other_statuses {
                    if let Some(resp) = responses.get_mut(status) {
                        strip_response_body_keep_headers(resp, &components_responses);
                    }
                }
            }

            // No other non-2xx responses existed before `429` was added:
            // strip its body so the operation's error type stays `()`,
            // exactly as it was before this change.
            let rl = responses.get_mut("429").expect("checked above");
            strip_response_body_keep_headers(rl, &components_responses);
        }
    }
}

/// A comparable identity for a response's `application/json` body schema:
/// the schema's `$ref` when present, else its full JSON text, else `None`
/// when the response has no body. Resolves one level of `$ref` to
/// `components.responses.*` first, since utoipa emits shared responses (like
/// `RateLimited`) that way.
fn response_content_schema_key(
    response: &serde_json::Value,
    components_responses: &serde_json::Value,
) -> Option<String> {
    let resolved = resolve_response(response, components_responses)?;
    let schema = resolved.pointer("/content/application~1json/schema")?;
    Some(
        schema
            .get("$ref")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| schema.to_string()),
    )
}

/// Resolve one level of `$ref` against `components.responses`; otherwise
/// return the response object as-is.
fn resolve_response<'a>(
    response: &'a serde_json::Value,
    components_responses: &'a serde_json::Value,
) -> Option<&'a serde_json::Value> {
    match response.get("$ref").and_then(|v| v.as_str()) {
        Some(r) => {
            let name = r.rsplit('/').next()?;
            components_responses.get(name)
        }
        None => Some(response),
    }
}

/// Replace `response` with an inline object carrying only `description` and
/// `headers` (no `content`), resolving one level of `$ref` first.
fn strip_response_body_keep_headers(
    response: &mut serde_json::Value,
    components_responses: &serde_json::Value,
) {
    if let Some(resolved) = resolve_response(response, components_responses) {
        *response = resolved.clone();
    }
    if let Some(obj) = response.as_object_mut() {
        obj.remove("content");
    }
}

/// Replace `response`'s `content` field with `content` (typically another
/// response's, so the two now agree on a body type for progenitor), keeping
/// `response`'s own `description` and `headers` (resolving one level of
/// `$ref` first, since `response` may be a bare `$ref` to a shared
/// component like `RateLimited`).
fn retarget_response_content(
    response: &mut serde_json::Value,
    content: serde_json::Value,
    components_responses: &serde_json::Value,
) {
    if let Some(resolved) = resolve_response(response, components_responses) {
        *response = resolved.clone();
    }
    if let Some(obj) = response.as_object_mut() {
        obj.insert("content".to_string(), content);
    }
}

/// Recursively rewrite OpenAPI 3.1 nullable encodings into the 3.0 form that
/// the `openapiv3` crate (and thus progenitor) understands. Two shapes:
///
/// - `"type": ["string", "null"]` becomes `"type": "string", "nullable":
///   true` (a single-element `"type": ["string"]` unwraps to `"type":
///   "string"` with no `nullable`).
/// - `"oneOf": [{"type": "null"}, {"$ref": "...", ...}]` (utoipa's encoding
///   for `Option<SomeStruct>`, where the non-null branch is a `$ref` rather
///   than an inline `type`) becomes the non-null branch's fields merged
///   directly onto this object plus `"nullable": true`. Progenitor's schema
///   converter has no 3.1 `oneOf`-with-null-branch handling and panics
///   (`not yet implemented: invalid type: null`) on the raw form.
fn downconvert_nullable_types(value: &mut serde_json::Value) {
    if let serde_json::Value::Object(map) = value
        && let Some(serde_json::Value::Array(variants)) = map.get("oneOf")
        && variants.len() == 2
    {
        let null_index = variants.iter().position(|v| {
            v.as_object()
                .and_then(|o| o.get("type"))
                .and_then(|t| t.as_str())
                == Some("null")
        });
        if let Some(null_index) = null_index {
            let other = variants[1 - null_index].clone();
            if let Some(other_obj) = other.as_object() {
                let other_obj = other_obj.clone();
                map.remove("oneOf");
                for (k, v) in other_obj {
                    map.insert(k, v);
                }
                map.insert("nullable".to_string(), serde_json::Value::Bool(true));
            }
        }
    }
    match value {
        serde_json::Value::Object(map) => {
            if let Some(serde_json::Value::Array(types)) = map.get("type") {
                let mut non_null: Vec<String> = Vec::new();
                let mut has_null = false;
                for t in types {
                    match t.as_str() {
                        Some("null") => has_null = true,
                        Some(other) => non_null.push(other.to_string()),
                        None => {}
                    }
                }
                if non_null.len() == 1 {
                    map.insert(
                        "type".to_string(),
                        serde_json::Value::String(non_null.remove(0)),
                    );
                    if has_null {
                        map.insert("nullable".to_string(), serde_json::Value::Bool(true));
                    }
                }
            }
            for v in map.values_mut() {
                downconvert_nullable_types(v);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                downconvert_nullable_types(v);
            }
        }
        _ => {}
    }
}

/// Generate (or verify) the UI's TypeScript client from the OpenAPI spec using
/// `@hey-api/openapi-ts` (config in src/ui/openapi-ts.config.ts). In check mode
/// the client is regenerated into a temp directory and compared against the
/// committed output, so a stale client fails CI without mutating the tree.
fn generate_ts_client(root: &Path, check_only: bool) -> Result<()> {
    let out_rel = "src/ui/src/api/gen";
    let committed = root.join(out_rel);

    if check_only {
        let tmp = std::env::temp_dir().join(format!("signaldb-ts-gen-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        run_openapi_ts(root, Some(&tmp))?;
        let equal = dirs_equal(&tmp, &committed)?;
        let _ = std::fs::remove_dir_all(&tmp);
        if !equal {
            anyhow::bail!(
                "Generated code is out of date: {out_rel}\nRun `cargo xtask generate` to update."
            );
        }
        eprintln!("  OK  {out_rel}");
    } else {
        run_openapi_ts(root, None)?;
        eprintln!("  GEN {out_rel}");
    }
    Ok(())
}

/// Invoke `@hey-api/openapi-ts` via pnpm in the signaldb-ui package. When
/// `output` is set it overrides the config's output directory (used for the
/// check-mode temp comparison).
fn run_openapi_ts(root: &Path, output: Option<&Path>) -> Result<()> {
    let mut cmd = std::process::Command::new("pnpm");
    cmd.current_dir(root)
        .args(["--filter", "signaldb-ui", "exec", "openapi-ts"]);
    if let Some(out) = output {
        cmd.arg("-o").arg(out);
    }
    let status = cmd
        .status()
        .context("running `pnpm --filter signaldb-ui exec openapi-ts` (is pnpm installed and `pnpm install` run?)")?;
    if !status.success() {
        anyhow::bail!("openapi-ts exited with status {status}");
    }
    Ok(())
}

/// Recursively compare two directory trees for identical relative file sets and
/// byte-identical contents.
fn dirs_equal(a: &Path, b: &Path) -> Result<bool> {
    let mut files_a = collect_files(a)?;
    let mut files_b = collect_files(b)?;
    files_a.sort();
    files_b.sort();
    if files_a != files_b {
        return Ok(false);
    }
    for rel in &files_a {
        if std::fs::read(a.join(rel))? != std::fs::read(b.join(rel))? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Collect file paths under `dir`, relative to it, recursing into subdirectories.
fn collect_files(dir: &Path) -> Result<Vec<PathBuf>> {
    fn walk(base: &Path, dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() {
                walk(base, &path, out)?;
            } else {
                out.push(path.strip_prefix(base)?.to_path_buf());
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out)?;
    Ok(out)
}

/// Run `rustfmt` on a Rust source string, returning the formatted output.
fn run_rustfmt(code: &str) -> Result<String> {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let mut child = Command::new("rustfmt")
        .arg("--edition")
        .arg("2024")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("spawning rustfmt (is it installed?)")?;

    child
        .stdin
        .take()
        .context("opening rustfmt stdin")?
        .write_all(code.as_bytes())
        .context("writing to rustfmt stdin")?;

    let output = child.wait_with_output().context("waiting for rustfmt")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("rustfmt failed: {stderr}");
    }

    String::from_utf8(output.stdout).context("rustfmt produced non-UTF-8 output")
}

/// Write content to a file, or in check mode, verify the file matches.
fn write_or_check(path: &Path, content: &str, check_only: bool) -> Result<()> {
    let rel = path.strip_prefix(project_root()).unwrap_or(path).display();

    if check_only {
        let existing = std::fs::read_to_string(path)
            .with_context(|| format!("reading {rel} (does it exist?)"))?;
        if existing != content {
            anyhow::bail!(
                "Generated code is out of date: {rel}\nRun `cargo xtask generate` to update."
            );
        }
        eprintln!("  OK  {rel}");
    } else {
        std::fs::write(path, content).with_context(|| format!("writing {rel}"))?;
        eprintln!("  GEN {rel}");
    }
    Ok(())
}
