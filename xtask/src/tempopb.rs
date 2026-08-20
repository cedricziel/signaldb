//! Generate `tempo-api`'s protobuf bindings.
//!
//! This used to be `src/tempo-api/build.rs`, where it could not stay. A build
//! script may only write into `OUT_DIR`; that one wrote generated `.proto`
//! files into `src/tempo-api/proto/` and generated Rust into
//! `src/tempo-api/src/generated/`, both inside the package. It also read
//! `opentelemetry-proto/`, a submodule *outside* the crate root, which no
//! `.crate` archive can carry — so the packaged build script would panic on
//! any consumer's machine, and `cargo publish` rejected the dirty package
//! before it got that far. On top of that it decided staleness by comparing
//! mtimes, which git checkouts assign arbitrarily.
//!
//! Here instead, generation is an explicit developer command whose output is
//! committed and verified — the same `write_or_check` contract as the OpenAPI
//! clients. `tempo-api` becomes a plain source crate: no build script, no
//! submodule at build time, no `protoc` for anyone who merely depends on it.

use std::path::Path;

use anyhow::{Context, Result};

/// One OTel proto vendored into `tempopb`'s namespace.
///
/// Tempo publishes its `tempopb` package with the OTel messages re-declared
/// under its own package, so both the package line and every import must be
/// rewritten to match rather than pointing at upstream OTel.
struct Vendored {
    /// Path within the `opentelemetry-proto` submodule.
    upstream: &'static str,
    /// Path within the scratch proto tree, as `tempo.proto` imports it.
    vendored: &'static str,
    /// Literal substitutions, applied in order.
    edits: &'static [(&'static str, &'static str)],
}

const OTEL_PROTOS: &[Vendored] = &[
    Vendored {
        upstream: "opentelemetry/proto/common/v1/common.proto",
        vendored: "common/v1/common.proto",
        edits: &[(
            "package opentelemetry.proto.common.v1",
            "package tempopb.common.v1",
        )],
    },
    Vendored {
        upstream: "opentelemetry/proto/resource/v1/resource.proto",
        vendored: "resource/v1/resource.proto",
        edits: &[
            (
                "package opentelemetry.proto.resource.v1",
                "package tempopb.resource.v1",
            ),
            (
                "import \"opentelemetry/proto/common/v1/common.proto\"",
                "import \"common/v1/common.proto\"",
            ),
            ("opentelemetry.proto.common.v1", "tempopb.common.v1"),
        ],
    },
    Vendored {
        upstream: "opentelemetry/proto/trace/v1/trace.proto",
        vendored: "trace/v1/trace.proto",
        edits: &[
            (
                "package opentelemetry.proto.trace.v1",
                "package tempopb.trace.v1",
            ),
            (
                "import \"opentelemetry/proto/common/v1/common.proto\"",
                "import \"common/v1/common.proto\"",
            ),
            (
                "import \"opentelemetry/proto/resource/v1/resource.proto\"",
                "import \"resource/v1/resource.proto\"",
            ),
            ("opentelemetry.proto.common.v1", "tempopb.common.v1"),
            ("opentelemetry.proto.resource.v1", "tempopb.resource.v1"),
        ],
    },
];

/// The four files `tonic-prost-build` emits, all committed under
/// `src/tempo-api/src/generated/` and `include!`d by `tempo-api`'s `lib.rs`.
const GENERATED: &[&str] = &[
    "tempopb.rs",
    "tempopb.common.v1.rs",
    "tempopb.resource.v1.rs",
    "tempopb.trace.v1.rs",
];

/// Regenerate (or verify) `tempo-api`'s protobuf bindings.
pub fn generate(root: &Path, check_only: bool, write_or_check: super::WriteOrCheck) -> Result<()> {
    let crate_dir = root.join("src/tempo-api");
    let otel = root.join("opentelemetry-proto");

    if !otel
        .join("opentelemetry/proto/common/v1/common.proto")
        .exists()
    {
        anyhow::bail!(
            "the opentelemetry-proto submodule is not initialised — run\n  \
             git submodule update --init opentelemetry-proto"
        );
    }

    // Everything intermediate goes to a scratch directory. Nothing this
    // function does may leave a file inside the package.
    let scratch = std::env::temp_dir().join("signaldb-tempopb-gen");
    let _ = std::fs::remove_dir_all(&scratch);
    let proto_dir = scratch.join("proto");
    let out_dir = scratch.join("out");
    std::fs::create_dir_all(&out_dir).context("creating the scratch output directory")?;

    // The hand-maintained tempo.proto is the one real source file; the OTel
    // ones are derived beside it so its imports resolve.
    std::fs::create_dir_all(&proto_dir)?;
    std::fs::copy(
        crate_dir.join("proto/tempo.proto"),
        proto_dir.join("tempo.proto"),
    )
    .context("copying tempo.proto into the scratch tree")?;

    for proto in OTEL_PROTOS {
        let src = otel.join(proto.upstream);
        let mut text =
            std::fs::read_to_string(&src).with_context(|| format!("reading {}", src.display()))?;
        for (from, to) in proto.edits {
            text = text.replace(from, to);
        }
        // gogoproto annotations are Tempo-side and meaningless to prost.
        text = text.replace("[(gogoproto.nullable) = false]", "");

        let dest = proto_dir.join(proto.vendored);
        std::fs::create_dir_all(dest.parent().expect("vendored path has a parent"))?;
        std::fs::write(&dest, text).with_context(|| format!("writing {}", dest.display()))?;
    }

    tonic_prost_build::configure()
        .out_dir(&out_dir)
        .build_client(true)
        .client_mod_attribute("tempopb", "#[cfg(feature = \"client\")]")
        .build_server(true)
        .server_mod_attribute("tempopb", "#[cfg(feature = \"server\")]")
        .compile_protos(
            &[proto_dir.join("tempo.proto")],
            &[proto_dir.clone(), otel.clone()],
        )
        .context("compiling tempo.proto (is protoc installed?)")?;

    for name in GENERATED {
        let produced = std::fs::read_to_string(out_dir.join(name))
            .with_context(|| format!("reading generated {name}"))?;
        write_or_check(
            &crate_dir.join("src/generated").join(name),
            &produced,
            check_only,
        )?;
    }

    let _ = std::fs::remove_dir_all(&scratch);
    Ok(())
}
