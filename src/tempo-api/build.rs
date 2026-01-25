use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    time::SystemTime,
};

/// Check if source file is newer than target file
fn is_source_newer(source: &Path, target: &Path) -> bool {
    if !target.exists() {
        return true;
    }

    let source_modified = source
        .metadata()
        .and_then(|m| m.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH);

    let target_modified = target
        .metadata()
        .and_then(|m| m.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH);

    source_modified > target_modified
}

fn main() {
    let binding = std::env::var_os("CARGO_MANIFEST_DIR").unwrap();
    let crate_path = Path::new(&binding).join("proto");
    let out_path = Path::new(&binding).join("src/generated");

    let otel_proto_path = Path::new(&std::env::var_os("CARGO_MANIFEST_DIR").unwrap())
        .join("../../opentelemetry-proto");

    // Generate common.proto only if needed
    let common_source = otel_proto_path.join("opentelemetry/proto/common/v1/common.proto");
    let common_target = crate_path.join("common/v1/common.proto");

    if is_source_newer(&common_source, &common_target) {
        fs::create_dir_all(common_target.parent().unwrap()).unwrap();

        let common_string = fs::read_to_string(&common_source)
            .unwrap()
            .replace(
                "package opentelemetry.proto.common.v1",
                "package tempopb.common.v1",
            )
            .replace("[(gogoproto.nullable) = false]", "");

        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&common_target)
            .unwrap();
        file.write_all(common_string.as_bytes()).unwrap();
        println!("cargo:warning=Regenerated common.proto");
    }

    // Generate resource.proto only if needed
    let resource_source = otel_proto_path.join("opentelemetry/proto/resource/v1/resource.proto");
    let resource_target = crate_path.join("resource/v1/resource.proto");

    if is_source_newer(&resource_source, &resource_target) {
        fs::create_dir_all(resource_target.parent().unwrap()).unwrap();

        let resource_string = fs::read_to_string(&resource_source)
            .unwrap()
            .replace(
                "package opentelemetry.proto.resource.v1",
                "package tempopb.resource.v1",
            )
            .replace(
                "import \"opentelemetry/proto/common/v1/common.proto\"",
                "import \"common/v1/common.proto\"",
            )
            .replace("opentelemetry.proto.common.v1", "tempopb.common.v1")
            .replace("[(gogoproto.nullable) = false]", "");

        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&resource_target)
            .unwrap();
        file.write_all(resource_string.as_bytes()).unwrap();
        println!("cargo:warning=Regenerated resource.proto");
    }

    // Generate trace.proto only if needed
    let trace_source = otel_proto_path.join("opentelemetry/proto/trace/v1/trace.proto");
    let trace_target = crate_path.join("trace/v1/trace.proto");

    if is_source_newer(&trace_source, &trace_target) {
        fs::create_dir_all(trace_target.parent().unwrap()).unwrap();

        let trace_string = fs::read_to_string(&trace_source)
            .unwrap()
            .replace(
                "package opentelemetry.proto.trace.v1",
                "package tempopb.trace.v1",
            )
            .replace(
                "import \"opentelemetry/proto/common/v1/common.proto\"",
                "import \"common/v1/common.proto\"",
            )
            .replace(
                "import \"opentelemetry/proto/resource/v1/resource.proto\"",
                "import \"resource/v1/resource.proto\"",
            )
            .replace("opentelemetry.proto.common.v1", "tempopb.common.v1")
            .replace("opentelemetry.proto.resource.v1", "tempopb.resource.v1")
            .replace("[(gogoproto.nullable) = false]", "");

        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&trace_target)
            .unwrap();
        file.write_all(trace_string.as_bytes()).unwrap();
        println!("cargo:warning=Regenerated trace.proto");
    }

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=proto/tempo.proto");

    // Watch OpenTelemetry proto source files
    println!(
        "cargo:rerun-if-changed=../../opentelemetry-proto/opentelemetry/proto/common/v1/common.proto"
    );
    println!(
        "cargo:rerun-if-changed=../../opentelemetry-proto/opentelemetry/proto/resource/v1/resource.proto"
    );
    println!(
        "cargo:rerun-if-changed=../../opentelemetry-proto/opentelemetry/proto/trace/v1/trace.proto"
    );

    // Check if any proto files are newer than generated Rust files
    let needs_regeneration = {
        let tempo_proto = crate_path.join("tempo.proto");
        // Check all generated files
        let generated_files = [
            out_path.join("tempopb.rs"),
            out_path.join("tempopb.common.v1.rs"),
            out_path.join("tempopb.resource.v1.rs"),
            out_path.join("tempopb.trace.v1.rs"),
        ];

        // If any generated file is missing or any source is newer than any generated file
        generated_files.iter().any(|f| !f.exists())
            || generated_files.iter().any(|generated| {
                is_source_newer(&tempo_proto, generated)
                    || is_source_newer(&common_target, generated)
                    || is_source_newer(&resource_target, generated)
                    || is_source_newer(&trace_target, generated)
            })
    };

    if needs_regeneration {
        tonic_prost_build::configure()
            .out_dir(&out_path)
            .build_client(true)
            .client_mod_attribute("tempopb", "#[cfg(feature = \"client\")]")
            .build_server(true)
            .server_mod_attribute("tempopb", "#[cfg(feature = \"server\")]")
            .compile_protos(
                &["proto/tempo.proto"],
                &[otel_proto_path.to_str().unwrap(), "proto/"],
            )
            .unwrap();

        println!("cargo:warning=Regenerated Rust protobuf code");
    }
}
