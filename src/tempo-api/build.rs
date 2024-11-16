use std::path::Path;

extern crate prost_build;

fn main() {
    let folder_path = Path::new(&std::env::var_os("CARGO_MANIFEST_DIR").unwrap())
        .join("../../opentelemetry-proto");

    println!("cargo:rerun-if-changed=build.rs");

    prost_build::compile_protos(
        &[
            "opentelemetry/proto/common/v1/common.proto",
            "opentelemetry/proto/resource/v1/resource.proto",
            "opentelemetry/proto/trace/v1/trace.proto",
        ],
        &[folder_path.to_str().unwrap()],
    )
    .unwrap();

    // prost_build::compile_protos(
    //     &["proto/tempo.proto"],
    //     &["proto/", "src/", folder_path.to_str().unwrap()],
    // )
    // .unwrap();
}
