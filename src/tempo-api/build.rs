use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
};

extern crate prost_build;

fn main() {
    let binding = std::env::var_os("CARGO_MANIFEST_DIR").unwrap();
    let crate_path = Path::new(&binding).join("proto");
    let out_path = Path::new(&binding).join("src/tempopb");

    let otel_proto_path = Path::new(&std::env::var_os("CARGO_MANIFEST_DIR").unwrap())
        .join("../../opentelemetry-proto");

    fs::create_dir_all(format!(
        "{}/common/v1",
        fs::canonicalize(crate_path.clone()).unwrap().display()
    ))
    .unwrap();

    let common_string = fs::read_to_string(format!(
        "{}/opentelemetry/proto/common/v1/common.proto",
        fs::canonicalize(otel_proto_path.clone()).unwrap().display()
    ))
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
        .open(format!(
            "{}/common/v1/common.proto",
            fs::canonicalize(crate_path.clone()).unwrap().display()
        ))
        .unwrap();
    file.write(common_string.as_bytes()).unwrap();

    fs::create_dir_all(format!(
        "{}/resource/v1",
        fs::canonicalize(crate_path.clone()).unwrap().display()
    ))
    .unwrap();

    let resource_string = fs::read_to_string(format!(
        "{}/opentelemetry/proto/resource/v1/resource.proto",
        fs::canonicalize(otel_proto_path.clone()).unwrap().display()
    ))
    .unwrap()
    .replace(
        "package opentelemetry.proto.resource.v1",
        "package tempopb.resource.v1",
    )
    .replace("[(gogoproto.nullable) = false]", "");

    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(format!(
            "{}/resource/v1/resource.proto",
            fs::canonicalize(crate_path.clone()).unwrap().display()
        ))
        .unwrap();
    file.write(resource_string.as_bytes()).unwrap();

    fs::create_dir_all(format!(
        "{}/trace/v1",
        fs::canonicalize(crate_path.clone()).unwrap().display()
    ))
    .unwrap();

    let trace_string = fs::read_to_string(format!(
        "{}/opentelemetry/proto/trace/v1/trace.proto",
        fs::canonicalize(otel_proto_path.clone()).unwrap().display()
    ))
    .unwrap()
    .replace(
        "package opentelemetry.proto.trace.v1",
        "package tempopb.trace.v1",
    )
    .replace("[(gogoproto.nullable) = false]", "");

    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(format!(
            "{}/trace/v1/trace.proto",
            fs::canonicalize(crate_path.clone()).unwrap().display()
        ))
        .unwrap();
    file.write(trace_string.as_bytes()).unwrap();

    println!("cargo:rerun-if-changed=build.rs");

    prost_build::Config::new()
        .out_dir(out_path)
        .disable_comments(&["."])
        .compile_protos(
            &["proto/tempo.proto"],
            &[otel_proto_path.to_str().unwrap(), "proto/"],
        )
        .unwrap();
}
