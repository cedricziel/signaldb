//! End-to-end checks of the multi-call binary's command surface: every
//! service subcommand answers `--help` with its own flags and `--version`
//! with the workspace version, and unknown subcommands are rejected.

use std::process::Command;

const SERVICES: &[&str] = &[
    "acceptor",
    "router",
    "writer",
    "querier",
    "compactor",
    "mcp",
];

fn signaldb(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_signaldb"))
        .args(args)
        .output()
        .expect("spawn signaldb")
}

#[test]
fn every_service_reports_the_workspace_version() {
    let top = signaldb(&["--version"]);
    assert!(top.status.success());
    let top_version = String::from_utf8_lossy(&top.stdout);
    let version = top_version.trim().rsplit(' ').next().unwrap().to_string();
    assert!(version.contains('.'), "top-level version: {top_version}");
    for svc in SERVICES {
        let out = signaldb(&[svc, "--version"]);
        assert!(out.status.success(), "{svc} --version failed: {out:?}");
        let text = String::from_utf8_lossy(&out.stdout);
        assert!(
            text.contains(svc),
            "{svc} --version does not name the service: {text}"
        );
        assert!(
            text.contains(&version),
            "{svc} --version {text} != {version}"
        );
    }
}

#[test]
fn every_service_help_is_its_own() {
    let expected: &[(&str, &str)] = &[
        ("acceptor", "--grpc-port"),
        ("router", "--flight-port"),
        ("writer", "--flight-port"),
        ("querier", "--flight-port"),
        ("compactor", "--config"),
        ("mcp", "--stdio"),
    ];
    for (svc, flag) in expected {
        let out = signaldb(&[svc, "--help"]);
        assert!(out.status.success(), "{svc} --help failed: {out:?}");
        let text = String::from_utf8_lossy(&out.stdout);
        assert!(text.contains(flag), "{svc} --help lacks {flag}:\n{text}");
        assert!(
            text.contains("--config"),
            "{svc} --help lacks the shared --config:\n{text}"
        );
    }
    let top = String::from_utf8_lossy(&signaldb(&["--help"]).stdout).to_string();
    for svc in SERVICES {
        assert!(
            top.contains(&format!("\n  {svc}")),
            "top-level help lacks {svc}:\n{top}"
        );
    }
}

#[test]
fn unknown_subcommand_exits_non_zero() {
    let out = signaldb(&["frobnicate"]);
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("unrecognized subcommand"), "{err}");
}
