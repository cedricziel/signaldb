//! # Topology
//!
//! Describes the two synthetic estates the producer emits telemetry for and
//! builds a fully-populated OpenTelemetry [`Resource`] for every service in
//! them.
//!
//! Both estates model a realistic microservice deployment on AWS EKS:
//!
//! - **`rideshare`** — a ride-hailing platform (`us-east-1`). Rider and driver
//!   handsets talk to an edge gateway that fans out to trip, matching,
//!   pricing, location, payment and notification services over gRPC, Kafka and
//!   Redis/PostgreSQL.
//! - **`shop`** — an online storefront (`eu-west-1`). A browser SPA and a
//!   mobile app hit an edge gateway fronting catalog, cart, checkout,
//!   inventory, payment, shipping, search and recommendation services.
//!
//! Every service advertises a coherent set of OpenTelemetry semantic-convention
//! resource attributes (`service.*`, `cloud.*`, `k8s.*`, `host.*`,
//! `container.*`, `device.*`, `browser.*`) so that traces, logs and metrics are
//! correlatable by `service.name` + `service.namespace` and joinable to their
//! infrastructure via the shared `k8s.*` / `host.*` attributes.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use opentelemetry::KeyValue;
use opentelemetry_sdk::Resource;

/// The OpenTelemetry SDK version this producer models in resource attributes.
const SDK_VERSION: &str = "0.32.0";

/// Runtime kind for a service. Drives which family of resource attributes the
/// service advertises — a handset and a Kubernetes pod describe themselves very
/// differently.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    /// Native mobile application running on a rider/driver/shopper handset.
    Mobile,
    /// Browser single-page application.
    Browser,
    /// Backend service running as a pod on Kubernetes.
    Backend,
}

/// Shared cloud + Kubernetes context for every service in an estate.
pub struct Estate {
    /// `service.namespace` shared by all services in the estate.
    pub namespace: &'static str,
    /// `deployment.environment.name`.
    pub environment: &'static str,
    /// `cloud.region`.
    pub region: &'static str,
    /// Availability zones services are spread across (`cloud.availability_zone`).
    pub zones: &'static [&'static str],
    /// `cloud.account.id`.
    pub account_id: &'static str,
    /// `k8s.cluster.name`.
    pub cluster: &'static str,
    /// The services deployed in this estate.
    pub services: &'static [ServiceSpec],
}

/// Static description of a single service.
pub struct ServiceSpec {
    /// `service.name`.
    pub name: &'static str,
    /// Runtime platform.
    pub platform: Platform,
    /// `service.version`.
    pub version: &'static str,
    /// `telemetry.sdk.language` (also selects the runtime for backend pods).
    pub language: &'static str,
}

impl ServiceSpec {
    /// Stable fleet key: `namespace/name`. Service names (e.g. `edge-gateway`,
    /// `payment-service`) repeat across estates, so the namespace disambiguates.
    pub fn key(&self, estate: &Estate) -> String {
        format!("{}/{}", estate.namespace, self.name)
    }
}

/// Returns the estates to emit for, filtered by the `--estate` selector.
pub fn estates(selector: &str) -> Vec<&'static Estate> {
    let want = selector.trim().to_ascii_lowercase();
    ALL_ESTATES
        .iter()
        .filter(|e| want == "all" || want == e.namespace)
        .copied()
        .collect()
}

static ALL_ESTATES: &[&Estate] = &[&RIDESHARE, &SHOP];

/// Ride-hailing estate in `us-east-1`.
static RIDESHARE: Estate = Estate {
    namespace: "rideshare",
    environment: "production",
    region: "us-east-1",
    zones: &["us-east-1a", "us-east-1b", "us-east-1c"],
    account_id: "471100000001",
    cluster: "rideshare-prod-use1",
    services: &[
        ServiceSpec {
            name: "rider-app",
            platform: Platform::Mobile,
            version: "8.24.1",
            language: "swift",
        },
        ServiceSpec {
            name: "driver-app",
            platform: Platform::Mobile,
            version: "8.24.0",
            language: "kotlin",
        },
        ServiceSpec {
            name: "edge-gateway",
            platform: Platform::Backend,
            version: "3.12.4",
            language: "go",
        },
        ServiceSpec {
            name: "trip-service",
            platform: Platform::Backend,
            version: "2.7.0",
            language: "java",
        },
        ServiceSpec {
            name: "matching-service",
            platform: Platform::Backend,
            version: "1.19.2",
            language: "go",
        },
        ServiceSpec {
            name: "pricing-service",
            platform: Platform::Backend,
            version: "4.3.1",
            language: "rust",
        },
        ServiceSpec {
            name: "location-service",
            platform: Platform::Backend,
            version: "1.8.5",
            language: "go",
        },
        ServiceSpec {
            name: "payment-service",
            platform: Platform::Backend,
            version: "5.1.0",
            language: "java",
        },
        ServiceSpec {
            name: "notification-service",
            platform: Platform::Backend,
            version: "2.0.3",
            language: "nodejs",
        },
    ],
};

/// Online storefront estate in `eu-west-1`.
static SHOP: Estate = Estate {
    namespace: "shop",
    environment: "production",
    region: "eu-west-1",
    zones: &["eu-west-1a", "eu-west-1b", "eu-west-1c"],
    account_id: "471100000002",
    cluster: "shop-prod-euw1",
    services: &[
        ServiceSpec {
            name: "web-storefront",
            platform: Platform::Browser,
            version: "2026.7.3",
            language: "webjs",
        },
        ServiceSpec {
            name: "shop-mobile",
            platform: Platform::Mobile,
            version: "6.11.0",
            language: "kotlin",
        },
        ServiceSpec {
            name: "edge-gateway",
            platform: Platform::Backend,
            version: "3.12.4",
            language: "go",
        },
        ServiceSpec {
            name: "catalog-service",
            platform: Platform::Backend,
            version: "7.4.2",
            language: "java",
        },
        ServiceSpec {
            name: "cart-service",
            platform: Platform::Backend,
            version: "3.0.1",
            language: "go",
        },
        ServiceSpec {
            name: "checkout-service",
            platform: Platform::Backend,
            version: "4.9.0",
            language: "java",
        },
        ServiceSpec {
            name: "payment-service",
            platform: Platform::Backend,
            version: "5.1.0",
            language: "java",
        },
        ServiceSpec {
            name: "inventory-service",
            platform: Platform::Backend,
            version: "2.6.7",
            language: "go",
        },
        ServiceSpec {
            name: "shipping-service",
            platform: Platform::Backend,
            version: "1.4.0",
            language: "rust",
        },
        ServiceSpec {
            name: "search-service",
            platform: Platform::Backend,
            version: "3.8.1",
            language: "java",
        },
        ServiceSpec {
            name: "recommendation-service",
            platform: Platform::Backend,
            version: "0.14.2",
            language: "python",
        },
    ],
};

/// Builds the OpenTelemetry resource for `spec` within `estate`.
pub fn build_resource(estate: &Estate, spec: &ServiceSpec) -> Resource {
    let mut attrs = vec![
        KeyValue::new("service.name", spec.name),
        KeyValue::new("service.namespace", estate.namespace),
        KeyValue::new("service.version", spec.version),
        KeyValue::new("service.instance.id", instance_id(estate, spec)),
        KeyValue::new("deployment.environment.name", estate.environment),
        KeyValue::new("telemetry.sdk.name", "opentelemetry"),
        KeyValue::new("telemetry.sdk.language", spec.language),
        KeyValue::new("telemetry.sdk.version", SDK_VERSION),
    ];

    match spec.platform {
        Platform::Backend => attrs.extend(backend_attrs(estate, spec)),
        Platform::Mobile => attrs.extend(mobile_attrs(spec)),
        Platform::Browser => attrs.extend(browser_attrs()),
    }

    Resource::builder().with_attributes(attrs).build()
}

/// Cloud, Kubernetes, container, host and process attributes for a backend pod.
fn backend_attrs(estate: &Estate, spec: &ServiceSpec) -> Vec<KeyValue> {
    let zone = pick_zone(estate, spec.name);
    let pod = format!(
        "{}-{}-{}",
        spec.name,
        hash_seg(spec.name, 1),
        hash_seg(spec.name, 2)
    );
    let node = format!(
        "ip-10-{}-{}-{}.ec2.internal",
        byte(spec.name, 3),
        byte(spec.name, 4),
        byte(spec.name, 5)
    );
    vec![
        KeyValue::new("cloud.provider", "aws"),
        KeyValue::new("cloud.platform", "aws_eks"),
        KeyValue::new("cloud.region", estate.region),
        KeyValue::new("cloud.availability_zone", zone),
        KeyValue::new("cloud.account.id", estate.account_id),
        KeyValue::new("k8s.cluster.name", estate.cluster),
        KeyValue::new("k8s.namespace.name", estate.namespace),
        KeyValue::new("k8s.deployment.name", spec.name.to_string()),
        KeyValue::new("k8s.pod.name", pod.clone()),
        KeyValue::new("k8s.pod.uid", uuid_like(&pod)),
        KeyValue::new("k8s.node.name", node.clone()),
        KeyValue::new("k8s.container.name", spec.name.to_string()),
        KeyValue::new("container.name", pod),
        KeyValue::new(
            "container.image.name",
            format!("ghcr.io/acme/{}", spec.name),
        ),
        KeyValue::new("container.image.tag", spec.version),
        KeyValue::new("container.runtime", "containerd"),
        KeyValue::new("host.name", node),
        KeyValue::new("host.id", uuid_like(spec.name)),
        KeyValue::new("host.type", "m6i.xlarge"),
        KeyValue::new("host.arch", "amd64"),
        KeyValue::new("os.type", "linux"),
        KeyValue::new("process.pid", 10_000 + byte(spec.name, 6) as i64),
        KeyValue::new("process.runtime.name", runtime_name(spec.language)),
    ]
}

/// Device and OS attributes for a native mobile handset.
fn mobile_attrs(spec: &ServiceSpec) -> Vec<KeyValue> {
    let ios = spec.language == "swift";
    vec![
        KeyValue::new("device.id", uuid_like(spec.name)),
        KeyValue::new("device.manufacturer", if ios { "Apple" } else { "Google" }),
        KeyValue::new(
            "device.model.identifier",
            if ios { "iPhone16,2" } else { "Pixel 9 Pro" },
        ),
        KeyValue::new(
            "device.model.name",
            if ios {
                "iPhone 15 Pro Max"
            } else {
                "Pixel 9 Pro"
            },
        ),
        KeyValue::new("os.type", if ios { "darwin" } else { "linux" }),
        KeyValue::new("os.name", if ios { "iOS" } else { "Android" }),
        KeyValue::new("os.version", if ios { "18.5" } else { "15" }),
        KeyValue::new(
            "app.installation.id",
            uuid_like(&format!("{}-install", spec.name)),
        ),
    ]
}

/// Browser and platform attributes for a web SPA.
fn browser_attrs() -> Vec<KeyValue> {
    vec![
        KeyValue::new(
            "browser.brands",
            "Chromium 133;Google Chrome 133;Not_A Brand 24",
        ),
        KeyValue::new("browser.platform", "macOS"),
        KeyValue::new("browser.language", "en-US"),
        KeyValue::new("browser.mobile", false),
        KeyValue::new("os.type", "darwin"),
        KeyValue::new(
            "user_agent.original",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 \
             (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        ),
    ]
}

fn runtime_name(language: &str) -> &'static str {
    match language {
        "java" => "OpenJDK Runtime Environment",
        "go" => "go",
        "rust" => "rustc",
        "nodejs" => "nodejs",
        "python" => "CPython",
        _ => "unknown",
    }
}

/// Deterministic `service.instance.id` (a UUID-like string) for a service.
fn instance_id(estate: &Estate, spec: &ServiceSpec) -> String {
    uuid_like(&format!("{}/{}", estate.namespace, spec.name))
}

/// Deterministically picks an availability zone for a service.
fn pick_zone(estate: &Estate, name: &str) -> &'static str {
    estate.zones[(byte(name, 0) as usize) % estate.zones.len()]
}

/// A stable 64-bit hash of `s` mixed with `salt`.
fn mix(s: &str, salt: u64) -> u64 {
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    salt.hash(&mut h);
    h.finish()
}

/// A single deterministic byte derived from `s` and `salt`.
fn byte(s: &str, salt: u64) -> u8 {
    (mix(s, salt) & 0xff) as u8
}

/// A short lowercase hex segment (used for pod-name suffixes).
fn hash_seg(s: &str, salt: u64) -> String {
    format!("{:05x}", mix(s, salt) & 0xf_ffff)
}

/// A deterministic UUID-shaped string derived from `s` (not RFC 4122, but
/// stable and realistic-looking for demo data).
fn uuid_like(s: &str) -> String {
    let a = mix(s, 0x1);
    let b = mix(s, 0x2);
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (a >> 32) as u32,
        (a >> 16) as u16,
        a as u16,
        (b >> 48) as u16,
        b & 0xffff_ffff_ffff,
    )
}
