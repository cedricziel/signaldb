pub mod tempopb {
    include!("tempopb.rs");

    pub mod common {
        pub mod v1 {
            include!("tempopb.common.v1.rs");
        }
    }

    pub mod resource {
        pub mod v1 {
            include!("opentelemetry.proto.resource.v1.rs");
        }
    }
    pub mod trace {
        pub mod v1 {
            include!("tempopb.trace.v1.rs");
        }
    }
}

pub mod opentelemetry {
    pub mod proto {
        pub mod common {
            pub mod v1 {
                include!("opentelemetry.proto.common.v1.rs");
            }
        }
        pub mod resource {
            pub mod v1 {
                include!("opentelemetry.proto.resource.v1.rs");
            }
        }
    }
}
