// This file is @generated by prost-build.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TracesData {
    #[prost(message, repeated, tag = "1")]
    pub resource_spans: ::prost::alloc::vec::Vec<ResourceSpans>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResourceSpans {
    #[prost(message, optional, tag = "1")]
    pub resource: ::core::option::Option<super::super::resource::v1::Resource>,
    #[prost(message, repeated, tag = "2")]
    pub scope_spans: ::prost::alloc::vec::Vec<ScopeSpans>,
    #[prost(string, tag = "3")]
    pub schema_url: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScopeSpans {
    #[prost(message, optional, tag = "1")]
    pub scope: ::core::option::Option<super::super::common::v1::InstrumentationScope>,
    #[prost(message, repeated, tag = "2")]
    pub spans: ::prost::alloc::vec::Vec<Span>,
    #[prost(string, tag = "3")]
    pub schema_url: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Span {
    #[prost(bytes = "vec", tag = "1")]
    pub trace_id: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub span_id: ::prost::alloc::vec::Vec<u8>,
    #[prost(string, tag = "3")]
    pub trace_state: ::prost::alloc::string::String,
    #[prost(bytes = "vec", tag = "4")]
    pub parent_span_id: ::prost::alloc::vec::Vec<u8>,
    #[prost(fixed32, tag = "16")]
    pub flags: u32,
    #[prost(string, tag = "5")]
    pub name: ::prost::alloc::string::String,
    #[prost(enumeration = "span::SpanKind", tag = "6")]
    pub kind: i32,
    #[prost(fixed64, tag = "7")]
    pub start_time_unix_nano: u64,
    #[prost(fixed64, tag = "8")]
    pub end_time_unix_nano: u64,
    #[prost(message, repeated, tag = "9")]
    pub attributes: ::prost::alloc::vec::Vec<super::super::common::v1::KeyValue>,
    #[prost(uint32, tag = "10")]
    pub dropped_attributes_count: u32,
    #[prost(message, repeated, tag = "11")]
    pub events: ::prost::alloc::vec::Vec<span::Event>,
    #[prost(uint32, tag = "12")]
    pub dropped_events_count: u32,
    #[prost(message, repeated, tag = "13")]
    pub links: ::prost::alloc::vec::Vec<span::Link>,
    #[prost(uint32, tag = "14")]
    pub dropped_links_count: u32,
    #[prost(message, optional, tag = "15")]
    pub status: ::core::option::Option<Status>,
}
/// Nested message and enum types in `Span`.
pub mod span {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Event {
        #[prost(fixed64, tag = "1")]
        pub time_unix_nano: u64,
        #[prost(string, tag = "2")]
        pub name: ::prost::alloc::string::String,
        #[prost(message, repeated, tag = "3")]
        pub attributes: ::prost::alloc::vec::Vec<
            super::super::super::common::v1::KeyValue,
        >,
        #[prost(uint32, tag = "4")]
        pub dropped_attributes_count: u32,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Link {
        #[prost(bytes = "vec", tag = "1")]
        pub trace_id: ::prost::alloc::vec::Vec<u8>,
        #[prost(bytes = "vec", tag = "2")]
        pub span_id: ::prost::alloc::vec::Vec<u8>,
        #[prost(string, tag = "3")]
        pub trace_state: ::prost::alloc::string::String,
        #[prost(message, repeated, tag = "4")]
        pub attributes: ::prost::alloc::vec::Vec<
            super::super::super::common::v1::KeyValue,
        >,
        #[prost(uint32, tag = "5")]
        pub dropped_attributes_count: u32,
        #[prost(fixed32, tag = "6")]
        pub flags: u32,
    }
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum SpanKind {
        Unspecified = 0,
        Internal = 1,
        Server = 2,
        Client = 3,
        Producer = 4,
        Consumer = 5,
    }
    impl SpanKind {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Self::Unspecified => "SPAN_KIND_UNSPECIFIED",
                Self::Internal => "SPAN_KIND_INTERNAL",
                Self::Server => "SPAN_KIND_SERVER",
                Self::Client => "SPAN_KIND_CLIENT",
                Self::Producer => "SPAN_KIND_PRODUCER",
                Self::Consumer => "SPAN_KIND_CONSUMER",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "SPAN_KIND_UNSPECIFIED" => Some(Self::Unspecified),
                "SPAN_KIND_INTERNAL" => Some(Self::Internal),
                "SPAN_KIND_SERVER" => Some(Self::Server),
                "SPAN_KIND_CLIENT" => Some(Self::Client),
                "SPAN_KIND_PRODUCER" => Some(Self::Producer),
                "SPAN_KIND_CONSUMER" => Some(Self::Consumer),
                _ => None,
            }
        }
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Status {
    #[prost(string, tag = "2")]
    pub message: ::prost::alloc::string::String,
    #[prost(enumeration = "status::StatusCode", tag = "3")]
    pub code: i32,
}
/// Nested message and enum types in `Status`.
pub mod status {
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum StatusCode {
        Unset = 0,
        Ok = 1,
        Error = 2,
    }
    impl StatusCode {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Self::Unset => "STATUS_CODE_UNSET",
                Self::Ok => "STATUS_CODE_OK",
                Self::Error => "STATUS_CODE_ERROR",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "STATUS_CODE_UNSET" => Some(Self::Unset),
                "STATUS_CODE_OK" => Some(Self::Ok),
                "STATUS_CODE_ERROR" => Some(Self::Error),
                _ => None,
            }
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SpanFlags {
    DoNotUse = 0,
    TraceFlagsMask = 255,
    ContextHasIsRemoteMask = 256,
    ContextIsRemoteMask = 512,
}
impl SpanFlags {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::DoNotUse => "SPAN_FLAGS_DO_NOT_USE",
            Self::TraceFlagsMask => "SPAN_FLAGS_TRACE_FLAGS_MASK",
            Self::ContextHasIsRemoteMask => "SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE_MASK",
            Self::ContextIsRemoteMask => "SPAN_FLAGS_CONTEXT_IS_REMOTE_MASK",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "SPAN_FLAGS_DO_NOT_USE" => Some(Self::DoNotUse),
            "SPAN_FLAGS_TRACE_FLAGS_MASK" => Some(Self::TraceFlagsMask),
            "SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE_MASK" => Some(Self::ContextHasIsRemoteMask),
            "SPAN_FLAGS_CONTEXT_IS_REMOTE_MASK" => Some(Self::ContextIsRemoteMask),
            _ => None,
        }
    }
}