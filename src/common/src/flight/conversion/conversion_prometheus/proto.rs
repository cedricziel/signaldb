//! Protobuf wire format types for Prometheus remote_write protocol
//!
//! These prost-derived types match the Prometheus remote_write protobuf specification.
//! They are used for deserializing the wire format, then converted to internal types.
//!
//! Reference: https://github.com/prometheus/prometheus/blob/main/prompb/types.proto

use prost::Message;

/// Prometheus WriteRequest - the top-level message in remote_write protocol
#[derive(Clone, PartialEq, Message)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
    // Field 2 is reserved (used by Cortex)
    #[prost(message, repeated, tag = "3")]
    pub metadata: Vec<MetricMetadata>,
}

/// A single time series with labels, samples, and optional histograms
#[derive(Clone, PartialEq, Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
    #[prost(message, repeated, tag = "3")]
    pub exemplars: Vec<Exemplar>,
    #[prost(message, repeated, tag = "4")]
    pub histograms: Vec<Histogram>,
}

/// A label key-value pair
#[derive(Clone, PartialEq, Message)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

/// A sample value with timestamp
#[derive(Clone, PartialEq, Message)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

/// Exemplar for trace correlation
#[derive(Clone, PartialEq, Message)]
pub struct Exemplar {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(double, tag = "2")]
    pub value: f64,
    #[prost(int64, tag = "3")]
    pub timestamp: i64,
}

/// Native histogram (remote_write v2)
#[derive(Clone, PartialEq, Message)]
pub struct Histogram {
    #[prost(oneof = "histogram::Count", tags = "1, 2")]
    pub count: Option<histogram::Count>,
    #[prost(double, tag = "3")]
    pub sum: f64,
    #[prost(sint32, tag = "4")]
    pub schema: i32,
    #[prost(double, tag = "5")]
    pub zero_threshold: f64,
    #[prost(oneof = "histogram::ZeroCount", tags = "6, 7")]
    pub zero_count: Option<histogram::ZeroCount>,
    #[prost(message, repeated, tag = "8")]
    pub negative_spans: Vec<BucketSpan>,
    #[prost(sint64, repeated, tag = "9")]
    pub negative_deltas: Vec<i64>,
    #[prost(double, repeated, tag = "10")]
    pub negative_counts: Vec<f64>,
    #[prost(message, repeated, tag = "11")]
    pub positive_spans: Vec<BucketSpan>,
    #[prost(sint64, repeated, tag = "12")]
    pub positive_deltas: Vec<i64>,
    #[prost(double, repeated, tag = "13")]
    pub positive_counts: Vec<f64>,
    #[prost(enumeration = "histogram::ResetHint", tag = "14")]
    pub reset_hint: i32,
    #[prost(int64, tag = "15")]
    pub timestamp: i64,
}

pub mod histogram {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Count {
        #[prost(uint64, tag = "1")]
        CountInt(u64),
        #[prost(double, tag = "2")]
        CountFloat(f64),
    }

    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ZeroCount {
        #[prost(uint64, tag = "6")]
        ZeroCountInt(u64),
        #[prost(double, tag = "7")]
        ZeroCountFloat(f64),
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ResetHint {
        Unknown = 0,
        Yes = 1,
        No = 2,
        Gauge = 3,
    }
}

/// Bucket span for native histograms
#[derive(Clone, PartialEq, Message)]
pub struct BucketSpan {
    #[prost(sint32, tag = "1")]
    pub offset: i32,
    #[prost(uint32, tag = "2")]
    pub length: u32,
}

/// Metric metadata
#[derive(Clone, PartialEq, Message)]
pub struct MetricMetadata {
    #[prost(enumeration = "MetricType", tag = "1")]
    pub r#type: i32,
    #[prost(string, tag = "2")]
    pub metric_family_name: String,
    #[prost(string, tag = "4")]
    pub help: String,
    #[prost(string, tag = "5")]
    pub unit: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum MetricType {
    Unknown = 0,
    Counter = 1,
    Gauge = 2,
    Summary = 3,
    Histogram = 4,
    GaugeHistogram = 5,
    Info = 6,
    Stateset = 7,
}
