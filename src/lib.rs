use chrono::{DateTime, Utc};
use rand::{Rng, SeedableRng, rngs};
use serde::{Serialize, Serializer};
use std::{
    cell::RefCell,
    collections::HashMap,
    error,
    num::{NonZeroU64, NonZeroU128},
};
use tokio::sync::mpsc;
use tracing::field::{Field, Visit};

pub mod builder;
pub use builder::{
    Builder, HONEYCOMB_AUTH_HEADER_NAME, HONEYCOMB_SERVER_EU, HONEYCOMB_SERVER_US, builder,
};
pub use reqwest::Url;

pub fn event_channel(
    size: usize,
) -> (
    mpsc::Sender<Option<HoneycombEvent>>,
    mpsc::Receiver<Option<HoneycombEvent>>,
) {
    mpsc::channel(size)
}

#[derive(Clone, Debug, PartialEq, Default, Serialize)]
pub struct Fields {
    #[serde(flatten)]
    pub fields: HashMap<String, serde_json::Value>,
}

// list of reserved field names (case insensitive):
// trace.span_id
// trace.trace_id
// trace.parent_id
// service.name
// level
// Timestamp
// name
// target
// duration_ms

impl Fields {
    pub fn new(fields: HashMap<String, serde_json::Value>) -> Self {
        Self { fields }
    }

    pub fn record<T: Into<serde_json::Value>>(&mut self, field: &Field, value: T) {
        self.fields.insert(field.name().into(), value.into());
    }
}

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.record(field, format!("{:?}", value));
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.record(field, value);
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record(field, value);
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record(field, value);
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record(field, value);
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.record(field, value);
    }
    fn record_error(&mut self, field: &Field, value: &(dyn error::Error + 'static)) {
        self.record(field, format!("{}", value));
    }
}

thread_local! {
    /// Store random number generator for each thread
    static CURRENT_RNG: RefCell<rngs::SmallRng> = RefCell::new(rngs::SmallRng::from_os_rng());
}

/// A 8-byte value which identifies a given span.
///
/// The id is valid if it contains at least one non-zero byte.
#[derive(Clone, PartialEq, Eq, Copy, Hash)]
pub struct SpanId(NonZeroU64);

impl SpanId {
    fn generate() -> Self {
        CURRENT_RNG.with(|rng| Self::from(rng.borrow_mut().random::<NonZeroU64>()))
    }
}

impl From<NonZeroU64> for SpanId {
    fn from(value: NonZeroU64) -> Self {
        SpanId(value)
    }
}

impl std::fmt::Debug for SpanId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:016x}", self.0))
    }
}

impl std::fmt::Display for SpanId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:016x}", self.0))
    }
}

impl std::fmt::LowerHex for SpanId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::LowerHex::fmt(&self.0, f)
    }
}

impl Serialize for SpanId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(format!("{}", self).as_ref())
    }
}

/// A 16-byte value which identifies a given trace.
///
/// The id is valid if it contains at least one non-zero byte.
#[derive(Clone, PartialEq, Eq, Copy, Hash)]
pub struct TraceId(NonZeroU128);

impl TraceId {
    fn generate() -> Self {
        CURRENT_RNG.with(|rng| Self::from(rng.borrow_mut().random::<NonZeroU128>()))
    }
}

impl From<NonZeroU128> for TraceId {
    fn from(value: NonZeroU128) -> Self {
        TraceId(value)
    }
}

impl std::fmt::Debug for TraceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:032x}", self.0))
    }
}

impl std::fmt::Display for TraceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:032x}", self.0))
    }
}

impl std::fmt::LowerHex for TraceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::LowerHex::fmt(&self.0, f)
    }
}

impl Serialize for TraceId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(format!("{}", self).as_ref())
    }
}

#[derive(Serialize, Clone, Debug, PartialEq)]
pub struct HoneycombEvent {
    #[serde(serialize_with = "serialize_datetime_as_rfc3339")]
    pub time: DateTime<Utc>,

    pub data: HoneycombEventInner,
}

#[derive(Serialize, Clone, Debug, PartialEq)]
pub struct HoneycombEventInner {
    #[serde(rename = "trace.span_id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_id: Option<u64>,

    #[serde(rename = "trace.trace_id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<TraceId>,

    #[serde(rename = "trace.parent_id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_span_id: Option<u64>,

    #[serde(rename = "service.name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idle_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub busy_ns: Option<u64>,

    pub level: &'static str,
    pub name: String,
    pub target: String,
    // TODO: custom value type
    #[serde(flatten)]
    pub fields: Fields,
}

fn serialize_datetime_as_rfc3339<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, /* use_z */ true))
}

pub mod layer;

pub mod background;
