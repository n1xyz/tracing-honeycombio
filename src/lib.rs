use chrono::{DateTime, Utc};
use rand::{Rng, SeedableRng, rngs};
use serde::{Serialize, Serializer, ser::SerializeMap};
use std::{
    borrow::Cow,
    cell::RefCell,
    collections::HashMap,
    error,
    num::{NonZeroU64, NonZeroU128},
};
use tracing::field::{Field, Visit};

pub mod background;
pub mod builder;
pub mod layer;

pub use builder::{
    Builder, HONEYCOMB_AUTH_HEADER_NAME, HONEYCOMB_SERVER_EU, HONEYCOMB_SERVER_US, builder,
};
pub use reqwest::Url;

#[derive(Clone, Debug, PartialEq, Default, Serialize)]
pub struct Fields {
    #[serde(flatten)]
    pub fields: HashMap<Cow<'static, str>, serde_json::Value>,
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
    pub fn new(fields: HashMap<Cow<'static, str>, serde_json::Value>) -> Self {
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
#[repr(transparent)]
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
#[repr(transparent)]
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

#[derive(Clone, Debug, PartialEq)]
pub struct HoneycombEvent {
    pub time: DateTime<Utc>,
    pub span_id: Option<SpanId>,
    pub trace_id: Option<TraceId>,
    pub parent_span_id: Option<SpanId>,
    pub service_name: Option<Cow<'static, str>>,
    pub annotation_type: Option<Cow<'static, str>>,
    pub duration_ms: Option<u64>,
    pub idle_ns: Option<u64>,
    pub busy_ns: Option<u64>,
    pub level: &'static str,
    pub name: Cow<'static, str>,
    pub target: Cow<'static, str>,
    pub fields: Fields,
}

impl Serialize for HoneycombEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut root = serializer.serialize_map(None)?;
        root.serialize_entry(
            "time",
            &self
                .time
                .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, /* use_z */ true),
        )?;

        struct InnerData<'a>(&'a HoneycombEvent);

        impl<'a> Serialize for InnerData<'a> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                let mut m = serializer.serialize_map(None)?;
                if let Some(ref span_id) = self.0.span_id {
                    m.serialize_entry("trace.span_id", span_id)?;
                }
                if let Some(ref trace_id) = self.0.trace_id {
                    m.serialize_entry("trace.trace_id", trace_id)?;
                }
                if let Some(ref parent_span_id) = self.0.parent_span_id {
                    m.serialize_entry("trace.parent_id", parent_span_id)?;
                }
                if let Some(ref service_name) = self.0.service_name {
                    m.serialize_entry("service.name", service_name)?;
                }
                if let Some(ref annotation_type) = self.0.annotation_type {
                    m.serialize_entry("meta.annotation_type", annotation_type)?;
                }
                if let Some(ref duration_ms) = self.0.duration_ms {
                    m.serialize_entry("duration_ms", duration_ms)?;
                }
                if let Some(ref idle_ns) = self.0.idle_ns {
                    m.serialize_entry("duration_ms", idle_ns)?;
                }
                if let Some(ref busy_ns) = self.0.busy_ns {
                    m.serialize_entry("duration_ms", busy_ns)?;
                }
                m.serialize_entry("level", self.0.level)?;
                m.serialize_entry("name", self.0.name.as_ref())?;
                m.serialize_entry("target", self.0.target.as_ref())?;
                for (k, v) in self.0.fields.fields.iter() {
                    m.serialize_entry(k, v)?;
                }
                m.end()
            }
        }

        root.serialize_entry("data", &InnerData(self))?;
        root.end()
    }
}
