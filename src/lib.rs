use rand::{Rng, SeedableRng, rngs};
use serde::{
    Serialize, Serializer,
    ser::{SerializeMap, SerializeSeq},
};
use std::{
    borrow::Cow,
    cell::RefCell,
    collections::HashMap,
    error,
    num::{NonZeroU64, NonZeroU128},
};
use time::UtcDateTime;
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
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    pub fn record<T: Into<serde_json::Value>>(&mut self, field: &Field, value: T) {
        self.fields.insert(field.name().into(), value.into());
    }
}

impl From<HashMap<Cow<'static, str>, serde_json::Value>> for Fields {
    fn from(value: HashMap<Cow<'static, str>, serde_json::Value>) -> Self {
        Self { fields: value }
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
    pub time: UtcDateTime,
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

impl HoneycombEvent {
    fn serialize_data_fields<M: SerializeMap>(
        &self,
        m: &mut M,
    ) -> Result<(), <M as SerializeMap>::Error> {
        if let Some(ref span_id) = self.span_id {
            m.serialize_entry("trace.span_id", span_id)?;
        }
        if let Some(ref trace_id) = self.trace_id {
            m.serialize_entry("trace.trace_id", trace_id)?;
        }
        if let Some(ref parent_span_id) = self.parent_span_id {
            m.serialize_entry("trace.parent_id", parent_span_id)?;
        }
        if let Some(ref service_name) = self.service_name {
            m.serialize_entry("service.name", service_name)?;
        }
        if let Some(ref annotation_type) = self.annotation_type {
            m.serialize_entry("meta.annotation_type", annotation_type)?;
        }
        if let Some(ref duration_ms) = self.duration_ms {
            m.serialize_entry("duration_ms", duration_ms)?;
        }
        if let Some(ref idle_ns) = self.idle_ns {
            m.serialize_entry("duration_ms", idle_ns)?;
        }
        if let Some(ref busy_ns) = self.busy_ns {
            m.serialize_entry("duration_ms", busy_ns)?;
        }
        m.serialize_entry("level", self.level)?;
        m.serialize_entry("name", self.name.as_ref())?;
        m.serialize_entry("target", self.target.as_ref())?;
        for (k, v) in self.fields.fields.iter() {
            m.serialize_entry(k, v)?;
        }
        Ok(())
    }
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
                .format(&time::format_description::well_known::Rfc3339)
                .map_err(serde::ser::Error::custom)?,
        )?;

        struct InnerData<'a>(&'a HoneycombEvent);

        impl<'a> Serialize for InnerData<'a> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                let mut m = serializer.serialize_map(None)?;
                self.0.serialize_data_fields(&mut m)?;
                m.end()
            }
        }

        root.serialize_entry("data", &InnerData(self))?;
        root.end()
    }
}

// TODO: custom value type
pub type ExtraFields = Vec<(Cow<'static, str>, serde_json::Value)>;

pub struct CreateEventsPayload<'a> {
    events: &'a Vec<HoneycombEvent>,
    extra_fields: &'a ExtraFields,
}

impl<'a> Serialize for CreateEventsPayload<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut events_list = serializer.serialize_seq(None)?;
        for event in self.events.iter() {
            events_list.serialize_element(&CreateEventPayload {
                event,
                extra_fields: self.extra_fields,
            })?;
        }
        events_list.end()
    }
}

struct CreateEventPayload<'a> {
    event: &'a HoneycombEvent,
    extra_fields: &'a ExtraFields,
}

impl<'a> Serialize for CreateEventPayload<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut root = serializer.serialize_map(None)?;
        root.serialize_entry(
            "time",
            &self
                .event
                .time
                .format(&time::format_description::well_known::Rfc3339)
                .map_err(serde::ser::Error::custom)?,
        )?;
        struct InnerData<'a>(&'a HoneycombEvent, &'a ExtraFields);

        impl<'a> Serialize for InnerData<'a> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                let mut m = serializer.serialize_map(None)?;
                // in most JSON parsers, second key wins and earlier occurrence gets overwritten
                // we want event fields to take precedence over extra_fields so serialize extra_fields first
                for (k, v) in self.1.iter() {
                    m.serialize_entry(k, v)?;
                }
                self.0.serialize_data_fields(&mut m)?;
                m.end()
            }
        }

        root.serialize_entry("data", &InnerData(self.event, self.extra_fields))?;
        root.end()
    }
}
