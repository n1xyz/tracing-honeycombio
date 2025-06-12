use chrono::{DateTime, Utc};
use serde::Serialize;
use std::{collections::HashMap, error};
use tokio::sync::mpsc;
use tracing::field::{Field, Visit};

pub mod builder;
pub use builder::{
    Builder, HONEYCOMB_AUTH_HEADER_NAME, HONEYCOMB_SERVER_EU, HONEYCOMB_SERVER_US, builder,
};
pub use reqwest::Url;

fn event_channel(
    size: usize,
) -> (
    mpsc::Sender<Option<HoneycombEvent>>,
    mpsc::Receiver<Option<HoneycombEvent>>,
) {
    mpsc::channel(size) // make it so big that if we drop events, it's already kind of bad
}

#[derive(Clone, Default, Serialize)]
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

#[derive(Serialize, Clone)]
pub struct HoneycombEvent {
    #[serde(serialize_with = "serialize_datetime_as_rfc3339")]
    pub timestamp: DateTime<Utc>,

    #[serde(rename = "trace.span_id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_id: Option<u64>,

    #[serde(rename = "trace.parent_id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<u64>,

    #[serde(rename = "service.name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
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
